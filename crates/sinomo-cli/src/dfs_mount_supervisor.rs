use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_runtime::config::{ConfigRegistry, ConfigStore, ConfigWriteTarget};
use sinomo_runtime::daemon::{DaemonApiClient, DaemonPaths, DaemonServiceRegistryHook};
use sinomo_runtime::dfs::{
    SIFS_MOUNT_CONFIG_KEY, SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE, SIFS_MOUNT_STATUS_CONFIG_KEY,
    SIFS_MOUNT_STATUS_ROUTE, SifsMountConfig, SifsMountCursorOnlyRecoveryRequest,
    SifsMountCursorOnlyRecoveryResponse, SifsMountPlatform, SifsMountRuntimePhase,
    SifsMountRuntimeStatus, SifsMountStatusRequest, SifsMountStatusResponse,
    SifsMountWritebackMode, sifs_mount_config_definition, sifs_mount_status_config_definition,
};
#[cfg(any(windows, test))]
use sinomo_runtime::dfs::{
    SifsMountAttentionKind, SifsMountAttentionNextAction, SifsMountAttentionSummary,
    SifsMountRetainedLocalEvidence,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(500);
const SUPERVISOR_DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
const MOUNT_START_RETRY_INITIAL: Duration = Duration::from_secs(1);
#[cfg(windows)]
const CLOUD_FILES_WORKER_STALE_AFTER: Duration = Duration::from_secs(3);
const MOUNT_START_RETRY_MAX: Duration = Duration::from_secs(30);
#[cfg(any(windows, test))]
const PENDING_CLOUD_UNREGISTER_FILE: &str = "cloud-files-unregister.pending.sobj";
#[cfg(any(windows, test))]
const PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION: u16 = 2;
#[cfg(any(windows, test))]
const STAGED_CLOUD_REGISTRATION_FILE: &str = "cloud-files-registration.staged.sobj";
#[cfg(any(windows, test))]
const STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION: u16 = 3;
#[cfg(any(windows, test))]
const CLOUD_UNREGISTER_RECOVERY_DIR: &str = "cloud-files-unregister-recovery";
#[cfg(any(windows, test))]
const CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE: &str = "provenance.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_REGISTRATION_RECEIPT_FILE: &str = "registration.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_REGISTRATION_JOURNAL_FILE: &str = "registration-journal.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE: &str = "cursor-recovery-control.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_FILE: &str = "cloud-files-cursor-recovery.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_FILE: &str =
    "cloud-files-cursor-recovery-provider.sobj";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_STAGED_TEMP_FILE: &str =
    ".cloud-files-registration.staged.sobj.cursor-recovery-next";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_PENDING_TEMP_FILE: &str =
    ".cloud-files-unregister.pending.sobj.cursor-recovery-next";
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION: u16 = 1;
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES: u64 = 20 * 1024 * 1024;
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES: u64 = 128 * 1024 * 1024;
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES: u64 = 160 * 1024 * 1024;
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_HOST_LOCK_MAX_BYTES: u64 = 64 * 1024;
#[cfg(any(windows, test))]
const CLOUD_FILES_CURSOR_RECOVERY_HOST_TOKEN_DOMAIN: &str =
    "sinomo.dfs.cloud-files.cursor-only-recovery.host.v1";
#[cfg(windows)]
const MOUNT_STOP_TIMEOUT: Duration = Duration::from_secs(5);
// Give the worker enough time to consume its own bounded mount-stop timeout
// and report completion before the service owner stops waiting for it.
const SUPERVISOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(7);

#[derive(Clone)]
pub(crate) struct SifsMountSupervisorService {
    inner: Arc<SifsMountSupervisorInner>,
}

struct SifsMountSupervisorInner {
    status: Arc<RwLock<SifsMountRuntimeStatus>>,
    shutdown: Arc<AtomicBool>,
    mount_cancellation: Arc<Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>>,
    cursor_recovery: mpsc::SyncSender<SifsMountCursorRecoveryControl>,
    worker: Mutex<Option<SifsMountSupervisorWorker>>,
}

struct SifsMountCursorRecoveryControl {
    request: SifsMountCursorOnlyRecoveryRequest,
    response: mpsc::SyncSender<Result<SifsMountCursorOnlyRecoveryResponse, String>>,
}

struct SifsMountSupervisorWorker {
    thread: Option<thread::JoinHandle<()>>,
    stopped: mpsc::Receiver<()>,
}

#[derive(Default)]
struct MountStartRetry {
    config: Option<SifsMountConfig>,
    delay: Duration,
    next_attempt_at: Option<Instant>,
    last_error: Option<String>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MountRetirementReason {
    ExplicitDisable,
    RegistrationReplacement,
    ConfigurationRestart,
    InvalidConfiguration,
    ConfigurationUnavailable,
    DaemonReadinessLoss,
    ProviderRestart,
    ServiceShutdown,
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
impl MountRetirementReason {
    fn requires_cloud_unregister(self) -> bool {
        self == Self::ExplicitDisable
    }
}

#[derive(Debug)]
struct RetiredMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
    #[cfg(windows)]
    lease: Option<Arc<SifsMountLease>>,
    #[cfg(windows)]
    module_provenance: CloudFilesModuleProvenance,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesModuleProvenance {
    module_id: sinomo_runtime::module::ModuleId,
    content_hash: String,
    cache_key: String,
    library_name: String,
    platform_triple: String,
    native_abi_version: u32,
    interface_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct PendingCloudUnregisterRecord {
    schema_version: u16,
    request: sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest,
    operation: String,
    payload_sobj: Vec<u8>,
    module: CloudFilesModuleProvenance,
    /// Exact deployed-v2 provider which may have created writable state before
    /// its registration journal. A preserve replay carrying this origin may
    /// settle only with a typed Retained outcome from the exact v3 provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    legacy_prejournal_origin: Option<CloudFilesModuleProvenance>,
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum StagedCloudRegistrationState {
    MayBeRegistered,
    RetainedAbsent,
}

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StagedCloudRegistrationRecord {
    schema_version: u16,
    state: StagedCloudRegistrationState,
    recovery: PendingCloudUnregisterRecord,
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum HostCursorRecoveryPhase {
    Requested,
    ModuleApplied,
    AdoptedRetained,
    ArmedAwaitingReady,
    ReadyObserved,
    ActiveObserved,
    AuthorityFinalized,
    PreservedRetained,
}

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HostCursorRecoveryMarker {
    schema_version: u16,
    disabled_config: SifsMountConfig,
    module: CloudFilesModuleProvenance,
    identity: sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity,
    outer_token: String,
    inner_token: String,
    recovery_id: String,
    inspection: sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied: Option<sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplied>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ready_artifacts_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preserved_artifacts_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_module_marker_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion_module_marker_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion_registration_artifacts_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_artifacts_fingerprint: Option<String>,
    /// Monotonic disable/re-enable generation. Initial adoption is cycle zero;
    /// every forward PreserveState successor increments it exactly once.
    #[serde(default)]
    recovery_cycle: u64,
    /// Exact opaque native `PreservedStateOwned` successor for cycles after
    /// the initial adoption. The host carries but never interprets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_successor_fingerprint: Option<String>,
    phase: HostCursorRecoveryPhase,
}

/// The first durable host effect for cursor recovery. Keeping the exact
/// provider bytes and the complete Requested authorization in one canonical
/// object makes the cache-loss and marker-publication cuts independently
/// replayable without trusting whichever loader happens to be active later.
#[cfg(any(windows, test))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HostCursorRecoveryProviderBundle {
    schema_version: u16,
    requested: HostCursorRecoveryMarker,
    provider_bytes: Vec<u8>,
    /// A rollout may replace the process-resident provider after recovery was
    /// authorized. The original provider above remains the durable ownership
    /// and unregister authority; this optional pair pins only the exact code
    /// allowed to resume that already-authorized recovery cycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_module: Option<CloudFilesModuleProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_provider_bytes: Option<Vec<u8>>,
}

#[cfg(any(windows, test))]
struct HostCursorRecoveryArtifactSnapshot<T> {
    value: Option<T>,
    temporary: bool,
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
#[derive(Debug)]
struct PendingCloudUnregister {
    record: PendingCloudUnregisterRecord,
    lease: Option<Arc<SifsMountLease>>,
    retry_delay: Duration,
    next_attempt_at: Option<Instant>,
    last_error: Option<String>,
}

#[cfg(any(windows, test))]
struct PendingCloudUnregisterArtifactSnapshot {
    value: Option<PendingCloudUnregisterRecord>,
    current_present: bool,
    temporary_present: bool,
}

#[cfg(any(windows, test))]
struct CursorRecoveryStagedArtifactSnapshot {
    current: Option<StagedCloudRegistrationRecord>,
    temporary: Option<StagedCloudRegistrationRecord>,
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
impl PendingCloudUnregister {
    fn new(record: PendingCloudUnregisterRecord, lease: Option<Arc<SifsMountLease>>) -> Self {
        Self {
            record,
            lease,
            retry_delay: Duration::ZERO,
            next_attempt_at: None,
            last_error: None,
        }
    }

    fn permits_attempt(&self, now: Instant) -> bool {
        self.next_attempt_at.is_none_or(|deadline| now >= deadline)
    }

    fn record_failure(&mut self, error: String, now: Instant) {
        self.retry_delay = if self.retry_delay.is_zero() {
            MOUNT_START_RETRY_INITIAL
        } else {
            self.retry_delay
                .saturating_mul(2)
                .min(MOUNT_START_RETRY_MAX)
        };
        self.next_attempt_at = Some(now + self.retry_delay);
        self.last_error = Some(error);
    }

    #[cfg(windows)]
    fn clear_retry(&mut self) {
        self.retry_delay = Duration::ZERO;
        self.next_attempt_at = None;
        self.last_error = None;
    }
}

fn cloud_registration_identity_changed(old: &SifsMountConfig, new: &SifsMountConfig) -> bool {
    old.mount_point != new.mount_point || old.root_path != new.root_path
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn pending_unregister_for_transition(reason: MountRetirementReason) -> bool {
    reason.requires_cloud_unregister()
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn drive_pending_cloud_unregister_with<F>(
    pending: &mut Option<PendingCloudUnregister>,
    now: Instant,
    unregister: F,
) -> Result<(), String>
where
    F: FnOnce(&mut PendingCloudUnregister) -> Result<(), String>,
{
    let Some(current) = pending.as_mut() else {
        return Ok(());
    };
    if !current.permits_attempt(now) {
        return Err(current
            .last_error
            .clone()
            .unwrap_or_else(|| "Cloud Files unregister retry is not due yet".to_string()));
    }
    match unregister(current) {
        Ok(()) => {
            pending.take();
            Ok(())
        }
        Err(error) => {
            current.record_failure(error.clone(), now);
            Err(error)
        }
    }
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn stop_then_drive_pending_cloud_unregister_with<FStop, FUnregister>(
    pending: &mut Option<PendingCloudUnregister>,
    now: Instant,
    stop: FStop,
    unregister: FUnregister,
) -> Result<(), String>
where
    FStop: FnOnce() -> Result<(), String>,
    FUnregister: FnOnce(&mut PendingCloudUnregister) -> Result<(), String>,
{
    if let Err(error) = stop() {
        if let Some(current) = pending.as_mut() {
            current.record_failure(error.clone(), now);
        }
        return Err(error);
    }
    drive_pending_cloud_unregister_with(pending, now, unregister)
}

fn stop_after_failed_cloud_retirement_preparation_with<FStop, FUnregister, FStart>(
    preparation_error: String,
    stop: FStop,
    unregister: FUnregister,
    start_replacement: FStart,
) -> Result<(), String>
where
    FStop: FnOnce() -> Result<(), String>,
    FUnregister: FnOnce() -> Result<(), String>,
    FStart: FnOnce() -> Result<(), String>,
{
    // Keep forbidden effects owned by this ordering seam so callers cannot
    // accidentally run them while handling a preparation failure.
    let _forbidden_effects = (unregister, start_replacement);
    match stop() {
        Ok(()) => Err(format!(
            "{preparation_error}; the active Cloud Files provider was stopped and all registration ownership evidence remains pinned"
        )),
        Err(stop_error) => Err(format!(
            "{preparation_error}; additionally failed to stop the active Cloud Files provider: {stop_error}"
        )),
    }
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn cloud_unregister_request(
    data_dir: &SinomoDataDir,
    retired: &RetiredMount,
) -> sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
    sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
        sync_root: retired.mount_point.clone(),
        root_path: retired.config.root_path.clone(),
        data_dir_root: Some(data_dir.root()),
        provider_name: sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
            .to_string(),
        cursor_recovery: None,
    }
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_context(
    marker: &HostCursorRecoveryMarker,
) -> sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
    sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
        recovery_id: marker.recovery_id.clone(),
        recovery_token: marker.inner_token.clone(),
        recovery_cycle: marker.recovery_cycle,
        native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
    }
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloudUnregisterPolicy {
    Release,
    PreserveState,
}

#[cfg(any(windows, test))]
fn provider_supports_state_preserving_unregister(
    interface_version: &str,
    supported_operations: &[String],
    capabilities: &[String],
) -> bool {
    interface_version
        == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        && supported_operations.iter().any(|operation| {
            operation
                == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        })
        && capabilities.iter().any(|capability| {
            capability
                == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY
        })
}

#[cfg(any(windows, test))]
fn admit_cloud_files_provider_before_effect_with<R, FEffect>(
    provenance: &CloudFilesModuleProvenance,
    descriptor: &sinomo_runtime::module_api::NativeModuleDescriptor,
    required_operation: &str,
    required_capability: Option<&str>,
    effect: FEffect,
) -> Result<R, String>
where
    FEffect: FnOnce() -> Result<R, String>,
{
    validate_cloud_files_provider_descriptor(provenance, descriptor, required_operation)?;
    if let Some(required_capability) = required_capability {
        if descriptor.interface_version
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
            || !descriptor
                .capabilities
                .iter()
                .any(|capability| capability == required_capability)
        {
            return Err(format!(
                "Cloud Files provider lacks required capability {required_capability}"
            ));
        }
    }
    effect()
}

#[cfg(any(windows, test))]
fn validate_cloud_files_provider_descriptor(
    provenance: &CloudFilesModuleProvenance,
    descriptor: &sinomo_runtime::module_api::NativeModuleDescriptor,
    required_operation: &str,
) -> Result<(), String> {
    use sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID;
    use sinomo_runtime::module::ModuleId;
    use sinomo_runtime::module_api::NativeModuleBackend;

    let descriptor_module_id = ModuleId::from_legacy_named(&descriptor.module_id)
        .map_err(|error| format!("invalid loaded Cloud Files descriptor module id: {error}"))?;

    if descriptor.module_id != SINOMO_DFS_CLOUD_FILES_MODULE_ID
        || descriptor_module_id != provenance.module_id
        || descriptor.backend != NativeModuleBackend::NativeDylib
        || descriptor.interface_version != provenance.interface_version
        || descriptor.native_abi_version != provenance.native_abi_version
        || descriptor.platform_triple != provenance.platform_triple
    {
        return Err(format!(
            "loaded Cloud Files descriptor does not match durable provenance: descriptor {} / {} / ABI {} / {}, provenance {} / {} / ABI {} / {}",
            descriptor.module_id,
            descriptor.interface_version,
            descriptor.native_abi_version,
            descriptor.platform_triple,
            provenance.module_id,
            provenance.interface_version,
            provenance.native_abi_version,
            provenance.platform_triple,
        ));
    }
    if !descriptor
        .supported_operations
        .iter()
        .any(|operation| operation == required_operation)
    {
        return Err(format!(
            "Cloud Files provider {} does not advertise required operation {}",
            descriptor.interface_version, required_operation
        ));
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn pending_cloud_unregister_record_with_policy(
    request: sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest,
    module: CloudFilesModuleProvenance,
    policy: CloudUnregisterPolicy,
) -> Result<PendingCloudUnregisterRecord, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleRequest, SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
    };
    if policy == CloudUnregisterPolicy::PreserveState
        && module.interface_version
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
    {
        return Err(format!(
            "Cloud Files provider interface {} does not support state-preserving unregister",
            module.interface_version
        ));
    }
    let (operation, payload) = match policy {
        CloudUnregisterPolicy::Release => (
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
            CloudFilesModuleRequest::Unregister(request.clone()),
        ),
        CloudUnregisterPolicy::PreserveState => (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudFilesModuleRequest::PreserveStateUnregister(request.clone()),
        ),
    };
    let payload_sobj = sinomo_runtime::module_api::encode_sobj(&payload)
        .map_err(|error| format!("encode durable Cloud Files unregister descriptor: {error}"))?;
    Ok(PendingCloudUnregisterRecord {
        schema_version: PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION,
        request,
        operation: operation.to_string(),
        payload_sobj,
        module,
        legacy_prejournal_origin: None,
    })
}

#[cfg(any(windows, test))]
fn pending_cloud_unregister_record(
    request: sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest,
    module: CloudFilesModuleProvenance,
) -> Result<PendingCloudUnregisterRecord, String> {
    pending_cloud_unregister_record_with_policy(request, module, CloudUnregisterPolicy::Release)
}

#[cfg(any(windows, test))]
fn same_cloud_registration_recovery_identity(
    left: &PendingCloudUnregisterRecord,
    right: &PendingCloudUnregisterRecord,
) -> bool {
    same_cloud_registration_base_identity(left, right)
        && left.module == right.module
        && left.legacy_prejournal_origin == right.legacy_prejournal_origin
}

#[cfg(any(windows, test))]
fn same_cloud_registration_base_identity(
    left: &PendingCloudUnregisterRecord,
    right: &PendingCloudUnregisterRecord,
) -> bool {
    left.schema_version == right.schema_version
        && left.request.sync_root == right.request.sync_root
        && left.request.root_path == right.request.root_path
        && left.request.data_dir_root == right.request.data_dir_root
        && left.request.provider_name == right.request.provider_name
}

#[cfg(any(windows, test))]
fn policy_neutral_cloud_registration_recovery(
    record: &PendingCloudUnregisterRecord,
) -> Result<PendingCloudUnregisterRecord, String> {
    let mut normalized = pending_cloud_unregister_record_with_policy(
        record.request.clone(),
        record.module.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    normalized.legacy_prejournal_origin = record.legacy_prejournal_origin.clone();
    Ok(normalized)
}

#[cfg(any(windows, test))]
fn staged_cloud_registration_record(
    recovery: &PendingCloudUnregisterRecord,
    state: StagedCloudRegistrationState,
) -> Result<StagedCloudRegistrationRecord, String> {
    Ok(StagedCloudRegistrationRecord {
        schema_version: STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION,
        state,
        recovery: policy_neutral_cloud_registration_recovery(recovery)?,
    })
}

#[cfg(any(windows, test))]
fn staged_cursor_recovery_successor_record(
    pending: &PendingCloudUnregisterRecord,
    response: &sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse,
) -> Result<StagedCloudRegistrationRecord, String> {
    let current = pending.request.cursor_recovery.as_ref().ok_or_else(|| {
        "cursor-recovery Retained response requires a context-bound pending intent".to_string()
    })?;
    let next = response.cursor_recovery.as_ref().ok_or_else(|| {
        "cursor-recovery Retained response omitted its native successor context".to_string()
    })?;
    let valid_successor_fingerprint =
        next.native_successor_fingerprint
            .as_deref()
            .is_some_and(|value| {
                value.strip_prefix("blake3:").is_some_and(|hex| {
                    hex.len() == 64
                        && hex
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            });
    if current.recovery_id != next.recovery_id
        || current.recovery_token != next.recovery_token
        || current
            .recovery_cycle
            .checked_add(1)
            .is_none_or(|cycle| cycle != next.recovery_cycle)
        || !valid_successor_fingerprint
    {
        return Err(
            "cursor-recovery Retained response is not the exact next-cycle native successor"
                .to_string(),
        );
    }
    let mut successor = pending.clone();
    successor.request.cursor_recovery = Some(next.clone());
    let mut retained =
        staged_cloud_registration_record(&successor, StagedCloudRegistrationState::RetainedAbsent)?;
    retained.recovery.legacy_prejournal_origin = None;
    Ok(retained)
}

#[cfg(any(windows, test))]
fn staged_state_after_unregister_response(
    record: &PendingCloudUnregisterRecord,
    _staged: &StagedCloudRegistrationRecord,
    response: &CloudUnregisterCompletion,
) -> Result<Option<StagedCloudRegistrationState>, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesPreserveStateUnregisterOutcome,
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
    };
    match (record.operation.as_str(), response) {
        (SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION, CloudUnregisterCompletion::Released(_)) => {
            Ok(None)
        }
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudUnregisterCompletion::StatePreserved(response),
        ) if response.outcome == CloudFilesPreserveStateUnregisterOutcome::Retained => {
            Ok(Some(StagedCloudRegistrationState::RetainedAbsent))
        }
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudUnregisterCompletion::StatePreserved(response),
        ) if response.outcome == CloudFilesPreserveStateUnregisterOutcome::Released
            && record.request.cursor_recovery.is_some() =>
        {
            Err("cursor-bound PreserveState may only complete with the exact Retained successor"
                .to_string())
        }
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudUnregisterCompletion::StatePreserved(response),
        ) if response.outcome == CloudFilesPreserveStateUnregisterOutcome::Released
            && record.legacy_prejournal_origin.is_some() =>
        {
            Err("legacy Cloud Files pre-journal risk cannot accept a Released preserve response; exact stage, provider, and pending intent remain pinned"
                .to_string())
        }
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudUnregisterCompletion::StatePreserved(_),
        ) => Ok(None),
        _ => Err("Cloud Files unregister operation and typed response do not match".to_string()),
    }
}

#[cfg(any(windows, test))]
fn finalize_cloud_unregister_completion_with<F>(
    data_dir: &SinomoDataDir,
    pending: &PendingCloudUnregisterRecord,
    staged: &StagedCloudRegistrationRecord,
    response: &CloudUnregisterCompletion,
    after_first_durable_step: F,
) -> Result<(), String>
where
    F: FnOnce() -> Result<(), String>,
{
    if !same_cloud_registration_recovery_identity(&staged.recovery, pending) {
        return Err(
            "pending Cloud Files unregister no longer matches its staged ownership identity"
                .to_string(),
        );
    }
    let next_state = staged_state_after_unregister_response(pending, staged, response)?;
    if next_state == Some(StagedCloudRegistrationState::RetainedAbsent) {
        let mut retained = match response {
            CloudUnregisterCompletion::StatePreserved(response)
                if pending.request.cursor_recovery.is_some() =>
            {
                staged_cursor_recovery_successor_record(pending, response)?
            }
            _ => staged_cloud_registration_record(
                pending,
                StagedCloudRegistrationState::RetainedAbsent,
            )?,
        };
        // A typed Retained response proves that the module found or normalized
        // its exact journal to StateOwned. Legacy pre-journal ambiguity is now
        // discharged; the original v2 provider is no longer cleanup authority.
        retained.recovery.legacy_prejournal_origin = None;
        if pending.request.cursor_recovery.is_some() {
            transition_or_promote_cursor_recovery_staged_record(data_dir, staged, &retained)?;
        } else {
            replace_staged_cloud_registration_record(data_dir, staged, &retained)?;
        }
        after_first_durable_step()?;
        advance_host_cursor_recovery_preserved_successor(data_dir, pending, &retained)?;
        clear_pending_cloud_unregister_record(data_dir, pending)?;
        remove_discharged_legacy_recovery_module(data_dir, pending);
    } else {
        // Released is replay-idempotent only because the module proved journal,
        // receipt, and CFAPI absence. Clear pending first: a crash then leaves a
        // staged MayBeRegistered record which safely reconstructs the attempt.
        clear_pending_cloud_unregister_record(data_dir, pending)?;
        after_first_durable_step()?;
        clear_staged_cloud_registration_record(data_dir, pending)?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn remove_discharged_legacy_recovery_module(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
) {
    let Some(origin) = record.legacy_prejournal_origin.as_ref() else {
        return;
    };
    if origin == &record.module {
        return;
    }
    if let Err(error) = remove_cloud_unregister_recovery_module(data_dir, origin) {
        tracing::warn!(
            "Cloud Files retained-state transition completed but its discharged legacy provider could not be removed: {error}"
        );
    }
}

#[cfg(any(windows, test))]
fn finish_already_applied_retained_completion(
    data_dir: &SinomoDataDir,
    pending: &PendingCloudUnregisterRecord,
    staged: &StagedCloudRegistrationRecord,
) -> Result<bool, String> {
    if !is_already_applied_retained_completion(pending, staged) {
        return Ok(false);
    }
    // Crash after fsyncing the typed Retained transition but before clearing
    // the pending intent. RetainedAbsent is the durable completion proof.
    advance_host_cursor_recovery_preserved_successor(data_dir, pending, staged)?;
    clear_pending_cloud_unregister_record(data_dir, pending)?;
    remove_discharged_legacy_recovery_module(data_dir, pending);
    Ok(true)
}

#[cfg(any(windows, test))]
fn is_already_applied_retained_completion(
    pending: &PendingCloudUnregisterRecord,
    staged: &StagedCloudRegistrationRecord,
) -> bool {
    let legacy_successor = staged.recovery.legacy_prejournal_origin.is_none()
        && pending.legacy_prejournal_origin.is_some();
    let cursor_successor = match (
        pending.request.cursor_recovery.as_ref(),
        staged.recovery.request.cursor_recovery.as_ref(),
    ) {
        (Some(current), Some(next)) => {
            current.recovery_id == next.recovery_id
                && current.recovery_token == next.recovery_token
                && current
                    .recovery_cycle
                    .checked_add(1)
                    .is_some_and(|cycle| cycle == next.recovery_cycle)
        }
        _ => false,
    };
    if staged.state != StagedCloudRegistrationState::RetainedAbsent
        || (!legacy_successor && !cursor_successor)
        || !same_cloud_registration_base_identity(&staged.recovery, pending)
        || staged.recovery.module != pending.module
        || pending.operation
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
    {
        return false;
    }
    true
}

#[cfg(any(windows, test))]
fn advance_host_cursor_recovery_preserved_successor(
    data_dir: &SinomoDataDir,
    pending: &PendingCloudUnregisterRecord,
    retained: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    let Some(mut marker) = marker_snapshot.value else {
        if pending.request.cursor_recovery.is_some() {
            return Err(
                "cursor-recovery retained successor lost its durable host authorization"
                    .to_string(),
            );
        }
        return Ok(());
    };
    let pending_context = pending.request.cursor_recovery.as_ref().ok_or_else(|| {
        "host cursor-recovery preserve successor lacks its pending cycle context".to_string()
    })?;
    let retained_context = retained
        .recovery
        .request
        .cursor_recovery
        .as_ref()
        .ok_or_else(|| {
            "host cursor-recovery retained successor lacks its native cycle context".to_string()
        })?;
    let exact_context_identity = pending_context.recovery_id == marker.recovery_id
        && pending_context.recovery_token == marker.inner_token
        && retained_context.recovery_id == marker.recovery_id
        && retained_context.recovery_token == marker.inner_token
        && pending_context
            .recovery_cycle
            .checked_add(1)
            .is_some_and(|cycle| cycle == retained_context.recovery_cycle);
    if retained.state != StagedCloudRegistrationState::RetainedAbsent
        || retained.recovery.module != marker.module
        || retained.recovery.request.sync_root != marker.identity.sync_root
        || retained.recovery.request.root_path != marker.identity.root_path
        || retained.recovery.request.data_dir_root.as_ref()
            != Some(&marker.identity.data_dir_root)
        || retained.recovery.request.provider_name != marker.identity.provider_name
        || !same_cloud_registration_base_identity(&retained.recovery, pending)
        || pending.operation
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        || !exact_context_identity
    {
        return Err(
            "RetainedAbsent cursor-recovery successor does not match the exact host authorization"
                .to_string(),
        );
    }
    let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    if bundle_snapshot.temporary {
        return Err(
            "cursor-recovery preserve successor found an unpromoted provider bundle".to_string(),
        );
    }
    let bundle = bundle_snapshot.value.as_ref().ok_or_else(|| {
        "cursor-recovery preserve successor lost its provider authorization bundle".to_string()
    })?;
    validate_host_cursor_recovery_bundle_link(&marker, bundle)?;
    let fingerprint = cloud_files_preserved_artifacts_fingerprint(data_dir)?;
    if marker.phase == HostCursorRecoveryPhase::PreservedRetained {
        if marker.recovery_cycle != retained_context.recovery_cycle
            || pending_context
                .recovery_cycle
                .checked_add(1)
                .is_none_or(|cycle| cycle != marker.recovery_cycle)
            || marker.native_successor_fingerprint != retained_context.native_successor_fingerprint
            || marker.preserved_artifacts_fingerprint.as_ref() != Some(&fingerprint)
        {
            return Err(
                "preserved cursor-recovery successor changed before pending cleanup".to_string(),
            );
        }
        if marker_snapshot.temporary {
            promote_host_cursor_recovery_marker(data_dir, &marker)?;
        }
        return Ok(());
    }
    if marker_snapshot.temporary {
        return Err(
            "cursor-recovery preserve found a non-PreservedRetained host successor temp"
                .to_string(),
        );
    }
    if !matches!(
        marker.phase,
        HostCursorRecoveryPhase::ArmedAwaitingReady
            | HostCursorRecoveryPhase::ReadyObserved
            | HostCursorRecoveryPhase::ActiveObserved
    ) {
        return Err(format!(
            "cursor recovery {} cannot enter PreservedRetained from {:?}",
            marker.recovery_id, marker.phase
        ));
    }
    if pending_context.recovery_cycle != marker.recovery_cycle
        || retained_context.recovery_cycle
            != marker
                .recovery_cycle
                .checked_add(1)
                .ok_or_else(|| "cursor-recovery preserve cycle overflowed".to_string())?
        || retained_context.native_successor_fingerprint.is_none()
    {
        return Err(
            "Retained cursor-recovery response is not the exact native cycle successor".to_string(),
        );
    }
    marker.phase = HostCursorRecoveryPhase::PreservedRetained;
    marker.ready_artifacts_fingerprint = None;
    marker.preserved_artifacts_fingerprint = Some(fingerprint);
    marker.active_module_marker_fingerprint = None;
    marker.completion_module_marker_fingerprint = None;
    marker.completion_registration_artifacts_fingerprint = None;
    marker.completed_artifacts_fingerprint = None;
    marker.native_successor_fingerprint = retained_context.native_successor_fingerprint.clone();
    marker.recovery_cycle = marker
        .recovery_cycle
        .checked_add(1)
        .ok_or_else(|| "cursor-recovery preserve cycle overflowed".to_string())?;
    persist_host_cursor_recovery_marker(data_dir, &marker)
}

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Eq, PartialEq)]
enum CloudUnregisterCompletion {
    Released(sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterResponse),
    StatePreserved(sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse),
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
impl CloudFilesModuleProvenance {
    fn from_loader_state(state: &sinomo_runtime::module::NativeDylibModuleLoaderState) -> Self {
        Self {
            module_id: state.module_id.clone(),
            content_hash: state.content_hash.clone(),
            cache_key: state.cache_key.clone(),
            library_name: state.library_name.clone(),
            platform_triple: state.platform_triple.clone(),
            native_abi_version: state.native_abi_version,
            interface_version: state.interface_version.clone(),
        }
    }

    fn from_cache_entry(entry: &sinomo_runtime::module::NativeDylibModuleCacheEntry) -> Self {
        Self {
            module_id: entry.module_id.clone(),
            content_hash: entry.content_hash.clone(),
            cache_key: entry.cache_key.clone(),
            library_name: entry.library_name.clone(),
            platform_triple: entry.platform_triple.clone(),
            native_abi_version: entry.native_abi_version,
            interface_version: entry.interface_version.clone(),
        }
    }

    #[cfg(any(windows, test))]
    fn matches_cache_entry(
        &self,
        entry: &sinomo_runtime::module::NativeDylibModuleCacheEntry,
    ) -> bool {
        self == &Self::from_cache_entry(entry)
    }
}

#[cfg(any(windows, test))]
fn hash_host_cursor_recovery_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(any(windows, test))]
fn cloud_files_provenance_fingerprint(
    provenance: &CloudFilesModuleProvenance,
) -> Result<String, String> {
    let encoded = sinomo_runtime::module_api::encode_sobj(provenance)
        .map_err(|error| format!("encode Cloud Files provider provenance: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.provider-provenance.v1\0");
    hash_host_cursor_recovery_field(&mut hasher, &encoded);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn disabled_mount_config_fingerprint(config: &SifsMountConfig) -> Result<String, String> {
    if config.enabled {
        return Err("cursor recovery requires a disabled mount configuration".to_string());
    }
    let encoded = sinomo_runtime::module_api::encode_sobj(config)
        .map_err(|error| format!("encode disabled mount configuration: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.disabled-config.v1\0");
    hash_host_cursor_recovery_field(&mut hasher, &encoded);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_outer_token(
    config: &SifsMountConfig,
    provenance: &CloudFilesModuleProvenance,
    host_lock_bytes: &[u8],
    identity: &sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity,
    inspection: &sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection,
) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CLOUD_FILES_CURSOR_RECOVERY_HOST_TOKEN_DOMAIN.as_bytes());
    hasher.update(&[0]);
    for bytes in [
        sinomo_runtime::module_api::encode_sobj(config)
            .map_err(|error| format!("encode token config: {error}"))?,
        sinomo_runtime::module_api::encode_sobj(provenance)
            .map_err(|error| format!("encode token provenance: {error}"))?,
        sinomo_runtime::module_api::encode_sobj(identity)
            .map_err(|error| format!("encode token identity: {error}"))?,
        sinomo_runtime::module_api::encode_sobj(inspection)
            .map_err(|error| format!("encode token inspection: {error}"))?,
    ] {
        hash_host_cursor_recovery_field(&mut hasher, &bytes);
    }
    hash_host_cursor_recovery_field(&mut hasher, inspection.recovery_token.as_bytes());
    hash_host_cursor_recovery_field(&mut hasher, host_lock_bytes);
    // Domain-bind the exact negative host topology observed while the
    // pre-existing mount lock is held. Fresh inspection has already proven
    // each path physically absent with symlink_metadata.
    for absent in [
        PENDING_CLOUD_UNREGISTER_FILE,
        CLOUD_FILES_CURSOR_RECOVERY_PENDING_TEMP_FILE,
        STAGED_CLOUD_REGISTRATION_FILE,
        CLOUD_FILES_CURSOR_RECOVERY_STAGED_TEMP_FILE,
        CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_FILE,
        ".cloud-files-cursor-recovery.sobj.cursor-recovery-host-next",
        CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_FILE,
        ".cloud-files-cursor-recovery-provider.sobj.cursor-recovery-provider-next",
        CLOUD_FILES_REGISTRATION_RECEIPT_FILE,
        CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
        CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        CLOUD_UNREGISTER_RECOVERY_DIR,
    ] {
        hash_host_cursor_recovery_field(&mut hasher, absent.as_bytes());
        hash_host_cursor_recovery_field(&mut hasher, b"physically-absent");
    }
    // Bind the exact directory-enumeration predicate proved by
    // require_fresh_host_cursor_recovery_absence. A future predicate change is
    // therefore a token-domain change rather than an implicit weakening.
    let forbidden_dfs_crash_patterns = [
        ".cloud-files-registration.staged.sobj.",
        ".cloud-files-unregister.pending.sobj.",
        ".cloud-files-cursor-recovery",
    ];
    hash_host_cursor_recovery_field(&mut hasher, b"no-unclassified-dfs-crash-evidence.v1");
    hash_host_cursor_recovery_field(
        &mut hasher,
        &(forbidden_dfs_crash_patterns.len() as u64).to_le_bytes(),
    );
    for pattern in forbidden_dfs_crash_patterns {
        hash_host_cursor_recovery_field(&mut hasher, pattern.as_bytes());
    }
    Ok(format!("v1:blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn deterministic_host_cursor_recovery_id(outer_token: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-only-recovery.host-id.v1\0");
    hash_host_cursor_recovery_field(&mut hasher, outer_token.as_bytes());
    format!("cursor-recovery-{}", hasher.finalize().to_hex())
}

#[cfg(any(windows, test))]
fn cloud_unregister_recovery_variant_dir(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<PathBuf, String> {
    let hash_suffix = provenance
        .content_hash
        .strip_prefix("blake3:")
        .filter(|suffix| suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            format!(
                "Cloud Files cleanup provider has invalid blake3 content hash {}",
                provenance.content_hash
            )
        })?;
    if provenance.library_name.is_empty()
        || provenance.library_name == "."
        || provenance.library_name == ".."
        || provenance.library_name.contains(['/', '\\', ':'])
    {
        return Err(format!(
            "Cloud Files cleanup provider has unsafe library name {}",
            provenance.library_name
        ));
    }
    Ok(data_dir
        .dfs_dir()
        .join(CLOUD_UNREGISTER_RECOVERY_DIR)
        .join(hash_suffix))
}

#[cfg(any(windows, test))]
fn verify_cloud_unregister_recovery_module(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<PathBuf, String> {
    let variant_dir = cloud_unregister_recovery_variant_dir(data_dir, provenance)?;
    let library_path = variant_dir.join(&provenance.library_name);
    let bytes = std::fs::read(&library_path).map_err(|error| {
        format!(
            "read staged Cloud Files cleanup provider {}: {error}",
            library_path.display()
        )
    })?;
    let actual_hash = sinomo_runtime::module::native_module_content_hash(&bytes);
    if actual_hash != provenance.content_hash {
        return Err(format!(
            "staged Cloud Files cleanup provider {} hash mismatch: expected {}, got {}",
            library_path.display(),
            provenance.content_hash,
            actual_hash
        ));
    }
    let provenance_path = variant_dir.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE);
    let persisted: CloudFilesModuleProvenance = sinomo_runtime::module_api::decode_sobj(
        &std::fs::read(&provenance_path).map_err(|error| {
            format!(
                "read staged Cloud Files cleanup provenance {}: {error}",
                provenance_path.display()
            )
        })?,
    )
    .map_err(|error| {
        format!(
            "decode staged Cloud Files cleanup provenance {}: {error}",
            provenance_path.display()
        )
    })?;
    if persisted != *provenance {
        return Err(format!(
            "staged Cloud Files cleanup provenance {} does not match the provider bytes",
            provenance_path.display()
        ));
    }
    Ok(library_path)
}

#[cfg(any(windows, test))]
fn stage_cloud_unregister_recovery_module(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
    source_path: &Path,
) -> Result<PathBuf, String> {
    if let Ok(path) = verify_cloud_unregister_recovery_module(data_dir, provenance) {
        return Ok(path);
    }
    let variant_dir = cloud_unregister_recovery_variant_dir(data_dir, provenance)?;
    let library_path = variant_dir.join(&provenance.library_name);
    if let Ok(staged_bytes) = std::fs::read(&library_path)
        && sinomo_runtime::module::native_module_content_hash(&staged_bytes)
            == provenance.content_hash
    {
        let provenance_bytes = sinomo_runtime::module_api::encode_sobj(provenance)
            .map_err(|error| format!("encode Cloud Files cleanup provider provenance: {error}"))?;
        persist_recovery_file_if_absent(
            &variant_dir.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE),
            &provenance_bytes,
            "Cloud Files cleanup provider provenance",
        )?;
        sync_pending_cloud_unregister_directory(&variant_dir)?;
        return verify_cloud_unregister_recovery_module(data_dir, provenance);
    }
    let source_bytes = std::fs::read(source_path).map_err(|error| {
        format!(
            "read Cloud Files cleanup provider source {}: {error}",
            source_path.display()
        )
    })?;
    let source_hash = sinomo_runtime::module::native_module_content_hash(&source_bytes);
    if source_hash != provenance.content_hash {
        return Err(format!(
            "Cloud Files cleanup provider source {} hash mismatch: expected {}, got {}",
            source_path.display(),
            provenance.content_hash,
            source_hash
        ));
    }
    std::fs::create_dir_all(&variant_dir).map_err(|error| {
        format!(
            "create Cloud Files unregister recovery directory {}: {error}",
            variant_dir.display()
        )
    })?;
    persist_recovery_file_if_absent(&library_path, &source_bytes, "Cloud Files cleanup provider")?;
    let provenance_bytes = sinomo_runtime::module_api::encode_sobj(provenance)
        .map_err(|error| format!("encode Cloud Files cleanup provider provenance: {error}"))?;
    persist_recovery_file_if_absent(
        &variant_dir.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE),
        &provenance_bytes,
        "Cloud Files cleanup provider provenance",
    )?;
    sync_pending_cloud_unregister_directory(&variant_dir)?;
    verify_cloud_unregister_recovery_module(data_dir, provenance)
}

#[cfg(any(windows, test))]
fn cursor_recovery_pin_temp_path(path: &Path) -> Result<PathBuf, String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("cursor-recovery pin path {} has no parent", path.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "cursor-recovery pin path {} has no file name",
                path.display()
            )
        })?;
    Ok(parent.join(format!(".{name}.cursor-recovery-pin-next")))
}

#[cfg(any(windows, test))]
fn persist_exact_cursor_recovery_pin_file(
    path: &Path,
    expected: &[u8],
    maximum_bytes: u64,
    label: &str,
) -> Result<(), String> {
    if expected.len() as u64 > maximum_bytes {
        return Err(format!("{label} exceeds its {maximum_bytes}-byte bound"));
    }
    let temporary = cursor_recovery_pin_temp_path(path)?;
    let current = read_bounded_plain_host_file(path, maximum_bytes, label)?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        maximum_bytes,
        &format!("{label} temporary file"),
    )?;
    if current.as_deref().is_some_and(|bytes| bytes != expected)
        || staged.as_deref().is_some_and(|bytes| bytes != expected)
    {
        return Err(format!(
            "cursor-recovery pin {} contains changed or unrecognized bytes",
            path.display()
        ));
    }
    match (current.is_some(), staged.is_some()) {
        (true, false) => return Ok(()),
        (true, true) => {
            std::fs::remove_file(&temporary).map_err(|error| {
                format!(
                    "clear exact replayed {label} temporary file {}: {error}",
                    temporary.display()
                )
            })?;
            if let Some(parent) = path.parent() {
                sync_pending_cloud_unregister_directory(parent)?;
            }
            return Ok(());
        }
        (false, true) => {
            sinomo_runtime::storage::durable_file::replace_file(&temporary, path)
                .map_err(|error| format!("promote exact {label} {}: {error}", path.display()))?;
            if let Some(parent) = path.parent() {
                sync_pending_cloud_unregister_directory(parent)?;
            }
            return Ok(());
        }
        (false, false) => {}
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            format!(
                "create exact {label} temporary file {}: {error}",
                temporary.display()
            )
        })?;
    file.write_all(expected)
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            format!(
                "persist exact {label} temporary file {}: {error}",
                temporary.display()
            )
        })?;
    if let Some(parent) = path.parent() {
        sync_pending_cloud_unregister_directory(parent)?;
    }
    sinomo_runtime::storage::durable_file::replace_file(&temporary, path)
        .map_err(|error| format!("commit exact {label} {}: {error}", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_pending_cloud_unregister_directory(parent)?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn validate_cursor_recovery_pin_topology(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
    allow_temps: bool,
) -> Result<(), String> {
    let recovery_root = data_dir.dfs_dir().join(CLOUD_UNREGISTER_RECOVERY_DIR);
    ensure_plain_existing_host_path(&recovery_root, true, "cursor-recovery provider root")?;
    let variant = cloud_unregister_recovery_variant_dir(data_dir, provenance)?;
    let expected_variant = variant
        .file_name()
        .ok_or_else(|| "cursor-recovery provider variant has no name".to_string())?;
    let mut root_entries = std::fs::read_dir(&recovery_root)
        .map_err(|error| format!("enumerate cursor-recovery provider root: {error}"))?;
    let only = root_entries
        .next()
        .transpose()
        .map_err(|error| format!("enumerate cursor-recovery provider root: {error}"))?
        .ok_or_else(|| "cursor-recovery provider root is empty".to_string())?;
    if only.file_name() != expected_variant || root_entries.next().is_some() {
        return Err(
            "cursor-recovery provider root contains another provider or unknown topology"
                .to_string(),
        );
    }
    ensure_plain_existing_host_path(&variant, true, "cursor-recovery provider variant")?;
    let library_temp = cursor_recovery_pin_temp_path(&variant.join(&provenance.library_name))?;
    let provenance_temp =
        cursor_recovery_pin_temp_path(&variant.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE))?;
    let mut allowed = vec![
        provenance.library_name.clone(),
        CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE.to_string(),
    ];
    if allow_temps {
        allowed.push(
            library_temp
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| "cursor-recovery provider library temp name is invalid".to_string())?
                .to_string(),
        );
        allowed.push(
            provenance_temp
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    "cursor-recovery provider provenance temp name is invalid".to_string()
                })?
                .to_string(),
        );
    }
    let mut observed = Vec::new();
    for entry in std::fs::read_dir(&variant)
        .map_err(|error| format!("enumerate cursor-recovery provider variant: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read provider variant entry: {error}"))?;
        ensure_plain_existing_host_path(&entry.path(), false, "cursor-recovery provider entry")?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "cursor-recovery provider entry name is not UTF-8".to_string())?;
        if !allowed.contains(&name) {
            return Err(format!(
                "cursor-recovery provider variant contains unknown entry {name}"
            ));
        }
        observed.push(name);
    }
    observed.sort();
    observed.dedup();
    if !allow_temps {
        let mut expected = vec![
            provenance.library_name.clone(),
            CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE.to_string(),
        ];
        expected.sort();
        if observed != expected {
            return Err("cursor-recovery provider variant is incomplete".to_string());
        }
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn materialize_cursor_recovery_provider_bundle(
    data_dir: &SinomoDataDir,
    bundle: &HostCursorRecoveryProviderBundle,
) -> Result<PathBuf, String> {
    validate_host_cursor_recovery_marker(&bundle.requested)?;
    if sinomo_runtime::module::native_module_content_hash(&bundle.provider_bytes)
        != bundle.requested.module.content_hash
    {
        return Err("cursor-recovery provider bundle bytes changed".to_string());
    }
    let recovery_root = data_dir.dfs_dir().join(CLOUD_UNREGISTER_RECOVERY_DIR);
    if let Err(error) = std::fs::symlink_metadata(&recovery_root) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(format!("inspect cursor-recovery provider root: {error}"));
        }
        std::fs::create_dir(&recovery_root)
            .map_err(|error| format!("create cursor-recovery provider root: {error}"))?;
        sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
    } else {
        ensure_plain_existing_host_path(&recovery_root, true, "cursor-recovery provider root")?;
    }
    let variant = cloud_unregister_recovery_variant_dir(data_dir, &bundle.requested.module)?;
    if let Err(error) = std::fs::symlink_metadata(&variant) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(format!("inspect cursor-recovery provider variant: {error}"));
        }
        if std::fs::read_dir(&recovery_root)
            .map_err(|error| format!("enumerate cursor-recovery provider root: {error}"))?
            .next()
            .is_some()
        {
            return Err(
                "cursor-recovery provider root already contains another variant".to_string(),
            );
        }
        std::fs::create_dir(&variant)
            .map_err(|error| format!("create cursor-recovery provider variant: {error}"))?;
        sync_pending_cloud_unregister_directory(&recovery_root)?;
    } else {
        validate_cursor_recovery_pin_topology(data_dir, &bundle.requested.module, true)?;
    }
    let provenance_bytes = sinomo_runtime::module_api::encode_sobj(&bundle.requested.module)
        .map_err(|error| format!("encode cursor-recovery provider provenance: {error}"))?;
    persist_exact_cursor_recovery_pin_file(
        &variant.join(&bundle.requested.module.library_name),
        &bundle.provider_bytes,
        CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES,
        "cursor-recovery provider library",
    )?;
    persist_exact_cursor_recovery_pin_file(
        &variant.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE),
        &provenance_bytes,
        64 * 1024,
        "cursor-recovery provider provenance",
    )?;
    validate_cursor_recovery_pin_topology(data_dir, &bundle.requested.module, false)?;
    verify_cloud_unregister_recovery_module(data_dir, &bundle.requested.module)
}

#[cfg(any(windows, test))]
fn validate_cursor_recovery_execution_provider(
    bundle: &HostCursorRecoveryProviderBundle,
) -> Result<(), String> {
    match (
        bundle.execution_module.as_ref(),
        bundle.execution_provider_bytes.as_ref(),
    ) {
        (None, None) => Ok(()),
        (Some(provenance), Some(bytes)) => {
            sinomo_runtime::module::validate_native_dylib_module_cache_key(&provenance.cache_key)
                .map_err(|error| format!("invalid cursor-recovery execution cache key: {error}"))?;
            sinomo_runtime::module::validate_native_dylib_module_library_name(
                &provenance.library_name,
            )
            .map_err(|error| format!("invalid cursor-recovery execution library name: {error}"))?;
            if provenance == &bundle.requested.module
                || provenance.module_id != bundle.requested.module.module_id
                || provenance.platform_triple != bundle.requested.module.platform_triple
                || provenance.native_abi_version != bundle.requested.module.native_abi_version
                || provenance.interface_version
                    != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                || bytes.is_empty()
                || bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES
                || sinomo_runtime::module::native_module_content_hash(bytes)
                    != provenance.content_hash
            {
                return Err(
                    "cursor-recovery execution provider does not exactly bind a distinct compatible v3 provider"
                        .to_string(),
                );
            }
            Ok(())
        }
        _ => Err(
            "cursor-recovery execution provider provenance and bytes must be present together"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn materialize_cursor_recovery_execution_provider(
    data_dir: &SinomoDataDir,
    bundle: &HostCursorRecoveryProviderBundle,
) -> Result<Option<(CloudFilesModuleProvenance, PathBuf)>, String> {
    validate_cursor_recovery_execution_provider(bundle)?;
    let Some(provenance) = bundle.execution_module.as_ref() else {
        return Ok(None);
    };
    let bytes = bundle
        .execution_provider_bytes
        .as_deref()
        .expect("validated execution provider bytes");
    let path = sinomo_runtime::module::native_module_cache_path(
        data_dir.root(),
        &provenance.cache_key,
        &provenance.content_hash,
        &provenance.library_name,
    );
    let parent = path
        .parent()
        .ok_or_else(|| "cursor-recovery execution cache path has no parent".to_string())?;
    let mut directory = data_dir.root();
    for component in ["module", "cache", "native-dylib"] {
        directory.push(component);
        ensure_plain_cursor_recovery_directory(&directory)?;
    }
    for component in std::path::Path::new(&provenance.cache_key).components() {
        let std::path::Component::Normal(component) = component else {
            return Err("cursor-recovery execution cache key escaped its cache root".to_string());
        };
        directory.push(component);
        ensure_plain_cursor_recovery_directory(&directory)?;
    }
    let hash_directory = provenance.content_hash.replace(':', "_");
    directory.push(hash_directory);
    ensure_plain_cursor_recovery_directory(&directory)?;
    if directory != parent {
        return Err("cursor-recovery execution cache path was not canonical".to_string());
    }
    persist_exact_cursor_recovery_pin_file(
        &path,
        bytes,
        CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES,
        "cursor-recovery execution provider",
    )?;
    let actual = std::fs::read(&path).map_err(|error| {
        format!(
            "read cursor-recovery execution provider {}: {error}",
            path.display()
        )
    })?;
    if sinomo_runtime::module::native_module_content_hash(&actual) != provenance.content_hash {
        return Err("materialized cursor-recovery execution provider hash changed".to_string());
    }
    Ok(Some((provenance.clone(), path)))
}

#[cfg(any(windows, test))]
fn ensure_plain_cursor_recovery_directory(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            ensure_plain_existing_host_path(path, true, "cursor-recovery execution cache directory")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(path).map_err(|error| {
                format!(
                    "create cursor-recovery execution cache directory {}: {error}",
                    path.display()
                )
            })?;
            if let Some(parent) = path.parent() {
                sync_pending_cloud_unregister_directory(parent)?;
            }
            Ok(())
        }
        Err(error) => Err(format!(
            "inspect cursor-recovery execution cache directory {}: {error}",
            path.display()
        )),
    }
}

#[cfg(any(windows, test))]
fn persist_recovery_file_if_absent(
    path: &Path,
    bytes: &[u8],
    description: &str,
) -> Result<(), String> {
    if path.exists() {
        let existing = std::fs::read(path)
            .map_err(|error| format!("read existing {description} {}: {error}", path.display()))?;
        return if existing == bytes {
            Ok(())
        } else {
            Err(format!(
                "refusing to overwrite changed {description} {}",
                path.display()
            ))
        };
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("{description} path {} has no parent", path.display()))?;
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("recovery"),
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                format!(
                    "create {description} temporary file {}: {error}",
                    temporary.display()
                )
            })?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| {
                format!(
                    "persist {description} temporary file {}: {error}",
                    temporary.display()
                )
            })?;
        std::fs::rename(&temporary, path)
            .map_err(|error| format!("commit {description} {}: {error}", path.display()))?;
        sync_pending_cloud_unregister_directory(parent)
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

#[cfg(any(windows, test))]
fn ensure_cloud_unregister_recovery_module(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<PathBuf, String> {
    if let Ok(path) = verify_cloud_unregister_recovery_module(data_dir, provenance) {
        return Ok(path);
    }
    let entries = sinomo_runtime::module::read_native_dylib_module_cache_entries(&data_dir.root())
        .map_err(|error| format!("read native module cache for Cloud Files cleanup: {error}"))?;
    let source = entries
        .iter()
        .find(|entry| provenance.matches_cache_entry(entry))
        .map(|entry| entry.library_path.as_path())
        .ok_or_else(|| {
            format!(
                "Cloud Files cleanup provider {} ({}, interface {}) is absent from both durable recovery storage and the content-addressed module cache",
                provenance.module_id, provenance.content_hash, provenance.interface_version
            )
        })?;
    stage_cloud_unregister_recovery_module(data_dir, provenance, source)
}

#[cfg(any(windows, test))]
fn remove_cloud_unregister_recovery_module(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<(), String> {
    let variant_dir = cloud_unregister_recovery_variant_dir(data_dir, provenance)?;
    for path in [
        variant_dir.join(&provenance.library_name),
        variant_dir.join(CLOUD_UNREGISTER_RECOVERY_PROVENANCE_FILE),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "remove completed Cloud Files cleanup recovery file {}: {error}",
                    path.display()
                ));
            }
        }
    }
    match std::fs::remove_dir(&variant_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "remove completed Cloud Files cleanup recovery directory {}: {error}",
                variant_dir.display()
            ));
        }
    }
    if let Some(parent) = variant_dir.parent() {
        sync_pending_cloud_unregister_directory(parent)?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn pending_cloud_unregister_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join(PENDING_CLOUD_UNREGISTER_FILE)
}

#[cfg(any(windows, test))]
fn staged_cloud_registration_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join(STAGED_CLOUD_REGISTRATION_FILE)
}

#[cfg(any(windows, test))]
fn cursor_recovery_staged_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join(CLOUD_FILES_CURSOR_RECOVERY_STAGED_TEMP_FILE)
}

#[cfg(any(windows, test))]
fn cursor_recovery_pending_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join(CLOUD_FILES_CURSOR_RECOVERY_PENDING_TEMP_FILE)
}

#[cfg(any(windows, test))]
fn require_cursor_recovery_pending_intent_absent(
    data_dir: &SinomoDataDir,
    boundary: &str,
) -> Result<(), String> {
    require_physically_absent(
        &pending_cloud_unregister_path(data_dir),
        &format!("pending Cloud Files unregister at {boundary}"),
    )?;
    require_physically_absent(
        &cursor_recovery_pending_temp_path(data_dir),
        &format!("pending Cloud Files cursor-recovery temporary intent at {boundary}"),
    )
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_marker_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join(CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_FILE)
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_marker_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join(format!(
        ".{CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_FILE}.cursor-recovery-host-next"
    ))
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_provider_bundle_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join(CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_FILE)
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_provider_bundle_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join(format!(
        ".{CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_FILE}.cursor-recovery-provider-next"
    ))
}

#[cfg(any(windows, test))]
fn cloud_files_module_runtime_path(data_dir: &SinomoDataDir, file_name: &str) -> PathBuf {
    data_dir
        .module_runtime_dir()
        .join(sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(file_name)
}

#[cfg(any(windows, test))]
fn cloud_files_cursor_recovery_module_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cloud_files_module_runtime_path(
        data_dir,
        ".cursor-recovery-control.sobj.cursor-recovery-marker-next",
    )
}

#[cfg(any(windows, test))]
fn cloud_files_registration_journal_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cloud_files_module_runtime_path(
        data_dir,
        ".registration-journal.sobj.registration-journal-next",
    )
}

#[cfg(any(windows, test))]
fn cloud_files_registration_receipt_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cloud_files_module_runtime_path(data_dir, ".registration.sobj.registration-receipt-next")
}

#[cfg(any(windows, test))]
fn require_physically_absent(path: &Path, label: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
        Ok(_) => Err(format!(
            "{label} {} must be physically absent (including dangling links and reparse points)",
            path.display()
        )),
    }
}

#[cfg(any(windows, test))]
fn require_fresh_host_cursor_recovery_absence(data_dir: &SinomoDataDir) -> Result<(), String> {
    for (path, label) in [
        (
            pending_cloud_unregister_path(data_dir),
            "pending Cloud Files unregister",
        ),
        (
            data_dir
                .dfs_dir()
                .join(CLOUD_FILES_CURSOR_RECOVERY_PENDING_TEMP_FILE),
            "pending Cloud Files cursor-recovery temporary intent",
        ),
        (
            staged_cloud_registration_path(data_dir),
            "staged Cloud Files ownership",
        ),
        (
            host_cursor_recovery_marker_path(data_dir),
            "host cursor-recovery marker",
        ),
        (
            host_cursor_recovery_marker_temp_path(data_dir),
            "host cursor-recovery temporary marker",
        ),
        (
            host_cursor_recovery_provider_bundle_path(data_dir),
            "host cursor-recovery provider bundle",
        ),
        (
            host_cursor_recovery_provider_bundle_temp_path(data_dir),
            "host cursor-recovery provider temporary bundle",
        ),
        (
            data_dir.dfs_dir().join(CLOUD_UNREGISTER_RECOVERY_DIR),
            "Cloud Files unregister recovery tree",
        ),
        (
            cloud_files_registration_receipt_path(data_dir),
            "Cloud Files registration receipt",
        ),
        (
            cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            "Cloud Files registration journal",
        ),
        (
            cloud_files_module_runtime_path(
                data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ),
            "module cursor-recovery marker",
        ),
    ] {
        require_physically_absent(&path, label)?;
    }
    let dfs_dir = data_dir.dfs_dir();
    let entries = std::fs::read_dir(&dfs_dir)
        .map_err(|error| format!("enumerate DFS directory {}: {error}", dfs_dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read DFS directory entry: {error}"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "DFS directory contains a non-UTF-8 entry".to_string())?;
        let staged_prefix = format!(".{STAGED_CLOUD_REGISTRATION_FILE}.");
        let pending_prefix = format!(".{PENDING_CLOUD_UNREGISTER_FILE}.");
        if name == CLOUD_FILES_CURSOR_RECOVERY_STAGED_TEMP_FILE
            || name.starts_with(&staged_prefix)
            || name.starts_with(&pending_prefix)
            || name.starts_with(".cloud-files-cursor-recovery")
        {
            return Err(format!(
                "fresh cursor recovery found unclassified DFS crash evidence `{name}`"
            ));
        }
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn read_bounded_plain_host_file(
    path: &Path,
    maximum_bytes: u64,
    label: &str,
) -> Result<Option<Vec<u8>>, String> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect {label} {}: {error}", path.display())),
    };
    ensure_plain_existing_host_path(path, false, label)?;
    if metadata.len() > maximum_bytes {
        return Err(format!(
            "{label} {} is {} bytes; maximum is {maximum_bytes}",
            path.display(),
            metadata.len()
        ));
    }
    let mut file = OpenOptions::new()
        .create(false)
        .read(true)
        .open(path)
        .map_err(|error| format!("open {label} {}: {error}", path.display()))?;
    let handle_length = file
        .metadata()
        .map_err(|error| format!("inspect open {label} {}: {error}", path.display()))?
        .len();
    if handle_length != metadata.len() || handle_length > maximum_bytes {
        return Err(format!(
            "{label} {} changed while it was inspected",
            path.display()
        ));
    }
    let mut bytes = Vec::with_capacity(handle_length as usize);
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    if bytes.len() as u64 != handle_length {
        return Err(format!(
            "{label} {} changed while it was read",
            path.display()
        ));
    }
    Ok(Some(bytes))
}

#[cfg(any(windows, test))]
fn decode_host_cursor_recovery_provider_bundle(
    path: &Path,
    bytes: &[u8],
) -> Result<HostCursorRecoveryProviderBundle, String> {
    if bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES {
        return Err(format!(
            "host cursor-recovery provider bundle {} exceeds {} bytes",
            path.display(),
            CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES
        ));
    }
    let bundle: HostCursorRecoveryProviderBundle = sinomo_runtime::module_api::decode_sobj(bytes)
        .map_err(|error| {
        format!(
            "decode host cursor-recovery provider bundle {}: {error}",
            path.display()
        )
    })?;
    let canonical = sinomo_runtime::module_api::encode_sobj(&bundle)
        .map_err(|error| format!("re-encode cursor-recovery provider bundle: {error}"))?;
    if canonical != bytes {
        return Err(format!(
            "host cursor-recovery provider bundle {} is not canonical",
            path.display()
        ));
    }
    validate_host_cursor_recovery_marker(&bundle.requested)?;
    if bundle.schema_version != CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION
        || bundle.requested.phase != HostCursorRecoveryPhase::Requested
        || bundle.requested.applied.is_some()
        || bundle.provider_bytes.is_empty()
        || bundle.provider_bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES
        || sinomo_runtime::module::native_module_content_hash(&bundle.provider_bytes)
            != bundle.requested.module.content_hash
    {
        return Err(
            "host cursor-recovery provider bundle does not exactly bind its Requested authorization and provider bytes"
                .to_string(),
        );
    }
    validate_cursor_recovery_execution_provider(&bundle)?;
    Ok(bundle)
}

#[cfg(any(windows, test))]
fn is_cursor_recovery_execution_bundle_successor(
    current: &HostCursorRecoveryProviderBundle,
    next: &HostCursorRecoveryProviderBundle,
) -> bool {
    current.schema_version == next.schema_version
        && current.requested == next.requested
        && current.provider_bytes == next.provider_bytes
        && next.execution_module.is_some()
        && next.execution_provider_bytes.is_some()
        && (current.execution_module != next.execution_module
            || current.execution_provider_bytes != next.execution_provider_bytes)
        && validate_cursor_recovery_execution_provider(next).is_ok()
}

#[cfg(any(windows, test))]
fn inspect_host_cursor_recovery_provider_bundle_for_resume(
    data_dir: &SinomoDataDir,
) -> Result<HostCursorRecoveryArtifactSnapshot<HostCursorRecoveryProviderBundle>, String> {
    let path = host_cursor_recovery_provider_bundle_path(data_dir);
    let temporary = host_cursor_recovery_provider_bundle_temp_path(data_dir);
    let current = read_bounded_plain_host_file(
        &path,
        CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES,
        "host cursor-recovery provider bundle",
    )?
    .map(|bytes| decode_host_cursor_recovery_provider_bundle(&path, &bytes))
    .transpose()?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES,
        "host cursor-recovery provider temporary bundle",
    )?
    .map(|bytes| decode_host_cursor_recovery_provider_bundle(&temporary, &bytes))
    .transpose()?;
    match (current, staged) {
        (None, None) => Ok(HostCursorRecoveryArtifactSnapshot {
            value: None,
            temporary: false,
        }),
        (Some(current), None) => Ok(HostCursorRecoveryArtifactSnapshot {
            value: Some(current),
            temporary: false,
        }),
        (None, Some(staged)) => Ok(HostCursorRecoveryArtifactSnapshot {
            value: Some(staged),
            temporary: true,
        }),
        (Some(current), Some(staged))
            if current == staged
                || is_cursor_recovery_execution_bundle_successor(&current, &staged) =>
        {
            Ok(HostCursorRecoveryArtifactSnapshot {
                value: Some(staged),
                temporary: true,
            })
        }
        (Some(_), Some(_)) => Err(
            "final and temporary cursor-recovery provider bundles are not one exact execution-provider successor"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn promote_host_cursor_recovery_provider_bundle(
    data_dir: &SinomoDataDir,
    expected: &HostCursorRecoveryProviderBundle,
) -> Result<(), String> {
    let snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    if snapshot.value.as_ref() != Some(expected) {
        return Err("cursor-recovery provider bundle changed before promotion".to_string());
    }
    if snapshot.temporary {
        sinomo_runtime::storage::durable_file::replace_file(
            &host_cursor_recovery_provider_bundle_temp_path(data_dir),
            &host_cursor_recovery_provider_bundle_path(data_dir),
        )
        .map_err(|error| format!("promote cursor-recovery provider bundle: {error}"))?;
        sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
    }

    Ok(())
}

#[cfg(any(windows, test))]
fn persist_host_cursor_recovery_provider_bundle(
    data_dir: &SinomoDataDir,
    bundle: &HostCursorRecoveryProviderBundle,
) -> Result<(), String> {
    validate_host_cursor_recovery_marker(&bundle.requested)?;
    validate_cursor_recovery_execution_provider(bundle)?;
    let snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    if let Some(existing) = snapshot.value {
        return if existing == *bundle {
            promote_host_cursor_recovery_provider_bundle(data_dir, bundle)
        } else if !snapshot.temporary
            && is_cursor_recovery_execution_bundle_successor(&existing, bundle)
        {
            let bytes = sinomo_runtime::module_api::encode_sobj(bundle)
                .map_err(|error| format!("encode execution-provider bundle successor: {error}"))?;
            let temporary = host_cursor_recovery_provider_bundle_temp_path(data_dir);
            let expected = decode_host_cursor_recovery_provider_bundle(&temporary, &bytes)?;
            if expected != *bundle {
                return Err(
                    "execution-provider bundle successor failed canonical round-trip".to_string(),
                );
            }
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| {
                    format!(
                        "create execution-provider bundle successor {}: {error}",
                        temporary.display()
                    )
                })?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| {
                    format!(
                        "persist execution-provider bundle successor {}: {error}",
                        temporary.display()
                    )
                })?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
            promote_host_cursor_recovery_provider_bundle(data_dir, bundle)
        } else {
            Err(
                "durable cursor-recovery provider bundle belongs to another authorization"
                    .to_string(),
            )
        };
    }
    let bytes = sinomo_runtime::module_api::encode_sobj(bundle)
        .map_err(|error| format!("encode cursor-recovery provider bundle: {error}"))?;
    if bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES {
        return Err(format!(
            "cursor-recovery provider bundle is {} bytes; maximum is {}",
            bytes.len(),
            CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_BUNDLE_MAX_BYTES
        ));
    }
    // Decode our exact durable image before making it the first host effect, so
    // eligibility can never succeed for an authorization that cannot replay.
    let temporary = host_cursor_recovery_provider_bundle_temp_path(data_dir);
    let expected = decode_host_cursor_recovery_provider_bundle(&temporary, &bytes)?;
    if expected != *bundle {
        return Err("cursor-recovery provider bundle failed canonical round-trip".to_string());
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            format!(
                "create cursor-recovery provider temporary bundle {}: {error}",
                temporary.display()
            )
        })?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            format!(
                "persist cursor-recovery provider temporary bundle {}: {error}",
                temporary.display()
            )
        })?;
    sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
    promote_host_cursor_recovery_provider_bundle(data_dir, bundle)?;
    Ok(())
}

#[cfg(any(windows, test))]
fn decode_host_cursor_recovery_marker(
    path: &Path,
    bytes: &[u8],
) -> Result<HostCursorRecoveryMarker, String> {
    if bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES {
        return Err(format!(
            "host cursor-recovery marker {} exceeds {} bytes",
            path.display(),
            CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES
        ));
    }
    let marker: HostCursorRecoveryMarker =
        sinomo_runtime::module_api::decode_sobj(bytes).map_err(|error| {
            format!(
                "decode host cursor-recovery marker {}: {error}",
                path.display()
            )
        })?;
    let canonical = sinomo_runtime::module_api::encode_sobj(&marker)
        .map_err(|error| format!("re-encode host cursor-recovery marker: {error}"))?;
    if canonical != bytes {
        return Err(format!(
            "host cursor-recovery marker {} is not canonical",
            path.display()
        ));
    }
    validate_host_cursor_recovery_marker(&marker)?;
    Ok(marker)
}

#[cfg(any(windows, test))]
fn load_host_cursor_recovery_marker_strict(
    data_dir: &SinomoDataDir,
) -> Result<Option<HostCursorRecoveryMarker>, String> {
    let path = host_cursor_recovery_marker_path(data_dir);
    require_physically_absent(
        &host_cursor_recovery_marker_temp_path(data_dir),
        "host cursor-recovery temporary marker",
    )?;
    let Some(bytes) = read_bounded_plain_host_file(
        &path,
        CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES,
        "host cursor-recovery marker",
    )?
    else {
        return Ok(None);
    };
    decode_host_cursor_recovery_marker(&path, &bytes).map(Some)
}

#[cfg(any(windows, test))]
fn inspect_host_cursor_recovery_marker_for_resume(
    data_dir: &SinomoDataDir,
) -> Result<HostCursorRecoveryArtifactSnapshot<HostCursorRecoveryMarker>, String> {
    let path = host_cursor_recovery_marker_path(data_dir);
    let temporary = host_cursor_recovery_marker_temp_path(data_dir);
    let current = read_bounded_plain_host_file(
        &path,
        CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES,
        "host cursor-recovery marker",
    )?
    .map(|bytes| decode_host_cursor_recovery_marker(&path, &bytes))
    .transpose()?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES,
        "host cursor-recovery temporary marker",
    )?
    .map(|bytes| decode_host_cursor_recovery_marker(&temporary, &bytes))
    .transpose()?;
    let Some(staged) = staged else {
        return Ok(HostCursorRecoveryArtifactSnapshot {
            value: current,
            temporary: false,
        });
    };
    if let Some(current) = current.as_ref() {
        validate_host_cursor_recovery_transition(current, &staged)?;
    } else if staged.phase != HostCursorRecoveryPhase::Requested || staged.applied.is_some() {
        return Err(
            "initial host cursor-recovery temporary marker is not the exact Requested authorization"
                .to_string(),
        );
    }
    Ok(HostCursorRecoveryArtifactSnapshot {
        value: Some(staged),
        temporary: true,
    })
}

#[cfg(any(windows, test))]
fn promote_host_cursor_recovery_marker(
    data_dir: &SinomoDataDir,
    expected: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    let snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    if snapshot.value.as_ref() != Some(expected) {
        return Err("host cursor-recovery marker changed before promotion".to_string());
    }
    if snapshot.temporary {
        sinomo_runtime::storage::durable_file::replace_file(
            &host_cursor_recovery_marker_temp_path(data_dir),
            &host_cursor_recovery_marker_path(data_dir),
        )
        .map_err(|error| format!("promote exact host cursor-recovery marker: {error}"))?;
        sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn persist_host_cursor_recovery_marker(
    data_dir: &SinomoDataDir,
    marker: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    let snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    if let Some(current) = snapshot.value.as_ref() {
        if snapshot.temporary {
            if current != marker {
                return Err(
                    "host cursor-recovery marker retry does not match the exact durable temporary successor"
                        .to_string(),
                );
            }
            return promote_host_cursor_recovery_marker(data_dir, marker);
        }
        if current == marker {
            return Ok(());
        }
        validate_host_cursor_recovery_transition(current, marker)?;
    } else {
        if marker.phase != HostCursorRecoveryPhase::Requested || marker.applied.is_some() {
            return Err(
                "initial host cursor-recovery marker must be the exact Requested authorization"
                    .to_string(),
            );
        }
    }
    stage_host_cursor_recovery_marker(data_dir, marker)?;
    commit_staged_host_cursor_recovery_marker(data_dir)
}

#[cfg(any(windows, test))]
fn stage_host_cursor_recovery_marker(
    data_dir: &SinomoDataDir,
    marker: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    validate_host_cursor_recovery_marker(marker)?;
    let bytes = sinomo_runtime::module_api::encode_sobj(marker)
        .map_err(|error| format!("encode host cursor-recovery marker: {error}"))?;
    if bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES {
        return Err(format!(
            "host cursor-recovery marker is {} bytes; maximum is {}",
            bytes.len(),
            CLOUD_FILES_CURSOR_RECOVERY_HOST_MARKER_MAX_BYTES
        ));
    }
    let temporary = host_cursor_recovery_marker_temp_path(data_dir);
    match std::fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "clear stale cursor-recovery marker staging path {}: {error}",
                temporary.display()
            ));
        }
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            format!(
                "create host cursor-recovery temporary marker {}: {error}",
                temporary.display()
            )
        })?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            format!(
                "persist host cursor-recovery temporary marker {}: {error}",
                temporary.display()
            )
        })?;
    sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
}

#[cfg(any(windows, test))]
fn commit_staged_host_cursor_recovery_marker(data_dir: &SinomoDataDir) -> Result<(), String> {
    let path = host_cursor_recovery_marker_path(data_dir);
    let temporary = host_cursor_recovery_marker_temp_path(data_dir);
    sinomo_runtime::storage::durable_file::replace_file(&temporary, &path).map_err(|error| {
        format!(
            "commit host cursor-recovery marker {}: {error}",
            path.display()
        )
    })?;
    sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
}

#[cfg(any(windows, test))]
fn host_cursor_recovery_phase_ordinal(phase: HostCursorRecoveryPhase) -> u8 {
    match phase {
        HostCursorRecoveryPhase::Requested => 0,
        HostCursorRecoveryPhase::ModuleApplied => 1,
        HostCursorRecoveryPhase::AdoptedRetained => 2,
        HostCursorRecoveryPhase::ArmedAwaitingReady => 3,
        HostCursorRecoveryPhase::ReadyObserved => 4,
        HostCursorRecoveryPhase::ActiveObserved => 5,
        HostCursorRecoveryPhase::AuthorityFinalized => 6,
        HostCursorRecoveryPhase::PreservedRetained => 7,
    }
}

#[cfg(any(windows, test))]
fn same_host_cursor_recovery_identity(
    left: &HostCursorRecoveryMarker,
    right: &HostCursorRecoveryMarker,
) -> bool {
    left.schema_version == right.schema_version
        && left.disabled_config == right.disabled_config
        && left.module == right.module
        && left.identity == right.identity
        && left.outer_token == right.outer_token
        && left.inner_token == right.inner_token
        && left.recovery_id == right.recovery_id
        && left.inspection == right.inspection
}

#[cfg(any(windows, test))]
fn same_host_cursor_recovery_callback_authority(
    left: &HostCursorRecoveryMarker,
    right: &HostCursorRecoveryMarker,
) -> bool {
    same_host_cursor_recovery_identity(left, right)
        && left.recovery_cycle == right.recovery_cycle
        && left.native_successor_fingerprint == right.native_successor_fingerprint
}

#[cfg(any(windows, test))]
fn validate_host_cursor_recovery_transition(
    current: &HostCursorRecoveryMarker,
    next: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    let phase_transition = if current.phase == next.phase {
        current.recovery_cycle == next.recovery_cycle
    } else {
        match (current.phase, next.phase) {
            (HostCursorRecoveryPhase::Requested, HostCursorRecoveryPhase::ModuleApplied)
            | (HostCursorRecoveryPhase::ModuleApplied, HostCursorRecoveryPhase::AdoptedRetained)
            | (
                HostCursorRecoveryPhase::AdoptedRetained,
                HostCursorRecoveryPhase::ArmedAwaitingReady,
            )
            | (
                HostCursorRecoveryPhase::ArmedAwaitingReady,
                HostCursorRecoveryPhase::ReadyObserved,
            )
            | (HostCursorRecoveryPhase::ReadyObserved, HostCursorRecoveryPhase::ActiveObserved)
            | (
                HostCursorRecoveryPhase::ActiveObserved,
                HostCursorRecoveryPhase::AuthorityFinalized,
            )
            | (
                HostCursorRecoveryPhase::PreservedRetained,
                HostCursorRecoveryPhase::ArmedAwaitingReady,
            ) => current.recovery_cycle == next.recovery_cycle,
            (
                HostCursorRecoveryPhase::ArmedAwaitingReady
                | HostCursorRecoveryPhase::ReadyObserved
                | HostCursorRecoveryPhase::ActiveObserved,
                HostCursorRecoveryPhase::PreservedRetained,
            ) => current
                .recovery_cycle
                .checked_add(1)
                .is_some_and(|cycle| cycle == next.recovery_cycle),
            _ => false,
        }
    };
    if !same_host_cursor_recovery_identity(current, next) || !phase_transition {
        return Err(
            "host cursor-recovery temporary marker is not an exact same-or-next phase transition"
                .to_string(),
        );
    }
    let successor_proof_transition = if next.phase == HostCursorRecoveryPhase::PreservedRetained
        && current.phase != HostCursorRecoveryPhase::PreservedRetained
    {
        next.native_successor_fingerprint
            .as_deref()
            .is_some_and(valid_canonical_blake3_fingerprint)
    } else {
        current.native_successor_fingerprint == next.native_successor_fingerprint
    };
    if !successor_proof_transition {
        return Err(
            "host cursor-recovery transition changed its native cycle commitment".to_string(),
        );
    }
    let artifact_transition = if current.phase == next.phase {
        current.ready_artifacts_fingerprint == next.ready_artifacts_fingerprint
            && current.preserved_artifacts_fingerprint == next.preserved_artifacts_fingerprint
            && current.active_module_marker_fingerprint == next.active_module_marker_fingerprint
            && current.completion_module_marker_fingerprint
                == next.completion_module_marker_fingerprint
            && current.completion_registration_artifacts_fingerprint
                == next.completion_registration_artifacts_fingerprint
            && (current.completed_artifacts_fingerprint == next.completed_artifacts_fingerprint
                || (current.phase == HostCursorRecoveryPhase::AuthorityFinalized
                    && current.completed_artifacts_fingerprint.is_none()
                    && next
                        .completed_artifacts_fingerprint
                        .as_deref()
                        .is_some_and(valid_blake3_fingerprint)))
    } else {
        match (current.phase, next.phase) {
            (
                HostCursorRecoveryPhase::ArmedAwaitingReady,
                HostCursorRecoveryPhase::ReadyObserved,
            ) => {
                current.ready_artifacts_fingerprint.is_none()
                    && next.ready_artifacts_fingerprint.is_some()
                    && (current.preserved_artifacts_fingerprint.is_none()
                        || current.recovery_cycle > 0)
                    && next.preserved_artifacts_fingerprint.is_none()
                    && current.active_module_marker_fingerprint.is_none()
                    && next.active_module_marker_fingerprint.is_none()
                    && current.completion_module_marker_fingerprint.is_none()
                    && next.completion_module_marker_fingerprint.is_none()
                    && current
                        .completion_registration_artifacts_fingerprint
                        .is_none()
                    && next.completion_registration_artifacts_fingerprint.is_none()
                    && current.completed_artifacts_fingerprint.is_none()
                    && next.completed_artifacts_fingerprint.is_none()
            }
            (HostCursorRecoveryPhase::ReadyObserved, HostCursorRecoveryPhase::ActiveObserved) => {
                current.ready_artifacts_fingerprint == next.ready_artifacts_fingerprint
                    && current.preserved_artifacts_fingerprint.is_none()
                    && next.preserved_artifacts_fingerprint.is_none()
                    && current.active_module_marker_fingerprint.is_none()
                    && next.active_module_marker_fingerprint.is_some()
                    && current.completion_module_marker_fingerprint.is_none()
                    && next.completion_module_marker_fingerprint.is_none()
                    && current
                        .completion_registration_artifacts_fingerprint
                        .is_none()
                    && next.completion_registration_artifacts_fingerprint.is_none()
                    && current.completed_artifacts_fingerprint.is_none()
                    && next.completed_artifacts_fingerprint.is_none()
            }
            (
                HostCursorRecoveryPhase::ActiveObserved,
                HostCursorRecoveryPhase::AuthorityFinalized,
            ) => {
                current.ready_artifacts_fingerprint == next.ready_artifacts_fingerprint
                    && current.preserved_artifacts_fingerprint.is_none()
                    && next.preserved_artifacts_fingerprint.is_none()
                    && current.active_module_marker_fingerprint
                        == next.active_module_marker_fingerprint
                    && current.completion_module_marker_fingerprint.is_none()
                    && next.completion_module_marker_fingerprint.is_some()
                    && current
                        .completion_registration_artifacts_fingerprint
                        .is_none()
                    && next
                        .completion_registration_artifacts_fingerprint
                        .as_deref()
                        .is_some_and(valid_blake3_fingerprint)
                    && current.completed_artifacts_fingerprint.is_none()
                    && next.completed_artifacts_fingerprint.is_none()
            }
            (
                HostCursorRecoveryPhase::ArmedAwaitingReady
                | HostCursorRecoveryPhase::ReadyObserved
                | HostCursorRecoveryPhase::ActiveObserved,
                HostCursorRecoveryPhase::PreservedRetained,
            ) => {
                next.ready_artifacts_fingerprint.is_none()
                    && current.preserved_artifacts_fingerprint.is_none()
                    && next.preserved_artifacts_fingerprint.is_some()
                    && next.active_module_marker_fingerprint.is_none()
                    && next.completion_module_marker_fingerprint.is_none()
                    && next.completion_registration_artifacts_fingerprint.is_none()
                    && next.completed_artifacts_fingerprint.is_none()
            }
            (
                HostCursorRecoveryPhase::PreservedRetained,
                HostCursorRecoveryPhase::ArmedAwaitingReady,
            ) => {
                next.ready_artifacts_fingerprint.is_none()
                    && current.preserved_artifacts_fingerprint.is_some()
                    && next.preserved_artifacts_fingerprint
                        == current.preserved_artifacts_fingerprint
                    && current.active_module_marker_fingerprint.is_none()
                    && next.active_module_marker_fingerprint.is_none()
                    && current.completion_module_marker_fingerprint.is_none()
                    && next.completion_module_marker_fingerprint.is_none()
                    && current
                        .completion_registration_artifacts_fingerprint
                        .is_none()
                    && next.completion_registration_artifacts_fingerprint.is_none()
                    && current.completed_artifacts_fingerprint.is_none()
                    && next.completed_artifacts_fingerprint.is_none()
            }
            _ => {
                current.ready_artifacts_fingerprint == next.ready_artifacts_fingerprint
                    && current.preserved_artifacts_fingerprint
                        == next.preserved_artifacts_fingerprint
                    && current.active_module_marker_fingerprint
                        == next.active_module_marker_fingerprint
                    && current.completion_module_marker_fingerprint
                        == next.completion_module_marker_fingerprint
                    && current.completion_registration_artifacts_fingerprint
                        == next.completion_registration_artifacts_fingerprint
                    && current.completed_artifacts_fingerprint
                        == next.completed_artifacts_fingerprint
            }
        }
    };
    if !artifact_transition {
        return Err(
            "host cursor-recovery transition changed or discarded its phase-bound artifact fingerprint"
                .to_string(),
        );
    }
    match (&current.applied, &next.applied, current.phase, next.phase) {
        (None, None, HostCursorRecoveryPhase::Requested, HostCursorRecoveryPhase::Requested)
        | (
            None,
            Some(_),
            HostCursorRecoveryPhase::Requested,
            HostCursorRecoveryPhase::ModuleApplied,
        ) => Ok(()),
        (Some(current), Some(next), _, _) if current == next => Ok(()),
        _ => Err(
            "host cursor-recovery transition changed or discarded its exact module result"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn validate_host_cursor_recovery_marker(marker: &HostCursorRecoveryMarker) -> Result<(), String> {
    let expected_mount_point =
        lexical_absolute_path(&expand_mount_point(&marker.disabled_config.mount_point)?)?;
    if marker.schema_version != CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION
        || marker.disabled_config.enabled
        || !valid_v1_blake3_token(&marker.outer_token)
        || !valid_v1_blake3_token(&marker.inner_token)
        || !valid_blake3_fingerprint(&marker.inspection.raw_state_fingerprint)
        || marker.inner_token != marker.inspection.recovery_token
        || marker.recovery_id.trim().is_empty()
        || marker.identity.enabled
        || marker.identity.sync_root != expected_mount_point
        || marker.identity.root_path != marker.disabled_config.root_path
        || marker.identity.data_dir_root.as_os_str().is_empty()
        || marker.identity.provider_name
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
        || marker.identity.provider_version != env!("CARGO_PKG_VERSION")
        || marker.identity.read_only != marker.disabled_config.read_only
        || marker.identity.allow_other != marker.disabled_config.allow_other
        || marker.identity.sync_local_changes != marker.disabled_config.sync_local_changes
        || marker.identity.prepopulate_depth != 0
        || !marker.identity.connect
        || marker.identity.max_run_seconds.is_some()
        || marker.identity.disabled_config_fingerprint
            != disabled_mount_config_fingerprint(&marker.disabled_config)?
        || marker.identity.sync_root != marker.inspection.sync_root
        || marker.identity.root_path != marker.inspection.root_path
        || marker.identity.provider_provenance_fingerprint
            != cloud_files_provenance_fingerprint(&marker.module)?
        || matches!(
            marker.phase,
            HostCursorRecoveryPhase::Requested
                | HostCursorRecoveryPhase::ModuleApplied
                | HostCursorRecoveryPhase::AdoptedRetained
        ) && marker.recovery_cycle != 0
        || (marker.recovery_cycle == 0 && marker.native_successor_fingerprint.is_some())
        || (marker.recovery_cycle > 0
            && marker
                .native_successor_fingerprint
                .as_deref()
                .is_none_or(|value| !valid_canonical_blake3_fingerprint(value)))
        || match marker.phase {
            HostCursorRecoveryPhase::Requested
            | HostCursorRecoveryPhase::ModuleApplied
            | HostCursorRecoveryPhase::AdoptedRetained => {
                marker.ready_artifacts_fingerprint.is_some()
                    || marker.preserved_artifacts_fingerprint.is_some()
                    || marker.active_module_marker_fingerprint.is_some()
                    || marker.completion_module_marker_fingerprint.is_some()
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .is_some()
                    || marker.completed_artifacts_fingerprint.is_some()
            }
            HostCursorRecoveryPhase::ArmedAwaitingReady => {
                marker.ready_artifacts_fingerprint.is_some()
                    || if marker.recovery_cycle == 0 {
                        marker.preserved_artifacts_fingerprint.is_some()
                    } else {
                        marker
                            .preserved_artifacts_fingerprint
                            .as_deref()
                            .is_none_or(|value| !valid_blake3_fingerprint(value))
                    }
                    || marker.active_module_marker_fingerprint.is_some()
                    || marker.completion_module_marker_fingerprint.is_some()
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .is_some()
                    || marker.completed_artifacts_fingerprint.is_some()
            }
            HostCursorRecoveryPhase::ReadyObserved => {
                marker
                    .ready_artifacts_fingerprint
                    .as_deref()
                    .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker.preserved_artifacts_fingerprint.is_some()
                    || marker.active_module_marker_fingerprint.is_some()
                    || marker.completion_module_marker_fingerprint.is_some()
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .is_some()
                    || marker.completed_artifacts_fingerprint.is_some()
            }
            HostCursorRecoveryPhase::ActiveObserved => {
                marker
                    .ready_artifacts_fingerprint
                    .as_deref()
                    .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker.preserved_artifacts_fingerprint.is_some()
                    || marker
                        .active_module_marker_fingerprint
                        .as_deref()
                        .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker.completion_module_marker_fingerprint.is_some()
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .is_some()
                    || marker.completed_artifacts_fingerprint.is_some()
            }
            HostCursorRecoveryPhase::AuthorityFinalized => {
                marker
                    .ready_artifacts_fingerprint
                    .as_deref()
                    .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker.preserved_artifacts_fingerprint.is_some()
                    || marker
                        .active_module_marker_fingerprint
                        .as_deref()
                        .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker
                        .completion_module_marker_fingerprint
                        .as_deref()
                        .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .as_deref()
                        .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker
                        .completed_artifacts_fingerprint
                        .as_deref()
                        .is_some_and(|value| !valid_blake3_fingerprint(value))
            }
            HostCursorRecoveryPhase::PreservedRetained => {
                marker.ready_artifacts_fingerprint.is_some()
                    || marker
                        .preserved_artifacts_fingerprint
                        .as_deref()
                        .is_none_or(|value| !valid_blake3_fingerprint(value))
                    || marker.active_module_marker_fingerprint.is_some()
                    || marker.completion_module_marker_fingerprint.is_some()
                    || marker
                        .completion_registration_artifacts_fingerprint
                        .is_some()
                    || marker.completed_artifacts_fingerprint.is_some()
            }
        }
    {
        return Err("host cursor-recovery marker invariants do not match".to_string());
    }
    if matches!(
        marker.phase,
        HostCursorRecoveryPhase::ModuleApplied
            | HostCursorRecoveryPhase::AdoptedRetained
            | HostCursorRecoveryPhase::ArmedAwaitingReady
            | HostCursorRecoveryPhase::ReadyObserved
            | HostCursorRecoveryPhase::ActiveObserved
            | HostCursorRecoveryPhase::AuthorityFinalized
            | HostCursorRecoveryPhase::PreservedRetained
    ) && marker.applied.as_ref().is_none_or(|applied| {
        applied.recovery_id != marker.recovery_id
            || applied.recovery_token != marker.inner_token
            || applied.sync_root != marker.identity.sync_root
            || applied.root_path != marker.identity.root_path
            || applied.sync_root_file_id != marker.inspection.sync_root_file_id
            || applied.cursor_count_cleared != marker.inspection.cursor_count
            || applied.schema_version
                != sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
    }) {
        return Err(
            "host cursor-recovery applied result does not match its durable authorization"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn requested_host_cursor_recovery_marker(
    marker: &HostCursorRecoveryMarker,
) -> HostCursorRecoveryMarker {
    let mut requested = marker.clone();
    requested.phase = HostCursorRecoveryPhase::Requested;
    requested.applied = None;
    requested.ready_artifacts_fingerprint = None;
    requested.preserved_artifacts_fingerprint = None;
    requested.active_module_marker_fingerprint = None;
    requested.completion_module_marker_fingerprint = None;
    requested.completion_registration_artifacts_fingerprint = None;
    requested.completed_artifacts_fingerprint = None;
    requested.recovery_cycle = 0;
    requested.native_successor_fingerprint = None;
    requested
}

#[cfg(any(windows, test))]
fn validate_host_cursor_recovery_bundle_link(
    marker: &HostCursorRecoveryMarker,
    bundle: &HostCursorRecoveryProviderBundle,
) -> Result<(), String> {
    if bundle.requested != requested_host_cursor_recovery_marker(marker) {
        return Err(
            "cursor-recovery provider bundle does not bind the exact immutable host authorization"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn validate_current_host_cursor_recovery_authority(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    host_lock_bytes: &[u8],
    expected_token: &str,
    marker: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    validate_host_cursor_recovery_marker(marker)?;
    if marker.outer_token != expected_token
        || marker.disabled_config != *config
        || marker.identity.data_dir_root != data_dir.root()
        || marker.recovery_id != deterministic_host_cursor_recovery_id(expected_token)
    {
        return Err(
            "durable cursor recovery belongs to another token, data directory, or config"
                .to_string(),
        );
    }
    let recomputed = host_cursor_recovery_outer_token(
        &marker.disabled_config,
        &marker.module,
        host_lock_bytes,
        &marker.identity,
        &marker.inspection,
    )?;
    if recomputed != marker.outer_token {
        return Err(
            "current host lock bytes no longer match the authorized recovery token".to_string(),
        );
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn valid_blake3_fingerprint(value: &str) -> bool {
    value
        .strip_prefix("blake3:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[cfg(any(windows, test))]
fn valid_canonical_blake3_fingerprint(value: &str) -> bool {
    value.strip_prefix("blake3:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[cfg(any(windows, test))]
fn valid_v1_blake3_token(value: &str) -> bool {
    value
        .strip_prefix("v1:blake3:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[cfg(any(windows, test))]
fn validate_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    validate_cloud_unregister_record(data_dir, record, false)
}

#[cfg(any(windows, test))]
fn validate_cloud_unregister_record(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
    allow_policy_neutral_legacy_origin: bool,
) -> Result<(), String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleRequest, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
        SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
    };
    use sinomo_runtime::module::ModuleId;

    if record.schema_version != PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION {
        return Err(format!(
            "unsupported pending Cloud Files unregister schema {}; expected {}",
            record.schema_version, PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION
        ));
    }
    if !matches!(
        record.operation.as_str(),
        SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
            | SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
    ) {
        return Err(format!(
            "pending Cloud Files unregister operation mismatch: expected {}, got {}",
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION, record.operation
        ));
    }
    if record.legacy_prejournal_origin.is_some()
        && !allow_policy_neutral_legacy_origin
        && record.operation != SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
    {
        return Err(
            "pending Cloud Files unregister with legacy pre-journal origin must use the distinct v3 preserve-state operation"
                .to_string(),
        );
    }
    let expected_module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    if record.module.module_id != expected_module_id {
        return Err(format!(
            "pending Cloud Files unregister module mismatch: expected {}, got {}",
            expected_module_id, record.module.module_id
        ));
    }
    if record.request.data_dir_root.as_ref() != Some(&data_dir.root()) {
        return Err(format!(
            "pending Cloud Files unregister belongs to data directory {}, not {}",
            record.request.data_dir_root.as_deref().map_or_else(
                || "<missing>".to_string(),
                |path| path.display().to_string()
            ),
            data_dir.root().display()
        ));
    }
    let payload =
        sinomo_runtime::module_api::decode_sobj::<CloudFilesModuleRequest>(&record.payload_sobj)
            .map_err(|error| {
                format!("decode durable Cloud Files unregister descriptor: {error}")
            })?;
    match (record.operation.as_str(), payload) {
        (SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION, CloudFilesModuleRequest::Unregister(request))
            if request == record.request => Ok(()),
        (
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            CloudFilesModuleRequest::PreserveStateUnregister(request),
        ) if request == record.request
            && record.module.interface_version == SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION => Ok(()),
        _ => Err(
            "durable Cloud Files unregister operation, payload, provider capability, or recorded identity does not match"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn validate_legacy_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleRequest, SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
        SINOMO_DFS_CLOUD_FILES_MODULE_ID, SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
    };
    use sinomo_runtime::module::ModuleId;

    let expected_module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    if record.schema_version != 1
        || record.operation != SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
        || record.module.module_id != expected_module_id
        || record.module.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
        || record.request.data_dir_root.as_ref() != Some(&data_dir.root())
        || record.legacy_prejournal_origin.is_some()
    {
        return Err(
            "legacy staged Cloud Files descriptor has invalid schema, operation, provider, or data-directory provenance"
                .to_string(),
        );
    }
    match sinomo_runtime::module_api::decode_sobj::<CloudFilesModuleRequest>(&record.payload_sobj)
        .map_err(|error| format!("decode legacy staged Cloud Files payload: {error}"))?
    {
        CloudFilesModuleRequest::Unregister(request) if request == record.request => {}
        _ => {
            return Err(
                "legacy staged Cloud Files operation and payload identity do not match".to_string(),
            );
        }
    }
    verify_cloud_unregister_recovery_module(data_dir, &record.module).map_err(|error| {
        format!("legacy staged Cloud Files provider is not self-contained: {error}")
    })?;
    Ok(())
}

#[cfg(any(windows, test))]
fn inspect_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
) -> Result<PendingCloudUnregisterArtifactSnapshot, String> {
    let path = pending_cloud_unregister_path(data_dir);
    let temporary = cursor_recovery_pending_temp_path(data_dir);
    let current =
        read_bounded_plain_host_file(&path, 1024 * 1024, "pending Cloud Files unregister")?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        1024 * 1024,
        "pending Cloud Files cursor-recovery temporary intent",
    )?;
    let (bytes, current_present, temporary_present) = match (current, staged) {
        (None, None) => {
            return Ok(PendingCloudUnregisterArtifactSnapshot {
                value: None,
                current_present: false,
                temporary_present: false,
            });
        }
        (Some(current), None) => (current, true, false),
        (None, Some(staged)) => (staged, false, true),
        (Some(current), Some(staged)) if current == staged => (current, true, true),
        (Some(_), Some(_)) => {
            return Err(
                "pending Cloud Files current/temporary cursor-recovery evidence is ambiguous"
                    .to_string(),
            );
        }
    };
    let record: PendingCloudUnregisterRecord = sinomo_runtime::module_api::decode_sobj(&bytes)
        .map_err(|error| {
            format!(
                "decode pending Cloud Files unregister {}: {error}",
                path.display()
            )
        })?;
    if record.schema_version != PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION {
        return Err(format!(
            "legacy pending Cloud Files unregister schema {} is policy-ambiguous and will not be replayed automatically",
            record.schema_version
        ));
    }
    validate_pending_cloud_unregister_record(data_dir, &record)?;
    if sinomo_runtime::module_api::encode_sobj(&record)
        .map_err(|error| format!("encode canonical pending Cloud Files unregister: {error}"))?
        != bytes
    {
        return Err("pending Cloud Files unregister is noncanonical".to_string());
    }
    if temporary_present && record.request.cursor_recovery.is_none() {
        return Err(
            "ordinary pending intent cannot own the cursor-recovery temporary path".to_string(),
        );
    }
    Ok(PendingCloudUnregisterArtifactSnapshot {
        value: Some(record),
        current_present,
        temporary_present,
    })
}

#[cfg(any(windows, test))]
fn load_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
) -> Result<Option<PendingCloudUnregisterRecord>, String> {
    // This loader is deliberately read-only. A cursor-recovery temporary file
    // is the durable disable linearization point, but it may be promoted only
    // after the exact host marker, provider bundle, staged ownership, config,
    // and pre-existing lease have been authenticated together.
    Ok(inspect_pending_cloud_unregister_record(data_dir)?.value)
}

#[cfg(any(windows, test))]
fn promote_cursor_recovery_pending_intent(
    data_dir: &SinomoDataDir,
    expected: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    if expected.request.cursor_recovery.is_none() {
        return Err("ordinary pending intent cannot use cursor-recovery promotion".to_string());
    }
    let snapshot = inspect_pending_cloud_unregister_record(data_dir)?;
    if snapshot.value.as_ref() != Some(expected) {
        return Err(
            "cursor-recovery pending intent changed before authority-bound promotion".to_string(),
        );
    }
    let path = pending_cloud_unregister_path(data_dir);
    let temporary = cursor_recovery_pending_temp_path(data_dir);
    match (snapshot.current_present, snapshot.temporary_present) {
        (true, false) => Ok(()),
        (false, true) => {
            sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                .map_err(|error| format!("promote cursor-recovery pending intent: {error}"))?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
        }
        (true, true) => {
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(&temporary)
                .map(|_| ())
                .map_err(|error| {
                    format!("remove redundant cursor-recovery pending intent: {error}")
                })
        }
        (false, false) => {
            Err("cursor-recovery pending intent disappeared before promotion".to_string())
        }
    }
}

#[cfg(any(windows, test))]
fn load_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
) -> Result<Option<StagedCloudRegistrationRecord>, String> {
    let path = staged_cloud_registration_path(data_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "read staged Cloud Files registration recovery {}: {error}",
                path.display()
            ));
        }
    };
    if let Ok(record) =
        sinomo_runtime::module_api::decode_sobj::<StagedCloudRegistrationRecord>(&bytes)
    {
        validate_staged_cloud_registration_record(data_dir, &record)?;
        return Ok(Some(record));
    }
    let legacy = sinomo_runtime::module_api::decode_sobj::<PendingCloudUnregisterRecord>(&bytes)
        .map_err(|error| {
            format!(
                "decode staged Cloud Files registration recovery {}: {error}",
                path.display()
            )
        })?;
    validate_legacy_staged_cloud_registration_record(data_dir, &legacy)?;
    // A legacy staged descriptor is identity/provenance evidence, never an
    // authorization to replay its ambiguous destructive operation.
    let legacy_origin = legacy.module.clone();
    let mut recovery = pending_cloud_unregister_record(legacy.request, legacy.module)?;
    recovery.legacy_prejournal_origin = Some(legacy_origin);
    let migrated = StagedCloudRegistrationRecord {
        schema_version: STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION,
        state: StagedCloudRegistrationState::MayBeRegistered,
        recovery,
    };
    validate_staged_cloud_registration_record(data_dir, &migrated)?;
    Ok(Some(migrated))
}

#[cfg(any(windows, test))]
fn inspect_cursor_recovery_staged_record_snapshot(
    data_dir: &SinomoDataDir,
) -> Result<CursorRecoveryStagedArtifactSnapshot, String> {
    fn decode(
        data_dir: &SinomoDataDir,
        path: &Path,
        bytes: Option<Vec<u8>>,
        label: &str,
    ) -> Result<Option<StagedCloudRegistrationRecord>, String> {
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let record: StagedCloudRegistrationRecord = sinomo_runtime::module_api::decode_sobj(&bytes)
            .map_err(|error| format!("decode {label} {}: {error}", path.display()))?;
        validate_staged_cloud_registration_record(data_dir, &record)?;
        if sinomo_runtime::module_api::encode_sobj(&record)
            .map_err(|error| format!("encode canonical {label}: {error}"))?
            != bytes
        {
            return Err(format!("{label} {} is noncanonical", path.display()));
        }
        Ok(Some(record))
    }

    let path = staged_cloud_registration_path(data_dir);
    let temporary_path = cursor_recovery_staged_temp_path(data_dir);
    let current = decode(
        data_dir,
        &path,
        read_bounded_plain_host_file(&path, 1024 * 1024, "cursor-recovery staged ownership")?,
        "cursor-recovery staged ownership",
    )?;
    let temporary = decode(
        data_dir,
        &temporary_path,
        read_bounded_plain_host_file(
            &temporary_path,
            1024 * 1024,
            "cursor-recovery staged ownership temporary file",
        )?,
        "cursor-recovery staged ownership temporary file",
    )?;
    Ok(CursorRecoveryStagedArtifactSnapshot { current, temporary })
}

#[cfg(any(windows, test))]
fn validate_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
    record: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    if record.schema_version != STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION {
        return Err(format!(
            "unsupported staged Cloud Files registration schema {}; expected {}",
            record.schema_version, STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION
        ));
    }
    validate_cloud_unregister_record(data_dir, &record.recovery, true)?;
    if record.recovery.operation
        != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
    {
        return Err(
            "staged Cloud Files registration recovery must remain cleanup-policy neutral"
                .to_string(),
        );
    }
    if let Some(legacy_provider) = record.recovery.legacy_prejournal_origin.as_ref() {
        if record.state != StagedCloudRegistrationState::MayBeRegistered
            || legacy_provider.interface_version
                != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
            || !matches!(
                record.recovery.module.interface_version.as_str(),
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
                    | sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
            )
        {
            return Err(
                "legacy Cloud Files pre-journal risk must remain MayBeRegistered with original v2 provenance until v3 Ready proof"
                    .to_string(),
            );
        }
        if record.recovery.module.interface_version
            == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
            && &record.recovery.module != legacy_provider
        {
            return Err(
                "legacy Cloud Files pre-journal origin does not exactly match the pre-upgrade v2 recovery provider"
                    .to_string(),
            );
        }
    }
    verify_cloud_unregister_recovery_module(data_dir, &record.recovery.module).map_err(
        |error| format!("staged Cloud Files registration recovery is not self-contained: {error}"),
    )?;
    if let Some(origin) = record.recovery.legacy_prejournal_origin.as_ref() {
        verify_cloud_unregister_recovery_module(data_dir, origin).map_err(|error| {
            format!("legacy Cloud Files pre-journal provider is not self-contained: {error}")
        })?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn persist_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
    record: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    validate_staged_cloud_registration_record(data_dir, record)?;
    let path = staged_cloud_registration_path(data_dir);
    if let Some(existing) = load_staged_cloud_registration_record(data_dir)? {
        if existing == *record {
            return Ok(());
        }
        return Err(format!(
            "refusing generic Cloud Files staged-state transition at {}; use the explicit compare-and-swap lifecycle transition",
            path.display()
        ));
    }
    let bytes = sinomo_runtime::module_api::encode_sobj(record)
        .map_err(|error| format!("encode staged Cloud Files registration recovery: {error}"))?;
    sinomo_runtime::storage::durable_file::write_file_atomic(
        &path,
        &bytes,
        sinomo_runtime::storage::durable_file::DurableAtomicFileOptions::new(
            STAGED_CLOUD_REGISTRATION_FILE,
            "cloud-files-registration-state",
        )
        .sync_all(),
    )
    .map_err(|error| {
        format!(
            "persist staged Cloud Files registration recovery {}: {error}",
            path.display()
        )
    })
}

#[cfg(any(windows, test))]
fn persist_or_promote_cursor_recovery_staged_record(
    data_dir: &SinomoDataDir,
    expected: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    validate_staged_cloud_registration_record(data_dir, expected)?;
    let path = staged_cloud_registration_path(data_dir);
    let temporary = cursor_recovery_staged_temp_path(data_dir);
    let expected_bytes = sinomo_runtime::module_api::encode_sobj(expected)
        .map_err(|error| format!("encode cursor-recovery staged ownership: {error}"))?;
    let current =
        read_bounded_plain_host_file(&path, 1024 * 1024, "cursor-recovery staged ownership")?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        1024 * 1024,
        "cursor-recovery staged ownership temporary file",
    )?;
    match (current.as_deref(), staged.as_deref()) {
        (Some(current), None) if current == expected_bytes => Ok(()),
        (None, Some(staged)) if staged == expected_bytes => {
            sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                .map_err(|error| format!("promote cursor-recovery staged ownership: {error}"))?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
        }
        (Some(current), Some(staged)) if current == expected_bytes && staged == expected_bytes => {
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
                &temporary,
            )
            .map_err(|error| {
                format!("remove redundant cursor-recovery staged temporary file: {error}")
            })?;
            Ok(())
        }
        (None, None) => {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| {
                    format!(
                        "create cursor-recovery staged temporary file {}: {error}",
                        temporary.display()
                    )
                })?;
            file.write_all(&expected_bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| {
                    format!(
                        "persist cursor-recovery staged temporary file {}: {error}",
                        temporary.display()
                    )
                })?;
            drop(file);
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
            sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                .map_err(|error| format!("commit cursor-recovery staged ownership: {error}"))?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
        }
        _ => Err(
            "cursor-recovery staged current/temporary snapshot is changed, torn, or ambiguous"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn transition_or_promote_cursor_recovery_staged_record(
    data_dir: &SinomoDataDir,
    predecessor: &StagedCloudRegistrationRecord,
    expected: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    validate_staged_cloud_registration_record(data_dir, predecessor)?;
    validate_staged_cloud_registration_record(data_dir, expected)?;
    if predecessor == expected {
        return persist_or_promote_cursor_recovery_staged_record(data_dir, expected);
    }
    let path = staged_cloud_registration_path(data_dir);
    let temporary = cursor_recovery_staged_temp_path(data_dir);
    let predecessor_bytes = sinomo_runtime::module_api::encode_sobj(predecessor)
        .map_err(|error| format!("encode cursor-recovery staged predecessor: {error}"))?;
    let expected_bytes = sinomo_runtime::module_api::encode_sobj(expected)
        .map_err(|error| format!("encode cursor-recovery staged successor: {error}"))?;
    let current =
        read_bounded_plain_host_file(&path, 1024 * 1024, "cursor-recovery staged ownership")?;
    let staged = read_bounded_plain_host_file(
        &temporary,
        1024 * 1024,
        "cursor-recovery staged ownership temporary file",
    )?;
    match (current.as_deref(), staged.as_deref()) {
        (Some(current), None) if current == expected_bytes => Ok(()),
        (Some(current), Some(staged))
            if current == expected_bytes && staged == expected_bytes =>
        {
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
                &temporary,
            )
            .map_err(|error| {
                format!("remove redundant cursor-recovery staged successor: {error}")
            })?;
            Ok(())
        }
        (Some(current), Some(staged))
            if current == predecessor_bytes && staged == expected_bytes =>
        {
            sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                .map_err(|error| format!("promote cursor-recovery staged successor: {error}"))?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
        }
        (Some(current), None) if current == predecessor_bytes => {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| {
                    format!(
                        "create cursor-recovery staged successor {}: {error}",
                        temporary.display()
                    )
                })?;
            file.write_all(&expected_bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| {
                    format!(
                        "persist cursor-recovery staged successor {}: {error}",
                        temporary.display()
                    )
                })?;
            drop(file);
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())?;
            sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                .map_err(|error| format!("promote cursor-recovery staged successor: {error}"))?;
            sync_pending_cloud_unregister_directory(&data_dir.dfs_dir())
        }
        _ => Err(
            "cursor-recovery staged successor does not match its exact retained predecessor/current bytes"
                .to_string(),
        ),
    }
}

#[cfg(any(windows, test))]
fn replace_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
    expected: &StagedCloudRegistrationRecord,
    replacement: &StagedCloudRegistrationRecord,
) -> Result<(), String> {
    validate_staged_cloud_registration_record(data_dir, replacement)?;
    let path = staged_cloud_registration_path(data_dir);
    let existing = load_staged_cloud_registration_record(data_dir)?.ok_or_else(|| {
        format!(
            "cannot transition missing staged Cloud Files registration {}",
            path.display()
        )
    })?;
    if existing != *expected {
        return Err(format!(
            "refusing changed staged Cloud Files registration transition {}",
            path.display()
        ));
    }
    let bytes = sinomo_runtime::module_api::encode_sobj(replacement)
        .map_err(|error| format!("encode staged Cloud Files registration transition: {error}"))?;
    sinomo_runtime::storage::durable_file::write_file_atomic(
        &path,
        &bytes,
        sinomo_runtime::storage::durable_file::DurableAtomicFileOptions::new(
            STAGED_CLOUD_REGISTRATION_FILE,
            "cloud-files-registration-transition",
        )
        .sync_all(),
    )
    .map_err(|error| {
        format!(
            "transition staged Cloud Files registration {}: {error}",
            path.display()
        )
    })
}

#[cfg(any(windows, test))]
fn persist_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    validate_pending_cloud_unregister_record(data_dir, record)?;
    verify_cloud_unregister_recovery_module(data_dir, &record.module).map_err(|error| {
        format!(
            "pending Cloud Files unregister is not self-contained in durable recovery storage: {error}"
        )
    })?;
    if let Some(origin) = record.legacy_prejournal_origin.as_ref() {
        verify_cloud_unregister_recovery_module(data_dir, origin).map_err(|error| {
            format!(
                "pending Cloud Files unregister lost its exact legacy pre-journal provider: {error}"
            )
        })?;
    }
    let path = pending_cloud_unregister_path(data_dir);
    if record.request.cursor_recovery.is_none()
        && let Some(existing) = load_pending_cloud_unregister_record(data_dir)?
    {
        return if existing == *record {
            Ok(())
        } else {
            Err(format!(
                "refusing to replace pending Cloud Files unregister {} for {} -> {} with a different registration identity",
                path.display(),
                existing.request.sync_root.display(),
                existing.request.root_path
            ))
        };
    }
    let parent = path.parent().ok_or_else(|| {
        format!(
            "pending Cloud Files unregister path {} has no parent",
            path.display()
        )
    })?;
    std::fs::create_dir_all(parent).map_err(|error| {
        format!(
            "create pending Cloud Files unregister directory {}: {error}",
            parent.display()
        )
    })?;
    let bytes = sinomo_runtime::module_api::encode_sobj(record)
        .map_err(|error| format!("encode pending Cloud Files unregister: {error}"))?;
    if record.request.cursor_recovery.is_some() {
        let temporary = cursor_recovery_pending_temp_path(data_dir);
        let current = read_bounded_plain_host_file(
            &path,
            1024 * 1024,
            "pending Cloud Files cursor-recovery intent",
        )?;
        let staged = read_bounded_plain_host_file(
            &temporary,
            1024 * 1024,
            "pending Cloud Files cursor-recovery temporary intent",
        )?;
        return match (current.as_deref(), staged.as_deref()) {
            (Some(current), None) if current == bytes => Ok(()),
            (None, Some(staged)) if staged == bytes => {
                sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                    .map_err(|error| format!("promote cursor-recovery pending intent: {error}"))?;
                sync_pending_cloud_unregister_directory(parent)
            }
            (Some(current), Some(staged)) if current == bytes && staged == bytes => {
                sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
                    &temporary,
                )
                .map_err(|error| {
                    format!("remove redundant cursor-recovery pending intent: {error}")
                })?;
                Ok(())
            }
            (None, None) => {
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temporary)
                    .map_err(|error| {
                        format!(
                            "create cursor-recovery pending temporary file {}: {error}",
                            temporary.display()
                        )
                    })?;
                file.write_all(&bytes)
                    .and_then(|_| file.sync_all())
                    .map_err(|error| {
                        format!(
                            "persist cursor-recovery pending temporary file {}: {error}",
                            temporary.display()
                        )
                    })?;
                drop(file);
                sync_pending_cloud_unregister_directory(parent)?;
                sinomo_runtime::storage::durable_file::replace_file(&temporary, &path)
                    .map_err(|error| format!("commit cursor-recovery pending intent: {error}"))?;
                sync_pending_cloud_unregister_directory(parent)
            }
            _ => Err(
                "cursor-recovery pending current/temporary snapshot changed or is ambiguous"
                    .to_string(),
            ),
        };
    }
    let temporary = parent.join(format!(
        ".{PENDING_CLOUD_UNREGISTER_FILE}.{}.{}.tmp",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                format!(
                    "create pending Cloud Files unregister temporary file {}: {error}",
                    temporary.display()
                )
            })?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| {
                format!(
                    "persist pending Cloud Files unregister temporary file {}: {error}",
                    temporary.display()
                )
            })?;
        std::fs::rename(&temporary, &path).map_err(|error| {
            format!(
                "commit pending Cloud Files unregister {}: {error}",
                path.display()
            )
        })?;
        sync_pending_cloud_unregister_directory(parent)
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

#[cfg(any(windows, test))]
fn clear_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
    expected: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    let path = pending_cloud_unregister_path(data_dir);
    let Some(existing) = load_pending_cloud_unregister_record(data_dir)? else {
        return Ok(());
    };
    if existing != *expected {
        return Err(format!(
            "refusing to clear changed pending Cloud Files unregister {}",
            path.display()
        ));
    }
    std::fs::remove_file(&path).map_err(|error| {
        format!(
            "clear completed pending Cloud Files unregister {}: {error}",
            path.display()
        )
    })?;
    if let Some(parent) = path.parent() {
        sync_pending_cloud_unregister_directory(parent)?;
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn clear_staged_cloud_registration_record(
    data_dir: &SinomoDataDir,
    expected: &PendingCloudUnregisterRecord,
) -> Result<(), String> {
    let path = staged_cloud_registration_path(data_dir);
    let Some(existing) = load_staged_cloud_registration_record(data_dir)? else {
        return Ok(());
    };
    if !same_cloud_registration_recovery_identity(&existing.recovery, expected) {
        return Err(format!(
            "refusing to clear staged Cloud Files registration recovery with changed ownership identity {}",
            path.display()
        ));
    }
    std::fs::remove_file(&path).map_err(|error| {
        format!(
            "clear staged Cloud Files registration recovery {}: {error}",
            path.display()
        )
    })?;
    if let Some(parent) = path.parent() {
        sync_pending_cloud_unregister_directory(parent)?;
    }
    Ok(())
}

#[cfg(all(unix, any(windows, test)))]
fn sync_pending_cloud_unregister_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "sync pending Cloud Files unregister directory {}: {error}",
                path.display()
            )
        })
}

#[cfg(all(not(unix), any(windows, test)))]
fn sync_pending_cloud_unregister_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(any(windows, test))]
fn cloud_files_registration_receipt_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .module_runtime_dir()
        .join(sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(CLOUD_FILES_REGISTRATION_RECEIPT_FILE)
}

#[cfg(any(windows, test))]
fn load_cloud_files_registration_receipt(
    data_dir: &SinomoDataDir,
) -> Result<Option<sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt>, String> {
    let path = cloud_files_registration_receipt_path(data_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "read Cloud Files registration receipt {}: {error}",
                path.display()
            ));
        }
    };
    sinomo_runtime::module_api::decode_sobj(&bytes)
        .map(Some)
        .map_err(|error| {
            format!(
                "decode Cloud Files registration receipt {}: {error}",
                path.display()
            )
        })
}

#[cfg(any(windows, test))]
fn cloud_files_ready_artifacts_fingerprint(data_dir: &SinomoDataDir) -> Result<String, String> {
    let receipt_path = cloud_files_registration_receipt_path(data_dir);
    let journal_path =
        cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE);
    let receipt =
        read_bounded_plain_host_file(&receipt_path, 64 * 1024, "Cloud Files Ready receipt")?
            .ok_or_else(|| "Cloud Files Ready receipt is absent".to_string())?;
    let decoded_receipt: sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt =
        sinomo_runtime::module_api::decode_sobj(&receipt)
            .map_err(|error| format!("decode Cloud Files Ready receipt: {error}"))?;
    if sinomo_runtime::module_api::encode_sobj(&decoded_receipt)
        .map_err(|error| format!("re-encode Cloud Files Ready receipt: {error}"))?
        != receipt
    {
        return Err("Cloud Files Ready receipt is not canonical".to_string());
    }
    let journal = read_bounded_plain_host_file(
        &journal_path,
        1024 * 1024,
        "Cloud Files Ready registration journal",
    )?
    .ok_or_else(|| "Cloud Files Ready registration journal is absent".to_string())?;
    // The journal schema is module-owned; the host binds its exact canonical
    // raw bytes and the module independently performs typed canonical
    // validation before the acknowledged event can be emitted.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.cloud-files.cursor-recovery.ready-artifacts.v1\0");
    for (label, bytes) in [
        (b"receipt".as_slice(), receipt.as_slice()),
        (b"journal".as_slice(), journal.as_slice()),
    ] {
        hasher.update(&(label.len() as u64).to_le_bytes());
        hasher.update(label);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn cloud_files_ready_artifacts_fingerprint_from_raw(receipt: &[u8], journal: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.cloud-files.cursor-recovery.ready-artifacts.v1\0");
    for (label, bytes) in [
        (b"receipt".as_slice(), receipt),
        (b"journal".as_slice(), journal),
    ] {
        hasher.update(&(label.len() as u64).to_le_bytes());
        hasher.update(label);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

#[cfg(any(windows, test))]
fn cloud_files_completed_registration_artifacts_fingerprint_from_raw(
    receipt: &[u8],
    journal: &[u8],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.completed-registration.v1\0");
    for (label, bytes) in [
        (b"receipt".as_slice(), receipt),
        (b"journal".as_slice(), journal),
    ] {
        hash_host_cursor_recovery_field(&mut hasher, label);
        hash_host_cursor_recovery_field(&mut hasher, bytes);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

#[cfg(any(windows, test))]
fn cloud_files_completed_registration_artifacts_fingerprint(
    data_dir: &SinomoDataDir,
) -> Result<String, String> {
    let receipt = read_bounded_plain_host_file(
        &cloud_files_registration_receipt_path(data_dir),
        64 * 1024,
        "completed Cloud Files Ready receipt",
    )?
    .ok_or_else(|| "completed Cloud Files Ready receipt is absent".to_string())?;
    let journal = read_bounded_plain_host_file(
        &cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
        1024 * 1024,
        "completed ordinary Cloud Files Ready journal",
    )?
    .ok_or_else(|| "completed ordinary Cloud Files Ready journal is absent".to_string())?;
    Ok(cloud_files_completed_registration_artifacts_fingerprint_from_raw(&receipt, &journal))
}

#[cfg(any(windows, test))]
fn cloud_files_completed_artifacts_fingerprint(
    data_dir: &SinomoDataDir,
    stage: &StagedCloudRegistrationRecord,
) -> Result<String, String> {
    let registration = cloud_files_completed_registration_artifacts_fingerprint(data_dir)?;
    let stage_bytes = sinomo_runtime::module_api::encode_sobj(stage)
        .map_err(|error| format!("encode normalized Cloud Files stage: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.completed-artifacts.v1\0");
    hash_host_cursor_recovery_field(&mut hasher, registration.as_bytes());
    hash_host_cursor_recovery_field(&mut hasher, &stage_bytes);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn cloud_files_module_marker_raw_fingerprint(data_dir: &SinomoDataDir) -> Result<String, String> {
    let bytes = read_bounded_plain_host_file(
        &cloud_files_module_runtime_path(data_dir, CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE),
        1024 * 1024,
        "Cloud Files cursor-recovery module marker",
    )?
    .ok_or_else(|| "Cloud Files cursor-recovery module marker is absent".to_string())?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.module-marker.v1\0");
    hash_host_cursor_recovery_field(&mut hasher, &bytes);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
struct AuthorityFinalizedCompletionCut {
    cursor_stage: StagedCloudRegistrationRecord,
    ordinary_stage: StagedCloudRegistrationRecord,
    registration_normalized: bool,
    stage_normalized: bool,
}

#[cfg(any(windows, test))]
fn validate_authority_finalized_completion_cut(
    data_dir: &SinomoDataDir,
    marker: &HostCursorRecoveryMarker,
) -> Result<AuthorityFinalizedCompletionCut, String> {
    if marker.phase != HostCursorRecoveryPhase::AuthorityFinalized {
        return Err("terminal completion cut requires AuthorityFinalized host state".to_string());
    }
    require_cursor_recovery_pending_intent_absent(data_dir, "AuthorityFinalized completion")?;
    require_physically_absent(
        &cloud_files_cursor_recovery_module_temp_path(data_dir),
        "AuthorityFinalized module cursor-recovery temporary marker",
    )?;
    if cloud_files_module_marker_raw_fingerprint(data_dir)?
        != marker
            .completion_module_marker_fingerprint
            .as_deref()
            .ok_or_else(|| "AuthorityFinalized marker lacks its native marker proof".to_string())?
    {
        return Err("AuthorityFinalized module marker proof changed".to_string());
    }
    require_physically_absent(
        &cloud_files_registration_receipt_temp_path(data_dir),
        "AuthorityFinalized registration receipt temporary file",
    )?;
    let receipt = read_bounded_plain_host_file(
        &cloud_files_registration_receipt_path(data_dir),
        64 * 1024,
        "AuthorityFinalized registration receipt",
    )?
    .ok_or_else(|| "AuthorityFinalized registration receipt is absent".to_string())?;
    let decoded_receipt: sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt =
        sinomo_runtime::module_api::decode_sobj(&receipt)
            .map_err(|error| format!("decode AuthorityFinalized receipt: {error}"))?;
    if sinomo_runtime::module_api::encode_sobj(&decoded_receipt)
        .map_err(|error| format!("re-encode AuthorityFinalized receipt: {error}"))?
        != receipt
    {
        return Err("AuthorityFinalized receipt is noncanonical".to_string());
    }
    let journal_path =
        cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE);
    let journal = read_bounded_plain_host_file(
        &journal_path,
        1024 * 1024,
        "AuthorityFinalized registration journal",
    )?
    .ok_or_else(|| "AuthorityFinalized registration journal is absent".to_string())?;
    let journal_temp = read_bounded_plain_host_file(
        &cloud_files_registration_journal_temp_path(data_dir),
        1024 * 1024,
        "AuthorityFinalized registration journal temporary file",
    )?;
    let ready_current = cloud_files_ready_artifacts_fingerprint_from_raw(&receipt, &journal)
        == marker
            .ready_artifacts_fingerprint
            .as_deref()
            .ok_or_else(|| "AuthorityFinalized marker lacks its Ready proof".to_string())?;
    let completed_expected = marker
        .completion_registration_artifacts_fingerprint
        .as_deref()
        .ok_or_else(|| {
            "AuthorityFinalized marker lacks its normalized registration proof".to_string()
        })?;
    let completed_current =
        cloud_files_completed_registration_artifacts_fingerprint_from_raw(&receipt, &journal)
            == completed_expected;
    let completed_temp = journal_temp.as_ref().is_some_and(|temporary| {
        cloud_files_completed_registration_artifacts_fingerprint_from_raw(&receipt, temporary)
            == completed_expected
    });
    let registration_normalized = match journal_temp.as_ref() {
        None if completed_current => true,
        None if ready_current => false,
        Some(_temporary) if ready_current && completed_temp => false,
        Some(temporary) if completed_current && temporary == &journal => false,
        _ => {
            return Err(
                "AuthorityFinalized registration current/temporary cut is not an exact normalization predecessor or successor"
                    .to_string(),
            );
        }
    };

    let cursor_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            data_dir_root: Some(marker.identity.data_dir_root.clone()),
            provider_name: marker.identity.provider_name.clone(),
            cursor_recovery: Some(host_cursor_recovery_context(marker)),
        },
        marker.module.clone(),
        CloudUnregisterPolicy::PreserveState,
    )?;
    let cursor_stage = staged_cloud_registration_record(
        &cursor_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    let ordinary_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            cursor_recovery: None,
            ..cursor_recovery.request.clone()
        },
        marker.module.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    let ordinary_stage = staged_cloud_registration_record(
        &ordinary_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
    let cursor_current = staged.current.as_ref() == Some(&cursor_stage);
    let ordinary_current = staged.current.as_ref() == Some(&ordinary_stage);
    let ordinary_temp = staged.temporary.as_ref() == Some(&ordinary_stage);
    let cursor_temp = staged.temporary.as_ref() == Some(&cursor_stage);
    let exact_stage_cut = if !registration_normalized {
        cursor_current && staged.temporary.is_none()
    } else if marker.completed_artifacts_fingerprint.is_some() {
        ordinary_current && staged.temporary.is_none()
    } else {
        (cursor_current && (staged.temporary.is_none() || ordinary_temp))
            || (ordinary_current && (staged.temporary.is_none() || ordinary_temp))
    };
    if !exact_stage_cut || cursor_temp {
        return Err(
            "AuthorityFinalized staged current/temporary cut is not an exact normalization predecessor or successor"
                .to_string(),
        );
    }
    let stage_normalized = ordinary_current && staged.temporary.is_none();
    if let Some(completed) = marker.completed_artifacts_fingerprint.as_ref() {
        if !registration_normalized
            || !stage_normalized
            || completed != &cloud_files_completed_artifacts_fingerprint(data_dir, &ordinary_stage)?
        {
            return Err("AuthorityFinalized completed artifact proof changed".to_string());
        }
    }
    Ok(AuthorityFinalizedCompletionCut {
        cursor_stage,
        ordinary_stage,
        registration_normalized,
        stage_normalized,
    })
}

#[cfg(any(windows, test))]
fn validate_authority_finalized_released_cut(
    data_dir: &SinomoDataDir,
    marker: &HostCursorRecoveryMarker,
) -> Result<(), String> {
    if marker.phase != HostCursorRecoveryPhase::AuthorityFinalized {
        return Err("released completion cut requires AuthorityFinalized host state".to_string());
    }
    require_cursor_recovery_pending_intent_absent(data_dir, "released AuthorityFinalized")?;
    require_physically_absent(
        &cloud_files_module_runtime_path(data_dir, CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE),
        "released module cursor-recovery marker",
    )?;
    require_physically_absent(
        &cloud_files_cursor_recovery_module_temp_path(data_dir),
        "released module cursor-recovery temporary marker",
    )?;
    require_physically_absent(
        &cloud_files_registration_journal_temp_path(data_dir),
        "released registration journal temporary file",
    )?;
    require_physically_absent(
        &cloud_files_registration_receipt_temp_path(data_dir),
        "released registration receipt temporary file",
    )?;
    let ordinary_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            data_dir_root: Some(marker.identity.data_dir_root.clone()),
            provider_name: marker.identity.provider_name.clone(),
            cursor_recovery: None,
        },
        marker.module.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    let ordinary_stage = staged_cloud_registration_record(
        &ordinary_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
    let completed_registration =
        cloud_files_completed_registration_artifacts_fingerprint(data_dir)?;
    if marker
        .completion_registration_artifacts_fingerprint
        .as_deref()
        != Some(&completed_registration)
        || staged.current.as_ref() != Some(&ordinary_stage)
        || staged.temporary.is_some()
        || marker.completed_artifacts_fingerprint.as_ref()
            != Some(&cloud_files_completed_artifacts_fingerprint(
                data_dir,
                &ordinary_stage,
            )?)
    {
        return Err("released AuthorityFinalized artifacts changed".to_string());
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn cloud_files_preserved_artifacts_fingerprint(data_dir: &SinomoDataDir) -> Result<String, String> {
    let receipt_path = cloud_files_registration_receipt_path(data_dir);
    let journal_path =
        cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE);
    let receipt =
        read_bounded_plain_host_file(&receipt_path, 64 * 1024, "Cloud Files preserved receipt")?;
    if let Some(receipt) = receipt.as_ref() {
        let decoded: sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt =
            sinomo_runtime::module_api::decode_sobj(receipt)
                .map_err(|error| format!("decode Cloud Files preserved receipt: {error}"))?;
        if sinomo_runtime::module_api::encode_sobj(&decoded)
            .map_err(|error| format!("re-encode Cloud Files preserved receipt: {error}"))?
            != *receipt
        {
            return Err("Cloud Files preserved receipt is not canonical".to_string());
        }
    }
    let journal = read_bounded_plain_host_file(
        &journal_path,
        1024 * 1024,
        "Cloud Files preserved StateOwned registration journal",
    )?
    .ok_or_else(|| "Cloud Files preserved StateOwned journal is absent".to_string())?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.cloud-files.cursor-recovery.preserved-artifacts.v1\0");
    match receipt.as_ref() {
        Some(bytes) => {
            hasher.update(b"receipt-present\0");
            hasher.update(&(bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        }
        None => {
            hasher.update(b"receipt-absent\0");
        }
    }
    hasher.update(b"journal\0");
    hasher.update(&(journal.len() as u64).to_le_bytes());
    hasher.update(&journal);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(any(windows, test))]
fn registration_receipt_matches_config(
    receipt: &sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt,
    config: &SifsMountConfig,
) -> Result<bool, String> {
    if receipt.root_path != config.root_path
        || receipt.provider_name
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
    {
        return Ok(false);
    }
    let configured = canonical_absolute_path_allow_missing(&lexical_absolute_path(
        &expand_mount_point(&config.mount_point)?,
    )?)?;
    let registered = canonical_absolute_path_allow_missing(&receipt.sync_root)?;
    Ok(configured == registered)
}

#[cfg(any(windows, test))]
fn registration_request_matches_config(
    request: &sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest,
    config: &SifsMountConfig,
) -> Result<bool, String> {
    if request.root_path != config.root_path
        || request.provider_name
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
    {
        return Ok(false);
    }
    let configured = canonical_absolute_path_allow_missing(&lexical_absolute_path(
        &expand_mount_point(&config.mount_point)?,
    )?)?;
    let registered = canonical_absolute_path_allow_missing(&request.sync_root)?;
    Ok(configured == registered)
}

#[cfg(any(windows, test))]
fn recover_pending_cloud_unregister_from_staged_with(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    active_module: Option<&CloudFilesModuleProvenance>,
) -> Result<Option<PendingCloudUnregisterRecord>, String> {
    let Some(record) = load_staged_cloud_registration_record(data_dir)? else {
        return Ok(None);
    };
    verify_cloud_unregister_recovery_module(data_dir, &record.recovery.module).map_err(
        |error| {
            format!(
                "staged Cloud Files registration cannot be recovered by its exact provider: {error}"
            )
        },
    )?;
    let registration_is_current =
        registration_request_matches_config(&record.recovery.request, config)?;
    let provider_is_current = active_module.is_none_or(|active| active == &record.recovery.module);
    if registration_is_current
        && !provider_is_current
        && record.recovery.legacy_prejournal_origin.is_none()
        && record.recovery.request.cursor_recovery.is_none()
        && record.recovery.module.interface_version
            == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
    {
        // A newly activated provider is an explicit replacement request, but
        // it must never inherit an old provider's durable ownership by loader
        // drift alone. Replay the exact old pinned provider's idempotent
        // Release first. The Released response clears the old stage and pin;
        // only a later supervisor turn may stage and mount the new provider.
        // RetainedAbsent is already physically unregistered. MayBeRegistered
        // is also safe here because Release re-proves CFAPI, receipt, and
        // journal absence before the host discards the old authority.
        return Ok(Some(record.recovery.clone()));
    }
    if registration_is_current && (provider_is_current || !config.enabled) {
        if config.enabled {
            if record.state == StagedCloudRegistrationState::RetainedAbsent {
                let may_be_registered = StagedCloudRegistrationRecord {
                    state: StagedCloudRegistrationState::MayBeRegistered,
                    ..record.clone()
                };
                transition_or_promote_cursor_recovery_staged_record(
                    data_dir,
                    &record,
                    &may_be_registered,
                )?;
            }
            return Ok(None);
        }
        if record.state == StagedCloudRegistrationState::RetainedAbsent {
            return Ok(None);
        }
        if record.recovery.legacy_prejournal_origin.is_some()
            && record.recovery.module.interface_version
                == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
        {
            return Err(format!(
                "Cloud Files disable is blocked for {}: deployed v2 may have created writable state before its nonce journal, and no v3 Ready proof has normalized that risk",
                record.recovery.request.sync_root.display()
            ));
        }
        let mut pending = pending_cloud_unregister_record_with_policy(
            record.recovery.request,
            record.recovery.module,
            CloudUnregisterPolicy::PreserveState,
        )?;
        pending.legacy_prejournal_origin = record.recovery.legacy_prejournal_origin;
        return Ok(Some(pending));
    }
    Err(format!(
        "Cloud Files registration replacement is blocked: retained registration {} -> {} belongs to provider {}, not the configured identity/provider",
        record.recovery.request.sync_root.display(),
        record.recovery.request.root_path,
        record.recovery.module.content_hash
    ))
}

#[cfg(windows)]
fn active_cloud_files_replacement_provider(
    data_dir: &SinomoDataDir,
    staged_provider: &CloudFilesModuleProvenance,
) -> Result<Option<CloudFilesModuleProvenance>, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
    };
    use sinomo_runtime::module::{
        ModuleId, NativeModule, native_module_cache_path, read_native_dylib_module_loader_state,
    };

    let module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    let Some(active) = read_native_dylib_module_loader_state(&data_dir.root(), &module_id)
        .map_err(|error| format!("read active Cloud Files replacement provider: {error}"))?
    else {
        return Ok(None);
    };
    if active.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION {
        return Err(format!(
            "active Cloud Files replacement interface mismatch: expected {}, got {}",
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, active.interface_version
        ));
    }
    let provenance = CloudFilesModuleProvenance::from_loader_state(&active);
    if &provenance == staged_provider {
        return Ok(Some(provenance));
    }
    let library_path = native_module_cache_path(
        &data_dir.root(),
        &active.cache_key,
        &active.content_hash,
        &active.library_name,
    );
    let module = unsafe {
        NativeModule::load_from_content_addressed_path(&library_path, &active.content_hash)
    }
    .map_err(|error| format!("load active Cloud Files replacement provider: {error}"))?;
    admit_cloud_files_provider_before_effect_with(
        &provenance,
        module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        Some(SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY),
        || Ok(()),
    )?;
    drop(module);
    Ok(Some(provenance))
}

#[cfg(any(windows, test))]
fn legacy_staged_provider_upgrade_record(
    record: &StagedCloudRegistrationRecord,
    v3_provider: CloudFilesModuleProvenance,
) -> Result<StagedCloudRegistrationRecord, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        SINOMO_DFS_CLOUD_FILES_INTERFACE_V2, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
    };

    let origin = record
        .recovery
        .legacy_prejournal_origin
        .as_ref()
        .ok_or_else(|| "legacy Cloud Files adoption has no exact v2 origin".to_string())?;
    if record.state != StagedCloudRegistrationState::MayBeRegistered
        || record.recovery.module != *origin
        || origin.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
        || v3_provider.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
    {
        return Err(
            "legacy Cloud Files adoption requires an exact MayBeRegistered v2 origin and a current v3 provider"
                .to_string(),
        );
    }
    let mut recovery =
        pending_cloud_unregister_record(record.recovery.request.clone(), v3_provider)?;
    recovery.legacy_prejournal_origin = Some(origin.clone());
    Ok(StagedCloudRegistrationRecord {
        schema_version: STAGED_CLOUD_REGISTRATION_SCHEMA_VERSION,
        state: StagedCloudRegistrationState::MayBeRegistered,
        recovery,
    })
}

#[cfg(windows)]
fn upgrade_legacy_staged_provider(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    record: StagedCloudRegistrationRecord,
    active: &sinomo_runtime::module::NativeDylibModuleLoaderState,
) -> Result<StagedCloudRegistrationRecord, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        SINOMO_DFS_CLOUD_FILES_INTERFACE_V2, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
    };
    use sinomo_runtime::module::{NativeModule, native_module_cache_path};

    let Some(origin) = record.recovery.legacy_prejournal_origin.as_ref() else {
        return Ok(record);
    };
    if record.recovery.module.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_V2 {
        return Ok(record);
    }
    if origin != &record.recovery.module {
        return Err(
            "legacy Cloud Files pre-journal origin does not match its exact v2 recovery provider"
                .to_string(),
        );
    }
    if !registration_request_matches_config(&record.recovery.request, config)? {
        return Ok(record);
    }
    if active.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION {
        return Err(format!(
            "active Cloud Files provider interface {} cannot adopt legacy v2 ownership",
            active.interface_version
        ));
    }
    let provenance = CloudFilesModuleProvenance::from_loader_state(active);
    let source = native_module_cache_path(
        &data_dir.root(),
        &active.cache_key,
        &active.content_hash,
        &active.library_name,
    );
    let module =
        unsafe { NativeModule::load_from_content_addressed_path(&source, &active.content_hash) }
            .map_err(|error| {
                format!("load v3 Cloud Files provider for legacy adoption: {error}")
            })?;
    admit_cloud_files_provider_before_effect_with(
        &provenance,
        module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        Some(
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
        ),
        || stage_cloud_unregister_recovery_module(data_dir, &provenance, &source).map(|_| ()),
    )?;
    drop(module);
    let upgraded = legacy_staged_provider_upgrade_record(&record, provenance)?;
    replace_staged_cloud_registration_record(data_dir, &record, &upgraded)?;
    Ok(upgraded)
}

#[cfg(windows)]
fn recover_pending_cloud_unregister_from_staged(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<Option<PendingCloudUnregisterRecord>, String> {
    use sinomo_runtime::module::{ModuleId, read_native_dylib_module_loader_state};

    let Some(record) = load_staged_cloud_registration_record(data_dir)? else {
        require_physically_absent(
            &cursor_recovery_staged_temp_path(data_dir),
            "orphan deterministic staged Cloud Files successor",
        )?;
        return Ok(None);
    };
    if record.recovery.module.interface_version
        == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        && record.recovery.legacy_prejournal_origin.is_none()
    {
        if record.recovery.request.cursor_recovery.is_some() {
            return Err(
                "cursor-context staged ownership exists without its host authorization".to_string(),
            );
        }
        if !registration_request_matches_config(&record.recovery.request, config)? {
            return Err(
                "ordinary staged Cloud Files ownership does not match the exact configured registration"
                    .to_string(),
            );
        }
        let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
        let may_be_registered = StagedCloudRegistrationRecord {
            state: StagedCloudRegistrationState::MayBeRegistered,
            ..record.clone()
        };
        let exact_enable_successor = record.state == StagedCloudRegistrationState::RetainedAbsent
            && staged.current.as_ref() == Some(&record)
            && staged.temporary.as_ref() == Some(&may_be_registered);
        if staged.current.as_ref() != Some(&record)
            || staged
                .temporary
                .as_ref()
                .is_some_and(|temporary| temporary != &record)
        {
            if !exact_enable_successor {
                return Err(
                    "ordinary staged Cloud Files current/temporary authority is changed or impossible"
                        .to_string(),
                );
            }
        }
        verify_cloud_unregister_recovery_module(data_dir, &record.recovery.module)?;
        if staged.temporary.as_ref() == Some(&record) {
            persist_or_promote_cursor_recovery_staged_record(data_dir, &record)?;
        } else if exact_enable_successor {
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &record,
                &may_be_registered,
            )?;
        }
        // A canonical v3 stage remains the provider-selection authority until
        // a different, fully validated active provider requests replacement.
        // Replacement first invokes Release through the old pinned provider;
        // there is no direct loader fallback or ownership substitution.
        let active = active_cloud_files_replacement_provider(data_dir, &record.recovery.module)?;
        return recover_pending_cloud_unregister_from_staged_with(
            data_dir,
            config,
            active.as_ref(),
        );
    }
    require_physically_absent(
        &cursor_recovery_staged_temp_path(data_dir),
        "legacy staged Cloud Files deterministic successor",
    )?;
    ensure_cloud_unregister_recovery_module(data_dir, &record.recovery.module)?;
    let module_id = ModuleId::from_legacy_named(
        sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID,
    )
    .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    let active_state = read_native_dylib_module_loader_state(&data_dir.root(), &module_id)
        .map_err(|error| format!("read Cloud Files module state for staged recovery: {error}"))?;
    if let Some(active) = active_state.as_ref() {
        upgrade_legacy_staged_provider(data_dir, config, record, active)?;
    }
    let active = active_state
        .as_ref()
        .map(CloudFilesModuleProvenance::from_loader_state);
    recover_pending_cloud_unregister_from_staged_with(data_dir, config, active.as_ref())
}

#[cfg(any(windows, test))]
fn recover_pending_cloud_unregister_from_receipt(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<Option<PendingCloudUnregisterRecord>, String> {
    // A valid staged record is the authoritative ownership/provenance state.
    // In particular, RetainedAbsent is a settled disabled state: consulting
    // the receipt again would synthesize a second unregister on every poll.
    if load_staged_cloud_registration_record(data_dir)?.is_some() {
        return Ok(None);
    }
    let Some(receipt) = load_cloud_files_registration_receipt(data_dir)? else {
        return Ok(None);
    };
    let receipt_matches = registration_receipt_matches_config(&receipt, config)?;
    if receipt_matches && config.enabled {
        return Ok(None);
    }
    if !receipt_matches {
        return Err(format!(
            "Cloud Files registration replacement is blocked: durable receipt {} -> {} does not match the configured identity",
            receipt.sync_root.display(),
            receipt.root_path
        ));
    }
    Err(format!(
        "Cloud Files registration receipt for {} -> {} has no pinned staged provider/nonce journal; refusing to synthesize a state-preserving unregister",
        receipt.sync_root.display(),
        receipt.root_path
    ))
}

/// Process-wide host-mount ownership for one node-local Sinomo data directory.
///
/// The lock file is intentionally never unlinked: removing it after unlock can
/// race a new owner which already locked the old inode, allowing a third owner
/// to create and lock a different inode at the same path.
#[derive(Debug)]
pub(crate) struct SifsMountLease {
    file: File,
    path: PathBuf,
    cursor_recovery_bytes: Option<Vec<u8>>,
}

impl SifsMountLease {
    pub(crate) fn try_acquire(data_dir: &SinomoDataDir, owner: &str) -> Result<Self, String> {
        let lock_dir = data_dir.dfs_dir();
        std::fs::create_dir_all(&lock_dir).map_err(|error| {
            format!(
                "create SIFS host-mount lease directory {}: {error}",
                lock_dir.display()
            )
        })?;
        let path = lock_dir.join("host-mount.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| format!("open SIFS host-mount lease {}: {error}", path.display()))?;
        if let Err(error) = try_lock_mount_lease_file(&file) {
            let current_owner = std::fs::read_to_string(&path)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| format!("; current owner: {}", value.replace('\n', ", ")))
                .unwrap_or_default();
            return Err(format!(
                "SIFS host-mount lease {} is unavailable; another managed or foreground mount may already own this data directory{current_owner}: {error}",
                path.display()
            ));
        }

        let mut lease = Self {
            file,
            path,
            cursor_recovery_bytes: None,
        };
        lease.write_owner(owner)?;
        Ok(lease)
    }

    #[cfg(any(windows, test))]
    fn try_acquire_existing_for_cursor_recovery(
        data_dir: &SinomoDataDir,
    ) -> Result<(Self, Vec<u8>), String> {
        let data_root = data_dir.root();
        let lock_dir = data_dir.dfs_dir();
        ensure_plain_existing_host_path(&data_root, true, "Sinomo data directory")?;
        ensure_plain_existing_host_path(&lock_dir, true, "DFS runtime directory")?;
        let path = lock_dir.join("host-mount.lock");
        ensure_plain_existing_host_path(&path, false, "SIFS host-mount lease")?;
        let mut file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                format!(
                    "open pre-existing SIFS host-mount lease {} without creating it: {error}",
                    path.display()
                )
            })?;
        try_lock_mount_lease_file(&file).map_err(|error| {
            format!(
                "SIFS host-mount lease {} is unavailable; a foreground or managed mount may be active: {error}",
                path.display()
            )
        })?;
        let length = file
            .metadata()
            .map_err(|error| format!("inspect SIFS host-mount lease {}: {error}", path.display()))?
            .len();
        if length > CLOUD_FILES_CURSOR_RECOVERY_HOST_LOCK_MAX_BYTES {
            let _ = unlock_mount_lease_file(&file);
            return Err(format!(
                "SIFS host-mount lease {} is {length} bytes; maximum recovery evidence size is {CLOUD_FILES_CURSOR_RECOVERY_HOST_LOCK_MAX_BYTES}",
                path.display()
            ));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        if let Err(error) = file
            .seek(SeekFrom::Start(0))
            .and_then(|_| file.read_to_end(&mut bytes))
        {
            let _ = unlock_mount_lease_file(&file);
            return Err(format!(
                "read SIFS host-mount lease {} without rewriting it: {error}",
                path.display()
            ));
        }
        Ok((
            Self {
                file,
                path,
                cursor_recovery_bytes: Some(bytes.clone()),
            },
            bytes,
        ))
    }

    #[cfg(any(windows, test))]
    fn cursor_recovery_bytes(&self) -> Option<&[u8]> {
        self.cursor_recovery_bytes.as_deref()
    }

    fn write_owner(&mut self, owner: &str) -> Result<(), String> {
        self.file.set_len(0).map_err(|error| {
            format!(
                "clear SIFS host-mount lease {}: {error}",
                self.path.display()
            )
        })?;
        self.file.seek(SeekFrom::Start(0)).map_err(|error| {
            format!(
                "seek SIFS host-mount lease {}: {error}",
                self.path.display()
            )
        })?;
        let owner = owner.replace(['\r', '\n'], " ");
        writeln!(self.file, "pid={}", std::process::id())
            .and_then(|_| writeln!(self.file, "owner={owner}"))
            .and_then(|_| self.file.flush())
            .map_err(|error| {
                format!(
                    "write SIFS host-mount lease {}: {error}",
                    self.path.display()
                )
            })
    }
}

#[cfg(any(windows, test))]
fn ensure_plain_existing_host_path(
    path: &Path,
    expected_directory: bool,
    label: &str,
) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink()
        || (expected_directory && !metadata.is_dir())
        || (!expected_directory && !metadata.is_file())
    {
        return Err(format!(
            "{label} {} must be a plain {}",
            path.display(),
            if expected_directory {
                "directory"
            } else {
                "file"
            }
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(format!(
                "{label} {} is a Windows reparse point",
                path.display()
            ));
        }
    }
    Ok(())
}

impl Drop for SifsMountLease {
    fn drop(&mut self) {
        if let Err(error) = unlock_mount_lease_file(&self.file) {
            tracing::warn!(
                "failed to unlock SIFS host-mount lease {}: {error}",
                self.path.display()
            );
        }
    }
}

#[cfg(unix)]
fn try_lock_mount_lease_file(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn unlock_mount_lease_file(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(windows)]
mod windows_mount_lease {
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        event: *mut c_void,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LockFileEx(
            file: *mut c_void,
            flags: u32,
            reserved: u32,
            bytes_low: u32,
            bytes_high: u32,
            overlapped: *mut Overlapped,
        ) -> i32;
        fn UnlockFileEx(
            file: *mut c_void,
            reserved: u32,
            bytes_low: u32,
            bytes_high: u32,
            overlapped: *mut Overlapped,
        ) -> i32;
    }

    fn overlapped() -> Overlapped {
        Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            event: std::ptr::null_mut(),
        }
    }

    pub(super) fn try_lock(file: &File) -> io::Result<()> {
        let mut overlapped = overlapped();
        let result = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn unlock(file: &File) -> io::Result<()> {
        let mut overlapped = overlapped();
        let result =
            unsafe { UnlockFileEx(file.as_raw_handle(), 0, u32::MAX, u32::MAX, &mut overlapped) };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(windows)]
fn try_lock_mount_lease_file(file: &File) -> std::io::Result<()> {
    windows_mount_lease::try_lock(file)
}

#[cfg(windows)]
fn unlock_mount_lease_file(file: &File) -> std::io::Result<()> {
    windows_mount_lease::unlock(file)
}

#[cfg(not(any(unix, windows)))]
fn try_lock_mount_lease_file(_file: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "this platform has no supported host-mount file lock",
    ))
}

#[cfg(not(any(unix, windows)))]
fn unlock_mount_lease_file(_file: &File) -> std::io::Result<()> {
    Ok(())
}

impl MountStartRetry {
    fn permits_attempt(&mut self, config: &SifsMountConfig, now: Instant) -> bool {
        if self.config.as_ref() != Some(config) {
            self.clear();
            self.config = Some(config.clone());
            return true;
        }
        self.next_attempt_at.is_none_or(|deadline| now >= deadline)
    }

    fn record_failure(&mut self, config: &SifsMountConfig, error: impl Into<String>, now: Instant) {
        if self.config.as_ref() != Some(config) {
            self.clear();
            self.config = Some(config.clone());
        }
        self.delay = if self.delay.is_zero() {
            MOUNT_START_RETRY_INITIAL
        } else {
            self.delay.saturating_mul(2).min(MOUNT_START_RETRY_MAX)
        };
        self.next_attempt_at = Some(now + self.delay);
        self.last_error = Some(error.into());
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl SifsMountSupervisorService {
    fn new(data_dir: SinomoDataDir, daemon_paths: DaemonPaths, daemon_pid: u32) -> Self {
        let status = Arc::new(RwLock::new(SifsMountRuntimeStatus::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mount_cancellation = Arc::new(Mutex::new(None));
        let worker_status = Arc::clone(&status);
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_mount_cancellation = Arc::clone(&mount_cancellation);
        let (cursor_recovery, worker_cursor_recovery) = mpsc::sync_channel(1);
        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("sinomo-sifs-mount".to_string())
            .spawn(move || {
                run_mount_supervisor(
                    &data_dir,
                    &daemon_paths,
                    daemon_pid,
                    worker_status,
                    worker_shutdown,
                    worker_mount_cancellation,
                    worker_cursor_recovery,
                );
                let _ = stopped_tx.send(());
            })
            .expect("SIFS mount supervisor thread should start");
        Self {
            inner: Arc::new(SifsMountSupervisorInner {
                status,
                shutdown,
                mount_cancellation,
                cursor_recovery,
                worker: Mutex::new(Some(SifsMountSupervisorWorker {
                    thread: Some(thread),
                    stopped: stopped_rx,
                })),
            }),
        }
    }

    fn status(&self) -> SifsMountRuntimeStatus {
        self.inner
            .status
            .read()
            .map(|status| status.clone())
            .unwrap_or_else(|_| SifsMountRuntimeStatus {
                phase: SifsMountRuntimePhase::Error,
                last_error: Some("mount status lock is unavailable".to_string()),
                detail: "SIFS mount status is unavailable".to_string(),
                ..SifsMountRuntimeStatus::default()
            })
    }
}

impl Drop for SifsMountSupervisorInner {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Ok(cancellation) = self.mount_cancellation.lock()
            && let Some(cancellation) = cancellation.as_ref()
        {
            cancellation.cancel();
        }
        let Ok(worker) = self.worker.get_mut() else {
            return;
        };
        let Some(mut worker) = worker.take() else {
            return;
        };
        if let Some(handle) = worker.thread.as_ref() {
            handle.thread().unpark();
        }
        if worker
            .stopped
            .recv_timeout(SUPERVISOR_SHUTDOWN_TIMEOUT)
            .is_ok()
            && let Some(handle) = worker.thread.take()
        {
            let _ = handle.join();
        }
    }
}

impl Service for SifsMountSupervisorService {
    fn name(&self) -> &'static str {
        "sifs-mount-supervisor"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            SIFS_MOUNT_STATUS_ROUTE,
            SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE,
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let status = self.status();
        let cursor_recovery = self.inner.cursor_recovery.clone();
        Box::pin(async move {
            match request.route.as_str() {
                SIFS_MOUNT_STATUS_ROUTE => {
                    let _: SifsMountStatusRequest = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(SIFS_MOUNT_STATUS_ROUTE, SifsMountStatusResponse { status })?;
                }
                SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE => {
                    let command: SifsMountCursorOnlyRecoveryRequest =
                        ctx.decode_request_body(&request).await?;
                    let (response_tx, response_rx) = mpsc::sync_channel(1);
                    cursor_recovery
                        .send(SifsMountCursorRecoveryControl {
                            request: command,
                            response: response_tx,
                        })
                        .map_err(|_| {
                            ServiceError::Service("SIFS mount supervisor stopped".to_string())
                        })?;
                    let response = response_rx
                        .recv_timeout(Duration::from_secs(60))
                        .map_err(|_| {
                            ServiceError::Service(
                                "SIFS cursor recovery supervisor response timed out".to_string(),
                            )
                        })?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub(crate) fn sifs_mount_supervisor_service_hook(
    data_dir: SinomoDataDir,
    daemon_paths: DaemonPaths,
) -> DaemonServiceRegistryHook {
    let service = SifsMountSupervisorService::new(data_dir, daemon_paths, std::process::id());
    DaemonServiceRegistryHook::new(move |builder: ServiceRegistryBuilder| {
        builder.register(service.clone())
    })
}

fn retire_after_exit_status_persisted<P, R>(persist_status: P, retire: R) -> Result<(), String>
where
    P: FnOnce() -> Result<(), String>,
    R: FnOnce() -> Result<(), String>,
{
    persist_status().map_err(|error| {
        format!("persist mount exit status before retiring the adapter: {error}")
    })?;
    retire()
}

fn run_mount_supervisor(
    data_dir: &SinomoDataDir,
    daemon_paths: &DaemonPaths,
    daemon_pid: u32,
    observed_status: Arc<RwLock<SifsMountRuntimeStatus>>,
    shutdown: Arc<AtomicBool>,
    mount_cancellation: Arc<Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>>,
    cursor_recovery: mpsc::Receiver<SifsMountCursorRecoveryControl>,
) {
    let store = match sifs_mount_config_store() {
        Ok(store) => store,
        Err(error) => {
            publish_status_without_store(
                &observed_status,
                status_error(&SifsMountConfig::default(), None, daemon_pid, 0, error),
            );
            return;
        }
    };
    let mut active: Option<ActiveMount> = None;
    let mut restart_count = 0u64;
    let mut last_persisted_status = None;
    let mut start_retry = MountStartRetry::default();
    let mut pending_cloud_unregister = None;
    #[cfg(windows)]
    let mut pending_unregister_recovery_loaded = false;
    while !shutdown.load(Ordering::SeqCst) {
        if let Ok(command) = cursor_recovery.try_recv() {
            let result = handle_cursor_recovery_control_serialized(
                data_dir,
                &store,
                active.as_ref(),
                pending_cloud_unregister.as_ref(),
                &command.request,
            );
            let _ = command.response.send(result);
            continue;
        }
        #[cfg(windows)]
        {
            if !pending_unregister_recovery_loaded {
                match load_pending_cloud_unregister_record(data_dir) {
                    Ok(record) => {
                        pending_cloud_unregister =
                            record.map(|record| PendingCloudUnregister::new(record, None));
                        pending_unregister_recovery_loaded = true;
                    }
                    Err(error) => {
                        let mut blocked = status_error(
                            &SifsMountConfig::default(),
                            None,
                            daemon_pid,
                            restart_count,
                            format!(
                                "recover pending Windows Cloud Files unregister before mount startup: {error}"
                            ),
                        );
                        blocked.unregister_pending = true;
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            blocked,
                        );
                        supervisor_wait(&shutdown);
                        continue;
                    }
                }
            }

            if pending_cloud_unregister.is_some() {
                let now = Instant::now();
                if active.is_some() {
                    let permits_attempt = pending_cloud_unregister
                        .as_ref()
                        .is_some_and(|pending| pending.permits_attempt(now));
                    if permits_attempt
                        && let Err(error) = stop_active_mount(&mut active, &mount_cancellation)
                    {
                        let pending = pending_cloud_unregister
                            .as_mut()
                            .expect("durable unregister remains pending after stop failure");
                        pending.record_failure(error.clone(), now);
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            pending_cloud_unregister_status(
                                pending,
                                daemon_pid,
                                restart_count,
                                format!("stop registered provider before unregister: {error}"),
                            ),
                        );
                    }
                    if active.is_some() {
                        let pending = pending_cloud_unregister
                            .as_ref()
                            .expect("durable unregister remains pending while provider is active");
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            pending_cloud_unregister_status(
                                pending,
                                daemon_pid,
                                restart_count,
                                pending.last_error.clone().unwrap_or_else(|| {
                                    "waiting to retry the registered provider stop".to_string()
                                }),
                            ),
                        );
                        supervisor_wait(&shutdown);
                        continue;
                    }
                    pending_cloud_unregister
                        .as_mut()
                        .expect("durable unregister remains after confirmed stop")
                        .clear_retry();
                }

                if let Err(error) = drive_pending_cloud_unregister_with(
                    &mut pending_cloud_unregister,
                    Instant::now(),
                    |pending| complete_pending_cloud_unregister(data_dir, pending),
                ) {
                    let pending = pending_cloud_unregister
                        .as_ref()
                        .expect("failed unregister remains pending");
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        pending_cloud_unregister_status(pending, daemon_pid, restart_count, error),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
        }

        let config = match store.get_typed::<SifsMountConfig>(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        {
            Ok(config) => config,
            Err(error) => {
                start_retry.clear();
                let stop_error = retire_active_mount(
                    data_dir,
                    &mut active,
                    &mount_cancellation,
                    &mut pending_cloud_unregister,
                    MountRetirementReason::ConfigurationUnavailable,
                )
                .err();
                let error = match stop_error {
                    Some(stop_error) => format!(
                        "read mount configuration: {error}; stop active mount: {stop_error}"
                    ),
                    None => format!("read mount configuration: {error}"),
                };
                publish_status(
                    &store,
                    data_dir,
                    &observed_status,
                    &mut last_persisted_status,
                    status_error(
                        &SifsMountConfig::default(),
                        None,
                        daemon_pid,
                        restart_count,
                        error,
                    ),
                );
                supervisor_wait(&shutdown);
                continue;
            }
        };

        #[cfg(windows)]
        if active.is_none() {
            match resume_active_observed_host_cursor_recovery_cleanup(data_dir, &config) {
                Ok(true) => {
                    // Cleanup is a standalone serialized supervisor turn. Do
                    // not start a mount in the same turn after removing the
                    // durable provider-selection marker.
                    supervisor_wait(&shutdown);
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        status_error(
                            &config,
                            None,
                            daemon_pid,
                            restart_count,
                            format!("resume completed cursor-recovery cleanup: {error}"),
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
        }

        #[cfg(windows)]
        let (cursor_recovery_host_owned, mut cursor_recovery_enable_lease) = if active.is_none() {
            match admit_host_cursor_recovery_enable(data_dir, &config) {
                Ok(admission) => admission,
                Err(error) => {
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        status_error(
                            &config,
                            None,
                            daemon_pid,
                            restart_count,
                            format!("admit cursor-recovery mount enable: {error}"),
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
        } else {
            (false, None)
        };
        #[cfg(not(windows))]
        let cursor_recovery_host_owned = false;
        #[cfg(not(windows))]
        let mut cursor_recovery_enable_lease: Option<SifsMountLease> = None;
        #[cfg(windows)]
        let cursor_recovery_completion_committed = cursor_recovery_host_owned
            && load_host_cursor_recovery_marker_strict(data_dir)
                .ok()
                .flatten()
                .is_some_and(|marker| marker.phase == HostCursorRecoveryPhase::AuthorityFinalized);
        #[cfg(not(windows))]
        let cursor_recovery_completion_committed = false;

        #[cfg(windows)]
        if active.is_none()
            && (!cursor_recovery_host_owned
                || (!config.enabled && !cursor_recovery_completion_committed))
        {
            let cursor_recovery_module = if cursor_recovery_host_owned {
                match load_host_cursor_recovery_marker_strict(data_dir) {
                    Ok(Some(marker)) => Some(marker.module),
                    Ok(None) => None,
                    Err(error) => {
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            status_error(
                                &config,
                                None,
                                daemon_pid,
                                restart_count,
                                format!(
                                    "load pinned cursor-recovery provider for disable: {error}"
                                ),
                            ),
                        );
                        supervisor_wait(&shutdown);
                        continue;
                    }
                }
            } else {
                None
            };
            let staged_recovery = match cursor_recovery_module.as_ref() {
                Some(module) => recover_pending_cloud_unregister_from_staged_with(
                    data_dir,
                    &config,
                    Some(module),
                ),
                None => recover_pending_cloud_unregister_from_staged(data_dir, &config),
            };
            match staged_recovery {
                Ok(Some(record)) => {
                    if let Err(error) = persist_pending_cloud_unregister_record(data_dir, &record) {
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            status_error(
                                &config,
                                Some(&record.request.sync_root),
                                daemon_pid,
                                restart_count,
                                format!(
                                    "persist cleanup for the staged Cloud Files registration: {error}"
                                ),
                            ),
                        );
                        supervisor_wait(&shutdown);
                        continue;
                    }
                    let lease = if cursor_recovery_host_owned {
                        cursor_recovery_enable_lease.take().map(Arc::new)
                    } else {
                        None
                    };
                    pending_cloud_unregister = Some(PendingCloudUnregister::new(record, lease));
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        status_error(
                            &config,
                            None,
                            daemon_pid,
                            restart_count,
                            format!(
                                "recover the staged Cloud Files registration before mount startup: {error}"
                            ),
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
            match recover_pending_cloud_unregister_from_receipt(data_dir, &config) {
                Ok(Some(record)) => {
                    if let Err(error) = persist_pending_cloud_unregister_record(data_dir, &record) {
                        publish_status(
                            &store,
                            data_dir,
                            &observed_status,
                            &mut last_persisted_status,
                            status_error(
                                &config,
                                Some(&record.request.sync_root),
                                daemon_pid,
                                restart_count,
                                format!(
                                    "persist cleanup for the previous Cloud Files registration: {error}"
                                ),
                            ),
                        );
                        supervisor_wait(&shutdown);
                        continue;
                    }
                    pending_cloud_unregister = Some(PendingCloudUnregister::new(record, None));
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        status_error(
                            &config,
                            None,
                            daemon_pid,
                            restart_count,
                            format!(
                                "reconcile the previous Cloud Files registration before mount startup: {error}"
                            ),
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
        }

        if !config.enabled && !cursor_recovery_completion_committed {
            start_retry.clear();
            if let Err(error) = retire_active_mount(
                data_dir,
                &mut active,
                &mount_cancellation,
                &mut pending_cloud_unregister,
                MountRetirementReason::ExplicitDisable,
            ) {
                publish_status(
                    &store,
                    data_dir,
                    &observed_status,
                    &mut last_persisted_status,
                    mount_retirement_failure_status(
                        &pending_cloud_unregister,
                        &config,
                        None,
                        daemon_pid,
                        restart_count,
                        "stop disabled mount",
                        error,
                    ),
                );
                supervisor_wait(&shutdown);
                continue;
            }
            publish_status(
                &store,
                data_dir,
                &observed_status,
                &mut last_persisted_status,
                status_for_config(
                    &config,
                    None,
                    daemon_pid,
                    restart_count,
                    SifsMountRuntimePhase::Disabled,
                    "SIFS host mount is disabled",
                ),
            );
            supervisor_wait(&shutdown);
            continue;
        }

        let mount_point = match validate_mount_config(data_dir, &config) {
            Ok(mount_point) => mount_point,
            Err(validation_error) => {
                start_retry.clear();
                let retirement_reason =
                    active
                        .as_ref()
                        .map_or(MountRetirementReason::InvalidConfiguration, |mount| {
                            if cloud_registration_identity_changed(mount.config(), &config) {
                                MountRetirementReason::RegistrationReplacement
                            } else {
                                MountRetirementReason::InvalidConfiguration
                            }
                        });
                let error = match retire_active_mount(
                    data_dir,
                    &mut active,
                    &mount_cancellation,
                    &mut pending_cloud_unregister,
                    retirement_reason,
                ) {
                    Ok(()) => validation_error,
                    Err(stop_error) => {
                        format!(
                            "{validation_error}; stop mount with invalid configuration: {stop_error}"
                        )
                    }
                };
                publish_status(
                    &store,
                    data_dir,
                    &observed_status,
                    &mut last_persisted_status,
                    mount_retirement_failure_status(
                        &pending_cloud_unregister,
                        &config,
                        None,
                        daemon_pid,
                        restart_count,
                        "invalid mount configuration",
                        error,
                    ),
                );
                supervisor_wait(&shutdown);
                continue;
            }
        };

        if let Some(mount) = active.as_mut() {
            let config_changed = mount.config() != &config;
            let mount_active = mount.is_active();
            if config_changed || !mount_active {
                let retirement_reason = if config_changed {
                    if cloud_registration_identity_changed(mount.config(), &config) {
                        MountRetirementReason::RegistrationReplacement
                    } else {
                        MountRetirementReason::ConfigurationRestart
                    }
                } else {
                    MountRetirementReason::ProviderRestart
                };
                let exit_status = (!mount_active).then(|| {
                    let mut status = mount.status(daemon_pid, restart_count);
                    if status.phase != SifsMountRuntimePhase::Error {
                        status = status_error(
                            mount.config(),
                            Some(mount.mount_point()),
                            daemon_pid,
                            restart_count,
                            "mount adapter exited while the mount remained enabled",
                        );
                    }
                    status
                });
                let exit_error = exit_status
                    .as_ref()
                    .and_then(|status| status.last_error.clone());
                let retirement = match exit_status {
                    Some(status) => retire_after_exit_status_persisted(
                        || {
                            publish_status_before_retirement(
                                &store,
                                data_dir,
                                &observed_status,
                                &mut last_persisted_status,
                                status,
                            )
                        },
                        || {
                            retire_active_mount(
                                data_dir,
                                &mut active,
                                &mount_cancellation,
                                &mut pending_cloud_unregister,
                                retirement_reason,
                            )
                        },
                    ),
                    None => retire_active_mount(
                        data_dir,
                        &mut active,
                        &mount_cancellation,
                        &mut pending_cloud_unregister,
                        retirement_reason,
                    ),
                };
                if let Err(error) = retirement {
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        mount_retirement_failure_status(
                            &pending_cloud_unregister,
                            &config,
                            Some(&mount_point),
                            daemon_pid,
                            restart_count,
                            "previous mount did not stop cleanly",
                            error,
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
                restart_count = restart_count.saturating_add(1);
                if config_changed {
                    start_retry.clear();
                } else if let Some(error) = exit_error {
                    start_retry.record_failure(&config, error, Instant::now());
                }
            }
        }

        if !daemon_is_ready(daemon_paths, daemon_pid) {
            start_retry.clear();
            if let Err(error) = retire_active_mount(
                data_dir,
                &mut active,
                &mount_cancellation,
                &mut pending_cloud_unregister,
                MountRetirementReason::DaemonReadinessLoss,
            ) {
                publish_status(
                    &store,
                    data_dir,
                    &observed_status,
                    &mut last_persisted_status,
                    mount_retirement_failure_status(
                        &pending_cloud_unregister,
                        &config,
                        Some(&mount_point),
                        daemon_pid,
                        restart_count,
                        "stop mount after daemon readiness loss",
                        error,
                    ),
                );
                supervisor_wait(&shutdown);
                continue;
            }
            publish_status(
                &store,
                data_dir,
                &observed_status,
                &mut last_persisted_status,
                status_for_config(
                    &config,
                    Some(&mount_point),
                    daemon_pid,
                    restart_count,
                    SifsMountRuntimePhase::WaitingForDaemon,
                    "waiting for this main daemon to accept object RPCs",
                ),
            );
            supervisor_wait(&shutdown);
            continue;
        }

        if active.is_none() {
            if !start_retry.permits_attempt(&config, Instant::now()) {
                publish_status(
                    &store,
                    data_dir,
                    &observed_status,
                    &mut last_persisted_status,
                    status_error(
                        &config,
                        Some(&mount_point),
                        daemon_pid,
                        restart_count,
                        start_retry
                            .last_error()
                            .unwrap_or("mount start is waiting for its retry deadline"),
                    ),
                );
                supervisor_wait(&shutdown);
                continue;
            }
            publish_status(
                &store,
                data_dir,
                &observed_status,
                &mut last_persisted_status,
                status_for_config(
                    &config,
                    Some(&mount_point),
                    daemon_pid,
                    restart_count,
                    SifsMountRuntimePhase::Starting,
                    "starting the supervised SIFS mount",
                ),
            );
            let owner = format!("managed supervisor mount {}", mount_point.display());
            let start_result = cursor_recovery_enable_lease
                .take()
                .map(Ok)
                .unwrap_or_else(|| SifsMountLease::try_acquire(data_dir, &owner))
                .and_then(|lease| {
                    start_platform_mount(
                        data_dir,
                        &config,
                        &mount_point,
                        Arc::clone(&shutdown),
                        &mount_cancellation,
                        lease,
                    )
                });
            match start_result {
                Ok(mount) => {
                    active = Some(mount);
                }
                Err(error) => {
                    restart_count = restart_count.saturating_add(1);
                    start_retry.record_failure(&config, error.clone(), Instant::now());
                    publish_status(
                        &store,
                        data_dir,
                        &observed_status,
                        &mut last_persisted_status,
                        status_error(
                            &config,
                            Some(&mount_point),
                            daemon_pid,
                            restart_count,
                            error,
                        ),
                    );
                    supervisor_wait(&shutdown);
                    continue;
                }
            }
        }

        if let Some(mount) = active.as_mut() {
            let mut status = mount.status(daemon_pid, restart_count);
            status.configured_mount_point = config.mount_point.clone();
            status.resolved_mount_point = Some(mount_point.display().to_string());
            if mount_runtime_confirms_start(&status) {
                start_retry.clear();
            }
            publish_status(
                &store,
                data_dir,
                &observed_status,
                &mut last_persisted_status,
                status,
            );
        }
        supervisor_wait(&shutdown);
    }

    if active.is_some() {
        let config = active
            .as_ref()
            .map(|mount| mount.config().clone())
            .unwrap_or_default();
        publish_status(
            &store,
            data_dir,
            &observed_status,
            &mut last_persisted_status,
            status_for_config(
                &config,
                active.as_ref().map(|mount| mount.mount_point()),
                daemon_pid,
                restart_count,
                SifsMountRuntimePhase::Stopping,
                "stopping the supervised SIFS mount",
            ),
        );
    }
    if let Err(error) = stop_active_mount(&mut active, &mount_cancellation) {
        let config = active
            .as_ref()
            .map(|mount| mount.config().clone())
            .unwrap_or_default();
        publish_status(
            &store,
            data_dir,
            &observed_status,
            &mut last_persisted_status,
            status_error(
                &config,
                active.as_ref().map(|mount| mount.mount_point()),
                daemon_pid,
                restart_count,
                format!("mount did not stop before supervisor shutdown: {error}"),
            ),
        );
    }
}

#[cfg(not(windows))]
fn handle_cursor_recovery_control_serialized(
    _data_dir: &SinomoDataDir,
    _store: &ConfigStore,
    _active: Option<&ActiveMount>,
    _pending: Option<&PendingCloudUnregister>,
    _request: &SifsMountCursorOnlyRecoveryRequest,
) -> Result<SifsMountCursorOnlyRecoveryResponse, String> {
    Err("cursor-only state recovery is supported only by Windows Cloud Files".to_string())
}

#[cfg(windows)]
fn handle_cursor_recovery_control_serialized(
    data_dir: &SinomoDataDir,
    store: &ConfigStore,
    active: Option<&ActiveMount>,
    pending: Option<&PendingCloudUnregister>,
    request: &SifsMountCursorOnlyRecoveryRequest,
) -> Result<SifsMountCursorOnlyRecoveryResponse, String> {
    match request {
        SifsMountCursorOnlyRecoveryRequest::Inspect => {
            inspect_cursor_recovery_control_serialized(data_dir, store, active, pending)
        }
        SifsMountCursorOnlyRecoveryRequest::Apply {
            expected_token,
            acknowledge_authoritative_full_reconciliation,
        } => apply_cursor_recovery_control_serialized(
            data_dir,
            store,
            active,
            pending,
            expected_token,
            *acknowledge_authoritative_full_reconciliation,
        ),
    }
}

#[cfg(windows)]
struct LoadedCursorRecoveryProvider {
    module: sinomo_runtime::module::NativeModule,
    provenance: CloudFilesModuleProvenance,
    source_path: PathBuf,
}

#[cfg(windows)]
fn load_cursor_recovery_provider(
    data_dir: &SinomoDataDir,
) -> Result<LoadedCursorRecoveryProvider, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
    };
    use sinomo_runtime::module::{
        ModuleId, NativeModule, native_module_cache_path, read_native_dylib_module_loader_state,
    };
    let module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    let state = read_native_dylib_module_loader_state(&data_dir.root(), &module_id)
        .map_err(|error| format!("read Cloud Files module loader state: {error}"))?
        .ok_or_else(|| "Cloud Files module is not active".to_string())?;
    if state.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION {
        return Err(format!(
            "cursor recovery requires Cloud Files interface {SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION}; active interface is {}",
            state.interface_version
        ));
    }
    let path = native_module_cache_path(
        &data_dir.root(),
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let module = unsafe {
        NativeModule::load_process_resident_from_content_addressed_path(&path, &state.content_hash)
    }
    .map_err(|error| format!("load Cloud Files cursor-recovery provider: {error}"))?;
    Ok(LoadedCursorRecoveryProvider {
        provenance: CloudFilesModuleProvenance::from_loader_state(&state),
        module,
        source_path: path,
    })
}

#[cfg(windows)]
fn load_pinned_cursor_recovery_provider(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<LoadedCursorRecoveryProvider, String> {
    use sinomo_runtime::module::NativeModule;
    let source_path = resolve_cloud_unregister_module_path(data_dir, provenance)?;
    let module = unsafe {
        NativeModule::load_process_resident_from_content_addressed_path(
            &source_path,
            &provenance.content_hash,
        )
    }
    .map_err(|error| format!("load pinned Cloud Files cursor-recovery provider: {error}"))?;
    Ok(LoadedCursorRecoveryProvider {
        module,
        provenance: provenance.clone(),
        source_path,
    })
}

#[cfg(windows)]
fn exact_disabled_cursor_recovery_config(
    data_dir: &SinomoDataDir,
    store: &ConfigStore,
    active: Option<&ActiveMount>,
    pending: Option<&PendingCloudUnregister>,
) -> Result<(SifsMountConfig, PathBuf), String> {
    if active.is_some() || pending.is_some() {
        return Err(
            "cursor recovery requires no active mount and no in-memory unregister intent"
                .to_string(),
        );
    }
    let config: SifsMountConfig = store
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| format!("read cursor-recovery mount configuration: {error}"))?;
    if config.enabled {
        return Err("disable the managed SIFS mount before cursor recovery".to_string());
    }
    let mount_point = validate_mount_config(data_dir, &config)?;
    Ok((config, mount_point))
}

#[cfg(windows)]
fn cursor_recovery_module_identity(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    mount_point: PathBuf,
    provenance: &CloudFilesModuleProvenance,
) -> Result<sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION, SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME,
    };
    Ok(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            enabled: false,
            sync_root: mount_point,
            root_path: config.root_path.clone(),
            data_dir_root: data_dir.root(),
            provider_name: SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME.to_string(),
            provider_version: env!("CARGO_PKG_VERSION").to_string(),
            read_only: config.read_only,
            allow_other: config.allow_other,
            sync_local_changes: config.sync_local_changes,
            prepopulate_depth: 0,
            connect: true,
            max_run_seconds: None,
            disabled_config_fingerprint: disabled_mount_config_fingerprint(config)?,
            provider_provenance_fingerprint: cloud_files_provenance_fingerprint(provenance)?,
        },
    )
}

#[cfg(windows)]
fn invoke_cursor_recovery_inspect(
    provider: &LoadedCursorRecoveryProvider,
    identity: sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity,
) -> Result<sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesCursorOnlyRecoveryInspectRequest, CloudFilesModuleRequest,
        CloudFilesModuleResponse, SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
    };
    use sinomo_runtime::module_api::{
        NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
        encode_sobj,
    };
    let payload_sobj = encode_sobj(&CloudFilesModuleRequest::InspectCursorOnlyRecovery(
        CloudFilesCursorOnlyRecoveryInspectRequest { identity },
    ))
    .map_err(|error| format!("encode Cloud Files cursor-recovery inspection: {error}"))?;
    let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION.to_string(),
        payload_sobj,
        request_body: None,
        response_body_path: None,
    });
    let response = admit_cloud_files_provider_before_effect_with(
        &provider.provenance,
        provider.module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
        Some(SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY),
        || {
            provider
                .module
                .invoke(&request)
                .map_err(|error| format!("invoke Cloud Files cursor-recovery inspection: {error}"))
        },
    )?;
    match response {
        NativeModuleResponse::Invocation(response)
            if response.operation
                == SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION =>
        {
            match decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj)
                .map_err(|error| format!("decode cursor-recovery inspection response: {error}"))?
            {
                CloudFilesModuleResponse::CursorOnlyRecoveryInspected(inspection) => Ok(inspection),
                CloudFilesModuleResponse::Error(error) => Err(error.to_string()),
                other => Err(format!(
                    "Cloud Files cursor-recovery inspect returned a cross-paired response: {other:?}"
                )),
            }
        }
        NativeModuleResponse::Error(error) => Err(format!("{}: {}", error.code, error.detail)),
        other => Err(format!(
            "unexpected Cloud Files cursor-recovery inspect response: {other:?}"
        )),
    }
}

#[cfg(windows)]
fn invoke_cursor_recovery_apply(
    provider: &LoadedCursorRecoveryProvider,
    marker: &HostCursorRecoveryMarker,
) -> Result<sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplied, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesCursorOnlyRecoveryApplyRequest, CloudFilesModuleRequest,
        CloudFilesModuleResponse, SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
    };
    use sinomo_runtime::module_api::{
        NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
        encode_sobj,
    };
    let payload_sobj = encode_sobj(&CloudFilesModuleRequest::ApplyCursorOnlyRecovery(
        CloudFilesCursorOnlyRecoveryApplyRequest {
            identity: marker.identity.clone(),
            expected_token: marker.inner_token.clone(),
            recovery_id: marker.recovery_id.clone(),
            acknowledge_authoritative_full_reconciliation: true,
        },
    ))
    .map_err(|error| format!("encode Cloud Files cursor-recovery apply: {error}"))?;
    let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION.to_string(),
        payload_sobj,
        request_body: None,
        response_body_path: None,
    });
    let response = admit_cloud_files_provider_before_effect_with(
        &provider.provenance,
        provider.module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
        Some(SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY),
        || {
            provider
                .module
                .invoke(&request)
                .map_err(|error| format!("invoke Cloud Files cursor-recovery apply: {error}"))
        },
    )?;
    match response {
        NativeModuleResponse::Invocation(response)
            if response.operation
                == SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION =>
        {
            match decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj)
                .map_err(|error| format!("decode cursor-recovery apply response: {error}"))?
            {
                CloudFilesModuleResponse::CursorOnlyRecoveryApplied(applied) => Ok(applied),
                CloudFilesModuleResponse::Error(error) => Err(error.to_string()),
                other => Err(format!(
                    "Cloud Files cursor-recovery apply returned a cross-paired response: {other:?}"
                )),
            }
        }
        NativeModuleResponse::Error(error) => Err(format!("{}: {}", error.code, error.detail)),
        other => Err(format!(
            "unexpected Cloud Files cursor-recovery apply response: {other:?}"
        )),
    }
}

#[cfg(windows)]
fn inspect_cursor_recovery_control_serialized(
    data_dir: &SinomoDataDir,
    store: &ConfigStore,
    active: Option<&ActiveMount>,
    pending: Option<&PendingCloudUnregister>,
) -> Result<SifsMountCursorOnlyRecoveryResponse, String> {
    let (config, mount_point) =
        exact_disabled_cursor_recovery_config(data_dir, store, active, pending)?;
    let (_lease, host_lock_bytes) =
        SifsMountLease::try_acquire_existing_for_cursor_recovery(data_dir)?;
    require_fresh_host_cursor_recovery_absence(data_dir)?;
    let provider = load_cursor_recovery_provider(data_dir)?;
    let identity =
        cursor_recovery_module_identity(data_dir, &config, mount_point, &provider.provenance)?;
    let inspection = invoke_cursor_recovery_inspect(&provider, identity.clone())?;
    // Recheck host-private absence after the read-only module inspection while
    // the same pre-existing host lock remains held.
    require_fresh_host_cursor_recovery_absence(data_dir)?;
    let token = host_cursor_recovery_outer_token(
        &config,
        &provider.provenance,
        &host_lock_bytes,
        &identity,
        &inspection,
    )?;
    Ok(SifsMountCursorOnlyRecoveryResponse::Inspected {
        recovery_token: token,
        raw_state_fingerprint: inspection.raw_state_fingerprint,
        sync_root_file_id: inspection.sync_root_file_id,
        cursor_count: inspection.cursor_count,
        provider_content_hash: provider.provenance.content_hash,
        provider_interface_version: provider.provenance.interface_version,
        detail: inspection.detail,
    })
}

#[cfg(windows)]
fn apply_cursor_recovery_control_serialized(
    data_dir: &SinomoDataDir,
    store: &ConfigStore,
    active: Option<&ActiveMount>,
    pending: Option<&PendingCloudUnregister>,
    expected_token: &str,
    acknowledged: bool,
) -> Result<SifsMountCursorOnlyRecoveryResponse, String> {
    if expected_token.trim().is_empty() || !acknowledged {
        return Err(
            "apply requires the exact inspection token and explicit authoritative reconciliation acknowledgement"
                .to_string(),
        );
    }
    let (config, mount_point) =
        exact_disabled_cursor_recovery_config(data_dir, store, active, pending)?;
    let (_lease, host_lock_bytes) =
        SifsMountLease::try_acquire_existing_for_cursor_recovery(data_dir)?;
    let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    let (mut marker, provider) = if let Some(marker) = marker_snapshot.value {
        validate_current_host_cursor_recovery_authority(
            data_dir,
            &config,
            &host_lock_bytes,
            expected_token,
            &marker,
        )?;
        let bundle = bundle_snapshot.value.ok_or_else(|| {
            "durable cursor-recovery marker lost its first-effect provider bundle".to_string()
        })?;
        validate_host_cursor_recovery_bundle_link(&marker, &bundle)?;
        // All authorization and current-lock checks precede replay mutations.
        promote_host_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        materialize_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        promote_host_cursor_recovery_marker(data_dir, &marker)?;
        let provider = load_pinned_cursor_recovery_provider(data_dir, &marker.module)?;
        (marker, provider)
    } else if let Some(bundle) = bundle_snapshot.value {
        let marker = bundle.requested.clone();
        validate_current_host_cursor_recovery_authority(
            data_dir,
            &config,
            &host_lock_bytes,
            expected_token,
            &marker,
        )?;
        // A crash after the first durable bundle but before marker creation is
        // replayed from its exact bytes, independently of loader/cache drift.
        promote_host_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        materialize_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        persist_host_cursor_recovery_marker(data_dir, &marker)?;
        let provider = load_pinned_cursor_recovery_provider(data_dir, &marker.module)?;
        (marker, provider)
    } else {
        require_fresh_host_cursor_recovery_absence(data_dir)?;
        let provider = load_cursor_recovery_provider(data_dir)?;
        let identity =
            cursor_recovery_module_identity(data_dir, &config, mount_point, &provider.provenance)?;
        let inspection = invoke_cursor_recovery_inspect(&provider, identity.clone())?;
        require_fresh_host_cursor_recovery_absence(data_dir)?;
        let observed_token = host_cursor_recovery_outer_token(
            &config,
            &provider.provenance,
            &host_lock_bytes,
            &identity,
            &inspection,
        )?;
        if observed_token != expected_token {
            return Err(
                "cursor-recovery token no longer matches the exact disabled snapshot".to_string(),
            );
        }
        // Consent/config must still be byte-for-byte the inspected disabled
        // snapshot immediately before the first durable host effect.
        let current: SifsMountConfig = store
            .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
            .map_err(|error| {
                format!("revalidate disabled config before cursor-recovery authorization: {error}")
            })?;
        if current != config || current.enabled {
            return Err(
                "mount configuration changed before cursor-recovery authorization".to_string(),
            );
        }
        require_fresh_host_cursor_recovery_absence(data_dir)?;
        let marker = HostCursorRecoveryMarker {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            disabled_config: config.clone(),
            module: provider.provenance.clone(),
            identity,
            outer_token: observed_token.clone(),
            inner_token: inspection.recovery_token.clone(),
            recovery_id: deterministic_host_cursor_recovery_id(&observed_token),
            inspection,
            applied: None,
            ready_artifacts_fingerprint: None,
            preserved_artifacts_fingerprint: None,
            active_module_marker_fingerprint: None,
            completion_module_marker_fingerprint: None,
            completion_registration_artifacts_fingerprint: None,
            completed_artifacts_fingerprint: None,
            recovery_cycle: 0,
            native_successor_fingerprint: None,
            phase: HostCursorRecoveryPhase::Requested,
        };
        let provider_bytes = read_bounded_plain_host_file(
            &provider.source_path,
            CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES,
            "selected cursor-recovery provider",
        )?
        .ok_or_else(|| "selected cursor-recovery provider disappeared".to_string())?;
        let bundle = HostCursorRecoveryProviderBundle {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            requested: marker.clone(),
            provider_bytes,
            execution_module: None,
            execution_provider_bytes: None,
        };
        // This single canonical bundle is the first durable host effect.
        persist_host_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        materialize_cursor_recovery_provider_bundle(data_dir, &bundle)?;
        persist_host_cursor_recovery_marker(data_dir, &marker)?;
        let pinned = load_pinned_cursor_recovery_provider(data_dir, &marker.module)?;
        (marker, pinned)
    };
    let current: SifsMountConfig = store
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| format!("revalidate disabled cursor-recovery config: {error}"))?;
    if current != marker.disabled_config || current.enabled {
        return Err(
            "mount configuration or local consent changed after cursor-recovery authorization"
                .to_string(),
        );
    }
    if marker.phase == HostCursorRecoveryPhase::Requested {
        let applied = invoke_cursor_recovery_apply(&provider, &marker)?;
        marker.applied = Some(applied);
        marker.phase = HostCursorRecoveryPhase::ModuleApplied;
        persist_host_cursor_recovery_marker(data_dir, &marker)?;
    }
    if marker.phase == HostCursorRecoveryPhase::ModuleApplied {
        let current: SifsMountConfig = store
            .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
            .map_err(|error| {
                format!("revalidate cursor-recovery consent before adoption: {error}")
            })?;
        if current != marker.disabled_config || current.enabled {
            return Err(
                "mount configuration changed after module adoption; retained ownership remains pinned"
                    .to_string(),
            );
        }
        require_physically_absent(
            &pending_cloud_unregister_path(data_dir),
            "pending Cloud Files unregister",
        )?;
        require_physically_absent(
            &cloud_files_registration_receipt_path(data_dir),
            "Cloud Files registration receipt",
        )?;
        let recovery = pending_cloud_unregister_record_with_policy(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                data_dir_root: Some(marker.identity.data_dir_root.clone()),
                provider_name: marker.identity.provider_name.clone(),
                cursor_recovery: Some(
                    sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                        recovery_id: marker.recovery_id.clone(),
                        recovery_token: marker.inner_token.clone(),
                        recovery_cycle: marker.recovery_cycle,
                        native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
                    },
                ),
            },
            marker.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )?;
        let retained = staged_cloud_registration_record(
            &recovery,
            StagedCloudRegistrationState::RetainedAbsent,
        )?;
        match load_staged_cloud_registration_record(data_dir)? {
            None => persist_or_promote_cursor_recovery_staged_record(data_dir, &retained)?,
            Some(existing) if existing == retained => {
                persist_or_promote_cursor_recovery_staged_record(data_dir, &retained)?;
            }
            Some(_) => {
                return Err(
                    "module-applied cursor recovery found changed staged ownership".to_string(),
                );
            }
        }
        marker.phase = HostCursorRecoveryPhase::AdoptedRetained;
        persist_host_cursor_recovery_marker(data_dir, &marker)?;
    }
    let applied = marker
        .applied
        .as_ref()
        .ok_or_else(|| "durable recovery has no matching module apply result".to_string())?;
    Ok(SifsMountCursorOnlyRecoveryResponse::Applied {
        recovery_id: marker.recovery_id.clone(),
        recovery_token: marker.outer_token.clone(),
        sync_root_file_id: applied.sync_root_file_id,
        cursor_count_cleared: applied.cursor_count_cleared,
        detail: "cursor state is adopted and retained while the mount remains disabled; explicitly enable the exact same configuration to run the gated authoritative reconciliation"
            .to_string(),
    })
}

#[cfg(windows)]
fn admit_host_cursor_recovery_enable(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<(bool, Option<SifsMountLease>), String> {
    let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    let Some(mut marker) = marker_snapshot.value else {
        if bundle_snapshot.value.is_some() {
            return Err(
                "cursor-recovery provider authorization exists without its host marker; retry the exact disabled Apply before enabling"
                    .to_string(),
            );
        }
        return Ok((false, None));
    };
    if !matches!(
        marker.phase,
        HostCursorRecoveryPhase::AdoptedRetained
            | HostCursorRecoveryPhase::ArmedAwaitingReady
            | HostCursorRecoveryPhase::ReadyObserved
            | HostCursorRecoveryPhase::ActiveObserved
            | HostCursorRecoveryPhase::AuthorityFinalized
            | HostCursorRecoveryPhase::PreservedRetained
    ) {
        return Err(format!(
            "cursor recovery {} is {:?}, not adopted and ready for mount admission",
            marker.recovery_id, marker.phase
        ));
    }
    let mut expected_enabled = marker.disabled_config.clone();
    expected_enabled.enabled = true;
    let disabled_admission = !config.enabled;
    if disabled_admission && config != &marker.disabled_config {
        return Err(
            "disabled mount configuration drifted from cursor-recovery authorization".to_string(),
        );
    }
    if !disabled_admission && config != &expected_enabled {
        return Err(
            "cursor-recovery mount enable must change only enabled=false to enabled=true"
                .to_string(),
        );
    }
    let (lease, host_lock_bytes) =
        SifsMountLease::try_acquire_existing_for_cursor_recovery(data_dir)?;
    validate_current_host_cursor_recovery_authority(
        data_dir,
        &marker.disabled_config,
        &host_lock_bytes,
        &marker.outer_token,
        &marker,
    )?;
    if load_pending_cloud_unregister_record(data_dir)?.is_some() {
        return Err("cursor-recovery enable found a pending unregister intent".to_string());
    }
    let bundle = match bundle_snapshot.value {
        Some(bundle) => {
            validate_host_cursor_recovery_bundle_link(&marker, &bundle)?;
            Some(bundle)
        }
        _ => {
            return Err(
                "cursor-recovery enable lost its durable provider authorization bundle".to_string(),
            );
        }
    };
    let staged_snapshot = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
    let expected_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            data_dir_root: Some(marker.identity.data_dir_root.clone()),
            provider_name: marker.identity.provider_name.clone(),
            cursor_recovery: Some(
                sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                    recovery_id: marker.recovery_id.clone(),
                    recovery_token: marker.inner_token.clone(),
                    recovery_cycle: marker.recovery_cycle,
                    native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
                },
            ),
        },
        marker.module.clone(),
        CloudUnregisterPolicy::PreserveState,
    )?;
    let expected_retained = staged_cloud_registration_record(
        &expected_recovery,
        StagedCloudRegistrationState::RetainedAbsent,
    )?;
    let expected_may_be_registered = staged_cloud_registration_record(
        &expected_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    if marker.phase == HostCursorRecoveryPhase::AuthorityFinalized {
        if bundle_snapshot.temporary {
            return Err(
                "AuthorityFinalized completion found an unpromoted provider bundle".to_string(),
            );
        }
        let bundle = bundle.as_ref().ok_or_else(|| {
            "AuthorityFinalized completion lost its durable provider bundle".to_string()
        })?;
        let completion = validate_authority_finalized_completion_cut(data_dir, &marker)?;
        if completion.registration_normalized && !completion.stage_normalized {
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &completion.cursor_stage,
                &completion.ordinary_stage,
            )?;
        }
        materialize_cursor_recovery_provider_bundle(data_dir, bundle)?;
        promote_host_cursor_recovery_marker(data_dir, &marker)?;
        return Ok((true, Some(lease)));
    }
    if matches!(
        marker.phase,
        HostCursorRecoveryPhase::ReadyObserved | HostCursorRecoveryPhase::ActiveObserved
    ) {
        if marker.ready_artifacts_fingerprint.as_ref()
            != Some(&cloud_files_ready_artifacts_fingerprint(data_dir)?)
        {
            return Err(
                "cursor-recovery enable found changed Ready receipt/journal bytes".to_string(),
            );
        }
    } else if marker.phase == HostCursorRecoveryPhase::PreservedRetained
        && marker.preserved_artifacts_fingerprint.as_ref()
            != Some(&cloud_files_preserved_artifacts_fingerprint(data_dir)?)
    {
        return Err(
            "cursor-recovery enable found changed preserved StateOwned journal/receipt bytes"
                .to_string(),
        );
    }
    let retained_current = staged_snapshot.current.as_ref() == Some(&expected_retained);
    let armed_current = staged_snapshot.current.as_ref() == Some(&expected_may_be_registered);
    let retained_temp = staged_snapshot.temporary.as_ref() == Some(&expected_retained);
    let armed_temp = staged_snapshot.temporary.as_ref() == Some(&expected_may_be_registered);
    let exact_stage_cut = match marker.phase {
        HostCursorRecoveryPhase::AdoptedRetained | HostCursorRecoveryPhase::PreservedRetained => {
            retained_current && (staged_snapshot.temporary.is_none() || retained_temp)
        }
        HostCursorRecoveryPhase::ArmedAwaitingReady => {
            (retained_current
                && (staged_snapshot.temporary.is_none() || retained_temp || armed_temp))
                || (armed_current && (staged_snapshot.temporary.is_none() || armed_temp))
        }
        HostCursorRecoveryPhase::ReadyObserved | HostCursorRecoveryPhase::ActiveObserved => {
            armed_current && (staged_snapshot.temporary.is_none() || armed_temp)
        }
        _ => false,
    };
    if !exact_stage_cut {
        return Err(format!(
            "cursor-recovery enable found an impossible {:?} host/staged current+temporary ordering",
            marker.phase
        ));
    }
    if disabled_admission {
        // Disabled admission still consumes only exact replayable host/bundle
        // temps under the pre-existing lease. It never arms or starts a worker;
        // Armed/Ready/Active are left for the pinned PreserveState turn, while
        // Adopted/Preserved RetainedAbsent are already parked.
        if let Some(bundle) = bundle.as_ref() {
            promote_host_cursor_recovery_provider_bundle(data_dir, bundle)?;
            materialize_cursor_recovery_provider_bundle(data_dir, bundle)?;
        }
        promote_host_cursor_recovery_marker(data_dir, &marker)?;
        if marker.phase == HostCursorRecoveryPhase::ArmedAwaitingReady && retained_current {
            if retained_temp {
                persist_or_promote_cursor_recovery_staged_record(data_dir, &expected_retained)?;
            }
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &expected_retained,
                &expected_may_be_registered,
            )?;
        }
        return Ok((true, Some(lease)));
    }
    // Every read-only authority, config, and staged-ownership check above must
    // succeed before resuming first-effect artifacts or advancing the host
    // phase. Loader state is intentionally irrelevant after authorization.
    if let Some(bundle) = bundle.as_ref() {
        promote_host_cursor_recovery_provider_bundle(data_dir, bundle)?;
        materialize_cursor_recovery_provider_bundle(data_dir, bundle)?;
    }
    promote_host_cursor_recovery_marker(data_dir, &marker)?;
    let provider = load_pinned_cursor_recovery_provider(data_dir, &marker.module)?;
    use sinomo_runtime::dfs_cloud_files_api::{
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
    };
    admit_cloud_files_provider_before_effect_with(
        &provider.provenance,
        provider.module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        Some(SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY),
        || Ok(()),
    )?;
    for capability in [
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
        SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
    ] {
        if !provider
            .module
            .descriptor()
            .capabilities
            .iter()
            .any(|candidate| candidate == capability)
        {
            return Err(format!(
                "pinned cursor-recovery provider lacks required capability {capability}"
            ));
        }
    }
    let current: SifsMountConfig = sifs_mount_config_store()?
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| format!("revalidate cursor-recovery consent before arming: {error}"))?;
    if current != expected_enabled {
        return Err("mount configuration changed before cursor-recovery arming".to_string());
    }
    if matches!(
        marker.phase,
        HostCursorRecoveryPhase::AdoptedRetained | HostCursorRecoveryPhase::PreservedRetained
    ) {
        if marker.phase == HostCursorRecoveryPhase::AdoptedRetained {
            require_physically_absent(
                &cloud_files_registration_receipt_path(data_dir),
                "pre-enable Cloud Files registration receipt",
            )?;
        }
        marker.phase = HostCursorRecoveryPhase::ArmedAwaitingReady;
        persist_host_cursor_recovery_marker(data_dir, &marker)?;
    }
    let current: SifsMountConfig = sifs_mount_config_store()?
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| {
            format!(
                "revalidate cursor-recovery consent before staged ownership transition: {error}"
            )
        })?;
    if current != expected_enabled {
        return Err(
            "mount configuration changed after cursor-recovery arming and before staged ownership transition"
                .to_string(),
        );
    }
    if retained_temp {
        persist_or_promote_cursor_recovery_staged_record(data_dir, &expected_retained)?;
    }
    transition_or_promote_cursor_recovery_staged_record(
        data_dir,
        &expected_retained,
        &expected_may_be_registered,
    )?;
    Ok((true, Some(lease)))
}

fn supervisor_wait(shutdown: &AtomicBool) {
    if !shutdown.load(Ordering::SeqCst) {
        thread::park_timeout(SUPERVISOR_POLL_INTERVAL);
    }
}

fn daemon_is_ready(paths: &DaemonPaths, daemon_pid: u32) -> bool {
    let Ok(client) =
        DaemonApiClient::connect_for_paths_with_timeout(paths, SUPERVISOR_DAEMON_PROBE_TIMEOUT)
    else {
        return false;
    };
    client.status().is_ok_and(|status| {
        status.pid == daemon_pid
            && status.cluster.as_ref().is_none_or(|cluster| {
                cluster.process == sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
            })
    })
}

fn sifs_mount_config_store() -> Result<ConfigStore, String> {
    let builder = ConfigRegistry::builder()
        .register(sifs_mount_config_definition())
        .map_err(|error| error.to_string())?
        .register(sifs_mount_status_config_definition())
        .map_err(|error| error.to_string())?;
    Ok(ConfigStore::new(builder.build()))
}

pub(crate) fn configure_managed_sifs_mount(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<PathBuf, String> {
    let mount_point = validate_mount_config(data_dir, config)?;
    sifs_mount_config_store()?
        .set_typed(
            data_dir,
            None,
            SIFS_MOUNT_CONFIG_KEY,
            config,
            ConfigWriteTarget::Local,
        )
        .map_err(|error| format!("write managed SIFS mount configuration: {error}"))?;
    Ok(mount_point)
}

pub(crate) fn managed_sifs_mount_config(
    data_dir: &SinomoDataDir,
) -> Result<SifsMountConfig, String> {
    sifs_mount_config_store()?
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| format!("read managed SIFS mount configuration: {error}"))
}

fn publish_status(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    observed_status: &RwLock<SifsMountRuntimeStatus>,
    last_persisted_status: &mut Option<SifsMountRuntimeStatus>,
    status: SifsMountRuntimeStatus,
) {
    publish_status_with(observed_status, last_persisted_status, status, |status| {
        store
            .set_typed(
                data_dir,
                None,
                SIFS_MOUNT_STATUS_CONFIG_KEY,
                status,
                ConfigWriteTarget::Local,
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    });
}

fn publish_status_before_retirement(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    observed_status: &RwLock<SifsMountRuntimeStatus>,
    last_persisted_status: &mut Option<SifsMountRuntimeStatus>,
    status: SifsMountRuntimeStatus,
) -> Result<(), String> {
    let mut persistence_error = None;
    publish_status_with(observed_status, last_persisted_status, status, |status| {
        store
            .set_typed(
                data_dir,
                None,
                SIFS_MOUNT_STATUS_CONFIG_KEY,
                status,
                ConfigWriteTarget::Local,
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
            .inspect_err(|error| persistence_error = Some(error.clone()))
    });
    persistence_error.map_or(Ok(()), Err)
}

fn publish_status_with<F>(
    observed_status: &RwLock<SifsMountRuntimeStatus>,
    last_persisted_status: &mut Option<SifsMountRuntimeStatus>,
    mut status: SifsMountRuntimeStatus,
    persist: F,
) where
    F: FnOnce(&SifsMountRuntimeStatus) -> Result<(), String>,
{
    let observation_changed = observed_status.read().map_or(true, |previous| {
        status_without_timestamp(&previous) != status_without_timestamp(&status)
    });
    let persistence_changed = last_persisted_status
        .as_ref()
        .is_none_or(|previous| status_persistence_key(previous) != status_persistence_key(&status));
    if !observation_changed && !persistence_changed {
        return;
    }
    if observation_changed {
        status.updated_at = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_default();
    } else if let Ok(previous) = observed_status.read() {
        status.updated_at.clone_from(&previous.updated_at);
    }
    if persistence_changed {
        if let Err(error) = persist(&status) {
            tracing::warn!("failed to publish SIFS mount status: {error}");
        } else {
            *last_persisted_status = Some(status.clone());
        }
    }
    if observation_changed {
        publish_status_without_store(observed_status, status);
    }
}

fn publish_status_without_store(
    observed_status: &RwLock<SifsMountRuntimeStatus>,
    status: SifsMountRuntimeStatus,
) {
    if let Ok(mut observed) = observed_status.write() {
        *observed = status;
    }
}

fn status_without_timestamp(status: &SifsMountRuntimeStatus) -> SifsMountRuntimeStatus {
    let mut status = status.clone();
    status.updated_at.clear();
    status
}

fn status_persistence_key(status: &SifsMountRuntimeStatus) -> SifsMountRuntimeStatus {
    let mut status = status_without_timestamp(status);
    // Heartbeats are liveness evidence for the in-process status route, but persisting a new
    // local configuration document every second would turn an idle mount into unbounded disk
    // churn. State transitions and queue counters still remain persistence-sensitive.
    status.worker_health_generation = 0;
    status.last_worker_heartbeat_at = None;
    status
}

fn mount_runtime_confirms_start(status: &SifsMountRuntimeStatus) -> bool {
    if status.last_error.is_some() {
        return false;
    }
    match status.platform {
        SifsMountPlatform::UnixFuse => status.phase == SifsMountRuntimePhase::Mounted,
        SifsMountPlatform::WindowsCloudFiles => {
            status.phase == SifsMountRuntimePhase::Connected
                && status.provider_registered
                && status.provider_connected
                && status.worker_alive
                && status.worker_ready
                && status.worker_health_generation > 0
        }
        SifsMountPlatform::Unsupported => false,
    }
}

fn status_for_config(
    config: &SifsMountConfig,
    mount_point: Option<&Path>,
    daemon_pid: u32,
    restart_count: u64,
    phase: SifsMountRuntimePhase,
    detail: impl Into<String>,
) -> SifsMountRuntimeStatus {
    SifsMountRuntimeStatus {
        phase,
        platform: SifsMountPlatform::current(),
        configured_mount_point: config.mount_point.clone(),
        resolved_mount_point: mount_point.map(|path| path.display().to_string()),
        root_path: config.root_path.clone(),
        read_only: config.read_only,
        sync_local_changes: config.sync_local_changes,
        writeback_mode: match (SifsMountPlatform::current(), config.enabled, phase) {
            (SifsMountPlatform::WindowsCloudFiles, true, SifsMountRuntimePhase::Error) => {
                SifsMountWritebackMode::Blocked
            }
            (SifsMountPlatform::WindowsCloudFiles, true, _) => SifsMountWritebackMode::Starting,
            _ => SifsMountWritebackMode::Disabled,
        },
        provider_registered: false,
        provider_connected: false,
        worker_alive: false,
        worker_ready: false,
        worker_health_generation: 0,
        last_worker_heartbeat_at: None,
        pending_operations: 0,
        retrying_operations: 0,
        failed_operations: 0,
        conflict_count: 0,
        attention: Vec::new(),
        attention_overflow: 0,
        unregister_pending: false,
        daemon_pid: Some(daemon_pid),
        provider_content_hash: None,
        provider_interface_version: None,
        placeholders_created: None,
        restart_count,
        last_error: None,
        detail: detail.into(),
        updated_at: String::new(),
    }
}

fn status_error(
    config: &SifsMountConfig,
    mount_point: Option<&Path>,
    daemon_pid: u32,
    restart_count: u64,
    error: impl Into<String>,
) -> SifsMountRuntimeStatus {
    let error = error.into();
    let mut status = status_for_config(
        config,
        mount_point,
        daemon_pid,
        restart_count,
        SifsMountRuntimePhase::Error,
        format!("SIFS mount failed: {error}"),
    );
    status.last_error = Some(error);
    status
}

fn pending_cloud_unregister_status(
    pending: &PendingCloudUnregister,
    daemon_pid: u32,
    restart_count: u64,
    error: impl Into<String>,
) -> SifsMountRuntimeStatus {
    let config = SifsMountConfig {
        enabled: false,
        mount_point: pending.record.request.sync_root.display().to_string(),
        root_path: pending.record.request.root_path.clone(),
        read_only: true,
        allow_other: false,
        sync_local_changes: false,
    };
    let mut status = status_error(
        &config,
        Some(&pending.record.request.sync_root),
        daemon_pid,
        restart_count,
        format!(
            "unregister retired Windows Cloud Files sync root: {}",
            error.into()
        ),
    );
    status.unregister_pending = true;
    status
}

fn mount_retirement_failure_status(
    pending: &Option<PendingCloudUnregister>,
    fallback_config: &SifsMountConfig,
    fallback_mount_point: Option<&Path>,
    daemon_pid: u32,
    restart_count: u64,
    context: &str,
    error: String,
) -> SifsMountRuntimeStatus {
    if let Some(pending) = pending.as_ref() {
        pending_cloud_unregister_status(pending, daemon_pid, restart_count, error)
    } else {
        status_error(
            fallback_config,
            fallback_mount_point,
            daemon_pid,
            restart_count,
            format!("{context}: {error}"),
        )
    }
}

fn validate_mount_config(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<PathBuf, String> {
    if config.mount_point.trim() != config.mount_point {
        return Err("mount point may not have surrounding whitespace".to_string());
    }
    let root_path = config.root_path.trim();
    if root_path != config.root_path || !root_path.starts_with('/') {
        return Err("SIFS root_path must be absolute".to_string());
    }
    if root_path.contains('\0')
        || root_path.contains('\\')
        || (root_path != "/" && root_path.ends_with('/'))
        || (root_path != "/"
            && root_path
                .split('/')
                .skip(1)
                .any(|component| component.is_empty() || component == "." || component == ".."))
    {
        return Err(
            "SIFS root_path must be normalized and may not contain NUL, backslash, `.`, `..`, `//`, or a trailing slash"
                .to_string(),
        );
    }
    #[cfg(windows)]
    if config.allow_other {
        return Err("allow_other is only supported by Unix FUSE".to_string());
    }
    #[cfg(windows)]
    if config.read_only || !config.sync_local_changes {
        return Err(
            "Windows Cloud Files requires durable writable synchronization: read_only=false and sync_local_changes=true"
                .to_string(),
        );
    }
    #[cfg(not(windows))]
    if config.sync_local_changes {
        return Err("sync_local_changes is only supported by Windows Cloud Files".to_string());
    }
    let mount_point = expand_mount_point(&config.mount_point)?;
    let mount_root = lexical_absolute_path(&mount_point)?;
    if mount_root == Path::new("/") || mount_root.parent().is_none() {
        return Err("the host filesystem root may not be used as a SIFS mount point".to_string());
    }
    let data_root = canonical_absolute_path_allow_missing(&data_dir.root())?;
    let resolved_mount_root = canonical_absolute_path_allow_missing(&mount_root)?;
    if resolved_mount_root == Path::new("/") || resolved_mount_root.parent().is_none() {
        return Err("the host filesystem root may not be used as a SIFS mount point".to_string());
    }
    if resolved_mount_root.starts_with(&data_root) || data_root.starts_with(&resolved_mount_root) {
        return Err(format!(
            "mount point {} overlaps the Sinomo data directory {}",
            resolved_mount_root.display(),
            data_root.display()
        ));
    }
    Ok(mount_root)
}

fn expand_mount_point(value: &str) -> Result<PathBuf, String> {
    if value == "~" || value.starts_with("~/") || value.starts_with("~\\") {
        let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .ok_or_else(|| "current user home directory is unavailable".to_string())?;
        let suffix = value
            .strip_prefix("~/")
            .or_else(|| value.strip_prefix("~\\"))
            .unwrap_or("");
        return Ok(PathBuf::from(home).join(suffix));
    }
    if value.starts_with('~') {
        return Err("only the current-user `~/` mount path form is supported".to_string());
    }
    Ok(PathBuf::from(value))
}

fn lexical_absolute_path(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("mount point {} must be absolute", path.display()));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(format!(
                        "mount point {} escapes the filesystem root",
                        path.display()
                    ));
                }
            }
            Component::Normal(component) => normalized.push(component),
        }
    }
    Ok(normalized)
}

fn canonical_absolute_path_allow_missing(path: &Path) -> Result<PathBuf, String> {
    let normalized = lexical_absolute_path(path)?;
    let mut candidate = normalized.as_path();
    let mut missing_suffix = Vec::new();
    loop {
        match std::fs::canonicalize(candidate) {
            Ok(mut resolved) => {
                for component in missing_suffix.iter().rev() {
                    resolved.push(component);
                }
                return lexical_absolute_path(&resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(candidate) {
                    Ok(_) => {
                        return Err(format!(
                            "resolve existing host path {}: {error}",
                            candidate.display()
                        ));
                    }
                    Err(metadata_error)
                        if metadata_error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(metadata_error) => {
                        return Err(format!(
                            "inspect host path {}: {metadata_error}",
                            candidate.display()
                        ));
                    }
                }
                let component = candidate.file_name().ok_or_else(|| {
                    format!(
                        "no existing ancestor for host path {}",
                        normalized.display()
                    )
                })?;
                missing_suffix.push(component.to_os_string());
                candidate = candidate.parent().ok_or_else(|| {
                    format!(
                        "no existing ancestor for host path {}",
                        normalized.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "resolve host path {}: {error}",
                    candidate.display()
                ));
            }
        }
    }
}

fn stop_active_mount(
    active: &mut Option<ActiveMount>,
    mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
) -> Result<(), String> {
    stop_active_mount_with_identity(active, mount_cancellation).map(|_| ())
}

fn stop_active_mount_with_identity(
    active: &mut Option<ActiveMount>,
    mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
) -> Result<Option<RetiredMount>, String> {
    let retired = active.as_ref().map(|mount| RetiredMount {
        config: mount.config().clone(),
        mount_point: mount.mount_point().to_path_buf(),
        #[cfg(windows)]
        lease: Some(Arc::clone(&mount.lease)),
        #[cfg(windows)]
        module_provenance: mount.module_provenance.clone(),
    });
    if let Ok(cancellation) = mount_cancellation.lock()
        && let Some(cancellation) = cancellation.as_ref()
    {
        cancellation.cancel();
    }
    if let Some(mount) = active.as_mut() {
        mount.stop()?;
    }
    active.take();
    if let Ok(mut cancellation) = mount_cancellation.lock() {
        *cancellation = None;
    }
    Ok(retired)
}

fn retire_active_mount(
    data_dir: &SinomoDataDir,
    active: &mut Option<ActiveMount>,
    mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
    pending_unregister: &mut Option<PendingCloudUnregister>,
    reason: MountRetirementReason,
) -> Result<(), String> {
    if pending_unregister.is_some() {
        return Err("a previous Cloud Files unregister is still pending".to_string());
    }
    #[cfg(windows)]
    let authority_finalized_worker_finished = if let Some(mount) = active.as_ref() {
        let operation_fence = Arc::clone(&mount.operation_fence);
        let _operation = operation_fence.lock().map_err(|_| {
            "cursor-recovery operation fence is poisoned during terminal retirement".to_string()
        })?;
        let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
        if let Some(marker) = marker_snapshot.value.as_ref()
            && marker.phase == HostCursorRecoveryPhase::AuthorityFinalized
        {
            if marker.module != mount.module_provenance
                || marker.identity.sync_root != mount.mount_point
                || marker.identity.root_path != mount.config.root_path
                || marker.identity.data_dir_root != data_dir.root()
            {
                return Err(
                    "active mount no longer matches its irreversible AuthorityFinalized recovery"
                        .to_string(),
                );
            }
            let bundle_snapshot =
                inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
            if bundle_snapshot.temporary {
                return Err(
                    "AuthorityFinalized retirement found an unpromoted provider bundle".to_string(),
                );
            }
            let module_marker_path = cloud_files_module_runtime_path(
                data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            );
            match std::fs::symlink_metadata(&module_marker_path) {
                Ok(_) => {
                    let bundle = bundle_snapshot.value.as_ref().ok_or_else(|| {
                        "AuthorityFinalized retirement lost its durable provider bundle".to_string()
                    })?;
                    validate_host_cursor_recovery_bundle_link(marker, bundle)?;
                    validate_authority_finalized_completion_cut(data_dir, marker)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if let Some(bundle) = bundle_snapshot.value.as_ref() {
                        validate_host_cursor_recovery_bundle_link(marker, bundle)?;
                    }
                    validate_authority_finalized_released_cut(data_dir, marker)?;
                }
                Err(error) => {
                    return Err(format!(
                        "inspect AuthorityFinalized native marker during retirement: {error}"
                    ));
                }
            }
            if marker_snapshot.temporary {
                promote_host_cursor_recovery_marker(data_dir, marker)?;
            }
            Some(
                mount.finished_result.is_some()
                    || mount
                        .thread
                        .as_ref()
                        .is_none_or(|thread| thread.is_finished()),
            )
        } else {
            None
        }
    } else {
        None
    };
    #[cfg(windows)]
    if let Some(worker_finished) = authority_finalized_worker_finished {
        if !worker_finished {
            // AF is the irreversible winner. A live provider remains parked
            // and its callback completes normalization/authority release.
            return Ok(());
        }
        // The fence was released above. Retire a failed/exited provider with
        // no U/Preserve transition so the next turn can reacquire the exact
        // existing lease and resume pinned AF completion.
        return stop_active_mount(active, mount_cancellation);
    }
    #[cfg(windows)]
    if matches!(
        reason,
        MountRetirementReason::ExplicitDisable | MountRetirementReason::RegistrationReplacement
    ) && let Some(mount) = active.as_ref()
    {
        let operation_fence = Arc::clone(&mount.operation_fence);
        let retired = RetiredMount {
            config: mount.config().clone(),
            mount_point: mount.mount_point().to_path_buf(),
            lease: Some(Arc::clone(&mount.lease)),
            module_provenance: mount.module_provenance.clone(),
        };
        let preparation = (|| {
            let _operation = operation_fence.lock().map_err(|_| {
                "cursor-recovery operation fence is poisoned during retirement".to_string()
            })?;
            if reason == MountRetirementReason::ExplicitDisable {
                let mut expected_disabled = retired.config.clone();
                expected_disabled.enabled = false;
                let observed: SifsMountConfig = sifs_mount_config_store()?
                    .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
                    .map_err(|error| {
                        format!("revalidate disabled mount consent under retirement fence: {error}")
                    })?;
                if observed != expected_disabled {
                    return Err(
                        "mount configuration changed before durable PreserveState intent"
                            .to_string(),
                    );
                }
            }
            let host_recovery = load_host_cursor_recovery_marker_strict(data_dir)?;
            let mut unregister_request = cloud_unregister_request(data_dir, &retired);
            if let Some(marker) = host_recovery.as_ref() {
                if marker.module != retired.module_provenance
                    || marker.identity.sync_root != retired.mount_point
                    || marker.identity.root_path != retired.config.root_path
                    || marker.identity.data_dir_root != data_dir.root()
                    || !matches!(
                        marker.phase,
                        HostCursorRecoveryPhase::ArmedAwaitingReady
                            | HostCursorRecoveryPhase::ReadyObserved
                            | HostCursorRecoveryPhase::ActiveObserved
                    )
                {
                    return Err(
                        "active mount no longer matches its exact cursor-recovery retirement authority"
                            .to_string(),
                    );
                }
                let bundle_snapshot =
                    inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
                if bundle_snapshot.temporary {
                    return Err(
                        "cursor-recovery retirement found an unpromoted provider bundle"
                            .to_string(),
                    );
                }
                let bundle = bundle_snapshot.value.as_ref().ok_or_else(|| {
                    "cursor-recovery retirement lost its durable provider bundle".to_string()
                })?;
                validate_host_cursor_recovery_bundle_link(marker, bundle)?;
                unregister_request.cursor_recovery = Some(host_cursor_recovery_context(marker));
            }
            let mut expected_record = pending_cloud_unregister_record_with_policy(
                unregister_request,
                retired.module_provenance.clone(),
                CloudUnregisterPolicy::Release,
            )?;
            let existing_stage = load_staged_cloud_registration_record(data_dir)?;
            match existing_stage.as_ref() {
                Some(staged) => {
                    expected_record.legacy_prejournal_origin =
                        staged.recovery.legacy_prejournal_origin.clone();
                    if !same_cloud_registration_recovery_identity(
                        &staged.recovery,
                        &expected_record,
                    ) {
                        return Err(format!(
                            "staged Cloud Files registration recovery identifies {} -> {} with provider {}, but the active mount identifies {} -> {} with provider {}; ownership remains pinned",
                            staged.recovery.request.sync_root.display(),
                            staged.recovery.request.root_path,
                            staged.recovery.module.content_hash,
                            expected_record.request.sync_root.display(),
                            expected_record.request.root_path,
                            expected_record.module.content_hash
                        ));
                    }
                }
                None => {
                    if reason == MountRetirementReason::RegistrationReplacement {
                        return Err(
                            "Cloud Files registration replacement found no exact staged old-provider ownership; stopping the active host while leaving all other evidence untouched"
                                .to_string(),
                        );
                    }
                    let staged = staged_cloud_registration_record(
                        &expected_record,
                        StagedCloudRegistrationState::MayBeRegistered,
                    )?;
                    persist_staged_cloud_registration_record(data_dir, &staged)?;
                }
            }
            if reason == MountRetirementReason::RegistrationReplacement {
                return Err(
                    "Cloud Files registration replacement is blocked until an explicit release-if-quiescent operation is available; retained old registration ownership was not altered"
                        .to_string(),
                );
            }
            ensure_cloud_unregister_recovery_module(data_dir, &expected_record.module)?;
            if expected_record.module.interface_version
                != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
            {
                return Err(format!(
                    "Cloud Files provider interface {} cannot preserve state during disable; registration and durable ownership remain retained",
                    expected_record.module.interface_version
                ));
            }
            let mut record = pending_cloud_unregister_record_with_policy(
                expected_record.request,
                expected_record.module,
                CloudUnregisterPolicy::PreserveState,
            )?;
            record.legacy_prejournal_origin = expected_record.legacy_prejournal_origin;
            if host_recovery.is_some() {
                let expected_stage = staged_cloud_registration_record(
                    &record,
                    StagedCloudRegistrationState::MayBeRegistered,
                )?;
                let stage_snapshot = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
                if stage_snapshot.current.as_ref() != Some(&expected_stage)
                    || stage_snapshot
                        .temporary
                        .as_ref()
                        .is_some_and(|temporary| temporary != &expected_stage)
                {
                    return Err(
                        "cursor-recovery retirement lost its exact full-context MayBeRegistered stage"
                            .to_string(),
                    );
                }
                if stage_snapshot.temporary.is_some() {
                    persist_or_promote_cursor_recovery_staged_record(data_dir, &expected_stage)?;
                }
            }
            // Write the exact preserve-state intent before stopping. A crash
            // after this point is replayable but cannot race the live worker.
            persist_pending_cloud_unregister_record(data_dir, &record)?;
            Ok(record)
        })();

        match preparation {
            Ok(record) => {
                *pending_unregister = Some(PendingCloudUnregister::new(record, retired.lease));
                return stop_then_drive_pending_cloud_unregister_with(
                    pending_unregister,
                    Instant::now(),
                    || stop_active_mount(active, mount_cancellation),
                    |pending| complete_pending_cloud_unregister(data_dir, pending),
                );
            }
            Err(preparation_error) => {
                // Consent revocation always attempts a clean disconnect even
                // when provenance/persistence cannot prepare a safe cleanup.
                return stop_after_failed_cloud_retirement_preparation_with(
                    preparation_error,
                    || stop_active_mount(active, mount_cancellation),
                    || Err("blocked Cloud Files retirement cannot invoke unregister".to_string()),
                    || Err("blocked Cloud Files retirement cannot start a replacement".to_string()),
                );
            }
        }
    }
    let retired = stop_active_mount_with_identity(active, mount_cancellation)?;
    #[cfg(not(windows))]
    let _ = (data_dir, pending_unregister, reason, retired);
    #[cfg(windows)]
    let _ = retired;
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
struct ActiveMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
    handle: Option<sinomo_dfs_fuse::DfsFuseMountHandle>,
    stop_error: Option<String>,
    _lease: SifsMountLease,
}

#[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
impl ActiveMount {
    fn config(&self) -> &SifsMountConfig {
        &self.config
    }

    fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    fn is_active(&mut self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| handle.is_active())
    }

    fn status(&mut self, daemon_pid: u32, restart_count: u64) -> SifsMountRuntimeStatus {
        unix_fuse_mount_status(
            &self.config,
            &self.mount_point,
            daemon_pid,
            restart_count,
            self.handle
                .as_ref()
                .and_then(sinomo_dfs_fuse::DfsFuseMountHandle::fatal_error),
        )
    }

    fn stop(&mut self) -> Result<(), String> {
        if let Some(error) = self.stop_error.as_ref() {
            return Err(error.clone());
        }
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle.shutdown().map_err(|error| {
            let error = format!("shut down supervised SIFS FUSE mount: {error}");
            self.stop_error = Some(error.clone());
            error
        })
    }
}

#[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
fn unix_fuse_mount_status(
    config: &SifsMountConfig,
    mount_point: &Path,
    daemon_pid: u32,
    restart_count: u64,
    fatal_error: Option<String>,
) -> SifsMountRuntimeStatus {
    match fatal_error {
        Some(error) => status_error(config, Some(mount_point), daemon_pid, restart_count, error),
        None => status_for_config(
            config,
            Some(mount_point),
            daemon_pid,
            restart_count,
            SifsMountRuntimePhase::Mounted,
            "SIFS is mounted through Unix FUSE",
        ),
    }
}

#[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
fn start_platform_mount(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    mount_point: &Path,
    _shutdown: Arc<AtomicBool>,
    mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
    lease: SifsMountLease,
) -> Result<ActiveMount, String> {
    let mut options = sinomo_dfs_fuse::DfsFuseMountOptions::new(mount_point)
        .with_data_dir(data_dir.root())
        .with_root_path(&config.root_path);
    options.read_only = config.read_only;
    options.allow_other = config.allow_other;
    let cancellation = sinomo_runtime::dfs::DfsCancellationFlag::new();
    if let Ok(mut current) = mount_cancellation.lock() {
        *current = Some(cancellation.clone());
    }
    let handle = sinomo_dfs_fuse::mount_sifs_background_with_cancellation(options, cancellation)
        .map_err(|error| format!("start Unix FUSE mount: {error}"))?;
    Ok(ActiveMount {
        config: config.clone(),
        mount_point: mount_point.to_path_buf(),
        handle: Some(handle),
        stop_error: None,
        _lease: lease,
    })
}

#[cfg(all(target_os = "linux", not(feature = "dfs-fuse")))]
struct ActiveMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
}

#[cfg(all(target_os = "linux", not(feature = "dfs-fuse")))]
impl ActiveMount {
    fn config(&self) -> &SifsMountConfig {
        &self.config
    }
    fn mount_point(&self) -> &Path {
        &self.mount_point
    }
    fn is_active(&mut self) -> bool {
        false
    }
    fn status(&mut self, daemon_pid: u32, restart_count: u64) -> SifsMountRuntimeStatus {
        status_for_config(
            &self.config,
            Some(&self.mount_point),
            daemon_pid,
            restart_count,
            SifsMountRuntimePhase::Error,
            "Unix FUSE support is disabled in this build",
        )
    }
    fn stop(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(all(target_os = "linux", not(feature = "dfs-fuse")))]
fn start_platform_mount(
    _data_dir: &SinomoDataDir,
    _config: &SifsMountConfig,
    _mount_point: &Path,
    _shutdown: Arc<AtomicBool>,
    _mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
    _lease: SifsMountLease,
) -> Result<ActiveMount, String> {
    Err("Unix FUSE support is disabled in this build".to_string())
}

#[cfg(windows)]
struct ActiveMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
    cancel: Arc<AtomicBool>,
    events: mpsc::Receiver<sinomo_runtime::dfs_cloud_files_api::CloudFilesMountLifecycleEvent>,
    finished: mpsc::Receiver<Result<(), String>>,
    thread: Option<thread::JoinHandle<()>>,
    lifecycle: Option<sinomo_runtime::dfs_cloud_files_api::CloudFilesMountLifecycleEvent>,
    finished_result: Option<Result<(), String>>,
    provider_content_hash: String,
    provider_interface_version: String,
    module_provenance: CloudFilesModuleProvenance,
    lease: Arc<SifsMountLease>,
    /// Serializes provider-thread recovery acknowledgements with the
    /// supervisor's durable retirement intent. It is deliberately not held
    /// while stopping/joining the provider, because callbacks may still need
    /// to finish before the worker can exit.
    operation_fence: Arc<Mutex<()>>,
    last_health_generation: u64,
    last_health_observed: Option<Instant>,
    last_health_observed_at: Option<String>,
}

#[cfg(any(windows, test))]
fn poll_cloud_files_completion(
    finished: &mpsc::Receiver<Result<(), String>>,
    worker_finished: bool,
) -> Option<Result<(), String>> {
    match finished.try_recv() {
        Ok(result) => Some(result),
        Err(mpsc::TryRecvError::Empty) => None,
        Err(mpsc::TryRecvError::Disconnected) => Some(Err(if worker_finished {
            "Cloud Files provider worker exited without a terminal result".to_string()
        } else {
            "Cloud Files provider result channel disconnected before the worker stopped".to_string()
        })),
    }
}

#[cfg(any(windows, test))]
fn cloud_files_completion_error(result: Option<&Result<(), String>>) -> Option<String> {
    result.map(|result| match result {
        Ok(()) => "Cloud Files provider exited while the mount remained enabled".to_string(),
        Err(error) => error.clone(),
    })
}

#[cfg(any(windows, test))]
fn cloud_files_attention_summary(
    summary: sinomo_runtime::dfs_cloud_files_api::CloudFilesSyncAttentionSummary,
) -> SifsMountAttentionSummary {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesSyncAttentionKind, CloudFilesSyncAttentionNextAction,
    };

    SifsMountAttentionSummary {
        kind: match summary.kind {
            CloudFilesSyncAttentionKind::LocalOperationConflict => {
                SifsMountAttentionKind::LocalOperationConflict
            }
            CloudFilesSyncAttentionKind::FailedLocalOperation => {
                SifsMountAttentionKind::FailedLocalOperation
            }
            CloudFilesSyncAttentionKind::RemoteNamespaceConflict => {
                SifsMountAttentionKind::RemoteNamespaceConflict
            }
            CloudFilesSyncAttentionKind::StartupConflict => SifsMountAttentionKind::StartupConflict,
            CloudFilesSyncAttentionKind::UnsupportedObjectKind => {
                SifsMountAttentionKind::UnsupportedObjectKind
            }
            CloudFilesSyncAttentionKind::StaleOrAmbiguousReconciliation => {
                SifsMountAttentionKind::StaleOrAmbiguousReconciliation
            }
        },
        path: summary.path,
        reason_code: summary.reason_code,
        reason: summary.reason,
        retained_local_evidence: summary.retained_local_evidence.map(|evidence| {
            SifsMountRetainedLocalEvidence {
                operation_id: evidence.operation_id,
                evidence_id: evidence.evidence_id,
                staged_content_hash: evidence.staged_content_hash,
                staged_content_size_bytes: evidence.staged_content_size_bytes,
                local_state: evidence.local_state,
            }
        }),
        safe_next_action: match summary.safe_next_action {
            CloudFilesSyncAttentionNextAction::ReviewAndResolve => {
                SifsMountAttentionNextAction::ReviewAndResolve
            }
            CloudFilesSyncAttentionNextAction::RepairCauseAndRetry => {
                SifsMountAttentionNextAction::RepairCauseAndRetry
            }
            CloudFilesSyncAttentionNextAction::PreserveLocalCopyAndRescan => {
                SifsMountAttentionNextAction::PreserveLocalCopyAndRescan
            }
            CloudFilesSyncAttentionNextAction::ReviewUnsupportedEntry => {
                SifsMountAttentionNextAction::ReviewUnsupportedEntry
            }
            CloudFilesSyncAttentionNextAction::WaitForAuthoritativeReconciliation => {
                SifsMountAttentionNextAction::WaitForAuthoritativeReconciliation
            }
        },
    }
}

#[cfg(windows)]
impl ActiveMount {
    fn config(&self) -> &SifsMountConfig {
        &self.config
    }

    fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    fn refresh(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            self.provider_interface_version = event.interface_version.clone();
            if self.last_health_observed.is_none()
                || event.health_generation > self.last_health_generation
            {
                self.last_health_generation = event.health_generation;
                self.last_health_observed = Some(Instant::now());
                self.last_health_observed_at = OffsetDateTime::now_utc().format(&Rfc3339).ok();
            }
            self.lifecycle = Some(event);
        }
        if self.finished_result.is_none() {
            let worker_finished = self
                .thread
                .as_ref()
                .is_none_or(|thread| thread.is_finished());
            self.finished_result = poll_cloud_files_completion(&self.finished, worker_finished);
        }
    }

    fn is_active(&mut self) -> bool {
        self.refresh();
        self.finished_result.is_none()
    }

    fn status(&mut self, daemon_pid: u32, restart_count: u64) -> SifsMountRuntimeStatus {
        use sinomo_runtime::dfs_cloud_files_api::{
            CloudFilesLocalChangeSyncMode, CloudFilesMountLifecyclePhase,
        };

        self.refresh();
        let (phase, detail, last_error, placeholders_created) = match self.lifecycle.as_ref() {
            Some(event) => {
                let last_error = event.error.as_ref().map(ToString::to_string);
                let phase = match event.phase {
                    CloudFilesMountLifecyclePhase::Registered => SifsMountRuntimePhase::Registered,
                    CloudFilesMountLifecyclePhase::Connected => SifsMountRuntimePhase::Connected,
                    CloudFilesMountLifecyclePhase::Disconnected => SifsMountRuntimePhase::Stopping,
                    CloudFilesMountLifecyclePhase::Unregistered => SifsMountRuntimePhase::Stopping,
                    CloudFilesMountLifecyclePhase::Failed => SifsMountRuntimePhase::Error,
                };
                (
                    phase,
                    event.detail.clone(),
                    last_error,
                    Some(event.placeholders_created),
                )
            }
            None => (
                SifsMountRuntimePhase::Starting,
                "loading the Windows Cloud Files provider".to_string(),
                None,
                None,
            ),
        };
        let mut status = status_for_config(
            &self.config,
            Some(&self.mount_point),
            daemon_pid,
            restart_count,
            phase,
            detail,
        );
        status.provider_content_hash = Some(self.provider_content_hash.clone());
        status.provider_interface_version = Some(self.provider_interface_version.clone());
        status.placeholders_created = placeholders_created;
        if let Some(event) = self.lifecycle.as_ref() {
            status.provider_registered = event.registered;
            status.provider_connected = event.connected;
            status.worker_alive = event.worker_alive;
            status.worker_ready = event.worker_ready;
            status.worker_health_generation = event.health_generation;
            status.last_worker_heartbeat_at = self.last_health_observed_at.clone();
            status.writeback_mode = match event.local_change_sync.mode {
                CloudFilesLocalChangeSyncMode::Disabled => SifsMountWritebackMode::Disabled,
                CloudFilesLocalChangeSyncMode::Unsupported => SifsMountWritebackMode::Unsupported,
                CloudFilesLocalChangeSyncMode::ReadOnlyEnforcementUnavailable => {
                    SifsMountWritebackMode::ReadOnlyEnforcementUnavailable
                }
                CloudFilesLocalChangeSyncMode::Starting => SifsMountWritebackMode::Starting,
                CloudFilesLocalChangeSyncMode::Active => SifsMountWritebackMode::Active,
                CloudFilesLocalChangeSyncMode::Blocked => SifsMountWritebackMode::Blocked,
            };
            status.pending_operations = event.local_change_sync.pending_operations;
            status.retrying_operations = event.local_change_sync.retrying_operations;
            status.failed_operations = event.local_change_sync.failed_operations;
            status.conflict_count = event.local_change_sync.conflict_count;
            status.attention = event
                .local_change_sync
                .attention
                .iter()
                .cloned()
                .map(cloud_files_attention_summary)
                .collect();
            status.attention_overflow = event.local_change_sync.attention_overflow;
        }
        status.last_error =
            last_error.or_else(|| cloud_files_completion_error(self.finished_result.as_ref()));
        if status.last_error.is_none() {
            status.last_error = self
                .lifecycle
                .as_ref()
                .and_then(|event| event.local_change_sync.last_error.as_ref())
                .map(ToString::to_string);
        }
        if status.last_error.is_some() {
            status.phase = SifsMountRuntimePhase::Error;
        }
        if status.phase == SifsMountRuntimePhase::Connected
            && (!status.worker_alive || !status.worker_ready)
        {
            status.phase = SifsMountRuntimePhase::Error;
            status.last_error = Some(
                "Cloud Files connected before the writable synchronization worker was ready"
                    .to_string(),
            );
        }
        if status.phase == SifsMountRuntimePhase::Connected
            && self
                .last_health_observed
                .is_none_or(|observed| observed.elapsed() >= CLOUD_FILES_WORKER_STALE_AFTER)
        {
            status.phase = SifsMountRuntimePhase::Error;
            status.last_error = Some(format!(
                "Cloud Files worker heartbeat is stale for at least {} seconds",
                CLOUD_FILES_WORKER_STALE_AFTER.as_secs()
            ));
        }
        status
    }

    fn stop(&mut self) -> Result<(), String> {
        self.cancel.store(true, Ordering::SeqCst);
        self.refresh();
        if self.finished_result.is_none() {
            match self.finished.recv_timeout(MOUNT_STOP_TIMEOUT) {
                Ok(result) => self.finished_result = Some(result),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(format!(
                        "Cloud Files provider did not stop within {} seconds",
                        MOUNT_STOP_TIMEOUT.as_secs()
                    ));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if self
                        .thread
                        .as_ref()
                        .is_none_or(|thread| !thread.is_finished())
                    {
                        return Err(
                            "Cloud Files provider result channel disconnected before the worker stopped"
                                .to_string(),
                        );
                    }
                }
            }
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| "Cloud Files provider worker panicked during shutdown".to_string())?;
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for ActiveMount {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

#[cfg(windows)]
struct CloudFilesSupervisorCallbacks {
    cancel: Arc<AtomicBool>,
    supervisor_shutdown: Arc<AtomicBool>,
    events: mpsc::SyncSender<sinomo_runtime::dfs_cloud_files_api::CloudFilesMountLifecycleEvent>,
    data_dir: SinomoDataDir,
    cursor_recovery: Option<HostCursorRecoveryMarker>,
    operation_fence: Arc<Mutex<()>>,
}

#[cfg(windows)]
impl CloudFilesSupervisorCallbacks {
    fn handle_output(&self, payload_sobj: &[u8]) -> bool {
        use sinomo_runtime::dfs_cloud_files_api::CloudFilesModuleEvent;

        match sinomo_runtime::module_api::decode_sobj::<CloudFilesModuleEvent>(payload_sobj) {
            Ok(CloudFilesModuleEvent::MountLifecycle(event)) => {
                if event.error.is_some() || event.local_change_sync.last_error.is_some() {
                    tracing::warn!(
                        target: "sinomo.dfs.cloud_files",
                        phase = ?event.phase,
                        detail = %event.detail,
                        lifecycle_error = ?event.error,
                        writeback_error = ?event.local_change_sync.last_error,
                        "Cloud Files lifecycle reported an error"
                    );
                }
                let _ = self.events.try_send(event);
                true
            }
            Ok(CloudFilesModuleEvent::CursorRecoveryReadyObserved(request)) => {
                let Some(expected) = self.cursor_recovery.as_ref() else {
                    tracing::error!(
                        "rejected cursor-recovery ReadyObserved request from an ordinary mount"
                    );
                    return false;
                };
                let _operation = match self.operation_fence.lock() {
                    Ok(operation) => operation,
                    Err(_) => {
                        tracing::error!("cursor-recovery operation fence is poisoned at Ready");
                        return false;
                    }
                };
                match acknowledge_host_cursor_recovery_ready_observed(
                    &self.data_dir,
                    expected,
                    &request,
                ) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(
                            "failed durable cursor-recovery ReadyObserved acknowledgement: {error}"
                        );
                        false
                    }
                }
            }
            Ok(CloudFilesModuleEvent::CursorRecoveryActiveObserved(request)) => {
                let Some(expected) = self.cursor_recovery.as_ref() else {
                    tracing::error!(
                        "rejected cursor-recovery ActiveObserved request from an ordinary mount"
                    );
                    return false;
                };
                let _operation = match self.operation_fence.lock() {
                    Ok(operation) => operation,
                    Err(_) => {
                        tracing::error!("cursor-recovery operation fence is poisoned at Active");
                        return false;
                    }
                };
                match acknowledge_host_cursor_recovery_active_observed(
                    &self.data_dir,
                    expected,
                    &request,
                    &self.cancel,
                    &self.supervisor_shutdown,
                ) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(
                            "failed durable cursor-recovery ActiveObserved acknowledgement: {error}"
                        );
                        false
                    }
                }
            }
            Err(error) => {
                tracing::warn!("ignored malformed Cloud Files lifecycle output: {error}");
                false
            }
        }
    }
}

#[cfg(windows)]
fn acknowledge_host_cursor_recovery_active_observed(
    data_dir: &SinomoDataDir,
    expected: &HostCursorRecoveryMarker,
    request: &sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryActiveObservedRequest,
    cancel: &AtomicBool,
    supervisor_shutdown: &AtomicBool,
) -> Result<(), String> {
    use sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION;
    if request.schema_version != CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
        || request.recovery_id != expected.recovery_id
        || request.recovery_token != expected.inner_token
        || request.sync_root != expected.identity.sync_root
        || request.root_path != expected.identity.root_path
        || request.sync_root_file_id != expected.inspection.sync_root_file_id
        || request.recovery_cycle != expected.recovery_cycle
        || !valid_canonical_blake3_fingerprint(&request.module_marker_fingerprint)
        || !valid_canonical_blake3_fingerprint(
            &request.completed_registration_artifacts_fingerprint,
        )
    {
        return Err(
            "ActiveObserved request does not match the exact captured host authorization"
                .to_string(),
        );
    }

    let cursor_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: expected.identity.sync_root.clone(),
            root_path: expected.identity.root_path.clone(),
            data_dir_root: Some(expected.identity.data_dir_root.clone()),
            provider_name: expected.identity.provider_name.clone(),
            cursor_recovery: Some(
                sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                    recovery_id: expected.recovery_id.clone(),
                    recovery_token: expected.inner_token.clone(),
                    recovery_cycle: expected.recovery_cycle,
                    native_successor_fingerprint: expected.native_successor_fingerprint.clone(),
                },
            ),
        },
        expected.module.clone(),
        CloudUnregisterPolicy::PreserveState,
    )?;
    let cursor_stage = staged_cloud_registration_record(
        &cursor_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    let ordinary_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            cursor_recovery: None,
            ..cursor_recovery.request.clone()
        },
        expected.module.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    let ordinary_stage = staged_cloud_registration_record(
        &ordinary_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;

    let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    let Some(mut current) = marker_snapshot.value else {
        require_physically_absent(
            &host_cursor_recovery_provider_bundle_path(data_dir),
            "completed cursor-recovery provider bundle",
        )?;
        require_physically_absent(
            &host_cursor_recovery_provider_bundle_temp_path(data_dir),
            "completed cursor-recovery provider temporary bundle",
        )?;
        require_physically_absent(
            &cloud_files_module_runtime_path(
                data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ),
            "completed module cursor-recovery marker",
        )?;
        require_physically_absent(
            &cloud_files_cursor_recovery_module_temp_path(data_dir),
            "completed module cursor-recovery temporary marker",
        )?;
        require_physically_absent(
            &cloud_files_registration_journal_temp_path(data_dir),
            "completed registration journal temporary file",
        )?;
        require_physically_absent(
            &cloud_files_registration_receipt_temp_path(data_dir),
            "completed registration receipt temporary file",
        )?;
        let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
        if cloud_files_completed_registration_artifacts_fingerprint(data_dir)?
            != request.completed_registration_artifacts_fingerprint
            || expected.completion_module_marker_fingerprint.as_deref()
                != Some(&request.module_marker_fingerprint)
            || staged.current.as_ref() != Some(&ordinary_stage)
            || staged.temporary.is_some()
        {
            return Err(
                "completed ActiveObserved replay changed its ordinary artifacts".to_string(),
            );
        }
        let mut enabled = expected.disabled_config.clone();
        enabled.enabled = true;
        let observed: SifsMountConfig = sifs_mount_config_store()?
            .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
            .map_err(|error| format!("revalidate completed cursor-recovery config: {error}"))?;
        if observed == expected.disabled_config || supervisor_shutdown.load(Ordering::SeqCst) {
            cancel.store(true, Ordering::SeqCst);
        } else if observed != enabled {
            return Err("completed cursor-recovery configuration changed".to_string());
        }
        return Ok(());
    };
    if !same_host_cursor_recovery_callback_authority(&current, expected)
        || !matches!(
            current.phase,
            HostCursorRecoveryPhase::ReadyObserved
                | HostCursorRecoveryPhase::ActiveObserved
                | HostCursorRecoveryPhase::AuthorityFinalized
        )
    {
        return Err("ActiveObserved host marker is not an exact callback successor".to_string());
    }
    let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    if bundle_snapshot.temporary {
        return Err("ActiveObserved found an unpromoted provider bundle".to_string());
    }
    if let Some(bundle) = bundle_snapshot.value.as_ref() {
        validate_host_cursor_recovery_bundle_link(&current, bundle)?;
    }

    let module_marker_path =
        cloud_files_module_runtime_path(data_dir, CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE);
    let module_marker_present = std::fs::symlink_metadata(&module_marker_path)
        .map(|_| true)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })
        .map_err(|error: std::io::Error| {
            format!("inspect module cursor-recovery marker: {error}")
        })?;
    require_physically_absent(
        &cloud_files_cursor_recovery_module_temp_path(data_dir),
        "module cursor-recovery temporary marker at ActiveObserved",
    )?;
    let module_fingerprint = module_marker_present
        .then(|| cloud_files_module_marker_raw_fingerprint(data_dir))
        .transpose()?;
    if matches!(
        current.phase,
        HostCursorRecoveryPhase::ReadyObserved | HostCursorRecoveryPhase::ActiveObserved
    ) {
        require_physically_absent(
            &cloud_files_registration_journal_temp_path(data_dir),
            "terminal predecessor registration journal temporary file",
        )?;
        require_physically_absent(
            &cloud_files_registration_receipt_temp_path(data_dir),
            "terminal predecessor registration receipt temporary file",
        )?;
        let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
        if staged.current.as_ref() != Some(&cursor_stage) || staged.temporary.is_some() {
            return Err(
                "terminal predecessor lacks its exact cursor stage with no temporary successor"
                    .to_string(),
            );
        }
    }
    let mut expected_enabled = current.disabled_config.clone();
    expected_enabled.enabled = true;
    let observed: SifsMountConfig = sifs_mount_config_store()?
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| {
            format!("revalidate cursor-recovery consent before ActiveObserved: {error}")
        })?;
    let consent_disabled = observed == current.disabled_config;
    if observed != expected_enabled && !consent_disabled {
        return Err(
            "mount configuration was replaced before durable cursor-recovery ActiveObserved"
                .to_string(),
        );
    }
    match current.phase {
        HostCursorRecoveryPhase::ReadyObserved => {
            require_cursor_recovery_pending_intent_absent(data_dir, "NamespaceFinalized")?;
            if bundle_snapshot.value.is_none()
                || consent_disabled
                || module_fingerprint.as_deref() != Some(&request.module_marker_fingerprint)
                || current.ready_artifacts_fingerprint.as_ref()
                    != Some(&cloud_files_ready_artifacts_fingerprint(data_dir)?)
            {
                return Err(
                    "NamespaceFinalized ActiveObserved lost enabled consent or exact artifacts"
                        .to_string(),
                );
            }
            current.active_module_marker_fingerprint = module_fingerprint;
            current.phase = HostCursorRecoveryPhase::ActiveObserved;
            persist_host_cursor_recovery_marker(data_dir, &current)
        }
        HostCursorRecoveryPhase::ActiveObserved => {
            require_cursor_recovery_pending_intent_absent(data_dir, "CompletionAuthorized")?;
            if bundle_snapshot.value.is_none()
                || module_fingerprint.as_deref() != Some(&request.module_marker_fingerprint)
                || current.ready_artifacts_fingerprint.as_ref()
                    != Some(&cloud_files_ready_artifacts_fingerprint(data_dir)?)
            {
                return Err(
                    "CompletionAuthorized ActiveObserved lacks its exact enabled predecessor"
                        .to_string(),
                );
            }
            if current.active_module_marker_fingerprint.as_deref()
                == Some(&request.module_marker_fingerprint)
            {
                if marker_snapshot.temporary {
                    promote_host_cursor_recovery_marker(data_dir, &current)?;
                }
                return Ok(());
            }
            if consent_disabled {
                return Err(
                    "CompletionAuthorized cannot linearize after disabled consent won".to_string(),
                );
            }
            current.completion_module_marker_fingerprint = module_fingerprint;
            current.completion_registration_artifacts_fingerprint =
                Some(request.completed_registration_artifacts_fingerprint.clone());
            current.phase = HostCursorRecoveryPhase::AuthorityFinalized;
            persist_host_cursor_recovery_marker(data_dir, &current)
        }
        HostCursorRecoveryPhase::AuthorityFinalized if module_marker_present => {
            let completion_cut = validate_authority_finalized_completion_cut(data_dir, &current)?;
            if bundle_snapshot.value.is_none()
                || current.completion_module_marker_fingerprint.as_deref()
                    != module_fingerprint.as_deref()
                || current.completion_module_marker_fingerprint.as_deref()
                    != Some(&request.module_marker_fingerprint)
                || current
                    .completion_registration_artifacts_fingerprint
                    .as_deref()
                    != Some(&request.completed_registration_artifacts_fingerprint)
            {
                return Err("AuthorityFinalized native completion proof changed".to_string());
            }
            let completed_registration =
                cloud_files_completed_registration_artifacts_fingerprint(data_dir)?;
            if completed_registration != request.completed_registration_artifacts_fingerprint {
                // Exact replay of the AF acknowledgement before native journal
                // normalization. The recovery Ready fingerprint must still be
                // unchanged and the cursor-context stage must remain final.
                if current.ready_artifacts_fingerprint.as_ref()
                    != Some(&cloud_files_ready_artifacts_fingerprint(data_dir)?)
                    || load_staged_cloud_registration_record(data_dir)?.as_ref()
                        != Some(&cursor_stage)
                {
                    return Err(
                        "AuthorityFinalized predecessor artifacts changed before normalization"
                            .to_string(),
                    );
                }
                if marker_snapshot.temporary {
                    promote_host_cursor_recovery_marker(data_dir, &current)?;
                }
                return Ok(());
            }
            if !completion_cut.registration_normalized {
                return Err(
                    "AuthorityFinalized host cannot normalize staged ownership before its registration journal"
                        .to_string(),
                );
            }
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &cursor_stage,
                &ordinary_stage,
            )?;
            let completed = cloud_files_completed_artifacts_fingerprint(data_dir, &ordinary_stage)?;
            if let Some(expected) = current.completed_artifacts_fingerprint.as_ref()
                && expected != &completed
            {
                return Err("normalized completion artifacts changed".to_string());
            }
            if current.completed_artifacts_fingerprint.is_none() {
                current.completed_artifacts_fingerprint = Some(completed);
                persist_host_cursor_recovery_marker(data_dir, &current)?;
            } else if marker_snapshot.temporary {
                promote_host_cursor_recovery_marker(data_dir, &current)?;
            }
            Ok(())
        }
        HostCursorRecoveryPhase::AuthorityFinalized => {
            require_physically_absent(
                &cloud_files_cursor_recovery_module_temp_path(data_dir),
                "completed module cursor-recovery temporary marker",
            )?;
            require_cursor_recovery_pending_intent_absent(data_dir, "final completion")?;
            require_physically_absent(
                &cloud_files_registration_journal_temp_path(data_dir),
                "completed registration journal temporary file",
            )?;
            require_physically_absent(
                &cloud_files_registration_receipt_temp_path(data_dir),
                "completed registration receipt temporary file",
            )?;
            let completed_registration =
                cloud_files_completed_registration_artifacts_fingerprint(data_dir)?;
            let staged = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
            if completed_registration != request.completed_registration_artifacts_fingerprint
                || current.completion_module_marker_fingerprint.as_deref()
                    != Some(&request.module_marker_fingerprint)
                || current
                    .completion_registration_artifacts_fingerprint
                    .as_deref()
                    != Some(&completed_registration)
                || staged.current.as_ref() != Some(&ordinary_stage)
                || staged.temporary.is_some()
                || current.completed_artifacts_fingerprint.as_ref()
                    != Some(&cloud_files_completed_artifacts_fingerprint(
                        data_dir,
                        &ordinary_stage,
                    )?)
            {
                return Err(
                    "final completion artifacts changed before authority release".to_string(),
                );
            }
            bundle_snapshot.value.as_ref().ok_or_else(|| {
                "final completion lost its cursor-recovery provider bundle".to_string()
            })?;
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
                &host_cursor_recovery_provider_bundle_path(data_dir),
            )
            .map_err(|error| {
                format!("remove completed cursor-recovery provider bundle: {error}")
            })?;
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
                &host_cursor_recovery_marker_path(data_dir),
            )
            .map_err(|error| format!("remove completed host cursor-recovery marker: {error}"))?;
            if consent_disabled || supervisor_shutdown.load(Ordering::SeqCst) {
                cancel.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
        _ => Err("ActiveObserved host phase is not a terminal successor".to_string()),
    }
}

#[cfg(windows)]
fn resume_active_observed_host_cursor_recovery_cleanup(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<bool, String> {
    let snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
    let Some(marker) = snapshot.value else {
        return Ok(false);
    };
    if marker.phase != HostCursorRecoveryPhase::AuthorityFinalized {
        return Ok(false);
    }
    let (_lease, host_lock_bytes) =
        SifsMountLease::try_acquire_existing_for_cursor_recovery(data_dir)?;
    validate_current_host_cursor_recovery_authority(
        data_dir,
        &marker.disabled_config,
        &host_lock_bytes,
        &marker.outer_token,
        &marker,
    )?;
    let mut enabled = marker.disabled_config.clone();
    enabled.enabled = true;
    let config_is_enabled = config == &enabled;
    let config_is_disabled = config == &marker.disabled_config;
    if !config_is_enabled && !config_is_disabled {
        return Err(
            "AuthorityFinalized cleanup found configuration drift from its exact enabled/disabled snapshot"
                .to_string(),
        );
    }
    require_cursor_recovery_pending_intent_absent(data_dir, "AuthorityFinalized cleanup")?;
    require_physically_absent(
        &cloud_files_cursor_recovery_module_temp_path(data_dir),
        "AuthorityFinalized module cursor-recovery temporary marker",
    )?;
    let module_marker_path =
        cloud_files_module_runtime_path(data_dir, CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE);
    let module_marker_present = std::fs::symlink_metadata(&module_marker_path)
        .map(|_| true)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })
        .map_err(|error: std::io::Error| {
            format!("inspect AuthorityFinalized native marker: {error}")
        })?;
    if module_marker_present {
        validate_authority_finalized_completion_cut(data_dir, &marker)?;
        // Exact CA remains native-owned. Defer to the pinned completion mount;
        // only N-absent cuts are host-only cleanup below.
        return Ok(false);
    }
    require_physically_absent(
        &module_marker_path,
        "completed module cursor-recovery marker",
    )?;
    require_physically_absent(
        &cloud_files_registration_journal_temp_path(data_dir),
        "completed registration journal temporary file",
    )?;
    require_physically_absent(
        &cloud_files_registration_receipt_temp_path(data_dir),
        "completed registration receipt temporary file",
    )?;
    validate_authority_finalized_released_cut(data_dir, &marker)?;
    let ordinary_recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            data_dir_root: Some(marker.identity.data_dir_root.clone()),
            provider_name: marker.identity.provider_name.clone(),
            cursor_recovery: None,
        },
        marker.module.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    let ordinary_stage = staged_cloud_registration_record(
        &ordinary_recovery,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    if load_staged_cloud_registration_record(data_dir)?.as_ref() != Some(&ordinary_stage)
        || marker
            .completion_registration_artifacts_fingerprint
            .as_ref()
            != Some(&cloud_files_completed_registration_artifacts_fingerprint(
                data_dir,
            )?)
        || marker.completed_artifacts_fingerprint.as_ref()
            != Some(&cloud_files_completed_artifacts_fingerprint(
                data_dir,
                &ordinary_stage,
            )?)
    {
        return Err("AuthorityFinalized cleanup changed its completed artifacts".to_string());
    }
    let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
    if bundle_snapshot.temporary {
        return Err("AuthorityFinalized cleanup found an unpromoted provider bundle".to_string());
    }
    if let Some(bundle) = bundle_snapshot.value.as_ref() {
        validate_host_cursor_recovery_bundle_link(&marker, bundle)?;
        sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
            &host_cursor_recovery_provider_bundle_path(data_dir),
        )
        .map_err(|error| format!("remove completed cursor-recovery provider bundle: {error}"))?;
    }
    if snapshot.temporary {
        promote_host_cursor_recovery_marker(data_dir, &marker)?;
    }
    sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(
        &host_cursor_recovery_marker_path(data_dir),
    )
    .map_err(|error| format!("remove completed host cursor-recovery marker: {error}"))?;
    Ok(true)
}

#[cfg(windows)]
fn acknowledge_host_cursor_recovery_ready_observed(
    data_dir: &SinomoDataDir,
    expected: &HostCursorRecoveryMarker,
    request: &sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryReadyObservedRequest,
) -> Result<(), String> {
    use sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION;
    let mut current = load_host_cursor_recovery_marker_strict(data_dir)?.ok_or_else(|| {
        "host cursor-recovery marker disappeared before ReadyObserved".to_string()
    })?;
    if !same_host_cursor_recovery_callback_authority(&current, expected)
        || !matches!(
            current.phase,
            HostCursorRecoveryPhase::ArmedAwaitingReady
                | HostCursorRecoveryPhase::ReadyObserved
                | HostCursorRecoveryPhase::ActiveObserved
        )
        || request.schema_version != CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
        || request.recovery_id != current.recovery_id
        || request.recovery_token != current.inner_token
        || request.sync_root != current.identity.sync_root
        || request.root_path != current.identity.root_path
        || request.sync_root_file_id != current.inspection.sync_root_file_id
        || request.recovery_cycle != current.recovery_cycle
    {
        return Err(
            "ReadyObserved request does not match the exact armed host authorization".to_string(),
        );
    }
    let mut enabled = current.disabled_config.clone();
    enabled.enabled = true;
    let store = sifs_mount_config_store()?;
    let observed: SifsMountConfig = store
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| format!("revalidate cursor-recovery consent at Ready: {error}"))?;
    if observed != enabled {
        return Err(
            "mount consent or configuration changed before ReadyObserved durability".to_string(),
        );
    }
    require_cursor_recovery_pending_intent_absent(data_dir, "ReadyObserved")?;
    let recovery = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: current.identity.sync_root.clone(),
            root_path: current.identity.root_path.clone(),
            data_dir_root: Some(current.identity.data_dir_root.clone()),
            provider_name: current.identity.provider_name.clone(),
            cursor_recovery: Some(
                sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                    recovery_id: current.recovery_id.clone(),
                    recovery_token: current.inner_token.clone(),
                    recovery_cycle: current.recovery_cycle,
                    native_successor_fingerprint: current.native_successor_fingerprint.clone(),
                },
            ),
        },
        current.module.clone(),
        CloudUnregisterPolicy::PreserveState,
    )?;
    let expected_stage =
        staged_cloud_registration_record(&recovery, StagedCloudRegistrationState::MayBeRegistered)?;
    if load_staged_cloud_registration_record(data_dir)?.as_ref() != Some(&expected_stage) {
        return Err("ReadyObserved requires exact MayBeRegistered staged ownership".to_string());
    }
    for (path, label) in [
        (
            cloud_files_registration_receipt_path(data_dir),
            "Cloud Files Ready receipt",
        ),
        (
            cloud_files_module_runtime_path(data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            "Cloud Files Ready journal",
        ),
        (
            cloud_files_module_runtime_path(
                data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ),
            "Cloud Files Ready recovery marker",
        ),
    ] {
        ensure_plain_existing_host_path(&path, false, label)?;
    }
    let artifacts_fingerprint = cloud_files_ready_artifacts_fingerprint(data_dir)?;
    if matches!(
        current.phase,
        HostCursorRecoveryPhase::ReadyObserved | HostCursorRecoveryPhase::ActiveObserved
    ) && let Some(expected_fingerprint) = current.ready_artifacts_fingerprint.as_ref()
    {
        if expected_fingerprint != &artifacts_fingerprint {
            return Err(
                "ReadyObserved durable receipt/journal bytes changed before acknowledgement"
                    .to_string(),
            );
        }
    }
    if current.phase == HostCursorRecoveryPhase::ArmedAwaitingReady {
        current.ready_artifacts_fingerprint = Some(artifacts_fingerprint);
        current.preserved_artifacts_fingerprint = None;
        current.phase = HostCursorRecoveryPhase::ReadyObserved;
        persist_host_cursor_recovery_marker(data_dir, &current)?;
    }
    Ok(())
}

#[cfg(windows)]
impl sinomo_runtime::module::NativeModuleHostCallbacks for CloudFilesSupervisorCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        let _ = self.handle_output(payload_sobj);
    }

    fn output_payload_status(&self, payload_sobj: &[u8]) -> bool {
        self.handle_output(payload_sobj)
    }

    fn log(&self, message: &[u8]) {
        tracing::debug!(
            target: "sinomo.dfs.cloud_files",
            "{}",
            String::from_utf8_lossy(message)
        );
    }

    fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst) || self.supervisor_shutdown.load(Ordering::SeqCst)
    }
}

#[cfg(windows)]
fn start_platform_mount(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
    mount_point: &Path,
    supervisor_shutdown: Arc<AtomicBool>,
    _mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
    lease: SifsMountLease,
) -> Result<ActiveMount, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleRequest, CloudFilesModuleResponse, CloudFilesMountRequest,
        SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
    };
    use sinomo_runtime::module::{
        ModuleId, NativeModule, native_module_cache_path, read_native_dylib_module_loader_state,
    };
    use sinomo_runtime::module_api::{
        NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
        encode_sobj,
    };

    let data_dir_root = data_dir.root();
    let host_cursor_recovery = load_host_cursor_recovery_marker_strict(data_dir)?;
    let ordinary_staged_provider = if host_cursor_recovery.is_none() {
        match load_staged_cloud_registration_record(data_dir)? {
            None => {
                require_physically_absent(
                    &cursor_recovery_staged_temp_path(data_dir),
                    "orphan deterministic staged Cloud Files successor",
                )?;
                None
            }
            Some(record)
                if record.recovery.module.interface_version
                    == SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    && record.recovery.legacy_prejournal_origin.is_none() =>
            {
                if record.state != StagedCloudRegistrationState::MayBeRegistered
                    || record.recovery.request.cursor_recovery.is_some()
                    || !registration_request_matches_config(&record.recovery.request, config)?
                {
                    return Err(
                        "canonical ordinary staged ownership is not an exact MayBeRegistered authority for this mount"
                            .to_string(),
                    );
                }
                let snapshot = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
                if snapshot.current.as_ref() != Some(&record)
                    || snapshot
                        .temporary
                        .as_ref()
                        .is_some_and(|temporary| temporary != &record)
                {
                    return Err(
                        "canonical ordinary staged ownership has a changed deterministic successor"
                            .to_string(),
                    );
                }
                if snapshot.temporary.is_some() {
                    persist_or_promote_cursor_recovery_staged_record(data_dir, &record)?;
                }
                let library =
                    verify_cloud_unregister_recovery_module(data_dir, &record.recovery.module)?;
                Some((record.recovery.module, library))
            }
            Some(record) => {
                require_physically_absent(
                    &cursor_recovery_staged_temp_path(data_dir),
                    "legacy staged Cloud Files deterministic successor",
                )?;
                if record.recovery.request.cursor_recovery.is_some() {
                    return Err(
                        "cursor-context staged ownership exists without its host authorization"
                            .to_string(),
                    );
                }
                None
            }
        }
    } else {
        None
    };
    let has_ordinary_staged_provider = ordinary_staged_provider.is_some();
    let (authority_module_provenance, module_provenance, library_path, cursor_recovery_bundle) =
        if let Some(marker) = host_cursor_recovery.as_ref() {
            if !matches!(
                marker.phase,
                HostCursorRecoveryPhase::ArmedAwaitingReady
                    | HostCursorRecoveryPhase::ReadyObserved
                    | HostCursorRecoveryPhase::ActiveObserved
                    | HostCursorRecoveryPhase::AuthorityFinalized
            ) {
                return Err(format!(
                    "cursor recovery {} is {:?}, not armed for provider mount",
                    marker.recovery_id, marker.phase
                ));
            }
            let mut exact_enabled = marker.disabled_config.clone();
            exact_enabled.enabled = true;
            let exact_completion_config = marker.phase
                == HostCursorRecoveryPhase::AuthorityFinalized
                && (config == &exact_enabled || config == &marker.disabled_config);
            if (!exact_completion_config && config != &exact_enabled)
                || marker.identity.data_dir_root != data_dir_root
                || marker.identity.sync_root != mount_point
            {
                return Err(
                "cursor-recovery mount no longer matches its exact authorized config and data directory"
                    .to_string(),
            );
            }
            let bundle_snapshot =
                inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
            let mut bundle = bundle_snapshot.value.ok_or_else(|| {
                "cursor-recovery mount lost its durable provider bundle".to_string()
            })?;
            validate_host_cursor_recovery_bundle_link(marker, &bundle)?;
            if bundle_snapshot.temporary {
                promote_host_cursor_recovery_provider_bundle(data_dir, &bundle)?;
            }
            if marker.phase == HostCursorRecoveryPhase::AuthorityFinalized {
                validate_authority_finalized_completion_cut(data_dir, marker)?;
            }
            let authority_path = materialize_cursor_recovery_provider_bundle(data_dir, &bundle)?;
            let pinned_execution =
                materialize_cursor_recovery_execution_provider(data_dir, &bundle)?;
            let module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
                .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
            let active = read_native_dylib_module_loader_state(&data_dir_root, &module_id)
                .map_err(|error| format!("read Cloud Files module loader state: {error}"))?;
            let active = active.map(|active| {
                let provenance = CloudFilesModuleProvenance::from_loader_state(&active);
                let path = native_module_cache_path(
                    &data_dir_root,
                    &active.cache_key,
                    &active.content_hash,
                    &active.library_name,
                );
                (provenance, path)
            });
            let active_is_new_execution = active.as_ref().is_some_and(|(provenance, _)| {
                provenance != &marker.module && bundle.execution_module.as_ref() != Some(provenance)
            });
            let (execution_provenance, execution_path) = if active_is_new_execution {
                let (active_provenance, active_path) = active.expect("active execution provider");
                let active_bytes = read_bounded_plain_host_file(
                    &active_path,
                    CLOUD_FILES_CURSOR_RECOVERY_PROVIDER_MAX_BYTES,
                    "active cursor-recovery execution provider",
                )?
                .ok_or_else(|| {
                    "active cursor-recovery execution provider disappeared".to_string()
                })?;
                if sinomo_runtime::module::native_module_content_hash(&active_bytes)
                    != active_provenance.content_hash
                {
                    return Err(
                        "active cursor-recovery execution provider hash changed".to_string()
                    );
                }
                bundle.execution_module = Some(active_provenance.clone());
                bundle.execution_provider_bytes = Some(active_bytes);
                validate_cursor_recovery_execution_provider(&bundle)?;
                (active_provenance, active_path)
            } else if let Some(execution) = pinned_execution {
                execution
            } else {
                (marker.module.clone(), authority_path)
            };
            (
                marker.module.clone(),
                execution_provenance,
                execution_path,
                Some(bundle),
            )
        } else if let Some(authority) = ordinary_staged_provider {
            (authority.0.clone(), authority.0, authority.1, None)
        } else {
            let module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
                .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
            let state = read_native_dylib_module_loader_state(&data_dir_root, &module_id)
            .map_err(|error| format!("read Cloud Files module loader state: {error}"))?
            .ok_or_else(|| {
                format!(
                    "Cloud Files module is not active; install or activate {SINOMO_DFS_CLOUD_FILES_MODULE_ID}"
                )
            })?;
            if state.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION {
                return Err(format!(
                    "Cloud Files module interface mismatch: expected {}, got {}",
                    SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, state.interface_version
                ));
            }
            let provenance = CloudFilesModuleProvenance::from_loader_state(&state);
            (
                provenance.clone(),
                provenance,
                native_module_cache_path(
                    &data_dir_root,
                    &state.cache_key,
                    &state.content_hash,
                    &state.library_name,
                ),
                None,
            )
        };
    let module = unsafe {
        NativeModule::load_process_resident_from_content_addressed_path(
            &library_path,
            &module_provenance.content_hash,
        )
    }
    .map_err(|error| format!("load Cloud Files native module: {error}"))?;
    if host_cursor_recovery.is_some() {
        for capability in [
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
        ] {
            if !module
                .descriptor()
                .capabilities
                .iter()
                .any(|candidate| candidate == capability)
            {
                return Err(format!(
                    "cursor-recovery execution provider lacks required capability {capability}"
                ));
            }
        }
    }
    admit_cloud_files_provider_before_effect_with(
        &module_provenance,
        module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        Some(
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
        ),
        || {
            if let Some(bundle) = cursor_recovery_bundle.as_ref() {
                // The provider descriptor has been admitted without effects.
                // Freeze the exact execution image before any staged/native
                // recovery mutation, while retaining the original authority
                // provider in the same immutable bundle.
                persist_host_cursor_recovery_provider_bundle(data_dir, bundle)?;
                materialize_cursor_recovery_provider_bundle(data_dir, bundle)?;
                match materialize_cursor_recovery_execution_provider(data_dir, bundle)? {
                    Some((execution, _)) if execution == module_provenance => Ok(()),
                    None if module_provenance == authority_module_provenance => Ok(()),
                    _ => Err(
                        "cursor-recovery execution provider changed before durable admission"
                            .to_string(),
                    ),
                }
            } else if has_ordinary_staged_provider {
                verify_cloud_unregister_recovery_module(data_dir, &module_provenance).map(|_| ())
            } else {
                stage_cloud_unregister_recovery_module(data_dir, &module_provenance, &library_path)
                    .map(|_| ())
                    .map_err(|error| {
                        format!("stage durable Cloud Files unregister provider before mount: {error}")
                    })
            }
        },
    )?;
    let request = CloudFilesMountRequest {
        sync_root: mount_point.to_path_buf(),
        root_path: config.root_path.clone(),
        data_dir_root: Some(data_dir_root),
        read_only: config.read_only,
        sync_local_changes: config.sync_local_changes,
        provider_name: "Sinomo SIFS".to_string(),
        provider_version: env!("CARGO_PKG_VERSION").to_string(),
        prepopulate_depth: 0,
        connect: true,
        max_run_seconds: None,
        cursor_recovery: host_cursor_recovery.as_ref().map(|marker| {
            sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                recovery_id: marker.recovery_id.clone(),
                recovery_token: marker.inner_token.clone(),
                recovery_cycle: marker.recovery_cycle,
                native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
            }
        }),
    };
    if host_cursor_recovery.is_some() {
        let current: SifsMountConfig = sifs_mount_config_store()?
            .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
            .map_err(|error| {
                format!("revalidate cursor-recovery config before mount effect: {error}")
            })?;
        if current != *config {
            return Err(
                "mount configuration changed after cursor-recovery admission and before staged/native effects"
                    .to_string(),
            );
        }
    }
    let recovery_record = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: request.sync_root.clone(),
            root_path: request.root_path.clone(),
            data_dir_root: request.data_dir_root.clone(),
            provider_name: request.provider_name.clone(),
            cursor_recovery: host_cursor_recovery.as_ref().map(|marker| {
                sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                    recovery_id: marker.recovery_id.clone(),
                    recovery_token: marker.inner_token.clone(),
                    recovery_cycle: marker.recovery_cycle,
                    native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
                }
            }),
        },
        authority_module_provenance.clone(),
        if host_cursor_recovery.is_some() {
            CloudUnregisterPolicy::PreserveState
        } else {
            CloudUnregisterPolicy::Release
        },
    )?;
    let staged_recovery = staged_cloud_registration_record(
        &recovery_record,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    if host_cursor_recovery.is_some() {
        if host_cursor_recovery.as_ref().is_some_and(|marker| {
            marker.phase == HostCursorRecoveryPhase::AuthorityFinalized
        }) {
            validate_authority_finalized_completion_cut(
                data_dir,
                host_cursor_recovery
                    .as_ref()
                    .expect("checked terminal completion marker"),
            )
            .map(|_| ())
        } else {
            let retained = staged_cloud_registration_record(
                &recovery_record,
                StagedCloudRegistrationState::RetainedAbsent,
            )?;
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &retained,
                &staged_recovery,
            )
        }
    } else {
        match load_staged_cloud_registration_record(data_dir)? {
            None => persist_staged_cloud_registration_record(data_dir, &staged_recovery),
            Some(existing) if existing == staged_recovery => Ok(()),
            Some(existing)
                if existing.state == StagedCloudRegistrationState::MayBeRegistered
                    && existing.recovery.request == staged_recovery.recovery.request
                    && existing.recovery.legacy_prejournal_origin.as_ref()
                        == Some(&existing.recovery.module)
                    && existing.recovery.module.interface_version
                        == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2 =>
            {
                let upgraded =
                    legacy_staged_provider_upgrade_record(&existing, module_provenance.clone())?;
                replace_staged_cloud_registration_record(data_dir, &existing, &upgraded)
            }
            Some(existing)
                if existing.state == StagedCloudRegistrationState::MayBeRegistered
                    && existing.recovery.request == staged_recovery.recovery.request
                    && existing.recovery.module == staged_recovery.recovery.module
                    && existing.recovery.legacy_prejournal_origin.is_some() =>
            {
                // Crash after the atomic v2-origin -> v3-provider upgrade. Keep the
                // exact origin and provider record unchanged across retries.
                Ok(())
            }
            Some(_) => Err(
                "existing staged Cloud Files ownership cannot be replaced by mount startup"
                    .to_string(),
            ),
        }
    }
    .map_err(|error| {
        format!("persist exact Cloud Files registration recovery before mount invocation: {error}")
    })?;
    let payload_sobj = encode_sobj(&CloudFilesModuleRequest::Mount(request))
        .map_err(|error| format!("encode Cloud Files mount request: {error}"))?;
    let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string(),
        payload_sobj,
        request_body: None,
        response_body_path: None,
    });
    let cancel = Arc::new(AtomicBool::new(false));
    let operation_fence = Arc::new(Mutex::new(()));
    let (event_tx, event_rx) = mpsc::sync_channel(32);
    let callbacks: Arc<dyn sinomo_runtime::module::NativeModuleHostCallbacks> =
        Arc::new(CloudFilesSupervisorCallbacks {
            cancel: Arc::clone(&cancel),
            supervisor_shutdown,
            events: event_tx,
            data_dir: data_dir.clone(),
            cursor_recovery: host_cursor_recovery.clone(),
            operation_fence: Arc::clone(&operation_fence),
        });
    let (finished_tx, finished_rx) = mpsc::sync_channel(1);
    let lease = Arc::new(lease);
    let worker_lease = Arc::clone(&lease);
    let thread = thread::Builder::new()
        .name("sinomo-cloud-files".to_string())
        .spawn(move || {
            let _lease = worker_lease;
            let result = module
                .invoke_with_callbacks(&native_request, Some(callbacks))
                .map_err(|error| format!("invoke Cloud Files native module: {error}"))
                .and_then(|response| match response {
                    NativeModuleResponse::Invocation(response)
                        if response.operation == SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION =>
                    {
                        match decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj)
                            .map_err(|error| {
                                format!("decode Cloud Files mount response: {error}")
                            })? {
                            CloudFilesModuleResponse::Mounted(_) => Ok(()),
                            CloudFilesModuleResponse::Unregistered(_) => Err(
                                "Cloud Files provider returned an unregister response to a mount request"
                                    .to_string(),
                            ),
                            CloudFilesModuleResponse::StatePreservingUnregistered(_) => Err(
                                "Cloud Files provider returned a state-preserving unregister response to a mount request"
                                    .to_string(),
                            ),
                            CloudFilesModuleResponse::CursorOnlyRecoveryInspected(_)
                            | CloudFilesModuleResponse::CursorOnlyRecoveryApplied(_) => Err(
                                "Cloud Files provider returned a cursor-recovery response to a mount request"
                                    .to_string(),
                            ),
                            CloudFilesModuleResponse::Error(error) => Err(error.to_string()),
                        }
                    }
                    NativeModuleResponse::Error(error) => {
                        Err(format!("{}: {}", error.code, error.detail))
                    }
                    other => Err(format!(
                        "unexpected Cloud Files native module response: {other:?}"
                    )),
                });
            let _ = finished_tx.send(result);
        })
        .map_err(|error| format!("start Cloud Files provider thread: {error}"))?;
    Ok(ActiveMount {
        config: config.clone(),
        mount_point: mount_point.to_path_buf(),
        cancel,
        events: event_rx,
        finished: finished_rx,
        thread: Some(thread),
        lifecycle: None,
        finished_result: None,
        provider_content_hash: module_provenance.content_hash.clone(),
        provider_interface_version: module_provenance.interface_version.clone(),
        module_provenance: authority_module_provenance,
        lease,
        operation_fence,
        last_health_generation: 0,
        last_health_observed: None,
        last_health_observed_at: None,
    })
}

#[cfg(windows)]
fn resolve_cloud_unregister_module_path(
    data_dir: &SinomoDataDir,
    provenance: &CloudFilesModuleProvenance,
) -> Result<PathBuf, String> {
    verify_cloud_unregister_recovery_module(data_dir, provenance).map_err(|error| {
        format!(
            "durable Cloud Files cleanup provider {} ({}, interface {}) is unavailable: {error}",
            provenance.module_id, provenance.content_hash, provenance.interface_version
        )
    })
}

#[cfg(windows)]
fn complete_pending_cloud_unregister(
    data_dir: &SinomoDataDir,
    pending: &mut PendingCloudUnregister,
) -> Result<(), String> {
    if pending.lease.is_none() {
        if pending.record.request.cursor_recovery.is_some() {
            let (lease, _) = SifsMountLease::try_acquire_existing_for_cursor_recovery(data_dir)?;
            pending.lease = Some(Arc::new(lease));
        } else {
            let owner = format!(
                "pending Cloud Files unregister {}",
                pending.record.request.sync_root.display()
            );
            pending.lease = Some(Arc::new(SifsMountLease::try_acquire(data_dir, &owner)?));
        }
    }
    if pending.record.request.cursor_recovery.is_none() {
        ensure_cloud_unregister_recovery_module(data_dir, &pending.record.module)?;
    }
    let staged_snapshot = inspect_cursor_recovery_staged_record_snapshot(data_dir)?;
    let mut staged = staged_snapshot.current.ok_or_else(|| {
        "pending Cloud Files unregister has no authoritative staged ownership record".to_string()
    })?;
    if let Some(context) = pending.record.request.cursor_recovery.as_ref() {
        let marker_snapshot = inspect_host_cursor_recovery_marker_for_resume(data_dir)?;
        let marker = marker_snapshot.value.ok_or_else(|| {
            "cursor-recovery pending replay lost its durable host marker".to_string()
        })?;
        let lease_bytes = pending
            .lease
            .as_ref()
            .and_then(|lease| lease.cursor_recovery_bytes())
            .ok_or_else(|| {
                "cursor-recovery pending replay did not retain the existing host lease bytes"
                    .to_string()
            })?;
        validate_current_host_cursor_recovery_authority(
            data_dir,
            &marker.disabled_config,
            lease_bytes,
            &marker.outer_token,
            &marker,
        )?;
        let preserved_artifacts_match = marker.phase == HostCursorRecoveryPhase::PreservedRetained
            && marker.preserved_artifacts_fingerprint.as_ref()
                == Some(&cloud_files_preserved_artifacts_fingerprint(data_dir)?);
        if marker_snapshot.temporary && !preserved_artifacts_match {
            return Err(
                "durable pending intent found an impossible predecessor host-marker temp"
                    .to_string(),
            );
        }
        let predecessor_authority = !marker_snapshot.temporary
            && context == &host_cursor_recovery_context(&marker)
            && matches!(
                marker.phase,
                HostCursorRecoveryPhase::ArmedAwaitingReady
                    | HostCursorRecoveryPhase::ReadyObserved
                    | HostCursorRecoveryPhase::ActiveObserved
            );
        let successor_authority = marker.phase == HostCursorRecoveryPhase::PreservedRetained
            && context.recovery_id == marker.recovery_id
            && context.recovery_token == marker.inner_token
            && context
                .recovery_cycle
                .checked_add(1)
                .is_some_and(|cycle| cycle == marker.recovery_cycle)
            && preserved_artifacts_match;
        if !predecessor_authority && !successor_authority {
            return Err(
                "cursor-recovery pending replay does not match its exact host cycle authority"
                    .to_string(),
            );
        }
        if pending.record.module != marker.module
            || pending.record.request.sync_root != marker.identity.sync_root
            || pending.record.request.root_path != marker.identity.root_path
            || pending.record.request.data_dir_root.as_ref()
                != Some(&marker.identity.data_dir_root)
            || pending.record.request.provider_name != marker.identity.provider_name
            || pending.record.operation
                != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        {
            return Err(
                "cursor-recovery pending replay changed its exact host identity, provider, or operation"
                    .to_string(),
            );
        }
        let expected_stage = staged_cloud_registration_record(
            &pending.record,
            StagedCloudRegistrationState::MayBeRegistered,
        )?;
        let current_is_predecessor = staged == expected_stage;
        let current_is_successor = is_already_applied_retained_completion(&pending.record, &staged);
        let temporary_is_redundant_predecessor = staged_snapshot
            .temporary
            .as_ref()
            .is_some_and(|temporary| temporary == &expected_stage);
        let temporary_is_successor = staged_snapshot.temporary.as_ref().is_some_and(|temporary| {
            is_already_applied_retained_completion(&pending.record, temporary)
        });
        let exact_predecessor_host_cut = predecessor_authority
            && ((current_is_successor
                && (staged_snapshot.temporary.is_none()
                    || staged_snapshot.temporary.as_ref() == Some(&staged)))
                || (current_is_predecessor
                    && (staged_snapshot.temporary.is_none()
                        || temporary_is_redundant_predecessor
                        || temporary_is_successor)));
        let exact_successor_host_cut = successor_authority
            && current_is_successor
            && staged.recovery.request.cursor_recovery.as_ref()
                == Some(&host_cursor_recovery_context(&marker))
            && (staged_snapshot.temporary.is_none()
                || staged_snapshot.temporary.as_ref() == Some(&staged));
        if !exact_predecessor_host_cut && !exact_successor_host_cut {
            return Err(
                "cursor-recovery pending replay found an impossible staged predecessor/successor cut"
                    .to_string(),
            );
        }
        let bundle_snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(data_dir)?;
        if bundle_snapshot.temporary {
            return Err(
                "cursor-recovery pending replay found an unpromoted provider bundle".to_string(),
            );
        }
        let bundle = bundle_snapshot
            .value
            .as_ref()
            .ok_or_else(|| "cursor-recovery pending replay lost its provider bundle".to_string())?;
        validate_host_cursor_recovery_bundle_link(&marker, bundle)?;
        if marker_snapshot.temporary {
            promote_host_cursor_recovery_marker(data_dir, &marker)?;
        }
        materialize_cursor_recovery_provider_bundle(data_dir, bundle)?;
        verify_cloud_unregister_recovery_module(data_dir, &pending.record.module)
            .map_err(|error| format!("verify cursor-recovery pending provider pin: {error}"))?;
        promote_cursor_recovery_pending_intent(data_dir, &pending.record)?;
        if current_is_successor {
            if let Some(temporary) = staged_snapshot.temporary.as_ref() {
                if temporary != &staged {
                    return Err(
                        "RetainedAbsent completion has changed staged temporary evidence"
                            .to_string(),
                    );
                }
                persist_or_promote_cursor_recovery_staged_record(data_dir, &staged)?;
            }
            finish_already_applied_retained_completion(data_dir, &pending.record, &staged)?;
            return Ok(());
        }
        if temporary_is_successor {
            let temporary = staged_snapshot
                .temporary
                .as_ref()
                .expect("classified RetainedAbsent temporary successor");
            transition_or_promote_cursor_recovery_staged_record(data_dir, &staged, temporary)?;
            finish_already_applied_retained_completion(data_dir, &pending.record, temporary)?;
            return Ok(());
        }
        if !predecessor_authority {
            return Err(
                "PreservedRetained host authority lacks its exact RetainedAbsent successor"
                    .to_string(),
            );
        }
        if staged != expected_stage
            || staged_snapshot
                .temporary
                .as_ref()
                .is_some_and(|temporary| temporary != &expected_stage)
        {
            return Err(
                "cursor-recovery pending replay lacks its exact full-context MayBeRegistered predecessor"
                    .to_string(),
            );
        }
        if staged_snapshot.temporary.is_some() {
            persist_or_promote_cursor_recovery_staged_record(data_dir, &expected_stage)?;
        }
    } else {
        if staged_snapshot.temporary.is_some() {
            return Err(
                "ordinary pending unregister found cursor-recovery staged temporary evidence"
                    .to_string(),
            );
        }
        let retained = staged_cloud_registration_record(
            &pending.record,
            StagedCloudRegistrationState::RetainedAbsent,
        )?;
        if staged == retained
            && pending.record.operation
                == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
        {
            // Provider rollout while quiescent persists the Release intent
            // before changing RetainedAbsent. Replay that exact cut by moving
            // to MayBeRegistered, then let the old pinned provider prove the
            // registration/journal/receipt are absent. No new provider is
            // loaded until Released clears this stage.
            let may_be_registered = staged_cloud_registration_record(
                &pending.record,
                StagedCloudRegistrationState::MayBeRegistered,
            )?;
            transition_or_promote_cursor_recovery_staged_record(
                data_dir,
                &staged,
                &may_be_registered,
            )?;
            staged = may_be_registered;
        }
    }
    if finish_already_applied_retained_completion(data_dir, &pending.record, &staged)? {
        return Ok(());
    }
    let exact_may_be_registered = staged_cloud_registration_record(
        &pending.record,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
    if staged != exact_may_be_registered {
        return Err(
            "pending Cloud Files unregister no longer matches its exact MayBeRegistered staged ownership"
                .to_string(),
        );
    }
    let response = unregister_pending_cloud_mount(data_dir, &pending.record)?;
    finalize_cloud_unregister_completion_with(
        data_dir,
        &pending.record,
        &staged,
        &response,
        || Ok(()),
    )?;
    if matches!(response, CloudUnregisterCompletion::Released(_))
        && let Err(error) =
            remove_cloud_unregister_recovery_module(data_dir, &pending.record.module)
    {
        tracing::warn!(
            "Cloud Files unregister completed but its staged recovery provider could not be removed: {error}"
        );
    }
    Ok(())
}

#[cfg(windows)]
fn unregister_pending_cloud_mount(
    data_dir: &SinomoDataDir,
    record: &PendingCloudUnregisterRecord,
) -> Result<CloudUnregisterCompletion, String> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleResponse, SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
    };
    use sinomo_runtime::module::NativeModule;
    use sinomo_runtime::module_api::{
        NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
    };

    validate_pending_cloud_unregister_record(data_dir, record)?;
    let library_path = resolve_cloud_unregister_module_path(data_dir, &record.module)?;
    let module = unsafe {
        NativeModule::load_from_content_addressed_path(&library_path, &record.module.content_hash)
    }
    .map_err(|error| {
        format!(
            "load recorded Cloud Files cleanup provider {}: {error}",
            library_path.display()
        )
    })?;
    let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: record.operation.clone(),
        payload_sobj: record.payload_sobj.clone(),
        request_body: None,
        response_body_path: None,
    });
    let expected_sync_root = &record.request.sync_root;
    let expected_root_path = &record.request.root_path;
    let native_response = admit_cloud_files_provider_before_effect_with(
        &record.module,
        module.descriptor(),
        &record.operation,
        (record.operation == SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION).then_some(
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
        ),
        || {
            module
                .invoke(&native_request)
                .map_err(|error| format!("invoke Cloud Files unregister: {error}"))
        },
    )?;
    match native_response {
        NativeModuleResponse::Invocation(response) if response.operation == record.operation => {
            match decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj)
                .map_err(|error| format!("decode Cloud Files unregister response: {error}"))?
            {
                CloudFilesModuleResponse::Unregistered(response)
                    if record.operation == SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
                        && response.sync_root == *expected_sync_root
                        && response.root_path == *expected_root_path =>
                {
                    Ok(CloudUnregisterCompletion::Released(response))
                }
                CloudFilesModuleResponse::Unregistered(response) => Err(format!(
                    "Cloud Files unregister response identity mismatch: expected {} -> {}, got {} -> {}",
                    expected_sync_root.display(),
                    expected_root_path,
                    response.sync_root.display(),
                    response.root_path
                )),
                CloudFilesModuleResponse::Mounted(_) => Err(
                    "Cloud Files module returned a mount response to an unregister request"
                        .to_string(),
                ),
                CloudFilesModuleResponse::StatePreservingUnregistered(response)
                    if record.operation
                        == SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
                        && response.sync_root == *expected_sync_root
                        && response.root_path == *expected_root_path =>
                {
                    Ok(CloudUnregisterCompletion::StatePreserved(response))
                }
                CloudFilesModuleResponse::StatePreservingUnregistered(_) => Err(
                    "Cloud Files state-preserving unregister response identity/operation mismatch"
                        .to_string(),
                ),
                CloudFilesModuleResponse::CursorOnlyRecoveryInspected(_)
                | CloudFilesModuleResponse::CursorOnlyRecoveryApplied(_) => Err(
                    "Cloud Files module returned a cursor-recovery response to an unregister request"
                        .to_string(),
                ),
                CloudFilesModuleResponse::Error(error) => Err(error.to_string()),
            }
        }
        NativeModuleResponse::Error(error) => Err(format!("{}: {}", error.code, error.detail)),
        other => Err(format!(
            "unexpected Cloud Files unregister module response: {other:?}"
        )),
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
struct ActiveMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
}

#[cfg(not(any(target_os = "linux", windows)))]
impl ActiveMount {
    fn config(&self) -> &SifsMountConfig {
        &self.config
    }
    fn mount_point(&self) -> &Path {
        &self.mount_point
    }
    fn is_active(&mut self) -> bool {
        false
    }
    fn status(&mut self, daemon_pid: u32, restart_count: u64) -> SifsMountRuntimeStatus {
        status_for_config(
            &self.config,
            Some(&self.mount_point),
            daemon_pid,
            restart_count,
            SifsMountRuntimePhase::Error,
            "this platform has no SIFS host mount adapter",
        )
    }
    fn stop(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
fn start_platform_mount(
    _data_dir: &SinomoDataDir,
    _config: &SifsMountConfig,
    _mount_point: &Path,
    _shutdown: Arc<AtomicBool>,
    _mount_cancellation: &Mutex<Option<sinomo_runtime::dfs::DfsCancellationFlag>>,
    _lease: SifsMountLease,
) -> Result<ActiveMount, String> {
    Err("this platform has no SIFS host mount adapter".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_testdir::TestDir;

    const MOUNT_LEASE_PROBE_ROOT: &str = "SINOMO_TEST_SIFS_MOUNT_LEASE_ROOT";
    const MOUNT_LEASE_PROBE_EXPECT_AVAILABLE: &str =
        "SINOMO_TEST_SIFS_MOUNT_LEASE_EXPECT_AVAILABLE";

    fn retired_mount_fixture() -> RetiredMount {
        RetiredMount {
            config: SifsMountConfig {
                enabled: true,
                mount_point: "C:\\Sinomo\\Old".to_string(),
                root_path: "/old-root".to_string(),
                read_only: true,
                allow_other: false,
                sync_local_changes: false,
            },
            mount_point: PathBuf::from("C:\\Sinomo\\Old"),
            #[cfg(windows)]
            lease: None,
            #[cfg(windows)]
            module_provenance: module_provenance_fixture(
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
                "old-provider-hash",
            ),
        }
    }

    fn module_provenance_fixture(
        interface_version: &str,
        content_hash: &str,
    ) -> CloudFilesModuleProvenance {
        CloudFilesModuleProvenance {
            module_id: sinomo_runtime::module::ModuleId::from_legacy_named(
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID,
            )
            .expect("Cloud Files module id"),
            content_hash: content_hash.to_string(),
            cache_key: "cloud-files-cache".to_string(),
            library_name: "sinomo_dfs_cloud_files.dll".to_string(),
            platform_triple: "x86_64-pc-windows-gnu".to_string(),
            native_abi_version: sinomo_runtime::module::NATIVE_DYLIB_HOST_ABI_VERSION,
            interface_version: interface_version.to_string(),
        }
    }

    fn module_descriptor_fixture(
        provenance: &CloudFilesModuleProvenance,
    ) -> sinomo_runtime::module_api::NativeModuleDescriptor {
        use sinomo_runtime::dfs_cloud_files_api::{
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
        };
        sinomo_runtime::module_api::NativeModuleDescriptor {
            module_id: sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID
                .to_string(),
            module_version: "test".to_string(),
            owner: "test".to_string(),
            native_abi_version: provenance.native_abi_version,
            backend: sinomo_runtime::module_api::NativeModuleBackend::NativeDylib,
            interface_version: provenance.interface_version.clone(),
            platform_triple: provenance.platform_triple.clone(),
            supported_operations: vec![
                SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string(),
                SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION.to_string(),
                SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION.to_string(),
            ],
            capabilities: vec![
                SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY.to_string(),
            ],
            surfaces: Default::default(),
        }
    }

    #[test]
    fn cursor_recovery_bundle_replays_exact_execution_provider_successors() {
        let temp = TestDir::new("cursor-recovery-execution-provider-successor");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        std::fs::create_dir_all(data_dir.dfs_dir()).expect("DFS directory");
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");

        let authority_bytes = b"original cursor recovery authority provider".to_vec();
        let mut marker = host_cursor_recovery_marker_fixture(
            &data_dir,
            &mount_point,
            HostCursorRecoveryPhase::ArmedAwaitingReady,
        );
        marker.module.content_hash =
            sinomo_runtime::module::native_module_content_hash(&authority_bytes);
        marker.identity.provider_provenance_fingerprint =
            cloud_files_provenance_fingerprint(&marker.module).expect("authority fingerprint");
        let original = HostCursorRecoveryProviderBundle {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            requested: requested_host_cursor_recovery_marker(&marker),
            provider_bytes: authority_bytes,
            execution_module: None,
            execution_provider_bytes: None,
        };
        persist_host_cursor_recovery_provider_bundle(&data_dir, &original)
            .expect("original provider bundle");

        let execution_bytes = b"replacement cursor recovery execution provider".to_vec();
        let mut execution = marker.module.clone();
        execution.content_hash =
            sinomo_runtime::module::native_module_content_hash(&execution_bytes);
        execution.cache_key = "replacement-cloud-files-cache".to_string();
        let mut successor = original.clone();
        successor.execution_module = Some(execution.clone());
        successor.execution_provider_bytes = Some(execution_bytes.clone());
        let successor_bytes =
            sinomo_runtime::module_api::encode_sobj(&successor).expect("successor bytes");
        std::fs::write(
            host_cursor_recovery_provider_bundle_temp_path(&data_dir),
            successor_bytes,
        )
        .expect("durable successor cut");

        let snapshot = inspect_host_cursor_recovery_provider_bundle_for_resume(&data_dir)
            .expect("classify exact successor");
        assert!(snapshot.temporary);
        assert_eq!(snapshot.value.as_ref(), Some(&successor));
        promote_host_cursor_recovery_provider_bundle(&data_dir, &successor)
            .expect("promote exact successor");
        let (observed, path) =
            materialize_cursor_recovery_execution_provider(&data_dir, &successor)
                .expect("materialize execution provider")
                .expect("execution provider");
        assert_eq!(observed, execution);
        assert_eq!(
            std::fs::read(&path).expect("execution bytes"),
            execution_bytes
        );

        let mut changed = successor.clone();
        changed.provider_bytes.push(0);
        assert!(persist_host_cursor_recovery_provider_bundle(&data_dir, &changed).is_err());
        assert_eq!(
            inspect_host_cursor_recovery_provider_bundle_for_resume(&data_dir)
                .expect("unchanged bundle")
                .value,
            Some(successor.clone())
        );

        assert_eq!(
            materialize_cursor_recovery_execution_provider(&data_dir, &successor)
                .expect("replay execution cache pin")
                .expect("execution provider")
                .1,
            path
        );

        let second_execution_bytes = b"second replacement recovery execution provider".to_vec();
        let mut second_execution = execution.clone();
        second_execution.content_hash =
            sinomo_runtime::module::native_module_content_hash(&second_execution_bytes);
        second_execution.cache_key = "second-replacement-cloud-files-cache".to_string();
        let mut second_successor = successor.clone();
        second_successor.execution_module = Some(second_execution.clone());
        second_successor.execution_provider_bytes = Some(second_execution_bytes.clone());
        persist_host_cursor_recovery_provider_bundle(&data_dir, &second_successor)
            .expect("second execution-provider successor");
        let (observed, second_path) =
            materialize_cursor_recovery_execution_provider(&data_dir, &second_successor)
                .expect("materialize second execution provider")
                .expect("second execution provider");
        assert_eq!(observed, second_execution);
        assert_eq!(
            std::fs::read(second_path).expect("second execution bytes"),
            second_execution_bytes
        );
    }

    fn pending_record_fixture(data_dir: &SinomoDataDir) -> PendingCloudUnregisterRecord {
        pending_record_fixture_with_interface(
            data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            b"deterministic fake Cloud Files recovery provider",
        )
    }

    fn pending_record_fixture_with_interface(
        data_dir: &SinomoDataDir,
        interface_version: &str,
        provider_bytes: &[u8],
    ) -> PendingCloudUnregisterRecord {
        let retired = retired_mount_fixture();
        let provider_hash = sinomo_runtime::module::native_module_content_hash(provider_bytes);
        let provenance = module_provenance_fixture(interface_version, &provider_hash);
        let source = data_dir.root().join("fixture-cloud-files-provider.dll");
        std::fs::create_dir_all(data_dir.root()).expect("fixture data directory");
        std::fs::write(&source, provider_bytes).expect("fixture provider source");
        stage_cloud_unregister_recovery_module(data_dir, &provenance, &source)
            .expect("stage fixture provider");
        std::fs::remove_file(source).expect("remove cache-independent fixture source");
        pending_cloud_unregister_record(cloud_unregister_request(data_dir, &retired), provenance)
            .expect("pending unregister record")
    }

    fn staged_record_fixture(
        data_dir: &SinomoDataDir,
        mount_point: &Path,
        root_path: &str,
    ) -> PendingCloudUnregisterRecord {
        let base = pending_record_fixture(data_dir);
        pending_cloud_unregister_record(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: mount_point.to_path_buf(),
                root_path: root_path.to_string(),
                data_dir_root: Some(data_dir.root()),
                provider_name:
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                        .to_string(),
                cursor_recovery: None,
            },
            base.module,
        )
        .expect("staged registration recovery record")
    }

    fn may_be_registered_stage(
        recovery: &PendingCloudUnregisterRecord,
    ) -> StagedCloudRegistrationRecord {
        staged_cloud_registration_record(recovery, StagedCloudRegistrationState::MayBeRegistered)
            .expect("MayBeRegistered staged record")
    }

    fn windows_mount_config(mount_point: &Path, root_path: &str) -> SifsMountConfig {
        SifsMountConfig {
            enabled: true,
            mount_point: mount_point.display().to_string(),
            root_path: root_path.to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
        }
    }

    fn host_cursor_recovery_marker_fixture(
        data_dir: &SinomoDataDir,
        mount_point: &Path,
        phase: HostCursorRecoveryPhase,
    ) -> HostCursorRecoveryMarker {
        let mut disabled = windows_mount_config(mount_point, "/root");
        disabled.enabled = false;
        let module = module_provenance_fixture(
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            &format!("blake3:{}", "a".repeat(64)),
        );
        let inner_token = format!("v1:blake3:{}", "b".repeat(64));
        let recovery_id = "cursor-recovery-host-fixture".to_string();
        let identity = sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryIdentity {
            schema_version:
                sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            enabled: false,
            sync_root: mount_point.to_path_buf(),
            root_path: "/root".to_string(),
            data_dir_root: data_dir.root(),
            provider_name:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                    .to_string(),
            provider_version: env!("CARGO_PKG_VERSION").to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
            prepopulate_depth: 0,
            connect: true,
            max_run_seconds: None,
            disabled_config_fingerprint: disabled_mount_config_fingerprint(&disabled)
                .expect("config fingerprint"),
            provider_provenance_fingerprint: cloud_files_provenance_fingerprint(&module)
                .expect("provider fingerprint"),
        };
        let inspection =
            sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryInspection {
                schema_version:
                    sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
                sync_root: mount_point.to_path_buf(),
                root_path: "/root".to_string(),
                sync_root_file_id: 41,
                cursor_count: 3,
                raw_state_fingerprint: format!("blake3:{}", "c".repeat(64)),
                recovery_token: inner_token.clone(),
                detail: "fixture".to_string(),
            };
        let applied = sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorOnlyRecoveryApplied {
            schema_version:
                sinomo_runtime::dfs_cloud_files_api::CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            sync_root: mount_point.to_path_buf(),
            root_path: "/root".to_string(),
            sync_root_file_id: 41,
            cursor_count_cleared: 3,
            recovery_id: recovery_id.clone(),
            recovery_token: inner_token.clone(),
            detail: "fixture".to_string(),
        };
        HostCursorRecoveryMarker {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            disabled_config: disabled,
            module,
            identity,
            outer_token: format!("v1:blake3:{}", "d".repeat(64)),
            inner_token,
            recovery_id,
            inspection,
            applied: Some(applied),
            ready_artifacts_fingerprint: matches!(
                phase,
                HostCursorRecoveryPhase::ReadyObserved
                    | HostCursorRecoveryPhase::ActiveObserved
                    | HostCursorRecoveryPhase::AuthorityFinalized
            )
            .then(|| format!("blake3:{}", "e".repeat(64))),
            preserved_artifacts_fingerprint: (phase == HostCursorRecoveryPhase::PreservedRetained)
                .then(|| format!("blake3:{}", "f".repeat(64))),
            active_module_marker_fingerprint: matches!(
                phase,
                HostCursorRecoveryPhase::ActiveObserved
                    | HostCursorRecoveryPhase::AuthorityFinalized
            )
            .then(|| format!("blake3:{}", "1".repeat(64))),
            completion_module_marker_fingerprint: (phase
                == HostCursorRecoveryPhase::AuthorityFinalized)
                .then(|| format!("blake3:{}", "2".repeat(64))),
            completion_registration_artifacts_fingerprint: (phase
                == HostCursorRecoveryPhase::AuthorityFinalized)
                .then(|| format!("blake3:{}", "4".repeat(64))),
            completed_artifacts_fingerprint: None,
            recovery_cycle: u64::from(phase == HostCursorRecoveryPhase::PreservedRetained),
            native_successor_fingerprint: (phase == HostCursorRecoveryPhase::PreservedRetained)
                .then(|| format!("blake3:{}", "3".repeat(64))),
            phase,
        }
    }

    #[cfg(windows)]
    struct HostTerminalFixture {
        data_dir: SinomoDataDir,
        marker: HostCursorRecoveryMarker,
        active_request:
            sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryActiveObservedRequest,
        cursor_stage: StagedCloudRegistrationRecord,
        ordinary_stage: StagedCloudRegistrationRecord,
        ordinary_journal: Vec<u8>,
    }

    #[cfg(windows)]
    fn host_terminal_fixture(temp: &TestDir) -> HostTerminalFixture {
        use sinomo_runtime::dfs_cloud_files_api::{
            CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            CloudFilesCursorRecoveryActiveObservedRequest, CloudFilesCursorRecoveryContext,
            CloudFilesRegistrationReceipt,
        };

        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("terminal fixture mount point");
        let pinned = pending_record_fixture(&data_dir);
        let provider_path = verify_cloud_unregister_recovery_module(&data_dir, &pinned.module)
            .expect("terminal fixture provider");
        let provider_bytes = std::fs::read(provider_path).expect("terminal fixture provider bytes");
        let seed = SifsMountLease::try_acquire(&data_dir, "terminal fixture seed")
            .expect("terminal fixture seed lease");
        drop(seed);
        let (observed, lock_bytes) =
            SifsMountLease::try_acquire_existing_for_cursor_recovery(&data_dir)
                .expect("read terminal fixture host lock through its owner handle");
        drop(observed);

        let mut marker = host_cursor_recovery_marker_fixture(
            &data_dir,
            &mount_point,
            HostCursorRecoveryPhase::ReadyObserved,
        );
        marker.module = pinned.module;
        marker.identity.provider_provenance_fingerprint =
            cloud_files_provenance_fingerprint(&marker.module).expect("provider fingerprint");
        marker.outer_token = host_cursor_recovery_outer_token(
            &marker.disabled_config,
            &marker.module,
            &lock_bytes,
            &marker.identity,
            &marker.inspection,
        )
        .expect("terminal outer token");
        marker.recovery_id = deterministic_host_cursor_recovery_id(&marker.outer_token);
        marker
            .applied
            .as_mut()
            .expect("terminal applied fixture")
            .recovery_id = marker.recovery_id.clone();

        let cursor_recovery = pending_cloud_unregister_record_with_policy(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                data_dir_root: Some(marker.identity.data_dir_root.clone()),
                provider_name: marker.identity.provider_name.clone(),
                cursor_recovery: Some(CloudFilesCursorRecoveryContext {
                    recovery_id: marker.recovery_id.clone(),
                    recovery_token: marker.inner_token.clone(),
                    recovery_cycle: marker.recovery_cycle,
                    native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
                }),
            },
            marker.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )
        .expect("terminal cursor recovery record");
        let cursor_stage = staged_cloud_registration_record(
            &cursor_recovery,
            StagedCloudRegistrationState::MayBeRegistered,
        )
        .expect("terminal cursor stage");
        let ordinary_recovery = pending_cloud_unregister_record_with_policy(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                cursor_recovery: None,
                ..cursor_recovery.request.clone()
            },
            marker.module.clone(),
            CloudUnregisterPolicy::Release,
        )
        .expect("terminal ordinary recovery record");
        let ordinary_stage = staged_cloud_registration_record(
            &ordinary_recovery,
            StagedCloudRegistrationState::MayBeRegistered,
        )
        .expect("terminal ordinary stage");

        let receipt = sinomo_runtime::module_api::encode_sobj(&CloudFilesRegistrationReceipt {
            interface_version:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    .to_string(),
            registration_identity_interface_version: None,
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            provider_name: marker.identity.provider_name.clone(),
        })
        .expect("terminal receipt bytes");
        let recovery_journal = b"canonical-recovery-ready-journal".to_vec();
        let ordinary_journal = b"canonical-ordinary-ready-journal".to_vec();
        marker.ready_artifacts_fingerprint = Some(
            cloud_files_ready_artifacts_fingerprint_from_raw(&receipt, &recovery_journal),
        );

        let bundle = HostCursorRecoveryProviderBundle {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            requested: requested_host_cursor_recovery_marker(&marker),
            provider_bytes,
            execution_module: None,
            execution_provider_bytes: None,
        };
        persist_host_cursor_recovery_provider_bundle(&data_dir, &bundle)
            .expect("terminal provider bundle");
        stage_host_cursor_recovery_marker(&data_dir, &marker).expect("terminal host marker");
        commit_staged_host_cursor_recovery_marker(&data_dir).expect("commit terminal host marker");
        persist_or_promote_cursor_recovery_staged_record(&data_dir, &cursor_stage)
            .expect("terminal cursor stage");
        let runtime = cloud_files_module_runtime_path(&data_dir, "fixture");
        std::fs::create_dir_all(runtime.parent().expect("terminal runtime parent"))
            .expect("terminal runtime");
        std::fs::write(cloud_files_registration_receipt_path(&data_dir), &receipt)
            .expect("terminal receipt");
        std::fs::write(
            cloud_files_module_runtime_path(&data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            &recovery_journal,
        )
        .expect("terminal recovery journal");
        let module_marker = b"canonical-native-namespace-finalized-marker".to_vec();
        std::fs::write(
            cloud_files_module_runtime_path(
                &data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ),
            &module_marker,
        )
        .expect("terminal native marker");
        configure_managed_sifs_mount(
            &data_dir,
            &SifsMountConfig {
                enabled: true,
                ..marker.disabled_config.clone()
            },
        )
        .expect("terminal enabled config");

        let module_marker_fingerprint =
            cloud_files_module_marker_raw_fingerprint(&data_dir).expect("module fingerprint");
        let completed_registration_artifacts_fingerprint =
            cloud_files_completed_registration_artifacts_fingerprint_from_raw(
                &receipt,
                &ordinary_journal,
            );
        let active_request = CloudFilesCursorRecoveryActiveObservedRequest {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            recovery_id: marker.recovery_id.clone(),
            recovery_token: marker.inner_token.clone(),
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            sync_root_file_id: marker.inspection.sync_root_file_id,
            recovery_cycle: marker.recovery_cycle,
            module_marker_fingerprint,
            completed_registration_artifacts_fingerprint,
        };
        HostTerminalFixture {
            data_dir,
            marker,
            active_request,
            cursor_stage,
            ordinary_stage,
            ordinary_journal,
        }
    }

    #[cfg(windows)]
    fn advance_host_terminal_fixture_to_completion_authorized(fixture: &mut HostTerminalFixture) {
        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ),
            b"canonical-native-completion-authorized-marker",
        )
        .expect("terminal CompletionAuthorized native marker");
        fixture.active_request.module_marker_fingerprint =
            cloud_files_module_marker_raw_fingerprint(&fixture.data_dir)
                .expect("CompletionAuthorized module fingerprint");
    }

    #[cfg(windows)]
    fn host_terminal_active_mount_and_callbacks(
        fixture: &HostTerminalFixture,
        worker_finished: bool,
    ) -> (ActiveMount, CloudFilesSupervisorCallbacks) {
        let (lease, _) =
            SifsMountLease::try_acquire_existing_for_cursor_recovery(&fixture.data_dir)
                .expect("terminal active fixture existing lease");
        let lease = Arc::new(lease);
        let cancel = Arc::new(AtomicBool::new(false));
        let supervisor_shutdown = Arc::new(AtomicBool::new(false));
        let operation_fence = Arc::new(Mutex::new(()));
        let (event_tx, event_rx) = mpsc::sync_channel(8);
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let (thread, finished_result) = if worker_finished {
            drop(finished_tx);
            (
                None,
                Some(Err(
                    "terminal fixture provider exited after AuthorityFinalized".to_string(),
                )),
            )
        } else {
            let worker_cancel = Arc::clone(&cancel);
            let thread = thread::Builder::new()
                .name("cursor-terminal-fixture-worker".to_string())
                .spawn(move || {
                    while !worker_cancel.load(Ordering::SeqCst) {
                        thread::yield_now();
                    }
                    let _ = finished_tx.send(Ok(()));
                })
                .expect("terminal fixture worker");
            (Some(thread), None)
        };
        let enabled = SifsMountConfig {
            enabled: true,
            ..fixture.marker.disabled_config.clone()
        };
        let active = ActiveMount {
            config: enabled,
            mount_point: fixture.marker.identity.sync_root.clone(),
            cancel: Arc::clone(&cancel),
            events: event_rx,
            finished: finished_rx,
            thread,
            lifecycle: None,
            finished_result,
            provider_content_hash: fixture.marker.module.content_hash.clone(),
            provider_interface_version: fixture.marker.module.interface_version.clone(),
            module_provenance: fixture.marker.module.clone(),
            lease,
            operation_fence: Arc::clone(&operation_fence),
            last_health_generation: 0,
            last_health_observed: None,
            last_health_observed_at: None,
        };
        let callbacks = CloudFilesSupervisorCallbacks {
            cancel,
            supervisor_shutdown,
            events: event_tx,
            data_dir: fixture.data_dir.clone(),
            cursor_recovery: Some(fixture.marker.clone()),
            operation_fence,
        };
        (active, callbacks)
    }

    #[cfg(windows)]
    fn host_terminal_active_event_payload(
        request: &sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryActiveObservedRequest,
    ) -> Vec<u8> {
        sinomo_runtime::module_api::encode_sobj(
            &sinomo_runtime::dfs_cloud_files_api::CloudFilesModuleEvent::CursorRecoveryActiveObserved(
                request.clone(),
            ),
        )
        .expect("terminal ActiveObserved event payload")
    }

    #[cfg(windows)]
    #[test]
    fn active_callback_drives_authority_finalized_normalization_and_release_cuts() {
        let temp = TestDir::new("sifs-cursor-terminal-active-callback");
        let mut fixture = host_terminal_fixture(&temp);
        let cancel = AtomicBool::new(false);
        let shutdown = AtomicBool::new(false);

        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("NamespaceFinalized acknowledgement");
        let active = load_host_cursor_recovery_marker_strict(&fixture.data_dir)
            .expect("load ActiveObserved marker")
            .expect("ActiveObserved marker");
        assert_eq!(active.phase, HostCursorRecoveryPhase::ActiveObserved);
        assert_eq!(
            active.active_module_marker_fingerprint.as_deref(),
            Some(fixture.active_request.module_marker_fingerprint.as_str())
        );

        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("CompletionAuthorized acknowledgement");
        let authority_finalized = load_host_cursor_recovery_marker_strict(&fixture.data_dir)
            .expect("load AuthorityFinalized marker")
            .expect("AuthorityFinalized marker");
        assert_eq!(
            authority_finalized.phase,
            HostCursorRecoveryPhase::AuthorityFinalized
        );
        assert_eq!(
            authority_finalized
                .completion_registration_artifacts_fingerprint
                .as_deref(),
            Some(
                fixture
                    .active_request
                    .completed_registration_artifacts_fingerprint
                    .as_str()
            )
        );

        // Duplicate AF acknowledgement is an idempotent predecessor replay.
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("idempotent AuthorityFinalized acknowledgement");
        assert_eq!(
            load_staged_cloud_registration_record(&fixture.data_dir)
                .expect("cursor stage before native normalization"),
            Some(fixture.cursor_stage.clone())
        );

        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
            ),
            &fixture.ordinary_journal,
        )
        .expect("native normalized ordinary journal");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("normalization acknowledgement");
        let normalized = load_host_cursor_recovery_marker_strict(&fixture.data_dir)
            .expect("load normalized host marker")
            .expect("normalized host marker");
        assert!(normalized.completed_artifacts_fingerprint.is_some());
        assert_eq!(
            load_staged_cloud_registration_record(&fixture.data_dir)
                .expect("ordinary stage after normalization"),
            Some(fixture.ordinary_stage.clone())
        );

        std::fs::remove_file(cloud_files_module_runtime_path(
            &fixture.data_dir,
            CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        ))
        .expect("native clears CompletionAuthorized marker");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("final authority release acknowledgement");
        assert!(
            load_host_cursor_recovery_marker_strict(&fixture.data_dir)
                .expect("host marker after release")
                .is_none()
        );
        assert!(!host_cursor_recovery_provider_bundle_path(&fixture.data_dir).exists());
        assert!(!cancel.load(Ordering::SeqCst));

        let temp = TestDir::new("sifs-cursor-terminal-disabled-final-ack");
        let mut fixture = host_terminal_fixture(&temp);
        let cancel = AtomicBool::new(false);
        let shutdown = AtomicBool::new(false);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled fixture Ready to Active");
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled fixture Active to AF");
        configure_managed_sifs_mount(&fixture.data_dir, &fixture.marker.disabled_config)
            .expect("disable after AF linearization");
        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
            ),
            &fixture.ordinary_journal,
        )
        .expect("disabled fixture ordinary journal");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled fixture normalization");
        std::fs::remove_file(cloud_files_module_runtime_path(
            &fixture.data_dir,
            CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        ))
        .expect("disabled fixture native marker clear");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled fixture final ACK");
        assert!(cancel.load(Ordering::SeqCst));
    }

    #[cfg(windows)]
    #[test]
    fn operation_fence_serializes_unregister_first_and_authority_finalized_first() {
        let temp = TestDir::new("sifs-cursor-terminal-fence-u-first");
        let mut fixture = host_terminal_fixture(&temp);
        let (active_mount, callbacks) = host_terminal_active_mount_and_callbacks(&fixture, false);
        let mut active = Some(active_mount);
        let mut pending = None;
        let mount_cancellation = Mutex::new(None);
        assert!(
            callbacks.handle_output(&host_terminal_active_event_payload(&fixture.active_request,))
        );
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        let ca_payload = host_terminal_active_event_payload(&fixture.active_request);
        configure_managed_sifs_mount(&fixture.data_dir, &fixture.marker.disabled_config)
            .expect("persist U-first disabled consent");
        let pending_path = pending_cloud_unregister_path(&fixture.data_dir);
        let start = Arc::new(std::sync::Barrier::new(2));
        let (retire_result, callback_result) = thread::scope(|scope| {
            let retire_start = Arc::clone(&start);
            let data_dir = &fixture.data_dir;
            let active = &mut active;
            let pending = &mut pending;
            let mount_cancellation = &mount_cancellation;
            let retire = scope.spawn(move || {
                retire_start.wait();
                retire_active_mount(
                    data_dir,
                    active,
                    mount_cancellation,
                    pending,
                    MountRetirementReason::ExplicitDisable,
                )
            });
            let callback_start = Arc::clone(&start);
            let callbacks = &callbacks;
            let ca_payload = &ca_payload;
            let pending_path = &pending_path;
            let callback = scope.spawn(move || {
                callback_start.wait();
                let deadline = Instant::now() + Duration::from_secs(5);
                while !pending_path.exists() && Instant::now() < deadline {
                    thread::yield_now();
                }
                assert!(pending_path.exists(), "retirement must linearize U first");
                callbacks.handle_output(&ca_payload)
            });
            (
                retire.join().expect("U-first retirement thread"),
                callback.join().expect("U-first callback thread"),
            )
        });
        assert!(retire_result.is_err());
        assert!(!callback_result);
        assert!(pending.is_some());
        assert!(pending_path.exists());
        assert_eq!(
            load_host_cursor_recovery_marker_strict(&fixture.data_dir)
                .expect("U-first host marker")
                .expect("U-first host authority")
                .phase,
            HostCursorRecoveryPhase::ActiveObserved
        );

        let temp = TestDir::new("sifs-cursor-terminal-fence-af-first");
        let mut fixture = host_terminal_fixture(&temp);
        let (active_mount, callbacks) = host_terminal_active_mount_and_callbacks(&fixture, false);
        let mut active = Some(active_mount);
        let mut pending = None;
        let mount_cancellation = Mutex::new(None);
        assert!(
            callbacks.handle_output(&host_terminal_active_event_payload(&fixture.active_request,))
        );
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        let ca_payload = host_terminal_active_event_payload(&fixture.active_request);
        let start = Arc::new(std::sync::Barrier::new(2));
        thread::scope(|scope| {
            let callback_start = Arc::clone(&start);
            let callbacks = &callbacks;
            let ca_payload = &ca_payload;
            let callback = scope.spawn(move || {
                callback_start.wait();
                callbacks.handle_output(&ca_payload)
            });
            start.wait();
            assert!(callback.join().expect("AF-first callback thread"));
        });
        assert_eq!(
            load_host_cursor_recovery_marker_strict(&fixture.data_dir)
                .expect("AF-first host marker")
                .expect("AF-first host authority")
                .phase,
            HostCursorRecoveryPhase::AuthorityFinalized
        );
        configure_managed_sifs_mount(&fixture.data_dir, &fixture.marker.disabled_config)
            .expect("persist AF-first disabled consent");
        retire_active_mount(
            &fixture.data_dir,
            &mut active,
            &mount_cancellation,
            &mut pending,
            MountRetirementReason::ExplicitDisable,
        )
        .expect("live AF worker remains parked");
        assert!(active.is_some());
        assert!(pending.is_none());
        assert!(!pending_cloud_unregister_path(&fixture.data_dir).exists());
        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
            ),
            &fixture.ordinary_journal,
        )
        .expect("AF-first ordinary journal");
        assert!(callbacks.handle_output(&ca_payload));
        std::fs::remove_file(cloud_files_module_runtime_path(
            &fixture.data_dir,
            CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        ))
        .expect("AF-first native authority release");
        assert!(callbacks.handle_output(&ca_payload));
        assert!(callbacks.cancel.load(Ordering::SeqCst));
        stop_active_mount(&mut active, &mount_cancellation).expect("join cancelled AF worker");

        let temp = TestDir::new("sifs-cursor-terminal-fence-af-finished");
        let mut fixture = host_terminal_fixture(&temp);
        let (active_mount, callbacks) = host_terminal_active_mount_and_callbacks(&fixture, true);
        let mut active = Some(active_mount);
        let mut pending = None;
        let mount_cancellation = Mutex::new(None);
        assert!(
            callbacks.handle_output(&host_terminal_active_event_payload(&fixture.active_request,))
        );
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        assert!(
            callbacks.handle_output(&host_terminal_active_event_payload(&fixture.active_request,))
        );
        configure_managed_sifs_mount(&fixture.data_dir, &fixture.marker.disabled_config)
            .expect("persist finished-AF disabled consent");
        retire_active_mount(
            &fixture.data_dir,
            &mut active,
            &mount_cancellation,
            &mut pending,
            MountRetirementReason::ExplicitDisable,
        )
        .expect("retire finished AF worker without Preserve");
        assert!(active.is_none());
        assert!(pending.is_none());
        assert!(!pending_cloud_unregister_path(&fixture.data_dir).exists());
        assert_eq!(
            load_host_cursor_recovery_marker_strict(&fixture.data_dir)
                .expect("finished-AF host marker")
                .expect("finished-AF host authority")
                .phase,
            HostCursorRecoveryPhase::AuthorityFinalized
        );
    }

    #[cfg(windows)]
    #[test]
    fn active_callback_rejects_u_first_and_terminal_temps_before_host_mutation() {
        for evidence in ["pending", "journal-temp", "stage-temp", "changed-proof"] {
            let temp = TestDir::new(&format!("sifs-cursor-terminal-reject-{evidence}"));
            let mut fixture = host_terminal_fixture(&temp);
            match evidence {
                "pending" => {
                    let record = pending_cloud_unregister_record_with_policy(
                        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                            sync_root: fixture.marker.identity.sync_root.clone(),
                            root_path: fixture.marker.identity.root_path.clone(),
                            data_dir_root: Some(fixture.marker.identity.data_dir_root.clone()),
                            provider_name: fixture.marker.identity.provider_name.clone(),
                            cursor_recovery: Some(host_cursor_recovery_context(&fixture.marker)),
                        },
                        fixture.marker.module.clone(),
                        CloudUnregisterPolicy::PreserveState,
                    )
                    .expect("U-first pending record");
                    persist_pending_cloud_unregister_record(&fixture.data_dir, &record)
                        .expect("U-first durable intent");
                }
                "journal-temp" => std::fs::write(
                    cloud_files_registration_journal_temp_path(&fixture.data_dir),
                    b"unexpected-journal-temp",
                )
                .expect("journal temp"),
                "stage-temp" => std::fs::write(
                    cursor_recovery_staged_temp_path(&fixture.data_dir),
                    sinomo_runtime::module_api::encode_sobj(&fixture.ordinary_stage)
                        .expect("ordinary stage temp bytes"),
                )
                .expect("stage temp"),
                "changed-proof" => {
                    fixture.active_request.module_marker_fingerprint =
                        format!("blake3:{}", "9".repeat(64));
                }
                _ => unreachable!(),
            }
            let marker_before = std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                .expect("host marker before rejection");
            let stage_before = inspect_cursor_recovery_staged_record_snapshot(&fixture.data_dir)
                .expect("stage before rejection");
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &AtomicBool::new(false),
                &AtomicBool::new(false),
            )
            .expect_err("terminal ambiguity must fail closed");
            assert_eq!(
                std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                    .expect("host marker after rejection"),
                marker_before
            );
            let stage_after = inspect_cursor_recovery_staged_record_snapshot(&fixture.data_dir)
                .expect("stage after rejection");
            assert_eq!(stage_after.current, stage_before.current);
            assert_eq!(stage_after.temporary, stage_before.temporary);
        }
    }

    #[cfg(windows)]
    #[test]
    fn authority_finalized_restart_cleans_bundle_first_and_rejects_orphan_stage_temp() {
        for bundle_present in [true, false] {
            let temp = TestDir::new(if bundle_present {
                "sifs-cursor-terminal-restart-bundle-present"
            } else {
                "sifs-cursor-terminal-restart-bundle-absent"
            });
            let mut fixture = host_terminal_fixture(&temp);
            let cancel = AtomicBool::new(false);
            let shutdown = AtomicBool::new(false);
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &cancel,
                &shutdown,
            )
            .expect("Ready to Active");
            advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &cancel,
                &shutdown,
            )
            .expect("Active to AuthorityFinalized");
            std::fs::write(
                cloud_files_module_runtime_path(
                    &fixture.data_dir,
                    CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
                ),
                &fixture.ordinary_journal,
            )
            .expect("ordinary journal");
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &cancel,
                &shutdown,
            )
            .expect("host normalization");
            std::fs::remove_file(cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
            ))
            .expect("native marker clear");
            if !bundle_present {
                std::fs::remove_file(host_cursor_recovery_provider_bundle_path(&fixture.data_dir))
                    .expect("simulate bundle-first crash");
            }
            let enabled = SifsMountConfig {
                enabled: true,
                ..fixture.marker.disabled_config.clone()
            };
            assert!(
                resume_active_observed_host_cursor_recovery_cleanup(&fixture.data_dir, &enabled)
                    .expect("restart final cleanup")
            );
            assert!(!host_cursor_recovery_marker_path(&fixture.data_dir).exists());
            assert!(!host_cursor_recovery_provider_bundle_path(&fixture.data_dir).exists());
        }

        let temp = TestDir::new("sifs-cursor-terminal-disabled-restart");
        let mut fixture = host_terminal_fixture(&temp);
        let cancel = AtomicBool::new(false);
        let shutdown = AtomicBool::new(false);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled restart Ready to Active");
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled restart Active to AF");
        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
            ),
            &fixture.ordinary_journal,
        )
        .expect("disabled restart ordinary journal");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("disabled restart normalization");
        std::fs::remove_file(cloud_files_module_runtime_path(
            &fixture.data_dir,
            CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        ))
        .expect("disabled restart native marker clear");
        configure_managed_sifs_mount(&fixture.data_dir, &fixture.marker.disabled_config)
            .expect("persist disabled restart consent");
        assert!(
            resume_active_observed_host_cursor_recovery_cleanup(
                &fixture.data_dir,
                &fixture.marker.disabled_config,
            )
            .expect("disabled restart host cleanup")
        );

        let temp = TestDir::new("sifs-cursor-terminal-restart-stage-temp");
        let mut fixture = host_terminal_fixture(&temp);
        let cancel = AtomicBool::new(false);
        let shutdown = AtomicBool::new(false);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("Ready to Active");
        advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("Active to AF");
        std::fs::write(
            cloud_files_module_runtime_path(
                &fixture.data_dir,
                CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
            ),
            &fixture.ordinary_journal,
        )
        .expect("ordinary journal");
        acknowledge_host_cursor_recovery_active_observed(
            &fixture.data_dir,
            &fixture.marker,
            &fixture.active_request,
            &cancel,
            &shutdown,
        )
        .expect("host normalization");
        std::fs::remove_file(cloud_files_module_runtime_path(
            &fixture.data_dir,
            CLOUD_FILES_CURSOR_RECOVERY_MODULE_MARKER_FILE,
        ))
        .expect("native marker clear");
        std::fs::write(
            cursor_recovery_staged_temp_path(&fixture.data_dir),
            sinomo_runtime::module_api::encode_sobj(&fixture.ordinary_stage)
                .expect("orphan stage temp bytes"),
        )
        .expect("orphan stage temp");
        let marker_before = std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
            .expect("AF marker before orphan temp rejection");
        resume_active_observed_host_cursor_recovery_cleanup(
            &fixture.data_dir,
            &SifsMountConfig {
                enabled: true,
                ..fixture.marker.disabled_config.clone()
            },
        )
        .expect_err("orphan stage temp blocks authority release");
        assert_eq!(
            std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                .expect("AF marker after orphan temp rejection"),
            marker_before
        );
    }

    #[cfg(windows)]
    #[test]
    fn authority_finalized_restart_classifies_exact_native_and_stage_normalization_cuts() {
        for cut in [
            "journal-successor-temp",
            "stage-successor-temp",
            "changed-journal-temp",
            "changed-stage-temp",
            "missing-bundle",
        ] {
            let temp = TestDir::new(&format!("sifs-cursor-terminal-af-restart-{cut}"));
            let mut fixture = host_terminal_fixture(&temp);
            let cancel = AtomicBool::new(false);
            let shutdown = AtomicBool::new(false);
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &cancel,
                &shutdown,
            )
            .expect("restart fixture Ready to Active");
            advance_host_terminal_fixture_to_completion_authorized(&mut fixture);
            acknowledge_host_cursor_recovery_active_observed(
                &fixture.data_dir,
                &fixture.marker,
                &fixture.active_request,
                &cancel,
                &shutdown,
            )
            .expect("restart fixture Active to AF");
            let marker = load_host_cursor_recovery_marker_strict(&fixture.data_dir)
                .expect("load AF restart marker")
                .expect("AF restart marker");
            match cut {
                "journal-successor-temp" => std::fs::write(
                    cloud_files_registration_journal_temp_path(&fixture.data_dir),
                    &fixture.ordinary_journal,
                )
                .expect("ordinary journal successor temp"),
                "stage-successor-temp" => {
                    std::fs::write(
                        cloud_files_module_runtime_path(
                            &fixture.data_dir,
                            CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
                        ),
                        &fixture.ordinary_journal,
                    )
                    .expect("ordinary journal final");
                    std::fs::write(
                        cursor_recovery_staged_temp_path(&fixture.data_dir),
                        sinomo_runtime::module_api::encode_sobj(&fixture.ordinary_stage)
                            .expect("ordinary stage successor bytes"),
                    )
                    .expect("ordinary stage successor temp");
                }
                "changed-journal-temp" => std::fs::write(
                    cloud_files_registration_journal_temp_path(&fixture.data_dir),
                    b"changed ordinary journal successor",
                )
                .expect("changed journal temp"),
                "changed-stage-temp" => {
                    std::fs::write(
                        cloud_files_module_runtime_path(
                            &fixture.data_dir,
                            CLOUD_FILES_REGISTRATION_JOURNAL_FILE,
                        ),
                        &fixture.ordinary_journal,
                    )
                    .expect("ordinary journal final");
                    let mut changed = fixture.ordinary_stage.clone();
                    changed.recovery.request.root_path = "/changed".to_string();
                    std::fs::write(
                        cursor_recovery_staged_temp_path(&fixture.data_dir),
                        sinomo_runtime::module_api::encode_sobj(&changed)
                            .expect("changed stage temp bytes"),
                    )
                    .expect("changed stage temp");
                }
                "missing-bundle" => {
                    std::fs::remove_file(host_cursor_recovery_provider_bundle_path(
                        &fixture.data_dir,
                    ))
                    .expect("remove required AF bundle");
                }
                _ => unreachable!(),
            }
            let positive = matches!(cut, "journal-successor-temp" | "stage-successor-temp");
            if positive {
                let classified =
                    validate_authority_finalized_completion_cut(&fixture.data_dir, &marker)
                        .expect("classify exact AF normalization cut");
                assert_eq!(
                    classified.registration_normalized,
                    cut == "stage-successor-temp"
                );
                assert!(!classified.stage_normalized);
                assert!(
                    !resume_active_observed_host_cursor_recovery_cleanup(
                        &fixture.data_dir,
                        &SifsMountConfig {
                            enabled: true,
                            ..fixture.marker.disabled_config.clone()
                        },
                    )
                    .expect("AF+CA restart defers to pinned native completion")
                );
            } else if cut == "missing-bundle" {
                let marker_before =
                    std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                        .expect("AF marker before missing-bundle admission");
                admit_host_cursor_recovery_enable(
                    &fixture.data_dir,
                    &SifsMountConfig {
                        enabled: true,
                        ..fixture.marker.disabled_config.clone()
                    },
                )
                .expect_err("AF+CA admission requires its durable provider bundle");
                assert_eq!(
                    std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                        .expect("AF marker after missing-bundle admission"),
                    marker_before
                );
            } else {
                let marker_before =
                    std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                        .expect("AF marker before rejected restart cut");
                let result = resume_active_observed_host_cursor_recovery_cleanup(
                    &fixture.data_dir,
                    &SifsMountConfig {
                        enabled: true,
                        ..fixture.marker.disabled_config.clone()
                    },
                );
                assert!(result.is_err(), "{cut} must fail closed");
                assert_eq!(
                    std::fs::read(host_cursor_recovery_marker_path(&fixture.data_dir))
                        .expect("AF marker after rejected restart cut"),
                    marker_before
                );
            }
        }
    }

    #[test]
    fn cloud_unregister_transition_policy_distinguishes_explicit_retirement() {
        assert!(pending_unregister_for_transition(
            MountRetirementReason::ExplicitDisable
        ));
        for reason in [
            MountRetirementReason::RegistrationReplacement,
            MountRetirementReason::ConfigurationRestart,
            MountRetirementReason::InvalidConfiguration,
            MountRetirementReason::ConfigurationUnavailable,
            MountRetirementReason::DaemonReadinessLoss,
            MountRetirementReason::ProviderRestart,
            MountRetirementReason::ServiceShutdown,
        ] {
            assert!(
                !pending_unregister_for_transition(reason),
                "{reason:?} must not unregister a persistent registration"
            );
        }

        let old = retired_mount_fixture().config;
        let mut mode_only = old.clone();
        mode_only.read_only = false;
        assert!(!cloud_registration_identity_changed(&old, &mode_only));
        let mut new_root = old.clone();
        new_root.root_path = "/replacement".to_string();
        assert!(cloud_registration_identity_changed(&old, &new_root));
        let mut new_mount = old.clone();
        new_mount.mount_point = "C:\\Sinomo\\Replacement".to_string();
        assert!(cloud_registration_identity_changed(&old, &new_mount));
    }

    #[test]
    fn legacy_unregister_payload_defaults_to_releasing_ownership() {
        #[derive(serde::Serialize)]
        struct LegacyCloudFilesUnregisterRequest {
            sync_root: PathBuf,
            root_path: String,
            data_dir_root: Option<PathBuf>,
            provider_name: String,
        }

        let legacy = LegacyCloudFilesUnregisterRequest {
            sync_root: "C:\\SinomoSIFS".into(),
            root_path: "/services".to_string(),
            data_dir_root: Some("C:\\Users\\test\\.sinomo".into()),
            provider_name: "Sinomo SIFS".to_string(),
        };
        let bytes =
            sinomo_runtime::module_api::encode_sobj(&legacy).expect("encode legacy unregister");
        let decoded = sinomo_runtime::module_api::decode_sobj::<
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest,
        >(&bytes)
        .expect("decode legacy unregister");

        assert_eq!(decoded.sync_root, legacy.sync_root);
        assert_eq!(decoded.root_path, legacy.root_path);
        assert_eq!(decoded.data_dir_root, legacy.data_dir_root);
        assert_eq!(decoded.provider_name, legacy.provider_name);
        assert_eq!(
            sinomo_runtime::module_api::encode_sobj(&decoded)
                .expect("re-encode compatible unregister"),
            bytes
        );
    }

    #[test]
    fn legacy_staged_descriptor_decodes_as_policy_neutral_may_be_registered() {
        let temp = TestDir::new("sifs-cloud-legacy-staged-state");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mut legacy = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
            b"deterministic deployed v2 Cloud Files provider",
        );
        legacy.schema_version = 1;
        let legacy_provider = legacy.module.clone();
        let path = staged_cloud_registration_path(&data_dir);
        std::fs::create_dir_all(path.parent().expect("staged parent")).expect("staged dir");
        std::fs::write(
            &path,
            sinomo_runtime::module_api::encode_sobj(&legacy).expect("legacy staged bytes"),
        )
        .expect("legacy staged descriptor");

        let decoded = load_staged_cloud_registration_record(&data_dir)
            .expect("decode legacy stage")
            .expect("legacy stage present");

        assert_eq!(decoded.state, StagedCloudRegistrationState::MayBeRegistered);
        assert_eq!(
            decoded.recovery.operation,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
        );
        assert_eq!(
            decoded.recovery.schema_version,
            PENDING_CLOUD_UNREGISTER_SCHEMA_VERSION
        );
        assert_eq!(decoded.recovery.request, legacy.request);
        assert_eq!(decoded.recovery.module, legacy_provider);
        assert_eq!(
            decoded.recovery.legacy_prejournal_origin,
            Some(legacy_provider)
        );
    }

    #[test]
    fn malformed_legacy_staged_descriptors_remain_byte_exact_and_blocked() {
        use sinomo_runtime::dfs_cloud_files_api::{
            CloudFilesModuleRequest, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        };
        use sinomo_runtime::module::ModuleId;

        fn assert_rejected<F>(label: &str, mutate: F)
        where
            F: FnOnce(&mut PendingCloudUnregisterRecord, &SinomoDataDir),
        {
            let temp = TestDir::new("sifs-cloud-malformed-legacy-stage");
            let data_dir = SinomoDataDir::new(temp.path().join(label));
            let mut legacy = pending_record_fixture_with_interface(
                &data_dir,
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
                label.as_bytes(),
            );
            legacy.schema_version = 1;
            mutate(&mut legacy, &data_dir);
            let path = staged_cloud_registration_path(&data_dir);
            std::fs::create_dir_all(path.parent().expect("staged parent"))
                .expect("staged directory");
            let bytes =
                sinomo_runtime::module_api::encode_sobj(&legacy).expect("malformed legacy bytes");
            std::fs::write(&path, &bytes).expect("malformed legacy stage");

            load_staged_cloud_registration_record(&data_dir)
                .expect_err("malformed legacy stage must fail closed");
            assert_eq!(
                std::fs::read(&path).expect("blocked legacy evidence"),
                bytes,
                "{label} must not rewrite blocked evidence"
            );
        }

        assert_rejected("operation", |record, _| {
            record.operation =
                SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION.to_string();
        });
        assert_rejected("module", |record, _| {
            record.module.module_id =
                ModuleId::from_legacy_named("module.dfs.other").expect("other module id");
        });
        assert_rejected("interface", |record, _| {
            record.module.interface_version = SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string();
        });
        assert_rejected("data-dir", |record, _| {
            record.request.data_dir_root = Some(PathBuf::from("C:\\OtherData"));
        });
        assert_rejected("payload", |record, _| {
            let mut request = record.request.clone();
            request.root_path = "/different".to_string();
            record.payload_sobj = sinomo_runtime::module_api::encode_sobj(
                &CloudFilesModuleRequest::Unregister(request),
            )
            .expect("mismatched legacy payload");
        });
        assert_rejected("provider", |record, data_dir| {
            let library = cloud_unregister_recovery_variant_dir(data_dir, &record.module)
                .expect("recovery variant")
                .join(&record.module.library_name);
            std::fs::remove_file(library).expect("remove exact legacy provider");
        });
    }

    #[test]
    fn legacy_stage_cas_pins_v3_and_preserves_exact_v2_origin_across_restart() {
        let temp = TestDir::new("sifs-cloud-legacy-stage-cas");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mut legacy = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
            b"legacy-stage-cas-v2-provider",
        );
        legacy.schema_version = 1;
        let legacy_origin = legacy.module.clone();
        let v3 = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            b"legacy-stage-cas-v3-provider",
        );
        let path = staged_cloud_registration_path(&data_dir);
        std::fs::create_dir_all(path.parent().expect("staged parent")).expect("staged dir");
        let legacy_bytes =
            sinomo_runtime::module_api::encode_sobj(&legacy).expect("legacy stage bytes");
        std::fs::write(&path, &legacy_bytes).expect("legacy stage");
        let migrated = load_staged_cloud_registration_record(&data_dir)
            .expect("load legacy stage")
            .expect("legacy stage present");

        let effects = std::cell::Cell::new(0usize);
        let mut rejected_descriptor = module_descriptor_fixture(&v3.module);
        rejected_descriptor.capabilities.clear();
        admit_cloud_files_provider_before_effect_with(
            &v3.module,
            &rejected_descriptor,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            Some(
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
            ),
            || {
                effects.set(effects.get() + 1);
                let upgraded =
                    legacy_staged_provider_upgrade_record(&migrated, v3.module.clone())?;
                replace_staged_cloud_registration_record(&data_dir, &migrated, &upgraded)
            },
        )
        .expect_err("descriptor failure must precede adoption CAS or invocation");
        assert_eq!(effects.get(), 0);
        assert_eq!(
            std::fs::read(&path).expect("stage after descriptor rejection"),
            legacy_bytes,
            "descriptor rejection leaves the legacy stage byte exact"
        );
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("no pending cleanup")
                .is_none()
        );

        assert!(
            legacy_staged_provider_upgrade_record(&migrated, legacy_origin.clone()).is_err(),
            "a non-v3 replacement cannot discharge or rewrite legacy risk"
        );
        assert_eq!(
            std::fs::read(&path).expect("stage after failed upgrade"),
            legacy_bytes,
            "failed validation leaves deployed stage bytes untouched"
        );

        let upgraded = legacy_staged_provider_upgrade_record(&migrated, v3.module.clone())
            .expect("build exact v3 adoption transition");
        replace_staged_cloud_registration_record(&data_dir, &migrated, &upgraded)
            .expect("atomic v2-origin to v3-provider transition");
        let restarted = load_staged_cloud_registration_record(&data_dir)
            .expect("restart load")
            .expect("upgraded stage present");
        assert_eq!(restarted, upgraded);
        assert_eq!(restarted.recovery.module, v3.module);
        assert_eq!(
            restarted.recovery.legacy_prejournal_origin,
            Some(legacy_origin)
        );
    }

    #[test]
    fn legacy_schema_one_pending_unregister_is_blocked_without_replay_or_clear() {
        let temp = TestDir::new("sifs-cloud-legacy-pending-blocked");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mut legacy = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
            b"deterministic legacy pending Cloud Files provider",
        );
        legacy.schema_version = 1;
        let path = pending_cloud_unregister_path(&data_dir);
        std::fs::create_dir_all(path.parent().expect("pending parent")).expect("pending dir");
        std::fs::write(
            &path,
            sinomo_runtime::module_api::encode_sobj(&legacy).expect("legacy pending bytes"),
        )
        .expect("legacy pending record");

        let error = load_pending_cloud_unregister_record(&data_dir)
            .expect_err("ambiguous legacy pending record must block");
        assert!(error.contains("policy-ambiguous"));
        assert!(path.exists(), "blocked evidence must remain durable");
    }

    #[test]
    fn descriptor_and_preserve_capability_are_bound_before_native_invocation() {
        use sinomo_runtime::dfs_cloud_files_api::{
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
        };

        let provenance = module_provenance_fixture(
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            "descriptor-hash",
        );
        let descriptor = module_descriptor_fixture(&provenance);
        let effects = std::cell::Cell::new(0usize);
        admit_cloud_files_provider_before_effect_with(
            &provenance,
            &descriptor,
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
            Some(
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
            ),
            || {
                effects.set(effects.get() + 1);
                Ok(())
            },
        )
        .expect("matching mount descriptor admits its first effect");
        assert_eq!(effects.get(), 1);
        assert!(provider_supports_state_preserving_unregister(
            &descriptor.interface_version,
            &descriptor.supported_operations,
            &descriptor.capabilities,
        ));

        let mut missing_capability = descriptor.clone();
        missing_capability.capabilities.clear();
        assert!(!provider_supports_state_preserving_unregister(
            &missing_capability.interface_version,
            &missing_capability.supported_operations,
            &missing_capability.capabilities,
        ));
        assert!(
            admit_cloud_files_provider_before_effect_with(
                &provenance,
                &missing_capability,
                SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
                Some(
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
                ),
                || {
                    effects.set(effects.get() + 1);
                    Ok(())
                },
            )
            .is_err()
        );

        let mut missing_operation = descriptor.clone();
        missing_operation.supported_operations.retain(|operation| {
            operation != SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        });
        assert!(!provider_supports_state_preserving_unregister(
            &missing_operation.interface_version,
            &missing_operation.supported_operations,
            &missing_operation.capabilities,
        ));
        assert!(
            admit_cloud_files_provider_before_effect_with(
                &provenance,
                &missing_operation,
                SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
                Some(
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
                ),
                || {
                    effects.set(effects.get() + 1);
                    Ok(())
                },
            )
            .is_err()
        );

        for mismatched in [
            {
                let mut value = descriptor.clone();
                value.module_id = "module.dfs.other".to_string();
                value
            },
            {
                let mut value = descriptor.clone();
                value.interface_version =
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
                        .to_string();
                value
            },
            {
                let mut value = descriptor.clone();
                value.native_abi_version = value.native_abi_version.saturating_add(1);
                value
            },
            {
                let mut value = descriptor.clone();
                value.platform_triple = "different-platform".to_string();
                value
            },
        ] {
            assert!(
                admit_cloud_files_provider_before_effect_with(
                    &provenance,
                    &mismatched,
                    SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
                    Some(
                        sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY,
                    ),
                    || {
                        effects.set(effects.get() + 1);
                        Ok(())
                    },
                )
                .is_err()
            );
        }
        assert_eq!(
            effects.get(),
            1,
            "descriptor or capability failure must precede staging or invocation"
        );
    }

    #[test]
    fn retained_absent_stage_is_stable_while_disabled_and_armed_before_enable() {
        let temp = TestDir::new("sifs-cloud-retained-absent-state");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let retained = staged_cloud_registration_record(
            &recovery,
            StagedCloudRegistrationState::RetainedAbsent,
        )
        .expect("retained stage");
        persist_staged_cloud_registration_record(&data_dir, &retained).expect("persist retained");
        let mut config = windows_mount_config(&mount_point, "/root");
        config.enabled = false;

        assert!(
            recover_pending_cloud_unregister_from_staged_with(
                &data_dir,
                &config,
                Some(&recovery.module),
            )
            .expect("disabled retained recovery")
            .is_none()
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("disabled stage"),
            Some(retained.clone())
        );
        let receipt = sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt {
            interface_version:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    .to_string(),
            registration_identity_interface_version: None,
            sync_root: mount_point.clone(),
            root_path: "/root".to_string(),
            provider_name:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                    .to_string(),
        };
        let receipt_path = cloud_files_registration_receipt_path(&data_dir);
        std::fs::create_dir_all(receipt_path.parent().expect("receipt parent"))
            .expect("receipt directory");
        std::fs::write(
            &receipt_path,
            sinomo_runtime::module_api::encode_sobj(&receipt).expect("receipt bytes"),
        )
        .expect("matching retained receipt");
        for _ in 0..2 {
            assert!(
                recover_pending_cloud_unregister_from_receipt(&data_dir, &config)
                    .expect("settled stage suppresses receipt fallback")
                    .is_none()
            );
            assert!(
                load_pending_cloud_unregister_record(&data_dir)
                    .expect("no repeated cleanup intent")
                    .is_none()
            );
            assert_eq!(
                load_staged_cloud_registration_record(&data_dir).expect("stable retained stage"),
                Some(retained.clone())
            );
        }

        config.enabled = true;
        assert!(
            recover_pending_cloud_unregister_from_staged_with(
                &data_dir,
                &config,
                Some(&recovery.module),
            )
            .expect("enable retained recovery")
            .is_none()
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir)
                .expect("stage armed before mount")
                .expect("stage retained")
                .state,
            StagedCloudRegistrationState::MayBeRegistered
        );
    }

    #[test]
    fn cursor_recovery_staged_enable_transition_replays_only_exact_deterministic_cuts() {
        let temp = TestDir::new("sifs-cursor-recovery-staged-enable-cuts");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let retained = staged_cloud_registration_record(
            &recovery,
            StagedCloudRegistrationState::RetainedAbsent,
        )
        .expect("retained stage");
        let armed = staged_cloud_registration_record(
            &recovery,
            StagedCloudRegistrationState::MayBeRegistered,
        )
        .expect("armed stage");
        let retained_bytes =
            sinomo_runtime::module_api::encode_sobj(&retained).expect("retained bytes");
        let armed_bytes = sinomo_runtime::module_api::encode_sobj(&armed).expect("armed bytes");
        let final_path = staged_cloud_registration_path(&data_dir);
        let temporary_path = cursor_recovery_staged_temp_path(&data_dir);

        persist_or_promote_cursor_recovery_staged_record(&data_dir, &retained)
            .expect("initial retained stage");
        transition_or_promote_cursor_recovery_staged_record(&data_dir, &retained, &armed)
            .expect("retained to armed transition");
        assert_eq!(
            std::fs::read(&final_path).expect("armed final"),
            armed_bytes
        );
        assert!(!temporary_path.exists());

        std::fs::write(&final_path, &retained_bytes).expect("restore retained predecessor");
        std::fs::write(&temporary_path, &armed_bytes).expect("persist successor cut");
        transition_or_promote_cursor_recovery_staged_record(&data_dir, &retained, &armed)
            .expect("promote exact successor cut");
        assert_eq!(
            std::fs::read(&final_path).expect("promoted final"),
            armed_bytes
        );
        assert!(!temporary_path.exists());

        std::fs::write(&temporary_path, &armed_bytes).expect("redundant successor cut");
        transition_or_promote_cursor_recovery_staged_record(&data_dir, &retained, &armed)
            .expect("clean redundant successor cut");
        assert_eq!(
            std::fs::read(&final_path).expect("stable final"),
            armed_bytes
        );
        assert!(!temporary_path.exists());

        std::fs::write(&final_path, &retained_bytes).expect("restore retained predecessor");
        std::fs::write(&temporary_path, b"torn successor").expect("torn successor cut");
        let error =
            transition_or_promote_cursor_recovery_staged_record(&data_dir, &retained, &armed)
                .expect_err("torn successor must fail closed");
        assert!(error.contains("exact retained predecessor"));
        assert_eq!(
            std::fs::read(&final_path).expect("retained final unchanged"),
            retained_bytes
        );
        assert_eq!(
            std::fs::read(&temporary_path).expect("torn evidence retained"),
            b"torn successor"
        );
    }

    #[cfg(windows)]
    #[derive(Clone, Copy, Debug)]
    enum CursorPendingReplayCut {
        PendingAndRetainedTemps,
        RetainedWithHostPredecessor,
        PreservedHostTemp,
        PreservedHostFinal,
        ReversedPreservedHostAndRetainedTemp,
        PredecessorHostTemp,
        PreservedHostProofMismatch,
        PreservedHostArtifactDrift,
    }

    #[cfg(windows)]
    fn run_cursor_pending_replay_cut(label: &str, cut: CursorPendingReplayCut) {
        let temp = TestDir::new(label);
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");

        let pinned = pending_record_fixture(&data_dir);
        let provider_path = verify_cloud_unregister_recovery_module(&data_dir, &pinned.module)
            .expect("pinned provider");
        let provider_bytes = std::fs::read(provider_path).expect("pinned provider bytes");
        let initial_lease =
            SifsMountLease::try_acquire(&data_dir, "cursor replay fixture").expect("initial lease");
        drop(initial_lease);
        let (observed, host_lock_bytes) =
            SifsMountLease::try_acquire_existing_for_cursor_recovery(&data_dir)
                .expect("read cursor replay host lock through its owner handle");
        drop(observed);

        let mut marker = host_cursor_recovery_marker_fixture(
            &data_dir,
            &mount_point,
            HostCursorRecoveryPhase::ActiveObserved,
        );
        marker.module = pinned.module;
        marker.identity.provider_provenance_fingerprint =
            cloud_files_provenance_fingerprint(&marker.module).expect("provider fingerprint");
        marker.outer_token = host_cursor_recovery_outer_token(
            &marker.disabled_config,
            &marker.module,
            &host_lock_bytes,
            &marker.identity,
            &marker.inspection,
        )
        .expect("outer token");
        marker.recovery_id = deterministic_host_cursor_recovery_id(&marker.outer_token);
        marker
            .applied
            .as_mut()
            .expect("applied fixture")
            .recovery_id = marker.recovery_id.clone();

        let pending = pending_cloud_unregister_record_with_policy(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                data_dir_root: Some(marker.identity.data_dir_root.clone()),
                provider_name: marker.identity.provider_name.clone(),
                cursor_recovery: Some(host_cursor_recovery_context(&marker)),
            },
            marker.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )
        .expect("pending preserve");
        let may_be_registered = staged_cloud_registration_record(
            &pending,
            StagedCloudRegistrationState::MayBeRegistered,
        )
        .expect("MayBeRegistered stage");
        let successor_context =
            sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                recovery_id: marker.recovery_id.clone(),
                recovery_token: marker.inner_token.clone(),
                recovery_cycle: 1,
                native_successor_fingerprint: Some(format!("blake3:{}", "3".repeat(64))),
            };
        let retained_response =
            sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                was_registered: true,
                outcome: sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterOutcome::Retained,
                cursor_recovery: Some(successor_context.clone()),
                detail: "retained replay fixture".to_string(),
            };
        let mut retained = staged_cursor_recovery_successor_record(&pending, &retained_response)
            .expect("RetainedAbsent successor");
        if matches!(cut, CursorPendingReplayCut::PreservedHostProofMismatch) {
            retained
                .recovery
                .request
                .cursor_recovery
                .as_mut()
                .expect("retained context")
                .native_successor_fingerprint = Some(format!("blake3:{}", "4".repeat(64)));
            retained.recovery.payload_sobj = sinomo_runtime::module_api::encode_sobj(
                &sinomo_runtime::dfs_cloud_files_api::CloudFilesModuleRequest::Unregister(
                    retained.recovery.request.clone(),
                ),
            )
            .expect("policy-neutral staged payload");
        }

        let receipt = sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt {
            interface_version:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    .to_string(),
            registration_identity_interface_version: None,
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            provider_name: marker.identity.provider_name.clone(),
        };
        let receipt_path = cloud_files_registration_receipt_path(&data_dir);
        std::fs::create_dir_all(receipt_path.parent().expect("receipt parent"))
            .expect("module runtime");
        std::fs::write(
            &receipt_path,
            sinomo_runtime::module_api::encode_sobj(&receipt).expect("receipt bytes"),
        )
        .expect("receipt");
        std::fs::write(
            cloud_files_module_runtime_path(&data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            b"exact-next-cycle-StateOwned-journal",
        )
        .expect("StateOwned journal");

        let mut preserved_marker = marker.clone();
        preserved_marker.phase = HostCursorRecoveryPhase::PreservedRetained;
        preserved_marker.ready_artifacts_fingerprint = None;
        preserved_marker.preserved_artifacts_fingerprint = Some(
            cloud_files_preserved_artifacts_fingerprint(&data_dir)
                .expect("preserved artifacts fingerprint"),
        );
        preserved_marker.active_module_marker_fingerprint = None;
        preserved_marker.completion_module_marker_fingerprint = None;
        preserved_marker.completion_registration_artifacts_fingerprint = None;
        preserved_marker.completed_artifacts_fingerprint = None;
        preserved_marker.recovery_cycle = 1;
        preserved_marker.native_successor_fingerprint =
            successor_context.native_successor_fingerprint.clone();

        let bundle = HostCursorRecoveryProviderBundle {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            requested: requested_host_cursor_recovery_marker(&marker),
            provider_bytes,
            execution_module: None,
            execution_provider_bytes: None,
        };
        persist_host_cursor_recovery_provider_bundle(&data_dir, &bundle).expect("provider bundle");

        let write_marker_final = |value: &HostCursorRecoveryMarker| {
            stage_host_cursor_recovery_marker(&data_dir, value).expect("stage marker fixture");
            commit_staged_host_cursor_recovery_marker(&data_dir).expect("commit marker fixture");
        };
        match cut {
            CursorPendingReplayCut::PreservedHostTemp => {
                write_marker_final(&marker);
                stage_host_cursor_recovery_marker(&data_dir, &preserved_marker)
                    .expect("stage preserved host successor");
            }
            CursorPendingReplayCut::PreservedHostFinal
            | CursorPendingReplayCut::ReversedPreservedHostAndRetainedTemp
            | CursorPendingReplayCut::PreservedHostProofMismatch
            | CursorPendingReplayCut::PreservedHostArtifactDrift => {
                write_marker_final(&preserved_marker);
            }
            CursorPendingReplayCut::PredecessorHostTemp => {
                let mut ready = marker.clone();
                ready.phase = HostCursorRecoveryPhase::ReadyObserved;
                ready.active_module_marker_fingerprint = None;
                write_marker_final(&ready);
                stage_host_cursor_recovery_marker(&data_dir, &marker)
                    .expect("stage impossible Active predecessor temp");
            }
            _ => write_marker_final(&marker),
        }

        persist_or_promote_cursor_recovery_staged_record(&data_dir, &may_be_registered)
            .expect("MayBeRegistered stage");
        match cut {
            CursorPendingReplayCut::PendingAndRetainedTemps
            | CursorPendingReplayCut::ReversedPreservedHostAndRetainedTemp => {
                std::fs::write(
                    cursor_recovery_staged_temp_path(&data_dir),
                    sinomo_runtime::module_api::encode_sobj(&retained)
                        .expect("RetainedAbsent temp bytes"),
                )
                .expect("RetainedAbsent temp");
            }
            _ => {
                transition_or_promote_cursor_recovery_staged_record(
                    &data_dir,
                    &may_be_registered,
                    &retained,
                )
                .expect("RetainedAbsent final");
            }
        }

        let pending_bytes =
            sinomo_runtime::module_api::encode_sobj(&pending).expect("pending bytes");
        let pending_temp_only = matches!(cut, CursorPendingReplayCut::PendingAndRetainedTemps);
        std::fs::write(
            if pending_temp_only {
                cursor_recovery_pending_temp_path(&data_dir)
            } else {
                pending_cloud_unregister_path(&data_dir)
            },
            &pending_bytes,
        )
        .expect("pending intent cut");
        if matches!(cut, CursorPendingReplayCut::PreservedHostArtifactDrift) {
            std::fs::write(
                cloud_files_module_runtime_path(&data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
                b"changed-after-preserved-host-commit",
            )
            .expect("drift preserved journal");
        }

        let before_pending =
            inspect_pending_cloud_unregister_record(&data_dir).expect("pending before replay");
        let before_stage =
            inspect_cursor_recovery_staged_record_snapshot(&data_dir).expect("stage before replay");
        let before_marker = inspect_host_cursor_recovery_marker_for_resume(&data_dir)
            .expect("marker before replay");
        let (lease, observed_lock_bytes) =
            SifsMountLease::try_acquire_existing_for_cursor_recovery(&data_dir)
                .expect("existing cursor-recovery lease");
        assert_eq!(observed_lock_bytes, host_lock_bytes);
        let mut runnable = PendingCloudUnregister::new(pending.clone(), Some(Arc::new(lease)));

        let impossible = matches!(
            cut,
            CursorPendingReplayCut::ReversedPreservedHostAndRetainedTemp
                | CursorPendingReplayCut::PredecessorHostTemp
                | CursorPendingReplayCut::PreservedHostProofMismatch
                | CursorPendingReplayCut::PreservedHostArtifactDrift
        );
        if impossible {
            complete_pending_cloud_unregister(&data_dir, &mut runnable)
                .expect_err("impossible cursor replay cut must fail closed");
            let after_pending = inspect_pending_cloud_unregister_record(&data_dir)
                .expect("pending after rejection");
            let after_stage = inspect_cursor_recovery_staged_record_snapshot(&data_dir)
                .expect("stage after rejection");
            let after_marker = inspect_host_cursor_recovery_marker_for_resume(&data_dir)
                .expect("marker after rejection");
            assert_eq!(after_pending.value, before_pending.value);
            assert_eq!(
                after_pending.current_present,
                before_pending.current_present
            );
            assert_eq!(
                after_pending.temporary_present,
                before_pending.temporary_present
            );
            assert_eq!(after_stage.current, before_stage.current);
            assert_eq!(after_stage.temporary, before_stage.temporary);
            assert_eq!(after_marker.value, before_marker.value);
            assert_eq!(after_marker.temporary, before_marker.temporary);
            return;
        }

        complete_pending_cloud_unregister(&data_dir, &mut runnable)
            .expect("complete exact cursor replay cut");
        assert!(
            inspect_pending_cloud_unregister_record(&data_dir)
                .expect("pending after replay")
                .value
                .is_none()
        );
        assert_eq!(
            inspect_cursor_recovery_staged_record_snapshot(&data_dir)
                .expect("stage after replay")
                .current,
            Some(retained)
        );
        let completed_marker = load_host_cursor_recovery_marker_strict(&data_dir)
            .expect("load completed host marker")
            .expect("completed host marker");
        assert_eq!(completed_marker, preserved_marker);
    }

    #[cfg(windows)]
    #[test]
    fn cursor_pending_replay_classifies_u_stage_and_host_successor_cuts() {
        for (label, cut) in [
            (
                "sifs-cursor-replay-u-and-stage-temps",
                CursorPendingReplayCut::PendingAndRetainedTemps,
            ),
            (
                "sifs-cursor-replay-retained-before-host",
                CursorPendingReplayCut::RetainedWithHostPredecessor,
            ),
            (
                "sifs-cursor-replay-host-successor-temp",
                CursorPendingReplayCut::PreservedHostTemp,
            ),
            (
                "sifs-cursor-replay-host-successor-final",
                CursorPendingReplayCut::PreservedHostFinal,
            ),
            (
                "sifs-cursor-replay-reversed-host-stage",
                CursorPendingReplayCut::ReversedPreservedHostAndRetainedTemp,
            ),
            (
                "sifs-cursor-replay-predecessor-host-temp",
                CursorPendingReplayCut::PredecessorHostTemp,
            ),
            (
                "sifs-cursor-replay-host-proof-mismatch",
                CursorPendingReplayCut::PreservedHostProofMismatch,
            ),
            (
                "sifs-cursor-replay-host-artifact-drift",
                CursorPendingReplayCut::PreservedHostArtifactDrift,
            ),
        ] {
            run_cursor_pending_replay_cut(label, cut);
        }
    }

    #[cfg(windows)]
    #[test]
    fn disabled_armed_replay_rolls_stage_forward_and_retains_existing_lease() {
        for with_may_be_registered_temp in [false, true] {
            let temp = TestDir::new(if with_may_be_registered_temp {
                "sifs-cursor-disabled-armed-with-stage-temp"
            } else {
                "sifs-cursor-disabled-armed-before-stage"
            });
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let mount_point = temp.path().join("mount");
            std::fs::create_dir_all(&mount_point).expect("mount point");

            let pinned = pending_record_fixture(&data_dir);
            let provider_path = verify_cloud_unregister_recovery_module(&data_dir, &pinned.module)
                .expect("pinned provider");
            let provider_bytes = std::fs::read(provider_path).expect("pinned provider bytes");
            let seed = SifsMountLease::try_acquire(&data_dir, "rapid disabled replay fixture")
                .expect("seed lease");
            drop(seed);
            let (observed, lock_bytes) =
                SifsMountLease::try_acquire_existing_for_cursor_recovery(&data_dir)
                    .expect("read rapid replay host lock through its owner handle");
            drop(observed);

            let mut marker = host_cursor_recovery_marker_fixture(
                &data_dir,
                &mount_point,
                HostCursorRecoveryPhase::ArmedAwaitingReady,
            );
            marker.module = pinned.module;
            marker.identity.provider_provenance_fingerprint =
                cloud_files_provenance_fingerprint(&marker.module).expect("provider fingerprint");
            marker.outer_token = host_cursor_recovery_outer_token(
                &marker.disabled_config,
                &marker.module,
                &lock_bytes,
                &marker.identity,
                &marker.inspection,
            )
            .expect("outer token");
            marker.recovery_id = deterministic_host_cursor_recovery_id(&marker.outer_token);
            marker
                .applied
                .as_mut()
                .expect("applied fixture")
                .recovery_id = marker.recovery_id.clone();
            let recovery = pending_cloud_unregister_record_with_policy(
                sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                    sync_root: marker.identity.sync_root.clone(),
                    root_path: marker.identity.root_path.clone(),
                    data_dir_root: Some(marker.identity.data_dir_root.clone()),
                    provider_name: marker.identity.provider_name.clone(),
                    cursor_recovery: Some(host_cursor_recovery_context(&marker)),
                },
                marker.module.clone(),
                CloudUnregisterPolicy::PreserveState,
            )
            .expect("stage recovery");
            let retained = staged_cloud_registration_record(
                &recovery,
                StagedCloudRegistrationState::RetainedAbsent,
            )
            .expect("RetainedAbsent stage");
            let may_be_registered = staged_cloud_registration_record(
                &recovery,
                StagedCloudRegistrationState::MayBeRegistered,
            )
            .expect("MayBeRegistered stage");
            persist_or_promote_cursor_recovery_staged_record(&data_dir, &retained)
                .expect("RetainedAbsent final");
            if with_may_be_registered_temp {
                std::fs::write(
                    cursor_recovery_staged_temp_path(&data_dir),
                    sinomo_runtime::module_api::encode_sobj(&may_be_registered)
                        .expect("MayBeRegistered temp bytes"),
                )
                .expect("MayBeRegistered temp");
            }
            let bundle = HostCursorRecoveryProviderBundle {
                schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
                requested: requested_host_cursor_recovery_marker(&marker),
                provider_bytes,
                execution_module: None,
                execution_provider_bytes: None,
            };
            persist_host_cursor_recovery_provider_bundle(&data_dir, &bundle)
                .expect("provider bundle");
            stage_host_cursor_recovery_marker(&data_dir, &marker).expect("stage Armed marker");
            commit_staged_host_cursor_recovery_marker(&data_dir).expect("commit Armed marker");

            configure_managed_sifs_mount(&data_dir, &marker.disabled_config)
                .expect("persist exact disabled config");
            let (owned, lease) =
                admit_host_cursor_recovery_enable(&data_dir, &marker.disabled_config)
                    .expect("disabled Armed admission");
            assert!(owned);
            let lease = lease.expect("returned existing lease");
            assert_eq!(lease.cursor_recovery_bytes(), Some(lock_bytes.as_slice()));
            assert_eq!(
                load_host_cursor_recovery_marker_strict(&data_dir)
                    .expect("load Armed marker")
                    .expect("Armed marker")
                    .phase,
                HostCursorRecoveryPhase::ArmedAwaitingReady
            );
            let staged = inspect_cursor_recovery_staged_record_snapshot(&data_dir)
                .expect("stage after disabled admission");
            assert_eq!(staged.current, Some(may_be_registered));
            assert!(staged.temporary.is_none());

            let pending = recover_pending_cloud_unregister_from_staged_with(
                &data_dir,
                &marker.disabled_config,
                Some(&marker.module),
            )
            .expect("recover disabled Preserve intent")
            .expect("disabled Armed requires Preserve");
            assert_eq!(
                pending.request.cursor_recovery,
                Some(host_cursor_recovery_context(&marker))
            );
            persist_pending_cloud_unregister_record(&data_dir, &pending)
                .expect("persist Preserve intent");
            let runnable = PendingCloudUnregister::new(pending, Some(Arc::new(lease)));
            assert!(SifsMountLease::try_acquire_existing_for_cursor_recovery(&data_dir).is_err());
            assert!(
                runnable
                    .lease
                    .as_ref()
                    .and_then(|lease| lease.cursor_recovery_bytes())
                    .is_some()
            );
        }
    }

    #[test]
    fn host_cursor_recovery_preserve_successor_is_durable_before_pending_clear() {
        let temp = TestDir::new("sifs-cursor-recovery-preserved-successor");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let mut marker = host_cursor_recovery_marker_fixture(
            &data_dir,
            &mount_point,
            HostCursorRecoveryPhase::ActiveObserved,
        );
        let pinned = pending_record_fixture(&data_dir);
        marker.module = pinned.module;
        let provider_bytes = std::fs::read(
            verify_cloud_unregister_recovery_module(&data_dir, &marker.module)
                .expect("pinned provider path"),
        )
        .expect("pinned provider bytes");
        marker.identity.provider_provenance_fingerprint =
            cloud_files_provenance_fingerprint(&marker.module).expect("provider fingerprint");
        let pending = pending_cloud_unregister_record_with_policy(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: mount_point.clone(),
                root_path: "/root".to_string(),
                data_dir_root: Some(data_dir.root()),
                provider_name:
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                        .to_string(),
                cursor_recovery: Some(
                    sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                        recovery_id: marker.recovery_id.clone(),
                        recovery_token: marker.inner_token.clone(),
                        recovery_cycle: marker.recovery_cycle,
                        native_successor_fingerprint: marker.native_successor_fingerprint.clone(),
                    },
                ),
            },
            marker.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )
        .expect("pending preserve");
        let successor_context =
            sinomo_runtime::dfs_cloud_files_api::CloudFilesCursorRecoveryContext {
                recovery_id: marker.recovery_id.clone(),
                recovery_token: marker.inner_token.clone(),
                recovery_cycle: marker.recovery_cycle + 1,
                native_successor_fingerprint: Some(format!("blake3:{}", "3".repeat(64))),
            };
        let retained_response =
            sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                was_registered: true,
                outcome: sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterOutcome::Retained,
                cursor_recovery: Some(successor_context.clone()),
                detail: "retained fixture".to_string(),
            };
        let retained = staged_cursor_recovery_successor_record(&pending, &retained_response)
            .expect("next-cycle retained stage");
        persist_or_promote_cursor_recovery_staged_record(&data_dir, &retained)
            .expect("retained ownership");
        std::fs::create_dir_all(data_dir.dfs_dir()).expect("dfs directory");
        std::fs::write(
            pending_cloud_unregister_path(&data_dir),
            sinomo_runtime::module_api::encode_sobj(&pending).expect("pending bytes"),
        )
        .expect("pending intent");
        let receipt = sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt {
            interface_version:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    .to_string(),
            registration_identity_interface_version: None,
            sync_root: mount_point,
            root_path: "/root".to_string(),
            provider_name:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                    .to_string(),
        };
        let receipt_path = cloud_files_registration_receipt_path(&data_dir);
        std::fs::create_dir_all(receipt_path.parent().expect("receipt parent"))
            .expect("module runtime");
        std::fs::write(
            &receipt_path,
            sinomo_runtime::module_api::encode_sobj(&receipt).expect("receipt bytes"),
        )
        .expect("receipt");
        std::fs::write(
            cloud_files_module_runtime_path(&data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            b"exact-module-validated-state-owned-journal",
        )
        .expect("StateOwned journal bytes");
        let bundle = HostCursorRecoveryProviderBundle {
            schema_version: CLOUD_FILES_CURSOR_RECOVERY_HOST_SCHEMA_VERSION,
            requested: requested_host_cursor_recovery_marker(&marker),
            provider_bytes,
            execution_module: None,
            execution_provider_bytes: None,
        };
        persist_host_cursor_recovery_provider_bundle(&data_dir, &bundle).expect("provider bundle");
        stage_host_cursor_recovery_marker(&data_dir, &marker)
            .expect("stage ActiveObserved fixture marker");
        commit_staged_host_cursor_recovery_marker(&data_dir)
            .expect("commit ActiveObserved fixture marker");

        advance_host_cursor_recovery_preserved_successor(&data_dir, &pending, &retained)
            .expect("durable preserved successor");
        marker = load_host_cursor_recovery_marker_strict(&data_dir)
            .expect("load preserved marker")
            .expect("preserved marker");
        assert_eq!(marker.phase, HostCursorRecoveryPhase::PreservedRetained);
        assert_eq!(marker.recovery_cycle, successor_context.recovery_cycle);
        assert_eq!(
            marker.native_successor_fingerprint,
            successor_context.native_successor_fingerprint
        );
        assert_eq!(marker.ready_artifacts_fingerprint, None);
        assert_eq!(
            marker.preserved_artifacts_fingerprint,
            Some(
                cloud_files_preserved_artifacts_fingerprint(&data_dir)
                    .expect("preserved fingerprint")
            )
        );
        assert!(
            pending_cloud_unregister_path(&data_dir).is_file(),
            "host successor must precede pending deletion"
        );
        std::fs::remove_file(pending_cloud_unregister_path(&data_dir)).expect("clear pending");

        std::fs::write(
            cloud_files_module_runtime_path(&data_dir, CLOUD_FILES_REGISTRATION_JOURNAL_FILE),
            b"changed-state-owned-journal",
        )
        .expect("change journal");
        let error =
            advance_host_cursor_recovery_preserved_successor(&data_dir, &pending, &retained)
                .expect_err("preserved artifact drift must fail closed");
        assert!(error.contains("changed before pending cleanup"));
    }

    #[test]
    fn provider_replacement_releases_pinned_stage_before_new_loader() {
        for state in [
            StagedCloudRegistrationState::MayBeRegistered,
            StagedCloudRegistrationState::RetainedAbsent,
        ] {
            let temp = TestDir::new(&format!("sifs-cloud-provider-replacement-{state:?}"));
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let mount_point = temp.path().join("mount");
            std::fs::create_dir_all(&mount_point).expect("mount point");
            let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
            let staged =
                staged_cloud_registration_record(&recovery, state).expect("old provider stage");
            persist_staged_cloud_registration_record(&data_dir, &staged)
                .expect("persist old stage");
            let mut selected = recovery.module.clone();
            selected.content_hash = "different-selected-v3-provider".to_string();
            let config = windows_mount_config(&mount_point, "/root");

            let pending = recover_pending_cloud_unregister_from_staged_with(
                &data_dir,
                &config,
                Some(&selected),
            )
            .expect("validated replacement requests old-provider release")
            .expect("old provider release intent");

            assert_eq!(pending, recovery);
            assert_eq!(
                pending.operation,
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
            );
            assert_eq!(
                load_staged_cloud_registration_record(&data_dir).expect("stage remains exact"),
                Some(staged),
                "the durable pending intent must precede any staged-state change"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_v3_stage_settles_exact_enable_temp_before_loader_selection() {
        for enabled in [true, false] {
            let temp = TestDir::new(if enabled {
                "sifs-cloud-ordinary-option-b-enabled-temp"
            } else {
                "sifs-cloud-ordinary-option-b-disabled-temp"
            });
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let mount_point = temp.path().join("mount");
            std::fs::create_dir_all(&mount_point).expect("mount point");
            let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
            let retained = staged_cloud_registration_record(
                &recovery,
                StagedCloudRegistrationState::RetainedAbsent,
            )
            .expect("ordinary retained stage");
            let may_be_registered = may_be_registered_stage(&recovery);
            persist_or_promote_cursor_recovery_staged_record(&data_dir, &retained)
                .expect("ordinary retained final");
            std::fs::write(
                cursor_recovery_staged_temp_path(&data_dir),
                sinomo_runtime::module_api::encode_sobj(&may_be_registered)
                    .expect("ordinary MayBeRegistered temp bytes"),
            )
            .expect("ordinary MayBeRegistered temp");
            let mut config = windows_mount_config(&mount_point, "/root");
            config.enabled = enabled;
            let pending = recover_pending_cloud_unregister_from_staged(&data_dir, &config)
                .expect("settle exact ordinary stage successor");
            assert_eq!(
                load_staged_cloud_registration_record(&data_dir).expect("ordinary settled stage"),
                Some(may_be_registered)
            );
            assert!(!cursor_recovery_staged_temp_path(&data_dir).exists());
            if enabled {
                assert!(pending.is_none());
            } else {
                let pending = pending.expect("disabled ordinary stage requires Preserve(None)");
                assert!(pending.request.cursor_recovery.is_none());
                assert_eq!(pending.module, recovery.module);
                assert_eq!(
                    pending.operation,
                    sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
                );
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_v3_stage_rejects_pin_tamper_changed_temp_and_config_without_fallback() {
        for cut in ["pin", "missing-pin", "temp", "config"] {
            let temp = TestDir::new(&format!("sifs-cloud-ordinary-option-b-reject-{cut}"));
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let mount_point = temp.path().join("mount");
            std::fs::create_dir_all(&mount_point).expect("mount point");
            let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
            let staged = may_be_registered_stage(&recovery);
            persist_or_promote_cursor_recovery_staged_record(&data_dir, &staged)
                .expect("ordinary authoritative stage");
            let mut config = windows_mount_config(&mount_point, "/root");
            match cut {
                "pin" => {
                    let pin = verify_cloud_unregister_recovery_module(&data_dir, &recovery.module)
                        .expect("ordinary pin before tamper");
                    std::fs::write(pin, b"tampered provider bytes")
                        .expect("tamper ordinary provider pin");
                }
                "missing-pin" => {
                    let pin = verify_cloud_unregister_recovery_module(&data_dir, &recovery.module)
                        .expect("ordinary pin before removal");
                    std::fs::remove_file(pin).expect("remove ordinary provider pin");
                }
                "temp" => {
                    let mut changed = staged.clone();
                    changed.recovery.request.root_path = "/changed".to_string();
                    std::fs::write(
                        cursor_recovery_staged_temp_path(&data_dir),
                        sinomo_runtime::module_api::encode_sobj(&changed)
                            .expect("changed ordinary stage temp bytes"),
                    )
                    .expect("changed ordinary stage temp");
                }
                "config" => config.root_path = "/changed".to_string(),
                _ => unreachable!(),
            }
            let staged_path = staged_cloud_registration_path(&data_dir);
            let staged_temp_path = cursor_recovery_staged_temp_path(&data_dir);
            let before_current = read_bounded_plain_host_file(
                &staged_path,
                1024 * 1024,
                "ordinary staged ownership before rejection",
            )
            .expect("ordinary staged ownership bytes before rejection");
            let before_temporary = read_bounded_plain_host_file(
                &staged_temp_path,
                1024 * 1024,
                "ordinary staged ownership temporary file before rejection",
            )
            .expect("ordinary staged temporary bytes before rejection");
            recover_pending_cloud_unregister_from_staged(&data_dir, &config)
                .expect_err("ordinary authoritative stage must fail closed");
            assert_eq!(
                read_bounded_plain_host_file(
                    &staged_path,
                    1024 * 1024,
                    "ordinary staged ownership after rejection",
                )
                .expect("ordinary staged ownership bytes after rejection"),
                before_current
            );
            assert_eq!(
                read_bounded_plain_host_file(
                    &staged_temp_path,
                    1024 * 1024,
                    "ordinary staged ownership temporary file after rejection",
                )
                .expect("ordinary staged temporary bytes after rejection"),
                before_temporary
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_v3_stage_rejects_invalid_replacement_without_loader_fallback() {
        use sinomo_runtime::module::{
            NativeDylibModuleLoaderState, native_module_cache_path,
            write_native_dylib_module_loader_state,
        };

        let temp = TestDir::new("sifs-cloud-ordinary-option-b-loader-drift");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let stage = may_be_registered_stage(&recovery);
        persist_or_promote_cursor_recovery_staged_record(&data_dir, &stage)
            .expect("ordinary stage authority");

        let loader_bytes = b"different active loader provider";
        let loader_hash = sinomo_runtime::module::native_module_content_hash(loader_bytes);
        let loader = NativeDylibModuleLoaderState {
            module_id: recovery.module.module_id.clone(),
            artifact_id: None,
            content_hash: loader_hash.clone(),
            cache_key: "drifted/cloud-files".to_string(),
            library_name: "drifted-cloud-files.dll".to_string(),
            platform_triple: recovery.module.platform_triple.clone(),
            native_abi_version: recovery.module.native_abi_version,
            interface_version: recovery.module.interface_version.clone(),
            activated_at: "2026-08-12T00:00:00Z".to_string(),
            source: "test loader drift".to_string(),
        };
        write_native_dylib_module_loader_state(&data_dir.root(), &loader)
            .expect("persist drifted loader state");
        let loader_path = native_module_cache_path(
            &data_dir.root(),
            &loader.cache_key,
            &loader.content_hash,
            &loader.library_name,
        );
        std::fs::create_dir_all(loader_path.parent().expect("loader cache parent"))
            .expect("loader cache directory");
        std::fs::write(&loader_path, loader_bytes).expect("drifted loader bytes");

        let config = windows_mount_config(&mount_point, "/root");
        let error = recover_pending_cloud_unregister_from_staged(&data_dir, &config)
            .expect_err("invalid active replacement must fail before old-provider release");
        assert!(
            error.contains("load active Cloud Files replacement provider"),
            "replacement validation error: {error}"
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir)
                .expect("old staged provider remains authoritative"),
            Some(stage)
        );
        assert!(!pending_cloud_unregister_path(&data_dir).exists());
    }

    #[test]
    fn retained_stage_identity_or_provider_change_blocks_without_altering_ownership() {
        let temp = TestDir::new("sifs-cloud-retained-replacement");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let retained = staged_cloud_registration_record(
            &recovery,
            StagedCloudRegistrationState::RetainedAbsent,
        )
        .expect("retained stage");
        persist_staged_cloud_registration_record(&data_dir, &retained).expect("persist retained");

        let changed = windows_mount_config(&mount_point, "/replacement");
        let error = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &changed,
            Some(&recovery.module),
        )
        .expect_err("changed identity must remain blocked");
        assert!(error.contains("registration replacement is blocked"));

        let mut other_module = recovery.module.clone();
        other_module.content_hash = "different-provider".to_string();
        let error = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &windows_mount_config(&mount_point, "/root"),
            Some(&other_module),
        )
        .expect_err("changed provider must remain blocked");
        assert!(error.contains("registration replacement is blocked"));
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("retained stage"),
            Some(retained)
        );
    }

    #[test]
    fn retained_unregister_response_requires_the_distinct_preserve_operation() {
        use sinomo_runtime::dfs_cloud_files_api::{
            CloudFilesPreserveStateUnregisterOutcome, CloudFilesPreserveStateUnregisterResponse,
        };

        let temp = TestDir::new("sifs-cloud-typed-retained-response");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let release = pending_record_fixture(&data_dir);
        let preserve = pending_cloud_unregister_record_with_policy(
            release.request.clone(),
            release.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )
        .expect("preserve descriptor");
        let response =
            CloudUnregisterCompletion::StatePreserved(CloudFilesPreserveStateUnregisterResponse {
                sync_root: preserve.request.sync_root.clone(),
                root_path: preserve.request.root_path.clone(),
                was_registered: true,
                outcome: CloudFilesPreserveStateUnregisterOutcome::Retained,
                detail: "retained".to_string(),
                cursor_recovery: None,
            });
        let release_stage = may_be_registered_stage(&release);
        let preserve_stage = may_be_registered_stage(&preserve);

        assert!(
            staged_state_after_unregister_response(&release, &release_stage, &response).is_err()
        );
        assert_eq!(
            staged_state_after_unregister_response(&preserve, &preserve_stage, &response)
                .expect("conditional retain"),
            Some(StagedCloudRegistrationState::RetainedAbsent)
        );
    }

    #[test]
    fn disabled_config_retains_receipt_only_for_the_same_mount_identity() {
        let temp = TestDir::new("sifs-cloud-disabled-receipt-identity");
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let mut config = windows_mount_config(&mount_point, "/root");
        config.enabled = false;
        let receipt = sinomo_runtime::dfs_cloud_files_api::CloudFilesRegistrationReceipt {
            interface_version:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
                    .to_string(),
            registration_identity_interface_version: None,
            sync_root: mount_point,
            root_path: "/root".to_string(),
            provider_name:
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
                    .to_string(),
        };

        assert!(
            registration_receipt_matches_config(&receipt, &config).expect("same disabled identity")
        );
        config.root_path = "/replacement".to_string();
        assert!(
            !registration_receipt_matches_config(&receipt, &config)
                .expect("changed disabled identity")
        );
    }

    #[test]
    fn cloud_unregister_failure_stays_pending_and_retries_with_old_identity() {
        let temp = TestDir::new("sifs-cloud-unregister-state");
        let data_dir = SinomoDataDir::new(temp.path().join("old-data"));
        let retired = retired_mount_fixture();
        let request = cloud_unregister_request(&data_dir, &retired);
        assert_eq!(request.sync_root, retired.mount_point);
        assert_eq!(request.root_path, retired.config.root_path);
        assert_eq!(request.data_dir_root, Some(data_dir.root()));

        let now = Instant::now();
        let calls = std::cell::Cell::new(0);
        let record = pending_cloud_unregister_record(
            request,
            module_provenance_fixture(
                sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
                "old-provider-hash",
            ),
        )
        .expect("pending record");
        let mut pending = Some(PendingCloudUnregister::new(record, None));
        let error = drive_pending_cloud_unregister_with(&mut pending, now, |pending| {
            calls.set(calls.get() + 1);
            assert_eq!(pending.record.request.root_path, "/old-root");
            Err("simulated unregister failure".to_string())
        })
        .expect_err("failed unregister must block retirement");
        assert_eq!(error, "simulated unregister failure");
        assert!(pending.is_some(), "failure must remain fail-closed");
        assert_eq!(calls.get(), 1);
        let failure_status = pending_cloud_unregister_status(
            pending.as_ref().expect("pending unregister"),
            37,
            4,
            error,
        );
        assert_eq!(failure_status.phase, SifsMountRuntimePhase::Error);
        assert_eq!(failure_status.configured_mount_point, "C:\\Sinomo\\Old");
        assert_eq!(
            failure_status.resolved_mount_point,
            Some("C:\\Sinomo\\Old".to_string())
        );
        assert!(
            failure_status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("simulated unregister failure"))
        );

        let before_retry = now + MOUNT_START_RETRY_INITIAL - Duration::from_millis(1);
        drive_pending_cloud_unregister_with(&mut pending, before_retry, |_| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .expect_err("retry before the deadline must remain blocked");
        assert_eq!(calls.get(), 1, "early retry must not invoke the module");

        drive_pending_cloud_unregister_with(
            &mut pending,
            now + MOUNT_START_RETRY_INITIAL,
            |pending| {
                calls.set(calls.get() + 1);
                assert_eq!(
                    pending.record.request.sync_root,
                    PathBuf::from("C:\\Sinomo\\Old")
                );
                Ok(())
            },
        )
        .expect("idempotent retry succeeds");
        assert!(pending.is_none());
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn stop_timeout_retains_active_cleanup_lease_and_never_unregisters() {
        let temp = TestDir::new("sifs-cloud-stop-timeout");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let lease = Arc::new(
            SifsMountLease::try_acquire(&data_dir, "active provider")
                .expect("active provider lease"),
        );
        let record = pending_record_fixture(&data_dir);
        let mut pending = Some(PendingCloudUnregister::new(record, Some(lease)));
        let active = std::cell::Cell::new(true);
        let unregister_calls = std::cell::Cell::new(0);

        let error = stop_then_drive_pending_cloud_unregister_with(
            &mut pending,
            Instant::now(),
            || Err("provider did not stop before the deadline".to_string()),
            |_| {
                unregister_calls.set(unregister_calls.get() + 1);
                active.set(false);
                Ok(())
            },
        )
        .expect_err("a live provider must block unregister");

        assert!(error.contains("did not stop"));
        assert!(active.get(), "the active provider remains owned");
        assert_eq!(unregister_calls.get(), 0);
        let pending = pending.as_ref().expect("durable cleanup remains pending");
        assert!(
            pending.lease.is_some(),
            "cleanup retains the singleton lease"
        );
        assert!(
            pending
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("did not stop"))
        );
        assert!(
            SifsMountLease::try_acquire(&data_dir, "replacement provider").is_err(),
            "replacement remains excluded until the live provider stops"
        );
    }

    #[test]
    fn blocked_replacement_stops_once_without_unregister_or_ownership_mutation() {
        let temp = TestDir::new("sifs-cloud-blocked-replacement-stop");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let staged = may_be_registered_stage(&recovery);
        persist_staged_cloud_registration_record(&data_dir, &staged).expect("old exact stage");
        let stop_calls = std::cell::Cell::new(0);
        let unregister_calls = std::cell::Cell::new(0);
        let start_calls = std::cell::Cell::new(0);

        let error = stop_after_failed_cloud_retirement_preparation_with(
            "Cloud Files registration replacement is blocked".to_string(),
            || {
                stop_calls.set(stop_calls.get() + 1);
                Ok(())
            },
            || {
                unregister_calls.set(unregister_calls.get() + 1);
                Ok(())
            },
            || {
                start_calls.set(start_calls.get() + 1);
                Ok(())
            },
        )
        .expect_err("replacement remains blocked after clean disconnect");

        assert!(error.contains("provider was stopped"));
        assert_eq!(stop_calls.get(), 1);
        assert_eq!(unregister_calls.get(), 0);
        assert_eq!(start_calls.get(), 0);
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("old stage remains"),
            Some(staged)
        );
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("replacement never creates pending cleanup")
                .is_none()
        );
    }

    #[test]
    fn cleanup_preparation_error_still_attempts_one_clean_stop() {
        let stop_calls = std::cell::Cell::new(0);
        let unregister_calls = std::cell::Cell::new(0);
        let start_calls = std::cell::Cell::new(0);
        let error = stop_after_failed_cloud_retirement_preparation_with(
            "exact cleanup provider could not be pinned".to_string(),
            || {
                stop_calls.set(stop_calls.get() + 1);
                Ok(())
            },
            || {
                unregister_calls.set(unregister_calls.get() + 1);
                Ok(())
            },
            || {
                start_calls.set(start_calls.get() + 1);
                Ok(())
            },
        )
        .expect_err("preparation failure remains blocked");

        assert_eq!(stop_calls.get(), 1);
        assert_eq!(unregister_calls.get(), 0);
        assert_eq!(start_calls.get(), 0);
        assert!(error.contains("ownership evidence remains pinned"));
    }

    #[test]
    fn pending_unregister_survives_crash_after_stop_and_holds_lease_until_clear() {
        let temp = TestDir::new("sifs-cloud-unregister-crash-recovery");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let record = pending_record_fixture(&data_dir);

        persist_pending_cloud_unregister_record(&data_dir, &record)
            .expect("journal unregister before provider stop");
        assert_eq!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("load journal before crash")
                .as_ref(),
            Some(&record)
        );

        // A process crash loses all in-memory state after the provider stopped.
        // Recovery must reconstruct the exact descriptor and own the node-local
        // mount lease before it can clear the durable intent.
        let recovered = load_pending_cloud_unregister_record(&data_dir)
            .expect("recover journal")
            .expect("pending journal after crash");
        let mut pending = Some(PendingCloudUnregister::new(recovered, None));
        drive_pending_cloud_unregister_with(&mut pending, Instant::now(), |pending| {
            pending.lease = Some(Arc::new(
                SifsMountLease::try_acquire(&data_dir, "crash recovery cleanup")
                    .expect("cleanup lease"),
            ));
            assert!(
                SifsMountLease::try_acquire(&data_dir, "replacement mount").is_err(),
                "replacement must be excluded until the durable cleanup clears"
            );
            assert_eq!(pending.record.request.root_path, "/old-root");
            assert!(
                verify_cloud_unregister_recovery_module(&data_dir, &pending.record.module).is_ok(),
                "recovery must not depend on the deleted source/cache artifact"
            );
            clear_pending_cloud_unregister_record(&data_dir, &pending.record)?;
            remove_cloud_unregister_recovery_module(&data_dir, &pending.record.module)
        })
        .expect("idempotent unregister and journal clear");

        assert!(pending.is_none());
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("journal state after recovery")
                .is_none()
        );
        assert!(
            verify_cloud_unregister_recovery_module(&data_dir, &record.module).is_err(),
            "the staged provider is removed only after durable journal clear"
        );
        SifsMountLease::try_acquire(&data_dir, "replacement after cleanup")
            .expect("clearing pending cleanup releases the mount lease");
    }

    #[test]
    fn pre_ready_crash_uses_conditional_preserve_and_can_finish_as_released() {
        let temp = TestDir::new("sifs-cloud-registration-pre-event-disabled");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("old-mount");
        std::fs::create_dir_all(&mount_point).expect("old mount point");
        let record = staged_record_fixture(&data_dir, &mount_point, "/old-root");
        let staged = may_be_registered_stage(&record);
        persist_staged_cloud_registration_record(&data_dir, &staged)
            .expect("persist recovery before module invocation");
        assert!(
            !data_dir
                .module_runtime_dir()
                .join(sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID)
                .join(CLOUD_FILES_REGISTRATION_RECEIPT_FILE)
                .exists(),
            "the simulated crash occurs before any public registration receipt"
        );

        let mut disabled = windows_mount_config(&mount_point, "/old-root");
        disabled.enabled = false;
        let recovered = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &disabled,
            Some(&record.module),
        )
        .expect("recover staged registration")
        .expect("disabled config must promote conditional cleanup");
        assert_eq!(
            recovered.operation,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        );
        assert!(same_cloud_registration_recovery_identity(
            &recovered, &record
        ));
        persist_pending_cloud_unregister_record(&data_dir, &recovered)
            .expect("persist promoted cleanup before host effect");

        let response = CloudUnregisterCompletion::StatePreserved(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse {
                sync_root: recovered.request.sync_root.clone(),
                root_path: recovered.request.root_path.clone(),
                was_registered: false,
                outcome: sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterOutcome::Released,
                detail: "journal, receipt, and CFAPI registration were absent".to_string(),
                cursor_recovery: None,
            },
        );
        let error = finalize_cloud_unregister_completion_with(
            &data_dir,
            &recovered,
            &staged,
            &response,
            || Err("simulated crash after first durable completion step".to_string()),
        )
        .expect_err("simulate crash after pending clear");
        assert!(error.contains("simulated crash"));
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("pending cut state")
                .is_none(),
            "Released clears pending before touching the authoritative stage"
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("stage cut state"),
            Some(staged.clone())
        );

        let replay = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &disabled,
            Some(&record.module),
        )
        .expect("reconstruct released cleanup after crash")
        .expect("staged record reconstructs cleanup");
        persist_pending_cloud_unregister_record(&data_dir, &replay)
            .expect("re-persist replay intent");
        finalize_cloud_unregister_completion_with(
            &data_dir,
            &replay,
            &staged,
            &response,
            || Ok(()),
        )
        .expect("replayed Released completion clears stage after pending");

        assert!(
            load_staged_cloud_registration_record(&data_dir)
                .expect("staged recovery after cleanup")
                .is_none()
        );
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("pending recovery after cleanup")
                .is_none()
        );
    }

    #[test]
    fn retained_completion_cut_clears_pending_then_retires_only_legacy_provider() {
        let temp = TestDir::new("sifs-cloud-retained-origin-cut");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let v2 = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
            b"retained-cut-v2-provider",
        );
        let v3 = pending_record_fixture_with_interface(
            &data_dir,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            b"retained-cut-v3-provider",
        );
        let mut pending = pending_cloud_unregister_record_with_policy(
            v3.request.clone(),
            v3.module.clone(),
            CloudUnregisterPolicy::PreserveState,
        )
        .expect("v3 preserve intent");
        pending.legacy_prejournal_origin = Some(v2.module.clone());
        let staged = may_be_registered_stage(&pending);
        persist_staged_cloud_registration_record(&data_dir, &staged)
            .expect("stage adopted v3 provider and exact v2 origin");
        persist_pending_cloud_unregister_record(&data_dir, &pending)
            .expect("persist retain-required cleanup");
        let response = CloudUnregisterCompletion::StatePreserved(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterResponse {
                sync_root: pending.request.sync_root.clone(),
                root_path: pending.request.root_path.clone(),
                was_registered: false,
                outcome: sinomo_runtime::dfs_cloud_files_api::CloudFilesPreserveStateUnregisterOutcome::Retained,
                detail: "journal normalized to StateOwned".to_string(),
                cursor_recovery: None,
            },
        );

        finalize_cloud_unregister_completion_with(&data_dir, &pending, &staged, &response, || {
            Err("simulated crash after RetainedAbsent fsync".to_string())
        })
        .expect_err("simulate response-loss cut");
        let retained = load_staged_cloud_registration_record(&data_dir)
            .expect("load retained cut state")
            .expect("retained stage exists");
        assert_eq!(retained.state, StagedCloudRegistrationState::RetainedAbsent);
        assert!(retained.recovery.legacy_prejournal_origin.is_none());
        assert_eq!(
            load_pending_cloud_unregister_record(&data_dir).expect("pending cut state"),
            Some(pending.clone())
        );
        assert!(
            finish_already_applied_retained_completion(&data_dir, &pending, &retained)
                .expect("finish already-applied Retained")
        );
        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("pending after retained replay")
                .is_none()
        );
        assert!(
            verify_cloud_unregister_recovery_module(&data_dir, &pending.module).is_ok(),
            "current v3 cleanup authority remains pinned"
        );
        assert!(
            verify_cloud_unregister_recovery_module(&data_dir, &v2.module).is_err(),
            "discharged v2 artifact is removed only after stage and pending settle"
        );
    }

    #[test]
    fn staged_registration_recovery_fails_closed_without_its_exact_provider() {
        let temp = TestDir::new("sifs-cloud-registration-provider-missing");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("old-mount");
        std::fs::create_dir_all(&mount_point).expect("old mount point");
        let record = staged_record_fixture(&data_dir, &mount_point, "/old-root");
        let staged = may_be_registered_stage(&record);
        persist_staged_cloud_registration_record(&data_dir, &staged)
            .expect("persist exact staged recovery");
        remove_cloud_unregister_recovery_module(&data_dir, &record.module)
            .expect("simulate unavailable exact provider");

        let mut disabled = windows_mount_config(&mount_point, "/old-root");
        disabled.enabled = false;
        let error = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &disabled,
            Some(&record.module),
        )
        .expect_err("missing exact provider must block cleanup and replacement");

        assert!(
            error.contains("not self-contained")
                || error.contains("cannot be recovered by its exact provider")
        );
        assert!(staged_cloud_registration_path(&data_dir).exists());
    }

    #[test]
    fn path_change_restart_cannot_replace_staged_provider_before_cleanup() {
        let temp = TestDir::new("sifs-cloud-registration-path-change");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let old_mount = temp.path().join("old-mount");
        let new_mount = temp.path().join("new-mount");
        std::fs::create_dir_all(&old_mount).expect("old mount point");
        std::fs::create_dir_all(&new_mount).expect("new mount point");
        let old_record = staged_record_fixture(&data_dir, &old_mount, "/old-root");
        let old_staged = may_be_registered_stage(&old_record);
        persist_staged_cloud_registration_record(&data_dir, &old_staged)
            .expect("persist old recovery descriptor");

        let replacement_config = windows_mount_config(&new_mount, "/new-root");
        let error = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &replacement_config,
            Some(&old_record.module),
        )
        .expect_err("path replacement must remain blocked");
        assert!(error.contains("registration replacement is blocked"));

        let replacement_record = pending_cloud_unregister_record(
            sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
                sync_root: new_mount,
                root_path: "/new-root".to_string(),
                data_dir_root: Some(data_dir.root()),
                provider_name: old_record.request.provider_name.clone(),
                cursor_recovery: None,
            },
            old_record.module.clone(),
        )
        .expect("replacement descriptor");
        let replacement_staged = may_be_registered_stage(&replacement_record);
        let error = persist_staged_cloud_registration_record(&data_dir, &replacement_staged)
            .expect_err("replacement must not discard old staged ownership");
        assert!(error.contains("explicit compare-and-swap"));
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("retained old staged record"),
            Some(old_staged)
        );

        assert!(
            load_pending_cloud_unregister_record(&data_dir)
                .expect("no replacement pending effect")
                .is_none()
        );
    }

    #[test]
    fn matching_staged_registration_is_retained_for_same_provider_restart() {
        let temp = TestDir::new("sifs-cloud-registration-same-provider");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let record = staged_record_fixture(&data_dir, &mount_point, "/root");
        let staged = may_be_registered_stage(&record);
        persist_staged_cloud_registration_record(&data_dir, &staged)
            .expect("persist exact recovery descriptor");

        assert!(
            recover_pending_cloud_unregister_from_staged_with(
                &data_dir,
                &windows_mount_config(&mount_point, "/root"),
                Some(&record.module),
            )
            .expect("same registration recovery")
            .is_none(),
            "same provider and identity should re-enter the module's own idempotent mount recovery"
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir)
                .expect("staged descriptor remains pinned"),
            Some(staged)
        );
    }

    #[test]
    fn pending_unregister_journal_rejects_identity_replacement() {
        let temp = TestDir::new("sifs-cloud-unregister-journal-conflict");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let original = pending_record_fixture(&data_dir);
        persist_pending_cloud_unregister_record(&data_dir, &original)
            .expect("persist original cleanup identity");

        let mut replacement_request = original.request.clone();
        replacement_request.root_path = "/replacement-root".to_string();
        let replacement =
            pending_cloud_unregister_record(replacement_request, original.module.clone())
                .expect("replacement descriptor");
        let error = persist_pending_cloud_unregister_record(&data_dir, &replacement)
            .expect_err("a different identity must not overwrite durable cleanup");

        assert!(error.contains("refusing to replace"));
        assert_eq!(
            load_pending_cloud_unregister_record(&data_dir).expect("load retained cleanup"),
            Some(original)
        );
    }

    #[test]
    fn mount_lease_excludes_same_process_and_releases_on_drop() {
        let temp = TestDir::new("sifs-mount-lease-local");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let other_data_dir = SinomoDataDir::new(temp.path().join("other-data"));

        let first = SifsMountLease::try_acquire(&data_dir, "first test owner")
            .expect("first lease must be available");
        let error = SifsMountLease::try_acquire(&data_dir, "second test owner")
            .expect_err("same data directory must be excluded");
        assert!(error.contains("another managed or foreground mount"));
        let other = SifsMountLease::try_acquire(&other_data_dir, "other test owner")
            .expect("different data directories must not contend");

        drop(first);
        let replacement = SifsMountLease::try_acquire(&data_dir, "replacement test owner")
            .expect("dropping the owner must release the lease");
        drop((replacement, other));
    }

    #[test]
    fn mount_lease_excludes_another_process() {
        let temp = TestDir::new("sifs-mount-lease-process");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let lease = SifsMountLease::try_acquire(&data_dir, "parent test owner")
            .expect("parent lease must be available");

        run_mount_lease_probe(&data_dir, false);
        drop(lease);
        run_mount_lease_probe(&data_dir, true);
    }

    fn run_mount_lease_probe(data_dir: &SinomoDataDir, expect_available: bool) {
        let output = std::process::Command::new(std::env::current_exe().expect("current test exe"))
            .arg("--exact")
            .arg("dfs_mount_supervisor::tests::mount_lease_subprocess_probe")
            .arg("--nocapture")
            .env(MOUNT_LEASE_PROBE_ROOT, data_dir.root())
            .env(
                MOUNT_LEASE_PROBE_EXPECT_AVAILABLE,
                if expect_available { "true" } else { "false" },
            )
            .output()
            .expect("run mount lease subprocess probe");
        assert!(
            output.status.success(),
            "mount lease subprocess failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("mount-lease-probe-complete"),
            "mount lease subprocess test did not run: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[test]
    fn mount_lease_subprocess_probe() {
        let Some(root) = std::env::var_os(MOUNT_LEASE_PROBE_ROOT) else {
            return;
        };
        let expect_available = std::env::var(MOUNT_LEASE_PROBE_EXPECT_AVAILABLE)
            .expect("probe availability expectation")
            == "true";
        let result =
            SifsMountLease::try_acquire(&SinomoDataDir::new(root), "mount lease subprocess probe");
        assert_eq!(
            result.is_ok(),
            expect_available,
            "unexpected subprocess lease result: {:?}",
            result.err()
        );
        println!("mount-lease-probe-complete");
    }

    #[test]
    fn supervisor_hook_retains_the_status_service() {
        let temp = TestDir::new("sifs-mount-supervisor-service");
        let data_dir = SinomoDataDir::new(temp.path());
        let hook =
            sifs_mount_supervisor_service_hook(data_dir.clone(), DaemonPaths::new(&data_dir));
        let registry = hook.register(ServiceRegistryBuilder::default()).build();

        assert!(registry.owns(SIFS_MOUNT_STATUS_ROUTE));
        assert!(registry.service_names().contains(&"sifs-mount-supervisor"));
    }

    #[test]
    fn mount_config_rejects_data_dir_overlap_and_parent_escape() {
        let temp = TestDir::new("sifs-mount-supervisor-overlap");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let overlapping = SifsMountConfig {
            enabled: true,
            mount_point: data_dir.root().join("mount").display().to_string(),
            ..SifsMountConfig::default()
        };
        assert!(
            validate_mount_config(&data_dir, &overlapping)
                .expect_err("data-dir overlap must fail")
                .contains("overlaps")
        );

        let escaping = SifsMountConfig {
            enabled: true,
            mount_point: "/../../sinomo".to_string(),
            ..SifsMountConfig::default()
        };
        assert!(
            validate_mount_config(&data_dir, &escaping)
                .expect_err("root escape must fail")
                .contains("escapes")
        );
    }

    #[cfg(unix)]
    #[test]
    fn mount_config_rejects_overlap_through_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let temp = TestDir::new("sifs-mount-supervisor-symlink-overlap");
        let data_root = temp.path().join("data");
        std::fs::create_dir_all(&data_root).expect("create data root");
        let alias = temp.path().join("data-alias");
        symlink(&data_root, &alias).expect("create data-root symlink");
        let data_dir = SinomoDataDir::new(&data_root);
        let config = SifsMountConfig {
            enabled: true,
            mount_point: alias.join("mount").display().to_string(),
            ..SifsMountConfig::default()
        };

        assert!(
            validate_mount_config(&data_dir, &config)
                .expect_err("symlinked data-dir overlap must fail")
                .contains("overlaps")
        );
    }

    #[test]
    fn mount_start_retry_is_bounded_and_configuration_changes_retry_immediately() {
        let now = Instant::now();
        let config = SifsMountConfig::default();
        let mut retry = MountStartRetry::default();

        assert!(retry.permits_attempt(&config, now));
        retry.record_failure(&config, "first failure", now);
        assert_eq!(retry.delay, MOUNT_START_RETRY_INITIAL);
        assert!(!retry.permits_attempt(
            &config,
            now + MOUNT_START_RETRY_INITIAL - Duration::from_millis(1)
        ));
        assert!(retry.permits_attempt(&config, now + MOUNT_START_RETRY_INITIAL));

        retry.record_failure(&config, "second failure", now + MOUNT_START_RETRY_INITIAL);
        assert_eq!(retry.delay, MOUNT_START_RETRY_INITIAL * 2);
        for attempt in 0..16 {
            retry.record_failure(&config, format!("failure {attempt}"), now);
        }
        assert_eq!(retry.delay, MOUNT_START_RETRY_MAX);

        let mut changed = config.clone();
        changed.root_path = "/changed".to_string();
        assert!(retry.permits_attempt(&changed, now));
        assert!(retry.last_error().is_none());
    }

    #[test]
    fn post_spawn_provider_failure_keeps_backing_off_until_runtime_is_healthy() {
        let now = Instant::now();
        let mut config = SifsMountConfig::default();
        config.enabled = true;
        let mut retry = MountStartRetry::default();

        retry.record_failure(&config, "first provider failure", now);
        let second_attempt_at = now + MOUNT_START_RETRY_INITIAL;
        assert!(retry.permits_attempt(&config, second_attempt_at));

        let mut loading = status_for_config(
            &config,
            None,
            31,
            1,
            SifsMountRuntimePhase::Starting,
            "loading the Windows Cloud Files provider",
        );
        loading.platform = SifsMountPlatform::WindowsCloudFiles;
        assert!(!mount_runtime_confirms_start(&loading));
        assert_eq!(retry.delay, MOUNT_START_RETRY_INITIAL);

        retry.record_failure(&config, "post-spawn provider failure", second_attempt_at);
        assert_eq!(retry.delay, MOUNT_START_RETRY_INITIAL * 2);
        assert!(!retry.permits_attempt(&config, second_attempt_at + MOUNT_START_RETRY_INITIAL));

        let mut connected = status_for_config(
            &config,
            None,
            31,
            2,
            SifsMountRuntimePhase::Connected,
            "Cloud Files hydration callbacks connected",
        );
        connected.platform = SifsMountPlatform::WindowsCloudFiles;
        connected.provider_registered = true;
        connected.provider_connected = true;
        connected.worker_alive = true;
        connected.worker_ready = true;
        connected.worker_health_generation = 1;
        assert!(mount_runtime_confirms_start(&connected));
        retry.clear();
        assert!(retry.permits_attempt(&config, second_attempt_at));
    }

    #[test]
    fn disconnected_cloud_files_completion_is_a_terminal_error() {
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        drop(finished_tx);

        let result = poll_cloud_files_completion(&finished_rx, true)
            .expect("disconnected completion must be terminal");
        let error = cloud_files_completion_error(Some(&result))
            .expect("terminal completion must carry a status error");
        assert_eq!(
            error,
            "Cloud Files provider worker exited without a terminal result"
        );

        let mut status = status_for_config(
            &SifsMountConfig::default(),
            None,
            37,
            0,
            SifsMountRuntimePhase::Starting,
            "loading the Windows Cloud Files provider",
        );
        status.platform = SifsMountPlatform::WindowsCloudFiles;
        status.last_error = Some(error);
        if status.last_error.is_some() {
            status.phase = SifsMountRuntimePhase::Error;
        }
        assert_eq!(status.phase, SifsMountRuntimePhase::Error);
        assert!(!mount_runtime_confirms_start(&status));
    }

    #[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
    #[test]
    fn fatal_fuse_health_is_reported_as_error_before_worker_join() {
        let config = SifsMountConfig {
            enabled: true,
            read_only: false,
            ..SifsMountConfig::default()
        };
        let status = unix_fuse_mount_status(
            &config,
            Path::new("/home/ix/sinomo"),
            41,
            3,
            Some("mounted mutation requires durable recovery".to_string()),
        );
        assert_eq!(status.phase, SifsMountRuntimePhase::Error);
        assert_eq!(
            status.last_error.as_deref(),
            Some("mounted mutation requires durable recovery")
        );
        assert!(!mount_runtime_confirms_start(&status));
    }

    #[test]
    fn mount_exit_status_is_persisted_before_retirement() {
        let events = std::cell::RefCell::new(Vec::new());

        retire_after_exit_status_persisted(
            || {
                events.borrow_mut().push("persist-error");
                Ok(())
            },
            || {
                events.borrow_mut().push("retire-adapter");
                Ok(())
            },
        )
        .expect("persisted exit status permits retirement");

        assert_eq!(events.into_inner(), vec!["persist-error", "retire-adapter"]);
    }

    #[test]
    fn mount_exit_status_persistence_failure_blocks_retirement() {
        let retired = std::cell::Cell::new(false);

        let error = retire_after_exit_status_persisted(
            || Err("simulated status store failure".to_string()),
            || {
                retired.set(true);
                Ok(())
            },
        )
        .expect_err("unpersisted fatal status must retain the adapter");

        assert!(error.contains("simulated status store failure"));
        assert!(!retired.get());
    }

    #[test]
    fn failed_status_persistence_is_retried_before_deduplication() {
        let observed = RwLock::new(SifsMountRuntimeStatus::default());
        let mut last_persisted_status = None;
        let status = status_for_config(
            &SifsMountConfig::default(),
            None,
            23,
            0,
            SifsMountRuntimePhase::Disabled,
            "disabled by retry test",
        );
        let mut persistence_attempts = 0;

        publish_status_with(
            &observed,
            &mut last_persisted_status,
            status.clone(),
            |_| {
                persistence_attempts += 1;
                Err("simulated write failure".to_string())
            },
        );
        assert!(last_persisted_status.is_none());
        publish_status_with(
            &observed,
            &mut last_persisted_status,
            status.clone(),
            |_| {
                persistence_attempts += 1;
                Ok(())
            },
        );
        assert!(last_persisted_status.is_some());
        publish_status_with(&observed, &mut last_persisted_status, status, |_| {
            persistence_attempts += 1;
            Ok(())
        });

        assert_eq!(persistence_attempts, 2);
        assert_eq!(
            observed.read().expect("observed status").daemon_pid,
            Some(23)
        );
    }

    #[test]
    fn mount_status_persistence_is_change_driven() {
        let temp = TestDir::new("sifs-mount-supervisor-status");
        let data_dir = SinomoDataDir::new(temp.path());
        let store = sifs_mount_config_store().expect("mount config store");
        let observed = RwLock::new(SifsMountRuntimeStatus::default());
        let mut last_persisted_status = None;
        let config = SifsMountConfig::default();
        let status = status_for_config(
            &config,
            None,
            17,
            0,
            SifsMountRuntimePhase::Disabled,
            "disabled by test",
        );
        publish_status(
            &store,
            &data_dir,
            &observed,
            &mut last_persisted_status,
            status.clone(),
        );
        let first = store
            .get_typed::<SifsMountRuntimeStatus>(&data_dir, None, SIFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("persisted status");
        publish_status(
            &store,
            &data_dir,
            &observed,
            &mut last_persisted_status,
            status,
        );
        let second = store
            .get_typed::<SifsMountRuntimeStatus>(&data_dir, None, SIFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("same persisted status");
        assert_eq!(first.updated_at, second.updated_at);
        assert_eq!(second.daemon_pid, Some(17));
    }

    #[test]
    fn worker_heartbeats_refresh_live_status_without_rewriting_local_config() {
        let observed = RwLock::new(SifsMountRuntimeStatus::default());
        let mut last_persisted_status = None;
        let mut status = status_for_config(
            &SifsMountConfig::default(),
            None,
            29,
            0,
            SifsMountRuntimePhase::Connected,
            "worker heartbeat",
        );
        status.worker_alive = true;
        status.worker_ready = true;
        status.worker_health_generation = 1;
        status.last_worker_heartbeat_at = Some("2026-08-10T21:00:00Z".to_string());
        let mut persistence_attempts = 0;

        publish_status_with(
            &observed,
            &mut last_persisted_status,
            status.clone(),
            |_| {
                persistence_attempts += 1;
                Ok(())
            },
        );
        status.worker_health_generation = 2;
        status.last_worker_heartbeat_at = Some("2026-08-10T21:00:01Z".to_string());
        publish_status_with(&observed, &mut last_persisted_status, status, |_| {
            persistence_attempts += 1;
            Ok(())
        });

        assert_eq!(persistence_attempts, 1);
        assert_eq!(
            observed
                .read()
                .expect("observed status")
                .worker_health_generation,
            2
        );
        assert_eq!(
            last_persisted_status
                .expect("persisted status")
                .worker_health_generation,
            1
        );
    }

    #[test]
    fn cloud_attention_is_preserved_in_the_local_mount_status_contract() {
        use sinomo_runtime::dfs_cloud_files_api::{
            CloudFilesRetainedLocalEvidenceSummary, CloudFilesSyncAttentionKind,
            CloudFilesSyncAttentionNextAction, CloudFilesSyncAttentionSummary,
        };

        let summary = cloud_files_attention_summary(CloudFilesSyncAttentionSummary {
            kind: CloudFilesSyncAttentionKind::LocalOperationConflict,
            path: "/services/report.txt".to_string(),
            reason_code: "remote-version-changed".to_string(),
            reason: "the authoritative version changed while a local edit was pending".to_string(),
            retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                operation_id: Some("cf_01localoperation".to_string()),
                evidence_id: Some("evidence-1".to_string()),
                staged_content_hash: Some("blake3:staged".to_string()),
                staged_content_size_bytes: Some(42),
                local_state: Some("pending-conflict".to_string()),
            }),
            safe_next_action: CloudFilesSyncAttentionNextAction::PreserveLocalCopyAndRescan,
        });

        assert_eq!(summary.kind, SifsMountAttentionKind::LocalOperationConflict);
        assert_eq!(summary.path, "/services/report.txt");
        assert_eq!(
            summary.safe_next_action,
            SifsMountAttentionNextAction::PreserveLocalCopyAndRescan
        );
        let evidence = summary
            .retained_local_evidence
            .expect("retained local evidence");
        assert_eq!(
            evidence.operation_id.as_deref(),
            Some("cf_01localoperation")
        );
        assert_eq!(evidence.staged_content_size_bytes, Some(42));
    }
}
