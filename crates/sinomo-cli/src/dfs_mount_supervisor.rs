use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_runtime::config::{ConfigRegistry, ConfigStore, ConfigWriteTarget};
use sinomo_runtime::daemon::{DaemonApiClient, DaemonPaths, DaemonServiceRegistryHook};
use sinomo_runtime::dfs::{
    SIFS_MOUNT_CONFIG_KEY, SIFS_MOUNT_STATUS_CONFIG_KEY, SIFS_MOUNT_STATUS_ROUTE, SifsMountConfig,
    SifsMountPlatform, SifsMountRuntimePhase, SifsMountRuntimeStatus, SifsMountStatusRequest,
    SifsMountStatusResponse, SifsMountWritebackMode, sifs_mount_config_definition,
    sifs_mount_status_config_definition,
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
    worker: Mutex<Option<SifsMountSupervisorWorker>>,
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

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
#[derive(Debug)]
struct PendingCloudUnregister {
    record: PendingCloudUnregisterRecord,
    lease: Option<Arc<SifsMountLease>>,
    retry_delay: Duration,
    next_attempt_at: Option<Instant>,
    last_error: Option<String>,
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
    require_preserve_capability: bool,
    effect: FEffect,
) -> Result<R, String>
where
    FEffect: FnOnce() -> Result<R, String>,
{
    validate_cloud_files_provider_descriptor(provenance, descriptor, required_operation)?;
    if require_preserve_capability
        && !provider_supports_state_preserving_unregister(
            &descriptor.interface_version,
            &descriptor.supported_operations,
            &descriptor.capabilities,
        )
    {
        return Err(
            "Cloud Files provider lacks the required v3 preserve-state capability".to_string(),
        );
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
        let mut retained = staged_cloud_registration_record(
            pending,
            StagedCloudRegistrationState::RetainedAbsent,
        )?;
        // A typed Retained response proves that the module found or normalized
        // its exact journal to StateOwned. Legacy pre-journal ambiguity is now
        // discharged; the original v2 provider is no longer cleanup authority.
        retained.recovery.legacy_prejournal_origin = None;
        replace_staged_cloud_registration_record(data_dir, staged, &retained)?;
        after_first_durable_step()?;
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
    if staged.state != StagedCloudRegistrationState::RetainedAbsent
        || staged.recovery.legacy_prejournal_origin.is_some()
        || pending.legacy_prejournal_origin.is_none()
        || !same_cloud_registration_base_identity(&staged.recovery, pending)
        || staged.recovery.module != pending.module
        || pending.operation
            != sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
    {
        return Ok(false);
    }
    // Crash after fsyncing the typed Retained transition but before clearing
    // the pending intent. RetainedAbsent is the durable completion proof.
    clear_pending_cloud_unregister_record(data_dir, pending)?;
    remove_discharged_legacy_recovery_module(data_dir, pending);
    Ok(true)
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

    #[cfg(windows)]
    fn matches_cache_entry(
        &self,
        entry: &sinomo_runtime::module::NativeDylibModuleCacheEntry,
    ) -> bool {
        self == &Self::from_cache_entry(entry)
    }
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

#[cfg(windows)]
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
fn load_pending_cloud_unregister_record(
    data_dir: &SinomoDataDir,
) -> Result<Option<PendingCloudUnregisterRecord>, String> {
    let path = pending_cloud_unregister_path(data_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "read pending Cloud Files unregister {}: {error}",
                path.display()
            ));
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
    Ok(Some(record))
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
    if let Some(existing) = load_pending_cloud_unregister_record(data_dir)? {
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
    if registration_is_current && (provider_is_current || !config.enabled) {
        if config.enabled {
            if record.state == StagedCloudRegistrationState::RetainedAbsent {
                let may_be_registered = StagedCloudRegistrationRecord {
                    state: StagedCloudRegistrationState::MayBeRegistered,
                    ..record.clone()
                };
                replace_staged_cloud_registration_record(data_dir, &record, &may_be_registered)?;
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
        true,
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
        return Ok(None);
    };
    ensure_cloud_unregister_recovery_module(data_dir, &record.recovery.module)?;
    if !config.enabled
        && record.recovery.module.interface_version
            == sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
    {
        // Consent-driven cleanup is bound to the exact pinned stage. A missing,
        // corrupt, or newly selected loader is neither evidence nor authority
        // for this already-staged v3 registration and must not block disable.
        return recover_pending_cloud_unregister_from_staged_with(data_dir, config, None);
    }
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

        let mut lease = Self { file, path };
        lease.write_owner(owner)?;
        Ok(lease)
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
                );
                let _ = stopped_tx.send(());
            })
            .expect("SIFS mount supervisor thread should start");
        Self {
            inner: Arc::new(SifsMountSupervisorInner {
                status,
                shutdown,
                mount_cancellation,
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
        &[SIFS_MOUNT_STATUS_ROUTE]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let status = self.status();
        Box::pin(async move {
            match request.route.as_str() {
                SIFS_MOUNT_STATUS_ROUTE => {
                    let _: SifsMountStatusRequest = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(SIFS_MOUNT_STATUS_ROUTE, SifsMountStatusResponse { status })?;
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
            match recover_pending_cloud_unregister_from_staged(data_dir, &config) {
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

        if !config.enabled {
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
            let start_result = SifsMountLease::try_acquire(data_dir, &owner).and_then(|lease| {
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
    if matches!(
        reason,
        MountRetirementReason::ExplicitDisable | MountRetirementReason::RegistrationReplacement
    ) && let Some(mount) = active.as_ref()
    {
        let retired = RetiredMount {
            config: mount.config().clone(),
            mount_point: mount.mount_point().to_path_buf(),
            lease: Some(Arc::clone(&mount.lease)),
            module_provenance: mount.module_provenance.clone(),
        };
        let preparation = (|| {
            let mut expected_record = pending_cloud_unregister_record_with_policy(
                cloud_unregister_request(data_dir, &retired),
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
}

#[cfg(windows)]
impl sinomo_runtime::module::NativeModuleHostCallbacks for CloudFilesSupervisorCallbacks {
    fn output_payload(&self, payload_sobj: &[u8]) {
        use sinomo_runtime::dfs_cloud_files_api::CloudFilesModuleEvent;

        match sinomo_runtime::module_api::decode_sobj::<CloudFilesModuleEvent>(payload_sobj) {
            Ok(CloudFilesModuleEvent::MountLifecycle(event)) => {
                let _ = self.events.try_send(event);
            }
            Err(error) => {
                tracing::warn!("ignored malformed Cloud Files lifecycle output: {error}");
            }
        }
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

    let module_id = ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .map_err(|error| format!("invalid Cloud Files module id: {error}"))?;
    let data_dir_root = data_dir.root();
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
    let library_path = native_module_cache_path(
        &data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let module_provenance = CloudFilesModuleProvenance::from_loader_state(&state);
    let module = unsafe {
        NativeModule::load_process_resident_from_content_addressed_path(
            &library_path,
            &state.content_hash,
        )
    }
    .map_err(|error| format!("load Cloud Files native module: {error}"))?;
    admit_cloud_files_provider_before_effect_with(
        &module_provenance,
        module.descriptor(),
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
        true,
        || {
            stage_cloud_unregister_recovery_module(data_dir, &module_provenance, &library_path)
                .map(|_| ())
                .map_err(|error| {
                    format!("stage durable Cloud Files unregister provider before mount: {error}")
                })
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
    };
    let recovery_record = pending_cloud_unregister_record_with_policy(
        sinomo_runtime::dfs_cloud_files_api::CloudFilesUnregisterRequest {
            sync_root: request.sync_root.clone(),
            root_path: request.root_path.clone(),
            data_dir_root: request.data_dir_root.clone(),
            provider_name: request.provider_name.clone(),
        },
        module_provenance.clone(),
        CloudUnregisterPolicy::Release,
    )?;
    let staged_recovery = staged_cloud_registration_record(
        &recovery_record,
        StagedCloudRegistrationState::MayBeRegistered,
    )?;
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
            "existing staged Cloud Files ownership cannot be replaced by mount startup".to_string(),
        ),
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
    let (event_tx, event_rx) = mpsc::sync_channel(32);
    let callbacks: Arc<dyn sinomo_runtime::module::NativeModuleHostCallbacks> =
        Arc::new(CloudFilesSupervisorCallbacks {
            cancel: Arc::clone(&cancel),
            supervisor_shutdown,
            events: event_tx,
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
        provider_content_hash: state.content_hash,
        provider_interface_version: state.interface_version,
        module_provenance,
        lease,
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
        let owner = format!(
            "pending Cloud Files unregister {}",
            pending.record.request.sync_root.display()
        );
        pending.lease = Some(Arc::new(SifsMountLease::try_acquire(data_dir, &owner)?));
    }
    ensure_cloud_unregister_recovery_module(data_dir, &pending.record.module)?;
    let staged = load_staged_cloud_registration_record(data_dir)?.ok_or_else(|| {
        "pending Cloud Files unregister has no authoritative staged ownership record".to_string()
    })?;
    if finish_already_applied_retained_completion(data_dir, &pending.record, &staged)? {
        return Ok(());
    }
    if !same_cloud_registration_recovery_identity(&staged.recovery, &pending.record) {
        return Err(
            "pending Cloud Files unregister no longer matches its staged ownership identity"
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
        record.operation == SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
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
            true,
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
            true,
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
                true,
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
                true,
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
                    true,
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
    fn disabled_v3_cleanup_uses_pinned_stage_when_selected_loader_differs() {
        let temp = TestDir::new("sifs-cloud-disabled-pinned-provider");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        std::fs::create_dir_all(&mount_point).expect("mount point");
        let recovery = staged_record_fixture(&data_dir, &mount_point, "/root");
        let staged = may_be_registered_stage(&recovery);
        persist_staged_cloud_registration_record(&data_dir, &staged).expect("persist old stage");
        let mut selected = recovery.module.clone();
        selected.content_hash = "different-selected-v3-provider".to_string();
        let mut disabled = windows_mount_config(&mount_point, "/root");
        disabled.enabled = false;

        let pending = recover_pending_cloud_unregister_from_staged_with(
            &data_dir,
            &disabled,
            Some(&selected),
        )
        .expect("pinned provider remains cleanup authority")
        .expect("disabled stage requires cleanup");

        assert_eq!(pending.module, recovery.module);
        assert_eq!(
            pending.operation,
            sinomo_runtime::dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION
        );
        assert_eq!(
            load_staged_cloud_registration_record(&data_dir).expect("stage remains exact"),
            Some(staged)
        );
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
