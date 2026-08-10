use crate::process::{MainExitKind, MainExitObservation};
use crate::{
    RolloutAction, RolloutDecision, RolloutError, RolloutRuntimeState, RolloutStage,
    SelfRolloutPaths, executable_name,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_config::{
    ConfigClusterMode, ConfigDefinition, ConfigError, ConfigRegistry, ConfigRegistryBuilder,
    ConfigStore, ConfigValueType, ConfigWriteTarget,
};
use sinomo_core::events::EventSequence;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::{ClusterNodeRolloutStatus, HotSwapState, Id};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const ROLLOUT_DIAGNOSTIC_TAIL_BYTES: usize = 8 * 1024;
pub const ROLLOUT_TELEMETRY_GATE_CONFIG_KEY: &str = "rollout.telemetry.gates";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMainActivation {
    pub source: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_main_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_shadow_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_main_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dylib_loader_rollback_generation: Option<String>,
    #[serde(default = "default_rollout_main_host")]
    pub main_host: String,
    #[serde(default = "default_rollout_main_port")]
    pub main_port: u16,
    /// Desired shadow-supervisor bind host. Older activation records omit it
    /// and therefore preserve the running shadow's listener during handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_host: Option<String>,
    pub shadow_port: u16,
    pub requested_at: String,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingShadowRollout {
    pub source: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_main_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_shadow_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_main_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dylib_loader_rollback_generation: Option<String>,
    pub previous_shadow_binary: PathBuf,
    pub main_host: String,
    pub main_port: u16,
    pub shadow_host: String,
    pub shadow_port: u16,
    pub requested_at: String,
    pub main_healthy_at: String,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeDylibLoaderStateRollback {
    pub generation: String,
    pub captured_at: String,
    /// Lifecycle of this pre-mutation rollback intent. Older records did not
    /// carry the field; treating them as unresolved is the fail-safe migration
    /// because the associated loader mutation may already have happened.
    #[serde(default)]
    pub lifecycle: NativeDylibLoaderRollbackLifecycle,
    pub states: Vec<sinomo_module::NativeDylibModuleLoaderState>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeDylibLoaderRollbackLifecycle {
    #[default]
    Unresolved,
    Restored,
    Accepted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutFailureDiagnostics {
    pub diagnostics_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMainExitRecord {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub requested_shutdown: bool,
    pub kind: MainExitKind,
    pub observed_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutTransitionRecord {
    pub record_id: Id,
    pub node_id: NodeId,
    pub rollout_plan_id: Id,
    pub artifact_id: Id,
    pub module_id: Id,
    pub action: RolloutAction,
    pub stage: RolloutStage,
    pub current_weight_bps: u16,
    pub active_artifact_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<Id>,
    #[serde(default = "default_transition_hot_swap_state")]
    pub hot_swap_state: HotSwapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggering_evidence_refs: Vec<String>,
    pub reason: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutTelemetryAdvanceState {
    pub rollout_plan_id: Id,
    pub module_id: Id,
    pub candidate_artifact_id: Id,
    #[serde(default = "default_rollout_telemetry_kind")]
    pub telemetry_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_consumed_sequence: Option<EventSequence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_record_id: Option<Id>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutTelemetryGateConfig {
    #[serde(default = "default_automatic_promotion_enabled")]
    pub automatic_promotion_enabled: bool,
    #[serde(default = "default_rollout_telemetry_event_limit")]
    pub event_limit: usize,
    #[serde(default = "default_rollout_telemetry_minimum_event_count")]
    pub minimum_event_count: u64,
    #[serde(default = "default_maximum_automatic_weight_bps")]
    pub maximum_automatic_weight_bps: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_candidate_latency_p95_ms: Option<u64>,
}

impl Default for RolloutTelemetryGateConfig {
    fn default() -> Self {
        Self {
            automatic_promotion_enabled: default_automatic_promotion_enabled(),
            event_limit: default_rollout_telemetry_event_limit(),
            minimum_event_count: default_rollout_telemetry_minimum_event_count(),
            maximum_automatic_weight_bps: default_maximum_automatic_weight_bps(),
            maximum_candidate_latency_p95_ms: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedRolloutTransition {
    pub record: RolloutTransitionRecord,
    pub path: PathBuf,
}

pub trait PendingShadowRolloutContext {
    fn activation(&self) -> &RolloutMainActivation;
    fn main_binary(&self) -> &Path;
    fn main_host(&self) -> &str;
    fn main_port(&self) -> u16;
    fn previous_main_binary(&self) -> Option<&Path> {
        None
    }
    fn previous_shadow_binary(&self) -> &Path;
}

pub fn rollout_main_activation_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-main-rollout.sobj")
}

pub fn rollout_shadow_finalize_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("pending-shadow-rollout.sobj")
}

pub fn rollout_activation_lock_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("activation.lock")
}

/// Exclusive admission guard for node-local rollout activation and module mutation.
///
/// The OS releases the advisory lock if the process exits, while the persistent
/// lock path gives every process on the node one coordination point. Acquisition
/// is non-blocking so a competing request fails clearly instead of waiting behind
/// an unbounded activation. Callers must acquire it before writing shared staged
/// binary paths and hold it through the durable pending-activation write. Build or
/// upload work may remain outside the guard when it does not touch shared paths.
#[derive(Debug)]
pub struct RolloutActivationGuard {
    file: fs::File,
    path: PathBuf,
}

impl RolloutActivationGuard {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RolloutActivationGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub fn acquire_rollout_activation_guard(
    paths: &SelfRolloutPaths,
) -> Result<RolloutActivationGuard, RolloutError> {
    let guard = acquire_rollout_activation_file_guard(paths)?;
    ensure_rollout_activation_admission(paths)?;
    Ok(guard)
}

fn acquire_rollout_activation_file_guard(
    paths: &SelfRolloutPaths,
) -> Result<RolloutActivationGuard, RolloutError> {
    fs::create_dir_all(&paths.root).map_err(|source| RolloutError::Io {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    let path = rollout_activation_lock_path(paths);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
            return Err(RolloutError::InvalidTransition {
                detail: format!(
                    "another rollout activation or module mutation holds {}",
                    path.display()
                ),
            });
        }
        Err(source) => {
            return Err(RolloutError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    Ok(RolloutActivationGuard { file, path })
}

fn ensure_rollout_activation_admission(paths: &SelfRolloutPaths) -> Result<(), RolloutError> {
    for (label, pending_path) in [
        ("main", rollout_main_activation_path(paths)),
        ("shadow", rollout_shadow_finalize_path(paths)),
    ] {
        if pending_path.exists() {
            return Err(RolloutError::InvalidTransition {
                detail: format!(
                    "cannot mutate native modules while a pending {label} rollout exists at {}",
                    pending_path.display()
                ),
            });
        }
    }
    if let Some((path, rollback)) = native_dylib_loader_rollback_records(paths)?
        .into_iter()
        .find(|(_, rollback)| rollback.lifecycle == NativeDylibLoaderRollbackLifecycle::Unresolved)
    {
        return Err(RolloutError::InvalidTransition {
            detail: format!(
                "cannot mutate native modules while unresolved pre-mutation intent {} exists at {}; restart the daemon to recover it",
                rollback.generation,
                path.display()
            ),
        });
    }
    Ok(())
}

/// Compatibility alias for module command paths that share rollout admission.
pub type NativeDylibRolloutGuard = RolloutActivationGuard;

pub fn native_dylib_rollout_lock_path(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_activation_lock_path(paths)
}

pub fn acquire_native_dylib_rollout_guard(
    paths: &SelfRolloutPaths,
) -> Result<NativeDylibRolloutGuard, RolloutError> {
    acquire_rollout_activation_guard(paths)
}

pub fn rollout_native_dylib_loader_rollback_dir(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("native-loader-rollback")
}

pub fn rollout_native_dylib_loader_rollback_path(
    paths: &SelfRolloutPaths,
    generation: &str,
) -> Result<PathBuf, RolloutError> {
    validate_rollout_generation(generation)?;
    Ok(rollout_native_dylib_loader_rollback_dir(paths).join(format!("{generation}.sobj")))
}

fn native_dylib_loader_rollback_records(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Vec<(PathBuf, NativeDylibLoaderStateRollback)>, RolloutError> {
    let dir = rollout_native_dylib_loader_rollback_dir(rollout_paths);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(&dir)
        .map_err(|source| RolloutError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| RolloutError::Io {
                    path: dir.display().to_string(),
                    detail: source.to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();

    let mut records = Vec::new();
    for path in paths {
        if path.extension().and_then(|extension| extension.to_str()) != Some("sobj") {
            continue;
        }
        let rollback: NativeDylibLoaderStateRollback = read_object(&path)?;
        let file_generation = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| RolloutError::InvalidArtifact {
                detail: format!(
                    "native loader rollback record has invalid file name {}",
                    path.display()
                ),
            })?;
        if rollback.generation != file_generation {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "native loader rollback generation mismatch at {}: file names {file_generation}, record names {}",
                    path.display(),
                    rollback.generation
                ),
            });
        }
        records.push((path, rollback));
    }
    Ok(records)
}

/// Returns every content-addressed native module artifact referenced by a
/// rollback generation that has not yet been finalized and removed. A malformed
/// rollback record fails closed so callers defer pruning instead of deleting a
/// potentially required artifact.
pub fn unresolved_native_dylib_loader_rollback_artifacts(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
) -> Result<BTreeSet<PathBuf>, RolloutError> {
    let mut artifacts = BTreeSet::new();
    for (_, rollback) in native_dylib_loader_rollback_records(rollout_paths)? {
        if rollback.lifecycle != NativeDylibLoaderRollbackLifecycle::Unresolved {
            continue;
        }
        for state in rollback.states {
            artifacts.insert(sinomo_module::native_module_cache_path(
                data_dir,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            ));
        }
    }
    Ok(artifacts)
}

fn validate_rollout_generation(generation: &str) -> Result<(), RolloutError> {
    if generation.is_empty()
        || !generation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("invalid native loader rollback generation `{generation}`"),
        });
    }
    Ok(())
}

pub fn stage_native_dylib_loader_state_rollback(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: &mut RolloutMainActivation,
) -> Result<String, RolloutError> {
    if let Some(generation) = activation.native_dylib_loader_rollback_generation.as_ref() {
        let rollback_path = rollout_native_dylib_loader_rollback_path(rollout_paths, generation)?;
        let rollback: NativeDylibLoaderStateRollback = read_object(&rollback_path)?;
        if rollback.generation != *generation
            || rollback.lifecycle != NativeDylibLoaderRollbackLifecycle::Unresolved
        {
            return Err(RolloutError::InvalidTransition {
                detail: format!("native loader pre-mutation intent {generation} is not unresolved"),
            });
        }
        return Ok(generation.clone());
    }
    let generation = crate::rollout_artifact_suffix();
    let mut states = sinomo_module::read_native_dylib_module_loader_states(data_dir)?;
    states.sort_by(|left, right| left.module_id.cmp(&right.module_id));
    for state in &states {
        let active_path = sinomo_module::native_module_cache_path(
            data_dir,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        if !active_path.is_file() {
            return Err(RolloutError::MissingArtifact {
                artifact: format!(
                    "active native module cache artifact {} for {}",
                    active_path.display(),
                    state.module_id
                ),
            });
        }
    }
    let rollback = NativeDylibLoaderStateRollback {
        generation: generation.clone(),
        captured_at: format_now(),
        lifecycle: NativeDylibLoaderRollbackLifecycle::Unresolved,
        states,
    };
    let rollback_path = rollout_native_dylib_loader_rollback_path(rollout_paths, &generation)?;
    if let Err(error) = write_rollout_object_durable(&rollback_path, &rollback) {
        let _ = fs::remove_file(&rollback_path);
        return Err(error);
    }
    activation.native_dylib_loader_rollback_generation = Some(generation.clone());
    Ok(generation)
}

pub fn restore_native_dylib_loader_state_rollback(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    generation: &str,
) -> Result<bool, RolloutError> {
    let rollback_path = rollout_native_dylib_loader_rollback_path(rollout_paths, generation)?;
    if !rollback_path.exists() {
        return Err(RolloutError::MissingArtifact {
            artifact: format!(
                "native loader rollback record {} for generation {generation}",
                rollback_path.display()
            ),
        });
    }
    let mut rollback: NativeDylibLoaderStateRollback = read_object(&rollback_path)?;
    if rollback.generation != generation {
        return Err(RolloutError::InvalidArtifact {
            detail: format!(
                "native loader rollback generation mismatch: expected {generation}, found {}",
                rollback.generation
            ),
        });
    }
    if rollback.lifecycle != NativeDylibLoaderRollbackLifecycle::Unresolved {
        return Ok(false);
    }

    for state in &rollback.states {
        let active_path = sinomo_module::native_module_cache_path(
            data_dir,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        if !active_path.is_file() {
            return Err(RolloutError::MissingArtifact {
                artifact: format!(
                    "retained native module cache artifact {} for rollback generation {generation}",
                    active_path.display()
                ),
            });
        }
    }

    sinomo_module::replace_native_dylib_module_loader_states_transactionally(
        data_dir,
        &rollback.states,
    )?;
    rollback.lifecycle = NativeDylibLoaderRollbackLifecycle::Restored;
    write_rollout_object_durable(&rollback_path, &rollback)?;
    Ok(true)
}

pub fn restore_native_dylib_loader_state_for_activation(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<bool, RolloutError> {
    let Some(generation) = activation
        .native_dylib_loader_rollback_generation
        .as_deref()
    else {
        return Ok(false);
    };
    restore_native_dylib_loader_state_rollback(data_dir, rollout_paths, generation)
}

/// Recovers the single orphaned pre-mutation intent left by a process crash.
///
/// Startup calls this while no native module registry has been loaded. Pending
/// main/shadow activation records own their intent and deliberately suppress
/// orphan recovery. More than one unresolved intent is ambiguous and fails
/// closed rather than guessing at rollback ordering.
pub fn recover_unresolved_native_dylib_loader_rollback_intents(
    data_dir: &Path,
    rollout_paths: &SelfRolloutPaths,
) -> Result<Vec<String>, RolloutError> {
    // Candidate main/shadow startup can occur while the rollout request still
    // owns the activation lock. Its durable marker makes the intent non-orphaned,
    // so avoid contending with that owner. Recheck after taking the lock in the
    // no-marker case to close the admission race.
    if rollout_main_activation_path(rollout_paths).exists()
        || rollout_shadow_finalize_path(rollout_paths).exists()
    {
        return Ok(Vec::new());
    }
    let _guard = acquire_rollout_activation_file_guard(rollout_paths)?;
    if rollout_main_activation_path(rollout_paths).exists()
        || rollout_shadow_finalize_path(rollout_paths).exists()
    {
        return Ok(Vec::new());
    }

    let unresolved = native_dylib_loader_rollback_records(rollout_paths)?
        .into_iter()
        .filter(|(_, rollback)| {
            rollback.lifecycle == NativeDylibLoaderRollbackLifecycle::Unresolved
        })
        .collect::<Vec<_>>();
    if unresolved.len() > 1 {
        return Err(RolloutError::InvalidTransition {
            detail: format!(
                "multiple unresolved native loader pre-mutation intents require operator recovery: {}",
                unresolved
                    .iter()
                    .map(|(path, rollback)| format!("{} ({})", rollback.generation, path.display()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }
    let Some((_, rollback)) = unresolved.into_iter().next() else {
        return Ok(Vec::new());
    };
    let generation = rollback.generation;
    restore_native_dylib_loader_state_rollback(data_dir, rollout_paths, &generation)?;
    Ok(vec![generation])
}

pub fn remove_native_dylib_loader_state_rollback(
    rollout_paths: &SelfRolloutPaths,
    generation: &str,
) -> Result<bool, RolloutError> {
    if !mark_native_dylib_loader_state_rollback_accepted(rollout_paths, generation)? {
        return Ok(false);
    }
    let rollback_path = rollout_native_dylib_loader_rollback_path(rollout_paths, generation)?;
    match fs::remove_file(&rollback_path) {
        Ok(()) => {
            sync_rollout_dir(&rollout_native_dylib_loader_rollback_dir(rollout_paths))?;
            Ok(true)
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(RolloutError::Io {
            path: rollback_path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn mark_native_dylib_loader_state_rollback_accepted(
    rollout_paths: &SelfRolloutPaths,
    generation: &str,
) -> Result<bool, RolloutError> {
    let rollback_path = rollout_native_dylib_loader_rollback_path(rollout_paths, generation)?;
    if !rollback_path.exists() {
        return Ok(false);
    }
    let mut rollback: NativeDylibLoaderStateRollback = read_object(&rollback_path)?;
    if rollback.generation != generation {
        return Err(RolloutError::InvalidArtifact {
            detail: format!(
                "native loader rollback generation mismatch: expected {generation}, found {}",
                rollback.generation
            ),
        });
    }
    // Persist acceptance before unlinking. If either write or fsync fails the
    // rollback remains fail-closed; if unlink or its directory fsync fails,
    // startup sees an accepted record and will never replay it.
    if rollback.lifecycle != NativeDylibLoaderRollbackLifecycle::Accepted {
        rollback.lifecycle = NativeDylibLoaderRollbackLifecycle::Accepted;
        write_rollout_object_durable(&rollback_path, &rollback)?;
    }
    Ok(true)
}

fn write_rollout_object_durable<T: Serialize>(path: &Path, value: &T) -> Result<(), RolloutError> {
    let bytes =
        serde_ext::to_bytes(value, ROLLOUT_OBJECT_VERSION, LengthMode::U32).map_err(|source| {
            RolloutError::Object {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        &bytes,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
            "rollout.sobj",
            "durable",
        )
        .sync_all(),
    )
    .map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

#[cfg(unix)]
fn sync_rollout_dir(path: &Path) -> Result<(), RolloutError> {
    if !path.exists() {
        return Ok(());
    }
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[cfg(not(unix))]
fn sync_rollout_dir(_path: &Path) -> Result<(), RolloutError> {
    Ok(())
}

pub fn rollout_main_exit_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("last-main-exit.sobj")
}

pub fn rollout_transition_history_dir(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("history")
}

pub fn rollout_runtime_state_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("runtime-state.sobj")
}

pub fn rollout_telemetry_advance_state_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("telemetry-advance-state.sobj")
}

pub fn rollout_telemetry_gate_config_path(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_data_dir_root(paths)
        .map(|root| {
            root.join("configs")
                .join("rollout")
                .join("telemetry")
                .join("gates.json")
        })
        .unwrap_or_else(|| rollout_telemetry_gate_legacy_config_path(paths))
}

pub fn rollout_telemetry_gate_legacy_config_path(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("telemetry-gates.sobj")
}

pub fn rollout_telemetry_gate_config_schema() -> sinomo_config::ConfigSchema {
    use sinomo_config::{ConfigNumberRange, ConfigSchema, ConfigSchemaField, ConfigSchemaValue};

    let positive_integer = || {
        Some(ConfigNumberRange {
            min: Some(1.0),
            max: None,
        })
    };

    ConfigSchema::object(
        "RolloutTelemetryGateConfig",
        "Telemetry thresholds that govern automatic rollout promotion.",
        vec![
            ConfigSchemaField::new(
                "automatic_promotion_enabled",
                "Automatic promotion",
                "Allow qualifying shadow or canary rollouts to advance automatically.",
                ConfigSchemaValue::bool(),
            )
            .optional()
            .with_default_value(serde_json::json!(default_automatic_promotion_enabled())),
            ConfigSchemaField::new(
                "event_limit",
                "Telemetry event limit",
                "Maximum recent telemetry events evaluated for one rollout decision.",
                ConfigSchemaValue::u64(positive_integer()),
            )
            .optional()
            .with_default_value(serde_json::json!(default_rollout_telemetry_event_limit())),
            ConfigSchemaField::new(
                "minimum_event_count",
                "Minimum event count",
                "Minimum observations required before automatic promotion is considered.",
                ConfigSchemaValue::u64(positive_integer()),
            )
            .optional()
            .with_default_value(serde_json::json!(
                default_rollout_telemetry_minimum_event_count()
            )),
            ConfigSchemaField::new(
                "maximum_automatic_weight_bps",
                "Maximum automatic weight",
                "Optional upper traffic weight, in basis points, that automation may assign.",
                ConfigSchemaValue::u64(Some(ConfigNumberRange {
                    min: Some(0.0),
                    max: Some(10_000.0),
                })),
            )
            .optional()
            .with_default_value(serde_json::json!(
                default_maximum_automatic_weight_bps().expect("rollout automatic weight default")
            )),
            ConfigSchemaField::new(
                "maximum_candidate_latency_p95_ms",
                "Maximum candidate p95 latency",
                "Optional maximum accepted candidate p95 latency in milliseconds.",
                ConfigSchemaValue::u64(None),
            )
            .optional(),
        ],
    )
}

pub fn rollout_telemetry_gate_config_definition() -> Result<ConfigDefinition, RolloutError> {
    ConfigDefinition::new(
        ROLLOUT_TELEMETRY_GATE_CONFIG_KEY,
        "sinomo-rollout",
        ConfigValueType::Object {
            schema_name: "RolloutTelemetryGateConfig".to_string(),
        },
        "Local rollout telemetry gates used for shadow/canary auto-promotion.",
        ConfigClusterMode::LocalOnly,
    )
    .map_err(map_rollout_config_error)?
    .with_default(&RolloutTelemetryGateConfig::default())
    .map_err(map_rollout_config_error)?
    .with_schema(rollout_telemetry_gate_config_schema())
    .map_err(map_rollout_config_error)?
    .with_module_id(crate::rollout_module_id().to_string())
    .map_err(map_rollout_config_error)
}

pub fn register_config_definitions(
    builder: ConfigRegistryBuilder,
) -> Result<ConfigRegistryBuilder, RolloutError> {
    builder
        .register(rollout_telemetry_gate_config_definition()?)
        .map_err(map_rollout_config_error)
}

fn rollout_config_store() -> Result<ConfigStore, RolloutError> {
    let registry = register_config_definitions(ConfigRegistry::builder())?.build();
    Ok(ConfigStore::new(registry))
}

fn rollout_config_data_dir(paths: &SelfRolloutPaths) -> Result<SinomoDataDir, RolloutError> {
    let root = rollout_data_dir_root(paths).ok_or_else(|| RolloutError::Io {
        path: paths.root.display().to_string(),
        detail: "rollout path does not have a data-dir parent".to_string(),
    })?;
    Ok(SinomoDataDir::new(root.to_path_buf()))
}

fn rollout_data_dir_root(paths: &SelfRolloutPaths) -> Option<&Path> {
    paths.root.parent()
}

fn map_rollout_config_error(source: ConfigError) -> RolloutError {
    RolloutError::Config {
        key: ROLLOUT_TELEMETRY_GATE_CONFIG_KEY.to_string(),
        detail: source.to_string(),
    }
}

pub fn rollout_staging_dir(paths: &SelfRolloutPaths) -> PathBuf {
    paths.root.join("staged")
}

pub fn rollout_staged_main_binary(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_staging_dir(paths).join(executable_name("sinomo"))
}

pub fn rollout_staged_shadow_binary(paths: &SelfRolloutPaths) -> PathBuf {
    rollout_staging_dir(paths).join(executable_name("sinomo-shadow"))
}

pub fn read_tail_lossy(path: &Path, max_bytes: usize) -> Result<Option<String>, RolloutError> {
    match fs::read(path) {
        Ok(bytes) => {
            let start = bytes.len().saturating_sub(max_bytes);
            Ok(Some(String::from_utf8_lossy(&bytes[start..]).to_string()))
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn rollout_diagnostics_report(
    rollout_paths: &SelfRolloutPaths,
) -> Result<String, RolloutError> {
    let mut output = String::new();
    output.push_str(&format!("rollout root: {}\n", rollout_paths.root.display()));
    append_rollout_status_diagnostic(&mut output, rollout_paths)?;
    append_rollout_diagnostic_file(&mut output, "build log", &rollout_paths.build_log)?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending main",
        &rollout_main_activation_path(rollout_paths),
    )?;
    append_rollout_diagnostic_file(
        &mut output,
        "pending shadow",
        &rollout_shadow_finalize_path(rollout_paths),
    )?;
    append_native_loader_rollback_diagnostics(&mut output, rollout_paths)?;
    append_rollout_diagnostic_file(
        &mut output,
        "runtime state",
        &rollout_runtime_state_path(rollout_paths),
    )?;
    append_rollout_main_exit_diagnostic(&mut output, rollout_paths)?;

    if let Ok(Some(status)) = read_rollout_status(rollout_paths) {
        if let Some(path) = status.diagnostics.as_deref() {
            append_rollout_diagnostic_file(&mut output, "failure diagnostics", Path::new(path))?;
        }
        if let Some(path) = status.stdout_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stdout", Path::new(path))?;
        }
        if let Some(path) = status.stderr_log.as_deref() {
            append_rollout_diagnostic_file(&mut output, "candidate stderr", Path::new(path))?;
        }
    }

    let logs_dir = rollout_paths.root.join("logs");
    if logs_dir.exists() {
        output.push_str("\n[rollout logs]\n");
        let mut entries = fs::read_dir(&logs_dir)
            .map_err(|source| RolloutError::Io {
                path: logs_dir.display().to_string(),
                detail: source.to_string(),
            })?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                Some((entry.path(), metadata.modified().ok(), metadata.len()))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, modified, _)| *modified);
        for (path, _, len) in entries.into_iter().rev().take(8) {
            output.push_str(&format!("{} ({} bytes)\n", path.display(), len));
        }
    }

    Ok(output)
}

fn append_native_loader_rollback_diagnostics(
    output: &mut String,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), RolloutError> {
    let dir = rollout_native_dylib_loader_rollback_dir(rollout_paths);
    output.push_str(&format!("\n[native loader rollbacks] {}\n", dir.display()));
    if !dir.exists() {
        output.push_str("<missing>\n");
        return Ok(());
    }
    let mut paths = fs::read_dir(&dir)
        .map_err(|source| RolloutError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("sobj"))
        .collect::<Vec<_>>();
    paths.sort();
    if paths.is_empty() {
        output.push_str("<empty>\n");
    } else {
        for path in paths {
            output.push_str(&format!("{}\n", path.display()));
        }
    }
    Ok(())
}

pub fn append_rollout_status_diagnostic(
    output: &mut String,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), RolloutError> {
    output.push_str(&format!("\n[state] {}\n", rollout_paths.state.display()));
    match read_rollout_status(rollout_paths)? {
        Some(status) => {
            output.push_str(&format!("status: {}\n", status.status));
            if let Some(mode) = status.mode {
                output.push_str(&format!("mode: {mode}\n"));
            }
            if let Some(detail) = status.detail {
                output.push_str(&format!("detail: {detail}\n"));
            }
            if let Some(phase) = status.phase {
                output.push_str(&format!("phase: {phase}\n"));
            }
            match (status.progress_current, status.progress_total) {
                (Some(current), Some(total)) => {
                    output.push_str(&format!("progress: {current}/{total}\n"));
                }
                (Some(current), None) => {
                    output.push_str(&format!("progress: {current}\n"));
                }
                _ => {}
            }
            if let Some(message) = status.progress_message {
                output.push_str(&format!("progress_message: {message}\n"));
            }
            if let Some(source) = status.source {
                output.push_str(&format!("source: {source}\n"));
            }
            if let Some(requested_at) = status.requested_at {
                output.push_str(&format!("requested_at: {requested_at}\n"));
            }
            if let Some(staged_at) = status.staged_at {
                output.push_str(&format!("staged_at: {staged_at}\n"));
            }
            if let Some(updated_at) = status.updated_at {
                output.push_str(&format!("updated_at: {updated_at}\n"));
            }
            if let Some(failed_at) = status.failed_at {
                output.push_str(&format!("failed_at: {failed_at}\n"));
            }
        }
        None => output.push_str("<missing>\n"),
    }
    Ok(())
}

pub fn append_rollout_diagnostic_file(
    output: &mut String,
    label: &str,
    path: &Path,
) -> Result<(), RolloutError> {
    output.push_str(&format!("\n[{label}] {}\n", path.display()));
    match read_tail_lossy(path, ROLLOUT_DIAGNOSTIC_TAIL_BYTES)? {
        Some(content) if !content.is_empty() => output.push_str(&content),
        Some(_) => output.push_str("<empty>\n"),
        None => output.push_str("<missing>\n"),
    }
    if !output.ends_with('\n') {
        output.push('\n');
    }
    Ok(())
}

pub fn append_rollout_main_exit_diagnostic(
    output: &mut String,
    rollout_paths: &SelfRolloutPaths,
) -> Result<(), RolloutError> {
    let path = rollout_main_exit_path(rollout_paths);
    output.push_str(&format!("\n[last main exit] {}\n", path.display()));
    match read_rollout_main_exit(rollout_paths)? {
        Some(record) => {
            output.push_str(&format!("kind: {:?}\n", record.kind));
            if let Some(exit_code) = record.exit_code {
                output.push_str(&format!("exit_code: {exit_code}\n"));
            }
            if let Some(signal) = record.signal {
                output.push_str(&format!("signal: {signal}\n"));
            }
            output.push_str(&format!(
                "requested_shutdown: {}\n",
                record.requested_shutdown
            ));
            output.push_str(&format!("observed_at: {}\n", record.observed_at));
        }
        None => output.push_str("<missing>\n"),
    }
    Ok(())
}

pub fn read_rollout_main_activation(
    paths: &SelfRolloutPaths,
) -> Result<Option<RolloutMainActivation>, RolloutError> {
    let path = rollout_main_activation_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn read_pending_shadow_rollout(
    paths: &SelfRolloutPaths,
) -> Result<Option<PendingShadowRollout>, RolloutError> {
    let path = rollout_shadow_finalize_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn stage_pending_shadow_rollout(
    rollout_paths: &SelfRolloutPaths,
    pending: &impl PendingShadowRolloutContext,
    shadow_host: &str,
    shadow_port: u16,
) -> Result<(), RolloutError> {
    let activation = pending.activation();
    let finalize = PendingShadowRollout {
        source: activation.source.clone(),
        main_binary: pending.main_binary().to_path_buf(),
        shadow_binary: activation.shadow_binary.clone(),
        staged_main_binary: activation.staged_main_binary.clone(),
        staged_shadow_binary: activation.staged_shadow_binary.clone(),
        previous_main_binary: pending.previous_main_binary().map(Path::to_path_buf),
        native_dylib_loader_rollback_generation: activation
            .native_dylib_loader_rollback_generation
            .clone(),
        previous_shadow_binary: pending.previous_shadow_binary().to_path_buf(),
        main_host: pending.main_host().to_string(),
        main_port: pending.main_port(),
        shadow_host: shadow_host.to_string(),
        shadow_port,
        requested_at: activation.requested_at.clone(),
        main_healthy_at: format_now(),
        mode: activation.mode.clone(),
    };
    write_rollout_object_durable(&rollout_shadow_finalize_path(rollout_paths), &finalize)
}

pub fn write_main_activation(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_rollout_object_durable(&rollout_main_activation_path(rollout_paths), activation)
}

pub fn record_main_activation_staged(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
) -> Result<(), RolloutError> {
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "main-staged".to_string(),
            mode: Some(activation.mode.clone()),
            detail: None,
            phase: Some("main-staged".to_string()),
            progress_current: Some(5),
            progress_total: Some(5),
            progress_message: Some("candidate main staged".to_string()),
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: Some(format_now()),
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )
}

pub fn record_activation_failure(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    detail: &str,
) -> Result<(), RolloutError> {
    let _ = previous_main_binary;
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "failed".to_string(),
            mode: Some(activation.mode.clone()),
            detail: Some(detail.to_string()),
            phase: Some("activation-failed".to_string()),
            progress_current: Some(5),
            progress_total: Some(5),
            progress_message: Some(detail.to_string()),
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: None,
            updated_at: None,
            failed_at: Some(format_now()),
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )
}

pub fn record_main_rollout_failure(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    previous_shadow_binary: &Path,
    detail: &str,
) -> Result<(), RolloutError> {
    record_main_rollout_failure_with_diagnostics(
        rollout_paths,
        activation,
        previous_main_binary,
        previous_shadow_binary,
        detail,
        None,
    )
}

pub fn record_main_rollout_failure_with_diagnostics(
    rollout_paths: &SelfRolloutPaths,
    activation: &RolloutMainActivation,
    previous_main_binary: &Path,
    previous_shadow_binary: &Path,
    detail: &str,
    diagnostics: Option<&RolloutFailureDiagnostics>,
) -> Result<(), RolloutError> {
    let _ = (previous_main_binary, previous_shadow_binary);
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: "failed".to_string(),
            mode: Some(activation.mode.clone()),
            detail: Some(detail.to_string()),
            phase: Some("activation-failed".to_string()),
            progress_current: Some(5),
            progress_total: Some(5),
            progress_message: Some(detail.to_string()),
            source: Some(activation.source.clone()),
            requested_at: Some(activation.requested_at.clone()),
            staged_at: None,
            updated_at: None,
            failed_at: Some(format_now()),
            diagnostics: diagnostics.map(|value| value.diagnostics_path.display().to_string()),
            stdout_log: diagnostics
                .and_then(|value| value.stdout_log.as_ref())
                .map(|path| path.display().to_string()),
            stderr_log: diagnostics
                .and_then(|value| value.stderr_log.as_ref())
                .map(|path| path.display().to_string()),
            stdout_tail: diagnostics.and_then(|value| value.stdout_tail.clone()),
            stderr_tail: diagnostics.and_then(|value| value.stderr_tail.clone()),
        },
    )
}

pub fn record_shadow_rollout_finalized(
    rollout_paths: &SelfRolloutPaths,
    finalize: &PendingShadowRollout,
    status: &str,
    detail: &str,
) -> Result<(), RolloutError> {
    write_rollout_status(
        rollout_paths,
        &ClusterNodeRolloutStatus {
            status: status.to_string(),
            mode: Some(finalize.mode.clone()),
            detail: Some(detail.to_string()),
            phase: Some("shadow-finalized".to_string()),
            progress_current: Some(5),
            progress_total: Some(5),
            progress_message: Some(detail.to_string()),
            source: Some(finalize.source.clone()),
            requested_at: Some(finalize.requested_at.clone()),
            staged_at: None,
            updated_at: Some(format_now()),
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        },
    )
}

pub fn read_rollout_status(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<ClusterNodeRolloutStatus>, RolloutError> {
    if !rollout_paths.state.exists() {
        return Ok(None);
    }
    match read_object(&rollout_paths.state) {
        Ok(status) => Ok(Some(status)),
        Err(RolloutError::Object { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn write_rollout_status(
    rollout_paths: &SelfRolloutPaths,
    status: &ClusterNodeRolloutStatus,
) -> Result<(), RolloutError> {
    write_object(&rollout_paths.state, status)
}

pub fn clear_rollout_status(rollout_paths: &SelfRolloutPaths) -> Result<bool, RolloutError> {
    match fs::remove_file(&rollout_paths.state) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RolloutError::Io {
            path: rollout_paths.state.display().to_string(),
            detail: error.to_string(),
        }),
    }
}

pub fn read_rollout_runtime_state(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<RolloutRuntimeState>, RolloutError> {
    let path = rollout_runtime_state_path(rollout_paths);
    if !path.exists() {
        return Ok(None);
    }
    match read_object(&path) {
        Ok(state) => Ok(Some(state)),
        Err(RolloutError::Object { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn write_rollout_runtime_state(
    rollout_paths: &SelfRolloutPaths,
    state: &RolloutRuntimeState,
) -> Result<(), RolloutError> {
    let path = rollout_runtime_state_path(rollout_paths);
    if read_rollout_runtime_state(rollout_paths)?.as_ref() == Some(state) {
        return Ok(());
    }
    write_object(&path, state)
}

pub fn read_rollout_telemetry_advance_state(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<RolloutTelemetryAdvanceState>, RolloutError> {
    let path = rollout_telemetry_advance_state_path(rollout_paths);
    if !path.exists() {
        return Ok(None);
    }
    match read_object(&path) {
        Ok(state) => Ok(Some(state)),
        Err(RolloutError::Object { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn write_rollout_telemetry_advance_state(
    rollout_paths: &SelfRolloutPaths,
    state: &RolloutTelemetryAdvanceState,
) -> Result<(), RolloutError> {
    write_object(&rollout_telemetry_advance_state_path(rollout_paths), state)
}

pub fn read_rollout_telemetry_gate_config(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<RolloutTelemetryGateConfig>, RolloutError> {
    let data_dir = rollout_config_data_dir(rollout_paths)?;
    let definition = rollout_telemetry_gate_config_definition()?;
    let store = rollout_config_store()?;
    if definition.local_path(&data_dir).exists() {
        return store
            .get_typed(&data_dir, None, ROLLOUT_TELEMETRY_GATE_CONFIG_KEY)
            .map(Some)
            .map_err(map_rollout_config_error);
    }
    let legacy_path = rollout_telemetry_gate_legacy_config_path(rollout_paths);
    match read_object(&legacy_path) {
        Ok(config) => {
            store
                .set_typed(
                    &data_dir,
                    None,
                    ROLLOUT_TELEMETRY_GATE_CONFIG_KEY,
                    &config,
                    ConfigWriteTarget::Local,
                )
                .map_err(map_rollout_config_error)?;
            Ok(Some(config))
        }
        Err(RolloutError::Object { .. }) => Ok(None),
        Err(RolloutError::Io { .. }) if !legacy_path.exists() => store
            .get_typed(&data_dir, None, ROLLOUT_TELEMETRY_GATE_CONFIG_KEY)
            .map(Some)
            .map_err(map_rollout_config_error),
        Err(error) => Err(error),
    }
}

pub fn write_rollout_telemetry_gate_config(
    rollout_paths: &SelfRolloutPaths,
    config: &RolloutTelemetryGateConfig,
) -> Result<(), RolloutError> {
    let data_dir = rollout_config_data_dir(rollout_paths)?;
    let path = rollout_telemetry_gate_config_path(rollout_paths);
    let store = rollout_config_store()?;
    if path.exists()
        && store
            .get_typed::<RolloutTelemetryGateConfig>(
                &data_dir,
                None,
                ROLLOUT_TELEMETRY_GATE_CONFIG_KEY,
            )
            .is_ok_and(|current| current == *config)
    {
        return Ok(());
    }
    store
        .set_typed(
            &data_dir,
            None,
            ROLLOUT_TELEMETRY_GATE_CONFIG_KEY,
            config,
            ConfigWriteTarget::Local,
        )
        .map_err(map_rollout_config_error)?;
    Ok(())
}

pub fn read_rollout_main_exit(
    rollout_paths: &SelfRolloutPaths,
) -> Result<Option<RolloutMainExitRecord>, RolloutError> {
    let path = rollout_main_exit_path(rollout_paths);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn record_rollout_main_exit(
    rollout_paths: &SelfRolloutPaths,
    observation: MainExitObservation,
    kind: MainExitKind,
) -> Result<(), RolloutError> {
    write_object(
        &rollout_main_exit_path(rollout_paths),
        &RolloutMainExitRecord {
            exit_code: observation.exit_code,
            signal: observation.signal,
            requested_shutdown: observation.requested_shutdown,
            kind,
            observed_at: format_now(),
        },
    )
}

pub fn record_rollout_transition(
    rollout_paths: &SelfRolloutPaths,
    node_id: &NodeId,
    decision: &RolloutDecision,
    triggering_evidence_refs: impl IntoIterator<Item = String>,
) -> Result<RecordedRolloutTransition, RolloutError> {
    let record = rollout_transition_record(node_id, decision, triggering_evidence_refs);
    let path = rollout_transition_record_path(rollout_paths, &record);
    write_json(&path, &record)?;
    Ok(RecordedRolloutTransition { record, path })
}

pub fn rollout_transition_record(
    node_id: &NodeId,
    decision: &RolloutDecision,
    triggering_evidence_refs: impl IntoIterator<Item = String>,
) -> RolloutTransitionRecord {
    let promoted_artifact_id = (decision.action == RolloutAction::Promote)
        .then(|| decision.state.plan.artifact_id.clone());
    let failed_artifact_id = (decision.action == RolloutAction::RollBack)
        .then(|| decision.state.plan.artifact_id.clone());
    let restored_artifact_id = (decision.action == RolloutAction::RollBack)
        .then(|| decision.state.loader_state.active_artifact_id.clone());
    RolloutTransitionRecord {
        record_id: Id::generate(),
        node_id: node_id.clone(),
        rollout_plan_id: decision.state.plan.rollout_plan_id.clone(),
        artifact_id: decision.state.plan.artifact_id.clone(),
        module_id: decision.state.plan.module_id.clone(),
        action: decision.action.clone(),
        stage: decision.state.stage.clone(),
        current_weight_bps: decision.state.current_weight_bps,
        active_artifact_id: decision.state.loader_state.active_artifact_id.clone(),
        previous_artifact_id: decision.state.loader_state.previous_artifact_id.clone(),
        candidate_artifact_id: decision.state.loader_state.candidate_artifact_id.clone(),
        shadow_artifact_id: decision.state.loader_state.shadow_artifact_id.clone(),
        hot_swap_state: decision.state.loader_state.hot_swap_state.clone(),
        promoted_artifact_id,
        failed_artifact_id,
        restored_artifact_id,
        triggering_evidence_refs: triggering_evidence_refs.into_iter().collect(),
        reason: decision.reason.clone(),
        recorded_at: format_now(),
    }
}

fn default_transition_hot_swap_state() -> HotSwapState {
    HotSwapState::Stable
}

fn default_automatic_promotion_enabled() -> bool {
    false
}

fn default_rollout_telemetry_event_limit() -> usize {
    1_000
}

fn default_rollout_telemetry_kind() -> String {
    "module.shadow.decision".to_string()
}

fn default_rollout_telemetry_minimum_event_count() -> u64 {
    1
}

fn default_maximum_automatic_weight_bps() -> Option<u16> {
    Some(500)
}

fn rollout_transition_record_path(
    rollout_paths: &SelfRolloutPaths,
    record: &RolloutTransitionRecord,
) -> PathBuf {
    rollout_transition_history_dir(rollout_paths).join(format!(
        "{}-{}-{}-{}.json",
        OffsetDateTime::now_utc().unix_timestamp_nanos(),
        record.rollout_plan_id,
        record.record_id,
        rollout_action_file_label(&record.action)
    ))
}

fn rollout_action_file_label(action: &RolloutAction) -> &'static str {
    match action {
        RolloutAction::StayShadow => "stay-shadow",
        RolloutAction::AdvanceCanary => "advance-canary",
        RolloutAction::Promote => "promote",
        RolloutAction::GuardR1Activation => "guard-r1-activation",
        RolloutAction::RollBack => "rollback",
        RolloutAction::Quarantine => "quarantine",
    }
}

pub fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), RolloutError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes =
        serde_ext::to_bytes(value, ROLLOUT_OBJECT_VERSION, LengthMode::U32).map_err(|source| {
            RolloutError::Object {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    fs::write(path, bytes).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn read_object<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, RolloutError> {
    let bytes = fs::read(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_ext::from_bytes(&bytes).map_err(|source| RolloutError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), RolloutError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let file = fs::File::create(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::to_writer_pretty(file, value).map_err(|source| RolloutError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn remove_main_activation(paths: &SelfRolloutPaths) {
    let _ = fs::remove_file(rollout_main_activation_path(paths));
}

pub fn remove_shadow_finalize(paths: &SelfRolloutPaths) {
    let _ = fs::remove_file(rollout_shadow_finalize_path(paths));
}

fn default_rollout_main_host() -> String {
    "127.0.0.1".to_string()
}

fn default_rollout_main_port() -> u16 {
    21_353
}

fn format_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
const ROLLOUT_OBJECT_VERSION: u16 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_fully_typed_schema(value: &sinomo_config::ConfigSchemaValue) {
        use sinomo_config::ConfigSchemaValue;

        match value {
            ConfigSchemaValue::Object { fields, .. } => {
                assert!(
                    !fields.is_empty(),
                    "object schemas must declare their fields"
                );
                for field in fields {
                    assert_fully_typed_schema(&field.value);
                }
            }
            ConfigSchemaValue::Map { value } | ConfigSchemaValue::List { value } => {
                assert_fully_typed_schema(value);
            }
            ConfigSchemaValue::Json => panic!("config schema must not use an untyped JSON value"),
            ConfigSchemaValue::Bool
            | ConfigSchemaValue::I64 { .. }
            | ConfigSchemaValue::U64 { .. }
            | ConfigSchemaValue::F64 { .. }
            | ConfigSchemaValue::String { .. }
            | ConfigSchemaValue::StringList => {}
        }
    }

    #[test]
    fn rollout_telemetry_gate_schema_is_recursive_and_validates_default() {
        let definition =
            rollout_telemetry_gate_config_definition().expect("rollout telemetry definition");
        let default = definition
            .default_value
            .as_ref()
            .expect("rollout telemetry default");
        definition
            .validate_value(default)
            .expect("rollout telemetry default must satisfy its schema");

        let schema = definition
            .schema
            .as_ref()
            .expect("rollout telemetry schema");
        assert_fully_typed_schema(&schema.value);
        let sinomo_config::ConfigSchemaValue::Object { fields, .. } = &schema.value else {
            panic!("rollout telemetry schema must be an object")
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "automatic_promotion_enabled",
                "event_limit",
                "minimum_event_count",
                "maximum_automatic_weight_bps",
                "maximum_candidate_latency_p95_ms",
            ]
        );

        let sparse = serde_json::json!({});
        definition
            .validate_value(&sparse)
            .expect("legacy omitted rollout defaults must satisfy the schema");
        let decoded: RolloutTelemetryGateConfig =
            serde_json::from_value(sparse).expect("decode legacy sparse rollout config");
        assert_eq!(decoded, RolloutTelemetryGateConfig::default());
    }

    #[test]
    fn read_tail_lossy_limits_bytes_and_handles_missing_file() {
        let root = unique_temp("sinomo-rollout-tail");
        let path = root.join("tail.log");
        fs::write(&path, "0123456789").expect("log");

        assert_eq!(
            read_tail_lossy(&path, 4).expect("tail"),
            Some("6789".to_string())
        );
        assert_eq!(
            read_tail_lossy(&root.join("missing.log"), 4).expect("missing"),
            None
        );
    }

    #[test]
    fn rollout_diagnostics_report_includes_state_and_tail_files() {
        let root = unique_temp("sinomo-rollout-diagnostics");
        let paths = SelfRolloutPaths::new(&root);
        fs::create_dir_all(&paths.root).expect("rollout root");
        fs::write(&paths.build_log, "build log\n").expect("build log");
        write_rollout_status(
            &paths,
            &ClusterNodeRolloutStatus {
                status: "main-staged".to_string(),
                mode: Some("cluster-remote-build".to_string()),
                detail: Some("ready".to_string()),
                phase: None,
                progress_current: None,
                progress_total: None,
                progress_message: None,
                source: Some("source-head".to_string()),
                requested_at: Some("2026-05-08T00:00:00Z".to_string()),
                staged_at: None,
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            },
        )
        .expect("status");

        let report = rollout_diagnostics_report(&paths).expect("diagnostics");

        assert!(report.contains("rollout root:"));
        assert!(report.contains("status: main-staged"));
        assert!(report.contains("mode: cluster-remote-build"));
        assert!(report.contains("[build log]"));
        assert!(report.contains("build log"));
        assert!(report.contains("[last main exit]"));
    }

    #[test]
    fn read_rollout_status_ignores_corrupt_state_object() {
        let root = unique_temp("sinomo-rollout-corrupt-state");
        let paths = SelfRolloutPaths::new(&root);
        fs::create_dir_all(&paths.root).expect("rollout root");
        fs::write(&paths.state, b"not-a-sinomo-object").expect("corrupt state");

        assert_eq!(read_rollout_status(&paths).expect("status"), None);

        let replacement = ClusterNodeRolloutStatus {
            status: "accepted".to_string(),
            mode: Some("cluster-remote-build".to_string()),
            detail: Some("recovered".to_string()),
            phase: None,
            progress_current: None,
            progress_total: None,
            progress_message: None,
            source: None,
            requested_at: None,
            staged_at: None,
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        };
        write_rollout_status(&paths, &replacement).expect("rewrite state");
        assert_eq!(
            read_rollout_status(&paths).expect("status"),
            Some(replacement)
        );
    }

    #[test]
    fn rollout_telemetry_gate_config_writes_shared_config_document() {
        let root = unique_temp("sinomo-rollout-telemetry-gates-config");
        let paths = SelfRolloutPaths::new(&root);
        let config = RolloutTelemetryGateConfig {
            automatic_promotion_enabled: false,
            event_limit: 250,
            minimum_event_count: 20,
            maximum_automatic_weight_bps: None,
            maximum_candidate_latency_p95_ms: Some(125),
        };

        write_rollout_telemetry_gate_config(&paths, &config).expect("write config");

        let path = root
            .join("configs")
            .join("rollout")
            .join("telemetry")
            .join("gates.json");
        assert_eq!(rollout_telemetry_gate_config_path(&paths), path);
        let document: sinomo_config::ConfigDocument =
            serde_json::from_str(&fs::read_to_string(&path).expect("config document"))
                .expect("config document");
        assert_eq!(document.key.as_str(), ROLLOUT_TELEMETRY_GATE_CONFIG_KEY);
        let stored: RolloutTelemetryGateConfig =
            serde_json::from_value(document.value).expect("stored config");
        assert_eq!(stored, config);
        assert_eq!(
            read_rollout_telemetry_gate_config(&paths).expect("read config"),
            Some(config)
        );
    }

    #[test]
    fn rollout_telemetry_gate_config_migrates_legacy_sobj() {
        let root = unique_temp("sinomo-rollout-telemetry-gates-migrate");
        let paths = SelfRolloutPaths::new(&root);
        let config = RolloutTelemetryGateConfig {
            automatic_promotion_enabled: false,
            event_limit: 125,
            minimum_event_count: 5,
            maximum_automatic_weight_bps: Some(2000),
            maximum_candidate_latency_p95_ms: Some(88),
        };
        write_object(&rollout_telemetry_gate_legacy_config_path(&paths), &config)
            .expect("legacy config");

        let stored = read_rollout_telemetry_gate_config(&paths)
            .expect("read config")
            .expect("config");

        assert_eq!(stored, config);
        let document: sinomo_config::ConfigDocument = serde_json::from_str(
            &fs::read_to_string(rollout_telemetry_gate_config_path(&paths))
                .expect("migrated config document"),
        )
        .expect("config document");
        assert_eq!(document.key.as_str(), ROLLOUT_TELEMETRY_GATE_CONFIG_KEY);
    }

    #[test]
    fn clear_rollout_status_removes_only_status_state() {
        let root = unique_temp("sinomo-rollout-clear-status");
        let paths = SelfRolloutPaths::new(&root);
        fs::create_dir_all(&paths.root).expect("rollout root");
        write_rollout_status(
            &paths,
            &ClusterNodeRolloutStatus {
                status: "main-staged".to_string(),
                mode: Some("cluster-remote-build".to_string()),
                detail: None,
                phase: Some("main-staged".to_string()),
                progress_current: Some(5),
                progress_total: Some(5),
                progress_message: None,
                source: None,
                requested_at: None,
                staged_at: None,
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            },
        )
        .expect("status");
        fs::write(rollout_shadow_finalize_path(&paths), b"pending shadow").expect("pending");

        assert!(clear_rollout_status(&paths).expect("clear existing"));
        assert_eq!(read_rollout_status(&paths).expect("status"), None);
        assert!(rollout_shadow_finalize_path(&paths).exists());
        assert!(!clear_rollout_status(&paths).expect("clear missing"));
    }

    #[test]
    fn rollout_activation_guard_rejects_concurrent_staging_without_waiting() {
        let root = unique_temp("sinomo-rollout-activation-guard-concurrent");
        let paths = SelfRolloutPaths::new(&root);
        let guard = acquire_rollout_activation_guard(&paths).expect("first guard");
        assert_eq!(guard.path(), rollout_activation_lock_path(&paths));
        let staged_main = rollout_staged_main_binary(&paths);
        fs::create_dir_all(staged_main.parent().expect("staged parent")).expect("staged parent");
        fs::write(&staged_main, b"first-candidate").expect("first staged candidate");

        let contender_paths = paths.clone();
        let contender_staged_main = staged_main.clone();
        let contender =
            std::thread::spawn(
                move || match acquire_rollout_activation_guard(&contender_paths) {
                    Ok(_guard) => {
                        fs::write(&contender_staged_main, b"second-candidate")
                            .expect("second staged candidate");
                        "unexpected admission".to_string()
                    }
                    Err(error) => error.to_string(),
                },
            );
        let detail = contender.join().expect("contender thread");
        assert!(detail.contains("another rollout activation or module mutation"));
        assert_eq!(
            fs::read(&staged_main).expect("staged candidate after rejection"),
            b"first-candidate",
            "a rejected contender must not overwrite fixed staged paths"
        );

        drop(guard);
        acquire_rollout_activation_guard(&paths).expect("guard is released on drop");
    }

    #[test]
    fn rollout_activation_guard_rejects_pending_main_and_shadow_admission() {
        let root = unique_temp("sinomo-rollout-activation-guard-pending");
        let paths = SelfRolloutPaths::new(&root);
        fs::create_dir_all(&paths.root).expect("rollout root");

        let pending_main = rollout_main_activation_path(&paths);
        fs::write(&pending_main, b"pending-main").expect("pending main marker");
        let main_error = acquire_rollout_activation_guard(&paths)
            .expect_err("pending main must block admission")
            .to_string();
        assert!(main_error.contains("pending main rollout"));
        fs::remove_file(&pending_main).expect("remove pending main");

        let pending_shadow = rollout_shadow_finalize_path(&paths);
        fs::write(&pending_shadow, b"pending-shadow").expect("pending shadow marker");
        let shadow_error = acquire_rollout_activation_guard(&paths)
            .expect_err("pending shadow must block admission")
            .to_string();
        assert!(shadow_error.contains("pending shadow rollout"));
        fs::remove_file(&pending_shadow).expect("remove pending shadow");

        acquire_rollout_activation_guard(&paths)
            .expect("admission succeeds after pending markers clear");
    }

    #[test]
    fn orphaned_native_loader_intent_before_mutation_is_recovered_idempotently() {
        let root = unique_temp("sinomo-rollout-native-loader-intent-before-mutation");
        let paths = SelfRolloutPaths::new(&root);
        let previous = test_native_loader_state("before-mutation", "test:previous");
        write_test_native_loader_state(&root, &previous);
        let mut activation = test_native_loader_activation(&root);

        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("write pre-mutation intent");

        let admission = acquire_rollout_activation_guard(&paths)
            .expect_err("unresolved intent must block a second mutation")
            .to_string();
        assert!(admission.contains("unresolved pre-mutation intent"));
        assert_eq!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("startup recovery"),
            vec![generation.clone()]
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &previous.module_id)
                .expect("read loader state"),
            Some(previous)
        );
        let rollback: NativeDylibLoaderStateRollback = read_object(
            &rollout_native_dylib_loader_rollback_path(&paths, &generation).expect("rollback path"),
        )
        .expect("rollback record");
        assert_eq!(
            rollback.lifecycle,
            NativeDylibLoaderRollbackLifecycle::Restored
        );
        assert!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("idempotent second recovery")
                .is_empty()
        );
        acquire_rollout_activation_guard(&paths)
            .expect("restored history must no longer block admission");
    }

    #[test]
    fn native_loader_intent_lifecycle_defaults_to_unresolved_for_legacy_records() {
        #[derive(Serialize)]
        struct LegacyNativeDylibLoaderStateRollback {
            generation: String,
            captured_at: String,
            states: Vec<sinomo_module::NativeDylibModuleLoaderState>,
        }

        let root = unique_temp("sinomo-rollout-native-loader-legacy-intent");
        let paths = SelfRolloutPaths::new(&root);
        let generation = "legacy-generation";
        let path =
            rollout_native_dylib_loader_rollback_path(&paths, generation).expect("rollback path");
        write_object(
            &path,
            &LegacyNativeDylibLoaderStateRollback {
                generation: generation.to_string(),
                captured_at: "2026-07-12T00:00:00Z".to_string(),
                states: Vec::new(),
            },
        )
        .expect("legacy rollback record");

        let decoded: NativeDylibLoaderStateRollback =
            read_object(&path).expect("decode legacy rollback record");
        assert_eq!(
            decoded.lifecycle,
            NativeDylibLoaderRollbackLifecycle::Unresolved
        );
    }

    #[test]
    fn orphaned_native_loader_intent_after_mutation_before_activation_marker_restores_snapshot() {
        let root = unique_temp("sinomo-rollout-native-loader-intent-after-mutation");
        let paths = SelfRolloutPaths::new(&root);
        let previous = test_native_loader_state("pre-crash", "test:previous");
        write_test_native_loader_state(&root, &previous);
        let mut activation = test_native_loader_activation(&root);
        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("write pre-mutation intent");

        let candidate = test_native_loader_state("candidate", "test:candidate");
        write_test_native_loader_state(&root, &candidate);
        assert!(!rollout_main_activation_path(&paths).exists());
        assert!(!rollout_shadow_finalize_path(&paths).exists());

        assert_eq!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("startup recovery"),
            vec![generation]
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &previous.module_id)
                .expect("read restored loader state"),
            Some(previous)
        );
    }

    #[test]
    fn startup_recovery_defers_intent_owned_by_pending_activation() {
        let root = unique_temp("sinomo-rollout-native-loader-pending-owner");
        let paths = SelfRolloutPaths::new(&root);
        let previous = test_native_loader_state("pending-previous", "test:previous");
        write_test_native_loader_state(&root, &previous);
        let mut activation = test_native_loader_activation(&root);
        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("pre-mutation intent");
        let candidate = test_native_loader_state("pending-candidate", "test:candidate");
        write_test_native_loader_state(&root, &candidate);
        write_main_activation(&paths, &activation).expect("pending activation marker");

        assert!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("pending activation owns intent")
                .is_empty()
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &candidate.module_id)
                .expect("candidate remains visible"),
            Some(candidate)
        );

        remove_main_activation(&paths);
        assert_eq!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("orphan recovery after marker removal"),
            vec![generation]
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &previous.module_id)
                .expect("previous state restored"),
            Some(previous)
        );
    }

    #[test]
    fn restored_native_loader_history_never_replays_over_newer_accepted_state() {
        let root = unique_temp("sinomo-rollout-native-loader-restored-history");
        let paths = SelfRolloutPaths::new(&root);
        let previous = test_native_loader_state("historical", "test:historical");
        write_test_native_loader_state(&root, &previous);
        let mut activation = test_native_loader_activation(&root);
        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("write pre-mutation intent");
        let failed_candidate = test_native_loader_state("failed", "test:failed");
        write_test_native_loader_state(&root, &failed_candidate);
        assert!(
            restore_native_dylib_loader_state_rollback(&root, &paths, &generation)
                .expect("explicit restore")
        );

        let newer_accepted = test_native_loader_state("accepted", "test:accepted");
        write_test_native_loader_state(&root, &newer_accepted);
        assert!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("startup recovery ignores restored history")
                .is_empty()
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &newer_accepted.module_id,)
                .expect("read accepted loader state"),
            Some(newer_accepted)
        );
    }

    #[test]
    fn accepted_native_loader_intent_surviving_unlink_is_never_replayed() {
        let root = unique_temp("sinomo-rollout-native-loader-accepted-intent");
        let paths = SelfRolloutPaths::new(&root);
        let previous = test_native_loader_state("accepted-previous", "test:previous");
        write_test_native_loader_state(&root, &previous);
        let mut activation = test_native_loader_activation(&root);
        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("write pre-mutation intent");
        let accepted = test_native_loader_state("accepted-current", "test:accepted");
        write_test_native_loader_state(&root, &accepted);

        assert!(
            mark_native_dylib_loader_state_rollback_accepted(&paths, &generation)
                .expect("durable accepted lifecycle")
        );
        assert!(
            rollout_native_dylib_loader_rollback_path(&paths, &generation)
                .expect("rollback path")
                .exists(),
            "test simulates a crash before accepted-record unlink"
        );
        assert!(
            recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
                .expect("startup ignores accepted record")
                .is_empty()
        );
        assert_eq!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &accepted.module_id)
                .expect("read accepted loader state"),
            Some(accepted)
        );
    }

    #[test]
    fn multiple_unresolved_native_loader_intents_fail_startup_recovery_closed() {
        let root = unique_temp("sinomo-rollout-native-loader-ambiguous-intents");
        let paths = SelfRolloutPaths::new(&root);
        write_test_native_loader_state(&root, &test_native_loader_state("first", "test:first"));
        let mut first = test_native_loader_activation(&root);
        stage_native_dylib_loader_state_rollback(&root, &paths, &mut first).expect("first intent");
        write_test_native_loader_state(&root, &test_native_loader_state("second", "test:second"));
        let mut second = test_native_loader_activation(&root);
        stage_native_dylib_loader_state_rollback(&root, &paths, &mut second)
            .expect("second intent");

        let error = recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
            .expect_err("ambiguous rollback chain must fail closed")
            .to_string();
        assert!(error.contains("multiple unresolved native loader pre-mutation intents"));
    }

    #[test]
    fn native_loader_rollback_survives_prune_and_restores_load_address() {
        let root = unique_temp("sinomo-rollout-native-loader-rollback");
        let paths = SelfRolloutPaths::new(&root);
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.network.iroh").expect("module id");
        let previous = sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: "blake3:previous".to_string(),
            cache_key: "network/iroh".to_string(),
            library_name: "libsinomo_network_iroh_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo-network-module-v1".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: "test:previous".to_string(),
        };
        let previous_cache = sinomo_module::native_module_cache_path(
            &root,
            &previous.cache_key,
            &previous.content_hash,
            &previous.library_name,
        );
        fs::create_dir_all(previous_cache.parent().expect("cache parent")).expect("cache dir");
        fs::write(&previous_cache, b"previous native module").expect("previous cache");
        sinomo_module::write_native_dylib_module_loader_state(&root, &previous)
            .expect("previous loader state");

        let mut activation = RolloutMainActivation {
            source: "test-source".to_string(),
            main_binary: root.join("bin/sinomo-main"),
            shadow_binary: root.join("bin/sinomo-shadow"),
            staged_main_binary: None,
            staged_shadow_binary: None,
            previous_main_binary: None,
            native_dylib_loader_rollback_generation: None,
            main_host: "127.0.0.1".to_string(),
            main_port: 21353,
            shadow_host: None,
            shadow_port: 21354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
            mode: "test".to_string(),
        };
        let generation = stage_native_dylib_loader_state_rollback(&root, &paths, &mut activation)
            .expect("stage rollback");

        let mut candidate = previous.clone();
        candidate.content_hash = "blake3:candidate".to_string();
        candidate.native_abi_version = 2;
        candidate.interface_version = "sinomo-network-module-v2".to_string();
        candidate.source = "test:candidate".to_string();
        let candidate_cache = sinomo_module::native_module_cache_path(
            &root,
            &candidate.cache_key,
            &candidate.content_hash,
            &candidate.library_name,
        );
        fs::create_dir_all(candidate_cache.parent().expect("candidate parent"))
            .expect("candidate dir");
        fs::write(&candidate_cache, b"candidate native module").expect("candidate cache");
        sinomo_module::write_native_dylib_module_loader_state(&root, &candidate)
            .expect("candidate loader state");

        sinomo_module::prune_inactive_native_dylib_module_cache(&root, 1)
            .expect("prune inactive cache");
        assert!(
            previous_cache.is_file(),
            "the rollback generation must remain the newest retained inactive cache variant"
        );

        assert!(
            restore_native_dylib_loader_state_for_activation(&root, &paths, &activation)
                .expect("restore loader state")
        );
        let restored = sinomo_module::read_native_dylib_module_loader_state(&root, &module_id)
            .expect("read restored state")
            .expect("restored state");
        assert_eq!(restored, previous);
        let restored_cache = sinomo_module::native_module_cache_path(
            &root,
            &restored.cache_key,
            &restored.content_hash,
            &restored.library_name,
        );
        assert_eq!(restored_cache, previous_cache);
        assert!(
            restored_cache.is_file(),
            "restored state must be load-addressable"
        );
        assert!(
            rollout_native_dylib_loader_rollback_path(&paths, &generation)
                .expect("rollback path")
                .is_file(),
            "failed generations remain available for diagnostics and retry"
        );
        assert!(
            remove_native_dylib_loader_state_rollback(&paths, &generation)
                .expect("remove accepted rollback")
        );
    }

    #[test]
    fn ambiguous_unresolved_loader_rollbacks_pin_every_generation_and_fail_recovery_closed() {
        let root = unique_temp("sinomo-rollout-native-loader-multi-generation");
        let paths = SelfRolloutPaths::new(&root);
        let module_id =
            sinomo_module::ModuleId::from_legacy_named("module.network.iroh").expect("module id");
        let state = |hash: &str, source: &str| sinomo_module::NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: format!("blake3:{hash}"),
            cache_key: "network/iroh".to_string(),
            library_name: "libsinomo_network_iroh_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 2,
            interface_version: "sinomo-network-module-v2".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: source.to_string(),
        };
        let write_artifact = |state: &sinomo_module::NativeDylibModuleLoaderState| {
            let path = sinomo_module::native_module_cache_path(
                &root,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            );
            fs::create_dir_all(path.parent().expect("cache parent")).expect("cache dir");
            fs::write(&path, state.content_hash.as_bytes()).expect("cache artifact");
            path
        };

        let first = state("first", "test:first");
        let first_path = write_artifact(&first);
        sinomo_module::write_native_dylib_module_loader_state(&root, &first)
            .expect("first loader state");
        let mut first_activation = RolloutMainActivation {
            source: "first-rollout".to_string(),
            main_binary: root.join("bin/sinomo-main"),
            shadow_binary: root.join("bin/sinomo-shadow"),
            staged_main_binary: None,
            staged_shadow_binary: None,
            previous_main_binary: None,
            native_dylib_loader_rollback_generation: None,
            main_host: "127.0.0.1".to_string(),
            main_port: 21353,
            shadow_host: None,
            shadow_port: 21354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
            mode: "test".to_string(),
        };
        let first_generation =
            stage_native_dylib_loader_state_rollback(&root, &paths, &mut first_activation)
                .expect("first rollback generation");

        let second = state("second", "test:second");
        let second_path = write_artifact(&second);
        sinomo_module::write_native_dylib_module_loader_state(&root, &second)
            .expect("second loader state");
        let mut second_activation = first_activation.clone();
        second_activation.source = "second-rollout".to_string();
        second_activation.native_dylib_loader_rollback_generation = None;
        let second_generation =
            stage_native_dylib_loader_state_rollback(&root, &paths, &mut second_activation)
                .expect("second rollback generation");
        assert_ne!(first_generation, second_generation);

        let candidate = state("candidate", "test:candidate");
        let candidate_path = write_artifact(&candidate);
        sinomo_module::write_native_dylib_module_loader_state(&root, &candidate)
            .expect("candidate loader state");
        let unpinned = state("unpinned", "test:unpinned");
        let unpinned_path = write_artifact(&unpinned);

        let pins = unresolved_native_dylib_loader_rollback_artifacts(&root, &paths)
            .expect("collect unresolved rollback pins");
        assert_eq!(
            pins,
            BTreeSet::from([first_path.clone(), second_path.clone()])
        );
        sinomo_module::prune_inactive_native_dylib_module_cache_with_pinned_artifacts(
            &root, 0, &pins,
        )
        .expect("pin-aware prune");
        assert!(
            first_path.is_file(),
            "first failed generation remains restorable"
        );
        assert!(
            second_path.is_file(),
            "second failed generation remains restorable"
        );
        assert!(
            candidate_path.is_file(),
            "active candidate remains retained"
        );
        assert!(
            !unpinned_path.exists(),
            "unreferenced inactive variant is pruned"
        );

        let error = recover_unresolved_native_dylib_loader_rollback_intents(&root, &paths)
            .expect_err("ambiguous rollback history must not be replayed")
            .to_string();
        assert!(error.contains(&first_generation));
        assert!(error.contains(&second_generation));
    }

    fn test_native_loader_state(
        hash: &str,
        source: &str,
    ) -> sinomo_module::NativeDylibModuleLoaderState {
        sinomo_module::NativeDylibModuleLoaderState {
            module_id: sinomo_module::ModuleId::from_legacy_named("module.network.test")
                .expect("module id"),
            artifact_id: None,
            content_hash: format!("blake3:{hash}"),
            cache_key: "network/test".to_string(),
            library_name: "libsinomo_network_test_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 2,
            interface_version: "sinomo-network-module-v2".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: source.to_string(),
        }
    }

    fn write_test_native_loader_state(
        root: &Path,
        state: &sinomo_module::NativeDylibModuleLoaderState,
    ) {
        let artifact = sinomo_module::native_module_cache_path(
            root,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        fs::create_dir_all(artifact.parent().expect("cache parent")).expect("cache directory");
        fs::write(&artifact, state.content_hash.as_bytes()).expect("cache artifact");
        sinomo_module::write_native_dylib_module_loader_state(root, state).expect("loader state");
    }

    fn test_native_loader_activation(root: &Path) -> RolloutMainActivation {
        RolloutMainActivation {
            source: "test-source".to_string(),
            main_binary: root.join("bin/sinomo-main"),
            shadow_binary: root.join("bin/sinomo-shadow"),
            staged_main_binary: None,
            staged_shadow_binary: None,
            previous_main_binary: None,
            native_dylib_loader_rollback_generation: None,
            main_host: "127.0.0.1".to_string(),
            main_port: 21353,
            shadow_host: None,
            shadow_port: 21354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
            mode: "test".to_string(),
        }
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }
}
