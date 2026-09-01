//! Reactive rollout telemetry reconciliation owned by the rollout module.

use crate::{
    AppliedRolloutTransition, RolloutError, RolloutTelemetryAdvanceStatus, SelfRolloutPaths,
};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROLLOUT_TELEMETRY_RETRY_INITIAL: Duration = Duration::from_millis(250);
const ROLLOUT_TELEMETRY_RETRY_MAX: Duration = Duration::from_secs(30);
const ROLLOUT_TELEMETRY_INCOMPLETE_TAIL_RETRY: Duration = Duration::from_millis(10);
const ROLLOUT_TELEMETRY_EVENT_READ_BUFFER_BYTES: usize = 64 * 1024;
const ROLLOUT_TELEMETRY_MAX_EVENT_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// Result of one host-owned telemetry advancement transaction.
#[derive(Debug)]
pub struct RolloutTelemetryReconcileOutcome {
    pub status: RolloutTelemetryAdvanceStatus,
    pub applied_transition: Option<AppliedRolloutTransition>,
}

/// Host operations required by the rollout-owned telemetry reactor.
///
/// Implementations bridge rollout policy to the process's event store and
/// cluster-status publisher. The reactor owns source observation, retry, and
/// shutdown behavior; hosts own one synchronous advancement transaction.
pub trait RolloutTelemetryHost: Send + Sync + 'static {
    fn reconcile_rollout_telemetry(&self) -> Result<RolloutTelemetryReconcileOutcome, String>;
}

pub(crate) struct ComponentRolloutTelemetryHost {
    data_dir: PathBuf,
    cluster: Option<Arc<dyn sinomo_core::service::ClusterAccess>>,
    runtime: tokio::runtime::Handle,
}

impl ComponentRolloutTelemetryHost {
    pub(crate) fn new(
        data_dir: PathBuf,
        cluster: Option<Arc<dyn sinomo_core::service::ClusterAccess>>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            data_dir,
            cluster,
            runtime,
        }
    }

    fn reconcile(&self) -> Result<RolloutTelemetryReconcileOutcome, String> {
        let rollout_paths = SelfRolloutPaths::new(&self.data_dir);
        let event_store = sinomo_core::events::JsonlEventStore::new(
            self.data_dir
                .join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
        );
        let node_id = component_rollout_node_id(&self.data_dir)?;
        let controller = crate::RolloutController::new(node_id);
        let gate_config = crate::read_rollout_telemetry_gate_config(&rollout_paths)
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        let result = controller
            .advance_rollout_from_live_telemetry(
                &self.data_dir,
                &rollout_paths,
                &event_store,
                crate::RolloutTelemetryAdvanceOptions::from(gate_config),
            )
            .map_err(|error| error.to_string())?;
        let status = result.status;
        let applied_transition = if status == RolloutTelemetryAdvanceStatus::AppliedTransition {
            result.applied_transition
        } else {
            None
        };
        self.publish_module_status(&rollout_paths)?;
        Ok(RolloutTelemetryReconcileOutcome {
            status,
            applied_transition,
        })
    }

    fn publish_module_status(&self, rollout_paths: &SelfRolloutPaths) -> Result<(), String> {
        let Some(cluster) = self.cluster.as_ref() else {
            return Ok(());
        };
        let data_dir = sinomo_core::local::SinomoDataDir::new(&self.data_dir);
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let Ok(cluster_state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        else {
            return Ok(());
        };
        let Some(runtime_state) =
            crate::read_rollout_runtime_state(rollout_paths).map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        let telemetry_state = crate::read_rollout_telemetry_advance_state(rollout_paths)
            .map_err(|error| error.to_string())?
            .filter(|telemetry| {
                telemetry.rollout_plan_id == runtime_state.plan.rollout_plan_id
                    && telemetry.module_id == runtime_state.plan.module_id
            });
        let record = sinomo_core::ClusterModuleRolloutStatusRecord {
            cluster_id: cluster_state.cluster_id,
            node_id: cluster_state.local_node_id,
            rollout_plan_id: runtime_state.plan.rollout_plan_id,
            module_id: runtime_state.plan.module_id,
            artifact_id: runtime_state.plan.artifact_id,
            stage: rollout_stage_label(&runtime_state.stage).to_string(),
            current_weight_bps: runtime_state.current_weight_bps,
            active_artifact_id: runtime_state.loader_state.active_artifact_id,
            previous_artifact_id: runtime_state.loader_state.previous_artifact_id,
            candidate_artifact_id: runtime_state.loader_state.candidate_artifact_id,
            shadow_artifact_id: runtime_state.loader_state.shadow_artifact_id,
            hot_swap_state: hot_swap_state_label(&runtime_state.loader_state.hot_swap_state)
                .to_string(),
            telemetry_kind: telemetry_state
                .as_ref()
                .map(|telemetry| telemetry.telemetry_kind.clone()),
            telemetry_last_consumed_sequence: telemetry_state
                .as_ref()
                .and_then(|telemetry| telemetry.last_consumed_sequence)
                .map(|sequence| sequence.0),
            last_transition_record_id: telemetry_state
                .and_then(|telemetry| telemetry.last_transition_record_id),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        let op = crate::build_cluster_module_rollout_status_put_op(&record)
            .map_err(|error| error.to_string())?;
        self.runtime
            .block_on(
                cluster.kv_commit_async(sinomo_core::kv::KvTxnRequest::generated(
                    sinomo_core::kv::KvTxn {
                        compares: Vec::new(),
                        success: vec![op],
                        failure: Vec::new(),
                    },
                )),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl RolloutTelemetryHost for ComponentRolloutTelemetryHost {
    fn reconcile_rollout_telemetry(&self) -> Result<RolloutTelemetryReconcileOutcome, String> {
        self.reconcile()
    }
}

fn component_rollout_node_id(data_dir: &Path) -> Result<sinomo_core::node::NodeId, String> {
    let data_dir_handle = sinomo_core::local::SinomoDataDir::new(data_dir);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir_handle);
    if let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) {
        return Ok(state.local_node_id);
    }
    let identity = sinomo_core::node::load_or_create_main_node_identity(data_dir)
        .map_err(|error| error.to_string())?;
    Ok(sinomo_core::node::NodeId::derived(
        "sinomo.rollout.local-node",
        identity.public_key.as_bytes(),
    ))
}

fn rollout_stage_label(stage: &crate::RolloutStage) -> &'static str {
    match stage {
        crate::RolloutStage::Shadow => "shadow",
        crate::RolloutStage::Canary => "canary",
        crate::RolloutStage::GuardedR1 => "guarded-r1",
        crate::RolloutStage::Promoted => "promoted",
        crate::RolloutStage::RolledBack => "rolled-back",
        crate::RolloutStage::Quarantined => "quarantined",
    }
}

fn hot_swap_state_label(state: &sinomo_core::HotSwapState) -> &'static str {
    match state {
        sinomo_core::HotSwapState::Stable => "stable",
        sinomo_core::HotSwapState::Loading => "loading",
        sinomo_core::HotSwapState::Shadowing => "shadowing",
        sinomo_core::HotSwapState::Draining => "draining",
        sinomo_core::HotSwapState::ActivationCheckpoint => "activation-checkpoint",
        sinomo_core::HotSwapState::ActiveCandidate => "active-candidate",
        sinomo_core::HotSwapState::RollbackCheckpoint => "rollback-checkpoint",
        sinomo_core::HotSwapState::RollingBack => "rolling-back",
        sinomo_core::HotSwapState::Quarantined => "quarantined",
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RolloutTelemetryEventTailOutcome {
    relevant_event_observed: bool,
    /// Absolute end offset of the observed incomplete record. This is source
    /// progress, unlike filesystem watcher generations: one append may emit
    /// several indistinguishable backend notifications.
    incomplete_record_end_offset: Option<u64>,
}

#[derive(Debug)]
struct RolloutTelemetryEventLogTailer {
    path: PathBuf,
    offset: u64,
}

#[derive(Clone, Debug)]
struct RolloutTelemetrySourcePaths {
    event_log: PathBuf,
    runtime_state: PathBuf,
    gate_config: PathBuf,
}

fn rollout_telemetry_source_watcher(
    source_paths: &RolloutTelemetrySourcePaths,
    runtime_source_tx: tokio::sync::watch::Sender<u64>,
    gate_source_tx: tokio::sync::watch::Sender<u64>,
    event_file_source_tx: tokio::sync::watch::Sender<u64>,
    watcher_fault_tx: tokio::sync::watch::Sender<u64>,
) -> Result<notify::RecommendedWatcher, RolloutError> {
    let callback_runtime_path = source_paths.runtime_state.clone();
    let callback_gate_path = source_paths.gate_config.clone();
    let callback_event_path = source_paths.event_log.clone();
    let watcher_error_path = source_paths.event_log.clone();
    let mut source_watcher =
        notify::recommended_watcher(move |result: Result<notify::Event, notify::Error>| {
            match result {
                Ok(event) => {
                    let paths = event.paths;
                    if paths.iter().any(|path| path == &callback_runtime_path) {
                        runtime_source_tx.send_modify(|generation| {
                            *generation = generation.saturating_add(1);
                        });
                    }
                    if paths.iter().any(|path| path == &callback_gate_path) {
                        gate_source_tx.send_modify(|generation| {
                            *generation = generation.saturating_add(1);
                        });
                    }
                    if paths.iter().any(|path| path == &callback_event_path) {
                        event_file_source_tx.send_modify(|generation| {
                            *generation = generation.saturating_add(1);
                        });
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: "sinomo.rollout.telemetry",
                        path = %watcher_error_path.display(),
                        "rollout telemetry source watcher failed: {error}"
                    );
                    // A backend watcher error means the current observation
                    // boundary can no longer be trusted. The reactor owns the
                    // fault as a generation so it can rebuild the watcher and
                    // reconcile every durable source after the new watches are
                    // installed.
                    watcher_fault_tx.send_modify(|generation| {
                        *generation = generation.saturating_add(1);
                    });
                }
            }
        })
        .map_err(|source| RolloutError::Io {
            path: source_paths.event_log.display().to_string(),
            detail: format!("create rollout telemetry source watcher: {source}"),
        })?;
    let mut watched_directories = BTreeSet::new();
    for directory in [
        source_paths.event_log.parent(),
        source_paths.runtime_state.parent(),
        source_paths.gate_config.parent(),
    ]
    .into_iter()
    .flatten()
    {
        if watched_directories.insert(directory.to_path_buf()) {
            notify::Watcher::watch(
                &mut source_watcher,
                directory,
                notify::RecursiveMode::NonRecursive,
            )
            .map_err(|source| RolloutError::Io {
                path: directory.display().to_string(),
                detail: format!("watch rollout telemetry source directory: {source}"),
            })?;
        }
    }
    Ok(source_watcher)
}

impl RolloutTelemetryEventLogTailer {
    fn at_end(path: PathBuf) -> Result<Self, RolloutError> {
        let offset = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
            Err(source) => {
                return Err(RolloutError::Io {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        };
        Ok(Self { path, offset })
    }

    fn catch_up(&mut self) -> Result<RolloutTelemetryEventTailOutcome, RolloutError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                self.offset = 0;
                return Ok(RolloutTelemetryEventTailOutcome::default());
            }
            Err(source) => {
                return Err(RolloutError::Io {
                    path: self.path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        };
        let len = file
            .metadata()
            .map_err(|source| RolloutError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?
            .len();
        if len < self.offset {
            self.offset = 0;
        }
        file.seek(SeekFrom::Start(self.offset))
            .map_err(|source| RolloutError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut buffer = [0_u8; ROLLOUT_TELEMETRY_EVENT_READ_BUFFER_BYTES];
        let mut line = Vec::new();
        let mut committed_bytes = 0_u64;
        let mut pending_record_bytes = 0_u64;
        let mut relevant_event_observed = false;
        loop {
            let read = file.read(&mut buffer).map_err(|source| RolloutError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
            if read == 0 {
                break;
            }
            let mut cursor = 0_usize;
            while cursor < read {
                let remaining = &buffer[cursor..read];
                let relative_newline = remaining.iter().position(|byte| *byte == b'\n');
                let segment_len = relative_newline.unwrap_or(remaining.len());
                if line.len().saturating_add(segment_len) > ROLLOUT_TELEMETRY_MAX_EVENT_RECORD_BYTES
                {
                    return Err(RolloutError::Json {
                        path: self.path.display().to_string(),
                        detail: format!(
                            "committed rollout telemetry event at offset {} exceeds {} bytes",
                            self.offset.saturating_add(committed_bytes),
                            ROLLOUT_TELEMETRY_MAX_EVENT_RECORD_BYTES
                        ),
                    });
                }
                line.extend_from_slice(&remaining[..segment_len]);
                pending_record_bytes = pending_record_bytes
                    .saturating_add(u64::try_from(segment_len).unwrap_or(u64::MAX));
                cursor = cursor.saturating_add(segment_len);
                let Some(_) = relative_newline else {
                    continue;
                };

                cursor = cursor.saturating_add(1);
                pending_record_bytes = pending_record_bytes.saturating_add(1);
                let record = line.strip_suffix(b"\r").unwrap_or(&line);
                if !record.iter().all(u8::is_ascii_whitespace) {
                    let stored: sinomo_core::events::StoredEvent =
                        serde_json::from_slice(record).map_err(|source| RolloutError::Json {
                            path: self.path.display().to_string(),
                            detail: format!(
                                "decode committed record at offset {}: {source}",
                                self.offset.saturating_add(committed_bytes)
                            ),
                        })?;
                    relevant_event_observed |=
                        crate::rollout_telemetry_event_kind_is_relevant(&stored.event.kind);
                }
                committed_bytes = committed_bytes.saturating_add(pending_record_bytes);
                pending_record_bytes = 0;
                line.clear();
            }
        }
        self.offset = self.offset.saturating_add(committed_bytes);
        Ok(RolloutTelemetryEventTailOutcome {
            relevant_event_observed,
            incomplete_record_end_offset: (pending_record_bytes != 0)
                .then(|| self.offset.saturating_add(pending_record_bytes)),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RolloutTelemetrySourceGenerations {
    runtime_state: u64,
    gate_config: u64,
    relevant_event: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutTelemetryArming {
    /// No event or gate can create a runtime state/candidate or leave a
    /// terminal stage. Only the runtime-state owner can re-arm the reactor.
    RuntimeStateOnly,
    /// A live candidate can change from telemetry, gates, or runtime state.
    LiveCandidateSources,
}

/// Reactive owner for local rollout telemetry advancement.
///
/// The initial generation reconciles durable sources left by a previous
/// process. Afterwards exact runtime/config commits and relevant event-log
/// appends are the only healthy wake sources. Timers exist only while an
/// attempted generation is failing or for the single bounded retry attached
/// to an observed partial JSONL append.
pub struct RolloutTelemetryReactor {
    runtime_source_tx: tokio::sync::watch::Sender<u64>,
    runtime_source_rx: tokio::sync::watch::Receiver<u64>,
    gate_source_tx: tokio::sync::watch::Sender<u64>,
    gate_source_rx: tokio::sync::watch::Receiver<u64>,
    event_source_tx: tokio::sync::watch::Sender<u64>,
    event_source_rx: tokio::sync::watch::Receiver<u64>,
    event_file_source_tx: tokio::sync::watch::Sender<u64>,
    event_file_source_rx: tokio::sync::watch::Receiver<u64>,
    watcher_fault_tx: tokio::sync::watch::Sender<u64>,
    watcher_fault_rx: tokio::sync::watch::Receiver<u64>,
    source_paths: RolloutTelemetrySourcePaths,
    source_watcher: notify::RecommendedWatcher,
    event_log_tailer: RolloutTelemetryEventLogTailer,
    completed_watcher_fault_generation: u64,
    completed_event_file_generation: u64,
    event_tail_retry_at: Option<Instant>,
    event_tail_retry_end_offset: Option<u64>,
    completed_generations: RolloutTelemetrySourceGenerations,
    arming: RolloutTelemetryArming,
    consecutive_failures: u32,
    retry_at: Option<Instant>,
}

impl std::fmt::Debug for RolloutTelemetryReactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RolloutTelemetryReactor")
            .field(
                "runtime_source_generation",
                &*self.runtime_source_rx.borrow(),
            )
            .field("gate_source_generation", &*self.gate_source_rx.borrow())
            .field("event_source_generation", &*self.event_source_rx.borrow())
            .field(
                "event_file_source_generation",
                &*self.event_file_source_rx.borrow(),
            )
            .field("watcher_fault_generation", &*self.watcher_fault_rx.borrow())
            .field("completed_generations", &self.completed_generations)
            .field("arming", &self.arming)
            .field("consecutive_failures", &self.consecutive_failures)
            .field("retry_at", &self.retry_at)
            .finish_non_exhaustive()
    }
}

impl RolloutTelemetryReactor {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, RolloutError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let rollout_paths = SelfRolloutPaths::new(&data_dir);
        let source_paths = RolloutTelemetrySourcePaths {
            event_log: data_dir.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
            runtime_state: crate::rollout_runtime_state_path(&rollout_paths),
            gate_config: crate::rollout_telemetry_gate_config_path(&rollout_paths),
        };
        for parent in [
            source_paths.event_log.parent(),
            source_paths.runtime_state.parent(),
            source_paths.gate_config.parent(),
        ]
        .into_iter()
        .flatten()
        {
            fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        // Generation one is the source-owned startup reconciliation for
        // durable writes that occurred before this process started watching.
        let (runtime_source_tx, runtime_source_rx) = tokio::sync::watch::channel(1_u64);
        let (gate_source_tx, gate_source_rx) = tokio::sync::watch::channel(0_u64);
        let (event_source_tx, event_source_rx) = tokio::sync::watch::channel(0_u64);
        let (event_file_source_tx, event_file_source_rx) = tokio::sync::watch::channel(0_u64);
        let (watcher_fault_tx, watcher_fault_rx) = tokio::sync::watch::channel(0_u64);
        let source_watcher = rollout_telemetry_source_watcher(
            &source_paths,
            runtime_source_tx.clone(),
            gate_source_tx.clone(),
            event_file_source_tx.clone(),
            watcher_fault_tx.clone(),
        )?;
        // Establish the OS watches first. The dirty startup generation owns
        // the durable catch-up; later mutations cannot fall between sampling
        // and watcher registration.
        let event_log_tailer =
            RolloutTelemetryEventLogTailer::at_end(source_paths.event_log.clone())?;
        Ok(Self {
            runtime_source_tx,
            runtime_source_rx,
            gate_source_tx,
            gate_source_rx,
            event_source_tx,
            event_source_rx,
            event_file_source_tx,
            event_file_source_rx,
            watcher_fault_tx,
            watcher_fault_rx,
            source_paths,
            source_watcher,
            event_log_tailer,
            completed_watcher_fault_generation: 0,
            completed_event_file_generation: 0,
            event_tail_retry_at: None,
            event_tail_retry_end_offset: None,
            completed_generations: RolloutTelemetrySourceGenerations::default(),
            arming: RolloutTelemetryArming::RuntimeStateOnly,
            consecutive_failures: 0,
            retry_at: None,
        })
    }

    fn rebuild_source_watcher(&mut self) -> Result<(), RolloutError> {
        // Install the replacement before dropping the old watcher. Once the
        // exact-path watches are live, mark every durable source dirty so the
        // reconciliation closes the observation gap that caused the fault.
        let replacement = rollout_telemetry_source_watcher(
            &self.source_paths,
            self.runtime_source_tx.clone(),
            self.gate_source_tx.clone(),
            self.event_file_source_tx.clone(),
            self.watcher_fault_tx.clone(),
        )?;
        self.source_watcher = replacement;
        self.runtime_source_tx.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        self.gate_source_tx.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        self.event_file_source_tx.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        Ok(())
    }

    fn prepare_runnable_generations(
        &mut self,
        now: Instant,
    ) -> Result<Option<RolloutTelemetrySourceGenerations>, RolloutError> {
        if self.retry_at.is_some_and(|retry_at| now < retry_at) {
            return Ok(None);
        }

        let watcher_fault_generation = *self.watcher_fault_rx.borrow_and_update();
        if watcher_fault_generation > self.completed_watcher_fault_generation {
            self.rebuild_source_watcher()?;
            self.completed_watcher_fault_generation = watcher_fault_generation;
        }

        let runtime_state_generation = *self.runtime_source_rx.borrow_and_update();
        let runtime_state_pending =
            runtime_state_generation > self.completed_generations.runtime_state;
        let event_file_generation = *self.event_file_source_rx.borrow_and_update();
        let event_tail_retry_due = self
            .event_tail_retry_at
            .is_some_and(|retry_at| now >= retry_at);
        let event_file_pending = event_file_generation > self.completed_event_file_generation;
        if runtime_state_pending
            || (self.arming == RolloutTelemetryArming::LiveCandidateSources
                && (event_file_pending || event_tail_retry_due))
        {
            let tail_outcome = self.event_log_tailer.catch_up()?;
            self.completed_event_file_generation = self
                .completed_event_file_generation
                .max(event_file_generation);
            if tail_outcome.relevant_event_observed {
                self.event_source_tx.send_modify(|generation| {
                    *generation = generation.saturating_add(1);
                });
            }
            if let Some(incomplete_record_end_offset) = tail_outcome.incomplete_record_end_offset {
                // One short retry is attached to observed source progress, not
                // a watcher generation. Backends may emit duplicate events for
                // one append; the same incomplete length can neither extend a
                // pending retry nor re-arm a consumed retry. Newly appended
                // bytes produce a new end offset and may own one new retry.
                if self.event_tail_retry_end_offset == Some(incomplete_record_end_offset) {
                    if event_tail_retry_due {
                        self.event_tail_retry_at = None;
                    }
                } else {
                    self.event_tail_retry_at =
                        now.checked_add(ROLLOUT_TELEMETRY_INCOMPLETE_TAIL_RETRY);
                    self.event_tail_retry_end_offset = Some(incomplete_record_end_offset);
                }
            } else {
                self.event_tail_retry_at = None;
                self.event_tail_retry_end_offset = None;
            }
        }

        let source_generations = RolloutTelemetrySourceGenerations {
            runtime_state: runtime_state_generation,
            gate_config: *self.gate_source_rx.borrow_and_update(),
            relevant_event: *self.event_source_rx.borrow_and_update(),
        };
        let runtime_state_pending =
            source_generations.runtime_state > self.completed_generations.runtime_state;
        let live_candidate_source_pending = self.arming
            == RolloutTelemetryArming::LiveCandidateSources
            && (source_generations.gate_config > self.completed_generations.gate_config
                || source_generations.relevant_event > self.completed_generations.relevant_event);
        if !runtime_state_pending && !live_candidate_source_pending {
            return Ok(None);
        }
        Ok(Some(source_generations))
    }

    fn complete_generations(
        &mut self,
        generations: RolloutTelemetrySourceGenerations,
        status: &crate::RolloutTelemetryAdvanceStatus,
    ) {
        self.completed_generations.runtime_state = self
            .completed_generations
            .runtime_state
            .max(generations.runtime_state);
        self.completed_generations.gate_config = self
            .completed_generations
            .gate_config
            .max(generations.gate_config);
        self.completed_generations.relevant_event = self
            .completed_generations
            .relevant_event
            .max(generations.relevant_event);
        self.arming = rollout_telemetry_arming_after(status);
        if self.arming == RolloutTelemetryArming::RuntimeStateOnly {
            self.event_tail_retry_at = None;
            self.event_tail_retry_end_offset = None;
        }
        self.consecutive_failures = 0;
        self.retry_at = None;
    }

    fn fail_generation(&mut self, now: Instant) {
        let retry_delay = rollout_telemetry_retry_delay(self.consecutive_failures);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.retry_at = now.checked_add(retry_delay).or(Some(now));
    }

    #[cfg(test)]
    fn reconcile_pending_with<T, E>(
        &mut self,
        now: Instant,
        reconcile: impl FnOnce() -> Result<(T, crate::RolloutTelemetryAdvanceStatus), E>,
    ) -> Result<Option<T>, E> {
        let Some(generations) = self
            .prepare_runnable_generations(now)
            .expect("prepare rollout telemetry sources")
        else {
            return Ok(None);
        };
        match reconcile() {
            Ok((value, status)) => {
                self.complete_generations(generations, &status);
                Ok(Some(value))
            }
            Err(error) => {
                self.fail_generation(now);
                Err(error)
            }
        }
    }

    async fn wait_for_source_retry_or_shutdown(
        &mut self,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        if let Some(retry_at) = self.retry_at {
            return tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(retry_at)) => false,
                changed = shutdown_rx.changed() => {
                    changed.is_err() || *shutdown_rx.borrow()
                }
            };
        }
        match self.arming {
            RolloutTelemetryArming::RuntimeStateOnly => {
                tokio::select! {
                    changed = self.runtime_source_rx.changed() => changed.is_err(),
                    changed = self.watcher_fault_rx.changed() => changed.is_err(),
                    changed = shutdown_rx.changed() => {
                        changed.is_err() || *shutdown_rx.borrow()
                    }
                }
            }
            RolloutTelemetryArming::LiveCandidateSources => {
                let event_tail_retry_at = self.event_tail_retry_at;
                tokio::select! {
                    changed = self.runtime_source_rx.changed() => changed.is_err(),
                    changed = self.gate_source_rx.changed() => changed.is_err(),
                    changed = self.event_file_source_rx.changed() => changed.is_err(),
                    changed = self.watcher_fault_rx.changed() => changed.is_err(),
                    _ = rollout_telemetry_wait_for_optional_retry(event_tail_retry_at) => false,
                    changed = shutdown_rx.changed() => {
                        changed.is_err() || *shutdown_rx.borrow()
                    }
                }
            }
        }
    }

    pub async fn run_until_stopped_async(
        mut self,
        host: Arc<dyn RolloutTelemetryHost>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
        label: String,
    ) {
        loop {
            if *shutdown_rx.borrow() {
                return;
            }
            let now = Instant::now();
            let generations = match self.prepare_runnable_generations(now) {
                Ok(generations) => generations,
                Err(error) => {
                    tracing::warn!(
                        "sinomo {label} rollout telemetry source observation failed: {error}"
                    );
                    self.fail_generation(Instant::now());
                    continue;
                }
            };
            if let Some(generations) = generations {
                let host = Arc::clone(&host);
                let result =
                    tokio::task::spawn_blocking(move || host.reconcile_rollout_telemetry()).await;
                match result {
                    Ok(Ok(outcome)) => {
                        if let Some(applied) = outcome.applied_transition {
                            tracing::info!(
                                "sinomo {label} rollout telemetry applied {:?} at {} bps",
                                applied.transition.action,
                                applied.transition.current_weight_bps
                            );
                        }
                        self.complete_generations(generations, &outcome.status);
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(
                            "sinomo {label} rollout telemetry reconciliation failed: {error}"
                        );
                        self.fail_generation(Instant::now());
                    }
                    Err(error) => {
                        tracing::warn!("sinomo {label} rollout telemetry task failed: {error}");
                        self.fail_generation(Instant::now());
                    }
                }
                continue;
            }
            if self
                .wait_for_source_retry_or_shutdown(&mut shutdown_rx)
                .await
            {
                return;
            }
        }
    }
}

async fn rollout_telemetry_wait_for_optional_retry(retry_at: Option<Instant>) {
    match retry_at {
        Some(retry_at) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(retry_at)).await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn rollout_telemetry_arming_after(
    status: &crate::RolloutTelemetryAdvanceStatus,
) -> RolloutTelemetryArming {
    match status {
        crate::RolloutTelemetryAdvanceStatus::NoRuntimeState
        | crate::RolloutTelemetryAdvanceStatus::NoCandidate
        | crate::RolloutTelemetryAdvanceStatus::TerminalStage
        | crate::RolloutTelemetryAdvanceStatus::AppliedTransition => {
            RolloutTelemetryArming::RuntimeStateOnly
        }
        crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry
        | crate::RolloutTelemetryAdvanceStatus::PromotionGateBlocked => {
            RolloutTelemetryArming::LiveCandidateSources
        }
    }
}

fn rollout_telemetry_retry_delay(consecutive_failures: u32) -> Duration {
    let multiplier = 1_u32
        .checked_shl(consecutive_failures.min(16))
        .unwrap_or(u32::MAX);
    ROLLOUT_TELEMETRY_RETRY_INITIAL
        .saturating_mul(multiplier)
        .min(ROLLOUT_TELEMETRY_RETRY_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    fn rollout_telemetry_reactor_test_event(label: &str, kind: &str) -> sinomo_core::EventEnvelope {
        sinomo_core::EventEnvelope::new(
            sinomo_core::Id::from_legacy_named(format!("event_{label}")).expect("event id"),
            time::OffsetDateTime::now_utc(),
            sinomo_core::node::NodeId::from_legacy_named("node_rollout_reactor").expect("node id"),
            sinomo_core::Source::new("test.rollout-reactor").expect("source"),
            sinomo_core::EventKind::new(kind).expect("event kind"),
            sinomo_core::Salience::new(0.5).expect("salience"),
            sinomo_core::Confidence::new(0.9).expect("confidence"),
            sinomo_core::RoutingMetadata::new(
                sinomo_core::RoutingScope::Local,
                sinomo_core::LatencyClass::Reactive,
                sinomo_core::PrivacyClass::Derived,
            ),
        )
    }

    fn rollout_telemetry_test_reactor(
        label: &str,
    ) -> (sinomo_testdir::TestDir, PathBuf, RolloutTelemetryReactor) {
        let root = sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{label}"));
        let data_dir = root.path().to_path_buf();
        let reactor = RolloutTelemetryReactor::new(&data_dir).expect("telemetry reactor");
        (root, data_dir, reactor)
    }

    fn complete_rollout_telemetry_startup(
        reactor: &mut RolloutTelemetryReactor,
        status: crate::RolloutTelemetryAdvanceStatus,
    ) {
        assert_eq!(
            reactor
                .reconcile_pending_with(Instant::now(), || Ok::<_, ()>(("startup", status)))
                .expect("startup reconciliation"),
            Some("startup")
        );
    }

    fn append_rollout_telemetry_event_raw(
        path: &Path,
        event: sinomo_core::EventEnvelope,
        terminate_record: bool,
    ) {
        let stored = sinomo_core::events::StoredEvent {
            sequence: sinomo_core::events::EventSequence(1),
            appended_at: time::OffsetDateTime::now_utc(),
            event,
        };
        let mut bytes = serde_json::to_vec(&stored).expect("stored event JSON");
        if terminate_record {
            bytes.push(b'\n');
        }
        append_rollout_telemetry_bytes_raw(path, &bytes);
    }

    fn append_rollout_telemetry_bytes_raw(path: &Path, bytes: &[u8]) {
        use std::io::Write as _;

        fs::create_dir_all(path.parent().expect("event log parent")).expect("event log parent");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("raw event log append");
        file.write_all(bytes).expect("raw event bytes");
        file.flush().expect("raw event flush");
    }

    fn replace_rollout_telemetry_source_raw(path: &Path, bytes: &[u8]) {
        let parent = path.parent().expect("source parent");
        fs::create_dir_all(parent).expect("source parent");
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("source file name");
        let temporary = parent.join(format!(".{file_name}.external-replacement"));
        fs::write(&temporary, bytes).expect("external source staging write");
        sinomo_core::storage::durable_file::replace_file(&temporary, path)
            .expect("external source atomic replacement");
    }

    async fn wait_for_source_generation_after(
        source_rx: &mut tokio::sync::watch::Receiver<u64>,
        generation: u64,
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while *source_rx.borrow_and_update() <= generation {
                source_rx.changed().await.expect("source owner retained");
            }
        })
        .await
        .expect("source filesystem notification");
    }

    fn prepared_rollout_telemetry_generations(
        reactor: &mut RolloutTelemetryReactor,
        now: Instant,
    ) -> Option<RolloutTelemetrySourceGenerations> {
        reactor
            .prepare_runnable_generations(now)
            .expect("prepare rollout telemetry sources")
    }

    #[tokio::test]
    async fn rollout_telemetry_idle_and_terminal_generations_have_no_healthy_wake() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-idle-terminal");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::NoRuntimeState,
        );
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
        assert_eq!(reactor.retry_at, None);

        let event_log_path = root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH);
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &event_log_path,
            rollout_telemetry_reactor_test_event(
                "no-runtime-relevant",
                sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
            ),
            true,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;

        let gate_generation = *reactor.gate_source_rx.borrow();
        replace_rollout_telemetry_source_raw(&reactor.source_paths.gate_config, b"external gate");
        wait_for_source_generation_after(&mut reactor.gate_source_rx, gate_generation).await;
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None,
            "events and gates cannot create a missing runtime state"
        );

        let runtime_generation = *reactor.runtime_source_rx.borrow();
        replace_rollout_telemetry_source_raw(
            &reactor.source_paths.runtime_state,
            b"external runtime state",
        );
        wait_for_source_generation_after(&mut reactor.runtime_source_rx, runtime_generation).await;
        assert_eq!(
            reactor
                .reconcile_pending_with(Instant::now(), || {
                    Ok::<_, ()>((
                        "terminal",
                        crate::RolloutTelemetryAdvanceStatus::TerminalStage,
                    ))
                })
                .expect("terminal reconciliation"),
            Some("terminal")
        );
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &event_log_path,
            rollout_telemetry_reactor_test_event(
                "terminal-relevant",
                sinomo_module::MODULE_INVOCATION_DECISION_EVENT_KIND,
            ),
            true,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;
        assert_eq!(
            prepared_rollout_telemetry_generations(
                &mut reactor,
                Instant::now() + Duration::from_secs(24 * 60 * 60)
            ),
            None,
            "terminal state ignores telemetry until runtime state changes"
        );
        let runtime_generation = *reactor.runtime_source_rx.borrow();
        replace_rollout_telemetry_source_raw(
            &reactor.source_paths.runtime_state,
            b"new external runtime state",
        );
        wait_for_source_generation_after(&mut reactor.runtime_source_rx, runtime_generation).await;
        assert!(prepared_rollout_telemetry_generations(&mut reactor, Instant::now()).is_some());
        assert_eq!(reactor.retry_at, None, "success must not retain a timer");
    }

    #[tokio::test]
    async fn rollout_telemetry_disarmed_event_activity_does_not_wake_task() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-disarmed-wait");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::NoRuntimeState,
        );
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let event_generation = *reactor.event_file_source_rx.borrow();
        let event_log_path = root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH);
        let event_writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            append_rollout_telemetry_event_raw(
                &event_log_path,
                rollout_telemetry_reactor_test_event(
                    "disarmed",
                    sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
                ),
                true,
            );
        });
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                reactor.wait_for_source_retry_or_shutdown(&mut shutdown_rx)
            )
            .await
            .is_err(),
            "event-log activity must not wake a reactor with no live candidate"
        );
        event_writer.await.expect("event writer");
        assert!(
            *reactor.event_file_source_rx.borrow() > event_generation,
            "the filesystem watcher did observe the ignored event append"
        );

        let runtime_path = reactor.source_paths.runtime_state.clone();
        let runtime_writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            replace_rollout_telemetry_source_raw(&runtime_path, b"external runtime rearm");
        });
        assert!(
            !tokio::time::timeout(
                Duration::from_secs(2),
                reactor.wait_for_source_retry_or_shutdown(&mut shutdown_rx)
            )
            .await
            .expect("runtime source wake"),
            "runtime source activity re-arms the reactor"
        );
        runtime_writer.await.expect("runtime writer");
    }

    #[tokio::test]
    async fn rollout_telemetry_irrelevant_external_event_does_not_wake_reactor() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-irrelevant-event");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
        );
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
            rollout_telemetry_reactor_test_event("irrelevant", "daemon.status.observed"),
            true,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;

        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
    }

    #[tokio::test]
    async fn rollout_telemetry_relevant_external_event_runs_exactly_once() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-relevant-event");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
        );
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
            rollout_telemetry_reactor_test_event(
                "relevant",
                sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
            ),
            true,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;

        let mut reconciliations = 0;
        assert_eq!(
            reactor
                .reconcile_pending_with(Instant::now(), || {
                    reconciliations += 1;
                    Ok::<_, ()>((
                        (),
                        crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
                    ))
                })
                .expect("event reconciliation"),
            Some(())
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
        assert_eq!(reconciliations, 1);
    }

    #[tokio::test]
    async fn rollout_telemetry_partial_external_event_gets_one_activity_bound_retry() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-partial-event");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
        );
        let event_log_path = root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH);
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &event_log_path,
            rollout_telemetry_reactor_test_event(
                "partial",
                sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
            ),
            false,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;
        // A single append can map to more than one backend notification.
        // Let that finite burst coalesce into the source generation before
        // asserting the one retry owned by this write.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None,
            "an uncommitted JSONL record is not a telemetry source"
        );
        let retry_at = reactor
            .event_tail_retry_at
            .expect("activity-bound partial-record retry");
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, retry_at),
            None
        );
        assert_eq!(
            reactor.event_tail_retry_at, None,
            "a torn writer cannot create an indefinite timer"
        );

        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_bytes_raw(&event_log_path, b"\n");
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;
        assert!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()).is_some(),
            "the next committed file mutation completes and emits the relevant event generation"
        );
    }

    #[test]
    fn rollout_telemetry_duplicate_notification_cannot_rearm_incomplete_tail_retry() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-duplicate-partial-notification");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
        );
        let event_log_path = root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH);
        append_rollout_telemetry_event_raw(
            &event_log_path,
            rollout_telemetry_reactor_test_event(
                "partial-duplicate",
                sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
            ),
            false,
        );
        reactor
            .event_file_source_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));

        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
        let retry_at = reactor
            .event_tail_retry_at
            .expect("first incomplete length owns one retry");
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, retry_at),
            None
        );
        assert_eq!(reactor.event_tail_retry_at, None);

        reactor
            .event_file_source_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
        assert_eq!(
            reactor.event_tail_retry_at, None,
            "a duplicate watcher notification at the same incomplete length cannot re-arm"
        );

        append_rollout_telemetry_bytes_raw(&event_log_path, b" ");
        reactor
            .event_file_source_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );
        assert!(
            reactor.event_tail_retry_at.is_some(),
            "newly appended bytes may own one bounded retry"
        );
    }

    #[test]
    fn rollout_telemetry_watcher_fault_is_owned_and_reconciles_all_sources() {
        let (_root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-watcher-fault");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::NoRuntimeState,
        );
        reactor.watcher_fault_tx.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        assert!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()).is_some(),
            "rebuilding the watcher dirties durable runtime state after the observation gap"
        );
        assert_eq!(
            reactor.completed_watcher_fault_generation,
            *reactor.watcher_fault_rx.borrow()
        );
    }

    #[tokio::test]
    async fn rollout_telemetry_blocked_gate_rearms_from_external_atomic_replacement() {
        let (root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-gate-source");
        complete_rollout_telemetry_startup(
            &mut reactor,
            crate::RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
        );
        let event_generation = *reactor.event_file_source_rx.borrow();
        append_rollout_telemetry_event_raw(
            &root.join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
            rollout_telemetry_reactor_test_event(
                "gate-blocked",
                sinomo_module::MODULE_SHADOW_DECISION_EVENT_KIND,
            ),
            true,
        );
        wait_for_source_generation_after(&mut reactor.event_file_source_rx, event_generation).await;
        assert_eq!(
            reactor
                .reconcile_pending_with(Instant::now(), || {
                    Ok::<_, ()>((
                        "gate-blocked",
                        crate::RolloutTelemetryAdvanceStatus::PromotionGateBlocked,
                    ))
                })
                .expect("blocked gate reconciliation"),
            Some("gate-blocked")
        );
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, Instant::now()),
            None
        );

        let gate_generation = *reactor.gate_source_rx.borrow();
        replace_rollout_telemetry_source_raw(
            &reactor.source_paths.gate_config,
            b"external gate replacement",
        );
        wait_for_source_generation_after(&mut reactor.gate_source_rx, gate_generation).await;
        assert!(prepared_rollout_telemetry_generations(&mut reactor, Instant::now()).is_some());
    }

    #[test]
    fn rollout_telemetry_retry_timer_exists_only_after_failure() {
        let (_root, _paths, mut reactor) =
            rollout_telemetry_test_reactor("telemetry-reactor-failure-retry");
        assert_eq!(reactor.retry_at, None);
        let now = Instant::now();
        let error = reactor
            .reconcile_pending_with(now, || {
                Err::<((), crate::RolloutTelemetryAdvanceStatus), _>("injected failure")
            })
            .expect_err("failed reconciliation");
        assert_eq!(error, "injected failure");
        let retry_at = reactor.retry_at.expect("failure-owned retry");
        assert_eq!(
            retry_at.saturating_duration_since(now),
            ROLLOUT_TELEMETRY_RETRY_INITIAL
        );

        let mut ran_early = false;
        assert_eq!(
            reactor
                .reconcile_pending_with(retry_at - Duration::from_millis(1), || {
                    ran_early = true;
                    Ok::<_, ()>(((), crate::RolloutTelemetryAdvanceStatus::NoRuntimeState))
                })
                .expect("retry not due"),
            None
        );
        assert!(!ran_early);
        assert_eq!(
            reactor
                .reconcile_pending_with(retry_at, || {
                    Ok::<_, ()>((
                        "recovered",
                        crate::RolloutTelemetryAdvanceStatus::NoRuntimeState,
                    ))
                })
                .expect("retry due"),
            Some("recovered")
        );
        assert_eq!(reactor.retry_at, None);
        assert_eq!(
            prepared_rollout_telemetry_generations(&mut reactor, retry_at),
            None
        );
    }
}
