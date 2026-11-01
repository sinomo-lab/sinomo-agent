//! Operational events, logs, and object-RPC observation service.

mod builtin_metrics;
mod cli;
mod logs;
pub mod performance;
mod profile;
mod prometheus;
mod prometheus_server;
mod runtime;
mod timeline;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use sinomo_core::{
    Confidence, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source, TrustRing,
};
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

use sinomo_api::observe::{
    OBSERVE_METRIC_EXPORTER_INTERFACE_ID, OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION,
    OBSERVE_METRIC_EXPORTER_MAX_RECORDS, OBSERVE_METRIC_EXPORTER_OPERATION,
    ObserveMetricExportRequest, ObserveMetricExportResponse,
};

pub use builtin_metrics::{dfs_status_observe_metrics, register_builtin_observe_metric_exporters};
pub use cli::{
    ObserveCliCommandHandler, observe_cli_command_handler,
    observe_cli_command_handler_with_cluster_rpc, observe_cli_command_handler_with_rpc,
};
pub use profile::{
    OBSERVE_CPU_PROFILE_ARTIFACT_ROOT, OBSERVE_CPU_PROFILE_DEFAULT_DURATION_SECONDS,
    OBSERVE_CPU_PROFILE_DEFAULT_FREQUENCY_HZ, OBSERVE_CPU_PROFILE_MAX_ARTIFACT_BYTES,
    OBSERVE_CPU_PROFILE_MAX_DURATION_SECONDS, OBSERVE_CPU_PROFILE_MAX_FREQUENCY_HZ,
    OBSERVE_CPU_PROFILE_MIN_DURATION_SECONDS, OBSERVE_CPU_PROFILE_MIN_FREQUENCY_HZ,
    ObserveCpuProfileAction, ObserveCpuProfileRequest, ObserveCpuProfileResponse,
    ObserveCpuProfileState, handle_observe_cpu_profile, observe_cpu_profile_snapshot,
};
pub use prometheus::{
    OPENMETRICS_CONTENT_TYPE, OpenMetricsEncodeError, OpenMetricsEncoder, encode_openmetrics,
    encode_openmetrics_with_labels,
};
pub use runtime::{
    OBSERVE_BUILT_IN_QUIET_FILTER_DIRECTIVES, OBSERVE_DEFAULT_BASE_FILTER, OBSERVE_DEFAULT_FILTER,
    OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES, ObserveControlRequest, ObserveControlResponse,
    ObserveHistogramBucket, ObserveMetricAggregation, ObserveMetricDescriptor,
    ObserveMetricExporterStatus, ObserveMetricKind, ObserveMetricRecord, ObserveMetricSemantics,
    ObserveMetricUnit, ObserveMetricsRequest, ObserveMetricsResponse, ObserveRuntimeConfig,
    ObserveSpanContext, ObserveTraceProcess, ObserveTraceRecord, ObserveTracesRequest,
    ObserveTracesResponse, init_observe_runtime, init_observe_runtime_with_log_file,
    init_observe_runtime_with_stderr, observe_control_snapshot,
    observe_filter_with_builtin_quiet_directives, observe_filter_with_quiet_directives,
    observe_metric_descriptors, observe_metric_records_interval_delta,
    observe_metrics_interval_delta, observe_metrics_response_from_metrics,
    observe_process_started_at, read_observe_metrics, read_observe_metrics_for_data_dir,
    read_observe_traces, register_observe_metric_descriptor, register_observe_metric_exporter,
    register_observe_metric_exporter_with_generation, register_observe_metric_exporter_with_policy,
    register_observe_metric_exporter_with_prefixes, update_observe_control,
};
pub use sinomo_api::{
    IdleStatsAtomics, install_process_idle_stats, process_idle_stats, register_io_observer,
};
pub use timeline::{
    OBSERVE_NETWORK_TIMELINE_ARTIFACT_FILE_NAME, OBSERVE_NETWORK_TIMELINE_ARTIFACT_ROOT,
    OBSERVE_NETWORK_TIMELINE_DEFAULT_DURATION_SECONDS, OBSERVE_NETWORK_TIMELINE_DEFAULT_MAX_EVENTS,
    OBSERVE_NETWORK_TIMELINE_MAX_DURATION_SECONDS, OBSERVE_NETWORK_TIMELINE_MAX_EVENTS,
    OBSERVE_NETWORK_TIMELINE_MIN_DURATION_SECONDS, ObserveNetworkTimelineAction,
    ObserveNetworkTimelineEvent, ObserveNetworkTimelineRequest, ObserveNetworkTimelineResponse,
    handle_observe_network_timeline, observe_network_timeline_response_from_snapshot,
    persist_active_observe_network_timeline,
};

pub const OBSERVE_PROTOCOL: &str = "sinomo.observe.v1";
pub use sinomo_api::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID;
pub const SINOMO_OBSERVE_MODULE_OWNER: &str = "sinomo-observe";
pub const SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY: &str = "sinomo-observe";
pub use sinomo_api::observe::OBSERVE_RPC_CATEGORY;
pub const OBSERVE_EVENTS_ROUTE: &str = "/observe/events";
pub use logs::read_observe_logs;
pub use sinomo_api::observe::OBSERVE_METRICS_ROUTE;
pub use sinomo_api::observe::{
    OBSERVE_LOGS_DEFAULT_LIMIT, OBSERVE_LOGS_MAX_RECORDS, OBSERVE_LOGS_MAX_RESPONSE_BYTES,
    OBSERVE_LOGS_ROUTE, ObserveLogSource, ObserveLogsRequest, ObserveLogsResponse,
    OperationalLogRecord, OperationalSeverity,
};
pub const OBSERVE_TRACES_ROUTE: &str = "/observe/traces";
pub const OBSERVE_CONTROL_ROUTE: &str = "/observe/control";
pub const OBSERVE_CPU_PROFILE_ROUTE: &str = "/observe/profile/cpu";
pub const OBSERVE_NETWORK_TIMELINE_ROUTE: &str = "/observe/timeline/network";
pub const OBSERVE_OBJECT_RPC_ROUTES: &[&str] = &[
    OBSERVE_EVENTS_ROUTE,
    OBSERVE_LOGS_ROUTE,
    OBSERVE_METRICS_ROUTE,
    OBSERVE_TRACES_ROUTE,
    OBSERVE_CONTROL_ROUTE,
    OBSERVE_CPU_PROFILE_ROUTE,
    OBSERVE_NETWORK_TIMELINE_ROUTE,
];

pub const OPERATIONAL_EVENTS_RELATIVE_PATH: &str = "events/ops/operational-events.jsonl";
pub const OPERATIONAL_LOG_RELATIVE_PATH: &str = "events/ops/logs/operational-log.jsonl";
pub const OPERATIONAL_EVENTS_LOCK_RELATIVE_PATH: &str = "events/ops/operational-events.jsonl.lock";
pub const OPERATIONAL_LOG_LOCK_RELATIVE_PATH: &str = "events/ops/logs/operational-log.jsonl.lock";
pub const OPERATIONAL_JSONL_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub const OPERATIONAL_JSONL_RETAINED_FILES: usize = 4;

const OBSERVE_ROUTES: &[&str] = OBSERVE_OBJECT_RPC_ROUTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JsonlRotationConfig {
    max_bytes: u64,
    retained_files: usize,
}

impl JsonlRotationConfig {
    const OPERATIONAL: Self = Self {
        max_bytes: OPERATIONAL_JSONL_MAX_BYTES,
        retained_files: OPERATIONAL_JSONL_RETAINED_FILES,
    };
}

fn operational_salience(severity: OperationalSeverity) -> Salience {
    let value = match severity {
        OperationalSeverity::Trace => 0.1,
        OperationalSeverity::Debug => 0.2,
        OperationalSeverity::Info => 0.4,
        OperationalSeverity::Warn => 0.75,
        OperationalSeverity::Error => 0.95,
    };
    Salience::new(value).expect("operational salience is in range")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OperationalEventRecord {
    #[serde(with = "time::serde::rfc3339")]
    pub appended_at: OffsetDateTime,
    pub severity: OperationalSeverity,
    pub component: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    pub event: EventEnvelope,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperationalEventQuery {
    pub kind_prefix: Option<String>,
    pub component: Option<String>,
    pub operation_id: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperationalLogQuery {
    pub component: Option<String>,
    pub operation_id: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveEventsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveEventsResponse {
    pub records: Vec<OperationalEventRecord>,
}

impl From<ObserveEventsRequest> for OperationalEventQuery {
    fn from(request: ObserveEventsRequest) -> Self {
        Self {
            kind_prefix: request.kind_prefix,
            component: request.component,
            operation_id: request.operation_id,
            limit: request.limit,
        }
    }
}

impl From<ObserveLogsRequest> for OperationalLogQuery {
    fn from(request: ObserveLogsRequest) -> Self {
        Self {
            component: request.component,
            operation_id: request.operation_id,
            limit: request.limit,
        }
    }
}

#[derive(Debug, Error)]
pub enum ObserveError {
    #[error("observe I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("observe JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("observe parse error: {detail}")]
    Parse { detail: String },
    #[error("observe module state error: {detail}")]
    ModuleState { detail: String },
    #[error("observe runtime error: {detail}")]
    Runtime { detail: String },
}

pub fn operational_events_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(OPERATIONAL_EVENTS_RELATIVE_PATH)
}

pub fn operational_logs_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(OPERATIONAL_LOG_RELATIVE_PATH)
}

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event(
    data_dir: impl AsRef<Path>,
    origin_node_id: Option<Id>,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<OperationalEventRecord, ObserveError> {
    let data_dir = data_dir.as_ref();
    let appended_at = OffsetDateTime::now_utc();
    let message = message.into();
    let operation_id = operation_id.map(ToString::to_string);
    let mut payload = serde_json::Map::new();
    payload.insert("severity".to_string(), serde_json::json!(severity.as_str()));
    payload.insert(
        "component".to_string(),
        serde_json::Value::String(component.to_string()),
    );
    payload.insert(
        "message".to_string(),
        serde_json::Value::String(message.clone()),
    );
    if let Some(operation_id) = operation_id.as_ref() {
        payload.insert(
            "operation_id".to_string(),
            serde_json::Value::String(operation_id.clone()),
        );
    }
    if !fields.is_null() {
        payload.insert("fields".to_string(), fields);
    }

    let event = EventEnvelope::new(
        Id::generate(),
        appended_at,
        origin_node_id.unwrap_or_else(|| operational_origin_node_id(data_dir)),
        Source::new(format!("observe.{component}")).map_err(|source| ObserveError::Parse {
            detail: source.to_string(),
        })?,
        EventKind::new(kind).map_err(|source| ObserveError::Parse {
            detail: source.to_string(),
        })?,
        operational_salience(severity),
        Confidence::new(1.0).expect("operational confidence is in range"),
        RoutingMetadata::new(
            RoutingScope::Local,
            LatencyClass::Background,
            PrivacyClass::Private,
        ),
    )
    .with_payload(serde_json::Value::Object(payload));
    let record = OperationalEventRecord {
        appended_at,
        severity,
        component: component.to_string(),
        message,
        operation_id,
        event,
    };
    append_jsonl_record(&operational_events_path(data_dir), &record)?;
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event_if_enabled(
    data_dir: impl AsRef<Path>,
    origin_node_id: Option<Id>,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<Option<OperationalEventRecord>, ObserveError> {
    let data_dir = data_dir.as_ref();
    if !observe_module_enabled(data_dir)? {
        return Ok(None);
    }
    append_operational_event(
        data_dir,
        origin_node_id,
        severity,
        component,
        kind,
        message,
        operation_id,
        fields,
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event_best_effort(
    data_dir: impl AsRef<Path>,
    origin_node_id: Option<Id>,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    if let Err(error) = append_operational_event_if_enabled(
        data_dir,
        origin_node_id,
        severity,
        component,
        kind,
        message,
        operation_id,
        fields,
    ) {
        eprintln!("sinomo observe operational event append failed: {error}");
    }
}

pub fn append_operational_log(
    data_dir: impl AsRef<Path>,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<OperationalLogRecord, ObserveError> {
    let data_dir = data_dir.as_ref();
    let record = OperationalLogRecord {
        source: ObserveLogSource::Operational,
        truncated: false,
        timestamp: OffsetDateTime::now_utc(),
        severity,
        component: component.to_string(),
        message: message.into(),
        operation_id: operation_id.map(ToString::to_string),
        fields,
    };
    append_jsonl_record(&operational_logs_path(data_dir), &record)?;
    Ok(record)
}

pub fn append_operational_log_if_enabled(
    data_dir: impl AsRef<Path>,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<Option<OperationalLogRecord>, ObserveError> {
    let data_dir = data_dir.as_ref();
    if !observe_module_enabled(data_dir)? {
        return Ok(None);
    }
    append_operational_log(data_dir, severity, component, message, operation_id, fields).map(Some)
}

pub fn append_operational_log_best_effort(
    data_dir: impl AsRef<Path>,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    if let Err(error) = append_operational_log_if_enabled(
        data_dir,
        severity,
        component,
        message,
        operation_id,
        fields,
    ) {
        eprintln!("sinomo observe operational log append failed: {error}");
    }
}

pub fn read_operational_events(
    data_dir: impl AsRef<Path>,
    query: &OperationalEventQuery,
) -> Result<Vec<OperationalEventRecord>, ObserveError> {
    read_jsonl_tail::<OperationalEventRecord, _>(
        operational_events_path(data_dir),
        query.limit,
        |record| {
            query
                .kind_prefix
                .as_ref()
                .is_none_or(|prefix| record.event.kind.as_str().starts_with(prefix))
                && query
                    .component
                    .as_ref()
                    .is_none_or(|component| &record.component == component)
                && query
                    .operation_id
                    .as_ref()
                    .is_none_or(|operation_id| record.operation_id.as_ref() == Some(operation_id))
        },
    )
}

pub fn read_operational_logs(
    data_dir: impl AsRef<Path>,
    query: &OperationalLogQuery,
) -> Result<Vec<OperationalLogRecord>, ObserveError> {
    read_jsonl_tail::<OperationalLogRecord, _>(
        operational_logs_path(data_dir),
        query.limit,
        |record| {
            query
                .component
                .as_ref()
                .is_none_or(|component| &record.component == component)
                && query
                    .operation_id
                    .as_ref()
                    .is_none_or(|operation_id| record.operation_id.as_ref() == Some(operation_id))
        },
    )
}

fn operational_origin_node_id(data_dir: &Path) -> Id {
    Id::derived(
        "sinomo.observe.operational.local-node",
        data_dir.display().to_string().as_bytes(),
    )
}

fn append_jsonl_record<T: Serialize>(path: &Path, record: &T) -> Result<(), ObserveError> {
    append_jsonl_record_with_config(path, record, JsonlRotationConfig::OPERATIONAL)
}

fn append_jsonl_record_with_config<T: Serialize>(
    path: &Path,
    record: &T,
    rotation: JsonlRotationConfig,
) -> Result<(), ObserveError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ObserveError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let lock = open_jsonl_lock(path)?;
    FileExt::lock_exclusive(&lock).map_err(|source| lock_error(path, source))?;
    let result = (|| {
        rotate_jsonl_if_needed(path, rotation)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|source| ObserveError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        serde_json::to_writer(&mut file, record).map_err(|source| ObserveError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        writeln!(file).map_err(|source| ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.flush().map_err(|source| ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    })();
    let unlock = FileExt::unlock(&lock).map_err(|source| lock_error(path, source));
    match (result, unlock) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

fn read_jsonl_tail<T, F>(path: PathBuf, limit: usize, keep: F) -> Result<Vec<T>, ObserveError>
where
    T: for<'de> Deserialize<'de>,
    F: FnMut(&T) -> bool,
{
    read_jsonl_tail_with_config(path, limit, JsonlRotationConfig::OPERATIONAL, keep)
}

fn read_jsonl_tail_with_config<T, F>(
    path: PathBuf,
    limit: usize,
    rotation: JsonlRotationConfig,
    mut keep: F,
) -> Result<Vec<T>, ObserveError>
where
    T: for<'de> Deserialize<'de>,
    F: FnMut(&T) -> bool,
{
    let lock = open_jsonl_lock(&path)?;
    FileExt::lock_shared(&lock).map_err(|source| lock_error(&path, source))?;
    let result = (|| {
        let mut records = VecDeque::new();
        for path in jsonl_read_paths_chronological(&path, rotation.retained_files) {
            read_jsonl_file_into_tail(&path, limit, &mut keep, &mut records)?;
        }
        Ok(records.into_iter().collect())
    })();
    let unlock = FileExt::unlock(&lock).map_err(|source| lock_error(&path, source));
    match (result, unlock) {
        (Ok(records), Ok(())) => Ok(records),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

fn read_jsonl_file_into_tail<T, F>(
    path: &Path,
    limit: usize,
    keep: &mut F,
    records: &mut VecDeque<T>,
) -> Result<(), ObserveError>
where
    T: for<'de> Deserialize<'de>,
    F: FnMut(&T) -> bool,
{
    if !path.exists() {
        return Ok(());
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|source| ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    for (index, line) in BufReader::new(&file).lines().enumerate() {
        let line = line.map_err(|source| ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        // A Windows process killed during an append can leave a sparse,
        // zero-filled prefix before the next complete JSONL record. Recover
        // that complete record without weakening validation of the JSON that
        // follows the hole.
        let line = line.trim();
        let line = line.trim_start_matches('\0').trim_start();
        if line.is_empty() {
            continue;
        }
        let record: T = serde_json::from_str(line).map_err(|source| ObserveError::Json {
            path: format!("{}:{}", path.display(), index + 1),
            detail: source.to_string(),
        })?;
        if keep(&record) {
            records.push_back(record);
            if limit > 0 && records.len() > limit {
                records.pop_front();
            }
        }
    }
    Ok(())
}

fn open_jsonl_lock(path: &Path) -> Result<File, ObserveError> {
    let lock_path = jsonl_lock_path(path);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ObserveError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| ObserveError::Io {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })
}

fn jsonl_lock_path(path: &Path) -> PathBuf {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return path.with_extension("lock");
    };
    path.with_file_name(format!("{file_name}.lock"))
}

fn lock_error(path: &Path, source: std::io::Error) -> ObserveError {
    ObserveError::Io {
        path: jsonl_lock_path(path).display().to_string(),
        detail: source.to_string(),
    }
}

fn rotate_jsonl_if_needed(path: &Path, rotation: JsonlRotationConfig) -> Result<(), ObserveError> {
    if rotation.max_bytes == 0 || !path.exists() {
        return Ok(());
    }
    let size = fs::metadata(path)
        .map_err(|source| ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    if size < rotation.max_bytes {
        return Ok(());
    }
    rotate_jsonl_files(path, rotation.retained_files)
}

fn rotate_jsonl_files(path: &Path, retained_files: usize) -> Result<(), ObserveError> {
    if retained_files == 0 {
        return remove_file_if_exists(path);
    }
    remove_file_if_exists(&rotated_jsonl_path(path, retained_files))?;
    for index in (1..retained_files).rev() {
        rename_file_if_exists(
            &rotated_jsonl_path(path, index),
            &rotated_jsonl_path(path, index + 1),
        )?;
    }
    rename_file_if_exists(path, &rotated_jsonl_path(path, 1))
}

fn remove_file_if_exists(path: &Path) -> Result<(), ObserveError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ObserveError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn rename_file_if_exists(source: &Path, destination: &Path) -> Result<(), ObserveError> {
    if !source.exists() {
        return Ok(());
    }
    if destination.exists() {
        remove_file_if_exists(destination)?;
    }
    fs::rename(source, destination).map_err(|error| ObserveError::Io {
        path: format!("{} -> {}", source.display(), destination.display()),
        detail: error.to_string(),
    })
}

fn jsonl_read_paths_chronological(path: &Path, retained_files: usize) -> Vec<PathBuf> {
    let mut paths = (1..=retained_files)
        .rev()
        .map(|index| rotated_jsonl_path(path, index))
        .collect::<Vec<_>>();
    paths.push(path.to_path_buf());
    paths
}

fn rotated_jsonl_path(path: &Path, index: usize) -> PathBuf {
    path.with_file_name(format!(
        "{}.{index}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("operational.jsonl")
    ))
}

pub fn observe_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(SINOMO_OBSERVE_MODULE_LEGACY_ID)
        .expect("valid observe module id")
}

pub fn observe_module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        observe_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_OBSERVE_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        OBSERVE_PROTOCOL,
        TrustRing::R1TrustedRuntime,
        SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY,
    )
    .with_display_name("Observability");
    registration.manifest.capabilities = observe_module_capabilities();
    registration.manifest.limits = sinomo_core::ModuleResourceLimits {
        cpu_ms: 5_000,
        memory_mb: 128,
        wall_time_ms: 5_000,
        network: sinomo_core::NetworkAccess::Deny,
        filesystem: sinomo_core::FileSystemAccess::Scoped,
    };
    registration.surfaces = observe_module_surfaces();
    registration
}

pub fn observe_module_surfaces() -> sinomo_module::ModuleSurfaces {
    use sinomo_module::{ModuleObjectRpcRoute, ModuleRouteAuth, ModuleSurfaces};

    let mut surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_EVENTS_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.events.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_LOGS_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.logs.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_METRICS_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.metrics.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_TRACES_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.traces.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_CONTROL_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.control.write",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_CPU_PROFILE_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.profile.cpu.write",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            OBSERVE_NETWORK_TIMELINE_ROUTE,
            OBSERVE_RPC_CATEGORY,
            "observe.timeline.network.write",
            ModuleRouteAuth::Admin,
        ))
        .with_event_kind_name("daemon.lifecycle.started")
        .with_event_kind_name("daemon.lifecycle.shutdown_requested")
        .with_event_kind_name("daemon.lifecycle.stopped")
        .with_event_kind_name("daemon.supervision.started")
        .with_event_kind_name("daemon.supervision.main-exit")
        .with_event_kind_name("rollout.stage.progress")
        .with_event_kind_name("rollout.stage.completed")
        .with_event_kind_name("rollout.stage.failed")
        .with_storage_owner(SINOMO_OBSERVE_MODULE_OWNER);
    surfaces.capabilities = observe_module_capabilities();
    surfaces
}

pub fn observe_module_capabilities() -> sinomo_core::ModuleCapabilities {
    sinomo_core::ModuleCapabilities {
        imports: vec![
            observe_capability("storage.local.read"),
            observe_capability("storage.local.write"),
            observe_capability(OBSERVE_METRIC_EXPORTER_OPERATION),
        ],
        exports: vec![
            observe_capability("observe.events.read"),
            observe_capability("observe.logs.read"),
            observe_capability("observe.metrics.read"),
            observe_capability("observe.traces.read"),
            observe_capability("observe.control.write"),
            observe_capability("observe.profile.cpu.write"),
            observe_capability("observe.timeline.network.write"),
            observe_capability("observe.events.write"),
            observe_capability("observe.logs.write"),
        ],
    }
}

fn observe_capability(name: &str) -> sinomo_core::CapabilityName {
    sinomo_core::CapabilityName::new(name).expect("valid observe module capability")
}

pub fn observe_module_enabled(data_dir_root: impl AsRef<Path>) -> Result<bool, ObserveError> {
    sinomo_module::native_rust_module_enabled(data_dir_root.as_ref(), &observe_module_id(), true)
        .map_err(|source| ObserveError::ModuleState {
            detail: source.to_string(),
        })
}

pub fn set_observe_module_enabled(
    data_dir_root: impl AsRef<Path>,
    enabled: bool,
    source: impl Into<String>,
    reason: impl Into<String>,
) -> Result<PathBuf, ObserveError> {
    sinomo_module::set_native_rust_module_enabled(
        data_dir_root.as_ref(),
        &observe_module_id(),
        enabled,
        source,
        reason,
    )
    .map_err(|source| ObserveError::ModuleState {
        detail: source.to_string(),
    })
}

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::StorageRing;

    for relative_path in [
        OPERATIONAL_EVENTS_RELATIVE_PATH,
        OPERATIONAL_LOG_RELATIVE_PATH,
    ] {
        layout.register_object_log(SINOMO_OBSERVE_MODULE_OWNER, StorageRing::R1, relative_path)?;
        for index in 1..=OPERATIONAL_JSONL_RETAINED_FILES {
            layout.register_object_log(
                SINOMO_OBSERVE_MODULE_OWNER,
                StorageRing::R1,
                format!("{relative_path}.{index}"),
            )?;
        }
    }
    for relative_path in [
        OPERATIONAL_EVENTS_LOCK_RELATIVE_PATH,
        OPERATIONAL_LOG_LOCK_RELATIVE_PATH,
    ] {
        layout.register_binary(SINOMO_OBSERVE_MODULE_OWNER, StorageRing::R1, relative_path)?;
    }
    layout.register_escape_path(
        SINOMO_OBSERVE_MODULE_OWNER,
        StorageRing::R1,
        OBSERVE_CPU_PROFILE_ARTIFACT_ROOT,
        "bounded node-local CPU profile artifacts and manifests",
    )?;
    layout.register_escape_path(
        SINOMO_OBSERVE_MODULE_OWNER,
        StorageRing::R1,
        "observe/formation",
        "operator diagnostic formation captures: JSON responses, stderr text, capture indexes and process-generation markers",
    )?;
    Ok(())
}

const OBSERVE_COMPONENT_EXPORTER_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const OBSERVE_COMPONENT_EXPORTER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
const OBSERVE_COMPONENT_PROVENANCE_LABELS: &[&str] = &[
    "component_id",
    "component_version",
    "component_service_id",
    "component_generation",
    "component_artifact_id",
];

#[derive(Default)]
pub struct ObserveService {
    component_services: Option<sinomo_module::ComponentServiceClient>,
}

impl ObserveService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_component_services(
        component_services: sinomo_module::ComponentServiceClient,
    ) -> Self {
        Self {
            component_services: Some(component_services),
        }
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(ObserveService::new())
}

pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor =
        sinomo_module::component_descriptor_from_registration(&observe_module_registration())?
            .require(observe_metric_exporter_requirement())
            .subscribe_event(sinomo_module::ComponentEventSubscription::new(
                "*", "*", "*",
            ));
    Ok(
        sinomo_module::StaticComponentFactory::new(descriptor, |context, scope| {
            let component_services = context.service_client(scope.descriptor())?;
            scope.set_services(
                ServiceRegistryBuilder::default()
                    .register(ObserveService::with_component_services(component_services))
                    .build(),
            );
            if let Some(runtime) = context.runtime() {
                let label = match context.host_role() {
                    sinomo_module::ComponentHostRole::Main => "daemon",
                    sinomo_module::ComponentHostRole::Shadow => "shadow-daemon",
                    sinomo_module::ComponentHostRole::Embedded => "embedded-daemon",
                };
                let cluster_paths = sinomo_core::network::ClusterPaths::new(context.data_dir());
                let node_id = cluster_paths
                    .state
                    .exists()
                    .then(|| sinomo_core::cluster::read_cluster_state(&cluster_paths).ok())
                    .flatten()
                    .map(|state| state.local_node_id.to_string());
                if let Err(error) = init_observe_runtime(ObserveRuntimeConfig::daemon(label, node_id))
                {
                    tracing::warn!(%error, "observe component runtime initialization failed");
                }
                register_builtin_observe_metric_exporters();
                let timeline_data_dir = context.data_dir_root();
                scope.defer(move || {
                    if let Err(error) = persist_active_observe_network_timeline(&timeline_data_dir)
                    {
                        tracing::warn!(
                            %error,
                            "observe network timeline shutdown artifact was not preserved"
                        );
                    }
                });
                let subscription = sinomo_module::ComponentEventSubscription::new("*", "*", "*");
                let mut events = context
                    .event_client(scope.descriptor())?
                    .subscribe(&subscription)
                    .map_err(|error| {
                        sinomo_module::ComponentError::component(
                            SINOMO_OBSERVE_MODULE_LEGACY_ID,
                            error,
                        )
                    })?;
                let data_dir = context.data_dir_root();
                let mut shutdown = scope.shutdown_receiver();
                scope.own_task(
                    "observe component event recorder",
                    runtime.spawn(async move {
                        loop {
                            tokio::select! {
                                event = events.recv() => match event {
                                    Ok(event) => {
                                        let data_dir = data_dir.clone();
                                        let record = event.as_ref().clone();
                                        match tokio::task::spawn_blocking(move || {
                                            append_component_event_record(&data_dir, &record)
                                        }).await {
                                            Ok(Ok(())) => {}
                                            Ok(Err(error)) => tracing::warn!(%error, "observe component event append failed"),
                                            Err(error) => tracing::warn!(%error, "observe component event append task failed"),
                                        }
                                    }
                                    Err(sinomo_module::ComponentEventReceiveError::Lagged { skipped }) => {
                                        tracing::warn!(skipped, "observe component event recorder lagged");
                                    }
                                    Err(sinomo_module::ComponentEventReceiveError::Closed) => break,
                                },
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        break;
                                    }
                                }
                            }
                        }
                    }),
                );
            }
            Ok(())
        })?
        .with_storage_paths(register_storage_paths),
    )
}

fn append_component_event_record(
    data_dir: &Path,
    event: &sinomo_module::ComponentEvent,
) -> Result<(), ObserveError> {
    let payload = event.envelope.payload.as_object();
    let severity = match payload
        .and_then(|payload| payload.get("severity"))
        .and_then(serde_json::Value::as_str)
    {
        Some("trace") => OperationalSeverity::Trace,
        Some("debug") => OperationalSeverity::Debug,
        Some("warn") => OperationalSeverity::Warn,
        Some("error") => OperationalSeverity::Error,
        _ => OperationalSeverity::Info,
    };
    let component = payload
        .and_then(|payload| payload.get("component"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(event.publisher.component_id.as_str());
    let message = payload
        .and_then(|payload| payload.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| event.envelope.kind.as_str());
    let operation_id = payload
        .and_then(|payload| payload.get("operation_id"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    if event.envelope.kind.as_str() == "daemon.log.recorded" {
        let fields = payload
            .and_then(|payload| payload.get("fields"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        append_operational_log(
            data_dir,
            severity,
            component,
            message,
            operation_id.as_deref(),
            fields,
        )?;
        return Ok(());
    }
    append_jsonl_record(
        &operational_events_path(data_dir),
        &OperationalEventRecord {
            appended_at: OffsetDateTime::now_utc(),
            severity,
            component: component.to_string(),
            message: message.to_string(),
            operation_id,
            event: event.envelope.clone(),
        },
    )
}

impl Service for ObserveService {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn routes(&self) -> &'static [&'static str] {
        OBSERVE_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        OBSERVE_ROUTES
            .iter()
            .copied()
            .map(observe_service_route)
            .collect()
    }

    fn handle(
        &self,
        mut ctx: ServiceContext,
        request: sinomo_core::network::ObjectRpcRequest,
    ) -> ServiceFuture<'_> {
        let component_services = self.component_services.clone();
        Box::pin(async move {
            match request.route.as_str() {
                OBSERVE_EVENTS_ROUTE => {
                    let request: ObserveEventsRequest = ctx.decode_request_body(&request).await?;
                    let records = read_operational_events(ctx.data_dir.root(), &request.into())
                        .map_err(service_error)?;
                    ctx.respond_value(OBSERVE_EVENTS_ROUTE, ObserveEventsResponse { records })?;
                }
                OBSERVE_LOGS_ROUTE => {
                    let request: ObserveLogsRequest = ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.root().to_path_buf();
                    let response =
                        tokio::task::spawn_blocking(move || read_observe_logs(&data_dir, &request))
                            .await
                            .map_err(|error| {
                                service_error(ObserveError::Runtime {
                                    detail: error.to_string(),
                                })
                            })?;
                    ctx.respond_value(OBSERVE_LOGS_ROUTE, response)?;
                }
                OBSERVE_METRICS_ROUTE => {
                    let request: ObserveMetricsRequest = ctx.decode_request_body(&request).await?;
                    let mut response =
                        read_observe_metrics_for_data_dir(ctx.data_dir.as_ref(), &request);
                    if let Some(component_services) = component_services {
                        collect_component_metric_exporters(
                            &component_services,
                            &request,
                            &mut response,
                        )
                        .await;
                    }
                    ctx.respond_value(OBSERVE_METRICS_ROUTE, response)?;
                }
                OBSERVE_TRACES_ROUTE => {
                    let request: ObserveTracesRequest = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(OBSERVE_TRACES_ROUTE, read_observe_traces(&request))?;
                }
                OBSERVE_CONTROL_ROUTE => {
                    let request: ObserveControlRequest = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        OBSERVE_CONTROL_ROUTE,
                        update_observe_control(request).map_err(service_error)?,
                    )?;
                }
                OBSERVE_CPU_PROFILE_ROUTE => {
                    let request: ObserveCpuProfileRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        OBSERVE_CPU_PROFILE_ROUTE,
                        handle_observe_cpu_profile(ctx.data_dir.root(), request)
                            .map_err(service_error)?,
                    )?;
                }
                OBSERVE_NETWORK_TIMELINE_ROUTE => {
                    let request: ObserveNetworkTimelineRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        OBSERVE_NETWORK_TIMELINE_ROUTE,
                        handle_observe_network_timeline(request).map_err(service_error)?,
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn observe_metric_exporter_requirement() -> sinomo_module::ServiceRequirement {
    sinomo_module::ServiceRequirement::new(
        OBSERVE_METRIC_EXPORTER_INTERFACE_ID,
        OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION,
    )
    .with_operation(OBSERVE_METRIC_EXPORTER_OPERATION)
    .with_caller_capability(OBSERVE_METRIC_EXPORTER_OPERATION)
    .optional()
}

async fn collect_component_metric_exporters(
    component_services: &sinomo_module::ComponentServiceClient,
    request: &ObserveMetricsRequest,
    response: &mut ObserveMetricsResponse,
) {
    let exporter_request = ObserveMetricExportRequest {
        name_prefix: request.name_prefix.clone(),
        limit: if request.limit == 0 {
            OBSERVE_METRIC_EXPORTER_MAX_RECORDS
        } else {
            request.limit.min(OBSERVE_METRIC_EXPORTER_MAX_RECORDS)
        },
    };
    let calls = match component_services
        .call_local_all::<_, ObserveMetricExportResponse>(
            &observe_metric_exporter_requirement(),
            OBSERVE_METRIC_EXPORTER_OPERATION,
            &exporter_request,
            OBSERVE_COMPONENT_EXPORTER_DEADLINE,
            OBSERVE_COMPONENT_EXPORTER_MAX_RESPONSE_BYTES,
        )
        .await
    {
        Ok(calls) => calls,
        Err(error) => {
            response.partial = true;
            tracing::warn!(
                target: "sinomo.observe.exporter",
                error = %error,
                "component metric exporter resolution failed"
            );
            return;
        }
    };

    let mut descriptors = response
        .descriptors
        .drain(..)
        .map(|descriptor| (descriptor.name.clone(), descriptor))
        .collect::<std::collections::BTreeMap<_, _>>();
    for call in calls {
        match call {
            Ok(call) => {
                let exporter_name = component_metric_exporter_name(&call.provider);
                match merge_component_metric_export(
                    request,
                    &call.provider,
                    call.value,
                    &mut descriptors,
                    &mut response.metrics,
                ) {
                    Ok(()) => response.exporters.push(ObserveMetricExporterStatus {
                        name: exporter_name,
                        observed_at: OffsetDateTime::now_utc(),
                        duration_us: call.duration_us,
                        cached: false,
                        success: true,
                        error_code: None,
                    }),
                    Err(detail) => {
                        response.partial = true;
                        response.exporters.push(ObserveMetricExporterStatus {
                            name: exporter_name.clone(),
                            observed_at: OffsetDateTime::now_utc(),
                            duration_us: call.duration_us,
                            cached: false,
                            success: false,
                            error_code: Some("invalid-export".to_string()),
                        });
                        tracing::warn!(
                            target: "sinomo.observe.exporter",
                            exporter = %exporter_name,
                            error = %detail,
                            "component metric exporter returned an invalid batch"
                        );
                    }
                }
            }
            Err(error) => {
                response.partial = true;
                response.exporters.push(ObserveMetricExporterStatus {
                    name: component_metric_exporter_name(&error.provider),
                    observed_at: OffsetDateTime::now_utc(),
                    duration_us: error.duration_us,
                    cached: false,
                    success: false,
                    error_code: Some("component-call".to_string()),
                });
                tracing::warn!(
                    target: "sinomo.observe.exporter",
                    component_id = %error.provider.component_id,
                    route = %error.route,
                    error = %error.detail,
                    "component metric exporter call failed"
                );
            }
        }
    }

    response.metrics.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.labels.cmp(&right.labels))
    });
    if request.limit > 0 && response.metrics.len() > request.limit {
        response.metrics.truncate(request.limit);
    }
    let returned_names = response
        .metrics
        .iter()
        .map(|record| record.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    response.descriptors = descriptors
        .into_values()
        .filter(|descriptor| returned_names.contains(descriptor.name.as_str()))
        .collect();
}

fn component_metric_exporter_name(
    provider: &sinomo_core::service::ServiceDiscoveryIdentity,
) -> String {
    format!(
        "component:{}/{}",
        provider.component_id, provider.service_id
    )
}

fn merge_component_metric_export(
    request: &ObserveMetricsRequest,
    provider: &sinomo_core::service::ServiceDiscoveryIdentity,
    mut export: ObserveMetricExportResponse,
    descriptors: &mut std::collections::BTreeMap<String, ObserveMetricDescriptor>,
    metrics: &mut Vec<ObserveMetricRecord>,
) -> Result<(), String> {
    if export.metrics.len() > OBSERVE_METRIC_EXPORTER_MAX_RECORDS {
        return Err(format!(
            "returned {} records; maximum is {OBSERVE_METRIC_EXPORTER_MAX_RECORDS}",
            export.metrics.len()
        ));
    }
    let mut batch_descriptors = std::collections::BTreeMap::new();
    for mut descriptor in export.descriptors.drain(..) {
        validate_component_metric_name(&descriptor.name)?;
        if descriptor.help.len() > 4_096 {
            return Err(format!(
                "metric `{}` help exceeds 4096 bytes",
                descriptor.name
            ));
        }
        for label in OBSERVE_COMPONENT_PROVENANCE_LABELS {
            if !descriptor
                .semantics
                .labels
                .iter()
                .any(|existing| existing == label)
            {
                descriptor.semantics.labels.push((*label).to_string());
            }
        }
        descriptor.semantics.labels.sort();
        descriptor.semantics.labels.dedup();
        if !descriptor
            .semantics
            .aggregation
            .is_compatible_with(descriptor.kind)
        {
            descriptor.semantics.aggregation = ObserveMetricAggregation::None;
        }
        if let Some(existing) = descriptors
            .get(&descriptor.name)
            .or_else(|| batch_descriptors.get(&descriptor.name))
            && (existing.kind != descriptor.kind || existing.unit != descriptor.unit)
        {
            return Err(format!(
                "metric `{}` conflicts with an existing descriptor",
                descriptor.name
            ));
        }
        batch_descriptors.insert(descriptor.name.clone(), descriptor);
    }

    let mut accepted = Vec::new();
    let mut accepted_series = std::collections::BTreeSet::new();
    for mut record in export.metrics {
        validate_component_metric_record(&record)?;
        if request
            .name_prefix
            .as_ref()
            .is_some_and(|prefix| !record.name.starts_with(prefix))
        {
            continue;
        }
        if let Some(descriptor) = batch_descriptors
            .get(&record.name)
            .or_else(|| descriptors.get(&record.name))
            && descriptor.kind != record.kind
        {
            return Err(format!(
                "metric `{}` kind disagrees with its descriptor",
                record.name
            ));
        }
        record
            .labels
            .insert("component_id".to_string(), provider.component_id.clone());
        record.labels.insert(
            "component_version".to_string(),
            provider.component_version.clone(),
        );
        record.labels.insert(
            "component_service_id".to_string(),
            provider.service_id.clone(),
        );
        record.labels.insert(
            "component_generation".to_string(),
            provider.component_generation.to_string(),
        );
        if let Some(artifact_id) = &provider.artifact_id {
            record
                .labels
                .insert("component_artifact_id".to_string(), artifact_id.clone());
        }
        if !accepted_series.insert((record.name.clone(), record.labels.clone())) {
            return Err(format!(
                "metric `{}` repeats a label-identical series",
                record.name
            ));
        }
        accepted.push(record);
    }
    descriptors.extend(batch_descriptors);
    metrics.extend(accepted);
    Ok(())
}

fn validate_component_metric_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 256
        || name != name.trim()
        || !name.contains('.')
        || !name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
    {
        return Err(format!("invalid dotted metric name `{name}`"));
    }
    Ok(())
}

fn validate_component_metric_record(record: &ObserveMetricRecord) -> Result<(), String> {
    validate_component_metric_name(&record.name)?;
    if record.labels.len() > 32
        || record
            .labels
            .iter()
            .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 1_024)
    {
        return Err(format!(
            "metric `{}` has invalid or excessive labels",
            record.name
        ));
    }
    let finite = |value: Option<f64>| value.is_none_or(f64::is_finite);
    if !finite(record.value)
        || !finite(record.sum)
        || !finite(record.min)
        || !finite(record.max)
        || record.buckets.len() > 128
        || record
            .buckets
            .iter()
            .any(|bucket| !bucket.upper_bound.is_finite())
        || record.buckets.windows(2).any(|buckets| {
            buckets[0].upper_bound >= buckets[1].upper_bound || buckets[0].count > buckets[1].count
        })
    {
        return Err(format!(
            "metric `{}` has an invalid numeric value",
            record.name
        ));
    }
    match record.kind {
        ObserveMetricKind::Counter | ObserveMetricKind::Gauge if record.value.is_none() => {
            Err(format!("metric `{}` has no scalar value", record.name))
        }
        ObserveMetricKind::Counter if record.value.is_some_and(|value| value < 0.0) => {
            Err(format!("counter metric `{}` is negative", record.name))
        }
        ObserveMetricKind::Counter | ObserveMetricKind::Gauge
            if record.count.is_some()
                || record.sum.is_some()
                || record.min.is_some()
                || record.max.is_some()
                || !record.buckets.is_empty() =>
        {
            Err(format!(
                "scalar metric `{}` contains histogram fields",
                record.name
            ))
        }
        ObserveMetricKind::Histogram if record.count.is_none() || record.sum.is_none() => Err(
            format!("histogram metric `{}` has no count or sum", record.name),
        ),
        _ => Ok(()),
    }
}

fn observe_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = observe_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("observe module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown observe service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_concurrency_limit(8)
}

fn service_error(error: ObserveError) -> ServiceError {
    ServiceError::Service(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use sinomo_core::network::{
        ObjectRpcIncomingBody, ObjectRpcOutgoingResponse, ObjectRpcRequest,
    };
    use sinomo_core::service::{
        ResponseSink, ServiceContext, ServiceDiscoveryHealth, ServiceDiscoveryIdentity,
        ServiceDiscoveryLocality, ServiceRegistry, ServiceSigner,
    };

    struct FixtureMetricExporterService;

    impl Service for FixtureMetricExporterService {
        fn name(&self) -> &'static str {
            "fixture-metric-exporter"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/fixture/metrics/export"]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![ServiceRoute::from_route_metadata(
                "/fixture/metrics/export",
                sinomo_api::observe::OBSERVE_METRIC_EXPORTER_CATEGORY,
                "component",
                [OBSERVE_METRIC_EXPORTER_OPERATION],
            )]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let _: ObserveMetricExportRequest = ctx.decode_request_body(&request).await?;
                ctx.respond_value(
                    request.route,
                    ObserveMetricExportResponse {
                        descriptors: vec![ObserveMetricDescriptor::new(
                            "fixture.jobs.completed",
                            ObserveMetricKind::Counter,
                            Some(ObserveMetricUnit::Count),
                            "Completed fixture jobs.",
                        )],
                        metrics: vec![ObserveMetricRecord::counter(
                            "fixture.jobs.completed",
                            std::collections::BTreeMap::new(),
                            9,
                        )],
                    },
                )?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    struct TestResponseSink {
        response: Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>,
    }

    impl ResponseSink for TestResponseSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            *self
                .response
                .lock()
                .map_err(|_| ServiceError::Service("test response lock poisoned".to_string()))? =
                Some(response);
            Ok(())
        }
    }

    #[tokio::test]
    async fn observe_pulls_external_component_metrics_through_the_live_registry() {
        let provider_descriptor =
            sinomo_module::ComponentDescriptor::new("fixture.metrics.provider", "1.0.0")
                .with_offer(
                    sinomo_module::ServiceOffer::new(
                        "fixture-metrics",
                        OBSERVE_METRIC_EXPORTER_INTERFACE_ID,
                        OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION,
                    )
                    .with_operation(
                        sinomo_module::ServiceOperationOffer::new(
                            OBSERVE_METRIC_EXPORTER_OPERATION,
                        )
                        .at_route("/fixture/metrics/export")
                        .require_caller_capability(OBSERVE_METRIC_EXPORTER_OPERATION),
                    ),
                );
        let provider: Arc<dyn sinomo_module::ComponentFactory> = Arc::new(
            sinomo_module::StaticComponentFactory::new(provider_descriptor, |_, scope| {
                scope.set_services(
                    ServiceRegistry::builder()
                        .register(FixtureMetricExporterService)
                        .build(),
                );
                Ok(())
            })
            .expect("provider factory"),
        );
        let observe: Arc<dyn sinomo_module::ComponentFactory> =
            Arc::new(component_factory().expect("observe factory"));
        let data_dir = Arc::new(sinomo_core::local::SinomoDataDir::new(
            sinomo_testdir::TestDir::new("observe-component-exporter").path(),
        ));
        let mut reconciler = sinomo_module::ComponentReconciler::new(
            sinomo_module::ComponentContext::new(Arc::clone(&data_dir)),
            ServiceRegistry::default(),
        );
        reconciler
            .reconcile(&[provider, observe])
            .expect("reconcile observe and exporter");

        let response = Arc::new(Mutex::new(None));
        let context = ServiceContext::new(
            data_dir,
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(TestResponseSink {
                response: Arc::clone(&response),
            }),
        );
        let request = ObjectRpcRequest::encode_body(
            OBSERVE_METRICS_ROUTE,
            OBSERVE_RPC_CATEGORY,
            &ObserveMetricsRequest::default(),
        )
        .expect("metrics request");
        reconciler
            .service_registry()
            .active_snapshot()
            .registry_arc()
            .dispatch(context, request)
            .await
            .expect("dispatch observe metrics");
        let outgoing = response
            .lock()
            .expect("response lock")
            .take()
            .expect("observe response");
        let response: ObserveMetricsResponse = outgoing
            .into_materialized_value_response(OBSERVE_COMPONENT_EXPORTER_MAX_RESPONSE_BYTES)
            .await
            .expect("materialize response")
            .decode_body()
            .expect("decode response");
        let metric = response
            .metrics
            .iter()
            .find(|metric| metric.name == "fixture.jobs.completed")
            .expect("external metric");
        assert_eq!(metric.value, Some(9.0));
        assert_eq!(metric.labels["component_id"], "fixture.metrics.provider");
        assert!(response.exporters.iter().any(|exporter| {
            exporter.name == "component:fixture.metrics.provider/fixture-metrics"
                && exporter.success
        }));
    }

    #[test]
    fn observe_component_declares_optional_pull_exporter_dependency() {
        let factory = component_factory().expect("observe component factory");
        let descriptor = sinomo_module::ComponentFactory::descriptor(&factory);
        assert!(descriptor.requirements.iter().any(|requirement| {
            requirement.optional
                && requirement.interface_id == OBSERVE_METRIC_EXPORTER_INTERFACE_ID
                && requirement.interface_version == OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION
                && requirement
                    .operations
                    .iter()
                    .any(|operation| operation == OBSERVE_METRIC_EXPORTER_OPERATION)
                && requirement
                    .caller_capabilities
                    .iter()
                    .any(|capability| capability == OBSERVE_METRIC_EXPORTER_OPERATION)
        }));
    }

    #[test]
    fn component_metric_exports_receive_trusted_provider_labels() {
        let provider = ServiceDiscoveryIdentity {
            service_id: "fixture-metrics".to_string(),
            interface_id: OBSERVE_METRIC_EXPORTER_INTERFACE_ID.to_string(),
            interface_version: OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION.to_string(),
            provider_capabilities: vec![OBSERVE_METRIC_EXPORTER_OPERATION.to_string()],
            caller_requirements: vec![OBSERVE_METRIC_EXPORTER_OPERATION.to_string()],
            component_id: "fixture.metrics".to_string(),
            component_version: "2.0.0".to_string(),
            artifact_id: Some("fixture-artifact".to_string()),
            component_generation: 7,
            health: ServiceDiscoveryHealth::Ready,
            locality: ServiceDiscoveryLocality::Local,
        };
        let mut descriptors = std::collections::BTreeMap::new();
        let mut metrics = Vec::new();
        merge_component_metric_export(
            &ObserveMetricsRequest::default(),
            &provider,
            ObserveMetricExportResponse {
                descriptors: vec![ObserveMetricDescriptor::new(
                    "fixture.jobs.completed",
                    ObserveMetricKind::Counter,
                    Some(ObserveMetricUnit::Count),
                    "Completed fixture jobs.",
                )],
                metrics: vec![ObserveMetricRecord::counter(
                    "fixture.jobs.completed",
                    std::collections::BTreeMap::from([(
                        "component_id".to_string(),
                        "spoofed".to_string(),
                    )]),
                    3,
                )],
            },
            &mut descriptors,
            &mut metrics,
        )
        .expect("merge component metric export");

        assert_eq!(metrics[0].labels["component_id"], "fixture.metrics");
        assert_eq!(metrics[0].labels["component_generation"], "7");
        assert!(
            descriptors["fixture.jobs.completed"]
                .semantics
                .labels
                .contains(&"component_id".to_string())
        );
    }

    #[test]
    fn component_events_feed_operational_event_and_log_views() {
        let root = sinomo_testdir::TestDir::new("observe-component-events");
        let component_event = |kind: &str, message: &str| sinomo_module::ComponentEvent {
            publisher: sinomo_module::ComponentEventPublisher {
                component_id: "fixture.component".to_string(),
                component_version: "1.0.0".to_string(),
                artifact_id: None,
                artifact_generation: 0,
            },
            schema_id: "sinomo.operational-event".to_string(),
            schema_version: "1.0.0".to_string(),
            envelope: EventEnvelope::new(
                Id::generate(),
                OffsetDateTime::now_utc(),
                Id::from_legacy_named("fixture-node").expect("node"),
                Source::new("component/fixture.component").expect("source"),
                EventKind::new(kind).expect("kind"),
                Salience::new(0.5).expect("salience"),
                Confidence::new(1.0).expect("confidence"),
                RoutingMetadata::new(
                    RoutingScope::Local,
                    LatencyClass::Background,
                    PrivacyClass::Private,
                ),
            )
            .with_payload(serde_json::json!({
                "severity": "warn",
                "component": "fixture.component",
                "message": message,
                "fields": {"attempt": 2},
            })),
        };

        append_component_event_record(
            root.path(),
            &component_event("fixture.operation.failed", "operation failed"),
        )
        .expect("append component event");
        append_component_event_record(
            root.path(),
            &component_event("daemon.log.recorded", "log message"),
        )
        .expect("append component log");

        let events = read_operational_events(
            root.path(),
            &OperationalEventQuery {
                limit: 10,
                ..OperationalEventQuery::default()
            },
        )
        .expect("read events");
        let logs = read_operational_logs(
            root.path(),
            &OperationalLogQuery {
                limit: 10,
                ..OperationalLogQuery::default()
            },
        )
        .expect("read logs");
        assert_eq!(events[0].message, "operation failed");
        assert_eq!(events[0].severity, OperationalSeverity::Warn);
        assert_eq!(logs[0].message, "log message");
        assert_eq!(logs[0].fields["attempt"], 2);
    }

    #[test]
    fn cpu_profile_artifacts_are_registered_in_the_storage_contract() {
        let mut layout = sinomo_core::storage::StorageLayout::new();
        register_storage_paths(&mut layout).expect("register observe storage paths");

        for relative_path in [
            "observe/profiles/capture/cpu.pprof.pb",
            "observe/profiles/capture/manifest.json",
        ] {
            let owner = layout
                .owner_for(relative_path)
                .unwrap_or_else(|| panic!("missing storage owner for {relative_path}"));
            assert!(matches!(
                owner,
                sinomo_core::storage::StoragePathOwner::Escape(_)
            ));
        }
    }

    #[test]
    fn observe_service_route_metadata_matches_module_surfaces() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let service_routes = registry
            .route_categories()
            .into_iter()
            .map(|route| (route.route.clone(), route))
            .collect::<std::collections::BTreeMap<_, _>>();
        let module_routes = observe_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("observe module surfaces")
            .into_iter()
            .map(|descriptor| (descriptor.route.clone(), descriptor))
            .collect::<std::collections::BTreeMap<_, _>>();

        for route in OBSERVE_ROUTES {
            let service_route = service_routes
                .get(*route)
                .unwrap_or_else(|| panic!("service registry should own observe route {route}"));
            let module_route = module_routes
                .get(*route)
                .unwrap_or_else(|| panic!("module surfaces should declare observe route {route}"));
            assert_eq!(service_route.category, module_route.category);
            assert_eq!(
                service_route.auth_scope.as_deref(),
                Some(module_route.auth_scope.as_str())
            );
            assert_eq!(
                service_route.required_capabilities,
                module_route
                    .required_capabilities
                    .iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect::<Vec<_>>()
            );
            assert_eq!(service_route.concurrency_limit, Some(8));
        }
    }

    #[test]
    fn operational_events_are_filterable_and_limited() {
        let root = unique_temp("sinomo-daemon-operational-events");
        append_operational_event(
            &root,
            None,
            OperationalSeverity::Info,
            "daemon-control",
            "daemon.lifecycle.started",
            "daemon started",
            Some("op-a"),
            serde_json::json!({"address": "127.0.0.1:21353"}),
        )
        .expect("append first event");
        append_operational_event(
            &root,
            None,
            OperationalSeverity::Warn,
            "rollout",
            "rollout.stage.progress",
            "rollout build",
            Some("op-b"),
            serde_json::json!({"phase": "build"}),
        )
        .expect("append second event");

        let records = read_operational_events(
            &root,
            &OperationalEventQuery {
                kind_prefix: Some("rollout.".to_string()),
                limit: 10,
                ..OperationalEventQuery::default()
            },
        )
        .expect("read rollout events");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].component, "rollout");
        assert_eq!(records[0].operation_id.as_deref(), Some("op-b"));

        let records = read_operational_events(
            &root,
            &OperationalEventQuery {
                limit: 1,
                ..OperationalEventQuery::default()
            },
        )
        .expect("read limited events");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event.kind.as_str(), "rollout.stage.progress");
    }

    #[test]
    fn operational_events_recover_zero_filled_windows_append_hole() {
        let root = unique_temp("sinomo-daemon-operational-zero-hole");
        let path = operational_events_path(&root);
        append_operational_event(
            &root,
            None,
            OperationalSeverity::Info,
            "daemon-control",
            "daemon.lifecycle.started",
            "first daemon start",
            None,
            serde_json::Value::Null,
        )
        .expect("append first event");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open event log")
            .write_all(&[0; 512])
            .expect("write sparse-hole equivalent");
        append_operational_event(
            &root,
            None,
            OperationalSeverity::Info,
            "daemon-control",
            "daemon.lifecycle.started",
            "second daemon start",
            None,
            serde_json::Value::Null,
        )
        .expect("append second event");

        let records = read_operational_events(
            &root,
            &OperationalEventQuery {
                limit: 10,
                ..OperationalEventQuery::default()
            },
        )
        .expect("read events around sparse hole");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].message, "first daemon start");
        assert_eq!(records[1].message, "second daemon start");
    }

    #[test]
    fn operational_events_reject_trailing_zero_corruption() {
        let root = unique_temp("sinomo-daemon-operational-trailing-zero");
        let path = operational_events_path(&root);
        let record = append_operational_event(
            &root,
            None,
            OperationalSeverity::Info,
            "daemon-control",
            "daemon.lifecycle.started",
            "valid daemon start",
            None,
            serde_json::Value::Null,
        )
        .expect("append valid event");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open event log");
        serde_json::to_writer(&mut file, &record).expect("append corrupt event payload");
        file.write_all(b"\0\n")
            .expect("append trailing zero corruption");

        let error = read_operational_events(
            &root,
            &OperationalEventQuery {
                limit: 10,
                ..OperationalEventQuery::default()
            },
        )
        .expect_err("trailing zero must remain invalid JSON");

        assert!(matches!(error, ObserveError::Json { .. }));
    }

    #[test]
    fn operational_logs_are_filterable() {
        let root = unique_temp("sinomo-daemon-operational-logs");
        append_operational_log(
            &root,
            OperationalSeverity::Debug,
            "cluster-network",
            "probe completed",
            None,
            serde_json::json!({"reachable": true}),
        )
        .expect("append first log");
        append_operational_log(
            &root,
            OperationalSeverity::Error,
            "shadow-daemon",
            "main crashed",
            Some("supervision"),
            serde_json::json!({"exit": 1}),
        )
        .expect("append second log");

        let records = read_operational_logs(
            &root,
            &OperationalLogQuery {
                component: Some("shadow-daemon".to_string()),
                limit: 10,
                ..OperationalLogQuery::default()
            },
        )
        .expect("read logs");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].severity, OperationalSeverity::Error);
        assert_eq!(records[0].operation_id.as_deref(), Some("supervision"));
    }

    #[test]
    fn jsonl_rotation_retains_bounded_tail_across_shards() {
        let root = unique_temp("sinomo-daemon-operational-rotation");
        let path = operational_logs_path(&root);
        let rotation = JsonlRotationConfig {
            max_bytes: 1,
            retained_files: 2,
        };
        for index in 0..5 {
            append_jsonl_record_with_config(
                &path,
                &OperationalLogRecord {
                    source: ObserveLogSource::Operational,
                    truncated: false,
                    timestamp: OffsetDateTime::now_utc(),
                    severity: OperationalSeverity::Info,
                    component: "test".to_string(),
                    message: format!("record-{index}"),
                    operation_id: None,
                    fields: serde_json::Value::Null,
                },
                rotation,
            )
            .expect("append rotated record");
        }

        assert!(path.exists());
        assert!(rotated_jsonl_path(&path, 1).exists());
        assert!(rotated_jsonl_path(&path, 2).exists());
        assert!(!rotated_jsonl_path(&path, 3).exists());

        let records =
            read_jsonl_tail_with_config::<OperationalLogRecord, _>(path, 10, rotation, |record| {
                record.component == "test"
            })
            .expect("read rotated records");
        let messages = records
            .into_iter()
            .map(|record| record.message)
            .collect::<Vec<_>>();
        assert_eq!(messages, vec!["record-2", "record-3", "record-4"]);
    }

    #[test]
    fn disabled_module_suppresses_conditional_recording() {
        let root = unique_temp("sinomo-observe-disabled");
        assert!(observe_module_enabled(&root).expect("default enabled"));
        set_observe_module_enabled(&root, false, "test", "disable observe")
            .expect("write disabled state");
        assert!(!observe_module_enabled(&root).expect("disabled"));

        let record = append_operational_log_if_enabled(
            &root,
            OperationalSeverity::Info,
            "test",
            "suppressed",
            None,
            serde_json::Value::Null,
        )
        .expect("conditional append");
        assert!(record.is_none());
        assert!(!operational_logs_path(&root).exists());

        set_observe_module_enabled(&root, true, "test", "enable observe")
            .expect("write enabled state");
        let record = append_operational_log_if_enabled(
            &root,
            OperationalSeverity::Info,
            "test",
            "recorded",
            None,
            serde_json::Value::Null,
        )
        .expect("conditional append")
        .expect("record");
        assert_eq!(record.message, "recorded");
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }
}
