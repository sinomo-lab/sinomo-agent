//! Operational events, logs, and object-RPC observation service.

mod cli;
mod runtime;

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

pub use cli::{
    ObserveCliCommandHandler, observe_cli_command_handler,
    observe_cli_command_handler_with_cluster_rpc, observe_cli_command_handler_with_rpc,
};
pub use runtime::{
    OBSERVE_BUILT_IN_QUIET_FILTER_DIRECTIVES, OBSERVE_DEFAULT_BASE_FILTER, OBSERVE_DEFAULT_FILTER,
    OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES, ObserveControlRequest, ObserveControlResponse,
    ObserveMetricKind, ObserveMetricRecord, ObserveMetricsRequest, ObserveMetricsResponse,
    ObserveRuntimeConfig, ObserveTraceRecord, ObserveTracesRequest, ObserveTracesResponse,
    init_observe_runtime, init_observe_runtime_with_log_file, observe_control_snapshot,
    observe_filter_with_builtin_quiet_directives, observe_filter_with_quiet_directives,
    read_observe_metrics, read_observe_metrics_for_data_dir, read_observe_traces,
    register_observe_metric_exporter, update_observe_control,
};

pub const OBSERVE_PROTOCOL: &str = "sinomo.observe.v1";
pub const SINOMO_OBSERVE_MODULE_LEGACY_ID: &str = "module.observe";
pub const SINOMO_OBSERVE_MODULE_OWNER: &str = "sinomo-observe";
pub const SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY: &str = "sinomo-observe";
pub const OBSERVE_RPC_CATEGORY: &str = "observe";
pub const OBSERVE_EVENTS_ROUTE: &str = "/observe/events";
pub const OBSERVE_LOGS_ROUTE: &str = "/observe/logs";
pub const OBSERVE_METRICS_ROUTE: &str = "/observe/metrics";
pub const OBSERVE_TRACES_ROUTE: &str = "/observe/traces";
pub const OBSERVE_CONTROL_ROUTE: &str = "/observe/control";
pub const OBSERVE_OBJECT_RPC_ROUTES: &[&str] = &[
    OBSERVE_EVENTS_ROUTE,
    OBSERVE_LOGS_ROUTE,
    OBSERVE_METRICS_ROUTE,
    OBSERVE_TRACES_ROUTE,
    OBSERVE_CONTROL_ROUTE,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum OperationalSeverity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl OperationalSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    fn salience(self) -> Salience {
        let value = match self {
            Self::Trace => 0.1,
            Self::Debug => 0.2,
            Self::Info => 0.4,
            Self::Warn => 0.75,
            Self::Error => 0.95,
        };
        Salience::new(value).expect("operational salience is in range")
    }
}

impl std::fmt::Display for OperationalSeverity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OperationalLogRecord {
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub severity: OperationalSeverity,
    pub component: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub fields: serde_json::Value,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveLogsRequest {
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveLogsResponse {
    pub records: Vec<OperationalLogRecord>,
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
        severity.salience(),
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
    lock.lock_exclusive()
        .map_err(|source| lock_error(path, source))?;
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
    let unlock = lock.unlock().map_err(|source| lock_error(path, source));
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
    lock.lock_shared()
        .map_err(|source| lock_error(&path, source))?;
    let result = (|| {
        let mut records = VecDeque::new();
        for path in jsonl_read_paths_chronological(&path, rotation.retained_files) {
            read_jsonl_file_into_tail(&path, limit, &mut keep, &mut records)?;
        }
        Ok(records.into_iter().collect())
    })();
    let unlock = lock.unlock().map_err(|source| lock_error(&path, source));
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
    );
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
        ],
        exports: vec![
            observe_capability("observe.events.read"),
            observe_capability("observe.logs.read"),
            observe_capability("observe.metrics.read"),
            observe_capability("observe.traces.read"),
            observe_capability("observe.control.write"),
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
    Ok(())
}

#[derive(Default)]
pub struct ObserveService;

impl ObserveService {
    pub fn new() -> Self {
        Self
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(ObserveService::new())
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
        Box::pin(async move {
            let response = match request.route.as_str() {
                OBSERVE_EVENTS_ROUTE => {
                    let request: ObserveEventsRequest = request.decode_body()?;
                    let records = read_operational_events(ctx.data_dir.root(), &request.into())
                        .map_err(service_error)?;
                    sinomo_core::network::ObjectRpcResponse::encode_body(
                        OBSERVE_EVENTS_ROUTE,
                        &ObserveEventsResponse { records },
                    )?
                }
                OBSERVE_LOGS_ROUTE => {
                    let request: ObserveLogsRequest = request.decode_body()?;
                    let records = read_operational_logs(ctx.data_dir.root(), &request.into())
                        .map_err(service_error)?;
                    sinomo_core::network::ObjectRpcResponse::encode_body(
                        OBSERVE_LOGS_ROUTE,
                        &ObserveLogsResponse { records },
                    )?
                }
                OBSERVE_METRICS_ROUTE => {
                    let request: ObserveMetricsRequest = request.decode_body()?;
                    sinomo_core::network::ObjectRpcResponse::encode_body(
                        OBSERVE_METRICS_ROUTE,
                        &read_observe_metrics_for_data_dir(ctx.data_dir.as_ref(), &request),
                    )?
                }
                OBSERVE_TRACES_ROUTE => {
                    let request: ObserveTracesRequest = request.decode_body()?;
                    sinomo_core::network::ObjectRpcResponse::encode_body(
                        OBSERVE_TRACES_ROUTE,
                        &read_observe_traces(&request),
                    )?
                }
                OBSERVE_CONTROL_ROUTE => {
                    let request: ObserveControlRequest = request.decode_body()?;
                    sinomo_core::network::ObjectRpcResponse::encode_body(
                        OBSERVE_CONTROL_ROUTE,
                        &update_observe_control(request).map_err(service_error)?,
                    )?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
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
    use sinomo_core::service::ServiceRegistry;

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
