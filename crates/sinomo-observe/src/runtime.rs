use crate::ObserveError;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use serde::{Deserialize, Serialize};
use sinomo_core::local::SinomoDataDir;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tracing::{Event, Subscriber, field};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry, fmt};

pub const OBSERVE_DEFAULT_BASE_FILTER: &str = "warn,sinomo=info";
pub const OBSERVE_DEFAULT_FILTER: &str = "warn,sinomo=info,iroh::net_report::report=error";
pub const OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES: &[&str] = &["iroh::net_report::report=error"];
pub const OBSERVE_BUILT_IN_QUIET_FILTER_DIRECTIVES: &[&str] = &["iroh::net_report::report=error"];
const TRACE_BUFFER_LIMIT: usize = 4096;
const CAPTURE_ID_MAX_BYTES: usize = 128;
const CAPTURE_LEASE: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserveRuntimeConfig {
    pub process_role: String,
    pub node_id: Option<String>,
    pub filter: String,
    pub quiet_filter_directives: Vec<String>,
}

impl ObserveRuntimeConfig {
    pub fn daemon(label: impl Into<String>, node_id: Option<String>) -> Self {
        Self::process(label, node_id)
    }

    /// Config for any Sinomo process role (daemon, graphical app, tooling), with the
    /// filter taken from `SINOMO_OBSERVE_FILTER` when set.
    pub fn process(label: impl Into<String>, node_id: Option<String>) -> Self {
        let configured_filter = std::env::var("SINOMO_OBSERVE_FILTER")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| OBSERVE_DEFAULT_BASE_FILTER.to_string());
        Self {
            process_role: label.into(),
            node_id,
            filter: configured_filter,
            quiet_filter_directives: observe_quiet_filter_directives_from_env(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveControlRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_filter_directives: Option<Vec<String>>,
    #[serde(default)]
    pub clear_quiet_filter_directives: bool,
    #[serde(default)]
    pub reset: bool,
    /// Tag newly recorded trace events with a cluster-wide capture identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    /// Stop tagging newly recorded trace events with the active capture.
    #[serde(default)]
    pub clear_capture: bool,
    /// Internal compare-and-clear guard used when rolling back a partial
    /// cluster-wide capture start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_capture_id: Option<String>,
    /// Clear the bounded in-memory trace buffer before starting a capture.
    #[serde(default)]
    pub clear_traces: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveControlResponse {
    pub active_filter: String,
    pub base_filter: String,
    pub quiet_filter_directives: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_capture_id: Option<String>,
    #[serde(default)]
    pub cleared_trace_count: usize,
    pub changed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_prefix: Option<String>,
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveMetricsResponse {
    pub metrics: Vec<ObserveMetricRecord>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveTracesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveTracesResponse {
    pub records: Vec<ObserveTraceRecord>,
    /// Whether the requested capture is still active on this process. `None`
    /// means the request did not filter by capture ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_active: Option<bool>,
    /// Records evicted from the bounded ring since it was last cleared.
    #[serde(default)]
    pub dropped_records: u64,
    /// True when either the ring overflowed or this response's limit omitted
    /// otherwise available records.
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveMetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl ObserveMetricKind {
    fn as_key_prefix(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveMetricRecord {
    pub name: String,
    pub kind: ObserveMetricKind,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

impl ObserveMetricRecord {
    pub fn counter(name: impl Into<String>, labels: BTreeMap<String, String>, value: u64) -> Self {
        Self {
            name: name.into(),
            kind: ObserveMetricKind::Counter,
            labels,
            value: Some(value as f64),
            count: None,
            sum: None,
            min: None,
            max: None,
        }
    }

    pub fn gauge(name: impl Into<String>, labels: BTreeMap<String, String>, value: f64) -> Self {
        Self {
            name: name.into(),
            kind: ObserveMetricKind::Gauge,
            labels,
            value: Some(value),
            count: None,
            sum: None,
            min: None,
            max: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveTraceRecord {
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub level: String,
    pub target: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub fields: serde_json::Value,
}

#[derive(Clone)]
struct ObserveRuntime {
    config: ObserveRuntimeConfig,
    filter: Arc<Mutex<ObserveFilterState>>,
    filter_handle: reload::Handle<EnvFilter, Registry>,
    metrics: Arc<ObserveMetricsRecorder>,
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
    dropped_traces: Arc<AtomicU64>,
    capture: Arc<Mutex<Option<ActiveObserveCapture>>>,
    control_update: Arc<Mutex<()>>,
}

#[derive(Clone, Debug)]
struct ActiveObserveCapture {
    id: String,
    expires_at: Instant,
    restore_filter: Option<ObserveFilterState>,
}

impl ActiveObserveCapture {
    fn new(id: String, restore_filter: Option<ObserveFilterState>) -> Self {
        Self {
            id,
            expires_at: Instant::now() + CAPTURE_LEASE,
            restore_filter,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObserveFilterState {
    base_filter: String,
    quiet_filter_directives: Vec<String>,
    active_filter: String,
}

static OBSERVE_RUNTIME: OnceLock<ObserveRuntime> = OnceLock::new();
type ObserveMetricExporter =
    Arc<dyn Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError> + Send + Sync>;
static OBSERVE_METRIC_EXPORTERS: OnceLock<Mutex<BTreeMap<String, ObserveMetricExporter>>> =
    OnceLock::new();

pub fn register_observe_metric_exporter(
    name: impl Into<String>,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    observe_metric_exporters()
        .lock()
        .expect("observe metric exporters lock poisoned")
        .insert(name.into(), Arc::new(exporter));
}

pub fn init_observe_runtime(config: ObserveRuntimeConfig) -> Result<(), ObserveError> {
    init_observe_runtime_inner(config, None)
}

/// Initialize the observe runtime with formatted trace output additionally copied to
/// `log_file` (created/appended, ANSI disabled). Graphical processes use this: they
/// have no supervised stdout, so without a file sink their diagnostics are lost.
pub fn init_observe_runtime_with_log_file(
    config: ObserveRuntimeConfig,
    log_file: &std::path::Path,
) -> Result<(), ObserveError> {
    if OBSERVE_RUNTIME.get().is_some() {
        return Ok(());
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .map_err(|source| ObserveError::Runtime {
            detail: format!(
                "failed to open observe log file {}: {source}",
                log_file.display()
            ),
        })?;
    init_observe_runtime_inner(config, Some(Arc::new(file)))
}

fn init_observe_runtime_inner(
    config: ObserveRuntimeConfig,
    log_file: Option<Arc<std::fs::File>>,
) -> Result<(), ObserveError> {
    if OBSERVE_RUNTIME.get().is_some() {
        return Ok(());
    }

    let initial_state = ObserveFilterState::new(
        config.filter.clone(),
        config.quiet_filter_directives.clone(),
    );
    let filter = EnvFilter::try_new(initial_state.active_filter.as_str()).map_err(|source| {
        ObserveError::Runtime {
            detail: format!(
                "invalid observe filter `{}`: {source}",
                initial_state.active_filter
            ),
        }
    })?;
    let (filter_layer, filter_handle) = reload::Layer::new(filter);
    let metrics = Arc::new(ObserveMetricsRecorder::default());
    let traces = Arc::new(Mutex::new(VecDeque::with_capacity(TRACE_BUFFER_LIMIT)));
    let dropped_traces = Arc::new(AtomicU64::new(0));
    let capture = Arc::new(Mutex::new(None));
    let runtime = ObserveRuntime {
        filter: Arc::new(Mutex::new(initial_state)),
        filter_handle,
        metrics: Arc::clone(&metrics),
        traces: Arc::clone(&traces),
        dropped_traces: Arc::clone(&dropped_traces),
        capture: Arc::clone(&capture),
        control_update: Arc::new(Mutex::new(())),
        config,
    };

    let _ = metrics::set_global_recorder(Arc::clone(&metrics));
    let _ = OBSERVE_RUNTIME.set(runtime);

    let file_layer = log_file.map(|file| {
        fmt::layer()
            .with_target(true)
            .with_ansi(false)
            .with_writer(file)
    });
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(ObserveTraceLayer {
            traces,
            dropped_traces,
            capture,
        })
        .with(fmt::layer().with_target(true))
        .with(file_layer)
        .try_init()
        .map_err(|source| ObserveError::Runtime {
            detail: format!("failed to initialize observe tracing subscriber: {source}"),
        })
        .or_else(|_| Ok(()))
}

pub fn update_observe_control(
    request: ObserveControlRequest,
) -> Result<ObserveControlResponse, ObserveError> {
    let runtime = OBSERVE_RUNTIME.get().ok_or_else(|| ObserveError::Runtime {
        detail: "observe runtime is not initialized".to_string(),
    })?;
    if request.clear_quiet_filter_directives && request.quiet_filter_directives.is_some() {
        return Err(ObserveError::Runtime {
            detail: "observe control cannot both set and clear quiet filter directives".to_string(),
        });
    }
    if request.clear_capture && request.capture_id.is_some() {
        return Err(ObserveError::Runtime {
            detail: "observe control cannot both set and clear the active capture".to_string(),
        });
    }
    if request.expected_capture_id.is_some() && !request.clear_capture {
        return Err(ObserveError::Runtime {
            detail: "observe expected capture id requires clear_capture".to_string(),
        });
    }
    if request.clear_capture && request.changes_filter_state() {
        return Err(ObserveError::Runtime {
            detail: "observe capture clearing and filter changes must be separate control requests"
                .to_string(),
        });
    }
    if let Some(capture_id) = request.capture_id.as_deref() {
        if capture_id.trim().is_empty() {
            return Err(ObserveError::Runtime {
                detail: "observe capture id cannot be empty".to_string(),
            });
        }
        if capture_id.len() > CAPTURE_ID_MAX_BYTES {
            return Err(ObserveError::Runtime {
                detail: format!("observe capture id exceeds {CAPTURE_ID_MAX_BYTES} bytes"),
            });
        }
    }
    if !request.changes_runtime_state() {
        return Ok(runtime.control_snapshot(false, 0));
    }

    let _control_update = runtime
        .control_update
        .lock()
        .map_err(|_| ObserveError::Runtime {
            detail: "observe control update lock is poisoned".to_string(),
        })?;
    if let Some(restore_filter) = runtime.take_expired_capture_restore()? {
        runtime.install_filter_state(restore_filter)?;
    }
    let previous_filter_state = runtime.try_current_filter_state()?;
    let capture = runtime.capture.lock().map_err(|_| ObserveError::Runtime {
        detail: "observe capture state lock is poisoned".to_string(),
    })?;
    if let Some(requested_capture_id) = request.capture_id.as_ref()
        && let Some(active_capture) = capture.as_ref()
        && active_capture.id != *requested_capture_id
    {
        return Err(ObserveError::Runtime {
            detail: format!(
                "observe capture `{}` is already active; clear it before starting `{requested_capture_id}`",
                active_capture.id
            ),
        });
    }
    if request.clear_capture
        && let Some(expected_capture_id) = request.expected_capture_id.as_ref()
        && let Some(active_capture) = capture.as_ref()
        && active_capture.id != *expected_capture_id
    {
        return Err(ObserveError::Runtime {
            detail: format!(
                "observe capture `{}` replaced `{expected_capture_id}` before it could be cleared",
                active_capture.id
            ),
        });
    }
    drop(capture);

    if request.changes_filter_state() {
        let next_state = if request.reset {
            ObserveFilterState::default()
        } else {
            let base_filter = request
                .filter
                .clone()
                .unwrap_or_else(|| previous_filter_state.base_filter.clone());
            let quiet_filter_directives = if request.clear_quiet_filter_directives {
                Vec::new()
            } else {
                request
                    .quiet_filter_directives
                    .clone()
                    .unwrap_or_else(|| previous_filter_state.quiet_filter_directives.clone())
            };
            ObserveFilterState::new(base_filter, quiet_filter_directives)
        };
        runtime.install_filter_state(next_state)?;
    }

    let mut capture = runtime.capture.lock().map_err(|_| ObserveError::Runtime {
        detail: "observe capture state lock is poisoned".to_string(),
    })?;
    // Capture activation and buffer clearing are atomic relative to the trace
    // layer (which takes the same capture -> traces lock order). No event can
    // acquire the new capture tag and then be erased by this start request.
    let cleared_trace_count = if request.clear_traces {
        let cleared = runtime
            .traces
            .lock()
            .map_err(|_| ObserveError::Runtime {
                detail: "observe trace buffer lock is poisoned".to_string(),
            })
            .map(|mut traces| {
                let count = traces.len();
                traces.clear();
                count
            })?;
        runtime.dropped_traces.store(0, Ordering::Relaxed);
        cleared
    } else {
        0
    };
    let capture_owns_filter = request.changes_filter_state();
    let mut schedule_capture_expiry = false;
    let restore_filter = if request.clear_capture {
        capture
            .take()
            .and_then(|active_capture| active_capture.restore_filter)
    } else if let Some(capture_id) = request.capture_id.as_ref() {
        if let Some(active_capture) = capture
            .as_mut()
            .filter(|active_capture| active_capture.id == *capture_id)
        {
            if active_capture.restore_filter.is_none() && capture_owns_filter {
                active_capture.restore_filter = Some(previous_filter_state.clone());
            }
        } else {
            let restore_filter = capture_owns_filter.then(|| previous_filter_state.clone());
            *capture = Some(ActiveObserveCapture::new(
                capture_id.clone(),
                restore_filter,
            ));
            schedule_capture_expiry = true;
        }
        None
    } else {
        None
    };
    let capture_expiry = schedule_capture_expiry.then(|| {
        let capture = capture.as_ref().expect("new capture is active");
        (capture.id.clone(), capture.expires_at)
    });
    drop(capture);
    if let Some(restore_filter) = restore_filter {
        runtime.install_filter_state(restore_filter)?;
    }
    if let Some((capture_id, expires_at)) = capture_expiry {
        if let Err(error) = runtime.schedule_capture_expiry(capture_id.clone(), expires_at) {
            let restore_filter = runtime
                .capture
                .lock()
                .ok()
                .and_then(|mut capture| {
                    (capture
                        .as_ref()
                        .is_some_and(|capture| capture.id == capture_id))
                    .then(|| capture.take())
                    .flatten()
                })
                .and_then(|capture| capture.restore_filter);
            if let Some(restore_filter) = restore_filter {
                runtime.install_filter_state(restore_filter)?;
            }
            return Err(error);
        }
    }
    Ok(runtime.control_snapshot(true, cleared_trace_count))
}

pub fn observe_filter_with_builtin_quiet_directives(filter: &str) -> String {
    observe_filter_with_quiet_directives(filter, OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES)
}

pub fn observe_filter_with_quiet_directives(
    filter: &str,
    quiet_filter_directives: &[impl AsRef<str>],
) -> String {
    let trimmed = filter.trim();
    let mut directives = Vec::new();
    if !trimmed.is_empty() {
        directives.push(trimmed.to_string());
    }
    for directive in quiet_filter_directives {
        let directive = directive.as_ref().trim();
        let Some(target) = observe_filter_directive_target(directive) else {
            continue;
        };
        if !observe_filter_mentions_target(trimmed, target) {
            directives.push(directive.to_string());
        }
    }
    directives.join(",")
}

fn observe_filter_mentions_target(filter: &str, target: &str) -> bool {
    filter
        .split(',')
        .filter_map(observe_filter_directive_target)
        .any(|candidate| candidate == target)
}

fn observe_filter_directive_target(directive: &str) -> Option<&str> {
    let directive = directive.trim();
    if directive.is_empty() {
        return None;
    }
    let target_and_spans = directive
        .rsplit_once('=')
        .map(|(target, _level)| target)
        .unwrap_or(directive)
        .trim();
    if target_and_spans.is_empty() || is_filter_level(target_and_spans) {
        return None;
    }
    let target = target_and_spans
        .split_once('[')
        .map(|(target, _spans)| target)
        .unwrap_or(target_and_spans)
        .trim();
    if target.is_empty() || is_filter_level(target) {
        None
    } else {
        Some(target)
    }
}

fn is_filter_level(value: &str) -> bool {
    ["off", "error", "warn", "info", "debug", "trace"]
        .iter()
        .any(|level| value.eq_ignore_ascii_case(level))
}

pub fn observe_control_snapshot() -> ObserveControlResponse {
    OBSERVE_RUNTIME
        .get()
        .map(|runtime| runtime.control_snapshot(false, 0))
        .unwrap_or_else(|| ObserveFilterState::default().to_control_response(None, false, 0))
}

pub fn read_observe_metrics(request: &ObserveMetricsRequest) -> ObserveMetricsResponse {
    finish_observe_metrics(observe_runtime_metrics(), request)
}

pub fn read_observe_metrics_for_data_dir(
    data_dir: &SinomoDataDir,
    request: &ObserveMetricsRequest,
) -> ObserveMetricsResponse {
    let mut metrics = observe_runtime_metrics();
    for (name, exporter) in observe_metric_exporter_snapshot() {
        match exporter(data_dir) {
            Ok(records) => metrics.extend(records),
            Err(error) => {
                let mut labels = BTreeMap::new();
                labels.insert("exporter".to_string(), name);
                labels.insert("error".to_string(), error.to_string());
                metrics.push(ObserveMetricRecord::gauge(
                    "sinomo.observe.metric_exporter.errors",
                    labels,
                    1.0,
                ));
            }
        }
    }
    finish_observe_metrics(metrics, request)
}

fn observe_runtime_metrics() -> Vec<ObserveMetricRecord> {
    OBSERVE_RUNTIME
        .get()
        .map(|runtime| runtime.metrics.snapshot())
        .unwrap_or_default()
}

fn finish_observe_metrics(
    mut metrics: Vec<ObserveMetricRecord>,
    request: &ObserveMetricsRequest,
) -> ObserveMetricsResponse {
    if let Some(prefix) = request.name_prefix.as_ref() {
        metrics.retain(|record| record.name.starts_with(prefix));
    }
    metrics.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.labels.cmp(&right.labels))
    });
    if request.limit > 0 && metrics.len() > request.limit {
        metrics.truncate(request.limit);
    }
    ObserveMetricsResponse { metrics }
}

fn observe_metric_exporters() -> &'static Mutex<BTreeMap<String, ObserveMetricExporter>> {
    OBSERVE_METRIC_EXPORTERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn observe_metric_exporter_snapshot() -> Vec<(String, ObserveMetricExporter)> {
    observe_metric_exporters()
        .lock()
        .map(|exporters| {
            exporters
                .iter()
                .map(|(name, exporter)| (name.clone(), Arc::clone(exporter)))
                .collect()
        })
        .unwrap_or_default()
}

pub fn read_observe_traces(request: &ObserveTracesRequest) -> ObserveTracesResponse {
    let Some(runtime) = OBSERVE_RUNTIME.get() else {
        return ObserveTracesResponse {
            records: Vec::new(),
            capture_active: request.capture_id.as_ref().map(|_| false),
            dropped_records: 0,
            truncated: false,
        };
    };
    // Resolve capture state before taking the trace-buffer lock; the trace
    // layer uses capture -> traces ordering.
    let capture_active = request
        .capture_id
        .as_ref()
        .map(|capture_id| runtime.current_capture_id().as_ref() == Some(capture_id));
    let Ok(traces) = runtime.traces.lock() else {
        return ObserveTracesResponse {
            records: Vec::new(),
            capture_active,
            dropped_records: runtime.dropped_traces.load(Ordering::Relaxed),
            truncated: true,
        };
    };
    let mut records = traces
        .iter()
        .filter(|record| {
            request
                .target_prefix
                .as_ref()
                .is_none_or(|prefix| record.target.starts_with(prefix))
                && request
                    .traceparent
                    .as_ref()
                    .is_none_or(|traceparent| record.traceparent.as_ref() == Some(traceparent))
                && request
                    .capture_id
                    .as_ref()
                    .is_none_or(|capture_id| record.capture_id.as_ref() == Some(capture_id))
        })
        .cloned()
        .collect::<Vec<_>>();
    let limit_truncated = request.limit > 0 && records.len() > request.limit;
    if limit_truncated {
        records = records.split_off(records.len() - request.limit);
    }
    let dropped_records = runtime.dropped_traces.load(Ordering::Relaxed);
    ObserveTracesResponse {
        records,
        capture_active,
        dropped_records,
        truncated: dropped_records > 0 || limit_truncated,
    }
}

impl ObserveRuntime {
    fn try_current_filter_state(&self) -> Result<ObserveFilterState, ObserveError> {
        self.filter
            .lock()
            .map(|filter| filter.clone())
            .map_err(|_| ObserveError::Runtime {
                detail: "observe filter state lock is poisoned".to_string(),
            })
    }

    fn current_filter_state(&self) -> ObserveFilterState {
        self.try_current_filter_state().unwrap_or_else(|_| {
            ObserveFilterState::new(
                self.config.filter.clone(),
                self.config.quiet_filter_directives.clone(),
            )
        })
    }

    fn install_filter_state(&self, next_state: ObserveFilterState) -> Result<(), ObserveError> {
        let parsed = EnvFilter::try_new(next_state.active_filter.as_str()).map_err(|source| {
            ObserveError::Runtime {
                detail: format!(
                    "invalid observe filter `{}`: {source}",
                    next_state.active_filter
                ),
            }
        })?;
        self.filter_handle
            .modify(|filter| *filter = parsed)
            .map_err(|source| ObserveError::Runtime {
                detail: format!("failed to reload observe filter: {source}"),
            })?;
        let mut filter = self.filter.lock().map_err(|_| ObserveError::Runtime {
            detail: "observe filter state lock is poisoned".to_string(),
        })?;
        *filter = next_state;
        Ok(())
    }

    fn take_expired_capture_restore(&self) -> Result<Option<ObserveFilterState>, ObserveError> {
        let mut capture = self.capture.lock().map_err(|_| ObserveError::Runtime {
            detail: "observe capture state lock is poisoned".to_string(),
        })?;
        if !capture
            .as_ref()
            .is_some_and(ActiveObserveCapture::is_expired)
        {
            return Ok(None);
        }
        Ok(capture.take().and_then(|capture| capture.restore_filter))
    }

    fn schedule_capture_expiry(
        &self,
        capture_id: String,
        expires_at: Instant,
    ) -> Result<(), ObserveError> {
        let runtime = self.clone();
        std::thread::Builder::new()
            .name("sinomo-observe-capture-expiry".to_string())
            .spawn(move || {
                std::thread::sleep(expires_at.saturating_duration_since(Instant::now()));
                let Ok(_control_update) = runtime.control_update.lock() else {
                    return;
                };
                let restore_filter = runtime.capture.lock().ok().and_then(|mut capture| {
                    let should_expire = capture.as_ref().is_some_and(|capture| {
                        capture.id == capture_id
                            && capture.expires_at == expires_at
                            && capture.is_expired()
                    });
                    should_expire
                        .then(|| capture.take())
                        .flatten()
                        .and_then(|capture| capture.restore_filter)
                });
                if let Some(restore_filter) = restore_filter
                    && let Err(error) = runtime.install_filter_state(restore_filter)
                {
                    tracing::warn!(
                        "failed to restore observe filter after capture expiry: {error}"
                    );
                }
            })
            .map(|_| ())
            .map_err(|source| ObserveError::Runtime {
                detail: format!("failed to schedule observe capture expiry: {source}"),
            })
    }

    fn control_snapshot(
        &self,
        changed: bool,
        cleared_trace_count: usize,
    ) -> ObserveControlResponse {
        let capture_id = self.current_capture_id();
        self.current_filter_state()
            .to_control_response(capture_id, changed, cleared_trace_count)
    }

    fn current_capture_id(&self) -> Option<String> {
        let Ok(capture) = self.capture.lock() else {
            return None;
        };
        if capture
            .as_ref()
            .is_some_and(ActiveObserveCapture::is_expired)
        {
            return None;
        }
        capture.as_ref().map(|capture| capture.id.clone())
    }
}

impl ObserveFilterState {
    fn new(base_filter: String, quiet_filter_directives: Vec<String>) -> Self {
        let quiet_filter_directives = normalized_quiet_filter_directives(quiet_filter_directives);
        let active_filter =
            observe_filter_with_quiet_directives(&base_filter, &quiet_filter_directives);
        Self {
            base_filter,
            quiet_filter_directives,
            active_filter,
        }
    }

    fn to_control_response(
        &self,
        active_capture_id: Option<String>,
        changed: bool,
        cleared_trace_count: usize,
    ) -> ObserveControlResponse {
        ObserveControlResponse {
            active_filter: self.active_filter.clone(),
            base_filter: self.base_filter.clone(),
            quiet_filter_directives: self.quiet_filter_directives.clone(),
            active_capture_id,
            cleared_trace_count,
            changed,
        }
    }
}

impl Default for ObserveFilterState {
    fn default() -> Self {
        Self::new(
            OBSERVE_DEFAULT_BASE_FILTER.to_string(),
            default_quiet_filter_directives(),
        )
    }
}

impl ObserveControlRequest {
    fn changes_filter_state(&self) -> bool {
        self.reset
            || self.filter.is_some()
            || self.quiet_filter_directives.is_some()
            || self.clear_quiet_filter_directives
    }

    fn changes_runtime_state(&self) -> bool {
        self.changes_filter_state()
            || self.capture_id.is_some()
            || self.clear_capture
            || self.clear_traces
    }
}

fn default_quiet_filter_directives() -> Vec<String> {
    OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES
        .iter()
        .map(|directive| (*directive).to_string())
        .collect()
}

fn normalized_quiet_filter_directives(directives: Vec<String>) -> Vec<String> {
    directives
        .into_iter()
        .map(|directive| directive.trim().to_string())
        .filter(|directive| !directive.is_empty())
        .collect()
}

fn observe_quiet_filter_directives_from_env() -> Vec<String> {
    let Some(value) = std::env::var("SINOMO_OBSERVE_QUIET_FILTERS")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return default_quiet_filter_directives();
    };
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "none" | "off"
    ) {
        return Vec::new();
    }
    normalized_quiet_filter_directives(value.split(',').map(ToString::to_string).collect())
}

#[derive(Clone)]
struct ObserveTraceLayer {
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
    dropped_traces: Arc<AtomicU64>,
    capture: Arc<Mutex<Option<ActiveObserveCapture>>>,
}

impl<S> Layer<S> for ObserveTraceLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);
        let traceparent = visitor
            .fields
            .get("traceparent")
            .and_then(serde_json::Value::as_str)
            .filter(|traceparent| !traceparent.is_empty())
            .map(ToString::to_string);
        let capture_id = self.capture.lock().ok().and_then(|capture| {
            capture
                .as_ref()
                .filter(|capture| !capture.is_expired())
                .map(|capture| capture.id.clone())
        });
        let record = ObserveTraceRecord {
            timestamp: OffsetDateTime::now_utc(),
            level: metadata.level().to_string(),
            target: metadata.target().to_string(),
            name: metadata.name().to_string(),
            traceparent,
            capture_id,
            fields: serde_json::Value::Object(visitor.fields),
        };
        if let Ok(mut traces) = self.traces.lock() {
            if traces.len() >= TRACE_BUFFER_LIMIT {
                traces.pop_front();
                self.dropped_traces.fetch_add(1, Ordering::Relaxed);
            }
            traces.push_back(record);
        }
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    fields: serde_json::Map<String, serde_json::Value>,
}

impl field::Visit for JsonFieldVisitor {
    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{value:?}")),
        );
    }

    fn record_i64(&mut self, field: &field::Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &field::Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &field::Field, value: &str) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
}

#[derive(Default)]
struct ObserveMetricsRecorder {
    slots: Mutex<BTreeMap<String, Arc<ObserveMetricSlot>>>,
}

impl ObserveMetricsRecorder {
    fn slot(&self, key: &Key, kind: ObserveMetricKind) -> Arc<ObserveMetricSlot> {
        let map_key = observe_metric_map_key(key, kind);
        let mut slots = self.slots.lock().expect("observe metrics lock poisoned");
        Arc::clone(slots.entry(map_key).or_insert_with(|| {
            Arc::new(ObserveMetricSlot::new(
                key.name().to_string(),
                observe_metric_labels(key),
                kind,
            ))
        }))
    }

    fn snapshot(&self) -> Vec<ObserveMetricRecord> {
        let Ok(slots) = self.slots.lock() else {
            return Vec::new();
        };
        slots.values().map(|slot| slot.snapshot()).collect()
    }
}

impl Recorder for ObserveMetricsRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.slot(key, ObserveMetricKind::Counter))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.slot(key, ObserveMetricKind::Gauge))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.slot(key, ObserveMetricKind::Histogram))
    }
}

struct ObserveMetricSlot {
    name: String,
    labels: BTreeMap<String, String>,
    kind: ObserveMetricKind,
    value: Mutex<ObserveMetricValue>,
}

impl ObserveMetricSlot {
    fn new(name: String, labels: BTreeMap<String, String>, kind: ObserveMetricKind) -> Self {
        Self {
            name,
            labels,
            kind,
            value: Mutex::new(ObserveMetricValue::new(kind)),
        }
    }

    fn snapshot(&self) -> ObserveMetricRecord {
        let value = self
            .value
            .lock()
            .map(|value| value.clone())
            .unwrap_or_else(|_| ObserveMetricValue::new(self.kind));
        match value {
            ObserveMetricValue::Counter(value) => ObserveMetricRecord {
                name: self.name.clone(),
                kind: self.kind,
                labels: self.labels.clone(),
                value: Some(value as f64),
                count: None,
                sum: None,
                min: None,
                max: None,
            },
            ObserveMetricValue::Gauge(value) => ObserveMetricRecord {
                name: self.name.clone(),
                kind: self.kind,
                labels: self.labels.clone(),
                value: Some(value),
                count: None,
                sum: None,
                min: None,
                max: None,
            },
            ObserveMetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } => ObserveMetricRecord {
                name: self.name.clone(),
                kind: self.kind,
                labels: self.labels.clone(),
                value: None,
                count: Some(count),
                sum: Some(sum),
                min,
                max,
            },
        }
    }
}

impl CounterFn for ObserveMetricSlot {
    fn increment(&self, value: u64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Counter(current) = &mut *state
        {
            *current = current.saturating_add(value);
        }
    }

    fn absolute(&self, value: u64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Counter(current) = &mut *state
        {
            *current = (*current).max(value);
        }
    }
}

impl GaugeFn for ObserveMetricSlot {
    fn increment(&self, value: f64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Gauge(current) = &mut *state
        {
            *current += value;
        }
    }

    fn decrement(&self, value: f64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Gauge(current) = &mut *state
        {
            *current -= value;
        }
    }

    fn set(&self, value: f64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Gauge(current) = &mut *state
        {
            *current = value;
        }
    }
}

impl HistogramFn for ObserveMetricSlot {
    fn record(&self, value: f64) {
        if let Ok(mut state) = self.value.lock()
            && let ObserveMetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } = &mut *state
        {
            *count = count.saturating_add(1);
            *sum += value;
            *min = Some(min.map_or(value, |current| current.min(value)));
            *max = Some(max.map_or(value, |current| current.max(value)));
        }
    }
}

#[derive(Clone, Debug)]
enum ObserveMetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram {
        count: u64,
        sum: f64,
        min: Option<f64>,
        max: Option<f64>,
    },
}

impl ObserveMetricValue {
    fn new(kind: ObserveMetricKind) -> Self {
        match kind {
            ObserveMetricKind::Counter => Self::Counter(0),
            ObserveMetricKind::Gauge => Self::Gauge(0.0),
            ObserveMetricKind::Histogram => Self::Histogram {
                count: 0,
                sum: 0.0,
                min: None,
                max: None,
            },
        }
    }
}

fn observe_metric_map_key(key: &Key, kind: ObserveMetricKind) -> String {
    let mut value = format!("{}:{}", kind.as_key_prefix(), key.name());
    for label in key.labels() {
        value.push('|');
        value.push_str(label.key());
        value.push('=');
        value.push_str(label.value());
    }
    value
}

fn observe_metric_labels(key: &Key) -> BTreeMap<String, String> {
    key.labels()
        .map(|label| (label.key().to_string(), label.value().to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing_subscriber::Registry;

    #[test]
    fn default_filter_includes_builtin_quiet_directives() {
        assert_eq!(
            observe_filter_with_builtin_quiet_directives(OBSERVE_DEFAULT_BASE_FILTER),
            OBSERVE_DEFAULT_FILTER
        );
        EnvFilter::try_new(OBSERVE_DEFAULT_FILTER).expect("default observe filter parses");
    }

    #[test]
    fn custom_filter_keeps_builtin_quiet_directives() {
        assert_eq!(
            observe_filter_with_builtin_quiet_directives("debug,sinomo_network=trace"),
            "debug,sinomo_network=trace,iroh::net_report::report=error"
        );
    }

    #[test]
    fn quiet_directives_are_runtime_composable() {
        assert_eq!(
            observe_filter_with_quiet_directives("debug,sinomo_network=trace", &[] as &[&str]),
            "debug,sinomo_network=trace"
        );
        assert_eq!(
            observe_filter_with_quiet_directives(
                "debug",
                &["iroh::net_report::report=error", "hyper=error"]
            ),
            "debug,iroh::net_report::report=error,hyper=error"
        );
        assert_eq!(
            ObserveFilterState::new("debug".to_string(), vec!["  ".to_string()])
                .quiet_filter_directives,
            Vec::<String>::new()
        );
    }

    #[test]
    fn exact_builtin_quiet_target_can_be_overridden() {
        assert_eq!(
            observe_filter_with_builtin_quiet_directives("debug,iroh::net_report::report=warn"),
            "debug,iroh::net_report::report=warn"
        );
    }

    #[test]
    fn filter_target_parser_ignores_levels_and_keeps_targets() {
        assert_eq!(observe_filter_directive_target("warn"), None);
        assert_eq!(
            observe_filter_directive_target("sinomo=info"),
            Some("sinomo")
        );
        assert_eq!(
            observe_filter_directive_target("iroh::net_report::report[span]=error"),
            Some("iroh::net_report::report")
        );
    }

    #[test]
    fn builtin_quiet_directive_suppresses_iroh_net_report_warning() {
        let observed = Arc::new(AtomicUsize::new(0));
        let subscriber = Registry::default()
            .with(EnvFilter::try_new(OBSERVE_DEFAULT_FILTER).expect("filter parses"))
            .with(CountingLayer {
                observed: Arc::clone(&observed),
            });

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                target: "iroh::net_report::report",
                "IPv4 address detected by QAD varies by destination"
            );
            tracing::error!(target: "iroh::net_report::report", "iroh net report error");
            tracing::warn!(target: "iroh::endpoint", "other iroh warning");
        });

        assert_eq!(observed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn trace_layer_tags_each_record_with_capture_active_at_event_time() {
        let traces = Arc::new(Mutex::new(VecDeque::new()));
        let capture = Arc::new(Mutex::new(Some(ActiveObserveCapture::new(
            "cluster-capture-a".to_string(),
            None,
        ))));
        let subscriber = Registry::default().with(ObserveTraceLayer {
            traces: Arc::clone(&traces),
            dropped_traces: Arc::new(AtomicU64::new(0)),
            capture: Arc::clone(&capture),
        });

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                target: "sinomo.test.capture",
                traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "captured event"
            );
            *capture.lock().expect("capture") = Some(ActiveObserveCapture {
                id: "expired-capture".to_string(),
                expires_at: Instant::now(),
                restore_filter: None,
            });
            tracing::info!(target: "sinomo.test.capture", "event after capture lease");
        });

        let traces = traces.lock().expect("trace records");
        assert_eq!(traces.len(), 2);
        assert_eq!(traces[0].capture_id.as_deref(), Some("cluster-capture-a"));
        assert_eq!(
            traces[0].traceparent.as_deref(),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        );
        assert_eq!(traces[1].capture_id, None);
    }

    #[test]
    fn trace_layer_counts_ring_overflow() {
        let template = ObserveTraceRecord {
            timestamp: OffsetDateTime::now_utc(),
            level: "INFO".to_string(),
            target: "sinomo.test".to_string(),
            name: "test".to_string(),
            traceparent: None,
            capture_id: None,
            fields: serde_json::Value::Null,
        };
        let traces = Arc::new(Mutex::new(VecDeque::from(vec![
            template;
            TRACE_BUFFER_LIMIT
        ])));
        let dropped_traces = Arc::new(AtomicU64::new(0));
        let subscriber = Registry::default().with(ObserveTraceLayer {
            traces: Arc::clone(&traces),
            dropped_traces: Arc::clone(&dropped_traces),
            capture: Arc::new(Mutex::new(None)),
        });

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "sinomo.test.capture", "overflow event");
        });

        assert_eq!(traces.lock().expect("traces").len(), TRACE_BUFFER_LIMIT);
        assert_eq!(dropped_traces.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn capture_expiry_restores_owned_filter_state() {
        let original = ObserveFilterState::new("warn,sinomo=info".to_string(), Vec::new());
        let capture_filter =
            ObserveFilterState::new("warn,sinomo.dfs.perf=trace".to_string(), Vec::new());
        let filter = EnvFilter::try_new(original.active_filter.as_str()).expect("filter");
        let (_filter_layer, filter_handle) = reload::Layer::new(filter);
        let runtime = ObserveRuntime {
            config: ObserveRuntimeConfig {
                process_role: "test".to_string(),
                node_id: None,
                filter: original.base_filter.clone(),
                quiet_filter_directives: original.quiet_filter_directives.clone(),
            },
            filter: Arc::new(Mutex::new(original.clone())),
            filter_handle,
            metrics: Arc::new(ObserveMetricsRecorder::default()),
            traces: Arc::new(Mutex::new(VecDeque::new())),
            dropped_traces: Arc::new(AtomicU64::new(0)),
            capture: Arc::new(Mutex::new(None)),
            control_update: Arc::new(Mutex::new(())),
        };
        runtime
            .install_filter_state(capture_filter.clone())
            .expect("install capture filter");
        let expires_at = Instant::now() + Duration::from_millis(20);
        *runtime.capture.lock().expect("capture") = Some(ActiveObserveCapture {
            id: "expiring-capture".to_string(),
            expires_at,
            restore_filter: Some(original.clone()),
        });
        runtime
            .schedule_capture_expiry("expiring-capture".to_string(), expires_at)
            .expect("schedule expiry");

        std::thread::sleep(Duration::from_millis(80));

        assert_eq!(runtime.current_capture_id(), None);
        assert_eq!(runtime.current_filter_state(), original);
    }

    struct CountingLayer {
        observed: Arc<AtomicUsize>,
    }

    impl<S> Layer<S> for CountingLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
            self.observed.fetch_add(1, Ordering::Relaxed);
        }
    }
}
