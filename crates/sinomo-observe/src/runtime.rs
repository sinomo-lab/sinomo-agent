use crate::ObserveError;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use serde::{Deserialize, Serialize};
pub use sinomo_api::observe::{
    ObserveHistogramBucket, ObserveMetricAggregation, ObserveMetricDescriptor, ObserveMetricKind,
    ObserveMetricRecord, ObserveMetricSemantics, ObserveMetricUnit,
};
use sinomo_core::local::SinomoDataDir;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock, RwLock};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tracing::{Event, Subscriber, field};
use tracing_subscriber::filter::{EnvFilter, dynamic_filter_fn};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry, fmt};

pub const OBSERVE_DEFAULT_BASE_FILTER: &str = "warn,sinomo=info";
pub const OBSERVE_DEFAULT_FILTER: &str = "warn,sinomo=info,iroh::net_report::report=error";
pub const OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES: &[&str] = &["iroh::net_report::report=error"];
pub const OBSERVE_BUILT_IN_QUIET_FILTER_DIRECTIVES: &[&str] = &["iroh::net_report::report=error"];
const DFS_PERF_TRACE_TARGET: &str = "sinomo.dfs.perf";
const DFS_PERF_TRACE_DIRECTIVE: &str = "sinomo.dfs.perf=trace";
const TRACE_BUFFER_LIMIT: usize = 4096;
const TRACE_SCOPE_DEPTH_LIMIT: usize = 32;
const TRACE_MAX_CONFIGURED_RECORDS: usize = 32 * 1024;
static LOG_COLLECTOR_INSTALLED: AtomicBool = AtomicBool::new(false);
static CONSOLE_SUPPRESSION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A terminal UI owns console output while this guard is alive. Structured
/// collection, configured file sinks and log-level policy remain unchanged.
#[must_use = "keep this guard alive while the TUI owns the terminal"]
pub struct ObserveConsoleGuard {
    _private: (),
}

/// Suppress only this process's formatted console sinks until all guards drop.
pub fn suppress_observe_console() -> ObserveConsoleGuard {
    CONSOLE_SUPPRESSION.fetch_add(1, Ordering::AcqRel);
    ObserveConsoleGuard { _private: () }
}

impl Drop for ObserveConsoleGuard {
    fn drop(&mut self) {
        CONSOLE_SUPPRESSION.fetch_sub(1, Ordering::AcqRel);
    }
}
const CAPTURE_ID_MAX_BYTES: usize = 128;
const CAPTURE_LEASE: Duration = Duration::from_secs(10 * 60);
const OBSERVE_HISTOGRAM_BUCKET_UPPER_BOUNDS: &[f64] = &[
    1.0,
    2.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_500.0,
    5_000.0,
    10_000.0,
    25_000.0,
    50_000.0,
    100_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
    2_500_000.0,
    5_000_000.0,
    10_000_000.0,
    25_000_000.0,
    50_000_000.0,
    100_000_000.0,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserveRuntimeConfig {
    pub process_role: String,
    pub node_id: Option<String>,
    pub filter: String,
    pub quiet_filter_directives: Vec<String>,
    /// Write subsequent enabled routine events to the existing formatted sinks.
    /// Structured collection is independent and remains bounded in memory.
    pub live_log_writing: bool,
}

impl ObserveRuntimeConfig {
    pub fn daemon(label: impl Into<String>, node_id: Option<String>) -> Self {
        Self::process(label, node_id)
    }

    /// Config for any Sinomo process role (daemon, graphical app, tooling), with the
    /// filter taken from `SINOMO_OBSERVE_FILTER` when set. A truthy
    /// `SINOMO_DFS_PERF_TRACE` adds `sinomo.dfs.perf=trace` unless the configured
    /// filter already has an explicit directive for that exact target.
    pub fn process(label: impl Into<String>, node_id: Option<String>) -> Self {
        let configured_filter = std::env::var("SINOMO_OBSERVE_FILTER")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| OBSERVE_DEFAULT_BASE_FILTER.to_string());
        let configured_filter = observe_filter_with_dfs_perf_trace(
            &configured_filter,
            std::env::var("SINOMO_DFS_PERF_TRACE")
                .ok()
                .is_some_and(|value| observe_env_value_is_truthy(&value)),
        );
        Self {
            process_role: label.into(),
            node_id,
            filter: configured_filter,
            quiet_filter_directives: observe_quiet_filter_directives_from_env(),
            live_log_writing: std::env::var("SINOMO_OBSERVE_LIVE_LOG_WRITING")
                .ok()
                .is_some_and(|value| observe_env_value_is_truthy(&value)),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveControlRequest {
    /// Opt in/out of formatted routine output without flushing buffered history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_log_writing: Option<bool>,
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
    /// None means unsupported or no initialized runtime, never confirmed disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_log_writing: Option<bool>,
    pub active_filter: String,
    pub base_filter: String,
    pub quiet_filter_directives: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_capture_id: Option<String>,
    #[serde(default)]
    pub cleared_trace_count: usize,
    pub changed: bool,
}

pub use sinomo_api::observe::ObserveMetricsRequest;

pub use sinomo_api::observe::ObserveMetricsResponse;

pub fn observe_metrics_response_from_metrics(
    metrics: Vec<ObserveMetricRecord>,
) -> ObserveMetricsResponse {
    let (process_role, node_id) = observe_runtime_process_identity();
    let descriptor_names = metrics
        .iter()
        .map(|record| record.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let descriptors = observe_metric_descriptors()
        .into_iter()
        .filter(|descriptor| descriptor_names.contains(descriptor.name.as_str()))
        .collect();
    ObserveMetricsResponse {
        observed_at: Some(OffsetDateTime::now_utc()),
        process_started_at: observe_process_started_at(),
        process_role: (process_role != "uninitialized").then_some(process_role),
        node_id,
        exporters: Vec::new(),
        partial: false,
        descriptors,
        metrics,
    }
}

pub use sinomo_api::observe::ObserveMetricExporterStatus;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveTracesRequest {
    /// Exclusive process-local cursor. Omit for the complete retained ring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<usize>,
    /// Advance the next request to this cursor, even when filters return no rows.
    #[serde(default)]
    pub last_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_available_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ObserveTraceProcess>,
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

/// Derives one interval sample from cumulative process metrics. Gauge values
/// remain instantaneous. Counter resets (including a process restart) use the
/// current value as the first delta instead of producing a negative result.
pub fn observe_metrics_interval_delta(
    previous: &ObserveMetricsResponse,
    current: &ObserveMetricsResponse,
) -> ObserveMetricsResponse {
    let same_process = previous.process_started_at.is_none()
        || current.process_started_at.is_none()
        || previous.process_started_at == current.process_started_at;
    let previous_metrics = if same_process {
        previous.metrics.as_slice()
    } else {
        Default::default()
    };
    let mut delta = current.clone();
    delta.metrics = observe_metric_records_interval_delta(previous_metrics, &current.metrics);
    delta
}

pub fn observe_metric_records_interval_delta(
    previous: &[ObserveMetricRecord],
    current: &[ObserveMetricRecord],
) -> Vec<ObserveMetricRecord> {
    current
        .iter()
        .map(|current| {
            let previous = previous.iter().find(|previous| {
                previous.name == current.name
                    && previous.kind == current.kind
                    && previous.labels == current.labels
            });
            observe_metric_record_interval_delta(previous, current)
        })
        .collect()
}

fn observe_metric_record_interval_delta(
    previous: Option<&ObserveMetricRecord>,
    current: &ObserveMetricRecord,
) -> ObserveMetricRecord {
    let mut delta = current.clone();
    match current.kind {
        ObserveMetricKind::Gauge => {}
        ObserveMetricKind::Counter => {
            delta.value = Some(observe_f64_counter_delta(
                previous.and_then(|record| record.value),
                current.value.unwrap_or_default(),
            ));
        }
        ObserveMetricKind::Histogram => {
            delta.count = Some(observe_u64_counter_delta(
                previous.and_then(|record| record.count),
                current.count.unwrap_or_default(),
            ));
            delta.sum = Some(observe_f64_counter_delta(
                previous.and_then(|record| record.sum),
                current.sum.unwrap_or_default(),
            ));
            delta.min = None;
            delta.max = None;
            for bucket in &mut delta.buckets {
                let previous_count = previous.and_then(|record| {
                    record
                        .buckets
                        .iter()
                        .find(|candidate| {
                            candidate.upper_bound.to_bits() == bucket.upper_bound.to_bits()
                        })
                        .map(|candidate| candidate.count)
                });
                bucket.count = observe_u64_counter_delta(previous_count, bucket.count);
            }
        }
    }
    delta
}

fn observe_u64_counter_delta(previous: Option<u64>, current: u64) -> u64 {
    previous
        .filter(|previous| *previous <= current)
        .map_or(current, |previous| current - previous)
}

fn observe_f64_counter_delta(previous: Option<f64>, current: f64) -> f64 {
    previous
        .filter(|previous| previous.is_finite() && *previous <= current)
        .map_or(current, |previous| current - previous)
}

/// Identity and observation window for process-local monotonic trace clocks.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveTraceProcess {
    pub node_id: Option<String>,
    pub process_role: String,
    pub process_id: u32,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    pub observed_monotonic_ns: u64,
    /// Linux kernel process counters, including unrelated/background work.
    pub io: crate::performance::ObserveProcessIo,
}

/// Root-to-leaf context; IDs are unique within this process incarnation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveSpanContext {
    pub span_id: u64,
    pub name: String,
    pub target: String,
    pub fields: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveTraceRecord {
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub monotonic_ns: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<ObserveSpanContext>,
    #[serde(default)]
    pub context_truncated: bool,
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
    trace_capacity: usize,
    config: ObserveRuntimeConfig,
    started_at: OffsetDateTime,
    filter: Arc<Mutex<ObserveFilterState>>,
    filter_handle: reload::Handle<EnvFilter, Registry>,
    metrics: Arc<ObserveMetricsRecorder>,
    logs: Arc<Mutex<crate::logs::LogBuffer>>,
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
    dropped_traces: Arc<AtomicU64>,
    capture: Arc<Mutex<Option<ActiveObserveCapture>>>,
    control_update: Arc<Mutex<()>>,
    live_log_writing: Arc<AtomicBool>,
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
type ObserveMetricExporterGeneration =
    Arc<dyn Fn(&SinomoDataDir) -> Result<u64, ObserveError> + Send + Sync>;
#[derive(Clone)]
struct ObserveMetricExporterRegistration {
    prefixes: Vec<String>,
    exporter: ObserveMetricExporter,
    cache_ttl: Option<Duration>,
    cache_generation: Option<ObserveMetricExporterGeneration>,
    cache: Arc<Mutex<Option<ObserveMetricExporterCache>>>,
}

#[derive(Clone)]
struct ObserveMetricExporterCache {
    data_dir: std::path::PathBuf,
    collected_at: OffsetDateTime,
    cached_at: Instant,
    generation: Option<u64>,
    duration_us: u64,
    records: Vec<ObserveMetricRecord>,
}
static OBSERVE_METRIC_EXPORTERS: OnceLock<
    Mutex<BTreeMap<String, ObserveMetricExporterRegistration>>,
> = OnceLock::new();
static OBSERVE_METRIC_DESCRIPTORS: OnceLock<Mutex<BTreeMap<String, ObserveMetricDescriptor>>> =
    OnceLock::new();

pub fn register_observe_metric_exporter(
    name: impl Into<String>,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    register_observe_metric_exporter_with_prefixes(name, Vec::<String>::new(), exporter);
}

/// Registers a pull-time exporter together with the metric prefixes it can
/// produce. Prefix metadata lets a filtered live view skip unrelated durable
/// state scans. An empty prefix list preserves the legacy always-run behavior.
pub fn register_observe_metric_exporter_with_prefixes(
    name: impl Into<String>,
    prefixes: impl IntoIterator<Item = impl Into<String>>,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    register_observe_metric_exporter_with_policy(name, prefixes, None, exporter);
}

/// Registers an exporter with optional successful-result caching. The cache is
/// process-local, bounded to one snapshot, and keyed by the authoritative data
/// directory so test or embedded runtimes cannot reuse another instance's
/// durable-state view.
pub fn register_observe_metric_exporter_with_policy(
    name: impl Into<String>,
    prefixes: impl IntoIterator<Item = impl Into<String>>,
    cache_ttl: Option<Duration>,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    register_observe_metric_exporter_with_cache_policy(name, prefixes, cache_ttl, None, exporter);
}

/// Registers an exporter whose successful snapshot remains current until a
/// process-local generation changes. The generation callback must be a cheap
/// resident read; hosts typically drive it from durable write notifications.
pub fn register_observe_metric_exporter_with_generation(
    name: impl Into<String>,
    prefixes: impl IntoIterator<Item = impl Into<String>>,
    generation: impl Fn(&SinomoDataDir) -> Result<u64, ObserveError> + Send + Sync + 'static,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    register_observe_metric_exporter_with_cache_policy(
        name,
        prefixes,
        None,
        Some(Arc::new(generation)),
        exporter,
    );
}

fn register_observe_metric_exporter_with_cache_policy(
    name: impl Into<String>,
    prefixes: impl IntoIterator<Item = impl Into<String>>,
    cache_ttl: Option<Duration>,
    cache_generation: Option<ObserveMetricExporterGeneration>,
    exporter: impl Fn(&SinomoDataDir) -> Result<Vec<ObserveMetricRecord>, ObserveError>
    + Send
    + Sync
    + 'static,
) {
    observe_metric_exporters()
        .lock()
        .expect("observe metric exporters lock poisoned")
        .insert(
            name.into(),
            ObserveMetricExporterRegistration {
                prefixes: prefixes.into_iter().map(Into::into).collect(),
                exporter: Arc::new(exporter),
                cache_ttl,
                cache_generation,
                cache: Arc::new(Mutex::new(None)),
            },
        );
}

pub fn register_observe_metric_descriptor(mut descriptor: ObserveMetricDescriptor) {
    descriptor
        .semantics
        .labels
        .retain(|label| !label.is_empty());
    descriptor.semantics.labels.sort();
    descriptor.semantics.labels.dedup();
    if descriptor
        .semantics
        .domain
        .as_ref()
        .is_some_and(String::is_empty)
    {
        descriptor.semantics.domain = None;
    }
    let mut descriptors = observe_metric_descriptors_store()
        .lock()
        .expect("observe metric descriptors lock poisoned");
    if let Some(existing) = descriptors.get(&descriptor.name) {
        let mut semantics = existing.semantics.clone();
        semantics.merge_rolling(&descriptor.semantics);
        descriptor.semantics = semantics;
    }
    if !descriptor
        .semantics
        .aggregation
        .is_compatible_with(descriptor.kind)
    {
        tracing::warn!(
            metric = %descriptor.name,
            kind = ?descriptor.kind,
            aggregation = ?descriptor.semantics.aggregation,
            "ignoring incompatible observe metric aggregation metadata"
        );
        descriptor.semantics.aggregation = ObserveMetricAggregation::None;
    }
    descriptors.insert(descriptor.name.clone(), descriptor);
}

pub fn observe_metric_descriptors() -> Vec<ObserveMetricDescriptor> {
    observe_metric_descriptors_store()
        .lock()
        .map(|descriptors| descriptors.values().cloned().collect())
        .unwrap_or_default()
}

pub fn init_observe_runtime(config: ObserveRuntimeConfig) -> Result<(), ObserveError> {
    init_observe_runtime_inner(config, None, false)
}

/// Initialize the observe runtime with formatted trace output on stderr.
///
/// One-shot CLI commands use this when explicitly asked for DFS performance
/// tracing so machine-readable stdout remains valid.
pub fn init_observe_runtime_with_stderr(config: ObserveRuntimeConfig) -> Result<(), ObserveError> {
    // This entry point is an explicit one-shot diagnostic text export, unlike
    // the default main/shadow initialization. Keep JSON stdout untouched.
    init_observe_runtime_inner(
        ObserveRuntimeConfig {
            live_log_writing: true,
            ..config
        },
        None,
        true,
    )
}

/// Initialize the observe runtime with formatted trace output additionally copied to
/// `log_file` (created/appended, ANSI disabled). Graphical processes use this: they
/// have no supervised stdout. Enabled warnings/errors use the file by default;
/// routine history stays in the structured buffers unless live writing is enabled.
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
    init_observe_runtime_inner(config, Some(Arc::new(file)), false)
}

fn init_observe_runtime_inner(
    config: ObserveRuntimeConfig,
    log_file: Option<Arc<std::fs::File>>,
    stderr: bool,
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
    let logs = Arc::new(Mutex::new(crate::logs::LogBuffer::default()));
    let trace_capacity = parse_trace_capacity(
        std::env::var("SINOMO_OBSERVE_TRACE_RECORDS")
            .ok()
            .as_deref(),
    )?;
    let traces = Arc::new(Mutex::new(VecDeque::with_capacity(trace_capacity)));
    let dropped_traces = Arc::new(AtomicU64::new(0));
    let capture = Arc::new(Mutex::new(None));
    let live_log_writing = Arc::new(AtomicBool::new(config.live_log_writing));
    let runtime = ObserveRuntime {
        trace_capacity,
        started_at: OffsetDateTime::now_utc(),
        filter: Arc::new(Mutex::new(initial_state)),
        filter_handle,
        metrics: Arc::clone(&metrics),
        logs: Arc::clone(&logs),
        traces: Arc::clone(&traces),
        dropped_traces: Arc::clone(&dropped_traces),
        capture: Arc::clone(&capture),
        control_update: Arc::new(Mutex::new(())),
        live_log_writing: Arc::clone(&live_log_writing),
        config,
    };

    let _ = metrics::set_global_recorder(Arc::clone(&metrics));
    let _ = OBSERVE_RUNTIME.set(runtime);

    let file_layer = log_file.map(|file| {
        fmt::layer()
            .with_target(true)
            .with_ansi(false)
            .with_writer(file)
            .with_filter(formatted_log_filter(Arc::clone(&live_log_writing), false))
    });
    let stdout_layer = (!stderr).then(|| {
        fmt::layer()
            .with_target(true)
            .with_filter(formatted_log_filter(Arc::clone(&live_log_writing), true))
    });
    let stderr_layer = stderr.then(|| {
        fmt::layer()
            .with_target(true)
            .with_writer(std::io::stderr)
            .with_filter(formatted_log_filter(Arc::clone(&live_log_writing), true))
    });
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(ObserveTraceLayer {
            trace_capacity,
            logs,
            traces,
            dropped_traces,
            capture,
        })
        .with(stdout_layer)
        .with(stderr_layer)
        .with(file_layer)
        .try_init()
        .map(|()| LOG_COLLECTOR_INSTALLED.store(true, Ordering::Release))
        .map_err(|source| ObserveError::Runtime {
            detail: format!("failed to initialize observe tracing subscriber: {source}"),
        })
        .or(Ok(()))
}

fn formatted_log_filter<S: Subscriber>(
    live_log_writing: Arc<AtomicBool>,
    console: bool,
) -> impl tracing_subscriber::layer::Filter<S> {
    // Dynamic interest is essential: a callsite disabled at startup must be
    // checked again after live writing changes. Admit span context without
    // formatting span lifecycle events. The global filter still applies.
    dynamic_filter_fn(move |metadata, _| {
        metadata.is_span()
            || ((!console || CONSOLE_SUPPRESSION.load(Ordering::Acquire) == 0)
                && (*metadata.level() <= tracing::Level::WARN
                    || live_log_writing.load(Ordering::Acquire)))
    })
}

pub fn update_observe_control(
    request: ObserveControlRequest,
) -> Result<ObserveControlResponse, ObserveError> {
    let runtime = OBSERVE_RUNTIME.get().ok_or_else(|| ObserveError::Runtime {
        detail: "observe runtime is not initialized".to_string(),
    })?;
    if request.live_log_writing.is_some() && !LOG_COLLECTOR_INSTALLED.load(Ordering::Acquire) {
        return Err(ObserveError::Runtime {
            detail: "observe formatted sinks are not installed; live log writing is unavailable"
                .to_string(),
        });
    }
    if request.live_log_writing.is_some() && (request.capture_id.is_some() || request.clear_capture)
    {
        return Err(ObserveError::Runtime {
            detail:
                "live log writing and capture lifecycle changes must be separate control requests"
                    .to_string(),
        });
    }
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
    if let Some((capture_id, expires_at)) = capture_expiry
        && let Err(error) = runtime.schedule_capture_expiry(capture_id.clone(), expires_at)
    {
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
    if let Some(enabled) = request.live_log_writing {
        // No queue, backlog dump or deferred flush: only subsequent formatted
        // events change. Already executing synchronous writes still drain normally.
        runtime.live_log_writing.store(enabled, Ordering::Release);
    }
    Ok(runtime.control_snapshot(true, cleared_trace_count))
}

pub fn observe_filter_with_builtin_quiet_directives(filter: &str) -> String {
    observe_filter_with_quiet_directives(filter, OBSERVE_DEFAULT_QUIET_FILTER_DIRECTIVES)
}

fn observe_filter_with_dfs_perf_trace(filter: &str, enabled: bool) -> String {
    let filter = filter.trim();
    if !enabled || observe_filter_mentions_target(filter, DFS_PERF_TRACE_TARGET) {
        return filter.to_string();
    }
    if filter.is_empty() {
        DFS_PERF_TRACE_DIRECTIVE.to_string()
    } else {
        format!("{filter},{DFS_PERF_TRACE_DIRECTIVE}")
    }
}

fn observe_env_value_is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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

pub(crate) fn observe_runtime_process_identity() -> (String, Option<String>) {
    OBSERVE_RUNTIME
        .get()
        .map(|runtime| {
            (
                runtime.config.process_role.clone(),
                runtime.config.node_id.clone(),
            )
        })
        .unwrap_or_else(|| ("uninitialized".to_string(), None))
}

pub fn observe_process_started_at() -> Option<OffsetDateTime> {
    OBSERVE_RUNTIME.get().map(|runtime| runtime.started_at)
}

pub fn read_observe_metrics(request: &ObserveMetricsRequest) -> ObserveMetricsResponse {
    let started = Instant::now();
    let mut metrics = observe_runtime_metrics();
    metrics.extend(observe_self_health_metrics(
        started.elapsed(),
        std::iter::empty::<&ObserveMetricExporterHealth>(),
    ));
    append_observe_response_health(&mut metrics, request);
    finish_observe_metrics(metrics, request)
}

pub fn read_observe_metrics_for_data_dir(
    data_dir: &SinomoDataDir,
    request: &ObserveMetricsRequest,
) -> ObserveMetricsResponse {
    let started = Instant::now();
    let mut metrics = observe_runtime_metrics();
    let mut exporters = Vec::new();
    let mut exporter_health = Vec::new();
    let mut partial = false;
    for (name, registration) in observe_metric_exporter_snapshot(request) {
        let exporter_started = Instant::now();
        match collect_observe_metric_exporter(&registration, data_dir) {
            Ok(collection) => {
                let record_count = collection.records.len();
                metrics.extend(collection.records);
                exporter_health.push(ObserveMetricExporterHealth {
                    name: name.clone(),
                    duration_us: collection.duration_us,
                    record_count,
                    cached: collection.cached,
                    success: true,
                });
                exporters.push(ObserveMetricExporterStatus {
                    name,
                    observed_at: collection.observed_at,
                    duration_us: collection.duration_us,
                    cached: collection.cached,
                    success: true,
                    error_code: None,
                });
            }
            Err(error) => {
                partial = true;
                let error_code = observe_error_code(&error).to_string();
                let observed_at = OffsetDateTime::now_utc();
                let mut labels = BTreeMap::new();
                labels.insert("exporter".to_string(), name.clone());
                labels.insert("reason".to_string(), error_code.clone());
                metrics.push(ObserveMetricRecord::gauge(
                    "sinomo.observe.metric_exporter.errors",
                    labels,
                    1.0,
                ));
                exporter_health.push(ObserveMetricExporterHealth {
                    name: name.clone(),
                    duration_us: duration_as_micros_u64(exporter_started.elapsed()),
                    record_count: 0,
                    cached: false,
                    success: false,
                });
                exporters.push(ObserveMetricExporterStatus {
                    name: name.clone(),
                    observed_at,
                    duration_us: duration_as_micros_u64(exporter_started.elapsed()),
                    cached: false,
                    success: false,
                    error_code: Some(error_code),
                });
                tracing::warn!(
                    target: "sinomo.observe.exporter",
                    exporter = %name,
                    error = %error,
                    "observe metric exporter failed"
                );
            }
        }
    }
    metrics.extend(observe_self_health_metrics(
        started.elapsed(),
        exporter_health.iter(),
    ));
    append_observe_response_health(&mut metrics, request);
    let mut response = finish_observe_metrics(metrics, request);
    response.exporters = exporters;
    response.partial = partial;
    response
}

struct ObserveMetricExporterHealth {
    name: String,
    duration_us: u64,
    record_count: usize,
    cached: bool,
    success: bool,
}

fn observe_self_health_metrics<'a>(
    collection_duration: Duration,
    exporters: impl IntoIterator<Item = &'a ObserveMetricExporterHealth>,
) -> Vec<ObserveMetricRecord> {
    register_observe_self_metric_descriptors();
    let (metric_series, trace_records, dropped_traces) = OBSERVE_RUNTIME
        .get()
        .map(|runtime| {
            (
                runtime.metrics.slot_count(),
                runtime
                    .traces
                    .lock()
                    .map(|traces| traces.len())
                    .unwrap_or_default(),
                runtime.dropped_traces.load(Ordering::Relaxed),
            )
        })
        .unwrap_or_default();
    let mut records = vec![
        ObserveMetricRecord::gauge(
            "sinomo.observe.runtime.metric_series",
            BTreeMap::new(),
            metric_series as f64,
        ),
        ObserveMetricRecord::gauge(
            "sinomo.observe.trace_buffer.records",
            BTreeMap::new(),
            trace_records as f64,
        ),
        ObserveMetricRecord::gauge(
            "sinomo.observe.trace_buffer.utilization",
            BTreeMap::new(),
            trace_records as f64
                / OBSERVE_RUNTIME
                    .get()
                    .map_or(TRACE_BUFFER_LIMIT, |runtime| runtime.trace_capacity)
                    as f64,
        ),
        ObserveMetricRecord::counter(
            "sinomo.observe.trace_buffer.dropped",
            BTreeMap::new(),
            dropped_traces,
        ),
        ObserveMetricRecord::gauge(
            "sinomo.observe.collection.us",
            BTreeMap::new(),
            duration_as_micros_u64(collection_duration) as f64,
        ),
    ];
    for exporter in exporters {
        let labels = BTreeMap::from([
            ("exporter".to_string(), exporter.name.clone()),
            (
                "outcome".to_string(),
                if exporter.success { "success" } else { "error" }.to_string(),
            ),
            ("cached".to_string(), exporter.cached.to_string()),
        ]);
        records.push(ObserveMetricRecord::gauge(
            "sinomo.observe.metric_exporter.collection.us",
            labels,
            exporter.duration_us as f64,
        ));
        records.push(ObserveMetricRecord::gauge(
            "sinomo.observe.metric_exporter.records",
            BTreeMap::from([("exporter".to_string(), exporter.name.clone())]),
            exporter.record_count as f64,
        ));
    }
    records
}

fn append_observe_response_health(
    metrics: &mut Vec<ObserveMetricRecord>,
    request: &ObserveMetricsRequest,
) {
    let self_records = [
        "sinomo.observe.response.records",
        "sinomo.observe.response.truncated",
    ];
    let matches_prefix = |name: &str| {
        request
            .name_prefix
            .as_ref()
            .is_none_or(|prefix| name.starts_with(prefix))
    };
    let total = metrics
        .iter()
        .filter(|record| matches_prefix(record.name.as_str()))
        .count()
        + self_records
            .iter()
            .filter(|name| matches_prefix(name))
            .count();
    let truncated = request.limit > 0 && total > request.limit;
    let returned = if request.limit > 0 {
        total.min(request.limit)
    } else {
        total
    };
    metrics.push(ObserveMetricRecord::gauge(
        self_records[0],
        BTreeMap::new(),
        returned as f64,
    ));
    metrics.push(ObserveMetricRecord::gauge(
        self_records[1],
        BTreeMap::new(),
        if truncated { 1.0 } else { 0.0 },
    ));
}

fn register_observe_self_metric_descriptors() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        let register = |name, kind, unit, help, aggregation, labels| {
            register_observe_metric_descriptor(
                ObserveMetricDescriptor::new(name, kind, Some(unit), help).with_semantics(
                    ObserveMetricSemantics::new("observe", aggregation, labels),
                ),
            );
        };
        register(
            "sinomo.observe.runtime.metric_series",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            "Metric series retained by the process-local observation recorder.",
            ObserveMetricAggregation::Sum,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.trace_buffer.records",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            "Trace records currently retained in the bounded observation buffer.",
            ObserveMetricAggregation::Sum,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.trace_buffer.utilization",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Ratio,
            "Fraction of the bounded observation trace buffer currently occupied.",
            ObserveMetricAggregation::Max,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.trace_buffer.dropped",
            ObserveMetricKind::Counter,
            ObserveMetricUnit::Count,
            "Trace records evicted from the bounded observation buffer.",
            ObserveMetricAggregation::Sum,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.collection.us",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Microseconds,
            "Time spent collecting live metrics and snapshot exporters before response shaping.",
            ObserveMetricAggregation::Max,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.metric_exporter.collection.us",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Microseconds,
            "Time spent in the latest snapshot exporter collection.",
            ObserveMetricAggregation::Max,
            vec!["exporter", "outcome", "cached"],
        );
        register(
            "sinomo.observe.metric_exporter.records",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            "Metric records returned by the latest snapshot exporter collection.",
            ObserveMetricAggregation::Sum,
            vec!["exporter"],
        );
        register(
            "sinomo.observe.response.records",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Count,
            "Metric records returned by the latest observation response after filtering and limits.",
            ObserveMetricAggregation::Sum,
            Vec::<&str>::new(),
        );
        register(
            "sinomo.observe.response.truncated",
            ObserveMetricKind::Gauge,
            ObserveMetricUnit::Ratio,
            "Whether the latest observation metric response was truncated by its record limit.",
            ObserveMetricAggregation::Max,
            Vec::<&str>::new(),
        );
    });
}

struct ObserveMetricExporterCollection {
    records: Vec<ObserveMetricRecord>,
    observed_at: OffsetDateTime,
    duration_us: u64,
    cached: bool,
}

fn collect_observe_metric_exporter(
    registration: &ObserveMetricExporterRegistration,
    data_dir: &SinomoDataDir,
) -> Result<ObserveMetricExporterCollection, ObserveError> {
    let data_dir_root = data_dir.root();
    let generation = registration
        .cache_generation
        .as_ref()
        .map(|generation| generation(data_dir))
        .transpose()?;
    let cache_enabled = registration.cache_ttl.is_some() || generation.is_some();
    if cache_enabled
        && let Ok(cache) = registration.cache.lock()
        && let Some(cache) = cache.as_ref()
        && cache.data_dir == data_dir_root
        && registration
            .cache_ttl
            .is_none_or(|ttl| cache.cached_at.elapsed() <= ttl)
        && generation.is_none_or(|generation| cache.generation == Some(generation))
    {
        return Ok(ObserveMetricExporterCollection {
            records: cache.records.clone(),
            observed_at: cache.collected_at,
            duration_us: cache.duration_us,
            cached: true,
        });
    }

    let started = Instant::now();
    let observed_at = OffsetDateTime::now_utc();
    let records = (registration.exporter)(data_dir)?;
    let duration_us = duration_as_micros_u64(started.elapsed());
    if cache_enabled && let Ok(mut cache) = registration.cache.lock() {
        *cache = Some(ObserveMetricExporterCache {
            data_dir: data_dir_root.to_path_buf(),
            collected_at: observed_at,
            cached_at: Instant::now(),
            generation,
            duration_us,
            records: records.clone(),
        });
    }
    Ok(ObserveMetricExporterCollection {
        records,
        observed_at,
        duration_us,
        cached: false,
    })
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
    crate::observe_metrics_response_from_metrics(metrics)
}

fn observe_metric_exporters() -> &'static Mutex<BTreeMap<String, ObserveMetricExporterRegistration>>
{
    OBSERVE_METRIC_EXPORTERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn observe_metric_exporter_snapshot(
    request: &ObserveMetricsRequest,
) -> Vec<(String, ObserveMetricExporterRegistration)> {
    observe_metric_exporters()
        .lock()
        .map(|exporters| {
            exporters
                .iter()
                .filter(|(_, registration)| exporter_matches_request(registration, request))
                .map(|(name, registration)| (name.clone(), registration.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn observe_metric_descriptors_store() -> &'static Mutex<BTreeMap<String, ObserveMetricDescriptor>> {
    OBSERVE_METRIC_DESCRIPTORS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn exporter_matches_request(
    registration: &ObserveMetricExporterRegistration,
    request: &ObserveMetricsRequest,
) -> bool {
    let Some(requested) = request.name_prefix.as_deref() else {
        return true;
    };
    registration.prefixes.is_empty()
        || registration
            .prefixes
            .iter()
            .any(|prefix| prefix.starts_with(requested) || requested.starts_with(prefix))
}

fn duration_as_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn observe_error_code(error: &ObserveError) -> &'static str {
    match error {
        ObserveError::Io { .. } => "io",
        ObserveError::Json { .. } => "json",
        ObserveError::Parse { .. } => "parse",
        ObserveError::ModuleState { .. } => "module-state",
        ObserveError::Runtime { .. } => "runtime",
    }
}

fn parse_trace_capacity(value: Option<&str>) -> Result<usize, ObserveError> {
    let Some(value) = value else {
        return Ok(TRACE_BUFFER_LIMIT);
    };
    let capacity = value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=TRACE_MAX_CONFIGURED_RECORDS).contains(value));
    capacity.ok_or_else(|| ObserveError::Runtime {
        detail: format!("SINOMO_OBSERVE_TRACE_RECORDS must be 1..={TRACE_MAX_CONFIGURED_RECORDS}"),
    })
}

pub fn read_observe_traces(request: &ObserveTracesRequest) -> ObserveTracesResponse {
    let Some(runtime) = OBSERVE_RUNTIME.get() else {
        return ObserveTracesResponse {
            capacity: None,
            last_sequence: 0,
            first_available_sequence: None,
            process: Some(observe_trace_process()),
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
            capacity: None,
            last_sequence: 0,
            first_available_sequence: None,
            process: Some(observe_trace_process()),
            records: Vec::new(),
            capture_active,
            dropped_records: runtime.dropped_traces.load(Ordering::Relaxed),
            truncated: true,
        };
    };
    let dropped_records = runtime.dropped_traces.load(Ordering::Relaxed);
    let mut response = trace_ring_response(&traces, request, dropped_records);
    drop(traces);
    response.process = Some(observe_trace_process());
    response.capacity = Some(runtime.trace_capacity);
    response.capture_active = capture_active;
    response
}

fn trace_ring_response(
    traces: &VecDeque<ObserveTraceRecord>,
    request: &ObserveTracesRequest,
    dropped_records: u64,
) -> ObserveTracesResponse {
    let first_available_sequence = traces.front().map(|record| record.sequence);
    let last_sequence = traces.back().map_or_else(
        || {
            NEXT_OBSERVE_TRACE_SEQUENCE
                .load(Ordering::Relaxed)
                .saturating_sub(1)
        },
        |record| record.sequence,
    );
    let cursor_truncated = request
        .after_sequence
        .map_or(dropped_records > 0, |cursor| {
            cursor > last_sequence
                || first_available_sequence.is_some_and(|first| cursor.saturating_add(1) < first)
        });
    let mut records = traces
        .iter()
        .filter(|record| {
            request
                .after_sequence
                .is_none_or(|cursor| record.sequence > cursor)
                && request
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
    ObserveTracesResponse {
        capacity: None,
        process: None,
        records,
        capture_active: None,
        dropped_records,
        first_available_sequence,
        last_sequence,
        truncated: cursor_truncated || limit_truncated,
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
        let mut response = self.current_filter_state().to_control_response(
            capture_id,
            changed,
            cleared_trace_count,
        );
        response.live_log_writing = LOG_COLLECTOR_INSTALLED
            .load(Ordering::Acquire)
            .then(|| self.live_log_writing.load(Ordering::Acquire));
        response
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
            live_log_writing: None,
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
        self.live_log_writing.is_some()
            || self.changes_filter_state()
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
    trace_capacity: usize,
    logs: Arc<Mutex<crate::logs::LogBuffer>>,
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
    dropped_traces: Arc<AtomicU64>,
    capture: Arc<Mutex<Option<ActiveObserveCapture>>>,
}

impl<S> Layer<S> for ObserveTraceLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if metadata.target() == "sinomo.perf" && *metadata.level() == tracing::Level::TRACE {
            tracing::subscriber::Interest::sometimes()
        } else {
            tracing::subscriber::Interest::always()
        }
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>, ctx: Context<'_, S>) -> bool {
        metadata.target() != "sinomo.perf"
            || *metadata.level() != tracing::Level::TRACE
            || !observation_scope(ctx.lookup_current().as_ref())
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = JsonFieldVisitor::default();
        attrs.record(&mut fields);
        let capture = self.capture.lock().ok().and_then(|capture| {
            capture
                .as_ref()
                .filter(|capture| !capture.is_expired())
                .map(|capture| (capture.id.clone(), capture.expires_at))
        });
        let capture_id = capture.as_ref().map(|(id, _)| id.clone());
        let capture_lease = capture.map(|(_, lease)| lease);
        let own_observation = attrs.metadata().target() == "sinomo.perf"
            && fields
                .fields
                .get("observation")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
        let timed = !own_observation
            && !observation_scope(span.parent().as_ref())
            && attrs.metadata().target() == "sinomo.perf"
            && fields
                .fields
                .get("stage")
                .is_some_and(serde_json::Value::is_string)
            && capture_id.is_some();
        span.extensions_mut().insert(ObserveSpanState {
            id: NEXT_OBSERVE_SPAN_ID.fetch_add(1, Ordering::Relaxed),
            fields,
            started_ns: timed.then(crate::performance::monotonic_ns),
            capture_id,
            capture_lease,
        });
        if own_observation {
            self.mark_observation_request(&span);
        }
        if timed {
            self.record_span(&span, "span_started", false);
        }
    }

    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let newly_observed = {
            let mut extensions = span.extensions_mut();
            let Some(state) = extensions.get_mut::<ObserveSpanState>() else {
                return;
            };
            let before = state
                .fields
                .fields
                .get("observation")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
            values.record(&mut state.fields);
            !before
                && span.metadata().target() == "sinomo.perf"
                && state
                    .fields
                    .fields
                    .get("observation")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
        };
        if newly_observed {
            self.mark_observation_request(&span);
        }
    }

    fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && span
                .extensions()
                .get::<ObserveSpanState>()
                .is_some_and(|state| state.started_ns.is_some())
        {
            self.record_span(&span, "span_closed", true);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if metadata.target() == "sinomo.perf"
            && *metadata.level() == tracing::Level::TRACE
            && observation_scope(ctx.event_span(event).as_ref())
        {
            return;
        }
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);
        let (spans, context_truncated) = observe_span_context(ctx.event_span(event).as_ref());
        let traceparent = visitor
            .fields
            .get("traceparent")
            .and_then(serde_json::Value::as_str)
            .filter(|traceparent| !traceparent.is_empty())
            .map(ToString::to_string)
            .or_else(|| inherited_traceparent(&spans));
        let capture_id = self.capture.lock().ok().and_then(|capture| {
            capture
                .as_ref()
                .filter(|capture| !capture.is_expired())
                .map(|capture| capture.id.clone())
        });
        let record = ObserveTraceRecord {
            sequence: 0,
            monotonic_ns: crate::performance::monotonic_ns(),
            spans,
            context_truncated: context_truncated || visitor.truncated,
            timestamp: OffsetDateTime::now_utc(),
            level: metadata.level().to_string(),
            target: metadata.target().to_string(),
            name: metadata.name().to_string(),
            traceparent,
            capture_id,
            fields: serde_json::Value::Object(visitor.fields),
        };
        if let Ok(mut logs) = self.logs.lock() {
            use crate::OperationalSeverity as Severity;
            let severity = match *metadata.level() {
                tracing::Level::TRACE => Severity::Trace,
                tracing::Level::DEBUG => Severity::Debug,
                tracing::Level::INFO => Severity::Info,
                tracing::Level::WARN => Severity::Warn,
                tracing::Level::ERROR => Severity::Error,
            };
            let mut fields = record.fields.clone();
            let message = fields
                .as_object_mut()
                .and_then(|fields| fields.remove("message"))
                .map(|value| match value {
                    serde_json::Value::String(text) => text,
                    other => other.to_string(),
                })
                .unwrap_or_else(|| record.name.clone());
            logs.push(crate::OperationalLogRecord {
                timestamp: record.timestamp,
                severity,
                component: record.target.clone(),
                message,
                operation_id: fields
                    .get("operation_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
                fields,
                source: crate::ObserveLogSource::Runtime,
                truncated: visitor.truncated,
            });
        }
        self.push_trace(record);
    }
}

static NEXT_OBSERVE_SPAN_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_OBSERVE_TRACE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct ObserveSpanState {
    id: u64,
    fields: JsonFieldVisitor,
    started_ns: Option<u64>,
    capture_id: Option<String>,
    capture_lease: Option<Instant>,
}

fn observe_trace_process() -> ObserveTraceProcess {
    let (process_role, node_id) = observe_runtime_process_identity();
    ObserveTraceProcess {
        node_id,
        process_role,
        process_id: std::process::id(),
        started_at: observe_process_started_at(),
        observed_monotonic_ns: crate::performance::monotonic_ns(),
        io: crate::performance::ObserveProcessIo::sample(),
    }
}

fn inherited_traceparent(spans: &[ObserveSpanContext]) -> Option<String> {
    spans.iter().rev().find_map(|span| {
        span.fields
            .get("traceparent")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

fn observation_scope<S>(span: Option<&tracing_subscriber::registry::SpanRef<'_, S>>) -> bool
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    span.is_some_and(|span| {
        span.scope().any(|span| {
            span.metadata().target() == "sinomo.perf"
                && span
                    .extensions()
                    .get::<ObserveSpanState>()
                    .is_some_and(|state| {
                        state
                            .fields
                            .fields
                            .get("observation")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                    })
        })
    })
}

fn observe_span_context<S>(
    span: Option<&tracing_subscriber::registry::SpanRef<'_, S>>,
) -> (Vec<ObserveSpanContext>, bool)
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let mut result = Vec::new();
    let mut truncated = false;
    let mut bytes = 0;
    if let Some(span) = span {
        // Keep nearest ancestors when bounded. Never silently flatten colliding keys.
        for span in span.scope() {
            if result.len() >= TRACE_SCOPE_DEPTH_LIMIT {
                truncated = true;
                break;
            }
            let extensions = span.extensions();
            let Some(state) = extensions.get::<ObserveSpanState>() else {
                continue;
            };
            if bytes + state.fields.bytes > 16 * 1024 {
                truncated = true;
                break;
            }
            bytes += state.fields.bytes;
            truncated |= state.fields.truncated;
            result.push(ObserveSpanContext {
                span_id: state.id,
                name: span.metadata().name().to_owned(),
                target: span.metadata().target().to_owned(),
                fields: serde_json::Value::Object(state.fields.fields.clone()),
            });
        }
    }
    result.reverse();
    (result, truncated)
}

impl ObserveTraceLayer {
    fn mark_observation_request<S>(&self, span: &tracing_subscriber::registry::SpanRef<'_, S>)
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        let scope: Vec<_> = span.scope().collect();
        // Mark only this request envelope, never a shared connection/daemon span.
        let root = scope
            .iter()
            .position(|span| {
                span.extensions()
                    .get::<ObserveSpanState>()
                    .is_some_and(|state| {
                        state
                            .fields
                            .fields
                            .get("stage")
                            .and_then(serde_json::Value::as_str)
                            == Some("network.server")
                    })
            })
            .unwrap_or(0);
        for span in &scope[..=root] {
            if let Some(state) = span.extensions_mut().get_mut::<ObserveSpanState>() {
                state.fields.insert_named("observation", true.into());
            }
        }
        // One marker before the handler runs lets the snapshot exclude its own
        // unfinished RPC. Bulk response framing then produces no perf records.
        self.record_span(&scope[root], "observation", false);
    }

    fn push_trace(&self, mut record: ObserveTraceRecord) {
        if let Ok(mut traces) = self.traces.lock() {
            record.sequence = NEXT_OBSERVE_TRACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            if traces.len() >= self.trace_capacity {
                traces.pop_front();
                self.dropped_traces.fetch_add(1, Ordering::Relaxed);
            }
            traces.push_back(record);
        } else {
            self.dropped_traces.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_span<S>(
        &self,
        span: &tracing_subscriber::registry::SpanRef<'_, S>,
        kind: &str,
        closed: bool,
    ) where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        let now = crate::performance::monotonic_ns();
        let (spans, truncated) = observe_span_context(Some(span));
        let extensions = span.extensions();
        let Some(state) = extensions.get::<ObserveSpanState>() else {
            return;
        };
        let Some(started_ns) = state.started_ns else {
            return;
        };
        let mut fields = state.fields.fields.clone();
        fields.insert("record_kind".into(), kind.into());
        fields.insert("span_id".into(), state.id.into());
        fields.insert("started_monotonic_ns".into(), started_ns.into());
        if closed {
            fields.insert("duration_ns".into(), now.saturating_sub(started_ns).into());
            let same_capture = self.capture.lock().ok().is_some_and(|capture| {
                capture.as_ref().is_some_and(|capture| {
                    !capture.is_expired() && Some(capture.expires_at) == state.capture_lease
                })
            });
            fields.insert("capture_complete".into(), same_capture.into());
        }
        self.push_trace(ObserveTraceRecord {
            sequence: 0,
            monotonic_ns: now,
            timestamp: OffsetDateTime::now_utc(),
            level: span.metadata().level().to_string(),
            target: span.metadata().target().to_owned(),
            name: span.metadata().name().to_owned(),
            traceparent: inherited_traceparent(&spans),
            capture_id: state.capture_id.clone(),
            fields: serde_json::Value::Object(fields),
            spans,
            context_truncated: truncated,
        });
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    fields: serde_json::Map<String, serde_json::Value>,
    bytes: usize,
    truncated: bool,
}

impl JsonFieldVisitor {
    fn insert(&mut self, field: &field::Field, value: serde_json::Value) {
        self.insert_named(field.name(), value);
    }

    fn insert_named(&mut self, name: &str, value: serde_json::Value) {
        if let Some(previous) = self.fields.remove(name) {
            self.bytes = self
                .bytes
                .saturating_sub(name.len() + previous.to_string().len());
        }
        let bytes = name.len() + value.to_string().len();
        if self.fields.len() >= 64 || self.bytes + bytes > 16 * 1024 {
            self.truncated = true;
            return;
        }
        self.bytes += bytes;
        self.fields.insert(name.to_string(), value);
    }
}

#[derive(Default)]
struct BoundedFieldText {
    text: String,
    truncated: bool,
}
impl std::fmt::Write for BoundedFieldText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let mut end = value.len().min(4_096usize.saturating_sub(self.text.len()));
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&value[..end]);
        if end < value.len() {
            self.truncated = true;
            return Err(std::fmt::Error);
        }
        Ok(())
    }
}

impl field::Visit for JsonFieldVisitor {
    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        let mut text = BoundedFieldText::default();
        let _ = std::fmt::write(&mut text, format_args!("{value:?}"));
        self.truncated |= text.truncated;
        self.insert(field, serde_json::Value::String(text.text));
    }
    fn record_i64(&mut self, field: &field::Field, value: i64) {
        self.insert(field, serde_json::json!(value));
    }
    fn record_u64(&mut self, field: &field::Field, value: u64) {
        self.insert(field, serde_json::json!(value));
    }
    fn record_bool(&mut self, field: &field::Field, value: bool) {
        self.insert(field, serde_json::json!(value));
    }
    fn record_str(&mut self, field: &field::Field, value: &str) {
        let mut text = BoundedFieldText::default();
        let _ = std::fmt::Write::write_str(&mut text, value);
        self.truncated |= text.truncated;
        self.insert(field, serde_json::Value::String(text.text));
    }
}

pub(crate) fn runtime_logs_snapshot(
    request: &crate::ObserveLogsRequest,
) -> Option<(Vec<crate::OperationalLogRecord>, u64, bool)> {
    if !LOG_COLLECTOR_INSTALLED.load(Ordering::Acquire) {
        return None;
    }
    OBSERVE_RUNTIME
        .get()?
        .logs
        .lock()
        .ok()
        .map(|logs| logs.snapshot(request))
}

struct ObserveMetricsRecorder {
    id: u64,
    slots: RwLock<ObserveMetricSlots>,
}

static NEXT_OBSERVE_METRICS_RECORDER_ID: AtomicU64 = AtomicU64::new(1);

impl Default for ObserveMetricsRecorder {
    fn default() -> Self {
        Self {
            id: NEXT_OBSERVE_METRICS_RECORDER_ID.fetch_add(1, Ordering::Relaxed),
            slots: RwLock::new(ObserveMetricSlots::default()),
        }
    }
}

#[derive(Default)]
struct ObserveMetricSlots {
    counters: HashMap<Key, Arc<ObserveMetricSlot>>,
    gauges: HashMap<Key, Arc<ObserveMetricSlot>>,
    histograms: HashMap<Key, Arc<ObserveMetricSlot>>,
}

impl ObserveMetricSlots {
    fn for_kind(&self, kind: ObserveMetricKind) -> &HashMap<Key, Arc<ObserveMetricSlot>> {
        match kind {
            ObserveMetricKind::Counter => &self.counters,
            ObserveMetricKind::Gauge => &self.gauges,
            ObserveMetricKind::Histogram => &self.histograms,
        }
    }

    fn for_kind_mut(
        &mut self,
        kind: ObserveMetricKind,
    ) -> &mut HashMap<Key, Arc<ObserveMetricSlot>> {
        match kind {
            ObserveMetricKind::Counter => &mut self.counters,
            ObserveMetricKind::Gauge => &mut self.gauges,
            ObserveMetricKind::Histogram => &mut self.histograms,
        }
    }

    fn len(&self) -> usize {
        self.counters
            .len()
            .saturating_add(self.gauges.len())
            .saturating_add(self.histograms.len())
    }
}

#[derive(Default)]
struct ObserveMetricThreadCache {
    recorder_id: u64,
    slots: ObserveMetricSlots,
}

thread_local! {
    static OBSERVE_METRIC_THREAD_CACHE: RefCell<ObserveMetricThreadCache> =
        RefCell::new(ObserveMetricThreadCache::default());
}

impl ObserveMetricsRecorder {
    fn slot(&self, key: &Key, kind: ObserveMetricKind) -> Arc<ObserveMetricSlot> {
        if let Some(slot) = OBSERVE_METRIC_THREAD_CACHE.with(|cache| {
            let cache = cache.borrow();
            (cache.recorder_id == self.id)
                .then(|| cache.slots.for_kind(kind).get(key).cloned())
                .flatten()
        }) {
            return slot;
        }

        if let Some(slot) = self
            .slots
            .read()
            .expect("observe metrics lock poisoned")
            .for_kind(kind)
            .get(key)
        {
            let slot = Arc::clone(slot);
            self.remember_thread_slot(key, kind, &slot);
            return slot;
        }

        let mut slots = self.slots.write().expect("observe metrics lock poisoned");
        let slot = Arc::clone(
            slots
                .for_kind_mut(kind)
                .entry(key.to_retained())
                .or_insert_with(|| {
                    Arc::new(ObserveMetricSlot::new(
                        key.name().to_string(),
                        observe_metric_labels(key),
                        kind,
                    ))
                }),
        );
        drop(slots);
        self.remember_thread_slot(key, kind, &slot);
        slot
    }

    fn remember_thread_slot(
        &self,
        key: &Key,
        kind: ObserveMetricKind,
        slot: &Arc<ObserveMetricSlot>,
    ) {
        OBSERVE_METRIC_THREAD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.recorder_id != self.id {
                *cache = ObserveMetricThreadCache {
                    recorder_id: self.id,
                    slots: ObserveMetricSlots::default(),
                };
            }
            cache
                .slots
                .for_kind_mut(kind)
                .insert(key.to_retained(), Arc::clone(slot));
        });
    }

    fn snapshot(&self) -> Vec<ObserveMetricRecord> {
        let Ok(slots) = self.slots.read() else {
            return Vec::new();
        };
        slots
            .counters
            .values()
            .chain(slots.gauges.values())
            .chain(slots.histograms.values())
            .map(|slot| slot.snapshot())
            .collect()
    }

    fn slot_count(&self) -> usize {
        self.slots
            .read()
            .map(|slots| slots.len())
            .unwrap_or_default()
    }
}

impl Recorder for ObserveMetricsRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        describe_observe_metric(key, ObserveMetricKind::Counter, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        describe_observe_metric(key, ObserveMetricKind::Gauge, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        describe_observe_metric(key, ObserveMetricKind::Histogram, unit, description);
    }

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

fn describe_observe_metric(
    key: KeyName,
    kind: ObserveMetricKind,
    unit: Option<Unit>,
    description: SharedString,
) {
    register_observe_metric_descriptor(ObserveMetricDescriptor::new(
        key.as_str(),
        kind,
        unit.and_then(observe_metric_unit),
        description.to_string(),
    ));
}

fn observe_metric_unit(unit: Unit) -> Option<ObserveMetricUnit> {
    match unit {
        Unit::Count => Some(ObserveMetricUnit::Count),
        Unit::Bytes => Some(ObserveMetricUnit::Bytes),
        Unit::Seconds => Some(ObserveMetricUnit::Seconds),
        Unit::Microseconds => Some(ObserveMetricUnit::Microseconds),
        _ => None,
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
                buckets: Vec::new(),
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
                buckets: Vec::new(),
            },
            ObserveMetricValue::Histogram {
                count,
                sum,
                min,
                max,
                bucket_counts,
            } => ObserveMetricRecord {
                name: self.name.clone(),
                kind: self.kind,
                labels: self.labels.clone(),
                value: None,
                count: Some(count),
                sum: Some(sum),
                min,
                max,
                buckets: histogram_bucket_snapshot(&bucket_counts),
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
                bucket_counts,
            } = &mut *state
        {
            *count = count.saturating_add(1);
            *sum += value;
            *min = Some(min.map_or(value, |current| current.min(value)));
            *max = Some(max.map_or(value, |current| current.max(value)));
            if let Some(index) = OBSERVE_HISTOGRAM_BUCKET_UPPER_BOUNDS
                .iter()
                .position(|upper_bound| value <= *upper_bound)
                && let Some(bucket_count) = bucket_counts.get_mut(index)
            {
                *bucket_count = bucket_count.saturating_add(1);
            }
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
        bucket_counts: Vec<u64>,
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
                bucket_counts: vec![0; OBSERVE_HISTOGRAM_BUCKET_UPPER_BOUNDS.len()],
            },
        }
    }
}

fn histogram_bucket_snapshot(bucket_counts: &[u64]) -> Vec<ObserveHistogramBucket> {
    let mut cumulative = 0_u64;
    OBSERVE_HISTOGRAM_BUCKET_UPPER_BOUNDS
        .iter()
        .copied()
        .zip(bucket_counts.iter().copied())
        .map(|(upper_bound, bucket_count)| {
            cumulative = cumulative.saturating_add(bucket_count);
            ObserveHistogramBucket {
                upper_bound,
                count: cumulative,
            }
        })
        .collect()
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
    fn metric_recorder_reuses_a_registered_slot_without_rebuilding_its_key() {
        let recorder = ObserveMetricsRecorder::default();
        let key = Key::from_parts(
            "sinomo.test.hot_counter",
            vec![metrics::Label::new("path", "routed")],
        );

        let first = recorder.slot(&key, ObserveMetricKind::Counter);
        let second = recorder.slot(&key, ObserveMetricKind::Counter);

        assert!(Arc::ptr_eq(&first, &second));
        CounterFn::increment(first.as_ref(), 2);
        CounterFn::increment(second.as_ref(), 3);
        assert_eq!(recorder.slot_count(), 1);
        assert_eq!(recorder.snapshot()[0].value, Some(5.0));

        let replacement_recorder = ObserveMetricsRecorder::default();
        let replacement = replacement_recorder.slot(&key, ObserveMetricKind::Counter);
        assert!(!Arc::ptr_eq(&first, &replacement));
        assert_eq!(replacement_recorder.slot_count(), 1);
    }

    #[test]
    fn self_health_reports_current_exporter_state_with_bounded_labels() {
        let exporter = ObserveMetricExporterHealth {
            name: "sinomo-dfs".to_string(),
            duration_us: 321,
            record_count: 7,
            cached: true,
            success: true,
        };

        let records = observe_self_health_metrics(Duration::from_micros(900), [&exporter]);
        let collection = records
            .iter()
            .find(|record| record.name == "sinomo.observe.metric_exporter.collection.us")
            .expect("exporter collection metric");
        assert_eq!(collection.value, Some(321.0));
        assert_eq!(
            collection.labels.get("exporter"),
            Some(&"sinomo-dfs".to_string())
        );
        assert_eq!(
            collection.labels.get("outcome"),
            Some(&"success".to_string())
        );
        assert_eq!(collection.labels.get("cached"), Some(&"true".to_string()));
        assert!(records.iter().any(|record| {
            record.name == "sinomo.observe.metric_exporter.records" && record.value == Some(7.0)
        }));
        assert!(records.iter().any(|record| {
            record.name == "sinomo.observe.collection.us" && record.value == Some(900.0)
        }));
    }

    #[test]
    fn response_self_health_reports_filter_limit_truncation() {
        let request = ObserveMetricsRequest {
            name_prefix: Some("sinomo.observe.".to_string()),
            limit: 2,
        };
        let mut records = vec![
            ObserveMetricRecord::gauge(
                "sinomo.observe.runtime.metric_series",
                BTreeMap::new(),
                1.0,
            ),
            ObserveMetricRecord::gauge("sinomo.observe.trace_buffer.records", BTreeMap::new(), 1.0),
        ];

        append_observe_response_health(&mut records, &request);

        assert!(records.iter().any(|record| {
            record.name == "sinomo.observe.response.records" && record.value == Some(2.0)
        }));
        assert!(records.iter().any(|record| {
            record.name == "sinomo.observe.response.truncated" && record.value == Some(1.0)
        }));
    }

    #[test]
    fn histogram_snapshot_exposes_cumulative_buckets_and_quantiles() {
        let slot = ObserveMetricSlot::new(
            "sinomo.test.duration.us".to_string(),
            BTreeMap::new(),
            ObserveMetricKind::Histogram,
        );
        for value in [10.0, 20.0, 200.0, 2_000.0, 20_000.0] {
            HistogramFn::record(&slot, value);
        }

        let snapshot = slot.snapshot();
        assert_eq!(snapshot.count, Some(5));
        assert_eq!(snapshot.sum, Some(22_230.0));
        assert_eq!(snapshot.min, Some(10.0));
        assert_eq!(snapshot.max, Some(20_000.0));
        assert_eq!(snapshot.histogram_quantile(0.5), Some(250.0));
        assert_eq!(snapshot.histogram_quantile(0.95), Some(25_000.0));
        assert!(
            snapshot
                .buckets
                .windows(2)
                .all(|window| window[0].count <= window[1].count)
        );
        assert_eq!(snapshot.buckets.last().map(|bucket| bucket.count), Some(5));
    }

    #[test]
    fn metrics_response_accepts_legacy_payload_without_snapshot_metadata() {
        let response: ObserveMetricsResponse =
            serde_json::from_str(r#"{"metrics":[]}"#).expect("legacy metrics response");
        assert_eq!(response.observed_at, None);
        assert_eq!(response.process_started_at, None);
        assert_eq!(response.process_role, None);
        assert_eq!(response.node_id, None);
        assert!(response.exporters.is_empty());
        assert!(!response.partial);
        assert!(response.descriptors.is_empty());
    }

    #[test]
    fn metric_descriptor_semantics_are_rolling_compatible() {
        let legacy: ObserveMetricDescriptor = serde_json::from_str(
            r#"{"name":"sinomo.test.load","kind":"gauge","unit":"ratio","help":"Load."}"#,
        )
        .expect("legacy descriptor");
        assert_eq!(legacy.semantics, ObserveMetricSemantics::default());
        assert_eq!(
            legacy.effective_aggregation(),
            ObserveMetricAggregation::None
        );
        assert!(
            !serde_json::to_value(&legacy)
                .expect("legacy descriptor JSON")
                .as_object()
                .expect("descriptor object")
                .contains_key("semantics")
        );
        assert_eq!(
            serde_json::from_str::<ObserveMetricAggregation>("\"future-reduction\"")
                .expect("unknown aggregation fails closed"),
            ObserveMetricAggregation::None
        );

        let descriptor = ObserveMetricDescriptor::new(
            "sinomo.test.latency.us",
            ObserveMetricKind::Histogram,
            Some(ObserveMetricUnit::Microseconds),
            "Latency.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "test",
            ObserveMetricAggregation::MergeHistogram,
            ["outcome", "route", "outcome", ""],
        ));
        assert_eq!(
            descriptor.semantics.labels,
            vec!["outcome".to_string(), "route".to_string()]
        );
        assert_eq!(
            descriptor.effective_aggregation(),
            ObserveMetricAggregation::MergeHistogram
        );
        let encoded = serde_json::to_string(&descriptor).expect("semantic descriptor JSON");
        assert!(encoded.contains("\"aggregation\":\"merge-histogram\""));
        assert_eq!(
            serde_json::from_str::<ObserveMetricDescriptor>(&encoded)
                .expect("semantic descriptor round trip"),
            descriptor
        );
    }

    #[test]
    fn incompatible_metric_aggregation_fails_closed() {
        for aggregation in [
            ObserveMetricAggregation::Sum,
            ObserveMetricAggregation::Min,
            ObserveMetricAggregation::Max,
            ObserveMetricAggregation::Mean,
        ] {
            let descriptor = ObserveMetricDescriptor::new(
                "sinomo.test.histogram",
                ObserveMetricKind::Histogram,
                None,
                "Histogram.",
            )
            .with_semantics(ObserveMetricSemantics::new(
                "test",
                aggregation,
                std::iter::empty::<&str>(),
            ));
            assert_eq!(
                descriptor.effective_aggregation(),
                ObserveMetricAggregation::None
            );
        }

        let descriptor = ObserveMetricDescriptor::new(
            "sinomo.test.gauge",
            ObserveMetricKind::Gauge,
            None,
            "Gauge.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "test",
            ObserveMetricAggregation::MergeHistogram,
            std::iter::empty::<&str>(),
        ));
        assert_eq!(
            descriptor.effective_aggregation(),
            ObserveMetricAggregation::None
        );
    }

    #[test]
    fn interval_delta_resets_baseline_when_process_start_changes() {
        let mut previous =
            crate::observe_metrics_response_from_metrics(vec![ObserveMetricRecord::counter(
                "sinomo.test.requests",
                BTreeMap::new(),
                90,
            )]);
        previous.process_started_at = Some(OffsetDateTime::UNIX_EPOCH);
        let mut current =
            crate::observe_metrics_response_from_metrics(vec![ObserveMetricRecord::counter(
                "sinomo.test.requests",
                BTreeMap::new(),
                4,
            )]);
        current.process_started_at = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));

        let delta = observe_metrics_interval_delta(&previous, &current);
        assert_eq!(delta.metrics[0].value, Some(4.0));
        assert_eq!(delta.process_started_at, current.process_started_at);
    }

    #[test]
    fn exporter_prefixes_skip_unrelated_snapshot_collectors() {
        let registration = ObserveMetricExporterRegistration {
            prefixes: vec!["sinomo.kv.".to_string()],
            exporter: Arc::new(|_| Ok(Vec::new())),
            cache_ttl: None,
            cache_generation: None,
            cache: Arc::new(Mutex::new(None)),
        };
        assert!(exporter_matches_request(
            &registration,
            &ObserveMetricsRequest {
                name_prefix: Some("sinomo.kv.peer.".to_string()),
                limit: 0,
            }
        ));
        assert!(exporter_matches_request(
            &registration,
            &ObserveMetricsRequest {
                name_prefix: Some("sinomo.".to_string()),
                limit: 0,
            }
        ));
        assert!(!exporter_matches_request(
            &registration,
            &ObserveMetricsRequest {
                name_prefix: Some("sinomo.dfs.".to_string()),
                limit: 0,
            }
        ));
    }

    #[test]
    fn exporter_cache_reuses_one_bounded_snapshot_for_its_ttl() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registration = ObserveMetricExporterRegistration {
            prefixes: vec!["sinomo.test.".to_string()],
            exporter: {
                let calls = Arc::clone(&calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(vec![ObserveMetricRecord::gauge(
                        "sinomo.test.value",
                        BTreeMap::new(),
                        1.0,
                    )])
                })
            },
            cache_ttl: Some(Duration::from_secs(30)),
            cache_generation: None,
            cache: Arc::new(Mutex::new(None)),
        };
        let root = sinomo_testdir::TestDir::new("observe-exporter-cache");
        let data_dir = SinomoDataDir::new(root.path());

        let first =
            collect_observe_metric_exporter(&registration, &data_dir).expect("first collection");
        let second =
            collect_observe_metric_exporter(&registration, &data_dir).expect("cached collection");

        assert!(!first.cached);
        assert!(second.cached);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(first.records, second.records);
        assert_eq!(first.observed_at, second.observed_at);
    }

    #[test]
    fn exporter_cache_recollects_only_after_generation_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let generation = Arc::new(AtomicU64::new(1));
        let registration = ObserveMetricExporterRegistration {
            prefixes: vec!["sinomo.test.".to_string()],
            exporter: {
                let calls = Arc::clone(&calls);
                Arc::new(move |_| {
                    let value = calls.fetch_add(1, Ordering::Relaxed) + 1;
                    Ok(vec![ObserveMetricRecord::gauge(
                        "sinomo.test.value",
                        BTreeMap::new(),
                        value as f64,
                    )])
                })
            },
            cache_ttl: None,
            cache_generation: Some({
                let generation = Arc::clone(&generation);
                Arc::new(move |_| Ok(generation.load(Ordering::Acquire)))
            }),
            cache: Arc::new(Mutex::new(None)),
        };
        let root = sinomo_testdir::TestDir::new("observe-exporter-generation-cache");
        let data_dir = SinomoDataDir::new(root.path());

        let first =
            collect_observe_metric_exporter(&registration, &data_dir).expect("first collection");
        let cached =
            collect_observe_metric_exporter(&registration, &data_dir).expect("cached collection");
        generation.fetch_add(1, Ordering::AcqRel);
        let refreshed =
            collect_observe_metric_exporter(&registration, &data_dir).expect("generation refresh");

        assert!(!first.cached);
        assert!(cached.cached);
        assert!(!refreshed.cached);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(first.records, cached.records);
        assert_ne!(cached.records, refreshed.records);
    }

    #[test]
    fn metric_record_deserializes_without_histogram_buckets() {
        let record: ObserveMetricRecord = serde_json::from_str(
            r#"{"name":"legacy","kind":"histogram","count":1,"sum":5,"min":5,"max":5}"#,
        )
        .expect("legacy metric record");
        assert!(record.buckets.is_empty());
        assert_eq!(record.histogram_quantile(0.95), Some(5.0));
    }

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
    fn dfs_perf_trace_env_adds_exact_trace_target() {
        let filter = observe_filter_with_dfs_perf_trace("warn,sinomo=info", true);
        assert_eq!(filter, "warn,sinomo=info,sinomo.dfs.perf=trace");
        assert_eq!(
            observe_filter_with_dfs_perf_trace("warn,sinomo=info", false),
            "warn,sinomo=info"
        );

        let observed = Arc::new(AtomicUsize::new(0));
        let subscriber = Registry::default()
            .with(EnvFilter::try_new(filter).expect("DFS performance filter parses"))
            .with(CountingLayer {
                observed: Arc::clone(&observed),
            });
        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(target: "sinomo.dfs.perf", "enabled DFS performance event");
            tracing::debug!(target: "unrelated", "filtered unrelated event");
        });
        assert_eq!(observed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dfs_perf_trace_env_preserves_explicit_target_directive() {
        assert_eq!(
            observe_filter_with_dfs_perf_trace("warn,sinomo.dfs.perf=debug", true),
            "warn,sinomo.dfs.perf=debug"
        );
        assert_eq!(
            observe_filter_with_dfs_perf_trace("warn,sinomo.dfs.perf=off", true),
            "warn,sinomo.dfs.perf=off"
        );
    }

    #[test]
    fn dfs_perf_trace_env_uses_explicit_truthy_values() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(
                observe_env_value_is_truthy(value),
                "{value:?} should be truthy"
            );
        }
        for value in ["", "0", "false", "no", "off", "trace"] {
            assert!(
                !observe_env_value_is_truthy(value),
                "{value:?} should not be truthy"
            );
        }
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
            trace_capacity: TRACE_BUFFER_LIMIT,
            logs: Arc::new(Mutex::new(crate::logs::LogBuffer::default())),
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
            sequence: 0,
            monotonic_ns: 0,
            spans: Vec::new(),
            context_truncated: false,
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
            trace_capacity: TRACE_BUFFER_LIMIT,
            logs: Arc::new(Mutex::new(crate::logs::LogBuffer::default())),
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
    fn logs_capture_enabled_events_and_survive_trace_clear() {
        let logs = Arc::new(Mutex::new(crate::logs::LogBuffer::default()));
        let traces = Arc::new(Mutex::new(VecDeque::new()));
        let subscriber = Registry::default()
            .with(EnvFilter::new("warn"))
            .with(ObserveTraceLayer {
                trace_capacity: TRACE_BUFFER_LIMIT,
                logs: logs.clone(),
                traces: traces.clone(),
                dropped_traces: Arc::new(AtomicU64::new(0)),
                capture: Arc::new(Mutex::new(None)),
            });
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "sinomo.test.logs", "filtered");
            tracing::warn!(target: "sinomo.test.logs", operation_id = "operation-7", count = 3u64, "retry scheduled");
            traces.lock().unwrap().clear();
            tracing::error!(target: "sinomo.test.logs", payload = %"界".repeat(50_000), "large field");
        });
        let (records, _, _) = logs
            .lock()
            .unwrap()
            .snapshot(&crate::ObserveLogsRequest::default());
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].message, "retry scheduled");
        assert_eq!(records[0].operation_id.as_deref(), Some("operation-7"));
        assert_eq!(records[0].fields["count"], 3);
        assert!(records[1].truncated);
        assert_eq!(traces.lock().unwrap().len(), 1);
    }

    #[test]
    fn dfs_routine_repair_observations_stay_in_ram_and_failures_persist() {
        let root = sinomo_testdir::TestDir::new("observe-dfs-repair-summary");
        let path = root.path().join("formatted.log");
        let file = Arc::new(std::fs::File::create(&path).unwrap());
        let logs = Arc::new(Mutex::new(crate::logs::LogBuffer::default()));
        let traces = Arc::new(Mutex::new(VecDeque::new()));
        let live = Arc::new(AtomicBool::new(false));
        let subscriber = Registry::default().with(EnvFilter::new("debug,sinomo.perf=trace"))
            .with(ObserveTraceLayer {
                trace_capacity: TRACE_BUFFER_LIMIT, logs: logs.clone(), traces: traces.clone(),
                dropped_traces: Arc::new(AtomicU64::new(0)), capture: Arc::new(Mutex::new(None)),
            }).with(fmt::layer().with_ansi(false).with_writer(file)
                .with_filter(formatted_log_filter(live.clone(), false)));
        let attempt = sinomo_dfs::DfsRepairAttemptRecord {
            intent_id: "original-unfinished-repair".into(),
            object_id: sinomo_dfs::DfsObjectId::generate(), version: sinomo_dfs::DfsVersion::generate(),
            content_hash: "retained-content".into(), missing_replica_node_id: sinomo_core::node::NodeId::generate(),
            attempts: 1, in_flight: true, last_attempt_unix_nanos: 1, next_retry_unix_nanos: 0,
            transfer_request_id: Some("original-transfer".into()), last_error: None,
            transfer_delivery_state: None, completed_transfer_request_id: None, transfer_kind: None,
        };
        let report = sinomo_dfs::DfsMaintenanceTickReport {
            namespace_jobs: Default::default(), write_intents: Default::default(), placement_reconciliation: None,
            lifecycle_reports: Vec::new(), scheduled_repairs: Vec::new(), followup_events: Vec::new(),
            repair_attempts_before: vec![attempt.clone()], repair_attempts_after: vec![attempt],
            repair_budget_exhausted: false, repair_page_checkpoint: Default::default(), metadata_replication: sinomo_dfs::DfsMetadataJournalReplicationReport { outcomes: Vec::new() },
        };
        let observations = sinomo_dfs::dfs_maintenance_tick_observations(&report);
        assert_eq!(observations.repair_attempt_in_flight_count, 1);
        tracing::subscriber::with_default(subscriber, || {
            observations.emit_daemon_notice_logs();
            observations.emit_daemon_notice_logs();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 0,
                "unchanged routine repair summaries must not append formatted disk logs");
            let mut failed = report.clone();
            failed.followup_events.push(sinomo_dfs::NetworkedDfsServiceEvent::RepairRetryFailed { reason: "controlled failure".into() });
            sinomo_dfs::dfs_maintenance_tick_observations(&failed).emit_daemon_notice_logs();
            let warning = std::fs::read_to_string(&path).unwrap();
            assert!(warning.contains("DFS repair retry failed: controlled failure"));
            assert!(!warning.contains("DFS repairs: scheduled="));
            live.store(true, Ordering::Release);
            observations.emit_daemon_notice_logs();
            let explicit = std::fs::read_to_string(&path).unwrap();
            assert!(explicit.contains("DFS repairs: scheduled=0, followups=0"));
            assert!(explicit.contains("active=1, in_flight=1"));
            live.store(false, Ordering::Release);
            observations.emit_daemon_notice_logs();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), explicit);
        });
        let (records, _, _) = logs.lock().unwrap().snapshot(&crate::ObserveLogsRequest::default());
        let routine = records.iter().filter(|r| r.message.contains("DFS repairs:") || r.message.contains("DFS repair attempts:")).collect::<Vec<_>>();
        assert_eq!(routine.len(), 10, "all routine summaries remain observable in RAM");
        assert!(routine.iter().all(|r| r.severity == crate::OperationalSeverity::Info));
        assert!(records.iter().any(|r| r.message.contains("controlled failure")));
        assert_eq!(report.repair_attempts_before, report.repair_attempts_after, "observation cannot complete or mutate retained repair work");
    }

    #[test]
    fn routine_logs_stay_in_memory_and_live_writing_has_no_backlog() {
        let root = sinomo_testdir::TestDir::new("observe-live-log-writing");
        let path = root.path().join("formatted.log");
        let file = Arc::new(std::fs::File::create(&path).unwrap());
        let logs = Arc::new(Mutex::new(crate::logs::LogBuffer::default()));
        let traces = Arc::new(Mutex::new(VecDeque::new()));
        let live = Arc::new(AtomicBool::new(false));
        let subscriber = Registry::default()
            .with(EnvFilter::new("trace"))
            .with(ObserveTraceLayer {
                trace_capacity: TRACE_BUFFER_LIMIT,
                logs: Arc::clone(&logs),
                traces: Arc::clone(&traces),
                dropped_traces: Arc::new(AtomicU64::new(0)),
                capture: Arc::new(Mutex::new(Some(ActiveObserveCapture::new(
                    "retention-control".into(),
                    None,
                )))),
            })
            .with(
                fmt::layer()
                    .with_ansi(false)
                    .with_writer(file)
                    .with_filter(formatted_log_filter(Arc::clone(&live), false)),
            );
        // Reuse the SAME callsites across switches, proving that cached interest
        // cannot keep a routine event enabled/disabled after the mode changes.
        fn emit_routine(phase: &str) {
            let span = tracing::info_span!("retention-context", owner = "main-or-shadow");
            let _entered = span.enter();
            tracing::trace!(target: "sinomo.test.retention", phase, sequence_in_phase = 1u64, "routine trace");
            tracing::debug!(target: "sinomo.test.retention", phase, sequence_in_phase = 2u64, "routine debug");
            tracing::info!(target: "sinomo.test.retention", phase, sequence_in_phase = 3u64, "routine info");
        }
        tracing::subscriber::with_default(subscriber, || {
            emit_routine("memory-before");
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
            tracing::warn!(target: "sinomo.test.retention", operation_id = "warn-original", "persistent warning");
            tracing::error!(target: "sinomo.test.retention", operation_id = "error-original", "persistent error");
            let warning_text = std::fs::read_to_string(&path).unwrap();
            assert!(warning_text.contains("persistent warning"));
            assert!(warning_text.contains("persistent error"));
            assert!(!warning_text.contains("routine"));

            live.store(true, Ordering::Release);
            emit_routine("live-now");
            let live_text = std::fs::read_to_string(&path).unwrap();
            assert!(live_text.contains("routine trace"));
            assert!(live_text.contains("routine debug"));
            assert!(live_text.contains("routine info"));
            assert!(live_text.contains("live-now"));
            assert!(!live_text.contains("memory-before"));

            live.store(false, Ordering::Release);
            emit_routine("memory-after");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), live_text);
            // A subsequent enable also must not replay the retained backlog.
            live.store(true, Ordering::Release);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), live_text);
            live.store(false, Ordering::Release);
        });
        let (records, _, _) = logs
            .lock()
            .unwrap()
            .snapshot(&crate::ObserveLogsRequest::default());
        assert_eq!(records.len(), 11);
        for phase in ["memory-before", "live-now", "memory-after"] {
            assert_eq!(
                records
                    .iter()
                    .filter(|r| r.fields["phase"] == phase)
                    .count(),
                3
            );
        }
        let traces = traces.lock().unwrap();
        for phase in ["memory-before", "live-now", "memory-after"] {
            let records = traces
                .iter()
                .filter(|r| r.fields["phase"] == phase)
                .collect::<Vec<_>>();
            assert_eq!(records.len(), 3);
            assert!(
                records
                    .iter()
                    .all(|r| r.capture_id.as_deref() == Some("retention-control"))
            );
            assert!(records.iter().all(|r| {
                r.spans
                    .iter()
                    .any(|s| s.name == "retention-context" && s.fields["owner"] == "main-or-shadow")
            }));
            assert!(records.windows(2).all(|r| r[0].sequence < r[1].sequence));
        }
    }

    #[test]
    fn live_writing_does_not_override_the_global_observe_filter() {
        let root = sinomo_testdir::TestDir::new("observe-live-global-filter");
        let path = root.path().join("formatted.log");
        let logs = Arc::new(Mutex::new(crate::logs::LogBuffer::default()));
        let traces = Arc::new(Mutex::new(VecDeque::new()));
        let subscriber = Registry::default()
            .with(EnvFilter::new("warn"))
            .with(ObserveTraceLayer {
                trace_capacity: TRACE_BUFFER_LIMIT,
                logs: Arc::clone(&logs),
                traces: Arc::clone(&traces),
                dropped_traces: Arc::new(AtomicU64::new(0)),
                capture: Arc::new(Mutex::new(None)),
            })
            .with(
                fmt::layer()
                    .with_ansi(false)
                    .with_writer(Arc::new(std::fs::File::create(&path).unwrap()))
                    .with_filter(formatted_log_filter(Arc::new(AtomicBool::new(true)), false)),
            );
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("excluded routine event");
            tracing::warn!("included warning event");
        });
        let text = std::fs::read_to_string(path).unwrap();
        assert!(!text.contains("excluded routine event"));
        assert!(text.contains("included warning event"));
        let (records, _, _) = logs
            .lock()
            .unwrap()
            .snapshot(&crate::ObserveLogsRequest::default());
        assert_eq!(records.len(), 1);
        assert_eq!(traces.lock().unwrap().len(), 1);
    }

    #[test]
    fn legacy_control_response_does_not_claim_live_writing_disabled() {
        let response: ObserveControlResponse = serde_json::from_str(
            r#"{"active_filter":"trace","base_filter":"trace","quiet_filter_directives":[],"changed":false}"#,
        ).unwrap();
        assert_eq!(response.live_log_writing, None);
        let request: ObserveControlRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(request.live_log_writing, None);
        assert!(!request.changes_runtime_state());
        let request: ObserveControlRequest =
            serde_json::from_str(r#"{"live_log_writing":false}"#).unwrap();
        assert!(request.changes_runtime_state());
        assert!(!request.changes_filter_state());
    }

    #[test]
    fn capture_expiry_restores_owned_filter_state() {
        let original = ObserveFilterState::new("warn,sinomo=info".to_string(), Vec::new());
        let capture_filter =
            ObserveFilterState::new("warn,sinomo.dfs.perf=trace".to_string(), Vec::new());
        let filter = EnvFilter::try_new(original.active_filter.as_str()).expect("filter");
        let (_filter_layer, filter_handle) = reload::Layer::new(filter);
        let runtime = ObserveRuntime {
            trace_capacity: TRACE_BUFFER_LIMIT,
            logs: Arc::new(Mutex::new(crate::logs::LogBuffer::default())),
            config: ObserveRuntimeConfig {
                live_log_writing: false,
                process_role: "test".to_string(),
                node_id: None,
                filter: original.base_filter.clone(),
                quiet_filter_directives: original.quiet_filter_directives.clone(),
            },
            started_at: OffsetDateTime::now_utc(),
            filter: Arc::new(Mutex::new(original.clone())),
            filter_handle,
            metrics: Arc::new(ObserveMetricsRecorder::default()),
            traces: Arc::new(Mutex::new(VecDeque::new())),
            dropped_traces: Arc::new(AtomicU64::new(0)),
            capture: Arc::new(Mutex::new(None)),
            control_update: Arc::new(Mutex::new(())),
            live_log_writing: Arc::new(AtomicBool::new(false)),
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

#[cfg(test)]
#[path = "span_tests.rs"]
mod span_tests;
