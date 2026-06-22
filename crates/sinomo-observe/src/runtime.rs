use crate::ObserveError;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use time::OffsetDateTime;
use tracing::{Event, Subscriber, field};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry, fmt};

pub const OBSERVE_DEFAULT_FILTER: &str = "warn,sinomo=info";
const TRACE_BUFFER_LIMIT: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserveRuntimeConfig {
    pub process_role: String,
    pub node_id: Option<String>,
    pub filter: String,
}

impl ObserveRuntimeConfig {
    pub fn daemon(label: impl Into<String>, node_id: Option<String>) -> Self {
        Self {
            process_role: label.into(),
            node_id,
            filter: std::env::var("SINOMO_OBSERVE_FILTER")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| OBSERVE_DEFAULT_FILTER.to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveControlRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default)]
    pub reset: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveControlResponse {
    pub active_filter: String,
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
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveTracesResponse {
    pub records: Vec<ObserveTraceRecord>,
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
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub fields: serde_json::Value,
}

#[derive(Clone)]
struct ObserveRuntime {
    config: ObserveRuntimeConfig,
    filter: Arc<Mutex<String>>,
    filter_handle: reload::Handle<EnvFilter, Registry>,
    metrics: Arc<ObserveMetricsRecorder>,
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
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
    if OBSERVE_RUNTIME.get().is_some() {
        return Ok(());
    }

    let filter =
        EnvFilter::try_new(config.filter.as_str()).map_err(|source| ObserveError::Runtime {
            detail: format!("invalid observe filter `{}`: {source}", config.filter),
        })?;
    let (filter_layer, filter_handle) = reload::Layer::new(filter);
    let metrics = Arc::new(ObserveMetricsRecorder::default());
    let traces = Arc::new(Mutex::new(VecDeque::with_capacity(TRACE_BUFFER_LIMIT)));
    let runtime = ObserveRuntime {
        filter: Arc::new(Mutex::new(config.filter.clone())),
        filter_handle,
        metrics: Arc::clone(&metrics),
        traces: Arc::clone(&traces),
        config,
    };

    let _ = metrics::set_global_recorder(Arc::clone(&metrics));
    let _ = OBSERVE_RUNTIME.set(runtime);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(ObserveTraceLayer { traces })
        .with(fmt::layer().with_target(true))
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
    let next_filter = if request.reset {
        OBSERVE_DEFAULT_FILTER.to_string()
    } else if let Some(filter) = request.filter {
        filter
    } else {
        return Ok(ObserveControlResponse {
            active_filter: runtime.active_filter(),
            changed: false,
        });
    };
    let parsed =
        EnvFilter::try_new(next_filter.as_str()).map_err(|source| ObserveError::Runtime {
            detail: format!("invalid observe filter `{next_filter}`: {source}"),
        })?;
    runtime
        .filter_handle
        .modify(|filter| *filter = parsed)
        .map_err(|source| ObserveError::Runtime {
            detail: format!("failed to reload observe filter: {source}"),
        })?;
    if let Ok(mut active_filter) = runtime.filter.lock() {
        *active_filter = next_filter.clone();
    }
    Ok(ObserveControlResponse {
        active_filter: next_filter,
        changed: true,
    })
}

pub fn observe_control_snapshot() -> ObserveControlResponse {
    ObserveControlResponse {
        active_filter: OBSERVE_RUNTIME
            .get()
            .map(ObserveRuntime::active_filter)
            .unwrap_or_else(|| OBSERVE_DEFAULT_FILTER.to_string()),
        changed: false,
    }
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
        };
    };
    let Ok(traces) = runtime.traces.lock() else {
        return ObserveTracesResponse {
            records: Vec::new(),
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
        })
        .cloned()
        .collect::<Vec<_>>();
    if request.limit > 0 && records.len() > request.limit {
        records = records.split_off(records.len() - request.limit);
    }
    ObserveTracesResponse { records }
}

impl ObserveRuntime {
    fn active_filter(&self) -> String {
        self.filter
            .lock()
            .map(|filter| filter.clone())
            .unwrap_or_else(|_| self.config.filter.clone())
    }
}

#[derive(Clone)]
struct ObserveTraceLayer {
    traces: Arc<Mutex<VecDeque<ObserveTraceRecord>>>,
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
            .map(ToString::to_string);
        let record = ObserveTraceRecord {
            timestamp: OffsetDateTime::now_utc(),
            level: metadata.level().to_string(),
            target: metadata.target().to_string(),
            name: metadata.name().to_string(),
            traceparent,
            fields: serde_json::Value::Object(visitor.fields),
        };
        if let Ok(mut traces) = self.traces.lock() {
            if traces.len() >= TRACE_BUFFER_LIMIT {
                traces.pop_front();
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
