//! App observation RPC values, sharing the module-facing metric vocabulary.
use serde::{Deserialize, Serialize};
pub use sinomo_module_api::observe::*;
use time::OffsetDateTime;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_prefix: Option<String>,
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveMetricsResponse {
    /// Wall-clock time at which this process assembled the response. Older
    /// daemons omit it, so clients must retain their local receive time as a
    /// compatibility fallback.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub observed_at: Option<OffsetDateTime>,
    /// Start time of the process that owns the cumulative counters. A change
    /// tells interval consumers to treat the next sample as a fresh baseline.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub process_started_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Snapshot exporters that were actually invoked for this request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exporters: Vec<ObserveMetricExporterStatus>,
    /// True when one or more requested exporters failed. Successfully
    /// collected metrics remain useful and are still returned.
    #[serde(default)]
    pub partial: bool,
    /// Metric-family metadata for records present in this snapshot. Carrying
    /// it over the observe route keeps remote Prometheus bridges from guessing
    /// units or descriptions from metric names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub descriptors: Vec<ObserveMetricDescriptor>,
    pub metrics: Vec<ObserveMetricRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricExporterStatus {
    pub name: String,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub duration_us: u64,
    #[serde(default)]
    pub cached: bool,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

pub const OBSERVE_METRICS_ROUTE: &str = "/observe/metrics";

pub const OBSERVE_RPC_CATEGORY: &str = "observe";

pub const SINOMO_OBSERVE_MODULE_LEGACY_ID: &str = "module.observe";

pub const OBSERVE_LOGS_ROUTE: &str = "/observe/logs";
pub const OBSERVE_LOGS_DEFAULT_LIMIT: usize = 200;
pub const OBSERVE_LOGS_MAX_RECORDS: usize = 1_000;
pub const OBSERVE_LOGS_MAX_RESPONSE_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
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
}

impl std::fmt::Display for OperationalSeverity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveLogSource {
    /// Explicitly recorded, durable operational diagnostics.
    #[default]
    Operational,
    /// Enabled tracing events retained by the current daemon process.
    Runtime,
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
    #[serde(default)]
    pub source: ObserveLogSource,
    /// A field or message exceeded the per-record collection bound.
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveLogsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_severity: Option<OperationalSeverity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ObserveLogSource>,
    /// Zero selects the default; all requests are bounded by the service.
    #[serde(default)]
    pub limit: usize,
}

impl ObserveLogsRequest {
    pub fn bounded_limit(&self) -> usize {
        if self.limit == 0 {
            OBSERVE_LOGS_DEFAULT_LIMIT
        } else {
            self.limit.min(OBSERVE_LOGS_MAX_RECORDS)
        }
    }

    pub fn matches(&self, record: &OperationalLogRecord) -> bool {
        self.component
            .as_ref()
            .is_none_or(|v| *v == record.component)
            && self
                .operation_id
                .as_ref()
                .is_none_or(|v| Some(v) == record.operation_id.as_ref())
            && self.min_severity.is_none_or(|v| record.severity >= v)
            && self.source.is_none_or(|v| record.source == v)
            && self.contains.as_ref().is_none_or(|v| {
                let needle = v.to_lowercase();
                record.message.to_lowercase().contains(&needle)
                    || record.component.to_lowercase().contains(&needle)
                    || record
                        .operation_id
                        .as_ref()
                        .is_some_and(|id| id.to_lowercase().contains(&needle))
                    || record.fields.to_string().to_lowercase().contains(&needle)
            })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ObserveLogsResponse {
    /// Oldest first within this bounded tail. Node clocks are not a cluster ordering guarantee.
    pub records: Vec<OperationalLogRecord>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub observed_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub process_started_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub process_role: Option<String>,
    #[serde(default)]
    pub node_id: Option<String>,
    /// False for older servers and processes without an installed collector.
    #[serde(default)]
    pub runtime_available: bool,
    #[serde(default)]
    pub runtime_evicted: u64,
    /// More records exist outside the returned tail or bounded disk scan.
    #[serde(default)]
    pub truncated: bool,
    /// Source failures do not discard logs successfully collected from other sources.
    #[serde(default)]
    pub errors: Vec<String>,
}

#[cfg(test)]
mod log_tests {
    use super::*;

    #[test]
    fn legacy_logs_decode_without_claiming_runtime_collection() {
        let response: ObserveLogsResponse = serde_json::from_str(r#"{"records":[{"timestamp":"2026-09-22T00:00:00Z","severity":"info","component":"dfs","message":"recovered"}]}"#).unwrap();
        assert!(!response.runtime_available);
        assert_eq!(response.records[0].source, ObserveLogSource::Operational);
        assert_eq!(ObserveLogsRequest::default().bounded_limit(), 200);
        assert_eq!(
            ObserveLogsRequest {
                limit: usize::MAX,
                ..Default::default()
            }
            .bounded_limit(),
            1_000
        );
    }
}

pub mod recording;

pub const OBSERVE_MAX_CONCURRENT_NODES: usize = 8;
/// Shared collection policy for live metrics, logs and retained usage. It keeps
/// target order and target identity even when a worker fails.
pub async fn collect_bounded<Target, Observe, Fut, Response>(
    targets: Vec<Target>,
    observe: Observe,
) -> Vec<(Target, Result<Response, String>)>
where
    Target: Clone + Send + 'static,
    Observe: Fn(Target) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<Response, String>> + Send + 'static,
    Response: Send + 'static,
{
    let mut samples = Vec::with_capacity(targets.len());
    for chunk in targets.chunks(OBSERVE_MAX_CONCURRENT_NODES) {
        let jobs = chunk
            .iter()
            .cloned()
            .map(|target| {
                let work = target.clone();
                let observe = observe.clone();
                (
                    target,
                    tokio::spawn(async move { Box::pin(observe(work)).await }),
                )
            })
            .collect::<Vec<_>>();
        for (target, job) in jobs {
            samples.push((
                target,
                match job.await {
                    Ok(value) => value,
                    Err(error) => Err(format!("cluster observation task failed: {error}")),
                },
            ));
        }
    }
    samples
}
