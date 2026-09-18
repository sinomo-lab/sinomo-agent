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
