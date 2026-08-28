//! Data-only observation contracts shared with independently built components.
//!
//! This crate deliberately contains no recorder, registry, daemon, or runtime
//! implementation. A dynamic module can link it without pulling a second copy
//! of `sinomo-observe` (or its process-global metrics state) into its artifact.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const OBSERVE_METRIC_EXPORTER_INTERFACE_ID: &str = "sinomo.observe.metric-exporter";
pub const OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION: &str = "1.0.0";
pub const OBSERVE_METRIC_EXPORTER_OPERATION: &str = "observe.metrics.collect";
pub const OBSERVE_METRIC_EXPORTER_CATEGORY: &str = "observe.metric-exporter";
pub const OBSERVE_METRIC_EXPORTER_MAX_RECORDS: usize = 16_384;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricExportRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_prefix: Option<String>,
    /// Maximum desired records. Zero selects the host maximum.
    #[serde(default)]
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ObserveMetricExportResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub descriptors: Vec<ObserveMetricDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<ObserveMetricRecord>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveMetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveMetricUnit {
    Count,
    Bytes,
    Seconds,
    Microseconds,
    Ratio,
}

/// Safe cross-series reduction for a metric family. `None` is intentionally
/// the default: an absent or older descriptor must never make a client guess
/// that values from different nodes can be combined.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveMetricAggregation {
    Sum,
    Min,
    Max,
    Mean,
    MergeHistogram,
    /// Missing and future unknown reductions fail closed.
    #[default]
    #[serde(other)]
    None,
}

impl ObserveMetricAggregation {
    /// Whether this reduction is structurally compatible with the metric
    /// record kind. Domain-level correctness still comes from the descriptor.
    pub fn is_compatible_with(self, kind: ObserveMetricKind) -> bool {
        match self {
            Self::None => true,
            Self::MergeHistogram => kind == ObserveMetricKind::Histogram,
            Self::Sum | Self::Min | Self::Max | Self::Mean => kind != ObserveMetricKind::Histogram,
        }
    }

    fn is_none(value: &Self) -> bool {
        *value == Self::None
    }
}

/// Optional operator-facing semantics. `labels` contains bounded dimension
/// names, never observed label values.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricSemantics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "ObserveMetricAggregation::is_none")]
    pub aggregation: ObserveMetricAggregation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

impl ObserveMetricSemantics {
    pub fn new(
        domain: impl Into<String>,
        aggregation: ObserveMetricAggregation,
        labels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let domain = domain.into();
        let domain = domain.trim();
        let mut labels = labels
            .into_iter()
            .map(Into::into)
            .filter(|label: &String| !label.is_empty())
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup();
        Self {
            domain: (!domain.is_empty()).then(|| domain.to_string()),
            aggregation,
            labels,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.domain.is_none()
            && self.aggregation == ObserveMetricAggregation::None
            && self.labels.is_empty()
    }

    /// Merges metadata from a rolling-version peer. Missing legacy metadata is
    /// neutral; conflicting new aggregation declarations fail closed.
    pub fn merge_rolling(&mut self, incoming: &Self) {
        if self.is_empty() {
            *self = incoming.clone();
            return;
        }
        if incoming.is_empty() || self == incoming {
            return;
        }
        if self.domain != incoming.domain {
            self.domain = None;
            self.aggregation = ObserveMetricAggregation::None;
        }
        if self.aggregation != incoming.aggregation {
            self.aggregation = ObserveMetricAggregation::None;
        }
        self.labels.extend(incoming.labels.iter().cloned());
        self.labels.sort();
        self.labels.dedup();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObserveMetricDescriptor {
    pub name: String,
    pub kind: ObserveMetricKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<ObserveMetricUnit>,
    pub help: String,
    #[serde(default, skip_serializing_if = "ObserveMetricSemantics::is_empty")]
    pub semantics: ObserveMetricSemantics,
}

impl ObserveMetricDescriptor {
    pub fn new(
        name: impl Into<String>,
        kind: ObserveMetricKind,
        unit: Option<ObserveMetricUnit>,
        help: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            unit,
            help: help.into(),
            semantics: ObserveMetricSemantics::default(),
        }
    }

    pub fn with_semantics(mut self, semantics: ObserveMetricSemantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// Returns the declared aggregation when it is structurally safe for this
    /// metric kind, otherwise the fail-closed `None` aggregation.
    pub fn effective_aggregation(&self) -> ObserveMetricAggregation {
        if self.semantics.aggregation.is_compatible_with(self.kind) {
            self.semantics.aggregation
        } else {
            ObserveMetricAggregation::None
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<ObserveHistogramBucket>,
}

/// One cumulative histogram bucket. `count` includes every recorded value less
/// than or equal to `upper_bound`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObserveHistogramBucket {
    pub upper_bound: f64,
    pub count: u64,
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
            buckets: Vec::new(),
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
            buckets: Vec::new(),
        }
    }

    pub fn histogram_quantile(&self, quantile: f64) -> Option<f64> {
        if self.kind != ObserveMetricKind::Histogram || !(0.0..=1.0).contains(&quantile) {
            return None;
        }
        let count = self.count?;
        if count == 0 {
            return None;
        }
        let rank = ((count as f64) * quantile).ceil().max(1.0) as u64;
        self.buckets
            .iter()
            .find(|bucket| bucket.count >= rank)
            .map(|bucket| bucket.upper_bound)
            .or(self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exporter_contract_is_data_only_and_round_trip_safe() {
        let response = ObserveMetricExportResponse {
            descriptors: vec![ObserveMetricDescriptor::new(
                "fixture.jobs.completed",
                ObserveMetricKind::Counter,
                Some(ObserveMetricUnit::Count),
                "Completed fixture jobs.",
            )],
            metrics: vec![ObserveMetricRecord::counter(
                "fixture.jobs.completed",
                BTreeMap::new(),
                7,
            )],
        };
        assert_eq!(response.descriptors[0].name, response.metrics[0].name);
        assert_eq!(OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION, "1.0.0");
    }
}
