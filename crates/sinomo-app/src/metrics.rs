//! Bounded live metric catalog and query model for operator-facing UI.
//!
//! This deliberately retains only the series selected in the metric explorer.
//! Prometheus remains the durable history path; the app keeps enough state to
//! inspect a short, reset-aware live window without becoming a time-series DB.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use sinomo_runtime::observe::{
    ObserveHistogramBucket, ObserveMetricDescriptor, ObserveMetricKind, ObserveMetricRecord,
    ObserveMetricUnit, ObserveMetricsResponse,
};
use time::OffsetDateTime;

pub const DEFAULT_METRIC_HISTORY_POINTS: usize = 180;
pub const DEFAULT_METRIC_HISTORY_SERIES: usize = 96;
pub const DEFAULT_METRIC_CURRENT_SERIES: usize = 4_096;
pub const DEFAULT_METRIC_CATALOG_FAMILIES: usize = 4_096;
const DEFAULT_METRIC_HISTORY_BUCKETS: usize = 256;
const DEFAULT_LABEL_KEYS: usize = 64;
const DEFAULT_LABEL_VALUES: usize = 64;

pub(crate) fn metric_process_identity_matches(
    previous: Option<OffsetDateTime>,
    current: Option<OffsetDateTime>,
) -> bool {
    match (previous, current) {
        (Some(previous), Some(current)) => previous == current,
        (None, None) => true,
        _ => false,
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MetricSeriesKey {
    pub metric_name: String,
    pub node_id: String,
    pub labels: BTreeMap<String, String>,
}

impl MetricSeriesKey {
    pub fn display_name(&self) -> String {
        if self.labels.is_empty() {
            return self.node_id.clone();
        }
        let labels = self
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} · {labels}", self.node_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricCatalogEntry {
    pub descriptor: ObserveMetricDescriptor,
    pub descriptor_conflict: bool,
    pub label_keys: BTreeSet<String>,
    pub label_values: BTreeMap<String, BTreeSet<String>>,
    pub label_values_truncated: BTreeSet<String>,
    pub label_keys_truncated: bool,
    pub node_ids: BTreeSet<String>,
    pub current_series: usize,
    pub value_conflict: bool,
    descriptor_registered: bool,
}

impl MetricCatalogEntry {
    fn new(mut descriptor: ObserveMetricDescriptor, descriptor_registered: bool) -> Self {
        let effective_aggregation = descriptor.effective_aggregation();
        let descriptor_conflict = descriptor.semantics.aggregation != effective_aggregation;
        descriptor.semantics.aggregation = effective_aggregation;
        let label_keys_truncated = descriptor.semantics.labels.len() > DEFAULT_LABEL_KEYS;
        descriptor.semantics.labels.truncate(DEFAULT_LABEL_KEYS);
        let label_keys = descriptor.semantics.labels.iter().cloned().collect();
        Self {
            descriptor,
            descriptor_conflict,
            label_keys,
            label_values: BTreeMap::new(),
            label_values_truncated: BTreeSet::new(),
            label_keys_truncated,
            node_ids: BTreeSet::new(),
            current_series: 0,
            value_conflict: false,
            descriptor_registered,
        }
    }

    pub fn domain(&self) -> String {
        self.descriptor
            .semantics
            .domain
            .clone()
            .or_else(|| self.descriptor.name.split('.').nth(1).map(str::to_string))
            .unwrap_or_else(|| "other".to_string())
    }

    fn begin_batch(&mut self) {
        self.node_ids.clear();
        self.current_series = 0;
        self.label_values.clear();
        self.label_values_truncated.clear();
    }

    fn merge_descriptor(&mut self, incoming: &ObserveMetricDescriptor) {
        let mut incoming = incoming.clone();
        let effective_aggregation = incoming.effective_aggregation();
        let incoming_aggregation_conflict = incoming.semantics.aggregation != effective_aggregation;
        if incoming_aggregation_conflict {
            incoming.semantics.aggregation = effective_aggregation;
        }
        if !self.descriptor_registered {
            let observed_kind = self.descriptor.kind;
            let previous_descriptor_conflict = self.descriptor_conflict;
            let previous_value_conflict = self.value_conflict;
            let observed_label_keys = std::mem::take(&mut self.label_keys);
            let label_values = std::mem::take(&mut self.label_values);
            let label_values_truncated = std::mem::take(&mut self.label_values_truncated);
            let label_keys_truncated = self.label_keys_truncated;
            let node_ids = std::mem::take(&mut self.node_ids);
            let current_series = self.current_series;
            *self = Self::new(incoming, true);
            self.label_keys.extend(observed_label_keys);
            while self.label_keys.len() > DEFAULT_LABEL_KEYS {
                self.label_keys.pop_last();
                self.label_keys_truncated = true;
            }
            self.label_values = label_values;
            self.label_values_truncated = label_values_truncated;
            self.label_keys_truncated |= label_keys_truncated;
            self.node_ids = node_ids;
            self.current_series = current_series;
            self.descriptor_conflict |= previous_descriptor_conflict
                || incoming_aggregation_conflict
                || observed_kind != self.descriptor.kind;
            self.value_conflict |= previous_value_conflict || observed_kind != self.descriptor.kind;
            return;
        }
        self.descriptor_conflict |= incoming_aggregation_conflict;
        if self.descriptor.kind != incoming.kind {
            self.descriptor_conflict = true;
            self.value_conflict = true;
            return;
        }
        match (self.descriptor.unit, incoming.unit) {
            (None, Some(unit)) => self.descriptor.unit = Some(unit),
            (Some(existing), Some(incoming)) if existing != incoming => {
                self.descriptor_conflict = true;
                self.value_conflict = true;
            }
            _ => {}
        }
        if self.descriptor.help.is_empty() && !incoming.help.is_empty() {
            self.descriptor.help = incoming.help.clone();
        }
        let domain_conflict = self.descriptor.semantics.domain.is_some()
            && incoming.semantics.domain.is_some()
            && self.descriptor.semantics.domain != incoming.semantics.domain;
        let aggregation_conflict = !self.descriptor.semantics.is_empty()
            && !incoming.semantics.is_empty()
            && self.descriptor.semantics.aggregation != incoming.semantics.aggregation;
        self.descriptor.semantics.merge_rolling(&incoming.semantics);
        if self.descriptor.semantics.labels.len() > DEFAULT_LABEL_KEYS {
            self.descriptor
                .semantics
                .labels
                .truncate(DEFAULT_LABEL_KEYS);
            self.label_keys_truncated = true;
        }
        self.descriptor_conflict |= domain_conflict || aggregation_conflict;
        self.label_keys
            .extend(incoming.semantics.labels.iter().cloned());
        while self.label_keys.len() > DEFAULT_LABEL_KEYS {
            self.label_keys.pop_last();
            self.label_keys_truncated = true;
        }
    }

    fn observe_series(&mut self, node_id: &str, record: &ObserveMetricRecord) {
        self.node_ids.insert(node_id.to_string());
        if self.descriptor.kind != record.kind {
            self.descriptor_conflict = true;
            self.value_conflict = true;
        }
        for (key, value) in &record.labels {
            if !self.label_keys.contains(key) && self.label_keys.len() >= DEFAULT_LABEL_KEYS {
                self.label_keys_truncated = true;
                continue;
            }
            self.label_keys.insert(key.clone());
            let values = self.label_values.entry(key.clone()).or_default();
            if values.len() < DEFAULT_LABEL_VALUES || values.contains(value) {
                values.insert(value.clone());
            } else {
                self.label_values_truncated.insert(key.clone());
            }
        }
        self.current_series = self.current_series.saturating_add(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricTransform {
    Current,
    Rate,
    Delta,
    HistogramQuantile(u8),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricSelection {
    pub metric_name: String,
    pub node_id: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub transform: MetricTransform,
}

impl MetricSelection {
    pub fn matches(&self, key: &MetricSeriesKey) -> bool {
        key.metric_name == self.metric_name
            && self
                .node_id
                .as_ref()
                .is_none_or(|node_id| &key.node_id == node_id)
            && self
                .labels
                .iter()
                .all(|(label, value)| key.labels.get(label) == Some(value))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricPoint {
    pub at: OffsetDateTime,
    pub value: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricSeries {
    pub key: MetricSeriesKey,
    pub points: Vec<MetricPoint>,
}

impl MetricSeries {
    pub fn finite_values(&self) -> impl Iterator<Item = f64> + '_ {
        self.points
            .iter()
            .filter_map(|point| point.value)
            .filter(|value| value.is_finite())
    }

    pub fn latest(&self) -> Option<f64> {
        self.points
            .last()
            .and_then(|point| point.value)
            .filter(|value| value.is_finite())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricQueryResult {
    pub kind: ObserveMetricKind,
    pub unit: Option<ObserveMetricUnit>,
    pub series: Vec<MetricSeries>,
    pub matching_series: usize,
    pub history_truncated: bool,
    pub value_conflict: bool,
}

impl MetricQueryResult {
    pub fn oldest_at(&self) -> Option<OffsetDateTime> {
        self.series
            .iter()
            .flat_map(|series| series.points.iter().map(|point| point.at))
            .min()
    }

    pub fn newest_at(&self) -> Option<OffsetDateTime> {
        self.series
            .iter()
            .flat_map(|series| series.points.iter().map(|point| point.at))
            .max()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetricHistoryScope {
    metric_name: String,
    node_id: Option<String>,
    labels: BTreeMap<String, String>,
}

impl MetricHistoryScope {
    fn matches(&self, key: &MetricSeriesKey) -> bool {
        key.metric_name == self.metric_name
            && self
                .node_id
                .as_ref()
                .is_none_or(|node_id| &key.node_id == node_id)
            && self
                .labels
                .iter()
                .all(|(label, value)| key.labels.get(label) == Some(value))
    }
}

#[derive(Clone, Debug)]
struct LatestMetric {
    observed_at: OffsetDateTime,
    collected_at: OffsetDateTime,
    process_started_at: Option<OffsetDateTime>,
    raw: ObserveMetricRecord,
    interval: Option<ObserveMetricRecord>,
    elapsed_seconds: Option<f64>,
    sample_valid: bool,
}

#[derive(Clone, Debug)]
struct StoredMetricValue {
    kind: ObserveMetricKind,
    value: Option<f64>,
    count: Option<u64>,
    max: Option<f64>,
    buckets: Vec<ObserveHistogramBucket>,
}

impl StoredMetricValue {
    fn from_record(record: &ObserveMetricRecord) -> Option<Self> {
        if record.kind == ObserveMetricKind::Histogram
            && record.buckets.len() > DEFAULT_METRIC_HISTORY_BUCKETS
        {
            return None;
        }
        Some(Self {
            kind: record.kind,
            value: record.value,
            count: record.count,
            max: record.max,
            buckets: if record.kind == ObserveMetricKind::Histogram {
                record.buckets.clone()
            } else {
                Vec::new()
            },
        })
    }

    fn histogram_quantile(&self, quantile: f64) -> Option<f64> {
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

#[derive(Clone, Debug)]
struct StoredMetricSample {
    at: OffsetDateTime,
    raw: Option<StoredMetricValue>,
    interval: Option<StoredMetricValue>,
    elapsed_seconds: Option<f64>,
}

impl StoredMetricSample {
    fn from_latest(latest: &LatestMetric) -> Self {
        if !latest.sample_valid {
            return Self::gap(latest.collected_at);
        }
        Self {
            at: latest.collected_at,
            raw: StoredMetricValue::from_record(&latest.raw),
            interval: latest
                .interval
                .as_ref()
                .and_then(StoredMetricValue::from_record),
            elapsed_seconds: latest.elapsed_seconds,
        }
    }

    fn gap(at: OffsetDateTime) -> Self {
        Self {
            at,
            raw: None,
            interval: None,
            elapsed_seconds: None,
        }
    }

    fn value(&self, transform: MetricTransform) -> Option<f64> {
        match transform {
            MetricTransform::Current => self.raw.as_ref()?.value,
            MetricTransform::Delta => {
                let interval = self.interval.as_ref()?;
                (interval.kind == ObserveMetricKind::Counter)
                    .then_some(interval.value)
                    .flatten()
            }
            MetricTransform::Rate => {
                let elapsed = self
                    .elapsed_seconds
                    .filter(|elapsed| *elapsed > f64::EPSILON)?;
                let interval = self.interval.as_ref()?;
                (interval.kind == ObserveMetricKind::Counter)
                    .then_some(interval.value)
                    .flatten()
                    .map(|value| value / elapsed)
            }
            MetricTransform::HistogramQuantile(percentile) => {
                let interval = self.interval.as_ref()?;
                (interval.kind == ObserveMetricKind::Histogram)
                    .then(|| interval.histogram_quantile(f64::from(percentile.min(100)) / 100.0))
                    .flatten()
            }
        }
        .filter(|value| value.is_finite())
    }
}

#[derive(Clone, Debug)]
pub struct MetricStore {
    catalog: BTreeMap<String, MetricCatalogEntry>,
    latest: BTreeMap<MetricSeriesKey, LatestMetric>,
    history: BTreeMap<MetricSeriesKey, VecDeque<StoredMetricSample>>,
    history_scope: Option<MetricHistoryScope>,
    history_points_limit: usize,
    history_series_limit: usize,
    current_series_limit: usize,
    catalog_family_limit: usize,
    catalog_truncated: bool,
}

impl Default for MetricStore {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_METRIC_HISTORY_POINTS,
            DEFAULT_METRIC_HISTORY_SERIES,
            DEFAULT_METRIC_CURRENT_SERIES,
        )
    }
}

impl MetricStore {
    pub fn with_limits(
        history_points_limit: usize,
        history_series_limit: usize,
        current_series_limit: usize,
    ) -> Self {
        Self {
            catalog: BTreeMap::new(),
            latest: BTreeMap::new(),
            history: BTreeMap::new(),
            history_scope: None,
            history_points_limit: history_points_limit.max(1),
            history_series_limit: history_series_limit.max(1),
            current_series_limit: current_series_limit.max(1),
            catalog_family_limit: DEFAULT_METRIC_CATALOG_FAMILIES,
            catalog_truncated: false,
        }
    }

    pub fn catalog(&self) -> impl Iterator<Item = &MetricCatalogEntry> {
        self.catalog.values()
    }

    pub fn catalog_entry(&self, name: &str) -> Option<&MetricCatalogEntry> {
        self.catalog.get(name)
    }

    pub fn catalog_truncated(&self) -> bool {
        self.catalog_truncated
    }

    pub fn search(&self, query: &str) -> Vec<&MetricCatalogEntry> {
        let query = query.trim().to_ascii_lowercase();
        self.catalog
            .values()
            .filter(|entry| {
                query.is_empty()
                    || entry.descriptor.name.to_ascii_lowercase().contains(&query)
                    || entry.descriptor.help.to_ascii_lowercase().contains(&query)
                    || entry.domain().to_ascii_lowercase().contains(&query)
            })
            .collect()
    }

    pub fn set_history_scope(
        &mut self,
        metric_name: Option<String>,
        node_id: Option<String>,
        labels: BTreeMap<String, String>,
    ) {
        let scope = metric_name.map(|metric_name| MetricHistoryScope {
            metric_name,
            node_id,
            labels,
        });
        if self.history_scope == scope {
            return;
        }
        self.history_scope = scope;
        self.history.clear();
        let Some(scope) = self.history_scope.as_ref() else {
            return;
        };
        for (key, latest) in self
            .latest
            .iter()
            .filter(|(key, _)| scope.matches(key))
            .take(self.history_series_limit)
        {
            self.history.insert(
                key.clone(),
                VecDeque::from([StoredMetricSample::from_latest(latest)]),
            );
        }
    }

    fn reserve_catalog_family(&mut self, name: &str) -> bool {
        if self.catalog.contains_key(name) || self.catalog.len() < self.catalog_family_limit {
            return true;
        }
        self.catalog_truncated = true;
        let selected = self
            .history_scope
            .as_ref()
            .is_some_and(|scope| scope.metric_name == name);
        if !selected {
            return false;
        }
        let eviction = self
            .catalog
            .keys()
            .rev()
            .find(|candidate| candidate.as_str() != name)
            .cloned();
        if let Some(eviction) = eviction {
            self.catalog.remove(&eviction);
            return true;
        }
        false
    }

    pub fn ingest_batch<'a>(
        &mut self,
        received_at: OffsetDateTime,
        responses: impl IntoIterator<Item = (&'a str, &'a ObserveMetricsResponse)>,
    ) {
        for entry in self.catalog.values_mut() {
            entry.begin_batch();
        }
        self.catalog_truncated = false;
        let mut candidates = BTreeMap::<MetricSeriesKey, LatestMetric>::new();
        let history_scope = self.history_scope.clone();

        for (source_node_id, response) in responses {
            let node_id = source_node_id;
            for descriptor in &response.descriptors {
                if !self.reserve_catalog_family(&descriptor.name) {
                    continue;
                }
                self.catalog
                    .entry(descriptor.name.clone())
                    .and_modify(|entry| entry.merge_descriptor(descriptor))
                    .or_insert_with(|| MetricCatalogEntry::new(descriptor.clone(), true));
            }
            let at = response.observed_at.unwrap_or(received_at);
            for record in &response.metrics {
                if !self.reserve_catalog_family(&record.name) {
                    continue;
                }
                let fallback = ObserveMetricDescriptor::new(
                    record.name.clone(),
                    record.kind,
                    None,
                    "No registered descriptor was returned for this metric.",
                );
                let entry = self
                    .catalog
                    .entry(record.name.clone())
                    .or_insert_with(|| MetricCatalogEntry::new(fallback, false));
                entry.observe_series(node_id, record);
                let value_conflict = entry.value_conflict;
                let key = MetricSeriesKey {
                    metric_name: record.name.clone(),
                    node_id: node_id.to_string(),
                    labels: record.labels.clone(),
                };
                if value_conflict {
                    candidates.retain(|candidate, _| candidate.metric_name != record.name);
                    continue;
                }

                let previous = self.latest.get(&key);
                let same_process = previous.is_some_and(|previous| {
                    metric_process_identity_matches(
                        previous.process_started_at,
                        response.process_started_at,
                    )
                });
                if let Some(previous) = previous
                    && same_process
                    && at <= previous.observed_at
                {
                    let mut retained = previous.clone();
                    retained.collected_at = received_at;
                    retained.sample_valid = false;
                    if candidates.contains_key(&key) || candidates.len() < self.current_series_limit
                    {
                        candidates.insert(key, retained);
                    } else if history_scope
                        .as_ref()
                        .is_some_and(|scope| scope.matches(&key))
                        && let Some(eviction) = candidates
                            .keys()
                            .rev()
                            .find(|candidate| {
                                history_scope
                                    .as_ref()
                                    .is_none_or(|scope| !scope.matches(candidate))
                            })
                            .cloned()
                    {
                        candidates.remove(&eviction);
                        candidates.insert(key, retained);
                    }
                    continue;
                }
                let elapsed_seconds = previous
                    .map(|previous| (at - previous.observed_at).as_seconds_f64())
                    .filter(|elapsed| same_process && *elapsed > f64::EPSILON);
                let interval = previous
                    .filter(|_| elapsed_seconds.is_some())
                    .and_then(|previous| interval_delta(&previous.raw, record));
                let latest = LatestMetric {
                    observed_at: at,
                    collected_at: received_at,
                    process_started_at: response.process_started_at,
                    raw: record.clone(),
                    interval,
                    elapsed_seconds,
                    sample_valid: true,
                };
                if candidates.contains_key(&key) || candidates.len() < self.current_series_limit {
                    candidates.insert(key, latest);
                    continue;
                }
                let in_scope = history_scope
                    .as_ref()
                    .is_some_and(|scope| scope.matches(&key));
                if in_scope {
                    let eviction = candidates
                        .keys()
                        .rev()
                        .find(|candidate| {
                            history_scope
                                .as_ref()
                                .is_none_or(|scope| !scope.matches(candidate))
                        })
                        .cloned();
                    if let Some(eviction) = eviction {
                        candidates.remove(&eviction);
                        candidates.insert(key, latest);
                    }
                }
            }
        }

        candidates.retain(|key, _| {
            !self
                .catalog
                .get(&key.metric_name)
                .is_some_and(|entry| entry.value_conflict)
        });
        self.append_history(received_at, &candidates);
        self.latest = candidates;
    }

    fn append_history(
        &mut self,
        received_at: OffsetDateTime,
        candidates: &BTreeMap<MetricSeriesKey, LatestMetric>,
    ) {
        let Some(scope) = self.history_scope.as_ref() else {
            return;
        };
        for (key, samples) in &mut self.history {
            if !candidates.contains_key(key) {
                samples.push_back(StoredMetricSample::gap(received_at));
                trim_history(samples, self.history_points_limit);
            }
        }

        for (key, latest) in candidates.iter().filter(|(key, _)| scope.matches(key)) {
            if !self.history.contains_key(key) && self.history.len() >= self.history_series_limit {
                let stale = self
                    .history
                    .keys()
                    .rev()
                    .find(|retained| !candidates.contains_key(*retained))
                    .cloned();
                let Some(stale) = stale else {
                    continue;
                };
                self.history.remove(&stale);
            }
            let samples = self.history.entry(key.clone()).or_default();
            samples.push_back(StoredMetricSample::from_latest(latest));
            trim_history(samples, self.history_points_limit);
        }
    }

    pub fn query(&self, selection: &MetricSelection, sample_limit: usize) -> MetricQueryResult {
        self.query_window(selection, sample_limit, None)
    }

    pub fn query_window(
        &self,
        selection: &MetricSelection,
        sample_limit: usize,
        since: Option<OffsetDateTime>,
    ) -> MetricQueryResult {
        let entry = self.catalog.get(&selection.metric_name);
        let kind = entry
            .map(|entry| entry.descriptor.kind)
            .unwrap_or(ObserveMetricKind::Gauge);
        let unit = entry.and_then(|entry| entry.descriptor.unit);
        let value_conflict = entry.is_some_and(|entry| entry.value_conflict);
        let matching_series = self
            .latest
            .keys()
            .filter(|key| selection.matches(key))
            .count();
        let limit = sample_limit.max(1);
        let series = if value_conflict {
            Vec::new()
        } else {
            self.history
                .iter()
                .filter(|(key, _)| selection.matches(key))
                .map(|(key, samples)| MetricSeries {
                    key: key.clone(),
                    points: samples
                        .iter()
                        .rev()
                        .filter(|sample| since.is_none_or(|since| sample.at >= since))
                        .take(limit)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .map(|sample| MetricPoint {
                            at: sample.at,
                            value: sample.value(selection.transform),
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        };
        MetricQueryResult {
            kind,
            unit,
            history_truncated: matching_series > series.len(),
            series,
            matching_series,
            value_conflict,
        }
    }
}

fn trim_history(samples: &mut VecDeque<StoredMetricSample>, limit: usize) {
    while samples.len() > limit {
        samples.pop_front();
    }
}

pub(crate) fn strict_metrics_interval_delta(
    previous: &ObserveMetricsResponse,
    previous_received_at: OffsetDateTime,
    current: &ObserveMetricsResponse,
    current_received_at: OffsetDateTime,
) -> Option<(ObserveMetricsResponse, f64)> {
    let same_process =
        metric_process_identity_matches(previous.process_started_at, current.process_started_at);
    if !same_process {
        return None;
    }
    let previous_at = previous.observed_at.unwrap_or(previous_received_at);
    let current_at = current.observed_at.unwrap_or(current_received_at);
    let elapsed_seconds = (current_at - previous_at).as_seconds_f64();
    if elapsed_seconds <= f64::EPSILON {
        return None;
    }
    let mut delta = current.clone();
    delta.metrics = current
        .metrics
        .iter()
        .filter_map(|current| {
            previous
                .metrics
                .iter()
                .find(|previous| {
                    previous.name == current.name
                        && previous.kind == current.kind
                        && previous.labels == current.labels
                })
                .and_then(|previous| interval_delta(previous, current))
        })
        .collect();
    Some((delta, elapsed_seconds))
}

fn interval_delta(
    previous: &ObserveMetricRecord,
    current: &ObserveMetricRecord,
) -> Option<ObserveMetricRecord> {
    if previous.name != current.name
        || previous.kind != current.kind
        || previous.labels != current.labels
    {
        return None;
    }
    let mut delta = current.clone();
    match current.kind {
        ObserveMetricKind::Gauge => return Some(delta),
        ObserveMetricKind::Counter => {
            let previous = previous.value?;
            let current = current.value?;
            if !previous.is_finite() || !current.is_finite() || current < previous {
                return None;
            }
            delta.value = Some(current - previous);
        }
        ObserveMetricKind::Histogram => {
            let previous_count = previous.count?;
            let current_count = current.count?;
            if current_count < previous_count
                || previous.buckets.len() != current.buckets.len()
                || !valid_histogram_buckets(&previous.buckets, previous_count)
                || !valid_histogram_buckets(&current.buckets, current_count)
                || previous
                    .buckets
                    .iter()
                    .zip(&current.buckets)
                    .any(|(previous, current)| {
                        previous.upper_bound.to_bits() != current.upper_bound.to_bits()
                    })
            {
                return None;
            }
            delta.count = Some(current_count - previous_count);
            delta.sum = match (previous.sum, current.sum) {
                (Some(previous), Some(current)) if previous.is_finite() && current.is_finite() => {
                    Some(current - previous)
                }
                (None, None) => None,
                _ => None,
            };
            delta.min = None;
            delta.max = None;
            let mut buckets = Vec::with_capacity(current.buckets.len());
            for (previous, bucket) in previous.buckets.iter().zip(&current.buckets) {
                if bucket.count < previous.count {
                    return None;
                }
                buckets.push(ObserveHistogramBucket {
                    upper_bound: bucket.upper_bound,
                    count: bucket.count - previous.count,
                });
            }
            delta.buckets = buckets;
            if !valid_histogram_buckets(&delta.buckets, delta.count?) {
                return None;
            }
        }
    }
    Some(delta)
}

fn valid_histogram_buckets(buckets: &[ObserveHistogramBucket], count: u64) -> bool {
    let mut previous_bound = f64::NEG_INFINITY;
    let mut previous_count = 0;
    for bucket in buckets {
        if bucket.upper_bound.is_nan()
            || bucket.upper_bound <= previous_bound
            || bucket.count < previous_count
            || bucket.count > count
        {
            return false;
        }
        previous_bound = bucket.upper_bound;
        previous_count = bucket.count;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::observe::{
        ObserveMetricAggregation, ObserveMetricSemantics, ObserveMetricsResponse,
    };

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds)
    }

    fn response(
        at: OffsetDateTime,
        process_started_at: OffsetDateTime,
        descriptors: Vec<ObserveMetricDescriptor>,
        metrics: Vec<ObserveMetricRecord>,
    ) -> ObserveMetricsResponse {
        ObserveMetricsResponse {
            observed_at: Some(at),
            process_started_at: Some(process_started_at),
            process_role: Some("daemon".to_string()),
            node_id: None,
            exporters: Vec::new(),
            partial: false,
            descriptors,
            metrics,
        }
    }

    fn labels(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn catalog_searches_descriptors_and_observed_dimensions() {
        let descriptor = ObserveMetricDescriptor::new(
            "sinomo.test.load",
            ObserveMetricKind::Gauge,
            Some(ObserveMetricUnit::Ratio),
            "Worker saturation ratio.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "scheduler",
            ObserveMetricAggregation::Max,
            ["worker"],
        ));
        let snapshot = response(
            at(1),
            at(0),
            vec![descriptor],
            vec![ObserveMetricRecord::gauge(
                "sinomo.test.load",
                labels(&[("worker", "alpha")]),
                0.75,
            )],
        );
        let mut store = MetricStore::default();
        store.ingest_batch(at(1), [("node-a", &snapshot)]);

        let entry = store.catalog_entry("sinomo.test.load").unwrap();
        assert_eq!(entry.domain(), "scheduler");
        assert_eq!(entry.current_series, 1);
        assert_eq!(entry.node_ids, BTreeSet::from(["node-a".to_string()]));
        assert_eq!(
            entry.label_values["worker"],
            BTreeSet::from(["alpha".to_string()])
        );
        assert_eq!(store.search("saturation").len(), 1);
        assert_eq!(store.search("scheduler").len(), 1);
    }

    #[test]
    fn counter_rate_uses_a_baseline_and_gaps_on_restart_or_reset() {
        let mut store = MetricStore::default();
        store.set_history_scope(
            Some("sinomo.test.requests".to_string()),
            None,
            BTreeMap::new(),
        );
        let selection = MetricSelection {
            metric_name: "sinomo.test.requests".to_string(),
            node_id: None,
            labels: BTreeMap::new(),
            transform: MetricTransform::Rate,
        };
        for (sample_at, process_at, value) in [(10, 1, 100), (15, 1, 130), (20, 19, 9)] {
            let snapshot = response(
                at(sample_at),
                at(process_at),
                Vec::new(),
                vec![ObserveMetricRecord::counter(
                    "sinomo.test.requests",
                    BTreeMap::new(),
                    value,
                )],
            );
            store.ingest_batch(at(sample_at), [("node-a", &snapshot)]);
        }
        let values = store.query(&selection, 10).series[0]
            .points
            .iter()
            .map(|point| point.value)
            .collect::<Vec<_>>();
        assert_eq!(values, vec![None, Some(6.0), None]);

        let reset = response(
            at(25),
            at(19),
            Vec::new(),
            vec![ObserveMetricRecord::counter(
                "sinomo.test.requests",
                BTreeMap::new(),
                2,
            )],
        );
        store.ingest_batch(at(25), [("node-a", &reset)]);
        assert_eq!(
            store.query(&selection, 10).series[0]
                .points
                .last()
                .unwrap()
                .value,
            None
        );
    }

    #[test]
    fn histogram_quantile_uses_interval_buckets() {
        let histogram = |count: u64, first: u64, second: u64| ObserveMetricRecord {
            name: "sinomo.test.latency.us".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::new(),
            value: None,
            count: Some(count),
            sum: Some((first * 100 + second * 1_000) as f64),
            min: Some(100.0),
            max: Some(1_000.0),
            buckets: vec![
                ObserveHistogramBucket {
                    upper_bound: 100.0,
                    count: first,
                },
                ObserveHistogramBucket {
                    upper_bound: 1_000.0,
                    count,
                },
            ],
        };
        let mut store = MetricStore::default();
        store.set_history_scope(
            Some("sinomo.test.latency.us".to_string()),
            None,
            BTreeMap::new(),
        );
        let first = response(at(1), at(0), Vec::new(), vec![histogram(10, 8, 2)]);
        let second = response(at(6), at(0), Vec::new(), vec![histogram(20, 9, 11)]);
        store.ingest_batch(at(1), [("node-a", &first)]);
        store.ingest_batch(at(6), [("node-a", &second)]);
        let result = store.query(
            &MetricSelection {
                metric_name: "sinomo.test.latency.us".to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::HistogramQuantile(50),
            },
            10,
        );
        assert_eq!(result.series[0].points[0].value, None);
        assert_eq!(result.series[0].points[1].value, Some(1_000.0));
    }

    #[test]
    fn history_scope_uses_full_series_identity_and_stays_bounded() {
        let mut store = MetricStore::with_limits(2, 1, 2);
        store.set_history_scope(
            Some("sinomo.test.queue".to_string()),
            Some("node-b".to_string()),
            labels(&[("queue", "jobs")]),
        );
        for second in 1..=3 {
            let a = response(
                at(second),
                at(0),
                Vec::new(),
                vec![ObserveMetricRecord::gauge(
                    "sinomo.test.queue",
                    labels(&[("queue", "jobs")]),
                    second as f64,
                )],
            );
            let b = response(
                at(second),
                at(0),
                Vec::new(),
                vec![ObserveMetricRecord::gauge(
                    "sinomo.test.queue",
                    labels(&[("queue", "jobs")]),
                    (second * 10) as f64,
                )],
            );
            store.ingest_batch(at(second), [("node-a", &a), ("node-b", &b)]);
        }
        let result = store.query(
            &MetricSelection {
                metric_name: "sinomo.test.queue".to_string(),
                node_id: Some("node-b".to_string()),
                labels: labels(&[("queue", "jobs")]),
                transform: MetricTransform::Current,
            },
            10,
        );
        assert_eq!(result.series.len(), 1);
        assert_eq!(
            result.series[0]
                .points
                .iter()
                .map(|point| point.value)
                .collect::<Vec<_>>(),
            vec![Some(20.0), Some(30.0)]
        );
    }

    #[test]
    fn fallback_descriptors_upgrade_and_semantic_conflicts_fail_closed() {
        let name = "sinomo.test.requests";
        let mut store = MetricStore::default();
        let legacy = response(
            at(1),
            at(0),
            Vec::new(),
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 1)],
        );
        store.ingest_batch(at(1), [("node-a", &legacy)]);
        assert_eq!(store.catalog_entry(name).unwrap().descriptor.unit, None);

        let descriptor = ObserveMetricDescriptor::new(
            name,
            ObserveMetricKind::Counter,
            Some(ObserveMetricUnit::Count),
            "Authoritative request count.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "daemon",
            ObserveMetricAggregation::Sum,
            ["route"],
        ));
        let registered = response(
            at(2),
            at(0),
            vec![descriptor.clone()],
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 2)],
        );
        store.ingest_batch(at(2), [("node-a", &registered)]);
        let entry = store.catalog_entry(name).unwrap();
        assert_eq!(entry.descriptor.unit, Some(ObserveMetricUnit::Count));
        assert_eq!(entry.descriptor.help, "Authoritative request count.");
        assert!(!entry.descriptor_conflict);

        let conflicting = descriptor.with_semantics(ObserveMetricSemantics::new(
            "other",
            ObserveMetricAggregation::Max,
            ["route"],
        ));
        let conflict = response(
            at(3),
            at(0),
            vec![conflicting],
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 3)],
        );
        store.ingest_batch(at(3), [("node-b", &conflict)]);
        let entry = store.catalog_entry(name).unwrap();
        assert!(entry.descriptor_conflict);
        assert_eq!(entry.descriptor.semantics.domain, None);
        assert_eq!(
            entry.descriptor.semantics.aggregation,
            ObserveMetricAggregation::None
        );
        assert!(!entry.value_conflict, "raw series remain comparable");
    }

    #[test]
    fn fallback_kind_upgrade_preserves_observed_conflict_and_disables_values() {
        let name = "sinomo.test.load";
        let mut store = MetricStore::default();
        store.set_history_scope(Some(name.to_string()), None, BTreeMap::new());
        let legacy = response(
            at(1),
            at(0),
            Vec::new(),
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 10)],
        );
        store.ingest_batch(at(1), [("node-a", &legacy)]);

        let registered = response(
            at(2),
            at(0),
            vec![ObserveMetricDescriptor::new(
                name,
                ObserveMetricKind::Gauge,
                Some(ObserveMetricUnit::Ratio),
                "Authoritative load ratio.",
            )],
            vec![ObserveMetricRecord::gauge(name, BTreeMap::new(), 0.5)],
        );
        store.ingest_batch(at(2), [("node-a", &registered)]);

        let entry = store.catalog_entry(name).unwrap();
        assert_eq!(entry.descriptor.kind, ObserveMetricKind::Gauge);
        assert!(entry.descriptor_conflict);
        assert!(entry.value_conflict);
        let result = store.query(
            &MetricSelection {
                metric_name: name.to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::Current,
            },
            10,
        );
        assert!(result.value_conflict);
        assert!(result.series.is_empty());
    }

    #[test]
    fn kind_or_unit_conflicts_disable_values_instead_of_mixing_series() {
        let name = "sinomo.test.bytes";
        let count = ObserveMetricDescriptor::new(
            name,
            ObserveMetricKind::Counter,
            Some(ObserveMetricUnit::Count),
            "Count.",
        );
        let bytes = ObserveMetricDescriptor::new(
            name,
            ObserveMetricKind::Counter,
            Some(ObserveMetricUnit::Bytes),
            "Bytes.",
        );
        let first = response(
            at(1),
            at(0),
            vec![count],
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 10)],
        );
        let second = response(
            at(1),
            at(0),
            vec![bytes],
            vec![ObserveMetricRecord::counter(name, BTreeMap::new(), 20)],
        );
        let mut store = MetricStore::default();
        store.set_history_scope(Some(name.to_string()), None, BTreeMap::new());
        store.ingest_batch(at(1), [("node-a", &first), ("node-b", &second)]);

        let result = store.query(
            &MetricSelection {
                metric_name: name.to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::Rate,
            },
            10,
        );
        assert!(store.catalog_entry(name).unwrap().value_conflict);
        assert!(result.value_conflict);
        assert!(result.series.is_empty());
    }

    #[test]
    fn mixed_process_metadata_and_changed_histogram_schema_create_gaps() {
        let counter_name = "sinomo.test.requests";
        let mut store = MetricStore::default();
        store.set_history_scope(Some(counter_name.to_string()), None, BTreeMap::new());
        let known = response(
            at(10),
            at(1),
            Vec::new(),
            vec![ObserveMetricRecord::counter(
                counter_name,
                BTreeMap::new(),
                100,
            )],
        );
        let mut unknown = response(
            at(15),
            at(1),
            Vec::new(),
            vec![ObserveMetricRecord::counter(
                counter_name,
                BTreeMap::new(),
                130,
            )],
        );
        unknown.process_started_at = None;
        store.ingest_batch(at(10), [("node-a", &known)]);
        store.ingest_batch(at(15), [("node-a", &unknown)]);
        let result = store.query(
            &MetricSelection {
                metric_name: counter_name.to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::Rate,
            },
            10,
        );
        assert_eq!(result.series[0].points.last().unwrap().value, None);
        assert_eq!(result.series[0].latest(), None);

        let histogram = |buckets| ObserveMetricRecord {
            name: "sinomo.test.latency".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::new(),
            value: None,
            count: Some(10),
            sum: Some(-5.0),
            min: Some(-10.0),
            max: Some(10.0),
            buckets,
        };
        let previous = histogram(vec![
            ObserveHistogramBucket {
                upper_bound: 0.0,
                count: 5,
            },
            ObserveHistogramBucket {
                upper_bound: 10.0,
                count: 10,
            },
        ]);
        let changed = histogram(vec![ObserveHistogramBucket {
            upper_bound: 10.0,
            count: 10,
        }]);
        assert!(interval_delta(&previous, &changed).is_none());

        let previous = histogram(vec![
            ObserveHistogramBucket {
                upper_bound: 0.0,
                count: 1,
            },
            ObserveHistogramBucket {
                upper_bound: 10.0,
                count: 10,
            },
        ]);
        let mut current = histogram(vec![
            ObserveHistogramBucket {
                upper_bound: 0.0,
                count: 10,
            },
            ObserveHistogramBucket {
                upper_bound: 10.0,
                count: 15,
            },
        ]);
        current.count = Some(15);
        assert!(
            interval_delta(&previous, &current).is_none(),
            "a non-cumulative interval histogram must fail closed"
        );
    }

    #[test]
    fn non_forward_observation_time_gaps_without_replacing_the_baseline() {
        let name = "sinomo.test.requests";
        let mut store = MetricStore::default();
        store.set_history_scope(Some(name.to_string()), None, BTreeMap::new());
        let selection = MetricSelection {
            metric_name: name.to_string(),
            node_id: None,
            labels: BTreeMap::new(),
            transform: MetricTransform::Rate,
        };
        for (received, observed, value) in [(10, 10, 100), (15, 9, 150), (20, 20, 200)] {
            let snapshot = response(
                at(observed),
                at(0),
                Vec::new(),
                vec![ObserveMetricRecord::counter(name, BTreeMap::new(), value)],
            );
            store.ingest_batch(at(received), [("node-a", &snapshot)]);
        }
        assert_eq!(
            store.query(&selection, 10).series[0]
                .points
                .iter()
                .map(|point| point.value)
                .collect::<Vec<_>>(),
            vec![None, None, Some(10.0)]
        );
    }

    #[test]
    fn history_rejects_oversized_histogram_payloads() {
        let buckets = (0..=DEFAULT_METRIC_HISTORY_BUCKETS)
            .map(|index| ObserveHistogramBucket {
                upper_bound: index as f64,
                count: index as u64,
            })
            .collect::<Vec<_>>();
        let record = ObserveMetricRecord {
            name: "sinomo.test.latency".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::new(),
            value: None,
            count: Some(DEFAULT_METRIC_HISTORY_BUCKETS as u64),
            sum: Some(1.0),
            min: Some(0.0),
            max: Some(DEFAULT_METRIC_HISTORY_BUCKETS as f64),
            buckets,
        };
        assert!(StoredMetricValue::from_record(&record).is_none());
    }

    #[test]
    fn source_identity_history_slots_and_catalog_are_bounded() {
        let name = "sinomo.test.queue";
        let gauge = |value| {
            let mut snapshot = response(
                at(value as i64),
                at(0),
                Vec::new(),
                vec![ObserveMetricRecord::gauge(name, BTreeMap::new(), value)],
            );
            snapshot.node_id = Some("stale-payload-id".to_string());
            snapshot
        };
        let first = gauge(1.0);
        let second = gauge(2.0);
        let mut store = MetricStore::with_limits(3, 1, 4);
        store.set_history_scope(Some(name.to_string()), None, BTreeMap::new());
        store.ingest_batch(at(1), [("node-a", &first)]);
        store.ingest_batch(at(2), [("node-b", &second)]);
        let result = store.query(
            &MetricSelection {
                metric_name: name.to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::Current,
            },
            10,
        );
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].key.node_id, "node-b");

        let descriptors = (0..DEFAULT_METRIC_CATALOG_FAMILIES + 4)
            .map(|index| {
                ObserveMetricDescriptor::new(
                    format!("sinomo.generated.metric.{index:04}"),
                    ObserveMetricKind::Gauge,
                    None,
                    "Generated.",
                )
            })
            .collect();
        let catalog = response(at(3), at(0), descriptors, Vec::new());
        store.ingest_batch(at(3), [("node-a", &catalog)]);
        assert_eq!(store.catalog().count(), DEFAULT_METRIC_CATALOG_FAMILIES);
        assert!(store.catalog_truncated());
    }

    #[test]
    fn query_window_filters_by_local_collection_time() {
        let name = "sinomo.test.load";
        let mut store = MetricStore::default();
        store.set_history_scope(Some(name.to_string()), None, BTreeMap::new());
        for (received, observed, value) in [(10, 1_000, 1.0), (70, 1_001, 2.0), (500, 1_002, 3.0)] {
            let snapshot = response(
                at(observed),
                at(0),
                Vec::new(),
                vec![ObserveMetricRecord::gauge(name, BTreeMap::new(), value)],
            );
            store.ingest_batch(at(received), [("node-a", &snapshot)]);
        }
        let result = store.query_window(
            &MetricSelection {
                metric_name: name.to_string(),
                node_id: None,
                labels: BTreeMap::new(),
                transform: MetricTransform::Current,
            },
            180,
            Some(at(400)),
        );
        assert_eq!(result.series[0].points.len(), 1);
        assert_eq!(result.series[0].points[0].at, at(500));
        assert_eq!(result.series[0].points[0].value, Some(3.0));
    }
}
