//! Deterministic OpenMetrics 1.0 text encoding for observation snapshots.

use crate::{ObserveMetricDescriptor, ObserveMetricKind, ObserveMetricRecord, ObserveMetricUnit};
use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt::Write as _;
use thiserror::Error;

/// HTTP content type required for an OpenMetrics 1.0 text response.
pub const OPENMETRICS_CONTENT_TYPE: &str =
    "application/openmetrics-text; version=1.0.0; charset=utf-8";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OpenMetricsEncoder {
    constant_labels: BTreeMap<String, String>,
}

impl OpenMetricsEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds labels to every emitted series. Label names are normalized with the
    /// same conservative rules as record labels; collisions are rejected.
    pub fn with_constant_labels(
        mut self,
        labels: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.constant_labels.extend(
            labels
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
        self
    }

    pub fn encode(
        &self,
        records: &[ObserveMetricRecord],
        descriptors: &[ObserveMetricDescriptor],
    ) -> Result<String, OpenMetricsEncodeError> {
        encode_openmetrics_with_labels(records, descriptors, &self.constant_labels)
    }
}

pub fn encode_openmetrics(
    records: &[ObserveMetricRecord],
    descriptors: &[ObserveMetricDescriptor],
) -> Result<String, OpenMetricsEncodeError> {
    OpenMetricsEncoder::new().encode(records, descriptors)
}

pub fn encode_openmetrics_with_labels(
    records: &[ObserveMetricRecord],
    descriptors: &[ObserveMetricDescriptor],
    constant_labels: &BTreeMap<String, String>,
) -> Result<String, OpenMetricsEncodeError> {
    let descriptors = descriptor_index(descriptors)?;
    let constant_labels = normalize_labels("<constant>", constant_labels, None)?;
    let mut records = records.iter().collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| kind_rank(left.kind).cmp(&kind_rank(right.kind)))
            .then_with(|| left.labels.cmp(&right.labels))
    });

    let mut original_kinds = BTreeMap::<&str, ObserveMetricKind>::new();
    let mut families = BTreeMap::<String, MetricFamily<'_>>::new();
    for record in records {
        if let Some(previous_kind) = original_kinds.insert(&record.name, record.kind)
            && previous_kind != record.kind
        {
            return Err(OpenMetricsEncodeError::TypeCollision {
                name: record.name.clone(),
                first: previous_kind,
                second: record.kind,
            });
        }

        let descriptor = descriptors.get(record.name.as_str()).copied();
        if let Some(descriptor) = descriptor
            && descriptor.kind != record.kind
        {
            return Err(OpenMetricsEncodeError::DescriptorKindMismatch {
                name: record.name.clone(),
                descriptor: descriptor.kind,
                record: record.kind,
            });
        }
        let unit = descriptor.and_then(|descriptor| descriptor.unit);
        let export_name = export_metric_name(&record.name, record.kind, unit);
        let labels = merge_labels(&record.name, &constant_labels, &record.labels, record.kind)?;
        validate_record(record)?;

        match families.entry(export_name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(MetricFamily {
                    export_name,
                    internal_name: record.name.clone(),
                    kind: record.kind,
                    help: descriptor.map(|descriptor| descriptor.help.as_str()),
                    unit,
                    series: vec![PreparedSeries { record, labels }],
                });
            }
            Entry::Occupied(mut entry) => {
                let family = entry.get_mut();
                if family.internal_name != record.name || family.kind != record.kind {
                    return Err(OpenMetricsEncodeError::MetricFamilyCollision {
                        export_name,
                        first_name: family.internal_name.clone(),
                        first_kind: family.kind,
                        second_name: record.name.clone(),
                        second_kind: record.kind,
                    });
                }
                family.series.push(PreparedSeries { record, labels });
            }
        }
    }

    reject_sample_name_collisions(&families)?;
    let mut output = String::new();
    for family in families.values_mut() {
        family
            .series
            .sort_by(|left, right| left.labels.cmp(&right.labels));
        for pair in family.series.windows(2) {
            if pair[0].labels == pair[1].labels {
                return Err(OpenMetricsEncodeError::DuplicateSeries {
                    name: family.export_name.clone(),
                    labels: format_label_set(&pair[0].labels),
                });
            }
        }
        encode_family(&mut output, family)?;
    }
    output.push_str("# EOF\n");
    Ok(output)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OpenMetricsEncodeError {
    #[error("metric descriptor `{name}` was registered more than once with different metadata")]
    ConflictingDescriptor { name: String },
    #[error(
        "metric `{name}` has descriptor type {descriptor:?}, but its record type is {record:?}"
    )]
    DescriptorKindMismatch {
        name: String,
        descriptor: ObserveMetricKind,
        record: ObserveMetricKind,
    },
    #[error("metric `{name}` is present as both {first:?} and {second:?}")]
    TypeCollision {
        name: String,
        first: ObserveMetricKind,
        second: ObserveMetricKind,
    },
    #[error(
        "metrics `{first_name}` ({first_kind:?}) and `{second_name}` ({second_kind:?}) both export as `{export_name}`"
    )]
    MetricFamilyCollision {
        export_name: String,
        first_name: String,
        first_kind: ObserveMetricKind,
        second_name: String,
        second_kind: ObserveMetricKind,
    },
    #[error(
        "metric families `{first_family}` and `{second_family}` both emit sample `{sample_name}`"
    )]
    SampleNameCollision {
        sample_name: String,
        first_family: String,
        second_family: String,
    },
    #[error("metric `{metric}` has colliding labels `{first}` and `{second}` as `{label}`")]
    LabelCollision {
        metric: String,
        label: String,
        first: String,
        second: String,
    },
    #[error("histogram metric `{metric}` cannot define the reserved `le` label")]
    ReservedHistogramLabel { metric: String },
    #[error("metric `{name}` has an invalid record: {detail}")]
    InvalidRecord { name: String, detail: String },
    #[error("metric `{name}` contains a duplicate series with labels {labels}")]
    DuplicateSeries { name: String, labels: String },
}

struct MetricFamily<'a> {
    export_name: String,
    internal_name: String,
    kind: ObserveMetricKind,
    help: Option<&'a str>,
    unit: Option<ObserveMetricUnit>,
    series: Vec<PreparedSeries<'a>>,
}

struct PreparedSeries<'a> {
    record: &'a ObserveMetricRecord,
    labels: BTreeMap<String, String>,
}

fn descriptor_index(
    descriptors: &[ObserveMetricDescriptor],
) -> Result<BTreeMap<&str, &ObserveMetricDescriptor>, OpenMetricsEncodeError> {
    let mut index = BTreeMap::new();
    for descriptor in descriptors {
        match index.entry(descriptor.name.as_str()) {
            Entry::Vacant(entry) => {
                entry.insert(descriptor);
            }
            Entry::Occupied(entry) if *entry.get() != descriptor => {
                return Err(OpenMetricsEncodeError::ConflictingDescriptor {
                    name: descriptor.name.clone(),
                });
            }
            Entry::Occupied(_) => {}
        }
    }
    Ok(index)
}

fn encode_family(
    output: &mut String,
    family: &MetricFamily<'_>,
) -> Result<(), OpenMetricsEncodeError> {
    writeln!(
        output,
        "# TYPE {} {}",
        family.export_name,
        kind_name(family.kind)
    )
    .expect("writing to a String cannot fail");
    if let Some(unit) = openmetrics_unit(family.unit) {
        writeln!(output, "# UNIT {} {unit}", family.export_name)
            .expect("writing to a String cannot fail");
    }
    if let Some(help) = family.help {
        writeln!(
            output,
            "# HELP {} {}",
            family.export_name,
            escape_help(help)
        )
        .expect("writing to a String cannot fail");
    }

    for series in &family.series {
        match family.kind {
            ObserveMetricKind::Counter | ObserveMetricKind::Gauge => {
                let sample_name = scalar_sample_name(&family.export_name, family.kind);
                write_sample(
                    output,
                    &sample_name,
                    &series.labels,
                    scale_value(
                        series.record.value.expect("validated scalar value"),
                        family.unit,
                    ),
                );
            }
            ObserveMetricKind::Histogram => {
                encode_histogram(output, family, series)?;
            }
        }
    }
    Ok(())
}

fn encode_histogram(
    output: &mut String,
    family: &MetricFamily<'_>,
    series: &PreparedSeries<'_>,
) -> Result<(), OpenMetricsEncodeError> {
    let record = series.record;
    let mut buckets = record.buckets.iter().collect::<Vec<_>>();
    buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
    let mut labels = series.labels.clone();
    for bucket in buckets {
        labels.insert(
            "le".to_string(),
            format_float(scale_value(bucket.upper_bound, family.unit)),
        );
        write_integer_sample(
            output,
            &format!("{}_bucket", family.export_name),
            &labels,
            bucket.count,
        );
    }
    labels.insert("le".to_string(), "+Inf".to_string());
    write_integer_sample(
        output,
        &format!("{}_bucket", family.export_name),
        &labels,
        record.count.expect("validated histogram count"),
    );
    write_sample(
        output,
        &format!("{}_sum", family.export_name),
        &series.labels,
        scale_value(record.sum.expect("validated histogram sum"), family.unit),
    );
    write_integer_sample(
        output,
        &format!("{}_count", family.export_name),
        &series.labels,
        record.count.expect("validated histogram count"),
    );
    Ok(())
}

fn write_sample(output: &mut String, name: &str, labels: &BTreeMap<String, String>, value: f64) {
    writeln!(
        output,
        "{name}{} {}",
        format_label_set(labels),
        format_float(value)
    )
    .expect("writing to a String cannot fail");
}

fn write_integer_sample(
    output: &mut String,
    name: &str,
    labels: &BTreeMap<String, String>,
    value: u64,
) {
    writeln!(output, "{name}{} {value}", format_label_set(labels))
        .expect("writing to a String cannot fail");
}

fn validate_record(record: &ObserveMetricRecord) -> Result<(), OpenMetricsEncodeError> {
    match record.kind {
        ObserveMetricKind::Counter => {
            let value = record
                .value
                .ok_or_else(|| invalid_record(record, "counter value is missing"))?;
            if value.is_nan() || value < 0.0 {
                return Err(invalid_record(
                    record,
                    "counter value must be non-negative and not NaN",
                ));
            }
        }
        ObserveMetricKind::Gauge => {
            record
                .value
                .ok_or_else(|| invalid_record(record, "gauge value is missing"))?;
        }
        ObserveMetricKind::Histogram => {
            let count = record
                .count
                .ok_or_else(|| invalid_record(record, "histogram count is missing"))?;
            let sum = record
                .sum
                .ok_or_else(|| invalid_record(record, "histogram sum is missing"))?;
            if sum.is_nan() || sum < 0.0 {
                return Err(invalid_record(
                    record,
                    "histogram sum must be non-negative and not NaN",
                ));
            }
            let mut buckets = record.buckets.iter().collect::<Vec<_>>();
            buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
            let mut previous_bound = None;
            let mut previous_count = 0;
            for bucket in buckets {
                if !bucket.upper_bound.is_finite() {
                    return Err(invalid_record(
                        record,
                        "histogram bucket bounds must be finite; +Inf is synthesized",
                    ));
                }
                if bucket.upper_bound < 0.0 {
                    return Err(invalid_record(
                        record,
                        "histograms with a sum cannot have negative bucket bounds",
                    ));
                }
                if previous_bound == Some(bucket.upper_bound) {
                    return Err(invalid_record(
                        record,
                        "histogram bucket bounds must be unique",
                    ));
                }
                if bucket.count < previous_count {
                    return Err(invalid_record(
                        record,
                        "cumulative histogram bucket counts must not decrease",
                    ));
                }
                if bucket.count > count {
                    return Err(invalid_record(
                        record,
                        "histogram bucket count exceeds total count",
                    ));
                }
                previous_bound = Some(bucket.upper_bound);
                previous_count = bucket.count;
            }
        }
    }
    Ok(())
}

fn invalid_record(
    record: &ObserveMetricRecord,
    detail: impl Into<String>,
) -> OpenMetricsEncodeError {
    OpenMetricsEncodeError::InvalidRecord {
        name: record.name.clone(),
        detail: detail.into(),
    }
}

fn merge_labels(
    metric: &str,
    constant: &BTreeMap<String, String>,
    record: &BTreeMap<String, String>,
    kind: ObserveMetricKind,
) -> Result<BTreeMap<String, String>, OpenMetricsEncodeError> {
    let mut labels = constant.clone();
    let normalized_record = normalize_labels(metric, record, Some(constant))?;
    for (name, value) in normalized_record {
        if labels.contains_key(&name) {
            return Err(OpenMetricsEncodeError::LabelCollision {
                metric: metric.to_string(),
                label: name.clone(),
                first: name.clone(),
                second: name,
            });
        }
        labels.insert(name, value);
    }
    if kind == ObserveMetricKind::Histogram && labels.contains_key("le") {
        return Err(OpenMetricsEncodeError::ReservedHistogramLabel {
            metric: metric.to_string(),
        });
    }
    Ok(labels)
}

fn normalize_labels(
    metric: &str,
    labels: &BTreeMap<String, String>,
    existing: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, OpenMetricsEncodeError> {
    let mut normalized = BTreeMap::<String, (String, String)>::new();
    for (original, value) in labels {
        let name = normalize_identifier(original);
        if let Some((first, _)) = normalized.get(&name) {
            return Err(OpenMetricsEncodeError::LabelCollision {
                metric: metric.to_string(),
                label: name,
                first: first.clone(),
                second: original.clone(),
            });
        }
        if existing.is_some_and(|existing| existing.contains_key(&name)) {
            return Err(OpenMetricsEncodeError::LabelCollision {
                metric: metric.to_string(),
                label: name,
                first: "constant label".to_string(),
                second: original.clone(),
            });
        }
        normalized.insert(name, (original.clone(), value.clone()));
    }
    Ok(normalized
        .into_iter()
        .map(|(name, (_, value))| (name, value))
        .collect())
}

fn reject_sample_name_collisions(
    families: &BTreeMap<String, MetricFamily<'_>>,
) -> Result<(), OpenMetricsEncodeError> {
    // Family names also occupy the namespace: a histogram family named
    // `foo_count` would be ambiguous beside the `foo_count` sample generated
    // by a histogram family named `foo` even though the former has no bare
    // sample of its own.
    let mut owners = families
        .keys()
        .map(|name| (name.clone(), name.clone()))
        .collect::<BTreeMap<_, _>>();
    for family in families.values() {
        let names = match family.kind {
            ObserveMetricKind::Counter => vec![
                format!("{}_total", family.export_name),
                format!("{}_created", family.export_name),
            ],
            ObserveMetricKind::Gauge => vec![family.export_name.clone()],
            ObserveMetricKind::Histogram => vec![
                format!("{}_bucket", family.export_name),
                format!("{}_sum", family.export_name),
                format!("{}_count", family.export_name),
                format!("{}_created", family.export_name),
            ],
        };
        for sample_name in names {
            if let Some(first_family) =
                owners.insert(sample_name.clone(), family.export_name.clone())
                && first_family != family.export_name
            {
                return Err(OpenMetricsEncodeError::SampleNameCollision {
                    sample_name,
                    first_family,
                    second_family: family.export_name.clone(),
                });
            }
        }
    }
    Ok(())
}

fn export_metric_name(
    internal_name: &str,
    kind: ObserveMetricKind,
    unit: Option<ObserveMetricUnit>,
) -> String {
    let normalized = normalize_metric_name(internal_name);
    let mut base = if kind == ObserveMetricKind::Counter {
        normalized
            .strip_suffix("_total")
            .unwrap_or(&normalized)
            .to_string()
    } else {
        normalized
    };
    // Removing a source-unit suffix is safe only because the descriptor made
    // that unit explicit. Undescribed names such as `duration_us` are kept
    // verbatim and are never scaled or reinterpreted.
    if unit == Some(ObserveMetricUnit::Microseconds) {
        if let Some(without_unit) = base.strip_suffix("_microseconds") {
            base = without_unit.to_string();
        } else if let Some(without_unit) = base.strip_suffix("_us") {
            base = without_unit.to_string();
        }
    }
    while base.ends_with('_') {
        base.pop();
    }
    if base.is_empty() {
        base.push_str("metric");
    }
    if let Some(suffix) = unit_suffix(unit)
        && base != suffix
        && !base.ends_with(&format!("_{suffix}"))
    {
        base.push('_');
        base.push_str(suffix);
    }
    base
}

fn scalar_sample_name(family_name: &str, kind: ObserveMetricKind) -> String {
    match kind {
        ObserveMetricKind::Counter => format!("{family_name}_total"),
        ObserveMetricKind::Gauge => family_name.to_string(),
        ObserveMetricKind::Histogram => unreachable!("histograms have structured samples"),
    }
}

fn normalize_metric_name(value: &str) -> String {
    let normalized = normalize_identifier(value);
    if normalized.starts_with('_') {
        format!("metric{normalized}")
    } else {
        normalized
    }
}

fn normalize_identifier(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_underscore = false;
    for character in value.chars() {
        let character = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            '_'
        };
        if character == '_' {
            if previous_underscore {
                continue;
            }
            previous_underscore = true;
        } else {
            previous_underscore = false;
        }
        normalized.push(character);
    }
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized.as_bytes()[0].is_ascii_digit() {
        normalized.insert(0, '_');
    }
    normalized
}

fn format_label_set(labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut output = String::from("{");
    for (index, (name, value)) in labels.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        write!(output, "{name}=\"{}\"", escape_label_value(value))
            .expect("writing to a String cannot fail");
    }
    output.push('}');
    output
}

fn escape_label_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' | '\r' => output.push_str("\\n"),
            character => output.push(character),
        }
    }
    output
}

fn escape_help(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' | '\r' => output.push_str("\\n"),
            character => output.push(character),
        }
    }
    output
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

fn kind_name(kind: ObserveMetricKind) -> &'static str {
    match kind {
        ObserveMetricKind::Counter => "counter",
        ObserveMetricKind::Gauge => "gauge",
        ObserveMetricKind::Histogram => "histogram",
    }
}

fn kind_rank(kind: ObserveMetricKind) -> u8 {
    match kind {
        ObserveMetricKind::Counter => 0,
        ObserveMetricKind::Gauge => 1,
        ObserveMetricKind::Histogram => 2,
    }
}

fn unit_suffix(unit: Option<ObserveMetricUnit>) -> Option<&'static str> {
    match unit {
        Some(ObserveMetricUnit::Bytes) => Some("bytes"),
        Some(ObserveMetricUnit::Seconds | ObserveMetricUnit::Microseconds) => Some("seconds"),
        Some(ObserveMetricUnit::Ratio) => Some("ratio"),
        Some(ObserveMetricUnit::Count) | None => None,
    }
}

fn openmetrics_unit(unit: Option<ObserveMetricUnit>) -> Option<&'static str> {
    unit_suffix(unit)
}

fn scale_value(value: f64, unit: Option<ObserveMetricUnit>) -> f64 {
    match unit {
        Some(ObserveMetricUnit::Microseconds) => value / 1_000_000.0,
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObserveHistogramBucket;

    fn descriptor(
        name: &str,
        kind: ObserveMetricKind,
        unit: Option<ObserveMetricUnit>,
        help: &str,
    ) -> ObserveMetricDescriptor {
        ObserveMetricDescriptor {
            name: name.to_string(),
            kind,
            unit,
            help: help.to_string(),
        }
    }

    #[test]
    fn encodes_sorted_scalar_families_with_metadata_and_escaping() {
        let records = vec![
            ObserveMetricRecord::gauge(
                "Sinomo.Node Load%",
                BTreeMap::from([("node.name".to_string(), "alpha\\\"one\nnext".to_string())]),
                0.5,
            ),
            ObserveMetricRecord::counter(
                "sinomo.rpc.requests",
                BTreeMap::from([("method".to_string(), "GET".to_string())]),
                7,
            ),
        ];
        let descriptors = vec![
            descriptor(
                "sinomo.rpc.requests",
                ObserveMetricKind::Counter,
                Some(ObserveMetricUnit::Count),
                "RPC \"calls\"\\total\nby method",
            ),
            descriptor(
                "Sinomo.Node Load%",
                ObserveMetricKind::Gauge,
                Some(ObserveMetricUnit::Ratio),
                "Current load",
            ),
        ];

        let encoded = OpenMetricsEncoder::new()
            .with_constant_labels([("cluster.id", "cluster-a")])
            .encode(&records, &descriptors)
            .unwrap();

        assert_eq!(
            encoded,
            concat!(
                "# TYPE sinomo_node_load_ratio gauge\n",
                "# UNIT sinomo_node_load_ratio ratio\n",
                "# HELP sinomo_node_load_ratio Current load\n",
                "sinomo_node_load_ratio{cluster_id=\"cluster-a\",node_name=\"alpha\\\\\\\"one\\nnext\"} 0.5\n",
                "# TYPE sinomo_rpc_requests counter\n",
                "# HELP sinomo_rpc_requests RPC \\\"calls\\\"\\\\total\\nby method\n",
                "sinomo_rpc_requests_total{cluster_id=\"cluster-a\",method=\"GET\"} 7\n",
                "# EOF\n",
            )
        );
        assert_eq!(
            OPENMETRICS_CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8"
        );
    }

    #[test]
    fn scales_explicit_microsecond_histogram_and_adds_infinity_bucket() {
        let records = vec![ObserveMetricRecord {
            name: "sinomo.rpc.latency.us".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::from([("route".to_string(), "/v1".to_string())]),
            value: None,
            count: Some(4),
            sum: Some(2_500_000.0),
            min: Some(100.0),
            max: Some(2_000_000.0),
            buckets: vec![
                ObserveHistogramBucket {
                    upper_bound: 1_000_000.0,
                    count: 3,
                },
                ObserveHistogramBucket {
                    upper_bound: 100_000.0,
                    count: 2,
                },
            ],
        }];
        let descriptors = vec![descriptor(
            "sinomo.rpc.latency.us",
            ObserveMetricKind::Histogram,
            Some(ObserveMetricUnit::Microseconds),
            "RPC latency",
        )];

        assert_eq!(
            encode_openmetrics(&records, &descriptors).unwrap(),
            concat!(
                "# TYPE sinomo_rpc_latency_seconds histogram\n",
                "# UNIT sinomo_rpc_latency_seconds seconds\n",
                "# HELP sinomo_rpc_latency_seconds RPC latency\n",
                "sinomo_rpc_latency_seconds_bucket{le=\"0.1\",route=\"/v1\"} 2\n",
                "sinomo_rpc_latency_seconds_bucket{le=\"1\",route=\"/v1\"} 3\n",
                "sinomo_rpc_latency_seconds_bucket{le=\"+Inf\",route=\"/v1\"} 4\n",
                "sinomo_rpc_latency_seconds_sum{route=\"/v1\"} 2.5\n",
                "sinomo_rpc_latency_seconds_count{route=\"/v1\"} 4\n",
                "# EOF\n",
            )
        );
    }

    #[test]
    fn missing_descriptor_does_not_guess_a_unit_or_scale() {
        let record = ObserveMetricRecord::gauge("work.duration_us", BTreeMap::new(), 1_000_000.0);
        assert_eq!(
            encode_openmetrics(&[record], &[]).unwrap(),
            "# TYPE work_duration_us gauge\nwork_duration_us 1000000\n# EOF\n"
        );
    }

    #[test]
    fn output_is_deterministic_across_input_order() {
        let one = ObserveMetricRecord::gauge(
            "example.value",
            BTreeMap::from([("node".to_string(), "b".to_string())]),
            2.0,
        );
        let two = ObserveMetricRecord::gauge(
            "example.value",
            BTreeMap::from([("node".to_string(), "a".to_string())]),
            1.0,
        );
        assert_eq!(
            encode_openmetrics(&[one.clone(), two.clone()], &[]).unwrap(),
            encode_openmetrics(&[two, one], &[]).unwrap()
        );
    }

    #[test]
    fn rejects_normalized_metric_and_type_collisions() {
        let collision = encode_openmetrics(
            &[
                ObserveMetricRecord::gauge("a.b", BTreeMap::new(), 1.0),
                ObserveMetricRecord::gauge("a-b", BTreeMap::new(), 2.0),
            ],
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            collision,
            OpenMetricsEncodeError::MetricFamilyCollision { .. }
        ));

        let type_collision = encode_openmetrics(
            &[
                ObserveMetricRecord::gauge("same", BTreeMap::new(), 1.0),
                ObserveMetricRecord::counter("same", BTreeMap::new(), 1),
            ],
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            type_collision,
            OpenMetricsEncodeError::TypeCollision { .. }
        ));
    }

    #[test]
    fn rejects_label_collisions_duplicates_and_reserved_histogram_label() {
        let label_collision = ObserveMetricRecord::gauge(
            "metric",
            BTreeMap::from([
                ("node.id".to_string(), "one".to_string()),
                ("node-id".to_string(), "two".to_string()),
            ]),
            1.0,
        );
        assert!(matches!(
            encode_openmetrics(&[label_collision], &[]),
            Err(OpenMetricsEncodeError::LabelCollision { .. })
        ));

        let duplicate = ObserveMetricRecord::gauge("metric", BTreeMap::new(), 1.0);
        assert!(matches!(
            encode_openmetrics(&[duplicate.clone(), duplicate], &[]),
            Err(OpenMetricsEncodeError::DuplicateSeries { .. })
        ));

        let histogram = ObserveMetricRecord {
            name: "histogram".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::from([("le".to_string(), "bad".to_string())]),
            value: None,
            count: Some(0),
            sum: Some(0.0),
            min: None,
            max: None,
            buckets: Vec::new(),
        };
        assert!(matches!(
            encode_openmetrics(&[histogram], &[]),
            Err(OpenMetricsEncodeError::ReservedHistogramLabel { .. })
        ));
    }

    #[test]
    fn rejects_invalid_histograms_and_generated_sample_collisions() {
        let invalid = ObserveMetricRecord {
            name: "hist".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::new(),
            value: None,
            count: Some(1),
            sum: Some(1.0),
            min: None,
            max: None,
            buckets: vec![ObserveHistogramBucket {
                upper_bound: f64::INFINITY,
                count: 1,
            }],
        };
        assert!(matches!(
            encode_openmetrics(&[invalid], &[]),
            Err(OpenMetricsEncodeError::InvalidRecord { .. })
        ));

        let histogram = ObserveMetricRecord {
            name: "latency".to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: BTreeMap::new(),
            value: None,
            count: Some(0),
            sum: Some(0.0),
            min: None,
            max: None,
            buckets: Vec::new(),
        };
        let gauge = ObserveMetricRecord::gauge("latency.count", BTreeMap::new(), 0.0);
        assert!(matches!(
            encode_openmetrics(&[histogram, gauge], &[]),
            Err(OpenMetricsEncodeError::SampleNameCollision { .. })
        ));
    }

    #[test]
    fn uses_openmetrics_spellings_for_special_floats() {
        let records = vec![
            ObserveMetricRecord::gauge("value.nan", BTreeMap::new(), f64::NAN),
            ObserveMetricRecord::gauge("value.pos", BTreeMap::new(), f64::INFINITY),
            ObserveMetricRecord::gauge("value.neg", BTreeMap::new(), f64::NEG_INFINITY),
        ];
        let encoded = encode_openmetrics(&records, &[]).unwrap();
        assert!(encoded.contains("value_nan NaN\n"));
        assert!(encoded.contains("value_pos +Inf\n"));
        assert!(encoded.contains("value_neg -Inf\n"));
    }
}
