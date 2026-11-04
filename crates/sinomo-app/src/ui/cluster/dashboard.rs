//! Curated operational dashboard. Snapshot gauges use reported maxima rather
//! than adding potentially replicated state. Rates remain process-reset aware.
use super::*;
use crate::metrics::{MetricPoint, MetricSeriesKey};
use sui::{IconButton, ScrollState};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum DashboardMetric {
    AgentActiveTurns,
    AgentPendingTasks,
    AgentFailedTasks,
    ModelRequests,
    ModelInputTokens,
    ModelOutputTokens,
    ToolCalls,
    DfsBacklog,

    DfsPendingWrites,
    RpcRate,
    RpcErrorRate,
    RpcP95Ms,
    CpuPercent,
    MemoryPercent,
    DiskFreePercent,
    NetworkRxRate,
    NetworkTxRate,
    KvP95Ms,
    KvQueueDepth,
    KvReplicaLag,
    DfsReplicaLag,
    DfsFailedJobs,
    AgentBufferedEvents,
    ExporterErrors,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Group {
    Agents,
    Storage,
    Distributed,
    Capacity,
    Diagnostics,
}

impl Group {
    const ALL: [Self; 5] = [
        Self::Agents,
        Self::Storage,
        Self::Distributed,
        Self::Capacity,
        Self::Diagnostics,
    ];
    fn title(self) -> &'static str {
        match self {
            Self::Agents => "Agent activity",
            Self::Storage => "DFS health & durability",
            Self::Distributed => "Cluster traffic & responsiveness",
            Self::Capacity => "Node capacity",
            Self::Diagnostics => "Detailed diagnostics",
        }
    }
    fn description(self) -> &'static str {
        match self {
            Self::Agents => "Work in progress, queued work, and retained failures",
            Self::Storage => {
                "Repair pressure, conflicting versions, and writes awaiting completion"
            }
            Self::Distributed => "Request volume, errors, and slowest observed request latency",
            Self::Capacity => "Resource pressure and host network throughput",
            Self::Diagnostics => {
                "Queues, replication lag, exporter failures, and per-node evidence"
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Read {
    Gauge,
    GaugeMax,
    FailedTasks,
    Rate,
    P95,
    Percent,
    Fraction(&'static str),
}

pub(super) struct DashboardPanelSpec {
    pub metric: DashboardMetric,
    pub title: &'static str,
    pub unit: &'static str,
    pub explorer_metric: &'static str,
    pub explorer_transform: ExplorerTransform,
    pub explorer_labels: &'static [(&'static str, &'static str)],
    group: Group,
    read: Read,
    input_unit: ObserveMetricUnit,
    window: ExplorerWindow,
    sum_nodes: bool,
    description: &'static str,
}

pub(super) struct DashboardSpec {
    pub panels: &'static [DashboardPanelSpec],
}

const fn panel(
    metric: DashboardMetric,
    title: &'static str,
    family: &'static str,
    group: Group,
    read: Read,
    input_unit: ObserveMetricUnit,
    window: ExplorerWindow,
    unit: &'static str,
    description: &'static str,
) -> DashboardPanelSpec {
    DashboardPanelSpec {
        metric,
        title,
        explorer_metric: family,
        group,
        read,
        input_unit,
        window,
        unit,
        description,
        explorer_transform: match read {
            Read::Rate => ExplorerTransform::Rate,
            Read::P95 => ExplorerTransform::P95,
            _ => ExplorerTransform::Current,
        },
        explorer_labels: &[],
        sum_nodes: matches!(read, Read::Rate),
    }
}

const fn labels(
    mut panel: DashboardPanelSpec,
    filters: &'static [(&'static str, &'static str)],
) -> DashboardPanelSpec {
    panel.explorer_labels = filters;
    panel
}

pub(super) const DEFAULT_DASHBOARD: DashboardSpec = DashboardSpec {
    panels: &[
        panel(
            DashboardMetric::AgentActiveTurns,
            "Active agent turns",
            "sinomo.agent.active_turns",
            Group::Agents,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "",
            "Busiest reporting node",
        ),
        labels(
            panel(
                DashboardMetric::AgentPendingTasks,
                "Queued agent tasks",
                "sinomo.agent.tasks",
                Group::Agents,
                Read::Gauge,
                ObserveMetricUnit::Count,
                ExplorerWindow::FiveMinutes,
                "",
                "Pending tasks · highest node snapshot",
            ),
            &[("status", "pending")],
        ),
        panel(
            DashboardMetric::AgentFailedTasks,
            "Failed / timed-out tasks",
            "sinomo.agent.tasks",
            Group::Agents,
            Read::FailedTasks,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "",
            "Retained failed + timed-out states · highest node",
        ),
        panel(
            DashboardMetric::ModelRequests,
            "Model requests",
            "sinomo.usage.model.requests",
            Group::Agents,
            Read::Rate,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "/s",
            "Completed provider requests across reporting nodes",
        ),
        labels(
            panel(
                DashboardMetric::ModelInputTokens,
                "Input tokens",
                "sinomo.usage.model.tokens",
                Group::Agents,
                Read::Rate,
                ObserveMetricUnit::Count,
                ExplorerWindow::FiveMinutes,
                "tokens/s",
                "Provider-reported input tokens including cache subsets",
            ),
            &[("unit", "input")],
        ),
        labels(
            panel(
                DashboardMetric::ModelOutputTokens,
                "Output tokens",
                "sinomo.usage.model.tokens",
                Group::Agents,
                Read::Rate,
                ObserveMetricUnit::Count,
                ExplorerWindow::FiveMinutes,
                "tokens/s",
                "Provider-reported output tokens including reasoning",
            ),
            &[("unit", "output")],
        ),
        panel(
            DashboardMetric::ToolCalls,
            "Tool calls",
            "sinomo.usage.tool.calls",
            Group::Agents,
            Read::Rate,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "/s",
            "Completed orchestrated tool calls across reporting nodes",
        ),
        panel(
            DashboardMetric::DfsBacklog,
            "DFS repairs",
            "sinomo.dfs.pending_repairs",
            Group::Storage,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "",
            "Pending repairs · highest node snapshot",
        ),
        panel(
            DashboardMetric::DfsPendingWrites,
            "Pending DFS writes",
            "sinomo.dfs.pending_writes",
            Group::Storage,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "",
            "Unfinished writes · highest node snapshot",
        ),
        panel(
            DashboardMetric::RpcRate,
            "RPC throughput",
            "sinomo.daemon.service.requests",
            Group::Distributed,
            Read::Rate,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "/s",
            "Sum of reporting nodes · observation calls excluded",
        ),
        labels(
            panel(
                DashboardMetric::RpcErrorRate,
                "RPC errors",
                "sinomo.daemon.service.requests",
                Group::Distributed,
                Read::Rate,
                ObserveMetricUnit::Count,
                ExplorerWindow::FiveMinutes,
                "/s",
                "Error responses per second · reporting nodes",
            ),
            &[("outcome", "error")],
        ),
        panel(
            DashboardMetric::RpcP95Ms,
            "RPC max-node p95",
            "sinomo.daemon.service.total.us",
            Group::Distributed,
            Read::P95,
            ObserveMetricUnit::Microseconds,
            ExplorerWindow::FiveMinutes,
            "ms",
            "Highest route / node p95 · interval histogram",
        ),
        panel(
            DashboardMetric::CpuPercent,
            "CPU utilization",
            "sinomo.node.cpu.utilization",
            Group::Capacity,
            Read::Percent,
            ObserveMetricUnit::Ratio,
            ExplorerWindow::OneMinute,
            "%",
            "Busiest node · fixed 0–100% scale",
        ),
        panel(
            DashboardMetric::MemoryPercent,
            "Memory utilization",
            "sinomo.node.memory.used.bytes",
            Group::Capacity,
            Read::Fraction("sinomo.node.memory.total.bytes"),
            ObserveMetricUnit::Bytes,
            ExplorerWindow::FifteenMinutes,
            "%",
            "Highest node utilization · fixed 0–100% scale",
        ),
        panel(
            DashboardMetric::DiskFreePercent,
            "Data disk free",
            "sinomo.node.data_disk.available.bytes",
            Group::Capacity,
            Read::Fraction("sinomo.node.data_disk.total.bytes"),
            ObserveMetricUnit::Bytes,
            ExplorerWindow::FifteenMinutes,
            "%",
            "Lowest free space on a node's data filesystem",
        ),
        panel(
            DashboardMetric::NetworkRxRate,
            "Network receive",
            "sinomo.node.network.received.bytes",
            Group::Capacity,
            Read::Rate,
            ObserveMetricUnit::Bytes,
            ExplorerWindow::FiveMinutes,
            "B/s",
            "Host receive traffic · sum of reporting nodes",
        ),
        panel(
            DashboardMetric::NetworkTxRate,
            "Network transmit",
            "sinomo.node.network.transmitted.bytes",
            Group::Capacity,
            Read::Rate,
            ObserveMetricUnit::Bytes,
            ExplorerWindow::FiveMinutes,
            "B/s",
            "Host transmit traffic · sum of reporting nodes",
        ),
        labels(
            panel(
                DashboardMetric::KvP95Ms,
                "KV max-node p95",
                "sinomo.kv.command.total.us",
                Group::Diagnostics,
                Read::P95,
                ObserveMetricUnit::Microseconds,
                ExplorerWindow::FiveMinutes,
                "ms",
                "Highest successful-command p95 · interval histogram",
            ),
            &[("outcome", "ok")],
        ),
        panel(
            DashboardMetric::KvQueueDepth,
            "KV commit queue",
            "sinomo.kv.commit.queue_depth",
            Group::Diagnostics,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "",
            "Highest node proposal queue",
        ),
        panel(
            DashboardMetric::KvReplicaLag,
            "KV replication lag",
            "sinomo.kv.peer.lag_entries",
            Group::Diagnostics,
            Read::GaugeMax,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "entries",
            "Largest reported peer lag",
        ),
        panel(
            DashboardMetric::DfsReplicaLag,
            "DFS metadata lag",
            "sinomo.dfs.metadata_shard.replica_lag_entries",
            Group::Diagnostics,
            Read::GaugeMax,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "entries",
            "Largest reported shard / replica lag",
        ),
        panel(
            DashboardMetric::DfsFailedJobs,
            "Failed namespace jobs",
            "sinomo.dfs.failed_namespace_jobs",
            Group::Diagnostics,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "",
            "Highest node snapshot",
        ),
        panel(
            DashboardMetric::AgentBufferedEvents,
            "Buffered agent events",
            "sinomo.agent.buffered_events",
            Group::Diagnostics,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "",
            "Highest node event buffer",
        ),
        panel(
            DashboardMetric::ExporterErrors,
            "Metric exporter errors",
            "sinomo.observe.metric_exporter.errors",
            Group::Diagnostics,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FiveMinutes,
            "",
            "Highest node exporter error count",
        ),
    ],
};

impl DashboardMetric {
    pub const ALL: [Self; 24] = [
        Self::AgentActiveTurns,
        Self::AgentPendingTasks,
        Self::AgentFailedTasks,
        Self::ModelRequests,
        Self::ModelInputTokens,
        Self::ModelOutputTokens,
        Self::ToolCalls,
        Self::DfsBacklog,
        Self::DfsPendingWrites,
        Self::RpcRate,
        Self::RpcErrorRate,
        Self::RpcP95Ms,
        Self::CpuPercent,
        Self::MemoryPercent,
        Self::DiskFreePercent,
        Self::NetworkRxRate,
        Self::NetworkTxRate,
        Self::KvP95Ms,
        Self::KvQueueDepth,
        Self::KvReplicaLag,
        Self::DfsReplicaLag,
        Self::DfsFailedJobs,
        Self::AgentBufferedEvents,
        Self::ExporterErrors,
    ];
    pub fn spec(self) -> &'static DashboardPanelSpec {
        DEFAULT_DASHBOARD
            .panels
            .iter()
            .find(|panel| panel.metric == self)
            .expect("curated metric spec")
    }
    pub fn title(self) -> &'static str {
        self.spec().title
    }
    pub fn unit(self) -> &'static str {
        self.spec().unit
    }
    pub fn window(self) -> ExplorerWindow {
        self.spec().window
    }
    pub fn aggregate(self, previous: f64, next: f64) -> f64 {
        if self.spec().sum_nodes {
            previous + next
        } else if self == Self::DiskFreePercent {
            previous.min(next)
        } else {
            previous.max(next)
        }
    }
    fn tone(self, value: Option<f64>) -> SemanticTone {
        let Some(value) = value else {
            return SemanticTone::Neutral;
        };
        match self {
            Self::DfsFailedJobs | Self::ExporterErrors | Self::RpcErrorRate if value > 0.0 => {
                SemanticTone::Danger
            }
            Self::AgentFailedTasks
            | Self::DfsBacklog
            | Self::KvReplicaLag
            | Self::DfsReplicaLag
                if value > 0.0 =>
            {
                SemanticTone::Warning
            }
            Self::CpuPercent | Self::MemoryPercent if value >= 90.0 => SemanticTone::Warning,
            Self::DiskFreePercent if value <= 10.0 => SemanticTone::Danger,
            _ => SemanticTone::Accent,
        }
    }
}

pub(super) fn metric_window(name: &str, kind: ObserveMetricKind) -> ExplorerWindow {
    DEFAULT_DASHBOARD
        .panels
        .iter()
        .find(|panel| panel.explorer_metric == name)
        .map(|panel| panel.window)
        .unwrap_or(
            if name.starts_with("sinomo.agent.") || kind != ObserveMetricKind::Gauge {
                ExplorerWindow::FiveMinutes
            } else {
                ExplorerWindow::FifteenMinutes
            },
        )
}

pub(super) fn dashboard_values(
    current: &ObserveMetricsResponse,
    interval: Option<&ObserveMetricsResponse>,
    elapsed: Option<f64>,
) -> BTreeMap<DashboardMetric, Option<f64>> {
    DashboardMetric::ALL
        .into_iter()
        .map(|metric| {
            let spec = metric.spec();
            let include = |record: &sinomo_runtime::observe::ObserveMetricRecord| {
                spec.explorer_labels.iter().all(|(key, value)| {
                    record
                        .labels
                        .get(*key)
                        .is_some_and(|actual| actual == value)
                }) && (!matches!(
                    metric,
                    DashboardMetric::RpcRate
                        | DashboardMetric::RpcErrorRate
                        | DashboardMetric::RpcP95Ms
                ) || (record.labels.get("category").map(String::as_str) != Some("observe")
                    && record.labels.get("route").map(String::as_str) != Some("/observe/metrics")))
            };
            let gauge = || {
                metric_value_sum(
                    current,
                    spec.explorer_metric,
                    ObserveMetricKind::Gauge,
                    spec.input_unit,
                    include,
                )
            };
            let value = match spec.read {
                Read::Gauge => gauge(),
                Read::GaugeMax => metric_family_compatible(
                    current,
                    spec.explorer_metric,
                    ObserveMetricKind::Gauge,
                    spec.input_unit,
                )
                .and_then(|_| {
                    current
                        .metrics
                        .iter()
                        .filter(|record| {
                            record.name == spec.explorer_metric
                                && record.kind == ObserveMetricKind::Gauge
                                && include(record)
                        })
                        .filter_map(|record| record.value)
                        .filter(|value| value.is_finite())
                        .reduce(f64::max)
                }),
                Read::FailedTasks => metric_value_sum(
                    current,
                    spec.explorer_metric,
                    ObserveMetricKind::Gauge,
                    ObserveMetricUnit::Count,
                    |record| {
                        matches!(
                            record.labels.get("status").map(String::as_str),
                            Some("failed" | "timed_out")
                        )
                    },
                ),
                Read::Rate => interval
                    .zip(elapsed)
                    .filter(|(_, elapsed)| elapsed.is_finite() && *elapsed > 0.0)
                    .and_then(|(sample, elapsed)| {
                        metric_value_sum(
                            sample,
                            spec.explorer_metric,
                            ObserveMetricKind::Counter,
                            spec.input_unit,
                            include,
                        )
                        .map(|value| value / elapsed)
                    }),
                Read::P95 => interval
                    .and_then(|sample| {
                        metric_histogram_quantile_max(
                            sample,
                            spec.explorer_metric,
                            spec.input_unit,
                            0.95,
                            include,
                        )
                    })
                    .map(|value| value / 1000.0),
                Read::Percent => gauge()
                    .filter(|value| (0.0..=1.0).contains(value))
                    .map(|value| value * 100.0),
                Read::Fraction(denominator) => gauge()
                    .zip(metric_value_sum(
                        current,
                        denominator,
                        ObserveMetricKind::Gauge,
                        spec.input_unit,
                        |_| true,
                    ))
                    .filter(|(used, total)| *total > 0.0 && *used >= 0.0 && used <= total)
                    .map(|(used, total)| used / total * 100.0),
            }
            .filter(|value| value.is_finite() && *value >= 0.0);
            (metric, value)
        })
        .collect()
}

/// The explorer has already ingested the full batch. Keep only curated inputs for the
/// dashboard's separate per-node baselines, including denominators for capacity ratios.
pub(super) fn dashboard_snapshot(mut response: ObserveMetricsResponse) -> ObserveMetricsResponse {
    static FAMILIES: std::sync::OnceLock<BTreeSet<&'static str>> = std::sync::OnceLock::new();
    let families = FAMILIES.get_or_init(|| {
        let mut names = BTreeSet::new();
        for spec in DEFAULT_DASHBOARD.panels {
            names.insert(spec.explorer_metric);
            if let Read::Fraction(denominator) = spec.read {
                names.insert(denominator);
            }
        }
        names
    });
    response
        .metrics
        .retain(|record| families.contains(record.name.as_str()));
    response
        .descriptors
        .retain(|descriptor| families.contains(descriptor.name.as_str()));
    response
}

pub(super) fn dashboard_metric_has_value_conflict(
    store: &MetricStore,
    metric: DashboardMetric,
) -> bool {
    let spec = metric.spec();
    store
        .catalog_entry(spec.explorer_metric)
        .is_some_and(|entry| entry.value_conflict)
        || matches!(spec.read, Read::Fraction(name) if store.catalog_entry(name).is_some_and(|entry| entry.value_conflict))
}

pub(super) fn format_dashboard_value(metric: DashboardMetric, value: Option<f64>) -> String {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "—".into();
    };
    match metric.unit() {
        "/s" => format!("{value:.2}/s"),
        "ms" => format!("{value:.1} ms"),
        "%" => format!("{value:.1}%"),
        "B/s" => format_metric_value(
            Some(value),
            Some(ObserveMetricUnit::Bytes),
            ExplorerTransform::Rate,
            ObserveMetricKind::Counter,
        ),
        "entries" => format!("{value:.0} entries"),
        _ => format!("{value:.0}"),
    }
}

/// A label at a type-ramp step, weight, and ink resolved from the live theme.
fn text(
    reader: &ThemeReader,
    value: impl Into<String>,
    token: fn(&DefaultTheme) -> ThemeTextToken,
    weight: FontWeight,
    ink: fn(&DefaultTheme) -> Color,
) -> Label {
    Label::new(value).text_style_when(themed_style(reader, token, weight, ink))
}

/// What dashboard values read besides the structure key: sample history, node targets, and
/// node availability. The observation revision and clock change with every sample.
type SampleKey = (u64, Option<OffsetDateTime>, ClusterSnapshot);
type Samples = ClusterSelector<SampleKey>;

fn samples(shared: &Shared) -> Samples {
    cluster_select(shared, "dashboard samples", |key| {
        (
            key.observation_revision,
            key.observation_time,
            key.snapshot.clone(),
        )
    })
}

/// A label whose text and ink follow the samples without being rebuilt.
fn live_label(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
    token: fn(&DefaultTheme) -> ThemeTextToken,
    weight: FontWeight,
    read: impl Fn(&Shared, &DefaultTheme) -> (String, Color) + 'static,
) -> widgets::Restyle<Label, SampleKey> {
    let read = Rc::new(read);
    let (text_shared, text_reader, text_read) =
        (shared.clone(), Rc::clone(reader), Rc::clone(&read));
    let (style_shared, style_reader) = (shared.clone(), Rc::clone(reader));
    live(
        Label::new(read(shared, &reader()).0)
            .text_when(move || text_read(&text_shared, &text_reader()).0)
            .text_style_when(move || {
                let theme = style_reader();
                let (_, ink) = read(&style_shared, &theme);
                crate::theme::token_text_style(&theme, token(&theme), weight, ink)
            }),
        samples.clone(),
    )
}

/// The ratio of the live body text size to the default, for geometry authored at the default.
fn text_scale(theme: &DefaultTheme) -> f32 {
    theme.text.base.size / DefaultTheme::default().text.base.size
}

/// A slot whose height is authored for the default text size and follows the live text scale,
/// so a text-size change re-flows the dashboard without rebuilding it.
struct ScaledHeight {
    reader: ThemeReader,
    height: f32,
    at_least_authored: bool,
    child: WidgetPod,
}

impl ScaledHeight {
    fn new(reader: &ThemeReader, height: f32, child: impl Widget + 'static) -> Self {
        Self {
            reader: Rc::clone(reader),
            height,
            at_least_authored: false,
            child: WidgetPod::new(child),
        }
    }

    /// Never shorter than the authored height, even at smaller text sizes.
    fn at_least_authored(mut self) -> Self {
        self.at_least_authored = true;
        self
    }
}

impl Widget for ScaledHeight {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let scale = text_scale(&(self.reader)());
        let scale = if self.at_least_authored {
            scale.max(1.0)
        } else {
            scale
        };
        let height = constraints
            .clamp(Size::new(0.0, self.height * scale))
            .height;
        let size = self.child.measure(
            ctx,
            Constraints::new(
                Size::new(constraints.min.width, height),
                Size::new(constraints.max.width, height),
            ),
        );
        constraints.clamp(Size::new(size.width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.child.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

fn current(shared: &Shared, metric: DashboardMetric) -> (Option<f64>, usize, usize) {
    let cluster = shared.cluster();
    let latest = cluster.observation.history.back();
    let recent = latest.is_some_and(|point| {
        (cluster.observation_time() - point.at).whole_seconds() <= OBSERVATION_FRESH_SECONDS
    });
    let value = recent
        .then(|| latest.and_then(|point| point.values.get(&metric).copied().flatten()))
        .flatten();
    let coverage = if value.is_some() {
        latest
            .and_then(|point| point.coverage.get(&metric))
            .copied()
            .unwrap_or(0)
    } else {
        0
    };
    (value, coverage, cluster.observation.targets.len())
}

fn inspect_button(shared: &Shared, metric: DashboardMetric) -> IconButton {
    let action = shared.clone();
    let theme = shared.clone();
    IconButton::new(
        IconGlyph::ChevronRight,
        format!("Inspect {}", metric.title()),
    )
    .description(format!("Inspect {} by node and labels", metric.title()))
    .appearance(ButtonAppearance::Ghost)
    .tone(SemanticTone::Neutral)
    .theme_when(move || theme.theme_sized(sui::ControlSize::Small))
    .on_press_with_ctx(move |ctx| {
        if matches!(
            metric,
            DashboardMetric::ModelRequests
                | DashboardMetric::ModelInputTokens
                | DashboardMetric::ModelOutputTokens
                | DashboardMetric::ToolCalls
        ) {
            super::open_usage(
                &action,
                sinomo_client::usage_contract::UsageFilter {
                    event_kind: Some(
                        if metric == DashboardMetric::ToolCalls {
                            "agent.tool.call.completed"
                        } else {
                            "lm.request.completed"
                        }
                        .into(),
                    ),
                    ..Default::default()
                },
            );
            request_metric_explorer_relayout(ctx);
            return;
        }
        let spec = metric.spec();
        action.cluster_mut().inspect_explorer_metric(
            spec.explorer_metric,
            spec.explorer_transform,
            spec.explorer_labels
                .iter()
                .map(|(key, value)| ((*key).into(), (*value).into())),
        );
        request_observation(&action);
        request_metric_explorer_relayout(ctx);
    })
}

fn metric_panel(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
    metric: DashboardMetric,
) -> impl Widget + use<> {
    let title = TrailingSlotRow::new(
        text(
            reader,
            metric.title(),
            |theme| theme.text.sm,
            FontWeight::SEMIBOLD,
            |theme| theme.palette.text,
        ),
        inspect_button(shared, metric),
    )
    .trailing_width(30.0)
    .trailing_height(30.0)
    .gap(4.0);
    let value = live_label(
        shared,
        reader,
        samples,
        |theme| theme.text._3xl,
        FontWeight::SEMIBOLD,
        move |shared, theme| {
            let (value, _, _) = current(shared, metric);
            (
                format_dashboard_value(metric, value),
                if value.is_some() {
                    theme.semantic_tone_color(metric.tone(value))
                } else {
                    theme.palette.text_muted
                },
            )
        },
    );
    let info = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(3.0)
        .with_child(ScaledHeight::new(reader, 30.0, title))
        .with_child(ScaledHeight::new(reader, 30.0, value))
        .with_child(text(
            reader,
            metric.spec().description,
            |theme| theme.text.xs,
            FontWeight::NORMAL,
            |theme| theme.palette.text_muted,
        ));
    let coverage = live_label(
        shared,
        reader,
        samples,
        |theme| theme.text.xs,
        FontWeight::NORMAL,
        move |shared, theme| {
            let (_, reporting, targets) = current(shared, metric);
            let conflict =
                dashboard_metric_has_value_conflict(&shared.cluster().observation.metrics, metric);
            (
                if conflict {
                    "Conflicting metric types or units".into()
                } else {
                    format!(
                        "Last {} · {reporting}/{targets} nodes reporting",
                        metric.window().label()
                    )
                },
                theme.palette.text_muted,
            )
        },
    );
    ScaledHeight::new(
        reader,
        270.0,
        Surface::panel(
            Flex::vertical()
                .align_items(Alignment::Stretch)
                .with_child(ScaledHeight::new(reader, 98.0, info))
                .with_item(
                    DashboardChart::new(shared, reader, samples, metric),
                    FlexItem::fill(),
                )
                .with_child(ScaledHeight::new(reader, 20.0, coverage)),
        )
        .name(metric.title())
        .padding(Insets::all(12.0))
        .fill()
        .theme_when(clone_reader(reader)),
    )
    .at_least_authored()
}

/// A curated panel's plot. It stays mounted; each sample refreshes its series from the
/// dashboard history, and its tone follows the latest value.
struct DashboardChart {
    shared: Shared,
    samples: Samples,
    metric: DashboardMetric,
    chart: MetricChart,
}

impl DashboardChart {
    fn new(
        shared: &Shared,
        reader: &ThemeReader,
        samples: &Samples,
        metric: DashboardMetric,
    ) -> Self {
        let mut chart = MetricChart::new(
            metric.title().into(),
            Vec::new(),
            Rc::clone(reader),
            None,
            ExplorerTransform::Current,
            ObserveMetricKind::Gauge,
        );
        chart.dashboard_metric = Some(metric);
        chart.height = 132.0;
        Self {
            shared: shared.clone(),
            samples: samples.clone(),
            metric,
            chart,
        }
    }
}

impl Widget for DashboardChart {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        ctx.observe(&self.samples);
        let cluster = self.shared.cluster();
        let end_at = cluster.observation_time();
        let window = self.metric.window();
        self.chart
            .set_time_range(end_at, window, cluster.observation.paused_at.is_some());
        let current = cluster
            .observation
            .history
            .back()
            .and_then(|point| point.values.get(&self.metric).copied().flatten());
        self.chart.tone = Some(self.metric.tone(current));
        self.chart.series = vec![MetricSeries {
            key: MetricSeriesKey {
                metric_name: self.metric.title().into(),
                node_id: self.metric.spec().description.into(),
                labels: BTreeMap::new(),
            },
            points: cluster
                .observation
                .history
                .iter()
                .filter(|point| point.at >= end_at - window.duration())
                .map(|point| MetricPoint {
                    at: point.at,
                    value: point.values.get(&self.metric).copied().flatten(),
                })
                .collect(),
        }];
        self.chart.measure(ctx, constraints)
    }
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.chart.event(ctx, event);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.chart.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.chart.semantics(ctx);
    }
}

fn stat_tile(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
    title: &'static str,
    metric: Option<DashboardMetric>,
) -> impl Widget + use<> {
    let value = live_label(
        shared,
        reader,
        samples,
        |theme| theme.text._3xl,
        FontWeight::SEMIBOLD,
        move |shared, theme| {
            let (value, tone) = if let Some(metric) = metric {
                let (value, _, _) = current(shared, metric);
                (format_dashboard_value(metric, value), metric.tone(value))
            } else {
                let cluster = shared.cluster();
                let counts = availability_counts(&cluster.snapshot);
                (
                    if cluster.snapshot.nodes.is_empty() {
                        "—".into()
                    } else {
                        format!("{} / {}", counts.online, cluster.snapshot.nodes.len())
                    },
                    if counts.offline > 0 {
                        SemanticTone::Warning
                    } else {
                        SemanticTone::Accent
                    },
                )
            };
            (value, theme.semantic_tone_color(tone))
        },
    );
    let caption = if metric.is_some() {
        "Max reported node"
    } else {
        "Online / known nodes"
    };
    Surface::panel(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(3.0)
            .with_child(text(
                reader,
                title,
                |theme| theme.text.sm,
                FontWeight::MEDIUM,
                |theme| theme.palette.text_muted,
            ))
            .with_child(value)
            .with_child(text(
                reader,
                caption,
                |theme| theme.text.xs,
                FontWeight::NORMAL,
                |theme| theme.palette.text_muted,
            )),
    )
    .name(format!("Summary: {title}"))
    .padding(Insets::all(12.0))
    .fill_width(true)
    .theme_when(clone_reader(reader))
}

fn dashboard_header(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
) -> impl Widget + use<> {
    let status = live_label(
        shared,
        reader,
        samples,
        |theme| theme.text.sm,
        FontWeight::NORMAL,
        |shared, theme| {
            let cluster = shared.cluster();
            let now = cluster.observation_time();
            let targets = &cluster.observation.targets;
            let fresh = targets
                .values()
                .filter(|target| target.freshness(now) == "Fresh")
                .count();
            let partial = targets
                .values()
                .filter(|target| target.freshness(now) == "Partial")
                .count();
            let unavailable = targets
                .values()
                .filter(|target| target.error.is_some())
                .count();
            let stale = targets.len().saturating_sub(fresh + partial + unavailable);
            let summary = if targets.is_empty() {
                "Waiting for node metrics".into()
            } else {
                format!(
                    "{fresh} fresh · {partial} partial · {stale} stale · {unavailable} unavailable · {}",
                    if cluster.observation.paused_at.is_some() {
                        "Snapshot paused"
                    } else if cluster.observation.in_flight {
                        "Collecting…"
                    } else {
                        "Samples every 5s while visible"
                    }
                )
            };
            (
                summary,
                if partial + stale + unavailable > 0 {
                    theme.semantic_tone_color(SemanticTone::Warning)
                } else {
                    theme.palette.text_muted
                },
            )
        },
    );
    let live = live_toggle(shared);
    let refresh_shared = shared.clone();
    let theme = shared.clone();
    let refresh = IconButton::new(IconGlyph::Redo, "Refresh now")
        .appearance(ButtonAppearance::Ghost)
        .theme_when(move || theme.theme_sized(sui::ControlSize::Small))
        .enabled_from(collection_idle(shared))
        .on_press_with_ctx(move |ctx| {
            request_refresh(&refresh_shared);
            request_observation(&refresh_shared);
            request_metric_explorer_relayout(ctx);
        });
    Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(4.0)
        .with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(12.0)
                .align_items(Alignment::Center)
                .with_item(
                    text(
                        reader,
                        "System overview",
                        |theme| theme.text.lg,
                        FontWeight::SEMIBOLD,
                        |theme| theme.palette.text,
                    ),
                    FlexItem::flex(1.0).min_width(140.0),
                )
                .with_child(live)
                .with_child(refresh),
        )
        .with_child(status)
}

fn group_panels(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
    group: Group,
) -> impl Widget + use<> {
    let mut cards = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .align_items(Alignment::Start)
        .gap(12.0);
    for spec in DEFAULT_DASHBOARD
        .panels
        .iter()
        .filter(|panel| panel.group == group)
    {
        cards.push_item(
            metric_panel(shared, reader, samples, spec.metric),
            FlexItem::new()
                .grow(1.0)
                .basis_gap_aware_fraction(1.0 / 3.0)
                .min_width(250.0),
        );
    }
    Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_child(text(
            reader,
            group.title(),
            |theme| theme.text.base,
            FontWeight::SEMIBOLD,
            |theme| theme.palette.text,
        ))
        .with_child(text(
            reader,
            group.description(),
            |theme| theme.text.sm,
            FontWeight::NORMAL,
            |theme| theme.palette.text_muted,
        ))
        .with_child(cards)
}

/// One node's row in the diagnostics list.
#[derive(Clone, Debug, PartialEq)]
struct NodeStatusRow {
    node_id: String,
    label: String,
    value: String,
}

fn node_status_rows(cluster: &ClusterState) -> Vec<NodeStatusRow> {
    let now = cluster.observation_time();
    let mut targets = cluster
        .observation
        .targets
        .values()
        .cloned()
        .collect::<Vec<_>>();
    sort_target_summaries(&mut targets, cluster.snapshot.local_node_id.as_deref());
    targets
        .into_iter()
        .map(|target| {
            let node = cluster
                .snapshot
                .nodes
                .iter()
                .find(|node| node.node_id == target.node_id);
            let name = node
                .map(|node| node.display_name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| short(&target.node_id, 24));
            let value = if let Some(error) = &target.error {
                format!("Metrics unavailable: {error}")
            } else {
                format!(
                    "{} · RPC {} · p95 {} · KV queue {} · DFS repairs {}",
                    target.freshness(now),
                    format_optional(target.rpc_rate, "/s"),
                    format_optional(target.rpc_p95_ms, " ms"),
                    format_optional(target.kv_queue_depth, ""),
                    format_optional(target.dfs_backlog, "")
                )
            };
            NodeStatusRow {
                label: format!(
                    "{name} · {}",
                    node.map(|node| node.availability.as_str())
                        .unwrap_or("unknown")
                ),
                node_id: target.node_id,
                value,
            }
        })
        .collect()
}

/// Per-node evidence. Rows are keyed by node and update in place as samples arrive.
fn node_diagnostics(
    shared: &Shared,
    reader: &ThemeReader,
    samples: &Samples,
) -> impl Widget + use<> {
    let rows_shared = shared.clone();
    let rows = Projection::new(samples.clone(), move || {
        node_status_rows(&rows_shared.cluster())
    });
    let row_reader = Rc::clone(reader);
    let list = KeyedStack::vertical(
        rows.clone(),
        |row: &NodeStatusRow| row.node_id.clone(),
        move |_, row| {
            let initial = row.get();
            let label = row.clone();
            let value = row.clone();
            live(
                DetailRow::new(initial.label, initial.value)
                    .label_when(move || label.get().label)
                    .value_when(move || value.get().value)
                    .max_value_lines(3)
                    .theme_when(clone_reader(&row_reader)),
                row,
            )
        },
    )
    .gap(8.0);
    let body = SwitchView::new()
        .selected_from(rows.map(|rows| usize::from(rows.is_empty())))
        .with_child(list)
        .with_child(Label::new("No observation results yet").theme_when(clone_reader(reader)));
    PanelSection::new("Per-node status", body).theme_when(clone_reader(reader))
}

fn page(shared: &Shared, reader: &ThemeReader) -> Flex {
    let samples = samples(shared);
    let mut summary = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(10.0)
        .align_items(Alignment::Start);
    for (title, metric) in [
        ("Agent turns", Some(DashboardMetric::AgentActiveTurns)),
        ("Queued tasks", Some(DashboardMetric::AgentPendingTasks)),
        ("Task failures", Some(DashboardMetric::AgentFailedTasks)),
        ("DFS repairs", Some(DashboardMetric::DfsBacklog)),
        ("Nodes online", None),
    ] {
        summary.push_item(
            stat_tile(shared, reader, &samples, title, metric),
            FlexItem::new()
                .grow(1.0)
                .basis_gap_aware_fraction(1.0 / 6.0)
                .min_width(120.0),
        );
    }
    let usage_action = shared.clone();
    let usage_button = Button::new("Agent usage history")
        .theme_when(clone_reader(reader))
        .on_press(move || super::open_usage(&usage_action, Default::default()));
    let mut body = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(20.0)
        .with_child(dashboard_header(shared, reader, &samples))
        .with_child(summary)
        .with_child(usage_button);
    for group in Group::ALL
        .into_iter()
        .filter(|group| *group != Group::Diagnostics)
    {
        body.push(group_panels(shared, reader, &samples, group));
    }
    let open = cluster_select(shared, "dashboard diagnostics visibility", |key| {
        key.dashboard_diagnostics
    });
    let action = shared.clone();
    let toggle = widgets::Restyle::new(
        Button::new("Show diagnostics")
            .icon(IconGlyph::ChevronDown)
            .appearance(ButtonAppearance::Ghost)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                {
                    let mut cluster = action.cluster_mut();
                    cluster.observation.dashboard_diagnostics =
                        !cluster.observation.dashboard_diagnostics;
                    cluster.observation.revision = cluster.observation.revision.wrapping_add(1);
                }
                request_metric_explorer_relayout(ctx);
            }),
        open,
        |mut button, open| {
            button.set_label(if *open {
                "Hide diagnostics"
            } else {
                "Show diagnostics"
            });
            button.icon(if *open {
                IconGlyph::ChevronUp
            } else {
                IconGlyph::ChevronDown
            })
        },
    );
    body.push(toggle);
    let diagnostics = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(18.0)
        .with_child(group_panels(shared, reader, &samples, Group::Diagnostics))
        .with_child(node_diagnostics(shared, reader, &samples))
        .with_child(
            Label::new(concat!(
                "Snapshot gauges may describe replicated state, so these panels show reported maxima, ",
                "not unique cluster totals. History is bounded to this app session; ",
                "Explore exposes the underlying node and label series.",
            ))
            .theme_when(clone_reader(reader)),
        );
    body.push(
        SwitchView::new()
            .selected_from(cluster_select(
                shared,
                "dashboard diagnostics page",
                |key| usize::from(key.dashboard_diagnostics),
            ))
            .with_child(SizedBox::new().height(0.0))
            .with_child(diagnostics),
    );
    body
}

/// The overview page is built once; samples, toggles, and theme changes update it in place.
pub(super) fn view(shared: Shared) -> impl Widget {
    let reader = cluster_theme_reader(&shared);
    Surface::window(
        ScrollView::vertical(page(&shared, &reader))
            .state(ScrollState::new())
            .name("Cluster performance overview")
            .viewport_width_hint(true)
            .theme_when(clone_reader(&reader)),
    )
    .padding(Insets::all(16.0))
    .fill()
    .theme_when(clone_reader(&reader))
}

/// Pauses or resumes live collection. The button stays mounted while its label and tone
/// follow the live state.
pub(super) fn live_toggle(shared: &Shared) -> impl Widget + use<> {
    let action = shared.clone();
    let theme = shared.clone();
    widgets::Restyle::new(
        Button::new("Live · 5s")
            .semantic_name("Pause live metrics")
            .description("Automatic collection while the metrics view is visible")
            .appearance(ButtonAppearance::Tonal)
            .tone(SemanticTone::Success)
            .theme_when(move || theme.theme_sized(sui::ControlSize::Small))
            .on_press_with_ctx(move |ctx| {
                let live = action.cluster().observation.paused_at.is_none();
                action.cluster_mut().set_observation_live(!live);
                if !live {
                    request_observation(&action);
                }
                request_metric_explorer_relayout(ctx);
            }),
        cluster_select(shared, "dashboard live mode", |key| key.observation_live),
        |mut button, live| {
            button.set_label(if *live {
                "Live · 5s"
            } else {
                "Paused · Resume"
            });
            button
                .semantic_name(if *live {
                    "Pause live metrics"
                } else {
                    "Resume live metrics"
                })
                .tone(if *live {
                    SemanticTone::Success
                } else {
                    SemanticTone::Warning
                })
        },
    )
}
