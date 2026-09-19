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
    DfsBacklog,
    DfsConflicts,
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
            DashboardMetric::DfsConflicts,
            "DFS conflicts",
            "sinomo.dfs.open_conflicts",
            Group::Storage,
            Read::Gauge,
            ObserveMetricUnit::Count,
            ExplorerWindow::FifteenMinutes,
            "",
            "Open conflicts · highest node snapshot",
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
    pub const ALL: [Self; 21] = [
        Self::AgentActiveTurns,
        Self::AgentPendingTasks,
        Self::AgentFailedTasks,
        Self::DfsBacklog,
        Self::DfsConflicts,
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
            Self::DfsConflicts
            | Self::DfsFailedJobs
            | Self::ExporterErrors
            | Self::RpcErrorRate
                if value > 0.0 =>
            {
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

fn text(
    theme: &DefaultTheme,
    value: impl Into<String>,
    size: f32,
    weight: FontWeight,
    color: Color,
) -> Label {
    Label::new(value).style(
        Palette::from_theme(theme, theme.surfaces.dark).text_style(theme, size, weight, color),
    )
}

fn live_content<F: Fn(&Shared, DefaultTheme) -> WidgetPod + 'static>(
    shared: &Shared,
    build: F,
) -> impl Widget + use<F> {
    let source = shared.signals().cluster_structure.clone();
    let shared = shared.clone();
    RebuildOnChange::new_observable(source, move |_| build(&shared, shared.theme()))
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
    .size(28.0)
    .icon_size(14.0)
    .appearance(ButtonAppearance::Ghost)
    .tone(SemanticTone::Neutral)
    .theme_when(move || theme.theme().with_size(sui::ControlSize::Small))
    .on_press_with_ctx(move |ctx| {
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

fn metric_panel(shared: &Shared, metric: DashboardMetric) -> impl Widget + use<> {
    let theme = shared.theme();
    let scale = theme.text.base.size / DefaultTheme::default().text.base.size;
    let title = TrailingSlotRow::new(
        text(
            &theme,
            metric.title(),
            theme.text.sm.size,
            FontWeight::SEMIBOLD,
            theme.palette.text,
        ),
        inspect_button(shared, metric),
    )
    .trailing_width(30.0)
    .trailing_height(30.0)
    .gap(4.0);
    let value = live_content(shared, move |shared, theme| {
        let (value, _, _) = current(shared, metric);
        WidgetPod::new(text(
            &theme,
            format_dashboard_value(metric, value),
            theme.text.xl.size * 1.3,
            FontWeight::SEMIBOLD,
            if value.is_some() {
                theme.semantic_tone_color(metric.tone(value))
            } else {
                theme.palette.text_muted
            },
        ))
    });
    let info = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(3.0)
        .with_item(title, FlexItem::fixed(30.0 * scale))
        .with_item(value, FlexItem::fixed(30.0 * scale))
        .with_child(text(
            &theme,
            metric.spec().description,
            theme.text.xs.size,
            FontWeight::NORMAL,
            theme.palette.text_muted,
        ));
    let coverage = live_content(shared, move |shared, theme| {
        let (_, reporting, targets) = current(shared, metric);
        let conflict =
            dashboard_metric_has_value_conflict(&shared.cluster().observation.metrics, metric);
        WidgetPod::new(text(
            &theme,
            if conflict {
                "Conflicting metric types or units".into()
            } else {
                format!(
                    "Last {} · {reporting}/{targets} nodes reporting",
                    metric.window().label()
                )
            },
            theme.text.xs.size,
            FontWeight::NORMAL,
            theme.palette.text_muted,
        ))
    });
    SizedBox::new().height(270.0 * scale.max(1.0)).with_child(
        Surface::panel(
            Dock::new(DashboardChart::new(shared.clone(), metric))
                .top(98.0 * scale, info)
                .bottom(20.0 * scale, coverage),
        )
        .name(metric.title())
        .padding(Insets::all(12.0))
        .fill()
        .theme(theme),
    )
}

struct DashboardChart {
    shared: Shared,
    metric: DashboardMetric,
    chart: MetricChart,
}

impl DashboardChart {
    fn new(shared: Shared, metric: DashboardMetric) -> Self {
        let theme = shared.theme();
        let pal = Palette::from_theme(&theme, theme.surfaces.dark);
        let mut chart = MetricChart::new(
            metric.title().into(),
            Vec::new(),
            theme,
            pal.agent,
            None,
            ExplorerTransform::Current,
            ObserveMetricKind::Gauge,
        );
        chart.dashboard_metric = Some(metric);
        chart.height = 132.0;
        Self {
            shared,
            metric,
            chart,
        }
    }
}

impl Widget for DashboardChart {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        ctx.observe(&self.shared.signals().cluster_structure);
        let cluster = self.shared.cluster();
        self.chart.end_at = cluster.observation_time();
        self.chart.window = self.metric.window();
        self.chart.paused = cluster.observation.paused_at.is_some();
        let current = cluster
            .observation
            .history
            .back()
            .and_then(|point| point.values.get(&self.metric).copied().flatten());
        self.chart.colors[0] = self
            .shared
            .theme()
            .semantic_tone_color(self.metric.tone(current));
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
                .filter(|point| point.at >= self.chart.end_at - self.chart.window.duration())
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
    title: &'static str,
    metric: Option<DashboardMetric>,
) -> impl Widget + use<> {
    live_content(shared, move |shared, theme| {
        let (value, caption, tone) = if let Some(metric) = metric {
            let (value, _, _) = current(shared, metric);
            (
                format_dashboard_value(metric, value),
                "Max reported node".to_string(),
                metric.tone(value),
            )
        } else {
            let cluster = shared.cluster();
            let counts = availability_counts(&cluster.snapshot);
            (
                if cluster.snapshot.nodes.is_empty() {
                    "—".into()
                } else {
                    format!("{} / {}", counts.online, cluster.snapshot.nodes.len())
                },
                "Online / known nodes".into(),
                if counts.offline > 0 {
                    SemanticTone::Warning
                } else {
                    SemanticTone::Accent
                },
            )
        };
        WidgetPod::new(
            Surface::panel(
                Flex::vertical()
                    .align_items(Alignment::Stretch)
                    .gap(3.0)
                    .with_child(text(
                        &theme,
                        title,
                        theme.text.sm.size,
                        FontWeight::MEDIUM,
                        theme.palette.text_muted,
                    ))
                    .with_child(text(
                        &theme,
                        value,
                        theme.text.xl.size * 1.4,
                        FontWeight::SEMIBOLD,
                        theme.semantic_tone_color(tone),
                    ))
                    .with_child(text(
                        &theme,
                        caption,
                        theme.text.xs.size,
                        FontWeight::NORMAL,
                        theme.palette.text_muted,
                    )),
            )
            .name(format!("Summary: {title}"))
            .padding(Insets::all(12.0))
            .fill_width()
            .theme(theme),
        )
    })
}

fn dashboard_header(shared: &Shared) -> impl Widget + use<> {
    let theme = shared.theme();
    let status = live_content(shared, |shared, theme| {
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
        WidgetPod::new(text(
            &theme,
            summary,
            theme.text.sm.size,
            FontWeight::NORMAL,
            if partial + stale + unavailable > 0 {
                theme.semantic_tone_color(SemanticTone::Warning)
            } else {
                theme.palette.text_muted
            },
        ))
    });
    let live = live_toggle(shared);
    let refresh_shared = shared.clone();
    let enabled = shared.clone();
    let refresh = IconButton::new(IconGlyph::Redo, "Refresh now")
        .size(30.0)
        .icon_size(16.0)
        .appearance(ButtonAppearance::Ghost)
        .theme(theme.with_size(sui::ControlSize::Small))
        .enabled_when(move || {
            let cluster = enabled.cluster();
            cluster.observation.paused_at.is_none() && !cluster.observation.in_flight
        })
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
                        &theme,
                        "System overview",
                        theme.text.lg.size,
                        FontWeight::SEMIBOLD,
                        theme.palette.text,
                    ),
                    FlexItem::flex(1.0).min_width(140.0),
                )
                .with_child(live)
                .with_child(refresh),
        )
        .with_child(status)
}

fn group_panels(shared: &Shared, group: Group) -> impl Widget + use<> {
    let theme = shared.theme();
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
            metric_panel(shared, spec.metric),
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
            &theme,
            group.title(),
            theme.text.base.size,
            FontWeight::SEMIBOLD,
            theme.palette.text,
        ))
        .with_child(text(
            &theme,
            group.description(),
            theme.text.sm.size,
            FontWeight::NORMAL,
            theme.palette.text_muted,
        ))
        .with_child(cards)
}

fn node_diagnostics(shared: &Shared) -> impl Widget + use<> {
    live_content(shared, |shared, theme| {
        let cluster = shared.cluster();
        let now = cluster.observation_time();
        let mut targets = cluster
            .observation
            .targets
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_target_summaries(&mut targets, cluster.snapshot.local_node_id.as_deref());
        let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
        for target in targets {
            let node = cluster
                .snapshot
                .nodes
                .iter()
                .find(|node| node.node_id == target.node_id);
            let name = node
                .map(|node| node.display_name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| short(&target.node_id, 24));
            let detail = if let Some(error) = &target.error {
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
            rows.push(
                DetailRow::new(
                    format!(
                        "{name} · {}",
                        node.map(|node| node.availability.as_str())
                            .unwrap_or("unknown")
                    ),
                    detail,
                )
                .max_value_lines(3)
                .theme(theme),
            );
        }
        if cluster.observation.targets.is_empty() {
            rows.push(Label::new("No observation results yet").theme(theme));
        }
        WidgetPod::new(PanelSection::new("Per-node status", rows).theme(theme))
    })
}

fn page(shared: &Shared) -> WidgetPod {
    let mut summary = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(10.0)
        .align_items(Alignment::Start);
    for (title, metric) in [
        ("Agent turns", Some(DashboardMetric::AgentActiveTurns)),
        ("Queued tasks", Some(DashboardMetric::AgentPendingTasks)),
        ("Task failures", Some(DashboardMetric::AgentFailedTasks)),
        ("DFS repairs", Some(DashboardMetric::DfsBacklog)),
        ("DFS conflicts", Some(DashboardMetric::DfsConflicts)),
        ("Nodes online", None),
    ] {
        summary.push_item(
            stat_tile(shared, title, metric),
            FlexItem::new()
                .grow(1.0)
                .basis_gap_aware_fraction(1.0 / 6.0)
                .min_width(120.0),
        );
    }
    let mut body = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(20.0)
        .with_child(dashboard_header(shared))
        .with_child(summary);
    for group in Group::ALL
        .into_iter()
        .filter(|group| *group != Group::Diagnostics)
    {
        body.push(group_panels(shared, group));
    }
    let source = shared
        .signals()
        .cluster_structure
        .select_named("dashboard diagnostics visibility", |key| {
            key.dashboard_diagnostics
        });
    let action = shared.clone();
    let toggle = RebuildOnChange::new_observable(source.clone(), move |open| {
        let action = action.clone();
        WidgetPod::new(
            Button::new(if *open {
                "Hide diagnostics"
            } else {
                "Show diagnostics"
            })
            .icon(if *open {
                IconGlyph::ChevronUp
            } else {
                IconGlyph::ChevronDown
            })
            .appearance(ButtonAppearance::Ghost)
            .theme(action.theme())
            .on_press_with_ctx(move |ctx| {
                {
                    let mut cluster = action.cluster_mut();
                    cluster.observation.dashboard_diagnostics =
                        !cluster.observation.dashboard_diagnostics;
                    cluster.observation.revision = cluster.observation.revision.wrapping_add(1);
                }
                request_metric_explorer_relayout(ctx);
            }),
        )
    });
    body.push(toggle);
    let diagnostics = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(18.0)
        .with_child(group_panels(shared, Group::Diagnostics))
        .with_child(node_diagnostics(shared))
        .with_child(
            Label::new(concat!(
                "Snapshot gauges may describe replicated state, so these panels show reported maxima, ",
                "not unique cluster totals. History is bounded to this app session; ",
                "Explore exposes the underlying node and label series.",
            ))
            .theme(shared.theme()),
        );
    let selected = shared
        .signals()
        .cluster_structure
        .select_named("dashboard diagnostics page", |key| {
            usize::from(key.dashboard_diagnostics)
        });
    body.push(
        SwitchView::new()
            .selected_from(selected)
            .with_child(SizedBox::new().height(0.0))
            .with_child(diagnostics),
    );
    WidgetPod::new(body)
}

pub(super) fn view(shared: Shared) -> impl Widget {
    let theme = shared.signals().theme.clone();
    let content_shared = shared.clone();
    let content = RebuildOnChange::new_observable(theme, move |_| page(&content_shared));
    let scroll = ScrollState::new();
    Surface::window(
        ScrollView::vertical(content)
            .state(scroll)
            .name("Cluster performance overview")
            .viewport_width_hint(true)
            .theme_when({
                let shared = shared.clone();
                move || shared.theme()
            }),
    )
    .padding(Insets::all(16.0))
    .fill()
    .theme_when(move || shared.theme())
}

pub(super) fn live_toggle(shared: &Shared) -> impl Widget + use<> {
    let source = shared
        .signals()
        .cluster_structure
        .select_named("dashboard live mode", |key| key.observation_live);
    let action = shared.clone();
    RebuildOnChange::new_observable(source, move |live| {
        let change = action.clone();
        let live = *live;
        WidgetPod::new(
            Button::new(if live {
                "Live · 5s"
            } else {
                "Paused · Resume"
            })
            .semantic_name(if live {
                "Pause live metrics"
            } else {
                "Resume live metrics"
            })
            .description("Automatic collection while the metrics view is visible")
            .appearance(ButtonAppearance::Tonal)
            .tone(if live {
                SemanticTone::Success
            } else {
                SemanticTone::Warning
            })
            .theme(action.theme().with_size(sui::ControlSize::Small))
            .on_press_with_ctx(move |ctx| {
                change.cluster_mut().set_observation_live(!live);
                if !live {
                    request_observation(&change);
                }
                request_metric_explorer_relayout(ctx);
            }),
        )
    })
}
