//! Cluster dashboard, metric explorer, topology, and observation activity.

use sinomo_runtime::observe::{ObserveMetricKind, ObserveMetricUnit, ObserveMetricsResponse};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use time::OffsetDateTime;

use sui::{
    Alignment, ArrangeCtx, Button, ButtonAppearance, Color, Command, ConstraintQuery,
    ConstraintView, Constraints, DefaultTheme, DetailRow, Dock, EmptyState, Event, EventCtx, Flex,
    FlexItem, FlexWrap, FontWeight, IconGlyph, Insets, KeyState, Label, MasterDetail,
    MasterDetailState, MeasureCtx, PaintCtx, PanelSection, Path, PathBuilder, Point, PointerButton,
    PointerEventKind, RebuildOnChange, Rect, ScrollView, SegmentedControl, Select, SemanticTone,
    SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole, Size, SizedBox, SplitState,
    StatusBadge, StrokeStyle, Surface, SurfaceAppearance, SurfaceBorder, SurfaceElevation,
    SwitchView, TabBar, TextInput, TrailingSlotRow, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor,
};

use crate::backend::Backend;
use crate::backend::cluster::{ClusterLinkInfo, ClusterNodeInfo, ClusterSnapshot};
use crate::backend::observe::ObserveSampleBatch;
use crate::metrics::{
    MetricCatalogEntry, MetricCatalogSummary, MetricQueryResult, MetricSelection, MetricSeries,
    MetricStore, MetricTransform, metric_process_identity_matches, strict_metrics_interval_delta,
};
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::offline::{DaemonOfflinePanel, daemon_gate};
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

const HEADER_H: f32 = 78.0;
const PAD: f32 = 22.0;
const NODE_R: f32 = 22.0;
const LAN_R: f32 = 18.0;
const PANEL_W: f32 = 360.0;
const PANEL_PAD: f32 = 16.0;
const DETAIL_PANEL_H: f32 = 620.0;
const DETAIL_BUTTON_H: f32 = 30.0;
const LEGEND_H: f32 = 32.0;
const VERTEX_TEXT_TOP_GAP: f32 = 6.0;
const VERTEX_TEXT_LINE_GAP: f32 = 2.0;
const CLUSTER_SECTION_H: f32 = 42.0;
const OBSERVATION_HISTORY_LIMIT: usize = 180;
const OBSERVATION_ACTIVITY_LIMIT: usize = 100;
const OBSERVATION_ACTIVITY_DETAIL_LIMIT: usize = 180;
const OBSERVATION_FRESH_SECONDS: i64 = 10;
const EXPLORER_CATALOG_CONTROLS_H: f32 = 104.0;
const EXPLORER_CHART_H: f32 = 280.0;
const EXPLORER_CATALOG_RESULT_LIMIT: usize = 100;
const EXPLORER_CHART_SERIES_LIMIT: usize = 6;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClusterSubview {
    #[default]
    Overview,
    Explore,
    Topology,
    Activity,
    Logs,
}

impl ClusterSubview {
    const ALL: [Self; 5] = [
        Self::Overview,
        Self::Explore,
        Self::Topology,
        Self::Activity,
        Self::Logs,
    ];

    fn index(self) -> usize {
        match self {
            Self::Overview => 0,
            Self::Explore => 1,
            Self::Topology => 2,
            Self::Activity => 3,
            Self::Logs => 4,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Explore => "Explore",
            Self::Topology => "Topology",
            Self::Activity => "Activity",
            Self::Logs => "Logs",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ExplorerTransform {
    #[default]
    Auto,
    Current,
    Rate,
    Delta,
    P50,
    P95,
    P99,
}

impl ExplorerTransform {
    fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Current => "Value",
            Self::Rate => "Rate / second",
            Self::Delta => "Interval delta",
            Self::P50 => "p50",
            Self::P95 => "p95",
            Self::P99 => "p99",
        }
    }

    fn available(kind: sinomo_runtime::observe::ObserveMetricKind) -> &'static [Self] {
        use sinomo_runtime::observe::ObserveMetricKind;

        const COUNTER: &[ExplorerTransform] = &[
            ExplorerTransform::Auto,
            ExplorerTransform::Rate,
            ExplorerTransform::Delta,
            ExplorerTransform::Current,
        ];
        const GAUGE: &[ExplorerTransform] = &[ExplorerTransform::Auto, ExplorerTransform::Current];
        const HISTOGRAM: &[ExplorerTransform] = &[
            ExplorerTransform::Auto,
            ExplorerTransform::P50,
            ExplorerTransform::P95,
            ExplorerTransform::P99,
        ];
        match kind {
            ObserveMetricKind::Counter => COUNTER,
            ObserveMetricKind::Gauge => GAUGE,
            ObserveMetricKind::Histogram => HISTOGRAM,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ExplorerKindFilter {
    #[default]
    All,
    Counter,
    Gauge,
    Histogram,
}

impl ExplorerKindFilter {
    const ALL: [Self; 4] = [Self::All, Self::Counter, Self::Gauge, Self::Histogram];

    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Counter => "Count",
            Self::Gauge => "Gauge",
            Self::Histogram => "Hist",
        }
    }

    fn includes(self, kind: sinomo_runtime::observe::ObserveMetricKind) -> bool {
        use sinomo_runtime::observe::ObserveMetricKind;

        matches!(
            (self, kind),
            (Self::All, _)
                | (Self::Counter, ObserveMetricKind::Counter)
                | (Self::Gauge, ObserveMetricKind::Gauge)
                | (Self::Histogram, ObserveMetricKind::Histogram)
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ExplorerWindow {
    OneMinute,
    FiveMinutes,
    #[default]
    FifteenMinutes,
}

impl ExplorerWindow {
    fn label(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
        }
    }

    fn sample_limit(self) -> usize {
        match self {
            Self::OneMinute => 12,
            Self::FiveMinutes => 60,
            Self::FifteenMinutes => OBSERVATION_HISTORY_LIMIT,
        }
    }

    fn duration(self) -> time::Duration {
        match self {
            Self::OneMinute => time::Duration::minutes(1),
            Self::FiveMinutes => time::Duration::minutes(5),
            Self::FifteenMinutes => time::Duration::minutes(15),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct MetricExplorerState {
    search: String,
    kind_filter: ExplorerKindFilter,
    selected_metric: Option<String>,
    selected_node: Option<String>,
    label_filters: BTreeMap<String, String>,
    transform: ExplorerTransform,
    window: ExplorerWindow,
}

impl Default for MetricExplorerState {
    fn default() -> Self {
        Self {
            search: String::new(),
            kind_filter: ExplorerKindFilter::All,
            selected_metric: Some("sinomo.daemon.service.requests".to_string()),
            selected_node: None,
            label_filters: BTreeMap::new(),
            transform: ExplorerTransform::Auto,
            window: ExplorerWindow::FifteenMinutes,
        }
    }
}

mod dashboard;
use dashboard::{
    DashboardMetric, dashboard_metric_has_value_conflict, dashboard_values, format_dashboard_value,
};

#[derive(Clone, Debug, PartialEq)]
struct DashboardPoint {
    at: OffsetDateTime,
    values: BTreeMap<DashboardMetric, Option<f64>>,
    coverage: BTreeMap<DashboardMetric, usize>,
}

#[derive(Clone, Debug, PartialEq)]
struct TargetObservationSummary {
    node_id: String,
    observed_at: Option<OffsetDateTime>,
    received_at: OffsetDateTime,
    partial: bool,
    error: Option<String>,
    rpc_rate: Option<f64>,
    rpc_p95_ms: Option<f64>,
    kv_queue_depth: Option<f64>,
    dfs_backlog: Option<f64>,
    failed_tasks: Option<f64>,
}

impl TargetObservationSummary {
    fn freshness(&self, now: OffsetDateTime) -> &'static str {
        if self.error.is_some() {
            "Offline"
        } else if (now - self.observed_at.unwrap_or(self.received_at)).whole_seconds()
            > OBSERVATION_FRESH_SECONDS
        {
            "Stale"
        } else if self.partial {
            "Partial"
        } else {
            "Fresh"
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ObservationActivity {
    at: OffsetDateTime,
    node_id: String,
    detail: String,
    warning: bool,
}

#[derive(Default)]
struct ClusterObservationState {
    paused_at: Option<OffsetDateTime>,
    timeline_at: Option<OffsetDateTime>,
    dashboard_diagnostics: bool,
    generation: u64,
    in_flight: bool,
    sample_rx: Option<Receiver<ObserveSampleBatch>>,
    previous: BTreeMap<
        String,
        (
            sinomo_runtime::observe::ObserveMetricsResponse,
            OffsetDateTime,
        ),
    >,
    targets: BTreeMap<String, TargetObservationSummary>,
    history: VecDeque<DashboardPoint>,
    metrics: MetricStore,
    activity: VecDeque<ObservationActivity>,
    revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterSelection {
    Node(String),
    LanSegment(String),
}

pub struct ClusterState {
    pub snapshot: ClusterSnapshot,
    pub subview: ClusterSubview,
    pub selected: Option<ClusterSelection>,
    pub diagnostics_open: bool,
    pub refreshing: bool,
    refresh_rx: Option<Receiver<ClusterSnapshot>>,
    last_topology_refresh: Option<std::time::Instant>,
    observation: ClusterObservationState,
    explorer: MetricExplorerState,
    explorer_navigation: MasterDetailState,
    logs: logs::LogState,
}

/// Cloneable UI projection shared by the Cluster tab's structural selectors.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterStructureKey {
    theme: DefaultTheme,
    snapshot: ClusterSnapshot,
    subview: ClusterSubview,
    selected: Option<ClusterSelection>,
    diagnostics_open: bool,
    refreshing: bool,
    observation_revision: u64,
    observation_live: bool,
    dashboard_diagnostics: bool,
    explorer: MetricExplorerState,
    explorer_entry: Option<Arc<MetricCatalogEntry>>,
    catalog: Arc<[MetricCatalogSummary]>,
    catalog_truncated: bool,
    metrics_revision: u64,
    observation_time: Option<OffsetDateTime>,
    logs: logs::LogKey,
    /// No node has reported a metric target yet, so the overview has nothing to chart.
    observation_empty: bool,
}

impl ClusterStructureKey {
    pub(crate) fn new(theme: DefaultTheme, cluster: Option<&ClusterState>) -> Self {
        let Some(cluster) = cluster else {
            return Self {
                theme,
                snapshot: ClusterSnapshot::default(),
                subview: ClusterSubview::Overview,
                selected: None,
                diagnostics_open: false,
                refreshing: false,
                observation_revision: 0,
                observation_live: true,
                dashboard_diagnostics: false,
                explorer: MetricExplorerState::default(),
                explorer_entry: None,
                catalog: Arc::from([]),
                catalog_truncated: false,
                metrics_revision: 0,
                observation_time: None,
                logs: logs::LogKey::default(),
                observation_empty: true,
            };
        };
        Self {
            theme,
            snapshot: cluster.snapshot.clone(),
            logs: cluster.logs.key.clone(),
            subview: cluster.subview,
            selected: cluster.selected.clone(),
            diagnostics_open: cluster.diagnostics_open,
            refreshing: cluster.refreshing,
            observation_revision: cluster.observation.revision,
            observation_live: cluster.observation.paused_at.is_none(),
            dashboard_diagnostics: cluster.observation.dashboard_diagnostics,
            explorer: cluster.explorer.clone(),
            explorer_entry: cluster
                .explorer
                .selected_metric
                .as_deref()
                .and_then(|name| cluster.observation.metrics.catalog_entry(name).cloned())
                .map(Arc::new),
            catalog: cluster.observation.metrics.catalog_summary(),
            catalog_truncated: cluster.observation.metrics.catalog_truncated(),
            metrics_revision: cluster.observation.metrics.revision(),
            observation_time: cluster
                .observation
                .paused_at
                .or(cluster.observation.timeline_at),
            observation_empty: cluster.observation.targets.is_empty(),
        }
    }

    /// The overview has neither nodes nor metrics to show.
    fn overview_empty(&self) -> bool {
        self.snapshot.nodes.is_empty() && self.observation_empty
    }
}

impl ClusterState {
    fn observation_time(&self) -> OffsetDateTime {
        self.observation
            .paused_at
            .or(self.observation.timeline_at)
            .or_else(|| self.observation.history.back().map(|point| point.at))
            .unwrap_or_else(OffsetDateTime::now_utc)
    }

    fn set_observation_live(&mut self, live: bool) {
        if live == self.observation.paused_at.is_none() {
            return;
        }
        if live {
            let now = OffsetDateTime::now_utc();
            self.observation.paused_at = None;
            self.observation.timeline_at = Some(now);
            self.observation.previous.clear();
            self.observation.metrics.reset_interval_baselines(now);
            self.observation.history.push_back(DashboardPoint {
                at: now,
                values: BTreeMap::new(),
                coverage: BTreeMap::new(),
            });
            while self.observation.history.len() > OBSERVATION_HISTORY_LIMIT {
                self.observation.history.pop_front();
            }
        } else {
            self.observation.paused_at = Some(self.observation_time());
            // An already-running batch must stay invalid even if resumed before it returns.
            self.observation.generation = self.observation.generation.wrapping_add(1);
        }
        self.observation.revision = self.observation.revision.wrapping_add(1);
    }

    pub fn new(backend: &Backend) -> Self {
        let mut state = Self {
            snapshot: backend.cluster_startup_snapshot(),
            subview: ClusterSubview::Overview,
            selected: None,
            diagnostics_open: false,
            refreshing: false,
            refresh_rx: None,
            last_topology_refresh: None,
            observation: ClusterObservationState::default(),
            explorer: MetricExplorerState::default(),
            explorer_navigation: MasterDetailState::default(),
            logs: logs::LogState::default(),
        };
        state.sync_explorer_history_scope();
        state
    }

    pub fn begin_refresh(&mut self, backend: Backend, waker: Option<sui::UiHandle>) -> bool {
        if self.refreshing {
            return false;
        }
        self.last_topology_refresh = Some(std::time::Instant::now());
        let (tx, rx) = mpsc::channel();
        match thread::Builder::new()
            .name("sinomo-app-cluster-refresh".to_string())
            .spawn(move || {
                let snapshot = backend.refresh_cluster_snapshot();
                let _ = tx.send(snapshot);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.refreshing = true;
                self.refresh_rx = Some(rx);
                true
            }
            Err(error) => {
                self.refreshing = false;
                self.refresh_rx = None;
                self.snapshot
                    .warnings
                    .push(format!("cluster refresh failed: {error}"));
                true
            }
        }
    }

    fn touch_explorer(&mut self) {
        self.observation.revision = self.observation.revision.wrapping_add(1);
    }

    fn set_explorer_search(&mut self, search: String) {
        if self.explorer.search != search {
            self.explorer.search = search;
            self.touch_explorer();
        }
    }

    fn set_explorer_kind_filter(&mut self, kind_filter: ExplorerKindFilter) {
        if self.explorer.kind_filter != kind_filter {
            self.explorer.kind_filter = kind_filter;
            self.touch_explorer();
        }
    }

    fn select_explorer_metric(&mut self, name: String) {
        if self.explorer.selected_metric.as_deref() != Some(name.as_str()) {
            self.explorer.selected_metric = Some(name);
            self.explorer.label_filters.clear();
            self.explorer.transform = ExplorerTransform::Auto;
            self.touch_explorer();
        }
        self.sync_explorer_history_scope();
        self.explorer_navigation.show_detail();
    }

    fn inspect_explorer_metric(
        &mut self,
        name: impl Into<String>,
        transform: ExplorerTransform,
        labels: impl IntoIterator<Item = (String, String)>,
    ) {
        self.explorer.selected_metric = Some(name.into());
        self.explorer.selected_node = None;
        self.explorer.label_filters = labels.into_iter().collect();
        self.explorer.transform = transform;
        self.subview = ClusterSubview::Explore;
        self.sync_explorer_history_scope();
        self.explorer_navigation.show_detail();
        self.touch_explorer();
    }

    pub(crate) fn navigate_back(&mut self) -> bool {
        if self.subview == ClusterSubview::Explore
            && self.explorer_navigation.route() == sui::MasterDetailRoute::Detail
        {
            self.explorer_navigation.show_master();
            self.touch_explorer();
            true
        } else {
            false
        }
    }

    fn set_explorer_node(&mut self, node_id: Option<String>) {
        if self.explorer.selected_node != node_id {
            self.explorer.selected_node = node_id;
            self.sync_explorer_history_scope();
            self.touch_explorer();
        }
    }

    fn set_explorer_label(&mut self, key: String, value: Option<String>) {
        let changed = match value {
            Some(value) => {
                let changed = self.explorer.label_filters.get(&key) != Some(&value);
                if changed {
                    self.explorer.label_filters.insert(key, value);
                }
                changed
            }
            None => self.explorer.label_filters.remove(&key).is_some(),
        };
        if changed {
            self.sync_explorer_history_scope();
            self.touch_explorer();
        }
    }

    fn set_explorer_transform(&mut self, transform: ExplorerTransform) {
        if self.explorer.transform != transform {
            self.explorer.transform = transform;
            self.touch_explorer();
        }
    }

    fn sync_explorer_history_scope(&mut self) {
        self.observation.metrics.set_history_scope(
            self.explorer.selected_metric.clone(),
            self.explorer.selected_node.clone(),
            self.explorer.label_filters.clone(),
        );
    }

    /// Drop any result tied to the previous daemon identity and immediately refresh through the
    /// newly-bound backend. The last good topology remains visible while the replacement snapshot
    /// is loading.
    pub(crate) fn rebind_backend(
        &mut self,
        backend: Backend,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        self.refresh_rx = None;
        self.refreshing = false;
        self.observation.generation = self.observation.generation.wrapping_add(1);
        self.observation.sample_rx = None;
        self.observation.in_flight = false;
        self.observation.previous.clear();
        self.observation.history.clear();
        self.observation.targets.clear();
        self.observation.timeline_at = None;
        self.observation.metrics = MetricStore::default();
        self.explorer = MetricExplorerState::default();
        self.sync_explorer_history_scope();
        self.explorer_navigation.show_master();
        self.observation.revision = self.observation.revision.wrapping_add(1);
        self.begin_refresh(backend, waker)
    }

    pub fn pump_refresh(&mut self) -> bool {
        let snapshot = match self.refresh_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(snapshot)) => snapshot,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.refresh_rx = None;
                self.refreshing = false;
                self.snapshot
                    .warnings
                    .push("cluster refresh worker stopped".to_string());
                return true;
            }
        };
        self.refresh_rx = None;
        self.refreshing = false;
        if !selection_exists(&snapshot, self.selected.as_ref()) {
            self.selected = None;
            self.diagnostics_open = false;
        }
        self.snapshot = snapshot;
        true
    }

    fn begin_observation(&mut self, backend: Backend, waker: Option<sui::UiHandle>) -> bool {
        if self.observation.paused_at.is_some() {
            return false;
        }
        self.observation.timeline_at = Some(OffsetDateTime::now_utc());
        self.observation.revision = self.observation.revision.wrapping_add(1);
        if self.observation.in_flight {
            return false;
        }
        let generation = self.observation.generation;
        let (tx, rx) = mpsc::channel();
        match thread::Builder::new()
            .name("sinomo-app-cluster-observe".to_string())
            .spawn(move || {
                let batch = backend.observe_cluster_metrics(generation);
                let _ = tx.send(batch);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.observation.in_flight = true;
                self.observation.sample_rx = Some(rx);
                self.observation.revision = self.observation.revision.wrapping_add(1);
                true
            }
            Err(error) => {
                self.observation.in_flight = false;
                self.observation.sample_rx = None;
                push_observation_activity(
                    &mut self.observation,
                    OffsetDateTime::now_utc(),
                    "local",
                    format!("observation worker failed to start: {error}"),
                    true,
                );
                self.observation.revision = self.observation.revision.wrapping_add(1);
                true
            }
        }
    }

    fn pump_observation(&mut self) -> bool {
        let batch = match self.observation.sample_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(batch)) => batch,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.observation.sample_rx = None;
                self.observation.in_flight = false;
                push_observation_activity(
                    &mut self.observation,
                    OffsetDateTime::now_utc(),
                    "local",
                    "observation worker stopped before returning a sample".to_string(),
                    true,
                );
                self.observation.revision = self.observation.revision.wrapping_add(1);
                return true;
            }
        };
        self.observation.sample_rx = None;
        self.observation.in_flight = false;
        if batch.generation != self.observation.generation || self.observation.paused_at.is_some() {
            self.observation.revision = self.observation.revision.wrapping_add(1);
            return false;
        }
        apply_observation_batch(&mut self.observation, batch);
        self.observation.revision = self.observation.revision.wrapping_add(1);
        true
    }
}

pub fn request_refresh(shared: &Shared) -> bool {
    let (backend, waker) = {
        let app = shared.borrow();
        let Some(ready) = app.ready.as_ref() else {
            return false;
        };
        (ready.backend.clone(), app.waker.clone())
    };
    shared.cluster_mut().begin_refresh(backend, waker)
}

pub fn pump_refresh(shared: &Shared) -> bool {
    if !shared.is_ready() {
        return false;
    }
    shared.cluster_mut().pump_refresh()
}

pub fn request_observation(shared: &Shared) -> bool {
    if shared.is_ready() && shared.cluster().subview == ClusterSubview::Logs {
        return logs::request(shared, false);
    }
    let (backend, waker) = {
        let app = shared.borrow();
        let Some(ready) = app.ready.as_ref() else {
            return false;
        };
        (ready.backend.clone(), app.waker.clone())
    };
    let mut cluster = shared.cluster_mut();
    if cluster.observation.paused_at.is_none()
        && cluster
            .last_topology_refresh
            .is_none_or(|at| at.elapsed().as_secs() >= 30)
    {
        cluster.begin_refresh(backend.clone(), waker.clone());
    }
    cluster.begin_observation(backend, waker)
}

pub fn pump_observation(shared: &Shared) -> bool {
    if !shared.is_ready() {
        return false;
    }
    let metrics = shared.cluster_mut().pump_observation();
    let logs = logs::pump(shared);
    metrics || logs
}

pub fn observation_visible(shared: &Shared) -> bool {
    if logs::live(shared) {
        return true;
    }
    shared.is_ready()
        && shared.active_tab() == Tab::Cluster
        && shared.cluster().observation.paused_at.is_none()
        && matches!(
            shared.cluster().subview,
            ClusterSubview::Overview | ClusterSubview::Explore
        )
}

fn apply_observation_batch(state: &mut ClusterObservationState, batch: ObserveSampleBatch) {
    if state
        .history
        .back()
        .is_some_and(|point| (batch.received_at - point.at).whole_seconds() > 20)
    {
        state.previous.clear();
        state
            .metrics
            .reset_interval_baselines(batch.received_at - time::Duration::seconds(1));
    }
    state.timeline_at = Some(batch.received_at);
    state.metrics.ingest_batch(
        batch.received_at,
        batch.targets.iter().filter_map(|target| {
            target
                .response
                .as_ref()
                .ok()
                .map(|response| (target.node_id.as_str(), response))
        }),
    );
    let sampled_nodes = batch
        .targets
        .iter()
        .map(|target| target.node_id.clone())
        .collect::<BTreeSet<_>>();
    state
        .targets
        .retain(|node_id, _| sampled_nodes.contains(node_id));
    state
        .previous
        .retain(|node_id, _| sampled_nodes.contains(node_id));
    let mut aggregate: BTreeMap<DashboardMetric, Option<f64>> = DashboardMetric::ALL
        .into_iter()
        .map(|metric| (metric, None))
        .collect();
    let mut coverage = BTreeMap::<DashboardMetric, usize>::new();

    for target in batch.targets {
        let node_id = target.node_id;
        let response = match target.response {
            Ok(response) => dashboard::dashboard_snapshot(response),
            Err(error) => {
                state.previous.remove(&node_id);
                let error = short(&error, 240);
                let observed_at = state
                    .targets
                    .get(&node_id)
                    .and_then(|target| target.observed_at);
                state.targets.insert(
                    node_id.clone(),
                    TargetObservationSummary {
                        node_id: node_id.clone(),
                        observed_at,
                        received_at: batch.received_at,
                        partial: false,
                        error: Some(error.clone()),
                        rpc_rate: None,
                        rpc_p95_ms: None,
                        kv_queue_depth: None,
                        dfs_backlog: None,
                        failed_tasks: None,
                    },
                );
                push_observation_activity(
                    state,
                    batch.received_at,
                    &node_id,
                    format!("metrics unavailable: {}", short(&error, 180)),
                    true,
                );
                continue;
            }
        };

        let observed_at = response.observed_at.unwrap_or(batch.received_at);
        let previous = state.previous.get(&node_id);
        let process_restarted = previous.is_some_and(|(previous, _)| {
            matches!(
                (previous.process_started_at, response.process_started_at),
                (Some(previous), Some(current)) if previous != current
            )
        });
        let non_forward_same_process = previous.is_some_and(|(previous, received_at)| {
            metric_process_identity_matches(
                previous.process_started_at,
                response.process_started_at,
            ) && observed_at <= previous.observed_at.unwrap_or(*received_at)
        });
        let interval = previous.and_then(|(previous, previous_received_at)| {
            strict_metrics_interval_delta(
                previous,
                *previous_received_at,
                &response,
                batch.received_at,
            )
        });
        let elapsed_seconds = interval.as_ref().map(|(_, elapsed)| *elapsed);
        let interval = interval.map(|(interval, _)| interval);
        let values = dashboard_values(&response, interval.as_ref(), elapsed_seconds);
        if process_restarted {
            push_observation_activity(
                state,
                batch.received_at,
                &node_id,
                "process restarted; interval baseline reset".to_string(),
                false,
            );
        }
        for (metric, value) in &values {
            let Some(value) = value else {
                continue;
            };
            let slot = aggregate.entry(*metric).or_insert(None);
            *coverage.entry(*metric).or_default() += 1;
            *slot = Some(match (*slot, metric) {
                (Some(current), metric) => metric.aggregate(current, *value),
                (None, _) => *value,
            });
        }
        let partial = response.partial;
        if partial {
            let failed = response
                .exporters
                .iter()
                .filter(|exporter| !exporter.success)
                .map(|exporter| exporter.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            push_observation_activity(
                state,
                batch.received_at,
                &node_id,
                if failed.is_empty() {
                    "metric snapshot was partial".to_string()
                } else {
                    format!("metric snapshot was partial; failed exporters: {failed}")
                },
                true,
            );
        }
        state.targets.insert(
            node_id.clone(),
            TargetObservationSummary {
                node_id: node_id.clone(),
                observed_at: Some(observed_at),
                received_at: batch.received_at,
                partial,
                error: None,
                rpc_rate: values[&DashboardMetric::RpcRate],
                rpc_p95_ms: values[&DashboardMetric::RpcP95Ms],
                kv_queue_depth: metric_value_sum(
                    &response,
                    "sinomo.kv.commit.queue_depth",
                    ObserveMetricKind::Gauge,
                    ObserveMetricUnit::Count,
                    |_| true,
                ),
                dfs_backlog: values[&DashboardMetric::DfsBacklog],
                failed_tasks: metric_value_sum(
                    &response,
                    "sinomo.agent.tasks",
                    ObserveMetricKind::Gauge,
                    ObserveMetricUnit::Count,
                    |record| {
                        matches!(
                            record.labels.get("status").map(String::as_str),
                            Some("failed" | "timed_out")
                        )
                    },
                ),
            },
        );
        if !non_forward_same_process {
            state
                .previous
                .insert(node_id, (response, batch.received_at));
        }
    }

    for metric in DashboardMetric::ALL {
        if dashboard_metric_has_value_conflict(&state.metrics, metric) {
            aggregate.insert(metric, None);
        }
    }

    state.history.push_back(DashboardPoint {
        at: batch.received_at,
        values: aggregate,
        coverage,
    });
    while state.history.len() > OBSERVATION_HISTORY_LIMIT {
        state.history.pop_front();
    }
}

fn metric_value_sum(
    response: &ObserveMetricsResponse,
    name: &str,
    kind: ObserveMetricKind,
    unit: ObserveMetricUnit,
    include: impl Fn(&sinomo_runtime::observe::ObserveMetricRecord) -> bool,
) -> Option<f64> {
    metric_family_compatible(response, name, kind, unit)?;
    response
        .metrics
        .iter()
        .filter(|record| record.name == name && record.kind == kind && include(record))
        .filter_map(|record| record.value)
        .filter(|value| value.is_finite())
        .reduce(|left, right| left + right)
}

fn metric_histogram_quantile_max(
    response: &ObserveMetricsResponse,
    name: &str,
    unit: ObserveMetricUnit,
    quantile: f64,
    include: impl Fn(&sinomo_runtime::observe::ObserveMetricRecord) -> bool,
) -> Option<f64> {
    metric_family_compatible(response, name, ObserveMetricKind::Histogram, unit)?;
    response
        .metrics
        .iter()
        .filter(|record| {
            record.name == name && record.kind == ObserveMetricKind::Histogram && include(record)
        })
        .filter_map(|record| record.histogram_quantile(quantile))
        .filter(|value| value.is_finite())
        .reduce(f64::max)
}

fn metric_family_compatible(
    response: &ObserveMetricsResponse,
    name: &str,
    kind: ObserveMetricKind,
    unit: ObserveMetricUnit,
) -> Option<()> {
    response
        .descriptors
        .iter()
        .filter(|descriptor| descriptor.name == name)
        .all(|descriptor| {
            descriptor.kind == kind && descriptor.unit.is_none_or(|actual| actual == unit)
        })
        .then_some(())
}

fn push_observation_activity(
    state: &mut ClusterObservationState,
    at: OffsetDateTime,
    node_id: impl Into<String>,
    detail: String,
    warning: bool,
) {
    let node_id = node_id.into();
    let detail = short(&detail, OBSERVATION_ACTIVITY_DETAIL_LIMIT);
    if state
        .activity
        .back()
        .is_some_and(|entry| entry.node_id == node_id && entry.detail == detail)
    {
        return;
    }
    state.activity.push_back(ObservationActivity {
        at,
        node_id,
        detail,
        warning,
    });
    while state.activity.len() > OBSERVATION_ACTIVITY_LIMIT {
        state.activity.pop_front();
    }
}

#[derive(Clone, Debug)]
struct Topology {
    vertices: Vec<GraphVertex>,
    edges: Vec<GraphEdge>,
    lan_segments: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct GraphVertex {
    id: String,
    label: String,
    detail: String,
    kind: GraphVertexKind,
    availability: String,
    public_reachable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphVertexKind {
    Node,
    LanSegment,
}

fn graph_vertex_selection(vertex: &GraphVertex) -> ClusterSelection {
    match vertex.kind {
        GraphVertexKind::Node => ClusterSelection::Node(vertex.id.clone()),
        GraphVertexKind::LanSegment => ClusterSelection::LanSegment(vertex.id.clone()),
    }
}

#[derive(Clone, Debug)]
struct GraphEdge {
    a: String,
    b: String,
    state: String,
    public_ip: bool,
    virtual_lan: bool,
}

#[derive(Clone, Debug)]
struct EdgeAccumulator {
    state: String,
    public_ip: bool,
    has_lan: bool,
}

impl EdgeAccumulator {
    fn from_link(link: &ClusterLinkInfo) -> Self {
        Self {
            state: link.state.clone(),
            public_ip: link.public_ip,
            has_lan: link_is_lan(link),
        }
    }

    fn merge(&mut self, link: &ClusterLinkInfo) {
        self.state = best_state(&self.state, &link.state).to_string();
        self.public_ip |= link.public_ip;
        self.has_lan |= link_is_lan(link);
    }
}

#[derive(Clone)]
struct LayoutVertex {
    vertex: GraphVertex,
    center: Point,
    radius: f32,
}

pub fn view(shared: Shared) -> impl Widget {
    let reader = cluster_theme_reader(&shared);
    let section_source = shared
        .signals()
        .cluster_structure
        .select_named("cluster section selection", |key| key.subview.index());
    let tab_section_source = shared
        .signals()
        .cluster_structure
        .select_named("cluster tab selection", |key| Some(key.subview.index()));
    let body = SwitchView::new()
        .selected_from(section_source.clone())
        .with_child(cluster_overview_host(&shared, &reader))
        .with_child(cluster_explorer_view(&shared, &reader))
        .with_child(ClusterView::new(shared.clone()))
        .with_child(cluster_activity_host(&shared, &reader))
        .with_child(logs::view(&shared, &reader));
    let sections = build_cluster_section_bar(&shared, &reader, tab_section_source);
    let cluster = Dock::new(Dock::new(body).top(CLUSTER_SECTION_H, sections))
        .top(HEADER_H, cluster_header_host(&shared, &reader));
    let offline =
        DaemonOfflinePanel::new(&shared, &reader, "Cluster offline", CLUSTER_OFFLINE_SUMMARY)
            .build();
    daemon_gate(&shared, cluster, offline)
}

/// What the cluster tab needs the daemon for, shown on the offline panel.
pub(crate) const CLUSTER_OFFLINE_SUMMARY: &str =
    "Cluster membership, health, metrics, and logs come from the daemon.";

fn build_cluster_section_bar(
    shared: &Shared,
    reader: &ThemeReader,
    selected: impl sui::Observable<Option<usize>> + 'static,
) -> Surface {
    let change_shared = shared.clone();
    let tabs = TabBar::new("cluster-sections")
        .tabs(ClusterSubview::ALL.into_iter().map(ClusterSubview::title))
        .gap(0.0)
        .selected_from(selected)
        .theme_when(clone_reader(reader))
        .on_change(move |index, _label| {
            let Some(subview) = ClusterSubview::ALL.get(index).copied() else {
                return;
            };
            if change_shared.cluster().subview == subview {
                return;
            }
            change_shared.cluster_mut().subview = subview;
            if subview == ClusterSubview::Logs {
                logs::request(&change_shared, true);
                request_refresh(&change_shared);
            } else if matches!(subview, ClusterSubview::Overview | ClusterSubview::Explore) {
                request_observation(&change_shared);
            }
        });
    Surface::window(tabs)
        .border(SurfaceBorder::Bottom)
        .padding(Insets {
            left: PAD,
            right: PAD,
            top: 5.0,
            bottom: 5.0,
        })
        .fill()
        .theme_when(clone_reader(reader))
}

fn cluster_explorer_view(shared: &Shared, reader: &ThemeReader) -> MasterDetail {
    let navigation = shared.cluster().explorer_navigation.clone();
    MasterDetail::new(
        cluster_metric_catalog(shared, reader),
        cluster_metric_detail_host(shared, reader),
    )
    .state(navigation)
    .split_state(SplitState::pixels(360.0))
}

fn cluster_metric_catalog(shared: &Shared, reader: &ThemeReader) -> Surface {
    let search = {
        let search_shared = shared.clone();
        let input = TextInput::new("Search registered metrics")
            .value(shared.cluster().explorer.search.clone())
            .placeholder("Search registered metrics")
            .leading_icon(IconGlyph::Search)
            .theme_when(clone_reader(reader))
            .on_change_with_ctx(move |ctx, value| {
                search_shared.cluster_mut().set_explorer_search(value);
                request_metric_explorer_relayout(ctx);
            });
        let value_shared = shared.clone();
        SyncedMetricSearch::new(input, move || {
            value_shared.cluster().explorer.search.clone()
        })
    };
    let selected_shared = shared.clone();
    let kind_shared = shared.clone();
    let kind_filter = SegmentedControl::new("Metric kind")
        .segments(
            ExplorerKindFilter::ALL
                .into_iter()
                .map(ExplorerKindFilter::label),
        )
        .selected_when(move || {
            ExplorerKindFilter::ALL
                .iter()
                .position(|kind| *kind == selected_shared.cluster().explorer.kind_filter)
        })
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _label| {
            if let Some(kind) = ExplorerKindFilter::ALL.get(index).copied() {
                kind_shared.cluster_mut().set_explorer_kind_filter(kind);
                request_metric_explorer_relayout(ctx);
            }
        });
    let controls = Surface::window(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(8.0)
            .with_child(search)
            .with_child(kind_filter),
    )
    .border(SurfaceBorder::Bottom)
    .padding(Insets::all(12.0))
    .fill()
    .theme_when(clone_reader(reader));
    Surface::window(
        Dock::new(cluster_metric_catalog_host(shared, reader))
            .top(EXPLORER_CATALOG_CONTROLS_H, controls),
    )
    .border(SurfaceBorder::Right)
    .fill()
    .theme_when(clone_reader(reader))
}

mod catalog;
mod logs;

fn cluster_metric_catalog_host(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    catalog::view(shared, reader)
}

#[derive(Clone, Debug, PartialEq)]
struct MetricDetailStructureKey {
    theme: DefaultTheme,
    entry: Option<Arc<MetricCatalogEntry>>,
    node: Option<String>,
    labels: BTreeMap<String, String>,
    transform: ExplorerTransform,
}

fn cluster_metric_detail_host(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared
        .signals()
        .cluster_structure
        .select_named("metric detail controls", |key| MetricDetailStructureKey {
            theme: key.theme,
            entry: key.explorer_entry.clone(),
            node: key.explorer.selected_node.clone(),
            labels: key.explorer.label_filters.clone(),
            transform: key.explorer.transform,
        });
    let shared = shared.clone();
    let reader = reader.clone();
    RebuildOnChange::key_from(source, move |_| {
        build_cluster_metric_detail(&shared, &reader)
    })
}

fn build_cluster_metric_detail(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (entry, mut explorer) = {
        let cluster = shared.cluster();
        (
            cluster
                .explorer
                .selected_metric
                .as_deref()
                .and_then(|name| cluster.observation.metrics.catalog_entry(name).cloned()),
            cluster.explorer.clone(),
        )
    };
    let Some(entry) = entry else {
        return WidgetPod::new(
            EmptyState::new(
                "Select a metric",
                "Search the registered catalog to inspect its live history",
            )
            .name("Metric explorer detail")
            .transparent()
            .theme_when(clone_reader(reader)),
        );
    };
    explorer.window = dashboard::metric_window(&entry.descriptor.name, entry.descriptor.kind);
    let title = ConstraintView::new(build_metric_detail_header(
        shared, reader, &entry, false, true,
    ))
    .when(
        ConstraintQuery::new().min_width(600.0),
        build_metric_detail_header(shared, reader, &entry, false, false),
    );
    let controls = build_metric_query_controls(shared, reader, &entry, &explorer);
    let source = shared
        .signals()
        .cluster_structure
        .select_named("metric live samples", |key| {
            (
                key.metrics_revision,
                key.observation_time,
                key.observation_live,
            )
        });
    let live_shared = shared.clone();
    let live_reader = reader.clone();
    let hover = Rc::new(Cell::new(None));
    let live = RebuildOnChange::key_from(source, move |_| {
        build_metric_live_details(&live_shared, &live_reader, hover.clone())
    });
    WidgetPod::new(
        Surface::window(
            ScrollView::vertical(
                Flex::vertical()
                    .align_items(Alignment::Stretch)
                    .gap(14.0)
                    .with_child(title)
                    .with_child(controls)
                    .with_child(live),
            )
            .name("Metric explorer detail")
            .viewport_width_hint(true)
            .theme_when(clone_reader(reader)),
        )
        .padding(Insets::all(PAD))
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn build_metric_live_details(
    shared: &Shared,
    reader: &ThemeReader,
    hover: Rc<Cell<Option<f32>>>,
) -> WidgetPod {
    let (entry, explorer, query, _in_flight) = {
        let cluster = shared.cluster();
        let mut explorer = cluster.explorer.clone();
        let entry = explorer
            .selected_metric
            .as_deref()
            .and_then(|name| cluster.observation.metrics.catalog_entry(name).cloned());
        if let Some(entry) = &entry {
            explorer.window =
                dashboard::metric_window(&entry.descriptor.name, entry.descriptor.kind);
        }
        let query = entry.as_ref().map(|entry| {
            let transform = metric_query_transform(explorer.transform, entry.descriptor.kind);
            cluster.observation.metrics.query_window(
                &MetricSelection {
                    metric_name: entry.descriptor.name.clone(),
                    node_id: explorer.selected_node.clone(),
                    labels: explorer.label_filters.clone(),
                    transform,
                },
                explorer.window.sample_limit(),
                Some(cluster.observation_time() - explorer.window.duration()),
            )
        });
        (entry, explorer, query, cluster.observation.in_flight)
    };
    let Some(entry) = entry else {
        return WidgetPod::new(
            EmptyState::new(
                "Select a metric",
                "Search the registered catalog to inspect current values and short live history",
            )
            .name("Metric explorer detail")
            .icon(IconGlyph::Search)
            .transparent()
            .theme_when(clone_reader(reader)),
        );
    };
    let query = query.expect("catalog entry builds a query");
    let effective_transform =
        if ExplorerTransform::available(entry.descriptor.kind).contains(&explorer.transform) {
            explorer.transform
        } else {
            ExplorerTransform::Auto
        };

    let mut badges = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .align_items(Alignment::Center)
        .gap(6.0)
        .with_child(
            StatusBadge::new(metric_kind_label(entry.descriptor.kind))
                .tone(SemanticTone::Neutral)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new(metric_unit_label(entry.descriptor.unit))
                .tone(SemanticTone::Neutral)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new(format!("{} series", entry.current_series))
                .tone(if entry.current_series > EXPLORER_CHART_SERIES_LIMIT {
                    SemanticTone::Warning
                } else {
                    SemanticTone::Neutral
                })
                .theme_when(clone_reader(reader)),
        );
    if entry.descriptor_conflict {
        badges.push(
            StatusBadge::new("Descriptor conflict")
                .tone(SemanticTone::Danger)
                .theme_when(clone_reader(reader)),
        );
    }
    if entry.label_keys_truncated || !entry.label_values_truncated.is_empty() {
        badges.push(
            StatusBadge::new("Dimensions capped")
                .tone(SemanticTone::Warning)
                .theme_when(clone_reader(reader)),
        );
    }

    let current = build_metric_current_summary(
        shared,
        reader,
        &entry,
        effective_transform,
        &explorer,
        &query,
    );
    let chart = build_metric_chart_panel(shared, reader, &entry, &explorer, &query, hover);
    let series = build_metric_series_table(reader, &entry, effective_transform, &query);
    WidgetPod::new(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(14.0)
            .with_child(badges)
            .with_child(current)
            .with_child(chart)
            .with_child(series),
    )
}

fn build_metric_detail_header(
    shared: &Shared,
    reader: &ThemeReader,
    entry: &MetricCatalogEntry,
    _in_flight: bool,
    compact: bool,
) -> impl Widget + use<> {
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let back_navigation = shared.cluster().explorer_navigation.clone();
    let back = Button::new("All metrics")
        .icon(IconGlyph::ChevronLeft)
        .appearance(ButtonAppearance::Ghost)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            back_navigation.show_master();
            request_metric_explorer_relayout(ctx);
        });
    let refresh_shared = shared.clone();
    let refresh_enabled = shared.clone();
    let refresh = Button::new("Refresh")
        .icon(IconGlyph::Redo)
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Accent)
        .enabled_when(move || {
            !refresh_enabled.cluster().observation.in_flight
                && refresh_enabled.cluster().observation.paused_at.is_none()
        })
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request_observation(&refresh_shared);
            request_metric_explorer_relayout(ctx);
        });
    let title = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(3.0)
        .with_child(
            Label::new(entry.descriptor.name.clone()).text_style(pal.text_style(
                &theme,
                if compact {
                    theme.text.base.size
                } else {
                    theme.text.lg.size
                },
                FontWeight::SEMIBOLD,
                pal.text,
            )),
        )
        .with_child(
            Label::new(entry.descriptor.help.clone()).text_style(pal.text_style(
                &theme,
                theme.text.sm.size,
                FontWeight::NORMAL,
                pal.text_muted,
            )),
        );
    let mut actions = Flex::horizontal().wrap(FlexWrap::Wrap).gap(6.0);
    if compact {
        actions = actions.with_child(back);
    }
    actions = actions
        .with_child(dashboard::live_toggle(shared))
        .with_child(refresh);
    Surface::window(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(8.0)
            .with_child(title)
            .with_child(actions),
    )
    .theme_when(clone_reader(reader))
}

fn build_metric_current_summary(
    shared: &Shared,
    reader: &ThemeReader,
    entry: &MetricCatalogEntry,
    transform: ExplorerTransform,
    explorer: &MetricExplorerState,
    query: &MetricQueryResult,
) -> Surface {
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let current_values = query
        .series
        .iter()
        .filter_map(MetricSeries::latest)
        .collect::<Vec<_>>();
    let min = current_values.iter().copied().reduce(f64::min);
    let max = current_values.iter().copied().reduce(f64::max);
    let value = if query.value_conflict {
        "Unavailable".to_string()
    } else if current_values.len() == 1 {
        format_metric_value(
            current_values.first().copied(),
            entry.descriptor.unit,
            transform,
            entry.descriptor.kind,
        )
    } else if let (Some(min), Some(max)) = (min, max) {
        if (max - min).abs() <= f64::EPSILON {
            format!(
                "{} across {} series",
                format_metric_value(
                    Some(max),
                    entry.descriptor.unit,
                    transform,
                    entry.descriptor.kind,
                ),
                current_values.len()
            )
        } else {
            format!(
                "{} – {}",
                format_metric_value(
                    Some(min),
                    entry.descriptor.unit,
                    transform,
                    entry.descriptor.kind,
                ),
                format_metric_value(
                    Some(max),
                    entry.descriptor.unit,
                    transform,
                    entry.descriptor.kind,
                )
            )
        }
    } else {
        "Awaiting a current sample".to_string()
    };
    let (reporting, total, partial, unavailable) = {
        let cluster = shared.cluster();
        let targets = explorer
            .selected_node
            .as_ref()
            .map(|node| vec![node.clone()])
            .unwrap_or_else(|| {
                let mut nodes = cluster
                    .observation
                    .targets
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                if nodes.is_empty() {
                    nodes.extend(entry.node_ids.iter().cloned());
                }
                nodes
            });
        let reporting = targets
            .iter()
            .filter(|node| entry.node_ids.contains(*node))
            .count();
        let partial = targets
            .iter()
            .filter(|node| {
                cluster
                    .observation
                    .targets
                    .get(*node)
                    .is_some_and(|target| target.partial && target.error.is_none())
            })
            .count();
        let unavailable = targets.len().saturating_sub(reporting);
        (reporting, targets.len(), partial, unavailable)
    };
    let updated = query
        .newest_at()
        .map(format_metric_sample_age)
        .unwrap_or_else(|| "No matching sample yet".to_string());
    let mut coverage = format!("{updated} · {reporting}/{total} nodes reporting");
    if partial > 0 {
        coverage.push_str(&format!(" · {partial} partial"));
    }
    if unavailable > 0 {
        coverage.push_str(&format!(" · {unavailable} unavailable"));
    }
    Surface::panel(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(2.0)
            .with_child(Label::new(value).text_style(pal.text_style(
                &theme,
                theme.text.lg.size,
                FontWeight::SEMIBOLD,
                pal.text,
            )))
            .with_child(Label::new(coverage).text_style(pal.text_style(
                &theme,
                theme.text.sm.size,
                FontWeight::NORMAL,
                pal.text_muted,
            ))),
    )
    .name("Metric current values")
    .appearance(SurfaceAppearance::Soft)
    .padding(Insets {
        left: 12.0,
        right: 12.0,
        top: 10.0,
        bottom: 10.0,
    })
    .fill_width(true)
    .theme_when(clone_reader(reader))
}

fn format_metric_sample_age(at: OffsetDateTime) -> String {
    let seconds = (OffsetDateTime::now_utc() - at).whole_seconds().max(0);
    if seconds < 5 {
        "Updated now".to_string()
    } else if seconds < 60 {
        format!("Updated {seconds}s ago")
    } else if seconds < 3_600 {
        format!("Updated {}m ago", seconds / 60)
    } else {
        format!("Updated {}h ago", seconds / 3_600)
    }
}

fn format_metric_span(span: time::Duration) -> String {
    let seconds = span.whole_seconds().max(0);
    if seconds < 1 {
        "one instant".to_string()
    } else if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m", seconds / 3_600, (seconds % 3_600) / 60)
    }
}

fn build_metric_query_controls(
    shared: &Shared,
    reader: &ThemeReader,
    entry: &MetricCatalogEntry,
    explorer: &MetricExplorerState,
) -> Flex {
    let mut nodes = std::iter::once("All nodes".to_string())
        .chain(entry.node_ids.iter().cloned())
        .collect::<Vec<_>>();
    if let Some(selected) = explorer.selected_node.as_ref()
        && !nodes.contains(selected)
    {
        nodes.push(selected.clone());
    }
    let selected_node = explorer
        .selected_node
        .as_ref()
        .and_then(|node| nodes.iter().position(|candidate| candidate == node))
        .unwrap_or(0);
    let node_shared = shared.clone();
    let node_options = nodes.clone();
    let node = Select::new("Metric node scope")
        .options(nodes)
        .selected(selected_node)
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _label| {
            let node_id = if index == 0 {
                None
            } else {
                node_options.get(index).cloned()
            };
            node_shared.cluster_mut().set_explorer_node(node_id);
            request_metric_explorer_relayout(ctx);
        });

    let transforms = ExplorerTransform::available(entry.descriptor.kind);
    let selected_transform = transforms
        .iter()
        .position(|transform| *transform == explorer.transform)
        .unwrap_or(0);
    let transform_shared = shared.clone();
    let transform_options = transforms.to_vec();
    let transform = Select::new("Metric transform")
        .options(transforms.iter().map(|transform| transform.label()))
        .selected(selected_transform)
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _label| {
            if let Some(transform) = transform_options.get(index).copied() {
                transform_shared
                    .cluster_mut()
                    .set_explorer_transform(transform);
                request_metric_explorer_relayout(ctx);
            }
        });

    let window = StatusBadge::new(format!("Fixed · {}", explorer.window.label()))
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader));

    let mut controls = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .align_items(Alignment::Start)
        .gap(8.0);
    controls.push_item(node, metric_query_control_item());
    controls.push_item(transform, metric_query_control_item());
    controls.push_item(window, metric_query_control_item());
    let mut filter_keys = entry.label_keys.clone();
    filter_keys.extend(explorer.label_filters.keys().cloned());
    for key in &filter_keys {
        let values = entry.label_values.get(key);
        if values.is_none_or(BTreeSet::is_empty) && !explorer.label_filters.contains_key(key) {
            continue;
        }
        let mut options = std::iter::once(format!("All {key}"))
            .chain(values.into_iter().flat_map(|values| values.iter().cloned()))
            .collect::<Vec<_>>();
        if let Some(selected) = explorer.label_filters.get(key)
            && !options.contains(selected)
        {
            options.push(selected.clone());
        }
        let selected = explorer
            .label_filters
            .get(key)
            .and_then(|value| options.iter().position(|candidate| candidate == value))
            .unwrap_or(0);
        let filter_shared = shared.clone();
        let filter_key = key.clone();
        let filter_options = options.clone();
        let select = Select::new(format!("Metric label {key}"))
            .options(options)
            .selected(selected)
            .theme_when(clone_reader(reader))
            .on_change_with_ctx(move |ctx, index, _label| {
                let value = if index == 0 {
                    None
                } else {
                    filter_options.get(index).cloned()
                };
                filter_shared
                    .cluster_mut()
                    .set_explorer_label(filter_key.clone(), value);
                request_metric_explorer_relayout(ctx);
            });
        controls.push_item(select, metric_query_control_item());
    }
    controls
}

fn metric_query_control_item() -> FlexItem {
    FlexItem::new()
        .grow(1.0)
        .basis_gap_aware_fraction(0.25)
        .min_width(180.0)
        .max_width(360.0)
}

fn sort_metric_series(series: &mut [MetricSeries]) {
    // Keep legend positions and colors attached to the same series as values change.
    series.sort_by(|left, right| left.key.cmp(&right.key));
}

fn build_metric_chart_panel(
    shared: &Shared,
    reader: &ThemeReader,
    entry: &MetricCatalogEntry,
    explorer: &MetricExplorerState,
    query: &MetricQueryResult,
    hover: Rc<Cell<Option<f32>>>,
) -> Surface {
    let theme = reader();
    let pal = Palette::from_theme(&theme, theme.surfaces.dark);
    let mut chart_series = query.series.clone();
    sort_metric_series(&mut chart_series);
    chart_series.truncate(EXPLORER_CHART_SERIES_LIMIT);
    let sample_count = query
        .series
        .iter()
        .map(|series| series.points.len())
        .max()
        .unwrap_or_default();
    let observed_span = query
        .oldest_at()
        .zip(query.newest_at())
        .map(|(oldest, newest)| format_metric_span(newest - oldest))
        .unwrap_or_else(|| "no span yet".to_string());
    let subtitle = format!(
        "{} · {} · {} window · {} retained sample{} over {}",
        explorer_transform_summary(explorer.transform, entry.descriptor.kind),
        metric_unit_label(entry.descriptor.unit),
        explorer.window.label(),
        sample_count,
        if sample_count == 1 { "" } else { "s" },
        observed_span,
    );
    let mut body = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(6.0)
        .with_child(Label::new("Live history").text_style(pal.text_style(
            &theme,
            theme.text.base.size,
            FontWeight::SEMIBOLD,
            pal.text,
        )))
        .with_child(Label::new(subtitle).text_style(pal.text_style(
            &theme,
            theme.text.sm.size,
            FontWeight::NORMAL,
            pal.text_muted,
        )));
    if !chart_series.is_empty() {
        let mut legend = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .align_items(Alignment::Center)
            .gap(8.0);
        for (index, series) in chart_series.iter().enumerate() {
            let label = format!(
                "S{} {} · {}",
                index + 1,
                short(&series.key.display_name(), 30),
                format_metric_value(
                    series.latest(),
                    entry.descriptor.unit,
                    explorer.transform,
                    entry.descriptor.kind,
                )
            );
            legend.push(Label::new(label).text_style(pal.text_style(
                &theme,
                theme.text.xs.size,
                FontWeight::MEDIUM,
                pal.agent[index % pal.agent.len()],
            )));
        }
        body.push(legend);
    }
    if query.value_conflict {
        body.push(
            SizedBox::new().height(EXPLORER_CHART_H).child(
                EmptyState::new(
                    "Conflicting metric descriptors",
                    "Nodes disagree on this metric's kind or unit, so Sinomo will not combine or transform its series",
                )
                .name("Metric chart descriptor conflict")
                .icon(IconGlyph::Alert)
                .transparent()
                .theme_when(clone_reader(reader)),
            ),
        );
    } else if chart_series.is_empty() {
        body.push(
            SizedBox::new().height(EXPLORER_CHART_H).child(
                EmptyState::new(
                    "Collecting live history",
                    "The current value appears after a matching sample; rates and quantiles need two process-stable samples",
                )
                .name("Metric chart empty state")
                .icon(IconGlyph::Redo)
                .transparent()
                .theme_when(clone_reader(reader)),
            ),
        );
    } else {
        body.push(
            SizedBox::new().height(EXPLORER_CHART_H).child(
                MetricChart::new(
                    entry.descriptor.name.clone(),
                    chart_series,
                    theme,
                    pal.agent,
                    entry.descriptor.unit,
                    explorer.transform,
                    entry.descriptor.kind,
                )
                .hover_state(hover)
                .time_range(
                    shared.cluster().observation_time(),
                    explorer.window,
                    shared.cluster().observation.paused_at.is_some(),
                ),
            ),
        );
    }
    if !query.value_conflict
        && (query.history_truncated || query.matching_series > EXPLORER_CHART_SERIES_LIMIT)
    {
        body.push(
            StatusBadge::new(format!(
                "Showing up to {} of {} matching series; filter by node or labels for a precise comparison",
                EXPLORER_CHART_SERIES_LIMIT,
                query.matching_series.max(query.series.len())
            ))
            .tone(SemanticTone::Warning)
            .theme_when(clone_reader(reader)),
        );
    }
    Surface::panel(body)
        .appearance(SurfaceAppearance::Raised)
        .elevation(SurfaceElevation::Small)
        .padding(Insets::all(14.0))
        .fill_width(true)
        .theme_when(clone_reader(reader))
}

fn build_metric_series_table(
    reader: &ThemeReader,
    entry: &MetricCatalogEntry,
    transform: ExplorerTransform,
    query: &MetricQueryResult,
) -> PanelSection {
    let theme = reader();
    let pal = Palette::from_theme(&theme, theme.surfaces.dark);
    let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(6.0);
    if query.value_conflict {
        rows.push(
            DetailRow::new(
                "Series",
                "Unavailable until every sampled node reports the same metric kind and unit",
            )
            .theme_when(clone_reader(reader)),
        );
    } else if query.series.is_empty() {
        rows.push(
            DetailRow::new("Series", "No matching retained series yet")
                .theme_when(clone_reader(reader)),
        );
    } else {
        let mut series = query.series.clone();
        sort_metric_series(&mut series);
        for (index, series) in series.iter().take(16).enumerate() {
            let values = series.finite_values().collect::<Vec<_>>();
            let latest = series.latest();
            let min = values.iter().copied().reduce(f64::min);
            let max = values.iter().copied().reduce(f64::max);
            let identity = if index < EXPLORER_CHART_SERIES_LIMIT {
                format!("S{} · {}", index + 1, series.key.display_name())
            } else {
                series.key.display_name()
            };
            let identity_color = if index < EXPLORER_CHART_SERIES_LIMIT {
                pal.agent[index % pal.agent.len()]
            } else {
                pal.text
            };
            let value = format!(
                "now {} · min {} · max {} · {} sample{}",
                format_metric_value(
                    latest,
                    entry.descriptor.unit,
                    transform,
                    entry.descriptor.kind,
                ),
                format_metric_value(min, entry.descriptor.unit, transform, entry.descriptor.kind,),
                format_metric_value(max, entry.descriptor.unit, transform, entry.descriptor.kind,),
                values.len(),
                if values.len() == 1 { "" } else { "s" }
            );
            rows.push(
                Surface::window(
                    Flex::vertical()
                        .align_items(Alignment::Stretch)
                        .gap(2.0)
                        .with_child(Label::new(identity).text_style(pal.text_style(
                            &theme,
                            theme.text.sm.size,
                            FontWeight::SEMIBOLD,
                            identity_color,
                        )))
                        .with_child(Label::new(value).text_style(pal.text_style(
                            &theme,
                            theme.text.sm.size,
                            FontWeight::NORMAL,
                            pal.text_muted,
                        ))),
                )
                .border(SurfaceBorder::Bottom)
                .padding(Insets {
                    left: 0.0,
                    right: 0.0,
                    top: 4.0,
                    bottom: 6.0,
                })
                .fill_width(true)
                .theme_when(clone_reader(reader)),
            );
        }
    }
    PanelSection::new("Matching series", rows).theme_when(clone_reader(reader))
}

fn request_metric_explorer_relayout(ctx: &mut EventCtx) {
    ctx.request_measure();
    ctx.request_arrange();
    ctx.request_paint();
    ctx.request_semantics();
}

fn metric_kind_label(kind: sinomo_runtime::observe::ObserveMetricKind) -> &'static str {
    use sinomo_runtime::observe::ObserveMetricKind;
    match kind {
        ObserveMetricKind::Counter => "Counter",
        ObserveMetricKind::Gauge => "Gauge",
        ObserveMetricKind::Histogram => "Histogram",
    }
}

fn metric_unit_label(unit: Option<sinomo_runtime::observe::ObserveMetricUnit>) -> &'static str {
    use sinomo_runtime::observe::ObserveMetricUnit;
    match unit {
        Some(ObserveMetricUnit::Count) => "count",
        Some(ObserveMetricUnit::Bytes) => "bytes",
        Some(ObserveMetricUnit::Seconds) => "seconds",
        Some(ObserveMetricUnit::Microseconds) => "microseconds",
        Some(ObserveMetricUnit::Ratio) => "ratio",
        None => "raw value",
    }
}

fn metric_query_transform(
    transform: ExplorerTransform,
    kind: sinomo_runtime::observe::ObserveMetricKind,
) -> MetricTransform {
    use sinomo_runtime::observe::ObserveMetricKind;
    match transform {
        ExplorerTransform::Auto => match kind {
            ObserveMetricKind::Counter => MetricTransform::Rate,
            ObserveMetricKind::Gauge => MetricTransform::Current,
            ObserveMetricKind::Histogram => MetricTransform::HistogramQuantile(95),
        },
        ExplorerTransform::Current => MetricTransform::Current,
        ExplorerTransform::Rate => MetricTransform::Rate,
        ExplorerTransform::Delta => MetricTransform::Delta,
        ExplorerTransform::P50 => MetricTransform::HistogramQuantile(50),
        ExplorerTransform::P95 => MetricTransform::HistogramQuantile(95),
        ExplorerTransform::P99 => MetricTransform::HistogramQuantile(99),
    }
}

fn explorer_transform_summary(
    transform: ExplorerTransform,
    kind: sinomo_runtime::observe::ObserveMetricKind,
) -> &'static str {
    match (transform, kind) {
        (ExplorerTransform::Auto, sinomo_runtime::observe::ObserveMetricKind::Counter) => {
            "Automatic rate"
        }
        (ExplorerTransform::Auto, sinomo_runtime::observe::ObserveMetricKind::Gauge) => {
            "Automatic value"
        }
        (ExplorerTransform::Auto, sinomo_runtime::observe::ObserveMetricKind::Histogram) => {
            "Automatic p95"
        }
        _ => transform.label(),
    }
}

fn short_metric_name(name: &str, domain: &str) -> String {
    name.strip_prefix(&format!("sinomo.{domain}."))
        .unwrap_or(name)
        .to_string()
}

struct ClusterView {
    shared: Shared,
    keyboard_focus: RefCell<Option<ClusterSelection>>,
    empty: WidgetPod,
    legend: WidgetPod,
    detail: WidgetPod,
    show_detail: Cell<bool>,
}

impl ClusterView {
    fn new(shared: Shared) -> Self {
        let reader = cluster_theme_reader(&shared);
        Self {
            empty: WidgetPod::new(cluster_empty_host(&shared, &reader)),
            legend: WidgetPod::new(cluster_legend(&reader)),
            detail: WidgetPod::new(cluster_detail_host(&shared, &reader)),
            show_detail: Cell::new(false),
            shared,
            keyboard_focus: RefCell::new(None),
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn focused_vertex_index(&self, topology: &Topology) -> Option<usize> {
        self.keyboard_focus
            .borrow()
            .as_ref()
            .and_then(|focused| {
                topology
                    .vertices
                    .iter()
                    .position(|vertex| graph_vertex_selection(vertex) == *focused)
            })
            .or_else(|| {
                let selected = self.shared.cluster().selected.clone()?;
                topology
                    .vertices
                    .iter()
                    .position(|vertex| graph_vertex_selection(vertex) == selected)
            })
            .or_else(|| (!topology.vertices.is_empty()).then_some(0))
    }

    fn move_keyboard_focus(&self, topology: &Topology, delta: isize) -> bool {
        if topology.vertices.is_empty() {
            *self.keyboard_focus.borrow_mut() = None;
            return false;
        }
        let current = self.focused_vertex_index(topology).unwrap_or(0);
        let len = topology.vertices.len() as isize;
        let next = (current as isize + delta).rem_euclid(len) as usize;
        *self.keyboard_focus.borrow_mut() = topology.vertices.get(next).map(graph_vertex_selection);
        true
    }

    fn activate_keyboard_vertex(&self, topology: &Topology) -> bool {
        let Some(index) = self.focused_vertex_index(topology) else {
            return false;
        };
        let Some(vertex) = topology.vertices.get(index) else {
            return false;
        };
        let selection = graph_vertex_selection(vertex);
        *self.keyboard_focus.borrow_mut() = Some(selection.clone());
        let mut cluster = self.shared.cluster_mut();
        if cluster.selected != Some(selection.clone()) {
            cluster.diagnostics_open = false;
        }
        cluster.selected = Some(selection);
        true
    }
}

impl Widget for ClusterView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Keyboard(key) if key.state == KeyState::Pressed && ctx.is_focused() => {
                let snapshot = self.shared.cluster().snapshot.clone();
                let topology = build_topology(&snapshot);
                let handled = match key.key.as_str() {
                    "ArrowRight" | "ArrowDown" => self.move_keyboard_focus(&topology, 1),
                    "ArrowLeft" | "ArrowUp" => self.move_keyboard_focus(&topology, -1),
                    "Home" if !topology.vertices.is_empty() => {
                        *self.keyboard_focus.borrow_mut() =
                            topology.vertices.first().map(graph_vertex_selection);
                        true
                    }
                    "End" if !topology.vertices.is_empty() => {
                        *self.keyboard_focus.borrow_mut() =
                            topology.vertices.last().map(graph_vertex_selection);
                        true
                    }
                    "Enter" | " " => self.activate_keyboard_vertex(&topology),
                    _ => false,
                };
                if handled {
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    ctx.request_semantics();
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && ctx.bounds().contains(pointer.position) =>
            {
                let snapshot = self.shared.cluster().snapshot.clone();
                let topology = build_topology(&snapshot);
                if topology.vertices.is_empty() {
                    return;
                }
                let graph = graph_rect(ctx.bounds());
                if !graph.contains(pointer.position)
                    || (self.show_detail.get()
                        && detail_panel_rect(ctx.bounds()).contains(pointer.position))
                {
                    return;
                }
                let layout = layout_vertices(&topology, graph);
                let selection = hit_test(&layout, pointer.position);
                *self.keyboard_focus.borrow_mut() = selection.clone();
                let mut cluster = self.shared.cluster_mut();
                if cluster.selected != selection {
                    cluster.diagnostics_open = false;
                }
                cluster.selected = selection;
                drop(cluster);
                ctx.request_focus();
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
            }
            _ => {}
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let structure = self.shared.signals().cluster_structure.clone();
        let _ = ctx.observe(&structure);
        let size = constraints.max;
        let bounds = Rect::new(0.0, 0.0, size.width.max(0.0), size.height.max(0.0));
        let graph = graph_rect(bounds);
        let detail = detail_panel_rect(bounds);
        let legend = legend_rect(bounds);
        let has_vertices = !self.shared.cluster().snapshot.nodes.is_empty();
        let show_detail =
            has_vertices && (size.width >= 900.0 || self.shared.cluster().selected.is_some());
        self.show_detail.set(show_detail);

        if has_vertices {
            self.empty.measure(ctx, Constraints::tight(Size::ZERO));
            self.legend
                .measure(ctx, Constraints::tight(rect_size(legend)));
            self.detail.measure(
                ctx,
                Constraints::tight(if show_detail {
                    rect_size(detail)
                } else {
                    Size::ZERO
                }),
            );
        } else {
            self.empty
                .measure(ctx, Constraints::tight(rect_size(graph)));
            self.legend.measure(ctx, Constraints::tight(Size::ZERO));
            self.detail.measure(ctx, Constraints::tight(Size::ZERO));
        }
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        if self.shared.cluster().snapshot.nodes.is_empty() {
            self.empty.arrange(ctx, graph_rect(bounds));
            self.legend.arrange(ctx, Rect::ZERO);
            self.detail.arrange(ctx, Rect::ZERO);
        } else {
            self.empty.arrange(ctx, Rect::ZERO);
            self.legend.arrange(ctx, legend_rect(bounds));
            self.detail.arrange(
                ctx,
                if self.show_detail.get() {
                    detail_panel_rect(bounds)
                } else {
                    Rect::ZERO
                },
            );
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let snapshot = self.shared.cluster().snapshot.clone();
        let topology = build_topology(&snapshot);
        let bounds = ctx.bounds();
        let graph = graph_rect(bounds);
        let layout = layout_vertices(&topology, graph);
        let selected = {
            let cluster = self.shared.cluster();
            cluster.selected.clone()
        };

        ctx.fill_bounds(pal.window);
        if topology.vertices.is_empty() {
            self.empty.paint(ctx);
            return;
        }

        paint_edges(ctx, &pal, &topology, &layout);
        paint_vertices(ctx, &theme, &pal, &layout, &selected);
        if ctx.is_focused()
            && let Some(index) = self.focused_vertex_index(&topology)
            && let Some(vertex) = layout.get(index)
        {
            ctx.stroke(
                Path::circle(vertex.center, vertex.radius + 5.0),
                theme.palette.focus_ring,
                StrokeStyle::new(2.0),
            );
        }
        self.legend.paint(ctx);
        if self.show_detail.get() {
            self.detail.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let snapshot = self.shared.cluster().snapshot.clone();
        let topology = build_topology(&snapshot);
        let layout = layout_vertices(&topology, graph_rect(ctx.bounds()));
        let focused_index = self.focused_vertex_index(&topology);
        let selected_vertex = self.shared.cluster().selected.clone();
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::List, ctx.bounds());
        node.name = Some("Cluster nodes and LAN segments".to_string());
        node.description = Some(
            "Use arrow keys to move between topology items and Enter or Space to select one"
                .to_string(),
        );
        node.state.focused = ctx.is_focused();
        node.actions = vec![SemanticsAction::Focus];
        ctx.push(node);

        for (index, vertex) in layout.iter().enumerate() {
            let selection = graph_vertex_selection(&vertex.vertex);
            let mut item = SemanticsNode::new(
                cluster_vertex_widget_id(ctx.widget_id(), &vertex.vertex),
                SemanticsRole::ListItem,
                Rect::new(
                    vertex.center.x - vertex.radius,
                    vertex.center.y - vertex.radius,
                    vertex.radius * 2.0,
                    vertex.radius * 2.0,
                ),
            );
            item.parent = Some(ctx.widget_id());
            item.name = Some(vertex.vertex.label.clone());
            item.description = Some(format!(
                "{}; {}; {}",
                match vertex.vertex.kind {
                    GraphVertexKind::Node => "Cluster node",
                    GraphVertexKind::LanSegment => "LAN segment",
                },
                vertex.vertex.detail,
                vertex.vertex.availability
            ));
            item.state.selected = selected_vertex.as_ref() == Some(&selection);
            item.state.focused = ctx.is_focused() && focused_index == Some(index);
            item.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
            ctx.push(item);
        }

        if topology.vertices.is_empty() {
            self.empty.semantics(ctx);
        } else {
            self.legend.semantics(ctx);
            if self.show_detail.get() {
                self.detail.semantics(ctx);
            }
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.empty);
        visitor.visit(&self.legend);
        if self.show_detail.get() {
            visitor.visit(&self.detail);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.empty);
        visitor.visit(&mut self.legend);
        if self.show_detail.get() {
            visitor.visit(&mut self.detail);
        }
    }

    fn accepts_focus(&self) -> bool {
        true
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, _focused: bool) {
        ctx.request_paint();
        ctx.request_semantics();
    }
}

type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

fn cluster_theme_reader(shared: &Shared) -> ThemeReader {
    let shared = shared.clone();
    Rc::new(move || shared.theme())
}

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterObservationViewKey {
    theme: DefaultTheme,
    snapshot: ClusterSnapshot,
    observation_revision: u64,
}

/// The performance dashboard, or the cluster empty state while there are no nodes and no
/// metrics, instead of a grid of blank cards and charts.
fn cluster_overview_host(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let structure = &shared.signals().cluster_structure;
    let selected = structure.select_named("cluster overview empty", |key| {
        usize::from(key.overview_empty())
    });
    let empty_source = structure.select_named("cluster overview empty state", |key| {
        (key.theme, key.snapshot.warnings.clone(), key.refreshing)
    });
    let empty_shared = shared.clone();
    let empty_reader = Rc::clone(reader);
    SwitchView::new()
        .selected_from(selected)
        .with_child(dashboard::view(shared.clone()))
        .with_child(RebuildOnChange::key_from(empty_source, move |_key| {
            build_cluster_empty(&empty_shared, &empty_reader)
        }))
}

fn cluster_activity_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<ClusterObservationViewKey> {
    let source =
        shared
            .signals()
            .cluster_structure
            .select_named("cluster activity structure", |key| {
                ClusterObservationViewKey {
                    theme: key.theme,
                    snapshot: key.snapshot.clone(),
                    observation_revision: key.observation_revision,
                }
            });
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |_key| {
        build_cluster_activity(&build_shared, &reader)
    })
}

fn build_cluster_activity(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let theme = shared.theme();
    let (warnings, activity) = {
        let cluster = shared.cluster();
        (
            cluster.snapshot.warnings.clone(),
            cluster
                .observation
                .activity
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        )
    };
    if warnings.is_empty() && activity.is_empty() {
        return WidgetPod::new(
            EmptyState::new(
                "No recent cluster activity",
                "Collection failures and topology warnings appear here",
            )
            .name("Cluster activity")
            .icon(IconGlyph::Check)
            .detail("This view updates while Cluster is open.")
            .transparent()
            .theme_when(clone_reader(reader)),
        );
    }
    let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    for warning in warnings {
        rows.push(
            DetailRow::new("Topology warning", warning)
                .max_value_lines(3)
                .theme_when(clone_reader(reader)),
        );
    }
    for entry in activity.into_iter().rev() {
        let time = format!(
            "{:02}:{:02}:{:02}",
            entry.at.hour(),
            entry.at.minute(),
            entry.at.second()
        );
        rows.push(
            DetailRow::new(
                format!("{time} · {}", short(&entry.node_id, 18)),
                entry.detail,
            )
            .max_value_lines(if entry.warning { 3 } else { 2 })
            .theme_when(clone_reader(reader)),
        );
    }
    WidgetPod::new(
        Surface::window(
            ScrollView::vertical(
                PanelSection::new("Operational activity", rows).theme_when(clone_reader(reader)),
            )
            .name("Cluster operational activity")
            .theme(theme)
            .viewport_width_hint(true),
        )
        .padding(Insets::all(PAD))
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn sort_target_summaries(targets: &mut [TargetObservationSummary], local_node_id: Option<&str>) {
    targets.sort_by(|left, right| {
        let left_local = local_node_id.is_some_and(|local| left.node_id == local);
        let right_local = local_node_id.is_some_and(|local| right.node_id == local);
        right_local
            .cmp(&left_local)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
}

fn format_optional(value: Option<f64>, suffix: &str) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:.1}{suffix}"))
        .unwrap_or_else(|| "—".to_string())
}

fn format_metric_value(
    value: Option<f64>,
    unit: Option<sinomo_runtime::observe::ObserveMetricUnit>,
    transform: ExplorerTransform,
    kind: sinomo_runtime::observe::ObserveMetricKind,
) -> String {
    use sinomo_runtime::observe::ObserveMetricUnit;

    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "—".to_string();
    };
    let rate = metric_query_transform(transform, kind) == MetricTransform::Rate;
    match unit {
        Some(ObserveMetricUnit::Bytes) => {
            let (scaled, suffix) = if value.abs() >= 1024.0_f64.powi(3) {
                (value / 1024.0_f64.powi(3), "GiB")
            } else if value.abs() >= 1024.0_f64.powi(2) {
                (value / 1024.0_f64.powi(2), "MiB")
            } else if value.abs() >= 1024.0 {
                (value / 1024.0, "KiB")
            } else {
                (value, "B")
            };
            format!("{scaled:.2} {suffix}{}", if rate { "/s" } else { "" })
        }
        Some(ObserveMetricUnit::Seconds) => {
            if value.abs() < 1.0 {
                format!("{:.2} ms{}", value * 1_000.0, if rate { "/s" } else { "" })
            } else {
                format!("{value:.2} s{}", if rate { "/s" } else { "" })
            }
        }
        Some(ObserveMetricUnit::Microseconds) => {
            if value.abs() >= 1_000_000.0 {
                format!(
                    "{:.2} s{}",
                    value / 1_000_000.0,
                    if rate { "/s" } else { "" }
                )
            } else if value.abs() >= 1_000.0 {
                format!("{:.2} ms{}", value / 1_000.0, if rate { "/s" } else { "" })
            } else {
                format!("{value:.1} µs{}", if rate { "/s" } else { "" })
            }
        }
        Some(ObserveMetricUnit::Ratio) => format!("{:.1}%", value * 100.0),
        Some(ObserveMetricUnit::Count) => {
            if rate {
                format!("{value:.2}/s")
            } else if value.abs() >= 100.0 || value.fract().abs() < f64::EPSILON {
                format!("{value:.0}")
            } else {
                format!("{value:.2}")
            }
        }
        None => {
            if rate {
                format!("{value:.2}/s")
            } else {
                format!("{value:.3}")
            }
        }
    }
}

struct SyncedMetricSearch {
    inner: TextInput,
    value_reader: Box<dyn Fn() -> String>,
}

impl SyncedMetricSearch {
    fn new<F>(inner: TextInput, value_reader: F) -> Self
    where
        F: Fn() -> String + 'static,
    {
        Self {
            inner,
            value_reader: Box::new(value_reader),
        }
    }

    fn sync_value(&mut self) {
        let value = (self.value_reader)();
        if self.inner.current_value() != value {
            self.inner.set_value(value);
        }
    }
}

impl Widget for SyncedMetricSearch {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.inner.event(ctx, event);
    }

    fn command(&mut self, ctx: &mut EventCtx, command: &Command<'_>) {
        self.inner.command(ctx, command);
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_value();
        self.inner.measure(ctx, constraints)
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.inner.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.inner.semantics(ctx);
    }

    fn accepts_focus(&self) -> bool {
        self.inner.accepts_focus()
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.inner.focus_changed(ctx, focused);
    }
}

mod chart;
use chart::MetricChart;

#[derive(Clone, Debug, PartialEq)]
struct ClusterHeaderKey {
    theme: DefaultTheme,
    snapshot: ClusterSnapshot,
    refreshing: bool,
}

fn cluster_header_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<ClusterHeaderKey> {
    let source =
        shared
            .signals()
            .cluster_structure
            .select_named("cluster header structure", |key| ClusterHeaderKey {
                theme: key.theme,
                snapshot: key.snapshot.clone(),
                refreshing: key.refreshing,
            });
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |_key| {
        build_cluster_header(&build_shared, &reader)
    })
}

fn build_cluster_header(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, refreshing) = {
        let cluster = shared.cluster();
        (cluster.snapshot.clone(), cluster.refreshing)
    };
    let topology = build_topology(&snapshot);
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let summary = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(2.0)
        .with_child(Label::new("Cluster").text_style(pal.text_style(
            &theme,
            theme.text.xl.size,
            FontWeight::SEMIBOLD,
            pal.text,
        )))
        .with_child(
            Label::new(cluster_header_summary(&snapshot, &topology)).text_style(pal.text_style(
                &theme,
                theme.text.sm.size,
                FontWeight::NORMAL,
                pal.text_muted,
            )),
        );
    let (status, tone) = if refreshing {
        ("Refreshing".to_string(), SemanticTone::Accent)
    } else {
        (
            cluster_health_badge_summary(&snapshot),
            cluster_health_tone(&snapshot),
        )
    };
    let row = TrailingSlotRow::new(
        summary,
        StatusBadge::new(status)
            .tone(tone)
            .theme_when(clone_reader(reader)),
    )
    .trailing_width(142.0)
    .trailing_height(30.0)
    .gap(12.0);
    WidgetPod::new(
        Surface::window(row)
            .border(SurfaceBorder::Bottom)
            .padding(Insets {
                left: PAD,
                right: PAD,
                top: 10.0,
                bottom: 10.0,
            })
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_empty_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<ClusterHeaderKey> {
    let source =
        shared
            .signals()
            .cluster_structure
            .select_named("cluster empty structure", |key| ClusterHeaderKey {
                theme: key.theme,
                snapshot: key.snapshot.clone(),
                refreshing: key.refreshing,
            });
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |_key| {
        build_cluster_empty(&build_shared, &reader)
    })
}

fn build_cluster_empty(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, refreshing) = {
        let cluster = shared.cluster();
        (cluster.snapshot.clone(), cluster.refreshing)
    };
    let title = if snapshot.warnings.is_empty() {
        "No cluster nodes"
    } else {
        "Cluster metadata unavailable"
    };
    let description = if refreshing {
        "Refreshing cluster membership and health in the background"
    } else {
        "Initialize or join a cluster to populate this view"
    };
    let detail = snapshot
        .warnings
        .first()
        .cloned()
        .unwrap_or_else(|| "Cluster nodes appear here after discovery completes.".to_string());
    let refresh_shared = shared.clone();
    let refresh = Button::new(if refreshing { "Refreshing" } else { "Refresh" })
        .icon(IconGlyph::Redo)
        .description("Refresh cluster health and topology")
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Accent)
        .enabled(!refreshing)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request_refresh(&refresh_shared);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        });
    WidgetPod::new(
        EmptyState::new(title, description)
            .name("Cluster empty state")
            .icon(IconGlyph::Link)
            .detail(detail)
            .action(refresh)
            .action_max_width(160.0)
            .transparent()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_legend(reader: &ThemeReader) -> Surface {
    let row = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(5.0)
        .with_child(
            StatusBadge::new("Online")
                .tone(SemanticTone::Success)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Offline")
                .tone(SemanticTone::Danger)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Unknown")
                .tone(SemanticTone::Neutral)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("LAN")
                .tone(SemanticTone::Warning)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Public route")
                .tone(SemanticTone::Info)
                .theme_when(clone_reader(reader)),
        );
    Surface::panel(row)
        .appearance(SurfaceAppearance::Raised)
        .elevation(SurfaceElevation::Small)
        .padding(Insets::all(5.0))
        .fill()
        .theme_when(clone_reader(reader))
}

fn paint_edges(ctx: &mut PaintCtx, pal: &Palette, topology: &Topology, layout: &[LayoutVertex]) {
    let centers = layout
        .iter()
        .map(|vertex| (vertex.vertex.id.as_str(), vertex.center))
        .collect::<BTreeMap<_, _>>();
    for edge in &topology.edges {
        let (Some(a), Some(b)) = (centers.get(edge.a.as_str()), centers.get(edge.b.as_str()))
        else {
            continue;
        };
        let color = if edge.public_ip {
            pal.info
        } else if edge.state == "degraded" {
            pal.warn
        } else {
            pal.border_strong
        };
        let width = if edge.virtual_lan { 1.0 } else { 1.5 };
        let mut path = PathBuilder::new();
        path.move_to(*a).line_to(*b);
        ctx.stroke(path.build(), color, StrokeStyle::new(width));
    }
}

fn paint_vertices(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    layout: &[LayoutVertex],
    selected: &Option<ClusterSelection>,
) {
    for vertex in layout {
        let selected_here = matches!(
            (selected, &vertex.vertex.kind),
            (Some(ClusterSelection::Node(id)), GraphVertexKind::Node) if id == &vertex.vertex.id
        ) || matches!(
            (selected, &vertex.vertex.kind),
            (Some(ClusterSelection::LanSegment(id)), GraphVertexKind::LanSegment)
                if id == &vertex.vertex.id
        );
        let fill = vertex_fill(pal, &vertex.vertex);
        let border = if selected_here {
            pal.accent
        } else if vertex.vertex.kind == GraphVertexKind::LanSegment {
            pal.warn
        } else if vertex.vertex.availability == "unknown" {
            pal.border_strong
        } else {
            pal.border
        };
        ctx.fill(Path::circle(vertex.center, vertex.radius), fill);
        ctx.stroke(
            Path::circle(
                vertex.center,
                vertex.radius + if selected_here { 4.0 } else { 1.0 },
            ),
            border,
            StrokeStyle::new(if selected_here { 2.0 } else { 1.25 }),
        );
        if selected_here {
            // Selected nodes glow like the Mesh cluster map's self node — a
            // soft accent halo outside the ring (empty in Light, damped in Void).
            if let Some(glow) = pal.glow_accent.first {
                ctx.stroke(
                    Path::circle(vertex.center, vertex.radius + 6.0),
                    glow.color,
                    StrokeStyle::new(glow.blur * 0.35),
                );
            }
        }
        if vertex.vertex.kind == GraphVertexKind::Node && vertex.vertex.public_reachable {
            paint_public_reachability_badge(ctx, pal, vertex.center, vertex.radius);
        }

        let label_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(theme, 11.0, FontWeight::NORMAL, pal.text_faint);
        let text_layout = vertex_text_layout(
            vertex.center,
            vertex.radius,
            label_style.line_height,
            detail_style.line_height,
        );
        widgets::draw_centered(
            ctx,
            &vertex.vertex.label,
            label_style,
            vertex.center.x,
            text_layout.label_center_y,
        );
        widgets::draw_centered(
            ctx,
            &vertex.vertex.detail,
            detail_style,
            vertex.center.x,
            text_layout.detail_center_y,
        );
    }
}

#[derive(Clone, Copy, Debug)]
struct VertexTextLayout {
    label_center_y: f32,
    detail_center_y: f32,
}

fn vertex_text_layout(
    center: Point,
    radius: f32,
    label_line_height: f32,
    detail_line_height: f32,
) -> VertexTextLayout {
    let label_line_height = label_line_height.max(1.0);
    let detail_line_height = detail_line_height.max(1.0);
    let label_top = center.y + radius + VERTEX_TEXT_TOP_GAP;
    let label_bottom = label_top + label_line_height;
    let detail_top = label_bottom + VERTEX_TEXT_LINE_GAP;
    VertexTextLayout {
        label_center_y: label_top + label_line_height * 0.5,
        detail_center_y: detail_top + detail_line_height * 0.5,
    }
}

fn vertex_fill(pal: &Palette, vertex: &GraphVertex) -> Color {
    match vertex.kind {
        GraphVertexKind::LanSegment => lan_segment_color(pal),
        GraphVertexKind::Node => match vertex.availability.as_str() {
            "online" => pal.good,
            "offline" => pal.bad,
            "degraded" => pal.warn,
            _ => unknown_node_color(pal),
        },
    }
}

fn paint_public_reachability_badge(ctx: &mut PaintCtx, pal: &Palette, center: Point, radius: f32) {
    let badge_center = Point::new(center.x + radius * 0.58, center.y - radius * 0.58);
    ctx.fill(Path::circle(badge_center, 5.5), public_node_color(pal));
    ctx.stroke(
        Path::circle(badge_center, 6.4),
        pal.panel,
        StrokeStyle::new(1.8),
    );
}

/// Public/internet reachability rides the informational hue.
fn public_node_color(pal: &Palette) -> Color {
    pal.info
}

/// LAN segment hubs use the amber agent hue — a topology identity, not a
/// warning (labels next to them stay on ink tokens).
fn lan_segment_color(pal: &Palette) -> Color {
    pal.agent[3]
}

/// Unknown availability reads as disabled, not as a status.
fn unknown_node_color(pal: &Palette) -> Color {
    pal.text_disabled
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterDetailKey {
    theme: DefaultTheme,
    snapshot: ClusterSnapshot,
    selected: Option<ClusterSelection>,
    diagnostics_open: bool,
    refreshing: bool,
}

fn cluster_detail_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<ClusterDetailKey> {
    let source =
        shared
            .signals()
            .cluster_structure
            .select_named("cluster detail structure", |key| ClusterDetailKey {
                theme: key.theme,
                snapshot: key.snapshot.clone(),
                selected: key.selected.clone(),
                diagnostics_open: key.diagnostics_open,
                refreshing: key.refreshing,
            });
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |_key| {
        build_cluster_detail(&build_shared, &reader)
    })
}

fn build_cluster_detail(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, selection, diagnostics_open, refreshing) = {
        let cluster = shared.cluster();
        (
            cluster.snapshot.clone(),
            cluster.selected.clone(),
            cluster.diagnostics_open,
            cluster.refreshing,
        )
    };
    let topology = build_topology(&snapshot);
    let lines = selection
        .as_ref()
        .map(|selection| detail_lines(&snapshot, &topology, selection, diagnostics_open))
        .unwrap_or_else(|| overview_lines(&snapshot, &topology, diagnostics_open, refreshing));
    let title = lines
        .first()
        .cloned()
        .unwrap_or_else(|| "Cluster overview".to_string());
    let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    for line in lines.iter().skip(1).filter(|line| !line.trim().is_empty()) {
        let (label, value, max_lines) = if let Some(hint) = line.strip_prefix("hint:") {
            ("Guidance", hint.trim(), 3)
        } else if let Some((label, value)) = line.split_once(':') {
            (
                label.trim(),
                value.trim(),
                if diagnostics_open { 3 } else { 2 },
            )
        } else {
            ("Detail", line.trim(), 2)
        };
        rows.push(
            DetailRow::new(label, value)
                .max_value_lines(max_lines)
                .theme_when(clone_reader(reader)),
        );
    }
    let (status, tone) = cluster_detail_status(&snapshot, &topology, selection.as_ref());
    let details = PanelSection::new(
        title,
        ScrollView::vertical(rows)
            .name("Cluster detail fields")
            .theme(shared.theme())
            .viewport_width_hint(true),
    )
    .header_action(
        StatusBadge::new(status)
            .tone(tone)
            .theme_when(clone_reader(reader)),
    )
    .theme_when(clone_reader(reader));

    let refresh_shared = shared.clone();
    let refresh = Button::new(if refreshing { "Refreshing" } else { "Refresh" })
        .icon(IconGlyph::Redo)
        .description("Refresh cluster health and topology")
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .min_height(DETAIL_BUTTON_H)
        .enabled(!refreshing)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request_refresh(&refresh_shared);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        });
    let warning_visible = if selection.is_some() {
        selected_node_needs_connection_settings(&snapshot, selection.as_ref())
    } else {
        !refreshing && cluster_actionable_warning(&snapshot).is_some()
    };
    let primary_count = 1 + usize::from(selection.is_some()) + usize::from(warning_visible);
    let primary_height =
        DETAIL_BUTTON_H * primary_count as f32 + 8.0 * primary_count.saturating_sub(1) as f32;
    let mut primary_actions = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(refresh, FlexItem::fixed(DETAIL_BUTTON_H));
    if selection.is_some() {
        let back = shared.clone();
        primary_actions.push_item(
            Button::new("Back to topology")
                .min_height(DETAIL_BUTTON_H)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    let mut cluster = back.cluster_mut();
                    cluster.selected = None;
                    cluster.diagnostics_open = false;
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                }),
            FlexItem::fixed(DETAIL_BUTTON_H),
        );
    }
    if warning_visible {
        let settings_shared = shared.clone();
        let description = if selection.is_some() {
            "Open connection settings for selected cluster node"
        } else {
            "Open connection settings for cluster warning"
        };
        primary_actions.push_item(
            Button::new("Connection settings")
                .icon(IconGlyph::Alert)
                .description(description)
                .appearance(ButtonAppearance::Filled)
                .tone(SemanticTone::Accent)
                .min_height(DETAIL_BUTTON_H)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    open_connection_settings(&settings_shared);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                }),
            FlexItem::fixed(DETAIL_BUTTON_H),
        );
    }

    let toggle_shared = shared.clone();
    let mut secondary_actions = Flex::horizontal()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(
            Button::new(if diagnostics_open {
                "Hide diagnostics"
            } else {
                "Show diagnostics"
            })
            .icon(if diagnostics_open {
                IconGlyph::ChevronUp
            } else {
                IconGlyph::ChevronDown
            })
            .description("Show technical cluster source and identifier details")
            .appearance(ButtonAppearance::Outline)
            .tone(SemanticTone::Neutral)
            .min_height(DETAIL_BUTTON_H)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                let open = toggle_shared.cluster().diagnostics_open;
                toggle_shared.cluster_mut().diagnostics_open = !open;
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
            }),
            FlexItem::fill().min_width(0.0),
        );
    if selection
        .as_ref()
        .is_some_and(|selection| selected_copy_details(&snapshot, selection).is_some())
    {
        let copy_shared = shared.clone();
        let copy_selection = selection.clone().expect("selection checked above");
        let copy_value = selected_copy_details(&snapshot, &copy_selection).unwrap_or_default();
        secondary_actions.push_item(
            Button::new("Copy details")
                .semantic_name("Copy node details")
                .icon(IconGlyph::Restore)
                .description(format!(
                    "Copy the full node ID, address, and endpoints: {copy_value}"
                ))
                .appearance(ButtonAppearance::Outline)
                .tone(SemanticTone::Neutral)
                .min_height(DETAIL_BUTTON_H)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    if let Some(value) =
                        selected_copy_details(&copy_shared.cluster().snapshot, &copy_selection)
                    {
                        ctx.set_clipboard_text(value);
                    }
                    ctx.request_semantics();
                }),
            FlexItem::fill().min_width(0.0),
        );
    }

    let actions = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(primary_actions, FlexItem::fixed(primary_height))
        .with_item(secondary_actions, FlexItem::fixed(DETAIL_BUTTON_H));
    let content = Dock::new(details).bottom(primary_height + DETAIL_BUTTON_H + 8.0, actions);
    WidgetPod::new(
        Surface::panel(content)
            .name("Cluster inspector")
            .appearance(SurfaceAppearance::Raised)
            .elevation(SurfaceElevation::Medium)
            .padding(Insets::all(PANEL_PAD))
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_detail_status(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    selection: Option<&ClusterSelection>,
) -> (String, SemanticTone) {
    match selection {
        Some(ClusterSelection::Node(id)) => snapshot
            .nodes
            .iter()
            .find(|node| &node.node_id == id)
            .map(|node| {
                (
                    node.availability.clone(),
                    availability_tone(&node.availability),
                )
            })
            .unwrap_or_else(|| ("Missing".to_string(), SemanticTone::Danger)),
        Some(ClusterSelection::LanSegment(id)) => (
            count_noun(
                topology
                    .lan_segments
                    .get(id)
                    .map(Vec::len)
                    .unwrap_or_default(),
                "member",
                "members",
            ),
            SemanticTone::Info,
        ),
        None => (
            cluster_health_summary(snapshot),
            cluster_health_tone(snapshot),
        ),
    }
}

fn availability_tone(availability: &str) -> SemanticTone {
    match availability.to_ascii_lowercase().as_str() {
        "online" | "healthy" => SemanticTone::Success,
        "offline" | "unavailable" => SemanticTone::Danger,
        "degraded" | "unknown" => SemanticTone::Warning,
        _ => SemanticTone::Neutral,
    }
}

fn cluster_health_tone(snapshot: &ClusterSnapshot) -> SemanticTone {
    let counts = availability_counts(snapshot);
    if counts.offline > 0 {
        SemanticTone::Danger
    } else if counts.unknown > 0 || counts.total == 0 {
        SemanticTone::Warning
    } else {
        SemanticTone::Success
    }
}

fn open_connection_settings(shared: &Shared) {
    if shared.is_ready() {
        shared.settings_mut().open(SettingsSection::System);
    }
    shared.set_active_tab(Tab::Settings);
}

fn overview_lines(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    diagnostics_open: bool,
    refreshing: bool,
) -> Vec<String> {
    let mut lines = vec![
        "Cluster overview".to_string(),
        format!("connection: {}", source_summary(&snapshot.source)),
        format!("health: {}", cluster_health_summary(snapshot)),
        format!("public route: {}", public_reachability_summary(snapshot)),
        format!("mode: {}", cluster_route_mode_summary(snapshot)),
        format!("LAN: {}", lan_segment_summary(topology)),
    ];
    let warning = cluster_actionable_warning(snapshot);
    if refreshing {
        lines.push("status: refreshing current health".to_string());
    } else if let Some(warning) = warning {
        lines.push(format!(
            "attention: {}",
            cluster_attention_summary(snapshot, warning)
        ));
    }
    if diagnostics_open {
        if let Some(warning) = snapshot.warnings.first() {
            let label = if refreshing {
                "previous warning detail"
            } else {
                "warning detail"
            };
            lines.push(format!("{label}: {}", short(warning, 42)));
        }
        lines.push(format!(
            "topology: {} direct edges",
            direct_topology_edge_count(topology)
        ));
        lines.push(format!("diagnostics: {}", snapshot.source));
        lines.push(format!(
            "cluster: {}",
            snapshot
                .cluster_id
                .as_deref()
                .map(|id| short(id, 42))
                .unwrap_or("-".into())
        ));
        lines.push(format!(
            "local: {}",
            snapshot
                .local_node_id
                .as_deref()
                .map(|id| short(id, 42))
                .unwrap_or("-".into())
        ));
    } else {
        lines.push(if warning.is_some() {
            if refreshing {
                "hint: Refreshing current health; show diagnostics for previous warning detail."
                    .to_string()
            } else {
                "hint: Show diagnostics for warning detail, source, and cluster identifiers."
                    .to_string()
            }
        } else {
            "hint: Show diagnostics for source and cluster identifiers.".to_string()
        });
    }
    lines.push(
        "hint: Select a node or LAN segment for process and reachability details.".to_string(),
    );
    lines
}

#[derive(Clone, Copy, Default)]
struct AvailabilityCounts {
    total: usize,
    online: usize,
    offline: usize,
    unknown: usize,
}

fn availability_counts(snapshot: &ClusterSnapshot) -> AvailabilityCounts {
    let mut counts = AvailabilityCounts {
        total: snapshot.nodes.len(),
        ..AvailabilityCounts::default()
    };
    for node in &snapshot.nodes {
        match node.availability.to_ascii_lowercase().as_str() {
            "online" => counts.online += 1,
            "offline" | "unavailable" => counts.offline += 1,
            _ => counts.unknown += 1,
        }
    }
    counts
}

fn cluster_header_summary(snapshot: &ClusterSnapshot, topology: &Topology) -> String {
    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    format!(
        "{} · LAN {} · public {}/{}",
        count_noun(snapshot.nodes.len(), "node", "nodes"),
        count_noun(topology.lan_segments.len(), "segment", "segments"),
        public,
        snapshot.nodes.len()
    )
}

fn cluster_health_summary(snapshot: &ClusterSnapshot) -> String {
    let counts = availability_counts(snapshot);
    if counts.total == 0 {
        return "no nodes discovered".to_string();
    }

    let mut parts = vec![format!("{} / {} online", counts.online, counts.total)];
    if counts.offline > 0 {
        parts.push(plural_count(counts.offline, "offline"));
    }
    if counts.unknown > 0 {
        parts.push(plural_count(counts.unknown, "unknown"));
    }
    parts.join(", ")
}

fn cluster_health_badge_summary(snapshot: &ClusterSnapshot) -> String {
    let counts = availability_counts(snapshot);
    if counts.total == 0 {
        "No nodes".to_string()
    } else {
        format!("{} / {} online", counts.online, counts.total)
    }
}

fn direct_topology_edge_count(topology: &Topology) -> usize {
    topology
        .edges
        .iter()
        .filter(|edge| !edge.virtual_lan)
        .count()
}

fn lan_segment_summary(topology: &Topology) -> String {
    count_noun(topology.lan_segments.len(), "segment", "segments")
}

fn public_reachability_summary(snapshot: &ClusterSnapshot) -> String {
    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    format!("{public} / {} nodes", snapshot.nodes.len())
}

fn cluster_route_mode_summary(snapshot: &ClusterSnapshot) -> &'static str {
    let counts = availability_counts(snapshot);
    if counts.total == 0 {
        return "unknown until nodes report endpoints";
    }

    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    if public > 0 {
        return "public and private routing";
    }
    if counts.online == counts.total {
        return "private/LAN; public ingress not required";
    }
    "private or unverified routing"
}

fn cluster_attention_summary(snapshot: &ClusterSnapshot, warning: &str) -> String {
    let counts = availability_counts(snapshot);
    if counts.offline > 0 {
        return format!(
            "{} {}",
            counts.offline,
            node_plural(counts.offline, "offline")
        );
    }
    if counts.unknown > 0 {
        return format!(
            "{} {}",
            counts.unknown,
            node_plural(counts.unknown, "unknown")
        );
    }
    cluster_warning_summary(warning).to_string()
}

fn cluster_actionable_warning(snapshot: &ClusterSnapshot) -> Option<&str> {
    let warning = snapshot.warnings.first()?.as_str();
    let counts = availability_counts(snapshot);
    if counts.offline > 0 || counts.unknown > 0 {
        return Some(warning);
    }
    if counts.total > 0
        && counts.online == counts.total
        && cluster_warning_is_direct_probe_fallback(warning)
    {
        return None;
    }
    Some(warning)
}

fn cluster_warning_is_direct_probe_fallback(warning: &str) -> bool {
    let lower = warning.to_ascii_lowercase();
    lower.contains("daemon health aggregation unavailable")
        && lower.contains("showing direct cluster probes")
}

fn node_plural(count: usize, state: &str) -> String {
    if count == 1 {
        format!("node {state}")
    } else {
        format!("nodes {state}")
    }
}

fn count_noun(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn plural_count(count: usize, label: &str) -> String {
    format!("{} {}", count, label)
}

fn cluster_warning_summary(warning: &str) -> &'static str {
    let lower = warning.to_ascii_lowercase();
    if lower.contains("daemon health")
        || lower.contains("daemon")
        || lower.contains("offline")
        || lower.contains("health unknown")
    {
        "Cluster health needs attention"
    } else if lower.contains("network") || lower.contains("connect") {
        "Cluster connectivity needs attention"
    } else {
        "Cluster needs attention"
    }
}

fn detail_lines(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    selection: &ClusterSelection,
    diagnostics_open: bool,
) -> Vec<String> {
    match selection {
        ClusterSelection::Node(id) => snapshot
            .nodes
            .iter()
            .find(|node| &node.node_id == id)
            .map(|node| node_detail_lines(node, diagnostics_open))
            .unwrap_or_else(|| vec![id.clone(), "status: missing".to_string()]),
        ClusterSelection::LanSegment(id) => {
            let members = topology.lan_segments.get(id).cloned().unwrap_or_default();
            let mut lines = vec![
                id.replace('-', " "),
                format!("members: {}", members.len()),
                "basis: observed LAN connectivity clique".to_string(),
            ];
            if diagnostics_open {
                lines.push("diagnostics: member node IDs".to_string());
                lines.extend(
                    members
                        .iter()
                        .take(6)
                        .map(|member| format!("node: {}", short(member, 42))),
                );
            } else if !members.is_empty() {
                lines.push("hint: Show diagnostics for member node IDs.".to_string());
            }
            lines
        }
    }
}

fn node_detail_lines(node: &ClusterNodeInfo, diagnostics_open: bool) -> Vec<String> {
    let reachable_endpoints = node
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.reachable)
        .count();
    let mut lines = vec![
        node_heading(node),
        format!("node: {}", short(&node.node_id, 26)),
        format!("health: {}", node.availability),
        format!("platform: {}", platform_summary(node.platform.as_deref())),
        format!("process: {}", node.process),
        format!("role: {} {}", node.member_role, node.member_state),
        format!("address: {}", node.address.as_deref().unwrap_or("-")),
        format!("route: {}", node_route_summary(node, reachable_endpoints)),
        format!(
            "endpoints: {} / {} reachable",
            reachable_endpoints,
            node.endpoints.len()
        ),
        format!(
            "next: {}",
            node_next_step_summary(node, reachable_endpoints)
        ),
    ];
    if diagnostics_open {
        lines.push("diagnostics: endpoint addresses".to_string());
        lines.extend(node.endpoints.iter().take(4).map(|endpoint| {
            let state = if endpoint.reachable {
                "reachable"
            } else {
                "unreachable"
            };
            format!(
                "{}: {} ({state})",
                endpoint.transport,
                short(&endpoint.address, 52)
            )
        }));
        lines.push(format!(
            "public: {}",
            if node.public_reachable {
                "reachable"
            } else {
                "not observed"
            }
        ));
        lines.push(format!("version: {}", node.daemon_version));
    }
    lines
}

fn selected_copy_details(
    snapshot: &ClusterSnapshot,
    selection: &ClusterSelection,
) -> Option<String> {
    match selection {
        ClusterSelection::Node(id) => {
            let node = snapshot.nodes.iter().find(|node| &node.node_id == id)?;
            let mut lines = vec![format!("node_id: {}", node.node_id)];
            if let Some(address) = node
                .address
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("address: {address}"));
            }
            lines.extend(
                node.endpoints
                    .iter()
                    .map(|endpoint| format!("{}: {}", endpoint.transport, endpoint.address)),
            );
            Some(lines.join("\n"))
        }
        ClusterSelection::LanSegment(_) => None,
    }
}

fn node_route_summary(node: &ClusterNodeInfo, reachable_endpoints: usize) -> &'static str {
    let availability = node.availability.to_ascii_lowercase();
    if matches!(availability.as_str(), "offline" | "unavailable") {
        return "node offline";
    }
    if node.endpoints.is_empty() {
        return "no endpoints published";
    }
    if reachable_endpoints == 0 {
        return "endpoints unreachable";
    }
    if node.public_reachable {
        "public route reachable"
    } else {
        "LAN/local route only"
    }
}

fn node_next_step_summary(node: &ClusterNodeInfo, reachable_endpoints: usize) -> &'static str {
    let availability = node.availability.to_ascii_lowercase();
    if matches!(availability.as_str(), "offline" | "unavailable") {
        return "start daemon on that node or check its route";
    }
    if node.endpoints.is_empty() {
        return "publish at least one daemon endpoint";
    }
    if reachable_endpoints == 0 {
        return "check endpoint reachability in diagnostics";
    }
    if node.public_reachable {
        "public route is reachable"
    } else {
        "LAN or local routing only"
    }
}

fn source_summary(source: &str) -> &'static str {
    if source.starts_with("daemon ") {
        "daemon connected"
    } else if source.trim().is_empty() {
        "not available"
    } else {
        "snapshot loaded"
    }
}

fn selected_node_needs_connection_settings(
    snapshot: &ClusterSnapshot,
    selection: Option<&ClusterSelection>,
) -> bool {
    let Some(ClusterSelection::Node(id)) = selection else {
        return false;
    };
    snapshot
        .nodes
        .iter()
        .find(|node| &node.node_id == id)
        .is_some_and(node_needs_connection_settings)
}

fn node_needs_connection_settings(node: &ClusterNodeInfo) -> bool {
    let availability = node.availability.to_ascii_lowercase();
    let reachable_endpoints = node
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.reachable)
        .count();
    matches!(availability.as_str(), "offline" | "unavailable" | "unknown")
        || node.endpoints.is_empty()
        || reachable_endpoints == 0
}

fn detail_panel_rect(bounds: Rect) -> Rect {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    let y = bounds.y() + 18.0;
    let available_h = (bounds.max_y() - y - PAD).max(0.0);
    Rect::new(
        bounds.max_x() - panel_w - PAD,
        y,
        panel_w,
        DETAIL_PANEL_H.min(available_h),
    )
}

fn cluster_vertex_widget_id(parent: sui::WidgetId, vertex: &GraphVertex) -> sui::WidgetId {
    // A stable FNV-1a key keeps UIA identity attached to the node/LAN rather than its current
    // position in a freshly-sorted topology snapshot.
    let mut key = match vertex.kind {
        GraphVertexKind::Node => 0xcbf2_9ce4_8422_2325_u64,
        GraphVertexKind::LanSegment => 0x8422_2325_cbf2_9ce4_u64,
    };
    for byte in vertex.id.as_bytes() {
        key ^= u64::from(*byte);
        key = key.wrapping_mul(0x0000_0100_0000_01b3);
    }
    sui::WidgetId::new(
        parent
            .get()
            .wrapping_mul(67)
            .wrapping_add(12_101)
            .wrapping_add(key),
    )
}

fn legend_rect(bounds: Rect) -> Rect {
    let available_w = (bounds.width() - PAD * 2.0).max(0.0);
    let width = 430.0_f32.min(available_w);
    Rect::new(
        bounds.x() + PAD,
        bounds.max_y() - PAD - LEGEND_H,
        width,
        LEGEND_H,
    )
}

fn rect_size(rect: Rect) -> Size {
    Size::new(rect.width().max(0.0), rect.height().max(0.0))
}

fn graph_rect(bounds: Rect) -> Rect {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    let reserve_panel = bounds.width() >= 900.0;
    let right_reserve = if reserve_panel { panel_w + PAD } else { 0.0 };
    Rect::new(
        bounds.x() + PAD,
        bounds.y() + PAD,
        (bounds.width() - PAD * 2.0 - right_reserve).max(1.0),
        (bounds.height() - PAD * 2.0 - 34.0).max(1.0),
    )
}

fn build_topology(snapshot: &ClusterSnapshot) -> Topology {
    let display_counts = node_display_name_counts(&snapshot.nodes);
    let mut vertices = snapshot
        .nodes
        .iter()
        .map(|node| GraphVertex {
            id: node.node_id.clone(),
            label: node_graph_label(node, &display_counts),
            detail: node_graph_detail(node),
            kind: GraphVertexKind::Node,
            availability: node.availability.clone(),
            public_reachable: node.public_reachable,
        })
        .collect::<Vec<_>>();

    let node_ids = vertices
        .iter()
        .map(|vertex| vertex.id.clone())
        .collect::<BTreeSet<_>>();
    let active_links = snapshot
        .links
        .iter()
        .filter(|link| link.state == "healthy" || link.state == "degraded")
        .filter(|link| {
            node_ids.contains(&link.source_node_id) && node_ids.contains(&link.target_node_id)
        })
        .collect::<Vec<_>>();

    let mut direct = BTreeMap::<(String, String), EdgeAccumulator>::new();
    let mut lan_pairs = BTreeSet::new();
    for link in &active_links {
        let pair = ordered_pair(&link.source_node_id, &link.target_node_id);
        if pair.0 == pair.1 {
            continue;
        }
        direct
            .entry(pair.clone())
            .and_modify(|edge| edge.merge(link))
            .or_insert_with(|| EdgeAccumulator::from_link(link));
        if link_is_lan(link) {
            lan_pairs.insert(pair);
        }
    }

    let lan_cliques = maximal_lan_cliques(&node_ids, &lan_pairs);
    let mut collapsed_lan_pairs = BTreeSet::new();
    let mut lan_segments = BTreeMap::new();
    let mut virtual_edges = Vec::new();
    for (index, clique) in lan_cliques.into_iter().enumerate() {
        let segment_id = format!("lan-segment-{}", index + 1);
        for i in 0..clique.len() {
            for j in (i + 1)..clique.len() {
                collapsed_lan_pairs.insert(ordered_pair(&clique[i], &clique[j]));
            }
        }
        vertices.push(GraphVertex {
            id: segment_id.clone(),
            label: format!("LAN {}", index + 1),
            detail: format!("{} nodes", clique.len()),
            kind: GraphVertexKind::LanSegment,
            availability: "online".to_string(),
            public_reachable: false,
        });
        for node_id in &clique {
            virtual_edges.push(GraphEdge {
                a: node_id.clone(),
                b: segment_id.clone(),
                state: "healthy".to_string(),
                public_ip: false,
                virtual_lan: true,
            });
        }
        lan_segments.insert(segment_id, clique);
    }

    let mut edges = Vec::new();
    for ((a, b), edge) in direct {
        if collapsed_lan_pairs.contains(&(a.clone(), b.clone())) && edge.has_lan && !edge.public_ip
        {
            continue;
        }
        edges.push(GraphEdge {
            a,
            b,
            state: edge.state,
            public_ip: edge.public_ip,
            virtual_lan: false,
        });
    }
    edges.extend(virtual_edges);

    Topology {
        vertices,
        edges,
        lan_segments,
    }
}

fn layout_vertices(topology: &Topology, rect: Rect) -> Vec<LayoutVertex> {
    if topology.vertices.is_empty() {
        return Vec::new();
    }
    if topology.vertices.len() == 1 {
        return vec![LayoutVertex {
            vertex: topology.vertices[0].clone(),
            center: Point::new(
                rect.x() + rect.width() / 2.0,
                rect.y() + rect.height() / 2.0,
            ),
            radius: NODE_R,
        }];
    }

    let center = Point::new(
        rect.x() + rect.width() / 2.0,
        rect.y() + rect.height() / 2.0,
    );
    let outer = (rect.width().min(rect.height()) * 0.38).max(72.0);
    let inner = (outer * 0.42).max(36.0);
    let nodes = topology
        .vertices
        .iter()
        .filter(|vertex| vertex.kind == GraphVertexKind::Node)
        .cloned()
        .collect::<Vec<_>>();
    let lans = topology
        .vertices
        .iter()
        .filter(|vertex| vertex.kind == GraphVertexKind::LanSegment)
        .cloned()
        .collect::<Vec<_>>();

    let mut out = Vec::new();
    for (index, vertex) in nodes.iter().enumerate() {
        let angle = angle_for(index, nodes.len(), -std::f32::consts::FRAC_PI_2);
        out.push(LayoutVertex {
            vertex: vertex.clone(),
            center: Point::new(
                center.x + angle.cos() * outer,
                center.y + angle.sin() * outer,
            ),
            radius: NODE_R,
        });
    }
    for (index, vertex) in lans.iter().enumerate() {
        let angle = angle_for(index, lans.len(), 0.0);
        out.push(LayoutVertex {
            vertex: vertex.clone(),
            center: if lans.len() == 1 {
                center
            } else {
                Point::new(
                    center.x + angle.cos() * inner,
                    center.y + angle.sin() * inner,
                )
            },
            radius: LAN_R,
        });
    }
    out
}

fn angle_for(index: usize, len: usize, offset: f32) -> f32 {
    if len == 0 {
        return offset;
    }
    offset + (index as f32 / len as f32) * std::f32::consts::TAU
}

fn hit_test(layout: &[LayoutVertex], point: Point) -> Option<ClusterSelection> {
    layout
        .iter()
        .rev()
        .find(|vertex| distance(vertex.center, point) <= vertex.radius + 6.0)
        .map(|vertex| match vertex.vertex.kind {
            GraphVertexKind::Node => ClusterSelection::Node(vertex.vertex.id.clone()),
            GraphVertexKind::LanSegment => ClusterSelection::LanSegment(vertex.vertex.id.clone()),
        })
}

fn selection_exists(snapshot: &ClusterSnapshot, selection: Option<&ClusterSelection>) -> bool {
    match selection {
        None => true,
        Some(ClusterSelection::Node(id)) => snapshot.nodes.iter().any(|node| &node.node_id == id),
        Some(ClusterSelection::LanSegment(id)) => {
            build_topology(snapshot).lan_segments.contains_key(id)
        }
    }
}

fn distance(a: Point, b: Point) -> f32 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    (dx * dx + dy * dy).sqrt()
}

fn maximal_lan_cliques(
    node_ids: &BTreeSet<String>,
    lan_pairs: &BTreeSet<(String, String)>,
) -> Vec<Vec<String>> {
    let mut adj = node_ids
        .iter()
        .map(|node| (node.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    for (a, b) in lan_pairs {
        adj.entry(a.clone()).or_default().insert(b.clone());
        adj.entry(b.clone()).or_default().insert(a.clone());
    }
    let mut out = Vec::new();
    bron_kerbosch(
        BTreeSet::new(),
        node_ids.clone(),
        BTreeSet::new(),
        &adj,
        &mut out,
    );
    out.retain(|clique| clique.len() >= 3);
    out.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    out
}

fn bron_kerbosch(
    r: BTreeSet<String>,
    mut p: BTreeSet<String>,
    mut x: BTreeSet<String>,
    adj: &BTreeMap<String, BTreeSet<String>>,
    out: &mut Vec<Vec<String>>,
) {
    if p.is_empty() && x.is_empty() {
        out.push(r.into_iter().collect());
        return;
    }
    let candidates = p.clone().into_iter().collect::<Vec<_>>();
    for v in candidates {
        let neighbors = adj.get(&v).cloned().unwrap_or_default();
        let mut next_r = r.clone();
        next_r.insert(v.clone());
        let next_p = p.intersection(&neighbors).cloned().collect();
        let next_x = x.intersection(&neighbors).cloned().collect();
        bron_kerbosch(next_r, next_p, next_x, adj, out);
        p.remove(&v);
        x.insert(v);
    }
}

fn ordered_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn link_is_lan(link: &ClusterLinkInfo) -> bool {
    link.privacy == "lan" || link.transport.starts_with("lan-")
}

fn best_state<'a>(left: &'a str, right: &'a str) -> &'a str {
    match (left, right) {
        ("healthy", _) | (_, "healthy") => "healthy",
        ("degraded", _) | (_, "degraded") => "degraded",
        _ => left,
    }
}

fn node_display_name_counts(nodes: &[ClusterNodeInfo]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for node in nodes {
        let name = node.display_name.trim();
        if !name.is_empty() {
            *counts.entry(name.to_string()).or_insert(0) += 1;
        }
    }
    counts
}

fn node_graph_label(node: &ClusterNodeInfo, display_counts: &BTreeMap<String, usize>) -> String {
    let name = node.display_name.trim();
    if name.is_empty() {
        short(&node.node_id, 11)
    } else if display_counts.get(name).copied().unwrap_or_default() > 1 {
        let suffix = node_id_suffix(&node.node_id, 6);
        let base_max = 17usize.saturating_sub(suffix.chars().count());
        format!("{} {suffix}", short(name, base_max.max(6)))
    } else {
        short(name, 18)
    }
}

fn node_graph_detail(node: &ClusterNodeInfo) -> String {
    let platform = platform_summary(node.platform.as_deref());
    if platform == "-" {
        node.availability.clone()
    } else {
        short(&format!("{} / {platform}", node.availability), 24)
    }
}

fn node_heading(node: &ClusterNodeInfo) -> String {
    let name = node.display_name.trim();
    if name.is_empty() {
        short(&node.node_id, 18)
    } else {
        let suffix = node_id_suffix(&node.node_id, 6);
        format!("{} {suffix}", short(name, 24))
    }
}

fn node_id_suffix(node_id: &str, count: usize) -> String {
    let len = node_id.chars().count();
    node_id
        .chars()
        .skip(len.saturating_sub(count))
        .collect::<String>()
}

fn platform_summary(platform: Option<&str>) -> String {
    let Some(platform) = platform.map(str::trim).filter(|value| !value.is_empty()) else {
        return "-".to_string();
    };
    platform
        .replace("x86_64", "x64")
        .replace("aarch64", "arm64")
}

fn short(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    if max <= 3 {
        return ".".repeat(max);
    }
    let head = max - 3;
    let trimmed = value.chars().take(head).collect::<String>();
    format!("{trimmed}...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::AppState;
    use crate::theme::{AppTextScale, AppThemeMode, resolve_theme, resolve_theme_with_preferences};
    use sinomo_runtime::observe::{
        ObserveHistogramBucket, ObserveMetricAggregation, ObserveMetricDescriptor,
        ObserveMetricExporterStatus, ObserveMetricKind, ObserveMetricRecord,
        ObserveMetricSemantics, ObserveMetricUnit, ObserveMetricsResponse,
    };

    fn node(id: &str) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: id.to_string(),
            display_name: id.to_string(),
            process: "daemon-main".to_string(),
            node_role: "workstation".to_string(),
            member_role: "voter".to_string(),
            member_state: "voter".to_string(),
            availability: "online".to_string(),
            daemon_version: "test".to_string(),
            commit: None,
            platform: None,
            pid: None,
            address: None,
            bind_address: None,
            rollout: None,
            observed_at: "now".to_string(),
            public_reachable: false,
            endpoints: Vec::new(),
        }
    }

    #[test]
    fn vertex_text_lines_follow_extra_large_theme_metrics_without_overlap() {
        let theme = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::ExtraLarge);
        let pal = Palette::from_theme(&theme, true);
        let label = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.text);
        let detail = pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_faint);
        let center = Point::new(120.0, 90.0);
        let layout = vertex_text_layout(center, NODE_R, label.line_height, detail.line_height);
        let label_top = layout.label_center_y - label.line_height * 0.5;
        let label_bottom = layout.label_center_y + label.line_height * 0.5;
        let detail_top = layout.detail_center_y - detail.line_height * 0.5;

        assert_eq!(label_top, center.y + NODE_R + VERTEX_TEXT_TOP_GAP);
        assert!(
            detail_top >= label_bottom + VERTEX_TEXT_LINE_GAP,
            "resolved label and detail line boxes must stay separated"
        );
        assert!(
            layout.detail_center_y > center.y + NODE_R + 40.0,
            "large text should expand the authored canvas spacing"
        );
    }

    fn link(a: &str, b: &str, privacy: &str, address: &str) -> ClusterLinkInfo {
        ClusterLinkInfo {
            source_node_id: a.to_string(),
            target_node_id: b.to_string(),
            endpoint_id: format!("endpoint-{b}"),
            transport: if privacy == "lan" {
                "lan-tcp".to_string()
            } else {
                "iroh".to_string()
            },
            address: address.to_string(),
            privacy: privacy.to_string(),
            state: "healthy".to_string(),
            latency_ms: Some(1),
            consecutive_failures: 0,
            public_ip: privacy == "internet-direct",
        }
    }

    fn endpoint(address: &str, reachable: bool) -> crate::backend::cluster::ClusterEndpointInfo {
        crate::backend::cluster::ClusterEndpointInfo {
            endpoint_id: "endpoint-a".to_string(),
            transport: "iroh".to_string(),
            address: address.to_string(),
            source: "member".to_string(),
            privacy: "internet".to_string(),
            priority: 10,
            latency_ms: Some(42),
            public_ip: true,
            reachable,
        }
    }

    fn test_time(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds)
    }

    fn metric_labels(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn metric_counter(name: &str, value: u64, labels: &[(&str, &str)]) -> ObserveMetricRecord {
        ObserveMetricRecord::counter(name, metric_labels(labels), value)
    }

    fn metric_histogram(
        name: &str,
        count: u64,
        upper_bound: f64,
        labels: &[(&str, &str)],
    ) -> ObserveMetricRecord {
        ObserveMetricRecord {
            name: name.to_string(),
            kind: ObserveMetricKind::Histogram,
            labels: metric_labels(labels),
            value: None,
            count: Some(count),
            sum: Some(upper_bound * count as f64),
            min: Some(upper_bound),
            max: Some(upper_bound),
            buckets: vec![ObserveHistogramBucket { upper_bound, count }],
        }
    }

    fn metrics_response(
        observed_at: OffsetDateTime,
        process_started_at: OffsetDateTime,
        metrics: Vec<ObserveMetricRecord>,
    ) -> ObserveMetricsResponse {
        ObserveMetricsResponse {
            observed_at: Some(observed_at),
            process_started_at: Some(process_started_at),
            process_role: Some("daemon".to_string()),
            node_id: Some("node-a".to_string()),
            exporters: Vec::new(),
            partial: false,
            descriptors: Vec::new(),
            metrics,
        }
    }

    fn sample_batch(
        at: OffsetDateTime,
        node_id: &str,
        response: Result<ObserveMetricsResponse, String>,
    ) -> ObserveSampleBatch {
        ObserveSampleBatch {
            generation: 0,
            requested_at: at,
            received_at: at,
            targets: vec![crate::backend::observe::ObserveTargetSample {
                node_id: node_id.to_string(),
                response,
            }],
        }
    }

    fn seed_performance_dashboard(shared: &Shared) {
        let now = OffsetDateTime::now_utc();
        let mut online = node("node-edge-east");
        online.display_name = "edge-east".to_string();
        let mut partial = node("node-compute-02");
        partial.display_name = "compute-02".to_string();
        let mut offline = node("node-offline-lab");
        offline.display_name = "offline-lab".to_string();
        offline.availability = "offline".to_string();

        let mut cluster = shared.cluster_mut();
        cluster.snapshot = ClusterSnapshot {
            nodes: vec![online, partial, offline],
            local_node_id: Some("node-edge-east".to_string()),
            source: "visual performance fixture".to_string(),
            observed_at: now.to_string(),
            ..ClusterSnapshot::default()
        };
        cluster.observation.in_flight = false;
        cluster.observation.history = (0..180)
            .map(|index| {
                let index = index as f64;
                DashboardPoint {
                    coverage: DashboardMetric::ALL
                        .into_iter()
                        .map(|metric| (metric, 2))
                        .collect(),
                    at: now - time::Duration::seconds((179.0 - index) as i64 * 5),
                    values: BTreeMap::from([
                        (DashboardMetric::RpcRate, Some(18.0 + index * 1.7)),
                        (
                            DashboardMetric::RpcErrorRate,
                            Some(if index % 6.0 == 0.0 { 0.8 } else { 0.15 }),
                        ),
                        (DashboardMetric::RpcP95Ms, Some(22.0 + (index % 5.0) * 3.4)),
                        (DashboardMetric::KvP95Ms, Some(7.0 + (index % 4.0) * 1.8)),
                        (
                            DashboardMetric::DfsBacklog,
                            Some((14.0 - index * 0.6).max(2.0)),
                        ),
                        (DashboardMetric::AgentActiveTurns, Some(2.0 + (index % 5.0))),
                        (DashboardMetric::AgentPendingTasks, Some(3.0 + index % 7.0)),
                        (DashboardMetric::AgentFailedTasks, Some(1.0)),
                        (DashboardMetric::DfsPendingWrites, Some(index % 4.0)),
                        (DashboardMetric::CpuPercent, Some(55.0 + index % 20.0)),
                        (DashboardMetric::MemoryPercent, Some(68.0 + index % 5.0)),
                        (DashboardMetric::DiskFreePercent, Some(35.0 - index * 0.02)),
                        (
                            DashboardMetric::NetworkRxRate,
                            Some(1_000_000.0 + (index % 5.0) * 300_000.0),
                        ),
                        (
                            DashboardMetric::NetworkTxRate,
                            Some(650_000.0 + (index % 3.0) * 200_000.0),
                        ),
                        (DashboardMetric::KvQueueDepth, Some(index % 3.0)),
                        (DashboardMetric::KvReplicaLag, Some(index % 8.0)),
                        (DashboardMetric::DfsReplicaLag, Some(index % 6.0)),
                        (DashboardMetric::DfsFailedJobs, Some(0.0)),
                        (
                            DashboardMetric::AgentBufferedEvents,
                            Some(24.0 + index % 20.0),
                        ),
                        (DashboardMetric::ExporterErrors, Some(1.0)),
                    ]),
                }
            })
            .collect();
        cluster.observation.targets = BTreeMap::from([
            (
                "node-edge-east".to_string(),
                TargetObservationSummary {
                    node_id: "node-edge-east".to_string(),
                    observed_at: Some(now - time::Duration::seconds(1)),
                    received_at: now,
                    partial: false,
                    error: None,
                    rpc_rate: Some(31.4),
                    rpc_p95_ms: Some(28.2),
                    kv_queue_depth: Some(1.0),
                    dfs_backlog: Some(2.0),
                    failed_tasks: Some(0.0),
                },
            ),
            (
                "node-compute-02".to_string(),
                TargetObservationSummary {
                    node_id: "node-compute-02".to_string(),
                    observed_at: Some(now - time::Duration::seconds(2)),
                    received_at: now,
                    partial: true,
                    error: None,
                    rpc_rate: Some(15.5),
                    rpc_p95_ms: Some(34.7),
                    kv_queue_depth: Some(3.0),
                    dfs_backlog: Some(2.0),
                    failed_tasks: Some(1.0),
                },
            ),
            (
                "node-offline-lab".to_string(),
                TargetObservationSummary {
                    node_id: "node-offline-lab".to_string(),
                    observed_at: Some(now - time::Duration::seconds(18)),
                    received_at: now,
                    partial: false,
                    error: Some("observe route unavailable".to_string()),
                    rpc_rate: None,
                    rpc_p95_ms: None,
                    kv_queue_depth: None,
                    dfs_backlog: None,
                    failed_tasks: None,
                },
            ),
        ]);
        cluster.observation.activity = VecDeque::from([
            ObservationActivity {
                at: now - time::Duration::seconds(12),
                node_id: "node-compute-02".to_string(),
                detail: "metric snapshot was partial; failed exporters: agent-runtime".to_string(),
                warning: true,
            },
            ObservationActivity {
                at: now - time::Duration::seconds(7),
                node_id: "node-edge-east".to_string(),
                detail: "process restarted; interval baseline reset".to_string(),
                warning: false,
            },
            ObservationActivity {
                at: now - time::Duration::seconds(2),
                node_id: "node-offline-lab".to_string(),
                detail: "metrics unavailable: observe route unavailable".to_string(),
                warning: true,
            },
        ]);
        cluster.observation.metrics = seeded_metric_store(now);
        cluster.observation.revision = cluster.observation.revision.wrapping_add(1);
    }

    fn seeded_metric_store(now: OffsetDateTime) -> MetricStore {
        let descriptor = |name, kind, unit, help: &str, domain: &str, labels: &[&str]| {
            ObserveMetricDescriptor::new(name, kind, Some(unit), help).with_semantics(
                ObserveMetricSemantics::new(
                    domain,
                    match kind {
                        ObserveMetricKind::Counter => ObserveMetricAggregation::Sum,
                        ObserveMetricKind::Histogram => ObserveMetricAggregation::MergeHistogram,
                        ObserveMetricKind::Gauge => ObserveMetricAggregation::None,
                    },
                    labels.iter().copied(),
                ),
            )
        };
        let descriptors = vec![
            descriptor(
                "sinomo.daemon.service.requests",
                ObserveMetricKind::Counter,
                ObserveMetricUnit::Count,
                "Daemon object RPC requests.",
                "daemon",
                &["route", "category", "outcome"],
            ),
            descriptor(
                "sinomo.daemon.service.total.us",
                ObserveMetricKind::Histogram,
                ObserveMetricUnit::Microseconds,
                "End-to-end daemon service request latency.",
                "daemon",
                &["route", "category", "outcome"],
            ),
            descriptor(
                "sinomo.kv.commit.queue_depth",
                ObserveMetricKind::Gauge,
                ObserveMetricUnit::Count,
                "Cluster KV commit queue depth.",
                "kv",
                &[],
            ),
            descriptor(
                "sinomo.dfs.pending_repairs",
                ObserveMetricKind::Gauge,
                ObserveMetricUnit::Count,
                "DFS repairs waiting for reconciliation.",
                "dfs",
                &[],
            ),
            descriptor(
                "sinomo.agent.active_turns",
                ObserveMetricKind::Gauge,
                ObserveMetricUnit::Count,
                "Agent turns currently active.",
                "agent",
                &[],
            ),
        ];
        let process_started_at = now - time::Duration::minutes(10);
        let mut store = MetricStore::default();
        store.set_history_scope(
            Some("sinomo.daemon.service.requests".to_string()),
            None,
            BTreeMap::new(),
        );
        for index in 0..18_u64 {
            let at = now - time::Duration::seconds((17 - index as i64) * 5);
            let make_response = |node_id: &str, base: u64, step: u64| {
                let phase = if node_id == "node-compute-02" { 2 } else { 0 };
                let request_total = (0..=index)
                    .map(|sample| step + ((sample + phase) % 5) * 2)
                    .sum::<u64>();
                let error_total = (0..=index)
                    .map(|sample| if (sample + phase) % 4 == 0 { 2 } else { 0 })
                    .sum::<u64>();
                ObserveMetricsResponse {
                    observed_at: Some(at),
                    process_started_at: Some(process_started_at),
                    process_role: Some("daemon".to_string()),
                    node_id: Some(node_id.to_string()),
                    exporters: Vec::new(),
                    partial: false,
                    descriptors: descriptors.clone(),
                    metrics: vec![
                        metric_counter(
                            "sinomo.daemon.service.requests",
                            base + request_total,
                            &[
                                ("route", "/objects"),
                                ("category", "object"),
                                ("outcome", "ok"),
                            ],
                        ),
                        metric_counter(
                            "sinomo.daemon.service.requests",
                            base / 20 + error_total,
                            &[
                                ("route", "/objects"),
                                ("category", "object"),
                                ("outcome", "error"),
                            ],
                        ),
                        metric_histogram(
                            "sinomo.daemon.service.total.us",
                            100 + index * 10,
                            50_000.0,
                            &[
                                ("route", "/objects"),
                                ("category", "object"),
                                ("outcome", "ok"),
                            ],
                        ),
                        ObserveMetricRecord::gauge(
                            "sinomo.kv.commit.queue_depth",
                            BTreeMap::new(),
                            (index % 5) as f64,
                        ),
                        ObserveMetricRecord::gauge(
                            "sinomo.dfs.pending_repairs",
                            BTreeMap::new(),
                            (8_u64.saturating_sub(index / 3)) as f64,
                        ),
                        ObserveMetricRecord::gauge(
                            "sinomo.agent.active_turns",
                            BTreeMap::new(),
                            2.0 + (index % 4) as f64,
                        ),
                    ],
                }
            };
            let edge = make_response("node-edge-east", 1_000, 20);
            let compute = make_response("node-compute-02", 500, 12);
            store.ingest_batch(
                at,
                [("node-edge-east", &edge), ("node-compute-02", &compute)],
            );
        }
        store
    }

    #[test]
    fn observation_uses_first_sample_as_baseline_and_resets_after_restart() {
        let process_started_at = test_time(1);
        let labels = [
            ("category", "object"),
            ("route", "/objects"),
            ("outcome", "ok"),
        ];
        let mut state = ClusterObservationState::default();

        apply_observation_batch(
            &mut state,
            sample_batch(
                test_time(10),
                "node-a",
                Ok(metrics_response(
                    test_time(10),
                    process_started_at,
                    vec![metric_counter(
                        "sinomo.daemon.service.requests",
                        1_000_000,
                        &labels,
                    )],
                )),
            ),
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            None,
            "a cumulative first snapshot must not render as an interval spike"
        );

        apply_observation_batch(
            &mut state,
            sample_batch(
                test_time(15),
                "node-a",
                Ok(metrics_response(
                    test_time(15),
                    process_started_at,
                    vec![metric_counter(
                        "sinomo.daemon.service.requests",
                        1_000_030,
                        &labels,
                    )],
                )),
            ),
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            Some(6.0)
        );

        apply_observation_batch(
            &mut state,
            sample_batch(
                test_time(20),
                "node-a",
                Ok(metrics_response(
                    test_time(20),
                    test_time(19),
                    vec![metric_counter(
                        "sinomo.daemon.service.requests",
                        4_000_000,
                        &labels,
                    )],
                )),
            ),
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            None,
            "a restarted process must establish a new interval baseline"
        );
        assert!(state.activity.iter().any(|entry| {
            entry.node_id == "node-a"
                && entry.detail == "process restarted; interval baseline reset"
                && !entry.warning
        }));

        apply_observation_batch(
            &mut state,
            sample_batch(
                test_time(25),
                "node-a",
                Ok(metrics_response(
                    test_time(25),
                    test_time(19),
                    vec![metric_counter("sinomo.daemon.service.requests", 2, &labels)],
                )),
            ),
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            None,
            "a same-process counter reset must produce a gap, not a cumulative spike"
        );

        let mut unknown_process = metrics_response(
            test_time(30),
            test_time(19),
            vec![metric_counter(
                "sinomo.daemon.service.requests",
                20,
                &labels,
            )],
        );
        unknown_process.process_started_at = None;
        apply_observation_batch(
            &mut state,
            sample_batch(test_time(30), "node-a", Ok(unknown_process)),
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            None,
            "a process-identity metadata transition must reset the interval baseline"
        );
    }

    #[test]
    fn overview_non_forward_sample_does_not_replace_the_rate_baseline() {
        let process_started_at = test_time(1);
        let labels = [
            ("category", "object"),
            ("route", "/objects"),
            ("outcome", "ok"),
        ];
        let mut state = ClusterObservationState::default();
        for (received, observed, value) in [(10, 10, 100), (15, 9, 150), (20, 20, 200)] {
            apply_observation_batch(
                &mut state,
                sample_batch(
                    test_time(received),
                    "node-a",
                    Ok(metrics_response(
                        test_time(observed),
                        process_started_at,
                        vec![metric_counter(
                            "sinomo.daemon.service.requests",
                            value,
                            &labels,
                        )],
                    )),
                ),
            );
        }
        assert_eq!(
            state.history[state.history.len() - 2].values[&DashboardMetric::RpcRate],
            None,
            "a non-forward observation must create an interval gap"
        );
        assert_eq!(
            state.history.back().unwrap().values[&DashboardMetric::RpcRate],
            Some(10.0),
            "the next interval must use the last forward baseline"
        );
    }

    #[test]
    fn observation_history_keeps_only_the_latest_180_samples() {
        let mut state = ClusterObservationState::default();
        for seconds in 0..(OBSERVATION_HISTORY_LIMIT as i64 + 5) {
            let at = test_time(seconds);
            apply_observation_batch(
                &mut state,
                sample_batch(
                    at,
                    "node-a",
                    Ok(metrics_response(at, test_time(-1), Vec::new())),
                ),
            );
        }

        assert_eq!(state.history.len(), OBSERVATION_HISTORY_LIMIT);
        assert_eq!(state.history.front().unwrap().at, test_time(5));
        assert_eq!(
            state.history.back().unwrap().at,
            test_time(OBSERVATION_HISTORY_LIMIT as i64 + 4)
        );
    }

    #[test]
    fn observation_state_prunes_nodes_missing_from_the_latest_batch() {
        let mut state = ClusterObservationState::default();
        let mut first = sample_batch(
            test_time(1),
            "node-a",
            Ok(metrics_response(test_time(1), test_time(0), Vec::new())),
        );
        first
            .targets
            .push(crate::backend::observe::ObserveTargetSample {
                node_id: "node-b".to_string(),
                response: Ok(metrics_response(test_time(1), test_time(0), Vec::new())),
            });
        apply_observation_batch(&mut state, first);
        assert_eq!(state.targets.len(), 2);
        assert_eq!(state.previous.len(), 2);

        apply_observation_batch(
            &mut state,
            sample_batch(
                test_time(2),
                "node-b",
                Ok(metrics_response(test_time(2), test_time(0), Vec::new())),
            ),
        );

        assert_eq!(
            state.targets.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["node-b"]
        );
        assert_eq!(
            state
                .previous
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["node-b"]
        );
    }

    #[test]
    fn dashboard_rates_and_latency_exclude_observation_self_traffic() {
        let user_labels = [
            ("category", "object"),
            ("route", "/objects"),
            ("outcome", "ok"),
        ];
        let user_error_labels = [
            ("category", "object"),
            ("route", "/objects"),
            ("outcome", "error"),
        ];
        let observe_labels = [
            ("category", "observe"),
            ("route", "/observe/metrics"),
            ("outcome", "ok"),
        ];
        let interval = metrics_response(
            test_time(5),
            test_time(0),
            vec![
                metric_counter("sinomo.daemon.service.requests", 20, &user_labels),
                metric_counter("sinomo.daemon.service.requests", 5, &user_error_labels),
                metric_counter("sinomo.daemon.service.requests", 10_000, &observe_labels),
                metric_histogram("sinomo.daemon.service.total.us", 25, 1_000.0, &user_labels),
                metric_histogram(
                    "sinomo.daemon.service.total.us",
                    10_000,
                    1_000_000.0,
                    &observe_labels,
                ),
            ],
        );
        let values = dashboard_values(&interval, Some(&interval), Some(5.0));

        assert_eq!(values[&DashboardMetric::RpcRate], Some(5.0));
        assert_eq!(values[&DashboardMetric::RpcErrorRate], Some(1.0));
        assert_eq!(values[&DashboardMetric::RpcP95Ms], Some(1.0));
    }

    #[test]
    fn dashboard_values_reject_incompatible_kinds_and_units() {
        let labels = [
            ("category", "object"),
            ("route", "/objects"),
            ("outcome", "ok"),
        ];
        let mut wrong_kind = metrics_response(
            test_time(5),
            test_time(0),
            vec![ObserveMetricRecord::gauge(
                "sinomo.daemon.service.requests",
                metric_labels(&labels),
                25.0,
            )],
        );
        wrong_kind.descriptors.push(ObserveMetricDescriptor::new(
            "sinomo.daemon.service.requests",
            ObserveMetricKind::Gauge,
            Some(ObserveMetricUnit::Count),
            "Malformed request gauge.",
        ));
        assert_eq!(
            dashboard_values(&wrong_kind, Some(&wrong_kind), Some(5.0))[&DashboardMetric::RpcRate],
            None
        );

        let mut wrong_unit = metrics_response(
            test_time(10),
            test_time(0),
            vec![metric_counter(
                "sinomo.daemon.service.requests",
                50,
                &labels,
            )],
        );
        wrong_unit.descriptors.push(ObserveMetricDescriptor::new(
            "sinomo.daemon.service.requests",
            ObserveMetricKind::Counter,
            Some(ObserveMetricUnit::Bytes),
            "Malformed request bytes.",
        ));
        assert_eq!(
            dashboard_values(&wrong_unit, Some(&wrong_unit), Some(5.0))[&DashboardMetric::RpcRate],
            None
        );
    }

    #[test]
    fn partial_samples_remain_useful_and_collection_errors_are_bounded() {
        let at = test_time(20);
        let mut partial = metrics_response(
            at,
            test_time(1),
            vec![
                ObserveMetricRecord::gauge("sinomo.agent.active_turns", BTreeMap::new(), 3.0),
                ObserveMetricRecord::gauge("sinomo.dfs.pending_repairs", BTreeMap::new(), 2.0),
                ObserveMetricRecord::gauge("sinomo.kv.commit.queue_depth", BTreeMap::new(), 4.0),
            ],
        );
        partial.partial = true;
        partial.exporters = vec![ObserveMetricExporterStatus {
            name: "agent-runtime".to_string(),
            observed_at: at,
            duration_us: 12,
            cached: false,
            success: false,
            error_code: Some("temporarily-unavailable".to_string()),
        }];
        let long_error = "route unavailable ".repeat(40);
        let mut state = ClusterObservationState::default();
        let mut batch = sample_batch(at, "node-partial", Ok(partial));
        batch
            .targets
            .push(crate::backend::observe::ObserveTargetSample {
                node_id: "node-offline".to_string(),
                response: Err(long_error),
            });

        apply_observation_batch(&mut state, batch);

        let latest = state.history.back().unwrap();
        assert_eq!(
            latest.values[&DashboardMetric::AgentActiveTurns],
            Some(3.0),
            "successful metric families from a partial snapshot stay visible"
        );
        assert_eq!(latest.values[&DashboardMetric::DfsBacklog], Some(2.0));
        assert!(state.targets["node-partial"].partial);
        assert_eq!(state.targets["node-partial"].kv_queue_depth, Some(4.0));
        assert!(state.targets["node-offline"].error.is_some());
        assert!(
            state.targets["node-offline"]
                .error
                .as_ref()
                .unwrap()
                .chars()
                .count()
                <= 240
        );
        assert_eq!(state.activity.len(), 2);
        assert!(state.activity.iter().all(|entry| {
            entry.warning && entry.detail.chars().count() <= OBSERVATION_ACTIVITY_DETAIL_LIMIT
        }));
    }

    fn cluster_test_app(shared: &Shared) -> sui::Result<sui_testing::TestApp> {
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        app.main_window()?
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 720.0,
            ))))?;
        app.run_until_idle()?;
        Ok(app)
    }

    #[test]
    fn offline_cluster_shows_the_daemon_panel_for_every_section() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(crate::ui::offline::offline_test_backend()));
        let app = cluster_test_app(&shared)?;
        let window = app.main_window()?;
        window
            .get_by_role(SemanticsRole::Text)
            .with_name(CLUSTER_OFFLINE_SUMMARY)
            .expect()
            .to_be_visible()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::TabBar)
                .with_name("cluster-sections")
                .count()?,
            0,
            "the section tabs belong to the cluster surface, which is hidden while offline"
        );
        Ok(())
    }

    #[test]
    fn overview_without_nodes_or_metrics_shows_the_cluster_empty_state() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let app = cluster_test_app(&shared)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| { node.name.as_deref() == Some("Cluster empty state") }),
            "an empty overview explains itself instead of drawing blank cards"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| { node.name.as_deref() == Some("Cluster performance overview") }),
            "the dashboard waits until there is something to chart"
        );

        seed_performance_dashboard(&shared);
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| { node.name.as_deref() == Some("Cluster performance overview") }),
            "reported metrics bring the dashboard back"
        );
        Ok(())
    }

    #[test]
    fn overview_metric_cards_wrap_without_viewport_height_stretch() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster performance layout")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 720.0,
            ))))?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let card_tops = [
            DashboardMetric::AgentActiveTurns,
            DashboardMetric::AgentPendingTasks,
            DashboardMetric::AgentFailedTasks,
            DashboardMetric::DfsBacklog,
        ]
        .into_iter()
        .map(|metric| {
            snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == SemanticsRole::GenericContainer
                        && node.name.as_deref() == Some(metric.title())
                })
                .unwrap_or_else(|| panic!("missing {} card semantics", metric.title()))
                .bounds
                .y()
        })
        .collect::<Vec<_>>();

        assert_eq!(card_tops.len(), 4);
        let first = card_tops.iter().copied().reduce(f32::min).unwrap();
        let last = card_tops.iter().copied().reduce(f32::max).unwrap();
        assert!(
            (280.0..420.0).contains(&(last - first)),
            "the storage section should follow the compact agent chart row: {card_tops:?}"
        );
        Ok(())
    }

    #[test]
    fn dashboard_time_windows_are_fixed_and_missing_gauges_stay_unknown() {
        let end = test_time(1000);
        let theme = resolve_theme(AppThemeMode::Dark);
        let chart = MetricChart::new(
            "sinomo.daemon.service.requests".into(),
            vec![],
            theme,
            Palette::from_theme(&theme, true).agent,
            Some(ObserveMetricUnit::Count),
            ExplorerTransform::Rate,
            ObserveMetricKind::Counter,
        )
        .time_range(end, ExplorerWindow::FiveMinutes, false);
        assert_eq!(chart.x_fraction(end - time::Duration::minutes(5)), 0.0);
        assert_eq!(chart.x_fraction(end - time::Duration::seconds(150)), 0.5);
        assert!((chart.x_fraction(end - time::Duration::seconds(5)) - 59.0 / 60.0).abs() < 0.0001);
        assert_eq!(
            DashboardMetric::CpuPercent.window(),
            ExplorerWindow::OneMinute
        );
        assert_eq!(
            DashboardMetric::DfsBacklog.window(),
            ExplorerWindow::FifteenMinutes
        );
        let empty = metrics_response(end, test_time(0), Vec::new());
        assert!(
            dashboard_values(&empty, None, None)
                .values()
                .all(Option::is_none)
        );
    }

    #[test]
    fn dashboard_snapshot_counts_are_not_added_across_replicas() {
        let at = test_time(10);
        let response = metrics_response(
            at,
            test_time(0),
            vec![
                ObserveMetricRecord::gauge("sinomo.agent.active_turns", BTreeMap::new(), 3.0),
                ObserveMetricRecord::gauge("sinomo.dfs.pending_repairs", BTreeMap::new(), 4.0),
                ObserveMetricRecord::gauge("sinomo.node.memory.used.bytes", BTreeMap::new(), 80.0),
                ObserveMetricRecord::gauge(
                    "sinomo.node.memory.total.bytes",
                    BTreeMap::new(),
                    100.0,
                ),
                ObserveMetricRecord::gauge(
                    "sinomo.node.data_disk.available.bytes",
                    BTreeMap::new(),
                    20.0,
                ),
                ObserveMetricRecord::gauge(
                    "sinomo.node.data_disk.total.bytes",
                    BTreeMap::new(),
                    100.0,
                ),
            ],
        );
        let mut batch = sample_batch(at, "one", Ok(response.clone()));
        batch
            .targets
            .push(crate::backend::observe::ObserveTargetSample {
                node_id: "two".into(),
                response: Ok(response),
            });
        let mut observation = ClusterObservationState::default();
        apply_observation_batch(&mut observation, batch);
        let latest = observation.history.back().unwrap();
        assert_eq!(latest.values[&DashboardMetric::AgentActiveTurns], Some(3.0));
        assert_eq!(latest.values[&DashboardMetric::DfsBacklog], Some(4.0));
        assert_eq!(latest.values[&DashboardMetric::MemoryPercent], Some(80.0));
        assert_eq!(latest.values[&DashboardMetric::DiskFreePercent], Some(20.0));
        assert_eq!(latest.coverage[&DashboardMetric::DfsBacklog], 2);
    }

    #[test]
    fn dashboard_pause_freezes_history_and_resume_restarts_interval_baselines() {
        let mut state = ClusterState::new(&Backend::local_fallback());
        let at = OffsetDateTime::now_utc();
        let response = metrics_response(
            at,
            at,
            vec![metric_counter("sinomo.daemon.service.requests", 100, &[])],
        );
        apply_observation_batch(
            &mut state.observation,
            sample_batch(at, "node", Ok(response)),
        );
        state.set_observation_live(false);
        let frozen = state.observation_time();
        let history = state.observation.history.clone();
        let (tx, rx) = mpsc::channel();
        state.observation.sample_rx = Some(rx);
        state.observation.in_flight = true;
        tx.send(sample_batch(
            at + time::Duration::seconds(5),
            "node",
            Err("unavailable".into()),
        ))
        .unwrap();
        state.pump_observation();
        assert_eq!(state.observation.history, history);
        assert_eq!(state.observation_time(), frozen);
        state.set_observation_live(true);
        assert!(state.observation.previous.is_empty());
        let fresh = at + time::Duration::seconds(10);
        apply_observation_batch(
            &mut state.observation,
            sample_batch(
                fresh,
                "node",
                Ok(metrics_response(
                    fresh,
                    at,
                    vec![metric_counter("sinomo.daemon.service.requests", 900, &[])],
                )),
            ),
        );
        assert_eq!(
            state.observation.history.back().unwrap().values[&DashboardMetric::RpcRate],
            None
        );

        let old_generation = state.observation.generation;
        state.set_observation_live(false);
        state.set_observation_live(true);
        let resumed = state.observation_time();
        let history = state.observation.history.clone();
        let (tx, rx) = mpsc::channel();
        state.observation.sample_rx = Some(rx);
        state.observation.in_flight = true;
        let mut late = sample_batch(at, "node", Err("old request".into()));
        late.generation = old_generation;
        tx.send(late).unwrap();
        state.pump_observation();
        assert!(!state.observation.in_flight);
        assert_eq!(state.observation_time(), resumed);
        assert_eq!(state.observation.history, history);
    }

    #[test]
    fn dashboard_refresh_keeps_scroll_and_inspect_controls_mounted() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.set_theme_mode(AppThemeMode::Dark);
        shared.set_text_scale(AppTextScale::Standard);
        seed_performance_dashboard(&shared);
        let app = sui_testing::TestApp::from_runtime(
            sui::Application::new()
                .window(sui::WindowBuilder::new().root(view(shared.clone())))
                .build()?,
        )?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 900.0,
            ))))?;
        let before = window.snapshot()?;
        let inspect_id = before
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Inspect Active agent turns"))
            .unwrap()
            .id;
        let pause_id = before
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Pause live metrics"))
            .unwrap()
            .id;
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Cluster performance overview")
            .scroll_pixels(sui::Vector::new(0.0, -400.0))?;
        let scrolled = window.snapshot()?;
        let before_y = scrolled
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Active agent turns"))
            .unwrap()
            .bounds
            .y();
        {
            let mut cluster = shared.cluster_mut();
            cluster.observation.revision += 1;
            cluster.observation.timeline_at =
                Some(cluster.observation_time() + time::Duration::seconds(5));
        }
        let after = window.snapshot()?;
        let inspect = after
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Inspect Active agent turns"))
            .unwrap();
        assert_eq!(inspect.id, inspect_id);
        assert_eq!(
            after
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Pause live metrics"))
                .unwrap()
                .id,
            pause_id
        );
        assert_eq!(
            after
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Active agent turns"))
                .unwrap()
                .bounds
                .y(),
            before_y
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::GenericContainer)
                .with_name("KV max-node p95")
                .count()?,
            0
        );
        if std::env::var_os("SINOMO_DASHBOARD_SHOTS").is_some() {
            window
                .get_by_role(SemanticsRole::ScrollView)
                .with_name("Cluster performance overview")
                .scroll_pixels(sui::Vector::new(0.0, 10000.0))?;
            let dir =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/visual-inspection");
            std::fs::create_dir_all(&dir).unwrap();
            for (width, theme, scale, name) in [
                (1280.0, AppThemeMode::Dark, AppTextScale::Standard, "1280"),
                (390.0, AppThemeMode::Dark, AppTextScale::Standard, "390"),
                (
                    1280.0,
                    AppThemeMode::Light,
                    AppTextScale::Large,
                    "light-large",
                ),
            ] {
                shared.set_theme_mode(theme);
                shared.set_text_scale(scale);
                window
                    .root()
                    .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                        width, 900.0,
                    ))))?;
                window
                    .capture_screenshot()?
                    .write_png(dir.join(format!("observation-dashboard-{name}.png")))?;
            }
        }
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Cluster performance overview")
            .scroll_pixels(sui::Vector::new(0.0, 10000.0))?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Pause live metrics")
            .click()?;
        assert!(shared.cluster().observation.paused_at.is_some());
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Resume live metrics")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Cluster performance overview")
            .scroll_pixels(sui::Vector::new(0.0, -10000.0))?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show diagnostics")
            .click()?;
        assert!(shared.cluster().observation.dashboard_diagnostics);
        assert_eq!(
            window
                .get_by_role(SemanticsRole::GenericContainer)
                .with_name("KV max-node p95")
                .count()?,
            1
        );
        if std::env::var_os("SINOMO_DASHBOARD_SHOTS").is_some() {
            window
                .get_by_role(SemanticsRole::ScrollView)
                .with_name("Cluster performance overview")
                .scroll_pixels(sui::Vector::new(0.0, -500.0))?;
            window.capture_screenshot()?.write_png(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("target/visual-inspection/observation-dashboard-diagnostics.png"),
            )?;
        }
        Ok(())
    }

    #[test]
    fn metric_explorer_sampling_preserves_query_controls_and_focus() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        shared.cluster_mut().subview = ClusterSubview::Explore;
        let app = sui_testing::TestApp::from_runtime(
            sui::Application::new()
                .window(sui::WindowBuilder::new().root(view(shared.clone())))
                .build()?,
        )?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 900.0,
            ))))?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("sinomo.daemon.service.requests")
            .click()?;
        let filter = window
            .get_by_role(SemanticsRole::ComboBox)
            .with_name("Metric node scope");
        filter.focus()?;
        let before = window.snapshot()?;
        let summary = before
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Metric current values"))
            .expect("current values panel");
        assert!(
            summary.bounds.height() < 140.0,
            "current values must remain compact so the plot stays within reach"
        );
        let control_id = before
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Metric node scope"))
            .unwrap()
            .id;
        {
            let mut cluster = shared.cluster_mut();
            cluster.observation.revision += 1;
            cluster.observation.timeline_at =
                Some(cluster.observation_time() + time::Duration::seconds(5));
        }
        let after = window.snapshot()?;
        assert_eq!(
            control_id,
            after
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Metric node scope"))
                .unwrap()
                .id
        );
        filter.expect().to_be_focused()?;
        Ok(())
    }

    fn dense_explorer_sample(at: OffsetDateTime, tick: u64) -> ObserveMetricsResponse {
        let mut response = metrics_response(at, test_time(0), Vec::new());
        for family in 0..128 {
            let name = format!("sinomo.load.metric_{family:03}");
            response.descriptors.push(ObserveMetricDescriptor::new(
                &name,
                ObserveMetricKind::Counter,
                Some(ObserveMetricUnit::Count),
                "Bounded operational metric with per-route dimensions for explorer profiling.",
            ));
            for route in 0..32 {
                response.metrics.push(ObserveMetricRecord::counter(
                    &name,
                    BTreeMap::from([("route".into(), format!("/route/{route:02}"))]),
                    100 + tick * 10 + route,
                ));
            }
        }
        response
    }

    #[test]
    fn explorer_catalog_is_virtualized_and_survives_live_samples() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let at = OffsetDateTime::now_utc();
        {
            let mut cluster = shared.cluster_mut();
            cluster.inspect_explorer_metric(
                "sinomo.load.metric_000",
                ExplorerTransform::Rate,
                std::iter::empty(),
            );
            apply_observation_batch(
                &mut cluster.observation,
                sample_batch(at, "node", Ok(dense_explorer_sample(at, 0))),
            );
        }
        let mut runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().root(view(shared.clone())))
            .build()?;
        let window = runtime.window_ids()[0];
        runtime.handle_event(
            window,
            Event::Window(sui::WindowEvent::Resized(Size::new(1280.0, 900.0))),
        )?;
        runtime.render(window)?;
        let buttons = |runtime: &sui::Runtime| {
            runtime
                .semantics(window)
                .unwrap()
                .iter()
                .filter(|node| {
                    node.role == SemanticsRole::Button
                        && node
                            .name
                            .as_deref()
                            .is_some_and(|name| name.starts_with("sinomo.load."))
                })
                .map(|node| (node.name.clone().unwrap(), (node.id, node.bounds.y())))
                .collect::<BTreeMap<_, _>>()
        };
        let before = buttons(&runtime);
        assert!(!before.is_empty());
        assert!(
            before.len() < 40,
            "offscreen catalog rows must not be realized"
        );
        let list = runtime
            .semantics(window)?
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List
                    && node.name.as_deref() == Some("Registered metric catalog")
            })
            .unwrap()
            .bounds;
        let point = Point::new(
            list.x() + list.width() / 2.0,
            list.y() + list.height() / 2.0,
        );
        runtime.handle_event(
            window,
            Event::Pointer(sui::PointerEvent::new(PointerEventKind::Move, point)),
        )?;
        let mut scroll = sui::PointerEvent::new(PointerEventKind::Scroll, point);
        scroll.scroll_delta = Some(sui::ScrollDelta::Pixels(sui::Vector::new(0.0, -400.0)));
        runtime.handle_event(window, Event::Pointer(scroll))?;
        runtime.render(window)?;
        let scrolled = buttons(&runtime);
        assert_ne!(scrolled, before, "fixture must actually scroll the catalog");
        let next = at + time::Duration::seconds(5);
        {
            let mut cluster = shared.cluster_mut();
            apply_observation_batch(
                &mut cluster.observation,
                sample_batch(next, "node", Ok(dense_explorer_sample(next, 1))),
            );
            cluster.observation.revision += 1;
        }
        runtime.render(window)?;
        assert_eq!(
            buttons(&runtime),
            scrolled,
            "a live sample must retain catalog row identity and scroll position"
        );
        let chart_id = |runtime: &sui::Runtime| {
            runtime
                .semantics(window)
                .unwrap()
                .iter()
                .find(|node| node.name.as_deref() == Some("sinomo.load.metric_000 live history"))
                .unwrap()
                .id
        };
        let chart_before = chart_id(&runtime);
        shared.cluster_mut().set_explorer_search("metric_00".into());
        runtime.render(window)?;
        assert_eq!(
            chart_id(&runtime),
            chart_before,
            "catalog search must not rebuild the live query"
        );
        Ok(())
    }

    #[test]
    fn dashboard_baselines_keep_curated_inputs_without_dropping_explorer_metrics() {
        let mut state = ClusterObservationState::default();
        for tick in 0..2 {
            let at = test_time(10 + tick * 5);
            let mut response = dense_explorer_sample(at, tick as u64);
            response.metrics.extend([
                ObserveMetricRecord::gauge("sinomo.node.memory.used.bytes", BTreeMap::new(), 80.0),
                ObserveMetricRecord::gauge(
                    "sinomo.node.memory.total.bytes",
                    BTreeMap::new(),
                    100.0,
                ),
                metric_counter(
                    "sinomo.daemon.service.requests",
                    100 + tick as u64 * 10,
                    &[],
                ),
            ]);
            apply_observation_batch(&mut state, sample_batch(at, "node", Ok(response)));
        }
        assert!(
            state
                .metrics
                .catalog_entry("sinomo.load.metric_000")
                .is_some()
        );
        assert_eq!(state.previous["node"].0.metrics.len(), 3);
        let point = state.history.back().unwrap();
        assert_eq!(point.values[&DashboardMetric::MemoryPercent], Some(80.0));
        assert_eq!(point.values[&DashboardMetric::RpcRate], Some(2.0));
    }

    #[test]
    #[ignore = "headless performance probe; run explicitly with --ignored --nocapture"]
    fn profile_metric_explorer_refresh() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let start = OffsetDateTime::now_utc() - time::Duration::seconds(90);
        {
            let mut cluster = shared.cluster_mut();
            cluster.inspect_explorer_metric(
                "sinomo.load.metric_000",
                ExplorerTransform::Rate,
                std::iter::empty(),
            );
            for tick in 0..18 {
                let at = start + time::Duration::seconds(tick * 5);
                apply_observation_batch(
                    &mut cluster.observation,
                    sample_batch(at, "node", Ok(dense_explorer_sample(at, tick as u64))),
                );
            }
        }
        let mut runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().root(view(shared.clone())))
            .build()?;
        let window = runtime.window_ids()[0];
        runtime.set_inspector_tracing(window, true)?;
        runtime.handle_event(
            window,
            Event::Window(sui::WindowEvent::Resized(Size::new(1280.0, 900.0))),
        )?;
        runtime.render(window)?;
        for tick in 18..23 {
            let at = start + time::Duration::seconds(tick * 5);
            let batch = sample_batch(at, "node", Ok(dense_explorer_sample(at, tick as u64)));
            let clock = std::time::Instant::now();
            let apply;
            {
                let mut cluster = shared.cluster_mut();
                apply_observation_batch(&mut cluster.observation, batch);
                cluster.observation.revision += 1;
                apply = clock.elapsed();
            }
            let ingest = clock.elapsed();
            let clock = std::time::Instant::now();
            let output = runtime.render(window)?;
            let elapsed = clock.elapsed();
            println!(
                "explorer tick={tick} ingest_ms={:.3} apply_ms={:.3} render_ms={:.3} widgets={}",
                ingest.as_secs_f64() * 1000.0,
                apply.as_secs_f64() * 1000.0,
                elapsed.as_secs_f64() * 1000.0,
                output.diagnostics.widget_count,
            );
        }
        let chart = runtime
            .semantics(window)?
            .iter()
            .find(|node| node.name.as_deref() == Some("sinomo.load.metric_000 live history"))
            .unwrap()
            .bounds;
        let clock = std::time::Instant::now();
        for step in 0..20 {
            runtime.handle_event(
                window,
                Event::Pointer(sui::PointerEvent::new(
                    PointerEventKind::Move,
                    Point::new(chart.x() + 130.0 + step as f32 * 5.0, chart.y() + 80.0),
                )),
            )?;
            runtime.render(window)?;
        }
        println!(
            "explorer hover_mean_ms={:.3}",
            clock.elapsed().as_secs_f64() * 1000.0 / 20.0
        );
        Ok(())
    }

    #[test]
    fn metric_explorer_search_keeps_focus_and_filters_registered_catalog() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        shared.cluster_mut().subview = ClusterSubview::Explore;
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster metric search")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 800.0,
            ))))?;
        app.run_until_idle()?;

        let search = window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("Search registered metrics");
        search.focus()?;
        search.fill("latency")?;
        search.expect().to_be_focused()?;
        search.expect().to_have_value("latency")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("sinomo.daemon.service.total.us")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("sinomo.daemon.service.requests")
            .expect()
            .to_be_hidden()?;
        assert_eq!(shared.cluster().explorer.search, "latency");
        Ok(())
    }

    #[test]
    fn compact_metric_explorer_drills_into_detail_and_back() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        shared.cluster_mut().subview = ClusterSubview::Explore;
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Compact metric explorer")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                390.0, 844.0,
            ))))?;
        app.run_until_idle()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("sinomo.daemon.service.requests")
            .click()?;
        app.run_until_idle()?;
        assert_eq!(
            shared.cluster().explorer_navigation.route(),
            sui::MasterDetailRoute::Detail
        );
        window
            .get_by_role(SemanticsRole::Image)
            .with_name("sinomo.daemon.service.requests live history")
            .expect()
            .to_be_visible()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("All metrics")
            .click()?;
        app.run_until_idle()?;
        assert_eq!(
            shared.cluster().explorer_navigation.route(),
            sui::MasterDetailRoute::Master
        );
        window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("Search registered metrics")
            .expect()
            .to_be_visible()?;
        Ok(())
    }

    #[test]
    fn metric_explorer_keeps_stale_node_and_label_filters_visible() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        {
            let mut cluster = shared.cluster_mut();
            cluster.inspect_explorer_metric(
                "sinomo.daemon.service.requests",
                ExplorerTransform::Rate,
                [("outcome".to_string(), "stale-outcome".to_string())],
            );
            cluster.set_explorer_node(Some("node-stale".to_string()));
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Stale metric filters")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                390.0, 844.0,
            ))))?;
        app.run_until_idle()?;

        window
            .get_by_role(SemanticsRole::ComboBox)
            .with_name("Metric node scope")
            .expect()
            .to_have_value("node-stale")?;
        window
            .get_by_role(SemanticsRole::ComboBox)
            .with_name("Metric label outcome")
            .expect()
            .to_have_value("stale-outcome")?;
        Ok(())
    }

    #[test]
    fn curated_dashboard_panel_opens_matching_explorer_query() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);

        shared.cluster_mut().inspect_explorer_metric(
            "sinomo.daemon.service.requests",
            ExplorerTransform::Rate,
            [("outcome".to_string(), "error".to_string())],
        );

        let cluster = shared.cluster();
        assert_eq!(cluster.subview, ClusterSubview::Explore);
        assert_eq!(
            cluster.explorer.selected_metric.as_deref(),
            Some("sinomo.daemon.service.requests")
        );
        assert_eq!(cluster.explorer.transform, ExplorerTransform::Rate);
        assert_eq!(
            cluster
                .explorer
                .label_filters
                .get("outcome")
                .map(String::as_str),
            Some("error")
        );
        assert_eq!(
            cluster.explorer_navigation.route(),
            sui::MasterDetailRoute::Detail
        );
        drop(cluster);
        assert!(shared.cluster_mut().navigate_back());
        assert_eq!(
            shared.cluster().explorer_navigation.route(),
            sui::MasterDetailRoute::Master
        );
        assert!(!shared.cluster_mut().navigate_back());
    }

    #[test]
    fn cluster_header_badge_uses_a_compact_single_line_health_summary() {
        let snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b"), {
                let mut node = node("node-c");
                node.availability = "offline".to_string();
                node
            }],
            ..ClusterSnapshot::default()
        };

        assert_eq!(cluster_health_badge_summary(&snapshot), "2 / 3 online");
        assert_eq!(
            cluster_health_badge_summary(&ClusterSnapshot::default()),
            "No nodes"
        );
        assert_eq!(
            cluster_health_summary(&snapshot),
            "2 / 3 online, 1 offline",
            "detail surfaces retain the full availability breakdown"
        );
    }

    #[test]
    fn observation_freshness_uses_source_time_and_keeps_local_node_first() {
        let now = test_time(100);
        let mut stale = TargetObservationSummary {
            node_id: "node-remote".to_string(),
            observed_at: Some(test_time(80)),
            received_at: now,
            partial: false,
            error: None,
            rpc_rate: None,
            rpc_p95_ms: None,
            kv_queue_depth: None,
            dfs_backlog: None,
            failed_tasks: None,
        };
        assert_eq!(stale.freshness(now), "Stale");
        stale.observed_at = Some(test_time(95));
        assert_eq!(stale.freshness(now), "Fresh");

        let mut local = stale.clone();
        local.node_id = "node-local".to_string();
        let mut other = stale;
        other.node_id = "node-alpha".to_string();
        let mut targets = vec![other, local];
        sort_target_summaries(&mut targets, Some("node-local"));
        assert_eq!(targets[0].node_id, "node-local");
        assert_eq!(targets[1].node_id, "node-alpha");
    }

    #[test]
    fn performance_refresh_action_starts_collection() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        seed_performance_dashboard(&shared);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster performance refresh")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;

        let revision_before = shared.cluster().observation.revision;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Refresh now")
            .click()?;
        assert!(shared.cluster().observation.in_flight);
        assert!(shared.cluster().observation.revision > revision_before);
        Ok(())
    }

    #[test]
    #[ignore = "dev dashboard capture; run explicitly with --ignored --nocapture"]
    fn capture_cluster_performance_dashboard() -> sui::Result<()> {
        use sui::{PointerButtons, PointerEvent, WindowEvent};

        let output = std::env::var("SINOMO_APP_SHOTS")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        std::fs::create_dir_all(&output).expect("create dashboard capture directory");
        let mut state = AppState::ready(Backend::local_fallback());
        state.active_tab = Tab::Cluster;
        let shared = Shared::new(state);
        shared.borrow_mut().theme_mode = AppThemeMode::Dark;

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        seed_performance_dashboard(&shared);
        app.run_until_idle()?;

        let capture = |name: &str| {
            let shot = window
                .capture_screenshot()
                .expect("capture dashboard screenshot");
            let path = output.join(name);
            shot.write_png(&path).expect("write dashboard screenshot");
            println!(
                "wrote {} ({}x{})",
                path.display(),
                shot.width(),
                shot.height()
            );
        };

        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
                1280.0, 900.0,
            ))))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-wide.png");

        shared.cluster_mut().inspect_explorer_metric(
            "sinomo.daemon.service.requests",
            ExplorerTransform::Rate,
            std::iter::empty(),
        );
        app.run_until_idle()?;
        app.advance_time(0.5)?;
        capture("sinomo-app-cluster-explorer-wide.png");

        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Metric explorer detail")
            .scroll_pixels(sui::Vector::new(0.0, -500.0))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-explorer-wide-scrolled.png");
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Metric explorer detail")
            .scroll_pixels(sui::Vector::new(0.0, 500.0))?;
        app.run_until_idle()?;

        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Overview;
            cluster.touch_explorer();
        }
        app.run_until_idle()?;
        app.advance_time(0.5)?;
        assert_eq!(shared.cluster().subview, ClusterSubview::Overview);

        shared.set_theme_mode(AppThemeMode::Light);
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-wide-light.png");

        shared.set_theme_mode(AppThemeMode::Dark);
        shared.set_text_scale(AppTextScale::ExtraLarge);
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-wide-large-text.png");

        shared.cluster_mut().inspect_explorer_metric(
            "sinomo.daemon.service.requests",
            ExplorerTransform::Rate,
            std::iter::empty(),
        );
        app.run_until_idle()?;
        app.advance_time(0.5)?;
        capture("sinomo-app-cluster-explorer-wide-large-text.png");

        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Overview;
            cluster.touch_explorer();
        }
        app.run_until_idle()?;
        app.advance_time(0.5)?;

        shared.set_text_scale(AppTextScale::Standard);
        app.run_until_idle()?;

        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 800.0))))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-narrow.png");

        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Cluster performance overview")
            .scroll_pixels(sui::Vector::new(0.0, -560.0))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-narrow-scrolled.png");

        seed_performance_dashboard(&shared);
        app.run_until_idle()?;

        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(390.0, 844.0))))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-performance-phone.png");

        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Explore;
            cluster.explorer_navigation.show_master();
            cluster.touch_explorer();
        }
        app.run_until_idle()?;
        app.advance_time(0.5)?;
        capture("sinomo-app-cluster-explorer-phone-catalog.png");

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("sinomo.daemon.service.requests")
            .click()?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-explorer-phone-detail.png");

        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Metric explorer detail")
            .scroll_pixels(sui::Vector::new(0.0, -620.0))?;
        app.run_until_idle()?;
        capture("sinomo-app-cluster-explorer-phone-detail-scrolled.png");
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("Metric explorer detail")
            .scroll_pixels(sui::Vector::new(0.0, 620.0))?;
        app.run_until_idle()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("All metrics")
            .click()?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        let sections = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar
                    && node.name.as_deref() == Some("cluster-sections")
            })
            .expect("cluster section tab bar semantics")
            .bounds;
        let point = Point::new(
            sections.x()
                + sections.width()
                    * ((ClusterSubview::Activity.index() as f32 + 0.5)
                        / ClusterSubview::ALL.len() as f32),
            sections.y() + sections.height() * 0.5,
        );
        let root = window.root();
        root.dispatch_event(Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            point,
        )))?;
        let mut down = PointerEvent::new(PointerEventKind::Down, point);
        down.button = Some(PointerButton::Primary);
        down.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(down))?;
        let mut up = PointerEvent::new(PointerEventKind::Up, point);
        up.button = Some(PointerButton::Primary);
        root.dispatch_event(Event::Pointer(up))?;
        app.run_until_idle()?;
        app.advance_time(0.5)?;

        assert_eq!(shared.cluster().subview, ClusterSubview::Activity);
        capture("sinomo-app-cluster-performance-activity.png");

        let revision_before_overview = shared.cluster().observation.revision;
        let point = Point::new(
            sections.x()
                + sections.width()
                    * ((ClusterSubview::Overview.index() as f32 + 0.5)
                        / ClusterSubview::ALL.len() as f32),
            sections.y() + sections.height() * 0.5,
        );
        root.dispatch_event(Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            point,
        )))?;
        let mut down = PointerEvent::new(PointerEventKind::Down, point);
        down.button = Some(PointerButton::Primary);
        down.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(down))?;
        let mut up = PointerEvent::new(PointerEventKind::Up, point);
        up.button = Some(PointerButton::Primary);
        root.dispatch_event(Event::Pointer(up))?;
        app.run_until_idle()?;
        assert_eq!(shared.cluster().subview, ClusterSubview::Overview);
        assert!(
            shared.cluster().observation.revision > revision_before_overview,
            "returning to Overview starts a new collection attempt"
        );
        Ok(())
    }

    #[test]
    fn selected_copy_details_preserves_full_id_address_and_endpoints() {
        let mut selected = node("node-01jz7x9m4k2p8q6r5s3t1v0w9y");
        selected.address = Some("https://10.20.30.40:21353/control".to_string());
        selected.endpoints = vec![endpoint("iroh://full-endpoint-address", true)];
        let snapshot = ClusterSnapshot {
            nodes: vec![selected.clone()],
            ..ClusterSnapshot::default()
        };

        let copied =
            selected_copy_details(&snapshot, &ClusterSelection::Node(selected.node_id.clone()))
                .expect("copyable node details");

        assert!(copied.contains(&selected.node_id));
        assert!(copied.contains("https://10.20.30.40:21353/control"));
        assert!(copied.contains("iroh://full-endpoint-address"));
        assert!(!copied.contains("..."));
    }

    #[test]
    fn keyboard_and_semantic_identity_follow_node_id_across_refresh_reordering() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let view = ClusterView::new(shared);
        let before = build_topology(&ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b")],
            ..ClusterSnapshot::default()
        });
        let after = build_topology(&ClusterSnapshot {
            nodes: vec![node("node-b"), node("node-c")],
            ..ClusterSnapshot::default()
        });
        *view.keyboard_focus.borrow_mut() = Some(ClusterSelection::Node("node-b".to_string()));

        assert_eq!(view.focused_vertex_index(&before), Some(1));
        assert_eq!(view.focused_vertex_index(&after), Some(0));
        let before_node = before
            .vertices
            .iter()
            .find(|vertex| vertex.id == "node-b")
            .expect("node-b before refresh");
        let after_node = after
            .vertices
            .iter()
            .find(|vertex| vertex.id == "node-b")
            .expect("node-b after refresh");
        let parent = sui::WidgetId::new(77);
        assert_eq!(
            cluster_vertex_widget_id(parent, before_node),
            cluster_vertex_widget_id(parent, after_node),
            "semantic identity must not depend on the row index"
        );
    }

    #[test]
    fn topology_collapses_three_node_lan_clique() {
        let snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            links: vec![
                link("node-a", "node-b", "lan", "192.168.1.2:21353"),
                link("node-b", "node-c", "lan", "192.168.1.3:21353"),
                link("node-a", "node-c", "lan", "192.168.1.4:21353"),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        assert_eq!(topology.lan_segments.len(), 1);
        assert_eq!(
            topology
                .edges
                .iter()
                .filter(|edge| edge.virtual_lan)
                .count(),
            3
        );
        assert!(
            topology
                .edges
                .iter()
                .all(|edge| edge.virtual_lan || edge.public_ip)
        );
    }

    #[test]
    fn rendered_cluster_nodes_are_semantic_and_keyboard_selectable() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                nodes: vec![node("node-a"), node("node-b")],
                ..ClusterSnapshot::default()
            };
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster keyboard navigation")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        assert_eq!(
            window.get_by_role(SemanticsRole::ListItem).count()?,
            2,
            "each topology vertex should be exposed as a semantic row"
        );
        let topology = window
            .get_by_role(SemanticsRole::List)
            .with_name("Cluster nodes and LAN segments");
        topology.press("ArrowDown")?;
        topology.press("Enter")?;
        app.run_until_idle()?;

        assert_eq!(
            shared.cluster().selected,
            Some(ClusterSelection::Node("node-b".to_string()))
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show diagnostics")
                .count()?,
            1,
            "keyboard selection should rebuild the stock inspector actions"
        );
        Ok(())
    }

    #[test]
    fn matching_subnet_without_observed_clique_is_not_collapsed() {
        let snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            links: vec![
                link("node-a", "node-b", "lan", "192.168.1.2:21353"),
                link("node-b", "node-c", "lan", "192.168.1.3:21353"),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        assert!(topology.lan_segments.is_empty());
        assert_eq!(topology.edges.len(), 2);
    }

    #[test]
    fn overview_lines_surface_cluster_inspector_summary() {
        let mut public = node("node-public");
        public.public_reachable = true;
        let snapshot = ClusterSnapshot {
            cluster_id: Some("cluster-test-123456789".to_string()),
            local_node_id: Some("node-public".to_string()),
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![public, node("node-b")],
            links: vec![link(
                "node-public",
                "node-b",
                "internet-direct",
                "203.0.113.10:21353",
            )],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let lines = overview_lines(&snapshot, &topology, false, false);

        assert_eq!(
            cluster_header_summary(&snapshot, &topology),
            "2 nodes · LAN 0 segments · public 1/2"
        );
        assert_eq!(lines[0], "Cluster overview");
        assert!(lines.iter().any(|line| line == "health: 2 / 2 online"));
        assert!(lines.iter().any(|line| line == "public route: 1 / 2 nodes"));
        assert!(
            lines
                .iter()
                .any(|line| line == "mode: public and private routing")
        );
        assert!(lines.iter().any(|line| line == "LAN: 0 segments"));
        assert!(
            lines
                .iter()
                .any(|line| line == "connection: daemon connected")
        );
        assert!(
            !lines.iter().any(|line| line.starts_with("topology:")),
            "direct topology edge counts belong behind diagnostics"
        );
        assert!(
            lines
                .iter()
                .any(|line| line == "hint: Show diagnostics for source and cluster identifiers.")
        );
        assert!(lines.iter().any(|line| line.starts_with("hint: Select")));
        assert!(
            !lines.iter().any(|line| line.contains("127.0.0.1")),
            "raw daemon source belongs in diagnostics"
        );
        assert!(
            !lines.iter().any(|line| line.contains("cluster-test")),
            "cluster identifiers belong in diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(open.iter().any(|line| line == "topology: 1 direct edges"));
        assert!(
            open.iter()
                .any(|line| line == "diagnostics: daemon 127.0.0.1:21353")
        );
        assert!(
            open.iter()
                .any(|line| line == "cluster: cluster-test-123456789")
        );
        assert!(open.iter().any(|line| line == "local: node-public"));
    }

    #[test]
    fn overview_private_cluster_explains_lan_only_routing() {
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let lines = overview_lines(&snapshot, &topology, false, false);

        assert!(lines.iter().any(|line| line == "health: 3 / 3 online"));
        assert!(lines.iter().any(|line| line == "public route: 0 / 3 nodes"));
        assert!(
            lines
                .iter()
                .any(|line| line == "mode: private/LAN; public ingress not required")
        );
        assert!(!lines.iter().any(|line| line.contains("attention:")));
    }

    #[test]
    fn overview_warning_keeps_raw_detail_behind_diagnostics() {
        let mut offline = node("node-b");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), offline],
            warnings: vec!["daemon health unavailable: network error".to_string()],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "attention: 1 node offline")
        );
        assert!(closed.iter().any(|line| line
            == "hint: Show diagnostics for warning detail, source, and cluster identifiers."));
        assert!(
            !closed.iter().any(|line| line.contains("network error")),
            "raw warning belongs in diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line == "warning detail: daemon health unavailable: network error")
        );
    }

    #[test]
    fn overview_keeps_probe_health_counts_when_daemon_aggregation_warns() {
        let mut offline = node("node-c");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), offline],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "health: 2 / 3 online, 1 offline"),
            "overview should trust direct probe rows instead of reducing to local health"
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "attention: 1 node offline")
        );
        assert!(
            !closed.iter().any(|line| line.contains("rpc failed")),
            "raw aggregation error belongs behind diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line
                    .starts_with("warning detail: daemon health aggregation unavailable"))
        );
    }

    #[test]
    fn refreshing_overview_does_not_surface_stale_warning_as_attention() {
        let mut offline = node("node-c");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), offline],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, true);
        assert!(
            closed
                .iter()
                .any(|line| line == "health: 2 / 3 online, 1 offline"),
            "the stale snapshot counts remain visible while the refresh runs"
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "status: refreshing current health")
        );
        assert!(
            !closed.iter().any(|line| line.starts_with("attention:")),
            "stale refresh snapshots should not read as confirmed current attention"
        );

        let open = overview_lines(&snapshot, &topology, true, true);
        assert!(
            open.iter().any(|line| line
                .starts_with("previous warning detail: daemon health aggregation unavailable")),
            "diagnostics should still expose the stale warning source"
        );
        assert!(
            !open.iter().any(|line| line.starts_with("warning detail:")),
            "refreshing warnings should be labeled as previous evidence"
        );
    }

    #[test]
    fn overview_direct_probe_fallback_is_diagnostic_only_when_all_nodes_online() {
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(closed.iter().any(|line| line == "health: 3 / 3 online"));
        assert!(
            !closed.iter().any(|line| line.starts_with("attention:")),
            "direct probe fallback should not surface as attention when every node is online"
        );
        assert!(
            cluster_actionable_warning(&snapshot).is_none(),
            "all-online direct-probe fallback should stay diagnostics-only"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line
                    .starts_with("warning detail: daemon health aggregation unavailable")),
            "technical fallback detail should remain available behind diagnostics"
        );
    }

    #[test]
    fn pump_refresh_applies_snapshot_and_clears_missing_selection() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut state = ClusterState {
            snapshot: ClusterSnapshot {
                nodes: vec![node("node-a"), node("node-b")],
                ..ClusterSnapshot::default()
            },
            subview: ClusterSubview::Topology,
            selected: Some(ClusterSelection::Node("node-b".to_string())),
            diagnostics_open: true,
            refreshing: true,
            refresh_rx: Some(rx),
            last_topology_refresh: None,
            observation: ClusterObservationState::default(),
            explorer: MetricExplorerState::default(),
            explorer_navigation: MasterDetailState::default(),
            logs: logs::LogState::default(),
        };
        tx.send(ClusterSnapshot {
            nodes: vec![node("node-a")],
            ..ClusterSnapshot::default()
        })
        .expect("send refreshed snapshot");

        assert!(state.pump_refresh());
        assert!(!state.refreshing);
        assert_eq!(state.snapshot.nodes.len(), 1);
        assert_eq!(state.selected, None);
        assert!(!state.diagnostics_open);
    }

    #[test]
    fn cluster_state_starts_with_lightweight_placeholder() {
        let backend = Backend::local_fallback();
        let state = ClusterState::new(&backend);

        assert!(state.snapshot.nodes.is_empty());
        assert!(!state.refreshing);
        assert!(
            state
                .snapshot
                .warnings
                .iter()
                .any(|warning| warning.contains("loading in the background")),
            "startup cluster state should not synchronously fetch live health"
        );
    }

    #[test]
    fn overview_refresh_action_is_exposed() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster refresh action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refresh")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn warning_overview_action_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                warnings: vec!["daemon health unavailable: network error".to_string()],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster warning action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Open connection settings for cluster warning")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(shared.settings().selected, Some(SettingsSection::System));

        Ok(())
    }

    #[test]
    fn refreshing_overview_hides_warning_recovery_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let mut offline = node("node-b");
        offline.availability = "offline".to_string();
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a"), offline],
                warnings: vec!["daemon health unavailable: network error".to_string()],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
            cluster.refreshing = true;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster refreshing warning action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection settings")
                .with_description("Open connection settings for cluster warning")
                .count()?,
            0
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refreshing")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn selected_offline_node_exposes_recovery_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        offline.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![offline],
                ..ClusterSnapshot::default()
            };
            cluster.selected = Some(ClusterSelection::Node("node-offline".to_string()));
            cluster.diagnostics_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster selected node actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refresh")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show diagnostics")
                .with_description("Show technical cluster source and identifier details")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Open connection settings for selected cluster node")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(shared.settings().selected, Some(SettingsSection::System));

        Ok(())
    }

    #[test]
    fn selected_node_connection_action_requires_attention() {
        let mut healthy = node("node-healthy");
        healthy.endpoints = vec![endpoint("127.0.0.1:21353", true)];
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            nodes: vec![healthy, offline],
            ..ClusterSnapshot::default()
        };

        assert!(!selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::Node("node-healthy".to_string()))
        ));
        assert!(selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::Node("node-offline".to_string()))
        ));
        assert!(!selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::LanSegment("lan-segment-1".to_string()))
        ));
    }

    #[test]
    fn overview_diagnostics_toggle_reveals_technical_details() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Topology;
            cluster.snapshot = ClusterSnapshot {
                cluster_id: Some("cluster-test-123456789".to_string()),
                local_node_id: Some("node-a".to_string()),
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
            cluster.diagnostics_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster overview diagnostics")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show diagnostics")
            .with_description("Show technical cluster source and identifier details")
            .click()?;

        assert!(shared.cluster().diagnostics_open);

        Ok(())
    }

    #[test]
    fn unknown_nodes_use_visible_neutral_graph_fill() {
        let mut unknown = node("node-unknown");
        unknown.availability = "unknown".to_string();
        let snapshot = ClusterSnapshot {
            nodes: vec![unknown],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let theme = resolve_theme(AppThemeMode::Light);
        let pal = Palette::from_theme(&theme, false);
        let fill = vertex_fill(&pal, &topology.vertices[0]);

        assert_eq!(fill, unknown_node_color(&pal));
        assert_ne!(fill, pal.field);
    }

    #[test]
    fn public_reachability_does_not_override_health_fill() {
        let mut offline = node("node-public-offline");
        offline.availability = "offline".to_string();
        offline.public_reachable = true;
        let snapshot = ClusterSnapshot {
            nodes: vec![offline],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let theme = resolve_theme(AppThemeMode::Light);
        let pal = Palette::from_theme(&theme, false);
        let fill = vertex_fill(&pal, &topology.vertices[0]);

        assert_eq!(fill, pal.bad);
        assert_ne!(fill, public_node_color(&pal));
    }

    #[test]
    fn duplicate_display_names_are_disambiguated_in_graph_labels() {
        let mut left = node("01kr4w089vqgdxpb84t6bq1qxn");
        left.display_name = "sinomo-node".to_string();
        left.platform = Some("linux/aarch64".to_string());
        left.availability = "offline".to_string();
        let mut right = node("01kr3d54d45zkm0p0xgps8k7mt");
        right.display_name = "sinomo-node".to_string();
        right.platform = Some("linux/x86_64".to_string());
        let snapshot = ClusterSnapshot {
            nodes: vec![left, right],
            ..ClusterSnapshot::default()
        };

        let topology = build_topology(&snapshot);
        let labels = topology
            .vertices
            .iter()
            .map(|vertex| vertex.label.as_str())
            .collect::<Vec<_>>();
        let details = topology
            .vertices
            .iter()
            .map(|vertex| vertex.detail.as_str())
            .collect::<Vec<_>>();

        assert!(labels.contains(&"sinomo-node bq1qxn"));
        assert!(labels.contains(&"sinomo-node s8k7mt"));
        assert!(details.contains(&"offline / linux/arm64"));
        assert!(details.contains(&"online / linux/x64"));
    }

    #[test]
    fn node_detail_surfaces_short_identity_and_platform_before_diagnostics() {
        let mut node = node("01kr4w089vqgdxpb84t6bq1qxn");
        node.display_name = "sinomo-node".to_string();
        node.platform = Some("linux/aarch64".to_string());

        let closed = node_detail_lines(&node, false);

        assert_eq!(closed[0], "sinomo-node bq1qxn");
        assert!(closed.iter().any(|line| line == "platform: linux/arm64"));
    }

    #[test]
    fn node_detail_keeps_endpoint_addresses_behind_diagnostics() {
        let mut node = node("node-a");
        node.endpoints = vec![endpoint(
            "iroh://e6e7579b45088dc226274eadd8dec9782eb042c1c753ba5be249c96c1c8c0e2e?relay=https://use1-1.relay.no.iroh-canary.iroh.link/",
            true,
        )];

        let closed = node_detail_lines(&node, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "endpoints: 1 / 1 reachable")
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "route: LAN/local route only")
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "next: LAN or local routing only")
        );
        assert!(
            !closed.iter().any(|line| line.contains("iroh://")),
            "raw endpoint should not be in primary node details"
        );

        let open = node_detail_lines(&node, true);
        assert!(
            open.iter()
                .any(|line| line == "diagnostics: endpoint addresses")
        );
        assert!(
            open.iter().any(|line| line.contains("iroh://")),
            "diagnostics should expose the endpoint address"
        );
    }

    #[test]
    fn node_detail_guidance_prioritizes_offline_and_unreachable_nodes() {
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        offline.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        let offline_lines = node_detail_lines(&offline, false);
        assert!(
            offline_lines
                .iter()
                .any(|line| line == "route: node offline")
        );
        assert!(
            offline_lines
                .iter()
                .any(|line| { line == "next: start daemon on that node or check its route" })
        );

        let mut unreachable = node("node-unreachable");
        unreachable.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        let unreachable_lines = node_detail_lines(&unreachable, false);
        assert!(
            unreachable_lines
                .iter()
                .any(|line| line == "route: endpoints unreachable")
        );
        assert!(
            unreachable_lines
                .iter()
                .any(|line| line == "next: check endpoint reachability in diagnostics")
        );

        let mut public = node("node-public");
        public.public_reachable = true;
        public.endpoints = vec![endpoint("203.0.113.10:21353", true)];
        let public_lines = node_detail_lines(&public, false);
        assert!(
            public_lines
                .iter()
                .any(|line| line == "route: public route reachable")
        );
        assert!(
            public_lines
                .iter()
                .any(|line| line == "next: public route is reachable")
        );
    }
}
