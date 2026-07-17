//! "Evolution" tab - self-evolution launch, candidate evidence, and rollout readiness.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;

#[cfg(test)]
use sui::WindowEvent;
use sui::{
    Alignment, ArrangeCtx, Button, Color, Constraints, ControlSize, DefaultTheme, DetailRow,
    EmptyState, Event, EventCtx, Flex, FlexItem, FontWeight, IconGlyph, Insets, KeyState, Label,
    MeasureCtx, PaintCtx, PanelSection, Path, PropertyRow, Rect, ScrollView, SectionLabel, Select,
    SemanticTone, SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, Signal, Size,
    SizedBox, StatusBadge, StrokeStyle, Surface, SurfaceAppearance, SurfaceBorder, SurfaceRole,
    TextInput, VirtualList, Widget, WidgetId, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
};

use crate::backend::Backend;
use crate::backend::evolution::{
    EvolutionDashboard, EvolutionDecisionAction, EvolutionDecisionEvent, EvolutionDecisionRequest,
    EvolutionJobSummary, EvolutionPhase, EvolutionStartEvent, EvolutionStartRequest,
    load_evolution_dashboard, spawn_evolution_decision, spawn_evolution_job,
};
use crate::state::Shared;
use crate::theme::{AppTextScale, AppThemeMode, Palette};
use crate::ui::widgets;
use sinomo_runtime::local::SinomoDataDir;

const HEADER_H: f32 = 78.0;
const PAD: f32 = 20.0;
const GAP: f32 = 14.0;
const START_WIDE_HEADER_H: f32 = 42.0;
const START_NARROW_HEADER_H: f32 = 60.0;
const START_BREAKPOINT: f32 = 760.0;
const CONTROL_LABEL_H: f32 = 18.0;
const CONTROL_LABEL_GAP: f32 = 4.0;
/// Evolution's start form is a dense configuration surface, so its fields use Small.
const CONTROL_H: f32 = 28.0;
const CONTROL_GAP: f32 = 8.0;
const STARTER_ACTION_COUNT: usize = 3;
const STARTER_ACTION_H: f32 = 28.0;
const STARTER_ACTION_GAP: f32 = 8.0;
const START_OPTIONS_W: f32 = 112.0;
const START_BUTTON_W: f32 = 112.0;
const START_TARGET_W: f32 = 138.0;
const TRUNCATION_MARKER: &str = "...";
const QUEUE_W: f32 = 306.0;
const RAIL_W: f32 = 318.0;
const QUEUE_HEADER_H: f32 = 48.0;
const QUEUE_ROW_MIN_H: f32 = 78.0;
const QUEUE_ROW_TOP_PAD: f32 = 8.0;
const QUEUE_ROW_LINE_GAP: f32 = 2.0;
const QUEUE_ROW_BOTTOM_PAD: f32 = 8.0;
const DECISION_SECTION_H: f32 = 146.0;
const NARROW_ACTION_BAR_H: f32 = 72.0;
/// Sticky queue decisions remain Small so the review surface keeps priority.
const DECISION_BUTTON_H: f32 = 28.0;
const READINESS_DETAIL_BUTTON_W: f32 = 136.0;
const OBJECTIVE_PLACEHOLDER: &str = "Describe the improvement or task";
const SCOPE_PLACEHOLDER: &str = "Optional: path globs, comma-separated";
const EVOLUTION_EMPTY_SUBTITLE: &str = "Describe an objective above, then Start.";
const LIFECYCLE_MIN_H: f32 = 66.0;
const LIFECYCLE_TOP_PAD: f32 = 8.0;
const LIFECYCLE_DOT_RADIUS: f32 = 10.0;
const LIFECYCLE_LABEL_GAP: f32 = 4.0;
const LIFECYCLE_BOTTOM_PAD: f32 = 8.0;
const LIFECYCLE_LABEL_HORIZONTAL_GAP: f32 = 4.0;

#[derive(Clone, Copy, Debug)]
struct QueueRowMetrics {
    height: f32,
    title_top: f32,
    title_line_height: f32,
    meta_top: f32,
    meta_line_height: f32,
    target_top: f32,
    target_line_height: f32,
}

impl QueueRowMetrics {
    fn for_theme(theme: &DefaultTheme) -> Self {
        let title_line_height = theme.text.sm.line_height.max(1.0);
        let meta_line_height = theme.text.xs.line_height.max(1.0);
        let target_line_height = theme.text.xs.line_height.max(1.0);
        let title_top = QUEUE_ROW_TOP_PAD;
        let meta_top = title_top + title_line_height + QUEUE_ROW_LINE_GAP;
        let target_top = meta_top + meta_line_height + QUEUE_ROW_LINE_GAP;
        let natural_height = target_top + target_line_height + QUEUE_ROW_BOTTOM_PAD;
        Self {
            height: natural_height.max(QUEUE_ROW_MIN_H),
            title_top,
            title_line_height,
            meta_top,
            meta_line_height,
            target_top,
            target_line_height,
        }
    }
}

struct EvolutionQueueRow {
    source: Signal<EvolutionJobSummary>,
    shared: Shared,
    job: EvolutionJobSummary,
}

impl EvolutionQueueRow {
    fn new(shared: Shared, source: Signal<EvolutionJobSummary>) -> Self {
        let job = source.get();
        Self {
            source,
            shared,
            job,
        }
    }
}

impl Widget for EvolutionQueueRow {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.job = ctx.observe(&self.source);
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            constraints.min.width.max(QUEUE_W)
        };
        constraints.clamp(Size::new(
            width,
            QueueRowMetrics::for_theme(&self.shared.theme()).height,
        ))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        let selected = {
            let evolution = self.shared.evolution();
            evolution
                .selected
                .and_then(|index| evolution.dashboard.jobs.get(index))
                .is_some_and(|selected| selected.id == self.job.id)
        };
        paint_queue_row(
            ctx,
            &theme,
            &pal,
            ctx.bounds(),
            &self.job,
            selected,
            QueueRowMetrics::for_theme(&theme),
        );
        widgets::hairline_bottom(ctx, ctx.bounds(), pal.border_subtle);
    }
}

#[derive(Clone, Copy, Debug)]
struct LifecycleMetrics {
    height: f32,
    track_center_y: f32,
    label_top: f32,
    label_line_height: f32,
}

impl LifecycleMetrics {
    fn for_theme(theme: &DefaultTheme) -> Self {
        let label_line_height = theme.text.xs.line_height.max(1.0);
        let track_center_y = LIFECYCLE_TOP_PAD + LIFECYCLE_DOT_RADIUS;
        let label_top = track_center_y + LIFECYCLE_DOT_RADIUS + LIFECYCLE_LABEL_GAP;
        let natural_height = label_top + label_line_height + LIFECYCLE_BOTTOM_PAD;
        Self {
            height: natural_height.max(LIFECYCLE_MIN_H),
            track_center_y,
            label_top,
            label_line_height,
        }
    }

    fn label_center_y(self) -> f32 {
        self.label_top + self.label_line_height * 0.5
    }
}

#[derive(Clone, Copy, Debug)]
struct LifecycleLabelPlacement {
    index: usize,
    center_x: f32,
    left: f32,
    right: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct EvolutionDashboardRequestId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvolutionDashboardRefreshReason {
    InitialLoad,
    JobStarted,
    JobFinished,
    DecisionFinished,
    Retry,
}

impl EvolutionDashboardRefreshReason {
    fn label(self) -> &'static str {
        match self {
            Self::InitialLoad => "initial dashboard load",
            Self::JobStarted => "job start refresh",
            Self::JobFinished => "completed job refresh",
            Self::DecisionFinished => "decision refresh",
            Self::Retry => "dashboard retry",
        }
    }

    fn pending_label(self) -> &'static str {
        match self {
            Self::InitialLoad => "Loading candidate queue in the background",
            Self::JobStarted => "Refreshing the new evolution job",
            Self::JobFinished => "Refreshing completed evolution evidence",
            Self::DecisionFinished => "Refreshing the recorded decision",
            Self::Retry => "Retrying candidate queue refresh",
        }
    }
}

struct EvolutionDashboardRefreshRequest {
    id: EvolutionDashboardRequestId,
    reason: EvolutionDashboardRefreshReason,
    prefer_newest: bool,
    data_dir: Arc<SinomoDataDir>,
    waker: Option<sui::UiHandle>,
}

struct EvolutionDashboardRefreshResult {
    id: EvolutionDashboardRequestId,
    reason: EvolutionDashboardRefreshReason,
    prefer_newest: bool,
    dashboard: EvolutionDashboard,
}

pub struct EvolutionState {
    pub dashboard: EvolutionDashboard,
    pub selected: Option<usize>,
    pub objective_draft: String,
    pub scope_draft: String,
    pub target_ring_index: usize,
    pub start_advanced_open: bool,
    pub readiness_details_open: bool,
    pub start_status: Option<String>,
    pub start_error: Option<String>,
    objective_revision: u64,
    start_rx: Option<Receiver<EvolutionStartEvent>>,
    pub decision_status: Option<String>,
    pub decision_error: Option<String>,
    decision_action: Option<EvolutionDecisionAction>,
    decision_rx: Option<Receiver<EvolutionDecisionEvent>>,
    dashboard_load_rx: Option<Receiver<EvolutionDashboardRefreshResult>>,
    queued_dashboard_refresh: Option<EvolutionDashboardRefreshRequest>,
    next_dashboard_request_id: u64,
    requested_dashboard_id: Option<EvolutionDashboardRequestId>,
    completed_dashboard_id: Option<EvolutionDashboardRequestId>,
    dashboard_refresh_reason: Option<EvolutionDashboardRefreshReason>,
    dashboard_refresh_pending: bool,
    dashboard_refresh_error: Option<String>,
    scope_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionHeaderStructure {
    theme: (AppThemeMode, AppTextScale),
    dashboard: EvolutionDashboard,
    refresh_pending: bool,
    refresh_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionQueueEmptyStructure {
    theme: (AppThemeMode, AppTextScale),
    empty: bool,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionStartStructure {
    theme: (AppThemeMode, AppTextScale),
    objective: String,
    scope: String,
    target_ring_index: usize,
    advanced_open: bool,
    status: Option<String>,
    error: Option<String>,
    job_count: usize,
    starting: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionDetailStructure {
    theme: (AppThemeMode, AppTextScale),
    selected: Option<EvolutionJobSummary>,
    can_start: bool,
    starting: bool,
    start_status: Option<String>,
    start_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionRailStructure {
    theme: (AppThemeMode, AppTextScale),
    dashboard: EvolutionDashboard,
    selected: Option<EvolutionJobSummary>,
    decision_status: Option<String>,
    decision_error: Option<String>,
    deciding: bool,
    readiness_details_open: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionNarrowActionStructure {
    theme: (AppThemeMode, AppTextScale),
    selected: Option<EvolutionJobSummary>,
    decision_status: Option<String>,
    decision_error: Option<String>,
    deciding: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvolutionStructureKey {
    theme: (AppThemeMode, AppTextScale),
    header: EvolutionHeaderStructure,
    queue_empty: EvolutionQueueEmptyStructure,
    start: EvolutionStartStructure,
    detail: EvolutionDetailStructure,
    rail: EvolutionRailStructure,
    narrow_action: EvolutionNarrowActionStructure,
}

impl EvolutionStructureKey {
    pub(crate) fn new(
        theme_mode: AppThemeMode,
        text_scale: AppTextScale,
        state: Option<&EvolutionState>,
    ) -> Self {
        let theme = (theme_mode, text_scale);
        let Some(state) = state else {
            let dashboard = EvolutionDashboard::default();
            return Self {
                theme,
                header: EvolutionHeaderStructure {
                    theme,
                    dashboard: dashboard.clone(),
                    refresh_pending: false,
                    refresh_error: None,
                },
                queue_empty: EvolutionQueueEmptyStructure {
                    theme,
                    empty: true,
                    error: None,
                },
                start: EvolutionStartStructure {
                    theme,
                    objective: String::new(),
                    scope: String::new(),
                    target_ring_index: 0,
                    advanced_open: false,
                    status: None,
                    error: None,
                    job_count: 0,
                    starting: false,
                },
                detail: EvolutionDetailStructure {
                    theme,
                    selected: None,
                    can_start: false,
                    starting: false,
                    start_status: None,
                    start_error: None,
                },
                rail: EvolutionRailStructure {
                    theme,
                    dashboard,
                    selected: None,
                    decision_status: None,
                    decision_error: None,
                    deciding: false,
                    readiness_details_open: false,
                },
                narrow_action: EvolutionNarrowActionStructure {
                    theme,
                    selected: None,
                    decision_status: None,
                    decision_error: None,
                    deciding: false,
                },
            };
        };
        let dashboard = state.dashboard.clone();
        let selected = state
            .selected
            .and_then(|index| dashboard.jobs.get(index))
            .cloned();
        let starting = state.is_starting();
        let deciding = state.is_deciding();
        Self {
            theme,
            header: EvolutionHeaderStructure {
                theme,
                dashboard: dashboard.clone(),
                refresh_pending: state.dashboard_refresh_pending(),
                refresh_error: state.dashboard_refresh_error().map(str::to_string),
            },
            queue_empty: EvolutionQueueEmptyStructure {
                theme,
                empty: dashboard.jobs.is_empty(),
                error: dashboard.error.clone(),
            },
            start: EvolutionStartStructure {
                theme,
                objective: state.objective_draft.clone(),
                scope: state.scope_draft.clone(),
                target_ring_index: state.target_ring_index,
                advanced_open: state.start_advanced_open,
                status: state.start_status.clone(),
                error: state.start_error.clone(),
                job_count: dashboard.jobs.len(),
                starting,
            },
            detail: EvolutionDetailStructure {
                theme,
                selected: selected.clone(),
                can_start: state.can_start(),
                starting,
                start_status: state.start_status.clone(),
                start_error: state.start_error.clone(),
            },
            rail: EvolutionRailStructure {
                theme,
                dashboard,
                selected: selected.clone(),
                decision_status: state.decision_status.clone(),
                decision_error: state.decision_error.clone(),
                deciding,
                readiness_details_open: state.readiness_details_open,
            },
            narrow_action: EvolutionNarrowActionStructure {
                theme,
                selected,
                decision_status: state.decision_status.clone(),
                decision_error: state.decision_error.clone(),
                deciding,
            },
        }
    }
}

impl EvolutionState {
    pub(crate) fn editor_revisions(&self) -> (u64, u64) {
        (self.objective_revision, self.scope_revision)
    }

    pub fn new(backend: &Backend) -> Self {
        let dashboard = EvolutionDashboard::load(backend);
        Self::from_dashboard(dashboard)
    }

    pub fn new_lightweight(backend: &Backend) -> Self {
        Self::from_dashboard(EvolutionDashboard {
            source: backend
                .data_dir
                .root()
                .join("evolution")
                .display()
                .to_string(),
            jobs: Vec::new(),
            error: Some("Evolution data is loading.".to_string()),
        })
    }

    fn from_dashboard(dashboard: EvolutionDashboard) -> Self {
        let selected = (!dashboard.jobs.is_empty()).then_some(0);
        Self {
            dashboard,
            selected,
            objective_draft: String::new(),
            scope_draft: String::new(),
            target_ring_index: 1,
            start_advanced_open: false,
            readiness_details_open: false,
            start_status: None,
            start_error: None,
            objective_revision: 0,
            start_rx: None,
            decision_status: None,
            decision_error: None,
            decision_action: None,
            decision_rx: None,
            dashboard_load_rx: None,
            queued_dashboard_refresh: None,
            next_dashboard_request_id: 0,
            requested_dashboard_id: None,
            completed_dashboard_id: None,
            dashboard_refresh_reason: None,
            dashboard_refresh_pending: false,
            dashboard_refresh_error: None,
            scope_revision: 0,
        }
    }

    pub fn begin_dashboard_load(
        &mut self,
        data_dir: Arc<SinomoDataDir>,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        if self.requested_dashboard_id.is_some() {
            return false;
        }
        self.request_dashboard_refresh(
            data_dir,
            waker,
            false,
            EvolutionDashboardRefreshReason::InitialLoad,
        )
    }

    pub(crate) fn retry_dashboard_refresh(
        &mut self,
        data_dir: Arc<SinomoDataDir>,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        if self.dashboard_refresh_pending() {
            return false;
        }
        self.request_dashboard_refresh(
            data_dir,
            waker,
            false,
            EvolutionDashboardRefreshReason::Retry,
        )
    }

    pub fn pump_dashboard_load(&mut self) -> bool {
        let Some(rx) = self.dashboard_load_rx.as_ref() else {
            return false;
        };
        let result = match rx.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return false,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.dashboard_load_rx = None;
                if let Some(request) = self.queued_dashboard_refresh.take() {
                    self.start_dashboard_refresh(request);
                } else {
                    self.dashboard_refresh_pending = false;
                    let reason = self
                        .dashboard_refresh_reason
                        .map(EvolutionDashboardRefreshReason::label)
                        .unwrap_or("dashboard refresh");
                    self.dashboard_refresh_error =
                        Some(format!("{reason} failed: background worker exited"));
                }
                return true;
            }
        };
        self.dashboard_load_rx = None;
        let _applied = self.apply_dashboard_refresh_result(result);
        if let Some(request) = self.queued_dashboard_refresh.take() {
            self.start_dashboard_refresh(request);
        } else {
            self.dashboard_refresh_pending = false;
            self.dashboard_refresh_reason = None;
        }
        true
    }

    fn request_dashboard_refresh(
        &mut self,
        data_dir: Arc<SinomoDataDir>,
        waker: Option<sui::UiHandle>,
        prefer_newest: bool,
        reason: EvolutionDashboardRefreshReason,
    ) -> bool {
        self.next_dashboard_request_id = self.next_dashboard_request_id.wrapping_add(1).max(1);
        let id = EvolutionDashboardRequestId(self.next_dashboard_request_id);
        let request = EvolutionDashboardRefreshRequest {
            id,
            reason,
            prefer_newest,
            data_dir,
            waker,
        };
        self.requested_dashboard_id = Some(id);
        self.dashboard_refresh_reason = Some(reason);
        self.dashboard_refresh_pending = true;
        self.dashboard_refresh_error = None;

        if self.dashboard_load_rx.is_some() {
            // The lane is intentionally single-flight. A newer request replaces any queued
            // request; the in-flight result carries its ID and will be rejected as stale.
            self.queued_dashboard_refresh = Some(request);
            true
        } else {
            self.start_dashboard_refresh(request)
        }
    }

    fn start_dashboard_refresh(&mut self, request: EvolutionDashboardRefreshRequest) -> bool {
        let (tx, rx) = mpsc::channel();
        let EvolutionDashboardRefreshRequest {
            id,
            reason,
            prefer_newest,
            data_dir,
            waker,
        } = request;
        match thread::Builder::new()
            .name(format!("sinomo-app-evolution-refresh-{}", id.0))
            .spawn(move || {
                let dashboard = load_evolution_dashboard(data_dir.as_ref());
                let _ = tx.send(EvolutionDashboardRefreshResult {
                    id,
                    reason,
                    prefer_newest,
                    dashboard,
                });
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.dashboard_load_rx = Some(rx);
                true
            }
            Err(error) => {
                self.dashboard_refresh_pending = false;
                self.dashboard_refresh_error =
                    Some(format!("{} failed to start: {error}", reason.label()));
                false
            }
        }
    }

    fn apply_dashboard_refresh_result(&mut self, result: EvolutionDashboardRefreshResult) -> bool {
        if self.requested_dashboard_id != Some(result.id) {
            return false;
        }
        self.completed_dashboard_id = Some(result.id);
        if let Some(error) = result.dashboard.error.as_deref() {
            self.dashboard_refresh_error =
                Some(format!("{} failed: {error}", result.reason.label()));
            if self.dashboard.jobs.is_empty() {
                self.dashboard = result.dashboard;
            }
            return true;
        }
        self.dashboard_refresh_error = None;
        self.apply_dashboard(result.dashboard, result.prefer_newest);
        true
    }

    fn apply_dashboard(&mut self, dashboard: EvolutionDashboard, prefer_newest: bool) {
        let selected_id = self
            .selected
            .and_then(|index| self.dashboard.jobs.get(index))
            .map(|job| job.id.clone());
        self.dashboard = dashboard;
        self.selected = if prefer_newest {
            (!self.dashboard.jobs.is_empty()).then_some(0)
        } else {
            selected_id
                .and_then(|id| self.dashboard.jobs.iter().position(|job| job.id == id))
                .or(match self.selected {
                    Some(index) if index < self.dashboard.jobs.len() => Some(index),
                    _ if !self.dashboard.jobs.is_empty() => Some(0),
                    _ => None,
                })
        };
    }

    pub fn dashboard_refresh_pending(&self) -> bool {
        self.dashboard_refresh_pending
    }

    pub fn dashboard_refresh_error(&self) -> Option<&str> {
        self.dashboard_refresh_error.as_deref()
    }

    fn dashboard_refresh_pending_label(&self) -> Option<&'static str> {
        self.dashboard_refresh_pending
            .then(|| {
                self.dashboard_refresh_reason
                    .map(|reason| reason.pending_label())
            })
            .flatten()
    }

    pub fn set_objective_draft(&mut self, value: String) {
        self.objective_draft = value;
        self.start_error = None;
    }

    fn apply_starter_objective(&mut self, objective: EvolutionStarterObjective) {
        self.objective_draft = objective.prompt().to_string();
        self.scope_draft = objective.scope().to_string();
        self.target_ring_index = target_ring_index_for_value(objective.target_ring());
        self.start_advanced_open = true;
        self.start_error = None;
        self.start_status = None;
        self.objective_revision = self.objective_revision.wrapping_add(1);
        self.scope_revision = self.scope_revision.wrapping_add(1);
    }

    pub fn set_scope_draft(&mut self, value: String) {
        self.scope_draft = value;
        self.start_error = None;
    }

    pub fn set_target_ring_index(&mut self, index: usize) {
        self.target_ring_index = index.min(EVOLUTION_TARGET_RINGS.len().saturating_sub(1));
    }

    pub fn toggle_start_advanced(&mut self) {
        self.start_advanced_open = !self.start_advanced_open;
    }

    pub fn set_readiness_details_open(&mut self, open: bool) {
        self.readiness_details_open = open;
    }

    pub fn can_start(&self) -> bool {
        !self.is_starting() && !self.objective_draft.trim().is_empty()
    }

    pub fn is_starting(&self) -> bool {
        self.start_rx.is_some()
    }

    pub fn is_deciding(&self) -> bool {
        self.decision_rx.is_some()
    }

    pub fn has_pending_worker(&self) -> bool {
        self.is_starting() || self.is_deciding()
    }

    pub fn start(&mut self) {
        if !self.can_start() {
            if self.objective_draft.trim().is_empty() {
                self.start_error = Some("enter an evolution objective first".to_string());
            }
            return;
        }
        let request = EvolutionStartRequest {
            objective: self.objective_draft.trim().to_string(),
            target_ring: EVOLUTION_TARGET_RINGS
                .get(self.target_ring_index)
                .map(|ring| ring.value.to_string())
                .unwrap_or_else(|| "r1".to_string()),
            intended_paths: parse_scope_paths(&self.scope_draft),
        };
        self.start_status = Some("starting evolution agent".to_string());
        self.start_error = None;
        self.start_rx = Some(spawn_evolution_job(request));
    }

    pub fn pump_start_events(&mut self, backend: &Backend, waker: Option<sui::UiHandle>) -> bool {
        let events = self
            .start_rx
            .as_ref()
            .map(|rx| rx.try_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut changed = false;
        let mut finished = false;
        for event in events {
            match event {
                EvolutionStartEvent::Started => {
                    self.start_status = Some("evolution agent running".to_string());
                    changed = true;
                }
                EvolutionStartEvent::Finished(Ok(())) => {
                    self.start_status = Some("candidate awaiting evidence review".to_string());
                    self.start_error = None;
                    finished = true;
                    changed = true;
                }
                EvolutionStartEvent::Finished(Err(error)) => {
                    self.start_status = Some("evolution run failed".to_string());
                    self.start_error = Some(error);
                    finished = true;
                    changed = true;
                }
            }
        }
        if finished {
            self.start_rx = None;
        }
        if changed {
            let reason = if finished {
                EvolutionDashboardRefreshReason::JobFinished
            } else {
                EvolutionDashboardRefreshReason::JobStarted
            };
            self.request_dashboard_refresh(backend.data_dir.clone(), waker, true, reason);
        }
        changed
    }

    pub fn can_decide_selected(&self) -> bool {
        !self.is_deciding()
            && self
                .selected_job()
                .is_some_and(|job| job.status_key == "awaiting-approval" && job.decision.is_none())
    }

    pub fn decide_selected(&mut self, action: EvolutionDecisionAction) {
        if !self.can_decide_selected() {
            self.decision_error = Some("select a candidate awaiting approval".to_string());
            return;
        }
        let Some((job_id, short_id)) = self
            .selected_job()
            .map(|job| (job.id.clone(), job.short_id.clone()))
        else {
            self.decision_error = Some("select a candidate awaiting approval".to_string());
            return;
        };
        let request = EvolutionDecisionRequest { job_id, action };
        self.decision_status = Some(format!("{} {}", action.running_label(), short_id));
        self.decision_error = None;
        self.decision_action = Some(action);
        self.decision_rx = Some(spawn_evolution_decision(request));
    }

    pub fn pump_decision_events(
        &mut self,
        backend: &Backend,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        let events = self
            .decision_rx
            .as_ref()
            .map(|rx| rx.try_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut changed = false;
        let mut finished = false;
        for event in events {
            match event {
                EvolutionDecisionEvent::Started => {
                    changed = true;
                }
                EvolutionDecisionEvent::Finished(Ok(())) => {
                    let label = self
                        .decision_action
                        .map(EvolutionDecisionAction::recorded_label)
                        .unwrap_or("decision recorded");
                    self.decision_status = Some(label.to_string());
                    self.decision_error = None;
                    finished = true;
                    changed = true;
                }
                EvolutionDecisionEvent::Finished(Err(error)) => {
                    let label = self
                        .decision_action
                        .map(EvolutionDecisionAction::label)
                        .unwrap_or("Decision");
                    self.decision_status = Some(format!("{label} failed"));
                    self.decision_error = Some(error);
                    finished = true;
                    changed = true;
                }
            }
        }
        if finished {
            self.decision_rx = None;
            self.decision_action = None;
            self.request_dashboard_refresh(
                backend.data_dir.clone(),
                waker,
                false,
                EvolutionDashboardRefreshReason::DecisionFinished,
            );
        }
        changed
    }

    pub fn select(&mut self, index: usize) {
        if self.selected != Some(index) && !self.is_deciding() {
            self.decision_status = None;
            self.decision_error = None;
        }
        self.selected = Some(index);
    }

    pub fn selected_job(&self) -> Option<&EvolutionJobSummary> {
        self.selected
            .and_then(|index| self.dashboard.jobs.get(index))
    }

    fn progress_announcement(&self) -> String {
        if let Some(error) = self.decision_error.as_deref() {
            return format!("Evolution decision failed: {error}");
        }
        if let Some(error) = self.start_error.as_deref() {
            return format!("Evolution job failed: {error}");
        }
        if let Some(error) = self.dashboard_refresh_error() {
            return format!("Candidate queue refresh failed. {error}. Press R to retry.");
        }
        if let Some(status) = self.decision_status.as_deref() {
            return status.to_string();
        }
        if let Some(status) = self.start_status.as_deref() {
            return status.to_string();
        }
        if let Some(status) = self.dashboard_refresh_pending_label() {
            return status.to_string();
        }
        let active = self.dashboard.active_count();
        if active == 1 {
            return "1 evolution job in progress".to_string();
        }
        if active > 1 {
            return format!("{active} evolution jobs in progress");
        }
        "Evolution is idle".to_string()
    }

    fn progress_busy(&self) -> bool {
        self.is_starting()
            || self.is_deciding()
            || self.dashboard_refresh_pending()
            || self.dashboard.active_count() > 0
    }
}

#[derive(Clone, Copy)]
struct TargetRingOption {
    label: &'static str,
    value: &'static str,
}

const EVOLUTION_TARGET_RINGS: [TargetRingOption; 3] = [
    TargetRingOption {
        label: "R2 service",
        value: "r2",
    },
    TargetRingOption {
        label: "R1 runtime",
        value: "r1",
    },
    TargetRingOption {
        label: "R3 module",
        value: "r3",
    },
];

fn target_ring_index_for_value(value: &str) -> usize {
    EVOLUTION_TARGET_RINGS
        .iter()
        .position(|ring| ring.value == value)
        .unwrap_or(1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvolutionStarterObjective {
    PolishApp,
    FixCheck,
    CheckCluster,
}

impl EvolutionStarterObjective {
    const ALL: [Self; STARTER_ACTION_COUNT] = [Self::PolishApp, Self::FixCheck, Self::CheckCluster];

    fn label(self) -> &'static str {
        match self {
            Self::PolishApp => "Polish app",
            Self::FixCheck => "Fix cargo check",
            Self::CheckCluster => "Check cluster",
        }
    }

    fn prompt(self) -> &'static str {
        match self {
            Self::PolishApp => {
                "Improve one sinomo-app workflow for clarity, verify the behavior, and keep low-level details behind explicit controls."
            }
            Self::FixCheck => {
                "Find the next failing focused check in this repository, fix it, and verify with the smallest relevant cargo command."
            }
            Self::CheckCluster => {
                "Check the local cluster status, fix any blocking connection issue, and only roll out if the runtime change requires it."
            }
        }
    }

    fn scope(self) -> &'static str {
        match self {
            Self::PolishApp => "crates/sinomo-app/**",
            Self::FixCheck => "crates/**, docs/**",
            Self::CheckCluster => {
                "crates/sinomo-app/**, crates/sinomo-daemon/**, crates/sinomo-cli/**, crates/sinomo-network/**, docs/clustering-architecture.md"
            }
        }
    }

    fn target_ring(self) -> &'static str {
        match self {
            Self::PolishApp => "r2",
            Self::FixCheck | Self::CheckCluster => "r1",
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            Self::PolishApp => IconGlyph::Sparkles,
            Self::FixCheck => IconGlyph::Check,
            Self::CheckCluster => IconGlyph::Storage,
        }
    }

    fn width(self) -> f32 {
        match self {
            Self::PolishApp => 122.0,
            Self::FixCheck => 136.0,
            Self::CheckCluster => 130.0,
        }
    }
}

fn parse_scope_paths(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch == ',' || ch == '\n' || ch == ';')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

fn theme_reader(shared: &Shared) -> ThemeReader {
    let shared = shared.clone();
    Rc::new(move || shared.theme())
}

fn small_theme_reader(shared: &Shared) -> ThemeReader {
    let shared = shared.clone();
    Rc::new(move || shared.theme().with_size(ControlSize::Small))
}

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

fn request_window_relayout(ctx: &mut EventCtx) {
    ctx.request_measure();
    ctx.request_arrange();
    ctx.request_paint();
    ctx.request_semantics();
}

fn ready_backend(shared: &Shared) -> Option<Backend> {
    shared
        .borrow()
        .ready
        .as_ref()
        .map(|ready| ready.backend.clone())
}

fn evolution_header_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionHeaderStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution header structure", |key| key.header.clone());
    let s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| {
        let build_s = s.clone();
        let build_r = Rc::clone(&r);
        WidgetPod::new(sui::RebuildOnConstraints::new(
            true,
            move |constraints| {
                let width = if constraints.max.width.is_finite() {
                    constraints.max.width
                } else {
                    constraints.min.width
                };
                width >= 740.0
            },
            move |show_metrics| build_evolution_header(&build_s, &build_r, *show_metrics),
        ))
    })
}

fn build_evolution_header(shared: &Shared, reader: &ThemeReader, show_metrics: bool) -> WidgetPod {
    let (dashboard, refresh_pending, refresh_error) = {
        let state = shared.evolution();
        (
            state.dashboard.clone(),
            state.dashboard_refresh_pending(),
            state.dashboard_refresh_error().map(str::to_string),
        )
    };
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let subtitle = evolution_header_subtitle_with_refresh(
        &dashboard,
        refresh_pending,
        refresh_error.as_deref(),
    );
    let subtitle_color = if refresh_error.is_some() {
        pal.bad
    } else if refresh_pending {
        pal.accent_text
    } else {
        pal.text_muted
    };

    let summary = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(2.0)
        .with_child(Label::new("Evolution").style(pal.text_style(
            &theme,
            theme.text.xl.size,
            FontWeight::SEMIBOLD,
            pal.text,
        )))
        .with_child(Label::new(subtitle).style(pal.text_style(
            &theme,
            theme.text.sm.size,
            FontWeight::NORMAL,
            subtitle_color,
        )));
    let mut row = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(8.0)
        .with_item(summary, FlexItem::flex(1.0).min_width(0.0));
    if show_metrics {
        row.push(
            StatusBadge::new(format!("{} awaiting", dashboard.awaiting_decision_count()))
                .tone(SemanticTone::Warning)
                .theme_when(clone_reader(reader)),
        );
        row.push(
            StatusBadge::new(format!("{} failed", dashboard.failed_count()))
                .tone(SemanticTone::Danger)
                .theme_when(clone_reader(reader)),
        );
        row.push(
            StatusBadge::new(format!("{} promoted", dashboard.promoted_count()))
                .tone(SemanticTone::Success)
                .theme_when(clone_reader(reader)),
        );
    }

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

fn evolution_queue_header_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<(AppThemeMode, AppTextScale)> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution queue header theme", |key| key.theme);
    let build_s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| {
        let theme = r();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&build_s));
        let column = Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(1.0)
            .with_child(Label::new("Candidate queue").style(pal.text_style(
                &theme,
                theme.text.sm.size,
                FontWeight::SEMIBOLD,
                pal.text,
            )))
            .with_child(Label::new("Observed to promoted").style(pal.text_style(
                &theme,
                theme.text.xs.size,
                FontWeight::NORMAL,
                pal.text_faint,
            )));
        WidgetPod::new(
            Surface::new(SurfaceRole::Sidebar, column)
                .border(SurfaceBorder::Bottom)
                .padding(Insets {
                    left: PAD,
                    top: 6.0,
                    right: PAD,
                    bottom: 6.0,
                })
                .fill()
                .theme_when(clone_reader(&r)),
        )
    })
}

fn evolution_queue_empty_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionQueueEmptyStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution queue empty structure", |key| {
            key.queue_empty.clone()
        });
    let build_s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| {
        let message = build_s
            .evolution()
            .dashboard
            .error
            .clone()
            .unwrap_or_else(|| "Start a job to create the first candidate".to_string());
        WidgetPod::new(
            EmptyState::new("No candidates", compact(&message, 48))
                .name("Evolution candidate queue empty")
                .icon(IconGlyph::Sparkles)
                .transparent()
                .theme_when(clone_reader(&r)),
        )
    })
}

fn evolution_start_chrome_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionStartStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution start chrome structure", |key| key.start.clone());
    let s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| {
        let build_s = s.clone();
        let build_r = Rc::clone(&r);
        WidgetPod::new(sui::RebuildOnConstraints::new(
            (true, true),
            move |constraints| {
                let width = if constraints.max.width.is_finite() {
                    constraints.max.width
                } else {
                    constraints.min.width
                };
                (width >= 640.0, (width - PAD * 2.0) >= START_BREAKPOINT)
            },
            move |(inline, wide_header)| {
                build_evolution_start_chrome(&build_s, &build_r, *inline, *wide_header)
            },
        ))
    })
}

fn evolution_start_status(shared: &Shared) -> (String, SemanticTone) {
    let state = shared.evolution();
    if let Some(error) = &state.start_error {
        return (
            format!("Failed - {}", compact(error, 108)),
            SemanticTone::Danger,
        );
    }
    if let Some(status) = &state.start_status {
        return (
            compact(status, 108),
            if state.is_starting() {
                SemanticTone::Accent
            } else {
                SemanticTone::Success
            },
        );
    }
    let objective = state.objective_draft.trim();
    if objective.is_empty() {
        let label = if state.dashboard.jobs.is_empty() {
            "Enter an objective to start a supervised evolution job".to_string()
        } else {
            format!(
                "Enter an objective to start - {} jobs tracked",
                state.dashboard.jobs.len()
            )
        };
        return (label, SemanticTone::Neutral);
    }
    if state.start_advanced_open {
        let ring = EVOLUTION_TARGET_RINGS
            .get(state.target_ring_index)
            .map(|option| option.label)
            .unwrap_or("R1 runtime");
        let scope_count = parse_scope_paths(&state.scope_draft).len();
        let scope = match scope_count {
            0 => "workspace policy scope".to_string(),
            1 => "1 scoped path".to_string(),
            count => format!("{count} scoped paths"),
        };
        (format!("Ready for {ring} - {scope}"), SemanticTone::Accent)
    } else {
        (
            "Ready to start - use Options for scope and target ring".to_string(),
            SemanticTone::Accent,
        )
    }
}

fn build_evolution_start_chrome(
    shared: &Shared,
    reader: &ThemeReader,
    inline: bool,
    wide_header: bool,
) -> WidgetPod {
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let (status, tone) = evolution_start_status(shared);
    let status_color = if tone == SemanticTone::Neutral {
        pal.text_muted
    } else {
        theme.semantic_tone_color(tone)
    };
    let title = Label::new("Start job").style(pal.text_style(
        &theme,
        theme.text.base.size,
        FontWeight::SEMIBOLD,
        pal.text,
    ));
    let status = Label::new(status).style(pal.text_style(
        &theme,
        theme.text.sm.size,
        FontWeight::MEDIUM,
        status_color,
    ));
    let header = if inline {
        Flex::horizontal()
            .align_items(Alignment::Center)
            .gap(18.0)
            .with_child(title)
            .with_item(status, FlexItem::flex(1.0).min_width(0.0))
    } else {
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(1.0)
            .with_child(title)
            .with_child(status)
    };
    let header_height = if wide_header {
        START_WIDE_HEADER_H
    } else {
        START_NARROW_HEADER_H
    };
    let header = Surface::window(header)
        .padding(Insets {
            left: PAD,
            right: PAD,
            top: if inline { 8.0 } else { 7.0 },
            bottom: if inline { 8.0 } else { 7.0 },
        })
        .fill_width()
        .theme_when(clone_reader(reader));
    let body = Flex::vertical()
        .align_items(Alignment::Stretch)
        .with_child(SizedBox::new().height(header_height).with_child(header))
        .spacer();
    WidgetPod::new(
        Surface::window(body)
            .border(SurfaceBorder::Bottom)
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn evolution_detail_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionDetailStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution detail structure", |key| key.detail.clone());
    let s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| build_evolution_detail(&s, &r))
}

fn phase_tone(phase: EvolutionPhase) -> SemanticTone {
    match phase {
        EvolutionPhase::Active => SemanticTone::Accent,
        EvolutionPhase::AwaitingDecision => SemanticTone::Warning,
        EvolutionPhase::Passed => SemanticTone::Success,
        EvolutionPhase::Failed => SemanticTone::Danger,
        EvolutionPhase::Recovered => SemanticTone::Neutral,
    }
}

fn detail_rows_surface(rows: Vec<(String, String)>, reader: &ThemeReader) -> Surface {
    let mut column = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    for (label, value) in rows {
        column.push(
            DetailRow::new(label, value)
                .max_value_lines(2)
                .theme_when(clone_reader(reader)),
        );
    }
    Surface::panel(column)
        .padding(Insets::all(12.0))
        .fill_width()
        .theme_when(clone_reader(reader))
}

fn evolution_summary_grid(job: &EvolutionJobSummary, reader: &ThemeReader) -> Surface {
    let entries = [
        ("Target", job.target_ring.clone()),
        (
            "Verification",
            job.verification_outcome
                .clone()
                .unwrap_or_else(|| "not recorded".to_string()),
        ),
        ("Changed files", job.changed_files.len().to_string()),
        ("Commands", job.verification_command_count.to_string()),
        ("Replay reports", job.module_replay_count.to_string()),
        ("Artifacts", job.artifact_ref_count.to_string()),
    ];
    let mut grid = Flex::vertical().align_items(Alignment::Stretch).gap(10.0);
    for pair in entries.chunks(2) {
        let mut row = Flex::horizontal().align_items(Alignment::Stretch).gap(16.0);
        for (label, value) in pair {
            row.push_item(
                DetailRow::new(*label, value.clone())
                    .max_value_lines(1)
                    .theme_when(clone_reader(reader)),
                FlexItem::fill().min_width(0.0),
            );
        }
        grid.push(row);
    }
    Surface::panel(grid)
        .padding(Insets::all(12.0))
        .fill_width()
        .theme_when(clone_reader(reader))
}

fn build_evolution_detail(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (job, start_ready, starting) = {
        let state = shared.evolution();
        (
            state
                .selected
                .and_then(|index| state.dashboard.jobs.get(index))
                .cloned(),
            state.can_start(),
            state.is_starting(),
        )
    };
    let Some(job) = job else {
        let detail = empty_detail_rows(start_ready, starting)
            .into_iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join(". ");
        return WidgetPod::new(
            EmptyState::new(
                "No evolution job selected",
                empty_detail_subtitle(start_ready, starting),
            )
            .icon(IconGlyph::Sparkles)
            .detail(detail)
            .transparent()
            .theme_when(clone_reader(reader)),
        );
    };

    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let header = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(12.0)
        .with_item(
            Label::new(job.objective.clone()).style(pal.text_style(
                &theme,
                theme.text.xl.size,
                FontWeight::SEMIBOLD,
                pal.text,
            )),
            FlexItem::flex(1.0).min_width(0.0),
        )
        .with_child(
            StatusBadge::new(job.status_label.clone())
                .tone(phase_tone(job.phase))
                .theme_when(clone_reader(reader)),
        );
    let meta = Label::new(format!(
        "{} - created {} - branch {}",
        job.short_id,
        compact(&job.created_at, 24),
        compact(&job.candidate_branch, 28)
    ))
    .style(pal.text_style(
        &theme,
        theme.text.sm.size,
        FontWeight::NORMAL,
        pal.text_muted,
    ));
    let lifecycle_height = LifecycleMetrics::for_theme(&theme).height;
    let lifecycle = PanelSection::new(
        "Lifecycle",
        SizedBox::new()
            .height(lifecycle_height)
            .with_child(EvolutionLifecycle::new(shared.clone(), job.clone())),
    )
    .theme_when(clone_reader(reader));
    let evidence = PanelSection::new("Evidence", detail_rows_surface(evidence_rows(&job), reader))
        .theme_when(clone_reader(reader));
    let scope = PanelSection::new("Scope", detail_rows_surface(scope_rows(&job), reader))
        .theme_when(clone_reader(reader));
    let content = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(14.0)
        .with_child(header)
        .with_child(meta)
        .with_child(lifecycle)
        .with_child(evolution_summary_grid(&job, reader))
        .with_child(evidence)
        .with_child(scope);
    let content = Surface::window(content)
        .padding(Insets::all(PAD))
        .fill_width()
        .theme_when(clone_reader(reader));
    WidgetPod::new(
        ScrollView::vertical(content)
            .name("Evolution candidate details")
            .theme(theme)
            .viewport_width_hint(true),
    )
}

fn evolution_rail_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionRailStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution rail structure", |key| key.rail.clone());
    let s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| build_evolution_rail(&s, &r))
}

fn compact_property_rows_surface(
    rows: Vec<(String, String)>,
    reader: &ThemeReader,
    reserve_action_space: bool,
    state_tone: Option<SemanticTone>,
) -> Surface {
    let theme = reader();
    let mut column = Flex::vertical().align_items(Alignment::Stretch).gap(5.0);
    for (index, (label, value)) in rows.into_iter().enumerate() {
        let color = if index == 0 {
            state_tone
                .map(|tone| theme.semantic_tone_color(tone))
                .unwrap_or(theme.palette.text)
        } else {
            theme.palette.text
        };
        let value = Label::new(value).style({
            let mut style = theme.body_text_style();
            style.font_size = theme.text.xs.size;
            style.line_height = theme.text.xs.line_height;
            style.color = color;
            style
        });
        column.push(
            PropertyRow::new(label, value)
                .inline()
                .label_width(82.0)
                .gap(6.0)
                .theme_when(clone_reader(reader)),
        );
    }
    if reserve_action_space {
        column.push(SizedBox::new().height(34.0));
    }
    Surface::panel(column)
        .padding(Insets::all(11.0))
        .fill_width()
        .theme_when(clone_reader(reader))
}

fn decision_state_tone(action: &DecisionActionStatus) -> SemanticTone {
    if action.error.is_some() {
        SemanticTone::Danger
    } else if action.deciding {
        SemanticTone::Accent
    } else {
        SemanticTone::Neutral
    }
}

fn build_evolution_rail(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (dashboard, job, action, readiness_details_open) = {
        let state = shared.evolution();
        (
            state.dashboard.clone(),
            state
                .selected
                .and_then(|index| state.dashboard.jobs.get(index))
                .cloned(),
            DecisionActionStatus {
                status: state.decision_status.clone(),
                error: state.decision_error.clone(),
                deciding: state.is_deciding(),
            },
            state.readiness_details_open,
        )
    };
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let title = Label::new("Safety and evidence").style(pal.text_style(
        &theme,
        theme.text.base.size,
        FontWeight::SEMIBOLD,
        pal.text,
    ));
    let mut content = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(14.0)
        .with_child(title);
    if let Some(job) = job {
        content.push(
            PanelSection::new(
                "Operator decision",
                compact_property_rows_surface(
                    decision_action_rows(&job, &action),
                    reader,
                    true,
                    Some(decision_state_tone(&action)),
                ),
            )
            .theme_when(clone_reader(reader)),
        );
        content.push(
            PanelSection::new(
                "Review summary",
                compact_property_rows_surface(selected_review_rows(&job), reader, false, None),
            )
            .theme_when(clone_reader(reader)),
        );
    } else {
        content.push(
            PanelSection::new(
                "Readiness",
                compact_property_rows_surface(
                    readiness_rows(&dashboard, readiness_details_open),
                    reader,
                    false,
                    None,
                ),
            )
            .theme_when(clone_reader(reader)),
        );
    }
    let content = Surface::new(SurfaceRole::Sidebar, content)
        .padding(Insets::all(PAD))
        .fill_width()
        .theme_when(clone_reader(reader));
    let scroll = ScrollView::vertical(content)
        .name("Evolution safety and evidence")
        .theme(theme)
        .viewport_width_hint(true);
    WidgetPod::new(
        Surface::new(SurfaceRole::Sidebar, scroll)
            .border(SurfaceBorder::Left)
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn evolution_narrow_action_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> sui::RebuildOnChange<EvolutionNarrowActionStructure> {
    let source = shared
        .signals()
        .evolution_structure
        .select_named("evolution narrow action structure", |key| {
            key.narrow_action.clone()
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    sui::RebuildOnChange::new_observable(source, move |_key| build_evolution_narrow_action(&s, &r))
}

fn build_evolution_narrow_action(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (job, action) = {
        let state = shared.evolution();
        (
            state
                .selected
                .and_then(|index| state.dashboard.jobs.get(index))
                .cloned(),
            DecisionActionStatus {
                status: state.decision_status.clone(),
                error: state.decision_error.clone(),
                deciding: state.is_deciding(),
            },
        )
    };
    let (label, tone, icon) = if let Some(error) = action.error.as_deref() {
        (
            format!("Decision failed - {}", compact(error, 72)),
            SemanticTone::Danger,
            IconGlyph::Alert,
        )
    } else if let Some(status) = action.status.as_deref() {
        (
            compact(status, 84),
            if action.deciding {
                SemanticTone::Accent
            } else {
                SemanticTone::Neutral
            },
            if action.deciding {
                IconGlyph::Sparkles
            } else {
                IconGlyph::Check
            },
        )
    } else if let Some(job) = job {
        (
            format!("{} - {}", job.short_id, job.status_label),
            phase_tone(job.phase),
            IconGlyph::FileText,
        )
    } else {
        (
            "Select a candidate to review".to_string(),
            SemanticTone::Neutral,
            IconGlyph::FileText,
        )
    };
    let status = Flex::horizontal()
        .align_items(Alignment::Center)
        .with_child(
            StatusBadge::new(label)
                .icon(icon)
                .tone(tone)
                .theme_when(clone_reader(reader)),
        )
        .spacer();
    WidgetPod::new(
        Surface::new(SurfaceRole::Sidebar, status)
            .appearance(SurfaceAppearance::Standard)
            .border(SurfaceBorder::Top)
            .padding(Insets {
                left: PAD,
                right: PAD,
                top: 6.0,
                bottom: 40.0,
            })
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

struct EvolutionLifecycle {
    shared: Shared,
    job: EvolutionJobSummary,
}

impl EvolutionLifecycle {
    fn new(shared: Shared, job: EvolutionJobSummary) -> Self {
        Self { shared, job }
    }
}

impl Widget for EvolutionLifecycle {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            480.0
        };
        let height = LifecycleMetrics::for_theme(&self.shared.theme()).height;
        constraints.clamp(Size::new(width, height))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        paint_lifecycle_track(ctx, &theme, &pal, ctx.bounds(), &self.job);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node =
            SemanticsNode::new(ctx.widget_id(), SemanticsRole::ProgressBar, ctx.bounds());
        node.name = Some("Evolution lifecycle".to_string());
        node.value = Some(SemanticsValue::Text(format!(
            "{}; stage {} of 11",
            self.job.status_label,
            stage_index(&self.job.status_key) + 1
        )));
        ctx.push(node);
    }
}

pub fn view(shared: Shared) -> impl Widget {
    EvolutionView::new(shared)
}

struct EvolutionView {
    shared: Shared,
    header: WidgetPod,
    start_chrome: WidgetPod,
    start: WidgetPod,
    queue_surface: WidgetPod,
    queue_header: WidgetPod,
    queue: WidgetPod,
    queue_empty: WidgetPod,
    detail: WidgetPod,
    rail: WidgetPod,
    narrow_action_status: WidgetPod,
    readiness_show: WidgetPod,
    readiness_hide: WidgetPod,
    approve: WidgetPod,
    reject: WidgetPod,
    pump_alive: Cell<bool>,
    bounds: Rect,
}

impl EvolutionView {
    fn new(shared: Shared) -> Self {
        let reader = theme_reader(&shared);
        let header = evolution_header_host(&shared, &reader);
        let start_chrome = evolution_start_chrome_host(&shared, &reader);
        let start = WidgetPod::new(StartControls::new(shared.clone()));
        let queue_surface = Surface::sidebar(SizedBox::new())
            .fill()
            .theme_when(clone_reader(&reader));
        let queue_header = evolution_queue_header_host(&shared, &reader);
        let queue_source = shared.signals().evolution_structure.select_named(
            "evolution virtual candidate collection",
            |key| {
                let selected = key.detail.selected.as_ref().map(|job| job.id.clone());
                widgets::VirtualCollectionSnapshot::new(
                    key.header
                        .dashboard
                        .jobs
                        .iter()
                        .cloned()
                        .map(|job| (job.id.clone(), job))
                        .collect(),
                    selected,
                )
            },
        );
        let queue_shared = shared.clone();
        let queue_theme = Rc::clone(&reader);
        let queue = widgets::VirtualCollectionHost::new(
            "Evolution candidate collection",
            queue_source,
            move |model, state| {
                let row_shared = queue_shared.clone();
                let action_shared = queue_shared.clone();
                VirtualList::new("Evolution candidate queue", model, move |_id, job| {
                    EvolutionQueueRow::new(row_shared.clone(), job)
                })
                .state(state)
                .estimated_row_height(QUEUE_ROW_MIN_H)
                .theme_when(clone_reader(&queue_theme))
                .row_name(|_id, job| {
                    if job.objective.trim().is_empty() {
                        format!("Evolution candidate {}", job.short_id)
                    } else {
                        job.objective.clone()
                    }
                })
                .row_description(|_id, job| {
                    format!(
                        "{}; candidate {}; target {}; {} required gates",
                        job.status_label, job.short_id, job.target_ring, job.required_gate_count
                    )
                })
                .on_change(move |job_id| {
                    let index = {
                        let evolution = action_shared.evolution();
                        evolution
                            .dashboard
                            .jobs
                            .iter()
                            .position(|job| job.id == job_id)
                    };
                    if let Some(index) = index {
                        action_shared.evolution_mut().select(index);
                    }
                })
            },
        );
        let queue_empty = evolution_queue_empty_host(&shared, &reader);
        let detail = evolution_detail_host(&shared, &reader);
        let rail = evolution_rail_host(&shared, &reader);
        let narrow_action_status = evolution_narrow_action_host(&shared, &reader);
        let readiness_show = {
            let s = shared.clone();
            Button::new("Show details")
                .icon(IconGlyph::ChevronDown)
                .min_height(DECISION_BUTTON_H)
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().set_readiness_details_open(true);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                })
        };
        let readiness_hide = {
            let s = shared.clone();
            Button::new("Hide details")
                .icon(IconGlyph::ChevronUp)
                .min_height(DECISION_BUTTON_H)
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().set_readiness_details_open(false);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                })
        };
        let approve = {
            let s = shared.clone();
            Button::new("Approve")
                .icon(IconGlyph::Check)
                .min_height(DECISION_BUTTON_H)
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Success)
                .enabled_when({
                    let s = s.clone();
                    move || s.evolution().can_decide_selected()
                })
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut()
                        .decide_selected(EvolutionDecisionAction::Approve);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_animation_frame();
                    ctx.request_paint();
                })
        };
        let reject = {
            let s = shared.clone();
            Button::new("Reject")
                .icon(IconGlyph::Close)
                .min_height(DECISION_BUTTON_H)
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Danger)
                .enabled_when({
                    let s = s.clone();
                    move || s.evolution().can_decide_selected()
                })
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut()
                        .decide_selected(EvolutionDecisionAction::Reject);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_animation_frame();
                    ctx.request_paint();
                })
        };
        Self {
            shared,
            header: WidgetPod::new(header),
            start_chrome: WidgetPod::new(start_chrome),
            start,
            queue_surface: WidgetPod::new(queue_surface),
            queue_header: WidgetPod::new(queue_header),
            queue: WidgetPod::new(queue),
            queue_empty: WidgetPod::new(queue_empty),
            detail: WidgetPod::new(detail),
            rail: WidgetPod::new(rail),
            narrow_action_status: WidgetPod::new(narrow_action_status),
            readiness_show: WidgetPod::new(readiness_show),
            readiness_hide: WidgetPod::new(readiness_hide),
            approve: WidgetPod::new(approve),
            reject: WidgetPod::new(reject),
            pump_alive: Cell::new(false),
            bounds: Rect::ZERO,
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn ensure_pump(&self, ctx: &mut EventCtx) {
        if !self.pump_alive.get() {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
    }

    fn pump(&self, ctx: &mut EventCtx) {
        self.pump_alive.set(false);
        let Some(backend) = ready_backend(&self.shared) else {
            return;
        };
        let waker = self.shared.borrow().waker.clone();
        let has_external_waker = waker.is_some();
        let changed = {
            let mut evolution = self.shared.evolution_mut_unpublished();
            let start_changed = evolution.pump_start_events(&backend, waker.clone());
            let decision_changed = evolution.pump_decision_events(&backend, waker);
            let dashboard_changed = evolution.pump_dashboard_load();
            start_changed || decision_changed || dashboard_changed
        };
        if changed {
            self.shared.publish_evolution_signals();
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        }
        let keep_polling = {
            let evolution = self.shared.evolution();
            evolution.has_pending_worker()
                || (!has_external_waker && evolution.dashboard_refresh_pending())
        };
        if keep_polling {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
    }
}

#[derive(Clone, Copy)]
struct StartControlsLayout {
    objective_label: Rect,
    options_label: Rect,
    scope_label: Rect,
    target_label: Rect,
    starter_actions: [Rect; STARTER_ACTION_COUNT],
    objective: Rect,
    options: Rect,
    scope: Rect,
    target: Rect,
    start: Rect,
}

struct StartControls {
    shared: Shared,
    labels: [WidgetPod; 4],
    objective: WidgetPod,
    options: WidgetPod,
    scope: WidgetPod,
    target: WidgetPod,
    start: WidgetPod,
    starter_actions: [WidgetPod; STARTER_ACTION_COUNT],
}

impl StartControls {
    fn new(shared: Shared) -> Self {
        let reader = small_theme_reader(&shared);

        let objective = {
            let source = shared
                .signals()
                .evolution_objective_revision
                .select_named("evolution objective editor replacement", |revision| {
                    *revision
                });
            let s = shared.clone();
            let r = Rc::clone(&reader);
            sui::RebuildOnChange::new_observable(source, move |_revision| {
                WidgetPod::new(objective_input(&s, &r))
            })
        };

        let options = {
            let s = shared.clone();
            Button::new("Options")
                .icon(IconGlyph::ChevronDown)
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().toggle_start_advanced();
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                })
        };

        let scope = {
            let source = shared
                .signals()
                .evolution_scope_revision
                .select_named("evolution scope editor replacement", |revision| *revision);
            let s = shared.clone();
            let r = Rc::clone(&reader);
            sui::RebuildOnChange::new_observable(source, move |_revision| {
                let s_for_change = s.clone();
                WidgetPod::new(
                    TextInput::new("evolution-scope")
                        .value(s.evolution().scope_draft.clone())
                        .placeholder(SCOPE_PLACEHOLDER)
                        .theme_when(clone_reader(&r))
                        .on_change(move |value| {
                            s_for_change.evolution_mut().set_scope_draft(value);
                        }),
                )
            })
        };

        let target = {
            let s = shared.clone();
            let options: Vec<_> = EVOLUTION_TARGET_RINGS
                .iter()
                .map(|ring| ring.label)
                .collect();
            Select::new("evolution-target-ring")
                .options(options)
                .placeholder("Ring")
                .selected_when({
                    let s = s.clone();
                    move || Some(s.evolution().target_ring_index)
                })
                .theme_when(clone_reader(&reader))
                .on_change(move |index, _label| {
                    s.evolution_mut().set_target_ring_index(index);
                })
        };

        let start = {
            let s = shared.clone();
            Button::new("Start")
                .icon(IconGlyph::Check)
                .enabled_when({
                    let s = s.clone();
                    move || s.evolution().can_start()
                })
                .theme_when(clone_reader(&reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Accent)
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().start();
                    request_window_relayout(ctx);
                })
        };
        let labels = ["Objective", "Advanced", "Scope", "Target ring"].map(|label| {
            WidgetPod::new(
                SectionLabel::new(label)
                    .semantic_name(format!("Evolution {label}"))
                    .theme_when(clone_reader(&reader)),
            )
        });
        let starter_actions = std::array::from_fn(|index| {
            let action = EvolutionStarterObjective::ALL[index];
            let s = shared.clone();
            let enabled_shared = shared.clone();
            let reader = Rc::clone(&reader);
            WidgetPod::new(
                Button::new(action.label())
                    .icon(action.icon())
                    .min_height(STARTER_ACTION_H)
                    .enabled_when(move || !enabled_shared.evolution().is_starting())
                    .theme_when(clone_reader(&reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
                    .on_press_with_ctx(move |ctx| {
                        s.evolution_mut().apply_starter_objective(action);
                        request_window_relayout(ctx);
                    }),
            )
        });

        Self {
            shared,
            labels,
            objective: WidgetPod::new(objective),
            options: WidgetPod::new(options),
            scope: WidgetPod::new(scope),
            target: WidgetPod::new(target),
            start: WidgetPod::new(start),
            starter_actions,
        }
    }
}

fn objective_input(shared: &Shared, reader: &ThemeReader) -> TextInput {
    let s = shared.clone();
    TextInput::new("evolution-objective")
        .value(shared.evolution().objective_draft.clone())
        .placeholder(OBJECTIVE_PLACEHOLDER)
        .theme_when(clone_reader(reader))
        .on_change(move |value| {
            s.evolution_mut().set_objective_draft(value);
        })
}

impl Widget for StartControls {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let bounds = Rect::new(0.0, 0.0, constraints.max.width, constraints.max.height);
        let advanced_open = self.shared.evolution().start_advanced_open;
        let layout = start_controls_layout(bounds, advanced_open);
        for (index, action) in self.starter_actions.iter_mut().enumerate() {
            action.measure(
                ctx,
                Constraints::tight(rect_size(layout.starter_actions[index])),
            );
        }
        for (index, label) in self.labels.iter_mut().enumerate() {
            let rect = match index {
                0 => layout.objective_label,
                1 => layout.options_label,
                2 if advanced_open => layout.scope_label,
                3 if advanced_open => layout.target_label,
                _ => Rect::ZERO,
            };
            label.measure(ctx, Constraints::tight(rect_size(rect)));
        }
        self.objective
            .measure(ctx, Constraints::tight(rect_size(layout.objective)));
        self.options
            .measure(ctx, Constraints::tight(rect_size(layout.options)));
        if advanced_open {
            self.scope
                .measure(ctx, Constraints::tight(rect_size(layout.scope)));
            self.target
                .measure(ctx, Constraints::tight(rect_size(layout.target)));
        } else {
            self.scope.measure(ctx, Constraints::tight(Size::ZERO));
            self.target.measure(ctx, Constraints::tight(Size::ZERO));
        }
        self.start
            .measure(ctx, Constraints::tight(rect_size(layout.start)));
        constraints.max
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let advanced_open = self.shared.evolution().start_advanced_open;
        let layout = start_controls_layout(bounds, advanced_open);
        for (index, action) in self.starter_actions.iter_mut().enumerate() {
            action.arrange(ctx, layout.starter_actions[index]);
        }
        for (index, label) in self.labels.iter_mut().enumerate() {
            let rect = match index {
                0 => layout.objective_label,
                1 => layout.options_label,
                2 if advanced_open => layout.scope_label,
                3 if advanced_open => layout.target_label,
                _ => Rect::ZERO,
            };
            label.arrange(ctx, rect);
        }
        self.objective.arrange(ctx, layout.objective);
        self.options.arrange(ctx, layout.options);
        if advanced_open {
            self.scope.arrange(ctx, layout.scope);
            self.target.arrange(ctx, layout.target);
        } else {
            self.scope.arrange(ctx, Rect::ZERO);
            self.target.arrange(ctx, Rect::ZERO);
        }
        self.start.arrange(ctx, layout.start);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let advanced_open = self.shared.evolution().start_advanced_open;
        for action in &self.starter_actions {
            action.paint(ctx);
        }
        self.labels[0].paint(ctx);
        self.labels[1].paint(ctx);
        if advanced_open {
            self.labels[2].paint(ctx);
            self.labels[3].paint(ctx);
        }
        self.objective.paint(ctx);
        self.options.paint(ctx);
        if advanced_open {
            self.scope.paint(ctx);
            self.target.paint(ctx);
        }
        self.start.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let advanced_open = self.shared.evolution().start_advanced_open;
        for action in &self.starter_actions {
            action.semantics(ctx);
        }
        self.labels[0].semantics(ctx);
        self.labels[1].semantics(ctx);
        if advanced_open {
            self.labels[2].semantics(ctx);
            self.labels[3].semantics(ctx);
        }
        self.objective.semantics(ctx);
        self.options.semantics(ctx);
        if advanced_open {
            self.scope.semantics(ctx);
            self.target.semantics(ctx);
        }
        self.start.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        for action in &self.starter_actions {
            visitor.visit(action);
        }
        for label in &self.labels {
            visitor.visit(label);
        }
        visitor.visit(&self.objective);
        visitor.visit(&self.options);
        visitor.visit(&self.scope);
        visitor.visit(&self.target);
        visitor.visit(&self.start);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        for action in &mut self.starter_actions {
            visitor.visit(action);
        }
        for label in &mut self.labels {
            visitor.visit(label);
        }
        visitor.visit(&mut self.objective);
        visitor.visit(&mut self.options);
        visitor.visit(&mut self.scope);
        visitor.visit(&mut self.target);
        visitor.visit(&mut self.start);
    }
}

fn start_controls_layout(bounds: Rect, advanced_open: bool) -> StartControlsLayout {
    let block_h = control_block_height();
    let starter_row_h = starter_actions_height_for(bounds.width());
    let starter_bounds = Rect::new(bounds.x(), bounds.y(), bounds.width(), starter_row_h);
    let starter_actions = starter_action_rects(starter_bounds);
    let controls_y = starter_bounds.max_y() + CONTROL_GAP;
    if bounds.width() >= START_BREAKPOINT {
        if advanced_open {
            let label_y = controls_y;
            let row1_y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
            let row2_label_y = row1_y + CONTROL_H + CONTROL_GAP;
            let row2_y = row2_label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
            let fixed_w = START_TARGET_W + START_OPTIONS_W + START_BUTTON_W + CONTROL_GAP * 3.0;
            let scope_w = (bounds.width() - fixed_w).max(0.0);
            let objective = Rect::new(bounds.x(), row1_y, bounds.width(), CONTROL_H);
            let scope = Rect::new(bounds.x(), row2_y, scope_w, CONTROL_H);
            let target = Rect::new(
                scope.max_x() + CONTROL_GAP,
                row2_y,
                START_TARGET_W,
                CONTROL_H,
            );
            let options = Rect::new(
                target.max_x() + CONTROL_GAP,
                row2_y,
                START_OPTIONS_W,
                CONTROL_H,
            );
            let start = Rect::new(
                options.max_x() + CONTROL_GAP,
                row2_y,
                START_BUTTON_W,
                CONTROL_H,
            );
            return StartControlsLayout {
                objective_label: label_rect(objective, label_y),
                options_label: label_rect(options, row2_label_y),
                scope_label: label_rect(scope, row2_label_y),
                target_label: label_rect(target, row2_label_y),
                starter_actions,
                objective,
                options,
                scope,
                target,
                start,
            };
        }

        let fixed_w = START_OPTIONS_W + START_BUTTON_W + CONTROL_GAP * 2.0;
        let flexible_w = (bounds.width() - fixed_w).max(0.0);
        let controls_h = (bounds.max_y() - controls_y).max(0.0);
        let label_y = controls_y + (controls_h - block_h).max(0.0) / 2.0;
        let y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let mut x = bounds.x();
        let objective = Rect::new(x, y, flexible_w, CONTROL_H);
        x = objective.max_x() + CONTROL_GAP;
        let scope = Rect::ZERO;
        let target = Rect::ZERO;
        let options = Rect::new(x, y, START_OPTIONS_W, CONTROL_H);
        x = options.max_x() + CONTROL_GAP;
        let start = Rect::new(x, y, START_BUTTON_W, CONTROL_H);
        StartControlsLayout {
            objective_label: label_rect(objective, label_y),
            options_label: label_rect(options, label_y),
            scope_label: Rect::ZERO,
            target_label: Rect::ZERO,
            starter_actions,
            objective,
            options,
            scope,
            target,
            start,
        }
    } else {
        let label_y = controls_y;
        let row1_y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let row2_label_y = row1_y + CONTROL_H + CONTROL_GAP;
        let row2_y = row2_label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let objective = Rect::new(bounds.x(), row1_y, bounds.width(), CONTROL_H);
        if advanced_open {
            let row3_label_y = row2_y + CONTROL_H + CONTROL_GAP;
            let row3_y = row3_label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
            let target_w = bounds.width().min(START_TARGET_W);
            let options_w = bounds.width().min(START_OPTIONS_W);
            let start_w = bounds.width().min(START_BUTTON_W);
            let scope = Rect::new(bounds.x(), row2_y, bounds.width(), CONTROL_H);
            let target = Rect::new(bounds.x(), row3_y, target_w, CONTROL_H);
            let options = Rect::new(target.max_x() + CONTROL_GAP, row3_y, options_w, CONTROL_H);
            let start_x = (bounds.max_x() - start_w).max(options.max_x() + CONTROL_GAP);
            let start = Rect::new(start_x, row3_y, start_w, CONTROL_H);
            StartControlsLayout {
                objective_label: label_rect(objective, label_y),
                options_label: label_rect(options, row3_label_y),
                scope_label: label_rect(scope, row2_label_y),
                target_label: label_rect(target, row3_label_y),
                starter_actions,
                objective,
                options,
                scope,
                target,
                start,
            }
        } else {
            let options_w = bounds.width().min(START_OPTIONS_W);
            let start_w = bounds.width().min(START_BUTTON_W);
            let options = Rect::new(bounds.x(), row2_y, options_w, CONTROL_H);
            let start_x = (bounds.max_x() - start_w).max(options.max_x() + CONTROL_GAP);
            let start = Rect::new(start_x, row2_y, start_w, CONTROL_H);
            StartControlsLayout {
                objective_label: label_rect(objective, label_y),
                options_label: label_rect(options, row2_label_y),
                scope_label: Rect::ZERO,
                target_label: Rect::ZERO,
                starter_actions,
                objective,
                options,
                scope: Rect::ZERO,
                target: Rect::ZERO,
                start,
            }
        }
    }
}

fn control_block_height() -> f32 {
    CONTROL_LABEL_H + CONTROL_LABEL_GAP + CONTROL_H
}

fn starter_actions_height_for(width: f32) -> f32 {
    let row_w = EvolutionStarterObjective::ALL
        .iter()
        .map(|action| action.width())
        .sum::<f32>()
        + STARTER_ACTION_GAP * (STARTER_ACTION_COUNT.saturating_sub(1) as f32);
    if row_w <= width {
        STARTER_ACTION_H
    } else {
        STARTER_ACTION_H * STARTER_ACTION_COUNT as f32
            + STARTER_ACTION_GAP * (STARTER_ACTION_COUNT.saturating_sub(1) as f32)
    }
}

fn starter_action_rects(bounds: Rect) -> [Rect; STARTER_ACTION_COUNT] {
    let mut rects = [Rect::ZERO; STARTER_ACTION_COUNT];
    let row_w = EvolutionStarterObjective::ALL
        .iter()
        .map(|action| action.width())
        .sum::<f32>()
        + STARTER_ACTION_GAP * (STARTER_ACTION_COUNT.saturating_sub(1) as f32);

    if row_w <= bounds.width() {
        let mut x = bounds.x() + (bounds.width() - row_w).max(0.0) / 2.0;
        for (index, action) in EvolutionStarterObjective::ALL.iter().enumerate() {
            let width = action.width();
            rects[index] = Rect::new(x, bounds.y(), width, STARTER_ACTION_H);
            x += width + STARTER_ACTION_GAP;
        }
    } else {
        let width = bounds.width().min(220.0).max(0.0);
        let x = bounds.x() + (bounds.width() - width).max(0.0) / 2.0;
        for (index, _) in EvolutionStarterObjective::ALL.iter().enumerate() {
            rects[index] = Rect::new(
                x,
                bounds.y() + index as f32 * (STARTER_ACTION_H + STARTER_ACTION_GAP),
                width,
                STARTER_ACTION_H,
            );
        }
    }

    rects
}

fn label_rect(control: Rect, y: f32) -> Rect {
    Rect::new(control.x(), y, control.width(), CONTROL_LABEL_H)
}

impl Widget for EvolutionView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Wake(sui::WakeEvent::AnimationFrame { .. }) = event {
            self.pump(ctx);
            return;
        }

        let pointer_or_key = matches!(event, Event::Pointer(_))
            || matches!(event, Event::Keyboard(k) if k.state == KeyState::Pressed);
        if pointer_or_key {
            self.ensure_pump(ctx);
        }

        if let Event::Keyboard(key) = event
            && key.state == KeyState::Pressed
            && matches!(key.key.as_str(), "r" | "R")
            && self.shared.evolution().dashboard_refresh_error().is_some()
            && !self.shared.evolution().dashboard_refresh_pending()
            && let Some(backend) = ready_backend(&self.shared)
        {
            let waker = self.shared.borrow().waker.clone();
            self.shared
                .evolution_mut()
                .retry_dashboard_refresh(backend.data_dir.clone(), waker);
            ctx.request_paint();
            ctx.request_semantics();
            ctx.set_handled();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let structure = self.shared.signals().evolution_structure.clone();
        let _ = ctx.observe(&structure);
        let advanced_open = self.shared.evolution().start_advanced_open;
        let layout = layout_for(
            Rect::new(0.0, 0.0, constraints.max.width, constraints.max.height),
            advanced_open,
        );
        self.header
            .measure(ctx, Constraints::tight(rect_size(layout.header)));
        self.start_chrome
            .measure(ctx, Constraints::tight(rect_size(layout.start)));
        self.start
            .measure(ctx, Constraints::tight(rect_size(layout.start_controls)));
        self.queue_surface
            .measure(ctx, Constraints::tight(rect_size(layout.queue)));
        let queue_header = Rect::new(
            layout.queue.x(),
            layout.queue.y(),
            layout.queue.width(),
            QUEUE_HEADER_H,
        );
        self.queue_header
            .measure(ctx, Constraints::tight(rect_size(queue_header)));
        if self.shared.evolution().dashboard.jobs.is_empty() {
            self.queue.measure(ctx, Constraints::tight(Size::ZERO));
            self.queue_empty.measure(
                ctx,
                Constraints::tight(rect_size(queue_viewport(layout.queue))),
            );
        } else {
            self.queue.measure(
                ctx,
                Constraints::tight(rect_size(queue_viewport(layout.queue))),
            );
            self.queue_empty
                .measure(ctx, Constraints::tight(Size::ZERO));
        }
        self.detail
            .measure(ctx, Constraints::tight(rect_size(layout.detail)));
        if let Some(rail) = layout.rail {
            self.rail.measure(ctx, Constraints::tight(rect_size(rail)));
            self.narrow_action_status
                .measure(ctx, Constraints::tight(Size::ZERO));
        } else if let Some(action_bar) = layout.action_bar {
            self.rail.measure(ctx, Constraints::tight(Size::ZERO));
            self.narrow_action_status
                .measure(ctx, Constraints::tight(rect_size(action_bar)));
        } else {
            self.rail.measure(ctx, Constraints::tight(Size::ZERO));
            self.narrow_action_status
                .measure(ctx, Constraints::tight(Size::ZERO));
        }
        let (show_readiness_toggle, readiness_details_open) = {
            let evolution = self.shared.evolution();
            let selected_job = evolution
                .selected
                .and_then(|index| evolution.dashboard.jobs.get(index));
            (
                readiness_detail_toggle_visible(&evolution.dashboard, selected_job),
                evolution.readiness_details_open,
            )
        };
        if let Some(rail) = layout.rail.filter(|_| show_readiness_toggle) {
            let rect = readiness_detail_toggle_rect(rail, readiness_details_open);
            if readiness_details_open {
                self.readiness_show
                    .measure(ctx, Constraints::tight(Size::ZERO));
                self.readiness_hide
                    .measure(ctx, Constraints::tight(rect_size(rect)));
            } else {
                self.readiness_show
                    .measure(ctx, Constraints::tight(rect_size(rect)));
                self.readiness_hide
                    .measure(ctx, Constraints::tight(Size::ZERO));
            }
        } else {
            self.readiness_show
                .measure(ctx, Constraints::tight(Size::ZERO));
            self.readiness_hide
                .measure(ctx, Constraints::tight(Size::ZERO));
        }
        if let Some((approve, reject)) = layout_decision_buttons(layout) {
            self.approve
                .measure(ctx, Constraints::tight(rect_size(approve)));
            self.reject
                .measure(ctx, Constraints::tight(rect_size(reject)));
        } else {
            self.approve.measure(ctx, Constraints::tight(Size::ZERO));
            self.reject.measure(ctx, Constraints::tight(Size::ZERO));
        }
        constraints.max
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bounds = bounds;
        let advanced_open = self.shared.evolution().start_advanced_open;
        let layout = layout_for(bounds, advanced_open);
        self.header.arrange(ctx, layout.header);
        self.start_chrome.arrange(ctx, layout.start);
        self.start.arrange(ctx, layout.start_controls);
        self.queue_surface.arrange(ctx, layout.queue);
        self.queue_header.arrange(
            ctx,
            Rect::new(
                layout.queue.x(),
                layout.queue.y(),
                layout.queue.width(),
                QUEUE_HEADER_H,
            ),
        );
        if self.shared.evolution().dashboard.jobs.is_empty() {
            self.queue.arrange(ctx, Rect::ZERO);
            self.queue_empty.arrange(ctx, queue_viewport(layout.queue));
        } else {
            self.queue.arrange(ctx, queue_viewport(layout.queue));
            self.queue_empty.arrange(ctx, Rect::ZERO);
        }
        self.detail.arrange(ctx, layout.detail);
        if let Some(rail) = layout.rail {
            self.rail.arrange(ctx, rail);
            self.narrow_action_status.arrange(ctx, Rect::ZERO);
        } else if let Some(action_bar) = layout.action_bar {
            self.rail.arrange(ctx, Rect::ZERO);
            self.narrow_action_status.arrange(ctx, action_bar);
        } else {
            self.rail.arrange(ctx, Rect::ZERO);
            self.narrow_action_status.arrange(ctx, Rect::ZERO);
        }
        let (show_readiness_toggle, readiness_details_open) = {
            let evolution = self.shared.evolution();
            let selected_job = evolution
                .selected
                .and_then(|index| evolution.dashboard.jobs.get(index));
            (
                readiness_detail_toggle_visible(&evolution.dashboard, selected_job),
                evolution.readiness_details_open,
            )
        };
        if let Some(rail) = layout.rail.filter(|_| show_readiness_toggle) {
            let rect = readiness_detail_toggle_rect(rail, readiness_details_open);
            if readiness_details_open {
                self.readiness_show.arrange(ctx, Rect::ZERO);
                self.readiness_hide.arrange(ctx, rect);
            } else {
                self.readiness_show.arrange(ctx, rect);
                self.readiness_hide.arrange(ctx, Rect::ZERO);
            }
        } else {
            self.readiness_show.arrange(ctx, Rect::ZERO);
            self.readiness_hide.arrange(ctx, Rect::ZERO);
        }
        if let Some((approve, reject)) = layout_decision_buttons(layout) {
            self.approve.arrange(ctx, approve);
            self.reject.arrange(ctx, reject);
        } else {
            self.approve.arrange(ctx, Rect::ZERO);
            self.reject.arrange(ctx, Rect::ZERO);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (_, pal) = self.palette();
        let bounds = ctx.bounds();
        let (dashboard, selected, advanced_open, readiness_details_open) = {
            let state = self.shared.evolution();
            (
                state.dashboard.clone(),
                state.selected,
                state.start_advanced_open,
                state.readiness_details_open,
            )
        };
        let layout = layout_for(bounds, advanced_open);
        let selected_job = selected.and_then(|index| dashboard.jobs.get(index));

        ctx.fill_bounds(pal.window);
        self.header.paint(ctx);
        self.start_chrome.paint(ctx);
        self.start.paint(ctx);
        self.queue_surface.paint(ctx);
        self.queue_header.paint(ctx);
        if dashboard.jobs.is_empty() {
            self.queue_empty.paint(ctx);
        } else {
            self.queue.paint(ctx);
        }
        self.detail.paint(ctx);
        if layout.rail.is_some() {
            self.rail.paint(ctx);
            if readiness_detail_toggle_visible(&dashboard, selected_job) {
                if readiness_details_open {
                    self.readiness_hide.paint(ctx);
                } else {
                    self.readiness_show.paint(ctx);
                }
            }
            if selected_job.is_some() {
                self.approve.paint(ctx);
                self.reject.paint(ctx);
            }
        }
        if layout.action_bar.is_some() {
            self.narrow_action_status.paint(ctx);
            self.approve.paint(ctx);
            self.reject.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let (dashboard, selected, readiness_details_open, advanced_open, progress, progress_busy) = {
            let state = self.shared.evolution();
            (
                state.dashboard.clone(),
                state.selected,
                state.readiness_details_open,
                state.start_advanced_open,
                state.progress_announcement(),
                state.progress_busy(),
            )
        };
        let layout = layout_for(ctx.bounds(), advanced_open);
        let selected_job = selected.and_then(|index| dashboard.jobs.get(index));
        let mut node = SemanticsNode::new(
            ctx.widget_id(),
            SemanticsRole::GenericContainer,
            ctx.bounds(),
        );
        node.name = Some("Evolution dashboard".to_string());
        node.description = Some(if let Some(job) = selected_job {
            format!(
                "Start supervised evolution jobs and review candidate evidence. Selected: {}; {}.",
                job.objective, job.status_label
            )
        } else {
            "Start supervised evolution jobs and review candidate evidence".to_string()
        });
        node.value = selected_job
            .map(|job| SemanticsValue::Text(format!("{}; {}", job.objective, job.status_label)));
        ctx.push(node);

        let mut status = SemanticsNode::new(
            evolution_progress_widget_id(ctx.widget_id()),
            SemanticsRole::BusyIndicator,
            layout.header,
        );
        status.parent = Some(ctx.widget_id());
        status.name = Some("Evolution progress".to_string());
        status.description = Some(progress.clone());
        status.value = Some(SemanticsValue::Text(progress));
        status.state.busy = progress_busy;
        ctx.push(status);

        if readiness_detail_toggle_visible(&dashboard, selected_job) {
            if readiness_details_open {
                self.readiness_hide.semantics(ctx);
            } else {
                self.readiness_show.semantics(ctx);
            }
        }
        self.header.semantics(ctx);
        self.start_chrome.semantics(ctx);
        self.start.semantics(ctx);
        self.queue_surface.semantics(ctx);
        self.queue_header.semantics(ctx);
        if dashboard.jobs.is_empty() {
            self.queue_empty.semantics(ctx);
        } else {
            self.queue.semantics(ctx);
        }
        self.detail.semantics(ctx);
        if layout.rail.is_some() {
            self.rail.semantics(ctx);
        }
        if layout.action_bar.is_some() {
            self.narrow_action_status.semantics(ctx);
        }
        self.approve.semantics(ctx);
        self.reject.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.header);
        visitor.visit(&self.start_chrome);
        visitor.visit(&self.start);
        visitor.visit(&self.queue_surface);
        visitor.visit(&self.queue_header);
        visitor.visit(&self.queue);
        visitor.visit(&self.queue_empty);
        visitor.visit(&self.detail);
        visitor.visit(&self.rail);
        visitor.visit(&self.narrow_action_status);
        visitor.visit(&self.readiness_show);
        visitor.visit(&self.readiness_hide);
        visitor.visit(&self.approve);
        visitor.visit(&self.reject);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.header);
        visitor.visit(&mut self.start_chrome);
        visitor.visit(&mut self.start);
        visitor.visit(&mut self.queue_surface);
        visitor.visit(&mut self.queue_header);
        visitor.visit(&mut self.queue);
        visitor.visit(&mut self.queue_empty);
        visitor.visit(&mut self.detail);
        visitor.visit(&mut self.rail);
        visitor.visit(&mut self.narrow_action_status);
        visitor.visit(&mut self.readiness_show);
        visitor.visit(&mut self.readiness_hide);
        visitor.visit(&mut self.approve);
        visitor.visit(&mut self.reject);
    }
}

#[derive(Clone, Copy)]
struct EvolutionLayout {
    header: Rect,
    start: Rect,
    start_controls: Rect,
    queue: Rect,
    detail: Rect,
    rail: Option<Rect>,
    action_bar: Option<Rect>,
}

#[derive(Clone, Debug, Default)]
struct DecisionActionStatus {
    status: Option<String>,
    error: Option<String>,
    deciding: bool,
}

fn layout_for(bounds: Rect, advanced_open: bool) -> EvolutionLayout {
    let header = Rect::new(bounds.x(), bounds.y(), bounds.width(), HEADER_H);
    let start_h = start_height_for(bounds.width(), advanced_open);
    let start = Rect::new(bounds.x(), header.max_y(), bounds.width(), start_h);
    let controls_h = start_controls_height_for(bounds.width(), advanced_open);
    let start_controls = Rect::new(
        start.x() + PAD,
        start.max_y() - PAD - controls_h,
        (start.width() - PAD * 2.0).max(0.0),
        controls_h,
    );
    let body_y = start.max_y();
    let body_h = (bounds.height() - HEADER_H - start_h).max(0.0);
    let body = Rect::new(bounds.x(), body_y, bounds.width(), body_h);

    if bounds.width() >= 980.0 {
        let queue = Rect::new(body.x(), body.y(), QUEUE_W, body.height());
        let rail = Rect::new(body.max_x() - RAIL_W, body.y(), RAIL_W, body.height());
        let detail_x = queue.max_x() + GAP;
        let detail_w = (rail.x() - detail_x - GAP).max(220.0);
        let detail = Rect::new(detail_x, body.y(), detail_w, body.height());
        EvolutionLayout {
            header,
            start,
            start_controls,
            queue,
            detail,
            rail: Some(rail),
            action_bar: None,
        }
    } else {
        let action_bar_h = NARROW_ACTION_BAR_H.min(body.height());
        let action_bar = Rect::new(
            body.x(),
            body.max_y() - action_bar_h,
            body.width(),
            action_bar_h,
        );
        let content_h = (action_bar.y() - body.y()).max(0.0);
        let queue_h = (content_h * 0.36)
            .clamp(170.0, 260.0)
            .min((content_h - GAP).max(0.0));
        let queue = Rect::new(body.x(), body.y(), body.width(), queue_h);
        let detail = Rect::new(
            body.x(),
            queue.max_y() + GAP,
            body.width(),
            (action_bar.y() - queue.max_y() - GAP).max(0.0),
        );
        EvolutionLayout {
            header,
            start,
            start_controls,
            queue,
            detail,
            rail: None,
            action_bar: Some(action_bar),
        }
    }
}

fn decision_button_layout(rail: Rect) -> Option<(Rect, Rect)> {
    if rail.width() <= PAD * 2.0 {
        return None;
    }
    let inner = rail.inflate(-PAD, -PAD);
    let section = Rect::new(
        inner.x(),
        inner.y() + 38.0,
        inner.width(),
        DECISION_SECTION_H,
    );
    let button_y = section.max_y() - 12.0 - DECISION_BUTTON_H;
    let button_w = ((section.width() - 24.0 - 8.0) / 2.0).max(0.0);
    let approve = Rect::new(section.x() + 12.0, button_y, button_w, DECISION_BUTTON_H);
    let reject = Rect::new(approve.max_x() + 8.0, button_y, button_w, DECISION_BUTTON_H);
    Some((approve, reject))
}

fn narrow_decision_button_layout(action_bar: Rect) -> Option<(Rect, Rect)> {
    if action_bar.width() <= PAD * 2.0 || action_bar.height() < DECISION_BUTTON_H {
        return None;
    }
    let button_y = action_bar.max_y() - 10.0 - DECISION_BUTTON_H;
    let button_w = ((action_bar.width() - PAD * 2.0 - 8.0) / 2.0).max(0.0);
    let approve = Rect::new(action_bar.x() + PAD, button_y, button_w, DECISION_BUTTON_H);
    let reject = Rect::new(approve.max_x() + 8.0, button_y, button_w, DECISION_BUTTON_H);
    Some((approve, reject))
}

fn layout_decision_buttons(layout: EvolutionLayout) -> Option<(Rect, Rect)> {
    layout
        .rail
        .and_then(decision_button_layout)
        .or_else(|| layout.action_bar.and_then(narrow_decision_button_layout))
}

fn readiness_section_height(details_open: bool) -> f32 {
    if details_open { 154.0 } else { 132.0 }
}

fn readiness_detail_toggle_rect(rail: Rect, details_open: bool) -> Rect {
    let inner = rail.inflate(-PAD, -PAD);
    let width = READINESS_DETAIL_BUTTON_W.min(inner.width());
    Rect::new(
        inner.x(),
        inner.y() + 38.0 + readiness_section_height(details_open) + 10.0,
        width,
        DECISION_BUTTON_H,
    )
}

fn readiness_detail_toggle_visible(
    dashboard: &EvolutionDashboard,
    selected_job: Option<&EvolutionJobSummary>,
) -> bool {
    selected_job.is_none() && !dashboard.source.trim().is_empty()
}

fn start_height_for(width: f32, advanced_open: bool) -> f32 {
    let controls_w = (width - PAD * 2.0).max(0.0);
    let header_h = if controls_w >= START_BREAKPOINT {
        START_WIDE_HEADER_H
    } else {
        START_NARROW_HEADER_H
    };
    header_h + start_controls_height_for(controls_w, advanced_open) + PAD
}

fn start_controls_height_for(width: f32, advanced_open: bool) -> f32 {
    let starter_extra = starter_actions_height_for(width) + CONTROL_GAP;
    if width >= START_BREAKPOINT && advanced_open {
        starter_extra + control_block_height() * 2.0 + CONTROL_GAP
    } else if width >= START_BREAKPOINT {
        starter_extra + control_block_height()
    } else if advanced_open {
        starter_extra + control_block_height() * 3.0 + CONTROL_GAP * 2.0
    } else {
        starter_extra + control_block_height() * 2.0 + CONTROL_GAP
    }
}

fn rect_size(rect: Rect) -> Size {
    Size::new(rect.width(), rect.height())
}

fn queue_viewport(queue: Rect) -> Rect {
    Rect::new(
        queue.x(),
        queue.y() + QUEUE_HEADER_H,
        queue.width(),
        (queue.height() - QUEUE_HEADER_H).max(0.0),
    )
}

fn evolution_progress_widget_id(parent: WidgetId) -> WidgetId {
    WidgetId::new(parent.get().wrapping_mul(131).wrapping_add(31_339))
}

fn paint_queue_row(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
    selected: bool,
    metrics: QueueRowMetrics,
) {
    let dot = tone_color(pal, job.phase);
    ctx.fill(
        Path::circle(
            widgets::point(
                rect.x() + PAD,
                rect.y() + metrics.title_top + metrics.title_line_height * 0.5,
            ),
            5.0,
        ),
        dot,
    );
    let objective = compact(
        &job.objective,
        ((rect.width() - 64.0) / 11.0).clamp(16.0, 34.0) as usize,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD + 16.0,
            rect.y() + metrics.title_top,
            rect.width() - PAD * 2.0 - 16.0,
            metrics.title_line_height,
        ),
        &objective,
        pal.text_style(
            theme,
            13.0,
            if selected {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            },
            pal.text,
        ),
    );
    let meta = format!(
        "{} - {} - {} gates",
        job.status_label, job.short_id, job.required_gate_count
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD + 16.0,
            rect.y() + metrics.meta_top,
            rect.width() - PAD * 2.0 - 16.0,
            metrics.meta_line_height,
        ),
        &compact(
            &meta,
            ((rect.width() - 64.0) / 8.7).clamp(18.0, 38.0) as usize,
        ),
        pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_muted),
    );
    let target = job
        .target_module
        .as_deref()
        .map(|module| format!("{} / {}", job.target_ring, short(module, 16)))
        .unwrap_or_else(|| job.target_ring.clone());
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD + 16.0,
            rect.y() + metrics.target_top,
            rect.width() - PAD * 2.0 - 16.0,
            metrics.target_line_height,
        ),
        &compact(
            &target,
            ((rect.width() - 64.0) / 8.7).clamp(18.0, 38.0) as usize,
        ),
        pal.text_style(theme, 11.0, FontWeight::NORMAL, pal.text_faint),
    );
}

fn empty_detail_subtitle(start_ready: bool, starting: bool) -> &'static str {
    if starting {
        "Starting the evolution agent."
    } else if start_ready {
        "Objective ready. Press Start to create a candidate."
    } else {
        EVOLUTION_EMPTY_SUBTITLE
    }
}

fn empty_detail_rows(start_ready: bool, starting: bool) -> Vec<(String, String)> {
    let (next, options) = if starting {
        (
            "wait for the candidate queue",
            "scope and target ring are locked",
        )
    } else if start_ready {
        ("press Start above", "review scope and target ring")
    } else {
        (
            "describe the improvement above",
            "scope and target ring are optional",
        )
    };
    vec![
        ("Next".to_string(), next.to_string()),
        ("Options".to_string(), options.to_string()),
    ]
}

fn paint_lifecycle_track(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
) {
    let stages = [
        ("observed", "Observed"),
        ("scoped", "Scoped"),
        ("proposed", "Proposed"),
        ("implemented", "Implemented"),
        ("built", "Built"),
        ("evaluated", "Evaluated"),
        ("policy-approved", "Approved"),
        ("staged", "Staged"),
        ("shadowed", "Shadowed"),
        ("canary", "Canary"),
        ("promoted", "Promoted"),
    ];
    let current = stage_index(&job.status_key);
    let metrics = LifecycleMetrics::for_theme(theme);
    let y = rect.y() + metrics.track_center_y;
    let left = rect.x() + 12.0;
    let right = rect.max_x() - 12.0;
    let step = if stages.len() > 1 {
        (right - left) / (stages.len() as f32 - 1.0)
    } else {
        0.0
    };
    // Mesh plan stepper: hairline connectors, accent for completed stages, the
    // active dot rides an accent-soft halo, incomplete dots are hollow rings.
    for index in 0..stages.len() - 1 {
        let x1 = left + step * index as f32;
        let x2 = left + step * (index + 1) as f32;
        let color = if index < current {
            pal.accent
        } else {
            pal.border
        };
        let mut builder = sui::PathBuilder::new();
        builder
            .move_to(widgets::point(x1, y))
            .line_to(widgets::point(x2, y));
        ctx.stroke(builder.build(), color, StrokeStyle::new(1.0));
    }
    for (index, _) in stages.iter().enumerate() {
        let x = left + step * index as f32;
        let center = widgets::point(x, y);
        if index == current {
            let tone = tone_color(pal, job.phase);
            let (halo, _) = tone_badge_colors(pal, job.phase);
            ctx.fill(Path::circle(center, 10.0), halo);
            ctx.fill(Path::circle(center, 4.0), tone);
            ctx.stroke(Path::circle(center, 7.0), tone, StrokeStyle::new(1.5));
        } else if index < current {
            ctx.fill(Path::circle(center, 7.0), pal.accent);
        } else {
            ctx.fill(Path::circle(center, 7.0), pal.window);
            ctx.stroke(
                Path::circle(center, 7.0),
                pal.border_strong,
                StrokeStyle::new(1.5),
            );
        }
    }

    let label_style = pal.text_style(theme, 10.0, FontWeight::NORMAL, pal.text_faint);
    let label_widths = stages
        .iter()
        .map(|(_, label)| {
            ctx.measure_text(*label, label_style.clone())
                .map(|measurement| measurement.width + 2.0)
                .unwrap_or_else(|_| {
                    label.chars().count() as f32 * label_style.font_size * 0.56 + 2.0
                })
        })
        .collect::<Vec<_>>();
    for placement in lifecycle_label_placements(rect, step, &label_widths, current) {
        widgets::draw_centered(
            ctx,
            stages[placement.index].1,
            label_style.clone(),
            placement.center_x,
            rect.y() + metrics.label_center_y(),
        );
    }
}

fn lifecycle_label_placements(
    rect: Rect,
    step: f32,
    widths: &[f32],
    current: usize,
) -> Vec<LifecycleLabelPlacement> {
    if widths.is_empty() || rect.width() <= 0.0 {
        return Vec::new();
    }

    let available_left = rect.x() + 2.0;
    let available_right = rect.max_x() - 2.0;
    let available_width = (available_right - available_left).max(0.0);
    if available_width <= 0.0 {
        return Vec::new();
    }

    // The active stage is the most useful caption. Endpoints come next, then remaining stages are
    // admitted only when their measured bounds do not collide with captions already selected.
    let last = widths.len() - 1;
    let current = current.min(last);
    let mut priority = Vec::with_capacity(widths.len());
    for index in [current, 0, last].into_iter().chain(0..widths.len()) {
        if !priority.contains(&index) {
            priority.push(index);
        }
    }

    let track_left = rect.x() + 12.0;
    let mut placements = Vec::<LifecycleLabelPlacement>::new();
    for index in priority {
        let width = widths[index].max(1.0).min(available_width);
        let half_width = width * 0.5;
        let nominal_center = track_left + step * index as f32;
        let center_x = if width >= available_width {
            available_left + available_width * 0.5
        } else {
            nominal_center.clamp(available_left + half_width, available_right - half_width)
        };
        let left = center_x - half_width;
        let right = center_x + half_width;
        if placements.iter().all(|existing| {
            right + LIFECYCLE_LABEL_HORIZONTAL_GAP <= existing.left
                || left >= existing.right + LIFECYCLE_LABEL_HORIZONTAL_GAP
        }) {
            placements.push(LifecycleLabelPlacement {
                index,
                center_x,
                left,
                right,
            });
        }
    }
    placements.sort_by_key(|placement| placement.index);
    placements
}

fn readiness_rows(dashboard: &EvolutionDashboard, details_open: bool) -> Vec<(String, String)> {
    let mut rows = vec![
        ("Decision".to_string(), "select a candidate".to_string()),
        ("Artifacts".to_string(), "shown after selection".to_string()),
        ("Rollback".to_string(), "shown after selection".to_string()),
        ("Source".to_string(), source_readiness_label(dashboard)),
    ];
    if details_open && !dashboard.source.trim().is_empty() {
        rows.push(("Source path".to_string(), tail_path(&dashboard.source, 72)));
    }
    rows
}

fn source_readiness_label(dashboard: &EvolutionDashboard) -> String {
    if dashboard.source.trim().is_empty() {
        "unavailable".to_string()
    } else if dashboard.error.is_some() {
        "needs attention".to_string()
    } else {
        "ready".to_string()
    }
}

fn evidence_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    vec![
        (
            "Outcome".to_string(),
            job.verification_outcome
                .clone()
                .unwrap_or_else(|| "not recorded".to_string()),
        ),
        (
            "Gates".to_string(),
            format!(
                "{} required, {} commands, {} replay reports",
                job.required_gate_count, job.verification_command_count, job.module_replay_count
            ),
        ),
        (
            "Changed".to_string(),
            changed_files_summary(&job.changed_files),
        ),
        (
            "Logs".to_string(),
            format!("{} command logs retained", job.command_log_count),
        ),
    ]
}

fn scope_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    vec![
        ("Workspace".to_string(), tail_path(&job.workspace_root, 72)),
        (
            "Candidate".to_string(),
            tail_path(&job.candidate_workspace, 72),
        ),
        (
            "Allowed".to_string(),
            if job.allowed_paths.is_empty() {
                "not constrained".to_string()
            } else {
                compact(&job.allowed_paths.join(", "), 84)
            },
        ),
        (
            "Forbidden".to_string(),
            if job.forbidden_paths.is_empty() {
                "none recorded".to_string()
            } else {
                compact(&job.forbidden_paths.join(", "), 84)
            },
        ),
    ]
}

fn decision_action_rows(
    job: &EvolutionJobSummary,
    action: &DecisionActionStatus,
) -> Vec<(String, String)> {
    let state = if let Some(error) = &action.error {
        format!("failed - {}", compact(error, 64))
    } else if let Some(status) = &action.status {
        status.clone()
    } else if job.decision.is_some() {
        "decision already recorded".to_string()
    } else if job.status_key == "awaiting-approval" {
        "ready for operator review".to_string()
    } else {
        "no decision pending".to_string()
    };

    if let Some(decision) = &job.decision {
        return vec![
            ("State".to_string(), state),
            ("Outcome".to_string(), decision.outcome.clone()),
            (
                "Preflight".to_string(),
                decision
                    .preflight_passed
                    .map(|passed| if passed { "passed" } else { "failed" }.to_string())
                    .unwrap_or_else(|| "not recorded".to_string()),
            ),
        ];
    }

    vec![
        ("State".to_string(), state),
        ("Candidate".to_string(), job.short_id.clone()),
        (
            "Evidence".to_string(),
            job.scorecard_path
                .as_ref()
                .map(|_| "scorecard available".to_string())
                .unwrap_or_else(|| "scorecard missing".to_string()),
        ),
    ]
}

fn selected_review_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    let rollback = match (
        job.recovery_strategy.trim().is_empty(),
        job.rollback_policy.trim().is_empty(),
    ) {
        (true, true) => "not recorded".to_string(),
        (false, true) => compact(&job.recovery_strategy, 52),
        (true, false) => compact(&job.rollback_policy, 52),
        (false, false) => compact(
            &format!("{} / {}", job.recovery_strategy, job.rollback_policy),
            52,
        ),
    };
    let scorecard = if job.scorecard_path.is_some() {
        "scorecard"
    } else {
        "no scorecard"
    };
    let next = if job.status_key == "awaiting-approval" && job.decision.is_none() {
        "approve or reject".to_string()
    } else if job.decision.is_some() {
        "decision recorded".to_string()
    } else {
        "watch lifecycle status".to_string()
    };

    vec![
        (
            "Verification".to_string(),
            job.verification_outcome
                .clone()
                .unwrap_or_else(|| "not recorded".to_string()),
        ),
        (
            "Gates".to_string(),
            format!(
                "{} required, {} commands",
                job.required_gate_count, job.verification_command_count
            ),
        ),
        ("Rollback".to_string(), rollback),
        (
            "Artifacts".to_string(),
            format!("{} refs, {scorecard}", job.artifact_ref_count),
        ),
        ("Next".to_string(), next),
    ]
}

fn tone_color(pal: &Palette, phase: EvolutionPhase) -> Color {
    match phase {
        EvolutionPhase::Active => pal.accent,
        EvolutionPhase::AwaitingDecision => pal.warn,
        EvolutionPhase::Passed => pal.good,
        EvolutionPhase::Failed => pal.bad,
        EvolutionPhase::Recovered => pal.text_muted,
    }
}

/// The Mesh badge pair for a phase: translucent wash + legible status text.
fn tone_badge_colors(pal: &Palette, phase: EvolutionPhase) -> (Color, Color) {
    match phase {
        EvolutionPhase::Active => (pal.accent_soft, pal.accent_text),
        EvolutionPhase::AwaitingDecision => (pal.warn_soft, pal.warn_text),
        EvolutionPhase::Passed => (pal.ok_soft, pal.ok_text),
        EvolutionPhase::Failed => (pal.danger_soft, pal.danger_text),
        EvolutionPhase::Recovered => (pal.surface_3, pal.text_muted),
    }
}

fn stage_index(status: &str) -> usize {
    match status {
        "observed" | "created" => 0,
        "scoped" => 1,
        "proposed" | "packet-written" | "mutation-started" => 2,
        "implemented" | "mutation-finished" => 3,
        "built" | "verified" => 4,
        "evaluated" | "awaiting-approval" | "failed" | "build-failed" | "eval-failed" => 5,
        "approved" | "policy-approved" | "denied" | "rejected" => 6,
        "staged" => 7,
        "shadowed" => 8,
        "canary" => 9,
        "promoted" | "rolled-back" | "reverted" | "restored" => 10,
        _ => 2,
    }
}

fn changed_files_summary(files: &[String]) -> String {
    if files.is_empty() {
        "none recorded".to_string()
    } else if files.len() <= 3 {
        files.join(", ")
    } else {
        format!("{}, and {} more", files[..3].join(", "), files.len() - 3)
    }
}

fn evolution_header_subtitle(dashboard: &EvolutionDashboard) -> String {
    match &dashboard.error {
        Some(error) => format!("local evidence unavailable - {}", compact(error, 110)),
        None => format!(
            "{} jobs - {} active - {} awaiting decision - {}",
            dashboard.jobs.len(),
            dashboard.active_count(),
            dashboard.awaiting_decision_count(),
            if dashboard.source.trim().is_empty() {
                "source unavailable"
            } else {
                "source ready"
            }
        ),
    }
}

fn evolution_header_subtitle_with_refresh(
    dashboard: &EvolutionDashboard,
    refresh_pending: bool,
    refresh_error: Option<&str>,
) -> String {
    if let Some(error) = refresh_error {
        return format!(
            "Refresh failed - showing saved candidates - {} - press R to retry",
            compact(error, 66)
        );
    }
    let subtitle = evolution_header_subtitle(dashboard);
    if refresh_pending {
        format!("{} - updating in background", compact(&subtitle, 92))
    } else {
        subtitle
    }
}

fn tail_path(path: &str, max: usize) -> String {
    if path.chars().count() <= max {
        path.to_string()
    } else if max <= TRUNCATION_MARKER.len() {
        TRUNCATION_MARKER.chars().take(max).collect()
    } else {
        let tail: String = path
            .chars()
            .rev()
            .take(max - TRUNCATION_MARKER.len())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{TRUNCATION_MARKER}{tail}")
    }
}

fn compact(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else if max <= TRUNCATION_MARKER.len() {
        TRUNCATION_MARKER.chars().take(max).collect()
    } else {
        let mut out: String = text.chars().take(max - TRUNCATION_MARKER.len()).collect();
        out.push_str(TRUNCATION_MARKER);
        out
    }
}

fn short(text: &str, max: usize) -> String {
    compact(text, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::{AppState, Shared};
    use crate::theme::{AppTextScale, AppThemeMode, resolve_theme_with_preferences};

    fn candidate(index: usize) -> EvolutionJobSummary {
        EvolutionJobSummary {
            id: format!("candidate-{index:02}"),
            short_id: format!("candidate-{index:02}"),
            objective: format!("Candidate objective {index:02}"),
            status_key: "awaiting-approval".to_string(),
            status_label: "awaiting approval".to_string(),
            phase: EvolutionPhase::AwaitingDecision,
            target_ring: "R1".to_string(),
            required_gate_count: 3,
            ..EvolutionJobSummary::default()
        }
    }

    fn test_window<W: Widget + 'static>(
        title: &str,
        root: W,
        size: Size,
    ) -> sui::Result<sui_testing::TestWindow> {
        let runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().title(title).root(root))
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(size)))?;
        window.run_until_idle()?;
        Ok(window)
    }

    #[test]
    fn dashboard_refresh_rejects_stale_results_and_keeps_last_good_data_on_error() {
        let mut state = EvolutionState::from_dashboard(EvolutionDashboard {
            source: "old".to_string(),
            jobs: vec![candidate(1)],
            error: None,
        });
        state.requested_dashboard_id = Some(EvolutionDashboardRequestId(2));

        assert!(
            !state.apply_dashboard_refresh_result(EvolutionDashboardRefreshResult {
                id: EvolutionDashboardRequestId(1),
                reason: EvolutionDashboardRefreshReason::JobStarted,
                prefer_newest: true,
                dashboard: EvolutionDashboard {
                    source: "stale".to_string(),
                    jobs: vec![candidate(9)],
                    error: None,
                },
            })
        );
        assert_eq!(state.dashboard.jobs[0].id, "candidate-01");
        assert_eq!(state.completed_dashboard_id, None);

        assert!(
            state.apply_dashboard_refresh_result(EvolutionDashboardRefreshResult {
                id: EvolutionDashboardRequestId(2),
                reason: EvolutionDashboardRefreshReason::JobFinished,
                prefer_newest: false,
                dashboard: EvolutionDashboard {
                    source: "fresh".to_string(),
                    jobs: vec![candidate(2), candidate(1)],
                    error: None,
                },
            })
        );
        assert_eq!(
            state.selected,
            Some(1),
            "selection follows the stable job ID"
        );
        assert_eq!(
            state.completed_dashboard_id,
            Some(EvolutionDashboardRequestId(2))
        );

        state.requested_dashboard_id = Some(EvolutionDashboardRequestId(3));
        assert!(
            state.apply_dashboard_refresh_result(EvolutionDashboardRefreshResult {
                id: EvolutionDashboardRequestId(3),
                reason: EvolutionDashboardRefreshReason::Retry,
                prefer_newest: false,
                dashboard: EvolutionDashboard {
                    source: "fresh".to_string(),
                    jobs: Vec::new(),
                    error: Some("temporary read failure".to_string()),
                },
            })
        );
        assert_eq!(state.dashboard.jobs.len(), 2);
        assert!(
            state
                .dashboard_refresh_error()
                .is_some_and(|error| error.contains("temporary read failure"))
        );
    }

    #[test]
    fn start_events_schedule_dashboard_refresh_without_sync_reload() {
        let backend = Backend::local_fallback();
        let mut state = EvolutionState::from_dashboard(EvolutionDashboard {
            source: "existing".to_string(),
            jobs: vec![candidate(4)],
            error: None,
        });
        let (tx, rx) = mpsc::channel();
        state.start_rx = Some(rx);
        tx.send(EvolutionStartEvent::Started).unwrap();

        assert!(state.pump_start_events(&backend, None));
        assert_eq!(state.dashboard.jobs[0].id, "candidate-04");
        assert!(state.dashboard_refresh_pending());
        assert_eq!(
            state.dashboard_refresh_reason,
            Some(EvolutionDashboardRefreshReason::JobStarted)
        );
        assert_eq!(
            state.requested_dashboard_id,
            Some(EvolutionDashboardRequestId(1))
        );
    }

    #[test]
    fn extra_large_queue_rows_expand_from_token_line_heights_without_overlap() {
        let standard = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::Standard);
        let extra_large =
            resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::ExtraLarge);
        let standard_metrics = QueueRowMetrics::for_theme(&standard);
        let metrics = QueueRowMetrics::for_theme(&extra_large);

        assert_eq!(standard_metrics.height, QUEUE_ROW_MIN_H);
        assert!(metrics.height > standard_metrics.height);
        assert!(
            metrics.meta_top >= metrics.title_top + metrics.title_line_height + QUEUE_ROW_LINE_GAP
        );
        assert!(
            metrics.target_top >= metrics.meta_top + metrics.meta_line_height + QUEUE_ROW_LINE_GAP
        );
        assert!(
            metrics.height
                >= metrics.target_top + metrics.target_line_height + QUEUE_ROW_BOTTOM_PAD
        );
    }

    #[test]
    fn extra_large_lifecycle_labels_clear_the_track_and_each_other() {
        let theme = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::ExtraLarge);
        let metrics = LifecycleMetrics::for_theme(&theme);
        assert!(
            metrics.label_top
                >= metrics.track_center_y + LIFECYCLE_DOT_RADIUS + LIFECYCLE_LABEL_GAP
        );
        assert!(
            metrics.height >= metrics.label_top + metrics.label_line_height + LIFECYCLE_BOTTOM_PAD
        );

        let labels = [
            "Observed",
            "Scoped",
            "Proposed",
            "Implemented",
            "Built",
            "Evaluated",
            "Approved",
            "Staged",
            "Shadowed",
            "Canary",
            "Promoted",
        ];
        let widths = labels
            .iter()
            .map(|label| label.chars().count() as f32 * theme.text.xs.size * 0.62 + 2.0)
            .collect::<Vec<_>>();
        let rect = Rect::new(0.0, 0.0, 480.0, metrics.height);
        let step = (rect.width() - 24.0) / (labels.len() as f32 - 1.0);
        let placements = lifecycle_label_placements(rect, step, &widths, 5);

        assert!(placements.iter().any(|placement| placement.index == 5));
        for (index, placement) in placements.iter().enumerate() {
            for other in placements.iter().skip(index + 1) {
                assert!(
                    placement.right + LIFECYCLE_LABEL_HORIZONTAL_GAP <= other.left
                        || other.right + LIFECYCLE_LABEL_HORIZONTAL_GAP <= placement.left,
                    "measured lifecycle captions must not overlap"
                );
            }
        }
    }

    #[test]
    fn candidate_queue_reorder_preserves_virtual_row_ids() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            evolution.dashboard.jobs = vec![candidate(0), candidate(1), candidate(2)];
            evolution.selected = Some(0);
        }
        let window = test_window(
            "Evolution retained candidate queue",
            view(shared.clone()),
            Size::new(760.0, 760.0),
        )?;
        let row_ids = |snapshot: &sui_testing::WindowSnapshot| {
            snapshot
                .accessibility
                .nodes
                .iter()
                .filter(|node| node.role == SemanticsRole::ListItem)
                .filter_map(|node| node.name.clone().map(|name| (name, node.id)))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = row_ids(&window.snapshot()?);
        assert_eq!(before.len(), 3);

        {
            let mut evolution = shared.evolution_mut();
            evolution.dashboard.jobs = vec![candidate(2), candidate(0), candidate(1)];
            evolution.selected = Some(1);
        }
        window.run_until_idle()?;
        let after = row_ids(&window.snapshot()?);
        assert_eq!(
            after, before,
            "same-key candidates should move without replacing virtual row identities"
        );
        Ok(())
    }

    #[test]
    fn narrow_layout_keeps_decision_actions_sticky() {
        let bounds = Rect::new(0.0, 0.0, 760.0, 760.0);
        let layout = layout_for(bounds, false);
        let bar = layout.action_bar.expect("narrow action bar");
        let (approve, reject) = layout_decision_buttons(layout).expect("decision buttons");

        assert!(layout.rail.is_none());
        assert_eq!(bar.max_y(), bounds.max_y());
        assert!(layout.detail.max_y() <= bar.y());
        assert!(bar.contains(sui::Point::new(
            approve.x() + approve.width() / 2.0,
            approve.y() + approve.height() / 2.0,
        )));
        assert!(bar.contains(sui::Point::new(
            reject.x() + reject.width() / 2.0,
            reject.y() + reject.height() / 2.0,
        )));
        assert!(approve.width() > 0.0 && reject.width() > 0.0);
    }

    #[test]
    fn candidate_rows_support_keyboard_navigation_and_live_progress_semantics() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            let mut jobs = (0..12).map(candidate).collect::<Vec<_>>();
            jobs[0].phase = EvolutionPhase::Active;
            jobs[0].status_key = "mutation-started".to_string();
            jobs[0].status_label = "mutation started".to_string();
            evolution.dashboard = EvolutionDashboard {
                source: "test".to_string(),
                jobs,
                error: None,
            };
            evolution.selected = Some(0);
        }
        let window = test_window(
            "Evolution keyboard queue",
            view(shared.clone()),
            Size::new(760.0, 760.0),
        )?;

        let snapshot = window.snapshot()?;
        let visible_rows = snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| node.role == SemanticsRole::ListItem)
            .count();
        assert!(visible_rows > 0 && visible_rows < 12);
        let progress = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::BusyIndicator
                    && node.name.as_deref() == Some("Evolution progress")
            })
            .expect("live evolution progress node");
        assert!(progress.state.busy);
        assert_eq!(
            progress.value,
            Some(SemanticsValue::Text(
                "1 evolution job in progress".to_string()
            ))
        );

        let root = window
            .get_by_role(SemanticsRole::GenericContainer)
            .with_name("Evolution dashboard");
        let queue = window
            .get_by_role(SemanticsRole::List)
            .with_name("Evolution candidate queue");
        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Candidate objective 00")
            .click()?;
        queue.press("ArrowDown")?;
        assert_eq!(shared.evolution().selected, Some(1));
        root.expect()
            .to_have_value("Candidate objective 01; awaiting approval")?;
        queue.press("End")?;
        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Candidate objective 11")
            .expect()
            .to_be_visible()?;
        assert_eq!(shared.evolution().selected, Some(11));

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Approve")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Reject")
            .expect()
            .to_be_visible()?;
        Ok(())
    }

    #[test]
    fn composed_evolution_detail_uses_semantic_sections_and_status_widgets() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            let mut job = candidate(3);
            job.verification_outcome = Some("passed".to_string());
            job.changed_files = vec!["crates/sinomo-app/src/ui/evolution.rs".to_string()];
            evolution.dashboard = EvolutionDashboard {
                source: "test".to_string(),
                jobs: vec![job],
                error: None,
            };
            evolution.selected = Some(0);
        }
        let window = test_window(
            "Evolution composed detail",
            view(shared),
            Size::new(1200.0, 820.0),
        )?;

        window
            .get_by_role(SemanticsRole::Text)
            .with_name("awaiting approval")
            .expect()
            .to_be_visible()?;
        for section in ["Lifecycle", "Evidence", "Scope", "Operator decision"] {
            window
                .get_by_role(SemanticsRole::GenericContainer)
                .with_name(section)
                .expect()
                .to_be_visible()?;
        }
        window
            .get_by_role(SemanticsRole::ProgressBar)
            .with_name("Evolution lifecycle")
            .expect()
            .to_have_value("awaiting approval; stage 6 of 11")?;
        window
            .get_by_role(SemanticsRole::GenericContainer)
            .with_name("Outcome")
            .expect()
            .to_have_value("passed")?;
        Ok(())
    }

    #[test]
    fn evolution_header_and_start_status_receive_usable_text_width() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            evolution.dashboard = EvolutionDashboard {
                source: "test".to_string(),
                jobs: vec![candidate(0), candidate(1), candidate(2)],
                error: None,
            };
            evolution.selected = Some(0);
            evolution.objective_draft = "Improve DFS performance".to_string();
        }
        let window = test_window(
            "Evolution header layout",
            view(shared),
            Size::new(1280.0, 720.0),
        )?;
        let snapshot = window.snapshot()?;

        let title = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text && node.name.as_deref() == Some("Evolution")
            })
            .expect("Evolution page title");
        assert!(
            title.bounds.width() > 300.0 && title.bounds.height() < 40.0,
            "page title should be laid out as a wide single line, got {:?}",
            title.bounds
        );

        let status = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text
                    && node.name.as_deref()
                        == Some("Ready to start - use Options for scope and target ring")
            })
            .expect("Evolution start readiness status");
        assert!(
            status.bounds.width() > 300.0 && status.bounds.height() < 40.0,
            "readiness should be laid out as a wide single line, got {:?}",
            status.bounds
        );
        Ok(())
    }

    #[test]
    fn wide_start_controls_collapse_advanced_fields_by_default() {
        let layout = start_controls_layout(
            Rect::new(0.0, 0.0, 900.0, start_controls_height_for(900.0, false)),
            false,
        );

        assert!(layout.objective_label.max_y() <= layout.objective.y());
        assert!(layout.options_label.max_y() <= layout.options.y());
        assert_eq!(layout.scope, Rect::ZERO);
        assert_eq!(layout.target, Rect::ZERO);
        assert_eq!(layout.objective.y(), layout.options.y());
        assert_eq!(layout.options.y(), layout.start.y());
        assert!(layout.objective.max_x() <= layout.options.x());
        assert!(layout.options.max_x() <= layout.start.x());
    }

    #[test]
    fn wide_start_controls_reserve_label_space_when_expanded() {
        let layout = start_controls_layout(
            Rect::new(0.0, 0.0, 900.0, start_controls_height_for(900.0, true)),
            true,
        );

        assert!(layout.objective_label.max_y() <= layout.objective.y());
        assert!(layout.scope_label.max_y() <= layout.scope.y());
        assert!(layout.target_label.max_y() <= layout.target.y());
        assert!(layout.options_label.max_y() <= layout.options.y());
        assert_eq!(layout.objective.x(), 0.0);
        assert_eq!(layout.objective.width(), 900.0);
        assert!(layout.objective.max_y() + CONTROL_GAP <= layout.scope_label.y());
        assert_eq!(layout.scope.y(), layout.target.y());
        assert_eq!(layout.target.y(), layout.options.y());
        assert_eq!(layout.options.y(), layout.start.y());
        assert!(layout.scope.max_x() <= layout.target.x());
        assert!(layout.target.max_x() <= layout.options.x());
        assert!(layout.options.max_x() <= layout.start.x());
    }

    #[test]
    fn narrow_start_controls_stack_labeled_rows_when_expanded() {
        let layout = start_controls_layout(
            Rect::new(0.0, 0.0, 520.0, start_controls_height_for(520.0, true)),
            true,
        );

        assert!(layout.objective_label.max_y() <= layout.objective.y());
        assert!(layout.objective.max_y() + CONTROL_GAP <= layout.scope_label.y());
        assert!(layout.scope_label.max_y() <= layout.scope.y());
        assert!(layout.scope.max_y() + CONTROL_GAP <= layout.target_label.y());
        assert!(layout.target_label.max_y() <= layout.target.y());
        assert!(layout.options_label.max_y() <= layout.options.y());
        assert_eq!(layout.target.y(), layout.options.y());
        assert_eq!(layout.options.y(), layout.start.y());
    }

    #[test]
    fn truncation_helpers_use_standard_ellipsis_marker() {
        assert_eq!(compact("abcdef", 6), "abcdef");
        assert_eq!(compact("abcdef", 5), "ab...");
        assert_eq!(compact("abcdef", 3), "...");
        assert_eq!(
            tail_path("/home/ix/.sinomo/evolution/candidates/01j9candidate", 18),
            "...s/01j9candidate"
        );
        assert_eq!(tail_path("/short/path", 18), "/short/path");
    }

    #[test]
    fn evolution_header_subtitle_hides_source_path() {
        let dashboard = EvolutionDashboard {
            source: r"C:\Users\IX\.sinomo\evolution".to_string(),
            jobs: vec![EvolutionJobSummary {
                phase: EvolutionPhase::AwaitingDecision,
                status_key: "awaiting-approval".to_string(),
                ..EvolutionJobSummary::default()
            }],
            error: None,
        };

        let subtitle = evolution_header_subtitle(&dashboard);
        assert!(subtitle.contains("1 jobs"));
        assert!(subtitle.contains("source ready"));
        assert!(
            !subtitle.contains(r"C:\Users"),
            "raw source paths belong in technical details, not the primary header"
        );
    }

    #[test]
    fn empty_evolution_state_points_to_start_workflow() {
        assert_eq!(
            EVOLUTION_EMPTY_SUBTITLE,
            "Describe an objective above, then Start."
        );
        assert_eq!(
            empty_detail_subtitle(false, false),
            "Describe an objective above, then Start."
        );
        let rows = empty_detail_rows(false, false);
        assert!(
            rows.iter()
                .any(|(label, value)| label == "Next" && value == "describe the improvement above")
        );
        assert!(
            rows.iter()
                .any(|(label, value)| label == "Options" && value.contains("optional"))
        );
        assert!(
            rows.iter()
                .all(|(_, value)| !value.contains("not available"))
        );
        assert!(
            rows.iter()
                .all(|(_, value)| !value.contains("no candidate selected"))
        );
    }

    #[test]
    fn lightweight_evolution_state_loads_dashboard_asynchronously() {
        let backend = Backend::local_fallback();
        let mut state = EvolutionState::new_lightweight(&backend);

        assert_eq!(
            state.dashboard.error.as_deref(),
            Some("Evolution data is loading.")
        );
        assert!(state.dashboard.jobs.is_empty());
        assert!(state.begin_dashboard_load(backend.data_dir.clone(), None));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state.pump_dashboard_load() {
                assert_ne!(
                    state.dashboard.error.as_deref(),
                    Some("Evolution data is loading.")
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        panic!("dashboard load worker did not finish");
    }

    #[test]
    fn ready_evolution_empty_state_points_to_start_action() {
        assert_eq!(
            empty_detail_subtitle(true, false),
            "Objective ready. Press Start to create a candidate."
        );
        let rows = empty_detail_rows(true, false);
        assert!(
            rows.iter()
                .any(|(label, value)| label == "Next" && value == "press Start above")
        );
        assert!(rows
            .iter()
            .any(|(label, value)| label == "Options" && value == "review scope and target ring"));
    }

    #[test]
    fn starting_evolution_empty_state_points_to_candidate_queue() {
        assert_eq!(
            empty_detail_subtitle(false, true),
            "Starting the evolution agent."
        );
        let rows = empty_detail_rows(false, true);
        assert!(
            rows.iter()
                .any(|(label, value)| label == "Next" && value == "wait for the candidate queue")
        );
        assert!(rows.iter().any(
            |(label, value)| label == "Options" && value == "scope and target ring are locked"
        ));
    }

    #[test]
    fn readiness_rows_hide_source_path_until_details_open() {
        let dashboard = EvolutionDashboard {
            source: r"C:\Users\IX\.sinomo\evolution".to_string(),
            error: None,
            ..EvolutionDashboard::default()
        };

        let collapsed = readiness_rows(&dashboard, false);
        let collapsed_text = collapsed
            .iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(collapsed_text.contains("Source: ready"));
        assert!(
            !collapsed_text.contains(r"C:\Users"),
            "collapsed readiness should not expose the local source path"
        );

        let expanded = readiness_rows(&dashboard, true);
        let expanded_text = expanded
            .iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(expanded_text.contains("Source path:"));
        assert!(expanded_text.contains(r"C:\Users\IX\.sinomo\evolution"));
    }

    #[test]
    fn selected_review_rows_keep_raw_paths_out_of_rail() {
        let job = EvolutionJobSummary {
            status_key: "awaiting-approval".to_string(),
            recovery_strategy: "git branch reset".to_string(),
            rollback_policy: "Manual".to_string(),
            verification_outcome: Some("passed".to_string()),
            required_gate_count: 4,
            verification_command_count: 3,
            artifact_ref_count: 7,
            scorecard_path: Some(
                r"C:\Users\IX\.sinomo\evolution\candidates\candidate\scorecard.json".to_string(),
            ),
            bundle_path: r"C:\Users\IX\.sinomo\evolution\candidates\candidate".to_string(),
            mutation_packet_path:
                r"C:\Users\IX\.sinomo\evolution\candidates\candidate\mutation-packet.json"
                    .to_string(),
            ..EvolutionJobSummary::default()
        };

        let rows = selected_review_rows(&job);
        let text = rows
            .iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("Verification: passed"));
        assert!(text.contains("Gates: 4 required, 3 commands"));
        assert!(text.contains("Rollback: git branch reset / Manual"));
        assert!(text.contains("Artifacts: 7 refs, scorecard"));
        assert!(text.contains("Next: approve or reject"));
        assert!(
            !text.contains(r"C:\Users"),
            "compact rail summary should not expose raw local artifact paths"
        );
    }

    #[test]
    fn readiness_details_button_toggles_source_details() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            evolution.dashboard = EvolutionDashboard {
                source: r"C:\Users\IX\.sinomo\evolution".to_string(),
                jobs: Vec::new(),
                error: None,
            };
            evolution.selected = None;
            evolution.readiness_details_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Evolution readiness details")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show details")
            .click()?;
        assert!(shared.evolution().readiness_details_open);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide details")
            .click()?;
        assert!(!shared.evolution().readiness_details_open);

        Ok(())
    }

    #[test]
    fn start_form_opens_blank_with_prompt_placeholders() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        assert!(shared.evolution().objective_draft.is_empty());
        assert!(shared.evolution().scope_draft.is_empty());
        assert!(!shared.evolution().start_advanced_open);
        assert!(!shared.evolution().can_start());

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Evolution form")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-objective")
            .expect()
            .to_have_value("")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-scope")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Options")
            .click()?;
        assert!(shared.evolution().start_advanced_open);
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-scope")
            .expect()
            .to_have_value("")?;

        Ok(())
    }

    #[test]
    fn starter_objective_button_fills_objective_without_starting_job() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut evolution = shared.evolution_mut();
            evolution.start_error = Some("old validation error".to_string());
            evolution.start_status = Some("old worker status".to_string());
            evolution.dashboard = EvolutionDashboard::default();
            evolution.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Evolution starter objective")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Check cluster")
            .click()?;

        let expected = EvolutionStarterObjective::CheckCluster.prompt();
        {
            let evolution = shared.evolution();
            assert_eq!(evolution.objective_draft, expected);
            assert_eq!(
                evolution.scope_draft,
                EvolutionStarterObjective::CheckCluster.scope()
            );
            assert_eq!(
                evolution.target_ring_index,
                target_ring_index_for_value(EvolutionStarterObjective::CheckCluster.target_ring())
            );
            assert!(evolution.start_advanced_open);
            assert!(evolution.can_start());
            assert!(evolution.start_error.is_none());
            assert!(evolution.start_status.is_none());
            assert!(evolution.start_rx.is_none());
            assert_eq!(evolution.objective_revision, 1);
            assert_eq!(evolution.scope_revision, 1);
        }
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-objective")
            .expect()
            .to_have_value(expected)?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-scope")
            .expect()
            .to_have_value(EvolutionStarterObjective::CheckCluster.scope())?;

        Ok(())
    }

    #[test]
    fn polish_app_starter_uses_app_scope() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Evolution app starter scope")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Polish app")
            .click()?;

        {
            let evolution = shared.evolution();
            assert_eq!(
                evolution.objective_draft,
                EvolutionStarterObjective::PolishApp.prompt()
            );
            assert_eq!(evolution.scope_draft, "crates/sinomo-app/**");
            assert_eq!(
                evolution.target_ring_index,
                target_ring_index_for_value("r2")
            );
            assert!(evolution.start_advanced_open);
            assert!(evolution.start_rx.is_none());
        }
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("evolution-scope")
            .expect()
            .to_have_value("crates/sinomo-app/**")?;

        Ok(())
    }
}
