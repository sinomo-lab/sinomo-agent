//! "Evolution" tab - self-evolution launch, candidate evidence, and rollout readiness.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc::Receiver;

use sui::{
    ArrangeCtx, Button, Color, Constraints, DefaultTheme, Event, EventCtx, FontWeight, IconGlyph,
    KeyState, MeasureCtx, PaintCtx, Path, PointerButton, PointerEventKind, Rect, Select,
    SemanticsCtx, SemanticsNode, SemanticsRole, Size, StrokeStyle, TextInput, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
};

use crate::backend::Backend;
use crate::backend::evolution::{
    EvolutionDashboard, EvolutionDecisionAction, EvolutionDecisionEvent, EvolutionDecisionRequest,
    EvolutionJobSummary, EvolutionPhase, EvolutionStartEvent, EvolutionStartRequest,
    load_evolution_dashboard, spawn_evolution_decision, spawn_evolution_job,
};
use crate::state::Shared;
use crate::theme::Palette;
use crate::ui::widgets;

const HEADER_H: f32 = 78.0;
const PAD: f32 = 20.0;
const GAP: f32 = 14.0;
const START_WIDE_H: f32 = 124.0;
const START_NARROW_COLLAPSED_H: f32 = 206.0;
const START_NARROW_EXPANDED_H: f32 = 270.0;
const START_BREAKPOINT: f32 = 760.0;
const CONTROL_LABEL_H: f32 = 14.0;
const CONTROL_LABEL_GAP: f32 = 5.0;
const CONTROL_H: f32 = 38.0;
const CONTROL_GAP: f32 = 8.0;
const STARTER_ACTION_COUNT: usize = 3;
const STARTER_ACTION_H: f32 = 30.0;
const STARTER_ACTION_GAP: f32 = 8.0;
const START_OPTIONS_W: f32 = 112.0;
const START_BUTTON_W: f32 = 112.0;
const START_TARGET_W: f32 = 138.0;
const TRUNCATION_MARKER: &str = "...";
const QUEUE_W: f32 = 306.0;
const RAIL_W: f32 = 318.0;
const QUEUE_HEADER_H: f32 = 48.0;
const ROW_H: f32 = 78.0;
const SECTION_GAP: f32 = 16.0;
const DECISION_SECTION_H: f32 = 146.0;
const DECISION_BUTTON_H: f32 = 32.0;
const READINESS_DETAIL_BUTTON_W: f32 = 136.0;
const OBJECTIVE_PLACEHOLDER: &str = "Describe the improvement or task";
const SCOPE_PLACEHOLDER: &str = "Optional: path globs, comma-separated";
const EVOLUTION_EMPTY_SUBTITLE: &str = "Describe an objective above, then Start.";

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
    scope_revision: u64,
}

impl EvolutionState {
    pub fn new(backend: &Backend) -> Self {
        let dashboard = EvolutionDashboard::load(backend);
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
            scope_revision: 0,
        }
    }

    pub fn reload(&mut self, backend: &Backend) {
        self.reload_with_selection(backend, false);
    }

    fn reload_with_selection(&mut self, backend: &Backend, prefer_newest: bool) {
        let selected_id = self
            .selected
            .and_then(|index| self.dashboard.jobs.get(index))
            .map(|job| job.id.clone());
        self.dashboard = load_evolution_dashboard(&backend.data_dir);
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

    pub fn pump_start_events(&mut self, backend: &Backend) -> bool {
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
        if changed || self.is_starting() {
            self.reload_with_selection(backend, true);
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

    pub fn pump_decision_events(&mut self, backend: &Backend) -> bool {
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
            self.reload_with_selection(backend, false);
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

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

struct RebuildOnChange<K: PartialEq + Clone> {
    key_fn: Box<dyn Fn() -> K>,
    build: Box<dyn Fn(&K) -> WidgetPod>,
    last_key: K,
    child: WidgetPod,
}

impl<K: PartialEq + Clone> RebuildOnChange<K> {
    fn new<KF, BF>(key_fn: KF, build: BF) -> Self
    where
        KF: Fn() -> K + 'static,
        BF: Fn(&K) -> WidgetPod + 'static,
    {
        let last_key = key_fn();
        let child = build(&last_key);
        Self {
            key_fn: Box::new(key_fn),
            build: Box::new(build),
            last_key,
            child,
        }
    }

    fn refresh(&mut self) -> bool {
        let key = (self.key_fn)();
        if key != self.last_key {
            self.child = (self.build)(&key);
            self.last_key = key;
            true
        } else {
            false
        }
    }
}

impl<K: PartialEq + Clone + 'static> Widget for RebuildOnChange<K> {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        let pointer_up = matches!(
            event,
            Event::Pointer(pointer) if matches!(pointer.kind, PointerEventKind::Up)
        );
        if pointer_up && self.refresh() {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.refresh();
        self.child.measure(ctx, constraints)
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

fn request_window_relayout(ctx: &mut EventCtx) {
    use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

    let window = InvalidationTarget::Window(ctx.window_id());
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
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

pub fn view(shared: Shared) -> impl Widget {
    EvolutionView::new(shared)
}

struct EvolutionView {
    shared: Shared,
    start: WidgetPod,
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
        let start = WidgetPod::new(StartControls::new(shared.clone()));
        let readiness_show = {
            let s = shared.clone();
            Button::new("Show details")
                .icon(IconGlyph::ChevronDown)
                .min_height(DECISION_BUTTON_H)
                .theme_when(clone_reader(&reader))
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().set_readiness_details_open(true);
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
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().set_readiness_details_open(false);
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
                .enabled_when({
                    let s = s.clone();
                    move || s.evolution().can_decide_selected()
                })
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut()
                        .decide_selected(EvolutionDecisionAction::Approve);
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
                .enabled_when({
                    let s = s.clone();
                    move || s.evolution().can_decide_selected()
                })
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut()
                        .decide_selected(EvolutionDecisionAction::Reject);
                    ctx.request_animation_frame();
                    ctx.request_paint();
                })
        };
        Self {
            shared,
            start,
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
        let changed = {
            let mut evolution = self.shared.evolution_mut();
            let start_changed = evolution.pump_start_events(&backend);
            let decision_changed = evolution.pump_decision_events(&backend);
            start_changed || decision_changed
        };
        if changed {
            ctx.request_paint();
        }
        if self.shared.evolution().has_pending_worker() {
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
    objective: WidgetPod,
    options: WidgetPod,
    scope: WidgetPod,
    target: WidgetPod,
    start: WidgetPod,
    starter_actions: [WidgetPod; STARTER_ACTION_COUNT],
}

impl StartControls {
    fn new(shared: Shared) -> Self {
        let reader = theme_reader(&shared);

        let objective = {
            let s = shared.clone();
            let r = Rc::clone(&reader);
            RebuildOnChange::new(
                {
                    let s = s.clone();
                    move || s.evolution().objective_revision
                },
                move |_revision| WidgetPod::new(objective_input(&s, &r)),
            )
        };

        let options = {
            let s = shared.clone();
            Button::new("Options")
                .icon(IconGlyph::ChevronDown)
                .theme_when(clone_reader(&reader))
                .on_press_with_ctx(move |ctx| {
                    s.evolution_mut().toggle_start_advanced();
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                })
        };

        let scope = {
            let s = shared.clone();
            let r = Rc::clone(&reader);
            RebuildOnChange::new(
                {
                    let s = s.clone();
                    move || s.evolution().scope_revision
                },
                move |_revision| {
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
                },
            )
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
                .on_press(move || {
                    s.evolution_mut().start();
                })
        };
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
                    .on_press_with_ctx(move |ctx| {
                        s.evolution_mut().apply_starter_objective(action);
                        request_window_relayout(ctx);
                    }),
            )
        });

        Self {
            shared,
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
        let fixed_w = if advanced_open {
            START_OPTIONS_W + START_BUTTON_W + START_TARGET_W + CONTROL_GAP * 4.0
        } else {
            START_OPTIONS_W + START_BUTTON_W + CONTROL_GAP * 2.0
        };
        let flexible_w = (bounds.width() - fixed_w).max(0.0);
        let controls_h = (bounds.max_y() - controls_y).max(0.0);
        let label_y = controls_y + (controls_h - block_h).max(0.0) / 2.0;
        let y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let mut x = bounds.x();
        let (objective_w, scope_w) = if advanced_open {
            let objective_w = flexible_w * 0.56;
            (objective_w, flexible_w - objective_w)
        } else {
            (flexible_w, 0.0)
        };
        let objective = Rect::new(x, y, objective_w, CONTROL_H);
        x = objective.max_x() + CONTROL_GAP;
        let scope = if advanced_open {
            let rect = Rect::new(x, y, scope_w, CONTROL_H);
            x = rect.max_x() + CONTROL_GAP;
            rect
        } else {
            Rect::ZERO
        };
        let target = if advanced_open {
            let rect = Rect::new(x, y, START_TARGET_W, CONTROL_H);
            x = rect.max_x() + CONTROL_GAP;
            rect
        } else {
            Rect::ZERO
        };
        let options = Rect::new(x, y, START_OPTIONS_W, CONTROL_H);
        x = options.max_x() + CONTROL_GAP;
        let start = Rect::new(x, y, START_BUTTON_W, CONTROL_H);
        StartControlsLayout {
            objective_label: label_rect(objective, label_y),
            options_label: label_rect(options, label_y),
            scope_label: if advanced_open {
                label_rect(scope, label_y)
            } else {
                Rect::ZERO
            },
            target_label: if advanced_open {
                label_rect(target, label_y)
            } else {
                Rect::ZERO
            },
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

        match event {
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && ctx.bounds().contains(pointer.position) =>
            {
                let advanced_open = self.shared.evolution().start_advanced_open;
                let layout = layout_for(ctx.bounds(), advanced_open);
                if layout.queue.contains(pointer.position) {
                    let row_start = layout.queue.y() + QUEUE_HEADER_H;
                    let index = ((pointer.position.y - row_start) / ROW_H).floor() as isize;
                    let job_count = self.shared.evolution().dashboard.jobs.len();
                    if index >= 0 && (index as usize) < job_count {
                        self.shared.evolution_mut().select(index as usize);
                        ctx.request_paint();
                        ctx.set_handled();
                    }
                }
                if self.shared.evolution().has_pending_worker() {
                    self.ensure_pump(ctx);
                }
            }
            Event::Window(WindowEvent::RedrawRequested) => {
                ctx.request_paint();
            }
            _ => {}
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let advanced_open = self.shared.evolution().start_advanced_open;
        let layout = layout_for(
            Rect::new(0.0, 0.0, constraints.max.width, constraints.max.height),
            advanced_open,
        );
        self.start
            .measure(ctx, Constraints::tight(rect_size(layout.start_controls)));
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
        if let Some((approve, reject)) = layout.rail.and_then(decision_button_layout) {
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
        self.start.arrange(ctx, layout.start_controls);
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
        if let Some((approve, reject)) = layout.rail.and_then(decision_button_layout) {
            self.approve.arrange(ctx, approve);
            self.reject.arrange(ctx, reject);
        } else {
            self.approve.arrange(ctx, Rect::ZERO);
            self.reject.arrange(ctx, Rect::ZERO);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
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
        paint_header(ctx, &theme, &pal, layout.header, &dashboard);
        paint_start_panel(
            ctx,
            &theme,
            &pal,
            layout.start,
            layout.start_controls,
            &dashboard,
            &self.shared,
            advanced_open,
        );
        self.start.paint(ctx);
        paint_queue(ctx, &theme, &pal, layout.queue, &dashboard, selected);
        paint_detail(ctx, &theme, &pal, layout.detail, selected_job);
        if let Some(rail) = layout.rail {
            let action = {
                let state = self.shared.evolution();
                DecisionActionStatus {
                    status: state.decision_status.clone(),
                    error: state.decision_error.clone(),
                    deciding: state.is_deciding(),
                }
            };
            paint_rail(
                ctx,
                &theme,
                &pal,
                rail,
                &dashboard,
                selected_job,
                &action,
                readiness_details_open,
            );
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
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(
            ctx.widget_id(),
            SemanticsRole::GenericContainer,
            ctx.bounds(),
        );
        node.name = Some("Evolution dashboard".to_string());
        ctx.push(node);
        let (dashboard, selected, readiness_details_open) = {
            let state = self.shared.evolution();
            (
                state.dashboard.clone(),
                state.selected,
                state.readiness_details_open,
            )
        };
        let selected_job = selected.and_then(|index| dashboard.jobs.get(index));
        if readiness_detail_toggle_visible(&dashboard, selected_job) {
            if readiness_details_open {
                self.readiness_hide.semantics(ctx);
            } else {
                self.readiness_show.semantics(ctx);
            }
        }
        self.start.semantics(ctx);
        self.approve.semantics(ctx);
        self.reject.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.start);
        visitor.visit(&self.readiness_show);
        visitor.visit(&self.readiness_hide);
        visitor.visit(&self.approve);
        visitor.visit(&self.reject);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.start);
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
        }
    } else {
        let queue_h = (body.height() * 0.36).clamp(170.0, 260.0);
        let queue = Rect::new(body.x(), body.y(), body.width(), queue_h);
        let detail = Rect::new(
            body.x(),
            queue.max_y() + GAP,
            body.width(),
            (body.max_y() - queue.max_y() - GAP).max(0.0),
        );
        EvolutionLayout {
            header,
            start,
            start_controls,
            queue,
            detail,
            rail: None,
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
    let starter_extra = starter_actions_height_for(controls_w) + CONTROL_GAP;
    if controls_w >= START_BREAKPOINT {
        START_WIDE_H + starter_extra
    } else if advanced_open {
        START_NARROW_EXPANDED_H + starter_extra
    } else {
        START_NARROW_COLLAPSED_H + starter_extra
    }
}

fn start_controls_height_for(width: f32, advanced_open: bool) -> f32 {
    let starter_extra = starter_actions_height_for(width) + CONTROL_GAP;
    if width >= START_BREAKPOINT {
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

fn paint_header(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    dashboard: &EvolutionDashboard,
) {
    ctx.fill_rect(rect, pal.window);
    let title = pal.text_style(theme, 21.0, FontWeight::SEMIBOLD, pal.text);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + PAD, rect.y() + 12.0, 260.0, 28.0),
        "Evolution",
        title,
    );

    let subtitle = evolution_header_subtitle(dashboard);
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD,
            rect.y() + 43.0,
            (rect.width() - PAD * 2.0).max(0.0),
            22.0,
        ),
        &subtitle,
        pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_muted),
    );

    if rect.width() > 740.0 {
        let metrics = [
            ("Awaiting", dashboard.awaiting_decision_count(), pal.warn),
            ("Failed", dashboard.failed_count(), pal.bad),
            ("Promoted", dashboard.promoted_count(), pal.good),
        ];
        let mut x = rect.max_x() - PAD - 100.0 * metrics.len() as f32;
        for (label, value, color) in metrics {
            paint_metric(
                ctx,
                theme,
                pal,
                Rect::new(x, rect.y() + 18.0, 88.0, 42.0),
                label,
                value,
                color,
            );
            x += 100.0;
        }
    }

    widgets::hairline_bottom(ctx, rect, pal.border);
}

fn paint_metric(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    label: &str,
    value: usize,
    color: Color,
) {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 7.0);
    ctx.fill(
        Path::circle(widgets::point(rect.x() + 14.0, rect.y() + 15.0), 4.0),
        color,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 24.0, rect.y() + 5.0, rect.width() - 30.0, 18.0),
        label,
        pal.text_style(theme, 11.0, FontWeight::MEDIUM, pal.text_muted),
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 14.0, rect.y() + 21.0, rect.width() - 20.0, 18.0),
        &value.to_string(),
        pal.text_style(theme, 16.0, FontWeight::SEMIBOLD, pal.text),
    );
}

fn paint_start_panel(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    controls_rect: Rect,
    dashboard: &EvolutionDashboard,
    shared: &Shared,
    advanced_open: bool,
) {
    ctx.fill_rect(rect, pal.window);
    widgets::hairline_bottom(ctx, rect, pal.border);

    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + PAD, rect.y() + 10.0, 150.0, 22.0),
        "Start job",
        pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text),
    );

    let (status, status_color) = {
        let evolution = shared.evolution();
        if let Some(error) = &evolution.start_error {
            (format!("failed - {}", compact(error, 108)), pal.bad)
        } else if let Some(status) = &evolution.start_status {
            (
                compact(status, 108),
                if evolution.is_starting() {
                    pal.accent
                } else {
                    pal.good
                },
            )
        } else {
            let objective = evolution.objective_draft.trim();
            if objective.is_empty() {
                let label = if dashboard.jobs.is_empty() {
                    "Enter an objective to start a supervised evolution job".to_string()
                } else {
                    format!(
                        "Enter an objective to start - {} jobs tracked",
                        dashboard.jobs.len()
                    )
                };
                (label, pal.text_muted)
            } else {
                if advanced_open {
                    let ring = EVOLUTION_TARGET_RINGS
                        .get(evolution.target_ring_index)
                        .map(|option| option.label)
                        .unwrap_or("R1 runtime");
                    let scope_count = parse_scope_paths(&evolution.scope_draft).len();
                    let scope = if scope_count == 0 {
                        "workspace policy scope".to_string()
                    } else if scope_count == 1 {
                        "1 scoped path".to_string()
                    } else {
                        format!("{scope_count} scoped paths")
                    };
                    (format!("Ready for {ring} - {scope}"), pal.accent)
                } else {
                    (
                        "Ready to start - use Options for scope and target ring".to_string(),
                        pal.accent,
                    )
                }
            }
        }
    };
    let status_rect = if rect.width() >= 640.0 {
        Rect::new(
            rect.x() + 170.0,
            rect.y() + 11.0,
            (rect.width() - 190.0).max(0.0),
            22.0,
        )
    } else {
        Rect::new(
            rect.x() + PAD,
            rect.y() + 32.0,
            (rect.width() - PAD * 2.0).max(0.0),
            20.0,
        )
    };
    widgets::draw_text_in(
        ctx,
        status_rect,
        &status,
        pal.text_style(theme, 12.0, FontWeight::MEDIUM, status_color),
    );
    paint_start_labels(ctx, theme, pal, controls_rect, advanced_open);
}

fn paint_start_labels(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    controls_rect: Rect,
    advanced_open: bool,
) {
    let layout = start_controls_layout(controls_rect, advanced_open);
    paint_start_label(ctx, theme, pal, layout.objective_label, "Objective");
    paint_start_label(ctx, theme, pal, layout.options_label, "Advanced");
    if advanced_open {
        paint_start_label(ctx, theme, pal, layout.scope_label, "Scope");
        paint_start_label(ctx, theme, pal, layout.target_label, "Target ring");
    }
}

fn paint_start_label(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    label: &str,
) {
    widgets::draw_text_in(
        ctx,
        rect,
        label,
        pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint),
    );
}

fn paint_queue(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    dashboard: &EvolutionDashboard,
    selected: Option<usize>,
) {
    ctx.fill_rect(rect, pal.sidebar);
    widgets::hairline_right(ctx, rect, pal.border);
    widgets::hairline_bottom(
        ctx,
        Rect::new(rect.x(), rect.y(), rect.width(), QUEUE_HEADER_H),
        pal.border,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD,
            rect.y() + 8.0,
            rect.width() - PAD * 2.0,
            20.0,
        ),
        "Candidate queue",
        pal.text_style(theme, 14.0, FontWeight::SEMIBOLD, pal.text),
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + PAD,
            rect.y() + 27.0,
            rect.width() - PAD * 2.0,
            18.0,
        ),
        "Observed through promoted lifecycle",
        pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text_faint),
    );

    if dashboard.jobs.is_empty() {
        let message = dashboard
            .error
            .as_deref()
            .unwrap_or("Start a job to create the first candidate");
        widgets::draw_centered(
            ctx,
            "No candidates",
            pal.text_style(theme, 16.0, FontWeight::SEMIBOLD, pal.text_muted),
            rect.x() + rect.width() / 2.0,
            rect.y() + rect.height() / 2.0 - 12.0,
        );
        widgets::draw_centered(
            ctx,
            &compact(message, 48),
            pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_faint),
            rect.x() + rect.width() / 2.0,
            rect.y() + rect.height() / 2.0 + 15.0,
        );
        return;
    }

    let mut y = rect.y() + QUEUE_HEADER_H;
    let max_rows = ((rect.max_y() - y) / ROW_H).floor().max(0.0) as usize;
    for (index, job) in dashboard.jobs.iter().take(max_rows).enumerate() {
        let row = Rect::new(rect.x(), y, rect.width(), ROW_H);
        let selected_here = selected == Some(index);
        if selected_here {
            ctx.fill_rect(row.inflate(-1.0, -1.0), pal.selected);
            widgets::hairline_left(ctx, row, pal.accent);
        } else if index % 2 == 1 {
            ctx.fill_rect(row, pal.window.with_alpha(0.18));
        }
        paint_queue_row(ctx, theme, pal, row, job, selected_here);
        widgets::hairline_bottom(ctx, row, pal.border.with_alpha(0.72));
        y += ROW_H;
    }
}

fn paint_queue_row(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
    selected: bool,
) {
    let dot = tone_color(pal, job.phase);
    ctx.fill(
        Path::circle(widgets::point(rect.x() + PAD, rect.y() + 18.0), 5.0),
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
            rect.y() + 8.0,
            rect.width() - PAD * 2.0 - 16.0,
            22.0,
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
            rect.y() + 33.0,
            rect.width() - PAD * 2.0 - 16.0,
            18.0,
        ),
        &compact(
            &meta,
            ((rect.width() - 64.0) / 8.7).clamp(18.0, 38.0) as usize,
        ),
        pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text_muted),
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
            rect.y() + 53.0,
            rect.width() - PAD * 2.0 - 16.0,
            18.0,
        ),
        &compact(
            &target,
            ((rect.width() - 64.0) / 8.7).clamp(18.0, 38.0) as usize,
        ),
        pal.text_style(theme, 11.0, FontWeight::NORMAL, pal.text_faint),
    );
}

fn paint_detail(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: Option<&EvolutionJobSummary>,
) {
    let inner = rect.inflate(-PAD, -PAD);
    let Some(job) = job else {
        paint_detail_empty(ctx, theme, pal, inner);
        return;
    };

    widgets::draw_text_in(
        ctx,
        Rect::new(inner.x(), inner.y(), inner.width() - 124.0, 30.0),
        &compact(
            &job.objective,
            ((inner.width() - 136.0) / 12.8).clamp(24.0, 52.0) as usize,
        ),
        pal.text_style(theme, 20.0, FontWeight::SEMIBOLD, pal.text),
    );
    paint_status_pill(
        ctx,
        theme,
        pal,
        Rect::new(inner.max_x() - 116.0, inner.y() + 3.0, 116.0, 24.0),
        &job.status_label,
        job.phase,
    );

    let meta = format!(
        "{} - created {} - branch {}",
        job.short_id,
        compact(&job.created_at, 24),
        compact(&job.candidate_branch, 28)
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(inner.x(), inner.y() + 34.0, inner.width(), 21.0),
        &meta,
        pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_muted),
    );

    let mut y = inner.y() + 72.0;
    paint_lifecycle(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 92.0),
        job,
    );
    y += 104.0;

    let summary_h = 86.0;
    widgets::round_panel(
        ctx,
        Rect::new(inner.x(), y, inner.width(), summary_h),
        pal.panel,
        pal.border,
        8.0,
    );
    paint_kv_grid(
        ctx,
        theme,
        pal,
        Rect::new(
            inner.x() + 14.0,
            y + 12.0,
            inner.width() - 28.0,
            summary_h - 24.0,
        ),
        job,
    );
    y += summary_h + SECTION_GAP;

    paint_section_title(ctx, theme, pal, inner.x(), y, "Evidence");
    y += 28.0;
    let evidence = evidence_rows(job);
    paint_rows(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 112.0),
        &evidence,
    );
    y += 126.0;

    paint_section_title(ctx, theme, pal, inner.x(), y, "Scope");
    y += 28.0;
    let scope = scope_rows(job);
    paint_rows(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 96.0),
        &scope,
    );
}

fn paint_detail_empty(ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette, rect: Rect) {
    let panel_w = rect.width().min(420.0);
    let panel_h = 154.0;
    let panel = Rect::new(
        rect.x() + (rect.width() - panel_w) / 2.0,
        rect.y() + (rect.height() - panel_h) / 2.0,
        panel_w,
        panel_h,
    );
    widgets::round_panel(ctx, panel, pal.panel, pal.border, 8.0);
    widgets::draw_centered(
        ctx,
        "No evolution job selected",
        pal.text_style(theme, 18.0, FontWeight::SEMIBOLD, pal.text_muted),
        panel.x() + panel.width() / 2.0,
        panel.y() + 32.0,
    );
    widgets::draw_centered(
        ctx,
        EVOLUTION_EMPTY_SUBTITLE,
        pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_faint),
        panel.x() + panel.width() / 2.0,
        panel.y() + 58.0,
    );
    paint_rows(
        ctx,
        theme,
        pal,
        Rect::new(
            panel.x() + 14.0,
            panel.y() + 80.0,
            panel.width() - 28.0,
            60.0,
        ),
        &empty_detail_rows(),
    );
}

fn empty_detail_rows() -> Vec<(String, String)> {
    vec![
        (
            "Next".to_string(),
            "describe the improvement above".to_string(),
        ),
        (
            "Options".to_string(),
            "scope and target ring are optional".to_string(),
        ),
    ]
}

fn paint_status_pill(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    label: &str,
    phase: EvolutionPhase,
) {
    let color = tone_color(pal, phase);
    widgets::round_panel(
        ctx,
        rect,
        pal.field,
        color.with_alpha(0.85),
        rect.height() / 2.0,
    );
    ctx.fill(
        Path::circle(
            widgets::point(rect.x() + 13.0, rect.y() + rect.height() / 2.0),
            4.0,
        ),
        color,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + 22.0,
            rect.y(),
            rect.width() - 26.0,
            rect.height(),
        ),
        &compact(label, 9),
        pal.text_style(theme, 11.0, FontWeight::MEDIUM, pal.text),
    );
}

fn paint_lifecycle(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
) {
    paint_section_title(ctx, theme, pal, rect.x(), rect.y(), "Lifecycle");
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
    let y = rect.y() + 52.0;
    let left = rect.x() + 12.0;
    let right = rect.max_x() - 12.0;
    let step = if stages.len() > 1 {
        (right - left) / (stages.len() as f32 - 1.0)
    } else {
        0.0
    };
    for index in 0..stages.len() - 1 {
        let x1 = left + step * index as f32;
        let x2 = left + step * (index + 1) as f32;
        let color = if index < current {
            pal.accent
        } else {
            pal.border_strong.with_alpha(0.55)
        };
        let mut builder = sui::PathBuilder::new();
        builder
            .move_to(widgets::point(x1, y))
            .line_to(widgets::point(x2, y));
        ctx.stroke(builder.build(), color, StrokeStyle::new(2.0));
    }
    for (index, (_, label)) in stages.iter().enumerate() {
        let x = left + step * index as f32;
        let passed = index <= current;
        let color = if index == current {
            tone_color(pal, job.phase)
        } else if passed {
            pal.accent
        } else {
            pal.field
        };
        ctx.fill(Path::circle(widgets::point(x, y), 7.0), color);
        ctx.stroke(
            Path::circle(widgets::point(x, y), 7.0),
            pal.border,
            StrokeStyle::new(1.0),
        );
        if index % 2 == 0 || step > 54.0 {
            widgets::draw_centered(
                ctx,
                label,
                pal.text_style(theme, 9.8, FontWeight::NORMAL, pal.text_faint),
                x,
                y + 22.0,
            );
        }
    }
}

fn paint_kv_grid(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
) {
    let cols = if rect.width() > 520.0 { 3 } else { 2 };
    let col_w = rect.width() / cols as f32;
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
    for (index, (label, value)) in entries.iter().enumerate() {
        let col = index % cols;
        let row = index / cols;
        let x = rect.x() + col as f32 * col_w;
        let y = rect.y() + row as f32 * 30.0;
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, col_w - 10.0, 14.0),
            label,
            pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint),
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y + 14.0, col_w - 10.0, 18.0),
            &compact(value, (col_w / 7.0).max(12.0) as usize),
            pal.text_style(theme, 12.0, FontWeight::SEMIBOLD, pal.text),
        );
    }
}

fn paint_rows(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    rows: &[(String, String)],
) {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 8.0);
    let mut y = rect.y() + 9.0;
    for (index, (label, value)) in rows.iter().take(4).enumerate() {
        let row = Rect::new(rect.x() + 12.0, y, rect.width() - 24.0, 22.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(row.x(), row.y(), 122.0, row.height()),
            label,
            pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.text_muted),
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(row.x() + 126.0, row.y(), row.width() - 126.0, row.height()),
            &compact(value, ((row.width() - 126.0) / 7.0).max(18.0) as usize),
            pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text),
        );
        y += 24.0;
        if index < rows.len() - 1 {
            ctx.fill_rect(
                Rect::new(rect.x() + 12.0, y - 1.0, rect.width() - 24.0, 1.0),
                pal.border.with_alpha(0.56),
            );
        }
    }
}

fn paint_section_title(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    x: f32,
    y: f32,
    label: &str,
) {
    widgets::draw_text_in(
        ctx,
        Rect::new(x, y, 220.0, 22.0),
        label,
        pal.text_style(theme, 14.0, FontWeight::SEMIBOLD, pal.text),
    );
}

fn paint_rail(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    dashboard: &EvolutionDashboard,
    job: Option<&EvolutionJobSummary>,
    action: &DecisionActionStatus,
    readiness_details_open: bool,
) {
    ctx.fill_rect(rect, pal.sidebar);
    widgets::hairline_left(ctx, rect, pal.border);
    let inner = rect.inflate(-PAD, -PAD);
    widgets::draw_text_in(
        ctx,
        Rect::new(inner.x(), inner.y(), inner.width(), 24.0),
        "Safety and evidence",
        pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text),
    );
    let Some(job) = job else {
        let rows = readiness_rows(dashboard, readiness_details_open);
        let section_h = readiness_section_height(readiness_details_open);
        paint_rail_section(
            ctx,
            theme,
            pal,
            Rect::new(inner.x(), inner.y() + 38.0, inner.width(), section_h),
            "Readiness",
            &rows,
        );
        return;
    };

    let mut y = inner.y() + 38.0;
    y = paint_decision_action_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), DECISION_SECTION_H),
        job,
        action,
    );
    y += SECTION_GAP;
    y = paint_rail_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 112.0),
        "Rollback",
        &rollback_rows(job),
    );
    y += SECTION_GAP;
    paint_rail_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 172.0),
        "Artifacts",
        &artifact_rows(job),
    );
}

fn paint_decision_action_section(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    job: &EvolutionJobSummary,
    action: &DecisionActionStatus,
) -> f32 {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 8.0);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 12.0, rect.y() + 10.0, rect.width() - 24.0, 20.0),
        "Operator decision",
        pal.text_style(theme, 12.5, FontWeight::SEMIBOLD, pal.text),
    );
    let mut y = rect.y() + 36.0;
    for (label, value) in decision_action_rows(job, action).iter().take(3) {
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 12.0, y, 78.0, 18.0),
            label,
            pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint),
        );
        let color = if label == "State" && action.error.is_some() {
            pal.bad
        } else if label == "State" && action.deciding {
            pal.accent
        } else {
            pal.text
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 90.0, y, rect.width() - 102.0, 18.0),
            &compact(value, ((rect.width() - 102.0) / 6.6).max(14.0) as usize),
            pal.text_style(theme, 10.8, FontWeight::NORMAL, color),
        );
        y += 21.0;
    }
    ctx.fill_rect(
        Rect::new(
            rect.x() + 12.0,
            rect.max_y() - DECISION_BUTTON_H - 18.0,
            rect.width() - 24.0,
            1.0,
        ),
        pal.border.with_alpha(0.56),
    );
    rect.max_y()
}

fn paint_rail_section(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    title: &str,
    rows: &[(String, String)],
) -> f32 {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 8.0);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 12.0, rect.y() + 10.0, rect.width() - 24.0, 20.0),
        title,
        pal.text_style(theme, 12.5, FontWeight::SEMIBOLD, pal.text),
    );
    let mut y = rect.y() + 36.0;
    for (label, value) in rows.iter().take(5) {
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 12.0, y, 88.0, 18.0),
            label,
            pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint),
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 100.0, y, rect.width() - 112.0, 18.0),
            &compact(value, ((rect.width() - 112.0) / 6.6).max(14.0) as usize),
            pal.text_style(theme, 10.8, FontWeight::NORMAL, pal.text),
        );
        y += 22.0;
    }
    rect.max_y()
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

fn rollback_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    vec![
        ("Strategy".to_string(), job.recovery_strategy.clone()),
        ("Policy".to_string(), job.rollback_policy.clone()),
        (
            "Revision".to_string(),
            job.source_revision
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        ("Note".to_string(), job.recovery_note.clone()),
    ]
}

fn artifact_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    vec![
        ("Bundle".to_string(), tail_path(&job.bundle_path, 54)),
        (
            "Packet".to_string(),
            tail_path(&job.mutation_packet_path, 54),
        ),
        (
            "Diff".to_string(),
            job.diff_path
                .as_deref()
                .map(|path| tail_path(path, 54))
                .unwrap_or_else(|| "missing".to_string()),
        ),
        (
            "Scorecard".to_string(),
            job.scorecard_path
                .as_deref()
                .map(|path| tail_path(path, 54))
                .unwrap_or_else(|| "missing".to_string()),
        ),
        (
            "Summary".to_string(),
            job.candidate_summary_path
                .as_deref()
                .map(|path| tail_path(path, 54))
                .unwrap_or_else(|| "missing".to_string()),
        ),
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
        assert_eq!(layout.objective.y(), layout.scope.y());
        assert_eq!(layout.scope.y(), layout.target.y());
        assert_eq!(layout.target.y(), layout.options.y());
        assert_eq!(layout.options.y(), layout.start.y());
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
        let rows = empty_detail_rows();
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
