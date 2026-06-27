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
    EvolutionDashboard, EvolutionJobSummary, EvolutionPhase, EvolutionStartEvent,
    EvolutionStartRequest, load_evolution_dashboard, spawn_evolution_job,
};
use crate::state::Shared;
use crate::theme::Palette;
use crate::ui::widgets;

const HEADER_H: f32 = 78.0;
const PAD: f32 = 20.0;
const GAP: f32 = 14.0;
const START_WIDE_H: f32 = 124.0;
const START_NARROW_H: f32 = 206.0;
const START_BREAKPOINT: f32 = 760.0;
const CONTROL_LABEL_H: f32 = 14.0;
const CONTROL_LABEL_GAP: f32 = 5.0;
const CONTROL_H: f32 = 38.0;
const CONTROL_GAP: f32 = 8.0;
const TRUNCATION_MARKER: &str = "...";
const QUEUE_W: f32 = 306.0;
const RAIL_W: f32 = 318.0;
const QUEUE_HEADER_H: f32 = 48.0;
const ROW_H: f32 = 78.0;
const SECTION_GAP: f32 = 16.0;
const OBJECTIVE_PLACEHOLDER: &str = "Describe the improvement or task";
const SCOPE_PLACEHOLDER: &str = "Optional: crates/sinomo-dfs/**, docs/**";

pub struct EvolutionState {
    pub dashboard: EvolutionDashboard,
    pub selected: Option<usize>,
    pub objective_draft: String,
    pub scope_draft: String,
    pub target_ring_index: usize,
    pub start_status: Option<String>,
    pub start_error: Option<String>,
    start_rx: Option<Receiver<EvolutionStartEvent>>,
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
            start_status: None,
            start_error: None,
            start_rx: None,
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

    pub fn set_scope_draft(&mut self, value: String) {
        self.scope_draft = value;
        self.start_error = None;
    }

    pub fn set_target_ring_index(&mut self, index: usize) {
        self.target_ring_index = index.min(EVOLUTION_TARGET_RINGS.len().saturating_sub(1));
    }

    pub fn can_start(&self) -> bool {
        !self.is_starting() && !self.objective_draft.trim().is_empty()
    }

    pub fn is_starting(&self) -> bool {
        self.start_rx.is_some()
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
    pump_alive: Cell<bool>,
    bounds: Rect,
}

impl EvolutionView {
    fn new(shared: Shared) -> Self {
        let start = WidgetPod::new(StartControls::new(shared.clone()));
        Self {
            shared,
            start,
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
        let changed = self.shared.evolution_mut().pump_start_events(&backend);
        if changed {
            ctx.request_paint();
        }
        if self.shared.evolution().is_starting() {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
    }
}

#[derive(Clone, Copy)]
struct StartControlsLayout {
    objective_label: Rect,
    scope_label: Rect,
    target_label: Rect,
    objective: Rect,
    scope: Rect,
    target: Rect,
    start: Rect,
}

struct StartControls {
    objective: WidgetPod,
    scope: WidgetPod,
    target: WidgetPod,
    start: WidgetPod,
}

impl StartControls {
    fn new(shared: Shared) -> Self {
        let reader = theme_reader(&shared);

        let objective = {
            let s = shared.clone();
            TextInput::new("evolution-objective")
                .value(shared.evolution().objective_draft.clone())
                .placeholder(OBJECTIVE_PLACEHOLDER)
                .theme_when(clone_reader(&reader))
                .on_change(move |value| {
                    s.evolution_mut().set_objective_draft(value);
                })
        };

        let scope = {
            let s = shared.clone();
            TextInput::new("evolution-scope")
                .value(shared.evolution().scope_draft.clone())
                .placeholder(SCOPE_PLACEHOLDER)
                .theme_when(clone_reader(&reader))
                .on_change(move |value| {
                    s.evolution_mut().set_scope_draft(value);
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
                .on_press(move || {
                    s.evolution_mut().start();
                })
        };

        Self {
            objective: WidgetPod::new(objective),
            scope: WidgetPod::new(scope),
            target: WidgetPod::new(target),
            start: WidgetPod::new(start),
        }
    }
}

impl Widget for StartControls {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let bounds = Rect::new(0.0, 0.0, constraints.max.width, constraints.max.height);
        let layout = start_controls_layout(bounds);
        self.objective
            .measure(ctx, Constraints::tight(rect_size(layout.objective)));
        self.scope
            .measure(ctx, Constraints::tight(rect_size(layout.scope)));
        self.target
            .measure(ctx, Constraints::tight(rect_size(layout.target)));
        self.start
            .measure(ctx, Constraints::tight(rect_size(layout.start)));
        constraints.max
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let layout = start_controls_layout(bounds);
        self.objective.arrange(ctx, layout.objective);
        self.scope.arrange(ctx, layout.scope);
        self.target.arrange(ctx, layout.target);
        self.start.arrange(ctx, layout.start);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.objective.paint(ctx);
        self.scope.paint(ctx);
        self.target.paint(ctx);
        self.start.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.objective.semantics(ctx);
        self.scope.semantics(ctx);
        self.target.semantics(ctx);
        self.start.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.objective);
        visitor.visit(&self.scope);
        visitor.visit(&self.target);
        visitor.visit(&self.start);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.objective);
        visitor.visit(&mut self.scope);
        visitor.visit(&mut self.target);
        visitor.visit(&mut self.start);
    }
}

fn start_controls_layout(bounds: Rect) -> StartControlsLayout {
    let block_h = control_block_height();
    if bounds.width() >= START_BREAKPOINT {
        let button_w = 112.0;
        let target_w = 138.0;
        let fixed_w = button_w + target_w + CONTROL_GAP * 3.0;
        let flexible_w = (bounds.width() - fixed_w).max(0.0);
        let objective_w = flexible_w * 0.58;
        let scope_w = flexible_w - objective_w;
        let label_y = bounds.y() + (bounds.height() - block_h).max(0.0) / 2.0;
        let y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let mut x = bounds.x();
        let objective = Rect::new(x, y, objective_w, CONTROL_H);
        x = objective.max_x() + CONTROL_GAP;
        let scope = Rect::new(x, y, scope_w, CONTROL_H);
        x = scope.max_x() + CONTROL_GAP;
        let target = Rect::new(x, y, target_w, CONTROL_H);
        x = target.max_x() + CONTROL_GAP;
        let start = Rect::new(x, y, button_w, CONTROL_H);
        StartControlsLayout {
            objective_label: label_rect(objective, label_y),
            scope_label: label_rect(scope, label_y),
            target_label: label_rect(target, label_y),
            objective,
            scope,
            target,
            start,
        }
    } else {
        let label_y = bounds.y();
        let row1_y = label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let row2_label_y = row1_y + CONTROL_H + CONTROL_GAP;
        let row2_y = row2_label_y + CONTROL_LABEL_H + CONTROL_LABEL_GAP;
        let button_w = bounds.width().min(104.0);
        let target_w = (bounds.width() - button_w - CONTROL_GAP)
            .max(0.0)
            .min(138.0);
        let scope_w = (bounds.width() - button_w - target_w - CONTROL_GAP * 2.0).max(0.0);
        let objective = Rect::new(bounds.x(), row1_y, bounds.width(), CONTROL_H);
        let scope = Rect::new(bounds.x(), row2_y, scope_w, CONTROL_H);
        let target = Rect::new(scope.max_x() + CONTROL_GAP, row2_y, target_w, CONTROL_H);
        let start = Rect::new(target.max_x() + CONTROL_GAP, row2_y, button_w, CONTROL_H);
        StartControlsLayout {
            objective_label: label_rect(objective, label_y),
            scope_label: label_rect(scope, row2_label_y),
            target_label: label_rect(target, row2_label_y),
            objective,
            scope,
            target,
            start,
        }
    }
}

fn control_block_height() -> f32 {
    CONTROL_LABEL_H + CONTROL_LABEL_GAP + CONTROL_H
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
                let layout = layout_for(ctx.bounds());
                if layout.queue.contains(pointer.position) {
                    let row_start = layout.queue.y() + QUEUE_HEADER_H;
                    let index = ((pointer.position.y - row_start) / ROW_H).floor() as isize;
                    let job_count = self.shared.evolution().dashboard.jobs.len();
                    if index >= 0 && (index as usize) < job_count {
                        self.shared.evolution_mut().selected = Some(index as usize);
                        ctx.request_paint();
                        ctx.set_handled();
                    }
                }
                if self.shared.evolution().is_starting() {
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
        let layout = layout_for(Rect::new(
            0.0,
            0.0,
            constraints.max.width,
            constraints.max.height,
        ));
        self.start
            .measure(ctx, Constraints::tight(rect_size(layout.start_controls)));
        constraints.max
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bounds = bounds;
        let layout = layout_for(bounds);
        self.start.arrange(ctx, layout.start_controls);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let bounds = ctx.bounds();
        let layout = layout_for(bounds);
        let (dashboard, selected) = {
            let state = self.shared.evolution();
            (state.dashboard.clone(), state.selected)
        };
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
        );
        self.start.paint(ctx);
        paint_queue(ctx, &theme, &pal, layout.queue, &dashboard, selected);
        paint_detail(ctx, &theme, &pal, layout.detail, selected_job);
        if let Some(rail) = layout.rail {
            paint_rail(ctx, &theme, &pal, rail, selected_job);
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
        self.start.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.start);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.start);
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

fn layout_for(bounds: Rect) -> EvolutionLayout {
    let header = Rect::new(bounds.x(), bounds.y(), bounds.width(), HEADER_H);
    let start_h = start_height_for(bounds.width());
    let start = Rect::new(bounds.x(), header.max_y(), bounds.width(), start_h);
    let controls_h = start_controls_height_for(bounds.width());
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

fn start_height_for(width: f32) -> f32 {
    if width >= START_BREAKPOINT {
        START_WIDE_H
    } else {
        START_NARROW_H
    }
}

fn start_controls_height_for(width: f32) -> f32 {
    if width >= START_BREAKPOINT {
        control_block_height()
    } else {
        control_block_height() * 2.0 + CONTROL_GAP
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

    let subtitle = match &dashboard.error {
        Some(error) => format!("local evidence unavailable - {}", compact(error, 110)),
        None => format!(
            "{} jobs - {} active - {} awaiting decision - source {}",
            dashboard.jobs.len(),
            dashboard.active_count(),
            dashboard.awaiting_decision_count(),
            tail_path(&dashboard.source, 42)
        ),
    };
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
    paint_start_labels(ctx, theme, pal, controls_rect);
}

fn paint_start_labels(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    controls_rect: Rect,
) {
    let layout = start_controls_layout(controls_rect);
    paint_start_label(ctx, theme, pal, layout.objective_label, "Objective");
    paint_start_label(ctx, theme, pal, layout.scope_label, "Scope");
    paint_start_label(ctx, theme, pal, layout.target_label, "Target ring");
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
            .unwrap_or("No self-evolution jobs have been recorded");
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
        "Awaiting a persisted candidate record",
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
        &[
            ("Queue".to_string(), "no candidate selected".to_string()),
            ("Evidence".to_string(), "not available".to_string()),
        ],
    );
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
    job: Option<&EvolutionJobSummary>,
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
        let rows = [
            ("Decision".to_string(), "no candidate selected".to_string()),
            ("Artifacts".to_string(), "not available".to_string()),
            ("Rollback".to_string(), "not available".to_string()),
        ];
        paint_rail_section(
            ctx,
            theme,
            pal,
            Rect::new(inner.x(), inner.y() + 38.0, inner.width(), 108.0),
            "Readiness",
            &rows,
        );
        return;
    };

    let mut y = inner.y() + 38.0;
    y = paint_rail_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 116.0),
        "Decision",
        &decision_rows(job),
    );
    y += SECTION_GAP;
    y = paint_rail_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 120.0),
        "Rollback",
        &rollback_rows(job),
    );
    y += SECTION_GAP;
    paint_rail_section(
        ctx,
        theme,
        pal,
        Rect::new(inner.x(), y, inner.width(), 196.0),
        "Artifacts",
        &artifact_rows(job),
    );
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

fn decision_rows(job: &EvolutionJobSummary) -> Vec<(String, String)> {
    match &job.decision {
        Some(decision) => vec![
            ("Outcome".to_string(), decision.outcome.clone()),
            ("By".to_string(), decision.decided_by.clone()),
            ("At".to_string(), decision.decided_at.clone()),
            (
                "Preflight".to_string(),
                decision
                    .preflight_passed
                    .map(|passed| if passed { "passed" } else { "failed" }.to_string())
                    .unwrap_or_else(|| "not recorded".to_string()),
            ),
            ("Note".to_string(), decision.note.clone()),
        ],
        None => vec![
            ("Status".to_string(), job.status_label.clone()),
            (
                "Decision".to_string(),
                if job.status_key == "awaiting-approval" {
                    "waiting for operator review".to_string()
                } else {
                    "not requested yet".to_string()
                },
            ),
            (
                "Preflight".to_string(),
                job.scorecard_path
                    .as_ref()
                    .map(|_| "scorecard available".to_string())
                    .unwrap_or_else(|| "scorecard missing".to_string()),
            ),
        ],
    }
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
    fn wide_start_controls_reserve_label_space() {
        let layout =
            start_controls_layout(Rect::new(0.0, 0.0, 900.0, start_controls_height_for(900.0)));

        assert!(layout.objective_label.max_y() <= layout.objective.y());
        assert!(layout.scope_label.max_y() <= layout.scope.y());
        assert!(layout.target_label.max_y() <= layout.target.y());
        assert_eq!(layout.objective.y(), layout.scope.y());
        assert_eq!(layout.scope.y(), layout.target.y());
        assert_eq!(layout.target.y(), layout.start.y());
    }

    #[test]
    fn narrow_start_controls_stack_labeled_rows() {
        let layout =
            start_controls_layout(Rect::new(0.0, 0.0, 520.0, start_controls_height_for(520.0)));

        assert!(layout.objective_label.max_y() <= layout.objective.y());
        assert!(layout.objective.max_y() + CONTROL_GAP <= layout.scope_label.y());
        assert!(layout.scope_label.max_y() <= layout.scope.y());
        assert!(layout.target_label.max_y() <= layout.target.y());
        assert!(layout.scope.max_x() <= layout.target.x());
        assert!(layout.target.max_x() <= layout.start.x());
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
    fn start_form_opens_blank_with_prompt_placeholders() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        assert!(shared.evolution().objective_draft.is_empty());
        assert!(shared.evolution().scope_draft.is_empty());
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
            .to_have_value("")?;

        Ok(())
    }
}
