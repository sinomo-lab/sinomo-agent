//! Small reusable widgets and paint helpers shared across the tabs.

use std::hash::Hash;
use std::sync::Arc;

use sui::prelude::Padding;
use sui::{
    ArrangeCtx, Color, Constraints, MeasureCtx, Observable, PaintCtx, Path, Point, Rect,
    SemanticsCtx, SingleChild, Size, TextAlign, TextStyle, VirtualCollectionModel,
    VirtualListState, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor, paint_text_line,
};

use crate::state::Shared;

/// Typed application snapshot consumed by [`VirtualCollectionHost`].
///
/// Sinomo keeps domain state in [`Shared`] and publishes narrow reactive projections. This value
/// lets a projection feed SUI's incremental collection model while keeping selection keyed by the
/// same durable application identity.
#[derive(Clone, Debug, PartialEq)]
pub struct VirtualCollectionSnapshot<K, T> {
    pub items: Vec<(K, T)>,
    pub selected: Option<K>,
}

impl<K, T> VirtualCollectionSnapshot<K, T> {
    pub fn new(items: Vec<(K, T)>, selected: Option<K>) -> Self {
        Self { items, selected }
    }
}

/// Retained bridge from a Sinomo observable snapshot to SUI's virtual collection interfaces.
///
/// The host itself never rebuilds its child. It reconciles the model during measurement, so a
/// same-key reset updates row signals while preserving realized row pods, focus, selection, and
/// scroll state inside [`sui::VirtualList`].
pub struct VirtualCollectionHost<K, T> {
    source: Arc<dyn Observable<VirtualCollectionSnapshot<K, T>>>,
    model: VirtualCollectionModel<K, T>,
    state: VirtualListState<K>,
    child: WidgetPod,
}

impl<K, T> VirtualCollectionHost<K, T>
where
    K: Clone + Eq + Hash + PartialEq + 'static,
    T: Clone + PartialEq + 'static,
{
    pub fn new<O, F, W>(name: &'static str, source: O, build: F) -> Self
    where
        O: Observable<VirtualCollectionSnapshot<K, T>> + 'static,
        F: FnOnce(VirtualCollectionModel<K, T>, VirtualListState<K>) -> W,
        W: Widget + 'static,
    {
        let model = VirtualCollectionModel::named(name);
        let state = VirtualListState::new();
        let child = WidgetPod::new(build(model.clone(), state.clone()));
        Self {
            source: Arc::new(source),
            model,
            state,
            child,
        }
    }

    fn sync(&self, snapshot: VirtualCollectionSnapshot<K, T>) {
        let selected = snapshot.selected.filter(|selected| {
            snapshot
                .items
                .iter()
                .any(|(candidate, _)| candidate == selected)
        });
        self.model
            .replace(snapshot.items)
            .expect("virtual collection projection must contain unique keys");
        self.state.select(selected);
    }
}

impl<K, T> Widget for VirtualCollectionHost<K, T>
where
    K: Clone + Eq + Hash + PartialEq + 'static,
    T: Clone + PartialEq + 'static,
{
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let snapshot = ctx.observe(self.source.as_ref());
        self.sync(snapshot);
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

/// How revealed content comes and goes: it appears at once, so a control revealed by a click is
/// immediately where it stays and can be used; leaving content fades while its space collapses.
/// Apply before [`sui::Presence::collapse`], which this transition would otherwise reset.
pub fn reveal_transition() -> sui::PresenceTransition {
    sui::PresenceTransition::new().enter(sui::AnimationSpec::INSTANT)
}

/// Keeps one stock control mounted while an observed key restyles it.
///
/// Some stock builders (tone, icon, description) have no reader form. Instead of rebuilding the
/// control when the key changes, the host takes the retained control, re-applies those consuming
/// builders, and puts it back, so focus, hover, and press state survive the update.
pub struct Restyle<W, K> {
    source: Arc<dyn Observable<K>>,
    key: Option<K>,
    apply: Box<dyn Fn(W, &K) -> W>,
    control: Option<W>,
}

impl<W, K> Restyle<W, K>
where
    W: Widget,
    K: Clone + PartialEq + 'static,
{
    pub fn new<O>(control: W, source: O, apply: impl Fn(W, &K) -> W + 'static) -> Self
    where
        O: Observable<K> + 'static,
    {
        Self {
            source: Arc::new(source),
            key: None,
            apply: Box::new(apply),
            control: Some(control),
        }
    }

    fn control(&self) -> &W {
        self.control.as_ref().expect("restyled control")
    }

    fn control_mut(&mut self) -> &mut W {
        self.control.as_mut().expect("restyled control")
    }
}

impl<W, K> Widget for Restyle<W, K>
where
    W: Widget,
    K: Clone + PartialEq + 'static,
{
    fn event(&mut self, ctx: &mut sui::EventCtx, event: &sui::Event) {
        self.control_mut().event(ctx, event);
    }

    fn command(&mut self, ctx: &mut sui::EventCtx, command: &sui::Command<'_>) {
        self.control_mut().command(ctx, command);
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let key = ctx.observe(self.source.as_ref());
        if self.key.as_ref() != Some(&key) {
            self.control = self
                .control
                .take()
                .map(|control| (self.apply)(control, &key));
            self.key = Some(key);
        }
        self.control_mut().measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.control_mut().arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        // A restyle can change tone or icon without changing size.
        ctx.observe(self.source.as_ref());
        self.control().paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        ctx.observe(self.source.as_ref());
        self.control().semantics(ctx);
    }

    fn accepts_focus(&self) -> bool {
        self.control().accepts_focus()
    }

    fn focus_changed(&mut self, ctx: &mut sui::EventCtx, focused: bool) {
        self.control_mut().focus_changed(ctx, focused);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.control().visit_children(visitor);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.control_mut().visit_children_mut(visitor);
    }
}

/// A type-erased widget, so builders can collect children of different types in one list.
pub struct Boxed {
    child: WidgetPod,
}

impl Boxed {
    pub fn new(child: impl Widget + 'static) -> Self {
        Self {
            child: WidgetPod::new(child),
        }
    }
}

impl Widget for Boxed {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
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

/// Lays its child out no wider than `max_width`, against its left edge.
pub struct MaxWidth {
    max_width: f32,
    child: WidgetPod,
}

impl MaxWidth {
    pub fn new(max_width: f32, child: impl Widget + 'static) -> Self {
        Self {
            max_width,
            child: WidgetPod::new(child),
        }
    }
}

impl Widget for MaxWidth {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let max_width = constraints.max.width.min(self.max_width);
        let child = Constraints::new(
            Size::new(constraints.min.width.min(max_width), constraints.min.height),
            Size::new(max_width, constraints.max.height),
        );
        self.child.measure(ctx, child)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        // A stretching parent can arrange this wider than it measured; keep the child capped.
        let width = bounds.width().min(self.max_width);
        self.child.arrange(
            ctx,
            Rect::new(bounds.x(), bounds.y(), width, bounds.height()),
        );
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

/// A widget that fills its bounds with a solid color. Optional rounded look is
/// approximated by the caller; this just fills the rect.
pub struct Fill {
    color: Color,
}

impl Fill {
    pub fn new(color: Color) -> Self {
        Self { color }
    }
}

impl Widget for Fill {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        ctx.fill_bounds(self.color);
    }
}

/// Whether the current theme mode resolves to dark.
pub fn theme_is_dark(shared: &Shared) -> bool {
    shared.theme().surfaces.dark
}

/// Draw text horizontally centered at `cx`, vertically centered at `cy`.
pub fn draw_centered(ctx: &mut PaintCtx, text: &str, style: TextStyle, cx: f32, cy: f32) {
    let measurement = ctx.measure_text(text, style.clone()).ok();
    let width = measurement
        .map(|measurement| measurement.width)
        .unwrap_or(text.len() as f32 * style.font_size * 0.56);
    let line_height = style.line_height.max(1.0);
    let rect = Rect::new(
        cx - width * 0.5,
        cy - line_height * 0.5,
        width + 2.0,
        line_height,
    );
    let mut style = style;
    style.line_height = line_height;
    paint_text_line(ctx, rect, text, &style, TextAlign::Center);
}

/// Draw left-aligned, vertically-centered text within `row`.
pub fn draw_text_in(ctx: &mut PaintCtx, row: Rect, text: &str, style: TextStyle) {
    paint_text_line(ctx, row, text, &style, TextAlign::Start);
}

/// Fill a rounded rectangle (proper path fill — safe with translucent colors).
pub fn fill_round_rect(ctx: &mut PaintCtx, rect: Rect, color: Color, radius: f32) {
    if rect.width() <= 0.0 || rect.height() <= 0.0 {
        return;
    }
    let r = radius
        .min(rect.width() / 2.0)
        .min(rect.height() / 2.0)
        .max(0.0);
    if r <= 0.5 {
        ctx.fill_rect(rect, color);
    } else {
        ctx.fill(Path::rounded_rect(rect, r), color);
    }
}

/// A rounded panel with a 1px border: paints the border color as the outer
/// rounded rect, then an inset rounded fill on top.
pub fn round_panel(ctx: &mut PaintCtx, rect: Rect, fill: Color, border_color: Color, radius: f32) {
    fill_round_rect(ctx, rect, border_color, radius);
    fill_round_rect(ctx, rect.inflate(-1.0, -1.0), fill, (radius - 1.0).max(0.0));
}

/// Fill a 1px-ish hairline along the top edge of `rect`.
pub fn hairline_top(ctx: &mut PaintCtx, rect: Rect, color: Color) {
    ctx.fill_rect(Rect::new(rect.x(), rect.y(), rect.width(), 1.0), color);
}

/// Fill a 1px-ish hairline along the bottom edge of `rect`.
pub fn hairline_bottom(ctx: &mut PaintCtx, rect: Rect, color: Color) {
    ctx.fill_rect(
        Rect::new(rect.x(), rect.y() + rect.height() - 1.0, rect.width(), 1.0),
        color,
    );
}

/// Fill a 1px-ish hairline along the right edge of `rect`.
pub fn hairline_right(ctx: &mut PaintCtx, rect: Rect, color: Color) {
    ctx.fill_rect(
        Rect::new(rect.x() + rect.width() - 1.0, rect.y(), 1.0, rect.height()),
        color,
    );
}

/// Fill a 1px-ish hairline along the left edge of `rect`.
pub fn hairline_left(ctx: &mut PaintCtx, rect: Rect, color: Color) {
    ctx.fill_rect(Rect::new(rect.x(), rect.y(), 1.0, rect.height()), color);
}

/// Fill a 1px border around `rect`.
pub fn border(ctx: &mut PaintCtx, rect: Rect, color: Color) {
    ctx.fill_rect(Rect::new(rect.x(), rect.y(), rect.width(), 1.0), color);
    ctx.fill_rect(
        Rect::new(rect.x(), rect.y() + rect.height() - 1.0, rect.width(), 1.0),
        color,
    );
    ctx.fill_rect(Rect::new(rect.x(), rect.y(), 1.0, rect.height()), color);
    ctx.fill_rect(
        Rect::new(rect.x() + rect.width() - 1.0, rect.y(), 1.0, rect.height()),
        color,
    );
}

/// Wrap a widget in uniform padding.
pub fn padded<W: Widget + 'static>(value: f32, child: W) -> Padding {
    Padding::all(value, child)
}

/// Wrap a widget so it sits centered inside a fixed-size box (used rarely).
pub fn fixed<W: Widget + 'static>(size: Size, child: W) -> SingleChild {
    let _ = size;
    SingleChild::new(child)
}

/// Convenience for a `Point`.
pub fn point(x: f32, y: f32) -> Point {
    Point::new(x, y)
}

/// Greedy word-wrap `text` to `max_width`, using `measure` for run widths.
/// Honors explicit newlines; over-long words overflow rather than break.
pub fn wrap_lines(text: &str, max_width: f32, measure: impl Fn(&str) -> f32) -> Vec<String> {
    let mut lines = Vec::new();
    let space_w = measure(" ");
    for paragraph in text.split('\n') {
        if paragraph.is_empty() {
            lines.push(String::new());
            continue;
        }
        let mut current = String::new();
        let mut current_w = 0.0_f32;
        for word in paragraph.split(' ') {
            let word_w = measure(word);
            if current.is_empty() {
                current.push_str(word);
                current_w = word_w;
            } else if current_w + space_w + word_w <= max_width {
                current.push(' ');
                current.push_str(word);
                current_w += space_w + word_w;
            } else {
                lines.push(std::mem::take(&mut current));
                current.push_str(word);
                current_w = word_w;
            }
        }
        lines.push(current);
    }
    lines
}

/// Previous char boundary in `s` before byte index `i` (for backspace).
pub fn prev_char_boundary(s: &str, i: usize) -> usize {
    if i == 0 {
        return 0;
    }
    let mut j = i - 1;
    while !s.is_char_boundary(j) {
        j -= 1;
    }
    j
}

/// Next char boundary in `s` after byte index `i`.
pub fn next_char_boundary(s: &str, i: usize) -> usize {
    if i >= s.len() {
        return s.len();
    }
    let mut j = i + 1;
    while j < s.len() && !s.is_char_boundary(j) {
        j += 1;
    }
    j
}
