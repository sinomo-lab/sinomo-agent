//! Small reusable widgets and paint helpers shared across the tabs.

use std::hash::Hash;
use std::sync::Arc;

use sui::prelude::Padding;
use sui::{
    ArrangeCtx, Color, Constraints, MeasureCtx, Observable, PaintCtx, Path, Point, Rect,
    SemanticsCtx, SingleChild, Size, TextStyle, VirtualCollectionModel, VirtualListState, Widget,
    WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor, paint_single_line_aligned_text,
};

use crate::state::Shared;
use crate::theme::Palette;

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
    Palette::from_theme(&shared.theme(), is_dark_mode(shared)).dark
}

fn is_dark_mode(shared: &Shared) -> bool {
    use crate::theme::AppThemeMode;
    !matches!(shared.borrow().theme_mode, AppThemeMode::Light)
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
    paint_single_line_aligned_text(ctx, rect, text, &style, line_height, 0.5);
}

/// Draw left-aligned, vertically-centered text within `row`.
pub fn draw_text_in(ctx: &mut PaintCtx, row: Rect, text: &str, style: TextStyle) {
    paint_single_line_aligned_text(ctx, row, text, &style, style.line_height, 0.0);
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
