//! Small reusable widgets and paint helpers shared across the tabs.

use sui::{ArrangeCtx, prelude::Padding};
use sui::{
    Color, Constraints, IconGlyph, MeasureCtx, PaintCtx, Path, Point, Rect, SemanticsCtx,
    SingleChild, Size, TextStyle, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
    draw_glyph,
};

use crate::state::Shared;
use crate::theme::Palette;

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

/// Vertical dock: an optional fixed-height header at the top, an optional fixed
/// footer at the bottom, and a body that fills the remaining space.
pub struct Dock {
    top: Option<WidgetPod>,
    bottom: Option<WidgetPod>,
    body: WidgetPod,
    top_height: f32,
    bottom_height: f32,
}

impl Dock {
    pub fn new<W: Widget + 'static>(body: W) -> Self {
        Self {
            top: None,
            bottom: None,
            body: WidgetPod::new(body),
            top_height: 0.0,
            bottom_height: 0.0,
        }
    }

    pub fn top<W: Widget + 'static>(mut self, height: f32, widget: W) -> Self {
        self.top = Some(WidgetPod::new(widget));
        self.top_height = height;
        self
    }

    pub fn bottom<W: Widget + 'static>(mut self, height: f32, widget: W) -> Self {
        self.bottom = Some(WidgetPod::new(widget));
        self.bottom_height = height;
        self
    }
}

impl Widget for Dock {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;
        let w = size.width;
        if let Some(top) = &mut self.top {
            top.measure(ctx, Constraints::tight(Size::new(w, self.top_height)));
        }
        if let Some(bottom) = &mut self.bottom {
            bottom.measure(ctx, Constraints::tight(Size::new(w, self.bottom_height)));
        }
        let body_h = (size.height - self.top_height - self.bottom_height).max(0.0);
        self.body
            .measure(ctx, Constraints::tight(Size::new(w, body_h)));
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let w = bounds.width();
        if let Some(top) = &mut self.top {
            top.arrange(ctx, Rect::new(bounds.x(), bounds.y(), w, self.top_height));
        }
        let body_y = bounds.y() + self.top_height;
        let body_h = (bounds.height() - self.top_height - self.bottom_height).max(0.0);
        self.body
            .arrange(ctx, Rect::new(bounds.x(), body_y, w, body_h));
        if let Some(bottom) = &mut self.bottom {
            bottom.arrange(
                ctx,
                Rect::new(
                    bounds.x(),
                    bounds.y() + bounds.height() - self.bottom_height,
                    w,
                    self.bottom_height,
                ),
            );
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.body.paint(ctx);
        if let Some(top) = &self.top {
            top.paint(ctx);
        }
        if let Some(bottom) = &self.bottom {
            bottom.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if let Some(top) = &self.top {
            top.semantics(ctx);
        }
        self.body.semantics(ctx);
        if let Some(bottom) = &self.bottom {
            bottom.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        if let Some(top) = &self.top {
            visitor.visit(top);
        }
        visitor.visit(&self.body);
        if let Some(bottom) = &self.bottom {
            visitor.visit(bottom);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        if let Some(top) = &mut self.top {
            visitor.visit(top);
        }
        visitor.visit(&mut self.body);
        if let Some(bottom) = &mut self.bottom {
            visitor.visit(bottom);
        }
    }
}

/// A centered empty-state panel (icon mark, title, subtitle). The mark is a real Lucide
/// [`IconGlyph`] when one is supplied via [`Placeholder::glyph`], otherwise a text rune.
pub struct Placeholder {
    shared: Shared,
    mark: String,
    glyph: Option<IconGlyph>,
    title: String,
    subtitle: String,
    action: Option<WidgetPod>,
    action_height: f32,
}

impl Placeholder {
    pub fn new(
        shared: Shared,
        mark: impl Into<String>,
        title: impl Into<String>,
        subtitle: impl Into<String>,
    ) -> Self {
        Self {
            shared,
            mark: mark.into(),
            glyph: None,
            title: title.into(),
            subtitle: subtitle.into(),
            action: None,
            action_height: 32.0,
        }
    }

    /// Use a real Lucide icon as the centered mark instead of the text rune.
    pub fn glyph(mut self, glyph: IconGlyph) -> Self {
        self.glyph = Some(glyph);
        self
    }

    /// Add a compact centered action below the subtitle.
    pub fn action<W>(mut self, action: W) -> Self
    where
        W: Widget + 'static,
    {
        self.action = Some(WidgetPod::new(action));
        self
    }

    /// Override the compact action slot height for multi-row recovery actions.
    pub fn action_height(mut self, height: f32) -> Self {
        self.action_height = height.max(32.0);
        self
    }
}

impl Widget for Placeholder {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        if let Some(action) = &mut self.action {
            let action_width = 360.0_f32.min((constraints.max.width - 32.0).max(0.0));
            action.measure(
                ctx,
                Constraints::tight(Size::new(action_width, self.action_height)),
            );
        }
        constraints.max
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        if let Some(action) = &mut self.action {
            let w = 360.0_f32.min((bounds.width() - 32.0).max(0.0));
            let cx = bounds.x() + bounds.width() / 2.0;
            let cy = bounds.y() + bounds.height() / 2.0 - 18.0;
            action.arrange(
                ctx,
                Rect::new(cx - w / 2.0, cy + 54.0, w, self.action_height),
            );
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, theme_is_dark(&self.shared));
        let bounds = ctx.bounds();
        ctx.fill_bounds(pal.window);

        let cx = bounds.x() + bounds.width() / 2.0;
        let cy =
            bounds.y() + bounds.height() / 2.0 - if self.action.is_some() { 18.0 } else { 0.0 };

        match self.glyph {
            Some(glyph) => {
                let side = 40.0;
                draw_glyph(
                    ctx,
                    glyph,
                    Rect::new(cx - side / 2.0, cy - 46.0 - side / 2.0, side, side),
                    pal.text_faint,
                );
            }
            None => {
                let mark_style =
                    pal.text_style(&theme, 40.0, sui::FontWeight::BOLD, pal.text_faint);
                draw_centered(ctx, &self.mark, mark_style, cx, cy - 46.0);
            }
        }

        let title_style = pal.text_style(&theme, 18.0, sui::FontWeight::SEMIBOLD, pal.text_muted);
        draw_centered(ctx, &self.title, title_style, cx, cy + 4.0);

        let sub_style = pal.text_style(&theme, 13.0, sui::FontWeight::NORMAL, pal.text_faint);
        draw_centered(ctx, &self.subtitle, sub_style, cx, cy + 30.0);

        if let Some(action) = &self.action {
            action.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if let Some(action) = &self.action {
            action.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        if let Some(action) = &self.action {
            visitor.visit(action);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        if let Some(action) = &mut self.action {
            visitor.visit(action);
        }
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
    let (w, h) = measurement
        .map(|m| (m.width, m.height))
        .unwrap_or((text.len() as f32 * 8.0, style.line_height));
    let rect = Rect::new(cx - w / 2.0, cy - h / 2.0, w + 2.0, h + 2.0);
    ctx.draw_text(rect, text, style);
}

/// Draw left-aligned, vertically-centered text within `row`.
pub fn draw_text_in(ctx: &mut PaintCtx, row: Rect, text: &str, style: TextStyle) {
    let h = ctx
        .measure_text(text, style.clone())
        .map(|m| m.height)
        .unwrap_or(style.line_height);
    let y = row.y() + (row.height() - h) / 2.0;
    ctx.draw_text(Rect::new(row.x(), y, row.width(), h + 2.0), text, style);
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
