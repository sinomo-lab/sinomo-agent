//! The application shell: a title bar (brand mark, tab segment, status pill) above
//! a switchable content area.

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, Image, ImageFit, ImageHandle,
    MeasureCtx, PaintCtx, PointerButton, PointerEventKind, Rect, SingleChild, Size, SwitchView,
    Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
};

use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::{chat, files, settings, widgets};

pub const TOPBAR_HEIGHT: f32 = 54.0;
const LOGO_SIZE: f32 = 26.0;
const EDGE_PAD: f32 = 18.0;
const TAB_GAP: f32 = 4.0;
const TAB_PAD_X: f32 = 18.0;
const TAB_SEG_HEIGHT: f32 = 34.0;

/// Stable image handle for the brand logo (registered at startup).
pub fn logo_image_handle() -> ImageHandle {
    ImageHandle::new(0x5_0901)
}

pub struct Shell {
    shared: Shared,
    logo: SingleChild,
    /// The switchable tab content — built only once the backend connection resolves. `None`
    /// while connecting, when a loading screen is shown in its place.
    content: Option<WidgetPod>,
    tab_rects: Vec<(Tab, Rect)>,
    tab_widths: Vec<f32>,
    bar_rect: Rect,
    body_rect: Rect,
    hovered_tab: Option<Tab>,
    pressed_tab: Option<Tab>,
}

impl Shell {
    pub fn new(shared: Shared) -> Self {
        let logo = SingleChild::new(
            Image::new(logo_image_handle())
                .fit(ImageFit::Contain)
                .size(Size::new(LOGO_SIZE, LOGO_SIZE))
                .without_border()
                .corner_radius(0.0),
        );

        let mut shell = Self {
            shared,
            logo,
            content: None,
            tab_rects: Vec::new(),
            tab_widths: Vec::new(),
            bar_rect: Rect::ZERO,
            body_rect: Rect::ZERO,
            hovered_tab: None,
            pressed_tab: None,
        };
        // Built already-ready (headless tooling / visual tests) → show the tabs immediately. The
        // interactive app builds content when the connection resolves (see `event`).
        if shell.shared.is_ready() {
            shell.build_content();
        }
        shell
    }

    /// Build the switchable tab content. Called once the backend is ready.
    fn build_content(&mut self) {
        let content = SwitchView::new()
            .with_child(chat::view(self.shared.clone()))
            .with_child(files::view(self.shared.clone()))
            .with_child(settings::view(self.shared.clone()))
            .selected_when({
                let shared = self.shared.clone();
                move || shared.active_tab().index()
            });
        self.content = Some(WidgetPod::new(content));
    }

    fn tab_text_style(&self) -> sui::TextStyle {
        let theme = self.shared.theme();
        sui::TextStyle {
            font_size: 13.5,
            line_height: 18.0,
            weight: FontWeight::MEDIUM,
            ..theme.body_text_style()
        }
    }

    fn tab_at(&self, position: sui::Point) -> Option<Tab> {
        self.tab_rects
            .iter()
            .find(|(_, rect)| rect.contains(position))
            .map(|(tab, _)| *tab)
    }

    fn switch_to(&mut self, tab: Tab, ctx: &mut EventCtx) {
        if self.shared.active_tab() != tab {
            self.shared.borrow_mut().active_tab = tab;
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
        }
        ctx.request_paint();
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let dark = widgets::theme_is_dark(&self.shared);
        let pal = Palette::from_theme(&theme, dark);
        (theme, pal)
    }
}

impl Widget for Shell {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;
        let style = self.tab_text_style();

        self.tab_widths = Tab::ALL
            .iter()
            .map(|tab| {
                ctx.layout()
                    .measure_text(tab.title(), style.clone())
                    .map(|m| (m.width + TAB_PAD_X * 2.0).clamp(72.0, 220.0))
                    .unwrap_or(96.0)
            })
            .collect();

        self.logo
            .measure(ctx, Constraints::tight(Size::new(LOGO_SIZE, LOGO_SIZE)));

        let body_h = (size.height - TOPBAR_HEIGHT).max(0.0);
        if let Some(content) = &mut self.content {
            content.measure(ctx, Constraints::tight(Size::new(size.width, body_h)));
        }

        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bar_rect = Rect::new(bounds.x(), bounds.y(), bounds.width(), TOPBAR_HEIGHT);

        let logo_y = bounds.y() + (TOPBAR_HEIGHT - LOGO_SIZE) / 2.0;
        self.logo.arrange(
            ctx,
            Rect::new(bounds.x() + EDGE_PAD, logo_y, LOGO_SIZE, LOGO_SIZE),
        );

        // Tab segment begins after the brand mark + wordmark.
        let seg_x = bounds.x() + EDGE_PAD + LOGO_SIZE + 12.0 + brand_word_width() + 26.0;
        let seg_y = bounds.y() + (TOPBAR_HEIGHT - TAB_SEG_HEIGHT) / 2.0;
        let mut x = seg_x;
        self.tab_rects.clear();
        for (i, tab) in Tab::ALL.iter().enumerate() {
            let w = self.tab_widths.get(i).copied().unwrap_or(96.0);
            self.tab_rects
                .push((*tab, Rect::new(x, seg_y, w, TAB_SEG_HEIGHT)));
            x += w + TAB_GAP;
        }

        let body_y = bounds.y() + TOPBAR_HEIGHT;
        let body_h = (bounds.height() - TOPBAR_HEIGHT).max(0.0);
        self.body_rect = Rect::new(bounds.x(), body_y, bounds.width(), body_h);
        if let Some(content) = &mut self.content {
            content.arrange(ctx, self.body_rect);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();

        // Content area background, then either the tab content or a loading screen.
        ctx.fill_bounds(pal.window);
        if let Some(content) = &self.content {
            content.paint(ctx);
        } else {
            self.paint_loading(ctx, &theme, &pal);
        }

        // Title bar.
        ctx.fill_rect(self.bar_rect, pal.titlebar);
        widgets::hairline_bottom(ctx, self.bar_rect, pal.border);

        self.logo.paint(ctx);

        // Wordmark.
        let word_x = self.bar_rect.x() + EDGE_PAD + LOGO_SIZE + 12.0;
        let word_style = pal.text_style(&theme, 16.0, FontWeight::SEMIBOLD, pal.text);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                word_x,
                self.bar_rect.y(),
                brand_word_width() + 8.0,
                TOPBAR_HEIGHT,
            ),
            "Sinomo",
            word_style,
        );

        // Tab segment.
        let active = self.shared.active_tab();
        for (tab, rect) in &self.tab_rects {
            let is_active = *tab == active;
            let is_hover = self.hovered_tab == Some(*tab);
            if is_active {
                widgets::fill_round_rect(ctx, *rect, pal.selected, 9.0);
            } else if is_hover {
                widgets::fill_round_rect(ctx, *rect, pal.hover, 9.0);
            }
            let color = if is_active { pal.text } else { pal.text_muted };
            let weight = if is_active {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            };
            let style = pal.text_style(&theme, 13.5, weight, color);
            let measurement = ctx.measure_text(tab.title(), style.clone()).ok();
            let tw = measurement.as_ref().map(|m| m.width).unwrap_or(0.0);
            let th = measurement.as_ref().map(|m| m.height).unwrap_or(16.0);
            let tx = rect.x() + (rect.width() - tw) / 2.0;
            let ty = rect.y() + (rect.height() - th) / 2.0;
            ctx.draw_text(Rect::new(tx, ty, tw + 2.0, th + 2.0), tab.title(), style);
        }

        // Status pill (right-aligned).
        self.paint_status_pill(ctx, &theme, &pal);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        // The background connect thread wakes us via the sui external-wake mechanism; drain its
        // progress/result here. When the backend arrives, build the tab content and relayout.
        if let Event::Custom(custom) = event {
            if custom.kind.as_str() == sui::EXTERNAL_WAKE_KIND {
                let (changed, became_ready) = self.shared.pump_connect();
                if became_ready {
                    self.build_content();
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_semantics();
                }
                // Drain any in-progress provider device-login (Settings tab).
                let device_changed =
                    self.shared.is_ready() && self.shared.settings_mut().pump_device_login();
                if changed || device_changed {
                    ctx.request_paint();
                }
            }
            return;
        }
        if let Event::Pointer(pointer) = event {
            match pointer.kind {
                PointerEventKind::Move => {
                    let hovered = self.tab_at(pointer.position);
                    if hovered != self.hovered_tab {
                        self.hovered_tab = hovered;
                        ctx.request_paint();
                    }
                }
                PointerEventKind::Leave => {
                    if self.hovered_tab.take().is_some() {
                        ctx.request_paint();
                    }
                }
                PointerEventKind::Down if pointer.button == Some(PointerButton::Primary) => {
                    if let Some(tab) = self.tab_at(pointer.position) {
                        self.pressed_tab = Some(tab);
                        ctx.set_handled();
                        ctx.request_paint();
                    }
                }
                PointerEventKind::Up if pointer.button == Some(PointerButton::Primary) => {
                    if let Some(pressed) = self.pressed_tab.take() {
                        if self.tab_at(pointer.position) == Some(pressed) {
                            self.switch_to(pressed, ctx);
                        }
                        ctx.set_handled();
                    }
                }
                _ => {}
            }
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.logo.visit_children(visitor);
        if let Some(content) = &self.content {
            visitor.visit(content);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.logo.visit_children_mut(visitor);
        if let Some(content) = &mut self.content {
            visitor.visit(content);
        }
    }
}

impl Shell {
    /// Centered status while the backend connection is resolving.
    fn paint_loading(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let status = self.shared.status_text();
        let style = pal.text_style(theme, 15.0, FontWeight::MEDIUM, pal.text_muted);
        let measurement = ctx.measure_text(status.clone(), style.clone()).ok();
        let tw = measurement.as_ref().map(|m| m.width).unwrap_or(160.0);
        let th = measurement.as_ref().map(|m| m.height).unwrap_or(20.0);
        let tx = self.body_rect.x() + (self.body_rect.width() - tw) / 2.0;
        let ty = self.body_rect.y() + (self.body_rect.height() - th) / 2.0;
        widgets::draw_text_in(ctx, Rect::new(tx, ty, tw + 4.0, th + 4.0), &status, style);
    }

    fn paint_status_pill(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let label = self.shared.status_text();
        let connected_daemon = {
            let state = self.shared.borrow();
            state
                .ready
                .as_ref()
                .is_some_and(|ready| ready.backend.connection.is_daemon())
        };
        let dot = if connected_daemon {
            pal.good
        } else {
            pal.accent
        };

        let style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text_muted);
        let tw = ctx
            .measure_text(label.clone(), style.clone())
            .map(|m| m.width)
            .unwrap_or(60.0);
        let pill_w = tw + 16.0 + 14.0;
        let pill_h = 26.0;
        let pill_x = self.bar_rect.x() + self.bar_rect.width() - EDGE_PAD - pill_w;
        let pill_y = self.bar_rect.y() + (TOPBAR_HEIGHT - pill_h) / 2.0;
        let pill = Rect::new(pill_x, pill_y, pill_w, pill_h);
        widgets::round_panel(ctx, pill, pal.field, pal.border, pill_h / 2.0);

        // status dot
        let r = 4.0;
        ctx.fill_rect(
            Rect::new(
                pill_x + 12.0 - r,
                pill_y + pill_h / 2.0 - r,
                r * 2.0,
                r * 2.0,
            ),
            dot,
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(pill_x + 22.0, pill_y, pill_w - 22.0, pill_h),
            &label,
            style,
        );
    }
}

/// Approximate width reserved for the "Sinomo" wordmark (kept simple; layout is
/// re-measured for the tab segment which is what users click).
fn brand_word_width() -> f32 {
    58.0
}
