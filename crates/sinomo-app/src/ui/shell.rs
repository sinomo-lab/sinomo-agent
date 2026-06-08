//! The application shell: a title bar (brand mark, tab strip, status pill) above
//! a switchable content area.
//!
//! The chrome is *composed* from `sui-widgets` where the toolkit has a stock control
//! — the tab strip is a [`sui::TabBar`] child pod, and the connecting indicator is a
//! [`sui::Spinner`] child pod. The brand mark (logo [`Image`] + "Sinomo" wordmark) and
//! the status pill stay as light custom paint so they can sit precisely in the title bar.
//!
//! The launch/runtime machinery is UNCHANGED: the connecting→ready state machine, the
//! content hosting (a [`SwitchView`] of the tab views inside a [`WidgetPod`], built once the
//! backend resolves), the loading screen, the [`Event::Custom`] external-wake handler that
//! drives [`Shared::pump_connect`] + the provider device-login pump, and the measure/arrange
//! that reserves the title-bar height and gives the rest to content. The TabBar drives the
//! active tab through `shared.active_tab`; the Shell detects that change on the next redraw
//! (diff-on-redraw) and requests the same relayout the old hand-painted strip did.

use std::cell::Cell;

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, Image, ImageFit, ImageHandle,
    MeasureCtx, PaintCtx, Rect, SemanticsCtx, SemanticsNode, SemanticsRole, SingleChild, Size,
    Spinner, SwitchView, TabBar, ThemeShadow, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor, WindowEvent,
};

use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::{chat, files, settings, widgets};

pub const TOPBAR_HEIGHT: f32 = 44.0;
const LOGO_SIZE: f32 = 24.0;
const EDGE_PAD: f32 = 18.0;
const TAB_SEG_HEIGHT: f32 = 32.0;
const TAB_GAP: f32 = 6.0;
/// Spinner shown in the connecting indicator / loading screen.
const SPINNER_SIZE: f32 = 18.0;

/// Stable image handle for the brand logo (registered at startup).
pub fn logo_image_handle() -> ImageHandle {
    ImageHandle::new(0x5_0901)
}

pub struct Shell {
    shared: Shared,
    logo: SingleChild,
    /// The tab strip, composed from a stock `TabBar`. It drives `shared.active_tab` via
    /// `on_change`; the Shell relayouts the content on the change (see `event`).
    tabs: WidgetPod,
    /// A spinner shown next to the status text while the backend connection resolves.
    spinner: WidgetPod,
    /// The switchable tab content — built only once the backend connection resolves. `None`
    /// while connecting, when a loading screen is shown in its place.
    content: Option<WidgetPod>,
    /// The active tab last laid out. The `TabBar` flips `shared.active_tab` and requests only a
    /// repaint, but the content `SwitchView` only measures/arranges its *active* child — so a tab
    /// switch needs a fresh measure+arrange for the newly-shown view. We diff on `RedrawRequested`
    /// to catch it (the same diff-on-redraw the tabs use internally for nested switches).
    last_tab: Cell<Tab>,
    bar_rect: Rect,
    tab_strip_rect: Rect,
    body_rect: Rect,
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

        let tabs = WidgetPod::new(build_tab_bar(&shared));
        let spinner = WidgetPod::new(
            Spinner::new("shell-connecting")
                .size(SPINNER_SIZE)
                .theme_when({
                    let s = shared.clone();
                    move || s.theme()
                }),
        );

        let initial_tab = shared.active_tab();
        let mut shell = Self {
            shared,
            logo,
            tabs,
            spinner,
            content: None,
            last_tab: Cell::new(initial_tab),
            bar_rect: Rect::ZERO,
            tab_strip_rect: Rect::ZERO,
            body_rect: Rect::ZERO,
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

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let dark = widgets::theme_is_dark(&self.shared);
        let pal = Palette::from_theme(&theme, dark);
        (theme, pal)
    }
}

/// Build the tab strip `TabBar`, bound to the active tab and writing the selection back to
/// `shared.active_tab` on change. Threads the theme reader so it restyles on theme switch.
fn build_tab_bar(shared: &Shared) -> TabBar {
    let labels = Tab::ALL.iter().map(|tab| tab.title());
    let on_change = {
        let s = shared.clone();
        move |index: usize, _label: String| {
            if let Some(tab) = Tab::ALL.get(index).copied() {
                s.borrow_mut().active_tab = tab;
            }
        }
    };
    TabBar::new("shell-tabs")
        .tabs(labels)
        // NOTE: TabBar has no reactive `selected_when`; it caches the clicked tab internally and the
        // Shell relayouts content via diff-on-redraw. Fine today (nothing switches tabs
        // programmatically); a reactive TabBar selection would be a future sui enhancement.
        .selected(shared.active_tab().index())
        .gap(TAB_GAP)
        .theme_when({
            let s = shared.clone();
            move || s.theme()
        })
        .on_change(on_change)
}

impl Widget for Shell {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;

        self.logo
            .measure(ctx, Constraints::tight(Size::new(LOGO_SIZE, LOGO_SIZE)));

        // The tab strip lays itself out at its natural width within the title bar.
        let tab_strip = self.tabs.measure(
            ctx,
            Constraints::new(Size::ZERO, Size::new(size.width, TAB_SEG_HEIGHT)),
        );
        self.tab_strip_rect = Rect::new(0.0, 0.0, tab_strip.width, TAB_SEG_HEIGHT);

        // The connecting spinner sits at its intrinsic size.
        self.spinner.measure(
            ctx,
            Constraints::new(
                Size::ZERO,
                Size::new(SPINNER_SIZE + 4.0, SPINNER_SIZE + 4.0),
            ),
        );

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

        // Tab strip begins after the brand mark + wordmark.
        let seg_x = bounds.x() + EDGE_PAD + LOGO_SIZE + 12.0 + brand_word_width() + 26.0;
        let seg_y = bounds.y() + (TOPBAR_HEIGHT - TAB_SEG_HEIGHT) / 2.0;
        let seg_w = self
            .tab_strip_rect
            .width()
            .min((bounds.width() - seg_x - EDGE_PAD).max(0.0));
        self.tab_strip_rect = Rect::new(seg_x, seg_y, seg_w, TAB_SEG_HEIGHT);
        self.tabs.arrange(ctx, self.tab_strip_rect);

        // Connecting spinner: just left of where the status pill sits, vertically centered.
        let spinner_x =
            bounds.x() + bounds.width() - EDGE_PAD - status_pill_max_width() - SPINNER_SIZE - 8.0;
        let spinner_y = bounds.y() + (TOPBAR_HEIGHT - SPINNER_SIZE) / 2.0;
        self.spinner.arrange(
            ctx,
            Rect::new(spinner_x, spinner_y, SPINNER_SIZE, SPINNER_SIZE),
        );

        let body_y = bounds.y() + TOPBAR_HEIGHT;
        let body_h = (bounds.height() - TOPBAR_HEIGHT).max(0.0);
        self.body_rect = Rect::new(bounds.x(), body_y, bounds.width(), body_h);
        if let Some(content) = &mut self.content {
            content.arrange(ctx, self.body_rect);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let connecting = !self.shared.is_ready();

        // Content area background, then either the tab content or a loading screen.
        ctx.fill_bounds(pal.window);
        if let Some(content) = &self.content {
            content.paint(ctx);
        } else {
            self.paint_loading(ctx, &theme, &pal);
        }

        // Title-bar elevation: a soft drop shadow under the bar so it separates from the content,
        // drawn BEFORE the bar fill so it sits behind it.
        let bar_radii = [0.0, 0.0, 0.0, 0.0];
        sui::paint_theme_shadow(ctx, self.bar_rect, bar_radii, &titlebar_shadow(&theme));

        // Title bar surface + hairline.
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

        // Tab strip (composed `TabBar`). Only meaningful once the tabs exist; while connecting the
        // content is `None`, but the strip is still shown so the chrome is stable.
        self.tabs.paint(ctx);

        // While connecting, a spinner sits next to the status text; once ready, the status pill.
        if connecting {
            self.spinner.paint(ctx);
        }
        self.paint_status_pill(ctx, &theme, &pal, connecting);
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
        // The composed `TabBar` flips `shared.active_tab` and requests only a repaint, but the
        // content `SwitchView` lays out only its active child — so the newly-shown tab view needs a
        // fresh measure+arrange. Detect the change on the redraw the TabBar triggered and request
        // the relayout (diff-on-redraw, mirroring how the old hand-painted `switch_to` relayouted,
        // and how the inner tab hosts catch nested switches).
        if matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            let current = self.shared.active_tab();
            if current != self.last_tab.get() {
                self.last_tab.set(current);
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
            }
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Window, ctx.bounds());
        node.name = Some("Sinomo".to_string());
        ctx.push(node);

        self.logo.semantics(ctx);
        self.tabs.semantics(ctx);
        if !self.shared.is_ready() {
            self.spinner.semantics(ctx);
        }
        if let Some(content) = &self.content {
            content.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.logo.visit_children(visitor);
        visitor.visit(&self.tabs);
        if !self.shared.is_ready() {
            visitor.visit(&self.spinner);
        }
        if let Some(content) = &self.content {
            visitor.visit(content);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.logo.visit_children_mut(visitor);
        // Forward to the tab strip so pointer / keyboard / focus reach the tabs.
        visitor.visit(&mut self.tabs);
        if !self.shared.is_ready() {
            visitor.visit(&mut self.spinner);
        }
        // CRITICAL: forward to the content pod so events/focus reach the active tab.
        if let Some(content) = &mut self.content {
            visitor.visit(content);
        }
    }
}

impl Shell {
    /// Centered status while the backend connection is resolving: a spinner above the live status
    /// line, for a more polished loading look.
    fn paint_loading(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let status = self.shared.status_text();
        let style = pal.text_style(theme, 15.0, FontWeight::MEDIUM, pal.text_muted);
        let measurement = ctx.measure_text(status.clone(), style.clone()).ok();
        let tw = measurement.as_ref().map(|m| m.width).unwrap_or(160.0);
        let th = measurement.as_ref().map(|m| m.height).unwrap_or(20.0);
        let cx = self.body_rect.x() + self.body_rect.width() / 2.0;
        let cy = self.body_rect.y() + self.body_rect.height() / 2.0;

        // A spinner centered above the status text. The spinner pod is painted by the title-bar
        // path; here we draw a second, larger ring with the same dotted look so the loading screen
        // reads as "working" without arranging a second pod.
        draw_loading_spinner(ctx, pal, cx, cy - 22.0, 22.0);

        let tx = cx - tw / 2.0;
        let ty = cy + 10.0 - th / 2.0;
        widgets::draw_text_in(ctx, Rect::new(tx, ty, tw + 4.0, th + 4.0), &status, style);
    }

    fn paint_status_pill(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        connecting: bool,
    ) {
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

        // While connecting, drop the leading dot (the spinner to the left conveys the activity) and
        // show just the status text in a pill; once ready, the pill has a status dot.
        let pill_w = if connecting {
            tw + 14.0 * 2.0
        } else {
            tw + 16.0 + 14.0
        };
        let pill_h = 24.0;
        let pill_x = self.bar_rect.x() + self.bar_rect.width() - EDGE_PAD - pill_w;
        let pill_y = self.bar_rect.y() + (TOPBAR_HEIGHT - pill_h) / 2.0;
        let pill = Rect::new(pill_x, pill_y, pill_w, pill_h);
        widgets::round_panel(ctx, pill, pal.field, pal.border, pill_h / 2.0);

        if connecting {
            widgets::draw_text_in(
                ctx,
                Rect::new(pill_x + 14.0, pill_y, pill_w - 14.0, pill_h),
                &label,
                style,
            );
        } else {
            // status dot
            let r = 4.0;
            ctx.fill(
                sui::Path::circle(sui::Point::new(pill_x + 12.0, pill_y + pill_h / 2.0), r),
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
}

/// A subtle title-bar drop shadow: a soft outer layer cast downward onto the content so the flat
/// chrome reads as slightly elevated. We use the theme's small drop-shadow token (offset+blur, ~15%
/// black) so the elevation stays on the toolkit's own scale rather than a one-off bespoke value. In
/// dark mode the black shadow is barely perceptible against the dark content, so the bottom hairline
/// (painted alongside) carries the separation there; in light mode the shadow does the lifting.
fn titlebar_shadow(theme: &sui::DefaultTheme) -> ThemeShadow {
    theme.shadows.drop.sm
}

/// The dotted loading ring used on the connecting screen (mirrors the `Spinner` look at a larger
/// size, drawn directly so we don't have to arrange a second pod).
fn draw_loading_spinner(ctx: &mut PaintCtx, pal: &Palette, cx: f32, cy: f32, size: f32) {
    let radius = size * 0.4;
    let dot_radius = (size * 0.09).max(1.5);
    for index in 0..10 {
        let angle = (index as f32 / 10.0) * std::f32::consts::TAU;
        let alpha = 0.22 + ((index as f32) / 10.0) * 0.72;
        let color = sui::Color::rgba(pal.accent.red, pal.accent.green, pal.accent.blue, alpha);
        let dot = sui::Point::new(cx + angle.cos() * radius, cy + angle.sin() * radius);
        ctx.fill(sui::Path::circle(dot, dot_radius), color);
    }
}

/// A roomy upper bound on the status pill width, used to reserve space for the connecting spinner to
/// its left during arrange (the exact pill width is measured at paint time).
fn status_pill_max_width() -> f32 {
    150.0
}

/// Approximate width reserved for the "Sinomo" wordmark (kept simple; the tab strip measures its
/// own natural width to the right of it).
fn brand_word_width() -> f32 {
    58.0
}
