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
//! active tab through `shared.active_tab`, reads the same state back through `selected_when`, and
//! the Shell detects that change on the next redraw (diff-on-redraw) to request the same relayout
//! the old hand-painted strip did.

use std::cell::Cell;

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, Image, ImageFit, ImageHandle,
    Insets, MeasureCtx, PaintCtx, PointerButton, PointerEventKind, Rect, SemanticsAction,
    SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, SingleChild, Size, Spinner,
    SwitchView, TabBar, ThemeShadow, TimerToken, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor, WindowEvent, WindowRenderOptions,
};

use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::{chat, cluster, evolution, files, settings, widgets};

pub const TOPBAR_HEIGHT: f32 = 44.0;
const LOGO_SIZE: f32 = 24.0;
const EDGE_PAD: f32 = 18.0;
const TAB_SEG_HEIGHT: f32 = 32.0;
const TAB_GAP: f32 = 6.0;
const COMPACT_TAB_GAP: f32 = 2.0;
const STATUS_GAP: f32 = 12.0;
const STATUS_PILL_H: f32 = 24.0;
const STATUS_PILL_W: f32 = 150.0;
const COMPACT_CHROME_WIDTH: f32 = 620.0;
const COMPACT_STATUS_PILL_W: f32 = 24.0;
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
    /// `on_change`; the Shell relayouts the content on the change (see `event`). The widget
    /// instance is preserved across tab switches so the stock TabBar owns switch animation.
    tabs: WidgetPod,
    /// A spinner shown next to the status text while the backend connection resolves.
    spinner: WidgetPod,
    /// The switchable tab content — built only once the backend connection resolves. `None`
    /// while connecting, when a loading screen is shown in its place.
    content: Option<WidgetPod>,
    /// The active tab last laid out. The `TabBar` flips `shared.active_tab` and requests only a
    /// repaint, but the content `SwitchView` only measures/arranges its *active* child — so a tab
    /// switch needs a fresh measure+arrange for the newly-shown view. We diff on `RedrawRequested`
    /// to catch it while preserving the TabBar instance and its in-flight selector animation.
    last_tab: Cell<Tab>,
    compact_tabs: Cell<bool>,
    last_render_options: Cell<Option<WindowRenderOptions>>,
    bar_rect: Rect,
    tab_strip_rect: Rect,
    status_pill_rect: Cell<Rect>,
    status_pill_pressed: Cell<bool>,
    chat_session_timer: Cell<Option<TimerToken>>,
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

        let tabs = WidgetPod::new(build_tab_bar(&shared, false));
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
            compact_tabs: Cell::new(false),
            last_render_options: Cell::new(None),
            bar_rect: Rect::ZERO,
            tab_strip_rect: Rect::ZERO,
            status_pill_rect: Cell::new(Rect::ZERO),
            status_pill_pressed: Cell::new(false),
            chat_session_timer: Cell::new(None),
            body_rect: Rect::ZERO,
        };
        // Built already-ready (headless tooling / visual tests) → show the tabs immediately. The
        // interactive app builds content when the connection resolves (see `event`).
        if shell.shared.is_ready() {
            shell.build_content();
            let waker = { shell.shared.borrow().waker.clone() };
            shell.shared.chat_mut().start_session_list_load(waker);
        }
        shell
    }

    /// Build the switchable tab content. Called once the backend is ready.
    fn build_content(&mut self) {
        let content = SwitchView::new()
            .with_child(chat::view(self.shared.clone()))
            .with_child(evolution::view(self.shared.clone()))
            .with_child(cluster::view(self.shared.clone()))
            .with_child(files::view(self.shared.clone()))
            .with_child(settings::view(self.shared.clone()))
            .selected_when({
                let shared = self.shared.clone();
                move || shared.active_tab().index()
            });
        self.content = Some(WidgetPod::new(content));
    }

    fn schedule_chat_session_timer(&self, ctx: &mut EventCtx) {
        if self.chat_session_timer.get().is_some() || !self.shared.is_ready() {
            return;
        }
        if self.shared.chat().session_list_is_loading() {
            let token = ctx.schedule_timer_after(1.0);
            self.chat_session_timer.set(Some(token));
        }
    }

    fn drain_chat_session_timer(&self, ctx: &mut EventCtx) -> bool {
        if !self.shared.is_ready() {
            return false;
        }
        let changed = self.shared.chat_mut().pump_session_list();
        self.schedule_chat_session_timer(ctx);
        changed
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let dark = widgets::theme_is_dark(&self.shared);
        let pal = Palette::from_theme(&theme, dark);
        (theme, pal)
    }

    fn sync_render_options(&self, ctx: &mut EventCtx) {
        let options = self.shared.render_options();
        if self.last_render_options.get() == Some(options) {
            return;
        }
        sui::set_window_render_options(ctx.window_id(), options);
        self.last_render_options.set(Some(options));
        ctx.request_paint();
    }

    fn sync_tab_bar_density(&mut self, compact: bool) {
        if self.compact_tabs.get() == compact {
            return;
        }
        self.compact_tabs.set(compact);
        self.tabs = WidgetPod::new(build_tab_bar(&self.shared, compact));
    }

    fn sync_active_tab(&mut self, ctx: &mut EventCtx) {
        let current = self.shared.active_tab();
        if current == self.last_tab.get() {
            return;
        }
        self.last_tab.set(current);
        self.start_cluster_refresh_if_active();
        ctx.request_measure();
        ctx.request_arrange();
        ctx.request_semantics();
        ctx.request_paint();
    }

    fn start_cluster_refresh_if_active(&self) -> bool {
        if !self.shared.is_ready() || self.shared.active_tab() != Tab::Cluster {
            return false;
        }
        cluster::request_refresh(&self.shared)
    }

    fn pump_cluster_refresh(&self) -> bool {
        cluster::pump_refresh(&self.shared)
    }

    fn open_connection_settings(&self) {
        let mut app = self.shared.borrow_mut();
        app.active_tab = Tab::Settings;
        if let Some(ready) = app.ready.as_mut() {
            ready.settings.open(settings::SettingsSection::Connection);
        }
    }
}

/// Build the tab strip `TabBar`, bound to the active tab and writing the selection back to
/// `shared.active_tab` on change. Threads the theme reader so it restyles on theme switch.
fn build_tab_bar(shared: &Shared, compact: bool) -> TabBar {
    let labels = Tab::ALL.iter().map(|tab| tab_label(*tab, compact));
    let on_change = {
        let s = shared.clone();
        move |index: usize, _label: String| {
            if let Some(tab) = Tab::ALL.get(index).copied() {
                s.borrow_mut().active_tab = tab;
            }
        }
    };
    let selected = {
        let s = shared.clone();
        move || Some(s.active_tab().index())
    };
    TabBar::new("shell-tabs")
        .tabs(labels)
        .selected_when(selected)
        .gap(if compact { COMPACT_TAB_GAP } else { TAB_GAP })
        .theme_when(tab_bar_theme(shared, compact))
        .on_change(on_change)
}

fn tab_bar_theme(shared: &Shared, compact: bool) -> impl Fn() -> sui::DefaultTheme + 'static {
    let s = shared.clone();
    move || {
        let mut theme = s.theme();
        if compact {
            theme.metrics.tab_min_width = 54.0;
            theme.metrics.tab_padding = Insets {
                left: 10.0,
                top: 4.0,
                right: 10.0,
                bottom: 4.0,
            };
            theme.metrics.tab_gap = COMPACT_TAB_GAP;
        }
        theme
    }
}

fn tab_label(tab: Tab, compact: bool) -> &'static str {
    if compact {
        match tab {
            Tab::Sinomo => "Chat",
            Tab::Evolution => "Evolve",
            Tab::Cluster => Tab::Cluster.title(),
            Tab::Files => Tab::Files.title(),
            Tab::Settings => Tab::Settings.title(),
        }
    } else {
        tab.title()
    }
}

impl Widget for Shell {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;
        self.sync_tab_bar_density(compact_chrome(size.width));

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
        let compact = compact_chrome(bounds.width());
        let show_spinner = !self.shared.is_ready() && !compact;

        let logo_y = bounds.y() + (TOPBAR_HEIGHT - LOGO_SIZE) / 2.0;
        self.logo.arrange(
            ctx,
            Rect::new(bounds.x() + EDGE_PAD, logo_y, LOGO_SIZE, LOGO_SIZE),
        );

        // Tab strip begins after the brand mark + wordmark.
        let seg_x = tab_strip_x(bounds.x(), compact);
        let seg_y = bounds.y() + (TOPBAR_HEIGHT - TAB_SEG_HEIGHT) / 2.0;
        let status_rect = status_pill_arrange_rect(bounds, compact);
        let status_x = status_rect.x();
        let tab_right = if compact {
            bounds.x() + bounds.width()
        } else {
            let spinner_w = if show_spinner {
                SPINNER_SIZE + 8.0
            } else {
                0.0
            };
            status_x - STATUS_GAP - spinner_w
        };
        let seg_w = self
            .tab_strip_rect
            .width()
            .min((tab_right - seg_x).max(0.0));
        self.tab_strip_rect = Rect::new(seg_x, seg_y, seg_w, TAB_SEG_HEIGHT);
        self.tabs.arrange(ctx, self.tab_strip_rect);

        // Connecting spinner: just left of where the status pill sits, vertically centered.
        let spinner_y = bounds.y() + (TOPBAR_HEIGHT - SPINNER_SIZE) / 2.0;
        let spinner_x = if show_spinner {
            status_x - SPINNER_SIZE - 8.0
        } else {
            status_x
        };
        self.spinner.arrange(
            ctx,
            Rect::new(
                spinner_x,
                spinner_y,
                if show_spinner { SPINNER_SIZE } else { 0.0 },
                if show_spinner { SPINNER_SIZE } else { 0.0 },
            ),
        );
        self.status_pill_rect.set(Rect::new(
            status_rect.x(),
            status_rect.y(),
            status_rect.width(),
            status_rect.height(),
        ));

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

        if !compact_chrome(self.bar_rect.width()) {
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
        }

        // Tab strip (composed `TabBar`). Only meaningful once the tabs exist; while connecting the
        // content is `None`, but the strip is still shown so the chrome is stable.
        self.tabs.paint(ctx);

        // While connecting, a spinner sits next to the status text; once ready, the status pill.
        if connecting && !compact_chrome(self.bar_rect.width()) {
            self.spinner.paint(ctx);
        }
        self.paint_status_pill(ctx, &theme, &pal, connecting);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary)
                    && self.shared.is_ready()
                    && self.status_pill_rect.get().contains(pointer.position) =>
            {
                self.status_pill_pressed.set(true);
                ctx.request_paint();
                ctx.request_semantics();
                ctx.request_pointer_capture(pointer.pointer_id);
                ctx.set_handled();
                return;
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && self.status_pill_pressed.get() =>
            {
                let activate = self.status_pill_rect.get().contains(pointer.position);
                self.status_pill_pressed.set(false);
                ctx.release_pointer_capture(pointer.pointer_id);
                if activate {
                    self.open_connection_settings();
                    self.sync_active_tab(ctx);
                }
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
                ctx.set_handled();
                return;
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Cancel | PointerEventKind::Leave
                ) && self.status_pill_pressed.get() =>
            {
                self.status_pill_pressed.set(false);
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
                return;
            }
            _ => {}
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.chat_session_timer.get() == Some(*token)
        {
            self.chat_session_timer.set(None);
            if self.drain_chat_session_timer(ctx) {
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
            }
            return;
        }
        // The background connect thread wakes us via the sui external-wake mechanism; drain its
        // progress/result here. When the backend arrives, build the tab content and relayout.
        if let Event::Custom(custom) = event {
            if custom.kind.as_str() == sui::EXTERNAL_WAKE_KIND {
                let (changed, became_ready) = self.shared.pump_connect();
                if became_ready {
                    self.build_content();
                    let waker = { self.shared.borrow().waker.clone() };
                    self.shared.chat_mut().start_session_list_load(waker);
                    self.schedule_chat_session_timer(ctx);
                    self.start_cluster_refresh_if_active();
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_semantics();
                }
                // Drain any in-progress provider device-login (Settings tab).
                let device_changed = if self.shared.is_ready() {
                    self.shared.settings_mut().pump_device_login()
                } else {
                    false
                };
                let files_changed = if self.shared.is_ready() {
                    self.shared.files_mut().pump_load()
                } else {
                    false
                };
                let cluster_changed = self.pump_cluster_refresh();
                let chat_changed = if self.shared.is_ready() {
                    let mut chat = self.shared.chat_mut();
                    let worker_changed = chat.pump_worker_events();
                    let list_changed = chat.pump_session_list();
                    let delete_changed = chat.pump_delete();
                    worker_changed || list_changed || delete_changed
                } else {
                    false
                };
                self.schedule_chat_session_timer(ctx);
                if files_changed || cluster_changed || chat_changed {
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_semantics();
                }
                if changed || device_changed || files_changed || cluster_changed || chat_changed {
                    ctx.request_paint();
                }
                self.sync_active_tab(ctx);
                self.sync_render_options(ctx);
            }
            return;
        }
        // The composed `TabBar` flips `shared.active_tab` and requests only a repaint, but the
        // content `SwitchView` lays out only its active child — so the newly-shown tab view needs a
        // fresh measure+arrange. Detect the change on the redraw the TabBar triggered and request
        // the relayout while preserving the TabBar instance and its selector animation.
        if matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            let cluster_changed = self.pump_cluster_refresh();
            let chat_changed = if self.shared.is_ready() {
                self.shared.chat_mut().pump_session_list()
            } else {
                false
            };
            self.schedule_chat_session_timer(ctx);
            if cluster_changed || chat_changed {
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
            }
            self.sync_active_tab(ctx);
            self.sync_render_options(ctx);
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
        let status = shell_status(&self.shared);
        let ready = self.shared.is_ready();
        let mut status_node = SemanticsNode::new(
            sui::WidgetId::new(ctx.widget_id().get().wrapping_mul(31).wrapping_add(1_001)),
            if ready {
                SemanticsRole::Button
            } else {
                SemanticsRole::Text
            },
            self.status_pill_rect.get(),
        );
        status_node.name = Some(status.label);
        status_node.description = Some(status.detail);
        status_node.value = Some(SemanticsValue::Text(if status.degraded {
            "degraded".to_string()
        } else if ready {
            "connected".to_string()
        } else {
            "connecting".to_string()
        }));
        if ready {
            status_node.actions = vec![SemanticsAction::Activate];
        }
        status_node.state.selected = status.degraded;
        ctx.push(status_node);
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
        let status = shell_status(&self.shared);
        let label = status.label;
        let dot = if status.degraded {
            pal.warn
        } else if status.connected_daemon {
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
        let compact = compact_chrome(self.bar_rect.width());
        let pill_w = if connecting {
            tw + 14.0 * 2.0
        } else {
            tw + 16.0 + 14.0
        };
        let pill_h = STATUS_PILL_H;
        let (pill_x, pill_y, pill_w, pill_h) = if compact {
            let rect = self.status_pill_rect.get();
            (rect.x(), rect.y(), rect.width(), rect.height())
        } else {
            (
                self.bar_rect.x() + self.bar_rect.width() - EDGE_PAD - pill_w,
                self.bar_rect.y() + (TOPBAR_HEIGHT - pill_h) / 2.0,
                pill_w,
                pill_h,
            )
        };
        let pill = Rect::new(pill_x, pill_y, pill_w, pill_h);
        self.status_pill_rect.set(pill);
        let pressed = self.status_pill_pressed.get();
        let bg = if pressed {
            pal.hover
        } else if status.degraded {
            pal.field
        } else {
            pal.field
        };
        widgets::round_panel(
            ctx,
            pill,
            bg,
            if status.degraded {
                pal.warn
            } else {
                pal.border
            },
            pill_h / 2.0,
        );

        if compact {
            let r = 4.0;
            ctx.fill(
                sui::Path::circle(
                    sui::Point::new(pill_x + pill_w / 2.0, pill_y + pill_h / 2.0),
                    r,
                ),
                dot,
            );
        } else if connecting {
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

fn compact_chrome(width: f32) -> bool {
    width < COMPACT_CHROME_WIDTH
}

fn tab_strip_x(bounds_x: f32, compact: bool) -> f32 {
    if compact {
        bounds_x + EDGE_PAD + LOGO_SIZE + 16.0
    } else {
        bounds_x + EDGE_PAD + LOGO_SIZE + 12.0 + brand_word_width() + 26.0
    }
}

/// Position reserved for the status affordance during arrange. On compact title bars the full status
/// text remains in semantics while the visual treatment becomes a small logo badge, keeping the tab
/// strip free to use the remaining title-bar width.
fn status_pill_arrange_rect(bounds: Rect, compact: bool) -> Rect {
    if compact {
        Rect::new(
            bounds.x() + EDGE_PAD + LOGO_SIZE - COMPACT_STATUS_PILL_W + 15.0,
            bounds.y() + (TOPBAR_HEIGHT - COMPACT_STATUS_PILL_W) / 2.0,
            COMPACT_STATUS_PILL_W,
            COMPACT_STATUS_PILL_W,
        )
    } else {
        Rect::new(
            bounds.x() + bounds.width() - EDGE_PAD - STATUS_PILL_W,
            bounds.y() + (TOPBAR_HEIGHT - STATUS_PILL_H) / 2.0,
            STATUS_PILL_W,
            STATUS_PILL_H,
        )
    }
}

/// Approximate width reserved for the "Sinomo" wordmark (kept simple; the tab strip measures its
/// own natural width to the right of it).
fn brand_word_width() -> f32 {
    58.0
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellStatus {
    label: String,
    detail: String,
    degraded: bool,
    connected_daemon: bool,
}

fn shell_status(shared: &Shared) -> ShellStatus {
    let state = shared.borrow();
    let Some(ready) = state.ready.as_ref() else {
        let status = state.status.clone();
        return ShellStatus {
            label: status.clone(),
            detail: status,
            degraded: false,
            connected_daemon: false,
        };
    };

    let mut issues = Vec::new();
    if let Some(detail) = ready.chat.connection_attention_detail() {
        issues.push(detail);
    }
    if let Some(error) = ready.files.error.as_deref().filter(|text| !text.is_empty()) {
        issues.push(format!("SIFS unavailable: {error}"));
    }

    if issues.is_empty() {
        ShellStatus {
            label: ready.backend.mode_label(),
            detail: ready.backend.status_detail(),
            degraded: false,
            connected_daemon: ready.backend.connection.is_daemon(),
        }
    } else {
        let count = issues.len();
        ShellStatus {
            label: if count == 1 {
                "Needs attention".to_string()
            } else {
                format!("{count} issues")
            },
            detail: if count == 1 {
                issues.join(" · ")
            } else {
                format!(
                    "{count} app surfaces need attention: {}",
                    issues.join(" · ")
                )
            },
            degraded: true,
            connected_daemon: ready.backend.connection.is_daemon(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::AppState;
    use crate::ui::chat::{ActivityTone, ChatActivityEntry};
    use crate::ui::settings::SettingsSection;
    use sinomo_agent::{
        AGENT_CHANNEL_APP_USER_ID, AgentChannel, AgentChannelSource, AgentChannelStore,
    };
    use sinomo_cli::app_api::SifsAgentChannelStore;
    use sinomo_runtime::{events::SessionId, local::SinomoDataDir};
    use std::fs;

    #[test]
    fn product_wordmark_and_chat_tab_use_distinct_labels() {
        assert_eq!(Tab::Sinomo.title(), "Chat");
        assert_eq!(tab_label(Tab::Sinomo, false), "Chat");
        assert_eq!(tab_label(Tab::Sinomo, true), "Chat");
    }

    #[test]
    fn already_ready_shell_starts_chat_history_load() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-app-ready-shell-history-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let data_dir = SinomoDataDir::new(&root);
        let channel_id = "01hxy6qa9k0t9vr4g9n8d3q6a0"
            .parse::<SessionId>()
            .expect("session id");
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(channel_id.clone(), "Ready shell history");
        let message = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "hello",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel.channel_id, &message)
            .expect("append backend message");
        store.save_channel(&channel).expect("save backend chat");
        let session_id = channel_id.to_string();

        let backend = Backend::local_fallback_for_data_dir(data_dir);
        let state = AppState::ready(backend);
        let shared = Shared::new(state);
        shared.chat_mut().sessions.clear();

        let _shell = Shell::new(shared.clone());

        assert!(
            shared
                .chat()
                .sessions
                .iter()
                .any(|session| session.id == session_id && session.title == "Ready shell history"),
            "already-ready shells should kick the chat history loader"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn programmatic_settings_navigation_updates_tab_bar_value() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Files;
        let shared = Shared::new(state);
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
            files.entries.clear();
        }

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        let snapshot = window.snapshot()?;
        let tab_bar = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("shell tab bar semantics");
        assert_eq!(
            tab_bar.value,
            Some(sui::SemanticsValue::Text("Settings".to_string()))
        );

        Ok(())
    }

    #[test]
    fn shell_tabbar_switch_preserves_widget_animation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Sinomo;
        let shared = Shared::new(state);
        let mut runtime = crate::build_runtime(shared.clone())?;
        let window_id = runtime.window_ids()[0];
        let switch_duration = shared.theme().motion.tab_switch_duration();

        let output = runtime.render(window_id)?;
        let tab_bar = output
            .semantics
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("shell tab bar semantics");
        let settings_point = sui::Point::new(
            tab_bar.bounds.max_x() - 42.0,
            tab_bar.bounds.y() + tab_bar.bounds.height() / 2.0,
        );
        click_runtime_point(&mut runtime, window_id, settings_point)?;
        assert_eq!(shared.borrow().active_tab, Tab::Settings);

        runtime.handle_event(window_id, Event::Window(WindowEvent::RedrawRequested))?;
        runtime.tick(switch_duration * 0.5);
        assert!(
            dispatch_ready_events(&mut runtime)? >= 1,
            "tab switch should schedule animation frames after shell redraw"
        );
        assert!(
            runtime.next_wakeup_time(window_id)?.is_some(),
            "tab switch should still be animating at half duration"
        );

        runtime.tick(switch_duration);
        dispatch_ready_events(&mut runtime)?;
        let output = runtime.render(window_id)?;
        let tab_bar = output
            .semantics
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("shell tab bar semantics");
        assert_eq!(
            tab_bar.value,
            Some(SemanticsValue::Text("Settings".to_string()))
        );

        Ok(())
    }

    fn click_runtime_point(
        runtime: &mut sui::Runtime,
        window_id: sui::WindowId,
        point: sui::Point,
    ) -> sui::Result<()> {
        runtime.handle_event(
            window_id,
            Event::Pointer(sui::PointerEvent::new(PointerEventKind::Move, point)),
        )?;

        let mut down = sui::PointerEvent::new(PointerEventKind::Down, point);
        down.button = Some(PointerButton::Primary);
        down.buttons = sui::PointerButtons::new(1);
        runtime.handle_event(window_id, Event::Pointer(down))?;

        let mut up = sui::PointerEvent::new(PointerEventKind::Up, point);
        up.button = Some(PointerButton::Primary);
        runtime.handle_event(window_id, Event::Pointer(up))
    }

    fn dispatch_ready_events(runtime: &mut sui::Runtime) -> sui::Result<usize> {
        let ready = runtime.drain_ready_events();
        let count = ready.len();
        for (window_id, event) in ready {
            runtime.handle_event(window_id, event)?;
        }
        Ok(count)
    }

    #[test]
    fn attention_status_pill_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Sinomo;
        let shared = Shared::new(state);
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
        }

        assert_eq!(shell_status(&shared).label, "Needs attention");

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Needs attention")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        let snapshot = window.snapshot()?;
        let tab_bar = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("shell tab bar semantics");
        assert_eq!(
            tab_bar.value,
            Some(sui::SemanticsValue::Text("Settings".to_string()))
        );

        Ok(())
    }

    #[test]
    fn compact_titlebar_keeps_attention_pill_out_of_tabs() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Settings;
        let shared = Shared::new(state);
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
        }

        let runtime = crate::build_runtime(shared)?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let root = window.root();
        app.run_until_idle()?;

        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        let tab_bar = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("shell tab bar semantics");
        let status = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Needs attention")
            })
            .expect("attention status semantics");

        assert!(
            status.bounds.max_x() <= tab_bar.bounds.x(),
            "compact status badge {:?} should stay before tab bar {:?}",
            status.bounds,
            tab_bar.bounds
        );
        assert!(
            tab_bar.bounds.max_x() <= 520.0 + 0.5,
            "tab bar should use the remaining title-bar width without running under right chrome: {:?}",
            tab_bar.bounds
        );
        assert!(
            tab_bar.bounds.width() <= 390.0,
            "compact tab labels should fit at their natural width instead of being clipped to the viewport: {:?}",
            tab_bar.bounds
        );
        assert!(
            status.bounds.width() <= COMPACT_STATUS_PILL_W + 0.5,
            "compact status should not reserve the full pill width: {:?}",
            status.bounds
        );

        Ok(())
    }

    #[test]
    fn attention_status_pill_tracks_chat_activity_errors() {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Sinomo;
        let shared = Shared::new(state);
        {
            let mut chat = shared.chat_mut();
            chat.activity.push(ChatActivityEntry {
                title: "agent error".to_string(),
                detail: "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied".to_string(),
                tone: ActivityTone::Bad,
            });
        }

        let status = shell_status(&shared);
        assert_eq!(status.label, "Needs attention");
        assert!(status.detail.contains("SIFS store failed"));
    }

    #[test]
    fn attention_status_pill_counts_multiple_surface_issues() {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Sinomo;
        let shared = Shared::new(state);
        {
            let mut chat = shared.chat_mut();
            chat.activity.push(ChatActivityEntry {
                title: "chat list".to_string(),
                detail: "chat unavailable: device authorization was denied".to_string(),
                tone: ActivityTone::Bad,
            });
        }
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
        }

        let status = shell_status(&shared);
        assert_eq!(status.label, "2 issues");
        assert!(
            status.detail.contains("2 app surfaces need attention"),
            "detail should summarize the surface count: {}",
            status.detail
        );
        assert!(status.detail.contains("chat unavailable"));
        assert!(status.detail.contains("SIFS unavailable"));
    }
}
