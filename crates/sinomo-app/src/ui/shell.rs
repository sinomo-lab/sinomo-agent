//! The application shell: a title bar (brand mark, tab strip, status pill) above
//! a switchable content area.
//!
//! The chrome is *composed* from `sui-widgets` where the toolkit has a stock control
//! — the tab strip is a [`sui::TabBar`] child pod, and the connecting indicator is a
//! [`sui::Spinner`] child pod. The brand mark (logo [`Image`] + "Sinomo" wordmark) and
//! the status pill stay as light custom paint so they can sit precisely in the title bar.
//!
//! Launch and runtime work stays asynchronous: the connecting-to-ready state machine builds tab
//! content only after the backend resolves, while external wakes drain typed worker results. After
//! launch, a short background-probe lane keeps daemon status live. The title-bar health control is
//! a focusable widget with an accessible recovery popover; it never performs daemon or storage work
//! on the UI thread.

use std::cell::Cell;

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, Image, ImageFit, ImageHandle,
    Insets, KeyState, MeasureCtx, PaintCtx, Point, PointerButton, PointerEventKind, Rect,
    SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, SingleChild, Size,
    Spinner, SwitchView, TabBar, ThemeShadow, TimerToken, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor, WindowEvent, WindowRenderOptions,
};

use crate::state::{ConnectionPhase, HealthIssue, HealthSurface, Shared, Tab};
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
const HEALTH_POPOVER_W: f32 = 420.0;
const HEALTH_POPOVER_GAP: f32 = 8.0;
const HEALTH_HEADER_H: f32 = 54.0;
const HEALTH_ISSUE_ROW_H: f32 = 96.0;
const HEALTH_EMPTY_ROW_H: f32 = 66.0;
const HEALTH_FOOTER_H: f32 = 42.0;
const HEALTH_ACTION_H: f32 = 26.0;
const HEALTH_ACTION_GAP: f32 = 6.0;
const HEALTH_PROBE_INTERVAL_SECS: f64 = 5.0;
/// Spinner shown in the connecting indicator / loading screen.
const SPINNER_SIZE: f32 = 18.0;
/// Loading screen: show the per-step elapsed time once a launch step has been in
/// flight this long, and point at the app log a bit later. Keeps a stalled boot
/// (e.g. behind distributed-file-system timeouts) readable instead of frozen.
const LOADING_STALL_AFTER_SECS: u64 = 5;
const LOADING_LOG_HINT_AFTER_SECS: u64 = 10;

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
    /// Focusable status trigger plus the unified health/recovery popover. This is a real child
    /// widget (rather than hand-painted shell chrome), so keyboard focus and UI Automation can
    /// reach the trigger, issue rows, and recovery actions.
    health_center: WidgetPod,
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
    chat_session_timer: Cell<Option<TimerToken>>,
    /// Repaint driver for the loading screen: keeps the per-step elapsed time live
    /// while the backend connection resolves, so a stalled launch step is visible.
    connect_status_timer: Cell<Option<TimerToken>>,
    /// Periodic live daemon observation; the probe itself always runs on a background thread.
    health_probe_timer: Cell<Option<TimerToken>>,
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
        let health_center = WidgetPod::new(HealthCenter::new(shared.clone(), false));

        let initial_tab = shared.active_tab();
        let mut shell = Self {
            shared,
            logo,
            tabs,
            spinner,
            health_center,
            content: None,
            last_tab: Cell::new(initial_tab),
            compact_tabs: Cell::new(false),
            last_render_options: Cell::new(None),
            bar_rect: Rect::ZERO,
            tab_strip_rect: Rect::ZERO,
            chat_session_timer: Cell::new(None),
            connect_status_timer: Cell::new(None),
            health_probe_timer: Cell::new(None),
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
        if self.chat_session_timer.get().is_some()
            || !self.shared.is_ready()
            || self.shared.borrow().waker.is_none()
        {
            return;
        }
        let token = ctx.schedule_timer_after(chat::MULTI_DEVICE_SYNC_INTERVAL_SECS);
        self.chat_session_timer.set(Some(token));
    }

    fn drain_chat_session_timer(&self, ctx: &mut EventCtx) -> bool {
        if !self.shared.is_ready() {
            return false;
        }
        let mut chat = self.shared.chat_mut();
        chat.begin_multi_device_sync();
        let changed = chat.pump_session_list() || chat.pump_transcript_load();
        drop(chat);
        self.schedule_chat_session_timer(ctx);
        changed
    }

    fn schedule_connect_status_timer(&self, ctx: &mut EventCtx) {
        if self.shared.is_ready() || self.connect_status_timer.get().is_some() {
            return;
        }
        let token = ctx.schedule_timer_after(1.0);
        self.connect_status_timer.set(Some(token));
    }

    fn schedule_health_probe_timer(&self, ctx: &mut EventCtx) {
        if !self.shared.is_ready()
            || self.health_probe_timer.get().is_some()
            || self.shared.borrow().waker.is_none()
        {
            return;
        }
        self.health_probe_timer
            .set(Some(ctx.schedule_timer_after(HEALTH_PROBE_INTERVAL_SECS)));
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
        self.health_center = WidgetPod::new(HealthCenter::new(self.shared.clone(), compact));
    }

    fn sync_active_tab(&mut self, ctx: &mut EventCtx) {
        let current = self.shared.active_tab();
        if current == self.last_tab.get() {
            return;
        }
        self.last_tab.set(current);
        if self.shared.is_ready() {
            self.shared
                .chat_mut()
                .sync_media_setup_visibility(current == Tab::Sinomo);
        }
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
        self.health_center.measure(
            ctx,
            Constraints::new(
                Size::new(COMPACT_STATUS_PILL_W, STATUS_PILL_H),
                Size::new(
                    (size.width - EDGE_PAD * 2.0).max(0.0),
                    (size.height - TOPBAR_HEIGHT / 2.0).max(0.0),
                ),
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
        let health_measured = self.health_center.measured_size();
        let health_x = if compact {
            status_rect.x()
        } else {
            bounds.max_x() - EDGE_PAD - health_measured.width
        };
        let available_w = (bounds.max_x() - EDGE_PAD - health_x).max(0.0);
        self.health_center.arrange(
            ctx,
            Rect::new(
                health_x,
                status_rect.y(),
                health_measured.width.min(available_w),
                health_measured.height,
            ),
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
        self.health_center.paint(ctx);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
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
        // Loading-screen heartbeat: repaint once a second while connecting so the
        // per-step elapsed time stays live (and a stalled step becomes obvious).
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.connect_status_timer.get() == Some(*token)
        {
            self.connect_status_timer.set(None);
            if !self.shared.is_ready() {
                ctx.request_paint();
                self.schedule_connect_status_timer(ctx);
            }
            return;
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.health_probe_timer.get() == Some(*token)
        {
            self.health_probe_timer.set(None);
            if self.shared.request_connection_probe(false) {
                ctx.request_semantics();
            }
            self.schedule_health_probe_timer(ctx);
            return;
        }
        // The background connect thread wakes us via the sui external-wake mechanism; drain its
        // progress/result here. When the backend arrives, build the tab content and relayout.
        if let Event::Custom(custom) = event {
            if custom.kind.as_str() == sui::EXTERNAL_WAKE_KIND {
                self.schedule_connect_status_timer(ctx);
                let (changed, became_ready) = self.shared.pump_connect();
                if became_ready {
                    self.build_content();
                    let (backend, waker) = {
                        let app = self.shared.borrow();
                        let ready = app.ready.as_ref().expect("ready after connect");
                        (ready.backend.clone(), app.waker.clone())
                    };
                    self.shared
                        .settings_mut()
                        .begin_initial_load(backend.clone(), waker.clone());
                    self.shared
                        .evolution_mut()
                        .begin_dashboard_load(backend.data_dir.clone(), waker.clone());
                    {
                        let mut chat = self.shared.chat_mut();
                        chat.start_provider_list_load(backend.config.clone(), waker.clone());
                        chat.start_agent_list_load(backend.data_dir.clone(), waker.clone());
                        chat.start_session_list_load(waker);
                    }
                    self.schedule_chat_session_timer(ctx);
                    self.schedule_health_probe_timer(ctx);
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
                let settings_changed = if self.shared.is_ready() {
                    self.shared.settings_mut().pump_initial_load()
                } else {
                    false
                };
                let settings_operations_changed = if self.shared.is_ready() {
                    settings::pump_background_operations(&self.shared)
                } else {
                    false
                };
                let evolution_changed = if self.shared.is_ready() {
                    self.shared.evolution_mut().pump_dashboard_load()
                } else {
                    false
                };
                let files_changed = if self.shared.is_ready() {
                    self.shared.files_mut().pump_load()
                } else {
                    false
                };
                let connection_changed = self.shared.pump_connection_probe();
                let cluster_changed = self.pump_cluster_refresh();
                let chat_visible = self.shared.active_tab() == Tab::Sinomo;
                let (chat_changed, chat_semantics_changed, chat_layout_changed) =
                    if self.shared.is_ready() {
                        let mut chat = self.shared.chat_mut();
                        let worker_changed = chat.pump_worker_events();
                        let providers_changed = chat.pump_provider_list();
                        let agents_changed = chat.pump_agent_list();
                        let list_changed = chat.pump_session_list();
                        let delete_changed = chat.pump_delete();
                        let media_changed = chat.pump_media_call_runtime();
                        let media_visibility_changed = if chat_visible {
                            false
                        } else {
                            chat.sync_media_setup_visibility(false)
                        };
                        let structural_changed = worker_changed
                            || providers_changed
                            || agents_changed
                            || list_changed
                            || delete_changed
                            || media_visibility_changed;
                        (
                            structural_changed || media_changed.paint,
                            structural_changed || media_changed.semantics,
                            structural_changed || media_changed.layout,
                        )
                    } else {
                        (false, false, false)
                    };
                self.schedule_chat_session_timer(ctx);
                if device_changed
                    || files_changed
                    || connection_changed
                    || cluster_changed
                    || chat_layout_changed
                    || device_changed
                    || settings_changed
                    || settings_operations_changed
                    || evolution_changed
                {
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_semantics();
                } else if chat_semantics_changed {
                    ctx.request_semantics();
                }
                if changed
                    || connection_changed
                    || device_changed
                    || settings_changed
                    || settings_operations_changed
                    || evolution_changed
                    || files_changed
                    || cluster_changed
                    || chat_changed
                {
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
            self.schedule_connect_status_timer(ctx);
            self.schedule_health_probe_timer(ctx);
            let connection_changed = self.shared.pump_connection_probe();
            let cluster_changed = self.pump_cluster_refresh();
            let chat_visible = self.shared.active_tab() == Tab::Sinomo;
            let (chat_changed, chat_semantics_changed, chat_layout_changed) = if self
                .shared
                .is_ready()
            {
                let mut chat = self.shared.chat_mut();
                let providers_changed = chat.pump_provider_list();
                let agents_changed = chat.pump_agent_list();
                let list_changed = chat.pump_session_list();
                let media_changed = chat.pump_media_call_runtime();
                let media_visibility_changed = if chat_visible {
                    false
                } else {
                    chat.sync_media_setup_visibility(false)
                };
                let structural_changed =
                    providers_changed || agents_changed || list_changed || media_visibility_changed;
                (
                    structural_changed || media_changed.paint,
                    structural_changed || media_changed.semantics,
                    structural_changed || media_changed.layout,
                )
            } else {
                (false, false, false)
            };
            let settings_changed = if self.shared.is_ready() {
                self.shared.settings_mut().pump_initial_load()
            } else {
                false
            };
            let settings_operations_changed = if self.shared.is_ready() {
                settings::pump_background_operations(&self.shared)
            } else {
                false
            };
            let evolution_changed = if self.shared.is_ready() {
                self.shared.evolution_mut().pump_dashboard_load()
            } else {
                false
            };
            self.schedule_chat_session_timer(ctx);
            if connection_changed
                || cluster_changed
                || chat_layout_changed
                || settings_changed
                || settings_operations_changed
                || evolution_changed
            {
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
            } else {
                if chat_semantics_changed {
                    ctx.request_semantics();
                }
                if chat_changed {
                    ctx.request_paint();
                }
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
        self.health_center.semantics(ctx);
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
        // Health is painted last and must also be hit-tested last so its open popover receives
        // actions instead of the tab content visually underneath it.
        visitor.visit(&self.health_center);
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
        visitor.visit(&mut self.health_center);
    }
}

impl Shell {
    /// Centered status while the backend connection is resolving: a spinner above the live status
    /// line, for a more polished loading look. Once the current step has been in flight for a
    /// while, the elapsed time and a pointer at the app log appear underneath so a stalled step
    /// reads as "stuck on X" (with somewhere to look for details) instead of a frozen screen.
    fn paint_loading(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let status = self.shared.status_text();
        let style = pal.text_style(theme, 14.0, FontWeight::MEDIUM, pal.text_muted);
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

        for (line_index, line) in self.loading_stall_lines().into_iter().flatten().enumerate() {
            let hint_style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_muted);
            let hint_w = ctx
                .measure_text(line.clone(), hint_style.clone())
                .map(|m| m.width)
                .unwrap_or(240.0);
            let hint_y = ty + th + 12.0 + line_index as f32 * 20.0;
            widgets::draw_text_in(
                ctx,
                Rect::new(cx - hint_w / 2.0, hint_y, hint_w + 4.0, 20.0),
                &line,
                hint_style,
            );
        }
    }

    /// Extra loading-screen lines once the current launch step has stalled: the elapsed
    /// time after [`LOADING_STALL_AFTER_SECS`], plus where to find technical details after
    /// [`LOADING_LOG_HINT_AFTER_SECS`].
    fn loading_stall_lines(&self) -> [Option<String>; 2] {
        let Some(elapsed) = self.shared.status_elapsed_secs() else {
            return [None, None];
        };
        if elapsed < LOADING_STALL_AFTER_SECS {
            return [None, None];
        }
        let stall = Some(format!("Still working — {elapsed}s on this step"));
        let log_hint = (elapsed >= LOADING_LOG_HINT_AFTER_SECS).then(|| {
            let log_path = {
                let state = self.shared.borrow();
                state.data_dir.root().join("logs").join("app.log")
            };
            format!("Details: {}", log_path.display())
        });
        [stall, log_hint]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum HealthActionKind {
    Reconnect,
    Retry(HealthSurface),
    Authorize(HealthSurface),
    Open(HealthSurface),
    OpenLogs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HealthActionItem {
    kind: HealthActionKind,
    label: String,
    enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthHit {
    Trigger,
    Action(usize),
}

struct HealthLayout {
    trigger: Rect,
    popup: Option<Rect>,
    header: Option<Rect>,
    issue_rows: Vec<Rect>,
    action_rows: Vec<(HealthActionItem, Rect)>,
    empty_row: Option<Rect>,
    action_error: Option<Rect>,
}

/// Focusable title-bar health affordance and its recovery popover. The widget owns no runtime
/// authority: it starts typed background work or navigates to the owning surface, and each result
/// is drained through the normal external-wake path.
struct HealthCenter {
    shared: Shared,
    compact: bool,
    open: bool,
    focused_action: usize,
    pressed: Option<HealthHit>,
    last_issue_count: usize,
    action_error: Option<String>,
}

impl HealthCenter {
    fn new(shared: Shared, compact: bool) -> Self {
        let last_issue_count = shared.health_issues().len();
        Self {
            shared,
            compact,
            open: false,
            focused_action: 0,
            pressed: None,
            last_issue_count,
            action_error: None,
        }
    }

    fn trigger_width(&self) -> f32 {
        if self.compact {
            COMPACT_STATUS_PILL_W
        } else {
            STATUS_PILL_W
        }
    }

    fn popup_height(&self, issue_count: usize) -> f32 {
        let rows = if issue_count == 0 {
            HEALTH_EMPTY_ROW_H
        } else {
            issue_count as f32 * HEALTH_ISSUE_ROW_H
        };
        HEALTH_HEADER_H
            + rows
            + HEALTH_FOOTER_H
            + if self.action_error.is_some() {
                24.0
            } else {
                0.0
            }
    }

    fn actions_for_issue(&self, issue: &HealthIssue) -> Vec<HealthActionItem> {
        let pending = self.shared.connection_health().is_pending();
        let mut actions = match issue.surface {
            HealthSurface::Connection => vec![
                HealthActionItem {
                    kind: HealthActionKind::Reconnect,
                    label: if pending {
                        "Reconnecting…".to_string()
                    } else {
                        "Reconnect".to_string()
                    },
                    enabled: !pending,
                },
                HealthActionItem {
                    kind: HealthActionKind::Open(HealthSurface::Settings(
                        settings::SettingsSection::Connection,
                    )),
                    label: "Connection settings".to_string(),
                    enabled: true,
                },
            ],
            HealthSurface::Chat => vec![
                HealthActionItem {
                    kind: HealthActionKind::Retry(HealthSurface::Chat),
                    label: "Retry Chat".to_string(),
                    enabled: true,
                },
                HealthActionItem {
                    kind: HealthActionKind::Open(HealthSurface::Chat),
                    label: "Open Chat".to_string(),
                    enabled: true,
                },
            ],
            HealthSurface::Files => vec![
                HealthActionItem {
                    kind: HealthActionKind::Retry(HealthSurface::Files),
                    label: "Retry Files".to_string(),
                    enabled: true,
                },
                HealthActionItem {
                    kind: HealthActionKind::Open(HealthSurface::Files),
                    label: "Open Files".to_string(),
                    enabled: true,
                },
            ],
            HealthSurface::Cluster => vec![
                HealthActionItem {
                    kind: HealthActionKind::Retry(HealthSurface::Cluster),
                    label: "Retry Cluster".to_string(),
                    enabled: true,
                },
                HealthActionItem {
                    kind: HealthActionKind::Open(HealthSurface::Cluster),
                    label: "Open Cluster".to_string(),
                    enabled: true,
                },
            ],
            HealthSurface::Evolution => {
                let pending = self.shared.evolution().dashboard_refresh_pending();
                vec![
                    HealthActionItem {
                        kind: HealthActionKind::Retry(HealthSurface::Evolution),
                        label: if pending {
                            "Retrying Evolution…".to_string()
                        } else {
                            "Retry Evolution".to_string()
                        },
                        enabled: !pending,
                    },
                    HealthActionItem {
                        kind: HealthActionKind::Open(HealthSurface::Evolution),
                        label: "Open Evolution".to_string(),
                        enabled: true,
                    },
                ]
            }
            HealthSurface::Settings(section) => vec![HealthActionItem {
                kind: HealthActionKind::Open(HealthSurface::Settings(section)),
                label: format!("Open {}", section.title()),
                enabled: true,
            }],
        };
        if issue.needs_authorization() {
            let label = match issue.surface {
                HealthSurface::Connection => "Authorize Connection".to_string(),
                HealthSurface::Chat => "Authorize Chat".to_string(),
                HealthSurface::Files => "Authorize Files".to_string(),
                HealthSurface::Cluster => "Authorize Cluster".to_string(),
                HealthSurface::Evolution => "Authorize Evolution".to_string(),
                HealthSurface::Settings(settings::SettingsSection::Providers) => {
                    "Authorize Provider".to_string()
                }
                HealthSurface::Settings(section) => {
                    format!("Authorize {}", section.title())
                }
            };
            actions.insert(
                actions.len().saturating_sub(1),
                HealthActionItem {
                    kind: HealthActionKind::Authorize(issue.surface),
                    label,
                    enabled: true,
                },
            );
        }
        actions
    }

    fn layout(&self, bounds: Rect, issues: &[HealthIssue]) -> HealthLayout {
        let trigger_x = if self.compact {
            bounds.x()
        } else {
            bounds.max_x() - self.trigger_width()
        };
        let trigger = Rect::new(
            trigger_x,
            bounds.y(),
            self.trigger_width().min(bounds.width()),
            if self.compact {
                COMPACT_STATUS_PILL_W
            } else {
                STATUS_PILL_H
            },
        );
        if !self.open {
            return HealthLayout {
                trigger,
                popup: None,
                header: None,
                issue_rows: Vec::new(),
                action_rows: Vec::new(),
                empty_row: None,
                action_error: None,
            };
        }

        let popup = Rect::new(
            bounds.x(),
            trigger.max_y() + HEALTH_POPOVER_GAP,
            bounds.width(),
            (bounds.max_y() - trigger.max_y() - HEALTH_POPOVER_GAP).max(0.0),
        );
        let header = Rect::new(popup.x(), popup.y(), popup.width(), HEALTH_HEADER_H);
        let mut issue_rows = Vec::new();
        let mut action_rows = Vec::new();
        let mut y = header.max_y();
        let empty_row = if issues.is_empty() {
            let row = Rect::new(
                popup.x() + 12.0,
                y,
                popup.width() - 24.0,
                HEALTH_EMPTY_ROW_H,
            );
            y += HEALTH_EMPTY_ROW_H;
            Some(row)
        } else {
            for issue in issues {
                let row = Rect::new(
                    popup.x() + 12.0,
                    y,
                    popup.width() - 24.0,
                    HEALTH_ISSUE_ROW_H,
                );
                let actions = self.actions_for_issue(issue);
                let count = actions.len().max(1) as f32;
                let available = (row.width() - 24.0 - HEALTH_ACTION_GAP * (count - 1.0)).max(0.0);
                let action_w = available / count;
                for (index, action) in actions.into_iter().enumerate() {
                    action_rows.push((
                        action,
                        Rect::new(
                            row.x() + 12.0 + index as f32 * (action_w + HEALTH_ACTION_GAP),
                            row.max_y() - HEALTH_ACTION_H - 8.0,
                            action_w,
                            HEALTH_ACTION_H,
                        ),
                    ));
                }
                issue_rows.push(row);
                y += HEALTH_ISSUE_ROW_H;
            }
            None
        };
        let action_error = self.action_error.as_ref().map(|_| {
            let rect = Rect::new(popup.x() + 12.0, y, popup.width() - 24.0, 24.0);
            y += 24.0;
            rect
        });
        action_rows.push((
            HealthActionItem {
                kind: HealthActionKind::OpenLogs,
                label: "Open app log".to_string(),
                enabled: true,
            },
            Rect::new(
                popup.x() + 12.0,
                y + 8.0,
                (popup.width() - 24.0).min(128.0),
                HEALTH_ACTION_H,
            ),
        ));

        HealthLayout {
            trigger,
            popup: Some(popup),
            header: Some(header),
            issue_rows,
            action_rows,
            empty_row,
            action_error,
        }
    }

    fn hit_test(&self, bounds: Rect, position: Point) -> Option<HealthHit> {
        let issues = self.shared.health_issues();
        let layout = self.layout(bounds, &issues);
        if layout.trigger.contains(position) {
            return Some(HealthHit::Trigger);
        }
        layout
            .action_rows
            .iter()
            .position(|(_, rect)| rect.contains(position))
            .map(HealthHit::Action)
    }

    fn open_surface(&self, surface: HealthSurface) {
        let mut app = self.shared.borrow_mut();
        match surface {
            HealthSurface::Connection => {
                app.active_tab = Tab::Settings;
                if let Some(ready) = app.ready.as_mut() {
                    ready.settings.open(settings::SettingsSection::Connection);
                }
            }
            HealthSurface::Chat => app.active_tab = Tab::Sinomo,
            HealthSurface::Files => app.active_tab = Tab::Files,
            HealthSurface::Cluster => app.active_tab = Tab::Cluster,
            HealthSurface::Evolution => app.active_tab = Tab::Evolution,
            HealthSurface::Settings(section) => {
                app.active_tab = Tab::Settings;
                if let Some(ready) = app.ready.as_mut() {
                    ready.settings.open(section);
                }
            }
        }
    }

    fn retry_surface(&self, surface: HealthSurface) {
        match surface {
            HealthSurface::Connection => {
                self.shared.request_connection_probe(true);
            }
            HealthSurface::Chat => {
                let waker = { self.shared.borrow().waker.clone() };
                self.shared.chat_mut().start_session_list_load(waker);
            }
            HealthSurface::Files => {
                let waker = { self.shared.borrow().waker.clone() };
                self.shared.files_mut().retry(waker);
            }
            HealthSurface::Cluster => {
                cluster::request_refresh(&self.shared);
            }
            HealthSurface::Evolution => {
                let (data_dir, waker) = {
                    let app = self.shared.borrow();
                    let Some(ready) = app.ready.as_ref() else {
                        return;
                    };
                    (ready.backend.data_dir.clone(), app.waker.clone())
                };
                self.shared
                    .evolution_mut()
                    .retry_dashboard_refresh(data_dir, waker);
            }
            HealthSurface::Settings(_) => {
                self.open_surface(surface);
            }
        }
    }

    fn authorize_surface(&self, surface: HealthSurface) {
        let section = match surface {
            HealthSurface::Settings(settings::SettingsSection::Providers) => {
                settings::SettingsSection::Providers
            }
            _ => settings::SettingsSection::Connection,
        };
        self.open_surface(HealthSurface::Settings(section));
    }

    /// Returns whether navigation occurred and the popover should close.
    fn activate_action(&mut self, action: HealthActionItem) -> bool {
        if !action.enabled {
            return false;
        }
        self.action_error = None;
        match action.kind {
            HealthActionKind::Reconnect => {
                self.shared.request_connection_probe(true);
                false
            }
            HealthActionKind::Retry(surface) => {
                self.retry_surface(surface);
                matches!(surface, HealthSurface::Settings(_))
            }
            HealthActionKind::Authorize(surface) => {
                self.authorize_surface(surface);
                true
            }
            HealthActionKind::Open(surface) => {
                self.open_surface(surface);
                true
            }
            HealthActionKind::OpenLogs => {
                if let Err(error) = open_app_log(&self.shared) {
                    tracing::warn!(%error, "failed to open app log");
                    self.action_error = Some(error);
                }
                false
            }
        }
    }

    fn set_open(&mut self, ctx: &mut EventCtx, open: bool) {
        if self.open == open {
            return;
        }
        self.open = open;
        self.focused_action = 0;
        self.pressed = None;
        if !open {
            self.action_error = None;
        }
        ctx.request_measure();
        ctx.request_arrange();
        ctx.request_semantics();
        ctx.request_paint();
    }

    fn move_action_focus(&mut self, delta: isize, action_count: usize) {
        if action_count == 0 {
            self.focused_action = 0;
            return;
        }
        self.focused_action =
            (self.focused_action as isize + delta).rem_euclid(action_count as isize) as usize;
    }

    fn paint_action(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        index: usize,
        action: &HealthActionItem,
        rect: Rect,
    ) {
        let focused = ctx.is_focused() && self.open && self.focused_action == index;
        let pressed = self.pressed == Some(HealthHit::Action(index));
        let fill = if !action.enabled {
            pal.panel
        } else if pressed {
            pal.surface_3
        } else if focused {
            pal.accent_soft
        } else {
            pal.surface_2
        };
        let border = if focused {
            pal.focus
        } else if action.enabled {
            pal.border
        } else {
            pal.border_subtle
        };
        widgets::round_panel(ctx, rect, fill, border, 5.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 8.0, rect.y(), rect.width() - 16.0, rect.height()),
            &action.label,
            pal.text_style(
                theme,
                11.0,
                FontWeight::MEDIUM,
                if action.enabled {
                    pal.text
                } else {
                    pal.text_disabled
                },
            ),
        );
    }
}

impl Widget for HealthCenter {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if let Some(hit) = self.hit_test(ctx.bounds(), pointer.position) {
                    if hit == HealthHit::Trigger && !self.shared.is_ready() {
                        return;
                    }
                    self.pressed = Some(hit);
                    ctx.request_pointer_capture(pointer.pointer_id);
                    ctx.request_focus();
                    ctx.request_paint();
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && self.pressed.is_some() =>
            {
                let pressed = self.pressed.take();
                let released = self.hit_test(ctx.bounds(), pointer.position);
                ctx.release_pointer_capture(pointer.pointer_id);
                if pressed == released {
                    match released {
                        Some(HealthHit::Trigger) => self.set_open(ctx, !self.open),
                        Some(HealthHit::Action(index)) => {
                            let issues = self.shared.health_issues();
                            let layout = self.layout(ctx.bounds(), &issues);
                            if let Some((action, _)) = layout.action_rows.get(index).cloned() {
                                let close = self.activate_action(action);
                                if close {
                                    self.set_open(ctx, false);
                                } else {
                                    ctx.request_measure();
                                    ctx.request_arrange();
                                    ctx.request_semantics();
                                    ctx.request_paint();
                                }
                            }
                        }
                        None => {}
                    }
                }
                ctx.request_paint();
                ctx.set_handled();
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Cancel => {
                if self.pressed.take().is_some() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                }
            }
            Event::Keyboard(key) if ctx.is_focused() && key.state == KeyState::Pressed => {
                if !self.open {
                    if matches!(key.key.as_str(), "Enter" | " ") && self.shared.is_ready() {
                        self.set_open(ctx, true);
                        ctx.set_handled();
                    }
                    return;
                }
                let issues = self.shared.health_issues();
                let layout = self.layout(ctx.bounds(), &issues);
                let action_count = layout.action_rows.len();
                match key.key.as_str() {
                    "Escape" => self.set_open(ctx, false),
                    "ArrowDown" | "ArrowRight" => {
                        self.move_action_focus(1, action_count);
                        ctx.request_paint();
                        ctx.request_semantics();
                    }
                    "ArrowUp" | "ArrowLeft" => {
                        self.move_action_focus(-1, action_count);
                        ctx.request_paint();
                        ctx.request_semantics();
                    }
                    "Home" => {
                        self.focused_action = 0;
                        ctx.request_paint();
                        ctx.request_semantics();
                    }
                    "End" => {
                        self.focused_action = action_count.saturating_sub(1);
                        ctx.request_paint();
                        ctx.request_semantics();
                    }
                    "Enter" | " " => {
                        if let Some((action, _)) =
                            layout.action_rows.get(self.focused_action).cloned()
                        {
                            let close = self.activate_action(action);
                            if close {
                                self.set_open(ctx, false);
                            } else {
                                ctx.request_measure();
                                ctx.request_arrange();
                                ctx.request_semantics();
                                ctx.request_paint();
                            }
                        }
                    }
                    _ => return,
                }
                ctx.set_handled();
            }
            Event::Custom(_) | Event::Window(WindowEvent::RedrawRequested) if self.open => {
                let count = self.shared.health_issues().len();
                if count != self.last_issue_count {
                    self.last_issue_count = count;
                    self.focused_action = 0;
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_semantics();
                }
                ctx.request_paint();
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let issue_count = self.shared.health_issues().len();
        self.last_issue_count = issue_count;
        let width = if self.open {
            HEALTH_POPOVER_W
        } else {
            self.trigger_width()
        };
        let trigger_h = if self.compact {
            COMPACT_STATUS_PILL_W
        } else {
            STATUS_PILL_H
        };
        let height = if self.open {
            trigger_h + HEALTH_POPOVER_GAP + self.popup_height(issue_count)
        } else {
            trigger_h
        };
        constraints.clamp(Size::new(width, height))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let dark = widgets::theme_is_dark(&self.shared);
        let pal = Palette::from_theme(&theme, dark);
        let status = shell_status(&self.shared);
        let issues = self.shared.health_issues();
        let layout = self.layout(ctx.bounds(), &issues);
        let trigger_pressed = self.pressed == Some(HealthHit::Trigger);
        let trigger_border = if ctx.is_focused() {
            pal.focus
        } else if status.degraded {
            pal.warn
        } else {
            pal.border
        };
        let trigger_fill = if status.degraded {
            pal.warn_soft
        } else if trigger_pressed || self.open {
            pal.surface_3
        } else {
            pal.surface_2
        };
        widgets::round_panel(
            ctx,
            layout.trigger,
            trigger_fill,
            trigger_border,
            layout.trigger.height() / 2.0,
        );
        let dot = if status.degraded {
            pal.warn
        } else if status.connected_daemon {
            pal.good
        } else {
            pal.accent
        };
        if self.compact {
            ctx.fill(
                sui::Path::circle(
                    Point::new(
                        layout.trigger.x() + layout.trigger.width() / 2.0,
                        layout.trigger.y() + layout.trigger.height() / 2.0,
                    ),
                    4.0,
                ),
                dot,
            );
        } else {
            ctx.fill(
                sui::Path::circle(
                    Point::new(
                        layout.trigger.x() + 12.0,
                        layout.trigger.y() + layout.trigger.height() / 2.0,
                    ),
                    4.0,
                ),
                dot,
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    layout.trigger.x() + 22.0,
                    layout.trigger.y(),
                    layout.trigger.width() - 30.0,
                    layout.trigger.height(),
                ),
                &status.label,
                pal.text_style(
                    &theme,
                    12.0,
                    FontWeight::MEDIUM,
                    if status.degraded {
                        pal.warn_text
                    } else {
                        pal.text_muted
                    },
                ),
            );
        }

        let Some(popup) = layout.popup else {
            return;
        };
        sui::paint_theme_shadow(ctx, popup, [8.0; 4], &theme.shadows.box_shadow.lg);
        widgets::round_panel(ctx, popup, pal.overlay, pal.border_strong, 8.0);
        if let Some(header) = layout.header {
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    header.x() + 14.0,
                    header.y() + 5.0,
                    header.width() - 28.0,
                    22.0,
                ),
                "Health and recovery",
                pal.text_style(&theme, 14.0, FontWeight::SEMIBOLD, pal.text),
            );
            let summary = if issues.is_empty() {
                self.shared.status_detail()
            } else {
                format!(
                    "{} affected {}",
                    issues.len(),
                    if issues.len() == 1 {
                        "surface"
                    } else {
                        "surfaces"
                    }
                )
            };
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    header.x() + 14.0,
                    header.y() + 27.0,
                    header.width() - 28.0,
                    19.0,
                ),
                &compact_health_line(&summary, 64),
                pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_muted),
            );
            widgets::hairline_bottom(ctx, header, pal.border_subtle);
        }

        for (issue, row) in issues.iter().zip(layout.issue_rows.iter().copied()) {
            widgets::round_panel(
                ctx,
                row.inflate(-2.0, -3.0),
                pal.panel,
                pal.border_subtle,
                6.0,
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 12.0, row.y() + 5.0, row.width() - 24.0, 20.0),
                &issue.title,
                pal.text_style(&theme, 12.0, FontWeight::SEMIBOLD, pal.text),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 12.0, row.y() + 26.0, row.width() - 24.0, 22.0),
                &compact_health_line(&issue.detail, 62),
                pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_muted),
            );
        }
        if let Some(row) = layout.empty_row {
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 4.0, row.y() + 4.0, row.width() - 8.0, 22.0),
                "All monitored surfaces are operational.",
                pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.ok_text),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 4.0, row.y() + 27.0, row.width() - 8.0, 22.0),
                &compact_health_line(&self.shared.status_detail(), 62),
                pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_muted),
            );
        }
        if let (Some(error), Some(rect)) = (&self.action_error, layout.action_error) {
            widgets::draw_text_in(
                ctx,
                rect,
                &compact_health_line(error, 66),
                pal.text_style(&theme, 11.0, FontWeight::MEDIUM, pal.danger_text),
            );
        }
        for (index, (action, rect)) in layout.action_rows.iter().enumerate() {
            self.paint_action(ctx, &theme, &pal, index, action, *rect);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let status = shell_status(&self.shared);
        let issues = self.shared.health_issues();
        let layout = self.layout(ctx.bounds(), &issues);
        let ready = self.shared.is_ready();
        let base = ctx.widget_id().get().wrapping_mul(131);

        let root_role = if !ready {
            SemanticsRole::Text
        } else if self.open {
            SemanticsRole::Popover
        } else {
            SemanticsRole::Button
        };
        let mut root = SemanticsNode::new(ctx.widget_id(), root_role, ctx.bounds());
        root.name = Some(if self.open {
            "Health and recovery".to_string()
        } else {
            status.label.clone()
        });
        root.description = Some(if self.open {
            status.detail.clone()
        } else {
            format!("Open Health and recovery. {}", status.detail)
        });
        root.value = Some(SemanticsValue::Text(if status.degraded {
            "needs attention".to_string()
        } else if ready {
            "connected".to_string()
        } else {
            "connecting".to_string()
        }));
        root.state.focused = ctx.is_focused();
        root.state.expanded = Some(self.open);
        root.state.selected = status.degraded;
        root.state.busy = self.shared.connection_health().phase == ConnectionPhase::Reconnecting;
        root.actions = if ready && self.open {
            vec![SemanticsAction::Focus, SemanticsAction::Collapse]
        } else if ready {
            vec![SemanticsAction::Focus, SemanticsAction::Activate]
        } else {
            Vec::new()
        };
        ctx.push(root);

        if !self.open {
            return;
        }
        if let Some(header) = layout.header {
            let health = self.shared.connection_health();
            let mut progress = SemanticsNode::new(
                sui::WidgetId::new(base.wrapping_add(2)),
                SemanticsRole::Text,
                header,
            );
            progress.name = Some("Connection status".to_string());
            progress.description = Some(health.detail.clone());
            progress.value = Some(SemanticsValue::Text(health.label.clone()));
            progress.state.busy = health.is_pending();
            progress.parent = Some(ctx.widget_id());
            ctx.push(progress);
        }
        let list_bounds = layout
            .popup
            .map(|popup| {
                Rect::new(
                    popup.x(),
                    popup.y() + HEALTH_HEADER_H,
                    popup.width(),
                    popup.height() - HEALTH_HEADER_H,
                )
            })
            .unwrap_or(Rect::ZERO);
        let list_id = sui::WidgetId::new(base.wrapping_add(3));
        let mut list = SemanticsNode::new(list_id, SemanticsRole::List, list_bounds);
        list.name = Some("Affected surfaces".to_string());
        list.value = Some(SemanticsValue::Text(issues.len().to_string()));
        list.parent = Some(ctx.widget_id());
        ctx.push(list);
        for (index, (issue, row)) in issues
            .iter()
            .zip(layout.issue_rows.iter().copied())
            .enumerate()
        {
            let mut node = SemanticsNode::new(
                sui::WidgetId::new(base.wrapping_add(100 + index as u64)),
                SemanticsRole::ListItem,
                row,
            );
            node.name = Some(issue.title.clone());
            node.description = Some(issue.detail.clone());
            node.parent = Some(list_id);
            ctx.push(node);
        }
        if let Some(row) = layout.empty_row {
            let mut node = SemanticsNode::new(
                sui::WidgetId::new(base.wrapping_add(90)),
                SemanticsRole::ListItem,
                row,
            );
            node.name = Some("All systems operational".to_string());
            node.description = Some(self.shared.status_detail());
            node.parent = Some(list_id);
            ctx.push(node);
        }
        for (index, (action, rect)) in layout.action_rows.iter().enumerate() {
            let mut node = SemanticsNode::new(
                sui::WidgetId::new(base.wrapping_add(1_000 + index as u64)),
                SemanticsRole::Button,
                *rect,
            );
            node.name = Some(action.label.clone());
            node.state.disabled = !action.enabled;
            node.state.focused = ctx.is_focused() && self.focused_action == index;
            node.state.selected = self.focused_action == index;
            node.parent = Some(ctx.widget_id());
            if action.enabled {
                node.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
            }
            ctx.push(node);
        }
        if let (Some(error), Some(rect)) = (&self.action_error, layout.action_error) {
            let mut node = SemanticsNode::new(
                sui::WidgetId::new(base.wrapping_add(9_000)),
                SemanticsRole::Text,
                rect,
            );
            node.name = Some("Recovery action failed".to_string());
            node.description = Some(error.clone());
            node.parent = Some(ctx.widget_id());
            ctx.push(node);
        }
    }

    fn accepts_focus(&self) -> bool {
        self.shared.is_ready()
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        if !focused && self.open {
            self.set_open(ctx, false);
        } else {
            ctx.request_paint();
            ctx.request_semantics();
        }
    }
}

fn compact_health_line(text: &str, max_chars: usize) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut compact = text
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    compact.push('…');
    compact
}

fn open_app_log(shared: &Shared) -> Result<(), String> {
    let path = {
        let state = shared.borrow();
        state.data_dir.root().join("logs").join("app.log")
    };
    open_log_path(&path)
}

#[cfg(target_os = "windows")]
fn open_log_path(path: &std::path::Path) -> Result<(), String> {
    let argument = format!("/select,{}", path.display());
    std::process::Command::new("explorer.exe")
        .arg(argument)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to open {}: {error}", path.display()))
}

#[cfg(target_os = "macos")]
fn open_log_path(path: &std::path::Path) -> Result<(), String> {
    std::process::Command::new("open")
        .arg("-R")
        .arg(path)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to reveal {}: {error}", path.display()))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn open_log_path(path: &std::path::Path) -> Result<(), String> {
    let parent = path.parent().unwrap_or(path);
    std::process::Command::new("xdg-open")
        .arg(parent)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to open {}: {error}", parent.display()))
}

#[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
fn open_log_path(path: &std::path::Path) -> Result<(), String> {
    Err(format!(
        "opening log files is not supported on this platform: {}",
        path.display()
    ))
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
    let Some(_) = state.ready.as_ref() else {
        let status = state.status.clone();
        return ShellStatus {
            label: status.clone(),
            detail: status,
            degraded: false,
            connected_daemon: false,
        };
    };
    let health = state.connection_health.clone();
    drop(state);
    let issues = shared.health_issues();

    if issues.is_empty() {
        ShellStatus {
            label: health.label,
            detail: health.detail,
            degraded: false,
            connected_daemon: health.phase.is_connected(),
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
                issues[0].detail.clone()
            } else {
                format!(
                    "{count} app surfaces need attention: {}",
                    issues
                        .iter()
                        .map(|issue| format!("{}: {}", issue.title, issue.detail))
                        .collect::<Vec<_>>()
                        .join(" · ")
                )
            },
            degraded: true,
            connected_daemon: health.phase.is_connected(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, Connection};
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

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !shared
            .chat()
            .sessions
            .iter()
            .any(|session| session.id == session_id && session.title == "Ready shell history")
            && std::time::Instant::now() < deadline
        {
            shared.chat_mut().pump_session_list();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

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
    fn attention_status_opens_recovery_center_and_authorize_deep_links() -> sui::Result<()> {
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

        assert_eq!(shared.borrow().active_tab, Tab::Sinomo);
        let snapshot = window.snapshot()?;
        let health_center = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Popover
                    && node.name.as_deref() == Some("Health and recovery")
            })
            .expect("health center semantics");
        assert_eq!(health_center.state.expanded, Some(true));
        let connection_status = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text
                    && node.name.as_deref() == Some("Connection status")
            })
            .expect("connection progress semantics");
        assert_eq!(connection_status.parent, Some(health_center.id));
        let affected = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List
                    && node.name.as_deref() == Some("Affected surfaces")
            })
            .expect("affected surfaces list");
        assert_eq!(affected.parent, Some(health_center.id));
        let files_issue = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ListItem
                    && node.name.as_deref() == Some("Files · SIFS")
                    && node
                        .description
                        .as_deref()
                        .is_some_and(|detail| detail.contains("authorization"))
            })
            .expect("Files issue semantics");
        assert_eq!(files_issue.parent, Some(affected.id));
        for action in [
            "Retry Files",
            "Authorize Files",
            "Open Files",
            "Open app log",
        ] {
            let node = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(action)
                })
                .unwrap_or_else(|| panic!("missing recovery action {action}"));
            assert_eq!(node.parent, Some(health_center.id));
        }

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Authorize Files")
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
    fn health_center_supports_escape_enter_and_arrow_navigation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
        }

        let runtime = crate::build_runtime(shared)?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let trigger = window
            .get_by_role(SemanticsRole::Button)
            .with_name("Needs attention");

        trigger.click()?;
        let health_center = window
            .get_by_role(SemanticsRole::Popover)
            .with_name("Health and recovery");
        health_center.press("Escape")?;
        let snapshot = window.snapshot()?;
        assert!(snapshot.accessibility.nodes.iter().any(|node| {
            node.role == SemanticsRole::Button
                && node.name.as_deref() == Some("Needs attention")
                && node.state.expanded == Some(false)
        }));

        trigger.press("Enter")?;
        health_center.press("ArrowDown")?;
        let snapshot = window.snapshot()?;
        let authorize = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Authorize Files")
            })
            .expect("authorize action");
        assert!(
            authorize.state.selected,
            "ArrowDown should move the recovery action cursor"
        );

        Ok(())
    }

    #[test]
    fn health_center_exposes_evolution_retry_and_deep_link() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        shared.evolution_mut().dashboard.error =
            Some("Evolution dashboard refresh failed: worker stopped".to_string());

        let runtime = crate::build_runtime(shared)?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Needs attention")
            .click()?;

        for action in ["Retry Evolution", "Open Evolution"] {
            assert_eq!(
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name(action)
                    .count()?,
                1,
                "missing {action}"
            );
        }
        Ok(())
    }

    #[test]
    fn loading_screen_surfaces_stalled_step_and_log_hint() {
        let shared = Shared::new(AppState::connecting());
        {
            let mut app = shared.borrow_mut();
            app.status = "Starting the chat engine…".to_string();
            app.status_since = Some(std::time::Instant::now() - std::time::Duration::from_secs(30));
        }
        let shell = Shell::new(shared.clone());

        let [stall, log_hint] = shell.loading_stall_lines();
        let stall = stall.expect("stalled step should surface its elapsed time");
        assert!(
            stall.contains("s on this step"),
            "unexpected stall line: {stall}"
        );
        let log_hint = log_hint.expect("stalled step should point at the app log");
        assert!(
            log_hint.contains("app.log"),
            "unexpected log hint: {log_hint}"
        );

        // A step that just started shows neither line.
        shared.borrow_mut().status_since = Some(std::time::Instant::now());
        assert_eq!(shell.loading_stall_lines(), [None, None]);
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
    fn embedded_daemon_failure_sets_attention_status() {
        let mut backend = Backend::local_fallback();
        backend.connection = Connection::LocalFallback {
            reason: Some("embedded daemon exited before ready: duplicate config key".to_string()),
        };
        let shared = Shared::new(AppState::ready(backend));

        let status = shell_status(&shared);

        assert!(
            status.label == "Needs attention" || status.label.ends_with(" issues"),
            "unexpected aggregate label: {}",
            status.label
        );
        assert!(status.degraded);
        assert!(!status.connected_daemon);
        assert!(status.detail.contains("embedded daemon failed"));
        assert!(status.detail.contains("duplicate config key"));
        assert_eq!(shared.borrow().status, "Local (daemon failed)");
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
