//! The application shell: a title bar (brand mark, tab strip, status pill) above
//! a switchable content area.
//!
//! The chrome is *composed* from `sui-widgets` where the toolkit has a stock control
//! — the title bar is a [`sui::Surface`], the wordmark is a [`sui::Label`], the tab strip is a
//! [`sui::BrowserTabBar`] plus dedicated pinned-icon widgets, the connecting indicator is a [`sui::Spinner`] child pod, and the
//! health affordance is a controlled [`sui::Popover`] assembled from typed SUI controls. Only the
//! brand image remains app-owned artwork.
//!
//! Launch and runtime work stays asynchronous: the connecting-to-ready state machine builds tab
//! content only after the backend resolves, while external wakes drain typed worker results. After
//! launch, a short background-probe lane keeps daemon status live. The title-bar health control is
//! a focusable widget with an accessible recovery popover; it never performs daemon or storage work
//! on the UI thread.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use sui::{
    Align, Alignment, ArrangeCtx, Button, ButtonAppearance, Command, CommandController, CommandCtx,
    Constraints, Event, EventCtx, EventPhase, Flex, FlexItem, FlexWrap, FontWeight, IconButton,
    IconGlyph, Image, ImageFit, ImageHandle, Insets, KeyState, Label, MeasureCtx, PaintCtx,
    PanelSection, Popover, PopoverAlignment, RebuildOnChange, Rect, RichText, ScrollView,
    SemanticRegion, SemanticTone, SemanticsAction, SemanticsActionRequest, SemanticsCtx,
    SemanticsNode, SemanticsRole, SemanticsValue, SingleChild, Size, SizedBox, Spinner,
    StatusBadge, Surface, SurfaceAppearance, SwitchView, TimerToken, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent, WindowRenderOptions,
};
use sui_lucide::LucideIcon;

#[cfg(test)]
use crate::state::ShellStatus;
use crate::state::{ConnectionPhase, HealthIssue, HealthStructureKey, HealthSurface, Shared, Tab};
use crate::theme::Palette;
use crate::ui::{chat, cluster, evolution, files, settings, widgets};

pub const TOPBAR_HEIGHT: f32 = 44.0;

/// Application work coordinator owned by the window rather than by the
/// presentation tree. Bare scheduler wakes keep legacy background lanes alive;
/// chat workers use the typed `CHAT_WORK_READY` multicast.
pub struct BackgroundWorkController {
    shared: Shared,
}

impl BackgroundWorkController {
    pub fn new(shared: Shared) -> Self {
        Self { shared }
    }

    /// Drain worker results. `CHAT_WORK_READY` is only sent by chat workers, so
    /// `chat_only` skips the other lanes: taking their state mutably republishes
    /// (clones and compares) every surface's structure key, which would otherwise
    /// run once per streamed token. Other lanes wake through the bare wake path.
    fn pump(&mut self, ctx: &mut CommandCtx, chat_only: bool) {
        let (changed, became_ready) = if chat_only {
            (false, false)
        } else {
            self.shared.pump_connect()
        };
        if became_ready {
            let (backend, waker) = {
                let app = self.shared.borrow();
                let ready = app.ready.as_ref().expect("ready after connect");
                (ready.backend.clone(), app.waker.clone())
            };
            self.shared
                .settings_mut()
                .begin_initial_load(backend.clone(), waker.clone());
            evolution::request_refresh(&self.shared);
            for scope in self.shared.chat_scopes() {
                let mut chat = scope.chat_mut();
                chat.start_provider_list_load(backend.config.clone(), waker.clone());
                chat.start_agent_list_load(waker.clone());
                chat.start_session_list_load(waker.clone());
            }
            if self.shared.active_tab() == Tab::Cluster {
                cluster::request_refresh(&self.shared);
                cluster::request_observation(&self.shared);
            }
        }

        let ready = self.shared.is_ready();
        let lanes = ready && !chat_only;
        let device_changed = lanes && self.shared.settings_mut().pump_device_login();
        let initial_settings = if lanes {
            self.shared.settings_mut().pump_initial_load()
        } else {
            settings::SettingsInitialLoadEffects::default()
        };
        if let Some(providers) = initial_settings.chat_providers {
            for scope in self.shared.chat_scopes() {
                scope.chat_mut().refresh_providers(providers.clone());
            }
        }
        let settings_changed = initial_settings.changed;
        let settings_operations_changed =
            lanes && settings::pump_background_operations(&self.shared);
        let scopes = if ready {
            std::iter::once(self.shared.root_scope())
                .chain(self.shared.workspace_scopes())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut files_changed = false;
        let mut files_attachment_changed = false;
        for scope in scopes.iter().filter(|_| !chat_only) {
            if scope.workspace_kind() != Some(crate::state::WorkspaceTabKind::Chat) {
                files_changed |= scope.files_mut().pump_load();
                files_attachment_changed |= drain_files_chat_attachment(scope);
            }
        }
        let evolution_changed = lanes && evolution::pump_refresh(&self.shared);
        let connection_changed = !chat_only && self.shared.pump_connection_probe();
        let cluster_changed = !chat_only && cluster::pump_refresh(&self.shared);
        let cluster_observation_changed = !chat_only && cluster::pump_observation(&self.shared);

        let (mut chat_changed, mut chat_semantics_changed, mut chat_layout_changed) =
            (false, false, false);
        for scope in &scopes {
            if scope
                .workspace_kind()
                .is_some_and(|kind| kind != crate::state::WorkspaceTabKind::Chat)
            {
                continue;
            }
            let chat_visible = scope.is_active_scope() && scope.active_tab() == Tab::Sinomo;
            let mut chat = scope.chat_mut();
            let worker_changed = chat.pump_worker_events();
            let providers_changed = chat.pump_provider_list();
            let agents_changed = chat.pump_agent_list();
            let list_changed = chat.pump_session_list();
            let transcript_changed = chat.pump_transcript_load();
            let delete_changed = chat.pump_delete();
            let channel_create_changed = chat.pump_channel_create();
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
                || transcript_changed
                || delete_changed
                || channel_create_changed
                || media_visibility_changed;
            chat_changed |= structural_changed || media_changed.paint;
            chat_semantics_changed |= structural_changed || media_changed.semantics;
            chat_layout_changed |= structural_changed || media_changed.layout;
        }

        let update_changed = crate::updates::pump(&self.shared);
        let layout_changed = update_changed
            || became_ready
            || device_changed
            || files_changed
            || evolution_changed
            || files_attachment_changed
            || connection_changed
            || cluster_changed
            || cluster_observation_changed
            || chat_layout_changed
            || settings_changed
            || settings_operations_changed;
        if layout_changed {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
            ctx.request_paint();
        } else {
            if chat_semantics_changed {
                ctx.request_semantics();
            }
            if changed || chat_changed {
                ctx.request_paint();
            }
        }

        if let Some(window_id) = ctx.window_id() {
            sui::set_window_render_options(window_id, self.shared.render_options());
        }
    }
}

impl CommandController for BackgroundWorkController {
    fn command(&mut self, ctx: &mut CommandCtx, command: &Command<'_>) {
        #[cfg(all(target_os = "android", feature = "mobile"))]
        if let Some(insets) = command.get(crate::ANDROID_SAFE_AREA_CHANGED) {
            crate::set_android_safe_area_insets(*insets);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
            ctx.request_paint();
            ctx.set_handled();
            return;
        }
        if command.is(chat::CHAT_WORK_READY) {
            // Re-arm before draining so events sent from here on queue a new broadcast.
            chat::clear_chat_work_broadcast();
            if crate::mobile_runtime::interactive() {
                self.pump(ctx, true);
            }
            ctx.set_handled();
        }
    }

    fn wake(&mut self, ctx: &mut CommandCtx) {
        if crate::mobile_runtime::interactive() {
            self.pump(ctx, false);
        }
    }
}
const LOGO_SIZE: f32 = 24.0;
const EDGE_PAD: f32 = 18.0;
const TAB_HEIGHT: f32 = 32.0;
/// Keep outward SUI focus chrome inside the horizontally clipped tab viewport.
const TAB_VIEWPORT_PADDING: f32 = 4.0;
const TAB_SEG_HEIGHT: f32 = TAB_HEIGHT + (TAB_VIEWPORT_PADDING * 2.0);
const STATUS_GAP: f32 = 12.0;
const STATUS_PILL_H: f32 = 24.0;
const STATUS_PILL_W: f32 = 150.0;
const COMPACT_CHROME_WIDTH: f32 = 620.0;
/// Stock compact `IconButton` footprint. Keep title-bar allocation in lockstep with the control.
const COMPACT_STATUS_PILL_W: f32 = 28.0;
const HEALTH_POPOVER_W: f32 = 420.0;
const HEALTH_CONTENT_MIN_H: f32 = 176.0;
const HEALTH_CONTENT_MAX_H: f32 = 548.0;
const HEALTH_ISSUE_ESTIMATED_H: f32 = 104.0;
const HEALTH_ACTION_H: f32 = 30.0;
const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const CLUSTER_OBSERVE_INTERVAL: Duration = Duration::from_secs(5);
const EVOLUTION_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Stock spinner shown in the centered launch-progress surface.
const LOADING_SPINNER_SIZE: f32 = 24.0;
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
    /// Stock title-bar surface owns the background, border, and theme response.
    titlebar: WidgetPod,
    logo: SingleChild,
    /// Stock text widget for the product wordmark. Hidden at the compact breakpoint.
    wordmark: WidgetPod,
    /// Fixed application icons, retained closable workspace tabs, and the new-tab menu.
    tabs: WidgetPod,
    /// Focusable status trigger plus the unified health/recovery popover. This is a real child
    /// widget (rather than hand-painted shell chrome), so keyboard focus and UI Automation can
    /// reach the trigger, issue rows, and recovery actions.
    health_center: WidgetPod,
    /// Reactive loading boundary and retained tab bodies keyed by stable workspace identities.
    body: WidgetPod,
    compact_tabs: Cell<bool>,
    last_render_options: Cell<Option<WindowRenderOptions>>,
    tab_strip_rect: Rect,
    /// Repaint driver for the loading screen: keeps the per-step elapsed time live
    /// while the backend connection resolves, so a stalled launch step is visible.
    connect_status_timer: Cell<Option<TimerToken>>,
    /// Periodic live daemon observation; the probe itself always runs on a background thread.
    health_probe_timer: Cell<Option<TimerToken>>,
    /// Five-second live metrics cadence while Cluster / Overview is visible.
    cluster_observe_timer: Cell<Option<TimerToken>>,
    /// Five-second goal/DAG refresh while the Evolution surface is visible.
    evolution_refresh_timer: Cell<Option<TimerToken>>,
    window_occluded: Cell<bool>,
    body_rect: Rect,
}

impl Shell {
    pub fn new(shared: Shared) -> Self {
        let titlebar = WidgetPod::new(build_titlebar_surface(&shared));
        let logo = SingleChild::new(
            Image::new(logo_image_handle())
                .fit(ImageFit::Contain)
                .size(Size::new(LOGO_SIZE, LOGO_SIZE))
                .show_border(false)
                .corner_radius(0.0),
        );
        let wordmark = WidgetPod::new(build_wordmark(&shared));

        let tabs = WidgetPod::new(build_tab_bar(&shared, false));
        let health_center = WidgetPod::new(HealthCenter::new(shared.clone(), false));
        let body = {
            let ready = shared.signals().ready.clone();
            let body_shared = shared.clone();
            WidgetPod::new(RebuildOnChange::key_from(ready, move |ready| {
                build_shell_body(body_shared.clone(), *ready)
            }))
        };

        let shell = Self {
            shared,
            titlebar,
            logo,
            wordmark,
            tabs,
            health_center,
            body,
            compact_tabs: Cell::new(false),
            last_render_options: Cell::new(None),
            tab_strip_rect: Rect::ZERO,
            connect_status_timer: Cell::new(None),
            health_probe_timer: Cell::new(None),
            cluster_observe_timer: Cell::new(None),
            evolution_refresh_timer: Cell::new(None),
            window_occluded: Cell::new(false),
            body_rect: Rect::ZERO,
        };
        if shell.shared.is_ready() {
            let waker = { shell.shared.borrow().waker.clone() };
            shell.shared.chat_mut().start_session_list_load(waker);
        }
        shell
    }

    fn schedule_connect_status_timer(&self, ctx: &mut EventCtx) {
        if !crate::mobile_runtime::interactive()
            || self.shared.is_ready()
            || self.connect_status_timer.get().is_some()
        {
            return;
        }
        let token = ctx.schedule_timer_after(1.0);
        self.connect_status_timer.set(Some(token));
    }

    fn schedule_health_probe_timer(&self, ctx: &mut EventCtx) {
        if !crate::mobile_runtime::interactive()
            || !self.shared.is_ready()
            || self.health_probe_timer.get().is_some()
            || self.shared.borrow().waker.is_none()
        {
            return;
        }
        let interval = crate::mobile_runtime::passive_network_interval(
            HEALTH_PROBE_INTERVAL,
            crate::mobile_runtime::MobilePassiveNetworkClass::Essential,
        );
        self.health_probe_timer
            .set(Some(ctx.schedule_timer_after(interval.as_secs_f64())));
    }

    fn schedule_cluster_observe_timer(&self, ctx: &mut EventCtx) {
        if !crate::mobile_runtime::interactive()
            || self.window_occluded.get()
            || !cluster::observation_visible(&self.shared)
            || self.cluster_observe_timer.get().is_some()
            || self.shared.borrow().waker.is_none()
        {
            return;
        }
        let interval = crate::mobile_runtime::passive_network_interval(
            CLUSTER_OBSERVE_INTERVAL,
            crate::mobile_runtime::MobilePassiveNetworkClass::Observability,
        );
        self.cluster_observe_timer
            .set(Some(ctx.schedule_timer_after(interval.as_secs_f64())));
    }

    fn schedule_evolution_refresh_timer(&self, ctx: &mut EventCtx) {
        if !crate::mobile_runtime::interactive()
            || self.window_occluded.get()
            || self.shared.active_tab() != Tab::Evolution
            || self.evolution_refresh_timer.get().is_some()
            || self.shared.borrow().waker.is_none()
        {
            return;
        }
        self.evolution_refresh_timer.set(Some(
            ctx.schedule_timer_after(EVOLUTION_REFRESH_INTERVAL.as_secs_f64()),
        ));
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

    /// Route operating-system file drags delivered at the window root to the
    /// visible workflow. SUI emits one event per path, so both Chat's
    /// validation lane and Files' import lane accept paths incrementally.
    fn handle_external_file_window_event(&self, event: &WindowEvent) -> bool {
        if !self.shared.is_ready() {
            return false;
        }
        if self.shared.active_scope().workspace_kind()
            == Some(crate::state::WorkspaceTabKind::Document)
        {
            return match event {
                WindowEvent::ExternalFileHovered(_) | WindowEvent::ExternalFileHoverCancelled => {
                    true
                }
                WindowEvent::ExternalFileDropped(path) => {
                    let title = path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.display().to_string());
                    self.shared.open_document_tab(
                        files::FilesSourceKind::Local,
                        path.to_string_lossy().into_owned(),
                        title,
                    );
                    true
                }
                _ => false,
            };
        }
        match event {
            WindowEvent::ExternalFileHovered(path) => match self.shared.active_tab() {
                Tab::Sinomo => {
                    self.shared
                        .active_scope()
                        .files_mut()
                        .cancel_external_file_hover();
                    self.shared
                        .active_scope()
                        .chat_mut()
                        .begin_external_attachment_hover();
                    true
                }
                Tab::Files => {
                    self.shared
                        .active_scope()
                        .chat_mut()
                        .cancel_external_attachment_hover();
                    self.shared
                        .active_scope()
                        .files_mut()
                        .begin_external_file_hover(path.clone());
                    true
                }
                _ => false,
            },
            WindowEvent::ExternalFileHoverCancelled => {
                self.shared
                    .active_scope()
                    .chat_mut()
                    .cancel_external_attachment_hover();
                self.shared
                    .active_scope()
                    .files_mut()
                    .cancel_external_file_hover();
                true
            }
            WindowEvent::ExternalFileDropped(path) => {
                self.shared
                    .active_scope()
                    .chat_mut()
                    .cancel_external_attachment_hover();
                self.shared
                    .active_scope()
                    .files_mut()
                    .cancel_external_file_hover();
                match self.shared.active_tab() {
                    Tab::Sinomo => {
                        let _ = self
                            .shared
                            .active_scope()
                            .chat_mut()
                            .queue_dropped_local_attachment_path(path.clone());
                        true
                    }
                    Tab::Files => {
                        let waker = { self.shared.borrow().waker.clone() };
                        self.shared
                            .active_scope()
                            .files_mut()
                            .accept_external_file_drop(path.clone(), waker);
                        true
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// Consume Android/browser Back while the active surface has an internal
    /// destination. Unhandled Back is left to the platform, which closes the
    /// root window once overlays and focused text input have had first refusal.
    fn handle_navigation_back(&self) -> bool {
        if !self.shared.is_ready() {
            return false;
        }
        match self.shared.active_tab() {
            Tab::Cluster => self.shared.cluster_mut().navigate_back(),
            Tab::Evolution => false,
            Tab::Settings => self.shared.settings_mut().navigate_back(),
            Tab::Files => {
                let waker = { self.shared.borrow().waker.clone() };
                self.shared.active_scope().files_mut().navigate_back(waker)
            }
            _ => false,
        }
    }
}

/// Move a verified Files snapshot into the current Chat draft. The Files
/// worker owns all reads; this bridge only transfers already-validated bytes
/// and switches surfaces after the intent is ready.
fn drain_files_chat_attachment(shared: &Shared) -> bool {
    let intent = shared
        .files_mut_unpublished()
        .take_ready_attachment_intent();
    let Some(intent) = intent else {
        return false;
    };
    shared.publish_files_signals();
    let files::FilesAttachmentIntent {
        file_name,
        bytes,
        provenance,
        is_unsaved_editor_buffer,
    } = intent;
    let target_chat = shared.current_chat_scope();
    {
        let mut chat = target_chat.chat_mut();
        let _ = match provenance {
            files::FilesAttachmentProvenance::Local {
                path,
                canonical_path,
                modified_at_unix_nanos,
            } => {
                let provenance = sinomo_client::chat_contract::ChatLocalAttachmentProvenance {
                    original_path: path,
                    canonical_path: (!canonical_path.trim().is_empty()).then_some(canonical_path),
                    modified_at_unix_nanos,
                    unsaved_editor_buffer: is_unsaved_editor_buffer,
                };
                chat.queue_local_attachment_snapshot_with_provenance(file_name, provenance, bytes)
            }
            files::FilesAttachmentProvenance::Sifs {
                path,
                object_id,
                version,
                content_hash,
                policy,
            } => {
                let provenance = sinomo_client::chat_contract::ChatSifsAttachmentProvenance {
                    path,
                    object_id: (!object_id.trim().is_empty()).then_some(object_id),
                    version: (!version.trim().is_empty()).then_some(version),
                    content_hash,
                    policy: (!policy.trim().is_empty()).then_some(policy),
                    unsaved_editor_buffer: is_unsaved_editor_buffer,
                };
                chat.queue_sifs_attachment_snapshot_with_provenance(file_name, provenance, bytes)
            }
        };
        chat.open_attachment_surface();
    }
    if let Some(id) = target_chat.workspace_id() {
        shared.select_workspace_tab(id);
    } else {
        shared.set_active_tab(Tab::Sinomo);
    }
    true
}

fn build_titlebar_surface(shared: &Shared) -> Surface {
    let shared = shared.clone();
    Surface::titlebar(SizedBox::new())
        .name("Application title bar")
        .theme_when(move || shared.theme())
        .fill()
}

fn build_wordmark(shared: &Shared) -> RebuildOnChange<sui::DefaultTheme> {
    RebuildOnChange::key_from(
        shared
            .signals()
            .theme
            .select_named("shell wordmark theme", |theme| *theme),
        move |theme| {
            let mut style = theme.body_text_style();
            style.font_size = theme.text.base.size;
            style.line_height = theme.text.base.line_height;
            style.weight = FontWeight::SEMIBOLD;
            style.color = theme.surfaces.text;
            WidgetPod::new(
                Label::new("Sinomo")
                    .semantic_name("Sinomo wordmark")
                    .text_style(style),
            )
        },
    )
}

/// Build fixed application destinations and the independently scrolling workspace tab bar.
fn build_tab_bar(shared: &Shared, compact: bool) -> impl Widget + use<> {
    super::workspace::TabStrip::new(shared.clone(), compact)
}

pub(crate) fn tab_icons() -> [LucideIcon; 5] {
    Tab::ALL.map(tab_icon)
}

fn tab_icon(tab: Tab) -> LucideIcon {
    match tab {
        Tab::Sinomo => LucideIcon::MessagesSquare,
        Tab::Cluster => LucideIcon::Cloudy,
        Tab::Evolution => LucideIcon::GitBranch,
        Tab::Files => LucideIcon::Folders,
        Tab::Settings => LucideIcon::Settings,
    }
}

fn build_shell_body(shared: Shared, ready: bool) -> WidgetPod {
    if !ready {
        return WidgetPod::new(build_loading_view(shared));
    }

    let busy = shared
        .signals()
        .app_update
        .select_named("app update restart surface", |v| usize::from(v.busy));
    let tabs = super::workspace::TabContents::new(shared.clone());
    WidgetPod::new(
        Flex::vertical()
            .with_child(crate::updates::banner(shared))
            .align_items(Alignment::Stretch)
            .with_item(
                SwitchView::new()
                    .selected_from(busy)
                    .with_child(tabs)
                    .with_child(Label::new("Restoring your workspace after the update…")),
                FlexItem::flex(1.0),
            ),
    )
}

fn build_loading_view(shared: Shared) -> Surface {
    let theme = shared.theme();
    let mut status_style = theme.text_style(theme.palette.text_muted);
    status_style.font_size = theme.text.base.size;
    status_style.line_height = theme.text.base.line_height;
    status_style.weight = FontWeight::MEDIUM;

    let hint_theme = theme;
    let hints =
        RebuildOnChange::key_from(shared.signals().loading_stall_lines.clone(), move |lines| {
            let mut hint_style = hint_theme.text_style(hint_theme.palette.text_muted);
            hint_style.font_size = hint_theme.text.xs.size;
            hint_style.line_height = hint_theme.text.xs.line_height;
            let mut content = Flex::vertical()
                .main_gap(4.0)
                .align_items(Alignment::Center);
            for line in lines.iter().flatten() {
                content = content.with_child(
                    SizedBox::new().width(560.0).child(
                        RichText::from_plain_text(line.clone(), hint_style.clone())
                            .semantic_name(line.clone()),
                    ),
                );
            }
            WidgetPod::new(content)
        });

    let content = Flex::vertical()
        .main_gap(10.0)
        .align_items(Alignment::Center)
        .with_child(
            Spinner::new("shell-connecting")
                .size(LOADING_SPINNER_SIZE)
                .theme(theme),
        )
        .with_child(
            Label::new(shared.status_text())
                .text_from(shared.signals().status_text.clone())
                .semantic_name("Connection status")
                .text_style(status_style),
        )
        .with_child(hints);

    Surface::window(Align::center(
        SemanticRegion::new("Sinomo launch progress", content)
            .role(SemanticsRole::GenericContainer),
    ))
    .theme(theme)
    .fill()
}

impl Widget for Shell {
    fn command(&mut self, ctx: &mut EventCtx, command: &Command<'_>) {
        if command.is(sui::REACTIVE_CHANGED) {
            if cluster::observation_visible(&self.shared) {
                self.schedule_cluster_observe_timer(ctx);
            } else if let Some(token) = self.cluster_observe_timer.take() {
                ctx.cancel_timer(token);
            }
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        // Starting/resuming observation must not depend on a platform redraw event.
        ctx.observe(&self.shared.signals().active_tab);
        ctx.observe(&self.shared.signals().cluster_structure);
        let size = constraints.max;
        let safe_size = crate::platform_safe_area_insets().inset(size);
        let compact = compact_chrome(safe_size.width);
        self.sync_tab_bar_density(compact);

        self.titlebar.measure(
            ctx,
            Constraints::tight(Size::new(safe_size.width, TOPBAR_HEIGHT)),
        );

        self.logo.measure(
            ctx,
            Constraints::tight(if compact {
                Size::ZERO
            } else {
                Size::new(LOGO_SIZE, LOGO_SIZE)
            }),
        );
        self.wordmark.measure(
            ctx,
            Constraints::tight(if compact {
                Size::ZERO
            } else {
                Size::new(brand_word_width() + 8.0, TOPBAR_HEIGHT)
            }),
        );

        let available_tabs = (status_pill_arrange_rect(
            Rect::new(0.0, 0.0, safe_size.width, safe_size.height),
            compact,
        )
        .x() - STATUS_GAP
            - tab_strip_x(0.0, compact))
        .max(0.0);
        let tab_strip = self.tabs.measure(
            ctx,
            Constraints::tight(Size::new(available_tabs, TAB_SEG_HEIGHT)),
        );
        self.tab_strip_rect = Rect::new(0.0, 0.0, tab_strip.width, TAB_SEG_HEIGHT);

        self.health_center.measure(
            ctx,
            Constraints::new(
                Size::new(COMPACT_STATUS_PILL_W, STATUS_PILL_H),
                Size::new(
                    (safe_size.width - EDGE_PAD * 2.0).max(0.0),
                    (safe_size.height - TOPBAR_HEIGHT / 2.0).max(0.0),
                ),
            ),
        );

        let body_h = (safe_size.height - TOPBAR_HEIGHT).max(0.0);
        self.body
            .measure(ctx, Constraints::tight(Size::new(safe_size.width, body_h)));

        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let safe_area = crate::platform_safe_area_insets();
        let bounds = Rect::new(
            bounds.x() + safe_area.left,
            bounds.y() + safe_area.top,
            (bounds.width() - safe_area.left - safe_area.right).max(0.0),
            (bounds.height() - safe_area.top - safe_area.bottom).max(0.0),
        );
        let bar_rect = Rect::new(bounds.x(), bounds.y(), bounds.width(), TOPBAR_HEIGHT);
        self.titlebar.arrange(ctx, bar_rect);
        let compact = compact_chrome(bounds.width());

        let logo_y = bounds.y() + (TOPBAR_HEIGHT - LOGO_SIZE) / 2.0;
        self.logo.arrange(
            ctx,
            if compact {
                Rect::ZERO
            } else {
                Rect::new(bounds.x() + EDGE_PAD, logo_y, LOGO_SIZE, LOGO_SIZE)
            },
        );
        if compact {
            self.wordmark.arrange(ctx, Rect::ZERO);
        } else {
            self.wordmark.arrange(
                ctx,
                Rect::new(
                    bounds.x() + EDGE_PAD + LOGO_SIZE + 12.0,
                    bounds.y(),
                    brand_word_width() + 8.0,
                    TOPBAR_HEIGHT,
                ),
            );
        }

        // Tab strip begins after the brand mark + wordmark.
        let seg_x = tab_strip_x(bounds.x(), compact);
        let seg_y = bounds.y() + (TOPBAR_HEIGHT - TAB_SEG_HEIGHT) / 2.0;
        let status_rect = status_pill_arrange_rect(bounds, compact);
        let status_x = status_rect.x();
        let tab_right = status_x - STATUS_GAP;
        let seg_w = (tab_right - seg_x).max(0.0);
        self.tab_strip_rect = Rect::new(seg_x, seg_y, seg_w, TAB_SEG_HEIGHT);
        self.tabs.arrange(ctx, self.tab_strip_rect);

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
        self.body.arrange(ctx, self.body_rect);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        // Content area background, then either the tab content or a loading screen.
        ctx.fill_bounds(self.shared.theme().surfaces.window);
        self.body.paint(ctx);

        self.titlebar.paint(ctx);
        if !self.compact_tabs.get() {
            self.logo.paint(ctx);
        }
        if !self.compact_tabs.get() {
            self.wordmark.paint(ctx);
        }

        // Pinned destinations and closable workspace tabs. Only meaningful once the tabs exist; while connecting the
        // content is `None`, but the strip is still shown so the chrome is stable.
        self.tabs.paint(ctx);

        self.health_center.paint(ctx);
        if self.shared.is_ready() {
            let app = self.shared.borrow();
            if !app
                .updates
                .rendered_ready
                .swap(true, std::sync::atomic::Ordering::AcqRel)
                && let Some(waker) = &app.waker
            {
                // The last startup worker may have completed before this
                // first paint. Wake the readiness owner for this final edge.
                waker.wake();
            }
        }
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        #[cfg(all(target_os = "android", feature = "mobile"))]
        if ctx.phase() == EventPhase::Target
            && let Event::Window(WindowEvent::Focused(interactive)) = event
        {
            crate::mobile_runtime::set_window_focused(*interactive);
            if *interactive {
                self.schedule_connect_status_timer(ctx);
                self.schedule_health_probe_timer(ctx);
                self.schedule_cluster_observe_timer(ctx);
                self.schedule_evolution_refresh_timer(ctx);
                if let Some(waker) = self.shared.borrow().waker.clone() {
                    waker.wake();
                }
            } else if self.shared.is_ready() {
                for scope in self.shared.chat_scopes() {
                    scope.chat_mut().sync_media_setup_visibility(false);
                }
            }
        }
        if let Event::Keyboard(key) = event
            && matches!(ctx.phase(), EventPhase::Target | EventPhase::Bubble)
            && key.state == KeyState::Pressed
            && !key.repeat
            && key.key == "BrowserBack"
            && self.handle_navigation_back()
        {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
            ctx.request_paint();
            ctx.set_handled();
            return;
        }
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            Event::Window(WindowEvent::Resized(size))
                if size.width.is_finite()
                    && size.height.is_finite()
                    && size.width > 0.0
                    && size.height > 0.0 =>
            {
                self.shared.borrow_mut().window_placement.size = Some([size.width, size.height]);
            }
            Event::Window(WindowEvent::Moved(point))
                if point.x.is_finite() && point.y.is_finite() =>
            {
                self.shared.borrow_mut().window_placement.position = Some([point.x, point.y]);
            }
            _ => {}
        }
        if let Event::Window(WindowEvent::Occluded(occluded)) = event {
            self.window_occluded.set(*occluded);
            if *occluded && let Some(token) = self.cluster_observe_timer.take() {
                ctx.cancel_timer(token);
            }
            if !occluded && cluster::observation_visible(&self.shared) {
                cluster::request_observation(&self.shared);
                self.schedule_cluster_observe_timer(ctx);
            }
        }
        if let Event::Window(window_event) = event
            && self.handle_external_file_window_event(window_event)
        {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
            ctx.request_paint();
            ctx.set_handled();
            return;
        }
        // Loading-screen heartbeat: repaint once a second while connecting so the
        // per-step elapsed time stays live (and a stalled step becomes obvious).
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.connect_status_timer.get() == Some(*token)
        {
            self.connect_status_timer.set(None);
            if crate::mobile_runtime::interactive() && !self.shared.is_ready() {
                self.shared
                    .publish_loading_stall_lines(loading_stall_lines_for(&self.shared));
                self.schedule_connect_status_timer(ctx);
            }
            return;
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.health_probe_timer.get() == Some(*token)
        {
            self.health_probe_timer.set(None);
            if crate::mobile_runtime::interactive() && self.shared.request_connection_probe(false) {
                ctx.request_semantics();
            }
            self.schedule_health_probe_timer(ctx);
            return;
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.cluster_observe_timer.get() == Some(*token)
        {
            self.cluster_observe_timer.set(None);
            if crate::mobile_runtime::interactive()
                && !self.window_occluded.get()
                && cluster::observation_visible(&self.shared)
                && cluster::request_observation(&self.shared)
            {
                ctx.request_semantics();
            }
            self.schedule_cluster_observe_timer(ctx);
            return;
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self.evolution_refresh_timer.get() == Some(*token)
        {
            self.evolution_refresh_timer.set(None);
            if crate::mobile_runtime::interactive()
                && !self.window_occluded.get()
                && self.shared.active_tab() == Tab::Evolution
            {
                evolution::request_refresh(&self.shared);
            }
            self.schedule_evolution_refresh_timer(ctx);
            return;
        }
        if matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            self.schedule_connect_status_timer(ctx);
            self.schedule_health_probe_timer(ctx);
            self.schedule_cluster_observe_timer(ctx);
            self.schedule_evolution_refresh_timer(ctx);
            self.sync_render_options(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Window, ctx.bounds());
        node.name = Some("Sinomo".to_string());
        ctx.push(node);

        self.titlebar.semantics(ctx);
        if !self.compact_tabs.get() {
            self.logo.semantics(ctx);
        }
        if !self.compact_tabs.get() {
            self.wordmark.semantics(ctx);
        }
        self.tabs.semantics(ctx);
        self.health_center.semantics(ctx);
        self.body.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.titlebar);
        if !self.compact_tabs.get() {
            self.logo.visit_children(visitor);
        }
        visitor.visit(&self.wordmark);
        visitor.visit(&self.tabs);
        visitor.visit(&self.body);
        // Health is painted last and must also be hit-tested last so its open popover receives
        // actions instead of the tab content visually underneath it.
        visitor.visit(&self.health_center);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.titlebar);
        if !self.compact_tabs.get() {
            self.logo.visit_children_mut(visitor);
        }
        visitor.visit(&mut self.wordmark);
        // Forward to the tab strip so pointer / keyboard / focus reach the tabs.
        visitor.visit(&mut self.tabs);
        // CRITICAL: forward to the body pod so events/focus reach the active surface.
        visitor.visit(&mut self.body);
        visitor.visit(&mut self.health_center);
    }
}

impl Shell {
    /// Extra loading-screen lines once the current launch step has stalled: the elapsed
    /// time after [`LOADING_STALL_AFTER_SECS`], plus where to find technical details after
    /// [`LOADING_LOG_HINT_AFTER_SECS`].
    #[cfg(test)]
    fn loading_stall_lines(&self) -> [Option<String>; 2] {
        loading_stall_lines_for(&self.shared)
    }
}

fn loading_stall_lines_for(shared: &Shared) -> [Option<String>; 2] {
    let Some(elapsed) = shared.status_elapsed_secs() else {
        return [None, None];
    };
    if elapsed < LOADING_STALL_AFTER_SECS {
        return [None, None];
    }
    let stall = Some(format!("Still working — {elapsed}s on this step"));
    let log_hint = (elapsed >= LOADING_LOG_HINT_AFTER_SECS).then(|| {
        let log_path = {
            let state = shared.borrow();
            state.data_dir.root().join("logs").join("app.log")
        };
        format!("Details: {}", log_path.display())
    });
    [stall, log_hint]
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

type HealthCenterKey = HealthStructureKey;

/// Controlled stock-SUI health popover. Recovery authority remains in each owning app surface.
struct HealthCenter {
    shared: Shared,
    open: Rc<Cell<bool>>,
    focused_action: Rc<Cell<usize>>,
    action_error: Rc<RefCell<Option<String>>>,
    child: SingleChild,
}

impl HealthCenter {
    fn new(shared: Shared, compact: bool) -> Self {
        let open = Rc::new(Cell::new(false));
        let focused_action = Rc::new(Cell::new(0));
        let action_error = Rc::new(RefCell::new(None));
        let source = shared
            .signals()
            .health_structure
            .select_named("shell health center structure", Clone::clone);
        let build_shared = shared.clone();
        let build_open = Rc::clone(&open);
        let build_focus = Rc::clone(&focused_action);
        let build_error = Rc::clone(&action_error);
        let child = RebuildOnChange::key_from(source, move |key| {
            build_health_center_tree(
                build_shared.clone(),
                key,
                compact,
                Rc::clone(&build_open),
                Rc::clone(&build_focus),
                Rc::clone(&build_error),
            )
        });
        Self {
            shared,
            open,
            focused_action,
            action_error,
            child: SingleChild::new(child),
        }
    }

    fn move_focus(&self, delta: isize, count: usize) {
        let next = if count == 0 {
            0
        } else {
            (self.focused_action.get() as isize + delta).rem_euclid(count as isize) as usize
        };
        self.focused_action.set(next);
    }
}

impl Widget for HealthCenter {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Capture || !self.open.get() {
            return;
        }
        let Event::Keyboard(key) = event else { return };
        if key.state != KeyState::Pressed {
            return;
        }
        let actions = all_health_actions(&self.shared);
        match key.key.as_str() {
            "Escape" => {
                self.open.set(false);
                self.focused_action.set(0);
                self.action_error.borrow_mut().take();
                self.shared.publish_health_action_error(None);
            }
            "ArrowDown" | "ArrowRight" => self.move_focus(1, actions.len()),
            "ArrowUp" | "ArrowLeft" => self.move_focus(-1, actions.len()),
            "Home" => self.focused_action.set(0),
            "End" => self.focused_action.set(actions.len().saturating_sub(1)),
            "Enter" | " " => {
                if let Some(action) = actions.get(self.focused_action.get()).cloned() {
                    activate_health_action_with_ctx(
                        &self.shared,
                        action,
                        &self.open,
                        &self.focused_action,
                        &self.action_error,
                        ctx,
                    );
                }
            }
            _ => return,
        }
        request_health_refresh(ctx);
        ctx.set_handled();
    }

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
        self.child.visit_children(visitor);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.child.visit_children_mut(visitor);
    }
}

fn build_health_center_tree(
    shared: Shared,
    key: &HealthCenterKey,
    compact: bool,
    open: Rc<Cell<bool>>,
    focused: Rc<Cell<usize>>,
    error: Rc<RefCell<Option<String>>>,
) -> WidgetPod {
    let theme = key.theme;
    let tone = health_status_tone(key);
    let icon = health_status_icon(key);
    let trigger = SwitchView::new()
        .selected(usize::from(compact))
        .with_child(
            Button::new(compact_health_line(&key.status.label, 22))
                .semantic_name(key.status.label.clone())
                .description(format!("Open Health and recovery. {}", key.status.detail))
                .icon(icon)
                .appearance(ButtonAppearance::Tonal)
                .tone(tone)
                .theme(theme)
                .min_width(STATUS_PILL_W)
                .min_height(STATUS_PILL_H)
                .padding(Insets {
                    left: 10.0,
                    top: 2.0,
                    right: 10.0,
                    bottom: 2.0,
                })
                .enabled(key.ready),
        )
        .with_child(
            IconButton::new(icon, key.status.label.clone())
                .description(format!("Open Health and recovery. {}", key.status.detail))
                .appearance(ButtonAppearance::Tonal)
                .tone(tone)
                .theme(theme)
                .size(COMPACT_STATUS_PILL_W)
                .icon_size(13.0)
                .selected(key.status.degraded)
                .enabled(key.ready),
        );
    if !key.ready {
        return WidgetPod::new(HealthReadOnlyStatus::new(
            trigger,
            key.status.label.clone(),
            key.status.detail.clone(),
        ));
    }

    let content = build_health_content(
        shared,
        key,
        Rc::clone(&open),
        Rc::clone(&focused),
        Rc::clone(&error),
    );
    let open_reader = Rc::clone(&open);
    let changed_open = Rc::clone(&open);
    let changed_focus = Rc::clone(&focused);
    let changed_error = Rc::clone(&error);
    WidgetPod::new(
        Popover::new("Health and recovery", trigger, content)
            .theme(theme)
            .open_when(move || open_reader.get())
            .on_open_change(move |value| {
                changed_open.set(value);
                changed_focus.set(0);
                if !value {
                    changed_error.borrow_mut().take();
                }
            })
            .alignment(PopoverAlignment::End),
    )
}

fn build_health_content(
    shared: Shared,
    key: &HealthCenterKey,
    open: Rc<Cell<bool>>,
    focused: Rc<Cell<usize>>,
    error: Rc<RefCell<Option<String>>>,
) -> impl Widget + use<> {
    let theme = key.theme;
    let palette = Palette::from_theme(&theme, widgets::theme_is_dark(&shared));
    let summary = if key.issues.is_empty() {
        key.status.detail.clone()
    } else {
        format!(
            "{} affected {}",
            key.issues.len(),
            if key.issues.len() == 1 {
                "surface"
            } else {
                "surfaces"
            }
        )
    };
    let header = HealthConnectionStatus::new(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(3.0)
            .with_child(
                Flex::horizontal()
                    .align_items(Alignment::Center)
                    .gap(8.0)
                    .with_item(
                        Label::new("Health and recovery").text_style(health_text_style(
                            &theme,
                            theme.text.base,
                            FontWeight::SEMIBOLD,
                            palette.text,
                        )),
                        FlexItem::flex(1.0),
                    )
                    .with_child(
                        StatusBadge::new(key.connection_label.clone())
                            .icon(health_status_icon(key))
                            .tone(health_status_tone(key))
                            .theme(theme),
                    ),
            )
            .with_child(Label::new(compact_health_line(&summary, 72)).text_style(
                health_text_style(
                    &theme,
                    theme.text.sm,
                    FontWeight::NORMAL,
                    palette.text_muted,
                ),
            )),
        key.connection_label.clone(),
        key.connection_detail.clone(),
        key.connection_pending,
    );

    let mut list = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    let mut action_index = 0;
    if key.issues.is_empty() {
        let empty = Surface::panel(
            Flex::vertical()
                .align_items(Alignment::Stretch)
                .gap(3.0)
                .with_child(
                    Label::new("All monitored surfaces are operational.").text_style(
                        health_text_style(
                            &theme,
                            theme.text.sm,
                            FontWeight::SEMIBOLD,
                            palette.ok_text,
                        ),
                    ),
                )
                .with_child(
                    Label::new(compact_health_line(&key.status.detail, 76)).text_style(
                        health_text_style(
                            &theme,
                            theme.text.sm,
                            FontWeight::NORMAL,
                            palette.text_muted,
                        ),
                    ),
                ),
        )
        .appearance(SurfaceAppearance::Soft)
        .tone(SemanticTone::Success)
        .padding(Insets::all(10.0))
        .fill_width(true)
        .theme(theme);
        list.push(
            SemanticRegion::new("All systems operational", empty)
                .description(key.status.detail.clone())
                .role(SemanticsRole::ListItem),
        );
    } else {
        for issue in &key.issues {
            let mut buttons = Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .align_items(Alignment::Center)
                .gap(6.0);
            for action in health_actions_for_issue(&shared, issue) {
                buttons.push_item(
                    SizedBox::new()
                        .height(HEALTH_ACTION_H)
                        .child(build_health_action_control(
                            shared.clone(),
                            action,
                            action_index,
                            theme,
                            Rc::clone(&open),
                            Rc::clone(&focused),
                            Rc::clone(&error),
                        )),
                    FlexItem::flex(1.0).basis(104.0).min_width(104.0),
                );
                action_index += 1;
            }
            let badge = StatusBadge::new(if issue.needs_authorization() {
                "Authorization"
            } else {
                "Needs attention"
            })
            .icon(if issue.needs_authorization() {
                IconGlyph::Unlock
            } else {
                IconGlyph::Alert
            })
            .tone(SemanticTone::Warning)
            .theme(theme);
            let card = Surface::panel(
                Flex::vertical()
                    .align_items(Alignment::Stretch)
                    .gap(6.0)
                    .with_child(
                        Flex::horizontal()
                            .align_items(Alignment::Center)
                            .gap(8.0)
                            .with_item(
                                Label::new(issue.title.clone()).text_style(health_text_style(
                                    &theme,
                                    theme.text.sm,
                                    FontWeight::SEMIBOLD,
                                    palette.text,
                                )),
                                FlexItem::flex(1.0),
                            )
                            .with_child(badge),
                    )
                    .with_child(
                        Label::new(compact_health_line(&issue.detail, 88)).text_style(
                            health_text_style(
                                &theme,
                                theme.text.sm,
                                FontWeight::NORMAL,
                                palette.text_muted,
                            ),
                        ),
                    )
                    .with_child(buttons),
            )
            .appearance(SurfaceAppearance::Soft)
            .tone(SemanticTone::Warning)
            .padding(Insets::all(10.0))
            .fill_width(true)
            .theme(theme);
            list.push(
                SemanticRegion::new(issue.title.clone(), card)
                    .description(issue.detail.clone())
                    .role(SemanticsRole::ListItem),
            );
        }
    }

    let issues = SemanticRegion::new("Affected surfaces", list).role(SemanticsRole::List);
    let mut body = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(12.0)
        .with_child(header)
        .with_child(
            PanelSection::new("Affected surfaces", issues)
                .gap(8.0)
                .title_style(health_text_style(
                    &theme,
                    theme.text.sm,
                    FontWeight::SEMIBOLD,
                    palette.text_muted,
                ))
                .theme(theme),
        );
    if let Some(message) = key.action_error.clone() {
        let callout = Surface::panel(
            Flex::vertical()
                .align_items(Alignment::Stretch)
                .gap(2.0)
                .with_child(
                    Label::new("Recovery action failed").text_style(health_text_style(
                        &theme,
                        theme.text.sm,
                        FontWeight::SEMIBOLD,
                        palette.danger_text,
                    )),
                )
                .with_child(Label::new(compact_health_line(&message, 76)).text_style(
                    health_text_style(
                        &theme,
                        theme.text.sm,
                        FontWeight::NORMAL,
                        palette.danger_text,
                    ),
                )),
        )
        .appearance(SurfaceAppearance::Soft)
        .tone(SemanticTone::Danger)
        .padding(Insets::all(9.0))
        .fill_width(true)
        .theme(theme);
        body.push(
            SemanticRegion::new("Recovery action failed", callout)
                .description(message)
                .role(SemanticsRole::Text),
        );
    }
    body.push(Flex::horizontal().with_item(
        build_health_action_control(
            shared,
            HealthActionItem {
                kind: HealthActionKind::OpenLogs,
                label: "Open app log".to_string(),
                enabled: true,
            },
            action_index,
            theme,
            open,
            focused,
            error,
        ),
        FlexItem::new().min_width(132.0),
    ));

    let width = (HEALTH_POPOVER_W
        - theme.metrics.popover_padding.left
        - theme.metrics.popover_padding.right)
        .max(240.0);
    let issue_space = if key.issues.is_empty() {
        0.0
    } else {
        key.issues.len() as f32 * HEALTH_ISSUE_ESTIMATED_H
    };
    let error_space = if key.action_error.is_some() {
        52.0
    } else {
        0.0
    };
    let height = (HEALTH_CONTENT_MIN_H + issue_space + error_space).min(HEALTH_CONTENT_MAX_H);
    SizedBox::new().width(width).height(height).child(
        ScrollView::vertical(body)
            .name("Health and recovery details")
            .theme(theme)
            .viewport_width_hint(true),
    )
}

fn health_status_tone(key: &HealthCenterKey) -> SemanticTone {
    if key.status.degraded {
        SemanticTone::Warning
    } else if key.status.connected_daemon {
        SemanticTone::Success
    } else {
        SemanticTone::Info
    }
}

fn health_status_icon(key: &HealthCenterKey) -> IconGlyph {
    if key.connection_pending || matches!(key.connection_phase, ConnectionPhase::Reconnecting) {
        IconGlyph::Hourglass
    } else if key.status.degraded {
        IconGlyph::Alert
    } else if key.status.connected_daemon {
        IconGlyph::Check
    } else {
        IconGlyph::Storage
    }
}

fn health_text_style(
    theme: &sui::DefaultTheme,
    token: sui::ThemeTextToken,
    weight: FontWeight,
    color: sui::Color,
) -> sui::TextStyle {
    let mut style = theme.body_text_style();
    style.font_size = token.size;
    style.line_height = token.line_height;
    style.weight = weight;
    style.color = color;
    style
}

fn retry_and_open_actions(surface: HealthSurface, label: &str) -> Vec<HealthActionItem> {
    vec![
        HealthActionItem {
            kind: HealthActionKind::Retry(surface),
            label: format!("Retry {label}"),
            enabled: true,
        },
        HealthActionItem {
            kind: HealthActionKind::Open(surface),
            label: format!("Open {label}"),
            enabled: true,
        },
    ]
}

fn health_actions_for_issue(shared: &Shared, issue: &HealthIssue) -> Vec<HealthActionItem> {
    let pending = shared.connection_health().is_pending();
    let mut actions = match issue.surface {
        HealthSurface::Connection => vec![
            HealthActionItem {
                kind: HealthActionKind::Reconnect,
                label: if pending {
                    "Reconnecting…"
                } else {
                    "Reconnect"
                }
                .to_string(),
                enabled: !pending,
            },
            HealthActionItem {
                kind: HealthActionKind::Open(HealthSurface::Settings(
                    settings::SettingsSection::System,
                )),
                label: "Connection settings".to_string(),
                enabled: true,
            },
        ],
        HealthSurface::Chat => retry_and_open_actions(HealthSurface::Chat, "Chat"),
        HealthSurface::Files => retry_and_open_actions(HealthSurface::Files, "Files"),
        HealthSurface::Cluster => retry_and_open_actions(HealthSurface::Cluster, "Cluster"),
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
            HealthSurface::Settings(settings::SettingsSection::Providers) => {
                "Authorize Provider".to_string()
            }
            HealthSurface::Settings(section) => format!("Authorize {}", section.title()),
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

fn all_health_actions(shared: &Shared) -> Vec<HealthActionItem> {
    let mut actions = shared
        .health_issues()
        .iter()
        .flat_map(|issue| health_actions_for_issue(shared, issue))
        .collect::<Vec<_>>();
    actions.push(HealthActionItem {
        kind: HealthActionKind::OpenLogs,
        label: "Open app log".to_string(),
        enabled: true,
    });
    actions
}

fn build_health_action_control(
    shared: Shared,
    action: HealthActionItem,
    index: usize,
    theme: sui::DefaultTheme,
    open: Rc<Cell<bool>>,
    focused: Rc<Cell<usize>>,
    error: Rc<RefCell<Option<String>>>,
) -> HealthActionControl {
    let idle = stock_health_action_button(
        shared.clone(),
        action.clone(),
        index,
        false,
        theme,
        Rc::clone(&open),
        Rc::clone(&focused),
        Rc::clone(&error),
    );
    let selected = stock_health_action_button(
        shared.clone(),
        action.clone(),
        index,
        true,
        theme,
        Rc::clone(&open),
        Rc::clone(&focused),
        Rc::clone(&error),
    );
    let selected_reader = Rc::clone(&focused);
    let child = SwitchView::new()
        .selected_when(move || usize::from(selected_reader.get() == index))
        .with_child(idle)
        .with_child(selected);
    let callback_shared = shared;
    let callback_action = action.clone();
    let callback_open = Rc::clone(&open);
    let callback_focus = Rc::clone(&focused);
    let callback_error = Rc::clone(&error);
    HealthActionControl::new(
        action.label,
        action.enabled,
        index,
        focused,
        child,
        move |ctx| {
            activate_health_action_with_ctx(
                &callback_shared,
                callback_action.clone(),
                &callback_open,
                &callback_focus,
                &callback_error,
                ctx,
            );
        },
    )
}

fn stock_health_action_button(
    shared: Shared,
    action: HealthActionItem,
    index: usize,
    selected: bool,
    theme: sui::DefaultTheme,
    open: Rc<Cell<bool>>,
    focused: Rc<Cell<usize>>,
    error: Rc<RefCell<Option<String>>>,
) -> Button {
    let (appearance, tone) = health_action_visuals(&action.kind, selected);
    let icon = match action.kind {
        HealthActionKind::Reconnect | HealthActionKind::Retry(_) => IconGlyph::History,
        HealthActionKind::Authorize(_) => IconGlyph::Unlock,
        HealthActionKind::Open(_) => IconGlyph::ChevronRight,
        HealthActionKind::OpenLogs => IconGlyph::FileText,
    };
    let callback_action = action.clone();
    Button::new(action.label)
        .icon(icon)
        .appearance(appearance)
        .tone(tone)
        .theme(theme)
        .min_width(0.0)
        .enabled(action.enabled)
        .on_press_with_ctx(move |ctx| {
            focused.set(index);
            activate_health_action_with_ctx(
                &shared,
                callback_action.clone(),
                &open,
                &focused,
                &error,
                ctx,
            );
        })
}

fn health_action_visuals(
    kind: &HealthActionKind,
    selected: bool,
) -> (ButtonAppearance, SemanticTone) {
    match kind {
        HealthActionKind::Reconnect | HealthActionKind::Retry(_) => (
            if selected {
                ButtonAppearance::Filled
            } else {
                ButtonAppearance::Tonal
            },
            SemanticTone::Info,
        ),
        HealthActionKind::Authorize(_) => (
            if selected {
                ButtonAppearance::Filled
            } else {
                ButtonAppearance::Tonal
            },
            SemanticTone::Warning,
        ),
        HealthActionKind::Open(_) => (
            if selected {
                ButtonAppearance::Tonal
            } else {
                ButtonAppearance::Ghost
            },
            if selected {
                SemanticTone::Accent
            } else {
                SemanticTone::Neutral
            },
        ),
        HealthActionKind::OpenLogs => (
            if selected {
                ButtonAppearance::Tonal
            } else {
                ButtonAppearance::Outline
            },
            if selected {
                SemanticTone::Accent
            } else {
                SemanticTone::Neutral
            },
        ),
    }
}

fn activate_health_action_with_ctx(
    shared: &Shared,
    action: HealthActionItem,
    open: &Rc<Cell<bool>>,
    focused: &Rc<Cell<usize>>,
    error: &Rc<RefCell<Option<String>>>,
    ctx: &mut EventCtx,
) {
    if activate_health_action(shared, action, error) {
        open.set(false);
        focused.set(0);
    }
    shared.publish_health_action_error(error.borrow().clone());
    request_health_refresh(ctx);
}

fn activate_health_action(
    shared: &Shared,
    action: HealthActionItem,
    error: &Rc<RefCell<Option<String>>>,
) -> bool {
    if !action.enabled {
        return false;
    }
    error.borrow_mut().take();
    match action.kind {
        HealthActionKind::Reconnect => {
            shared.request_connection_probe(true);
            false
        }
        HealthActionKind::Retry(surface) => {
            retry_health_surface(shared, surface);
            matches!(surface, HealthSurface::Settings(_))
        }
        HealthActionKind::Authorize(surface) => {
            authorize_health_surface(shared, surface);
            true
        }
        HealthActionKind::Open(surface) => {
            open_health_surface(shared, surface);
            true
        }
        HealthActionKind::OpenLogs => {
            if let Err(message) = open_app_log(shared) {
                tracing::warn!(error = %message, "failed to open app log");
                *error.borrow_mut() = Some(message);
            }
            false
        }
    }
}

fn open_health_surface(shared: &Shared, surface: HealthSurface) {
    let tab = match surface {
        HealthSurface::Connection => {
            if shared.is_ready() {
                shared
                    .settings_mut()
                    .open(settings::SettingsSection::System);
            }
            Tab::Settings
        }
        HealthSurface::Chat => Tab::Sinomo,
        HealthSurface::Files => Tab::Files,
        HealthSurface::Cluster => Tab::Cluster,
        HealthSurface::Settings(section) => {
            if shared.is_ready() {
                shared.settings_mut().open(section);
            }
            Tab::Settings
        }
    };
    shared.set_active_tab(tab);
}

fn retry_health_surface(shared: &Shared, surface: HealthSurface) {
    match surface {
        HealthSurface::Connection => {
            shared.request_connection_probe(true);
        }
        HealthSurface::Chat => {
            let waker = { shared.borrow().waker.clone() };
            shared.chat_mut().start_session_list_load(waker);
        }
        HealthSurface::Files => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().retry(waker);
        }
        HealthSurface::Cluster => {
            cluster::request_refresh(shared);
        }
        HealthSurface::Settings(_) => open_health_surface(shared, surface),
    }
}

fn authorize_health_surface(shared: &Shared, surface: HealthSurface) {
    let section = match surface {
        HealthSurface::Settings(settings::SettingsSection::Providers) => {
            settings::SettingsSection::Providers
        }
        _ => settings::SettingsSection::System,
    };
    open_health_surface(shared, HealthSurface::Settings(section));
}

fn request_health_refresh(ctx: &mut EventCtx) {
    ctx.request_measure();
    ctx.request_arrange();
    ctx.request_semantics();
    ctx.request_paint();
}

/// Non-paint semantic wrapper around stock typed Buttons; adds roving selected state.
struct HealthActionControl {
    label: String,
    enabled: bool,
    index: usize,
    focused: Rc<Cell<usize>>,
    on_activate: Box<dyn FnMut(&mut EventCtx)>,
    child: SingleChild,
}

impl HealthActionControl {
    fn new<W, F>(
        label: String,
        enabled: bool,
        index: usize,
        focused: Rc<Cell<usize>>,
        child: W,
        on_activate: F,
    ) -> Self
    where
        W: Widget + 'static,
        F: FnMut(&mut EventCtx) + 'static,
    {
        Self {
            label,
            enabled,
            index,
            focused,
            on_activate: Box::new(on_activate),
            child: SingleChild::new(child),
        }
    }
}

impl Widget for HealthActionControl {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target || !self.enabled {
            return;
        }
        let activate = matches!(
            event,
            Event::Keyboard(key)
                if ctx.is_focused()
                    && key.state == KeyState::Pressed
                    && matches!(key.key.as_str(), "Enter" | " ")
        ) || matches!(
            event,
            Event::Semantics(action)
                if action.target == ctx.widget_id()
                    && action.action == SemanticsActionRequest::Activate
        );
        if activate {
            self.focused.set(self.index);
            (self.on_activate)(ctx);
            ctx.set_handled();
        }
    }

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
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Button, ctx.bounds());
        node.name = Some(self.label.clone());
        node.state.disabled = !self.enabled;
        node.state.focused = ctx.is_focused();
        node.state.selected = self.focused.get() == self.index;
        if self.enabled {
            node.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
        }
        ctx.push(node);
    }

    fn accepts_focus(&self) -> bool {
        self.enabled
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, value: bool) {
        if value {
            self.focused.set(self.index);
            ctx.request_measure();
        }
        ctx.request_paint();
        ctx.request_semantics();
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.child.visit_children(visitor);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.child.visit_children_mut(visitor);
    }
}

struct HealthConnectionStatus {
    label: String,
    detail: String,
    busy: bool,
    child: SingleChild,
}

impl HealthConnectionStatus {
    fn new<W>(child: W, label: String, detail: String, busy: bool) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            label,
            detail,
            busy,
            child: SingleChild::new(child),
        }
    }
}

impl Widget for HealthConnectionStatus {
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
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Text, ctx.bounds());
        node.name = Some("Connection status".to_string());
        node.description = Some(self.detail.clone());
        node.value = Some(SemanticsValue::Text(self.label.clone()));
        node.state.busy = self.busy;
        ctx.push(node);
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.child.visit_children(visitor);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.child.visit_children_mut(visitor);
    }
}

struct HealthReadOnlyStatus {
    label: String,
    detail: String,
    child: SingleChild,
}

impl HealthReadOnlyStatus {
    fn new<W>(child: W, label: String, detail: String) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            label,
            detail,
            child: SingleChild::new(child),
        }
    }
}

impl Widget for HealthReadOnlyStatus {
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
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Text, ctx.bounds());
        node.name = Some(self.label.clone());
        node.description = Some(self.detail.clone());
        node.value = Some(SemanticsValue::Text("connecting".to_string()));
        node.state.busy = true;
        ctx.push(node);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        self.child.visit_children(visitor);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        self.child.visit_children_mut(visitor);
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

fn compact_chrome(width: f32) -> bool {
    // Treat the breakpoint itself as compact. Window managers and test surfaces commonly land on
    // the exact logical width, and keeping the desktop chrome there leaves no room for enlarged
    // text beside the health affordance.
    width <= COMPACT_CHROME_WIDTH
}

fn tab_strip_x(bounds_x: f32, compact: bool) -> f32 {
    if compact {
        bounds_x + EDGE_PAD
    } else {
        bounds_x + EDGE_PAD + LOGO_SIZE + 12.0 + brand_word_width() + 26.0
    }
}

/// Position reserved for the status affordance during arrange. Compact title bars retain the full
/// status in semantics while the visual treatment becomes a small trailing badge. The tab strip
/// scrolls horizontally through the space before it.
fn status_pill_arrange_rect(bounds: Rect, compact: bool) -> Rect {
    if compact {
        Rect::new(
            bounds.max_x() - EDGE_PAD - COMPACT_STATUS_PILL_W,
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

/// Width reserved for the "Sinomo" wordmark. This includes enough room for the supported 130%
/// text scale so the brand never wraps into a second title-bar line.
fn brand_word_width() -> f32 {
    84.0
}

#[cfg(test)]
fn shell_status(shared: &Shared) -> ShellStatus {
    shared.signals().health_structure.get().status
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
        assert_eq!(Tab::Sinomo.title(), "Chat");
        assert_eq!(
            Tab::ALL.map(Tab::title),
            ["Chat", "Cluster", "Evolution", "Files", "Settings"]
        );
        assert_eq!(tab_icon(Tab::Sinomo).name(), "messages-square");
        assert_eq!(tab_icon(Tab::Cluster).name(), "cloudy");
        assert_eq!(tab_icon(Tab::Evolution).name(), "git-branch");
        assert_eq!(tab_icon(Tab::Files).name(), "folders");
        assert_eq!(tab_icon(Tab::Settings).name(), "settings");
    }

    #[test]
    fn platform_back_returns_settings_detail_to_menu() {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Settings;
        let shared = Shared::new(state);
        shared.settings_mut().open(SettingsSection::Providers);
        let shell = Shell::new(shared.clone());

        assert!(shell.handle_navigation_back());
        assert_eq!(shared.settings().selected, None);
        assert!(!shell.handle_navigation_back());
    }

    #[test]
    fn platform_back_bubbles_from_focused_child_to_settings_navigation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Settings;
        let shared = Shared::new(state);
        shared
            .settings_mut()
            .open(SettingsSection::RemoteManagement);

        let mut runtime = crate::build_runtime(shared.clone())?;
        let window_id = runtime.window_ids()[0];
        let output = runtime.render(window_id)?;
        let focused_child = output
            .semantics
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button && node.actions.contains(&SemanticsAction::Focus)
            })
            .expect("settings shell should expose a focusable child")
            .id;
        assert!(runtime.handle_semantics_action(
            window_id,
            focused_child,
            SemanticsActionRequest::Focus,
        )?);

        assert!(runtime.dispatch_event(
            window_id,
            Event::Keyboard(sui::KeyboardEvent::new("BrowserBack", KeyState::Pressed,)),
        )?);
        assert_eq!(shared.settings().selected, None);
        Ok(())
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
        assert_eq!(shared.settings().selected, Some(SettingsSection::System));

        let snapshot = window.snapshot()?;
        let pinned = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| node.description.as_deref() == Some("Pinned Settings tab"))
            .expect("pinned settings semantics");
        assert!(pinned.state.selected);

        Ok(())
    }

    #[test]
    fn shell_tabbar_switch_preserves_widget_animation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut state = AppState::ready(backend);
        state.active_tab = Tab::Sinomo;
        let shared = Shared::new(state);
        let first = shared
            .new_workspace_tab(crate::state::WorkspaceTabKind::Chat)
            .unwrap();
        let second = shared
            .new_workspace_tab(crate::state::WorkspaceTabKind::Chat)
            .unwrap();
        shared.select_workspace_tab(first);
        let mut runtime = crate::build_runtime(shared.clone())?;
        let window_id = runtime.window_ids()[0];
        runtime.handle_event(
            window_id,
            Event::Window(WindowEvent::Resized(Size::new(1100.0, 760.0))),
        )?;
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
            tab_bar.bounds.max_x() - 60.0,
            tab_bar.bounds.y() + tab_bar.bounds.height() / 2.0,
        );
        click_runtime_point(&mut runtime, window_id, settings_point)?;
        assert_eq!(shared.borrow().active_workspace_tab, Some(second));

        runtime.handle_event(window_id, Event::Window(WindowEvent::RedrawRequested))?;
        runtime.tick(switch_duration * 0.5);
        dispatch_ready_events(&mut runtime)?;
        assert!(
            runtime.has_pending_animation_frames(window_id)?,
            "tab switch should keep its transition running after shell redraw"
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
            Some(SemanticsValue::Text("New chat".to_string()))
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
            Event::Pointer(sui::PointerEvent::new(sui::PointerEventKind::Move, point)),
        )?;

        let mut down = sui::PointerEvent::new(sui::PointerEventKind::Down, point);
        down.button = Some(sui::PointerButton::Primary);
        down.buttons = sui::PointerButtons::new(1);
        runtime.handle_event(window_id, Event::Pointer(down))?;

        let mut up = sui::PointerEvent::new(sui::PointerEventKind::Up, point);
        up.button = Some(sui::PointerButton::Primary);
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
        let is_descendant = |descendant: sui::WidgetId, ancestor: sui::WidgetId| {
            let mut current = Some(descendant);
            while let Some(id) = current {
                if id == ancestor {
                    return true;
                }
                current = snapshot
                    .accessibility
                    .nodes
                    .iter()
                    .find(|node| node.id == id)
                    .and_then(|node| node.parent);
            }
            false
        };
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
        let titlebar = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("Application title bar")
            })
            .expect("stock title-bar surface semantics");
        assert!(
            (titlebar.bounds.height() - TOPBAR_HEIGHT).abs() <= 0.5,
            "title-bar surface should own the shared chrome bounds: {:?}",
            titlebar.bounds
        );
        let wordmark = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text && node.name.as_deref() == Some("Sinomo wordmark")
            })
            .expect("stock wordmark label semantics");
        assert!(
            wordmark.bounds.height() <= TOPBAR_HEIGHT + 0.5,
            "wordmark should remain inside the title-bar line box: {:?}",
            wordmark.bounds
        );
        let health_title = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text
                    && node.name.as_deref() == Some("Health and recovery")
            })
            .expect("health center title semantics");
        assert!(
            health_title.bounds.width() > 120.0,
            "health title must retain a readable line width; bounds={:?}",
            health_title.bounds
        );
        let connection_status = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Text
                    && node.name.as_deref() == Some("Connection status")
            })
            .expect("connection progress semantics");
        assert!(
            connection_status.bounds.width() > 240.0,
            "health popover content must not collapse into its toolbar trigger; bounds={:?}",
            connection_status.bounds
        );
        assert!(
            is_descendant(connection_status.id, health_center.id),
            "connection status should remain inside the stock popover hierarchy"
        );
        let affected = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List
                    && node.name.as_deref() == Some("Affected surfaces")
            })
            .expect("affected surfaces list");
        assert!(
            is_descendant(affected.id, health_center.id),
            "affected surfaces should remain inside the stock popover hierarchy"
        );
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
        assert!(
            is_descendant(files_issue.id, affected.id),
            "issue rows should remain descendants of the affected-surfaces list"
        );
        let files_issue_bounds = files_issue.bounds;
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
            assert!(
                is_descendant(node.id, health_center.id),
                "{action} should remain inside the stock popover/scroll hierarchy"
            );
            assert!(
                node.bounds.height() <= 40.0,
                "compact recovery action {action} should not stretch vertically: {:?}",
                node.bounds
            );
            if action != "Open app log" {
                assert!(
                    node.bounds.y() >= files_issue_bounds.y() - 0.5
                        && node.bounds.max_y() <= files_issue_bounds.max_y() + 0.5,
                    "issue action {action} should stay inside its warning card: action={:?}, card={:?}",
                    node.bounds,
                    files_issue_bounds
                );
            }
        }

        let authorize_id = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Authorize Files")
            })
            .expect("Authorize Files semantic action")
            .id;
        window
            .root()
            .dispatch_event(Event::Semantics(sui::SemanticsEvent::new(
                authorize_id,
                SemanticsActionRequest::Activate,
            )))?;
        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(shared.settings().selected, Some(SettingsSection::System));

        let snapshot = window.snapshot()?;
        let pinned = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| node.description.as_deref() == Some("Pinned Settings tab"))
            .expect("pinned settings semantics");
        assert!(pinned.state.selected);

        Ok(())
    }

    #[test]
    fn compact_titlebar_keeps_attention_pill_trailing_and_tabs_scrollable() -> sui::Result<()> {
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

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let root = window.root();
        app.run_until_idle()?;

        // Exercise the exact breakpoint: this is a common snapped/window-test width and must not
        // fall back to the wider title-bar treatment.
        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
            COMPACT_CHROME_WIDTH,
            720.0,
        ))))?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        let titlebar = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("Application title bar")
            })
            .expect("stock title-bar surface semantics");
        assert!(
            (titlebar.bounds.height() - TOPBAR_HEIGHT).abs() <= 0.5,
            "compact title-bar surface should retain its stock height: {:?}",
            titlebar.bounds
        );
        assert!(
            !snapshot.accessibility.nodes.iter().any(|node| {
                node.role == SemanticsRole::Text && node.name.as_deref() == Some("Sinomo wordmark")
            }),
            "compact chrome should omit the hidden wordmark from semantics"
        );
        let navigation = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ScrollView
                    && node.name.as_deref() == Some("Primary navigation tabs")
            })
            .expect("shell navigation scroll semantics");
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
                    && node.description.as_deref().is_some_and(|description| {
                        description.starts_with("Open Health and recovery.")
                    })
            })
            .expect("health status control semantics");

        assert!(
            navigation.bounds.max_x() <= status.bounds.x(),
            "compact status badge {:?} should stay after navigation viewport {:?}",
            status.bounds,
            navigation.bounds
        );
        assert!(
            status.bounds.max_x() <= COMPACT_CHROME_WIDTH - EDGE_PAD + 0.5,
            "compact status badge should remain against the trailing inset: {:?}",
            status.bounds
        );
        assert!(
            tab_bar.bounds.width() <= navigation.bounds.width() + 0.5,
            "tabs should fit without overflow at the compact breakpoint: tabs={:?}, viewport={:?}",
            tab_bar.bounds,
            navigation.bounds
        );
        assert!(
            status.bounds.width() <= COMPACT_STATUS_PILL_W + 0.5,
            "compact status should not reserve the full pill width: {:?}",
            status.bounds
        );

        // Pinned destinations stay fixed while closable tabs overflow on a phone.
        shared.new_workspace_tab(crate::state::WorkspaceTabKind::Chat);
        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(320.0, 720.0))))?;
        app.run_until_idle()?;
        let narrow = window.snapshot()?;
        let navigation = narrow
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ScrollView
                    && node.name.as_deref() == Some("Primary navigation tabs")
            })
            .expect("narrow shell navigation scroll semantics");
        let tab_bar = narrow
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::TabBar && node.name.as_deref() == Some("shell-tabs")
            })
            .expect("narrow shell tab bar semantics");
        let status = narrow
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.description.as_deref().is_some_and(|description| {
                        description.starts_with("Open Health and recovery.")
                    })
            })
            .expect("narrow health status control semantics");
        assert!(
            tab_bar.bounds.width() > navigation.bounds.width(),
            "narrow tabs should retain their natural width inside a scrollable viewport: tabs={:?}, viewport={:?}",
            tab_bar.bounds,
            navigation.bounds
        );
        assert!(
            navigation.bounds.max_x() <= status.bounds.x(),
            "narrow navigation must stop before the trailing health control: viewport={:?}, status={:?}",
            navigation.bounds,
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
            node.role == SemanticsRole::Popover
                && node.name.as_deref() == Some("Health and recovery")
                && node.state.expanded == Some(false)
        }));

        // The stock Popover owns expanded state and keyboard toggling; its child Button remains
        // the pointer trigger and accessible label.
        health_center.press("Enter")?;
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

        assert_eq!(status.label, "Offline");
        assert!(status.degraded);
        assert!(!status.connected_daemon);
        assert!(status.detail.contains("app-owned daemon failed"));
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

    #[test]
    fn offline_daemon_folds_surface_issues_into_one_offline_status() {
        let mut state = AppState::ready(crate::ui::offline::offline_test_backend());
        state.active_tab = Tab::Files;
        let shared = Shared::new(state);
        {
            let mut chat = shared.chat_mut();
            chat.activity.push(ChatActivityEntry {
                title: "chat list".to_string(),
                detail: "chat history requires a daemon connection".to_string(),
                tone: ActivityTone::Bad,
            });
        }
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("SIFS browsing requires a daemon connection".to_string());
        }
        shared
            .cluster_mut()
            .snapshot
            .warnings
            .push("cluster state unavailable: daemon is offline".to_string());

        let issues = shared.health_issues();
        assert_eq!(
            issues.iter().map(|issue| issue.surface).collect::<Vec<_>>(),
            vec![crate::state::HealthSurface::Connection],
            "surface failures caused by the offline daemon are not counted separately: {issues:?}"
        );
        let status = shell_status(&shared);
        assert_eq!(status.label, "Offline");
        assert!(status.degraded);
        assert!(
            status
                .detail
                .contains(crate::ui::offline::OFFLINE_TEST_REASON)
        );

        // Local files do not need the daemon, so their own failures still count.
        {
            let mut files = shared.files_mut();
            files.source = crate::ui::files::FilesSourceKind::Local;
            files.error = Some("access denied".to_string());
        }
        assert_eq!(shell_status(&shared).label, "2 issues");

        // A user reconnect is reported while it runs.
        shared.files_mut().source = crate::ui::files::FilesSourceKind::Sifs;
        assert!(shared.request_connection_probe(true));
        assert_eq!(shell_status(&shared).label, "Reconnecting…");
    }
}
