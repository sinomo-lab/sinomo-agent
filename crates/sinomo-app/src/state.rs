//! Shared, mutable application state.
//!
//! All cross-widget state lives behind a single `Rc<RefCell<AppState>>`. The app launches in a
//! *connecting* phase (no backend yet, UI shown immediately) while a background thread resolves
//! the daemon connection, pushing progress + the finished [`Backend`] over a channel. The shell
//! drains that channel whenever the UI is woken (via the sui external-wake mechanism) and, once
//! the backend arrives, populates [`ReadyState`] and builds the tab content.

use std::cell::{Ref, RefCell, RefMut};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Instant;

use sui::{DefaultTheme, Signal, WindowRenderOptions};

use sinomo_client::api::DaemonStatus;
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::{Backend, Connection, app_config, probe_runtime_connection};
use crate::rendering::{AppRenderMode, resolve_render_options};
use crate::theme::{AppTextScale, AppThemeMode, resolve_theme_with_preferences};
use crate::ui::chat::{ChatState, ChatStructureKey};
use crate::ui::cluster::{ClusterState, ClusterStructureKey};
use crate::ui::evolution::EvolutionState;
use crate::ui::files::{FilesState, FilesStructureKey};
use crate::ui::settings::{
    SettingsSection, SettingsState, SettingsStructureKey, settings_structure_key,
};

mod tabs;
pub(crate) use tabs::{WorkspaceSelection, WorkspaceTab};
pub use tabs::{WorkspaceTabId, WorkspaceTabKind};

/// The top-level destinations shown in the title bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Tab {
    /// AI chat with the Sinomo agent.
    Sinomo,
    /// Cluster topology graph.
    Cluster,
    /// Goal-directed evolution revision DAG and agent activity.
    Evolution,
    /// SIFS distributed file browser.
    Files,
    /// App + cluster settings.
    Settings,
}

impl Tab {
    pub const ALL: [Tab; 5] = [
        Tab::Sinomo,
        Tab::Cluster,
        Tab::Evolution,
        Tab::Files,
        Tab::Settings,
    ];

    pub fn index(self) -> usize {
        match self {
            Tab::Sinomo => 0,
            Tab::Cluster => 1,
            Tab::Evolution => 2,
            Tab::Files => 3,
            Tab::Settings => 4,
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Tab::Sinomo => "Chat",
            Tab::Cluster => "Cluster",
            Tab::Evolution => "Evolution",
            Tab::Files => "Files",
            Tab::Settings => "Settings",
        }
    }
}

/// Progress + result of the background connection, drained on the UI thread.
pub enum ConnectEvent {
    /// Human-readable progress for the status indicator.
    Status(String),
    /// The connection resolved and tab state has been built off the UI thread.
    Connected(Box<ReadyState>),
}

/// Live connection phases after startup. Unlike [`Backend::connection`], this is observation
/// state: short background probes can move it between online, degraded, reconnecting, and offline
/// without blocking the UI thread or pretending the launch snapshot is permanent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionPhase {
    Connecting,
    Online,
    Degraded,
    Reconnecting,
    Offline,
    /// Headless/local tooling intentionally has no daemon and should not raise a recovery issue.
    LocalOnly,
}

impl ConnectionPhase {
    pub fn is_connected(self) -> bool {
        matches!(self, Self::Online)
    }

    pub fn needs_attention(self) -> bool {
        matches!(self, Self::Degraded | Self::Reconnecting | Self::Offline)
    }
}

/// Whether the daemon-backed surfaces (chat, SIFS files, cluster, evolution) can work right now.
/// All of them run through the daemon, so they share one offline presentation keyed off this
/// typed state instead of each inferring connection trouble from its own error text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DaemonAvailability {
    Available,
    Unavailable {
        /// Why the daemon is unreachable, when known.
        detail: Option<String>,
        /// A user-requested reconnect is in flight.
        reconnecting: bool,
    },
}

impl DaemonAvailability {
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }
}

/// State for the typed daemon-probe lane. `pending_request_id` prevents overlapping probes and
/// lets late/stale results be ignored. A failed probe remains recoverable: the last error stays
/// visible and the health center can issue a new request.
#[derive(Clone, Debug)]
pub struct ConnectionHealth {
    pub phase: ConnectionPhase,
    pub label: String,
    pub detail: String,
    pub pending_request_id: Option<u64>,
    pub last_checked_at: Option<Instant>,
    pub last_error: Option<String>,
    next_request_id: u64,
    pending_explicit: bool,
    consecutive_failures: u32,
}

impl ConnectionHealth {
    fn connecting() -> Self {
        Self {
            phase: ConnectionPhase::Connecting,
            label: "Starting…".to_string(),
            detail: "Starting…".to_string(),
            pending_request_id: None,
            last_checked_at: None,
            last_error: None,
            next_request_id: 1,
            pending_explicit: false,
            consecutive_failures: 0,
        }
    }

    fn from_backend(backend: &Backend) -> Self {
        let (phase, last_error) = match &backend.connection {
            Connection::Daemon(_) | Connection::AppOwnedDaemon(_) => {
                (ConnectionPhase::Online, None)
            }
            Connection::LocalFallback {
                reason: Some(reason),
            } => (ConnectionPhase::Offline, Some(reason.clone())),
            Connection::LocalFallback { reason: None } => (ConnectionPhase::LocalOnly, None),
        };
        Self {
            phase,
            label: backend.mode_label(),
            detail: backend.status_detail(),
            pending_request_id: None,
            last_checked_at: None,
            last_error,
            next_request_id: 1,
            pending_explicit: false,
            consecutive_failures: 0,
        }
    }

    pub fn is_pending(&self) -> bool {
        self.pending_request_id.is_some()
    }
}

#[derive(Debug)]
enum ConnectionProbeResult {
    Online(DaemonStatus),
    Unavailable(String),
}

#[derive(Debug)]
struct ConnectionProbeEvent {
    request_id: u64,
    result: ConnectionProbeResult,
}

/// A surface represented in the single health and recovery center.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthSurface {
    Connection,
    Chat,
    Files,
    Cluster,
    Settings(SettingsSection),
}

impl HealthSurface {
    pub fn title(self) -> String {
        match self {
            Self::Connection => "Daemon connection".to_string(),
            Self::Chat => "Chat".to_string(),
            Self::Files => "Files · SIFS".to_string(),
            Self::Cluster => "Cluster".to_string(),
            Self::Settings(section) => format!("Settings · {}", section.title()),
        }
    }
}

/// One recoverable problem shown by the health center. The complete technical detail remains in
/// semantics even when the visual row truncates it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthIssue {
    pub surface: HealthSurface,
    pub title: String,
    pub detail: String,
}

impl HealthIssue {
    fn new(surface: HealthSurface, detail: impl Into<String>) -> Self {
        Self {
            surface,
            title: surface.title(),
            detail: detail.into(),
        }
    }

    pub fn needs_authorization(&self) -> bool {
        let detail = self.detail.to_ascii_lowercase();
        [
            "authorization",
            "unauthorized",
            "permission denied",
            "access denied",
            "credential",
            "sign in",
            "login",
        ]
        .iter()
        .any(|needle| detail.contains(needle))
    }
}

/// Everything that requires a resolved backend — populated once connected.
pub struct ReadyState {
    pub backend: Backend,
    pub chat: ChatState,
    pub cluster: ClusterState,
    pub evolution: EvolutionState,
    pub files: FilesState,
    pub settings: SettingsState,
    /// The workspace snapshot read once on the connect thread. Taken by the UI
    /// thread when the app becomes ready so it never re-reads `workspace.json`.
    pub(crate) restored_workspace: Option<crate::restoration::WorkspaceSnapshot>,
}

impl ReadyState {
    /// Build all tab sub-states from a resolved backend (spawns the chat worker and loads config;
    /// SIFS browsing is opened lazily by the Files tab).
    pub fn build(backend: Backend) -> Self {
        Self::build_with_waker(backend, None)
    }

    pub fn build_with_waker(backend: Backend, waker: Option<sui::UiHandle>) -> Self {
        Self::build_with_waker_and_progress(backend, waker, |_| {})
    }

    /// Like [`build_with_waker`](Self::build_with_waker), reporting each stage through
    /// `progress` so the launch status names the step in flight instead of an opaque
    /// "Loading workspace…". If a stage ever stalls (regressions here have wedged the
    /// boot behind distributed-file-system timeouts), the stuck step stays on screen.
    pub fn build_with_waker_and_progress(
        backend: Backend,
        waker: Option<sui::UiHandle>,
        progress: impl Fn(&str),
    ) -> Self {
        let interactive = waker.is_some();
        if interactive {
            progress("Reconciling provider accounts…");
            if let Err(error) =
                crate::backend::config::migrate_legacy_device_login_provider_accounts(
                    backend.config.as_ref(),
                    &backend.data_dir,
                )
            {
                tracing::warn!(%error, "legacy provider account migration failed");
            }
        }
        progress("Restoring the workspace…");
        let restored = crate::restoration::load(&backend.data_dir);
        let restored_workspace = restored.as_ref().ok().cloned().flatten();
        progress("Starting the chat engine…");
        let chat = ChatState::new_with_restoration(&backend, waker, restored);
        progress("Reading cluster status…");
        let cluster = ClusterState::new(&backend);
        progress("Preparing the file browser…");
        let files = FilesState::new(&backend);
        progress("Reading evolution goals…");
        let evolution = EvolutionState::new(&backend);
        progress("Reading settings…");
        let settings = if interactive {
            SettingsState::new_lightweight(&backend)
        } else {
            SettingsState::new(&backend)
        };
        Self {
            backend,
            chat,
            cluster,
            evolution,
            files,
            settings,
            restored_workspace,
        }
    }
}

/// Root application state. Sub-tab state is held in dedicated modules.
pub struct AppState {
    #[cfg(all(feature="desktop", any(target_os="linux",target_os="windows",target_os="macos")))]
    pub(crate) reports: crate::ui::reports::ReportHost,
    pub(crate) window_placement: crate::restoration::WindowPlacement,
    pub(crate) updates: crate::updates::AppUpdates,
    pub active_tab: Tab,
    pub(crate) workspace_tabs: Vec<Rc<WorkspaceTab>>,
    pub(crate) active_workspace_tab: Option<WorkspaceTabId>,
    pub(crate) last_chat_tab: Option<WorkspaceTabId>,
    pub(crate) next_workspace_tab_id: u64,
    pub theme_mode: AppThemeMode,
    pub text_scale: AppTextScale,
    pub render_mode: AppRenderMode,
    pub data_dir: Arc<SinomoDataDir>,
    /// Live status line for the title-bar indicator while connecting.
    pub status: String,
    /// When the current `status` line was set — the loading screen surfaces the
    /// elapsed time once a launch step stalls, so a wedged step is visible instead
    /// of an apparently frozen "Loading…".
    pub status_since: Option<Instant>,
    /// `Some` once the backend connection has resolved.
    pub ready: Option<ReadyState>,
    /// Live daemon observation state, refreshed by the background probe lane after startup.
    pub connection_health: ConnectionHealth,
    /// Channel from the background connect thread (drained by the shell on wake).
    pub connect_rx: Option<Receiver<ConnectEvent>>,
    /// At most one short daemon probe is in flight. Results carry request IDs so stale responses
    /// cannot overwrite a newer recovery attempt.
    connection_probe_rx: Option<Receiver<ConnectionProbeEvent>>,
    /// Wakes the UI from background threads (set once the event loop is up). Used by long-running
    /// async work like provider device-login to refresh the UI as it progresses.
    pub waker: Option<sui::UiHandle>,
}

impl AppState {
    /// The pre-connection state shown immediately at launch. The backend resolves asynchronously.
    pub fn connecting() -> Self {
        let data_dir = Arc::new(app_config::default_data_dir());
        let theme_mode = app_config::load_theme(&data_dir);
        let text_scale = app_config::load_text_scale(&data_dir);
        let render_mode = app_config::load_render_mode(&data_dir);
        Self {
            #[cfg(all(feature="desktop", any(target_os="linux",target_os="windows",target_os="macos")))]
            reports: crate::ui::reports::ReportHost::new(&data_dir),
            updates: crate::updates::AppUpdates::default(),
            window_placement: crate::restoration::load(&data_dir)
                .ok()
                .flatten()
                .map(|s| s.window)
                .unwrap_or_default(),
            active_tab: Tab::Sinomo,
            workspace_tabs: Vec::new(),
            active_workspace_tab: None,
            last_chat_tab: None,
            next_workspace_tab_id: 1,
            theme_mode,
            text_scale,
            render_mode,
            data_dir,
            status: "Starting…".to_string(),
            status_since: Some(Instant::now()),
            ready: None,
            connection_health: ConnectionHealth::connecting(),
            connect_rx: None,
            connection_probe_rx: None,
            waker: None,
        }
    }

    /// A fully-resolved state built synchronously from a backend — used by headless tooling and
    /// visual tests, which render the tabs directly without the async connection dance.
    pub fn ready(backend: Backend) -> Self {
        let data_dir = backend.data_dir.clone();
        let theme_mode = app_config::load_theme(&data_dir);
        let text_scale = app_config::load_text_scale(&data_dir);
        let render_mode = app_config::load_render_mode(&data_dir);
        let connection_health = ConnectionHealth::from_backend(&backend);
        Self {
            #[cfg(all(feature="desktop", any(target_os="linux",target_os="windows",target_os="macos")))]
            reports: crate::ui::reports::ReportHost::new(&data_dir),
            updates: crate::updates::AppUpdates::default(),
            window_placement: crate::restoration::WindowPlacement::default(),
            active_tab: Tab::Sinomo,
            workspace_tabs: Vec::new(),
            active_workspace_tab: None,
            last_chat_tab: None,
            next_workspace_tab_id: 1,
            theme_mode,
            text_scale,
            render_mode,
            data_dir,
            status: backend.mode_label(),
            status_since: None,
            ready: Some(ReadyState::build(backend)),
            connection_health,
            connect_rx: None,
            connection_probe_rx: None,
            waker: None,
        }
    }

    /// The resolved widget theme for the current mode.
    pub fn theme(&self) -> DefaultTheme {
        resolve_theme_with_preferences(self.theme_mode, self.text_scale)
    }

    pub fn render_options(&self) -> WindowRenderOptions {
        resolve_render_options(self.render_mode)
    }

    pub fn is_ready(&self) -> bool {
        self.ready.is_some()
    }
}

/// Cheap, cloneable handle to the shared application state.
#[derive(Clone)]
pub struct Shared {
    state: Rc<RefCell<AppState>>,
    signals: Rc<AppSignals>,
    scope: Option<Rc<WorkspaceTab>>,
}

/// Small, equality-deduplicated projections consumed by retained UI widgets.
///
/// [`AppState`] remains the domain source of truth. These signals only mirror values that the UI
/// needs to subscribe to directly, plus typed structural projections for retained surface
/// boundaries.
#[derive(Clone)]
pub struct AppSignals {
    pub(crate) app_update: Signal<crate::updates::UpdateView>,
    pub active_tab: Signal<Tab>,
    pub(crate) workspace_selection: Signal<WorkspaceSelection>,
    pub(crate) workspace_tabs: Signal<Vec<WorkspaceTabId>>,
    pub(crate) workspace_titles: Signal<Vec<String>>,
    pub ready: Signal<bool>,
    pub status_text: Signal<String>,
    pub connection: Signal<ConnectionHealthView>,
    pub daemon: Signal<DaemonAvailability>,
    pub theme: Signal<DefaultTheme>,
    pub health_structure: Signal<HealthStructureKey>,
    pub loading_stall_lines: Signal<[Option<String>; 2]>,
    pub cluster_structure: Signal<ClusterStructureKey>,
    pub evolution: Signal<crate::ui::evolution::EvolutionView>,
    pub files_structure: Signal<FilesStructureKey>,
    pub files_thumbnail_revision: Signal<u64>,
    pub settings_structure: Signal<SettingsStructureKey>,
    pub chat_structure: Signal<ChatStructureKey>,
}

impl AppSignals {
    fn from_state(state: &AppState) -> Self {
        Self {
            app_update: Signal::named("app update", state.updates.view.clone()),
            active_tab: Signal::named("app active tab", state.active_tab),
            workspace_selection: Signal::named(
                "workspace selection",
                WorkspaceSelection::Pinned(state.active_tab),
            ),
            workspace_tabs: Signal::named("workspace tabs", Vec::new()),
            workspace_titles: Signal::named("workspace titles", Vec::new()),
            ready: Signal::named("app ready", state.is_ready()),
            status_text: Signal::named("app status text", status_text_for(state)),
            connection: Signal::named(
                "app connection health",
                ConnectionHealthView::from(&state.connection_health),
            ),
            daemon: Signal::named("app daemon availability", daemon_availability_for(state)),
            theme: Signal::named("app theme", state.theme()),
            health_structure: Signal::named(
                "app health center structure",
                HealthStructureKey::basic(state),
            ),
            loading_stall_lines: Signal::named("app loading stall lines", [None, None]),
            cluster_structure: Signal::named(
                "cluster structure",
                ClusterStructureKey::new(
                    state.theme(),
                    state.ready.as_ref().map(|ready| &ready.cluster),
                ),
            ),
            evolution: Signal::named(
                "evolution dashboard",
                state
                    .ready
                    .as_ref()
                    .map(|ready| crate::ui::evolution::EvolutionView::from_state(&ready.evolution))
                    .unwrap_or_default(),
            ),
            files_structure: Signal::named(
                "files structure",
                FilesStructureKey::new(
                    state.theme_mode,
                    state.text_scale,
                    state.ready.as_ref().map(|ready| &ready.files),
                )
                .with_daemon_available(daemon_availability_for(state).is_available()),
            ),
            files_thumbnail_revision: Signal::named(
                "files thumbnail repaint",
                state
                    .ready
                    .as_ref()
                    .map(|ready| ready.files.thumbnail_revision())
                    .unwrap_or_default(),
            ),
            settings_structure: Signal::named("settings structure", SettingsStructureKey::empty()),
            chat_structure: Signal::named(
                "chat structure",
                ChatStructureKey::new(
                    state.theme_mode,
                    state.text_scale,
                    state.ready.as_ref().map(|ready| &ready.chat),
                ),
            ),
        }
    }
}

/// UI-facing connection snapshot. Timing and request bookkeeping remain in [`ConnectionHealth`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionHealthView {
    pub phase: ConnectionPhase,
    pub label: String,
    pub detail: String,
    pub pending: bool,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShellStatus {
    pub label: String,
    pub detail: String,
    pub degraded: bool,
    pub connected_daemon: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HealthStructureKey {
    pub status: ShellStatus,
    pub issues: Vec<HealthIssue>,
    pub connection_label: String,
    pub connection_detail: String,
    pub connection_phase: ConnectionPhase,
    pub connection_pending: bool,
    pub action_error: Option<String>,
    pub ready: bool,
}

impl HealthStructureKey {
    fn basic(state: &AppState) -> Self {
        let ready = state.is_ready();
        let health = &state.connection_health;
        Self {
            status: ShellStatus {
                label: status_text_for(state),
                detail: if ready {
                    health.detail.clone()
                } else {
                    state.status.clone()
                },
                degraded: false,
                connected_daemon: ready && health.phase.is_connected(),
            },
            issues: Vec::new(),
            connection_label: health.label.clone(),
            connection_detail: health.detail.clone(),
            connection_phase: health.phase,
            connection_pending: health.is_pending(),
            action_error: None,
            ready,
        }
    }
}

impl From<&ConnectionHealth> for ConnectionHealthView {
    fn from(health: &ConnectionHealth) -> Self {
        Self {
            phase: health.phase,
            label: health.label.clone(),
            detail: health.detail.clone(),
            pending: health.is_pending(),
            last_error: health.last_error.clone(),
        }
    }
}

/// Mutable state guard that publishes its UI projection only after releasing the `RefMut`.
pub struct PublishedRefMut<'a, T> {
    value: Option<RefMut<'a, T>>,
    publish: Option<Box<dyn FnOnce() + 'a>>,
}

impl<'a, T> PublishedRefMut<'a, T> {
    fn new(value: RefMut<'a, T>, publish: impl FnOnce() + 'a) -> Self {
        Self {
            value: Some(value),
            publish: Some(Box::new(publish)),
        }
    }
}

impl<T> Deref for PublishedRefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
            .as_deref()
            .expect("published state guard is live")
    }
}

impl<T> DerefMut for PublishedRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
            .as_deref_mut()
            .expect("published state guard is live")
    }
}

impl<T> Drop for PublishedRefMut<'_, T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if let Some(publish) = self.publish.take() {
            publish();
        }
    }
}

impl Shared {
    pub fn new(state: AppState) -> Self {
        let signals = Rc::new(AppSignals::from_state(&state));
        let shared = Self {
            state: Rc::new(RefCell::new(state)),
            signals,
            scope: None,
        };
        if shared.is_ready() {
            shared.publish_all_surface_signals();
        }
        shared.publish_health_signals();
        shared
    }

    pub fn borrow(&self) -> Ref<'_, AppState> {
        self.state.borrow()
    }

    pub fn borrow_mut(&self) -> PublishedRefMut<'_, AppState> {
        let shared = self.clone();
        PublishedRefMut::new(self.state.borrow_mut(), move || {
            shared.publish_shell_signals();
        })
    }

    pub fn signals(&self) -> &AppSignals {
        self.scope
            .as_ref()
            .map(|tab| &tab.signals)
            .unwrap_or(&self.signals)
    }

    pub fn theme(&self) -> DefaultTheme {
        self.state.borrow().theme()
    }

    /// The current theme at a contextual control size.
    pub fn theme_sized(&self, size: sui::ControlSize) -> DefaultTheme {
        let state = self.state.borrow();
        crate::theme::resolve_sized_theme(state.theme_mode, state.text_scale, size)
    }

    pub fn render_options(&self) -> WindowRenderOptions {
        self.state.borrow().render_options()
    }

    pub fn render_mode(&self) -> AppRenderMode {
        self.state.borrow().render_mode
    }

    pub fn active_tab(&self) -> Tab {
        self.state.borrow().active_tab
    }

    /// Whether the backend connection has resolved (tabs are interactive).
    pub fn is_ready(&self) -> bool {
        self.state.borrow().is_ready()
    }

    /// Text for the title-bar status indicator: the daemon mode once connected, otherwise the
    /// live connection progress.
    pub fn status_text(&self) -> String {
        status_text_for(&self.state.borrow())
    }

    /// Seconds the current launch step has been in flight (`None` once ready). The
    /// loading screen uses this to surface a stalled step.
    pub fn status_elapsed_secs(&self) -> Option<u64> {
        let state = self.state.borrow();
        if state.ready.is_some() {
            return None;
        }
        state.status_since.map(|since| since.elapsed().as_secs())
    }

    /// One-line connection detail (daemon address/cluster once connected, else progress).
    pub fn status_detail(&self) -> String {
        let state = self.state.borrow();
        match &state.ready {
            Some(_) => state.connection_health.detail.clone(),
            None => state.status.clone(),
        }
    }

    /// Borrow the chat sub-state directly. Only valid once [`is_ready`](Self::is_ready) — the
    /// shell builds the tab views only after the connection resolves, so callers (the views)
    /// never run before then.
    pub fn chat(&self) -> Ref<'_, ChatState> {
        if let Some(chat) = self.scope.as_ref().and_then(|tab| tab.chat()) {
            return chat.borrow();
        }
        Ref::map(self.state.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").chat
        })
    }

    pub fn chat_mut(&self) -> PublishedRefMut<'_, ChatState> {
        let shared = self.clone();
        if let Some(chat) = self.scope.as_ref().and_then(|tab| tab.chat()) {
            return PublishedRefMut::new(chat.borrow_mut(), move || shared.publish_chat_signals());
        }
        PublishedRefMut::new(
            RefMut::map(self.state.borrow_mut(), |s| {
                &mut s.ready.as_mut().expect("app is ready").chat
            }),
            move || shared.publish_chat_signals(),
        )
    }

    pub fn files(&self) -> Ref<'_, FilesState> {
        if let Some(files) = self.scope.as_ref().and_then(|tab| tab.files()) {
            return files.borrow();
        }
        Ref::map(self.state.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").files
        })
    }

    pub fn files_mut(&self) -> PublishedRefMut<'_, FilesState> {
        let shared = self.clone();
        if let Some(files) = self.scope.as_ref().and_then(|tab| tab.files()) {
            return PublishedRefMut::new(files.borrow_mut(), move || {
                shared.publish_files_signals()
            });
        }
        PublishedRefMut::new(
            RefMut::map(self.state.borrow_mut(), |s| {
                &mut s.ready.as_mut().expect("app is ready").files
            }),
            move || shared.publish_files_signals(),
        )
    }

    pub(crate) fn files_mut_unpublished(&self) -> RefMut<'_, FilesState> {
        if let Some(files) = self.scope.as_ref().and_then(|tab| tab.files()) {
            return files.borrow_mut();
        }
        RefMut::map(self.state.borrow_mut(), |state| {
            &mut state.ready.as_mut().expect("app is ready").files
        })
    }

    pub fn cluster(&self) -> Ref<'_, ClusterState> {
        Ref::map(self.state.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").cluster
        })
    }

    pub fn cluster_mut(&self) -> PublishedRefMut<'_, ClusterState> {
        let shared = self.clone();
        PublishedRefMut::new(
            RefMut::map(self.state.borrow_mut(), |s| {
                &mut s.ready.as_mut().expect("app is ready").cluster
            }),
            move || shared.publish_cluster_signals(),
        )
    }

    pub fn evolution(&self) -> Ref<'_, EvolutionState> {
        Ref::map(self.state.borrow(), |state| {
            &state.ready.as_ref().expect("app is ready").evolution
        })
    }

    pub fn evolution_mut(&self) -> PublishedRefMut<'_, EvolutionState> {
        let shared = self.clone();
        PublishedRefMut::new(
            RefMut::map(self.state.borrow_mut(), |state| {
                &mut state.ready.as_mut().expect("app is ready").evolution
            }),
            move || shared.publish_evolution_signals(),
        )
    }

    pub fn settings(&self) -> Ref<'_, SettingsState> {
        Ref::map(self.state.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").settings
        })
    }

    pub fn settings_mut(&self) -> PublishedRefMut<'_, SettingsState> {
        let shared = self.clone();
        PublishedRefMut::new(
            RefMut::map(self.state.borrow_mut(), |s| {
                &mut s.ready.as_mut().expect("app is ready").settings
            }),
            move || shared.publish_settings_signals(),
        )
    }

    pub(crate) fn settings_mut_unpublished(&self) -> RefMut<'_, SettingsState> {
        RefMut::map(self.state.borrow_mut(), |state| {
            &mut state.ready.as_mut().expect("app is ready").settings
        })
    }

    /// Change the active application surface and perform its activation side effects as one
    /// action. Signal delivery happens only after every mutable application-state borrow is
    /// released.
    pub fn set_active_tab(&self, tab: Tab) -> bool {
        let changed = {
            let mut state = self.state.borrow_mut();
            if state.active_tab == tab && state.active_workspace_tab.is_none() {
                false
            } else {
                state.active_tab = tab;
                state.active_workspace_tab = None;
                if tab == Tab::Sinomo {
                    state.last_chat_tab = None;
                }
                true
            }
        };
        if !changed {
            return false;
        }

        if self.is_ready() {
            self.sync_chat_tab_visibility();
            if tab == Tab::Cluster {
                crate::ui::cluster::request_refresh(self);
                crate::ui::cluster::request_observation(self);
            }
            if tab == Tab::Evolution {
                crate::ui::evolution::request_refresh(self);
            }
        }
        self.signals.active_tab.set(tab);
        self.publish_workspace_signals();
        true
    }

    pub fn publish_loading_stall_lines(&self, lines: [Option<String>; 2]) {
        self.signals.loading_stall_lines.set(lines);
    }

    pub fn publish_cluster_signals(&self) {
        let key = {
            let state = self.state.borrow();
            ClusterStructureKey::new(
                state.theme(),
                state.ready.as_ref().map(|ready| &ready.cluster),
            )
        };
        if self.signals.cluster_structure.set(key) {
            self.publish_health_signals();
        }
    }

    pub fn publish_files_signals(&self) {
        let (structure, thumbnail_revision) = {
            let state = self.state.borrow();
            let files = self.is_ready().then(|| self.files());
            (
                FilesStructureKey::new(state.theme_mode, state.text_scale, files.as_deref())
                    .with_daemon_available(daemon_availability_for(&state).is_available()),
                files
                    .as_ref()
                    .map(|files| files.thumbnail_revision())
                    .unwrap_or_default(),
            )
        };
        self.signals()
            .files_thumbnail_revision
            .set(thumbnail_revision);
        if self.signals().files_structure.set(structure) {
            self.publish_health_signals();
        }
        self.publish_workspace_signals();
        if self.is_ready() {
            let request = self.files_mut_unpublished().take_tab_open();
            if let Some((source, path, title)) = request {
                self.open_document_tab(source, path, title);
            }
            if self.workspace_kind() == Some(WorkspaceTabKind::Document)
                && !self.files().has_document()
            {
                if let Some(id) = self.workspace_id() {
                    self.remove_workspace_tab(id);
                }
            }
        }
    }

    pub fn publish_evolution_signals(&self) {
        let view = self
            .state
            .borrow()
            .ready
            .as_ref()
            .map(|ready| crate::ui::evolution::EvolutionView::from_state(&ready.evolution))
            .unwrap_or_default();
        self.signals.evolution.set(view);
    }

    pub fn publish_settings_signals(&self) {
        let key = settings_structure_key(self);
        if self.signals.settings_structure.set(key) {
            self.publish_health_signals();
        }
    }

    pub fn publish_chat_signals(&self) {
        let key = {
            let state = self.state.borrow();
            let chat = self.is_ready().then(|| self.chat());
            ChatStructureKey::new(state.theme_mode, state.text_scale, chat.as_deref())
        };
        if self.signals().chat_structure.set(key) {
            self.publish_health_signals();
        }
        self.publish_workspace_signals();
    }

    pub fn publish_health_action_error(&self, action_error: Option<String>) {
        self.publish_health_signals_with_error(action_error);
    }

    fn publish_health_signals(&self) {
        let action_error = self.signals.health_structure.get().action_error;
        self.publish_health_signals_with_error(action_error);
    }

    fn publish_health_signals_with_error(&self, action_error: Option<String>) {
        let issues = self.health_issues();
        let (
            ready,
            connection_label,
            connection_detail,
            connection_phase,
            connection_pending,
            status,
        ) = {
            let state = self.state.borrow();
            let health = &state.connection_health;
            (
                state.is_ready(),
                health.label.clone(),
                health.detail.clone(),
                health.phase,
                health.is_pending(),
                shell_status_for(&state, &issues),
            )
        };
        self.signals.health_structure.set(HealthStructureKey {
            status,
            issues,
            connection_label,
            connection_detail,
            connection_phase,
            connection_pending,
            action_error,
            ready,
        });
    }

    fn publish_all_surface_signals(&self) {
        if !self.is_ready() {
            return;
        }
        self.publish_cluster_signals();
        self.publish_evolution_signals();
        self.publish_files_signals();
        self.publish_settings_signals();
        self.publish_chat_signals();
        for tab in self.workspace_scopes() {
            match tab.workspace_kind() {
                Some(WorkspaceTabKind::Chat) => tab.publish_chat_signals(),
                _ => tab.publish_files_signals(),
            }
        }
        self.publish_workspace_signals();
    }

    fn publish_shell_signals(&self) {
        self.signals
            .app_update
            .set(self.state.borrow().updates.view.clone());
        let (active_tab, ready, status_text, connection, daemon, theme) = {
            let state = self.state.borrow();
            (
                state.active_tab,
                state.is_ready(),
                status_text_for(&state),
                ConnectionHealthView::from(&state.connection_health),
                daemon_availability_for(&state),
                state.theme(),
            )
        };
        self.signals.active_tab.set(active_tab);
        self.publish_workspace_signals();
        self.signals.ready.set(ready);
        self.signals.status_text.set(status_text);
        self.signals.connection.set(connection);
        self.signals.daemon.set(daemon);
        self.signals.theme.set(theme);
        self.publish_health_signals();
    }

    fn publish_connection_signals(&self) {
        let (status_text, connection, daemon) = {
            let state = self.state.borrow();
            (
                status_text_for(&state),
                ConnectionHealthView::from(&state.connection_health),
                daemon_availability_for(&state),
            )
        };
        self.signals.status_text.set(status_text);
        self.signals.connection.set(connection);
        self.signals.daemon.set(daemon);
        self.publish_health_signals();
    }

    /// Drain pending connect events (called by the shell when the UI is woken). Updates the
    /// status line and, on `Connected`, installs the ready state that was built off the UI thread.
    /// Returns `true` if anything changed, and `Some(true)` of that when the app *became* ready
    /// (so the caller can build the tab content). The tuple is `(changed, became_ready)`.
    pub fn pump_connect(&self) -> (bool, bool) {
        // Drain without holding the borrow across ReadyState construction (which itself borrows).
        let events: Vec<ConnectEvent> = {
            let state = self.state.borrow();
            match &state.connect_rx {
                Some(rx) => rx.try_iter().collect(),
                None => Vec::new(),
            }
        };
        if events.is_empty() {
            return (false, false);
        }

        let mut changed = false;
        let mut became_ready = false;
        let mut loading_step_changed = false;
        let mut restored_workspace = None;
        for event in events {
            match event {
                ConnectEvent::Status(status) => {
                    let mut state = self.state.borrow_mut();
                    if state.status != status {
                        state.status = status;
                        state.status_since = Some(Instant::now());
                        loading_step_changed = true;
                    }
                    changed = true;
                }
                ConnectEvent::Connected(mut ready) => {
                    let data_dir = ready.backend.data_dir.clone();
                    let theme_mode = app_config::load_theme(&data_dir);
                    let text_scale = app_config::load_text_scale(&data_dir);
                    let render_mode = app_config::load_render_mode(&data_dir);
                    let connection_health = ConnectionHealth::from_backend(&ready.backend);
                    let mut state = self.state.borrow_mut();
                    state.theme_mode = theme_mode;
                    state.text_scale = text_scale;
                    state.render_mode = render_mode;
                    restored_workspace = ready.restored_workspace.take();
                    if let Some(snapshot) = restored_workspace.as_ref() {
                        state.active_tab = snapshot.active_tab;
                    }
                    state.data_dir = data_dir;
                    state.status = ready.backend.mode_label();
                    state.status_since = None;
                    state.ready = Some(*ready);
                    state.connection_health = connection_health;
                    state.connect_rx = None;
                    changed = true;
                    became_ready = true;
                }
            }
        }
        if became_ready {
            if let Some(snapshot) = restored_workspace {
                self.restore_workspace_tabs(snapshot);
            }
            self.publish_all_surface_signals();
        }
        if loading_step_changed {
            self.publish_loading_stall_lines([None, None]);
        }
        self.publish_shell_signals();
        (changed, became_ready)
    }

    pub fn connection_health(&self) -> ConnectionHealth {
        self.state.borrow().connection_health.clone()
    }

    pub fn daemon_availability(&self) -> DaemonAvailability {
        daemon_availability_for(&self.state.borrow())
    }

    /// Start a short daemon probe on a named background thread. Explicit probes are user-visible
    /// reconnect attempts; automatic probes leave a healthy label alone while they are pending.
    /// Returns `false` when no backend exists or a probe is already in flight.
    pub fn request_connection_probe(&self, explicit: bool) -> bool {
        let (tx, rx) = mpsc::channel();
        let (request_id, data_dir, address, waker) = {
            let mut state = self.state.borrow_mut();
            if state.ready.is_none()
                || state.connection_health.pending_request_id.is_some()
                || (!explicit
                    && matches!(state.connection_health.phase, ConnectionPhase::LocalOnly))
            {
                return false;
            }
            let request_id = state.connection_health.next_request_id;
            state.connection_health.next_request_id = request_id.wrapping_add(1);
            state.connection_health.pending_request_id = Some(request_id);
            state.connection_health.pending_explicit = explicit;
            if explicit {
                state.connection_health.phase = ConnectionPhase::Reconnecting;
                state.connection_health.label = "Reconnecting…".to_string();
                state.connection_health.detail =
                    "Checking the daemon connection in the background…".to_string();
            }
            let ready = state.ready.as_ref().expect("checked ready state");
            let data_dir = ready.backend.data_dir.clone();
            let address = ready
                .backend
                .connection
                .daemon_address()
                .map(str::to_string);
            let waker = state.waker.clone();
            state.connection_probe_rx = Some(rx);
            (request_id, data_dir, address, waker)
        };
        self.publish_connection_signals();

        let spawn = thread::Builder::new()
            .name("sinomo-app-connection-probe".to_string())
            .spawn(move || {
                let result = probe_runtime_connection(data_dir.as_ref(), address.as_deref())
                    .map(ConnectionProbeResult::Online)
                    .unwrap_or_else(ConnectionProbeResult::Unavailable);
                let _ = tx.send(ConnectionProbeEvent { request_id, result });
                if let Some(waker) = waker {
                    waker.wake();
                }
            });

        if let Err(error) = spawn {
            let detail = format!("could not start connection probe: {error}");
            {
                let mut state = self.state.borrow_mut();
                if state.connection_health.pending_request_id == Some(request_id) {
                    state.connection_health.pending_request_id = None;
                    state.connection_health.pending_explicit = false;
                    state.connection_health.phase = ConnectionPhase::Offline;
                    state.connection_health.label = "Reconnect failed".to_string();
                    state.connection_health.detail = detail.clone();
                    state.connection_health.last_error = Some(detail);
                    state.connection_probe_rx = None;
                }
            }
            self.publish_connection_signals();
            return false;
        }
        true
    }

    /// Drain the current daemon probe, ignoring stale request IDs. The first transient automatic
    /// failure becomes `Degraded`; a repeated failure or an explicit reconnect failure becomes
    /// `Offline`. A later successful probe clears the recoverable error and returns to `Online`.
    pub fn pump_connection_probe(&self) -> bool {
        let event = {
            let state = self.state.borrow();
            let Some(rx) = state.connection_probe_rx.as_ref() else {
                return false;
            };
            match rx.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => return false,
                Err(TryRecvError::Disconnected) => ConnectionProbeEvent {
                    request_id: state
                        .connection_health
                        .pending_request_id
                        .unwrap_or_default(),
                    result: ConnectionProbeResult::Unavailable(
                        "connection probe worker stopped before returning a result".to_string(),
                    ),
                },
            }
        };
        self.apply_connection_probe_event(event)
    }

    fn apply_connection_probe_event(&self, event: ConnectionProbeEvent) -> bool {
        {
            let mut state = self.state.borrow_mut();
            if state.connection_health.pending_request_id != Some(event.request_id) {
                return false;
            }
            let explicit = state.connection_health.pending_explicit;
            state.connection_health.pending_request_id = None;
            state.connection_health.pending_explicit = false;
            state.connection_health.last_checked_at = Some(Instant::now());
            state.connection_probe_rx = None;

            match event.result {
                ConnectionProbeResult::Online(status) => {
                    let (label, detail) = {
                        let waker = state.waker.clone();
                        let Some(ready) = state.ready.as_mut() else {
                            return false;
                        };
                        let binding_changed = ready.backend.apply_live_daemon_status(status);
                        let backend = ready.backend.clone();
                        let label = ready.backend.mode_label();
                        let detail = ready.backend.status_detail();
                        if binding_changed {
                            ready.settings.rebind_backend(&backend);
                            ready
                                .settings
                                .begin_initial_load(backend.clone(), waker.clone());
                            ready.cluster.rebind_backend(backend.clone(), waker.clone());
                        }
                        ready.chat.rebind_backend(&backend);
                        ready.files.rebind_backend(&backend, waker);
                        // These labels are server-owned summary fields. Drafts and in-progress
                        // Settings actions stay untouched while the live connection summary
                        // advances.
                        ready.settings.mode_label = label.clone();
                        ready.settings.status_detail = detail.clone();
                        (label, detail)
                    };
                    let backend = state.ready.as_ref().expect("ready").backend.clone();
                    for tab in &state.workspace_tabs {
                        if let Some(chat) = tab.chat() {
                            chat.borrow_mut().rebind_backend(&backend);
                        }
                        if let Some(files) = tab.files() {
                            files
                                .borrow_mut()
                                .rebind_backend(&backend, state.waker.clone());
                        }
                    }
                    state.connection_health.phase = ConnectionPhase::Online;
                    state.connection_health.label = label;
                    state.connection_health.detail = detail;
                    state.connection_health.last_error = None;
                    state.connection_health.consecutive_failures = 0;
                }
                ConnectionProbeResult::Unavailable(error) => {
                    state.connection_health.consecutive_failures = state
                        .connection_health
                        .consecutive_failures
                        .saturating_add(1);
                    let offline = explicit || state.connection_health.consecutive_failures >= 2;
                    state.connection_health.phase = if offline {
                        ConnectionPhase::Offline
                    } else {
                        ConnectionPhase::Degraded
                    };
                    state.connection_health.label = if offline {
                        "Offline".to_string()
                    } else {
                        "Connection interrupted".to_string()
                    };
                    state.connection_health.detail = if offline {
                        format!("Daemon unavailable: {error}. Reconnect when the service is ready.")
                    } else {
                        format!(
                            "Daemon probe failed: {error}. Sinomo will check again automatically."
                        )
                    };
                    state.connection_health.last_error = Some(error);
                }
            }
        }
        self.publish_connection_signals();
        self.publish_all_surface_signals();
        true
    }

    /// Current recoverable issues across app surfaces, ordered from the runtime foundation out to
    /// user-facing features. Loading placeholders are intentionally excluded.
    pub fn health_issues(&self) -> Vec<HealthIssue> {
        let state = self.state.borrow();
        let Some(ready) = state.ready.as_ref() else {
            return Vec::new();
        };
        let mut issues = Vec::new();
        // While the daemon is unreachable, chat, SIFS, and cluster failures are consequences of
        // that one root cause, already reported as the connection issue.
        let daemon_down = !daemon_availability_for(&state).is_available();

        if state.connection_health.phase.needs_attention() {
            issues.push(HealthIssue::new(
                HealthSurface::Connection,
                state.connection_health.detail.clone(),
            ));
        }
        if !daemon_down && let Some(detail) = ready.chat.connection_attention_detail_full() {
            issues.push(HealthIssue::new(HealthSurface::Chat, detail));
        }
        let files_need_daemon = ready.files.source == crate::ui::files::FilesSourceKind::Sifs;
        if !(daemon_down && files_need_daemon)
            && let Some(detail) = ready.files.connection_attention_detail()
        {
            issues.push(HealthIssue::new(HealthSurface::Files, detail));
        }
        if !daemon_down
            && let Some(detail) = ready
                .cluster
                .snapshot
                .warnings
                .iter()
                .rev()
                .find(|warning| !is_loading_placeholder(warning))
        {
            issues.push(HealthIssue::new(HealthSurface::Cluster, detail.clone()));
        }
        let settings = &ready.settings;
        let settings_errors = [
            (
                SettingsSection::Configuration,
                settings.config_error.as_deref(),
            ),
            (
                SettingsSection::Cluster,
                settings.cluster_action_error.as_deref(),
            ),
            (
                SettingsSection::Providers,
                settings
                    .provider_action_error
                    .as_deref()
                    .or(settings.provider_add_error.as_deref()),
            ),
            (
                SettingsSection::Agents,
                settings.agent_action_error.as_deref(),
            ),
            (
                SettingsSection::Modules,
                settings.module_action_error.as_deref(),
            ),
            (
                SettingsSection::Skills,
                settings.skill_action_error.as_deref(),
            ),
        ];
        for (section, detail) in settings_errors {
            // Cluster settings load through the daemon, like the cluster tab.
            if daemon_down && section == SettingsSection::Cluster {
                continue;
            }
            if let Some(detail) = detail.filter(|detail| !is_loading_placeholder(detail)) {
                issues.push(HealthIssue::new(HealthSurface::Settings(section), detail));
            }
        }
        if let Some(login) = settings.device_login.as_ref()
            && let crate::ui::settings::DeviceLoginStatus::Failed(error) = &login.status
        {
            issues.push(HealthIssue::new(
                HealthSurface::Settings(SettingsSection::Providers),
                format!("{} authorization failed: {error}", login.provider),
            ));
        }
        issues
    }

    pub fn set_theme_mode(&self, mode: AppThemeMode) {
        let data_dir = {
            let mut state = self.state.borrow_mut();
            if state.theme_mode == mode {
                return;
            }
            state.theme_mode = mode;
            state.data_dir.clone()
        };
        if let Err(error) = app_config::save_theme(&data_dir, mode) {
            tracing::warn!(%error, "failed to persist app UI theme preference");
        }
        self.signals.theme.set(self.theme());
        self.publish_all_surface_signals();
    }

    pub fn set_render_mode(&self, mode: AppRenderMode) {
        let data_dir = {
            let mut state = self.state.borrow_mut();
            if state.render_mode == mode {
                return;
            }
            state.render_mode = mode;
            state.data_dir.clone()
        };
        if let Err(error) = app_config::save_render_mode(&data_dir, mode) {
            tracing::warn!(%error, "failed to persist app render preference");
        }
    }

    pub fn set_text_scale(&self, scale: AppTextScale) {
        let data_dir = {
            let mut state = self.state.borrow_mut();
            if state.text_scale == scale {
                return;
            }
            state.text_scale = scale;
            state.data_dir.clone()
        };
        if let Err(error) = app_config::save_text_scale(&data_dir, scale) {
            tracing::warn!(%error, "failed to persist app text-scale preference");
        }
        self.signals.theme.set(self.theme());
        self.publish_all_surface_signals();
    }
}

fn daemon_availability_for(state: &AppState) -> DaemonAvailability {
    // Before the backend resolves, the shell shows its startup view instead of any surface.
    let Some(ready) = state.ready.as_ref() else {
        return DaemonAvailability::Available;
    };
    let health = &state.connection_health;
    // A daemon that failed to start or stopped answering lands in `Offline`. An explicit
    // reconnect from a healthy daemon keeps surfaces up; one that follows a failure stays offline
    // until the probe succeeds. `LocalOnly` is daemonless headless tooling, which renders the
    // surfaces as they are, like it raises no recovery issue.
    let lost = match health.phase {
        ConnectionPhase::Offline => true,
        ConnectionPhase::Reconnecting => health.last_error.is_some(),
        _ => false,
    };
    if !lost {
        return DaemonAvailability::Available;
    }
    DaemonAvailability::Unavailable {
        detail: health.last_error.clone().or_else(|| {
            ready
                .backend
                .connection
                .local_fallback_reason()
                .map(str::to_string)
        }),
        reconnecting: health.is_pending() && health.pending_explicit,
    }
}

fn status_text_for(state: &AppState) -> String {
    match &state.ready {
        Some(_) => state.connection_health.label.clone(),
        None => state.status.clone(),
    }
}

fn shell_status_for(state: &AppState, issues: &[HealthIssue]) -> ShellStatus {
    if state.ready.is_none() {
        let status = state.status.clone();
        return ShellStatus {
            label: status.clone(),
            detail: status,
            degraded: false,
            connected_daemon: false,
        };
    }
    let health = &state.connection_health;
    if issues.is_empty() {
        return ShellStatus {
            label: health.label.clone(),
            detail: health.detail.clone(),
            degraded: false,
            connected_daemon: health.phase.is_connected(),
        };
    }

    if let [issue] = issues
        && issue.surface == HealthSurface::Connection
        && let DaemonAvailability::Unavailable { reconnecting, .. } = daemon_availability_for(state)
    {
        return ShellStatus {
            label: if reconnecting {
                "Reconnecting…".to_string()
            } else {
                "Offline".to_string()
            },
            detail: issue.detail.clone(),
            degraded: true,
            connected_daemon: false,
        };
    }

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

fn is_loading_placeholder(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("is loading")
        || detail.contains("data is loading")
        || detail.contains("loading in the background")
        || detail.contains("open cluster to refresh live health")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn pump_connect_tracks_launch_step_transitions() {
        let shared = Shared::new(AppState::connecting());
        let (tx, rx) = mpsc::channel();
        shared.borrow_mut().connect_rx = Some(rx);

        tx.send(ConnectEvent::Status(
            "Starting the chat engine…".to_string(),
        ))
        .expect("send status");
        let (changed, became_ready) = shared.pump_connect();
        assert!(changed);
        assert!(!became_ready);
        assert_eq!(shared.borrow().status, "Starting the chat engine…");
        let first_since = shared.borrow().status_since.expect("step start time");
        assert!(shared.status_elapsed_secs().is_some());

        // The same status again must not reset the step clock — the elapsed time
        // is what makes a stalled step visible on the loading screen.
        tx.send(ConnectEvent::Status(
            "Starting the chat engine…".to_string(),
        ))
        .expect("send status");
        let (changed, _) = shared.pump_connect();
        assert!(changed);
        assert_eq!(shared.borrow().status_since, Some(first_since));

        // A new step restarts the clock.
        tx.send(ConnectEvent::Status("Reading settings…".to_string()))
            .expect("send status");
        shared.pump_connect();
        assert_eq!(shared.borrow().status, "Reading settings…");
        let second_since = shared.borrow().status_since.expect("new step start time");
        assert!(second_since >= first_since);
    }

    fn attach_test_daemon(shared: &Shared) {
        let status: DaemonStatus = serde_json::from_value(serde_json::json!({
            "pid": 4242,
            "host": "127.0.0.1",
            "port": 21353,
            "address": "127.0.0.1:21353",
            "started_at": "2026-10-01T00:00:00Z",
            "data_dir": "/tmp/sinomo-test",
            "lock_file": "/tmp/sinomo-test/daemon.lock",
        }))
        .expect("test daemon status");
        let mut state = shared.borrow_mut();
        state.ready.as_mut().expect("ready").backend.connection =
            Connection::Daemon(Box::new(status));
        state.connection_health.phase = ConnectionPhase::Online;
        state.connection_health.last_error = None;
    }

    #[test]
    fn daemon_availability_follows_the_probe_phase() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        assert!(
            shared.daemon_availability().is_available(),
            "daemonless headless tooling renders surfaces as they are"
        );

        attach_test_daemon(&shared);
        assert!(shared.daemon_availability().is_available());

        // One transient probe failure keeps surfaces up; going offline takes them down.
        shared.borrow_mut().connection_health.phase = ConnectionPhase::Degraded;
        assert!(shared.daemon_availability().is_available());
        {
            let mut state = shared.borrow_mut();
            state.connection_health.phase = ConnectionPhase::Offline;
            state.connection_health.last_error = Some("connection refused".to_string());
        }
        assert_eq!(
            shared.daemon_availability(),
            DaemonAvailability::Unavailable {
                detail: Some("connection refused".to_string()),
                reconnecting: false,
            }
        );

        // A user reconnect after a failure stays offline, marked as reconnecting, until it lands.
        {
            let mut state = shared.borrow_mut();
            state.connection_health.phase = ConnectionPhase::Reconnecting;
            state.connection_health.pending_request_id = Some(7);
            state.connection_health.pending_explicit = true;
        }
        assert_eq!(
            shared.daemon_availability(),
            DaemonAvailability::Unavailable {
                detail: Some("connection refused".to_string()),
                reconnecting: true,
            }
        );

        // A reconnect requested while healthy does not flash surfaces offline.
        shared.borrow_mut().connection_health.last_error = None;
        assert!(shared.daemon_availability().is_available());
    }

    #[test]
    fn daemon_availability_reports_the_local_fallback_reason() {
        let mut backend = Backend::local_fallback();
        backend.connection = Connection::LocalFallback {
            reason: Some("daemon failed to start: port in use".to_string()),
        };
        let shared = Shared::new(AppState::ready(backend));
        assert_eq!(
            shared.daemon_availability(),
            DaemonAvailability::Unavailable {
                detail: Some("daemon failed to start: port in use".to_string()),
                reconnecting: false,
            }
        );
        assert_eq!(
            shared.signals().daemon.get(),
            shared.daemon_availability(),
            "the published signal starts in sync with the state"
        );
    }

    #[test]
    fn probe_results_are_request_scoped_and_recoverable() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut state = shared.borrow_mut();
            state.connection_health.phase = ConnectionPhase::Online;
            state.connection_health.label = "Daemon".to_string();
            state.connection_health.pending_request_id = Some(42);
            state.connection_health.pending_explicit = false;
        }

        assert!(!shared.apply_connection_probe_event(ConnectionProbeEvent {
            request_id: 41,
            result: ConnectionProbeResult::Unavailable("stale failure".to_string()),
        }));
        assert_eq!(
            shared.connection_health().pending_request_id,
            Some(42),
            "stale results must not clear the active request"
        );

        assert!(shared.apply_connection_probe_event(ConnectionProbeEvent {
            request_id: 42,
            result: ConnectionProbeResult::Unavailable("connection refused".to_string()),
        }));
        let health = shared.connection_health();
        assert_eq!(health.phase, ConnectionPhase::Degraded);
        assert_eq!(health.pending_request_id, None);
        assert_eq!(health.last_error.as_deref(), Some("connection refused"));

        {
            let mut state = shared.borrow_mut();
            state.connection_health.pending_request_id = Some(43);
            state.connection_health.pending_explicit = true;
        }
        assert!(shared.apply_connection_probe_event(ConnectionProbeEvent {
            request_id: 43,
            result: ConnectionProbeResult::Unavailable("still unavailable".to_string()),
        }));
        let health = shared.connection_health();
        assert_eq!(health.phase, ConnectionPhase::Offline);
        assert!(health.detail.contains("Reconnect"));

        let status = {
            let state = shared.borrow();
            let ready = state.ready.as_ref().expect("ready");
            let paths = sinomo_client::api::DaemonPaths::new(ready.backend.data_dir.as_ref());
            sinomo_client::api::DaemonStatus::new(
                &sinomo_client::api::DaemonRunOptions::default(),
                &paths,
                None,
            )
        };
        {
            let mut state = shared.borrow_mut();
            state.connection_health.pending_request_id = Some(44);
            state.connection_health.pending_explicit = true;
        }
        assert!(shared.apply_connection_probe_event(ConnectionProbeEvent {
            request_id: 44,
            result: ConnectionProbeResult::Online(status),
        }));
        let health = shared.connection_health();
        assert_eq!(health.phase, ConnectionPhase::Online);
        assert!(health.last_error.is_none());
        assert!(
            shared
                .borrow()
                .ready
                .as_ref()
                .is_some_and(|ready| ready.backend.connection.is_daemon())
        );
    }

    #[test]
    fn health_issues_keep_surface_specific_deep_link_context() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.files_mut().error = Some("device authorization was denied".to_string());
        shared.settings_mut().provider_action_error =
            Some("provider credential expired".to_string());

        let issues = shared.health_issues();
        let files = issues
            .iter()
            .find(|issue| issue.surface == HealthSurface::Files)
            .expect("Files issue");
        assert!(files.needs_authorization());
        assert_eq!(files.title, "Files · SIFS");
        let provider = issues
            .iter()
            .find(|issue| issue.surface == HealthSurface::Settings(SettingsSection::Providers))
            .expect("provider Settings issue");
        assert!(provider.needs_authorization());
        assert_eq!(provider.title, "Settings · Providers");
    }
}
