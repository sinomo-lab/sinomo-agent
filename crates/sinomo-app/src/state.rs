//! Shared, mutable application state.
//!
//! All cross-widget state lives behind a single `Rc<RefCell<AppState>>`. The app launches in a
//! *connecting* phase (no backend yet, UI shown immediately) while a background thread resolves
//! the daemon connection, pushing progress + the finished [`Backend`] over a channel. The shell
//! drains that channel whenever the UI is woken (via the sui external-wake mechanism) and, once
//! the backend arrives, populates [`ReadyState`] and builds the tab content.

use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use sui::{DefaultTheme, WindowRenderOptions};

use sinomo_runtime::local::SinomoDataDir;

use crate::backend::{Backend, app_config};
use crate::rendering::{AppRenderMode, resolve_render_options};
use crate::theme::{AppThemeMode, resolve_theme};
use crate::ui::chat::ChatState;
use crate::ui::cluster::ClusterState;
use crate::ui::evolution::EvolutionState;
use crate::ui::files::FilesState;
use crate::ui::settings::SettingsState;

/// The top-level destinations shown in the title bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    /// AI chat with the Sinomo agent.
    Sinomo,
    /// Self-evolution jobs, candidate bundles, and rollout evidence.
    Evolution,
    /// Cluster topology graph.
    Cluster,
    /// SIFS distributed file browser.
    Files,
    /// App + cluster settings.
    Settings,
}

impl Tab {
    pub const ALL: [Tab; 5] = [
        Tab::Sinomo,
        Tab::Evolution,
        Tab::Cluster,
        Tab::Files,
        Tab::Settings,
    ];

    pub fn index(self) -> usize {
        match self {
            Tab::Sinomo => 0,
            Tab::Evolution => 1,
            Tab::Cluster => 2,
            Tab::Files => 3,
            Tab::Settings => 4,
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Tab::Sinomo => "Chat",
            Tab::Evolution => "Evolution",
            Tab::Cluster => "Cluster",
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

/// Everything that requires a resolved backend — populated once connected.
pub struct ReadyState {
    pub backend: Backend,
    pub chat: ChatState,
    pub evolution: EvolutionState,
    pub cluster: ClusterState,
    pub files: FilesState,
    pub settings: SettingsState,
}

impl ReadyState {
    /// Build all tab sub-states from a resolved backend (spawns the chat worker and loads config;
    /// SIFS browsing is opened lazily by the Files tab).
    pub fn build(backend: Backend) -> Self {
        Self::build_with_waker(backend, None)
    }

    pub fn build_with_waker(backend: Backend, waker: Option<sui::UiHandle>) -> Self {
        let interactive = waker.is_some();
        let chat = ChatState::new_with_waker(&backend, waker);
        let evolution = EvolutionState::new(&backend);
        let cluster = ClusterState::new(&backend);
        let files = FilesState::new(&backend);
        let settings = if interactive {
            SettingsState::new_lightweight(&backend)
        } else {
            SettingsState::new(&backend)
        };
        Self {
            backend,
            chat,
            evolution,
            cluster,
            files,
            settings,
        }
    }
}

/// Root application state. Sub-tab state is held in dedicated modules.
pub struct AppState {
    pub active_tab: Tab,
    pub theme_mode: AppThemeMode,
    pub render_mode: AppRenderMode,
    pub data_dir: Arc<SinomoDataDir>,
    /// Live status line for the title-bar indicator while connecting.
    pub status: String,
    /// `Some` once the backend connection has resolved.
    pub ready: Option<ReadyState>,
    /// Channel from the background connect thread (drained by the shell on wake).
    pub connect_rx: Option<Receiver<ConnectEvent>>,
    /// Wakes the UI from background threads (set once the event loop is up). Used by long-running
    /// async work like provider device-login to refresh the UI as it progresses.
    pub waker: Option<sui::UiHandle>,
}

impl AppState {
    /// The pre-connection state shown immediately at launch. The backend resolves asynchronously.
    pub fn connecting() -> Self {
        let data_dir = Arc::new(app_config::default_data_dir());
        let theme_mode = app_config::load_theme(&data_dir);
        let render_mode = app_config::load_render_mode(&data_dir);
        Self {
            active_tab: Tab::Sinomo,
            theme_mode,
            render_mode,
            data_dir,
            status: "Starting…".to_string(),
            ready: None,
            connect_rx: None,
            waker: None,
        }
    }

    /// A fully-resolved state built synchronously from a backend — used by headless tooling and
    /// visual tests, which render the tabs directly without the async connection dance.
    pub fn ready(backend: Backend) -> Self {
        let data_dir = backend.data_dir.clone();
        let theme_mode = app_config::load_theme(&data_dir);
        let render_mode = app_config::load_render_mode(&data_dir);
        Self {
            active_tab: Tab::Sinomo,
            theme_mode,
            render_mode,
            data_dir,
            status: backend.mode_label(),
            ready: Some(ReadyState::build(backend)),
            connect_rx: None,
            waker: None,
        }
    }

    /// The resolved widget theme for the current mode.
    pub fn theme(&self) -> DefaultTheme {
        resolve_theme(self.theme_mode)
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
pub struct Shared(Rc<RefCell<AppState>>);

impl Shared {
    pub fn new(state: AppState) -> Self {
        Self(Rc::new(RefCell::new(state)))
    }

    pub fn borrow(&self) -> Ref<'_, AppState> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> RefMut<'_, AppState> {
        self.0.borrow_mut()
    }

    pub fn theme(&self) -> DefaultTheme {
        self.0.borrow().theme()
    }

    pub fn render_options(&self) -> WindowRenderOptions {
        self.0.borrow().render_options()
    }

    pub fn render_mode(&self) -> AppRenderMode {
        self.0.borrow().render_mode
    }

    pub fn active_tab(&self) -> Tab {
        self.0.borrow().active_tab
    }

    /// Whether the backend connection has resolved (tabs are interactive).
    pub fn is_ready(&self) -> bool {
        self.0.borrow().is_ready()
    }

    /// Text for the title-bar status indicator: the daemon mode once connected, otherwise the
    /// live connection progress.
    pub fn status_text(&self) -> String {
        let state = self.0.borrow();
        match &state.ready {
            Some(ready) => ready.backend.mode_label(),
            None => state.status.clone(),
        }
    }

    /// One-line connection detail (daemon address/cluster once connected, else progress).
    pub fn status_detail(&self) -> String {
        let state = self.0.borrow();
        match &state.ready {
            Some(ready) => ready.backend.status_detail(),
            None => state.status.clone(),
        }
    }

    /// Borrow the chat sub-state directly. Only valid once [`is_ready`](Self::is_ready) — the
    /// shell builds the tab views only after the connection resolves, so callers (the views)
    /// never run before then.
    pub fn chat(&self) -> Ref<'_, ChatState> {
        Ref::map(self.0.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").chat
        })
    }

    pub fn chat_mut(&self) -> RefMut<'_, ChatState> {
        RefMut::map(self.0.borrow_mut(), |s| {
            &mut s.ready.as_mut().expect("app is ready").chat
        })
    }

    pub fn evolution(&self) -> Ref<'_, EvolutionState> {
        Ref::map(self.0.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").evolution
        })
    }

    pub fn evolution_mut(&self) -> RefMut<'_, EvolutionState> {
        RefMut::map(self.0.borrow_mut(), |s| {
            &mut s.ready.as_mut().expect("app is ready").evolution
        })
    }

    pub fn files(&self) -> Ref<'_, FilesState> {
        Ref::map(self.0.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").files
        })
    }

    pub fn files_mut(&self) -> RefMut<'_, FilesState> {
        RefMut::map(self.0.borrow_mut(), |s| {
            &mut s.ready.as_mut().expect("app is ready").files
        })
    }

    pub fn cluster(&self) -> Ref<'_, ClusterState> {
        Ref::map(self.0.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").cluster
        })
    }

    pub fn cluster_mut(&self) -> RefMut<'_, ClusterState> {
        RefMut::map(self.0.borrow_mut(), |s| {
            &mut s.ready.as_mut().expect("app is ready").cluster
        })
    }

    pub fn settings(&self) -> Ref<'_, SettingsState> {
        Ref::map(self.0.borrow(), |s| {
            &s.ready.as_ref().expect("app is ready").settings
        })
    }

    pub fn settings_mut(&self) -> RefMut<'_, SettingsState> {
        RefMut::map(self.0.borrow_mut(), |s| {
            &mut s.ready.as_mut().expect("app is ready").settings
        })
    }

    /// Drain pending connect events (called by the shell when the UI is woken). Updates the
    /// status line and, on `Connected`, installs the ready state that was built off the UI thread.
    /// Returns `true` if anything changed, and `Some(true)` of that when the app *became* ready
    /// (so the caller can build the tab content). The tuple is `(changed, became_ready)`.
    pub fn pump_connect(&self) -> (bool, bool) {
        // Drain without holding the borrow across ReadyState construction (which itself borrows).
        let events: Vec<ConnectEvent> = {
            let state = self.0.borrow();
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
        for event in events {
            match event {
                ConnectEvent::Status(status) => {
                    self.0.borrow_mut().status = status;
                    changed = true;
                }
                ConnectEvent::Connected(ready) => {
                    let data_dir = ready.backend.data_dir.clone();
                    let theme_mode = app_config::load_theme(&data_dir);
                    let render_mode = app_config::load_render_mode(&data_dir);
                    let mut state = self.0.borrow_mut();
                    state.theme_mode = theme_mode;
                    state.render_mode = render_mode;
                    state.data_dir = data_dir;
                    state.status = ready.backend.mode_label();
                    state.ready = Some(*ready);
                    state.connect_rx = None;
                    changed = true;
                    became_ready = true;
                }
            }
        }
        (changed, became_ready)
    }

    pub fn set_theme_mode(&self, mode: AppThemeMode) {
        let data_dir = {
            let mut state = self.0.borrow_mut();
            if state.theme_mode == mode {
                return;
            }
            state.theme_mode = mode;
            state.data_dir.clone()
        };
        if let Err(error) = app_config::save_theme(&data_dir, mode) {
            tracing::warn!(%error, "failed to persist app UI theme preference");
        }
    }

    pub fn set_render_mode(&self, mode: AppRenderMode) {
        let data_dir = {
            let mut state = self.0.borrow_mut();
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
}
