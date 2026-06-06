//! Shared, mutable application state.
//!
//! Following the SUI reactive idiom, all cross-widget state lives behind a single
//! `Rc<RefCell<AppState>>`. Widgets read it through `*_when`/`dynamic` closures and
//! mutate it from `on_press`/event handlers, then ask the runtime to relayout.

use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

use sui::DefaultTheme;

use crate::backend::Backend;
use crate::theme::{AppThemeMode, resolve_theme};

/// The three top-level destinations shown in the title bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    /// AI chat with the Sinomo agent.
    Sinomo,
    /// SIFS distributed file browser.
    Files,
    /// App + cluster settings.
    Settings,
}

impl Tab {
    pub const ALL: [Tab; 3] = [Tab::Sinomo, Tab::Files, Tab::Settings];

    pub fn index(self) -> usize {
        match self {
            Tab::Sinomo => 0,
            Tab::Files => 1,
            Tab::Settings => 2,
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Tab::Sinomo => "Sinomo",
            Tab::Files => "Files",
            Tab::Settings => "Settings",
        }
    }
}

/// Root application state. Sub-tab state is held in dedicated modules.
pub struct AppState {
    pub active_tab: Tab,
    pub theme_mode: AppThemeMode,
    pub backend: Backend,
    pub chat: crate::ui::chat::ChatState,
    pub files: crate::ui::files::FilesState,
    pub settings: crate::ui::settings::SettingsState,
}

impl AppState {
    pub fn new(backend: Backend) -> Self {
        let chat = crate::ui::chat::ChatState::new(&backend);
        let files = crate::ui::files::FilesState::new(&backend);
        let settings = crate::ui::settings::SettingsState::new(&backend);
        Self {
            active_tab: Tab::Sinomo,
            theme_mode: AppThemeMode::System,
            backend,
            chat,
            files,
            settings,
        }
    }

    /// The resolved widget theme for the current mode.
    pub fn theme(&self) -> DefaultTheme {
        resolve_theme(self.theme_mode)
    }
}

/// Cheap, cloneable handle to the shared application state.
#[derive(Clone)]
pub struct Shared(Rc<RefCell<AppState>>);

impl Shared {
    pub fn new(state: AppState) -> Self {
        Self(Rc::new(RefCell::new(state)))
    }

    pub fn borrow(&self) -> std::cell::Ref<'_, AppState> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, AppState> {
        self.0.borrow_mut()
    }

    pub fn theme(&self) -> DefaultTheme {
        self.0.borrow().theme()
    }

    pub fn active_tab(&self) -> Tab {
        self.0.borrow().active_tab
    }

    /// Borrow the chat sub-state directly.
    pub fn chat(&self) -> Ref<'_, crate::ui::chat::ChatState> {
        Ref::map(self.0.borrow(), |state| &state.chat)
    }

    pub fn chat_mut(&self) -> RefMut<'_, crate::ui::chat::ChatState> {
        RefMut::map(self.0.borrow_mut(), |state| &mut state.chat)
    }

    /// Borrow the files sub-state directly.
    pub fn files(&self) -> Ref<'_, crate::ui::files::FilesState> {
        Ref::map(self.0.borrow(), |state| &state.files)
    }

    pub fn files_mut(&self) -> RefMut<'_, crate::ui::files::FilesState> {
        RefMut::map(self.0.borrow_mut(), |state| &mut state.files)
    }

    /// Borrow the settings sub-state directly.
    pub fn settings(&self) -> Ref<'_, crate::ui::settings::SettingsState> {
        Ref::map(self.0.borrow(), |state| &state.settings)
    }

    pub fn settings_mut(&self) -> RefMut<'_, crate::ui::settings::SettingsState> {
        RefMut::map(self.0.borrow_mut(), |state| &mut state.settings)
    }
}
