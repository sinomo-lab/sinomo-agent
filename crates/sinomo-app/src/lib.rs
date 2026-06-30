//! Sinomo's graphical client app, built on the SUI toolkit.
//!
//! Five tabs sit under a brand title bar: **Chat** (agent chat), **Evolution**
//! (self-evolution jobs and evidence), **Cluster** (network topology graph), **Files**
//! (the SIFS distributed file browser) and **Settings** (app + cluster config).
//! The app probes for a local daemon and, if none is found, runs as an embedded
//! desktop app surface so it stays daemon-backed during bootstrap.

pub mod backend;
pub mod rendering;
pub mod state;
pub mod theme;
pub mod ui;

use sui::{App, Window, default_sui_logo_image};

use crate::backend::Backend;
use crate::state::{AppState, ConnectEvent, ReadyState, Shared, Tab};
use crate::ui::Shell;
use crate::ui::shell::logo_image_handle;

/// Construct the application around a given shared state. Exposed so headless
/// tooling (visual tests) can build and render the same widget tree the app uses.
pub fn build_application(shared: Shared) -> sui::Result<App> {
    App::new()
        .render_options(shared.render_options())
        .with_resources(|resources| {
            let logo = default_sui_logo_image(96)?;
            resources.image(logo_image_handle(), logo)
        })
        .map(|app| app.window(Window::new("Sinomo").root(Shell::new(shared))))
}

/// Build a fully-resolved shared state with a starting tab. Uses a synchronous, daemonless
/// backend — intended for headless tooling and visual tests, which render the tabs directly.
/// The interactive app uses the non-blocking path in [`run`].
pub fn boot(initial_tab: Tab) -> Shared {
    let backend = Backend::local_fallback();
    let mut state = AppState::ready(backend);
    state.active_tab = initial_tab;
    Shared::new(state)
}

/// Build a runtime with render options applied (for headless tooling/tests).
pub fn build_runtime(shared: Shared) -> sui::Result<sui::Runtime> {
    let options = shared.render_options();
    let runtime = build_application(shared)?.build()?;
    for window_id in runtime.window_ids() {
        sui::set_window_render_options(window_id, options);
    }
    Ok(runtime)
}

/// Build and run the desktop application. Blocks until the window closes.
///
/// The window opens immediately in a *connecting* state; the backend (daemon probe / starting an
/// in-process daemon) resolves on a background thread, which wakes the UI as it progresses and
/// once it's ready. This keeps launch responsive even when the in-process daemon takes a moment.
pub fn run() -> sui::Result<()> {
    let shared = Shared::new(AppState::connecting());
    let app = build_application(shared.clone())?;
    app.run_with_handle(move |ui| {
        // Stash a waker so background work (e.g. provider device-login) can refresh the UI.
        shared.borrow_mut().waker = Some(ui.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        shared.borrow_mut().connect_rx = Some(rx);
        std::thread::Builder::new()
            .name("sinomo-app-connect".to_string())
            .spawn(move || {
                let backend = Backend::connect(|status| {
                    let _ = tx.send(ConnectEvent::Status(status.to_string()));
                    ui.wake();
                });
                tracing::info!(mode = %backend.mode_label(), "sinomo-app connected");
                let _ = tx.send(ConnectEvent::Status("Loading workspace…".to_string()));
                ui.wake();
                let ready = ReadyState::build_with_waker(backend, Some(ui.clone()));
                let _ = tx.send(ConnectEvent::Connected(Box::new(ready)));
                ui.wake();
            })
            .expect("spawn connect thread");
    })
}

#[cfg(test)]
mod tests {
    use crate::rendering::{AppRenderMode, resolve_render_options};
    use sui::{WindowColorManagementMode, WindowStemDarkening, WindowTextHinting};

    #[test]
    fn render_options_match_widget_book_text_policy() {
        let options = resolve_render_options(AppRenderMode::Automatic);

        assert_eq!(options.text_hinting, WindowTextHinting::None);
        assert_eq!(options.stem_darkening, WindowStemDarkening::None);
        assert_eq!(
            options.color_management_mode,
            WindowColorManagementMode::Automatic
        );
    }

    #[test]
    fn render_options_can_force_sdr_for_debug_capture() {
        let options = resolve_render_options(AppRenderMode::ForceSdr);

        assert_eq!(
            options.color_management_mode,
            WindowColorManagementMode::ForceSdr
        );
    }
}
