//! Sinomo's graphical client app, built on the SUI toolkit.
//!
//! Four tabs sit under a brand title bar: **Sinomo** (agent chat), **Cluster**
//! (network topology graph), **Files** (the SIFS distributed file browser) and
//! **Settings** (app + cluster config).
//! The app probes for a local daemon and, if none is found, runs as an embedded
//! mobile node so it stays useful in local mode.

pub mod backend;
pub mod state;
pub mod theme;
pub mod ui;

use sui::{
    App, Window, WindowRenderOptions, WindowStemDarkening, WindowTextHinting,
    default_sui_logo_image,
};

use crate::backend::Backend;
use crate::state::{AppState, ConnectEvent, Shared, Tab};
use crate::ui::Shell;
use crate::ui::shell::logo_image_handle;

/// Crisp text: hint at small sizes and darken stems so light backgrounds stay
/// legible.
fn render_options() -> WindowRenderOptions {
    WindowRenderOptions::new(true, 1.0)
        .with_text_hinting(WindowTextHinting::Slight { max_ppem: 96.0 })
        .with_stem_darkening(WindowStemDarkening::Enabled {
            max_ppem: 72.0,
            amount: 0.4,
        })
}

/// Construct the application around a given shared state. Exposed so headless
/// tooling (visual tests) can build and render the same widget tree the app uses.
pub fn build_application(shared: Shared) -> sui::Result<App> {
    App::new()
        .render_options(render_options())
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
    let runtime = build_application(shared)?.build()?;
    let options = render_options();
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
                let _ = tx.send(ConnectEvent::Connected(Box::new(backend)));
                ui.wake();
            })
            .expect("spawn connect thread");
    })
}
