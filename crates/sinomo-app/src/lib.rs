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
use crate::state::{AppState, Shared, Tab};
#[cfg(any(feature = "desktop", all(target_os = "android", feature = "mobile")))]
use crate::state::{ConnectEvent, ReadyState};
use crate::ui::Shell;
use crate::ui::shell::logo_image_handle;

/// Construct the application around a given shared state. Exposed so headless
/// tooling (visual tests) can build and render the same widget tree the app uses.
pub fn build_application(shared: Shared) -> sui::Result<App> {
    let external_textures = shared.external_texture_registry();
    App::new()
        .external_texture_registry(external_textures)
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
#[cfg(feature = "desktop")]
pub fn run() -> sui::Result<()> {
    install_app_observability();
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
                let ready = ReadyState::build_with_waker_and_progress(
                    backend,
                    Some(ui.clone()),
                    |status| {
                        tracing::info!(%status, "sinomo-app launch step");
                        let _ = tx.send(ConnectEvent::Status(status.to_string()));
                        ui.wake();
                    },
                );
                let _ = tx.send(ConnectEvent::Connected(Box::new(ready)));
                ui.wake();
            })
            .expect("spawn connect thread");
    })
}

/// Build and run the Android application from the NativeActivity entrypoint.
#[cfg(all(target_os = "android", feature = "mobile"))]
pub fn run_android(android_app: sui::AndroidApp) -> sui::Result<()> {
    install_app_observability();
    let shared = Shared::new(AppState::connecting());
    let app = build_application(shared.clone())?;
    app.run_android_with_handle(android_app, move |ui| {
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
                let ready = ReadyState::build_with_waker_and_progress(
                    backend,
                    Some(ui.clone()),
                    |status| {
                        tracing::info!(%status, "sinomo-app launch step");
                        let _ = tx.send(ConnectEvent::Status(status.to_string()));
                        ui.wake();
                    },
                );
                let _ = tx.send(ConnectEvent::Connected(Box::new(ready)));
                ui.wake();
            })
            .expect("spawn connect thread");
    })
}

/// Install the observe/tracing runtime with the app's log file
/// (`<data_dir>/logs/app.log`) as an additional sink. The graphical app has no
/// supervised stdout, so without this every `tracing` diagnostic — including
/// launch-stall and SIFS-degradation warnings — would be silently dropped. Errors
/// here must never stop the app from opening.
#[cfg(any(feature = "desktop", all(target_os = "android", feature = "mobile")))]
fn install_app_observability() {
    let data_dir = crate::backend::app_config::default_data_dir();
    let logs_dir = data_dir.root().join("logs");
    let log_path = logs_dir.join("app.log");
    let _ = std::fs::create_dir_all(&logs_dir);
    rotate_app_log_if_large(&log_path);
    let config = sinomo_runtime::observe::ObserveRuntimeConfig::process("app", None);
    match sinomo_runtime::observe::init_observe_runtime_with_log_file(config, &log_path) {
        Ok(()) => {
            tracing::info!(
                version = env!("CARGO_PKG_VERSION"),
                log = %log_path.display(),
                "sinomo-app starting"
            );
        }
        Err(error) => {
            eprintln!("sinomo-app: observability init failed: {error}");
        }
    }
}

/// Keep the append-only app log bounded: past ~5 MB the previous log rolls to
/// `app.log.1` (replacing the roll before it) at startup.
#[cfg(any(feature = "desktop", all(target_os = "android", feature = "mobile")))]
fn rotate_app_log_if_large(path: &std::path::Path) {
    const MAX_APP_LOG_BYTES: u64 = 5 * 1024 * 1024;
    let large = std::fs::metadata(path)
        .map(|meta| meta.len() > MAX_APP_LOG_BYTES)
        .unwrap_or(false);
    if large {
        let mut rotated = path.as_os_str().to_owned();
        rotated.push(".1");
        let rotated = std::path::PathBuf::from(rotated);
        let _ = std::fs::remove_file(&rotated);
        let _ = std::fs::rename(path, rotated);
    }
}

#[cfg(test)]
mod tests {
    use crate::rendering::{AppRenderMode, resolve_render_options};
    use std::path::{Path, PathBuf};
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

    #[test]
    fn daemon_impl_imports_stay_in_embedded_backend_wrapper() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let src_dir = manifest_dir.join("src");
        let mut violations = Vec::new();
        let forbidden = ["sinomo_runtime", "::daemon"].concat();
        collect_daemon_impl_import_violations(&src_dir, &src_dir, &forbidden, &mut violations);

        assert!(
            violations.is_empty(),
            "app code should use sinomo_runtime::api; daemon implementation imports are allowed only in backend/mod.rs: {}",
            violations.join(", ")
        );
    }

    fn collect_daemon_impl_import_violations(
        root: &Path,
        dir: &Path,
        forbidden: &str,
        violations: &mut Vec<String>,
    ) {
        for entry in std::fs::read_dir(dir).expect("read app source directory") {
            let entry = entry.expect("read app source entry");
            let path = entry.path();
            if path.is_dir() {
                collect_daemon_impl_import_violations(root, &path, forbidden, violations);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .expect("app source file under root")
                .display()
                .to_string()
                .replace('\\', "/");
            if relative == "backend/mod.rs" {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read app source file");
            if source.contains(forbidden) {
                violations.push(relative);
            }
        }
    }
}
