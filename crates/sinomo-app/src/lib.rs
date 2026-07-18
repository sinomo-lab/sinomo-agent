//! Sinomo's graphical client app, built on the SUI toolkit.
//!
//! Five tabs sit under a brand title bar: **Chat** (agent chat), **Evolution**
//! (self-evolution jobs and evidence), **Cluster** (network topology graph), **Files**
//! (the SIFS distributed file browser) and **Settings** (app + cluster config).
//! The app probes for a local daemon and, if none is found, runs as an embedded
//! desktop app surface so it stays daemon-backed during bootstrap.

pub mod backend;
mod native_picker;
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

#[cfg(all(target_os = "android", feature = "mobile"))]
const ANDROID_SAFE_AREA_CHANGED: sui::CommandKey<sui::Insets> =
    sui::CommandKey::new("sinomo.android.safe-area-changed");

#[cfg(all(target_os = "android", feature = "mobile"))]
static ANDROID_SAFE_AREA: std::sync::OnceLock<std::sync::RwLock<sui::Insets>> =
    std::sync::OnceLock::new();

#[cfg(all(target_os = "android", feature = "mobile"))]
static ANDROID_APP: std::sync::OnceLock<sui::AndroidApp> = std::sync::OnceLock::new();

pub(crate) fn platform_safe_area_insets() -> sui::Insets {
    #[cfg(all(target_os = "android", feature = "mobile"))]
    {
        return *ANDROID_SAFE_AREA
            .get_or_init(|| std::sync::RwLock::new(sui::Insets::ZERO))
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    #[cfg(not(all(target_os = "android", feature = "mobile")))]
    sui::Insets::ZERO
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn set_android_safe_area_insets(insets: sui::Insets) {
    *ANDROID_SAFE_AREA
        .get_or_init(|| std::sync::RwLock::new(sui::Insets::ZERO))
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = insets;
}

/// Construct the application around a given shared state. Exposed so headless
/// tooling (visual tests) can build and render the same widget tree the app uses.
pub fn build_application(shared: Shared) -> sui::Result<App> {
    App::new()
        .render_options(shared.render_options())
        .with_resources(|resources| {
            let logo = default_sui_logo_image(96)?;
            resources.image(logo_image_handle(), logo)
        })
        .map(|app| {
            app.window(
                Window::new("Sinomo")
                    .controller(crate::ui::shell::BackgroundWorkController::new(
                        shared.clone(),
                    ))
                    .root(Shell::new(shared)),
            )
        })
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
    configure_android_data_dir(&android_app);
    let _ = ANDROID_APP.set(android_app.clone());
    set_android_safe_area_insets(sui::Insets::ZERO);
    install_app_observability();
    let shared = Shared::new(AppState::connecting());
    let app = build_application(shared.clone())?;
    let safe_area_app = android_app.clone();
    app.run_android_with_handle(android_app, move |ui| {
        shared.borrow_mut().waker = Some(ui.clone());
        start_android_safe_area_monitor(safe_area_app, ui.clone());
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

/// Open a validated web URL through Android's resolver. Provider-login links
/// originate on Winit's native event-loop thread, while Android framework calls
/// must run on the Java main thread, so the small JNI operation is posted there
/// through the `AndroidApp` retained at startup.
#[cfg(all(target_os = "android", feature = "mobile"))]
pub(crate) fn open_android_external_url(url: &str) -> Result<(), String> {
    let android_app = ANDROID_APP
        .get()
        .cloned()
        .ok_or_else(|| "Android activity is not available".to_string())?;
    let url = url.to_string();
    let app = android_app.clone();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    android_app.run_on_java_main_thread(Box::new(move || {
        let result = open_android_external_url_on_java_main_thread(&app, &url)
            .map_err(|error| error.to_string());
        let _ = send.send(result);
    }));
    receive
        .recv_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| format!("Android browser launch did not complete: {error}"))?
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn open_android_external_url_on_java_main_thread(
    android_app: &sui::AndroidApp,
    url: &str,
) -> jni::errors::Result<()> {
    use jni::objects::{JObject, JValue};
    use jni::refs::Global;

    let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
    let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let url: JObject = env.new_string(url)?.into();
        let uri = env
            .call_static_method(
                jni::jni_str!("android/net/Uri"),
                jni::jni_str!("parse"),
                jni::jni_sig!("(Ljava/lang/String;)Landroid/net/Uri;"),
                &[JValue::Object(&url)],
            )?
            .l()?;
        let action: JObject = env.new_string("android.intent.action.VIEW")?.into();
        let intent = env.new_object(
            jni::jni_str!("android/content/Intent"),
            jni::jni_sig!("(Ljava/lang/String;Landroid/net/Uri;)V"),
            &[JValue::Object(&action), JValue::Object(&uri)],
        )?;
        env.call_method(
            &activity,
            jni::jni_str!("startActivity"),
            jni::jni_sig!("(Landroid/content/Intent;)V"),
            &[JValue::Object(&intent)],
        )?;
        Ok(())
    })
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn start_android_safe_area_monitor(android_app: sui::AndroidApp, ui: sui::UiHandle) {
    std::thread::Builder::new()
        .name("sinomo-app-insets".to_string())
        .spawn(move || {
            let mut last = None;
            loop {
                let insets = android_safe_area_insets(&android_app);
                if last != Some(insets) {
                    last = Some(insets);
                    let content = android_app.content_rect();
                    let config = android_app.config();
                    tracing::info!(
                        ?content,
                        density = ?config.density(),
                        screen_width_dp = ?config.screen_width_dp(),
                        screen_height_dp = ?config.screen_height_dp(),
                        ?insets,
                        "Android safe area changed"
                    );
                    ui.broadcast_application(ANDROID_SAFE_AREA_CHANGED, insets);
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        })
        .expect("spawn Android safe-area monitor");
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_safe_area_insets(android_app: &sui::AndroidApp) -> sui::Insets {
    let config = android_app.config();
    let scale = config.density().unwrap_or(160).max(1) as f32 / 160.0;
    android_window_insets(android_app)
        .map(|insets| safe_area_insets_from_physical(insets, scale))
        .unwrap_or(sui::Insets::ZERO)
}

#[cfg(any(all(target_os = "android", feature = "mobile"), test))]
fn safe_area_insets_from_physical(insets: [i32; 4], scale: f32) -> sui::Insets {
    let scale = scale.max(f32::EPSILON);
    sui::Insets {
        left: insets[0].max(0) as f32 / scale,
        top: insets[1].max(0) as f32 / scale,
        right: insets[2].max(0) as f32 / scale,
        bottom: insets[3].max(0) as f32 / scale,
    }
}

/// Read the system-bar and display-cutout insets from the NativeActivity's
/// decor view. `content_rect()` spans the entire edge-to-edge surface when
/// targeting recent Android releases, so it cannot describe the safe area.
#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_window_insets(android_app: &sui::AndroidApp) -> Option<[i32; 4]> {
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let app = android_app.clone();
    android_app.run_on_java_main_thread(Box::new(move || {
        let _ = send.send(android_window_insets_on_java_main_thread(&app));
    }));
    match receive.recv_timeout(std::time::Duration::from_millis(500)) {
        Ok(Ok(insets)) => insets,
        Ok(Err(error)) => {
            tracing::debug!(%error, "failed to read Android window insets");
            None
        }
        Err(error) => {
            tracing::debug!(%error, "Android window-insets query did not complete");
            None
        }
    }
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_window_insets_on_java_main_thread(
    android_app: &sui::AndroidApp,
) -> jni::errors::Result<Option<[i32; 4]>> {
    use jni::objects::{JObject, JValue};
    use jni::refs::Global;

    let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
    let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let window = env
            .call_method(
                &activity,
                jni::jni_str!("getWindow"),
                jni::jni_sig!("()Landroid/view/Window;"),
                &[],
            )?
            .l()?;
        let decor = env
            .call_method(
                &window,
                jni::jni_str!("getDecorView"),
                jni::jni_sig!("()Landroid/view/View;"),
                &[],
            )?
            .l()?;
        let root_insets = env
            .call_method(
                &decor,
                jni::jni_str!("getRootWindowInsets"),
                jni::jni_sig!("()Landroid/view/WindowInsets;"),
                &[],
            )?
            .l()?;
        if root_insets.is_null() {
            env.call_method(
                &decor,
                jni::jni_str!("requestApplyInsets"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            return Ok(None);
        }

        let sdk = env
            .get_static_field(
                jni::jni_str!("android/os/Build$VERSION"),
                jni::jni_str!("SDK_INT"),
                jni::jni_sig!("I"),
            )?
            .i()?;
        if sdk >= 30 {
            let system_bars = env
                .call_static_method(
                    jni::jni_str!("android/view/WindowInsets$Type"),
                    jni::jni_str!("systemBars"),
                    jni::jni_sig!("()I"),
                    &[],
                )?
                .i()?;
            let display_cutout = env
                .call_static_method(
                    jni::jni_str!("android/view/WindowInsets$Type"),
                    jni::jni_str!("displayCutout"),
                    jni::jni_sig!("()I"),
                    &[],
                )?
                .i()?;
            let insets = env
                .call_method(
                    &root_insets,
                    jni::jni_str!("getInsetsIgnoringVisibility"),
                    jni::jni_sig!("(I)Landroid/graphics/Insets;"),
                    &[JValue::Int(system_bars | display_cutout)],
                )?
                .l()?;
            return Ok(Some([
                env.get_field(&insets, jni::jni_str!("left"), jni::jni_sig!("I"))?
                    .i()?,
                env.get_field(&insets, jni::jni_str!("top"), jni::jni_sig!("I"))?
                    .i()?,
                env.get_field(&insets, jni::jni_str!("right"), jni::jni_sig!("I"))?
                    .i()?,
                env.get_field(&insets, jni::jni_str!("bottom"), jni::jni_sig!("I"))?
                    .i()?,
            ]));
        }

        let mut insets = [0; 4];
        for (index, method) in [
            jni::jni_str!("getStableInsetLeft"),
            jni::jni_str!("getStableInsetTop"),
            jni::jni_str!("getStableInsetRight"),
            jni::jni_str!("getStableInsetBottom"),
        ]
        .into_iter()
        .enumerate()
        {
            insets[index] = env
                .call_method(&root_insets, method, jni::jni_sig!("()I"), &[])?
                .i()?;
        }
        if sdk >= 28 {
            let cutout = env
                .call_method(
                    &root_insets,
                    jni::jni_str!("getDisplayCutout"),
                    jni::jni_sig!("()Landroid/view/DisplayCutout;"),
                    &[],
                )?
                .l()?;
            if !cutout.is_null() {
                for (index, method) in [
                    jni::jni_str!("getSafeInsetLeft"),
                    jni::jni_str!("getSafeInsetTop"),
                    jni::jni_str!("getSafeInsetRight"),
                    jni::jni_str!("getSafeInsetBottom"),
                ]
                .into_iter()
                .enumerate()
                {
                    let cutout_inset = env
                        .call_method(&cutout, method, jni::jni_sig!("()I"), &[])?
                        .i()?;
                    insets[index] = insets[index].max(cutout_inset);
                }
            }
        }
        Ok(Some(insets))
    })
}

/// Bind every embedded subsystem to Android's durable app-private files
/// directory before observability, daemon, DFS, or API clients are initialized.
/// NativeActivity does not provide `HOME`, and the temporary-directory fallback
/// points at Android's evictable cache directory.
#[cfg(all(target_os = "android", feature = "mobile"))]
fn configure_android_data_dir(android_app: &sui::AndroidApp) {
    let Some(internal_data_path) = android_app.internal_data_path() else {
        eprintln!("sinomo-app: Android internal data path is unavailable");
        return;
    };
    let durable_root = internal_data_path.join("sinomo");
    migrate_android_cache_data_dir(&durable_root);
    let installed = sinomo_runtime::local::install_process_data_dir(durable_root.clone());
    if installed != durable_root {
        eprintln!(
            "sinomo-app: process data directory was already set to {} instead of {}",
            installed.display(),
            durable_root.display()
        );
    }
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn migrate_android_cache_data_dir(durable_root: &std::path::Path) {
    let cache_root = std::env::temp_dir().join("sinomo");
    if cache_root == durable_root || !cache_root.exists() || durable_root.exists() {
        return;
    }
    if let Some(parent) = durable_root.parent()
        && let Err(error) = std::fs::create_dir_all(parent)
    {
        eprintln!(
            "sinomo-app: failed to prepare durable Android data directory {}: {error}",
            parent.display()
        );
        return;
    }
    if let Err(error) = std::fs::rename(&cache_root, durable_root) {
        eprintln!(
            "sinomo-app: failed to migrate Android data from {} to {}: {error}",
            cache_root.display(),
            durable_root.display()
        );
    }
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
    use super::safe_area_insets_from_physical;
    use crate::rendering::{AppRenderMode, resolve_render_options};
    use std::path::{Path, PathBuf};
    use sui::{WindowColorManagementMode, WindowStemDarkening, WindowTextHinting};

    #[test]
    fn android_safe_area_converts_physical_insets_to_logical_units() {
        let insets = safe_area_insets_from_physical([0, 85, 0, 84], 2.625);

        assert_eq!(insets.left, 0.0);
        assert!((insets.top - 32.380_95).abs() < 0.001);
        assert_eq!(insets.right, 0.0);
        assert_eq!(insets.bottom, 32.0);
    }

    #[test]
    fn render_options_match_widget_book_text_policy() {
        let options = resolve_render_options(AppRenderMode::Automatic);

        assert_eq!(options.text_hinting, WindowTextHinting::default());
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
