//! Sinomo's graphical client app, built on the SUI toolkit.
//!
//! Pinned icons provide Chat, Cluster, Evolution, Files, and Settings navigation. Independent
//! chat, file browser, and document tabs can be opened and closed alongside them.
//! The app probes for a local daemon and, if none is found, starts an app-owned sibling daemon
//! process on desktop so the UI stays daemon-backed without linking the daemon host into the GUI.

pub mod backend;
pub mod external_edit;
pub mod metrics;
mod mobile_runtime;
mod native_picker;
pub mod rendering;
mod restoration;
pub mod state;
pub mod theme;
pub mod ui;
mod updates;

use sui::{App, Window};
use sui_lucide::LucideIcon;

use crate::backend::Backend;
use crate::state::{AppState, Shared, Tab};
#[cfg(any(feature = "desktop", all(target_os = "android", feature = "mobile")))]
use crate::state::{ConnectEvent, ReadyState};
use crate::ui::Shell;
use crate::ui::shell::{logo_image_handle, tab_icons};

const LOGO_SVG: &[u8] = include_bytes!("../assets/logo.svg");

#[cfg(all(target_os = "android", feature = "mobile"))]
const ANDROID_SAFE_AREA_CHANGED: sui::CommandKey<sui::Insets> =
    sui::CommandKey::new("sinomo.android.safe-area-changed");

#[cfg(all(target_os = "android", feature = "mobile"))]
static ANDROID_SAFE_AREA: std::sync::OnceLock<std::sync::RwLock<sui::Insets>> =
    std::sync::OnceLock::new();

#[cfg(all(target_os = "android", feature = "mobile"))]
static ANDROID_APP: std::sync::OnceLock<sui::AndroidApp> = std::sync::OnceLock::new();

#[cfg(any(test, all(target_os = "android", feature = "mobile")))]
mod android_app_use_consent;

#[cfg(all(target_os = "android", feature = "mobile"))]
static ANDROID_UI_HANDLE: std::sync::OnceLock<std::sync::Mutex<Option<sui::UiHandle>>> =
    std::sync::OnceLock::new();

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

#[cfg(all(target_os = "android", feature = "mobile"))]
pub(crate) fn sync_android_system_bars(mode: theme::AppThemeMode) {
    use std::sync::atomic::{AtomicU8, Ordering};
    static APPEARANCE: AtomicU8 = AtomicU8::new(0);
    let light = mode == theme::AppThemeMode::Light;
    let appearance = if light { 1 } else { 2 };
    let Some(app) = ANDROID_APP.get() else { return };
    if APPEARANCE.swap(appearance, Ordering::Relaxed) == appearance {
        return;
    }
    let android_app = app.clone();
    app.run_on_java_main_thread(Box::new(move || {
        use jni::objects::JObject;
        use jni::refs::Global;
        use jni::{jni_sig, jni_str};
        let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
        let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
        let result: jni::errors::Result<()> = vm.attach_current_thread(|env| {
            let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
            let window = env
                .call_method(
                    &activity,
                    jni_str!("getWindow"),
                    jni_sig!("()Landroid/view/Window;"),
                    &[],
                )?
                .l()?;
            let decor = env
                .call_method(
                    &window,
                    jni_str!("getDecorView"),
                    jni_sig!("()Landroid/view/View;"),
                    &[],
                )?
                .l()?;
            let flags = env
                .call_method(
                    &decor,
                    jni_str!("getSystemUiVisibility"),
                    jni_sig!("()I"),
                    &[],
                )?
                .i()?;
            // LIGHT_STATUS_BAR (API 23) and LIGHT_NAVIGATION_BAR (API 26).
            let mask = 0x2000 | 0x10;
            let flags = if light { flags | mask } else { flags & !mask };
            env.call_method(
                &decor,
                jni_str!("setSystemUiVisibility"),
                jni_sig!("(I)V"),
                &[flags.into()],
            )?;
            Ok(())
        });
        if let Err(error) = result {
            APPEARANCE.store(0, Ordering::Relaxed);
            tracing::warn!(%error, "Android system bar appearance update failed");
        }
    }));
}

/// Construct the application around a given shared state. Exposed so headless
/// tooling (visual tests) can build and render the same widget tree the app uses.
pub fn build_application(shared: Shared) -> sui::Result<App> {
    App::new()
        .render_options(shared.render_options())
        .with_resources(|resources| {
            for icon in tab_icons().into_iter().chain([LucideIcon::User]) {
                resources.image(icon.handle(), icon.registered_mask_image()?)?;
            }
            resources.svg_image_at_size_with_handle(logo_image_handle(), 96, 96, LOGO_SVG)
        })
        .map(|app| {
            let placement = shared.borrow().window_placement;
            let mut window = Window::new("Sinomo").icon_svg(LOGO_SVG);
            if let Some([width, height]) = placement.size
                && width.is_finite()
                && height.is_finite()
                && width > 0.0
                && height > 0.0
            {
                window = window.initial_size(sui::Size::new(width, height));
            }
            if let Some([x, y]) = placement.position
                && x.is_finite()
                && y.is_finite()
            {
                window = window.initial_position(sui::Point::new(x, y));
            }
            app.window(
                window
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
/// app-owned daemon process) resolves on a background thread, which wakes the UI as it progresses
/// and once it's ready. This keeps launch responsive while the daemon becomes ready.
#[cfg(feature = "desktop")]
pub fn run() -> sui::Result<()> {
    install_app_observability();
    let shared = Shared::new(AppState::connecting());
    let app = build_application(shared.clone())?;
    let final_shared = shared.clone();
    let platform = sui::DesktopPlatform::new().with_extension(shared.borrow().reports.clone());
    let result = app.run_with_platform_and_handle(platform, move |ui| {
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
    });
    if final_shared.is_ready()
        && let Err(error) = crate::restoration::save(&final_shared)
    {
        tracing::warn!(%error, "workspace save failed");
    }
    sinomo_client::api::observe::recording::shutdown_producers(std::time::Duration::from_secs(5));
    result
}

/// Build and run the Android application from the NativeActivity entrypoint.
#[cfg(all(target_os = "android", feature = "mobile"))]
pub fn run_android(android_app: sui::AndroidApp) -> sui::Result<()> {
    configure_android_data_dir(&android_app);
    let _ = ANDROID_APP.set(android_app.clone());
    set_android_safe_area_insets(sui::Insets::ZERO);
    initialize_android_tls(&android_app)
        .map_err(|error| sui::Error::new(format!("Android TLS initialization failed: {error}")))?;
    install_app_observability();
    let shared = Shared::new(AppState::connecting());
    let app = build_application(shared.clone())?;
    let conditions_app = android_app.clone();
    let lifecycle_app = android_app.clone();
    app.run_android_with_handle(android_app, move |ui| {
        *ANDROID_UI_HANDLE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ui.clone());
        shared.borrow_mut().waker = Some(ui.clone());
        mobile_runtime::start_android_lifecycle_monitor(lifecycle_app);
        mobile_runtime::start_android_operating_conditions_monitor(conditions_app, ui.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        shared.borrow_mut().connect_rx = Some(rx);
        std::thread::Builder::new()
            .name("sinomo-app-connect".to_string())
            .spawn(move || {
                mobile_runtime::wait_until_interactive_blocking();
                let backend = Backend::connect_with_node_resources(
                    |status| {
                        let _ = tx.send(ConnectEvent::Status(status.to_string()));
                        ui.wake();
                    },
                    mobile_runtime::node_resource_monitor(),
                );
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

/// Install Android's certificate verifier before any background HTTP client can
/// start. Keep the application context, rather than retaining an activity.
#[cfg(all(target_os = "android", feature = "mobile"))]
fn initialize_android_tls(android_app: &sui::AndroidApp) -> jni::errors::Result<()> {
    use jni::objects::JObject;
    use jni::refs::Global;

    let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
    let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let context = env
            .call_method(
                &activity,
                jni::jni_str!("getApplicationContext"),
                jni::jni_sig!("()Landroid/content/Context;"),
                &[],
            )?
            .l()?;
        rustls_platform_verifier::android::init_with_env(env, context)
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
pub(crate) fn android_safe_area_changed_physical(physical: [i32; 4], scale: f32) {
    // Java inset callbacks must not lock NativeActivity configuration: its
    // native event thread can be waiting for the Java thread to acknowledge
    // the same resize. Android supplies density with the inset sample instead.
    let insets = safe_area_insets_from_physical(physical, scale);
    if platform_safe_area_insets() == insets {
        return;
    }
    set_android_safe_area_insets(insets);
    tracing::info!(?insets, "Android safe area changed");
    let ui = ANDROID_UI_HANDLE
        .get()
        .and_then(|ui| ui.lock().ok().and_then(|ui| ui.as_ref().cloned()));
    if let Some(ui) = ui {
        ui.broadcast_application(ANDROID_SAFE_AREA_CHANGED, insets);
    }
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
    #[cfg(target_os = "android")]
    let initialized = sinomo_runtime::observe::init_observe_runtime_with_log_file(
        sinomo_runtime::observe::ObserveRuntimeConfig::process("app", None),
        &log_path,
    )
    .map_err(|error| error.to_string());
    #[cfg(not(target_os = "android"))]
    let initialized = (|| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| error.to_string())?;
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn,sinomo=info".into()),
            )
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(file))
            .try_init()
            .map_err(|error| error.to_string())
    })();
    match initialized {
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
    fn daemon_impl_imports_stay_in_android_embedded_backend_wrapper() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let src_dir = manifest_dir.join("src");
        let mut violations = Vec::new();
        let forbidden = ["sinomo_runtime", "::daemon"].concat();
        collect_daemon_impl_import_violations(&src_dir, &src_dir, &forbidden, &mut violations);

        assert!(
            violations.is_empty(),
            "desktop app code should use sinomo_client::api; the Android implementation exception is isolated in backend/mod.rs: {}",
            violations.join(", ")
        );
    }

    #[test]
    fn desktop_binary_does_not_enable_daemon_host_features_or_cli_mode() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let manifest =
            std::fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read app manifest");
        let desktop_feature = manifest
            .split("desktop = [")
            .nth(1)
            .and_then(|tail| tail.split(']').next())
            .expect("desktop feature");
        assert!(!desktop_feature.contains("sinomo-cli/sandbox"));
        assert!(!desktop_feature.contains("sinomo-cli/dfs-fuse"));
        assert!(!desktop_feature.contains("sinomo-product-profile"));

        let main =
            std::fs::read_to_string(manifest_dir.join("src/main.rs")).expect("read app entrypoint");
        assert!(
            !main.contains("sinomo_cli::run"),
            "the graphical binary must not double as the full CLI host"
        );

        let client_dir = manifest_dir
            .parent()
            .expect("app crate parent")
            .join("sinomo-client");
        let supervisor = std::fs::read_to_string(client_dir.join("src/daemon_process.rs"))
            .expect("read desktop daemon supervisor");
        assert!(!supervisor.contains("run_daemon_control_loop"));
        assert!(!supervisor.contains("sinomo_product_profile"));
    }

    #[test]
    fn desktop_chat_engine_is_a_contract_client_not_an_agent_host() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let engine = std::fs::read_to_string(manifest_dir.join("src/backend/chat_engine.rs"))
            .expect("read desktop chat client actor");

        for forbidden in ["sinomo_agent", "sinomo_cli::app_api", "AgentChat"] {
            assert!(
                !engine.contains(forbidden),
                "desktop chat engine must not compile daemon-owned `{forbidden}` execution"
            );
        }
        assert!(engine.contains("sinomo_client::ChatDaemonClient"));
        assert!(engine.contains("sinomo_client::chat_contract"));
    }

    #[test]
    fn desktop_normal_graph_excludes_authority_owning_implementations() {
        use std::collections::BTreeSet;
        use std::process::Command;

        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .to_path_buf();
        let output = Command::new(env!("CARGO"))
            .args([
                "tree",
                "-p",
                "sinomo-app",
                "--edges",
                "normal,build",
                "--prefix",
                "none",
            ])
            .current_dir(workspace)
            .output()
            .expect("inspect desktop app dependency graph");
        assert!(
            output.status.success(),
            "cargo tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let tree = String::from_utf8_lossy(&output.stdout);
        let packages = tree
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|name| !name.starts_with('('))
            .collect::<BTreeSet<_>>();
        for forbidden in [
            "sinomo-dfs",
            "sinomo-dfs-nfs",
            "sinomo-module",
            "sinomo-observe",
            "sinomo-agent",
            "sinomo-cli",
            "sinomo-evolution",
            "sinomo-daemon",
            "sinomo-build",
            "sinomo-rollout",
            "sinomo-sandbox",
            "sinomo-sandbox-simple",
            "sinomo-product-profile",
            "sinomo-pytorch",
            "sinomo-memory",
            "sinomo-eval",
            "sinomo-installer",
            "sinomo-policy",
            "sinomo-registry",
            "sinomo-remote-management",
            "sinomo-supervision",
            "sinomo-test",
            "sinomo-vfs",
            "sinomo-vfs-sifs",
        ] {
            assert!(
                !packages.contains(forbidden),
                "desktop sinomo-app normal graph must not contain {forbidden}"
            );
        }
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

#[cfg(target_os = "android")]
mod android_media;
