//! Process-local Android operating conditions and foreground gate.
//!
//! Android may pause or kill the Activity independently from Sinomo's daemon
//! threads.  This module gives app-owned background work one shared gate and
//! records the current power/network shape without making those observations a
//! source of cluster authority.

use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum MobileNetworkTransport {
    #[default]
    Unknown,
    #[cfg(any(target_os = "android", test))]
    Cellular,
    #[cfg(any(target_os = "android", test))]
    Wifi,
    #[cfg(any(target_os = "android", test))]
    Ethernet,
    #[cfg(any(target_os = "android", test))]
    Vpn,
    #[cfg(any(target_os = "android", test))]
    Other,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MobileNetworkConditions {
    pub(crate) available: bool,
    pub(crate) validated: bool,
    pub(crate) metered: bool,
    pub(crate) roaming: Option<bool>,
    pub(crate) data_saver: bool,
    pub(crate) suspended: Option<bool>,
    pub(crate) congested: Option<bool>,
    pub(crate) transport: MobileNetworkTransport,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MobilePowerConditions {
    pub(crate) battery_percent: Option<u8>,
    pub(crate) charging: bool,
    pub(crate) power_saver: bool,
    pub(crate) thermal_status: Option<i32>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MobileOperatingConditions {
    pub(crate) network: MobileNetworkConditions,
    pub(crate) power: MobilePowerConditions,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum MobileResourceMode {
    #[default]
    Normal,
    Constrained,
    Restricted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MobilePassiveNetworkClass {
    /// Connectivity needed to keep the visible app usable.
    Essential,
    /// Metrics, status, and other passive observations which can be stale.
    Observability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MobileRuntimeSnapshot {
    activity_resumed: bool,
    window_focused: bool,
    lifecycle_observed: bool,
    interactive: bool,
    operating: MobileOperatingConditions,
    operating_observed: bool,
    generation: u64,
}

impl Default for MobileRuntimeSnapshot {
    fn default() -> Self {
        let active = !cfg!(target_os = "android");
        Self {
            // Android work opens only after both the authoritative Activity
            // lifecycle and window focus say the app is interactive. This
            // avoids starting passive workers for a background intent or while
            // a system surface covers the Activity.
            activity_resumed: active,
            window_focused: active,
            lifecycle_observed: active,
            interactive: active,
            operating: MobileOperatingConditions::default(),
            operating_observed: false,
            generation: 0,
        }
    }
}

struct MobileRuntimeControl {
    snapshot: Mutex<MobileRuntimeSnapshot>,
    changed: Condvar,
    updates: tokio::sync::watch::Sender<MobileRuntimeSnapshot>,
}

impl Default for MobileRuntimeControl {
    fn default() -> Self {
        let snapshot = MobileRuntimeSnapshot::default();
        let (updates, _) = tokio::sync::watch::channel(snapshot);
        Self {
            snapshot: Mutex::new(snapshot),
            changed: Condvar::new(),
            updates,
        }
    }
}

impl MobileRuntimeControl {
    #[cfg(any(target_os = "android", test))]
    fn set_activity_resumed(&self, resumed: bool) -> bool {
        let next = {
            let mut snapshot = self
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if snapshot.lifecycle_observed && snapshot.activity_resumed == resumed {
                return false;
            }
            snapshot.lifecycle_observed = true;
            snapshot.activity_resumed = resumed;
            snapshot.interactive = resumed && snapshot.window_focused;
            snapshot.generation = snapshot.generation.wrapping_add(1);
            *snapshot
        };
        self.updates.send_replace(next);
        self.changed.notify_all();
        true
    }

    #[cfg(any(target_os = "android", test))]
    fn set_window_focused(&self, focused: bool) -> bool {
        let next = {
            let mut snapshot = self
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if snapshot.window_focused == focused {
                return false;
            }
            snapshot.window_focused = focused;
            // A NativeActivity can reach Rust after onActivityResumed. Until a
            // lifecycle callback is observed, gaining focus is sufficient
            // evidence that the Activity is resumed. Once callbacks are live,
            // focus can never override an authoritative pause.
            if focused && !snapshot.lifecycle_observed {
                snapshot.activity_resumed = true;
            }
            snapshot.interactive = snapshot.activity_resumed && focused;
            snapshot.generation = snapshot.generation.wrapping_add(1);
            *snapshot
        };
        self.updates.send_replace(next);
        self.changed.notify_all();
        true
    }

    #[cfg(any(target_os = "android", test))]
    fn set_operating_conditions(&self, operating: MobileOperatingConditions) -> bool {
        let next = {
            let mut snapshot = self
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if snapshot.operating_observed && snapshot.operating == operating {
                return false;
            }
            snapshot.operating = operating;
            snapshot.operating_observed = true;
            snapshot.generation = snapshot.generation.wrapping_add(1);
            *snapshot
        };
        self.updates.send_replace(next);
        self.changed.notify_all();
        true
    }

    #[cfg(target_os = "android")]
    fn request_conditions_refresh(&self) {
        let next = {
            let mut snapshot = self
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            snapshot.generation = snapshot.generation.wrapping_add(1);
            *snapshot
        };
        self.updates.send_replace(next);
        self.changed.notify_all();
    }

    fn wait_until_interactive(&self) -> u64 {
        let mut snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !snapshot.interactive {
            snapshot = self
                .changed
                .wait(snapshot)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        snapshot.generation
    }

    fn wait_for_change_or_timeout(&self, generation: u64, timeout: Duration) {
        let snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if snapshot.generation != generation || !snapshot.interactive {
            return;
        }
        drop(
            self.changed
                .wait_timeout_while(snapshot, timeout, |snapshot| {
                    snapshot.generation == generation && snapshot.interactive
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    async fn wait_for_interactive(&self) {
        let mut updates = self.updates.subscribe();
        loop {
            if updates.borrow().interactive {
                return;
            }
            if updates.changed().await.is_err() {
                return;
            }
        }
    }

    async fn wait_for_background(&self) {
        let mut updates = self.updates.subscribe();
        loop {
            if !updates.borrow().interactive {
                return;
            }
            if updates.changed().await.is_err() {
                return;
            }
        }
    }
}

fn runtime_control() -> &'static MobileRuntimeControl {
    static CONTROL: OnceLock<MobileRuntimeControl> = OnceLock::new();
    CONTROL.get_or_init(MobileRuntimeControl::default)
}

/// Update the authoritative Android Activity lifecycle state.
#[cfg(target_os = "android")]
fn set_activity_resumed(resumed: bool) {
    if runtime_control().set_activity_resumed(resumed) {
        tracing::info!(
            resumed,
            interactive = interactive(),
            "Android Activity lifecycle changed"
        );
    }
}

/// Update whether the Android window is focused. Losing focus is deliberately
/// treated as background so system surfaces do not leave streams churning.
#[cfg(target_os = "android")]
pub(crate) fn set_window_focused(focused: bool) {
    if runtime_control().set_window_focused(focused) {
        tracing::info!(
            focused,
            interactive = interactive(),
            "Android window focus changed"
        );
    }
}

pub(crate) fn interactive() -> bool {
    runtime_control()
        .snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .interactive
}

pub(crate) fn resource_mode() -> MobileResourceMode {
    let snapshot = runtime_control()
        .snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    resource_mode_for_conditions(
        snapshot.operating,
        snapshot.operating_observed,
        cfg!(target_os = "android"),
    )
}

pub(crate) fn passive_network_interval(
    base: Duration,
    class: MobilePassiveNetworkClass,
) -> Duration {
    passive_network_interval_for_mode(base, class, resource_mode())
}

#[cfg(any(target_os = "android", test))]
pub(crate) fn interactive_poll_interval(base: Duration) -> Duration {
    interactive_poll_interval_for_mode(base, resource_mode())
}

#[cfg(any(target_os = "android", test))]
fn interactive_poll_interval_for_mode(base: Duration, mode: MobileResourceMode) -> Duration {
    let multiplier = match mode {
        MobileResourceMode::Normal => 1,
        MobileResourceMode::Constrained => 2,
        MobileResourceMode::Restricted => 4,
    };
    base.saturating_mul(multiplier)
}

fn passive_network_interval_for_mode(
    base: Duration,
    class: MobilePassiveNetworkClass,
    mode: MobileResourceMode,
) -> Duration {
    let multiplier = match (class, mode) {
        (_, MobileResourceMode::Normal) => 1,
        (MobilePassiveNetworkClass::Essential, MobileResourceMode::Constrained) => 4,
        (MobilePassiveNetworkClass::Essential, MobileResourceMode::Restricted) => 12,
        (MobilePassiveNetworkClass::Observability, MobileResourceMode::Constrained) => 12,
        (MobilePassiveNetworkClass::Observability, MobileResourceMode::Restricted) => 60,
    };
    base.saturating_mul(multiplier)
}

fn resource_mode_for_conditions(
    operating: MobileOperatingConditions,
    observed: bool,
    mobile: bool,
) -> MobileResourceMode {
    if !mobile || !observed {
        return MobileResourceMode::Normal;
    }
    let network = operating.network;
    let power = operating.power;
    let restricted_network = !network.available
        || !network.validated
        || network.data_saver
        || network.roaming == Some(true)
        || network.suspended == Some(true);
    let restricted_power = (!power.charging
        && power.battery_percent.is_some_and(|percent| percent <= 15))
        || power.thermal_status.is_some_and(|status| status >= 4);
    if restricted_network || restricted_power {
        return MobileResourceMode::Restricted;
    }
    let constrained_network = network.metered
        || network.congested == Some(true)
        || network_transport_is_cellular(network.transport);
    let constrained_power = power.power_saver
        || (!power.charging && power.battery_percent.is_some_and(|percent| percent <= 30))
        || power.thermal_status.is_some_and(|status| status >= 3);
    if constrained_network || constrained_power {
        MobileResourceMode::Constrained
    } else {
        MobileResourceMode::Normal
    }
}

fn network_transport_is_cellular(transport: MobileNetworkTransport) -> bool {
    #[cfg(any(target_os = "android", test))]
    {
        transport == MobileNetworkTransport::Cellular
    }
    #[cfg(not(any(target_os = "android", test)))]
    {
        let _ = transport;
        false
    }
}

pub(crate) async fn wait_for_interactive() {
    runtime_control().wait_for_interactive().await;
}

pub(crate) async fn wait_for_background() {
    runtime_control().wait_for_background().await;
}

#[cfg(target_os = "android")]
pub(crate) fn wait_until_interactive_blocking() -> u64 {
    runtime_control().wait_until_interactive()
}

#[cfg(target_os = "android")]
pub(crate) fn wait_for_change_or_timeout(generation: u64, timeout: Duration) {
    runtime_control().wait_for_change_or_timeout(generation, timeout);
}

pub(crate) fn wait_for_interactive_delay(delay: Duration) {
    let deadline = std::time::Instant::now() + delay;
    loop {
        let generation = runtime_control().wait_until_interactive();
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        runtime_control().wait_for_change_or_timeout(generation, remaining);
    }
}

#[cfg(all(target_os = "android", feature = "mobile"))]
const ANDROID_LIFECYCLE_DEX: &[u8] = include_bytes!(env!("SINOMO_ANDROID_LIFECYCLE_DEX_PATH"));

/// Install an Application.ActivityLifecycleCallbacks bridge before starting
/// any app-owned background work. The Java Application retains the callback.
#[cfg(all(target_os = "android", feature = "mobile"))]
pub(crate) fn start_android_lifecycle_monitor(android_app: sui::AndroidApp) {
    let app = android_app.clone();
    android_app.run_on_java_main_thread(Box::new(move || {
        if let Err(error) = install_android_lifecycle_monitor(&app) {
            // Keep the app usable on vendor runtimes that reject DEX loading.
            // Window focus still gates work, and this fallback is logged rather
            // than silently deadlocking startup.
            tracing::warn!(%error, "Android lifecycle bridge unavailable; using focus fallback");
            set_activity_resumed(true);
        }
    }));
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn install_android_lifecycle_monitor(
    android_app: &sui::AndroidApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use jni::objects::{JClass, JObject, JString};
    use jni::refs::Global;
    use jni::{NativeMethod, jni_sig, jni_str};

    let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
    let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let cache_dir = env
            .call_method(
                &activity,
                jni_str!("getCodeCacheDir"),
                jni_sig!("()Ljava/io/File;"),
                &[],
            )?
            .l()?;
        let cache_dir_path = env
            .call_method(
                &cache_dir,
                jni_str!("getAbsolutePath"),
                jni_sig!("()Ljava/lang/String;"),
                &[],
            )?
            .l()?;
        let cache_dir_path = JString::cast_local(env, cache_dir_path)?.try_to_string(env)?;
        let dex_path = std::path::PathBuf::from(&cache_dir_path).join("sinomo-lifecycle.dex");
        let _ = std::fs::remove_file(&dex_path);
        std::fs::write(&dex_path, ANDROID_LIFECYCLE_DEX)?;
        let mut permissions = std::fs::metadata(&dex_path)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&dex_path, permissions)?;

        let parent_loader = env
            .call_method(
                &activity,
                jni_str!("getClassLoader"),
                jni_sig!("()Ljava/lang/ClassLoader;"),
                &[],
            )?
            .l()?;
        let dex_path_string = env.new_string(dex_path.to_string_lossy())?;
        let cache_path_string = env.new_string(&cache_dir_path)?;
        let null_library_path = JObject::null();
        let dex_loader = env.new_object(
            jni_str!("dalvik/system/DexClassLoader"),
            jni_sig!(
                "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/ClassLoader;)V"
            ),
            &[
                (&dex_path_string).into(),
                (&cache_path_string).into(),
                (&null_library_path).into(),
                (&parent_loader).into(),
            ],
        )?;
        let class_name = env.new_string("net.sinomo.app.runtime.SinomoLifecycleCallbacks")?;
        let lifecycle_class = env
            .call_method(
                &dex_loader,
                jni_str!("loadClass"),
                jni_sig!("(Ljava/lang/String;)Ljava/lang/Class;"),
                &[(&class_name).into()],
            )?
            .l()?;
        let lifecycle_class = JClass::cast_local(env, lifecycle_class)?;
        let _ = std::fs::remove_file(&dex_path);

        let native_methods = [unsafe {
            NativeMethod::from_raw_parts(
                jni_str!("sinomoLifecycleChanged"),
                jni_str!("(Z)V"),
                android_lifecycle_changed as *mut _,
            )
        }];
        unsafe {
            env.register_native_methods(&lifecycle_class, &native_methods)?;
        }
        let callback = env.new_object(
            &lifecycle_class,
            jni_sig!("(Landroid/app/Activity;)V"),
            &[activity.as_ref().into()],
        )?;
        env.call_method(&callback, jni_str!("start"), jni_sig!("()V"), &[])?;
        Ok(())
    })
}

#[cfg(all(target_os = "android", feature = "mobile"))]
extern "system" fn android_lifecycle_changed(
    _env: jni::EnvUnowned<'_>,
    _callback: jni::objects::JObject<'_>,
    resumed: jni::sys::jboolean,
) {
    set_activity_resumed(resumed);
}

#[cfg(all(target_os = "android", feature = "mobile"))]
pub(crate) fn start_android_operating_conditions_monitor(
    android_app: sui::AndroidApp,
    ui: sui::UiHandle,
) {
    std::thread::Builder::new()
        .name("sinomo-app-conditions".to_string())
        .spawn(move || {
            // Netwatcher already carries a small Android NetworkCallback DEX.
            // Keep its handle alive to wake this sampler promptly on address
            // changes; the bounded timer also observes Data Saver/power changes
            // that do not alter the interface list.
            let _network_watch = match netwatcher::watch_interfaces_with_callback(|_| {
                runtime_control().request_conditions_refresh();
            }) {
                Ok(watch) => Some(watch),
                Err(error) => {
                    tracing::debug!(%error, "Android network-change callback unavailable");
                    None
                }
            };
            loop {
                let generation = runtime_control().wait_until_interactive();
                match android_operating_conditions(&android_app) {
                    Ok(conditions) => {
                        if runtime_control().set_operating_conditions(conditions) {
                            tracing::info!(
                                resource_mode = ?resource_mode_for_conditions(conditions, true, true),
                                network_available = conditions.network.available,
                                network_validated = conditions.network.validated,
                                network_metered = conditions.network.metered,
                                network_roaming = ?conditions.network.roaming,
                                data_saver = conditions.network.data_saver,
                                network_suspended = ?conditions.network.suspended,
                                network_congested = ?conditions.network.congested,
                                transport = ?conditions.network.transport,
                                battery_percent = ?conditions.power.battery_percent,
                                charging = conditions.power.charging,
                                power_saver = conditions.power.power_saver,
                                thermal_status = ?conditions.power.thermal_status,
                                "Android operating conditions changed"
                            );
                            ui.wake();
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%error, "failed to read Android operating conditions");
                    }
                }
                // Focus/network transitions wake immediately. In the
                // background this waits indefinitely without polling.
                runtime_control().wait_for_change_or_timeout(generation, Duration::from_secs(60));
            }
        })
        .expect("spawn Android operating-conditions monitor");
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_operating_conditions(
    android_app: &sui::AndroidApp,
) -> Result<MobileOperatingConditions, String> {
    let app = android_app.clone();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    android_app.run_on_java_main_thread(Box::new(move || {
        let _ = send.send(android_operating_conditions_on_java_main_thread(&app));
    }));
    receive
        .recv_timeout(Duration::from_secs(2))
        .map_err(|error| format!("Android operating-conditions query timed out: {error}"))?
        .map_err(|error| error.to_string())
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_operating_conditions_on_java_main_thread(
    android_app: &sui::AndroidApp,
) -> jni::errors::Result<MobileOperatingConditions> {
    use jni::objects::{JObject, JValue};
    use jni::refs::Global;

    let vm = unsafe { jni::JavaVM::from_raw(android_app.vm_as_ptr().cast()) };
    let raw_activity = android_app.activity_as_ptr() as jni::sys::jobject;
    vm.attach_current_thread(|env| {
        let activity = unsafe { env.as_cast_raw::<Global<JObject>>(&raw_activity)? };
        let sdk = env
            .get_static_field(
                jni::jni_str!("android/os/Build$VERSION"),
                jni::jni_str!("SDK_INT"),
                jni::jni_sig!("I"),
            )?
            .i()?;

        let connectivity_name: JObject = env.new_string("connectivity")?.into();
        let connectivity = env
            .call_method(
                &activity,
                jni::jni_str!("getSystemService"),
                jni::jni_sig!("(Ljava/lang/String;)Ljava/lang/Object;"),
                &[JValue::Object(&connectivity_name)],
            )?
            .l()?;
        let network = env
            .call_method(
                &connectivity,
                jni::jni_str!("getActiveNetwork"),
                jni::jni_sig!("()Landroid/net/Network;"),
                &[],
            )?
            .l()?;
        let available = !network.is_null();
        let capabilities = if available {
            env.call_method(
                &connectivity,
                jni::jni_str!("getNetworkCapabilities"),
                jni::jni_sig!("(Landroid/net/Network;)Landroid/net/NetworkCapabilities;"),
                &[JValue::Object(&network)],
            )?
            .l()?
        } else {
            JObject::null()
        };

        let validated_capability =
            android_network_capability(env, "NET_CAPABILITY_VALIDATED")?;
        let not_metered_capability =
            android_network_capability(env, "NET_CAPABILITY_NOT_METERED")?;
        let validated =
            android_has_network_capability(env, &capabilities, validated_capability)?;
        let metered = available
            && !android_has_network_capability(env, &capabilities, not_metered_capability)?;
        let roaming = if !available {
            Some(false)
        } else if sdk >= 28 {
            let capability = android_network_capability(env, "NET_CAPABILITY_NOT_ROAMING")?;
            Some(!android_has_network_capability(
                env,
                &capabilities,
                capability,
            )?)
        } else {
            let network_info = env
                .call_method(
                    &connectivity,
                    jni::jni_str!("getActiveNetworkInfo"),
                    jni::jni_sig!("()Landroid/net/NetworkInfo;"),
                    &[],
                )?
                .l()?;
            if network_info.is_null() {
                None
            } else {
                Some(
                    env.call_method(
                        &network_info,
                        jni::jni_str!("isRoaming"),
                        jni::jni_sig!("()Z"),
                        &[],
                    )?
                    .z()?,
                )
            }
        };
        let suspended = (available && sdk >= 28)
            .then(|| {
                let capability =
                    android_network_capability(env, "NET_CAPABILITY_NOT_SUSPENDED")?;
                android_has_network_capability(env, &capabilities, capability)
                    .map(|not_suspended| !not_suspended)
            })
            .transpose()?;
        let congested = (available && sdk >= 28)
            .then(|| {
                let capability =
                    android_network_capability(env, "NET_CAPABILITY_NOT_CONGESTED")?;
                android_has_network_capability(env, &capabilities, capability)
                    .map(|not_congested| !not_congested)
            })
            .transpose()?;

        let vpn_transport = android_network_transport(env, "TRANSPORT_VPN")?;
        let wifi_transport = android_network_transport(env, "TRANSPORT_WIFI")?;
        let cellular_transport = android_network_transport(env, "TRANSPORT_CELLULAR")?;
        let ethernet_transport = android_network_transport(env, "TRANSPORT_ETHERNET")?;
        let transport = if !available {
            MobileNetworkTransport::Unknown
        } else if android_has_network_transport(env, &capabilities, vpn_transport)? {
            MobileNetworkTransport::Vpn
        } else if android_has_network_transport(env, &capabilities, wifi_transport)? {
            MobileNetworkTransport::Wifi
        } else if android_has_network_transport(env, &capabilities, cellular_transport)? {
            MobileNetworkTransport::Cellular
        } else if android_has_network_transport(env, &capabilities, ethernet_transport)? {
            MobileNetworkTransport::Ethernet
        } else {
            MobileNetworkTransport::Other
        };

        let restrict_background_status = env
            .call_method(
                &connectivity,
                jni::jni_str!("getRestrictBackgroundStatus"),
                jni::jni_sig!("()I"),
                &[],
            )?
            .i()?;
        let data_saver_enabled = android_connectivity_manager_constant(
            env,
            "RESTRICT_BACKGROUND_STATUS_ENABLED",
        )?;

        let power_name: JObject = env.new_string("power")?.into();
        let power = env
            .call_method(
                &activity,
                jni::jni_str!("getSystemService"),
                jni::jni_sig!("(Ljava/lang/String;)Ljava/lang/Object;"),
                &[JValue::Object(&power_name)],
            )?
            .l()?;
        let power_saver = env
            .call_method(
                &power,
                jni::jni_str!("isPowerSaveMode"),
                jni::jni_sig!("()Z"),
                &[],
            )?
            .z()?;
        let thermal_status = (sdk >= 29)
            .then(|| {
                env.call_method(
                    &power,
                    jni::jni_str!("getCurrentThermalStatus"),
                    jni::jni_sig!("()I"),
                    &[],
                )?
                .i()
            })
            .transpose()?;

        let battery_action: JObject =
            env.new_string("android.intent.action.BATTERY_CHANGED")?.into();
        let battery_filter = env.new_object(
            jni::jni_str!("android/content/IntentFilter"),
            jni::jni_sig!("(Ljava/lang/String;)V"),
            &[JValue::Object(&battery_action)],
        )?;
        let null_receiver = JObject::null();
        let battery_intent = env
            .call_method(
                &activity,
                jni::jni_str!("registerReceiver"),
                jni::jni_sig!("(Landroid/content/BroadcastReceiver;Landroid/content/IntentFilter;)Landroid/content/Intent;"),
                &[
                    JValue::Object(&null_receiver),
                    JValue::Object(&battery_filter),
                ],
            )?
            .l()?;
        let (battery_percent, charging) = if battery_intent.is_null() {
            (None, false)
        } else {
            let level = android_intent_extra(env, &battery_intent, "level", -1)?;
            let scale = android_intent_extra(env, &battery_intent, "scale", -1)?;
            let status = android_intent_extra(env, &battery_intent, "status", -1)?;
            let percent = (level >= 0 && scale > 0)
                .then(|| ((level.saturating_mul(100) / scale).clamp(0, 100)) as u8);
            (percent, matches!(status, 2 | 5))
        };

        Ok(MobileOperatingConditions {
            network: MobileNetworkConditions {
                available,
                validated,
                metered,
                roaming,
                data_saver: restrict_background_status == data_saver_enabled,
                suspended,
                congested,
                transport,
            },
            power: MobilePowerConditions {
                battery_percent,
                charging,
                power_saver,
                thermal_status,
            },
        })
    })
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_network_capability(env: &mut jni::Env<'_>, name: &str) -> jni::errors::Result<i32> {
    let name = jni::strings::JNIString::new(name);
    env.get_static_field(
        jni::jni_str!("android/net/NetworkCapabilities"),
        &name,
        jni::jni_sig!("I"),
    )?
    .i()
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_network_transport(env: &mut jni::Env<'_>, name: &str) -> jni::errors::Result<i32> {
    android_network_capability(env, name)
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_has_network_capability(
    env: &mut jni::Env<'_>,
    capabilities: &jni::objects::JObject<'_>,
    capability: i32,
) -> jni::errors::Result<bool> {
    use jni::objects::JValue;

    if capabilities.is_null() {
        return Ok(false);
    }
    env.call_method(
        capabilities,
        jni::jni_str!("hasCapability"),
        jni::jni_sig!("(I)Z"),
        &[JValue::Int(capability)],
    )?
    .z()
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_has_network_transport(
    env: &mut jni::Env<'_>,
    capabilities: &jni::objects::JObject<'_>,
    transport: i32,
) -> jni::errors::Result<bool> {
    use jni::objects::JValue;

    if capabilities.is_null() {
        return Ok(false);
    }
    env.call_method(
        capabilities,
        jni::jni_str!("hasTransport"),
        jni::jni_sig!("(I)Z"),
        &[JValue::Int(transport)],
    )?
    .z()
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_connectivity_manager_constant(
    env: &mut jni::Env<'_>,
    name: &str,
) -> jni::errors::Result<i32> {
    let name = jni::strings::JNIString::new(name);
    env.get_static_field(
        jni::jni_str!("android/net/ConnectivityManager"),
        &name,
        jni::jni_sig!("I"),
    )?
    .i()
}

#[cfg(all(target_os = "android", feature = "mobile"))]
fn android_intent_extra(
    env: &mut jni::Env<'_>,
    intent: &jni::objects::JObject<'_>,
    name: &str,
    default: i32,
) -> jni::errors::Result<i32> {
    use jni::objects::{JObject, JValue};

    let name: JObject = env.new_string(name)?.into();
    env.call_method(
        intent,
        jni::jni_str!("getIntExtra"),
        jni::jni_sig!("(Ljava/lang/String;I)I"),
        &[JValue::Object(&name), JValue::Int(default)],
    )?
    .i()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_gate_tracks_visibility_and_conditions() {
        let supported_transports = [
            MobileNetworkTransport::Cellular,
            MobileNetworkTransport::Wifi,
            MobileNetworkTransport::Ethernet,
            MobileNetworkTransport::Vpn,
            MobileNetworkTransport::Other,
        ];
        assert_eq!(supported_transports.len(), 5);

        let control = MobileRuntimeControl::default();
        assert!(control.snapshot.lock().expect("snapshot").interactive);
        assert!(control.set_window_focused(false));
        assert!(!control.snapshot.lock().expect("snapshot").interactive);
        assert!(control.set_activity_resumed(false));
        assert!(control.set_window_focused(true));
        assert!(!control.snapshot.lock().expect("snapshot").interactive);
        assert!(control.set_activity_resumed(true));
        assert!(control.snapshot.lock().expect("snapshot").interactive);
        assert!(!control.set_activity_resumed(true));

        let conditions = MobileOperatingConditions {
            network: MobileNetworkConditions {
                available: true,
                validated: true,
                metered: true,
                transport: MobileNetworkTransport::Cellular,
                ..MobileNetworkConditions::default()
            },
            power: MobilePowerConditions {
                battery_percent: Some(42),
                power_saver: true,
                ..MobilePowerConditions::default()
            },
        };
        assert!(control.set_operating_conditions(conditions));
        assert_eq!(
            control.snapshot.lock().expect("snapshot").operating,
            conditions
        );
    }

    #[test]
    fn initial_focus_recovers_a_missed_native_activity_resume_only_once() {
        let control = MobileRuntimeControl::default();
        {
            let mut snapshot = control.snapshot.lock().expect("snapshot");
            snapshot.activity_resumed = false;
            snapshot.window_focused = false;
            snapshot.lifecycle_observed = false;
            snapshot.interactive = false;
        }

        assert!(control.set_window_focused(true));
        assert!(control.snapshot.lock().expect("snapshot").interactive);
        assert!(control.set_activity_resumed(false));
        assert!(!control.snapshot.lock().expect("snapshot").interactive);
        assert!(control.set_window_focused(false));
        assert!(control.set_window_focused(true));
        assert!(!control.snapshot.lock().expect("snapshot").interactive);
    }

    fn connected_conditions() -> MobileOperatingConditions {
        MobileOperatingConditions {
            network: MobileNetworkConditions {
                available: true,
                validated: true,
                transport: MobileNetworkTransport::Wifi,
                ..MobileNetworkConditions::default()
            },
            power: MobilePowerConditions {
                battery_percent: Some(80),
                ..MobilePowerConditions::default()
            },
        }
    }

    #[test]
    fn mobile_resource_mode_uses_network_power_and_thermal_evidence() {
        let normal = connected_conditions();
        assert_eq!(
            resource_mode_for_conditions(normal, true, true),
            MobileResourceMode::Normal
        );
        assert_eq!(
            resource_mode_for_conditions(normal, false, true),
            MobileResourceMode::Normal,
            "unknown startup conditions must not delay initial attachment"
        );

        let mut metered = normal;
        metered.network.metered = true;
        metered.network.transport = MobileNetworkTransport::Cellular;
        assert_eq!(
            resource_mode_for_conditions(metered, true, true),
            MobileResourceMode::Constrained
        );

        let mut data_saver = normal;
        data_saver.network.data_saver = true;
        assert_eq!(
            resource_mode_for_conditions(data_saver, true, true),
            MobileResourceMode::Restricted
        );

        let mut low_battery = normal;
        low_battery.power.battery_percent = Some(10);
        assert_eq!(
            resource_mode_for_conditions(low_battery, true, true),
            MobileResourceMode::Restricted
        );
        low_battery.power.charging = true;
        assert_eq!(
            resource_mode_for_conditions(low_battery, true, true),
            MobileResourceMode::Normal
        );

        let mut critical_thermal = normal;
        critical_thermal.power.thermal_status = Some(4);
        assert_eq!(
            resource_mode_for_conditions(critical_thermal, true, true),
            MobileResourceMode::Restricted
        );
        assert_eq!(
            resource_mode_for_conditions(data_saver, true, false),
            MobileResourceMode::Normal,
            "desktop targets must retain their ordinary cadence"
        );
    }

    #[test]
    fn mobile_resource_mode_scales_only_passive_work() {
        let base = Duration::from_secs(5);
        assert_eq!(
            interactive_poll_interval(Duration::from_millis(250)),
            Duration::from_millis(250),
            "desktop tests retain normal cadence through the public helper"
        );
        assert_eq!(
            passive_network_interval_for_mode(
                base,
                MobilePassiveNetworkClass::Essential,
                MobileResourceMode::Normal,
            ),
            Duration::from_secs(5)
        );
        assert_eq!(
            passive_network_interval_for_mode(
                base,
                MobilePassiveNetworkClass::Essential,
                MobileResourceMode::Constrained,
            ),
            Duration::from_secs(20)
        );
        assert_eq!(
            passive_network_interval_for_mode(
                base,
                MobilePassiveNetworkClass::Essential,
                MobileResourceMode::Restricted,
            ),
            Duration::from_secs(60)
        );
        assert_eq!(
            passive_network_interval_for_mode(
                base,
                MobilePassiveNetworkClass::Observability,
                MobileResourceMode::Constrained,
            ),
            Duration::from_secs(60)
        );
        assert_eq!(
            passive_network_interval_for_mode(
                base,
                MobilePassiveNetworkClass::Observability,
                MobileResourceMode::Restricted,
            ),
            Duration::from_secs(300)
        );
        assert_eq!(
            interactive_poll_interval_for_mode(
                Duration::from_millis(250),
                MobileResourceMode::Restricted,
            ),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn first_default_operating_sample_is_observed_and_wakes_waiters() {
        let control = MobileRuntimeControl::default();
        assert!(control.set_operating_conditions(MobileOperatingConditions::default()));
        let snapshot = control.snapshot.lock().expect("snapshot");
        assert!(snapshot.operating_observed);
        assert_eq!(snapshot.generation, 1);
        drop(snapshot);
        assert!(!control.set_operating_conditions(MobileOperatingConditions::default()));
    }
}
