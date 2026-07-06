#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
pub fn android_main(android_app: sui::AndroidApp) {
    if let Err(error) = sinomo_app::run_android(android_app) {
        tracing::error!("sinomo-app android runtime failed: {error}");
    }
}

#[cfg(not(target_os = "android"))]
pub fn android_entrypoint_unavailable() {}
