#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
pub fn android_main(android_app: sui::AndroidApp) {
    let exit_code = match sinomo_app::run_android(android_app) {
        Ok(()) => 0,
        Err(error) => {
            tracing::error!("sinomo-app android runtime failed: {error}");
            1
        }
    };
    // Winit permits only one Android event loop per process. The embedded
    // daemon owns background threads, so returning from NativeActivity would
    // otherwise keep a UI-less process alive and make every relaunch fail with
    // `EventLoop can't be recreated`.
    std::process::exit(exit_code);
}

#[cfg(not(target_os = "android"))]
pub fn android_entrypoint_unavailable() {}
