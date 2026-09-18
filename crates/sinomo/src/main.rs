use std::env;

fn main() {
    let args = env::args_os()
        .skip(1)
        .map(|arg| arg.into_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|_| {
            eprintln!("sinomo: command arguments must be valid UTF-8");
            std::process::exit(2);
        });
    configure_long_lived_daemon_allocator(&args);
    install_build_info();
    let runtime_config = sinomo_cli::default_cli_runtime_config();
    sinomo_cli::install_cli_runtime_config(runtime_config)
        .expect("default CLI module bindings should be installed once");
    if let Err(error) = sinomo_cli::run(args) {
        eprintln!("sinomo: {error}");
        std::process::exit(error.exit_code());
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn configure_long_lived_daemon_allocator(args: &[String]) {
    if !matches!(args, [daemon, run, ..] if daemon == "daemon" && run == "run")
        && !matches!(args, [daemon, shadow, run, ..]
            if daemon == "daemon" && shadow == "shadow" && run == "run")
    {
        return;
    }

    unsafe extern "C" {
        fn mallopt(parameter: std::ffi::c_int, value: std::ffi::c_int) -> std::ffi::c_int;
    }

    // glibc otherwise creates an arena per busy worker. DFS and KV hydration
    // can leave tens of MiB fragmented in each arena after the live objects are
    // released, making an idle daemon retain the peak RSS of parallel work.
    // Two arenas preserve concurrent allocation while bounding that retention.
    const M_ARENA_MAX: std::ffi::c_int = -8;
    const DAEMON_ARENA_MAX: std::ffi::c_int = 2;
    let _ = unsafe { mallopt(M_ARENA_MAX, DAEMON_ARENA_MAX) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn configure_long_lived_daemon_allocator(_args: &[String]) {}

fn install_build_info() {
    let build_info = sinomo_runtime::api::DaemonBuildInfo::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("SINOMO_GIT_COMMIT").map(str::to_string),
    );
    sinomo_runtime::api::install_daemon_build_info(build_info)
        .expect("daemon build info should be installed once");
}
