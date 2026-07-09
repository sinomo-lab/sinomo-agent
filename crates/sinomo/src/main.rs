use std::env;

fn main() {
    install_build_info();
    let runtime_config = sinomo_cli::default_cli_runtime_config()
        .with_native_rust_module_binding(sinomo_evolution::EVOLUTION_NATIVE_RUST_MODULE_BINDING)
        .with_test_cases(sinomo_evolution::register_evolution_test_cases);
    sinomo_cli::install_cli_runtime_config(runtime_config)
        .expect("default CLI module bindings should be installed once");
    if let Err(error) = sinomo_cli::run(env::args().skip(1).collect()) {
        eprintln!("sinomo: {error}");
        std::process::exit(1);
    }
}

fn install_build_info() {
    let build_info = sinomo_runtime::api::DaemonBuildInfo::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("SINOMO_GIT_COMMIT").map(str::to_string),
    );
    sinomo_runtime::api::install_daemon_build_info(build_info)
        .expect("daemon build info should be installed once");
}
