//! Entry point for the Sinomo client app.
//!
//! With arguments, it behaves like the `sinomo` CLI (so the same binary can be
//! scripted). With no arguments, it launches the graphical app.

use std::process::ExitCode;

fn main() -> ExitCode {
    install_build_info();
    let args: Vec<String> = std::env::args().skip(1).collect();

    if !args.is_empty() {
        return match sinomo_cli::run(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!("sinomo: {error}");
                ExitCode::FAILURE
            }
        };
    }

    match sinomo_app::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("sinomo-app: {error}");
            ExitCode::FAILURE
        }
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
