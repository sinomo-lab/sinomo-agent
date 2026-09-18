//! Entry point for the Sinomo graphical client.

use std::process::ExitCode;

fn main() -> ExitCode {
    if std::env::var_os(sinomo_client::api::app::update::APP_SUPERVISED_ENV).is_none() {
        let result = (|| -> Result<std::process::ExitStatus, String> {
            let app = std::env::current_exe().map_err(|e| e.to_string())?;
            let helper = std::env::var_os(sinomo_client::api::SINOMO_APP_DAEMON_BINARY_ENV)
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    app.with_file_name(format!("sinomo{}", std::env::consts::EXE_SUFFIX))
                });
            std::process::Command::new(helper)
                .args(["app", "supervise", "--"])
                .arg(app)
                .args(std::env::args_os().skip(1))
                .status()
                .map_err(|e| e.to_string())
        })();
        return match result {
            Ok(status) => ExitCode::from(status.code().unwrap_or(1).clamp(0, 255) as u8),
            Err(error) => {
                eprintln!("sinomo-app launcher: {error}");
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
