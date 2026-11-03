//! Entry point for the Sinomo graphical client.
#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::process::ExitCode;

fn main() -> ExitCode {
    #[cfg(not(target_os = "android"))]
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--external-edit-launch"))
    {
        return match sinomo_app::external_edit::monitor() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                println!("ERR {error}");
                ExitCode::FAILURE
            }
        };
    }
    #[cfg(not(target_os = "android"))]
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--external-edit-worker"))
    {
        return match sinomo_app::external_edit::run_helper() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("External edit: {error}");
                ExitCode::FAILURE
            }
        };
    }
    if std::env::var_os(sinomo_client::api::app::update::APP_SUPERVISED_ENV).is_none() {
        let result = (|| -> Result<Option<std::process::Child>, String> {
            let app = std::env::current_exe().map_err(|e| e.to_string())?;
            let helper = std::env::var_os(sinomo_client::api::SINOMO_APP_DAEMON_BINARY_ENV)
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    app.with_file_name(format!("sinomo{}", std::env::consts::EXE_SUFFIX))
                });
            if let Ok(data) = sinomo_runtime::local::SinomoDataDir::from_env_or_home() {
                sinomo_runtime::local::ensure_deployment_enabled(&data.root())?;
            }
            if !helper.is_file() {
                return Ok(None);
            }
            let mut command = std::process::Command::new(helper);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                command.creation_flags(0x08000000); // CREATE_NO_WINDOW for the CLI supervisor.
            }
            command
                .args(["app", "supervise", "--"])
                .arg(app)
                .args(std::env::args_os().skip(1))
                .spawn()
                .map(Some)
                .map_err(|e| e.to_string())
        })();
        match result {
            Ok(Some(_)) => return ExitCode::SUCCESS,
            Ok(None) => {}
            Err(error) => {
                eprintln!("sinomo-app launcher: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    #[cfg(not(target_os = "android"))]
    sinomo_app::external_edit::resume_sessions();
    match sinomo_app::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("sinomo-app: {error}");
            ExitCode::FAILURE
        }
    }
}
