//! Entry point for the Sinomo graphical client.
#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use sinomo_client::api::app::update::{
    APP_FATAL_DIALOG_ARG, APP_FATAL_DIALOG_ENV, APP_REPORTED_ERROR_EXIT_CODE, APP_SUPERVISED_ENV,
};
use std::process::{Command, ExitCode};

const STANDALONE_UI_ENV: &str = "SINOMO_APP_STANDALONE_UI";

fn fatal(detail: &str) -> ExitCode {
    eprintln!("Sinomo: {detail}");
    rfd::MessageDialog::new()
        .set_level(rfd::MessageLevel::Error)
        .set_title("Sinomo stopped unexpectedly")
        .set_description(detail)
        .show();
    ExitCode::from(APP_REPORTED_ERROR_EXIT_CODE)
}

fn main() -> ExitCode {
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new(APP_FATAL_DIALOG_ARG)) {
        let detail = std::env::args()
            .nth(2)
            .unwrap_or_else(|| "The desktop session ended unexpectedly.".into());
        fatal(&detail);
        return ExitCode::SUCCESS;
    }
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
    match std::panic::catch_unwind(run_desktop) {
        Ok(Ok(code)) => code,
        Ok(Err(error)) => fatal(&error),
        Err(panic) => {
            let detail = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("Unknown panic");
            fatal(&format!("The desktop application panicked.\n\n{detail}"))
        }
    }
}

fn run_desktop() -> Result<ExitCode, String> {
    if std::env::var_os(APP_SUPERVISED_ENV).is_none()
        && std::env::var_os(STANDALONE_UI_ENV).is_none()
    {
        let app = std::env::current_exe().map_err(|e| e.to_string())?;
        let helper = std::env::var_os(sinomo_client::api::SINOMO_APP_DAEMON_BINARY_ENV)
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                app.with_file_name(format!("sinomo{}", std::env::consts::EXE_SUFFIX))
            });
        let data =
            sinomo_runtime::local::SinomoDataDir::from_env_or_home().map_err(|e| e.to_string())?;
        sinomo_runtime::local::ensure_deployment_enabled(&data.root())?;
        if helper.is_file() {
            let mut command = Command::new(&helper);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                command.creation_flags(0x08000000);
            }
            // Release the bootstrap image so rollout can replace it on Windows.
            command
                .args(["app", "supervise", "--"])
                .arg(app)
                .args(std::env::args_os().skip(1))
                .env(APP_FATAL_DIALOG_ENV, "1")
                .spawn()
                .map_err(|e| {
                    format!(
                        "Could not start the desktop supervisor at {}.\n\n{e}",
                        helper.display()
                    )
                })?;
            return Ok(ExitCode::SUCCESS);
        }

        // Without the CLI, keep a parent outside the renderer to report native faults.
        let status = Command::new(app)
            .args(std::env::args_os().skip(1))
            .env(STANDALONE_UI_ENV, "1")
            .status()
            .map_err(|e| format!("Could not launch the desktop application.\n\n{e}"))?;
        if status.code() == Some(i32::from(APP_REPORTED_ERROR_EXIT_CODE)) {
            return Ok(ExitCode::from(APP_REPORTED_ERROR_EXIT_CODE));
        }
        if !status.success() {
            return Err(format!(
                "The desktop application stopped unexpectedly ({status})."
            ));
        }
        return Ok(ExitCode::SUCCESS);
    }
    #[cfg(not(target_os = "android"))]
    sinomo_app::external_edit::resume_sessions();
    sinomo_app::run().map_err(|error| error.to_string())?;
    Ok(ExitCode::SUCCESS)
}
