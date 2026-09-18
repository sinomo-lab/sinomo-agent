//! Foreground daemon host whose lifetime is leased by one graphical app process.

use std::io::Read;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use sinomo_runtime::api::{
    DaemonPaths, DaemonProbeStatus, DaemonRunOptions, SINOMO_APP_DAEMON_LAUNCH_ID_ENV,
    probe_daemon_status,
};
use sinomo_runtime::local::SinomoDataDir;

const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const LEASE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    install_build_info();
    if std::env::args().nth(1).as_deref() == Some("--app-supervisor") {
        let result = (|| {
            let mut args = std::env::args().skip(2);
            let app = args
                .next()
                .ok_or("--app-supervisor requires the app executable")?;
            let data_dir = SinomoDataDir::from_env_or_home().map_err(|e| e.to_string())?;
            sinomo_cli::run_desktop_app_supervisor(
                &data_dir,
                std::path::Path::new(&app),
                &args.collect::<Vec<_>>(),
            )
        })();
        return match result {
            Ok(code) => ExitCode::from(code.clamp(0, 255) as u8),
            Err(error) => {
                eprintln!("sinomo-app supervisor: {error}");
                ExitCode::FAILURE
            }
        };
    }
    let options = match parse_options(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("sinomo-app-daemon: {error}");
            return ExitCode::FAILURE;
        }
    };
    let launch_id = match std::env::var(SINOMO_APP_DAEMON_LAUNCH_ID_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!(
                "sinomo-app-daemon: missing app lifecycle lease identity in {SINOMO_APP_DAEMON_LAUNCH_ID_ENV}"
            );
            return ExitCode::FAILURE;
        }
    };
    start_parent_lease_watcher(launch_id);

    match sinomo_cli::run_app_owned_daemon(options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sinomo-app-daemon: {error}");
            ExitCode::FAILURE
        }
    }
}

fn install_build_info() {
    let _ =
        sinomo_runtime::api::install_daemon_build_info(sinomo_runtime::api::DaemonBuildInfo::new(
            env!("CARGO_PKG_VERSION"),
            option_env!("SINOMO_GIT_COMMIT").map(str::to_string),
        ));
}

fn parse_options(args: impl IntoIterator<Item = String>) -> Result<DaemonRunOptions, String> {
    let mut options = DaemonRunOptions::main_from_environment()?;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => {
                options.host = args
                    .next()
                    .ok_or_else(|| "--host requires a value".to_string())?;
            }
            "--port" => {
                let port = args
                    .next()
                    .ok_or_else(|| "--port requires a value".to_string())?;
                options.port = port
                    .parse::<u16>()
                    .map_err(|error| format!("invalid --port `{port}`: {error}"))?;
                options.port_fallback = false;
            }
            "--no-port-fallback" => options.port_fallback = false,
            value => return Err(format!("unknown argument `{value}`")),
        }
    }
    Ok(options)
}

/// The GUI owns the write side of stdin. Normal close and abrupt process death both close that
/// handle, producing EOF here. The watcher then asks only the exact PID/launch instance hosted by
/// this process to drain and forces self-exit if graceful shutdown wedges.
fn start_parent_lease_watcher(launch_id: String) {
    std::thread::Builder::new()
        .name("sinomo-app-parent-lease".to_string())
        .spawn(move || {
            let mut sink = Vec::new();
            let _ = std::io::stdin().lock().read_to_end(&mut sink);
            shutdown_after_parent_lease_closed(&launch_id);
        })
        .expect("spawn app parent lease watcher");
}

fn shutdown_after_parent_lease_closed(launch_id: &str) {
    let Ok(data_dir) = SinomoDataDir::from_env_or_home() else {
        std::process::exit(0);
    };
    let paths = DaemonPaths::new(&data_dir);
    let deadline = Instant::now() + LEASE_SHUTDOWN_TIMEOUT;
    let mut shutdown_sent = false;

    while Instant::now() < deadline {
        match probe_daemon_status(&paths, PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status) => {
                if status.pid != std::process::id()
                    || status.app_launch_id.as_deref() != Some(launch_id)
                {
                    // A competing or replacement daemon owns the shared path. Never stop it.
                    return;
                }
                if !shutdown_sent {
                    shutdown_sent = sinomo_runtime::daemon::send_daemon_shutdown_request(
                        &status.address,
                        PROBE_TIMEOUT,
                    )
                    .is_ok();
                }
            }
            DaemonProbeStatus::Locked { status } => {
                if let Some(status) = status
                    && (status.pid != std::process::id()
                        || status.app_launch_id.as_deref() != Some(launch_id))
                {
                    return;
                }
            }
            DaemonProbeStatus::NotRunning => return,
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // This is the leased child itself, so forced self-exit cannot target a replacement process.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_accept_only_the_daemon_endpoint_pair() {
        let options = parse_options([
            "--host".to_string(),
            "127.0.0.2".to_string(),
            "--port".to_string(),
            "32100".to_string(),
        ])
        .expect("options");
        assert_eq!(options.host, "127.0.0.2");
        assert_eq!(options.port, 32100);
        assert!(!options.port_fallback);
        assert!(parse_options(["--detach".to_string()]).is_err());
    }
}
