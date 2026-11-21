//! Desktop launch and app-owned daemon lifetime, hosted by the ordinary CLI binary.

use super::*;
use sinomo_runtime::api::SINOMO_APP_DAEMON_LAUNCH_ID_ENV;
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliParseError, CliPositionalSpec,
    CliValueSpec,
};

#[derive(Debug, Eq, PartialEq)]
enum AppCommand {
    LaunchInstalled,
    Supervise {
        executable: PathBuf,
        args: Vec<String>,
    },
}

pub(crate) fn app_cli_command_handler() -> impl sinomo_runtime::cli_registry::CliCommandHandler {
    DeclarativeCommandHandler::new(
        "app",
        CliCommandPolicy::default(),
        app_cli_spec,
        parse_app_matches,
        run_app_command,
    )
}

fn app_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("app", "launch the installed desktop application").with_children([
        CliCommandSpec::new("supervise", "supervise the desktop session and its daemon")
            .with_positionals([
                CliPositionalSpec::required(CliValueSpec::new("executable", "app executable")),
                CliPositionalSpec::optional(CliValueSpec::new("args", "app arguments"))
                    .repeatable(),
            ]),
    ])
}

fn parse_app_matches(matches: &CliCommandMatches) -> Result<AppCommand, CliParseError> {
    match matches.path().get(1).map(String::as_str) {
        None => Ok(AppCommand::LaunchInstalled),
        Some("supervise") => Ok(AppCommand::Supervise {
            executable: PathBuf::from(required_positional(matches, 0)?),
            args: matches.positionals()[1..].to_vec(),
        }),
        _ => Err(CliParseError::Usage),
    }
}

fn run_app_command(command: AppCommand) -> Result<(), CliError> {
    match command {
        AppCommand::LaunchInstalled => launch_installed_app(&SinomoDataDir::from_env_or_home()?),
        AppCommand::Supervise { executable, args } => run_app_supervisor(&executable, &args),
    }
}

pub(super) fn launch_installed_app(data_dir: &SinomoDataDir) -> Result<(), CliError> {
    let executable = data_dir.binary("sinomo-app");
    if matches!(executable.try_exists(), Ok(false)) {
        return Err(CliError::DesktopApp {
            detail: format!("Desktop app is not installed at {}", executable.display()),
            exit_code: 1,
        });
    }
    supervise_desktop(data_dir, &executable, &[])
}

fn run_app_supervisor(executable: &Path, args: &[String]) -> Result<(), CliError> {
    use sinomo_runtime::api::app::update::{
        APP_FATAL_DIALOG_ARG, APP_FATAL_DIALOG_ENV, APP_REPORTED_ERROR_EXIT_CODE,
    };

    let result = SinomoDataDir::from_env_or_home()
        .map_err(CliError::from)
        .and_then(|data_dir| supervise_desktop(&data_dir, executable, args));
    if let Err(error) = &result
        && std::env::var_os(APP_FATAL_DIALOG_ENV).is_some()
        && error.exit_code() != i32::from(APP_REPORTED_ERROR_EXIT_CODE)
    {
        // This mode runs just the native dialog, even if startup failed before
        // the daemon, the renderer or the user's data directory was available.
        let detail = error.to_string();
        if let Err(report_error) = std::process::Command::new(executable)
            .arg(APP_FATAL_DIALOG_ARG)
            .arg(detail.chars().take(4096).collect::<String>())
            .spawn()
        {
            eprintln!("Could not open the Sinomo error dialog: {report_error}");
        }
    }
    result
}

fn supervise_desktop(
    data_dir: &SinomoDataDir,
    executable: &Path,
    args: &[String],
) -> Result<(), CliError> {
    let code = run_desktop_app_supervisor(data_dir, executable, args).map_err(|detail| {
        CliError::DesktopApp {
            detail,
            exit_code: 1,
        }
    })?;
    if code == 0 {
        Ok(())
    } else {
        Err(CliError::DesktopApp {
            detail: format!("desktop application exited with status {code}"),
            exit_code: code.clamp(1, 255),
        })
    }
}

pub(crate) fn run_leased_app_daemon(options: DaemonRunOptions) -> Result<(), CliError> {
    let launch_id = std::env::var(SINOMO_APP_DAEMON_LAUNCH_ID_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "--app-owned requires an app lifecycle lease identity in {SINOMO_APP_DAEMON_LAUNCH_ID_ENV}"
            ),
        })?;
    std::thread::Builder::new()
        .name("sinomo-app-parent-lease".into())
        .spawn(move || {
            let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
            shutdown_after_parent_lease_closed(&launch_id);
        })
        .map_err(|error| CliError::ExternalCommand {
            command: "daemon run --app-owned".into(),
            detail: format!("start app parent lease watcher: {error}"),
        })?;
    run_app_owned_daemon_host(options)
}

fn shutdown_after_parent_lease_closed(launch_id: &str) {
    let Ok(data_dir) = SinomoDataDir::from_env_or_home() else {
        std::process::exit(0);
    };
    let paths = DaemonPaths::new(&data_dir);
    let probe_timeout = Duration::from_millis(300);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut shutdown_sent = false;
    while Instant::now() < deadline {
        match probe_daemon_status(&paths, probe_timeout) {
            DaemonProbeStatus::Running(status) => {
                if status.pid != std::process::id()
                    || status.app_launch_id.as_deref() != Some(launch_id)
                {
                    // A competing or replacement daemon owns the path. Never stop it.
                    return;
                }
                if !shutdown_sent {
                    shutdown_sent = sinomo_runtime::daemon::send_daemon_shutdown_request(
                        &status.address,
                        probe_timeout,
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
            // Give an acknowledged shutdown the rest of its bounded lease
            // deadline to drain components and retire process-owned mounts.
            DaemonProbeStatus::NotRunning if shutdown_sent => {}
            // EOF before startup must also prevent an orphaned daemon from starting later.
            DaemonProbeStatus::NotRunning => std::process::exit(0),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // This is the leased child itself, so forced exit cannot target a replacement process.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_supervisor_preserves_arguments_after_separator() {
        let command = parse_schema_command_for_test(
            app_cli_spec(),
            &[
                "supervise".into(),
                "--".into(),
                "path with spaces/sinomo-app".into(),
                "--help".into(),
                "--".into(),
                "hello world".into(),
            ],
            parse_app_matches,
        )
        .unwrap();
        assert_eq!(
            command,
            AppCommand::Supervise {
                executable: PathBuf::from("path with spaces/sinomo-app"),
                args: ["--help", "--", "hello world"].map(String::from).to_vec(),
            }
        );
        assert!(parse_schema_command_for_test(
            app_cli_spec(),
            &["supervise".into()],
            parse_app_matches,
        ).is_err());
    }

    #[test]
    fn app_owned_mode_is_only_available_on_foreground_main_daemon() {
        let parsed = parse_daemon_schema_for_test(&[
            "run".into(),
            "--app-owned".into(),
            "--host".into(),
            "127.0.0.2".into(),
            "--port".into(),
            "32100".into(),
        ])
        .unwrap();
        let DaemonCliCommand::RunAppOwned(options) = parsed else {
            panic!("expected app-owned daemon mode");
        };
        assert_eq!(options.host, "127.0.0.2");
        assert_eq!(options.port, 32100);
        assert!(!options.port_fallback);
        for args in [
            vec!["start", "--app-owned"],
            vec!["shadow", "run", "--app-owned"],
        ] {
            assert!(
                parse_daemon_schema_for_test(
                    &args.into_iter().map(str::to_string).collect::<Vec<_>>()
                )
                .is_err()
            );
        }
    }
}
