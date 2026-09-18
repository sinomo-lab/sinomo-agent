use super::*;
use sinomo_runtime::{
    api::{DaemonApiClient, app::update::*},
    rollout::desktop::DesktopReleases,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DesktopOptions {
    action: String,
    directory: Option<PathBuf>,
    repo: Option<PathBuf>,
    revision: Option<String>,
    target_os: String,
    target_arch: String,
    targets: Vec<String>,
}

pub(super) fn spec() -> CliCommandSpec {
    let directory = || {
        CliOptionSpec::value(
            "--directory",
            "desktop release directory",
            CliValueSpec::new("path", "path"),
        )
    };
    CliCommandSpec::new("app", "build, stage, inspect, or launch a desktop release")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new(
                "build",
                "resolve a desktop bundle through the cluster build service",
            )
            .with_options([
                directory(),
                CliOptionSpec::value(
                    "--target",
                    "node whose desktop platform to build",
                    CliValueSpec::new("node", "node"),
                ),
                CliOptionSpec::value(
                    "--repo",
                    "prepared source checkout (defaults to managed source)",
                    CliValueSpec::new("path", "path"),
                ),
            ]),
            CliCommandSpec::new("package", "manifest already built native desktop binaries")
                .with_options([
                    directory(),
                    CliOptionSpec::value(
                        "--revision",
                        "exact source revision",
                        CliValueSpec::new("commit", "commit"),
                    ),
                    CliOptionSpec::value(
                        "--target-os",
                        "artifact operating system",
                        CliValueSpec::new("os", "os"),
                    ),
                    CliOptionSpec::value(
                        "--target-arch",
                        "artifact architecture",
                        CliValueSpec::new("arch", "arch"),
                    ),
                ]),
            CliCommandSpec::new("stage", "verify a local desktop bundle and notify the app")
                .with_options([
                    directory(),
                    CliOptionSpec::value(
                        "--target",
                        "authorized node to offer the update to",
                        CliValueSpec::new("node", "node"),
                    )
                    .repeatable(),
                ]),
            CliCommandSpec::new("status", "show desktop rollout state"),
            CliCommandSpec::new(
                "launch",
                "launch the installed desktop app with supervision",
            ),
        ])
}

pub(super) fn parse(matches: &CliCommandMatches) -> Result<DesktopOptions, CliParseError> {
    let action = matches
        .path()
        .last()
        .cloned()
        .ok_or_else(|| CliParseError::Parse {
            detail: "app action required".into(),
        })?;
    let directory = matches.value("--directory").map(PathBuf::from);
    let revision = matches.value("--revision").map(str::to_owned);
    if matches!(action.as_str(), "stage" | "package") && directory.is_none() {
        return Err(CliParseError::Parse {
            detail: "--directory is required".into(),
        });
    }
    if action == "package" && revision.is_none() {
        return Err(CliParseError::Parse {
            detail: "--revision is required".into(),
        });
    }
    Ok(DesktopOptions {
        targets: matches.values("--target").map(str::to_owned).collect(),
        action,
        directory,
        repo: matches.value("--repo").map(PathBuf::from),
        revision,
        target_os: matches
            .value("--target-os")
            .unwrap_or(std::env::consts::OS)
            .into(),
        target_arch: matches
            .value("--target-arch")
            .unwrap_or(std::env::consts::ARCH)
            .into(),
    })
}

pub(super) fn run(options: DesktopOptions) -> Result<(), CliError> {
    if options.action == "stage" && !options.targets.is_empty() {
        return stage_remote(options);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let releases = DesktopReleases::new(data_dir.root());
    let result: Result<AppUpdateStatus, String> = match options.action.as_str() {
        "status" => releases.status(),
        "stage" => {
            let directory =
                fs::canonicalize(options.directory.as_ref().expect("validated directory"))
                    .map_err(|e| CliError::Parse {
                        detail: e.to_string(),
                    })?;
            match sinomo_runtime::api::probe_daemon_status(
                &sinomo_runtime::api::DaemonPaths::new(&data_dir),
                Duration::from_millis(500),
            ) {
                sinomo_runtime::api::DaemonProbeStatus::Running(status) => (|| {
                    let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
                        APP_UPDATE_STAGE_ROUTE,
                        "rollout",
                        &AppUpdateStageRequest {
                            bundle_directory: directory,
                        },
                    )
                    .map_err(|e| e.to_string())?;
                    DaemonApiClient::at_address(status.address)
                        .with_timeout(Duration::from_secs(300))
                        .call_request(&request)
                        .map_err(|e| e.to_string())?
                        .decode_body()
                        .map_err(|e| e.to_string())
                })(),
                sinomo_runtime::api::DaemonProbeStatus::NotRunning => releases.stage(&directory),
                _ => Err("daemon is starting; retry staging when it is ready".into()),
            }
        }
        "package" => {
            let directory = options.directory.as_ref().expect("validated directory");
            let suffix = if options.target_os == "windows" {
                ".exe"
            } else {
                ""
            };
            let files = sinomo_runtime::rollout::desktop::DESKTOP_BINARY_STEMS
                .map(|name| format!("{name}{suffix}"));
            sinomo_runtime::rollout::desktop::write_bundle_manifest_for_target(
                directory,
                options.revision.expect("validated revision"),
                &files,
                &options.target_os,
                &options.target_arch,
            )
            .map_err(|detail| CliError::Parse { detail })?;
            println!(
                "desktop manifest: {}",
                directory.join(APP_BUNDLE_MANIFEST).display()
            );
            return Ok(());
        }
        "build" => return build(options),
        "launch" => {
            let app = data_dir
                .root()
                .join("bin")
                .join(sinomo_runtime::rollout::executable_name("sinomo-app"));
            let code = crate::run_desktop_app_supervisor(&data_dir, &app, &[])
                .map_err(|detail| CliError::Parse { detail })?;
            if code != 0 {
                return Err(CliError::Parse {
                    detail: format!("desktop app exited with status {code}"),
                });
            }
            return Ok(());
        }
        _ => Err("unknown app rollout action".into()),
    };
    let status = result.map_err(|detail| CliError::Parse { detail })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(|e| CliError::Parse {
            detail: e.to_string()
        })?
    );
    Ok(())
}

fn build(options: DesktopOptions) -> Result<(), CliError> {
    let rollout_options = RolloutOptions {
        repo: options.repo,
        targets: options.targets,
        ..RolloutOptions::default()
    };
    let local = if rollout_options.targets.is_empty() {
        local_rollout_target(&rollout_options)?.ok_or_else(|| CliError::Parse {
            detail: "start the local daemon before requesting a desktop build".into(),
        })?
    } else {
        rollout_cluster_targets(&rollout_options)?
            .into_iter()
            .next()
            .ok_or_else(|| CliError::Parse {
                detail: "desktop build target not found".into(),
            })?
    };
    let status = read_rollout_target_status(&local)?;
    let target = build_target_from_daemon_status(local.label(), &status)?;
    let group = ClusterBuildTargetGroup {
        target,
        rollout_targets: vec![local],
    };
    let source = prepare_cluster_build_source_input(&rollout_options)?;
    let contacts = cluster_build_contacts(&rollout_options, std::slice::from_ref(&group));
    let built = resolve_cluster_build_group(
        &source,
        group,
        &contacts,
        &ClusterBuildOutputSelection::Desktop,
    )?;
    let directory = options.directory.unwrap_or_else(|| {
        rollout_paths_from_defaults()
            .expect("validated rollout paths")
            .root
            .join("staged/desktop")
            .join(built.builder.plan.key.path_key())
    });
    fs::create_dir_all(&directory).map_err(|e| CliError::Parse {
        detail: e.to_string(),
    })?;
    let target_os = &built.group.target.os;
    let suffix = if target_os == "windows" { ".exe" } else { "" };
    for (name, path) in &built.artifacts.desktop_binaries {
        sinomo_runtime::rollout::install_binary_atomically(
            path,
            &directory.join(format!("{name}{suffix}")),
        )?;
    }
    if let Some(binary) = &built.artifacts.binary {
        sinomo_runtime::rollout::install_binary_atomically(
            binary,
            &directory.join(format!("sinomo{suffix}")),
        )?;
    }
    sinomo_runtime::rollout::desktop::write_bundle_manifest_for_target(
        &directory,
        source
            .source
            .revision
            .clone()
            .ok_or_else(|| CliError::Parse {
                detail: "desktop build has no source revision".into(),
            })?,
        &sinomo_runtime::rollout::desktop::DESKTOP_BINARY_STEMS
            .map(|name| format!("{name}{suffix}")),
        target_os,
        &built.group.target.arch,
    )
    .map_err(|detail| CliError::Parse { detail })?;
    println!("desktop bundle: {}", directory.display());
    println!("build identity: {}", built.builder.plan.key);
    println!(
        "Stage it with: sinomo rollout app stage --directory {}",
        directory.display()
    );
    Ok(())
}

fn stage_remote(options: DesktopOptions) -> Result<(), CliError> {
    use sinomo_runtime::{
        api::{DaemonObjectRpcDestinationProcessAffinity, transport::block_on_daemon_client},
        network::{DaemonRpcTimeouts, ObjectRpcBodySource, ObjectRpcCall, ObjectRpcRequest},
    };
    let directory = options.directory.expect("validated directory");
    let manifest = sinomo_runtime::rollout::desktop::read_manifest(&directory)
        .map_err(build_service_cli_error)?;
    sinomo_runtime::rollout::desktop::verify_bundle_content(&directory, &manifest)
        .map_err(build_service_cli_error)?;
    let id =
        sinomo_runtime::rollout::desktop::release_id(&manifest).map_err(build_service_cli_error)?;
    let targets = rollout_cluster_targets(&RolloutOptions {
        targets: options.targets,
        skip_local: true,
        ..RolloutOptions::default()
    })?;
    let mut selected = Vec::new();
    for target in targets {
        let status = read_rollout_target_status(&target)?;
        if status.target_os.as_deref() != Some(&manifest.target_os)
            || status.target_arch.as_deref() != Some(&manifest.target_arch)
            || status.api_protocol_version != manifest.daemon_protocol
        {
            return Err(CliError::Parse {
                detail: format!("desktop bundle is incompatible with {}", target.label()),
            });
        }
        let node_id = target
            .rollout_node_id()
            .cloned()
            .or_else(|| {
                target
                    .endpoint
                    .as_ref()
                    .map(|endpoint| endpoint.node_id.clone())
            })
            .or_else(|| {
                status
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.node_id.clone())
            })
            .ok_or_else(|| CliError::Parse {
                detail: "remote desktop staging requires an authenticated cluster node identity"
                    .into(),
            })?;
        selected.push((target.label().to_string(), node_id));
    }
    if selected.is_empty() {
        return Err(CliError::Parse {
            detail: "no desktop staging targets selected".into(),
        });
    }
    let client = DaemonApiClient::connect()?;
    for (label, node_id) in selected {
        for (file_index, file) in manifest.files.iter().enumerate() {
            let request = AppUpdateUploadRequest {
                manifest: manifest.clone(),
                file_index,
            };
            let head = ObjectRpcRequest::encode_body(APP_UPDATE_UPLOAD_ROUTE, "rollout", &request)
                .map_err(build_service_cli_error)?
                .with_operation_id(format!("desktop:{id}:{file_index}"))
                .with_response_retry_safe();
            let body = ObjectRpcBodySource::file(directory.join(&file.path))
                .map_err(build_service_cli_error)?;
            if body.total_bytes() != Some(file.size) {
                return Err(CliError::Parse {
                    detail: "desktop artifact changed after verification".into(),
                });
            }
            let call = ObjectRpcCall::new(head, body);
            let response: AppUpdateUploadResponse = block_on_daemon_client(Box::pin(async {
                client
                    .object_rpc_call_to_target_with_transport_timeouts_and_affinity_async(
                        &node_id,
                        call,
                        DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(60)),
                        None,
                        true,
                        DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                    )
                    .await?
                    .decode_body()
                    .await
                    .map_err(Into::into)
            }))?;
            if response.release_id != id || response.expected_files != manifest.files.len() {
                return Err(CliError::Parse {
                    detail: "desktop upload receipt does not match the requested release".into(),
                });
            }
            println!(
                "{label}: desktop release {id}, {}/{} files",
                response.received_files, response.expected_files
            );
            if file_index + 1 == manifest.files.len() && response.status.is_none() {
                return Err(CliError::Parse {
                    detail: format!("{label}: upload completed without a durable desktop offer"),
                });
            }
        }
    }
    Ok(())
}
