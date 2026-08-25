use crate::{
    DfsNfsError, DfsNfsServer, DfsNfsServerOptions, NFS_MOUNT_CONFIG_KEY,
    NFS_MOUNT_STATUS_CONFIG_KEY, NativeNfsMountError, NfsMountConfig, NfsMountRuntimePhase,
    NfsMountRuntimeStatus, mount_native_nfs_target, nfs_config_store, nfs_mount_config_definition,
    prepare_native_mount_target, unmount_native_nfs_target,
};
use sinomo_config::{ConfigStore, ConfigWriteTarget};
use sinomo_core::local::SinomoDataDir;
use std::sync::Arc;
use std::time::Duration;

const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const CONFIG_WRITE_SETTLE_RETRY: Duration = Duration::from_millis(10);
const CONFIG_WRITE_SETTLE_ATTEMPTS: usize = 100;

enum SupervisorEvent {
    ConfigChanged,
    ServerFinished(Result<(), String>),
}

struct ActiveNfsMount {
    config: NfsMountConfig,
    target: String,
    server_task: tokio::task::JoinHandle<()>,
}

pub fn spawn_nfs_mount_supervisor(
    data_dir: SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    runtime.spawn(run_nfs_mount_supervisor(data_dir, shutdown_rx))
}

async fn run_nfs_mount_supervisor(
    data_dir: SinomoDataDir,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let store = match nfs_config_store() {
        Ok(store) => Arc::new(store),
        Err(error) => {
            tracing::error!(target: "sinomo.dfs.nfs", %error, "NFS config store is unavailable");
            return;
        }
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let _watcher = match start_config_watcher(&data_dir, event_tx.clone()) {
        Ok(watcher) => watcher,
        Err(error) => {
            let status = NfsMountRuntimeStatus::error(
                &NfsMountConfig::default(),
                None,
                "config-watch-failed",
                error,
                "Restore access to the Sinomo configs directory and restart the daemon.",
            );
            publish_status(&store, &data_dir, &status);
            return;
        }
    };

    let mut active: Option<ActiveNfsMount> = None;
    let mut reconcile = true;
    loop {
        if reconcile {
            reconcile = false;
            let config = match store.get_typed::<NfsMountConfig>(
                &data_dir,
                None,
                NFS_MOUNT_CONFIG_KEY,
            ) {
                Ok(config) => config,
                Err(error) => {
                    publish_status(
                        &store,
                        &data_dir,
                        &NfsMountRuntimeStatus::error(
                            &NfsMountConfig::default(),
                            None,
                            "config-read-failed",
                            format!("read NFS mount settings: {error}"),
                            "Correct or reset the NFS module settings, then refresh the module page.",
                        ),
                    );
                    continue;
                }
            };

            if active
                .as_ref()
                .is_some_and(|active| active.config != config)
            {
                let previous = active.take().expect("checked active NFS mount");
                match stop_active_mount(previous).await {
                    Ok(()) => {}
                    Err((previous, error)) => {
                        publish_native_error(
                            &store,
                            &data_dir,
                            &previous.config,
                            Some(previous.target.clone()),
                            &error,
                        );
                        active = Some(previous);
                        continue;
                    }
                }
            }
            if !config.enabled {
                publish_status(&store, &data_dir, &NfsMountRuntimeStatus::disabled(&config));
            } else if active.is_none() {
                publish_status(&store, &data_dir, &starting_status(&config, None));
                match start_active_mount(&data_dir, config.clone(), event_tx.clone()).await {
                    Ok((next, status)) => {
                        publish_status(&store, &data_dir, &status);
                        active = Some(next);
                    }
                    Err(StartMountError::Native { target, source }) => {
                        publish_native_error(&store, &data_dir, &config, target, &source);
                    }
                    Err(StartMountError::Server(source)) => {
                        publish_server_error(&store, &data_dir, &config, source);
                    }
                }
            }
        }

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    if let Some(previous) = active.take()
                        && let Err((previous, _)) = stop_active_mount(previous).await
                    {
                        previous.server_task.abort();
                        let _ = previous.server_task.await;
                    }
                    let mut status = NfsMountRuntimeStatus::disabled(
                        &store.get_typed(&data_dir, None, NFS_MOUNT_CONFIG_KEY)
                            .unwrap_or_default(),
                    );
                    status.detail = "NFS mount stopped with the daemon".to_string();
                    publish_status(&store, &data_dir, &status);
                    break;
                }
            }
            event = event_rx.recv() => {
                match event {
                    Some(SupervisorEvent::ConfigChanged) => {
                        await_config_write_settled(&data_dir).await;
                        reconcile = true;
                    }
                    Some(SupervisorEvent::ServerFinished(result)) => {
                        if let Some(previous) = active.take() {
                            let detail = match result {
                                Ok(()) => "NFS server stopped while the mount remained enabled".to_string(),
                                Err(error) => format!("NFS server stopped: {error}"),
                            };
                            publish_status(
                                &store,
                                &data_dir,
                                &NfsMountRuntimeStatus::error(
                                    &previous.config,
                                    Some(previous.target),
                                    "server-stopped",
                                    detail,
                                    "Disable and re-enable the NFS mount. If it repeats, inspect the daemon log.",
                                ),
                            );
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

enum StartMountError {
    Server(DfsNfsError),
    Native {
        target: Option<String>,
        source: NativeNfsMountError,
    },
}

async fn start_active_mount(
    data_dir: &SinomoDataDir,
    config: NfsMountConfig,
    event_tx: tokio::sync::mpsc::UnboundedSender<SupervisorEvent>,
) -> Result<(ActiveNfsMount, NfsMountRuntimeStatus), StartMountError> {
    let target = prepare_native_mount_target(data_dir, &config.target, true).map_err(|source| {
        StartMountError::Native {
            target: None,
            source,
        }
    })?;
    wait_for_local_daemon_if_clustered(data_dir)
        .await
        .map_err(StartMountError::Server)?;
    let server = DfsNfsServer::bind(DfsNfsServerOptions {
        data_dir: Some(data_dir.root()),
        bind: config.bind.clone(),
        export_name: config.export_name.clone(),
        root_path: config.root_path.clone(),
        read_only: config.read_only,
    })
    .await
    .map_err(StartMountError::Server)?;
    let endpoint = server.local_addr();
    let server_task = tokio::spawn(async move {
        let result = server
            .serve_until_unmounted()
            .await
            .map_err(|error| error.to_string());
        let _ = event_tx.send(SupervisorEvent::ServerFinished(result));
    });
    if let Err(source) =
        mount_native_nfs_target(&target, endpoint, &config.export_name, config.read_only).await
    {
        server_task.abort();
        let _ = server_task.await;
        return Err(StartMountError::Native {
            target: Some(target),
            source,
        });
    }

    let status = NfsMountRuntimeStatus {
        phase: NfsMountRuntimePhase::Mounted,
        platform: crate::platform_label().to_string(),
        configured_target: config.target.clone(),
        resolved_target: Some(target.clone()),
        bind: config.bind.clone(),
        endpoint: Some(endpoint.to_string()),
        export_name: config.export_name.clone(),
        root_path: config.root_path.clone(),
        read_only: config.read_only,
        server_running: true,
        mounted: true,
        error_code: None,
        last_error: None,
        resolution: None,
        detail: "SIFS is mounted through the native NFSv3 client".to_string(),
        updated_at: now_rfc3339(),
    };
    Ok((
        ActiveNfsMount {
            config,
            target,
            server_task,
        },
        status,
    ))
}

async fn wait_for_local_daemon_if_clustered(data_dir: &SinomoDataDir) -> Result<(), DfsNfsError> {
    if !sinomo_core::network::ClusterPaths::new(data_dir)
        .state
        .exists()
    {
        return Ok(());
    }
    let paths = sinomo_api::DaemonPaths::new(data_dir);
    let mut last_error = None;
    for _ in 0..20 {
        let paths = paths.clone();
        match tokio::task::spawn_blocking(move || {
            sinomo_api::DaemonApiClient::connect_for_paths(&paths)
                .and_then(|client| client.status().map(drop))
        })
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(error) => last_error = Some(format!("daemon readiness worker failed: {error}")),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(DfsNfsError::Dfs(format!(
        "local daemon did not become ready for the clustered NFS mount: {}",
        last_error.unwrap_or_else(|| "readiness was not observed".to_string())
    )))
}

async fn stop_active_mount(
    mut active: ActiveNfsMount,
) -> Result<(), (ActiveNfsMount, NativeNfsMountError)> {
    if let Err(error) = unmount_native_nfs_target(&active.target).await {
        return Err((active, error));
    }
    if tokio::time::timeout(SERVER_STOP_TIMEOUT, &mut active.server_task)
        .await
        .is_err()
    {
        active.server_task.abort();
        let _ = active.server_task.await;
    }
    Ok(())
}

fn starting_status(
    config: &NfsMountConfig,
    resolved_target: Option<String>,
) -> NfsMountRuntimeStatus {
    NfsMountRuntimeStatus {
        phase: NfsMountRuntimePhase::Starting,
        platform: crate::platform_label().to_string(),
        configured_target: config.target.clone(),
        resolved_target,
        bind: config.bind.clone(),
        endpoint: None,
        export_name: config.export_name.clone(),
        root_path: config.root_path.clone(),
        read_only: config.read_only,
        server_running: false,
        mounted: false,
        error_code: None,
        last_error: None,
        resolution: None,
        detail: "Starting loopback NFS server and native mount".to_string(),
        updated_at: now_rfc3339(),
    }
}

fn publish_native_error(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    config: &NfsMountConfig,
    target: Option<String>,
    error: &NativeNfsMountError,
) {
    publish_status(
        store,
        data_dir,
        &NfsMountRuntimeStatus::error(
            config,
            target,
            error.code,
            error.detail.clone(),
            error.resolution.clone(),
        ),
    );
}

fn publish_server_error(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    config: &NfsMountConfig,
    error: DfsNfsError,
) {
    let (code, resolution) = match error {
        DfsNfsError::Bind { .. } => (
            "endpoint-bind-failed",
            "Choose an unused loopback port. Windows drive mapping requires daemon authority for port 111.",
        ),
        DfsNfsError::InvalidConfiguration(_) => (
            "invalid-server-config",
            "Correct the endpoint, export name, or SIFS root and save the settings again.",
        ),
        DfsNfsError::Dfs(_) => (
            "dfs-unavailable",
            "Ensure the local daemon and DFS module are healthy, then disable and re-enable the mount.",
        ),
        DfsNfsError::Serve(_) => (
            "server-failed",
            "Disable and re-enable the mount. If it repeats, inspect the daemon log.",
        ),
    };
    publish_status(
        store,
        data_dir,
        &NfsMountRuntimeStatus::error(config, None, code, error.to_string(), resolution),
    );
}

fn publish_status(store: &ConfigStore, data_dir: &SinomoDataDir, status: &NfsMountRuntimeStatus) {
    if let Err(error) = store.set_typed(
        data_dir,
        None,
        NFS_MOUNT_STATUS_CONFIG_KEY,
        status,
        ConfigWriteTarget::Local,
    ) {
        tracing::error!(target: "sinomo.dfs.nfs", %error, "could not publish NFS mount status");
    }
}

fn start_config_watcher(
    data_dir: &SinomoDataDir,
    event_tx: tokio::sync::mpsc::UnboundedSender<SupervisorEvent>,
) -> Result<notify::RecommendedWatcher, String> {
    use notify::Watcher as _;

    let config_path = nfs_mount_config_definition().local_path(data_dir);
    let directory = config_path
        .parent()
        .expect("NFS config definition path has a parent")
        .to_path_buf();
    std::fs::create_dir_all(&directory).map_err(|error| {
        format!(
            "create NFS mount config directory {}: {error}",
            directory.display()
        )
    })?;
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if match result.as_ref() {
            Ok(event) => event
                .paths
                .iter()
                .any(|path| config_event_path_matches(path, &config_path)),
            Err(_) => true,
        } {
            let _ = event_tx.send(SupervisorEvent::ConfigChanged);
        }
    })
    .map_err(|error| format!("create NFS config watcher: {error}"))?;
    watcher
        .watch(&directory, notify::RecursiveMode::NonRecursive)
        .map_err(|error| {
            format!(
                "watch NFS config directory {}: {error}",
                directory.display()
            )
        })?;
    Ok(watcher)
}

async fn await_config_write_settled(data_dir: &SinomoDataDir) {
    let temporary_path = nfs_mount_config_definition()
        .local_path(data_dir)
        .with_extension("json.tmp");
    for _ in 0..CONFIG_WRITE_SETTLE_ATTEMPTS {
        if !temporary_path.exists() {
            return;
        }
        tokio::time::sleep(CONFIG_WRITE_SETTLE_RETRY).await;
    }
}

fn config_event_path_matches(observed: &std::path::Path, config_path: &std::path::Path) -> bool {
    let Some(observed_name) = observed.file_name() else {
        return false;
    };
    let config_name = config_path
        .file_name()
        .expect("NFS config definition path has a file name");
    let temporary_path = config_path.with_extension("json.tmp");
    let temporary_name = temporary_path
        .file_name()
        .expect("NFS temporary config path has a file name");
    observed_name_eq(observed_name, config_name) || observed_name_eq(observed_name, temporary_name)
}

#[cfg(windows)]
fn observed_name_eq(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left.to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
fn observed_name_eq(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left == right
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_status(
        store: &ConfigStore,
        data_dir: &SinomoDataDir,
        predicate: impl Fn(&NfsMountRuntimeStatus) -> bool,
    ) -> NfsMountRuntimeStatus {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(status) = store.get_typed::<NfsMountRuntimeStatus>(
                    data_dir,
                    None,
                    NFS_MOUNT_STATUS_CONFIG_KEY,
                ) && predicate(&status)
                {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("NFS status transition timed out")
    }

    #[cfg(any(target_os = "linux", windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supervisor_reacts_to_config_and_publishes_actionable_server_error() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-nfs-supervisor-data");
        #[cfg(target_os = "linux")]
        let mount_root = sinomo_testdir::TestDir::new("sinomo-nfs-supervisor-mount");
        let data_dir = SinomoDataDir::new(data_root.path().to_path_buf());
        let store = nfs_config_store().expect("NFS config store");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let supervisor = tokio::spawn(run_nfs_mount_supervisor(data_dir.clone(), shutdown_rx));

        wait_for_status(&store, &data_dir, |status| {
            status.phase == NfsMountRuntimePhase::Disabled
        })
        .await;
        #[cfg(target_os = "linux")]
        let target = mount_root.path().join("mount").display().to_string();
        #[cfg(windows)]
        let target = NfsMountConfig::default().target;
        let config = NfsMountConfig {
            enabled: true,
            target,
            bind: "0.0.0.0:0".to_string(),
            ..NfsMountConfig::default()
        };
        store
            .set_typed(
                &data_dir,
                None,
                NFS_MOUNT_CONFIG_KEY,
                &config,
                ConfigWriteTarget::Local,
            )
            .expect("enable invalid NFS config");
        let status = wait_for_status(&store, &data_dir, |status| {
            status.error_code.as_deref() == Some("invalid-server-config")
        })
        .await;
        assert_eq!(status.phase, NfsMountRuntimePhase::Error);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| { error.contains("refusing non-loopback NFS bind") })
        );
        assert!(
            status
                .resolution
                .as_deref()
                .is_some_and(|resolution| { resolution.contains("endpoint") })
        );

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        supervisor.await.expect("supervisor task");
    }
}
