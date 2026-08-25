use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(windows)]
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use fs2::FileExt as _;
use notify::Watcher as _;
use sinomo_runtime::config::{ConfigRegistry, ConfigStore, ConfigWriteTarget};
use sinomo_runtime::daemon::{DaemonComponentFactory, DaemonPaths};
use sinomo_runtime::dfs::{
    SIFS_MOUNT_CONFIG_KEY, SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE, SIFS_MOUNT_STATUS_CONFIG_KEY,
    SIFS_MOUNT_STATUS_ROUTE, SifsMountConfig, SifsMountCursorOnlyRecoveryRequest,
    SifsMountPlatform, SifsMountRuntimePhase, SifsMountRuntimeStatus, SifsMountStatusRequest,
    SifsMountStatusResponse, sifs_mount_config_definition, sifs_mount_status_config_definition,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct SifsMountSupervisorService {
    inner: Arc<SupervisorInner>,
}

struct SupervisorInner {
    status: RwLock<SifsMountRuntimeStatus>,
    shutdown: AtomicBool,
    wake: Arc<SupervisorWake>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

#[derive(Default)]
struct SupervisorWake {
    pending: Mutex<bool>,
    changed: Condvar,
}

impl SupervisorWake {
    fn signal(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            *pending = true;
            self.changed.notify_all();
        }
    }

    fn wait(&self, timeout: Option<Duration>) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        if *pending {
            *pending = false;
            return;
        }
        match timeout {
            Some(timeout) => {
                let (mut pending, _) = self
                    .changed
                    .wait_timeout(pending, timeout)
                    .unwrap_or_else(|error| error.into_inner());
                *pending = false;
            }
            None => {
                let mut pending = self
                    .changed
                    .wait(pending)
                    .unwrap_or_else(|error| error.into_inner());
                *pending = false;
            }
        }
    }
}

impl SifsMountSupervisorService {
    fn new(data_dir: SinomoDataDir, daemon_paths: DaemonPaths, daemon_pid: u32) -> Self {
        let wake = Arc::new(SupervisorWake::default());
        let inner = Arc::new(SupervisorInner {
            status: RwLock::new(SifsMountRuntimeStatus::default()),
            shutdown: AtomicBool::new(false),
            wake: Arc::clone(&wake),
            worker: Mutex::new(None),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("sinomo-sifs-mount".to_string())
            .spawn(move || run_supervisor(data_dir, daemon_paths, daemon_pid, worker_inner))
            .expect("spawn SIFS mount component worker");
        *inner.worker.lock().expect("mount worker lock") = Some(worker);
        Self { inner }
    }

    fn status(&self) -> SifsMountRuntimeStatus {
        self.inner
            .status
            .read()
            .map(|status| status.clone())
            .unwrap_or_else(|_| {
                error_status(&SifsMountConfig::default(), None, "status lock failed")
            })
    }
}

impl Drop for SupervisorInner {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.wake.signal();
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl Service for SifsMountSupervisorService {
    fn name(&self) -> &'static str {
        "sifs-mount-supervisor"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            SIFS_MOUNT_STATUS_ROUTE,
            SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE,
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let status = self.status();
        Box::pin(async move {
            match request.route.as_str() {
                SIFS_MOUNT_STATUS_ROUTE => {
                    let _: SifsMountStatusRequest = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(SIFS_MOUNT_STATUS_ROUTE, SifsMountStatusResponse { status })?;
                }
                SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE => {
                    let _: SifsMountCursorOnlyRecoveryRequest =
                        ctx.decode_request_body(&request).await?;
                    return Err(ServiceError::Service(
                        "filesystem-provider recovery is owned by the active external component"
                            .to_string(),
                    ));
                }
                route => return Err(ServiceError::UnknownRoute(route.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub(crate) fn sifs_mount_supervisor_component_factory(
    data_dir: SinomoDataDir,
    daemon_paths: DaemonPaths,
) -> DaemonComponentFactory {
    let service = SifsMountSupervisorService::new(data_dir, daemon_paths, std::process::id());
    let ready = service.clone();
    Arc::new(
        sinomo_runtime::module::StaticComponentFactory::new(
            sinomo_runtime::module::ComponentDescriptor::new(
                "sifs-mount-supervisor",
                env!("CARGO_PKG_VERSION"),
            ),
            move |_, scope| {
                scope.set_services(
                    ServiceRegistryBuilder::default()
                        .register(service.clone())
                        .build(),
                );
                let ready = ready.clone();
                scope.on_host_ready(move || ready.inner.wake.signal());
                Ok(())
            },
        )
        .expect("SIFS mount supervisor component descriptor"),
    )
}

fn run_supervisor(
    data_dir: SinomoDataDir,
    _daemon_paths: DaemonPaths,
    daemon_pid: u32,
    inner: Arc<SupervisorInner>,
) {
    let store = match mount_config_store() {
        Ok(store) => store,
        Err(error) => {
            publish_observed(
                &inner,
                error_status(&SifsMountConfig::default(), None, error),
            );
            return;
        }
    };
    let _lease = match SifsMountLease::try_acquire(&data_dir, "managed daemon component") {
        Ok(lease) => lease,
        Err(error) => {
            publish_observed(
                &inner,
                error_status(&SifsMountConfig::default(), None, error),
            );
            return;
        }
    };
    let _watcher = start_config_watcher(&data_dir, Arc::clone(&inner.wake)).ok();
    let mut active: Option<ActiveMount> = None;
    let mut active_config: Option<SifsMountConfig> = None;
    let mut restart_count = 0_u64;
    while !inner.shutdown.load(Ordering::Acquire) {
        let config =
            match store.get_typed::<SifsMountConfig>(&data_dir, None, SIFS_MOUNT_CONFIG_KEY) {
                Ok(config) => config,
                Err(error) => {
                    stop_active(&mut active);
                    publish(
                        &store,
                        &data_dir,
                        &inner,
                        error_status(
                            &SifsMountConfig::default(),
                            None,
                            format!("read SIFS mount config: {error}"),
                        ),
                    );
                    inner.wake.wait(Some(RETRY_DELAY));
                    continue;
                }
            };
        if !config.enabled {
            stop_active(&mut active);
            active_config = None;
            publish(
                &store,
                &data_dir,
                &inner,
                disabled_status(&config, daemon_pid, restart_count),
            );
            inner.wake.wait(None);
            continue;
        }
        let mount_point = match resolve_mount_point(&data_dir, &config.mount_point) {
            Ok(path) => path,
            Err(error) => {
                stop_active(&mut active);
                publish(
                    &store,
                    &data_dir,
                    &inner,
                    error_status(&config, None, error),
                );
                inner.wake.wait(Some(RETRY_DELAY));
                continue;
            }
        };
        let needs_start = active.is_none() || active_config.as_ref() != Some(&config);
        if needs_start {
            stop_active(&mut active);
            restart_count = restart_count.saturating_add(1);
            publish(
                &store,
                &data_dir,
                &inner,
                starting_status(&config, &mount_point, daemon_pid, restart_count),
            );
            match ActiveMount::start(&data_dir, &config, &mount_point, Arc::clone(&inner)) {
                Ok(mount) => {
                    active = Some(mount);
                    active_config = Some(config.clone());
                }
                Err(error) => {
                    publish(
                        &store,
                        &data_dir,
                        &inner,
                        error_status(&config, Some(&mount_point), error),
                    );
                    inner.wake.wait(Some(RETRY_DELAY));
                    continue;
                }
            }
        }
        if let Some(mount) = active.as_mut() {
            match mount.status(daemon_pid, restart_count) {
                Ok(status) => publish(&store, &data_dir, &inner, status),
                Err(error) => {
                    publish(
                        &store,
                        &data_dir,
                        &inner,
                        error_status(&config, Some(&mount_point), error),
                    );
                    stop_active(&mut active);
                    active_config = None;
                }
            }
        }
        inner.wake.wait(Some(RETRY_DELAY));
    }
    stop_active(&mut active);
}

fn mount_config_store() -> Result<ConfigStore, String> {
    let registry = ConfigRegistry::builder()
        .register(sifs_mount_config_definition())
        .map_err(|error| error.to_string())?
        .register(sifs_mount_status_config_definition())
        .map_err(|error| error.to_string())?
        .build();
    Ok(ConfigStore::new(registry))
}

pub(crate) fn managed_sifs_mount_config(
    data_dir: &SinomoDataDir,
) -> Result<SifsMountConfig, String> {
    mount_config_store()?
        .get_typed(data_dir, None, SIFS_MOUNT_CONFIG_KEY)
        .map_err(|error| error.to_string())
}

pub(crate) fn configure_managed_sifs_mount(
    data_dir: &SinomoDataDir,
    config: &SifsMountConfig,
) -> Result<PathBuf, String> {
    data_dir
        .ensure_root_layout()
        .map_err(|error| error.to_string())?;
    let mount_point = resolve_mount_point(data_dir, &config.mount_point)?;
    mount_config_store()?
        .set_typed(
            data_dir,
            None,
            SIFS_MOUNT_CONFIG_KEY,
            config,
            ConfigWriteTarget::Local,
        )
        .map_err(|error| error.to_string())?;
    Ok(mount_point)
}

fn start_config_watcher(
    data_dir: &SinomoDataDir,
    wake: Arc<SupervisorWake>,
) -> Result<notify::RecommendedWatcher, String> {
    let path = sifs_mount_config_definition().local_path(data_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let watched = path.clone();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event
            .ok()
            .is_some_and(|event| event.paths.iter().any(|path| path == &watched))
        {
            wake.signal();
        }
    })
    .map_err(|error| error.to_string())?;
    watcher
        .watch(
            path.parent().unwrap_or_else(|| Path::new(".")),
            notify::RecursiveMode::NonRecursive,
        )
        .map_err(|error| error.to_string())?;
    Ok(watcher)
}

pub(crate) struct SifsMountLease {
    file: File,
}

impl SifsMountLease {
    pub(crate) fn try_acquire(data_dir: &SinomoDataDir, owner: &str) -> Result<Self, String> {
        let path = data_dir.root().join("daemon/sifs-mount.lock");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| format!("open mount lease {}: {error}", path.display()))?;
        file.try_lock_exclusive().map_err(|error| {
            format!(
                "another managed or foreground mount owns {} while {owner} requested it: {error}",
                path.display()
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for SifsMountLease {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

enum ActiveMount {
    #[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
    Fuse {
        handle: Option<sinomo_dfs_fuse::DfsFuseMountHandle>,
        config: SifsMountConfig,
        mount_point: PathBuf,
    },
    #[cfg(windows)]
    External(ExternalMount),
    #[cfg(not(any(all(target_os = "linux", feature = "dfs-fuse"), windows)))]
    Unsupported,
}

impl ActiveMount {
    fn start(
        data_dir: &SinomoDataDir,
        config: &SifsMountConfig,
        mount_point: &Path,
        inner: Arc<SupervisorInner>,
    ) -> Result<Self, String> {
        #[cfg(not(windows))]
        let _ = &inner;
        #[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
        {
            let mut options = sinomo_dfs_fuse::DfsFuseMountOptions::new(mount_point)
                .with_data_dir(data_dir.root())
                .with_root_path(&config.root_path);
            options.read_only = config.read_only;
            options.allow_other = config.allow_other;
            let handle = sinomo_dfs_fuse::mount_sifs_background(options)
                .map_err(|error| format!("start Unix FUSE mount: {error}"))?;
            return Ok(Self::Fuse {
                handle: Some(handle),
                config: config.clone(),
                mount_point: mount_point.to_path_buf(),
            });
        }
        #[cfg(windows)]
        {
            return ExternalMount::start(data_dir, config, mount_point, inner).map(Self::External);
        }
        #[cfg(not(any(all(target_os = "linux", feature = "dfs-fuse"), windows)))]
        {
            return Ok(Self::Unsupported);
        }
        #[allow(unreachable_code)]
        Err("host filesystem presentation is unsupported on this platform".to_string())
    }

    fn status(
        &mut self,
        daemon_pid: u32,
        restart_count: u64,
    ) -> Result<SifsMountRuntimeStatus, String> {
        match self {
            #[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
            Self::Fuse {
                handle,
                config,
                mount_point,
            } => {
                if !handle.as_ref().is_some_and(|handle| handle.is_active()) {
                    return Err(handle
                        .as_ref()
                        .and_then(|handle| handle.fatal_error())
                        .unwrap_or_else(|| "Unix FUSE mount stopped".to_string()));
                }
                let mut status = starting_status(config, mount_point, daemon_pid, restart_count);
                status.phase = SifsMountRuntimePhase::Mounted;
                status.detail = "SIFS is mounted through Unix FUSE".to_string();
                Ok(status)
            }
            #[cfg(windows)]
            Self::External(mount) => mount.status(daemon_pid, restart_count),
            #[cfg(not(any(all(target_os = "linux", feature = "dfs-fuse"), windows)))]
            Self::Unsupported => Err("host filesystem presentation is unsupported".to_string()),
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        match self {
            #[cfg(all(target_os = "linux", feature = "dfs-fuse"))]
            Self::Fuse { handle, .. } => match handle.take() {
                Some(handle) => handle.shutdown().map_err(|error| error.to_string()),
                None => Ok(()),
            },
            #[cfg(windows)]
            Self::External(mount) => mount.stop(),
            #[cfg(not(any(all(target_os = "linux", feature = "dfs-fuse"), windows)))]
            Self::Unsupported => Ok(()),
        }
    }
}

#[cfg(windows)]
struct ExternalMount {
    config: SifsMountConfig,
    mount_point: PathBuf,
    data_dir_root: PathBuf,
    cancellation: sinomo_runtime::dfs::HostFilesystemCancellation,
    finished: mpsc::Receiver<Result<(), String>>,
    thread: Option<thread::JoinHandle<()>>,
    inner: Arc<SupervisorInner>,
}

#[cfg(windows)]
impl ExternalMount {
    fn start(
        data_dir: &SinomoDataDir,
        config: &SifsMountConfig,
        mount_point: &Path,
        inner: Arc<SupervisorInner>,
    ) -> Result<Self, String> {
        let data_dir_root = data_dir.root();
        let cancellation = sinomo_runtime::dfs::HostFilesystemCancellation::default();
        let worker_cancellation = cancellation.clone();
        let worker_data_dir = data_dir_root.clone();
        let worker_mount_point = mount_point.to_path_buf();
        let worker_config = config.clone();
        let sink: Arc<dyn sinomo_runtime::dfs::HostFilesystemStatusSink> =
            Arc::new(ExternalStatusSink {
                inner: Arc::clone(&inner),
            });
        let (finished_tx, finished) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("sinomo-host-filesystem-provider".to_string())
            .spawn(move || {
                let result = (|| {
                    let provider =
                        sinomo_runtime::dfs::HostFilesystemProviderClient::from_active_component(
                            &worker_data_dir,
                        )
                        .map_err(|error| error.to_string())?;
                    match provider
                        .reconcile(
                            worker_data_dir,
                            worker_mount_point,
                            worker_config,
                            Some(sink),
                            worker_cancellation,
                        )
                        .map_err(|error| error.to_string())?
                    {
                        sinomo_runtime::dfs::HostFilesystemProviderResponse::Error(error) => {
                            Err(format!("{}: {}", error.code, error.detail))
                        }
                        _ => Ok(()),
                    }
                })();
                let _ = finished_tx.send(result);
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            config: config.clone(),
            mount_point: mount_point.to_path_buf(),
            data_dir_root,
            cancellation,
            finished,
            thread: Some(thread),
            inner,
        })
    }

    fn status(
        &mut self,
        daemon_pid: u32,
        restart_count: u64,
    ) -> Result<SifsMountRuntimeStatus, String> {
        match self.finished.try_recv() {
            Ok(Ok(())) => return Err("filesystem provider stopped while enabled".to_string()),
            Ok(Err(error)) => return Err(error),
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err("filesystem provider result channel disconnected".to_string());
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let mut status = self
            .inner
            .status
            .read()
            .map(|status| status.clone())
            .unwrap_or_else(|_| {
                starting_status(&self.config, &self.mount_point, daemon_pid, restart_count)
            });
        status.daemon_pid = Some(daemon_pid);
        status.restart_count = restart_count;
        Ok(status)
    }

    fn stop(&mut self) -> Result<(), String> {
        self.cancellation.cancel();
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| "filesystem provider thread panicked".to_string())?;
        }
        let provider = sinomo_runtime::dfs::HostFilesystemProviderClient::from_active_component(
            &self.data_dir_root,
        )
        .map_err(|error| error.to_string())?;
        match provider
            .stop(
                self.data_dir_root.clone(),
                self.mount_point.clone(),
                self.config.clone(),
                true,
            )
            .map_err(|error| error.to_string())?
        {
            sinomo_runtime::dfs::HostFilesystemProviderResponse::Error(error) => {
                Err(format!("{}: {}", error.code, error.detail))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(windows)]
struct ExternalStatusSink {
    inner: Arc<SupervisorInner>,
}

#[cfg(windows)]
impl sinomo_runtime::dfs::HostFilesystemStatusSink for ExternalStatusSink {
    fn status(&self, status: SifsMountRuntimeStatus) {
        publish_observed(&self.inner, status);
        self.inner.wake.signal();
    }
}

fn stop_active(active: &mut Option<ActiveMount>) {
    if let Some(mut mount) = active.take() {
        if let Err(error) = mount.stop() {
            tracing::warn!("stop SIFS host mount: {error}");
        }
    }
}

fn resolve_mount_point(data_dir: &SinomoDataDir, value: &str) -> Result<PathBuf, String> {
    if value.trim() != value || value.is_empty() {
        return Err("mount point must be non-empty without surrounding whitespace".to_string());
    }
    let path = if value == "~" || value.starts_with("~/") || value.starts_with("~\\") {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or_else(|| "cannot expand mount point without a home directory".to_string())?;
        PathBuf::from(home).join(value.trim_start_matches(['~', '/', '\\']))
    } else {
        PathBuf::from(value)
    };
    if !path.is_absolute() {
        return Err("mount point must be absolute or begin with ~/".to_string());
    }
    if path.starts_with(data_dir.root()) || data_dir.root().starts_with(&path) {
        return Err("mount point must not overlap the Sinomo data directory".to_string());
    }
    Ok(path)
}

fn starting_status(
    config: &SifsMountConfig,
    mount_point: &Path,
    daemon_pid: u32,
    restart_count: u64,
) -> SifsMountRuntimeStatus {
    let mut status = SifsMountRuntimeStatus::default();
    status.phase = SifsMountRuntimePhase::Starting;
    status.platform = SifsMountPlatform::current();
    status.configured_mount_point = config.mount_point.clone();
    status.resolved_mount_point = Some(mount_point.display().to_string());
    status.root_path = config.root_path.clone();
    status.read_only = config.read_only;
    status.sync_local_changes = config.sync_local_changes;
    status.daemon_pid = Some(daemon_pid);
    status.restart_count = restart_count;
    status.detail = "starting SIFS host filesystem provider".to_string();
    status.updated_at = now();
    status
}

fn disabled_status(
    config: &SifsMountConfig,
    daemon_pid: u32,
    restart_count: u64,
) -> SifsMountRuntimeStatus {
    let mut status = SifsMountRuntimeStatus::default();
    status.phase = SifsMountRuntimePhase::Disabled;
    status.platform = SifsMountPlatform::current();
    status.configured_mount_point = config.mount_point.clone();
    status.root_path = config.root_path.clone();
    status.read_only = config.read_only;
    status.sync_local_changes = config.sync_local_changes;
    status.daemon_pid = Some(daemon_pid);
    status.restart_count = restart_count;
    status.detail = "SIFS host mount is disabled".to_string();
    status.updated_at = now();
    status
}

fn error_status(
    config: &SifsMountConfig,
    mount_point: Option<&Path>,
    error: impl Into<String>,
) -> SifsMountRuntimeStatus {
    let error = error.into();
    let mut status = SifsMountRuntimeStatus::default();
    status.phase = SifsMountRuntimePhase::Error;
    status.platform = SifsMountPlatform::current();
    status.configured_mount_point = config.mount_point.clone();
    status.resolved_mount_point = mount_point.map(|path| path.display().to_string());
    status.root_path = config.root_path.clone();
    status.read_only = config.read_only;
    status.sync_local_changes = config.sync_local_changes;
    status.last_error = Some(error.clone());
    status.detail = error;
    status.updated_at = now();
    status
}

fn publish(
    store: &ConfigStore,
    data_dir: &SinomoDataDir,
    inner: &SupervisorInner,
    status: SifsMountRuntimeStatus,
) {
    if let Err(error) = store.set_typed(
        data_dir,
        None,
        SIFS_MOUNT_STATUS_CONFIG_KEY,
        &status,
        ConfigWriteTarget::Local,
    ) {
        tracing::warn!("persist SIFS mount status: {error}");
    }
    publish_observed(inner, status);
}

fn publish_observed(inner: &SupervisorInner, status: SifsMountRuntimeStatus) {
    if let Ok(mut observed) = inner.status.write() {
        *observed = status;
    }
}

fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_registers_provider_neutral_status_routes() {
        let root = sinomo_testdir::TestDir::new("sifs-mount-component");
        let data_dir = SinomoDataDir::new(&root);
        let factory =
            sifs_mount_supervisor_component_factory(data_dir.clone(), DaemonPaths::new(&data_dir));
        let scope = factory
            .mount(&sinomo_runtime::module::ComponentContext::new(Arc::new(
                data_dir,
            )))
            .expect("mount component");
        assert!(scope.services().owns(SIFS_MOUNT_STATUS_ROUTE));
        assert!(scope.services().owns(SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE));
    }

    #[test]
    fn managed_configuration_roundtrips_without_provider_types() {
        let root = sinomo_testdir::TestDir::new("sifs-mount-config");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let config = SifsMountConfig {
            mount_point: root.join("mount").display().to_string(),
            ..SifsMountConfig::default()
        };
        let resolved = configure_managed_sifs_mount(&data_dir, &config).expect("configure");
        assert_eq!(resolved, root.join("mount"));
        assert_eq!(managed_sifs_mount_config(&data_dir).expect("read"), config);
    }
}
