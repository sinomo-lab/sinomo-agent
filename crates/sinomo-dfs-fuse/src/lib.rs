//! Optional FUSE mount adapter for SIFS.
//!
//! The crate is intentionally cheap by default. Real FUSE support is compiled
//! only on Unix targets with the `fuse` feature.

use std::fmt;
#[cfg(any(test, all(unix, feature = "fuse")))]
use std::path::Component;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

#[cfg(all(unix, feature = "fuse"))]
mod fuse;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsFuseMountOptions {
    pub data_dir: Option<PathBuf>,
    pub mount_point: PathBuf,
    pub root_path: String,
    pub read_only: bool,
    pub foreground: bool,
    pub allow_other: bool,
    pub mount_name: String,
    pub attr_ttl: Duration,
    pub entry_ttl: Duration,
    /// The longest one syscall waits for its DFS work. `None`, the default,
    /// sets no deadline: a syscall waits for its outcome, however long the
    /// cluster takes to reach one. The budget is the mount operator's own
    /// deadline and travels with every request the syscall sends.
    pub operation_budget: Option<Duration>,
}

impl DfsFuseMountOptions {
    pub fn new(mount_point: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: None,
            mount_point: mount_point.into(),
            root_path: "/".to_string(),
            read_only: false,
            foreground: true,
            allow_other: false,
            mount_name: "sinomo-sifs".to_string(),
            attr_ttl: Duration::from_secs(1),
            entry_ttl: Duration::from_secs(1),
            operation_budget: None,
        }
    }

    pub fn with_operation_budget(mut self, operation_budget: Duration) -> Self {
        self.operation_budget = Some(operation_budget);
        self
    }

    pub fn with_root_path(mut self, root_path: impl Into<String>) -> Self {
        self.root_path = root_path.into();
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }
}

#[derive(Debug, Error)]
pub enum DfsFuseError {
    #[error(
        "SIFS FUSE support is not available on this platform or in this build; use a Unix build with FUSE enabled"
    )]
    Unsupported,
    #[error("invalid SIFS path `{path}`: {detail}")]
    InvalidPath { path: String, detail: String },
    #[error("invalid SIFS FUSE mount point `{mount_point}`: {detail}")]
    InvalidMountPoint { mount_point: String, detail: String },
    #[error("local data directory error: {0}")]
    LocalData(String),
    #[error("DFS error: {0}")]
    Dfs(String),
    #[error("FUSE mount error at {mount_point}: {detail}")]
    Mount { mount_point: String, detail: String },
}

pub type Result<T> = std::result::Result<T, DfsFuseError>;

/// Read-only Linux host readiness captured before a long-lived background
/// mount is started.
///
/// The result is evidence, not a durable capability: the device, helper, or
/// host configuration can still change before the actual mount syscall.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsFuseLinuxPreflight {
    pub mount_point: PathBuf,
    pub fuse_device: PathBuf,
    pub mount_helper: PathBuf,
    pub direct_fuse_device_access: bool,
    pub allow_other_enabled: bool,
}

/// Owns a nonblocking SIFS FUSE mount.
///
/// The resolved mount root remains valid for as long as this handle is kept
/// alive. Dropping the handle closes the background FUSE session and unmounts
/// the filesystem. Nothing the mount serves retires it: an operation's
/// outcome and an unacknowledged daemon handoff both leave it serving.
/// Owner-only mounts intentionally avoid FUSE `auto_unmount`, because fuser
/// implements that option by implicitly requesting `allow_other`; the owning
/// session still guarantees orderly unmounting.
#[must_use = "dropping the SIFS mount handle retires the filesystem"]
pub struct DfsFuseMountHandle {
    mount_root: PathBuf,
    cancellation: sinomo_runtime::dfs::DfsCancellationFlag,
    #[cfg(all(unix, feature = "fuse"))]
    session: Option<fuser::BackgroundSession>,
}

impl DfsFuseMountHandle {
    /// Absolute, canonical host path at which the selected SIFS root is
    /// mounted.
    pub fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    /// Whether the background FUSE worker has not exited yet.
    pub fn is_active(&self) -> bool {
        #[cfg(all(unix, feature = "fuse"))]
        {
            self.session
                .as_ref()
                .is_some_and(|session| !session.guard.is_finished())
        }
        #[cfg(not(all(unix, feature = "fuse")))]
        {
            false
        }
    }

    /// Returns a clone of the mount lifecycle cancellation source.
    ///
    /// A long-lived supervisor can retain this clone before entering a
    /// blocking mount/open path and cancel it when daemon shutdown begins.
    pub fn cancellation(&self) -> sinomo_runtime::dfs::DfsCancellationFlag {
        self.cancellation.clone()
    }

    /// Cooperatively cancels in-flight DFS work owned by this mount.
    ///
    /// This does not itself unmount. Drop the handle after requesting shutdown
    /// so distributed waits can observe cancellation before the FUSE session
    /// is released.
    pub fn request_shutdown(&self) {
        self.cancellation.cancel();
    }

    /// Requests cooperative shutdown, unmounts the FUSE session and joins
    /// its worker. Long-lived supervisors should prefer this over plain drop
    /// when they need an actionable unmount error.
    pub fn shutdown(self) -> Result<()> {
        self.request_shutdown();
        #[cfg(all(unix, feature = "fuse"))]
        {
            let mut handle = self;
            if let Some(session) = handle.session.take() {
                session
                    .umount_and_join()
                    .map_err(|source| DfsFuseError::Mount {
                        mount_point: handle.mount_root.display().to_string(),
                        detail: format!("failed to shut down background mount: {source}"),
                    })?;
            }
        }
        Ok(())
    }

    #[cfg(all(unix, feature = "fuse"))]
    fn new(
        mount_root: PathBuf,
        session: fuser::BackgroundSession,
        cancellation: sinomo_runtime::dfs::DfsCancellationFlag,
    ) -> Self {
        Self {
            mount_root,
            cancellation,
            session: Some(session),
        }
    }
}

impl fmt::Debug for DfsFuseMountHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfsFuseMountHandle")
            .field("mount_root", &self.mount_root)
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

impl Drop for DfsFuseMountHandle {
    fn drop(&mut self) {
        // Distributed reads, lock waits, and bootstrap retries must observe
        // cancellation before unmount begins. Otherwise a supervisor can drop
        // the session while accepted work continues through its full network
        // timeout budget.
        self.cancellation.cancel();
        #[cfg(all(unix, feature = "fuse"))]
        {
            // fuser's BackgroundSession owns the mount object. Dropping it
            // first initiates unmount while the rest of this handle is still
            // intact.
            drop(self.session.take());
        }
    }
}

pub fn mount_sifs(options: DfsFuseMountOptions) -> Result<()> {
    #[cfg(all(unix, feature = "fuse"))]
    {
        fuse::mount_sifs(options)
    }
    #[cfg(not(all(unix, feature = "fuse")))]
    {
        let _ = options;
        Err(DfsFuseError::Unsupported)
    }
}

/// Mount SIFS on a background worker and return an owning session handle.
///
/// Unlike [`mount_sifs`], this returns after the kernel mount is established.
/// Missing mount-point directories are created, and the returned handle
/// exposes the canonical mount root suitable for use as a host-side sandbox
/// filesystem boundary. Keep the handle alive for as long as paths below that
/// root may be used. This lifecycle guarantee requires an executable
/// `fusermount3` or `fusermount` helper.
pub fn mount_sifs_background(options: DfsFuseMountOptions) -> Result<DfsFuseMountHandle> {
    mount_sifs_background_with_cancellation(
        options,
        sinomo_runtime::dfs::DfsCancellationFlag::new(),
    )
}

/// Like [`mount_sifs_background`], using a caller-owned cooperative
/// cancellation source during both mount bootstrap and the mounted lifetime.
///
/// The caller should retain a clone and cancel it before joining a supervisor
/// which may currently be blocked opening the mount. Dropping the returned
/// handle also cancels the same source before releasing the FUSE session.
pub fn mount_sifs_background_with_cancellation(
    options: DfsFuseMountOptions,
    cancellation: sinomo_runtime::dfs::DfsCancellationFlag,
) -> Result<DfsFuseMountHandle> {
    #[cfg(all(unix, feature = "fuse"))]
    {
        fuse::mount_sifs_background(options, cancellation)
    }
    #[cfg(not(all(unix, feature = "fuse")))]
    {
        let _ = (options, cancellation);
        Err(DfsFuseError::Unsupported)
    }
}

/// Verify the Linux prerequisites required by a daemon-owned background
/// mount without changing mount or cluster state.
pub fn preflight_linux_sifs_background_mount(
    options: &DfsFuseMountOptions,
) -> Result<DfsFuseLinuxPreflight> {
    #[cfg(all(target_os = "linux", feature = "fuse"))]
    {
        fuse::preflight_linux_sifs_background_mount(options)
    }
    #[cfg(not(all(target_os = "linux", feature = "fuse")))]
    {
        let _ = options;
        Err(DfsFuseError::Unsupported)
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn validate_safe_mount_point_path(path: &Path) -> Result<()> {
    let mount_point = path.display().to_string();
    if !path.is_absolute() {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point,
            detail: "SIFS mounts require an absolute host path".to_string(),
        });
    }
    if path == Path::new("/") {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point,
            detail: "refusing to mount over the host filesystem root".to_string(),
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point,
            detail: "parent-directory traversal is not allowed".to_string(),
        });
    }
    #[cfg(target_os = "linux")]
    for reserved in [
        "/bin", "/boot", "/dev", "/etc", "/proc", "/run", "/sbin", "/sys", "/usr", "/var",
    ] {
        if path == Path::new(reserved) || path.starts_with(reserved) {
            return Err(DfsFuseError::InvalidMountPoint {
                mount_point,
                detail: format!("refusing to mount inside the system path {reserved}"),
            });
        }
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(DfsFuseError::InvalidMountPoint {
                mount_point,
                detail: "mount point must not be a symbolic link".to_string(),
            });
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(DfsFuseError::InvalidMountPoint {
                mount_point,
                detail: "mount point is not a directory".to_string(),
            });
        }
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        // A disconnected FUSE mount reports ENOTCONN here on Linux. The
        // background mount path retains its explicit lazy-unmount recovery.
        Err(_) => {}
    }
    Ok(())
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn prepare_safe_mount_point(path: &Path) -> Result<PathBuf> {
    validate_safe_mount_point_path(path)?;
    std::fs::create_dir_all(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to create mount point: {source}"),
    })?;
    let metadata = std::fs::symlink_metadata(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to inspect mount point: {source}"),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point: path.display().to_string(),
            detail: "mount point must not be a symbolic link".to_string(),
        });
    }
    if !metadata.is_dir() {
        return Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: "mount point is not a directory".to_string(),
        });
    }
    let resolved = std::fs::canonicalize(path).map_err(|source| DfsFuseError::Mount {
        mount_point: path.display().to_string(),
        detail: format!("failed to resolve mount point: {source}"),
    })?;
    if std::fs::read_dir(&resolved)
        .map_err(|source| DfsFuseError::Mount {
            mount_point: resolved.display().to_string(),
            detail: format!("failed to inspect mount point contents: {source}"),
        })?
        .next()
        .transpose()
        .map_err(|source| DfsFuseError::Mount {
            mount_point: resolved.display().to_string(),
            detail: format!("failed to inspect mount point contents: {source}"),
        })?
        .is_some()
    {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point: resolved.display().to_string(),
            detail: "refusing to hide entries in a non-empty mount point".to_string(),
        });
    }
    Ok(resolved)
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn normalize_sifs_root(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(DfsFuseError::InvalidPath {
            path: path.to_string(),
            detail: "path cannot be empty".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(DfsFuseError::InvalidPath {
            path: path.to_string(),
            detail: "path must be absolute".to_string(),
        });
    }
    let mut parts = Vec::new();
    for component in trimmed.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn join_sifs_child(parent: &str, name: &std::ffi::OsStr) -> Result<String> {
    let Some(name) = name.to_str() else {
        return Err(DfsFuseError::InvalidPath {
            path: parent.to_string(),
            detail: "path segment is not UTF-8".to_string(),
        });
    };
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(DfsFuseError::InvalidPath {
            path: format!("{parent}/{name}"),
            detail: "invalid path segment".to_string(),
        });
    }
    if parent == "/" {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("{}/{name}", parent.trim_end_matches('/')))
    }
}

#[cfg(any(test, all(unix, feature = "fuse")))]
fn parent_sifs_path(path: &str) -> String {
    let normalized = normalize_sifs_root(path).unwrap_or_else(|_| "/".to_string());
    if normalized == "/" {
        return "/".to_string();
    }
    normalized
        .rsplit_once('/')
        .and_then(|(parent, _)| (!parent.is_empty()).then_some(parent.to_string()))
        .unwrap_or_else(|| "/".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn normalizes_absolute_sifs_roots() {
        assert_eq!(normalize_sifs_root("/").expect("root"), "/");
        assert_eq!(
            normalize_sifs_root("/sinomo//nodes/./node/../node/files/").expect("normalized"),
            "/sinomo/nodes/node/files"
        );
    }

    #[test]
    fn joins_child_segments_without_escaping_parent() {
        assert_eq!(
            join_sifs_child("/", OsStr::new("users")).expect("child"),
            "/users"
        );
        assert_eq!(
            join_sifs_child("/sinomo", OsStr::new("nodes")).expect("child"),
            "/sinomo/nodes"
        );
        assert!(join_sifs_child("/sinomo", OsStr::new("../x")).is_err());
    }

    #[test]
    fn computes_parent_paths() {
        assert_eq!(parent_sifs_path("/"), "/");
        assert_eq!(parent_sifs_path("/sinomo"), "/");
        assert_eq!(parent_sifs_path("/sinomo/nodes"), "/sinomo");
    }

    #[test]
    fn background_mount_point_is_created_and_resolved() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-point");
        let mount_point = root.join("nested").join("sifs");

        let resolved = prepare_safe_mount_point(&mount_point).expect("mount point");

        assert!(mount_point.is_dir());
        assert!(resolved.is_absolute());
        assert_eq!(resolved, std::fs::canonicalize(&mount_point).unwrap());
    }

    #[test]
    fn background_mount_point_rejects_files_and_empty_paths() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-file");
        let file = root.join("not-a-directory");
        std::fs::write(&file, b"fixture").expect("mount point fixture");

        assert!(prepare_safe_mount_point(&file).is_err());
        assert!(prepare_safe_mount_point(Path::new("")).is_err());
    }

    #[test]
    fn background_mount_point_requires_a_safe_absolute_empty_directory() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-safety");
        let nonempty = root.join("nonempty");
        std::fs::create_dir_all(&nonempty).expect("nonempty mount point");
        std::fs::write(nonempty.join("keep"), b"host data").expect("host data");

        assert!(matches!(
            validate_safe_mount_point_path(Path::new("relative/mount")),
            Err(DfsFuseError::InvalidMountPoint { detail, .. })
                if detail.contains("absolute")
        ));
        assert!(matches!(
            validate_safe_mount_point_path(Path::new("/")),
            Err(DfsFuseError::InvalidMountPoint { detail, .. })
                if detail.contains("filesystem root")
        ));
        assert!(matches!(
            validate_safe_mount_point_path(Path::new("/tmp/sinomo/../mount")),
            Err(DfsFuseError::InvalidMountPoint { detail, .. })
                if detail.contains("traversal")
        ));
        assert!(matches!(
            prepare_safe_mount_point(&nonempty),
            Err(DfsFuseError::InvalidMountPoint { detail, .. })
                if detail.contains("non-empty")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn background_mount_point_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let root = sinomo_testdir::TestDir::new("sinomo-fuse-background-mount-symlink");
        let target = root.join("target");
        let link = root.join("link");
        std::fs::create_dir_all(&target).expect("symlink target");
        symlink(&target, &link).expect("mount point symlink");

        assert!(matches!(
            prepare_safe_mount_point(&link),
            Err(DfsFuseError::InvalidMountPoint { detail, .. })
                if detail.contains("symbolic link")
        ));
    }

    #[test]
    fn mount_handle_shutdown_cancels_the_external_source() {
        let cancellation = sinomo_runtime::dfs::DfsCancellationFlag::new();
        let handle = DfsFuseMountHandle {
            mount_root: PathBuf::from("/tmp/sinomo-inactive-test-mount"),
            cancellation: cancellation.clone(),
            #[cfg(all(unix, feature = "fuse"))]
            session: None,
        };

        handle.shutdown().expect("inactive handle shutdown");

        assert!(cancellation.is_cancelled());
    }

    #[cfg(all(unix, feature = "fuse"))]
    struct MountedTestDaemon(std::process::Child);

    #[cfg(all(unix, feature = "fuse"))]
    impl MountedTestDaemon {
        fn start(data: &sinomo_runtime::local::SinomoDataDir) -> Self {
            use sinomo_runtime::api::{
                DaemonApiClient, DaemonPaths, DaemonProbeStatus, probe_daemon_status,
            };
            use std::process::{Command, Stdio};
            use std::time::{Duration, Instant};
            data.ensure_root_layout().unwrap();
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let binary = std::env::var_os("SINOMO_TEST_BINARY")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    std::env::current_exe()
                        .unwrap()
                        .parent()
                        .unwrap()
                        .parent()
                        .unwrap()
                        .join("sinomo")
                });
            let spawn = |phase| {
                let mut child = Self(
                    Command::new(&binary)
                        .args([
                            "daemon",
                            "run",
                            "--app-owned",
                            "--host",
                            "127.0.0.1",
                            "--port",
                        ])
                        .arg(port.to_string())
                        .env("SINOMO_DATA_DIR", data.root())
                        .env(
                            sinomo_runtime::api::SINOMO_APP_DAEMON_LAUNCH_ID_ENV,
                            format!("mount-test-{}-{port}-{phase}", std::process::id()),
                        )
                        .stdin(Stdio::piped())
                        .stdout(Stdio::null())
                        .stderr(
                            std::fs::File::create(data.root().join(format!("{phase}.stderr")))
                                .unwrap(),
                        )
                        .spawn()
                        .expect("start isolated daemon from the test build"),
                );
                let until = Instant::now() + Duration::from_secs(45);
                loop {
                    assert!(
                        child.0.try_wait().unwrap().is_none() && Instant::now() < until,
                        "{phase} daemon unavailable"
                    );
                    if let DaemonProbeStatus::Running(status) =
                        probe_daemon_status(&DaemonPaths::new(data), Duration::from_millis(200))
                    {
                        assert_eq!(status.pid, child.0.id());
                        if phase == "bootstrap" {
                            break;
                        }
                        let client = DaemonApiClient::at_address(&status.address);
                        let request = sinomo_runtime::dfs::dfs_status_rpc_request().unwrap();
                        if let Ok(reply) = client.call_request(&request)
                            && let Ok(status) =
                                sinomo_runtime::dfs::decode_dfs_status_rpc_response(&reply)
                            && status.incomplete_reasons.is_empty()
                            && status.object_count > 0
                        {
                            break;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                child
            };
            let mut bootstrap = spawn("bootstrap");
            DaemonApiClient::at_address(format!("127.0.0.1:{port}"))
                .cluster_init(sinomo_runtime::cluster::ClusterControlInitRequest {
                    cluster_name: "kernel mount test".into(),
                    node_display_name: None,
                    node_role: None,
                })
                .expect("initialize the private test node");
            let until = Instant::now() + Duration::from_secs(30);
            while bootstrap.0.try_wait().unwrap().is_none() {
                assert!(Instant::now() < until, "bootstrap daemon did not exit");
                std::thread::sleep(Duration::from_millis(25));
            }
            spawn("main")
        }
    }

    #[cfg(all(unix, feature = "fuse"))]
    impl Drop for MountedTestDaemon {
        fn drop(&mut self) {
            drop(self.0.stdin.take());
            let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while matches!(self.0.try_wait(), Ok(None)) && std::time::Instant::now() < until {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            if matches!(self.0.try_wait(), Ok(None)) {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }

    #[cfg(all(unix, feature = "fuse"))]
    #[test]
    #[ignore = "requires the built sinomo binary, a FUSE helper and a usable /dev/fuse"]
    fn owner_only_mount_can_remove_dfs_native_root_owned_entries() {
        use std::os::unix::fs::MetadataExt;

        let root = sinomo_testdir::TestDir::new("sinomo-fuse-owner-projection");
        let mount_point = root.join("mount");
        let data_dir = root.join("data");
        let data = sinomo_runtime::local::SinomoDataDir::new(data_dir.clone());
        let _daemon = MountedTestDaemon::start(&data);
        let peer = std::sync::Arc::new(
            sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(data.clone()).unwrap(),
        );
        let mut opened = sinomo_runtime::dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane(data, peer.clone(), peer).unwrap();
        opened
            .service
            .posix_mkdir_all("/service-created")
            .expect("create DFS-native directory");
        assert_eq!(
            opened
                .service
                .posix_stat_distributed("/service-created")
                .expect("stat durable DFS-native directory")
                .uid,
            0,
            "the regression fixture must use DFS-native neutral ownership"
        );
        drop(opened);

        let handle =
            mount_sifs_background(DfsFuseMountOptions::new(&mount_point).with_data_dir(data_dir))
                .expect("background SIFS mount");
        let mounted_directory = handle.mount_root().join("service-created");
        let metadata = std::fs::metadata(&mounted_directory).expect("stat mounted directory");
        // SAFETY: `geteuid` and `getegid` have no preconditions.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);

        std::fs::remove_dir(&mounted_directory)
            .expect("mount owner removes a DFS-native root-owned directory");
        assert!(!mounted_directory.exists());
        handle.shutdown().expect("shutdown owner-only mount");
    }

    #[cfg(all(unix, feature = "fuse"))]
    #[test]
    #[ignore = "requires the built sinomo binary, a FUSE helper and a usable /dev/fuse"]
    fn dead_fuse_worker_is_unmounted_and_removed() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-dead-worker-cleanup");
        let mount_point = root.join("mount");
        let data_dir = root.join("data");
        let _daemon =
            MountedTestDaemon::start(&sinomo_runtime::local::SinomoDataDir::new(data_dir.clone()));
        let mut handle =
            mount_sifs_background(DfsFuseMountOptions::new(&mount_point).with_data_dir(data_dir))
                .expect("background SIFS mount");
        assert!(handle.is_active(), "fixture must start a live FUSE worker");

        // Model the worker exiting before its owning application handle. The
        // BackgroundSession drop is the same lifecycle edge reached when the
        // worker terminates or the owner is torn down.
        drop(handle.session.take());
        assert!(!handle.is_active());
        drop(handle);

        std::fs::remove_dir(&mount_point)
            .expect("an exited worker must leave an unmounted, removable mount point");
        assert!(!mount_point.exists());
    }
}
