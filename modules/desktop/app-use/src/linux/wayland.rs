//! User-approved portal sessions, shared by capture/input leases. No global
//! Wayland input protocol, Xwayland fallback, restore token or permission writer.
use super::*;
use std::os::fd::OwnedFd;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
#[path = "wayland/portal.rs"]
mod portal;
#[path = "wayland/video.rs"]
mod video;

#[derive(Clone, Debug)]
struct Target {
    info: DesktopTargetInfo,
    node: u32,
    serial: Option<u64>,
    devices: u32,
}
enum Command {
    Input(DesktopInput, mpsc::SyncSender<DesktopResult<()>>),
    Remote(mpsc::SyncSender<DesktopResult<OwnedFd>>),
}
struct Shared {
    permissions: LocalNodePermissionStore,
    preparing_until: Instant,
    status: Mutex<DesktopResult<Option<Target>>>,
    stopped: AtomicBool,
    users: AtomicUsize,
    commands: tokio::sync::mpsc::Sender<Command>,
}
impl Shared {
    fn target(&self) -> DesktopResult<Target> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(DesktopError::TargetUnavailable(
                "Wayland portal session ended; enumerate and approve a fresh target".into(),
            ));
        }
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()?
            .ok_or_else(|| {
                DesktopError::TargetUnavailable(
                    "approve the local Wayland portal chooser, then enumerate targets again".into(),
                )
            })
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}
struct Owner {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
static OWNER: OnceLock<Mutex<Option<Owner>>> = OnceLock::new();
fn owner() -> &'static Mutex<Option<Owner>> {
    OWNER.get_or_init(|| Mutex::new(None))
}
fn platform(error: impl std::fmt::Display) -> DesktopError {
    DesktopError::Platform(error.to_string())
}
fn unsupported(detail: &str) -> DesktopError {
    DesktopError::Unsupported(detail.into())
}

pub fn capabilities() -> DesktopCapabilities {
    let mut caps = DesktopCapabilities { frame_rate_ceiling: false, accessibility: false, capture_formats: vec![sinomo_device::NativeVideoSampleFormat::Bgra8],
        platform: "linux".into(), capture: false, keyboard_mouse: false, background_window_input: false,
        gamepad: "unsupported".into(), limitations: vec![
            "Wayland uses explicit raw mode through RemoteDesktop/ScreenCast portals; background window messages and gamepads are unavailable".into(),
            "The first raw targets request opens a local chooser asynchronously; repeat targets after approval and grant the exact returned resource locally".into(),
            "Portal targets use portal-advertised stream coordinates; map video pixels using target bounds, without assuming desktop logical or physical units".into(),
            "Cursor capture is hidden for the lifetime of the portal session; include_cursor does not add a synthetic cursor".into(),
        ],
    };
    match std::thread::spawn(|| portal::runtime()?.block_on(portal::capabilities())).join() {
        Ok(Ok((capture, input))) => {
            if capture {
                match pipewire::check_available() {
                    Ok(()) => caps.capture = true,
                    Err(error) => caps
                        .limitations
                        .push(format!("PipeWire capture client unavailable: {error}")),
                }
            }
            caps.keyboard_mouse = input;
        }
        Ok(Err(error)) => caps.limitations.push(error.to_string()),
        Err(_) => caps
            .limitations
            .push("Wayland portal capability probe failed".into()),
    }
    caps
}
/// Generic video discovery observes an approved portal session; it never
/// launches the chooser while a caller is only listing camera/screen sources.
pub fn video_targets(
    permissions: &LocalNodePermissionStore,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    authorize_host(permissions)?;
    let slot = owner().lock().unwrap_or_else(|e| e.into_inner());
    let Some(current) = slot.as_ref() else {
        return Ok(Vec::new());
    };
    if current.shared.stopped.load(Ordering::Acquire) {
        return Ok(Vec::new());
    }
    if current.shared.permissions.path() != permissions.path() {
        return Err(DesktopError::PermissionDenied(
            "portal session belongs to a different node-local permission store".into(),
        ));
    }
    current
        .shared
        .status
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .map(|target| target.map(|target| vec![target.info]).unwrap_or_default())
}
pub fn targets(
    permissions: &LocalNodePermissionStore,
    windows: bool,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    if windows {
        return Err(unsupported(
            "Wayland background input is unavailable; explicitly request raw targets",
        ));
    }
    authorize_host(permissions)?;
    let mut slot = owner().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(current) = slot.as_ref() {
        let same_owner = current.shared.permissions.path() == permissions.path();
        let status = current
            .shared
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if !current.shared.stopped.load(Ordering::Acquire) {
            if !same_owner {
                return Err(DesktopError::PermissionDenied(
                    "portal session belongs to a different node-local permission store".into(),
                ));
            }
            return status.map(|target| target.map(|t| vec![t.info]).unwrap_or_default());
        }
        drop(slot.take());
        if same_owner && let Err(error) = status {
            return Err(error);
        }
    }
    let (commands, receiver) = tokio::sync::mpsc::channel(8);
    let shared = Arc::new(Shared {
        permissions: permissions.clone(),
        preparing_until: Instant::now() + Duration::from_secs(120),
        status: Mutex::new(Ok(None)),
        stopped: AtomicBool::new(false),
        users: AtomicUsize::new(0),
        commands,
    });
    let state = shared.clone();
    let worker = std::thread::Builder::new()
        .name("wayland-portal".into())
        .spawn(move || {
            let result = portal::runtime()
                .and_then(|runtime| runtime.block_on(portal::run(&state, receiver)));
            if let Err(error) = result {
                *state.status.lock().unwrap_or_else(|e| e.into_inner()) = Err(error);
            }
            state.stop();
        })
        .map_err(platform)?;
    *slot = Some(Owner {
        shared,
        worker: Some(worker),
    });
    Ok(Vec::new())
}
pub fn validate_target(target: &DesktopTarget) -> DesktopResult<()> {
    let slot = owner().lock().unwrap_or_else(|e| e.into_inner());
    let shared = &slot
        .as_ref()
        .ok_or_else(|| {
            DesktopError::TargetUnavailable("unknown Wayland portal target; enumerate first".into())
        })?
        .shared;
    if shared.target()?.info.target != *target {
        return Err(DesktopError::TargetUnavailable(
            "stale or foreign Wayland portal target".into(),
        ));
    }
    Ok(())
}
struct Lease {
    shared: Arc<Shared>,
    target: Target,
}
impl Lease {
    fn acquire(
        target: &DesktopTarget,
        permissions: &LocalNodePermissionStore,
        input: bool,
    ) -> DesktopResult<Self> {
        let slot = owner().lock().unwrap_or_else(|e| e.into_inner());
        let shared = slot
            .as_ref()
            .ok_or_else(|| DesktopError::TargetUnavailable("portal is not open".into()))?
            .shared
            .clone();
        let actual = shared.target()?;
        if shared.permissions.path() != permissions.path() {
            return Err(DesktopError::PermissionDenied(
                "Wayland target belongs to another node-local permission store".into(),
            ));
        }
        if actual.info.target != *target {
            return Err(DesktopError::TargetUnavailable(
                "portal target changed".into(),
            ));
        }
        if input && actual.devices & 3 == 0 {
            return Err(unsupported(
                "the portal did not grant pointer or keyboard control",
            ));
        }
        shared.users.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            shared,
            target: actual,
        })
    }
    fn remote(&self) -> DesktopResult<OwnedFd> {
        let (reply, response) = mpsc::sync_channel(1);
        self.shared
            .commands
            .try_send(Command::Remote(reply))
            .map_err(platform)?;
        response
            .recv_timeout(Duration::from_secs(4))
            .map_err(platform)?
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.shared.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.stop();
        }
    }
}
pub struct Input {
    lease: Lease,
}
impl Input {
    pub fn new(
        target: &DesktopTarget,
        permissions: &LocalNodePermissionStore,
    ) -> DesktopResult<Self> {
        let lease = Lease::acquire(target, permissions, true)?;
        Ok(Self { lease })
    }
    pub fn send(&mut self, event: &DesktopInput) -> DesktopResult<()> {
        self.lease.shared.target()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.lease
            .shared
            .commands
            .try_send(Command::Input(event.clone(), reply))
            .map_err(platform)?;
        // A timed out input is never replayed. End its session so queued work
        // cannot survive the caller's failed lease.
        match response.recv_timeout(Duration::from_secs(4)) {
            Ok(result) => result,
            Err(error) => {
                self.lease.shared.stop();
                Err(platform(format!(
                    "portal input outcome unknown; do not replay: {error}"
                )))
            }
        }
    }
}
pub fn capture(
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
) -> DesktopResult<()> {
    let result = Lease::acquire(&config.target, &permissions, false).and_then(|lease| {
        pipewire::check_available()
            .map_err(|error| unsupported(&format!("PipeWire client unavailable: {error}")))?;
        lease.remote().map(|fd| (lease, fd))
    });
    let (lease, fd) = match result {
        Ok(pair) => pair,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    video::capture(&lease, fd, config, permissions, mailbox, stop, ready)
}
pub fn shutdown() {
    drop(owner().lock().unwrap_or_else(|e| e.into_inner()).take());
}

#[cfg(test)]
#[path = "wayland/tests.rs"]
mod tests;
