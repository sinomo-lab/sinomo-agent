//! Linux selects its native session backend once per target/lease. A Wayland
//! session never falls back to Xwayland's global XTest surface.
use super::*;
#[path = "linux/wayland.rs"]
mod wayland;
#[path = "linux/x11.rs"]
mod x11;

fn is_wayland() -> bool {
    std::env::var_os("WAYLAND_DISPLAY").is_some()
        || std::env::var("XDG_SESSION_TYPE").is_ok_and(|v| v == "wayland")
}
fn portal_target(target: &DesktopTarget) -> bool {
    matches!(target, DesktopTarget::Monitor { device_name } if device_name.starts_with("wayland:"))
}
pub fn capabilities() -> DesktopCapabilities {
    if is_wayland() {
        wayland::capabilities()
    } else {
        x11::capabilities()
    }
}
pub fn video_targets(
    permissions: &LocalNodePermissionStore,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    if is_wayland() {
        wayland::video_targets(permissions)
    } else {
        x11::enumerate_targets(false)
    }
}
pub fn enumerate_targets(
    permissions: &LocalNodePermissionStore,
    windows: bool,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    if is_wayland() {
        wayland::targets(permissions, windows)
    } else {
        x11::enumerate_targets(windows)
    }
}
pub fn enumerate_child_windows(target: &DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
    if is_wayland() || portal_target(target) {
        Err(DesktopError::Unsupported(
            "Wayland has no background child-window input; select raw mode explicitly".into(),
        ))
    } else {
        x11::enumerate_child_windows(target)
    }
}
pub fn validate_target(target: &DesktopTarget) -> DesktopResult<()> {
    if portal_target(target) {
        wayland::validate_target(target)
    } else if is_wayland() {
        Err(DesktopError::Unsupported(
            "Wayland requires an approved portal target in explicit raw mode".into(),
        ))
    } else {
        x11::validate_target(target)
    }
}
pub fn validate_capture_target(target: &DesktopTarget) -> DesktopResult<()> {
    if portal_target(target) {
        wayland::validate_target(target)
    } else {
        Ok(())
    }
}
pub fn initialize_input_thread() -> DesktopResult<()> {
    Ok(())
}
pub enum Input {
    X11(x11::Input),
    Wayland(wayland::Input),
}
impl Input {
    pub fn new(
        target: &DesktopTarget,
        permissions: &LocalNodePermissionStore,
    ) -> DesktopResult<Self> {
        validate_target(target)?;
        if portal_target(target) {
            wayland::Input::new(target, permissions).map(Self::Wayland)
        } else {
            x11::Input::new(target).map(Self::X11)
        }
    }
    pub fn send(&mut self, event: &DesktopInput) -> DesktopResult<()> {
        match self {
            Self::X11(input) => input.send(event),
            Self::Wayland(input) => input.send(event),
        }
    }
    pub fn release_gamepad(&mut self) {
        if let Self::X11(input) = self {
            input.release_gamepad();
        }
    }
}
pub fn capture(
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
    wake: std::os::unix::net::UnixStream,
) -> DesktopResult<()> {
    if portal_target(&config.target) {
        wayland::capture(config, permissions, mailbox, stop, ready)
    } else {
        x11::capture(config, permissions, mailbox, stop, ready, &wake)
    }
}
pub fn shutdown() {
    wayland::shutdown();
}

/// Opt-in native diagnostics; kept separate from normal capture work.
pub(crate) fn frame_trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("SINOMO_MEDIA_STAGE_TRACE").is_some_and(|v| v == "1"))
}
pub(crate) fn trace_frame(value: serde_json::Value) {
    eprintln!("{value}");
}
