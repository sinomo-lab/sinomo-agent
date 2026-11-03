use super::*;
fn unsupported() -> DesktopError {
    DesktopError::Unsupported(format!(
        "app-use OS integration is not implemented on {}",
        std::env::consts::OS
    ))
}
pub fn capabilities() -> DesktopCapabilities {
    DesktopCapabilities {
        capture_formats: vec![],
        platform: std::env::consts::OS.into(),
        capture: false,
        keyboard_mouse: false,
        background_window_input: false,
        gamepad: "unsupported".into(),
        limitations: vec![unsupported().to_string()],
    }
}
pub fn enumerate_targets(_: bool) -> DesktopResult<Vec<DesktopTargetInfo>> {
    Err(unsupported())
}
pub fn enumerate_child_windows(_: &DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
    Err(unsupported())
}
pub fn validate_target(_: &DesktopTarget) -> DesktopResult<()> {
    Err(unsupported())
}
pub struct Input;
pub fn initialize_input_thread() -> DesktopResult<()> {
    Err(unsupported())
}
impl Input {
    pub fn new(_: &DesktopTarget) -> DesktopResult<Self> {
        Err(unsupported())
    }
    pub fn send(&mut self, _: &DesktopInput) -> DesktopResult<()> {
        Err(unsupported())
    }
    pub fn release_gamepad(&mut self) {}
}
pub fn capture(
    _: DesktopCaptureConfig,
    _: LocalNodePermissionStore,
    _: &CaptureMailbox,
    _: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
) -> DesktopResult<()> {
    let _ = ready.send(Err(unsupported()));
    Err(unsupported())
}
