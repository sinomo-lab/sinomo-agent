//! Existing non-Windows input/capture paths remain available independently.
use super::*;
use sinomo_device::desktop::surfaces::*;
fn unsupported() -> DesktopError {
    DesktopError::Unsupported(format!(
        "application sessions and semantic tray/menu surfaces are not implemented on {}",
        std::env::consts::OS
    ))
}
pub fn surface_capabilities() -> DesktopSurfaceCapabilities {
    DesktopSurfaceCapabilities {
        limitations: vec![unsupported().to_string()],
        ..Default::default()
    }
}
pub fn applications(_: &LocalNodePermissionStore) -> DesktopResult<Vec<DesktopApplicationInfo>> {
    Err(unsupported())
}
pub struct ApplicationTracker;
impl ApplicationTracker {
    pub fn new(
        _: DesktopApplication,
        _: LocalNodePermissionStore,
        _: Duration,
    ) -> DesktopResult<Arc<Self>> {
        Err(unsupported())
    }
    pub fn remaining(&self) -> Duration {
        Duration::ZERO
    }
    pub fn snapshot(&self) -> DesktopResult<DesktopApplicationSnapshot> {
        Err(unsupported())
    }
    pub fn resolve(&self, _: &str, _: &str) -> DesktopResult<DesktopWindowSurface> {
        Err(unsupported())
    }
    pub fn authority(self: &Arc<Self>, _: String) -> DesktopResult<DeviceAuthority> {
        Err(unsupported())
    }
    pub fn close(&self) {}
}
pub fn tray_items(_: &LocalNodePermissionStore) -> DesktopResult<DesktopSemanticSnapshot> {
    Err(unsupported())
}
pub fn invoke_tray(
    _: &LocalNodePermissionStore,
    _: String,
    _: DesktopSemanticAction,
    _: bool,
) -> DesktopResult<()> {
    Err(unsupported())
}
pub fn menu_items(_: Arc<ApplicationTracker>, _: String) -> DesktopResult<DesktopSemanticSnapshot> {
    Err(unsupported())
}
pub fn invoke_menu_item(
    _: Arc<ApplicationTracker>,
    _: String,
    _: String,
    _: bool,
) -> DesktopResult<()> {
    Err(unsupported())
}
pub fn shutdown_surfaces() -> bool {
    true
}
