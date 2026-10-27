//! Application-instance and semantic-surface contracts. Discovery is evidence,
//! never permission to broaden a window grant or control the shell.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopApplication {
    pub process_id: u32,
    pub process_started: u64,
}
impl DesktopApplication {
    pub fn resource_id(&self) -> String {
        format!("application:{}:{}", self.process_id, self.process_started)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopApplicationInfo {
    pub application: DesktopApplication,
    pub resource_id: String,
    pub name: String,
    pub executable: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopSurfaceCapabilities {
    pub application_sessions: bool,
    pub window_events: bool,
    pub tray_items: bool,
    pub menu_items: bool,
    pub limitations: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopWindowSurface {
    /// Opaque reference valid only for this observed window lifetime.
    pub surface_id: String,
    pub resource_id: String,
    pub window: DesktopTargetInfo,
    pub owner: Option<DesktopTarget>,
    pub parent: Option<DesktopTarget>,
    pub top_level: bool,
    pub class_name: String,
    pub visible: bool,
    pub enabled: bool,
    /// An enabled owned popup with a disabled owner; independent of focus.
    pub blocks_owner: bool,
    /// Cross-process owner links are discovery evidence, not application authority.
    pub application_grant_applies: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopApplicationSnapshot {
    pub application: DesktopApplication,
    pub revision: u64,
    pub windows: Vec<DesktopWindowSurface>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplicationWindowBinding {
    pub application_id: String,
    pub surface_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopSemanticItem {
    pub item_id: String,
    pub resource_id: String,
    pub name: String,
    pub kind: String,
    /// Shell-hosted tray controls have no verified application ownership.
    pub owner_application: Option<DesktopApplication>,
    pub bounds: [i32; 4],
    pub invoke: bool,
    /// None means the UIA method exists but provider support is unverified.
    pub context_menu: Option<bool>,
    pub may_activate_window: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopSemanticSnapshot {
    pub items: Vec<DesktopSemanticItem>,
    pub limitations: Vec<String>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DesktopSemanticAction {
    Invoke,
    ContextMenu,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DesktopCaptureIdentity {
    Target {
        target: DesktopTarget,
        application: Option<ApplicationWindowBinding>,
    },
    Overview {
        application: DesktopApplication,
        omitted_surfaces: Vec<String>,
    },
    /// A derived view-status frame, never pixels attributed to a live window.
    Unavailable {
        application: DesktopApplication,
        surface_id: Option<String>,
        reason: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopOverviewTile {
    pub surface_id: String,
    pub bounds: [u32; 4],
    pub source_width: u32,
    pub source_height: u32,
    pub captured_at_unix_ms: u64,
}
