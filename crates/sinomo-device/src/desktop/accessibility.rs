//! Semantic observations are evidence, not grants. Actions bind to the exact
//! bounded observation and are reauthorized on the device that owns the UI.
use serde::{Deserialize, Serialize};

pub const ANDROID_ACCESSIBILITY_TARGET: &str = "android:accessibility:display:0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessibilitySnapshot {
    pub snapshot_id: String,
    pub captured_at_unix_ms: u64,
    pub width: u32,
    pub height: u32,
    pub nodes: Vec<AccessibilityNode>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessibilityNode {
    pub id: String,
    pub parent: Option<String>,
    pub window_id: i32,
    pub package: String,
    pub class_name: String,
    pub view_id: String,
    pub text: String,
    pub description: String,
    pub bounds: [i32; 4],
    pub enabled: bool,
    pub clickable: bool,
    pub editable: bool,
    pub scrollable: bool,
    pub password: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccessibilityAction {
    Click,
    LongClick,
    Focus,
    SetText { text: String },
    ScrollForward,
    ScrollBackward,
}
