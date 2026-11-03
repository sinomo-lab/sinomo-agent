//! Declarative conversation panels supplied by daemon-hosted modules. Native
//! code produces bounded paint data; no provider code executes in the GUI.
use crate::*;
use serde::{Deserialize, Serialize};
pub const CONVERSATION_PANELS: &str = "sinomo.app.conversation.panels";
pub const PANEL_SCHEMA: &str = "sinomo.ui.session-panel/v1";
pub const PANEL_INTERFACE: &str = "sinomo.ui.session-panel";
pub const PANEL_RENDER: &str = "ui.session-panel.render";
pub const PANEL_ROUTE: &str = "/agent/session/panel";
pub const SESSION_ROUTE: &str = "/agent/session/access";
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionPanelDefinition {
    pub label: String,
    pub service_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionPanelRequest {
    pub conversation_id: String,
    pub contribution_id: String,
    #[serde(default)]
    pub expected_generation: Option<u64>,
    pub module_id: String,
    pub render: PanelRenderRequest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PanelRenderRequest {
    pub viewport: UiViewport,
    pub theme: UiThemeSnapshot,
    pub event: Option<UiInputEvent>,
    #[serde(default)]
    pub state: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PanelRenderResponse {
    pub frame: UiWidgetFrame,
    pub state: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionAccessRequest {
    pub conversation_id: String,
    pub request: crate::sessions::SessionRequest,
}
pub fn extension_point() -> ExtensionPointDefinition {
    ExtensionPointDefinition::new(
        CONVERSATION_PANELS,
        "1.0.0",
        UI_EXTENSION_CONTEXT_SCHEMA_ID,
        UI_EXTENSION_PAYLOAD_SCHEMA_ID,
        UI_EXTENSION_RESOLVED_SCHEMA_ID,
        ExtensionCompositionPolicy::ManyOrdered,
    )
    .with_limits(16, 64 * 1024, 2 * 1024 * 1024, 5000)
}
pub fn validate_response(response: &PanelRenderResponse) -> Result<(), String> {
    if encode_sobj(response).map_err(|e| e.to_string())?.len() > 2 * 1024 * 1024 {
        return Err("panel response exceeds limit".into());
    }
    response.frame.validate().map_err(|e| e.to_string())?;
    if !response.frame.host_requests.is_empty() {
        return Err("declarative panels cannot request ambient host effects".into());
    }
    if serde_json::to_vec(&response.state)
        .map_err(|e| e.to_string())?
        .len()
        > 64 * 1024
    {
        return Err("panel state exceeds limit".into());
    }
    Ok(())
}
