//! Stable, renderer-neutral contracts for module-contributed application UI.
//!
//! The catalog contains declarative metadata and immutable artifact references,
//! never Rust trait objects or SUI widgets. Executable custom surfaces speak the
//! asynchronous widget protocol below and return bounded display lists plus a
//! semantic tree. Applications remain responsible for validating and rendering
//! every frame.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ExtensionCompositionPolicy, ExtensionContextCondition, ExtensionContextValue,
    ExtensionContribution, ExtensionPointDefinition, ExtensionVersionRequirement,
    NativeModuleApiError,
};

pub const UI_WIDGET_PROTOCOL_ID: &str = "sinomo.ui.widget";
pub const UI_WIDGET_PROTOCOL_VERSION: &str = "1.0.0";
pub const UI_WIDGET_WIT: &str = include_str!("../wit/sinomo-ui-widget-v1.wit");
pub const UI_EXTENSION_CONTEXT_SCHEMA_ID: &str = "sinomo.ui.context/v1";
pub const UI_EXTENSION_PAYLOAD_SCHEMA_ID: &str = "sinomo.ui.contribution/v1";
pub const UI_EXTENSION_RESOLVED_SCHEMA_ID: &str = "sinomo.ui.resolved-contribution-list/v1";
pub const FILE_PREVIEW_EXTENSION_POINT_ID: &str = "sinomo.app.files.preview-provider";
pub const FILE_PREVIEW_EXTENSION_POINT_VERSION: &str = "1.0.0";

pub const MAX_UI_CONTRIBUTIONS_PER_MODULE: usize = 256;
pub const MAX_UI_COMPONENT_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_UI_FRAME_COMMANDS: usize = 8_192;
pub const MAX_UI_FRAME_PATH_ELEMENTS: usize = 65_536;
pub const MAX_UI_FRAME_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_UI_FRAME_IMAGES: usize = 32;
pub const MAX_UI_FRAME_IMAGE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_UI_FRAME_SEMANTICS_NODES: usize = 4_096;
pub const MAX_UI_FRAME_STACK_DEPTH: usize = 64;
pub const MAX_UI_CONTENT_READ_BYTES: u64 = 8 * 1024 * 1024;

/// One module-owned contribution to a host-owned UI extension point.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionContribution {
    pub contribution_id: String,
    pub extension_point_id: String,
    pub extension_point_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default)]
    pub order: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub when: Vec<UiContextCondition>,
    pub payload: UiContributionPayload,
}

impl UiExtensionContribution {
    pub fn custom_widget(
        contribution_id: impl Into<String>,
        extension_point_id: impl Into<String>,
        extension_point_version: impl Into<String>,
        widget: UiCustomWidgetContribution,
    ) -> Self {
        Self {
            contribution_id: contribution_id.into(),
            extension_point_id: extension_point_id.into(),
            extension_point_version: extension_point_version.into(),
            group: String::new(),
            order: 0,
            before: Vec::new(),
            after: Vec::new(),
            when: Vec::new(),
            payload: UiContributionPayload::CustomWidget { widget },
        }
    }

    pub fn with_order(mut self, group: impl Into<String>, order: i32) -> Self {
        self.group = group.into();
        self.order = order;
        self
    }

    pub fn validate(&self) -> Result<(), UiExtensionContractError> {
        validate_ui_token(&self.contribution_id, "contribution id")?;
        validate_ui_token(&self.extension_point_id, "extension point id")?;
        validate_ui_token(&self.extension_point_version, "extension point version")?;
        if !self.group.is_empty() {
            validate_ui_token(&self.group, "contribution group")?;
        }
        validate_unique_tokens(&self.before, "before contribution")?;
        validate_unique_tokens(&self.after, "after contribution")?;
        if self
            .before
            .iter()
            .any(|candidate| candidate == &self.contribution_id)
            || self
                .after
                .iter()
                .any(|candidate| candidate == &self.contribution_id)
        {
            return Err(UiExtensionContractError::InvalidContribution {
                contribution_id: self.contribution_id.clone(),
                detail: "a contribution cannot order itself before or after itself".to_string(),
            });
        }
        let mut context_keys = BTreeSet::new();
        for condition in &self.when {
            condition.validate()?;
            if !context_keys.insert(condition.key.as_str()) {
                return Err(UiExtensionContractError::InvalidContribution {
                    contribution_id: self.contribution_id.clone(),
                    detail: format!("duplicate context condition `{}`", condition.key),
                });
            }
        }
        self.payload.validate(&self.contribution_id)
    }

    /// Adapt the legacy UI-specific surface onto the generic extension
    /// contribution envelope. Hosts use this during the migration period so
    /// old manifests and new generic contributors share one registry.
    pub fn to_extension_contribution(&self) -> Result<ExtensionContribution, NativeModuleApiError> {
        let mut contribution = ExtensionContribution::declarative(
            self.contribution_id.clone(),
            self.extension_point_id.clone(),
            ExtensionVersionRequirement::exact(self.extension_point_version.clone()),
            UI_EXTENSION_PAYLOAD_SCHEMA_ID,
            crate::encode_sobj(&self.payload)?,
        )
        .with_order(self.group.clone(), self.order);
        contribution.before = self.before.clone();
        contribution.after = self.after.clone();
        contribution.when = self
            .when
            .iter()
            .map(|condition| ExtensionContextCondition {
                key: condition.key.clone(),
                value: match &condition.value {
                    UiContextValue::Bool(value) => ExtensionContextValue::Bool(*value),
                    UiContextValue::String(value) => ExtensionContextValue::String(value.clone()),
                    UiContextValue::Integer(value) => ExtensionContextValue::Integer(*value),
                },
            })
            .collect();
        Ok(contribution)
    }
}

pub fn file_preview_extension_point_definition() -> ExtensionPointDefinition {
    ExtensionPointDefinition::new(
        FILE_PREVIEW_EXTENSION_POINT_ID,
        FILE_PREVIEW_EXTENSION_POINT_VERSION,
        UI_EXTENSION_CONTEXT_SCHEMA_ID,
        UI_EXTENSION_PAYLOAD_SCHEMA_ID,
        UI_EXTENSION_RESOLVED_SCHEMA_ID,
        ExtensionCompositionPolicy::SelectOne,
    )
    .require_explicit_selection()
    .with_limits(32, 64 * 1024, MAX_UI_COMPONENT_ARTIFACT_BYTES, 2_000)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiContributionPayload {
    Declarative {
        schema_id: String,
        payload_sobj: Vec<u8>,
    },
    CustomWidget {
        widget: UiCustomWidgetContribution,
    },
}

impl UiContributionPayload {
    fn validate(&self, contribution_id: &str) -> Result<(), UiExtensionContractError> {
        match self {
            Self::Declarative {
                schema_id,
                payload_sobj,
            } => {
                validate_ui_token(schema_id, "declarative UI schema id")?;
                if payload_sobj.is_empty() {
                    return Err(UiExtensionContractError::InvalidContribution {
                        contribution_id: contribution_id.to_string(),
                        detail: "declarative UI payload must not be empty".to_string(),
                    });
                }
                Ok(())
            }
            Self::CustomWidget { widget } => {
                widget
                    .validate()
                    .map_err(|detail| UiExtensionContractError::InvalidContribution {
                        contribution_id: contribution_id.to_string(),
                        detail,
                    })
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiContextCondition {
    pub key: String,
    pub value: UiContextValue,
}

impl UiContextCondition {
    pub fn validate(&self) -> Result<(), UiExtensionContractError> {
        validate_ui_token(&self.key, "UI context key")?;
        if let UiContextValue::String(value) = &self.value
            && value.len() > 4_096
        {
            return Err(UiExtensionContractError::InvalidText {
                field: "UI context value".to_string(),
                detail: "must not exceed 4096 bytes".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum UiContextValue {
    Bool(bool),
    String(String),
    Integer(i64),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiCustomWidgetContribution {
    pub label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub component: UiComponentArtifact,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selectors: Vec<UiContentSelector>,
    #[serde(default)]
    pub sizing: UiWidgetSizing,
}

impl UiCustomWidgetContribution {
    pub fn new(label: impl Into<String>, component: UiComponentArtifact) -> Self {
        Self {
            label: label.into(),
            description: String::new(),
            component,
            selectors: Vec::new(),
            sizing: UiWidgetSizing::default(),
        }
    }

    pub fn with_selector(mut self, selector: UiContentSelector) -> Self {
        self.selectors.push(selector);
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.label.trim().is_empty() || self.label.len() > 256 {
            return Err("custom widget label must contain 1 to 256 bytes".to_string());
        }
        if self.description.len() > 4_096 {
            return Err("custom widget description must not exceed 4096 bytes".to_string());
        }
        self.component.validate()?;
        for selector in &self.selectors {
            selector.validate()?;
        }
        self.sizing.validate()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiComponentArtifact {
    pub artifact_id: String,
    pub content_hash: String,
    /// Normalized path inside the immutable module payload. The daemon resolves
    /// it only for local application clients and the isolated host verifies the
    /// content hash again before compilation.
    pub resource_path: String,
    pub runtime: UiComponentRuntime,
    pub protocol_id: String,
    pub protocol_version: String,
    pub entrypoint: String,
}

impl UiComponentArtifact {
    pub fn wasm(
        artifact_id: impl Into<String>,
        content_hash: impl Into<String>,
        resource_path: impl Into<String>,
        entrypoint: impl Into<String>,
    ) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            content_hash: content_hash.into(),
            resource_path: resource_path.into(),
            runtime: UiComponentRuntime::WasmComponent,
            protocol_id: UI_WIDGET_PROTOCOL_ID.to_string(),
            protocol_version: UI_WIDGET_PROTOCOL_VERSION.to_string(),
            entrypoint: entrypoint.into(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        validate_ui_token_string(&self.artifact_id, "UI component artifact id")?;
        validate_blake3_hash(&self.content_hash)?;
        validate_ui_resource_path(&self.resource_path)?;
        validate_ui_token_string(&self.protocol_id, "UI component protocol id")?;
        validate_ui_token_string(&self.protocol_version, "UI component protocol version")?;
        validate_ui_token_string(&self.entrypoint, "UI component entrypoint")?;
        if self.protocol_id != UI_WIDGET_PROTOCOL_ID {
            return Err(format!(
                "unsupported UI component protocol `{}`",
                self.protocol_id
            ));
        }
        if self.protocol_version != UI_WIDGET_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported UI component protocol version `{}`; expected `{UI_WIDGET_PROTOCOL_VERSION}`",
                self.protocol_version
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum UiComponentRuntime {
    WasmComponent,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiContentSelector {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_extensions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub magic_prefix: Vec<u8>,
}

impl UiContentSelector {
    pub fn extension(extension: impl Into<String>) -> Self {
        Self {
            file_extensions: vec![extension.into()],
            ..Self::default()
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.file_extensions.is_empty()
            && self.media_types.is_empty()
            && self.magic_prefix.is_empty()
        {
            return Err(
                "content selector must declare an extension, media type, or magic prefix"
                    .to_string(),
            );
        }
        let mut extensions = BTreeSet::new();
        for extension in &self.file_extensions {
            if !extension.starts_with('.')
                || extension.len() < 2
                || extension.len() > 64
                || !extension
                    .bytes()
                    .skip(1)
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(format!("invalid file extension selector `{extension}`"));
            }
            if !extensions.insert(extension.to_ascii_lowercase()) {
                return Err(format!("duplicate file extension selector `{extension}`"));
            }
        }
        let mut media_types = BTreeSet::new();
        for media_type in &self.media_types {
            if media_type.len() > 256
                || !media_type.contains('/')
                || !media_type.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'-' | b'.' | b'_')
                })
            {
                return Err(format!("invalid media type selector `{media_type}`"));
            }
            if !media_types.insert(media_type.to_ascii_lowercase()) {
                return Err(format!("duplicate media type selector `{media_type}`"));
            }
        }
        if self.magic_prefix.len() > 256 {
            return Err("content selector magic prefix must not exceed 256 bytes".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiWidgetSizing {
    pub minimum_width: u32,
    pub minimum_height: u32,
    pub preferred_width: u32,
    pub preferred_height: u32,
}

impl Default for UiWidgetSizing {
    fn default() -> Self {
        Self {
            minimum_width: 64,
            minimum_height: 64,
            preferred_width: 480,
            preferred_height: 320,
        }
    }
}

impl UiWidgetSizing {
    fn validate(&self) -> Result<(), String> {
        for (label, value) in [
            ("minimum width", self.minimum_width),
            ("minimum height", self.minimum_height),
            ("preferred width", self.preferred_width),
            ("preferred height", self.preferred_height),
        ] {
            if value > 16_384 {
                return Err(format!(
                    "UI widget {label} must be at most 16384 logical pixels"
                ));
            }
        }
        if self.preferred_width < self.minimum_width || self.preferred_height < self.minimum_height
        {
            return Err(
                "UI widget preferred size must not be smaller than its minimum size".to_string(),
            );
        }
        Ok(())
    }
}

/// Request envelope exchanged with one isolated UI component instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiWidgetRequest {
    pub instance_id: String,
    pub request_id: u64,
    pub message: UiWidgetRequestKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiWidgetRequestKind {
    Initialize {
        contribution_id: String,
        viewport: UiViewport,
        theme: UiThemeSnapshot,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<UiContentGrant>,
    },
    Event {
        event: UiInputEvent,
    },
    Render {
        viewport: UiViewport,
        theme: UiThemeSnapshot,
        monotonic_time_ms: u64,
    },
    HostResult {
        result: UiHostResult,
    },
    Close,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiWidgetResponse {
    pub instance_id: String,
    pub request_id: u64,
    pub result: UiWidgetResponseKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiWidgetResponseKind {
    Frame { frame: UiWidgetFrame },
    Closed,
    Error { code: String, detail: String },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiViewport {
    pub width: f32,
    pub height: f32,
    pub scale_factor: f32,
}

impl UiViewport {
    pub fn validate(self) -> Result<(), UiFrameValidationError> {
        for (field, value) in [
            ("viewport width", self.width),
            ("viewport height", self.height),
            ("viewport scale factor", self.scale_factor),
        ] {
            if !value.is_finite() || value <= 0.0 || value > 16_384.0 {
                return Err(UiFrameValidationError::InvalidGeometry {
                    detail: format!("{field} must be finite, positive, and at most 16384"),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiThemeSnapshot {
    pub mode: UiThemeMode,
    pub accent: UiColor,
    pub foreground: UiColor,
    pub muted_foreground: UiColor,
    pub background: UiColor,
    pub surface: UiColor,
    pub danger: UiColor,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum UiThemeMode {
    Light,
    Dark,
    HighContrast,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiContentGrant {
    pub grant_id: String,
    pub display_name: String,
    pub content_version: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiHostRequest {
    ReadContent {
        request_id: u64,
        grant_id: String,
        offset: u64,
        length: u64,
    },
    InvokeAction {
        request_id: u64,
        operation_ref: String,
        payload_sobj: Vec<u8>,
        user_gesture: bool,
    },
    RequestFrame {
        not_before_monotonic_ms: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiHostResult {
    Content {
        request_id: u64,
        offset: u64,
        bytes: Vec<u8>,
        eof: bool,
    },
    Action {
        request_id: u64,
        result_sobj: Vec<u8>,
    },
    Error {
        request_id: u64,
        code: String,
        detail: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiInputEvent {
    Pointer {
        phase: UiPointerPhase,
        pointer_id: u64,
        position: UiPoint,
        buttons: u32,
    },
    Scroll {
        delta_x: f32,
        delta_y: f32,
    },
    Key {
        key: String,
        pressed: bool,
        repeat: bool,
        modifiers: UiModifiers,
    },
    Text {
        text: String,
    },
    Focus {
        focused: bool,
    },
    SemanticsAction {
        node_id: String,
        action: UiSemanticsAction,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum UiPointerPhase {
    Down,
    Move,
    Up,
    Cancel,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiModifiers {
    pub shift: bool,
    pub control: bool,
    pub alt: bool,
    pub meta: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UiWidgetFrame {
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<UiImageResource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<UiPaintCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub semantics: Vec<UiSemanticsNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub host_requests: Vec<UiHostRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<UiCursor>,
}

impl UiWidgetFrame {
    pub fn validate(&self) -> Result<(), UiFrameValidationError> {
        if self.commands.len() > MAX_UI_FRAME_COMMANDS {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "paint commands",
                actual: self.commands.len() as u64,
                limit: MAX_UI_FRAME_COMMANDS as u64,
            });
        }
        if self.images.len() > MAX_UI_FRAME_IMAGES {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "images",
                actual: self.images.len() as u64,
                limit: MAX_UI_FRAME_IMAGES as u64,
            });
        }
        if self.semantics.len() > MAX_UI_FRAME_SEMANTICS_NODES {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "semantic nodes",
                actual: self.semantics.len() as u64,
                limit: MAX_UI_FRAME_SEMANTICS_NODES as u64,
            });
        }

        let mut image_bytes = 0usize;
        let mut image_ids = BTreeSet::new();
        for image in &self.images {
            image.validate()?;
            if !image_ids.insert(image.id.as_str()) {
                return Err(UiFrameValidationError::InvalidResource {
                    detail: format!("duplicate image id `{}`", image.id),
                });
            }
            image_bytes = image_bytes.saturating_add(image.rgba.len());
        }
        if image_bytes > MAX_UI_FRAME_IMAGE_BYTES {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "image bytes",
                actual: image_bytes as u64,
                limit: MAX_UI_FRAME_IMAGE_BYTES as u64,
            });
        }

        let mut stack = Vec::new();
        let mut path_elements = 0usize;
        let mut text_bytes = 0usize;
        for command in &self.commands {
            command.validate(&image_ids, &mut path_elements, &mut text_bytes)?;
            match command {
                UiPaintCommand::PushClipRect { .. } => stack.push("clip"),
                UiPaintCommand::PushTransform { .. } => stack.push("transform"),
                UiPaintCommand::PopClip => match stack.pop() {
                    Some("clip") => {}
                    _ => {
                        return Err(UiFrameValidationError::UnbalancedPaintStack {
                            detail: "PopClip does not match the latest push".to_string(),
                        });
                    }
                },
                UiPaintCommand::PopTransform => match stack.pop() {
                    Some("transform") => {}
                    _ => {
                        return Err(UiFrameValidationError::UnbalancedPaintStack {
                            detail: "PopTransform does not match the latest push".to_string(),
                        });
                    }
                },
                _ => {}
            }
            if stack.len() > MAX_UI_FRAME_STACK_DEPTH {
                return Err(UiFrameValidationError::LimitExceeded {
                    resource: "paint stack depth",
                    actual: stack.len() as u64,
                    limit: MAX_UI_FRAME_STACK_DEPTH as u64,
                });
            }
        }
        if !stack.is_empty() {
            return Err(UiFrameValidationError::UnbalancedPaintStack {
                detail: format!("{} paint stack entries remain", stack.len()),
            });
        }
        if path_elements > MAX_UI_FRAME_PATH_ELEMENTS {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "path elements",
                actual: path_elements as u64,
                limit: MAX_UI_FRAME_PATH_ELEMENTS as u64,
            });
        }
        if text_bytes > MAX_UI_FRAME_TEXT_BYTES {
            return Err(UiFrameValidationError::LimitExceeded {
                resource: "text bytes",
                actual: text_bytes as u64,
                limit: MAX_UI_FRAME_TEXT_BYTES as u64,
            });
        }

        let mut semantic_ids = BTreeSet::new();
        for node in &self.semantics {
            node.validate()?;
            if !semantic_ids.insert(node.id.as_str()) {
                return Err(UiFrameValidationError::InvalidSemantics {
                    detail: format!("duplicate semantic node id `{}`", node.id),
                });
            }
        }
        for node in &self.semantics {
            if let Some(parent_id) = node.parent_id.as_deref()
                && !semantic_ids.contains(parent_id)
            {
                return Err(UiFrameValidationError::InvalidSemantics {
                    detail: format!(
                        "semantic node `{}` references missing parent `{parent_id}`",
                        node.id
                    ),
                });
            }
        }
        let parents = self
            .semantics
            .iter()
            .map(|node| (node.id.as_str(), node.parent_id.as_deref()))
            .collect::<BTreeMap<_, _>>();
        for node in &self.semantics {
            let mut seen = BTreeSet::new();
            let mut current = Some(node.id.as_str());
            while let Some(id) = current {
                if !seen.insert(id) {
                    return Err(UiFrameValidationError::InvalidSemantics {
                        detail: format!("semantic parent cycle includes `{id}`"),
                    });
                }
                current = parents.get(id).copied().flatten();
            }
        }

        let mut host_request_ids = BTreeSet::new();
        for request in &self.host_requests {
            match request {
                UiHostRequest::ReadContent {
                    request_id,
                    grant_id,
                    length,
                    ..
                } => {
                    validate_ui_token_frame(grant_id, "content grant id")?;
                    if *length == 0 || *length > MAX_UI_CONTENT_READ_BYTES {
                        return Err(UiFrameValidationError::InvalidHostRequest {
                            detail: format!(
                                "content read length must be between 1 and {MAX_UI_CONTENT_READ_BYTES}"
                            ),
                        });
                    }
                    if !host_request_ids.insert(*request_id) {
                        return Err(UiFrameValidationError::InvalidHostRequest {
                            detail: format!("duplicate host request id `{request_id}`"),
                        });
                    }
                }
                UiHostRequest::InvokeAction {
                    request_id,
                    operation_ref,
                    payload_sobj,
                    ..
                } => {
                    validate_ui_token_frame(operation_ref, "logical operation reference")?;
                    if payload_sobj.len() > MAX_UI_CONTENT_READ_BYTES as usize {
                        return Err(UiFrameValidationError::LimitExceeded {
                            resource: "action payload bytes",
                            actual: payload_sobj.len() as u64,
                            limit: MAX_UI_CONTENT_READ_BYTES,
                        });
                    }
                    if !host_request_ids.insert(*request_id) {
                        return Err(UiFrameValidationError::InvalidHostRequest {
                            detail: format!("duplicate host request id `{request_id}`"),
                        });
                    }
                }
                UiHostRequest::RequestFrame { .. } => {}
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiImageResource {
    pub id: String,
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

impl UiImageResource {
    fn validate(&self) -> Result<(), UiFrameValidationError> {
        validate_ui_token_frame(&self.id, "image id")?;
        if self.width == 0 || self.height == 0 || self.width > 16_384 || self.height > 16_384 {
            return Err(UiFrameValidationError::InvalidResource {
                detail: format!("image `{}` dimensions must be between 1 and 16384", self.id),
            });
        }
        let expected = u64::from(self.width)
            .checked_mul(u64::from(self.height))
            .and_then(|pixels| pixels.checked_mul(4))
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| UiFrameValidationError::InvalidResource {
                detail: format!("image `{}` dimensions overflow", self.id),
            })?;
        if self.rgba.len() != expected {
            return Err(UiFrameValidationError::InvalidResource {
                detail: format!(
                    "image `{}` has {} RGBA bytes; expected {expected}",
                    self.id,
                    self.rgba.len()
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiPaintCommand {
    Clear {
        color: UiColor,
    },
    FillRect {
        rect: UiRect,
        color: UiColor,
    },
    StrokeRect {
        rect: UiRect,
        color: UiColor,
        width: f32,
    },
    FillPath {
        path: UiPath,
        color: UiColor,
    },
    StrokePath {
        path: UiPath,
        color: UiColor,
        width: f32,
    },
    DrawText {
        rect: UiRect,
        text: String,
        style: UiTextStyle,
    },
    DrawImage {
        rect: UiRect,
        image_id: String,
    },
    PushClipRect {
        rect: UiRect,
    },
    PopClip,
    PushTransform {
        transform: UiTransform,
    },
    PopTransform,
}

impl UiPaintCommand {
    fn validate(
        &self,
        image_ids: &BTreeSet<&str>,
        path_elements: &mut usize,
        text_bytes: &mut usize,
    ) -> Result<(), UiFrameValidationError> {
        match self {
            Self::Clear { .. } => Ok(()),
            Self::FillRect { rect, .. }
            | Self::PushClipRect { rect }
            | Self::DrawImage { rect, .. } => {
                rect.validate()?;
                if let Self::DrawImage { image_id, .. } = self
                    && !image_ids.contains(image_id.as_str())
                {
                    return Err(UiFrameValidationError::InvalidResource {
                        detail: format!("paint command references missing image `{image_id}`"),
                    });
                }
                Ok(())
            }
            Self::StrokeRect { rect, width, .. } => {
                rect.validate()?;
                validate_stroke_width(*width)
            }
            Self::FillPath { path, .. } => {
                path.validate()?;
                *path_elements = path_elements.saturating_add(path.elements.len());
                Ok(())
            }
            Self::StrokePath { path, width, .. } => {
                path.validate()?;
                validate_stroke_width(*width)?;
                *path_elements = path_elements.saturating_add(path.elements.len());
                Ok(())
            }
            Self::DrawText { rect, text, style } => {
                rect.validate()?;
                style.validate()?;
                *text_bytes = text_bytes.saturating_add(text.len());
                Ok(())
            }
            Self::PushTransform { transform } => transform.validate(),
            Self::PopClip | Self::PopTransform => Ok(()),
        }
    }
}

fn validate_stroke_width(width: f32) -> Result<(), UiFrameValidationError> {
    if !width.is_finite() || width <= 0.0 || width > 1024.0 {
        return Err(UiFrameValidationError::InvalidGeometry {
            detail: "stroke width must be finite, positive, and at most 1024".to_string(),
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UiPoint {
    pub x: f32,
    pub y: f32,
}

impl UiPoint {
    fn validate(self) -> Result<(), UiFrameValidationError> {
        if !self.x.is_finite() || !self.y.is_finite() {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "point coordinates must be finite".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UiRect {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

impl UiRect {
    fn validate(self) -> Result<(), UiFrameValidationError> {
        UiPoint {
            x: self.x,
            y: self.y,
        }
        .validate()?;
        if !self.width.is_finite()
            || !self.height.is_finite()
            || self.width < 0.0
            || self.height < 0.0
            || self.width > 1_000_000.0
            || self.height > 1_000_000.0
        {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "rectangle dimensions must be finite, non-negative, and bounded"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiColor {
    pub red: u8,
    pub green: u8,
    pub blue: u8,
    pub alpha: u8,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiTransform {
    pub matrix: [f32; 6],
}

impl UiTransform {
    fn validate(self) -> Result<(), UiFrameValidationError> {
        if self.matrix.iter().any(|value| !value.is_finite()) {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "transform entries must be finite".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UiPath {
    pub elements: Vec<UiPathElement>,
}

impl UiPath {
    fn validate(&self) -> Result<(), UiFrameValidationError> {
        if self.elements.is_empty() {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "path must contain at least one element".to_string(),
            });
        }
        for element in &self.elements {
            match element {
                UiPathElement::MoveTo { point } | UiPathElement::LineTo { point } => {
                    point.validate()?
                }
                UiPathElement::QuadraticTo { control, point } => {
                    control.validate()?;
                    point.validate()?;
                }
                UiPathElement::CubicTo {
                    control1,
                    control2,
                    point,
                } => {
                    control1.validate()?;
                    control2.validate()?;
                    point.validate()?;
                }
                UiPathElement::Close => {}
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UiPathElement {
    MoveTo {
        point: UiPoint,
    },
    LineTo {
        point: UiPoint,
    },
    QuadraticTo {
        control: UiPoint,
        point: UiPoint,
    },
    CubicTo {
        control1: UiPoint,
        control2: UiPoint,
        point: UiPoint,
    },
    Close,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiTextStyle {
    pub color: UiColor,
    pub font_size: f32,
    #[serde(default = "default_font_weight")]
    pub font_weight: u16,
    #[serde(default)]
    pub monospace: bool,
}

fn default_font_weight() -> u16 {
    400
}

impl UiTextStyle {
    fn validate(&self) -> Result<(), UiFrameValidationError> {
        if !self.font_size.is_finite() || !(1.0..=512.0).contains(&self.font_size) {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "font size must be finite and between 1 and 512".to_string(),
            });
        }
        if !(1..=1_000).contains(&self.font_weight) {
            return Err(UiFrameValidationError::InvalidGeometry {
                detail: "font weight must be between 1 and 1000".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UiSemanticsNode {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub role: UiSemanticsRole,
    pub bounds: UiRect,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<UiSemanticsAction>,
}

impl UiSemanticsNode {
    fn validate(&self) -> Result<(), UiFrameValidationError> {
        validate_ui_token_frame(&self.id, "semantic node id")?;
        if let Some(parent_id) = self.parent_id.as_deref() {
            validate_ui_token_frame(parent_id, "semantic parent id")?;
            if parent_id == self.id {
                return Err(UiFrameValidationError::InvalidSemantics {
                    detail: format!("semantic node `{}` cannot parent itself", self.id),
                });
            }
        }
        self.bounds.validate()?;
        for (field, value) in [("name", &self.name), ("value", &self.value)] {
            if value.as_ref().is_some_and(|value| value.len() > 65_536) {
                return Err(UiFrameValidationError::InvalidSemantics {
                    detail: format!("semantic node {field} must not exceed 65536 bytes"),
                });
            }
        }
        let mut actions = BTreeSet::new();
        if self.actions.iter().any(|action| !actions.insert(action)) {
            return Err(UiFrameValidationError::InvalidSemantics {
                detail: format!("semantic node `{}` contains duplicate actions", self.id),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum UiSemanticsRole {
    GenericContainer,
    Canvas,
    Image,
    Text,
    Button,
    Link,
    Checkbox,
    Slider,
    List,
    ListItem,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum UiSemanticsAction {
    Focus,
    Activate,
    Increment,
    Decrement,
    ScrollForward,
    ScrollBackward,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum UiCursor {
    Default,
    Pointer,
    Crosshair,
    Text,
    Grab,
    Grabbing,
    ZoomIn,
    ZoomOut,
}

pub fn validate_ui_contributions(
    contributions: &[UiExtensionContribution],
) -> Result<(), UiExtensionContractError> {
    if contributions.len() > MAX_UI_CONTRIBUTIONS_PER_MODULE {
        return Err(UiExtensionContractError::TooManyContributions {
            actual: contributions.len(),
            limit: MAX_UI_CONTRIBUTIONS_PER_MODULE,
        });
    }
    let mut ids = BTreeSet::new();
    for contribution in contributions {
        contribution.validate()?;
        if !ids.insert(contribution.contribution_id.as_str()) {
            return Err(UiExtensionContractError::DuplicateContributionId {
                contribution_id: contribution.contribution_id.clone(),
            });
        }
    }
    Ok(())
}

/// Deterministically orders contributions for one `many-ordered` point.
/// Unknown relative-order targets are ignored; cycles are rejected so callers
/// can diagnose the affected point without depending on activation order.
pub fn order_ui_contributions(
    contributions: &[UiExtensionContribution],
) -> Result<Vec<usize>, UiExtensionContractError> {
    validate_ui_contributions(contributions)?;
    let by_id = contributions
        .iter()
        .enumerate()
        .map(|(index, contribution)| (contribution.contribution_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = vec![BTreeSet::<usize>::new(); contributions.len()];
    let mut indegree = vec![0usize; contributions.len()];
    for (index, contribution) in contributions.iter().enumerate() {
        for before in &contribution.before {
            if let Some(&target) = by_id.get(before.as_str())
                && outgoing[index].insert(target)
            {
                indegree[target] += 1;
            }
        }
        for after in &contribution.after {
            if let Some(&source) = by_id.get(after.as_str())
                && outgoing[source].insert(index)
            {
                indegree[index] += 1;
            }
        }
    }

    let sort_key = |index: usize| {
        let contribution = &contributions[index];
        (
            contribution.group.as_str(),
            contribution.order,
            contribution.contribution_id.as_str(),
        )
    };
    let mut ready = (0..contributions.len())
        .filter(|index| indegree[*index] == 0)
        .collect::<Vec<_>>();
    let mut ordered = Vec::with_capacity(contributions.len());
    while !ready.is_empty() {
        ready.sort_by(|left, right| sort_key(*right).cmp(&sort_key(*left)));
        let index = ready.pop().expect("non-empty ready set");
        ordered.push(index);
        for target in outgoing[index].iter().copied() {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }
    if ordered.len() != contributions.len() {
        let cyclic = indegree
            .iter()
            .enumerate()
            .filter(|(_, degree)| **degree > 0)
            .map(|(index, _)| contributions[index].contribution_id.clone())
            .collect();
        return Err(UiExtensionContractError::CyclicOrdering {
            contributions: cyclic,
        });
    }
    Ok(ordered)
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UiExtensionContractError {
    #[error("invalid {field}: {detail}")]
    InvalidText { field: String, detail: String },
    #[error("invalid UI contribution {contribution_id}: {detail}")]
    InvalidContribution {
        contribution_id: String,
        detail: String,
    },
    #[error("duplicate UI contribution id `{contribution_id}`")]
    DuplicateContributionId { contribution_id: String },
    #[error("module declares {actual} UI contributions; limit is {limit}")]
    TooManyContributions { actual: usize, limit: usize },
    #[error("cyclic UI contribution ordering among {contributions:?}")]
    CyclicOrdering { contributions: Vec<String> },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UiFrameValidationError {
    #[error("UI frame {resource} limit exceeded: actual {actual}, limit {limit}")]
    LimitExceeded {
        resource: &'static str,
        actual: u64,
        limit: u64,
    },
    #[error("invalid UI frame geometry: {detail}")]
    InvalidGeometry { detail: String },
    #[error("invalid UI frame resource: {detail}")]
    InvalidResource { detail: String },
    #[error("unbalanced UI frame paint stack: {detail}")]
    UnbalancedPaintStack { detail: String },
    #[error("invalid UI frame semantics: {detail}")]
    InvalidSemantics { detail: String },
    #[error("invalid UI host request: {detail}")]
    InvalidHostRequest { detail: String },
}

fn validate_unique_tokens(values: &[String], field: &str) -> Result<(), UiExtensionContractError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_ui_token(value, field)?;
        if !seen.insert(value.as_str()) {
            return Err(UiExtensionContractError::InvalidText {
                field: field.to_string(),
                detail: format!("duplicate value `{value}`"),
            });
        }
    }
    Ok(())
}

fn validate_ui_token(value: &str, field: &str) -> Result<(), UiExtensionContractError> {
    validate_ui_token_string(value, field).map_err(|detail| UiExtensionContractError::InvalidText {
        field: field.to_string(),
        detail,
    })
}

fn validate_ui_token_string(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 256 {
        return Err(format!("{field} must contain 1 to 256 bytes"));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':' | b'@')
    }) {
        return Err(format!("{field} contains unsupported characters"));
    }
    Ok(())
}

fn validate_ui_token_frame(value: &str, field: &str) -> Result<(), UiFrameValidationError> {
    validate_ui_token_string(value, field)
        .map_err(|detail| UiFrameValidationError::InvalidResource { detail })
}

fn validate_blake3_hash(value: &str) -> Result<(), String> {
    let Some(hex) = value.strip_prefix("blake3:") else {
        return Err("UI component content hash must use the blake3: prefix".to_string());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(
            "UI component content hash must contain 64 lowercase hexadecimal digits".to_string(),
        );
    }
    Ok(())
}

fn validate_ui_resource_path(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(
            "UI component resource path must be a normalized relative slash path".to_string(),
        );
    }
    if value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(
            "UI component resource path is too long or contains control characters".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact() -> UiComponentArtifact {
        UiComponentArtifact::wasm(
            "artifact.preview.example",
            format!("blake3:{}", "0".repeat(64)),
            "ui/preview.wasm",
            "sinomo:ui-widget/widget",
        )
    }

    fn contribution(id: &str, order: i32) -> UiExtensionContribution {
        UiExtensionContribution::custom_widget(
            id,
            FILE_PREVIEW_EXTENSION_POINT_ID,
            FILE_PREVIEW_EXTENSION_POINT_VERSION,
            UiCustomWidgetContribution::new("Example preview", artifact())
                .with_selector(UiContentSelector::extension(".example")),
        )
        .with_order("preview", order)
    }

    #[test]
    fn custom_widget_contribution_roundtrips_through_sobj() {
        let contribution = contribution("module.example.preview", 10);
        contribution.validate().expect("valid contribution");
        let bytes = crate::encode_sobj(&contribution).expect("encode contribution");
        let decoded: UiExtensionContribution =
            crate::decode_sobj(&bytes).expect("decode contribution");
        assert_eq!(decoded, contribution);
        assert!(UI_WIDGET_WIT.contains("handle: func(request-sobj: list<u8>) -> list<u8>"));
    }

    #[test]
    fn ordering_is_deterministic_and_honors_relative_constraints() {
        let first = contribution("module.example.first", 20);
        let mut second = contribution("module.example.second", 10);
        second.after.push(first.contribution_id.clone());
        let third = contribution("module.example.third", 5);
        let contributions = vec![second, third, first];
        let order = order_ui_contributions(&contributions).expect("order contributions");
        let ids = order
            .into_iter()
            .map(|index| contributions[index].contribution_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                "module.example.third",
                "module.example.first",
                "module.example.second"
            ]
        );
    }

    #[test]
    fn cyclic_relative_order_is_rejected() {
        let mut first = contribution("module.example.first", 0);
        let mut second = contribution("module.example.second", 0);
        first.before.push(second.contribution_id.clone());
        second.before.push(first.contribution_id.clone());
        assert!(matches!(
            order_ui_contributions(&[first, second]),
            Err(UiExtensionContractError::CyclicOrdering { .. })
        ));
    }

    #[test]
    fn widget_frame_validates_resources_stacks_and_semantics() {
        let frame = UiWidgetFrame {
            generation: 1,
            commands: vec![
                UiPaintCommand::PushClipRect {
                    rect: UiRect {
                        width: 320.0,
                        height: 200.0,
                        ..UiRect::default()
                    },
                },
                UiPaintCommand::DrawText {
                    rect: UiRect {
                        x: 8.0,
                        y: 8.0,
                        width: 200.0,
                        height: 32.0,
                    },
                    text: "Custom preview".to_string(),
                    style: UiTextStyle {
                        color: UiColor {
                            red: 255,
                            green: 255,
                            blue: 255,
                            alpha: 255,
                        },
                        font_size: 14.0,
                        font_weight: 500,
                        monospace: false,
                    },
                },
                UiPaintCommand::PopClip,
            ],
            semantics: vec![UiSemanticsNode {
                id: "preview.root".to_string(),
                parent_id: None,
                role: UiSemanticsRole::Canvas,
                bounds: UiRect {
                    width: 320.0,
                    height: 200.0,
                    ..UiRect::default()
                },
                name: Some("Custom preview".to_string()),
                value: None,
                actions: vec![UiSemanticsAction::Focus],
            }],
            ..UiWidgetFrame::default()
        };
        frame.validate().expect("valid frame");

        let mut invalid = frame;
        invalid.commands.pop();
        assert!(matches!(
            invalid.validate(),
            Err(UiFrameValidationError::UnbalancedPaintStack { .. })
        ));
    }

    #[test]
    fn content_reads_are_individually_bounded() {
        let frame = UiWidgetFrame {
            host_requests: vec![UiHostRequest::ReadContent {
                request_id: 7,
                grant_id: "grant.preview".to_string(),
                offset: 0,
                length: MAX_UI_CONTENT_READ_BYTES + 1,
            }],
            ..UiWidgetFrame::default()
        };
        assert!(matches!(
            frame.validate(),
            Err(UiFrameValidationError::InvalidHostRequest { .. })
        ));
    }

    #[test]
    fn semantic_parent_cycles_are_rejected() {
        let node = |id: &str, parent_id: &str| UiSemanticsNode {
            id: id.to_string(),
            parent_id: Some(parent_id.to_string()),
            role: UiSemanticsRole::GenericContainer,
            bounds: UiRect {
                width: 10.0,
                height: 10.0,
                ..UiRect::default()
            },
            name: None,
            value: None,
            actions: Vec::new(),
        };
        let frame = UiWidgetFrame {
            semantics: vec![node("first", "second"), node("second", "first")],
            ..UiWidgetFrame::default()
        };
        assert!(matches!(
            frame.validate(),
            Err(UiFrameValidationError::InvalidSemantics { ref detail })
                if detail.contains("cycle")
        ));
    }
}
