//! Backend-neutral contracts for typed module extension points.
//!
//! An extension point is owned by the module or host subsystem whose behavior
//! changes. Contributors provide bounded data and/or a reference to one of
//! their own logical interface operations. No raw route, callback, Rust type,
//! or authority token crosses this catalog boundary.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const EXTENSION_VETO_SCHEMA_ID: &str = "sinomo.extension.veto/v1";
pub const MAX_EXTENSION_POINTS_PER_MODULE: usize = 256;
pub const MAX_EXTENSION_CONTRIBUTIONS_PER_MODULE: usize = 512;
pub const MAX_EXTENSION_CONTEXT_CONDITIONS: usize = 64;
pub const MAX_EXTENSION_ORDER_REFERENCES: usize = 128;
pub const MAX_EXTENSION_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionCompositionPolicy {
    ManyOrdered,
    Collect,
    Reduce,
    Pipeline,
    SelectOne,
    Veto,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionLocality {
    #[default]
    Local,
    Cluster,
    Any,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionContributionKind {
    Declarative,
    Operation,
    Hybrid,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionFailurePolicy {
    /// Keep independent successes and attribute failures to their contributors.
    Partial,
    /// Omit a failed optional contribution but retain the point result.
    #[default]
    OmitFailed,
    /// Fail the extension-point invocation without authorizing an owner effect.
    FailPoint,
    /// Convert malformed output, timeout, disappearance, or provider failure
    /// into a constraining result. Required for veto points.
    FailClosed,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionSelectionPolicy {
    #[default]
    None,
    Explicit,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionPointDefinition {
    pub point_id: String,
    pub point_version: String,
    pub input_schema_id: String,
    pub contribution_schema_id: String,
    pub output_schema_id: String,
    pub composition: ExtensionCompositionPolicy,
    #[serde(default)]
    pub locality: ExtensionLocality,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_contribution_kinds: Vec<ExtensionContributionKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_contract: Option<ExtensionOperationContract>,
    #[serde(default)]
    pub failure_policy: ExtensionFailurePolicy,
    #[serde(default)]
    pub selection_policy: ExtensionSelectionPolicy,
    pub max_contributions: u32,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_deadline_ms: u64,
    #[serde(default)]
    pub requires_cancellation: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributor_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub apply_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_context_keys: Vec<String>,
}

impl ExtensionPointDefinition {
    pub fn new(
        point_id: impl Into<String>,
        point_version: impl Into<String>,
        input_schema_id: impl Into<String>,
        contribution_schema_id: impl Into<String>,
        output_schema_id: impl Into<String>,
        composition: ExtensionCompositionPolicy,
    ) -> Self {
        Self {
            point_id: point_id.into(),
            point_version: point_version.into(),
            input_schema_id: input_schema_id.into(),
            contribution_schema_id: contribution_schema_id.into(),
            output_schema_id: output_schema_id.into(),
            composition,
            locality: ExtensionLocality::Local,
            allowed_contribution_kinds: vec![ExtensionContributionKind::Declarative],
            operation_contract: None,
            failure_policy: ExtensionFailurePolicy::OmitFailed,
            selection_policy: ExtensionSelectionPolicy::None,
            max_contributions: 32,
            max_request_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_deadline_ms: 1_000,
            requires_cancellation: false,
            contributor_capabilities: Vec::new(),
            apply_capabilities: Vec::new(),
            allowed_context_keys: Vec::new(),
        }
    }

    pub fn allow_kind(mut self, kind: ExtensionContributionKind) -> Self {
        if !self.allowed_contribution_kinds.contains(&kind) {
            self.allowed_contribution_kinds.push(kind);
        }
        self
    }

    pub fn operation_backed(mut self) -> Self {
        self.allowed_contribution_kinds = vec![ExtensionContributionKind::Operation];
        self
    }

    pub fn with_operation_contract(
        mut self,
        interface_id: impl Into<String>,
        interface_version: impl Into<String>,
        logical_operation: impl Into<String>,
    ) -> Self {
        self.operation_contract = Some(ExtensionOperationContract {
            interface_id: interface_id.into(),
            interface_version: interface_version.into(),
            logical_operation: logical_operation.into(),
        });
        self
    }

    pub fn with_failure_policy(mut self, policy: ExtensionFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    pub fn require_explicit_selection(mut self) -> Self {
        self.selection_policy = ExtensionSelectionPolicy::Explicit;
        self
    }

    pub fn with_limits(
        mut self,
        max_contributions: u32,
        max_request_bytes: u64,
        max_response_bytes: u64,
        max_deadline_ms: u64,
    ) -> Self {
        self.max_contributions = max_contributions;
        self.max_request_bytes = max_request_bytes;
        self.max_response_bytes = max_response_bytes;
        self.max_deadline_ms = max_deadline_ms;
        self
    }

    pub fn require_cancellation(mut self) -> Self {
        self.requires_cancellation = true;
        self
    }

    pub fn with_contributor_capability(mut self, capability: impl Into<String>) -> Self {
        self.contributor_capabilities.push(capability.into());
        self
    }

    pub fn with_apply_capability(mut self, capability: impl Into<String>) -> Self {
        self.apply_capabilities.push(capability.into());
        self
    }

    pub fn with_context_key(mut self, key: impl Into<String>) -> Self {
        self.allowed_context_keys.push(key.into());
        self
    }

    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.point_id, "extension point id")?;
        parse_extension_version(&self.point_version)?;
        validate_extension_token(&self.input_schema_id, "extension input schema id")?;
        validate_extension_token(
            &self.contribution_schema_id,
            "extension contribution schema id",
        )?;
        validate_extension_token(&self.output_schema_id, "extension output schema id")?;
        if self.allowed_contribution_kinds.is_empty() {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: self.point_id.clone(),
                detail: "at least one contribution kind is required".to_string(),
            });
        }
        validate_unique_copy_values(
            &self.allowed_contribution_kinds,
            &self.point_id,
            "contribution kind",
        )?;
        let accepts_operation = self.allowed_contribution_kinds.iter().any(|kind| {
            matches!(
                kind,
                ExtensionContributionKind::Operation | ExtensionContributionKind::Hybrid
            )
        });
        match (&self.operation_contract, accepts_operation) {
            (Some(contract), true) => contract.validate()?,
            (None, true) => {
                return Err(ExtensionContractError::InvalidPoint {
                    point_id: self.point_id.clone(),
                    detail: "operation-backed points require an operation interface contract"
                        .to_string(),
                });
            }
            (Some(_), false) => {
                return Err(ExtensionContractError::InvalidPoint {
                    point_id: self.point_id.clone(),
                    detail: "declarative-only points cannot declare an operation contract"
                        .to_string(),
                });
            }
            (None, false) => {}
        }
        if self.max_contributions == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_deadline_ms == 0
        {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: self.point_id.clone(),
                detail: "contribution, request, response, and deadline limits must be non-zero"
                    .to_string(),
            });
        }
        if self.composition == ExtensionCompositionPolicy::SelectOne
            && self.selection_policy != ExtensionSelectionPolicy::Explicit
        {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: self.point_id.clone(),
                detail: "select-one points require explicit selection".to_string(),
            });
        }
        if self.composition != ExtensionCompositionPolicy::SelectOne
            && self.selection_policy != ExtensionSelectionPolicy::None
        {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: self.point_id.clone(),
                detail: "only select-one points may declare selection policy".to_string(),
            });
        }
        if self.composition == ExtensionCompositionPolicy::Veto
            && self.failure_policy != ExtensionFailurePolicy::FailClosed
        {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: self.point_id.clone(),
                detail: "veto points must fail closed".to_string(),
            });
        }
        validate_unique_extension_tokens(
            &self.contributor_capabilities,
            &self.point_id,
            "contributor capability",
        )?;
        validate_unique_extension_tokens(
            &self.apply_capabilities,
            &self.point_id,
            "apply capability",
        )?;
        validate_unique_extension_tokens(&self.allowed_context_keys, &self.point_id, "context key")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionOperationContract {
    pub interface_id: String,
    pub interface_version: String,
    pub logical_operation: String,
}

impl ExtensionOperationContract {
    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.interface_id, "extension operation interface id")?;
        validate_extension_token(
            &self.interface_version,
            "extension operation interface version",
        )?;
        validate_extension_token(&self.logical_operation, "extension logical operation")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ExtensionVersionRequirement {
    Exact {
        version: String,
    },
    CompatibleMajor {
        major: u64,
        #[serde(default)]
        minimum_minor: u64,
        #[serde(default)]
        minimum_patch: u64,
    },
    Any,
}

impl ExtensionVersionRequirement {
    pub fn exact(version: impl Into<String>) -> Self {
        Self::Exact {
            version: version.into(),
        }
    }

    pub fn matches(&self, version: &str) -> bool {
        let Ok(actual) = parse_extension_version(version) else {
            return false;
        };
        match self {
            Self::Exact { version } => parse_extension_version(version) == Ok(actual),
            Self::CompatibleMajor {
                major,
                minimum_minor,
                minimum_patch,
            } => actual.0 == *major && (actual.1, actual.2) >= (*minimum_minor, *minimum_patch),
            Self::Any => true,
        }
    }

    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        match self {
            Self::Exact { version } => parse_extension_version(version).map(|_| ()),
            Self::CompatibleMajor { .. } | Self::Any => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionOperationReference {
    pub export_id: String,
    pub logical_operation: String,
}

impl ExtensionOperationReference {
    pub fn new(export_id: impl Into<String>, logical_operation: impl Into<String>) -> Self {
        Self {
            export_id: export_id.into(),
            logical_operation: logical_operation.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.export_id, "extension operation export id")?;
        validate_extension_token(&self.logical_operation, "extension logical operation")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionContribution {
    pub contribution_id: String,
    pub point_id: String,
    pub point_version: ExtensionVersionRequirement,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default)]
    pub order: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub when: Vec<ExtensionContextCondition>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_schema_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub payload_sobj: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<ExtensionOperationReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
}

impl ExtensionContribution {
    pub fn declarative(
        contribution_id: impl Into<String>,
        point_id: impl Into<String>,
        point_version: ExtensionVersionRequirement,
        payload_schema_id: impl Into<String>,
        payload_sobj: Vec<u8>,
    ) -> Self {
        Self {
            contribution_id: contribution_id.into(),
            point_id: point_id.into(),
            point_version,
            group: String::new(),
            order: 0,
            before: Vec::new(),
            after: Vec::new(),
            when: Vec::new(),
            payload_schema_id: payload_schema_id.into(),
            payload_sobj,
            operation: None,
            required_capabilities: Vec::new(),
        }
    }

    pub fn operation(
        contribution_id: impl Into<String>,
        point_id: impl Into<String>,
        point_version: ExtensionVersionRequirement,
        operation: ExtensionOperationReference,
    ) -> Self {
        Self {
            contribution_id: contribution_id.into(),
            point_id: point_id.into(),
            point_version,
            group: String::new(),
            order: 0,
            before: Vec::new(),
            after: Vec::new(),
            when: Vec::new(),
            payload_schema_id: String::new(),
            payload_sobj: Vec::new(),
            operation: Some(operation),
            required_capabilities: Vec::new(),
        }
    }

    pub fn with_order(mut self, group: impl Into<String>, order: i32) -> Self {
        self.group = group.into();
        self.order = order;
        self
    }

    pub fn kind(&self) -> Option<ExtensionContributionKind> {
        match (!self.payload_sobj.is_empty(), self.operation.is_some()) {
            (true, false) => Some(ExtensionContributionKind::Declarative),
            (false, true) => Some(ExtensionContributionKind::Operation),
            (true, true) => Some(ExtensionContributionKind::Hybrid),
            (false, false) => None,
        }
    }

    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.contribution_id, "extension contribution id")?;
        validate_extension_token(&self.point_id, "target extension point id")?;
        self.point_version.validate()?;
        if !self.group.is_empty() {
            validate_extension_token(&self.group, "extension contribution group")?;
        }
        validate_order_references(&self.contribution_id, &self.before, "before contribution")?;
        validate_order_references(&self.contribution_id, &self.after, "after contribution")?;
        if self.before.iter().any(|id| id == &self.contribution_id)
            || self.after.iter().any(|id| id == &self.contribution_id)
        {
            return Err(ExtensionContractError::InvalidContribution {
                contribution_id: self.contribution_id.clone(),
                detail: "a contribution cannot order itself relative to itself".to_string(),
            });
        }
        if self.when.len() > MAX_EXTENSION_CONTEXT_CONDITIONS {
            return Err(ExtensionContractError::InvalidContribution {
                contribution_id: self.contribution_id.clone(),
                detail: format!(
                    "{} context conditions exceed the limit of {MAX_EXTENSION_CONTEXT_CONDITIONS}",
                    self.when.len()
                ),
            });
        }
        let mut context_keys = BTreeSet::new();
        for condition in &self.when {
            condition.validate()?;
            if !context_keys.insert(condition.key.as_str()) {
                return Err(ExtensionContractError::InvalidContribution {
                    contribution_id: self.contribution_id.clone(),
                    detail: format!("duplicate context condition `{}`", condition.key),
                });
            }
        }
        if !self.payload_sobj.is_empty() {
            validate_extension_token(&self.payload_schema_id, "extension payload schema id")?;
            if self.payload_sobj.len() > MAX_EXTENSION_PAYLOAD_BYTES {
                return Err(ExtensionContractError::InvalidContribution {
                    contribution_id: self.contribution_id.clone(),
                    detail: format!(
                        "payload is {} bytes; limit is {MAX_EXTENSION_PAYLOAD_BYTES}",
                        self.payload_sobj.len()
                    ),
                });
            }
        } else if !self.payload_schema_id.is_empty() {
            return Err(ExtensionContractError::InvalidContribution {
                contribution_id: self.contribution_id.clone(),
                detail: "payload schema is present without a payload".to_string(),
            });
        }
        if let Some(operation) = &self.operation {
            operation.validate()?;
        }
        if self.kind().is_none() {
            return Err(ExtensionContractError::InvalidContribution {
                contribution_id: self.contribution_id.clone(),
                detail: "a declarative payload, logical operation, or both are required"
                    .to_string(),
            });
        }
        validate_unique_extension_tokens(
            &self.required_capabilities,
            &self.contribution_id,
            "required capability",
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionContextCondition {
    pub key: String,
    pub value: ExtensionContextValue,
}

impl ExtensionContextCondition {
    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.key, "extension context key")?;
        if let ExtensionContextValue::String(value) = &self.value
            && value.len() > 4_096
        {
            return Err(ExtensionContractError::InvalidText {
                field: "extension context value".to_string(),
                detail: "must not exceed 4096 bytes".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum ExtensionContextValue {
    Bool(bool),
    String(String),
    Integer(i64),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionContext {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<ExtensionContextCondition>,
}

impl ExtensionContext {
    pub fn with(mut self, key: impl Into<String>, value: ExtensionContextValue) -> Self {
        let key = key.into();
        if let Some(existing) = self.values.iter_mut().find(|entry| entry.key == key) {
            existing.value = value;
        } else {
            self.values.push(ExtensionContextCondition { key, value });
        }
        self
    }

    pub fn matches(&self, conditions: &[ExtensionContextCondition]) -> bool {
        conditions.iter().all(|condition| {
            self.values
                .iter()
                .any(|value| value.key == condition.key && value.value == condition.value)
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionSelection {
    pub point_id: String,
    pub point_version: String,
    pub contribution_id: String,
}

impl ExtensionSelection {
    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        validate_extension_token(&self.point_id, "selected extension point id")?;
        parse_extension_version(&self.point_version)?;
        validate_extension_token(&self.contribution_id, "selected extension contribution id")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionVeto {
    pub denied: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

impl ExtensionVeto {
    pub fn allow() -> Self {
        Self {
            denied: false,
            code: String::new(),
            detail: String::new(),
            evidence_refs: Vec::new(),
        }
    }

    pub fn deny(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            denied: true,
            code: code.into(),
            detail: detail.into(),
            evidence_refs: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), ExtensionContractError> {
        if self.denied {
            validate_extension_token(&self.code, "extension veto code")?;
            if self.detail.trim().is_empty() || self.detail.len() > 16_384 {
                return Err(ExtensionContractError::InvalidVeto {
                    detail: "a denial requires bounded non-empty detail".to_string(),
                });
            }
        } else if !self.code.is_empty() || !self.detail.is_empty() || !self.evidence_refs.is_empty()
        {
            return Err(ExtensionContractError::InvalidVeto {
                detail: "an allow result cannot carry denial data".to_string(),
            });
        }
        if self.evidence_refs.len() > 128
            || self
                .evidence_refs
                .iter()
                .any(|value| value.is_empty() || value.len() > 4_096)
        {
            return Err(ExtensionContractError::InvalidVeto {
                detail: "veto evidence references are invalid or exceed their bound".to_string(),
            });
        }
        Ok(())
    }
}

pub fn validate_extension_points(
    definitions: &[ExtensionPointDefinition],
) -> Result<(), ExtensionContractError> {
    if definitions.len() > MAX_EXTENSION_POINTS_PER_MODULE {
        return Err(ExtensionContractError::TooManyPoints {
            actual: definitions.len(),
            limit: MAX_EXTENSION_POINTS_PER_MODULE,
        });
    }
    let mut ids = BTreeSet::new();
    for definition in definitions {
        definition.validate()?;
        if !ids.insert((
            definition.point_id.as_str(),
            definition.point_version.as_str(),
        )) {
            return Err(ExtensionContractError::DuplicatePoint {
                point_id: definition.point_id.clone(),
                point_version: definition.point_version.clone(),
            });
        }
    }
    Ok(())
}

pub fn validate_extension_contributions(
    contributions: &[ExtensionContribution],
) -> Result<(), ExtensionContractError> {
    if contributions.len() > MAX_EXTENSION_CONTRIBUTIONS_PER_MODULE {
        return Err(ExtensionContractError::TooManyContributions {
            actual: contributions.len(),
            limit: MAX_EXTENSION_CONTRIBUTIONS_PER_MODULE,
        });
    }
    let mut ids = BTreeSet::new();
    for contribution in contributions {
        contribution.validate()?;
        if !ids.insert(contribution.contribution_id.as_str()) {
            return Err(ExtensionContractError::DuplicateContribution {
                contribution_id: contribution.contribution_id.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtensionContractError {
    #[error("invalid {field}: {detail}")]
    InvalidText { field: String, detail: String },
    #[error("invalid extension semantic version `{version}`; expected major.minor.patch")]
    InvalidVersion { version: String },
    #[error("invalid extension point `{point_id}`: {detail}")]
    InvalidPoint { point_id: String, detail: String },
    #[error("duplicate extension point `{point_id}` version `{point_version}`")]
    DuplicatePoint {
        point_id: String,
        point_version: String,
    },
    #[error("invalid extension contribution `{contribution_id}`: {detail}")]
    InvalidContribution {
        contribution_id: String,
        detail: String,
    },
    #[error("duplicate extension contribution `{contribution_id}`")]
    DuplicateContribution { contribution_id: String },
    #[error("module declares {actual} extension points; limit is {limit}")]
    TooManyPoints { actual: usize, limit: usize },
    #[error("module declares {actual} extension contributions; limit is {limit}")]
    TooManyContributions { actual: usize, limit: usize },
    #[error("invalid extension veto: {detail}")]
    InvalidVeto { detail: String },
}

fn parse_extension_version(value: &str) -> Result<(u64, u64, u64), ExtensionContractError> {
    let mut parts = value.split('.');
    let parsed = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(major), Some(minor), Some(patch), None) => major
            .parse::<u64>()
            .ok()
            .zip(minor.parse::<u64>().ok())
            .zip(patch.parse::<u64>().ok())
            .map(|((major, minor), patch)| (major, minor, patch)),
        _ => None,
    };
    parsed.ok_or_else(|| ExtensionContractError::InvalidVersion {
        version: value.to_string(),
    })
}

fn validate_extension_token(value: &str, field: &str) -> Result<(), ExtensionContractError> {
    if value.is_empty() || value.len() > 256 {
        return Err(ExtensionContractError::InvalidText {
            field: field.to_string(),
            detail: "must contain 1 to 256 bytes".to_string(),
        });
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':' | b'@' | b'*')
    }) {
        return Err(ExtensionContractError::InvalidText {
            field: field.to_string(),
            detail: "contains unsupported characters".to_string(),
        });
    }
    Ok(())
}

fn validate_unique_extension_tokens(
    values: &[String],
    owner: &str,
    kind: &str,
) -> Result<(), ExtensionContractError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_extension_token(value, kind)?;
        if !seen.insert(value.as_str()) {
            return Err(ExtensionContractError::InvalidContribution {
                contribution_id: owner.to_string(),
                detail: format!("duplicate {kind} `{value}`"),
            });
        }
    }
    Ok(())
}

fn validate_unique_copy_values<T: Copy + Ord + std::fmt::Debug>(
    values: &[T],
    point_id: &str,
    kind: &str,
) -> Result<(), ExtensionContractError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(*value) {
            return Err(ExtensionContractError::InvalidPoint {
                point_id: point_id.to_string(),
                detail: format!("duplicate {kind} {value:?}"),
            });
        }
    }
    Ok(())
}

fn validate_order_references(
    contribution_id: &str,
    values: &[String],
    kind: &str,
) -> Result<(), ExtensionContractError> {
    if values.len() > MAX_EXTENSION_ORDER_REFERENCES {
        return Err(ExtensionContractError::InvalidContribution {
            contribution_id: contribution_id.to_string(),
            detail: format!(
                "{} {kind} references exceed the limit of {MAX_EXTENSION_ORDER_REFERENCES}",
                values.len()
            ),
        });
    }
    validate_unique_extension_tokens(values, contribution_id, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_point() -> ExtensionPointDefinition {
        ExtensionPointDefinition::new(
            "sinomo.fixture.diagnostics",
            "1.2.0",
            "sinomo.fixture.request/v1",
            "sinomo.fixture.diagnostic/v1",
            "sinomo.fixture.diagnostic-list/v1",
            ExtensionCompositionPolicy::Collect,
        )
        .allow_kind(ExtensionContributionKind::Operation)
        .with_operation_contract(
            "sinomo.fixture.diagnostics-provider",
            "1.0.0",
            "diagnostics.collect",
        )
    }

    #[test]
    fn point_and_contribution_roundtrip_through_sobj() {
        let point = collect_point();
        point.validate().expect("point");
        let contribution = ExtensionContribution::operation(
            "module.fixture.diagnostics",
            &point.point_id,
            ExtensionVersionRequirement::CompatibleMajor {
                major: 1,
                minimum_minor: 1,
                minimum_patch: 0,
            },
            ExtensionOperationReference::new("fixture.diagnostics", "diagnostics.collect"),
        );
        contribution.validate().expect("contribution");
        let encoded = crate::encode_sobj(&(point.clone(), contribution.clone())).expect("encode");
        let decoded: (ExtensionPointDefinition, ExtensionContribution) =
            crate::decode_sobj(&encoded).expect("decode");
        assert_eq!(decoded, (point, contribution));
    }

    #[test]
    fn version_requirements_match_exact_and_compatible_major() {
        assert!(ExtensionVersionRequirement::exact("1.2.3").matches("1.2.3"));
        assert!(!ExtensionVersionRequirement::exact("1.2.3").matches("1.2.4"));
        let compatible = ExtensionVersionRequirement::CompatibleMajor {
            major: 1,
            minimum_minor: 2,
            minimum_patch: 3,
        };
        assert!(compatible.matches("1.2.3"));
        assert!(compatible.matches("1.4.0"));
        assert!(!compatible.matches("1.2.2"));
        assert!(!compatible.matches("2.0.0"));
    }

    #[test]
    fn select_one_requires_explicit_selection_and_veto_fails_closed() {
        let select = ExtensionPointDefinition::new(
            "sinomo.fixture.selected",
            "1.0.0",
            "sinomo.fixture.input/v1",
            "sinomo.fixture.provider/v1",
            "sinomo.fixture.output/v1",
            ExtensionCompositionPolicy::SelectOne,
        );
        assert!(select.validate().is_err());
        select
            .require_explicit_selection()
            .validate()
            .expect("explicit selection");

        let veto = ExtensionPointDefinition::new(
            "sinomo.fixture.veto",
            "1.0.0",
            "sinomo.fixture.input/v1",
            EXTENSION_VETO_SCHEMA_ID,
            EXTENSION_VETO_SCHEMA_ID,
            ExtensionCompositionPolicy::Veto,
        );
        assert!(veto.validate().is_err());
        veto.with_failure_policy(ExtensionFailurePolicy::FailClosed)
            .validate()
            .expect("fail-closed veto");
    }
}
