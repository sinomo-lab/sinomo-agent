use crate::{AgentSkillDescriptor, AgentSkillSource};
use serde::{Deserialize, Serialize};
use sinomo_runtime::module::{
    DeclarativeModuleLoaderState, ModuleBackendRegistration, ModuleDeclarativeResourceExport,
    ModuleDeclarativeResourceKind, ModuleRegistration, active_native_dylib_module_registrations,
    declarative_module_payload_cache_path, read_declarative_module_loader_states,
    read_declarative_module_registrations,
};
use sinomo_runtime::object::{LengthMode, serde_ext};
use sinomo_runtime::vfs::{VfsHandle, VfsWriteOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

pub const SKILL_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const SKILL_CATALOG_SOBJ_VERSION: u16 = 1;
pub const SKILL_CATALOG_SIFS_ROOT: &str = "/services/sinomo/skills";
pub const SKILL_CATALOG_FILE_NAME: &str = "catalog.sobj";
pub const SKILL_FILE_NAME: &str = "SKILL.md";
pub const SKILL_MANIFEST_FILE_NAME: &str = "skill.sobj";
pub const AGENT_SKILLS_SPEC_URL: &str = "https://agentskills.io/specification";
const SKILL_MARKDOWN_FRONTMATTER_DELIMITER: &str = "---";
const AGENT_SKILL_NAME_MAX_CHARS: usize = 64;
const AGENT_SKILL_DESCRIPTION_MAX_CHARS: usize = 1024;
const AGENT_SKILL_COMPATIBILITY_MAX_CHARS: usize = 500;
const AGENT_SKILL_PACKAGE_MAX_FILES: usize = 2_000;
const AGENT_SKILL_PACKAGE_MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
const AGENT_SKILL_PACKAGE_MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillCatalog {
    #[serde(default = "default_skill_catalog_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<SkillCatalogEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activations: Vec<SkillActivationRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<SkillConflictRecord>,
}

impl Default for SkillCatalog {
    fn default() -> Self {
        Self {
            schema_version: SKILL_CATALOG_SCHEMA_VERSION,
            entries: Vec::new(),
            activations: Vec::new(),
            conflicts: Vec::new(),
        }
    }
}

impl SkillCatalog {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.activations.is_empty() && self.conflicts.is_empty()
    }

    pub fn validate(&self) -> Result<(), SkillCatalogError> {
        if self.schema_version != SKILL_CATALOG_SCHEMA_VERSION {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "unsupported skill catalog schema version {}",
                    self.schema_version
                ),
            });
        }
        let mut entries = BTreeSet::new();
        for entry in &self.entries {
            entry.validate()?;
            if !entries.insert((entry.skill_id.clone(), entry.version.clone())) {
                return Err(SkillCatalogError::InvalidCatalog {
                    detail: format!(
                        "duplicate skill catalog entry {}@{}",
                        entry.skill_id, entry.version
                    ),
                });
            }
        }
        for activation in &self.activations {
            activation.validate()?;
            if self
                .entry(&activation.skill_id, &activation.version)
                .is_none()
            {
                return Err(SkillCatalogError::InvalidCatalog {
                    detail: format!(
                        "activation references missing skill {}@{}",
                        activation.skill_id, activation.version
                    ),
                });
            }
        }
        for conflict in &self.conflicts {
            conflict.validate()?;
        }
        Ok(())
    }

    pub fn entry(&self, skill_id: &str, version: &str) -> Option<&SkillCatalogEntry> {
        self.entries
            .iter()
            .find(|entry| entry.skill_id == skill_id && entry.version == version)
    }

    pub fn upsert_entry(&mut self, entry: SkillCatalogEntry) -> Result<(), SkillCatalogError> {
        entry.validate()?;
        if let Some(existing) = self.entries.iter_mut().find(|candidate| {
            candidate.skill_id == entry.skill_id && candidate.version == entry.version
        }) {
            *existing = entry;
        } else {
            self.entries.push(entry);
            self.entries.sort_by(|left, right| {
                left.skill_id
                    .cmp(&right.skill_id)
                    .then(left.version.cmp(&right.version))
            });
        }
        Ok(())
    }

    pub fn set_activation(
        &mut self,
        activation: SkillActivationRecord,
    ) -> Result<(), SkillCatalogError> {
        activation.validate()?;
        if self
            .entry(&activation.skill_id, &activation.version)
            .is_none()
        {
            return Err(SkillCatalogError::MissingSkillVersion {
                skill_id: activation.skill_id,
                version: activation.version,
            });
        }
        if let Some(existing) = self.activations.iter_mut().find(|candidate| {
            candidate.scope == activation.scope && candidate.skill_id == activation.skill_id
        }) {
            *existing = activation;
        } else {
            self.activations.push(activation);
            self.activations.sort_by(|left, right| {
                left.scope
                    .cmp(&right.scope)
                    .then(left.skill_id.cmp(&right.skill_id))
            });
        }
        Ok(())
    }

    pub fn active_entry_for_scope(
        &self,
        scope: &str,
        skill_id: &str,
    ) -> Option<&SkillCatalogEntry> {
        let activation = self
            .activations
            .iter()
            .find(|candidate| candidate.scope == scope && candidate.skill_id == skill_id)
            .or_else(|| {
                self.activations.iter().find(|candidate| {
                    candidate.scope == SkillActivationScope::Cluster.as_str()
                        && candidate.skill_id == skill_id
                })
            })?;
        self.entry(&activation.skill_id, &activation.version)
    }

    pub fn active_entries_for_scopes<'a, 'scope>(
        &'a self,
        scopes: impl IntoIterator<Item = &'scope str>,
    ) -> Vec<&'a SkillCatalogEntry> {
        let scopes = scopes.into_iter().collect::<Vec<_>>();
        let mut chosen = BTreeMap::<String, &SkillCatalogEntry>::new();
        for scope in scopes
            .iter()
            .copied()
            .chain(std::iter::once(SkillActivationScope::Cluster.as_str()))
        {
            for activation in self
                .activations
                .iter()
                .filter(|activation| activation.scope == scope)
            {
                if chosen.contains_key(&activation.skill_id) {
                    continue;
                }
                if let Some(entry) = self.entry(&activation.skill_id, &activation.version) {
                    chosen.insert(activation.skill_id.clone(), entry);
                }
            }
        }
        chosen.into_values().collect()
    }

    pub fn add_conflict(&mut self, conflict: SkillConflictRecord) -> Result<(), SkillCatalogError> {
        conflict.validate()?;
        if let Some(existing) = self
            .conflicts
            .iter_mut()
            .find(|candidate| candidate.conflict_id == conflict.conflict_id)
        {
            *existing = conflict;
        } else {
            self.conflicts.push(conflict);
            self.conflicts
                .sort_by(|left, right| left.conflict_id.cmp(&right.conflict_id));
        }
        Ok(())
    }

    pub fn resolve_conflict(
        &mut self,
        conflict_id: &str,
        status: SkillConflictStatus,
    ) -> Result<(), SkillCatalogError> {
        let Some(conflict) = self
            .conflicts
            .iter_mut()
            .find(|candidate| candidate.conflict_id == conflict_id)
        else {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!("unknown skill conflict `{conflict_id}`"),
            });
        };
        conflict.status = status;
        Ok(())
    }

    pub fn upstream_update_decision(
        &self,
        active: &SkillCatalogEntry,
        incoming: &SkillCatalogEntry,
    ) -> Result<SkillUpstreamUpdateDecision, SkillCatalogError> {
        active.validate()?;
        incoming.validate()?;
        if active.skill_id != incoming.skill_id {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "cannot compare updates for different skills: {} and {}",
                    active.skill_id, incoming.skill_id
                ),
            });
        }
        if active.source.is_same_managed_source(&incoming.source) {
            return Ok(SkillUpstreamUpdateDecision::AutoReplace);
        }
        if active.source.kind.is_override() {
            if active.base_hash.as_deref() == Some(incoming.content_hash.as_str()) {
                return Ok(SkillUpstreamUpdateDecision::Noop);
            }
            if active.base_hash.is_some() {
                return Ok(SkillUpstreamUpdateDecision::AttemptThreeWayMerge {
                    conflict: SkillConflictRecord::new(active, incoming),
                });
            }
            return Ok(SkillUpstreamUpdateDecision::Conflict {
                conflict: SkillConflictRecord::new(active, incoming),
            });
        }
        Ok(SkillUpstreamUpdateDecision::AutoReplace)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SkillUpstreamUpdateDecision {
    AutoReplace,
    Noop,
    AttemptThreeWayMerge { conflict: SkillConflictRecord },
    Conflict { conflict: SkillConflictRecord },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentSkillFrontmatter {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(
        default,
        rename = "allowed-tools",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_tools: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SkillMarkdownDocument {
    pub frontmatter: AgentSkillFrontmatter,
    pub body: String,
}

impl SkillMarkdownDocument {
    pub fn name(&self) -> &str {
        &self.frontmatter.name
    }

    pub fn description(&self) -> &str {
        &self.frontmatter.description
    }
}

pub fn parse_skill_markdown(text: &str) -> Result<SkillMarkdownDocument, SkillCatalogError> {
    let (frontmatter, body) = split_skill_markdown_frontmatter(text).ok_or_else(|| {
        SkillCatalogError::InvalidAgentSkill {
            detail: "SKILL.md must start with YAML frontmatter delimited by `---`".to_string(),
        }
    })?;
    let frontmatter =
        serde_yaml::from_str::<AgentSkillFrontmatter>(frontmatter).map_err(|source| {
            SkillCatalogError::InvalidAgentSkill {
                detail: format!("invalid SKILL.md YAML frontmatter: {source}"),
            }
        })?;
    validate_agent_skill_frontmatter(&frontmatter)?;
    Ok(SkillMarkdownDocument {
        frontmatter,
        body: body.trim().to_string(),
    })
}

pub fn render_common_skill_markdown(
    entry: &SkillCatalogEntry,
    text: &str,
) -> Result<String, SkillCatalogError> {
    let document = parse_skill_markdown(text)?;
    if document.name() != entry.skill_id {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "SKILL.md name `{}` does not match catalog skill id `{}`",
                document.name(),
                entry.skill_id
            ),
        });
    }
    Ok(text.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentSkillPackageFile {
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentSkillPackage {
    pub markdown: String,
    pub files: Vec<AgentSkillPackageFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillResourceDescriptor {
    pub relative_path: String,
    pub sifs_path: String,
    pub content_hash: String,
    pub byte_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillCatalogEntry {
    pub skill_id: String,
    pub version: String,
    pub source: SkillCatalogSource,
    pub sifs_path: String,
    pub manifest_path: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_hash: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<SkillResourceDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub status: SkillCatalogEntryStatus,
}

impl SkillCatalogEntry {
    pub fn new(
        skill_id: impl Into<String>,
        version: impl Into<String>,
        source: SkillCatalogSource,
        text: impl AsRef<str>,
    ) -> Self {
        let skill_id = skill_id.into();
        let version = version.into();
        let sifs_path = skill_sifs_path(&source, &skill_id, &version);
        let manifest_path = skill_manifest_sifs_path(&source, &skill_id, &version);
        Self {
            skill_id,
            version,
            source,
            sifs_path,
            manifest_path,
            content_hash: skill_content_hash(text.as_ref()),
            base_hash: None,
            description: String::new(),
            license: None,
            compatibility: None,
            allowed_tools: None,
            metadata: BTreeMap::new(),
            resources: Vec::new(),
            target_agents: Vec::new(),
            model_families: Vec::new(),
            activation_tags: Vec::new(),
            required_capabilities: Vec::new(),
            status: SkillCatalogEntryStatus::Available,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_agent_skill_frontmatter(mut self, frontmatter: &AgentSkillFrontmatter) -> Self {
        self.description = frontmatter.description.clone();
        self.license = frontmatter.license.clone();
        self.compatibility = frontmatter.compatibility.clone();
        self.allowed_tools = frontmatter.allowed_tools.clone();
        self.metadata = frontmatter.metadata.clone();
        self
    }

    pub fn with_target_agents(mut self, target_agents: Vec<String>) -> Self {
        self.target_agents = target_agents;
        self
    }

    pub fn with_model_families(mut self, model_families: Vec<String>) -> Self {
        self.model_families = model_families;
        self
    }

    pub fn with_activation_tags(mut self, activation_tags: Vec<String>) -> Self {
        self.activation_tags = activation_tags;
        self
    }

    pub fn with_required_capabilities(mut self, required_capabilities: Vec<String>) -> Self {
        self.required_capabilities = required_capabilities;
        self
    }

    pub fn with_base_hash(mut self, base_hash: impl Into<String>) -> Self {
        self.base_hash = Some(base_hash.into());
        self
    }

    pub fn validate(&self) -> Result<(), SkillCatalogError> {
        validate_skill_token(&self.skill_id, "skill id")?;
        validate_skill_token(&self.version, "skill version")?;
        self.source.validate()?;
        validate_sifs_path(&self.sifs_path, "skill SIFS path")?;
        validate_sifs_path(&self.manifest_path, "skill manifest SIFS path")?;
        validate_content_hash(&self.content_hash)?;
        if let Some(base_hash) = &self.base_hash {
            validate_content_hash(base_hash)?;
        }
        validate_free_text(&self.description, "skill description")?;
        validate_optional_free_text(&self.license, "skill license")?;
        validate_optional_free_text(&self.compatibility, "skill compatibility")?;
        validate_optional_free_text(&self.allowed_tools, "skill allowed tools")?;
        for (key, value) in &self.metadata {
            validate_free_text(key, "skill metadata key")?;
            validate_free_text(value, "skill metadata value")?;
        }
        validate_skill_resources(&self.sifs_path, &self.resources)?;
        validate_token_list(&self.target_agents, "target agent")?;
        validate_token_list(&self.model_families, "model family")?;
        validate_token_list(&self.activation_tags, "activation tag")?;
        validate_token_list(&self.required_capabilities, "required capability")?;
        Ok(())
    }

    pub fn to_agent_descriptor(&self, embedded_text: Option<String>) -> AgentSkillDescriptor {
        AgentSkillDescriptor {
            id: self.skill_id.clone(),
            description: self.description.clone(),
            license: self.license.clone(),
            compatibility: self.compatibility.clone(),
            allowed_tools: self.allowed_tools.clone(),
            metadata: self.metadata.clone(),
            resource_paths: self
                .resources
                .iter()
                .map(|resource| resource.relative_path.clone())
                .collect(),
            resource_contents: BTreeMap::new(),
            source: AgentSkillSource::Catalog,
            package_declared: false,
            module_id: match self.source.kind {
                SkillCatalogSourceKind::Module => self.source.source_id.clone(),
                _ => None,
            },
            module_version: match self.source.kind {
                SkillCatalogSourceKind::Module => self.source.source_version.clone(),
                _ => None,
            },
            path: Some(self.sifs_path.clone()),
            target_agents: self.target_agents.clone(),
            model_families: self.model_families.clone(),
            activation_tags: self.activation_tags.clone(),
            required_capabilities: self.required_capabilities.clone(),
            embedded_text,
            catalog_version: Some(self.version.clone()),
            catalog_content_hash: Some(self.content_hash.clone()),
            catalog_base_hash: self.base_hash.clone(),
            catalog_status: Some(self.status.as_str().to_string()),
            catalog_manifest_path: Some(self.manifest_path.clone()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillCatalogSource {
    pub kind: SkillCatalogSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
}

impl SkillCatalogSource {
    pub fn sinomo(version: impl Into<String>) -> Self {
        Self {
            kind: SkillCatalogSourceKind::Sinomo,
            source_id: Some("sinomo".to_string()),
            source_version: Some(version.into()),
        }
    }

    pub fn module(module_id: impl Into<String>, module_version: impl Into<String>) -> Self {
        Self {
            kind: SkillCatalogSourceKind::Module,
            source_id: Some(module_id.into()),
            source_version: Some(module_version.into()),
        }
    }

    pub fn user(user_id: impl Into<String>) -> Self {
        Self {
            kind: SkillCatalogSourceKind::User,
            source_id: Some(user_id.into()),
            source_version: None,
        }
    }

    pub fn agent(agent_id: impl Into<String>) -> Self {
        Self {
            kind: SkillCatalogSourceKind::Agent,
            source_id: Some(agent_id.into()),
            source_version: None,
        }
    }

    pub fn evolution(job_id: impl Into<String>) -> Self {
        Self {
            kind: SkillCatalogSourceKind::Evolution,
            source_id: Some(job_id.into()),
            source_version: None,
        }
    }

    pub fn validate(&self) -> Result<(), SkillCatalogError> {
        if let Some(source_id) = &self.source_id {
            validate_skill_token(source_id, "skill source id")?;
        }
        if let Some(source_version) = &self.source_version {
            validate_skill_token(source_version, "skill source version")?;
        }
        match self.kind {
            SkillCatalogSourceKind::Sinomo
            | SkillCatalogSourceKind::Module
            | SkillCatalogSourceKind::User
            | SkillCatalogSourceKind::Agent
            | SkillCatalogSourceKind::Evolution => {
                if self.source_id.is_none() {
                    return Err(SkillCatalogError::InvalidCatalog {
                        detail: format!("{} skill source requires a source id", self.kind.as_str()),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn is_same_managed_source(&self, other: &Self) -> bool {
        self.kind.is_provider_managed()
            && other.kind.is_provider_managed()
            && self.kind == other.kind
            && self.source_id == other.source_id
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SkillCatalogSourceKind {
    Sinomo,
    Module,
    User,
    Agent,
    Evolution,
}

impl SkillCatalogSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sinomo => "sinomo",
            Self::Module => "module",
            Self::User => "user",
            Self::Agent => "agent",
            Self::Evolution => "evolution",
        }
    }

    pub fn is_provider_managed(self) -> bool {
        matches!(self, Self::Sinomo | Self::Module)
    }

    pub fn is_override(self) -> bool {
        matches!(self, Self::User | Self::Agent | Self::Evolution)
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SkillCatalogEntryStatus {
    #[default]
    Available,
    Superseded,
    Conflicted,
    Rejected,
}

impl SkillCatalogEntryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Superseded => "superseded",
            Self::Conflicted => "conflicted",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillActivationRecord {
    pub scope: String,
    pub skill_id: String,
    pub version: String,
    pub content_hash: String,
    pub sifs_path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub activated_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

impl SkillActivationRecord {
    pub fn new(
        scope: impl Into<String>,
        entry: &SkillCatalogEntry,
        activated_by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            scope: scope.into(),
            skill_id: entry.skill_id.clone(),
            version: entry.version.clone(),
            content_hash: entry.content_hash.clone(),
            sifs_path: entry.sifs_path.clone(),
            activated_by: activated_by.into(),
            reason: reason.into(),
        }
    }

    pub fn validate(&self) -> Result<(), SkillCatalogError> {
        validate_skill_token(&self.scope, "activation scope")?;
        validate_skill_token(&self.skill_id, "skill id")?;
        validate_skill_token(&self.version, "skill version")?;
        validate_content_hash(&self.content_hash)?;
        validate_sifs_path(&self.sifs_path, "activation SIFS path")?;
        validate_free_text(&self.activated_by, "activation actor")?;
        validate_free_text(&self.reason, "activation reason")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkillActivationScope {
    Cluster,
}

impl SkillActivationScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillConflictRecord {
    pub conflict_id: String,
    pub skill_id: String,
    pub current_version: String,
    pub incoming_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_hash: Option<String>,
    pub current_hash: String,
    pub incoming_hash: String,
    pub current_path: String,
    pub incoming_path: String,
    #[serde(default)]
    pub status: SkillConflictStatus,
}

impl SkillConflictRecord {
    pub fn new(current: &SkillCatalogEntry, incoming: &SkillCatalogEntry) -> Self {
        let conflict_id = skill_conflict_id(current, incoming);
        Self {
            conflict_id,
            skill_id: current.skill_id.clone(),
            current_version: current.version.clone(),
            incoming_version: incoming.version.clone(),
            base_hash: current.base_hash.clone(),
            current_hash: current.content_hash.clone(),
            incoming_hash: incoming.content_hash.clone(),
            current_path: current.sifs_path.clone(),
            incoming_path: incoming.sifs_path.clone(),
            status: SkillConflictStatus::Open,
        }
    }

    pub fn validate(&self) -> Result<(), SkillCatalogError> {
        validate_skill_token(&self.conflict_id, "conflict id")?;
        validate_skill_token(&self.skill_id, "skill id")?;
        validate_skill_token(&self.current_version, "current version")?;
        validate_skill_token(&self.incoming_version, "incoming version")?;
        if let Some(base_hash) = &self.base_hash {
            validate_content_hash(base_hash)?;
        }
        validate_content_hash(&self.current_hash)?;
        validate_content_hash(&self.incoming_hash)?;
        validate_sifs_path(&self.current_path, "current conflict path")?;
        validate_sifs_path(&self.incoming_path, "incoming conflict path")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SkillConflictStatus {
    #[default]
    Open,
    ResolvedKeepExisting,
    ResolvedOverwriteWithBackup,
    ResolvedRenameOverride,
    ResolvedMerged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillCatalogWrite {
    pub entry: SkillCatalogEntry,
    pub text: String,
    pub resources: Vec<AgentSkillPackageFile>,
    pub activate_scope: Option<String>,
    pub activated_by: String,
    pub reason: String,
}

impl SkillCatalogWrite {
    pub fn new(entry: SkillCatalogEntry, text: impl Into<String>) -> Self {
        Self {
            entry,
            text: text.into(),
            resources: Vec::new(),
            activate_scope: None,
            activated_by: String::new(),
            reason: String::new(),
        }
    }

    pub fn from_markdown_skill(
        skill_id: Option<String>,
        version: impl Into<String>,
        source: SkillCatalogSource,
        text: impl Into<String>,
    ) -> Result<Self, SkillCatalogError> {
        let text = text.into();
        let document = parse_skill_markdown(&text)?;
        if let Some(requested_id) = skill_id.as_deref()
            && requested_id != document.name()
        {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "requested skill id `{requested_id}` does not match SKILL.md name `{}`",
                    document.name()
                ),
            });
        }
        let entry = SkillCatalogEntry::new(document.name(), version, source, &text)
            .with_agent_skill_frontmatter(&document.frontmatter);
        Ok(Self::new(entry, text))
    }

    pub fn from_agent_skill_package(
        skill_id: Option<String>,
        version: impl Into<String>,
        source: SkillCatalogSource,
        package: AgentSkillPackage,
    ) -> Result<Self, SkillCatalogError> {
        let mut write = Self::from_markdown_skill(skill_id, version, source, package.markdown)?;
        let package_root = parent_sifs_path(&write.entry.sifs_path).ok_or_else(|| {
            SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "skill path `{}` does not have a package directory",
                    write.entry.sifs_path
                ),
            }
        })?;
        validate_package_files(&package.files)?;
        write.entry.resources = package
            .files
            .iter()
            .map(|file| SkillResourceDescriptor {
                relative_path: file.relative_path.clone(),
                sifs_path: format!("{package_root}/{}", file.relative_path),
                content_hash: skill_resource_content_hash(&file.bytes),
                byte_len: u64::try_from(file.bytes.len()).unwrap_or(u64::MAX),
            })
            .collect();
        write.entry.content_hash = skill_package_content_hash(&write.text, &package.files);
        write.resources = package.files;
        write.entry.validate()?;
        Ok(write)
    }

    pub fn from_module_resource(
        registration: &ModuleRegistration,
        resource: &ModuleDeclarativeResourceExport,
        text: impl Into<String>,
    ) -> Result<Self, SkillCatalogError> {
        if resource.kind != ModuleDeclarativeResourceKind::Skill {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!("module resource `{}` is not a skill", resource.id),
            });
        }
        let text = text.into();
        let document = parse_skill_markdown(&text)?;
        if document.name() != resource.id {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "module skill resource id `{}` does not match SKILL.md name `{}`",
                    resource.id,
                    document.name()
                ),
            });
        }
        let resource_path = Path::new(&resource.path);
        if resource_path.file_name().and_then(|name| name.to_str()) != Some(SKILL_FILE_NAME)
            || resource_path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some(document.name())
        {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "module Agent Skills resource `{}` must end in `{}/SKILL.md`",
                    resource.path,
                    document.name()
                ),
            });
        }
        if !resource.description.is_empty() && resource.description != document.description() {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "module skill resource `{}` description must match SKILL.md frontmatter",
                    resource.id
                ),
            });
        }
        let entry = SkillCatalogEntry::new(
            resource.id.clone(),
            registration.manifest.version.clone(),
            SkillCatalogSource::module(
                registration.module_id().as_str().to_string(),
                registration.manifest.version.clone(),
            ),
            &text,
        )
        .with_agent_skill_frontmatter(&document.frontmatter);
        let entry = apply_module_resource_metadata(entry, resource);
        Ok(Self::new(entry, text))
    }

    pub fn from_embedded_module_resource(
        registration: &ModuleRegistration,
        resource: &ModuleDeclarativeResourceExport,
    ) -> Result<Option<Self>, SkillCatalogError> {
        let Some(text) = resource.embedded_text.clone() else {
            return Ok(None);
        };
        Ok(Some(Self::from_module_resource(
            registration,
            resource,
            text,
        )?))
    }

    pub fn activate_for_scope(
        mut self,
        scope: impl Into<String>,
        activated_by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        self.activate_scope = Some(scope.into());
        self.activated_by = activated_by.into();
        self.reason = reason.into();
        self
    }
}

fn apply_module_resource_metadata(
    entry: SkillCatalogEntry,
    resource: &ModuleDeclarativeResourceExport,
) -> SkillCatalogEntry {
    entry
        .with_target_agents(resource.target_agents.clone())
        .with_model_families(resource.model_families.clone())
        .with_activation_tags(resource.activation_tags.clone())
        .with_required_capabilities(
            resource
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect(),
        )
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillCatalogReconcileReport {
    pub scanned: usize,
    pub materialized: usize,
    pub activated: usize,
    pub unchanged: usize,
    pub conflicts: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<SkillCatalogReconcileSkip>,
}

impl SkillCatalogReconcileReport {
    pub fn merge(&mut self, other: Self) {
        self.scanned = self.scanned.saturating_add(other.scanned);
        self.materialized = self.materialized.saturating_add(other.materialized);
        self.activated = self.activated.saturating_add(other.activated);
        self.unchanged = self.unchanged.saturating_add(other.unchanged);
        self.conflicts = self.conflicts.saturating_add(other.conflicts);
        self.skipped.extend(other.skipped);
    }

    pub fn skipped(mut self, item: impl Into<String>, reason: impl Into<String>) -> Self {
        self.skipped.push(SkillCatalogReconcileSkip {
            item: item.into(),
            reason: reason.into(),
        });
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillCatalogReconcileSkip {
    pub item: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuiltInSkillDefinition {
    pub skill_id: String,
    pub version: String,
    pub description: String,
    pub text: String,
    pub target_agents: Vec<String>,
    pub model_families: Vec<String>,
    pub activation_tags: Vec<String>,
    pub required_capabilities: Vec<String>,
}

impl BuiltInSkillDefinition {
    pub fn new(
        skill_id: impl Into<String>,
        version: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            skill_id: skill_id.into(),
            version: version.into(),
            description: String::new(),
            text: text.into(),
            target_agents: Vec::new(),
            model_families: Vec::new(),
            activation_tags: Vec::new(),
            required_capabilities: Vec::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_target_agent(mut self, target_agent: impl Into<String>) -> Self {
        self.target_agents.push(target_agent.into());
        self
    }

    pub fn with_model_family(mut self, model_family: impl Into<String>) -> Self {
        self.model_families.push(model_family.into());
        self
    }

    pub fn with_activation_tag(mut self, tag: impl Into<String>) -> Self {
        self.activation_tags.push(tag.into());
        self
    }

    pub fn with_required_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }

    pub fn to_catalog_write(&self) -> Result<SkillCatalogWrite, SkillCatalogError> {
        let mut write = SkillCatalogWrite::from_markdown_skill(
            Some(self.skill_id.clone()),
            self.version.clone(),
            SkillCatalogSource::sinomo(self.version.clone()),
            self.text.clone(),
        )?;
        if !self.description.is_empty() && self.description != write.entry.description {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "built-in skill `{}` description must match SKILL.md frontmatter",
                    self.skill_id
                ),
            });
        }
        write.entry = write
            .entry
            .with_target_agents(self.target_agents.clone())
            .with_model_families(self.model_families.clone())
            .with_activation_tags(self.activation_tags.clone())
            .with_required_capabilities(self.required_capabilities.clone());
        Ok(write)
    }
}

pub fn reconcile_builtin_skills_to_catalog<S: SkillCatalogStore>(
    store: &S,
    definitions: &[BuiltInSkillDefinition],
    activate_scope: Option<&str>,
    activated_by: &str,
    reason: &str,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let mut report = SkillCatalogReconcileReport::default();
    for definition in definitions {
        report.scanned = report.scanned.saturating_add(1);
        let outcome = reconcile_skill_catalog_write(
            store,
            definition.to_catalog_write()?,
            activate_scope,
            activated_by,
            reason,
        )?;
        report.apply_outcome(outcome);
    }
    Ok(report)
}

pub fn reconcile_module_skills_to_catalog<S: SkillCatalogStore>(
    store: &S,
    registration: &ModuleRegistration,
    payload_root: Option<&Path>,
    activate_scope: Option<&str>,
    activated_by: &str,
    reason: &str,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let mut report = SkillCatalogReconcileReport::default();
    for resource in &registration.surfaces.declarative_resources {
        if resource.kind != ModuleDeclarativeResourceKind::Skill {
            continue;
        }
        report.scanned = report.scanned.saturating_add(1);
        let Some(write) =
            module_resource_catalog_write(registration, resource, payload_root, &mut report)?
        else {
            continue;
        };
        let outcome =
            reconcile_skill_catalog_write(store, write, activate_scope, activated_by, reason)?;
        report.apply_outcome(outcome);
    }
    Ok(report)
}

pub fn reconcile_active_declarative_module_skills_to_catalog<S: SkillCatalogStore>(
    store: &S,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let root = data_dir.root();
    let states = read_declarative_module_loader_states(&root).map_err(|source| {
        SkillCatalogError::Store {
            path: root.join("module/loader/declarative").display().to_string(),
            detail: source.to_string(),
        }
    })?;
    let active_states = states
        .into_iter()
        .filter(|state| state.active)
        .map(|state| (state.module_id.to_string(), state))
        .collect::<BTreeMap<_, _>>();
    if active_states.is_empty() {
        return Ok(SkillCatalogReconcileReport::default());
    }

    let registrations = read_declarative_module_registrations(&root).map_err(|source| {
        SkillCatalogError::Store {
            path: root
                .join("module/catalog/declarative")
                .display()
                .to_string(),
            detail: source.to_string(),
        }
    })?;
    let mut report = SkillCatalogReconcileReport::default();
    for registration in registrations {
        let module_id = registration.module_id().to_string();
        let Some(state) = active_states.get(&module_id) else {
            continue;
        };
        if !declarative_registration_matches_loader_state(&registration, state) {
            report = report.skipped(
                module_id,
                "declarative registration does not match the active loader state",
            );
            continue;
        }
        let payload_path = declarative_module_payload_cache_path(
            &root,
            &state.module_id,
            &state.version,
            &state.content_hash,
        );
        let module_report = reconcile_module_skills_to_catalog(
            store,
            &registration,
            Some(&payload_path),
            Some(SkillActivationScope::Cluster.as_str()),
            &format!("module:{}", registration.module_id()),
            "active declarative module skill reconciliation",
        )?;
        report.merge(module_report);
    }
    Ok(report)
}

/// Reconciles skills exported by every active module backend. Native dylib
/// module resources are inline in their signed descriptor, so unlike
/// declarative modules they do not have a separate payload root.
pub fn reconcile_active_module_skills_to_catalog<S: SkillCatalogStore>(
    store: &S,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let mut report = reconcile_active_declarative_module_skills_to_catalog(store, data_dir)?;
    let root = data_dir.root();
    let registrations = active_native_dylib_module_registrations(&root).map_err(|source| {
        SkillCatalogError::Store {
            path: root
                .join("module/loader/native-dylib")
                .display()
                .to_string(),
            detail: source.to_string(),
        }
    })?;
    for registration in registrations {
        let module_report = reconcile_module_skills_to_catalog(
            store,
            &registration,
            None,
            Some(SkillActivationScope::Cluster.as_str()),
            &format!("module:{}", registration.module_id()),
            "active native module skill reconciliation",
        )?;
        report.merge(module_report);
    }
    Ok(report)
}

pub fn reconcile_active_declarative_module_skills_to_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let store = SkillCatalogSifsStore::open(Some(data_dir.root()))?;
    reconcile_active_declarative_module_skills_to_catalog(&store, data_dir)
}

pub fn reconcile_active_module_skills_to_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalogReconcileReport, SkillCatalogError> {
    let store = SkillCatalogSifsStore::open(Some(data_dir.root()))?;
    reconcile_active_module_skills_to_catalog(&store, data_dir)
}

pub fn load_skill_catalog_from_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalog, SkillCatalogError> {
    SkillCatalogSifsStore::open(Some(data_dir.root()))?.load_catalog()
}

pub fn load_reconciled_skill_catalog_from_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<SkillCatalog, SkillCatalogError> {
    let store = SkillCatalogSifsStore::open(Some(data_dir.root()))?;
    reconcile_active_module_skills_to_catalog(&store, data_dir)?;
    store.load_catalog()
}

pub fn load_skill_catalog_texts_from_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    catalog: &SkillCatalog,
) -> Result<BTreeMap<String, String>, SkillCatalogError> {
    let store = SkillCatalogSifsStore::open(Some(data_dir.root()))?;
    let mut texts = BTreeMap::new();
    for entry in &catalog.entries {
        if let Some(text) = store.read_skill_text(entry)? {
            texts.insert(skill_catalog_entry_text_key(entry), text);
        }
    }
    Ok(texts)
}

pub fn load_skill_catalog_resources_from_sifs(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    catalog: &SkillCatalog,
) -> Result<BTreeMap<String, Vec<AgentSkillPackageFile>>, SkillCatalogError> {
    let store = SkillCatalogSifsStore::open(Some(data_dir.root()))?;
    let mut resources = BTreeMap::new();
    for entry in &catalog.entries {
        let Some(package) = store.read_skill_package(entry)? else {
            continue;
        };
        if !package.files.is_empty() {
            resources.insert(skill_catalog_entry_text_key(entry), package.files);
        }
    }
    Ok(resources)
}

pub fn skill_catalog_entry_text_key(entry: &SkillCatalogEntry) -> String {
    skill_catalog_text_key(&entry.skill_id, &entry.version, &entry.content_hash)
}

pub fn skill_catalog_text_key(skill_id: &str, version: &str, content_hash: &str) -> String {
    format!("{skill_id}@{version}#{content_hash}")
}

impl SkillCatalogReconcileReport {
    fn apply_outcome(&mut self, outcome: SkillCatalogReconcileOutcome) {
        match outcome {
            SkillCatalogReconcileOutcome::Materialized { activated } => {
                self.materialized = self.materialized.saturating_add(1);
                if activated {
                    self.activated = self.activated.saturating_add(1);
                }
            }
            SkillCatalogReconcileOutcome::Unchanged => {
                self.unchanged = self.unchanged.saturating_add(1);
            }
            SkillCatalogReconcileOutcome::Conflict => {
                self.materialized = self.materialized.saturating_add(1);
                self.conflicts = self.conflicts.saturating_add(1);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SkillCatalogReconcileOutcome {
    Materialized { activated: bool },
    Unchanged,
    Conflict,
}

fn reconcile_skill_catalog_write<S: SkillCatalogStore>(
    store: &S,
    write: SkillCatalogWrite,
    activate_scope: Option<&str>,
    activated_by: &str,
    reason: &str,
) -> Result<SkillCatalogReconcileOutcome, SkillCatalogError> {
    let Some(scope) = activate_scope else {
        store.write_skill_version(write)?;
        return Ok(SkillCatalogReconcileOutcome::Materialized { activated: false });
    };
    let catalog = store.load_catalog()?;
    let decision = catalog
        .active_entry_for_scope(scope, &write.entry.skill_id)
        .map(|active| catalog.upstream_update_decision(active, &write.entry))
        .transpose()?
        .unwrap_or(SkillUpstreamUpdateDecision::AutoReplace);
    match decision {
        SkillUpstreamUpdateDecision::AutoReplace => {
            store.write_skill_version(write.activate_for_scope(scope, activated_by, reason))?;
            Ok(SkillCatalogReconcileOutcome::Materialized { activated: true })
        }
        SkillUpstreamUpdateDecision::Noop => {
            store.write_skill_version(write)?;
            Ok(SkillCatalogReconcileOutcome::Unchanged)
        }
        SkillUpstreamUpdateDecision::AttemptThreeWayMerge { conflict }
        | SkillUpstreamUpdateDecision::Conflict { conflict } => {
            store.write_skill_version(write)?;
            store.record_conflict(conflict)?;
            Ok(SkillCatalogReconcileOutcome::Conflict)
        }
    }
}

fn module_resource_catalog_write(
    registration: &ModuleRegistration,
    resource: &ModuleDeclarativeResourceExport,
    payload_root: Option<&Path>,
    report: &mut SkillCatalogReconcileReport,
) -> Result<Option<SkillCatalogWrite>, SkillCatalogError> {
    if let Some(write) = SkillCatalogWrite::from_embedded_module_resource(registration, resource)? {
        return Ok(Some(write));
    }
    let Some(payload_root) = payload_root else {
        report.skipped.push(SkillCatalogReconcileSkip {
            item: resource.id.clone(),
            reason: "path-backed module skill has no payload root".to_string(),
        });
        return Ok(None);
    };
    let path = module_payload_resource_path(payload_root, &resource.path)?;
    let package = read_agent_skill_package(&path)?;
    let source = SkillCatalogSource::module(
        registration.module_id().as_str().to_string(),
        registration.manifest.version.clone(),
    );
    let mut write = SkillCatalogWrite::from_agent_skill_package(
        Some(resource.id.clone()),
        registration.manifest.version.clone(),
        source,
        package,
    )?;
    if !resource.description.is_empty() && resource.description != write.entry.description {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "module skill resource `{}` description must match SKILL.md frontmatter",
                resource.id
            ),
        });
    }
    write.entry = apply_module_resource_metadata(write.entry, resource);
    Ok(Some(write))
}

fn module_payload_resource_path(
    payload_root: &Path,
    resource_path: &str,
) -> Result<PathBuf, SkillCatalogError> {
    let candidate = Path::new(resource_path);
    if candidate.is_absolute() {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("module skill resource path `{resource_path}` must be relative"),
        });
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SkillCatalogError::InvalidCatalog {
                    detail: format!(
                        "module skill resource path `{resource_path}` must stay inside payload root"
                    ),
                });
            }
        }
    }
    Ok(payload_root.join(candidate))
}

fn declarative_registration_matches_loader_state(
    registration: &ModuleRegistration,
    state: &DeclarativeModuleLoaderState,
) -> bool {
    if registration.module_id() != &state.module_id
        || registration.manifest.version != state.version
    {
        return false;
    }
    match &registration.backend {
        ModuleBackendRegistration::Declarative { payload_ref } => payload_ref == &state.payload_ref,
        _ => false,
    }
}

pub fn read_agent_skill_package(path: &Path) -> Result<AgentSkillPackage, SkillCatalogError> {
    let import_metadata =
        std::fs::symlink_metadata(path).map_err(|source| SkillCatalogError::Store {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if import_metadata.file_type().is_symlink() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "skill package import path `{}` must not be a symbolic link",
                path.display()
            ),
        });
    }
    let (skill_dir, skill_file) = if path.is_dir() {
        (path, path.join(SKILL_FILE_NAME))
    } else {
        let file_name = path.file_name().and_then(|name| name.to_str());
        if file_name != Some(SKILL_FILE_NAME) {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "Agent Skills packages must be directories containing exactly `{SKILL_FILE_NAME}`"
                ),
            });
        }
        let skill_dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| SkillCatalogError::InvalidAgentSkill {
                detail: format!("`{}` has no skill package directory", path.display()),
            })?;
        (skill_dir, path.to_path_buf())
    };
    let skill_metadata =
        std::fs::symlink_metadata(&skill_file).map_err(|source| SkillCatalogError::Store {
            path: skill_file.display().to_string(),
            detail: source.to_string(),
        })?;
    if !skill_metadata.file_type().is_file() || skill_metadata.file_type().is_symlink() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!("`{}` must be a regular file", skill_file.display()),
        });
    }
    let markdown =
        std::fs::read_to_string(&skill_file).map_err(|source| SkillCatalogError::Store {
            path: skill_file.display().to_string(),
            detail: source.to_string(),
        })?;
    let document = parse_skill_markdown(&markdown)?;
    let directory_name = skill_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "skill package directory `{}` is not valid UTF-8",
                skill_dir.display()
            ),
        })?;
    if directory_name != document.name() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "skill directory name `{directory_name}` must match SKILL.md name `{}`",
                document.name()
            ),
        });
    }

    let mut files = Vec::new();
    let mut total_bytes = markdown.len();
    collect_agent_skill_package_files(skill_dir, skill_dir, &mut files, &mut total_bytes)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    validate_package_files(&files)?;
    Ok(AgentSkillPackage { markdown, files })
}

pub fn write_agent_skill_package(
    package_dir: &Path,
    package: &AgentSkillPackage,
) -> Result<(), SkillCatalogError> {
    let document = parse_skill_markdown(&package.markdown)?;
    let directory_name = package_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "skill package directory `{}` is not valid UTF-8",
                package_dir.display()
            ),
        })?;
    if directory_name != document.name() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "export directory name `{directory_name}` must match SKILL.md name `{}`",
                document.name()
            ),
        });
    }
    validate_package_files(&package.files)?;
    std::fs::create_dir_all(package_dir).map_err(|source| SkillCatalogError::Store {
        path: package_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let skill_file = package_dir.join(SKILL_FILE_NAME);
    std::fs::write(&skill_file, package.markdown.as_bytes()).map_err(|source| {
        SkillCatalogError::Store {
            path: skill_file.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    for file in &package.files {
        let path = package_dir.join(&file.relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| SkillCatalogError::Store {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        std::fs::write(&path, &file.bytes).map_err(|source| SkillCatalogError::Store {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

fn collect_agent_skill_package_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<AgentSkillPackageFile>,
    total_bytes: &mut usize,
) -> Result<(), SkillCatalogError> {
    let entries = std::fs::read_dir(current).map_err(|source| SkillCatalogError::Store {
        path: current.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| SkillCatalogError::Store {
            path: current.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| SkillCatalogError::Store {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if file_type.is_symlink() {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill package resource `{}` must not be a symbolic link",
                    path.display()
                ),
            });
        }
        if file_type.is_dir() {
            collect_agent_skill_package_files(root, &path, files, total_bytes)?;
            continue;
        }
        if !file_type.is_file() || path == root.join(SKILL_FILE_NAME) {
            continue;
        }
        if files.len() >= AGENT_SKILL_PACKAGE_MAX_FILES {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill package exceeds the {AGENT_SKILL_PACKAGE_MAX_FILES} resource file limit"
                ),
            });
        }
        let metadata = entry
            .metadata()
            .map_err(|source| SkillCatalogError::Store {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        let byte_len =
            usize::try_from(metadata.len()).map_err(|_| SkillCatalogError::InvalidAgentSkill {
                detail: format!("skill package resource `{}` is too large", path.display()),
            })?;
        if byte_len > AGENT_SKILL_PACKAGE_MAX_FILE_BYTES {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill package resource `{}` exceeds the {} byte file limit",
                    path.display(),
                    AGENT_SKILL_PACKAGE_MAX_FILE_BYTES
                ),
            });
        }
        *total_bytes = total_bytes.checked_add(byte_len).ok_or_else(|| {
            SkillCatalogError::InvalidAgentSkill {
                detail: "skill package size overflow".to_string(),
            }
        })?;
        if *total_bytes > AGENT_SKILL_PACKAGE_MAX_TOTAL_BYTES {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill package exceeds the {} byte total limit",
                    AGENT_SKILL_PACKAGE_MAX_TOTAL_BYTES
                ),
            });
        }
        let relative_path =
            path.strip_prefix(root)
                .map_err(|source| SkillCatalogError::InvalidAgentSkill {
                    detail: format!(
                        "skill resource `{}` escaped package root: {source}",
                        path.display()
                    ),
                })?;
        let relative_path = portable_skill_relative_path(relative_path)?;
        let bytes = std::fs::read(&path).map_err(|source| SkillCatalogError::Store {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        files.push(AgentSkillPackageFile {
            relative_path,
            bytes,
        });
    }
    Ok(())
}

#[derive(Debug)]
pub struct SkillCatalogSifsStore {
    root: String,
    vfs: VfsHandle,
}

impl SkillCatalogSifsStore {
    pub fn from_vfs(vfs: VfsHandle) -> Self {
        Self {
            root: SKILL_CATALOG_SIFS_ROOT.to_string(),
            vfs,
        }
    }

    pub fn open(data_dir: Option<PathBuf>) -> Result<Self, SkillCatalogError> {
        let vfs = sinomo_runtime::vfs::mounted_sifs(data_dir).map_err(|source| {
            SkillCatalogError::Store {
                path: SKILL_CATALOG_SIFS_ROOT.to_string(),
                detail: source.to_string(),
            }
        })?;
        Ok(Self::from_vfs(vfs))
    }
}

impl SkillCatalogStore for SkillCatalogSifsStore {
    fn root(&self) -> &str {
        &self.root
    }

    fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SkillCatalogError> {
        validate_sifs_path(path, "SIFS read path")?;
        self.vfs
            .read_optional(path)
            .map_err(|source| SkillCatalogError::Store {
                path: path.to_string(),
                detail: source.to_string(),
            })
    }

    fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), SkillCatalogError> {
        validate_sifs_path(path, "SIFS write path")?;
        if let Some(parent) = parent_sifs_path(path) {
            self.vfs
                .create_dir_all(&parent)
                .map_err(|source| SkillCatalogError::Store {
                    path: parent,
                    detail: source.to_string(),
                })?;
        }
        self.vfs
            .write_with_options(path, bytes, VfsWriteOptions::service_artifact())
            .map_err(|source| SkillCatalogError::Store {
                path: path.to_string(),
                detail: source.to_string(),
            })
    }
}

pub trait SkillCatalogStore {
    fn root(&self) -> &str;
    fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SkillCatalogError>;
    fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), SkillCatalogError>;

    fn catalog_path(&self) -> String {
        format!(
            "{}/{}",
            self.root().trim_end_matches('/'),
            SKILL_CATALOG_FILE_NAME
        )
    }

    fn load_catalog(&self) -> Result<SkillCatalog, SkillCatalogError> {
        let path = self.catalog_path();
        let Some(bytes) = self.read_file(&path)? else {
            return Ok(SkillCatalog::default());
        };
        let catalog = decode_catalog_sobj::<SkillCatalog>(&path, &bytes)?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn save_catalog(&self, catalog: &SkillCatalog) -> Result<(), SkillCatalogError> {
        catalog.validate()?;
        let path = self.catalog_path();
        self.write_file(&path, &encode_catalog_sobj(&path, catalog)?)
    }

    fn read_skill_text(
        &self,
        entry: &SkillCatalogEntry,
    ) -> Result<Option<String>, SkillCatalogError> {
        let Some(bytes) = self.read_file(&entry.sifs_path)? else {
            return Ok(None);
        };
        let text =
            String::from_utf8(bytes).map_err(|source| SkillCatalogError::InvalidSkillText {
                path: entry.sifs_path.clone(),
                detail: source.to_string(),
            })?;
        Ok(Some(text))
    }

    fn read_skill_package(
        &self,
        entry: &SkillCatalogEntry,
    ) -> Result<Option<AgentSkillPackage>, SkillCatalogError> {
        let Some(markdown) = self.read_skill_text(entry)? else {
            return Ok(None);
        };
        let mut files = Vec::with_capacity(entry.resources.len());
        for resource in &entry.resources {
            let bytes =
                self.read_file(&resource.sifs_path)?
                    .ok_or_else(|| SkillCatalogError::Store {
                        path: resource.sifs_path.clone(),
                        detail: "catalog resource is missing".to_string(),
                    })?;
            let actual_hash = skill_resource_content_hash(&bytes);
            if actual_hash != resource.content_hash || bytes.len() as u64 != resource.byte_len {
                return Err(SkillCatalogError::ResourceHashMismatch {
                    path: resource.sifs_path.clone(),
                    expected: resource.content_hash.clone(),
                    actual: actual_hash,
                });
            }
            files.push(AgentSkillPackageFile {
                relative_path: resource.relative_path.clone(),
                bytes,
            });
        }
        let actual_hash = skill_package_content_hash(&markdown, &files);
        if actual_hash != entry.content_hash {
            return Err(SkillCatalogError::ContentHashMismatch {
                skill_id: entry.skill_id.clone(),
                expected: entry.content_hash.clone(),
                actual: actual_hash,
            });
        }
        Ok(Some(AgentSkillPackage { markdown, files }))
    }

    fn write_skill_version(
        &self,
        write: SkillCatalogWrite,
    ) -> Result<SkillCatalog, SkillCatalogError> {
        write.entry.validate()?;
        validate_package_files(&write.resources)?;
        let actual_hash = skill_package_content_hash(&write.text, &write.resources);
        if write.entry.content_hash != actual_hash {
            return Err(SkillCatalogError::ContentHashMismatch {
                skill_id: write.entry.skill_id,
                expected: write.entry.content_hash,
                actual: actual_hash,
            });
        }
        if write.resources.len() != write.entry.resources.len() {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "skill {} has {} resource descriptors for {} resource files",
                    write.entry.skill_id,
                    write.entry.resources.len(),
                    write.resources.len()
                ),
            });
        }
        for (resource, descriptor) in write.resources.iter().zip(&write.entry.resources) {
            if resource.relative_path != descriptor.relative_path {
                return Err(SkillCatalogError::InvalidCatalog {
                    detail: format!(
                        "skill resource order mismatch for `{}`",
                        resource.relative_path
                    ),
                });
            }
            let actual_resource_hash = skill_resource_content_hash(&resource.bytes);
            if descriptor.content_hash != actual_resource_hash
                || descriptor.byte_len != resource.bytes.len() as u64
            {
                return Err(SkillCatalogError::ResourceHashMismatch {
                    path: descriptor.sifs_path.clone(),
                    expected: descriptor.content_hash.clone(),
                    actual: actual_resource_hash,
                });
            }
        }
        let mut catalog = self.load_catalog()?;
        self.write_file(&write.entry.sifs_path, write.text.as_bytes())?;
        for (resource, descriptor) in write.resources.iter().zip(&write.entry.resources) {
            self.write_file(&descriptor.sifs_path, &resource.bytes)?;
        }
        self.write_file(
            &write.entry.manifest_path,
            &encode_catalog_sobj(&write.entry.manifest_path, &write.entry)?,
        )?;
        catalog.upsert_entry(write.entry.clone())?;
        if let Some(scope) = write.activate_scope {
            let activation =
                SkillActivationRecord::new(scope, &write.entry, write.activated_by, write.reason);
            catalog.set_activation(activation.clone())?;
            let path = skill_active_sifs_path(&activation.scope, &activation.skill_id);
            self.write_file(&path, &encode_catalog_sobj(&path, &activation)?)?;
        }
        self.save_catalog(&catalog)?;
        Ok(catalog)
    }

    fn activate_skill_version(
        &self,
        entry: &SkillCatalogEntry,
        scope: impl Into<String>,
        activated_by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<SkillCatalog, SkillCatalogError> {
        entry.validate()?;
        let activation =
            SkillActivationRecord::new(scope.into(), entry, activated_by.into(), reason.into());
        let mut catalog = self.load_catalog()?;
        catalog.set_activation(activation.clone())?;
        let path = skill_active_sifs_path(&activation.scope, &activation.skill_id);
        self.write_file(&path, &encode_catalog_sobj(&path, &activation)?)?;
        self.save_catalog(&catalog)?;
        Ok(catalog)
    }

    fn record_conflict(
        &self,
        conflict: SkillConflictRecord,
    ) -> Result<SkillCatalog, SkillCatalogError> {
        let mut catalog = self.load_catalog()?;
        catalog.add_conflict(conflict.clone())?;
        let path = skill_conflict_sifs_path(&conflict.conflict_id);
        self.write_file(&path, &encode_catalog_sobj(&path, &conflict)?)?;
        self.save_catalog(&catalog)?;
        Ok(catalog)
    }
}

#[derive(Debug)]
pub struct LocalSkillCatalogStore {
    root: String,
    vfs: VfsHandle,
}

impl LocalSkillCatalogStore {
    pub fn new(local_root: impl Into<PathBuf>) -> Self {
        let local_root = local_root.into();
        Self {
            root: SKILL_CATALOG_SIFS_ROOT.to_string(),
            vfs: VfsHandle::local(local_root).expect("local skill catalog VFS root"),
        }
    }
}

impl SkillCatalogStore for LocalSkillCatalogStore {
    fn root(&self) -> &str {
        &self.root
    }

    fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SkillCatalogError> {
        validate_sifs_path(path, "local SIFS path")?;
        self.vfs
            .read_optional(path)
            .map_err(|source| SkillCatalogError::Store {
                path: path.to_string(),
                detail: source.to_string(),
            })
    }

    fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), SkillCatalogError> {
        validate_sifs_path(path, "local SIFS path")?;
        if let Some(parent) = parent_sifs_path(path) {
            self.vfs
                .create_dir_all(&parent)
                .map_err(|source| SkillCatalogError::Store {
                    path: parent,
                    detail: source.to_string(),
                })?;
        }
        self.vfs
            .write_with_options(path, bytes, VfsWriteOptions::local_replace())
            .map_err(|source| SkillCatalogError::Store {
                path: path.to_string(),
                detail: source.to_string(),
            })
    }
}

pub fn skill_content_hash(text: &str) -> String {
    format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex())
}

pub fn skill_resource_content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn skill_package_content_hash(text: &str, files: &[AgentSkillPackageFile]) -> String {
    if files.is_empty() {
        return skill_content_hash(text);
    }
    let mut files = files.iter().collect::<Vec<_>>();
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.agent-skill-package.v1\0");
    hasher.update(SKILL_FILE_NAME.as_bytes());
    hasher.update(b"\0");
    hasher.update(&(text.len() as u64).to_le_bytes());
    hasher.update(text.as_bytes());
    for file in files {
        hasher.update(file.relative_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(&(file.bytes.len() as u64).to_le_bytes());
        hasher.update(&file.bytes);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

pub fn skill_sifs_path(source: &SkillCatalogSource, skill_id: &str, version: &str) -> String {
    let source_id = source.source_id.as_deref().unwrap_or("unknown");
    let source_version = source.source_version.as_deref().unwrap_or(version);
    match source.kind {
        SkillCatalogSourceKind::Sinomo => format!(
            "{}/sources/sinomo/{}/{}/{}",
            SKILL_CATALOG_SIFS_ROOT, skill_id, version, SKILL_FILE_NAME
        ),
        SkillCatalogSourceKind::Module => format!(
            "{}/sources/modules/{}/{}/{}/{}/{}",
            SKILL_CATALOG_SIFS_ROOT, source_id, source_version, skill_id, version, SKILL_FILE_NAME
        ),
        SkillCatalogSourceKind::User => format!(
            "{}/sources/users/{}/{}/{}/{}",
            SKILL_CATALOG_SIFS_ROOT, source_id, skill_id, version, SKILL_FILE_NAME
        ),
        SkillCatalogSourceKind::Agent => format!(
            "{}/sources/agents/{}/{}/{}/{}",
            SKILL_CATALOG_SIFS_ROOT, source_id, skill_id, version, SKILL_FILE_NAME
        ),
        SkillCatalogSourceKind::Evolution => format!(
            "{}/sources/evolution/{}/{}/{}/{}",
            SKILL_CATALOG_SIFS_ROOT, source_id, skill_id, version, SKILL_FILE_NAME
        ),
    }
}

pub fn skill_manifest_sifs_path(
    source: &SkillCatalogSource,
    skill_id: &str,
    version: &str,
) -> String {
    skill_sifs_path(source, skill_id, version).replace(SKILL_FILE_NAME, SKILL_MANIFEST_FILE_NAME)
}

pub fn skill_active_sifs_path(scope: &str, skill_id: &str) -> String {
    format!(
        "{}/active/{}/{}.sobj",
        SKILL_CATALOG_SIFS_ROOT,
        scope,
        path_token(skill_id)
    )
}

pub fn skill_conflict_sifs_path(conflict_id: &str) -> String {
    format!(
        "{}/conflicts/{}.sobj",
        SKILL_CATALOG_SIFS_ROOT,
        path_token(conflict_id)
    )
}

pub fn skill_conflict_id(current: &SkillCatalogEntry, incoming: &SkillCatalogEntry) -> String {
    let input = format!(
        "{}\0{}\0{}\0{}",
        current.skill_id, current.content_hash, incoming.version, incoming.content_hash
    );
    format!("conflict_{}", blake3::hash(input.as_bytes()).to_hex())
}

fn split_skill_markdown_frontmatter(text: &str) -> Option<(&str, &str)> {
    let first_line_end = text.find('\n').unwrap_or(text.len());
    let first_line = text[..first_line_end].trim_end_matches('\r');
    if first_line != SKILL_MARKDOWN_FRONTMATTER_DELIMITER {
        return None;
    }
    if first_line_end == text.len() {
        return None;
    }
    let frontmatter_start = first_line_end + 1;
    let mut line_start = frontmatter_start;
    while line_start <= text.len() {
        let line_end = text[line_start..]
            .find('\n')
            .map(|offset| line_start + offset + 1)
            .unwrap_or(text.len());
        let line = text[line_start..line_end].trim_end_matches(['\r', '\n']);
        if line == SKILL_MARKDOWN_FRONTMATTER_DELIMITER {
            return Some((&text[frontmatter_start..line_start], &text[line_end..]));
        }
        if line_end == text.len() {
            break;
        }
        line_start = line_end;
    }
    None
}

fn validate_agent_skill_frontmatter(
    frontmatter: &AgentSkillFrontmatter,
) -> Result<(), SkillCatalogError> {
    validate_agent_skill_name(&frontmatter.name)?;
    let description_chars = frontmatter.description.chars().count();
    if frontmatter.description.trim().is_empty() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: "frontmatter `description` must be a non-empty string".to_string(),
        });
    }
    if description_chars > AGENT_SKILL_DESCRIPTION_MAX_CHARS {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "frontmatter `description` exceeds the {AGENT_SKILL_DESCRIPTION_MAX_CHARS} character limit"
            ),
        });
    }
    if let Some(compatibility) = &frontmatter.compatibility {
        let compatibility_chars = compatibility.chars().count();
        if compatibility.trim().is_empty()
            || compatibility_chars > AGENT_SKILL_COMPATIBILITY_MAX_CHARS
        {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "frontmatter `compatibility` must contain 1-{AGENT_SKILL_COMPATIBILITY_MAX_CHARS} characters"
                ),
            });
        }
    }
    if let Some(license) = &frontmatter.license
        && license.trim().is_empty()
    {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: "frontmatter `license` must be non-empty when provided".to_string(),
        });
    }
    for (key, value) in &frontmatter.metadata {
        if key.trim().is_empty() {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: "frontmatter `metadata` keys must be non-empty strings".to_string(),
            });
        }
        validate_free_text(value, "Agent Skills metadata value")?;
    }
    Ok(())
}

fn validate_agent_skill_name(name: &str) -> Result<(), SkillCatalogError> {
    let char_count = name.chars().count();
    if !(1..=AGENT_SKILL_NAME_MAX_CHARS).contains(&char_count) {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "frontmatter `name` must contain 1-{AGENT_SKILL_NAME_MAX_CHARS} characters"
            ),
        });
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: "frontmatter `name` must not start or end with a hyphen".to_string(),
        });
    }
    if name.contains("--") {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: "frontmatter `name` must not contain consecutive hyphens".to_string(),
        });
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail:
                "frontmatter `name` may contain only lowercase ASCII letters, numbers, and hyphens"
                    .to_string(),
        });
    }
    Ok(())
}

fn portable_skill_relative_path(path: &Path) -> Result<String, SkillCatalogError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part
                    .to_str()
                    .ok_or_else(|| SkillCatalogError::InvalidAgentSkill {
                        detail: format!(
                            "skill resource path `{}` is not valid UTF-8",
                            path.display()
                        ),
                    })?;
                if part.is_empty() || part.contains(['/', '\\', '\0']) {
                    return Err(SkillCatalogError::InvalidAgentSkill {
                        detail: format!(
                            "skill resource path `{}` has an invalid component",
                            path.display()
                        ),
                    });
                }
                parts.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SkillCatalogError::InvalidAgentSkill {
                    detail: format!(
                        "skill resource path `{}` must stay inside the skill directory",
                        path.display()
                    ),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: "skill resource path must be non-empty".to_string(),
        });
    }
    let relative_path = parts.join("/");
    if relative_path == SKILL_FILE_NAME {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!("`{SKILL_FILE_NAME}` cannot also be a package resource"),
        });
    }
    Ok(relative_path)
}

fn validate_package_files(files: &[AgentSkillPackageFile]) -> Result<(), SkillCatalogError> {
    if files.len() > AGENT_SKILL_PACKAGE_MAX_FILES {
        return Err(SkillCatalogError::InvalidAgentSkill {
            detail: format!(
                "skill package exceeds the {AGENT_SKILL_PACKAGE_MAX_FILES} resource file limit"
            ),
        });
    }
    let mut seen = BTreeSet::new();
    let mut total_bytes = 0usize;
    for file in files {
        let normalized = portable_skill_relative_path(Path::new(&file.relative_path))?;
        if normalized != file.relative_path {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill resource path `{}` is not portable",
                    file.relative_path
                ),
            });
        }
        if !seen.insert(&file.relative_path) {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!("duplicate skill resource `{}`", file.relative_path),
            });
        }
        if file.bytes.len() > AGENT_SKILL_PACKAGE_MAX_FILE_BYTES {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill resource `{}` exceeds the {} byte file limit",
                    file.relative_path, AGENT_SKILL_PACKAGE_MAX_FILE_BYTES
                ),
            });
        }
        total_bytes = total_bytes.checked_add(file.bytes.len()).ok_or_else(|| {
            SkillCatalogError::InvalidAgentSkill {
                detail: "skill package size overflow".to_string(),
            }
        })?;
        if total_bytes > AGENT_SKILL_PACKAGE_MAX_TOTAL_BYTES {
            return Err(SkillCatalogError::InvalidAgentSkill {
                detail: format!(
                    "skill package exceeds the {} byte total limit",
                    AGENT_SKILL_PACKAGE_MAX_TOTAL_BYTES
                ),
            });
        }
    }
    Ok(())
}

fn validate_skill_resources(
    skill_path: &str,
    resources: &[SkillResourceDescriptor],
) -> Result<(), SkillCatalogError> {
    let package_root =
        parent_sifs_path(skill_path).ok_or_else(|| SkillCatalogError::InvalidCatalog {
            detail: format!("skill path `{skill_path}` has no package directory"),
        })?;
    let mut seen = BTreeSet::new();
    for resource in resources {
        let normalized = portable_skill_relative_path(Path::new(&resource.relative_path))?;
        if normalized != resource.relative_path {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "skill resource path `{}` is not portable",
                    resource.relative_path
                ),
            });
        }
        if !seen.insert(&resource.relative_path) {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!("duplicate skill resource `{}`", resource.relative_path),
            });
        }
        let expected_path = format!("{package_root}/{}", resource.relative_path);
        if resource.sifs_path != expected_path {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "skill resource `{}` must be stored at `{expected_path}`",
                    resource.relative_path
                ),
            });
        }
        validate_sifs_path(&resource.sifs_path, "skill resource SIFS path")?;
        validate_content_hash(&resource.content_hash)?;
        if resource.byte_len > AGENT_SKILL_PACKAGE_MAX_FILE_BYTES as u64 {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!(
                    "skill resource `{}` exceeds the file size limit",
                    resource.relative_path
                ),
            });
        }
    }
    Ok(())
}

fn default_skill_catalog_schema_version() -> u32 {
    SKILL_CATALOG_SCHEMA_VERSION
}

fn encode_catalog_sobj<T: Serialize + ?Sized>(
    path: &str,
    value: &T,
) -> Result<Vec<u8>, SkillCatalogError> {
    serde_ext::to_bytes(value, SKILL_CATALOG_SOBJ_VERSION, LengthMode::U32).map_err(|source| {
        SkillCatalogError::Object {
            path: path.to_string(),
            detail: source.to_string(),
        }
    })
}

fn decode_catalog_sobj<T: for<'de> Deserialize<'de>>(
    path: &str,
    bytes: &[u8],
) -> Result<T, SkillCatalogError> {
    serde_ext::from_bytes(bytes).map_err(|source| SkillCatalogError::Object {
        path: path.to_string(),
        detail: source.to_string(),
    })
}

fn parent_sifs_path(path: &str) -> Option<String> {
    let index = path.rfind('/')?;
    if index == 0 {
        Some("/".to_string())
    } else {
        Some(path[..index].to_string())
    }
}

fn path_token(value: &str) -> String {
    value.replace('.', "_")
}

fn validate_skill_token(value: &str, context: &str) -> Result<(), SkillCatalogError> {
    if value.trim().is_empty() {
        return Err(SkillCatalogError::InvalidToken {
            context: context.to_string(),
            value: value.to_string(),
            detail: "value must be non-empty".to_string(),
        });
    }
    if value != value.trim() {
        return Err(SkillCatalogError::InvalidToken {
            context: context.to_string(),
            value: value.to_string(),
            detail: "value must not have leading or trailing whitespace".to_string(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(SkillCatalogError::InvalidToken {
            context: context.to_string(),
            value: value.to_string(),
            detail: "value must not contain control characters".to_string(),
        });
    }
    if value.contains('/') || value.contains('\\') || value == "." || value == ".." {
        return Err(SkillCatalogError::InvalidToken {
            context: context.to_string(),
            value: value.to_string(),
            detail: "value must be a path-safe token".to_string(),
        });
    }
    Ok(())
}

fn validate_token_list(values: &[String], context: &str) -> Result<(), SkillCatalogError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_skill_token(value, context)?;
        if !seen.insert(value) {
            return Err(SkillCatalogError::InvalidCatalog {
                detail: format!("duplicate {context} `{value}`"),
            });
        }
    }
    Ok(())
}

fn validate_free_text(value: &str, context: &str) -> Result<(), SkillCatalogError> {
    if value.chars().any(|ch| ch == '\0') {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("{context} must not contain NUL bytes"),
        });
    }
    Ok(())
}

fn validate_optional_free_text(
    value: &Option<String>,
    context: &str,
) -> Result<(), SkillCatalogError> {
    if let Some(value) = value {
        validate_free_text(value, context)?;
    }
    Ok(())
}

fn validate_sifs_path(value: &str, context: &str) -> Result<(), SkillCatalogError> {
    if !value.starts_with('/') {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("{context} must be an absolute SIFS path"),
        });
    }
    if value.contains('\0') || value.split('/').any(|part| part == "..") {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("{context} contains an invalid path component"),
        });
    }
    Ok(())
}

fn validate_content_hash(value: &str) -> Result<(), SkillCatalogError> {
    let Some(hash) = value.strip_prefix("blake3:") else {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("content hash `{value}` must use blake3: prefix"),
        });
    };
    if hash.len() != 64 || !hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(SkillCatalogError::InvalidCatalog {
            detail: format!("content hash `{value}` is not a blake3 hex digest"),
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SkillCatalogError {
    #[error("invalid skill catalog: {detail}")]
    InvalidCatalog { detail: String },
    #[error("invalid Agent Skills package: {detail}")]
    InvalidAgentSkill { detail: String },
    #[error("invalid {context} `{value}`: {detail}")]
    InvalidToken {
        context: String,
        value: String,
        detail: String,
    },
    #[error("missing skill version {skill_id}@{version}")]
    MissingSkillVersion { skill_id: String, version: String },
    #[error("skill {skill_id} content hash mismatch: expected {expected}, got {actual}")]
    ContentHashMismatch {
        skill_id: String,
        expected: String,
        actual: String,
    },
    #[error("skill resource {path} content hash mismatch: expected {expected}, got {actual}")]
    ResourceHashMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("invalid skill text at {path}: {detail}")]
    InvalidSkillText { path: String, detail: String },
    #[error("skill catalog store error at {path}: {detail}")]
    Store { path: String, detail: String },
    #[error("skill catalog object error at {path}: {detail}")]
    Object { path: String, detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_skill_frontmatter_drives_import_metadata_without_rewriting_text() {
        let text = "---\nname: rust-debug\ndescription: \"Debug Rust failures. Use for failing Rust tests.\"\nlicense: Apache-2.0\ncompatibility: Requires cargo.\nallowed-tools: Bash(cargo:*) Read\nmetadata:\n  example.org/owner: compiler-team\n---\n\n# Rust Debug\nRead the assertion first.\n";
        let write = SkillCatalogWrite::from_markdown_skill(
            None,
            "1.0.0",
            SkillCatalogSource::user("user.test"),
            text,
        )
        .expect("markdown write");

        assert_eq!(write.entry.skill_id, "rust-debug");
        assert_eq!(
            write.entry.description,
            "Debug Rust failures. Use for failing Rust tests."
        );
        assert_eq!(write.entry.license.as_deref(), Some("Apache-2.0"));
        assert_eq!(
            write.entry.compatibility.as_deref(),
            Some("Requires cargo.")
        );
        assert_eq!(
            write.entry.allowed_tools.as_deref(),
            Some("Bash(cargo:*) Read")
        );
        assert_eq!(
            write
                .entry
                .metadata
                .get("example.org/owner")
                .map(String::as_str),
            Some("compiler-team")
        );
        assert_eq!(write.text, text);
        assert_eq!(write.entry.content_hash, skill_content_hash(text));
        let document = parse_skill_markdown(text).expect("standard skill");
        assert_eq!(document.name(), "rust-debug");
        assert_eq!(document.body, "# Rust Debug\nRead the assertion first.");
    }

    #[test]
    fn markdown_skill_import_rejects_plain_markdown_and_name_mismatches() {
        let text = "# Plain Skill\nFollow plain markdown.";
        let error = SkillCatalogWrite::from_markdown_skill(
            Some("plain-skill".to_string()),
            "1.0.0",
            SkillCatalogSource::agent("agent.test"),
            text,
        )
        .expect_err("plain markdown must fail");
        assert!(matches!(error, SkillCatalogError::InvalidAgentSkill { .. }));

        let error = SkillCatalogWrite::from_markdown_skill(
            Some("other-skill".to_string()),
            "1.0.0",
            SkillCatalogSource::agent("agent.test"),
            "---\nname: plain-skill\ndescription: Follow plain markdown. Use for plain tasks.\n---\n\n# Plain Skill",
        )
        .expect_err("mismatched id must fail");
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn export_markdown_preserves_valid_agent_skills_document() {
        let text = "---\nname: plain-skill\ndescription: Follow plain markdown. Use for plain tasks.\n---\n\n# Plain Skill\nFollow plain markdown.";
        let entry = SkillCatalogEntry::new(
            "plain-skill",
            "1.0.0",
            SkillCatalogSource::agent("agent.test"),
            text,
        )
        .with_description("Plain portable skill.");
        let rendered = render_common_skill_markdown(&entry, text).expect("render");

        assert_eq!(rendered, text);
    }

    #[test]
    fn agent_skill_directory_resources_round_trip_through_catalog_and_export() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-package-round-trip");
        let source = root.path().join("source").join("pdf-processing");
        std::fs::create_dir_all(source.join("scripts")).expect("scripts");
        std::fs::create_dir_all(source.join("references")).expect("references");
        let markdown = "---\nname: pdf-processing\ndescription: Extract PDF text. Use when handling PDF documents.\nmetadata:\n  example.org/version: \"1.0\"\n---\n\n# PDF Processing\nRun `scripts/extract.py`.\n";
        std::fs::write(source.join(SKILL_FILE_NAME), markdown).expect("skill");
        std::fs::write(source.join("scripts").join("extract.py"), b"print('pdf')\n")
            .expect("script");
        std::fs::write(
            source.join("references").join("formats.md"),
            b"# Formats\nPDF.\n",
        )
        .expect("reference");

        let package = read_agent_skill_package(&source).expect("read package");
        assert_eq!(
            package
                .files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["references/formats.md", "scripts/extract.py"]
        );

        let store = LocalSkillCatalogStore::new(root.path().join("catalog"));
        let write = SkillCatalogWrite::from_agent_skill_package(
            None,
            "1.0.0",
            SkillCatalogSource::user("user.test"),
            package.clone(),
        )
        .expect("package write");
        assert_ne!(write.entry.content_hash, skill_content_hash(markdown));
        let entry = write.entry.clone();
        store.write_skill_version(write).expect("store package");
        let stored = store
            .read_skill_package(&entry)
            .expect("read stored package")
            .expect("stored package");
        assert_eq!(stored, package);

        let export_dir = root.path().join("export").join("pdf-processing");
        write_agent_skill_package(&export_dir, &stored).expect("export package");
        assert_eq!(
            std::fs::read(export_dir.join("scripts").join("extract.py")).expect("exported script"),
            b"print('pdf')\n"
        );
    }

    #[test]
    fn agent_skill_directory_name_must_match_frontmatter_name() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-directory-name");
        let source = root.path().join("wrong-name");
        std::fs::create_dir_all(&source).expect("source");
        std::fs::write(
            source.join(SKILL_FILE_NAME),
            "---\nname: right-name\ndescription: Check names. Use for name validation.\n---\n",
        )
        .expect("skill");

        let error = read_agent_skill_package(&source).expect_err("directory mismatch");
        assert!(error.to_string().contains("must match"));
    }

    #[test]
    fn local_store_writes_catalog_skill_and_activation() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-catalog-write");
        let store = LocalSkillCatalogStore::new(root.path());
        let text = "# Debug Rust\nInspect the first failing assertion.";
        let entry = SkillCatalogEntry::new(
            "skill.rust.debug",
            "1.0.0",
            SkillCatalogSource::sinomo("1.0.0"),
            text,
        )
        .with_description("Debug Rust tests.");
        let catalog = store
            .write_skill_version(
                SkillCatalogWrite::new(entry.clone(), text).activate_for_scope(
                    SkillActivationScope::Cluster.as_str(),
                    "test",
                    "built-in default",
                ),
            )
            .expect("write skill");

        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.activations.len(), 1);
        let reloaded = store.load_catalog().expect("reload catalog");
        let active = reloaded
            .active_entry_for_scope(SkillActivationScope::Cluster.as_str(), "skill.rust.debug")
            .expect("active skill");
        assert_eq!(active.content_hash, skill_content_hash(text));
        let read = store
            .read_skill_text(active)
            .expect("read skill")
            .expect("skill text");
        assert_eq!(read, text);
        assert!(
            store
                .read_file(&skill_active_sifs_path("cluster", "skill.rust.debug"))
                .expect("read active pointer")
                .is_some()
        );
    }

    #[test]
    fn catalog_rejects_duplicate_entries_and_missing_activation() {
        let text = "# Skill";
        let entry = SkillCatalogEntry::new(
            "skill.test",
            "1.0.0",
            SkillCatalogSource::user("user.test"),
            text,
        );
        let mut catalog = SkillCatalog {
            entries: vec![entry.clone(), entry],
            ..SkillCatalog::default()
        };
        assert!(catalog.validate().is_err());

        catalog.entries.pop();
        catalog.activations.push(SkillActivationRecord {
            scope: "cluster".to_string(),
            skill_id: "skill.test".to_string(),
            version: "2.0.0".to_string(),
            content_hash: skill_content_hash(text),
            sifs_path: "/services/sinomo/skills/missing/SKILL.md".to_string(),
            activated_by: "test".to_string(),
            reason: "missing".to_string(),
        });
        assert!(catalog.validate().is_err());
    }

    #[test]
    fn conflict_record_is_stable_for_current_and_incoming_hashes() {
        let current_text = "# Current";
        let incoming_text = "# Incoming";
        let current = SkillCatalogEntry::new(
            "skill.merge",
            "1.0.0-evolved",
            SkillCatalogSource::evolution("evojob_1"),
            current_text,
        )
        .with_base_hash(skill_content_hash("# Base"));
        let incoming = SkillCatalogEntry::new(
            "skill.merge",
            "1.1.0",
            SkillCatalogSource::sinomo("1.1.0"),
            incoming_text,
        );
        let conflict = SkillConflictRecord::new(&current, &incoming);
        let same = SkillConflictRecord::new(&current, &incoming);
        assert_eq!(conflict.conflict_id, same.conflict_id);
        assert_eq!(conflict.base_hash, current.base_hash);
        conflict.validate().expect("valid conflict");
    }

    #[test]
    fn module_embedded_skill_resource_materializes_catalog_write() {
        let skill_text = "---\nname: module-debug\ndescription: Debug with module guidance.\n---\n\n# Module skill\nUse module-specific steps.";
        let module_id =
            sinomo_runtime::core::Id::from_legacy_named("module.skill.test").expect("module id");
        let registration = ModuleRegistration::native_rust(
            module_id.clone(),
            "2.0.0",
            "skill-module",
            "sinomo-module-test",
            "module-api",
            sinomo_runtime::core::TrustRing::R1TrustedRuntime,
            "module.skill.test",
        );
        let resource =
            ModuleDeclarativeResourceExport::skill("module-debug", "skills/module-debug/SKILL.md")
                .with_description("Debug with module guidance.")
                .with_target_agent("coding-agent")
                .with_activation_tag("debug")
                .with_embedded_text(skill_text);
        let write = SkillCatalogWrite::from_embedded_module_resource(&registration, &resource)
            .expect("embedded resource")
            .expect("catalog write");

        assert_eq!(write.entry.skill_id, "module-debug");
        assert_eq!(write.entry.version, "2.0.0");
        assert_eq!(write.entry.source.kind, SkillCatalogSourceKind::Module);
        assert_eq!(
            write.entry.source.source_id.as_deref(),
            Some(module_id.as_str())
        );
        assert_eq!(write.entry.target_agents, vec!["coding-agent"]);
        assert_eq!(write.entry.activation_tags, vec!["debug"]);
        assert!(write.entry.sifs_path.contains("/sources/modules/"));
        assert_eq!(write.entry.content_hash, skill_content_hash(&write.text));
    }

    #[test]
    fn reconciliation_materializes_path_backed_module_skill() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-catalog-module-reconcile");
        let store = LocalSkillCatalogStore::new(root.path().join("catalog"));
        let payload_root = root.path().join("payload");
        let skill_text = "---\nname: module-debug\ndescription: Debug with module guidance.\n---\n\n# Module skill\nUse module-specific steps.";
        let skill_path = payload_root
            .join("skills")
            .join("module-debug")
            .join("SKILL.md");
        std::fs::create_dir_all(skill_path.parent().expect("skill parent"))
            .expect("create payload");
        std::fs::write(&skill_path, skill_text).expect("write payload skill");
        let module_id =
            sinomo_runtime::core::Id::from_legacy_named("module.skill.test").expect("module id");
        let registration = ModuleRegistration::native_rust(
            module_id.clone(),
            "2.0.0",
            "skill-module",
            "sinomo-module-test",
            "module-api",
            sinomo_runtime::core::TrustRing::R1TrustedRuntime,
            "module.skill.test",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_declarative_resource(
                ModuleDeclarativeResourceExport::skill(
                    "module-debug",
                    "skills/module-debug/SKILL.md",
                )
                .with_description("Debug with module guidance.")
                .with_target_agent("coding-agent")
                .with_activation_tag("debug"),
            ),
        );

        let report = reconcile_module_skills_to_catalog(
            &store,
            &registration,
            Some(&payload_root),
            Some(SkillActivationScope::Cluster.as_str()),
            "test",
            "module reconciliation",
        )
        .expect("reconcile");

        assert_eq!(report.scanned, 1);
        assert_eq!(report.materialized, 1);
        assert_eq!(report.activated, 1);
        assert_eq!(report.conflicts, 0);
        let catalog = store.load_catalog().expect("catalog");
        let active = catalog
            .active_entry_for_scope(SkillActivationScope::Cluster.as_str(), "module-debug")
            .expect("active module skill");
        assert_eq!(active.version, "2.0.0");
        assert_eq!(active.source.source_id.as_deref(), Some(module_id.as_str()));
        assert_eq!(active.target_agents, vec!["coding-agent"]);
        let text = store
            .read_skill_text(active)
            .expect("read skill")
            .expect("skill text");
        assert_eq!(text, skill_text);
    }

    #[test]
    fn active_declarative_module_reconciliation_scans_loader_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-catalog-module-scan");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(root.path().join("data"));
        let store = LocalSkillCatalogStore::new(root.path().join("catalog"));
        let module_id = "module.skill.scan";
        let version = "3.0.0";
        let content_hash =
            "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parsed_module_id =
            sinomo_runtime::core::Id::from_legacy_named(module_id).expect("module id");
        let payload_ref = sinomo_runtime::module::declarative_module_payload_ref(
            &parsed_module_id,
            version,
            content_hash,
        );
        let registration = sinomo_runtime::module::DeclarativeModulePackageManifest {
            module_id: module_id.to_string(),
            version: version.to_string(),
            owner: "skill-module".to_string(),
            api_version: "sinomo.module.declarative.v1".to_string(),
            target_ring: sinomo_runtime::core::TrustRing::R3SandboxedModule,
            capabilities: sinomo_runtime::core::ModuleCapabilities::default(),
            surfaces: sinomo_runtime::module::ModuleSurfaces::default().with_declarative_resource(
                ModuleDeclarativeResourceExport::skill(
                    "module-scan",
                    "skills/module-scan/SKILL.md",
                ),
            ),
            resources: Vec::new(),
        }
        .into_registration(payload_ref.clone())
        .expect("registration");
        let payload_path = sinomo_runtime::module::declarative_module_payload_cache_path(
            &data_dir.root(),
            &parsed_module_id,
            version,
            content_hash,
        );
        let skill_path = payload_path
            .join("skills")
            .join("module-scan")
            .join("SKILL.md");
        std::fs::create_dir_all(skill_path.parent().expect("skill parent"))
            .expect("create payload");
        std::fs::write(
            &skill_path,
            "---\nname: module-scan\ndescription: Follow scan steps.\n---\n\n# Declarative module skill\nFollow scan steps.",
        )
        .expect("write payload skill");
        sinomo_runtime::module::write_declarative_module_registration(
            &data_dir.root(),
            &registration,
        )
        .expect("write registration");
        sinomo_runtime::module::write_declarative_module_loader_state(
            &data_dir.root(),
            &DeclarativeModuleLoaderState {
                module_id: parsed_module_id,
                version: version.to_string(),
                content_hash: content_hash.to_string(),
                payload_ref,
                active: true,
                installed_at: "test".to_string(),
                source: "test".to_string(),
            },
        )
        .expect("write state");

        let report = reconcile_active_declarative_module_skills_to_catalog(&store, &data_dir)
            .expect("reconcile active modules");

        assert_eq!(report.scanned, 1);
        assert_eq!(report.materialized, 1);
        assert_eq!(report.activated, 1);
        assert_eq!(report.conflicts, 0);
        let catalog = store.load_catalog().expect("catalog");
        let active = catalog
            .active_entry_for_scope(SkillActivationScope::Cluster.as_str(), "module-scan")
            .expect("active module skill");
        assert_eq!(active.version, version);
    }

    #[test]
    fn reconciliation_records_conflict_without_replacing_evolved_skill() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-catalog-module-conflict");
        let store = LocalSkillCatalogStore::new(root.path().join("catalog"));
        let base_text = "---\nname: module-debug\ndescription: Debug with module guidance.\n---\n\n# Module skill\nBase guidance.";
        let evolved_text = "---\nname: module-debug\ndescription: Debug with module guidance.\n---\n\n# Module skill\nEvolved guidance.";
        let evolved = SkillCatalogEntry::new(
            "module-debug",
            "1.0.0-evolved",
            SkillCatalogSource::evolution("evojob_1"),
            evolved_text,
        )
        .with_base_hash(skill_content_hash(base_text));
        store
            .write_skill_version(
                SkillCatalogWrite::new(evolved.clone(), evolved_text).activate_for_scope(
                    SkillActivationScope::Cluster.as_str(),
                    "evolution",
                    "test override",
                ),
            )
            .expect("write evolved skill");
        let module_id =
            sinomo_runtime::core::Id::from_legacy_named("module.skill.test").expect("module id");
        let registration = ModuleRegistration::native_rust(
            module_id,
            "1.1.0",
            "skill-module",
            "sinomo-module-test",
            "module-api",
            sinomo_runtime::core::TrustRing::R1TrustedRuntime,
            "module.skill.test",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_declarative_resource(
                ModuleDeclarativeResourceExport::skill(
                    "module-debug",
                    "skills/module-debug/SKILL.md",
                )
                .with_embedded_text(
                    "---\nname: module-debug\ndescription: Debug with module guidance.\n---\n\n# Module skill\nUpstream changed.",
                ),
            ),
        );

        let report = reconcile_module_skills_to_catalog(
            &store,
            &registration,
            None,
            Some(SkillActivationScope::Cluster.as_str()),
            "test",
            "module reconciliation",
        )
        .expect("reconcile");

        assert_eq!(report.scanned, 1);
        assert_eq!(report.materialized, 1);
        assert_eq!(report.activated, 0);
        assert_eq!(report.conflicts, 1);
        let catalog = store.load_catalog().expect("catalog");
        let active = catalog
            .active_entry_for_scope(SkillActivationScope::Cluster.as_str(), "module-debug")
            .expect("active skill");
        assert_eq!(active.source.kind, SkillCatalogSourceKind::Evolution);
        assert_eq!(active.version, "1.0.0-evolved");
        assert!(
            catalog
                .entry("module-debug", "1.1.0")
                .is_some_and(|entry| entry.source.kind == SkillCatalogSourceKind::Module)
        );
        assert_eq!(catalog.conflicts.len(), 1);
        assert_eq!(catalog.conflicts[0].current_version, "1.0.0-evolved");
        assert_eq!(catalog.conflicts[0].incoming_version, "1.1.0");
    }

    #[test]
    fn upstream_update_decision_conflicts_for_evolved_override() {
        let base = "# Base";
        let current = SkillCatalogEntry::new(
            "skill.merge",
            "1.0.0-evolved",
            SkillCatalogSource::evolution("evojob_1"),
            "# Evolved",
        )
        .with_base_hash(skill_content_hash(base));
        let incoming = SkillCatalogEntry::new(
            "skill.merge",
            "1.1.0",
            SkillCatalogSource::sinomo("1.1.0"),
            "# Upstream changed",
        );
        let catalog = SkillCatalog::default();
        let decision = catalog
            .upstream_update_decision(&current, &incoming)
            .expect("decision");

        match decision {
            SkillUpstreamUpdateDecision::AttemptThreeWayMerge { conflict } => {
                assert_eq!(conflict.skill_id, "skill.merge");
                assert_eq!(conflict.current_version, "1.0.0-evolved");
                assert_eq!(conflict.incoming_version, "1.1.0");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }
}
