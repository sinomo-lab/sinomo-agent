//! Stable app-facing skill catalog contracts.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const SKILL_RPC_CATEGORY: &str = "skill.app";
pub const SKILL_ROUTE_AUTH_SCOPE: &str = "local-command";
pub const SKILL_LIST_ROUTE: &str = "/skill/app/list";
pub const SKILL_DETAIL_ROUTE: &str = "/skill/app/detail";
pub const SKILL_IMPORT_ROUTE: &str = "/skill/app/import";
pub const SKILL_EXPORT_ROUTE: &str = "/skill/app/export";
pub const SKILL_ACTIVATE_ROUTE: &str = "/skill/app/activate";
pub const SKILL_RPC_ROUTES: &[&str] = &[
    SKILL_LIST_ROUTE,
    SKILL_DETAIL_ROUTE,
    SKILL_IMPORT_ROUTE,
    SKILL_EXPORT_ROUTE,
    SKILL_ACTIVATE_ROUTE,
];
pub const SKILL_READ_CAPABILITY: &str = "skill.app.read";
pub const SKILL_WRITE_CAPABILITY: &str = "skill.app.write";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillSummary {
    pub id: String,
    pub version: String,
    pub description: String,
    pub source_kind: String,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    pub active: bool,
    pub status: String,
    pub sifs_path: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillDetail {
    pub summary: SkillSummary,
    pub manifest_path: String,
    pub base_hash: Option<String>,
    pub license: Option<String>,
    pub compatibility: Option<String>,
    pub allowed_tools: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub resources: Vec<String>,
    pub target_agents: Vec<String>,
    pub model_families: Vec<String>,
    pub activation_tags: Vec<String>,
    pub required_capabilities: Vec<String>,
    pub text: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillMutationResult {
    pub status: String,
    pub skill: SkillSummary,
    pub path: Option<String>,
    pub scope: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillListRequest {
    pub include_all: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillListResponse {
    pub skills: Vec<SkillSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillDetailRequest {
    pub skill_id: String,
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillDetailResponse {
    pub detail: SkillDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillImportRequest {
    pub path: PathBuf,
    pub skill_id: Option<String>,
    pub version: Option<String>,
    pub activate: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillExportRequest {
    pub skill_id: String,
    pub version: Option<String>,
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillActivateRequest {
    pub skill_id: String,
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillMutationResponse {
    pub result: SkillMutationResult,
}
