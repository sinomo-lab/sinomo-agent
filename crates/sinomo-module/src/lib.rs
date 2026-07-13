//! Module registry, routing, loader state, and native module host glue.

mod cli;
mod native_host;

pub use cli::{
    ModuleCliCommandConfig, ModuleCliCommandHandler, ModuleNativeRustToggleTarget,
    ModuleWasixCliConfig, module_cli_command_handler,
};
pub use native_host::*;

use arc_swap::ArcSwap;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fs2::FileExt;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::events::{EventId, EventStore, EventStoreError, StoredEvent};
use sinomo_core::node::{NodeId, NodeIdentity};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::{
    ArtifactManifest, CapabilityName, Confidence, EventEnvelope, EventKind, HotSwapState,
    LatencyClass, ModuleCapabilities, ModuleKind, ModuleLoaderState, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source, TrustRing,
};
use sinomo_module_api::{
    NativeModuleDeclarativeResource, NativeModuleDescriptor, NativeModuleObjectRpcRoute,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub type ArtifactId = sinomo_core::Id;
pub type ModuleId = sinomo_core::Id;
pub type ModuleActivationId = sinomo_core::Id;

pub const ARTIFACT_SIGNATURE_PROTOCOL: &str = "sinomo.artifact-manifest-signature.v1";
pub const LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME: &str = "local-ed25519";
pub const MODULE_OBJECT_VERSION: u16 = 1;
pub const NATIVE_DYLIB_HOST_ABI_VERSION: u32 = sinomo_module_api::SINOMO_NATIVE_MODULE_ABI_VERSION;
pub const DEFAULT_LOCAL_COMMAND_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_LOCAL_COMMAND_OUTPUT_LIMIT_BYTES: usize = 128 * 1024;
pub const SINOMO_MODULE_V1_INTERFACE_VERSION: &str = "sinomo-module-v1";
pub const SINOMO_MODULE_V1_WIT: &str = include_str!("../wit/sinomo-module-v1.wit");
pub const MODULE_INVOCATION_DECISION_EVENT_KIND: &str = "module.invocation.decision";
pub const MODULE_SHADOW_DECISION_EVENT_KIND: &str = "module.shadow.decision";
pub const MODULE_USER_CORRECTION_EVENT_KIND: &str = "module.user.correction";
pub const MODULE_CRASH_EVENT_KIND: &str = "module.crash";
pub const MODULE_TIMEOUT_EVENT_KIND: &str = "module.timeout";
pub const MODULE_POLICY_BLOCKED_EVENT_KIND: &str = "module.policy.blocked";
pub const DECLARATIVE_MODULE_API_VERSION: &str = "sinomo.declarative.module.v1";
pub const MODULE_RPC_CATEGORY: &str = "module";
pub const MODULE_NATIVE_DYLIB_CANCEL_ROUTE: &str = "/module/native-dylib/cancel";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinomoModuleV1Context {
    #[serde(rename = "node-id", alias = "node_id")]
    pub node_id: String,
    #[serde(rename = "module-id", alias = "module_id")]
    pub module_id: String,
    #[serde(rename = "module-version", alias = "module_version")]
    pub module_version: String,
    #[serde(rename = "budget-ms", alias = "budget_ms")]
    pub budget_ms: u64,
    #[serde(rename = "capabilities-json", alias = "capabilities_json")]
    pub capabilities_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinomoModuleV1Result {
    pub status: String,
    #[serde(rename = "events-json", alias = "events_json")]
    pub events_json: Vec<String>,
    #[serde(rename = "metrics-json", alias = "metrics_json")]
    pub metrics_json: String,
}

impl SinomoModuleV1Result {
    pub fn from_json_output(output: &str) -> Result<Self, SinomoModuleV1ResultError> {
        let result: Self = serde_json::from_str(output).map_err(|source| {
            SinomoModuleV1ResultError::InvalidJson {
                detail: source.to_string(),
            }
        })?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), SinomoModuleV1ResultError> {
        if self.status.trim().is_empty() {
            return Err(SinomoModuleV1ResultError::MissingStatus);
        }
        for (index, event_json) in self.events_json.iter().enumerate() {
            serde_json::from_str::<serde_json::Value>(event_json).map_err(|source| {
                SinomoModuleV1ResultError::InvalidEventJson {
                    index,
                    detail: source.to_string(),
                }
            })?;
        }
        serde_json::from_str::<serde_json::Value>(&self.metrics_json).map_err(|source| {
            SinomoModuleV1ResultError::InvalidMetricsJson {
                detail: source.to_string(),
            }
        })?;
        Ok(())
    }

    pub fn to_json_output(&self) -> Result<String, SinomoModuleV1ResultError> {
        self.validate()?;
        serde_json::to_string(self).map_err(|source| SinomoModuleV1ResultError::Serialize {
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleLoaderRuntimeState {
    pub node_id: NodeId,
    pub activation_id: ModuleActivationId,
    pub module_id: ModuleId,
    pub loader_state: ModuleLoaderState,
    pub traffic_weight_bps: u16,
    pub last_loader_action: String,
    pub last_loader_stage: String,
    pub last_transition_record_id: sinomo_core::Id,
    pub reason: String,
    pub applied_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeDylibModuleLoaderState {
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    pub content_hash: String,
    pub cache_key: String,
    pub library_name: String,
    pub platform_triple: String,
    pub native_abi_version: u32,
    pub interface_version: String,
    pub activated_at: String,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDylibModuleCacheEntry {
    pub module_id: ModuleId,
    pub content_hash: String,
    pub cache_key: String,
    pub library_name: String,
    pub platform_triple: String,
    pub native_abi_version: u32,
    pub interface_version: String,
    pub library_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDylibModuleCachePruneReport {
    pub retained_active_variants: usize,
    pub retained_inactive_variants: usize,
    pub removed_inactive_variants: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustModuleRuntimeState {
    pub module_id: ModuleId,
    pub enabled: bool,
    pub updated_at: String,
    pub source: String,
    pub reason: String,
}

pub const MODULE_CLUSTER_CONFIG_KEY: &str = "module.runtime.cluster-defaults";
pub const MODULE_NODE_CONFIG_KEY: &str = "module.runtime.node-overrides";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRuntimeConfig {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub modules: BTreeMap<String, ModuleRuntimeConfigEntry>,
}

impl ModuleRuntimeConfig {
    pub fn module_ids(&self) -> impl Iterator<Item = &str> {
        self.modules.keys().map(String::as_str)
    }

    pub fn setting(&self, module_id: &str) -> Option<ModuleRuntimeConfigEntry> {
        self.modules.get(module_id).copied()
    }

    pub fn set(
        &mut self,
        module_id: impl AsRef<str>,
        setting: ModuleRuntimeConfigEntry,
    ) -> Result<(), String> {
        let module_id = normalize_module_config_id(module_id.as_ref())?;
        self.modules.insert(module_id, setting);
        Ok(())
    }

    pub fn clear(&mut self, module_id: impl AsRef<str>) -> Result<(), String> {
        let module_id = normalize_module_config_id(module_id.as_ref())?;
        self.modules.remove(&module_id);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRuntimeConfigEntry {
    pub enabled: bool,
}

impl ModuleRuntimeConfigEntry {
    pub fn enabled(enabled: bool) -> Self {
        Self { enabled }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRuntimeConfigSource {
    Node,
    Cluster,
    DisabledDefault,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResolvedModuleRuntimeConfig {
    pub enabled: bool,
    pub source: ModuleRuntimeConfigSource,
}

pub fn resolve_module_runtime_config(
    node: &ModuleRuntimeConfig,
    cluster: &ModuleRuntimeConfig,
    module_id: &str,
) -> ResolvedModuleRuntimeConfig {
    if let Some(setting) = node.setting(module_id) {
        return ResolvedModuleRuntimeConfig {
            enabled: setting.enabled,
            source: ModuleRuntimeConfigSource::Node,
        };
    }
    if let Some(setting) = cluster.setting(module_id) {
        return ResolvedModuleRuntimeConfig {
            enabled: setting.enabled,
            source: ModuleRuntimeConfigSource::Cluster,
        };
    }
    ResolvedModuleRuntimeConfig {
        enabled: false,
        source: ModuleRuntimeConfigSource::DisabledDefault,
    }
}

pub fn module_runtime_config_schema() -> sinomo_config::ConfigSchema {
    sinomo_config::ConfigSchema::object(
        "ModuleRuntimeConfig",
        "Runtime module enablement keyed by module id.",
        vec![
            sinomo_config::ConfigSchemaField::new(
                "modules",
                "Modules",
                "Configured module runtime states. Missing module ids inherit the next config layer.",
                sinomo_config::ConfigSchemaValue::map(
                    sinomo_config::ConfigSchemaValue::object(vec![
                        sinomo_config::ConfigSchemaField::new(
                            "enabled",
                            "Enabled",
                            "Whether this module should run when this config layer applies.",
                            sinomo_config::ConfigSchemaValue::bool(),
                        ),
                    ])
                    .without_unknown_fields(),
                ),
            )
            .optional(),
        ],
    )
}

pub fn module_cluster_config_definition() -> Result<sinomo_config::ConfigDefinition, String> {
    sinomo_config::ConfigDefinition::new(
        MODULE_CLUSTER_CONFIG_KEY,
        "sinomo-module",
        sinomo_config::ConfigValueType::Object {
            schema_name: "ModuleRuntimeConfig".to_string(),
        },
        "Cluster-level default module runtime enablement. Node-local overrides take priority.",
        sinomo_config::ConfigClusterMode::ClusterOnly,
    )
    .map_err(|error| error.to_string())?
    .with_default(&ModuleRuntimeConfig::default())
    .map_err(|error| error.to_string())?
    .with_schema(module_runtime_config_schema())
    .map_err(|error| error.to_string())
}

pub fn module_node_config_definition() -> Result<sinomo_config::ConfigDefinition, String> {
    sinomo_config::ConfigDefinition::new(
        MODULE_NODE_CONFIG_KEY,
        "sinomo-module",
        sinomo_config::ConfigValueType::Object {
            schema_name: "ModuleRuntimeConfig".to_string(),
        },
        "Node-local module runtime enablement overrides. Missing modules inherit cluster defaults.",
        sinomo_config::ConfigClusterMode::LocalOnly,
    )
    .map_err(|error| error.to_string())?
    .with_default(&ModuleRuntimeConfig::default())
    .map_err(|error| error.to_string())?
    .with_schema(module_runtime_config_schema())
    .map_err(|error| error.to_string())
}

pub fn register_config_definitions(
    builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    let builder = builder
        .register(module_cluster_config_definition()?)
        .map_err(|error| error.to_string())?;
    builder
        .register(module_node_config_definition()?)
        .map_err(|error| error.to_string())
}

fn normalize_module_config_id(module_id: &str) -> Result<String, String> {
    let module_id = module_id.trim();
    if module_id.is_empty() {
        return Err("module id must not be empty".to_string());
    }
    if ModuleId::new(module_id).is_ok() || ModuleId::from_legacy_named(module_id).is_ok() {
        Ok(module_id.to_string())
    } else {
        Err(format!("invalid module id `{module_id}`"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDylibModuleInstallRequest {
    pub expected_module_id: Option<ModuleId>,
    pub artifact_id: Option<ArtifactId>,
    pub expected_interface_version: Option<String>,
    pub cache_key: Option<String>,
    pub library_name: Option<String>,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDylibModuleInstallResult {
    pub descriptor_module_id: String,
    pub state: NativeDylibModuleLoaderState,
    pub cache_path: PathBuf,
    pub state_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeclarativeModuleLoaderState {
    pub module_id: ModuleId,
    pub version: String,
    pub content_hash: String,
    pub payload_ref: String,
    pub active: bool,
    pub installed_at: String,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclarativeModuleInstallRequest {
    pub source: String,
    pub activate: bool,
}

impl DeclarativeModuleInstallRequest {
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            activate: true,
        }
    }

    pub fn inactive(mut self) -> Self {
        self.activate = false;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclarativeModuleInstallResult {
    pub registration: ModuleRegistration,
    pub state: DeclarativeModuleLoaderState,
    pub payload_path: PathBuf,
    pub catalog_path: PathBuf,
    pub state_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeclarativeModulePackageManifest {
    pub module_id: String,
    pub version: String,
    #[serde(default = "default_declarative_module_owner")]
    pub owner: String,
    #[serde(default = "default_declarative_module_api_version")]
    pub api_version: String,
    #[serde(default = "default_declarative_module_target_ring")]
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "ModuleCapabilities::is_empty")]
    pub capabilities: ModuleCapabilities,
    #[serde(default, skip_serializing_if = "ModuleSurfaces::is_empty")]
    pub surfaces: ModuleSurfaces,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ModuleDeclarativeResourceExport>,
}

impl DeclarativeModulePackageManifest {
    pub fn into_registration(
        mut self,
        payload_ref: impl Into<String>,
    ) -> Result<ModuleRegistration, DeclarativeModuleInstallError> {
        let module_id = parse_declarative_module_id(&self.module_id)?;
        let payload_ref = payload_ref.into();
        if self.version.trim().is_empty() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: "module version must be non-empty".to_string(),
            });
        }
        if self.version != self.version.trim() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: "module version must not have leading or trailing whitespace".to_string(),
            });
        }
        validate_declarative_module_version(&self.version)?;
        if self.owner.trim().is_empty() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: "module owner must be non-empty".to_string(),
            });
        }
        if self.api_version.trim().is_empty() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: "module api_version must be non-empty".to_string(),
            });
        }
        for resource in self.resources {
            self.surfaces.declarative_resources.push(resource);
        }
        self.surfaces
            .validate()
            .map_err(|source| DeclarativeModuleInstallError::Descriptor {
                detail: source.to_string(),
            })?;
        let mut manifest = sinomo_core::ModuleManifest::declarative(
            module_id,
            self.version,
            self.owner,
            self.api_version,
            self.target_ring,
        );
        manifest.capabilities = self.capabilities;
        Ok(ModuleRegistration {
            manifest,
            backend: ModuleBackendRegistration::Declarative { payload_ref },
            surfaces: self.surfaces,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleUnloadRecord {
    pub module_id: ModuleId,
    pub backend: ModuleKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_name: Option<String>,
    pub state_removed: bool,
    pub runtime_unload_attempted: bool,
    pub runtime_unload_succeeded: bool,
    pub runtime_unload_detail: String,
    pub unloaded_at: String,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ModuleLocalCommandRegistry {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ModuleLocalCommandArtifact>,
}

impl ModuleLocalCommandRegistry {
    pub fn artifact(&self, artifact_id: &ArtifactId) -> Option<&ModuleLocalCommandArtifact> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.artifact_id == *artifact_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleLocalCommandArtifact {
    pub artifact_id: ArtifactId,
    pub program: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRouteRequest {
    pub module_id: ModuleId,
    pub route_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRouteDecision {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub primary_artifact_id: ArtifactId,
    pub primary_lane: ModuleRouteLane,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<ArtifactId>,
    pub traffic_weight_bps: u16,
    pub route_bucket_bps: u16,
    pub hot_swap_state: HotSwapState,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRouteLane {
    Active,
    Candidate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInvocationRequest {
    pub module_id: ModuleId,
    pub route_key: String,
    pub input: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleArtifactInvocation {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub artifact_id: ArtifactId,
    pub lane: ModuleInvocationLane,
    pub input: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleInvocationLane {
    Active,
    Candidate,
    Shadow,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleArtifactInvocationResult {
    pub artifact_id: ArtifactId,
    pub lane: ModuleInvocationLane,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInvocationOutcome {
    pub route: ModuleRouteDecision,
    pub primary: ModuleArtifactInvocationResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<ModuleArtifactInvocationResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayFixture {
    pub fixture_id: sinomo_core::Id,
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<ModuleReplayCase>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayCase {
    pub case_id: String,
    pub route_key: String,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event: Option<EventEnvelope>,
}

impl ModuleReplayCase {
    pub fn from_event(
        case_id: impl Into<String>,
        route_key: impl Into<String>,
        event: EventEnvelope,
    ) -> Self {
        let input = serde_json::to_string(&event).unwrap_or_else(|_| event.payload.to_string());
        Self {
            case_id: case_id.into(),
            route_key: route_key.into(),
            input,
            source_event: Some(event),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayReport {
    pub fixture_id: sinomo_core::Id,
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<ModuleReplayCaseOutcome>,
    pub replay_event_count: u64,
    pub missing_route_count: u64,
    pub primary_error_count: u64,
    pub shadow_error_count: u64,
    pub shadow_mismatch_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventEnvelope>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleShadowTelemetryReport {
    pub module_id: ModuleId,
    pub event_count: u64,
    pub matched_count: u64,
    pub mismatch_count: u64,
    pub primary_error_count: u64,
    pub shadow_error_count: u64,
    pub ignored_event_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
}

impl ModuleShadowTelemetryReport {
    pub fn from_stored_events<'a>(
        module_id: ModuleId,
        events: impl IntoIterator<Item = &'a StoredEvent>,
    ) -> Self {
        let mut report = Self {
            module_id,
            event_count: 0,
            matched_count: 0,
            mismatch_count: 0,
            primary_error_count: 0,
            shadow_error_count: 0,
            ignored_event_count: 0,
            event_ids: Vec::new(),
        };
        for stored in events {
            if stored.event.kind.as_str() != MODULE_SHADOW_DECISION_EVENT_KIND {
                report.ignored_event_count += 1;
                continue;
            }
            if stored
                .event
                .payload
                .get("module_id")
                .and_then(serde_json::Value::as_str)
                != Some(report.module_id.as_str())
            {
                report.ignored_event_count += 1;
                continue;
            }
            report.event_count += 1;
            report.event_ids.push(stored.event.event_id.clone());
            match stored
                .event
                .payload
                .get("shadow_matched")
                .and_then(serde_json::Value::as_bool)
            {
                Some(true) => report.matched_count += 1,
                Some(false) => report.mismatch_count += 1,
                None => report.ignored_event_count += 1,
            }
            if payload_field_has_error(&stored.event.payload, "primary_error") {
                report.primary_error_count += 1;
            }
            if payload_field_has_error(&stored.event.payload, "shadow_error") {
                report.shadow_error_count += 1;
            }
        }
        report
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleCanaryTelemetryReport {
    pub module_id: ModuleId,
    pub event_count: u64,
    pub active_primary_count: u64,
    pub candidate_primary_count: u64,
    pub correction_event_count: u64,
    pub active_user_correction_count: u64,
    pub candidate_user_correction_count: u64,
    pub unknown_user_correction_count: u64,
    pub active_error_count: u64,
    pub candidate_error_count: u64,
    pub active_latency_sample_count: u64,
    pub candidate_latency_sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_latency_p95_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_latency_p95_ms: Option<u64>,
    pub ignored_event_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
}

impl ModuleCanaryTelemetryReport {
    pub fn from_stored_events<'a>(
        module_id: ModuleId,
        candidate_artifact_id: Option<&ArtifactId>,
        events: impl IntoIterator<Item = &'a StoredEvent>,
    ) -> Self {
        let mut report = Self {
            module_id,
            event_count: 0,
            active_primary_count: 0,
            candidate_primary_count: 0,
            correction_event_count: 0,
            active_user_correction_count: 0,
            candidate_user_correction_count: 0,
            unknown_user_correction_count: 0,
            active_error_count: 0,
            candidate_error_count: 0,
            active_latency_sample_count: 0,
            candidate_latency_sample_count: 0,
            active_latency_p95_ms: None,
            candidate_latency_p95_ms: None,
            ignored_event_count: 0,
            event_ids: Vec::new(),
        };
        let mut active_latencies = Vec::new();
        let mut candidate_latencies = Vec::new();
        for stored in events {
            match stored.event.kind.as_str() {
                MODULE_INVOCATION_DECISION_EVENT_KIND => {
                    report.ingest_invocation_event(
                        stored,
                        candidate_artifact_id,
                        &mut active_latencies,
                        &mut candidate_latencies,
                    );
                }
                MODULE_USER_CORRECTION_EVENT_KIND => {
                    report.ingest_user_correction_event(stored, candidate_artifact_id);
                }
                _ => report.ignored_event_count += 1,
            }
        }
        report.active_latency_sample_count = active_latencies.len() as u64;
        report.candidate_latency_sample_count = candidate_latencies.len() as u64;
        report.active_latency_p95_ms = percentile_nearest_rank(active_latencies, 95);
        report.candidate_latency_p95_ms = percentile_nearest_rank(candidate_latencies, 95);
        report
    }

    fn ingest_invocation_event(
        &mut self,
        stored: &StoredEvent,
        candidate_artifact_id: Option<&ArtifactId>,
        active_latencies: &mut Vec<u64>,
        candidate_latencies: &mut Vec<u64>,
    ) {
        if !payload_module_matches(&stored.event.payload, &self.module_id) {
            self.ignored_event_count += 1;
            return;
        }
        let Some(primary_lane) = stored
            .event
            .payload
            .get("primary_lane")
            .and_then(serde_json::Value::as_str)
        else {
            self.ignored_event_count += 1;
            return;
        };
        if primary_lane == "candidate"
            && candidate_artifact_id.is_some_and(|candidate| {
                stored
                    .event
                    .payload
                    .get("primary_artifact_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(candidate.as_str())
            })
        {
            self.ignored_event_count += 1;
            return;
        }
        self.event_count += 1;
        self.event_ids.push(stored.event.event_id.clone());
        match primary_lane {
            "active" => {
                self.active_primary_count += 1;
                if payload_field_has_error(&stored.event.payload, "primary_error") {
                    self.active_error_count += 1;
                }
                if let Some(latency_ms) = payload_u64(&stored.event.payload, "primary_latency_ms") {
                    active_latencies.push(latency_ms);
                }
            }
            "candidate" => {
                self.candidate_primary_count += 1;
                if payload_field_has_error(&stored.event.payload, "primary_error") {
                    self.candidate_error_count += 1;
                }
                if let Some(latency_ms) = payload_u64(&stored.event.payload, "primary_latency_ms") {
                    candidate_latencies.push(latency_ms);
                }
            }
            _ => self.ignored_event_count += 1,
        }
    }

    fn ingest_user_correction_event(
        &mut self,
        stored: &StoredEvent,
        candidate_artifact_id: Option<&ArtifactId>,
    ) {
        if !payload_module_matches(&stored.event.payload, &self.module_id) {
            self.ignored_event_count += 1;
            return;
        }
        self.correction_event_count += 1;
        self.event_ids.push(stored.event.event_id.clone());
        match user_correction_lane(&stored.event.payload, candidate_artifact_id) {
            Some("active") => self.active_user_correction_count += 1,
            Some("candidate") => self.candidate_user_correction_count += 1,
            _ => self.unknown_user_correction_count += 1,
        }
    }
}

fn payload_module_matches(payload: &serde_json::Value, module_id: &ModuleId) -> bool {
    payload.get("module_id").and_then(serde_json::Value::as_str) == Some(module_id.as_str())
}

fn user_correction_lane<'a>(
    payload: &'a serde_json::Value,
    candidate_artifact_id: Option<&ArtifactId>,
) -> Option<&'a str> {
    let artifact_id = payload
        .get("corrected_artifact_id")
        .or_else(|| payload.get("artifact_id"))
        .or_else(|| payload.get("primary_artifact_id"))
        .and_then(serde_json::Value::as_str);
    if let (Some(candidate), Some(artifact_id)) = (candidate_artifact_id, artifact_id) {
        if artifact_id == candidate.as_str() {
            return Some("candidate");
        }
        return payload
            .get("primary_lane")
            .or_else(|| payload.get("lane"))
            .and_then(serde_json::Value::as_str)
            .filter(|lane| *lane == "active");
    }
    payload
        .get("primary_lane")
        .or_else(|| payload.get("lane"))
        .and_then(serde_json::Value::as_str)
        .filter(|lane| *lane == "active" || *lane == "candidate")
}

fn payload_u64(payload: &serde_json::Value, field: &str) -> Option<u64> {
    payload.get(field).and_then(serde_json::Value::as_u64)
}

fn payload_field_has_error(payload: &serde_json::Value, field: &str) -> bool {
    payload
        .get(field)
        .is_some_and(|value| !value.is_null() && value.as_str() != Some(""))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayCaseOutcome {
    pub case_id: String,
    pub route_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<ModuleInvocationOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_matched: Option<bool>,
}

pub trait ModuleArtifactRunner {
    fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRegistration {
    pub manifest: sinomo_core::ModuleManifest,
    pub backend: ModuleBackendRegistration,
    #[serde(default, skip_serializing_if = "ModuleSurfaces::is_empty")]
    pub surfaces: ModuleSurfaces,
}

impl ModuleRegistration {
    pub fn module_id(&self) -> &ModuleId {
        &self.manifest.module_id
    }

    pub fn native_rust(
        module_id: ModuleId,
        version: impl Into<String>,
        owner: impl Into<String>,
        crate_name: impl Into<String>,
        api_version: impl Into<String>,
        target_ring: TrustRing,
        registration_key: impl Into<String>,
    ) -> Self {
        let owner = owner.into();
        Self {
            manifest: sinomo_core::ModuleManifest::native_rust(
                module_id,
                version,
                owner.clone(),
                api_version,
                target_ring,
            ),
            backend: ModuleBackendRegistration::NativeRust {
                registration: NativeRustBackendRegistration::new(registration_key, crate_name),
            },
            surfaces: ModuleSurfaces::default(),
        }
    }

    pub fn with_surfaces(mut self, surfaces: ModuleSurfaces) -> Self {
        self.surfaces = surfaces;
        self
    }

    pub fn validate_surfaces(&self) -> Result<(), ModuleSurfaceError> {
        self.surfaces.validate()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum ModuleBackendRegistration {
    NativeRust {
        #[serde(flatten)]
        registration: NativeRustBackendRegistration,
    },
    NativeDylib {
        artifact_id: ArtifactId,
        content_hash: String,
        library_name: String,
        platform_triple: String,
        interface_version: String,
    },
    Declarative {
        payload_ref: String,
    },
    SandboxProcess {
        artifact_id: ArtifactId,
        command: String,
    },
    WasmComponent {
        artifact_id: ArtifactId,
        interface_version: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeRustBackendRegistration {
    pub registration_key: String,
    pub crate_name: String,
}

impl NativeRustBackendRegistration {
    pub fn new(registration_key: impl Into<String>, crate_name: impl Into<String>) -> Self {
        Self {
            registration_key: registration_key.into(),
            crate_name: crate_name.into(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct NativeRustModuleBinding {
    pub registration_key: &'static str,
    pub register_cli_command: Option<
        fn(sinomo_cli_registry::CliCommandRegistry) -> sinomo_cli_registry::CliCommandRegistry,
    >,
    pub register_config_definitions: Option<
        fn(
            sinomo_config::ConfigRegistryBuilder,
        ) -> Result<sinomo_config::ConfigRegistryBuilder, String>,
    >,
    pub register_storage_paths: Option<
        fn(
            &mut sinomo_core::storage::StorageLayout,
        ) -> Result<(), sinomo_core::storage::StorageContractError>,
    >,
}

impl NativeRustModuleBinding {
    pub const fn new(registration_key: &'static str) -> Self {
        Self {
            registration_key,
            register_cli_command: None,
            register_config_definitions: None,
            register_storage_paths: None,
        }
    }

    pub const fn with_cli_command(
        mut self,
        register_cli_command: fn(
            sinomo_cli_registry::CliCommandRegistry,
        ) -> sinomo_cli_registry::CliCommandRegistry,
    ) -> Self {
        self.register_cli_command = Some(register_cli_command);
        self
    }

    pub const fn with_config_definitions(
        mut self,
        register_config_definitions: fn(
            sinomo_config::ConfigRegistryBuilder,
        )
            -> Result<sinomo_config::ConfigRegistryBuilder, String>,
    ) -> Self {
        self.register_config_definitions = Some(register_config_definitions);
        self
    }

    pub const fn with_storage_paths(
        mut self,
        register_storage_paths: fn(
            &mut sinomo_core::storage::StorageLayout,
        )
            -> Result<(), sinomo_core::storage::StorageContractError>,
    ) -> Self {
        self.register_storage_paths = Some(register_storage_paths);
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleSurfaces {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_rpc_routes: Vec<ModuleObjectRpcRoute>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_kinds: Vec<EventKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_owners: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_definitions: Vec<ModuleAgentDefinitionExport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declarative_resources: Vec<ModuleDeclarativeResourceExport>,
    #[serde(default, skip_serializing_if = "ModuleCapabilities::is_empty")]
    pub capabilities: ModuleCapabilities,
    /// Cluster transport kinds this module serves as a runtime network module
    /// (mirrors `sinomo_module_api::NativeModuleSurfaces::network_transports`).
    /// Non-empty means the module's dylib also exports the network-module
    /// entry symbol and should be loaded into the daemon's
    /// `NetworkModuleRegistry`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_transports: Vec<String>,
}

impl ModuleSurfaces {
    pub fn is_empty(&self) -> bool {
        self.object_rpc_routes.is_empty()
            && self.event_kinds.is_empty()
            && self.storage_owners.is_empty()
            && self.agent_definitions.is_empty()
            && self.declarative_resources.is_empty()
            && self.capabilities.is_empty()
            && self.network_transports.is_empty()
    }

    pub fn with_object_rpc_route(mut self, route: ModuleObjectRpcRoute) -> Self {
        self.object_rpc_routes.push(route);
        self
    }

    pub fn with_event_kind(mut self, event_kind: EventKind) -> Self {
        self.event_kinds.push(event_kind);
        self
    }

    pub fn with_event_kind_name(mut self, event_kind: impl Into<String>) -> Self {
        self.event_kinds.push(
            EventKind::new(event_kind.into()).expect("module surface event kind should be valid"),
        );
        self
    }

    pub fn with_storage_owner(mut self, owner: impl Into<String>) -> Self {
        let owner = owner.into();
        if !self.storage_owners.contains(&owner) {
            self.storage_owners.push(owner);
        }
        self
    }

    pub fn with_agent_definition(mut self, definition: ModuleAgentDefinitionExport) -> Self {
        self.agent_definitions.push(definition);
        self
    }

    pub fn with_declarative_resource(mut self, resource: ModuleDeclarativeResourceExport) -> Self {
        self.declarative_resources.push(resource);
        self
    }

    pub fn with_skill_resource(self, id: impl Into<String>, path: impl Into<String>) -> Self {
        self.with_declarative_resource(ModuleDeclarativeResourceExport::skill(id, path))
    }

    pub fn with_prompt_resource(self, id: impl Into<String>, path: impl Into<String>) -> Self {
        self.with_declarative_resource(ModuleDeclarativeResourceExport::prompt(id, path))
    }

    /// Declares one cluster transport kind this module serves as a runtime
    /// network module (deduplicated).
    pub fn with_network_transport(mut self, transport: impl Into<String>) -> Self {
        let transport = transport.into();
        if !self.network_transports.contains(&transport) {
            self.network_transports.push(transport);
        }
        self
    }

    pub fn with_capability_import(mut self, capability: CapabilityName) -> Self {
        self.capabilities.imports.push(capability);
        self
    }

    pub fn with_capability_export(mut self, capability: CapabilityName) -> Self {
        self.capabilities.exports.push(capability);
        self
    }

    pub fn object_rpc_route_descriptors(
        &self,
    ) -> Result<Vec<ModuleObjectRpcRouteDescriptor>, ModuleSurfaceError> {
        self.validate()?;
        self.object_rpc_routes
            .iter()
            .map(ModuleObjectRpcRoute::descriptor)
            .collect()
    }

    pub fn object_rpc_route_descriptor(
        &self,
        route: &str,
    ) -> Result<Option<ModuleObjectRpcRouteDescriptor>, ModuleSurfaceError> {
        self.validate()?;
        self.object_rpc_routes
            .iter()
            .find(|candidate| candidate.route == route)
            .map(ModuleObjectRpcRoute::descriptor)
            .transpose()
    }

    pub fn validate(&self) -> Result<(), ModuleSurfaceError> {
        let mut object_rpc_routes = BTreeSet::new();
        let exported_capabilities = self
            .capabilities
            .exports
            .iter()
            .map(CapabilityName::as_str)
            .collect::<BTreeSet<_>>();
        for route in &self.object_rpc_routes {
            route.validate()?;
            if !object_rpc_routes.insert(route.route.as_str()) {
                return Err(ModuleSurfaceError::DuplicateObjectRpcRoute {
                    route: route.route.clone(),
                });
            }
            if !exported_capabilities.contains(route.operation.as_str()) {
                return Err(ModuleSurfaceError::ObjectRpcRouteOperationNotExported {
                    route: route.route.clone(),
                    operation: route.operation.clone(),
                });
            }
        }

        let mut storage_owners = BTreeSet::new();
        for owner in &self.storage_owners {
            validate_module_surface_text(owner, "module storage owner")?;
            if !storage_owners.insert(owner.as_str()) {
                return Err(ModuleSurfaceError::DuplicateStorageOwner {
                    owner: owner.clone(),
                });
            }
        }

        let mut imports = BTreeSet::new();
        for capability in &self.capabilities.imports {
            if !imports.insert(capability.as_str()) {
                return Err(ModuleSurfaceError::DuplicateCapabilityImport {
                    capability: capability.as_str().to_string(),
                });
            }
        }
        let mut exports = BTreeSet::new();
        for capability in &self.capabilities.exports {
            if !exports.insert(capability.as_str()) {
                return Err(ModuleSurfaceError::DuplicateCapabilityExport {
                    capability: capability.as_str().to_string(),
                });
            }
        }

        let mut declarative_resource_ids = BTreeSet::new();
        for resource in &self.declarative_resources {
            resource.validate()?;
            if !declarative_resource_ids.insert(resource.id.as_str()) {
                return Err(ModuleSurfaceError::DuplicateDeclarativeResource {
                    id: resource.id.clone(),
                });
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleAgentDefinitionExport {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub role: String,
    pub system_prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ModuleAgentToolExport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_pipeline_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_strategy_id: Option<String>,
}

impl ModuleAgentDefinitionExport {
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        role: impl Into<String>,
        system_prompt: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            role: role.into(),
            system_prompt: system_prompt.into(),
            tools: Vec::new(),
            visible_toolsets: Vec::new(),
            context_pipeline_id: None,
            compaction_strategy_id: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_tool(mut self, tool: ModuleAgentToolExport) -> Self {
        self.tools.push(tool);
        self
    }

    pub fn with_visible_toolset(mut self, toolset: impl Into<String>) -> Self {
        self.visible_toolsets.push(toolset.into());
        self
    }

    pub fn with_context_pipeline(mut self, pipeline_id: impl Into<String>) -> Self {
        self.context_pipeline_id = Some(pipeline_id.into());
        self
    }

    pub fn with_compaction_strategy(mut self, strategy_id: impl Into<String>) -> Self {
        self.compaction_strategy_id = Some(strategy_id.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleAgentToolExport {
    pub kind: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub async_default: bool,
}

impl ModuleAgentToolExport {
    pub fn new(
        kind: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        async_default: bool,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            description: description.into(),
            async_default,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleDeclarativeResourceKind {
    Skill,
    Prompt,
    PromptPack,
    EvalSuite,
    Example,
    Policy,
    Config,
}

impl ModuleDeclarativeResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::Prompt => "prompt",
            Self::PromptPack => "prompt-pack",
            Self::EvalSuite => "eval-suite",
            Self::Example => "example",
            Self::Policy => "policy",
            Self::Config => "config",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleDeclarativeResourceExport {
    pub id: String,
    pub kind: ModuleDeclarativeResourceKind,
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_text: Option<String>,
}

impl ModuleDeclarativeResourceExport {
    pub fn new(
        id: impl Into<String>,
        kind: ModuleDeclarativeResourceKind,
        path: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            path: path.into(),
            description: String::new(),
            target_agents: Vec::new(),
            model_families: Vec::new(),
            activation_tags: Vec::new(),
            required_capabilities: Vec::new(),
            embedded_text: None,
        }
    }

    pub fn skill(id: impl Into<String>, path: impl Into<String>) -> Self {
        Self::new(id, ModuleDeclarativeResourceKind::Skill, path)
    }

    pub fn prompt(id: impl Into<String>, path: impl Into<String>) -> Self {
        Self::new(id, ModuleDeclarativeResourceKind::Prompt, path)
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

    pub fn with_required_capability(mut self, capability: CapabilityName) -> Self {
        self.required_capabilities.push(capability);
        self
    }

    pub fn with_embedded_text(mut self, text: impl Into<String>) -> Self {
        self.embedded_text = Some(text.into());
        self
    }

    pub fn validate(&self) -> Result<(), ModuleSurfaceError> {
        validate_module_declarative_resource_token(&self.id, &self.id, "declarative resource id")?;
        validate_module_declarative_resource_path(&self.id, &self.path)?;
        if !self.description.is_empty() && self.description.chars().any(char::is_control) {
            return Err(ModuleSurfaceError::InvalidDeclarativeResource {
                id: self.id.clone(),
                detail: "description must not contain control characters".to_string(),
            });
        }
        validate_module_declarative_resource_tokens(
            &self.id,
            &self.target_agents,
            "declarative resource target agent",
        )?;
        validate_module_declarative_resource_tokens(
            &self.id,
            &self.model_families,
            "declarative resource model family",
        )?;
        validate_module_declarative_resource_tokens(
            &self.id,
            &self.activation_tags,
            "declarative resource activation tag",
        )?;

        let mut capabilities = BTreeSet::new();
        for capability in &self.required_capabilities {
            if !capabilities.insert(capability.as_str()) {
                return Err(ModuleSurfaceError::InvalidDeclarativeResource {
                    id: self.id.clone(),
                    detail: format!(
                        "required capability `{}` is listed more than once",
                        capability.as_str()
                    ),
                });
            }
        }
        if self
            .embedded_text
            .as_deref()
            .is_some_and(|text| text.chars().any(|ch| ch == '\0'))
        {
            return Err(ModuleSurfaceError::InvalidDeclarativeResource {
                id: self.id.clone(),
                detail: "embedded text must not contain NUL bytes".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleObjectRpcRoute {
    pub route: String,
    pub category: String,
    pub operation: String,
    pub auth: ModuleRouteAuth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleObjectRpcRouteDescriptor {
    pub route: String,
    pub category: String,
    pub operation: CapabilityName,
    pub auth_scope: String,
    pub required_capabilities: Vec<CapabilityName>,
}

impl ModuleObjectRpcRoute {
    pub fn new(
        route: impl Into<String>,
        category: impl Into<String>,
        operation: impl Into<String>,
        auth: ModuleRouteAuth,
    ) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            operation: operation.into(),
            auth,
        }
    }

    pub fn validate(&self) -> Result<(), ModuleSurfaceError> {
        validate_module_object_rpc_route_path(&self.route)?;
        validate_module_surface_token(&self.category, "module object RPC route category")?;
        CapabilityName::new(self.operation.clone()).map_err(|source| {
            ModuleSurfaceError::InvalidObjectRpcRouteOperation {
                route: self.route.clone(),
                operation: self.operation.clone(),
                detail: source.to_string(),
            }
        })?;
        Ok(())
    }

    pub fn auth_scope(&self) -> &'static str {
        self.auth.as_scope()
    }

    pub fn operation_capability(&self) -> Result<CapabilityName, ModuleSurfaceError> {
        CapabilityName::new(self.operation.clone()).map_err(|source| {
            ModuleSurfaceError::InvalidObjectRpcRouteOperation {
                route: self.route.clone(),
                operation: self.operation.clone(),
                detail: source.to_string(),
            }
        })
    }

    pub fn descriptor(&self) -> Result<ModuleObjectRpcRouteDescriptor, ModuleSurfaceError> {
        self.validate()?;
        let operation = self.operation_capability()?;
        Ok(ModuleObjectRpcRouteDescriptor {
            route: self.route.clone(),
            category: self.category.clone(),
            operation: operation.clone(),
            auth_scope: self.auth_scope().to_string(),
            required_capabilities: vec![operation],
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRouteAuth {
    LocalCommand,
    LocalOrClusterMember,
    ClusterMember,
    Admin,
}

impl ModuleRouteAuth {
    pub fn as_scope(self) -> &'static str {
        match self {
            Self::LocalCommand => "local-command",
            Self::LocalOrClusterMember => "local-or-cluster-member",
            Self::ClusterMember => "cluster-member",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModuleRegistry {
    registrations: BTreeMap<ModuleId, ModuleRegistration>,
    registration_order: Vec<ModuleId>,
}

impl ModuleRegistry {
    pub fn builder() -> ModuleRegistryBuilder {
        ModuleRegistryBuilder::default()
    }

    pub fn to_builder(&self) -> ModuleRegistryBuilder {
        ModuleRegistryBuilder {
            registrations: self.registrations.clone(),
            registration_order: self.registration_order.clone(),
        }
    }

    pub fn get(&self, module_id: &ModuleId) -> Option<&ModuleRegistration> {
        self.registrations.get(module_id)
    }

    pub fn contains(&self, module_id: &ModuleId) -> bool {
        self.registrations.contains_key(module_id)
    }

    pub fn registrations(&self) -> impl Iterator<Item = &ModuleRegistration> {
        self.registration_order
            .iter()
            .filter_map(|module_id| self.registrations.get(module_id))
    }

    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    pub fn validate(&self) -> Result<(), ModuleRegistryError> {
        let mut ordered_ids = BTreeSet::new();
        for module_id in &self.registration_order {
            if !ordered_ids.insert(module_id.clone()) {
                return Err(ModuleRegistryError::InvalidRegistrationOrder {
                    detail: format!("module {module_id} appears more than once"),
                });
            }
            if !self.registrations.contains_key(module_id) {
                return Err(ModuleRegistryError::InvalidRegistrationOrder {
                    detail: format!("ordered module {module_id} has no registration"),
                });
            }
        }
        if ordered_ids.len() != self.registrations.len() {
            return Err(ModuleRegistryError::InvalidRegistrationOrder {
                detail: "registration order does not cover every module".to_string(),
            });
        }

        let mut object_rpc_routes = BTreeMap::<String, ModuleId>::new();
        for (module_id, registration) in &self.registrations {
            if registration.module_id() != module_id {
                return Err(ModuleRegistryError::InvalidRegistrationOrder {
                    detail: format!(
                        "registration key {module_id} does not match manifest module id {}",
                        registration.module_id()
                    ),
                });
            }
            registration.validate_surfaces().map_err(|source| {
                ModuleRegistryError::InvalidModuleSurfaces {
                    module_id: module_id.clone(),
                    detail: source.to_string(),
                }
            })?;
            for descriptor in registration
                .surfaces
                .object_rpc_route_descriptors()
                .map_err(|source| ModuleRegistryError::InvalidModuleSurfaces {
                    module_id: module_id.clone(),
                    detail: source.to_string(),
                })?
            {
                if let Some(previous_module_id) =
                    object_rpc_routes.insert(descriptor.route.clone(), module_id.clone())
                {
                    return Err(ModuleRegistryError::DuplicateObjectRpcRoute {
                        route: descriptor.route,
                        first_module_id: previous_module_id,
                        second_module_id: module_id.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ModuleRegistrySnapshot {
    version: u64,
    generated_at: OffsetDateTime,
    registry: Arc<ModuleRegistry>,
}

impl ModuleRegistrySnapshot {
    fn new(version: u64, registry: ModuleRegistry) -> Result<Self, ModuleRegistryError> {
        registry.validate()?;
        Ok(Self {
            version,
            generated_at: OffsetDateTime::now_utc(),
            registry: Arc::new(registry),
        })
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn generated_at(&self) -> OffsetDateTime {
        self.generated_at
    }

    pub fn registry(&self) -> &ModuleRegistry {
        &self.registry
    }

    pub fn registry_arc(&self) -> Arc<ModuleRegistry> {
        Arc::clone(&self.registry)
    }
}

#[derive(Clone)]
pub struct ModuleRegistrySnapshotStore {
    active: Arc<ArcSwap<ModuleRegistrySnapshot>>,
    update_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for ModuleRegistrySnapshotStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModuleRegistrySnapshotStore")
            .field("active_version", &self.active.load().version())
            .finish_non_exhaustive()
    }
}

impl Default for ModuleRegistrySnapshotStore {
    fn default() -> Self {
        Self::new(ModuleRegistry::default()).expect("empty module registry should validate")
    }
}

impl ModuleRegistrySnapshotStore {
    pub fn new(registry: ModuleRegistry) -> Result<Self, ModuleRegistryError> {
        let snapshot = Arc::new(ModuleRegistrySnapshot::new(1, registry)?);
        Ok(Self {
            active: Arc::new(ArcSwap::from(snapshot)),
            update_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn active_snapshot(&self) -> Result<Arc<ModuleRegistrySnapshot>, ModuleRegistryError> {
        Ok(self.active.load_full())
    }

    pub fn replace_registry(
        &self,
        registry: ModuleRegistry,
    ) -> Result<Arc<ModuleRegistrySnapshot>, ModuleRegistryError> {
        let _guard =
            self.update_lock
                .lock()
                .map_err(|source| ModuleRegistryError::SnapshotLock {
                    detail: source.to_string(),
                })?;
        let active = self.active.load();
        let version = active.version().saturating_add(1);
        let snapshot = Arc::new(ModuleRegistrySnapshot::new(version, registry)?);
        self.active.store(Arc::clone(&snapshot));
        Ok(snapshot)
    }

    pub fn register(
        &self,
        registration: ModuleRegistration,
    ) -> Result<Arc<ModuleRegistrySnapshot>, ModuleRegistryError> {
        self.register_validated(registration, |_| Ok(()))
    }

    pub fn register_validated(
        &self,
        registration: ModuleRegistration,
        validate: impl FnOnce(Arc<ModuleRegistrySnapshot>) -> Result<(), ModuleRegistryError>,
    ) -> Result<Arc<ModuleRegistrySnapshot>, ModuleRegistryError> {
        let _guard =
            self.update_lock
                .lock()
                .map_err(|source| ModuleRegistryError::SnapshotLock {
                    detail: source.to_string(),
                })?;
        let active = self.active.load();
        let registry = active
            .registry()
            .to_builder()
            .register(registration)?
            .try_build()?;
        let snapshot = Arc::new(ModuleRegistrySnapshot::new(
            active.version().saturating_add(1),
            registry,
        )?);
        validate(Arc::clone(&snapshot))?;
        self.active.store(Arc::clone(&snapshot));
        Ok(snapshot)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ModuleRegistryBuilder {
    registrations: BTreeMap<ModuleId, ModuleRegistration>,
    registration_order: Vec<ModuleId>,
}

impl ModuleRegistryBuilder {
    pub fn register(
        mut self,
        registration: ModuleRegistration,
    ) -> Result<Self, ModuleRegistryError> {
        let module_id = registration.module_id().clone();
        if self.registrations.contains_key(&module_id) {
            return Err(ModuleRegistryError::DuplicateModuleId { module_id });
        }
        registration.validate_surfaces().map_err(|source| {
            ModuleRegistryError::InvalidModuleSurfaces {
                module_id: module_id.clone(),
                detail: source.to_string(),
            }
        })?;
        self.registration_order.push(module_id.clone());
        self.registrations.insert(module_id, registration);
        Ok(self)
    }

    pub fn try_build(self) -> Result<ModuleRegistry, ModuleRegistryError> {
        let registry = ModuleRegistry {
            registrations: self.registrations,
            registration_order: self.registration_order,
        };
        registry.validate()?;
        Ok(registry)
    }

    pub fn build(self) -> ModuleRegistry {
        self.try_build().expect("valid module registry")
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleRegistryError {
    #[error("duplicate module registration for {module_id}")]
    DuplicateModuleId { module_id: ModuleId },
    #[error("invalid module surfaces for {module_id}: {detail}")]
    InvalidModuleSurfaces { module_id: ModuleId, detail: String },
    #[error(
        "duplicate module object RPC route `{route}` from {first_module_id} and {second_module_id}"
    )]
    DuplicateObjectRpcRoute {
        route: String,
        first_module_id: ModuleId,
        second_module_id: ModuleId,
    },
    #[error("invalid module registry order: {detail}")]
    InvalidRegistrationOrder { detail: String },
    #[error("module registry snapshot lock failed: {detail}")]
    SnapshotLock { detail: String },
    #[error("module registry snapshot validation failed: {detail}")]
    SnapshotValidation { detail: String },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleSurfaceError {
    #[error("module object RPC route `{route}` is invalid: {detail}")]
    InvalidObjectRpcRoute { route: String, detail: String },
    #[error("module object RPC route `{route}` operation `{operation}` is invalid: {detail}")]
    InvalidObjectRpcRouteOperation {
        route: String,
        operation: String,
        detail: String,
    },
    #[error("duplicate module object RPC route `{route}`")]
    DuplicateObjectRpcRoute { route: String },
    #[error("module object RPC route `{route}` operation `{operation}` is not exported")]
    ObjectRpcRouteOperationNotExported { route: String, operation: String },
    #[error("module storage owner `{owner}` is invalid: {detail}")]
    InvalidStorageOwner { owner: String, detail: String },
    #[error("duplicate module storage owner `{owner}`")]
    DuplicateStorageOwner { owner: String },
    #[error("module declarative resource `{id}` is invalid: {detail}")]
    InvalidDeclarativeResource { id: String, detail: String },
    #[error("duplicate module declarative resource `{id}`")]
    DuplicateDeclarativeResource { id: String },
    #[error("duplicate module capability import `{capability}`")]
    DuplicateCapabilityImport { capability: String },
    #[error("duplicate module capability export `{capability}`")]
    DuplicateCapabilityExport { capability: String },
}

fn validate_module_object_rpc_route_path(route: &str) -> Result<(), ModuleSurfaceError> {
    if route.trim().is_empty() {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: route.to_string(),
            detail: "route must be non-empty".to_string(),
        });
    }
    if !route.starts_with('/') {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: route.to_string(),
            detail: "route must start with /".to_string(),
        });
    }
    if route.chars().any(char::is_control) {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: route.to_string(),
            detail: "route must not contain control characters".to_string(),
        });
    }
    Ok(())
}

fn validate_module_surface_token(value: &str, context: &str) -> Result<(), ModuleSurfaceError> {
    if value.trim().is_empty() {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: value.to_string(),
            detail: format!("{context} must be non-empty"),
        });
    }
    if value != value.trim() {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: value.to_string(),
            detail: format!("{context} must not have leading or trailing whitespace"),
        });
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
    {
        return Err(ModuleSurfaceError::InvalidObjectRpcRoute {
            route: value.to_string(),
            detail: format!(
                "{context} must contain only ASCII letters, digits, '.', '-', '_', or ':'"
            ),
        });
    }
    Ok(())
}

fn validate_module_surface_text(value: &str, context: &str) -> Result<(), ModuleSurfaceError> {
    if value.trim().is_empty() {
        return Err(ModuleSurfaceError::InvalidStorageOwner {
            owner: value.to_string(),
            detail: format!("{context} must be non-empty"),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(ModuleSurfaceError::InvalidStorageOwner {
            owner: value.to_string(),
            detail: format!("{context} must not contain control characters"),
        });
    }
    Ok(())
}

fn validate_module_declarative_resource_token(
    resource_id: &str,
    value: &str,
    context: &str,
) -> Result<(), ModuleSurfaceError> {
    if value.trim().is_empty() {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: format!("{context} must be non-empty"),
        });
    }
    if value != value.trim() {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: format!("{context} must not have leading or trailing whitespace"),
        });
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':' | '/'))
    {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: format!(
                "{context} must contain only ASCII letters, digits, '.', '-', '_', ':', or '/'"
            ),
        });
    }
    Ok(())
}

fn validate_module_declarative_resource_tokens(
    resource_id: &str,
    values: &[String],
    context: &str,
) -> Result<(), ModuleSurfaceError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_module_declarative_resource_token(resource_id, value, context)?;
        if !seen.insert(value.as_str()) {
            return Err(ModuleSurfaceError::InvalidDeclarativeResource {
                id: resource_id.to_string(),
                detail: format!("{context} `{value}` is listed more than once"),
            });
        }
    }
    Ok(())
}

fn validate_module_declarative_resource_path(
    resource_id: &str,
    path: &str,
) -> Result<(), ModuleSurfaceError> {
    if path.trim().is_empty() {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: "path must be non-empty".to_string(),
        });
    }
    if path != path.trim() {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: "path must not have leading or trailing whitespace".to_string(),
        });
    }
    if path.chars().any(char::is_control) {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: "path must not contain control characters".to_string(),
        });
    }
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return Err(ModuleSurfaceError::InvalidDeclarativeResource {
            id: resource_id.to_string(),
            detail: "path must be relative to the module payload root".to_string(),
        });
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ModuleSurfaceError::InvalidDeclarativeResource {
                    id: resource_id.to_string(),
                    detail: "path must stay inside the module payload root".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn default_declarative_module_owner() -> String {
    "local".to_string()
}

fn default_declarative_module_api_version() -> String {
    DECLARATIVE_MODULE_API_VERSION.to_string()
}

fn default_declarative_module_target_ring() -> TrustRing {
    TrustRing::R3SandboxedModule
}

fn parse_declarative_module_id(value: &str) -> Result<ModuleId, DeclarativeModuleInstallError> {
    ModuleId::new(value.to_string())
        .or_else(|_| ModuleId::from_legacy_named(value))
        .map_err(|source| DeclarativeModuleInstallError::Descriptor {
            detail: format!("invalid module id `{value}`: {source}"),
        })
}

fn validate_declarative_module_version(value: &str) -> Result<(), DeclarativeModuleInstallError> {
    if value.chars().any(char::is_control) {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: "module version must not contain control characters".to_string(),
        });
    }
    if value
        .chars()
        .any(|ch| matches!(ch, '/' | '\\') || ch.is_whitespace())
    {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: "module version must be a single path-safe token".to_string(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: "module version must not be . or ..".to_string(),
        });
    }
    Ok(())
}

fn module_content_hash_path_key(content_hash: &str) -> String {
    content_hash
        .strip_prefix("blake3:")
        .unwrap_or(content_hash)
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct LocalCommandModuleArtifactRunner {
    registry: ModuleLocalCommandRegistry,
    default_cwd: PathBuf,
}

impl LocalCommandModuleArtifactRunner {
    pub fn new(registry: ModuleLocalCommandRegistry, default_cwd: impl Into<PathBuf>) -> Self {
        Self {
            registry,
            default_cwd: default_cwd.into(),
        }
    }

    pub fn from_data_dir(
        data_dir_root: &Path,
        default_cwd: impl Into<PathBuf>,
    ) -> Result<Self, ModuleLoaderStoreError> {
        Ok(Self::new(
            read_module_local_command_registry(data_dir_root)?.unwrap_or_default(),
            default_cwd,
        ))
    }
}

impl ModuleArtifactRunner for LocalCommandModuleArtifactRunner {
    fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String> {
        let artifact = self
            .registry
            .artifact(&invocation.artifact_id)
            .ok_or_else(|| {
                format!(
                    "module artifact {} is not registered as a local command",
                    invocation.artifact_id
                )
            })?;
        run_local_command_artifact(artifact, invocation, &self.default_cwd)
    }
}

pub fn module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root.join("module").join("loader-state")
}

pub fn module_loader_state_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn native_dylib_module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("native-dylib")
}

fn native_dylib_module_loader_state_lock_path(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("native-dylib.lock")
}

fn native_dylib_module_loader_state_transaction_path(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("native-dylib.restore.sobj")
}

fn native_dylib_module_loader_state_staging_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join(".native-dylib.restore-staged")
}

fn native_dylib_module_loader_state_previous_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join(".native-dylib.restore-previous")
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct NativeDylibModuleLoaderStateTransaction {
    prepared_at: String,
}

#[derive(Debug)]
struct NativeDylibModuleStateLock {
    file: fs::File,
}

impl Drop for NativeDylibModuleStateLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn acquire_native_dylib_module_state_lock(
    data_dir_root: &Path,
) -> Result<NativeDylibModuleStateLock, ModuleLoaderStoreError> {
    let path = native_dylib_module_loader_state_lock_path(data_dir_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ModuleLoaderStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    file.lock_exclusive()
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(NativeDylibModuleStateLock { file })
}

pub fn native_dylib_module_loader_state_path(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> PathBuf {
    native_dylib_module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn native_dylib_module_cache_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("module")
        .join("cache")
        .join("native-dylib")
}

pub fn native_rust_module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("native-rust")
}

pub fn native_rust_module_loader_state_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    native_rust_module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn module_lifecycle_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root.join("module").join("lifecycle")
}

pub fn module_unload_record_dir(data_dir_root: &Path) -> PathBuf {
    module_lifecycle_dir(data_dir_root).join("unload-records")
}

pub fn module_unload_record_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    module_unload_record_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn module_local_command_registry_path(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("module")
        .join("local-command-artifacts.sobj")
}

pub fn declarative_module_catalog_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("module")
        .join("catalog")
        .join("declarative")
}

pub fn declarative_module_catalog_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    declarative_module_catalog_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn declarative_module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("declarative")
}

pub fn declarative_module_loader_state_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    declarative_module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn declarative_module_cache_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("module")
        .join("cache")
        .join("declarative")
}

pub fn declarative_module_payload_cache_path(
    data_dir_root: &Path,
    module_id: &ModuleId,
    version: &str,
    content_hash: &str,
) -> PathBuf {
    declarative_module_cache_dir(data_dir_root)
        .join(module_id.to_string())
        .join(version)
        .join(module_content_hash_path_key(content_hash))
        .join("payload")
}

pub fn declarative_module_payload_ref(
    module_id: &ModuleId,
    version: &str,
    content_hash: &str,
) -> String {
    format!(
        "local://module/cache/declarative/{module_id}/{version}/{}/payload",
        module_content_hash_path_key(content_hash)
    )
}

pub fn read_module_loader_runtime_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<ModuleLoaderRuntimeState>, ModuleLoaderStoreError> {
    let path = module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_module_loader_runtime_state(
    data_dir_root: &Path,
    state: &ModuleLoaderRuntimeState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    Ok(path)
}

pub fn read_native_dylib_module_loader_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<NativeDylibModuleLoaderState>, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    let path = native_dylib_module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn read_native_dylib_module_loader_states(
    data_dir_root: &Path,
) -> Result<Vec<NativeDylibModuleLoaderState>, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    read_native_dylib_module_loader_states_unlocked(data_dir_root)
}

fn read_native_dylib_module_loader_states_unlocked(
    data_dir_root: &Path,
) -> Result<Vec<NativeDylibModuleLoaderState>, ModuleLoaderStoreError> {
    read_objects_from_dir(&native_dylib_module_loader_state_dir(data_dir_root))
}

/// Completes or rolls back an interrupted loader-directory swap. Readers and
/// writers call this while holding the native loader lock, so they never expose
/// a partially restored set of module records after process restart.
pub fn recover_native_dylib_module_loader_state_transaction(
    data_dir_root: &Path,
) -> Result<bool, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)
}

/// Replaces the complete native-dylib loader-state directory with `states` via a
/// durable staged-directory swap. The transaction marker and topology recovery
/// make every crash point restart-safe without copying module cache artifacts.
pub fn replace_native_dylib_module_loader_states_transactionally(
    data_dir_root: &Path,
    states: &[NativeDylibModuleLoaderState],
) -> Result<(), ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;

    let mut module_ids = BTreeSet::new();
    for state in states {
        if !module_ids.insert(state.module_id.clone()) {
            return Err(ModuleLoaderStoreError::Object {
                path: native_dylib_module_loader_state_dir(data_dir_root)
                    .display()
                    .to_string(),
                detail: format!(
                    "duplicate native dylib loader state for {}",
                    state.module_id
                ),
            });
        }
    }

    let live = native_dylib_module_loader_state_dir(data_dir_root);
    let staging = native_dylib_module_loader_state_staging_dir(data_dir_root);
    let previous = native_dylib_module_loader_state_previous_dir(data_dir_root);
    let marker = native_dylib_module_loader_state_transaction_path(data_dir_root);
    let parent = module_loader_state_dir(data_dir_root);
    remove_dir_if_exists(&staging)?;
    remove_dir_if_exists(&previous)?;
    fs::create_dir_all(&staging).map_err(|source| ModuleLoaderStoreError::Io {
        path: staging.display().to_string(),
        detail: source.to_string(),
    })?;
    for state in states {
        let path = staging.join(format!("{}.sobj", state.module_id));
        write_object(&path, state)?;
        sync_module_loader_file(&path)?;
    }
    sync_module_loader_dir(&staging)?;
    write_object(
        &marker,
        &NativeDylibModuleLoaderStateTransaction {
            prepared_at: module_loader_applied_at_now(),
        },
    )?;
    sync_module_loader_file(&marker)?;
    sync_module_loader_dir(&parent)?;

    if live.exists() {
        fs::rename(&live, &previous).map_err(|source| ModuleLoaderStoreError::Io {
            path: live.display().to_string(),
            detail: format!("move current loader directory to rollback slot: {source}"),
        })?;
        sync_module_loader_dir(&parent)?;
    }
    fs::rename(&staging, &live).map_err(|source| ModuleLoaderStoreError::Io {
        path: staging.display().to_string(),
        detail: format!("activate staged loader directory: {source}"),
    })?;
    sync_module_loader_dir(&parent)?;
    remove_dir_if_exists(&previous)?;
    remove_file_if_exists(&marker)?;
    sync_module_loader_dir(&parent)?;
    Ok(())
}

fn recover_native_dylib_module_loader_state_transaction_unlocked(
    data_dir_root: &Path,
) -> Result<bool, ModuleLoaderStoreError> {
    let marker = native_dylib_module_loader_state_transaction_path(data_dir_root);
    if !marker.exists() {
        return Ok(false);
    }
    // Validate the marker before using the recovery topology. A corrupt marker
    // fails closed; silently guessing could discard the only complete state set.
    let _: NativeDylibModuleLoaderStateTransaction = read_object(&marker)?;
    let live = native_dylib_module_loader_state_dir(data_dir_root);
    let staging = native_dylib_module_loader_state_staging_dir(data_dir_root);
    let previous = native_dylib_module_loader_state_previous_dir(data_dir_root);
    let parent = module_loader_state_dir(data_dir_root);

    if live.exists() {
        // Either the swap never began (staging remains, no previous) or the new
        // directory is already live. In both cases `live` is the authoritative
        // complete set and the other directories are cleanup-only.
        remove_dir_if_exists(&staging)?;
        remove_dir_if_exists(&previous)?;
    } else if staging.exists() {
        // The old directory was moved aside and the process stopped before the
        // second rename. Complete the intended restore.
        fs::rename(&staging, &live).map_err(|source| ModuleLoaderStoreError::Io {
            path: staging.display().to_string(),
            detail: format!("complete interrupted loader restore: {source}"),
        })?;
        sync_module_loader_dir(&parent)?;
        remove_dir_if_exists(&previous)?;
    } else if previous.exists() {
        // The staged set disappeared before it could become live. Roll back to
        // the last complete directory instead of exposing an empty loader set.
        fs::rename(&previous, &live).map_err(|source| ModuleLoaderStoreError::Io {
            path: previous.display().to_string(),
            detail: format!("roll back interrupted loader restore: {source}"),
        })?;
        sync_module_loader_dir(&parent)?;
    } else {
        return Err(ModuleLoaderStoreError::Io {
            path: marker.display().to_string(),
            detail: "loader restore marker exists but no complete state directory is recoverable"
                .to_string(),
        });
    }
    remove_file_if_exists(&marker)?;
    sync_module_loader_dir(&parent)?;
    Ok(true)
}

fn remove_dir_if_exists(path: &Path) -> Result<(), ModuleLoaderStoreError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<(), ModuleLoaderStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn sync_module_loader_file(path: &Path) -> Result<(), ModuleLoaderStoreError> {
    // `File::open` only requests read access. Unix permits flushing that
    // handle, but Windows' `FlushFileBuffers` rejects it with
    // ERROR_ACCESS_DENIED. Reopen the state file with write access so the
    // durability barrier has the same semantics on every supported platform.
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[cfg(unix)]
fn sync_module_loader_dir(path: &Path) -> Result<(), ModuleLoaderStoreError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[cfg(not(unix))]
fn sync_module_loader_dir(_path: &Path) -> Result<(), ModuleLoaderStoreError> {
    Ok(())
}

pub fn read_native_dylib_module_cache_entries(
    data_dir_root: &Path,
) -> Result<Vec<NativeDylibModuleCacheEntry>, ModuleLoaderStoreError> {
    read_native_dylib_module_cache_entries_excluding_cache_keys(data_dir_root, &BTreeSet::new())
}

pub fn read_native_dylib_module_cache_entries_excluding_cache_keys(
    data_dir_root: &Path,
    excluded_cache_keys: &BTreeSet<String>,
) -> Result<Vec<NativeDylibModuleCacheEntry>, ModuleLoaderStoreError> {
    let cache_root = native_dylib_module_cache_dir(data_dir_root);
    if !cache_root.exists() {
        return Ok(Vec::new());
    }
    let excluded_cache_dirs = excluded_cache_keys
        .iter()
        .map(|cache_key| cache_root.join(cache_key))
        .collect::<BTreeSet<_>>();
    let mut paths = Vec::new();
    collect_native_dylib_module_cache_files(
        &cache_root,
        &cache_root,
        &excluded_cache_dirs,
        &mut paths,
    )?;
    paths.sort();
    let mut entries = Vec::new();
    for path in paths {
        if let Ok(Some(entry)) = native_dylib_module_cache_entry_from_path(&cache_root, &path) {
            entries.push(entry);
        }
    }
    entries.sort_by(|left, right| {
        left.module_id
            .cmp(&right.module_id)
            .then(left.interface_version.cmp(&right.interface_version))
            .then(left.content_hash.cmp(&right.content_hash))
            .then(left.library_path.cmp(&right.library_path))
    });
    Ok(entries)
}

pub fn prune_inactive_native_dylib_module_cache(
    data_dir_root: &Path,
    retain_inactive_variants_per_cache_key: usize,
) -> Result<NativeDylibModuleCachePruneReport, ModuleLoaderStoreError> {
    prune_inactive_native_dylib_module_cache_with_pinned_artifacts(
        data_dir_root,
        retain_inactive_variants_per_cache_key,
        &BTreeSet::new(),
    )
}

/// Prunes inactive content-addressed native module variants while retaining every
/// variant referenced by `pinned_artifacts`. Pins do not consume the normal
/// inactive-retention allowance: a rollback artifact remains available until its
/// owning subsystem removes the pin, and the newest unpinned variants are still
/// retained for ordinary operator recovery.
pub fn prune_inactive_native_dylib_module_cache_with_pinned_artifacts(
    data_dir_root: &Path,
    retain_inactive_variants_per_cache_key: usize,
    pinned_artifacts: &BTreeSet<PathBuf>,
) -> Result<NativeDylibModuleCachePruneReport, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    let cache_root = native_dylib_module_cache_dir(data_dir_root);
    if !cache_root.exists() {
        return Ok(NativeDylibModuleCachePruneReport {
            retained_active_variants: 0,
            retained_inactive_variants: 0,
            removed_inactive_variants: Vec::new(),
        });
    }
    let pinned_variant_dirs = pinned_artifacts
        .iter()
        .map(|path| {
            let relative =
                path.strip_prefix(&cache_root)
                    .map_err(|source| ModuleLoaderStoreError::Io {
                        path: path.display().to_string(),
                        detail: format!(
                            "pinned native module artifact is not under {}: {source}",
                            cache_root.display()
                        ),
                    })?;
            if relative.components().count() < 3 {
                return Err(ModuleLoaderStoreError::Io {
                    path: path.display().to_string(),
                    detail:
                        "pinned native module artifact has no content-addressed variant directory"
                            .to_string(),
                });
            }
            path.parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| ModuleLoaderStoreError::Io {
                    path: path.display().to_string(),
                    detail: "pinned native module artifact has no parent directory".to_string(),
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let active_variant_dirs = read_native_dylib_module_loader_states_unlocked(data_dir_root)?
        .into_iter()
        .filter_map(|state| {
            native_module_cache_path(
                data_dir_root,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            )
            .parent()
            .map(Path::to_path_buf)
        })
        .collect::<BTreeSet<_>>();
    let mut retained_active_variants = 0usize;
    let mut inactive_by_cache_key =
        BTreeMap::<String, Vec<NativeDylibModuleCacheVariantDir>>::new();
    for variant in native_dylib_module_cache_variant_dirs(&cache_root)? {
        if active_variant_dirs.contains(&variant.path) {
            retained_active_variants += 1;
            continue;
        }
        inactive_by_cache_key
            .entry(variant.cache_key.clone())
            .or_default()
            .push(variant);
    }

    let mut retained_inactive_variants = 0usize;
    let mut removed_inactive_variants = Vec::new();
    for mut variants in inactive_by_cache_key.into_values() {
        variants.sort_by(|left, right| {
            right
                .modified
                .cmp(&left.modified)
                .then(left.path.cmp(&right.path))
        });
        let mut retained_unpinned = 0usize;
        for variant in variants {
            if pinned_variant_dirs.contains(&variant.path) {
                retained_inactive_variants += 1;
                continue;
            }
            if retained_unpinned < retain_inactive_variants_per_cache_key {
                retained_unpinned += 1;
                retained_inactive_variants += 1;
                continue;
            }
            fs::remove_dir_all(&variant.path).map_err(|source| ModuleLoaderStoreError::Io {
                path: variant.path.display().to_string(),
                detail: source.to_string(),
            })?;
            removed_inactive_variants.push(variant.path);
        }
    }
    removed_inactive_variants.sort();
    Ok(NativeDylibModuleCachePruneReport {
        retained_active_variants,
        retained_inactive_variants,
        removed_inactive_variants,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeDylibModuleCacheVariantDir {
    cache_key: String,
    path: PathBuf,
    modified: SystemTime,
}

fn native_dylib_module_cache_variant_dirs(
    cache_root: &Path,
) -> Result<Vec<NativeDylibModuleCacheVariantDir>, ModuleLoaderStoreError> {
    let mut variants = Vec::new();
    collect_native_dylib_module_cache_variant_dirs(cache_root, cache_root, &mut variants)?;
    variants.sort_by(|left, right| {
        left.cache_key
            .cmp(&right.cache_key)
            .then(left.path.cmp(&right.path))
    });
    Ok(variants)
}

fn collect_native_dylib_module_cache_variant_dirs(
    cache_root: &Path,
    dir: &Path,
    variants: &mut Vec<NativeDylibModuleCacheVariantDir>,
) -> Result<(), ModuleLoaderStoreError> {
    for entry in fs::read_dir(dir).map_err(|source| ModuleLoaderStoreError::Io {
        path: dir.display().to_string(),
        detail: source.to_string(),
    })? {
        let entry = entry.map_err(|source| ModuleLoaderStoreError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ModuleLoaderStoreError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if !file_type.is_dir() {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some(".staged") {
            continue;
        }
        if let Some(variant) = native_dylib_module_cache_variant_dir_from_path(cache_root, &path)? {
            variants.push(variant);
            continue;
        }
        collect_native_dylib_module_cache_variant_dirs(cache_root, &path, variants)?;
    }
    Ok(())
}

fn native_dylib_module_cache_variant_dir_from_path(
    cache_root: &Path,
    path: &Path,
) -> Result<Option<NativeDylibModuleCacheVariantDir>, ModuleLoaderStoreError> {
    let relative = path
        .strip_prefix(cache_root)
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: format!(
                "native module cache path is not under {}: {source}",
                cache_root.display()
            ),
        })?;
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_string_lossy().into_owned(),
            _ => String::new(),
        })
        .collect::<Vec<_>>();
    let Some(hash_key) = components.last() else {
        return Ok(None);
    };
    if !hash_key.starts_with("blake3_")
        || components.len() < 2
        || components.iter().any(|component| component.is_empty())
    {
        return Ok(None);
    }
    let cache_key = components[..components.len() - 1].join("/");
    validate_native_dylib_module_cache_key(&cache_key).map_err(|source| {
        ModuleLoaderStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    Ok(Some(NativeDylibModuleCacheVariantDir {
        cache_key,
        path: path.to_path_buf(),
        modified,
    }))
}

fn collect_native_dylib_module_cache_files(
    cache_root: &Path,
    dir: &Path,
    excluded_cache_dirs: &BTreeSet<PathBuf>,
    paths: &mut Vec<PathBuf>,
) -> Result<(), ModuleLoaderStoreError> {
    for entry in fs::read_dir(dir).map_err(|source| ModuleLoaderStoreError::Io {
        path: dir.display().to_string(),
        detail: source.to_string(),
    })? {
        let entry = entry.map_err(|source| ModuleLoaderStoreError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ModuleLoaderStoreError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if file_type.is_dir() {
            if excluded_cache_dirs.contains(&path)
                || path.file_name().and_then(|name| name.to_str()) == Some(".staged")
            {
                continue;
            }
            collect_native_dylib_module_cache_files(cache_root, &path, excluded_cache_dirs, paths)?;
        } else if file_type.is_file()
            && path
                .strip_prefix(cache_root)
                .ok()
                .and_then(|relative| relative.parent())
                .is_some()
        {
            paths.push(path);
        }
    }
    Ok(())
}

fn native_dylib_module_cache_entry_from_path(
    cache_root: &Path,
    path: &Path,
) -> Result<Option<NativeDylibModuleCacheEntry>, NativeDylibModuleInstallError> {
    let relative = path.strip_prefix(cache_root).map_err(|source| {
        NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native module cache path `{}` is not under `{}`: {source}",
                path.display(),
                cache_root.display()
            ),
        }
    })?;
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_string_lossy().into_owned(),
            _ => String::new(),
        })
        .collect::<Vec<_>>();
    if components.len() < 3 || components.iter().any(|component| component.is_empty()) {
        return Ok(None);
    }
    let library_name = components.last().expect("component length checked").clone();
    let hash_key = &components[components.len() - 2];
    let Some(hash_suffix) = hash_key.strip_prefix("blake3_") else {
        return Ok(None);
    };
    let content_hash = format!("blake3:{hash_suffix}");
    let cache_key = components[..components.len() - 2].join("/");
    let cache_key = validate_native_dylib_module_cache_key(&cache_key)?;
    let library_name = validate_native_dylib_module_library_name(&library_name)?;
    let descriptor =
        unsafe { NativeModule::descriptor_from_content_addressed_path(path, &content_hash) }?;
    let module_id = parse_native_dylib_module_id(&descriptor.module_id)?;
    Ok(Some(NativeDylibModuleCacheEntry {
        module_id,
        content_hash,
        cache_key,
        library_name,
        platform_triple: descriptor.platform_triple,
        native_abi_version: descriptor.native_abi_version,
        interface_version: descriptor.interface_version,
        library_path: path.to_path_buf(),
    }))
}

pub fn read_native_rust_module_runtime_states(
    data_dir_root: &Path,
) -> Result<Vec<NativeRustModuleRuntimeState>, ModuleLoaderStoreError> {
    read_objects_from_dir(&native_rust_module_loader_state_dir(data_dir_root))
}

fn read_objects_from_dir<T: DeserializeOwned>(
    dir: &Path,
) -> Result<Vec<T>, ModuleLoaderStoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(dir)
        .map_err(|source| ModuleLoaderStoreError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| ModuleLoaderStoreError::Io {
                    path: dir.display().to_string(),
                    detail: source.to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    let mut objects = Vec::new();
    for path in paths {
        if path.extension().and_then(|extension| extension.to_str()) != Some("sobj") {
            continue;
        }
        objects.push(read_object(&path)?);
    }
    Ok(objects)
}

pub fn write_native_dylib_module_loader_state(
    data_dir_root: &Path,
    state: &NativeDylibModuleLoaderState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    write_native_dylib_module_loader_state_unlocked(data_dir_root, state)
}

fn write_native_dylib_module_loader_state_unlocked(
    data_dir_root: &Path,
    state: &NativeDylibModuleLoaderState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = native_dylib_module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    sync_module_loader_file(&path)?;
    if let Some(parent) = path.parent() {
        sync_module_loader_dir(parent)?;
    }
    Ok(path)
}

pub fn native_dylib_module_registration_from_loader_state(
    data_dir_root: &Path,
    state: &NativeDylibModuleLoaderState,
) -> Result<ModuleRegistration, NativeDylibModuleInstallError> {
    let library_path = native_module_cache_path(
        data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let module = unsafe {
        NativeModule::load_from_content_addressed_path(&library_path, &state.content_hash)
    }?;
    native_dylib_module_registration_from_state_and_descriptor(state, module.descriptor())
}

pub fn active_native_dylib_module_registrations(
    data_dir_root: &Path,
) -> Result<Vec<ModuleRegistration>, NativeDylibModuleInstallError> {
    let states = read_native_dylib_module_loader_states(data_dir_root)?;
    states
        .iter()
        .map(|state| native_dylib_module_registration_from_loader_state(data_dir_root, state))
        .collect()
}

pub fn native_dylib_module_registration_from_state_and_descriptor(
    state: &NativeDylibModuleLoaderState,
    descriptor: &NativeModuleDescriptor,
) -> Result<ModuleRegistration, NativeDylibModuleInstallError> {
    validate_native_dylib_descriptor_matches_state(state, descriptor)?;
    let module_id = parse_native_dylib_module_id(&descriptor.module_id)?;
    let owner = if descriptor.owner.trim().is_empty() {
        descriptor.module_id.clone()
    } else {
        descriptor.owner.clone()
    };
    let mut registration = ModuleRegistration {
        manifest: sinomo_core::ModuleManifest::native_dylib(
            module_id,
            descriptor.module_version.clone(),
            owner,
            descriptor.interface_version.clone(),
            TrustRing::R1TrustedRuntime,
        ),
        backend: ModuleBackendRegistration::NativeDylib {
            artifact_id: state
                .artifact_id
                .clone()
                .unwrap_or_else(|| native_dylib_fallback_artifact_id(&descriptor.module_id)),
            content_hash: state.content_hash.clone(),
            library_name: state.library_name.clone(),
            platform_triple: state.platform_triple.clone(),
            interface_version: state.interface_version.clone(),
        },
        surfaces: native_descriptor_surfaces_to_module_surfaces(descriptor)?,
    };
    registration.manifest.capabilities = registration.surfaces.capabilities.clone();
    Ok(registration)
}

fn native_dylib_fallback_artifact_id(module_id: &str) -> ArtifactId {
    ArtifactId::from_legacy_named(format!("artifact.{module_id}"))
        .expect("module id-derived artifact id should be valid")
}

fn validate_native_dylib_descriptor_matches_state(
    state: &NativeDylibModuleLoaderState,
    descriptor: &NativeModuleDescriptor,
) -> Result<(), NativeDylibModuleInstallError> {
    let descriptor_module_id = parse_native_dylib_module_id(&descriptor.module_id)?;
    if descriptor_module_id != state.module_id.clone() {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib descriptor module_id `{}` does not match loader state `{}`",
                descriptor.module_id, state.module_id
            ),
        });
    }
    if descriptor.native_abi_version != state.native_abi_version {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib descriptor ABI {} does not match loader state {}",
                descriptor.native_abi_version, state.native_abi_version
            ),
        });
    }
    if descriptor.interface_version != state.interface_version {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib descriptor interface `{}` does not match loader state `{}`",
                descriptor.interface_version, state.interface_version
            ),
        });
    }
    if descriptor.platform_triple != state.platform_triple {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib descriptor platform `{}` does not match loader state `{}`",
                descriptor.platform_triple, state.platform_triple
            ),
        });
    }
    Ok(())
}

fn native_descriptor_surfaces_to_module_surfaces(
    descriptor: &NativeModuleDescriptor,
) -> Result<ModuleSurfaces, NativeDylibModuleInstallError> {
    let mut surfaces = ModuleSurfaces::default();
    for owner in &descriptor.surfaces.storage_owners {
        surfaces = surfaces.with_storage_owner(owner.clone());
    }
    for route in &descriptor.surfaces.object_rpc_routes {
        surfaces = surfaces.with_object_rpc_route(native_descriptor_route_to_module_route(route)?);
    }
    for resource in &descriptor.surfaces.declarative_resources {
        surfaces = surfaces
            .with_declarative_resource(native_descriptor_resource_to_module_resource(resource)?);
    }
    for transport in &descriptor.surfaces.network_transports {
        surfaces = surfaces.with_network_transport(transport.clone());
    }
    surfaces.capabilities = ModuleCapabilities {
        imports: descriptor
            .surfaces
            .capability_imports
            .iter()
            .map(|capability| native_descriptor_capability(capability))
            .collect::<Result<Vec<_>, _>>()?,
        exports: descriptor
            .surfaces
            .capability_exports
            .iter()
            .map(|capability| native_descriptor_capability(capability))
            .collect::<Result<Vec<_>, _>>()?,
    };
    surfaces
        .validate()
        .map_err(|source| NativeDylibModuleInstallError::Descriptor {
            detail: source.to_string(),
        })?;
    Ok(surfaces)
}

fn native_descriptor_route_to_module_route(
    route: &NativeModuleObjectRpcRoute,
) -> Result<ModuleObjectRpcRoute, NativeDylibModuleInstallError> {
    Ok(ModuleObjectRpcRoute::new(
        route.route.clone(),
        route.category.clone(),
        route.operation.clone(),
        native_descriptor_route_auth(&route.auth, &route.route)?,
    ))
}

fn native_descriptor_route_auth(
    auth: &str,
    route: &str,
) -> Result<ModuleRouteAuth, NativeDylibModuleInstallError> {
    match auth {
        "local-command" => Ok(ModuleRouteAuth::LocalCommand),
        "local-or-cluster-member" => Ok(ModuleRouteAuth::LocalOrClusterMember),
        "cluster-member" => Ok(ModuleRouteAuth::ClusterMember),
        "admin" => Ok(ModuleRouteAuth::Admin),
        other => Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!("native dylib route `{route}` has unsupported auth `{other}`"),
        }),
    }
}

fn native_descriptor_resource_to_module_resource(
    resource: &NativeModuleDeclarativeResource,
) -> Result<ModuleDeclarativeResourceExport, NativeDylibModuleInstallError> {
    let mut export = ModuleDeclarativeResourceExport::new(
        resource.id.clone(),
        native_descriptor_resource_kind(&resource.kind, &resource.id)?,
        resource.path.clone(),
    )
    .with_description(resource.description.clone());
    for target_agent in &resource.target_agents {
        export = export.with_target_agent(target_agent.clone());
    }
    for model_family in &resource.model_families {
        export = export.with_model_family(model_family.clone());
    }
    for activation_tag in &resource.activation_tags {
        export = export.with_activation_tag(activation_tag.clone());
    }
    for capability in &resource.required_capabilities {
        export = export.with_required_capability(native_descriptor_capability(capability)?);
    }
    if let Some(text) = resource.embedded_text.clone() {
        export = export.with_embedded_text(text);
    }
    Ok(export)
}

fn native_descriptor_resource_kind(
    kind: &str,
    id: &str,
) -> Result<ModuleDeclarativeResourceKind, NativeDylibModuleInstallError> {
    match kind {
        "skill" => Ok(ModuleDeclarativeResourceKind::Skill),
        "prompt" => Ok(ModuleDeclarativeResourceKind::Prompt),
        "prompt-pack" => Ok(ModuleDeclarativeResourceKind::PromptPack),
        "eval-suite" => Ok(ModuleDeclarativeResourceKind::EvalSuite),
        "example" => Ok(ModuleDeclarativeResourceKind::Example),
        "policy" => Ok(ModuleDeclarativeResourceKind::Policy),
        "config" => Ok(ModuleDeclarativeResourceKind::Config),
        other => Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib declarative resource `{id}` has unsupported kind `{other}`"
            ),
        }),
    }
}

fn native_descriptor_capability(
    capability: &str,
) -> Result<CapabilityName, NativeDylibModuleInstallError> {
    CapabilityName::new(capability.to_string()).map_err(|source| {
        NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native dylib descriptor capability `{capability}` is invalid: {source}"
            ),
        }
    })
}

pub fn remove_native_dylib_module_loader_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<NativeDylibModuleLoaderState>, ModuleLoaderStoreError> {
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    let path = native_dylib_module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    let state = read_object(&path)?;
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ModuleLoaderStoreError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    if let Some(parent) = path.parent() {
        sync_module_loader_dir(parent)?;
    }
    Ok(Some(state))
}

pub fn read_native_rust_module_runtime_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<NativeRustModuleRuntimeState>, ModuleLoaderStoreError> {
    let path = native_rust_module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_native_rust_module_runtime_state(
    data_dir_root: &Path,
    state: &NativeRustModuleRuntimeState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = native_rust_module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    Ok(path)
}

pub fn set_native_rust_module_enabled(
    data_dir_root: &Path,
    module_id: &ModuleId,
    enabled: bool,
    source: impl Into<String>,
    reason: impl Into<String>,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let state = NativeRustModuleRuntimeState {
        module_id: module_id.clone(),
        enabled,
        updated_at: module_loader_applied_at_now(),
        source: source.into(),
        reason: reason.into(),
    };
    write_native_rust_module_runtime_state(data_dir_root, &state)
}

pub fn native_rust_module_enabled(
    data_dir_root: &Path,
    module_id: &ModuleId,
    default_enabled: bool,
) -> Result<bool, ModuleLoaderStoreError> {
    Ok(
        read_native_rust_module_runtime_state(data_dir_root, module_id)?
            .map(|state| state.enabled)
            .unwrap_or(default_enabled),
    )
}

pub fn read_module_unload_record(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<ModuleUnloadRecord>, ModuleLoaderStoreError> {
    let path = module_unload_record_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_module_unload_record(
    data_dir_root: &Path,
    record: &ModuleUnloadRecord,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = module_unload_record_path(data_dir_root, &record.module_id);
    write_object(&path, record)?;
    Ok(path)
}

pub fn read_module_local_command_registry(
    data_dir_root: &Path,
) -> Result<Option<ModuleLocalCommandRegistry>, ModuleLoaderStoreError> {
    let path = module_local_command_registry_path(data_dir_root);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_module_local_command_registry(
    data_dir_root: &Path,
    registry: &ModuleLocalCommandRegistry,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = module_local_command_registry_path(data_dir_root);
    write_object(&path, registry)?;
    Ok(path)
}

pub fn read_declarative_module_registration(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<ModuleRegistration>, ModuleLoaderStoreError> {
    let path = declarative_module_catalog_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn read_declarative_module_registrations(
    data_dir_root: &Path,
) -> Result<Vec<ModuleRegistration>, ModuleLoaderStoreError> {
    read_objects_from_dir(&declarative_module_catalog_dir(data_dir_root))
}

pub fn write_declarative_module_registration(
    data_dir_root: &Path,
    registration: &ModuleRegistration,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = declarative_module_catalog_path(data_dir_root, registration.module_id());
    write_object(&path, registration)?;
    Ok(path)
}

pub fn read_declarative_module_loader_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<DeclarativeModuleLoaderState>, ModuleLoaderStoreError> {
    let path = declarative_module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn read_declarative_module_loader_states(
    data_dir_root: &Path,
) -> Result<Vec<DeclarativeModuleLoaderState>, ModuleLoaderStoreError> {
    read_objects_from_dir(&declarative_module_loader_state_dir(data_dir_root))
}

pub fn write_declarative_module_loader_state(
    data_dir_root: &Path,
    state: &DeclarativeModuleLoaderState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = declarative_module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    Ok(path)
}

pub fn module_loader_applied_at_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn install_declarative_module_package(
    data_dir_root: &Path,
    source_path: &Path,
    request: DeclarativeModuleInstallRequest,
) -> Result<DeclarativeModuleInstallResult, DeclarativeModuleInstallError> {
    let package = collect_declarative_module_package(source_path)?;
    validate_declarative_module_resource_paths(&package.files, &package.manifest)?;

    let content_hash = declarative_module_content_hash(&package.files);
    let package_manifest = package.manifest;
    let module_id = parse_declarative_module_id(&package_manifest.module_id)?;
    let version = package_manifest.version.clone();
    let payload_ref = declarative_module_payload_ref(&module_id, &version, &content_hash);
    let registration = package_manifest.into_registration(payload_ref.clone())?;
    if registration.manifest.kind != ModuleKind::Declarative {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: "declarative package manifest produced a non-declarative module".to_string(),
        });
    }
    if registration.module_id() != &module_id {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: format!(
                "manifest module id `{}` did not round-trip to registration module id `{}`",
                module_id,
                registration.module_id()
            ),
        });
    }

    let payload_path =
        declarative_module_payload_cache_path(data_dir_root, &module_id, &version, &content_hash);
    copy_declarative_module_package_files(&payload_path, &package.files)?;
    let catalog_path = write_declarative_module_registration(data_dir_root, &registration)?;
    let state = DeclarativeModuleLoaderState {
        module_id,
        version,
        content_hash,
        payload_ref,
        active: request.activate,
        installed_at: module_loader_applied_at_now(),
        source: request.source,
    };
    let state_path = if request.activate {
        Some(write_declarative_module_loader_state(
            data_dir_root,
            &state,
        )?)
    } else {
        None
    };

    Ok(DeclarativeModuleInstallResult {
        registration,
        state,
        payload_path,
        catalog_path,
        state_path,
    })
}

struct CollectedDeclarativeModulePackage {
    manifest: DeclarativeModulePackageManifest,
    files: Vec<(PathBuf, Vec<u8>)>,
}

fn collect_declarative_module_package(
    source_path: &Path,
) -> Result<CollectedDeclarativeModulePackage, DeclarativeModuleInstallError> {
    if source_path.is_dir() {
        collect_declarative_module_directory_package(source_path)
    } else if source_path.extension().and_then(|value| value.to_str()) == Some("zip") {
        collect_declarative_module_zip_package(source_path)
    } else {
        Err(DeclarativeModuleInstallError::Descriptor {
            detail: format!(
                "declarative module package source must be a directory or .zip archive: {}",
                source_path.display()
            ),
        })
    }
}

fn collect_declarative_module_directory_package(
    source_path: &Path,
) -> Result<CollectedDeclarativeModulePackage, DeclarativeModuleInstallError> {
    let manifest_path = declarative_module_package_manifest_path(source_path)?;
    let manifest = read_declarative_module_package_manifest(&manifest_path)?;
    let files = collect_declarative_module_package_files(source_path)?;
    Ok(CollectedDeclarativeModulePackage { manifest, files })
}

fn collect_declarative_module_zip_package(
    source_path: &Path,
) -> Result<CollectedDeclarativeModulePackage, DeclarativeModuleInstallError> {
    let file = fs::File::open(source_path).map_err(|source| DeclarativeModuleInstallError::Io {
        path: source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|source| DeclarativeModuleInstallError::Archive {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut files = Vec::new();
    for index in 0..archive.len() {
        let mut entry =
            archive
                .by_index(index)
                .map_err(|source| DeclarativeModuleInstallError::Archive {
                    path: source_path.display().to_string(),
                    detail: source.to_string(),
                })?;
        if entry.is_dir() {
            continue;
        }
        let Some(relative_path) = entry.enclosed_name() else {
            return Err(DeclarativeModuleInstallError::Archive {
                path: source_path.display().to_string(),
                detail: format!("zip entry `{}` escapes the archive root", entry.name()),
            });
        };
        validate_module_declarative_resource_path(
            &relative_path.display().to_string(),
            &relative_path.display().to_string(),
        )
        .map_err(|source| DeclarativeModuleInstallError::Descriptor {
            detail: source.to_string(),
        })?;
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(|source| DeclarativeModuleInstallError::Io {
                path: format!("{}:{}", source_path.display(), relative_path.display()),
                detail: source.to_string(),
            })?;
        files.push((relative_path, bytes));
    }
    normalize_declarative_zip_package_prefix(&mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let manifest = read_declarative_module_package_manifest_from_files(source_path, &files)?;
    Ok(CollectedDeclarativeModulePackage { manifest, files })
}

fn normalize_declarative_zip_package_prefix(
    files: &mut Vec<(PathBuf, Vec<u8>)>,
) -> Result<(), DeclarativeModuleInstallError> {
    if declarative_module_package_manifest_file_index(files).is_some() {
        return Ok(());
    }
    let mut common_prefix = None::<std::ffi::OsString>;
    for (relative_path, _) in files.iter() {
        let Some(Component::Normal(component)) = relative_path.components().next() else {
            return Ok(());
        };
        match common_prefix.as_ref() {
            Some(prefix) if prefix == component => {}
            Some(_) => return Ok(()),
            None => common_prefix = Some(component.to_os_string()),
        }
    }
    let Some(prefix) = common_prefix else {
        return Ok(());
    };
    for (relative_path, _) in files.iter_mut() {
        let stripped = relative_path
            .strip_prefix(Path::new(&prefix))
            .map_err(|source| DeclarativeModuleInstallError::Descriptor {
                detail: format!(
                    "failed to strip zip package prefix `{}` from {}: {source}",
                    prefix.to_string_lossy(),
                    relative_path.display()
                ),
            })?
            .to_path_buf();
        if stripped.as_os_str().is_empty() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: "zip package contains only a wrapper directory".to_string(),
            });
        }
        *relative_path = stripped;
    }
    Ok(())
}

fn declarative_module_package_manifest_file_index(files: &[(PathBuf, Vec<u8>)]) -> Option<usize> {
    files.iter().position(|(path, _)| {
        path == Path::new("sinomo.module.json")
            || path == Path::new("module.json")
            || path == Path::new("sinomo.module.sobj")
    })
}

fn read_declarative_module_package_manifest_from_files(
    source_path: &Path,
    files: &[(PathBuf, Vec<u8>)],
) -> Result<DeclarativeModulePackageManifest, DeclarativeModuleInstallError> {
    let index = declarative_module_package_manifest_file_index(files).ok_or_else(|| {
        DeclarativeModuleInstallError::Descriptor {
            detail: format!(
                "declarative module package {} does not contain sinomo.module.json or sinomo.module.sobj",
                source_path.display()
            ),
        }
    })?;
    let (path, bytes) = &files[index];
    match path.extension().and_then(|value| value.to_str()) {
        Some("sobj") => serde_ext::from_bytes(bytes).map_err(|source| {
            DeclarativeModuleInstallError::Store(ModuleLoaderStoreError::Object {
                path: format!("{}:{}", source_path.display(), path.display()),
                detail: source.to_string(),
            })
        }),
        Some("json") => {
            serde_json::from_slice(bytes).map_err(|source| DeclarativeModuleInstallError::Json {
                path: format!("{}:{}", source_path.display(), path.display()),
                detail: source.to_string(),
            })
        }
        _ => Err(DeclarativeModuleInstallError::Descriptor {
            detail: format!(
                "unsupported declarative module manifest format: {}",
                path.display()
            ),
        }),
    }
}

fn declarative_module_package_manifest_path(
    source_path: &Path,
) -> Result<PathBuf, DeclarativeModuleInstallError> {
    for file_name in ["sinomo.module.sobj", "sinomo.module.json", "module.json"] {
        let candidate = source_path.join(file_name);
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(DeclarativeModuleInstallError::Descriptor {
        detail: format!(
            "declarative module package {} does not contain sinomo.module.json or sinomo.module.sobj",
            source_path.display()
        ),
    })
}

fn read_declarative_module_package_manifest(
    manifest_path: &Path,
) -> Result<DeclarativeModulePackageManifest, DeclarativeModuleInstallError> {
    let extension = manifest_path.extension().and_then(|value| value.to_str());
    match extension {
        Some("sobj") => read_object(manifest_path).map_err(DeclarativeModuleInstallError::Store),
        Some("json") => {
            let bytes =
                fs::read(manifest_path).map_err(|source| DeclarativeModuleInstallError::Io {
                    path: manifest_path.display().to_string(),
                    detail: source.to_string(),
                })?;
            serde_json::from_slice(&bytes).map_err(|source| DeclarativeModuleInstallError::Json {
                path: manifest_path.display().to_string(),
                detail: source.to_string(),
            })
        }
        _ => Err(DeclarativeModuleInstallError::Descriptor {
            detail: format!(
                "unsupported declarative module manifest format: {}",
                manifest_path.display()
            ),
        }),
    }
}

fn validate_declarative_module_resource_paths(
    files: &[(PathBuf, Vec<u8>)],
    manifest: &DeclarativeModulePackageManifest,
) -> Result<(), DeclarativeModuleInstallError> {
    manifest
        .surfaces
        .validate()
        .map_err(|source| DeclarativeModuleInstallError::Descriptor {
            detail: source.to_string(),
        })?;
    for resource in manifest
        .surfaces
        .declarative_resources
        .iter()
        .chain(manifest.resources.iter())
    {
        resource
            .validate()
            .map_err(|source| DeclarativeModuleInstallError::Descriptor {
                detail: source.to_string(),
            })?;
        let resource_path = Path::new(&resource.path);
        let exists_in_package = files
            .iter()
            .any(|(path, _)| path == resource_path || path.starts_with(resource_path));
        if !exists_in_package {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: format!(
                    "declarative resource `{}` points to missing payload path `{}`",
                    resource.id, resource.path
                ),
            });
        }
    }
    Ok(())
}

fn collect_declarative_module_package_files(
    source_path: &Path,
) -> Result<Vec<(PathBuf, Vec<u8>)>, DeclarativeModuleInstallError> {
    let mut relative_paths = Vec::new();
    collect_declarative_module_package_file_paths(source_path, source_path, &mut relative_paths)?;
    relative_paths.sort();
    let mut files = Vec::new();
    for relative_path in relative_paths {
        let full_path = source_path.join(&relative_path);
        let bytes = fs::read(&full_path).map_err(|source| DeclarativeModuleInstallError::Io {
            path: full_path.display().to_string(),
            detail: source.to_string(),
        })?;
        files.push((relative_path, bytes));
    }
    if files.is_empty() {
        return Err(DeclarativeModuleInstallError::Descriptor {
            detail: "declarative module package contains no files".to_string(),
        });
    }
    Ok(files)
}

fn collect_declarative_module_package_file_paths(
    root: &Path,
    current: &Path,
    relative_paths: &mut Vec<PathBuf>,
) -> Result<(), DeclarativeModuleInstallError> {
    let entries = fs::read_dir(current).map_err(|source| DeclarativeModuleInstallError::Io {
        path: current.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| DeclarativeModuleInstallError::Io {
            path: current.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| DeclarativeModuleInstallError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if file_type.is_symlink() {
            return Err(DeclarativeModuleInstallError::Descriptor {
                detail: format!(
                    "declarative module package must not contain symlinks: {}",
                    path.display()
                ),
            });
        }
        if file_type.is_dir() {
            collect_declarative_module_package_file_paths(root, &path, relative_paths)?;
        } else if file_type.is_file() {
            let relative_path = path.strip_prefix(root).map_err(|source| {
                DeclarativeModuleInstallError::Descriptor {
                    detail: format!(
                        "failed to derive package-relative path for {}: {source}",
                        path.display()
                    ),
                }
            })?;
            validate_module_declarative_resource_path(
                &relative_path.display().to_string(),
                &relative_path.display().to_string(),
            )
            .map_err(|source| DeclarativeModuleInstallError::Descriptor {
                detail: source.to_string(),
            })?;
            relative_paths.push(relative_path.to_path_buf());
        }
    }
    Ok(())
}

fn declarative_module_content_hash(files: &[(PathBuf, Vec<u8>)]) -> String {
    let mut hasher = blake3::Hasher::new();
    for (relative_path, bytes) in files {
        hasher.update(relative_path.to_string_lossy().as_bytes());
        hasher.update(&[0]);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&[0]);
        hasher.update(bytes);
        hasher.update(&[0]);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn copy_declarative_module_package_files(
    payload_path: &Path,
    files: &[(PathBuf, Vec<u8>)],
) -> Result<(), DeclarativeModuleInstallError> {
    fs::create_dir_all(payload_path).map_err(|source| DeclarativeModuleInstallError::Io {
        path: payload_path.display().to_string(),
        detail: source.to_string(),
    })?;
    for (relative_path, bytes) in files {
        let destination = payload_path.join(relative_path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| DeclarativeModuleInstallError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&destination, bytes).map_err(|source| DeclarativeModuleInstallError::Io {
            path: destination.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

pub fn install_native_dylib_module_from_file(
    data_dir_root: &Path,
    source_path: &Path,
    request: NativeDylibModuleInstallRequest,
) -> Result<NativeDylibModuleInstallResult, NativeDylibModuleInstallError> {
    let bytes = fs::read(source_path).map_err(|source| NativeDylibModuleInstallError::Io {
        path: source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    install_native_dylib_module_from_path_and_bytes(data_dir_root, source_path, &bytes, request)
}

pub fn install_native_dylib_module_from_bytes(
    data_dir_root: &Path,
    bytes: &[u8],
    request: NativeDylibModuleInstallRequest,
) -> Result<NativeDylibModuleInstallResult, NativeDylibModuleInstallError> {
    let Some(library_name) = request.library_name.as_deref() else {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: "native dylib bytes require an explicit library name".to_string(),
        });
    };
    let library_name = validate_native_dylib_module_library_name(library_name)?;
    let _lock = acquire_native_dylib_module_state_lock(data_dir_root)?;
    recover_native_dylib_module_loader_state_transaction_unlocked(data_dir_root)?;
    let staging_path = native_dylib_module_staging_path(data_dir_root, &library_name);
    write_native_dylib_module_file(&staging_path, bytes, None)?;
    let result = install_native_dylib_module_from_path_and_bytes(
        data_dir_root,
        &staging_path,
        bytes,
        request,
    );
    if let Some(parent) = staging_path.parent() {
        let _ = fs::remove_dir_all(parent);
    } else {
        let _ = fs::remove_file(&staging_path);
    }
    result
}

fn install_native_dylib_module_from_path_and_bytes(
    data_dir_root: &Path,
    descriptor_path: &Path,
    bytes: &[u8],
    request: NativeDylibModuleInstallRequest,
) -> Result<NativeDylibModuleInstallResult, NativeDylibModuleInstallError> {
    let content_hash = native_module_content_hash(bytes);
    let library_name = match request.library_name.as_deref() {
        Some(library_name) => validate_native_dylib_module_library_name(library_name)?,
        None => native_dylib_module_source_library_name(descriptor_path)?,
    };
    let native_module =
        unsafe { NativeModule::load_from_content_addressed_path(descriptor_path, &content_hash) }?;
    let descriptor = native_module.descriptor().clone();
    drop(native_module);

    let descriptor_module_id = descriptor.module_id.clone();
    let module_id = parse_native_dylib_module_id(&descriptor.module_id)?;
    if let Some(expected) = request.expected_module_id.as_ref()
        && module_id != *expected
    {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native module descriptor module_id `{}` does not match expected `{expected}`",
                descriptor.module_id
            ),
        });
    }
    if let Some(expected) = request.expected_interface_version.as_ref()
        && descriptor.interface_version != *expected
    {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!(
                "native module descriptor interface `{}` does not match expected `{expected}`",
                descriptor.interface_version
            ),
        });
    }

    let cache_key = request
        .cache_key
        .unwrap_or_else(|| descriptor.module_id.replace('.', "/"));
    let cache_key = validate_native_dylib_module_cache_key(&cache_key)?;
    let cache_path =
        native_module_cache_path(data_dir_root, &cache_key, &content_hash, &library_name);
    write_native_dylib_module_file(&cache_path, bytes, Some(descriptor_path))?;

    let state = NativeDylibModuleLoaderState {
        module_id,
        artifact_id: request.artifact_id,
        content_hash,
        cache_key,
        library_name,
        platform_triple: descriptor.platform_triple,
        native_abi_version: descriptor.native_abi_version,
        interface_version: descriptor.interface_version,
        activated_at: module_loader_applied_at_now(),
        source: request.source,
    };
    let state_path = write_native_dylib_module_loader_state_unlocked(data_dir_root, &state)?;
    Ok(NativeDylibModuleInstallResult {
        descriptor_module_id,
        state,
        cache_path,
        state_path,
    })
}

pub fn validate_native_dylib_module_cache_key(
    value: &str,
) -> Result<String, NativeDylibModuleInstallError> {
    let trimmed = non_empty_native_dylib_value(value, "native module cache key")?;
    let path = Path::new(&trimmed);
    if path.is_absolute() {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!("native module cache key `{trimmed}` must be relative"),
        });
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(NativeDylibModuleInstallError::Descriptor {
                    detail: format!(
                        "native module cache key `{trimmed}` must not contain `.` or `..`"
                    ),
                });
            }
        }
    }
    Ok(trimmed)
}

pub fn validate_native_dylib_module_library_name(
    value: &str,
) -> Result<String, NativeDylibModuleInstallError> {
    let trimmed = non_empty_native_dylib_value(value, "native module library name")?;
    let path = Path::new(&trimmed);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(trimmed),
        _ => Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!("native module library name `{trimmed}` must be a file name"),
        }),
    }
}

fn native_dylib_module_source_library_name(
    path: &Path,
) -> Result<String, NativeDylibModuleInstallError> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!("native module path `{}` has no file name", path.display()),
        });
    };
    validate_native_dylib_module_library_name(file_name)
}

fn non_empty_native_dylib_value(
    value: &str,
    label: &str,
) -> Result<String, NativeDylibModuleInstallError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(NativeDylibModuleInstallError::Descriptor {
            detail: format!("{label} must be non-empty"),
        });
    }
    Ok(trimmed)
}

pub fn parse_native_dylib_module_id(
    value: &str,
) -> Result<ModuleId, NativeDylibModuleInstallError> {
    ModuleId::new(value.to_string())
        .or_else(|_| ModuleId::from_legacy_named(value))
        .map_err(|source| NativeDylibModuleInstallError::Descriptor {
            detail: format!("invalid native module id `{value}`: {source}"),
        })
}

fn native_dylib_module_staging_path(data_dir_root: &Path, library_name: &str) -> PathBuf {
    data_dir_root
        .join("module")
        .join("cache")
        .join("native-dylib")
        .join(".staged")
        .join(format!(
            "{}-{}",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
        .join(library_name)
}

fn write_native_dylib_module_file(
    path: &Path,
    bytes: &[u8],
    permissions_source: Option<&Path>,
) -> Result<(), NativeDylibModuleInstallError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NativeDylibModuleInstallError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if path.exists() {
        let existing = fs::read(path).map_err(|source| NativeDylibModuleInstallError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if native_module_content_hash(&existing) == native_module_content_hash(bytes) {
            return Ok(());
        }
        fs::remove_file(path).map_err(|source| NativeDylibModuleInstallError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let tmp_path = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("native-module"),
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    fs::write(&tmp_path, bytes).map_err(|source| NativeDylibModuleInstallError::Io {
        path: tmp_path.display().to_string(),
        detail: source.to_string(),
    })?;
    if let Some(source_path) = permissions_source
        && let Ok(metadata) = fs::metadata(source_path)
    {
        let _ = fs::set_permissions(&tmp_path, metadata.permissions());
    }
    #[cfg(unix)]
    if permissions_source.is_none() {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755));
    }
    fs::rename(&tmp_path, path).map_err(|source| NativeDylibModuleInstallError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn route_module_invocation(
    data_dir_root: &Path,
    request: &ModuleRouteRequest,
) -> Result<Option<ModuleRouteDecision>, ModuleLoaderStoreError> {
    let Some(state) = read_module_loader_runtime_state(data_dir_root, &request.module_id)? else {
        return Ok(None);
    };
    Ok(Some(route_module_invocation_from_state(
        &state,
        &request.route_key,
    )))
}

pub fn route_module_invocation_from_state(
    state: &ModuleLoaderRuntimeState,
    route_key: &str,
) -> ModuleRouteDecision {
    let bucket = stable_route_bucket_bps(&state.module_id, route_key);
    let weight = state.traffic_weight_bps.min(10_000);
    let candidate_is_primary = matches!(
        state.loader_state.hot_swap_state,
        HotSwapState::ActiveCandidate
    ) && weight > 0
        && weight < 10_000
        && bucket < weight
        && state.loader_state.candidate_artifact_id.is_some();
    let primary_artifact_id = if candidate_is_primary {
        state
            .loader_state
            .candidate_artifact_id
            .clone()
            .expect("checked candidate artifact")
    } else {
        state.loader_state.active_artifact_id.clone()
    };
    let primary_lane = if candidate_is_primary {
        ModuleRouteLane::Candidate
    } else {
        ModuleRouteLane::Active
    };
    let shadow_artifact_id = matches!(state.loader_state.hot_swap_state, HotSwapState::Shadowing)
        .then(|| state.loader_state.shadow_artifact_id.clone())
        .flatten();
    let reason = match (&primary_lane, &shadow_artifact_id) {
        (ModuleRouteLane::Candidate, _) => {
            "canary route selected candidate artifact by stable traffic bucket".to_string()
        }
        (ModuleRouteLane::Active, Some(_)) => {
            "shadow route keeps active artifact primary and mirrors to shadow artifact".to_string()
        }
        (ModuleRouteLane::Active, None) => {
            "route selected active artifact from loader runtime state".to_string()
        }
    };
    ModuleRouteDecision {
        node_id: state.node_id.clone(),
        module_id: state.module_id.clone(),
        primary_artifact_id,
        primary_lane,
        shadow_artifact_id,
        traffic_weight_bps: weight,
        route_bucket_bps: bucket,
        hot_swap_state: state.loader_state.hot_swap_state.clone(),
        reason,
    }
}

pub fn execute_routed_module_invocation<R: ModuleArtifactRunner>(
    data_dir_root: &Path,
    request: &ModuleInvocationRequest,
    runner: &R,
) -> Result<Option<ModuleInvocationOutcome>, ModuleLoaderStoreError> {
    let Some(route) = route_module_invocation(
        data_dir_root,
        &ModuleRouteRequest {
            module_id: request.module_id.clone(),
            route_key: request.route_key.clone(),
        },
    )?
    else {
        return Ok(None);
    };
    Ok(Some(execute_module_route_decision(
        route,
        request.input.clone(),
        runner,
    )))
}

pub fn execute_routed_module_invocation_with_event_store<R, S>(
    data_dir_root: &Path,
    request: &ModuleInvocationRequest,
    runner: &R,
    event_store: &mut S,
) -> Result<Option<ModuleInvocationOutcome>, ModuleInvocationTelemetryError>
where
    R: ModuleArtifactRunner,
    S: EventStore,
{
    let outcome = execute_routed_module_invocation(data_dir_root, request, runner)?;
    if let Some(outcome) = &outcome {
        event_store.append(module_invocation_decision_event_from_outcome(
            None,
            &request.route_key,
            None,
            "live",
            outcome,
        ))?;
        if let Some(event) = module_shadow_decision_event_from_outcome(
            None,
            &request.route_key,
            None,
            "live",
            outcome,
        ) {
            event_store.append(event)?;
        }
    }
    Ok(outcome)
}

pub fn execute_module_route_decision<R: ModuleArtifactRunner>(
    route: ModuleRouteDecision,
    input: String,
    runner: &R,
) -> ModuleInvocationOutcome {
    let primary_lane = match &route.primary_lane {
        ModuleRouteLane::Active => ModuleInvocationLane::Active,
        ModuleRouteLane::Candidate => ModuleInvocationLane::Candidate,
    };
    let primary = invoke_module_artifact(
        runner,
        ModuleArtifactInvocation {
            node_id: route.node_id.clone(),
            module_id: route.module_id.clone(),
            artifact_id: route.primary_artifact_id.clone(),
            lane: primary_lane,
            input: input.clone(),
        },
    );
    let shadow = route
        .shadow_artifact_id
        .as_ref()
        .filter(|artifact_id| **artifact_id != route.primary_artifact_id)
        .map(|artifact_id| {
            invoke_module_artifact(
                runner,
                ModuleArtifactInvocation {
                    node_id: route.node_id.clone(),
                    module_id: route.module_id.clone(),
                    artifact_id: artifact_id.clone(),
                    lane: ModuleInvocationLane::Shadow,
                    input,
                },
            )
        });
    ModuleInvocationOutcome {
        route,
        primary,
        shadow,
    }
}

pub fn run_module_replay_fixture<R: ModuleArtifactRunner>(
    data_dir_root: &Path,
    fixture: &ModuleReplayFixture,
    runner: &R,
) -> Result<ModuleReplayReport, ModuleLoaderStoreError> {
    let mut cases = Vec::with_capacity(fixture.cases.len());
    let mut replay_event_count = 0_u64;
    let mut missing_route_count = 0_u64;
    let mut primary_error_count = 0_u64;
    let mut shadow_error_count = 0_u64;
    let mut shadow_mismatch_count = 0_u64;
    let mut events = Vec::new();

    for case in &fixture.cases {
        replay_event_count += u64::from(case.source_event.is_some());
        let outcome = execute_routed_module_invocation(
            data_dir_root,
            &ModuleInvocationRequest {
                module_id: fixture.module_id.clone(),
                route_key: case.route_key.clone(),
                input: case.input.clone(),
            },
            runner,
        )?;
        if outcome.is_none() {
            missing_route_count += 1;
        }
        if outcome
            .as_ref()
            .is_some_and(|outcome| outcome.primary.error.is_some())
        {
            primary_error_count += 1;
        }
        if outcome
            .as_ref()
            .and_then(|outcome| outcome.shadow.as_ref())
            .is_some_and(|shadow| shadow.error.is_some())
        {
            shadow_error_count += 1;
        }
        let shadow_matched = outcome.as_ref().and_then(shadow_result_matches_primary);
        if shadow_matched == Some(false) {
            shadow_mismatch_count += 1;
        }
        let case_outcome = ModuleReplayCaseOutcome {
            case_id: case.case_id.clone(),
            route_key: case.route_key.clone(),
            source_event_id: case
                .source_event
                .as_ref()
                .map(|event| event.event_id.clone()),
            outcome,
            shadow_matched,
        };
        if let Some(event) = module_shadow_decision_event(&case_outcome) {
            events.push(event);
        }
        cases.push(case_outcome);
    }

    Ok(ModuleReplayReport {
        fixture_id: fixture.fixture_id.clone(),
        module_id: fixture.module_id.clone(),
        cases,
        replay_event_count,
        missing_route_count,
        primary_error_count,
        shadow_error_count,
        shadow_mismatch_count,
        events,
    })
}

fn invoke_module_artifact<R: ModuleArtifactRunner>(
    runner: &R,
    invocation: ModuleArtifactInvocation,
) -> ModuleArtifactInvocationResult {
    let artifact_id = invocation.artifact_id.clone();
    let lane = invocation.lane.clone();
    let started = Instant::now();
    let result = runner.invoke(&invocation);
    let latency_ms = Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
    match result {
        Ok(output) => ModuleArtifactInvocationResult {
            artifact_id,
            lane,
            output: Some(output),
            error: None,
            latency_ms,
        },
        Err(error) => ModuleArtifactInvocationResult {
            artifact_id,
            lane,
            output: None,
            error: Some(error),
            latency_ms,
        },
    }
}

fn run_local_command_artifact(
    artifact: &ModuleLocalCommandArtifact,
    invocation: &ModuleArtifactInvocation,
    default_cwd: &Path,
) -> Result<String, String> {
    if artifact.program.trim().is_empty() {
        return Err(format!(
            "local command artifact {} has an empty program",
            artifact.artifact_id
        ));
    }
    let stdout_path = local_command_output_path(&artifact.artifact_id, "stdout");
    let stderr_path = local_command_output_path(&artifact.artifact_id, "stderr");
    let stdout = fs::File::create(&stdout_path).map_err(|source| {
        format!(
            "failed to create stdout capture for artifact {}: {source}",
            artifact.artifact_id
        )
    })?;
    let stderr = match fs::File::create(&stderr_path) {
        Ok(stderr) => stderr,
        Err(source) => {
            cleanup_output_capture(&stdout_path);
            return Err(format!(
                "failed to create stderr capture for artifact {}: {source}",
                artifact.artifact_id
            ));
        }
    };
    let mut command = Command::new(&artifact.program);
    command
        .args(&artifact.args)
        .current_dir(artifact.cwd.as_deref().unwrap_or(default_cwd))
        .env("SINOMO_NODE_ID", invocation.node_id.as_str())
        .env("SINOMO_MODULE_ID", invocation.module_id.as_str())
        .env("SINOMO_ARTIFACT_ID", invocation.artifact_id.as_str())
        .env(
            "SINOMO_INVOCATION_LANE",
            invocation_lane_label(&invocation.lane),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for (key, value) in &artifact.env {
        command.env(key, value);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => {
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "failed to spawn local command artifact {} (`{}`): {source}",
                artifact.artifact_id, artifact.program
            ));
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        if let Err(source) = stdin.write_all(invocation.input.as_bytes()) {
            let _ = child.kill();
            let _ = child.wait();
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "failed to write stdin for artifact {}: {source}",
                artifact.artifact_id
            ));
        }
    }
    let timeout = Duration::from_millis(
        artifact
            .timeout_ms
            .unwrap_or(DEFAULT_LOCAL_COMMAND_TIMEOUT_MS),
    );
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|source| {
            format!(
                "failed waiting for local command artifact {}: {source}",
                artifact.artifact_id
            )
        })? {
            let stdout = read_output_capture(&stdout_path, artifact.max_output_bytes);
            let stderr = read_output_capture(&stderr_path, artifact.max_output_bytes);
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            if status.success() {
                return Ok(String::from_utf8_lossy(&stdout).to_string());
            }
            return Err(format!(
                "local command artifact {} exited with status {}: stdout={} stderr={}",
                artifact.artifact_id,
                status,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = read_output_capture(&stdout_path, artifact.max_output_bytes);
            let stderr = read_output_capture(&stderr_path, artifact.max_output_bytes);
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "local command artifact {} timed out after {}ms: stdout={} stderr={}",
                artifact.artifact_id,
                timeout.as_millis(),
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn local_command_output_path(artifact_id: &ArtifactId, stream: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sinomo-module-{}-{}-{}-{}.log",
        artifact_id,
        stream,
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ))
}

fn read_output_capture(path: &Path, max_output_bytes: Option<usize>) -> Vec<u8> {
    let limit = max_output_bytes.unwrap_or(DEFAULT_LOCAL_COMMAND_OUTPUT_LIMIT_BYTES);
    if limit == 0 {
        return Vec::new();
    }
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
    let start = length.saturating_sub(limit_u64);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return Vec::new();
    }
    let mut bytes = Vec::new();
    if file.take(limit_u64).read_to_end(&mut bytes).is_err() {
        return Vec::new();
    }
    bytes
}

fn cleanup_output_capture(path: &Path) {
    let _ = fs::remove_file(path);
}

fn invocation_lane_label(lane: &ModuleInvocationLane) -> &'static str {
    match lane {
        ModuleInvocationLane::Active => "active",
        ModuleInvocationLane::Candidate => "candidate",
        ModuleInvocationLane::Shadow => "shadow",
    }
}

fn shadow_result_matches_primary(outcome: &ModuleInvocationOutcome) -> Option<bool> {
    let shadow = outcome.shadow.as_ref()?;
    Some(
        outcome.primary.output == shadow.output
            && outcome.primary.error == shadow.error
            && shadow.lane == ModuleInvocationLane::Shadow,
    )
}

fn module_shadow_decision_event(case: &ModuleReplayCaseOutcome) -> Option<EventEnvelope> {
    let outcome = case.outcome.as_ref()?;
    module_shadow_decision_event_from_outcome(
        Some(&case.case_id),
        &case.route_key,
        case.source_event_id.as_ref(),
        "replay",
        outcome,
    )
}

pub fn module_invocation_decision_event_from_outcome(
    case_id: Option<&str>,
    route_key: &str,
    source_event_id: Option<&EventId>,
    telemetry_source: &str,
    outcome: &ModuleInvocationOutcome,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        outcome.route.node_id.clone(),
        Source::new(if telemetry_source == "live" {
            "module.live"
        } else {
            "module.replay"
        })
        .expect("valid source"),
        EventKind::new(MODULE_INVOCATION_DECISION_EVENT_KIND).expect("valid event kind"),
        Salience::new(if outcome.primary.error.is_some() {
            0.86
        } else if matches!(&outcome.route.primary_lane, ModuleRouteLane::Candidate) {
            0.7
        } else {
            0.58
        })
        .expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_target_node(outcome.route.node_id.clone())
    .with_payload(serde_json::json!({
    "case_id": case_id,
    "route_key": route_key,
    "source_event_id": source_event_id.map(|id| id.as_str()),
    "telemetry_source": telemetry_source,
    "module_id": outcome.route.module_id.as_str(),
    "primary_artifact_id": outcome.primary.artifact_id.as_str(),
    "primary_lane": outcome.primary.lane.clone(),
    "shadow_artifact_id": outcome.shadow.as_ref().map(|shadow| shadow.artifact_id.as_str()),
        "shadow_matched": shadow_result_matches_primary(outcome),
        "traffic_weight_bps": outcome.route.traffic_weight_bps,
        "route_bucket_bps": outcome.route.route_bucket_bps,
        "hot_swap_state": outcome.route.hot_swap_state.clone(),
        "primary_output_bytes": outcome.primary.output.as_ref().map(|output| output.len()),
        "primary_latency_ms": outcome.primary.latency_ms,
        "shadow_output_bytes": outcome.shadow.as_ref().and_then(|shadow| {
            shadow.output.as_ref().map(|output| output.len())
        }),
        "shadow_latency_ms": outcome.shadow.as_ref().and_then(|shadow| shadow.latency_ms),
        "primary_error": outcome.primary.error.as_deref(),
        "primary_error_kind": module_artifact_error_kind(outcome.primary.error.as_deref()),
        "shadow_error": outcome.shadow.as_ref().and_then(|shadow| shadow.error.as_deref()),
        "shadow_error_kind": module_artifact_error_kind(
            outcome.shadow.as_ref().and_then(|shadow| shadow.error.as_deref()),
        ),
    }))
}

pub fn module_shadow_decision_event_from_outcome(
    case_id: Option<&str>,
    route_key: &str,
    source_event_id: Option<&EventId>,
    telemetry_source: &str,
    outcome: &ModuleInvocationOutcome,
) -> Option<EventEnvelope> {
    let shadow = outcome.shadow.as_ref()?;
    Some(
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            outcome.route.node_id.clone(),
            Source::new(if telemetry_source == "live" {
                "module.live"
            } else {
                "module.replay"
            })
            .expect("valid source"),
            EventKind::new(MODULE_SHADOW_DECISION_EVENT_KIND).expect("valid event kind"),
            Salience::new(if shadow_result_matches_primary(outcome) == Some(false) {
                0.84
            } else {
                0.62
            })
            .expect("valid salience"),
            Confidence::new(0.95).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(outcome.route.node_id.clone())
        .with_payload(serde_json::json!({
            "case_id": case_id,
            "route_key": route_key,
            "source_event_id": source_event_id.map(|id| id.as_str()),
            "telemetry_source": telemetry_source,
            "module_id": outcome.route.module_id.as_str(),
            "primary_artifact_id": outcome.primary.artifact_id.as_str(),
            "primary_lane": outcome.primary.lane.clone(),
            "shadow_artifact_id": shadow.artifact_id.as_str(),
            "shadow_matched": shadow_result_matches_primary(outcome),
            "primary_output_bytes": outcome.primary.output.as_ref().map(|output| output.len()),
            "shadow_output_bytes": shadow.output.as_ref().map(|output| output.len()),
            "primary_latency_ms": outcome.primary.latency_ms,
            "shadow_latency_ms": shadow.latency_ms,
            "primary_error": outcome.primary.error.as_deref(),
            "primary_error_kind": module_artifact_error_kind(outcome.primary.error.as_deref()),
            "shadow_error": shadow.error.as_deref(),
            "shadow_error_kind": module_artifact_error_kind(shadow.error.as_deref()),
        })),
    )
}

fn module_artifact_error_kind(error: Option<&str>) -> Option<&'static str> {
    let error = error?.trim();
    if error.is_empty() {
        return None;
    }
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("timed out") || normalized.contains("timeout") {
        return Some("timeout");
    }
    if normalized.contains("policy") && normalized.contains("blocked") {
        return Some("policy-blocked");
    }
    Some("runtime-error")
}

fn percentile_nearest_rank(mut values: Vec<u64>, percentile: u64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let percentile = percentile.clamp(1, 100);
    let rank = (values.len() as u64 * percentile).div_ceil(100);
    let index = rank.saturating_sub(1).min(values.len() as u64 - 1) as usize;
    Some(values[index])
}

fn stable_route_bucket_bps(module_id: &ModuleId, route_key: &str) -> u16 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in module_id
        .as_str()
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0))
        .chain(route_key.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % 10_000) as u16
}

pub fn sign_artifact_manifest_with_node_identity(
    artifact: &ArtifactManifest,
    identity: &NodeIdentity,
) -> Result<String, ArtifactSignatureError> {
    let key_pair = identity
        .key_pair()
        .map_err(|source| ArtifactSignatureError::SigningKey {
            detail: source.to_string(),
        })?;
    let payload = artifact_signature_payload(artifact)?;
    let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(payload.as_bytes()).as_ref());
    Ok(format!(
        "{LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME}:{}:{signature}",
        identity.public_key
    ))
}

pub fn verify_artifact_signature(
    artifact: &ArtifactManifest,
) -> Result<(), ArtifactSignatureError> {
    let parsed = parse_local_ed25519_artifact_signature(artifact)?;
    let public_key = URL_SAFE_NO_PAD
        .decode(parsed.public_key.as_bytes())
        .map_err(|_| ArtifactSignatureError::MalformedPublicKey)?;
    let signature = URL_SAFE_NO_PAD
        .decode(parsed.signature.as_bytes())
        .map_err(|_| ArtifactSignatureError::MalformedSignature)?;
    let payload = artifact_signature_payload(artifact)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| ArtifactSignatureError::Invalid)
}

pub fn artifact_signature_signer_public_key(
    artifact: &ArtifactManifest,
) -> Result<String, ArtifactSignatureError> {
    parse_local_ed25519_artifact_signature(artifact).map(|parsed| parsed.public_key)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedArtifactSignature {
    public_key: String,
    signature: String,
}

fn parse_local_ed25519_artifact_signature(
    artifact: &ArtifactManifest,
) -> Result<ParsedArtifactSignature, ArtifactSignatureError> {
    let signature = artifact
        .signature
        .as_deref()
        .ok_or(ArtifactSignatureError::Missing)?;
    let mut parts = signature.split(':');
    let scheme = parts.next().unwrap_or_default();
    if scheme != LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME {
        return Err(ArtifactSignatureError::UnsupportedScheme {
            scheme: scheme.to_string(),
        });
    }
    let public_key = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactSignatureError::Malformed)?;
    let signature = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactSignatureError::Malformed)?;
    if parts.next().is_some() {
        return Err(ArtifactSignatureError::Malformed);
    }
    Ok(ParsedArtifactSignature {
        public_key: public_key.to_string(),
        signature: signature.to_string(),
    })
}

pub fn artifact_signature_payload(
    artifact: &ArtifactManifest,
) -> Result<String, ArtifactSignatureError> {
    let mut unsigned = artifact.clone();
    unsigned.signature = None;
    let target_ring = serde_json::to_string(&unsigned.target_ring).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    let module_manifest = serde_json::to_string(&unsigned.module_manifest).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    let provenance = serde_json::to_string(&unsigned.provenance).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    Ok(format!(
        "{ARTIFACT_SIGNATURE_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}",
        unsigned.artifact_id,
        unsigned.module_id,
        target_ring,
        unsigned.content_hash,
        module_manifest,
        provenance
    ))
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ArtifactSignatureError {
    #[error("artifact is missing a signature")]
    Missing,
    #[error("unsupported artifact signature scheme `{scheme}`")]
    UnsupportedScheme { scheme: String },
    #[error("artifact signature is malformed")]
    Malformed,
    #[error("artifact signature public key is malformed")]
    MalformedPublicKey,
    #[error("artifact signature bytes are malformed")]
    MalformedSignature,
    #[error("artifact signature is invalid")]
    Invalid,
    #[error("artifact signing key could not be loaded: {detail}")]
    SigningKey { detail: String },
    #[error("artifact signature payload could not be serialized: {detail}")]
    Serialize { detail: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SinomoModuleV1ResultError {
    #[error("sinomo-module-v1 result is invalid JSON: {detail}")]
    InvalidJson { detail: String },
    #[error("sinomo-module-v1 result omitted non-empty `status`")]
    MissingStatus,
    #[error("sinomo-module-v1 result events_json[{index}] is invalid JSON: {detail}")]
    InvalidEventJson { index: usize, detail: String },
    #[error("sinomo-module-v1 result metrics_json is invalid JSON: {detail}")]
    InvalidMetricsJson { detail: String },
    #[error("sinomo-module-v1 result could not be serialized: {detail}")]
    Serialize { detail: String },
}

#[derive(Debug, Error)]
pub enum ModuleLoaderStoreError {
    #[error("module loader state I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("module loader state object error at {path}: {detail}")]
    Object { path: String, detail: String },
}

#[derive(Debug, Error)]
pub enum NativeDylibModuleInstallError {
    #[error("native dylib module install I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error(transparent)]
    Host(#[from] NativeModuleHostError),
    #[error(transparent)]
    Store(#[from] ModuleLoaderStoreError),
    #[error("native dylib module descriptor mismatch: {detail}")]
    Descriptor { detail: String },
}

#[derive(Debug, Error)]
pub enum DeclarativeModuleInstallError {
    #[error("declarative module install I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("declarative module archive error at {path}: {detail}")]
    Archive { path: String, detail: String },
    #[error("declarative module manifest JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error(transparent)]
    Store(#[from] ModuleLoaderStoreError),
    #[error("declarative module descriptor mismatch: {detail}")]
    Descriptor { detail: String },
    #[error("unsupported declarative module package: {detail}")]
    UnsupportedPackage { detail: String },
}

#[derive(Debug, Error)]
pub enum ModuleInvocationTelemetryError {
    #[error(transparent)]
    ModuleStore(#[from] ModuleLoaderStoreError),
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

fn read_object<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ModuleLoaderStoreError> {
    let bytes = fs::read(path).map_err(|source| ModuleLoaderStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_ext::from_bytes(&bytes).map_err(|source| ModuleLoaderStoreError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), ModuleLoaderStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ModuleLoaderStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes =
        serde_ext::to_bytes(value, MODULE_OBJECT_VERSION, LengthMode::U32).map_err(|source| {
            ModuleLoaderStoreError::Object {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    fs::write(path, bytes).map_err(|source| ModuleLoaderStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::{STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state",
        "node-local module loader runtime state keyed by module id",
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "ModuleLoaderRuntimeState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("node_id", "NodeId")
        .field("activation_id", "ModuleActivationId")
        .field("module_id", "ModuleId")
        .field("loader_state", "ModuleLoaderState")
        .field("traffic_weight_bps", "u16")
        .field("last_loader_action", "String")
        .field("last_loader_stage", "String")
        .field("last_transition_record_id", "Id")
        .field("reason", "String")
        .field("applied_at", "String"),
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/native-dylib",
        "node-local native-dylib module activation records keyed by module id",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/native-rust",
        "node-local native Rust module enablement records keyed by module id",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/declarative",
        "node-local declarative module activation records keyed by module id",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/cache/native-dylib",
        "content-addressed native dynamic-library module cache",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/cache/declarative",
        "content-addressed declarative module payload cache",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/catalog/declarative",
        "node-local declarative module catalog registrations keyed by module id",
    )?;
    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/lifecycle/unload-records",
        "node-local best-effort module unload records keyed by module id",
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/native-dylib/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "NativeDylibModuleLoaderState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("module_id", "ModuleId")
        .field("artifact_id", "Option<ArtifactId>")
        .field("content_hash", "String")
        .field("cache_key", "String")
        .field("library_name", "String")
        .field("platform_triple", "String")
        .field("native_abi_version", "u32")
        .field("interface_version", "String")
        .field("activated_at", "String")
        .field("source", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/native-rust/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "NativeRustModuleRuntimeState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("module_id", "ModuleId")
        .field("enabled", "bool")
        .field("updated_at", "String")
        .field("source", "String")
        .field("reason", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/declarative/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "DeclarativeModuleLoaderState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("module_id", "ModuleId")
        .field("version", "String")
        .field("content_hash", "String")
        .field("payload_ref", "String")
        .field("active", "bool")
        .field("installed_at", "String")
        .field("source", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/catalog/declarative/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "ModuleRegistration",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("manifest", "ModuleManifest")
        .field("backend", "ModuleBackendRegistration")
        .field("surfaces", "ModuleSurfaces"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/lifecycle/unload-records/example.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "ModuleUnloadRecord",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("module_id", "ModuleId")
        .field("backend", "ModuleKind")
        .field("artifact_id", "Option<ArtifactId>")
        .field("content_hash", "Option<String>")
        .field("cache_key", "Option<String>")
        .field("library_name", "Option<String>")
        .field("state_removed", "bool")
        .field("runtime_unload_attempted", "bool")
        .field("runtime_unload_succeeded", "bool")
        .field("runtime_unload_detail", "String")
        .field("unloaded_at", "String")
        .field("source", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/local-command-artifacts.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "ModuleLocalCommandRegistry",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("artifacts", "Vec<ModuleLocalCommandArtifact>"),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{Id, ModuleManifest};
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[test]
    fn module_runtime_config_resolves_node_then_cluster_then_disabled() {
        let module_id = "module.remote-management";
        let mut cluster = ModuleRuntimeConfig::default();
        cluster
            .set(module_id, ModuleRuntimeConfigEntry::enabled(true))
            .expect("cluster default");
        let node = ModuleRuntimeConfig::default();

        let inherited = resolve_module_runtime_config(&node, &cluster, module_id);
        assert!(inherited.enabled);
        assert_eq!(inherited.source, ModuleRuntimeConfigSource::Cluster);

        let mut node = ModuleRuntimeConfig::default();
        node.set(module_id, ModuleRuntimeConfigEntry::enabled(false))
            .expect("node override");
        let overridden = resolve_module_runtime_config(&node, &cluster, module_id);
        assert!(!overridden.enabled);
        assert_eq!(overridden.source, ModuleRuntimeConfigSource::Node);

        let missing = resolve_module_runtime_config(&node, &cluster, "module.unconfigured");
        assert!(!missing.enabled);
        assert_eq!(missing.source, ModuleRuntimeConfigSource::DisabledDefault);
    }

    #[test]
    fn module_registry_preserves_registration_order_and_rejects_duplicates() {
        let first_module_id =
            ModuleId::from_legacy_named("module.agent.prompt_pack").expect("module");
        let second_module_id = ModuleId::from_legacy_named("module.sandbox.wasix").expect("module");
        let first = ModuleRegistration {
            manifest: ModuleManifest::r3_wasm(first_module_id.clone(), "0.1.0", "agent"),
            backend: ModuleBackendRegistration::Declarative {
                payload_ref: "config://modules/module/agent/prompt_pack/payload".to_string(),
            },
            surfaces: ModuleSurfaces::default(),
        };
        let second = ModuleRegistration {
            manifest: ModuleManifest::native_dylib(
                second_module_id.clone(),
                "0.1.0",
                "sandbox",
                "sinomo.sandbox.wasix.v1",
                TrustRing::R1TrustedRuntime,
            ),
            backend: ModuleBackendRegistration::NativeDylib {
                artifact_id: ArtifactId::from_legacy_named("artifact_wasix_dylib")
                    .expect("artifact"),
                content_hash: "blake3:abc".to_string(),
                library_name: "libsinomo_sandbox_wasix_module.so".to_string(),
                platform_triple: "x86_64-unknown-linux-gnu".to_string(),
                interface_version: "sinomo.sandbox.wasix.v1".to_string(),
            },
            surfaces: ModuleSurfaces::default(),
        };

        let duplicate = ModuleRegistry::builder()
            .register(first.clone())
            .expect("first")
            .register(first.clone())
            .expect_err("duplicate module id");
        assert!(matches!(
            duplicate,
            ModuleRegistryError::DuplicateModuleId { module_id } if module_id == first_module_id
        ));

        let registry = ModuleRegistry::builder()
            .register(first)
            .expect("first")
            .register(second)
            .expect("second")
            .build();

        let ordered_ids = registry
            .registrations()
            .map(|registration| registration.module_id().clone())
            .collect::<Vec<_>>();
        assert_eq!(ordered_ids, vec![first_module_id, second_module_id]);
        assert!(registry.contains(&ordered_ids[0]));
        assert_eq!(registry.len(), 2);
    }

    fn native_rust_registration_with_object_route(
        module_id_name: &str,
        route: &str,
        operation: &str,
    ) -> ModuleRegistration {
        let module_id = ModuleId::from_legacy_named(module_id_name).expect("module");
        ModuleRegistration::native_rust(
            module_id,
            "0.1.0",
            module_id_name,
            module_id_name,
            "sinomo.test.module.v1",
            TrustRing::R1TrustedRuntime,
            module_id_name,
        )
        .with_surfaces(
            ModuleSurfaces::default()
                .with_object_rpc_route(ModuleObjectRpcRoute::new(
                    route,
                    "test-module",
                    operation,
                    ModuleRouteAuth::LocalOrClusterMember,
                ))
                .with_capability_export(CapabilityName::new(operation).expect("capability")),
        )
    }

    #[test]
    fn module_registry_snapshot_store_swaps_validated_immutable_snapshots() {
        let store = ModuleRegistrySnapshotStore::default();
        let initial = store.active_snapshot().expect("initial snapshot");
        let registration = native_rust_registration_with_object_route(
            "module.snapshot.first",
            "/module/snapshot/first",
            "module.snapshot.first.read",
        );
        let module_id = registration.module_id().clone();

        let updated = store.register(registration).expect("register module");

        assert_eq!(updated.version(), initial.version() + 1);
        assert!(
            initial.registry().is_empty(),
            "existing readers must keep the old immutable registry snapshot"
        );
        assert!(updated.registry().contains(&module_id));
        assert_eq!(
            store.active_snapshot().expect("active snapshot").version(),
            updated.version()
        );
    }

    #[test]
    fn module_registry_snapshot_store_rejects_duplicate_object_rpc_routes_before_swap() {
        let store = ModuleRegistrySnapshotStore::default();
        let first = native_rust_registration_with_object_route(
            "module.snapshot.duplicate.first",
            "/module/snapshot/duplicate",
            "module.snapshot.duplicate.first.read",
        );
        let second = native_rust_registration_with_object_route(
            "module.snapshot.duplicate.second",
            "/module/snapshot/duplicate",
            "module.snapshot.duplicate.second.read",
        );
        let first_snapshot = store.register(first).expect("first registration");

        let error = store
            .register(second)
            .expect_err("duplicate route should be rejected before swap");

        assert!(matches!(
            error,
            ModuleRegistryError::DuplicateObjectRpcRoute { ref route, .. }
                if route == "/module/snapshot/duplicate"
        ));
        let active = store.active_snapshot().expect("active snapshot");
        assert_eq!(
            active.version(),
            first_snapshot.version(),
            "failed registration must leave the previous snapshot active"
        );
        assert_eq!(active.registry().len(), 1);
    }

    #[test]
    fn native_rust_registration_constructor_pairs_manifest_and_backend() {
        let module_id = ModuleId::from_legacy_named("module.dfs").expect("module");
        let registration = ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "sinomo-dfs",
            "sinomo-dfs",
            "sinomo.dfs.v1",
            TrustRing::R1TrustedRuntime,
            "sinomo-dfs",
        );

        assert_eq!(registration.module_id(), &module_id);
        assert_eq!(registration.manifest.kind, ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
        assert_eq!(
            registration.backend,
            ModuleBackendRegistration::NativeRust {
                registration: NativeRustBackendRegistration::new("sinomo-dfs", "sinomo-dfs")
            }
        );
        assert!(registration.surfaces.is_empty());
    }

    #[test]
    fn module_registration_surfaces_roundtrip_as_catalog_data() {
        let module_id = ModuleId::from_legacy_named("module.dfs").expect("module");
        let registration = ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "sinomo-dfs",
            "sinomo-dfs",
            "sinomo.dfs.v1",
            TrustRing::R1TrustedRuntime,
            "sinomo-dfs",
        )
        .with_surfaces(
            ModuleSurfaces::default()
                .with_object_rpc_route(ModuleObjectRpcRoute::new(
                    "/dfs/status",
                    "dfs",
                    "dfs.status.read",
                    ModuleRouteAuth::LocalOrClusterMember,
                ))
                .with_event_kind_name("dfs.metadata.updated")
                .with_storage_owner("sinomo-dfs")
                .with_skill_resource("skill.dfs.operator", "skills/dfs/SKILL.md")
                .with_prompt_resource("prompt.dfs.repair", "prompts/repair.md")
                .with_capability_export(CapabilityName::new("dfs.status.read").expect("capability"))
                .with_capability_export(
                    CapabilityName::new("dfs.metadata.replicate").expect("capability"),
                ),
        );

        let encoded = serde_json::to_string(&registration).expect("encode");
        let decoded: ModuleRegistration = serde_json::from_str(&encoded).expect("decode");

        assert_eq!(decoded.module_id(), &module_id);
        assert_eq!(decoded.surfaces.object_rpc_routes.len(), 1);
        assert_eq!(
            decoded.surfaces.object_rpc_routes[0].auth,
            ModuleRouteAuth::LocalOrClusterMember
        );
        assert_eq!(
            decoded.surfaces.object_rpc_routes[0].auth_scope(),
            "local-or-cluster-member"
        );
        assert_eq!(
            decoded.surfaces.object_rpc_routes[0]
                .operation_capability()
                .expect("operation capability")
                .as_str(),
            "dfs.status.read"
        );
        assert_eq!(
            decoded.surfaces.event_kinds[0].as_str(),
            "dfs.metadata.updated"
        );
        assert_eq!(decoded.surfaces.storage_owners[0], "sinomo-dfs");
        assert_eq!(decoded.surfaces.declarative_resources.len(), 2);
        assert_eq!(
            decoded.surfaces.declarative_resources[0].kind,
            ModuleDeclarativeResourceKind::Skill
        );
        assert_eq!(
            decoded.surfaces.declarative_resources[1].kind,
            ModuleDeclarativeResourceKind::Prompt
        );
        assert!(
            decoded
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "dfs.metadata.replicate")
        );
        let descriptors = decoded
            .surfaces
            .object_rpc_route_descriptors()
            .expect("route descriptors");
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].route, "/dfs/status");
        assert_eq!(descriptors[0].category, "dfs");
        assert_eq!(descriptors[0].auth_scope, "local-or-cluster-member");
        assert_eq!(descriptors[0].operation.as_str(), "dfs.status.read");
        assert_eq!(descriptors[0].required_capabilities.len(), 1);
        assert_eq!(
            descriptors[0].required_capabilities[0].as_str(),
            "dfs.status.read"
        );
        let descriptor = decoded
            .surfaces
            .object_rpc_route_descriptor("/dfs/status")
            .expect("route descriptor lookup")
            .expect("known route descriptor");
        assert_eq!(descriptor, descriptors[0]);
        assert!(
            decoded
                .surfaces
                .object_rpc_route_descriptor("/dfs/missing")
                .expect("missing route descriptor lookup")
                .is_none()
        );
    }

    #[test]
    fn native_dylib_descriptor_surfaces_convert_to_registration() {
        let module_id = ModuleId::from_legacy_named("module.dynamic-native").expect("module");
        let state = NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: "blake3:abc".to_string(),
            cache_key: "module/dynamic-native".to_string(),
            library_name: "libdynamic_native_module.so".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            native_abi_version: sinomo_module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            interface_version: "sinomo.dynamic-native.v1".to_string(),
            activated_at: "2026-06-11T00:00:00Z".to_string(),
            source: "test".to_string(),
        };
        let descriptor = NativeModuleDescriptor {
            module_id: "module.dynamic-native".to_string(),
            module_version: "0.1.0".to_string(),
            owner: "dynamic-native".to_string(),
            native_abi_version: sinomo_module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            backend: sinomo_module_api::NativeModuleBackend::NativeDylib,
            interface_version: "sinomo.dynamic-native.v1".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            supported_operations: vec!["dynamic.native.inspect".to_string()],
            capabilities: vec!["dynamic.native.inspect".to_string()],
            surfaces: sinomo_module_api::NativeModuleSurfaces {
                object_rpc_routes: vec![NativeModuleObjectRpcRoute {
                    route: "/dynamic-native/inspect".to_string(),
                    category: "dynamic.native".to_string(),
                    operation: "dynamic.native.inspect".to_string(),
                    auth: "admin".to_string(),
                }],
                declarative_resources: vec![NativeModuleDeclarativeResource {
                    id: "skill.dynamic-native".to_string(),
                    kind: "skill".to_string(),
                    path: "skills/dynamic-native/SKILL.md".to_string(),
                    description: "Dynamic native module skill".to_string(),
                    target_agents: vec!["agent.coding".to_string()],
                    model_families: vec!["gpt".to_string()],
                    activation_tags: vec!["dynamic-native".to_string()],
                    required_capabilities: vec!["dynamic.native.inspect".to_string()],
                    embedded_text: Some(
                        "# Dynamic Native\n\nInspect the dynamic module.".to_string(),
                    ),
                }],
                storage_owners: vec!["dynamic-native".to_string()],
                capability_imports: vec!["filesystem.read".to_string()],
                capability_exports: vec!["dynamic.native.inspect".to_string()],
                network_transports: vec!["iroh".to_string()],
            },
        };

        let registration =
            native_dylib_module_registration_from_state_and_descriptor(&state, &descriptor)
                .expect("native dylib descriptor registration");

        assert_eq!(registration.module_id(), &module_id);
        assert_eq!(registration.manifest.owner, "dynamic-native");
        assert_eq!(registration.manifest.kind, ModuleKind::NativeDylib);
        assert_eq!(
            registration.surfaces.network_transports,
            vec!["iroh".to_string()],
            "descriptor network transports map onto module surfaces"
        );
        assert_eq!(registration.surfaces.object_rpc_routes.len(), 1);
        assert_eq!(
            registration.surfaces.object_rpc_routes[0].auth,
            ModuleRouteAuth::Admin
        );
        assert_eq!(registration.surfaces.declarative_resources.len(), 1);
        assert_eq!(
            registration.surfaces.declarative_resources[0]
                .embedded_text
                .as_deref(),
            Some("# Dynamic Native\n\nInspect the dynamic module.")
        );
        assert_eq!(registration.surfaces.storage_owners, vec!["dynamic-native"]);
        assert!(
            registration
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "dynamic.native.inspect")
        );
        assert!(
            registration
                .surfaces
                .capabilities
                .imports
                .iter()
                .any(|capability| capability.as_str() == "filesystem.read")
        );
    }

    #[test]
    fn native_dylib_descriptor_legacy_capabilities_do_not_become_catalog_exports() {
        let module_id = ModuleId::from_legacy_named("module.legacy-native").expect("module");
        let state = NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: None,
            content_hash: "blake3:abc".to_string(),
            cache_key: "module/legacy-native".to_string(),
            library_name: "liblegacy_native_module.so".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            native_abi_version: sinomo_module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            interface_version: "sinomo.legacy-native.v1".to_string(),
            activated_at: "2026-06-12T00:00:00Z".to_string(),
            source: "test".to_string(),
        };
        let descriptor = NativeModuleDescriptor {
            module_id: "module.legacy-native".to_string(),
            module_version: "0.1.0".to_string(),
            owner: "legacy-native".to_string(),
            native_abi_version: sinomo_module_api::SINOMO_NATIVE_MODULE_ABI_VERSION,
            backend: sinomo_module_api::NativeModuleBackend::NativeDylib,
            interface_version: "sinomo.legacy-native.v1".to_string(),
            platform_triple: "x86_64-linux-unix".to_string(),
            supported_operations: vec!["legacy.probe".to_string()],
            capabilities: vec!["wasix".to_string()],
            surfaces: Default::default(),
        };

        let registration =
            native_dylib_module_registration_from_state_and_descriptor(&state, &descriptor)
                .expect("legacy descriptor registration");

        assert_eq!(registration.module_id(), &module_id);
        assert!(registration.surfaces.capabilities.exports.is_empty());
    }

    #[test]
    fn declarative_module_package_install_imports_payload_catalog_and_state() {
        let data_root = unique_temp("declarative-module-data");
        let package_root = unique_temp("declarative-module-package");
        std::fs::create_dir_all(package_root.join("skills/rust")).expect("skills dir");
        std::fs::create_dir_all(package_root.join("prompts")).expect("prompts dir");
        std::fs::write(
            package_root.join("skills/rust/SKILL.md"),
            "# Rust Debugging\n\nUse cargo diagnostics before editing.",
        )
        .expect("skill");
        std::fs::write(
            package_root.join("prompts/triage.md"),
            "Classify the failure mode before selecting tools.",
        )
        .expect("prompt");
        let package_manifest = DeclarativeModulePackageManifest {
            module_id: "module.skills.rust_debugging".to_string(),
            version: "0.1.0".to_string(),
            owner: "agent".to_string(),
            api_version: DECLARATIVE_MODULE_API_VERSION.to_string(),
            target_ring: TrustRing::R3SandboxedModule,
            capabilities: ModuleCapabilities::default(),
            surfaces: ModuleSurfaces::default(),
            resources: vec![
                ModuleDeclarativeResourceExport::skill(
                    "skill.rust-debugging",
                    "skills/rust/SKILL.md",
                )
                .with_description("Rust compiler and test failure workflow")
                .with_model_family("gpt")
                .with_activation_tag("rust"),
                ModuleDeclarativeResourceExport::prompt("prompt.rust-triage", "prompts/triage.md")
                    .with_target_agent("agent.coding"),
            ],
        };
        std::fs::write(
            package_root.join("sinomo.module.json"),
            serde_json::to_vec_pretty(&package_manifest).expect("manifest json"),
        )
        .expect("manifest");

        let result = install_declarative_module_package(
            &data_root,
            &package_root,
            DeclarativeModuleInstallRequest::new("test-package"),
        )
        .expect("install package");

        assert_eq!(result.registration.manifest.kind, ModuleKind::Declarative);
        assert_eq!(result.registration.manifest.version, "0.1.0");
        assert_eq!(result.registration.surfaces.declarative_resources.len(), 2);
        assert!(result.state.active);
        assert!(result.state.content_hash.starts_with("blake3:"));
        assert!(result.payload_path.join("skills/rust/SKILL.md").exists());
        assert!(result.payload_path.join("prompts/triage.md").exists());
        assert!(result.catalog_path.exists());
        assert!(result.state_path.as_ref().expect("state path").exists());

        let read_registration =
            read_declarative_module_registration(&data_root, result.registration.module_id())
                .expect("read registration")
                .expect("registration");
        assert_eq!(read_registration, result.registration);
        let read_state =
            read_declarative_module_loader_state(&data_root, result.registration.module_id())
                .expect("read state")
                .expect("state");
        assert_eq!(read_state, result.state);
    }

    #[test]
    fn declarative_module_package_install_accepts_zip_transport() {
        use std::io::Write as _;

        let data_root = unique_temp("declarative-module-zip-data");
        let zip_root = unique_temp("declarative-module-package");
        let zip_path = zip_root.join("package.zip");
        let package_manifest = DeclarativeModulePackageManifest {
            module_id: "module.prompts.system_management".to_string(),
            version: "0.2.0".to_string(),
            owner: "agent".to_string(),
            api_version: DECLARATIVE_MODULE_API_VERSION.to_string(),
            target_ring: TrustRing::R3SandboxedModule,
            capabilities: ModuleCapabilities::default(),
            surfaces: ModuleSurfaces::default(),
            resources: vec![ModuleDeclarativeResourceExport::prompt(
                "prompt.system-management.repair",
                "prompts/repair.md",
            )],
        };
        let file = std::fs::File::create(&zip_path).expect("zip file");
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        archive
            .start_file("package/sinomo.module.json", options)
            .expect("manifest entry");
        archive
            .write_all(&serde_json::to_vec_pretty(&package_manifest).expect("manifest json"))
            .expect("manifest bytes");
        archive
            .start_file("package/prompts/repair.md", options)
            .expect("prompt entry");
        archive
            .write_all(b"Use system service tools only after checking health.")
            .expect("prompt bytes");
        archive.finish().expect("finish zip");

        let result = install_declarative_module_package(
            &data_root,
            &zip_path,
            DeclarativeModuleInstallRequest::new("test-zip"),
        )
        .expect("install zip package");

        assert_eq!(result.registration.manifest.kind, ModuleKind::Declarative);
        assert_eq!(result.registration.surfaces.declarative_resources.len(), 1);
        assert!(result.payload_path.join("prompts/repair.md").exists());
        assert!(result.state_path.as_ref().expect("state path").exists());
    }

    #[test]
    fn module_surface_validation_rejects_invalid_object_rpc_routes() {
        let invalid_path = ModuleSurfaces::default()
            .with_object_rpc_route(ModuleObjectRpcRoute::new(
                "dfs/status",
                "dfs",
                "dfs.status.read",
                ModuleRouteAuth::LocalOrClusterMember,
            ))
            .with_capability_export(CapabilityName::new("dfs.status.read").expect("capability"))
            .validate()
            .expect_err("route without path prefix");
        assert!(
            matches!(invalid_path, ModuleSurfaceError::InvalidObjectRpcRoute { ref detail, .. }
                if detail.contains("must start with /")),
            "unexpected error: {invalid_path}"
        );

        let missing_export = ModuleSurfaces::default()
            .with_object_rpc_route(ModuleObjectRpcRoute::new(
                "/dfs/status",
                "dfs",
                "dfs.status.read",
                ModuleRouteAuth::LocalOrClusterMember,
            ))
            .validate()
            .expect_err("route operation must be exported");
        assert!(matches!(
            missing_export,
            ModuleSurfaceError::ObjectRpcRouteOperationNotExported {
                route,
                operation,
            } if route == "/dfs/status" && operation == "dfs.status.read"
        ));

        let duplicate = ModuleSurfaces::default()
            .with_object_rpc_route(ModuleObjectRpcRoute::new(
                "/dfs/status",
                "dfs",
                "dfs.status.read",
                ModuleRouteAuth::LocalOrClusterMember,
            ))
            .with_object_rpc_route(ModuleObjectRpcRoute::new(
                "/dfs/status",
                "dfs",
                "dfs.inspection.read",
                ModuleRouteAuth::LocalOrClusterMember,
            ))
            .with_capability_export(CapabilityName::new("dfs.status.read").expect("capability"))
            .with_capability_export(CapabilityName::new("dfs.inspection.read").expect("capability"))
            .validate()
            .expect_err("duplicate route must be rejected");
        assert!(matches!(
            duplicate,
            ModuleSurfaceError::DuplicateObjectRpcRoute { route } if route == "/dfs/status"
        ));
    }

    #[test]
    fn module_registry_rejects_invalid_registration_surfaces() {
        let module_id = ModuleId::from_legacy_named("module.invalid_surface").expect("module");
        let registration = ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "invalid",
            "invalid",
            "sinomo.invalid.v1",
            TrustRing::R1TrustedRuntime,
            "invalid",
        )
        .with_surfaces(ModuleSurfaces::default().with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                "/invalid/route",
                "invalid",
                "invalid.route.invoke",
                ModuleRouteAuth::Admin,
            ),
        ));

        let error = ModuleRegistry::builder()
            .register(registration)
            .expect_err("invalid surfaces");
        assert!(
            matches!(error, ModuleRegistryError::InvalidModuleSurfaces { module_id: ref observed, ref detail }
                if observed == &module_id && detail.contains("operation `invalid.route.invoke` is not exported")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn native_dylib_loader_state_roundtrips_from_data_dir() {
        let root = unique_temp("native-dylib-loader-state");
        let module_id = ModuleId::from_legacy_named("module.sandbox.wasix").expect("module");
        let mut state = NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: Some(
                ArtifactId::from_legacy_named("artifact_wasix_dylib").expect("artifact"),
            ),
            content_hash: "blake3:abc".to_string(),
            cache_key: "module/sandbox/wasix".to_string(),
            library_name: "libsinomo_sandbox_wasix_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo.sandbox.wasix.v1".to_string(),
            activated_at: module_loader_applied_at_now(),
            source: "local".to_string(),
        };

        let path = write_native_dylib_module_loader_state(&root, &state).expect("write state");
        assert_eq!(
            path,
            native_dylib_module_loader_state_path(&root, &module_id)
        );
        let read_back = read_native_dylib_module_loader_state(&root, &module_id)
            .expect("read state")
            .expect("state");
        assert_eq!(read_back, state);

        // Rollouts update an existing module record. In particular, this
        // exercises the writable durability handle required by Windows.
        state.content_hash = "blake3:def".to_string();
        state.activated_at = module_loader_applied_at_now();
        write_native_dylib_module_loader_state(&root, &state).expect("overwrite state");
        assert_eq!(
            read_native_dylib_module_loader_state(&root, &module_id)
                .expect("read overwritten state")
                .expect("overwritten state"),
            state
        );
    }

    #[test]
    fn native_dylib_loader_restore_recovers_every_durable_swap_topology() {
        #[derive(Clone, Copy)]
        enum Expected {
            Live,
            Staging,
            Previous,
        }

        for (name, has_live, has_staging, has_previous, expected) in [
            ("prepared", true, true, false, Expected::Live),
            ("old-moved", false, true, true, Expected::Staging),
            ("staging-lost", false, false, true, Expected::Previous),
            ("new-live", true, false, true, Expected::Live),
        ] {
            let root = unique_temp(&format!("native-dylib-loader-recovery-{name}"));
            let live = native_dylib_module_loader_state_dir(&root);
            let staging = native_dylib_module_loader_state_staging_dir(&root);
            let previous = native_dylib_module_loader_state_previous_dir(&root);
            let marker = native_dylib_module_loader_state_transaction_path(&root);
            let live_state = test_native_dylib_loader_state("live");
            let staging_state = test_native_dylib_loader_state("staging");
            let previous_state = test_native_dylib_loader_state("previous");
            if has_live {
                write_test_native_dylib_state_dir(&live, &live_state);
            }
            if has_staging {
                write_test_native_dylib_state_dir(&staging, &staging_state);
            }
            if has_previous {
                write_test_native_dylib_state_dir(&previous, &previous_state);
            }
            write_object(
                &marker,
                &NativeDylibModuleLoaderStateTransaction {
                    prepared_at: "2026-07-12T00:00:00Z".to_string(),
                },
            )
            .expect("transaction marker");

            assert!(
                recover_native_dylib_module_loader_state_transaction(&root)
                    .expect("recover transaction"),
                "{name} topology should recover"
            );
            let expected_state = match expected {
                Expected::Live => live_state,
                Expected::Staging => staging_state,
                Expected::Previous => previous_state,
            };
            assert_eq!(
                read_native_dylib_module_loader_states(&root).expect("read recovered states"),
                vec![expected_state],
                "unexpected authoritative state for {name} topology"
            );
            assert!(live.is_dir());
            assert!(!staging.exists());
            assert!(!previous.exists());
            assert!(!marker.exists());
        }
    }

    #[test]
    fn native_dylib_loader_restore_fails_closed_on_corrupt_and_partial_markers() {
        for marker_kind in ["corrupt", "partial"] {
            let root = unique_temp(&format!("native-dylib-loader-marker-{marker_kind}"));
            let live = native_dylib_module_loader_state_dir(&root);
            let staging = native_dylib_module_loader_state_staging_dir(&root);
            let previous = native_dylib_module_loader_state_previous_dir(&root);
            let marker = native_dylib_module_loader_state_transaction_path(&root);
            let live_state = test_native_dylib_loader_state("live");
            let staging_state = test_native_dylib_loader_state("staging");
            let previous_state = test_native_dylib_loader_state("previous");
            write_test_native_dylib_state_dir(&live, &live_state);
            write_test_native_dylib_state_dir(&staging, &staging_state);
            write_test_native_dylib_state_dir(&previous, &previous_state);
            if marker_kind == "corrupt" {
                std::fs::create_dir_all(marker.parent().expect("marker parent"))
                    .expect("marker parent");
                std::fs::write(&marker, b"not-a-sinomo-object").expect("corrupt marker");
            } else {
                write_object(
                    &marker,
                    &NativeDylibModuleLoaderStateTransaction {
                        prepared_at: "2026-07-12T00:00:00Z".to_string(),
                    },
                )
                .expect("complete marker");
                let mut bytes = std::fs::read(&marker).expect("read marker");
                bytes.truncate(bytes.len().saturating_sub(1).max(1));
                std::fs::write(&marker, bytes).expect("partial marker");
            }

            recover_native_dylib_module_loader_state_transaction(&root)
                .expect_err("invalid marker must fail closed");
            assert!(marker.is_file());
            assert_eq!(
                read_objects_from_dir::<NativeDylibModuleLoaderState>(&live)
                    .expect("raw live states"),
                vec![live_state]
            );
            assert_eq!(
                read_objects_from_dir::<NativeDylibModuleLoaderState>(&staging)
                    .expect("raw staging states"),
                vec![staging_state]
            );
            assert_eq!(
                read_objects_from_dir::<NativeDylibModuleLoaderState>(&previous)
                    .expect("raw previous states"),
                vec![previous_state]
            );
        }
    }

    #[test]
    fn native_dylib_loader_state_removal_records_unload() {
        let root = unique_temp("native-dylib-unload-state");
        let module_id = ModuleId::from_legacy_named("module.sandbox.wasix").expect("module");
        let state = NativeDylibModuleLoaderState {
            module_id: module_id.clone(),
            artifact_id: Some(
                ArtifactId::from_legacy_named("artifact_wasix_dylib").expect("artifact"),
            ),
            content_hash: "blake3:abc".to_string(),
            cache_key: "module/sandbox/wasix".to_string(),
            library_name: "libsinomo_sandbox_wasix_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo.sandbox.wasix.v1".to_string(),
            activated_at: module_loader_applied_at_now(),
            source: "local".to_string(),
        };
        write_native_dylib_module_loader_state(&root, &state).expect("write state");

        let removed = remove_native_dylib_module_loader_state(&root, &module_id)
            .expect("remove state")
            .expect("removed state");
        assert_eq!(removed, state);
        assert!(
            read_native_dylib_module_loader_state(&root, &module_id)
                .expect("read removed state")
                .is_none()
        );
        assert!(
            remove_native_dylib_module_loader_state(&root, &module_id)
                .expect("remove missing state")
                .is_none()
        );

        let record = ModuleUnloadRecord {
            module_id: module_id.clone(),
            backend: sinomo_core::ModuleKind::NativeDylib,
            artifact_id: removed.artifact_id.clone(),
            content_hash: Some(removed.content_hash.clone()),
            cache_key: Some(removed.cache_key.clone()),
            library_name: Some(removed.library_name.clone()),
            state_removed: true,
            runtime_unload_attempted: true,
            runtime_unload_succeeded: true,
            runtime_unload_detail: "native module shutdown completed during explicit unload"
                .to_string(),
            unloaded_at: module_loader_applied_at_now(),
            source: "test".to_string(),
        };
        let path = write_module_unload_record(&root, &record).expect("write unload record");
        assert_eq!(path, module_unload_record_path(&root, &module_id));
        let read_back = read_module_unload_record(&root, &module_id)
            .expect("read unload record")
            .expect("record");
        assert_eq!(read_back, record);
    }

    #[test]
    fn native_dylib_cache_prune_keeps_active_and_retained_inactive_variants() {
        let root = unique_temp("native-dylib-cache-prune");
        let module_id = ModuleId::from_legacy_named("module.sandbox.wasix").expect("module");
        let state = NativeDylibModuleLoaderState {
            module_id,
            artifact_id: None,
            content_hash: "blake3:active".to_string(),
            cache_key: "module/sandbox/wasix".to_string(),
            library_name: "libsinomo_sandbox_wasix_module.so".to_string(),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo.sandbox.wasix.v1".to_string(),
            activated_at: module_loader_applied_at_now(),
            source: "local".to_string(),
        };
        write_native_dylib_module_loader_state(&root, &state).expect("write state");

        let cache_root = native_dylib_module_cache_dir(&root)
            .join("module")
            .join("sandbox")
            .join("wasix");
        let active_dir = cache_root.join("blake3_active");
        let stale_a = cache_root.join("blake3_stale_a");
        let stale_b = cache_root.join("blake3_stale_b");
        let unrelated = native_dylib_module_cache_dir(&root)
            .join("module")
            .join("remote")
            .join("management")
            .join("blake3_stale_remote");
        for dir in [&active_dir, &stale_a, &stale_b, &unrelated] {
            std::fs::create_dir_all(dir).expect("variant dir");
            std::fs::write(
                dir.join("libsinomo_sandbox_wasix_module.so"),
                b"dummy native module bytes",
            )
            .expect("variant file");
        }

        let report =
            prune_inactive_native_dylib_module_cache(&root, 1).expect("prune inactive variants");

        assert_eq!(report.retained_active_variants, 1);
        assert_eq!(report.retained_inactive_variants, 2);
        assert_eq!(report.removed_inactive_variants.len(), 1);
        assert!(active_dir.exists());
        assert_eq!(
            usize::from(stale_a.exists()) + usize::from(stale_b.exists()),
            1
        );
        assert!(unrelated.exists());
    }

    #[test]
    fn native_rust_module_enablement_overrides_default() {
        let root = unique_temp("native-rust-enablement");
        let module_id = ModuleId::from_legacy_named("module.observe").expect("module");

        assert!(native_rust_module_enabled(&root, &module_id, true).expect("read default enabled"));
        assert!(
            !native_rust_module_enabled(&root, &module_id, false).expect("read default disabled")
        );

        let path = set_native_rust_module_enabled(
            &root,
            &module_id,
            false,
            "test",
            "operator disabled observation",
        )
        .expect("write disabled");
        assert_eq!(
            path,
            native_rust_module_loader_state_path(&root, &module_id)
        );
        assert!(!native_rust_module_enabled(&root, &module_id, true).expect("read disabled"));

        let state = read_native_rust_module_runtime_state(&root, &module_id)
            .expect("read state")
            .expect("state");
        assert_eq!(state.module_id, module_id);
        assert!(!state.enabled);
        assert_eq!(state.source, "test");
    }

    #[test]
    fn module_loader_runtime_state_roundtrips_from_data_dir() {
        let root = unique_temp("module-loader-runtime-state");
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let state = ModuleLoaderRuntimeState {
            node_id,
            activation_id: ModuleActivationId::from_legacy_named("module_activation_candidate")
                .expect("activation"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: Some(artifact_id.clone()),
                candidate_artifact_id: Some(artifact_id),
                hot_swap_state: HotSwapState::ActiveCandidate,
            },
            traffic_weight_bps: 500,
            last_loader_action: "advance-canary".to_string(),
            last_loader_stage: "canary".to_string(),
            last_transition_record_id: Id::from_legacy_named("rollout_transition")
                .expect("transition"),
            reason: "test apply".to_string(),
            applied_at: module_loader_applied_at_now(),
        };

        let path = write_module_loader_runtime_state(&root, &state).expect("write state");

        assert_eq!(path, module_loader_state_path(&root, &module_id));
        assert_eq!(
            read_module_loader_runtime_state(&root, &module_id).expect("read state"),
            Some(state)
        );
    }

    #[test]
    fn module_route_keeps_shadow_artifact_out_of_primary_lane() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );

        let route = route_module_invocation_from_state(&state, "session-alpha");

        assert_eq!(route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(route.primary_artifact_id, active_artifact_id);
        assert_eq!(route.shadow_artifact_id, Some(shadow_artifact_id));
        assert_eq!(route.traffic_weight_bps, 0);
    }

    #[test]
    fn module_route_uses_stable_bucket_for_canary_weight() {
        let root = unique_temp("module-route-canary");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let candidate_key = route_key_with_bucket(&module_id, 0..500);
        let active_key = route_key_with_bucket(&module_id, 500..10_000);

        let candidate_route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id: module_id.clone(),
                route_key: candidate_key,
            },
        )
        .expect("route")
        .expect("state");
        let active_route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id,
                route_key: active_key,
            },
        )
        .expect("route")
        .expect("state");

        assert_eq!(candidate_route.primary_lane, ModuleRouteLane::Candidate);
        assert_eq!(candidate_route.primary_artifact_id, candidate_artifact_id);
        assert_eq!(active_route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(active_route.primary_artifact_id, active_artifact_id);
        assert_eq!(candidate_route.traffic_weight_bps, 500);
    }

    #[test]
    fn module_route_treats_full_promotion_as_active_lane() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &artifact_id,
            Some(&artifact_id),
            HotSwapState::ActiveCandidate,
            10_000,
        );

        let route = route_module_invocation_from_state(&state, "session-alpha");

        assert_eq!(route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(route.primary_artifact_id, artifact_id);
        assert_eq!(route.traffic_weight_bps, 10_000);
    }

    #[test]
    fn routed_execution_runs_primary_and_shadow_without_overriding_primary() {
        let root = unique_temp("module-routed-execution-shadow");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Ok("shadow-output".to_string())),
        ]);

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Active);
        assert_eq!(outcome.primary.output.as_deref(), Some("active-output"));
        assert_eq!(
            outcome.shadow.as_ref().map(|result| &result.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.output.as_deref()),
            Some("shadow-output")
        );
        let calls = runner.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].lane, ModuleInvocationLane::Active);
        assert_eq!(calls[1].lane, ModuleInvocationLane::Shadow);
    }

    #[test]
    fn routed_execution_records_shadow_failure_without_failing_primary() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        let route = route_module_invocation_from_state(&state, "session-alpha");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Err("shadow panic".to_string())),
        ]);

        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);

        assert_eq!(outcome.primary.output.as_deref(), Some("active-output"));
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.error.as_deref()),
            Some("shadow panic")
        );
    }

    #[test]
    fn routed_execution_uses_candidate_primary_for_canary_route() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(&state, &route_key);
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);

        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);

        assert_eq!(outcome.primary.artifact_id, candidate_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Candidate);
        assert_eq!(outcome.primary.output.as_deref(), Some("candidate-output"));
        assert!(outcome.shadow.is_none());
    }

    #[test]
    fn module_replay_fixture_records_matching_shadow_decisions() {
        let root = unique_temp("module-replay-shadow-match");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("same-output".to_string())),
            (shadow_artifact_id.clone(), Ok("same-output".to_string())),
        ]);
        let fixture = ModuleReplayFixture {
            fixture_id: Id::from_legacy_named("replay_fixture_match").expect("fixture"),
            module_id,
            cases: vec![ModuleReplayCase::from_event(
                "case-1",
                "session-alpha",
                replay_event("event_match"),
            )],
        };

        let report = run_module_replay_fixture(&root, &fixture, &runner).expect("replay");

        assert_eq!(report.replay_event_count, 1);
        assert_eq!(report.missing_route_count, 0);
        assert_eq!(report.primary_error_count, 0);
        assert_eq!(report.shadow_error_count, 0);
        assert_eq!(report.shadow_mismatch_count, 0);
        assert_eq!(report.cases[0].shadow_matched, Some(true));
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].kind.as_str(), "module.shadow.decision");
        assert_eq!(report.events[0].payload["shadow_matched"], true);
    }

    #[test]
    fn live_routed_execution_appends_shadow_decision_telemetry() {
        let root = unique_temp("module-live-shadow-telemetry");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("same-output".to_string())),
            (shadow_artifact_id.clone(), Ok("same-output".to_string())),
        ]);
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();

        let outcome = execute_routed_module_invocation_with_event_store(
            &root,
            &ModuleInvocationRequest {
                module_id: module_id.clone(),
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
            &mut event_store,
        )
        .expect("execute")
        .expect("route");
        let events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleShadowTelemetryReport::from_stored_events(module_id.clone(), &events);

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(
            outcome.shadow.as_ref().map(|shadow| &shadow.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.source.as_str(), "module.live");
        assert_eq!(events[0].event.payload["telemetry_source"], "live");
        assert_eq!(events[0].event.payload["shadow_matched"], true);
        assert_eq!(report.event_count, 1);
        assert_eq!(report.matched_count, 1);
        assert_eq!(report.mismatch_count, 0);
    }

    #[test]
    fn live_routed_execution_appends_canary_invocation_telemetry_without_shadow() {
        let root = unique_temp("module-live-canary-telemetry");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        let route_key = route_key_with_bucket(&module_id, 0..500);

        let outcome = execute_routed_module_invocation_with_event_store(
            &root,
            &ModuleInvocationRequest {
                module_id: module_id.clone(),
                route_key: route_key.clone(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
            &mut event_store,
        )
        .expect("execute")
        .expect("route");
        let invocation_events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay invocation events");
        let shadow_events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay shadow events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id.clone(),
            Some(&candidate_artifact_id),
            &invocation_events,
        );

        assert_eq!(outcome.primary.artifact_id, candidate_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Candidate);
        assert!(outcome.shadow.is_none());
        assert_eq!(invocation_events.len(), 1);
        assert_eq!(shadow_events.len(), 0);
        assert_eq!(invocation_events[0].event.source.as_str(), "module.live");
        assert_eq!(invocation_events[0].event.payload["route_key"], route_key);
        assert_eq!(
            invocation_events[0].event.payload["primary_lane"],
            "candidate"
        );
        assert_eq!(
            invocation_events[0].event.payload["traffic_weight_bps"],
            500
        );
        assert!(
            invocation_events[0]
                .event
                .payload
                .get("primary_latency_ms")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        );
        assert_eq!(report.event_count, 1);
        assert_eq!(report.candidate_primary_count, 1);
        assert_eq!(report.active_primary_count, 0);
        assert_eq!(report.candidate_error_count, 0);
        assert_eq!(report.candidate_latency_sample_count, 1);
    }

    #[test]
    fn invocation_decision_telemetry_classifies_timed_out_candidate_error() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (
                candidate_artifact_id,
                Err("local command artifact timed out after 20ms".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);

        assert_eq!(event.payload["primary_error_kind"], "timeout");
        assert_eq!(event.payload["shadow_error_kind"], serde_json::Value::Null);
    }

    #[test]
    fn canary_telemetry_report_requires_candidate_events_and_fails_candidate_errors() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Err("candidate panic".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert_eq!(report.candidate_primary_count, 1);
        assert_eq!(report.candidate_error_count, 1);
    }

    #[test]
    fn canary_telemetry_report_fails_candidate_latency_gate() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let mut event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);
        event.payload["primary_latency_ms"] = serde_json::json!(250);
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert_eq!(report.candidate_latency_p95_ms, Some(250));
    }

    #[test]
    fn canary_telemetry_report_fails_candidate_user_correction() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let correction_event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            Source::new("user.correction").expect("source"),
            EventKind::new(MODULE_USER_CORRECTION_EVENT_KIND).expect("kind"),
            Salience::new(0.9).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id)
        .with_payload(serde_json::json!({
            "module_id": module_id.as_str(),
            "corrected_artifact_id": candidate_artifact_id.as_str(),
            "primary_lane": "candidate",
            "reason": "user corrected candidate output",
        }));
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(correction_event)
            .expect("append correction");
        let events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_USER_CORRECTION_EVENT_KIND,
            ))
            .expect("replay corrections");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert_eq!(report.correction_event_count, 1);
        assert_eq!(report.candidate_user_correction_count, 1);
        assert_eq!(report.candidate_primary_count, 0);
    }

    #[test]
    fn shadow_telemetry_report_fails_live_mismatches() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&shadow_artifact_id),
                HotSwapState::Shadowing,
                0,
            ),
            "session-alpha",
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Err("shadow panic".to_string())),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event = module_shadow_decision_event_from_outcome(
            None,
            "session-alpha",
            None,
            "live",
            &outcome,
        )
        .expect("shadow event");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_core::events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleShadowTelemetryReport::from_stored_events(module_id, &events);

        assert_eq!(report.event_count, 1);
        assert_eq!(report.mismatch_count, 1);
        assert_eq!(report.shadow_error_count, 1);
    }

    #[test]
    fn module_replay_fixture_fails_shadow_mismatches_without_overriding_primary() {
        let root = unique_temp("module-replay-shadow-mismatch");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Ok("shadow-output".to_string())),
        ]);
        let fixture = ModuleReplayFixture {
            fixture_id: Id::from_legacy_named("replay_fixture_mismatch").expect("fixture"),
            module_id,
            cases: vec![ModuleReplayCase {
                case_id: "case-1".to_string(),
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
                source_event: None,
            }],
        };

        let report = run_module_replay_fixture(&root, &fixture, &runner).expect("replay");

        assert_eq!(
            report.cases[0]
                .outcome
                .as_ref()
                .expect("outcome")
                .primary
                .output
                .as_deref(),
            Some("active-output")
        );
        assert_eq!(report.shadow_mismatch_count, 1);
        assert_eq!(report.cases[0].shadow_matched, Some(false));
        assert_eq!(report.events[0].payload["shadow_matched"], false);
    }

    #[test]
    fn module_local_command_registry_roundtrips_from_data_dir() {
        let root = unique_temp("module-local-command-registry");
        let artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let registry = ModuleLocalCommandRegistry {
            artifacts: vec![local_command_artifact(
                artifact_id.clone(),
                "printf '%s' registry",
            )],
        };

        let path = write_module_local_command_registry(&root, &registry).expect("write registry");
        let read_back = read_module_local_command_registry(&root)
            .expect("read registry")
            .expect("registry");

        assert_eq!(path, module_local_command_registry_path(&root));
        assert_eq!(
            read_back
                .artifact(&artifact_id)
                .map(|artifact| artifact.program.as_str()),
            Some("sh")
        );
        assert_eq!(read_back, registry);
    }

    #[test]
    fn local_command_runner_executes_routed_primary_artifact() {
        let root = unique_temp("module-local-command-primary");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            None,
            HotSwapState::Stable,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        write_module_local_command_registry(
            &root,
            &ModuleLocalCommandRegistry {
                artifacts: vec![local_command_artifact(
                    active_artifact_id.clone(),
                    "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                )],
            },
        )
        .expect("write registry");
        let runner = LocalCommandModuleArtifactRunner::from_data_dir(&root, &root)
            .expect("local command runner");

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "payload".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Active);
        assert_eq!(outcome.primary.output.as_deref(), Some("active:payload"));
        assert!(outcome.shadow.is_none());
    }

    #[test]
    fn local_command_runner_mirrors_shadow_artifact_without_overriding_primary() {
        let root = unique_temp("module-local-command-shadow");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        write_module_local_command_registry(
            &root,
            &ModuleLocalCommandRegistry {
                artifacts: vec![
                    local_command_artifact(
                        active_artifact_id.clone(),
                        "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                    ),
                    local_command_artifact(
                        shadow_artifact_id.clone(),
                        "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                    ),
                ],
            },
        )
        .expect("write registry");
        let runner = LocalCommandModuleArtifactRunner::from_data_dir(&root, &root)
            .expect("local command runner");

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "payload".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.output.as_deref(), Some("active:payload"));
        assert_eq!(
            outcome.shadow.as_ref().map(|result| &result.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.output.as_deref()),
            Some("shadow:payload")
        );
    }

    #[test]
    fn module_v1_wit_contract_exposes_run_boundary() {
        assert!(SINOMO_MODULE_V1_WIT.contains("package sinomo:module-v1;"));
        assert!(SINOMO_MODULE_V1_WIT.contains("interface module"));
        assert!(
            SINOMO_MODULE_V1_WIT.contains(
                "run: func(context: module-context, input-json: string) -> module-result"
            )
        );
        assert!(SINOMO_MODULE_V1_WIT.contains("world sinomo-module-v1"));
    }

    #[test]
    fn module_v1_result_validates_json_payload_contract() {
        let result = SinomoModuleV1Result {
            status: "ok".to_string(),
            events_json: vec![
                serde_json::json!({
                    "kind": "trigger.decision.created",
                    "action": "stay-silent"
                })
                .to_string(),
            ],
            metrics_json: serde_json::json!({"trigger_decisions": 1}).to_string(),
        };

        let output = result.to_json_output().expect("serialize result");
        let parsed = SinomoModuleV1Result::from_json_output(&output).expect("parse result");

        assert_eq!(parsed, result);
    }

    #[test]
    fn module_v1_result_rejects_invalid_nested_json() {
        let error = SinomoModuleV1Result::from_json_output(
            r#"{"status":"ok","events_json":["not-json"],"metrics_json":"{}"}"#,
        )
        .expect_err("invalid event json");

        assert!(matches!(
            error,
            SinomoModuleV1ResultError::InvalidEventJson { index: 0, .. }
        ));
    }

    #[test]
    fn module_route_returns_none_without_loader_state() {
        let root = unique_temp("module-route-missing");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");

        let route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id,
                route_key: "session-alpha".to_string(),
            },
        )
        .expect("route");

        assert_eq!(route, None);
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }

    fn test_native_dylib_loader_state(label: &str) -> NativeDylibModuleLoaderState {
        NativeDylibModuleLoaderState {
            module_id: ModuleId::from_legacy_named(format!("module.test.{label}"))
                .expect("module id"),
            artifact_id: None,
            content_hash: format!("blake3:{label}"),
            cache_key: format!("module/test/{label}"),
            library_name: format!("libsinomo_test_{label}.so"),
            platform_triple: "x86_64-unknown-linux-gnu".to_string(),
            native_abi_version: 1,
            interface_version: "sinomo.test.v1".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: format!("test:{label}"),
        }
    }

    fn write_test_native_dylib_state_dir(dir: &Path, state: &NativeDylibModuleLoaderState) {
        let path = dir.join(format!("{}.sobj", state.module_id));
        write_object(&path, state).expect("write native dylib state");
    }

    fn runtime_state(
        module_id: &ModuleId,
        active_artifact_id: &ArtifactId,
        candidate_artifact_id: Option<&ArtifactId>,
        hot_swap_state: HotSwapState,
        traffic_weight_bps: u16,
    ) -> ModuleLoaderRuntimeState {
        ModuleLoaderRuntimeState {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            activation_id: ModuleActivationId::from_legacy_named("module_activation_candidate")
                .expect("activation"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: active_artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: candidate_artifact_id.cloned(),
                candidate_artifact_id: candidate_artifact_id.cloned(),
                hot_swap_state,
            },
            traffic_weight_bps,
            last_loader_action: "advance-canary".to_string(),
            last_loader_stage: "canary".to_string(),
            last_transition_record_id: Id::from_legacy_named("rollout_transition")
                .expect("transition"),
            reason: "test apply".to_string(),
            applied_at: module_loader_applied_at_now(),
        }
    }

    fn route_key_with_bucket(module_id: &ModuleId, range: std::ops::Range<u16>) -> String {
        (0..20_000)
            .map(|index| format!("session-{index}"))
            .find(|route_key| range.contains(&stable_route_bucket_bps(module_id, route_key)))
            .expect("route key in bucket range")
    }

    fn replay_event(name: &str) -> EventEnvelope {
        EventEnvelope::new(
            EventId::from_legacy_named(name).expect("event"),
            OffsetDateTime::now_utc(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
            Source::new("test.replay").expect("source"),
            EventKind::new("test.replay.input").expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Local,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({"name": name}))
    }

    fn local_command_artifact(artifact_id: ArtifactId, script: &str) -> ModuleLocalCommandArtifact {
        ModuleLocalCommandArtifact {
            artifact_id,
            program: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            cwd: None,
            env: BTreeMap::new(),
            timeout_ms: Some(2_000),
            max_output_bytes: Some(16 * 1024),
        }
    }

    struct StaticModuleRunner {
        outputs: BTreeMap<ArtifactId, Result<String, String>>,
        calls: RefCell<Vec<ModuleArtifactInvocation>>,
    }

    impl StaticModuleRunner {
        fn new<const N: usize>(outputs: [(ArtifactId, Result<String, String>); N]) -> Self {
            Self {
                outputs: outputs.into_iter().collect(),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl ModuleArtifactRunner for StaticModuleRunner {
        fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String> {
            self.calls.borrow_mut().push(invocation.clone());
            self.outputs
                .get(&invocation.artifact_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("missing artifact {}", invocation.artifact_id)))
        }
    }
}
