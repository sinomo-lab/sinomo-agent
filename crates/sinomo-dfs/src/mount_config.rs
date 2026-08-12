//! Node-local SIFS host-mount configuration and observed runtime status.
//!
//! Mounting is an application-layer host effect. DFS owns the typed config
//! contract so every application sees the same schema, while the main Sinomo
//! application process owns the platform adapter and its lifecycle.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_config::{
    ConfigClusterMode, ConfigDefinition, ConfigSchema, ConfigSchemaField, ConfigSchemaValue,
    ConfigValueType,
};

use crate::{SINOMO_DFS_MODULE_OWNER, dfs_module_id};

pub const SIFS_MOUNT_CONFIG_KEY: &str = "storage.sifs-mount";
pub const SIFS_MOUNT_STATUS_CONFIG_KEY: &str = "storage.sifs-mount-status";
pub const SIFS_MOUNT_STATUS_ROUTE: &str = "/dfs/mount/status";
pub const SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE: &str = "/dfs/mount/recover-cursor-only-state";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_mount_point")]
    pub mount_point: String,
    #[serde(default = "default_root_path")]
    pub root_path: String,
    #[serde(default = "default_read_only")]
    pub read_only: bool,
    #[serde(default)]
    pub allow_other: bool,
    #[serde(default = "default_sync_local_changes")]
    pub sync_local_changes: bool,
}

impl Default for SifsMountConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mount_point: default_mount_point(),
            root_path: default_root_path(),
            read_only: default_read_only(),
            allow_other: false,
            sync_local_changes: default_sync_local_changes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SifsMountPlatform {
    UnixFuse,
    WindowsCloudFiles,
    Unsupported,
}

impl SifsMountPlatform {
    pub const fn current() -> Self {
        if cfg!(windows) {
            Self::WindowsCloudFiles
        } else if cfg!(target_os = "linux") {
            Self::UnixFuse
        } else {
            Self::Unsupported
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SifsMountRuntimePhase {
    Disabled,
    WaitingForDaemon,
    Starting,
    Registered,
    Mounted,
    Connected,
    Stopping,
    Error,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SifsMountWritebackMode {
    #[default]
    Disabled,
    Unsupported,
    ReadOnlyEnforcementUnavailable,
    Starting,
    Active,
    Blocked,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SifsMountAttentionKind {
    LocalOperationConflict,
    FailedLocalOperation,
    RemoteNamespaceConflict,
    StartupConflict,
    UnsupportedObjectKind,
    StaleOrAmbiguousReconciliation,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SifsMountAttentionNextAction {
    ReviewAndResolve,
    RepairCauseAndRetry,
    PreserveLocalCopyAndRescan,
    ReviewUnsupportedEntry,
    WaitForAuthoritativeReconciliation,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountRetainedLocalEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_state: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountAttentionSummary {
    pub kind: SifsMountAttentionKind,
    pub path: String,
    pub reason_code: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_local_evidence: Option<SifsMountRetainedLocalEvidence>,
    pub safe_next_action: SifsMountAttentionNextAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountRuntimeStatus {
    pub phase: SifsMountRuntimePhase,
    pub platform: SifsMountPlatform,
    pub configured_mount_point: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_mount_point: Option<String>,
    pub root_path: String,
    pub read_only: bool,
    #[serde(default)]
    pub sync_local_changes: bool,
    #[serde(default)]
    pub writeback_mode: SifsMountWritebackMode,
    #[serde(default)]
    pub provider_registered: bool,
    #[serde(default)]
    pub provider_connected: bool,
    #[serde(default)]
    pub worker_alive: bool,
    #[serde(default)]
    pub worker_ready: bool,
    #[serde(default)]
    pub worker_health_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_worker_heartbeat_at: Option<String>,
    #[serde(default)]
    pub pending_operations: u64,
    #[serde(default)]
    pub retrying_operations: u64,
    #[serde(default)]
    pub failed_operations: u64,
    #[serde(default)]
    pub conflict_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attention: Vec<SifsMountAttentionSummary>,
    #[serde(default)]
    pub attention_overflow: u64,
    #[serde(default)]
    pub unregister_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daemon_pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_interface_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholders_created: Option<u64>,
    #[serde(default)]
    pub restart_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub detail: String,
    pub updated_at: String,
}

impl Default for SifsMountRuntimeStatus {
    fn default() -> Self {
        let config = SifsMountConfig::default();
        Self {
            phase: SifsMountRuntimePhase::Disabled,
            platform: SifsMountPlatform::current(),
            configured_mount_point: config.mount_point,
            resolved_mount_point: None,
            root_path: config.root_path,
            read_only: config.read_only,
            sync_local_changes: config.sync_local_changes,
            writeback_mode: SifsMountWritebackMode::Disabled,
            provider_registered: false,
            provider_connected: false,
            worker_alive: false,
            worker_ready: false,
            worker_health_generation: 0,
            last_worker_heartbeat_at: None,
            pending_operations: 0,
            retrying_operations: 0,
            failed_operations: 0,
            conflict_count: 0,
            attention: Vec::new(),
            attention_overflow: 0,
            unregister_pending: false,
            daemon_pid: None,
            provider_content_hash: None,
            provider_interface_version: None,
            placeholders_created: None,
            restart_count: 0,
            last_error: None,
            detail: "SIFS host mount is disabled".to_string(),
            updated_at: String::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountStatusRequest;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SifsMountStatusResponse {
    pub status: SifsMountRuntimeStatus,
}

/// Daemon-local control request for the explicit Windows Cloud Files
/// cursor-only recovery. The main daemon's mount supervisor serializes this
/// with registration staging, cleanup, and active-provider lifecycle work.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum SifsMountCursorOnlyRecoveryRequest {
    Inspect,
    Apply {
        expected_token: String,
        acknowledge_authoritative_full_reconciliation: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum SifsMountCursorOnlyRecoveryResponse {
    Inspected {
        recovery_token: String,
        raw_state_fingerprint: String,
        sync_root_file_id: u64,
        cursor_count: u64,
        provider_content_hash: String,
        provider_interface_version: String,
        detail: String,
    },
    Applied {
        recovery_id: String,
        recovery_token: String,
        sync_root_file_id: u64,
        cursor_count_cleared: u64,
        detail: String,
    },
}

pub fn sifs_mount_config_definition() -> ConfigDefinition {
    ConfigDefinition::new(
        SIFS_MOUNT_CONFIG_KEY,
        SINOMO_DFS_MODULE_OWNER,
        ConfigValueType::Object {
            schema_name: "SifsMountConfig".to_string(),
        },
        "Configure the SIFS host filesystem mount on this device.",
        ConfigClusterMode::LocalOnly,
    )
    .expect("valid SIFS mount config definition")
    .with_default(&SifsMountConfig::default())
    .expect("valid SIFS mount config default")
    .with_schema(ConfigSchema::object(
        "SifsMountConfig",
        "One supervised SIFS mount owned by this device's main Sinomo process.",
        vec![
            ConfigSchemaField::new(
                "enabled",
                "Enabled",
                "Keep the configured SIFS mount active while the main daemon is running.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(false)),
            ConfigSchemaField::new(
                "mount_point",
                "Mount location",
                "Absolute host path, or a path beginning with ~/ for the current user.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_mount_point())),
            ConfigSchemaField::new(
                "root_path",
                "SIFS root",
                "Absolute SIFS subtree exposed at the host mount.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_root_path())),
            ConfigSchemaField::new(
                "read_only",
                "Read only",
                "Expose the mount without host-side write operations.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(default_read_only())),
            ConfigSchemaField::new(
                "allow_other",
                "Allow other local users",
                "Unix FUSE only. Requires the host FUSE allow_other policy.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(false)),
            ConfigSchemaField::new(
                "sync_local_changes",
                "Sync local changes",
                "Windows Cloud Files only. Durably journal, version-fence, and reconcile host-side changes before reporting them synchronized.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(default_sync_local_changes())),
        ],
    ))
    .expect("valid SIFS mount config schema")
    .with_module_id(dfs_module_id().to_string())
    .expect("valid DFS module id")
}

pub fn sifs_mount_status_config_definition() -> ConfigDefinition {
    ConfigDefinition::new(
        SIFS_MOUNT_STATUS_CONFIG_KEY,
        SINOMO_DFS_MODULE_OWNER,
        ConfigValueType::Object {
            schema_name: "SifsMountRuntimeStatus".to_string(),
        },
        "Observed state of the supervised SIFS mount on this device.",
        ConfigClusterMode::LocalOnly,
    )
    .expect("valid SIFS mount status definition")
    .with_default(&SifsMountRuntimeStatus::default())
    .expect("valid SIFS mount status default")
    .with_schema(ConfigSchema::object(
        "SifsMountRuntimeStatus",
        "Read-only observed state published by the local mount supervisor.",
        vec![
            ConfigSchemaField::new(
                "phase",
                "Phase",
                "Current mount lifecycle phase.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "platform",
                "Platform adapter",
                "Host adapter selected for this device.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "configured_mount_point",
                "Configured location",
                "Mount location exactly as configured.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "resolved_mount_point",
                "Resolved location",
                "Absolute host mount location after user-home expansion.",
                ConfigSchemaValue::string(),
            )
            .optional(),
            ConfigSchemaField::new(
                "root_path",
                "SIFS root",
                "SIFS subtree exposed by the active mount.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "read_only",
                "Read only",
                "Whether the observed mount is read-only.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "sync_local_changes",
                "Sync local changes",
                "Whether the observed adapter is configured to durably synchronize host-side changes.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "writeback_mode",
                "Writeback mode",
                "Observed durable writeback lifecycle mode.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "provider_registered",
                "Provider registered",
                "Whether the Windows Cloud Files sync root is persistently registered.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "provider_connected",
                "Provider connected",
                "Whether Cloud Files callbacks are currently connected.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "worker_alive",
                "Worker alive",
                "Whether the writable synchronization worker is alive.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "worker_ready",
                "Worker ready",
                "Whether startup reconciliation completed before provider connection.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "worker_health_generation",
                "Worker health generation",
                "Monotonic writable-worker heartbeat generation.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "last_worker_heartbeat_at",
                "Last worker heartbeat",
                "UTC time when the supervisor last received a newer worker health generation.",
                ConfigSchemaValue::string(),
            )
            .optional(),
            ConfigSchemaField::new(
                "pending_operations",
                "Pending operations",
                "Durable host-side operations waiting for DFS application.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "retrying_operations",
                "Retrying operations",
                "Durable operations being reconciled or retried.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "failed_operations",
                "Failed operations",
                "Durable operations requiring operator attention.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "conflict_count",
                "Conflicts",
                "Durable local-versus-remote conflicts awaiting resolution.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "attention",
                "Attention details",
                "Bounded typed summaries for conflicts, failures, unsupported entries, and ambiguous reconciliation.",
                ConfigSchemaValue::list(sifs_mount_attention_schema_value()),
            )
            .optional(),
            ConfigSchemaField::new(
                "attention_overflow",
                "Additional attention items",
                "Number of actionable details omitted from the bounded status payload.",
                ConfigSchemaValue::u64(None),
            )
            .optional()
            .with_default_value(Value::from(0u64)),
            ConfigSchemaField::new(
                "unregister_pending",
                "Unregister pending",
                "Whether a retired Windows Cloud Files registration still requires cleanup.",
                ConfigSchemaValue::bool(),
            ),
            ConfigSchemaField::new(
                "daemon_pid",
                "Daemon PID",
                "Main daemon process that owns the active adapter.",
                ConfigSchemaValue::u64(None),
            )
            .optional(),
            ConfigSchemaField::new(
                "provider_content_hash",
                "Provider hash",
                "Content hash of the active Windows Cloud Files provider.",
                ConfigSchemaValue::string(),
            )
            .optional(),
            ConfigSchemaField::new(
                "provider_interface_version",
                "Provider interface",
                "Interface version of the active Windows Cloud Files provider.",
                ConfigSchemaValue::string(),
            )
            .optional(),
            ConfigSchemaField::new(
                "placeholders_created",
                "Placeholders created",
                "Number of Cloud Files placeholders created while connecting.",
                ConfigSchemaValue::u64(None),
            )
            .optional(),
            ConfigSchemaField::new(
                "restart_count",
                "Restart count",
                "Number of adapter restarts in the current daemon process.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "last_error",
                "Last error",
                "Most recent adapter failure, when present.",
                ConfigSchemaValue::string(),
            )
            .optional(),
            ConfigSchemaField::new(
                "detail",
                "Detail",
                "Observed lifecycle result or failure detail.",
                ConfigSchemaValue::string(),
            ),
            ConfigSchemaField::new(
                "updated_at",
                "Updated",
                "UTC timestamp of the observation.",
                ConfigSchemaValue::string(),
            ),
        ],
    ))
    .expect("valid SIFS mount status schema")
    .with_module_id(dfs_module_id().to_string())
    .expect("valid DFS module id")
    .read_only()
}

fn default_mount_point() -> String {
    "~/sinomo".to_string()
}

fn sifs_mount_attention_schema_value() -> ConfigSchemaValue {
    ConfigSchemaValue::object(vec![
        ConfigSchemaField::new(
            "kind",
            "Kind",
            "Typed category of the actionable mount issue.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "path",
            "Path",
            "Canonical SIFS path associated with the issue.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "reason_code",
            "Reason code",
            "Stable machine-readable reason for the issue.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "reason",
            "Reason",
            "Bounded human-readable explanation.",
            ConfigSchemaValue::string(),
        ),
        ConfigSchemaField::new(
            "retained_local_evidence",
            "Retained local evidence",
            "Identifiers and content metadata for durable local evidence; never the content bytes.",
            sifs_mount_retained_evidence_schema_value(),
        )
        .optional(),
        ConfigSchemaField::new(
            "safe_next_action",
            "Safe next action",
            "Non-destructive operator guidance for the issue.",
            ConfigSchemaValue::string(),
        ),
    ])
    .without_unknown_fields()
}

fn sifs_mount_retained_evidence_schema_value() -> ConfigSchemaValue {
    ConfigSchemaValue::object(vec![
        ConfigSchemaField::new(
            "operation_id",
            "Operation ID",
            "Stable durable writeback operation identity.",
            ConfigSchemaValue::string(),
        )
        .optional(),
        ConfigSchemaField::new(
            "evidence_id",
            "Evidence ID",
            "Stable identity for retained local evidence.",
            ConfigSchemaValue::string(),
        )
        .optional(),
        ConfigSchemaField::new(
            "staged_content_hash",
            "Staged content hash",
            "Content hash of a retained immutable staged blob.",
            ConfigSchemaValue::string(),
        )
        .optional(),
        ConfigSchemaField::new(
            "staged_content_size_bytes",
            "Staged content size",
            "Size of the retained staged blob in bytes.",
            ConfigSchemaValue::u64(None),
        )
        .optional(),
        ConfigSchemaField::new(
            "local_state",
            "Local state",
            "Bounded provider-local state label for the evidence.",
            ConfigSchemaValue::string(),
        )
        .optional(),
    ])
    .without_unknown_fields()
}

fn default_root_path() -> String {
    "/".to_string()
}

fn default_sync_local_changes() -> bool {
    false
}

fn default_read_only() -> bool {
    cfg!(windows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_config::{ConfigRegistry, ConfigStore, ConfigWriteTarget};
    use sinomo_core::local::SinomoDataDir;
    use sinomo_testdir::TestDir;

    #[test]
    fn mount_configuration_is_node_local_and_disabled_by_default() {
        let definition = sifs_mount_config_definition();

        assert_eq!(definition.cluster_mode, ConfigClusterMode::LocalOnly);
        assert_eq!(
            definition.module_id.as_deref(),
            Some(dfs_module_id().as_str())
        );
        let default: SifsMountConfig =
            serde_json::from_value(definition.default_value.expect("mount config default"))
                .expect("typed mount config default");
        assert!(!default.enabled);
        assert_eq!(default.mount_point, "~/sinomo");
        assert_eq!(default.root_path, "/");
    }

    #[test]
    fn mount_status_is_catalogued_as_read_only_observation() {
        let definition = sifs_mount_status_config_definition();

        assert_eq!(definition.cluster_mode, ConfigClusterMode::LocalOnly);
        assert!(definition.read_only);
        let status: SifsMountRuntimeStatus =
            serde_json::from_value(definition.default_value.expect("mount status default"))
                .expect("typed mount status default");
        assert_eq!(status.phase, SifsMountRuntimePhase::Disabled);
        assert_eq!(status.platform, SifsMountPlatform::current());
    }

    #[test]
    fn legacy_mount_status_defaults_attention_details() {
        let registry = ConfigRegistry::builder()
            .register(sifs_mount_status_config_definition())
            .expect("register module-owned mount status schema")
            .build();
        let store = ConfigStore::new(registry);
        let temp = TestDir::new("sifs-mount-attention-status");
        let data_dir = SinomoDataDir::new(temp.path());

        let mut status = SifsMountRuntimeStatus::default();
        status.attention.push(SifsMountAttentionSummary {
            kind: SifsMountAttentionKind::LocalOperationConflict,
            path: "/services/report.txt".to_string(),
            reason_code: "remote-version-changed".to_string(),
            reason: "the authoritative version changed".to_string(),
            retained_local_evidence: Some(SifsMountRetainedLocalEvidence {
                operation_id: Some("cf_01localoperation".to_string()),
                staged_content_hash: Some("blake3:staged".to_string()),
                staged_content_size_bytes: Some(42),
                ..SifsMountRetainedLocalEvidence::default()
            }),
            safe_next_action: SifsMountAttentionNextAction::PreserveLocalCopyAndRescan,
        });
        status.attention_overflow = 3;
        store
            .set_typed(
                &data_dir,
                None,
                SIFS_MOUNT_STATUS_CONFIG_KEY,
                &status,
                ConfigWriteTarget::Local,
            )
            .expect("persist status with typed attention");
        let observed: SifsMountRuntimeStatus = store
            .get_typed(&data_dir, None, SIFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("read status with typed attention");
        assert_eq!(observed.attention, status.attention);
        assert_eq!(observed.attention_overflow, 3);

        let mut encoded =
            serde_json::to_value(SifsMountRuntimeStatus::default()).expect("encode mount status");
        let object = encoded.as_object_mut().expect("mount status object");
        object.remove("attention");
        object.remove("attention_overflow");

        store
            .set(
                &data_dir,
                None,
                SIFS_MOUNT_STATUS_CONFIG_KEY,
                encoded,
                ConfigWriteTarget::Local,
            )
            .expect("persist legacy status through the closed schema");
        let decoded: SifsMountRuntimeStatus = store
            .get_typed(&data_dir, None, SIFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("decode legacy mount status through ConfigStore");

        assert!(decoded.attention.is_empty());
        assert_eq!(decoded.attention_overflow, 0);
    }
}
