use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_config::{
    ConfigClusterMode, ConfigDefinition, ConfigRegistry, ConfigSchema, ConfigSchemaField,
    ConfigSchemaValue, ConfigStore, ConfigValueType,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::{
    CapabilityName, FileSystemAccess, ModuleCapabilities, ModuleResourceLimits, NetworkAccess,
    TrustRing,
};
use sinomo_module::{ModuleId, ModuleRegistration, ModuleSurfaces};

pub const SINOMO_DFS_NFS_MODULE_LEGACY_ID: &str = "module.dfs.nfs";
pub const SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY: &str = "sinomo.dfs.nfs.native-rust";
pub const SINOMO_DFS_NFS_MODULE_OWNER: &str = "sinomo-dfs-nfs";
pub const SINOMO_DFS_NFS_PROTOCOL: &str = "sinomo.dfs.nfs.v1";
pub const NFS_MOUNT_CONFIG_KEY: &str = "storage.sifs-nfs-mount";
pub const NFS_MOUNT_STATUS_CONFIG_KEY: &str = "storage.sifs-nfs-mount-status";
pub const NFS_MODULE_LABEL: &str = "NFS Mount";
pub const NFS_MODULE_DESCRIPTION: &str =
    "Direct SIFS mount through the operating system NFSv3 client without a projection tree.";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NfsMountConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_mount_target")]
    pub target: String,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_export_name")]
    pub export_name: String,
    #[serde(default = "default_root_path")]
    pub root_path: String,
    #[serde(default)]
    pub read_only: bool,
}

impl Default for NfsMountConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target: default_mount_target(),
            bind: default_bind(),
            export_name: default_export_name(),
            root_path: default_root_path(),
            read_only: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NfsMountRuntimePhase {
    #[default]
    Disabled,
    Starting,
    Mounted,
    Stopping,
    Error,
    Unsupported,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NfsMountRuntimeStatus {
    pub phase: NfsMountRuntimePhase,
    pub platform: String,
    pub configured_target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_target: Option<String>,
    pub bind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub export_name: String,
    pub root_path: String,
    pub read_only: bool,
    pub server_running: bool,
    pub mounted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    pub detail: String,
    pub updated_at: String,
}

impl Default for NfsMountRuntimeStatus {
    fn default() -> Self {
        let mut status = Self::disabled(&NfsMountConfig::default());
        status.updated_at.clear();
        status
    }
}

impl NfsMountRuntimeStatus {
    pub fn disabled(config: &NfsMountConfig) -> Self {
        Self {
            phase: NfsMountRuntimePhase::Disabled,
            platform: platform_label().to_string(),
            configured_target: config.target.clone(),
            resolved_target: None,
            bind: config.bind.clone(),
            endpoint: None,
            export_name: config.export_name.clone(),
            root_path: config.root_path.clone(),
            read_only: config.read_only,
            server_running: false,
            mounted: false,
            error_code: None,
            last_error: None,
            resolution: None,
            detail: "NFS mount is disabled".to_string(),
            updated_at: now_rfc3339(),
        }
    }

    pub fn error(
        config: &NfsMountConfig,
        resolved_target: Option<String>,
        code: impl Into<String>,
        detail: impl Into<String>,
        resolution: impl Into<String>,
    ) -> Self {
        let detail = detail.into();
        Self {
            phase: NfsMountRuntimePhase::Error,
            platform: platform_label().to_string(),
            configured_target: config.target.clone(),
            resolved_target,
            bind: config.bind.clone(),
            endpoint: None,
            export_name: config.export_name.clone(),
            root_path: config.root_path.clone(),
            read_only: config.read_only,
            server_running: false,
            mounted: false,
            error_code: Some(code.into()),
            last_error: Some(detail.clone()),
            resolution: Some(resolution.into()),
            detail,
            updated_at: now_rfc3339(),
        }
    }
}

pub fn nfs_module_id() -> ModuleId {
    ModuleId::from_legacy_named(SINOMO_DFS_NFS_MODULE_LEGACY_ID)
        .expect("NFS module id must be valid")
}

pub fn nfs_mount_config_definition() -> ConfigDefinition {
    ConfigDefinition::new(
        NFS_MOUNT_CONFIG_KEY,
        SINOMO_DFS_NFS_MODULE_OWNER,
        ConfigValueType::Object {
            schema_name: "NfsMountConfig".to_string(),
        },
        "Configure the direct SIFS NFS mount on this device.",
        ConfigClusterMode::LocalOnly,
    )
    .expect("valid NFS mount config definition")
    .with_remote_host_management()
    .with_default(&NfsMountConfig::default())
    .expect("valid NFS mount config default")
    .with_schema(ConfigSchema::object(
        "NfsMountConfig",
        "One daemon-supervised, loopback-only NFSv3 mount on this device.",
        vec![
            ConfigSchemaField::new(
                "enabled",
                "Enabled",
                "Keep the NFS server and native host mount active while the main daemon is running.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(false)),
            ConfigSchemaField::new(
                "target",
                "Mount target",
                "Linux directory or Windows drive letter used by the native NFS client.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_mount_target())),
            ConfigSchemaField::new(
                "bind",
                "Loopback endpoint",
                "Loopback listen address. Linux may use port 0; Windows drive mapping requires 127.0.0.1:111.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_bind())),
            ConfigSchemaField::new(
                "export_name",
                "Export name",
                "NFS export name without slash characters.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_export_name())),
            ConfigSchemaField::new(
                "root_path",
                "SIFS root",
                "Absolute SIFS subtree exposed by this mount.",
                ConfigSchemaValue::string(),
            )
            .with_default_value(Value::String(default_root_path())),
            ConfigSchemaField::new(
                "read_only",
                "Read only",
                "Reject NFS mutations and request a read-only native mount.",
                ConfigSchemaValue::bool(),
            )
            .with_default_value(Value::Bool(false)),
        ],
    ))
    .expect("valid NFS mount config schema")
    .with_module_id(nfs_module_id().to_string())
    .expect("valid NFS mount config module id")
    .with_module_presentation(NFS_MODULE_LABEL, NFS_MODULE_DESCRIPTION)
    .expect("valid NFS mount module presentation")
}

pub fn nfs_mount_status_config_definition() -> ConfigDefinition {
    ConfigDefinition::new(
        NFS_MOUNT_STATUS_CONFIG_KEY,
        SINOMO_DFS_NFS_MODULE_OWNER,
        ConfigValueType::Object {
            schema_name: "NfsMountRuntimeStatus".to_string(),
        },
        "Observed NFS mount lifecycle and actionable host errors.",
        ConfigClusterMode::LocalOnly,
    )
    .expect("valid NFS mount status definition")
    .with_default(&NfsMountRuntimeStatus::default())
    .expect("valid NFS mount status default")
    .with_schema(ConfigSchema::object(
        "NfsMountRuntimeStatus",
        "Read-only status published by the NFS module. Error and resolution fields explain required operator action.",
        vec![
            status_field("phase", "Phase", "Current NFS mount lifecycle phase."),
            status_field("platform", "Platform", "Native host platform."),
            status_field("configured_target", "Configured target", "Target exactly as configured."),
            ConfigSchemaField::new("resolved_target", "Resolved target", "Validated absolute Linux path or normalized Windows drive.", ConfigSchemaValue::string()).optional(),
            status_field("bind", "Configured endpoint", "Configured loopback listen address."),
            ConfigSchemaField::new("endpoint", "Active endpoint", "Bound loopback address and selected port.", ConfigSchemaValue::string()).optional(),
            status_field("export_name", "Export", "Active NFS export name."),
            status_field("root_path", "SIFS root", "Exported SIFS subtree."),
            ConfigSchemaField::new("read_only", "Read only", "Whether mutations are rejected.", ConfigSchemaValue::bool()),
            ConfigSchemaField::new("server_running", "Server running", "Whether the NFS RPC listener is active.", ConfigSchemaValue::bool()),
            ConfigSchemaField::new("mounted", "Mounted", "Whether the native client mount completed.", ConfigSchemaValue::bool()),
            ConfigSchemaField::new("error_code", "Error code", "Stable category for the current failure.", ConfigSchemaValue::string()).optional(),
            ConfigSchemaField::new("last_error", "Error", "Detailed mount failure shown to the operator.", ConfigSchemaValue::string()).optional(),
            ConfigSchemaField::new("resolution", "How to resolve", "Concrete action required before retrying the mount.", ConfigSchemaValue::string()).optional(),
            status_field("detail", "Detail", "Current lifecycle result."),
            status_field("updated_at", "Updated", "UTC timestamp of this observation."),
        ],
    ))
    .expect("valid NFS mount status schema")
    .with_module_id(nfs_module_id().to_string())
    .expect("valid NFS mount status module id")
    .with_module_presentation(NFS_MODULE_LABEL, NFS_MODULE_DESCRIPTION)
    .expect("valid NFS mount status presentation")
    .read_only()
    .with_operational_error_presentation("last_error", "resolution")
    .expect("valid NFS operational error presentation")
}

fn status_field(
    name: &'static str,
    label: &'static str,
    description: &'static str,
) -> ConfigSchemaField {
    ConfigSchemaField::new(name, label, description, ConfigSchemaValue::string())
}

pub fn nfs_module_surfaces() -> ModuleSurfaces {
    let mut surfaces = ModuleSurfaces::default()
        .with_config_definition(nfs_mount_config_definition())
        .expect("valid NFS mount settings surface")
        .with_config_definition(nfs_mount_status_config_definition())
        .expect("valid NFS mount status surface");
    surfaces.capabilities = nfs_module_capabilities();
    surfaces
}

pub fn nfs_module_registration() -> ModuleRegistration {
    let mut registration = ModuleRegistration::native_rust(
        nfs_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_DFS_NFS_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        SINOMO_DFS_NFS_PROTOCOL,
        TrustRing::R1TrustedRuntime,
        SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = nfs_module_capabilities();
    registration.manifest.limits = ModuleResourceLimits {
        network: NetworkAccess::Allowlisted,
        filesystem: FileSystemAccess::Scoped,
        ..ModuleResourceLimits::default()
    };
    registration.surfaces = nfs_module_surfaces();
    registration
}

pub fn nfs_module_capabilities() -> ModuleCapabilities {
    ModuleCapabilities {
        imports: vec![
            capability("storage.local.read"),
            capability("storage.local.write"),
            capability("network.object_rpc"),
        ],
        exports: vec![capability("sifs.nfs.mount")],
    }
}

pub fn nfs_config_store() -> Result<ConfigStore, String> {
    let builder = ConfigRegistry::builder()
        .register(nfs_mount_config_definition())
        .map_err(|error| error.to_string())?
        .register(nfs_mount_status_config_definition())
        .map_err(|error| error.to_string())?;
    Ok(ConfigStore::new(builder.build()))
}

pub fn register_config_definitions(
    builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    builder
        .register(nfs_mount_config_definition())
        .map_err(|error| error.to_string())?
        .register(nfs_mount_status_config_definition())
        .map_err(|error| error.to_string())
}

pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor =
        sinomo_module::component_descriptor_from_registration(&nfs_module_registration())?;
    sinomo_module::StaticComponentFactory::new(descriptor, |context, scope| {
        let runtime =
            context
                .runtime()
                .ok_or_else(|| sinomo_module::ComponentError::RuntimeUnavailable {
                    component_id: SINOMO_DFS_NFS_MODULE_LEGACY_ID.to_string(),
                })?;
        let data_dir = SinomoDataDir::new(context.data_dir_root());
        let task = crate::spawn_nfs_mount_supervisor(data_dir, runtime, scope.shutdown_receiver());
        scope.own_task("NFS mount supervisor", task);
        Ok(())
    })
}

pub fn nfs_module_enabled(data_dir: &SinomoDataDir) -> Result<bool, String> {
    sinomo_module::native_rust_module_enabled(&data_dir.root(), &nfs_module_id(), true)
        .map_err(|error| error.to_string())
}

fn capability(name: &str) -> CapabilityName {
    CapabilityName::new(name).expect("valid NFS module capability")
}

pub fn default_bind() -> String {
    if cfg!(windows) {
        "127.0.0.1:111".to_string()
    } else {
        "127.0.0.1:0".to_string()
    }
}

pub fn default_mount_target() -> String {
    if cfg!(windows) {
        "S:".to_string()
    } else {
        "~/sinomo-nfs".to_string()
    }
}

fn default_export_name() -> String {
    "sifs".to_string()
}

fn default_root_path() -> String {
    "/".to_string()
}

pub fn platform_label() -> &'static str {
    if cfg!(windows) {
        "windows-nfs"
    } else if cfg!(target_os = "linux") {
        "linux-nfs"
    } else {
        "unsupported"
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_registration_owns_editable_config_and_actionable_status_surfaces() {
        let registration = nfs_module_registration();
        assert_eq!(registration.module_id(), &nfs_module_id());
        registration
            .validate_surfaces()
            .expect("valid NFS surfaces");
        let definitions = registration
            .surfaces
            .config_definitions()
            .expect("decode NFS settings");
        assert_eq!(definitions.len(), 2);
        for definition in &definitions {
            assert_eq!(
                definition.module_id.as_deref(),
                Some(nfs_module_id().as_str())
            );
            assert_eq!(definition.module_label.as_deref(), Some(NFS_MODULE_LABEL));
            assert_eq!(
                definition.module_description.as_deref(),
                Some(NFS_MODULE_DESCRIPTION)
            );
        }
        let config = definitions
            .iter()
            .find(|definition| definition.key.as_str() == NFS_MOUNT_CONFIG_KEY)
            .expect("editable NFS config");
        assert!(!config.read_only);
        let status = definitions
            .iter()
            .find(|definition| definition.key.as_str() == NFS_MOUNT_STATUS_CONFIG_KEY)
            .expect("read-only NFS status");
        assert!(status.read_only);
        assert_eq!(
            status.operational_error,
            Some(sinomo_config::ConfigOperationalErrorPresentation {
                error_field: "last_error".to_string(),
                resolution_field: "resolution".to_string(),
            })
        );
    }

    #[test]
    fn runtime_error_carries_operator_resolution() {
        let config = NfsMountConfig::default();
        let status = NfsMountRuntimeStatus::error(
            &config,
            Some("/mnt/sinomo".to_string()),
            "mount-permission-required",
            "mount.nfs: Operation not permitted",
            "Grant mount authority and retry.",
        );
        assert_eq!(status.phase, NfsMountRuntimePhase::Error);
        assert_eq!(
            status.error_code.as_deref(),
            Some("mount-permission-required")
        );
        assert_eq!(
            status.last_error.as_deref(),
            Some("mount.nfs: Operation not permitted")
        );
        assert_eq!(
            status.resolution.as_deref(),
            Some("Grant mount authority and retry.")
        );
    }
}
