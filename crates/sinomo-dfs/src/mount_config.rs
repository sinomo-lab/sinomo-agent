//! Node-local SIFS host-mount configuration and observed runtime status.
//!
//! Mounting is an application-layer host effect. DFS owns the typed config
//! contract so every application sees the same schema, while the main Sinomo
//! application process owns the platform adapter and its lifecycle.

pub use sinomo_api::dfs::mount_config::SIFS_MOUNT_CONFIG_KEY;
pub use sinomo_api::dfs::mount_config::SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE;
pub use sinomo_api::dfs::mount_config::SIFS_MOUNT_STATUS_CONFIG_KEY;
pub use sinomo_api::dfs::mount_config::SIFS_MOUNT_STATUS_ROUTE;

pub use sinomo_api::dfs::mount_config::sifs_mount_settings_module_id;

pub use sinomo_api::dfs::mount_config::SifsMountConfig;

pub use sinomo_api::dfs::mount_config::SifsMountPlatform;

pub use sinomo_api::dfs::mount_config::SifsMountRuntimePhase;

pub use sinomo_api::dfs::mount_config::SifsMountWritebackMode;

pub use sinomo_api::dfs::mount_config::SifsMountAttentionKind;

pub use sinomo_api::dfs::mount_config::SifsMountAttentionNextAction;

pub use sinomo_api::dfs::mount_config::SifsMountRetainedLocalEvidence;

pub use sinomo_api::dfs::mount_config::SifsMountAttentionSummary;

pub use sinomo_api::dfs::mount_config::SifsMountRuntimeStatus;

pub use sinomo_api::dfs::mount_config::SifsMountStatusRequest;

pub use sinomo_api::dfs::mount_config::SifsMountStatusResponse;

pub use sinomo_api::dfs::mount_config::SifsMountCursorOnlyRecoveryRequest;

pub use sinomo_api::dfs::mount_config::SifsMountCursorOnlyRecoveryResponse;

pub use sinomo_api::dfs::mount_config::sifs_mount_config_definition;

pub use sinomo_api::dfs::mount_config::sifs_mount_config_definition_for_module;

pub use sinomo_api::dfs::mount_config::sifs_mount_status_config_definition;

pub use sinomo_api::dfs::mount_config::sifs_mount_status_config_definition_for_module;
