//! Node-local SIFS host-mount configuration for the Settings UI.
//!
//! Reads and writes target the selected node through its config service. Host mount
//! writes are authorized by that node; its supervisor owns validation and effects.

use std::path::{Component, Path, PathBuf};

use sinomo_runtime::config::{ConfigValueSource, ConfigWriteTarget};
use sinomo_runtime::dfs::{
    SIFS_MOUNT_CONFIG_KEY, SIFS_MOUNT_STATUS_CONFIG_KEY, SIFS_MOUNT_STATUS_ROUTE, SifsMountConfig,
    SifsMountPlatform, SifsMountRuntimeStatus, SifsMountStatusRequest, SifsMountStatusResponse,
};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::backend::Backend;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsMountSettingsSnapshot {
    pub config: SifsMountConfig,
    pub platform: SifsMountPlatform,
    pub status: SifsMountRuntimeStatus,
    pub config_error: Option<String>,
    pub status_error: Option<String>,
}

impl Default for SifsMountSettingsSnapshot {
    fn default() -> Self {
        Self {
            config: SifsMountConfig::default(),
            platform: SifsMountPlatform::current(),
            status: SifsMountRuntimeStatus::default(),
            config_error: None,
            status_error: None,
        }
    }
}

impl Backend {
    pub fn sifs_mount_settings_snapshot(&self) -> SifsMountSettingsSnapshot {
        let live_status = self.live_sifs_mount_status();
        let platform = live_status
            .as_ref()
            .map(|status| status.platform)
            .unwrap_or_else(|_| {
                if self.settings_node_is_remote() {
                    SifsMountPlatform::Unsupported
                } else {
                    SifsMountPlatform::current()
                }
            });
        let (config, config_error) = match self.config.get(SIFS_MOUNT_CONFIG_KEY) {
            Ok(resolution) => match serde_json::from_value::<SifsMountConfig>(resolution.value) {
                Ok(config) => {
                    let error = validate_sifs_mount_config(&config, platform)
                        .and_then(|()| {
                            if self.settings_node_is_remote() {
                                Ok(())
                            } else {
                                validate_sifs_mount_target(&config, &self.data_dir.root())
                            }
                        })
                        .err();
                    (config, error)
                }
                Err(error) => (
                    SifsMountConfig::default(),
                    Some(format!("invalid SIFS mount configuration: {error}")),
                ),
            },
            Err(error) => (
                SifsMountConfig::default(),
                Some(format!("SIFS mount configuration unavailable: {error}")),
            ),
        };
        let persisted_status = match self.config.get(SIFS_MOUNT_STATUS_CONFIG_KEY) {
            Ok(resolution) => {
                let source = resolution.source;
                match serde_json::from_value::<SifsMountRuntimeStatus>(resolution.value) {
                    Ok(status) => {
                        let error = validate_sifs_mount_status_on_platform(
                            &config, &status, source, platform,
                        )
                        .err();
                        (status, error)
                    }
                    Err(error) => (
                        SifsMountRuntimeStatus::default(),
                        Some(format!("invalid SIFS mount status: {error}")),
                    ),
                }
            }
            Err(error) => (
                SifsMountRuntimeStatus::default(),
                Some(format!("SIFS mount status unavailable: {error}")),
            ),
        };
        let (status, status_error) = match live_status {
            Ok(status) => {
                let error = validate_sifs_mount_status_on_platform(
                    &config,
                    &status,
                    ConfigValueSource::Local,
                    platform,
                )
                .err();
                (status, error)
            }
            Err(live_error) => {
                let (status, persisted_error) = persisted_status;
                let status_error = if config.enabled
                    || status.phase != sinomo_runtime::dfs::SifsMountRuntimePhase::Disabled
                {
                    Some(match persisted_error {
                        Some(persisted_error) => {
                            format!(
                                "{live_error}; persisted observation is also invalid: {persisted_error}"
                            )
                        }
                        None => format!(
                            "{live_error}; showing the last persisted observation, which is not liveness evidence"
                        ),
                    })
                } else {
                    persisted_error
                };
                (status, status_error)
            }
        };
        SifsMountSettingsSnapshot {
            config,
            platform,
            status,
            config_error,
            status_error,
        }
    }

    pub fn set_local_sifs_mount_config(&self, config: &SifsMountConfig) -> Result<(), String> {
        validate_sifs_mount_config(config, self.settings_mount_platform()?)?;
        if !self.settings_node_is_remote() {
            validate_sifs_mount_target(config, &self.data_dir.root())?;
        }
        let value = serde_json::to_value(config).map_err(|error| error.to_string())?;
        self.config
            .set(SIFS_MOUNT_CONFIG_KEY, value, ConfigWriteTarget::Local)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn live_sifs_mount_status(&self) -> Result<SifsMountRuntimeStatus, String> {
        self.settings_node_rpc::<_, SifsMountStatusResponse>(
            SIFS_MOUNT_STATUS_ROUTE,
            "sinomo.dfs.mount.status",
            &SifsMountStatusRequest,
        )
        .map(|response| response.status)
    }
}

#[cfg(test)]
fn validate_sifs_mount_status(
    config: &SifsMountConfig,
    status: &SifsMountRuntimeStatus,
    source: ConfigValueSource,
) -> Result<(), String> {
    validate_sifs_mount_status_on_platform(config, status, source, SifsMountPlatform::current())
}

fn validate_sifs_mount_status_on_platform(
    config: &SifsMountConfig,
    status: &SifsMountRuntimeStatus,
    source: ConfigValueSource,
    platform: SifsMountPlatform,
) -> Result<(), String> {
    if config.enabled
        && (source == ConfigValueSource::Default || status.updated_at.trim().is_empty())
    {
        return Err(
            "the enabled mount has no observation from the local supervisor yet".to_string(),
        );
    }
    if matches!(
        status.phase,
        sinomo_runtime::dfs::SifsMountRuntimePhase::Registered
            | sinomo_runtime::dfs::SifsMountRuntimePhase::Mounted
            | sinomo_runtime::dfs::SifsMountRuntimePhase::Connected
    ) && status.platform != platform
    {
        return Err("the observed mount uses a different platform adapter".to_string());
    }
    match (status.platform, status.phase) {
        (
            SifsMountPlatform::UnixFuse,
            sinomo_runtime::dfs::SifsMountRuntimePhase::Registered
            | sinomo_runtime::dfs::SifsMountRuntimePhase::Connected,
        ) => return Err("Unix FUSE reported a Windows-only lifecycle phase".to_string()),
        (
            SifsMountPlatform::ExternalProvider,
            sinomo_runtime::dfs::SifsMountRuntimePhase::Mounted,
        ) => return Err("external provider reported a Unix-only lifecycle phase".to_string()),
        (
            SifsMountPlatform::Unsupported,
            sinomo_runtime::dfs::SifsMountRuntimePhase::Registered
            | sinomo_runtime::dfs::SifsMountRuntimePhase::Mounted
            | sinomo_runtime::dfs::SifsMountRuntimePhase::Connected,
        ) => return Err("an unsupported platform reported an active mount".to_string()),
        _ => {}
    }
    let observation_relevant =
        config.enabled || status.phase != sinomo_runtime::dfs::SifsMountRuntimePhase::Disabled;
    if observation_relevant
        && (status.configured_mount_point != config.mount_point
            || status.root_path != config.root_path
            || status.read_only != config.read_only
            || status.sync_local_changes != config.sync_local_changes)
    {
        return Err(
            "the observed mount still describes a previous configuration; refresh after reconciliation"
                .to_string(),
        );
    }
    if status.phase == sinomo_runtime::dfs::SifsMountRuntimePhase::Connected {
        validate_connected_host_filesystem_provider_status(status)?;
    }
    Ok(())
}

fn validate_connected_host_filesystem_provider_status(
    status: &SifsMountRuntimeStatus,
) -> Result<(), String> {
    validate_connected_host_filesystem_provider_status_at(status, OffsetDateTime::now_utc())
}

pub(crate) fn validate_connected_host_filesystem_provider_status_at(
    status: &SifsMountRuntimeStatus,
    now: OffsetDateTime,
) -> Result<(), String> {
    if !status.provider_registered || !status.provider_connected {
        return Err(
            "host-filesystem provider reported connected without a registered, connected provider"
                .to_string(),
        );
    }
    if !status.worker_alive
        || !status.worker_ready
        || status.worker_health_generation == 0
        || status
            .last_worker_heartbeat_at
            .as_deref()
            .is_none_or(|observed| observed.trim().is_empty())
    {
        return Err(
            "host-filesystem provider reported connected without a live, ready writeback worker heartbeat"
                .to_string(),
        );
    }
    let heartbeat = status
        .last_worker_heartbeat_at
        .as_deref()
        .and_then(|observed| OffsetDateTime::parse(observed, &Rfc3339).ok())
        .ok_or_else(|| "host-filesystem provider heartbeat is not valid RFC3339".to_string())?;
    let heartbeat_age = now - heartbeat;
    if heartbeat_age > Duration::seconds(5) || heartbeat_age < -Duration::seconds(1) {
        return Err(format!(
            "host-filesystem provider heartbeat is outside the live freshness window ({heartbeat_age})"
        ));
    }
    if status.writeback_mode != sinomo_runtime::dfs::SifsMountWritebackMode::Active {
        return Err("host-filesystem provider writeback is not active".to_string());
    }
    if status.unregister_pending {
        return Err("a retired host-filesystem provider still requires cleanup".to_string());
    }
    Ok(())
}

/// Validate the host-effect contract before it reaches the config service.
///
/// The supervisor repeats platform validation when applying the document. The
/// UI-side check keeps malformed paths and platform-only flags from being
/// persisted through this dedicated surface in the first place.
pub fn validate_sifs_mount_config(
    config: &SifsMountConfig,
    platform: SifsMountPlatform,
) -> Result<(), String> {
    let mount_point = config.mount_point.trim();
    if mount_point.is_empty() {
        return Err("mount location is required".to_string());
    }
    if mount_point != config.mount_point {
        return Err("mount location cannot have surrounding whitespace".to_string());
    }
    if mount_point.contains('\0') {
        return Err("mount location cannot contain a NUL byte".to_string());
    }
    let home_relative = mount_point
        .strip_prefix("~/")
        .is_some_and(|relative| !relative.trim_matches('/').is_empty());
    if !home_relative && !Path::new(mount_point).is_absolute() {
        return Err("mount location must be absolute or begin with ~/".to_string());
    }
    let mount_tail = mount_point.strip_prefix("~/").unwrap_or(mount_point);
    if Path::new(mount_tail)
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err("mount location must be normalized and cannot contain . or ..".to_string());
    }

    let root_path = config.root_path.trim();
    if root_path != config.root_path {
        return Err("SIFS root cannot have surrounding whitespace".to_string());
    }
    if !root_path.starts_with('/') {
        return Err("SIFS root must be an absolute path beginning with /".to_string());
    }
    if root_path.contains('\0') || root_path.contains('\\') {
        return Err("SIFS root must use POSIX path separators".to_string());
    }
    if root_path != "/" && root_path.ends_with('/') {
        return Err("SIFS root must not end with /".to_string());
    }
    if root_path != "/"
        && root_path
            .split('/')
            .skip(1)
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err("SIFS root must be normalized and cannot contain ., .., or //".to_string());
    }

    match platform {
        SifsMountPlatform::UnixFuse => {
            if config.sync_local_changes {
                return Err(
                    "sync local changes is only available for the external Windows provider"
                        .to_string(),
                );
            }
        }
        SifsMountPlatform::ExternalProvider => {
            if config.allow_other {
                return Err("allow other users is only available for Unix FUSE".to_string());
            }
            if config.enabled && (config.read_only || !config.sync_local_changes) {
                return Err(
                    "the external Windows provider requires explicit durable writable synchronization"
                        .to_string(),
                );
            }
        }
        SifsMountPlatform::Unsupported if config.enabled => {
            return Err("SIFS host mounts are not supported on this platform".to_string());
        }
        SifsMountPlatform::Unsupported => {
            if config.allow_other || config.sync_local_changes {
                return Err("platform-specific mount options are unavailable".to_string());
            }
        }
    }
    Ok(())
}

fn validate_sifs_mount_target(config: &SifsMountConfig, data_dir: &Path) -> Result<(), String> {
    let mount_point =
        canonical_absolute_path_allow_missing(&resolve_mount_point(&config.mount_point)?)?;
    if mount_point.parent().is_none() {
        return Err("mount location cannot be a filesystem root".to_string());
    }
    let data_dir = canonical_absolute_path_allow_missing(data_dir)?;
    if mount_point.starts_with(&data_dir) || data_dir.starts_with(&mount_point) {
        return Err("mount location cannot overlap the Sinomo data directory".to_string());
    }
    if let Ok(workspace) = std::env::current_dir()
        && let Ok(workspace) = canonical_absolute_path_allow_missing(&workspace)
        && (mount_point.starts_with(&workspace) || workspace.starts_with(&mount_point))
    {
        return Err("mount location cannot overlap the current source workspace".to_string());
    }
    #[cfg(unix)]
    for reserved in [
        "/bin", "/boot", "/dev", "/etc", "/proc", "/run", "/sbin", "/sys", "/usr", "/var",
    ] {
        if mount_point == Path::new(reserved) || mount_point.starts_with(reserved) {
            return Err(format!(
                "mount location cannot be inside the system path {reserved}"
            ));
        }
    }
    Ok(())
}

fn resolve_mount_point(value: &str) -> Result<PathBuf, String> {
    let value = value.trim();
    if let Some(relative) = value.strip_prefix("~/") {
        let home = std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .map(PathBuf::from)
            .ok_or_else(|| {
                "cannot resolve ~/ because this process has no home directory".to_string()
            })?;
        return Ok(home.join(relative));
    }
    Ok(PathBuf::from(value))
}

fn canonical_absolute_path_allow_missing(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("mount location must resolve to an absolute path".to_string());
    }
    let mut candidate = path;
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(candidate) {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = candidate
                    .file_name()
                    .ok_or_else(|| format!("cannot resolve mount location {}", path.display()))?;
                missing.push(component.to_os_string());
                candidate = candidate
                    .parent()
                    .ok_or_else(|| format!("cannot resolve mount location {}", path.display()))?;
            }
            Err(error) => {
                return Err(format!(
                    "resolve mount location {}: {error}",
                    path.display()
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_validation_rejects_unsafe_or_ambiguous_paths() {
        let mut config = SifsMountConfig {
            mount_point: "relative/mount".to_string(),
            ..SifsMountConfig::default()
        };
        assert_eq!(
            validate_sifs_mount_config(&config, SifsMountPlatform::UnixFuse),
            Err("mount location must be absolute or begin with ~/".to_string())
        );

        config.mount_point = "~/sinomo".to_string();
        config.root_path = "/teams/../private".to_string();
        assert_eq!(
            validate_sifs_mount_config(&config, SifsMountPlatform::UnixFuse),
            Err("SIFS root must be normalized and cannot contain ., .., or //".to_string())
        );
    }

    #[test]
    fn mount_validation_is_platform_specific_and_fail_closed() {
        let mut config = SifsMountConfig {
            sync_local_changes: true,
            ..SifsMountConfig::default()
        };
        assert!(
            validate_sifs_mount_config(&config, SifsMountPlatform::UnixFuse)
                .expect_err("Unix must reject the Windows-only option")
                .contains("external Windows provider")
        );

        config.sync_local_changes = false;
        config.allow_other = true;
        assert!(
            validate_sifs_mount_config(&config, SifsMountPlatform::ExternalProvider)
                .expect_err("Windows must reject the FUSE-only option")
                .contains("Unix FUSE")
        );

        config.allow_other = false;
        config.enabled = true;
        config.read_only = false;
        config.sync_local_changes = false;
        assert!(
            validate_sifs_mount_config(&config, SifsMountPlatform::ExternalProvider)
                .expect_err("Windows writeback must be explicitly enabled")
                .contains("durable writable synchronization")
        );

        config.read_only = true;
        config.sync_local_changes = true;
        assert!(
            validate_sifs_mount_config(&config, SifsMountPlatform::ExternalProvider)
                .expect_err("Windows cannot combine read-only and writeback modes")
                .contains("durable writable synchronization")
        );

        config.read_only = false;
        assert_eq!(
            validate_sifs_mount_config(&config, SifsMountPlatform::ExternalProvider),
            Ok(()),
            "Windows writable sync is accepted only as an explicit pair"
        );

        config.sync_local_changes = false;
        assert!(
            validate_sifs_mount_config(&config, SifsMountPlatform::Unsupported)
                .expect_err("unsupported platforms must not enable a host mount")
                .contains("not supported")
        );
    }

    #[test]
    fn mount_target_validation_rejects_internal_and_system_locations() {
        let root = if cfg!(windows) {
            Path::new(r"C:\srv\sinomo-state")
        } else {
            Path::new("/srv/sinomo-state")
        };
        let mut config = SifsMountConfig {
            mount_point: root.join("mount").to_string_lossy().into_owned(),
            ..SifsMountConfig::default()
        };
        assert_eq!(
            validate_sifs_mount_target(&config, root),
            Err("mount location cannot overlap the Sinomo data directory".to_string())
        );

        config.mount_point = root.parent().unwrap().to_string_lossy().into_owned();
        assert_eq!(
            validate_sifs_mount_target(&config, root),
            Err("mount location cannot overlap the Sinomo data directory".to_string())
        );

        #[cfg(unix)]
        {
            config.mount_point = "/etc/sinomo".to_string();
            assert_eq!(
                validate_sifs_mount_target(&config, Path::new("/srv/sinomo-state")),
                Err("mount location cannot be inside the system path /etc".to_string())
            );
        }
    }

    #[test]
    fn local_mount_configuration_round_trips_through_the_config_service() {
        let backend = Backend::local_fallback();
        let initial = backend.sifs_mount_settings_snapshot();
        assert_eq!(initial.config_error, None);
        assert_eq!(initial.status_error, None);

        let mut config = initial.config;
        config.mount_point = if cfg!(windows) {
            r"C:\SinomoSettingsMount".to_string()
        } else {
            "/tmp/sinomo-settings-mount".to_string()
        };
        config.root_path = "/services".to_string();
        config.enabled = false;
        backend
            .set_local_sifs_mount_config(&config)
            .expect("save node-local mount configuration");

        let reloaded = backend.sifs_mount_settings_snapshot();
        assert_eq!(reloaded.config_error, None);
        assert_eq!(reloaded.config, config);
    }

    #[test]
    fn enabled_mount_never_treats_a_persisted_status_as_live() {
        let backend = Backend::local_fallback();
        let config = SifsMountConfig {
            enabled: true,
            read_only: false,
            sync_local_changes: cfg!(windows),
            mount_point: if cfg!(windows) {
                r"C:\SinomoSettingsLivenessMount".to_string()
            } else {
                "/tmp/sinomo-settings-liveness-mount".to_string()
            },
            ..SifsMountConfig::default()
        };
        backend
            .set_local_sifs_mount_config(&config)
            .expect("save enabled node-local mount configuration");

        let snapshot = backend.sifs_mount_settings_snapshot();
        assert!(
            snapshot
                .status_error
                .as_deref()
                .is_some_and(|error| error.contains("require a daemon connection")),
            "enabled mounts must require a successful live status RPC: {:?}",
            snapshot.status_error
        );
    }

    #[test]
    fn enabled_mount_status_requires_a_fresh_matching_supervisor_observation() {
        let mut config = SifsMountConfig {
            enabled: true,
            ..SifsMountConfig::default()
        };
        let mut status = SifsMountRuntimeStatus::default();
        assert!(
            validate_sifs_mount_status(&config, &status, ConfigValueSource::Default)
                .expect_err("default status must not claim an enabled mount is observed")
                .contains("no observation")
        );

        status.phase = match SifsMountPlatform::current() {
            SifsMountPlatform::ExternalProvider => {
                config.read_only = false;
                config.sync_local_changes = true;
                status.read_only = false;
                status.sync_local_changes = true;
                status.provider_registered = true;
                status.provider_connected = true;
                status.worker_alive = true;
                status.worker_ready = true;
                status.worker_health_generation = 1;
                status.last_worker_heartbeat_at = Some(
                    OffsetDateTime::now_utc()
                        .format(&Rfc3339)
                        .expect("heartbeat timestamp"),
                );
                status.writeback_mode = sinomo_runtime::dfs::SifsMountWritebackMode::Active;
                sinomo_runtime::dfs::SifsMountRuntimePhase::Connected
            }
            SifsMountPlatform::UnixFuse => sinomo_runtime::dfs::SifsMountRuntimePhase::Mounted,
            SifsMountPlatform::Unsupported => sinomo_runtime::dfs::SifsMountRuntimePhase::Starting,
        };
        status.updated_at = "2026-08-10T20:00:00Z".to_string();
        assert_eq!(
            validate_sifs_mount_status(&config, &status, ConfigValueSource::Local),
            Ok(())
        );

        status.root_path = "/previous".to_string();
        assert!(
            validate_sifs_mount_status(&config, &status, ConfigValueSource::Local)
                .expect_err("stale status must not be presented as current")
                .contains("previous configuration")
        );
    }

    #[test]
    fn connected_cloud_status_requires_a_fresh_worker_progress_heartbeat() {
        let mut status = SifsMountRuntimeStatus {
            phase: sinomo_runtime::dfs::SifsMountRuntimePhase::Connected,
            platform: SifsMountPlatform::ExternalProvider,
            provider_registered: true,
            provider_connected: true,
            worker_alive: true,
            worker_ready: true,
            worker_health_generation: 7,
            writeback_mode: sinomo_runtime::dfs::SifsMountWritebackMode::Active,
            ..SifsMountRuntimeStatus::default()
        };
        status.last_worker_heartbeat_at = Some(
            (OffsetDateTime::now_utc() - Duration::seconds(6))
                .format(&Rfc3339)
                .expect("stale heartbeat"),
        );
        assert!(
            validate_connected_host_filesystem_provider_status(&status)
                .expect_err("stale progress must not be shown as connected")
                .contains("freshness window")
        );

        status.last_worker_heartbeat_at = Some(
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .expect("fresh heartbeat"),
        );
        assert_eq!(
            validate_connected_host_filesystem_provider_status(&status),
            Ok(())
        );
    }
}
