use std::time::Duration;

use sinomo_runtime::api::{DaemonPaths, DaemonProbeStatus, DaemonStatus, probe_daemon_status};
use sinomo_runtime::local::SinomoDataDir;

use crate::{
    OwnedDaemonHandle, StartAppDaemonOutcome, daemon_protocol_is_compatible,
    incompatible_daemon_protocol_detail,
};

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);

pub enum LocalDaemonConnection {
    Attached(DaemonStatus),
    Owned {
        status: DaemonStatus,
        handle: OwnedDaemonHandle,
    },
}

impl LocalDaemonConnection {
    pub fn status(&self) -> &DaemonStatus {
        match self {
            Self::Attached(status) | Self::Owned { status, .. } => status,
        }
    }

    pub fn into_parts(self) -> (DaemonStatus, Option<OwnedDaemonHandle>) {
        match self {
            Self::Attached(status) => (status, None),
            Self::Owned { status, handle } => (status, Some(handle)),
        }
    }
}

/// Attach to the compatible daemon already owning `data_dir`, or start the verified sibling
/// helper and retain exact lifecycle ownership. A lock race is resolved by attaching to its
/// winner; an incompatible daemon is never stopped or replaced.
pub fn connect_or_start_local_daemon(
    data_dir: &SinomoDataDir,
    progress: &impl Fn(&str),
) -> Result<LocalDaemonConnection, String> {
    connect_or_start_local_daemon_with_binary(data_dir, None, progress)
}

pub fn connect_or_start_local_daemon_with_binary(
    data_dir: &SinomoDataDir,
    binary: Option<&std::path::Path>,
    progress: &impl Fn(&str),
) -> Result<LocalDaemonConnection, String> {
    let paths = DaemonPaths::new(data_dir);
    progress("Looking for a daemon…");
    match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => compatible_attached(status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            progress("Starting a local daemon…");
            match crate::daemon_process::start_app_owned_daemon_with_binary(
                &paths, progress, binary,
            )? {
                StartAppDaemonOutcome::Started(status, handle) => {
                    Ok(LocalDaemonConnection::Owned { status, handle })
                }
                StartAppDaemonOutcome::Attached(status) => compatible_attached(status),
            }
        }
    }
}

fn compatible_attached(status: DaemonStatus) -> Result<LocalDaemonConnection, String> {
    if daemon_protocol_is_compatible(&status) {
        Ok(LocalDaemonConnection::Attached(status))
    } else {
        Err(incompatible_daemon_protocol_detail(&status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::api::DaemonRunOptions;

    #[test]
    fn attached_daemon_compatibility_is_protocol_not_package_version() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join("sinomo-client-connection"));
        let paths = DaemonPaths::new(&data_dir);
        let mut status = DaemonStatus::new(&DaemonRunOptions::default(), &paths, None);
        status.version = "999.0.0".to_string();
        assert!(matches!(
            compatible_attached(status.clone()),
            Ok(LocalDaemonConnection::Attached(_))
        ));

        status.api_protocol_version = 0;
        assert!(compatible_attached(status).is_err());
    }
}
