//! Process locks and daemon-side re-exports of the status owner APIs.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use fs2::FileExt;

use crate::{DaemonClusterStatus, DaemonError, DaemonPaths, DaemonRunOptions, DaemonStatus};

pub use sinomo_api::{
    DaemonClusterProcess, DaemonClusterRegistration, DaemonEndpointAdvertisementConfig,
    DaemonManualEndpointConfig, current_shadow_daemon_options,
    daemon_advertised_endpoints_for_state, daemon_advertised_endpoints_for_state_async,
    daemon_advertised_endpoints_for_state_with_manual_config,
    daemon_advertised_endpoints_for_state_with_manual_config_async, daemon_cluster_registration,
    daemon_cluster_status, daemon_endpoint_advertisement_config_file,
    invite_introducers_from_daemon_status, read_daemon_endpoint_advertisement_config,
};
pub(crate) use sinomo_api::{
    current_daemon_cluster_options, current_shadow_daemon_options_on_runtime,
    daemon_cluster_options_from_status, daemon_cluster_process_from_paths,
    daemon_cluster_status_matches, normalize_manual_daemon_tcp_endpoint_address,
};

#[cfg(test)]
pub(crate) fn ensure_daemon_cluster_members(
    state: &mut sinomo_core::ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: time::OffsetDateTime,
) -> bool {
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = sinomo_core::node::shadow_daemon_node_id(&state.local_node_id);
    let lan_ips = sinomo_core::network::discover_default_lan_ip_candidates();
    let route_ips =
        sinomo_core::network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state(
            state,
        );
    let main_endpoints = sinomo_api::daemon_advertised_endpoints_for_state(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
    );
    let shadow_endpoints = sinomo_api::daemon_advertised_endpoints_for_state(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
    );
    sinomo_core::cluster::ensure_daemon_cluster_members(
        state,
        main_endpoints,
        shadow_endpoints,
        timestamp,
    )
}

#[derive(Debug)]
pub struct DaemonLock {
    file: File,
    paths: DaemonPaths,
}

impl DaemonLock {
    pub fn acquire(
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        cluster: Option<DaemonClusterStatus>,
    ) -> Result<Self, DaemonError> {
        paths.ensure()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&paths.lock)
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        FileExt::try_lock_exclusive(&file).map_err(|source| DaemonError::AlreadyRunning {
            lock_file: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;

        let status = DaemonStatus::new(options, paths, cluster);
        sinomo_api::write_daemon_status(paths, &status)?;
        file.set_len(0).map_err(|source| DaemonError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        (&file)
            .seek(SeekFrom::Start(0))
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        let status_bytes = sinomo_core::network::SinomoObjectCodec::default().encode(&status)?;
        (&file)
            .write_all(&status_bytes)
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file).flush().map_err(|source| DaemonError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self {
            file,
            paths: paths.clone(),
        })
    }
}

#[derive(Debug)]
pub struct DaemonDataOwnerLock {
    file: File,
}

impl DaemonDataOwnerLock {
    pub fn acquire(paths: &DaemonPaths) -> Result<Self, DaemonError> {
        paths.ensure()?;
        let path = paths.data_owner_lock.clone();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        FileExt::try_lock_exclusive(&file).map_err(|source| DaemonError::AlreadyRunning {
            lock_file: path.display().to_string(),
            detail: format!(
                "main daemon data directory is already owned by another process: {source}"
            ),
        })?;
        file.set_len(0).map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        (&file)
            .seek(SeekFrom::Start(0))
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file)
            .write_all(format!("pid={}\n", std::process::id()).as_bytes())
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file).flush().map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self { file })
    }
}

impl Drop for DaemonDataOwnerLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        // Keep the lock inode stable. Unlinking after unlock races a new owner
        // which can acquire the old inode before removal, allowing a third
        // process to create and lock a different inode at the same path.
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.paths.status);
        let _ = FileExt::unlock(&self.file);
        let _ = fs::remove_file(&self.paths.lock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_data_owner_lock_excludes_second_owner() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-data-owner-lock");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        let owner = DaemonDataOwnerLock::acquire(&paths).expect("owner lock");
        assert!(DaemonDataOwnerLock::acquire(&paths).is_err());
        drop(owner);
        DaemonDataOwnerLock::acquire(&paths).expect("lock released");
    }
}
