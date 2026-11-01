//! Canonical private HOME preparation, independent of a host mount.
use sinomo_runtime::dfs::{
    AsyncDfsPeerTransport, DaemonDfsPeerTransport, DfsPeerTransport, DfsPosixCreateAttrs,
    MountedSifsService, open_mounted_sifs_service_with_peer_transports_and_default_control_plane,
};
use sinomo_runtime::local::SinomoDataDir;
use std::sync::Arc;

pub fn prepare_agent_home_sifs(
    data_dir: &SinomoDataDir,
    agent_key: &str,
) -> Result<String, String> {
    let path = super::agent_home_sifs_path_for_id(agent_key);
    ensure_private_home_path(data_dir, &path)?;
    Ok(path)
}

pub(crate) fn ensure_private_home_path(
    data_dir: &SinomoDataDir,
    path: &str,
) -> Result<sinomo_runtime::core::Id, String> {
    with_home_requester(data_dir, |service, attrs| {
        service
            .posix_ensure_directory_beneath_no_links_with_attrs(path, attrs)
            .map(drop)
    })
}

pub(crate) fn inspect_private_home_path(
    data_dir: &SinomoDataDir,
    path: &str,
) -> Result<sinomo_runtime::core::Id, String> {
    with_home_requester(data_dir, |service, attrs| {
        service
            .posix_inspect_directory_beneath_no_links_with_attrs(path, attrs)
            .map(drop)
    })
}

fn with_home_requester(
    data_dir: &SinomoDataDir,
    operation: impl FnOnce(
        &mut MountedSifsService,
        DfsPosixCreateAttrs,
    ) -> Result<(), sinomo_runtime::dfs::DfsPosixError>,
) -> Result<sinomo_runtime::core::Id, String> {
    let peer =
        Arc::new(DaemonDfsPeerTransport::deferred(data_dir.clone()).map_err(|e| e.to_string())?);
    let cluster = peer
        .captured_cluster_id()
        .cloned()
        .ok_or_else(|| "private HOME requester has no captured cluster namespace".to_string())?;
    let sync: Arc<dyn DfsPeerTransport> = peer.clone();
    let asynchronous: Arc<dyn AsyncDfsPeerTransport> = peer;
    let mut opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
        data_dir.clone(),
        sync,
        asynchronous,
    )
    .map_err(|e| e.to_string())?;
    // These accessors only read the current process's effective credentials
    // and have no preconditions.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    operation(
        &mut opened.service,
        DfsPosixCreateAttrs::new(0o700, uid, gid),
    )
    .map_err(|e| e.to_string())?;
    Ok(cluster)
}
