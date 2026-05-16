use crate::internal::*;

pub(super) fn reset_local_staging_instance(
    local_store: &LocalSandboxInstanceStore,
    manifest: &SandboxInstanceManifest,
) -> Result<(), SandboxStoreError> {
    let layout = local_store.layout(&manifest.sandbox_id);
    if layout.root.exists() {
        fs::remove_dir_all(&layout.root).map_err(|source| SandboxStoreError::Io {
            path: layout.root.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    local_store.create(
        manifest.sandbox_id.clone(),
        manifest.toolchain_id.clone(),
        OffsetDateTime::now_utc(),
    )?;
    local_store.save_manifest(manifest)?;
    Ok(())
}

pub(super) fn stage_sifs_mounts_to_local<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    manifest: &SandboxInstanceManifest,
    layout: &LocalSandboxInstanceLayout,
) -> Result<Vec<SandboxWasiPreopen>, SandboxCommandExecutionError> {
    let mut preopens = Vec::new();
    for (index, mount) in manifest.mounts.iter().enumerate() {
        let local_path = staged_mount_path(layout, index);
        clear_local_mount_stage(&local_path)?;
        let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
        match dfs.posix_stat(&source) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                fs::create_dir_all(&local_path).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("create mount stage {}: {source}", local_path.display()),
                })?;
                copy_sifs_directory_contents_to_local(dfs, &source, &local_path)?;
            }
            Ok(stat) => {
                return Err(SandboxFsError::Unsupported {
                    operation: format!(
                        "mount non-directory SIFS object {source} ({:?})",
                        stat.kind
                    ),
                }
                .into());
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
                if mount.mode == SandboxMountMode::ReadWrite =>
            {
                dfs.posix_mkdir_all(&source)
                    .map_err(|error| sandbox_fs_error_from_dfs(&mount.mount_path, error))?;
                fs::create_dir_all(&local_path).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("create mount stage {}: {source}", local_path.display()),
                })?;
            }
            Err(error) => return Err(sandbox_fs_error_from_dfs(&mount.mount_path, error).into()),
        }
        preopens.push(SandboxWasiPreopen {
            host_path: local_path,
            guest_path: mount.mount_path.clone(),
            mode: mount.mode.clone(),
        });
    }
    Ok(preopens)
}

mod toolchain_cache;
pub(super) use toolchain_cache::stage_toolchain_runtime_to_local;

pub(super) fn load_local_toolchain_artifacts(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .artifacts
        .keys()
        .map(|relative_path| {
            store
                .load_artifact(manifest, relative_path)
                .map(|bytes| (relative_path.clone(), bytes))
        })
        .collect()
}

pub(super) fn load_local_toolchain_commands(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

pub(super) fn load_sifs_toolchain_artifacts<T: NetworkTransport>(
    store: &SifsSandboxToolchainStore<T>,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    store.load_artifacts(manifest)
}

pub(super) fn load_sifs_toolchain_commands<T: NetworkTransport>(
    store: &SifsSandboxToolchainStore<T>,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

pub(super) fn sync_rw_sifs_mounts_from_local<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    manifest: &SandboxInstanceManifest,
    layout: &LocalSandboxInstanceLayout,
    replica_candidates: &[NodeId],
) -> Result<(), SandboxCommandExecutionError> {
    for (index, mount) in manifest.mounts.iter().enumerate() {
        if mount.mode != SandboxMountMode::ReadWrite {
            continue;
        }
        let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
        clear_sifs_directory(dfs, &source, &mount.mount_path)?;
        copy_local_directory_contents_to_sifs(
            dfs,
            &staged_mount_path(layout, index),
            &source,
            replica_candidates,
        )?;
    }
    Ok(())
}

fn staged_mount_path(layout: &LocalSandboxInstanceLayout, index: usize) -> PathBuf {
    layout.mounts.join(format!("{index:04}"))
}

fn clear_local_mount_stage(path: &Path) -> Result<(), SandboxFsError> {
    if path.exists() {
        reject_symlink_chain(path, &SandboxPath::new("/mounts").expect("static path"))?;
        fs::remove_dir_all(path).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("clear mount stage {}: {source}", path.display()),
        })?;
    }
    fs::create_dir_all(path).map_err(|source| SandboxFsError::Unavailable {
        reason: format!("create mount stage {}: {source}", path.display()),
    })
}

fn copy_sifs_directory_contents_to_local<T: NetworkTransport>(
    dfs: &NetworkedDfsService<T>,
    source: &str,
    destination: &Path,
) -> Result<(), SandboxFsError> {
    let entries = dfs
        .posix_list_dir(source)
        .map_err(|error| sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error))?;
    for entry in entries {
        copy_sifs_tree_to_local(dfs, &entry.path, &destination.join(entry.name))?;
    }
    Ok(())
}

fn copy_sifs_tree_to_local<T: NetworkTransport>(
    dfs: &NetworkedDfsService<T>,
    source: &str,
    destination: &Path,
) -> Result<(), SandboxFsError> {
    let stat = dfs
        .posix_stat(source)
        .map_err(|error| sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error))?;
    match stat.kind {
        DfsObjectKind::Directory => {
            fs::create_dir_all(destination).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("create local mount dir {}: {source}", destination.display()),
            })?;
            copy_sifs_directory_contents_to_local(dfs, source, destination)
        }
        DfsObjectKind::File | DfsObjectKind::Artifact => {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("create local mount parent {}: {source}", parent.display()),
                })?;
            }
            let bytes = dfs.posix_read_file(source).map_err(|error| {
                sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error)
            })?;
            fs::write(destination, bytes).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("write local mount file {}: {source}", destination.display()),
            })
        }
        other => Err(SandboxFsError::Unsupported {
            operation: format!("copy unsupported SIFS object kind {other:?} from mount"),
        }),
    }
}

fn clear_sifs_directory<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    path: &str,
    sandbox_path: &SandboxPath,
) -> Result<(), SandboxFsError> {
    dfs.posix_mkdir_all(path)
        .map_err(|error| sandbox_fs_error_from_dfs(sandbox_path, error))?;
    let entries = dfs
        .posix_list_dir(path)
        .map_err(|error| sandbox_fs_error_from_dfs(sandbox_path, error))?;
    for entry in entries {
        remove_sifs_tree_if_exists(dfs, &entry.path, sandbox_path)?;
    }
    Ok(())
}

fn remove_sifs_tree_if_exists<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    path: &str,
    sandbox_path: &SandboxPath,
) -> Result<(), SandboxFsError> {
    let stat = match dfs.posix_stat(path) {
        Ok(stat) => stat,
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
            return Ok(());
        }
        Err(error) => return Err(sandbox_fs_error_from_dfs(sandbox_path, error)),
    };
    match stat.kind {
        DfsObjectKind::Directory => {
            let entries = dfs
                .posix_list_dir(path)
                .map_err(|error| sandbox_fs_error_from_dfs(sandbox_path, error))?;
            for entry in entries {
                remove_sifs_tree_if_exists(dfs, &entry.path, sandbox_path)?;
            }
            dfs.posix_rmdir(path)
                .map(|_| ())
                .map_err(|error| sandbox_fs_error_from_dfs(sandbox_path, error))
        }
        DfsObjectKind::File | DfsObjectKind::Artifact => dfs
            .posix_unlink(path)
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(sandbox_path, error)),
        other => Err(SandboxFsError::Unsupported {
            operation: format!("remove unsupported SIFS object kind {other:?} from mount"),
        }),
    }
}

fn copy_local_directory_contents_to_sifs<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    source: &Path,
    destination: &str,
    replica_candidates: &[NodeId],
) -> Result<(), SandboxFsError> {
    fs::create_dir_all(source).map_err(|error| SandboxFsError::Unavailable {
        reason: format!("create local mount source {}: {error}", source.display()),
    })?;
    for entry in fs::read_dir(source).map_err(|error| SandboxFsError::Unavailable {
        reason: format!("read local mount dir {}: {error}", source.display()),
    })? {
        let entry = entry.map_err(|error| SandboxFsError::Unavailable {
            reason: format!("read local mount dir {}: {error}", source.display()),
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        copy_local_tree_to_sifs(
            dfs,
            &entry.path(),
            &format!("{}/{}", destination.trim_end_matches('/'), name),
            replica_candidates,
        )?;
    }
    Ok(())
}

fn copy_local_tree_to_sifs<T: NetworkTransport>(
    dfs: &mut NetworkedDfsService<T>,
    source: &Path,
    destination: &str,
    replica_candidates: &[NodeId],
) -> Result<(), SandboxFsError> {
    let metadata = fs::symlink_metadata(source).map_err(|error| SandboxFsError::Unavailable {
        reason: format!("stat local mount path {}: {error}", source.display()),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
            path: source.display().to_string(),
        }));
    }
    if metadata.is_dir() {
        dfs.posix_mkdir_all(destination).map_err(|error| {
            sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error)
        })?;
        copy_local_directory_contents_to_sifs(dfs, source, destination, replica_candidates)
    } else if metadata.is_file() {
        let bytes = fs::read(source).map_err(|error| SandboxFsError::Unavailable {
            reason: format!("read local mount file {}: {error}", source.display()),
        })?;
        dfs.posix_write_file(
            destination,
            &bytes,
            DfsWriteOptions::replicated_eventual(sandbox_dfs_write_candidates(
                dfs.node_id(),
                replica_candidates,
            )),
        )
        .map(|_| ())
        .map_err(|error| sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error))
    } else {
        Err(SandboxFsError::Unsupported {
            operation: format!("copy special local mount path {}", source.display()),
        })
    }
}

fn sandbox_dfs_write_candidates(node_id: &NodeId, replica_candidates: &[NodeId]) -> Vec<NodeId> {
    let mut candidates = replica_candidates.to_vec();
    if !candidates.contains(node_id) {
        candidates.push(node_id.clone());
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

#[cfg(test)]
mod tests;
