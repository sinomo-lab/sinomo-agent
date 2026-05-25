use crate::internal::*;

pub fn sync_sandbox_workspace_roots<S, D>(
    source: &mut S,
    destination: &mut D,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let mut context = SandboxSyncContext::default();
    for path in sandbox_workspace_sync_roots() {
        sync_sandbox_tree_inner(source, destination, &path, &mut context)?;
    }
    Ok(())
}

#[derive(Default)]
struct SandboxSyncContext {
    hardlink_targets: BTreeMap<String, SandboxPath>,
}

pub fn sync_sandbox_tree<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let mut context = SandboxSyncContext::default();
    sync_sandbox_tree_inner(source, destination, path, &mut context)
}

fn sync_sandbox_tree_inner<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    context: &mut SandboxSyncContext,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    match source.stat_mut(path) {
        Ok(metadata) if metadata.is_dir => {
            ensure_destination_directory(destination, path)?;
            remove_destination_entries_missing_from_source(source, destination, path)?;
            for entry in source.list_unchecked_mut(path)? {
                sync_sandbox_tree_inner(source, destination, &entry.path, context)?;
            }
            sync_sandbox_metadata(destination, path, &metadata)?;
            Ok(())
        }
        Ok(metadata) if metadata.is_symlink => {
            sync_sandbox_symlink(source, destination, path, context)
        }
        Ok(metadata) => sync_sandbox_file(source, destination, path, &metadata, context),
        Err(SandboxFsError::NotFound { .. }) => {
            clear_sandbox_path_if_exists(destination, path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub fn sync_sandbox_changed_paths<S, D>(
    source: &mut S,
    destination: &mut D,
    paths: &[SandboxPath],
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    paths.sort_by_key(|path| path.as_str().matches('/').count());
    let mut context = SandboxSyncContext::default();
    for path in paths {
        sync_sandbox_changed_path(source, destination, &path, &mut context)?;
    }
    Ok(())
}

fn sync_sandbox_changed_path<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    context: &mut SandboxSyncContext,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    match source.stat_mut(path) {
        Ok(metadata) if metadata.is_dir => {
            ensure_destination_directory(destination, path)?;
            sync_sandbox_metadata(destination, path, &metadata)
        }
        Ok(metadata) if metadata.is_symlink => {
            if let Some(parent) = parent_sandbox_path(path)? {
                ensure_destination_directory(destination, &parent)?;
            }
            sync_sandbox_symlink(source, destination, path, context)
        }
        Ok(_) => {
            if let Some(parent) = parent_sandbox_path(path)? {
                ensure_destination_directory(destination, &parent)?;
            }
            let metadata = source.stat_mut(path)?;
            if sync_sandbox_hardlink_alias(destination, path, &metadata, context)? {
                return Ok(());
            }
            let source_bytes = source.read_unchecked_mut(path, None)?;
            match destination.stat(path) {
                Ok(metadata) if metadata.is_dir => {
                    clear_sandbox_path_if_exists(destination, path)?;
                }
                Ok(_) | Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
            destination.write(path, &source_bytes)?;
            sync_sandbox_metadata(destination, path, &metadata)?;
            remember_hardlink_destination(path, &metadata, context);
            Ok(())
        }
        Err(SandboxFsError::NotFound { .. }) => {
            clear_sandbox_path_if_exists(destination, path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn sync_sandbox_file<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
    context: &mut SandboxSyncContext,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    if sync_sandbox_hardlink_alias(destination, path, metadata, context)? {
        return Ok(());
    }
    let source_bytes = source.read_unchecked_mut(path, None)?;
    match destination.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            clear_sandbox_path_if_exists(destination, path)?;
        }
        Ok(destination_metadata) if destination_metadata.len == source_bytes.len() as u64 => {
            if destination.read(path, None)? == source_bytes {
                return sync_sandbox_metadata(destination, path, metadata);
            }
        }
        Ok(_) | Err(SandboxFsError::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    destination.write(path, &source_bytes)?;
    sync_sandbox_metadata(destination, path, metadata)?;
    remember_hardlink_destination(path, metadata, context);
    Ok(())
}

fn sync_sandbox_symlink<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    context: &mut SandboxSyncContext,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let target = source.readlink_mut(path)?;
    let metadata = source.stat_mut(path)?;
    if sync_sandbox_hardlink_alias(destination, path, &metadata, context)? {
        return Ok(());
    }
    match destination.stat(path) {
        Ok(destination_metadata) if destination_metadata.is_symlink => {
            if destination.readlink(path)? == target {
                return sync_sandbox_symlink_metadata(destination, path, &metadata);
            }
            destination.unlink(path)?;
        }
        Ok(destination_metadata) if destination_metadata.is_dir => {
            clear_sandbox_path_if_exists(destination, path)?;
        }
        Ok(_) => destination.unlink(path)?,
        Err(SandboxFsError::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    destination.symlink(&target, path)?;
    sync_sandbox_symlink_metadata(destination, path, &metadata)?;
    remember_hardlink_destination(path, &metadata, context);
    Ok(())
}

fn sync_sandbox_hardlink_alias<D>(
    destination: &mut D,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
    context: &mut SandboxSyncContext,
) -> Result<bool, SandboxFsError>
where
    D: SandboxFileSystem,
{
    let Some(key) = hardlink_key(metadata) else {
        return Ok(false);
    };
    let Some(existing) = context.hardlink_targets.get(&key).cloned() else {
        return Ok(false);
    };
    if &existing == path {
        return Ok(false);
    }
    match destination.stat(path) {
        Ok(current)
            if current.inode == metadata.inode && current.link_count == metadata.link_count =>
        {
            return Ok(true);
        }
        Ok(_) => clear_sandbox_path_if_exists(destination, path)?,
        Err(SandboxFsError::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    destination.hard_link(&existing, path)?;
    if metadata.is_symlink {
        sync_sandbox_symlink_metadata(destination, path, metadata)?;
    } else {
        sync_sandbox_metadata(destination, path, metadata)?;
    }
    Ok(true)
}

fn remember_hardlink_destination(
    path: &SandboxPath,
    metadata: &SandboxMetadata,
    context: &mut SandboxSyncContext,
) {
    if let Some(key) = hardlink_key(metadata) {
        context
            .hardlink_targets
            .entry(key)
            .or_insert_with(|| path.clone());
    }
}

fn hardlink_key(metadata: &SandboxMetadata) -> Option<String> {
    if metadata.link_count <= 1 {
        return None;
    }
    metadata
        .object_identity
        .clone()
        .or_else(|| (metadata.inode != 0).then(|| format!("inode:{}", metadata.inode)))
}

fn sync_sandbox_metadata<D>(
    destination: &mut D,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
) -> Result<(), SandboxFsError>
where
    D: SandboxFileSystem,
{
    destination.set_mode(path, metadata.mode)?;
    destination.set_owner(path, metadata.uid, metadata.gid)?;
    destination.set_times(
        path,
        Some(metadata.accessed_at_unix_nanos),
        Some(metadata.modified_at_unix_nanos),
    )
}

fn sync_sandbox_symlink_metadata<D>(
    destination: &mut D,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
) -> Result<(), SandboxFsError>
where
    D: SandboxFileSystem,
{
    destination.set_symlink_times(
        path,
        Some(metadata.accessed_at_unix_nanos),
        Some(metadata.modified_at_unix_nanos),
    )
}

fn ensure_destination_directory<D>(
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    D: SandboxFileSystem,
{
    match destination.stat(path) {
        Ok(metadata) if metadata.is_dir => destination.create_dir_all(path),
        Ok(_) => {
            destination.unlink(path)?;
            destination.create_dir_all(path)
        }
        Err(SandboxFsError::NotFound { .. }) => destination.create_dir_all(path),
        Err(error) => Err(error),
    }
}

fn remove_destination_entries_missing_from_source<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let source_entries = source
        .list_unchecked_mut(path)?
        .into_iter()
        .map(|entry| entry.path)
        .collect::<BTreeSet<_>>();
    for entry in destination.list(path)? {
        if !source_entries.contains(&entry.path) {
            clear_sandbox_path_if_exists(destination, &entry.path)?;
        }
    }
    Ok(())
}

pub fn copy_sandbox_tree<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let mut context = SandboxSyncContext::default();
    copy_sandbox_tree_inner(source, destination, path, &mut context)
}

fn copy_sandbox_tree_inner<S, D>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    context: &mut SandboxSyncContext,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    match source.stat_mut(path)? {
        metadata if metadata.is_dir => {
            destination.create_dir_all(path)?;
            for entry in source.list_unchecked_mut(path)? {
                copy_sandbox_tree_inner(source, destination, &entry.path, context)?;
            }
            sync_sandbox_metadata(destination, path, &metadata)
        }
        metadata if metadata.is_symlink => sync_sandbox_symlink(source, destination, path, context),
        metadata => {
            if sync_sandbox_hardlink_alias(destination, path, &metadata, context)? {
                return Ok(());
            }
            let bytes = source.read_unchecked_mut(path, None)?;
            destination.write(path, &bytes)?;
            sync_sandbox_metadata(destination, path, &metadata)?;
            remember_hardlink_destination(path, &metadata, context);
            Ok(())
        }
    }
}

pub fn clear_sandbox_directory<F>(
    sandbox_fs: &mut F,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    F: SandboxFileSystem,
{
    match sandbox_fs.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            for entry in sandbox_fs.list(path)? {
                clear_sandbox_path_if_exists(sandbox_fs, &entry.path)?;
            }
            Ok(())
        }
        Ok(_) => Err(SandboxFsError::ReadOnly { path: path.clone() }),
        Err(SandboxFsError::NotFound { .. }) => {
            sandbox_fs.create_dir_all(path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn clear_sandbox_path_if_exists<F>(
    sandbox_fs: &mut F,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    F: SandboxFileSystem,
{
    match sandbox_fs.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            clear_sandbox_directory(sandbox_fs, path)?;
            sandbox_fs.remove_dir(path)
        }
        Ok(_) => sandbox_fs.unlink(path),
        Err(SandboxFsError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

fn sandbox_workspace_sync_roots() -> Vec<SandboxPath> {
    [
        "/root",
        "/home",
        "/tmp",
        "/work",
        "/var",
        "/logs",
        "/artifacts",
    ]
    .into_iter()
    .map(|path| SandboxPath::new(path).expect("static sandbox path"))
    .collect()
}

fn parent_sandbox_path(path: &SandboxPath) -> Result<Option<SandboxPath>, SandboxFsError> {
    let value = path.as_str().trim_end_matches('/');
    if value.is_empty() || value == "/" {
        return Ok(None);
    }
    let Some((parent, _name)) = value.rsplit_once('/') else {
        return Ok(Some(SandboxPath::new("/").expect("static root path")));
    };
    let parent = if parent.is_empty() { "/" } else { parent };
    SandboxPath::new(parent)
        .map(Some)
        .map_err(|source| SandboxFsError::Unavailable {
            reason: format!("derive parent path for {path}: {source}"),
        })
}
