use crate::internal::*;

pub fn sync_sandbox_workspace_roots<S, D>(
    source: &S,
    destination: &mut D,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    for path in sandbox_workspace_sync_roots() {
        sync_sandbox_tree(source, destination, &path)?;
    }
    Ok(())
}

pub fn sync_sandbox_tree<S, D>(
    source: &S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    match source.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            ensure_destination_directory(destination, path)?;
            remove_destination_entries_missing_from_source(source, destination, path)?;
            for entry in source.list(path)? {
                sync_sandbox_tree(source, destination, &entry.path)?;
            }
            Ok(())
        }
        Ok(_) => sync_sandbox_file(source, destination, path),
        Err(SandboxFsError::NotFound { .. }) => {
            clear_sandbox_path_if_exists(destination, path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub fn sync_sandbox_changed_paths<S, D>(
    source: &S,
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
    for path in paths {
        sync_sandbox_changed_path(source, destination, &path)?;
    }
    Ok(())
}

fn sync_sandbox_changed_path<S, D>(
    source: &S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    match source.stat(path) {
        Ok(metadata) if metadata.is_dir => ensure_destination_directory(destination, path),
        Ok(_) => {
            if let Some(parent) = parent_sandbox_path(path)? {
                ensure_destination_directory(destination, &parent)?;
            }
            let source_bytes = source.read(path, None)?;
            match destination.stat(path) {
                Ok(metadata) if metadata.is_dir => {
                    clear_sandbox_path_if_exists(destination, path)?;
                }
                Ok(_) | Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
            destination.write(path, &source_bytes)
        }
        Err(SandboxFsError::NotFound { .. }) => {
            clear_sandbox_path_if_exists(destination, path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn sync_sandbox_file<S, D>(
    source: &S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let source_bytes = source.read(path, None)?;
    match destination.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            clear_sandbox_path_if_exists(destination, path)?;
        }
        Ok(metadata) if metadata.len == source_bytes.len() as u64 => {
            if destination.read(path, None)? == source_bytes {
                return Ok(());
            }
        }
        Ok(_) | Err(SandboxFsError::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    destination.write(path, &source_bytes)
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
    source: &S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let source_entries = source
        .list(path)?
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
    source: &S,
    destination: &mut D,
    path: &SandboxPath,
) -> Result<(), SandboxFsError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
{
    let metadata = source.stat(path)?;
    if metadata.is_dir {
        destination.create_dir_all(path)?;
        for entry in source.list(path)? {
            copy_sandbox_tree(source, destination, &entry.path)?;
        }
        Ok(())
    } else {
        let bytes = source.read(path, None)?;
        destination.write(path, &bytes)
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
            sandbox_fs.unlink(path)
        }
        Ok(_) => sandbox_fs.unlink(path),
        Err(SandboxFsError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

fn sandbox_workspace_sync_roots() -> Vec<SandboxPath> {
    ["/root", "/home", "/tmp", "/work", "/logs", "/artifacts"]
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
