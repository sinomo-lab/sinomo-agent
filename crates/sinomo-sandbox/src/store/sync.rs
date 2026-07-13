use crate::internal::*;

/// Perform an explicitly requested one-shot workspace copy for import, export,
/// or migration. This is not the SIFS synchronization path and command
/// execution must never call it; live DFS/VFS mounts provide synchronization.

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

const SANDBOX_SYNC_COPY_CHUNK_BYTES: usize = 256 * 1024;

/// Perform the explicit one-shot copy while allowing a caller to stop between
/// bounded filesystem operations. The callback is a cancellation progress
/// checkpoint, not a sandbox work checkpoint. A file is copied in bounded
/// ranges so a large remote object cannot hide cancellation behind one
/// whole-file transfer.
pub fn sync_sandbox_workspace_roots_with_checkpoints<S, D, F>(
    source: &mut S,
    destination: &mut D,
    mut checkpoint: F,
) -> Result<(), SandboxCommandExecutionError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
    F: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    let mut context = SandboxSyncContext::default();
    for path in sandbox_workspace_sync_roots() {
        checkpoint()?;
        sync_sandbox_tree_with_checkpoints(
            source,
            destination,
            &path,
            &mut context,
            &mut checkpoint,
        )?;
        checkpoint()?;
    }
    Ok(())
}

fn sync_sandbox_tree_with_checkpoints<S, D, F>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    context: &mut SandboxSyncContext,
    checkpoint: &mut F,
) -> Result<(), SandboxCommandExecutionError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
    F: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    checkpoint()?;
    let result = match source.stat_mut(path) {
        Ok(metadata) if metadata.is_dir => {
            ensure_destination_directory(destination, path)?;
            remove_destination_entries_missing_from_source_with_checkpoints(
                source,
                destination,
                path,
                checkpoint,
            )?;
            checkpoint()?;
            for entry in source.list_unchecked_mut(path)? {
                checkpoint()?;
                sync_sandbox_tree_with_checkpoints(
                    source,
                    destination,
                    &entry.path,
                    context,
                    checkpoint,
                )?;
            }
            sync_sandbox_metadata(destination, path, &metadata)?;
            Ok(())
        }
        Ok(metadata) if metadata.is_symlink => {
            sync_sandbox_symlink(source, destination, path, context).map_err(Into::into)
        }
        Ok(metadata) => sync_sandbox_file_with_checkpoints(
            source,
            destination,
            path,
            &metadata,
            context,
            checkpoint,
        ),
        Err(SandboxFsError::NotFound { .. }) => {
            clear_sandbox_path_if_exists_with_checkpoints(destination, path, checkpoint)
        }
        Err(error) => Err(error.into()),
    };
    checkpoint()?;
    result
}

fn sync_sandbox_file_with_checkpoints<S, D, F>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    metadata: &SandboxMetadata,
    context: &mut SandboxSyncContext,
    checkpoint: &mut F,
) -> Result<(), SandboxCommandExecutionError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
    F: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    if sync_sandbox_hardlink_alias(destination, path, metadata, context)? {
        return Ok(());
    }

    let destination_metadata = match destination.stat(path) {
        Ok(destination_metadata) if destination_metadata.is_dir => {
            clear_sandbox_path_if_exists_with_checkpoints(destination, path, checkpoint)?;
            None
        }
        Ok(destination_metadata) => Some(destination_metadata),
        Err(SandboxFsError::NotFound { .. }) => None,
        Err(error) => return Err(error.into()),
    };
    let mut offset = 0_u64;
    let mut source_content = Vec::new();
    while offset < metadata.len {
        checkpoint()?;
        let end = offset
            .saturating_add(SANDBOX_SYNC_COPY_CHUNK_BYTES as u64)
            .min(metadata.len);
        let range = Some(ByteRange {
            start: offset,
            end: Some(end),
        });
        let source_bytes = source.read_unchecked_mut(path, range)?;
        if source_bytes.is_empty() {
            return Err(SandboxFsError::Unavailable {
                reason: format!("short ranged read while synchronizing {path} at {offset}"),
            }
            .into());
        }
        source_content.extend_from_slice(&source_bytes);
        offset = offset.saturating_add(source_bytes.len() as u64);
        checkpoint()?;
    }
    let actual_hash = format!("blake3:{}", blake3::hash(&source_content).to_hex());
    if let Some(expected_hash) = metadata.content_hash.as_deref() {
        if actual_hash != expected_hash {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "assembled ranged read hash mismatch while synchronizing {path}: expected {expected_hash}, got {actual_hash}"
                ),
            }
            .into());
        }
    }
    // Destination ranged reads are not authoritative: a stale or adversarial
    // replica could forge matching ranges and suppress copy-back. Only a
    // destination's content-addressed full-file hash may prove that the bytes
    // already match the source assembled above. Backends without such a hash
    // are conservatively rewritten.
    let rewrite = destination_metadata.is_none_or(|destination_metadata| {
        destination_metadata.len != metadata.len
            || destination_metadata.content_hash.as_deref() != Some(actual_hash.as_str())
    });
    if rewrite {
        destination.write(path, &source_content)?;
    }
    sync_sandbox_metadata(destination, path, metadata)?;
    remember_hardlink_destination(path, metadata, context);
    Ok(())
}

fn remove_destination_entries_missing_from_source_with_checkpoints<S, D, F>(
    source: &mut S,
    destination: &mut D,
    path: &SandboxPath,
    checkpoint: &mut F,
) -> Result<(), SandboxCommandExecutionError>
where
    S: SandboxFileSystem,
    D: SandboxFileSystem,
    F: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    checkpoint()?;
    let source_entries = source
        .list_unchecked_mut(path)?
        .into_iter()
        .map(|entry| entry.path)
        .collect::<BTreeSet<_>>();
    for entry in destination.list(path)? {
        checkpoint()?;
        if !source_entries.contains(&entry.path) {
            clear_sandbox_path_if_exists_with_checkpoints(destination, &entry.path, checkpoint)?;
        }
    }
    checkpoint()?;
    Ok(())
}

fn clear_sandbox_path_if_exists_with_checkpoints<Fs, C>(
    sandbox_fs: &mut Fs,
    path: &SandboxPath,
    checkpoint: &mut C,
) -> Result<(), SandboxCommandExecutionError>
where
    Fs: SandboxFileSystem,
    C: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    checkpoint()?;
    match sandbox_fs.stat(path) {
        Ok(metadata) if metadata.is_dir => {
            for entry in sandbox_fs.list(path)? {
                checkpoint()?;
                clear_sandbox_path_if_exists_with_checkpoints(sandbox_fs, &entry.path, checkpoint)?;
            }
            sandbox_fs.remove_dir(path)?;
        }
        Ok(_) => sandbox_fs.unlink(path)?,
        Err(SandboxFsError::NotFound { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    checkpoint()?;
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

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use std::cell::Cell;

    struct ForgedReadDestination {
        inner: LocalSandboxFileSystem,
        forged: Vec<u8>,
        read_calls: Cell<usize>,
    }

    impl SandboxFileSystem for ForgedReadDestination {
        fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
            self.inner.stat(path)
        }

        fn read(
            &self,
            _path: &SandboxPath,
            range: Option<ByteRange>,
        ) -> Result<Vec<u8>, SandboxFsError> {
            self.read_calls.set(self.read_calls.get().saturating_add(1));
            let start = range.as_ref().map_or(0, |range| range.start as usize);
            let end = range
                .and_then(|range| range.end)
                .map_or(self.forged.len(), |end| end as usize)
                .min(self.forged.len());
            Ok(self.forged[start.min(end)..end].to_vec())
        }

        fn create_dir_all(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
            self.inner.create_dir_all(path)
        }

        fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
            self.inner.write(path, bytes)
        }

        fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
            self.inner.list(path)
        }

        fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError> {
            self.inner.rename(from, to)
        }

        fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
            self.inner.unlink(path)
        }

        fn set_times(
            &mut self,
            path: &SandboxPath,
            accessed_at_unix_nanos: Option<u64>,
            modified_at_unix_nanos: Option<u64>,
        ) -> Result<(), SandboxFsError> {
            self.inner
                .set_times(path, accessed_at_unix_nanos, modified_at_unix_nanos)
        }

        fn set_mode(&mut self, path: &SandboxPath, mode: u32) -> Result<(), SandboxFsError> {
            self.inner.set_mode(path, mode)
        }

        fn set_owner(
            &mut self,
            path: &SandboxPath,
            uid: u32,
            gid: u32,
        ) -> Result<(), SandboxFsError> {
            self.inner.set_owner(path, uid, gid)
        }

        fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError> {
            self.inner.checkpoint()
        }
    }

    fn local_filesystem(
        root: &Path,
        name: &str,
    ) -> (LocalSandboxInstanceStore, LocalSandboxFileSystem) {
        let store = LocalSandboxInstanceStore::new(root.join(name));
        let sandbox_id = SandboxId::from_legacy_named(name).expect("sandbox id");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("sync_test_toolchain").expect("toolchain id");
        let (_manifest, layout) = store
            .create(sandbox_id, toolchain_id, OffsetDateTime::UNIX_EPOCH)
            .expect("create local sandbox");
        let filesystem = LocalSandboxFileSystem::new(layout);
        (store, filesystem)
    }

    #[test]
    fn recursive_sync_observes_cancellation_inside_a_deep_tree() {
        let root = sinomo_testdir::TestDir::new("sandbox-recursive-sync-cancel");
        let (_source_store, mut source) = local_filesystem(&root, "recursive_sync_source");
        let (_destination_store, mut destination) =
            local_filesystem(&root, "recursive_sync_destination");
        let directory = SandboxPath::new("/work/a/b/c/d").expect("directory");
        let file = SandboxPath::new("/work/a/b/c/d/value.bin").expect("file");
        source
            .create_dir_all(&directory)
            .expect("create source tree");
        source.write(&file, b"deep-value").expect("write source");

        let mut context = SandboxSyncContext::default();
        let mut checkpoints = 0_u64;
        let error = sync_sandbox_tree_with_checkpoints(
            &mut source,
            &mut destination,
            &SandboxPath::new("/work").expect("work root"),
            &mut context,
            &mut || {
                checkpoints += 1;
                if checkpoints >= 22 {
                    Err(SandboxWasiError::Cancelled.into())
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("deep sync should observe cancellation");

        assert!(error.to_string().contains("cancelled"));
        assert!(checkpoints >= 22);
        assert!(matches!(
            destination.stat(&file),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn large_file_sync_observes_cancellation_between_bounded_ranges() {
        let root = sinomo_testdir::TestDir::new("sandbox-large-sync-cancel");
        let (_source_store, mut source) = local_filesystem(&root, "large_sync_source");
        let (_destination_store, mut destination) =
            local_filesystem(&root, "large_sync_destination");
        let file = SandboxPath::new("/work/large.bin").expect("file");
        let bytes = vec![0x5a; SANDBOX_SYNC_COPY_CHUNK_BYTES * 3];
        source.write(&file, &bytes).expect("write source");
        let metadata = source.stat(&file).expect("source metadata");
        let mut context = SandboxSyncContext::default();
        let mut checkpoints = 0_u64;

        let error = sync_sandbox_file_with_checkpoints(
            &mut source,
            &mut destination,
            &file,
            &metadata,
            &mut context,
            &mut || {
                checkpoints += 1;
                if checkpoints >= 3 {
                    Err(SandboxWasiError::Cancelled.into())
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("large sync should observe cancellation");

        assert!(error.to_string().contains("cancelled"));
        assert!(matches!(
            destination.stat(&file),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn ranged_sync_rejects_corrupt_middle_chunk_against_authoritative_hash() {
        let root = sinomo_testdir::TestDir::new("sandbox-large-sync-hash");
        let (_source_store, mut source) = local_filesystem(&root, "large_hash_source");
        let (_destination_store, mut destination) =
            local_filesystem(&root, "large_hash_destination");
        let file = SandboxPath::new("/work/large.bin").expect("file");
        let authoritative = vec![0x3c; SANDBOX_SYNC_COPY_CHUNK_BYTES * 3];
        let expected_hash = format!("blake3:{}", blake3::hash(&authoritative).to_hex());
        let mut corrupt = authoritative;
        corrupt[SANDBOX_SYNC_COPY_CHUNK_BYTES + 17] ^= 0xff;
        source.write(&file, &corrupt).expect("write corrupt source");
        let mut metadata = source.stat(&file).expect("source metadata");
        metadata.content_hash = Some(expected_hash);
        let mut context = SandboxSyncContext::default();

        let error = sync_sandbox_file_with_checkpoints(
            &mut source,
            &mut destination,
            &file,
            &metadata,
            &mut context,
            &mut || Ok(()),
        )
        .expect_err("corrupt assembled ranges must fail");

        assert!(error.to_string().contains("hash mismatch"));
        assert!(matches!(
            destination.stat(&file),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn copy_back_never_trusts_forged_destination_ranges_without_a_content_hash() {
        let root = sinomo_testdir::TestDir::new("sandbox-copy-back-forged-range");
        let (_source_store, mut source) = local_filesystem(&root, "forged_range_source");
        let (_destination_store, mut destination) =
            local_filesystem(&root, "forged_range_destination");
        let file = SandboxPath::new("/work/value.bin").expect("file");
        let source_bytes = b"authoritative-new".to_vec();
        let stale_bytes = b"destination-old!!".to_vec();
        assert_eq!(source_bytes.len(), stale_bytes.len());
        source.write(&file, &source_bytes).expect("write source");
        destination
            .write(&file, &stale_bytes)
            .expect("write destination");
        let metadata = source.stat(&file).expect("source metadata");
        let mut destination = ForgedReadDestination {
            inner: destination,
            forged: source_bytes.clone(),
            read_calls: Cell::new(0),
        };
        let mut context = SandboxSyncContext::default();

        sync_sandbox_file_with_checkpoints(
            &mut source,
            &mut destination,
            &file,
            &metadata,
            &mut context,
            &mut || Ok(()),
        )
        .expect("safe copy-back");

        assert_eq!(destination.read_calls.get(), 0);
        assert_eq!(
            destination
                .inner
                .read(&file, None)
                .expect("read copied file"),
            source_bytes
        );
    }
}
