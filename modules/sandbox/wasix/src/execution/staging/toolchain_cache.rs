use crate::internal::*;

pub(in crate::execution) fn stage_toolchain_runtime_to_local(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<Vec<SandboxWasiPreopen>, SandboxCommandExecutionError> {
    let toolchain_root = ensure_local_toolchain_runtime_cache(layout, artifacts, commands)?;
    let mut preopens = Vec::new();
    if toolchain_root.exists() {
        let mut entries = fs::read_dir(&toolchain_root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "list toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let file_type = entry
                .file_type()
                .map_err(|source| SandboxFsError::Unavailable {
                    reason: format!(
                        "inspect toolchain artifact stage entry {}: {source}",
                        entry.path().display()
                    ),
                })?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            validate_toolchain_stage_top_level_name(&name)?;
            preopens.push(SandboxWasiPreopen {
                host_path: entry.path(),
                guest_path: SandboxPath::new(format!("/{name}")).map_err(|source| {
                    SandboxFsError::Unavailable {
                        reason: format!(
                            "invalid staged toolchain top-level path `{name}`: {source}"
                        ),
                    }
                })?,
                mode: SandboxMountMode::ReadOnly,
            });
        }
    }
    Ok(preopens)
}

#[cfg(feature = "wasix-runtime")]
pub(in crate::execution) fn stage_toolchain_runtime_to_virtual(
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<Vec<SandboxWasiVirtualPreopen>, SandboxCommandExecutionError> {
    let mut preopens = BTreeMap::<String, virtual_fs::mem_fs::FileSystem>::new();

    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        let fs = virtual_toolchain_preopen(&mut preopens, "bin")?;
        write_virtual_toolchain_file(fs, Path::new(command_name), bytes, true)?;
    }

    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        let (top_level, tail) =
            relative_path
                .split_once('/')
                .ok_or_else(|| SandboxFsError::Unavailable {
                    reason: format!("invalid staged toolchain artifact path `{relative_path}`"),
                })?;
        let fs = virtual_toolchain_preopen(&mut preopens, top_level)?;
        write_virtual_toolchain_file(fs, Path::new(tail), bytes, top_level == "bin")?;
    }

    preopens
        .into_iter()
        .map(|(name, fs)| {
            Ok(SandboxWasiVirtualPreopen {
                guest_path: SandboxPath::new(format!("/{name}")).map_err(|source| {
                    SandboxFsError::Unavailable {
                        reason: format!(
                            "invalid staged toolchain top-level path `{name}`: {source}"
                        ),
                    }
                })?,
                mode: SandboxMountMode::ReadOnly,
                fs: Arc::new(fs),
            })
        })
        .collect()
}

#[cfg(feature = "wasix-runtime")]
fn virtual_toolchain_preopen<'a>(
    preopens: &'a mut BTreeMap<String, virtual_fs::mem_fs::FileSystem>,
    name: &str,
) -> Result<&'a virtual_fs::mem_fs::FileSystem, SandboxCommandExecutionError> {
    validate_toolchain_stage_top_level_name(name)?;
    Ok(preopens
        .entry(name.to_string())
        .or_insert_with(virtual_fs::mem_fs::FileSystem::default))
}

#[cfg(feature = "wasix-runtime")]
fn write_virtual_toolchain_file(
    fs: &virtual_fs::mem_fs::FileSystem,
    path: &Path,
    bytes: &[u8],
    executable: bool,
) -> Result<(), SandboxCommandExecutionError> {
    use virtual_fs::FileSystem as _;

    let virtual_path = PathBuf::from("/").join(path);
    if let Some(parent) = virtual_path.parent() {
        virtual_fs::create_dir_all(fs, parent).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "create virtual toolchain file parent {}: {source}",
                parent.display()
            ),
        })?;
    }
    let mut file = fs
        .new_open_options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&virtual_path)
        .map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "open virtual toolchain file {}: {source}",
                virtual_path.display()
            ),
        })?;
    virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, bytes)).map_err(
        |source| SandboxFsError::Unavailable {
            reason: format!(
                "write virtual toolchain file {}: {source}",
                virtual_path.display()
            ),
        },
    )?;
    if executable {
        match fs.set_mode(&virtual_path, 0o555) {
            Ok(()) | Err(virtual_fs::FsError::Unsupported) => {}
            Err(source) => {
                return Err(SandboxFsError::Unavailable {
                    reason: format!(
                        "mark virtual toolchain file executable {}: {source}",
                        virtual_path.display()
                    ),
                }
                .into());
            }
        }
    }
    Ok(())
}

fn ensure_local_toolchain_runtime_cache(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<PathBuf, SandboxCommandExecutionError> {
    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_key = local_toolchain_runtime_cache_key(artifacts, commands)?;
    let cache_manifest = local_toolchain_runtime_cache_manifest(&cache_key, artifacts, commands)?;
    let toolchain_root = cache_root.join(&cache_key);
    let complete_marker = toolchain_root.join(".complete");
    if complete_marker.is_file()
        && local_toolchain_runtime_cache_is_complete(
            &toolchain_root,
            &cache_manifest,
            artifacts,
            commands,
        )?
    {
        return Ok(toolchain_root);
    }

    fs::create_dir_all(&cache_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create toolchain runtime cache {}: {source}",
            cache_root.display()
        ),
    })?;

    if toolchain_root.exists() {
        reject_symlink_chain(
            &toolchain_root,
            &SandboxPath::new("/toolchain").expect("static path"),
        )?;
        fs::remove_dir_all(&toolchain_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear incomplete toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        })?;
    }

    let temp_root = cache_root.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    if temp_root.exists() {
        fs::remove_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear temporary toolchain runtime cache {}: {source}",
                temp_root.display()
            ),
        })?;
    }
    fs::create_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create temporary toolchain runtime cache {}: {source}",
            temp_root.display()
        ),
    })?;

    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        let host_path = temp_root.join("bin").join(command_name);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain command cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain command cache {}: {source}",
                host_path.display()
            ),
        })?;
        set_staged_toolchain_command_executable(&host_path)?;
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        let host_path = temp_root.join(&relative_path);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain artifact cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        if host_path.exists() {
            fs::remove_file(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "replace toolchain artifact cache {}: {source}",
                    host_path.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain artifact cache {}: {source}",
                host_path.display()
            ),
        })?;
        if relative_path.starts_with("bin/") {
            set_staged_toolchain_command_executable(&host_path)?;
        }
    }
    fs::write(&complete_marker_for_temp(&temp_root), &cache_manifest).map_err(|source| {
        SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain runtime cache marker {}: {source}",
                temp_root.join(".complete").display()
            ),
        }
    })?;

    match fs::rename(&temp_root, &toolchain_root) {
        Ok(()) => Ok(toolchain_root),
        Err(source) if toolchain_root.join(".complete").is_file() => {
            let _ = fs::remove_dir_all(&temp_root);
            let _ = source;
            Ok(toolchain_root)
        }
        Err(source) => Err(SandboxFsError::Unavailable {
            reason: format!(
                "publish toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        }
        .into()),
    }
}

fn local_toolchain_runtime_cache_is_complete(
    toolchain_root: &Path,
    expected_manifest: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<bool, SandboxCommandExecutionError> {
    match fs::read_to_string(toolchain_root.join(".complete")) {
        Ok(actual_manifest) if actual_manifest == expected_manifest => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain runtime cache marker {}: {source}",
                    toolchain_root.join(".complete").display()
                ),
            }
            .into());
        }
    }

    let mut expected_files = BTreeMap::new();

    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        expected_files.insert(format!("bin/{command_name}"), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join("bin").join(command_name),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        expected_files.insert(relative_path.clone(), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join(&relative_path),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }

    let mut actual_files = BTreeSet::new();
    if !collect_local_toolchain_runtime_cache_files(
        toolchain_root,
        toolchain_root,
        &mut actual_files,
    )? {
        return Ok(false);
    }
    if actual_files != expected_files.keys().cloned().collect::<BTreeSet<_>>() {
        return Ok(false);
    }

    Ok(true)
}

fn local_toolchain_runtime_cache_manifest(
    cache_key: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut manifest = String::new();
    manifest.push_str("sinomo-toolchain-runtime-cache-v1\n");
    manifest.push_str("key=");
    manifest.push_str(cache_key);
    manifest.push('\n');

    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, len, hash) in artifact_entries {
        manifest.push_str("artifact\t");
        manifest.push_str(&relative_path);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((
            command_name.clone(),
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    command_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (command_name, len, hash) in command_entries {
        manifest.push_str("command\t");
        manifest.push_str(&command_name);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    Ok(manifest)
}

fn collect_local_toolchain_runtime_cache_files(
    root: &Path,
    path: &Path,
    files: &mut BTreeSet<String>,
) -> Result<bool, SandboxCommandExecutionError> {
    let mut entry_count = 0usize;
    for entry in fs::read_dir(path).map_err(|source| SandboxFsError::Unavailable {
        reason: format!("list toolchain runtime cache {}: {source}", path.display()),
    })? {
        let entry = entry.map_err(|source| SandboxFsError::Unavailable {
            reason: format!("read toolchain runtime cache {}: {source}", path.display()),
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if path == root && name == ".complete" {
            continue;
        }
        entry_count += 1;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache entry {}: {source}",
                    entry.path().display()
                ),
            })?;
        if metadata.file_type().is_symlink() {
            return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
                path: entry.path().display().to_string(),
            })
            .into());
        }
        if metadata.is_dir() {
            if !collect_local_toolchain_runtime_cache_files(root, &entry.path(), files)? {
                return Ok(false);
            }
            continue;
        }
        if !metadata.is_file() {
            return Err(SandboxFsError::Unsupported {
                operation: format!(
                    "inspect special toolchain runtime cache entry {}",
                    entry.path().display()
                ),
            }
            .into());
        }
        let relative_path = entry
            .path()
            .strip_prefix(root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "derive toolchain runtime cache relative path {}: {source}",
                    entry.path().display()
                ),
            })?
            .to_string_lossy()
            .replace('\\', "/");
        files.insert(relative_path);
    }
    Ok(path == root || entry_count > 0)
}

fn local_toolchain_runtime_cache_file_matches(
    path: &Path,
    expected_len: u64,
) -> Result<bool, SandboxCommandExecutionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache file {}: {source}",
                    path.display()
                ),
            }
            .into());
        }
    };
    Ok(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() == expected_len)
}

fn complete_marker_for_temp(temp_root: &Path) -> PathBuf {
    temp_root.join(".complete")
}

fn local_toolchain_runtime_cache_key(
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut hasher = SandboxModuleHasher::new();
    hasher.update(b"sinomo-toolchain-runtime-cache-v1\n");
    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes,
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, bytes) in artifact_entries {
        hasher.update(b"artifact\0");
        hasher.update(relative_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((command_name, bytes));
    }
    command_entries.sort_by(|left, right| left.0.cmp(right.0));
    for (command_name, bytes) in command_entries {
        hasher.update(b"command\0");
        hasher.update(command_name.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }
    Ok(hasher
        .finalize()
        .strip_prefix("blake3:")
        .unwrap_or("invalid")
        .to_string())
}

fn validate_toolchain_stage_top_level_name(name: &str) -> Result<(), SandboxCommandExecutionError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SandboxFsError::Unavailable {
            reason: format!("invalid staged toolchain top-level path `{name}`"),
        }
        .into());
    }
    if toolchain_stage_top_level_is_reserved(name) {
        return Err(SandboxFsError::Unavailable {
            reason: format!(
                "staged toolchain top-level path `{name}` would shadow a sandbox filesystem mount"
            ),
        }
        .into());
    }
    Ok(())
}

fn toolchain_stage_top_level_is_reserved(name: &str) -> bool {
    matches!(
        name,
        "artifacts"
            | "dev"
            | "etc"
            | "home"
            | "logs"
            | "opt"
            | "proc"
            | "root"
            | "run"
            | "sys"
            | "tmp"
            | "var"
            | "work"
    )
}

#[cfg(unix)]
fn set_staged_toolchain_command_executable(
    host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(host_path, fs::Permissions::from_mode(0o755)).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "mark staged toolchain command executable {}: {source}",
            host_path.display()
        ),
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn set_staged_toolchain_command_executable(
    _host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    Ok(())
}
