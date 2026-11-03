use crate::internal::*;

pub fn validate_workspace_usage(
    usage: &SandboxWorkspaceUsage,
    limits: &SandboxResourceLimits,
) -> Result<(), SandboxFsError> {
    if usage.file_count > limits.max_file_count {
        return Err(SandboxFsError::QuotaExceeded {
            kind: "file-count".to_string(),
            actual: usage.file_count,
            limit: limits.max_file_count,
        });
    }
    if usage.total_bytes > limits.max_workspace_bytes {
        return Err(SandboxFsError::QuotaExceeded {
            kind: "workspace-bytes".to_string(),
            actual: usage.total_bytes,
            limit: limits.max_workspace_bytes,
        });
    }
    Ok(())
}

pub fn sandbox_filesystem_workspace_usage<F: SandboxFileSystem>(
    filesystem: &mut F,
) -> Result<SandboxWorkspaceUsage, SandboxFsError> {
    let mut usage = SandboxWorkspaceUsage::default();
    for root in [
        SandboxPath::new("/root").expect("static path"),
        SandboxPath::new("/home").expect("static path"),
        SandboxPath::new("/tmp").expect("static path"),
        SandboxPath::new("/work").expect("static path"),
        SandboxPath::new("/var").expect("static path"),
        SandboxPath::new("/logs").expect("static path"),
        SandboxPath::new("/artifacts").expect("static path"),
    ] {
        collect_sandbox_filesystem_usage(filesystem, &root, &mut usage)?;
    }
    Ok(usage)
}

pub fn validate_sandbox_write_resource_limits<F: SandboxFileSystem>(
    filesystem: &mut F,
    path: &SandboxPath,
    bytes_len: u64,
    limits: &SandboxResourceLimits,
) -> Result<SandboxWorkspaceUsage, SandboxFsError> {
    if sandbox_path_is_virtual_device_sink(path) {
        return sandbox_filesystem_workspace_usage(filesystem);
    }
    validate_written_bytes(bytes_len, limits)?;
    let mut usage = sandbox_filesystem_workspace_usage(filesystem)?;
    match filesystem.stat_mut(path) {
        Ok(metadata) if metadata.is_dir => {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(metadata) => {
            usage.total_bytes = usage.total_bytes.saturating_sub(metadata.len);
        }
        Err(SandboxFsError::NotFound { .. }) => {
            usage.file_count = usage.file_count.saturating_add(1);
        }
        Err(error) => return Err(error),
    }
    usage.total_bytes = usage.total_bytes.saturating_add(bytes_len);
    validate_workspace_usage(&usage, limits)?;
    Ok(usage)
}

fn collect_sandbox_filesystem_usage<F: SandboxFileSystem>(
    filesystem: &mut F,
    path: &SandboxPath,
    usage: &mut SandboxWorkspaceUsage,
) -> Result<(), SandboxFsError> {
    let metadata = filesystem.stat_mut(path)?;
    if metadata.is_dir {
        for entry in filesystem.list_mut(path)? {
            collect_sandbox_filesystem_usage(filesystem, &entry.path, usage)?;
        }
    } else {
        usage.file_count = usage.file_count.saturating_add(1);
        usage.total_bytes = usage.total_bytes.saturating_add(metadata.len);
    }
    Ok(())
}

pub fn validate_written_bytes(
    written_bytes: u64,
    limits: &SandboxResourceLimits,
) -> Result<(), SandboxFsError> {
    if written_bytes > limits.max_written_bytes_per_command {
        return Err(SandboxFsError::QuotaExceeded {
            kind: "written-bytes-per-command".to_string(),
            actual: written_bytes,
            limit: limits.max_written_bytes_per_command,
        });
    }
    Ok(())
}
