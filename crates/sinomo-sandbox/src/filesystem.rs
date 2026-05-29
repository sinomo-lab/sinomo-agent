use crate::internal::*;

mod local;
mod sifs;
mod support;
mod types;
mod usage;
mod virtual_devices;

pub use local::{LocalSandboxFileSnapshot, LocalSandboxFileSystem};
pub use sifs::SifsSandboxFileSystem;
pub use support::reject_symlink_chain;
pub(crate) use support::{
    normalize_sandbox_sifs_root, sandbox_store_error_from_dfs, sandbox_toolchain_error_from_dfs,
};
pub use types::*;
pub use usage::{
    sandbox_filesystem_workspace_usage, validate_sandbox_write_resource_limits,
    validate_workspace_usage, validate_written_bytes,
};
pub use virtual_devices::sandbox_path_is_virtual_device_sink;

pub fn validate_sandbox_working_directory(
    fs: &impl SandboxFileSystem,
    cwd: &SandboxPath,
) -> Result<(), SandboxFsError> {
    if virtual_devices::sandbox_path_is_dev_tree(cwd) {
        return Err(SandboxFsError::Unsupported {
            operation: format!("use virtual device path as sandbox cwd {cwd}"),
        });
    }
    let metadata = fs.stat_follow(cwd)?;
    if !metadata.is_dir {
        return Err(SandboxFsError::Unsupported {
            operation: format!("use non-directory sandbox cwd {cwd}"),
        });
    }
    Ok(())
}
