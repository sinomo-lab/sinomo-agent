//! Node-local module setting persistence, owned by the module runtime.
use crate::{ModuleId, NativeRustModuleRuntimeState, read_object, write_object};
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub fn module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root.join("module").join("loader-state")
}

pub fn native_rust_module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    module_loader_state_dir(data_dir_root).join("native-rust")
}

pub fn native_rust_module_loader_state_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    native_rust_module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn read_native_rust_module_runtime_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<NativeRustModuleRuntimeState>, ModuleLoaderStoreError> {
    let path = native_rust_module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_native_rust_module_runtime_state(
    data_dir_root: &Path,
    state: &NativeRustModuleRuntimeState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = native_rust_module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    Ok(path)
}

pub fn set_native_rust_module_enabled(
    data_dir_root: &Path,
    module_id: &ModuleId,
    enabled: bool,
    source: impl Into<String>,
    reason: impl Into<String>,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let state = NativeRustModuleRuntimeState {
        module_id: module_id.clone(),
        enabled,
        updated_at: module_loader_applied_at_now(),
        source: source.into(),
        reason: reason.into(),
    };
    write_native_rust_module_runtime_state(data_dir_root, &state)
}

pub fn module_loader_applied_at_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Debug, Error)]
pub enum ModuleLoaderStoreError {
    #[error("module loader state I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("module loader state object error at {path}: {detail}")]
    Object { path: String, detail: String },
}
