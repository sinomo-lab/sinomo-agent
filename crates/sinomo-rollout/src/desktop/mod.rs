//! Immutable desktop bundles and a durable, independently supervised app activation.
mod service;
mod supervisor;
mod upload;
pub(crate) use service::{observe_updates, update_publication};
pub use upload::receive_artifact;

pub use service::DesktopUpdateService;
pub use supervisor::supervise_app;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_api::app::update::*;
use sinomo_core::storage::durable_file::{
    DurableAtomicFileOptions, ensure_private_directory, remove_file_if_exists_and_sync_parent,
    sync_directory, write_file_atomic,
};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

type Result<T> = std::result::Result<T, String>;
pub const DESKTOP_BINARY_STEMS: [&str; 3] = [
    "sinomo-app",
    "sinomo-app-daemon",
    "sinomo-ui-extension-host",
];

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_BUNDLE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct DesktopReleases {
    root: PathBuf,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DesktopState {
    #[serde(default)]
    schema_version: u32,
    status: AppUpdateStatus,
    launch_token: Option<String>,
    launch_ready: bool,
    #[serde(default)]
    workspace_backup: Option<WorkspaceBackup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkspaceBackup {
    launch_id: String,
    existed: bool,
}

impl DesktopReleases {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            root: data_dir.as_ref().join("rollout/desktop"),
        }
    }

    fn lock(&self, name: &str) -> Result<File> {
        self.ensure_root()?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(name))
            .map_err(err)?;
        file.lock_exclusive().map_err(err)?;
        Ok(file)
    }

    fn ensure_root(&self) -> Result<()> {
        fs::create_dir_all(self.root.parent().ok_or("desktop root has no parent")?).map_err(err)?;
        ensure_private_directory(&self.root).map_err(err)
    }

    fn workspace_path(&self) -> Result<PathBuf> {
        Ok(self
            .root
            .parent()
            .and_then(Path::parent)
            .ok_or("desktop root has no data directory")?
            .join(APP_WORKSPACE_SNAPSHOT_PATH))
    }

    fn workspace_backup_path(&self, id: &str) -> Result<PathBuf> {
        crate::validate_rollout_upload_component("desktop workspace backup", id).map_err(err)?;
        Ok(self.root.join(format!("workspace-{id}.json")))
    }

    fn read(&self) -> Result<DesktopState> {
        match fs::read(self.root.join("state.json")) {
            Ok(bytes) => {
                let state: DesktopState = serde_json::from_slice(&bytes).map_err(err)?;
                if state.schema_version != 1 {
                    return Err("unsupported desktop activation state schema".into());
                }
                Ok(state)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(DesktopState::default())
            }
            Err(error) => Err(err(error)),
        }
    }

    fn change<T>(&self, f: impl FnOnce(&mut DesktopState) -> Result<T>) -> Result<T> {
        let _lock = self.lock("state.lock")?;
        let mut state = self.read()?;
        let result = f(&mut state)?;
        state.schema_version = 1;
        state.status.revision = state
            .status
            .revision
            .checked_add(1)
            .ok_or("desktop revision exhausted")?;
        atomic_json(&self.root.join("state.json"), &state)?;
        Ok(result)
    }

    pub fn status(&self) -> Result<AppUpdateStatus> {
        Ok(self.read()?.status)
    }

    pub fn release_dir(&self, id: &str) -> Result<PathBuf> {
        if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid desktop release identity".into());
        }
        Ok(self.root.join("releases").join(id))
    }

    /// Copy to an unpublished directory, verify stored bytes, then publish once. A rejected
    /// bundle never changes desired or active state. This is a local operator-authorized import.
    pub fn stage(&self, source: &Path) -> Result<AppUpdateStatus> {
        let manifest = read_manifest(source)?;
        validate_manifest(&manifest)?;
        let id = release_id(&manifest)?;
        let _stage_lock = self.lock("stage.lock")?;
        let destination = self.release_dir(&id)?;
        if !destination.exists() {
            let staging = self
                .root
                .join(format!(".stage-{}", sinomo_core::Id::generate()));
            fs::create_dir_all(&staging).map_err(err)?;
            let copy_result = (|| {
                for entry in &manifest.files {
                    let from = checked_file(source, &entry.path)?;
                    let to = staging.join(&entry.path);
                    fs::create_dir_all(to.parent().ok_or("missing artifact parent")?)
                        .map_err(err)?;
                    let mut input = File::open(from)
                        .map_err(err)?
                        .take(entry.size.saturating_add(1));
                    let mut output = File::create(&to).map_err(err)?;
                    let copied = std::io::copy(&mut input, &mut output).map_err(err)?;
                    if copied != entry.size {
                        return Err(format!("artifact length mismatch: {}", entry.path));
                    }
                    output.flush().map_err(err)?;
                    output.sync_all().map_err(err)?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        fs::set_permissions(
                            &to,
                            fs::Permissions::from_mode(if entry.executable {
                                0o755
                            } else {
                                0o644
                            }),
                        )
                        .map_err(err)?;
                    }
                    output.sync_all().map_err(err)?;
                    sync_ancestors(to.parent().ok_or("missing artifact parent")?, &staging)?;
                }
                atomic_json(&staging.join(APP_BUNDLE_MANIFEST), &manifest)?;
                verify_bundle(&staging, &manifest)?;
                fs::create_dir_all(destination.parent().ok_or("missing release parent")?)
                    .map_err(err)?;
                fs::rename(&staging, &destination).map_err(err)?;
                sync_directory(destination.parent().ok_or("missing release parent")?)
                    .map_err(err)?;
                Ok(())
            })();
            if copy_result.is_err() {
                let _ = fs::remove_dir_all(&staging);
            }
            copy_result?;
        } else {
            verify_bundle(&destination, &manifest)?;
        }
        self.change(|state| {
            if matches!(
                state.status.phase,
                AppUpdatePhase::RestartRequested | AppUpdatePhase::Starting
            ) {
                return Err("a desktop activation is already in progress".into());
            }
            if state.status.active_release.as_deref() != Some(&id) {
                state.status.candidate_release = Some(id);
                state.status.phase = AppUpdatePhase::Ready;
                state.status.detail =
                    "Update ready. Restart the app to restore your workspace.".into();
            }
            Ok(())
        })?;
        self.status()
    }

    pub fn request_restart(&self, request: AppUpdateRestartRequest) -> Result<AppUpdateStatus> {
        self.change(|state| {
            require_launch(state, &request.launch_token)?;
            if !matches!(
                state.status.phase,
                AppUpdatePhase::Ready | AppUpdatePhase::RestartRequested
            ) || state.status.candidate_release.as_deref() != Some(&request.candidate_release)
            {
                return Err("the selected app update is no longer ready".into());
            }
            state.status.phase = AppUpdatePhase::RestartRequested;
            state.status.detail = "Restarting and restoring the workspace…".into();
            Ok(())
        })?;
        self.status()
    }

    pub fn acknowledge_ready(&self, request: AppUpdateReadyRequest) -> Result<AppUpdateStatus> {
        if request.restoration_schema != 1 {
            return Err("unsupported app restoration schema".into());
        }
        self.change(|state| {
            require_launch(state, &request.launch_token)?;
            state.launch_ready = true;
            if state.status.phase == AppUpdatePhase::Starting {
                state.status.previous_release = state.status.active_release.take();
                state.status.active_release = state.status.candidate_release.take();
                state.status.phase = AppUpdatePhase::Current;
                state.status.detail = "App updated and workspace restored.".into();
                state.workspace_backup = None;
            }
            Ok(())
        })?;
        self.status()
    }

    fn begin_launch(&self, activating: bool) -> Result<String> {
        let token = sinomo_core::Id::generate().to_string();
        self.change(|state| {
            if activating {
                if state.status.phase != AppUpdatePhase::RestartRequested {
                    return Err("no app restart requested".into());
                }
                let workspace = self.workspace_path()?;
                let backup = match fs::read(&workspace) {
                    Ok(bytes) => {
                        if bytes.len() > 128 * 1024 * 1024 {
                            return Err("workspace snapshot exceeds the restoration limit".into());
                        }
                        write_file_atomic(
                            &self.workspace_backup_path(&token)?,
                            &bytes,
                            DurableAtomicFileOptions::new("workspace", "backup").sync_all(),
                        )
                        .map_err(err)?;
                        WorkspaceBackup {
                            launch_id: token.clone(),
                            existed: true,
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => WorkspaceBackup {
                        launch_id: token.clone(),
                        existed: false,
                    },
                    Err(e) => return Err(err(e)),
                };
                state.workspace_backup = Some(backup);
                state.status.phase = AppUpdatePhase::Starting;
            }
            state.launch_token = Some(token.clone());
            state.launch_ready = false;
            Ok(())
        })?;
        Ok(token)
    }

    fn rollback(&self, detail: String) -> Result<()> {
        self.change(|state| {
            if let Some(backup) = &state.workspace_backup {
                let workspace = self.workspace_path()?;
                if backup.existed {
                    let bytes =
                        fs::read(self.workspace_backup_path(&backup.launch_id)?).map_err(err)?;
                    write_file_atomic(
                        &workspace,
                        &bytes,
                        DurableAtomicFileOptions::new("workspace", "rollback").sync_all(),
                    )
                    .map_err(err)?;
                } else {
                    remove_file_if_exists_and_sync_parent(&workspace).map_err(err)?;
                }
                state.workspace_backup = None;
            }
            state.status.candidate_release = None;
            state.status.phase = AppUpdatePhase::RolledBack;
            state.status.detail = detail;
            state.launch_token = None;
            state.launch_ready = false;
            Ok(())
        })
    }

    /// Bootstrap is only used by the local launcher/installer, before a supervised activation.
    pub fn install_initial(&self, source: &Path) -> Result<AppUpdateStatus> {
        let status = self.stage(source)?;
        if status.active_release.is_none() && self.read()?.launch_token.is_none() {
            self.change(|state| {
                if state.launch_token.is_some() {
                    return Err("the running app must accept this update".into());
                }
                state.status.active_release = state.status.candidate_release.take();
                state.status.phase = AppUpdatePhase::Current;
                Ok(())
            })?;
        }
        self.status()
    }
}

fn require_launch(state: &DesktopState, token: &str) -> Result<()> {
    if token.is_empty() || state.launch_token.as_deref() != Some(token) {
        return Err("stale or unsupervised app launch".into());
    }
    Ok(())
}

pub fn read_manifest(directory: &Path) -> Result<AppBundleManifest> {
    let path = directory.join(APP_BUNDLE_MANIFEST);
    let file = File::open(path).map_err(err)?;
    if file.metadata().map_err(err)?.len() > MAX_MANIFEST_BYTES {
        return Err("desktop manifest too large".into());
    }
    serde_json::from_reader(file).map_err(err)
}

pub fn release_id(manifest: &AppBundleManifest) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(manifest).map_err(err)?)
        .to_hex()
        .to_string())
}

pub fn validate_manifest(manifest: &AppBundleManifest) -> Result<()> {
    if manifest.target_os != std::env::consts::OS || manifest.target_arch != std::env::consts::ARCH
    {
        return Err("desktop bundle is for a different platform".into());
    }
    validate_manifest_shape(manifest)
}

fn validate_manifest_shape(manifest: &AppBundleManifest) -> Result<()> {
    if manifest.schema_version != 1
        || manifest.restoration_schema != 1
        || manifest.daemon_protocol != sinomo_api::SINOMO_DAEMON_API_PROTOCOL_VERSION
    {
        return Err("desktop bundle has incompatible protocol or restoration schema".into());
    }
    if !matches!(manifest.target_os.as_str(), "linux" | "windows" | "macos")
        || !matches!(manifest.target_arch.as_str(), "x86_64" | "aarch64")
    {
        return Err("unsupported desktop platform".into());
    }
    if manifest.source_revision.is_empty()
        || manifest.source_revision.len() > 128
        || manifest.files.is_empty()
        || manifest.files.len() > 4096
    {
        return Err("invalid desktop bundle manifest".into());
    }
    let mut names = BTreeSet::new();
    let mut size = 0u64;
    for file in &manifest.files {
        validate_relative(&file.path)?;
        if file.path == APP_BUNDLE_MANIFEST || !names.insert(file.path.to_ascii_lowercase()) {
            return Err("duplicate or reserved bundle path".into());
        }
        if file.content_hash.len() != 64
            || !file.content_hash.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("invalid artifact digest".into());
        }
        size = size
            .checked_add(file.size)
            .ok_or("desktop bundle size overflow")?;
    }
    if size > MAX_BUNDLE_BYTES {
        return Err("desktop bundle too large".into());
    }
    for name in DESKTOP_BINARY_STEMS {
        let name = if manifest.target_os == "windows" {
            format!("{name}.exe")
        } else {
            name.to_string()
        };
        if !manifest
            .files
            .iter()
            .any(|f| f.path == name && f.executable && f.size > 0)
        {
            return Err(format!("desktop bundle is missing {name}"));
        }
    }
    Ok(())
}

fn validate_relative(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 1024
        || value.contains(['\\', ':'])
        || value.ends_with(['.', ' '])
        || value.split('/').any(|p| {
            p.is_empty()
                || p.starts_with('.')
                || p.ends_with(['.', ' '])
                || p.chars().any(char::is_control)
        })
        || !Path::new(value)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(format!("unsafe bundle path: {value}"));
    }
    Ok(())
}

fn sync_ancestors(mut directory: &Path, root: &Path) -> Result<()> {
    while directory.starts_with(root) {
        sync_directory(directory).map_err(err)?;
        if directory == root {
            break;
        }
        directory = directory
            .parent()
            .ok_or("artifact directory escaped staging root")?;
    }
    Ok(())
}

fn checked_file(root: &Path, value: &str) -> Result<PathBuf> {
    validate_relative(value)?;
    let mut path = root.to_path_buf();
    for part in Path::new(value).components() {
        path.push(part);
        if fs::symlink_metadata(&path)
            .map_err(err)?
            .file_type()
            .is_symlink()
        {
            return Err("bundle symlinks are forbidden".into());
        }
    }
    if !path.is_file() {
        return Err("bundle entry is not a regular file".into());
    }
    Ok(path)
}

pub fn verify_bundle(root: &Path, manifest: &AppBundleManifest) -> Result<()> {
    validate_manifest(manifest)?;
    verify_bundle_content(root, manifest)
}

pub fn verify_bundle_content(root: &Path, manifest: &AppBundleManifest) -> Result<()> {
    validate_manifest_shape(manifest)?;
    for entry in &manifest.files {
        let path = checked_file(root, &entry.path)?;
        let mut file = File::open(path).map_err(err)?;
        if file.metadata().map_err(err)?.len() != entry.size {
            return Err(format!("artifact length mismatch: {}", entry.path));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(&mut file).map_err(err)?;
        if hasher.finalize().to_hex().as_str() != entry.content_hash {
            return Err(format!("artifact digest mismatch: {}", entry.path));
        }
    }
    Ok(())
}

/// Build a release manifest for already built artifacts; no compilation or activation occurs.
pub fn write_bundle_manifest(
    directory: &Path,
    source_revision: String,
    files: &[String],
) -> Result<AppBundleManifest> {
    write_bundle_manifest_for_target(
        directory,
        source_revision,
        files,
        std::env::consts::OS,
        std::env::consts::ARCH,
    )
}

pub fn write_bundle_manifest_for_target(
    directory: &Path,
    source_revision: String,
    files: &[String],
    target_os: &str,
    target_arch: &str,
) -> Result<AppBundleManifest> {
    let mut entries = Vec::new();
    for name in files {
        let path = checked_file(directory, name)?;
        let mut file = File::open(path).map_err(err)?;
        let size = file.metadata().map_err(err)?.len();
        let mut hash = blake3::Hasher::new();
        hash.update_reader(&mut file).map_err(err)?;
        entries.push(AppBundleFile {
            path: name.clone(),
            size,
            content_hash: hash.finalize().to_hex().to_string(),
            executable: name.starts_with("sinomo-"),
        });
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest = AppBundleManifest {
        schema_version: 1,
        source_revision,
        target_os: target_os.into(),
        target_arch: target_arch.into(),
        daemon_protocol: sinomo_api::SINOMO_DAEMON_API_PROTOCOL_VERSION,
        restoration_schema: 1,
        files: entries,
    };
    validate_manifest_shape(&manifest)?;
    atomic_json(&directory.join(APP_BUNDLE_MANIFEST), &manifest)?;
    Ok(manifest)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(err)?;
    write_file_atomic(
        path,
        &bytes,
        DurableAtomicFileOptions::new("desktop", "update").sync_all(),
    )
    .map_err(err)
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests;
