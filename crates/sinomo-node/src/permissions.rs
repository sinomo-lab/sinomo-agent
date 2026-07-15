use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_object::{DecodeOptions, LengthMode, serde_ext};
use sinomo_storage::durable_file;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

pub const LOCAL_NODE_PERMISSIONS_RELATIVE_PATH: &str = "identity/local-permissions.sobj";
pub const LOCAL_NODE_PERMISSIONS_LOCK_RELATIVE_PATH: &str = "identity/local-permissions.sobj.lock";
pub const LOCAL_NODE_PERMISSIONS_OBJECT_VERSION: u16 = 1;

const LOCAL_NODE_PERMISSIONS_MAX_BYTES: u64 = 1024 * 1024;
static LOCAL_NODE_PERMISSIONS_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A host-local authority grant. These grants are deliberately not cluster
/// capabilities and must never be inferred from module activation or rollout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LocalNodePermission {
    /// Allows remote management as well as other explicitly marked,
    /// unrestricted host-control tools.
    RemoteManagement,
}

impl LocalNodePermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RemoteManagement => "remote-management",
        }
    }
}

impl TryFrom<&str> for LocalNodePermission {
    type Error = ParseLocalNodePermissionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "remote-management" => Ok(Self::RemoteManagement),
            _ => Err(ParseLocalNodePermissionError {
                value: value.to_string(),
            }),
        }
    }
}

impl FromStr for LocalNodePermission {
    type Err = ParseLocalNodePermissionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("unknown node-local permission `{value}`")]
pub struct ParseLocalNodePermissionError {
    pub value: String,
}

/// Operations that the local operator allows callers to expose for one local
/// resource. Resource identifiers and operations remain generic so device
/// implementations can use their stable device IDs and operation vocabulary.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalResourceExposure {
    #[serde(default)]
    operations: BTreeSet<String>,
}

impl LocalResourceExposure {
    pub fn operations(&self) -> &BTreeSet<String> {
        &self.operations
    }

    pub fn exposes(&self, operation: &str) -> bool {
        self.operations.contains(operation)
    }
}

/// The complete locally controlled permission document for one node.
///
/// The empty document is the secure default: it exposes no resources and
/// grants no host-control permission.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalNodePermissions {
    #[serde(default)]
    grants: BTreeSet<LocalNodePermission>,
    #[serde(default)]
    resource_exposures: BTreeMap<String, LocalResourceExposure>,
}

impl LocalNodePermissions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn grants(&self) -> &BTreeSet<LocalNodePermission> {
        &self.grants
    }

    pub fn resource_exposures(&self) -> &BTreeMap<String, LocalResourceExposure> {
        &self.resource_exposures
    }

    pub fn is_granted(&self, permission: LocalNodePermission) -> bool {
        self.grants.contains(&permission)
    }

    pub fn remote_management_enabled(&self) -> bool {
        self.is_granted(LocalNodePermission::RemoteManagement)
    }

    pub fn set_permission(&mut self, permission: LocalNodePermission, granted: bool) -> bool {
        if granted {
            self.grants.insert(permission)
        } else {
            self.grants.remove(&permission)
        }
    }

    pub fn set_remote_management_enabled(&mut self, enabled: bool) -> bool {
        self.set_permission(LocalNodePermission::RemoteManagement, enabled)
    }

    pub fn resource_operation_exposed(&self, resource_id: &str, operation: &str) -> bool {
        self.resource_exposures
            .get(resource_id)
            .is_some_and(|exposure| exposure.exposes(operation))
    }

    pub fn expose_resource_operation(
        &mut self,
        resource_id: impl Into<String>,
        operation: impl Into<String>,
    ) -> Result<bool, LocalNodePermissionError> {
        let resource_id = resource_id.into();
        let operation = operation.into();
        validate_resource_id(&resource_id)?;
        validate_resource_operation(&operation)?;
        Ok(self
            .resource_exposures
            .entry(resource_id)
            .or_default()
            .operations
            .insert(operation))
    }

    pub fn hide_resource_operation(
        &mut self,
        resource_id: &str,
        operation: &str,
    ) -> Result<bool, LocalNodePermissionError> {
        validate_resource_id(resource_id)?;
        validate_resource_operation(operation)?;
        let Some(exposure) = self.resource_exposures.get_mut(resource_id) else {
            return Ok(false);
        };
        let removed = exposure.operations.remove(operation);
        if exposure.operations.is_empty() {
            self.resource_exposures.remove(resource_id);
        }
        Ok(removed)
    }

    pub fn hide_resource(&mut self, resource_id: &str) -> Result<bool, LocalNodePermissionError> {
        validate_resource_id(resource_id)?;
        Ok(self.resource_exposures.remove(resource_id).is_some())
    }

    pub fn validate(&self) -> Result<(), LocalNodePermissionError> {
        for (resource_id, exposure) in &self.resource_exposures {
            validate_resource_id(resource_id)?;
            if exposure.operations.is_empty() {
                return Err(LocalNodePermissionError::InvalidState {
                    detail: format!("resource `{resource_id}` has no exposed operations"),
                });
            }
            for operation in &exposure.operations {
                validate_resource_operation(operation)?;
            }
        }
        Ok(())
    }
}

/// Filesystem-backed access to the node-local permission document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalNodePermissionStore {
    path: PathBuf,
}

impl LocalNodePermissionStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            path: data_dir.as_ref().join(LOCAL_NODE_PERMISSIONS_RELATIVE_PATH),
        }
    }

    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads the local permission document. A missing document is an empty,
    /// deny-all document; malformed or unreadable documents return an error.
    pub fn load(&self) -> Result<LocalNodePermissions, LocalNodePermissionError> {
        let Some(bytes) = durable_file::read_optional_object_file_bytes(
            &self.path,
            LOCAL_NODE_PERMISSIONS_MAX_BYTES,
        )
        .map_err(|source| local_permission_io_error(&self.path, source))?
        else {
            return Ok(LocalNodePermissions::default());
        };
        let permissions = serde_ext::from_bytes_with_options(
            &bytes,
            DecodeOptions::default()
                .with_expected_version_id(LOCAL_NODE_PERMISSIONS_OBJECT_VERSION),
        )
        .map_err(|source| LocalNodePermissionError::Object {
            path: self.path.display().to_string(),
            detail: source.to_string(),
        })?;
        LocalNodePermissions::validate(&permissions)?;
        Ok(permissions)
    }

    pub fn save(&self, permissions: &LocalNodePermissions) -> Result<(), LocalNodePermissionError> {
        permissions.validate()?;
        let bytes = serde_ext::to_bytes(
            permissions,
            LOCAL_NODE_PERMISSIONS_OBJECT_VERSION,
            LengthMode::U32,
        )
        .map_err(|source| LocalNodePermissionError::Object {
            path: self.path.display().to_string(),
            detail: source.to_string(),
        })?;
        if bytes.len() as u64 > LOCAL_NODE_PERMISSIONS_MAX_BYTES {
            return Err(LocalNodePermissionError::InvalidState {
                detail: format!(
                    "encoded permission document is {} bytes; maximum is {LOCAL_NODE_PERMISSIONS_MAX_BYTES}",
                    bytes.len()
                ),
            });
        }
        write_private_atomic(&self.path, &bytes)
    }

    /// Applies a local permission edit while holding a process-safe lock over
    /// the complete load/modify/save transaction. Callers that edit permission
    /// state must use this method so a concurrent revoke cannot be restored by
    /// a stale writer.
    pub fn mutate<F>(
        &self,
        mutate: F,
    ) -> Result<(LocalNodePermissions, bool), LocalNodePermissionError>
    where
        F: FnOnce(&mut LocalNodePermissions) -> Result<bool, LocalNodePermissionError>,
    {
        let lock = self.acquire_mutation_lock()?;
        let mut permissions = self.load()?;
        let changed = mutate(&mut permissions)?;
        if changed {
            self.save(&permissions)?;
        }
        fs2::FileExt::unlock(&lock).map_err(|source| LocalNodePermissionError::Io {
            path: self.mutation_lock_path().display().to_string(),
            detail: source.to_string(),
        })?;
        Ok((permissions, changed))
    }

    fn mutation_lock_path(&self) -> PathBuf {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("local-permissions.sobj");
        parent.join(format!("{file_name}.lock"))
    }

    fn acquire_mutation_lock(&self) -> Result<std::fs::File, LocalNodePermissionError> {
        let lock_path = self.mutation_lock_path();
        let parent = lock_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source| LocalNodePermissionError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        options.mode(0o600);
        let lock = options
            .open(&lock_path)
            .map_err(|source| LocalNodePermissionError::Io {
                path: lock_path.display().to_string(),
                detail: source.to_string(),
            })?;
        #[cfg(unix)]
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            LocalNodePermissionError::Io {
                path: lock_path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        lock.lock_exclusive()
            .map_err(|source| LocalNodePermissionError::Io {
                path: lock_path.display().to_string(),
                detail: source.to_string(),
            })?;
        Ok(lock)
    }

    /// Diagnostic form of the permission check. Missing state returns
    /// `Ok(false)` while malformed or unreadable state retains its error.
    pub fn try_permission_granted(
        &self,
        permission: LocalNodePermission,
    ) -> Result<bool, LocalNodePermissionError> {
        self.load()
            .map(|permissions| permissions.is_granted(permission))
    }

    /// Fail-closed permission check for final effect boundaries.
    pub fn permission_granted(&self, permission: LocalNodePermission) -> bool {
        self.try_permission_granted(permission).unwrap_or(false)
    }

    pub fn remote_management_enabled(&self) -> bool {
        self.permission_granted(LocalNodePermission::RemoteManagement)
    }

    pub fn try_resource_operation_exposed(
        &self,
        resource_id: &str,
        operation: &str,
    ) -> Result<bool, LocalNodePermissionError> {
        validate_resource_id(resource_id)?;
        validate_resource_operation(operation)?;
        self.load()
            .map(|permissions| permissions.resource_operation_exposed(resource_id, operation))
    }

    /// Fail-closed resource check for device and other resource brokers.
    pub fn resource_operation_exposed(&self, resource_id: &str, operation: &str) -> bool {
        self.try_resource_operation_exposed(resource_id, operation)
            .unwrap_or(false)
    }
}

fn validate_resource_id(resource_id: &str) -> Result<(), LocalNodePermissionError> {
    validate_local_permission_name(resource_id, "resource identifier")
}

fn validate_resource_operation(operation: &str) -> Result<(), LocalNodePermissionError> {
    validate_local_permission_name(operation, "resource operation")
}

fn validate_local_permission_name(
    value: &str,
    label: &'static str,
) -> Result<(), LocalNodePermissionError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(LocalNodePermissionError::InvalidState {
            detail: format!(
                "{label} must be non-empty, trimmed, and contain no control characters"
            ),
        });
    }
    Ok(())
}

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<(), LocalNodePermissionError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| LocalNodePermissionError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("local-permissions.sobj");
    let sequence = LOCAL_NODE_PERMISSIONS_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp_path = parent.join(format!(
        ".{file_name}.write-tmp-{}-{sequence}",
        std::process::id()
    ));

    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temp_path)
            .map_err(|source| LocalNodePermissionError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(bytes)
            .map_err(|source| LocalNodePermissionError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.sync_all()
            .map_err(|source| LocalNodePermissionError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        drop(file);

        durable_file::replace_file(&temp_path, path)
            .map_err(|source| local_permission_io_error(path, source))?;
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            LocalNodePermissionError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .and_then(|file| file.sync_all())
            .map_err(|source| LocalNodePermissionError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        durable_file::sync_directory(parent)
            .map_err(|source| local_permission_io_error(parent, source))
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn local_permission_io_error(
    path: &Path,
    source: durable_file::DurableFileError,
) -> LocalNodePermissionError {
    LocalNodePermissionError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LocalNodePermissionError {
    #[error("node-local permission I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("node-local permission Sinomo object error at {path}: {detail}")]
    Object { path: String, detail: String },
    #[error("invalid node-local permission state: {detail}")]
    InvalidState { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_corrupt_permission_state_fail_closed() {
        let root = sinomo_testdir::TestDir::new("sinomo-node-local-permissions-deny");
        let store = LocalNodePermissionStore::new(&root);

        assert_eq!(
            store.load().expect("missing state"),
            LocalNodePermissions::default()
        );
        assert!(!store.remote_management_enabled());
        assert!(!store.resource_operation_exposed("device:camera:front", "capture-raw"));

        fs::create_dir_all(store.path().parent().expect("permission parent"))
            .expect("create permission parent");
        fs::write(store.path(), b"not a Sinomo object").expect("write corrupt state");

        assert!(store.load().is_err());
        assert!(!store.remote_management_enabled());
        assert!(!store.resource_operation_exposed("device:camera:front", "capture-raw"));
    }

    #[test]
    fn permission_names_parse_strictly() {
        assert_eq!(
            LocalNodePermission::RemoteManagement.as_str(),
            "remote-management"
        );
        assert_eq!(
            LocalNodePermission::try_from("remote-management"),
            Ok(LocalNodePermission::RemoteManagement)
        );
        assert_eq!(
            "remote-management".parse(),
            Ok(LocalNodePermission::RemoteManagement)
        );
        assert!(LocalNodePermission::try_from("RemoteManagement").is_err());
        assert!(LocalNodePermission::try_from(" remote-management").is_err());
        assert!(LocalNodePermission::try_from("unknown").is_err());
    }

    #[test]
    fn permission_and_resource_exposure_round_trip_with_exact_matching() {
        let root = sinomo_testdir::TestDir::new("sinomo-node-local-permissions-roundtrip");
        let store = LocalNodePermissionStore::new(&root);
        let mut permissions = LocalNodePermissions::new();

        assert!(permissions.set_remote_management_enabled(true));
        assert!(
            permissions
                .expose_resource_operation("android:camera:front", "observe")
                .expect("expose observe")
        );
        assert!(
            permissions
                .expose_resource_operation("android:camera:front", "capture-raw")
                .expect("expose capture")
        );
        assert!(
            permissions
                .expose_resource_operation("android:touchscreen:primary", "execute-action")
                .expect("expose action")
        );
        store.save(&permissions).expect("save permissions");

        let loaded = store.load().expect("load permissions");
        assert_eq!(loaded, permissions);
        assert!(store.remote_management_enabled());
        assert!(store.resource_operation_exposed("android:camera:front", "capture-raw"));
        assert!(!store.resource_operation_exposed("android:camera:front", "emit-output"));
        assert!(!store.resource_operation_exposed("android:camera:rear", "capture-raw"));

        assert!(
            permissions
                .hide_resource_operation("android:camera:front", "capture-raw")
                .expect("hide capture")
        );
        assert!(
            permissions
                .hide_resource("android:touchscreen:primary")
                .expect("hide device")
        );
        assert!(permissions.set_remote_management_enabled(false));
        store.save(&permissions).expect("replace permissions");

        assert!(!store.remote_management_enabled());
        assert!(!store.resource_operation_exposed("android:camera:front", "capture-raw"));
        assert!(store.resource_operation_exposed("android:camera:front", "observe"));
    }

    #[test]
    fn wrong_object_version_and_invalid_resources_fail_closed() {
        let root = sinomo_testdir::TestDir::new("sinomo-node-local-permissions-invalid");
        let store = LocalNodePermissionStore::new(&root);
        let mut permissions = LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        let bytes = serde_ext::to_bytes(
            &permissions,
            LOCAL_NODE_PERMISSIONS_OBJECT_VERSION + 1,
            LengthMode::U32,
        )
        .expect("encode wrong-version state");
        fs::create_dir_all(store.path().parent().expect("permission parent"))
            .expect("create permission parent");
        fs::write(store.path(), bytes).expect("write wrong-version state");

        assert!(store.load().is_err());
        assert!(!store.remote_management_enabled());
        assert!(
            permissions
                .expose_resource_operation(" device:camera:front", "observe")
                .is_err()
        );
        assert!(
            permissions
                .expose_resource_operation("device:camera:front", "capture\nraw")
                .is_err()
        );
    }

    #[test]
    fn writes_replace_atomically_without_leaving_temps() {
        let root = sinomo_testdir::TestDir::new("sinomo-node-local-permissions-atomic");
        let store = LocalNodePermissionStore::new(&root);
        let mut permissions = LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        store.save(&permissions).expect("first save");
        permissions.set_remote_management_enabled(false);
        store.save(&permissions).expect("replacement save");

        assert_eq!(store.load().expect("load replacement"), permissions);
        let entries = fs::read_dir(store.path().parent().expect("permission parent"))
            .expect("list permission parent")
            .map(|entry| entry.expect("permission entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            entries,
            vec![store.path().file_name().expect("file name").to_os_string()]
        );

        #[cfg(unix)]
        {
            let mode = fs::metadata(store.path())
                .expect("permission metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn mutation_lock_serializes_local_permission_edits() {
        use std::sync::mpsc;
        use std::time::Duration;

        let root = sinomo_testdir::TestDir::new("sinomo-node-local-permissions-mutate-lock");
        let store = LocalNodePermissionStore::new(&root);
        let first_store = store.clone();
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first = std::thread::spawn(move || {
            first_store
                .mutate(|permissions| {
                    first_entered_tx.send(()).expect("signal first lock");
                    release_first_rx.recv().expect("release first lock");
                    Ok(permissions.set_remote_management_enabled(true))
                })
                .expect("first mutation")
        });
        first_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first mutation entered");

        let second_store = store.clone();
        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second = std::thread::spawn(move || {
            second_started_tx.send(()).expect("signal second start");
            let outcome = second_store
                .mutate(|permissions| {
                    permissions.expose_resource_operation("device:camera:front", "observe")
                })
                .expect("second mutation");
            second_done_tx.send(()).expect("signal second done");
            outcome
        });
        second_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second mutation started");
        assert!(
            second_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "the second mutation must wait for the first transaction lock"
        );

        release_first_tx.send(()).expect("release first mutation");
        first.join().expect("join first mutation");
        second.join().expect("join second mutation");

        let permissions = store.load().expect("load serialized mutations");
        assert!(permissions.remote_management_enabled());
        assert!(permissions.resource_operation_exposed("device:camera:front", "observe"));
    }

    #[test]
    fn storage_layout_registers_the_exact_permission_object() {
        let mut layout = sinomo_storage::StorageLayout::new();
        crate::register_storage_paths(&mut layout).expect("register node storage");

        let contract = layout
            .files
            .iter()
            .find(|contract| contract.relative_path == LOCAL_NODE_PERMISSIONS_RELATIVE_PATH)
            .expect("local permission contract");
        assert_eq!(contract.module, "sinomo-node");
        assert_eq!(contract.ring, sinomo_storage::StorageRing::R0);
        let schema = contract.schema.as_ref().expect("permission object schema");
        assert_eq!(schema.type_name, "LocalNodePermissions");
        assert_eq!(schema.version_id, LOCAL_NODE_PERMISSIONS_OBJECT_VERSION);
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["grants", "resource_exposures"]
        );
        assert!(
            layout
                .files
                .iter()
                .any(|contract| contract.relative_path == LOCAL_NODE_PERMISSIONS_LOCK_RELATIVE_PATH),
            "the durable mutation lock must be part of the storage contract"
        );
    }
}
