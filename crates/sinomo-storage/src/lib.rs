//! Storage path registry and object schema contracts for the Sinomo data directory.
//!
//! This crate defines the shared vocabulary and registry mechanics. Crates that
//! own data-dir files should register their own files or escape subtrees from
//! their crate, so storage usage can be inspected without hard-coding every
//! downstream path here.

pub mod durable_file;

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::RwLock;
use thiserror::Error;

pub const STORAGE_CONTRACT_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageRing {
    R0,
    R1,
    R2,
    Application,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageEncoding {
    SinomoObject,
    Binary,
    HumanText,
    External,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectSchema {
    pub type_name: String,
    pub version_id: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<ObjectFieldSchema>,
}

impl ObjectSchema {
    pub fn new(type_name: impl Into<String>, version_id: u16) -> Self {
        Self {
            type_name: type_name.into(),
            version_id,
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, name: impl Into<String>, type_name: impl Into<String>) -> Self {
        self.fields.push(ObjectFieldSchema {
            name: name.into(),
            type_name: type_name.into(),
        });
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectFieldSchema {
    pub name: String,
    pub type_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageFileContract {
    pub module: String,
    pub ring: StorageRing,
    pub relative_path: String,
    pub encoding: StorageEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<ObjectSchema>,
}

impl StorageFileContract {
    pub fn sinomo_object(
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
        schema: ObjectSchema,
    ) -> Result<Self, StorageContractError> {
        Self::new(
            module,
            ring,
            relative_path,
            StorageEncoding::SinomoObject,
            Some(schema),
        )
    }

    pub fn binary(
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
    ) -> Result<Self, StorageContractError> {
        Self::new(module, ring, relative_path, StorageEncoding::Binary, None)
    }

    pub fn human_text(
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
    ) -> Result<Self, StorageContractError> {
        Self::new(
            module,
            ring,
            relative_path,
            StorageEncoding::HumanText,
            None,
        )
    }

    fn new(
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
        encoding: StorageEncoding,
        schema: Option<ObjectSchema>,
    ) -> Result<Self, StorageContractError> {
        let contract = Self {
            module: module.into(),
            ring,
            relative_path: relative_path.into(),
            encoding,
            schema,
        };
        validate_relative_path(&contract.relative_path)?;
        Ok(contract)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageEscapePath {
    pub module: String,
    pub ring: StorageRing,
    pub relative_path: String,
    pub reason: String,
}

impl StorageEscapePath {
    pub fn new(
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self, StorageContractError> {
        let escape = Self {
            module: module.into(),
            ring,
            relative_path: relative_path.into(),
            reason: reason.into(),
        };
        validate_relative_path(&escape.relative_path)?;
        Ok(escape)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageLayout {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<StorageFileContract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escape_paths: Vec<StorageEscapePath>,
}

impl StorageLayout {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_file(
        &mut self,
        contract: StorageFileContract,
    ) -> Result<(), StorageContractError> {
        if self
            .files
            .iter()
            .any(|existing| existing.relative_path == contract.relative_path)
        {
            return Err(StorageContractError::DuplicatePath {
                relative_path: contract.relative_path.to_string(),
            });
        }
        self.files.push(contract);
        Ok(())
    }

    pub fn register_sinomo_object(
        &mut self,
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
        schema: ObjectSchema,
    ) -> Result<(), StorageContractError> {
        self.register_file(StorageFileContract::sinomo_object(
            module,
            ring,
            relative_path,
            schema,
        )?)
    }

    pub fn register_object_log(
        &mut self,
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
    ) -> Result<(), StorageContractError> {
        self.register_sinomo_object(
            module,
            ring,
            relative_path,
            ObjectSchema::new(
                "Vec<LengthPrefixedSinomoObject>",
                STORAGE_CONTRACT_OBJECT_VERSION,
            ),
        )
    }

    pub fn register_binary(
        &mut self,
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
    ) -> Result<(), StorageContractError> {
        self.register_file(StorageFileContract::binary(module, ring, relative_path)?)
    }

    pub fn register_human_text(
        &mut self,
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
    ) -> Result<(), StorageContractError> {
        self.register_file(StorageFileContract::human_text(
            module,
            ring,
            relative_path,
        )?)
    }

    pub fn register_escape(
        &mut self,
        escape: StorageEscapePath,
    ) -> Result<(), StorageContractError> {
        if self
            .escape_paths
            .iter()
            .any(|existing| existing.relative_path == escape.relative_path)
        {
            return Err(StorageContractError::DuplicatePath {
                relative_path: escape.relative_path.to_string(),
            });
        }
        self.escape_paths.push(escape);
        Ok(())
    }

    pub fn register_escape_path(
        &mut self,
        module: impl Into<String>,
        ring: StorageRing,
        relative_path: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), StorageContractError> {
        self.register_escape(StorageEscapePath::new(module, ring, relative_path, reason)?)
    }

    pub fn unregister_file(&mut self, relative_path: impl AsRef<Path>) -> bool {
        let Ok(relative_path) = normalize_relative_path(relative_path.as_ref()) else {
            return false;
        };
        let before = self.files.len();
        self.files
            .retain(|existing| existing.relative_path != relative_path);
        self.files.len() != before
    }

    pub fn unregister_escape(&mut self, relative_path: impl AsRef<Path>) -> bool {
        let Ok(relative_path) = normalize_relative_path(relative_path.as_ref()) else {
            return false;
        };
        let before = self.escape_paths.len();
        self.escape_paths
            .retain(|existing| existing.relative_path != relative_path);
        self.escape_paths.len() != before
    }

    pub fn unregister_path(&mut self, relative_path: impl AsRef<Path>) -> bool {
        let relative_path = relative_path.as_ref();
        self.unregister_file(relative_path) | self.unregister_escape(relative_path)
    }

    pub fn owner_for(&self, relative_path: impl AsRef<Path>) -> Option<StoragePathOwner<'_>> {
        let relative_path = relative_path.as_ref();
        let relative = normalize_relative_path(relative_path).ok()?;
        if let Some(file) = self
            .files
            .iter()
            .find(|file| file.relative_path.as_ref() == relative)
        {
            return Some(StoragePathOwner::File(file));
        }
        self.escape_paths
            .iter()
            .find(|escape| path_has_prefix(&relative, &escape.relative_path))
            .map(StoragePathOwner::Escape)
    }

    pub fn verify_relative_paths<I, P>(
        &self,
        relative_paths: I,
    ) -> Result<StorageVerificationReport, StorageContractError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut report = StorageVerificationReport::default();
        for relative_path in relative_paths {
            let relative_path = normalize_relative_path(relative_path.as_ref())?;
            match self.owner_for(&relative_path) {
                Some(StoragePathOwner::File(contract)) => {
                    report.covered.push(VerifiedStoragePath {
                        relative_path,
                        module: contract.module.clone(),
                        ring: contract.ring,
                        coverage: StorageCoverage::File,
                    });
                }
                Some(StoragePathOwner::Escape(escape)) => {
                    report.covered.push(VerifiedStoragePath {
                        relative_path,
                        module: escape.module.clone(),
                        ring: escape.ring,
                        coverage: StorageCoverage::Escape,
                    });
                }
                None => report.unregistered.push(relative_path),
            }
        }
        Ok(report)
    }

    pub fn verify_data_dir(
        &self,
        root: impl AsRef<Path>,
    ) -> Result<StorageVerificationReport, StorageContractError> {
        let root = root.as_ref();
        let mut files = Vec::new();
        collect_files(root, root, &mut files)?;
        self.verify_relative_paths(files)
    }
}

#[derive(Debug, Default)]
pub struct StorageRegistry {
    layout: RwLock<StorageLayout>,
}

impl StorageRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_layout(layout: StorageLayout) -> Self {
        Self {
            layout: RwLock::new(layout),
        }
    }

    pub fn register_file(&self, contract: StorageFileContract) -> Result<(), StorageContractError> {
        self.layout
            .write()
            .map_err(|_| StorageContractError::RegistryPoisoned)?
            .register_file(contract)
    }

    pub fn register_escape(&self, escape: StorageEscapePath) -> Result<(), StorageContractError> {
        self.layout
            .write()
            .map_err(|_| StorageContractError::RegistryPoisoned)?
            .register_escape(escape)
    }

    pub fn unregister_path(
        &self,
        relative_path: impl AsRef<Path>,
    ) -> Result<bool, StorageContractError> {
        Ok(self
            .layout
            .write()
            .map_err(|_| StorageContractError::RegistryPoisoned)?
            .unregister_path(relative_path))
    }

    pub fn layout(&self) -> Result<StorageLayout, StorageContractError> {
        self.layout
            .read()
            .map_err(|_| StorageContractError::RegistryPoisoned)
            .map(|layout| layout.clone())
    }

    pub fn owner_for(
        &self,
        relative_path: impl AsRef<Path>,
    ) -> Result<Option<StoragePathOwnerOwned>, StorageContractError> {
        let layout = self
            .layout
            .read()
            .map_err(|_| StorageContractError::RegistryPoisoned)?;
        Ok(layout.owner_for(relative_path).map(Into::into))
    }

    pub fn verify_relative_paths<I, P>(
        &self,
        relative_paths: I,
    ) -> Result<StorageVerificationReport, StorageContractError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        self.layout
            .read()
            .map_err(|_| StorageContractError::RegistryPoisoned)?
            .verify_relative_paths(relative_paths)
    }

    pub fn verify_data_dir(
        &self,
        root: impl AsRef<Path>,
    ) -> Result<StorageVerificationReport, StorageContractError> {
        self.layout
            .read()
            .map_err(|_| StorageContractError::RegistryPoisoned)?
            .verify_data_dir(root)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageVerificationReport {
    pub covered: Vec<VerifiedStoragePath>,
    pub unregistered: Vec<String>,
}

impl StorageVerificationReport {
    pub fn is_complete(&self) -> bool {
        self.unregistered.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedStoragePath {
    pub relative_path: String,
    pub module: String,
    pub ring: StorageRing,
    pub coverage: StorageCoverage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageCoverage {
    File,
    Escape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoragePathOwner<'a> {
    File(&'a StorageFileContract),
    Escape(&'a StorageEscapePath),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoragePathOwnerOwned {
    File(StorageFileContract),
    Escape(StorageEscapePath),
}

impl From<StoragePathOwner<'_>> for StoragePathOwnerOwned {
    fn from(owner: StoragePathOwner<'_>) -> Self {
        match owner {
            StoragePathOwner::File(file) => Self::File(file.clone()),
            StoragePathOwner::Escape(escape) => Self::Escape(escape.clone()),
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum StorageContractError {
    #[error("storage path must be relative and normalized: {path}")]
    InvalidRelativePath { path: String },
    #[error("storage path is registered more than once: {relative_path}")]
    DuplicatePath { relative_path: String },
    #[error("unregistered files in sinomo data directory: {}", paths.join(", "))]
    UnregisteredPaths { paths: Vec<String> },
    #[error("storage verifier I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("storage registry lock is poisoned")]
    RegistryPoisoned,
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), StorageContractError> {
    let entries = fs::read_dir(current).map_err(|source| StorageContractError::Io {
        path: current.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| StorageContractError::Io {
            path: current.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|source| StorageContractError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root).map_err(|source| {
                StorageContractError::InvalidRelativePath {
                    path: source.to_string(),
                }
            })?;
            files.push(relative.to_path_buf());
        }
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), StorageContractError> {
    normalize_relative_path(Path::new(path)).map(|_| ())
}

fn normalize_relative_path(path: &Path) -> Result<String, StorageContractError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(StorageContractError::InvalidRelativePath {
            path: path.display().to_string(),
        });
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                parts.push(value.to_str().ok_or_else(|| {
                    StorageContractError::InvalidRelativePath {
                        path: path.display().to_string(),
                    }
                })?);
            }
            _ => {
                return Err(StorageContractError::InvalidRelativePath {
                    path: path.display().to_string(),
                });
            }
        }
    }
    Ok(parts.join("/"))
}

fn path_has_prefix(relative_path: &str, prefix: &str) -> bool {
    relative_path == prefix
        || relative_path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/') || rest.starts_with('\\'))
}

impl fmt::Display for StoragePathOwner<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(file) => write!(formatter, "{} owns {}", file.module, file.relative_path),
            Self::Escape(escape) => {
                write!(
                    formatter,
                    "{} escapes {}",
                    escape.module, escape.relative_path
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_tracks_registered_file_owner_and_schema() {
        let mut layout = StorageLayout::new();
        layout
            .register_file(
                StorageFileContract::sinomo_object(
                    "sinomo-kv",
                    StorageRing::R1,
                    "cluster/kv-snapshot.sobj",
                    ObjectSchema::new("KvSnapshot", 1)
                        .field("revision", "KvRevision")
                        .field("entries", "Vec<KvEntry>"),
                )
                .expect("contract"),
            )
            .expect("register");

        let Some(StoragePathOwner::File(contract)) = layout.owner_for("cluster/kv-snapshot.sobj")
        else {
            panic!("expected file owner");
        };
        assert_eq!(contract.module, "sinomo-kv");
        assert_eq!(contract.encoding, StorageEncoding::SinomoObject);
        assert_eq!(
            contract.schema.as_ref().expect("schema").type_name,
            "KvSnapshot"
        );
    }

    #[test]
    fn escape_paths_cover_open_subtrees() {
        let mut layout = StorageLayout::new();
        layout
            .register_escape(
                StorageEscapePath::new(
                    "sinomo-workspace",
                    StorageRing::R2,
                    "workspace",
                    "user-authored files",
                )
                .expect("escape"),
            )
            .expect("register escape");

        let Some(StoragePathOwner::Escape(escape)) = layout.owner_for("workspace/project/notes.md")
        else {
            panic!("expected escape owner");
        };
        assert_eq!(escape.reason, "user-authored files");
        assert!(layout.owner_for("workspace-not-covered/file.txt").is_none());
    }

    #[test]
    fn rejects_absolute_or_parent_relative_paths() {
        assert!(matches!(
            StorageFileContract::binary("sinomo-kv", StorageRing::R1, "../outside"),
            Err(StorageContractError::InvalidRelativePath { .. })
        ));
        assert!(matches!(
            StorageEscapePath::new("sinomo-kv", StorageRing::R1, "/absolute", "invalid"),
            Err(StorageContractError::InvalidRelativePath { .. })
        ));
    }

    #[test]
    fn verifier_reports_unregistered_relative_paths() {
        let mut layout = StorageLayout::new();
        layout
            .register_sinomo_object(
                "sinomo-network",
                StorageRing::R1,
                "cluster/state.sobj",
                ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION),
            )
            .expect("register state");
        layout
            .register_escape_path(
                "sinomo-network",
                StorageRing::R1,
                "cluster/iroh",
                "dynamic iroh endpoint records",
            )
            .expect("register iroh");
        let report = layout
            .verify_relative_paths([
                "cluster/state.sobj",
                "cluster/iroh/01.secret.sobj",
                "unowned/file.bin",
            ])
            .expect("verify");

        assert!(!report.is_complete());
        assert_eq!(report.unregistered, vec!["unowned/file.bin"]);
        assert!(report.covered.iter().any(|path| {
            path.relative_path == "cluster/state.sobj" && path.coverage == StorageCoverage::File
        }));
        assert!(report.covered.iter().any(|path| {
            path.relative_path == "cluster/iroh/01.secret.sobj"
                && path.coverage == StorageCoverage::Escape
        }));
    }

    #[test]
    fn registry_supports_runtime_registration_and_deregistration() {
        let registry = StorageRegistry::new();
        registry
            .register_file(
                StorageFileContract::human_text(
                    "sinomo-rollout",
                    StorageRing::R1,
                    "rollout/last-main-exit.json",
                )
                .expect("contract"),
            )
            .expect("register");

        let owner = registry
            .owner_for("rollout/last-main-exit.json")
            .expect("owner")
            .expect("registered");
        match owner {
            StoragePathOwnerOwned::File(contract) => {
                assert_eq!(contract.module, "sinomo-rollout");
                assert_eq!(contract.encoding, StorageEncoding::HumanText);
            }
            StoragePathOwnerOwned::Escape(_) => panic!("expected file owner"),
        }

        assert!(
            registry
                .unregister_path("rollout/last-main-exit.json")
                .expect("unregister")
        );
        assert!(
            registry
                .owner_for("rollout/last-main-exit.json")
                .expect("owner")
                .is_none()
        );
    }

    #[test]
    fn verifier_covers_dynamic_cluster_raft_node_files() {
        let mut layout = StorageLayout::new();
        layout
            .register_escape_path(
                "sinomo-kv",
                StorageRing::R1,
                "cluster/kv-raft",
                "durable raft node subdirectories",
            )
            .expect("register raft escape");
        let report = layout
            .verify_relative_paths([
                "cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/node.lock",
                "cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/wal.sobjlog",
                "cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/snapshot.sobj",
                "cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/hard-state.sobj",
            ])
            .expect("verify");

        assert!(report.unregistered.is_empty());
        assert_eq!(report.covered.len(), 4);
        assert!(report.covered.iter().all(|path| {
            path.module == "sinomo-kv" && path.coverage == StorageCoverage::Escape
        }));
    }

    #[test]
    fn verifier_covers_windows_style_escape_paths() {
        let mut layout = StorageLayout::new();
        layout
            .register_escape_path(
                "sinomo-rollout",
                StorageRing::R1,
                "bin",
                "installed runtime binaries",
            )
            .expect("register bin");
        layout
            .register_escape_path(
                "sinomo-rollout",
                StorageRing::R1,
                "source",
                "managed source repositories",
            )
            .expect("register source");
        let report = layout
            .verify_relative_paths([
                "bin\\sinomo.exe",
                "bin\\sinomo-shadow.exe",
                "source\\sinomo\\.git\\config",
            ])
            .expect("verify");

        assert!(report.unregistered.is_empty());
        assert_eq!(report.covered.len(), 3);
        assert!(
            report
                .covered
                .iter()
                .all(|path| path.module == "sinomo-rollout")
        );
    }

    #[test]
    fn verifier_walks_data_dir_files_with_registered_escapes() {
        let root = sinomo_testdir::TestDir::new("sinomo-storage-verify");
        std::fs::create_dir_all(root.join("cluster/iroh")).expect("cluster iroh");
        std::fs::create_dir_all(root.join("config")).expect("config");
        std::fs::create_dir_all(root.join("providers/openai")).expect("provider");
        std::fs::create_dir_all(root.join("evolution/jobs/01hxy6qa9k0t9vr4g9n8d3q5d4"))
            .expect("evolution");
        std::fs::create_dir_all(root.join("dfs/metadata/root/records/filesystems/root"))
            .expect("dfs root metadata");
        std::fs::create_dir_all(root.join("dfs/pins")).expect("dfs pins");
        std::fs::create_dir_all(root.join("rollout/logs")).expect("rollout logs");
        std::fs::create_dir_all(root.join("startup")).expect("startup");
        std::fs::create_dir_all(root.join("unknown")).expect("unknown");
        std::fs::write(root.join("cluster/state.sobj"), b"state").expect("state");
        std::fs::write(root.join("cluster/iroh/node.endpoint.sobj"), b"endpoint")
            .expect("endpoint");
        std::fs::write(root.join("config/models.json"), b"{}").expect("model config");
        std::fs::write(root.join("providers/openai/credentials.json"), b"{}")
            .expect("provider config");
        std::fs::write(
            root.join("evolution/jobs/01hxy6qa9k0t9vr4g9n8d3q5d4/record.json"),
            b"{}",
        )
        .expect("evolution job");
        std::fs::write(
            root.join("dfs/metadata/root/records/filesystems/root/object.sobj"),
            b"metadata",
        )
        .expect("dfs metadata");
        std::fs::write(root.join("dfs/pins/root-hash-node.sobj"), b"pin").expect("dfs pin");
        std::fs::write(root.join("rollout/last-main-exit.json"), b"{}")
            .expect("legacy rollout exit");
        std::fs::write(root.join("rollout/logs/candidate.diagnostics.json"), b"{}")
            .expect("rollout diagnostics");
        std::fs::write(root.join("startup/sinomo.service"), b"service").expect("startup");
        std::fs::write(root.join("unknown/file.bin"), b"unknown").expect("unknown");

        let mut layout = StorageLayout::new();
        layout
            .register_sinomo_object(
                "sinomo-network",
                StorageRing::R1,
                "cluster/state.sobj",
                ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION),
            )
            .expect("register state");
        for (module, ring, relative_path, reason) in [
            (
                "sinomo-network",
                StorageRing::R1,
                "cluster/iroh",
                "dynamic iroh records",
            ),
            (
                "sinomo-local",
                StorageRing::Application,
                "config",
                "local config",
            ),
            (
                "sinomo-local",
                StorageRing::Application,
                "providers",
                "provider config",
            ),
            (
                "sinomo-evolution",
                StorageRing::R2,
                "evolution",
                "self-evolution state",
            ),
            (
                "sinomo-dfs",
                StorageRing::R1,
                "dfs/metadata/root",
                "dfs root metadata",
            ),
            ("sinomo-dfs", StorageRing::R1, "dfs/pins", "dfs pins"),
            (
                "sinomo-rollout",
                StorageRing::R1,
                "rollout/logs",
                "rollout logs",
            ),
            (
                "sinomo-daemon",
                StorageRing::Application,
                "startup",
                "installed OS boot entries",
            ),
        ] {
            layout
                .register_escape_path(module, ring, relative_path, reason)
                .expect("register escape");
        }
        layout
            .register_human_text(
                "sinomo-rollout",
                StorageRing::R1,
                "rollout/last-main-exit.json",
            )
            .expect("register rollout exit");

        let report = layout.verify_data_dir(&root).expect("verify");

        assert_eq!(report.unregistered, vec!["unknown/file.bin"]);
        assert_eq!(report.covered.len(), 10);
    }
}
