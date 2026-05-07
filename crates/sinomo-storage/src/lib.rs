//! Storage layout and object schema contracts for the Sinomo data directory.
//!
//! R0/R1 crates should register every file they own here, or explicitly mark a
//! subtree as an escape path when user or external-system content may appear
//! there without a detailed schema.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
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

pub fn sinomo_data_dir_layout() -> Result<StorageLayout, StorageContractError> {
    let mut layout = StorageLayout::new();

    register_object(
        &mut layout,
        "sinomo-network",
        StorageRing::R1,
        "cluster/state.sobj",
        ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("cluster_id", "ClusterId")
            .field("local_node_id", "NodeId")
            .field("members", "Vec<ClusterMemberRecord>"),
    )?;
    register_object(
        &mut layout,
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-snapshot.sobj",
        ObjectSchema::new("KvSnapshot", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("revision", "KvRevision")
            .field("entries", "Vec<KvEntry>")
            .field("events", "Vec<KvEvent>"),
    )?;
    register_object(
        &mut layout,
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/hard-state.sobj",
        ObjectSchema::new("RaftHardState", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    register_object(
        &mut layout,
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/snapshot.sobj",
        ObjectSchema::new("RaftSnapshot", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    register_object_log(
        &mut layout,
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/wal.sobjlog",
    )?;
    register_object(
        &mut layout,
        "sinomo-network",
        StorageRing::R1,
        "cluster/watch-events.sobj",
        ObjectSchema::new(
            "Vec<ClusterKvWatchEventRecord>",
            STORAGE_CONTRACT_OBJECT_VERSION,
        ),
    )?;
    for relative_path in ["daemon/status.sobj", "shadow-daemon/status.sobj"] {
        register_object(
            &mut layout,
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("DaemonStatus", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("pid", "u32")
                .field("address", "String")
                .field("started_at", "String")
                .field("cluster", "Option<DaemonClusterStatus>"),
        )?;
    }
    register_object(
        &mut layout,
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/state.sobj",
        ObjectSchema::new("ClusterNodeRolloutStatus", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("status", "String")
            .field("mode", "Option<String>")
            .field("detail", "Option<String>"),
    )?;
    register_object(
        &mut layout,
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/last-main-exit.sobj",
        ObjectSchema::new("RolloutMainExitRecord", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("exit_code", "Option<i32>")
            .field("signal", "Option<i32>")
            .field("requested_shutdown", "bool")
            .field("kind", "MainExitKind")
            .field("observed_at", "String"),
    )?;
    register_object(
        &mut layout,
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/pending-main-rollout.sobj",
        ObjectSchema::new("RolloutMainActivation", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    register_object(
        &mut layout,
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/pending-shadow-rollout.sobj",
        ObjectSchema::new("PendingShadowRollout", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;

    for relative_path in ["identity/main-ed25519.sobj", "identity/shadow-ed25519.sobj"] {
        register_object(
            &mut layout,
            "sinomo-node",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new("NodeIdentity", STORAGE_CONTRACT_OBJECT_VERSION)
                .field("public_key", "String")
                .field("private_key_pkcs8", "String")
                .field("created_at", "String"),
        )?;
    }

    for relative_path in ["daemon/daemon.lock", "shadow-daemon/daemon.lock"] {
        layout.register_file(StorageFileContract::binary(
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
        )?)?;
    }
    for relative_path in ["daemon/auth-nonces.sobj", "shadow-daemon/auth-nonces.sobj"] {
        register_object(
            &mut layout,
            "sinomo-daemon",
            StorageRing::R1,
            relative_path,
            ObjectSchema::new(
                "BTreeMap<DaemonAuthNonceKey, Timestamp>",
                STORAGE_CONTRACT_OBJECT_VERSION,
            ),
        )?;
    }

    for (relative_path, reason) in [
        (
            "cluster/iroh",
            "dynamic iroh key and endpoint object files keyed by node id",
        ),
        (
            "cluster/watches",
            "dynamic cluster KV watch registration object files keyed by watch id",
        ),
        ("dfs/content/blobs", "content-addressed DFS blob bytes"),
        (
            "dfs/content/objects",
            "dynamic DFS object manifests stored as sinomo-object files",
        ),
        ("dfs/content/staging", "temporary DFS staging bytes"),
        (
            "dfs/metadata/records",
            "dynamic DFS metadata records stored as sinomo-object files",
        ),
        (
            "dfs/metadata/journal",
            "dynamic DFS metadata journal entries stored as sinomo-object files",
        ),
        (
            "dfs/metadata/root",
            "default DFS root metadata shard with object records and journal entries",
        ),
        (
            "dfs/operator",
            "dynamic DFS operator intent records stored as sinomo-object files",
        ),
        (
            "dfs/pins",
            "dynamic DFS pin records stored as sinomo-object files",
        ),
        ("files", "local-owned user files exposed into DFS"),
        ("workspace", "user-authored workspace files"),
        ("source", "managed or imported source repositories"),
        ("bin", "installed runtime binaries"),
        ("rollout/staged", "staged rollout build artifacts"),
        (
            "rollout/logs",
            "operator-readable rollout logs and diagnostics",
        ),
        ("config", "human-readable local configuration files"),
        (
            "providers",
            "external provider credential and configuration files",
        ),
        (
            "evolution",
            "self-evolution jobs, campaigns, logs, and candidate workspaces",
        ),
        ("startup", "installed OS boot entries and launch scripts"),
    ] {
        layout.register_escape(StorageEscapePath::new(
            owner_for_escape(relative_path),
            ring_for_escape(relative_path),
            relative_path,
            reason,
        )?)?;
    }

    Ok(layout)
}

pub fn verify_sinomo_data_dir(
    data_dir: impl AsRef<Path>,
) -> Result<StorageVerificationReport, StorageContractError> {
    sinomo_data_dir_layout()?.verify_data_dir(data_dir)
}

pub fn ensure_sinomo_data_dir_is_registered(
    data_dir: impl AsRef<Path>,
) -> Result<(), StorageContractError> {
    let report = verify_sinomo_data_dir(data_dir)?;
    if report.is_complete() {
        Ok(())
    } else {
        Err(StorageContractError::UnregisteredPaths {
            paths: report.unregistered,
        })
    }
}

fn register_object(
    layout: &mut StorageLayout,
    module: &'static str,
    ring: StorageRing,
    relative_path: &'static str,
    schema: ObjectSchema,
) -> Result<(), StorageContractError> {
    layout.register_file(StorageFileContract::sinomo_object(
        module,
        ring,
        relative_path,
        schema,
    )?)
}

fn register_object_log(
    layout: &mut StorageLayout,
    module: &'static str,
    ring: StorageRing,
    relative_path: &'static str,
) -> Result<(), StorageContractError> {
    layout.register_file(StorageFileContract::sinomo_object(
        module,
        ring,
        relative_path,
        ObjectSchema::new(
            "Vec<LengthPrefixedSinomoObject>",
            STORAGE_CONTRACT_OBJECT_VERSION,
        ),
    )?)
}

fn owner_for_escape(relative_path: &str) -> &'static str {
    if relative_path.starts_with("cluster/") {
        "sinomo-network"
    } else if relative_path.starts_with("dfs/") || relative_path == "files" {
        "sinomo-dfs"
    } else if relative_path.starts_with("rollout/")
        || relative_path == "source"
        || relative_path == "bin"
    {
        "sinomo-rollout"
    } else if relative_path == "startup" {
        "sinomo-cli"
    } else if relative_path == "config" || relative_path == "providers" {
        "sinomo-local"
    } else if relative_path == "evolution" {
        "sinomo-evolution"
    } else {
        "sinomo-workspace"
    }
}

fn ring_for_escape(relative_path: &str) -> StorageRing {
    if relative_path.starts_with("cluster/") || relative_path.starts_with("dfs/") {
        StorageRing::R1
    } else if relative_path == "files" {
        StorageRing::R1
    } else if relative_path == "startup" {
        StorageRing::Application
    } else if relative_path == "config" || relative_path == "providers" {
        StorageRing::Application
    } else if relative_path == "evolution" {
        StorageRing::R2
    } else {
        StorageRing::R2
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoragePathOwner<'a> {
    File(&'a StorageFileContract),
    Escape(&'a StorageEscapePath),
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
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            _ => {
                return Err(StorageContractError::InvalidRelativePath {
                    path: path.display().to_string(),
                });
            }
        }
    }
    normalized.to_str().map(str::to_string).ok_or_else(|| {
        StorageContractError::InvalidRelativePath {
            path: path.display().to_string(),
        }
    })
}

fn path_has_prefix(relative_path: &str, prefix: &str) -> bool {
    relative_path == prefix
        || relative_path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
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
    fn default_layout_registers_core_data_dir_contracts() {
        let layout = sinomo_data_dir_layout().expect("layout");

        let Some(StoragePathOwner::File(cluster_state)) = layout.owner_for("cluster/state.sobj")
        else {
            panic!("expected cluster state contract");
        };
        assert_eq!(cluster_state.module, "sinomo-network");
        assert_eq!(cluster_state.encoding, StorageEncoding::SinomoObject);

        let Some(StoragePathOwner::File(daemon_status)) = layout.owner_for("daemon/status.sobj")
        else {
            panic!("expected daemon status contract");
        };
        assert_eq!(daemon_status.encoding, StorageEncoding::SinomoObject);

        let Some(StoragePathOwner::Escape(dfs_record)) =
            layout.owner_for("dfs/metadata/records/objects/01/object.sobj")
        else {
            panic!("expected dfs metadata escape");
        };
        assert_eq!(dfs_record.module, "sinomo-dfs");

        let Some(StoragePathOwner::Escape(dfs_root_record)) =
            layout.owner_for("dfs/metadata/root/records/filesystems/root/object.sobj")
        else {
            panic!("expected dfs root metadata escape");
        };
        assert_eq!(dfs_root_record.module, "sinomo-dfs");

        let Some(StoragePathOwner::Escape(dfs_pin)) =
            layout.owner_for("dfs/pins/root-hash-node.sobj")
        else {
            panic!("expected dfs pin escape");
        };
        assert_eq!(dfs_pin.module, "sinomo-dfs");

        let Some(StoragePathOwner::Escape(workspace)) =
            layout.owner_for("workspace/project/README.md")
        else {
            panic!("expected workspace escape");
        };
        assert_eq!(workspace.module, "sinomo-workspace");

        let Some(StoragePathOwner::Escape(config)) = layout.owner_for("config/models.json") else {
            panic!("expected config escape");
        };
        assert_eq!(config.module, "sinomo-local");

        let Some(StoragePathOwner::Escape(provider)) =
            layout.owner_for("providers/openai/credentials.json")
        else {
            panic!("expected provider escape");
        };
        assert_eq!(provider.module, "sinomo-local");

        let Some(StoragePathOwner::Escape(evolution)) =
            layout.owner_for("evolution/jobs/01hxy6qa9k0t9vr4g9n8d3q5d4/record.json")
        else {
            panic!("expected evolution escape");
        };
        assert_eq!(evolution.module, "sinomo-evolution");
    }

    #[test]
    fn verifier_reports_unregistered_relative_paths() {
        let layout = sinomo_data_dir_layout().expect("layout");
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
    fn verifier_walks_data_dir_files() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-storage-verify-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
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
        std::fs::write(root.join("rollout/logs/candidate.diagnostics.json"), b"{}")
            .expect("rollout diagnostics");
        std::fs::write(root.join("startup/sinomo.service"), b"service").expect("startup");
        std::fs::write(root.join("unknown/file.bin"), b"unknown").expect("unknown");

        let report = sinomo_data_dir_layout()
            .expect("layout")
            .verify_data_dir(&root)
            .expect("verify");

        assert_eq!(report.unregistered, vec!["unknown/file.bin"]);
        assert_eq!(report.covered.len(), 9);
        std::fs::remove_dir_all(root).ok();
    }
}
