//! Durable discovery and references to the existing owning conversion jobs.
//! This inventory contains no source cursor, native request or publication proof.
use super::super::*;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext};

const VERSION: u16 = 1;
const MAX_REFERENCE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnitKey {
    pub relative_root: PathBuf,
    pub role: UnitRole,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum UnitRole {
    FileSystem { file_system_id: sinomo_dfs::DfsFileSystemId, source_catalog_generation: u64, target_format: u32 },
    Metadata { shard: sinomo_dfs::DfsMetadataShardId },
    MetadataGeneration { identity: sinomo_dfs::DfsMetadataGenerationIdentity },
    MetadataRouting,
    PlacementGoals,
    ContentRoot,
    ContentVersion { object_id: String, version: String },
    ContentObject { object_id: String },
    ContentBlob { hash: String },
    ContentInventory,
    LocalState,
    RetainedArtifact,
}
impl UnitKey {
    pub fn id(&self) -> Result<String> {
        self.validate()?;
        Ok(blake3::hash(&serde_ext::to_bytes(self, VERSION, LengthMode::U32).map_err(error)?).to_hex().to_string())
    }
    pub fn validate(&self) -> Result<()> {
        if self.relative_root.is_absolute() || self.relative_root.components().any(|part|
            !matches!(part, std::path::Component::Normal(_))) {
            return Err(error("migration unit leaves its actual managed root"));
        }
        let expected = match &self.role {
            UnitRole::Metadata { shard } => {
                validate_component(&shard.0)?;
                PathBuf::from("dfs/metadata").join(&shard.0)
            }
            UnitRole::MetadataGeneration { identity } => {
                identity.validate()?;
                validate_component(&identity.shard_id.0)?;
                PathBuf::from("dfs/metadata").join(&identity.shard_id.0)
            }
            UnitRole::MetadataRouting | UnitRole::PlacementGoals | UnitRole::FileSystem { .. } => PathBuf::from("dfs/metadata"),
            UnitRole::LocalState => PathBuf::from("dfs"),
            UnitRole::RetainedArtifact => {
                if !["dfs/metadata/.metadata-conversion", "dfs/metadata/.metadata-retained", "dfs/.content-retained", "dfs/.content-conversion-v2", "dfs/content/.content-conversion-v2", "daemon/dfs-migration/receivers"]
                    .iter().any(|prefix| self.relative_root.starts_with(prefix)) {
                    return Err(error("retained artifact is outside declared migration storage"));
                }
                self.relative_root.clone()
            },
            UnitRole::ContentRoot | UnitRole::ContentVersion { .. } | UnitRole::ContentObject { .. }
                | UnitRole::ContentBlob { .. } | UnitRole::ContentInventory => PathBuf::from("dfs/content"),
        };
        if self.relative_root != expected { return Err(error("migration unit does not name its declared managed role root")); }
        let role_bytes = serde_ext::to_bytes(&self.role, VERSION, LengthMode::U32).map_err(error)?;
        if role_bytes.len() > 48 * 1024 { return Err(error("migration unit role exceeds its bounded identity")); }
        match &self.role {
            UnitRole::ContentVersion { object_id, version } if object_id.is_empty() || version.is_empty() => return Err(error("content unit identity is empty")),
            UnitRole::ContentObject { object_id } if object_id.is_empty() => return Err(error("content object unit identity is empty")),
            UnitRole::ContentBlob { hash } if hash.is_empty() => return Err(error("content blob unit identity is empty")),
            _ => {},
        }
        Ok(())
    }
    pub fn scope(&self) -> Option<DfsStoragePreparationScope> {
        Some(match &self.role {
            UnitRole::Metadata { shard } => DfsStoragePreparationScope::Metadata(shard.clone()),
            UnitRole::MetadataGeneration { identity } => DfsStoragePreparationScope::MetadataGeneration { identity: identity.clone() },
            UnitRole::MetadataRouting => DfsStoragePreparationScope::MetadataRouting,
            UnitRole::ContentRoot => DfsStoragePreparationScope::Content,
            UnitRole::ContentVersion { object_id, version } => DfsStoragePreparationScope::ContentVersion { object_id: object_id.clone(), version: version.clone() },
            UnitRole::ContentObject { object_id } => DfsStoragePreparationScope::ContentObject { object_id: object_id.clone() },
            UnitRole::ContentBlob { hash } => DfsStoragePreparationScope::ContentBlob { hash: hash.clone() },
            UnitRole::ContentInventory => DfsStoragePreparationScope::ContentInventory,
            UnitRole::LocalState => DfsStoragePreparationScope::LocalState,
            UnitRole::RetainedArtifact | UnitRole::PlacementGoals | UnitRole::FileSystem { .. } => return None,
        })
    }
    pub fn from_scope(data_root: &Path, root: &Path, scope: &DfsStoragePreparationScope) -> Result<Self> {
        let relative_root = root.strip_prefix(data_root).map_err(|_| error("migration demand belongs to another data owner"))?.to_owned();
        let role = match scope {
            DfsStoragePreparationScope::Metadata(shard) => UnitRole::Metadata { shard: shard.clone() },
            DfsStoragePreparationScope::MetadataGeneration { identity } => UnitRole::MetadataGeneration { identity: identity.clone() },
            DfsStoragePreparationScope::MetadataRouting => UnitRole::MetadataRouting,
            DfsStoragePreparationScope::Content => UnitRole::ContentRoot,
            DfsStoragePreparationScope::ContentVersion { object_id, version } => UnitRole::ContentVersion { object_id: object_id.clone(), version: version.clone() },
            DfsStoragePreparationScope::ContentObject { object_id } => UnitRole::ContentObject { object_id: object_id.clone() },
            DfsStoragePreparationScope::ContentBlob { hash } => UnitRole::ContentBlob { hash: hash.clone() },
            DfsStoragePreparationScope::ContentInventory => UnitRole::ContentInventory,
            DfsStoragePreparationScope::LocalState => UnitRole::LocalState,
        };
        let unit = Self { relative_root, role };
        unit.validate()?;
        Ok(unit)
    }
}

fn validate_component(value: &str) -> Result<()> {
    let path = Path::new(value);
    if value.contains(['\\', ':', '\0']) || path.components().count() != 1
        || !matches!(path.components().next(), Some(std::path::Component::Normal(_))) {
        return Err(error("metadata unit has an invalid physical scope"));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JobReference {
    pub checkpoint: PathBuf,
    pub identity: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum ObservedState {
    Discovered,
    Runnable,
    Waiting { evidence: String },
    Blocked { evidence: String },
    /// Revalidate the current unit on recovery; this observation is not a permit.
    VerifiedCurrent,
    /// Completed job evidence retained in its original representation; no serving or GC permit.
    RetainedOriginal,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnitReference {
    pub version: u16,
    pub unit: UnitKey,
    pub job: Option<JobReference>,
    pub state: ObservedState,
}
impl UnitReference {
    pub fn discovered(unit: UnitKey) -> Self { Self { version: VERSION, unit, job: None, state: ObservedState::Discovered } }
    pub fn validate(&self) -> Result<()> {
        self.unit.validate()?;
        if self.version != VERSION { return Err(error("unknown migration inventory reference format")); }
        if let Some(job) = &self.job {
            if job.identity.is_empty() || job.identity.len() > 4096 || job.identity.contains('\0')
                || job.checkpoint.as_os_str().is_empty() || job.checkpoint.is_absolute()
                || job.checkpoint.components().any(|part| !matches!(part, std::path::Component::Normal(_)))
                || !(job.checkpoint.starts_with("dfs") || job.checkpoint.starts_with("daemon/dfs-migration")) {
                return Err(error("migration job reference leaves its managed owner"));
            }
        }
        if matches!(&self.state, ObservedState::Waiting { evidence } | ObservedState::Blocked { evidence } if evidence.len() > 8192) {
            return Err(error("migration evidence reference exceeds its bound"));
        }
        if serde_ext::to_bytes(self, VERSION, LengthMode::U32).map_err(error)?.len() as u64 > MAX_REFERENCE_BYTES {
            return Err(error("migration unit reference exceeds its bounded DATA contract"));
        }
        Ok(())
    }
}

pub(super) struct Inventory {
    root: PathBuf,
}
impl Inventory {
    pub fn open(data_root: &Path) -> Result<Self> {
        let root = data_root.join("dfs/migration-owner");
        for path in [data_root.join("dfs"), root.clone(), root.join("units")] {
            sinomo_core::storage::durable_file::ensure_directory(&path).map_err(error)?;
        }
        Ok(Self { root })
    }
    fn path(&self, unit: &UnitKey) -> Result<PathBuf> {
        let id = unit.id()?;
        Ok(self.root.join("units").join(&id[..2]).join(format!("{id}.sobj")))
    }
    pub fn record(&self, reference: &UnitReference) -> Result<()> {
        reference.validate()?;
        if let Some(previous) = self.read(&reference.unit)? {
            if previous == *reference { return Ok(()); }
            if previous.job.is_some() && previous.job != reference.job {
                return Err(error("migration inventory cannot replace or erase the original job without verified predecessor transition"));
            }
        }
        let path = self.path(&reference.unit)?;
        let bytes = serde_ext::to_bytes(reference, VERSION, LengthMode::U32).map_err(error)?;
        if bytes.len() as u64 > MAX_REFERENCE_BYTES { return Err(error("migration unit reference exceeds its bounded DATA contract")); }
        sinomo_core::storage::durable_file::ensure_directory(path.parent().expect("inventory prefix directory")).map_err(error)?;
        sinomo_core::storage::durable_file::write_file_atomic(&path, &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("migration-unit", "tmp").sync_all()).map_err(error)
    }
    pub fn read(&self, unit: &UnitKey) -> Result<Option<UnitReference>> {
        let path = self.path(unit)?;
        if !path.exists() { return Ok(None); }
        let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&path, MAX_REFERENCE_BYTES).map_err(error)?;
        let reference: UnitReference = serde_ext::from_bytes(&bytes).map_err(error)?;
        reference.validate()?;
        if reference.unit != *unit {
            return Err(error("migration inventory changed its exact unit identity"));
        }
        Ok(Some(reference))
    }
    pub fn root(&self) -> &Path { &self.root }
}
