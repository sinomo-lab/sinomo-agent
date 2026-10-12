//! Source-only preparation of the released derived routing cache. Historical
//! assignments remain routing data; fresh native configuration grants no new
//! authority to their recorded primaries, replicas, or epochs.
use super::{MetadataCapturedKvEntry, NativeImportAuthority, Result, error};
use crate::metadata::source_document::decode_original_document;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_dfs::{DfsFileSystemConfig, DfsMetadataShardConfig, DfsStoragePreparation};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

const MAX_CACHE_BYTES: usize = 4 * 1024 * 1024;
const MAX_CACHE_RECORDS: usize = 4096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleasedRoutingTopologyHistory {
    #[serde(default)]
    active: Vec<serde_json::Value>,
    #[serde(default)]
    fallbacks: Vec<Vec<serde_json::Value>>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct CurrentRoutingTopologyHistory {
    #[serde(default)]
    active: Vec<DfsMetadataShardConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fallbacks: Vec<Vec<DfsMetadataShardConfig>>,
}
impl CurrentRoutingTopologyHistory {
    fn records(&self) -> impl Iterator<Item = &DfsMetadataShardConfig> {
        self.active.iter().chain(self.fallbacks.iter().flatten())
    }
    fn validate(&self) -> Result<()> {
        if self.records().count() > MAX_CACHE_RECORDS {
            return Err(error("routing cache exceeds bounded topology record count"));
        }
        for topology in std::iter::once(&self.active).chain(self.fallbacks.iter()) {
            let mut keys = BTreeSet::new();
            for config in topology {
                let mut components = Path::new(&config.shard_id).components();
                if !matches!(components.next(), Some(Component::Normal(_)))
                    || components.next().is_some()
                    || config.shard_id.contains(['/', '\\'])
                    || config.shard_id.chars().any(char::is_control)
                    || !keys.insert(sinomo_dfs::metadata_shard_key(
                        &config.file_system_id,
                        &config.shard_id,
                    ))
                {
                    return Err(error(
                        "routing cache has an invalid or duplicate shard scope",
                    ));
                }
                let mut nodes = BTreeSet::from([config.primary_node_id.clone()]);
                if config
                    .replica_node_ids
                    .iter()
                    .any(|node| !nodes.insert(node.clone()))
                {
                    return Err(error("routing cache repeats a configured member"));
                }
                // Reuse the pure current routing selector for path syntax. No
                // assignment, member, publication receipt, or KV row is inferred.
                let path = match &config.assignment {
                    sinomo_dfs::DfsMetadataShardAssignment::PathPrefix { prefix } => {
                        prefix.as_str()
                    }
                    _ => "/",
                };
                sinomo_dfs::select_metadata_shard_config_for_path(
                    &config.file_system_id,
                    topology,
                    path,
                )
                .map_err(error)?;
            }
        }
        Ok(())
    }
    fn configuration_keys(&self) -> Vec<String> {
        let mut keys = BTreeSet::new();
        for config in self.records() {
            keys.insert(sinomo_dfs::file_system_config_key(&config.file_system_id));
            keys.insert(sinomo_dfs::metadata_shard_key(
                &config.file_system_id,
                &config.shard_id,
            ));
        }
        keys.into_iter().collect()
    }
}
fn path(root: &Path) -> PathBuf {
    root.join("shard-index").join("routing-topologies.sobj")
}
fn read(root: &Path) -> Result<Option<Vec<u8>>> {
    let path = path(root);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(failed) if failed.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(failed) => return Err(error(failed)),
    };
    if !metadata.is_file() || metadata.len() > MAX_CACHE_BYTES as u64 {
        return Err(error("routing cache is not a bounded ordinary file"));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&path)
        .map_err(error)?
        .take((MAX_CACHE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(error)?;
    if bytes.len() > MAX_CACHE_BYTES {
        return Err(error("routing cache grew beyond its bounded source size"));
    }
    Ok(Some(bytes))
}
fn strict_current(bytes: &[u8]) -> Result<CurrentRoutingTopologyHistory> {
    let current: CurrentRoutingTopologyHistory = serde_ext::from_bytes(bytes).map_err(error)?;
    current.validate()?;
    Ok(current)
}
pub(super) fn current(root: &Path) -> Result<bool> {
    Ok(match read(root)? {
        None => true,
        Some(bytes) => strict_current(&bytes).is_ok(),
    })
}
fn convert(bytes: &[u8]) -> Result<CurrentRoutingTopologyHistory> {
    let original: ReleasedRoutingTopologyHistory =
        serde_json::from_value(decode_original_document(bytes)?).map_err(error)?;
    let mut retired = false;
    let mut topology = |records: Vec<serde_json::Value>| -> Result<Vec<DfsMetadataShardConfig>> {
        records
            .into_iter()
            .map(|value| {
                retired |= value.get("namespace_prepare_codec").is_some();
                let fs = crate::legacy_wire::source_file_system_id(
                    value
                        .get("file_system_id")
                        .cloned()
                        .ok_or_else(|| error("routing descriptor has no original filesystem"))?,
                )?;
                let shard = value
                    .get("shard_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| error("routing descriptor has no original shard"))?;
                let key = sinomo_dfs::metadata_shard_key(&fs, shard);
                super::configuration::normalized_assignment_value(value, &key)
            })
            .collect()
    };
    let active = topology(original.active)?;
    let fallbacks = original
        .fallbacks
        .into_iter()
        .map(&mut topology)
        .collect::<Result<Vec<_>>>()?;
    if !retired {
        return Err(error(
            "malformed current routing cache cannot be retired as historical data",
        ));
    }
    let current = CurrentRoutingTopologyHistory { active, fallbacks };
    current.validate()?;
    Ok(current)
}
fn save_original(root: &Path, bytes: &[u8]) -> Result<()> {
    let path = root.join("original.sobj");
    if path.exists() {
        if std::fs::read(&path).map_err(error)? != bytes {
            return Err(error("immutable original routing cache archive changed"));
        }
        return Ok(());
    }
    write_atomic(&path, bytes)
}
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        bytes,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("routing-cache", "tmp")
            .sync_all(),
    )
    .map_err(error)
}
fn sync_archive_ancestry(archive: &Path, owner: &Path) -> Result<()> {
    // Persist each newly created directory entry as well as the archive files,
    // before replacing the only live copy of the released cache.
    for directory in archive.ancestors() {
        sinomo_core::storage::durable_file::sync_directory(directory).map_err(error)?;
        if directory == owner {
            return Ok(());
        }
    }
    Err(error("routing archive has no actual data-owner ancestor"))
}
fn publish_unchanged(root: &Path, original: &[u8], replacement: &[u8]) -> Result<()> {
    if read(root)?.as_deref() != Some(original) {
        return Err(error(
            "routing cache source changed before representation replacement",
        ));
    }
    write_atomic(&path(root), replacement)
}

impl NativeImportAuthority {
    pub(super) fn validate_routing_cache_root(&self, root: &Path) -> Result<()> {
        let expected = sinomo_core::local::SinomoDataDir::new(&self.data_root)
            .dfs_dir()
            .join("metadata");
        let physical = if root.exists() {
            root.canonicalize().map_err(error)?
        } else {
            root.to_owned()
        };
        if physical != expected {
            return Err(error(
                "routing preparation changed the component's actual metadata root",
            ));
        }
        Ok(())
    }
    fn validate_routing_configuration(
        history: &CurrentRoutingTopologyHistory,
        captured: &[MetadataCapturedKvEntry],
    ) -> Result<()> {
        let keys = history.configuration_keys();
        if captured.len() != keys.len() {
            return Err(error(
                "routing configuration changed its exact native cardinality",
            ));
        }
        let mut entries = BTreeMap::new();
        for (captured, expected) in captured.iter().zip(keys) {
            let entry = captured
                .entry
                .as_ref()
                .ok_or_else(|| error("routing source scope has no actual native configuration"))?;
            if captured.key != expected
                || entry.key != expected
                || entries.insert(expected, entry).is_some()
            {
                return Err(error(
                    "routing scope differs from its actual native configuration key",
                ));
            }
        }
        for source in history.records() {
            let fs_key = sinomo_dfs::file_system_config_key(&source.file_system_id);
            let config: DfsFileSystemConfig = entries[&fs_key].decode().map_err(error)?;
            let shard_key =
                sinomo_dfs::metadata_shard_key(&source.file_system_id, &source.shard_id);
            let actual: DfsMetadataShardConfig = entries[&shard_key].decode().map_err(error)?;
            if config.file_system_id != source.file_system_id
                || actual.file_system_id != source.file_system_id
                || actual.shard_id != source.shard_id
                || actual.epoch == 0
            {
                return Err(error(
                    "routing cache differs from its actual current native scope",
                ));
            }
            // Deliberately do not copy native current fields into historical
            // route data. The normal router replaces active from caller config
            // afterward, while genuine fallback route identities remain intact.
        }
        Ok(())
    }
    /// sinomo:kv-configuration: One bounded source-only cache conversion uses
    /// actual native configuration cuts. Their proofs never authorize DFS data.
    pub(super) fn prepare_routing_topology_cache(
        &self,
        root: &Path,
        max_records: usize,
    ) -> Result<DfsStoragePreparation> {
        self.validate_routing_cache_root(root)?;
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("routing cache component lifecycle closed"));
        }
        let Some(original) = read(root)? else {
            return Ok(DfsStoragePreparation::Ready);
        };
        if strict_current(&original).is_ok() {
            return Ok(DfsStoragePreparation::Ready);
        }
        if max_records == 0 {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        self.validate_source_root(root)?;
        let archive = self
            .receiver_root
            .join("routing-topology-cache-v1")
            .join(blake3::hash(&original).to_hex().to_string());
        std::fs::create_dir_all(&archive).map_err(error)?;
        let archive = archive.canonicalize().map_err(error)?;
        if !archive.starts_with(&self.data_root) {
            return Err(error("routing archive is outside the actual data owner"));
        }
        // Original bytes are durable evidence even when conversion or native
        // configuration validation refuses the source.
        save_original(&archive, &original)?;
        sync_archive_ancestry(&archive, &self.data_root)?;
        let history = convert(&original)?;
        if history.records().count() > max_records {
            return Err(error(
                "routing topology requires a larger bounded conversion admission",
            ));
        }
        let keys = history.configuration_keys();
        let (captured, authority) = self.read_exact(&keys)?;
        Self::validate_routing_configuration(&history, &captured)?;
        let input = (&self.data_root, root, &captured, &authority);
        Self::save_blueprint(
            &archive,
            &format!("native-input-{}.json", super::digest(&input)?),
            &input,
        )?;
        let replacement = serde_ext::to_bytes(&history, 1, LengthMode::U32).map_err(error)?;
        if strict_current(&replacement)? != history {
            return Err(error(
                "routing cache current encoding changed preserved history",
            ));
        }
        let (rechecked, fresh_authority) = self.read_exact(&keys)?;
        Self::validate_routing_configuration(&history, &rechecked)?;
        if super::digest(&captured)? != super::digest(&rechecked)? {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        }
        let fresh = (&self.data_root, root, &rechecked, &fresh_authority);
        Self::save_blueprint(
            &archive,
            &format!("native-publication-{}.json", super::digest(&fresh)?),
            &fresh,
        )?;
        if *self.lifecycle.borrow() || self.lifecycle.has_changed().is_err() {
            return Err(error("routing cache lifecycle closed before replacement"));
        }
        sync_archive_ancestry(&archive, &self.data_root)?;
        publish_unchanged(root, &original, &replacement)?;
        Ok(DfsStoragePreparation::Ready)
    }
}

#[cfg(test)]
mod actual_docker_fixture;
#[cfg(test)]
mod tests;
