//! One bounded stat cut from the existing owner when it holds every participant.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsStatPathRequest {
    pub path: String,
    pub read_id: String,
    pub catalog_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsStatShardPrefix {
    pub shard_id: DfsMetadataShardId,
    pub prefix: DfsMetadataPrefixProof,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsStatPathView {
    pub request: DfsStatPathRequest,
    /// Selected dentry or authoritative absence, distinct from a hardlink's
    /// canonical inode; both are original read predicates, not writer authority.
    pub path_object: Option<Box<DfsObjectMetadata>>,
    pub object: Option<Box<DfsObjectMetadata>>,
    pub link_count: Option<u64>,
    pub complete: bool,
    pub prefixes: Vec<DfsStatShardPrefix>,
    /// Data integrity and fresh-read binding, never a source of authority.
    pub integrity: String,
}

impl DfsStatPathView {
    pub(crate) fn integrity_digest(&self) -> Result<String, MetadataBackendError> {
        let bytes = serde_json::to_vec(&(
            &self.request,
            &self.path_object,
            &self.object,
            self.link_count,
            self.complete,
            &self.prefixes,
        ))
        .map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
        Ok(format!("dfs-stat-view-v1:blake3:{}", blake3::hash(&bytes)))
    }
}

pub(crate) fn stat_catalog_digest(
    catalog: &[DfsMetadataShardConfig],
) -> Result<String, MetadataBackendError> {
    let bytes =
        serde_json::to_vec(catalog).map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
    Ok(format!(
        "dfs-stat-catalog-v1:blake3:{}",
        blake3::hash(&bytes)
    ))
}

/// The service already owns/pins these scopes. This opens no backend and mints
/// no custody. An incomplete result supplies no derived count.
pub(crate) fn query_stat_path(
    backend: &dyn DfsMetadataBackend,
    query: &DfsMetadataShardQuery,
    request: &DfsStatPathRequest,
    complete_scope: bool,
    catalog: &[DfsMetadataShardConfig],
    ensure_read: &dyn Fn(&DfsFileSystemId, &str) -> Result<(), DfsError>,
    remaining: &mut usize,
) -> Result<DfsStatPathView, MetadataBackendError> {
    let invalid = |s: &str| MetadataBackendError::Serde(s.into());
    let owns = |id: &DfsMetadataShardId, object: &DfsObjectMetadata| {
        object.file_system_id == query.file_system_id
            && object.tombstone_provenance.is_none()
            && crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &query.file_system_id,
                catalog,
                &object.path,
            )
            .is_ok_and(|shard| shard.is_some_and(|s| s.shard_id == id.0))
    };
    let path_object = backend
        .get_object_by_path_on_shard(&query.shard_id, &query.file_system_id, &request.path)?
        .filter(|row| owns(&query.shard_id, row) && row.kind != DfsObjectKind::Tombstone)
        .map(Box::new);
    let mut view = DfsStatPathView {
        request: request.clone(),
        object: path_object.clone(),
        path_object,
        link_count: None,
        complete: false,
        prefixes: Vec::new(),
        integrity: String::new(),
    };
    let scopes: Vec<_> = catalog
        .iter()
        .filter(|s| complete_scope || s.shard_id == query.shard_id.0)
        .collect();
    for scope in &scopes {
        ensure_read(&query.file_system_id, &request.path).map_err(|e| invalid(&e.to_string()))?;
        let id = DfsMetadataShardId::new(scope.shard_id.clone());
        let prefix = backend
            .metadata_prefix_proof(&id)?
            .filter(DfsMetadataPrefixProof::is_well_formed)
            .ok_or_else(|| invalid("stat path has no exact materialized prefix"))?;
        view.prefixes.push(DfsStatShardPrefix {
            shard_id: id,
            prefix,
        });
    }
    if complete_scope {
        for _ in 0..16 {
            let Some(object) = view
                .object
                .as_ref()
                .filter(|row| row.kind == DfsObjectKind::Hardlink)
            else {
                break;
            };
            let target = object
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| invalid("stat hardlink has no target"))?;
            let mut current = None;
            for scope in &scopes {
                ensure_read(&query.file_system_id, &request.path)
                    .map_err(|e| invalid(&e.to_string()))?;
                let id = DfsMetadataShardId::new(scope.shard_id.clone());
                if let Some(row) = backend
                    .get_object_on_shard(&id, target)?
                    .filter(|row| owns(&id, row))
                {
                    if current.replace(Box::new(row)).is_some() {
                        return Err(invalid("stat hardlink has multiple current owners"));
                    }
                }
            }
            view.object = Some(current.ok_or_else(|| invalid("stat hardlink target unavailable"))?);
        }
        if let Some(object) = view.object.as_mut() {
            if matches!(
                object.kind,
                DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
            ) {
                return Err(invalid("stat path has no live canonical target"));
            }
            let mut count = if object.kind == DfsObjectKind::Directory {
                2u64
            } else {
                1u64
            };
            let mut complete = true;
            for scope in &scopes {
                ensure_read(&query.file_system_id, &object.path)
                    .map_err(|e| invalid(&e.to_string()))?;
                if *remaining == 0 {
                    complete = false;
                    break;
                }
                let id = DfsMetadataShardId::new(scope.shard_id.clone());
                let selection = DfsMetadataSelection::StatProjectionV1 {
                    request: Box::new(DfsStatProjectionRequest {
                        object_id: object.object_id.clone(),
                        object_path: object.path.clone(),
                        object_kind: object.kind.clone(),
                        object_version: object.version.clone(),
                        limit: *remaining,
                        after: None,
                        expected_prefix: None,
                        fence_only: false,
                    }),
                };
                let page_query = DfsMetadataShardQuery {
                    shard_id: id.clone(),
                    epoch: scope.epoch,
                    selection,
                    ..query.clone()
                };
                let DfsMetadataQueryResult::StatProjectionV1 { page } =
                    backend.query_file_history_on_shard(&page_query)?
                else {
                    return Err(invalid("stat scope omitted its bounded projection"));
                };
                if page.entries.len() > *remaining {
                    return Err(invalid("stat projection exceeded record budget"));
                }
                *remaining -= page.entries.len();
                if !page.complete {
                    complete = false;
                    break;
                }
                if let Some(row) = page.object.as_deref().filter(|row| owns(&id, row)) {
                    if row.object_id != object.object_id || row.version != object.version {
                        return Err(invalid("stat object changed within its held snapshot"));
                    }
                    *object = Box::new(row.clone());
                }
                for row in &page.entries {
                    if !owns(&id, row) {
                        continue;
                    }
                    ensure_read(&row.file_system_id, &row.path)
                        .map_err(|e| invalid(&e.to_string()))?;
                    if object.kind != DfsObjectKind::Directory
                        || row.kind == DfsObjectKind::Directory
                    {
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| invalid("stat link count overflow"))?;
                    }
                }
            }
            if complete {
                view.link_count = Some(count);
                view.complete = true;
            }
        } else {
            view.complete = true;
        }
    }
    view.integrity = view.integrity_digest()?;
    Ok(view)
}
