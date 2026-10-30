//! Operator and agent-facing DFS inspection surfaces.

use crate::metadata_pages::{collect_metadata_pages, walk_metadata_pages};
use crate::{
    DfsFileSystem, DfsMetadataBackend, DfsMetadataSummary, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsWriteIntentRecord, DfsWriteIntentStatus,
    MetadataBackendError,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

const OPERATOR_OBJECT_SCAN_PAGE_SIZE: usize = 1024;
const OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE: usize = 1024;
const DEFAULT_OPERATOR_INSPECTION_LIMIT: usize = 1024;
const MAX_OPERATOR_INSPECTION_LIMIT: usize = 10_000;

pub use sinomo_api::dfs::operator::DaemonDfsInspectionRequest;

pub use sinomo_api::dfs::operator::DaemonDfsInspectionResponse;

pub use sinomo_api::dfs::operator::DfsOperatorStatusMetric;

pub use sinomo_api::dfs::operator::DfsOperatorStatusOmission;

pub use sinomo_api::dfs::operator::DfsOperatorStatus;

pub use sinomo_api::dfs::operator::DfsOperatorFileSystemView;

pub use sinomo_api::dfs::operator::DfsOperatorObjectView;

pub use sinomo_api::dfs::operator::DfsOperatorRepairView;

pub use sinomo_api::dfs::operator::DfsOperatorRepairPage;

pub use sinomo_api::dfs::operator::DfsOperatorInspectionCursor;

pub use sinomo_api::dfs::operator::DfsOperatorInspectionItemCursor;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAgentFileSystemSummary {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub objects: Vec<DfsAgentObjectSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAgentObjectSummary {
    pub object_id: DfsObjectId,
    pub path: String,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub state: DfsAgentObjectState,
}

pub use sinomo_api::dfs::operator::DfsAgentObjectState;

#[derive(Debug)]
pub struct DfsOperatorInspector<'a> {
    backend: &'a dyn DfsMetadataBackend,
}

impl<'a> DfsOperatorInspector<'a> {
    pub fn new(backend: &'a dyn DfsMetadataBackend) -> Self {
        Self { backend }
    }

    pub fn status(&self) -> Result<DfsOperatorStatus, MetadataBackendError> {
        let summary = self.backend.metadata_summary()?;
        self.status_from_summary(summary)
    }

    pub(crate) fn status_from_summary(
        &self,
        summary: DfsMetadataSummary,
    ) -> Result<DfsOperatorStatus, MetadataBackendError> {
        Ok(DfsOperatorStatus {
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: summary.file_system_count,
            object_count: summary.object_count,
            version_count: summary.version_count,
            receipt_count: summary.receipt_count,
            intent_count: summary.intent_count,
            pending_write_count: summary.pending_write_count,

            pending_repair_count: summary.pending_repair_count,
            repair_attempt_count: 0,
            pending_namespace_job_count: summary.pending_namespace_job_count,
            failed_namespace_job_count: summary.failed_namespace_job_count,
            cross_shard_namespace_lock_count: Some(summary.cross_shard_namespace_lock_count),
            metadata_shards: self.backend.metadata_shard_statuses()?,
            repair_attempts: Vec::new(),
            storage_migrations: Vec::new(),
        })
    }

    pub fn file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsOperatorFileSystemView>, MetadataBackendError> {
        let Some(file_system) = self.backend.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let summary = self
            .backend
            .metadata_summary_for_file_system(file_system_id)?
            .unwrap_or_default();
        Ok(Some(DfsOperatorFileSystemView {
            file_system,
            object_count: summary.object_count,

            pending_repair_count: summary.pending_repair_count,
        }))
    }

    pub fn object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
        let Some(object) = self.backend.get_object(object_id)? else {
            return Ok(None);
        };
        let versions = versions_for_object_from_backend(self.backend, object_id)?;
        let intents = intents_for_object_from_backend(self.backend, object_id)?;
        let receipt_versions = versions
            .iter()
            .map(|record| record.version.clone())
            .chain(intents.iter().map(|record| record.target_version.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        let mut receipts = Vec::new();
        let mut receipts_by_version = BTreeMap::new();
        for version in receipt_versions {
            let version_receipts =
                receipts_for_version_from_backend(self.backend, object_id, &version)?;
            receipts_by_version.insert(version, version_receipts.clone());
            receipts.extend(version_receipts);
        }

        let repairs = repairs_from_intents_and_receipts(
            self.backend,
            &object,
            &intents,
            &receipts_by_version,
        )?;
        let agent_state = agent_state_for(&object, &intents, &repairs);
        let current_desired_replica_node_ids =
            crate::content_durability::current_content_durability_from_backend(
                self.backend,
                &object,
            )
            .ok()
            .flatten()
            .map(|durability| durability.desired_replica_node_ids);
        Ok(Some(DfsOperatorObjectView {
            object,
            versions,
            receipts,
            intents,
            current_desired_replica_node_ids,
            repairs,
            agent_state,
        }))
    }

    pub fn object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
        let Some(object) = self.backend.get_object_by_path(file_system_id, path)? else {
            return Ok(None);
        };
        self.object(&object.object_id)
    }

    pub fn repairs(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
        let mut repairs = collect_metadata_pages(|cursor| {
            let page = self.repairs_page(file_system_id, object_id, None, cursor)?;
            Ok((page.repairs, page.next_cursor))
        })?;
        repairs.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then_with(|| repair_view_order(left, right))
        });
        Ok(repairs)
    }

    pub fn repairs_page(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
        limit: Option<usize>,
        cursor: Option<&DfsOperatorInspectionCursor>,
    ) -> Result<DfsOperatorRepairPage, MetadataBackendError> {
        let limit = normalize_operator_inspection_limit(limit);
        let mut repairs = Vec::new();
        if let Some(object_id) = object_id {
            if let Some(object) = self.backend.get_object(object_id)? {
                let resume = cursor
                    .filter(|cursor| cursor.object_id.as_ref() == Some(object_id))
                    .and_then(|cursor| cursor.item_key.as_ref());
                let has_remaining =
                    self.extend_repairs_for_object(&object, resume, limit, &mut repairs)?;
                let next_cursor = next_operator_inspection_cursor(
                    self.backend,
                    &object,
                    has_remaining,
                    false,
                    repairs.last().map(repair_inspection_cursor),
                )?;
                return Ok(DfsOperatorRepairPage {
                    repairs,
                    next_cursor,
                });
            }
            return Ok(DfsOperatorRepairPage {
                repairs,
                next_cursor: None,
            });
        }

        // Namespace occupants cannot enumerate deleted objects after path reuse.
        // Walk durable executors and retain only the requested output window.
        let file_systems = self
            .inspection_file_systems(file_system_id)?
            .into_iter()
            .map(|fs| fs.file_system_id)
            .collect::<BTreeSet<_>>();
        let mut candidates = BTreeMap::new();
        let mut cached_object: Option<(DfsObjectMetadata, ObjectRepairRoots)> = None;
        walk_metadata_pages(|intent_cursor| {
            let page = self
                .backend
                .write_intents_page(intent_cursor, OPERATOR_OBJECT_SCAN_PAGE_SIZE)?;
            for intent in page.intents {
                if intent.status != DfsWriteIntentStatus::Committed
                    || !file_systems.contains(&intent.file_system_id)
                {
                    continue;
                }
                if cached_object
                    .as_ref()
                    .is_none_or(|(object, _)| object.object_id != intent.object_id)
                {
                    cached_object = self
                        .backend
                        .get_object(&intent.object_id)?
                        .map(|object| (object, ObjectRepairRoots::default()));
                }
                let Some((object, roots)) = cached_object.as_mut() else {
                    continue;
                };
                if !roots.intent_owns_repair_work(self.backend, object, &intent)? {
                    continue;
                }
                let path = crate::metadata_keys::normalize_path(&object.path);
                let object_key = (&object.file_system_id, &path, &object.object_id);
                if let Some(cursor) = cursor
                    && let Some(after) = &cursor.object_cursor
                    && object_key < (&cursor.file_system_id, &after.path, &after.object_id)
                {
                    continue;
                }
                let receipts = version_receipt_nodes_by_hash(
                    self.backend,
                    &object.object_id,
                    &intent.target_version,
                )?;
                let nodes = receipt_nodes_for_intent_hash(&receipts, &intent.content_hash);
                for node in &intent.desired_replica_node_ids {
                    if nodes.contains(node) {
                        continue;
                    }
                    let repair = DfsOperatorRepairView {
                        intent_id: intent.intent_id.clone(),
                        file_system_id: intent.file_system_id.clone(),
                        object_id: intent.object_id.clone(),
                        version: intent.target_version.clone(),
                        content_hash: intent.content_hash.clone(),
                        missing_replica_node_id: node.clone(),
                    };
                    if let Some(cursor) = cursor
                        && cursor.file_system_id == object.file_system_id
                        && cursor.object_id.as_ref() == Some(&object.object_id)
                        && !repair_is_after_cursor(&repair, cursor.item_key.as_ref())
                    {
                        continue;
                    }
                    let key = (
                        object.file_system_id.clone(),
                        path.clone(),
                        object.object_id.clone(),
                        repair_view_key(&repair),
                    );
                    candidates.insert(key, repair);
                    if candidates.len() > limit.saturating_add(1) {
                        candidates.pop_last();
                    }
                }
            }
            Ok(page.next_cursor)
        })?;
        let has_remaining = candidates.len() > limit;
        let selected = candidates.into_iter().take(limit).collect::<Vec<_>>();
        let next_cursor = if has_remaining {
            selected.last().map(
                |((fs, path, object, _), repair)| DfsOperatorInspectionCursor {
                    file_system_id: fs.clone(),
                    object_cursor: Some(crate::DfsObjectPageCursor {
                        path: path.clone(),
                        object_id: object.clone(),
                        shard_id: None,
                    }),
                    object_id: Some(object.clone()),
                    item_key: Some(repair_inspection_cursor(repair)),
                },
            )
        } else {
            None
        };
        Ok(DfsOperatorRepairPage {
            repairs: selected.into_iter().map(|(_, repair)| repair).collect(),
            next_cursor,
        })
    }

    pub fn agent_file_system_summary(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAgentFileSystemSummary>, MetadataBackendError> {
        let Some(file_system) = self.backend.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let backend = self.backend;
        let mut objects = Vec::new();
        self.for_filtered_objects(Some(file_system_id), None, |object| {
            let intents = intents_for_object_from_backend(backend, &object.object_id)?;

            let repairs = repairs_for_object_from_backend(backend, &object, &intents)?;
            objects.push(DfsAgentObjectSummary {
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                state: agent_state_for(&object, &intents, &repairs),
            });
            Ok(())
        })?;
        Ok(Some(DfsAgentFileSystemSummary {
            file_system_id: file_system.file_system_id,
            display_name: file_system.display_name,
            objects,
        }))
    }

    fn for_filtered_objects<F>(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
        mut visit: F,
    ) -> Result<(), MetadataBackendError>
    where
        F: FnMut(DfsObjectMetadata) -> Result<(), MetadataBackendError>,
    {
        if let Some(object_id) = object_id {
            if let Some(object) = self.backend.get_object(object_id)? {
                visit(object)?;
            }
            return Ok(());
        }

        let filesystems = match file_system_id {
            Some(file_system_id) => self
                .backend
                .get_file_system(file_system_id)?
                .into_iter()
                .collect::<Vec<_>>(),
            None => self.backend.list_file_systems()?,
        };
        for file_system in filesystems {
            walk_metadata_pages(|cursor| {
                let page = self.backend.objects_for_file_system_page(
                    &file_system.file_system_id,
                    cursor,
                    OPERATOR_OBJECT_SCAN_PAGE_SIZE,
                )?;
                for object in page.objects {
                    visit(object)?;
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(())
    }

    fn inspection_file_systems(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
    ) -> Result<Vec<DfsFileSystem>, MetadataBackendError> {
        match file_system_id {
            Some(file_system_id) => Ok(self
                .backend
                .get_file_system(file_system_id)?
                .into_iter()
                .collect()),
            None => self.backend.list_file_systems(),
        }
    }

    fn extend_repairs_for_object(
        &self,
        object: &DfsObjectMetadata,
        cursor: Option<&DfsOperatorInspectionItemCursor>,
        limit: usize,
        output: &mut Vec<DfsOperatorRepairView>,
    ) -> Result<bool, MetadataBackendError> {
        if limit == 0 {
            return Ok(true);
        }

        let mut roots = ObjectRepairRoots::default();
        let mut receipt_cache = None;
        let mut candidates = BTreeMap::<(DfsVersion, NodeId, String), DfsOperatorRepairView>::new();
        walk_metadata_pages(|intent_cursor| {
            let page = self.backend.intents_for_object_page(
                &object.object_id,
                intent_cursor,
                OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for intent in page.intents {
                if !roots.intent_owns_repair_work(self.backend, object, &intent)? {
                    continue;
                }
                if receipt_cache
                    .as_ref()
                    .is_none_or(|(version, _)| version != &intent.target_version)
                {
                    receipt_cache = Some((
                        intent.target_version.clone(),
                        version_receipt_nodes_by_hash(
                            self.backend,
                            &object.object_id,
                            &intent.target_version,
                        )?,
                    ));
                }
                let (_, receipt_nodes_by_hash) =
                    receipt_cache.as_ref().expect("loaded target receipts");
                let receipt_nodes =
                    receipt_nodes_for_intent_hash(receipt_nodes_by_hash, &intent.content_hash);
                let mut desired_replica_node_ids = intent.desired_replica_node_ids.clone();
                desired_replica_node_ids.sort();
                for node_id in desired_replica_node_ids {
                    if receipt_nodes.contains(&node_id) {
                        continue;
                    }
                    let repair = DfsOperatorRepairView {
                        intent_id: intent.intent_id.clone(),
                        file_system_id: intent.file_system_id.clone(),
                        object_id: intent.object_id.clone(),
                        version: intent.target_version.clone(),
                        content_hash: intent.content_hash.clone(),
                        missing_replica_node_id: node_id,
                    };
                    if !repair_is_after_cursor(&repair, cursor) {
                        continue;
                    }
                    candidates.insert(repair_view_key(&repair), repair);
                    if candidates.len() > limit.saturating_add(1) {
                        candidates.pop_last();
                    }
                }
            }
            Ok(page.next_cursor)
        })?;
        let has_remaining = candidates.len() > limit;
        output.extend(candidates.into_values().take(limit));
        Ok(has_remaining)
    }
}

fn normalize_operator_inspection_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_OPERATOR_INSPECTION_LIMIT)
        .clamp(1, MAX_OPERATOR_INSPECTION_LIMIT)
}

fn next_operator_inspection_cursor(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
    has_remaining_in_object: bool,
    has_more_after_object: bool,
    item_key: Option<DfsOperatorInspectionItemCursor>,
) -> Result<Option<DfsOperatorInspectionCursor>, MetadataBackendError> {
    if !has_remaining_in_object && !has_more_after_object {
        return Ok(None);
    }
    Ok(Some(DfsOperatorInspectionCursor {
        file_system_id: object.file_system_id.clone(),
        object_cursor: if has_more_after_object {
            Some(backend.object_page_cursor_for_object(object)?)
        } else {
            None
        },
        object_id: has_remaining_in_object.then(|| object.object_id.clone()),
        item_key: has_remaining_in_object.then_some(item_key).flatten(),
    }))
}

fn repair_inspection_cursor(repair: &DfsOperatorRepairView) -> DfsOperatorInspectionItemCursor {
    DfsOperatorInspectionItemCursor::Repair {
        intent_id: repair.intent_id.clone(),
        version: repair.version.clone(),
        missing_replica_node_id: repair.missing_replica_node_id.clone(),
    }
}

fn repair_view_order(
    left: &DfsOperatorRepairView,
    right: &DfsOperatorRepairView,
) -> std::cmp::Ordering {
    repair_view_key(left).cmp(&repair_view_key(right))
}

fn repair_view_key(repair: &DfsOperatorRepairView) -> (DfsVersion, NodeId, String) {
    (
        repair.version.clone(),
        repair.missing_replica_node_id.clone(),
        repair.intent_id.clone(),
    )
}

fn repair_is_after_cursor(
    repair: &DfsOperatorRepairView,
    cursor: Option<&DfsOperatorInspectionItemCursor>,
) -> bool {
    match cursor {
        Some(DfsOperatorInspectionItemCursor::Repair {
            intent_id,
            version,
            missing_replica_node_id,
        }) => {
            (
                &repair.version,
                &repair.missing_replica_node_id,
                &repair.intent_id,
            ) > (version, missing_replica_node_id, intent_id)
        }
        _ => true,
    }
}

fn versions_for_object_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
) -> Result<Vec<DfsObjectVersionRecord>, MetadataBackendError> {
    collect_metadata_pages(|cursor| {
        let page = backend.versions_for_object_page(
            object_id,
            cursor,
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        Ok((page.versions, page.next_cursor))
    })
}

fn receipts_for_version_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Result<Vec<DfsReplicaReceiptRecord>, MetadataBackendError> {
    collect_metadata_pages(|cursor| {
        let page = backend.receipts_for_version_page(
            object_id,
            version,
            cursor,
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        Ok((page.receipts, page.next_cursor))
    })
}

fn intents_for_object_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
) -> Result<Vec<DfsWriteIntentRecord>, MetadataBackendError> {
    collect_metadata_pages(|cursor| {
        let page = backend.intents_for_object_page(
            object_id,
            cursor,
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        Ok((page.intents, page.next_cursor))
    })
}

fn repairs_for_object_from_backend(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
    let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
    for intent in intents
        .iter()
        .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
    {
        if !receipts_by_version.contains_key(&intent.target_version) {
            receipts_by_version.insert(
                intent.target_version.clone(),
                receipts_for_version_from_backend(
                    backend,
                    &intent.object_id,
                    &intent.target_version,
                )?,
            );
        }
    }
    repairs_from_intents_and_receipts(backend, object, intents, &receipts_by_version)
}

fn version_receipt_nodes_by_hash(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Result<BTreeMap<Option<String>, BTreeSet<NodeId>>, MetadataBackendError> {
    let mut nodes_by_hash = BTreeMap::<Option<String>, BTreeSet<NodeId>>::new();
    walk_metadata_pages(|cursor| {
        let page = backend.receipts_for_version_page(
            object_id,
            version,
            cursor,
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        for receipt in page.receipts {
            nodes_by_hash
                .entry(receipt.content_hash.clone())
                .or_default()
                .insert(receipt.replica_node_id);
        }
        Ok(page.next_cursor)
    })?;
    Ok(nodes_by_hash)
}

fn receipt_nodes_for_intent_hash(
    nodes_by_hash: &BTreeMap<Option<String>, BTreeSet<NodeId>>,
    content_hash: &Option<String>,
) -> BTreeSet<NodeId> {
    match content_hash {
        Some(content_hash) => nodes_by_hash
            .get(&Some(content_hash.clone()))
            .cloned()
            .unwrap_or_default(),
        None => nodes_by_hash
            .values()
            .flat_map(|nodes| nodes.iter().cloned())
            .collect(),
    }
}

#[derive(Default)]
struct ObjectRepairRoots {
    history: Option<(BTreeMap<String, u64>, BTreeSet<DfsVersion>)>,
}

impl ObjectRepairRoots {
    fn intent_owns_repair_work(
        &mut self,
        backend: &dyn DfsMetadataBackend,
        object: &DfsObjectMetadata,
        intent: &DfsWriteIntentRecord,
    ) -> Result<bool, MetadataBackendError> {
        if intent.status != DfsWriteIntentStatus::Committed
            || backend
                .get_intent_receipt(&intent.object_id, &intent.intent_id)?
                .is_some()
        {
            // Compact terminal identities are retry evidence, never executors.
            return Ok(false);
        }
        use crate::metadata_summary::repair_root_for_intent;
        if repair_root_for_intent(object, intent, None, false, &BTreeMap::new()).is_some() {
            return Ok(true);
        }
        let Some(version) = backend
            .get_version(&object.object_id, &intent.target_version)?
            .filter(|version| version.has_retention_grant())
        else {
            return Ok(false);
        };
        if self.history.is_none() {
            let mut high = BTreeMap::new();
            walk_metadata_pages(|cursor| {
                let page = backend.versions_for_object_page(
                    &object.object_id,
                    cursor,
                    OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for version in page.versions {
                    for (namespace, state) in version.retention {
                        let order = high.entry(namespace).or_insert(0u64);
                        *order = (*order).max(state.order);
                    }
                }
                Ok(page.next_cursor)
            })?;
            self.history = Some((
                high,
                backend
                    .version_pins_for_object(&object.object_id)?
                    .into_iter()
                    .map(|pin| pin.version)
                    .collect(),
            ));
        }
        let (high, pins) = self.history.as_ref().expect("loaded history roots");
        Ok(repair_root_for_intent(
            object,
            intent,
            Some(&version),
            pins.contains(&intent.target_version),
            high,
        )
        .is_some_and(|root| root.is_live(crate::dfs_now_unix_nanos())))
    }
}

fn repairs_from_intents_and_receipts(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
    let mut repairs = Vec::new();
    let mut roots = ObjectRepairRoots::default();
    for intent in intents {
        if !roots.intent_owns_repair_work(backend, object, intent)? {
            continue;
        }
        let receipt_nodes: BTreeSet<NodeId> = receipts_by_version
            .get(&intent.target_version)
            .into_iter()
            .flatten()
            .filter(|receipt| {
                intent
                    .content_hash
                    .as_ref()
                    .is_none_or(|hash| receipt.content_hash.as_ref() == Some(hash))
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        for node_id in &intent.desired_replica_node_ids {
            if !receipt_nodes.contains(node_id) {
                repairs.push(DfsOperatorRepairView {
                    intent_id: intent.intent_id.clone(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    version: intent.target_version.clone(),
                    content_hash: intent.content_hash.clone(),
                    missing_replica_node_id: node_id.clone(),
                });
            }
        }
    }
    Ok(repairs)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPinRecord {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub pinned_by_node_id: NodeId,
    pub mode: DfsPinMode,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

pub use sinomo_api::dfs::operator::DfsPinMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPinAddRequest {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub object_id: Option<DfsObjectId>,
    pub pinned_by_node_id: NodeId,
    pub mode: DfsPinMode,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct LocalDfsPinStore {
    root: PathBuf,
}

impl LocalDfsPinStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn put_pin(&self, record: &DfsPinRecord) -> Result<(), DfsPinStoreError> {
        let path = self.pin_path(record);
        write_object_atomic(&path, record)?;
        Ok(())
    }

    pub fn remove_pin(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        node_id: Option<&NodeId>,
        mode: Option<&DfsPinMode>,
    ) -> Result<bool, DfsPinStoreError> {
        let mut removed = false;
        for (record_path, record) in self.list_pin_files(Some(file_system_id))? {
            if normalize_path(&record.path) != normalize_path(path) {
                continue;
            }
            if node_id.is_some_and(|expected| &record.pinned_by_node_id != expected) {
                continue;
            }
            if mode.is_some_and(|expected| &record.mode != expected) {
                continue;
            }
            if record_path.is_file() {
                fs::remove_file(&record_path).map_err(|source| DfsPinStoreError::Io {
                    path: record_path,
                    source,
                })?;
                removed = true;
            }
        }
        Ok(removed)
    }

    pub fn list_pins(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
    ) -> Result<Vec<DfsPinRecord>, DfsPinStoreError> {
        Ok(self
            .list_pin_files(file_system_id)?
            .into_iter()
            .map(|(_, record)| record)
            .collect())
    }

    fn list_pin_files(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
    ) -> Result<Vec<(PathBuf, DfsPinRecord)>, DfsPinStoreError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in read_dir(&self.root)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                continue;
            }
            let record: DfsPinRecord = read_object(&path)?;
            if path != self.pin_path(&record) {
                return Err(DfsPinStoreError::Serde(
                    "pin storage requires conversion before opening".into(),
                ));
            }
            if file_system_id.is_none_or(|expected| &record.file_system_id == expected) {
                records.push((path, record));
            }
        }
        records.sort_by(|left, right| {
            left.1
                .file_system_id
                .cmp(&right.1.file_system_id)
                .then_with(|| left.1.path.cmp(&right.1.path))
                .then_with(|| left.1.pinned_by_node_id.cmp(&right.1.pinned_by_node_id))
                .then_with(|| left.1.mode.cmp(&right.1.mode))
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(records)
    }

    pub fn add_pin(&self, request: DfsPinAddRequest) -> Result<DfsPinRecord, DfsPinStoreError> {
        let record = DfsPinRecord {
            file_system_id: request.file_system_id,
            path: request.path,
            object_id: request.object_id,
            pinned_by_node_id: request.pinned_by_node_id,
            mode: request.mode,
            created_at: request.created_at,
        };
        self.put_pin(&record)?;
        Ok(record)
    }

    fn pin_path(&self, record: &DfsPinRecord) -> PathBuf {
        self.root.join(format!(
            "{}-{}-{}-{}.sobj",
            key_segment(&record.file_system_id),
            path_hash(&record.path),
            key_segment(&record.pinned_by_node_id),
            key_segment(&record.mode)
        ))
    }
}

#[derive(Debug, Error)]
pub enum DfsPinStoreError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("pin serialization error: {0}")]
    Serde(String),
}

fn agent_state_for(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    repairs: &[DfsOperatorRepairView],
) -> DfsAgentObjectState {
    if intents
        .iter()
        .any(|intent| write_intent_is_active(&intent.status))
    {
        DfsAgentObjectState::PendingWrite
    } else if !repairs.is_empty() {
        DfsAgentObjectState::PendingRepair
    } else if object.content_hash.is_some() {
        DfsAgentObjectState::Current
    } else {
        DfsAgentObjectState::MetadataOnly
    }
}

fn write_intent_is_active(status: &DfsWriteIntentStatus) -> bool {
    matches!(
        status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    )
}

fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>, DfsPinStoreError> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| DfsPinStoreError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn read_object<T>(path: &Path) -> Result<T, DfsPinStoreError>
where
    T: DeserializeOwned,
{
    let bytes = fs::read(path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    sinomo_object_serde::from_bytes(&bytes)
        .map_err(|source| DfsPinStoreError::Serde(source.to_string()))
}

fn write_object_atomic<T>(path: &Path, value: &T) -> Result<(), DfsPinStoreError>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DfsPinStoreError::Io {
            path: parent.to_owned(),
            source,
        })?;
    }
    let tmp_path = path.with_extension("tmp");
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| DfsPinStoreError::Serde(source.to_string()))?;
    fs::write(&tmp_path, bytes).map_err(|source| DfsPinStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn path_hash(path: &str) -> String {
    blake3::hash(normalize_path(path).as_bytes())
        .to_hex()
        .to_string()
}

fn key_segment(value: &impl ToString) -> String {
    value
        .to_string()
        .trim_matches('/')
        .replace(['/', '\\', ':'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
        DfsObjectKind, DfsObjectMetadata, DfsReplicaReceiptRecord, DfsReplicationPolicy,
        DfsWriteIntentRecord, LocalSobjMetadataBackend,
    };
    use sinomo_core::events::EventId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};

    fn empty_operator_status() -> DfsOperatorStatus {
        DfsOperatorStatus {
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: 0,
            object_count: 0,
            version_count: 0,
            receipt_count: 0,
            intent_count: 0,
            pending_write_count: 0,

            pending_repair_count: 0,
            repair_attempt_count: 0,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: Vec::new(),
            storage_migrations: Vec::new(),
        }
    }

    #[test]
    fn typed_bounded_omissions_roundtrip() {
        let mut status = empty_operator_status();
        status.push_bounded_omission(DfsOperatorStatusOmission::historical_routing_topology(4));

        assert!(status.incomplete_reasons.is_empty());
        assert!(status.blocking_incomplete_reasons().is_empty());
        assert_eq!(status.bounded_omissions.len(), 1);

        assert!(
            status.bounded_omissions[0]
                .omitted_metrics()
                .contains(&DfsOperatorStatusMetric::CrossShardNamespaceLockCount)
        );
        assert_eq!(status.cross_shard_namespace_lock_count, None);

        let encoded = serde_json::to_value(&status).expect("encode status");
        let decoded: DfsOperatorStatus =
            serde_json::from_value(encoded.clone()).expect("decode current status");
        assert_eq!(decoded, status);
        assert!(decoded.blocking_incomplete_reasons().is_empty());
    }

    #[test]
    fn historical_routing_zero_lock_proof_is_explicit() {
        let omission =
            DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                4,
            );
        assert!(
            !omission
                .omitted_metrics()
                .contains(&DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "an explicit union-empty proof keeps the lock metric available"
        );

        let mut status = empty_operator_status();
        status.push_bounded_omission(omission.clone());
        assert_eq!(status.cross_shard_namespace_lock_count, Some(0));
    }

    #[test]
    fn unmatched_incomplete_reason_remains_blocking_with_typed_omissions() {
        let mut status = empty_operator_status();
        status
            .push_bounded_omission(DfsOperatorStatusOmission::resident_repair_queue_not_hydrated());
        assert!(status.incomplete_reasons.is_empty());
        status
            .incomplete_reasons
            .push("metadata shard cursor projection is corrupt".to_string());

        assert_eq!(
            status.blocking_incomplete_reasons(),
            vec!["metadata shard cursor projection is corrupt"]
        );
    }

    #[test]
    fn operator_inspector_reports_status_object_and_repairs() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-status-real-repair", 3);
        let mut runtime =
            DfsTestRuntime::new("operator-status-real-repair", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let path = "/users/operator-pending-repair.bin";
        let mut placement =
            crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        placement.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        runtime
            .apply_placement_policy(
                &nodes[0],
                path,
                crate::DfsPlacementScope::Subtree,
                placement.clone(),
            )
            .unwrap();
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let resolved = writer
            .resolved_placement_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 2)
            .unwrap();
        assert_eq!(resolved.policy, placement);
        assert_eq!(resolved.source_path.as_deref(), Some(path));
        assert_eq!(
            resolved.source_scope,
            Some(crate::DfsPlacementScope::Subtree)
        );
        assert!(!resolved.inherited);
        let replication = resolved
            .policy
            .replication_policy(Default::default())
            .unwrap();
        let baseline = DfsOperatorInspector::new(writer.metadata_backend().unwrap())
            .status()
            .unwrap();
        runtime.install_fault(DfsTestFaultRule {
            source: None,
            target: Some(nodes[2].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
        let stat = writer
            .posix_write_file(
                path,
                b"real pending repair",
                crate::DfsWriteOptions {
                    policy: replication,
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let backend = writer.metadata_backend().unwrap();
        let inspector = DfsOperatorInspector::new(backend);
        let status = inspector.status().unwrap();
        assert_eq!(status.file_system_count, baseline.file_system_count);
        assert_eq!(status.object_count, baseline.object_count + 1);
        assert_eq!(
            status.pending_repair_count,
            baseline.pending_repair_count + 1
        );
        let object = inspector.object(&stat.object_id).unwrap().unwrap();
        assert_eq!(object.agent_state, DfsAgentObjectState::PendingRepair);
        assert_eq!(
            object
                .current_desired_replica_node_ids
                .as_ref()
                .unwrap()
                .iter()
                .collect::<BTreeSet<_>>(),
            [&nodes[0], &nodes[2]].into_iter().collect::<BTreeSet<_>>()
        );
        assert!(
            !object.object.current_locations.contains(&nodes[2]),
            "the current desired set must retain the real missing target"
        );
        let owner = object
            .intents
            .iter()
            .find(|intent| {
                intent.publication.is_current()
                    && intent.file_system_id == stat.file_system_id
                    && intent.object_id == stat.object_id
                    && intent.target_version == stat.version
                    && intent.content_hash == stat.content_hash
            })
            .expect("actual current write owner for the inspected target");
        assert_eq!(owner.status, DfsWriteIntentStatus::Committed);
        assert_eq!(owner.policy.min_commit_replicas, 1);
        assert_eq!(owner.policy.desired_replicas, 2);
        assert_eq!(
            owner
                .desired_replica_node_ids
                .iter()
                .collect::<BTreeSet<_>>(),
            [&nodes[0], &nodes[2]].into_iter().collect::<BTreeSet<_>>()
        );
        assert!(
            object
                .repairs
                .iter()
                .any(|repair| repair.missing_replica_node_id == nodes[2])
        );
        let by_path = inspector
            .object_by_path(&stat.file_system_id, path)
            .unwrap()
            .unwrap();
        assert_eq!(by_path.object.object_id, stat.object_id);
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn native_terminal_history_collection_preserves_current_durability_and_operator_state() {
        let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(
            "operator-native-terminal-history",
            1,
        );
        let mut runtime = crate::test_support::DfsTestRuntime::new(
            "operator-native-terminal-history",
            nodes[0].clone(),
        )
        .unwrap();
        runtime.start_nodes().unwrap();
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let path = "/users/terminal-current.bin".to_string();
        let mut file_system = runtime.file_system();
        file_system
            .subtree_policies
            .push(crate::DfsSubtreePolicy::new(
                &path,
                DfsObjectPolicy::ReplicatedConfirmed,
                1,
                1,
            ));
        writer.add_file_system(file_system).unwrap();
        let bytes = b"current grant survives terminal collection";
        let stat = writer
            .posix_write_file_with_operation_id(
                &path,
                bytes,
                crate::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![nodes[0].clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
                "operator-terminal-current",
            )
            .unwrap();
        let before = writer
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        let grant = before
            .content_durability
            .clone()
            .expect("normal publication carries actual W/N grant");
        let collection = writer
            .gc_metadata_history(
                crate::DfsMetadataHistoryGcOptions::delete_eligible(std::time::Duration::ZERO, 1)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
        assert!(
            collection.compacted_intent_count > 0,
            "the regression must actually compact a real terminal intent"
        );
        let after = writer
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        assert_eq!(
            after.content_durability,
            Some(grant),
            "terminal cleanup cannot reconstruct or weaken the durable plan"
        );
        let view = DfsOperatorInspector::new(writer.metadata_backend().unwrap())
            .object(&stat.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            view.current_desired_replica_node_ids,
            Some(
                after
                    .content_durability
                    .as_ref()
                    .unwrap()
                    .desired_replica_node_ids
                    .clone()
            ),
            "ordinary terminal intent compaction keeps the exact recorded current goal"
        );
        let current = view.object.clone();
        let mut changed = Vec::new();
        let mut stale = current.clone();
        stale.path.push_str("-stale");
        changed.push(stale);
        let mut stale = current.clone();
        stale.policy = DfsObjectPolicy::LocalOwned;
        changed.push(stale);
        let mut stale = current.clone();
        stale.owner_node_id = NodeId::generate();
        changed.push(stale);
        let mut stale = current.clone();
        stale.version = DfsVersion::generate();
        changed.push(stale);
        let mut stale = current.clone();
        stale.content_hash = Some("blake3:foreign-current-content".into());
        changed.push(stale);
        let mut stale = current.clone();
        stale.content_size_bytes = Some(bytes.len() as u64 + 1);
        changed.push(stale);
        for stale in changed {
            assert!(
                crate::content_durability::current_content_durability_from_backend(
                    writer.metadata_backend().unwrap(),
                    &stale,
                )
                .is_err(),
                "an observation cannot reuse durability for changed current object semantics"
            );
        }
        assert_eq!(
            writer
                .metadata_backend()
                .unwrap()
                .get_object(&stat.object_id)
                .unwrap()
                .unwrap(),
            current,
            "durability observation never rewrites the current object"
        );
        assert!(view.repairs.is_empty());
        assert_eq!(view.agent_state, DfsAgentObjectState::Current);
        assert_eq!(writer.posix_read_file(&path).unwrap(), bytes);
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn operator_inspector_counts_staging_intents_as_pending_writes() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-staging-real-write", 3);
        let mut runtime =
            DfsTestRuntime::new("operator-staging-real-write", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let mut writer = runtime.metadata_primary_service("/").unwrap();
        let path = "/operator-staging.bin";
        let original = writer
            .posix_write_file(
                path,
                b"old visible bytes",
                crate::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        let baseline = DfsOperatorInspector::new(writer.metadata_backend().unwrap())
            .status()
            .unwrap();
        let mut file_system = runtime.file_system();
        let mut policy =
            crate::DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedConfirmed, 2, 2);
        policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        file_system.subtree_policies.push(policy);
        writer.add_file_system(file_system).unwrap();
        drop(writer);
        let mut writer = runtime.metadata_primary_service("/").unwrap();
        let mut pending_policy = DfsReplicationPolicy::new(2, 2).unwrap();
        pending_policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        runtime.install_fault(DfsTestFaultRule {
            source: None,
            target: Some(nodes[2].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
        let outcome = writer
            .write_distributed_eventual(
                sinomo_core::DfsWriteRequest {
                    file_system_id: original.file_system_id.clone(),
                    object_id: original.object_id.clone(),
                    target_version: DfsVersion::generate(),
                    requesting_node_id: nodes[0].clone(),
                },
                b"pending actual W2 bytes".to_vec(),
                vec![nodes[0].clone(), nodes[2].clone()],
                pending_policy,
            )
            .unwrap();
        let crate::NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts,
            required_receipts,
        } = outcome
        else {
            panic!("missing actual replica must keep W2 pending");
        };
        assert_eq!(durable_receipts, 1);
        assert_eq!(required_receipts, 2);
        let backend = writer.metadata_backend().unwrap();
        let inspector = DfsOperatorInspector::new(backend);
        assert_eq!(
            inspector.status().unwrap().pending_write_count,
            baseline.pending_write_count + 1
        );
        assert_eq!(
            inspector
                .object(&original.object_id)
                .unwrap()
                .unwrap()
                .agent_state,
            DfsAgentObjectState::PendingWrite
        );
        assert_eq!(
            backend
                .get_object(&original.object_id)
                .unwrap()
                .unwrap()
                .version,
            original.version,
            "pending write cannot become visible"
        );
        writer.abort_pending_write(&intent_id).unwrap();
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn operator_inspector_ignores_superseded_version_repairs() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-superseded-real-repair", 3);
        let mut runtime =
            DfsTestRuntime::new("operator-superseded-real-repair", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let path = "/operator-superseded.bin";
        let mut file_system = runtime.file_system();
        let mut policy =
            crate::DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedEventual, 1, 2);
        policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        file_system.subtree_policies.push(policy);
        writer.add_file_system(file_system).unwrap();
        let options = crate::DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 2).unwrap(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        runtime.install_fault(DfsTestFaultRule {
            source: None,
            target: Some(nodes[2].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
        let old = writer
            .posix_write_file(path, b"old incomplete placement", options.clone())
            .unwrap();
        assert!(
            !DfsOperatorInspector::new(writer.metadata_backend().unwrap())
                .object(&old.object_id)
                .unwrap()
                .unwrap()
                .repairs
                .is_empty(),
            "the regression begins with real old-version repair work"
        );
        runtime.clear_faults();
        let current = writer
            .posix_write_file(
                path,
                b"current fully placed",
                crate::DfsWriteOptions {
                    foreground_replica_repair: true,
                    ..options
                },
            )
            .unwrap();
        writer
            .posix_sync_file(path, crate::DfsPosixSyncKind::File)
            .unwrap();
        assert_ne!(old.version, current.version);
        let view = DfsOperatorInspector::new(writer.metadata_backend().unwrap())
            .object(&current.object_id)
            .unwrap()
            .unwrap();
        assert!(
            view.repairs.is_empty(),
            "missing old-version receipt cannot reappear as current repair work"
        );
        assert_eq!(view.agent_state, DfsAgentObjectState::Current);
        assert_eq!(
            writer.posix_read_file(path).unwrap(),
            b"current fully placed"
        );
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn operator_inspection_scans_repairs_and_agent_summary_in_pages() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-native-large-page", 4);
        let mut runtime =
            DfsTestRuntime::new("operator-native-large-page", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let path = "/operator-paging-target.bin";
        let mut file_system = runtime.file_system();
        let mut policy =
            crate::DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedEventual, 1, 4);
        policy.required_node_ids = nodes.clone();
        file_system.subtree_policies.push(policy);
        writer.add_file_system(file_system).unwrap();
        for node in &nodes[1..] {
            // Main maintenance can repair after the W1 write returns, even
            // when foreground repair is disabled. Keep all three replicas
            // missing throughout the independent paging reads below.
            for route in [
                crate::DFS_DATA_STAGE_BLOB_ROUTE,
                crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
            ] {
                runtime.install_fault(DfsTestFaultRule {
                    source: None,
                    target: Some(node.clone()),
                    route: Some(route.into()),
                    call_id: None,
                    origin: None,
                    remaining_hits: 32,
                    action: DfsTestFaultAction::BudgetExhausted,
                });
            }
        }
        let baseline = writer
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .object_count;
        let template = writer
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&runtime.file_system().file_system_id, "/")
            .unwrap()
            .unwrap();
        let mut effects = Vec::new();
        let mut versions = Vec::new();
        let mut paths = Vec::new();
        for index in 0..OPERATOR_OBJECT_SCAN_PAGE_SIZE {
            let mut directory = template.clone();
            directory.path = format!("/operator-fill-{index:05}");
            directory.object_id = crate::object_id_for_path(&directory.path);
            directory.version = DfsVersion::generate();
            versions.push(crate::DfsNamespaceObjectVersionExpectation {
                object_id: directory.object_id.clone(),
                expected_version: None,
            });
            paths.push(crate::DfsNamespacePathOccupantExpectation {
                path: directory.path.clone(),
                expected_object_id: None,
            });
            effects.push(crate::DfsPreparedMetadataEffect::PutObject { object: directory });
        }
        let shard = runtime.root_shard().unwrap();
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: shard.file_system_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(shard.shard_id),
                owner_epoch: shard.epoch,
                operation_id: "operator-paging-directories".into(),
            },
            versions,
            paths,
            effects,
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        // The custom corpus is authored by the actual Root Core, rather than
        // by the browser used for ordinary writes and later inspection.
        drop(writer);
        let mut writer = runtime.metadata_primary_service(path).unwrap();
        crate::test_support::publish_current_payload(&mut writer, payload).unwrap();
        // The kernel helper has committed the 1,024-row setup corpus, while
        // replicas may still hold only its hidden prepare. Complete that setup
        // through the normal directory durability barrier before creating the
        // independent pending-repair object inspected below.
        let corpus_head = writer
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap();
        let corpus = writer.posix_sync_directory("/").unwrap();
        assert_eq!(
            corpus.metadata_certificate.target.journal_sequence,
            corpus_head
        );
        assert_eq!(
            corpus
                .metadata_certificate
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            nodes.iter().cloned().collect(),
            "every configured member must acknowledge the exact complete corpus"
        );
        // This direct Core handle has no retained Main ingress execution scope.
        // Admit the independent fresh write through the normal registered Main.
        drop(writer);
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let stat = writer
            .posix_write_file(
                path,
                b"real paged repair",
                crate::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 4).unwrap(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        drop(writer);
        let reader = runtime.client(&nodes[0]).unwrap();
        let backend = reader.metadata_backend().unwrap();
        let inspector = DfsOperatorInspector::new(backend);
        let all = inspector
            .repairs(Some(&stat.file_system_id), Some(&stat.object_id))
            .unwrap();
        assert_eq!(all.len(), 3, "three real missing content replicas");
        let first = inspector
            .repairs_page(Some(&stat.file_system_id), None, Some(1), None)
            .unwrap();
        let second = inspector
            .repairs_page(
                Some(&stat.file_system_id),
                None,
                Some(1),
                first.next_cursor.as_ref(),
            )
            .unwrap();
        let third = inspector
            .repairs_page(
                Some(&stat.file_system_id),
                None,
                Some(1),
                second.next_cursor.as_ref(),
            )
            .unwrap();
        let merged = first
            .repairs
            .into_iter()
            .chain(second.repairs)
            .chain(third.repairs)
            .collect::<Vec<_>>();
        assert_eq!(merged, all);
        assert!(third.next_cursor.is_none());
        let summary = inspector
            .agent_file_system_summary(&stat.file_system_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            summary.objects.len(),
            baseline as usize + OPERATOR_OBJECT_SCAN_PAGE_SIZE + 1
        );
        assert_eq!(
            summary
                .objects
                .iter()
                .find(|object| object.object_id == stat.object_id)
                .unwrap()
                .state,
            DfsAgentObjectState::PendingRepair
        );
        drop(reader);
        runtime.close().unwrap();
    }

    #[test]
    fn operator_repair_pages_preserve_order_without_full_object_materialization() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-native-repair-order", 4);
        let mut runtime =
            DfsTestRuntime::new("operator-native-repair-order", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let mut writer = runtime.client(&nodes[0]).unwrap();
        let path = "/operator-paging-target.bin";
        let mut file_system = runtime.file_system();
        let mut policy =
            crate::DfsSubtreePolicy::new(path, DfsObjectPolicy::ReplicatedEventual, 1, 4);
        policy.required_node_ids = nodes.clone();
        file_system.subtree_policies.push(policy);
        writer.add_file_system(file_system).unwrap();
        for node in &nodes[1..] {
            runtime.install_fault(DfsTestFaultRule {
                source: None,
                target: Some(node.clone()),
                route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: DfsTestFaultAction::BudgetExhausted,
            });
        }
        let stat = writer
            .posix_write_file(
                path,
                b"real paged repair",
                crate::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 4).unwrap(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let backend = writer.metadata_backend().unwrap();
        let inspector = DfsOperatorInspector::new(backend);
        let all = inspector
            .repairs(Some(&stat.file_system_id), Some(&stat.object_id))
            .unwrap();
        assert_eq!(all.len(), 3, "three real missing content replicas");
        let first = inspector
            .repairs_page(
                Some(&stat.file_system_id),
                Some(&stat.object_id),
                Some(2),
                None,
            )
            .unwrap();
        assert_eq!(first.repairs, all[..2]);
        let second = inspector
            .repairs_page(
                Some(&stat.file_system_id),
                Some(&stat.object_id),
                Some(2),
                first.next_cursor.as_ref(),
            )
            .unwrap();
        assert_eq!(second.repairs, all[2..]);
        assert!(first.next_cursor.is_some());
        assert!(second.next_cursor.is_none());
        let observed = all
            .iter()
            .map(|repair| repair.missing_replica_node_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(observed, nodes[1..]);
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn local_pin_store_adds_lists_and_removes_records() {
        let root = temp_root("local_pin_store_adds_lists_and_removes_records");
        let store = LocalDfsPinStore::new(&root);
        let record = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: "src/lib.rs".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: node("node_laptop"),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add pin");

        assert_eq!(store.list_pins(None).expect("pins"), vec![record.clone()]);
        assert!(
            store
                .remove_pin(
                    &file_system_id(),
                    "src/lib.rs",
                    Some(&node("node_laptop")),
                    None,
                )
                .expect("remove")
        );
        assert!(store.list_pins(None).expect("pins").is_empty());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn local_pin_store_treats_backslash_as_filename_character() {
        let root = temp_root("local_pin_store_treats_backslash_as_filename_character");
        let store = LocalDfsPinStore::new(&root);
        let owner = node("node_laptop");
        let backslash = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: r"src\a.txt".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: owner.clone(),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add backslash pin");
        let slash = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: "src/a.txt".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: owner.clone(),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add slash pin");

        assert_eq!(
            store.list_pins(None).expect("pins"),
            vec![slash.clone(), backslash.clone()]
        );
        assert!(
            store
                .remove_pin(&file_system_id(), "src/a.txt", Some(&owner), None)
                .expect("remove slash")
        );
        assert_eq!(store.list_pins(None).expect("pins"), vec![backslash]);

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn local_pin_store_removes_only_matching_mode_when_requested() {
        let root = temp_root("local_pin_store_removes_only_matching_mode");
        let store = LocalDfsPinStore::new(&root);
        let owner = node("node_laptop");
        for mode in [DfsPinMode::Metadata, DfsPinMode::Content] {
            store
                .add_pin(DfsPinAddRequest {
                    file_system_id: file_system_id(),
                    path: "src/lib.rs".to_string(),
                    object_id: Some(object_id()),
                    pinned_by_node_id: owner.clone(),
                    mode,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("add pin");
        }

        assert!(
            store
                .remove_pin(
                    &file_system_id(),
                    "src/lib.rs",
                    Some(&owner),
                    Some(&DfsPinMode::Metadata),
                )
                .expect("remove metadata pin")
        );
        let pins = store.list_pins(None).expect("pins");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].mode, DfsPinMode::Content);

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_status_aggregates_counts_across_multiple_objects() {
        use crate::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
        let nodes = DfsTestRuntime::ordered_node_ids("operator-native-aggregate", 3);
        let mut runtime =
            DfsTestRuntime::new("operator-native-aggregate", nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let mut writer = runtime.metadata_primary_service("/").unwrap();
        let fs_id = runtime.file_system().file_system_id;
        let baseline = writer
            .metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap();
        let mut file_system = runtime.file_system();
        for (path, kind, w) in [
            (
                "/operator-alpha.bin",
                DfsObjectPolicy::ReplicatedEventual,
                1,
            ),
            (
                "/operator-beta.bin",
                DfsObjectPolicy::ReplicatedConfirmed,
                2,
            ),
        ] {
            let mut policy = crate::DfsSubtreePolicy::new(path, kind, w, 2);
            policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
            file_system.subtree_policies.push(policy);
        }
        writer.add_file_system(file_system).unwrap();
        drop(writer);
        let mut writer = runtime.metadata_primary_service("/").unwrap();
        let mut pending_policy = DfsReplicationPolicy::new(2, 2).unwrap();
        pending_policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        let options = crate::DfsWriteOptions {
            policy: pending_policy.clone(),
            replica_candidates: nodes.clone(),
            wait_for_commit: true,
            foreground_replica_repair: true,
        };
        let alpha_old = writer
            .posix_write_file("/operator-alpha.bin", b"old alpha", options.clone())
            .unwrap();
        let beta = writer
            .posix_write_file("/operator-beta.bin", b"visible beta", options)
            .unwrap();
        runtime.install_fault(DfsTestFaultRule {
            source: None,
            target: Some(nodes[2].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
        let mut alpha_policy = DfsReplicationPolicy::new(1, 2).unwrap();
        alpha_policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
        let alpha = writer
            .posix_write_file(
                "/operator-alpha.bin",
                b"new alpha",
                crate::DfsWriteOptions {
                    policy: alpha_policy,
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        assert_eq!(alpha.object_id, alpha_old.object_id);
        assert_ne!(alpha.version, alpha_old.version);
        let pending = writer
            .write_distributed_eventual(
                sinomo_core::DfsWriteRequest {
                    file_system_id: fs_id.clone(),
                    object_id: beta.object_id.clone(),
                    target_version: DfsVersion::generate(),
                    requesting_node_id: nodes[0].clone(),
                },
                b"pending beta".to_vec(),
                vec![nodes[0].clone(), nodes[2].clone()],
                pending_policy,
            )
            .unwrap();
        let crate::NetworkedDfsWriteOutcome::Pending {
            intent_id,
            durable_receipts,
            required_receipts,
        } = pending
        else {
            panic!("a real missing replica must keep beta pending");
        };
        assert_eq!((durable_receipts, required_receipts), (1, 2));
        let now = OffsetDateTime::now_utc();
        let running = DfsNamespaceJobRecord {
            job_id: "operator-native-running-job".into(),
            file_system_id: fs_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/operator-alpha.bin".into(),
            target_path: Some("/users/operator-alpha.bin".into()),
            pending_frames: vec![DfsNamespaceJobFrame::ScanDirectory {
                path: "/operator-alpha.bin".into(),
                cursor: None,
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
        let failed = DfsNamespaceJobRecord {
            job_id: "operator-native-failed-job".into(),
            kind: DfsNamespaceJobKind::RenameDirectory,
            status: DfsNamespaceJobStatus::Failed,
            root_path: "/operator-missing-directory".into(),
            target_path: Some("/operator-invalid-destination".into()),
            pending_frames: Vec::new(),
            failure_reason: Some("invalid destination".into()),
            ..running.clone()
        };
        let shard = runtime.root_shard().unwrap();
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: fs_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(shard.shard_id),
                owner_epoch: shard.epoch,
                operation_id: "operator-native-job-counts".into(),
            },
            Vec::new(),
            Vec::new(),
            vec![
                crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: running },
                crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: failed },
            ],
            now,
        )
        .unwrap();
        crate::test_support::publish_current_payload(&mut writer, payload).unwrap();
        let backend = writer.metadata_backend().unwrap();
        let summary = backend.metadata_summary().unwrap();
        assert_eq!(summary.file_system_count, baseline.file_system_count);
        assert_eq!(summary.object_count, baseline.object_count + 2);
        assert_eq!(summary.version_count, backend.all_versions().unwrap().len());
        assert_eq!(summary.receipt_count, backend.all_receipts().unwrap().len());
        assert_eq!(summary.intent_count, backend.all_intents().unwrap().len());
        let actual_file_versions = backend
            .all_versions()
            .unwrap()
            .into_iter()
            .filter(|row| row.object_id == alpha.object_id || row.object_id == beta.object_id)
            .collect::<Vec<_>>();
        assert_eq!(actual_file_versions.len(), 3);
        assert_eq!(
            summary.pending_write_count,
            baseline.pending_write_count + 1
        );
        assert_eq!(
            summary.pending_repair_count,
            baseline.pending_repair_count + 1
        );
        assert_eq!(
            summary.pending_namespace_job_count,
            baseline.pending_namespace_job_count + 1
        );
        assert_eq!(
            summary.failed_namespace_job_count,
            baseline.failed_namespace_job_count + 1
        );
        assert_eq!(
            summary.cross_shard_namespace_lock_count,
            baseline.cross_shard_namespace_lock_count + 1
        );
        let scoped = backend
            .metadata_summary_for_file_system(&fs_id)
            .unwrap()
            .unwrap();
        assert_eq!(scoped.object_count, summary.object_count);
        assert_eq!(scoped.version_count, summary.version_count);
        assert_eq!(scoped.receipt_count, summary.receipt_count);
        assert_eq!(scoped.pending_repair_count, summary.pending_repair_count);
        assert!(
            backend
                .metadata_summary_for_file_system(&DfsFileSystemId::generate())
                .unwrap()
                .is_none()
        );
        let inspector = DfsOperatorInspector::new(backend);
        let status = inspector.status().unwrap();
        assert_eq!(status.object_count, summary.object_count);
        assert_eq!(status.version_count, summary.version_count);
        assert_eq!(status.receipt_count, summary.receipt_count);
        assert_eq!(status.intent_count, summary.intent_count);
        assert_eq!(status.pending_write_count, summary.pending_write_count);
        assert_eq!(status.pending_repair_count, summary.pending_repair_count);
        assert_eq!(
            status.pending_namespace_job_count,
            summary.pending_namespace_job_count
        );
        assert_eq!(
            status.failed_namespace_job_count,
            summary.failed_namespace_job_count
        );
        assert_eq!(
            status.cross_shard_namespace_lock_count,
            Some(summary.cross_shard_namespace_lock_count)
        );
        let file_system = inspector.file_system(&fs_id).unwrap().unwrap();
        assert_eq!(file_system.object_count, scoped.object_count);
        assert_eq!(
            file_system.pending_repair_count,
            scoped.pending_repair_count
        );
        assert_eq!(
            inspector
                .object(&alpha.object_id)
                .unwrap()
                .unwrap()
                .agent_state,
            DfsAgentObjectState::PendingRepair
        );
        assert_eq!(
            inspector
                .object(&beta.object_id)
                .unwrap()
                .unwrap()
                .agent_state,
            DfsAgentObjectState::PendingWrite
        );
        assert_eq!(
            backend
                .get_object(&beta.object_id)
                .unwrap()
                .unwrap()
                .version,
            beta.version
        );
        writer.abort_pending_write(&intent_id).unwrap();
        drop(writer);
        runtime.close().unwrap();
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_artifact").expect("object")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
    }
}
