//! Operator and agent-facing DFS inspection surfaces.

use crate::{
    DfsConflictPageCursor, DfsConflictRecord, DfsConflictStatus, DfsFileSystem, DfsMetadataBackend,
    DfsMetadataShardStatus, DfsObjectMetadata, DfsObjectPageCursor, DfsObjectVersionRecord,
    DfsRepairAttemptRecord, DfsReplicaReceiptRecord, DfsWriteIntentRecord, DfsWriteIntentStatus,
    MetadataBackendError,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_events::EventId;
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
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

/// A DFS operator inspection request (status / filesystem / path / object / conflicts /
/// repair). Host-agnostic contract: the same request drives in-process and daemon paths.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionRequest {
    Status,
    FileSystem {
        file_system_id: DfsFileSystemId,
    },
    Path {
        file_system_id: DfsFileSystemId,
        path: String,
    },
    Object {
        object_id: DfsObjectId,
    },
    Conflicts {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsOperatorInspectionCursor>,
    },
    ResolveConflicts {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    ConflictVersions {
        object_id: DfsObjectId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    UseConflictVersion {
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    Repair {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsOperatorInspectionCursor>,
    },
}

/// The response to a [`DaemonDfsInspectionRequest`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionResponse {
    Status {
        status: DfsOperatorStatus,
    },
    FileSystem {
        file_system: Option<DfsOperatorFileSystemView>,
    },
    Path {
        object: Option<DfsOperatorObjectView>,
    },
    Object {
        object: Option<DfsOperatorObjectView>,
    },
    Conflicts {
        conflicts: Vec<DfsConflictRecord>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_cursor: Option<DfsOperatorInspectionCursor>,
    },
    ResolveConflicts {
        report: DfsOperatorConflictResolutionReport,
    },
    ConflictVersions {
        report: Option<DfsOperatorConflictVersionsReport>,
    },
    UseConflictVersion {
        report: DfsOperatorUseConflictVersionReport,
    },
    Repair {
        repairs: Vec<DfsOperatorRepairView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_cursor: Option<DfsOperatorInspectionCursor>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorStatus {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    #[serde(default)]
    pub pending_namespace_job_count: usize,
    #[serde(default)]
    pub failed_namespace_job_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair_attempts: Vec<DfsRepairAttemptRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorFileSystemView {
    pub file_system: DfsFileSystem,
    pub object_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorObjectView {
    pub object: DfsObjectMetadata,
    pub versions: Vec<DfsObjectVersionRecord>,
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    pub intents: Vec<DfsWriteIntentRecord>,
    pub conflicts: Vec<DfsConflictRecord>,
    pub repairs: Vec<DfsOperatorRepairView>,
    pub agent_state: DfsAgentObjectState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorRepairView {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub missing_replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictPage {
    pub conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictResolutionReport {
    pub removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_found_conflict_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionsReport {
    pub object: DfsObjectMetadata,
    pub conflicts: Vec<DfsConflictRecord>,
    pub versions: Vec<DfsOperatorConflictVersionView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionView {
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    pub visible: bool,
    pub current: bool,
    pub stored: bool,
    pub roles: Vec<DfsOperatorConflictVersionRole>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionRole {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_id: Option<String>,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorUseConflictVersionReport {
    pub previous_object: DfsObjectMetadata,
    pub committed_object: DfsObjectMetadata,
    pub selected_version: DfsObjectVersionRecord,
    pub changed: bool,
    pub removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_found_conflict_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorRepairPage {
    pub repairs: Vec<DfsOperatorRepairView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorInspectionCursor {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_cursor: Option<DfsObjectPageCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_key: Option<DfsOperatorInspectionItemCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsOperatorInspectionItemCursor {
    Conflict {
        conflict_id: String,
    },
    Repair {
        intent_id: String,
        version: DfsVersion,
        missing_replica_node_id: NodeId,
    },
}

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsAgentObjectState {
    Current,
    PendingWrite,
    PendingRepair,
    Conflicted,
    MetadataOnly,
}

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
        Ok(DfsOperatorStatus {
            file_system_count: summary.file_system_count,
            object_count: summary.object_count,
            version_count: summary.version_count,
            receipt_count: summary.receipt_count,
            intent_count: summary.intent_count,
            pending_write_count: summary.pending_write_count,
            open_conflict_count: summary.open_conflict_count,
            pending_repair_count: summary.pending_repair_count,
            pending_namespace_job_count: summary.pending_namespace_job_count,
            failed_namespace_job_count: summary.failed_namespace_job_count,
            metadata_shards: self.backend.metadata_shard_statuses()?,
            repair_attempts: Vec::new(),
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
            open_conflict_count: summary.open_conflict_count,
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
        let mut receipts = Vec::new();
        let mut receipts_by_version = BTreeMap::new();
        for version in &versions {
            let version_receipts =
                receipts_for_version_from_backend(self.backend, object_id, &version.version)?;
            receipts_by_version.insert(version.version.clone(), version_receipts.clone());
            receipts.extend(version_receipts);
        }
        let intents = intents_for_object_from_backend(self.backend, object_id)?;
        let conflicts = conflicts_for_object_from_backend(self.backend, object_id)?;
        let repairs = repairs_from_intents_and_receipts(&object, &intents, &receipts_by_version);
        let agent_state = agent_state_for(&object, &intents, &conflicts, &repairs);
        Ok(Some(DfsOperatorObjectView {
            object,
            versions,
            receipts,
            intents,
            conflicts,
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

    pub fn conflict_versions(
        &self,
        object_id: &DfsObjectId,
        conflict_ids: &[String],
    ) -> Result<Option<DfsOperatorConflictVersionsReport>, MetadataBackendError> {
        let Some(object) = self.backend.get_object(object_id)? else {
            return Ok(None);
        };
        let requested_ids: BTreeSet<String> = conflict_ids.iter().cloned().collect();
        let conflicts: Vec<DfsConflictRecord> =
            conflicts_for_object_from_backend(self.backend, &object.object_id)?
                .into_iter()
                .filter(|conflict| {
                    requested_ids.is_empty() || requested_ids.contains(&conflict.conflict_id)
                })
                .collect();
        let versions = conflict_version_views(self.backend, &object, &conflicts)?;
        Ok(Some(DfsOperatorConflictVersionsReport {
            object,
            conflicts,
            versions,
        }))
    }

    pub fn conflicts(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
        let backend = self.backend;
        let mut conflicts = Vec::new();
        self.for_filtered_objects(file_system_id, object_id, |object| {
            conflicts.extend(conflicts_for_object_from_backend(
                backend,
                &object.object_id,
            )?);
            Ok(())
        })?;
        conflicts.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then_with(|| left.conflict_id.cmp(&right.conflict_id))
        });
        Ok(conflicts)
    }

    pub fn conflicts_page(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
        limit: Option<usize>,
        cursor: Option<&DfsOperatorInspectionCursor>,
    ) -> Result<DfsOperatorConflictPage, MetadataBackendError> {
        let limit = normalize_operator_inspection_limit(limit);
        let mut conflicts = Vec::new();
        if let Some(object_id) = object_id {
            if let Some(object) = self.backend.get_object(object_id)? {
                let resume = cursor
                    .filter(|cursor| cursor.object_id.as_ref() == Some(object_id))
                    .and_then(|cursor| cursor.item_key.as_ref());
                let has_remaining =
                    self.extend_conflicts_for_object(&object, resume, limit, &mut conflicts)?;
                let next_cursor = next_operator_inspection_cursor(
                    self.backend,
                    &object,
                    has_remaining,
                    false,
                    conflicts.last().map(conflict_inspection_cursor),
                )?;
                return Ok(DfsOperatorConflictPage {
                    conflicts,
                    next_cursor,
                });
            }
            return Ok(DfsOperatorConflictPage {
                conflicts,
                next_cursor: None,
            });
        }

        let file_systems = self.inspection_file_systems(file_system_id)?;
        for (file_system_index, file_system) in file_systems.iter().enumerate() {
            let page_cursor = match self.resume_cursor_for_file_system(
                file_system,
                cursor,
                &mut conflicts,
                limit,
                |inspector, object, item_cursor, item_limit, output| {
                    inspector.extend_conflicts_for_object(object, item_cursor, item_limit, output)
                },
                conflict_inspection_cursor,
            )? {
                OperatorResumeState::SkipFileSystem => continue,
                OperatorResumeState::Filled(next_cursor) => {
                    return Ok(DfsOperatorConflictPage {
                        next_cursor,
                        conflicts,
                    });
                }
                OperatorResumeState::Continue(cursor) => cursor,
            };

            let mut object_page_cursor = page_cursor.and_then(|cursor| cursor.object_cursor);
            loop {
                let page = self.backend.objects_for_file_system_page(
                    &file_system.file_system_id,
                    object_page_cursor.as_ref(),
                    OPERATOR_OBJECT_SCAN_PAGE_SIZE,
                )?;
                let page_next_cursor = page.next_cursor.clone();
                let objects = page.objects;
                if objects.is_empty() {
                    break;
                }
                let object_count = objects.len();
                for (object_index, object) in objects.iter().enumerate() {
                    let item_limit = limit.saturating_sub(conflicts.len());
                    if item_limit == 0 {
                        break;
                    }
                    let has_remaining =
                        self.extend_conflicts_for_object(object, None, item_limit, &mut conflicts)?;
                    if conflicts.len() >= limit {
                        let has_more_after_object = object_index + 1 < object_count
                            || page_next_cursor.is_some()
                            || file_system_index + 1 < file_systems.len();
                        return Ok(DfsOperatorConflictPage {
                            next_cursor: next_operator_inspection_cursor(
                                self.backend,
                                object,
                                has_remaining,
                                has_more_after_object,
                                conflicts.last().map(conflict_inspection_cursor),
                            )?,
                            conflicts,
                        });
                    }
                }
                let Some(next_cursor) = page_next_cursor else {
                    break;
                };
                object_page_cursor = Some(next_cursor);
            }
        }

        Ok(DfsOperatorConflictPage {
            conflicts,
            next_cursor: None,
        })
    }

    pub fn repairs(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
        let backend = self.backend;
        let mut repairs = Vec::new();
        self.for_filtered_objects(file_system_id, object_id, |object| {
            let intents = intents_for_object_from_backend(backend, &object.object_id)?;
            repairs.extend(repairs_for_object_from_backend(backend, &object, &intents)?);
            Ok(())
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

        let file_systems = self.inspection_file_systems(file_system_id)?;
        for (file_system_index, file_system) in file_systems.iter().enumerate() {
            let page_cursor = match self.resume_cursor_for_file_system(
                file_system,
                cursor,
                &mut repairs,
                limit,
                |inspector, object, item_cursor, item_limit, output| {
                    inspector.extend_repairs_for_object(object, item_cursor, item_limit, output)
                },
                repair_inspection_cursor,
            )? {
                OperatorResumeState::SkipFileSystem => continue,
                OperatorResumeState::Filled(next_cursor) => {
                    return Ok(DfsOperatorRepairPage {
                        next_cursor,
                        repairs,
                    });
                }
                OperatorResumeState::Continue(cursor) => cursor,
            };

            let mut object_page_cursor = page_cursor.and_then(|cursor| cursor.object_cursor);
            loop {
                let page = self.backend.objects_for_file_system_page(
                    &file_system.file_system_id,
                    object_page_cursor.as_ref(),
                    OPERATOR_OBJECT_SCAN_PAGE_SIZE,
                )?;
                let page_next_cursor = page.next_cursor.clone();
                let objects = page.objects;
                if objects.is_empty() {
                    break;
                }
                let object_count = objects.len();
                for (object_index, object) in objects.iter().enumerate() {
                    let item_limit = limit.saturating_sub(repairs.len());
                    if item_limit == 0 {
                        break;
                    }
                    let has_remaining =
                        self.extend_repairs_for_object(object, None, item_limit, &mut repairs)?;
                    if repairs.len() >= limit {
                        let has_more_after_object = object_index + 1 < object_count
                            || page_next_cursor.is_some()
                            || file_system_index + 1 < file_systems.len();
                        return Ok(DfsOperatorRepairPage {
                            next_cursor: next_operator_inspection_cursor(
                                self.backend,
                                object,
                                has_remaining,
                                has_more_after_object,
                                repairs.last().map(repair_inspection_cursor),
                            )?,
                            repairs,
                        });
                    }
                }
                let Some(next_cursor) = page_next_cursor else {
                    break;
                };
                object_page_cursor = Some(next_cursor);
            }
        }

        Ok(DfsOperatorRepairPage {
            repairs,
            next_cursor: None,
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
            let has_open_conflict = object_has_open_conflict(backend, &object.object_id)?;
            let repairs = repairs_for_object_from_backend(backend, &object, &intents)?;
            objects.push(DfsAgentObjectSummary {
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                state: agent_state_from_flags(&object, &intents, has_open_conflict, &repairs),
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
            let mut cursor = None;
            loop {
                let page = self.backend.objects_for_file_system_page(
                    &file_system.file_system_id,
                    cursor.as_ref(),
                    OPERATOR_OBJECT_SCAN_PAGE_SIZE,
                )?;
                for object in page.objects {
                    visit(object)?;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
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

    fn resume_cursor_for_file_system<T, Extend, Key>(
        &self,
        file_system: &DfsFileSystem,
        cursor: Option<&DfsOperatorInspectionCursor>,
        output: &mut Vec<T>,
        limit: usize,
        mut extend: Extend,
        key_for_item: Key,
    ) -> Result<OperatorResumeState, MetadataBackendError>
    where
        Extend: FnMut(
            &Self,
            &DfsObjectMetadata,
            Option<&DfsOperatorInspectionItemCursor>,
            usize,
            &mut Vec<T>,
        ) -> Result<bool, MetadataBackendError>,
        Key: Fn(&T) -> DfsOperatorInspectionItemCursor,
    {
        let Some(cursor) = cursor else {
            return Ok(OperatorResumeState::Continue(Some(
                DfsOperatorInspectionCursor {
                    file_system_id: file_system.file_system_id.clone(),
                    object_cursor: None,
                    object_id: None,
                    item_key: None,
                },
            )));
        };
        if file_system.file_system_id < cursor.file_system_id {
            return Ok(OperatorResumeState::SkipFileSystem);
        }
        if file_system.file_system_id > cursor.file_system_id {
            return Ok(OperatorResumeState::Continue(Some(
                DfsOperatorInspectionCursor {
                    file_system_id: file_system.file_system_id.clone(),
                    object_cursor: None,
                    object_id: None,
                    item_key: None,
                },
            )));
        }

        if let Some(object_id) = cursor.object_id.as_ref() {
            if let Some(object) = self.backend.get_object(object_id)? {
                let item_limit = limit.saturating_sub(output.len());
                if item_limit > 0 {
                    let has_remaining =
                        extend(self, &object, cursor.item_key.as_ref(), item_limit, output)?;
                    if output.len() >= limit {
                        return Ok(OperatorResumeState::Filled(
                            next_operator_inspection_cursor(
                                self.backend,
                                &object,
                                has_remaining,
                                cursor.object_cursor.is_some(),
                                output.last().map(key_for_item),
                            )?,
                        ));
                    }
                }
            }
        }

        Ok(OperatorResumeState::Continue(Some(
            DfsOperatorInspectionCursor {
                file_system_id: file_system.file_system_id.clone(),
                object_cursor: cursor.object_cursor.clone(),
                object_id: None,
                item_key: None,
            },
        )))
    }

    fn extend_conflicts_for_object(
        &self,
        object: &DfsObjectMetadata,
        cursor: Option<&DfsOperatorInspectionItemCursor>,
        limit: usize,
        output: &mut Vec<DfsConflictRecord>,
    ) -> Result<bool, MetadataBackendError> {
        if limit == 0 {
            return Ok(true);
        }
        let after = conflict_page_cursor_from_operator(cursor);
        let page =
            self.backend
                .conflicts_for_object_page(&object.object_id, after.as_ref(), limit)?;
        let has_remaining = page.next_cursor.is_some();
        output.extend(page.conflicts);
        Ok(has_remaining)
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

        let receipt_nodes_by_hash = current_version_receipt_nodes_by_hash(self.backend, object)?;
        let mut candidates = BTreeMap::<(DfsVersion, NodeId, String), DfsOperatorRepairView>::new();
        let mut intent_cursor = None;
        loop {
            let page = self.backend.intents_for_object_page(
                &object.object_id,
                intent_cursor.as_ref(),
                OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for intent in page.intents {
                if intent.status != DfsWriteIntentStatus::Committed
                    || intent.target_version != object.version
                {
                    continue;
                }
                let receipt_nodes =
                    receipt_nodes_for_intent_hash(&receipt_nodes_by_hash, &intent.content_hash);
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
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        let has_remaining = candidates.len() > limit;
        output.extend(candidates.into_values().take(limit));
        Ok(has_remaining)
    }
}

pub fn resolve_dfs_conflicts(
    backend: &mut dyn DfsMetadataBackend,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
    conflict_ids: &[String],
) -> Result<DfsOperatorConflictResolutionReport, MetadataBackendError> {
    let requested_ids: BTreeSet<String> = conflict_ids.iter().cloned().collect();
    let conflicts = {
        let inspector = DfsOperatorInspector::new(backend);
        inspector.conflicts(file_system_id, object_id)?
    };
    let mut removed = Vec::new();
    for conflict in conflicts {
        if !requested_ids.is_empty() && !requested_ids.contains(&conflict.conflict_id) {
            continue;
        }
        if let Some(removed_conflict) =
            backend.remove_conflict(&conflict.object_id, &conflict.conflict_id)?
        {
            removed.push(removed_conflict);
        }
    }
    removed.sort_by(|left, right| {
        left.object_id
            .cmp(&right.object_id)
            .then_with(|| left.conflict_id.cmp(&right.conflict_id))
    });
    let removed_ids: BTreeSet<String> = removed
        .iter()
        .map(|conflict| conflict.conflict_id.clone())
        .collect();
    let not_found_conflict_ids = requested_ids
        .into_iter()
        .filter(|conflict_id| !removed_ids.contains(conflict_id))
        .collect();
    Ok(DfsOperatorConflictResolutionReport {
        removed,
        not_found_conflict_ids,
    })
}

pub fn use_dfs_conflict_version(
    backend: &mut dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    conflict_ids: &[String],
) -> Result<DfsOperatorUseConflictVersionReport, MetadataBackendError> {
    let previous_object =
        backend
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
    let mut selected_version = backend.get_version(object_id, version)?.ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "DFS object {object_id} version {version} was not found"
        ))
    })?;

    normalize_node_locations(&mut selected_version.locations);
    selected_version.visible = true;

    let mut committed_object = previous_object.clone();
    committed_object.version = selected_version.version.clone();
    committed_object.content_hash = selected_version.content_hash.clone();
    committed_object.content_size_bytes = selected_version.size_bytes;
    committed_object.current_locations = selected_version.locations.clone();

    let changed = committed_object.version != previous_object.version
        || committed_object.content_hash != previous_object.content_hash
        || committed_object.content_size_bytes != previous_object.content_size_bytes
        || normalized_nodes(&committed_object.current_locations)
            != normalized_nodes(&previous_object.current_locations);
    if changed {
        let now = now_unix_nanos();
        committed_object.modified_at_unix_nanos = now;
        committed_object.changed_at_unix_nanos = now;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        backend.commit_visible_version(
            &previous_object.version,
            &committed_object,
            &selected_version,
            &[],
        )?;
    }

    let requested_ids: BTreeSet<String> = conflict_ids.iter().cloned().collect();
    let conflicts = conflicts_for_object_from_backend(backend, object_id)?;
    let mut removed = Vec::new();
    for conflict in conflicts {
        if !requested_ids.is_empty() && !requested_ids.contains(&conflict.conflict_id) {
            continue;
        }
        if let Some(removed_conflict) =
            backend.remove_conflict(&conflict.object_id, &conflict.conflict_id)?
        {
            removed.push(removed_conflict);
        }
    }
    removed.sort_by(|left, right| left.conflict_id.cmp(&right.conflict_id));
    let removed_ids: BTreeSet<String> = removed
        .iter()
        .map(|conflict| conflict.conflict_id.clone())
        .collect();
    let not_found_conflict_ids = requested_ids
        .into_iter()
        .filter(|conflict_id| !removed_ids.contains(conflict_id))
        .collect();

    Ok(DfsOperatorUseConflictVersionReport {
        previous_object,
        committed_object,
        selected_version,
        changed,
        removed,
        not_found_conflict_ids,
    })
}

#[derive(Debug, Default)]
struct ConflictVersionViewBuilder {
    content_hash: Option<String>,
    version_record: Option<DfsObjectVersionRecord>,
    current: bool,
    roles: Vec<DfsOperatorConflictVersionRole>,
}

fn conflict_version_views(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
    conflicts: &[DfsConflictRecord],
) -> Result<Vec<DfsOperatorConflictVersionView>, MetadataBackendError> {
    let mut views = BTreeMap::<DfsVersion, ConflictVersionViewBuilder>::new();
    add_conflict_version_role(
        &mut views,
        object.version.clone(),
        DfsOperatorConflictVersionRole {
            conflict_id: None,
            role: "current".to_string(),
            content_hash: object.content_hash.clone(),
        },
    );
    if let Some(view) = views.get_mut(&object.version) {
        view.current = true;
    }

    for conflict in conflicts {
        if let Some(base_version) = conflict.base_version.clone() {
            add_conflict_version_role(
                &mut views,
                base_version,
                DfsOperatorConflictVersionRole {
                    conflict_id: Some(conflict.conflict_id.clone()),
                    role: "base".to_string(),
                    content_hash: None,
                },
            );
        }
        add_conflict_version_role(
            &mut views,
            conflict.local_version.clone(),
            DfsOperatorConflictVersionRole {
                conflict_id: Some(conflict.conflict_id.clone()),
                role: "local".to_string(),
                content_hash: conflict.local_content_hash.clone(),
            },
        );
        add_conflict_version_role(
            &mut views,
            conflict.remote_version.clone(),
            DfsOperatorConflictVersionRole {
                conflict_id: Some(conflict.conflict_id.clone()),
                role: "remote".to_string(),
                content_hash: conflict.remote_content_hash.clone(),
            },
        );
    }

    for (version, view) in &mut views {
        let version_record = backend.get_version(&object.object_id, version)?;
        if let Some(version_record) = version_record {
            if view.content_hash.is_none() {
                view.content_hash = version_record.content_hash.clone();
            }
            view.version_record = Some(version_record);
        }
    }

    Ok(views
        .into_iter()
        .map(|(version, view)| {
            let stored = view.version_record.is_some();
            let visible = view
                .version_record
                .as_ref()
                .map(|record| record.visible)
                .unwrap_or(false);
            let size_bytes = view
                .version_record
                .as_ref()
                .and_then(|record| record.size_bytes);
            let locations = view
                .version_record
                .as_ref()
                .map(|record| normalized_nodes(&record.locations))
                .unwrap_or_default();
            DfsOperatorConflictVersionView {
                version,
                content_hash: view.content_hash,
                size_bytes,
                locations,
                visible,
                current: view.current,
                stored,
                roles: view.roles,
            }
        })
        .collect())
}

fn add_conflict_version_role(
    views: &mut BTreeMap<DfsVersion, ConflictVersionViewBuilder>,
    version: DfsVersion,
    role: DfsOperatorConflictVersionRole,
) {
    let view = views.entry(version).or_default();
    if view.content_hash.is_none() {
        view.content_hash = role.content_hash.clone();
    }
    if !view.roles.contains(&role) {
        view.roles.push(role);
    }
}

fn normalized_nodes(nodes: &[NodeId]) -> Vec<NodeId> {
    let mut nodes = nodes.to_vec();
    normalize_node_locations(&mut nodes);
    nodes
}

fn normalize_node_locations(nodes: &mut Vec<NodeId>) {
    nodes.sort();
    nodes.dedup();
}

fn now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

enum OperatorResumeState {
    SkipFileSystem,
    Continue(Option<DfsOperatorInspectionCursor>),
    Filled(Option<DfsOperatorInspectionCursor>),
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

fn conflict_inspection_cursor(conflict: &DfsConflictRecord) -> DfsOperatorInspectionItemCursor {
    DfsOperatorInspectionItemCursor::Conflict {
        conflict_id: conflict.conflict_id.clone(),
    }
}

fn conflict_page_cursor_from_operator(
    cursor: Option<&DfsOperatorInspectionItemCursor>,
) -> Option<DfsConflictPageCursor> {
    match cursor {
        Some(DfsOperatorInspectionItemCursor::Conflict { conflict_id }) => {
            Some(DfsConflictPageCursor {
                conflict_id: conflict_id.clone(),
            })
        }
        _ => None,
    }
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
    let mut versions = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend.versions_for_object_page(
            object_id,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        versions.extend(page.versions);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(versions)
}

fn receipts_for_version_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> Result<Vec<DfsReplicaReceiptRecord>, MetadataBackendError> {
    let mut receipts = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend.receipts_for_version_page(
            object_id,
            version,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        receipts.extend(page.receipts);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(receipts)
}

fn intents_for_object_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
) -> Result<Vec<DfsWriteIntentRecord>, MetadataBackendError> {
    let mut intents = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend.intents_for_object_page(
            object_id,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        intents.extend(page.intents);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(intents)
}

fn conflicts_for_object_from_backend(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
    let mut conflicts = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend.conflicts_for_object_page(
            object_id,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        conflicts.extend(page.conflicts);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(conflicts)
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
    Ok(repairs_from_intents_and_receipts(
        object,
        intents,
        &receipts_by_version,
    ))
}

fn object_has_open_conflict(
    backend: &dyn DfsMetadataBackend,
    object_id: &DfsObjectId,
) -> Result<bool, MetadataBackendError> {
    let mut cursor = None;
    loop {
        let page = backend.conflicts_for_object_page(
            object_id,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        if page
            .conflicts
            .iter()
            .any(|conflict| conflict.status == DfsConflictStatus::Open)
        {
            return Ok(true);
        }
        let Some(next_cursor) = page.next_cursor else {
            return Ok(false);
        };
        cursor = Some(next_cursor);
    }
}

fn current_version_receipt_nodes_by_hash(
    backend: &dyn DfsMetadataBackend,
    object: &DfsObjectMetadata,
) -> Result<BTreeMap<Option<String>, BTreeSet<NodeId>>, MetadataBackendError> {
    let mut nodes_by_hash = BTreeMap::<Option<String>, BTreeSet<NodeId>>::new();
    let mut cursor = None;
    loop {
        let page = backend.receipts_for_version_page(
            &object.object_id,
            &object.version,
            cursor.as_ref(),
            OPERATOR_OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )?;
        for receipt in page.receipts {
            nodes_by_hash
                .entry(receipt.content_hash.clone())
                .or_default()
                .insert(receipt.replica_node_id);
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
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

fn repairs_from_intents_and_receipts(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
) -> Vec<DfsOperatorRepairView> {
    let mut repairs = Vec::new();
    for intent in intents {
        if intent.status != DfsWriteIntentStatus::Committed
            || intent.target_version != object.version
        {
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
    repairs
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPinMode {
    Metadata,
    Content,
}

impl std::fmt::Display for DfsPinMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Metadata => formatter.write_str("metadata"),
            Self::Content => formatter.write_str("content"),
        }
    }
}

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
        let legacy_path = self.legacy_pin_path(record);
        if legacy_path != path && legacy_path.is_file() {
            let legacy_record: Result<DfsPinRecord, DfsPinStoreError> = read_object(&legacy_path);
            if legacy_record.is_ok_and(|legacy_record| {
                legacy_record.file_system_id == record.file_system_id
                    && normalize_path(&legacy_record.path) == normalize_path(&record.path)
                    && legacy_record.pinned_by_node_id == record.pinned_by_node_id
                    && legacy_record.mode == record.mode
            }) {
                let _ = fs::remove_file(&legacy_path);
            }
        }
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

    fn legacy_pin_path(&self, record: &DfsPinRecord) -> PathBuf {
        self.root.join(format!(
            "{}-{}-{}.sobj",
            key_segment(&record.file_system_id),
            path_hash(&record.path),
            key_segment(&record.pinned_by_node_id)
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
    conflicts: &[DfsConflictRecord],
    repairs: &[DfsOperatorRepairView],
) -> DfsAgentObjectState {
    let has_open_conflict = conflicts
        .iter()
        .any(|conflict| conflict.status == DfsConflictStatus::Open);
    agent_state_from_flags(object, intents, has_open_conflict, repairs)
}

fn agent_state_from_flags(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    has_open_conflict: bool,
    repairs: &[DfsOperatorRepairView],
) -> DfsAgentObjectState {
    if has_open_conflict {
        return DfsAgentObjectState::Conflicted;
    }
    if intents
        .iter()
        .any(|intent| write_intent_is_active(&intent.status))
    {
        return DfsAgentObjectState::PendingWrite;
    }
    if !repairs.is_empty() {
        return DfsAgentObjectState::PendingRepair;
    }
    if object.content_hash.is_some() {
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
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use sinomo_events::EventId;

    #[test]
    fn operator_inspector_reports_status_object_conflicts_and_repairs() {
        let root = temp_root("operator_inspector_reports_status_object_conflicts_and_repairs");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(1024),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_2"),
                content_hash: Some("blake3:new".to_string()),
                size_bytes: Some(3),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_commit").expect("event"),
                },
                replica_node_id: node("node_owner"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_1".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some("blake3:new".to_string()),
                object_metadata: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent");
        backend
            .put_conflict(&DfsConflictRecord {
                conflict_id: "conflict_a".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                path: "artifact.bin".to_string(),
                base_version: Some(version("version_1")),
                local_version: version("version_2_local"),
                remote_version: version("version_2"),
                detected_by_node_id: node("node_laptop"),
                reason: "offline edit".to_string(),
                local_content_hash: Some("blake3:local".to_string()),
                remote_content_hash: Some("blake3:new".to_string()),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("conflict");

        let inspector = DfsOperatorInspector::new(&backend);
        let status = inspector.status().expect("status");
        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.open_conflict_count, 1);
        assert_eq!(status.pending_repair_count, 1);

        let object = inspector
            .object(&object_id())
            .expect("object")
            .expect("object");
        assert_eq!(object.agent_state, DfsAgentObjectState::Conflicted);
        assert_eq!(
            object.repairs[0].missing_replica_node_id,
            node("node_replica")
        );
        let path_object = inspector
            .object_by_path(&file_system_id(), "/artifact.bin")
            .expect("path object")
            .expect("path object");
        assert_eq!(path_object.object.object_id, object_id());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_inspector_counts_staging_intents_as_pending_writes() {
        let root = temp_root("operator_inspector_counts_staging_intents_as_pending_writes");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: Some(512),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_1"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_staging".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::Staging,
                content_hash: Some("blake3:new".to_string()),
                object_metadata: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent");

        let inspector = DfsOperatorInspector::new(&backend);
        let status = inspector.status().expect("status");
        assert_eq!(status.pending_write_count, 1);
        assert_eq!(
            inspector
                .object(&object_id())
                .expect("object")
                .expect("object")
                .agent_state,
            DfsAgentObjectState::PendingWrite
        );

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_inspector_ignores_superseded_version_repairs() {
        let root = temp_root("operator_inspector_ignores_superseded_version_repairs");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner"), node("node_replica")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(1024),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        for (version_id, hash) in [
            (version("version_1"), "blake3:old"),
            (version("version_2"), "blake3:new"),
        ] {
            backend
                .put_version(&DfsObjectVersionRecord {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    version: version_id.clone(),
                    content_hash: Some(hash.to_string()),
                    size_bytes: Some(3),
                    locations: vec![node("node_owner"), node("node_replica")],
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    visible: true,
                })
                .expect("version");
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id(),
                        object_id: object_id(),
                        committed_version: version_id,
                        owner_node_id: node("node_owner"),
                        event_id: EventId::from_legacy_named(format!("evt_{hash}")).expect("event"),
                    },
                    replica_node_id: node("node_owner"),
                    content_hash: Some(hash.to_string()),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("owner receipt");
        }
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_current_replica").expect("event"),
                },
                replica_node_id: node("node_replica"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("replica receipt");
        for (intent_id, target_version, hash) in [
            ("dfs_intent_old", version("version_1"), "blake3:old"),
            ("dfs_intent_current", version("version_2"), "blake3:new"),
        ] {
            backend
                .put_intent(&DfsWriteIntentRecord {
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    base_version: None,
                    target_version,
                    coordinator_node_id: node("node_owner"),
                    owner_epoch: 1,
                    desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                    policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                    status: DfsWriteIntentStatus::Committed,
                    content_hash: Some(hash.to_string()),
                    object_metadata: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                })
                .expect("intent");
        }

        let inspector = DfsOperatorInspector::new(&backend);
        assert_eq!(
            inspector.status().expect("status").pending_repair_count,
            0,
            "a superseded version missing an old receipt must not keep the current object pending repair"
        );
        assert_eq!(
            inspector
                .object(&object_id())
                .expect("object")
                .expect("object")
                .agent_state,
            DfsAgentObjectState::Current
        );

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_inspection_scans_conflicts_repairs_and_agent_summary_in_pages() {
        let root = temp_root("operator_inspection_scans_conflicts_repairs_in_pages");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend
            .put_file_system(&DfsFileSystem {
                file_system_id: file_system_id(),
                display_name: "repo".to_string(),
                owner_node_id: node("node_owner"),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem");

        let make_object = |object_id: DfsObjectId, path: String| DfsObjectMetadata {
            object_id,
            file_system_id: file_system_id(),
            path,
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(3),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };

        for index in 0..OPERATOR_OBJECT_SCAN_PAGE_SIZE {
            backend
                .put_object(&make_object(
                    DfsObjectId::from_legacy_named(format!("obj_operator_page_filler_{index}"))
                        .expect("filler object"),
                    format!("a/{index:05}.bin"),
                ))
                .expect("filler object");
        }

        let target_object_id =
            DfsObjectId::from_legacy_named("obj_operator_page_target").expect("target object");
        backend
            .put_object(&make_object(
                target_object_id.clone(),
                "z/target.bin".to_string(),
            ))
            .expect("target object");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: target_object_id.clone(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_page_target").expect("event"),
                },
                replica_node_id: node("node_owner"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("target receipt");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_page_target".to_string(),
                file_system_id: file_system_id(),
                object_id: target_object_id.clone(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![
                    node("node_owner"),
                    node("node_replica_a"),
                    node("node_replica_b"),
                ],
                policy: DfsReplicationPolicy::new(1, 3).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some("blake3:new".to_string()),
                object_metadata: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("target intent");
        for conflict_id in ["conflict_page_target_a", "conflict_page_target_b"] {
            backend
                .put_conflict(&DfsConflictRecord {
                    conflict_id: conflict_id.to_string(),
                    file_system_id: file_system_id(),
                    object_id: target_object_id.clone(),
                    path: "z/target.bin".to_string(),
                    base_version: Some(version("version_1")),
                    local_version: version("version_2_local"),
                    remote_version: version("version_2"),
                    detected_by_node_id: node("node_laptop"),
                    reason: "offline edit".to_string(),
                    local_content_hash: Some("blake3:local".to_string()),
                    remote_content_hash: Some("blake3:new".to_string()),
                    created_by_intent_id: None,
                    strategy: None,
                    status: DfsConflictStatus::Open,
                    detected_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("target conflict");
        }

        let inspector = DfsOperatorInspector::new(&backend);
        let conflicts = inspector
            .conflicts(Some(&file_system_id()), None)
            .expect("conflicts");
        assert_eq!(conflicts.len(), 2);
        assert_eq!(&conflicts[0].object_id, &target_object_id);
        let first_conflict_page = inspector
            .conflicts_page(Some(&file_system_id()), None, Some(1), None)
            .expect("first conflict page");
        assert_eq!(first_conflict_page.conflicts.len(), 1);
        assert_eq!(
            first_conflict_page.conflicts[0].conflict_id,
            "conflict_page_target_a"
        );
        assert_eq!(
            first_conflict_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.object_id.as_ref()),
            Some(&target_object_id)
        );
        assert_eq!(
            first_conflict_page
                .next_cursor
                .as_ref()
                .and_then(|cursor| cursor.item_key.as_ref()),
            Some(&DfsOperatorInspectionItemCursor::Conflict {
                conflict_id: "conflict_page_target_a".to_string()
            })
        );
        let second_conflict_page = inspector
            .conflicts_page(
                Some(&file_system_id()),
                None,
                Some(1),
                first_conflict_page.next_cursor.as_ref(),
            )
            .expect("second conflict page");
        assert_eq!(second_conflict_page.conflicts.len(), 1);
        assert_eq!(
            second_conflict_page.conflicts[0].conflict_id,
            "conflict_page_target_b"
        );
        assert!(second_conflict_page.next_cursor.is_none());

        let repairs = inspector
            .repairs(Some(&file_system_id()), None)
            .expect("repairs");
        assert_eq!(repairs.len(), 2);
        assert_eq!(&repairs[0].object_id, &target_object_id);
        let first_repair_page = inspector
            .repairs_page(Some(&file_system_id()), None, Some(1), None)
            .expect("first repair page");
        assert_eq!(first_repair_page.repairs.len(), 1);
        let second_repair_page = inspector
            .repairs_page(
                Some(&file_system_id()),
                None,
                Some(1),
                first_repair_page.next_cursor.as_ref(),
            )
            .expect("second repair page");
        assert_eq!(second_repair_page.repairs.len(), 1);
        let paged_missing_replicas = BTreeSet::from([
            first_repair_page.repairs[0].missing_replica_node_id.clone(),
            second_repair_page.repairs[0]
                .missing_replica_node_id
                .clone(),
        ]);
        assert_eq!(
            paged_missing_replicas,
            BTreeSet::from([node("node_replica_a"), node("node_replica_b")])
        );
        assert!(second_repair_page.next_cursor.is_none());

        let agent_summary = inspector
            .agent_file_system_summary(&file_system_id())
            .expect("agent summary")
            .expect("filesystem");
        assert_eq!(
            agent_summary.objects.len(),
            OPERATOR_OBJECT_SCAN_PAGE_SIZE + 1
        );
        let target_summary = agent_summary
            .objects
            .iter()
            .find(|object| object.object_id == target_object_id)
            .expect("target summary");
        assert_eq!(target_summary.state, DfsAgentObjectState::Conflicted);

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_repair_pages_preserve_order_without_full_object_materialization() {
        let root = temp_root("operator_repair_pages_preserve_order");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(1024),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        for (replica_node_id, content_hash) in [
            (node("node_owner"), Some("blake3:new".to_string())),
            (
                node("node_seen_other_hash"),
                Some("blake3:other".to_string()),
            ),
        ] {
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id(),
                        object_id: object_id(),
                        committed_version: version("version_2"),
                        owner_node_id: node("node_owner"),
                        event_id: EventId::generate(),
                    },
                    replica_node_id,
                    content_hash,
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("receipt");
        }
        for (intent_id, content_hash, desired_replica_node_ids) in [
            (
                "dfs_intent_missing_b",
                Some("blake3:new".to_string()),
                vec![node("node_owner"), node("node_b")],
            ),
            (
                "dfs_intent_missing_a",
                Some("blake3:new".to_string()),
                vec![node("node_owner"), node("node_a")],
            ),
            (
                "dfs_intent_missing_c_hashless",
                None,
                vec![
                    node("node_owner"),
                    node("node_seen_other_hash"),
                    node("node_c"),
                ],
            ),
        ] {
            backend
                .put_intent(&DfsWriteIntentRecord {
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    base_version: Some(version("version_1")),
                    target_version: version("version_2"),
                    coordinator_node_id: node("node_owner"),
                    owner_epoch: 1,
                    desired_replica_node_ids,
                    policy: DfsReplicationPolicy::new(1, 3).expect("policy"),
                    status: DfsWriteIntentStatus::Committed,
                    content_hash,
                    object_metadata: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                })
                .expect("intent");
        }

        let inspector = DfsOperatorInspector::new(&backend);
        let first_page = inspector
            .repairs_page(None, Some(&object_id()), Some(2), None)
            .expect("first page");
        assert_eq!(
            first_page
                .repairs
                .iter()
                .map(|repair| repair.missing_replica_node_id.clone())
                .collect::<Vec<_>>(),
            vec![node("node_a"), node("node_b")]
        );
        assert!(first_page.next_cursor.is_some());
        let second_page = inspector
            .repairs_page(
                None,
                Some(&object_id()),
                Some(2),
                first_page.next_cursor.as_ref(),
            )
            .expect("second page");
        assert_eq!(
            second_page
                .repairs
                .iter()
                .map(|repair| repair.missing_replica_node_id.clone())
                .collect::<Vec<_>>(),
            vec![node("node_c")]
        );
        assert!(second_page.next_cursor.is_none());

        fs::remove_dir_all(&root).expect("cleanup");
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
    fn local_pin_store_removes_matching_legacy_pin_by_mode() {
        let root = temp_root("local_pin_store_removes_matching_legacy_pin_by_mode");
        let store = LocalDfsPinStore::new(&root);
        let owner = node("node_laptop");
        let record = DfsPinRecord {
            file_system_id: file_system_id(),
            path: "src/lib.rs".to_string(),
            object_id: Some(object_id()),
            pinned_by_node_id: owner.clone(),
            mode: DfsPinMode::Content,
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let legacy_path = store.legacy_pin_path(&record);
        write_object_atomic(&legacy_path, &record).expect("write legacy pin");

        assert_eq!(store.list_pins(None).expect("pins"), vec![record]);
        assert!(
            store
                .remove_pin(
                    &file_system_id(),
                    "src/lib.rs",
                    Some(&owner),
                    Some(&DfsPinMode::Content),
                )
                .expect("remove legacy content pin")
        );
        assert!(!legacy_path.exists());
        assert!(store.list_pins(None).expect("pins").is_empty());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_status_aggregates_counts_across_multiple_objects() {
        let root = temp_root("operator_status_aggregates_counts_across_multiple_objects");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        backend
            .put_file_system(&DfsFileSystem {
                file_system_id: file_system_id(),
                display_name: "repo".to_string(),
                owner_node_id: node("node_owner"),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem");

        let object_a = DfsObjectId::from_legacy_named("obj_alpha").expect("object");
        let object_b = DfsObjectId::from_legacy_named("obj_beta").expect("object");
        let make_object =
            |object_id: &DfsObjectId, path: &str, current: DfsVersion| DfsObjectMetadata {
                object_id: object_id.clone(),
                file_system_id: file_system_id(),
                path: path.to_string(),
                kind: DfsObjectKind::Artifact,
                owner_node_id: node("node_owner"),
                current_locations: vec![node("node_owner")],
                content_hash: Some("blake3:new".to_string()),
                content_size_bytes: Some(3),
                link_target_object_id: None,
                mode: 0o644,
                uid: 0,
                gid: 0,
                accessed_at_unix_nanos: 0,
                modified_at_unix_nanos: 0,
                created_at_unix_nanos: 0,
                changed_at_unix_nanos: 0,
                version: current,
                policy: DfsObjectPolicy::ReplicatedEventual,
                created_by_event_id: None,
                last_modified_by_event_id: None,
            };
        backend
            .put_object(&make_object(&object_a, "alpha.bin", version("version_2")))
            .expect("object a");
        backend
            .put_object(&make_object(&object_b, "beta.bin", version("version_1")))
            .expect("object b");

        // object_a: two versions, one receipt, a committed intent missing a
        // replica (one repair), and an open conflict.
        for version_id in [version("version_1"), version("version_2")] {
            backend
                .put_version(&DfsObjectVersionRecord {
                    file_system_id: file_system_id(),
                    object_id: object_a.clone(),
                    version: version_id.clone(),
                    content_hash: Some("blake3:new".to_string()),
                    size_bytes: Some(3),
                    locations: vec![node("node_owner")],
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    visible: true,
                })
                .expect("version a");
        }
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_a.clone(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_a").expect("event"),
                },
                replica_node_id: node("node_owner"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt a");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_a".to_string(),
                file_system_id: file_system_id(),
                object_id: object_a.clone(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some("blake3:new".to_string()),
                object_metadata: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent a");
        backend
            .put_conflict(&DfsConflictRecord {
                conflict_id: "conflict_a".to_string(),
                file_system_id: file_system_id(),
                object_id: object_a.clone(),
                path: "alpha.bin".to_string(),
                base_version: Some(version("version_1")),
                local_version: version("version_2_local"),
                remote_version: version("version_2"),
                detected_by_node_id: node("node_laptop"),
                reason: "offline edit".to_string(),
                local_content_hash: Some("blake3:local".to_string()),
                remote_content_hash: Some("blake3:new".to_string()),
                created_by_intent_id: None,
                strategy: None,
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("conflict a");

        // object_b: one version, one receipt, and a staging intent (pending write,
        // no repair, no conflict).
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_b.clone(),
                version: version("version_1"),
                content_hash: Some("blake3:new".to_string()),
                size_bytes: Some(3),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version b");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_b.clone(),
                    committed_version: version("version_1"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_b").expect("event"),
                },
                replica_node_id: node("node_owner"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt b");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_b".to_string(),
                file_system_id: file_system_id(),
                object_id: object_b.clone(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::Staging,
                content_hash: Some("blake3:newer".to_string()),
                object_metadata: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent b");

        // Bulk accessors return whole-backend totals in one call each.
        assert_eq!(backend.all_versions().expect("versions").len(), 3);
        assert_eq!(backend.all_receipts().expect("receipts").len(), 2);
        assert_eq!(backend.all_intents().expect("intents").len(), 2);
        assert_eq!(backend.all_conflicts().expect("conflicts").len(), 1);
        backend
            .put_namespace_job(&DfsNamespaceJobRecord {
                job_id: "namespace-job-running".to_string(),
                file_system_id: file_system_id(),
                kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
                status: DfsNamespaceJobStatus::Running,
                root_path: "/alpha".to_string(),
                target_path: None,
                pending_frames: vec![DfsNamespaceJobFrame::ScanDirectory {
                    path: "/alpha".to_string(),
                    cursor: None,
                }],
                processed_object_count: 0,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
                completed_at: None,
                failure_reason: None,
            })
            .expect("running namespace job");
        backend
            .put_namespace_job(&DfsNamespaceJobRecord {
                job_id: "namespace-job-failed".to_string(),
                file_system_id: file_system_id(),
                kind: DfsNamespaceJobKind::RenameDirectory,
                status: DfsNamespaceJobStatus::Failed,
                root_path: "/beta".to_string(),
                target_path: Some("/gamma".to_string()),
                pending_frames: Vec::new(),
                processed_object_count: 0,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
                completed_at: None,
                failure_reason: Some("invalid destination".to_string()),
            })
            .expect("failed namespace job");

        let summary = backend.metadata_summary().expect("summary");
        assert_eq!(summary.file_system_count, 1);
        assert_eq!(summary.object_count, 2);
        assert_eq!(summary.version_count, 3);
        assert_eq!(summary.receipt_count, 2);
        assert_eq!(summary.intent_count, 2);
        assert_eq!(summary.pending_write_count, 1);
        assert_eq!(summary.open_conflict_count, 1);
        assert_eq!(summary.pending_repair_count, 1);
        assert_eq!(summary.pending_namespace_job_count, 1);
        assert_eq!(summary.failed_namespace_job_count, 1);
        let scoped_summary = backend
            .metadata_summary_for_file_system(&file_system_id())
            .expect("scoped summary")
            .expect("filesystem summary");
        assert_eq!(scoped_summary.file_system_count, 1);
        assert_eq!(scoped_summary.object_count, 2);
        assert_eq!(scoped_summary.version_count, 3);
        assert_eq!(scoped_summary.open_conflict_count, 1);
        assert_eq!(scoped_summary.pending_repair_count, 1);
        assert!(
            backend
                .metadata_summary_for_file_system(
                    &DfsFileSystemId::from_legacy_named("missing_filesystem")
                        .expect("missing filesystem id")
                )
                .expect("missing scoped summary")
                .is_none()
        );

        let inspector = DfsOperatorInspector::new(&backend);
        let status = inspector.status().expect("status");
        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 2);
        assert_eq!(status.version_count, 3);
        assert_eq!(status.receipt_count, 2);
        assert_eq!(status.intent_count, 2);
        assert_eq!(status.pending_write_count, 1, "object_b staging intent");
        assert_eq!(status.open_conflict_count, 1, "object_a open conflict");
        assert_eq!(
            status.pending_repair_count, 1,
            "object_a committed intent is missing node_replica"
        );
        assert_eq!(status.pending_namespace_job_count, 1);
        assert_eq!(status.failed_namespace_job_count, 1);
        let file_system = inspector
            .file_system(&file_system_id())
            .expect("filesystem view")
            .expect("filesystem");
        assert_eq!(file_system.object_count, 2);
        assert_eq!(file_system.open_conflict_count, 1);
        assert_eq!(file_system.pending_repair_count, 1);

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_conflict_versions_can_be_viewed_and_selected() {
        let root = temp_root("operator_conflict_versions_can_be_viewed_and_selected");
        let mut backend =
            LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:local".to_string()),
            content_size_bytes: Some(5),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_local"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        for (version_id, hash, size, visible) in [
            (version("version_base"), "blake3:base", 4, false),
            (version("version_local"), "blake3:local", 5, true),
            (version("version_remote"), "blake3:remote", 6, false),
        ] {
            backend
                .put_version(&DfsObjectVersionRecord {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    version: version_id,
                    content_hash: Some(hash.to_string()),
                    size_bytes: Some(size),
                    locations: vec![node("node_owner")],
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    visible,
                })
                .expect("version");
        }
        backend
            .put_conflict(&DfsConflictRecord {
                conflict_id: "conflict_a".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                path: "artifact.bin".to_string(),
                base_version: Some(version("version_base")),
                local_version: version("version_local"),
                remote_version: version("version_remote"),
                detected_by_node_id: node("node_owner"),
                reason: "manual test conflict".to_string(),
                local_content_hash: Some("blake3:local".to_string()),
                remote_content_hash: Some("blake3:remote".to_string()),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("conflict");

        let report = DfsOperatorInspector::new(&backend)
            .conflict_versions(&object_id(), &[])
            .expect("versions")
            .expect("report");
        assert_eq!(report.versions.len(), 3);
        let remote = report
            .versions
            .iter()
            .find(|candidate| candidate.version == version("version_remote"))
            .expect("remote version");
        assert!(remote.stored);
        assert_eq!(remote.content_hash.as_deref(), Some("blake3:remote"));
        assert!(remote.roles.iter().any(|role| role.role == "remote"));
        let current = report
            .versions
            .iter()
            .find(|candidate| candidate.version == version("version_local"))
            .expect("current version");
        assert!(current.current);
        assert!(current.roles.iter().any(|role| role.role == "current"));

        let use_report = use_dfs_conflict_version(
            &mut backend,
            &object_id(),
            &version("version_remote"),
            &["conflict_a".to_string()],
        )
        .expect("use version");
        assert!(use_report.changed);
        assert_eq!(
            use_report.committed_object.version,
            version("version_remote")
        );
        assert_eq!(
            use_report.committed_object.content_hash.as_deref(),
            Some("blake3:remote")
        );
        assert_eq!(use_report.removed.len(), 1);
        assert!(
            backend
                .get_conflict(&object_id(), "conflict_a")
                .expect("conflict lookup")
                .is_none()
        );
        let committed = backend
            .get_object(&object_id())
            .expect("object lookup")
            .expect("object");
        assert_eq!(committed.version, version("version_remote"));

        fs::remove_dir_all(&root).expect("cleanup");
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
