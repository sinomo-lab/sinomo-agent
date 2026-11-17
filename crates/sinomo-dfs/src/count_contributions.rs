//! One shard's contributions to another shard's maintained namespace counts.
//!
//! A directory counts 2 plus its child directories; a canonical object counts
//! 1 plus its hardlink aliases. Shard T converting from an older format reads
//! from every other shard S the live aliases S owns, the live directories S
//! owns whose parent path T owns, and S's Running namespace jobs, so T can
//! account for moves that are still in flight. This is read data from S's
//! current primary, bound to one exact metadata prefix; it grants no write or
//! publication authority.
//!
//! The same read answers T's question about what S holds: which object ids
//! are live canonical rows S owns, which paths are live directories S owns,
//! and which job ids are Running jobs S records. A converting shard asks this
//! to learn whether the targets of its aliases and the parents of its rows
//! exist in another shard, and whether a fence it settles is still held.
//! Every answer of one read is bound to the same prefix as its pages.

use crate::*;
use serde::{Deserialize, Serialize};
use std::ops::Bound::{Excluded, Unbounded};

/// Largest number of reported entries (aliases, child directories and jobs
/// together) one contribution page carries.
pub(crate) const DFS_COUNT_CONTRIBUTION_PAGE_LIMIT: usize = 512;

/// Largest number of entries (objects, directories and jobs together) one
/// request's question asks.
pub const DFS_COUNT_CONTRIBUTION_QUESTION_LIMIT: usize = 512;

/// Rows one page may visit per reportable entry, whether or not they are
/// reported. Child directories have no index by parent ownership, so that
/// section walks S's objects table in object-id order: a full read costs
/// O(objects in S) rows spread over pages of at most
/// `limit * DFS_COUNT_CONTRIBUTION_VISITS_PER_ENTRY` visits each. Aliases cost
/// O(alias rows in S) and jobs O(job rows in S).
const DFS_COUNT_CONTRIBUTION_VISITS_PER_ENTRY: usize = 64;

/// Where the next page resumes: after this exact row of its section. Sections
/// are read in the order aliases, directories, jobs.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsCountContributionCursor {
    Aliases {
        target_object_id: DfsObjectId,
        alias_object_id: DfsObjectId,
    },
    Directories {
        object_id: DfsObjectId,
    },
    Jobs {
        job_id: String,
    },
    /// Every section is read; the request only asks its question.
    Complete,
}

/// What a converting target asks the source beside its contributions.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsCountContributionQuestion {
    /// Object ids that may be live canonical rows of the source.
    #[serde(default)]
    pub objects: Vec<DfsObjectId>,
    /// Canonical paths that may be live directories of the source.
    #[serde(default)]
    pub directories: Vec<String>,
    /// Job ids that may be Running jobs of the source.
    #[serde(default)]
    pub jobs: Vec<String>,
}

impl DfsCountContributionQuestion {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.objects.len() + self.directories.len() + self.jobs.len()
    }
}

/// The entries of a request's question the source holds, in question order.
/// `answered` counts the entries the source evaluated, so an answer from a
/// source that did not read the question is never taken as "holds none".
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsCountContributionHoldings {
    #[serde(default)]
    pub answered: u64,
    /// Live canonical (non-alias) rows whose path the source owns.
    #[serde(default)]
    pub objects: Vec<DfsObjectId>,
    /// Paths the source owns that hold a live directory.
    #[serde(default)]
    pub directories: Vec<String>,
    /// Running namespace jobs of the source.
    #[serde(default)]
    pub jobs: Vec<String>,
}

impl DfsCountContributionHoldings {
    /// Whether this answers exactly `question`: every entry evaluated and
    /// every held entry asked.
    pub fn answers(&self, question: &DfsCountContributionQuestion) -> bool {
        usize::try_from(self.answered).is_ok_and(|answered| answered == question.len())
            && self.objects.iter().all(|id| question.objects.contains(id))
            && self
                .directories
                .iter()
                .all(|path| question.directories.contains(path))
            && self.jobs.iter().all(|id| question.jobs.contains(id))
    }
}

/// A live hardlink alias whose path the source shard owns. The source cannot
/// tell which shard owns the target, so every live alias is reported.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsAliasContribution {
    pub alias_object_id: DfsObjectId,
    pub alias_path: String,
    pub target_object_id: DfsObjectId,
}

/// A live directory the source shard owns whose parent path the target owns.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsChildDirectoryContribution {
    pub parent_path: String,
    pub child_object_id: DfsObjectId,
    pub child_path: String,
}

/// A Running namespace job recorded on the source shard. `carried` holds the
/// objects its pending frames move: the `moved_object` of `CrossShardAdopt`
/// and `CrossShardComplete`, and the `alias` of `CrossShardHardlinkAdopt`,
/// once per object id in frame order. Alias mutation transactions and target
/// fence successors are not carried: those jobs change a count only through
/// the target's own fence record, and are identified here by id, kind and
/// paths.
///
/// A directory move parked at a scan page carries that page's objects in no
/// frame yet: each is still a live row at its old path in the shard that
/// serves the page, and is reported, at most once, among that shard's rows.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRunningJobContribution {
    pub job_id: String,
    pub kind: DfsNamespaceJobKind,
    pub root_path: String,
    pub target_path: Option<String>,
    pub carried: Vec<DfsObjectMetadata>,
    /// The shards whose storage the job's next step waits to be Active, as
    /// the serving primary recorded when it refused that step; empty while
    /// the job does not wait there. Its job cannot run that step, or any
    /// later one, until each of these shards activates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_for_storage_of: Vec<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsCountContributionPage {
    pub source_shard: String,
    /// Every page of one read names the same prefix; a continuation is
    /// refused with `CountContributionsChangedV1` once it moves.
    pub prefix: DfsMetadataPrefixProof,
    pub aliases: Vec<DfsAliasContribution>,
    pub child_directories: Vec<DfsChildDirectoryContribution>,
    pub running_jobs: Vec<DfsRunningJobContribution>,
    /// A page may report nothing and still advance `after`, when its visit
    /// budget ran out on unreported rows.
    pub after: Option<DfsCountContributionCursor>,
    /// The answer to the request's question, bound to `prefix`.
    #[serde(default)]
    pub held: DfsCountContributionHoldings,
    /// Explicitly required on the wire; omission is not an empty final page.
    pub complete: bool,
}

impl DfsCountContributionPage {
    pub(crate) fn entry_count(&self) -> usize {
        self.aliases.len() + self.child_directories.len() + self.running_jobs.len()
    }
}

/// What the serving primary recorded about a Running job's next step: the
/// shards whose storage it waits on (see `maintenance::storage_waits`).
pub(crate) type DfsJobStorageWaits<'a> = &'a dyn Fn(&DfsNamespaceJobRecord) -> Vec<DfsMetadataShardId>;

/// Serve `count-contributions-v1` from one shard's tables and the prefix
/// those tables materialize. `listable` is the backend's additional
/// visibility rule for rows whose path the catalog gives this shard;
/// `storage_waits` names, for a reported job as these tables hold it, the
/// shards its next step waits on.
pub(crate) fn query_count_contributions(
    tables: &crate::metadata_tables::DfsMetadataTables,
    query: &DfsMetadataShardQuery,
    catalog: &[DfsMetadataShardConfig],
    prefix: DfsMetadataPrefixProof,
    listable: &mut dyn FnMut(&DfsObjectMetadata) -> Result<bool, MetadataBackendError>,
    storage_waits: DfsJobStorageWaits<'_>,
) -> Result<DfsMetadataQueryResult, MetadataBackendError> {
    let invalid = |message: &str| MetadataBackendError::Serde(message.to_owned());
    let DfsMetadataSelection::CountContributionsV1 {
        target_shard,
        expected_prefix,
        after,
        question,
    } = &query.selection
    else {
        return Err(invalid(
            "count contribution backend received another selection",
        ));
    };
    if !prefix.is_well_formed() {
        return Err(invalid(
            "count contributions have no exact materialized prefix",
        ));
    }
    if question.len() > DFS_COUNT_CONTRIBUTION_QUESTION_LIMIT {
        return Err(invalid("count contribution question asks too many entries"));
    }
    if (after.is_some() && expected_prefix.is_none())
        || expected_prefix
            .as_ref()
            .is_some_and(|expected| !expected.is_well_formed())
    {
        return Err(invalid("invalid count contribution continuation"));
    }
    if expected_prefix
        .as_ref()
        .is_some_and(|expected| expected != &prefix)
    {
        return Ok(DfsMetadataQueryResult::CountContributionsChangedV1 { prefix });
    }
    let mut page = count_contributions_page(
        tables,
        &query.file_system_id,
        &query.shard_id.0,
        catalog,
        target_shard,
        after.as_ref(),
        DFS_COUNT_CONTRIBUTION_PAGE_LIMIT,
        prefix,
        listable,
    )?;
    for job in &mut page.running_jobs {
        if let Some(record) = tables.namespace_jobs.get(&job.job_id) {
            job.waiting_for_storage_of = storage_waits(record);
        }
    }
    page.held = held_entries(
        tables,
        &query.file_system_id,
        &query.shard_id.0,
        catalog,
        question,
    )?;
    Ok(DfsMetadataQueryResult::CountContributionsV1 { page })
}

/// Answer `question` from `source_shard`'s tables: the asked objects that are
/// live canonical rows at a path the catalog gives the source, the asked
/// paths the source owns that hold a live directory, and the asked jobs the
/// source records as Running. Only the request's catalog decides ownership.
/// Holdings are not subject to the backend's residue rule: a live row a
/// failed move cleanup left behind names an object that exists at the move's
/// destination, so it never makes an existing object look absent.
pub(crate) fn held_entries(
    tables: &crate::metadata_tables::DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    source_shard: &str,
    catalog: &[DfsMetadataShardConfig],
    question: &DfsCountContributionQuestion,
) -> Result<DfsCountContributionHoldings, MetadataBackendError> {
    let mut held = DfsCountContributionHoldings {
        answered: u64::try_from(question.len())
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?,
        ..Default::default()
    };
    if question.is_empty() {
        return Ok(held);
    }
    let routing =
        crate::control_plane::DfsMetadataShardRoutingIndex::new(file_system_id.clone(), catalog)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    let owned = |path: &str| -> Result<bool, MetadataBackendError> {
        routing
            .select_shard_id(path, true)
            .map(|shard| shard == Some(source_shard))
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))
    };
    let live = |object: &DfsObjectMetadata| {
        object.file_system_id == *file_system_id
            && object.kind != DfsObjectKind::Tombstone
            && object.tombstone_provenance.is_none()
    };
    for object_id in &question.objects {
        let Some(object) = tables.objects.get(object_id) else {
            continue;
        };
        if object.object_id == *object_id
            && live(object)
            && object.kind != DfsObjectKind::Hardlink
            && crate::metadata_pages::object_is_visible_at_path_from_tables(
                tables,
                file_system_id,
                object,
            )
            && owned(&object.path)?
        {
            held.objects.push(object_id.clone());
        }
    }
    for path in &question.directories {
        if crate::metadata_keys::normalize_posix_path(path) != *path {
            return Err(MetadataBackendError::Serde(
                "count contribution question names a noncanonical path".into(),
            ));
        }
        if !owned(path)? {
            continue;
        }
        let Some(object) =
            crate::metadata_pages::object_by_path_from_tables(tables, file_system_id, path)
        else {
            continue;
        };
        if object.kind == DfsObjectKind::Directory
            && live(&object)
            && crate::metadata_keys::normalize_posix_path(&object.path) == *path
        {
            held.directories.push(path.clone());
        }
    }
    for job_id in &question.jobs {
        if tables.namespace_jobs.get(job_id).is_some_and(|job| {
            job.job_id == *job_id
                && job.file_system_id == *file_system_id
                && job.status == DfsNamespaceJobStatus::Running
        }) {
            held.jobs.push(job_id.clone());
        }
    }
    Ok(held)
}

/// One bounded page of `source_shard`'s contributions to `target_shard`.
/// Ownership of every reported path is decided by `catalog`, so a physical
/// copy of a path the source no longer owns is never reported.
#[allow(clippy::too_many_arguments)]
pub(crate) fn count_contributions_page(
    tables: &crate::metadata_tables::DfsMetadataTables,
    file_system_id: &DfsFileSystemId,
    source_shard: &str,
    catalog: &[DfsMetadataShardConfig],
    target_shard: &str,
    after: Option<&DfsCountContributionCursor>,
    limit: usize,
    prefix: DfsMetadataPrefixProof,
    listable: &mut dyn FnMut(&DfsObjectMetadata) -> Result<bool, MetadataBackendError>,
) -> Result<DfsCountContributionPage, MetadataBackendError> {
    let invalid = |message: &str| MetadataBackendError::Serde(message.to_owned());
    if limit == 0 || limit > DFS_COUNT_CONTRIBUTION_PAGE_LIMIT {
        return Err(invalid("count contribution page limit must be in 1..=512"));
    }
    let in_catalog = |shard_id: &str| {
        catalog
            .iter()
            .any(|shard| shard.file_system_id == *file_system_id && shard.shard_id == shard_id)
    };
    if source_shard == target_shard || !in_catalog(source_shard) || !in_catalog(target_shard) {
        return Err(invalid(
            "count contributions need two distinct shards of the current catalog",
        ));
    }
    let routing =
        crate::control_plane::DfsMetadataShardRoutingIndex::new(file_system_id.clone(), catalog)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    let owner = |path: &str| -> Result<Option<String>, MetadataBackendError> {
        routing
            .select_shard_id(path, true)
            .map(|shard| shard.map(str::to_owned))
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))
    };
    let mut page = DfsCountContributionPage {
        source_shard: source_shard.to_owned(),
        prefix,
        aliases: Vec::new(),
        child_directories: Vec::new(),
        running_jobs: Vec::new(),
        after: None,
        complete: false,
        held: DfsCountContributionHoldings::default(),
    };
    if matches!(after, Some(DfsCountContributionCursor::Complete)) {
        page.complete = true;
        return Ok(page);
    }
    let mut visits = limit.saturating_mul(DFS_COUNT_CONTRIBUTION_VISITS_PER_ENTRY);
    let mut last = None::<DfsCountContributionCursor>;
    // Checked before each row, so a page that reaches the end of the last
    // section is complete even when it is also full.
    macro_rules! visit {
        ($cursor:expr) => {
            if page.entry_count() >= limit || visits == 0 {
                page.after = last;
                return Ok(page);
            }
            visits -= 1;
            last = Some($cursor);
        };
    }

    if matches!(
        after,
        None | Some(DfsCountContributionCursor::Aliases { .. })
    ) {
        let start = match after {
            Some(DfsCountContributionCursor::Aliases {
                target_object_id,
                alias_object_id,
            }) => Excluded((target_object_id.clone(), alias_object_id.clone())),
            _ => Unbounded,
        };
        for ((target_object_id, alias_object_id), path_record) in
            tables.hardlinks.range((start, Unbounded))
        {
            visit!(DfsCountContributionCursor::Aliases {
                target_object_id: target_object_id.clone(),
                alias_object_id: alias_object_id.clone(),
            });
            let Some(alias) = crate::metadata_hardlink::hardlink_object_from_path_record(
                tables,
                file_system_id,
                target_object_id,
                path_record,
            ) else {
                continue;
            };
            if alias.object_id != *alias_object_id
                || alias.tombstone_provenance.is_some()
                || owner(&alias.path)?.as_deref() != Some(source_shard)
                || !listable(&alias)?
            {
                continue;
            }
            page.aliases.push(DfsAliasContribution {
                alias_object_id: alias.object_id,
                alias_path: alias.path,
                target_object_id: target_object_id.clone(),
            });
        }
    }

    if !matches!(after, Some(DfsCountContributionCursor::Jobs { .. })) {
        let start = match after {
            Some(DfsCountContributionCursor::Directories { object_id }) => {
                Excluded(object_id.clone())
            }
            _ => Unbounded,
        };
        for (object_id, object) in tables.objects.range((start, Unbounded)) {
            visit!(DfsCountContributionCursor::Directories {
                object_id: object_id.clone(),
            });
            if object.kind != DfsObjectKind::Directory
                || object.tombstone_provenance.is_some()
                || object.file_system_id != *file_system_id
                || object.object_id != *object_id
                || !crate::metadata_pages::object_is_visible_at_path_from_tables(
                    tables,
                    file_system_id,
                    object,
                )
            {
                continue;
            }
            let child_path = crate::metadata_keys::normalize_posix_path(&object.path);
            let Some(parent_path) = crate::posix_path::parent_posix_path(&child_path) else {
                continue;
            };
            if owner(&child_path)?.as_deref() != Some(source_shard)
                || owner(&parent_path)?.as_deref() != Some(target_shard)
                || !listable(object)?
            {
                continue;
            }
            page.child_directories.push(DfsChildDirectoryContribution {
                parent_path,
                child_object_id: object_id.clone(),
                child_path,
            });
        }
    }

    let start = match after {
        Some(DfsCountContributionCursor::Jobs { job_id }) => Excluded(job_id.clone()),
        _ => Unbounded,
    };
    for (job_id, job) in tables.namespace_jobs.range((start, Unbounded)) {
        visit!(DfsCountContributionCursor::Jobs {
            job_id: job_id.clone(),
        });
        if job.status != DfsNamespaceJobStatus::Running
            || job.file_system_id != *file_system_id
            || job.job_id != *job_id
        {
            continue;
        }
        let mut carried = Vec::<DfsObjectMetadata>::new();
        for frame in &job.pending_frames {
            let object = match frame {
                DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. }
                | DfsNamespaceJobFrame::CrossShardComplete { moved_object, .. } => moved_object,
                DfsNamespaceJobFrame::CrossShardHardlinkAdopt { alias, .. } => alias,
                _ => continue,
            };
            if !carried
                .iter()
                .any(|existing| existing.object_id == object.object_id)
            {
                carried.push(object.clone());
            }
        }
        page.running_jobs.push(DfsRunningJobContribution {
            job_id: job.job_id.clone(),
            kind: job.kind.clone(),
            root_path: job.root_path.clone(),
            target_path: job.target_path.clone(),
            carried,
            waiting_for_storage_of: Vec::new(),
        });
    }
    page.complete = true;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_records::DfsMetadataTableOp;
    use crate::metadata_tables::DfsMetadataTables;
    use sinomo_core::object::serde_ext as sinomo_object_serde;

    fn node() -> NodeId {
        NodeId::from_legacy_named("count-contribution-owner").unwrap()
    }

    fn id(name: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(name).unwrap()
    }

    fn catalog() -> Vec<DfsMetadataShardConfig> {
        let root = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: node(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::LocalOwned,
        };
        let prefixed = |shard_id: &str, prefix: &str| DfsMetadataShardConfig {
            shard_id: shard_id.into(),
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: prefix.into(),
            },
            ..root.clone()
        };
        vec![
            root.clone(),
            prefixed("services", "/services"),
            prefixed("users", "/users"),
        ]
    }

    fn object(
        name: &str,
        path: &str,
        kind: DfsObjectKind,
        target: Option<&DfsObjectId>,
    ) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: id(name),
            file_system_id: sifs_file_system_id(),
            path: path.into(),
            namespace_link_count: kind.initial_namespace_link_count(),
            kind,
            tombstone_provenance: None,
            owner_node_id: node(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: target.cloned(),
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named("version_count_contribution_1").unwrap(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn put(tables: &mut DfsMetadataTables, object: &DfsObjectMetadata) {
        for op in
            crate::metadata_object_index::object_index_table_ops_with_previous(tables, object, None)
        {
            tables.apply(op);
        }
    }

    fn job(
        job_id: &str,
        status: DfsNamespaceJobStatus,
        root_path: &str,
        target_path: &str,
        pending_frames: Vec<DfsNamespaceJobFrame>,
    ) -> DfsNamespaceJobRecord {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        DfsNamespaceJobRecord {
            job_id: job_id.into(),
            file_system_id: sifs_file_system_id(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status,
            root_path: root_path.into(),
            target_path: Some(target_path.into()),
            pending_frames,
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        }
    }

    /// Shard `users` with live and stale rows of every section.
    fn users_tables() -> (DfsMetadataTables, DfsObjectMetadata) {
        let mut tables = DfsMetadataTables::default();
        let doc = object("obj_count_doc", "/users/doc", DfsObjectKind::File, None);
        let remote = id("obj_count_remote");
        for row in [
            object("obj_count_users", "/users", DfsObjectKind::Directory, None),
            object(
                "obj_count_alice",
                "/users/alice",
                DfsObjectKind::Directory,
                None,
            ),
            doc.clone(),
            object(
                "obj_count_doc_link",
                "/users/doc-link",
                DfsObjectKind::Hardlink,
                Some(&doc.object_id),
            ),
            object(
                "obj_count_remote_link",
                "/users/remote-link",
                DfsObjectKind::Hardlink,
                Some(&remote),
            ),
            // Physical copies of paths the catalog now gives `services`.
            object(
                "obj_count_stale_link",
                "/services/stale-link",
                DfsObjectKind::Hardlink,
                Some(&doc.object_id),
            ),
            object(
                "obj_count_stale_dir",
                "/services/stale-dir",
                DfsObjectKind::Directory,
                None,
            ),
        ] {
            put(&mut tables, &row);
        }
        let moved = object(
            "obj_count_moved",
            "/services/moved-link",
            DfsObjectKind::Hardlink,
            Some(&doc.object_id),
        );
        let adopt = DfsNamespaceJobFrame::CrossShardAdopt {
            moved_object: moved.clone(),
            old_path: "/users/moved-link".into(),
            promotion_alias: None,
            destination_primary_node_id: node(),
        };
        let complete = DfsNamespaceJobFrame::CrossShardComplete {
            moved_object: moved.clone(),
            old_path: "/users/moved-link".into(),
        };
        for job in [
            job(
                "job-count-rename",
                DfsNamespaceJobStatus::Running,
                "/users/moved-link",
                "/services/moved-link",
                vec![complete, adopt],
            ),
            job(
                "job-count-done",
                DfsNamespaceJobStatus::Completed,
                "/users/done",
                "/services/done",
                Vec::new(),
            ),
        ] {
            tables.apply(DfsMetadataTableOp::PutNamespaceJob { job });
        }
        (tables, moved)
    }

    fn prefix() -> DfsMetadataPrefixProof {
        crate::metadata_records::dfs_metadata_prefix_proof_genesis(&DfsMetadataShardId::new(
            "users",
        ))
    }

    fn read_page(
        tables: &DfsMetadataTables,
        target_shard: &str,
        after: Option<&DfsCountContributionCursor>,
        limit: usize,
    ) -> Result<DfsCountContributionPage, MetadataBackendError> {
        count_contributions_page(
            tables,
            &sifs_file_system_id(),
            "users",
            &catalog(),
            target_shard,
            after,
            limit,
            prefix(),
            &mut |_| Ok(true),
        )
    }

    #[test]
    fn page_reports_only_live_rows_the_source_owns() {
        let (tables, moved) = users_tables();
        let page = read_page(&tables, "root", None, DFS_COUNT_CONTRIBUTION_PAGE_LIMIT).unwrap();
        assert!(page.complete && page.after.is_none());
        assert_eq!(page.source_shard, "users");
        assert_eq!(page.prefix, prefix());
        let mut aliases = page
            .aliases
            .iter()
            .map(|a| (a.alias_path.as_str(), a.target_object_id.clone()))
            .collect::<Vec<_>>();
        aliases.sort();
        assert_eq!(
            aliases,
            vec![
                ("/users/doc-link", id("obj_count_doc")),
                ("/users/remote-link", id("obj_count_remote")),
            ],
            "every live owned alias with its target; never the stale copy"
        );
        assert_eq!(
            page.child_directories,
            vec![DfsChildDirectoryContribution {
                parent_path: "/".into(),
                child_object_id: id("obj_count_users"),
                child_path: "/users".into(),
            }],
            "only the directory under a target-owned parent"
        );
        assert_eq!(
            page.running_jobs,
            vec![DfsRunningJobContribution {
                job_id: "job-count-rename".into(),
                kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                root_path: "/users/moved-link".into(),
                target_path: Some("/services/moved-link".into()),
                carried: vec![moved],
                waiting_for_storage_of: Vec::new(),
            }],
            "the Running move, its object carried once"
        );

        // Under `services` as target, no users-owned directory has a
        // services-owned parent; aliases and jobs are target-independent.
        let services =
            read_page(&tables, "services", None, DFS_COUNT_CONTRIBUTION_PAGE_LIMIT).unwrap();
        assert!(services.complete && services.child_directories.is_empty());
        assert_eq!(services.aliases.len(), 2);
        assert_eq!(services.running_jobs.len(), 1);

        // The backend's residue rule removes rows the catalog alone admits.
        let hidden = count_contributions_page(
            &tables,
            &sifs_file_system_id(),
            "users",
            &catalog(),
            "root",
            None,
            DFS_COUNT_CONTRIBUTION_PAGE_LIMIT,
            prefix(),
            &mut |object| Ok(object.object_id != id("obj_count_doc_link")),
        )
        .unwrap();
        assert_eq!(hidden.aliases.len(), 1);
        assert_eq!(hidden.aliases[0].alias_path, "/users/remote-link");
    }

    #[test]
    fn small_pages_visit_every_entry_exactly_once() {
        let (mut tables, _) = users_tables();
        // Enough unreported rows to exhaust a one-entry page's visit budget.
        for index in 0..(3 * DFS_COUNT_CONTRIBUTION_VISITS_PER_ENTRY) {
            put(
                &mut tables,
                &object(
                    &format!("obj_count_file_{index:04}"),
                    &format!("/users/alice/file-{index:04}"),
                    DfsObjectKind::File,
                    None,
                ),
            );
        }
        let whole = read_page(&tables, "root", None, DFS_COUNT_CONTRIBUTION_PAGE_LIMIT).unwrap();
        assert!(whole.complete);
        assert_eq!(whole.entry_count(), 4);
        let mut aliases = Vec::new();
        let mut directories = Vec::new();
        let mut jobs = Vec::new();
        let mut after = None;
        let mut empty_pages = 0;
        for _ in 0..64 {
            let next = read_page(&tables, "root", after.as_ref(), 1).unwrap();
            assert!(next.entry_count() <= 1);
            assert_eq!(next.complete, next.after.is_none());
            assert_ne!(next.after, after, "every page makes progress");
            empty_pages += usize::from(next.entry_count() == 0 && !next.complete);
            aliases.extend(next.aliases);
            directories.extend(next.child_directories);
            jobs.extend(next.running_jobs);
            if next.complete {
                assert_eq!(aliases, whole.aliases);
                assert_eq!(directories, whole.child_directories);
                assert_eq!(jobs, whole.running_jobs);
                assert!(empty_pages > 0, "the visit budget bounds unreported rows");
                return;
            }
            after = next.after;
        }
        panic!("contribution paging did not complete");
    }

    #[test]
    fn query_binds_its_prefix_and_refuses_invalid_scopes() {
        let (tables, _) = users_tables();
        let query = |expected_prefix, after, target_shard: &str| DfsMetadataShardQuery {
            file_system_id: sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("users"),
            epoch: 1,
            selection: DfsMetadataSelection::CountContributionsV1 {
                target_shard: target_shard.into(),
                expected_prefix,
                after,
                question: Default::default(),
            },
        };
        let serve = |query: &DfsMetadataShardQuery, current: DfsMetadataPrefixProof| {
            query_count_contributions(&tables, query, &catalog(), current, &mut |_| Ok(true), &|_| {
                Vec::new()
            })
        };
        let mut moved = prefix();
        moved.journal_sequence += 1;
        let changed = DfsMetadataQueryResult::CountContributionsChangedV1 {
            prefix: moved.clone(),
        };
        assert_eq!(
            serve(&query(Some(prefix()), None, "root"), moved.clone()).unwrap(),
            changed,
            "a moved prefix is a typed refusal, not a page"
        );
        let resume = Some(DfsCountContributionCursor::Directories {
            object_id: id("obj_count_users"),
        });
        assert_eq!(
            serve(&query(Some(prefix()), resume.clone(), "root"), moved).unwrap(),
            changed
        );
        let DfsMetadataQueryResult::CountContributionsV1 { page } =
            serve(&query(Some(prefix()), resume.clone(), "root"), prefix()).unwrap()
        else {
            panic!("an unchanged prefix serves its page");
        };
        assert!(page.complete && page.aliases.is_empty() && page.child_directories.is_empty());
        assert_eq!(page.running_jobs.len(), 1);
        assert!(
            serve(&query(None, resume, "root"), prefix()).is_err(),
            "a continuation must name its first page's prefix"
        );
        assert!(serve(&query(None, None, "users"), prefix()).is_err());
        assert!(serve(&query(None, None, "unknown"), prefix()).is_err());
        assert!(read_page(&tables, "root", None, 0).is_err());
        assert!(read_page(&tables, "root", None, DFS_COUNT_CONTRIBUTION_PAGE_LIMIT + 1).is_err());
    }

    /// The read answers which asked objects, directories and jobs the
    /// source holds at paths the catalog gives it, and a question-only
    /// request after the last section visits no rows.
    #[test]
    fn a_question_is_answered_from_the_rows_the_source_owns() {
        let (tables, _) = users_tables();
        let question = DfsCountContributionQuestion {
            objects: vec![
                id("obj_count_doc"),
                id("obj_count_doc_link"),
                id("obj_count_remote"),
                id("obj_count_stale_dir"),
                id("obj_count_users"),
            ],
            directories: vec![
                "/users/alice".into(),
                "/users/doc".into(),
                "/users/missing".into(),
                "/services/stale-dir".into(),
            ],
            jobs: vec!["job-count-rename".into(), "job-count-done".into()],
        };
        let query = |after, question: DfsCountContributionQuestion| DfsMetadataShardQuery {
            file_system_id: sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("users"),
            epoch: 1,
            selection: DfsMetadataSelection::CountContributionsV1 {
                target_shard: "root".into(),
                expected_prefix: Some(prefix()),
                after,
                question,
            },
        };
        let serve = |query: &DfsMetadataShardQuery| {
            let DfsMetadataQueryResult::CountContributionsV1 { page } =
                query_count_contributions(&tables, query, &catalog(), prefix(), &mut |_| Ok(true), &|_| Vec::new())
                    .unwrap()
            else {
                panic!("an unchanged prefix serves its page");
            };
            page
        };
        let page = serve(&query(
            Some(DfsCountContributionCursor::Complete),
            question.clone(),
        ));
        assert!(page.complete && page.after.is_none() && page.entry_count() == 0);
        assert_eq!(
            page.held,
            DfsCountContributionHoldings {
                answered: 11,
                // Never an alias, or a stale copy of a path another shard owns.
                objects: vec![id("obj_count_doc"), id("obj_count_users")],
                directories: vec!["/users/alice".into()],
                jobs: vec!["job-count-rename".into()],
            }
        );
        assert!(page.held.answers(&question));
        assert!(
            !DfsCountContributionHoldings::default().answers(&question),
            "an answer that evaluated nothing does not answer"
        );
        let first = serve(&query(None, question.clone()));
        assert_eq!(first.held, page.held, "every page answers its own question");
        assert!(!first.aliases.is_empty());
        let oversized = DfsCountContributionQuestion {
            jobs: (0..=DFS_COUNT_CONTRIBUTION_QUESTION_LIMIT)
                .map(|index| format!("job-{index}"))
                .collect(),
            ..Default::default()
        };
        assert!(
            query_count_contributions(
                &tables,
                &query(None, oversized),
                &catalog(),
                prefix(),
                &mut |_| Ok(true),
                &|_| Vec::new()
            )
            .is_err()
        );
        let noncanonical = DfsCountContributionQuestion {
            directories: vec!["/users/alice/".into()],
            ..Default::default()
        };
        assert!(
            query_count_contributions(
                &tables,
                &query(None, noncanonical),
                &catalog(),
                prefix(),
                &mut |_| Ok(true),
                &|_| Vec::new()
            )
            .is_err()
        );
    }

    #[test]
    fn contribution_wire_is_tagged_and_explicit() {
        let selection = DfsMetadataSelection::CountContributionsV1 {
            target_shard: "root".into(),
            expected_prefix: None,
            after: None,
            question: Default::default(),
        };
        assert_eq!(
            serde_json::to_value(&selection).unwrap()["kind"],
            "count-contributions-v1"
        );
        assert_eq!(
            serde_json::from_value::<DfsMetadataSelection>(serde_json::json!({
                "kind": "count-contributions-v1", "target_shard": "root"
            }))
            .unwrap(),
            selection
        );
        let resumed = DfsMetadataSelection::CountContributionsV1 {
            target_shard: "root".into(),
            expected_prefix: Some(prefix()),
            after: Some(DfsCountContributionCursor::Aliases {
                target_object_id: id("obj_count_doc"),
                alias_object_id: id("obj_count_doc_link"),
            }),
            question: Default::default(),
        };
        let (tables, _) = users_tables();
        let result = DfsMetadataQueryResult::CountContributionsV1 {
            page: read_page(&tables, "root", None, DFS_COUNT_CONTRIBUTION_PAGE_LIMIT).unwrap(),
        };
        let changed = DfsMetadataQueryResult::CountContributionsChangedV1 { prefix: prefix() };
        assert_eq!(
            serde_json::to_value(&changed).unwrap()["kind"],
            "count-contributions-changed-v1"
        );
        for selection in [selection, resumed] {
            let bytes =
                sinomo_object_serde::to_bytes(&selection, 1, sinomo_core::object::LengthMode::U32)
                    .unwrap();
            assert_eq!(
                sinomo_object_serde::from_bytes::<DfsMetadataSelection>(&bytes).unwrap(),
                selection
            );
        }
        for result in [result.clone(), changed] {
            let bytes =
                sinomo_object_serde::to_bytes(&result, 1, sinomo_core::object::LengthMode::U32)
                    .unwrap();
            assert_eq!(
                sinomo_object_serde::from_bytes::<DfsMetadataQueryResult>(&bytes).unwrap(),
                result
            );
        }
        let mut value = serde_json::to_value(&result).unwrap();
        value["page"].as_object_mut().unwrap().remove("complete");
        assert!(
            serde_json::from_value::<DfsMetadataQueryResult>(value).is_err(),
            "omission is not a complete page"
        );
    }

    #[test]
    fn native_users_shard_reports_its_root_and_aliases_to_root() {
        let name = "count-contributions-v1";
        let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(name, 1).unwrap();
        let mut caller = runtime.client(&nodes[0]).unwrap();
        let path = "/users/count-target";
        let alias = "/users/count-alias";
        caller
            .posix_write_file(
                path,
                b"count bytes",
                DfsWriteOptions::replicated_eventual(vec![nodes[0].clone()]),
            )
            .unwrap();
        let target = caller.file_history_capture_object(path).unwrap();
        caller.posix_hardlink(path, alias).unwrap();
        let authorities = caller
            .metadata_shard_authorities(&target.file_system_id)
            .unwrap();
        let owner = |path: &str| {
            crate::select_metadata_shard_config_for_path(&target.file_system_id, &authorities, path)
                .unwrap()
                .unwrap()
        };
        let (users, root) = (owner(path), owner("/"));
        assert_ne!(users.shard_id, root.shard_id);
        let query = |caller: &mut NetworkedDfsService<DfsNodeTransport>,
                     shard: &DfsMetadataShardConfig,
                     target_shard: &str,
                     expected_prefix: Option<DfsMetadataPrefixProof>| {
            caller.query_metadata_authority_impl(
                shard,
                DfsMetadataSelection::CountContributionsV1 {
                    target_shard: target_shard.into(),
                    expected_prefix,
                    after: None,
                    question: Default::default(),
                },
            )
        };
        let DfsMetadataQueryResult::CountContributionsV1 { page } =
            query(&mut caller, &users, &root.shard_id, None).unwrap()
        else {
            panic!("wrong typed contribution response");
        };
        assert!(page.complete && page.after.is_none());
        assert_eq!(page.source_shard, users.shard_id);
        assert!(page.prefix.is_well_formed());
        assert!(
            page.child_directories
                .iter()
                .any(|child| child.child_path == "/users" && child.parent_path == "/"),
            "the users shard root is a child of the root directory: {page:?}"
        );
        assert!(
            page.child_directories
                .iter()
                .all(|child| child.parent_path == "/")
        );
        let reported = page
            .aliases
            .iter()
            .find(|entry| entry.alias_path == alias)
            .expect("the users-owned alias is reported");
        assert_eq!(reported.target_object_id, target.object_id);
        let DfsMetadataQueryResult::CountContributionsV1 { page: again } = query(
            &mut caller,
            &users,
            &root.shard_id,
            Some(page.prefix.clone()),
        )
        .unwrap() else {
            panic!("an unchanged prefix serves its page");
        };
        assert_eq!(again, page);
        let DfsMetadataQueryResult::CountContributionsV1 { page: from_root } =
            query(&mut caller, &root, &users.shard_id, None).unwrap()
        else {
            panic!("wrong typed contribution response");
        };
        assert!(from_root.complete && from_root.child_directories.is_empty());
        assert!(
            from_root
                .aliases
                .iter()
                .all(|entry| entry.alias_path != alias)
        );
        caller.posix_unlink(alias).unwrap();
        let DfsMetadataQueryResult::CountContributionsChangedV1 { prefix } = query(
            &mut caller,
            &users,
            &root.shard_id,
            Some(page.prefix.clone()),
        )
        .unwrap() else {
            panic!("a moved prefix is a typed refusal");
        };
        assert!(prefix.journal_sequence > page.prefix.journal_sequence);
        assert!(
            query(&mut caller, &users, &users.shard_id, None).is_err(),
            "a shard does not contribute to itself"
        );
        let mut stale = users.clone();
        stale.epoch += 1;
        assert!(query(&mut caller, &stale, &root.shard_id, None).is_err());
        assert!(caller.pending_routed_namespace_ops.is_empty());
        assert_eq!(caller.routed_namespace_settle_depth, 0);
        drop(caller);
        runtime.close().unwrap();
    }
}
