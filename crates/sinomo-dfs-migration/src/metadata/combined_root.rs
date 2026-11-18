//! The legacy combined `root` copy, which can hold other shards' rows.
//!
//! In a legacy layout one physical `root` copy holds the rows, floors and
//! journal of several logical shards, and a shard whose rows live only there
//! has no directory of its own. The combined copy is the live `root` copy
//! while it is legacy, or, once `root` converted per shard, the legacy copy
//! that conversion retained beside the image it installed in the copy's
//! place.
//!
//! Such a shard T converts per shard from the combined copy. Its capture
//! reads the copy through the converter session under the copy's shared
//! source fence, writes nothing into it, and keeps only T's rows: the
//! session's projection of the whole copy is partitioned by the scope
//! mapping ([`partition_capture`]), so the rows of `root` and of every other
//! shard stay in the copy for their own conversions. The copy's in-flight
//! work is split the same way: T keeps the original native work and the
//! unsettled operations it owns and leaves the rest to their owners
//! ([`attribution`]). T's installed image becomes T's own copy; the combined
//! copy is left as it is.
//!
//! `root` converts independently, before or after T, from the same copy,
//! partitioned the same way ([`capture_keeps_only_own_rows`]): `root`'s
//! image holds only `root`'s rows and the work `root` owns, so a shard's
//! rows reach only that shard's image. `root`'s activation can move the live
//! combined copy to its retained place while T's conversion still reads it,
//! so T resolves the copy's current place at each use ([`current_location`]).
use super::scope_mapping::RequiredNativeWork;
use super::*;

mod attribution;
#[cfg(test)]
mod tests;

/// The legacy combined `root` copy beside `metadata_root`'s shard copies
/// that can hold rows and floors of `shard`: the live `root` copy while it
/// is legacy, or the legacy copy a per-shard conversion of `root` retained
/// when it installed its image in the copy's place.
pub(crate) fn legacy_combined_root(
    metadata_root: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<PathBuf>> {
    if shard.0 == "root" {
        return Ok(None);
    }
    let live = metadata_root.join("root");
    if !live.try_exists().map_err(error)? {
        return Ok(None);
    }
    if original_native_root_format(&live)?.is_none() {
        return Ok(Some(live));
    }
    let Ok(header) = sinomo_dfs::inspect_metadata_storage_generation(&live) else {
        return Ok(None);
    };
    let identity = &header.identity;
    let sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition { transition_id } =
        &identity.origin
    else {
        return Ok(None);
    };
    if *identity
        != sinomo_dfs::metadata_shard_converted_generation(
            &identity.file_system_id,
            &identity.shard_id.0,
            transition_id,
        )
        || transition_id.len() != 64
        || !transition_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(None);
    }
    let retained = metadata_root
        .join(".metadata-retained")
        .join(format!("shard-conversion-{transition_id}"));
    if retained.is_dir() && original_native_root_format(&retained)?.is_none() {
        return Ok(Some(retained));
    }
    Ok(None)
}

/// The copy the per-shard conversion of `shard` captures for its copy at
/// `destination`: that copy while it holds original data or a generation of
/// its own, otherwise the legacy combined `root` copy while it holds
/// original data. `None` when neither does.
pub(crate) fn shard_conversion_source(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<PathBuf>> {
    if original_source_present(destination)?
        || sinomo_dfs::inspect_metadata_storage_generation(destination).is_ok()
    {
        return Ok(Some(destination.to_owned()));
    }
    let metadata_root = destination
        .parent()
        .ok_or_else(|| error("metadata destination has no parent"))?;
    match legacy_combined_root(metadata_root, shard)? {
        Some(combined) if original_source_present(&combined)? => Ok(Some(combined)),
        _ => Ok(None),
    }
}

/// Where the copy a capture of `shard` recorded at `recorded` is now: the
/// recorded place, unless it was the live combined `root` copy of another
/// shard and `root`'s own per-shard conversion has since installed its image
/// there, which retained the copy beside it.
pub(crate) fn current_location(recorded: &Path, shard: &DfsMetadataShardId) -> Result<PathBuf> {
    let (Some(metadata_root), Some(name)) = (recorded.parent(), recorded.file_name()) else {
        return Ok(recorded.to_owned());
    };
    if name != "root" || shard.0 == "root" || original_native_root_format(recorded)?.is_none() {
        return Ok(recorded.to_owned());
    }
    Ok(legacy_combined_root(metadata_root, shard)?.unwrap_or_else(|| recorded.to_owned()))
}

/// The stage intent naming a capture whose source can hold other shards'
/// rows.
const COMBINED_SOURCE_CAPTURE: &str = "shard-conversion-combined-source.json";

/// Whether the per-shard capture of `shard` from `source`, for its copy at
/// `destination`, keeps only the shard's rows: a capture of the combined
/// copy of another shard, and every capture of `root`, whose own legacy copy
/// is the combined copy of the shards whose rows live only there.
pub(crate) fn capture_keeps_only_own_rows(
    source: &Path,
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> bool {
    source != destination || shard.0 == "root"
}

/// Record, once, that the capture of `shard` at `destination` reads a copy
/// that can hold other shards' rows, so its projection keeps only the
/// shard's rows.
pub(super) fn record_combined_source_capture(
    destination: &Path,
    shard: &DfsMetadataShardId,
    file_system_id: &DfsFileSystemId,
) -> Result<()> {
    write_count_cohort_checkpoint(
        &conversion_stage_base_path(destination, shard)?.join(COMBINED_SOURCE_CAPTURE),
        file_system_id,
    )
}

/// Whether the capture of `shard` at `destination` keeps only the shard's
/// rows of its copy.
pub(super) fn combined_source_capture(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<bool> {
    conversion_stage_base_path(destination, shard)?
        .join(COMBINED_SOURCE_CAPTURE)
        .try_exists()
        .map_err(error)
}

/// The rows, original operations and original native work of `shard` in the
/// projection of a combined copy, by the scope mapping over the capture's
/// complete catalog and the rules of [`attribution`].
///
/// Every row the mapping attributes to the shard is kept, and every other
/// row is left to its own shard. A directory fragment record whose entries
/// several shards own is derived again over the shard's own entries, and a
/// retry floor or outcome receipt of an object no captured row places is
/// kept by every shard. An original operation is kept when the mapping
/// attributes it to the shard (its original rows, not the mapping's
/// rewritten form, so its outcome and any recovery are judged as recorded),
/// unless it is unsettled and another shard owns it; one the shard's rows
/// cannot depend on is left to its own shard. Native work is kept when the
/// shard owns it.
///
/// A row these rules cannot attribute becomes unresolved work of this
/// capture, so the conversion stops rather than guess. With ordinary data
/// none remains: the other object-keyed rows (inodes, versions, pins,
/// replica receipts and write intents) keep their object's row, live or
/// tombstone, because tombstone compaction waits for the object's versions,
/// receipts and intents and removes its inode with it, and path-keyed rows
/// always route.
/// Such a row without a witness, a request or receipt whose recorded
/// identity disagrees with itself, and an operation or native work whose
/// owner the copy does not name come only from damaged or altered data.
pub(super) fn partition_capture(
    rows: sinomo_dfs::DfsMetadataTableSnapshotTables,
    mut operations: Vec<Value>,
    mut required: Vec<RequiredNativeWork>,
    assignments: &[DfsMetadataShardConfig],
    file_system_ids: &[DfsFileSystemId],
    shard: &DfsMetadataShardId,
) -> Result<(
    sinomo_dfs::DfsMetadataTableSnapshotTables,
    Vec<Value>,
    Vec<RequiredNativeWork>,
)> {
    let targets = assignments
        .iter()
        .filter(|config| {
            config.shard_id == shard.0 && file_system_ids.contains(&config.file_system_id)
        })
        .collect::<Vec<_>>();
    let [target] = targets.as_slice() else {
        return Err(error(
            "a combined-copy capture needs exactly one catalog scope of its shard",
        ));
    };
    let prepared = rows.prepared_operations_v7.clone();
    let jobs = sinomo_dfs::DfsMetadataTableSnapshotTables {
        namespace_jobs: rows.namespace_jobs.clone(),
        ..Default::default()
    };
    let mapped = scope_mapping::map_scope(rows, operations.clone(), assignments, target)?;
    let operation_key = |row: &Value| {
        let payload = row.get("source_payload").or_else(|| row.get("payload"))?;
        Some((
            payload.get("file_system_id")?.to_string(),
            payload.get("operation_id")?.as_str()?.to_owned(),
        ))
    };
    let owned_elsewhere = |row: &Value| {
        attribution::unsettled_operation_owner(row, assignments).is_some_and(|owner| {
            owner.file_system_id != target.file_system_id || owner.shard_id != target.shard_id
        })
    };
    let (mut kept, mut left) = (
        std::collections::BTreeSet::new(),
        std::collections::BTreeSet::new(),
    );
    for row in &mapped.operation_mappings {
        if let Some(key) = operation_key(row) {
            if owned_elsewhere(row) {
                left.insert(key);
            } else {
                kept.insert(key);
            }
        }
    }
    operations.retain(|row| operation_key(row).is_some_and(|key| kept.contains(&key)));
    required.retain(|work| {
        attribution::keeps_native_work(work, &jobs, assignments, target, &operations)
    });
    let mut tables = mapped.tables;
    // The mapping moves the original V7 records into its operation map;
    // the kept ones stay rows of this capture as the source recorded them.
    tables.prepared_operations_v7 = prepared
        .into_iter()
        .filter(|record| {
            serde_json::to_value(&record.digest.file_system_id).is_ok_and(|fs| {
                kept.contains(&(fs.to_string(), record.digest.operation_id.clone()))
            })
        })
        .collect();
    // A directory fragment record indexes the entries one store holds. A
    // fragment of the combined copy whose entries several shards own becomes,
    // in this shard's rows, the record over the entries this shard keeps, as
    // the shard's own store records it; one holding none of them is another
    // shard's to index. A floor or receipt of an object no row places is
    // every shard's, and an unsettled operation another shard owns is left to
    // that shard, routed or not. Every other row the mapping cannot attribute stays
    // unresolved.
    for work in mapped.required_native_work {
        if attribution::keep_shared_row(&mut tables, &work, target)? {
            continue;
        }
        if work.family == "prepared_operations"
            && (operation_key(&work.source_row).is_some_and(|key| left.contains(&key))
                || owned_elsewhere(&work.source_row))
        {
            continue;
        }
        if work.family != "dirfrags" {
            required.push(work);
            continue;
        }
        let fragment: sinomo_dfs::DfsDirFragmentRecord =
            serde_json::from_value(work.source_row.clone()).map_err(error)?;
        let held = tables
            .dentries
            .iter()
            .filter(|dentry| {
                dentry.file_system_id == fragment.file_system_id
                    && dentry.parent_inode_id == fragment.dir_inode_id
                    && dentry.dirfrag_id == fragment.dirfrag_id
            })
            .count();
        if held > 0 {
            tables.dirfrags.push(sinomo_dfs::DfsDirFragmentRecord {
                entry_count: u64::try_from(held).map_err(error)?,
                ..fragment
            });
        }
    }
    Ok((tables, operations, required))
}
