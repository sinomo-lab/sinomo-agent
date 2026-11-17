//! Sharded metadata page cursor helpers.
//!
//! Local metadata backends return shard-local cursors. The sharded backend
//! exposes merged pages that must carry the shard id in each continuation
//! cursor, and must resume only the shard referenced by an incoming cursor.

use crate::metadata_error::Result;
use crate::metadata_keys::{
    object_version_page_cursor_with_shard, replica_receipt_page_cursor_with_shard,
    write_intent_page_cursor_with_shard,
};
use crate::metadata_records::{
    DfsMetadataShardId, DfsObjectVersionPage, DfsReplicaReceiptPage, DfsWriteIntentPage,
};

fn shard_is_before_resume(
    shard_id: &DfsMetadataShardId,
    resume_shard_id: Option<&DfsMetadataShardId>,
) -> bool {
    resume_shard_id.is_some_and(|resume_shard_id| shard_id < resume_shard_id)
}

fn local_cursor_for_resumed_shard<O, C>(
    after: Option<&O>,
    resume_shard_id: Option<&DfsMetadataShardId>,
    shard_id: &DfsMetadataShardId,
    map_cursor: impl FnOnce(&O) -> C,
) -> Option<C> {
    if resume_shard_id.is_some_and(|resume_shard_id| resume_shard_id != shard_id) {
        return None;
    }
    after.map(map_cursor)
}

/// Collect `(record, cursor_after_record)` pairs from every shard `shards`
/// yields, in its order. A shard the caller cannot read is yielded as an error,
/// which ends the scan.
///
/// Callers provide the shard-local page fetcher and record mapper so this
/// helper can centralize the repeated cursor progression, per-shard page
/// limit, and "collect one extra accepted record" behavior used by merged
/// global scans.
pub(crate) fn collect_sharded_scan_entries<'a, T, O, C: Clone, B: 'a>(
    shards: impl IntoIterator<Item = Result<(&'a DfsMetadataShardId, &'a B)>>,
    after: Option<&C>,
    max_entries: usize,
    mut fetch_page: impl FnMut(
        &DfsMetadataShardId,
        &B,
        Option<&C>,
        usize,
    ) -> Result<(Vec<T>, Option<C>)>,
    mut map_record: impl FnMut(&DfsMetadataShardId, &B, T) -> Result<Option<(O, C)>>,
) -> Result<Vec<(O, C)>> {
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    let per_shard_limit = max_entries.saturating_add(1);
    let mut entries = Vec::new();
    for shard in shards {
        let (shard_id, backend) = shard?;
        let mut cursor = after.cloned();
        let mut shard_entries = 0usize;
        loop {
            let (records, next_cursor) =
                fetch_page(shard_id, backend, cursor.as_ref(), per_shard_limit)?;
            for record in records {
                let Some(entry) = map_record(shard_id, backend, record)? else {
                    continue;
                };
                entries.push(entry);
                shard_entries = shard_entries.saturating_add(1);
                if shard_entries > max_entries {
                    break;
                }
            }
            if shard_entries > max_entries {
                break;
            }
            let Some(next_cursor) = next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
    }
    Ok(entries)
}

/// Collect one merged page of shard-local page records.
///
/// This variant is for APIs whose cursors include a shard id. It skips shards
/// before the resume shard, resumes only the referenced shard with a local
/// cursor, then collects one extra accepted record across the merged shard
/// stream so callers can build a continuation cursor. `shards` yields shards
/// in shard order; one the caller cannot read is an error, which ends the page.
pub(crate) fn collect_resumed_sharded_page_entries<'a, T, O, C, L, B: 'a>(
    shards: impl IntoIterator<Item = Result<(&'a DfsMetadataShardId, &'a B)>>,
    after: Option<&C>,
    resume_shard_id: Option<&DfsMetadataShardId>,
    max_entries: usize,
    map_local_cursor: impl Fn(&C) -> L,
    mut fetch_page: impl FnMut(
        &DfsMetadataShardId,
        &B,
        Option<&L>,
        usize,
    ) -> Result<(Vec<T>, Option<L>)>,
    mut map_record: impl FnMut(&DfsMetadataShardId, &B, T) -> Result<Option<(O, C)>>,
) -> Result<Vec<(O, C)>> {
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    'shards: for shard in shards {
        let (shard_id, backend) = shard?;
        if shard_is_before_resume(shard_id, resume_shard_id) {
            continue;
        }
        let mut local_after =
            local_cursor_for_resumed_shard(after, resume_shard_id, shard_id, &map_local_cursor);
        loop {
            let (records, next_local_cursor) = fetch_page(
                shard_id,
                backend,
                local_after.as_ref(),
                max_entries.saturating_add(1),
            )?;
            for record in records {
                let Some(entry) = map_record(shard_id, backend, record)? else {
                    continue;
                };
                entries.push(entry);
                if entries.len() > max_entries {
                    break 'shards;
                }
            }
            let Some(next_local_cursor) = next_local_cursor else {
                break;
            };
            local_after = Some(next_local_cursor);
        }
    }
    Ok(entries)
}

pub(crate) fn object_version_page_with_shard_cursor(
    mut page: DfsObjectVersionPage,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectVersionPage {
    page.next_cursor = page
        .next_cursor
        .map(|cursor| object_version_page_cursor_with_shard(cursor, shard_id));
    page
}

pub(crate) fn replica_receipt_page_with_shard_cursor(
    mut page: DfsReplicaReceiptPage,
    shard_id: &DfsMetadataShardId,
) -> DfsReplicaReceiptPage {
    page.next_cursor = page
        .next_cursor
        .map(|cursor| replica_receipt_page_cursor_with_shard(cursor, shard_id));
    page
}

pub(crate) fn write_intent_page_with_shard_cursor(
    mut page: DfsWriteIntentPage,
    shard_id: &DfsMetadataShardId,
) -> DfsWriteIntentPage {
    page.next_cursor = page
        .next_cursor
        .map(|cursor| write_intent_page_cursor_with_shard(cursor, shard_id));
    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_error::Result;
    use std::collections::BTreeMap;

    #[test]
    fn sharded_scan_entries_skip_filtered_records_and_collect_one_extra() {
        let mut shards = BTreeMap::new();
        shards.insert(DfsMetadataShardId::new("a"), vec![1, 2, 3, 4, 5]);
        shards.insert(DfsMetadataShardId::new("b"), vec![6, 7, 8]);

        let entries = collect_sharded_scan_entries(
            shards.iter().map(Ok),
            None,
            2,
            |_, records, cursor, limit| {
                let start = cursor
                    .and_then(|cursor| records.iter().position(|record| record > cursor))
                    .unwrap_or(0);
                let end = records.len().min(start.saturating_add(limit));
                let next_cursor = (end < records.len()).then_some(records[end - 1]);
                Ok((records[start..end].to_vec(), next_cursor))
            },
            |shard_id, _, record| -> Result<_> {
                let _ = shard_id;
                if record % 2 == 0 {
                    return Ok(None);
                }
                Ok(Some((record, record)))
            },
        )
        .expect("scan entries");

        assert_eq!(entries, vec![(1, 1), (3, 3), (5, 5), (7, 7)]);
    }

    #[test]
    fn resumed_sharded_page_entries_skip_prior_shards_and_collect_one_extra() {
        let mut shards = BTreeMap::new();
        shards.insert(DfsMetadataShardId::new("a"), vec![1, 2, 3]);
        shards.insert(DfsMetadataShardId::new("b"), vec![4, 5, 6, 7]);
        shards.insert(DfsMetadataShardId::new("c"), vec![8, 9]);

        let resume_shard_id = DfsMetadataShardId::new("b");
        let entries = collect_resumed_sharded_page_entries(
            shards.iter().map(Ok),
            Some(&5),
            Some(&resume_shard_id),
            2,
            |cursor| *cursor,
            |_, records, cursor, limit| {
                let start = cursor
                    .and_then(|cursor| records.iter().position(|record| record > cursor))
                    .unwrap_or(0);
                let end = records.len().min(start.saturating_add(limit));
                let next_cursor = (end < records.len()).then_some(records[end - 1]);
                Ok((records[start..end].to_vec(), next_cursor))
            },
            |shard_id, _, record| -> Result<_> {
                Ok(Some((format!("{}:{record}", shard_id.0), record)))
            },
        )
        .expect("resumed page entries");

        assert_eq!(
            entries,
            vec![
                ("b:6".to_string(), 6),
                ("b:7".to_string(), 7),
                ("c:8".to_string(), 8)
            ]
        );
    }
}
