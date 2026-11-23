//! Physically bounded, observational write-intent maintenance pages.
//!
//! A continuation never authorizes recovery or proves that a durable intent is
//! absent. Recovery still validates exact current records and ownership.

use crate::intent::write_intent_status_is_active;
use crate::metadata_keys::write_intent_scan_cursor_key;
use crate::metadata_tables::DfsMetadataTables;
use crate::{
    DfsError, DfsMetadataShardId, DfsObjectId, DfsWriteIntentRecord, DfsWriteIntentScanCursor,
};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Mutex;

/// Process-local scan position. `through` freezes one physical shard's sweep
/// boundary so continuing appends cannot starve later shards or the next sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsWriteIntentMaintenanceCursor {
    pub shard_id: DfsMetadataShardId,
    pub after: Option<DfsWriteIntentScanCursor>,
    pub through: Option<DfsWriteIntentScanCursor>,
}

impl DfsWriteIntentMaintenanceCursor {
    pub(crate) fn first_in_shard(shard_id: DfsMetadataShardId) -> Self {
        Self {
            shard_id,
            after: None,
            through: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsWriteIntentMaintenancePage {
    /// Physical active records, before cross-shard visibility/authority checks.
    /// They are candidates only, never a recovery authority proof.
    pub intents: Vec<DfsWriteIntentRecord>,
    pub next_cursor: Option<DfsWriteIntentMaintenanceCursor>,
    pub scanned_shard_id: Option<DfsMetadataShardId>,
    /// Raw physical order entries visited, including terminal or rejected rows.
    pub visited_record_count: usize,
    /// Raw enumeration visits at most one physical shard per nonzero request,
    /// including empty shards. Later visibility/recovery point reads are separate.
    pub visited_shard_count: usize,
}

/// Shared by the cache owner and its replaceable maintenance shells. This is
/// advisory progress only; restart/topology replacement may safely rewind it.
#[derive(Debug, Default)]
pub(crate) struct DfsWriteIntentMaintenanceScanState {
    cursor: Mutex<Option<DfsWriteIntentMaintenanceCursor>>,
}

impl DfsWriteIntentMaintenanceScanState {
    pub(crate) fn cursor(&self) -> Result<Option<DfsWriteIntentMaintenanceCursor>, DfsError> {
        self.cursor
            .lock()
            .map(|cursor| cursor.clone())
            .map_err(|_| DfsError::Metadata {
                message: "write-intent maintenance cursor lock poisoned".into(),
            })
    }
    pub(crate) fn advance(
        &self,
        expected: &Option<DfsWriteIntentMaintenanceCursor>,
        next: Option<DfsWriteIntentMaintenanceCursor>,
    ) -> Result<(), DfsError> {
        let mut cursor = self.cursor.lock().map_err(|_| DfsError::Metadata {
            message: "write-intent maintenance cursor lock poisoned".into(),
        })?;
        if &*cursor == expected {
            *cursor = next;
        }
        Ok(())
    }
}

fn cursor_from_key(key: &(time::OffsetDateTime, String, DfsObjectId)) -> DfsWriteIntentScanCursor {
    DfsWriteIntentScanCursor {
        created_at: key.0,
        intent_id: key.1.clone(),
        object_id: key.2.clone(),
    }
}

/// The loop visits at most `max_records` physical index entries plus one
/// lookahead. Filtering never refills the page and never loses its raw cursor.
pub(crate) fn bounded_intents_from_tables(
    tables: &DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    resume: Option<&DfsWriteIntentMaintenanceCursor>,
    max_records: usize,
) -> DfsWriteIntentMaintenancePage {
    if max_records == 0 {
        return DfsWriteIntentMaintenancePage {
            next_cursor: resume.cloned(),
            ..Default::default()
        };
    }
    let resume = resume.filter(|cursor| &cursor.shard_id == shard_id);
    let through = resume
        .and_then(|cursor| cursor.through.clone())
        .or_else(|| {
            tables
                .intent_order
                .last_key_value()
                .map(|(key, _)| cursor_from_key(key))
        });
    let mut page = DfsWriteIntentMaintenancePage {
        visited_shard_count: 1,
        scanned_shard_id: Some(shard_id.clone()),
        ..Default::default()
    };
    let Some(through) = through else {
        return page;
    };
    let upper_key = write_intent_scan_cursor_key(&through);
    let after = resume.and_then(|cursor| cursor.after.as_ref());
    let after_key = after.map(write_intent_scan_cursor_key);
    if after_key.as_ref().is_some_and(|after| after >= &upper_key) {
        return page;
    }
    let lower = after_key.map(Excluded).unwrap_or(Unbounded);
    let mut rows = tables.intent_order.range((lower, Included(upper_key)));
    let mut last = None;
    for (key, intent_key) in rows.by_ref().take(max_records) {
        page.visited_record_count += 1;
        last = Some(cursor_from_key(key));
        if let Some(intent) = tables.intents.get(intent_key)
            && write_intent_status_is_active(&intent.status)
        {
            page.intents.push(intent.clone());
        }
    }
    if rows.next().is_some() {
        page.next_cursor = Some(DfsWriteIntentMaintenanceCursor {
            shard_id: shard_id.clone(),
            after: last,
            through: Some(through),
        });
    }
    page
}

#[cfg(test)]
mod bounded_raw_page_tests {
    use super::*;
    use crate::{DfsReplicationPolicy, DfsVersion, DfsWriteIntentStatus};
    use sinomo_core::node::NodeId;
    use time::OffsetDateTime;

    fn record(second: i64, status: DfsWriteIntentStatus) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: format!("physical-intent-{second}"),
            file_system_id: crate::sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named(format!("physical-object-{second}")).unwrap(),
            base_version: None,
            target_version: DfsVersion::from_legacy_named(format!("physical-version-{second}"))
                .unwrap(),
            coordinator_node_id: NodeId::from_legacy_named("physical-page-owner").unwrap(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: Vec::new(),
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            status,
            content_hash: None,
            content_size_bytes: None,
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(second),
            updated_at: None,
        }
    }

    fn insert(tables: &mut DfsMetadataTables, record: DfsWriteIntentRecord) {
        let key = (record.object_id.clone(), record.intent_id.clone());
        tables.intent_order.insert(
            (
                record.created_at,
                record.intent_id.clone(),
                record.object_id.clone(),
            ),
            key.clone(),
        );
        tables.intents.insert(key, record);
    }

    #[test]
    fn physical_page_charges_terminal_and_missing_rows_without_refill() {
        let shard = DfsMetadataShardId::new("services");
        let mut tables = DfsMetadataTables::default();
        insert(&mut tables, record(0, DfsWriteIntentStatus::Committed));
        let missing = record(1, DfsWriteIntentStatus::ReceiptsPending);
        insert(&mut tables, missing.clone());
        tables
            .intents
            .remove(&(missing.object_id, missing.intent_id));
        insert(
            &mut tables,
            record(2, DfsWriteIntentStatus::ReceiptsPending),
        );
        let first = bounded_intents_from_tables(&tables, &shard, None, 2);
        assert_eq!(first.visited_record_count, 2);
        assert_eq!(first.visited_shard_count, 1);
        assert!(
            first.intents.is_empty(),
            "filtering cannot refill from the third raw row"
        );
        let cursor = first
            .next_cursor
            .expect("raw continuation after rejected rows");
        assert_eq!(
            cursor.after.as_ref().unwrap().created_at,
            record(1, DfsWriteIntentStatus::Committed).created_at
        );
        let second = bounded_intents_from_tables(&tables, &shard, Some(&cursor), 2);
        assert_eq!(second.visited_record_count, 1);
        assert_eq!(second.intents[0].intent_id, "physical-intent-2");
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn physical_page_freezes_upper_cut_and_wrap_revisits_backdated_rows() {
        let shard = DfsMetadataShardId::new("services");
        let mut tables = DfsMetadataTables::default();
        for second in [10, 20, 30, 40] {
            insert(
                &mut tables,
                record(second, DfsWriteIntentStatus::ReceiptsPending),
            );
        }
        let first = bounded_intents_from_tables(&tables, &shard, None, 2);
        let cursor = first.next_cursor.expect("first page continuation");
        assert_eq!(
            cursor.through.as_ref().unwrap().created_at,
            record(40, DfsWriteIntentStatus::Committed).created_at
        );
        insert(
            &mut tables,
            record(5, DfsWriteIntentStatus::ReceiptsPending),
        );
        insert(
            &mut tables,
            record(50, DfsWriteIntentStatus::ReceiptsPending),
        );
        let second = bounded_intents_from_tables(&tables, &shard, Some(&cursor), 2);
        assert_eq!(second.visited_record_count, 2);
        assert_eq!(
            second
                .intents
                .iter()
                .map(|r| r.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["physical-intent-30", "physical-intent-40"]
        );
        assert!(
            second.next_cursor.is_none(),
            "later appends must not extend the captured sweep cut"
        );
        let wrapped = bounded_intents_from_tables(&tables, &shard, None, 8);
        assert_eq!(wrapped.visited_record_count, 6);
        assert_eq!(
            wrapped.intents.first().unwrap().intent_id,
            "physical-intent-5"
        );
        assert_eq!(
            wrapped.intents.last().unwrap().intent_id,
            "physical-intent-50"
        );
    }

    #[test]
    fn zero_raw_budget_preserves_cursor_without_visiting_shard() {
        let shard = DfsMetadataShardId::new("services");
        let mut tables = DfsMetadataTables::default();
        for second in 0..3 {
            insert(
                &mut tables,
                record(second, DfsWriteIntentStatus::ReceiptsPending),
            );
        }
        let cursor = bounded_intents_from_tables(&tables, &shard, None, 1)
            .next_cursor
            .unwrap();
        let page = bounded_intents_from_tables(&tables, &shard, Some(&cursor), 0);
        assert_eq!(page.next_cursor, Some(cursor));
        assert_eq!(page.visited_record_count, 0);
        assert_eq!(page.visited_shard_count, 0);
        assert!(page.intents.is_empty());
    }
}
