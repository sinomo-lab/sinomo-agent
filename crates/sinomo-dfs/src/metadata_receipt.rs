//! Receipt lookup helpers shared by metadata replay and migration paths.

use sinomo_node::NodeId;

use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::Result;
use crate::metadata_records::DfsReplicaReceiptRecord;
use crate::{DfsObjectId, DfsVersion};

pub(crate) fn receipt_for_replica_from_backend<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    replica_node_id: &NodeId,
    page_size: usize,
) -> Result<Option<DfsReplicaReceiptRecord>> {
    let mut cursor = None;
    loop {
        let page =
            backend.receipts_for_version_page(object_id, version, cursor.as_ref(), page_size)?;
        if let Some(receipt) = page
            .receipts
            .into_iter()
            .find(|receipt| &receipt.replica_node_id == replica_node_id)
        {
            return Ok(Some(receipt));
        }
        let Some(next_cursor) = page.next_cursor else {
            return Ok(None);
        };
        cursor = Some(next_cursor);
    }
}

pub(crate) fn replay_receipt_matches_existing(
    existing: &DfsReplicaReceiptRecord,
    replayed: &DfsReplicaReceiptRecord,
) -> bool {
    existing == replayed
        || (existing.receipt.file_system_id == replayed.receipt.file_system_id
            && existing.receipt.object_id == replayed.receipt.object_id
            && existing.receipt.committed_version == replayed.receipt.committed_version
            && existing.replica_node_id == replayed.replica_node_id
            && existing.content_hash == replayed.content_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsWriteReceipt;
    use sinomo_events::EventId;
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_receipt").expect("valid object id")
    }

    fn version() -> DfsVersion {
        DfsVersion::from_legacy_named("version_receipt_1").expect("valid version")
    }

    fn receipt(event_id: &str, content_hash: Option<&str>) -> DfsReplicaReceiptRecord {
        DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: crate::DfsFileSystemId::from_legacy_named("sifs")
                    .expect("filesystem id"),
                object_id: object_id(),
                committed_version: version(),
                owner_node_id: node("node_owner"),
                event_id: EventId::from_legacy_named(event_id).expect("event id"),
            },
            replica_node_id: node("node_replica"),
            content_hash: content_hash.map(str::to_string),
            received_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn replay_receipt_allows_semantically_identical_receipt_with_different_event() {
        let existing = receipt("evt_receipt_a", Some("blake3:a"));
        let replayed = receipt("evt_receipt_b", Some("blake3:a"));

        assert!(replay_receipt_matches_existing(&existing, &replayed));
    }

    #[test]
    fn replay_receipt_rejects_different_content_hash() {
        let existing = receipt("evt_receipt_a", Some("blake3:a"));
        let replayed = receipt("evt_receipt_b", Some("blake3:b"));

        assert!(!replay_receipt_matches_existing(&existing, &replayed));
    }
}
