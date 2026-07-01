//! Receipt lookup helpers shared by metadata replay and migration paths.

use sinomo_node::NodeId;

use crate::metadata_backend::{DfsMetadataBackend, Result};
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
