use super::*;

/// Current typed row effects sealed before publication. No journal writer or
/// alternate prepare/commit family is encoded by an effect.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsPreparedMetadataEffect {
    PutFileSystem {
        file_system: DfsFileSystem,
    },
    PutObject {
        object: DfsObjectMetadata,
    },
    PutVersion {
        version: DfsObjectVersionRecord,
    },
    RemoveVersion {
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutVersionPin {
        pin: DfsMetadataVersionPinRecord,
    },
    RemoveVersionPin {
        object_id: DfsObjectId,
        version: DfsVersion,
        pin_id: String,
    },
    ReconcileCrossShardPinTransfer {
        versions: Vec<DfsObjectVersionRecord>,
        transfer: DfsCrossShardPinTransferRecord,
    },
    RemoveCrossShardPinTransfer {
        object_id: DfsObjectId,
        transfer_id: String,
    },
    FinalizeCrossShardSourceEviction {
        tombstone: DfsObjectMetadata,
        removed_pins: Vec<DfsMetadataVersionPinRecord>,
        removed_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    },
    PutReceipt {
        receipt: DfsReplicaReceiptRecord,
    },
    RemoveReceipt {
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
    },
    WithdrawReceipt {
        withdrawal: DfsReplicaReceiptWithdrawalRecord,
    },
    PutIntent {
        intent: DfsWriteIntentRecord,
    },
    RemoveRetainedRepairOwner {
        object_id: DfsObjectId,
        intent_id: String,
    },
    PutIntentReceipt {
        receipt: DfsWriteIntentReceipt,
    },
    RemoveIntentReceipt {
        object_id: DfsObjectId,
        intent_id: String,
        retention: DfsWriteIntentReceiptRetentionState,
    },
    MergeIntentReceiptRetention {
        retention: DfsWriteIntentReceiptRetentionState,
    },
    PutAdvisoryLockState {
        state: DfsAdvisoryLockStateRecord,
    },
    MergeContentReferenceOutcomeRetention {
        object_id: DfsObjectId,
        retention: DfsContentReferenceOutcomeRetention,
    },
    PutNamespaceTransactionReceipt {
        receipt: DfsNamespaceTransactionReceipt,
    },
    RemoveNamespaceTransactionReceipt {
        transaction_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceJob {
        job: DfsNamespaceJobRecord,
    },
    PutNamespaceJobReceipt {
        receipt: DfsNamespaceJobReceipt,
    },
    RemoveNamespaceJobReceipt {
        job_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceRequest {
        request: Box<DfsNamespaceRequestRecord>,
    },
    PutNamespaceRequestReceipt {
        receipt: Box<DfsNamespaceRequestReceipt>,
    },
    RemoveNamespaceRequestReceipt {
        requester_node_id: NodeId,
        request_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    RemovePreparedOperation {
        digest: DfsPreparedOperationDigest,
    },
    ResolvePreparedOperation {
        digest: DfsPreparedOperationDigest,
        status: DfsPreparedOperationStatus,
    },
}
impl DfsPreparedMetadataEffect {
    pub(crate) fn into_table_op(self) -> DfsMetadataTableOp {
        match self {
            Self::PutFileSystem { file_system } => {
                DfsMetadataTableOp::PutFileSystem { file_system }
            }
            Self::PutObject { object } => DfsMetadataTableOp::PutObject { object },
            Self::PutVersion { version } => DfsMetadataTableOp::PutVersion { version },
            Self::RemoveVersion { object_id, version } => {
                DfsMetadataTableOp::RemoveVersion { object_id, version }
            }
            Self::PutVersionPin { pin } => DfsMetadataTableOp::PutVersionPin { pin },
            Self::RemoveVersionPin {
                object_id,
                version,
                pin_id,
            } => DfsMetadataTableOp::RemoveVersionPin {
                object_id,
                version,
                pin_id,
            },
            Self::ReconcileCrossShardPinTransfer { versions, transfer } => {
                DfsMetadataTableOp::ReconcileCrossShardPinTransfer { versions, transfer }
            }
            Self::RemoveCrossShardPinTransfer {
                object_id,
                transfer_id,
            } => DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                object_id,
                transfer_id,
            },
            Self::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => DfsMetadataTableOp::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            },
            Self::PutReceipt { receipt } => DfsMetadataTableOp::PutReceipt { receipt },
            Self::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => DfsMetadataTableOp::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            },
            Self::WithdrawReceipt { withdrawal } => {
                DfsMetadataTableOp::WithdrawReceipt { withdrawal }
            }
            Self::PutIntent { intent } => DfsMetadataTableOp::PutIntent { intent },
            Self::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
            } => DfsMetadataTableOp::RemoveRetainedRepairOwner {
                object_id,
                intent_id,
            },
            Self::PutIntentReceipt { receipt } => DfsMetadataTableOp::PutIntentReceipt { receipt },
            Self::RemoveIntentReceipt {
                object_id,
                intent_id,
                retention,
            } => DfsMetadataTableOp::RemoveIntentReceipt {
                object_id,
                intent_id,
                retention,
            },
            Self::MergeIntentReceiptRetention { retention } => {
                DfsMetadataTableOp::MergeIntentReceiptRetention { retention }
            }
            Self::PutAdvisoryLockState { state } => {
                DfsMetadataTableOp::PutAdvisoryLockState { state }
            }
            Self::MergeContentReferenceOutcomeRetention {
                object_id,
                retention,
            } => DfsMetadataTableOp::MergeContentReferenceOutcomeRetention {
                object_id,
                retention,
            },
            Self::PutNamespaceTransactionReceipt { receipt } => {
                DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt }
            }
            Self::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
            } => DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                transaction_id,
                retention,
            },
            Self::PutNamespaceJob { job } => DfsMetadataTableOp::PutNamespaceJob { job },
            Self::PutNamespaceJobReceipt { receipt } => {
                DfsMetadataTableOp::PutNamespaceJobReceipt { receipt }
            }
            Self::RemoveNamespaceJobReceipt { job_id, retention } => {
                DfsMetadataTableOp::RemoveNamespaceJobReceipt { job_id, retention }
            }
            Self::PutNamespaceRequest { request } => {
                DfsMetadataTableOp::PutNamespaceRequest { request }
            }
            Self::PutNamespaceRequestReceipt { receipt } => {
                DfsMetadataTableOp::PutNamespaceRequestReceipt { receipt }
            }
            Self::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                retention,
            } => DfsMetadataTableOp::RemoveNamespaceRequestReceipt {
                requester_node_id,
                request_id,
                retention,
            },
            Self::RemovePreparedOperation { digest } => {
                DfsMetadataTableOp::RemovePreparedOperation { digest }
            }
            Self::ResolvePreparedOperation { digest, status } => {
                DfsMetadataTableOp::ResolvePreparedOperation { digest, status }
            }
        }
    }
}

impl DfsPreparedMetadataEffect {
    pub fn affected_object_ids(&self) -> std::collections::BTreeSet<DfsObjectId> {
        let mut ids = std::collections::BTreeSet::new();
        match self {
            Self::PutObject { object } => {
                ids.insert(object.object_id.clone());
            }
            Self::PutVersion { version } => {
                ids.insert(version.object_id.clone());
            }
            Self::PutVersionPin { pin } => {
                ids.insert(pin.object_id.clone());
            }
            Self::PutReceipt { receipt } => {
                ids.insert(receipt.receipt.object_id.clone());
            }
            Self::WithdrawReceipt { withdrawal } => {
                ids.insert(withdrawal.object_id.clone());
            }
            Self::PutIntent { intent } => {
                ids.insert(intent.object_id.clone());
            }
            Self::PutIntentReceipt { receipt } => {
                ids.insert(receipt.object_id.clone());
            }
            Self::PutAdvisoryLockState { state } => {
                ids.insert(state.object_id.clone());
            }
            Self::MergeContentReferenceOutcomeRetention { object_id, .. }
            | Self::RemoveVersion { object_id, .. }
            | Self::RemoveVersionPin { object_id, .. }
            | Self::RemoveCrossShardPinTransfer { object_id, .. }
            | Self::RemoveReceipt { object_id, .. }
            | Self::RemoveRetainedRepairOwner { object_id, .. }
            | Self::RemoveIntentReceipt { object_id, .. } => {
                ids.insert(object_id.clone());
            }
            Self::FinalizeCrossShardSourceEviction { tombstone, .. } => {
                ids.insert(tombstone.object_id.clone());
            }
            Self::ReconcileCrossShardPinTransfer { versions, .. } => {
                ids.extend(versions.iter().map(|version| version.object_id.clone()))
            }
            _ => {}
        }
        ids
    }
    pub fn namespace_jobs(&self) -> Option<&DfsNamespaceJobRecord> {
        match self {
            Self::PutNamespaceJob { job } => Some(job),
            _ => None,
        }
    }
}
