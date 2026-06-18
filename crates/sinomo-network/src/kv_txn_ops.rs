use crate::error::NetworkError;
use sinomo_kv::{KvError, KvOp};

pub(crate) fn push_bounded_txn_success_op(
    ops: &mut Vec<KvOp>,
    op: KvOp,
) -> Result<(), NetworkError> {
    if ops.len() >= sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH {
        return Err(NetworkError::Kv(KvError::TxnLimit {
            compares: 0,
            success_ops: ops.len().saturating_add(1),
            failure_ops: 0,
            max_compares: sinomo_kv::MAX_KV_TXN_COMPARES,
            max_ops_per_branch: sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH,
        }));
    }
    ops.push(op);
    Ok(())
}
