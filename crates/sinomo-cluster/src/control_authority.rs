//! Commit-time authorization evidence for remote cluster-control mutations.

use crate::{CLUSTER_CONTROL_ADMIN_CAPABILITY, ClusterMemberRecord};
use sinomo_kv::{
    ClusterKvRaftConfigRecord, ClusterKvTxnCommitResult, KvCompare, KvEntry, KvError, KvTxnRequest,
    MAX_KV_TXN_COMPARES, MAX_KV_TXN_OPS_PER_BRANCH,
};
use sinomo_network::NetworkError;
use sinomo_node::{ClusterCapabilityPolicyRecord, NodeId};

/// Exact quorum-observed authorization evidence for one remote
/// cluster-control request.
///
/// Every downstream transaction compares both the value and modify revision
/// of all three authority rows, closing the gap between service authorization
/// and the mutation's Raft commit.
#[derive(Clone, Debug)]
pub struct RemoteClusterControlAuthorityFence {
    signer_node_id: NodeId,
    signer_public_key: String,
    entries: [KvEntry; 3],
}

impl RemoteClusterControlAuthorityFence {
    pub fn new(
        signer_node_id: NodeId,
        signer_public_key: String,
        member: KvEntry,
        config: KvEntry,
        capability_policy: KvEntry,
    ) -> Self {
        Self {
            signer_node_id,
            signer_public_key,
            entries: [member, config, capability_policy],
        }
    }

    fn validate_entries(&self, entries: &[KvEntry; 3]) -> Result<(), NetworkError> {
        let member: ClusterMemberRecord =
            entries[0]
                .decode()
                .map_err(|error| NetworkError::InvalidClusterCommand {
                    detail: format!("remote cluster-control member evidence is invalid: {error}"),
                })?;
        let config: ClusterKvRaftConfigRecord =
            entries[1]
                .decode()
                .map_err(|error| NetworkError::InvalidClusterCommand {
                    detail: format!("remote cluster-control config evidence is invalid: {error}"),
                })?;
        let policy: ClusterCapabilityPolicyRecord =
            entries[2]
                .decode()
                .map_err(|error| NetworkError::InvalidClusterCommand {
                    detail: format!("remote cluster-control policy evidence is invalid: {error}"),
                })?;
        if member.node_id != self.signer_node_id
            || member.node_public_key != self.signer_public_key
            || !member.has_raft_vote()
            || config.cluster_id != member.cluster_id
            || !config.voters.contains(&self.signer_node_id)
            || policy.cluster_id != member.cluster_id
            || policy.node_id != self.signer_node_id
            || !policy
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == CLUSTER_CONTROL_ADMIN_CAPABILITY)
            || !policy
                .allowed_capabilities
                .iter()
                .any(|capability| capability.as_str() == CLUSTER_CONTROL_ADMIN_CAPABILITY)
        {
            return Err(NetworkError::InvalidClusterCommand {
                detail: "remote cluster-control authority was revoked during the mutation"
                    .to_string(),
            });
        }
        Ok(())
    }

    pub fn fence_request(&self, mut request: KvTxnRequest) -> Result<KvTxnRequest, NetworkError> {
        if !request.txn.failure.is_empty() {
            return Err(NetworkError::InvalidClusterCommand {
                detail: "remote cluster-control mutations cannot contain failure-branch operations because authorization guards must fail closed"
                    .to_string(),
            });
        }
        let guard_count = self.entries.len().saturating_mul(2);
        let compare_count = request
            .txn
            .compares
            .len()
            .checked_add(guard_count)
            .unwrap_or(usize::MAX);
        if compare_count > MAX_KV_TXN_COMPARES {
            let error = KvError::TxnLimit {
                compares: compare_count,
                success_ops: request.txn.success.len(),
                failure_ops: request.txn.failure.len(),
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            };
            return Err(NetworkError::InvalidClusterCommand {
                detail: error.to_string(),
            });
        }
        request.txn.compares.try_reserve(guard_count).map_err(|_| {
            let error = KvError::ResourceExhausted {
                operation: "remote cluster-control authorization guards".to_string(),
                requested_bytes: compare_count
                    .saturating_mul(std::mem::size_of::<KvCompare>().max(1)),
            };
            NetworkError::InvalidClusterCommand {
                detail: error.to_string(),
            }
        })?;
        for entry in &self.entries {
            request.txn.compares.push(KvCompare::ModifyRevisionEquals {
                key: entry.key.clone(),
                revision: entry.modify_revision,
            });
            request.txn.compares.push(KvCompare::ValueEquals {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
        }
        Ok(request)
    }

    pub fn confirm_commit_and_advance(
        &mut self,
        result: &ClusterKvTxnCommitResult,
    ) -> Result<(), NetworkError> {
        if !result
            .txn_response
            .as_ref()
            .is_some_and(|response| response.succeeded)
        {
            return Err(NetworkError::InvalidClusterCommand {
                detail: "remote cluster-control authority changed before the mutation committed"
                    .to_string(),
            });
        }
        let mut next_entries = self.entries.clone();
        for entry in &mut next_entries {
            *entry = result.snapshot.get(&entry.key).cloned().ok_or_else(|| {
                NetworkError::InvalidClusterCommand {
                    detail: format!(
                        "remote cluster-control authority row `{}` disappeared during the mutation",
                        entry.key
                    ),
                }
            })?;
        }
        self.validate_entries(&next_entries)?;
        self.entries = next_entries;
        Ok(())
    }
}
