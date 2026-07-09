#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

verify_steps="${QUINT_VERIFY_MAX_STEPS:-5}"
membership_verify_steps="${QUINT_MEMBERSHIP_VERIFY_MAX_STEPS:-4}"
membership_kv_verify_steps="${QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS:-4}"
network_endpoint_verify_steps="${QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS:-4}"
network_route_verify_steps="${QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS:-4}"
network_delivery_verify_steps="${QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS:-5}"
network_attempt_budget_verify_steps="${QUINT_NETWORK_ATTEMPT_BUDGET_VERIFY_MAX_STEPS:-3}"
system_smoke_run_steps="${QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS:-80}"
system_smoke_run_samples="${QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES:-500}"
dfs_routing_verify_steps="${QUINT_DFS_ROUTING_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_read_freshness_verify_steps="${QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS:-3}"
dfs_routed_namespace_verify_steps="${QUINT_DFS_ROUTED_NAMESPACE_VERIFY_MAX_STEPS:-3}"
dfs_posix_namespace_verify_steps="${QUINT_DFS_POSIX_NAMESPACE_VERIFY_MAX_STEPS:-2}"
dfs_posix_links_verify_steps="${QUINT_DFS_POSIX_LINKS_VERIFY_MAX_STEPS:-2}"
dfs_posix_cross_shard_verify_steps="${QUINT_DFS_POSIX_CROSS_SHARD_VERIFY_MAX_STEPS:-3}"
dfs_posix_read_write_verify_steps="${QUINT_DFS_POSIX_READ_WRITE_VERIFY_MAX_STEPS:-2}"
dfs_primary_failover_verify_steps="${QUINT_DFS_PRIMARY_FAILOVER_VERIFY_MAX_STEPS:-3}"
dfs_namespace_verify_steps="${QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_sharded_namespace_verify_steps="${QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_journal_verify_steps="${QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_repair_verify_steps="${QUINT_DFS_REPAIR_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_replication_verify_steps="${QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_no_conflict_verify_steps="${QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_pending_verify_steps="${QUINT_DFS_PENDING_VERIFY_MAX_STEPS:-3}"
dfs_repair_content_verify_steps="${QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_chunked_verify_steps="${QUINT_DFS_CHUNKED_VERIFY_MAX_STEPS:-$verify_steps}"
dfs_repair_lost_authority_verify_steps="${QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS:-4}"
dfs_anti_entropy_verify_steps="${QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS:-$verify_steps}"
run_steps="${QUINT_RUN_MAX_STEPS:-20}"
run_samples="${QUINT_RUN_MAX_SAMPLES:-50}"

quint typecheck specs/quint/raft_kv.qnt
quint typecheck specs/quint/network.qnt
quint typecheck specs/quint/network_endpoint_selection.qnt
quint typecheck specs/quint/network_route_planning.qnt
quint typecheck specs/quint/network_delivery_retry.qnt
quint typecheck specs/quint/network_attempt_budget.qnt
quint typecheck specs/quint/system_base.qnt
quint typecheck specs/quint/system_guarantees.qnt
quint typecheck specs/quint/system_smoke.qnt
quint typecheck specs/quint/membership_base.qnt
quint typecheck specs/quint/membership.qnt
quint typecheck specs/quint/membership_kv.qnt
quint typecheck specs/quint/dfs_metadata.qnt
quint typecheck specs/quint/dfs_authority_routing.qnt
quint typecheck specs/quint/dfs_read_freshness.qnt
quint typecheck specs/quint/dfs_routed_namespace_freshness.qnt
quint typecheck specs/quint/dfs_posix_namespace.qnt
quint typecheck specs/quint/dfs_posix_links.qnt
quint typecheck specs/quint/dfs_posix_cross_shard_namespace.qnt
quint typecheck specs/quint/dfs_posix_read_write_visibility.qnt
quint typecheck specs/quint/dfs_metadata_primary_failover.qnt
quint typecheck specs/quint/dfs_namespace_transaction.qnt
quint typecheck specs/quint/dfs_sharded_namespace_transaction.qnt
quint typecheck specs/quint/dfs_metadata_journal_source.qnt
quint typecheck specs/quint/dfs_repair_authorization.qnt
quint typecheck specs/quint/dfs_replication_policy.qnt
quint typecheck specs/quint/dfs_no_conflict.qnt
quint typecheck specs/quint/dfs_pending_request_lifecycle.qnt
quint typecheck specs/quint/dfs_repair_content_validation.qnt
quint typecheck specs/quint/dfs_chunked_transfer.qnt
quint typecheck specs/quint/dfs_repair_lost_authority.qnt
quint typecheck specs/quint/dfs_anti_entropy.qnt

quint run specs/quint/raft_kv.qnt \
  --main=raft_kv \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/network.qnt \
  --main=network \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/network_endpoint_selection.qnt \
  --main=network_endpoint_selection \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/network_route_planning.qnt \
  --main=network_route_planning \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/network_delivery_retry.qnt \
  --main=network_delivery_retry \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/network_attempt_budget.qnt \
  --main=network_attempt_budget \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/system_smoke.qnt \
  --main=system_smoke \
  --invariant=safety \
  --max-steps="$system_smoke_run_steps" \
  --max-samples="$system_smoke_run_samples" \
  --verbosity=1
quint run specs/quint/membership.qnt \
  --main=membership \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/membership_kv.qnt \
  --main=membership_kv \
  --step=step \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_metadata.qnt \
  --main=dfs_metadata \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_authority_routing.qnt \
  --main=dfs_authority_routing \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_read_freshness.qnt \
  --main=dfs_read_freshness \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_routed_namespace_freshness.qnt \
  --main=dfs_routed_namespace_freshness \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_posix_namespace.qnt \
  --main=dfs_posix_namespace \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_posix_links.qnt \
  --main=dfs_posix_links \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_posix_cross_shard_namespace.qnt \
  --main=dfs_posix_cross_shard_namespace \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_posix_read_write_visibility.qnt \
  --main=dfs_posix_read_write_visibility \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_metadata_primary_failover.qnt \
  --main=dfs_metadata_primary_failover \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_namespace_transaction.qnt \
  --main=dfs_namespace_transaction \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_sharded_namespace_transaction.qnt \
  --main=dfs_sharded_namespace_transaction \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_metadata_journal_source.qnt \
  --main=dfs_metadata_journal_source \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_repair_authorization.qnt \
  --main=dfs_repair_authorization \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_replication_policy.qnt \
  --main=dfs_replication_policy \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_no_conflict.qnt \
  --main=dfs_no_conflict \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_pending_request_lifecycle.qnt \
  --main=dfs_pending_request_lifecycle \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_repair_content_validation.qnt \
  --main=dfs_repair_content_validation \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_chunked_transfer.qnt \
  --main=dfs_chunked_transfer \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_repair_lost_authority.qnt \
  --main=dfs_repair_lost_authority \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint run specs/quint/dfs_anti_entropy.qnt \
  --main=dfs_anti_entropy \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1

quint verify specs/quint/raft_kv.qnt \
  --main=raft_kv \
  --invariant=safety \
  --max-steps="$verify_steps" \
  --verbosity=1
quint verify specs/quint/network.qnt \
  --main=network \
  --invariant=safety \
  --max-steps="$verify_steps" \
  --verbosity=1
quint verify specs/quint/network_endpoint_selection.qnt \
  --main=network_endpoint_selection \
  --invariant=safety \
  --max-steps="$network_endpoint_verify_steps" \
  --verbosity=1
quint verify specs/quint/network_route_planning.qnt \
  --main=network_route_planning \
  --invariant=safety \
  --max-steps="$network_route_verify_steps" \
  --verbosity=1
quint verify specs/quint/network_delivery_retry.qnt \
  --main=network_delivery_retry \
  --invariant=safety \
  --max-steps="$network_delivery_verify_steps" \
  --verbosity=1
quint verify specs/quint/network_attempt_budget.qnt \
  --main=network_attempt_budget \
  --invariant=safety \
  --max-steps="$network_attempt_budget_verify_steps" \
  --verbosity=1
quint verify specs/quint/membership.qnt \
  --main=membership \
  --invariant=safety \
  --max-steps="$membership_verify_steps" \
  --verbosity=1
quint verify specs/quint/membership_kv.qnt \
  --backend=tlc \
  --main=membership_kv \
  --step=stepCore \
  --invariant=safety \
  --max-steps="$membership_kv_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_metadata.qnt \
  --main=dfs_metadata \
  --invariant=safety \
  --max-steps="$verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_authority_routing.qnt \
  --main=dfs_authority_routing \
  --invariant=safety \
  --max-steps="$dfs_routing_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_read_freshness.qnt \
  --main=dfs_read_freshness \
  --invariant=safety \
  --max-steps="$dfs_read_freshness_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_routed_namespace_freshness.qnt \
  --main=dfs_routed_namespace_freshness \
  --invariant=safety \
  --max-steps="$dfs_routed_namespace_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_posix_namespace.qnt \
  --main=dfs_posix_namespace \
  --invariant=safety \
  --max-steps="$dfs_posix_namespace_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_posix_links.qnt \
  --main=dfs_posix_links \
  --invariant=safety \
  --max-steps="$dfs_posix_links_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_posix_cross_shard_namespace.qnt \
  --main=dfs_posix_cross_shard_namespace \
  --invariant=safety \
  --max-steps="$dfs_posix_cross_shard_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_posix_read_write_visibility.qnt \
  --main=dfs_posix_read_write_visibility \
  --invariant=safety \
  --max-steps="$dfs_posix_read_write_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_metadata_primary_failover.qnt \
  --main=dfs_metadata_primary_failover \
  --invariant=safety \
  --max-steps="$dfs_primary_failover_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_namespace_transaction.qnt \
  --main=dfs_namespace_transaction \
  --invariant=safety \
  --max-steps="$dfs_namespace_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_sharded_namespace_transaction.qnt \
  --main=dfs_sharded_namespace_transaction \
  --invariant=safety \
  --max-steps="$dfs_sharded_namespace_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_metadata_journal_source.qnt \
  --main=dfs_metadata_journal_source \
  --invariant=safety \
  --max-steps="$dfs_journal_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_repair_authorization.qnt \
  --main=dfs_repair_authorization \
  --invariant=safety \
  --max-steps="$dfs_repair_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_replication_policy.qnt \
  --main=dfs_replication_policy \
  --invariant=safety \
  --max-steps="$dfs_replication_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_no_conflict.qnt \
  --main=dfs_no_conflict \
  --invariant=safety \
  --max-steps="$dfs_no_conflict_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_pending_request_lifecycle.qnt \
  --main=dfs_pending_request_lifecycle \
  --invariant=safety \
  --max-steps="$dfs_pending_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_repair_content_validation.qnt \
  --main=dfs_repair_content_validation \
  --invariant=safety \
  --max-steps="$dfs_repair_content_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_chunked_transfer.qnt \
  --main=dfs_chunked_transfer \
  --invariant=safety \
  --max-steps="$dfs_chunked_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_repair_lost_authority.qnt \
  --main=dfs_repair_lost_authority \
  --invariant=safety \
  --max-steps="$dfs_repair_lost_authority_verify_steps" \
  --verbosity=1
quint verify specs/quint/dfs_anti_entropy.qnt \
  --main=dfs_anti_entropy \
  --invariant=safety \
  --max-steps="$dfs_anti_entropy_verify_steps" \
  --verbosity=1
