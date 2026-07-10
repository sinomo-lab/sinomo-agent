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
network_timed_transport_verify_steps="${QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS:-4}"
network_timed_delivery_verify_steps="${QUINT_NETWORK_TIMED_DELIVERY_VERIFY_MAX_STEPS:-4}"
network_timed_smoke_run_steps="${QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_STEPS:-80}"
network_timed_smoke_run_samples="${QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_SAMPLES:-500}"
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

typecheck_all() {
  find specs/quint -name '*.qnt' -print0 \
    | sort -z \
    | xargs -0 -n1 quint typecheck
}

run_check() {
  local file="$1"
  local main="$2"
  shift 2

  quint run "$file" "$@" \
    --main="$main" \
    --invariant=safety \
    --max-steps="$run_steps" \
    --max-samples="$run_samples" \
    --verbosity=1
}

verify_check() {
  local file="$1"
  local main="$2"
  local steps="$3"
  shift 3

  quint verify "$file" "$@" \
    --main="$main" \
    --invariant=safety \
    --max-steps="$steps" \
    --verbosity=1
}

run_system_smoke() {
  quint run specs/quint/scenarios/system/smoke.qnt \
    --main=system_smoke \
    --invariant=safety \
    --max-steps="$system_smoke_run_steps" \
    --max-samples="$system_smoke_run_samples" \
    --verbosity=1
}

run_network_timed_smoke() {
  quint run specs/quint/scenarios/network/timed_delivery_smoke.qnt \
    --main=network_timed_delivery_smoke \
    --invariant=safety \
    --max-steps="$network_timed_smoke_run_steps" \
    --max-samples="$network_timed_smoke_run_samples" \
    --verbosity=1
}

typecheck_all

run_check specs/quint/checks/kv/raft_kv_safety.qnt raft_kv_safety
run_check specs/quint/checks/network/network_safety.qnt network_safety
run_check specs/quint/checks/network/endpoint_selection_safety.qnt network_endpoint_selection_safety
run_check specs/quint/checks/network/route_planning_safety.qnt network_route_planning_safety
run_check specs/quint/checks/network/delivery_retry_safety.qnt network_delivery_retry_safety
run_check specs/quint/checks/network/attempt_budget_safety.qnt network_attempt_budget_safety
run_check specs/quint/checks/network/timed_transport_safety.qnt network_timed_transport_safety
run_check specs/quint/checks/network/timed_delivery_retry_safety.qnt network_timed_delivery_retry_safety
run_network_timed_smoke
run_system_smoke
run_check specs/quint/checks/membership/membership_safety.qnt membership_safety
run_check specs/quint/checks/membership/membership_kv_safety.qnt membership_kv_safety --step=step
run_check specs/quint/checks/dfs/metadata_safety.qnt dfs_metadata_safety
run_check specs/quint/checks/dfs/authority_routing_safety.qnt dfs_authority_routing_safety
run_check specs/quint/checks/dfs/read_freshness_safety.qnt dfs_read_freshness_safety
run_check specs/quint/checks/dfs/routed_namespace_freshness_safety.qnt dfs_routed_namespace_freshness_safety
run_check specs/quint/checks/dfs/posix_namespace_safety.qnt dfs_posix_namespace_safety
run_check specs/quint/checks/dfs/posix_links_safety.qnt dfs_posix_links_safety
run_check specs/quint/checks/dfs/posix_cross_shard_namespace_safety.qnt dfs_posix_cross_shard_namespace_safety
run_check specs/quint/checks/dfs/posix_read_write_visibility_safety.qnt dfs_posix_read_write_visibility_safety
run_check specs/quint/checks/dfs/metadata_primary_failover_safety.qnt dfs_metadata_primary_failover_safety
run_check specs/quint/checks/dfs/namespace_transaction_safety.qnt dfs_namespace_transaction_safety
run_check specs/quint/checks/dfs/sharded_namespace_transaction_safety.qnt dfs_sharded_namespace_transaction_safety
run_check specs/quint/checks/dfs/metadata_journal_source_safety.qnt dfs_metadata_journal_source_safety
run_check specs/quint/checks/dfs/repair_authorization_safety.qnt dfs_repair_authorization_safety
run_check specs/quint/checks/dfs/replication_policy_safety.qnt dfs_replication_policy_safety
run_check specs/quint/checks/dfs/no_conflict_safety.qnt dfs_no_conflict_safety
run_check specs/quint/checks/dfs/pending_request_lifecycle_safety.qnt dfs_pending_request_lifecycle_safety
run_check specs/quint/checks/dfs/repair_content_validation_safety.qnt dfs_repair_content_validation_safety
run_check specs/quint/checks/dfs/chunked_transfer_safety.qnt dfs_chunked_transfer_safety
run_check specs/quint/checks/dfs/repair_lost_authority_safety.qnt dfs_repair_lost_authority_safety
run_check specs/quint/checks/dfs/anti_entropy_safety.qnt dfs_anti_entropy_safety

verify_check specs/quint/checks/kv/raft_kv_safety.qnt raft_kv_safety "$verify_steps"
verify_check specs/quint/checks/network/network_safety.qnt network_safety "$verify_steps"
verify_check specs/quint/checks/network/endpoint_selection_safety.qnt network_endpoint_selection_safety "$network_endpoint_verify_steps"
verify_check specs/quint/checks/network/route_planning_safety.qnt network_route_planning_safety "$network_route_verify_steps"
verify_check specs/quint/checks/network/delivery_retry_safety.qnt network_delivery_retry_safety "$network_delivery_verify_steps"
verify_check specs/quint/checks/network/attempt_budget_safety.qnt network_attempt_budget_safety "$network_attempt_budget_verify_steps"
verify_check specs/quint/checks/network/timed_transport_safety.qnt network_timed_transport_safety "$network_timed_transport_verify_steps"
verify_check specs/quint/checks/network/timed_delivery_retry_safety.qnt network_timed_delivery_retry_safety "$network_timed_delivery_verify_steps"
verify_check specs/quint/checks/membership/membership_safety.qnt membership_safety "$membership_verify_steps"
verify_check specs/quint/checks/membership/membership_kv_safety.qnt membership_kv_safety "$membership_kv_verify_steps" --backend=tlc --step=stepCore
verify_check specs/quint/checks/dfs/metadata_safety.qnt dfs_metadata_safety "$verify_steps"
verify_check specs/quint/checks/dfs/authority_routing_safety.qnt dfs_authority_routing_safety "$dfs_routing_verify_steps"
verify_check specs/quint/checks/dfs/read_freshness_safety.qnt dfs_read_freshness_safety "$dfs_read_freshness_verify_steps"
verify_check specs/quint/checks/dfs/routed_namespace_freshness_safety.qnt dfs_routed_namespace_freshness_safety "$dfs_routed_namespace_verify_steps"
verify_check specs/quint/checks/dfs/posix_namespace_safety.qnt dfs_posix_namespace_safety "$dfs_posix_namespace_verify_steps"
verify_check specs/quint/checks/dfs/posix_links_safety.qnt dfs_posix_links_safety "$dfs_posix_links_verify_steps"
verify_check specs/quint/checks/dfs/posix_cross_shard_namespace_safety.qnt dfs_posix_cross_shard_namespace_safety "$dfs_posix_cross_shard_verify_steps"
verify_check specs/quint/checks/dfs/posix_read_write_visibility_safety.qnt dfs_posix_read_write_visibility_safety "$dfs_posix_read_write_verify_steps"
verify_check specs/quint/checks/dfs/metadata_primary_failover_safety.qnt dfs_metadata_primary_failover_safety "$dfs_primary_failover_verify_steps"
verify_check specs/quint/checks/dfs/namespace_transaction_safety.qnt dfs_namespace_transaction_safety "$dfs_namespace_verify_steps"
verify_check specs/quint/checks/dfs/sharded_namespace_transaction_safety.qnt dfs_sharded_namespace_transaction_safety "$dfs_sharded_namespace_verify_steps"
verify_check specs/quint/checks/dfs/metadata_journal_source_safety.qnt dfs_metadata_journal_source_safety "$dfs_journal_verify_steps"
verify_check specs/quint/checks/dfs/repair_authorization_safety.qnt dfs_repair_authorization_safety "$dfs_repair_verify_steps"
verify_check specs/quint/checks/dfs/replication_policy_safety.qnt dfs_replication_policy_safety "$dfs_replication_verify_steps"
verify_check specs/quint/checks/dfs/no_conflict_safety.qnt dfs_no_conflict_safety "$dfs_no_conflict_verify_steps"
verify_check specs/quint/checks/dfs/pending_request_lifecycle_safety.qnt dfs_pending_request_lifecycle_safety "$dfs_pending_verify_steps"
verify_check specs/quint/checks/dfs/repair_content_validation_safety.qnt dfs_repair_content_validation_safety "$dfs_repair_content_verify_steps"
verify_check specs/quint/checks/dfs/chunked_transfer_safety.qnt dfs_chunked_transfer_safety "$dfs_chunked_verify_steps"
verify_check specs/quint/checks/dfs/repair_lost_authority_safety.qnt dfs_repair_lost_authority_safety "$dfs_repair_lost_authority_verify_steps"
verify_check specs/quint/checks/dfs/anti_entropy_safety.qnt dfs_anti_entropy_safety "$dfs_anti_entropy_verify_steps"
