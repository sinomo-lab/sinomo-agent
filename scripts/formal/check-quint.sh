#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

mode="${1:-all}"
verify_steps="${QUINT_VERIFY_MAX_STEPS:-5}"
run_steps="${QUINT_RUN_MAX_STEPS:-20}"
run_samples="${QUINT_RUN_MAX_SAMPLES:-50}"
run_seed="${QUINT_RUN_SEED:-0x51a0f04d20260710}"
case_filter="${QUINT_CASE_FILTER:-}"
case_timeout_seconds="${QUINT_CASE_TIMEOUT_SECONDS:-300}"
expected_quint_version="${QUINT_EXPECTED_VERSION:-0.32.0}"
apalache_version="${QUINT_APALACHE_VERSION:-0.56.1}"
apalache_server_endpoint="${QUINT_APALACHE_SERVER_ENDPOINT:-localhost:8822}"
require_witnesses="${QUINT_REQUIRE_WITNESSES:-0}"
coverage_only=0
verify_lock_file="${QUINT_VERIFY_LOCK_FILE:-${TMPDIR:-/tmp}/sinomo-quint-apalache-8822.lock}"

network_timed_smoke_run_steps="${QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_STEPS:-80}"
network_timed_smoke_run_samples="${QUINT_NETWORK_TIMED_SMOKE_RUN_MAX_SAMPLES:-500}"
system_smoke_run_steps="${QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS:-80}"
system_smoke_run_samples="${QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES:-500}"

declare -a cases=()

register_case() {
  local id="$1"
  local file="$2"
  local main="$3"
  local steps="$4"
  local backend="${5:-apalache}"
  local run_step="${6:-step}"
  local verify_step="${7:-step}"
  local tier="${8:-fast}"
  local witnesses="${9:-}"

  cases+=("$id|$file|$main|$steps|$backend|$run_step|$verify_step|$tier|$witnesses")
}

register_case raft-kv specs/quint/checks/kv/raft_kv_safety.qnt raft_kv_safety "$verify_steps"
register_case raft-protocol specs/quint/checks/kv/raft_protocol_safety.qnt raft_protocol_safety "${QUINT_RAFT_PROTOCOL_VERIFY_MAX_STEPS:-1}" apalache step step deep "witnessVoteGranted witnessStaleCandidateRejected witnessLeaderElectedFromQuorum witnessAppendPrevMismatchRejected witnessDivergentSuffixTruncated witnessAcknowledgementQuorumCommit witnessHigherTermStepDown witnessDurableRestart witnessFollowerStateMachineReplay witnessLeaderCompletenessAfterElection"
register_case network-base specs/quint/checks/network/network_safety.qnt network_safety "$verify_steps"
register_case network-endpoint specs/quint/checks/network/endpoint_selection_safety.qnt network_endpoint_selection_safety "${QUINT_NETWORK_ENDPOINT_VERIFY_MAX_STEPS:-4}"
register_case network-route specs/quint/checks/network/route_planning_safety.qnt network_route_planning_safety "${QUINT_NETWORK_ROUTE_VERIFY_MAX_STEPS:-4}"
register_case network-route-cache specs/quint/checks/network/route_cache_safety.qnt network_route_cache_safety "${QUINT_NETWORK_ROUTE_CACHE_VERIFY_MAX_STEPS:-4}" apalache step step fast "witnessFreshCachedPlanReuse witnessStateFileChangeRejected witnessDirectoryChangeRejected witnessUnknownDirectoryDisablesCache witnessConnectivityFileChangeRejected witnessConnectivityContentChangeRejected witnessModuleDialerChangeRejected witnessExpiredEntryRejected witnessMembershipRemovalRejected witnessClusterIdentityChangeRejected witnessLocalIdentityChangeRejected witnessIdentityMismatchInvalidatesImmediately witnessBuildRaceDiscarded"
register_case network-delivery specs/quint/checks/network/delivery_retry_safety.qnt network_delivery_retry_safety "${QUINT_NETWORK_DELIVERY_VERIFY_MAX_STEPS:-5}" apalache step step fast "witnessPrimaryAttemptIdentityAndNonce witnessFallbackRetryStableOperationFreshNonce"
register_case network-attempt-budget specs/quint/checks/network/attempt_budget_safety.qnt network_attempt_budget_safety "${QUINT_NETWORK_ATTEMPT_BUDGET_VERIFY_MAX_STEPS:-3}" apalache step step fast "witnessSingleEndpointCap witnessVerifiedFullResponseWithFallbacks witnessUnverifiedEndpointCap witnessTunnelFallbackCap"
register_case network-timed-transport specs/quint/checks/network/timed_transport_safety.qnt network_timed_transport_safety "${QUINT_NETWORK_TIMED_TRANSPORT_VERIFY_MAX_STEPS:-5}"
register_case network-timed-delivery specs/quint/checks/network/timed_delivery_retry_safety.qnt network_timed_delivery_retry_safety "${QUINT_NETWORK_TIMED_DELIVERY_VERIFY_MAX_STEPS:-7}" apalache step step fast "witnessPrimaryDelivery witnessFallbackDelivery witnessTimeoutFailure witnessDeadlineFailure witnessLateResponse"
register_case membership specs/quint/checks/membership/membership_safety.qnt membership_safety "${QUINT_MEMBERSHIP_VERIFY_MAX_STEPS:-4}" apalache step step deep
register_case membership-kv specs/quint/checks/membership/membership_kv_safety.qnt membership_kv_safety "${QUINT_MEMBERSHIP_KV_VERIFY_MAX_STEPS:-4}" tlc step stepCore deep
register_case membership-raft-config specs/quint/checks/membership/raft_config_transition_safety.qnt raft_config_transition_safety "${QUINT_MEMBERSHIP_RAFT_CONFIG_VERIFY_MAX_STEPS:-4}" apalache step step fast "witnessOneToTwoLearnerPromotionCommitted witnessOneToTwoNonVotingPromotionCommitted witnessThreeToTwoShrinkCommitted witnessTwoToOneShrinkRejected witnessDirectVoterAdditionRejected witnessTwoVoterAdditionsRejected witnessHealthObservationWithoutConfigMutation witnessPartialAcknowledgementDoesNotCommit"
register_case dfs-metadata specs/quint/checks/dfs/metadata_safety.qnt dfs_metadata_safety "$verify_steps"
register_case dfs-authority-routing specs/quint/checks/dfs/authority_routing_safety.qnt dfs_authority_routing_safety "${QUINT_DFS_ROUTING_VERIFY_MAX_STEPS:-$verify_steps}" apalache step step deep
register_case dfs-read-freshness specs/quint/checks/dfs/read_freshness_safety.qnt dfs_read_freshness_safety "${QUINT_DFS_READ_FRESHNESS_VERIFY_MAX_STEPS:-3}"
register_case dfs-routed-namespace specs/quint/checks/dfs/routed_namespace_freshness_safety.qnt dfs_routed_namespace_freshness_safety "${QUINT_DFS_ROUTED_NAMESPACE_VERIFY_MAX_STEPS:-3}"
register_case dfs-posix-namespace specs/quint/checks/dfs/posix_namespace_safety.qnt dfs_posix_namespace_safety "${QUINT_DFS_POSIX_NAMESPACE_VERIFY_MAX_STEPS:-2}" apalache step step deep
register_case dfs-posix-links specs/quint/checks/dfs/posix_links_safety.qnt dfs_posix_links_safety "${QUINT_DFS_POSIX_LINKS_VERIFY_MAX_STEPS:-2}" apalache step step deep
register_case dfs-posix-cross-shard specs/quint/checks/dfs/posix_cross_shard_namespace_safety.qnt dfs_posix_cross_shard_namespace_safety "${QUINT_DFS_POSIX_CROSS_SHARD_VERIFY_MAX_STEPS:-3}" apalache step step deep "witnessExhaustedRetryBudgetForwardCompletion"
register_case dfs-posix-read-write specs/quint/checks/dfs/posix_read_write_visibility_safety.qnt dfs_posix_read_write_visibility_safety "${QUINT_DFS_POSIX_READ_WRITE_VERIFY_MAX_STEPS:-2}" apalache step step deep
register_case dfs-posix-advisory-locks specs/quint/checks/dfs/posix_advisory_locks_safety.qnt dfs_posix_advisory_locks_safety "${QUINT_DFS_POSIX_ADVISORY_LOCKS_VERIFY_MAX_STEPS:-3}" apalache step step fast "witnessCompatibleSharedLocks witnessExclusiveConflictRejectedNonBlocking witnessBlockingWaiterQueued witnessWaiterGrantedAfterUnlock witnessWaiterGrantedAfterClose witnessWaiterGrantedAfterCrash witnessWaiterGrantedAfterLeaseExpiry witnessHealthyTickRenewsHeldLock witnessEpochFencesHeldLockOrWaiter witnessEpochSettlesBlockedWaiter witnessStaleTokenCannotUnlockNewEpoch"
register_case dfs-posix-fsync specs/quint/checks/dfs/posix_fsync_safety.qnt dfs_posix_fsync_safety "${QUINT_DFS_POSIX_FSYNC_VERIFY_MAX_STEPS:-3}" apalache step step fast "fileFsyncSuccessWithoutDirectoryDurabilityWitness fdatasyncSuccessWithoutAncillaryMetadataWitness directoryFsyncDoesNotSyncFileDataWitness laterWriteNotCoveredByEarlierFsyncWitness committedUnknownOutcomeDeduplicatesWitness payloadMismatchRejectedWitness inflightSyncFencedWithoutStaleCommitWitness successfulSyncSurvivesAuthorityChangeWitness committedPrefixRepairWitness"
register_case dfs-posix-partitioned-operation specs/quint/checks/dfs/posix_partitioned_operation_safety.qnt dfs_posix_partitioned_operation_safety "${QUINT_DFS_POSIX_PARTITIONED_OPERATION_VERIFY_MAX_STEPS:-8}" apalache step step fast "witnessCompleteHolderContinuesBeforeFence witnessHolderSyncEvidencePrepared witnessIncompleteHolderSuspends witnessCompleteHolderCanStillBeFencedAfterExpiry witnessQuorumWinsAndWaitsForRepair witnessStaleUnlockRejected witnessQuorumCompletesAfterRepair witnessQuorumCommitsWhileHolderOffline witnessFencedHolderSettlesAfterRejoin witnessBothSidesReachCleanHolderWinnerEndState witnessBothSidesReachCleanQuorumWinnerEndState"
register_case dfs-primary-failover specs/quint/checks/dfs/metadata_primary_failover_safety.qnt dfs_metadata_primary_failover_safety "${QUINT_DFS_PRIMARY_FAILOVER_VERIFY_MAX_STEPS:-3}"
register_case dfs-namespace-command-replay specs/quint/checks/dfs/namespace_command_replay_safety.qnt dfs_namespace_command_replay_safety "${QUINT_DFS_NAMESPACE_REPLAY_VERIFY_MAX_STEPS:-5}" apalache step step deep "witnessExactResidentRetryReplays witnessSameRequestIdChangedPayloadExecutes witnessCancelledCommandStillExecutes witnessEvictedExactRetryReexecutes witnessRestartClearedRetryReexecutes witnessCancelledFreshIdRetryExecutes"
register_case dfs-namespace-transaction specs/quint/checks/dfs/namespace_transaction_safety.qnt dfs_namespace_transaction_safety "${QUINT_DFS_NAMESPACE_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-sharded-namespace specs/quint/checks/dfs/sharded_namespace_transaction_safety.qnt dfs_sharded_namespace_transaction_safety "${QUINT_DFS_SHARDED_NAMESPACE_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-journal-source specs/quint/checks/dfs/metadata_journal_source_safety.qnt dfs_metadata_journal_source_safety "${QUINT_DFS_JOURNAL_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-repair-authorization specs/quint/checks/dfs/repair_authorization_safety.qnt dfs_repair_authorization_safety "${QUINT_DFS_REPAIR_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-replication specs/quint/checks/dfs/replication_policy_safety.qnt dfs_replication_policy_safety "${QUINT_DFS_REPLICATION_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-no-conflict specs/quint/checks/dfs/no_conflict_safety.qnt dfs_no_conflict_safety "${QUINT_DFS_NO_CONFLICT_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-pending-requests specs/quint/checks/dfs/pending_request_lifecycle_safety.qnt dfs_pending_request_lifecycle_safety "${QUINT_DFS_PENDING_VERIFY_MAX_STEPS:-3}" apalache step step deep "successfulReadSettlementWitness wrongSourceDiscardWithPendingWitness wrongObjectTerminalRejectionWitness"
register_case dfs-repair-content specs/quint/checks/dfs/repair_content_validation_safety.qnt dfs_repair_content_validation_safety "${QUINT_DFS_REPAIR_CONTENT_VERIFY_MAX_STEPS:-$verify_steps}" apalache step step deep
register_case dfs-chunked-transfer specs/quint/checks/dfs/chunked_transfer_safety.qnt dfs_chunked_transfer_safety "${QUINT_DFS_CHUNKED_VERIFY_MAX_STEPS:-$verify_steps}"
register_case dfs-repair-lost-authority specs/quint/checks/dfs/repair_lost_authority_safety.qnt dfs_repair_lost_authority_safety "${QUINT_DFS_REPAIR_LOST_AUTHORITY_VERIFY_MAX_STEPS:-4}" apalache step step deep
register_case dfs-anti-entropy specs/quint/checks/dfs/anti_entropy_safety.qnt dfs_anti_entropy_safety "${QUINT_DFS_ANTI_ENTROPY_VERIFY_MAX_STEPS:-$verify_steps}"

usage() {
  cat <<'EOF'
Usage: scripts/formal/check-quint.sh [all|fast|typecheck|run|smoke|verify-fast|verify-full|coverage]

Environment:
  QUINT_CASE_FILTER=id[,id...]    Restrict focused run/verify cases.
  QUINT_RUN_SEED=seed             Reproduce all randomized runs (fixed by default).
  QUINT_REQUIRE_WITNESSES=1       Fail when a registered witness is not reached.
  QUINT_COVERAGE_MAX_SAMPLES=n    Samples for focused witness coverage (default: 5000).
  QUINT_CASE_TIMEOUT_SECONDS=n    Bound each Quint command (default: 300).
  QUINT_EXPECTED_VERSION=x.y.z    Required Quint CLI version (default: 0.32.0).
  QUINT_APALACHE_VERSION=x.y.z    Pinned Apalache version (default: 0.56.1).
EOF
}

case_selected() {
  local id="$1"
  [[ -z "$case_filter" || ",$case_filter," == *",$id,"* ]]
}

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout --foreground "${case_timeout_seconds}s" "$@"
  else
    "$@"
  fi
}

check_toolchain() {
  if ! command -v quint >/dev/null 2>&1; then
    echo "error: quint is required; expected version $expected_quint_version" >&2
    return 1
  fi

  local actual_version
  actual_version="$(quint --version)"
  if [[ "$actual_version" != "$expected_quint_version" && "${QUINT_ALLOW_VERSION_MISMATCH:-0}" != "1" ]]; then
    echo "error: quint $expected_quint_version is required, found $actual_version" >&2
    echo "set QUINT_ALLOW_VERSION_MISMATCH=1 only for an intentional compatibility audit" >&2
    return 1
  fi
}

check_case_inventory() {
  local registered discovered
  registered="$(printf '%s\n' "${cases[@]}" | cut -d'|' -f2 | sort)"
  discovered="$(find specs/quint/checks -name '*_safety.qnt' -print | sort)"
  if [[ "$registered" != "$discovered" ]]; then
    echo "error: focused Quint case registry does not match checks/**/*_safety.qnt" >&2
    diff -u <(printf '%s\n' "$registered") <(printf '%s\n' "$discovered") || true
    return 1
  fi
}

validate_case_filter() {
  [[ -z "$case_filter" ]] && return 0
  if [[ "$case_filter" == *, ]]; then
    echo "error: QUINT_CASE_FILTER contains a trailing empty case id" >&2
    return 1
  fi

  local requested registered row found
  local -a requested_cases=()
  IFS=',' read -r -a requested_cases <<<"$case_filter"
  for requested in "${requested_cases[@]}"; do
    if [[ -z "$requested" ]]; then
      echo "error: QUINT_CASE_FILTER contains an empty case id" >&2
      return 1
    fi
    found=0
    for row in "${cases[@]}"; do
      registered="${row%%|*}"
      if [[ "$requested" == "$registered" ]]; then
        found=1
        break
      fi
    done
    if [[ "$found" != "1" ]]; then
      echo "error: unknown QUINT_CASE_FILTER id '$requested'" >&2
      echo "available ids: $(printf '%s\n' "${cases[@]}" | cut -d'|' -f1 | paste -sd, -)" >&2
      return 1
    fi
  done
}

typecheck_all() {
  local file
  while IFS= read -r -d '' file; do
    run_with_timeout quint typecheck "$file"
  done < <(find specs/quint -name '*.qnt' -print0 | sort -z)
}

run_check() {
  local id="$1"
  local file="$2"
  local main="$3"
  local step="$4"
  local witnesses="$5"
  local -a args=(
    quint run "$file"
    "--main=$main"
    --invariant=safety
    "--step=$step"
    "--max-steps=$run_steps"
    "--max-samples=$run_samples"
    "--seed=$run_seed"
    --verbosity=1
  )
  local -a witness_names=()
  if [[ -n "$witnesses" ]]; then
    read -r -a witness_names <<<"$witnesses"
    args+=(--witnesses "${witness_names[@]}")
  fi

  echo "==> run $id"
  local output status=0
  output="$(run_with_timeout env NO_COLOR=1 "${args[@]}" 2>&1)" || status=$?
  printf '%s\n' "$output"
  if (( status != 0 )); then
    return "$status"
  fi

  if [[ "$require_witnesses" == "1" ]]; then
    local witness
    for witness in "${witness_names[@]}"; do
      if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" <<<"$output"; then
        echo "error: required witness '$witness' was not reached by $id" >&2
        return 1
      fi
    done
  fi
}

run_focused_checks() {
  local row id file main steps backend run_step verify_step tier witnesses
  for row in "${cases[@]}"; do
    IFS='|' read -r id file main steps backend run_step verify_step tier witnesses <<<"$row"
    if case_selected "$id" && [[ "$coverage_only" != "1" || -n "$witnesses" ]]; then
      run_check "$id" "$file" "$main" "$run_step" "$witnesses"
    fi
  done
}

run_scenario() {
  local id="$1"
  local file="$2"
  local main="$3"
  local steps="$4"
  local samples="$5"
  local witnesses="${6:-}"
  local -a args=(
    quint run "$file"
    "--main=$main"
    --invariant=safety
    "--max-steps=$steps"
    "--max-samples=$samples"
    "--seed=$run_seed"
    --verbosity=1
  )
  local -a witness_names=()
  if [[ -n "$witnesses" ]]; then
    read -r -a witness_names <<<"$witnesses"
    args+=(--witnesses "${witness_names[@]}")
  fi

  echo "==> run $id"
  local output status=0
  output="$(run_with_timeout env NO_COLOR=1 "${args[@]}" 2>&1)" || status=$?
  printf '%s\n' "$output"
  if (( status != 0 )); then
    return "$status"
  fi

  if [[ "$require_witnesses" == "1" ]]; then
    local witness
    for witness in "${witness_names[@]}"; do
      if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" <<<"$output"; then
        echo "error: required witness '$witness' was not reached by $id" >&2
        return 1
      fi
    done
  fi
}

run_network_timed_smoke() {
  run_scenario \
    network-timed-smoke \
    specs/quint/scenarios/network/timed_delivery_smoke.qnt \
    network_timed_delivery_smoke \
    "$network_timed_smoke_run_steps" \
    "$network_timed_smoke_run_samples" \
    "witnessCausalTransportDelivery"
}

run_smoke_scenarios() {
  run_network_timed_smoke
  run_scenario \
    dfs-pending-read-identity \
    specs/quint/scenarios/dfs/pending_read_identity.qnt \
    dfs_pending_read_identity_scenario \
    6 \
    1
  run_scenario \
    dfs-posix-complete-holder-partition \
    specs/quint/scenarios/dfs/posix_lock_partition_recovery.qnt \
    dfs_posix_complete_holder_partition_scenario \
    14 \
    1
  run_scenario \
    dfs-posix-quorum-repair-partition \
    specs/quint/scenarios/dfs/posix_lock_partition_recovery.qnt \
    dfs_posix_quorum_repair_partition_scenario \
    18 \
    1
  run_scenario \
    dfs-posix-advisory-lock-fencing \
    specs/quint/scenarios/dfs/posix_advisory_lock_fencing.qnt \
    dfs_posix_advisory_lock_fencing_scenario \
    10 \
    1
  run_scenario \
    dfs-posix-fsync-unknown-recovery \
    specs/quint/scenarios/dfs/posix_fsync_recovery.qnt \
    dfs_posix_fsync_unknown_recovery_scenario \
    12 \
    1
  run_scenario \
    dfs-posix-directory-fsync-generation \
    specs/quint/scenarios/dfs/posix_fsync_recovery.qnt \
    dfs_posix_directory_fsync_generation_scenario \
    12 \
    1
  run_scenario \
    dfs-posix-cross-shard-retry-exhaustion \
    specs/quint/scenarios/dfs/posix_cross_shard_retry_exhaustion.qnt \
    dfs_posix_cross_shard_retry_exhaustion_scenario \
    12 \
    1
  run_scenario \
    system-smoke \
    specs/quint/scenarios/system/smoke.qnt \
    system_smoke \
    "$system_smoke_run_steps" \
    "$system_smoke_run_samples"
}

verify_check() {
  local id="$1"
  local file="$2"
  local main="$3"
  local steps="$4"
  local backend="$5"
  local step="$6"
  local -a args=(
    quint verify "$file"
    "--main=$main"
    --invariant=safety
    "--step=$step"
    "--max-steps=$steps"
    "--backend=$backend"
    "--apalache-version=$apalache_version"
    "--server-endpoint=$apalache_server_endpoint"
    --verbosity=1
  )

  echo "==> verify $id ($backend, $steps steps)"
  run_with_timeout env NO_COLOR=1 "${args[@]}"
}

verify_cases() {
  local requested_tier="$1"
  if ! command -v java >/dev/null 2>&1; then
    echo "error: java is required for Quint symbolic verification" >&2
    return 1
  fi

  local verify_lock_fd=""
  if command -v flock >/dev/null 2>&1; then
    exec {verify_lock_fd}>"$verify_lock_file"
    flock "$verify_lock_fd"
  fi

  local row id file main steps backend run_step verify_step tier witnesses
  for row in "${cases[@]}"; do
    IFS='|' read -r id file main steps backend run_step verify_step tier witnesses <<<"$row"
    if case_selected "$id" && [[ "$requested_tier" == "all" || "$tier" == "$requested_tier" ]]; then
      verify_check "$id" "$file" "$main" "$steps" "$backend" "$verify_step"
    fi
  done

  # Long causal outcomes stay in deterministic modules so their complete
  # traces can be symbolically verified without widening the nondeterministic
  # focused models to impractical depths.
  if case_selected dfs-posix-advisory-locks; then
    verify_check \
      dfs-posix-advisory-lock-fencing-scenario \
      specs/quint/scenarios/dfs/posix_advisory_lock_fencing.qnt \
      dfs_posix_advisory_lock_fencing_scenario \
      8 \
      apalache \
      step
  fi
  if case_selected dfs-posix-fsync; then
    verify_check \
      dfs-posix-fsync-unknown-recovery-scenario \
      specs/quint/scenarios/dfs/posix_fsync_recovery.qnt \
      dfs_posix_fsync_unknown_recovery_scenario \
      11 \
      apalache \
      step
    verify_check \
      dfs-posix-directory-fsync-generation-scenario \
      specs/quint/scenarios/dfs/posix_fsync_recovery.qnt \
      dfs_posix_directory_fsync_generation_scenario \
      10 \
      apalache \
      step
  fi
  if case_selected dfs-posix-partitioned-operation; then
    verify_check \
      dfs-posix-complete-holder-partition-scenario \
      specs/quint/scenarios/dfs/posix_lock_partition_recovery.qnt \
      dfs_posix_complete_holder_partition_scenario \
      10 \
      apalache \
      step
    verify_check \
      dfs-posix-quorum-repair-partition-scenario \
      specs/quint/scenarios/dfs/posix_lock_partition_recovery.qnt \
      dfs_posix_quorum_repair_partition_scenario \
      13 \
      apalache \
      step
  fi
  if [[ "$requested_tier" == "all" ]] && case_selected dfs-posix-cross-shard; then
    verify_check \
      dfs-posix-cross-shard-retry-exhaustion-scenario \
      specs/quint/scenarios/dfs/posix_cross_shard_retry_exhaustion.qnt \
      dfs_posix_cross_shard_retry_exhaustion_scenario \
      10 \
      apalache \
      step
  fi

  if [[ -n "$verify_lock_fd" ]]; then
    flock --unlock "$verify_lock_fd"
  fi
}

check_toolchain
check_case_inventory
validate_case_filter

case "$mode" in
  all)
    typecheck_all
    run_focused_checks
    run_smoke_scenarios
    verify_cases all
    ;;
  fast)
    typecheck_all
    run_focused_checks
    run_smoke_scenarios
    verify_cases fast
    ;;
  typecheck)
    typecheck_all
    ;;
  run)
    run_focused_checks
    run_smoke_scenarios
    ;;
  smoke)
    run_smoke_scenarios
    ;;
  verify-fast)
    verify_cases fast
    ;;
  verify-full)
    verify_cases all
    ;;
  coverage)
    require_witnesses=1
    coverage_only=1
    run_samples="${QUINT_COVERAGE_MAX_SAMPLES:-5000}"
    run_focused_checks
    network_timed_smoke_run_samples="${QUINT_NETWORK_TIMED_COVERAGE_MAX_SAMPLES:-500}"
    run_network_timed_smoke
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "error: unknown mode '$mode'" >&2
    usage >&2
    exit 2
    ;;
esac
