#!/usr/bin/env bash
# End-to-end production-readiness gate for the segmented Docker cluster.
#
# It rebuilds/starts the current worktree image, waits for all expected main
# daemons to become reachable, runs the idle I/O budget gate, exercises bridge
# loss/recovery, and checks the cluster is clean afterward.
#
#   docker/measure-final.sh [idle_window_seconds] [bridge_recovery_seconds]
#
# Environment:
#   SINOMO_GATE_REBUILD=0       reuse the existing host binary/image/containers
#   SINOMO_GATE_RESET_VOLUMES=0 keep existing compose volumes when rebuilding
#   SINOMO_GATE_PROJECT         compose project name (default sinomo-dfs-test)
#   SINOMO_GATE_SKIP_MOBILE=1   skip the mobile/sensor role idle gate
#   SINOMO_GATE_CYCLES=3        repeat idle/mobile/bridge validation after startup
#   SINOMO_GATE_FORMATION_WAIT  seconds to wait for initial cluster health
#   SINOMO_GATE_POLL_INTERVAL   health poll interval in seconds
#   SINOMO_GATE_COMMAND_TIMEOUT host/container cap for each readiness command
#   SINOMO_GATE_EXPECTED_MAINS  expected online main-daemon rows
#   SINOMO_GATE_QUIESCE_WAIT    initial settle delay after readiness
#   SINOMO_GATE_QUIESCE_WINDOW  zero-write window required after the settle deadline
#   SINOMO_GATE_QUIESCE_TIMEOUT maximum seconds to find a full zero-write window
#   SINOMO_GATE_NO_QUIET_RETRY=1 fail after the first fixed quiet window
#   SINOMO_GATE_OWNED_IO_ARTIFACTS opt into complete stable-container I/O evidence

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${SINOMO_GATE_PROJECT:-sinomo-dfs-test}}"

set -euo pipefail
cd "$(dirname "$0")/.."

IDLE_WINDOW="${1:-${SINOMO_GATE_IDLE_WINDOW:-60}}"
BRIDGE_RECOVERY_WAIT="${2:-${SINOMO_GATE_BRIDGE_RECOVERY_WAIT:-90}}"
FORMATION_WAIT="${SINOMO_GATE_FORMATION_WAIT:-30}"
POLL_INTERVAL="${SINOMO_GATE_POLL_INTERVAL:-1}"
COMMAND_TIMEOUT="${SINOMO_GATE_COMMAND_TIMEOUT:-10}"
EXPECTED_MAINS="${SINOMO_GATE_EXPECTED_MAINS:-6}"
REBUILD="${SINOMO_GATE_REBUILD:-1}"
RESET_VOLUMES="${SINOMO_GATE_RESET_VOLUMES:-$REBUILD}"
SKIP_MOBILE="${SINOMO_GATE_SKIP_MOBILE:-0}"
CYCLES="${SINOMO_GATE_CYCLES:-1}"
QUIESCE_WAIT="${SINOMO_GATE_QUIESCE_WAIT:-10}"
QUIESCE_WINDOW="${SINOMO_GATE_QUIESCE_WINDOW:-135}"
QUIESCE_TIMEOUT="${SINOMO_GATE_QUIESCE_TIMEOUT:-600}"
NO_QUIET_RETRY="${SINOMO_GATE_NO_QUIET_RETRY:-0}"
MEASURE_NODES="${SINOMO_GATE_IDLE_NODES:-node1 node2 node3 node4 node5 node6}"

case "$NO_QUIET_RETRY" in
  0|1) ;;
  *) echo "SINOMO_GATE_NO_QUIET_RETRY must be 0 or 1"; exit 2 ;;
esac

case "$IDLE_WINDOW:$BRIDGE_RECOVERY_WAIT:$FORMATION_WAIT:$POLL_INTERVAL:$COMMAND_TIMEOUT:$EXPECTED_MAINS:$CYCLES:$QUIESCE_WAIT:$QUIESCE_WINDOW:$QUIESCE_TIMEOUT" in
  *[!0-9:]*)
    echo "all timing/count arguments must be positive integers"
    exit 2
    ;;
esac
for value in "$IDLE_WINDOW" "$BRIDGE_RECOVERY_WAIT" "$FORMATION_WAIT" "$POLL_INTERVAL" "$COMMAND_TIMEOUT" "$EXPECTED_MAINS" "$CYCLES" "$QUIESCE_WAIT" "$QUIESCE_WINDOW" "$QUIESCE_TIMEOUT"; do
  if [ "$value" -le 0 ]; then
    echo "all timing/count arguments must be greater than 0"
    exit 2
  fi
done

compose_exec_timeout() {
  local seconds="$1" node="$2"
  shift 2
  timeout --foreground "${seconds}s" docker compose \
    exec -T "$node" timeout --foreground "${seconds}s" "$@"
}

deadline_command_timeout() {
  local deadline="$1" maximum="$2" now remaining
  now="$(date +%s)"
  remaining=$(( deadline - now ))
  [ "$remaining" -gt 0 ] || return 1
  if [ "$remaining" -lt "$maximum" ]; then
    printf '%s\n' "$remaining"
  else
    printf '%s\n' "$maximum"
  fi
}

doctor_connectivity_warnings() {
  local seconds="${1:-$COMMAND_TIMEOUT}" output
  if ! output="$(compose_exec_timeout "$seconds" node1 sinomo cluster doctor 2>/dev/null)"; then
    return 1
  fi
  printf '%s\n' "$output" | grep -E "endpoint-unreachable|node-offline" || true
}

cluster_health() {
  local seconds="${1:-$COMMAND_TIMEOUT}"
  compose_exec_timeout "$seconds" node1 sinomo cluster health 2>/dev/null || true
}

wait_for_cluster_ready() {
  local started deadline now elapsed health online warnings command_timeout
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  while :; do
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")"; then
      return 1
    fi
    health="$(cluster_health "$command_timeout")"
    online="$(printf '%s\n' "$health" | grep -c "daemon-main.*online" || true)"
    warnings=""
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ]; then
      if command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")"; then
        if ! warnings="$(doctor_connectivity_warnings "$command_timeout")"; then
          warnings="cluster doctor failed or timed out"
        fi
      else
        warnings="readiness deadline exhausted before doctor completed"
      fi
    fi
    now="$(date +%s)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      elapsed=$(( now - started ))
      if [ "$now" -le "$deadline" ]; then
        echo "cluster ready (${online} main daemons online, ${elapsed}s)"
        return 0
      fi
    fi
    [ "$now" -ge "$deadline" ] && return 1
    sleep "$POLL_INTERVAL"
  done
}

print_failure_context() {
  echo
  echo "=== compose ps ==="
  docker compose ps || true
  echo
  echo "=== cluster health ==="
  cluster_health
  echo
  echo "=== cluster doctor connectivity findings ==="
  doctor_connectivity_warnings || true
}

wait_for_named_ready() {
  local label="$1"
  echo "=== ${label}: wait for clean cluster ==="
  if ! wait_for_cluster_ready; then
    echo "${label}: cluster did not reach a clean ${EXPECTED_MAINS}-main state within ${FORMATION_WAIT}s"
    print_failure_context
    exit 1
  fi
}

node_daemon_write_snapshot() {
  docker compose exec -T "$1" sh -c '
    main_count=0; shadow_count=0
    main_identity=""; shadow_identity=""
    main_write_bytes=""; shadow_write_bytes=""
    for pid in $(pgrep -x sinomo 2>/dev/null); do
      io=/proc/$pid/io
      [ -r "$io" ] || continue
      cmd=$(tr "\000" " " </proc/$pid/cmdline 2>/dev/null) || continue
      case "$cmd" in
        *" daemon shadow run "*) role=shadow; shadow_count=$((shadow_count + 1)) ;;
        *" daemon run "*) role=main; main_count=$((main_count + 1)) ;;
        *) continue ;;
      esac
      start=$(awk "{print \$22}" /proc/$pid/stat 2>/dev/null)
      wb=$(awk "/^write_bytes:/{print \$2}" "$io")
      [ -n "$start" ] && [ -n "$wb" ] || exit 1
      if [ "$role" = main ]; then
        main_identity="$pid:$start"
        main_write_bytes="$wb"
      else
        shadow_identity="$pid:$start"
        shadow_write_bytes="$wb"
      fi
    done
    [ "$main_count" -eq 1 ] && [ "$shadow_count" -eq 1 ] || exit 1
    printf "%s %s %s %s\n" \
      "$main_identity" "$main_write_bytes" \
      "$shadow_identity" "$shadow_write_bytes"
  ' 2>/dev/null
}

daemon_snapshot_identity() {
  printf '%s\n' "$1" | awk '{print $1 " " $3}'
}

daemon_snapshot_write_bytes() {
  printf '%s\n' "$1" | awk '{print $2 + $4}'
}

enforce_idle_write_stability() {
  local label="$1"
  local node before_snapshot after_snapshot before_identity after_identity
  local before_value after_value delta total_delta delta_details
  local deadline now attempt
  echo "=== ${label}: find a ${QUIESCE_WINDOW}s zero-write window within ${QUIESCE_TIMEOUT}s ==="
  sleep "$QUIESCE_WAIT"
  deadline=$(( $(date +%s) + QUIESCE_TIMEOUT ))
  attempt=0

  local -A before=()
  while :; do
    attempt=$(( attempt + 1 ))
    if [ -n "${SINOMO_GATE_OWNED_IO_ARTIFACTS:-}" ]; then
      mkdir -p "$SINOMO_GATE_OWNED_IO_ARTIFACTS"
      local owned_artifacts owned_status
      local -a owned_nodes
      owned_artifacts="$(mktemp -d "$SINOMO_GATE_OWNED_IO_ARTIFACTS/settling-XXXXXXXX")"
      read -r -a owned_nodes <<< "$MEASURE_NODES"
      owned_status=0
      python3 "$SINOMO_DOCKER_DIR/measure-owned-io.py" \
        --project "$COMPOSE_PROJECT_NAME" --nodes "${owned_nodes[@]}" \
        --window "$QUIESCE_WINDOW" --output "$owned_artifacts/io.json" || owned_status=$?
      if [ "$owned_status" -eq 0 ]; then
        echo "owned main/shadow/helper block writes stayed quiet for ${QUIESCE_WINDOW}s (attempt ${attempt})"
        return
      fi
      if [ "$owned_status" -ne 1 ]; then
        echo "owned I/O observation incomplete; preserving failed coverage, no reset/stitching"
        exit "$owned_status"
      fi
      if [ "$NO_QUIET_RETRY" = 1 ]; then
        echo "first fixed owned quiet window failed; retaining evidence without phase search"
        exit 1
      fi
      now="$(date +%s)"
      if [ $(( now + QUIESCE_WINDOW )) -gt "$deadline" ]; then
        break
      fi
      echo "retrying owned zero-write window before the original quiescence deadline"
      continue
    fi
    before=()
    for node in $MEASURE_NODES; do
      if ! before_snapshot="$(node_daemon_write_snapshot "$node")" || [ -z "$before_snapshot" ]; then
        echo "failed to sample the main and shadow daemon identities/write bytes for $node"
        exit 1
      fi
      before[$node]="$before_snapshot"
    done

    sleep "$QUIESCE_WINDOW"

    total_delta=0
    delta_details=""
    for node in $MEASURE_NODES; do
      if ! after_snapshot="$(node_daemon_write_snapshot "$node")" || [ -z "$after_snapshot" ]; then
        echo "failed to sample the main and shadow daemon identities/write bytes for $node"
        exit 1
      fi
      before_identity="$(daemon_snapshot_identity "${before[$node]}")"
      after_identity="$(daemon_snapshot_identity "$after_snapshot")"
      if [ "$before_identity" != "$after_identity" ]; then
        echo "$node daemon process set changed during idle stability check"
        echo "before: $before_identity"
        echo "after:  $after_identity"
        exit 1
      fi
      before_value="$(daemon_snapshot_write_bytes "${before[$node]}")"
      after_value="$(daemon_snapshot_write_bytes "$after_snapshot")"
      delta=$(( after_value - before_value ))
      if [ "$delta" -lt 0 ]; then
        echo "$node daemon write counter reset while checking idle stability"
        exit 1
      fi
      total_delta=$(( total_delta + delta ))
      if [ "$delta" -gt 0 ]; then
        delta_details="${delta_details}${delta_details:+, }${node}=${delta}B"
      fi
    done

    if [ "$total_delta" -eq 0 ]; then
      echo "daemon block writes stayed quiet for ${QUIESCE_WINDOW}s (attempt ${attempt})"
      return
    fi

    echo "daemon block writes still settling on attempt ${attempt}; delta=${total_delta}B over ${QUIESCE_WINDOW}s"
    echo "nonzero node deltas: $delta_details"
    if [ "$NO_QUIET_RETRY" = 1 ]; then
      echo "first fixed quiet window failed; retaining evidence without phase search"
      exit 1
    fi
    now="$(date +%s)"
    if [ $(( now + QUIESCE_WINDOW )) -gt "$deadline" ]; then
      break
    fi
    echo "retrying zero-write window before the quiescence deadline"
  done

  echo "${label}: no ${QUIESCE_WINDOW}s zero-write window within ${QUIESCE_TIMEOUT}s"
  exit 1
}

run_validation_cycle() {
  local cycle="$1"
  enforce_idle_write_stability "cycle ${cycle}/${CYCLES} idle preflight"
  echo "=== cycle ${cycle}/${CYCLES}: enforce idle resource budget (${IDLE_WINDOW}s) ==="
  docker/measure-idle.sh "$IDLE_WINDOW"

  if [ "$SKIP_MOBILE" = "0" ]; then
    echo "=== cycle ${cycle}/${CYCLES}: enforce mobile/sensor idle resource budget (${IDLE_WINDOW}s) ==="
    SINOMO_MOBILE_IDLE_QUIESCE_WAIT="$QUIESCE_WAIT" \
      SINOMO_MOBILE_IDLE_QUIESCE_WINDOW="$QUIESCE_WINDOW" \
      docker/test-mobile-idle.sh "$IDLE_WINDOW"
  fi

  echo "=== cycle ${cycle}/${CYCLES}: bridge loss/recovery (${BRIDGE_RECOVERY_WAIT}s recovery budget) ==="
  docker/test-bridge-recovery.sh "$BRIDGE_RECOVERY_WAIT"
  wait_for_named_ready "cycle ${cycle}/${CYCLES} final"
  if [ -n "${SINOMO_GATE_OWNED_IO_ARTIFACTS:-}" ]; then
    # Recovery is a new workload. Reuse the same bounded settling and idle
    # checks; never splice its counters into the pre-recovery ownership window.
    enforce_idle_write_stability "cycle ${cycle}/${CYCLES} post-recovery"
    docker/measure-idle.sh "$IDLE_WINDOW"
  fi
}

if [ "$REBUILD" != "0" ]; then
  echo "=== build current host binary ==="
  docker compose stop -t 10
  env -u CARGO_BUILD_JOBS cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
  echo "=== build segmented cluster image ==="
  docker compose build
fi

if [ "$RESET_VOLUMES" != "0" ]; then
  echo "=== reset segmented cluster volumes ==="
  docker compose down -v --remove-orphans
fi

echo "=== start segmented cluster ==="
docker compose up -d

wait_for_named_ready "initial formation"

for cycle in $(seq 1 "$CYCLES"); do
  run_validation_cycle "$cycle"
done

echo "=== final health after ${CYCLES} cycle(s) ==="
cluster_health

echo "production gate ok (${CYCLES} cycle(s))"
