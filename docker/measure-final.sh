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
#   SINOMO_GATE_REBUILD=0       reuse the currently running image/containers
#   SINOMO_GATE_RESET_VOLUMES=0 keep existing compose volumes when rebuilding
#   SINOMO_GATE_SKIP_MOBILE=1   skip the mobile/sensor role idle gate
#   SINOMO_GATE_CYCLES=3        repeat idle/mobile/bridge validation after startup
#   SINOMO_GATE_FORMATION_WAIT  seconds to wait for initial cluster health
#   SINOMO_GATE_POLL_INTERVAL   health poll interval in seconds
#   SINOMO_GATE_EXPECTED_MAINS  expected online main-daemon rows
#   SINOMO_GATE_QUIESCE_WAIT    seconds allowed for writes to settle after readiness
#   SINOMO_GATE_QUIESCE_WINDOW  zero-write window required after the settle deadline

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"

set -euo pipefail
cd "$(dirname "$0")/.."

IDLE_WINDOW="${1:-${SINOMO_GATE_IDLE_WINDOW:-60}}"
BRIDGE_RECOVERY_WAIT="${2:-${SINOMO_GATE_BRIDGE_RECOVERY_WAIT:-90}}"
FORMATION_WAIT="${SINOMO_GATE_FORMATION_WAIT:-10}"
POLL_INTERVAL="${SINOMO_GATE_POLL_INTERVAL:-1}"
EXPECTED_MAINS="${SINOMO_GATE_EXPECTED_MAINS:-6}"
REBUILD="${SINOMO_GATE_REBUILD:-1}"
RESET_VOLUMES="${SINOMO_GATE_RESET_VOLUMES:-$REBUILD}"
SKIP_MOBILE="${SINOMO_GATE_SKIP_MOBILE:-0}"
CYCLES="${SINOMO_GATE_CYCLES:-1}"
QUIESCE_WAIT="${SINOMO_GATE_QUIESCE_WAIT:-10}"
QUIESCE_WINDOW="${SINOMO_GATE_QUIESCE_WINDOW:-75}"
MEASURE_NODES="${SINOMO_GATE_IDLE_NODES:-node1 node2 node3 node4 node5 node6}"

case "$IDLE_WINDOW:$BRIDGE_RECOVERY_WAIT:$FORMATION_WAIT:$POLL_INTERVAL:$EXPECTED_MAINS:$CYCLES:$QUIESCE_WAIT:$QUIESCE_WINDOW" in
  *[!0-9:]*)
    echo "all timing/count arguments must be positive integers"
    exit 2
    ;;
esac
for value in "$IDLE_WINDOW" "$BRIDGE_RECOVERY_WAIT" "$FORMATION_WAIT" "$POLL_INTERVAL" "$EXPECTED_MAINS" "$CYCLES" "$QUIESCE_WAIT" "$QUIESCE_WINDOW"; do
  if [ "$value" -le 0 ]; then
    echo "all timing/count arguments must be greater than 0"
    exit 2
  fi
done

main_online_count() {
  docker compose exec -T node1 sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*online" || true
}

doctor_connectivity_warnings() {
  docker compose exec -T node1 sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline" || true
}

cluster_health() {
  docker compose exec -T node1 sinomo cluster health 2>/dev/null || true
}

wait_for_cluster_ready() {
  local started deadline now elapsed online warnings
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      elapsed=$(( $(date +%s) - started ))
      echo "cluster ready (${online} main daemons online, ${elapsed}s)"
      return 0
    fi
    now="$(date +%s)"
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
  doctor_connectivity_warnings
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

node_write_bytes() {
  docker compose exec -T "$1" sh -c '
    total=0
    for pid in $(pgrep -x sinomo 2>/dev/null || pgrep sinomo 2>/dev/null); do
      io=/proc/$pid/io
      [ -r "$io" ] || continue
      wb=$(awk "/^write_bytes:/{print \$2}" "$io")
      total=$((total + ${wb:-0}))
    done
    echo "$total"
  ' 2>/dev/null
}

enforce_idle_write_stability() {
  local label="$1"
  local node before_value after_value delta total_delta delta_details
  echo "=== ${label}: enforce zero daemon block writes after ${QUIESCE_WAIT}s stabilization deadline ==="
  sleep "$QUIESCE_WAIT"

  local -A before=()
  for node in $MEASURE_NODES; do
    if ! before_value="$(node_write_bytes "$node")" || [ -z "$before_value" ]; then
      echo "failed to sample daemon write bytes for $node"
      exit 1
    fi
    before[$node]="$before_value"
  done

  sleep "$QUIESCE_WINDOW"

  total_delta=0
  delta_details=""
  for node in $MEASURE_NODES; do
    if ! after_value="$(node_write_bytes "$node")" || [ -z "$after_value" ]; then
      echo "failed to sample daemon write bytes for $node"
      exit 1
    fi
    delta=$(( after_value - before[$node] ))
    if [ "$delta" -lt 0 ]; then
      echo "$node daemon write counter reset while checking idle stability"
      total_delta=1
    else
      total_delta=$(( total_delta + delta ))
      if [ "$delta" -gt 0 ]; then
        delta_details="${delta_details}${delta_details:+, }${node}=${delta}B"
      fi
    fi
  done

  if [ "$total_delta" -eq 0 ]; then
    echo "daemon block writes stayed quiet for ${QUIESCE_WINDOW}s"
    return
  fi

  echo "daemon block writes occurred after ${label} stabilization deadline; delta=${total_delta}B over ${QUIESCE_WINDOW}s"
  if [ -n "$delta_details" ]; then
    echo "nonzero node deltas: $delta_details"
  fi
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
}

if [ "$REBUILD" != "0" ]; then
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
