#!/usr/bin/env bash
# Validate battery-constrained node roles in the segmented Docker cluster. The
# script changes selected services to mobile/sensor roles from inside their own
# containers so the local cluster-state copy adopts the role-aware maintenance
# cadence, enforces the idle I/O budget, then restores the original roles.
#
#   docker/test-mobile-idle.sh [idle_window_seconds]   (default 60)
#
# Environment:
#   SINOMO_MOBILE_IDLE_PROJECT="sinomo-dfs-test"
#   SINOMO_MOBILE_IDLE_ROLES="node5=mobile node6=sensor"
#   SINOMO_MOBILE_IDLE_MEASURE_NODES="node1 node2 node3 node4 node5 node6"
#   SINOMO_MOBILE_IDLE_ROLE_WAIT=60
#   SINOMO_MOBILE_IDLE_QUIESCE_WAIT=10
#   SINOMO_MOBILE_IDLE_QUIESCE_WINDOW=75
#   SINOMO_MOBILE_IDLE_POLL_INTERVAL=3

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${SINOMO_MOBILE_IDLE_PROJECT:-sinomo-dfs-test}}"

set -euo pipefail
cd "$(dirname "$0")/.."

WINDOW="${1:-${SINOMO_MOBILE_IDLE_WINDOW:-60}}"
ROLE_ASSIGNMENTS="${SINOMO_MOBILE_IDLE_ROLES:-node5=mobile node6=sensor}"
MEASURE_NODES="${SINOMO_MOBILE_IDLE_MEASURE_NODES:-node1 node2 node3 node4 node5 node6}"
ROLE_WAIT="${SINOMO_MOBILE_IDLE_ROLE_WAIT:-60}"
QUIESCE_WAIT="${SINOMO_MOBILE_IDLE_QUIESCE_WAIT:-10}"
QUIESCE_WINDOW="${SINOMO_MOBILE_IDLE_QUIESCE_WINDOW:-75}"
POLL_INTERVAL="${SINOMO_MOBILE_IDLE_POLL_INTERVAL:-3}"

case "$WINDOW:$ROLE_WAIT:$QUIESCE_WAIT:$QUIESCE_WINDOW:$POLL_INTERVAL" in
  *[!0-9:]*)
    echo "all timing arguments must be positive integers"
    exit 2
    ;;
esac
for value in "$WINDOW" "$ROLE_WAIT" "$QUIESCE_WAIT" "$QUIESCE_WINDOW" "$POLL_INTERVAL"; do
  if [ "$value" -le 0 ]; then
    echo "all timing arguments must be greater than 0"
    exit 2
  fi
done

role_valid() {
  case "$1" in
    workstation|mobile|server|sensor|test) return 0 ;;
    *) return 1 ;;
  esac
}

local_node_id() {
  docker compose exec -T "$1" sinomo cluster status \
    | awk '/^local node:/{print $3; found=1} END{if(!found) exit 1}'
}

role_for_node_id() {
  docker compose exec -T node1 sinomo cluster peers \
    | awk -F '\t' -v node_id="$1" '$1 == node_id { print $3; found=1 } END{ if(!found) exit 1 }'
}

cluster_health() {
  docker compose exec -T node1 sinomo cluster health 2>/dev/null || true
}

doctor_connectivity_warnings() {
  docker compose exec -T node1 sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline" || true
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
  echo "=== enforce zero daemon block writes after ${label} + ${QUIESCE_WAIT}s stabilization deadline ==="
  sleep "$QUIESCE_WAIT"

  if [ -n "${SINOMO_GATE_OWNED_IO_ARTIFACTS:-}" ]; then
    # Reuse the same stable live-owner window as the ordinary settling gate.
    # It retains exact main/shadow zero-write checks and additionally covers
    # exited helpers, output sinks and delayed writeback without resetting time.
    mkdir -p "$SINOMO_GATE_OWNED_IO_ARTIFACTS"
    local owned_artifacts owned_status
    local -a owned_nodes
    owned_artifacts="$(mktemp -d "$SINOMO_GATE_OWNED_IO_ARTIFACTS/mobile-stability-XXXXXXXX")"
    printf '%s\n' "$label" > "$owned_artifacts/stage.txt"
    read -r -a owned_nodes <<< "$MEASURE_NODES"
    owned_status=0
    python3 "$SINOMO_DOCKER_DIR/measure-owned-io.py" \
      --project "$COMPOSE_PROJECT_NAME" --nodes "${owned_nodes[@]}" \
      --window "$QUIESCE_WINDOW" --output "$owned_artifacts/io.json" || owned_status=$?
    if [ "$owned_status" -ne 0 ]; then
      echo "owned mobile/role-restoration write observation failed or was incomplete; no retry or deadline reset"
      exit "$owned_status"
    fi
    echo "owned main/shadow/helper block writes stayed quiet for ${QUIESCE_WINDOW}s"
    return
  fi

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

declare -a TARGET_SERVICES=()
declare -a TARGET_IDS=()
declare -a TARGET_ROLES=()
declare -a ORIGINAL_ROLES=()
RESTORE_STARTED=0

restore_roles() {
  if [ "$RESTORE_STARTED" -ne 0 ]; then
    return
  fi
  RESTORE_STARTED=1
  set +e
  for index in "${!TARGET_SERVICES[@]}"; do
    service="${TARGET_SERVICES[$index]}"
    node_id="${TARGET_IDS[$index]}"
    role="${ORIGINAL_ROLES[$index]}"
    if [ -n "$node_id" ] && [ -n "$role" ]; then
      docker compose exec -T "$service" sinomo cluster role "$node_id" "$role" >/dev/null 2>&1 \
        || docker compose exec -T node1 sinomo cluster role "$node_id" "$role" >/dev/null 2>&1 \
        || true
    fi
  done
}
trap restore_roles EXIT

for assignment in $ROLE_ASSIGNMENTS; do
  service="${assignment%%=*}"
  role="${assignment#*=}"
  if [ -z "$service" ] || [ "$service" = "$assignment" ] || ! role_valid "$role"; then
    echo "invalid role assignment: $assignment"
    exit 2
  fi
  node_id="$(local_node_id "$service")"
  original_role="$(role_for_node_id "$node_id")"
  TARGET_SERVICES+=("$service")
  TARGET_IDS+=("$node_id")
  TARGET_ROLES+=("$role")
  ORIGINAL_ROLES+=("$original_role")
done

echo "=== apply battery-constrained roles ==="
for index in "${!TARGET_SERVICES[@]}"; do
  service="${TARGET_SERVICES[$index]}"
  node_id="${TARGET_IDS[$index]}"
  role="${TARGET_ROLES[$index]}"
  original_role="${ORIGINAL_ROLES[$index]}"
  echo "$service $node_id: $original_role -> $role"
  docker compose exec -T "$service" sinomo cluster role "$node_id" "$role"
done

echo "=== wait for role publication ==="
deadline=$(( $(date +%s) + ROLE_WAIT ))
while :; do
  all_ready=1
  for index in "${!TARGET_IDS[@]}"; do
    node_id="${TARGET_IDS[$index]}"
    expected="${TARGET_ROLES[$index]}"
    actual="$(role_for_node_id "$node_id" 2>/dev/null || true)"
    if [ "$actual" != "$expected" ]; then
      all_ready=0
      break
    fi
  done
  if [ "$all_ready" -eq 1 ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "role changes did not publish within ${ROLE_WAIT}s"
    docker compose exec -T node1 sinomo cluster peers || true
    exit 1
  fi
  sleep "$POLL_INTERVAL"
done

if [ -n "$(doctor_connectivity_warnings)" ]; then
  echo "cluster has connectivity warnings before mobile idle measurement"
  cluster_health
  doctor_connectivity_warnings
  exit 1
fi
enforce_idle_write_stability "battery-constrained role transition"

echo "=== enforce mobile/sensor idle budget (${WINDOW}s) ==="
SINOMO_IDLE_NODES="$MEASURE_NODES" docker/measure-idle.sh "$WINDOW"

echo "=== restore original roles ==="
restore_roles
trap - EXIT

echo "=== wait for restored roles ==="
deadline=$(( $(date +%s) + ROLE_WAIT ))
while :; do
  all_ready=1
  for index in "${!TARGET_IDS[@]}"; do
    node_id="${TARGET_IDS[$index]}"
    expected="${ORIGINAL_ROLES[$index]}"
    actual="$(role_for_node_id "$node_id" 2>/dev/null || true)"
    if [ "$actual" != "$expected" ]; then
      all_ready=0
      break
    fi
  done
  if [ "$all_ready" -eq 1 ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "original roles were not restored within ${ROLE_WAIT}s"
    docker compose exec -T node1 sinomo cluster peers || true
    exit 1
  fi
  sleep "$POLL_INTERVAL"
done

if [ -n "$(doctor_connectivity_warnings)" ]; then
  echo "cluster has connectivity warnings after role restoration"
  cluster_health
  doctor_connectivity_warnings
  exit 1
fi
enforce_idle_write_stability "role restoration"

echo "mobile/sensor idle gate ok"
