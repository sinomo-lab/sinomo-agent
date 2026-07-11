#!/usr/bin/env bash
# Validate that the segmented Docker cluster detects and recovers from losing the
# node3<->node4 bridge network. This exercises native multi-hop routing with
# iroh disabled.
#
#   docker/test-bridge-recovery.sh [recovery_wait_seconds]   (default 90)
#
#   SINOMO_BRIDGE_PROJECT overrides the compose project (default sinomo-dfs-test).

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${SINOMO_BRIDGE_PROJECT:-sinomo-dfs-test}}"

set -euo pipefail
cd "$(dirname "$0")/.."

RECOVERY_WAIT="${1:-${SINOMO_BRIDGE_RECOVERY_WAIT:-90}}"
BASELINE_WAIT="${SINOMO_BRIDGE_BASELINE_WAIT:-120}"
OUTAGE_WAIT="${SINOMO_BRIDGE_OUTAGE_WAIT:-45}"
POLL_INTERVAL="${SINOMO_BRIDGE_POLL_INTERVAL:-3}"
EXPECTED_MAINS="${SINOMO_BRIDGE_EXPECTED_MAINS:-6}"
COMMAND_TIMEOUT="${SINOMO_BRIDGE_COMMAND_TIMEOUT:-10}"
HOST_TIMEOUT_GRACE="${SINOMO_BRIDGE_HOST_TIMEOUT_GRACE:-5}"
SERVICE="${SINOMO_BRIDGE_TEST_SERVICE:-node4}"

compose_timeout() {
  local seconds="$1"
  shift
  timeout --foreground "${seconds}s" docker compose "$@"
}

compose_exec_timeout() {
  local seconds="$1" node="$2" host_seconds
  shift 2
  host_seconds=$(( seconds + HOST_TIMEOUT_GRACE ))
  timeout --foreground "${host_seconds}s" docker compose exec -T "$node" \
    timeout --foreground "${seconds}s" "$@"
}

CONTAINER="${SINOMO_BRIDGE_TEST_CONTAINER:-$(compose_timeout "$COMMAND_TIMEOUT" ps -q "$SERVICE")}"
if [ -z "$CONTAINER" ]; then
  echo "could not resolve compose service container: $SERVICE"
  exit 1
fi
NETWORK="${SINOMO_BRIDGE_TEST_NETWORK:-}"
if [ -z "$NETWORK" ]; then
  NETWORK="$(
    docker inspect "$CONTAINER" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' \
      | awk '/(^|_)seg_ab$/ { print; exit }'
  )"
fi
if [ -z "$NETWORK" ]; then
  echo "could not resolve bridge network for $CONTAINER; set SINOMO_BRIDGE_TEST_NETWORK"
  exit 1
fi

connected() {
  docker inspect "$CONTAINER" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' \
    | grep -qx "$NETWORK"
}

main_online_count() {
  compose_exec_timeout "$COMMAND_TIMEOUT" node1 sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*online" || true
}

main_offline_count() {
  compose_exec_timeout "$COMMAND_TIMEOUT" node1 sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*offline" || true
}

doctor_connectivity_warnings() {
  compose_exec_timeout "$COMMAND_TIMEOUT" node1 sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline" || true
}

cluster_health() {
  compose_exec_timeout "$COMMAND_TIMEOUT" node1 sinomo cluster health 2>/dev/null || true
}

restore_bridge() {
  if ! connected; then
    docker network connect "$NETWORK" "$CONTAINER" >/dev/null 2>&1 || true
  fi
}

wait_for_baseline() {
  local deadline now online warnings
  deadline=$(( $(date +%s) + BASELINE_WAIT ))
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      return 0
    fi
    now="$(date +%s)"
    [ "$now" -ge "$deadline" ] && return 1
    sleep "$POLL_INTERVAL"
  done
}

wait_for_outage_observed() {
  local deadline now offline
  deadline=$(( $(date +%s) + OUTAGE_WAIT ))
  while :; do
    offline="$(main_offline_count)"
    if [ "${offline:-0}" -gt 0 ]; then
      echo "$offline"
      return 0
    fi
    now="$(date +%s)"
    [ "$now" -ge "$deadline" ] && return 1
    sleep "$POLL_INTERVAL"
  done
}

wait_for_recovery() {
  local deadline now online warnings
  deadline=$(( $(date +%s) + RECOVERY_WAIT ))
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      echo "$online"
      return 0
    fi
    now="$(date +%s)"
    [ "$now" -ge "$deadline" ] && return 1
    sleep "$POLL_INTERVAL"
  done
}

trap restore_bridge EXIT

echo "=== baseline ==="
if ! wait_for_baseline; then
  echo "baseline cluster is not fully online"
  cluster_health
  doctor_connectivity_warnings
  exit 1
fi
echo "baseline ok"

echo "=== break bridge: $CONTAINER -X- $NETWORK ==="
if connected; then
  docker network disconnect "$NETWORK" "$CONTAINER"
fi
if compose_exec_timeout "$COMMAND_TIMEOUT" node3 sh -c 'getent hosts node4 >/dev/null'; then
  echo "node3 can still resolve node4 after bridge disconnect"
  exit 1
fi

if ! offline_count="$(wait_for_outage_observed)"; then
  echo "bridge loss was not observed as any offline main daemon"
  cluster_health
  exit 1
fi
echo "bridge loss observed (${offline_count} offline main daemon rows)"

echo "=== restore bridge and wait up to ${RECOVERY_WAIT}s ==="
restore_bridge
if ! online_count="$(wait_for_recovery)"; then
  echo "cluster did not recover all main daemons after bridge restore"
  cluster_health
  doctor_connectivity_warnings
  exit 1
fi

echo "bridge recovery ok (${online_count} main daemons online)"
