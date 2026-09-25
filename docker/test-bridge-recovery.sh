#!/usr/bin/env bash
# Real traffic acceptance for the existing six-node segmented debug cluster.
# Usage: docker/test-bridge-recovery.sh [recovery_wait_seconds] (default 90).
# Requires the selected prebuilt binary to match both running processes per node.
set -euo pipefail
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
PROJECT="${COMPOSE_PROJECT_NAME:-${SINOMO_BRIDGE_PROJECT:-sinomo-dfs-test}}"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
BINARY="${SINOMO_TEST_BINARY:-$SINOMO_ROOT/target/debug/sinomo}"
export SINOMO_DOCKER_BINARY="${SINOMO_DOCKER_BINARY:-$BINARY}"
ARTIFACTS="${SINOMO_BRIDGE_ARTIFACTS:-$SINOMO_ROOT/target/bridge-recovery/$(date +%Y%m%d-%H%M%S)-$$}"
if [ "${SINOMO_BRIDGE_TEST_SERVICE:-node4}" != node4 ] || [ "${SINOMO_BRIDGE_EXPECTED_MAINS:-6}" != 6 ]; then
  echo 'bridge traffic checks require the standard six-node topology and node4 bridge' >&2
  exit 1
fi
args=(--project "$PROJECT" --compose-file "$COMPOSE_FILE" --binary "$BINARY"
      --artifacts "$ARTIFACTS" --wait "${1:-${SINOMO_BRIDGE_RECOVERY_WAIT:-90}}"
      --baseline-wait "${SINOMO_BRIDGE_BASELINE_WAIT:-120}"
      --outage-wait "${SINOMO_BRIDGE_OUTAGE_WAIT:-45}")
compose=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
if [ -n "${SINOMO_TEST_COMPOSE_OVERRIDE:-}" ]; then
  args+=(--compose-file "$SINOMO_TEST_COMPOSE_OVERRIDE")
  compose+=(-f "$SINOMO_TEST_COMPOSE_OVERRIDE")
fi
capture_failure() {
  local rc=$?
  if [ "$rc" != 0 ]; then
    mkdir -p "$ARTIFACTS"
    timeout 20 "${compose[@]}" logs --no-color > "$ARTIFACTS/containers.log" 2>&1 || true
  fi
  echo "bridge evidence: $ARTIFACTS"
  exit "$rc"
}
trap capture_failure EXIT
python3 "$SINOMO_DOCKER_DIR/lib/network_checks.py" bridge "${args[@]}"
