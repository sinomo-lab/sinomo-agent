#!/usr/bin/env bash
# Focused routing/DFS scenario for the segmented Docker dev cluster.
#
# This starts exactly one Sinomo compose project, extends the 6-node segmented
# topology shape into a smaller host-forwarded/manual-endpoint plus routed-LAN
# case, then verifies cluster health, endpoint probing, routed RPC, and a small
# DFS cross-node flow.
set -euo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.routing-scenario.yml}"
PROJECT="${SINOMO_SCENARIO_PROJECT:-sinomo-routing-scenario}"
FORMATION_WAIT="${SINOMO_SCENARIO_FORMATION_WAIT:-10}"
DFS_READY_WAIT="${SINOMO_SCENARIO_DFS_READY_WAIT:-120}"
EXPECT_TIMEOUT="${SINOMO_SCENARIO_EXPECT_TIMEOUT:-90}"
POLL_INTERVAL="${SINOMO_SCENARIO_POLL_INTERVAL:-1}"
PUBLIC_PORT="${SINOMO_SCENARIO_PUBLIC_PORT:-32166}"
EXPECTED_MAINS=3
NODES="node1 node2 node3"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_SCENARIO_ARTIFACTS:-$SINOMO_ROOT/target/routing-scenario-artifacts/$RUN_ID}"

mkdir -p "$ARTIFACTS"

log() { echo "[routing-scenario] $*"; }

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

running_other_sinomo_projects() {
  docker ps \
    --filter label=com.docker.compose.project \
    --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null \
    | sort -u \
    | while IFS= read -r project; do
        case "$project" in
          "$PROJECT"|"")
            ;;
          sinomo*)
            printf '%s\n' "$project"
            ;;
        esac
      done
}

require_no_other_sinomo_projects() {
  local others
  others="$(running_other_sinomo_projects || true)"
  if [ -n "$others" ]; then
    log "refusing to start $PROJECT while another Sinomo compose project is running:"
    printf '%s\n' "$others" | sed 's/^/[routing-scenario]   /'
    return 1
  fi
}

collect_artifacts() {
  log "collecting artifacts in $ARTIFACTS"
  compose ps > "$ARTIFACTS/compose-ps.txt" 2>&1 || true
  compose exec -T node1 sinomo cluster health > "$ARTIFACTS/cluster-health-node1.txt" 2>&1 || true
  compose exec -T node1 sinomo cluster doctor > "$ARTIFACTS/cluster-doctor-node1.txt" 2>&1 || true
  compose exec -T node1 sinomo cluster connectivity > "$ARTIFACTS/cluster-connectivity-node1.txt" 2>&1 || true
  for node in $NODES; do
    compose logs --no-color "$node" > "$ARTIFACTS/$node.log" 2>&1 || true
  done
}

cleanup() {
  local rc="$?"
  if [ "$rc" -ne 0 ]; then
    collect_artifacts
  fi
  if [ "${SINOMO_SCENARIO_KEEP:-0}" = 1 ]; then
    log "leaving compose project $PROJECT running"
  else
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

main_online_count() {
  compose exec -T node1 sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*online" || true
}

doctor_connectivity_warnings() {
  compose exec -T node1 sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline" || true
}

wait_for_cluster_ready() {
  local started deadline now elapsed online warnings
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  log "waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    now="$(date +%s)"
    elapsed=$(( now - started ))
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      log "cluster ready (${online} main daemons online, ${elapsed}s)"
      return 0
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "cluster NOT ready after ${FORMATION_WAIT}s (online=${online:-0})"
      compose exec -T node1 sinomo cluster health || true
      compose exec -T node1 sinomo cluster doctor || true
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

wait_for_dfs_ready() {
  local deadline node pending still
  deadline=$(( $(date +%s) + DFS_READY_WAIT ))
  pending="$NODES"
  log "waiting for SIFS readiness on all nodes (<=${DFS_READY_WAIT}s)"
  while :; do
    still=""
    for node in $pending; do
      if compose exec -T "$node" sinomo test run cluster.dfs.invariants \
          --require-cluster \
          --json \
          --param timeout-seconds=5 \
          >"$ARTIFACTS/dfs-ready-$node.last.json" 2>&1; then
        log "  dfs-ready: $node"
      else
        still="$still $node"
      fi
    done
    pending="${still# }"
    [ -z "$pending" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "SIFS readiness timed out for: $pending"
      for node in $pending; do
        sed 's/^/[routing-scenario]   /' "$ARTIFACTS/dfs-ready-$node.last.json" | tail -12
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

PROBE_INDEX=0
run_probe() {
  local node="$1" id="$2" slug probe_log
  shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-${id//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  if compose exec -T "$node" sinomo test run "$id" --require-cluster --json "$@" \
      >"$probe_log" 2>&1; then
    log "  ok: $node $id"
    return 0
  fi
  log "  FAILED: $node $id $*"
  sed 's/^/[routing-scenario]   /' "$probe_log"
  return 1
}

run_network_probe() {
  local node="$1"
  run_probe "$node" cluster.network.ping-benchmark \
    --param expected-targets=2 \
    --param requests-per-target=4 \
    --param warmup-requests-per-target=2 \
    --param concurrency=8 \
    --param timeout-ms=2000 \
    --param max-failures=0 \
    --param max-p50-ms=1000 \
    --param max-p95-ms=3000 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param min-requests-per-second=1
}

assert_forwarded_endpoint_configured() {
  log "checking node1 manual forwarded endpoint config"
  compose exec -T node1 grep -F "host.docker.internal:$PUBLIC_PORT" /data/configs/cluster/endpoints.json
}

assert_forwarded_endpoint_active() {
  local deadline output
  deadline=$(( $(date +%s) + 30 ))
  log "checking active connectivity for host-forwarded node1 endpoint from node3"
  while :; do
    output="$(compose exec -T node3 sinomo cluster connectivity 2>&1 || true)"
    printf '%s\n' "$output" > "$ARTIFACTS/cluster-connectivity-node3.last.txt"
    if printf '%s\n' "$output" \
        | grep -F "host.docker.internal:$PUBLIC_PORT" \
        | grep -F "Healthy" >/dev/null; then
      log "  forwarded endpoint is healthy"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "forwarded endpoint did not become healthy"
      printf '%s\n' "$output"
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

run_cross_segment_dfs() {
  local path_a path_b payload_a payload_b
  path_a="/services/routing-scenario/$RUN_ID/node2-to-node3.txt"
  path_b="/services/routing-scenario/$RUN_ID/node3-to-node1.txt"
  payload_a="routing-scenario node2 node3 $RUN_ID"
  payload_b="routing-scenario node3 node1 $RUN_ID"
  run_probe node2 cluster.dfs.write \
    --param "path=$path_a" \
    --param "payload=$payload_a"
  run_probe node3 cluster.dfs.expect-file \
    --param "path=$path_a" \
    --param "payload=$payload_a" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
  run_probe node3 cluster.dfs.write \
    --param "path=$path_b" \
    --param "payload=$payload_b"
  run_probe node1 cluster.dfs.expect-file \
    --param "path=$path_b" \
    --param "payload=$payload_b" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

require_no_other_sinomo_projects
if [ "${SINOMO_SCENARIO_BUILD:-1}" = 1 ]; then
  log "building host binary"
  cargo build -p sinomo
fi

log "resetting compose project $PROJECT"
compose down -v --remove-orphans >/dev/null 2>&1 || true
require_no_other_sinomo_projects

log "starting compose project $PROJECT"
compose up --build -d
wait_for_cluster_ready
assert_forwarded_endpoint_configured
run_network_probe node1
run_network_probe node3
assert_forwarded_endpoint_active
wait_for_dfs_ready
run_cross_segment_dfs

log "PASS; artifacts in $ARTIFACTS"
