#!/usr/bin/env bash
# Focused routing/DFS scenario for the segmented Docker dev cluster.
#
# This starts exactly one Sinomo compose project, extends the 6-node segmented
# topology shape into a smaller host-forwarded/manual-endpoint plus routed-LAN
# case, then verifies cluster health, endpoint probing, routed RPC, routed
# payload throughput, and small plus large DFS cross-node flows.
set -euo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.routing-scenario.yml}"
PROJECT="${SINOMO_SCENARIO_PROJECT:-sinomo-routing-scenario}"
FORMATION_WAIT="${SINOMO_SCENARIO_FORMATION_WAIT:-30}"
DFS_READY_WAIT="${SINOMO_SCENARIO_DFS_READY_WAIT:-120}"
EXPECT_TIMEOUT="${SINOMO_SCENARIO_EXPECT_TIMEOUT:-90}"
POLL_INTERVAL="${SINOMO_SCENARIO_POLL_INTERVAL:-1}"
PUBLIC_PORT="${SINOMO_SCENARIO_PUBLIC_PORT:-32166}"
DIRECT_STALL_PORT="${SINOMO_SCENARIO_DIRECT_STALL_PORT:-32169}"
NETWORK_REQUESTS_PER_TARGET="${SINOMO_SCENARIO_NETWORK_REQUESTS_PER_TARGET:-4}"
NETWORK_WARMUP_REQUESTS_PER_TARGET="${SINOMO_SCENARIO_NETWORK_WARMUP_REQUESTS_PER_TARGET:-2}"
NETWORK_CONCURRENCY="${SINOMO_SCENARIO_NETWORK_CONCURRENCY:-8}"
NETWORK_TIMEOUT_MS="${SINOMO_SCENARIO_NETWORK_TIMEOUT_MS:-2000}"
NETWORK_PAYLOAD_REQUESTS_PER_TARGET="${SINOMO_SCENARIO_NETWORK_PAYLOAD_REQUESTS_PER_TARGET:-4}"
NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET="${SINOMO_SCENARIO_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET:-2}"
NETWORK_PAYLOAD_CONCURRENCY="${SINOMO_SCENARIO_NETWORK_PAYLOAD_CONCURRENCY:-8}"
NETWORK_PAYLOAD_TIMEOUT_MS="${SINOMO_SCENARIO_NETWORK_PAYLOAD_TIMEOUT_MS:-5000}"
# Exercise equal-size large values in both directions through the ordinary RPC
# interface; the fixture sizes are workload choices, not protocol boundaries.
NETWORK_PAYLOAD_REQUEST_BYTES="${SINOMO_SCENARIO_NETWORK_PAYLOAD_REQUEST_BYTES:-262144}"
NETWORK_PAYLOAD_RESPONSE_BYTES="${SINOMO_SCENARIO_NETWORK_PAYLOAD_RESPONSE_BYTES:-262144}"
NETWORK_PAYLOAD_MIN_BYTES_PER_SEC="${SINOMO_SCENARIO_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC:-8388608}"
NETWORK_READY_WAIT="${SINOMO_SCENARIO_NETWORK_READY_WAIT:-20}"
NETWORK_READY_STABLE_SWEEPS="${SINOMO_SCENARIO_NETWORK_READY_STABLE_SWEEPS:-2}"
NETWORK_READY_PROBE_TIMEOUT="${SINOMO_SCENARIO_NETWORK_READY_PROBE_TIMEOUT:-$(( NETWORK_TIMEOUT_MS / 1000 + 6 ))}"
NODE_RESTART_WAIT="${SINOMO_SCENARIO_NODE_RESTART_WAIT:-30}"
READINESS_COMMAND_TIMEOUT="${SINOMO_SCENARIO_READINESS_COMMAND_TIMEOUT:-10}"
ARTIFACT_COMMAND_TIMEOUT="${SINOMO_SCENARIO_ARTIFACT_COMMAND_TIMEOUT:-10}"
PROBE_COMMAND_TIMEOUT="${SINOMO_SCENARIO_PROBE_COMMAND_TIMEOUT:-300}"
HOST_TIMEOUT_GRACE="${SINOMO_SCENARIO_HOST_TIMEOUT_GRACE:-5}"
ENDPOINT_FALLBACK_TIMEOUT_MS="${SINOMO_SCENARIO_ENDPOINT_FALLBACK_TIMEOUT_MS:-5000}"
DIRECT_STALE_FALLBACK_TIMEOUT_MS="${SINOMO_SCENARIO_DIRECT_STALE_FALLBACK_TIMEOUT_MS:-5000}"
DIRECT_STALE_FOLLOWUP_MAX_MS="${SINOMO_SCENARIO_DIRECT_STALE_FOLLOWUP_MAX_MS:-750}"
LARGE_BYTES="${SINOMO_SCENARIO_LARGE_BYTES:-16777216}"
LARGE_TIMEOUT="${SINOMO_SCENARIO_LARGE_TIMEOUT:-180}"
LARGE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_SCENARIO_LARGE_MIN_WRITE_BYTES_PER_SEC:-4194304}"
LARGE_MIN_READ_BYTES_PER_SEC="${SINOMO_SCENARIO_LARGE_MIN_READ_BYTES_PER_SEC:-4194304}"
LARGE_MAX_READ_PROC_WRITE_BYTES="${SINOMO_SCENARIO_LARGE_MAX_READ_PROC_WRITE_BYTES:-0}"
CONCURRENT_DFS_LATENCY_ITERATIONS="${SINOMO_SCENARIO_CONCURRENT_DFS_LATENCY_ITERATIONS:-40}"
CONCURRENT_DFS_PAYLOAD_BYTES="${SINOMO_SCENARIO_CONCURRENT_DFS_PAYLOAD_BYTES:-4096}"
EXPECTED_MAINS=3
NODES="node1 node2 node3"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_SCENARIO_ARTIFACTS:-$SINOMO_ROOT/target/routing-scenario-artifacts/$RUN_ID}"
DIRECT_STALL_PROXY_PID=""
PROJECT_OWNED=0

mkdir -p "$ARTIFACTS"

log() { echo "[routing-scenario] $*"; }

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

compose_timeout() {
  local seconds="$1"
  shift
  timeout --foreground "${seconds}s" docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

compose_exec_timeout() {
  local seconds="$1"
  local node="$2"
  local host_seconds
  shift 2
  host_seconds=$(( seconds + HOST_TIMEOUT_GRACE ))
  timeout --foreground "${host_seconds}s" \
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" \
    exec -T "$node" timeout --foreground "${seconds}s" "$@"
}

deadline_command_timeout() {
  local deadline="$1" maximum="$2" now remaining
  now="$(date +%s)"
  remaining=$(( deadline - now ))
  [ "$remaining" -gt 0 ] || return 1
  [ "$maximum" -gt 0 ] || return 1
  if [ "$remaining" -lt "$maximum" ]; then
    printf '%s\n' "$remaining"
  else
    printf '%s\n' "$maximum"
  fi
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
  compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" ps > "$ARTIFACTS/compose-ps.txt" 2>&1 || true
  for node in $NODES; do
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo cluster health > "$ARTIFACTS/cluster-health-$node.txt" 2>&1 || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo cluster doctor > "$ARTIFACTS/cluster-doctor-$node.txt" 2>&1 || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo cluster connectivity > "$ARTIFACTS/cluster-connectivity-$node.txt" 2>&1 || true
    compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" logs --no-color "$node" > "$ARTIFACTS/$node.log" 2>&1 || true
  done
}

cleanup() {
  local rc="$?"
  stop_direct_stall_proxy || true
  if [ "$PROJECT_OWNED" != 1 ]; then
    exit "$rc"
  fi
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

cluster_health() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}"
  compose_exec_timeout "$seconds" node1 sinomo cluster health 2>/dev/null || true
}

formation_failure_markers() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}"
  compose_exec_timeout "$seconds" node1 sh -c '
    for marker in /shared/*.failed /shared/*.join-failed; do
      [ -f "$marker" ] || continue
      printf "%s\n" "--- $marker"
      sed -n "1,80p" "$marker"
    done
  ' 2>/dev/null
}

doctor_connectivity_warnings() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}" output
  if ! output="$(compose_exec_timeout "$seconds" node1 sinomo cluster doctor 2>/dev/null)"; then
    return 1
  fi
  printf '%s\n' "$output" \
    | grep -E "endpoint-unreachable|node-offline" \
    | grep -Fv "foreign:21353" \
    | grep -Fv "host.docker.internal:$DIRECT_STALL_PORT" || true
}

wait_for_cluster_ready() {
  local started deadline now elapsed health online warnings doctor_warnings command_timeout markers marker_check_ok best_online=0
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  log "waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  while :; do
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
      log "cluster NOT ready after ${FORMATION_WAIT}s (best-online=$best_online)"
      return 1
    fi
    health="$(cluster_health "$command_timeout")"
    online="$(printf '%s\n' "$health" | grep -c "daemon-main.*online" || true)"
    if [ "${online:-0}" -gt "$best_online" ]; then
      best_online="$online"
    fi
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
      log "cluster NOT ready after ${FORMATION_WAIT}s (best-online=$best_online)"
      return 1
    fi
    marker_check_ok=1
    if ! markers="$(formation_failure_markers "$command_timeout")"; then
      marker_check_ok=0
      markers=""
    fi
    if [ -n "$markers" ]; then
      log "cluster formation reported a terminal failure"
      printf '%s\n' "$markers" | sed 's/^/[routing-scenario]   /'
      return 1
    fi
    warnings=""
    if [ "$marker_check_ok" -ne 1 ]; then
      warnings="formation marker check failed or timed out"
    fi
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ]; then
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
        if ! doctor_warnings="$(doctor_connectivity_warnings "$command_timeout")"; then
          warnings="${warnings}${warnings:+; }cluster doctor failed or timed out"
        elif [ -n "$doctor_warnings" ]; then
          warnings="${warnings}${warnings:+; }$doctor_warnings"
        fi
      else
        warnings="${warnings}${warnings:+; }readiness deadline exhausted before doctor completed"
      fi
    fi
    now="$(date +%s)"
    elapsed=$(( now - started ))
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      if [ "$now" -le "$deadline" ]; then
        log "cluster ready (${online} main daemons online, ${elapsed}s)"
        return 0
      fi
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "cluster NOT ready after ${FORMATION_WAIT}s (best-online=$best_online)"
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

wait_for_dfs_ready() {
  local deadline node pending still probe_log probe_err command_timeout now
  deadline=$(( $(date +%s) + DFS_READY_WAIT ))
  pending="$NODES"
  log "waiting for SIFS readiness on all nodes (<=${DFS_READY_WAIT}s)"
  while :; do
    still=""
    for node in $pending; do
      probe_log="$ARTIFACTS/dfs-ready-$node.last.json"
      probe_err="$ARTIFACTS/dfs-ready-$node.last.stderr.log"
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
        && compose_exec_timeout "$command_timeout" "$node" sinomo test run cluster.dfs.invariants \
          --require-cluster \
          --json \
          --param timeout-seconds=5 \
          >"$probe_log" 2>"$probe_err"; then
        now="$(date +%s)"
        if [ "$now" -le "$deadline" ]; then
          log "  dfs-ready: $node"
        else
          still="$still $node"
        fi
      else
        still="$still $node"
      fi
    done
    pending="${still# }"
    now="$(date +%s)"
    if [ -z "$pending" ] && [ "$now" -le "$deadline" ]; then
      return 0
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "SIFS readiness timed out for: $pending"
      for node in $pending; do
        sed 's/^/[routing-scenario]   /' "$ARTIFACTS/dfs-ready-$node.last.json" | tail -12
        sed 's/^/[routing-scenario]   /' "$ARTIFACTS/dfs-ready-$node.last.stderr.log" | tail -4
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

wait_for_network_ready() {
  local deadline node failed probe_log probe_err command_timeout now stable_sweeps
  deadline=$(( $(date +%s) + NETWORK_READY_WAIT ))
  stable_sweeps=0
  failed="$NODES"
  log "waiting for routed RPC readiness on all nodes (<=${NETWORK_READY_WAIT}s, ${NETWORK_READY_STABLE_SWEEPS} stable sweeps)"
  while :; do
    failed=""
    for node in $NODES; do
      probe_log="$ARTIFACTS/network-ready-$node.last.json"
      probe_err="$ARTIFACTS/network-ready-$node.last.stderr.log"
      if command_timeout="$(deadline_command_timeout "$deadline" "$NETWORK_READY_PROBE_TIMEOUT")" \
        && compose_exec_timeout "$command_timeout" \
          "$node" sinomo test run cluster.network.ping-benchmark \
          --require-cluster \
          --json \
          --param expected-targets=2 \
          --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
          --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
          --param "concurrency=$NETWORK_CONCURRENCY" \
          --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
          --param max-failures=0 \
          --param max-p50-ms=1000 \
          --param max-p95-ms=3000 \
          --param max-p50-us=0 \
          --param max-p95-us=0 \
          --param min-requests-per-second=1 \
          >"$probe_log" 2>"$probe_err"; then
        now="$(date +%s)"
        if [ "$now" -le "$deadline" ]; then
          log "  network-ready: $node"
        else
          failed="$failed $node"
        fi
      else
        failed="$failed $node"
      fi
    done
    failed="${failed# }"
    now="$(date +%s)"
    if [ -z "$failed" ] && [ "$now" -le "$deadline" ]; then
      stable_sweeps=$(( stable_sweeps + 1 ))
      if [ "$stable_sweeps" -ge "$NETWORK_READY_STABLE_SWEEPS" ]; then
        return 0
      fi
      log "  routed RPC readiness stable sweep ${stable_sweeps}/${NETWORK_READY_STABLE_SWEEPS}"
    else
      stable_sweeps=0
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "routed RPC readiness timed out for: ${failed:-all nodes failed to remain stable}"
      for node in ${failed:-$NODES}; do
        sed 's/^/[routing-scenario]   /' "$ARTIFACTS/network-ready-$node.last.json" | tail -12
        sed 's/^/[routing-scenario]   /' "$ARTIFACTS/network-ready-$node.last.stderr.log" | tail -4
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

PROBE_INDEX=0
run_probe() {
  local node="$1" id="$2" slug probe_log probe_err
  shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-${id//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  probe_err="$ARTIFACTS/probe-$slug.stderr.log"
  if compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" \
      "$node" sinomo test run "$id" --require-cluster --json "$@" \
      >"$probe_log" 2>"$probe_err"; then
    log "  ok: $node $id"
    return 0
  fi
  log "  FAILED: $node $id $*"
  sed 's/^/[routing-scenario]   /' "$probe_log"
  sed 's/^/[routing-scenario]   /' "$probe_err"
  return 1
}

run_tag_probe() {
  local node="$1" tag="$2" slug probe_log probe_err
  shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-tag-${tag//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  probe_err="$ARTIFACTS/probe-$slug.stderr.log"
  if compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" \
      "$node" sinomo test run --tag "$tag" --require-cluster --json "$@" \
      >"$probe_log" 2>"$probe_err"; then
    log "  ok: $node tag:$tag"
    return 0
  fi
  log "  FAILED: $node tag:$tag $*"
  sed 's/^/[routing-scenario]   /' "$probe_log"
  sed 's/^/[routing-scenario]   /' "$probe_err"
  return 1
}

cluster_local_node_id() {
  local node="$1" node_id
  node_id="$(compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" \
    "$node" sinomo cluster connectivity 2>/dev/null \
    | awk '/^local node:/ { print $3; exit }')"
  if [ -z "$node_id" ]; then
    log "failed to resolve local node id for $node"
    return 1
  fi
  printf '%s\n' "$node_id"
}

run_network_probe() {
  local node="$1"
  run_probe "$node" cluster.network.ping-benchmark \
    --param expected-targets=2 \
    --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
    --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
    --param "concurrency=$NETWORK_CONCURRENCY" \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=1000 \
    --param max-p95-ms=3000 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param min-requests-per-second=1
}

run_network_payload_probe() {
  local node="$1"
  run_probe "$node" cluster.network.ping-benchmark \
    --param expected-targets=2 \
    --param "requests-per-target=$NETWORK_PAYLOAD_REQUESTS_PER_TARGET" \
    --param "warmup-requests-per-target=$NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET" \
    --param "concurrency=$NETWORK_PAYLOAD_CONCURRENCY" \
    --param "timeout-ms=$NETWORK_PAYLOAD_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=0 \
    --param max-p95-ms=0 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param "payload-bytes=$NETWORK_PAYLOAD_REQUEST_BYTES" \
    --param "response-payload-bytes=$NETWORK_PAYLOAD_RESPONSE_BYTES" \
    --param min-requests-per-second=0 \
    --param "min-payload-bytes-per-second=$NETWORK_PAYLOAD_MIN_BYTES_PER_SEC"
}

run_network_hygiene_probe() {
  local node="$1"
  run_probe "$node" cluster.network.hygiene \
    --param expected-targets=2 \
    --param max-findings=0 \
    --param require-active-routes=true
}

run_network_hygiene_dirty_fixture_probe() {
  local node="$1"
  run_probe "$node" cluster.network.hygiene-dirty-fixture \
    --param expected-targets=2
}

run_network_dfs_acceptance_probe() {
  local node="$1"
  run_tag_probe "$node" cluster-network-dfs-acceptance \
    --param expected-targets=2 \
    --param max-findings=0 \
    --param require-active-routes=true \
    --param max-remote-open-conflicts=0 \
    --param max-remote-pending-writes=0 \
    --param max-remote-pending-repairs=0 \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
    --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
    --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
    --param "concurrency=$NETWORK_CONCURRENCY" \
    --param max-failures=0 \
    --param max-p50-ms=1000 \
    --param max-p95-ms=3000 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param "iterations=$CONCURRENT_DFS_LATENCY_ITERATIONS" \
    --param warmup-iterations=8 \
    --param min-requests-per-second=1
}

run_dfs_routed_status_probe() {
  local node="$1"
  run_probe "$node" cluster.dfs.routed-status \
    --param expected-targets=2 \
    --param max-remote-open-conflicts=0 \
    --param max-remote-pending-writes=0 \
    --param max-remote-pending-repairs=0 \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS"
}

run_remote_shard_dfs_probe() {
  local node="$1"
  run_probe "$node" cluster.dfs.remote-shard-write \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

run_remote_shard_dfs_split_probe() {
  local node="$1"
  run_probe "$node" cluster.dfs.remote-shard-write \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
    --param split-diagnostics=true
}

assert_forwarded_endpoint_configured() {
  log "checking node1 manual forwarded endpoint config"
  compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" node1 \
    grep -F "host.docker.internal:$PUBLIC_PORT" /data/configs/cluster/endpoints.json
}

assert_forwarded_endpoint_active() {
  local deadline output command_timeout now
  deadline=$(( $(date +%s) + 30 ))
  log "checking active connectivity for host-forwarded node1 endpoint from node3"
  while :; do
    if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
      output="$(compose_exec_timeout "$command_timeout" node3 sinomo cluster connectivity 2>&1 || true)"
    else
      output=""
    fi
    printf '%s\n' "$output" > "$ARTIFACTS/cluster-connectivity-node3.last.txt"
    now="$(date +%s)"
    if printf '%s\n' "$output" \
        | grep -F "host.docker.internal:$PUBLIC_PORT" \
        | grep -F "Healthy" >/dev/null \
      && [ "$now" -le "$deadline" ]; then
      log "  forwarded endpoint is healthy"
      return 0
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "forwarded endpoint did not become healthy"
      printf '%s\n' "$output"
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

assert_endpoint_error_fallback() {
  log "checking routed RPC fallback after reachable auth-error endpoint"
  run_probe node1 cluster.network.endpoint-error-fallback \
    --param target-name=node3 \
    --param bad-endpoint-address=foreign:21353 \
    --param "timeout-ms=$ENDPOINT_FALLBACK_TIMEOUT_MS"
}

start_direct_stall_proxy() {
  local proxy_log started deadline
  if [ -n "$DIRECT_STALL_PROXY_PID" ] && kill -0 "$DIRECT_STALL_PROXY_PID" 2>/dev/null; then
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    log "python3 is required for the direct stall proxy"
    return 1
  fi
  proxy_log="$ARTIFACTS/direct-stall-proxy.log"
  log "starting direct stall proxy on 0.0.0.0:$DIRECT_STALL_PORT"
  python3 "$SINOMO_DOCKER_DIR/direct_stall_proxy.py" \
    --listen-host 0.0.0.0 \
    --listen-port "$DIRECT_STALL_PORT" \
    >"$proxy_log" 2>&1 &
  DIRECT_STALL_PROXY_PID="$!"
  started="$(date +%s)"
  deadline=$(( started + 10 ))
  while :; do
    if (exec 9<>"/dev/tcp/127.0.0.1/$DIRECT_STALL_PORT") 2>/dev/null; then
      exec 9>&- 2>/dev/null || true
      return 0
    fi
    if ! kill -0 "$DIRECT_STALL_PROXY_PID" 2>/dev/null; then
      log "direct stall proxy exited early"
      sed 's/^/[routing-scenario]   /' "$proxy_log" || true
      return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "direct stall proxy did not start"
      sed 's/^/[routing-scenario]   /' "$proxy_log" || true
      return 1
    fi
    sleep 0.1
  done
}

stop_direct_stall_proxy() {
  if [ -n "$DIRECT_STALL_PROXY_PID" ] && kill -0 "$DIRECT_STALL_PROXY_PID" 2>/dev/null; then
    kill "$DIRECT_STALL_PROXY_PID" 2>/dev/null || true
    wait "$DIRECT_STALL_PROXY_PID" 2>/dev/null || true
  fi
  DIRECT_STALL_PROXY_PID=""
}

assert_stale_direct_demotes_after_timeout() {
  local rc
  log "checking route demotion after a stalled direct endpoint timeout"
  start_direct_stall_proxy
  rc=0
  run_probe node3 cluster.network.stale-direct-timeout-demotion \
    --param target-name=node1 \
    --param "stall-endpoint-address=host.docker.internal:$DIRECT_STALL_PORT" \
    --param "timeout-ms=$DIRECT_STALE_FALLBACK_TIMEOUT_MS" \
    --param "followup-max-ms=$DIRECT_STALE_FOLLOWUP_MAX_MS" || rc="$?"
  stop_direct_stall_proxy
  return "$rc"
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

run_large_routed_dfs() {
  local path_a path_b
  path_a="/services/routing-scenario/$RUN_ID/large-node2-to-node3.bin"
  path_b="/services/routing-scenario/$RUN_ID/large-node3-to-node2.bin"
  run_probe node2 cluster.dfs.large-write \
    --param "path=$path_a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
  && run_probe node3 cluster.dfs.large-expect \
    --param "path=$path_a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node3 cluster.dfs.large-write \
    --param "path=$path_b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
  && run_probe node2 cluster.dfs.large-expect \
    --param "path=$path_b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES"
}

# Prints "<file-system-id> <primary> <epoch> <comma-separated sorted replicas>"
# for one metadata shard as $node's retained configuration describes it.
dfs_shard_layout() {
  local node="$1" shard="$2"
  compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" sinomo dfs status --json 2>/dev/null \
    | python3 -c '
import json, sys
text = sys.stdin.read()
status = json.loads(text[text.index("{"):])
for shard in status.get("metadata_shards", []):
    if shard["shard_id"] == sys.argv[1]:
        print(shard["file_system_id"], shard["primary_node_id"], shard["epoch"],
              ",".join(sorted(shard.get("replica_node_ids", []))) or "-")
        break
else:
    sys.exit(1)
' "$shard"
}

# Shard expansion runs once, with whichever members are voting metadata
# providers at that moment, so which of node2 and node3 replicate a shard
# depends on join timing. Pin the services shard to the nodes that stay
# online, so the offline-node case below tests that an absent non-replica
# never blocks writes, independent of that timing.
pin_services_shard_to_online_nodes() {
  local stopped_id="$1" online_a="$2" online_b="$3"
  local layout fs primary epoch replicas desired now_epoch now_replicas node deadline settled
  if ! layout="$(dfs_shard_layout node1 services)"; then
    log "  could not read the services shard layout"
    return 1
  fi
  read -r fs primary epoch replicas <<<"$layout"
  if [ "$primary" = "$stopped_id" ]; then
    log "  services shard primary is the node about to stop; cannot pin its replicas"
    return 1
  fi
  desired="$(printf '%s\n' "$online_a" "$online_b" | grep -vx "$primary" | sort | paste -sd, -)"
  if [ "$replicas" = "$desired" ]; then
    log "  services shard already replicated only on the online nodes (epoch $epoch)"
    return 0
  fi
  log "  pinning services shard replicas to the online nodes (was $replicas at epoch $epoch)"
  if ! compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" node1 sinomo dfs metadata shard set-replicas \
      --filesystem "$fs" --shard services --expected-epoch "$epoch" --json \
      $(printf -- '--replica %s ' ${desired//,/ }) \
      >"$ARTIFACTS/pin-services-shard.json" 2>"$ARTIFACTS/pin-services-shard.stderr.log"; then
    log "  services shard replica assignment was refused"
    sed 's/^/[routing-scenario]   /' "$ARTIFACTS/pin-services-shard.json" "$ARTIFACTS/pin-services-shard.stderr.log"
    return 1
  fi
  deadline=$(( $(date +%s) + EXPECT_TIMEOUT ))
  while :; do
    settled=1
    for node in node1 node2; do
      if ! layout="$(dfs_shard_layout "$node" services)"; then
        settled=0
        continue
      fi
      read -r _ _ now_epoch now_replicas <<<"$layout"
      if [ "$now_replicas" != "$desired" ] || [ "$now_epoch" -le "$epoch" ]; then
        settled=0
      fi
    done
    if [ "$settled" = 1 ]; then
      log "  services shard pinned to the online nodes"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "  services shard pin did not activate within ${EXPECT_TIMEOUT}s"
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

run_two_online_one_offline_dfs_probe() {
  local node1_id node2_id node3_id path_a path_b payload_a payload_b

  log "checking DFS quorum with node3 offline"
  node1_id="$(cluster_local_node_id node1)"
  node2_id="$(cluster_local_node_id node2)"
  node3_id="$(cluster_local_node_id node3)"
  pin_services_shard_to_online_nodes "$node3_id" "$node1_id" "$node2_id" || return 1
  compose stop node3 >/dev/null

  run_probe node1 cluster.network.ping-benchmark \
    --param "target-node-id=$node2_id" \
    --param expected-targets=1 \
    --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
    --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
    --param "concurrency=$NETWORK_CONCURRENCY" \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=1000 \
    --param max-p95-ms=3000 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param min-requests-per-second=1
  run_probe node2 cluster.network.ping-benchmark \
    --param "target-node-id=$node1_id" \
    --param expected-targets=1 \
    --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
    --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
    --param "concurrency=$NETWORK_CONCURRENCY" \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=1000 \
    --param max-p95-ms=3000 \
    --param max-p50-us=0 \
    --param max-p95-us=0 \
    --param min-requests-per-second=1
  run_probe node1 cluster.dfs.routed-status \
    --param "target-node-id=$node2_id" \
    --param expected-targets=1 \
    --param max-remote-open-conflicts=0 \
    --param max-remote-pending-writes=0 \
    --param max-remote-pending-repairs=0 \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS"

  path_a="/services/routing-scenario/$RUN_ID/offline-node3-node1-to-node2.txt"
  path_b="/services/routing-scenario/$RUN_ID/offline-node3-node2-to-node1.txt"
  payload_a="routing-scenario node1 node2 with node3 offline $RUN_ID"
  payload_b="routing-scenario node2 node1 with node3 offline $RUN_ID"
  run_probe node1 cluster.dfs.write \
    --param "path=$path_a" \
    --param "payload=$payload_a" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
  run_probe node2 cluster.dfs.expect-file \
    --param "path=$path_a" \
    --param "payload=$payload_a" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
  run_probe node2 cluster.dfs.write \
    --param "path=$path_b" \
    --param "payload=$payload_b" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
  run_probe node1 cluster.dfs.expect-file \
    --param "path=$path_b" \
    --param "payload=$payload_b" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"

  compose start node3 >/dev/null
  wait_for_node_daemons node3
  wait_for_network_ready
  run_dfs_routed_status_probe node1
  run_dfs_routed_status_probe node2
  run_dfs_routed_status_probe node3
}

wait_for_node_daemons() {
  local node="$1" deadline command_timeout now
  deadline=$(( $(date +%s) + NODE_RESTART_WAIT ))
  log "waiting for $node main/shadow daemons after restart (<=${NODE_RESTART_WAIT}s)"
  while :; do
    if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
      && compose_exec_timeout "$command_timeout" "$node" sinomo daemon status \
        >"$ARTIFACTS/restart-$node-main.last.txt" 2>&1 \
      && grep -F "sinomo daemon is running" "$ARTIFACTS/restart-$node-main.last.txt" >/dev/null \
      && command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
      && compose_exec_timeout "$command_timeout" "$node" sinomo daemon shadow status \
        >"$ARTIFACTS/restart-$node-shadow.last.txt" 2>&1 \
      && grep -F "sinomo shadow daemon is running" "$ARTIFACTS/restart-$node-shadow.last.txt" >/dev/null; then
      now="$(date +%s)"
      if [ "$now" -le "$deadline" ]; then
        log "  daemons-ready: $node"
        return 0
      fi
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "daemon restart timed out for $node"
      sed 's/^/[routing-scenario]   main: /' "$ARTIFACTS/restart-$node-main.last.txt" | tail -12 || true
      sed 's/^/[routing-scenario]   shadow: /' "$ARTIFACTS/restart-$node-shadow.last.txt" | tail -12 || true
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

run_restart_recovery_probe() {
  local node="$1"
  log "checking network/DFS recovery after restarting $node"
  compose restart "$node"
  wait_for_node_daemons "$node"
  wait_for_network_ready
  run_network_payload_probe "$node"
  run_dfs_routed_status_probe "$node"
}

run_concurrent_duplex_dfs_probe() {
  local pids labels logs failed index
  pids=""
  labels=""
  logs=""
  failed=0
  index=0

  log "checking concurrent bidirectional DFS routing"

  run_concurrent_probe() {
    local node="$1" id="$2" label probe_log
    shift 2
    index=$((index + 1))
    label="$(printf '%02d' "$index")-$node-$id"
    label="${label//[^a-zA-Z0-9._-]/_}"
    probe_log="$ARTIFACTS/concurrent-dfs-$label.json"
    (
      compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" \
        "$node" sinomo test run "$id" --require-cluster --json "$@" \
        >"$probe_log" 2>"$probe_log.stderr.log"
    ) &
    pids="${pids}${pids:+ }$!"
    labels="${labels}${labels:+ }$node:$id"
    logs="${logs}${logs:+ }$probe_log"
  }

  run_concurrent_probe node1 cluster.dfs.read-your-write
  run_concurrent_probe node1 cluster.dfs.lightweight-latency \
    --param "iterations=$CONCURRENT_DFS_LATENCY_ITERATIONS" \
    --param "payload-bytes=$CONCURRENT_DFS_PAYLOAD_BYTES" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
  run_concurrent_probe node3 cluster.dfs.lightweight-latency \
    --param "iterations=$CONCURRENT_DFS_LATENCY_ITERATIONS" \
    --param "payload-bytes=$CONCURRENT_DFS_PAYLOAD_BYTES" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"

  set -- $pids
  local pid_count=$#
  local i=1
  while [ "$i" -le "$pid_count" ]; do
    local pid label probe_log
    pid="$(printf '%s\n' "$pids" | cut -d ' ' -f "$i")"
    label="$(printf '%s\n' "$labels" | cut -d ' ' -f "$i")"
    probe_log="$(printf '%s\n' "$logs" | cut -d ' ' -f "$i")"
    if wait "$pid"; then
      log "  ok: concurrent $label"
    else
      log "  FAILED: concurrent $label"
      sed 's/^/[routing-scenario]   /' "$probe_log"
      sed 's/^/[routing-scenario]   /' "$probe_log.stderr.log"
      failed=1
    fi
    i=$((i + 1))
  done

  if [ "$failed" -ne 0 ]; then
    return 1
  fi
}

require_no_other_sinomo_projects
trap cleanup EXIT
if [ "${SINOMO_SCENARIO_BUILD:-1}" = 1 ]; then
  log "building host binary"
  compose stop -t 10
  env -u CARGO_BUILD_JOBS cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
fi

log "resetting compose project $PROJECT"
PROJECT_OWNED=1
compose down -v --remove-orphans >/dev/null 2>&1 || true
require_no_other_sinomo_projects

log "starting compose project $PROJECT"
compose up --build -d
wait_for_cluster_ready
assert_forwarded_endpoint_configured
wait_for_network_ready
assert_endpoint_error_fallback
assert_stale_direct_demotes_after_timeout
wait_for_network_ready
run_network_probe node1
run_network_probe node2
run_network_probe node3
run_network_payload_probe node1
run_network_payload_probe node2
run_network_payload_probe node3
run_network_hygiene_dirty_fixture_probe node1
wait_for_dfs_ready
run_network_dfs_acceptance_probe node1
run_network_dfs_acceptance_probe node2
run_network_dfs_acceptance_probe node3
run_dfs_routed_status_probe node1
run_dfs_routed_status_probe node2
run_dfs_routed_status_probe node3
run_remote_shard_dfs_probe node1
run_remote_shard_dfs_probe node2
run_remote_shard_dfs_probe node3
run_remote_shard_dfs_split_probe node1
run_remote_shard_dfs_split_probe node2
run_remote_shard_dfs_split_probe node3
assert_forwarded_endpoint_active
run_concurrent_duplex_dfs_probe
run_cross_segment_dfs
run_large_routed_dfs
run_two_online_one_offline_dfs_probe
run_restart_recovery_probe node3

log "PASS; artifacts in $ARTIFACTS"
