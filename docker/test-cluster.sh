#!/usr/bin/env bash
# Automated DFS correctness suite on the 6-node segmented dev cluster.
#
# Brings up a DEDICATED compose project (so it never collides with a manually
# started dev cluster), waits for clean 6-node formation, then drives the
# in-binary cluster runtime test cases (sinomo test run --require-cluster)
# from different nodes to verify network health and cluster-wide POSIX behavior:
#
#   1. DFS protocol codec — in-memory DFS data-plane frame encode/decode baseline
#   2. network latency — signed object-RPC latency/RPS with no failed requests
#   3. network payload — signed object-RPC payload throughput with no failed requests
#   4. invariants     — per-node DFS operational invariants (baseline)
#   5. read-your-write — write/read/rename/delete roundtrip on node1 and node5
#   6. cross-node     — write on one segment, poll-read on the other (both
#                       directions), then a routed delete observed cluster-wide
#   7. rename         — /users -> /services rename issued by a NON-writer node,
#                       observed (and the old path absent) on far nodes
#   8. small baseline — node-local raw tiny-file write baseline for DFS comparison
#   9. bulk perf      — small-file bulk write/read throughput with process I/O
#                       amplification checks and read-side write_bytes == 0
#   10. disk baseline — node-local raw file write/read baseline for DFS comparison
#   11. large-file perf — large object write throughput, cross-node convergence
#                       latency, final-read throughput, process I/O amplification
#                       checks, and read-side write_bytes == 0
#   12. disk damage   — delete one durable loose blob and read through the
#                       remaining replica without read-side disk writes
#   13. network heal  — temporarily disconnect a leaf node, write while it is
#                       isolated, reconnect it, and verify convergence
#   14. restart       — restart a member, re-gate formation, re-read its data
#   15. final sweep   — invariants again on every node
#
# Usage:
#   docker/test-cluster.sh                  # build-if-missing, up, test, down -v
#   SINOMO_TEST_REUSE=1 docker/test-cluster.sh   # reuse the running test project
#   SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
#
# Env knobs:
#   SINOMO_TEST_PROJECT         compose project name   (default sinomo-dfs-test)
#   SINOMO_TEST_FORMATION_WAIT  formation gate seconds (default 10)
#   SINOMO_TEST_EXPECT_TIMEOUT  cross-node poll seconds (default 90)
#   SINOMO_TEST_PERF_FILES      files per bulk-performance leg (default 64)
#   SINOMO_TEST_PERF_WARMUP_FILES files in the no-threshold warmup leg (default 8)
#   SINOMO_TEST_PERF_BYTES      bytes per bulk-performance file (default 4096)
#   SINOMO_TEST_PERF_TIMEOUT    bulk write/read timeout seconds (default 120)
#   SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC  min writer throughput (default 0.75)
#   SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC   min cross-node read/convergence throughput (default 1)
#   SINOMO_TEST_PERF_MAX_PROC_WCHAR_AMPLIFICATION       max writer-process wchar/logical bytes (default 0, disabled)
#   SINOMO_TEST_PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION max writer-process write_bytes/logical bytes (default 0, disabled)
#   SINOMO_TEST_PERF_MAX_PROC_SYSCW_PER_FILE max writer-process write syscalls/logical file (default 4)
#   SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during bulk-expect (default 0)
#   SINOMO_TEST_SMALL_BASELINE_FILES files in raw tiny-file baseline leg (default same as SINOMO_TEST_PERF_FILES)
#   SINOMO_TEST_SMALL_BASELINE_BYTES bytes per raw tiny-file baseline file (default same as SINOMO_TEST_PERF_BYTES)
#   SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES sync each raw tiny-file write before timing completes (default 0)
#   SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC min raw tiny-file throughput (default 0, disabled)
#   SINOMO_TEST_LARGE_BYTES    bytes in large-file performance leg (default 67108864)
#   SINOMO_TEST_LARGE_TIMEOUT  large-file write/read timeout seconds (default 180)
#   SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC min large writer throughput (default 4194304)
#   SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC  min final large read throughput after convergence (default 4194304)
#   SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO min large DFS write / raw write baseline ratio (default 0.33)
#   SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO  min large DFS read / raw read baseline ratio (default 0, disabled)
#   SINOMO_TEST_LARGE_RAW_SYNC_WRITE      sync raw baseline writes before timing completes (default 1)
#   SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION       max large writer-process wchar/logical bytes (default 0, disabled)
#   SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION max large writer-process write_bytes/logical bytes (default 4)
#   SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during large-expect (default 0)
#   SINOMO_TEST_DISK_BASELINE_BYTES bytes in raw disk baseline leg (default same as SINOMO_TEST_LARGE_BYTES)
#   SINOMO_TEST_DISK_BASELINE_SYNC_WRITE sync raw disk baseline writes before timing completes (default 1)
#   SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC min raw write throughput (default 0, disabled)
#   SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC  min raw read throughput (default 0, disabled)
#   SINOMO_TEST_DAMAGE_BYTES    bytes in loose-blob disk damage scenario (default 65536)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES bytes per DFS protocol codec message (default 4194304)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS codec encode/decode iterations per sample (default 8)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC min codec payload encode throughput (default 67108864)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC min codec payload decode throughput (default 67108864)
#   SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET cluster network ping RPCs per remote node (default 20)
#   SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET warmup RPCs per remote node before measured pings (default same as requests)
#   SINOMO_TEST_NETWORK_CONCURRENCY         concurrent network ping workers (default 32)
#   SINOMO_TEST_NETWORK_TIMEOUT_MS          per network ping RPC timeout (default 2000)
#   SINOMO_TEST_NETWORK_MAX_FAILURES        allowed failed network ping RPCs (default 0)
#   SINOMO_TEST_NETWORK_MAX_P50_MS          max network ping p50 latency (default 50)
#   SINOMO_TEST_NETWORK_MAX_P95_MS          max network ping p95 latency (default 250)
#   SINOMO_TEST_NETWORK_MAX_P50_US          max network ping p50 latency (default 10000)
#   SINOMO_TEST_NETWORK_MAX_P95_US          max network ping p95 latency (default 50000)
#   SINOMO_TEST_NETWORK_MIN_RPS             min network ping throughput (default 250)
#   SINOMO_TEST_NETWORK_PAYLOAD_BYTES       request bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES response bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC min latency-baseline payload throughput (default 0, disabled)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET payload-throughput RPCs per remote node (default 4)
#   SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET warmup payload RPCs per remote node (default same as payload requests)
#   SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY payload-throughput workers (default 8)
#   SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS  payload-throughput RPC timeout (default 5000)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES request bytes per payload-throughput RPC (default 262144)
#   SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES response bytes per payload-throughput RPC (default 262144)
#   SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC min payload-throughput rate (default 16777216)
#   SINOMO_TEST_NETWORK_READY_WAIT          network/auth convergence gate seconds (default 120)
#   SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET requests per target during readiness warmup (default 20)
#   SINOMO_TEST_NETWORK_READY_CONCURRENCY   readiness warmup concurrency (default 32)
#   SINOMO_TEST_NETWORK_PARTITION_SECONDS   seconds to keep node6 disconnected in network-heal leg (default 5)
#   SINOMO_TEST_NETWORK_ONLY=1              stop after network baseline
#   SINOMO_TEST_BUILD=1         force `cargo build -p sinomo` first
#   SINOMO_TEST_ARTIFACTS       artifact dir (default target/cluster-test-artifacts/<run>)
#   SINOMO_TEST_ALLOW_OTHER_COMPOSE=1       allow running while unrelated compose projects are up
#
# Exits non-zero if any scenario fails; failure evidence (probe JSON, compose ps,
# cluster health/doctor, per-node logs) lands in the artifact directory.
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"

PROJECT="${SINOMO_TEST_PROJECT:-sinomo-dfs-test}"
FORMATION_WAIT="${SINOMO_TEST_FORMATION_WAIT:-10}"
DFS_READY_WAIT="${SINOMO_TEST_DFS_READY_WAIT:-240}"
EXPECT_TIMEOUT="${SINOMO_TEST_EXPECT_TIMEOUT:-90}"
POLL_INTERVAL="${SINOMO_TEST_POLL_INTERVAL:-1}"
EXPECTED_MAINS=6
PERF_FILES="${SINOMO_TEST_PERF_FILES:-64}"
PERF_WARMUP_FILES="${SINOMO_TEST_PERF_WARMUP_FILES:-8}"
PERF_BYTES="${SINOMO_TEST_PERF_BYTES:-4096}"
PERF_TIMEOUT="${SINOMO_TEST_PERF_TIMEOUT:-120}"
PERF_MIN_WRITE_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC:-0.75}"
PERF_MIN_READ_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC:-1}"
PERF_MAX_PROC_WCHAR_AMPLIFICATION="${SINOMO_TEST_PERF_MAX_PROC_WCHAR_AMPLIFICATION:-0}"
PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION="${SINOMO_TEST_PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION:-0}"
PERF_MAX_PROC_SYSCW_PER_FILE="${SINOMO_TEST_PERF_MAX_PROC_SYSCW_PER_FILE:-4}"
PERF_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES:-0}"
SMALL_BASELINE_FILES="${SINOMO_TEST_SMALL_BASELINE_FILES:-$PERF_FILES}"
SMALL_BASELINE_BYTES="${SINOMO_TEST_SMALL_BASELINE_BYTES:-$PERF_BYTES}"
SMALL_BASELINE_SYNC_WRITES="${SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES:-0}"
SMALL_BASELINE_MIN_FILES_PER_SEC="${SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC:-0}"
LARGE_BYTES="${SINOMO_TEST_LARGE_BYTES:-67108864}"
LARGE_TIMEOUT="${SINOMO_TEST_LARGE_TIMEOUT:-180}"
LARGE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC:-4194304}"
LARGE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC:-4194304}"
LARGE_MIN_RAW_WRITE_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO:-0.33}"
LARGE_MIN_RAW_READ_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO:-0}"
LARGE_RAW_SYNC_WRITE="${SINOMO_TEST_LARGE_RAW_SYNC_WRITE:-1}"
LARGE_MAX_PROC_WCHAR_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION:-0}"
LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION:-4}"
LARGE_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES:-0}"
DISK_BASELINE_BYTES="${SINOMO_TEST_DISK_BASELINE_BYTES:-$LARGE_BYTES}"
DISK_BASELINE_SYNC_WRITE="${SINOMO_TEST_DISK_BASELINE_SYNC_WRITE:-1}"
DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC:-0}"
DISK_BASELINE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC:-0}"
DAMAGE_BYTES="${SINOMO_TEST_DAMAGE_BYTES:-65536}"
DFS_PROTOCOL_CODEC_BYTES="${SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES:-4194304}"
DFS_PROTOCOL_CODEC_ITERATIONS="${SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS:-8}"
DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC:-67108864}"
DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC:-67108864}"
NETWORK_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET:-20}"
NETWORK_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_REQUESTS_PER_TARGET}"
NETWORK_CONCURRENCY="${SINOMO_TEST_NETWORK_CONCURRENCY:-16}"
NETWORK_TIMEOUT_MS="${SINOMO_TEST_NETWORK_TIMEOUT_MS:-2000}"
NETWORK_MAX_FAILURES="${SINOMO_TEST_NETWORK_MAX_FAILURES:-0}"
NETWORK_MAX_P50_MS="${SINOMO_TEST_NETWORK_MAX_P50_MS:-50}"
NETWORK_MAX_P95_MS="${SINOMO_TEST_NETWORK_MAX_P95_MS:-250}"
NETWORK_MAX_P50_US="${SINOMO_TEST_NETWORK_MAX_P50_US:-10000}"
NETWORK_MAX_P95_US="${SINOMO_TEST_NETWORK_MAX_P95_US:-50000}"
NETWORK_MIN_RPS="${SINOMO_TEST_NETWORK_MIN_RPS:-250}"
NETWORK_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_BYTES:-0}"
NETWORK_RESPONSE_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES:-0}"
NETWORK_MIN_PAYLOAD_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC:-0}"
NETWORK_PAYLOAD_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET:-4}"
NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_PAYLOAD_REQUESTS_PER_TARGET}"
NETWORK_PAYLOAD_CONCURRENCY="${SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY:-8}"
NETWORK_PAYLOAD_TIMEOUT_MS="${SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS:-5000}"
NETWORK_PAYLOAD_REQUEST_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES:-262144}"
NETWORK_PAYLOAD_RESPONSE_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES:-262144}"
NETWORK_PAYLOAD_MIN_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC:-16777216}"
NETWORK_READY_WAIT="${SINOMO_TEST_NETWORK_READY_WAIT:-120}"
NETWORK_READY_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET:-20}"
NETWORK_READY_CONCURRENCY="${SINOMO_TEST_NETWORK_READY_CONCURRENCY:-$NETWORK_CONCURRENCY}"
NETWORK_PARTITION_SECONDS="${SINOMO_TEST_NETWORK_PARTITION_SECONDS:-5}"
NODES="node1 node2 node3 node4 node5 node6"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/$RUN_ID}"
BINARY="$SINOMO_ROOT/target/debug/sinomo"

mkdir -p "$ARTIFACTS"

log() { echo "[test-cluster] $*"; }

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

running_other_compose_projects() {
  docker ps \
    --filter label=com.docker.compose.project \
    --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null \
    | grep -v "^$PROJECT$" \
    | sort -u
}

require_no_other_compose_projects() {
  if [ "${SINOMO_TEST_ALLOW_OTHER_COMPOSE:-0}" = 1 ]; then
    return 0
  fi
  local others
  others="$(running_other_compose_projects || true)"
  if [ -z "$others" ]; then
    return 0
  fi
  log "refusing to start/reuse compose project $PROJECT while other compose projects are running:"
  echo "$others" | sed 's/^/[test-cluster]   /'
  log "stop those projects first, or set SINOMO_TEST_ALLOW_OTHER_COMPOSE=1 for an explicit override"
  return 1
}

main_online_count() {
  compose exec -T node1 sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*online" || true
}

doctor_connectivity_warnings() {
  compose exec -T node1 sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline" || true
}

cluster_health() {
  compose exec -T node1 sinomo cluster health 2>/dev/null || true
}

wait_for_cluster_ready() {
  local label="$1" started deadline now elapsed online warnings
  log "$label: waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      elapsed=$(( $(date +%s) - started ))
      log "$label: cluster ready (${online} main daemons online, ${elapsed}s)"
      return 0
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "$label: cluster NOT ready after ${FORMATION_WAIT}s (online=${online:-0})"
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

# A node is DFS-ready when its CLI can open the SIFS backend and pass the
# invariants probe: this needs the relay route to the voter (cluster KV access)
# and bootstraps the node's SIFS namespace, both of which can lag formation by
# a few seconds on the deep-segment nodes.
wait_for_dfs_ready() {
  local deadline node pending still
  log "dfs-ready: waiting for every node to open SIFS and pass invariants (<=${DFS_READY_WAIT}s)"
  deadline=$(( $(date +%s) + DFS_READY_WAIT ))
  pending="$NODES"
  while :; do
    still=""
    for node in $pending; do
      if compose exec -T "$node" sinomo test run cluster.dfs.invariants \
          --require-cluster --param timeout-seconds=5 \
          >"$ARTIFACTS/dfs-ready-$node.last.json" 2>&1; then
        log "  dfs-ready: $node"
      else
        still="$still $node"
      fi
    done
    pending="${still# }"
    [ -z "$pending" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "dfs-ready: nodes never became ready: $pending"
      for node in $pending; do
        log "  $node last probe output:"
        sed 's/^/      /' "$ARTIFACTS/dfs-ready-$node.last.json" | tail -4
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

wait_for_network_ready() {
  local deadline node still
  log "network-ready: waiting for relay/auth convergence (<=${NETWORK_READY_WAIT}s)"
  deadline=$(( $(date +%s) + NETWORK_READY_WAIT ))
  while :; do
    still=""
    for node in node1 node6; do
      if compose exec -T "$node" sinomo test run cluster.network.ping-benchmark \
          --require-cluster \
          --param "requests-per-target=$NETWORK_READY_REQUESTS_PER_TARGET" \
          --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
          --param "concurrency=$NETWORK_READY_CONCURRENCY" \
          --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
          --param "max-failures=0" \
          --param "max-p50-ms=$NETWORK_MAX_P50_MS" \
          --param "max-p95-ms=$NETWORK_MAX_P95_MS" \
          --param "max-p50-us=$NETWORK_MAX_P50_US" \
          --param "max-p95-us=$NETWORK_MAX_P95_US" \
          --param "min-requests-per-second=$NETWORK_MIN_RPS" \
          >"$ARTIFACTS/network-ready-$node.last.json" 2>&1; then
        log "  network-ready: $node"
      else
        still="$still $node"
      fi
    done
    [ -z "$still" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "network-ready: nodes never became ready:$still"
      for node in $still; do
        log "  $node last probe output:"
        sed 's/^/      /' "$ARTIFACTS/network-ready-$node.last.json" | tail -12
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

PROBE_INDEX=0
run_probe() {
  local node="$1" id="$2"
  shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  local slug probe_log
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-${id//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  if compose exec -T "$node" sinomo test run "$id" --require-cluster --json "$@" \
      >"$probe_log" 2>&1; then
    log "  ok: $node $id"
    return 0
  fi
  log "  FAILED: $node $id $*"
  sed 's/^/      /' "$probe_log"
  return 1
}

FAILED_SCENARIOS=""
scenario() {
  local name="$1"
  shift
  echo
  log "--- scenario: $name ---"
  if "$@"; then
    log "--- PASS: $name ---"
  else
    log "--- FAIL: $name ---"
    FAILED_SCENARIOS="$FAILED_SCENARIOS $name"
  fi
}

invariants_sweep() {
  local rc=0 node
  for node in $NODES; do
    run_probe "$node" cluster.dfs.invariants --param timeout-seconds=60 || rc=1
  done
  return $rc
}

dfs_protocol_codec_baseline() {
  run_probe node1 cluster.dfs.protocol-codec-baseline \
    --param "bytes=$DFS_PROTOCOL_CODEC_BYTES" \
    --param "iterations=$DFS_PROTOCOL_CODEC_ITERATIONS" \
    --param "min-encode-bytes-per-second=$DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC" \
    --param "min-decode-bytes-per-second=$DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC"
}

network_baseline() {
  local rc=0 node
  for node in node1 node6; do
    run_probe "$node" cluster.network.ping-benchmark \
      --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
      --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
      --param "concurrency=$NETWORK_CONCURRENCY" \
      --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
      --param "max-failures=$NETWORK_MAX_FAILURES" \
      --param "max-p50-ms=$NETWORK_MAX_P50_MS" \
      --param "max-p95-ms=$NETWORK_MAX_P95_MS" \
      --param "max-p50-us=$NETWORK_MAX_P50_US" \
      --param "max-p95-us=$NETWORK_MAX_P95_US" \
      --param "payload-bytes=$NETWORK_PAYLOAD_BYTES" \
      --param "response-payload-bytes=$NETWORK_RESPONSE_PAYLOAD_BYTES" \
      --param "min-payload-bytes-per-second=$NETWORK_MIN_PAYLOAD_BYTES_PER_SEC" \
      --param "min-requests-per-second=$NETWORK_MIN_RPS" || rc=1
  done
  return $rc
}

network_payload_baseline() {
  local rc=0 node
  for node in node1 node6; do
    run_probe "$node" cluster.network.ping-benchmark \
      --param "requests-per-target=$NETWORK_PAYLOAD_REQUESTS_PER_TARGET" \
      --param "warmup-requests-per-target=$NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET" \
      --param "concurrency=$NETWORK_PAYLOAD_CONCURRENCY" \
      --param "timeout-ms=$NETWORK_PAYLOAD_TIMEOUT_MS" \
      --param "max-failures=0" \
      --param "max-p50-ms=0" \
      --param "max-p95-ms=0" \
      --param "max-p50-us=0" \
      --param "max-p95-us=0" \
      --param "payload-bytes=$NETWORK_PAYLOAD_REQUEST_BYTES" \
      --param "response-payload-bytes=$NETWORK_PAYLOAD_RESPONSE_BYTES" \
      --param "min-requests-per-second=0" \
      --param "min-payload-bytes-per-second=$NETWORK_PAYLOAD_MIN_BYTES_PER_SEC" || rc=1
  done
  return $rc
}

read_your_write() {
  run_probe node1 cluster.dfs.read-your-write \
    --param "base=/services/cluster-tests/$RUN_ID" \
  && run_probe node5 cluster.dfs.read-your-write \
    --param "base=/services/cluster-tests/$RUN_ID"
}

cross_node() {
  local a="/services/cluster-tests/$RUN_ID/cross-a.txt"
  local b="/services/cluster-tests/$RUN_ID/cross-b.txt"
  local payload_a="cross-node a $RUN_ID" payload_b="cross-node b $RUN_ID"
  run_probe node5 cluster.dfs.write \
    --param "path=$a" --param "payload=$payload_a" \
  && run_probe node1 cluster.dfs.expect-file \
    --param "path=$a" --param "payload=$payload_a" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.expect-file \
    --param "path=$a" --param "payload=$payload_a" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node2 cluster.dfs.write \
    --param "path=$b" --param "payload=$payload_b" \
  && run_probe node6 cluster.dfs.expect-file \
    --param "path=$b" --param "payload=$payload_b" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node1 cluster.dfs.remove --param "path=$a" \
  && run_probe node5 cluster.dfs.expect-absent \
    --param "path=$a" --param "timeout-seconds=$EXPECT_TIMEOUT"
}

rename_across_prefixes() {
  local src="/users/cluster-tests/$RUN_ID/move-src.txt"
  local dst="/services/cluster-tests/$RUN_ID/move-dst.txt"
  local payload="rename $RUN_ID"
  run_probe node2 cluster.dfs.write \
    --param "path=$src" --param "payload=$payload" \
  && run_probe node4 cluster.dfs.expect-file \
    --param "path=$src" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node4 cluster.dfs.rename --param "from=$src" --param "to=$dst" \
  && run_probe node6 cluster.dfs.expect-file \
    --param "path=$dst" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node1 cluster.dfs.expect-absent \
    --param "path=$src" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.remove --param "path=$dst"
}

small_file_baseline() {
  local rc=0 node
  for node in node2 node5; do
    run_probe "$node" cluster.dfs.small-file-baseline \
      --param "files=$SMALL_BASELINE_FILES" \
      --param "bytes=$SMALL_BASELINE_BYTES" \
      --param "sync-writes=$SMALL_BASELINE_SYNC_WRITES" \
      --param "min-files-per-second=$SMALL_BASELINE_MIN_FILES_PER_SEC" || rc=1
  done
  return $rc
}

bulk_performance() {
  local warm="/services/cluster-tests/$RUN_ID/perf-warmup"
  local a="/services/cluster-tests/$RUN_ID/perf-a"
  local b="/services/cluster-tests/$RUN_ID/perf-b"
  run_probe node2 cluster.dfs.bulk-write \
    --param "base=$warm" \
    --param "files=$PERF_WARMUP_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
  && run_probe node6 cluster.dfs.bulk-expect \
    --param "base=$warm" \
    --param "files=$PERF_WARMUP_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node2 cluster.dfs.bulk-write \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
    --param "max-proc-wchar-amplification=$PERF_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
    --param "max-proc-syscw-per-file=$PERF_MAX_PROC_SYSCW_PER_FILE" \
  && run_probe node6 cluster.dfs.bulk-expect \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node5 cluster.dfs.bulk-write \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
    --param "max-proc-wchar-amplification=$PERF_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
    --param "max-proc-syscw-per-file=$PERF_MAX_PROC_SYSCW_PER_FILE" \
  && run_probe node1 cluster.dfs.bulk-expect \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES"
}

disk_baseline() {
  local rc=0 node
  for node in node2 node6 node5 node1; do
    run_probe "$node" cluster.dfs.disk-baseline \
      --param "bytes=$DISK_BASELINE_BYTES" \
      --param "sync-write=$DISK_BASELINE_SYNC_WRITE" \
      --param "min-write-bytes-per-second=$DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC" \
      --param "min-read-bytes-per-second=$DISK_BASELINE_MIN_READ_BYTES_PER_SEC" || rc=1
  done
  return $rc
}

large_file_performance() {
  local a="/services/cluster-tests/$RUN_ID/large-a.bin"
  local b="/services/cluster-tests/$RUN_ID/large-b.bin"
  run_probe node2 cluster.dfs.large-write \
    --param "path=$a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
    --param "min-raw-write-ratio=$LARGE_MIN_RAW_WRITE_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-wchar-amplification=$LARGE_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
  && run_probe node6 cluster.dfs.large-expect \
    --param "path=$a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "min-raw-read-ratio=$LARGE_MIN_RAW_READ_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node5 cluster.dfs.large-write \
    --param "path=$b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
    --param "min-raw-write-ratio=$LARGE_MIN_RAW_WRITE_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-wchar-amplification=$LARGE_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
  && run_probe node1 cluster.dfs.large-expect \
    --param "path=$b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "min-raw-read-ratio=$LARGE_MIN_RAW_READ_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES"
}

disk_damage_recovery() {
  local path="/services/cluster-tests/$RUN_ID/disk-damage.bin"
  local seed="disk-damage-$RUN_ID"
  run_probe node3 cluster.dfs.write \
    --param "path=$path" \
    --param "payload-seed=$seed" \
    --param "payload-bytes=$DAMAGE_BYTES" \
  && run_probe node3 cluster.dfs.expect-replicated \
    --param "path=$path" \
    --param "min-locations=2" \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.damage-local-loose-blob \
    --param "path=$path" \
  && run_probe node3 cluster.dfs.expect-file \
    --param "path=$path" \
    --param "payload-seed=$seed" \
    --param "payload-bytes=$DAMAGE_BYTES" \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
    --param "max-proc-write-bytes=0"
}

temporary_network_recovery() {
  local path="/services/cluster-tests/$RUN_ID/network-heal.txt"
  local payload="network heal $RUN_ID"
  local network="${PROJECT}_seg_b"
  local node6_container
  node6_container="$(compose ps -q node6)"
  if [ -z "$node6_container" ]; then
    log "cannot find node6 container id"
    return 1
  fi

  log "disconnecting node6 from $network for ${NETWORK_PARTITION_SECONDS}s"
  if ! docker network disconnect "$network" "$node6_container" >/dev/null 2>&1; then
    log "failed to disconnect node6 from $network"
    return 1
  fi

  local rc=0
  sleep "$NETWORK_PARTITION_SECONDS"
  run_probe node2 cluster.dfs.write \
    --param "path=$path" --param "payload=$payload" || rc=1

  log "reconnecting node6 to $network"
  if ! docker network connect --alias node6 "$network" "$node6_container" >/dev/null 2>&1; then
    log "failed to reconnect node6 to $network"
    return 1
  fi

  wait_for_cluster_ready "post-network-heal" || rc=1
  wait_for_dfs_ready || rc=1
  if [ "$rc" = 0 ]; then
    run_probe node6 cluster.dfs.expect-file \
      --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" || rc=1
  fi
  return "$rc"
}

restart_durability() {
  local path="/services/cluster-tests/$RUN_ID/durable.txt"
  local payload="durable $RUN_ID"
  run_probe node3 cluster.dfs.write \
    --param "path=$path" --param "payload=$payload" \
  && run_probe node2 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  || return 1
  log "restarting node2"
  compose restart node2 >/dev/null 2>&1 || { log "node2 restart failed"; return 1; }
  wait_for_cluster_ready "post-restart" || return 1
  wait_for_dfs_ready || return 1
  run_probe node2 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node5 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT"
}

capture_failure_context() {
  log "capturing failure context into $ARTIFACTS"
  compose ps > "$ARTIFACTS/compose-ps.txt" 2>&1 || true
  cluster_health > "$ARTIFACTS/cluster-health.txt" 2>&1 || true
  compose exec -T node1 sinomo cluster doctor > "$ARTIFACTS/cluster-doctor.txt" 2>&1 || true
  local node
  for node in $NODES; do
    compose logs --no-color "$node" > "$ARTIFACTS/$node.log" 2>&1 || true
    # The shadow supervisor redirects the main daemon's output into /data.
    compose exec -T "$node" cat /data/daemon/main.stderr.log \
      > "$ARTIFACTS/$node.daemon.stderr.log" 2>/dev/null || true
    compose exec -T "$node" cat /data/daemon/main.stdout.log \
      > "$ARTIFACTS/$node.daemon.stdout.log" 2>/dev/null || true
    # Local cluster-KV view: snapshot revision + invitation/shard keys, the
    # usual suspects when a node's view has diverged.
    compose exec -T "$node" sh -c \
      'sinomo debug sobj /data/cluster/kv-snapshot.sobj 2>/dev/null | grep -o "\"revision\": uint64([0-9]*)" | head -1' \
      > "$ARTIFACTS/$node.kv-revision.txt" 2>/dev/null || true
  done
}

TEARDOWN_DONE=0
teardown() {
  [ "$TEARDOWN_DONE" = 1 ] && return
  TEARDOWN_DONE=1
  if [ -n "$FAILED_SCENARIOS" ]; then
    capture_failure_context
  fi
  if [ "${SINOMO_TEST_KEEP:-0}" = 1 ] || [ "${SINOMO_TEST_REUSE:-0}" = 1 ]; then
    log "leaving cluster up (project $PROJECT); tear down with:"
    log "  docker compose -p $PROJECT -f $COMPOSE_FILE down -v"
    return
  fi
  log "tearing down compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  local attempt
  for attempt in 1 2 3; do
    if [ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)" ] \
      && [ -z "$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)" ]; then
      return
    fi
    sleep 2
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  done
}
trap teardown EXIT

# --- bring-up -----------------------------------------------------------------

require_no_other_compose_projects || exit 1

if [ "${SINOMO_TEST_BUILD:-0}" = 1 ] || [ ! -x "$BINARY" ]; then
  log "building host binary (cargo build -p sinomo)"
  (cd "$SINOMO_ROOT" && cargo build -p sinomo) || { log "host build failed"; exit 1; }
fi

if [ "${SINOMO_TEST_REUSE:-0}" != 1 ]; then
  log "resetting compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  log "starting 6-node cluster (project $PROJECT)"
  if ! compose up --build -d; then
    log "compose up failed"
    FAILED_SCENARIOS="bring-up"
    exit 1
  fi
fi

if ! wait_for_cluster_ready "formation"; then
  cluster_health
  FAILED_SCENARIOS="formation"
  exit 1
fi
if ! wait_for_network_ready; then
  FAILED_SCENARIOS="network-ready"
  exit 1
fi

scenario "dfs-protocol-codec-baseline" dfs_protocol_codec_baseline
scenario "network-latency-baseline" network_baseline
scenario "network-payload-baseline" network_payload_baseline
if [ -n "$FAILED_SCENARIOS" ]; then
  exit 1
fi
if [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; then
  log "RESULT: PASS — network baselines green (probe reports in $ARTIFACTS)"
  exit 0
fi

if ! wait_for_dfs_ready; then
  FAILED_SCENARIOS="dfs-ready"
  exit 1
fi

# --- scenarios ----------------------------------------------------------------

scenario "invariants-baseline"  invariants_sweep
scenario "read-your-write"      read_your_write
scenario "cross-node"           cross_node
scenario "rename-across-prefixes" rename_across_prefixes
scenario "small-file-baseline"  small_file_baseline
scenario "bulk-performance"     bulk_performance
scenario "disk-baseline"        disk_baseline
scenario "large-file-performance" large_file_performance
scenario "disk-damage-recovery" disk_damage_recovery
scenario "temporary-network-recovery" temporary_network_recovery
scenario "restart-durability"   restart_durability
scenario "invariants-final"     invariants_sweep

echo
if [ -n "$FAILED_SCENARIOS" ]; then
  log "RESULT: FAIL —$FAILED_SCENARIOS (evidence in $ARTIFACTS)"
  exit 1
fi
log "RESULT: PASS — all scenarios green (probe reports in $ARTIFACTS)"
