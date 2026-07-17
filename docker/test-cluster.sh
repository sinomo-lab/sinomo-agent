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
#   4. cluster KV     — daemon-mediated quorum commit latency and throughput
#                       from a direct and a deep-segment node
#   5. invariants     — per-node DFS operational invariants (baseline)
#   6. read-your-write — write/read/rename/delete roundtrip on node1 and node5
#   7. cross-node     — write on one segment, poll-read on the other (both
#                       directions), then a routed delete observed cluster-wide
#   8. rename         — /users -> /services rename issued by a NON-writer node,
#                       observed (and the old path absent) on far nodes
#   9. small baseline — node-local raw tiny-file write baseline for DFS comparison
#   10. bulk perf     — small-file bulk write/read throughput with process I/O
#                       amplification checks and read-side write_bytes == 0
#   11. disk baseline — node-local raw file write/read baseline for DFS comparison
#   12. large-file perf — large object write throughput, cross-node convergence
#                       latency, final-read throughput, process I/O amplification
#                       checks, and read-side write_bytes == 0
#   13. disk damage   — delete one durable loose blob and read through the
#                       remaining replica without read-side disk writes
#   14. network heal  — temporarily disconnect a leaf node, write while it is
#                       isolated, reconnect it, and verify convergence
#   15. restart       — restart a member, re-gate formation, re-read its data
#   16. final sweep   — invariants again on every node, then routed status
#                       with no pending remote DFS work
#
# Usage:
#   docker/test-cluster.sh                  # build current binary, up, test, down -v
#   SINOMO_TEST_REUSE=1 docker/test-cluster.sh   # reuse the running test project
#   SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
#
# Env knobs:
#   SINOMO_TEST_PROJECT         compose project name   (default sinomo-dfs-test)
#   SINOMO_TEST_FORMATION_WAIT  formation gate seconds (default 60)
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
#   SINOMO_TEST_PERF_MAX_PROC_SYSCW_FIXED fixed writer-process write syscall budget per measured leg (default 16)
#   SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during bulk-expect (default 0)
#   SINOMO_TEST_SMALL_BASELINE_FILES files in raw tiny-file baseline leg (default same as SINOMO_TEST_PERF_FILES)
#   SINOMO_TEST_SMALL_BASELINE_BYTES bytes per raw tiny-file baseline file (default same as SINOMO_TEST_PERF_BYTES)
#   SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES sync each raw tiny-file write before timing completes (default 0)
#   SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC min raw tiny-file throughput (default 0, disabled)
#   SINOMO_TEST_LARGE_BYTES    bytes in large-file performance leg (default 268435456)
#   SINOMO_TEST_LARGE_TIMEOUT  large-file write/read timeout seconds (default 180)
#   SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC min large writer throughput (default 134217728)
#   SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC  min final large read throughput after convergence (default 4194304)
#   SINOMO_TEST_LARGE_MEASURE_RAW_WRITE_BASELINE measure raw write throughput alongside DFS writes (default 1)
#   SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO min large DFS write / raw write baseline ratio (default 0, informational)
#   SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO  min large DFS read / raw read baseline ratio (default 0, disabled)
#   SINOMO_TEST_LARGE_RAW_SYNC_WRITE      sync raw baseline writes before timing completes (default 1)
#   SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION       max large writer-process wchar/logical bytes (default 0, disabled)
#   SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION max large writer-process write_bytes/logical bytes (default 4)
#   SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during large-expect (default 0)
#   SINOMO_TEST_DISK_BASELINE_BYTES bytes in raw disk baseline leg (default same as SINOMO_TEST_LARGE_BYTES)
#   SINOMO_TEST_DISK_BASELINE_SYNC_WRITE sync raw disk baseline writes before timing completes (default 1)
#   SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC min raw write throughput (default 0, disabled)
#   SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC  min raw read throughput (default 0, disabled)
#   SINOMO_TEST_DAMAGE_BYTES    bytes in loose-blob disk damage scenario (default 49152)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES bytes per DFS protocol codec message (default 4194304)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS codec encode/decode iterations per sample (default 8)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC min codec payload encode throughput (default 67108864)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC min codec payload decode throughput (default 67108864)
#   SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET cluster network ping RPCs per remote node (default 20)
#   SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET warmup RPCs per remote node before measured pings (default same as requests)
#   SINOMO_TEST_NETWORK_CONCURRENCY         concurrent network ping workers (default 16)
#   SINOMO_TEST_NETWORK_TIMEOUT_MS          per network ping RPC timeout (default 2000)
#   SINOMO_TEST_NETWORK_MAX_FAILURES        allowed failed network ping RPCs (default 0)
#   SINOMO_TEST_NETWORK_MAX_P50_US          max aggregate network ping p50 latency (default 25000)
#   SINOMO_TEST_NETWORK_MAX_P95_US          max network ping p95 latency (default 50000)
#   SINOMO_TEST_NETWORK_MIN_RPS             min network ping throughput (default 250)
#   SINOMO_TEST_NETWORK_PAYLOAD_BYTES       request bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES response bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC min latency-baseline payload throughput (default 0, disabled)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET payload-throughput RPCs per remote node (default 4)
#   SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET warmup payload RPCs per remote node (default same as payload requests)
#   SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY payload-throughput workers (default 8)
#   SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS  payload-throughput RPC timeout (default 5000)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES request bytes per payload-throughput RPC (default 49152)
#   SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES response bytes per payload-throughput RPC (default 475136)
#   SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC min payload-throughput rate (default 16777216)
#   SINOMO_TEST_CLUSTER_KV_ITERATIONS        quorum commits per selected node (default 12)
#   SINOMO_TEST_CLUSTER_KV_VALUE_BYTES       value bytes per quorum commit (default 256)
#   SINOMO_TEST_CLUSTER_KV_TIMEOUT_MS        per-commit timeout (default 5000)
#   SINOMO_TEST_CLUSTER_KV_MAX_FAILURES      allowed failed quorum commits per node (default 0)
#   SINOMO_TEST_CLUSTER_KV_MAX_P50_MS        max quorum commit p50 latency (default 1000)
#   SINOMO_TEST_CLUSTER_KV_MAX_P95_MS        max quorum commit p95 latency (default 2000)
#   SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND min quorum commit throughput (default 1)
#   SINOMO_TEST_NETWORK_READY_WAIT          network/auth convergence gate seconds (default 120)
#   SINOMO_TEST_DFS_CONVERGENCE_WAIT        fully distributed DFS shard-layout gate seconds (default 240)
#   SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET requests per target during readiness warmup (default 20)
#   SINOMO_TEST_NETWORK_READY_CONCURRENCY   readiness warmup concurrency (default 16)
#   SINOMO_TEST_READINESS_COMMAND_TIMEOUT   host/container cap for each readiness command (default 10)
#   SINOMO_TEST_PROBE_COMMAND_TIMEOUT       host/container cap for each test probe (default 300)
#   SINOMO_TEST_ARTIFACT_COMMAND_TIMEOUT    cap for each failure artifact command (default 10)
#   SINOMO_TEST_HOST_TIMEOUT_GRACE          host cleanup grace beyond the container timeout (default 5)
#   SINOMO_TEST_NETWORK_PARTITION_SECONDS   seconds to keep node6 disconnected in network-heal leg (default 5)
#   SINOMO_TEST_NETWORK_ONLY=1              stop after network/KV baselines and DFS readiness/mutation/convergence gates
#   SINOMO_TEST_BUILD=0         reuse an existing host binary instead of rebuilding
#   SINOMO_TEST_BINARY          host binary to validate/mount (default target/debug/sinomo)
#   SINOMO_TEST_ARTIFACTS       artifact dir (default target/cluster-test-artifacts/<run>)
#   SINOMO_TEST_ALLOW_OTHER_COMPOSE=1       allow running while unrelated compose projects are up
#   SINOMO_TEST_COMPOSE_OVERRIDE optional second compose file (for measurement-only overrides)
#
# Exits non-zero if any scenario fails; failure evidence (probe JSON, compose ps,
# cluster health/doctor, per-node logs) lands in the artifact directory.
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
COMPOSE_OVERRIDE_FILE="${SINOMO_TEST_COMPOSE_OVERRIDE:-}"

PROJECT="${SINOMO_TEST_PROJECT:-sinomo-dfs-test}"
FORMATION_WAIT="${SINOMO_TEST_FORMATION_WAIT:-60}"
DFS_READY_WAIT="${SINOMO_TEST_DFS_READY_WAIT:-240}"
DFS_CONVERGENCE_WAIT="${SINOMO_TEST_DFS_CONVERGENCE_WAIT:-240}"
EXPECT_TIMEOUT="${SINOMO_TEST_EXPECT_TIMEOUT:-90}"
ROUTED_STATUS_MAX_ATTEMPTS="${SINOMO_TEST_ROUTED_STATUS_MAX_ATTEMPTS:-12}"
ROUTED_STATUS_MAX_ELAPSED_MS="${SINOMO_TEST_ROUTED_STATUS_MAX_ELAPSED_MS:-$((EXPECT_TIMEOUT * 1000))}"
POLL_INTERVAL="${SINOMO_TEST_POLL_INTERVAL:-1}"
READINESS_COMMAND_TIMEOUT="${SINOMO_TEST_READINESS_COMMAND_TIMEOUT:-10}"
PROBE_COMMAND_TIMEOUT="${SINOMO_TEST_PROBE_COMMAND_TIMEOUT:-300}"
ARTIFACT_COMMAND_TIMEOUT="${SINOMO_TEST_ARTIFACT_COMMAND_TIMEOUT:-10}"
HOST_TIMEOUT_GRACE="${SINOMO_TEST_HOST_TIMEOUT_GRACE:-5}"
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
PERF_MAX_PROC_SYSCW_FIXED="${SINOMO_TEST_PERF_MAX_PROC_SYSCW_FIXED:-16}"
PERF_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES:-0}"
SMALL_BASELINE_FILES="${SINOMO_TEST_SMALL_BASELINE_FILES:-$PERF_FILES}"
SMALL_BASELINE_BYTES="${SINOMO_TEST_SMALL_BASELINE_BYTES:-$PERF_BYTES}"
SMALL_BASELINE_SYNC_WRITES="${SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES:-0}"
SMALL_BASELINE_MIN_FILES_PER_SEC="${SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC:-0}"
LARGE_BYTES="${SINOMO_TEST_LARGE_BYTES:-268435456}"
LARGE_TIMEOUT="${SINOMO_TEST_LARGE_TIMEOUT:-180}"
LARGE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC:-134217728}"
LARGE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC:-4194304}"
LARGE_MEASURE_RAW_WRITE_BASELINE="${SINOMO_TEST_LARGE_MEASURE_RAW_WRITE_BASELINE:-1}"
LARGE_MIN_RAW_WRITE_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO:-0}"
LARGE_MIN_RAW_READ_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO:-0}"
LARGE_RAW_SYNC_WRITE="${SINOMO_TEST_LARGE_RAW_SYNC_WRITE:-1}"
LARGE_MAX_PROC_WCHAR_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION:-0}"
LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION:-4}"
LARGE_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES:-0}"
DISK_BASELINE_BYTES="${SINOMO_TEST_DISK_BASELINE_BYTES:-$LARGE_BYTES}"
DISK_BASELINE_SYNC_WRITE="${SINOMO_TEST_DISK_BASELINE_SYNC_WRITE:-1}"
DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC:-0}"
DISK_BASELINE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC:-0}"
# Use a substantial fixture so corruption repair exercises the same RPC
# interface used by ordinary large values.
DAMAGE_BYTES="${SINOMO_TEST_DAMAGE_BYTES:-1048576}"
DFS_PROTOCOL_CODEC_BYTES="${SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES:-4194304}"
DFS_PROTOCOL_CODEC_ITERATIONS="${SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS:-8}"
DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC:-67108864}"
DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC:-67108864}"
NETWORK_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET:-20}"
NETWORK_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_REQUESTS_PER_TARGET}"
NETWORK_CONCURRENCY="${SINOMO_TEST_NETWORK_CONCURRENCY:-16}"
NETWORK_TIMEOUT_MS="${SINOMO_TEST_NETWORK_TIMEOUT_MS:-2000}"
NETWORK_MAX_FAILURES="${SINOMO_TEST_NETWORK_MAX_FAILURES:-0}"
NETWORK_MAX_P50_US="${SINOMO_TEST_NETWORK_MAX_P50_US:-25000}"
NETWORK_MAX_P95_US="${SINOMO_TEST_NETWORK_MAX_P95_US:-50000}"
NETWORK_MIN_RPS="${SINOMO_TEST_NETWORK_MIN_RPS:-250}"
NETWORK_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_BYTES:-0}"
NETWORK_RESPONSE_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES:-0}"
NETWORK_MIN_PAYLOAD_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC:-0}"
NETWORK_PAYLOAD_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET:-4}"
NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_PAYLOAD_REQUESTS_PER_TARGET}"
NETWORK_PAYLOAD_CONCURRENCY="${SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY:-8}"
NETWORK_PAYLOAD_TIMEOUT_MS="${SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS:-5000}"
# Exercise equal-size large values in both directions through the ordinary RPC
# interface; the fixture sizes are workload choices, not protocol boundaries.
NETWORK_PAYLOAD_REQUEST_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES:-262144}"
NETWORK_PAYLOAD_RESPONSE_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES:-262144}"
NETWORK_PAYLOAD_MIN_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC:-16777216}"
CLUSTER_KV_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_ITERATIONS:-12}"
CLUSTER_KV_VALUE_BYTES="${SINOMO_TEST_CLUSTER_KV_VALUE_BYTES:-256}"
CLUSTER_KV_TIMEOUT_MS="${SINOMO_TEST_CLUSTER_KV_TIMEOUT_MS:-5000}"
CLUSTER_KV_MAX_FAILURES="${SINOMO_TEST_CLUSTER_KV_MAX_FAILURES:-0}"
CLUSTER_KV_MAX_P50_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P50_MS:-1000}"
CLUSTER_KV_MAX_P95_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P95_MS:-2000}"
CLUSTER_KV_MIN_COMMITS_PER_SECOND="${SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND:-1}"
NETWORK_READY_WAIT="${SINOMO_TEST_NETWORK_READY_WAIT:-120}"
NETWORK_READY_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET:-20}"
NETWORK_READY_CONCURRENCY="${SINOMO_TEST_NETWORK_READY_CONCURRENCY:-$NETWORK_CONCURRENCY}"
NETWORK_PARTITION_SECONDS="${SINOMO_TEST_NETWORK_PARTITION_SECONDS:-5}"
NODES="node1 node2 node3 node4 node5 node6"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/$RUN_ID}"
BINARY="${SINOMO_TEST_BINARY:-$SINOMO_ROOT/target/debug/sinomo}"
export SINOMO_DOCKER_BINARY="${SINOMO_DOCKER_BINARY:-$BINARY}"
COMPOSE_FILES=(-f "$COMPOSE_FILE")
if [ -n "$COMPOSE_OVERRIDE_FILE" ]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE_FILE")
fi

mkdir -p "$ARTIFACTS"

log() { echo "[test-cluster] $*"; }

compose() {
  docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"
}

compose_timeout() {
  local seconds="$1"
  shift
  timeout --foreground "${seconds}s" \
    docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"
}

compose_exec_timeout() {
  local seconds="$1" node="$2" host_seconds
  shift 2
  host_seconds=$(( seconds + HOST_TIMEOUT_GRACE ))
  timeout --foreground "${host_seconds}s" \
    docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" \
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

doctor_connectivity_warnings() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}" output_file="${2:-}" output
  if ! output="$(compose_exec_timeout "$seconds" node1 sinomo cluster doctor 2>&1)"; then
    if [ -n "$output_file" ]; then
      printf '%s\n' "$output" > "$output_file"
    fi
    return 1
  fi
  if [ -n "$output_file" ]; then
    printf '%s\n' "$output" > "$output_file"
  fi
  printf '%s\n' "$output" | grep -E "endpoint-unreachable|node-offline" || true
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
  ' 2>/dev/null || true
}

report_cluster_readiness_timeout() {
  local label="$1" elapsed="$2" best_online="$3" last_online="$4"
  local health_file="$5" doctor_file="$6" health_header unresolved
  log "$label: cluster NOT ready after ${elapsed}s (best-online=$best_online, last-online=$last_online)"
  if [ -s "$health_file" ] && grep -q '[^[:space:]]' "$health_file"; then
    unresolved="$(awk '
      $0 ~ /daemon-main/ && $0 !~ /daemon-main.*online/ { print }
    ' "$health_file")"
    if [ -n "$unresolved" ]; then
      health_header="$(sed -n '1p' "$health_file")"
      log "$label: last unresolved daemon rows:"
      printf '%s\n%s\n' "$health_header" "$unresolved" | sed 's/^/[test-cluster]   /'
    fi
  else
    log "$label: no cluster health output was captured"
  fi
  log "$label: last health output: $health_file"
  log "$label: last doctor output: $doctor_file"
}

wait_for_cluster_ready() {
  local label="$1" started deadline now elapsed health online warnings command_timeout markers
  local label_slug health_file doctor_file best_online=0 last_online=0
  label_slug="${label//[^a-zA-Z0-9._-]/_}"
  health_file="$ARTIFACTS/cluster-ready-$label_slug.last-health.txt"
  doctor_file="$ARTIFACTS/cluster-ready-$label_slug.last-doctor.txt"
  : > "$health_file"
  printf '%s\n' "doctor not run: health has not reported $EXPECTED_MAINS online main daemons" \
    > "$doctor_file"
  log "$label: waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  started="$(date +%s)"
  deadline=$(( started + FORMATION_WAIT ))
  while :; do
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
      now="$(date +%s)"
      elapsed=$(( now - started ))
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" "$health_file" "$doctor_file"
      return 1
    fi
    health="$(cluster_health "$command_timeout")"
    printf '%s\n' "$health" > "$health_file"
    online="$(printf '%s\n' "$health" | grep -c "daemon-main.*online" || true)"
    last_online="${online:-0}"
    if [ "${online:-0}" -gt "$best_online" ]; then
      best_online="$online"
    fi
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
      now="$(date +%s)"
      elapsed=$(( now - started ))
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" "$health_file" "$doctor_file"
      return 1
    fi
    markers="$(formation_failure_markers "$command_timeout")"
    if [ -n "$markers" ]; then
      log "$label: cluster formation reported a terminal failure"
      printf '%s\n' "$markers" | sed 's/^/[test-cluster]   /'
      return 1
    fi
    warnings=""
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ]; then
      # Doctor performs another live health aggregation. Keep it off the hot
      # formation polling path and run it only once all expected mains appear.
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")"; then
        if ! warnings="$(doctor_connectivity_warnings "$command_timeout" "$doctor_file")"; then
          warnings="cluster doctor failed or timed out"
        fi
      else
        warnings="readiness deadline exhausted before doctor completed"
      fi
    fi
    now="$(date +%s)"
    elapsed=$(( now - started ))
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      if [ "$now" -le "$deadline" ]; then
        log "$label: cluster ready (${online} main daemons online, ${elapsed}s)"
        return 0
      fi
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" "$health_file" "$doctor_file"
      return 1
    fi
    if [ "$now" -ge "$deadline" ]; then
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" "$health_file" "$doctor_file"
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
  local deadline node pending still probe_log probe_err command_timeout now
  log "dfs-ready: waiting for every node to open SIFS and pass invariants (<=${DFS_READY_WAIT}s)"
  deadline=$(( $(date +%s) + DFS_READY_WAIT ))
  pending="$NODES"
  while :; do
    still=""
    for node in $pending; do
      probe_log="$ARTIFACTS/dfs-ready-$node.last.json"
      probe_err="$ARTIFACTS/dfs-ready-$node.last.stderr.log"
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
        && compose_exec_timeout "$command_timeout" "$node" sinomo test run cluster.dfs.invariants \
          --require-cluster --json --param timeout-seconds=5 \
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
      log "dfs-ready: nodes never became ready: $pending"
      for node in $pending; do
        log "  $node last probe output:"
        sed 's/^/      /' "$ARTIFACTS/dfs-ready-$node.last.json" | tail -4
        sed 's/^/      /' "$ARTIFACTS/dfs-ready-$node.last.stderr.log" | tail -4
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

dfs_converged_epoch_signature() {
  jq -er '
    if .status != "passed" then
      error("shard-layout probe did not pass")
    else
      .diagnostics.shards
    end
    | if type != "array" or length != 3 then
        error("expected exactly three shard diagnostics")
      elif ([.[].lane] | sort) != ["root", "services", "users"] then
        error("expected root, services, and users lanes")
      elif any(.[];
        ((.replica_node_ids | type) != "array")
          or ((.replica_node_ids | length) != 5)
      ) then
        error("expected five replicas on every shard")
      elif ([.[].primary_node_id] | unique | length) != 3 then
        error("expected three distinct shard primaries")
      elif any(.[];
        ((.file_system_id | type) != "string")
          or ((.shard_id | type) != "string")
          or ((.epoch | type) != "number")
      ) then
        error("invalid shard identity or epoch diagnostic")
      else
        sort_by(.shard_id)
        | map("\(.file_system_id)/\(.shard_id)=\(.epoch)")
        | join(",")
      end
  ' "$1"
}

# A clean operational snapshot is stronger than an active shard layout. The
# retiring layout remains active while a membership handoff is pending, and a
# completed control-plane handoff can leave the active primary's freshly opened
# backend lane fenced. Require exact layout, no current pending handoff keys,
# zero active-primary freezes, and zero configured-replica lag on every local
# primary. The separate routed-mutation gate below checks the live daemon lanes.
# Bracket the invariant sweep with layout reads and require two unchanged epoch
# signatures so a transition completing during either observation cannot pass.
wait_for_dfs_converged() {
  local deadline probe_log probe_err confirm_log confirm_err pending_log pending_err
  local command_timeout now consecutive node invariants_clean invariant_log invariant_err
  local epoch_signature confirm_epoch_signature previous_epoch_signature observation_clean
  if ! command -v jq >/dev/null 2>&1; then
    log "dfs-converged: jq is required to validate stable shard epochs"
    return 1
  fi
  log "dfs-converged: waiting for the fully distributed three-lane shard layout (<=${DFS_CONVERGENCE_WAIT}s)"
  deadline=$(( $(date +%s) + DFS_CONVERGENCE_WAIT ))
  probe_log="$ARTIFACTS/dfs-converged.last.json"
  probe_err="$ARTIFACTS/dfs-converged.last.stderr.log"
  confirm_log="$ARTIFACTS/dfs-converged.confirm.last.json"
  confirm_err="$ARTIFACTS/dfs-converged.confirm.last.stderr.log"
  pending_log="$ARTIFACTS/dfs-converged.pending-handoffs.last.txt"
  pending_err="$ARTIFACTS/dfs-converged.pending-handoffs.last.stderr.log"
  consecutive=0
  previous_epoch_signature=""
  while :; do
    observation_clean=0
    if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
      && compose_exec_timeout "$command_timeout" node1 sinomo test run cluster.dfs.shard-layout \
        --require-cluster \
        --json \
        --param expected-replica-count=5 \
        --param expected-distinct-primary-count=3 \
        >"$probe_log" 2>"$probe_err" \
      && epoch_signature="$(dfs_converged_epoch_signature "$probe_log" 2>>"$probe_err")"; then
      invariants_clean=1
      for node in $NODES; do
        invariant_log="$ARTIFACTS/dfs-converged-$node.last.json"
        invariant_err="$ARTIFACTS/dfs-converged-$node.last.stderr.log"
        if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
          || ! compose_exec_timeout "$command_timeout" "$node" \
            sinomo test run cluster.dfs.invariants \
              --require-cluster --json --param timeout-seconds=1 \
              >"$invariant_log" 2>"$invariant_err" \
          || ! jq -e '
            .status == "passed"
              and .diagnostics.authoritative_write_frozen_shard_count == 0
          ' "$invariant_log" >/dev/null 2>>"$invariant_err"; then
          invariants_clean=0
          break
        fi
      done
      if [ "$invariants_clean" = 1 ]; then
        if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
          && compose_exec_timeout "$command_timeout" node1 sinomo test run cluster.dfs.shard-layout \
            --require-cluster \
            --json \
            --param expected-replica-count=5 \
            --param expected-distinct-primary-count=3 \
            >"$confirm_log" 2>"$confirm_err" \
          && confirm_epoch_signature="$(dfs_converged_epoch_signature "$confirm_log" 2>>"$confirm_err")" \
          && [ "$confirm_epoch_signature" = "$epoch_signature" ] \
          && command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
          && compose_exec_timeout "$command_timeout" node1 sh -c '
            snapshot="$(sinomo debug sobj /data/cluster/kv-snapshot.sobj)" || exit 2
            # Completed handoff keys remain in bounded event history. Inspect
            # only the current-entry prefix of the decoded snapshot.
            entries="${snapshot%%, \"events\":*}"
            revision="$(printf "%s\n" "$entries" \
              | grep -o "\"revision\": uint64([0-9]*)" | head -1)"
            [ -n "$revision" ] || exit 3
            pending="$(printf "%s\n" "$entries" \
              | grep -o "\"key\": \"/dfs/filesystems/[^\"]*/metadata-membership-handoffs/[^\"]*\"" \
              || true)"
            printf "%s\n" "$revision"
            if [ -n "$pending" ]; then
              printf "%s\n" "$pending"
              exit 1
            fi
          ' >"$pending_log" 2>"$pending_err"; then
          observation_clean=1
        fi
      fi
    fi
    if [ "$observation_clean" = 1 ]; then
      if [ -n "$previous_epoch_signature" ] \
        && [ "$epoch_signature" = "$previous_epoch_signature" ]; then
        consecutive=$((consecutive + 1))
      else
        consecutive=1
      fi
      previous_epoch_signature="$epoch_signature"
    else
      consecutive=0
      previous_epoch_signature=""
    fi
    if [ "$consecutive" -ge 2 ]; then
      log "  dfs-converged: exact layout, zero handoffs, unfrozen active primaries, and zero replica lag across two stable epoch probes"
      return 0
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "dfs-converged: shard layout did not converge before the deadline"
      sed 's/^/      /' "$probe_log" | tail -12
      sed 's/^/      /' "$probe_err" | tail -12
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

wait_for_network_ready() {
  local deadline node pending still probe_log probe_err command_timeout now
  log "network-ready: waiting for relay/auth convergence (<=${NETWORK_READY_WAIT}s)"
  deadline=$(( $(date +%s) + NETWORK_READY_WAIT ))
  pending="$NODES"
  while :; do
    still=""
    for node in $pending; do
      probe_log="$ARTIFACTS/network-ready-$node.last.json"
      probe_err="$ARTIFACTS/network-ready-$node.last.stderr.log"
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
        && compose_exec_timeout "$command_timeout" "$node" sinomo test run cluster.network.ping-benchmark \
          --require-cluster \
          --json \
          --param "requests-per-target=$NETWORK_READY_REQUESTS_PER_TARGET" \
          --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
          --param "concurrency=$NETWORK_READY_CONCURRENCY" \
          --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
          --param "max-failures=0" \
          --param "max-p50-ms=0" \
          --param "max-p95-ms=0" \
          --param "max-p50-us=$NETWORK_MAX_P50_US" \
          --param "max-p95-us=$NETWORK_MAX_P95_US" \
          --param "min-requests-per-second=$NETWORK_MIN_RPS" \
          >"$probe_log" 2>"$probe_err"; then
        now="$(date +%s)"
        if [ "$now" -le "$deadline" ]; then
          log "  network-ready: $node"
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
      log "network-ready: nodes never became ready: $pending"
      for node in $pending; do
        log "  $node last probe output:"
        sed 's/^/      /' "$ARTIFACTS/network-ready-$node.last.json" | tail -12
        sed 's/^/      /' "$ARTIFACTS/network-ready-$node.last.stderr.log" | tail -4
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
  local slug probe_log probe_err
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-${id//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  probe_err="$ARTIFACTS/probe-$slug.stderr.log"
  if compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" "$node" \
      sinomo test run "$id" --require-cluster --json "$@" \
      >"$probe_log" 2>"$probe_err"; then
    log "  ok: $node $id"
    return 0
  fi
  log "  FAILED: $node $id $*"
  sed 's/^/      /' "$probe_log"
  sed 's/^/      /' "$probe_err"
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

# Exercise one routed mutation on every metadata authority through the live
# daemon message service. Fresh CLI invariant probes can rebind their own
# backend handles and therefore cannot prove that a long-lived daemon lane is
# writable. Requiring zero internal retries makes a stale cached lane visible
# instead of hiding it behind the probe's bounded retry loop.
dfs_daemon_mutation_lanes_ready() {
  local lane path payload rc=0
  for lane in root services users; do
    case "$lane" in
      root) path="/cluster-tests/$RUN_ID/daemon-lane-root.txt" ;;
      services) path="/services/cluster-tests/$RUN_ID/daemon-lane-services.txt" ;;
      users) path="/users/cluster-tests/$RUN_ID/daemon-lane-users.txt" ;;
    esac
    payload="daemon mutation lane $lane $RUN_ID"
    run_probe node6 cluster.dfs.write \
      --param "path=$path" \
      --param "payload=$payload" \
      --param max-retries=0 \
      --param "timeout-seconds=$EXPECT_TIMEOUT" \
    && run_probe node5 cluster.dfs.expect-file \
      --param "path=$path" \
      --param "payload=$payload" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" \
    && run_probe node4 cluster.dfs.remove --param "path=$path" \
    || rc=1
  done
  return "$rc"
}

final_routed_status() {
  run_probe node1 cluster.dfs.routed-status \
    --param expected-targets=5 \
    --param max-remote-open-conflicts=0 \
    --param max-remote-pending-writes=0 \
    --param max-remote-pending-repairs=0 \
    --param "max-attempts=$ROUTED_STATUS_MAX_ATTEMPTS" \
    --param "max-elapsed-ms=$ROUTED_STATUS_MAX_ELAPSED_MS" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
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
  for node in $NODES; do
    run_probe "$node" cluster.network.ping-benchmark \
      --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
      --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
      --param "concurrency=$NETWORK_CONCURRENCY" \
      --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
      --param "max-failures=$NETWORK_MAX_FAILURES" \
      --param "max-p50-ms=0" \
      --param "max-p95-ms=0" \
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
  for node in $NODES; do
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

cluster_kv_quorum_baseline() {
  local rc=0 node
  # node1 exercises the usual local-authority path; node5 exercises forwarding
  # across the deeper segmented route in the development topology.
  for node in node1 node5; do
    run_probe "$node" cluster.kv.quorum-commit-benchmark \
      --param "iterations=$CLUSTER_KV_ITERATIONS" \
      --param "value-bytes=$CLUSTER_KV_VALUE_BYTES" \
      --param "timeout-ms=$CLUSTER_KV_TIMEOUT_MS" \
      --param "max-failures=$CLUSTER_KV_MAX_FAILURES" \
      --param "max-p50-ms=$CLUSTER_KV_MAX_P50_MS" \
      --param "max-p95-ms=$CLUSTER_KV_MAX_P95_MS" \
      --param "min-commits-per-second=$CLUSTER_KV_MIN_COMMITS_PER_SECOND" || rc=1
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

capture_rename_failure_context() {
  local src="$1" dst="$2" node
  log "capturing rename failure context into $ARTIFACTS"
  compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" node1 \
    sinomo dfs status --cluster --json \
    > "$ARTIFACTS/rename-failure-cluster-dfs-status.json" \
    2> "$ARTIFACTS/rename-failure-cluster-dfs-status.stderr.log" || true
  for node in $NODES; do
    compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" \
      sinomo dfs path "$src" --json \
      > "$ARTIFACTS/rename-failure-$node-source.json" \
      2> "$ARTIFACTS/rename-failure-$node-source.stderr.log" || true
    compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" \
      sinomo dfs path "$dst" --json \
      > "$ARTIFACTS/rename-failure-$node-destination.json" \
      2> "$ARTIFACTS/rename-failure-$node-destination.stderr.log" || true
  done
}

rename_across_prefixes() {
  local src="/users/cluster-tests/$RUN_ID/move-src.txt"
  local dst="/services/cluster-tests/$RUN_ID/move-dst.txt"
  local payload="rename $RUN_ID"
  if run_probe node2 cluster.dfs.write \
    --param "path=$src" --param "payload=$payload" \
  && run_probe node4 cluster.dfs.expect-file \
    --param "path=$src" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node4 cluster.dfs.rename --param "from=$src" --param "to=$dst" \
  && run_probe node6 cluster.dfs.expect-file \
    --param "path=$dst" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node1 cluster.dfs.expect-absent \
    --param "path=$src" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.remove --param "path=$dst"; then
    return 0
  fi
  capture_rename_failure_context "$src" "$dst"
  return 1
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
    --param "max-proc-syscw-fixed=$PERF_MAX_PROC_SYSCW_FIXED" \
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
    --param "max-proc-syscw-fixed=$PERF_MAX_PROC_SYSCW_FIXED" \
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
    --param "measure-raw-write-baseline=$LARGE_MEASURE_RAW_WRITE_BASELINE" \
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
  && run_probe node2 cluster.dfs.expect-replicated \
    --param "path=$a" \
    --param min-locations=2 \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node5 cluster.dfs.large-write \
    --param "path=$b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
    --param "measure-raw-write-baseline=$LARGE_MEASURE_RAW_WRITE_BASELINE" \
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
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node5 cluster.dfs.expect-replicated \
    --param "path=$b" \
    --param min-locations=2 \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

disk_damage_recovery() {
  local path="/services/cluster-tests/$RUN_ID/disk-damage.bin"
  local seed="disk-damage-$RUN_ID"
  run_probe node3 cluster.dfs.write \
    --param "path=$path" \
    --param "payload-seed=$seed" \
    --param "payload-bytes=$DAMAGE_BYTES" \
    --param "foreground-repair=true" \
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
    --param "max-proc-write-bytes=0" \
  && run_probe node4 cluster.dfs.remove \
    --param "path=$path" \
  && run_probe node3 cluster.dfs.expect-absent \
    --param "path=$path" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
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
  wait_for_dfs_converged || rc=1
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
  wait_for_dfs_converged || return 1
  run_probe node2 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node5 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT"
}

capture_failure_context() {
  log "capturing failure context into $ARTIFACTS"
  compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" ps > "$ARTIFACTS/compose-ps.txt" 2>&1 || true
  cluster_health > "$ARTIFACTS/cluster-health.txt" 2>&1 || true
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" node1 sinomo cluster doctor \
    > "$ARTIFACTS/cluster-doctor.txt" 2>&1 || true
  local node
  for node in $NODES; do
    compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" logs --no-color "$node" \
      > "$ARTIFACTS/$node.log" 2>&1 || true
    # The shadow supervisor redirects the main daemon's output into /data.
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" cat /data/daemon/main.stderr.log \
      > "$ARTIFACTS/$node.daemon.stderr.log" 2>/dev/null || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" cat /data/daemon/main.stdout.log \
      > "$ARTIFACTS/$node.daemon.stdout.log" 2>/dev/null || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo cluster connectivity \
      > "$ARTIFACTS/$node.cluster-connectivity.txt" 2>&1 || true
    # Local cluster-KV view: snapshot revision + invitation/shard keys, the
    # usual suspects when a node's view has diverged.
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sh -c \
      'sinomo debug sobj /data/cluster/kv-snapshot.sobj 2>/dev/null | grep -o "\"revision\": uint64([0-9]*)" | head -1' \
      > "$ARTIFACTS/$node.kv-revision.txt" 2>/dev/null || true
  done
}

TEARDOWN_DONE=0
PROJECT_OWNED=0
teardown() {
  [ "$TEARDOWN_DONE" = 1 ] && return
  TEARDOWN_DONE=1
  [ "$PROJECT_OWNED" = 1 ] || return
  if [ -n "$FAILED_SCENARIOS" ]; then
    capture_failure_context
  fi
  if [ "${SINOMO_TEST_KEEP:-0}" = 1 ] || [ "${SINOMO_TEST_REUSE:-0}" = 1 ]; then
    log "leaving cluster up (project $PROJECT); tear down with:"
    if [ -n "$COMPOSE_OVERRIDE_FILE" ]; then
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE -f $COMPOSE_OVERRIDE_FILE down -v"
    else
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE down -v"
    fi
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

# --- bring-up -----------------------------------------------------------------

require_no_other_compose_projects || exit 1
trap teardown EXIT

if [ "${SINOMO_TEST_BUILD:-1}" = 1 ] || [ ! -x "$BINARY" ]; then
  if [ "$BINARY" != "$SINOMO_ROOT/target/debug/sinomo" ]; then
    log "custom host binary is missing or SINOMO_TEST_BUILD requested: $BINARY"
    exit 1
  fi
  log "building host binary (cargo build -p sinomo)"
  (cd "$SINOMO_ROOT" && cargo build -p sinomo) || { log "host build failed"; exit 1; }
fi

if [ "${SINOMO_TEST_REUSE:-0}" != 1 ]; then
  PROJECT_OWNED=1
  log "resetting compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  log "starting 6-node cluster (project $PROJECT)"
  if ! compose up --build -d; then
    log "compose up failed"
    FAILED_SCENARIOS="bring-up"
    exit 1
  fi
else
  PROJECT_OWNED=1
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
scenario "cluster-kv-quorum-baseline" cluster_kv_quorum_baseline
if [ -n "$FAILED_SCENARIOS" ]; then
  exit 1
fi
if ! wait_for_dfs_ready; then
  FAILED_SCENARIOS="dfs-ready"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="dfs-converged"
  exit 1
fi
if ! dfs_daemon_mutation_lanes_ready; then
  FAILED_SCENARIOS="dfs-daemon-mutation-lanes"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="dfs-post-daemon-mutation-convergence"
  exit 1
fi
if [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; then
  log "RESULT: PASS — network/KV baselines and DFS readiness/mutation/convergence gates green (probe reports in $ARTIFACTS)"
  exit 0
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
scenario "routed-status-final"  final_routed_status

echo
if [ -n "$FAILED_SCENARIOS" ]; then
  log "RESULT: FAIL —$FAILED_SCENARIOS (evidence in $ARTIFACTS)"
  exit 1
fi
log "RESULT: PASS — all scenarios green (probe reports in $ARTIFACTS)"
