#!/usr/bin/env bash
# Automated DFS correctness suite on the 6-node segmented dev cluster.
#
# Brings up a DEDICATED compose project (so it never collides with a manually
# started dev cluster), waits for clean 6-node formation, then drives the
# in-binary `cluster-dfs` runtime test cases (sinomo test run --require-cluster)
# from different nodes to verify cluster-wide POSIX behavior:
#
#   1. invariants     — per-node DFS operational invariants (baseline)
#   2. read-your-write — write/read/rename/delete roundtrip on node1 and node5
#   3. cross-node     — write on one segment, poll-read on the other (both
#                       directions), then a routed delete observed cluster-wide
#   4. rename         — /users -> /services rename issued by a NON-writer node,
#                       observed (and the old path absent) on far nodes
#   5. restart        — restart a member, re-gate formation, re-read its data
#   6. final sweep    — invariants again on every node
#
# Usage:
#   docker/test-cluster.sh                  # build-if-missing, up, test, down -v
#   SINOMO_TEST_REUSE=1 docker/test-cluster.sh   # reuse the running test project
#   SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
#
# Env knobs:
#   SINOMO_TEST_PROJECT         compose project name   (default sinomo-dfs-test)
#   SINOMO_TEST_FORMATION_WAIT  formation gate seconds (default 420)
#   SINOMO_TEST_EXPECT_TIMEOUT  cross-node poll seconds (default 90)
#   SINOMO_TEST_PERF_FILES      files per bulk-performance leg (default 64)
#   SINOMO_TEST_PERF_WARMUP_FILES files in the no-threshold warmup leg (default 8)
#   SINOMO_TEST_PERF_BYTES      bytes per bulk-performance file (default 4096)
#   SINOMO_TEST_PERF_TIMEOUT    bulk write/read timeout seconds (default 120)
#   SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC  min writer throughput (default 0.75)
#   SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC   min cross-node read/convergence throughput (default 1)
#   SINOMO_TEST_BUILD=1         force `cargo build -p sinomo` first
#   SINOMO_TEST_ARTIFACTS       artifact dir (default target/cluster-test-artifacts/<run>)
#
# Exits non-zero if any scenario fails; failure evidence (probe JSON, compose ps,
# cluster health/doctor, per-node logs) lands in the artifact directory.
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"

PROJECT="${SINOMO_TEST_PROJECT:-sinomo-dfs-test}"
FORMATION_WAIT="${SINOMO_TEST_FORMATION_WAIT:-420}"
DFS_READY_WAIT="${SINOMO_TEST_DFS_READY_WAIT:-240}"
EXPECT_TIMEOUT="${SINOMO_TEST_EXPECT_TIMEOUT:-90}"
POLL_INTERVAL="${SINOMO_TEST_POLL_INTERVAL:-5}"
EXPECTED_MAINS=6
PERF_FILES="${SINOMO_TEST_PERF_FILES:-64}"
PERF_WARMUP_FILES="${SINOMO_TEST_PERF_WARMUP_FILES:-8}"
PERF_BYTES="${SINOMO_TEST_PERF_BYTES:-4096}"
PERF_TIMEOUT="${SINOMO_TEST_PERF_TIMEOUT:-120}"
PERF_MIN_WRITE_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC:-0.75}"
PERF_MIN_READ_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC:-1}"
NODES="node1 node2 node3 node4 node5 node6"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/$RUN_ID}"
BINARY="$SINOMO_ROOT/target/debug/sinomo"

mkdir -p "$ARTIFACTS"

log() { echo "[test-cluster] $*"; }

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
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
  local label="$1" deadline now online warnings
  log "$label: waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  deadline=$(( $(date +%s) + FORMATION_WAIT ))
  while :; do
    online="$(main_online_count)"
    warnings="$(doctor_connectivity_warnings)"
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ] && [ -z "$warnings" ]; then
      log "$label: cluster ready (${online} main daemons online)"
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
  && run_probe node2 cluster.dfs.bulk-write \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
  && run_probe node6 cluster.dfs.bulk-expect \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC" \
  && run_probe node5 cluster.dfs.bulk-write \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
  && run_probe node1 cluster.dfs.bulk-expect \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC"
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
}
trap teardown EXIT

# --- bring-up -----------------------------------------------------------------

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

if ! wait_for_dfs_ready; then
  FAILED_SCENARIOS="dfs-ready"
  exit 1
fi

# --- scenarios ----------------------------------------------------------------

scenario "invariants-baseline"  invariants_sweep
scenario "read-your-write"      read_your_write
scenario "cross-node"           cross_node
scenario "rename-across-prefixes" rename_across_prefixes
scenario "bulk-performance"     bulk_performance
scenario "restart-durability"   restart_durability
scenario "invariants-final"     invariants_sweep

echo
if [ -n "$FAILED_SCENARIOS" ]; then
  log "RESULT: FAIL —$FAILED_SCENARIOS (evidence in $ARTIFACTS)"
  exit 1
fi
log "RESULT: PASS — all scenarios green (probe reports in $ARTIFACTS)"
