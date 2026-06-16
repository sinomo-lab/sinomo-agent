#!/usr/bin/env bash
# End-to-end Docker test for single-node cluster join migration.
#
# This uses a dedicated two-node compose project. The target node becomes a live
# cluster introducer. The source node first initializes its own single-node
# cluster, seeds source KV/DFS migration fixtures, then runs `sinomo cluster
# join` against the target invite. The final probe verifies target KV authority,
# node-local config import, source KV quarantine, and DFS metadata quarantine.
#
# Usage:
#   docker/test-cluster-migration.sh
#   SINOMO_MIGRATION_TEST_KEEP=1 docker/test-cluster-migration.sh
#
# Env knobs:
#   SINOMO_MIGRATION_TEST_PROJECT      compose project name (default sinomo-migration-test)
#   SINOMO_MIGRATION_TEST_BUILD        force cargo build -p sinomo first
#   SINOMO_MIGRATION_TEST_KEEP         leave compose project running
#   SINOMO_MIGRATION_TEST_ARTIFACTS    artifact dir
#   SINOMO_MIGRATION_TEST_WAIT         daemon/health wait seconds (default 120)
#   SINOMO_MIGRATION_TEST_ALLOW_OTHER_COMPOSE=1 allow unrelated compose projects
set -euo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.migration.yml}"
PROJECT="${SINOMO_MIGRATION_TEST_PROJECT:-sinomo-migration-test}"
WAIT_SECONDS="${SINOMO_MIGRATION_TEST_WAIT:-120}"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_MIGRATION_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/migration-$RUN_ID}"
BINARY="$SINOMO_ROOT/target/debug/sinomo"
FAILED=0
TEARDOWN_DONE=0

mkdir -p "$ARTIFACTS"

log() {
  echo "[test-cluster-migration] $*"
}

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

exec_node() {
  local node="$1"
  shift
  compose exec -T \
    -e SINOMO_DATA_DIR=/data \
    -e SINOMO_DISABLE_IROH=1 \
    -e SINOMO_ENABLE_DEFAULT_DISTRIBUTED_DFS=1 \
    -e SINOMO_DAEMON_ADVERTISE_HOST="$node" \
    "$node" "$@"
}

exec_node_sh() {
  local node="$1"
  shift
  exec_node "$node" sh -lc "$*"
}

running_other_compose_projects() {
  docker ps \
    --filter label=com.docker.compose.project \
    --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null \
    | grep -v "^$PROJECT$" \
    | sort -u
}

require_no_other_compose_projects() {
  if [ "${SINOMO_MIGRATION_TEST_ALLOW_OTHER_COMPOSE:-0}" = 1 ]; then
    return 0
  fi
  local others
  others="$(running_other_compose_projects || true)"
  if [ -z "$others" ]; then
    return 0
  fi
  log "refusing to start/reuse compose project $PROJECT while other compose projects are running:"
  echo "$others" | sed 's/^/[test-cluster-migration]   /'
  log "stop those projects first, or set SINOMO_MIGRATION_TEST_ALLOW_OTHER_COMPOSE=1"
  return 1
}

start_shadow_supervisor() {
  local node="$1"
  log "$node: starting shadow supervisor"
  exec_node_sh "$node" '
    mkdir -p /data/daemon /data/shadow-daemon
    if [ -f /data/shadow-daemon/manual.pid ] && kill -0 "$(cat /data/shadow-daemon/manual.pid)" 2>/dev/null; then
      exit 0
    fi
    nohup env \
      SINOMO_DATA_DIR=/data \
      SINOMO_DISABLE_IROH=1 \
      SINOMO_ENABLE_DEFAULT_DISTRIBUTED_DFS=1 \
      SINOMO_SHADOW_MAIN_BINARY=/usr/local/bin/sinomo \
      SINOMO_SHADOW_MAIN_HOST=0.0.0.0 \
      SINOMO_SHADOW_MAIN_PORT=21353 \
      SINOMO_SHADOW_START_DELAY_MS=0 \
      sinomo daemon shadow run --host 0.0.0.0 --port 21354 \
      >/data/shadow-daemon/manual.stdout.log \
      2>/data/shadow-daemon/manual.stderr.log &
    echo $! >/data/shadow-daemon/manual.pid
  '
}

wait_for_daemon() {
  local node="$1" deadline
  log "$node: waiting for main daemon (<=${WAIT_SECONDS}s)"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    if exec_node "$node" sinomo daemon status >"$ARTIFACTS/$node.daemon-status.last.txt" 2>&1; then
      log "$node: main daemon is reachable"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "$node: daemon did not become reachable"
      sed 's/^/[test-cluster-migration]   /' "$ARTIFACTS/$node.daemon-status.last.txt" || true
      return 1
    fi
    sleep 2
  done
}

target_main_online_count() {
  exec_node target sinomo cluster health 2>/dev/null \
    | grep -c "daemon-main.*online" || true
}

target_doctor_warnings() {
  exec_node target sinomo cluster doctor 2>/dev/null \
    | grep -E "endpoint-unreachable|node-offline|cluster-orphan|duplicate" || true
}

wait_for_cluster_ready() {
  local deadline online warnings
  log "target: waiting for target + migrated source health (<=${WAIT_SECONDS}s)"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    online="$(target_main_online_count)"
    warnings="$(target_doctor_warnings)"
    if [ "${online:-0}" -ge 2 ] && [ -z "$warnings" ]; then
      log "target: cluster ready (${online} main daemons online)"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "target: cluster did not become ready (online=${online:-0})"
      [ -n "$warnings" ] && echo "$warnings" | sed 's/^/[test-cluster-migration]   warning: /'
      return 1
    fi
    sleep 2
  done
}

run_probe() {
  local node="$1" id="$2" outfile="$3"
  shift 3
  if exec_node "$node" sinomo test run "$id" --json "$@" >"$outfile" 2>&1; then
    log "$node: ok $id"
    return 0
  fi
  log "$node: FAILED $id $*"
  sed 's/^/[test-cluster-migration]   /' "$outfile"
  return 1
}

capture_context() {
  log "capturing context into $ARTIFACTS"
  compose ps >"$ARTIFACTS/compose-ps.txt" 2>&1 || true
  for node in target source; do
    compose logs --no-color "$node" >"$ARTIFACTS/$node.compose.log" 2>&1 || true
    exec_node "$node" sinomo cluster status >"$ARTIFACTS/$node.cluster-status.txt" 2>&1 || true
    exec_node "$node" sinomo cluster health >"$ARTIFACTS/$node.cluster-health.txt" 2>&1 || true
    exec_node "$node" sinomo cluster doctor >"$ARTIFACTS/$node.cluster-doctor.txt" 2>&1 || true
    exec_node "$node" cat /data/daemon/main.stderr.log >"$ARTIFACTS/$node.main.stderr.log" 2>/dev/null || true
    exec_node "$node" cat /data/daemon/main.stdout.log >"$ARTIFACTS/$node.main.stdout.log" 2>/dev/null || true
    exec_node "$node" cat /data/shadow-daemon/manual.stderr.log >"$ARTIFACTS/$node.shadow.stderr.log" 2>/dev/null || true
    exec_node_sh "$node" "find /data/cluster -maxdepth 5 -type f -o -type d | sort" \
      >"$ARTIFACTS/$node.cluster-tree.txt" 2>/dev/null || true
    exec_node_sh "$node" \
      'for f in /data/cluster/migrations/*/source/migration.json /data/cluster/migrations/*/source/kv-import.json; do [ -f "$f" ] && echo "===== $f =====" && cat "$f"; done' \
      >"$ARTIFACTS/$node.migrations.txt" 2>/dev/null || true
  done
}

teardown() {
  [ "$TEARDOWN_DONE" = 1 ] && return
  TEARDOWN_DONE=1
  if [ "$FAILED" = 1 ]; then
    capture_context
  fi
  if [ "${SINOMO_MIGRATION_TEST_KEEP:-0}" = 1 ]; then
    log "leaving compose project $PROJECT running; tear down with:"
    log "  docker compose -p $PROJECT -f $COMPOSE_FILE down -v"
    return
  fi
  log "tearing down compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    FAILED=1
  fi
  teardown
  exit "$rc"
}
trap on_exit EXIT

main() {
  require_no_other_compose_projects
  if [ "${SINOMO_MIGRATION_TEST_BUILD:-0}" = 1 ] || [ ! -x "$BINARY" ]; then
    log "building host binary"
    (cd "$SINOMO_ROOT" && cargo build -p sinomo)
  fi

  log "starting compose project $PROJECT"
  compose up --build -d target source

  log "target: initializing cluster and invite"
  exec_node target sinomo cluster init --name target-migration >"$ARTIFACTS/target-init.txt" 2>&1
  exec_node target sinomo cluster invite \
    --introducer target:21353 \
    --ttl-seconds 86400 \
    --output /shared/target-invite.txt \
    >"$ARTIFACTS/target-invite.txt" 2>&1
  start_shadow_supervisor target
  wait_for_daemon target

  log "source: initializing source cluster and fixture"
  exec_node source sinomo cluster init --name source-migration >"$ARTIFACTS/source-init.txt" 2>&1
  run_probe source cluster.migration.seed-source "$ARTIFACTS/source-seed.json" \
    --param node-config-key-suffix=docker-local-test \
    --param node-config-payload=docker-node-local \
    --param cluster-config-key=/config/docker-source-cluster-only \
    --param cluster-config-payload=docker-source-cluster

  log "source: joining target invite through migration path"
  exec_node source sinomo cluster join /shared/target-invite.txt --name migrated-source \
    >"$ARTIFACTS/source-join.txt" 2>&1
  start_shadow_supervisor source
  wait_for_daemon source
  wait_for_cluster_ready

  run_probe source cluster.migration.verify-joined "$ARTIFACTS/source-verify.json" \
    --require-cluster \
    --param node-config-key-suffix=docker-local-test \
    --param node-config-payload=docker-node-local \
    --param cluster-config-key=/config/docker-source-cluster-only

  exec_node target sinomo cluster health >"$ARTIFACTS/final-target-health.txt" 2>&1
  exec_node target sinomo cluster doctor >"$ARTIFACTS/final-target-doctor.txt" 2>&1
  log "cluster join migration docker test passed"
  log "artifacts: $ARTIFACTS"
}

main "$@"
