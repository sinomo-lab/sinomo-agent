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
#   SINOMO_MIGRATION_TEST_BUILD=0      reuse an existing host binary
#   SINOMO_MIGRATION_TEST_KEEP         leave compose project running
#   SINOMO_MIGRATION_TEST_ARTIFACTS    artifact dir
#   SINOMO_MIGRATION_TEST_WAIT         daemon/health wait seconds (default 120)
#   SINOMO_MIGRATION_TEST_COMMAND_TIMEOUT readiness command cap seconds (default 10)
#   SINOMO_MIGRATION_TEST_PROBE_TIMEOUT test probe cap seconds (default WAIT)
#   SINOMO_MIGRATION_TEST_HOST_TIMEOUT_GRACE host cleanup grace seconds (default 5)
#   SINOMO_MIGRATION_TEST_ALLOW_OTHER_COMPOSE=1 allow unrelated compose projects
set -euo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.migration.yml}"
PROJECT="${SINOMO_MIGRATION_TEST_PROJECT:-sinomo-migration-test}"
WAIT_SECONDS="${SINOMO_MIGRATION_TEST_WAIT:-120}"
COMMAND_TIMEOUT="${SINOMO_MIGRATION_TEST_COMMAND_TIMEOUT:-10}"
PROBE_TIMEOUT="${SINOMO_MIGRATION_TEST_PROBE_TIMEOUT:-$WAIT_SECONDS}"
HOST_TIMEOUT_GRACE="${SINOMO_MIGRATION_TEST_HOST_TIMEOUT_GRACE:-5}"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_MIGRATION_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/migration-$RUN_ID}"
BINARY="$SINOMO_ROOT/target/debug/sinomo"
FAILED=0
TEARDOWN_DONE=0
PROJECT_OWNED=0

mkdir -p "$ARTIFACTS"

case "$WAIT_SECONDS:$COMMAND_TIMEOUT:$PROBE_TIMEOUT:$HOST_TIMEOUT_GRACE" in
  *[!0-9:]*)
    echo "migration wait and command timeout values must be positive integers"
    exit 2
    ;;
esac
for value in "$WAIT_SECONDS" "$COMMAND_TIMEOUT" "$PROBE_TIMEOUT"; do
  if [ "$value" -le 0 ]; then
    echo "migration wait and command timeout values must be greater than 0"
    exit 2
  fi
done

log() {
  echo "[test-cluster-migration] $*"
}

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

exec_node_timeout() {
  local seconds="$1" node="$2" host_seconds
  shift 2
  host_seconds=$(( seconds + HOST_TIMEOUT_GRACE ))
  timeout --foreground "${host_seconds}s" \
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" \
    exec -T \
      -e SINOMO_DATA_DIR=/data \
      -e SINOMO_DISABLE_IROH=1 \
      -e SINOMO_DAEMON_ADVERTISE_HOST="$node" \
      "$node" timeout --foreground "${seconds}s" "$@"
}

deadline_command_timeout() {
  local deadline="$1" maximum="$2" now remaining
  now="$(date +%s)"
  remaining=$(( deadline - now ))
  [ "$remaining" -gt 0 ] || return 1
  if [ "$remaining" -lt "$maximum" ]; then
    printf '%s\n' "$remaining"
  else
    printf '%s\n' "$maximum"
  fi
}

exec_node_sh() {
  local node="$1"
  shift
  exec_node_timeout "$COMMAND_TIMEOUT" "$node" sh -lc "$*"
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
  exec_node_timeout "$COMMAND_TIMEOUT" "$node" sh -lc '
    mkdir -p /data/daemon /data/shadow-daemon
    if [ -f /tmp/sinomo-migration-shadow.pid ] && kill -0 "$(cat /tmp/sinomo-migration-shadow.pid)" 2>/dev/null; then
      exit 0
    fi
    nohup env \
      SINOMO_DATA_DIR=/data \
      SINOMO_DISABLE_IROH=1 \
      SINOMO_SHADOW_MAIN_BINARY=/usr/local/bin/sinomo \
      SINOMO_SHADOW_MAIN_HOST=0.0.0.0 \
      SINOMO_SHADOW_MAIN_PORT=21353 \
      SINOMO_SHADOW_START_DELAY_MS=0 \
      sinomo daemon shadow run --host 0.0.0.0 --port 21354 \
      >/tmp/sinomo-migration-shadow.stdout.log \
      2>/tmp/sinomo-migration-shadow.stderr.log &
    echo $! >/tmp/sinomo-migration-shadow.pid
  '
}

wait_for_daemon() {
  local node="$1" deadline command_timeout now
  log "$node: waiting for main daemon (<=${WAIT_SECONDS}s)"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    if command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")" \
      && exec_node_timeout "$command_timeout" "$node" sinomo daemon status \
        >"$ARTIFACTS/$node.daemon-status.last.txt" 2>&1 \
      && grep -Fx "sinomo daemon is running" \
        "$ARTIFACTS/$node.daemon-status.last.txt" >/dev/null; then
      now="$(date +%s)"
      if [ "$now" -le "$deadline" ]; then
        log "$node: main daemon is reachable"
        return 0
      fi
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "$node: daemon did not become reachable"
      sed 's/^/[test-cluster-migration]   /' "$ARTIFACTS/$node.daemon-status.last.txt" || true
      return 1
    fi
    sleep 2
  done
}

wait_for_daemon_identity_restart() {
  local node="$1" previous_pid="$2" expected_cluster_id="$3"
  local deadline command_timeout now current_pid current_cluster status_file
  status_file="$ARTIFACTS/$node.daemon-status.after-join.last.txt"
  log "$node: waiting for main daemon identity restart (<=${WAIT_SECONDS}s)"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    if command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")" \
      && exec_node_timeout "$command_timeout" "$node" sinomo daemon status \
        >"$status_file" 2>&1 \
      && grep -Fx "sinomo daemon is running" "$status_file" >/dev/null; then
      current_pid="$(awk '/^  pid:/ { print $2; exit }' "$status_file")"
      current_cluster="$(awk '/^  cluster:/ { print $2; exit }' "$status_file")"
      now="$(date +%s)"
      if [ -n "$current_pid" ] \
        && [ "$current_pid" != "$previous_pid" ] \
        && [ "$current_cluster" = "$expected_cluster_id" ] \
        && [ "$now" -le "$deadline" ]; then
        log "$node: main daemon restarted as pid $current_pid in cluster $current_cluster"
        return 0
      fi
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "$node: daemon did not restart with target cluster identity $expected_cluster_id"
      sed 's/^/[test-cluster-migration]   /' "$status_file" || true
      return 1
    fi
    sleep 1
  done
}

target_cluster_health() {
  local seconds="${1:-$COMMAND_TIMEOUT}"
  exec_node_timeout "$seconds" target sinomo cluster health 2>/dev/null || true
}

target_doctor_warnings() {
  local seconds="${1:-$COMMAND_TIMEOUT}" output
  if ! output="$(exec_node_timeout "$seconds" target sinomo cluster doctor 2>/dev/null)"; then
    return 1
  fi
  printf '%s\n' "$output" | awk -F '\t' '$1 == "warning" || $1 == "error"' || true
}

wait_for_cluster_ready() {
  local deadline health online warnings command_timeout now
  log "target: waiting for target + migrated source health (<=${WAIT_SECONDS}s)"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    if ! command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")"; then
      log "target: cluster did not become ready (online=${online:-0})"
      return 1
    fi
    health="$(target_cluster_health "$command_timeout")"
    online="$(printf '%s\n' "$health" | grep -c "daemon-main.*online" || true)"
    warnings=""
    if [ "${online:-0}" -ge 2 ]; then
      if command_timeout="$(deadline_command_timeout "$deadline" "$COMMAND_TIMEOUT")"; then
        if ! warnings="$(target_doctor_warnings "$command_timeout")"; then
          warnings="cluster doctor failed or timed out"
        fi
      else
        warnings="readiness deadline exhausted before doctor completed"
      fi
    fi
    now="$(date +%s)"
    if [ "${online:-0}" -ge 2 ] && [ -z "$warnings" ]; then
      if [ "$now" -le "$deadline" ]; then
        log "target: cluster ready (${online} main daemons online)"
        return 0
      fi
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "target: cluster did not become ready (online=${online:-0})"
      [ -n "$warnings" ] && echo "$warnings" | sed 's/^/[test-cluster-migration]   warning: /'
      return 1
    fi
    sleep 2
  done
}

run_probe() {
  local node="$1" id="$2" outfile="$3" errfile
  shift 3
  errfile="${outfile%.json}.stderr.log"
  if exec_node_timeout "$PROBE_TIMEOUT" "$node" sinomo test run "$id" --json "$@" \
      >"$outfile" 2>"$errfile"; then
    log "$node: ok $id"
    return 0
  fi
  log "$node: FAILED $id $*"
  sed 's/^/[test-cluster-migration]   /' "$outfile"
  sed 's/^/[test-cluster-migration]   /' "$errfile"
  return 1
}

capture_context() {
  log "capturing context into $ARTIFACTS"
  compose ps >"$ARTIFACTS/compose-ps.txt" 2>&1 || true
  for node in target source; do
    compose logs --no-color "$node" >"$ARTIFACTS/$node.compose.log" 2>&1 || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" sinomo cluster status >"$ARTIFACTS/$node.cluster-status.txt" 2>&1 || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" sinomo cluster health >"$ARTIFACTS/$node.cluster-health.txt" 2>&1 || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" sinomo cluster doctor >"$ARTIFACTS/$node.cluster-doctor.txt" 2>&1 || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" cat /data/daemon/main.stderr.log >"$ARTIFACTS/$node.main.stderr.log" 2>/dev/null || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" cat /data/daemon/main.stdout.log >"$ARTIFACTS/$node.main.stdout.log" 2>/dev/null || true
    exec_node_timeout "$COMMAND_TIMEOUT" "$node" cat /tmp/sinomo-migration-shadow.stderr.log >"$ARTIFACTS/$node.shadow.stderr.log" 2>/dev/null || true
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
  [ "$PROJECT_OWNED" = 1 ] || return
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

main() {
  require_no_other_compose_projects
  trap on_exit EXIT
  if [ "${SINOMO_MIGRATION_TEST_BUILD:-1}" = 1 ] || [ ! -x "$BINARY" ]; then
    log "building host binary"
    compose stop -t 10
    (cd "$SINOMO_ROOT" && env -u CARGO_BUILD_JOBS cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler)
  fi

  log "resetting compose project $PROJECT"
  PROJECT_OWNED=1
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  log "starting compose project $PROJECT"
  compose up --build -d target source

  log "target: initializing cluster and daemon"
  exec_node_timeout "$PROBE_TIMEOUT" target sinomo cluster init --name target-migration >"$ARTIFACTS/target-init.txt" 2>&1
  start_shadow_supervisor target
  wait_for_daemon target
  log "target: creating daemon-mediated invite"
  exec_node_timeout "$PROBE_TIMEOUT" target sinomo cluster invite \
    --introducer target:21353 \
    --ttl-seconds 86400 \
    --output /shared/target-invite.txt \
    >"$ARTIFACTS/target-invite.txt" 2>&1

  log "source: initializing source cluster and fixture"
  exec_node_timeout "$PROBE_TIMEOUT" source sinomo cluster init --name source-migration >"$ARTIFACTS/source-init.txt" 2>&1
  run_probe source cluster.migration.seed-source "$ARTIFACTS/source-seed.json" \
    --param node-config-key-suffix=docker-local-test \
    --param node-config-payload=docker-node-local \
    --param cluster-config-key=/config/docker-source-cluster-only \
    --param cluster-config-payload=docker-source-cluster
  start_shadow_supervisor source
  wait_for_daemon source
  cp "$ARTIFACTS/source.daemon-status.last.txt" \
    "$ARTIFACTS/source.daemon-status.pre-join.txt"
  local source_daemon_pid target_cluster_id
  source_daemon_pid="$(awk '/^  pid:/ { print $2; exit }' \
    "$ARTIFACTS/source.daemon-status.pre-join.txt")"
  if [ -z "$source_daemon_pid" ]; then
    log "source: could not capture pre-join daemon pid"
    return 1
  fi

  log "source: joining target invite through migration path"
  exec_node_timeout "$PROBE_TIMEOUT" source sinomo cluster join /shared/target-invite.txt --name migrated-source \
    >"$ARTIFACTS/source-join.txt" 2>&1
  target_cluster_id="$(awk '/^cluster join completed:/ { print $4; exit }' \
    "$ARTIFACTS/source-join.txt")"
  if [ -z "$target_cluster_id" ]; then
    log "source: join output did not report the target cluster id"
    return 1
  fi
  wait_for_daemon_identity_restart source "$source_daemon_pid" "$target_cluster_id"
  wait_for_cluster_ready

  run_probe source cluster.migration.verify-joined "$ARTIFACTS/source-verify.json" \
    --require-cluster \
    --param node-config-key-suffix=docker-local-test \
    --param node-config-payload=docker-node-local \
    --param cluster-config-key=/config/docker-source-cluster-only

  exec_node_timeout "$COMMAND_TIMEOUT" target sinomo cluster health >"$ARTIFACTS/final-target-health.txt" 2>&1
  exec_node_timeout "$COMMAND_TIMEOUT" target sinomo cluster doctor >"$ARTIFACTS/final-target-doctor.txt" 2>&1
  log "cluster join migration docker test passed"
  log "artifacts: $ARTIFACTS"
}

main "$@"
