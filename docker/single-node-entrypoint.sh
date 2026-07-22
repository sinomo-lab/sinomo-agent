#!/usr/bin/env bash
# Sinomo single-node cluster entrypoint for local evolution/DFS smoke testing.
set -euo pipefail

BIN="${SINOMO_BIN:-/usr/local/bin/sinomo}"
DATA="${SINOMO_DATA_DIR:-/data}"
SRC="${SINOMO_SOURCE_DIR:-/sinomo-src}"
CLUSTER_NAME="${CLUSTER_NAME:-single-node-evolution}"
NODE_ID="${SINOMO_SINGLE_NODE_ID:-01hxy6qa9k0t9vr4g9n8d3q5bf}"
MAIN_PORT="${MAIN_PORT:-21353}"
SHADOW_PORT="${SHADOW_PORT:-21354}"
MODE="${SINOMO_SINGLE_NODE_MODE:-run}"
AGENT_READINESS_FIXTURES="${SINOMO_ENABLE_AGENT_READINESS_FIXTURES:-0}"
BENCHMARK_MODULE_LIBRARY="${SINOMO_BENCHMARK_MODULE_LIBRARY:-/opt/sinomo/test-modules/libsinomo_benchmarks.so}"

export SINOMO_DATA_DIR="$DATA"
export SINOMO_DISABLE_IROH="${SINOMO_DISABLE_IROH:-1}"

log() { echo "[single-node $(hostname)] $*"; }

wait_for_tcp() {
  local hostport="$1" timeout="${2:-60}" host port elapsed=0
  host="${hostport%%:*}"
  port="${hostport##*:}"
  while ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    elapsed=$((elapsed + 1))
    if [ "$elapsed" -ge "$timeout" ]; then
      return 1
    fi
    sleep 1
  done
  exec 3>&- 2>/dev/null || true
}

link_rollout_source() {
  if [ ! -e "$DATA/source/sinomo" ]; then
    log "linking managed rollout source repo -> $SRC"
    mkdir -p "$DATA/source"
    ln -s "$SRC" "$DATA/source/sinomo"
  fi
}

init_cluster() {
  if [ ! -f "$DATA/cluster/state.sobj" ]; then
    log "bootstrapping single-node cluster '$CLUSTER_NAME'"
    "$BIN" cluster init --name "$CLUSTER_NAME" --node-id "$NODE_ID"
  else
    log "existing cluster state found"
  fi
}

activate_agent_readiness_fixtures() {
  if [ "$AGENT_READINESS_FIXTURES" != "1" ]; then
    return 0
  fi
  if [ ! -f "$BENCHMARK_MODULE_LIBRARY" ]; then
    echo "agent readiness benchmark module not found: $BENCHMARK_MODULE_LIBRARY" >&2
    return 1
  fi
  log "activating native-dylib agent readiness fixture"
  "$BIN" module native-dylib activate \
    --module module.benchmarks \
    --interface sinomo.benchmarks.v1 \
    --cache-key module/benchmark/sinomo-benchmarks \
    --source single-node-agent-readiness \
    "$BENCHMARK_MODULE_LIBRARY"
}

run_smoke() {
  log "starting main daemon for smoke on 0.0.0.0:$MAIN_PORT"
  "$BIN" daemon run --host 0.0.0.0 --port "$MAIN_PORT" &
  local daemon_pid=$!
  trap 'kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true' EXIT

  wait_for_tcp "127.0.0.1:$MAIN_PORT" 60
  "$BIN" cluster status
  "$BIN" dfs smoke --json
  "$BIN" evolve propose "single-node evolution smoke" \
    --repo "$SRC" \
    --dry-run \
    --approve \
    --target-ring r1
  log "single-node smoke passed"
  trap - EXIT
  kill "$daemon_pid" 2>/dev/null || true
  wait "$daemon_pid" 2>/dev/null || true
  return 0
}

link_rollout_source
init_cluster
activate_agent_readiness_fixtures

case "$MODE" in
  smoke)
    run_smoke
    ;;
  run)
    export SINOMO_SHADOW_MAIN_BINARY="$BIN"
    export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
    export SINOMO_SHADOW_MAIN_PORT="$MAIN_PORT"
    export SINOMO_SHADOW_START_DELAY_MS=0
    log "starting shadow supervisor on 0.0.0.0:$SHADOW_PORT (main on 0.0.0.0:$MAIN_PORT)"
    exec "$BIN" daemon shadow run --host 0.0.0.0 --port "$SHADOW_PORT"
    ;;
  *)
    echo "unknown SINOMO_SINGLE_NODE_MODE=$MODE" >&2
    exit 2
    ;;
esac
