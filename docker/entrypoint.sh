#!/usr/bin/env bash
# Sinomo SEGMENTED dev-cluster node entrypoint.
#
# Forms the cluster best-effort, then runs the shadow supervisor as PID 1.
# This cluster is partitioned into network segments with only node3<->node4 bridging them,
# and iroh is disabled -- so cross-segment joins depend on native multi-hop routing through
# the bridge nodes. Every node joins once (a failed/timed-out join is logged, not retried)
# and always ends up running its daemon, so route convergence stays observable.
#
# Per-node env (set in docker-compose.yml / docker/docker-compose.dev.yml):
#   ROLE                seed | joiner
#   NODE_NAME           cluster node name
#   CLUSTER_NAME        cluster name (seed only)
#   INTRODUCER          reachable host:port to join through (joiners)
#   JOIN_INVITE         invite file to read when joining (joiners)
#   PUBLISH_INVITE      invite file to (re)publish with PUBLISH_INTRODUCER (seed + bridges)
#   PUBLISH_INTRODUCER  introducer host:port to advertise in the published invite
#   SINOMO_DISABLE_IROH "1" => run on direct LAN transports only (default here)
#   SINOMO_MANUAL_ENDPOINTS_JSON optional raw configs/cluster/endpoints.json content
#   WAIT_FOR_INTRODUCER_READY "1" => wait for /shared/<introducer-host>.joined (dev harness)
#   WAIT_FOR_JOIN_READY_NODES optional space-separated node names to wait for before joining
set -uo pipefail   # NOTE: not -e; cross-segment join failures are expected and handled.

BOOTSTRAP_BIN=/usr/local/bin/sinomo
DATA=/data
SRC=/sinomo-src
ROLE="${ROLE:-joiner}"
CLUSTER_NAME="${CLUSTER_NAME:-dev-cluster}"
NODE_NAME="${NODE_NAME:-$(hostname)}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-60}"
JOIN_WAIT_INTERVAL="${JOIN_WAIT_INTERVAL:-0.1}"
JOIN_WAIT_TICKS_PER_SECOND="${JOIN_WAIT_TICKS_PER_SECOND:-10}"
CLUSTER_STATUS_TIMEOUT="${CLUSTER_STATUS_TIMEOUT:-5}"
MAIN_PORT="${MAIN_PORT:-21353}"
JOIN_READY_DIR="${JOIN_READY_DIR:-/shared}"
WAIT_FOR_INTRODUCER_READY="${WAIT_FOR_INTRODUCER_READY:-0}"
WAIT_FOR_JOIN_READY_NODES="${WAIT_FOR_JOIN_READY_NODES:-}"

export SINOMO_DATA_DIR="$DATA"
# This segmented test cluster runs with iroh off by default so segmentation is real.
export SINOMO_DISABLE_IROH="${SINOMO_DISABLE_IROH:-1}"
export SINOMO_DAEMON_ADVERTISE_HOST="${SINOMO_DAEMON_ADVERTISE_HOST:-$NODE_NAME}"

log() { echo "[entrypoint $(hostname)] $*"; }

write_manual_endpoint_config() {
  [ -n "${SINOMO_MANUAL_ENDPOINTS_JSON:-}" ] || return 0
  local config_dir="$DATA/configs/cluster"
  mkdir -p "$config_dir" || return 1
  printf '%s\n' "$SINOMO_MANUAL_ENDPOINTS_JSON" > "$config_dir/endpoints.json" || return 1
  log "wrote manual endpoint advertisement config"
}

if ! write_manual_endpoint_config; then
  log "FATAL: failed to write manual endpoint advertisement config"
  exit 1
fi

# If Docker restarts the container after a rollout stops shadow-as-PID1, finish the
# file-install part of shadow finalization before launching the next supervisor.
if [ -f "$DATA/rollout/pending-shadow-rollout.sobj" ] && [ -x "$DATA/rollout/staged/sinomo-shadow" ] && [ ! -x "$DATA/bin/sinomo-shadow" ]; then
  log "installing staged shadow binary before supervisor restart"
  mkdir -p "$DATA/bin"
  cp -f "$DATA/rollout/staged/sinomo-shadow" "$DATA/bin/sinomo-shadow.next"
  chmod +x "$DATA/bin/sinomo-shadow.next"
  mv -f "$DATA/bin/sinomo-shadow.next" "$DATA/bin/sinomo-shadow"
fi

# After a rollout, the stable binaries live in /data/bin. Prefer them on container
# restart so a shadow-supervisor replacement can complete even though shadow is PID 1.
BIN="$BOOTSTRAP_BIN"
if [ -x "$DATA/bin/sinomo" ]; then
  BIN="$DATA/bin/sinomo"
fi
SHADOW_BIN="$BIN"
if [ -x "$DATA/bin/sinomo-shadow" ]; then
  SHADOW_BIN="$DATA/bin/sinomo-shadow"
fi

# Wait up to $2 seconds for a file to appear; returns 1 on timeout.
wait_for_file() {
  local path="$1" timeout="${2:-$JOIN_TIMEOUT}" i=0 max_attempts
  max_attempts=$(( timeout * JOIN_WAIT_TICKS_PER_SECOND ))
  while [ ! -f "$path" ]; do
    i=$((i + 1)); [ "$i" -ge "$max_attempts" ] && return 1
    sleep "$JOIN_WAIT_INTERVAL"
  done
}

# Wait up to $2 seconds for a host:port to accept a TCP connection; returns 1 on timeout.
wait_for_tcp() {
  local hostport="$1" timeout="${2:-$JOIN_TIMEOUT}" host port i=0 max_attempts
  max_attempts=$(( timeout * JOIN_WAIT_TICKS_PER_SECOND ))
  host="${hostport%%:*}"; port="${hostport##*:}"
  while ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    i=$((i + 1)); [ "$i" -ge "$max_attempts" ] && return 1
    sleep "$JOIN_WAIT_INTERVAL"
  done
  exec 3>&- 2>/dev/null || true
}

cluster_join_status() {
  timeout --foreground "${CLUSTER_STATUS_TIMEOUT}s" "$BIN" cluster status 2>/dev/null \
    | awk '/^status:/{print $2; exit}'
}

cluster_join_already_accepted() {
  local status
  [ -f "$DATA/cluster/state.sobj" ] || return 1
  status="$(cluster_join_status || true)"
  case "$status" in
    voter|read-only)
      log "cluster state already accepted locally (status: $status); skipping join"
      return 0
      ;;
  esac
  return 1
}

publish_join_ready() {
  [ -d "$JOIN_READY_DIR" ] || return
  : > "$JOIN_READY_DIR/$NODE_NAME.joined" 2>/dev/null || true
}

wait_for_introducer_ready() {
  [ "$WAIT_FOR_INTRODUCER_READY" = "1" ] || return 0
  local introducer_host marker
  introducer_host="${INTRODUCER%%:*}"
  [ -n "$introducer_host" ] || return 0
  marker="$JOIN_READY_DIR/$introducer_host.joined"
  if ! wait_for_file "$marker" "$JOIN_TIMEOUT"; then
    log "WARN: introducer readiness marker $marker did not appear (<=${JOIN_TIMEOUT}s); staying unclustered"
    return 1
  fi
}

wait_for_join_ready_nodes() {
  local node marker
  for node in $WAIT_FOR_JOIN_READY_NODES; do
    marker="$JOIN_READY_DIR/$node.joined"
    if ! wait_for_file "$marker" "$JOIN_TIMEOUT"; then
      log "WARN: join dependency marker $marker did not appear (<=${JOIN_TIMEOUT}s); staying unclustered"
      return 1
    fi
  done
}

publish_invites_once() {
  local spec introducer output
  [ -n "${PUBLISH_INVITES:-}" ] || return 0
  for spec in $PUBLISH_INVITES; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    log "publishing join invite (introducer $introducer) -> $output"
    "$BIN" cluster invite --introducer "$introducer" --ttl-seconds 86400 --output "$output" \
      || log "WARN: failed to publish invite for introducer $introducer"
  done
}

publish_invites_once_in_background() {
  if ! wait_for_tcp "127.0.0.1:$MAIN_PORT" "$JOIN_TIMEOUT"; then
    log "WARN: local daemon never came up on :$MAIN_PORT; not publishing join invites"; return
  fi
  publish_invites_once
}

# Join the cluster once in the background AFTER our own daemon is listening. This is
# intentionally one-shot: a failed join leaves the node unclustered, and the test
# harness must provide deterministic readiness instead of relying on retries.
join_once_in_background() {
  local started elapsed output
  started="$SECONDS"
  if ! wait_for_tcp "127.0.0.1:$MAIN_PORT" "$JOIN_TIMEOUT"; then
    log "WARN: local daemon never came up on :$MAIN_PORT; staying unclustered"; return
  fi
  if [ -z "${JOIN_INVITE:-}" ] || [ -z "${INTRODUCER:-}" ]; then
    log "WARN: no JOIN_INVITE/INTRODUCER set; staying unclustered"; return
  fi
  if ! wait_for_file "$JOIN_INVITE" "$JOIN_TIMEOUT"; then
    log "WARN: invite $JOIN_INVITE never appeared (<=${JOIN_TIMEOUT}s); staying unclustered"; return
  fi
  if ! wait_for_tcp "$INTRODUCER" "$JOIN_TIMEOUT"; then
    log "WARN: introducer $INTRODUCER unreachable (<=${JOIN_TIMEOUT}s); staying unclustered"; return
  fi
  if ! wait_for_introducer_ready; then
    return
  fi
  if ! wait_for_join_ready_nodes; then
    return
  fi
  log "joining cluster as '$NODE_NAME' via introducer $INTRODUCER"
  if cluster_join_already_accepted; then
    publish_join_ready
    return
  fi
  output="$("$BIN" cluster join "$JOIN_INVITE" --name "$NODE_NAME" 2>&1)"
  if [ "$?" -eq 0 ]; then
    printf '%s\n' "$output"
    elapsed=$((SECONDS - started))
    log "joined cluster in ${elapsed}s"
    publish_join_ready
    return
  fi
  printf '%s\n' "$output"
  elapsed=$((SECONDS - started))
  log "WARN: cluster join via $INTRODUCER failed after ${elapsed}s; staying unclustered"
}

# 1. Managed rollout source repo: symlink to the image's pre-built source tree (/sinomo-src)
#    so any in-container rollout build reuses its warm target/ (incremental, not cold).
#    The thin dev image (docker/Dockerfile.dev) ships no source/toolchain; rollout
#    builds are unavailable there and the link is skipped.
if [ ! -e "$DATA/source/sinomo" ] && [ -d "$SRC" ]; then
  log "linking managed rollout source repo -> $SRC (shared warm build cache)"
  mkdir -p "$DATA/source"
  ln -s "$SRC" "$DATA/source/sinomo"
fi

# 2. Cluster formation (only on first boot). Best-effort: never exit on a join failure.
if [ ! -f "$DATA/cluster/state.sobj" ]; then
  if [ "$ROLE" = "seed" ]; then
    log "bootstrapping cluster '$CLUSTER_NAME'"
    if ! "$BIN" cluster init --name "$CLUSTER_NAME"; then
      log "FATAL: seed cluster init failed"; exit 1
    fi
    publish_join_ready
  else
    # Defer the join until the daemon is up (launched below), so this node is reachable as
    # an introducer for its downstream segment while it joins its own upstream introducer.
    DEFER_JOIN=1
  fi

  # The seed (a voter) publishes a join invite for EACH segment, naming the reachable
  # bridge as that segment's introducer. PUBLISH_INVITES is a space-separated list of
  # "introducer-host:port=output-file". Defer publication until the daemon is reachable
  # so node1 can answer health and forwarded join traffic immediately after startup.
  [ -n "${PUBLISH_INVITES:-}" ] && DEFER_PUBLISH_INVITES=1
else
  existing_status="$(cluster_join_status || true)"
  case "$existing_status" in
    voter|read-only)
      log "existing accepted cluster state found (status: $existing_status); skipping formation"
      publish_join_ready
      ;;
    *)
      if [ "$ROLE" = "seed" ]; then
        log "existing non-accepted seed cluster state found (status: ${existing_status:-unknown}); leaving formation unchanged"
      else
        log "existing non-accepted cluster state found (status: ${existing_status:-unknown}); not retrying automatically"
      fi
      ;;
  esac
fi

# 3. Start the daemon first (so a bridge can be reached as an introducer), then run the
#    deferred one-shot join in the background once our control port is listening.
if [ "${DEFER_JOIN:-0}" = 1 ]; then
  join_once_in_background &
fi
if [ "${DEFER_PUBLISH_INVITES:-0}" = 1 ]; then
  publish_invites_once_in_background &
fi

# Run the shadow supervisor as PID 1; it starts and supervises the main daemon.
export SINOMO_SHADOW_MAIN_BINARY="$BIN"
export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
export SINOMO_SHADOW_MAIN_PORT=21353
export SINOMO_SHADOW_START_DELAY_MS=0
log "starting shadow supervisor $SHADOW_BIN on 0.0.0.0:21354 (main $BIN on 0.0.0.0:21353, iroh disabled=$SINOMO_DISABLE_IROH)"
exec "$SHADOW_BIN" daemon shadow run --host 0.0.0.0 --port 21354
