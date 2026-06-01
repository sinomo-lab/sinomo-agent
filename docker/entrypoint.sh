#!/usr/bin/env bash
# Sinomo SEGMENTED dev-cluster node entrypoint.
#
# Forms (or rejoins) the cluster best-effort, then runs the shadow supervisor as PID 1.
# This cluster is partitioned into network segments with only node3<->node4 bridging them,
# and iroh is disabled -- so cross-segment joins are EXPECTED TO FAIL until native routing
# exists. Every node therefore joins best-effort (a failed/timed-out join is logged, not
# fatal) and always ends up running its daemon, so all nodes stay up and LAN-discoverable.
#
# Per-node env (set in docker-compose.yml):
#   ROLE                seed | joiner
#   NODE_NAME           cluster node name
#   CLUSTER_NAME        cluster name (seed only)
#   INTRODUCER          reachable host:port to join through (joiners)
#   JOIN_INVITE         invite file to read when joining (joiners)
#   PUBLISH_INVITE      invite file to (re)publish with PUBLISH_INTRODUCER (seed + bridges)
#   PUBLISH_INTRODUCER  introducer host:port to advertise in the published invite
#   SINOMO_DISABLE_IROH "1" => run on direct LAN transports only (default here)
set -uo pipefail   # NOTE: not -e; cross-segment join failures are expected and handled.

BIN=/usr/local/bin/sinomo
DATA=/data
SRC=/sinomo-src
ROLE="${ROLE:-joiner}"
CLUSTER_NAME="${CLUSTER_NAME:-dev-cluster}"
NODE_NAME="${NODE_NAME:-$(hostname)}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-60}"

export SINOMO_DATA_DIR="$DATA"
# This segmented test cluster runs with iroh off by default so segmentation is real.
export SINOMO_DISABLE_IROH="${SINOMO_DISABLE_IROH:-1}"

log() { echo "[entrypoint $(hostname)] $*"; }

# Wait up to $2 seconds for a file to appear; returns 1 on timeout.
wait_for_file() {
  local path="$1" timeout="${2:-$JOIN_TIMEOUT}" i=0
  while [ ! -f "$path" ]; do
    i=$((i + 1)); [ "$i" -ge "$timeout" ] && return 1
    sleep 1
  done
}

# Wait up to $2 seconds for a host:port to accept a TCP connection; returns 1 on timeout.
wait_for_tcp() {
  local hostport="$1" timeout="${2:-$JOIN_TIMEOUT}" host port i=0
  host="${hostport%%:*}"; port="${hostport##*:}"
  while ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    i=$((i + 1)); [ "$i" -ge "$timeout" ] && return 1
    sleep 1
  done
  exec 3>&- 2>/dev/null || true
}

# 1. Managed rollout source repo: symlink to the image's pre-built source tree (/sinomo-src)
#    so any in-container rollout build reuses its warm target/ (incremental, not cold).
if [ ! -e "$DATA/source/sinomo" ]; then
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
  else
    if [ -z "${JOIN_INVITE:-}" ] || [ -z "${INTRODUCER:-}" ]; then
      log "WARN: no JOIN_INVITE/INTRODUCER set; starting unclustered"
    elif ! wait_for_file "$JOIN_INVITE" "$JOIN_TIMEOUT"; then
      log "WARN: invite $JOIN_INVITE never appeared (<=${JOIN_TIMEOUT}s); starting unclustered (an upstream segment likely failed to form)"
    elif ! wait_for_tcp "$INTRODUCER" "$JOIN_TIMEOUT"; then
      log "WARN: introducer $INTRODUCER unreachable (<=${JOIN_TIMEOUT}s); starting unclustered"
    else
      log "joining cluster as '$NODE_NAME' via introducer $INTRODUCER"
      if "$BIN" cluster join "$JOIN_INVITE" --name "$NODE_NAME"; then
        log "joined cluster"
      else
        log "WARN: cluster join via $INTRODUCER failed; starting unclustered (EXPECTED across segments without routing)"
      fi
    fi
  fi

  # Seed and bridge nodes (re)publish an invite naming themselves as introducer, so the
  # next segment has a reachable peer to attempt joining through. Best-effort: this needs
  # cluster state, which a node that failed to join will not have.
  if [ -n "${PUBLISH_INVITE:-}" ] && [ -n "${PUBLISH_INTRODUCER:-}" ]; then
    log "publishing join invite (introducer $PUBLISH_INTRODUCER) -> $PUBLISH_INVITE"
    "$BIN" cluster invite --introducer "$PUBLISH_INTRODUCER" --ttl-seconds 86400 --output "$PUBLISH_INVITE" \
      || log "WARN: invite publish failed (node is not a cluster member yet)"
  fi
else
  log "existing cluster state found; skipping formation"
fi

# 3. Run the shadow supervisor as PID 1; it starts and supervises the main daemon.
export SINOMO_SHADOW_MAIN_BINARY="$BIN"
export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
export SINOMO_SHADOW_MAIN_PORT=21353
export SINOMO_SHADOW_START_DELAY_MS=0
log "starting shadow supervisor on 0.0.0.0:21354 (main on 0.0.0.0:21353, iroh disabled=$SINOMO_DISABLE_IROH)"
exec "$BIN" daemon shadow run --host 0.0.0.0 --port 21354
