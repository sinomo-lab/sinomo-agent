#!/usr/bin/env bash
# Sinomo SEGMENTED dev-cluster node entrypoint.
#
# Forms (or rejoins) the cluster best-effort, then runs the shadow supervisor as PID 1.
# This cluster is partitioned into network segments with only node3<->node4 bridging them,
# and iroh is disabled -- so cross-segment joins depend on native multi-hop routing through
# the bridge nodes. Every node joins best-effort (a failed/timed-out join is logged, not
# fatal) and always ends up running its daemon, so route convergence stays observable.
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

BOOTSTRAP_BIN=/usr/local/bin/sinomo
DATA=/data
SRC=/sinomo-src
ROLE="${ROLE:-joiner}"
CLUSTER_NAME="${CLUSTER_NAME:-dev-cluster}"
NODE_NAME="${NODE_NAME:-$(hostname)}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-60}"
JOIN_ATTEMPTS="${JOIN_ATTEMPTS:-12}"
JOIN_RETRY_DELAY="${JOIN_RETRY_DELAY:-5}"
MAIN_PORT="${MAIN_PORT:-21353}"

export SINOMO_DATA_DIR="$DATA"
# This segmented test cluster runs with iroh off by default so segmentation is real.
export SINOMO_DISABLE_IROH="${SINOMO_DISABLE_IROH:-1}"

log() { echo "[entrypoint $(hostname)] $*"; }

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

# Join the cluster in the background AFTER our own daemon is listening, so downstream nodes
# can reach us as their introducer while we join our own upstream. The CLI join is itself
# synchronous (it blocks until the introducer -- which forwards to a voter -- confirms the
# registration), so this logs the real outcome; it just no longer blocks daemon startup.
join_in_background() {
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
  log "joining cluster as '$NODE_NAME' via introducer $INTRODUCER"
  for attempt in $(seq 1 "$JOIN_ATTEMPTS"); do
    if "$BIN" cluster join "$JOIN_INVITE" --name "$NODE_NAME"; then
      log "joined cluster (attempt $attempt)"; return
    fi
    log "join attempt $attempt/$JOIN_ATTEMPTS via $INTRODUCER failed; retrying in ${JOIN_RETRY_DELAY}s (upstream bridge may still be joining)"
    sleep "$JOIN_RETRY_DELAY"
  done
  log "WARN: cluster join via $INTRODUCER did not succeed after $JOIN_ATTEMPTS attempts; staying unclustered"
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
    # Defer the join until the daemon is up (launched below), so this node is reachable as
    # an introducer for its downstream segment while it joins its own upstream introducer.
    DEFER_JOIN=1
  fi

  # The seed (a voter) publishes a join invite for EACH segment, naming the reachable
  # bridge as that segment's introducer. PUBLISH_INVITES is a space-separated list of
  # "introducer-host:port=output-file". Only a voter can mint invites, so the bridge
  # learners do not publish; they relay the joins they receive on to a voter.
  if [ -n "${PUBLISH_INVITES:-}" ]; then
    for spec in $PUBLISH_INVITES; do
      introducer="${spec%%=*}"; output="${spec##*=}"
      log "publishing join invite (introducer $introducer) -> $output"
      "$BIN" cluster invite --introducer "$introducer" --ttl-seconds 86400 --output "$output" \
        || log "WARN: failed to publish invite for introducer $introducer"
    done
  fi
else
  log "existing cluster state found; skipping formation"
fi

# 3. Start the daemon first (so we are reachable as an introducer), then run the deferred
#    join in the background once our control port is listening.
if [ "${DEFER_JOIN:-0}" = 1 ]; then
  join_in_background &
fi

# Run the shadow supervisor as PID 1; it starts and supervises the main daemon.
export SINOMO_SHADOW_MAIN_BINARY="$BIN"
export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
export SINOMO_SHADOW_MAIN_PORT=21353
export SINOMO_SHADOW_START_DELAY_MS=0
log "starting shadow supervisor $SHADOW_BIN on 0.0.0.0:21354 (main $BIN on 0.0.0.0:21353, iroh disabled=$SINOMO_DISABLE_IROH)"
exec "$SHADOW_BIN" daemon shadow run --host 0.0.0.0 --port 21354
