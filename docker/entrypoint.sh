#!/usr/bin/env bash
# Sinomo dev-cluster node entrypoint.
#
# Forms (or rejoins) the cluster, then runs the shadow supervisor as PID 1.
# Roles (set via the ROLE env var in docker-compose):
#   seed   - bootstraps the cluster and publishes a join invite to /shared
#   joiner - waits for the invite + the seed daemon, then joins
set -euo pipefail

BIN=/usr/local/bin/sinomo
DATA=/data
SRC=/sinomo-src
INVITE=/shared/invite.json
ROLE="${ROLE:-joiner}"
SEED_HOST="${SEED_HOST:-node1:21353}"
CLUSTER_NAME="${CLUSTER_NAME:-dev-cluster}"
NODE_NAME="${NODE_NAME:-$(hostname)}"

export SINOMO_DATA_DIR="$DATA"

log() { echo "[entrypoint $(hostname)] $*"; }

# 1. Managed rollout source repo (so `rollout apply`/`cluster` can build in-container).
#    Symlink it to the image's pre-built source tree (/sinomo-src) instead of
#    importing a fresh copy. The rollout then builds *in* /sinomo-src, reusing its
#    already-warm target/ — so an in-container rollout build is incremental
#    (seconds) rather than recompiling the vendored wasmer/WASIX crates from cold
#    (minutes). /sinomo-src is a real git repo (see Dockerfile), so the rollout
#    treats it as an initialized managed source repo.
if [ ! -e "$DATA/source/sinomo" ]; then
  log "linking managed rollout source repo -> $SRC (shared warm build cache)"
  mkdir -p "$DATA/source"
  ln -s "$SRC" "$DATA/source/sinomo"
fi

# 2. Cluster formation (only on first boot).
if [ ! -f "$DATA/cluster/state.sobj" ]; then
  if [ "$ROLE" = "seed" ]; then
    log "bootstrapping cluster '$CLUSTER_NAME'"
    "$BIN" cluster init --name "$CLUSTER_NAME"
    log "publishing join invite (introducer $SEED_HOST) -> $INVITE"
    "$BIN" cluster invite --introducer "$SEED_HOST" --ttl-seconds 86400 --output "$INVITE"
  else
    log "waiting for join invite at $INVITE"
    while [ ! -f "$INVITE" ]; do sleep 1; done
    seed_host="${SEED_HOST%%:*}"; seed_port="${SEED_HOST##*:}"
    log "waiting for seed daemon $seed_host:$seed_port"
    until (exec 3<>"/dev/tcp/$seed_host/$seed_port") 2>/dev/null; do sleep 1; done
    exec 3>&- 2>/dev/null || true
    log "joining cluster as '$NODE_NAME'"
    "$BIN" cluster join "$INVITE" --name "$NODE_NAME"
  fi
else
  log "existing cluster state found; skipping formation"
fi

# 3. Run the shadow supervisor as PID 1; it starts and supervises the main daemon.
export SINOMO_SHADOW_MAIN_BINARY="$BIN"
export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
export SINOMO_SHADOW_MAIN_PORT=21353
export SINOMO_SHADOW_START_DELAY_MS=0
log "starting shadow supervisor on 0.0.0.0:21354 (main on 0.0.0.0:21353)"
exec "$BIN" daemon shadow run --host 0.0.0.0 --port 21354
