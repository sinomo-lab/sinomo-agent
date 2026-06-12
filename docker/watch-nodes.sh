#!/usr/bin/env bash
# Poll the dev-cluster container states and print a line whenever any node's
# state changes. Used to validate the rollout: a node that exits (State=exited)
# during a rollout is the dead-node failure; nodes staying "running" through a
# rollout means the shadow re-exec kept the node supervised.
#
#   docker/watch-nodes.sh            # poll until interrupted
#   docker/watch-nodes.sh 120        # poll for ~120s then exit

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"

set -uo pipefail
cd "$(dirname "$0")/.."

deadline=0
[ "${1:-}" != "" ] && deadline=$(( $(date +%s) + $1 ))
prev=""
while :; do
  now=$(docker compose ps -a --format '{{.Service}}={{.State}}({{.ExitCode}})' 2>/dev/null \
        | sort | tr '\n' ' ')
  if [ "$now" != "$prev" ]; then
    echo "[$(date +%H:%M:%S)] $now"
    prev="$now"
  fi
  case "$now" in
    *=exited*) echo "[$(date +%H:%M:%S)] !! a node EXITED (dead-node signature)";;
  esac
  [ "$deadline" -ne 0 ] && [ "$(date +%s)" -ge "$deadline" ] && break
  sleep 2
done
