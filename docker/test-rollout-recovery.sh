#!/usr/bin/env bash
# Real process/transport recovery on the six-node segmented debug cluster.
# Both executables must be compatible Linux builds with different commits.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${SINOMO_ROLLOUT_BASELINE:?Set the absolute baseline Linux binary path}"
: "${SINOMO_ROLLOUT_CANDIDATE:?Set the absolute candidate Linux binary path}"
: "${SINOMO_ROLLOUT_CANDIDATE_COMMIT:?Set the full candidate source commit}"
PROJECT="${SINOMO_ROLLOUT_TEST_PROJECT:-sinomo-rollout-recovery-$(date +%s)}"
ARTIFACTS="${SINOMO_ROLLOUT_TEST_ARTIFACTS:-$ROOT/target/rollout-recovery/$PROJECT}"
mkdir -p "$ARTIFACTS"
ARTIFACTS="$(cd "$ARTIFACTS" && pwd)"
if [[ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")" ]]; then
  echo "Refusing to reset existing project $PROJECT" >&2
  exit 1
fi
export SINOMO_DOCKER_BINARY="$SINOMO_ROLLOUT_BASELINE"
OVERRIDE="$ARTIFACTS/compose.override.yml"
python3 - "$SINOMO_ROLLOUT_CANDIDATE" "$OVERRIDE" <<'PY'
import json, sys
candidate, output = sys.argv[1:]
with open(output, 'w') as f:
    f.write('services:\n')
    for i in range(1, 7):
        f.write(f'  node{i}:\n    volumes:\n      - {json.dumps(candidate + ":/opt/sinomo-candidate:ro")}\n')
        if i == 2:
            f.write('    environment:\n      MAIN_PORT: "24502"\n      SHADOW_PORT: "24503"\n')
PY
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT/docker/docker-compose.dev.yml" -f "$OVERRIDE")
run() { timeout 180 "${COMPOSE[@]}" exec -T "$1" /opt/sinomo-candidate "${@:2}"; }
container() { "${COMPOSE[@]}" ps -q "$1"; }
status() { run "$1" rollout diagnostics --json > "$ARTIFACTS/$1.status.json"; }
field() { python3 - "$ARTIFACTS/$1.status.json" "$2" <<'PY'
import json, sys
d=json.load(open(sys.argv[1]))['targets'][0]['runtime']
if sys.argv[2] == 'generation':
    print(json.dumps([d['pid'], d['started_at']]))
    raise SystemExit
for key in sys.argv[2].split('.'):
    d=d[key]
print(d)
PY
}
capture() {
  for node in node1 node2 node3 node4 node5 node6; do
    run "$node" rollout diagnostics --json > "$ARTIFACTS/$node.final.json" 2>&1 || true
    local id
    id="$(container "$node")"
    if [[ -n "$id" ]]; then
      for record in state.sobj pending-main-rollout.sobj pending-shadow-rollout.sobj; do
        docker cp "$id:/data/rollout/$record" "$ARTIFACTS/$node-$record" 2>/dev/null || true
      done
      docker cp "$id:/data/rollout/logs" "$ARTIFACTS/$node-rollout-logs" 2>/dev/null || true
    fi
  done
  "${COMPOSE[@]}" logs --no-color > "$ARTIFACTS/containers.log" 2>&1 || true
}
RESOURCE_WATCH_PID=
resource_watch_stop() {
  if [[ -n "$RESOURCE_WATCH_PID" ]]; then
    kill -TERM "$RESOURCE_WATCH_PID" 2>/dev/null || true
    local watch_result=0
    wait "$RESOURCE_WATCH_PID" || watch_result=$?
    RESOURCE_WATCH_PID=
    [[ "$watch_result" == 0 ]] || return "$watch_result"
    [[ -f "$ARTIFACTS/runtime-resources/watch-complete.json" &&
       ! -f "$ARTIFACTS/runtime-resources/HARD-FAILURE.json" ]]
  fi
}
cleanup() {
  local result=$?
  capture
  resource_watch_stop || result=1
  if [[ "${SINOMO_ROLLOUT_TEST_KEEP:-0}" != 1 ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans > "$ARTIFACTS/teardown.log" 2>&1 || true
  fi
  exit "$result"
}
trap cleanup EXIT
SINOMO_TEST_PROJECT="$PROJECT" SINOMO_TEST_BUILD=0 SINOMO_TEST_KEEP=1 \
  SINOMO_TEST_FORMATION_ONLY=1 SINOMO_TEST_BINARY="$SINOMO_ROLLOUT_BASELINE" \
  SINOMO_TEST_COMPOSE_OVERRIDE="$OVERRIDE" SINOMO_TEST_ARTIFACTS="$ARTIFACTS/formation" \
  bash "$ROOT/docker/test-cluster.sh" > "$ARTIFACTS/formation.log" 2>&1

export SINOMO_TEST_RESOURCE_INTENTS="$ARTIFACTS/runtime-resource-intents.jsonl"
: > "$SINOMO_TEST_RESOURCE_INTENTS"
trap 'exit 1' USR1
python3 "$ROOT/docker/runtime_resources.py" watch --project "$PROJECT" \
  --artifacts "$ARTIFACTS/runtime-resources" --parent-pid "$$" \
  --activation-candidate "$SINOMO_ROLLOUT_CANDIDATE" \
  > "$ARTIFACTS/runtime-resources-watch.log" 2>&1 &
RESOURCE_WATCH_PID=$!
while [[ ! -f "$ARTIFACTS/runtime-resources/watch-ready.json" ]]; do
  kill -0 "$RESOURCE_WATCH_PID" 2>/dev/null || exit 1
  sleep 0.1
done
record_activation_exit() {
  python3 "$ROOT/docker/runtime_resources.py" expect-rollout-exit --project "$PROJECT" \
    --nodes "$1" --activation-candidate "$SINOMO_ROLLOUT_CANDIDATE" \
    --plan-log "$ARTIFACTS/$2" --artifacts "$ARTIFACTS"
}

for node in node1 node2 node3 node4; do status "$node"; done
NODE1="$(field node1 cluster.node_id)"
NODE2="$(field node2 cluster.node_id)"
NODE3="$(field node3 cluster.node_id)"
OLD1="$(field node1 generation)"
OLD2="$(field node2 generation)"
OLD3="$(field node3 generation)"
[[ "$(field node3 commit)" != "$SINOMO_ROLLOUT_CANDIDATE_COMMIT" ]]
run node1 cluster promote "$NODE2" > "$ARTIFACTS/promote-node2.log" 2>&1
run node1 cluster promote "$NODE3" > "$ARTIFACTS/promote-node3.log" 2>&1

wait_claim() {
  local node=$1
  for ((i=0; i<600; i++)); do
    if "${COMPOSE[@]}" exec -T "$node" sh -c 'test -n "$(find /data/rollout/staged/uploads -name activation-started.sobj -print -quit 2>/dev/null)"' 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "No activation claim on $node" >&2
  return 1
}
wait_final() {
  local node=$1
  for ((i=0; i<180; i++)); do
    if status "$node" 2>/dev/null && [[ "$(field "$node" rollout.phase)" == shadow-finalized ]]; then return 0; fi
    sleep 1
  done
  return 1
}

# Drop the connection after activation has been claimed. Both main and shadow
# are inaccessible to the coordinator during this gap, while the target's
# local supervisor continues. The original CLI must reconcile that request.
run node1 rollout cluster --binary /opt/sinomo-candidate --skip-native-modules \
  --target "node2:24502" > "$ARTIFACTS/disconnected-activation.log" 2>&1 &
first=$!
wait_claim node2
record_activation_exit node2 disconnected-activation.log
echo 'node2: activation claimed; disconnecting network for five seconds' >> "$ARTIFACTS/faults.log"
C2="$(container node2)"
docker network disconnect "${PROJECT}_seg_a" "$C2"
sleep 5
docker network connect --alias node2 "${PROJECT}_seg_a" "$C2"
wait "$first"
status node2
[[ "$(field node2 generation)" != "$OLD2" ]]
[[ "$(field node2 port)" == 24502 ]]
[[ "$(field node2 commit)" == "$SINOMO_ROLLOUT_CANDIDATE_COMMIT" ]]
run node2 daemon shadow status --host 127.0.0.1 --port 24503 > "$ARTIFACTS/nondefault-shadow.log"
grep -q '24503' "$ARTIFACTS/nondefault-shadow.log"

# Interrupt the coordinator after the first voter starts activation. The
# second voter's process must remain unchanged until resume proves readiness.
"${COMPOSE[@]}" exec -T node2 sh -c \
  'mkdir -p /data/rollout/coordinator; echo "$$" > /data/rollout/coordinator/test.pid; exec "$@"' sh \
  /opt/sinomo-candidate rollout cluster --binary /opt/sinomo-candidate --skip-native-modules \
  --target "$NODE3" --target "$NODE1" > "$ARTIFACTS/interrupted-coordinator.log" 2>&1 &
second=$!
wait_claim node3
record_activation_exit node3 interrupted-coordinator.log
echo 'node3: activation claimed; terminating the coordinator before its reply' >> "$ARTIFACTS/faults.log"
"${COMPOSE[@]}" exec -T node2 sh -c \
  'pid=$(cat /data/rollout/coordinator/test.pid); case "$pid" in *[!0-9]*|"") exit 1;; esac; grep -aq /opt/sinomo-candidate /proc/"$pid"/cmdline && kill -KILL "$pid"'
wait "$second" && { echo 'coordinator unexpectedly succeeded' >&2; exit 1; }
status node1
[[ "$(field node1 generation)" == "$OLD1" ]]
wait_final node3
NEW3="$(field node3 generation)"
[[ "$NEW3" != "$OLD3" ]]
run node2 rollout resume > "$ARTIFACTS/resume.log" 2>&1 &
resuming=$!
wait_claim node1
record_activation_exit node1 interrupted-coordinator.log
wait "$resuming"
status node1
status node3
[[ "$(field node1 generation)" != "$OLD1" ]]
[[ "$(field node3 generation)" == "$NEW3" ]]
[[ "$(field node1 commit)" == "$SINOMO_ROLLOUT_CANDIDATE_COMMIT" ]]
[[ "$(field node3 commit)" == "$SINOMO_ROLLOUT_CANDIDATE_COMMIT" ]]
python3 - "$ARTIFACTS/resume.log" "$NODE3" "$NODE1" <<'PY'
import sys
text=open(sys.argv[1]).read()
assert text.index(sys.argv[2] + ': Raft voter rejoined') < text.index(sys.argv[3] + ': [')
PY
run node2 rollout verify > "$ARTIFACTS/verify.log" 2>&1
run node2 rollout verify --target "$NODE1" --commit "$SINOMO_ROLLOUT_CANDIDATE_COMMIT" \
  > "$ARTIFACTS/verify-without-source.log" 2>&1
run node2 rollout cancel > "$ARTIFACTS/cancel.log" 2>&1
if run node2 rollout resume > "$ARTIFACTS/cancelled-resume.log" 2>&1; then
  echo 'cancelled plan unexpectedly resumed' >&2
  exit 1
fi
status node3
[[ "$(field node3 generation)" == "$NEW3" ]]
run node1 test run cluster.dfs.read-your-write --require-cluster --json \
  --param base=/services/cluster-tests --param timeout-seconds=90 > "$ARTIFACTS/dfs-roundtrip.json"
resource_watch_stop
echo "PASS: activation disconnect, coordinator interruption/resume, no duplicate activation, voter sequencing, custom ports, and guarded cancellation" | tee "$ARTIFACTS/result.txt"
