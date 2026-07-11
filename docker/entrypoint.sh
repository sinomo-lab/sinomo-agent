#!/usr/bin/env bash
# Sinomo SEGMENTED dev-cluster node entrypoint.
#
# Forms the cluster best-effort, then runs the shadow supervisor as PID 1.
# This cluster is partitioned into network segments with only node3<->node4 bridging them,
# and iroh is disabled -- so cross-segment joins depend on native multi-hop routing through
# the bridge nodes. Every node sends one join attempt. If that command loses an accepted
# response, the entrypoint briefly observes local state for the already-committed join; it
# never resends registration. Terminal failures leave the daemon running so route convergence
# stays observable.
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
#   JOIN_RECONCILE_GRACE seconds to observe an accepted join after command failure (default 5,
#                        capped by the original JOIN_TIMEOUT budget; 0 disables)
set -uo pipefail   # NOTE: not -e; cross-segment join failures are expected and handled.

BOOTSTRAP_BIN=/usr/local/bin/sinomo
DATA=/data
SRC=/sinomo-src
ROLE="${ROLE:-joiner}"
CLUSTER_NAME="${CLUSTER_NAME:-dev-cluster}"
NODE_NAME="${NODE_NAME:-$(hostname)}"
JOIN_TIMEOUT="${JOIN_TIMEOUT:-60}"
JOIN_RECONCILE_GRACE="${JOIN_RECONCILE_GRACE:-5}"
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

JOIN_FAILURE_MARKER="/shared/$NODE_NAME.join-failed"

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

# A listening socket is not enough: the daemon binds before its service host and
# control accept loop are ready. Wait for the exact main-daemon status command to
# complete successfully so daemon-required cluster mutations do not race startup.
LOCAL_DAEMON_READY_FAILURE=""
wait_for_local_daemon_status() {
  local timeout="${1:-$JOIN_TIMEOUT}" started elapsed remaining probe_timeout
  local status_output="" status=0
  started="$SECONDS"
  LOCAL_DAEMON_READY_FAILURE=""

  if ! wait_for_tcp "127.0.0.1:$MAIN_PORT" "$timeout"; then
    LOCAL_DAEMON_READY_FAILURE="local daemon TCP port :$MAIN_PORT did not open within ${timeout}s"
    return 1
  fi

  while :; do
    elapsed=$((SECONDS - started))
    if [ "$elapsed" -ge "$timeout" ]; then
      break
    fi
    remaining=$((timeout - elapsed))
    probe_timeout="$CLUSTER_STATUS_TIMEOUT"
    if [ "$probe_timeout" -gt "$remaining" ]; then
      probe_timeout="$remaining"
    fi

    status_output="$(timeout --foreground "${probe_timeout}s" "$BIN" daemon status 2>&1)"
    status=$?
    if [ "$status" -eq 0 ] \
      && printf '%s\n' "$status_output" | grep -Fxq "sinomo daemon is running"; then
      return 0
    fi

    elapsed=$((SECONDS - started))
    [ "$elapsed" -ge "$timeout" ] && break
    sleep "$JOIN_WAIT_INTERVAL"
  done

  LOCAL_DAEMON_READY_FAILURE="local daemon did not pass 'sinomo daemon status' within ${timeout}s (last exit $status)"
  if [ -n "$status_output" ]; then
    LOCAL_DAEMON_READY_FAILURE="$LOCAL_DAEMON_READY_FAILURE: $status_output"
  fi
  return 1
}

cluster_join_status() {
  local timeout_seconds="${1:-$CLUSTER_STATUS_TIMEOUT}"
  timeout --foreground "${timeout_seconds}s" "$BIN" cluster status 2>/dev/null \
    | awk '/^status:/{print $2; exit}'
}

cluster_state_id() {
  local timeout_seconds="${1:-$CLUSTER_STATUS_TIMEOUT}"
  timeout --foreground "${timeout_seconds}s" "$BIN" cluster status 2>/dev/null \
    | awk '/^cluster:/{print $2; exit}'
}

local_daemon_pid() {
  local timeout_seconds="${1:-$CLUSTER_STATUS_TIMEOUT}"
  timeout --foreground "${timeout_seconds}s" "$BIN" daemon status 2>/dev/null \
    | awk '/^  pid:/{print $2; exit}'
}

# A successful join replaces the cluster identity captured by the boot-time
# daemon runtime. The daemon drains after sending the response and the shadow
# supervisor starts a replacement. Do not publish this node as an introducer
# until that replacement has a new PID and advertises the accepted disk state.
LOCAL_DAEMON_ACTIVATION_FAILURE=""
wait_for_local_daemon_cluster_activation() {
  local previous_pid="$1" timeout="${2:-$JOIN_TIMEOUT}"
  local started elapsed remaining probe_timeout expected_cluster status_output
  local current_pid current_cluster network_output="" status=0
  started="$SECONDS"
  LOCAL_DAEMON_ACTIVATION_FAILURE=""
  expected_cluster="$(cluster_state_id "$CLUSTER_STATUS_TIMEOUT" || true)"
  if [ -z "$expected_cluster" ]; then
    LOCAL_DAEMON_ACTIVATION_FAILURE="accepted cluster state did not report a cluster id"
    return 1
  fi

  while :; do
    elapsed=$((SECONDS - started))
    [ "$elapsed" -lt "$timeout" ] || break
    remaining=$((timeout - elapsed))
    probe_timeout="$CLUSTER_STATUS_TIMEOUT"
    if [ "$probe_timeout" -gt "$remaining" ]; then
      probe_timeout="$remaining"
    fi
    status_output="$(timeout --foreground "${probe_timeout}s" "$BIN" daemon status 2>&1)"
    status=$?
    current_pid="$(printf '%s\n' "$status_output" | awk '/^  pid:/{print $2; exit}')"
    current_cluster="$(printf '%s\n' "$status_output" | awk '/^  cluster:/{print $2; exit}')"
    if [ "$status" -eq 0 ] \
      && [ -n "$current_pid" ] \
      && [ "$current_pid" != "$previous_pid" ] \
      && [ "$current_cluster" = "$expected_cluster" ]; then
      network_output="$(timeout --foreground "${probe_timeout}s" \
        "$BIN" test run cluster.network.ping-benchmark --require-cluster --json \
          --param requests-per-target=1 \
          --param warmup-requests-per-target=0 \
          --param concurrency=4 \
          --param timeout-ms=2000 \
          --param max-failures=0 \
          --param max-p50-ms=0 \
          --param max-p95-ms=0 \
          --param max-p50-us=0 \
          --param max-p95-us=0 \
          --param min-requests-per-second=0 2>&1)"
      if [ "$?" -eq 0 ]; then
        log "activated joined cluster $current_cluster in network-ready replacement daemon pid $current_pid"
        return 0
      fi
    fi
    sleep "$JOIN_WAIT_INTERVAL"
  done

  LOCAL_DAEMON_ACTIVATION_FAILURE="replacement daemon did not advertise accepted cluster $expected_cluster within ${timeout}s (previous pid ${previous_pid:-unknown}, last exit $status)"
  if [ -n "${status_output:-}" ]; then
    LOCAL_DAEMON_ACTIVATION_FAILURE="$LOCAL_DAEMON_ACTIVATION_FAILURE: $status_output"
  fi
  if [ -n "$network_output" ]; then
    LOCAL_DAEMON_ACTIVATION_FAILURE="$LOCAL_DAEMON_ACTIVATION_FAILURE; last network readiness probe: $network_output"
  fi
  return 1
}

cluster_join_already_accepted() {
  local status probe_timeout="${1:-$CLUSTER_STATUS_TIMEOUT}"
  [ -f "$DATA/cluster/state.sobj" ] || return 1
  status="$(cluster_join_status "$probe_timeout" || true)"
  case "$status" in
    voter|read-only)
      log "cluster state already accepted locally (status: $status); skipping join"
      return 0
      ;;
  esac
  return 1
}

# A join registration can commit at the authority even when the joining command loses the
# accepted response. Observe local state for that committed outcome without resending the join.
# The grace is always capped by the original end-to-end JOIN_TIMEOUT budget.
wait_for_committed_join_reconciliation() {
  local join_started="$1" elapsed remaining grace deadline probe_timeout
  elapsed=$((SECONDS - join_started))
  remaining=$((JOIN_TIMEOUT - elapsed))
  [ "$remaining" -gt 0 ] || return 1

  grace="$JOIN_RECONCILE_GRACE"
  [ "$grace" -gt 0 ] 2>/dev/null || return 1
  if [ "$grace" -gt "$remaining" ]; then
    grace="$remaining"
  fi
  deadline=$((SECONDS + grace))
  log "join command failed; observing local state for an already-committed join (<=${grace}s, no resend)"

  while :; do
    remaining=$((deadline - SECONDS))
    [ "$remaining" -gt 0 ] || return 1
    probe_timeout="$CLUSTER_STATUS_TIMEOUT"
    if [ "$probe_timeout" -gt "$remaining" ]; then
      probe_timeout="$remaining"
    fi
    if cluster_join_already_accepted "$probe_timeout"; then
      return 0
    fi
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep "$JOIN_WAIT_INTERVAL"
  done
}

publish_join_ready() {
  rm -f "$JOIN_FAILURE_MARKER" 2>/dev/null || true
  [ -d "$JOIN_READY_DIR" ] || return
  : > "$JOIN_READY_DIR/$NODE_NAME.joined" 2>/dev/null || true
}

write_join_failure() {
  local reason="$1" detail="${2:-}" marker_tmp="$JOIN_FAILURE_MARKER.tmp.$$"
  mkdir -p /shared 2>/dev/null || true
  if {
    printf '%s\n' "$reason"
    [ -z "$detail" ] || printf '%s\n' "$detail"
  } > "$marker_tmp" 2>/dev/null && mv -f "$marker_tmp" "$JOIN_FAILURE_MARKER"; then
    log "join failure marker: $JOIN_FAILURE_MARKER"
  else
    rm -f "$marker_tmp" 2>/dev/null || true
    log "WARN: failed to write join failure marker $JOIN_FAILURE_MARKER"
  fi
}

fail_join() {
  local reason="$1" detail="${2:-}"
  log "WARN: $reason"
  if [ -n "$detail" ]; then
    log "join command output follows:"
    printf '%s\n' "$detail"
  fi
  write_join_failure "$reason" "$detail"
}

JOIN_WAIT_FAILURE_REASON=""
wait_for_introducer_ready() {
  [ "$WAIT_FOR_INTRODUCER_READY" = "1" ] || return 0
  local introducer_host marker
  introducer_host="${INTRODUCER%%:*}"
  [ -n "$introducer_host" ] || return 0
  marker="$JOIN_READY_DIR/$introducer_host.joined"
  if ! wait_for_file "$marker" "$JOIN_TIMEOUT"; then
    JOIN_WAIT_FAILURE_REASON="introducer readiness marker $marker did not appear (<=${JOIN_TIMEOUT}s); staying unclustered"
    return 1
  fi
}

wait_for_join_ready_nodes() {
  local node marker
  for node in $WAIT_FOR_JOIN_READY_NODES; do
    marker="$JOIN_READY_DIR/$node.joined"
    if ! wait_for_file "$marker" "$JOIN_TIMEOUT"; then
      JOIN_WAIT_FAILURE_REASON="join dependency marker $marker did not appear (<=${JOIN_TIMEOUT}s); staying unclustered"
      return 1
    fi
  done
}

write_invite_failure_marker() {
  local introducer="$1" output="$2" reason="$3" detail="${4:-}"
  local marker="$output.failed" marker_tmp="$output.failed.tmp.$$"
  mkdir -p "$(dirname "$marker")" 2>/dev/null || true
  if {
    printf 'introducer: %s\n' "$introducer"
    printf '%s\n' "$reason"
    [ -z "$detail" ] || printf '%s\n' "$detail"
  } > "$marker_tmp" 2>/dev/null && mv -f "$marker_tmp" "$marker"; then
    log "invite failure marker: $marker"
  else
    rm -f "$marker_tmp" 2>/dev/null || true
    log "WARN: failed to write invite failure marker $marker"
  fi
}

mark_all_invites_failed() {
  local reason="$1" spec introducer output
  for spec in ${PUBLISH_INVITES:-}; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    write_invite_failure_marker "$introducer" "$output" "$reason"
  done
}

publish_invites_once() {
  local spec introducer output command_output command_status marker reason
  [ -n "${PUBLISH_INVITES:-}" ] || return 0
  for spec in $PUBLISH_INVITES; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    marker="$output.failed"
    log "publishing join invite (introducer $introducer) -> $output"
    command_output="$("$BIN" cluster invite --introducer "$introducer" --ttl-seconds 86400 --output "$output" 2>&1)"
    command_status=$?
    if [ -n "$command_output" ]; then
      printf '%s\n' "$command_output"
    fi
    if [ "$command_status" -eq 0 ] && [ -f "$output" ]; then
      rm -f "$marker" 2>/dev/null || true
      continue
    fi

    if [ "$command_status" -eq 0 ]; then
      reason="cluster invite returned success but did not create $output"
    else
      reason="cluster invite exited with status $command_status"
    fi
    log "WARN: failed to publish invite for introducer $introducer: $reason"
    write_invite_failure_marker "$introducer" "$output" "$reason" "$command_output"
  done
}

publish_invites_once_in_background() {
  if ! wait_for_local_daemon_status "$JOIN_TIMEOUT"; then
    log "WARN: $LOCAL_DAEMON_READY_FAILURE; not publishing join invites"
    mark_all_invites_failed "$LOCAL_DAEMON_READY_FAILURE"
    return
  fi
  publish_invites_once
}

# Join the cluster once in the background AFTER our own daemon is listening. This is
# intentionally one-shot: after a command failure, the bounded reconciliation check only
# observes whether that same join was already committed; it never resends registration.
join_once_in_background() {
  local started elapsed output command_status pre_join_pid activation_timeout
  started="$SECONDS"
  if ! wait_for_local_daemon_status "$JOIN_TIMEOUT"; then
    fail_join "$LOCAL_DAEMON_READY_FAILURE; staying unclustered"
    return
  fi
  if [ -z "${JOIN_INVITE:-}" ] || [ -z "${INTRODUCER:-}" ]; then
    fail_join "no JOIN_INVITE/INTRODUCER set; staying unclustered"
    return
  fi
  if ! wait_for_file "$JOIN_INVITE" "$JOIN_TIMEOUT"; then
    fail_join "invite $JOIN_INVITE never appeared (<=${JOIN_TIMEOUT}s); staying unclustered"
    return
  fi
  if ! wait_for_tcp "$INTRODUCER" "$JOIN_TIMEOUT"; then
    fail_join "introducer $INTRODUCER unreachable (<=${JOIN_TIMEOUT}s); staying unclustered"
    return
  fi
  if ! wait_for_introducer_ready; then
    fail_join "$JOIN_WAIT_FAILURE_REASON"
    return
  fi
  if ! wait_for_join_ready_nodes; then
    fail_join "$JOIN_WAIT_FAILURE_REASON"
    return
  fi
  log "joining cluster as '$NODE_NAME' via introducer $INTRODUCER"
  if cluster_join_already_accepted; then
    publish_join_ready
    return
  fi
  pre_join_pid="$(local_daemon_pid "$CLUSTER_STATUS_TIMEOUT" || true)"
  if [ -z "$pre_join_pid" ]; then
    fail_join "could not capture the local daemon pid before join; staying unclustered"
    return
  fi
  output="$("$BIN" cluster join "$JOIN_INVITE" --name "$NODE_NAME" 2>&1)"
  command_status=$?
  if [ "$command_status" -eq 0 ]; then
    printf '%s\n' "$output"
    elapsed=$((SECONDS - started))
    activation_timeout=$((JOIN_TIMEOUT - elapsed))
    if [ "$activation_timeout" -le 0 ] \
      || ! wait_for_local_daemon_cluster_activation "$pre_join_pid" "$activation_timeout"; then
      fail_join "join committed but the replacement daemon did not activate within the ${JOIN_TIMEOUT}s join budget" "$LOCAL_DAEMON_ACTIVATION_FAILURE"
      return
    fi
    elapsed=$((SECONDS - started))
    log "joined cluster in ${elapsed}s"
    publish_join_ready
    return
  fi
  if wait_for_committed_join_reconciliation "$started"; then
    elapsed=$((SECONDS - started))
    activation_timeout=$((JOIN_TIMEOUT - elapsed))
    if [ "$activation_timeout" -le 0 ] \
      || ! wait_for_local_daemon_cluster_activation "$pre_join_pid" "$activation_timeout"; then
      fail_join "join reconciled as accepted but the replacement daemon did not activate within the ${JOIN_TIMEOUT}s join budget" "$LOCAL_DAEMON_ACTIVATION_FAILURE"
      return
    fi
    elapsed=$((SECONDS - started))
    log "join command exited with status $command_status, but the same join reconciled as accepted after ${elapsed}s"
    publish_join_ready
    return
  fi
  elapsed=$((SECONDS - started))
  fail_join "cluster join via $INTRODUCER failed with status $command_status after ${elapsed}s; staying unclustered" "$output"
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
