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
#   MAIN_PORT           main listener port (default 21353)
#   SHADOW_PORT         shadow listener port (default 21354)
#   CLUSTER_NAME        cluster name (seed only)
#   INTRODUCER          reachable host:port to join through (joiners)
#   JOIN_INVITE         invite file to read when joining (joiners)
#   PUBLISH_INVITE      invite file to (re)publish with PUBLISH_INTRODUCER (seed + bridges)
#   PUBLISH_INTRODUCER  introducer host:port to advertise in the published invite
#   SINOMO_DISABLE_IROH "1" => run on direct LAN transports only (default here)
#   SINOMO_DFS_METADATA_CHECKPOINT_COORDINATION_LOCK optional host-local advisory
#                        lock shared by co-located DFS metadata replicas
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
SHADOW_PORT="${SHADOW_PORT:-21354}"
JOIN_READY_DIR="${JOIN_READY_DIR:-/shared}"
WAIT_FOR_INTRODUCER_READY="${WAIT_FOR_INTRODUCER_READY:-0}"
WAIT_FOR_JOIN_READY_NODES="${WAIT_FOR_JOIN_READY_NODES:-}"
FORMATION_OBSERVE="${SINOMO_TEST_FORMATION_OBSERVE:-1}"
FORMATION_OBSERVE_DURATION_SECONDS="${SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS:-12}"
FORMATION_OBSERVE_FREQUENCY_HZ="${SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ:-49}"
FORMATION_OBSERVE_CPU_NODES="${SINOMO_TEST_FORMATION_OBSERVE_CPU_NODES:-all}"
FORMATION_OBSERVE_MAX_EVENTS="${SINOMO_TEST_FORMATION_OBSERVE_MAX_EVENTS:-32768}"
FORMATION_OBSERVE_FILTER="${SINOMO_TEST_FORMATION_OBSERVE_FILTER:-debug,sinomo.perf=trace,sinomo.cluster.formation=trace,sinomo.cluster.health=trace,sinomo.daemon.cluster_network_maintenance=trace,sinomo.daemon.cluster_member_publish=trace,sinomo.daemon.node_status_publish=trace,sinomo.cluster.raft_fanout=trace}"

export SINOMO_DATA_DIR="$DATA"
export SINOMO_DFS_PERF_TRACE="${SINOMO_DFS_PERF_TRACE:-1}"
export SINOMO_OBSERVE_TRACE_RECORDS="${SINOMO_OBSERVE_TRACE_RECORDS:-32768}"
export SINOMO_OBSERVE_FILTER="${SINOMO_OBSERVE_FILTER:-debug,sinomo.perf=trace}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
# All dev-cluster nodes share /shared. Serialize the memory-heavy snapshot fold
# across these co-located replicas so a replay-pressure wave cannot multiply
# full-table decode/encode memory and stall foreground namespace writes.
export SINOMO_DFS_METADATA_CHECKPOINT_COORDINATION_LOCK="${SINOMO_DFS_METADATA_CHECKPOINT_COORDINATION_LOCK:-/shared/dfs-metadata-checkpoint-fold.lock}"
# This segmented test cluster runs with iroh off by default so segmentation is real.
export SINOMO_DISABLE_IROH="${SINOMO_DISABLE_IROH:-1}"
export SINOMO_DAEMON_ADVERTISE_HOST="${SINOMO_DAEMON_ADVERTISE_HOST:-$NODE_NAME}"
# Install the diagnostic filter before the shadow supervisor starts either the
# initial or identity-replacement daemon. The ordinary dev cluster keeps the
# Docker debug filter. All verification gates run with tracing enabled;
# disabling it is an explicit diagnostic override, not acceptance.
if [ "$FORMATION_OBSERVE" = 1 ]; then
  export SINOMO_OBSERVE_FILTER="$FORMATION_OBSERVE_FILTER"
fi

log() { echo "[entrypoint $(hostname)] $*"; }

formation_cpu_profile_enabled() {
  local configured="${FORMATION_OBSERVE_CPU_NODES//[[:space:]]/}"
  case ",$configured," in
    *,all,*|*,"$NODE_NAME",*) return 0 ;;
    *) return 1 ;;
  esac
}

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
# A PID 1 replacement kills the old finalization helper with the container.
# The staged image completes its authorized promotion through rollout-owned
# boot recovery, retaining the previous binary and consuming the stage.
if [ -f "$DATA/rollout/pending-shadow-rollout.sobj" ] && [ -x "$DATA/rollout/staged/sinomo-shadow" ]; then
  SHADOW_BIN="$DATA/rollout/staged/sinomo-shadow"
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

local_cluster_state_id() {
  local state_file="$DATA/cluster/state.sobj"
  [ -f "$state_file" ] || return 1
  # `cluster status` decorates local identity with live cluster reads and can
  # consume a full transport timeout on a freshly joined deep-segment node.
  # Decode only the durable local state that the completed join just wrote.
  "$BIN" debug sobj "$state_file" 2>/dev/null \
    | sed -n 's/.*payload={"cluster_id": "\([^"]*\)".*/\1/p'
}

local_daemon_pid() {
  local excluded_pid="${1:-}"
  # `daemon status` performs a live RPC and decorates the result with cluster
  # state. A freshly joined deep-segment node can therefore spend its complete
  # transport timeout here even though the replacement process already wrote
  # status and bound its listener. Process identity is all callers need. Skip
  # the retiring process explicitly because it may briefly overlap its
  # replacement in the process table.
  ps -eo pid=,args= 2>/dev/null \
    | awk -v bin="$BIN" -v excluded_pid="$excluded_pid" '
        $1 != excluded_pid && $2 == bin && $3 == "daemon" && $4 == "run" {
          print $1
          exit
        }
      '
}

# Run bounded observe RPCs off the formation dependency path. Even local
# observation RPCs can queue behind the work being measured, so synchronously
# waiting for them here would delay the next bridge's readiness marker and
# contaminate the ten-second Docker-only acceptance measurement.
run_formation_observation() {
  local pid="$1" capture="$2" observe_dir="$3" command_timeout=10
  local trace_pid startup_trace_pid timeline_pid profile_pid
  timeout --foreground "${command_timeout}s" "$BIN" observe control \
    --capture "$capture" --filter "$FORMATION_OBSERVE_FILTER" --json \
    > "$observe_dir/$capture.trace-control.json" 2> "$observe_dir/$capture.trace-control.stderr.log" \
    || log "WARN: formation trace capture did not start for daemon pid $pid" &
  trace_pid=$!
  # Capture the complete process-local ring without a capture-id filter. It
  # includes startup records emitted before the asynchronous control RPC could
  # attach `$capture`, and remains on disk if this daemon exits during join.
  timeout --foreground "${command_timeout}s" "$BIN" observe traces \
    --limit 4096 --json \
    > "$observe_dir/$capture.startup-traces.json" \
    2> "$observe_dir/$capture.startup-traces.stderr.log" \
    || log "WARN: formation startup traces were unavailable for daemon pid $pid" &
  startup_trace_pid=$!
  timeout --foreground "${command_timeout}s" "$BIN" observe timeline network start \
    --capture "$capture" --duration-seconds "$FORMATION_OBSERVE_DURATION_SECONDS" \
    --max-events "$FORMATION_OBSERVE_MAX_EVENTS" --json \
    > "$observe_dir/$capture.timeline-start.json" 2> "$observe_dir/$capture.timeline-start.stderr.log" \
    || log "WARN: formation network timeline did not start for daemon pid $pid" &
  timeline_pid=$!
  profile_pid=""
  if formation_cpu_profile_enabled; then
    timeout --foreground "${command_timeout}s" "$BIN" observe profile cpu start \
      --capture "$capture" --duration-seconds "$FORMATION_OBSERVE_DURATION_SECONDS" \
      --frequency-hz "$FORMATION_OBSERVE_FREQUENCY_HZ" --json \
      > "$observe_dir/$capture.profile-start.json" 2> "$observe_dir/$capture.profile-start.stderr.log" \
      || log "WARN: formation CPU profile did not start for daemon pid $pid" &
    profile_pid=$!
  else
    log "skipping in-process formation CPU profile for daemon pid $pid"
  fi

  wait "$trace_pid" "$startup_trace_pid" "$timeline_pid" 2>/dev/null || true
  if [ -n "$profile_pid" ]; then
    wait "$profile_pid" 2>/dev/null || true
  fi
  : > "$observe_dir/$capture.ready"

  log "started bounded formation observation capture $capture"
}

# Default Docker observation capture. The production daemon has no Docker
# formation deadline; this helper simply brackets the dev cluster's
# formation window with the ordinary observe RPCs. It schedules once per main
# daemon PID, including identity-changing replacement daemons, without making
# observer startup a prerequisite for downstream cluster formation.
start_formation_observation() {
  [ "$FORMATION_OBSERVE" = 1 ] || return 0
  local pid="${1:-}" capture observe_dir current_pid_file generation
  if [ -z "$pid" ]; then
    pid="$(local_daemon_pid || true)"
  fi
  if [ -z "$pid" ]; then
    log "WARN: formation observation could not resolve the main daemon pid"
    return 0
  fi
  observe_dir="$DATA/observe/formation"
  current_pid_file="$observe_dir/current-daemon-pid"
  generation="$(awk '{print $22}' "/proc/$pid/stat" 2>/dev/null)"
  [ -n "$generation" ] || return 0
  if [ -f "$current_pid_file" ] \
    && [ "$(sed -n '1p' "$current_pid_file" 2>/dev/null)" = "$pid:$generation" ]; then
    return 0
  fi
  mkdir -p "$observe_dir" || return 0
  # Container PID namespaces reuse PIDs across restarts. Include process start
  # ticks and claim the generation atomically across the readiness helpers.
  mkdir "$observe_dir/.start-$pid-$generation" 2>/dev/null || return 0
  capture="formation-${NODE_NAME}-${pid}-${generation}"

  # Publish the intent before spawning so concurrent readiness paths cannot
  # schedule duplicate captures for the same daemon generation.
  printf '%s\n' "$capture" > "$observe_dir/current-capture"
  printf '%s\n' "$pid:$generation" > "$current_pid_file"
  printf '%s\t%s\n' "$capture" "$pid" >> "$observe_dir/captures.tsv"
  run_formation_observation "$pid" "$capture" "$observe_dir" &
  log "scheduled bounded formation observation capture $capture"
}

local_daemon_status_contains_cluster() {
  local cluster_id="$1" status_file="$DATA/daemon/status.sobj"
  [ -n "$cluster_id" ] \
    && [ -f "$status_file" ] \
    && grep -aFq "$cluster_id" "$status_file"
}

# A successful join replaces the cluster identity captured by the boot-time
# daemon runtime. The daemon drains after sending the response and the shadow
# supervisor starts a replacement. Do not publish this node as an introducer
# until that replacement has a new PID and advertises the accepted disk state.
LOCAL_DAEMON_ACTIVATION_FAILURE=""
wait_for_local_daemon_cluster_activation() {
  local previous_pid="$1" timeout="${2:-$JOIN_TIMEOUT}"
  local started elapsed remaining expected_cluster current_pid
  started="$SECONDS"
  LOCAL_DAEMON_ACTIVATION_FAILURE=""
  expected_cluster="$(local_cluster_state_id || true)"
  if [ -z "$expected_cluster" ]; then
    LOCAL_DAEMON_ACTIVATION_FAILURE="accepted cluster state did not report a cluster id"
    return 1
  fi

  while :; do
    elapsed=$((SECONDS - started))
    [ "$elapsed" -lt "$timeout" ] || break
    remaining=$((timeout - elapsed))
    current_pid="$(local_daemon_pid "$previous_pid" || true)"
    if [ -n "$current_pid" ] \
      && [ "$current_pid" != "$previous_pid" ] \
      && local_daemon_status_contains_cluster "$expected_cluster"; then
      elapsed=$((SECONDS - started))
      remaining=$((timeout - elapsed))
      if [ "$remaining" -le 0 ] \
        || ! wait_for_tcp "127.0.0.1:$MAIN_PORT" "$remaining"; then
        break
      fi
      # Use the replacement PID proven above. Re-resolving without excluding
      # the retiring process can select its lower PID during the brief overlap.
      start_formation_observation "$current_pid"
      # This marker gates the next hop's join, not steady-state acceptance.
      # Prove only that this replacement has bound its local control listener;
      # the outer cluster test owns the single end-to-end health/doctor gate.
      log "activated joined cluster $expected_cluster in listening replacement daemon pid $current_pid"
      return 0
    fi
    sleep "$JOIN_WAIT_INTERVAL"
  done

  LOCAL_DAEMON_ACTIVATION_FAILURE="replacement daemon did not publish accepted cluster $expected_cluster and bind its listener within ${timeout}s (previous pid ${previous_pid:-unknown}, current pid ${current_pid:-unknown})"
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
  local spec introducer output staged command_output command_status marker reason
  local invite_count=0 batch_started_ms batch_finished_ms batch_elapsed_ms
  local publish_failed=0
  local -a publish_args=()
  [ -n "${PUBLISH_INVITES:-}" ] || return 0

  # One durable invite may advertise multiple introducers. Ask the daemon to
  # publish that invite once, then have the CLI project an exact one-channel
  # compact reference into each staged file. This keeps route-specific join
  # files without repeating the authoritative barrier and quorum transaction.
  for spec in $PUBLISH_INVITES; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    staged="$output.pending.$$"
    invite_count=$((invite_count + 1))
    log "staging join invite channel ordinal=$invite_count introducer=$introducer output=$output"
    rm -f "$staged" 2>/dev/null || true
    publish_args+=(--channel-output "$introducer=$staged")
  done

  batch_started_ms="$(date +%s%3N 2>/dev/null || true)"
  log "publishing join invite batch phase=cli_call batch_size=$invite_count"
  command_output="$("$BIN" cluster invite --ttl-seconds 86400 "${publish_args[@]}" 2>&1)"
  command_status=$?
  batch_finished_ms="$(date +%s%3N 2>/dev/null || true)"
  batch_elapsed_ms=""
  if [[ "$batch_started_ms" =~ ^[0-9]+$ ]] && [[ "$batch_finished_ms" =~ ^[0-9]+$ ]]; then
    batch_elapsed_ms=$((batch_finished_ms - batch_started_ms))
  fi
  log "published join invite batch phase=cli_call batch_size=$invite_count status=$command_status duration_ms=${batch_elapsed_ms:-unknown}"
  if [ -n "$command_output" ]; then
    printf '%s\n' "$command_output"
  fi

  for spec in $PUBLISH_INVITES; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    staged="$output.pending.$$"
    if [ "$command_status" -eq 0 ] && [ -f "$staged" ]; then
      continue
    fi
    if [ "$command_status" -eq 0 ]; then
      reason="cluster invite returned success but did not create its staged output"
    else
      reason="cluster invite exited with status $command_status"
    fi
    log "WARN: failed to publish invite for introducer $introducer: $reason"
    write_invite_failure_marker "$introducer" "$output" "$reason" "$command_output"
    publish_failed=1
  done

  if [ "$publish_failed" -ne 0 ]; then
    for spec in $PUBLISH_INVITES; do
      output="${spec##*=}"
      rm -f "$output.pending.$$" 2>/dev/null || true
    done
    return 1
  fi

  for spec in $PUBLISH_INVITES; do
    introducer="${spec%%=*}"; output="${spec##*=}"
    staged="$output.pending.$$"
    marker="$output.failed"
    if mv -f "$staged" "$output"; then
      rm -f "$marker" 2>/dev/null || true
      continue
    fi
    reason="could not atomically expose staged invitation at $output"
    log "WARN: failed to publish invite for introducer $introducer: $reason"
    write_invite_failure_marker "$introducer" "$output" "$reason"
    publish_failed=1
  done

  [ "$publish_failed" -eq 0 ]
}

publish_invites_once_in_background() {
  if ! wait_for_local_daemon_status "$JOIN_TIMEOUT"; then
    log "WARN: $LOCAL_DAEMON_READY_FAILURE; not publishing join invites"
    mark_all_invites_failed "$LOCAL_DAEMON_READY_FAILURE"
    return
  fi
  start_formation_observation
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
  if ! wait_for_introducer_ready; then
    fail_join "$JOIN_WAIT_FAILURE_REASON"
    return
  fi
  if ! wait_for_join_ready_nodes; then
    fail_join "$JOIN_WAIT_FAILURE_REASON"
    return
  fi
  # The readiness marker may follow an identity-changing daemon restart. Probe
  # after observing it so this one-shot mutation never relies on a connection
  # check that succeeded against the introducer's retired process.
  elapsed=$((SECONDS - started))
  activation_timeout=$((JOIN_TIMEOUT - elapsed))
  if [ "$activation_timeout" -le 0 ] \
    || ! wait_for_tcp "$INTRODUCER" "$activation_timeout"; then
    fail_join "introducer $INTRODUCER unreachable after its readiness marker within the ${JOIN_TIMEOUT}s join budget; staying unclustered"
    return
  fi
  log "joining cluster as '$NODE_NAME' via introducer $INTRODUCER"
  if cluster_join_already_accepted; then
    publish_join_ready
    return
  fi
  pre_join_pid="$(local_daemon_pid || true)"
  if [ -z "$pre_join_pid" ]; then
    fail_join "could not capture the local daemon pid before join; staying unclustered"
    return
  fi
  # Downstream nodes may wait several seconds for a bridge readiness marker.
  # Start the old-process profile at the actual mutation boundary so its
  # bounded window measures join work instead of expiring during dependency
  # waiting. The accepted replacement gets its own capture after activation.
  start_formation_observation "$pre_join_pid"
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

  # The seed (a voter) publishes one multi-channel invite and projects one compact
  # reference for each segment, naming its reachable bridge. PUBLISH_INVITES is a
  # space-separated list of "introducer-host:port=output-file". Defer publication
  # until the daemon is reachable so node1 can answer health and forwarded join traffic.
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
if [ "$FORMATION_OBSERVE" = 1 ]; then
  # Retained clusters skip join/invite readiness helpers. Observe their normal
  # startup too, without making capture startup a dependency of daemon boot.
  (
    if wait_for_local_daemon_status "$JOIN_TIMEOUT"; then
      start_formation_observation
    else
      log "WARN: $LOCAL_DAEMON_READY_FAILURE; startup observation unavailable"
    fi
  ) &
fi

# Run the shadow supervisor as PID 1; it starts and supervises the main daemon.
export SINOMO_SHADOW_MAIN_BINARY="$BIN"
export SINOMO_SHADOW_MAIN_HOST=0.0.0.0
export SINOMO_SHADOW_MAIN_PORT="$MAIN_PORT"
export SINOMO_SHADOW_START_DELAY_MS=0
log "starting shadow supervisor $SHADOW_BIN on 0.0.0.0:$SHADOW_PORT (main $BIN on 0.0.0.0:$MAIN_PORT, iroh disabled=$SINOMO_DISABLE_IROH)"
exec "$SHADOW_BIN" daemon shadow run --host 0.0.0.0 --port "$SHADOW_PORT"
