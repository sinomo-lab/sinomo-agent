#!/usr/bin/env bash
# Measure the core DFS metadata algorithm on the already-running six-node
# Docker debug cluster. The workload resolves the live root/services/users
# shard layout, measures each primary in a sequential single-shard attribution
# window, then starts three remote client lanes concurrently to measure aggregate
# utilization. Every phase resolves and samples the exact main-daemon host PID
# separately from whole-container cgroup-v2 CPU/pressure/I/O/memory and
# network-namespace counters. Host available memory is sampled once per
# interval. Utilization gates use main-daemon process CPU; cgroup CPU remains
# diagnostic telemetry so helper/client activity cannot make a primary look
# saturated. Any cgroup OOM event fails the gate.
#
# This script never creates, restarts, or removes containers or volumes. Use
# docker/test-cluster.sh to form the cluster first. A one-byte payload minimizes
# content I/O; metadata remains durable unless the running compose project was
# explicitly created with tmpfs-backed /data.
#
# CPU ownership is exact once a sample is taken, but workload-window attribution
# is approximate: client `docker exec` process liveness and a warmup interval
# bracket remote work rather than server-side operation boundaries. Correlate
# `sinomo.dfs.perf` traces when diagnosing either edge of a short window.
# Process-state samples include the exact host PID's leader state and a snapshot
# count of every current daemon thread state. Context-switch snapshots sum those
# same Linux tasks; a worker that exits can make consecutive snapshot totals
# regress, so phase totals clamp that interval to zero instead of fabricating a
# negative count. The exact cumulative snapshots remain in timeline.csv.
# Each node's redirected `/data/daemon/main.stdout.log` and `main.stderr.log`
# are retained as exact byte ranges appended between measurement start and
# workload completion. The manifests record reset/full-log fallbacks; capture
# is best effort so an unavailable diagnostic cannot hide the primary workload
# result. Captured stderr is scanned separately for panics, fatal process
# failures, DFS failures, and retry-path anomalies. Expected transient
# control-plane diagnostics (for example a client-side broken pipe or a
# join/health probe timeout) remain in the raw artifact but do not fail this
# focused main-daemon stderr scan. Existing `sinomo.dfs.perf` timing records in
# both streams are summarized without treating an absent record as zero.
#
# Important environment variables:
#   SINOMO_DFS_PERF_PROJECT                 compose project (sinomo-dfs-test)
#   SINOMO_DFS_PERF_COMPOSE_OVERRIDE        optional second compose file used to form the project
#   SINOMO_DFS_PERF_MODE                    debug-gate (default), release-ceiling, or sustained-churn
#   SINOMO_DFS_PERF_BINARY_PROFILE          debug (default) or release; release-ceiling requires release
#   SINOMO_DFS_PERF_EXPECT_NAMESPACE_PREPARE_CODEC
#                                             required live shard codec (packed-v1)
#   SINOMO_DFS_PERF_FILES                   files per shard lane per phase (mode default)
#   SINOMO_DFS_PERF_BYTES                   bytes per file (1)
#   SINOMO_DFS_PERF_SUSTAINED_ROUNDS        extra back-to-back three-lane rounds (mode default)
#   SINOMO_DFS_PERF_MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND
#                                             sustained end-to-end cluster floor (1000)
#   SINOMO_DFS_PERF_BATCH_SIZE_SWEEP        comma-separated files-per-bulk-call full-gate sweep
#   SINOMO_DFS_PERF_MIN_FILES_PER_SECOND    default isolated-write/aggregate floor (1000)
#   SINOMO_DFS_PERF_MIN_ISOLATED_FILES_PER_SECOND
#                                             required bulk write+commit publications/s per isolated lane
#   SINOMO_DFS_PERF_MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND
#                                             required end-to-end concurrent cluster publications/s
#   SINOMO_DFS_PERF_MIN_CONCURRENT_LANE_FILES_PER_SECOND
#                                             optional bulk write+commit concurrent per-lane floor (0)
#   SINOMO_DFS_PERF_SAMPLE_SECONDS          sample interval (0.05)
#   SINOMO_DFS_PERF_MIN_PRIMARY_CPU_PERCENT isolated diagnostic/aggregate CPU floor (80)
#   SINOMO_DFS_PERF_MIN_LANE_MS             minimum measured lane window (500)
#   SINOMO_DFS_PERF_MAX_IDLE_MS             max low-CPU hot-path streak (100)
#   SINOMO_DFS_PERF_WARMUP_MS               launch/grace interval (250)
#   SINOMO_DFS_PERF_TIMEOUT_SECONDS         per-lane timeout (300)
#   SINOMO_DFS_PERF_CONVERGENCE_SECONDS     final clean-status wait (60)
#   SINOMO_DFS_PERF_ARTIFACTS               output directory
#
# `cluster.dfs.bulk-write` must expose probe-level retry_count diagnostics. Any
# retry, command failure, cgroup throttle, an avoidable ready-work low-CPU
# streak (runnable daemon threads with no observed CPU/I/O pressure or
# storage/network progress), sub-threshold isolated bulk write throughput, or
# sub-threshold concurrent cluster throughput/utilization fails this gate.
# Sleeping and externally progressing low-CPU samples remain evidence, not
# inferred faults.
#
# `--summarize-traces ARTIFACT_DIR` rebuilds only the bounded
# `sinomo.dfs.perf` timing summaries from previously captured daemon logs. It
# does not contact Docker or alter an existing gate result.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$SCRIPT_DIR/docker-compose.dev.yml}"
COMPOSE_OVERRIDE_FILE="${SINOMO_DFS_PERF_COMPOSE_OVERRIDE:-}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${SINOMO_DFS_PERF_PROJECT:-sinomo-dfs-test}}"
PERF_MODE="${SINOMO_DFS_PERF_MODE:-debug-gate}"
BINARY_PROFILE="${SINOMO_DFS_PERF_BINARY_PROFILE:-debug}"
EXPECTED_NAMESPACE_PREPARE_CODEC="${SINOMO_DFS_PERF_EXPECT_NAMESPACE_PREPARE_CODEC:-packed-v1}"
case "$PERF_MODE" in
  debug-gate)
    MODE_DEFAULT_FILES=1024
    MODE_DEFAULT_SUSTAINED_ROUNDS=0
    ;;
  release-ceiling)
    MODE_DEFAULT_FILES=16384
    MODE_DEFAULT_SUSTAINED_ROUNDS=0
    ;;
  sustained-churn)
    MODE_DEFAULT_FILES=4096
    MODE_DEFAULT_SUSTAINED_ROUNDS=8
    ;;
  *)
    printf '[dfs-shard-perf] ERROR: SINOMO_DFS_PERF_MODE must be debug-gate, release-ceiling, or sustained-churn; got %q\n' \
      "$PERF_MODE" >&2
    exit 1
    ;;
esac
FILES="${SINOMO_DFS_PERF_FILES:-$MODE_DEFAULT_FILES}"
BYTES="${SINOMO_DFS_PERF_BYTES:-1}"
SUSTAINED_ROUNDS="${SINOMO_DFS_PERF_SUSTAINED_ROUNDS:-$MODE_DEFAULT_SUSTAINED_ROUNDS}"
BATCH_SIZE_SWEEP="${SINOMO_DFS_PERF_BATCH_SIZE_SWEEP:-}"
DEFAULT_MIN_FILES_PER_SECOND="${SINOMO_DFS_PERF_MIN_FILES_PER_SECOND:-1000}"
MIN_ISOLATED_FILES_PER_SECOND="${SINOMO_DFS_PERF_MIN_ISOLATED_FILES_PER_SECOND:-$DEFAULT_MIN_FILES_PER_SECOND}"
MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND="${SINOMO_DFS_PERF_MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND:-$DEFAULT_MIN_FILES_PER_SECOND}"
MIN_CONCURRENT_LANE_FILES_PER_SECOND="${SINOMO_DFS_PERF_MIN_CONCURRENT_LANE_FILES_PER_SECOND:-0}"
MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND="${SINOMO_DFS_PERF_MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND:-$DEFAULT_MIN_FILES_PER_SECOND}"
SAMPLE_SECONDS="${SINOMO_DFS_PERF_SAMPLE_SECONDS:-0.05}"
MIN_PRIMARY_CPU_PERCENT="${SINOMO_DFS_PERF_MIN_PRIMARY_CPU_PERCENT:-80}"
MIN_LANE_MS="${SINOMO_DFS_PERF_MIN_LANE_MS:-500}"
MAX_IDLE_MS="${SINOMO_DFS_PERF_MAX_IDLE_MS:-100}"
WARMUP_MS="${SINOMO_DFS_PERF_WARMUP_MS:-250}"
TIMEOUT_SECONDS="${SINOMO_DFS_PERF_TIMEOUT_SECONDS:-300}"
CONVERGENCE_SECONDS="${SINOMO_DFS_PERF_CONVERGENCE_SECONDS:-60}"
CGROUP_ROOT="${SINOMO_CGROUP_ROOT:-/sys/fs/cgroup}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACTS="${SINOMO_DFS_PERF_ARTIFACTS:-$REPO_ROOT/target/dfs-shard-perf/$RUN_ID}"
NODES=(node1 node2 node3 node4 node5 node6)
LANES=(root services users)
SAMPLE_CLASSIFICATIONS=(
  cpu_active
  low_cpu_ready_cpu_pressure
  low_cpu_ready_io_pressure_or_progress
  low_cpu_ready_network_progress
  low_cpu_ready_no_resource_wait_or_progress
  low_cpu_waiting_io_pressure_or_progress
  low_cpu_sleeping_network_progress
  low_cpu_sleeping_cgroup_cpu_pressure
  low_cpu_sleeping_no_observed_external_progress
)
EXPECTED_REPLICA_COUNT=5
COMPOSE_FILES=(-f "$COMPOSE_FILE")
if [[ -n "$COMPOSE_OVERRIDE_FILE" ]]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE_FILE")
fi

usage() {
  sed -n '2,/^[[:space:]]*$/ {
    s/^# \{0,1\}//
    p
  }' "$0"
}

TRACE_SUMMARY_ONLY=0
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--summarize-traces" && $# -eq 2 ]]; then
  TRACE_SUMMARY_ONLY=1
  ARTIFACTS="$2"
elif [[ $# -ne 0 ]]; then
  usage >&2
  exit 2
fi

log() {
  printf '[dfs-shard-perf] %s\n' "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

require_uint() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer, got '$value'"
}

require_positive_uint() {
  local name="$1" value="$2"
  require_uint "$name" "$value"
  (( value > 0 )) || die "$name must be greater than zero"
}

summarize_dfs_perf_traces() {
  local artifacts="$1"
  local records="$artifacts/dfs-perf-timing-records.tsv"
  local summary="$artifacts/dfs-perf-timing-summary.tsv"
  local coverage="$artifacts/dfs-perf-timing-coverage.tsv"
  local status="$artifacts/dfs-perf-trace-summary-status.tsv"
  local node source artifact observed_logs=0 parse_failures=0 record_count

  mkdir -p "$artifacts"
  printf 'node\tsource\trecord\tmetric\tunit\tvalue\tline\n' >"$records"
  for node in "${NODES[@]}"; do
    for source in stdout stderr; do
      artifact="$artifacts/main-daemon-$node.workload.$source.log"
      [[ -f "$artifact" ]] || continue
      observed_logs=$((observed_logs + 1))
      if ! LC_ALL=C sed -E $'s/\033\\[[0-9;]*[mK]//g' "$artifact" \
        | awk -v node="$node" -v source="$source" '
          function emit(metric, unit) {
            if (metric in values) {
              print node "\t" source "\t" record "\t" metric "\t" unit "\t" \
                values[metric] "\t" NR
            }
          }
          function has_record(name) {
            return $0 ~ ("(^|[^[:alnum:]_-])" name "([^[:alnum:]_-]|$)")
          }
          {
            record = ""
            if (has_record("blob-only-write-streamed")) {
              record = "blob-only-write-streamed"
            } else if (has_record("blob-only-write")) {
              record = "blob-only-write"
            } else if (has_record("content-addressed-blob-write")) {
              record = "content-addressed-blob-write"
            } else if (has_record("packed-blob-lock")) {
              record = "packed-blob-lock"
            } else if (has_record("posix-bulk-authority-commit")) {
              record = "posix-bulk-authority-commit"
            } else if (has_record("posix-bulk-local-write")) {
              record = "posix-bulk-local-write"
            } else if (has_record("metadata-authority-confirmation")) {
              record = "metadata-authority-confirmation"
            } else if (has_record("content-root-catalog-shard-refresh")) {
              record = "content-root-catalog-shard-refresh"
            } else if (has_record("content-root-catalog-refresh")) {
              record = "content-root-catalog-refresh"
            } else if (has_record("metadata-journal-segment")) {
              record = "metadata-journal-segment"
            } else if (has_record("metadata-journal-entries")) {
              record = "metadata-journal-entries"
            } else if (has_record("metadata-table-batch")) {
              record = "metadata-table-batch"
            } else if (has_record("metadata-publish-plan")) {
              record = "metadata-publish-plan"
            }
            if (record == "") next

            delete values
            for (field = 1; field <= NF; field++) {
              separator = index($field, "=")
              if (separator <= 1) continue
              key = substr($field, 1, separator - 1)
              value = substr($field, separator + 1)
              gsub(/^[^0-9]+/, "", value)
              sub(/[^0-9].*$/, "", value)
              if (value ~ /^[0-9]+$/) values[key] = value
            }

            if (record == "packed-blob-lock") {
              emit("wait_us", "us")
              emit("hold_us", "us")
            } else if (record == "posix-bulk-authority-commit") {
              emit("prepare_us", "us")
              emit("commit_ms", "ms")
            } else if (record == "posix-bulk-local-write") {
              emit("content_ms", "ms")
              emit("prepare_setup_ms", "ms")
              emit("group_prepare_ms", "ms")
              emit("authority_commit_ms", "ms")
              emit("stat_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "metadata-authority-confirmation") {
              emit("probe_ms", "ms")
              emit("assignment_ms", "ms")
              emit("quorum_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "content-root-catalog-shard-refresh") {
              emit("pages", "count")
              emit("entries", "count")
              emit("total_ms", "ms")
            } else if (record == "content-root-catalog-refresh") {
              emit("shards", "count")
              emit("pages", "count")
              emit("entries", "count")
              emit("total_ms", "ms")
            } else if (record == "blob-only-write") {
              emit("hash_ms", "ms")
              emit("manifest_check_ms", "ms")
              emit("has_blob_ms", "ms")
              emit("quota_ms", "ms")
              emit("blob_write_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "blob-only-write-streamed") {
              emit("manifest_check_ms", "ms")
              emit("stream_hash_write_ms", "ms")
              emit("create_dir_ms", "ms")
              emit("publish_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "content-addressed-blob-write") {
              emit("create_dir_ms", "ms")
              emit("existing_check_ms", "ms")
              emit("write_sync_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "metadata-journal-segment") {
              emit("entries", "count")
              emit("bytes", "bytes")
              emit("create_dir_ms", "ms")
              emit("encode_ms", "ms")
              emit("write_sync_ms", "ms")
              emit("sync_dir_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "metadata-journal-entries") {
              emit("entries", "count")
              emit("chunks", "count")
              emit("sequence_ms", "ms")
              emit("build_ms", "ms")
              emit("append_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "metadata-table-batch") {
              emit("ops", "count")
              emit("bytes", "bytes")
              emit("create_dir_ms", "ms")
              emit("encode_ms", "ms")
              emit("write_ms", "ms")
              emit("total_ms", "ms")
            } else if (record == "metadata-publish-plan") {
              emit("outcomes", "count")
              emit("targets", "count")
              emit("total_ms", "ms")
            }
          }
        ' >>"$records"; then
        parse_failures=$((parse_failures + 1))
      fi
    done
  done

  printf 'record\tmetric\tunit\tsamples\tminimum\tp50\tp95\tmaximum\tmean\n' >"$summary"
  sed -n '2,$p' "$records" \
    | sort -t $'\t' -k3,3 -k4,4 -k5,5 -k6,6n \
    | awk -F '\t' '
        function flush(    p50_index, p95_index) {
          if (samples == 0) return
          p50_index = int((samples * 50 + 99) / 100)
          p95_index = int((samples * 95 + 99) / 100)
          printf "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%.3f\n",
            current_record, current_metric, current_unit, samples,
            minimum, values[p50_index], values[p95_index], maximum, total / samples
          delete values
          samples = 0
          total = 0
        }
        {
          key = $3 SUBSEP $4 SUBSEP $5
          if (current_key != "" && key != current_key) flush()
          if (key != current_key) {
            current_key = key
            current_record = $3
            current_metric = $4
            current_unit = $5
            minimum = $6
          }
          samples += 1
          values[samples] = $6
          maximum = $6
          total += $6
        }
        END { flush() }
      ' >>"$summary"

  record_count=$(( $(wc -l <"$records") - 1 ))
  (( record_count >= 0 )) || record_count=0
  {
    printf 'captured_log_files\t%s\n' "$observed_logs"
    printf 'timing_records\t%s\n' "$record_count"
    printf 'parse_failures\t%s\n' "$parse_failures"
    if (( observed_logs == 0 )); then
      printf 'status\tno-captured-daemon-logs\n'
    elif (( parse_failures != 0 )); then
      printf 'status\tpartial-parser-failure\n'
    elif (( record_count == 0 )); then
      printf 'status\tno-matching-records-or-tracing-disabled\n'
    else
      printf 'status\tobserved\n'
    fi
  } >"$status"

  printf 'domain\trecord\ttiming\tsamples\tavailability\tnote\n' >"$coverage"
  while IFS=$'\t' read -r domain record timing note; do
    if [[ "$timing" == unavailable ]]; then
      samples=0
      availability=not-instrumented
    else
      samples="$(awk -F '\t' -v record="$record" -v metric="$timing" '
        NR > 1 && $3 == record && $4 == metric { count += 1 }
        END { print count + 0 }
      ' "$records")"
      if (( samples > 0 )); then
        availability=observed
      else
        availability=not-observed
      fi
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$domain" "$record" "$timing" "$samples" "$availability" "$note" \
      >>"$coverage"
  done <<'EOF'
content-pack	packed-blob-lock	wait_us	packed small-blob lock wait only
content-pack	packed-blob-lock	hold_us	packed small-blob lock hold only
content-pack	packed-blob-write	unavailable	no existing successful pack append or sync duration record
bulk-prepare	posix-bulk-local-write	prepare_setup_ms	build committed objects and authority groups
bulk-prepare	posix-bulk-local-write	group_prepare_ms	build per-authority transactions and write proofs
bulk-commit	posix-bulk-authority-commit	commit_ms	per-authority local or routed namespace commit
replication-confirmation	metadata-authority-confirmation	total_ms	full-shard acknowledgement or cluster-quorum permit
catalog	content-root-catalog-shard-refresh	total_ms	one exact-cut shard fragment refresh
catalog	content-root-catalog-refresh	total_ms	complete multi-shard catalog refresh and durable install
content-ingest	blob-only-write	blob_write_ms	loose blob write when emitted
content-ingest	blob-only-write-streamed	stream_hash_write_ms	streamed hash and write when emitted
content-ingest	content-addressed-blob-write	write_sync_ms	durable loose-blob write and sync when emitted
journal	metadata-journal-segment	encode_ms	serialized authoritative journal segment entries
journal	metadata-journal-segment	write_sync_ms	durable journal append and file sync
journal	metadata-journal-segment	sync_dir_ms	first-segment parent directory sync
journal	metadata-journal-entries	append_ms	batched journal append wrapper across segment chunks
table	metadata-table-batch	encode_ms	materialized replay-cache table encoding
table	metadata-table-batch	write_ms	unsynced materialized replay-cache append
replication	metadata-publish-plan	total_ms	replication planning only
replication	metadata-journal-transport	unavailable	no existing successful journal transport latency record
EOF

  if (( parse_failures != 0 )); then
    log "DFS perf trace summary completed with $parse_failures parser input failure(s)"
    return 1
  fi
  log "DFS perf trace summary: records=$record_count captured_logs=$observed_logs"
}

if (( TRACE_SUMMARY_ONLY != 0 )); then
  for command in awk sed sort wc mkdir; do
    require_command "$command"
  done
  summarize_dfs_perf_traces "$ARTIFACTS"
  exit
fi

compose() {
  docker compose -p "$COMPOSE_PROJECT_NAME" "${COMPOSE_FILES[@]}" "$@"
}

run_node_timeout() {
  local seconds="$1" node="$2"
  shift 2
  timeout --foreground "${seconds}s" \
    docker compose -p "$COMPOSE_PROJECT_NAME" "${COMPOSE_FILES[@]}" \
    exec -T "$node" "$@"
}

run_container_timeout() {
  local seconds="$1" node="$2"
  shift 2
  timeout --foreground "${seconds}s" \
    docker exec -i "${container_id[$node]}" "$@"
}

for command in docker jq timeout date awk sed grep cmp getconf sort wc; do
  require_command "$command"
done
case "$BINARY_PROFILE" in
  debug | release) ;;
  *) die "SINOMO_DFS_PERF_BINARY_PROFILE must be debug or release, got '$BINARY_PROFILE'" ;;
esac
case "$EXPECTED_NAMESPACE_PREPARE_CODEC" in
  legacy | packed-v1) ;;
  *)
    die "SINOMO_DFS_PERF_EXPECT_NAMESPACE_PREPARE_CODEC must be legacy or packed-v1, got '$EXPECTED_NAMESPACE_PREPARE_CODEC'"
    ;;
esac
if [[ "$PERF_MODE" == release-ceiling && "$BINARY_PROFILE" != release ]]; then
  die "release-ceiling mode requires SINOMO_DFS_PERF_BINARY_PROFILE=release; this is an operator assertion recorded with the artifacts"
fi
require_positive_uint SINOMO_DFS_PERF_FILES "$FILES"
require_positive_uint SINOMO_DFS_PERF_BYTES "$BYTES"
require_uint SINOMO_DFS_PERF_SUSTAINED_ROUNDS "$SUSTAINED_ROUNDS"
require_positive_uint SINOMO_DFS_PERF_MIN_FILES_PER_SECOND "$DEFAULT_MIN_FILES_PER_SECOND"
require_positive_uint \
  SINOMO_DFS_PERF_MIN_ISOLATED_FILES_PER_SECOND "$MIN_ISOLATED_FILES_PER_SECOND"
require_positive_uint \
  SINOMO_DFS_PERF_MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND \
  "$MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND"
require_uint \
  SINOMO_DFS_PERF_MIN_CONCURRENT_LANE_FILES_PER_SECOND \
  "$MIN_CONCURRENT_LANE_FILES_PER_SECOND"
require_positive_uint \
  SINOMO_DFS_PERF_MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND \
  "$MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND"
require_positive_uint SINOMO_DFS_PERF_MIN_PRIMARY_CPU_PERCENT "$MIN_PRIMARY_CPU_PERCENT"
require_positive_uint SINOMO_DFS_PERF_MIN_LANE_MS "$MIN_LANE_MS"
require_uint SINOMO_DFS_PERF_MAX_IDLE_MS "$MAX_IDLE_MS"
require_uint SINOMO_DFS_PERF_WARMUP_MS "$WARMUP_MS"
require_positive_uint SINOMO_DFS_PERF_TIMEOUT_SECONDS "$TIMEOUT_SECONDS"
require_positive_uint SINOMO_DFS_PERF_CONVERGENCE_SECONDS "$CONVERGENCE_SECONDS"
[[ "$SAMPLE_SECONDS" =~ ^0\.[0-9]+$|^[1-9][0-9]*(\.[0-9]+)?$ ]] \
  || die "SINOMO_DFS_PERF_SAMPLE_SECONDS must be a positive decimal"
CLK_TCK="$(getconf CLK_TCK)"
require_positive_uint CLK_TCK "$CLK_TCK"

mkdir -p "$ARTIFACTS"
cd "$REPO_ROOT"

if [[ -n "$BATCH_SIZE_SWEEP" ]]; then
  compact_sweep="${BATCH_SIZE_SWEEP//[[:space:]]/}"
  [[ "$compact_sweep" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] \
    || die "SINOMO_DFS_PERF_BATCH_SIZE_SWEEP must be a comma-separated list of positive integers"
  IFS=',' read -r -a sweep_sizes <<<"$compact_sweep"
  declare -A seen_sweep_sizes=()
  printf 'batch_size\tstatus\tartifact\tminimum_isolated_data_write_files_per_second\tconcurrent_aggregate_files_per_second\tsustained_aggregate_files_per_second\n' \
    >"$ARTIFACTS/batch-sweep-summary.tsv"
  {
    printf 'mode\t%s\n' "$PERF_MODE"
    printf 'binary_profile_operator_assertion\t%s\n' "$BINARY_PROFILE"
    printf 'expected_namespace_prepare_codec\t%s\n' "$EXPECTED_NAMESPACE_PREPARE_CODEC"
    printf 'batch_sizes\t%s\n' "$compact_sweep"
    printf 'gate_contract\tfull-configured-gate-per-batch-size\n'
  } >"$ARTIFACTS/batch-sweep-config.tsv"
  sweep_failed=0
  for sweep_size in "${sweep_sizes[@]}"; do
    [[ -z "${seen_sweep_sizes[$sweep_size]:-}" ]] \
      || die "SINOMO_DFS_PERF_BATCH_SIZE_SWEEP contains duplicate size $sweep_size"
    seen_sweep_sizes[$sweep_size]=1
    child_artifact="$ARTIFACTS/batch-$sweep_size"
    [[ ! -e "$child_artifact" ]] \
      || die "batch sweep child artifact already exists: $child_artifact"
    log "batch sweep: running full gate with $sweep_size files per bulk call"
    child_status=failed
    if SINOMO_DFS_PERF_BATCH_SIZE_SWEEP= \
      SINOMO_DFS_PERF_FILES="$sweep_size" \
      SINOMO_DFS_PERF_ARTIFACTS="$child_artifact" \
      "$SCRIPT_DIR/measure-dfs-shards.sh"; then
      child_status=passed
    else
      sweep_failed=1
    fi
    minimum_isolated=unavailable
    if [[ -f "$child_artifact/workloads.tsv" ]]; then
      minimum_isolated="$(
        awk -F '\t' '
          NR == 1 {
            for (column = 1; column <= NF; column++) {
              if ($column == "data_write_files_per_second") rate_column = column
            }
            next
          }
          $1 == "isolated" && rate_column > 0 {
            if (minimum == "" || $rate_column + 0 < minimum + 0) minimum = $rate_column
          }
          END { if (minimum != "") print minimum }
        ' "$child_artifact/workloads.tsv"
      )"
      minimum_isolated="${minimum_isolated:-unavailable}"
    fi
    concurrent_aggregate="$(
      awk -F '\t' '$1 == "concurrent_aggregate_files_per_second" { print $2; exit }' \
        "$child_artifact/aggregate-summary.tsv" 2>/dev/null || true
    )"
    concurrent_aggregate="${concurrent_aggregate:-unavailable}"
    sustained_aggregate="$(
      awk -F '\t' '$1 == "sustained_aggregate_files_per_second" { print $2; exit }' \
        "$child_artifact/sustained-summary.tsv" 2>/dev/null || true
    )"
    sustained_aggregate="${sustained_aggregate:-disabled}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$sweep_size" "$child_status" "batch-$sweep_size" "$minimum_isolated" \
      "$concurrent_aggregate" "$sustained_aggregate" \
      >>"$ARTIFACTS/batch-sweep-summary.tsv"
    if (( sweep_failed != 0 )); then
      log "batch sweep stopped after failed size $sweep_size; inspect $child_artifact"
      exit 1
    fi
  done
  log "batch sweep passed: $ARTIFACTS/batch-sweep-summary.tsv"
  exit
fi

declare -A container_id=()
declare -A container_pid=()
declare -A container_cgroup=()
declare -A main_daemon_namespace_pid=()
declare -A main_daemon_host_pid=()
declare -A node_id_by_service=()
declare -A service_by_node_id=()
declare -A data_fs_type=()

resolve_main_daemon_host_pid() {
  local cgroup="$1" namespace_pid="$2" host_pid candidate_namespace_pid
  local match_count=0 match=""
  while read -r host_pid; do
    [[ "$host_pid" =~ ^[0-9]+$ && -r "/proc/$host_pid/status" ]] || continue
    candidate_namespace_pid="$(awk '/^NSpid:/ { print $NF; exit }' "/proc/$host_pid/status")"
    if [[ "$candidate_namespace_pid" == "$namespace_pid" ]]; then
      match="$host_pid"
      match_count=$((match_count + 1))
    fi
  done <"$cgroup/cgroup.procs"
  (( match_count == 1 )) || return 1
  printf '%s\n' "$match"
}

log "preflight: verifying six already-running containers"
for node in "${NODES[@]}"; do
  cid="$(compose ps -q "$node")"
  [[ -n "$cid" ]] || die "$node is not running in compose project $COMPOSE_PROJECT_NAME"
  [[ "$(docker inspect -f '{{.State.Running}}' "$cid")" == "true" ]] \
    || die "$node container is not running"
  pid="$(docker inspect -f '{{.State.Pid}}' "$cid")"
  cgroup_path="$(awk -F: '$1 == "0" { print $3; exit }' "/proc/$pid/cgroup")"
  [[ -n "$cgroup_path" ]] || die "$node is not attached to a readable cgroup-v2 path"
  [[ -r "$CGROUP_ROOT$cgroup_path/cpu.stat" ]] \
    || die "cannot read cgroup CPU stats for $node at $CGROUP_ROOT$cgroup_path"
  [[ -r "$CGROUP_ROOT$cgroup_path/cgroup.procs" ]] \
    || die "cannot inspect cgroup processes for $node at $CGROUP_ROOT$cgroup_path"
  for cgroup_file in \
    cpu.pressure io.pressure memory.pressure memory.current memory.peak memory.events \
    io.stat cpu.max cpuset.cpus.effective; do
    [[ -r "$CGROUP_ROOT$cgroup_path/$cgroup_file" ]] \
      || die "cannot read cgroup metric $cgroup_file for $node at $CGROUP_ROOT$cgroup_path"
  done
  [[ -r "/proc/$pid/net/dev" ]] || die "cannot read network counters for $node"

  daemon_status="$(run_node_timeout 15 "$node" sinomo daemon status)"
  daemon_namespace_pid="$(sed -n 's/^[[:space:]]*pid: //p' <<<"$daemon_status" | sed -n '1p')"
  [[ "$daemon_namespace_pid" =~ ^[1-9][0-9]*$ ]] \
    || die "cannot read the main daemon namespace PID for $node"
  daemon_host_pid="$(
    resolve_main_daemon_host_pid "$CGROUP_ROOT$cgroup_path" "$daemon_namespace_pid"
  )" || die "cannot uniquely map $node main daemon namespace PID $daemon_namespace_pid to its host cgroup PID"
  [[ -r "/proc/$daemon_host_pid/stat" ]] \
    || die "cannot read main daemon host process stats for $node at PID $daemon_host_pid"
  node_id="$(sed -n 's/^[[:space:]]*node: //p' <<<"$daemon_status" | sed -n '1p')"
  [[ -n "$node_id" ]] || die "cannot map $node to its runtime node id"
  [[ -z "${service_by_node_id[$node_id]:-}" ]] \
    || die "duplicate runtime node id $node_id for $node and ${service_by_node_id[$node_id]}"

  container_id[$node]="$cid"
  container_pid[$node]="$pid"
  container_cgroup[$node]="$CGROUP_ROOT$cgroup_path"
  main_daemon_namespace_pid[$node]="$daemon_namespace_pid"
  main_daemon_host_pid[$node]="$daemon_host_pid"
  node_id_by_service[$node]="$node_id"
  service_by_node_id[$node_id]="$node"
  data_fs_type[$node]="$(run_node_timeout 15 "$node" stat -f -c %T /data)"
done

compose ps >"$ARTIFACTS/compose-ps.before.txt"
run_node_timeout 30 node1 sinomo cluster health >"$ARTIFACTS/cluster-health.before.txt"
run_node_timeout 30 node1 sinomo cluster doctor >"$ARTIFACTS/cluster-doctor.before.txt"
run_node_timeout 30 node1 sinomo dfs status --cluster --json \
  >"$ARTIFACTS/dfs-status.before.json"

validate_local_node_status() {
  local file="$1"
  jq -e '
    .pending_write_count == 0
    and .open_conflict_count == 0
    and .pending_repair_count == 0
    and .pending_namespace_job_count == 0
    and .failed_namespace_job_count == 0
  ' "$file" >/dev/null
}

capture_and_validate_local_node_statuses() {
  local suffix="$1" node file all_clean=0
  for node in "${NODES[@]}"; do
    file="$ARTIFACTS/dfs-status-$node.$suffix.json"
    if ! run_node_timeout 30 "$node" sinomo dfs status --json \
        >"$file" 2>"$file.stderr.log" \
      || ! validate_local_node_status "$file"; then
      all_clean=1
    fi
  done
  return "$all_clean"
}

# No node in the deliberately segmented Docker topology has a direct endpoint
# to every other node. `dfs status --cluster` intentionally records those
# routed-only peers as unavailable according to the local node's direct
# connectivity probes even while signed multi-hop RPCs to them succeed. Keep
# that aggregate report as routing diagnostics, but establish the six-node DFS
# cleanliness invariant from each daemon's local status instead.
capture_and_validate_local_node_statuses before \
  || die "local DFS status is not clean on every one of the six nodes"
[[ "$(grep -c $'daemon-main.*online' "$ARTIFACTS/cluster-health.before.txt" || true)" -eq 6 ]] \
  || die "cluster health does not report six online main daemons"
if grep -Eiq 'warning|error|unavailable|offline|unhealthy' "$ARTIFACTS/cluster-doctor.before.txt"; then
  die "cluster doctor reported a warning or error; inspect $ARTIFACTS/cluster-doctor.before.txt"
fi

log "preflight: resolving live shard routing"
run_node_timeout 60 node1 sinomo test run cluster.dfs.shard-layout \
  --require-cluster --json \
  --param "expected-namespace-prepare-codec=$EXPECTED_NAMESPACE_PREPARE_CODEC" \
  >"$ARTIFACTS/shard-layout.before.json" \
  2>"$ARTIFACTS/shard-layout.before.stderr.log" \
  || die "cluster.dfs.shard-layout failed; the running containers may need the newly built binary"
jq -e --arg expected_codec "$EXPECTED_NAMESPACE_PREPARE_CODEC" '
  .status == "passed"
  and .diagnostics.distinct_shard_count == 3
  and (.diagnostics.shards | length) == 3
  and all(.diagnostics.shards[];
    (.lane | type) == "string"
    and (.base_prefix | startswith("/"))
    and (.shard_id | type) == "string"
    and (.primary_node_id | type) == "string"
    and (.replica_node_ids | type) == "array"
    and (.epoch | type) == "number"
    and .namespace_prepare_codec == $expected_codec)
' "$ARTIFACTS/shard-layout.before.json" >/dev/null \
  || die "cluster.dfs.shard-layout returned incomplete runtime routing evidence"

declare -A lane_base=()
declare -A lane_shard=()
declare -A lane_primary_id=()
declare -A lane_primary_service=()
declare -A lane_epoch=()
declare -A lane_replica_count=()
declare -A lane_namespace_prepare_codec=()
while IFS=$'\t' read -r lane base shard primary epoch replica_count namespace_prepare_codec; do
  lane_base[$lane]="$base"
  lane_shard[$lane]="$shard"
  lane_primary_id[$lane]="$primary"
  lane_epoch[$lane]="$epoch"
  lane_replica_count[$lane]="$replica_count"
  lane_namespace_prepare_codec[$lane]="$namespace_prepare_codec"
done < <(jq -r '.diagnostics.shards[] |
  [.lane, .base_prefix, .shard_id, .primary_node_id,
   (.epoch | tostring), (.replica_node_ids | length | tostring),
   .namespace_prepare_codec] | @tsv' \
  "$ARTIFACTS/shard-layout.before.json")

declare -A primary_services=()
for lane in "${LANES[@]}"; do
  [[ -n "${lane_shard[$lane]:-}" ]] || die "live layout omitted the $lane lane"
  primary_service="${service_by_node_id[${lane_primary_id[$lane]}]:-}"
  [[ -n "$primary_service" ]] \
    || die "$lane primary ${lane_primary_id[$lane]} is not one of the six compose nodes"
  (( lane_replica_count[$lane] == EXPECTED_REPLICA_COUNT )) \
    || die "$lane shard has ${lane_replica_count[$lane]} replicas, expected $EXPECTED_REPLICA_COUNT"
  lane_primary_service[$lane]="$primary_service"
  primary_services[$primary_service]="$lane"
done
(( ${#primary_services[@]} == ${#LANES[@]} )) \
  || die "the three metadata shards do not have distinct live primaries; three-core utilization cannot be measured"

validate_primary_shard() {
  local lane="$1" suffix="$2"
  local node="${lane_primary_service[$lane]}"
  local file="$ARTIFACTS/primary-$lane-$suffix.json"
  run_node_timeout 30 "$node" sinomo dfs status --json >"$file" 2>"$file.stderr" || return 1
  jq -e \
    --arg shard "${lane_shard[$lane]}" \
    --arg primary "${lane_primary_id[$lane]}" \
    --argjson replicas "$EXPECTED_REPLICA_COUNT" '
      .pending_write_count == 0
      and .open_conflict_count == 0
      and .pending_repair_count == 0
      and .pending_namespace_job_count == 0
      and .failed_namespace_job_count == 0
      and ([.metadata_shards[] | select(.shard_id == $shard)] | length) == 1
      and ([.metadata_shards[] | select(.shard_id == $shard)][0] |
        .primary_node_id == $primary
        and (.replica_node_ids | length) == $replicas
        and (.replica_lag | length) == $replicas
        and all(.replica_lag[]; .lag_entries == 0))
    ' "$file" >/dev/null
}

for lane in "${LANES[@]}"; do
  validate_primary_shard "$lane" before \
    || die "$lane primary is not clean, fully replicated, and caught up"
done

declare -A lane_client=()
declare -A used_client=()
for lane in "${LANES[@]}"; do
  for candidate in node4 node5 node6 node1 node2 node3; do
    if [[ -z "${primary_services[$candidate]:-}" && -z "${used_client[$candidate]:-}" ]]; then
      lane_client[$lane]="$candidate"
      used_client[$candidate]=1
      break
    fi
  done
  [[ -n "${lane_client[$lane]:-}" ]] || die "cannot assign an isolated remote client to $lane"
done

{
  printf 'run_id\t%s\n' "$RUN_ID"
  printf 'performance_mode\t%s\n' "$PERF_MODE"
  printf 'binary_profile_operator_assertion\t%s\n' "$BINARY_PROFILE"
  printf 'binary_profile_verified_by_script\tno\n'
  printf 'expected_namespace_prepare_codec\t%s\n' "$EXPECTED_NAMESPACE_PREPARE_CODEC"
  printf 'compose_project\t%s\n' "$COMPOSE_PROJECT_NAME"
  printf 'compose_file\t%s\n' "$COMPOSE_FILE"
  printf 'compose_override_file\t%s\n' "${COMPOSE_OVERRIDE_FILE:-none}"
  printf 'files_per_lane_per_phase\t%s\n' "$FILES"
  if (( SUSTAINED_ROUNDS > 0 )); then
    printf 'measurement_phases\tisolated-per-lane,concurrent-three-lane,sustained-churn\n'
  else
    printf 'measurement_phases\tisolated-per-lane,concurrent-three-lane\n'
  fi
  printf 'bytes_per_file\t%s\n' "$BYTES"
  printf 'sustained_rounds\t%s\n' "$SUSTAINED_ROUNDS"
  printf 'default_min_files_per_second\t%s\n' "$DEFAULT_MIN_FILES_PER_SECOND"
  printf 'min_isolated_data_write_files_per_second_per_lane\t%s\n' \
    "$MIN_ISOLATED_FILES_PER_SECOND"
  printf 'min_concurrent_aggregate_files_per_second\t%s\n' \
    "$MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND"
  printf 'min_concurrent_data_write_files_per_second_per_lane\t%s\n' \
    "$MIN_CONCURRENT_LANE_FILES_PER_SECOND"
  printf 'min_sustained_aggregate_files_per_second\t%s\n' \
    "$MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND"
  printf 'sample_seconds\t%s\n' "$SAMPLE_SECONDS"
  printf 'clock_ticks_per_second\t%s\n' "$CLK_TCK"
  printf 'min_primary_cpu_percent\t%s\n' "$MIN_PRIMARY_CPU_PERCENT"
  printf 'max_idle_ms\t%s\n' "$MAX_IDLE_MS"
  printf 'workload_window_attribution\tclient-process-liveness-plus-warmup-approximation\n'
  for lane in "${LANES[@]}"; do
    printf '%s\tshard=%s\tprimary=%s\tclient=%s\tepoch=%s\tbase=%s\tnamespace_prepare_codec=%s\n' \
      "$lane" "${lane_shard[$lane]}" "${lane_primary_service[$lane]}" \
      "${lane_client[$lane]}" "${lane_epoch[$lane]}" "${lane_base[$lane]}" \
      "${lane_namespace_prepare_codec[$lane]}"
  done
  for node in "${NODES[@]}"; do
    cpu_max="$(<"${container_cgroup[$node]}/cpu.max")"
    cpuset="$(<"${container_cgroup[$node]}/cpuset.cpus.effective")"
    process_io_available=no
    [[ -r "/proc/${main_daemon_host_pid[$node]}/io" ]] && process_io_available=yes
    printf '%s\tnode_id=%s\tmain_daemon_namespace_pid=%s\tmain_daemon_host_pid=%s\tdata_fs=%s\tcpu_max=%s\tcpuset=%s\tprocess_io_available=%s\n' \
      "$node" "${node_id_by_service[$node]}" "${main_daemon_namespace_pid[$node]}" \
      "${main_daemon_host_pid[$node]}" "${data_fs_type[$node]}" "$cpu_max" "$cpuset" \
      "$process_io_available"
  done
} >"$ARTIFACTS/config.tsv"

sample_usage_usec=0
sample_nr_throttled=0
sample_throttled_usec=0
sample_pressure_some=0
sample_pressure_full=0
sample_memory_current=0
sample_memory_peak=0
sample_memory_low=0
sample_memory_high=0
sample_memory_max=0
sample_memory_oom=0
sample_memory_oom_kill=0
sample_memory_oom_group_kill=0
sample_io_rbytes=0
sample_io_wbytes=0
sample_rx_bytes=0
sample_tx_bytes=0
sample_process_cpu_ticks=0
sample_process_usage_usec=0
sample_process_start_ticks=0
sample_process_leader_state=""
sample_process_thread_count=0
sample_process_runnable_threads=0
sample_process_sleeping_threads=0
sample_process_uninterruptible_threads=0
sample_process_other_threads=0
sample_process_voluntary_context_switches=0
sample_process_nonvoluntary_context_switches=0
sample_process_read_bytes=-1
sample_process_write_bytes=-1
sample_host_memory_total_bytes=0
sample_host_memory_available_bytes=0
sample_host_swap_total_bytes=0
sample_host_swap_free_bytes=0

read_process_observation() {
  local host_pid="$1" stat rest status_file key value state
  local -a fields=()
  [[ -r "/proc/$host_pid/stat" ]] || return 1
  IFS= read -r stat <"/proc/$host_pid/stat" || return 1
  rest="${stat##*) }"
  read -r -a fields <<<"$rest"
  (( ${#fields[@]} >= 20 )) || return 1
  sample_process_leader_state="${fields[0]}"
  sample_process_cpu_ticks=$((fields[11] + fields[12]))
  sample_process_usage_usec=$((sample_process_cpu_ticks * 1000000 / CLK_TCK))
  sample_process_start_ticks="${fields[19]}"

  sample_process_thread_count=0
  sample_process_runnable_threads=0
  sample_process_sleeping_threads=0
  sample_process_uninterruptible_threads=0
  sample_process_other_threads=0
  sample_process_voluntary_context_switches=0
  sample_process_nonvoluntary_context_switches=0
  for status_file in "/proc/$host_pid/task"/[0-9]*/status; do
    [[ -r "$status_file" ]] || continue
    state=""
    while read -r key value _; do
      case "$key" in
        State:) state="$value" ;;
        voluntary_ctxt_switches:)
          sample_process_voluntary_context_switches=$((
            sample_process_voluntary_context_switches + value
          ))
          ;;
        nonvoluntary_ctxt_switches:)
          sample_process_nonvoluntary_context_switches=$((
            sample_process_nonvoluntary_context_switches + value
          ))
          ;;
      esac
    done <"$status_file" 2>/dev/null
    [[ -n "$state" ]] || continue
    sample_process_thread_count=$((sample_process_thread_count + 1))
    case "$state" in
      R) sample_process_runnable_threads=$((sample_process_runnable_threads + 1)) ;;
      S|I) sample_process_sleeping_threads=$((sample_process_sleeping_threads + 1)) ;;
      D) sample_process_uninterruptible_threads=$((sample_process_uninterruptible_threads + 1)) ;;
      *) sample_process_other_threads=$((sample_process_other_threads + 1)) ;;
    esac
  done
  (( sample_process_thread_count > 0 )) || return 1

  sample_process_read_bytes=-1
  sample_process_write_bytes=-1
  if [[ -r "/proc/$host_pid/io" ]]; then
    while read -r key value _; do
      case "$key" in
        read_bytes:)
          sample_process_read_bytes="$value"
          ;;
        write_bytes:)
          sample_process_write_bytes="$value"
          ;;
      esac
    done <"/proc/$host_pid/io"
  fi
}

read_cpu_stat() {
  local file="$1" key value
  sample_usage_usec=0
  sample_nr_throttled=0
  sample_throttled_usec=0
  while read -r key value _; do
    case "$key" in
      usage_usec) sample_usage_usec="$value" ;;
      nr_throttled) sample_nr_throttled="$value" ;;
      throttled_usec) sample_throttled_usec="$value" ;;
    esac
  done <"$file"
}

read_pressure() {
  local file="$1" class rest token
  local -a tokens=()
  sample_pressure_some=0
  sample_pressure_full=0
  while read -r class rest; do
    read -r -a tokens <<<"$rest"
    for token in "${tokens[@]}"; do
      case "$token" in
        total=*)
          if [[ "$class" == "some" ]]; then
            sample_pressure_some="${token#total=}"
          elif [[ "$class" == "full" ]]; then
            sample_pressure_full="${token#total=}"
          fi
          ;;
      esac
    done
  done <"$file"
}

read_memory_observation() {
  local cgroup="$1" key value
  IFS= read -r sample_memory_current <"$cgroup/memory.current"
  IFS= read -r sample_memory_peak <"$cgroup/memory.peak"
  sample_memory_low=0
  sample_memory_high=0
  sample_memory_max=0
  sample_memory_oom=0
  sample_memory_oom_kill=0
  sample_memory_oom_group_kill=0
  while read -r key value _; do
    case "$key" in
      low) sample_memory_low="$value" ;;
      high) sample_memory_high="$value" ;;
      max) sample_memory_max="$value" ;;
      oom) sample_memory_oom="$value" ;;
      oom_kill) sample_memory_oom_kill="$value" ;;
      oom_group_kill) sample_memory_oom_group_kill="$value" ;;
    esac
  done <"$cgroup/memory.events"
}

read_host_memory_observation() {
  local key value
  sample_host_memory_total_bytes=0
  sample_host_memory_available_bytes=0
  sample_host_swap_total_bytes=0
  sample_host_swap_free_bytes=0
  while read -r key value _; do
    case "$key" in
      MemTotal:) sample_host_memory_total_bytes=$((value * 1024)) ;;
      MemAvailable:) sample_host_memory_available_bytes=$((value * 1024)) ;;
      SwapTotal:) sample_host_swap_total_bytes=$((value * 1024)) ;;
      SwapFree:) sample_host_swap_free_bytes=$((value * 1024)) ;;
    esac
  done </proc/meminfo
  (( sample_host_memory_total_bytes > 0 && sample_host_memory_available_bytes > 0 )) \
    || die "cannot read host memory totals from /proc/meminfo"
}

record_host_memory_observation() {
  local timestamp_ns="$1" elapsed_ms="$2" phase="$3" workload_lanes="$4"
  read_host_memory_observation
  if (( host_min_memory_available_bytes < 0 \
      || sample_host_memory_available_bytes < host_min_memory_available_bytes )); then
    host_min_memory_available_bytes="$sample_host_memory_available_bytes"
  fi
  host_last_memory_available_bytes="$sample_host_memory_available_bytes"
  host_last_swap_free_bytes="$sample_host_swap_free_bytes"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$timestamp_ns" "$elapsed_ms" "$phase" "$workload_lanes" \
    "$sample_host_memory_total_bytes" "$sample_host_memory_available_bytes" \
    "$sample_host_swap_total_bytes" "$sample_host_swap_free_bytes" \
    >>"$ARTIFACTS/host-memory.csv"
}

read_io_stat() {
  local file="$1" _device rest token
  local -a tokens=()
  sample_io_rbytes=0
  sample_io_wbytes=0
  while read -r _device rest; do
    read -r -a tokens <<<"$rest"
    for token in "${tokens[@]}"; do
      case "$token" in
        rbytes=*) sample_io_rbytes=$((sample_io_rbytes + ${token#rbytes=})) ;;
        wbytes=*) sample_io_wbytes=$((sample_io_wbytes + ${token#wbytes=})) ;;
      esac
    done
  done <"$file"
}

read_net_stat() {
  local file="$1" line
  local -a fields=()
  sample_rx_bytes=0
  sample_tx_bytes=0
  while IFS= read -r line; do
    [[ "$line" == *:* ]] || continue
    read -r -a fields <<<"$line"
    (( ${#fields[@]} >= 10 )) || continue
    sample_rx_bytes=$((sample_rx_bytes + fields[1]))
    sample_tx_bytes=$((sample_tx_bytes + fields[9]))
  done <"$file"
}

declare -A previous_usage=()
declare -A previous_process_usage_usec=()
declare -A process_start_ticks=()
declare -A previous_process_voluntary_context_switches=()
declare -A previous_process_nonvoluntary_context_switches=()
declare -A previous_process_read_bytes=()
declare -A previous_process_write_bytes=()
declare -A process_io_complete=()
declare -A previous_timestamp_ns=()
declare -A previous_nr_throttled=()
declare -A previous_cpu_pressure_some=()
declare -A previous_io_pressure_some=()
declare -A previous_io_rbytes=()
declare -A previous_io_wbytes=()
declare -A previous_rx_bytes=()
declare -A previous_tx_bytes=()
declare -A initial_nr_throttled=()
declare -A initial_throttled_usec=()
declare -A initial_cpu_pressure_some=()
declare -A initial_cpu_pressure_full=()
declare -A initial_io_pressure_some=()
declare -A initial_io_pressure_full=()
declare -A initial_memory_pressure_some=()
declare -A initial_memory_pressure_full=()
declare -A initial_memory_current=()
declare -A initial_memory_peak=()
declare -A initial_memory_low=()
declare -A initial_memory_high=()
declare -A initial_memory_max=()
declare -A initial_memory_oom=()
declare -A initial_memory_oom_kill=()
declare -A initial_memory_oom_group_kill=()
declare -A max_sampled_memory_current=()
declare -A initial_io_rbytes=()
declare -A initial_io_wbytes=()
declare -A initial_rx_bytes=()
declare -A initial_tx_bytes=()
declare -A initial_process_voluntary_context_switches=()
declare -A initial_process_nonvoluntary_context_switches=()
declare -A initial_process_read_bytes=()
declare -A initial_process_write_bytes=()
declare -A last_nr_throttled=()
declare -A last_throttled_usec=()
declare -A last_cpu_pressure_some=()
declare -A last_cpu_pressure_full=()
declare -A last_io_pressure_some=()
declare -A last_io_pressure_full=()
declare -A last_memory_pressure_some=()
declare -A last_memory_pressure_full=()
declare -A last_io_rbytes=()
declare -A last_io_wbytes=()
declare -A last_rx_bytes=()
declare -A last_tx_bytes=()
declare -A last_process_voluntary_context_switches=()
declare -A last_process_nonvoluntary_context_switches=()
declare -A last_process_read_bytes=()
declare -A last_process_write_bytes=()
declare -A isolated_window_usec=()
declare -A isolated_process_usage_usec=()
declare -A isolated_cgroup_usage_usec=()
declare -A isolated_idle_streak_usec=()
declare -A isolated_max_idle_streak_usec=()
declare -A isolated_avoidable_idle_streak_usec=()
declare -A isolated_max_avoidable_idle_streak_usec=()
declare -A isolated_process_voluntary_context_switches=()
declare -A isolated_process_nonvoluntary_context_switches=()
declare -A isolated_process_read_bytes=()
declare -A isolated_process_write_bytes=()
declare -A classification_samples=()
declare -A classification_usec=()
declare -A classification_current=()
declare -A classification_current_streak_usec=()
declare -A classification_max_streak_usec=()

printf 'node\tkey\tvalue\n' >"$ARTIFACTS/memory-events.before.tsv"
baseline_timestamp_ns="$(date +%s%N)"
for node in "${NODES[@]}"; do
  cgroup="${container_cgroup[$node]}"
  pid="${container_pid[$node]}"
  read_cpu_stat "$cgroup/cpu.stat"
  previous_usage[$node]="$sample_usage_usec"
  read_process_observation "${main_daemon_host_pid[$node]}" \
    || die "$node main daemon host PID ${main_daemon_host_pid[$node]} disappeared before measurement"
  previous_process_usage_usec[$node]="$sample_process_usage_usec"
  process_start_ticks[$node]="$sample_process_start_ticks"
  previous_process_voluntary_context_switches[$node]="$sample_process_voluntary_context_switches"
  previous_process_nonvoluntary_context_switches[$node]="$sample_process_nonvoluntary_context_switches"
  previous_process_read_bytes[$node]="$sample_process_read_bytes"
  previous_process_write_bytes[$node]="$sample_process_write_bytes"
  process_io_complete[$node]=1
  if (( sample_process_read_bytes < 0 || sample_process_write_bytes < 0 )); then
    process_io_complete[$node]=0
  fi
  initial_process_voluntary_context_switches[$node]="$sample_process_voluntary_context_switches"
  initial_process_nonvoluntary_context_switches[$node]="$sample_process_nonvoluntary_context_switches"
  initial_process_read_bytes[$node]="$sample_process_read_bytes"
  initial_process_write_bytes[$node]="$sample_process_write_bytes"
  previous_timestamp_ns[$node]="$baseline_timestamp_ns"
  initial_nr_throttled[$node]="$sample_nr_throttled"
  previous_nr_throttled[$node]="$sample_nr_throttled"
  initial_throttled_usec[$node]="$sample_throttled_usec"
  read_pressure "$cgroup/cpu.pressure"
  initial_cpu_pressure_some[$node]="$sample_pressure_some"
  previous_cpu_pressure_some[$node]="$sample_pressure_some"
  initial_cpu_pressure_full[$node]="$sample_pressure_full"
  read_pressure "$cgroup/io.pressure"
  initial_io_pressure_some[$node]="$sample_pressure_some"
  previous_io_pressure_some[$node]="$sample_pressure_some"
  initial_io_pressure_full[$node]="$sample_pressure_full"
  read_pressure "$cgroup/memory.pressure"
  initial_memory_pressure_some[$node]="$sample_pressure_some"
  initial_memory_pressure_full[$node]="$sample_pressure_full"
  read_memory_observation "$cgroup"
  initial_memory_current[$node]="$sample_memory_current"
  initial_memory_peak[$node]="$sample_memory_peak"
  initial_memory_low[$node]="$sample_memory_low"
  initial_memory_high[$node]="$sample_memory_high"
  initial_memory_max[$node]="$sample_memory_max"
  initial_memory_oom[$node]="$sample_memory_oom"
  initial_memory_oom_kill[$node]="$sample_memory_oom_kill"
  initial_memory_oom_group_kill[$node]="$sample_memory_oom_group_kill"
  max_sampled_memory_current[$node]="$sample_memory_current"
  while read -r memory_event_key memory_event_value _; do
    printf '%s\t%s\t%s\n' "$node" "$memory_event_key" "$memory_event_value"
  done <"$cgroup/memory.events" >>"$ARTIFACTS/memory-events.before.tsv"
  read_io_stat "$cgroup/io.stat"
  initial_io_rbytes[$node]="$sample_io_rbytes"
  initial_io_wbytes[$node]="$sample_io_wbytes"
  previous_io_rbytes[$node]="$sample_io_rbytes"
  previous_io_wbytes[$node]="$sample_io_wbytes"
  read_net_stat "/proc/$pid/net/dev"
  initial_rx_bytes[$node]="$sample_rx_bytes"
  initial_tx_bytes[$node]="$sample_tx_bytes"
  previous_rx_bytes[$node]="$sample_rx_bytes"
  previous_tx_bytes[$node]="$sample_tx_bytes"
done
for lane in "${LANES[@]}"; do
  isolated_window_usec[$lane]=0
  isolated_process_usage_usec[$lane]=0
  isolated_cgroup_usage_usec[$lane]=0
  isolated_idle_streak_usec[$lane]=0
  isolated_max_idle_streak_usec[$lane]=0
  isolated_avoidable_idle_streak_usec[$lane]=0
  isolated_max_avoidable_idle_streak_usec[$lane]=0
  isolated_process_voluntary_context_switches[$lane]=0
  isolated_process_nonvoluntary_context_switches[$lane]=0
  isolated_process_read_bytes[$lane]=0
  isolated_process_write_bytes[$lane]=0
done

printf '%s\n' \
  'timestamp_ns,elapsed_ms,phase,workload_lanes,node,active_primary_lanes,main_daemon_host_pid,main_daemon_leader_state,main_daemon_thread_count,main_daemon_runnable_threads,main_daemon_sleeping_threads,main_daemon_uninterruptible_threads,main_daemon_other_threads,main_daemon_cpu_ticks,main_daemon_cpu_usage_usec,main_daemon_cpu_percent,main_daemon_thread_voluntary_context_switches,main_daemon_thread_voluntary_context_switches_delta,main_daemon_thread_nonvoluntary_context_switches,main_daemon_thread_nonvoluntary_context_switches_delta,main_daemon_read_bytes,main_daemon_read_bytes_delta,main_daemon_write_bytes,main_daemon_write_bytes_delta,cgroup_cpu_usage_usec,cgroup_cpu_percent,nr_throttled,throttled_usec,cpu_some_usec,cpu_full_usec,io_some_usec,io_full_usec,memory_some_usec,memory_full_usec,io_rbytes,io_wbytes,rx_bytes,tx_bytes,cpu_pressure_some_delta_usec,io_pressure_some_delta_usec,cgroup_io_bytes_delta,network_bytes_delta,low_cpu_classification,memory_current_bytes,memory_peak_bytes,memory_events_low,memory_events_high,memory_events_max,memory_events_oom,memory_events_oom_kill,memory_events_oom_group_kill' \
  >"$ARTIFACTS/timeline.csv"

declare -A workload_pid=()
declare -A workload_started_ns=()
declare -A ready_lanes=()
declare -a active_lanes=()
declare -a workload_phases=(isolated concurrent)
declare -a sustained_phases=()
declare -A sustained_round_wall_usec=()
workloads_finished=0
workload_failed=0
overlap_window_usec=0
overlap_process_usage_usec=0
overlap_cgroup_usage_usec=0
overlap_process_voluntary_context_switches=0
overlap_process_nonvoluntary_context_switches=0
overlap_process_read_bytes=0
overlap_process_write_bytes=0
iteration_overlap_process_usage=0
iteration_overlap_cgroup_usage=0
min_cpu_milli=$((MIN_PRIMARY_CPU_PERCENT * 1000))
measurement_started_ns="$(date +%s%N)"
docker_log_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
printf '%s\n' \
  'timestamp_ns,elapsed_ms,phase,workload_lanes,memory_total_bytes,memory_available_bytes,swap_total_bytes,swap_free_bytes' \
  >"$ARTIFACTS/host-memory.csv"
host_min_memory_available_bytes=-1
host_last_memory_available_bytes=0
host_last_swap_free_bytes=0
record_host_memory_observation "$measurement_started_ns" 0 preflight ""
host_initial_memory_total_bytes="$sample_host_memory_total_bytes"
host_initial_memory_available_bytes="$sample_host_memory_available_bytes"
host_initial_swap_total_bytes="$sample_host_swap_total_bytes"
host_initial_swap_free_bytes="$sample_host_swap_free_bytes"
declare -A main_daemon_stderr_start_bytes=()
declare -A main_daemon_stdout_start_bytes=()
main_daemon_stderr_capture_done=0
main_daemon_stdout_capture_done=0
main_daemon_stderr_scan_done=0
main_daemon_stderr_scan_failed=0

main_daemon_stderr_size() {
  local node="$1"
  run_container_timeout 5 "$node" sh -c '
    test -f /data/daemon/main.stderr.log \
      && stat -c %s /data/daemon/main.stderr.log
  '
}

main_daemon_stdout_size() {
  local node="$1"
  run_container_timeout 5 "$node" sh -c '
    test -f /data/daemon/main.stdout.log \
      && stat -c %s /data/daemon/main.stdout.log
  '
}

for node in "${NODES[@]}"; do
  capture_error="$ARTIFACTS/main-daemon-$node.workload.stderr.capture.log"
  if start_bytes="$(main_daemon_stderr_size "$node" 2>"$capture_error")" \
    && [[ "$start_bytes" =~ ^[0-9]+$ ]]; then
    main_daemon_stderr_start_bytes[$node]="$start_bytes"
  else
    main_daemon_stderr_start_bytes[$node]=-1
    log "$node main daemon stderr start offset unavailable; workload capture will fall back to the current full log"
  fi
  capture_error="$ARTIFACTS/main-daemon-$node.workload.stdout.capture.log"
  if start_bytes="$(main_daemon_stdout_size "$node" 2>"$capture_error")" \
    && [[ "$start_bytes" =~ ^[0-9]+$ ]]; then
    main_daemon_stdout_start_bytes[$node]="$start_bytes"
  else
    main_daemon_stdout_start_bytes[$node]=-1
    log "$node main daemon stdout start offset unavailable; DFS perf trace capture will fall back to the current full log"
  fi
done

capture_main_daemon_stderr_workload() {
  local node start_bytes end_bytes offset count mode status artifact artifact_name
  local capture_error
  (( main_daemon_stderr_capture_done == 0 )) || return 0
  main_daemon_stderr_capture_done=1
  printf 'node\tstart_bytes\tend_bytes\tmode\tstatus\tartifact\n' \
    >"$ARTIFACTS/main-daemon-stderr-workload.tsv"
  for node in "${NODES[@]}"; do
    start_bytes="${main_daemon_stderr_start_bytes[$node]:--1}"
    artifact_name="main-daemon-$node.workload.stderr.log"
    artifact="$ARTIFACTS/$artifact_name"
    capture_error="$ARTIFACTS/main-daemon-$node.workload.stderr.capture.log"
    end_bytes=-1
    offset=0
    count=0
    mode=unavailable
    status=failed
    if end_bytes="$(main_daemon_stderr_size "$node" 2>>"$capture_error")" \
      && [[ "$end_bytes" =~ ^[0-9]+$ ]]; then
      if (( start_bytes >= 0 && end_bytes >= start_bytes )); then
        offset="$start_bytes"
        count=$((end_bytes - start_bytes))
        mode=appended-byte-range
      else
        # A missing baseline or a smaller current file means the daemon log was
        # replaced/truncated. Preserve the complete current file as bounded by
        # the observed end size and make that fallback explicit in the manifest.
        offset=0
        count="$end_bytes"
        if (( start_bytes < 0 )); then
          mode=full-current-no-start-offset
        else
          mode=full-current-after-reset
        fi
      fi
      if run_container_timeout 5 "$node" sh -c '
          file="$1"
          offset="$2"
          count="$3"
          test -f "$file" || exit 2
          test "$count" -eq 0 && exit 0
          tail -c "+$((offset + 1))" "$file" | head -c "$count"
        ' sh /data/daemon/main.stderr.log "$offset" "$count" \
          >"$artifact" 2>>"$capture_error"; then
        status=ok
      fi
    else
      end_bytes=-1
      if run_container_timeout 5 "$node" cat /data/daemon/main.stderr.log \
          >"$artifact" 2>>"$capture_error"; then
        mode=full-current-end-size-unavailable
        status=best-effort
      fi
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$node" "$start_bytes" "$end_bytes" "$mode" "$status" "$artifact_name" \
      >>"$ARTIFACTS/main-daemon-stderr-workload.tsv"
    if [[ "$status" == failed ]]; then
      log "$node main daemon stderr workload-window capture unavailable; inspect $capture_error"
    fi
  done
}

capture_main_daemon_stdout_workload() {
  local node start_bytes end_bytes offset count mode status artifact artifact_name
  local capture_error
  (( main_daemon_stdout_capture_done == 0 )) || return 0
  main_daemon_stdout_capture_done=1
  printf 'node\tstart_bytes\tend_bytes\tmode\tstatus\tartifact\n' \
    >"$ARTIFACTS/main-daemon-stdout-workload.tsv"
  for node in "${NODES[@]}"; do
    start_bytes="${main_daemon_stdout_start_bytes[$node]:--1}"
    artifact_name="main-daemon-$node.workload.stdout.log"
    artifact="$ARTIFACTS/$artifact_name"
    capture_error="$ARTIFACTS/main-daemon-$node.workload.stdout.capture.log"
    end_bytes=-1
    offset=0
    count=0
    mode=unavailable
    status=failed
    if end_bytes="$(main_daemon_stdout_size "$node" 2>>"$capture_error")" \
      && [[ "$end_bytes" =~ ^[0-9]+$ ]]; then
      if (( start_bytes >= 0 && end_bytes >= start_bytes )); then
        offset="$start_bytes"
        count=$((end_bytes - start_bytes))
        mode=appended-byte-range
      else
        offset=0
        count="$end_bytes"
        if (( start_bytes < 0 )); then
          mode=full-current-no-start-offset
        else
          mode=full-current-after-reset
        fi
      fi
      if run_container_timeout 5 "$node" sh -c '
          file="$1"
          offset="$2"
          count="$3"
          test -f "$file" || exit 2
          test "$count" -eq 0 && exit 0
          tail -c "+$((offset + 1))" "$file" | head -c "$count"
        ' sh /data/daemon/main.stdout.log "$offset" "$count" \
          >"$artifact" 2>>"$capture_error"; then
        status=ok
      fi
    else
      end_bytes=-1
      if run_container_timeout 5 "$node" cat /data/daemon/main.stdout.log \
          >"$artifact" 2>>"$capture_error"; then
        mode=full-current-end-size-unavailable
        status=best-effort
      fi
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$node" "$start_bytes" "$end_bytes" "$mode" "$status" "$artifact_name" \
      >>"$ARTIFACTS/main-daemon-stdout-workload.tsv"
    if [[ "$status" == failed ]]; then
      log "$node main daemon stdout workload-window capture unavailable; DFS perf timings will be marked not observed"
    fi
  done
}

# Main-daemon stderr is deliberately more selective than the container/shadow
# log scan below. The daemon redirects ordinary warning diagnostics here, and
# bounded probes can legitimately leave messages such as client broken pipes,
# join-challenge failures, or health-probe timeouts. Those lines are valuable
# evidence but are not proof that the DFS workload retried or that the daemon
# failed. These patterns target process-fatal signals, explicit daemon task
# termination, DFS errors, and positive retry counters without treating a
# harmless `retry_count=0`/`failed=0` metric as a failure.
MAIN_DAEMON_STDERR_PANIC_PATTERN='(^|[^[:alnum:]_])(panicked|panic:|panic in a destructor|panic during cleanup|cannot unwind)([^[:alnum:]_]|$)'
MAIN_DAEMON_STDERR_FATAL_PATTERN='(^|[^[:alnum:]_])(fatal runtime error|fatal error|fatal:|out of memory|memory allocation of [0-9]+ bytes failed|segmentation fault|core dumped|sigsegv|sigabrt)([^[:alnum:]_]|$)'
MAIN_DAEMON_STDERR_LEVEL_PATTERN='(^|[^[:alnum:]_])(ERROR|FATAL)([^[:alnum:]_]|$)'
MAIN_DAEMON_STDERR_DAEMON_ERROR_PATTERN='(^error:|sinomo (main )?daemon .*task stopped with error:|sinomo (main )?daemon (startup|runtime) failed:)'
MAIN_DAEMON_STDERR_DFS_RETRY_PATTERN='(sinomo daemon dfs deferred .*failed([:;]|$)|sinomo daemon dfs commit[- ]promotion recovery remains pending;[[:space:]]*retrying|write did not commit)'
MAIN_DAEMON_STDERR_DFS_ERROR_PATTERN='sinomo daemon dfs .*failed([:;]|$)'
MAIN_DAEMON_STDERR_DFS_PERF_ERROR_PATTERN='^\[sinomo-dfs-perf\][[:space:]]+[^[:space:]]*(-error|-failed)([[:space:]]|$)'
MAIN_DAEMON_STDERR_RETRY_COUNTER_PATTERN='(^|[^[:alnum:]_])([[:alnum:]_]*_)?(retry([-_]?count)?|retries)[=:][[:space:]]*[1-9][0-9]*([^0-9]|$)'

scan_main_daemon_stderr_workload() {
  local summary="$ARTIFACTS/main-daemon-stderr-scan.tsv"
  local findings="$ARTIFACTS/main-daemon-stderr-findings.tsv"
  local policy="$ARTIFACTS/main-daemon-stderr-scan-policy.tsv"
  local node artifact_name artifact capture_mode capture_status scan_status
  local normalized lower excerpt category line_number finding_count
  local total_findings=0 scan_failed=0

  if (( main_daemon_stderr_scan_done != 0 )); then
    if (( main_daemon_stderr_scan_failed != 0 )); then
      return 1
    fi
    return 0
  fi
  main_daemon_stderr_scan_done=1

  printf 'category\tcase\tgate\textended_regex\n' >"$policy"
  printf 'panic\tinsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_PANIC_PATTERN" >>"$policy"
  printf 'fatal-process\tinsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_FATAL_PATTERN" >>"$policy"
  printf 'error-level\tsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_LEVEL_PATTERN" >>"$policy"
  printf 'daemon-error\tinsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_DAEMON_ERROR_PATTERN" >>"$policy"
  printf 'dfs-retry-path\tinsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_DFS_RETRY_PATTERN" >>"$policy"
  printf 'dfs-error\tinsensitive\tfail\t%s\n' "$MAIN_DAEMON_STDERR_DFS_ERROR_PATTERN" >>"$policy"
  printf 'dfs-perf-error\tinsensitive\tfail\t%s\n' \
    "$MAIN_DAEMON_STDERR_DFS_PERF_ERROR_PATTERN" >>"$policy"
  printf 'positive-retry-counter\tinsensitive\tfail\t%s\n' \
    "$MAIN_DAEMON_STDERR_RETRY_COUNTER_PATTERN" >>"$policy"
  printf 'expected-transient\tn/a\tretain-only\tgeneric warning/error/failed/retry/timed-out/unavailable words do not match without one of the conditions above\n' \
    >>"$policy"

  printf 'node\tcapture_mode\tcapture_status\tscan_status\tfinding_count\tartifact\n' \
    >"$summary"
  printf 'node\tcategory\tline\tartifact\texcerpt\n' >"$findings"

  for node in "${NODES[@]}"; do
    artifact_name="main-daemon-$node.workload.stderr.log"
    artifact="$ARTIFACTS/$artifact_name"
    capture_mode="$(awk -F '\t' -v node="$node" '$1 == node { print $4; exit }' \
      "$ARTIFACTS/main-daemon-stderr-workload.tsv")"
    capture_status="$(awk -F '\t' -v node="$node" '$1 == node { print $5; exit }' \
      "$ARTIFACTS/main-daemon-stderr-workload.tsv")"
    capture_mode="${capture_mode:-unknown}"
    capture_status="${capture_status:-unknown}"
    finding_count=0
    line_number=0

    if [[ ! -f "$artifact" ]]; then
      scan_status=unavailable
    elif [[ ! -s "$artifact" ]]; then
      scan_status=empty
    else
      scan_status=scanned
      # Strip ANSI SGR/color sequences before matching severity tokens while
      # preserving the original byte-range artifact unchanged.
      while IFS= read -r normalized || [[ -n "$normalized" ]]; do
        line_number=$((line_number + 1))
        normalized="${normalized//$'\r'/}"
        lower="${normalized,,}"
        category=""
        if [[ "$lower" =~ $MAIN_DAEMON_STDERR_PANIC_PATTERN ]]; then
          category=panic
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_FATAL_PATTERN ]]; then
          category=fatal-process
        elif [[ "$normalized" =~ $MAIN_DAEMON_STDERR_LEVEL_PATTERN ]]; then
          category=error-level
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_DAEMON_ERROR_PATTERN ]]; then
          category=daemon-error
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_DFS_RETRY_PATTERN ]]; then
          category=dfs-retry-path
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_DFS_ERROR_PATTERN ]]; then
          category=dfs-error
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_DFS_PERF_ERROR_PATTERN ]]; then
          category=dfs-perf-error
        elif [[ "$lower" =~ $MAIN_DAEMON_STDERR_RETRY_COUNTER_PATTERN ]]; then
          category=positive-retry-counter
        fi
        if [[ -n "$category" ]]; then
          finding_count=$((finding_count + 1))
          excerpt="${normalized//$'\t'/ }"
          excerpt="${excerpt:0:500}"
          printf '%s\t%s\t%s\t%s\t%s\n' \
            "$node" "$category" "$line_number" "$artifact_name" "$excerpt" \
            >>"$findings"
        fi
      done < <(LC_ALL=C sed -E $'s/\033\\[[0-9;]*[mK]//g' "$artifact")
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$node" "$capture_mode" "$capture_status" "$scan_status" \
      "$finding_count" "$artifact_name" >>"$summary"
    total_findings=$((total_findings + finding_count))
    if (( finding_count > 0 )); then
      log "$node main daemon stderr contains $finding_count panic/fatal/DFS-error/retry-path finding(s)"
      scan_failed=1
    fi
  done

  log "main daemon stderr scan: findings=$total_findings summary=$summary"
  if (( total_findings > 0 )); then
    log "main daemon stderr findings: $findings"
  fi
  main_daemon_stderr_scan_failed="$scan_failed"
  (( scan_failed == 0 ))
}

record_classification() {
  local phase="$1" lane="$2" classification="$3" elapsed_usec="$4"
  local phase_lane="$phase|$lane"
  local key="$phase_lane|$classification"
  classification_samples[$key]=$(( ${classification_samples[$key]:-0} + 1 ))
  classification_usec[$key]=$(( ${classification_usec[$key]:-0} + elapsed_usec ))
  if [[ "${classification_current[$phase_lane]:-}" == "$classification" ]]; then
    classification_current_streak_usec[$phase_lane]=$((
      ${classification_current_streak_usec[$phase_lane]:-0} + elapsed_usec
    ))
  else
    classification_current[$phase_lane]="$classification"
    classification_current_streak_usec[$phase_lane]="$elapsed_usec"
  fi
  if (( classification_current_streak_usec[$phase_lane] > ${classification_max_streak_usec[$key]:-0} )); then
    classification_max_streak_usec[$key]="${classification_current_streak_usec[$phase_lane]}"
  fi
}

cleanup_workloads() {
  local lane pid
  (( workloads_finished == 1 )) && return 0
  for lane in "${LANES[@]}"; do
    pid="${workload_pid[$lane]:-}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
cleanup_and_capture_workload_evidence() {
  cleanup_workloads
  capture_main_daemon_stderr_workload || true
  capture_main_daemon_stdout_workload || true
  scan_main_daemon_stderr_workload || true
}
trap cleanup_and_capture_workload_evidence EXIT INT TERM

launch_workload() {
  local phase="$1" lane="$2"
  local client="${lane_client[$lane]}"
  local base overwrite_existing=false
  if [[ "$phase" == churn-* ]]; then
    # Keep the live namespace bounded during long runs. The first round
    # creates one stable file set and later rounds overwrite those exact paths,
    # exercising version/history GC and retry semantics without making host
    # memory scale with the total operation count.
    base="${lane_base[$lane]%/}/$RUN_ID-sustained-$lane"
    if [[ "$phase" != churn-0001 ]]; then
      overwrite_existing=true
    fi
  else
    base="${lane_base[$lane]%/}/$RUN_ID-$phase-$lane"
  fi
  workload_started_ns[$lane]="$(date +%s%N)"
  (
    run_node_timeout "$TIMEOUT_SECONDS" "$client" \
      sinomo test run cluster.dfs.bulk-write --require-cluster --json \
      --param "base=$base" \
      --param "files=$FILES" \
      --param "bytes=$BYTES" \
      --param "overwrite-existing=$overwrite_existing" \
      --param "min-files-per-second=0" \
      --param "timeout-seconds=$TIMEOUT_SECONDS"
  ) >"$ARTIFACTS/workload-$phase-$lane.json" \
    2>"$ARTIFACTS/workload-$phase-$lane.stderr.log" &
  workload_pid[$lane]=$!
  log "  $phase/$lane: client=$client primary=${lane_primary_service[$lane]} pid=${workload_pid[$lane]}"
}

sample_phase() {
  local phase="$1" all_ready="$2" now_ns="$3"
  local node lane cgroup pid daemon_host_pid usage_usec nr_throttled throttled_usec
  local elapsed_usec usage_delta cpu_milli cpu_percent
  local process_usage_usec process_usage_delta process_cpu_milli process_cpu_percent process_cpu_ticks
  local process_leader_state process_thread_count process_runnable_threads
  local process_sleeping_threads process_uninterruptible_threads process_other_threads
  local process_voluntary_context_switches process_voluntary_context_switches_delta
  local process_nonvoluntary_context_switches process_nonvoluntary_context_switches_delta
  local process_read_bytes process_read_bytes_delta process_write_bytes process_write_bytes_delta
  local cpu_some cpu_full io_some io_full memory_some memory_full
  local memory_current memory_peak memory_low memory_high memory_max
  local memory_oom memory_oom_kill memory_oom_group_kill
  local io_rbytes io_wbytes rx_bytes tx_bytes primary_lanes elapsed_ms
  local cpu_pressure_some_delta io_pressure_some_delta cgroup_io_bytes_delta network_bytes_delta
  local nr_throttled_delta sample_classification
  local workload_lanes=""
  for lane in "${active_lanes[@]}"; do
    workload_lanes="${workload_lanes:+$workload_lanes+}$lane"
  done
  elapsed_ms=$(((now_ns - measurement_started_ns) / 1000000))
  record_host_memory_observation "$now_ns" "$elapsed_ms" "$phase" "$workload_lanes"
  iteration_overlap_process_usage=0
  iteration_overlap_cgroup_usage=0
  for node in "${NODES[@]}"; do
    cgroup="${container_cgroup[$node]}"
    pid="${container_pid[$node]}"
    daemon_host_pid="${main_daemon_host_pid[$node]}"
    read_cpu_stat "$cgroup/cpu.stat"
    usage_usec="$sample_usage_usec"
    nr_throttled="$sample_nr_throttled"
    throttled_usec="$sample_throttled_usec"
    elapsed_usec=$(((now_ns - previous_timestamp_ns[$node]) / 1000))
    (( elapsed_usec > 0 )) || elapsed_usec=1
    usage_delta=$((usage_usec - previous_usage[$node]))
    (( usage_delta >= 0 )) || die "$node CPU counter regressed; container likely restarted"
    cpu_milli=$((usage_delta * 100000 / elapsed_usec))
    printf -v cpu_percent '%d.%03d' $((cpu_milli / 1000)) $((cpu_milli % 1000))
    read_process_observation "$daemon_host_pid" \
      || die "$node main daemon host PID $daemon_host_pid disappeared during measurement"
    [[ "$sample_process_start_ticks" == "${process_start_ticks[$node]}" ]] \
      || die "$node main daemon host PID $daemon_host_pid was replaced during measurement"
    process_cpu_ticks="$sample_process_cpu_ticks"
    process_usage_usec="$sample_process_usage_usec"
    process_leader_state="$sample_process_leader_state"
    process_thread_count="$sample_process_thread_count"
    process_runnable_threads="$sample_process_runnable_threads"
    process_sleeping_threads="$sample_process_sleeping_threads"
    process_uninterruptible_threads="$sample_process_uninterruptible_threads"
    process_other_threads="$sample_process_other_threads"
    process_voluntary_context_switches="$sample_process_voluntary_context_switches"
    process_nonvoluntary_context_switches="$sample_process_nonvoluntary_context_switches"
    process_read_bytes="$sample_process_read_bytes"
    process_write_bytes="$sample_process_write_bytes"
    if (( process_read_bytes < 0 || process_write_bytes < 0 )); then
      process_io_complete[$node]=0
    fi
    process_usage_delta=$((process_usage_usec - previous_process_usage_usec[$node]))
    (( process_usage_delta >= 0 )) \
      || die "$node main daemon CPU counter regressed; process likely restarted"
    process_cpu_milli=$((process_usage_delta * 100000 / elapsed_usec))
    printf -v process_cpu_percent '%d.%03d' \
      $((process_cpu_milli / 1000)) $((process_cpu_milli % 1000))
    process_voluntary_context_switches_delta=$((
      process_voluntary_context_switches - previous_process_voluntary_context_switches[$node]
    ))
    process_nonvoluntary_context_switches_delta=$((
      process_nonvoluntary_context_switches - previous_process_nonvoluntary_context_switches[$node]
    ))
    # Aggregate thread counters can regress if a daemon worker exits between
    # samples. Preserve the exact snapshots and avoid inventing a negative
    # amount of scheduling activity for that interval.
    (( process_voluntary_context_switches_delta >= 0 )) \
      || process_voluntary_context_switches_delta=0
    (( process_nonvoluntary_context_switches_delta >= 0 )) \
      || process_nonvoluntary_context_switches_delta=0
    process_read_bytes_delta=0
    process_write_bytes_delta=0
    if (( process_read_bytes >= 0 && previous_process_read_bytes[$node] >= 0 )); then
      process_read_bytes_delta=$((process_read_bytes - previous_process_read_bytes[$node]))
      (( process_read_bytes_delta >= 0 )) \
        || die "$node main daemon read-byte counter regressed; process likely restarted"
    fi
    if (( process_write_bytes >= 0 && previous_process_write_bytes[$node] >= 0 )); then
      process_write_bytes_delta=$((process_write_bytes - previous_process_write_bytes[$node]))
      (( process_write_bytes_delta >= 0 )) \
        || die "$node main daemon write-byte counter regressed; process likely restarted"
    fi

    read_pressure "$cgroup/cpu.pressure"
    cpu_some="$sample_pressure_some"
    cpu_full="$sample_pressure_full"
    read_pressure "$cgroup/io.pressure"
    io_some="$sample_pressure_some"
    io_full="$sample_pressure_full"
    read_pressure "$cgroup/memory.pressure"
    memory_some="$sample_pressure_some"
    memory_full="$sample_pressure_full"
    read_memory_observation "$cgroup"
    memory_current="$sample_memory_current"
    memory_peak="$sample_memory_peak"
    memory_low="$sample_memory_low"
    memory_high="$sample_memory_high"
    memory_max="$sample_memory_max"
    memory_oom="$sample_memory_oom"
    memory_oom_kill="$sample_memory_oom_kill"
    memory_oom_group_kill="$sample_memory_oom_group_kill"
    if (( memory_current > max_sampled_memory_current[$node] )); then
      max_sampled_memory_current[$node]="$memory_current"
    fi
    read_io_stat "$cgroup/io.stat"
    io_rbytes="$sample_io_rbytes"
    io_wbytes="$sample_io_wbytes"
    read_net_stat "/proc/$pid/net/dev"
    rx_bytes="$sample_rx_bytes"
    tx_bytes="$sample_tx_bytes"
    nr_throttled_delta=$((nr_throttled - previous_nr_throttled[$node]))
    cpu_pressure_some_delta=$((cpu_some - previous_cpu_pressure_some[$node]))
    io_pressure_some_delta=$((io_some - previous_io_pressure_some[$node]))
    cgroup_io_bytes_delta=$((
      io_rbytes - previous_io_rbytes[$node] + io_wbytes - previous_io_wbytes[$node]
    ))
    network_bytes_delta=$((
      rx_bytes - previous_rx_bytes[$node] + tx_bytes - previous_tx_bytes[$node]
    ))
    if (( nr_throttled_delta < 0 || cpu_pressure_some_delta < 0 \
        || io_pressure_some_delta < 0 || cgroup_io_bytes_delta < 0 \
        || network_bytes_delta < 0 )); then
      die "$node cumulative cgroup/network counter regressed during measurement"
    fi

    primary_lanes=""
    for lane in "${active_lanes[@]}"; do
      if [[ "${lane_primary_service[$lane]}" == "$node" && "${ready_lanes[$lane]}" == 1 ]]; then
        primary_lanes="${primary_lanes:+$primary_lanes+}$lane"
      fi
    done
    sample_classification="not_in_ready_primary_window"
    if [[ -n "$primary_lanes" ]]; then
      if (( process_cpu_milli >= min_cpu_milli )); then
        sample_classification="cpu_active"
      elif (( process_runnable_threads > 0 )); then
        if (( nr_throttled_delta > 0 || cpu_pressure_some_delta > 0 )); then
          sample_classification="low_cpu_ready_cpu_pressure"
        elif (( process_uninterruptible_threads > 0 || io_pressure_some_delta > 0 \
            || process_read_bytes_delta > 0 || process_write_bytes_delta > 0 \
            || cgroup_io_bytes_delta > 0 )); then
          sample_classification="low_cpu_ready_io_pressure_or_progress"
        elif (( network_bytes_delta > 0 )); then
          sample_classification="low_cpu_ready_network_progress"
        else
          sample_classification="low_cpu_ready_no_resource_wait_or_progress"
        fi
      elif (( process_uninterruptible_threads > 0 || io_pressure_some_delta > 0 \
          || process_read_bytes_delta > 0 || process_write_bytes_delta > 0 \
          || cgroup_io_bytes_delta > 0 )); then
        sample_classification="low_cpu_waiting_io_pressure_or_progress"
      elif (( network_bytes_delta > 0 )); then
        sample_classification="low_cpu_sleeping_network_progress"
      elif (( nr_throttled_delta > 0 || cpu_pressure_some_delta > 0 )); then
        sample_classification="low_cpu_sleeping_cgroup_cpu_pressure"
      else
        sample_classification="low_cpu_sleeping_no_observed_external_progress"
      fi
      if [[ "$phase" == "isolated" || "$all_ready" == 1 ]]; then
        lane="${primary_lanes%%+*}"
        record_classification "$phase" "$lane" "$sample_classification" "$elapsed_usec"
      fi
    fi
    if [[ "$phase" == "isolated" && -n "$primary_lanes" ]]; then
      lane="${active_lanes[0]}"
      isolated_window_usec[$lane]=$((isolated_window_usec[$lane] + elapsed_usec))
      isolated_process_usage_usec[$lane]=$((isolated_process_usage_usec[$lane] + process_usage_delta))
      isolated_cgroup_usage_usec[$lane]=$((isolated_cgroup_usage_usec[$lane] + usage_delta))
      if (( process_cpu_milli < min_cpu_milli )); then
        isolated_idle_streak_usec[$lane]=$((isolated_idle_streak_usec[$lane] + elapsed_usec))
        if (( isolated_idle_streak_usec[$lane] > isolated_max_idle_streak_usec[$lane] )); then
          isolated_max_idle_streak_usec[$lane]="${isolated_idle_streak_usec[$lane]}"
        fi
      else
        isolated_idle_streak_usec[$lane]=0
      fi
      if [[ "$sample_classification" == "low_cpu_ready_no_resource_wait_or_progress" ]]; then
        isolated_avoidable_idle_streak_usec[$lane]=$((
          isolated_avoidable_idle_streak_usec[$lane] + elapsed_usec
        ))
        if (( isolated_avoidable_idle_streak_usec[$lane] > isolated_max_avoidable_idle_streak_usec[$lane] )); then
          isolated_max_avoidable_idle_streak_usec[$lane]="${isolated_avoidable_idle_streak_usec[$lane]}"
        fi
      else
        isolated_avoidable_idle_streak_usec[$lane]=0
      fi
      isolated_process_voluntary_context_switches[$lane]=$((
        isolated_process_voluntary_context_switches[$lane] + process_voluntary_context_switches_delta
      ))
      isolated_process_nonvoluntary_context_switches[$lane]=$((
        isolated_process_nonvoluntary_context_switches[$lane] + process_nonvoluntary_context_switches_delta
      ))
      isolated_process_read_bytes[$lane]=$((
        isolated_process_read_bytes[$lane] + process_read_bytes_delta
      ))
      isolated_process_write_bytes[$lane]=$((
        isolated_process_write_bytes[$lane] + process_write_bytes_delta
      ))
    fi
    if [[ "$phase" == "concurrent" ]] \
      && (( all_ready == 1 )) \
      && [[ -n "${primary_services[$node]:-}" ]]; then
      iteration_overlap_process_usage=$((iteration_overlap_process_usage + process_usage_delta))
      iteration_overlap_cgroup_usage=$((iteration_overlap_cgroup_usage + usage_delta))
      overlap_process_voluntary_context_switches=$((
        overlap_process_voluntary_context_switches + process_voluntary_context_switches_delta
      ))
      overlap_process_nonvoluntary_context_switches=$((
        overlap_process_nonvoluntary_context_switches + process_nonvoluntary_context_switches_delta
      ))
      overlap_process_read_bytes=$((overlap_process_read_bytes + process_read_bytes_delta))
      overlap_process_write_bytes=$((overlap_process_write_bytes + process_write_bytes_delta))
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$now_ns" "$elapsed_ms" "$phase" "$workload_lanes" "$node" "$primary_lanes" \
      "$daemon_host_pid" "$process_leader_state" "$process_thread_count" \
      "$process_runnable_threads" "$process_sleeping_threads" \
      "$process_uninterruptible_threads" "$process_other_threads" \
      "$process_cpu_ticks" "$process_usage_usec" "$process_cpu_percent" \
      "$process_voluntary_context_switches" "$process_voluntary_context_switches_delta" \
      "$process_nonvoluntary_context_switches" "$process_nonvoluntary_context_switches_delta" \
      "$process_read_bytes" "$process_read_bytes_delta" \
      "$process_write_bytes" "$process_write_bytes_delta" \
      "$usage_usec" "$cpu_percent" \
      "$nr_throttled" "$throttled_usec" "$cpu_some" "$cpu_full" \
      "$io_some" "$io_full" "$memory_some" "$memory_full" \
      "$io_rbytes" "$io_wbytes" "$rx_bytes" "$tx_bytes" \
      "$cpu_pressure_some_delta" "$io_pressure_some_delta" \
      "$cgroup_io_bytes_delta" "$network_bytes_delta" "$sample_classification" \
      "$memory_current" "$memory_peak" "$memory_low" "$memory_high" "$memory_max" \
      "$memory_oom" "$memory_oom_kill" "$memory_oom_group_kill" \
      >>"$ARTIFACTS/timeline.csv"

    previous_usage[$node]="$usage_usec"
    previous_process_usage_usec[$node]="$process_usage_usec"
    previous_process_voluntary_context_switches[$node]="$process_voluntary_context_switches"
    previous_process_nonvoluntary_context_switches[$node]="$process_nonvoluntary_context_switches"
    previous_process_read_bytes[$node]="$process_read_bytes"
    previous_process_write_bytes[$node]="$process_write_bytes"
    previous_timestamp_ns[$node]="$now_ns"
    previous_nr_throttled[$node]="$nr_throttled"
    previous_cpu_pressure_some[$node]="$cpu_some"
    previous_io_pressure_some[$node]="$io_some"
    previous_io_rbytes[$node]="$io_rbytes"
    previous_io_wbytes[$node]="$io_wbytes"
    previous_rx_bytes[$node]="$rx_bytes"
    previous_tx_bytes[$node]="$tx_bytes"
    last_nr_throttled[$node]="$nr_throttled"
    last_throttled_usec[$node]="$throttled_usec"
    last_cpu_pressure_some[$node]="$cpu_some"
    last_cpu_pressure_full[$node]="$cpu_full"
    last_io_pressure_some[$node]="$io_some"
    last_io_pressure_full[$node]="$io_full"
    last_memory_pressure_some[$node]="$memory_some"
    last_memory_pressure_full[$node]="$memory_full"
    last_io_rbytes[$node]="$io_rbytes"
    last_io_wbytes[$node]="$io_wbytes"
    last_rx_bytes[$node]="$rx_bytes"
    last_tx_bytes[$node]="$tx_bytes"
    last_process_voluntary_context_switches[$node]="$process_voluntary_context_switches"
    last_process_nonvoluntary_context_switches[$node]="$process_nonvoluntary_context_switches"
    last_process_read_bytes[$node]="$process_read_bytes"
    last_process_write_bytes[$node]="$process_write_bytes"
  done
}

wait_for_workload() {
  local phase="$1" lane="$2"
  if wait "${workload_pid[$lane]}"; then
    :
  else
    log "$phase/$lane workload command failed"
    workload_failed=1
  fi
  unset "workload_pid[$lane]" "workload_started_ns[$lane]"
}

wait_for_shard_convergence() {
  local label="$1" check_lane
  local deadline=$(( $(date +%s) + CONVERGENCE_SECONDS ))
  local clean
  while (( $(date +%s) <= deadline )); do
    clean=1
    for check_lane in "${LANES[@]}"; do
      validate_primary_shard "$check_lane" "$label" || clean=0
    done
    (( clean == 1 )) && return 0
    sleep 1
  done
  return 1
}

log "workload: measuring sequential single-shard attribution windows"
for lane in "${LANES[@]}"; do
  active_lanes=("$lane")
  ready_lanes[$lane]=0
  launch_workload isolated "$lane"
  previous_loop_ns="$(date +%s%N)"

  while :; do
    now_ns="$(date +%s%N)"
    any_running=0
    ready_lanes[$lane]=0
    if kill -0 "${workload_pid[$lane]}" 2>/dev/null; then
      any_running=1
      grace_end_ns=$((workload_started_ns[$lane] + WARMUP_MS * 1000000))
      if (( previous_loop_ns >= grace_end_ns )); then
        ready_lanes[$lane]=1
      fi
    fi
    sample_phase isolated 0 "$now_ns"
    previous_loop_ns="$now_ns"
    (( any_running == 1 )) || break
    sleep "$SAMPLE_SECONDS"
  done
  wait_for_workload isolated "$lane"
  wait_for_shard_convergence "after-isolated-$lane" \
    || die "shards did not return to zero lag after the isolated $lane window"
done

run_three_lane_workload_phase() {
  local phase="$1" account_core_overlap="$2"
  local phase_started_ns phase_finished_ns
  active_lanes=("${LANES[@]}")
  phase_started_ns="$(date +%s%N)"
  for lane in "${LANES[@]}"; do
    ready_lanes[$lane]=0
    launch_workload "$phase" "$lane"
  done

  previous_loop_ns="$(date +%s%N)"
  while :; do
    now_ns="$(date +%s%N)"
    any_running=0
    all_ready=1
    for lane in "${LANES[@]}"; do
      ready_lanes[$lane]=0
      if kill -0 "${workload_pid[$lane]}" 2>/dev/null; then
        any_running=1
        grace_end_ns=$((workload_started_ns[$lane] + WARMUP_MS * 1000000))
        if (( previous_loop_ns >= grace_end_ns )); then
          ready_lanes[$lane]=1
        else
          all_ready=0
        fi
      else
        all_ready=0
      fi
    done

    sample_phase "$phase" "$all_ready" "$now_ns"

    if (( account_core_overlap == 1 && all_ready == 1 )); then
      overlap_window_usec=$((overlap_window_usec + (now_ns - previous_loop_ns) / 1000))
      overlap_process_usage_usec=$((overlap_process_usage_usec + iteration_overlap_process_usage))
      overlap_cgroup_usage_usec=$((overlap_cgroup_usage_usec + iteration_overlap_cgroup_usage))
    fi
    previous_loop_ns="$now_ns"
    (( any_running == 1 )) || break
    sleep "$SAMPLE_SECONDS"
  done

  for lane in "${LANES[@]}"; do
    wait_for_workload "$phase" "$lane"
  done
  phase_finished_ns="$(date +%s%N)"
  last_three_lane_wall_usec=$((
    (phase_finished_ns - phase_started_ns + 999) / 1000
  ))
}

log "workload: measuring concurrent three-shard aggregate utilization"
run_three_lane_workload_phase concurrent 1
concurrent_wall_usec="$last_three_lane_wall_usec"

sustained_completed_rounds=0
sustained_wall_usec=0
if (( SUSTAINED_ROUNDS > 0 )); then
  log "workload: measuring $SUSTAINED_ROUNDS back-to-back sustained churn round(s)"
  sustained_started_ns="$(date +%s%N)"
  for ((round = 1; round <= SUSTAINED_ROUNDS; round++)); do
    printf -v phase 'churn-%04d' "$round"
    workload_phases+=("$phase")
    sustained_phases+=("$phase")
    workload_failed_before_round="$workload_failed"
    run_three_lane_workload_phase "$phase" 0
    sustained_round_wall_usec[$phase]="$last_three_lane_wall_usec"
    sustained_completed_rounds=$((sustained_completed_rounds + 1))
    if (( workload_failed != workload_failed_before_round )); then
      log "sustained churn stopped after command failure in $phase"
      break
    fi
  done
  sustained_finished_ns="$(date +%s%N)"
  sustained_wall_usec=$((
    (sustained_finished_ns - sustained_started_ns + 999) / 1000
  ))
fi
workloads_finished=1
capture_main_daemon_stderr_workload
capture_main_daemon_stdout_workload
if ! scan_main_daemon_stderr_workload; then
  workload_failed=1
fi
summarize_dfs_perf_traces "$ARTIFACTS" \
  || log "DFS perf trace timing summary is partial; raw captured logs are retained"

for node in "${NODES[@]}"; do
  docker logs --timestamps --since "$docker_log_since" "${container_id[$node]}" \
    >"$ARTIFACTS/container-$node.workload.log" 2>&1 || workload_failed=1
  if grep -Eiq \
      'error|failed|panic|retry|timed out|unavailable|write did not commit' \
      "$ARTIFACTS/container-$node.workload.log"; then
    log "$node daemon log contains an error/retry marker during the workload"
    workload_failed=1
  fi
done

printf 'phase\tlane\tclient\tprimary\tshard\tfiles\toverwrite_existing\tmkdir_elapsed_ms\twrite_elapsed_ms\tpublish_elapsed_ms\telapsed_ms\tend_to_end_files_per_second\tdata_write_files_per_second\tretry_count\n' \
  >"$ARTIFACTS/workloads.tsv"
concurrent_total_files=0
sustained_total_files=0
for phase in "${workload_phases[@]}"; do
  for lane in "${LANES[@]}"; do
    result="$ARTIFACTS/workload-$phase-$lane.json"
    stderr_file="$ARTIFACTS/workload-$phase-$lane.stderr.log"
    expected_overwrite=false
    if [[ "$phase" == churn-* && "$phase" != churn-0001 ]]; then
      expected_overwrite=true
    fi
    if ! jq -e \
      --argjson expected_files "$FILES" \
      --argjson expected_overwrite "$expected_overwrite" '
      .status == "passed"
      and (.diagnostics.retry_count | type) == "number"
      and .diagnostics.retry_count >= 0
      and .diagnostics.retry_count == (.diagnostics.retry_count | floor)
      and (.diagnostics.retry_reasons | type) == "array"
      and all(.diagnostics.retry_reasons[]; type == "string")
      and (
        (.diagnostics.retry_count == 0 and (.diagnostics.retry_reasons | length) == 0)
        or
        (.diagnostics.retry_count > 0 and (.diagnostics.retry_reasons | length) > 0)
      )
      and (.diagnostics.files | type) == "number"
      and .diagnostics.files == $expected_files
      and .diagnostics.files == (.diagnostics.files | floor)
      and (.diagnostics.overwrite_existing | type) == "boolean"
      and .diagnostics.overwrite_existing == $expected_overwrite
      and (.diagnostics.elapsed_ms | type) == "number"
      and .diagnostics.elapsed_ms > 0
      and .diagnostics.elapsed_ms == (.diagnostics.elapsed_ms | floor)
      and (.diagnostics.mkdir_elapsed_ms | type) == "number"
      and .diagnostics.mkdir_elapsed_ms >= 0
      and .diagnostics.mkdir_elapsed_ms == (.diagnostics.mkdir_elapsed_ms | floor)
      and (.diagnostics.files_per_second | type) == "number"
      and .diagnostics.files_per_second > 0
      and (.diagnostics.write_elapsed_ms | type) == "number"
      and .diagnostics.write_elapsed_ms > 0
      and .diagnostics.write_elapsed_ms == (.diagnostics.write_elapsed_ms | floor)
      and (.diagnostics.publish_elapsed_ms | type) == "number"
      and .diagnostics.publish_elapsed_ms >= 0
      and .diagnostics.publish_elapsed_ms == (.diagnostics.publish_elapsed_ms | floor)
      and (.diagnostics.data_write_bytes_per_second | type) == "number"
      and .diagnostics.data_write_bytes_per_second > 0
    ' "$result" >/dev/null 2>&1; then
      log "$phase/$lane workload omitted a passing result with retry telemetry"
      workload_failed=1
      continue
    fi
    retry_count="$(jq -r '.diagnostics.retry_count' "$result")"
    if (( retry_count != 0 )); then
      log "$phase/$lane workload used $retry_count probe-level retry/retries"
      jq -r '.diagnostics.retry_reasons[]?' "$result" | sed 's/^/[dfs-shard-perf]   /'
      workload_failed=1
    fi
    if [[ -s "$stderr_file" ]] \
      && grep -Eiq 'error|failed|panic|retry|timed out|unavailable' "$stderr_file"; then
      log "$phase/$lane workload stderr contains an error/retry marker"
      workload_failed=1
    fi
    files_completed="$(jq -r '.diagnostics.files' "$result")"
    overwrite_existing="$(jq -r '.diagnostics.overwrite_existing' "$result")"
    mkdir_elapsed_ms="$(jq -r '.diagnostics.mkdir_elapsed_ms' "$result")"
    elapsed_ms="$(jq -r '.diagnostics.elapsed_ms' "$result")"
    files_per_second="$(jq -r '.diagnostics.files_per_second' "$result")"
    write_elapsed_ms="$(jq -r '.diagnostics.write_elapsed_ms' "$result")"
    publish_elapsed_ms="$(jq -r '.diagnostics.publish_elapsed_ms' "$result")"
    data_write_files_per_second="$(
      jq -r --argjson bytes_per_file "$BYTES" \
        '.diagnostics.data_write_bytes_per_second / $bytes_per_file' "$result"
    )"
    if [[ ! "$files_completed" =~ ^[0-9]+$ ]] || (( files_completed != FILES )); then
      log "$phase/$lane completed $files_completed files, expected $FILES"
      workload_failed=1
    fi
    if [[ "$phase" == "concurrent" ]]; then
      concurrent_total_files=$((concurrent_total_files + files_completed))
      lane_data_write_min="$MIN_CONCURRENT_LANE_FILES_PER_SECOND"
    elif [[ "$phase" == churn-* ]]; then
      sustained_total_files=$((sustained_total_files + files_completed))
      lane_data_write_min="$MIN_CONCURRENT_LANE_FILES_PER_SECOND"
    else
      lane_data_write_min="$MIN_ISOLATED_FILES_PER_SECOND"
    fi
    if ! jq -e \
      --argjson bytes_per_file "$BYTES" \
      --argjson minimum "$lane_data_write_min" \
      '(.diagnostics.data_write_bytes_per_second / $bytes_per_file) >= $minimum' \
      "$result" >/dev/null; then
      log "$phase/$lane bulk write throughput was ${data_write_files_per_second} publications/s, below $lane_data_write_min"
      workload_failed=1
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$phase" "$lane" "${lane_client[$lane]}" "${lane_primary_service[$lane]}" \
      "${lane_shard[$lane]}" "$files_completed" "$overwrite_existing" "$mkdir_elapsed_ms" \
      "$write_elapsed_ms" "$publish_elapsed_ms" "$elapsed_ms" \
      "$files_per_second" "$data_write_files_per_second" "$retry_count" \
      >>"$ARTIFACTS/workloads.tsv"
  done
done

gate_failed="$workload_failed"
concurrent_wall_ms=$((concurrent_wall_usec / 1000))
concurrent_aggregate_milli=0
if (( concurrent_wall_usec > 0 )); then
  concurrent_aggregate_milli=$((concurrent_total_files * 1000000000 / concurrent_wall_usec))
fi
printf -v concurrent_aggregate_files_per_second '%d.%03d' \
  $((concurrent_aggregate_milli / 1000)) $((concurrent_aggregate_milli % 1000))
if (( concurrent_wall_ms < MIN_LANE_MS )); then
  log "concurrent cluster wall-clock window was only ${concurrent_wall_ms}ms; increase SINOMO_DFS_PERF_FILES"
  gate_failed=1
fi
if (( concurrent_aggregate_milli < MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND * 1000 )); then
  log "concurrent cluster throughput was ${concurrent_aggregate_files_per_second} publications/s, below ${MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND}"
  gate_failed=1
fi

printf 'phase\twall_ms\tfiles\tend_to_end_files_per_second\n' \
  >"$ARTIFACTS/sustained-rounds.tsv"
sustained_wall_ms=$((sustained_wall_usec / 1000))
sustained_aggregate_milli=0
sustained_aggregate_files_per_second=disabled
sustained_status=disabled
if (( SUSTAINED_ROUNDS > 0 )); then
  sustained_status=passed
  if (( sustained_wall_usec > 0 )); then
    sustained_aggregate_milli=$((
      sustained_total_files * 1000000000 / sustained_wall_usec
    ))
  fi
  printf -v sustained_aggregate_files_per_second '%d.%03d' \
    $((sustained_aggregate_milli / 1000)) $((sustained_aggregate_milli % 1000))
  for phase in "${sustained_phases[@]}"; do
    round_files="$(
      awk -F '\t' -v phase="$phase" '
        NR > 1 && $1 == phase { files += $6 }
        END { print files + 0 }
      ' "$ARTIFACTS/workloads.tsv"
    )"
    round_wall_usec="${sustained_round_wall_usec[$phase]}"
    round_wall_ms=$((round_wall_usec / 1000))
    round_aggregate_milli=0
    if (( round_wall_usec > 0 )); then
      round_aggregate_milli=$((round_files * 1000000000 / round_wall_usec))
    fi
    printf -v round_aggregate_files_per_second '%d.%03d' \
      $((round_aggregate_milli / 1000)) $((round_aggregate_milli % 1000))
    printf '%s\t%s\t%s\t%s\n' \
      "$phase" "$round_wall_ms" "$round_files" "$round_aggregate_files_per_second" \
      >>"$ARTIFACTS/sustained-rounds.tsv"
  done

  sustained_expected_files=$((SUSTAINED_ROUNDS * ${#LANES[@]} * FILES))
  if (( sustained_completed_rounds != SUSTAINED_ROUNDS )); then
    log "sustained churn completed $sustained_completed_rounds of $SUSTAINED_ROUNDS requested rounds"
    sustained_status=failed
    gate_failed=1
  fi
  if (( sustained_total_files != sustained_expected_files )); then
    log "sustained churn completed $sustained_total_files files, expected $sustained_expected_files"
    sustained_status=failed
    gate_failed=1
  fi
  if (( sustained_wall_ms < MIN_LANE_MS )); then
    log "sustained churn wall-clock window was only ${sustained_wall_ms}ms; increase files or rounds"
    sustained_status=failed
    gate_failed=1
  fi
  if (( sustained_aggregate_milli < MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND * 1000 )); then
    log "sustained churn throughput was ${sustained_aggregate_files_per_second} publications/s, below ${MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND}"
    sustained_status=failed
    gate_failed=1
  fi
fi
{
  printf 'status\t%s\n' "$sustained_status"
  printf 'requested_rounds\t%s\n' "$SUSTAINED_ROUNDS"
  printf 'completed_rounds\t%s\n' "$sustained_completed_rounds"
  printf 'sustained_wall_ms\t%s\n' "$sustained_wall_ms"
  printf 'sustained_total_files\t%s\n' "$sustained_total_files"
  printf 'sustained_aggregate_files_per_second\t%s\n' \
    "$sustained_aggregate_files_per_second"
  printf 'min_sustained_aggregate_files_per_second\t%s\n' \
    "$MIN_SUSTAINED_AGGREGATE_FILES_PER_SECOND"
} >"$ARTIFACTS/sustained-summary.tsv"

printf '%s\n' \
  'lane,primary,main_daemon_host_pid,active_ms,main_daemon_average_cpu_percent,cgroup_average_cpu_percent,max_main_daemon_low_cpu_streak_ms,max_avoidable_ready_work_low_cpu_streak_ms,main_daemon_thread_voluntary_context_switches_delta,main_daemon_thread_nonvoluntary_context_switches_delta,main_daemon_process_io_observed,main_daemon_read_bytes_delta,main_daemon_write_bytes_delta' \
  >"$ARTIFACTS/isolated-primary-summary.csv"
for lane in "${LANES[@]}"; do
  active_usec="${isolated_window_usec[$lane]}"
  process_usage_usec="${isolated_process_usage_usec[$lane]}"
  cgroup_usage_usec="${isolated_cgroup_usage_usec[$lane]}"
  average_cpu_milli=0
  cgroup_average_cpu_milli=0
  if (( active_usec > 0 )); then
    average_cpu_milli=$((process_usage_usec * 100000 / active_usec))
    cgroup_average_cpu_milli=$((cgroup_usage_usec * 100000 / active_usec))
  fi
  printf -v average_cpu '%d.%03d' $((average_cpu_milli / 1000)) $((average_cpu_milli % 1000))
  printf -v cgroup_average_cpu '%d.%03d' \
    $((cgroup_average_cpu_milli / 1000)) $((cgroup_average_cpu_milli % 1000))
  active_ms=$((active_usec / 1000))
  max_idle=$((isolated_max_idle_streak_usec[$lane] / 1000))
  max_avoidable_idle=$((isolated_max_avoidable_idle_streak_usec[$lane] / 1000))
  primary_node="${lane_primary_service[$lane]}"
  process_io_observed=no
  if (( process_io_complete[$primary_node] == 1 )); then
    process_io_observed=yes
  fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$lane" "${lane_primary_service[$lane]}" \
    "${main_daemon_host_pid[${lane_primary_service[$lane]}]}" \
    "$active_ms" "$average_cpu" "$cgroup_average_cpu" "$max_idle" \
    "$max_avoidable_idle" \
    "${isolated_process_voluntary_context_switches[$lane]}" \
    "${isolated_process_nonvoluntary_context_switches[$lane]}" \
    "$process_io_observed" "${isolated_process_read_bytes[$lane]}" \
    "${isolated_process_write_bytes[$lane]}" \
    >>"$ARTIFACTS/isolated-primary-summary.csv"

  if (( active_ms < MIN_LANE_MS )); then
    log "isolated ${lane_primary_service[$lane]}/$lane measured only ${active_ms}ms; increase SINOMO_DFS_PERF_FILES"
    gate_failed=1
  fi
  if (( average_cpu_milli < min_cpu_milli )); then
    log "isolated ${lane_primary_service[$lane]}/$lane main daemon averaged ${average_cpu}% CPU, below the ${MIN_PRIMARY_CPU_PERCENT}% diagnostic floor; avoidable-idle classification determines failure"
  fi
  if (( max_avoidable_idle > MAX_IDLE_MS )); then
    log "isolated ${lane_primary_service[$lane]}/$lane main daemon had a ${max_avoidable_idle}ms low-CPU ready-work streak with no observed CPU/I/O pressure or storage/network progress"
    gate_failed=1
  elif (( max_idle > MAX_IDLE_MS )); then
    log "isolated ${lane_primary_service[$lane]}/$lane had a ${max_idle}ms low-CPU streak, but those samples carried observed state/resource evidence and were not automatically classified avoidable; see sample-classification-summary.csv"
  fi
done

avoidable_classification="low_cpu_ready_no_resource_wait_or_progress"
for phase in "${workload_phases[@]}"; do
  [[ "$phase" != isolated ]] || continue
  for lane in "${LANES[@]}"; do
    classification_key="$phase|$lane|$avoidable_classification"
    concurrent_max_avoidable_idle=$((
      ${classification_max_streak_usec[$classification_key]:-0} / 1000
    ))
    if (( concurrent_max_avoidable_idle > MAX_IDLE_MS )); then
      log "$phase ${lane_primary_service[$lane]}/$lane main daemon had a ${concurrent_max_avoidable_idle}ms low-CPU ready-work streak with no observed CPU/I/O pressure or storage/network progress"
      gate_failed=1
    fi
  done
done

printf '%s\n' \
  'phase,lane,primary,classification,samples,duration_ms,max_streak_ms' \
  >"$ARTIFACTS/sample-classification-summary.csv"
for phase in "${workload_phases[@]}"; do
  for lane in "${LANES[@]}"; do
    for sample_classification in "${SAMPLE_CLASSIFICATIONS[@]}"; do
      classification_key="$phase|$lane|$sample_classification"
      sample_count="${classification_samples[$classification_key]:-0}"
      (( sample_count > 0 )) || continue
      printf '%s,%s,%s,%s,%s,%s,%s\n' \
        "$phase" "$lane" "${lane_primary_service[$lane]}" "$sample_classification" \
        "$sample_count" "$(( ${classification_usec[$classification_key]:-0} / 1000 ))" \
        "$(( ${classification_max_streak_usec[$classification_key]:-0} / 1000 ))" \
        >>"$ARTIFACTS/sample-classification-summary.csv"
    done
  done
done

printf '%s\n' \
  'node,primary_lane,main_daemon_thread_voluntary_context_switches_snapshot_delta,main_daemon_thread_nonvoluntary_context_switches_snapshot_delta,main_daemon_process_io_observed,main_daemon_read_bytes_delta,main_daemon_write_bytes_delta,nr_throttled_delta,throttled_usec_delta,cpu_some_usec_delta,cpu_full_usec_delta,io_some_usec_delta,io_full_usec_delta,memory_some_usec_delta,memory_full_usec_delta,io_rbytes_delta,io_wbytes_delta,rx_bytes_delta,tx_bytes_delta' \
  >"$ARTIFACTS/container-summary.csv"

for node in "${NODES[@]}"; do
  throttled_count_delta=$((last_nr_throttled[$node] - initial_nr_throttled[$node]))
  throttled_usec_delta=$((last_throttled_usec[$node] - initial_throttled_usec[$node]))
  cpu_some_delta=$((last_cpu_pressure_some[$node] - initial_cpu_pressure_some[$node]))
  cpu_full_delta=$((last_cpu_pressure_full[$node] - initial_cpu_pressure_full[$node]))
  io_some_delta=$((last_io_pressure_some[$node] - initial_io_pressure_some[$node]))
  io_full_delta=$((last_io_pressure_full[$node] - initial_io_pressure_full[$node]))
  memory_some_delta=$((last_memory_pressure_some[$node] - initial_memory_pressure_some[$node]))
  memory_full_delta=$((last_memory_pressure_full[$node] - initial_memory_pressure_full[$node]))
  io_rbytes_delta=$((last_io_rbytes[$node] - initial_io_rbytes[$node]))
  io_wbytes_delta=$((last_io_wbytes[$node] - initial_io_wbytes[$node]))
  rx_delta=$((last_rx_bytes[$node] - initial_rx_bytes[$node]))
  tx_delta=$((last_tx_bytes[$node] - initial_tx_bytes[$node]))
  process_voluntary_context_switches_delta=$((
    last_process_voluntary_context_switches[$node] \
      - initial_process_voluntary_context_switches[$node]
  ))
  process_nonvoluntary_context_switches_delta=$((
    last_process_nonvoluntary_context_switches[$node] \
      - initial_process_nonvoluntary_context_switches[$node]
  ))
  process_io_observed=no
  process_read_bytes_delta=na
  process_write_bytes_delta=na
  if (( process_io_complete[$node] == 1 \
      && initial_process_read_bytes[$node] >= 0 && last_process_read_bytes[$node] >= 0 \
      && initial_process_write_bytes[$node] >= 0 && last_process_write_bytes[$node] >= 0 )); then
    process_io_observed=yes
    process_read_bytes_delta=$((last_process_read_bytes[$node] - initial_process_read_bytes[$node]))
    process_write_bytes_delta=$((last_process_write_bytes[$node] - initial_process_write_bytes[$node]))
  fi
  lanes="${primary_services[$node]:-}"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$node" "$lanes" "$process_voluntary_context_switches_delta" \
    "$process_nonvoluntary_context_switches_delta" "$process_io_observed" \
    "$process_read_bytes_delta" "$process_write_bytes_delta" \
    "$throttled_count_delta" "$throttled_usec_delta" "$cpu_some_delta" "$cpu_full_delta" \
    "$io_some_delta" "$io_full_delta" "$memory_some_delta" "$memory_full_delta" \
    "$io_rbytes_delta" "$io_wbytes_delta" "$rx_delta" "$tx_delta" \
    >>"$ARTIFACTS/container-summary.csv"

  if (( cpu_some_delta < 0 || cpu_full_delta < 0 \
      || io_some_delta < 0 || io_full_delta < 0 \
      || memory_some_delta < 0 || memory_full_delta < 0 \
      || io_rbytes_delta < 0 || io_wbytes_delta < 0 \
      || rx_delta < 0 || tx_delta < 0 )); then
    log "$node cumulative cgroup/network counters regressed during the workload"
    gate_failed=1
  fi

  if (( throttled_count_delta != 0 || throttled_usec_delta != 0 )); then
    log "$node was CPU-throttled during the measurement phases"
    gate_failed=1
  fi
done

overlap_ms=$((overlap_window_usec / 1000))
aggregate_efficiency_milli=0
cgroup_aggregate_efficiency_milli=0
if (( overlap_window_usec > 0 )); then
  aggregate_efficiency_milli=$((overlap_process_usage_usec * 100000 / (overlap_window_usec * 3)))
  cgroup_aggregate_efficiency_milli=$((overlap_cgroup_usage_usec * 100000 / (overlap_window_usec * 3)))
fi
printf -v aggregate_efficiency '%d.%03d' \
  $((aggregate_efficiency_milli / 1000)) $((aggregate_efficiency_milli % 1000))
printf -v cgroup_aggregate_efficiency '%d.%03d' \
  $((cgroup_aggregate_efficiency_milli / 1000)) $((cgroup_aggregate_efficiency_milli % 1000))
aggregate_process_io_observed=yes
for lane in "${LANES[@]}"; do
  if (( process_io_complete[${lane_primary_service[$lane]}] != 1 )); then
    aggregate_process_io_observed=no
  fi
done
{
  printf 'concurrent_wall_ms\t%s\n' "$concurrent_wall_ms"
  printf 'concurrent_total_files\t%s\n' "$concurrent_total_files"
  printf 'concurrent_aggregate_files_per_second\t%s\n' \
    "$concurrent_aggregate_files_per_second"
  printf 'min_concurrent_aggregate_files_per_second\t%s\n' \
    "$MIN_CONCURRENT_AGGREGATE_FILES_PER_SECOND"
  printf 'min_concurrent_data_write_files_per_second_per_lane\t%s\n' \
    "$MIN_CONCURRENT_LANE_FILES_PER_SECOND"
  printf 'overlap_ms\t%s\n' "$overlap_ms"
  printf 'three_lane_main_daemon_efficiency_percent\t%s\n' "$aggregate_efficiency"
  printf 'three_lane_cgroup_efficiency_percent\t%s\n' "$cgroup_aggregate_efficiency"
  printf 'three_lane_main_daemon_thread_voluntary_context_switches_delta\t%s\n' \
    "$overlap_process_voluntary_context_switches"
  printf 'three_lane_main_daemon_thread_nonvoluntary_context_switches_delta\t%s\n' \
    "$overlap_process_nonvoluntary_context_switches"
  printf 'three_lane_main_daemon_process_io_observed\t%s\n' \
    "$aggregate_process_io_observed"
  printf 'three_lane_main_daemon_read_bytes_delta\t%s\n' "$overlap_process_read_bytes"
  printf 'three_lane_main_daemon_write_bytes_delta\t%s\n' "$overlap_process_write_bytes"
} >"$ARTIFACTS/aggregate-summary.tsv"
if (( overlap_ms < MIN_LANE_MS )); then
  log "three-lane overlap was only ${overlap_ms}ms; increase SINOMO_DFS_PERF_FILES"
  gate_failed=1
fi
if (( aggregate_efficiency_milli < min_cpu_milli )); then
  log "three-lane main-daemon CPU efficiency was ${aggregate_efficiency}%, below ${MIN_PRIMARY_CPU_PERCENT}%"
  gate_failed=1
fi

log "postflight: waiting for zero lag and clean DFS status"
convergence_deadline=$(( $(date +%s) + CONVERGENCE_SECONDS ))
postflight_clean=0
while (( $(date +%s) <= convergence_deadline )); do
  run_node_timeout 30 node1 sinomo dfs status --cluster --json \
    >"$ARTIFACTS/dfs-status.after.json" 2>"$ARTIFACTS/dfs-status.after.stderr.log" \
    || true
  if capture_and_validate_local_node_statuses after; then
    all_primaries_clean=1
    for lane in "${LANES[@]}"; do
      validate_primary_shard "$lane" after || all_primaries_clean=0
    done
    if (( all_primaries_clean == 1 )); then
      postflight_clean=1
      break
    fi
  fi
  sleep 1
done
if (( postflight_clean != 1 )); then
  log "postflight DFS status did not converge cleanly within ${CONVERGENCE_SECONDS}s"
  gate_failed=1
fi

run_node_timeout 60 node1 sinomo test run cluster.dfs.shard-layout \
  --require-cluster --json \
  --param "expected-namespace-prepare-codec=$EXPECTED_NAMESPACE_PREPARE_CODEC" \
  >"$ARTIFACTS/shard-layout.after.json" \
  2>"$ARTIFACTS/shard-layout.after.stderr.log" || gate_failed=1
jq -S '[.diagnostics.shards[] | {
  lane, base_prefix, shard_id, primary_node_id, replica_node_ids, epoch,
  namespace_prepare_codec
}]' "$ARTIFACTS/shard-layout.before.json" >"$ARTIFACTS/shard-layout.before.canonical.json"
jq -S '[.diagnostics.shards[] | {
  lane, base_prefix, shard_id, primary_node_id, replica_node_ids, epoch,
  namespace_prepare_codec
}]' "$ARTIFACTS/shard-layout.after.json" >"$ARTIFACTS/shard-layout.after.canonical.json" \
  || gate_failed=1
if ! cmp -s "$ARTIFACTS/shard-layout.before.canonical.json" \
    "$ARTIFACTS/shard-layout.after.canonical.json"; then
  log "metadata-shard routing changed during the measurement"
  gate_failed=1
fi

run_node_timeout 30 node1 sinomo cluster health >"$ARTIFACTS/cluster-health.after.txt" || gate_failed=1
run_node_timeout 30 node1 sinomo cluster doctor >"$ARTIFACTS/cluster-doctor.after.txt" || gate_failed=1
compose ps >"$ARTIFACTS/compose-ps.after.txt"
if [[ "$(grep -c $'daemon-main.*online' "$ARTIFACTS/cluster-health.after.txt" || true)" -ne 6 ]]; then
  log "postflight cluster health does not report six online main daemons"
  gate_failed=1
fi
if grep -Eiq 'warning|error|unavailable|offline|unhealthy' "$ARTIFACTS/cluster-doctor.after.txt"; then
  log "postflight cluster doctor reported a warning or error"
  gate_failed=1
fi
for node in "${NODES[@]}"; do
  current_cid="$(compose ps -q "$node")"
  if [[ "$current_cid" != "${container_id[$node]}" ]]; then
    log "$node container changed during the measurement"
    gate_failed=1
    continue
  fi
  if [[ "$(docker inspect -f '{{.State.Pid}}' "$current_cid")" != "${container_pid[$node]}" ]]; then
    log "$node process namespace restarted during the measurement"
    gate_failed=1
  fi
  if ! current_daemon_status="$(run_node_timeout 15 "$node" sinomo daemon status)"; then
    log "$node main daemon status failed after the measurement"
    gate_failed=1
    continue
  fi
  current_daemon_namespace_pid="$(
    sed -n 's/^[[:space:]]*pid: //p' <<<"$current_daemon_status" | sed -n '1p'
  )"
  if [[ "$current_daemon_namespace_pid" != "${main_daemon_namespace_pid[$node]}" ]]; then
    log "$node main daemon namespace PID changed during the measurement"
    gate_failed=1
    continue
  fi
  if ! current_daemon_host_pid="$(
    resolve_main_daemon_host_pid "${container_cgroup[$node]}" "$current_daemon_namespace_pid"
  )"; then
    log "$node main daemon PID no longer maps uniquely into its container cgroup"
    gate_failed=1
    continue
  fi
  if [[ "$current_daemon_host_pid" != "${main_daemon_host_pid[$node]}" ]]; then
    log "$node main daemon host PID changed during the measurement"
    gate_failed=1
  fi
done

printf '%s\n' \
  'phase,workload_lanes,node,samples,memory_current_first_bytes,memory_current_last_bytes,memory_current_min_bytes,memory_current_max_bytes,memory_peak_first_bytes,memory_peak_last_bytes' \
  >"$ARTIFACTS/memory-phase-summary.csv"
awk -F, '
  NR == 1 { next }
  {
    key = $3 SUBSEP $4 SUBSEP $5
    if (!(key in samples)) {
      phase[key] = $3
      lanes[key] = $4
      node[key] = $5
      current_first[key] = $44
      current_min[key] = $44
      current_max[key] = $44
      peak_first[key] = $45
    }
    samples[key] += 1
    current_last[key] = $44
    peak_last[key] = $45
    if ($44 < current_min[key]) current_min[key] = $44
    if ($44 > current_max[key]) current_max[key] = $44
  }
  END {
    for (key in samples) {
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        phase[key], lanes[key], node[key], samples[key],
        current_first[key], current_last[key], current_min[key], current_max[key],
        peak_first[key], peak_last[key]
    }
  }
' "$ARTIFACTS/timeline.csv" \
  | sort -t, -k1,1 -k2,2 -k3,3 >>"$ARTIFACTS/memory-phase-summary.csv"

printf '%s\n' \
  'node,memory_current_initial_bytes,memory_current_sampled_peak_bytes,memory_current_final_bytes,memory_peak_initial_bytes,memory_peak_final_bytes,memory_peak_growth_bytes,memory_events_low_delta,memory_events_high_delta,memory_events_max_delta,memory_events_oom_delta,memory_events_oom_kill_delta,memory_events_oom_group_kill_delta' \
  >"$ARTIFACTS/memory-summary.csv"
printf 'node\tkey\tvalue\n' >"$ARTIFACTS/memory-events.after.tsv"
for node in "${NODES[@]}"; do
  read_memory_observation "${container_cgroup[$node]}"
  memory_peak_growth=$((sample_memory_peak - initial_memory_peak[$node]))
  memory_low_delta=$((sample_memory_low - initial_memory_low[$node]))
  memory_high_delta=$((sample_memory_high - initial_memory_high[$node]))
  memory_max_delta=$((sample_memory_max - initial_memory_max[$node]))
  memory_oom_delta=$((sample_memory_oom - initial_memory_oom[$node]))
  memory_oom_kill_delta=$((sample_memory_oom_kill - initial_memory_oom_kill[$node]))
  memory_oom_group_kill_delta=$((
    sample_memory_oom_group_kill - initial_memory_oom_group_kill[$node]
  ))
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$node" "${initial_memory_current[$node]}" \
    "${max_sampled_memory_current[$node]}" "$sample_memory_current" \
    "${initial_memory_peak[$node]}" "$sample_memory_peak" "$memory_peak_growth" \
    "$memory_low_delta" "$memory_high_delta" "$memory_max_delta" \
    "$memory_oom_delta" "$memory_oom_kill_delta" "$memory_oom_group_kill_delta" \
    >>"$ARTIFACTS/memory-summary.csv"
  while read -r memory_event_key memory_event_value _; do
    printf '%s\t%s\t%s\n' "$node" "$memory_event_key" "$memory_event_value"
  done <"${container_cgroup[$node]}/memory.events" >>"$ARTIFACTS/memory-events.after.tsv"
  if (( memory_peak_growth < 0 || memory_low_delta < 0 || memory_high_delta < 0 \
      || memory_max_delta < 0 || memory_oom_delta < 0 || memory_oom_kill_delta < 0 \
      || memory_oom_group_kill_delta < 0 )); then
    log "$node cumulative cgroup memory counter regressed during the measurement"
    gate_failed=1
  fi
  if (( memory_oom_delta != 0 || memory_oom_kill_delta != 0 \
      || memory_oom_group_kill_delta != 0 )); then
    log "$node experienced cgroup OOM activity during the measurement"
    gate_failed=1
  fi
done

memory_final_timestamp_ns="$(date +%s%N)"
memory_final_elapsed_ms=$(((memory_final_timestamp_ns - measurement_started_ns) / 1000000))
record_host_memory_observation \
  "$memory_final_timestamp_ns" "$memory_final_elapsed_ms" postflight ""
{
  printf 'memory_total_bytes\t%s\n' "$host_initial_memory_total_bytes"
  printf 'memory_available_initial_bytes\t%s\n' "$host_initial_memory_available_bytes"
  printf 'memory_available_minimum_bytes\t%s\n' "$host_min_memory_available_bytes"
  printf 'memory_available_final_bytes\t%s\n' "$host_last_memory_available_bytes"
  printf 'swap_total_bytes\t%s\n' "$host_initial_swap_total_bytes"
  printf 'swap_free_initial_bytes\t%s\n' "$host_initial_swap_free_bytes"
  printf 'swap_free_final_bytes\t%s\n' "$host_last_swap_free_bytes"
} >"$ARTIFACTS/host-memory-summary.tsv"

log "results: $ARTIFACTS"
log "mode=$PERF_MODE concurrent cluster=${concurrent_aggregate_files_per_second} publications/s over ${concurrent_wall_ms}ms; three-lane overlap=${overlap_ms}ms main-daemon efficiency=${aggregate_efficiency}% cgroup efficiency=${cgroup_aggregate_efficiency}%"
if (( SUSTAINED_ROUNDS > 0 )); then
  log "sustained churn=${sustained_aggregate_files_per_second} publications/s over ${sustained_wall_ms}ms and $sustained_completed_rounds round(s)"
fi
if (( gate_failed != 0 )); then
  log "DFS shard performance gate FAILED"
  exit 1
fi

log "DFS shard performance gate passed: isolated lanes met their bulk write+commit publication floor, the concurrent cluster met its end-to-end aggregate publication and utilization floors, and every enabled opt-in mode met its additional floor"
