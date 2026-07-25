#!/usr/bin/env bash
# Reproducible cluster-KV benchmark suite on the segmented six-node debug cluster.
#
# This wrapper selects the KV-only path in docker/test-cluster.sh, records the
# environment under the gitignored benchmark-results directory, and preserves
# the raw JSON report for every node/workload pair.
#
# Usage:
#   docker/benchmark-cluster-kv.sh
#   SINOMO_TEST_BUILD=0 docker/benchmark-cluster-kv.sh
#   SINOMO_TEST_REUSE=1 SINOMO_TEST_KEEP=1 docker/benchmark-cluster-kv.sh
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACTS="${SINOMO_KV_BENCHMARK_ARTIFACTS:-$SINOMO_ROOT/benchmark-results/cluster-kv/$RUN_ID}"
HARNESS="$SINOMO_DOCKER_DIR/test-cluster.sh"

mkdir -p "$ARTIFACTS"

export SINOMO_TEST_CLUSTER_KV_ONLY=1
export SINOMO_TEST_PROJECT="${SINOMO_TEST_PROJECT:-sinomo-kv-benchmark}"
export SINOMO_TEST_ARTIFACTS="$ARTIFACTS"

# These defaults produce stable distributions without turning the benchmark
# into a correctness hardgate. Functional failures still fail closed.
export SINOMO_TEST_CLUSTER_KV_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_ITERATIONS:-64}"
export SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS:-8}"
export SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS:-32}"
export SINOMO_TEST_CLUSTER_KV_BATCH_OPS="${SINOMO_TEST_CLUSTER_KV_BATCH_OPS:-32}"
export SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS:-16}"
export SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS:-64}"
export SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES="${SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES:-64}"
export SINOMO_TEST_CLUSTER_KV_MAX_FAILURES="${SINOMO_TEST_CLUSTER_KV_MAX_FAILURES:-0}"
export SINOMO_TEST_CLUSTER_KV_MAX_P50_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P50_MS:-0}"
export SINOMO_TEST_CLUSTER_KV_MAX_P95_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P95_MS:-0}"
export SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND="${SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND:-0}"
export SINOMO_TEST_CLUSTER_KV_MIN_READS_PER_SECOND="${SINOMO_TEST_CLUSTER_KV_MIN_READS_PER_SECOND:-0}"

{
  printf 'protocol=sinomo.cluster-kv-benchmark.v1\n'
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'git_revision=%s\n' "$(git -C "$SINOMO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'git_dirty_files=%s\n' "$(
    git -C "$SINOMO_ROOT" status --porcelain 2>/dev/null | wc -l | tr -d ' '
  )"
  printf 'compose_project=%s\n' "$SINOMO_TEST_PROJECT"
  printf 'host_uname=%s\n' "$(uname -a)"
  printf 'docker_version=%s\n' "$(
    docker version --format '{{.Server.Version}}' 2>/dev/null || printf unavailable
  )"
  printf 'suite_nodes=%s\n' "${SINOMO_TEST_CLUSTER_KV_SUITE_NODES:-node1 node5}"
  printf 'iterations=%s\n' "$SINOMO_TEST_CLUSTER_KV_ITERATIONS"
  printf 'warmup_iterations=%s\n' "$SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS"
  printf 'batch_iterations=%s\n' "$SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS"
  printf 'batch_ops=%s\n' "$SINOMO_TEST_CLUSTER_KV_BATCH_OPS"
  printf 'large_iterations=%s\n' "$SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS"
  printf 'range_iterations=%s\n' "$SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS"
  printf 'range_entries=%s\n' "$SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES"
} >"$ARTIFACTS/environment.env"

printf '[benchmark-cluster-kv] artifacts: %s\n' "$ARTIFACTS"
set +e
"$HARNESS" 2>&1 | tee "$ARTIFACTS/harness.log"
result=${PIPESTATUS[0]}
set -e

shopt -s nullglob
reports=("$ARTIFACTS"/probe-*-cluster.kv.*.json)
if [ "${#reports[@]}" -gt 0 ]; then
  if ! jq -s '
    map({
      case_id: .case.id,
      status,
      profile: .diagnostics.profile,
      node_id: .diagnostics.node_id,
      consistency: (.diagnostics.consistency // null),
      iterations: .diagnostics.iterations,
      warmup_iterations: .diagnostics.warmup_iterations,
      operations_per_transaction: (.diagnostics.ops_per_transaction // null),
      entries_per_read: (.diagnostics.entries // null),
      value_bytes: .diagnostics.value_bytes,
      success_count: .diagnostics.success_count,
      failure_count: .diagnostics.failure_count,
      latency_p50_us: .diagnostics.latency_p50_us,
      latency_p95_us: .diagnostics.latency_p95_us,
      latency_max_us: .diagnostics.latency_max_us,
      requests_per_second: (
        .diagnostics.commits_per_second // .diagnostics.reads_per_second
      ),
      logical_items_per_second: (
        .diagnostics.operations_per_second // .diagnostics.entries_per_second
      ),
      value_bytes_per_second: .diagnostics.value_bytes_per_second,
      revision_min: .diagnostics.revision_min,
      revision_max: .diagnostics.revision_max,
      storage_path: .diagnostics.storage_path,
      transport_path: .diagnostics.transport_path
    })
  ' "${reports[@]}" >"$ARTIFACTS/summary.json"; then
    printf '[benchmark-cluster-kv] failed to aggregate benchmark JSON\n' >&2
    result=1
  elif ! jq -r '
    ([
      "profile", "node_id", "status", "p50_us", "p95_us", "max_us",
      "requests_per_second", "logical_items_per_second",
      "value_bytes_per_second", "revision_min", "revision_max"
    ] | @tsv),
    (.[] | [
      .profile, .node_id, .status, .latency_p50_us, .latency_p95_us,
      .latency_max_us, .requests_per_second, .logical_items_per_second,
      .value_bytes_per_second, .revision_min, .revision_max
    ] | @tsv)
  ' "$ARTIFACTS/summary.json" >"$ARTIFACTS/summary.tsv"; then
    printf '[benchmark-cluster-kv] failed to render benchmark summary TSV\n' >&2
    result=1
  fi
elif [ "$result" -eq 0 ]; then
  printf '[benchmark-cluster-kv] harness passed without cluster-KV reports\n' >&2
  result=1
fi

{
  printf 'protocol=sinomo.cluster-kv-benchmark-result.v1\n'
  printf 'finished_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'exit_code=%s\n' "$result"
  if [ "$result" -eq 0 ]; then
    printf 'status=passed\n'
  else
    printf 'status=failed\n'
  fi
  binary="${SINOMO_TEST_BINARY:-$SINOMO_ROOT/target/debug/sinomo}"
  if [ -f "$binary" ]; then
    printf 'binary=%s\n' "$binary"
    printf 'binary_sha256=%s\n' "$(sha256sum "$binary" | awk '{print $1}')"
  fi
} >"$ARTIFACTS/result.env"

if [ "$result" -eq 0 ]; then
  printf '[benchmark-cluster-kv] PASS: reports in %s\n' "$ARTIFACTS"
else
  printf '[benchmark-cluster-kv] FAIL: evidence in %s\n' "$ARTIFACTS" >&2
fi
exit "$result"
