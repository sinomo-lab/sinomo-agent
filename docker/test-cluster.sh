#!/usr/bin/env bash
# Automated DFS correctness suite on the 6-node segmented dev cluster.
#
# Brings up a DEDICATED compose project (so it never collides with a manually
# started dev cluster), waits for clean 6-node formation, then drives the
# in-binary cluster runtime test cases (sinomo test run --require-cluster)
# from different nodes to verify network health and cluster-wide POSIX behavior:
#
#   1. DFS protocol codec — in-memory DFS data-plane frame encode/decode baseline
#   2. network latency — transport-bound object-RPC latency/RPS with no failed requests
#   3. network payload — object-RPC payload throughput with no failed requests
#   4. cluster KV     — daemon-mediated quorum commit latency and throughput
#                       from a direct and a deep-segment node
#   5. invariants     — per-node DFS operational invariants (baseline)
#   6. read-your-write — write/read/rename/delete roundtrip on node1 and node5
#   7. cross-node     — write on one segment, poll-read on the other (both
#                       directions), then a routed delete observed cluster-wide
#   8. rename         — /users -> /services rename issued by a NON-writer node,
#                       observed (and the old path absent) on far nodes
#   9. small baseline — node-local raw tiny-file write baseline for DFS comparison
#   10. bulk perf     — small-file bulk write/read throughput with process I/O
#                       amplification checks and read-side write_bytes == 0
#   11. disk baseline — node-local raw file write/read baseline for DFS comparison
#   12. large-file perf — large object write throughput, cross-node convergence
#                       latency, final-read throughput, process I/O amplification
#                       checks, and read-side write_bytes == 0
#   13. disk damage   — delete one durable loose blob and read through the
#                       remaining replica without read-side disk writes
#   14. network heal  — temporarily disconnect a leaf node, write while it is
#                       isolated, reconnect it, and verify convergence
#   15. restart       — restart a member, re-gate formation, re-read its data
#   16. normal/history — all-node mutation visibility, retained versions,
#                       restore/recover, crash receipts, and authority handoff
#   17. history perf  — CLI distributions for ordinary/retained/checkpoint paths
#   18. final sweep   — invariants again on every node, then routed status
#                       with no pending remote DFS work
#
# Usage:
#   docker/test-cluster.sh                  # build current binary, up, test, down -v
#   SINOMO_TEST_REUSE=1 docker/test-cluster.sh   # reuse the running test project
#   SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
#   SINOMO_TEST_TIMING_HELPERS_SELF_TEST=1 docker/test-cluster.sh  # no-Docker helper test
#
# Env knobs:
#   SINOMO_TEST_PROJECT         compose project name   (default sinomo-dfs-test)
#   Cluster formation has a non-configurable 60-second bounded acceptance gate.
#   SINOMO_TEST_EXPECT_TIMEOUT  cross-node poll seconds (default 90)
#   SINOMO_TEST_PERF_FILES      files per bulk-performance leg (default 64)
#   SINOMO_TEST_PERF_WARMUP_FILES files in the no-threshold warmup leg (default 8)
#   SINOMO_TEST_PERF_BYTES      bytes per bulk-performance file (default 4096)
#   SINOMO_TEST_PERF_TIMEOUT    bulk write/read timeout seconds (default 120)
#   SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC  min writer throughput (default 0.75)
#   SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC   min cross-node read/convergence throughput (default 1)
#   SINOMO_TEST_PERF_MAX_PROC_WCHAR_AMPLIFICATION       max writer-process wchar/logical bytes (default 0, disabled)
#   SINOMO_TEST_PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION max writer-process write_bytes/logical bytes (default 0, disabled)
#   SINOMO_TEST_PERF_MAX_PROC_SYSCW_PER_FILE max writer-process write syscalls/logical file (default 4)
#   SINOMO_TEST_PERF_MAX_PROC_SYSCW_FIXED fixed writer-process write syscall budget per measured leg (default 16)
#   SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during bulk-expect (default 0)
#   SINOMO_TEST_SMALL_BASELINE_FILES files in raw tiny-file baseline leg (default same as SINOMO_TEST_PERF_FILES)
#   SINOMO_TEST_SMALL_BASELINE_BYTES bytes per raw tiny-file baseline file (default same as SINOMO_TEST_PERF_BYTES)
#   SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES sync each raw tiny-file write before timing completes (default 0)
#   SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC min raw tiny-file throughput (default 0, disabled)
#   SINOMO_TEST_LARGE_BYTES    bytes in large-file performance leg (default 268435456)
#   SINOMO_TEST_LARGE_TIMEOUT  large-file write/read timeout seconds (default 180)
#   SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC min large writer throughput (default 134217728)
#   SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC  min final large read throughput after convergence (default 4194304)
#   SINOMO_TEST_LARGE_MEASURE_RAW_WRITE_BASELINE measure raw write throughput alongside DFS writes (default 1)
#   SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO min large DFS write / raw write baseline ratio (default 0, informational)
#   SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO  min large DFS read / raw read baseline ratio (default 0, disabled)
#   SINOMO_TEST_LARGE_RAW_SYNC_WRITE      sync raw baseline writes before timing completes (default 1)
#   SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION       max large writer-process wchar/logical bytes (default 0, disabled)
#   SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION max large writer-process write_bytes/logical bytes (default 4)
#   SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES max reader-process write_bytes during large-expect (default 0)
#   SINOMO_TEST_DISK_BASELINE_BYTES bytes in raw disk baseline leg (default same as SINOMO_TEST_LARGE_BYTES)
#   SINOMO_TEST_DISK_BASELINE_SYNC_WRITE sync raw disk baseline writes before timing completes (default 1)
#   SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC min raw write throughput (default 0, disabled)
#   SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC  min raw read throughput (default 0, disabled)
#   SINOMO_TEST_DAMAGE_BYTES    bytes in loose-blob disk damage scenario (default 49152)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES bytes per DFS protocol codec message (default 4194304)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS codec encode/decode iterations per sample (default 8)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC min codec payload encode throughput (default 67108864)
#   SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC min codec payload decode throughput (default 67108864)
#   SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET cluster network ping RPCs per remote node (default 20)
#   SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET warmup RPCs per remote node before measured pings (default same as requests)
#   SINOMO_TEST_NETWORK_CONCURRENCY         concurrent network ping workers (default 16)
#   SINOMO_TEST_NETWORK_TIMEOUT_MS          per network ping RPC timeout (default 2000)
#   SINOMO_TEST_NETWORK_MAX_FAILURES        allowed failed network ping RPCs (default 0)
#   SINOMO_TEST_NETWORK_MAX_P50_US          max 16-worker aggregate network ping p50 latency (default 60000)
#   SINOMO_TEST_NETWORK_MAX_P95_US          max 16-worker aggregate network ping p95 latency (default 200000)
#   SINOMO_TEST_NETWORK_MIN_RPS             min network ping throughput (default 250)
#   SINOMO_TEST_NETWORK_REALTIME_REQUESTS   measured RPCs in each no-load realtime gate (default 100)
#   SINOMO_TEST_NETWORK_REALTIME_WARMUP_REQUESTS warmup RPCs in each no-load realtime gate (default 20)
#   SINOMO_TEST_NETWORK_QUIET_WAIT           seconds to find the hard 30-second idle window (default and maximum 120)
#   SINOMO_TEST_NETWORK_QUIET_MAX_CPU_PERCENT max CPU percent allowed per cluster container (default and maximum 10)
#   SINOMO_TEST_NETWORK_PAYLOAD_BYTES       request bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES response bytes per latency-baseline ping payload (default 0)
#   SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC min latency-baseline payload throughput (default 0, disabled)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET payload-throughput RPCs per remote node (default 4)
#   SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET warmup payload RPCs per remote node (default same as payload requests)
#   SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY payload-throughput workers (default 8)
#   SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS  payload-throughput RPC timeout (default 5000)
#   SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES request bytes per payload-throughput RPC (default 262144)
#   SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES response bytes per payload-throughput RPC (default 262144)
#   SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC min payload-throughput rate (default 16777216)
#   SINOMO_TEST_NETWORK_ATTACHMENT_REQUESTS_PER_TARGET semantic-attachment RPCs per remote node (default 4)
#   SINOMO_TEST_NETWORK_ATTACHMENT_WARMUP_REQUESTS_PER_TARGET warmup attachment RPCs per remote node (default 1)
#   SINOMO_TEST_NETWORK_ATTACHMENT_CONCURRENCY semantic-attachment workers (default 4)
#   SINOMO_TEST_NETWORK_ATTACHMENT_TIMEOUT_MS semantic-attachment RPC timeout (default 30000)
#   SINOMO_TEST_NETWORK_ATTACHMENT_REQUEST_BYTES request attachment bytes per RPC (default 8388608)
#   SINOMO_TEST_NETWORK_ATTACHMENT_RESPONSE_BYTES response attachment bytes per RPC (default 8388608)
#   SINOMO_TEST_NETWORK_ATTACHMENT_MIN_BYTES_PER_SEC min aggregate duplex logical attachment throughput (default 0, disabled)
#   SINOMO_TEST_NETWORK_SATURATION=1 enable the optional one-origin saturation profile (default 0)
#   SINOMO_TEST_NETWORK_SATURATION_ORIGIN origin node for the saturation profile (default node1)
#   SINOMO_TEST_NETWORK_SATURATION_REQUESTS_PER_TARGET measured RPCs per remote target (default 128)
#   SINOMO_TEST_NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET warmup RPCs per remote target (default 1)
#   SINOMO_TEST_NETWORK_SATURATION_CONCURRENCY saturation workers (default 8)
#   SINOMO_TEST_NETWORK_SATURATION_TIMEOUT_MS per-RPC timeout (default 120000)
#   SINOMO_TEST_NETWORK_SATURATION_REQUEST_BYTES request attachment bytes per RPC (default 67108864)
#   SINOMO_TEST_NETWORK_SATURATION_RESPONSE_BYTES response attachment bytes per RPC (default 67108864)
#   SINOMO_TEST_CLUSTER_KV_ITERATIONS        measured quorum commits per selected node (default 12)
#   SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS unmeasured warmup operations per workload (default 4)
#   SINOMO_TEST_CLUSTER_KV_VALUE_BYTES       small-workload value bytes (default 256)
#   SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS  measured batched quorum commits (default 32)
#   SINOMO_TEST_CLUSTER_KV_BATCH_OPS         put operations per batched commit (default 32)
#   SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS  measured large-value quorum commits (default 16)
#   SINOMO_TEST_CLUSTER_KV_LARGE_VALUE_BYTES large-workload value bytes (default 16384)
#   SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS  measured local/authoritative prefix reads (default 64)
#   SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES     entries returned per prefix read (default 64)
#   SINOMO_TEST_CLUSTER_KV_SUITE_NODES       benchmark origins (default "node1 node5")
#   SINOMO_TEST_CLUSTER_KV_TIMEOUT_MS        per-commit timeout (default 5000)
#   SINOMO_TEST_CLUSTER_KV_MAX_FAILURES      allowed failed quorum commits per node (default 0)
#   SINOMO_TEST_CLUSTER_KV_MAX_P50_MS        max quorum commit p50 latency (default 1000)
#   SINOMO_TEST_CLUSTER_KV_MAX_P95_MS        max quorum commit p95 latency (default 2000)
#   SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND min quorum commit throughput (default 1)
#   SINOMO_TEST_CLUSTER_KV_MIN_READS_PER_SECOND min prefix reads/s (default 0, informational)
#   SINOMO_TEST_NETWORK_READY_WAIT          network/auth convergence gate seconds (default 120)
#   SINOMO_TEST_DFS_CONVERGENCE_WAIT        fully distributed DFS shard-layout gate seconds (default 360)
#   SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET requests per target during readiness warmup (default 20)
#   SINOMO_TEST_NETWORK_READY_CONCURRENCY   readiness warmup concurrency (default 16)
#   SINOMO_TEST_READINESS_COMMAND_TIMEOUT   host/container cap for each readiness command (default 10)
#   SINOMO_TEST_PROBE_COMMAND_TIMEOUT       host/container cap for each test probe (default 300)
#   SINOMO_TEST_ARTIFACT_COMMAND_TIMEOUT    cap for each failure artifact command (default 10)
#   SINOMO_TEST_HOST_TIMEOUT_GRACE          host cleanup grace beyond the container timeout (default 5)
#   SINOMO_TEST_NETWORK_PARTITION_SECONDS   seconds to keep node6 disconnected in network-heal leg (default 5)
#   SINOMO_TEST_NETWORK_ONLY=1              stop after network/KV baselines and DFS readiness/mutation/convergence gates
#   SINOMO_TEST_DFS_READY_ONLY=1            stop after initial DFS readiness/mutation/convergence gates
#   SINOMO_TEST_DFS_READY_REQUIRE_QUIET=1   also enforce the standard quiet gate before ready-only returns
#   SINOMO_TEST_QUIET_ONLY=1               readiness and standard quiet gate, without mutation workloads
#   SINOMO_TEST_DFS_ONLY=1                 normal/history/restart/handoff correctness and DFS benchmarks
#   SINOMO_TEST_DFS_BENCH_ITERATIONS        measured CLI operations per profile/origin (default 8)
#   SINOMO_TEST_DFS_BENCH_WARMUP            excluded iterations per profile/origin (default 2)
#   SINOMO_TEST_DFS_BENCH_BYTES             payload bytes for CLI history benchmark (default 4096)
#   SINOMO_TEST_CLUSTER_KV_ONLY=1           run only the stabilized cluster-KV benchmark suite
#   SINOMO_TEST_PJDFSTEST_ONLY=1            run only the external pjdfstest mounted-filesystem lane
#   SINOMO_TEST_FORMATION_ONLY=1            stop after the 60-second formation gate and optional captures
#   SINOMO_PJDFSTEST_PROFILE                pjdfstest selection: smoke or full (default full)
#   SINOMO_PJDFSTEST_STRICT                 fail on discovered SIFS incompatibilities (default 0/report-only)
#   SINOMO_PJDFSTEST_TIMEOUT_SECONDS        timeout for each native/SIFS prove run (default 3600)
#   SINOMO_TEST_BUILD=0         reuse an existing host binary instead of rebuilding
#   SINOMO_TEST_BINARY          host binary to validate/mount (default target/debug/sinomo)
#   SINOMO_TEST_ARTIFACTS       artifact dir (default target/cluster-test-artifacts/<run>)
#   SINOMO_TEST_ALLOW_OTHER_COMPOSE=1       allow running while unrelated compose projects are up
#   SINOMO_TEST_COMPOSE_OVERRIDE optional second compose file (for measurement-only overrides)
#   SINOMO_TEST_FORMATION_OBSERVE=1 start bounded trace, RPC-timeline, and CPU
#                                  captures before Docker formation work; diagnostic only
#   SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS capture watchdog seconds (default 12)
#   SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ CPU samples/second (default 49)
#   SINOMO_TEST_FORMATION_OBSERVE_CPU_NODES comma-separated nodes profiled in-process
#                                           (default node1; use all explicitly)
#   SINOMO_TEST_FORMATION_OBSERVE_MAX_EVENTS bounded RPC-timeline records/node (default 32768)
#
# Exits non-zero if any scenario fails; failure evidence (probe JSON, compose ps,
# cluster health/doctor, per-node logs) lands in the artifact directory.
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
NETWORK_CHECKS="$SINOMO_DOCKER_DIR/lib/network_checks.py"
if [ "${SINOMO_TEST_NETWORK_LIST:-0}" = 1 ]; then
  python3 "$NETWORK_CHECKS" list
  exit $?
fi
NETWORK_CASES="${SINOMO_TEST_NETWORK_CASES:-}"
if [ "${SINOMO_TEST_NETWORK_PLAN:-0}" = 1 ] && [ -z "$NETWORK_CASES" ]; then
  echo "NETWORK_PLAN requires an explicit NETWORK_CASES selection" >&2
  exit 1
fi
if [ -n "$NETWORK_CASES" ]; then
  NETWORK_CASES="$(python3 "$NETWORK_CHECKS" select --cases "$NETWORK_CASES")" || exit 1
  export SINOMO_TEST_BUILD="${SINOMO_TEST_BUILD:-0}"
fi
COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"
COMPOSE_OVERRIDE_FILE="${SINOMO_TEST_COMPOSE_OVERRIDE:-}"

PROJECT="${SINOMO_TEST_PROJECT:-sinomo-dfs-test}"
# BOUNDED ACCEPTANCE GATE: all six clean main daemons must form within 60 wall-clock
# seconds. This Docker-only correctness guard is intentionally non-configurable.
# Exceeding it still indicates a formation bug; it is not a production join
# deadline and must never be copied into runtime timeout policy. The mandatory
# live doctor runs after formation is observed and does not alter that result.
FORMATION_WAIT=60
# Keep formation observations dense enough to enforce the hard gate precisely.
# This cadence is intentionally independent of the configurable polling used by
# longer readiness and convergence waits.
FORMATION_POLL_MILLIS=100
FORMATION_OBSERVE="${SINOMO_TEST_FORMATION_OBSERVE:-1}"
FORMATION_ONLY="${SINOMO_TEST_FORMATION_ONLY:-0}"
DFS_READY_ONLY="${SINOMO_TEST_DFS_READY_ONLY:-0}"
DFS_READY_REQUIRE_QUIET="${SINOMO_TEST_DFS_READY_REQUIRE_QUIET:-0}"
QUIET_ONLY="${SINOMO_TEST_QUIET_ONLY:-0}"
DFS_ONLY="${SINOMO_TEST_DFS_ONLY:-0}"
CLUSTER_KV_ONLY="${SINOMO_TEST_CLUSTER_KV_ONLY:-0}"
PJDFSTEST_ONLY="${SINOMO_TEST_PJDFSTEST_ONLY:-0}"
PJDFSTEST_PROFILE="${SINOMO_PJDFSTEST_PROFILE:-full}"
PJDFSTEST_STRICT="${SINOMO_PJDFSTEST_STRICT:-0}"
PJDFSTEST_TIMEOUT_SECONDS="${SINOMO_PJDFSTEST_TIMEOUT_SECONDS:-3600}"
PJDFSTEST_PATCHSET="sinomo-linux-portability-v1"
PJDFSTEST_MOUNT_POINT="/mnt/sinomo-pjdfstest"
FORMATION_OBSERVE_DURATION_SECONDS="${SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS:-12}"
FORMATION_OBSERVE_FREQUENCY_HZ="${SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ:-49}"
FORMATION_OBSERVE_CPU_NODES="${SINOMO_TEST_FORMATION_OBSERVE_CPU_NODES:-all}"
FORMATION_OBSERVE_MAX_EVENTS="${SINOMO_TEST_FORMATION_OBSERVE_MAX_EVENTS:-32768}"
FORMATION_OBSERVE_FILTER="${SINOMO_TEST_FORMATION_OBSERVE_FILTER:-debug,sinomo.perf=trace,sinomo.cluster.formation=trace,sinomo.cluster.health=trace,sinomo.daemon.cluster_network_maintenance=trace,sinomo.daemon.cluster_member_publish=trace,sinomo.daemon.node_status_publish=trace,sinomo.cluster.raft_fanout=trace}"
export SINOMO_TEST_FORMATION_OBSERVE="$FORMATION_OBSERVE"
export SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS="$FORMATION_OBSERVE_DURATION_SECONDS"
export SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ="$FORMATION_OBSERVE_FREQUENCY_HZ"
export SINOMO_TEST_FORMATION_OBSERVE_CPU_NODES="$FORMATION_OBSERVE_CPU_NODES"
export SINOMO_TEST_FORMATION_OBSERVE_MAX_EVENTS="$FORMATION_OBSERVE_MAX_EVENTS"
export SINOMO_TEST_FORMATION_OBSERVE_FILTER="$FORMATION_OBSERVE_FILTER"
DFS_READY_WAIT="${SINOMO_TEST_DFS_READY_WAIT:-240}"
# Three proof-fenced default-shard handoffs serialize inside the daemon's
# 300-second membership phase window. Preserve one additional minute for the
# all-node invariant sweep and the second unchanged-epoch observation.
DFS_CONVERGENCE_WAIT="${SINOMO_TEST_DFS_CONVERGENCE_WAIT:-360}"
EXPECT_TIMEOUT="${SINOMO_TEST_EXPECT_TIMEOUT:-90}"
ROUTED_STATUS_MAX_ATTEMPTS="${SINOMO_TEST_ROUTED_STATUS_MAX_ATTEMPTS:-12}"
ROUTED_STATUS_MAX_ELAPSED_MS="${SINOMO_TEST_ROUTED_STATUS_MAX_ELAPSED_MS:-$((EXPECT_TIMEOUT * 1000))}"
POLL_INTERVAL="${SINOMO_TEST_POLL_INTERVAL:-1}"
READINESS_COMMAND_TIMEOUT="${SINOMO_TEST_READINESS_COMMAND_TIMEOUT:-10}"
PROBE_COMMAND_TIMEOUT="${SINOMO_TEST_PROBE_COMMAND_TIMEOUT:-300}"
ARTIFACT_COMMAND_TIMEOUT="${SINOMO_TEST_ARTIFACT_COMMAND_TIMEOUT:-10}"
HOST_TIMEOUT_GRACE="${SINOMO_TEST_HOST_TIMEOUT_GRACE:-5}"
EXPECTED_MAINS=6
PERF_FILES="${SINOMO_TEST_PERF_FILES:-64}"
PERF_WARMUP_FILES="${SINOMO_TEST_PERF_WARMUP_FILES:-8}"
PERF_BYTES="${SINOMO_TEST_PERF_BYTES:-4096}"
PERF_TIMEOUT="${SINOMO_TEST_PERF_TIMEOUT:-120}"
PERF_MIN_WRITE_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC:-0.75}"
PERF_MIN_READ_FILES_PER_SEC="${SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC:-1}"
PERF_MAX_PROC_WCHAR_AMPLIFICATION="${SINOMO_TEST_PERF_MAX_PROC_WCHAR_AMPLIFICATION:-0}"
PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION="${SINOMO_TEST_PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION:-0}"
PERF_MAX_PROC_SYSCW_PER_FILE="${SINOMO_TEST_PERF_MAX_PROC_SYSCW_PER_FILE:-4}"
PERF_MAX_PROC_SYSCW_FIXED="${SINOMO_TEST_PERF_MAX_PROC_SYSCW_FIXED:-16}"
PERF_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_PERF_MAX_READ_PROC_WRITE_BYTES:-0}"
SMALL_BASELINE_FILES="${SINOMO_TEST_SMALL_BASELINE_FILES:-$PERF_FILES}"
SMALL_BASELINE_BYTES="${SINOMO_TEST_SMALL_BASELINE_BYTES:-$PERF_BYTES}"
SMALL_BASELINE_SYNC_WRITES="${SINOMO_TEST_SMALL_BASELINE_SYNC_WRITES:-0}"
SMALL_BASELINE_MIN_FILES_PER_SEC="${SINOMO_TEST_SMALL_BASELINE_MIN_FILES_PER_SEC:-0}"
LARGE_BYTES="${SINOMO_TEST_LARGE_BYTES:-268435456}"
LARGE_TIMEOUT="${SINOMO_TEST_LARGE_TIMEOUT:-180}"
LARGE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_WRITE_BYTES_PER_SEC:-134217728}"
LARGE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_LARGE_MIN_READ_BYTES_PER_SEC:-4194304}"
LARGE_MEASURE_RAW_WRITE_BASELINE="${SINOMO_TEST_LARGE_MEASURE_RAW_WRITE_BASELINE:-1}"
LARGE_MIN_RAW_WRITE_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_WRITE_RATIO:-0}"
LARGE_MIN_RAW_READ_RATIO="${SINOMO_TEST_LARGE_MIN_RAW_READ_RATIO:-0}"
LARGE_RAW_SYNC_WRITE="${SINOMO_TEST_LARGE_RAW_SYNC_WRITE:-1}"
LARGE_MAX_PROC_WCHAR_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WCHAR_AMPLIFICATION:-0}"
LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION="${SINOMO_TEST_LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION:-4}"
LARGE_MAX_READ_PROC_WRITE_BYTES="${SINOMO_TEST_LARGE_MAX_READ_PROC_WRITE_BYTES:-0}"
DISK_BASELINE_BYTES="${SINOMO_TEST_DISK_BASELINE_BYTES:-$LARGE_BYTES}"
DISK_BASELINE_SYNC_WRITE="${SINOMO_TEST_DISK_BASELINE_SYNC_WRITE:-1}"
DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC:-0}"
DISK_BASELINE_MIN_READ_BYTES_PER_SEC="${SINOMO_TEST_DISK_BASELINE_MIN_READ_BYTES_PER_SEC:-0}"
# Use a substantial fixture so corruption repair exercises the same RPC
# interface used by ordinary large values.
DAMAGE_BYTES="${SINOMO_TEST_DAMAGE_BYTES:-1048576}"
DFS_PROTOCOL_CODEC_BYTES="${SINOMO_TEST_DFS_PROTOCOL_CODEC_BYTES:-4194304}"
DFS_PROTOCOL_CODEC_ITERATIONS="${SINOMO_TEST_DFS_PROTOCOL_CODEC_ITERATIONS:-8}"
DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC:-67108864}"
DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC="${SINOMO_TEST_DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC:-67108864}"
NETWORK_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_REQUESTS_PER_TARGET:-20}"
NETWORK_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_REQUESTS_PER_TARGET}"
NETWORK_CONCURRENCY="${SINOMO_TEST_NETWORK_CONCURRENCY:-16}"
NETWORK_TIMEOUT_MS="${SINOMO_TEST_NETWORK_TIMEOUT_MS:-2000}"
NETWORK_MAX_FAILURES="${SINOMO_TEST_NETWORK_MAX_FAILURES:-0}"
# Every stream-native service call opens a logical duplex stream over a
# persistent transport-authenticated routed carrier. This loaded gate runs
# sixteen such calls concurrently across five targets;
# keep its latency ceiling distinct from the tighter, concurrency-one direct
# and one-forward realtime gates below.
NETWORK_MAX_P50_US="${SINOMO_TEST_NETWORK_MAX_P50_US:-60000}"
NETWORK_MAX_P95_US="${SINOMO_TEST_NETWORK_MAX_P95_US:-200000}"
NETWORK_MIN_RPS="${SINOMO_TEST_NETWORK_MIN_RPS:-250}"
NETWORK_REALTIME_REQUESTS="${SINOMO_TEST_NETWORK_REALTIME_REQUESTS:-100}"
NETWORK_REALTIME_WARMUP_REQUESTS="${SINOMO_TEST_NETWORK_REALTIME_WARMUP_REQUESTS:-20}"
# HARD CORRECTNESS GATES: after route stabilization, a direct small RPC and the
# same RPC over exactly one relay must both remain at or below 2ms at p50 and
# p95. These are deliberately non-configurable.
# Exceeding either limit is an implementation error (for example, reconnecting
# or rebuilding a stabilized route per call), not ordinary benchmark variance
# or an inefficiency to tolerate by widening the threshold.
NETWORK_DIRECT_MAX_US=2000
NETWORK_FORWARD_MAX_US=2000
# HARD ACCEPTANCE GATE: the cluster must remain continuously below the CPU
# ceiling for thirty monotonic seconds. A continuous Docker stats stream covers
# the complete interval; observation count is deliberately not part of the
# invariant. Keep this non-configurable and do not shorten it.
NETWORK_QUIET_DURATION_SECONDS=30
# Discovery has one absolute deadline. Operators may fail faster, but cannot
# lengthen this acceptance budget to hide a control-plane quietness failure.
NETWORK_QUIET_MAX_WAIT_SECONDS=120
NETWORK_QUIET_WAIT="${SINOMO_TEST_NETWORK_QUIET_WAIT:-$NETWORK_QUIET_MAX_WAIT_SECONDS}"
NETWORK_QUIET_MAX_CPU_PERCENT="${SINOMO_TEST_NETWORK_QUIET_MAX_CPU_PERCENT:-10}"
# A continuous Docker stats stream normally produces one cumulative CPU
# interval per second. Treat a five-second silence as unavailable evidence and
# reset rather than assuming the unobserved interval was quiet.
NETWORK_QUIET_STREAM_STALL_SECONDS=5
NETWORK_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_BYTES:-0}"
NETWORK_RESPONSE_PAYLOAD_BYTES="${SINOMO_TEST_NETWORK_RESPONSE_PAYLOAD_BYTES:-0}"
NETWORK_MIN_PAYLOAD_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_MIN_PAYLOAD_BYTES_PER_SEC:-0}"
NETWORK_PAYLOAD_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_REQUESTS_PER_TARGET:-4}"
NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET:-$NETWORK_PAYLOAD_REQUESTS_PER_TARGET}"
NETWORK_PAYLOAD_CONCURRENCY="${SINOMO_TEST_NETWORK_PAYLOAD_CONCURRENCY:-8}"
NETWORK_PAYLOAD_TIMEOUT_MS="${SINOMO_TEST_NETWORK_PAYLOAD_TIMEOUT_MS:-5000}"
# Exercise equal-size large values in both directions through the ordinary RPC
# interface; the fixture sizes are workload choices, not protocol boundaries.
NETWORK_PAYLOAD_REQUEST_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_REQUEST_BYTES:-262144}"
NETWORK_PAYLOAD_RESPONSE_BYTES="${SINOMO_TEST_NETWORK_PAYLOAD_RESPONSE_BYTES:-262144}"
NETWORK_PAYLOAD_MIN_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_PAYLOAD_MIN_BYTES_PER_SEC:-16777216}"
NETWORK_ATTACHMENT_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_ATTACHMENT_REQUESTS_PER_TARGET:-4}"
NETWORK_ATTACHMENT_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_ATTACHMENT_WARMUP_REQUESTS_PER_TARGET:-1}"
NETWORK_ATTACHMENT_CONCURRENCY="${SINOMO_TEST_NETWORK_ATTACHMENT_CONCURRENCY:-4}"
NETWORK_ATTACHMENT_TIMEOUT_MS="${SINOMO_TEST_NETWORK_ATTACHMENT_TIMEOUT_MS:-30000}"
NETWORK_ATTACHMENT_REQUEST_BYTES="${SINOMO_TEST_NETWORK_ATTACHMENT_REQUEST_BYTES:-8388608}"
NETWORK_ATTACHMENT_RESPONSE_BYTES="${SINOMO_TEST_NETWORK_ATTACHMENT_RESPONSE_BYTES:-8388608}"
NETWORK_ATTACHMENT_MIN_BYTES_PER_SEC="${SINOMO_TEST_NETWORK_ATTACHMENT_MIN_BYTES_PER_SEC:-0}"
NETWORK_SATURATION="${SINOMO_TEST_NETWORK_SATURATION:-0}"
NETWORK_SATURATION_ORIGIN="${SINOMO_TEST_NETWORK_SATURATION_ORIGIN:-node1}"
NETWORK_SATURATION_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_SATURATION_REQUESTS_PER_TARGET:-128}"
NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET:-1}"
NETWORK_SATURATION_CONCURRENCY="${SINOMO_TEST_NETWORK_SATURATION_CONCURRENCY:-8}"
NETWORK_SATURATION_TIMEOUT_MS="${SINOMO_TEST_NETWORK_SATURATION_TIMEOUT_MS:-120000}"
NETWORK_SATURATION_REQUEST_BYTES="${SINOMO_TEST_NETWORK_SATURATION_REQUEST_BYTES:-67108864}"
NETWORK_SATURATION_RESPONSE_BYTES="${SINOMO_TEST_NETWORK_SATURATION_RESPONSE_BYTES:-67108864}"
CLUSTER_KV_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_ITERATIONS:-12}"
CLUSTER_KV_WARMUP_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS:-4}"
CLUSTER_KV_VALUE_BYTES="${SINOMO_TEST_CLUSTER_KV_VALUE_BYTES:-256}"
CLUSTER_KV_BATCH_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS:-32}"
CLUSTER_KV_BATCH_OPS="${SINOMO_TEST_CLUSTER_KV_BATCH_OPS:-32}"
CLUSTER_KV_LARGE_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS:-16}"
CLUSTER_KV_LARGE_VALUE_BYTES="${SINOMO_TEST_CLUSTER_KV_LARGE_VALUE_BYTES:-16384}"
CLUSTER_KV_RANGE_ITERATIONS="${SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS:-64}"
CLUSTER_KV_RANGE_ENTRIES="${SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES:-64}"
CLUSTER_KV_SUITE_NODES="${SINOMO_TEST_CLUSTER_KV_SUITE_NODES:-node1 node5}"
CLUSTER_KV_TIMEOUT_MS="${SINOMO_TEST_CLUSTER_KV_TIMEOUT_MS:-5000}"
CLUSTER_KV_MAX_FAILURES="${SINOMO_TEST_CLUSTER_KV_MAX_FAILURES:-0}"
CLUSTER_KV_MAX_P50_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P50_MS:-1000}"
CLUSTER_KV_MAX_P95_MS="${SINOMO_TEST_CLUSTER_KV_MAX_P95_MS:-2000}"
CLUSTER_KV_MIN_COMMITS_PER_SECOND="${SINOMO_TEST_CLUSTER_KV_MIN_COMMITS_PER_SECOND:-1}"
CLUSTER_KV_MIN_READS_PER_SECOND="${SINOMO_TEST_CLUSTER_KV_MIN_READS_PER_SECOND:-0}"
NETWORK_READY_WAIT="${SINOMO_TEST_NETWORK_READY_WAIT:-120}"
NETWORK_READY_REQUESTS_PER_TARGET="${SINOMO_TEST_NETWORK_READY_REQUESTS_PER_TARGET:-20}"
NETWORK_READY_CONCURRENCY="${SINOMO_TEST_NETWORK_READY_CONCURRENCY:-$NETWORK_CONCURRENCY}"
NETWORK_PARTITION_SECONDS="${SINOMO_TEST_NETWORK_PARTITION_SECONDS:-5}"
NODES="node1 node2 node3 node4 node5 node6"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS="${SINOMO_TEST_ARTIFACTS:-$SINOMO_ROOT/target/cluster-test-artifacts/$RUN_ID}"
BINARY="${SINOMO_TEST_BINARY:-$SINOMO_ROOT/target/debug/sinomo}"
export SINOMO_DOCKER_BINARY="${SINOMO_DOCKER_BINARY:-$BINARY}"
COMPOSE_FILES=(-f "$COMPOSE_FILE")
if [ "$PJDFSTEST_ONLY" = 1 ]; then
  COMPOSE_FILES+=(-f "$SINOMO_DOCKER_DIR/docker-compose.pjdfstest.yml")
fi
if [ -n "$COMPOSE_OVERRIDE_FILE" ]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE_FILE")
fi

mkdir -p "$ARTIFACTS"

log() { echo "[test-cluster] $*"; }

require_bounded_uint() {
  local name="$1" value="$2" minimum="$3" maximum="$4" numeric
  if [[ ! "$value" =~ ^[0-9]+$ ]] || [ "${#value}" -gt 9 ]; then
    log "$name must be an integer in [$minimum, $maximum], got: $value"
    return 1
  fi
  numeric=$((10#$value))
  if [ "$numeric" -lt "$minimum" ] || [ "$numeric" -gt "$maximum" ]; then
    log "$name must be in [$minimum, $maximum], got: $value"
    return 1
  fi
}

validate_network_realtime_gate_config() {
  # The benchmark prepares every warmup and measured request up front. Keep
  # operator overrides useful for longer samples without allowing an accidental
  # enormous value to turn a latency gate into an OOM event.
  require_bounded_uint SINOMO_TEST_NETWORK_REALTIME_REQUESTS \
    "$NETWORK_REALTIME_REQUESTS" 1 10000 \
    && require_bounded_uint SINOMO_TEST_NETWORK_REALTIME_WARMUP_REQUESTS \
      "$NETWORK_REALTIME_WARMUP_REQUESTS" 1 1000 \
    && require_bounded_uint SINOMO_TEST_NETWORK_QUIET_WAIT \
      "$NETWORK_QUIET_WAIT" "$NETWORK_QUIET_DURATION_SECONDS" \
      "$NETWORK_QUIET_MAX_WAIT_SECONDS" \
    && require_bounded_uint SINOMO_TEST_NETWORK_QUIET_MAX_CPU_PERCENT \
      "$NETWORK_QUIET_MAX_CPU_PERCENT" 1 10 \
    && if [ -n "${SINOMO_TEST_NETWORK_QUIET_SAMPLES+x}" ]; then
      log "SINOMO_TEST_NETWORK_QUIET_SAMPLES is no longer supported; the quiet gate is a hard ${NETWORK_QUIET_DURATION_SECONDS}-second monotonic duration"
      return 1
    elif [ -n "${SINOMO_TEST_NETWORK_QUIET_POLL_SECONDS+x}" ]; then
      log "SINOMO_TEST_NETWORK_QUIET_POLL_SECONDS is no longer supported; the quiet gate consumes a continuous Docker stats stream"
      return 1
    fi
}

validate_network_saturation_config() {
  case "$NETWORK_SATURATION" in
    0) return 0 ;;
    1) ;;
    *)
      log "SINOMO_TEST_NETWORK_SATURATION must be 0 or 1, got: $NETWORK_SATURATION"
      return 1
      ;;
  esac
  case " $NODES " in
    *" $NETWORK_SATURATION_ORIGIN "*) ;;
    *)
      log "SINOMO_TEST_NETWORK_SATURATION_ORIGIN must name one of: $NODES"
      return 1
      ;;
  esac
  require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_REQUESTS_PER_TARGET \
    "$NETWORK_SATURATION_REQUESTS_PER_TARGET" 1 1024 \
    && require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET \
      "$NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET" 0 64 \
    && require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_CONCURRENCY \
      "$NETWORK_SATURATION_CONCURRENCY" 1 64 \
    && require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_TIMEOUT_MS \
      "$NETWORK_SATURATION_TIMEOUT_MS" 1000 300000 \
    && require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_REQUEST_BYTES \
      "$NETWORK_SATURATION_REQUEST_BYTES" 1 67108864 \
    && require_bounded_uint SINOMO_TEST_NETWORK_SATURATION_RESPONSE_BYTES \
      "$NETWORK_SATURATION_RESPONSE_BYTES" 1 67108864
}

validate_cluster_kv_benchmark_config() {
  case "$CLUSTER_KV_ONLY" in
    0|1) ;;
    *)
      log "SINOMO_TEST_CLUSTER_KV_ONLY must be 0 or 1, got: $CLUSTER_KV_ONLY"
      return 1
      ;;
  esac
  if [ "$CLUSTER_KV_ONLY" = 1 ] \
    && [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; then
    log "SINOMO_TEST_CLUSTER_KV_ONLY and SINOMO_TEST_NETWORK_ONLY are mutually exclusive"
    return 1
  fi
  if [ -z "$CLUSTER_KV_SUITE_NODES" ]; then
    log "SINOMO_TEST_CLUSTER_KV_SUITE_NODES must name at least one node"
    return 1
  fi
  local node
  for node in $CLUSTER_KV_SUITE_NODES; do
    case " $NODES " in
      *" $node "*) ;;
      *)
        log "SINOMO_TEST_CLUSTER_KV_SUITE_NODES contains unknown node $node; expected a subset of: $NODES"
        return 1
        ;;
    esac
  done
  require_bounded_uint SINOMO_TEST_CLUSTER_KV_ITERATIONS \
    "$CLUSTER_KV_ITERATIONS" 1 128 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_WARMUP_ITERATIONS \
      "$CLUSTER_KV_WARMUP_ITERATIONS" 0 64 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_VALUE_BYTES \
      "$CLUSTER_KV_VALUE_BYTES" 0 16384 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_BATCH_ITERATIONS \
      "$CLUSTER_KV_BATCH_ITERATIONS" 1 128 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_BATCH_OPS \
      "$CLUSTER_KV_BATCH_OPS" 1 256 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_LARGE_ITERATIONS \
      "$CLUSTER_KV_LARGE_ITERATIONS" 1 128 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_LARGE_VALUE_BYTES \
      "$CLUSTER_KV_LARGE_VALUE_BYTES" 0 16384 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_RANGE_ITERATIONS \
      "$CLUSTER_KV_RANGE_ITERATIONS" 1 512 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_RANGE_ENTRIES \
      "$CLUSTER_KV_RANGE_ENTRIES" 1 1024 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_TIMEOUT_MS \
      "$CLUSTER_KV_TIMEOUT_MS" 1 300000 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_MAX_FAILURES \
      "$CLUSTER_KV_MAX_FAILURES" 0 128 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_MAX_P50_MS \
      "$CLUSTER_KV_MAX_P50_MS" 0 60000 \
    && require_bounded_uint SINOMO_TEST_CLUSTER_KV_MAX_P95_MS \
      "$CLUSTER_KV_MAX_P95_MS" 0 60000
}

validate_dfs_suite_config() {
  require_bounded_uint SINOMO_TEST_QUIET_ONLY "$QUIET_ONLY" 0 1 || return 1
  if [ "$QUIET_ONLY" = 1 ] && { [ "$DFS_ONLY$DFS_READY_ONLY$FORMATION_ONLY$CLUSTER_KV_ONLY$PJDFSTEST_ONLY${SINOMO_TEST_NETWORK_ONLY:-0}" != 000000 ] || [ -n "$NETWORK_CASES" ]; }; then
    log "SINOMO_TEST_QUIET_ONLY is mutually exclusive with other focused lanes"
    return 1
  fi
  require_bounded_uint SINOMO_TEST_DFS_ONLY "$DFS_ONLY" 0 1 || return 1
  require_bounded_uint SINOMO_TEST_DFS_READY_REQUIRE_QUIET "$DFS_READY_REQUIRE_QUIET" 0 1 || return 1
  if [ "$DFS_READY_REQUIRE_QUIET" = 1 ] && [ "$DFS_READY_ONLY" != 1 ]; then
    log "SINOMO_TEST_DFS_READY_REQUIRE_QUIET requires SINOMO_TEST_DFS_READY_ONLY=1"
    return 1
  fi
  require_bounded_uint SINOMO_TEST_DFS_BENCH_ITERATIONS "${SINOMO_TEST_DFS_BENCH_ITERATIONS:-8}" 1 1000 || return 1
  require_bounded_uint SINOMO_TEST_DFS_BENCH_WARMUP "${SINOMO_TEST_DFS_BENCH_WARMUP:-2}" 0 100 || return 1
  require_bounded_uint SINOMO_TEST_DFS_BENCH_BYTES "${SINOMO_TEST_DFS_BENCH_BYTES:-4096}" 1 32768 || return 1
  if [ "$DFS_ONLY" = 1 ] && { [ "$DFS_READY_ONLY" = 1 ] || [ "$FORMATION_ONLY" = 1 ] \
    || [ "$CLUSTER_KV_ONLY" = 1 ] || [ "$PJDFSTEST_ONLY" = 1 ] \
    || [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; }; then
    log "SINOMO_TEST_DFS_ONLY is mutually exclusive with other focused lanes"
    return 1
  fi
}

validate_formation_observation_config() {
  case "$FORMATION_ONLY" in
    0|1) ;;
    *)
      log "SINOMO_TEST_FORMATION_ONLY must be 0 or 1, got: $FORMATION_ONLY"
      return 1
      ;;
  esac
  case "$DFS_READY_ONLY" in
    0|1) ;;
    *)
      log "SINOMO_TEST_DFS_READY_ONLY must be 0 or 1, got: $DFS_READY_ONLY"
      return 1
      ;;
  esac
  if [ "$DFS_READY_ONLY" = 1 ] \
    && { [ "$FORMATION_ONLY" = 1 ] \
      || [ "$CLUSTER_KV_ONLY" = 1 ] \
      || [ "$PJDFSTEST_ONLY" = 1 ] \
      || [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; }; then
    log "SINOMO_TEST_DFS_READY_ONLY is mutually exclusive with formation-only, cluster-KV-only, pjdfstest-only, and network-only lanes"
    return 1
  fi
  case "$FORMATION_OBSERVE" in
    0) return 0 ;;
    1) ;;
    *)
      log "SINOMO_TEST_FORMATION_OBSERVE must be 0 or 1, got: $FORMATION_OBSERVE"
      return 1
      ;;
  esac
  require_bounded_uint SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS \
    "$FORMATION_OBSERVE_DURATION_SECONDS" 1 30 \
    && require_bounded_uint SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ \
      "$FORMATION_OBSERVE_FREQUENCY_HZ" 10 500 \
    && require_bounded_uint SINOMO_TEST_FORMATION_OBSERVE_MAX_EVENTS \
      "$FORMATION_OBSERVE_MAX_EVENTS" 1 32768
}

validate_pjdfstest_config() {
  case "$PJDFSTEST_ONLY" in
    0) return 0 ;;
    1) ;;
    *)
      log "SINOMO_TEST_PJDFSTEST_ONLY must be 0 or 1, got: $PJDFSTEST_ONLY"
      return 1
      ;;
  esac
  if [ "$CLUSTER_KV_ONLY" = 1 ] \
    || [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ] \
    || [ "$FORMATION_ONLY" = 1 ]; then
    log "SINOMO_TEST_PJDFSTEST_ONLY is mutually exclusive with the cluster-KV, network-only, and formation-only lanes"
    return 1
  fi
  case "$PJDFSTEST_PROFILE" in
    smoke|full) ;;
    *)
      log "SINOMO_PJDFSTEST_PROFILE must be smoke or full, got: $PJDFSTEST_PROFILE"
      return 1
      ;;
  esac
  case "$PJDFSTEST_STRICT" in
    0|1) ;;
    *)
      log "SINOMO_PJDFSTEST_STRICT must be 0 or 1, got: $PJDFSTEST_STRICT"
      return 1
      ;;
  esac
  require_bounded_uint SINOMO_PJDFSTEST_TIMEOUT_SECONDS \
    "$PJDFSTEST_TIMEOUT_SECONDS" 60 86400
}

compose() {
  docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"
}

compose_timeout() {
  local seconds="$1"
  shift
  timeout --foreground "${seconds}s" \
    docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"
}

compose_exec_timeout() {
  local seconds="$1" node="$2" seconds_ms grace_ms host_seconds_ms host_seconds
  shift 2
  if ! seconds_ms="$(duration_seconds_to_millis "$seconds")" \
    || [ "$seconds_ms" -le 0 ]; then
    log "invalid compose exec timeout: $seconds" >&2
    return 2
  fi
  if ! grace_ms="$(duration_seconds_to_millis "$HOST_TIMEOUT_GRACE")"; then
    log "invalid host timeout grace: $HOST_TIMEOUT_GRACE" >&2
    return 2
  fi
  host_seconds_ms=$(( seconds_ms + grace_ms ))
  host_seconds="$(duration_millis_to_seconds "$host_seconds_ms")" || return 2
  timeout --foreground "${host_seconds}s" \
    docker compose -p "$PROJECT" "${COMPOSE_FILES[@]}" \
    exec -T "$node" timeout --foreground "${seconds}s" "$@"
}

container_config_env_value() {
  local node="$1" key="$2" container
  container="$(compose ps -q "$node")"
  [ -n "$container" ] || return 1
  docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" 2>/dev/null \
    | awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }'
}

container_network_set() {
  local node="$1" container
  container="$(compose ps -q "$node")"
  [ -n "$container" ] || return 1
  # `println` plus Docker inspect's own terminator leaves one blank record.
  # Ignore it so an otherwise valid network set does not gain a leading space.
  docker inspect \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' \
    "$container" 2>/dev/null \
    | sort \
    | awk 'BEGIN { separator = "" } NF { printf "%s%s", separator, $0; separator = " " } END { print "" }'
}

assert_network_realtime_topology() {
  # The ordinary ping response proves the terminal responder but deliberately
  # carries no hop trace. Pin the container adjacency (and disable the overlay)
  # so the forwarding leg has exactly the intended Docker path without adding
  # benchmark-only protocol metadata.
  local node1_networks node3_networks node4_networks iroh
  node1_networks="$(container_network_set node1)" || return 1
  node3_networks="$(container_network_set node3)" || return 1
  node4_networks="$(container_network_set node4)" || return 1
  if [ "$node1_networks" != "${PROJECT}_seg_a" ] \
    || [ "$node3_networks" != "${PROJECT}_seg_a ${PROJECT}_seg_ab" ] \
    || [ "$node4_networks" != "${PROJECT}_seg_ab ${PROJECT}_seg_b" ]; then
    log "network-realtime gate: topology does not guarantee node4 -> node3 -> node1"
    log "  node1 networks: $node1_networks"
    log "  node3 networks: $node3_networks"
    log "  node4 networks: $node4_networks"
    return 1
  fi
  for node in node1 node3 node4; do
    iroh="$(container_config_env_value "$node" SINOMO_DISABLE_IROH)" || iroh=""
    if [ "$iroh" != 1 ]; then
      log "network-realtime gate: $node must have SINOMO_DISABLE_IROH=1 to prove the Docker route"
      return 1
    fi
  done
  log "  network-realtime topology: node4 -> node3 -> node1 is the only Docker path"
}

network_quiet_cpu_sample_status() {
  local value="${1%%%}" limit="$2"
  if [[ ! "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    printf '%s\n' invalid
    return 0
  fi
  if awk -v value="$value" -v limit="$limit" \
    'BEGIN { exit !((value + 0) <= (limit + 0)) }'; then
    printf '%s\n' within
  else
    printf '%s\n' above
  fi
}

network_quiet_generation_matches() {
  [ "$1" = "$2" ]
}

# Pure state helpers keep the gate's temporal invariant independently
# testable. A quiet observation retains the first qualifying monotonic instant;
# every non-quiet outcome (busy CPU, sampling error, or daemon-generation
# transition) clears it.
network_quiet_window_start_after_observation() {
  local current="$1" observed_at="$2" outcome="$3"
  [[ "$current" =~ ^[0-9]{1,16}$ ]] || return 1
  [[ "$observed_at" =~ ^[0-9]{1,16}$ ]] || return 1
  case "$outcome" in
    quiet)
      if [ "$((10#$current))" -eq 0 ]; then
        printf '%s\n' "$observed_at"
      elif [ "$((10#$observed_at))" -ge "$((10#$current))" ]; then
        printf '%s\n' "$current"
      else
        return 1
      fi
      ;;
    reset)
      printf '%s\n' 0
      ;;
    *) return 1 ;;
  esac
}

network_quiet_elapsed_millis() {
  local started="$1" now="$2"
  [[ "$started" =~ ^[0-9]{1,16}$ ]] || return 1
  [[ "$now" =~ ^[0-9]{1,16}$ ]] || return 1
  [ "$((10#$started))" -gt 0 ] || return 1
  [ "$((10#$now))" -ge "$((10#$started))" ] || return 1
  printf '%s\n' "$((10#$now - 10#$started))"
}

network_quiet_duration_reached() {
  local started="$1" now="$2" required="$3" elapsed
  [[ "$required" =~ ^[0-9]{1,16}$ ]] || return 1
  [ "$((10#$required))" -gt 0 ] || return 1
  elapsed="$(network_quiet_elapsed_millis "$started" "$now")" || return 1
  [ "$((10#$elapsed))" -ge "$((10#$required))" ]
}

# Portable process scanner used both inside the benchmark containers and by the
# helper regression below. The process comm check is load-bearing: the scanner
# itself carries the daemon role literals in its argv and must not identify
# that observer shell as another daemon.
NETWORK_QUIET_DAEMON_GENERATION_PROGRAM='
    proc_root="${1:-/proc}"
    main_count=0
    shadow_count=0
    main_pid=""
    main_start=""
    shadow_pid=""
    shadow_start=""
    for proc in "$proc_root"/[0-9]*; do
      [ -r "$proc/comm" ] && [ -r "$proc/cmdline" ] && [ -r "$proc/stat" ] \
        || continue
      pid=${proc##*/}
      IFS= read -r comm < "$proc/comm" || continue
      [ "$comm" = sinomo ] || continue
      cmd=$(tr "\000" " " < "$proc/cmdline" 2>/dev/null) || continue
      case " $cmd " in
        *" daemon shadow run "*) role=shadow ;;
        *" daemon run "*) role=main ;;
        *) continue ;;
      esac
      start=$(awk "{print \$22}" "$proc/stat" 2>/dev/null)
      [ -n "$start" ] || exit 1
      if [ "$role" = main ]; then
        main_count=$((main_count + 1))
        main_pid=$pid
        main_start=$start
      else
        shadow_count=$((shadow_count + 1))
        shadow_pid=$pid
        shadow_start=$start
      fi
    done
    [ "$main_count" -eq 1 ] && [ "$shadow_count" -eq 1 ] || exit 1
    printf "%s\t%s\t%s\t%s\n" \
      "$main_pid" "$main_start" "$shadow_pid" "$shadow_start"
  '

# Return the exact main/shadow process generations inside one container. The
# aggregate container CPU sample remains the quietness authority; these
# identities only prevent a daemon replacement from resetting its counters or
# briefly disappearing during a candidate quiet window.
container_daemon_generation_snapshot() {
  local container="$1" command_timeout="${2:-${READINESS_COMMAND_TIMEOUT}s}"
  timeout --foreground "$command_timeout" docker exec "$container" \
    sh -c "$NETWORK_QUIET_DAEMON_GENERATION_PROGRAM" sh /proc 2>/dev/null
}

network_quiet_command_timeout_before_deadline() {
  local deadline_ms="$1" maximum_seconds="$2" now_ms maximum_ms timeout_ms
  now_ms="$(monotonic_millis)" || return 1
  maximum_ms="$(duration_seconds_to_millis "$maximum_seconds")" || return 1
  timeout_ms="$(
    bounded_deadline_timeout_millis "$deadline_ms" "$now_ms" "$maximum_ms"
  )" || return 1
  duration_millis_to_seconds "$timeout_ms"
}

network_quiet_generation_snapshot_before_deadline() {
  local container="$1" deadline_ms="$2" command_timeout
  command_timeout="$(
    network_quiet_command_timeout_before_deadline \
      "$deadline_ms" "$READINESS_COMMAND_TIMEOUT"
  )" || return 1
  container_daemon_generation_snapshot "$container" "$command_timeout"
}

network_quiet_container_for_node_before_deadline() {
  local node="$1" deadline_ms="$2" command_timeout
  command_timeout="$(
    network_quiet_command_timeout_before_deadline \
      "$deadline_ms" "$READINESS_COMMAND_TIMEOUT"
  )" || return 1
  compose_timeout "$command_timeout" ps -q "$node"
}

network_quiet_container_name_before_deadline() {
  local container="$1" deadline_ms="$2" command_timeout
  command_timeout="$(
    network_quiet_command_timeout_before_deadline \
      "$deadline_ms" "$READINESS_COMMAND_TIMEOUT"
  )" || return 1
  timeout --foreground "$command_timeout" \
    docker inspect --format '{{.Name}}' "$container" 2>/dev/null
}

NETWORK_QUIET_STATS_STREAM_FD=""
NETWORK_QUIET_STATS_STREAM_PID=""
# Test-only lifecycle evidence for the continuous stats pipeline. Production
# runs leave this empty and pay no artifact or synchronization cost.
NETWORK_QUIET_STATS_STREAM_TEST_LIFECYCLE_FILE=""
NETWORK_QUIET_GROUP_MIN_MS=0
NETWORK_QUIET_GROUP_MAX_MS=0
NETWORK_QUIET_GROUP_REASON=""
NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=0
declare -A NETWORK_QUIET_GROUP_CPU=()
declare -A NETWORK_QUIET_GROUP_CPU_STATUS=()
declare -A NETWORK_QUIET_GROUP_NAME=()
declare -A NETWORK_QUIET_GROUP_OBSERVED_MS=()
declare -A NETWORK_QUIET_LAST_OBSERVED_MS=()

network_quiet_stats_stream_worker_cleanup() {
  local producer_pid="$1" transform_pid="$2"
  local raw_fifo="$3" clean_fifo="$4" worker_dir="$5"
  local lifecycle_file="${6:-}" pid
  for pid in "$producer_pid" "$transform_pid"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || true
  done
  if [ -n "$producer_pid" ]; then
    wait "$producer_pid" 2>/dev/null || true
    if [ -n "$lifecycle_file" ]; then
      printf 'producer_waited=true\n' >> "$lifecycle_file" || true
    fi
  fi
  if [ -n "$transform_pid" ]; then
    wait "$transform_pid" 2>/dev/null || true
    if [ -n "$lifecycle_file" ]; then
      printf 'transform_waited=true\n' >> "$lifecycle_file" || true
    fi
  fi
  rm -f -- "$raw_fifo" "$clean_fifo"
  rmdir "$worker_dir" 2>/dev/null || true
  if [ -n "$lifecycle_file" ]; then
    printf 'cleanup_complete=true\n' >> "$lifecycle_file" || true
  fi
}

network_quiet_stats_stream_worker() {
  local worker_dir raw_fifo clean_fifo producer_pid="" transform_pid="" row
  local observed_ms
  local lifecycle_file="$NETWORK_QUIET_STATS_STREAM_TEST_LIFECYCLE_FILE"
  worker_dir="$(
    mktemp -d "${TMPDIR:-/tmp}/sinomo-network-quiet-stream.XXXXXX"
  )" || return 1
  raw_fifo="$worker_dir/raw.fifo"
  clean_fifo="$worker_dir/clean.fifo"
  if ! mkfifo "$raw_fifo" "$clean_fifo"; then
    rm -f -- "$raw_fifo" "$clean_fifo"
    rmdir "$worker_dir" 2>/dev/null || true
    return 1
  fi
  trap 'network_quiet_stats_stream_worker_cleanup "$producer_pid" "$transform_pid" "$raw_fifo" "$clean_fifo" "$worker_dir" "$lifecycle_file"' EXIT
  trap 'exit 143' HUP INT TERM

  docker stats --format '{{.Name}}|{{.CPUPerc}}' "$@" \
    > "$raw_fifo" 2>/dev/null &
  producer_pid=$!
  sed -u $'s/\033\\[[0-9;?]*[[:alpha:]]//g' \
    < "$raw_fifo" > "$clean_fifo" &
  transform_pid=$!
  if [ -n "$lifecycle_file" ]; then
    {
      printf 'annotator_pid=%s\n' "$BASHPID"
      printf 'producer_pid=%s\n' "$producer_pid"
      printf 'transform_pid=%s\n' "$transform_pid"
      printf 'children_started=true\n'
    } > "$lifecycle_file" || return 1
  fi

  while IFS= read -r row; do
    [ -n "$row" ] || continue
    observed_ms="$(monotonic_millis)" || return 1
    printf '%s|%s\n' "$observed_ms" "$row" || return 1
  done < "$clean_fifo"
}

network_quiet_stats_stream_stop() {
  if [ -n "$NETWORK_QUIET_STATS_STREAM_FD" ]; then
    exec {NETWORK_QUIET_STATS_STREAM_FD}<&-
    NETWORK_QUIET_STATS_STREAM_FD=""
  fi
  if [ -n "$NETWORK_QUIET_STATS_STREAM_PID" ]; then
    kill "$NETWORK_QUIET_STATS_STREAM_PID" 2>/dev/null || true
    wait "$NETWORK_QUIET_STATS_STREAM_PID" 2>/dev/null || true
    NETWORK_QUIET_STATS_STREAM_PID=""
  fi
}

network_quiet_stats_stream_start() {
  network_quiet_stats_stream_stop
  NETWORK_QUIET_LAST_OBSERVED_MS=()
  exec {NETWORK_QUIET_STATS_STREAM_FD}< <(
    network_quiet_stats_stream_worker "$@"
  )
  NETWORK_QUIET_STATS_STREAM_PID=$!
}

# Read one complete interval group from the continuous Docker stats stream.
# Every requested container contributes exactly one cumulative CPU interval;
# duplicates, missing/unknown containers, malformed values, stream stalls, and
# timestamp regressions are unavailable evidence rather than implicit quiet.
network_quiet_stats_stream_read_group() {
  local deadline_ms="$1" expected_count="$2"
  local row read_timeout observed_ms observed_name observed_cpu extra
  local node sample_status line_count=0 now_ms interval_ms
  NETWORK_QUIET_GROUP_MIN_MS=0
  NETWORK_QUIET_GROUP_MAX_MS=0
  NETWORK_QUIET_GROUP_REASON=""
  NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=0
  NETWORK_QUIET_GROUP_CPU=()
  NETWORK_QUIET_GROUP_CPU_STATUS=()
  NETWORK_QUIET_GROUP_NAME=()
  NETWORK_QUIET_GROUP_OBSERVED_MS=()

  while [ "$line_count" -lt "$expected_count" ]; do
    read_timeout="$(
      network_quiet_command_timeout_before_deadline \
        "$deadline_ms" "$NETWORK_QUIET_STREAM_STALL_SECONDS"
    )" || {
      NETWORK_QUIET_GROUP_REASON="quiet-window-deadline-exhausted"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      return 1
    }
    if ! IFS= read -r -t "$read_timeout" -u "$NETWORK_QUIET_STATS_STREAM_FD" row; then
      now_ms="$(monotonic_millis || printf '%s' "$deadline_ms")"
      if [ "$((10#$now_ms))" -ge "$((10#$deadline_ms))" ]; then
        NETWORK_QUIET_GROUP_REASON="quiet-window-deadline-exhausted"
      elif [ -n "$NETWORK_QUIET_STATS_STREAM_PID" ] \
        && kill -0 "$NETWORK_QUIET_STATS_STREAM_PID" 2>/dev/null; then
        NETWORK_QUIET_GROUP_REASON="docker-stats-stream-stalled"
      else
        NETWORK_QUIET_GROUP_REASON="docker-stats-stream-exited"
      fi
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      return 1
    fi
    line_count=$((line_count + 1))
    observed_ms=""; observed_name=""; observed_cpu=""; extra=""
    IFS='|' read -r observed_ms observed_name observed_cpu extra <<< "$row"
    if [[ ! "$observed_ms" =~ ^[0-9]{1,16}$ ]] \
      || [ "$((10#$observed_ms))" -le 0 ] \
      || [ -n "$extra" ]; then
      NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }malformed-stream-row"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      continue
    fi
    if [ "$NETWORK_QUIET_GROUP_MIN_MS" -eq 0 ] \
      || [ "$((10#$observed_ms))" -lt "$NETWORK_QUIET_GROUP_MIN_MS" ]; then
      NETWORK_QUIET_GROUP_MIN_MS="$observed_ms"
    fi
    if [ "$((10#$observed_ms))" -gt "$NETWORK_QUIET_GROUP_MAX_MS" ]; then
      NETWORK_QUIET_GROUP_MAX_MS="$observed_ms"
    fi
    node="${node_by_container_name[$observed_name]:-}"
    if [ -z "$node" ]; then
      NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }unexpected-container=$observed_name"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      continue
    fi
    if [ -n "${NETWORK_QUIET_GROUP_NAME[$node]:-}" ]; then
      NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }duplicate-container=$observed_name"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      continue
    fi
    if [ -n "${NETWORK_QUIET_LAST_OBSERVED_MS[$node]:-}" ] \
      && [ "$((10#$observed_ms))" -le \
        "$((10#${NETWORK_QUIET_LAST_OBSERVED_MS[$node]}))" ]; then
      NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }non-monotonic-container=$observed_name"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
    elif [ -n "${NETWORK_QUIET_LAST_OBSERVED_MS[$node]:-}" ]; then
      interval_ms=$((
        10#$observed_ms - 10#${NETWORK_QUIET_LAST_OBSERVED_MS[$node]}
      ))
      if [ "$interval_ms" -gt \
        "$((NETWORK_QUIET_STREAM_STALL_SECONDS * 1000))" ]; then
        NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }container-interval-gap=${observed_name}:${interval_ms}ms"
        NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      fi
    fi
    NETWORK_QUIET_LAST_OBSERVED_MS[$node]="$observed_ms"
    NETWORK_QUIET_GROUP_NAME[$node]="$observed_name"
    NETWORK_QUIET_GROUP_CPU[$node]="$observed_cpu"
    NETWORK_QUIET_GROUP_OBSERVED_MS[$node]="$observed_ms"
    sample_status="$(
      network_quiet_cpu_sample_status \
        "$observed_cpu" "$NETWORK_QUIET_MAX_CPU_PERCENT"
    )"
    NETWORK_QUIET_GROUP_CPU_STATUS[$node]="$sample_status"
    case "$sample_status" in
      within) ;;
      above)
        NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }$observed_name=$observed_cpu"
        ;;
      *)
        NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }$observed_name=invalid($observed_cpu)"
        NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
        ;;
    esac
  done

  for node in $NODES; do
    if [ -z "${NETWORK_QUIET_GROUP_NAME[$node]:-}" ]; then
      NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }missing-container=$node"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
    fi
  done
  if [ "$NETWORK_QUIET_GROUP_MIN_MS" -le 0 ] \
    || [ "$NETWORK_QUIET_GROUP_MAX_MS" -lt "$NETWORK_QUIET_GROUP_MIN_MS" ]; then
    NETWORK_QUIET_GROUP_REASON="${NETWORK_QUIET_GROUP_REASON}${NETWORK_QUIET_GROUP_REASON:+ }invalid-group-timestamps"
    NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
  fi
  return 0
}

network_quiet_coverage_reaches() {
  local covered_through_ms="$1" target_ms="$2"
  [[ "$covered_through_ms" =~ ^[0-9]{1,16}$ ]] || return 1
  [[ "$target_ms" =~ ^[0-9]{1,16}$ ]] || return 1
  [ "$((10#$covered_through_ms))" -ge "$((10#$target_ms))" ]
}

network_quiet_write_stats_group_samples() {
  local path="$1" sample_group="$2" candidate="$3" quiet_elapsed_ms="$4"
  local group_state="$5" captured_at captured_ms node container_name
  local observed_cpu sample_status observed_ms
  captured_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
  captured_ms="$(wall_clock_millis 2>/dev/null || printf '0')"
  for node in $NODES; do
    container_name="${NETWORK_QUIET_GROUP_NAME[$node]:-unavailable}"
    observed_cpu="${NETWORK_QUIET_GROUP_CPU[$node]:-}"
    observed_ms="${NETWORK_QUIET_GROUP_OBSERVED_MS[$node]:-0}"
    sample_status="${NETWORK_QUIET_GROUP_CPU_STATUS[$node]:-invalid}"
    if [ "$sample_status" = within ]; then
      sample_status=true
    else
      sample_status=false
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$sample_group" "$quiet_elapsed_ms" "$candidate" "$captured_at" \
      "$captured_ms" "$observed_ms" "$NETWORK_QUIET_GROUP_MIN_MS" \
      "$NETWORK_QUIET_GROUP_MAX_MS" "$node" "${container_by_node[$node]:-}" \
      "$container_name" "$observed_cpu" "$NETWORK_QUIET_MAX_CPU_PERCENT" \
      "$sample_status" "$group_state" >> "$path"
  done
}

network_quiet_establish_generation_baseline() {
  local deadline_ms="$1" generation_epoch="$2" generations_file="$3"
  local node container snapshot main_pid main_start shadow_pid shadow_start
  local captured_at captured_ms baseline_ok=1
  captured_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
  captured_ms="$(wall_clock_millis 2>/dev/null || printf '0')"
  generation_baseline=()
  for node in $NODES; do
    container="${container_by_node[$node]}"
    if snapshot="$(
      network_quiet_generation_snapshot_before_deadline \
        "$container" "$deadline_ms"
    )"; then
      read -r main_pid main_start shadow_pid shadow_start <<< "$snapshot"
      generation_baseline[$node]="$container|$snapshot"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$generation_epoch" baseline "$captured_at" "$captured_ms" "$node" \
        "$container" "$main_pid" "$main_start" "$shadow_pid" \
        "$shadow_start" baseline ok >> "$generations_file"
    else
      baseline_ok=0
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$generation_epoch" baseline "$captured_at" "$captured_ms" "$node" \
        "$container" '' '' '' '' false unavailable >> "$generations_file"
    fi
  done
  [ "$baseline_ok" -eq 1 ]
}

NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS=0
network_quiet_verify_generation_baseline() {
  local deadline_ms="$1" generation_epoch="$2" generations_file="$3"
  local node current_container current_snapshot
  local main_pid main_start shadow_pid shadow_start matches=1 sample_status
  local captured_at captured_ms
  captured_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
  captured_ms="$(wall_clock_millis 2>/dev/null || printf '0')"
  for node in $NODES; do
    current_container="$(
      network_quiet_container_for_node_before_deadline "$node" "$deadline_ms" \
        || true
    )"
    current_snapshot=""
    if [ -n "$current_container" ]; then
      current_snapshot="$(
        network_quiet_generation_snapshot_before_deadline \
          "$current_container" "$deadline_ms" || true
      )"
    fi
    if [ -n "$current_snapshot" ]; then
      read -r main_pid main_start shadow_pid shadow_start <<< "$current_snapshot"
    else
      main_pid=""; main_start=""; shadow_pid=""; shadow_start=""
    fi
    if [ -n "$current_container" ] \
      && network_quiet_generation_matches \
        "${generation_baseline[$node]:-}" \
        "$current_container|$current_snapshot"; then
      sample_status=true
    else
      sample_status=false
      matches=0
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$generation_epoch" verify "$captured_at" "$captured_ms" "$node" \
      "${current_container:-unavailable}" "$main_pid" "$main_start" \
      "$shadow_pid" "$shadow_start" "$sample_status" \
      "$([ "$sample_status" = true ] && printf ok || printf changed)" \
      >> "$generations_file"
  done
  NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS="$(
    monotonic_millis || printf '%s' "$deadline_ms"
  )"
  [ "$((10#$NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS))" \
    -lt "$((10#$deadline_ms))" ] || return 1
  [ "$matches" -eq 1 ]
}

network_quiet_stream_restart_delay() {
  local deadline_ms="$1" delay
  delay="$(
    network_quiet_command_timeout_before_deadline "$deadline_ms" 1
  )" || return 1
  sleep "$delay"
}

write_network_quiet_summary() {
  local path="$1" label="$2" status="$3" reason="$4"
  local started_at="$5" finished_at="$6" elapsed="$7" elapsed_ms="$8"
  local observed_groups="$9" candidate="${10}" qualifying_groups="${11}"
  local quiet_elapsed_ms="${12}" generation_stable="${13}"
  local samples_file="${14}" generations_file="${15}"
  reason="${reason//$'\n'/ }"
  {
    printf 'protocol=sinomo.test.network-quiet.v2\n'
    printf 'label=%s\n' "$label"
    printf 'status=%s\n' "$status"
    printf 'reason=%s\n' "$reason"
    printf 'started_at=%s\n' "$started_at"
    printf 'finished_at=%s\n' "$finished_at"
    printf 'elapsed_seconds=%s\n' "$elapsed"
    printf 'elapsed_milliseconds=%s\n' "$elapsed_ms"
    printf 'duration_clock=linux-monotonic-uptime\n'
    printf 'cpu_authority=docker-container-aggregate\n'
    printf 'container_count=%s\n' "$EXPECTED_MAINS"
    printf 'max_cpu_percent=%s\n' "$NETWORK_QUIET_MAX_CPU_PERCENT"
    printf 'required_quiet_duration_seconds=%s\n' "$NETWORK_QUIET_DURATION_SECONDS"
    printf 'required_quiet_duration_milliseconds=%s\n' \
      "$((NETWORK_QUIET_DURATION_SECONDS * 1000))"
    printf 'cpu_sampling=docker-stats-continuous-stream\n'
    printf 'stream_stall_reset_seconds=%s\n' \
      "$NETWORK_QUIET_STREAM_STALL_SECONDS"
    printf 'wait_budget_seconds=%s\n' "$NETWORK_QUIET_WAIT"
    printf 'observed_sample_groups=%s\n' "$observed_groups"
    printf 'final_candidate=%s\n' "$candidate"
    printf 'final_qualifying_sample_groups=%s\n' "$qualifying_groups"
    printf 'final_quiet_elapsed_milliseconds=%s\n' "$quiet_elapsed_ms"
    printf 'daemon_generations_stable=%s\n' "$generation_stable"
    printf 'samples_file=%s\n' "$(basename "$samples_file")"
    printf 'generations_file=%s\n' "$(basename "$generations_file")"
    printf '%s\n' 'coverage=continuous cumulative CPU intervals for every process in node1..node6 containers through final generation verification; daemon generations are a stability guard, not the CPU authority'
  } > "$path"
}


wait_for_network_quiet() {
  local label="$1" label_slug diagnostic_pid="" gate_rc=0
  label_slug="${label//[^a-zA-Z0-9._-]/_}"
  if [ "${SINOMO_TEST_TIMING_HELPERS_SELF_TEST:-0}" != 1 ]; then
    python3 "$SINOMO_DOCKER_DIR/profile-idle.py" \
      --project "$PROJECT" --binary "$BINARY" \
      --source-revision "$(git -C "$SINOMO_ROOT" rev-parse HEAD)-working-tree" \
      --profile "${SINOMO_TEST_PROFILE:-debug+cpu-profiler}" \
      --artifacts "$ARTIFACTS/idle-$label_slug" \
      > "$ARTIFACTS/idle-$label_slug.log" 2>&1 &
    diagnostic_pid=$!
  fi
  wait_for_network_quiet_gate "$label" || gate_rc=$?
  if [ -n "$diagnostic_pid" ]; then
    if ! wait "$diagnostic_pid"; then
      log "network-quiet/$label: idle diagnostic coverage is partial; see idle-$label_slug/REPORT.md"
    fi
  fi
  return "$gate_rc"
}

wait_for_network_quiet_gate() {
  local label="$1" label_slug started_at finished_at
  local started_monotonic_ms finished_monotonic_ms deadline_ms now_ms
  local required_quiet_ms wait_budget_ms elapsed_ms elapsed
  local node container container_name resolution_ok stream_state
  local sample_group=0 candidate=0 qualifying_groups=0 quiet_elapsed_ms=0
  local generation_epoch=0
  local candidate_started_ms=0 baseline_finished_ms=0
  local generation_stable="unknown" last_reason="" restart_stream
  local verification_reset verification_target_ms
  local -a containers=()
  local -A container_by_node=() node_by_container_name=()
  local -A generation_baseline=()
  label_slug="${label//[^a-zA-Z0-9._-]/_}"
  local samples_file="$ARTIFACTS/network-quiet-$label_slug.samples.tsv"
  local generations_file="$ARTIFACTS/network-quiet-$label_slug.generations.tsv"
  local summary_file="$ARTIFACTS/network-quiet-$label_slug.summary.txt"

  started_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
  if ! started_monotonic_ms="$(monotonic_millis)"; then
    log "network-quiet/$label: monotonic clock unavailable"
    return 1
  fi
  required_quiet_ms="$(
    duration_seconds_to_millis "$NETWORK_QUIET_DURATION_SECONDS"
  )" || return 1
  wait_budget_ms="$(duration_seconds_to_millis "$NETWORK_QUIET_WAIT")" || return 1
  deadline_ms=$((10#$started_monotonic_ms + 10#$wait_budget_ms))
  printf '%s\n' $'sample_group\tquiet_elapsed_ms\tcandidate\tconsumed_at\tconsumed_epoch_ms\tobserved_monotonic_ms\tgroup_min_monotonic_ms\tgroup_max_monotonic_ms\tnode\tcontainer_id\tcontainer_name\tcpu_percent\tmax_cpu_percent\tcpu_within_limit\twindow_state' \
    > "$samples_file"
  printf '%s\n' $'generation_epoch\tphase\tcaptured_at\tcaptured_epoch_ms\tnode\tcontainer_id\tmain_pid\tmain_start_ticks\tshadow_pid\tshadow_start_ticks\tmatches_baseline\tstatus' \
    > "$generations_file"

  log "network-quiet/$label: waiting for $EXPECTED_MAINS whole containers at <=${NETWORK_QUIET_MAX_CPU_PERCENT}% CPU for an uninterrupted ${NETWORK_QUIET_DURATION_SECONDS}s continuous-stream window (absolute ${NETWORK_QUIET_WAIT}s deadline)"
  log "  network-quiet/$label: samples -> $samples_file"
  log "  network-quiet/$label: daemon generations -> $generations_file"

  while :; do
    now_ms="$(monotonic_millis || printf '%s' "$deadline_ms")"
    if [ "$((10#$now_ms))" -ge "$deadline_ms" ]; then
      last_reason="quiet-window-deadline-exhausted"
      break
    fi

    containers=()
    container_by_node=()
    node_by_container_name=()
    generation_baseline=()
    resolution_ok=1
    for node in $NODES; do
      container="$(
        network_quiet_container_for_node_before_deadline "$node" "$deadline_ms" \
          || true
      )"
      if [ -z "$container" ]; then
        last_reason="cannot-resolve-container=$node"
        resolution_ok=0
        break
      fi
      container_name="$(
        network_quiet_container_name_before_deadline "$container" "$deadline_ms" \
          || true
      )"
      container_name="${container_name#/}"
      if [ -z "$container_name" ] \
        || [ -n "${node_by_container_name[$container_name]:-}" ]; then
        last_reason="cannot-resolve-unique-container-name=$node"
        resolution_ok=0
        break
      fi
      containers+=("$container")
      container_by_node[$node]="$container"
      node_by_container_name[$container_name]="$node"
    done
    if [ "$resolution_ok" -ne 1 ] \
      || [ "${#containers[@]}" -ne "$EXPECTED_MAINS" ]; then
      network_quiet_stats_stream_stop
      network_quiet_stream_restart_delay "$deadline_ms" || {
        last_reason="quiet-window-deadline-exhausted"
        break
      }
      continue
    fi

    if ! network_quiet_stats_stream_start "${containers[@]}"; then
      last_reason="docker-stats-stream-start-failed"
      network_quiet_stream_restart_delay "$deadline_ms" || break
      continue
    fi
    generation_epoch=$((generation_epoch + 1))

    # Discard one complete stream group so every later percentage is backed by
    # a prior sample from this same continuous stream.
    if ! network_quiet_stats_stream_read_group \
      "$deadline_ms" "${#containers[@]}"; then
      last_reason="${NETWORK_QUIET_GROUP_REASON:-docker-stats-stream-unavailable}"
      network_quiet_stats_stream_stop
      network_quiet_stream_restart_delay "$deadline_ms" || break
      continue
    fi
    sample_group=$((sample_group + 1))
    if [ "$NETWORK_QUIET_GROUP_STRUCTURAL_ERROR" -eq 1 ]; then
      stream_state=stream-warmup-invalid
    else
      stream_state=stream-warmup
    fi
    network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
      "$candidate" 0 "$stream_state"
    if [ "$NETWORK_QUIET_GROUP_STRUCTURAL_ERROR" -eq 1 ]; then
      last_reason="${NETWORK_QUIET_GROUP_REASON:-invalid-stream-warmup}"
      network_quiet_stats_stream_stop
      network_quiet_stream_restart_delay "$deadline_ms" || break
      continue
    fi

    if ! network_quiet_establish_generation_baseline \
      "$deadline_ms" "$generation_epoch" "$generations_file"; then
      last_reason="daemon-generation-unavailable"
      network_quiet_stats_stream_stop
      network_quiet_stream_restart_delay "$deadline_ms" || break
      continue
    fi
    baseline_finished_ms="$(monotonic_millis || printf '%s' "$deadline_ms")"
    if [ "$((10#$baseline_finished_ms))" -ge "$deadline_ms" ]; then
      last_reason="quiet-window-deadline-exhausted"
      network_quiet_stats_stream_stop
      break
    fi
    candidate_started_ms=0
    quiet_elapsed_ms=0
    qualifying_groups=0
    generation_stable="unknown"
    restart_stream=0

    while :; do
      if ! network_quiet_stats_stream_read_group \
        "$deadline_ms" "${#containers[@]}"; then
        last_reason="${NETWORK_QUIET_GROUP_REASON:-docker-stats-stream-unavailable}"
        restart_stream=1
        break
      fi
      sample_group=$((sample_group + 1))

      if [ "$NETWORK_QUIET_GROUP_STRUCTURAL_ERROR" -eq 1 ]; then
        candidate_started_ms=0
        quiet_elapsed_ms=0
        qualifying_groups=0
        last_reason="${NETWORK_QUIET_GROUP_REASON:-invalid-stream-group}"
        network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
          "$candidate" 0 stream-error
        restart_stream=1
        break
      fi

      # Groups produced while the generation baseline was being captured are
      # retained as evidence but cannot begin the candidate interval.
      if [ "$NETWORK_QUIET_GROUP_MIN_MS" -lt "$baseline_finished_ms" ]; then
        network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
          "$candidate" 0 prebaseline
        continue
      fi

      if [ -n "$NETWORK_QUIET_GROUP_REASON" ]; then
        candidate_started_ms="$(
          network_quiet_window_start_after_observation \
            "$candidate_started_ms" "$NETWORK_QUIET_GROUP_MAX_MS" reset
        )" || candidate_started_ms=0
        quiet_elapsed_ms=0
        qualifying_groups=0
        generation_stable="unknown"
        last_reason="$NETWORK_QUIET_GROUP_REASON"
        network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
          "$candidate" 0 busy
        log "  network-quiet/$label: busy ${NETWORK_QUIET_GROUP_REASON% }"
        continue
      fi

      if [ "$candidate_started_ms" -eq 0 ]; then
        candidate=$((candidate + 1))
        candidate_started_ms="$(
          network_quiet_window_start_after_observation \
            0 "$NETWORK_QUIET_GROUP_MAX_MS" quiet
        )" || {
          last_reason="invalid-stream-boundary"
          restart_stream=1
          break
        }
        quiet_elapsed_ms=0
        qualifying_groups=1
        last_reason=""
        network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
          "$candidate" 0 boundary
        log "  network-quiet/$label: candidate $candidate continuous CPU boundary established"
        continue
      fi

      quiet_elapsed_ms="$(
        network_quiet_elapsed_millis \
          "$candidate_started_ms" "$NETWORK_QUIET_GROUP_MIN_MS"
      )" || {
        last_reason="non-monotonic-stream-coverage"
        restart_stream=1
        break
      }
      qualifying_groups=$((qualifying_groups + 1))
      if ! network_quiet_duration_reached \
        "$candidate_started_ms" "$NETWORK_QUIET_GROUP_MIN_MS" \
        "$required_quiet_ms"; then
        network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
          "$candidate" "$quiet_elapsed_ms" qualifying
        log "  network-quiet/$label: continuously covered ${quiet_elapsed_ms}ms/${required_quiet_ms}ms across $qualifying_groups interval groups"
        continue
      fi

      network_quiet_write_stats_group_samples "$samples_file" "$sample_group" \
        "$candidate" "$quiet_elapsed_ms" duration-covered
      if ! network_quiet_verify_generation_baseline \
        "$deadline_ms" "$generation_epoch" "$generations_file"; then
        now_ms="$NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS"
        candidate_started_ms=0
        quiet_elapsed_ms=0
        qualifying_groups=0
        generation_stable=false
        if [ "$((10#$now_ms))" -ge "$deadline_ms" ]; then
          last_reason="quiet-window-deadline-exhausted"
        else
          last_reason="daemon-generation-changed"
        fi
        restart_stream=1
        break
      fi
      verification_target_ms="$NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS"
      generation_stable=true
      verification_reset=0

      # The stats producer never stopped while generation identities were read.
      # Consume all queued intervals and require every container to have a
      # quiet interval ending after verification completed before accepting.
      while :; do
        if ! network_quiet_stats_stream_read_group \
          "$deadline_ms" "${#containers[@]}"; then
          last_reason="${NETWORK_QUIET_GROUP_REASON:-docker-stats-stream-unavailable}"
          restart_stream=1
          break
        fi
        sample_group=$((sample_group + 1))
        if [ "$NETWORK_QUIET_GROUP_STRUCTURAL_ERROR" -eq 1 ]; then
          candidate_started_ms=0
          quiet_elapsed_ms=0
          qualifying_groups=0
          generation_stable="unknown"
          last_reason="${NETWORK_QUIET_GROUP_REASON:-invalid-stream-group}"
          network_quiet_write_stats_group_samples "$samples_file" \
            "$sample_group" "$candidate" 0 verification-stream-error
          restart_stream=1
          break
        fi
        if [ -n "$NETWORK_QUIET_GROUP_REASON" ]; then
          candidate_started_ms="$(
            network_quiet_window_start_after_observation \
              "$candidate_started_ms" "$NETWORK_QUIET_GROUP_MAX_MS" reset
          )" || candidate_started_ms=0
          quiet_elapsed_ms=0
          qualifying_groups=0
          generation_stable="unknown"
          last_reason="$NETWORK_QUIET_GROUP_REASON"
          network_quiet_write_stats_group_samples "$samples_file" \
            "$sample_group" "$candidate" 0 verification-busy
          log "  network-quiet/$label: CPU activity during final generation verification reset candidate $candidate: ${NETWORK_QUIET_GROUP_REASON% }"
          verification_reset=1
          break
        fi
        quiet_elapsed_ms="$(
          network_quiet_elapsed_millis \
            "$candidate_started_ms" "$NETWORK_QUIET_GROUP_MIN_MS"
        )" || {
          last_reason="non-monotonic-stream-coverage"
          restart_stream=1
          break
        }
        if network_quiet_coverage_reaches \
          "$NETWORK_QUIET_GROUP_MIN_MS" "$verification_target_ms"; then
          network_quiet_write_stats_group_samples "$samples_file" \
            "$sample_group" "$candidate" "$quiet_elapsed_ms" pass
          finished_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
          finished_monotonic_ms="$(
            monotonic_millis || printf '%s' "$NETWORK_QUIET_GROUP_MIN_MS"
          )"
          elapsed_ms=$((10#$finished_monotonic_ms - 10#$started_monotonic_ms))
          elapsed=$((elapsed_ms / 1000))
          network_quiet_stats_stream_stop
          write_network_quiet_summary "$summary_file" "$label" pass \
            "continuous CPU coverage through stable main/shadow generation verification" \
            "$started_at" "$finished_at" "$elapsed" "$elapsed_ms" \
            "$sample_group" "$candidate" "$qualifying_groups" \
            "$quiet_elapsed_ms" true "$samples_file" "$generations_file"
          log "  network-quiet/$label: continuously quiet through generation verification (${quiet_elapsed_ms}ms covered)"
          log "  network-quiet/$label: PASS summary -> $summary_file"
          return 0
        fi
        network_quiet_write_stats_group_samples "$samples_file" \
          "$sample_group" "$candidate" "$quiet_elapsed_ms" \
          verification-covering
      done

      if [ "$restart_stream" -eq 1 ]; then
        break
      fi
      if [ "$verification_reset" -eq 1 ]; then
        continue
      fi
    done

    network_quiet_stats_stream_stop
    now_ms="$(monotonic_millis || printf '%s' "$deadline_ms")"
    if [ "$((10#$now_ms))" -ge "$deadline_ms" ]; then
      last_reason="quiet-window-deadline-exhausted"
      break
    fi
    network_quiet_stream_restart_delay "$deadline_ms" || {
      last_reason="quiet-window-deadline-exhausted"
      break
    }
  done

  network_quiet_stats_stream_stop
  log "network-quiet/$label: cluster did not sustain the hard ${NETWORK_QUIET_DURATION_SECONDS}s continuously observed quiet window before the deadline"
  finished_at="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
  finished_monotonic_ms="$(monotonic_millis || printf '%s' "$deadline_ms")"
  elapsed_ms=$((10#$finished_monotonic_ms - 10#$started_monotonic_ms))
  elapsed=$((elapsed_ms / 1000))
  write_network_quiet_summary "$summary_file" "$label" fail \
    "${last_reason:-quiet-window-deadline-exhausted}" \
    "$started_at" "$finished_at" "$elapsed" "$elapsed_ms" \
    "$sample_group" "$candidate" "$qualifying_groups" "$quiet_elapsed_ms" \
    "$generation_stable" "$samples_file" "$generations_file"
  log "  network-quiet/$label: FAIL summary -> $summary_file"
  return 1
}

deadline_command_timeout() {
  local deadline="$1" maximum="$2" now remaining
  now="$(date +%s)"
  remaining=$(( deadline - now ))
  [ "$remaining" -gt 0 ] || return 1
  [ "$maximum" -gt 0 ] || return 1
  if [ "$remaining" -lt "$maximum" ]; then
    printf '%s\n' "$remaining"
  else
    printf '%s\n' "$maximum"
  fi
}

# GNU timeout accepts fractional seconds. Keep conversion in integer
# milliseconds so the formation hard gate never depends on floating-point
# shell arithmetic or locale-specific decimal formatting.
duration_seconds_to_millis() {
  local value="$1" whole fraction
  if [[ ! "$value" =~ ^([0-9]{1,9})([.]([0-9]{1,3}))?$ ]]; then
    return 1
  fi
  whole="${BASH_REMATCH[1]}"
  fraction="${BASH_REMATCH[3]-}000"
  fraction="${fraction:0:3}"
  printf '%s\n' "$((10#$whole * 1000 + 10#$fraction))"
}

duration_millis_to_seconds() {
  local value="$1" milliseconds
  if [[ ! "$value" =~ ^[0-9]{1,16}$ ]]; then
    return 1
  fi
  milliseconds=$((10#$value))
  printf '%d.%03d\n' "$((milliseconds / 1000))" "$((milliseconds % 1000))"
}

wall_clock_millis() {
  local now
  now="$(date +%s%3N)" || return 1
  [[ "$now" =~ ^[0-9]{13,16}$ ]] || return 1
  printf '%s\n' "$now"
}

# Linux uptime is monotonic across wall-clock corrections and is available on
# every host capable of running this Docker benchmark. Wall-clock timestamps
# remain in artifacts for correlation only; acceptance timing uses this clock.
monotonic_millis() {
  local uptime ignored
  IFS=' ' read -r uptime ignored < /proc/uptime || return 1
  duration_seconds_to_millis "$uptime"
}

bounded_deadline_timeout_millis() {
  local deadline="$1" now="$2" maximum="$3" remaining
  [[ "$deadline" =~ ^[0-9]{1,16}$ ]] || return 1
  [[ "$now" =~ ^[0-9]{1,16}$ ]] || return 1
  [[ "$maximum" =~ ^[0-9]{1,16}$ ]] || return 1
  remaining=$((10#$deadline - 10#$now))
  [ "$remaining" -gt 0 ] || return 1
  [ "$maximum" -gt 0 ] || return 1
  if [ "$remaining" -lt "$maximum" ]; then
    printf '%s\n' "$remaining"
  else
    printf '%s\n' "$maximum"
  fi
}

formation_deadline_command_timeout() {
  local deadline_ms="$1" maximum_seconds="$2" now_ms maximum_ms timeout_ms
  now_ms="$(wall_clock_millis)" || return 1
  maximum_ms="$(duration_seconds_to_millis "$maximum_seconds")" || return 1
  timeout_ms="$(
    bounded_deadline_timeout_millis "$deadline_ms" "$now_ms" "$maximum_ms"
  )" || return 1
  duration_millis_to_seconds "$timeout_ms"
}

# A stalled producer is the hostile cleanup case: the transform is blocked on
# input and the annotator is blocked on transformed output. Stopping the stream
# must synchronously reap all three stages before a benchmark can begin.
network_quiet_stream_ownership_self_test() {
  local saved_docker saved_lifecycle lifecycle_dir lifecycle_file
  local annotator_pid="" producer_pid="" transform_pid="" key value
  local producer_args transform_comm attempt rc=0
  saved_docker="$(declare -f docker 2>/dev/null || true)"
  saved_lifecycle="$NETWORK_QUIET_STATS_STREAM_TEST_LIFECYCLE_FILE"
  lifecycle_dir="$(
    mktemp -d "${TMPDIR:-/tmp}/sinomo-network-quiet-lifecycle.XXXXXX"
  )" || return 1
  lifecycle_file="$lifecycle_dir/worker.lifecycle"
  NETWORK_QUIET_STATS_STREAM_TEST_LIFECYCLE_FILE="$lifecycle_file"
  docker() {
    exec -a sinomo-quiet-stalled-producer sleep 300
  }

  if ! network_quiet_stats_stream_start stalled-container; then
    rc=1
  fi
  for attempt in $(seq 1 100); do
    if [ -s "$lifecycle_file" ] \
      && grep -Fxq 'children_started=true' "$lifecycle_file"; then
      break
    fi
    sleep 0.01
  done
  if [ ! -s "$lifecycle_file" ] \
    || ! grep -Fxq 'children_started=true' "$lifecycle_file"; then
    rc=1
  else
    while IFS='=' read -r key value; do
      case "$key" in
        annotator_pid) annotator_pid="$value" ;;
        producer_pid) producer_pid="$value" ;;
        transform_pid) transform_pid="$value" ;;
      esac
    done < "$lifecycle_file"
  fi
  [[ "$annotator_pid" =~ ^[0-9]+$ ]] || rc=1
  [[ "$producer_pid" =~ ^[0-9]+$ ]] || rc=1
  [[ "$transform_pid" =~ ^[0-9]+$ ]] || rc=1
  [ "$annotator_pid" = "$NETWORK_QUIET_STATS_STREAM_PID" ] || rc=1
  [ -n "$annotator_pid" ] && kill -0 "$annotator_pid" 2>/dev/null || rc=1
  [ -n "$producer_pid" ] && kill -0 "$producer_pid" 2>/dev/null || rc=1
  [ -n "$transform_pid" ] && kill -0 "$transform_pid" 2>/dev/null || rc=1
  producer_args="$(
    [ -n "$producer_pid" ] \
      && tr '\0' ' ' < "/proc/$producer_pid/cmdline" 2>/dev/null \
      || true
  )"
  transform_comm=""
  if [ -n "$transform_pid" ] && [ -r "/proc/$transform_pid/comm" ]; then
    IFS= read -r transform_comm < "/proc/$transform_pid/comm" || true
  fi
  case "$producer_args" in
    *sinomo-quiet-stalled-producer*) ;;
    *) rc=1 ;;
  esac
  [ "$transform_comm" = sed ] || rc=1

  network_quiet_stats_stream_stop
  if [ -n "$annotator_pid" ] && [ -e "/proc/$annotator_pid" ]; then
    rc=1
  fi
  [ -z "$producer_pid" ] || [ ! -e "/proc/$producer_pid" ] || rc=1
  [ -z "$transform_pid" ] || [ ! -e "/proc/$transform_pid" ] || rc=1
  [ -z "$NETWORK_QUIET_STATS_STREAM_PID" ] || rc=1
  [ -z "$NETWORK_QUIET_STATS_STREAM_FD" ] || rc=1
  grep -Fxq 'producer_waited=true' "$lifecycle_file" || rc=1
  grep -Fxq 'transform_waited=true' "$lifecycle_file" || rc=1
  grep -Fxq 'cleanup_complete=true' "$lifecycle_file" || rc=1

  NETWORK_QUIET_STATS_STREAM_TEST_LIFECYCLE_FILE="$saved_lifecycle"
  unset -f docker
  if [ -n "$saved_docker" ]; then
    eval "$saved_docker"
  fi
  rm -f -- "$lifecycle_file"
  rmdir "$lifecycle_dir" 2>/dev/null || rc=1
  return "$rc"
}

# Exercise the real quiet-gate state loop without Docker. Both regressions put
# an above-threshold cumulative interval where the old one-shot design had no
# CPU evidence: first between polls, then while final generations are scanned.
network_quiet_full_gate_mock_self_test() {
  local saved_monotonic saved_container saved_container_name saved_start
  local saved_stop saved_delay saved_baseline saved_verify saved_read saved_log
  local original_nodes="$NODES" original_expected="$EXPECTED_MAINS"
  local original_artifacts="$ARTIFACTS" mock_dir rc=0
  local mock_index=0 mock_verify_index=0
  local -a mock_times=() mock_cpu=() mock_verify_targets=()
  saved_monotonic="$(declare -f monotonic_millis)"
  saved_container="$(declare -f network_quiet_container_for_node_before_deadline)"
  saved_container_name="$(declare -f network_quiet_container_name_before_deadline)"
  saved_start="$(declare -f network_quiet_stats_stream_start)"
  saved_stop="$(declare -f network_quiet_stats_stream_stop)"
  saved_delay="$(declare -f network_quiet_stream_restart_delay)"
  saved_baseline="$(declare -f network_quiet_establish_generation_baseline)"
  saved_verify="$(declare -f network_quiet_verify_generation_baseline)"
  saved_read="$(declare -f network_quiet_stats_stream_read_group)"
  saved_log="$(declare -f log)"
  mock_dir="$(mktemp -d "${TMPDIR:-/tmp}/sinomo-network-quiet-gate.XXXXXX")" \
    || return 1

  NODES="node1 node2"
  EXPECTED_MAINS=2
  log() { :; }
  monotonic_millis() { printf '%s\n' 100; }
  network_quiet_container_for_node_before_deadline() {
    printf 'id-%s\n' "$1"
  }
  network_quiet_container_name_before_deadline() {
    printf 'mock-%s\n' "${1#id-}"
  }
  network_quiet_stats_stream_start() { :; }
  network_quiet_stats_stream_stop() { :; }
  # Any unexpected restart terminates the mock instead of hiding a failure in
  # an unbounded synthetic retry loop.
  network_quiet_stream_restart_delay() { return 1; }
  network_quiet_establish_generation_baseline() {
    generation_baseline[node1]="id-node1|10 100 11 101"
    generation_baseline[node2]="id-node2|10 100 11 101"
  }
  network_quiet_verify_generation_baseline() {
    NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS="${mock_verify_targets[$mock_verify_index]}"
    mock_verify_index=$((mock_verify_index + 1))
  }
  network_quiet_stats_stream_read_group() {
    local at cpu node
    if [ "$mock_index" -ge "${#mock_times[@]}" ]; then
      NETWORK_QUIET_GROUP_REASON="mock-sequence-exhausted"
      NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=1
      return 1
    fi
    at="${mock_times[$mock_index]}"
    cpu="${mock_cpu[$mock_index]}"
    mock_index=$((mock_index + 1))
    NETWORK_QUIET_GROUP_MIN_MS="$at"
    NETWORK_QUIET_GROUP_MAX_MS="$at"
    NETWORK_QUIET_GROUP_REASON=""
    NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=0
    NETWORK_QUIET_GROUP_CPU=()
    NETWORK_QUIET_GROUP_CPU_STATUS=()
    NETWORK_QUIET_GROUP_NAME=()
    NETWORK_QUIET_GROUP_OBSERVED_MS=()
    for node in $NODES; do
      NETWORK_QUIET_GROUP_CPU[$node]="$cpu"
      NETWORK_QUIET_GROUP_NAME[$node]="mock-$node"
      NETWORK_QUIET_GROUP_OBSERVED_MS[$node]="$at"
      if [ "$cpu" = 20.00% ]; then
        NETWORK_QUIET_GROUP_CPU_STATUS[$node]=above
        NETWORK_QUIET_GROUP_REASON="mock-$node=$cpu"
      else
        NETWORK_QUIET_GROUP_CPU_STATUS[$node]=within
      fi
    done
  }

  network_quiet_run_mock_case() {
    local scenario="$1" summary samples
    mock_index=0
    mock_verify_index=0
    ARTIFACTS="$mock_dir/$scenario"
    mkdir -p "$ARTIFACTS" || return 1
    case "$scenario" in
      former-sleep-gap)
        mock_times=(1000 2000 12000 17000 18000 48000 50000)
        mock_cpu=(1.00% 1.00% 1.00% 20.00% 1.00% 1.00% 1.00%)
        mock_verify_targets=(49000)
        ;;
      verification-busy)
        mock_times=(1000 2000 32000 34000 35000 65000 67000)
        mock_cpu=(1.00% 1.00% 1.00% 20.00% 1.00% 1.00% 1.00%)
        mock_verify_targets=(33000 66000)
        ;;
      *) return 1 ;;
    esac
    wait_for_network_quiet "$scenario" || return 1
    summary="$ARTIFACTS/network-quiet-$scenario.summary.txt"
    samples="$ARTIFACTS/network-quiet-$scenario.samples.tsv"
    grep -Fxq 'status=pass' "$summary" || return 1
    grep -Fxq 'final_candidate=2' "$summary" || return 1
    case "$scenario" in
      former-sleep-gap)
        awk -F '\t' \
          '$15 == "busy" && $12 == "20.00%" { found = 1 } END { exit !found }' \
          "$samples"
        ;;
      verification-busy)
        awk -F '\t' \
          '$15 == "verification-busy" && $12 == "20.00%" { found = 1 } END { exit !found }' \
          "$samples"
        ;;
    esac
  }

  network_quiet_run_mock_case former-sleep-gap || rc=1
  network_quiet_run_mock_case verification-busy || rc=1

  eval "$saved_monotonic"
  eval "$saved_container"
  eval "$saved_container_name"
  eval "$saved_start"
  eval "$saved_stop"
  eval "$saved_delay"
  eval "$saved_baseline"
  eval "$saved_verify"
  eval "$saved_read"
  eval "$saved_log"
  unset -f network_quiet_run_mock_case
  NODES="$original_nodes"
  EXPECTED_MAINS="$original_expected"
  ARTIFACTS="$original_artifacts"
  rm -rf -- "$mock_dir"
  return "$rc"
}

network_quiet_helpers_self_test() {
  local actual summary fixture snapshot monotonic_now required_ms rc=0
  [ "$NETWORK_QUIET_DURATION_SECONDS" = 30 ] || rc=1
  required_ms="$(duration_seconds_to_millis "$NETWORK_QUIET_DURATION_SECONDS")" \
    || rc=1
  [ "$required_ms" = 30000 ] || rc=1
  actual="$(network_quiet_cpu_sample_status 9.99% 10)"
  [ "$actual" = within ] || rc=1
  actual="$(network_quiet_cpu_sample_status 10.00% 10)"
  [ "$actual" = within ] || rc=1
  actual="$(network_quiet_cpu_sample_status 10.01% 10)"
  [ "$actual" = above ] || rc=1
  actual="$(network_quiet_cpu_sample_status unavailable 10)"
  [ "$actual" = invalid ] || rc=1
  network_quiet_generation_matches \
    $'container-a|10\t100\t11\t101' $'container-a|10\t100\t11\t101' || rc=1
  if network_quiet_generation_matches \
    $'container-a|10\t100\t11\t101' $'container-a|12\t102\t11\t101'; then
    rc=1
  fi

  # Slow observations do not redefine the acceptance window. The first quiet
  # instant remains authoritative, 29.999 seconds cannot pass, and 30.000
  # seconds passes regardless of how few observations fit in that duration.
  actual="$(network_quiet_window_start_after_observation 0 1000 quiet)" || rc=1
  [ "$actual" = 1000 ] || rc=1
  actual="$(
    network_quiet_window_start_after_observation "$actual" 13250 quiet
  )" || rc=1
  [ "$actual" = 1000 ] || rc=1
  if network_quiet_duration_reached "$actual" 30999 "$required_ms"; then
    rc=1
  fi
  network_quiet_duration_reached "$actual" 31000 "$required_ms" || rc=1
  network_quiet_duration_reached "$actual" 93000 "$required_ms" || rc=1
  if network_quiet_coverage_reaches 31999 32000; then
    rc=1
  fi
  network_quiet_coverage_reaches 32000 32000 || rc=1

  # Busy CPU, a sampling error, and a generation transition all use the same
  # fail-closed reset transition. A later quiet observation starts a new full
  # 30-second window rather than resuming accumulated time.
  actual="$(
    network_quiet_window_start_after_observation "$actual" 32000 reset
  )" || rc=1
  [ "$actual" = 0 ] || rc=1
  actual="$(network_quiet_window_start_after_observation 1000 33000 reset)" \
    || rc=1
  [ "$actual" = 0 ] || rc=1
  actual="$(network_quiet_window_start_after_observation 1000 34000 reset)" \
    || rc=1
  [ "$actual" = 0 ] || rc=1
  actual="$(network_quiet_window_start_after_observation 0 50000 quiet)" || rc=1
  if network_quiet_duration_reached "$actual" 79999 "$required_ms"; then
    rc=1
  fi
  network_quiet_duration_reached "$actual" 80000 "$required_ms" || rc=1

  # A streamed busy interval that arrives during what used to be the explicit
  # sleep gap resets the candidate, even if quiet groups on either side would
  # otherwise span 30 seconds.
  actual="$(network_quiet_window_start_after_observation 1000 16000 reset)" \
    || rc=1
  [ "$actual" = 0 ] || rc=1
  actual="$(network_quiet_window_start_after_observation "$actual" 17000 quiet)" \
    || rc=1
  if network_quiet_duration_reached "$actual" 46999 "$required_ms"; then
    rc=1
  fi

  # Final generation verification is inside the CPU coverage interval. A
  # quiet group ending before verification is insufficient, and activity in
  # the first group covering through verification resets the candidate.
  if network_quiet_coverage_reaches 51999 52000; then
    rc=1
  fi
  actual="$(network_quiet_window_start_after_observation 17000 53000 reset)" \
    || rc=1
  [ "$actual" = 0 ] || rc=1
  if network_quiet_window_start_after_observation 50000 49999 quiet >/dev/null; then
    rc=1
  fi
  monotonic_now="$(monotonic_millis)" || rc=1
  [[ "$monotonic_now" =~ ^[0-9]{1,16}$ ]] || rc=1

  fixture="$(mktemp -d "${TMPDIR:-/tmp}/sinomo-network-quiet-proc.XXXXXX")" \
    || return 1
  mkdir "$fixture/1" "$fixture/28" "$fixture/99" || return 1
  printf '%s\n' sinomo > "$fixture/1/comm"
  printf '/usr/local/bin/sinomo\0daemon\0shadow\0run\0' > "$fixture/1/cmdline"
  printf '%s\n' '1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 101' \
    > "$fixture/1/stat"
  printf '%s\n' sinomo > "$fixture/28/comm"
  printf '/usr/local/bin/sinomo\0daemon\0run\0' > "$fixture/28/cmdline"
  printf '%s\n' '1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 202' \
    > "$fixture/28/stat"
  printf '%s\n' sh > "$fixture/99/comm"
  printf 'sh\0-c\0case daemon shadow run and daemon run\0' > "$fixture/99/cmdline"
  printf '%s\n' '1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 303' \
    > "$fixture/99/stat"
  snapshot="$(
    sh -c "$NETWORK_QUIET_DAEMON_GENERATION_PROGRAM" sh "$fixture"
  )" || rc=1
  [ "$snapshot" = $'28\t202\t1\t101' ] || rc=1
  rm -f -- \
    "$fixture/1/comm" "$fixture/1/cmdline" "$fixture/1/stat" \
    "$fixture/28/comm" "$fixture/28/cmdline" "$fixture/28/stat" \
    "$fixture/99/comm" "$fixture/99/cmdline" "$fixture/99/stat"
  rmdir "$fixture/1" "$fixture/28" "$fixture/99" "$fixture"
  summary="$(mktemp "${TMPDIR:-/tmp}/sinomo-network-quiet-summary.XXXXXX")" || return 1
  write_network_quiet_summary "$summary" self-test pass complete \
    2026-01-01T00:00:00.000Z 2026-01-01T00:00:30.000Z 30 30000 \
    11 1 11 30000 true /tmp/samples.tsv /tmp/generations.tsv
  grep -Fxq 'protocol=sinomo.test.network-quiet.v2' "$summary" || rc=1
  grep -Fxq 'status=pass' "$summary" || rc=1
  grep -Fxq 'cpu_authority=docker-container-aggregate' "$summary" || rc=1
  grep -Fxq 'required_quiet_duration_seconds=30' "$summary" || rc=1
  grep -Fxq 'required_quiet_duration_milliseconds=30000' "$summary" || rc=1
  grep -Fxq 'cpu_sampling=docker-stats-continuous-stream' "$summary" || rc=1
  grep -Fxq 'stream_stall_reset_seconds=5' "$summary" || rc=1
  grep -Fxq 'final_qualifying_sample_groups=11' "$summary" || rc=1
  grep -Fxq 'final_quiet_elapsed_milliseconds=30000' "$summary" || rc=1
  grep -Fxq 'daemon_generations_stable=true' "$summary" || rc=1
  grep -Fxq 'samples_file=samples.tsv' "$summary" || rc=1
  rm -f -- "$summary"
  network_quiet_stream_ownership_self_test || rc=1
  network_quiet_full_gate_mock_self_test || rc=1
  return "$rc"
}

timing_helpers_self_test() {
  local actual
  [ "$FORMATION_WAIT" = 60 ] || return 1
  [ "$FORMATION_POLL_MILLIS" = 100 ] || return 1
  [ "$NETWORK_QUIET_DURATION_SECONDS" = 30 ] || return 1
  [ "$NETWORK_QUIET_MAX_WAIT_SECONDS" = 120 ] || return 1
  [ "$NETWORK_QUIET_STREAM_STALL_SECONDS" = 5 ] || return 1
  actual="$(duration_seconds_to_millis 5)" && [ "$actual" = 5000 ] || return 1
  actual="$(duration_seconds_to_millis 0.125)" && [ "$actual" = 125 ] || return 1
  actual="$(duration_millis_to_seconds 5000)" && [ "$actual" = 5.000 ] || return 1
  actual="$(bounded_deadline_timeout_millis 6000 1000 10000)" \
    && [ "$actual" = 5000 ] || return 1
  actual="$(bounded_deadline_timeout_millis 6000 1000 1250)" \
    && [ "$actual" = 1250 ] || return 1
  actual="$(bounded_deadline_timeout_millis 6000 5950 "$FORMATION_POLL_MILLIS")" \
    && [ "$actual" = 50 ] || return 1
  if bounded_deadline_timeout_millis 6000 6000 1000 >/dev/null; then
    return 1
  fi
  if duration_seconds_to_millis 0.0001 >/dev/null; then
    return 1
  fi
  network_quiet_helpers_self_test || return 1
  formation_health_sample_self_test || return 1
  log "timing helper self-test passed"
}

running_other_compose_projects() {
  docker ps \
    --filter label=com.docker.compose.project \
    --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null \
    | grep -v "^$PROJECT$" \
    | sort -u
}

require_no_other_compose_projects() {
  if [ "${SINOMO_TEST_ALLOW_OTHER_COMPOSE:-0}" = 1 ]; then
    return 0
  fi
  local others
  others="$(running_other_compose_projects || true)"
  if [ -z "$others" ]; then
    return 0
  fi
  log "refusing to start/reuse compose project $PROJECT while other compose projects are running:"
  echo "$others" | sed 's/^/[test-cluster]   /'
  log "stop those projects first, or set SINOMO_TEST_ALLOW_OTHER_COMPOSE=1 for an explicit override"
  return 1
}

doctor_connectivity_warnings() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}" output_file="${2:-}" output
  if ! output="$(compose_exec_timeout "$seconds" node1 sinomo cluster doctor 2>&1)"; then
    if [ -n "$output_file" ]; then
      printf '%s\n' "$output" > "$output_file"
    fi
    return 1
  fi
  if [ -n "$output_file" ]; then
    printf '%s\n' "$output" > "$output_file"
  fi
  printf '%s\n' "$output" | grep -E "endpoint-unreachable|node-offline" || true
}

cluster_health_attempt() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}"
  compose_exec_timeout "$seconds" node1 sinomo cluster health 2>/dev/null
}

cluster_health() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}"
  cluster_health_attempt "$seconds" || true
}

record_valid_formation_health() {
  local health="$1" health_file="$2" online
  [[ "$health" =~ [^[:space:]] ]] || return 1
  online="$(printf '%s\n' "$health" | grep -c "daemon-main.*online" || true)"
  printf '%s\n' "$health" > "$health_file" || return 1
  printf '%s\n' "${online:-0}"
}

formation_health_sample_self_test() {
  local sample_file prior sample actual online rc=0
  sample_file="$(mktemp "${TMPDIR:-/tmp}/sinomo-formation-health.XXXXXX")" || return 1
  prior="prior valid health"
  printf '%s\n' "$prior" > "$sample_file" || rc=1
  if record_valid_formation_health "" "$sample_file" >/dev/null; then
    rc=1
  fi
  actual="$(cat "$sample_file")" || rc=1
  [ "$actual" = "$prior" ] || rc=1

  sample=$'node\tprocess\thealth\nnode-a\tdaemon-main\tonline\nnode-b\tdaemon-main\tunknown\nnode-c\tdaemon-main\tonline'
  online="$(record_valid_formation_health "$sample" "$sample_file")" || rc=1
  [ "$online" = 2 ] || rc=1
  actual="$(cat "$sample_file")" || rc=1
  [ "$actual" = "$sample" ] || rc=1
  rm -f -- "$sample_file"
  return "$rc"
}

cluster_local_node_id() {
  local node="$1" seconds="${2:-$READINESS_COMMAND_TIMEOUT}" node_id
  node_id="$(
    compose_exec_timeout "$seconds" "$node" sinomo cluster status 2>/dev/null \
      | awk '/^local node:/ { print $3; exit }'
  )"
  [ -n "$node_id" ] || return 1
  printf '%s\n' "$node_id"
}

formation_failure_markers() {
  local seconds="${1:-$READINESS_COMMAND_TIMEOUT}"
  compose_exec_timeout "$seconds" node1 sh -c '
    for marker in /shared/*.failed /shared/*.join-failed; do
      [ -f "$marker" ] || continue
      printf "%s\n" "--- $marker"
      sed -n "1,80p" "$marker"
    done
  ' 2>/dev/null || true
}

report_cluster_readiness_timeout() {
  local label="$1" elapsed="$2" best_online="$3" last_online="$4"
  local health_file="$5" doctor_file="$6" health_attempt_file="$7"
  local health_header unresolved
  log "$label: cluster NOT ready after ${elapsed}s (best-online=$best_online, last-online=$last_online)"
  if [ -s "$health_file" ] && grep -q '[^[:space:]]' "$health_file"; then
    unresolved="$(awk '
      $0 ~ /daemon-main/ && $0 !~ /daemon-main.*online/ { print }
    ' "$health_file")"
    if [ -n "$unresolved" ]; then
      health_header="$(sed -n '1p' "$health_file")"
      log "$label: last unresolved daemon rows:"
      printf '%s\n%s\n' "$health_header" "$unresolved" | sed 's/^/[test-cluster]   /'
    fi
  else
    log "$label: no cluster health output was captured"
  fi
  log "$label: last valid health output: $health_file"
  log "$label: last health attempt: $health_attempt_file"
  log "$label: last doctor output: $doctor_file"
}

wait_for_cluster_ready() {
  local label="$1" started_ms deadline_ms now_ms elapsed_ms elapsed
  local health health_rc online warnings command_timeout markers poll_sleep_ms poll_sleep
  local label_slug health_file health_attempt_file health_attempts_file doctor_file
  local best_online=0 last_online=0 attempt=0 attempt_outcome
  local attempt_started_ms attempt_finished_ms attempt_elapsed_ms attempt_start_offset_ms
  label_slug="${label//[^a-zA-Z0-9._-]/_}"
  health_file="$ARTIFACTS/cluster-ready-$label_slug.last-health.txt"
  health_attempt_file="$ARTIFACTS/cluster-ready-$label_slug.last-health-attempt.txt"
  health_attempts_file="$ARTIFACTS/cluster-ready-$label_slug.attempts.tsv"
  doctor_file="$ARTIFACTS/cluster-ready-$label_slug.last-doctor.txt"
  : > "$health_file"
  if [ "$FORMATION_OBSERVE" = 1 ]; then
    printf '%s\n' $'attempt\tstarted_ms\tstart_offset_ms\tfinished_ms\telapsed_ms\tcommand_timeout_s\texit\toutcome\tonline\toutput_bytes' \
      > "$health_attempts_file"
  fi
  printf '%s\n' "cluster health attempt not run" > "$health_attempt_file"
  printf '%s\n' "doctor not run: health has not reported $EXPECTED_MAINS online main daemons" \
    > "$doctor_file"
  log "$label: waiting for $EXPECTED_MAINS clean main daemons (<=${FORMATION_WAIT}s)"
  if ! started_ms="$(wall_clock_millis)"; then
    log "$label: millisecond wall clock is unavailable"
    return 1
  fi
  deadline_ms=$((10#$started_ms + FORMATION_WAIT * 1000))
  while :; do
    if ! command_timeout="$(
      formation_deadline_command_timeout "$deadline_ms" "$READINESS_COMMAND_TIMEOUT"
    )"; then
      now_ms="$(wall_clock_millis)" || now_ms="$deadline_ms"
      elapsed_ms=$((10#$now_ms - 10#$started_ms))
      [ "$elapsed_ms" -ge 0 ] || elapsed_ms=0
      elapsed="$(duration_millis_to_seconds "$elapsed_ms")"
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" \
        "$health_file" "$doctor_file" "$health_attempt_file"
      return 1
    fi
    health_rc=0
    attempt=$((attempt + 1))
    attempt_outcome="completed"
    if [ "$FORMATION_OBSERVE" = 1 ]; then
      attempt_started_ms="$(wall_clock_millis)" || attempt_started_ms="$started_ms"
    fi
    if health="$(cluster_health_attempt "$command_timeout")"; then
      if online="$(record_valid_formation_health "$health" "$health_file")"; then
        {
          printf 'cluster health attempt succeeded (command-timeout=%ss)\n' "$command_timeout"
          printf '%s\n' "$health"
        } > "$health_attempt_file"
        last_online="${online:-0}"
        if [ "${online:-0}" -gt "$best_online" ]; then
          best_online="$online"
        fi
      else
        online=0
        attempt_outcome="empty"
        printf 'cluster health attempt returned no output (command-timeout=%ss)\n' \
          "$command_timeout" > "$health_attempt_file"
      fi
    else
      health_rc=$?
      online=0
      if [ "$health_rc" -eq 124 ]; then
        attempt_outcome="timed_out"
      else
        attempt_outcome="failed"
      fi
      {
        printf 'cluster health attempt failed or timed out (command-timeout=%ss, exit=%s)\n' \
          "$command_timeout" "$health_rc"
        if [[ "$health" =~ [^[:space:]] ]]; then
          printf '%s\n' "$health"
        fi
      } > "$health_attempt_file"
    fi
    if [ "$FORMATION_OBSERVE" = 1 ]; then
      attempt_finished_ms="$(wall_clock_millis)" || attempt_finished_ms="$deadline_ms"
      attempt_elapsed_ms=$((10#$attempt_finished_ms - 10#$attempt_started_ms))
      [ "$attempt_elapsed_ms" -ge 0 ] || attempt_elapsed_ms=0
      attempt_start_offset_ms=$((10#$attempt_started_ms - 10#$started_ms))
      [ "$attempt_start_offset_ms" -ge 0 ] || attempt_start_offset_ms=0
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$attempt" "$attempt_started_ms" "$attempt_start_offset_ms" \
        "$attempt_finished_ms" "$attempt_elapsed_ms" "$command_timeout" \
        "$health_rc" "$attempt_outcome" "${online:-0}" "${#health}" \
        >> "$health_attempts_file"
    fi
    if ! command_timeout="$(
      formation_deadline_command_timeout "$deadline_ms" "$READINESS_COMMAND_TIMEOUT"
    )"; then
      now_ms="$(wall_clock_millis)" || now_ms="$deadline_ms"
      elapsed_ms=$((10#$now_ms - 10#$started_ms))
      [ "$elapsed_ms" -ge 0 ] || elapsed_ms=0
      elapsed="$(duration_millis_to_seconds "$elapsed_ms")"
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" \
        "$health_file" "$doctor_file" "$health_attempt_file"
      return 1
    fi
    # Once every expected main is online, a terminal join-failure marker is
    # incompatible with the observed formation state. Avoid another Docker
    # exec on that success path so the hard deadline is spent on the stronger
    # live doctor check. Partial formation still fails fast on markers.
    if [ "${online:-0}" -lt "$EXPECTED_MAINS" ]; then
      markers="$(formation_failure_markers "$command_timeout")"
      if [ -n "$markers" ]; then
        log "$label: cluster formation reported a terminal failure"
        printf '%s\n' "$markers" | sed 's/^/[test-cluster]   /'
        return 1
      fi
    fi
    warnings=""
    if [ "${online:-0}" -ge "$EXPECTED_MAINS" ]; then
      # Online membership is necessary but not sufficient: recently restarted
      # peers can still expose stale advertised endpoints. Keep both the health
      # and doctor probes inside the same formation deadline so transient relay
      # convergence consumes the remaining budget instead of failing the gate.
      now_ms="$(wall_clock_millis)" || now_ms="$deadline_ms"
      elapsed_ms=$((10#$now_ms - 10#$started_ms))
      [ "$elapsed_ms" -ge 0 ] || elapsed_ms=0
      elapsed="$(duration_millis_to_seconds "$elapsed_ms")"
      if [ "$now_ms" -gt "$deadline_ms" ]; then
        report_cluster_readiness_timeout \
          "$label" "$elapsed" "$best_online" "$last_online" \
          "$health_file" "$doctor_file" "$health_attempt_file"
        return 1
      fi
      if ! warnings="$(
        doctor_connectivity_warnings "$command_timeout" "$doctor_file"
      )"; then
        warnings="cluster doctor failed or timed out"
      fi
      if [ -n "$warnings" ]; then
        log "$label: all mains online, waiting for doctor to become clean"
        printf '%s\n' "$warnings" | sed 's/^/[test-cluster]   /'
      else
        log "$label: cluster ready (${online} main daemons online in ${elapsed}s; doctor clean)"
        return 0
      fi
    fi
    now_ms="$(wall_clock_millis)" || now_ms="$deadline_ms"
    elapsed_ms=$((10#$now_ms - 10#$started_ms))
    [ "$elapsed_ms" -ge 0 ] || elapsed_ms=0
    elapsed="$(duration_millis_to_seconds "$elapsed_ms")"
    if [ "$now_ms" -ge "$deadline_ms" ]; then
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" \
        "$health_file" "$doctor_file" "$health_attempt_file"
      return 1
    fi
    poll_sleep_ms="$(
      bounded_deadline_timeout_millis \
        "$deadline_ms" "$now_ms" "$FORMATION_POLL_MILLIS"
    )" || {
      report_cluster_readiness_timeout \
        "$label" "$elapsed" "$best_online" "$last_online" \
        "$health_file" "$doctor_file" "$health_attempt_file"
      return 1
    }
    poll_sleep="$(duration_millis_to_seconds "$poll_sleep_ms")" || return 1
    sleep "$poll_sleep"
  done
}

# A node is DFS-ready when its CLI can open the SIFS backend and pass the
# invariants probe: this needs the relay route to the voter (cluster KV access)
# and bootstraps the node's SIFS namespace, both of which can lag formation by
# a few seconds on the deep-segment nodes.
wait_for_dfs_ready() {
  local deadline node pending still probe_log probe_err command_timeout now
  log "dfs-ready: waiting for every node to open SIFS and pass invariants (<=${DFS_READY_WAIT}s)"
  deadline=$(( $(date +%s) + DFS_READY_WAIT ))
  pending="$NODES"
  while :; do
    still=""
    for node in $pending; do
      probe_log="$ARTIFACTS/dfs-ready-$node.last.json"
      probe_err="$ARTIFACTS/dfs-ready-$node.last.stderr.log"
      if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
        && compose_exec_timeout "$command_timeout" "$node" sinomo test run cluster.dfs.invariants \
          --require-cluster --json --param timeout-seconds=5 \
          >"$probe_log" 2>"$probe_err"; then
        now="$(date +%s)"
        if [ "$now" -le "$deadline" ]; then
          log "  dfs-ready: $node"
        else
          still="$still $node"
        fi
      else
        still="$still $node"
      fi
    done
    pending="${still# }"
    now="$(date +%s)"
    if [ -z "$pending" ] && [ "$now" -le "$deadline" ]; then
      return 0
    fi
    if [ "$now" -ge "$deadline" ]; then
      log "dfs-ready: nodes never became ready: $pending"
      for node in $pending; do
        log "  $node last probe output:"
        sed 's/^/      /' "$ARTIFACTS/dfs-ready-$node.last.json" | tail -4
        sed 's/^/      /' "$ARTIFACTS/dfs-ready-$node.last.stderr.log" | tail -4
      done
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

dfs_converged_epoch_signature() {
  jq -er '
    if .status != "passed" then
      error("shard-layout probe did not pass")
    elif (.diagnostics.pending_handoff_keys | type) != "array" then
      error("missing authoritative pending handoff diagnostics")
    elif (.diagnostics.pending_handoff_keys | length) != 0 then
      error("metadata membership handoff is pending")
    else
      .diagnostics.shards
    end
    | if type != "array" or length != 3 then
        error("expected exactly three shard diagnostics")
      elif ([.[].lane] | sort) != ["root", "services", "users"] then
        error("expected root, services, and users lanes")
      elif any(.[];
        ((.replica_node_ids | type) != "array")
          or ((.replica_node_ids | length) != 5)
      ) then
        error("expected five replicas on every shard")
      elif ([.[].primary_node_id] | unique | length) != 3 then
        error("expected three distinct shard primaries")
      elif any(.[];
        ((.file_system_id | type) != "string")
          or ((.shard_id | type) != "string")
          or ((.epoch | type) != "number")
      ) then
        error("invalid shard identity or epoch diagnostic")
      else
        sort_by(.shard_id)
        | map("\(.file_system_id)/\(.shard_id)=\(.epoch)")
        | join(",")
      end
  ' "$1"
}

# A clean operational snapshot is stronger than an active shard layout. The
# retiring layout remains active while a membership handoff is pending, so
# require exact layout, no current pending handoff keys, and zero
# configured-replica lag on every local primary. A write fence is process-local
# backend state, not durable observation evidence: the routed-mutation gate
# below checks the actual long-lived daemon lanes. Bracket the invariant sweep
# with layout reads and require two unchanged epoch signatures so a transition
# completing during either observation cannot pass.
wait_for_dfs_converged() {
  local deadline probe_log probe_err confirm_log confirm_err
  local command_timeout now consecutive node invariants_clean invariant_log invariant_err
  local epoch_signature confirm_epoch_signature previous_epoch_signature observation_clean
  if ! command -v jq >/dev/null 2>&1; then
    log "dfs-converged: jq is required to validate stable shard epochs"
    return 1
  fi
  log "dfs-converged: waiting for the fully distributed three-lane shard layout (<=${DFS_CONVERGENCE_WAIT}s)"
  deadline=$(( $(date +%s) + DFS_CONVERGENCE_WAIT ))
  probe_log="$ARTIFACTS/dfs-converged.last.json"
  probe_err="$ARTIFACTS/dfs-converged.last.stderr.log"
  confirm_log="$ARTIFACTS/dfs-converged.confirm.last.json"
  confirm_err="$ARTIFACTS/dfs-converged.confirm.last.stderr.log"
  consecutive=0
  previous_epoch_signature=""
  while :; do
    observation_clean=0
    if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
      && compose_exec_timeout "$command_timeout" node1 sinomo test run cluster.dfs.shard-layout \
        --require-cluster \
        --json \
        --param expected-replica-count=5 \
        --param expected-distinct-primary-count=3 \
        >"$probe_log" 2>"$probe_err" \
      && epoch_signature="$(dfs_converged_epoch_signature "$probe_log" 2>>"$probe_err")"; then
      invariants_clean=1
      for node in $NODES; do
        invariant_log="$ARTIFACTS/dfs-converged-$node.last.json"
        invariant_err="$ARTIFACTS/dfs-converged-$node.last.stderr.log"
        # Opening the observation shares this bounded RPC budget. A one-second
        # inner deadline can expire before inspecting a populated namespace.
        if ! command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
          || ! compose_exec_timeout "$command_timeout" "$node" \
            sinomo test run cluster.dfs.invariants \
              --require-cluster --json --param "timeout-seconds=$command_timeout" \
            >"$invariant_log" 2>"$invariant_err" \
          || ! jq -e '.status == "passed"' \
            "$invariant_log" >/dev/null 2>>"$invariant_err"; then
          invariants_clean=0
          break
        fi
      done
      if [ "$invariants_clean" = 1 ]; then
        if command_timeout="$(deadline_command_timeout "$deadline" "$READINESS_COMMAND_TIMEOUT")" \
          && compose_exec_timeout "$command_timeout" node1 sinomo test run cluster.dfs.shard-layout \
            --require-cluster \
            --json \
            --param expected-replica-count=5 \
            --param expected-distinct-primary-count=3 \
            >"$confirm_log" 2>"$confirm_err" \
          && confirm_epoch_signature="$(dfs_converged_epoch_signature "$confirm_log" 2>>"$confirm_err")" \
          && [ "$confirm_epoch_signature" = "$epoch_signature" ]; then
          observation_clean=1
        fi
      fi
    fi
    if [ "$observation_clean" = 1 ]; then
      if [ -n "$previous_epoch_signature" ] \
        && [ "$epoch_signature" = "$previous_epoch_signature" ]; then
        consecutive=$((consecutive + 1))
      else
        consecutive=1
      fi
      previous_epoch_signature="$epoch_signature"
    else
      consecutive=0
      previous_epoch_signature=""
    fi
    if [ "$consecutive" -ge 2 ]; then
      log "  dfs-converged: exact layout, zero handoffs, and zero replica lag across two stable epoch probes"
      return 0
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      log "dfs-converged: shard layout did not converge before the deadline"
      sed 's/^/      /' "$probe_log" | tail -12
      sed 's/^/      /' "$probe_err" | tail -12
      return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}

network_check() {
  local action="$1" directory="$2"
  shift 2
  local -a args=(--project "$PROJECT" --compose-file "$COMPOSE_FILE"
    --artifacts "$directory" --binary "$BINARY" --wait "$NETWORK_READY_WAIT"
    --timeout-ms "$NETWORK_TIMEOUT_MS" --concurrency "$NETWORK_READY_CONCURRENCY")
  if [ -n "$COMPOSE_OVERRIDE_FILE" ]; then args+=(--compose-file "$COMPOSE_OVERRIDE_FILE"); fi
  python3 "$NETWORK_CHECKS" "$action" "${args[@]}" "$@"
}

wait_for_network_ready() {
  network_check ready "$ARTIFACTS/network-ready" --requests "$NETWORK_READY_REQUESTS_PER_TARGET"
}

selected_network_cases() {
  local name rc prerequisites=0 delay
  for name in $NETWORK_CASES; do
    case "$name" in
      latency|payload|attachments|restart|restart-staggered)
        if [ "$prerequisites" = 0 ]; then
          wait_for_dfs_ready && wait_for_dfs_converged && dfs_daemon_mutation_lanes_ready && wait_for_dfs_converged || return 1
          prerequisites=1
        fi ;;
    esac
    rc=0
    case "$name" in
      ready) wait_for_network_ready || rc=$? ;;
      bridge) network_check bridge "$ARTIFACTS/bridge" || rc=$? ;;
      latency) network_realtime_latency_gate || rc=$? ;;
      payload) wait_for_network_quiet payload && network_payload_baseline || rc=$? ;;
      attachments) wait_for_network_quiet attachments && network_semantic_attachment_baseline || rc=$? ;;
      restart|restart-staggered)
        delay=0; [ "$name" != restart-staggered ] || delay=4
        SINOMO_TEST_NETWORK_CASES= SINOMO_TEST_COMPOSE_OVERRIDE="$COMPOSE_OVERRIDE_FILE" \
          python3 "$SINOMO_DOCKER_DIR/test-file-history.py" --case restart \
          --project "$PROJECT" --binary "$BINARY" --artifacts "$ARTIFACTS/$name" \
          --restart-cycles 3 --restart-stagger-seconds "$delay" || rc=$? ;;
    esac
    printf '%s\t%s\n' "$name" "$rc" >> "$ARTIFACTS/network-cases.tsv"
    if [ "$rc" != 0 ]; then FAILED_SCENARIOS="network-$name"; return "$rc"; fi
  done
}

PROBE_INDEX=0
run_probe() {
  local node="$1" id="$2"
  shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  local slug probe_log probe_err
  slug="$(printf '%03d' "$PROBE_INDEX")-$node-${id//[^a-zA-Z0-9._-]/_}"
  probe_log="$ARTIFACTS/probe-$slug.json"
  probe_err="$ARTIFACTS/probe-$slug.stderr.log"
  if compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" "$node" \
      sinomo test run "$id" --require-cluster --json "$@" \
      >"$probe_log" 2>"$probe_err"; then
    log "  ok: $node $id"
    return 0
  fi
  log "  FAILED: $node $id $*"
  sed 's/^/      /' "$probe_log"
  sed 's/^/      /' "$probe_err"
  return 1
}

FAILED_SCENARIOS=""
scenario() {
  local name="$1"
  shift
  echo
  log "--- scenario: $name ---"
  if "$@"; then
    log "--- PASS: $name ---"
  else
    log "--- FAIL: $name ---"
    FAILED_SCENARIOS="$FAILED_SCENARIOS $name"
  fi
}

invariants_sweep() {
  local rc=0 node
  for node in $NODES; do
    run_probe "$node" cluster.dfs.invariants --param timeout-seconds=60 || rc=1
  done
  return $rc
}

# Exercise one routed mutation on every metadata authority through the live
# daemon message service. Fresh CLI invariant probes can rebind their own
# backend handles and therefore cannot prove that a long-lived daemon lane is
# writable. Requiring zero internal retries makes a stale cached lane visible
# instead of hiding it behind the probe's bounded retry loop.
dfs_daemon_mutation_lanes_ready() {
  local lane path payload rc=0
  for lane in root services users; do
    case "$lane" in
      root) path="/cluster-tests/$RUN_ID/daemon-lane-root.txt" ;;
      services) path="/services/cluster-tests/$RUN_ID/daemon-lane-services.txt" ;;
      users) path="/users/cluster-tests/$RUN_ID/daemon-lane-users.txt" ;;
    esac
    payload="daemon mutation lane $lane $RUN_ID"
    run_probe node6 cluster.dfs.write \
      --param "path=$path" \
      --param "payload=$payload" \
      --param max-retries=0 \
      --param "timeout-seconds=$EXPECT_TIMEOUT" \
    && run_probe node5 cluster.dfs.expect-file \
      --param "path=$path" \
      --param "payload=$payload" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" \
    && run_probe node4 cluster.dfs.remove --param "path=$path" \
    || rc=1
  done
  return "$rc"
}

final_routed_status() {
  run_probe node1 cluster.dfs.routed-status \
    --param expected-targets=5 \
    --param max-remote-open-conflicts=0 \
    --param max-remote-pending-writes=0 \
    --param max-remote-pending-repairs=0 \
    --param "max-attempts=$ROUTED_STATUS_MAX_ATTEMPTS" \
    --param "max-elapsed-ms=$ROUTED_STATUS_MAX_ELAPSED_MS" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

PJDFSTEST_STARTED=0
PJDFSTEST_ARTIFACTS_COLLECTED=0
PJDFSTEST_COMPATIBILITY_STATUS="not_run"
PJDFSTEST_SENTINEL_STATUS="not_run"
PJDFSTEST_ZERO_BYTE_STATUS="not_run"
PJDFSTEST_ZERO_BYTE_PREFIX=""
PJDFSTEST_ZERO_BYTE_O_CREAT_PATH=""
PJDFSTEST_ZERO_BYTE_O_EXCL_PATH=""
PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID=""
PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID=""
PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID=""
PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH=""
PJDFSTEST_CONTAINER_ARTIFACTS="/tmp/sinomo-pjdfstest-$RUN_ID"
PJDFSTEST_HOST_ARTIFACTS="$ARTIFACTS/pjdfstest"

pjdfstest_result_value() {
  local result_file="$1" key="$2"
  awk -F= -v key="$key" '
    $1 == key {
      if (found) {
        exit 2
      }
      found = 1
      sub(/^[^=]*=/, "")
      value = $0
    }
    END {
      if (!found) {
        exit 1
      }
      print value
    }
  ' "$result_file"
}

collect_pjdfstest_artifacts() {
  [ "$PJDFSTEST_STARTED" = 1 ] || return 0
  [ "$PJDFSTEST_ARTIFACTS_COLLECTED" = 0 ] || return 0
  mkdir -p "$PJDFSTEST_HOST_ARTIFACTS"
  if ! compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" cp \
      "node5:$PJDFSTEST_CONTAINER_ARTIFACTS/." \
      "$PJDFSTEST_HOST_ARTIFACTS/"; then
    log "pjdfstest: failed to copy node5 artifacts from $PJDFSTEST_CONTAINER_ARTIFACTS"
    return 1
  fi
  PJDFSTEST_ARTIFACTS_COLLECTED=1
}

cleanup_pjdfstest_mount() {
  [ "$PJDFSTEST_STARTED" = 1 ] || return 0
  if ! compose_exec_timeout 30 node5 env \
      "SINOMO_PJDFSTEST_MOUNT_POINT=$PJDFSTEST_MOUNT_POINT" \
      /usr/local/bin/sinomo-pjdfstest-runner --cleanup-only; then
    log "pjdfstest: node5 mount cleanup failed for $PJDFSTEST_MOUNT_POINT"
    return 1
  fi
}

prepare_pjdfstest_zero_byte_placement() {
  local source_file="$PJDFSTEST_HOST_ARTIFACTS/zero-byte-shard-layout.json"
  local evidence_file="$PJDFSTEST_HOST_ARTIFACTS/zero-byte-placement.json"
  local selected mount_node_id primary_node_id shard_id epoch lane prefix

  mkdir -p "$PJDFSTEST_HOST_ARTIFACTS"
  if ! compose_exec_timeout "$PROBE_COMMAND_TIMEOUT" node5 \
      sinomo test run cluster.dfs.shard-layout \
        --require-cluster --json \
        --param expected-replica-count=5 \
        --param expected-distinct-primary-count=3 \
      >"$source_file" \
      2>"$PJDFSTEST_HOST_ARTIFACTS/zero-byte-shard-layout.stderr.log"; then
    log "pjdfstest: cannot resolve node5 metadata-shard layout for the mounted zero-byte probe"
    return 1
  fi

  selected="$(
    jq -er '
      if .status != "passed" then
        error("shard-layout probe did not pass")
      else
        .diagnostics as $diagnostics
        | $diagnostics.shards
        | map(select(.primary_node_id != $diagnostics.node))
        | first as $shard
        | if $shard == null then
            error("node5 has no remote metadata primary")
          elif ($shard.lane | IN("root", "services", "users") | not) then
            error("unsupported metadata shard lane")
          elif ($shard.replica_node_ids | index($diagnostics.node)) == null then
            error("node5 is not a metadata replica for the selected remote-primary shard")
          else
            [
              $diagnostics.node,
              $shard.primary_node_id,
              $shard.shard_id,
              ($shard.epoch | tostring),
              $shard.lane
            ]
            | @tsv
          end
      end
    ' "$source_file"
  )" || {
    log "pjdfstest: node5 shard layout cannot prove a remote metadata-primary path"
    return 1
  }
  IFS=$'\t' read -r \
    mount_node_id primary_node_id shard_id epoch lane <<<"$selected"
  case "$lane" in
    root) prefix="/cluster-tests/pjdfstest-zero-byte" ;;
    services) prefix="/services/cluster-tests/pjdfstest-zero-byte" ;;
    users) prefix="/users/cluster-tests/pjdfstest-zero-byte" ;;
    *)
      log "pjdfstest: unsupported zero-byte metadata lane: $lane"
      return 1
      ;;
  esac
  if [ -z "$mount_node_id" ] \
    || [ -z "$primary_node_id" ] \
    || [ "$mount_node_id" = "$primary_node_id" ] \
    || [[ ! "$epoch" =~ ^[1-9][0-9]*$ ]]; then
    log "pjdfstest: invalid remote metadata-primary selection"
    return 1
  fi

  jq -e \
    --arg mount_node_id "$mount_node_id" \
    --arg primary_node_id "$primary_node_id" \
    --arg shard_id "$shard_id" \
    --argjson epoch "$epoch" \
    --arg lane "$lane" \
    --arg prefix "$prefix" \
    '{
      schema_version: 1,
      status: "verified",
      remote_metadata_primary: true,
      mount_node_is_metadata_replica: true,
      selection: {
        mount_node_id: $mount_node_id,
        metadata_primary_node_id: $primary_node_id,
        metadata_shard_id: $shard_id,
        metadata_shard_epoch: $epoch,
        lane: $lane,
        dfs_prefix: $prefix
      },
      source: .
    }' "$source_file" >"$evidence_file" || {
      log "pjdfstest: cannot write zero-byte placement evidence"
      return 1
    }

  PJDFSTEST_ZERO_BYTE_PREFIX="$prefix"
  PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID="$mount_node_id"
  PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID="$primary_node_id"
  PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID="$shard_id"
  PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH="$epoch"
}

run_pjdfstest_compatibility_probe() {
  local command_timeout runner_rc=0 result_file harness_status compatibility_status
  local result_run_id result_profile result_mode cleanup_status sentinel_status
  local result_schema result_patchset control_unresolved sifs_unresolved
  local zero_byte_status zero_byte_o_creat_path zero_byte_o_excl_path
  local zero_byte_mount_node_id zero_byte_primary_node_id zero_byte_shard_id
  local zero_byte_shard_epoch zero_byte_remote_primary
  command_timeout=$((10#$PJDFSTEST_TIMEOUT_SECONDS * 2 + 120))
  mkdir -p "$PJDFSTEST_HOST_ARTIFACTS"
  prepare_pjdfstest_zero_byte_placement || return 20
  PJDFSTEST_STARTED=1
  log "pjdfstest: running $PJDFSTEST_PROFILE profile on native storage and the node5 SIFS mount"
  compose_exec_timeout "$command_timeout" node5 env \
    "SINOMO_PJDFSTEST_RUN_ID=$RUN_ID" \
    "SINOMO_PJDFSTEST_PROFILE=$PJDFSTEST_PROFILE" \
    "SINOMO_PJDFSTEST_PATCHSET=$PJDFSTEST_PATCHSET" \
    "SINOMO_PJDFSTEST_STRICT=$PJDFSTEST_STRICT" \
    "SINOMO_PJDFSTEST_TIMEOUT_SECONDS=$PJDFSTEST_TIMEOUT_SECONDS" \
    "SINOMO_PJDFSTEST_ARTIFACT_DIR=$PJDFSTEST_CONTAINER_ARTIFACTS" \
    "SINOMO_PJDFSTEST_NATIVE_ROOT=/pjdfstest-control" \
    "SINOMO_PJDFSTEST_MOUNT_POINT=$PJDFSTEST_MOUNT_POINT" \
    "SINOMO_PJDFSTEST_ZERO_BYTE_PREFIX=$PJDFSTEST_ZERO_BYTE_PREFIX" \
    "SINOMO_PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID=$PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID" \
    "SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID=$PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID" \
    "SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID=$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID" \
    "SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH=$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH" \
    /usr/local/bin/sinomo-pjdfstest-runner \
    >"$PJDFSTEST_HOST_ARTIFACTS/runner.stdout.log" \
    2>"$PJDFSTEST_HOST_ARTIFACTS/runner.stderr.log" \
    || runner_rc=$?

  if ! collect_pjdfstest_artifacts; then
    cleanup_pjdfstest_mount || true
    return 20
  fi
  if ! cleanup_pjdfstest_mount; then
    return 20
  fi

  result_file="$PJDFSTEST_HOST_ARTIFACTS/result.env"
  if [ ! -f "$result_file" ]; then
    log "pjdfstest: runner produced no result.env (exit=$runner_rc)"
    return 20
  fi
  harness_status="$(pjdfstest_result_value "$result_file" harness_status)" || {
    log "pjdfstest: result.env has no unique harness_status"
    return 20
  }
  compatibility_status="$(pjdfstest_result_value "$result_file" compatibility_status)" || {
    log "pjdfstest: result.env has no unique compatibility_status"
    return 20
  }
  result_run_id="$(pjdfstest_result_value "$result_file" run_id)" || {
    log "pjdfstest: result.env has no unique run_id"
    return 20
  }
  result_profile="$(pjdfstest_result_value "$result_file" profile)" || {
    log "pjdfstest: result.env has no unique profile"
    return 20
  }
  result_mode="$(pjdfstest_result_value "$result_file" mode)" || {
    log "pjdfstest: result.env has no unique mode"
    return 20
  }
  cleanup_status="$(pjdfstest_result_value "$result_file" cleanup_status)" || {
    log "pjdfstest: result.env has no unique cleanup_status"
    return 20
  }
  sentinel_status="$(pjdfstest_result_value "$result_file" sentinel_status)" || {
    log "pjdfstest: result.env has no unique sentinel_status"
    return 20
  }
  result_schema="$(pjdfstest_result_value "$result_file" schema_version)" || {
    log "pjdfstest: result.env has no unique schema_version"
    return 20
  }
  result_patchset="$(pjdfstest_result_value "$result_file" pjdfstest_patchset)" || {
    log "pjdfstest: result.env has no unique pjdfstest_patchset"
    return 20
  }
  control_unresolved="$(
    pjdfstest_result_value "$result_file" control_assertions_unresolved_directives
  )" || {
    log "pjdfstest: result.env has no unique control unresolved-directive count"
    return 20
  }
  sifs_unresolved="$(
    pjdfstest_result_value "$result_file" sifs_assertions_unresolved_directives
  )" || {
    log "pjdfstest: result.env has no unique SIFS unresolved-directive count"
    return 20
  }
  zero_byte_status="$(
    pjdfstest_result_value "$result_file" zero_byte_routed_write_status
  )" || {
    log "pjdfstest: result.env has no unique zero-byte routed-write status"
    return 20
  }
  zero_byte_o_creat_path="$(
    pjdfstest_result_value "$result_file" zero_byte_o_creat_path
  )" || {
    log "pjdfstest: result.env has no unique zero-byte O_CREAT path"
    return 20
  }
  zero_byte_o_excl_path="$(
    pjdfstest_result_value "$result_file" zero_byte_o_excl_path
  )" || {
    log "pjdfstest: result.env has no unique zero-byte O_EXCL path"
    return 20
  }
  zero_byte_mount_node_id="$(
    pjdfstest_result_value "$result_file" zero_byte_mount_node_id
  )" || return 20
  zero_byte_primary_node_id="$(
    pjdfstest_result_value "$result_file" zero_byte_metadata_primary_node_id
  )" || return 20
  zero_byte_shard_id="$(
    pjdfstest_result_value "$result_file" zero_byte_metadata_shard_id
  )" || return 20
  zero_byte_shard_epoch="$(
    pjdfstest_result_value "$result_file" zero_byte_metadata_shard_epoch
  )" || return 20
  zero_byte_remote_primary="$(
    pjdfstest_result_value "$result_file" zero_byte_remote_metadata_primary
  )" || return 20
  PJDFSTEST_COMPATIBILITY_STATUS="$compatibility_status"
  PJDFSTEST_SENTINEL_STATUS="$sentinel_status"
  PJDFSTEST_ZERO_BYTE_STATUS="$zero_byte_status"
  PJDFSTEST_ZERO_BYTE_O_CREAT_PATH="$zero_byte_o_creat_path"
  PJDFSTEST_ZERO_BYTE_O_EXCL_PATH="$zero_byte_o_excl_path"

  if [ "$result_run_id" != "$RUN_ID" ] \
    || [ "$result_profile" != "$PJDFSTEST_PROFILE" ] \
    || [ "$result_mode" != both ] \
    || [ "$result_schema" != 2 ] \
    || [ "$result_patchset" != "$PJDFSTEST_PATCHSET" ]; then
    log "pjdfstest: result identity mismatch (run=$result_run_id profile=$result_profile mode=$result_mode schema=$result_schema patchset=$result_patchset)"
    return 20
  fi
  if [ "$control_unresolved" != 0 ] || [ "$sifs_unresolved" != 0 ]; then
    log "pjdfstest: unresolved platform directives remain (control=$control_unresolved sifs=$sifs_unresolved)"
    return 20
  fi
  if [ "$zero_byte_status" != "$sentinel_status" ] \
    || { [ "$zero_byte_status" != passed ] && [ "$zero_byte_status" != failed ]; } \
    || [ "$zero_byte_o_creat_path" != "$PJDFSTEST_ZERO_BYTE_PREFIX/$RUN_ID/o-creat-empty" ] \
    || [ "$zero_byte_o_excl_path" != "$PJDFSTEST_ZERO_BYTE_PREFIX/$RUN_ID/o-excl-empty" ] \
    || [ "$zero_byte_mount_node_id" != "$PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID" ] \
    || [ "$zero_byte_primary_node_id" != "$PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID" ] \
    || [ "$zero_byte_shard_id" != "$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID" ] \
    || [ "$zero_byte_shard_epoch" != "$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH" ] \
    || [ "$zero_byte_remote_primary" != 1 ]; then
    log "pjdfstest: zero-byte routed-write identity or remote-primary proof mismatch"
    return 20
  fi
  if [ "$cleanup_status" != passed ]; then
    log "pjdfstest: kernel/native cleanup status is $cleanup_status"
    return 20
  fi
  case "$sentinel_status" in
    passed|failed) ;;
    *)
      log "pjdfstest: unusable sentinel status $sentinel_status"
      return 20
      ;;
  esac
  if [ "$harness_status" != passed ]; then
    log "pjdfstest: harness failed (runner exit=$runner_rc, compatibility=$compatibility_status)"
    return 20
  fi
  case "$compatibility_status" in
    compatible)
      if [ "$runner_rc" -ne 0 ]; then
        log "pjdfstest: compatible result had unexpected runner exit $runner_rc"
        return 20
      fi
      log "pjdfstest: executed profile is compatible"
      return 0
      ;;
    incompatible|incomplete)
      if [ "$PJDFSTEST_STRICT" = 1 ]; then
        if [ "$runner_rc" -ne 10 ]; then
          log "pjdfstest: strict $compatibility_status result had unexpected runner exit $runner_rc"
          return 20
        fi
        log "pjdfstest: compatibility=$compatibility_status (strict gate)"
        return 10
      fi
      if [ "$runner_rc" -ne 0 ]; then
        log "pjdfstest: report-only $compatibility_status result had unexpected runner exit $runner_rc"
        return 20
      fi
      log "pjdfstest: compatibility=$compatibility_status (recorded, report-only)"
      return 0
      ;;
    not_run|*)
      log "pjdfstest: unusable compatibility status $compatibility_status (runner exit=$runner_rc)"
      return 20
      ;;
  esac
}

write_pjdfstest_zero_byte_postcheck() {
  local status="$1" pre_restart_visibility="$2" restart_status="$3"
  local post_restart_visibility="$4" cleanup_status="$5"
  jq -n \
    --arg status "$status" \
    --arg pre_restart_visibility "$pre_restart_visibility" \
    --arg restart_status "$restart_status" \
    --arg post_restart_visibility "$post_restart_visibility" \
    --arg cleanup_status "$cleanup_status" \
    --arg o_creat_path "$PJDFSTEST_ZERO_BYTE_O_CREAT_PATH" \
    --arg o_excl_path "$PJDFSTEST_ZERO_BYTE_O_EXCL_PATH" \
    --arg mount_node_id "$PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID" \
    --arg primary_node_id "$PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID" \
    --arg shard_id "$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID" \
    --argjson shard_epoch "$PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH" \
    '{
      schema_version: 1,
      status: $status,
      pre_restart_cross_node_visibility: $pre_restart_visibility,
      node5_restart: $restart_status,
      post_restart_cross_node_visibility: $post_restart_visibility,
      cleanup: $cleanup_status,
      paths: {
        o_creat: $o_creat_path,
        o_excl: $o_excl_path
      },
      placement: {
        mount_node_id: $mount_node_id,
        metadata_primary_node_id: $primary_node_id,
        metadata_shard_id: $shard_id,
        metadata_shard_epoch: $shard_epoch,
        mount_node_is_metadata_replica: true,
        remote_metadata_primary: ($mount_node_id != $primary_node_id)
      }
    }' >"$PJDFSTEST_HOST_ARTIFACTS/zero-byte-post-cluster.json"
}

pjdfstest_post_cluster_checks() {
  local sentinel_root="$PJDFSTEST_ZERO_BYTE_PREFIX"
  local sentinel_dir="$sentinel_root/$RUN_ID"
  local sentinel_path="$PJDFSTEST_ZERO_BYTE_O_CREAT_PATH"
  local o_excl_path="$PJDFSTEST_ZERO_BYTE_O_EXCL_PATH"
  local sentinel_payload=""
  local doctor_file="$PJDFSTEST_HOST_ARTIFACTS/post-cluster-doctor.txt"
  local health_file="$PJDFSTEST_HOST_ARTIFACTS/post-cluster-health.txt"
  local warnings online rc=0
  local pre_restart_visibility="not_run"
  local restart_status="not_run"
  local post_restart_visibility="not_run"
  local zero_byte_cleanup="not_run"
  local zero_byte_postcheck_status="blocked"

  if [ "$PJDFSTEST_SENTINEL_STATUS" = passed ]; then
    pre_restart_visibility="passed"
    run_probe node1 cluster.dfs.expect-file \
      --param "path=$sentinel_path" \
      --param "payload=$sentinel_payload" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" || {
        pre_restart_visibility="failed"
        rc=1
      }
    run_probe node6 cluster.dfs.expect-file \
      --param "path=$o_excl_path" \
      --param "payload=$sentinel_payload" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" || {
        pre_restart_visibility="failed"
        rc=1
      }

    log "pjdfstest: restarting node5 for mounted zero-byte durability"
    if ! compose restart node5 >/dev/null 2>&1; then
      log "pjdfstest: node5 restart failed"
      restart_status="failed"
      rc=1
    elif ! wait_for_cluster_ready "pjdfstest-zero-byte-restart" \
      || ! wait_for_dfs_ready \
      || ! wait_for_dfs_converged; then
      restart_status="failed"
      rc=1
    else
      restart_status="passed"
      post_restart_visibility="passed"
      run_probe node5 cluster.dfs.expect-file \
        --param "path=$sentinel_path" \
        --param "payload=$sentinel_payload" \
        --param "timeout-seconds=$EXPECT_TIMEOUT" || {
          post_restart_visibility="failed"
          rc=1
        }
      run_probe node2 cluster.dfs.expect-file \
        --param "path=$o_excl_path" \
        --param "payload=$sentinel_payload" \
        --param "timeout-seconds=$EXPECT_TIMEOUT" || {
          post_restart_visibility="failed"
          rc=1
        }
    fi

    zero_byte_cleanup="passed"
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_path" || {
      zero_byte_cleanup="failed"
      rc=1
    }
    run_probe node4 cluster.dfs.remove --param "path=$o_excl_path" || {
      zero_byte_cleanup="failed"
      rc=1
    }
    run_probe node2 cluster.dfs.expect-absent \
      --param "path=$sentinel_path" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" || {
        zero_byte_cleanup="failed"
        rc=1
      }
    run_probe node6 cluster.dfs.expect-absent \
      --param "path=$o_excl_path" \
      --param "timeout-seconds=$EXPECT_TIMEOUT" || {
        zero_byte_cleanup="failed"
        rc=1
      }
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_dir" || {
      zero_byte_cleanup="failed"
      rc=1
    }
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_root" || {
      zero_byte_cleanup="failed"
      rc=1
    }
    if [ "$pre_restart_visibility" = passed ] \
      && [ "$restart_status" = passed ] \
      && [ "$post_restart_visibility" = passed ] \
      && [ "$zero_byte_cleanup" = passed ]; then
      zero_byte_postcheck_status="passed"
    else
      zero_byte_postcheck_status="failed"
    fi
  else
    log "pjdfstest: mounted zero-byte probe is inside the reported compatibility boundary"
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_path" || true
    run_probe node4 cluster.dfs.remove --param "path=$o_excl_path" || true
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_dir" || true
    run_probe node4 cluster.dfs.remove --param "path=$sentinel_root" || true
  fi
  if ! write_pjdfstest_zero_byte_postcheck \
      "$zero_byte_postcheck_status" \
      "$pre_restart_visibility" \
      "$restart_status" \
      "$post_restart_visibility" \
      "$zero_byte_cleanup"; then
    log "pjdfstest: cannot write zero-byte post-cluster evidence"
    rc=1
  fi
  wait_for_dfs_converged || rc=1
  invariants_sweep || rc=1

  if cluster_health_attempt "$READINESS_COMMAND_TIMEOUT" >"$health_file" 2>&1; then
    online="$(grep -c "daemon-main.*online" "$health_file" || true)"
    if [ "$online" -ne "$EXPECTED_MAINS" ]; then
      log "pjdfstest: post-suite health has $online/$EXPECTED_MAINS main daemons online"
      rc=1
    fi
  else
    log "pjdfstest: post-suite cluster health failed"
    rc=1
  fi
  warnings=""
  if ! warnings="$(
    doctor_connectivity_warnings "$READINESS_COMMAND_TIMEOUT" "$doctor_file"
  )"; then
    log "pjdfstest: post-suite cluster doctor failed"
    rc=1
  elif [ -n "$warnings" ]; then
    log "pjdfstest: post-suite cluster doctor reported connectivity warnings"
    printf '%s\n' "$warnings" | sed 's/^/[test-cluster]   /'
    rc=1
  fi
  final_routed_status || rc=1
  return "$rc"
}

pjdfstest_lane() {
  local compatibility_rc=0
  run_pjdfstest_compatibility_probe || compatibility_rc=$?
  if [ "$compatibility_rc" -ne 0 ] && [ "$compatibility_rc" -ne 10 ]; then
    return 1
  fi
  pjdfstest_post_cluster_checks || return 1
  [ "$compatibility_rc" -eq 0 ]
}

dfs_protocol_codec_baseline() {
  run_probe node1 cluster.dfs.protocol-codec-baseline \
    --param "bytes=$DFS_PROTOCOL_CODEC_BYTES" \
    --param "iterations=$DFS_PROTOCOL_CODEC_ITERATIONS" \
    --param "min-encode-bytes-per-second=$DFS_PROTOCOL_CODEC_MIN_ENCODE_BYTES_PER_SEC" \
    --param "min-decode-bytes-per-second=$DFS_PROTOCOL_CODEC_MIN_DECODE_BYTES_PER_SEC"
}

network_baseline() {
  local rc=0 node
  for node in $NODES; do
    run_probe "$node" cluster.network.ping-benchmark \
      --param "requests-per-target=$NETWORK_REQUESTS_PER_TARGET" \
      --param "warmup-requests-per-target=$NETWORK_WARMUP_REQUESTS_PER_TARGET" \
      --param "concurrency=$NETWORK_CONCURRENCY" \
      --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
      --param "max-failures=$NETWORK_MAX_FAILURES" \
      --param "max-p50-ms=0" \
      --param "max-p95-ms=0" \
      --param "max-p50-us=$NETWORK_MAX_P50_US" \
      --param "max-p95-us=$NETWORK_MAX_P95_US" \
      --param "payload-bytes=$NETWORK_PAYLOAD_BYTES" \
      --param "response-payload-bytes=$NETWORK_RESPONSE_PAYLOAD_BYTES" \
      --param "min-payload-bytes-per-second=$NETWORK_MIN_PAYLOAD_BYTES_PER_SEC" \
      --param "min-requests-per-second=$NETWORK_MIN_RPS" || rc=1
  done
  return $rc
}

# HARD CORRECTNESS GATE. Exercise the ordinary stream-native object-RPC API at
# concurrency one only after routes and background work have stabilized. In
# this topology node4 reaches node3 directly over seg_ab, while node1 requires
# exactly one relay through node3. Direct and one-relay p50/p95 must be <=2ms.
# A failure means the implementation is not reusing the stabilized routed data
# path correctly; do not classify it as an acceptable performance inefficiency.
network_realtime_latency_gate() {
  local rc=0 direct_target_id forward_target_id
  assert_network_realtime_topology || return 1
  direct_target_id="$(cluster_local_node_id node3)" || {
    log "  FAILED: cannot resolve node3 cluster node id for realtime latency gate"
    return 1
  }
  forward_target_id="$(cluster_local_node_id node1)" || {
    log "  FAILED: cannot resolve node1 cluster node id for realtime latency gate"
    return 1
  }
  wait_for_network_quiet direct || return 1
  run_probe node4 cluster.network.ping-benchmark \
    --param "target-node-id=$direct_target_id" \
    --param "requests-per-target=$NETWORK_REALTIME_REQUESTS" \
    --param "warmup-requests-per-target=$NETWORK_REALTIME_WARMUP_REQUESTS" \
    --param concurrency=1 \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=0 \
    --param max-p95-ms=0 \
    --param "max-p50-us=$NETWORK_DIRECT_MAX_US" \
    --param "max-p95-us=$NETWORK_DIRECT_MAX_US" \
    --param min-requests-per-second=0 || rc=1
  wait_for_network_quiet one-forward || return 1
  run_probe node4 cluster.network.ping-benchmark \
    --param "target-node-id=$forward_target_id" \
    --param "requests-per-target=$NETWORK_REALTIME_REQUESTS" \
    --param "warmup-requests-per-target=$NETWORK_REALTIME_WARMUP_REQUESTS" \
    --param concurrency=1 \
    --param "timeout-ms=$NETWORK_TIMEOUT_MS" \
    --param max-failures=0 \
    --param max-p50-ms=0 \
    --param max-p95-ms=0 \
    --param "max-p50-us=$NETWORK_FORWARD_MAX_US" \
    --param "max-p95-us=$NETWORK_FORWARD_MAX_US" \
    --param min-requests-per-second=0 || rc=1
  return $rc
}

network_payload_baseline() {
  local rc=0 node
  for node in $NODES; do
    run_probe "$node" cluster.network.ping-benchmark \
      --param "requests-per-target=$NETWORK_PAYLOAD_REQUESTS_PER_TARGET" \
      --param "warmup-requests-per-target=$NETWORK_PAYLOAD_WARMUP_REQUESTS_PER_TARGET" \
      --param "concurrency=$NETWORK_PAYLOAD_CONCURRENCY" \
      --param "timeout-ms=$NETWORK_PAYLOAD_TIMEOUT_MS" \
      --param "max-failures=0" \
      --param "max-p50-ms=0" \
      --param "max-p95-ms=0" \
      --param "max-p50-us=0" \
      --param "max-p95-us=0" \
      --param "payload-bytes=$NETWORK_PAYLOAD_REQUEST_BYTES" \
      --param "response-payload-bytes=$NETWORK_PAYLOAD_RESPONSE_BYTES" \
      --param "min-requests-per-second=0" \
      --param "min-payload-bytes-per-second=$NETWORK_PAYLOAD_MIN_BYTES_PER_SEC" || rc=1
  done
  return $rc
}

network_semantic_attachment_baseline() {
  local rc=0 node
  for node in $NODES; do
    run_probe "$node" cluster.network.semantic-attachment-benchmark \
      --param "attachment-requests-per-target=$NETWORK_ATTACHMENT_REQUESTS_PER_TARGET" \
      --param "attachment-warmup-requests-per-target=$NETWORK_ATTACHMENT_WARMUP_REQUESTS_PER_TARGET" \
      --param "attachment-concurrency=$NETWORK_ATTACHMENT_CONCURRENCY" \
      --param "attachment-timeout-ms=$NETWORK_ATTACHMENT_TIMEOUT_MS" \
      --param "attachment-max-failures=0" \
      --param "attachment-max-p50-ms=0" \
      --param "attachment-max-p95-ms=0" \
      --param "attachment-max-p50-us=0" \
      --param "attachment-max-p95-us=0" \
      --param "attachment-payload-bytes=$NETWORK_ATTACHMENT_REQUEST_BYTES" \
      --param "attachment-response-payload-bytes=$NETWORK_ATTACHMENT_RESPONSE_BYTES" \
      --param "attachment-min-requests-per-second=0" \
      --param "attachment-min-payload-bytes-per-second=$NETWORK_ATTACHMENT_MIN_BYTES_PER_SEC" || rc=1
  done
  return $rc
}

network_semantic_attachment_saturation_profile() {
  # Six nodes give the selected origin five remote targets. At the defaults,
  # 5 * 128 * (64 MiB request + 64 MiB response) is exactly 80 GiB of measured
  # duplex logical traffic: at least 40 GiB per direction for a multi-second
  # 100 GbE observation
  # without raising the route's 64 MiB per-direction bound.
  run_probe "$NETWORK_SATURATION_ORIGIN" cluster.network.semantic-attachment-benchmark \
    --param expected-targets=5 \
    --param "attachment-requests-per-target=$NETWORK_SATURATION_REQUESTS_PER_TARGET" \
    --param "attachment-warmup-requests-per-target=$NETWORK_SATURATION_WARMUP_REQUESTS_PER_TARGET" \
    --param "attachment-concurrency=$NETWORK_SATURATION_CONCURRENCY" \
    --param "attachment-timeout-ms=$NETWORK_SATURATION_TIMEOUT_MS" \
    --param attachment-max-failures=0 \
    --param attachment-max-p50-ms=0 \
    --param attachment-max-p95-ms=0 \
    --param attachment-max-p50-us=0 \
    --param attachment-max-p95-us=0 \
    --param "attachment-payload-bytes=$NETWORK_SATURATION_REQUEST_BYTES" \
    --param "attachment-response-payload-bytes=$NETWORK_SATURATION_RESPONSE_BYTES" \
    --param attachment-min-requests-per-second=0 \
    --param attachment-min-payload-bytes-per-second=0
}

cluster_kv_quorum_baseline() {
  local rc=0 node
  # node1 exercises the usual local-authority path; node5 exercises forwarding
  # across the deeper segmented route in the development topology.
  for node in node1 node5; do
    run_probe "$node" cluster.kv.quorum-commit-benchmark \
      --param "iterations=$CLUSTER_KV_ITERATIONS" \
      --param "warmup-iterations=$CLUSTER_KV_WARMUP_ITERATIONS" \
      --param ops-per-transaction=1 \
      --param "value-bytes=$CLUSTER_KV_VALUE_BYTES" \
      --param "timeout-ms=$CLUSTER_KV_TIMEOUT_MS" \
      --param "max-failures=$CLUSTER_KV_MAX_FAILURES" \
      --param "max-p50-ms=$CLUSTER_KV_MAX_P50_MS" \
      --param "max-p95-ms=$CLUSTER_KV_MAX_P95_MS" \
      --param "min-commits-per-second=$CLUSTER_KV_MIN_COMMITS_PER_SECOND" || rc=1
  done
  return $rc
}

cluster_kv_commit_benchmark_profile() {
  local node="$1" profile="$2" iterations="$3" ops_per_transaction="$4" value_bytes="$5"
  run_probe "$node" cluster.kv.quorum-commit-benchmark \
    --param "profile=$profile" \
    --param "iterations=$iterations" \
    --param "warmup-iterations=$CLUSTER_KV_WARMUP_ITERATIONS" \
    --param "ops-per-transaction=$ops_per_transaction" \
    --param "value-bytes=$value_bytes" \
    --param "timeout-ms=$CLUSTER_KV_TIMEOUT_MS" \
    --param "max-failures=$CLUSTER_KV_MAX_FAILURES" \
    --param "max-p50-ms=$CLUSTER_KV_MAX_P50_MS" \
    --param "max-p95-ms=$CLUSTER_KV_MAX_P95_MS" \
    --param "min-commits-per-second=$CLUSTER_KV_MIN_COMMITS_PER_SECOND"
}

cluster_kv_range_benchmark_profile() {
  local node="$1" consistency="$2"
  run_probe "$node" cluster.kv.range-read-benchmark \
    --param "profile=range-$consistency" \
    --param "consistency=$consistency" \
    --param "iterations=$CLUSTER_KV_RANGE_ITERATIONS" \
    --param "warmup-iterations=$CLUSTER_KV_WARMUP_ITERATIONS" \
    --param "entries=$CLUSTER_KV_RANGE_ENTRIES" \
    --param "value-bytes=$CLUSTER_KV_VALUE_BYTES" \
    --param "timeout-ms=$CLUSTER_KV_TIMEOUT_MS" \
    --param "max-failures=$CLUSTER_KV_MAX_FAILURES" \
    --param "max-p50-ms=$CLUSTER_KV_MAX_P50_MS" \
    --param "max-p95-ms=$CLUSTER_KV_MAX_P95_MS" \
    --param "min-reads-per-second=$CLUSTER_KV_MIN_READS_PER_SECOND"
}

cluster_kv_benchmark_suite() {
  local rc=0 node
  for node in $CLUSTER_KV_SUITE_NODES; do
    cluster_kv_commit_benchmark_profile \
      "$node" single-small "$CLUSTER_KV_ITERATIONS" 1 "$CLUSTER_KV_VALUE_BYTES" || rc=1
    cluster_kv_commit_benchmark_profile \
      "$node" batch-small "$CLUSTER_KV_BATCH_ITERATIONS" \
      "$CLUSTER_KV_BATCH_OPS" "$CLUSTER_KV_VALUE_BYTES" || rc=1
    cluster_kv_commit_benchmark_profile \
      "$node" single-large "$CLUSTER_KV_LARGE_ITERATIONS" \
      1 "$CLUSTER_KV_LARGE_VALUE_BYTES" || rc=1
    cluster_kv_range_benchmark_profile "$node" local || rc=1
    cluster_kv_range_benchmark_profile "$node" authoritative || rc=1
  done
  return $rc
}

read_your_write() {
  run_probe node1 cluster.dfs.read-your-write \
    --param "base=/services/cluster-tests" \
  && run_probe node5 cluster.dfs.read-your-write \
    --param "base=/services/cluster-tests"
}

cross_node() {
  local a="/services/cluster-tests/$RUN_ID/cross-a.txt"
  local b="/services/cluster-tests/$RUN_ID/cross-b.txt"
  local payload_a="cross-node a $RUN_ID" payload_b="cross-node b $RUN_ID"
  run_probe node5 cluster.dfs.write \
    --param "path=$a" --param "payload=$payload_a" \
  && run_probe node1 cluster.dfs.expect-file \
    --param "path=$a" --param "payload=$payload_a" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.expect-file \
    --param "path=$a" --param "payload=$payload_a" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node2 cluster.dfs.write \
    --param "path=$b" --param "payload=$payload_b" \
  && run_probe node6 cluster.dfs.expect-file \
    --param "path=$b" --param "payload=$payload_b" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node1 cluster.dfs.remove --param "path=$a" \
  && run_probe node5 cluster.dfs.expect-absent \
    --param "path=$a" --param "timeout-seconds=$EXPECT_TIMEOUT"
}

capture_rename_failure_context() {
  local src="$1" dst="$2" node
  log "capturing rename failure context into $ARTIFACTS"
  compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" node1 \
    sinomo dfs status --cluster --json \
    > "$ARTIFACTS/rename-failure-cluster-dfs-status.json" \
    2> "$ARTIFACTS/rename-failure-cluster-dfs-status.stderr.log" || true
  for node in $NODES; do
    compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" \
      sinomo dfs path "$src" --json \
      > "$ARTIFACTS/rename-failure-$node-source.json" \
      2> "$ARTIFACTS/rename-failure-$node-source.stderr.log" || true
    compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" \
      sinomo dfs path "$dst" --json \
      > "$ARTIFACTS/rename-failure-$node-destination.json" \
      2> "$ARTIFACTS/rename-failure-$node-destination.stderr.log" || true
  done
}

dfs_acceptance_case() {
  local case_name="$1"
  shift
  python3 "$SINOMO_DOCKER_DIR/test-file-history.py" \
    --project "$PROJECT" --binary "$BINARY" \
    --artifacts "$ARTIFACTS/dfs-$case_name" --case "$case_name" \
    --iterations "${SINOMO_TEST_DFS_BENCH_ITERATIONS:-8}" \
    --warmup "${SINOMO_TEST_DFS_BENCH_WARMUP:-2}" \
    --payload-bytes "${SINOMO_TEST_DFS_BENCH_BYTES:-4096}" "$@"
}

dfs_versions_correctness() {
  dfs_acceptance_case history --restart-cluster \
    && dfs_acceptance_case handoff
}

dfs_benchmarks() {
  # Recovery and handoff are deliberately excluded from timed workloads.
  wait_for_dfs_converged \
    && wait_for_network_quiet dfs-benchmark \
    && dfs_acceptance_case benchmark
}

rename_across_prefixes() {
  local src="/users/cluster-tests/$RUN_ID/move-src.txt"
  local dst="/services/cluster-tests/$RUN_ID/move-dst.txt"
  local payload="rename $RUN_ID"
  if run_probe node2 cluster.dfs.write \
    --param "path=$src" --param "payload=$payload" \
  && run_probe node4 cluster.dfs.expect-file \
    --param "path=$src" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node4 cluster.dfs.rename \
    --param "from=$src" --param "to=$dst" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node6 cluster.dfs.expect-file \
    --param "path=$dst" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node1 cluster.dfs.expect-absent \
    --param "path=$src" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.remove --param "path=$dst"; then
    return 0
  fi
  capture_rename_failure_context "$src" "$dst"
  return 1
}

small_file_baseline() {
  local rc=0 node
  for node in node2 node5; do
    run_probe "$node" cluster.dfs.small-file-baseline \
      --param "files=$SMALL_BASELINE_FILES" \
      --param "bytes=$SMALL_BASELINE_BYTES" \
      --param "sync-writes=$SMALL_BASELINE_SYNC_WRITES" \
      --param "min-files-per-second=$SMALL_BASELINE_MIN_FILES_PER_SEC" || rc=1
  done
  return $rc
}

bulk_requester_is_nonprimary() {
  jq -e '.status == "passed"
    and (.diagnostics.node | type) == "string"
    and (.diagnostics.shards | type) == "array"
    and (.diagnostics.shards | length) == 3
    and (.diagnostics.node as $node | all(.diagnostics.shards[]; .primary_node_id != $node))' "$1" >/dev/null
}

bulk_performance() {
  # The one-shot requester cannot own any metadata shard. A preceding handoff
  # can promote node4, so select two current non-primaries in the far segment.
  local node layout first="" second=""
  for node in node4 node5 node6; do
    layout="$ARTIFACTS/bulk-requester-$node.json"
    compose_exec_timeout "$READINESS_COMMAND_TIMEOUT" "$node" sinomo test run \
      cluster.dfs.shard-layout --require-cluster --json \
      >"$layout" 2>"$ARTIFACTS/bulk-requester-$node.stderr.log" || return 1
    if bulk_requester_is_nonprimary "$layout"; then
      if [ -z "$first" ]; then first="$node"; else second="$node"; break; fi
    fi
  done
  if [ -z "$second" ]; then
    log "bulk-performance: expected two non-primary requesters among node4/node5/node6"
    return 1
  fi
  log "bulk-performance: requesters $first and $second; readers node2 and node1"
  local warm="/services/cluster-tests/$RUN_ID/perf-warmup"
  local a="/services/cluster-tests/$RUN_ID/perf-a"
  local b="/services/cluster-tests/$RUN_ID/perf-b"
  run_probe "$first" cluster.dfs.bulk-write \
    --param "base=$warm" \
    --param "files=$PERF_WARMUP_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
  && run_probe node2 cluster.dfs.bulk-expect \
    --param "base=$warm" \
    --param "files=$PERF_WARMUP_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe "$first" cluster.dfs.bulk-write \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
    --param "max-proc-wchar-amplification=$PERF_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
    --param "max-proc-syscw-per-file=$PERF_MAX_PROC_SYSCW_PER_FILE" \
    --param "max-proc-syscw-fixed=$PERF_MAX_PROC_SYSCW_FIXED" \
  && run_probe node2 cluster.dfs.bulk-expect \
    --param "base=$a" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe "$second" cluster.dfs.bulk-write \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_WRITE_FILES_PER_SEC" \
    --param "max-proc-wchar-amplification=$PERF_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$PERF_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
    --param "max-proc-syscw-per-file=$PERF_MAX_PROC_SYSCW_PER_FILE" \
    --param "max-proc-syscw-fixed=$PERF_MAX_PROC_SYSCW_FIXED" \
  && run_probe node1 cluster.dfs.bulk-expect \
    --param "base=$b" \
    --param "files=$PERF_FILES" \
    --param "bytes=$PERF_BYTES" \
    --param "timeout-seconds=$PERF_TIMEOUT" \
    --param "min-files-per-second=$PERF_MIN_READ_FILES_PER_SEC" \
    --param "max-proc-write-bytes=$PERF_MAX_READ_PROC_WRITE_BYTES"
}

disk_baseline() {
  local rc=0 node
  for node in node2 node6 node5 node1; do
    run_probe "$node" cluster.dfs.disk-baseline \
      --param "bytes=$DISK_BASELINE_BYTES" \
      --param "sync-write=$DISK_BASELINE_SYNC_WRITE" \
      --param "min-write-bytes-per-second=$DISK_BASELINE_MIN_WRITE_BYTES_PER_SEC" \
      --param "min-read-bytes-per-second=$DISK_BASELINE_MIN_READ_BYTES_PER_SEC" || rc=1
  done
  return $rc
}

large_file_performance() {
  local a="/services/cluster-tests/$RUN_ID/large-a.bin"
  local b="/services/cluster-tests/$RUN_ID/large-b.bin"
  run_probe node2 cluster.dfs.large-write \
    --param "path=$a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
    --param "measure-raw-write-baseline=$LARGE_MEASURE_RAW_WRITE_BASELINE" \
    --param "min-raw-write-ratio=$LARGE_MIN_RAW_WRITE_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-wchar-amplification=$LARGE_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
  && run_probe node6 cluster.dfs.large-expect \
    --param "path=$a" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "min-raw-read-ratio=$LARGE_MIN_RAW_READ_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node2 cluster.dfs.expect-replicated \
    --param "path=$a" \
    --param min-locations=2 \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node5 cluster.dfs.large-write \
    --param "path=$b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_WRITE_BYTES_PER_SEC" \
    --param "measure-raw-write-baseline=$LARGE_MEASURE_RAW_WRITE_BASELINE" \
    --param "min-raw-write-ratio=$LARGE_MIN_RAW_WRITE_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-wchar-amplification=$LARGE_MAX_PROC_WCHAR_AMPLIFICATION" \
    --param "max-proc-write-bytes-amplification=$LARGE_MAX_PROC_WRITE_BYTES_AMPLIFICATION" \
  && run_probe node1 cluster.dfs.large-expect \
    --param "path=$b" \
    --param "bytes=$LARGE_BYTES" \
    --param "timeout-seconds=$LARGE_TIMEOUT" \
    --param "min-bytes-per-second=$LARGE_MIN_READ_BYTES_PER_SEC" \
    --param "min-raw-read-ratio=$LARGE_MIN_RAW_READ_RATIO" \
    --param "raw-sync-write=$LARGE_RAW_SYNC_WRITE" \
    --param "max-proc-write-bytes=$LARGE_MAX_READ_PROC_WRITE_BYTES" \
  && run_probe node5 cluster.dfs.expect-replicated \
    --param "path=$b" \
    --param min-locations=2 \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

disk_damage_recovery() {
  local path="/services/cluster-tests/$RUN_ID/disk-damage.bin"
  local seed="disk-damage-$RUN_ID"
  run_probe node3 cluster.dfs.write \
    --param "path=$path" \
    --param "payload-seed=$seed" \
    --param "payload-bytes=$DAMAGE_BYTES" \
    --param "foreground-repair=true" \
  && run_probe node3 cluster.dfs.expect-replicated \
    --param "path=$path" \
    --param "min-locations=2" \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node3 cluster.dfs.damage-local-loose-blob \
    --param "path=$path" \
  && run_probe node3 cluster.dfs.expect-file \
    --param "path=$path" \
    --param "payload-seed=$seed" \
    --param "payload-bytes=$DAMAGE_BYTES" \
    --param "timeout-seconds=$EXPECT_TIMEOUT" \
    --param "max-proc-write-bytes=0" \
  && run_probe node4 cluster.dfs.remove \
    --param "path=$path" \
  && run_probe node3 cluster.dfs.expect-absent \
    --param "path=$path" \
    --param "timeout-seconds=$EXPECT_TIMEOUT"
}

temporary_network_recovery() {
  local path="/services/cluster-tests/$RUN_ID/network-heal.txt"
  local payload="network heal $RUN_ID"
  local network="${PROJECT}_seg_b"
  local node6_container
  node6_container="$(compose ps -q node6)"
  if [ -z "$node6_container" ]; then
    log "cannot find node6 container id"
    return 1
  fi

  log "disconnecting node6 from $network for ${NETWORK_PARTITION_SECONDS}s"
  if ! docker network disconnect "$network" "$node6_container" >/dev/null 2>&1; then
    log "failed to disconnect node6 from $network"
    return 1
  fi

  local rc=0
  sleep "$NETWORK_PARTITION_SECONDS"
  run_probe node2 cluster.dfs.write \
    --param "path=$path" --param "payload=$payload" || rc=1

  log "reconnecting node6 to $network"
  if ! docker network connect --alias node6 "$network" "$node6_container" >/dev/null 2>&1; then
    log "failed to reconnect node6 to $network"
    return 1
  fi

  wait_for_cluster_ready "post-network-heal" || rc=1
  wait_for_dfs_ready || rc=1
  wait_for_dfs_converged || rc=1
  if [ "$rc" = 0 ]; then
    run_probe node6 cluster.dfs.expect-file \
      --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" || rc=1
  fi
  return "$rc"
}

restart_durability() {
  local path="/services/cluster-tests/$RUN_ID/durable.txt"
  local payload="durable $RUN_ID"
  run_probe node3 cluster.dfs.write \
    --param "path=$path" --param "payload=$payload" \
  && run_probe node2 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  || return 1
  log "restarting node2"
  compose restart node2 >/dev/null 2>&1 || { log "node2 restart failed"; return 1; }
  wait_for_cluster_ready "post-restart" || return 1
  wait_for_dfs_ready || return 1
  wait_for_dfs_converged || return 1
  run_probe node2 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT" \
  && run_probe node5 cluster.dfs.expect-file \
    --param "path=$path" --param "payload=$payload" --param "timeout-seconds=$EXPECT_TIMEOUT"
}

FORMATION_OBSERVATION_COLLECTED=0
freeze_formation_observation_node() {
  local observe_dir="$1" node="$2" node_dir="$1/$2" capture
  local timeline_pid profile_pid trace_pid captured_trace_pid all_trace_pid
  capture="$(
    compose_exec_timeout "$((ARTIFACT_COMMAND_TIMEOUT + 10))" "$node" sh -c '
      for attempt in $(seq 1 150); do
        pid=$(ps -eo pid=,args= | awk '\''$3 == "daemon" && $4 == "run" { print $1; exit }'\'')
        generation=$(awk '\''{print $22}'\'' "/proc/$pid/stat" 2>/dev/null)
        marker=$(cat /data/observe/formation/current-daemon-pid 2>/dev/null)
        capture=$(cat /data/observe/formation/current-capture 2>/dev/null)
        if [ "$marker" = "$pid:$generation" ] && [ -f "/data/observe/formation/$capture.ready" ]; then
          printf "%s\n" "$capture"; exit 0
        fi
        sleep 0.1
      done
      exit 1' 2>/dev/null
  )" || capture=""
  if [ -z "$capture" ]; then
    printf '%s\n' "unavailable" > "$node_dir/current-capture"
    return 0
  fi
  printf '%s\n' "$capture" > "$node_dir/current-capture"

  # Capture scheduling is deliberately off the formation dependency path.
  # At the end of the hard gate, wait briefly for a just-scheduled observer to
  # finish starting, then freeze all three process-local capture surfaces.
  compose_exec_timeout 4 "$node" sh -c \
    "for attempt in \$(seq 1 40); do [ -f /data/observe/formation/$capture.ready ] && exit 0; sleep 0.1; done; exit 1" \
    >/dev/null 2>&1 || true

  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe timeline network stop --capture "$capture" --json \
    > "$node_dir/network-timeline.json" 2> "$node_dir/network-timeline.stderr.log" &
  timeline_pid=$!
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe profile cpu stop --capture "$capture" --json \
    > "$node_dir/cpu-profile-stop.json" 2> "$node_dir/cpu-profile-stop.stderr.log" &
  profile_pid=$!
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe control --clear-capture --expected-capture "$capture" --json \
    > "$node_dir/trace-control-stop.json" 2> "$node_dir/trace-control-stop.stderr.log" &
  trace_pid=$!
  wait "$timeline_pid" "$profile_pid" "$trace_pid" 2>/dev/null || true

  # Trace records are process-local. Read both the capture-tagged subset and
  # the full ring immediately after clearing the tag, while the daemon
  # generation frozen above is still available. The latter retains startup
  # events emitted before the asynchronous capture-id RPC completed.
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe traces --capture "$capture" --limit 4096 --json \
    > "$node_dir/traces.json" 2> "$node_dir/traces.stderr.log" &
  captured_trace_pid=$!
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe traces --limit 4096 --json \
    > "$node_dir/all-process-traces.json" \
    2> "$node_dir/all-process-traces.stderr.log" &
  all_trace_pid=$!
  wait "$captured_trace_pid" "$all_trace_pid" 2>/dev/null || true
}

collect_formation_observation_node() {
  local observe_dir="$1" node="$2" node_dir="$1/$2" capture container attempt generation_capture
  capture="$(sed -n '1p' "$node_dir/current-capture" 2>/dev/null)" || capture=""
  if [ -z "$capture" ] || [ "$capture" = "unavailable" ]; then
    return 0
  fi

  for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
      observe profile cpu status --json \
      > "$node_dir/cpu-profile-status.json" 2> "$node_dir/cpu-profile-status.stderr.log" || true
    if grep -Eq '"state": "(complete|failed|unsupported)"' \
      "$node_dir/cpu-profile-status.json"; then
      break
    fi
    sleep 0.1
  done
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo \
    observe metrics --limit 4096 --json \
    > "$node_dir/metrics.json" 2> "$node_dir/metrics.stderr.log" || true

  container="$(compose ps -q "$node")"
  if [ -n "$container" ]; then
    mkdir -p \
      "$node_dir/profile-artifact" \
      "$node_dir/profile-artifacts" \
      "$node_dir/startup" \
      "$node_dir/network-timeline-artifacts"
    docker cp "$container:/data/observe/profiles/$capture/." \
      "$node_dir/profile-artifact/" >/dev/null 2>&1 || true
    # Preserve recorded identity-changing formation generations, including
    # joins which restarted the container. Do not re-export/re-render unrelated
    # historical write/idle profiles from retained volumes.
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sh -c '
      awk -F "\t" '\''{
        if ($1 ~ /^formation-[a-zA-Z0-9-]+$/)
          print $1
      }'\'' /data/observe/formation/captures.tsv' \
      > "$node_dir/formation-captures.txt" 2> "$node_dir/formation-captures.stderr.log" || true
    printf '%s\n' "$capture" >> "$node_dir/formation-captures.txt"
    while IFS= read -r generation_capture; do
      [[ "$generation_capture" =~ ^formation-[a-zA-Z0-9-]+$ ]] || continue
      mkdir -p "$node_dir/profile-artifacts/$generation_capture" \
        "$node_dir/network-timeline-artifacts/$generation_capture"
      docker cp "$container:/data/observe/profiles/$generation_capture/." \
        "$node_dir/profile-artifacts/$generation_capture/" >/dev/null 2>&1 || true
      docker cp "$container:/data/observe/timelines/$generation_capture/." \
        "$node_dir/network-timeline-artifacts/$generation_capture/" >/dev/null 2>&1 || true
    done < <(sort -u "$node_dir/formation-captures.txt")
    docker cp "$container:/data/observe/formation/." \
      "$node_dir/startup/" >/dev/null 2>&1 || true
  fi
}

render_formation_observation_artifacts() {
  local observe_dir="$1"
  local flamegraph_renderer="$SINOMO_ROOT/scripts/debug/render-pprof-flamegraph.py"
  local timeline_summarizer="$SINOMO_ROOT/scripts/debug/summarize-network-timeline.py"
  local perfetto_converter="$SINOMO_ROOT/scripts/debug/observe-traces-to-perfetto.py"
  local node node_dir profile profile_dir persisted_timeline persisted_dir

  # Produce reviewable artifacts while the capture is still next to the exact
  # profile metadata. Rendering is diagnostic-only: a host without Go/Python
  # must not change the 60-second formation result.
  for node in $NODES; do
    node_dir="$observe_dir/$node"
    if [ -x "$timeline_summarizer" ] && [ -s "$node_dir/network-timeline.json" ]; then
      "$timeline_summarizer" "$node_dir/network-timeline.json" \
        > "$node_dir/network-timeline-summary.txt" \
        2> "$node_dir/network-timeline-summary.stderr.log" || true
    fi
    if [ -x "$perfetto_converter" ] \
      && [ -s "$node_dir/all-process-traces.json" ] \
      && [ -s "$node_dir/network-timeline.json" ]; then
      "$perfetto_converter" --node "$node" \
        "$node_dir/all-process-traces.json" "$node_dir/network-timeline.json" \
        -o "$node_dir/formation.perfetto.json" \
        > "$node_dir/formation.perfetto.stdout.log" \
        2> "$node_dir/formation.perfetto.stderr.log" || true
    fi
    while IFS= read -r -d '' persisted_timeline; do
      persisted_dir="$(dirname "$persisted_timeline")"
      if [ -x "$timeline_summarizer" ]; then
        "$timeline_summarizer" "$persisted_timeline" \
          > "$persisted_dir/network-timeline-summary.txt" \
          2> "$persisted_dir/network-timeline-summary.stderr.log" || true
      fi
      if [ -x "$perfetto_converter" ]; then
        "$perfetto_converter" --node "$node" \
          "$persisted_timeline" \
          -o "$persisted_dir/formation.perfetto.json" \
          > "$persisted_dir/formation.perfetto.stdout.log" \
          2> "$persisted_dir/formation.perfetto.stderr.log" || true
      fi
    done < <(find "$node_dir/network-timeline-artifacts" -type f -name timeline.json -print0)
  done

  [ -x "$flamegraph_renderer" ] || return 0
  while IFS= read -r -d '' profile; do
    profile_dir="$(dirname "$profile")"
    "$flamegraph_renderer" "$profile" -o "$profile_dir/flamegraph.html" \
      > "$profile_dir/flamegraph.render.log" \
      2> "$profile_dir/flamegraph.render.stderr.log" || true
  done < <(find "$observe_dir" -type f -name cpu.pprof.pb -print0)
}

capture_formation_observation() {
  [ "$FORMATION_OBSERVE" = 1 ] || return 0
  [ "$FORMATION_OBSERVATION_COLLECTED" = 0 ] || return 0
  FORMATION_OBSERVATION_COLLECTED=1

  local observe_dir="$ARTIFACTS/formation-observe"
  local node node_dir capture worker_pid
  local -a freeze_workers=() collect_workers=()
  mkdir -p "$observe_dir"
  log "freezing bounded formation traces, RPC timelines, and CPU profiles"

  # Freeze every node at the same gate edge. A sequential stop allowed later
  # joining daemons to replace themselves before their capture was addressed.
  for node in $NODES; do
    node_dir="$observe_dir/$node"
    mkdir -p "$node_dir"
    freeze_formation_observation_node "$observe_dir" "$node" &
    freeze_workers+=("$!")
  done
  for worker_pid in "${freeze_workers[@]}"; do
    wait "$worker_pid" 2>/dev/null || true
  done

  : > "$observe_dir/captures.tsv"
  for node in $NODES; do
    capture="$(sed -n '1p' "$observe_dir/$node/current-capture" 2>/dev/null)" || capture=""
    printf '%s\t%s\n' "$node" "${capture:-unavailable}" >> "$observe_dir/captures.tsv"
  done

  log "collecting frozen formation observation artifacts"
  for node in $NODES; do
    collect_formation_observation_node "$observe_dir" "$node" &
    collect_workers+=("$!")
  done
  for worker_pid in "${collect_workers[@]}"; do
    wait "$worker_pid" 2>/dev/null || true
  done
  log "rendering formation timelines and flamegraphs"
  render_formation_observation_artifacts "$observe_dir"
}

capture_failure_context() {
  log "capturing failure context into $ARTIFACTS"
  printf '%s\n' 'Failure logs retain the latest 20000 lines per stream, not complete history. Full daemon logs remain in the selected project volumes.' \
    > "$ARTIFACTS/failure-log-coverage.txt"
  capture_formation_observation
  compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" ps > "$ARTIFACTS/compose-ps.txt" 2>&1 || true
  cluster_health > "$ARTIFACTS/cluster-health.txt" 2>&1 || true
  compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" node1 sinomo cluster doctor \
    > "$ARTIFACTS/cluster-doctor.txt" 2>&1 || true
  local node
  for node in $NODES; do
    compose_timeout "$ARTIFACT_COMMAND_TIMEOUT" logs --no-color --tail 20000 "$node" \
      > "$ARTIFACTS/$node.log" 2>&1 || true
    # The shadow supervisor redirects the main daemon's output into /data.
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" tail -n 20000 /data/daemon/main.stderr.log \
      > "$ARTIFACTS/$node.daemon.stderr.log" 2>/dev/null || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" tail -n 20000 /data/daemon/main.stdout.log \
      > "$ARTIFACTS/$node.daemon.stdout.log" 2>/dev/null || true
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sinomo cluster connectivity \
      > "$ARTIFACTS/$node.cluster-connectivity.txt" 2>&1 || true
    # Local cluster-KV view: snapshot revision + invitation/shard keys, the
    # usual suspects when a node's view has diverged.
    compose_exec_timeout "$ARTIFACT_COMMAND_TIMEOUT" "$node" sh -c \
      'sinomo debug sobj /data/cluster/kv-snapshot.sobj 2>/dev/null | grep -o "\"revision\": uint64([0-9]*)" | head -1' \
      > "$ARTIFACTS/$node.kv-revision.txt" 2>/dev/null || true
  done
}

TEARDOWN_DONE=0
PROJECT_OWNED=0
teardown_cluster() {
  local original_exit="$1"
  [ "$TEARDOWN_DONE" = 1 ] && return
  TEARDOWN_DONE=1
  if [ -n "$NETWORK_CASES" ]; then
    network_check finish "$ARTIFACTS" --cases "$NETWORK_CASES" --exit-code "$original_exit" || true
  fi
  network_quiet_stats_stream_stop
  if [ "$PJDFSTEST_STARTED" = 1 ]; then
    collect_pjdfstest_artifacts || true
    cleanup_pjdfstest_mount || true
  fi
  [ "$PROJECT_OWNED" = 1 ] || return
  if [ -n "$FAILED_SCENARIOS" ]; then
    capture_failure_context
  fi
  if [ "${SINOMO_TEST_KEEP:-0}" = 1 ] || [ "${SINOMO_TEST_REUSE:-0}" = 1 ] \
    || { [ -n "$FAILED_SCENARIOS" ] && [ "${SINOMO_TEST_KEEP_ON_FAILURE:-0}" = 1 ]; }; then
    log "leaving cluster up (project $PROJECT); tear down with:"
    if [ "$PJDFSTEST_ONLY" = 1 ] && [ -n "$COMPOSE_OVERRIDE_FILE" ]; then
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE -f $SINOMO_DOCKER_DIR/docker-compose.pjdfstest.yml -f $COMPOSE_OVERRIDE_FILE down -v"
    elif [ "$PJDFSTEST_ONLY" = 1 ]; then
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE -f $SINOMO_DOCKER_DIR/docker-compose.pjdfstest.yml down -v"
    elif [ -n "$COMPOSE_OVERRIDE_FILE" ]; then
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE -f $COMPOSE_OVERRIDE_FILE down -v"
    else
      log "  docker compose -p $PROJECT -f $COMPOSE_FILE down -v"
    fi
    return
  fi
  log "tearing down compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  local attempt
  for attempt in 1 2 3; do
    if [ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)" ] \
      && [ -z "$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)" ]; then
      return
    fi
    sleep 2
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  done
}

teardown() {
  local original_exit=$? report_exit=0
  teardown_cluster "$original_exit" || log "cluster teardown did not complete"
  python3 "$SINOMO_DOCKER_DIR/verification_report.py" "$ARTIFACTS" \
    --project "$PROJECT" --binary "$BINARY" --exit-code "$original_exit" \
    --profile "${SINOMO_TEST_PROFILE:-debug+cpu-profiler}" \
    --lane "formation=$FORMATION_ONLY dfs-ready=$DFS_READY_ONLY quiet=$QUIET_ONLY dfs=$DFS_ONLY network=${NETWORK_CASES:-${SINOMO_TEST_NETWORK_ONLY:-0}} kv=$CLUSTER_KV_ONLY pjdfstest=$PJDFSTEST_ONLY" \
    --failed-scenarios "$FAILED_SCENARIOS" || report_exit=$?
  trap - EXIT
  if [ "$original_exit" != 0 ]; then exit "$original_exit"; fi
  exit "$report_exit"
}

# --- bring-up -----------------------------------------------------------------

if [ "${SINOMO_TEST_TIMING_HELPERS_SELF_TEST:-0}" = 1 ]; then
  timing_helpers_self_test || exit 1
  exit 0
fi

if [ -n "$NETWORK_CASES" ]; then
  if [ "$FORMATION_ONLY$DFS_READY_ONLY$DFS_ONLY$CLUSTER_KV_ONLY$PJDFSTEST_ONLY${SINOMO_TEST_NETWORK_ONLY:-0}" != 000000 ]; then
    log "NETWORK_CASES cannot be combined with other lane selectors"; exit 1
  fi
  if [ "${SINOMO_TEST_NETWORK_PLAN:-0}" = 1 ]; then
    printf 'cases: %s\nproject: %s\nbinary: %s\nbuild: %s\nreuse: %s\nkeep: %s\nartifacts: %s\n' \
      "$NETWORK_CASES" "$PROJECT" "$BINARY" "${SINOMO_TEST_BUILD:-0}" "${SINOMO_TEST_REUSE:-0}" "${SINOMO_TEST_KEEP:-0}" "$ARTIFACTS"
    python3 "$NETWORK_CHECKS" list
    exit 0
  fi
fi

if [ -n "$NETWORK_CASES" ] && { [ -e "$ARTIFACTS/result.json" ] || [ -e "$ARTIFACTS/network-preflight" ]; }; then
  log "refusing to overwrite an earlier network run; choose a fresh SINOMO_TEST_ARTIFACTS directory"
  exit 1
fi

validate_network_realtime_gate_config || exit 1
validate_network_saturation_config || exit 1
validate_cluster_kv_benchmark_config || exit 1
validate_dfs_suite_config || exit 1
validate_formation_observation_config || exit 1
validate_pjdfstest_config || exit 1
require_no_other_compose_projects || exit 1
trap teardown EXIT

if [ "${SINOMO_TEST_BUILD:-1}" != 1 ] && [ ! -x "$BINARY" ]; then
  log "prebuilt binary missing or not executable: $BINARY (build disabled)"
  exit 1
fi
if [[ "$BINARY" != /* ]] || [ "$SINOMO_DOCKER_BINARY" != "$BINARY" ]; then
  log "binary and Docker mount must use the same absolute path"; exit 1
fi
if [ "${SINOMO_TEST_BUILD:-1}" = 1 ]; then
  if [ "$BINARY" != "$SINOMO_ROOT/target/debug/sinomo" ]; then
    log "custom host binary is missing or SINOMO_TEST_BUILD requested: $BINARY"
    exit 1
  fi
  if [ "${SINOMO_TEST_REUSE:-0}" = 1 ]; then
    log "reuse requires SINOMO_TEST_BUILD=0 and a verified profiler-enabled binary"; exit 1
  fi
  log "stopping this task's cluster before compiling"
  compose stop -t 10 || { log "cluster stop failed"; exit 1; }
  log "building host binary with Observe CPU profiler support"
  (cd "$SINOMO_ROOT" && env -u CARGO_BUILD_JOBS cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler) || { log "host build failed"; exit 1; }
fi

if [ "${SINOMO_TEST_REUSE:-0}" != 1 ]; then
  PROJECT_OWNED=1
  log "resetting compose project $PROJECT"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  log "starting 6-node cluster (project $PROJECT)"
  if ! compose up --build -d; then
    log "compose up failed"
    FAILED_SCENARIOS="bring-up"
    exit 1
  fi
else
  PROJECT_OWNED=1
fi

if [ -n "$NETWORK_CASES" ] && [ "${SINOMO_TEST_REUSE:-0}" = 1 ]; then
  network_check preflight "$ARTIFACTS/network-preflight" || { FAILED_SCENARIOS=network-preflight; exit 1; }
fi

if ! wait_for_cluster_ready "formation"; then
  capture_formation_observation
  cluster_health
  FAILED_SCENARIOS="formation"
  exit 1
fi
capture_formation_observation
if ! dfs_acceptance_case observe; then
  FAILED_SCENARIOS="observe-configuration"
  exit 1
fi
if [ "$FORMATION_ONLY" = 1 ]; then
  log "RESULT: PASS — 60-second formation gate green (observations in $ARTIFACTS/formation-observe)"
  exit 0
fi
if [ -n "$NETWORK_CASES" ]; then
  if [ "${SINOMO_TEST_REUSE:-0}" != 1 ]; then
    network_check preflight "$ARTIFACTS/network-preflight" || { FAILED_SCENARIOS=network-preflight; exit 1; }
  fi
  selected_network_cases || { FAILED_SCENARIOS="${FAILED_SCENARIOS:-network-prerequisites}"; exit 1; }
  log "RESULT: PASS — selected network cases: $NETWORK_CASES (artifacts: $ARTIFACTS)"
  exit 0
fi
if ! wait_for_network_ready; then
  FAILED_SCENARIOS="network-ready"
  exit 1
fi

# The quiet gate measures the complete service containers, so every control
# plane that shares those containers must have reached its own stable state
# first. In particular, a freshly formed cluster can already route RPCs while
# DFS metadata-shard handoffs are still committing their final placement
# records. Starting the quiet-window deadline before those handoffs converge
# would conflate expected formation work with idle control-plane load and make
# every throughput result depend on a race. These are readiness prerequisites,
# not part of the Docker-only 60-second cluster-join gate above.
if ! wait_for_dfs_ready; then
  FAILED_SCENARIOS="pre-benchmark-dfs-ready"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="pre-benchmark-dfs-converged"
  exit 1
fi
if [ "$QUIET_ONLY" = 1 ]; then
  if ! wait_for_network_quiet steady-state; then
    FAILED_SCENARIOS="steady-state-quiet"
    exit 1
  fi
  log "RESULT: PASS — readiness and whole-container quietness gates green (artifacts: $ARTIFACTS)"
  exit 0
fi
if ! dfs_daemon_mutation_lanes_ready; then
  FAILED_SCENARIOS="pre-benchmark-dfs-daemon-mutation-lanes"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="pre-benchmark-dfs-post-daemon-mutation-convergence"
  exit 1
fi
if [ "$DFS_READY_ONLY" = 1 ]; then
  if [ "$DFS_READY_REQUIRE_QUIET" = 1 ] && ! wait_for_network_quiet dfs-write-latency; then
    FAILED_SCENARIOS="dfs-write-latency-pre-benchmark-quiet"
    exit 1
  fi
  log "RESULT: PASS — DFS readiness, daemon mutation, and convergence gates green (probe reports in $ARTIFACTS)"
  exit 0
fi

if [ "$DFS_ONLY" = 1 ]; then
  scenario "dfs-normal-correctness" dfs_acceptance_case normal
  scenario "rename-across-prefixes" rename_across_prefixes
  scenario "dfs-versions-correctness" dfs_versions_correctness
  # Do not publish benchmark samples after a failed correctness prerequisite.
  if [ -n "$FAILED_SCENARIOS" ]; then exit 1; fi
  scenario "dfs-cli-benchmarks" dfs_benchmarks
  if [ -n "$FAILED_SCENARIOS" ]; then exit 1; fi
  scenario "bulk-performance" bulk_performance
  scenario "large-file-performance" large_file_performance
  scenario "invariants-final" invariants_sweep
  scenario "routed-status-final" final_routed_status
  if [ -n "$FAILED_SCENARIOS" ]; then exit 1; fi
  log "RESULT: PASS — DFS normal/version correctness and benchmarks green (reports in $ARTIFACTS)"
  exit 0
fi

if [ "$PJDFSTEST_ONLY" = 1 ]; then
  echo
  log "--- external mounted-filesystem compatibility: pjdfstest ---"
  if ! pjdfstest_lane; then
    FAILED_SCENARIOS="pjdfstest-$PJDFSTEST_COMPATIBILITY_STATUS"
    exit 1
  fi
  log "RESULT: PASS — pjdfstest harness and post-cluster gates green; compatibility=$PJDFSTEST_COMPATIBILITY_STATUS (evidence in $PJDFSTEST_HOST_ARTIFACTS)"
  exit 0
fi

if [ "$CLUSTER_KV_ONLY" = 1 ]; then
  scenario "cluster-kv-pre-benchmark-quiet" wait_for_network_quiet cluster-kv
  if [ -n "$FAILED_SCENARIOS" ]; then
    exit 1
  fi
  scenario "cluster-kv-benchmark-suite" cluster_kv_benchmark_suite
  if [ -n "$FAILED_SCENARIOS" ]; then
    exit 1
  fi
  log "RESULT: PASS — stabilized daemon-mediated cluster-KV benchmark suite green (reports in $ARTIFACTS)"
  exit 0
fi

scenario "network-realtime-latency-gate" network_realtime_latency_gate
# Quiet control-plane behavior is a prerequisite for every throughput result
# below. Fail closed instead of publishing measurements collected after a busy
# or unobservable interval.
if [ -n "$FAILED_SCENARIOS" ]; then
  exit 1
fi
scenario "dfs-protocol-codec-baseline" dfs_protocol_codec_baseline
scenario "network-latency-baseline" network_baseline
scenario "network-payload-baseline" network_payload_baseline
scenario "network-semantic-attachment-baseline" network_semantic_attachment_baseline
if [ "$NETWORK_SATURATION" = 1 ]; then
  scenario "network-semantic-attachment-saturation" \
    network_semantic_attachment_saturation_profile
else
  log "optional network semantic-attachment saturation profile disabled"
fi
scenario "cluster-kv-quorum-baseline" cluster_kv_quorum_baseline
if [ -n "$FAILED_SCENARIOS" ]; then
  exit 1
fi
if ! wait_for_dfs_ready; then
  FAILED_SCENARIOS="dfs-ready"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="dfs-converged"
  exit 1
fi
if ! dfs_daemon_mutation_lanes_ready; then
  FAILED_SCENARIOS="dfs-daemon-mutation-lanes"
  exit 1
fi
if ! wait_for_dfs_converged; then
  FAILED_SCENARIOS="dfs-post-daemon-mutation-convergence"
  exit 1
fi
if [ "${SINOMO_TEST_NETWORK_ONLY:-0}" = 1 ]; then
  if ! final_routed_status; then
    FAILED_SCENARIOS="dfs-stream-native-service-gate"
    exit 1
  fi
  log "RESULT: PASS — network/KV baselines and DFS readiness/mutation/convergence gates green (probe reports in $ARTIFACTS)"
  exit 0
fi

# --- scenarios ----------------------------------------------------------------

scenario "invariants-baseline"  invariants_sweep
scenario "read-your-write"      read_your_write
scenario "cross-node"           cross_node
scenario "rename-across-prefixes" rename_across_prefixes
scenario "small-file-baseline"  small_file_baseline
scenario "bulk-performance"     bulk_performance
scenario "disk-baseline"        disk_baseline
scenario "large-file-performance" large_file_performance
scenario "disk-damage-recovery" disk_damage_recovery
scenario "temporary-network-recovery" temporary_network_recovery
scenario "restart-durability"   restart_durability
scenario "dfs-normal-correctness" dfs_acceptance_case normal
scenario "dfs-versions-correctness" dfs_versions_correctness
if [ -z "$FAILED_SCENARIOS" ]; then
  scenario "dfs-cli-benchmarks" dfs_benchmarks
fi
scenario "invariants-final"     invariants_sweep
scenario "routed-status-final"  final_routed_status

echo
if [ -n "$FAILED_SCENARIOS" ]; then
  log "RESULT: FAIL —$FAILED_SCENARIOS (evidence in $ARTIFACTS)"
  exit 1
fi
log "RESULT: PASS — all scenarios green (probe reports in $ARTIFACTS)"
