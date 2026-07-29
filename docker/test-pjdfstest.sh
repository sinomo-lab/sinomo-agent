#!/usr/bin/env bash
# External mounted-filesystem compatibility lane for the segmented Docker cluster.
set -uo pipefail

SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINOMO_ROOT="$(dirname "$SINOMO_DOCKER_DIR")"
HARNESS="$SINOMO_DOCKER_DIR/test-cluster.sh"

usage() {
  cat <<'EOF'
Usage: docker/test-pjdfstest.sh [--report|--strict] [--smoke|--full]

Defaults to a full, report-only compatibility run. Report mode records SIFS
assertion failures without failing the command; native-control, mount, runner,
kernel-mount cleanup, and post-cluster failures always fail. Strict mode also
fails when the executed SIFS profile is incompatible or incomplete.

Environment equivalents:
  SINOMO_PJDFSTEST_MODE=report|strict
  SINOMO_PJDFSTEST_PROFILE=smoke|full
  SINOMO_PJDFSTEST_TIMEOUT_SECONDS=3600
  SINOMO_PJDFSTEST_ARTIFACTS=<host directory>
EOF
}

result_value() {
  local result_file="$1" key="$2"
  awk -F= -v key="$key" '
    $1 == key {
      sub(/^[^=]*=/, "")
      print
      exit
    }
  ' "$result_file"
}

PROFILE="${SINOMO_PJDFSTEST_PROFILE:-full}"
MODE="${SINOMO_PJDFSTEST_MODE:-}"
STRICT="${SINOMO_PJDFSTEST_STRICT:-0}"
if [ -n "$MODE" ]; then
  case "$MODE" in
    report) STRICT=0 ;;
    strict) STRICT=1 ;;
    *)
      printf '[test-pjdfstest] invalid mode: %s\n' "$MODE" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    --report) MODE=report; STRICT=0 ;;
    --strict) MODE=strict; STRICT=1 ;;
    --smoke) PROFILE=smoke ;;
    --full) PROFILE=full ;;
    --mode)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      MODE="$2"
      case "$MODE" in
        report) STRICT=0 ;;
        strict) STRICT=1 ;;
        *) printf '[test-pjdfstest] invalid mode: %s\n' "$MODE" >&2; exit 2 ;;
      esac
      shift
      ;;
    --profile)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      PROFILE="$2"
      shift
      ;;
    -h|--help) usage; exit 0 ;;
    *)
      printf '[test-pjdfstest] unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

case "$PROFILE" in
  smoke|full) ;;
  *) printf '[test-pjdfstest] invalid profile: %s\n' "$PROFILE" >&2; exit 2 ;;
esac
case "$STRICT" in
  0) MODE=report ;;
  1) MODE=strict ;;
  *) printf '[test-pjdfstest] SINOMO_PJDFSTEST_STRICT must be 0 or 1\n' >&2; exit 2 ;;
esac

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACTS="${SINOMO_PJDFSTEST_ARTIFACTS:-$SINOMO_ROOT/target/pjdfstest-results/$RUN_ID}"
mkdir -p "$ARTIFACTS"

export SINOMO_TEST_PJDFSTEST_ONLY=1
export SINOMO_TEST_PROJECT="${SINOMO_TEST_PROJECT:-sinomo-pjdfstest}"
export SINOMO_TEST_ARTIFACTS="$ARTIFACTS"
export SINOMO_PJDFSTEST_PROFILE="$PROFILE"
export SINOMO_PJDFSTEST_STRICT="$STRICT"
export SINOMO_PJDFSTEST_TIMEOUT_SECONDS="${SINOMO_PJDFSTEST_TIMEOUT_SECONDS:-3600}"

{
  printf 'protocol=sinomo.pjdfstest.environment.v1\n'
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'git_revision=%s\n' "$(git -C "$SINOMO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'git_dirty_files=%s\n' "$(
    git -C "$SINOMO_ROOT" status --porcelain 2>/dev/null | wc -l | tr -d ' '
  )"
  printf 'compose_project=%s\n' "$SINOMO_TEST_PROJECT"
  printf 'compose_overlay=%s\n' "$SINOMO_DOCKER_DIR/docker-compose.pjdfstest.yml"
  printf 'mode=%s\n' "$MODE"
  printf 'profile=%s\n' "$PROFILE"
  printf 'timeout_seconds=%s\n' "$SINOMO_PJDFSTEST_TIMEOUT_SECONDS"
  printf 'host_uname=%s\n' "$(uname -a)"
  printf 'docker_version=%s\n' "$(
    docker version --format '{{.Server.Version}}' 2>/dev/null || printf unavailable
  )"
} >"$ARTIFACTS/environment.env"

printf '[test-pjdfstest] mode=%s profile=%s artifacts=%s\n' "$MODE" "$PROFILE" "$ARTIFACTS"
set +e
"$HARNESS" 2>&1 | tee "$ARTIFACTS/harness.log"
result=${PIPESTATUS[0]}
set -e

compatibility="not_run"
pjdfstest_patchset="unknown"
control_files_passed="unknown"
control_files_unsupported="unknown"
control_assertions_passed="unknown"
control_assertions_unresolved_directives="unknown"
sifs_files_passed="unknown"
sifs_files_failed="unknown"
sifs_files_unsupported="unknown"
sifs_assertions_passed="unknown"
sifs_assertions_failed="unknown"
sifs_assertions_unresolved_directives="unknown"
zero_byte_routed_write_status="unknown"
zero_byte_metadata_shard_id="unknown"
zero_byte_metadata_primary_node_id="unknown"
runner_result="$ARTIFACTS/pjdfstest/result.env"
if [ -f "$runner_result" ]; then
  compatibility="$(result_value "$runner_result" compatibility_status)"
  [ -n "$compatibility" ] || compatibility="not_run"
  pjdfstest_patchset="$(result_value "$runner_result" pjdfstest_patchset)"
  control_files_passed="$(result_value "$runner_result" control_files_passed)"
  control_files_unsupported="$(result_value "$runner_result" control_files_unsupported)"
  control_assertions_passed="$(result_value "$runner_result" control_assertions_passed)"
  control_assertions_unresolved_directives="$(
    result_value "$runner_result" control_assertions_unresolved_directives
  )"
  sifs_files_passed="$(result_value "$runner_result" sifs_files_passed)"
  sifs_files_failed="$(result_value "$runner_result" sifs_files_failed)"
  sifs_files_unsupported="$(result_value "$runner_result" sifs_files_unsupported)"
  sifs_assertions_passed="$(result_value "$runner_result" sifs_assertions_passed)"
  sifs_assertions_failed="$(result_value "$runner_result" sifs_assertions_failed)"
  sifs_assertions_unresolved_directives="$(
    result_value "$runner_result" sifs_assertions_unresolved_directives
  )"
  zero_byte_routed_write_status="$(
    result_value "$runner_result" zero_byte_routed_write_status
  )"
  zero_byte_metadata_shard_id="$(
    result_value "$runner_result" zero_byte_metadata_shard_id
  )"
  zero_byte_metadata_primary_node_id="$(
    result_value "$runner_result" zero_byte_metadata_primary_node_id
  )"
fi

{
  printf 'protocol=sinomo.pjdfstest.wrapper-result.v2\n'
  printf 'finished_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'exit_code=%s\n' "$result"
  printf 'mode=%s\n' "$MODE"
  printf 'profile=%s\n' "$PROFILE"
  printf 'compatibility_status=%s\n' "$compatibility"
  printf 'pjdfstest_patchset=%s\n' "${pjdfstest_patchset:-unknown}"
  printf 'control_files_passed=%s\n' "${control_files_passed:-unknown}"
  printf 'control_files_unsupported=%s\n' "${control_files_unsupported:-unknown}"
  printf 'control_assertions_passed=%s\n' "${control_assertions_passed:-unknown}"
  printf 'control_assertions_unresolved_directives=%s\n' \
    "${control_assertions_unresolved_directives:-unknown}"
  printf 'sifs_files_passed=%s\n' "${sifs_files_passed:-unknown}"
  printf 'sifs_files_failed=%s\n' "${sifs_files_failed:-unknown}"
  printf 'sifs_files_unsupported=%s\n' "${sifs_files_unsupported:-unknown}"
  printf 'sifs_assertions_passed=%s\n' "${sifs_assertions_passed:-unknown}"
  printf 'sifs_assertions_failed=%s\n' "${sifs_assertions_failed:-unknown}"
  printf 'sifs_assertions_unresolved_directives=%s\n' \
    "${sifs_assertions_unresolved_directives:-unknown}"
  printf 'zero_byte_routed_write_status=%s\n' \
    "${zero_byte_routed_write_status:-unknown}"
  printf 'zero_byte_metadata_shard_id=%s\n' \
    "${zero_byte_metadata_shard_id:-unknown}"
  printf 'zero_byte_metadata_primary_node_id=%s\n' \
    "${zero_byte_metadata_primary_node_id:-unknown}"
  binary="${SINOMO_TEST_BINARY:-$SINOMO_ROOT/target/debug/sinomo}"
  if [ -f "$binary" ]; then
    printf 'binary=%s\n' "$binary"
    printf 'binary_sha256=%s\n' "$(sha256sum "$binary" | awk '{print $1}')"
  fi
} >"$ARTIFACTS/wrapper-result.env"

if [ "$result" -eq 0 ]; then
  printf '[test-pjdfstest] COMPLETE: compatibility=%s; evidence in %s\n' \
    "$compatibility" "$ARTIFACTS"
else
  printf '[test-pjdfstest] FAIL: compatibility=%s; evidence in %s\n' \
    "$compatibility" "$ARTIFACTS" >&2
fi
if [ -f "$runner_result" ]; then
  printf '[test-pjdfstest] native: files=%s passed, %s unsupported; assertions=%s passed, %s unresolved directives\n' \
    "${control_files_passed:-unknown}" \
    "${control_files_unsupported:-unknown}" \
    "${control_assertions_passed:-unknown}" \
    "${control_assertions_unresolved_directives:-unknown}"
  printf '[test-pjdfstest] SIFS: files=%s passed, %s failed, %s unsupported; assertions=%s passed, %s failed, %s unresolved directives\n' \
    "${sifs_files_passed:-unknown}" \
    "${sifs_files_failed:-unknown}" \
    "${sifs_files_unsupported:-unknown}" \
    "${sifs_assertions_passed:-unknown}" \
    "${sifs_assertions_failed:-unknown}" \
    "${sifs_assertions_unresolved_directives:-unknown}"
  printf '[test-pjdfstest] zero-byte routed write: %s (shard=%s remote-primary=%s)\n' \
    "${zero_byte_routed_write_status:-unknown}" \
    "${zero_byte_metadata_shard_id:-unknown}" \
    "${zero_byte_metadata_primary_node_id:-unknown}"
fi
exit "$result"
