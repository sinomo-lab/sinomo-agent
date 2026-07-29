#!/usr/bin/env bash
# Run pjdfstest against a native control filesystem and the mounted SIFS FUSE
# interface. Compatibility failures are evidence in report mode, not harness
# failures; setup, control, parsing, and kernel-mount cleanup failures always
# fail the run. A worktree that SIFS cannot remove is compatibility evidence.
set -uo pipefail

readonly PJDFSTEST_REVISION_DEFAULT="ededbeb2b44929972898afb87474b0937f78a877"
readonly PJDFSTEST_PATCHSET_DEFAULT="sinomo-linux-portability-v1"
readonly EXIT_COMPATIBILITY=10
readonly EXIT_HARNESS=20

RUN_ID="${SINOMO_PJDFSTEST_RUN_ID:-pjdfstest-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
PROFILE="${SINOMO_PJDFSTEST_PROFILE:-full}"
STRICT="${SINOMO_PJDFSTEST_STRICT:-0}"
RUN_MODE="${SINOMO_PJDFSTEST_MODE:-both}"
ARTIFACT_DIR="${SINOMO_PJDFSTEST_ARTIFACT_DIR:-}"
TIMEOUT_SECONDS="${SINOMO_PJDFSTEST_TIMEOUT_SECONDS:-3600}"
MOUNT_TIMEOUT_SECONDS="${SINOMO_PJDFSTEST_MOUNT_TIMEOUT_SECONDS:-30}"
PJDFSTEST_ROOT="${SINOMO_PJDFSTEST_ROOT:-/opt/pjdfstest}"
NATIVE_ROOT="${SINOMO_PJDFSTEST_NATIVE_ROOT:-/pjdfstest-control}"
MOUNT_POINT="${SINOMO_PJDFSTEST_MOUNT_POINT:-/mnt/sinomo-pjdfstest}"
SINOMO_BIN="${SINOMO_PJDFSTEST_SINOMO_BIN:-/usr/local/bin/sinomo}"
SINOMO_DATA_ROOT="${SINOMO_DATA_DIR:-/data}"
SIFS_ROOT="${SINOMO_PJDFSTEST_SIFS_ROOT:-/}"
EXPECTED_FSTYPE="${SINOMO_PJDFSTEST_EXPECTED_FSTYPE:-fuse}"
EXPECTED_SOURCE="${SINOMO_PJDFSTEST_EXPECTED_SOURCE:-sinomo-sifs}"
PJDFSTEST_REVISION="${SINOMO_PJDFSTEST_REVISION:-$PJDFSTEST_REVISION_DEFAULT}"
PJDFSTEST_PATCHSET="${SINOMO_PJDFSTEST_PATCHSET:-$PJDFSTEST_PATCHSET_DEFAULT}"
PROFILE_DIR="${SINOMO_PJDFSTEST_PROFILE_DIR:-/usr/local/share/sinomo/pjdfstest}"
ZERO_BYTE_PREFIX="${SINOMO_PJDFSTEST_ZERO_BYTE_PREFIX:-}"
ZERO_BYTE_MOUNT_NODE_ID="${SINOMO_PJDFSTEST_ZERO_BYTE_MOUNT_NODE_ID:-}"
ZERO_BYTE_METADATA_PRIMARY_NODE_ID="${SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_PRIMARY_NODE_ID:-}"
ZERO_BYTE_METADATA_SHARD_ID="${SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_SHARD_ID:-}"
ZERO_BYTE_METADATA_SHARD_EPOCH="${SINOMO_PJDFSTEST_ZERO_BYTE_METADATA_SHARD_EPOCH:-}"

ACTION="run"
HARNESS_STATUS="failed"
HARNESS_REASON="not_started"
COMPATIBILITY_STATUS="not_run"
CLEANUP_STATUS="not_run"
SIFS_WORKTREE_CLEANUP_STATUS="not_run"
CONTROL_PROVE_EXIT=-1
SIFS_PROVE_EXIT=-1
CONTROL_TIMED_OUT=0
SIFS_TIMED_OUT=0
CONTROL_FILES_TOTAL=0
CONTROL_FILES_PASSED=0
CONTROL_FILES_FAILED=0
CONTROL_FILES_SKIPPED=0
CONTROL_FILES_UNSUPPORTED=0
CONTROL_FILES_UNKNOWN=0
CONTROL_ASSERTIONS_TOTAL=0
CONTROL_ASSERTIONS_PASSED=0
CONTROL_ASSERTIONS_FAILED=0
CONTROL_ASSERTIONS_SKIPPED=0
CONTROL_ASSERTIONS_UNRESOLVED_DIRECTIVES=0
SIFS_FILES_TOTAL=0
SIFS_FILES_PASSED=0
SIFS_FILES_FAILED=0
SIFS_FILES_SKIPPED=0
SIFS_FILES_UNSUPPORTED=0
SIFS_FILES_UNKNOWN=0
SIFS_ASSERTIONS_TOTAL=0
SIFS_ASSERTIONS_PASSED=0
SIFS_ASSERTIONS_FAILED=0
SIFS_ASSERTIONS_SKIPPED=0
SIFS_ASSERTIONS_UNRESOLVED_DIRECTIVES=0
SENTINEL_PATH=""
SENTINEL_PAYLOAD=""
SENTINEL_STATUS="not_run"
ZERO_BYTE_O_CREAT_PATH=""
ZERO_BYTE_O_EXCL_PATH=""
ZERO_BYTE_O_CREAT_FIRST_RESULT="not_run"
ZERO_BYTE_O_CREAT_FIRST_EXIT=-1
ZERO_BYTE_O_CREAT_REPEAT_RESULT="not_run"
ZERO_BYTE_O_CREAT_REPEAT_EXIT=-1
ZERO_BYTE_O_CREAT_SIZE_RESULT="not_run"
ZERO_BYTE_O_CREAT_SIZE_EXIT=-1
ZERO_BYTE_O_EXCL_FIRST_RESULT="not_run"
ZERO_BYTE_O_EXCL_FIRST_EXIT=-1
ZERO_BYTE_O_EXCL_REPEAT_RESULT="not_run"
ZERO_BYTE_O_EXCL_REPEAT_EXIT=-1
ZERO_BYTE_O_EXCL_SIZE_RESULT="not_run"
ZERO_BYTE_O_EXCL_SIZE_EXIT=-1
ZERO_BYTE_ROUTED_WRITE_STATUS="not_run"

MOUNT_PID=""
MOUNT_STARTED=0
SIFS_WORK_DIR=""
NATIVE_WORK_DIR=""
SELECTED_TESTS_FILE=""
FINALIZED=0
IN_EXIT_TRAP=0

usage() {
  cat <<'EOF'
Usage: sinomo-pjdfstest-runner [options]

Runs a native-filesystem control followed by pjdfstest through the SIFS FUSE
mount. By default, SIFS incompatibilities are reported but do not fail the
harness. Harness, native-control, mount, parser, and kernel-mount cleanup
failures are always non-zero.

Options:
  --run-id ID              Stable run identity
  --profile full|smoke     Test selection (default: full)
  --strict                 Fail when SIFS compatibility tests fail or time out
  --report                 Report compatibility failures without failing
  --mode both|native|sifs  Select legs (default: both; sifs is diagnostic)
  --artifact-dir PATH      Artifact destination (default: /tmp/<run-id>)
  --timeout SECONDS        Per-leg prove deadline (default: 3600)
  --mount-point PATH       SIFS FUSE mount point
  --native-root PATH       Native control volume root
  --pjdfstest-root PATH    Built pjdfstest checkout
  --sinomo-bin PATH        Sinomo executable
  --cleanup-only           Unmount only the configured exact mount point
  --self-test              Exercise the compatibility-log parser without Docker
  -h, --help               Show this help

Environment equivalents use the SINOMO_PJDFSTEST_* names documented by each
option. SIFS mode additionally requires the cluster wrapper's verified
SINOMO_PJDFSTEST_ZERO_BYTE_{PREFIX,MOUNT_NODE_ID,METADATA_PRIMARY_NODE_ID,
METADATA_SHARD_ID,METADATA_SHARD_EPOCH} placement contract. result.env and
compatibility.json are the stable machine interfaces.
EOF
}

log() {
  printf '[pjdfstest %s] %s\n' "$RUN_ID" "$*"
}

fail_reason() {
  HARNESS_STATUS="failed"
  HARNESS_REASON="$1"
  return 1
}

compatibility_exit_code() {
  local requested_exit="$1" strict="$2" compatibility_status="$3"
  if [ "$requested_exit" -eq 0 ] \
    && [ "$strict" -eq 1 ] \
    && [ "$compatibility_status" != "compatible" ] \
    && [ "$compatibility_status" != "not_run" ]; then
    printf '%s\n' "$EXIT_COMPATIBILITY"
  else
    printf '%s\n' "$requested_exit"
  fi
}

unresolved_directives_gate() {
  local count="$1"
  [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -eq 0 ]
}

is_positive_integer() {
  [[ "$1" =~ ^[0-9]+$ ]] && [ "$1" -gt 0 ]
}

validate_run_id() {
  [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$ ]]
}

require_option_value() {
  local option="$1"
  local remaining="$2"
  if [ "$remaining" -lt 2 ]; then
    printf 'missing value for %s\n' "$option" >&2
    exit "$EXIT_HARNESS"
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --run-id)
      require_option_value "$1" "$#"
      RUN_ID="$2"
      shift 2
      ;;
    --profile)
      require_option_value "$1" "$#"
      PROFILE="$2"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    --report)
      STRICT=0
      shift
      ;;
    --mode)
      require_option_value "$1" "$#"
      RUN_MODE="$2"
      shift 2
      ;;
    --artifact-dir)
      require_option_value "$1" "$#"
      ARTIFACT_DIR="$2"
      shift 2
      ;;
    --timeout)
      require_option_value "$1" "$#"
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --mount-point)
      require_option_value "$1" "$#"
      MOUNT_POINT="$2"
      shift 2
      ;;
    --native-root)
      require_option_value "$1" "$#"
      NATIVE_ROOT="$2"
      shift 2
      ;;
    --pjdfstest-root)
      require_option_value "$1" "$#"
      PJDFSTEST_ROOT="$2"
      shift 2
      ;;
    --sinomo-bin)
      require_option_value "$1" "$#"
      SINOMO_BIN="$2"
      shift 2
      ;;
    --cleanup-only)
      ACTION="cleanup"
      shift
      ;;
    --self-test)
      ACTION="self-test"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2
      usage >&2
      exit "$EXIT_HARNESS"
      ;;
  esac
done

if [ -z "$ARTIFACT_DIR" ]; then
  ARTIFACT_DIR="/tmp/sinomo-$RUN_ID"
fi

mount_record() {
  findmnt -rn -M "$MOUNT_POINT" -o TARGET,FSTYPE,SOURCE,OPTIONS 2>/dev/null
}

mount_exists() {
  [ -n "$(mount_record)" ]
}

mount_has_expected_identity() {
  local record fstype source
  record="$(mount_record)"
  [ -n "$record" ] || return 1
  fstype="$(printf '%s\n' "$record" | awk '{print $2}')"
  source="$(printf '%s\n' "$record" | awk '{print $3}')"
  [ "$fstype" = "$EXPECTED_FSTYPE" ] && [ "$source" = "$EXPECTED_SOURCE" ]
}

safe_mount_target() {
  case "$1" in
    */) return 1 ;;
  esac
  case "$1" in
    /*/*) ;;
    *) return 1 ;;
  esac
  case "$1" in
    /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/mnt|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/var)
      return 1
      ;;
  esac
}

safe_owned_root() {
  case "$1" in
    /*) ;;
    *) return 1 ;;
  esac
  case "$1" in
    */) return 1 ;;
  esac
  case "$1" in
    /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/mnt|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/var)
      return 1
      ;;
  esac
}

safe_zero_byte_prefix() {
  case "$1" in
    /cluster-tests/pjdfstest-zero-byte|\
    /services/cluster-tests/pjdfstest-zero-byte|\
    /users/cluster-tests/pjdfstest-zero-byte)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

remove_exact_work_dir() {
  local base="$1"
  local path="$2"
  [ -n "$path" ] || return 0
  case "$path" in
    "$base"/*) ;;
    *)
      log "refusing to remove work path outside $base: $path"
      return 1
      ;;
  esac
  [ "$path" != "$base" ] || return 1
  [ ! -e "$path" ] || timeout --foreground 30s rm -rf -- "$path"
}

stop_mount_process() {
  local waited=0
  [ -n "$MOUNT_PID" ] || return 0
  if kill -0 "$MOUNT_PID" 2>/dev/null; then
    kill -TERM "$MOUNT_PID" 2>/dev/null || true
    while kill -0 "$MOUNT_PID" 2>/dev/null && [ "$waited" -lt 50 ]; do
      sleep 0.1
      waited=$((waited + 1))
    done
    if kill -0 "$MOUNT_PID" 2>/dev/null; then
      kill -KILL "$MOUNT_PID" 2>/dev/null || true
    fi
  fi
  wait "$MOUNT_PID" 2>/dev/null || true
  MOUNT_PID=""
}

unmount_exact_mount() {
  local waited=0
  if ! mount_exists; then
    return 0
  fi
  if ! mount_has_expected_identity; then
    log "refusing to unmount unexpected filesystem: $(mount_record)"
    return 1
  fi

  if command -v fusermount3 >/dev/null 2>&1; then
    timeout --foreground 15s fusermount3 -u "$MOUNT_POINT" >/dev/null 2>&1 || true
  elif command -v fusermount >/dev/null 2>&1; then
    timeout --foreground 15s fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true
  fi
  if mount_exists; then
    timeout --foreground 15s umount "$MOUNT_POINT" >/dev/null 2>&1 || true
  fi
  while mount_exists && [ "$waited" -lt 50 ]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  ! mount_exists
}

cleanup_runtime() {
  local cleanup_ok=0

  if [ "$MOUNT_STARTED" -eq 1 ]; then
    if [ -n "$SIFS_WORK_DIR" ] && mount_exists; then
      if remove_exact_work_dir "$MOUNT_POINT" "$SIFS_WORK_DIR"; then
        SIFS_WORKTREE_CLEANUP_STATUS="passed"
      else
        # Failed namespace cleanup through SIFS is part of the compatibility
        # boundary, not a leaked kernel mount. The default cluster wrapper
        # removes its isolated data volumes after retaining this evidence.
        SIFS_WORKTREE_CLEANUP_STATUS="failed"
        if [ "$COMPATIBILITY_STATUS" = "compatible" ]; then
          COMPATIBILITY_STATUS="incompatible"
        fi
      fi
    fi
    if ! unmount_exact_mount; then
      log "configured mount point remains mounted: $MOUNT_POINT"
      cleanup_ok=1
    fi
  elif mount_exists; then
    log "leaving pre-existing mount untouched: $(mount_record)"
    cleanup_ok=1
  fi
  stop_mount_process
  if [ "$MOUNT_STARTED" -eq 1 ] && mount_exists; then
    cleanup_ok=1
  fi
  if [ -n "$NATIVE_WORK_DIR" ]; then
    remove_exact_work_dir "$NATIVE_ROOT" "$NATIVE_WORK_DIR" || cleanup_ok=1
  fi

  if [ "$cleanup_ok" -eq 0 ]; then
    CLEANUP_STATUS="passed"
    return 0
  fi
  CLEANUP_STATUS="failed"
  return 1
}

write_result_env() {
  local path="$ARTIFACT_DIR/result.env"
  mkdir -p "$ARTIFACT_DIR" || return 1
  {
    printf 'schema_version=2\n'
    printf 'run_id=%q\n' "$RUN_ID"
    printf 'profile=%q\n' "$PROFILE"
    printf 'mode=%q\n' "$RUN_MODE"
    printf 'pjdfstest_revision=%q\n' "$PJDFSTEST_REVISION"
    printf 'pjdfstest_patchset=%q\n' "$PJDFSTEST_PATCHSET"
    printf 'harness_status=%q\n' "$HARNESS_STATUS"
    printf 'harness_reason=%q\n' "$HARNESS_REASON"
    printf 'compatibility_status=%q\n' "$COMPATIBILITY_STATUS"
    printf 'cleanup_status=%q\n' "$CLEANUP_STATUS"
    printf 'sifs_worktree_cleanup_status=%q\n' "$SIFS_WORKTREE_CLEANUP_STATUS"
    printf 'strict=%s\n' "$STRICT"
    printf 'control_prove_exit=%s\n' "$CONTROL_PROVE_EXIT"
    printf 'control_timed_out=%s\n' "$CONTROL_TIMED_OUT"
    printf 'control_files_total=%s\n' "$CONTROL_FILES_TOTAL"
    printf 'control_files_passed=%s\n' "$CONTROL_FILES_PASSED"
    printf 'control_files_failed=%s\n' "$CONTROL_FILES_FAILED"
    printf 'control_files_skipped=%s\n' "$CONTROL_FILES_SKIPPED"
    printf 'control_files_unsupported=%s\n' "$CONTROL_FILES_UNSUPPORTED"
    printf 'control_files_unknown=%s\n' "$CONTROL_FILES_UNKNOWN"
    printf 'control_assertions_total=%s\n' "$CONTROL_ASSERTIONS_TOTAL"
    printf 'control_assertions_passed=%s\n' "$CONTROL_ASSERTIONS_PASSED"
    printf 'control_assertions_failed=%s\n' "$CONTROL_ASSERTIONS_FAILED"
    printf 'control_assertions_skipped=%s\n' "$CONTROL_ASSERTIONS_SKIPPED"
    printf 'control_assertions_unresolved_directives=%s\n' \
      "$CONTROL_ASSERTIONS_UNRESOLVED_DIRECTIVES"
    printf 'sifs_prove_exit=%s\n' "$SIFS_PROVE_EXIT"
    printf 'sifs_timed_out=%s\n' "$SIFS_TIMED_OUT"
    printf 'sifs_files_total=%s\n' "$SIFS_FILES_TOTAL"
    printf 'sifs_files_passed=%s\n' "$SIFS_FILES_PASSED"
    printf 'sifs_files_failed=%s\n' "$SIFS_FILES_FAILED"
    printf 'sifs_files_skipped=%s\n' "$SIFS_FILES_SKIPPED"
    printf 'sifs_files_unsupported=%s\n' "$SIFS_FILES_UNSUPPORTED"
    printf 'sifs_files_unknown=%s\n' "$SIFS_FILES_UNKNOWN"
    printf 'sifs_assertions_total=%s\n' "$SIFS_ASSERTIONS_TOTAL"
    printf 'sifs_assertions_passed=%s\n' "$SIFS_ASSERTIONS_PASSED"
    printf 'sifs_assertions_failed=%s\n' "$SIFS_ASSERTIONS_FAILED"
    printf 'sifs_assertions_skipped=%s\n' "$SIFS_ASSERTIONS_SKIPPED"
    printf 'sifs_assertions_unresolved_directives=%s\n' \
      "$SIFS_ASSERTIONS_UNRESOLVED_DIRECTIVES"
    printf 'sentinel_path=%q\n' "$SENTINEL_PATH"
    printf 'sentinel_payload=%q\n' "$SENTINEL_PAYLOAD"
    printf 'sentinel_status=%q\n' "$SENTINEL_STATUS"
    printf 'zero_byte_routed_write_status=%q\n' "$ZERO_BYTE_ROUTED_WRITE_STATUS"
    printf 'zero_byte_o_creat_path=%q\n' "$ZERO_BYTE_O_CREAT_PATH"
    printf 'zero_byte_o_excl_path=%q\n' "$ZERO_BYTE_O_EXCL_PATH"
    printf 'zero_byte_o_creat_first_exit=%s\n' "$ZERO_BYTE_O_CREAT_FIRST_EXIT"
    printf 'zero_byte_o_creat_repeat_exit=%s\n' "$ZERO_BYTE_O_CREAT_REPEAT_EXIT"
    printf 'zero_byte_o_creat_size_exit=%s\n' "$ZERO_BYTE_O_CREAT_SIZE_EXIT"
    printf 'zero_byte_o_excl_first_exit=%s\n' "$ZERO_BYTE_O_EXCL_FIRST_EXIT"
    printf 'zero_byte_o_excl_repeat_exit=%s\n' "$ZERO_BYTE_O_EXCL_REPEAT_EXIT"
    printf 'zero_byte_o_excl_size_exit=%s\n' "$ZERO_BYTE_O_EXCL_SIZE_EXIT"
    printf 'zero_byte_mount_node_id=%q\n' "$ZERO_BYTE_MOUNT_NODE_ID"
    printf 'zero_byte_metadata_primary_node_id=%q\n' \
      "$ZERO_BYTE_METADATA_PRIMARY_NODE_ID"
    printf 'zero_byte_metadata_shard_id=%q\n' "$ZERO_BYTE_METADATA_SHARD_ID"
    printf 'zero_byte_metadata_shard_epoch=%q\n' "$ZERO_BYTE_METADATA_SHARD_EPOCH"
    printf 'zero_byte_remote_metadata_primary=%s\n' "$(
      if [ -n "$ZERO_BYTE_MOUNT_NODE_ID" ] \
        && [ "$ZERO_BYTE_MOUNT_NODE_ID" != "$ZERO_BYTE_METADATA_PRIMARY_NODE_ID" ]; then
        printf 1
      else
        printf 0
      fi
    )"
    printf 'zero_byte_retry_contract=%q\n' \
      "repeated O_CREAT plus repeated O_EXCL with fresh kernel request identities"
    printf 'artifact_dir=%q\n' "$ARTIFACT_DIR"
  } > "$path"
}

write_compatibility_json() {
  local output="$ARTIFACT_DIR/compatibility.json"
  local control_summary="$ARTIFACT_DIR/control/summary.json"
  local sifs_summary="$ARTIFACT_DIR/sifs/summary.json"
  local zero_byte_summary="$ARTIFACT_DIR/sifs/zero-byte-routed-write.json"
  local portability_contract="$PJDFSTEST_ROOT/SINOMO_PORTABILITY.json"
  perl -MJSON::PP - "$output" "$control_summary" "$sifs_summary" \
    "$portability_contract" "$zero_byte_summary" \
    "$RUN_ID" "$PROFILE" "$RUN_MODE" "$PJDFSTEST_REVISION" "$PJDFSTEST_PATCHSET" \
    "$HARNESS_STATUS" "$HARNESS_REASON" "$COMPATIBILITY_STATUS" \
    "$CLEANUP_STATUS" "$SIFS_WORKTREE_CLEANUP_STATUS" "$STRICT" \
    "$SENTINEL_PATH" "$SENTINEL_PAYLOAD" "$SENTINEL_STATUS" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my (
    $output, $control_path, $sifs_path, $portability_path, $zero_byte_path,
    $run_id, $profile, $mode,
    $revision, $patchset, $harness_status, $harness_reason, $compatibility_status,
    $cleanup_status, $sifs_worktree_cleanup_status, $strict,
    $sentinel_path, $sentinel_payload, $sentinel_status,
) = @ARGV;

sub load_optional_json {
    my ($path) = @_;
    return undef if !-f $path;
    open my $fh, '<', $path or die "open $path: $!";
    local $/;
    my $body = <$fh>;
    close $fh or die "close $path: $!";
    return JSON::PP->new->decode($body);
}

my $document = {
    schema_version => 2,
    suite => 'pjdfstest',
    pjdfstest_revision => $revision,
    pjdfstest_patchset => $patchset,
    portability_contract => load_optional_json($portability_path),
    run_id => $run_id,
    profile => $profile,
    mode => $mode,
    strict => $strict ? JSON::PP::true : JSON::PP::false,
    harness => {
        status => $harness_status,
        reason => $harness_reason,
        cleanup_status => $cleanup_status,
        sifs_worktree_cleanup_status => $sifs_worktree_cleanup_status,
    },
    compatibility => {
        status => $compatibility_status,
        claim => 'pjdfstest compatibility for the executed Linux mounted-interface subset',
        native_control => load_optional_json($control_path),
        sifs => load_optional_json($sifs_path),
    },
    zero_byte_routed_write => load_optional_json($zero_byte_path),
    sentinel => {
        dfs_path => $sentinel_path,
        payload => $sentinel_payload,
        status => $sentinel_status,
    },
};

open my $out, '>', $output or die "open $output: $!";
print {$out} JSON::PP->new->ascii->canonical->pretty->encode($document);
close $out or die "close $output: $!";
PERL
}

write_result_artifacts() {
  write_result_env || return 1
  write_compatibility_json || return 1
}

on_signal() {
  HARNESS_STATUS="failed"
  HARNESS_REASON="runner_interrupted"
  exit "$EXIT_HARNESS"
}

on_exit() {
  local status=$?
  [ "$IN_EXIT_TRAP" -eq 0 ] || exit "$status"
  IN_EXIT_TRAP=1
  trap - EXIT INT TERM HUP

  if [ "$FINALIZED" -eq 0 ]; then
    if ! cleanup_runtime; then
      HARNESS_STATUS="failed"
      HARNESS_REASON="cleanup_failed"
      status="$EXIT_HARNESS"
    fi
    write_result_artifacts >/dev/null 2>&1 || true
  fi
  exit "$status"
}

trap on_exit EXIT
trap on_signal INT TERM HUP

summarize_prove() {
  local log_path="$1"
  local selected_path="$2"
  local json_path="$3"
  local env_path="$4"
  local label="$5"
  local prove_exit="$6"
  local timed_out="$7"
  local suite_root="$8"

  perl -MJSON::PP - "$log_path" "$selected_path" "$json_path" "$env_path" \
    "$label" "$prove_exit" "$timed_out" "$suite_root" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my (
    $log_path, $selected_path, $json_path, $env_path, $label,
    $prove_exit, $timed_out, $suite_root,
) = @ARGV;

open my $selected_fh, '<', $selected_path or die "open $selected_path: $!";
chomp(my @selected = grep { length } <$selected_fh>);
close $selected_fh or die "close $selected_path: $!";
die "no selected tests" if !@selected;

my %selected = map { $_ => 1 } @selected;
my %status = map { $_ => 'unknown' } @selected;
my %tap;
my %unresolved_platform_directives = map { $_ => [] } @selected;
my %assertions = map {
    $_ => {
        total => 0,
        passed => 0,
        failed => 0,
        skipped => 0,
        unresolved_directives => 0,
    }
} @selected;
my $current;

sub resolve_test {
    my ($reported) = @_;
    return $reported if exists $selected{$reported};
    my @matches = grep {
        length($_) >= length($reported)
            && substr($_, -length($reported)) eq $reported
    } @selected;
    return @matches == 1 ? $matches[0] : undef;
}

sub record_assertion {
    my ($test, $line) = @_;
    return if $line !~ /^((?:not\s+)?ok)\s+(\d+)\b(.*)$/i;
    my ($result, $number, $tail) = (lc($1), 0 + $2, $3);
    my $counts = $assertions{$test};
    $counts->{total}++;
    if ($tail =~ /#\s*TODO\b\s*(.*?)\s*$/i) {
        $counts->{unresolved_directives}++;
        push @{ $unresolved_platform_directives{$test} }, {
            assertion_number => $number,
            result => $result,
            reason => $1,
        };
    } elsif ($tail =~ /#\s*SKIP\b/i) {
        $counts->{skipped}++;
    } elsif ($result =~ /^not/) {
        $counts->{failed}++;
    } else {
        $counts->{passed}++;
    }
}

open my $log_fh, '<', $log_path or die "open $log_path: $!";
while (my $line = <$log_fh>) {
    chomp $line;
    $line =~ s/\r$//;
    $line =~ s/^\[[^\]]+\]\s+//;

    if ($line =~ /^(\S+\.t)\s+\.{2,}(?:\s+(.*))?$/) {
        my ($reported, $tail) = ($1, defined($2) ? $2 : '');
        $current = resolve_test($reported);
        if (defined($current) && $tail =~ /skipped/i) {
            $status{$current} = 'skipped';
            $current = undef;
        } elsif (defined($current) && $tail =~ /^ok\b/i) {
            $status{$current} = 'passed';
            $current = undef;
        } elsif (defined($current) && $tail =~ /(?:dubious|fail)/i) {
            $status{$current} = 'failed';
            $current = undef;
        }
        next;
    }

    next if !defined($current);
    if ($line eq 'ok' || $line =~ /^ok\s+\d+\s+ms\b/) {
        $status{$current} = 'passed';
        $current = undef;
    } elsif ($line =~ /^(?:Dubious|Failed|Result:\s+FAIL)/i) {
        $status{$current} = 'failed';
        $current = undef;
    } elsif ($line =~ /^All tests skipped/i) {
        $status{$current} = 'skipped';
        $current = undef;
    } elsif ($line =~ /^(?:1\.\.\d+(?:\s+#.*)?|(?:not\s+)?ok\s+\d+\b.*)$/) {
        push @{ $tap{$current} }, $line;
        record_assertion($current, $line);
    }
}
close $log_fh or die "close $log_path: $!";

if (defined($current) && $prove_exit != 0 && !$timed_out) {
    $status{$current} = 'failed';
}

for my $test (@selected) {
    next if $status{$test} ne 'passed';
    my @tap = @{ $tap{$test} // [] };
    next if @tap != 2 || $tap[0] ne '1..1' || $tap[1] ne 'ok 1';
    open my $source, '<', $test or next;
    local $/;
    my $body = <$source>;
    close $source;
    if ($body =~ /^\s*(?:require\s+\S+|quick_exit)\s*$/m) {
        $status{$test} = 'unsupported';
        $assertions{$test} = {
            total => 0,
            passed => 0,
            failed => 0,
            skipped => 0,
            unresolved_directives => 0,
        };
        $unresolved_platform_directives{$test} = [];
    }
}

my %totals = (
    passed => 0,
    failed => 0,
    skipped => 0,
    unsupported => 0,
    unknown => 0,
);
my %groups;
my %assertion_totals = (
    total => 0,
    passed => 0,
    failed => 0,
    skipped => 0,
    unresolved_directives => 0,
);
my @tests;
for my $test (@selected) {
    my $relative = $test;
    $relative =~ s/^\Q$suite_root\E\/?//;
    my ($group) = $relative =~ m{^(?:tests/)?([^/]+)};
    $group //= 'ungrouped';
    my $test_status = $status{$test};
    $totals{$test_status}++;
    $groups{$group} //= {
        files_total => 0,
        passed => 0,
        failed => 0,
        skipped => 0,
        unsupported => 0,
        unknown => 0,
        assertions => {
            total => 0,
            passed => 0,
            failed => 0,
            skipped => 0,
            unresolved_directives => 0,
        },
    };
    $groups{$group}{files_total}++;
    $groups{$group}{$test_status}++;
    for my $counter (qw(total passed failed skipped unresolved_directives)) {
        $assertion_totals{$counter} += $assertions{$test}{$counter};
        $groups{$group}{assertions}{$counter} += $assertions{$test}{$counter};
    }
    push @tests, {
        path => $relative,
        status => $test_status,
        assertions => $assertions{$test},
        unresolved_platform_directives => $unresolved_platform_directives{$test},
    };
}

my $observed = @selected - $totals{unknown};
my $document = {
    schema_version => 2,
    leg => $label,
    prove_exit => 0 + $prove_exit,
    timed_out => $timed_out ? JSON::PP::true : JSON::PP::false,
    parser_complete => $observed == @selected
        ? JSON::PP::true
        : JSON::PP::false,
    files => {
        total => 0 + @selected,
        observed => 0 + $observed,
        %totals,
    },
    assertions => \%assertion_totals,
    groups => \%groups,
    tests => \@tests,
};

open my $json_fh, '>', $json_path or die "open $json_path: $!";
print {$json_fh} JSON::PP->new->ascii->canonical->pretty->encode($document);
close $json_fh or die "close $json_path: $!";

open my $env_fh, '>', $env_path or die "open $env_path: $!";
print {$env_fh} "files_total=" . scalar(@selected) . "\n";
print {$env_fh} "files_observed=$observed\n";
print {$env_fh} "files_passed=$totals{passed}\n";
print {$env_fh} "files_failed=$totals{failed}\n";
print {$env_fh} "files_skipped=$totals{skipped}\n";
print {$env_fh} "files_unsupported=$totals{unsupported}\n";
print {$env_fh} "files_unknown=$totals{unknown}\n";
print {$env_fh} "assertions_total=$assertion_totals{total}\n";
print {$env_fh} "assertions_passed=$assertion_totals{passed}\n";
print {$env_fh} "assertions_failed=$assertion_totals{failed}\n";
print {$env_fh} "assertions_skipped=$assertion_totals{skipped}\n";
print {$env_fh}
    "assertions_unresolved_directives=$assertion_totals{unresolved_directives}\n";
print {$env_fh} "parser_complete=" . ($observed == @selected ? 1 : 0) . "\n";
close $env_fh or die "close $env_path: $!";
PERL
}

load_summary() {
  local label="$1"
  local summary_env="$ARTIFACT_DIR/$label/summary.env"
  local files_total files_observed files_passed files_failed
  local files_skipped files_unsupported files_unknown parser_complete
  local assertions_total assertions_passed assertions_failed
  local assertions_skipped assertions_unresolved_directives
  local captured_result captured_exit
  # The file is generated locally by summarize_prove and contains integer-only
  # assignments. Keep it out of the stable artifact contract.
  source "$summary_env"

  if [ "$label" = "control" ]; then
    CONTROL_FILES_TOTAL="$files_total"
    CONTROL_FILES_PASSED="$files_passed"
    CONTROL_FILES_FAILED="$files_failed"
    CONTROL_FILES_SKIPPED="$files_skipped"
    CONTROL_FILES_UNSUPPORTED="$files_unsupported"
    CONTROL_FILES_UNKNOWN="$files_unknown"
    CONTROL_ASSERTIONS_TOTAL="$assertions_total"
    CONTROL_ASSERTIONS_PASSED="$assertions_passed"
    CONTROL_ASSERTIONS_FAILED="$assertions_failed"
    CONTROL_ASSERTIONS_SKIPPED="$assertions_skipped"
    CONTROL_ASSERTIONS_UNRESOLVED_DIRECTIVES="$assertions_unresolved_directives"
  else
    SIFS_FILES_TOTAL="$files_total"
    SIFS_FILES_PASSED="$files_passed"
    SIFS_FILES_FAILED="$files_failed"
    SIFS_FILES_SKIPPED="$files_skipped"
    SIFS_FILES_UNSUPPORTED="$files_unsupported"
    SIFS_FILES_UNKNOWN="$files_unknown"
    SIFS_ASSERTIONS_TOTAL="$assertions_total"
    SIFS_ASSERTIONS_PASSED="$assertions_passed"
    SIFS_ASSERTIONS_FAILED="$assertions_failed"
    SIFS_ASSERTIONS_SKIPPED="$assertions_skipped"
    SIFS_ASSERTIONS_UNRESOLVED_DIRECTIVES="$assertions_unresolved_directives"
  fi

  [ "$parser_complete" -eq 1 ]
}

select_tests() {
  local tests_dir="$PJDFSTEST_ROOT/tests"
  local profile_path="$PROFILE_DIR/smoke.tests"
  local line test_path
  SELECTED_TESTS_FILE="$ARTIFACT_DIR/selected-tests.txt"
  mkdir -p "$ARTIFACT_DIR"

  case "$PROFILE" in
    full)
      find "$tests_dir" -type f -name '*.t' -print | LC_ALL=C sort > "$SELECTED_TESTS_FILE"
      ;;
    smoke)
      if [ ! -r "$profile_path" ]; then
        profile_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/pjdfstest/smoke.tests"
      fi
      [ -r "$profile_path" ] || return 1
      : > "$SELECTED_TESTS_FILE"
      while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
          ""|\#*) continue ;;
        esac
        test_path="$PJDFSTEST_ROOT/$line"
        [ -f "$test_path" ] || {
          log "smoke profile test is missing: $test_path"
          return 1
        }
        printf '%s\n' "$test_path" >> "$SELECTED_TESTS_FILE"
      done < "$profile_path"
      ;;
    *)
      return 1
      ;;
  esac
  [ -s "$SELECTED_TESTS_FILE" ]
}

prepare_work_dir() {
  local base="$1"
  local work="$2"
  mkdir -p "$base" "$work" || return 1
  chmod 0777 "$base" "$work" || return 1
}

run_prove_leg() {
  local label="$1"
  local work_dir="$2"
  local leg_dir="$ARTIFACT_DIR/$label"
  local log_path="$leg_dir/prove.log"
  local summary_json="$leg_dir/summary.json"
  local summary_env="$leg_dir/summary.env"
  local -a tests=()
  local prove_exit timed_out=0

  mkdir -p "$leg_dir"
  mapfile -t tests < "$SELECTED_TESTS_FILE"
  cp "$SELECTED_TESTS_FILE" "$leg_dir/selected-tests.txt"

  log "running $label profile=$PROFILE files=${#tests[@]} timeout=${TIMEOUT_SECONDS}s"
  (
    cd "$work_dir" || exit 125
    timeout --foreground --signal=TERM --kill-after=30s "${TIMEOUT_SECONDS}s" \
      prove --norc --nocolor --timer -j1 -rv "${tests[@]}"
  ) > "$log_path" 2>&1
  prove_exit=$?
  if [ "$prove_exit" -eq 124 ] || [ "$prove_exit" -eq 137 ]; then
    timed_out=1
  fi

  summarize_prove "$log_path" "$SELECTED_TESTS_FILE" "$summary_json" \
    "$summary_env" "$label" "$prove_exit" "$timed_out" "$PJDFSTEST_ROOT" || return 1

  if [ "$label" = "control" ]; then
    CONTROL_PROVE_EXIT="$prove_exit"
    CONTROL_TIMED_OUT="$timed_out"
  else
    SIFS_PROVE_EXIT="$prove_exit"
    SIFS_TIMED_OUT="$timed_out"
  fi
  load_summary "$label"
}

wait_for_sifs_mount() {
  local started="$SECONDS"
  local record fstype source
  while [ $((SECONDS - started)) -lt "$MOUNT_TIMEOUT_SECONDS" ]; do
    if [ -n "$MOUNT_PID" ] && ! kill -0 "$MOUNT_PID" 2>/dev/null; then
      return 1
    fi
    record="$(mount_record)"
    if [ -n "$record" ]; then
      fstype="$(printf '%s\n' "$record" | awk '{print $2}')"
      source="$(printf '%s\n' "$record" | awk '{print $3}')"
      if [ "$fstype" = "$EXPECTED_FSTYPE" ] \
        && [ "$source" = "$EXPECTED_SOURCE" ]; then
        return 0
      fi
      printf '%s\n' "$record" > "$ARTIFACT_DIR/sifs/unexpected-mount.txt"
      return 1
    fi
    sleep 0.1
  done
  return 1
}

start_sifs_mount() {
  local mount_log="$ARTIFACT_DIR/sifs/mount.log"
  mkdir -p "$ARTIFACT_DIR/sifs" "$MOUNT_POINT"
  if mount_exists; then
    log "mount point is already mounted: $MOUNT_POINT"
    return 1
  fi
  if [ -n "$(find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    log "mount point is not empty: $MOUNT_POINT"
    return 1
  fi

  "$SINOMO_BIN" dfs mount \
    --root "$SIFS_ROOT" \
    --data-dir "$SINOMO_DATA_ROOT" \
    --allow-other \
    --foreground \
    "$MOUNT_POINT" > "$mount_log" 2>&1 &
  MOUNT_PID=$!
  MOUNT_STARTED=1

  if ! wait_for_sifs_mount; then
    log "SIFS mount did not become ready with source $EXPECTED_SOURCE and fstype $EXPECTED_FSTYPE"
    return 1
  fi
  {
    mount_record
    printf 'mount_pid=%s\n' "$MOUNT_PID"
    printf 'mount_point=%s\n' "$MOUNT_POINT"
    printf 'expected_fstype=%s\n' "$EXPECTED_FSTYPE"
    printf 'expected_source=%s\n' "$EXPECTED_SOURCE"
  } > "$ARTIFACT_DIR/sifs/mount-evidence.txt"
}

normalized_pjdfstest_result() {
  local output="$1" status="$2"
  if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
    printf 'timed_out_exit_%s\n' "$status"
  elif [ -n "$output" ]; then
    printf '%s\n' "$output"
  else
    printf 'command_failed_exit_%s\n' "$status"
  fi
}

capture_pjdfstest_result() {
  local result_variable="$1" exit_variable="$2"
  local output status normalized
  shift 2

  if output="$(
    timeout --foreground 30s "$PJDFSTEST_ROOT/pjdfstest" "$@"
  )"; then
    status=0
  else
    status=$?
  fi
  normalized="$(normalized_pjdfstest_result "$output" "$status")"
  printf -v "$result_variable" '%s' "$normalized"
  printf -v "$exit_variable" '%s' "$status"
}

write_sifs_sentinel() {
  local probe_dir="$MOUNT_POINT$ZERO_BYTE_PREFIX/$RUN_ID"
  local o_creat_local="$probe_dir/o-creat-empty"
  local o_excl_local="$probe_dir/o-excl-empty"
  local probe_ok=1

  SENTINEL_PATH="$ZERO_BYTE_PREFIX/$RUN_ID/o-creat-empty"
  SENTINEL_PAYLOAD=""
  ZERO_BYTE_O_CREAT_PATH="$SENTINEL_PATH"
  ZERO_BYTE_O_EXCL_PATH="$ZERO_BYTE_PREFIX/$RUN_ID/o-excl-empty"

  timeout --foreground 30s mkdir -p "$probe_dir" || probe_ok=0
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_CREAT_FIRST_RESULT ZERO_BYTE_O_CREAT_FIRST_EXIT \
      open "$o_creat_local" O_CREAT,O_WRONLY 0644
    [ "$ZERO_BYTE_O_CREAT_FIRST_EXIT" -eq 0 ] \
      && [ "$ZERO_BYTE_O_CREAT_FIRST_RESULT" = 0 ] \
      || probe_ok=0
  fi
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_CREAT_REPEAT_RESULT ZERO_BYTE_O_CREAT_REPEAT_EXIT \
      open "$o_creat_local" O_CREAT,O_WRONLY 0644
    [ "$ZERO_BYTE_O_CREAT_REPEAT_EXIT" -eq 0 ] \
      && [ "$ZERO_BYTE_O_CREAT_REPEAT_RESULT" = 0 ] \
      || probe_ok=0
  fi
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_CREAT_SIZE_RESULT ZERO_BYTE_O_CREAT_SIZE_EXIT \
      stat "$o_creat_local" size
    [ "$ZERO_BYTE_O_CREAT_SIZE_EXIT" -eq 0 ] \
      && [ "$ZERO_BYTE_O_CREAT_SIZE_RESULT" = 0 ] \
      || probe_ok=0
  fi
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_EXCL_FIRST_RESULT ZERO_BYTE_O_EXCL_FIRST_EXIT \
      create "$o_excl_local" 0644
    [ "$ZERO_BYTE_O_EXCL_FIRST_EXIT" -eq 0 ] \
      && [ "$ZERO_BYTE_O_EXCL_FIRST_RESULT" = 0 ] \
      || probe_ok=0
  fi
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_EXCL_REPEAT_RESULT ZERO_BYTE_O_EXCL_REPEAT_EXIT \
      create "$o_excl_local" 0644
    [ "$ZERO_BYTE_O_EXCL_REPEAT_EXIT" -ne 0 ] \
      && [ "$ZERO_BYTE_O_EXCL_REPEAT_RESULT" = EEXIST ] \
      || probe_ok=0
  fi
  if [ "$probe_ok" -eq 1 ]; then
    capture_pjdfstest_result \
      ZERO_BYTE_O_EXCL_SIZE_RESULT ZERO_BYTE_O_EXCL_SIZE_EXIT \
      stat "$o_excl_local" size
    [ "$ZERO_BYTE_O_EXCL_SIZE_EXIT" -eq 0 ] \
      && [ "$ZERO_BYTE_O_EXCL_SIZE_RESULT" = 0 ] \
      || probe_ok=0
  fi

  if [ "$probe_ok" -eq 1 ]; then
    ZERO_BYTE_ROUTED_WRITE_STATUS="passed"
  else
    ZERO_BYTE_ROUTED_WRITE_STATUS="failed"
  fi
  write_zero_byte_routed_write_json || return 1
  [ "$probe_ok" -eq 1 ]
}

write_zero_byte_routed_write_json() {
  local output="$ARTIFACT_DIR/sifs/zero-byte-routed-write.json"
  mkdir -p "$ARTIFACT_DIR/sifs" || return 1
  perl -MJSON::PP - "$output" \
    "$ZERO_BYTE_ROUTED_WRITE_STATUS" \
    "$ZERO_BYTE_MOUNT_NODE_ID" "$ZERO_BYTE_METADATA_PRIMARY_NODE_ID" \
    "$ZERO_BYTE_METADATA_SHARD_ID" "$ZERO_BYTE_METADATA_SHARD_EPOCH" \
    "$ZERO_BYTE_O_CREAT_PATH" "$ZERO_BYTE_O_EXCL_PATH" \
    "$ZERO_BYTE_O_CREAT_FIRST_RESULT" "$ZERO_BYTE_O_CREAT_REPEAT_RESULT" \
    "$ZERO_BYTE_O_CREAT_SIZE_RESULT" "$ZERO_BYTE_O_EXCL_FIRST_RESULT" \
    "$ZERO_BYTE_O_EXCL_REPEAT_RESULT" "$ZERO_BYTE_O_EXCL_SIZE_RESULT" \
    "$ZERO_BYTE_O_CREAT_FIRST_EXIT" "$ZERO_BYTE_O_CREAT_REPEAT_EXIT" \
    "$ZERO_BYTE_O_CREAT_SIZE_EXIT" "$ZERO_BYTE_O_EXCL_FIRST_EXIT" \
    "$ZERO_BYTE_O_EXCL_REPEAT_EXIT" "$ZERO_BYTE_O_EXCL_SIZE_EXIT" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my (
    $output, $status, $mount_node_id, $primary_node_id, $shard_id, $epoch,
    $o_creat_path, $o_excl_path, $o_creat_first, $o_creat_repeat,
    $o_creat_size, $o_excl_first, $o_excl_repeat, $o_excl_size,
    $o_creat_first_exit, $o_creat_repeat_exit, $o_creat_size_exit,
    $o_excl_first_exit, $o_excl_repeat_exit, $o_excl_size_exit,
) = @ARGV;

my $document = {
    schema_version => 1,
    status => $status,
    placement => {
        mount_node_id => $mount_node_id,
        metadata_primary_node_id => $primary_node_id,
        metadata_shard_id => $shard_id,
        metadata_shard_epoch => 0 + $epoch,
        mount_node_is_metadata_replica => JSON::PP::true,
        remote_metadata_primary => $mount_node_id ne $primary_node_id
            ? JSON::PP::true
            : JSON::PP::false,
    },
    contract => {
        ordinary_o_creat => JSON::PP::true,
        o_excl => JSON::PP::true,
        repeated_syscalls => JSON::PP::true,
        exact_operation_identity_injected => JSON::PP::false,
        exact_operation_identity_note =>
            'The mounted POSIX interface assigns a fresh kernel request identity to each syscall; exact distributed request replay is covered by the DFS integration tests.',
    },
    cases => {
        o_creat => {
            path => $o_creat_path,
            first_result => $o_creat_first,
            first_exit => 0 + $o_creat_first_exit,
            repeated_result => $o_creat_repeat,
            repeated_exit => 0 + $o_creat_repeat_exit,
            size => $o_creat_size,
            size_exit => 0 + $o_creat_size_exit,
        },
        o_excl => {
            path => $o_excl_path,
            first_result => $o_excl_first,
            first_exit => 0 + $o_excl_first_exit,
            repeated_result => $o_excl_repeat,
            repeated_exit => 0 + $o_excl_repeat_exit,
            size => $o_excl_size,
            size_exit => 0 + $o_excl_size_exit,
        },
    },
};

open my $out, '>', $output or die "open $output: $!";
print {$out} JSON::PP->new->ascii->canonical->pretty->encode($document);
close $out or die "close $output: $!";
PERL
}

run_self_test() {
  local temp_root
  local selected log_path json_path env_path
  local files_total files_observed files_passed files_failed
  local files_skipped files_unsupported files_unknown parser_complete
  local assertions_total assertions_passed assertions_failed
  local assertions_skipped assertions_unresolved_directives
  temp_root="$(mktemp -d /tmp/sinomo-pjdfstest-self-test.XXXXXX)" || return 1
  selected="$temp_root/selected.txt"
  log_path="$temp_root/prove.log"
  json_path="$temp_root/summary.json"
  env_path="$temp_root/summary.env"

  mkdir -p "$temp_root/tests/pass" "$temp_root/tests/unsupported" "$temp_root/tests/fail"
  printf '#!/bin/sh\necho "1..5"\necho "ok 1"\necho "ok 2"\necho "ok 3 # SKIP platform"\necho "not ok 4 # TODO known gap"\necho "ok 5 # TODO unexpected pass"\n' \
    > "$temp_root/tests/pass/00.t"
  printf '#!/bin/sh\nquick_exit() { echo "1..1"; echo "ok 1"; exit 0; }\nquick_exit\n' \
    > "$temp_root/tests/unsupported/00.t"
  printf '#!/bin/sh\necho "1..2"\necho "ok 1"\necho "not ok 2"\n' \
    > "$temp_root/tests/fail/00.t"
  printf '%s\n' \
    "$temp_root/tests/pass/00.t" \
    "$temp_root/tests/unsupported/00.t" \
    "$temp_root/tests/fail/00.t" > "$selected"
  chmod +x \
    "$temp_root/tests/pass/00.t" \
    "$temp_root/tests/unsupported/00.t" \
    "$temp_root/tests/fail/00.t"
  (
    cd "$temp_root" || exit 125
    prove --norc --nocolor --timer -j1 -rv \
      "$temp_root/tests/pass/00.t" \
      "$temp_root/tests/unsupported/00.t" \
      "$temp_root/tests/fail/00.t"
  ) > "$log_path" 2>&1

  if ! summarize_prove "$log_path" "$selected" "$json_path" "$env_path" \
    self-test 1 0 "$temp_root"; then
    rm -rf -- "$temp_root"
    return 1
  fi
  source "$env_path"
  if [ "$files_total" -ne 3 ] \
    || [ "$files_passed" -ne 1 ] \
    || [ "$files_failed" -ne 1 ] \
    || [ "$files_unsupported" -ne 1 ] \
    || [ "$files_unknown" -ne 0 ] \
    || [ "$assertions_total" -ne 7 ] \
    || [ "$assertions_passed" -ne 3 ] \
    || [ "$assertions_failed" -ne 1 ] \
    || [ "$assertions_skipped" -ne 1 ] \
    || [ "$assertions_unresolved_directives" -ne 2 ] \
    || [ "$parser_complete" -ne 1 ]; then
    printf 'parser self-test failed:\n' >&2
    sed -n '1,240p' "$log_path" >&2
    sed -n '1,240p' "$json_path" >&2
    rm -rf -- "$temp_root"
    return 1
  fi
  if ! perl -MJSON::PP - "$json_path" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my ($path) = @ARGV;
open my $fh, '<', $path or die "open $path: $!";
local $/;
my $summary = JSON::PP->new->decode(<$fh>);
close $fh or die "close $path: $!";

sub reject_todo_key {
    my ($value) = @_;
    if (ref($value) eq 'HASH') {
        for my $key (keys %{$value}) {
            die "generic todo key remains" if lc($key) eq 'todo';
            reject_todo_key($value->{$key});
        }
    } elsif (ref($value) eq 'ARRAY') {
        reject_todo_key($_) for @{$value};
    }
}

reject_todo_key($summary);
die "summary schema version" if $summary->{schema_version} != 2;

my $pass = $summary->{groups}{pass}{assertions};
die "pass group assertion counters" if
    $pass->{total} != 5
    || $pass->{passed} != 2
    || $pass->{failed} != 0
    || $pass->{skipped} != 1
    || $pass->{unresolved_directives} != 2;

my ($pass_test) = grep {
    $_->{path} eq 'tests/pass/00.t'
} @{ $summary->{tests} };
die "pass test missing" if !defined($pass_test);
my $directives = $pass_test->{unresolved_platform_directives};
die "unresolved directive count" if @{$directives} != 2;
die "not-ok directive evidence" if
    $directives->[0]{assertion_number} != 4
    || $directives->[0]{result} ne 'not ok'
    || $directives->[0]{reason} ne 'known gap';
die "ok directive evidence" if
    $directives->[1]{assertion_number} != 5
    || $directives->[1]{result} ne 'ok'
    || $directives->[1]{reason} ne 'unexpected pass';

my ($unsupported) = grep {
    $_->{path} eq 'tests/unsupported/00.t'
} @{ $summary->{tests} };
die "unsupported test missing" if !defined($unsupported);
die "unsupported synthetic assertion counted" if
    $unsupported->{assertions}{total} != 0
    || $unsupported->{assertions}{unresolved_directives} != 0
    || @{ $unsupported->{unresolved_platform_directives} } != 0
    || $unsupported->{status} ne 'unsupported';
PERL
  then
    printf 'assertion parser self-test failed:\n' >&2
    sed -n '1,240p' "$json_path" >&2
    rm -rf -- "$temp_root"
    return 1
  fi
  unresolved_directives_gate 0 || return 1
  ! unresolved_directives_gate 1 || return 1
  ! unresolved_directives_gate invalid || return 1
  [ "$(compatibility_exit_code 0 0 incompatible)" -eq 0 ] || return 1
  [ "$(compatibility_exit_code 0 1 incompatible)" -eq "$EXIT_COMPATIBILITY" ] || return 1
  [ "$(compatibility_exit_code 0 1 incomplete)" -eq "$EXIT_COMPATIBILITY" ] || return 1
  [ "$(compatibility_exit_code 0 1 compatible)" -eq 0 ] || return 1
  [ "$(compatibility_exit_code 0 1 not_run)" -eq 0 ] || return 1
  [ "$(compatibility_exit_code "$EXIT_HARNESS" 1 incompatible)" -eq "$EXIT_HARNESS" ] \
    || return 1
  safe_mount_target "/mnt/sinomo-pjdfstest" || return 1
  ! safe_mount_target "/mnt" || return 1
  ! safe_mount_target "/mnt/sinomo-pjdfstest/" || return 1
  safe_owned_root "/pjdfstest-control" || return 1
  safe_owned_root "/tmp/sinomo-pjdfstest-control" || return 1
  ! safe_owned_root "/" || return 1
  ! safe_owned_root "/tmp" || return 1
  ! safe_owned_root "/pjdfstest-control/" || return 1
  safe_zero_byte_prefix "/cluster-tests/pjdfstest-zero-byte" || return 1
  safe_zero_byte_prefix "/services/cluster-tests/pjdfstest-zero-byte" || return 1
  safe_zero_byte_prefix "/users/cluster-tests/pjdfstest-zero-byte" || return 1
  ! safe_zero_byte_prefix "/cluster-tests/pjdfstest-zero-byte/" || return 1
  ! safe_zero_byte_prefix "/tmp/pjdfstest-zero-byte" || return 1
  [ "$(normalized_pjdfstest_result EEXIST 1)" = EEXIST ] || return 1
  [ "$(normalized_pjdfstest_result "" 1)" = command_failed_exit_1 ] || return 1
  [ "$(normalized_pjdfstest_result EEXIST 124)" = timed_out_exit_124 ] || return 1
  mkdir -p "$temp_root/fake-pjdfstest" || return 1
  printf '#!/bin/sh\nprintf "EEXIST\\n"\nexit 1\n' \
    >"$temp_root/fake-pjdfstest/pjdfstest"
  chmod +x "$temp_root/fake-pjdfstest/pjdfstest"
  PJDFSTEST_ROOT="$temp_root/fake-pjdfstest"
  captured_result="not_run"
  captured_exit=-1
  capture_pjdfstest_result captured_result captured_exit create ignored 0644
  [ "$captured_result" = EEXIST ] || return 1
  [ "$captured_exit" -eq 1 ] || return 1
  ARTIFACT_DIR="$temp_root/artifacts"
  ZERO_BYTE_ROUTED_WRITE_STATUS="passed"
  ZERO_BYTE_MOUNT_NODE_ID="node-mount"
  ZERO_BYTE_METADATA_PRIMARY_NODE_ID="node-primary"
  ZERO_BYTE_METADATA_SHARD_ID="services"
  ZERO_BYTE_METADATA_SHARD_EPOCH="7"
  ZERO_BYTE_O_CREAT_PATH="/services/cluster-tests/pjdfstest-zero-byte/run/o-creat-empty"
  ZERO_BYTE_O_EXCL_PATH="/services/cluster-tests/pjdfstest-zero-byte/run/o-excl-empty"
  ZERO_BYTE_O_CREAT_FIRST_RESULT="0"
  ZERO_BYTE_O_CREAT_FIRST_EXIT=0
  ZERO_BYTE_O_CREAT_REPEAT_RESULT="0"
  ZERO_BYTE_O_CREAT_REPEAT_EXIT=0
  ZERO_BYTE_O_CREAT_SIZE_RESULT="0"
  ZERO_BYTE_O_CREAT_SIZE_EXIT=0
  ZERO_BYTE_O_EXCL_FIRST_RESULT="0"
  ZERO_BYTE_O_EXCL_FIRST_EXIT=0
  ZERO_BYTE_O_EXCL_REPEAT_RESULT="EEXIST"
  ZERO_BYTE_O_EXCL_REPEAT_EXIT=1
  ZERO_BYTE_O_EXCL_SIZE_RESULT="0"
  ZERO_BYTE_O_EXCL_SIZE_EXIT=0
  write_zero_byte_routed_write_json || return 1
  perl -MJSON::PP - "$ARTIFACT_DIR/sifs/zero-byte-routed-write.json" <<'PERL' \
    || return 1
use strict;
use warnings;
use JSON::PP;

my ($path) = @ARGV;
open my $fh, '<', $path or die "open $path: $!";
local $/;
my $probe = JSON::PP->new->decode(<$fh>);
close $fh or die "close $path: $!";
die "zero-byte probe status" if $probe->{status} ne 'passed';
die "remote-primary proof" if !$probe->{placement}{remote_metadata_primary};
die "metadata-replica proof" if !$probe->{placement}{mount_node_is_metadata_replica};
die "O_CREAT evidence" if
    $probe->{cases}{o_creat}{first_result} ne '0'
    || $probe->{cases}{o_creat}{first_exit} != 0
    || $probe->{cases}{o_creat}{repeated_result} ne '0'
    || $probe->{cases}{o_creat}{repeated_exit} != 0
    || $probe->{cases}{o_creat}{size} ne '0'
    || $probe->{cases}{o_creat}{size_exit} != 0;
die "O_EXCL evidence" if
    $probe->{cases}{o_excl}{first_result} ne '0'
    || $probe->{cases}{o_excl}{first_exit} != 0
    || $probe->{cases}{o_excl}{repeated_result} ne 'EEXIST'
    || $probe->{cases}{o_excl}{repeated_exit} != 1
    || $probe->{cases}{o_excl}{size} ne '0'
    || $probe->{cases}{o_excl}{size_exit} != 0;
die "exact retry overclaim" if
    $probe->{contract}{exact_operation_identity_injected};
PERL
  rm -rf -- "$temp_root"
  printf 'pjdfstest runner self-test passed\n'
}

validate_configuration() {
  if ! validate_run_id "$RUN_ID"; then
    fail_reason "invalid_run_id"
    return 1
  fi
  case "$PROFILE" in full|smoke) ;; *) fail_reason "invalid_profile"; return 1 ;; esac
  case "$RUN_MODE" in both|native|sifs) ;; *) fail_reason "invalid_mode"; return 1 ;; esac
  case "$STRICT" in 0|1) ;; *) fail_reason "invalid_strict_value"; return 1 ;; esac
  if ! is_positive_integer "$TIMEOUT_SECONDS" \
    || [ "$TIMEOUT_SECONDS" -lt 60 ] \
    || [ "$TIMEOUT_SECONDS" -gt 86400 ]; then
    fail_reason "invalid_timeout"
    return 1
  fi
  if ! is_positive_integer "$MOUNT_TIMEOUT_SECONDS"; then
    fail_reason "invalid_mount_timeout"
    return 1
  fi
  if ! safe_mount_target "$MOUNT_POINT"; then
    fail_reason "unsafe_mount_point"
    return 1
  fi
  if ! safe_owned_root "$NATIVE_ROOT"; then
    fail_reason "unsafe_native_root"
    return 1
  fi
  if [ "$RUN_MODE" != "native" ]; then
    if ! safe_zero_byte_prefix "$ZERO_BYTE_PREFIX"; then
      fail_reason "unsafe_zero_byte_prefix"
      return 1
    fi
    if [ -z "$ZERO_BYTE_MOUNT_NODE_ID" ] \
      || [ -z "$ZERO_BYTE_METADATA_PRIMARY_NODE_ID" ] \
      || [ "$ZERO_BYTE_MOUNT_NODE_ID" = "$ZERO_BYTE_METADATA_PRIMARY_NODE_ID" ]; then
      fail_reason "zero_byte_remote_primary_unproven"
      return 1
    fi
    case "$ZERO_BYTE_METADATA_SHARD_ID:$ZERO_BYTE_PREFIX" in
      root:/cluster-tests/pjdfstest-zero-byte|\
      services:/services/cluster-tests/pjdfstest-zero-byte|\
      users:/users/cluster-tests/pjdfstest-zero-byte)
        ;;
      *)
        fail_reason "zero_byte_shard_prefix_mismatch"
        return 1
        ;;
    esac
    if ! is_positive_integer "$ZERO_BYTE_METADATA_SHARD_EPOCH"; then
      fail_reason "invalid_zero_byte_shard_epoch"
      return 1
    fi
  fi
}

preflight() {
  local tool installed_revision installed_patchset
  [ "$(id -u)" -eq 0 ] || {
    fail_reason "root_required"
    return 1
  }
  for tool in find findmnt perl prove timeout awk sed; do
    command -v "$tool" >/dev/null 2>&1 || {
      fail_reason "missing_tool_$tool"
      return 1
    }
  done
  perl -MJSON::PP -e 1 >/dev/null 2>&1 || {
    fail_reason "missing_perl_json_pp"
    return 1
  }
  [ -x "$PJDFSTEST_ROOT/pjdfstest" ] || {
    fail_reason "missing_pjdfstest_binary"
    return 1
  }
  [ -d "$PJDFSTEST_ROOT/tests" ] || {
    fail_reason "missing_pjdfstest_tests"
    return 1
  }
  [ -r "$PJDFSTEST_ROOT/REVISION" ] || {
    fail_reason "missing_pjdfstest_revision"
    return 1
  }
  installed_revision="$(sed -n '1p' "$PJDFSTEST_ROOT/REVISION")"
  [ "$installed_revision" = "$PJDFSTEST_REVISION" ] || {
    fail_reason "pjdfstest_revision_mismatch"
    return 1
  }
  [ -r "$PJDFSTEST_ROOT/SINOMO_PATCHSET" ] || {
    fail_reason "missing_pjdfstest_patchset"
    return 1
  }
  installed_patchset="$(<"$PJDFSTEST_ROOT/SINOMO_PATCHSET")"
  [ "$installed_patchset" = "$PJDFSTEST_PATCHSET" ] || {
    fail_reason "pjdfstest_patchset_mismatch"
    return 1
  }
  [ -r "$PJDFSTEST_ROOT/SINOMO_PORTABILITY.json" ] || {
    fail_reason "missing_pjdfstest_portability_contract"
    return 1
  }
  if [ "$RUN_MODE" != "native" ]; then
    [ -c /dev/fuse ] || {
      fail_reason "missing_dev_fuse"
      return 1
    }
    [ -x "$SINOMO_BIN" ] || {
      fail_reason "missing_sinomo_binary"
      return 1
    }
    command -v umount >/dev/null 2>&1 || {
      fail_reason "missing_tool_umount"
      return 1
    }
  fi
}

finish() {
  local requested_exit="$1"
  if ! cleanup_runtime; then
    HARNESS_STATUS="failed"
    HARNESS_REASON="cleanup_failed"
    requested_exit="$EXIT_HARNESS"
  fi
  requested_exit="$(
    compatibility_exit_code "$requested_exit" "$STRICT" "$COMPATIBILITY_STATUS"
  )"
  if ! write_result_artifacts; then
    HARNESS_STATUS="failed"
    HARNESS_REASON="artifact_write_failed"
    requested_exit="$EXIT_HARNESS"
    write_result_env >/dev/null 2>&1 || true
  fi
  FINALIZED=1
  exit "$requested_exit"
}

main() {
  if [ "$ACTION" = "self-test" ]; then
    trap - EXIT INT TERM HUP
    run_self_test
    return
  fi

  if ! validate_run_id "$RUN_ID"; then
    log "invalid run id"
    return "$EXIT_HARNESS"
  fi

  if [ "$ACTION" = "cleanup" ]; then
    trap - EXIT INT TERM HUP
    [ "$(id -u)" -eq 0 ] || return "$EXIT_HARNESS"
    safe_mount_target "$MOUNT_POINT" || return "$EXIT_HARNESS"
    if unmount_exact_mount; then
      stop_mount_process
      log "cleanup-only: mount point is unmounted"
      return 0
    fi
    log "cleanup-only: mount point remains mounted"
    return "$EXIT_HARNESS"
  fi

  mkdir -p "$ARTIFACT_DIR" || return "$EXIT_HARNESS"
  if ! validate_configuration; then
    finish "$EXIT_HARNESS"
  fi
  if ! preflight; then
    finish "$EXIT_HARNESS"
  fi
  if ! select_tests; then
    HARNESS_REASON="test_selection_failed"
    finish "$EXIT_HARNESS"
  fi

  if [ "$RUN_MODE" = "both" ] || [ "$RUN_MODE" = "native" ]; then
    NATIVE_WORK_DIR="$NATIVE_ROOT/sinomo-pjdfstest-runs/$RUN_ID"
    if ! prepare_work_dir "$NATIVE_ROOT" "$NATIVE_WORK_DIR"; then
      HARNESS_REASON="native_work_dir_failed"
      finish "$EXIT_HARNESS"
    fi
    if ! run_prove_leg control "$NATIVE_WORK_DIR"; then
      HARNESS_REASON="native_control_failed"
      finish "$EXIT_HARNESS"
    fi
    if [ "$CONTROL_PROVE_EXIT" -ne 0 ] || [ "$CONTROL_TIMED_OUT" -ne 0 ]; then
      HARNESS_REASON="native_control_failed"
      finish "$EXIT_HARNESS"
    fi
    if ! unresolved_directives_gate "$CONTROL_ASSERTIONS_UNRESOLVED_DIRECTIVES"; then
      HARNESS_REASON="native_control_unresolved_directives"
      finish "$EXIT_HARNESS"
    fi
    if ! remove_exact_work_dir "$NATIVE_ROOT" "$NATIVE_WORK_DIR"; then
      HARNESS_REASON="native_cleanup_failed"
      finish "$EXIT_HARNESS"
    fi
    NATIVE_WORK_DIR=""
  fi

  if [ "$RUN_MODE" = "both" ] || [ "$RUN_MODE" = "sifs" ]; then
    if ! start_sifs_mount; then
      HARNESS_REASON="sifs_mount_failed"
      finish "$EXIT_HARNESS"
    fi
    SIFS_WORK_DIR="$MOUNT_POINT/.sinomo-pjdfstest-runs/$RUN_ID"
    if ! prepare_work_dir "$MOUNT_POINT/.sinomo-pjdfstest-runs" "$SIFS_WORK_DIR"; then
      HARNESS_REASON="sifs_work_dir_failed"
      finish "$EXIT_HARNESS"
    fi
    if write_sifs_sentinel; then
      SENTINEL_STATUS="passed"
    else
      SENTINEL_STATUS="failed"
    fi

    if ! run_prove_leg sifs "$SIFS_WORK_DIR"; then
      # A timed-out SIFS run is intentionally parse-incomplete compatibility
      # evidence. Any other parser failure is a harness failure.
      if [ "$SIFS_TIMED_OUT" -eq 0 ]; then
        HARNESS_REASON="sifs_result_parse_failed"
        finish "$EXIT_HARNESS"
      fi
    fi
    if [ "$SIFS_TIMED_OUT" -eq 0 ] \
      && ! unresolved_directives_gate "$SIFS_ASSERTIONS_UNRESOLVED_DIRECTIVES"; then
      HARNESS_REASON="sifs_unresolved_directives"
      finish "$EXIT_HARNESS"
    fi

    if [ "$SIFS_TIMED_OUT" -ne 0 ] || [ "$SIFS_FILES_UNKNOWN" -ne 0 ]; then
      COMPATIBILITY_STATUS="incomplete"
    elif [ "$SIFS_PROVE_EXIT" -ne 0 ] || [ "$SIFS_FILES_FAILED" -ne 0 ]; then
      COMPATIBILITY_STATUS="incompatible"
    else
      COMPATIBILITY_STATUS="compatible"
    fi

    if ! mount_exists; then
      COMPATIBILITY_STATUS="incomplete"
    fi
    if [ "$SENTINEL_STATUS" = "failed" ] \
      && [ "$COMPATIBILITY_STATUS" = "compatible" ]; then
      COMPATIBILITY_STATUS="incompatible"
    fi
  else
    COMPATIBILITY_STATUS="not_run"
  fi

  HARNESS_STATUS="passed"
  HARNESS_REASON="completed"
  finish 0
}

main "$@"
