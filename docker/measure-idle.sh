#!/usr/bin/env bash
# Measure per-container idle disk + network I/O over a window, and per-daemon
# read/write syscall + block bytes from /proc/<pid>/io. Targets: 0 disk write,
# <10 KB/s disk read, <10 KB/s network during idle.
#
#   docker/measure-idle.sh [window_seconds]   (default 60)

# These helpers target the dev cluster compose file regardless of the caller's
# working directory (the root docker-compose.yml is the production node).
SINOMO_DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${COMPOSE_FILE:-$SINOMO_DOCKER_DIR/docker-compose.dev.yml}"

set -uo pipefail
cd "$(dirname "$0")/.."
WINDOW="${1:-60}"
NODES="${SINOMO_IDLE_NODES:-node1 node2 node3 node4 node5 node6}"
MAX_READ_BLOCK_BPS="${SINOMO_IDLE_MAX_READ_BLOCK_BPS:-10240}"
MAX_WRITE_BLOCK_BPS="${SINOMO_IDLE_MAX_WRITE_BLOCK_BPS:-0}"
MAX_WRITE_BLOCK_BYTES="${SINOMO_IDLE_MAX_WRITE_BLOCK_BYTES:-0}"
MAX_RX_NET_BPS="${SINOMO_IDLE_MAX_RX_NET_BPS:-10240}"
MAX_TX_NET_BPS="${SINOMO_IDLE_MAX_TX_NET_BPS:-10240}"

case "$WINDOW" in
  ''|*[!0-9]*)
    echo "window_seconds must be a positive integer"
    exit 2
    ;;
esac
if [ "$WINDOW" -le 0 ]; then
  echo "window_seconds must be greater than 0"
  exit 2
fi
case "$MAX_WRITE_BLOCK_BYTES" in
  ''|*[!0-9]*)
    echo "SINOMO_IDLE_MAX_WRITE_BLOCK_BYTES must be a non-negative integer"
    exit 2
    ;;
esac

# Sum read_bytes/write_bytes/rchar/wchar across the main+shadow daemon pids in a node.
node_io() {
  docker compose exec -T "$1" sh -c '
    total_r=0; total_w=0; total_rc=0; total_wc=0
    for pid in $(pgrep -x sinomo 2>/dev/null || pgrep sinomo 2>/dev/null); do
      io=/proc/$pid/io
      [ -r "$io" ] || continue
      rc=$(awk "/^rchar:/{print \$2}" "$io"); wc=$(awk "/^wchar:/{print \$2}" "$io")
      rb=$(awk "/^read_bytes:/{print \$2}" "$io"); wb=$(awk "/^write_bytes:/{print \$2}" "$io")
      total_rc=$((total_rc + ${rc:-0})); total_wc=$((total_wc + ${wc:-0}))
      total_r=$((total_r + ${rb:-0})); total_w=$((total_w + ${wb:-0}))
    done
    echo "$total_rc $total_wc $total_r $total_w"
  ' 2>/dev/null
}

sample_node_io() {
  local out
  if ! out="$(node_io "$1")" || [ -z "$out" ]; then
    return 1
  fi
  set -- $out
  [ "$#" -eq 4 ] || return 1
  printf '%s %s %s %s\n' "$1" "$2" "$3" "$4"
}

node_net() {
  docker compose exec -T "$1" sh -c '
    awk "NR > 2 {
      iface=\$1
      sub(\":\", \"\", iface)
      if (iface != \"lo\") {
        rx += \$2
        tx += \$10
      }
    } END { printf \"%d %d\\n\", rx, tx }" /proc/net/dev
  ' 2>/dev/null
}

sample_node_net() {
  local out
  if ! out="$(node_net "$1")" || [ -z "$out" ]; then
    return 1
  fi
  set -- $out
  [ "$#" -eq 2 ] || return 1
  printf '%s %s\n' "$1" "$2"
}

check_rate() {
  local node="$1" metric="$2" rate="$3" max="$4"
  [ "$max" -lt 0 ] && return 0
  if [ "$rate" -gt "$max" ]; then
    echo "threshold exceeded: $node $metric ${rate}B/s > ${max}B/s" >&2
    return 1
  fi
  return 0
}

check_nonnegative_delta() {
  local node="$1" metric="$2" delta="$3"
  if [ "$delta" -lt 0 ]; then
    echo "counter reset during idle measurement: $node $metric delta=${delta}B" >&2
    return 1
  fi
  return 0
}

check_delta() {
  local node="$1" metric="$2" delta="$3" max="$4"
  if [ "$delta" -gt "$max" ]; then
    echo "threshold exceeded: $node $metric delta ${delta}B > ${max}B" >&2
    return 1
  fi
  return 0
}

declare -A r0 w0 rc0 wc0
declare -A rx0 tx0
for n in $NODES; do
  if ! sample="$(sample_node_io "$n")"; then
    echo "failed to sample daemon /proc I/O for $n"
    exit 1
  fi
  read -r rc wc rb wb <<<"$sample"
  rc0[$n]=$rc; wc0[$n]=$wc; r0[$n]=$rb; w0[$n]=$wb
  if ! sample="$(sample_node_net "$n")"; then
    echo "failed to sample network I/O for $n"
    exit 1
  fi
  read -r rx tx <<<"$sample"
  rx0[$n]=$rx; tx0[$n]=$tx
done

echo "measuring idle for ${WINDOW}s..."
echo "thresholds: rd_blk<=${MAX_READ_BLOCK_BPS}B/s wr_blk<=${MAX_WRITE_BLOCK_BPS}B/s wr_blk_bytes<=${MAX_WRITE_BLOCK_BYTES}B rx_net<=${MAX_RX_NET_BPS}B/s tx_net<=${MAX_TX_NET_BPS}B/s"
sleep "$WINDOW"

failed=0
printf '%-7s %12s %12s %12s %12s %12s %12s %12s\n' node rchar/s wchar/s rd_blk/s wr_blk/s wr_blk_B rx_net/s tx_net/s
for n in $NODES; do
  if ! sample="$(sample_node_io "$n")"; then
    echo "failed to sample daemon /proc I/O for $n"
    exit 1
  fi
  read -r rc wc rb wb <<<"$sample"
  if ! sample="$(sample_node_net "$n")"; then
    echo "failed to sample network I/O for $n"
    exit 1
  fi
  read -r rx tx <<<"$sample"
  rchar_delta=$(( rc - rc0[$n] ))
  wchar_delta=$(( wc - wc0[$n] ))
  read_block_delta=$(( rb - r0[$n] ))
  write_block_delta=$(( wb - w0[$n] ))
  rx_delta=$(( rx - rx0[$n] ))
  tx_delta=$(( tx - tx0[$n] ))
  rchar_rate=$(( rchar_delta / WINDOW ))
  wchar_rate=$(( wchar_delta / WINDOW ))
  read_block_rate=$(( read_block_delta / WINDOW ))
  write_block_rate=$(( write_block_delta / WINDOW ))
  rx_rate=$(( rx_delta / WINDOW ))
  tx_rate=$(( tx_delta / WINDOW ))
  printf '%-7s %12s %12s %12s %12s %12s %12s %12s\n' "$n" \
    "$rchar_rate" "$wchar_rate" \
    "$read_block_rate" "$write_block_rate" "$write_block_delta" \
    "$rx_rate" "$tx_rate"
  check_nonnegative_delta "$n" "rchar" "$rchar_delta" || failed=1
  check_nonnegative_delta "$n" "wchar" "$wchar_delta" || failed=1
  check_nonnegative_delta "$n" "rd_blk" "$read_block_delta" || failed=1
  check_nonnegative_delta "$n" "wr_blk" "$write_block_delta" || failed=1
  check_nonnegative_delta "$n" "rx_net" "$rx_delta" || failed=1
  check_nonnegative_delta "$n" "tx_net" "$tx_delta" || failed=1
  check_rate "$n" "rd_blk" "$read_block_rate" "$MAX_READ_BLOCK_BPS" || failed=1
  check_rate "$n" "wr_blk" "$write_block_rate" "$MAX_WRITE_BLOCK_BPS" || failed=1
  check_delta "$n" "wr_blk" "$write_block_delta" "$MAX_WRITE_BLOCK_BYTES" || failed=1
  check_rate "$n" "rx_net" "$rx_rate" "$MAX_RX_NET_BPS" || failed=1
  check_rate "$n" "tx_net" "$tx_rate" "$MAX_TX_NET_BPS" || failed=1
done
echo
echo "=== container BlockIO / NetIO (cumulative, eyeball deltas) ==="
docker stats --no-stream --format '{{.Name}}  blk={{.BlockIO}}  net={{.NetIO}}' $(docker compose ps -q) 2>/dev/null

if [ "$failed" -ne 0 ]; then
  echo "idle thresholds exceeded"
  exit 1
fi

echo "idle thresholds ok"
