#!/usr/bin/env bash
# Measure per-container idle disk + network I/O over a window, and per-daemon
# read/write syscall + block bytes from /proc/<pid>/io. Targets: 0 disk write,
# <10 KB/s disk read, <10 KB/s network during idle.
#
#   docker/measure-idle.sh [window_seconds]   (default 60)
set -uo pipefail
cd "$(dirname "$0")/.."
WINDOW="${1:-60}"
NODES="node1 node2 node3"

# Sum read_bytes/write_bytes/rchar/wchar across the main+shadow daemon pids in a node.
node_io() {
  docker compose exec -T "$1" sh -c '
    total_r=0; total_w=0; total_rc=0; total_wc=0
    for pid in $(pgrep -f "sinomo daemon"); do
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

declare -A r0 w0 rc0 wc0
for n in $NODES; do
  read -r rc wc rb wb <<<"$(node_io "$n")"
  rc0[$n]=$rc; wc0[$n]=$wc; r0[$n]=$rb; w0[$n]=$wb
done
net0="$(docker compose exec -T node1 sinomo daemon status 2>/dev/null | grep -iE 'net|bytes (sent|recv)' || true)"

echo "measuring idle for ${WINDOW}s..."
sleep "$WINDOW"

printf '%-7s %12s %12s %12s %12s\n' node rchar/s wchar/s rd_blk/s wr_blk/s
for n in $NODES; do
  read -r rc wc rb wb <<<"$(node_io "$n")"
  printf '%-7s %12s %12s %12s %12s\n' "$n" \
    "$(( (rc - rc0[$n]) / WINDOW ))" "$(( (wc - wc0[$n]) / WINDOW ))" \
    "$(( (rb - r0[$n]) / WINDOW ))" "$(( (wb - w0[$n]) / WINDOW ))"
done
echo
echo "=== container BlockIO / NetIO (cumulative, eyeball deltas) ==="
docker stats --no-stream --format '{{.Name}}  blk={{.BlockIO}}  net={{.NetIO}}' $(docker compose ps -q) 2>/dev/null
