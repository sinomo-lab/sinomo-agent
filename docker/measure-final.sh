#!/usr/bin/env bash
# Final clean idle measurement (diagnostics removed). Rebuild, fully settle, then
# measure per-node main+shadow disk io (by pid, no pgrep self-match) + container
# NetIO delta over the window. Targets: 0 disk write, <10KB/s disk read + network.
set -uo pipefail
cd "$(dirname "$0")"
WIN="${1:-120}"

echo "=== build ==="
if ! docker compose build node1 >/tmp/buildf.log 2>&1; then echo "BUILD FAILED"; tail -50 /tmp/buildf.log; exit 1; fi
echo "build ok"
docker compose up -d >/tmp/upf.log 2>&1 || { echo "UP FAILED"; tail -40 /tmp/upf.log; exit 1; }

echo "=== wait online (max 150s) ==="
for i in $(seq 1 30); do
  n=$(docker compose exec -T node1 sinomo cluster health 2>/dev/null | grep -c "daemon-main.*online" || true)
  [ "${n:-0}" -ge 3 ] && { echo "online after $((i*5))s"; break; }
  sleep 5
done

echo "=== settle 150s (well past the multi-minute settling phase) ==="
sleep 150

# sum read_bytes/write_bytes(disk blocks)+rchar across main+shadow pids of a node, by pid
node_disk() {
  docker compose exec -T "$1" sh -c '
    trc=0; trb=0; twb=0
    for pid in $(pgrep -x sinomo 2>/dev/null || pgrep sinomo); do
      io=/proc/$pid/io; [ -r "$io" ] || continue
      trc=$((trc+$(awk "/^rchar:/{print \$2}" $io 2>/dev/null || echo 0)))
      trb=$((trb+$(awk "/^read_bytes:/{print \$2}" $io 2>/dev/null || echo 0)))
      twb=$((twb+$(awk "/^write_bytes:/{print \$2}" $io 2>/dev/null || echo 0)))
    done
    echo "$trc $trb $twb"'
}
net_rx_tx() { docker stats --no-stream --format '{{.Name}} {{.NetIO}}' $(docker compose ps -q) 2>/dev/null; }

declare -A RC0 RB0 WB0
for n in node1 node2 node3; do read -r a b c <<<"$(node_disk $n)"; RC0[$n]=$a; RB0[$n]=$b; WB0[$n]=$c; done
NET0="$(net_rx_tx)"
echo "measuring ${WIN}s..."; sleep "$WIN"

printf '%-7s %12s %12s %12s\n' node rchar/s rd_blk/s wr_blk/s
for n in node1 node2 node3; do read -r a b c <<<"$(node_disk $n)"
  printf '%-7s %12s %12s %12s\n' $n $(( (a-${RC0[$n]})/WIN )) $(( (b-${RB0[$n]})/WIN )) $(( (c-${WB0[$n]})/WIN )); done
echo "=== NetIO (cumulative) BEFORE then AFTER — eyeball per-container delta/${WIN}s ==="
echo "$NET0"; echo "---"; net_rx_tx
echo "=== done ==="
