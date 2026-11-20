#!/bin/sh
# Run inside a disposable, FUSE-enabled dev node with the named fixture home.
set -eu
agent=${1:?cluster test agent required}
case "$agent" in cluster-test-*) ;; *) exit 2;; esac
case "$agent" in *[!a-z0-9-]*) exit 2;; esac
tmp=$(mktemp -d /tmp/sinomo-fuse-fences-XXXXXXXX)
mkdir "$tmp/first" "$tmp/second"
first_pid=
second_pid=
cleanup() {
 for path in "$tmp/first" "$tmp/second"; do
   # stat(2) can return ENOTCONN for the crashed mount. Its kernel mount
   # record remains authoritative until this exact test target is detached.
   if awk -v target="$path" '$5 == target { found = 1 } END { exit !found }' /proc/self/mountinfo; then
     fusermount3 -uz "$path"
   fi
 done
 for pid in "$first_pid" "$second_pid"; do
   if [ -n "$pid" ]; then wait "$pid" || :; fi
 done
 rm -f -- "$tmp/first.log" "$tmp/second.log"
 rmdir -- "$tmp/first" "$tmp/second" "$tmp"
}
trap cleanup EXIT
await_mount() {
 p=$1; pid=$2; n=0
 while ! mountpoint -q "$p"; do
   kill -0 "$pid" || { tail -c 1100 "$p.log"; return 1; }
   n=$((n+1)); [ "$n" -lt 450 ]; sleep .1
 done
}
sinomo dfs mount --foreground --root / "$tmp/first" > "$tmp/first.log" 2>&1 & first_pid=$!
await_mount "$tmp/first" "$first_pid"
sinomo dfs mount --foreground --root / "$tmp/second" > "$tmp/second.log" 2>&1 & second_pid=$!
await_mount "$tmp/second" "$second_pid"
path="sinomo/agents/$agent/home/${tmp##*/}.txt"
(set -C; printf 'first' > "$tmp/first/$path")
test "$(cat "$tmp/second/$path")" = first
kill -KILL "$first_pid"; wait "$first_pid" || :; first_pid=
printf 'survived' > "$tmp/second/$path"
test "$(cat "$tmp/second/$path")" = survived
rm -- "$tmp/second/$path"
echo 'Two foreground FUSE sessions exchanged the same canonical bytes. The surviving session wrote and read after the other process was killed.'
