# Sinomo quietness and optional native-host diagnostics

The active rollout requirement is **Sinomo main and shadow daemons plus all
Sinomo-attributable children, helpers, detached/delegated work and writeback**.
Unrelated host CPU, disk and network activity does not gate rollout.
Host-global lifecycle tracing, privileged
observers and approval of a new host disk allowance are not prerequisites.

`daemon_scope.py` defines the process-family policy and conservative offline
counter checks. Its evidence review cannot turn a fixture into live acceptance;
a live run still needs qualified ownership and counter sources.

## Runnable local ownership collector

`owned_counters.py` reads existing local proc/cgroup files; it never starts,
stops or configures a daemon. Supply a fresh operator-reviewed manifest matching
[`owned-counters.schema.json`](owned-counters.schema.json) and its SHA-256:

```sh
python3 -B scripts/verification/native-quietness/owned_counters.py \
  --manifest /absolute/runtime-ownership.json \
  --manifest-sha256 REVIEWED_MANIFEST_SHA256 \
  --output /absolute/new-counter-artifacts
```

The schema binds node/boot/source revision, DATA directory device/inode,
disjoint cgroup paths/device/inodes, and each main/shadow/helper PID, start ticks,
executable path/hash, exact argv, group and ownership reference. The manifest
also pins a JSON runtime receipt by path and digest; that receipt must repeat
`purpose`, `node_id`, `boot_id`, `source_revision`, `data_dir`, `data_identity`,
`groups`, `owners` and `delegations` exactly. Derive those fields from supported
runtime/rollout receipts and current kernel identities. The collector verifies
the binding and kernel objects, not the authenticity/completeness of a forged
or stale operator receipt. `runtime` purpose requires exactly one main and one
shadow; `isolated-control` is explicitly non-runtime test evidence.

Every declared owner must be live and identity-valid at attachment. Pidfds pin
those incarnations; executable hashes are cached only behind unchanged file
device/inode/size/mtime/ctime guards. Receipt, manifest, boot and DATA identity
are rechecked during capture. Cgroup directory FDs pin the declared objects;
the reader includes nested members but sums only disjoint root CPU totals.
Unknown members, changed identities, moved owners, denied reads and resets
remain explicit. Known owners are still observed after a cgroup move, while
the missing CPU handoff prevents using the aggregate as complete attribution.

Output `samples.ndjson` contains monotonic read brackets, `cpu` records consumed
directly by `daemon_scope.cpu_interval`, membership observations and a retained
owner ledger. Exited owners keep `last_io`; `final_io` stays unavailable.
Aggregate process I/O becomes null on missing/reset/unfinalized observations,
not zero. Its deliberately incomplete semantics are rejected by the full
`disk_window` gate. Supplied coverage/completion flags cannot qualify missing
network, delegation or delayed-writeback sources. No `/proc/PID/net/dev` or
partial framed-RPC byte substitution is used.

The original 120-second deadline starts before attachment; sampling uses the
one-second cadence without skipped tick fabrication. `--samples N` (1..121)
permits a shorter diagnostic control only; it does not shorten either acceptance
window or grant PASS. New output paths are required and files use mode 0600.
The receipt distinguishes completed reads, errors and remaining coverage gaps;
exit code 2 and `acceptance: false` remain expected until a complete source is
qualified. A quiet CPU interval alone cannot establish daemon quietness.

The collector's external raw evidence writes are not Sinomo runtime work;
they are not subtracted from any captured owned counter. If an operator places
the collector inside a measured root, it must be declared and its costs stay in
the root total. Actual Sinomo logging/tracing/helper or delayed work is never
exempted as observation overhead. Scope-specific ownership and final-I/O gaps
remain; this adapter does not revive host-global observer prerequisites.

| Metric | Preserved threshold | Window |
|---|---|---|
| CPU | <=10% of one CPU, aggregate owned work per node | continuously covered joint CPU/network 30s within the original absolute 120s |
| Network | <=65,536 combined TX+RX B/s; no burst or loopback exemption | same joint 30s/120s; counter semantics must match |
| Disk | <=10,240 block-read B/s; zero block-write bytes/B/s | separate legacy 60s observation; do not silently shorten to 30s |

CPU/network values come from `docker/test-cluster.sh`; disk values and its 60s
default come from `docker/measure-idle.sh`. The latter's separate RX/TX limits,
4 MiB network allowance and loopback exclusion are **not** adopted. Keep the
one-second cadence and conservative read bounds. Starting again after a failed
window cannot reset the original deadline. Startup-idle and post-agent settling
both need evidence of Sinomo work completion; no deferred work may be hidden.

Use nonoverlapping, identity-bound cgroup subtrees for aggregate CPU when
available. Main/shadow runtime identity, boot/cgroup incarnation, executable
revision and the ownership of helpers must be established independently.
Reparenting does not remove a process from its cgroup, but explicit migration,
external service handoff and delegated jobs need additional ownership records.
Retain retired work and cumulative counters until its effects finish; a current
PID census or process-name filter alone is insufficient. Never add parent and
child cgroup totals together.

For disk, retain `/proc/PID/io` read_bytes/write_bytes semantics across main,
shadow and helpers, including final/retired accounting. Cgroup io.stat can help
with writeback only where available and qualified for the filesystem/ownership
model; logical runtime file bytes are not a substitute. Dirty/writeback gauges
are diagnostics, not proof of all deferred work. In-process tracing/logging and
Sinomo helpers remain charged. Independently owned external diagnostics are not
Sinomo workloads, but no actual Sinomo worker can be relabelled to exclude it.

`/proc/PID/net/dev` is namespace-wide, **not process traffic**. Framed RPC
telemetry alone misses other sockets, providers, discovery, helpers and transport
overhead. Existing complete owned-network counters may qualify where ownership
is exclusive and proven. IP/socket-payload counters require an explicit byte-
semantics decision before comparison to the interface-byte budget; do not
silently weaken the metric. Missing process-family coverage stays explicit.

## Optional host tools

The remaining collector, `controller.py`, `disk_metrics.py`, and
`lifecycle_replay.py` retain useful host diagnostic and replay capabilities.
They are **not rollout gates**. Host capture reports `verdict: DIAGNOSTIC`,
`rollout_gate: false`, reference limits and diagnostic gaps. Missing global
namespace/interface or observer-exit coverage may invalidate a host diagnostic,
but cannot veto daemon quietness. No host-global observer project is required.
The reference comparisons use 10% CPU, 65,536 combined network B/s and the
30s/120s schedule solely for diagnostic interpretation.
Their observation costs remain in those diagnostic counters; they are not a
new requirement to charge all unrelated host work to Sinomo.

## Offline process-network artifact review

`ip-counter.bpf.c` is a small allow-only cgroup_skb artifact for review. Ingress
and egress increment separate per-CPU `skb_bytes`, `skb_packets` (hook visits)
and sticky overflow fields. Both return 1 without modifying packets. Each
disjoint owned root would need its own fresh map/object; userspace writes are
disabled in the map definition. No loader currently attaches this artifact.

`packet_loader_review.py` has **no kernel backend or apply mode**. Its CLI accepts
a pinned ownership manifest, pinned compiled-artifact receipt/object and an
explicitly synthetic snapshot. It reviews source/boot/root/runtime identities,
freshness, disjoint roots, exact main/shadow service bindings and proven helper
references. Whole-host/user/slice/session roots and any existing or unknown
effective attachments are rejected. Synthetic checks also require immutable
program/map/link identities, separate maps per root, no counter reset and
cleanup of exact owned link handles only. A real backend would need to establish
these facts from pinned kernel FDs and handle attachment races; supplied JSON
cannot do so.

The output always retains `execution_ready: false`, `acceptance: false` and
`kernel_actions_performed: false`. Compilation, disassembly and native fake-map
checks are not kernel verifier or live coverage evidence. The artifact counts
`__sk_buff.len` per hook visit, not interface-equivalent bytes: no automatic
65,536 B/s comparison or conversion is applied. Header/offload/fan-out/helper
coverage and the byte-semantics decision remain explicit review items.
Network loading/attachment needs its own authorization; authorization for I/O
accounting does not authorize it.

## Build and checks

Use an existing C compiler and OpenSSL development files; missing dependencies
are a resource blocker, not permission to install/change the host. Use a new
artifact directory under `data/operations/`. The binary is never committed.
After owned builds finish, serialize compilation and live probes on
`/tmp/sinomo-maintenance-build.lock`, with normal compiler scheduling:

```sh
flock /tmp/sinomo-maintenance-build.lock cc -O2 -g -std=c11 -Wall -Wextra -Werror \
  scripts/verification/native-quietness/linux-counters.c -lcrypto -o /absolute/artifacts/linux-counters
SINOMO_NATIVE_COUNTER_BINARY=/absolute/artifacts/linux-counters \
  flock /tmp/sinomo-maintenance-build.lock python3 -B -W error \
  -m unittest discover -s scripts/verification/native-quietness -p 'test_*.py'
flock /tmp/sinomo-maintenance-build.lock cc -O2 -g -std=c11 -Wall -Wextra -Werror \
  scripts/verification/native-quietness/test_ip_counter.c -o /absolute/artifacts/test-ip-counter
/absolute/artifacts/test-ip-counter
flock /tmp/sinomo-maintenance-build.lock python3 -B \
  scripts/verification/native-quietness/probe.py \
  --collector /absolute/artifacts/linux-counters --output /absolute/artifacts/NEW-probe
```

The probe uses an already cached, digest-pinned image, `--network none`, no DATA
mounts, a read-only filesystem and no added capabilities. It retains its owned
container stopped. It verifies held-thread CPU against process CPU time, TCP/UDP
IPv4/IPv6 loopback payloads and exact public/inside byte-counter agreement. A
short-lived disposable namespace demonstrates why even successful adjacent
censuses cannot certify continuous host completeness; an observed birth/exit
instead demonstrates interval rejection. Fixture work runs independently of
the sampler, so Docker startup/removal cannot excuse a missed tick. The receipt
distinguishes the full CLI lifetime from the active namespace observations;
cleanup duration is never used to invent a tighter namespace lifetime. Actual timestamps,
including a missed tick or inconclusive lifecycle demonstration, remain evidence.
It neither stops nor throttles unrelated workloads. A positive fixture result
cannot qualify a host or change any prior failure.

## Capture protocol and identities

The C collector takes `--raw NEW_LOCAL_NDJSON`, opens it exclusively with mode
0600, and sends a ready record containing its executable SHA256, boot ID,
PID/start ticks, host, HZ and namespace. It accepts only consecutive
`SAMPLE sequence nonce` commands, then `QUIT`. Unexpected EOF, malformed/repeated
commands, raw-write failures or truncation fail closed. Commands never execute
shell text. Detailed process/thread records stay local; small counter summaries
use the control transport, whose CPU and bytes are still charged.

Every live userspace **thread** is examined. `/proc/TGID/task/TID/stat` supplies
thread user/system CPU ticks; process aggregate and child-time counters are not
added again. Read-only `pidfd_open(PIDFD_THREAD)` (Linux >=6.9) plus zero-timeout
polls give collector-local incarnation epochs even when PID/start ticks collide.
Unavailable pidfds invalidate identity; no permissive fallback is used. Boot,
collector PID/start/hash and epoch jointly bound these identities. Process
executable inode/size/mtime/ctime and exact argv are guarded before/after the
thread census; Sinomo candidates also receive a cached, guarded executable
SHA256. Denied identities, births/exits, resets and unfinished reads remain
explicit. No pidfd signal, ptrace, namespace-get or namespace entry is used.

Public `/proc/TGID/task/TID/net/dev` files are deduplicated by their **counter
file** device/inode, cross-checked against readable namespace symlinks. The
counter-file inode is not assumed equal to the namespace inode or sufficient
for lifetime identity. Device numbers are proc-mount-specific: the fixture binds
inside/outside observations through its known owned host PID, retaining both
device/inode pairs; it never deduplicates different mounts by inode alone.
Each endpoint is reopened and its task/pidfd rechecked;
the collector does not retain counter FDs or pin network namespaces. All
interfaces and both loopback directions are retained. These bytes describe a
namespace, **not a process**. Missing/reset counters and changed namespace or
interface inventories invalidate an interval. A read-only route-netlink link
event reader and name/index inventory guard the collector's own namespace;
event loss, index changes or missing readers invalidate evidence. Foreign
interface indices/lifecycle coverage stay unavailable. Protected namespace
directories report actual errno; a denied path is never treated as absent.

Whole-host CPU uses the original user+nice+system+IRQ+softIRQ+steal sum. Guest
time is already included, so it is not added twice; idle/iowait are excluded.
Exact integer cross multiplication uses the **minimum** time between bracketed
counter reads. The display rates are not the gate authority. Network sums all
group/interface RX+TX deltas and uses the minimum of their conservative read
intervals. No CPU-count normalization, baseline subtraction, counter caching or
counter-layer substitution occurs. The collector's `observer_cpu_ns` measures
census work before its final flush; lifetime cost must also be measured through
exit (as in the probes), and is never deducted from host CPU.

`controller.py capture --config CONFIG.json --output NEW_DIRECTORY` accepts:

```json
{"nodes":[{"name":"sodium","boot_id":"PREFLIGHT-BOOT-ID","collector_sha256":"BUILT-EXE-SHA256","command":["/absolute/linux-counters","--raw","/absolute/NEW-raw.ndjson"]}]}
```

Use only existing permitted routes. Synchronize source by native Git commits
and authorized push/pull; do not send source bundles/patches between servers.
No Windows command or alternate route is authorized while `USER_CONTROL` holds.

## Optional host disk observations

The host disk metrics below remain diagnostic. They carry no rollout limits and
require no host disk-policy decision. The mandatory process-family disk criterion
is the legacy 10,240 read B/s and zero-write check described above.

Every collector sample also brackets read-only `/proc/diskstats` and
`/proc/vmstat` snapshots. `disk_metrics.py` retains each block-device row
separately, with read/write bytes (512-byte kernel sectors), completed operations,
discard bytes/operations, flush requests/time where present, in-flight I/O, and
dirty/writeback page gauges converted with the reported page size. Missing
fields remain unavailable. Errors, counter resets, changed inventories or
schemas, sequence gaps and overlapping/stalled reads invalidate deltas.
The diagnostic uses the minimum bracketed interval without subtracting any
observer cost. It shares the existing sampler cadence and absolute deadline;
it adds no parallel sampling schedule.

These rows overlap across disks, partitions and stacked devices. They have no
aggregate until the backing-device topology and one nonduplicated accounting
layer are established. Major/minor/name and equal endpoint inventories cannot
prove continuous device lifetime. Diskstats flushes are device requests, not
`fsync`/`fdatasync` calls, and partitions do not track flush requests. The tool
does not infer zero sync activity or deferred writes from zero block deltas.
See the [kernel I/O counter contract](https://www.kernel.org/doc/html/latest/admin-guide/iostats.html).

No forced `sync`, cache drop, writeback tuning, or stopped baseline is used.
Host topology/lifetime/sync/drain gaps are diagnostic gaps only. They do not
replace the narrower obligation to account for Sinomo-originated work. Synthetic
controls validate arithmetic and failure handling, not a live run.

The controller starts its absolute deadline before launching readers. All
hosts receive the same sequence/nonce on a one-second absolute schedule, with
no skipped ticks; delays/missing replies are recorded, never filled with zeros.
A start is after **all** baseline acknowledgements, an end before **all** end
commands, and end-read acknowledgements must arrive inside the original deadline.
No independent host clocks are subtracted. The one-second scheduler has a
50ms lateness guard; this does not enlarge a counter-rate denominator, quiet
window or deadline. The unchanged maintained 5s maximum counter-read interval
also fails closed. Resetting a candidate never restarts the absolute deadline.
Missing continuous coverage breaks every candidate. CPU/net coverage across
owned fixture/helper drain is retained; independent collector/transport-drain
coverage remains a separate explicit blocker.

`controller.py attribution --raw RAW --runtimes RUNTIMES.json --output NEW.json`
reports stable-thread CPU and unknown births/exits/execs. Runtime entries require
`role`, exact `executable`, `sha256`, `pid`, `start_ticks`, `boot_id`, `revision`,
`node_id`, `data_dir` from the verified manual-start/deployment receipt. Matching
the executable path or command substring alone never assigns node ownership.
Main argv begins `daemon run`; shadow argv begins `daemon shadow run`. Other
Sinomo candidates remain unclassified. Denied executable identity does not
hide the thread's CPU or convert it to an identified daemon. Namespace bytes
cannot establish per-daemon network attribution.

## Optional host completeness limitations

A complete optional host diagnostic would need an authorized, loss-detecting observation
source for **all** namespace/interface births, exits, changes, orphan/persistent
bindings and final counters, plus independent coverage through reader/transport
drain. An arbitrary process can create/destroy a namespace between polls;
container events alone cannot prove coverage of all native threads. Snapshot
stability, Docker access and an empty readable mount directory cannot establish
that proof. Record those limitations if using the optional host tools; do not carry them
forward as prerequisites for the process-family rollout gate. Do not grant
capabilities, change permissions or introduce production isolation to obtain it.

Kernel references: [thread proc files](https://man7.org/linux/man-pages/man5/proc_pid_task.5.html),
[pidfd identity/lifetime](https://man7.org/linux/man-pages/man2/pidfd_open.2.html),
[host CPU fields](https://man7.org/linux/man-pages/man5/proc_stat.5.html),
[network byte counters](https://docs.kernel.org/networking/statistics.html).
