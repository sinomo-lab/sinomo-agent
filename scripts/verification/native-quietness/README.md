# Native-host counter verification

This independent tool diagnoses the **entire native host**. The Linux backend
does **not** currently supply complete continuous network namespace/interface
lifecycle evidence or an independent reader across its own/transport exit.
Consequently `capture` always returns `BLOCKED`, nonzero, `acceptance: false`.
There is no flag to override these gaps. Fixture success is not real-cluster
acceptance. The maintained Docker harness remains unchanged.

The contract stays: each entire host at most **10% of one CPU**, **65,536 B/s
combined TX+RX** over every interface including loopback in every namespace;
one joint, continuously covered **30 seconds** within the original absolute
**120 seconds**. Collector, controller, tracing, profiling and all helper costs
remain charged. No observed cost is subtracted. This tool does not change
production startup, isolation, permissions, security or network settings.

## Build and checks

Use an existing C compiler and OpenSSL development files; missing dependencies
are a resource blocker, not permission to install/change the host. Use a new
artifact directory under `data/operations/`. The binary is never committed.
After owned builds finish, serialize compilation and live probes on
`/tmp/sinomo-maintenance-build.lock`, with normal compiler scheduling:

```sh
flock /tmp/sinomo-maintenance-build.lock cc -O2 -g -std=c11 -Wall -Wextra -Werror \
  scripts/verification/native-quietness/linux-counters.c -lcrypto -o /absolute/artifacts/linux-counters
python3 -B -W error scripts/verification/native-quietness/test_controller.py
SINOMO_NATIVE_COUNTER_BINARY=/absolute/artifacts/linux-counters \
  flock /tmp/sinomo-maintenance-build.lock python3 -B -W error \
  scripts/verification/native-quietness/test_collector.py
flock /tmp/sinomo-maintenance-build.lock python3 -B \
  scripts/verification/native-quietness/probe.py \
  --collector /absolute/artifacts/linux-counters --output /absolute/artifacts/NEW-probe
```

The probe uses an already cached, digest-pinned image, `--network none`, no DATA
mounts, a read-only filesystem and no added capabilities. It retains its owned
container stopped. It verifies held-thread CPU against process CPU time, TCP/UDP
IPv4/IPv6 loopback payloads and exact public/inside byte-counter agreement. A
short-lived disposable namespace demonstrates why even successful adjacent
censuses cannot certify continuous host completeness. Its actual timestamps,
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

## Remaining prerequisite

Full-host acceptance needs an already authorized, loss-detecting observation
source for **all** namespace/interface births, exits, changes, orphan/persistent
bindings and final counters, plus independent coverage through reader/transport
drain. An arbitrary process can create/destroy a namespace between polls;
container events alone cannot prove coverage of all native threads. Snapshot
stability, Docker access and an empty readable mount directory cannot establish
that proof. Supply a concrete read-only operator-owned observation receipt or
return the specific permission/resource gap to the orchestrator. Do not grant
capabilities, change permissions or introduce production isolation to obtain it.

Kernel references: [thread proc files](https://man7.org/linux/man-pages/man5/proc_pid_task.5.html),
[pidfd identity/lifetime](https://man7.org/linux/man-pages/man2/pidfd_open.2.html),
[host CPU fields](https://man7.org/linux/man-pages/man5/proc_stat.5.html),
[network byte counters](https://docs.kernel.org/networking/statistics.html).
