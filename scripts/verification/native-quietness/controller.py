#!/usr/bin/env python3
"""Whole-native-host quietness protocol and conservative evidence evaluation.

The Linux polling backend is deliberately coverage-incomplete. This controller
cannot turn it into an acceptance PASS. Fixture tests exercise the same arithmetic
and barriers, but fixture results are always labelled non-acceptance.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import queue
import secrets
import subprocess
import threading
import time

SECOND = 1_000_000_000
CPU_PERCENT = 10
NETWORK_BPS = 65_536
QUIET_NS = 30 * SECOND
DEADLINE_NS = 120 * SECOND
CADENCE_NS = SECOND
MAX_COUNTER_GAP_NS = 5 * SECOND  # unchanged maintained counter-reader bound
COVERAGE = ("continuous_host_namespaces", "all_interface_lifecycles",
            "persistent_namespaces", "process_lifetimes")
CPU_FIELDS = (0, 1, 2, 5, 6, 7)  # guest already included; no idle or iowait


def integer(value):
    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise ValueError("missing, negative or non-u64 counter/identity")
    return value


def span(value):
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("counter read interval unavailable")
    a, b = map(integer, value)
    if a > b:
        raise ValueError("counter read interval reversed")
    return a, b


def interval(current, previous, hertz):
    """Return independent gate and coverage verdicts, with no overhead subtraction.

    The numerator includes ALL interfaces in every observed counter group.
    The denominator uses the minimum possible read interval, including each
    group's read uncertainty. Namespace counters are never per-process bytes.
    """
    hertz = integer(hertz)
    if not hertz:
        raise ValueError("clock tick frequency unavailable")
    if not current.get("boot_id") or current["boot_id"] != previous.get("boot_id"):
        raise ValueError("boot incarnation changed or unavailable")
    if integer(current["sequence"]) != integer(previous["sequence"]) + 1:
        raise ValueError("missing/restarted counter reader")
    for row in (previous, current):
        begin, end = span([row["begin_ns"], row["end_ns"]])
        if begin == end or not row.get("nonce"):
            raise ValueError("empty sample or missing controller barrier")
        if integer(row["errors"]) or integer(row["vanished"]):
            raise ValueError("missing, changed or unexplained reader/process")
        if integer(row["self_link_losses"]) or not row["self_link_reader"]:
            raise ValueError("interface event reader lost or unavailable")
        for times in [row["cpu_read_ns"], *(g["read_ns"] for g in row["groups"].values())]:
            a, b = span(times)
            if not begin <= a <= b <= end:
                raise ValueError("counter read outside its census")
    if current["begin_ns"] <= previous["end_ns"]:
        raise ValueError("overlapping/reversed census")
    # A complete expected command stream is one observation per second. Read
    # uncertainty is charged, never used to lengthen the controller cadence.
    if current["begin_ns"] - previous["begin_ns"] > MAX_COUNTER_GAP_NS:
        raise ValueError("stalled census")
    if current["self_link_events"] != previous["self_link_events"]:
        raise ValueError("observed interface lifecycle change")
    old_cpu, new_cpu = previous["cpu_fields"], current["cpu_fields"]
    if len(old_cpu) != 10 or len(new_cpu) != 10:
        raise ValueError("whole-host CPU fields unavailable")
    deltas = [integer(b) - integer(a) for a, b in zip(old_cpu, new_cpu)]
    if any(deltas[i] < 0 for i in CPU_FIELDS):
        raise ValueError("host CPU counter reset")
    cpu_gap = span(current["cpu_read_ns"])[0] - span(previous["cpu_read_ns"])[1]
    if not 0 < cpu_gap <= MAX_COUNTER_GAP_NS:
        raise ValueError("CPU read overlap or stall")
    # Integer cross multiplication is authoritative; float rates are display only.
    cpu_ticks = sum(deltas[i] for i in CPU_FIELDS)
    cpu_within = cpu_ticks * 100 * SECOND <= CPU_PERCENT * hertz * cpu_gap
    old, new = previous["groups"], current["groups"]
    if not old or old.keys() != new.keys():
        raise ValueError("namespace counter inventory changed or missing")
    rx = tx = 0
    gaps = []
    for key in new:
        a, b = old[key], new[key]
        if a["netns"] != b["netns"]:
            raise ValueError("namespace identity changed")
        interfaces = b["interfaces"]
        if not interfaces or "lo" not in interfaces or a["interfaces"].keys() != interfaces.keys():
            raise ValueError("interface inventory changed or loopback missing")
        if a.get("interface_indices") != b.get("interface_indices"):
            raise ValueError("interface index lifecycle changed or reader missing")
        for g in (a, b):
            indices = g.get("interface_indices")
            if indices is not None and (indices.keys() != g["interfaces"].keys()
                                        or any(not integer(i) for i in indices.values())):
                raise ValueError("interface index inventory/counter binding incomplete")
        gap = span(b["read_ns"])[0] - span(a["read_ns"])[1]
        if not 0 < gap <= MAX_COUNTER_GAP_NS:
            raise ValueError("network counter read overlap or stall")
        gaps.append(gap)
        for name, values in interfaces.items():
            if len(values) != 2 or len(a["interfaces"][name]) != 2:
                raise ValueError("RX/TX counter pair missing")
            drx, dtx = (integer(v) - integer(u) for u, v in zip(a["interfaces"][name], values))
            if drx < 0 or dtx < 0:
                raise ValueError("network interface counter reset")
            rx += drx
            tx += dtx
    network_gap = min(gaps)
    network_within = (rx + tx) * SECOND <= NETWORK_BPS * network_gap
    coverage = all(row.get("coverage", {}).get(key) is True
                   for row in (previous, current) for key in COVERAGE)
    coverage = coverage and all(g.get("interface_indices") is not None
                                for row in (previous, current) for g in row["groups"].values())
    return dict(cpu_within=cpu_within, network_within=network_within,
                counters_within=cpu_within and network_within, coverage_complete=coverage,
                cpu_ticks=cpu_ticks, cpu_minimum_interval_ns=cpu_gap,
                cpu_percent_one_core_upper=cpu_ticks * 100 * SECOND / hertz / cpu_gap,
                rx_bytes=rx, tx_bytes=tx, network_minimum_interval_ns=network_gap,
                combined_Bps_upper=(rx + tx) * SECOND / network_gap)


class JointWindow:
    """All timestamps here are the ONE controller clock, never host-clock joins.

    Baseline reads must close before their acknowledgement. A common start is
    after ALL baseline ACKs. A common end is BEFORE sending ALL end commands;
    their counter reads must follow those commands. All ACKs, including the end
    and drain proofs, must arrive inside the original absolute 120-second budget.
    """
    def __init__(self, nodes, start_ns):
        if not nodes or len(set(nodes)) != len(nodes):
            raise ValueError("nonempty unique host set required")
        self.nodes = tuple(nodes)
        self.start = start_ns
        self.deadline = start_ns + DEADLINE_NS
        self.anchor = None
        self.sequence = -1
        self.last_send = None
        self.errors = []

    def observe(self, sequence, nonce, commands, replies, checks, *, end=False):
        try:
            if set(commands) != set(self.nodes) or set(replies) != set(self.nodes):
                raise ValueError("missing host barrier participant")
            if sequence != self.sequence + 1:
                raise ValueError("missing controller sample sequence")
            sends = [commands[n] for n in self.nodes]
            receives = [replies[n]["received_ns"] for n in self.nodes]
            if any(s < self.start or s > r or r > self.deadline
                   for s, r in zip(sends, receives)):
                raise ValueError("barrier response outside original absolute deadline")
            for n in self.nodes:
                r = replies[n]["sample"]
                if r["sequence"] != sequence or r["nonce"] != nonce:
                    raise ValueError("stale/wrong barrier or restarted reader")
            if self.last_send is not None:
                # Scheduling receipts, rather than remote clocks, prove cadence.
                # No skipped ticks; 50ms scheduling tolerance cannot change the
                # counter-rate denominator, the 30s window or the 120s deadline.
                if any(s - self.last_send[n] > CADENCE_NS + 50_000_000 for n, s in commands.items()):
                    raise ValueError("one-second observation cadence missed")
            all_good = bool(checks) and set(checks) == set(self.nodes) and all(
                v["counters_within"] and v["coverage_complete"] for v in checks.values())
            common_end = min(sends)
            complete = (end and all_good and self.anchor is not None
                        and common_end - self.anchor >= QUIET_NS)
            self.sequence = sequence
            self.last_send = commands.copy()
            if not all_good or self.anchor is None:
                self.anchor = max(receives)
            return dict(covered_window=complete,
                        common_start_ns=self.anchor, common_end_ns=common_end,
                        common_duration_ns=max(0, common_end - self.anchor),
                        absolute_deadline_ns=self.deadline)
        except (ValueError, KeyError, TypeError) as e:
            self.errors.append(str(e))
            self.anchor = None
            self.sequence = sequence
            self.last_send = commands.copy()
            return dict(covered_window=False, error=str(e))


class Reader:
    def __init__(self, command, stderr_path):
        self.stderr = open(stderr_path, "xb")
        self.cleanup_errors = []
        try:
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=self.stderr)
        except OSError:
            self.stderr.close()
            raise
        self.lines = queue.Queue()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            while line := self.process.stdout.readline(1_048_577):
                if len(line) > 1_048_576 or not line.endswith(b"\n"):
                    raise ValueError("truncated/oversized collector response")
                self.lines.put((time.monotonic_ns(), json.loads(line)))
        except (ValueError, OSError) as e:
            self.lines.put(e)
        finally:
            self.lines.put(EOFError("collector stopped or missing response"))

    def receive(self, deadline):
        remaining = (deadline - time.monotonic_ns()) / SECOND
        if remaining <= 0:
            raise TimeoutError("original absolute deadline expired")
        value = self.lines.get(timeout=remaining)
        if isinstance(value, Exception):
            raise value
        return {"received_ns": value[0], "sample": value[1]}

    def send(self, message):
        begin = time.monotonic_ns()
        self.process.stdin.write(message.encode("ascii"))
        self.process.stdin.flush()
        return begin

    def close(self):
        # Only the collector owned by this invocation is affected. No daemon,
        # user workload, service, namespace or container is stopped here.
        try:
            if self.process.poll() is None:
                self.send("QUIT\n")
            self.process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired) as error:
            self.cleanup_errors.append(str(error))
            if self.process.poll() is None:
                self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        finally:
            self.thread.join(timeout=2)
            if self.thread.is_alive():
                self.cleanup_errors.append("counter response reader did not drain")
            else:
                for pipe in (self.process.stdout, self.process.stdin):
                    try:
                        pipe.close()
                    except OSError as error:
                        self.cleanup_errors.append(str(error))
            self.stderr.close()


def run(config, output):
    """Capture diagnostic native evidence; never accept an incomplete backend.

    config.nodes entries contain an argv command for an already-built, permitted
    collector and its expected SHA256/boot identity. No shell is invoked locally.
    Raw task evidence stays at the collector's --raw path. Native acceptance must
    wait for an authorized, complete lifecycle/drain provider; no config flag can
    assert that provider into existence.
    """
    output.mkdir(parents=True, exist_ok=False)
    nodes = config["nodes"]
    names = [n["name"] for n in nodes]
    if not names or len(set(names)) != len(names):
        raise ValueError("unique expected native hosts required")
    started = time.monotonic_ns()
    window = JointWindow(names, started)
    readers = {}
    previous = {}
    ready = {}
    result = {"schema": "sinomo.native-joint-quietness.v1", "scope": "EACH ENTIRE NATIVE HOST",
              "limits": {"cpu_percent_of_one_core": CPU_PERCENT,
                         "combined_TX_RX_Bps_all_interfaces_including_loopback": NETWORK_BPS,
                         "continuous_ns": QUIET_NS, "absolute_search_ns": DEADLINE_NS,
                         "cadence_ns": CADENCE_NS},
              "started_ns": started, "absolute_deadline_ns": window.deadline,
              "acceptance": False, "verdict": "BLOCKED", "hosts": names,
              "blockers": [], "ready": ready, "collector_exit": {}}
    try:
        for n in nodes:
            readers[n["name"]] = Reader(n["command"], output / (n["name"] + ".stderr"))
        for n in nodes:
            r = readers[n["name"]].receive(window.deadline)["sample"]
            if r.get("type") != "ready" or r.get("schema") != "sinomo.native-host-counters.v1":
                raise ValueError("unexpected collector protocol")
            if r.get("collector_sha256") != n["collector_sha256"] or r.get("boot_id") != n["boot_id"]:
                raise ValueError("collector build/host boot identity differs from preflight")
            ready[n["name"]] = r
        result["blockers"].append("Linux public polling has no complete host namespace/interface/process lifecycle or persistent/orphan-namespace provider")
        with (output / "controller.ndjson").open("x") as evidence:
            tick = time.monotonic_ns()
            sequence = 0
            while time.monotonic_ns() < window.deadline:
                nonce = secrets.token_hex(16)
                # The same nonce and sequence bind every host to this barrier.
                sends = {name: r.send(f"SAMPLE {sequence} {nonce}\n") for name, r in readers.items()}
                # Dedicated persistent reader threads already receive and
                # timestamp all pipes concurrently. Pop their queues without
                # creating per-tick helper threads or serializing counter reads.
                replies = {name: r.receive(window.deadline) for name, r in readers.items()}
                checks = {}
                for name, response in replies.items():
                    sample = response["sample"]
                    if name in previous:
                        try:
                            checks[name] = interval(sample, previous[name], ready[name]["hertz"])
                        except (ValueError, KeyError, TypeError) as e:
                            checks[name] = dict(counters_within=False, coverage_complete=False, error=str(e))
                    previous[name] = sample
                end = window.anchor is not None and min(sends.values()) - window.anchor >= QUIET_NS
                status = window.observe(sequence, nonce, sends, replies, checks, end=end)
                evidence.write(json.dumps(dict(sequence=sequence, nonce=nonce, commands=sends,
                                               replies=replies, checks=checks, joint=status)) + "\n")
                evidence.flush()
                sequence += 1
                tick += CADENCE_NS
                remaining = min(tick, window.deadline) - time.monotonic_ns()
                if remaining > 0:
                    time.sleep(remaining / SECOND)
            result["samples"] = sequence
            result["barrier_errors"] = window.errors
    except (OSError, ValueError, KeyError, TypeError, TimeoutError, EOFError, queue.Empty) as e:
        result["blockers"].append(f"collector/controller evidence unavailable: {type(e).__name__}: {e}")
    finally:
        # Native drain is explicit evidence, not inferred from a successful exit.
        # This backend lacks an independent full-host reader across its own exit.
        for name, reader in readers.items():
            reader.close()
            result["collector_exit"][name] = reader.process.returncode
            result.setdefault("collector_cleanup_errors", {})[name] = reader.cleanup_errors
        result["blockers"].append("independent whole-host counter coverage through collector/transport drain is unavailable")
        result["drain_coverage_complete"] = False
        result["finished_ns"] = time.monotonic_ns()
        (output / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def attribution(raw_path, expected_runtimes):
    """Exact argv roles + inode/hash + PID/boot incarnation; diagnostics only.

    Runtime entries: {role, executable, sha256, revision, node_id, data_dir}.
    CPU is the sum of stable TID user/system deltas, never child-time fields or
    process totals (which already aggregate threads). Exits/births/execs cannot
    be silently assigned zero. Namespace bytes remain namespace-wide.
    """
    samples = []
    current = None
    ready = None
    with Path(raw_path).open() as source:
        for line in source:
            row = json.loads(line)
            kind = row.get("type")
            if kind == "ready":
                ready = row
            elif kind == "begin":
                if current is not None:
                    raise ValueError("unfinished/truncated census")
                current = dict(begin=row, processes={}, process_ends={}, threads={}, errors=[])
            elif current is not None and kind == "process":
                if row["tgid"] in current["processes"]:
                    raise ValueError("duplicate process incarnation")
                current["processes"][row["tgid"]] = row
            elif current is not None and kind == "process_end":
                if row["tgid"] in current["process_ends"]:
                    raise ValueError("duplicate process end guard")
                current["process_ends"][row["tgid"]] = row
            elif current is not None and kind == "thread":
                key = (row["tgid"], row["tid"], row["start_ticks"], row["thread_epoch"])
                if not row["thread_epoch"]:
                    raise ValueError("thread pidfd incarnation unavailable")
                if key in current["threads"]:
                    raise ValueError("duplicate thread incarnation")
                current["threads"][key] = row
            elif current is not None and kind == "reader_error":
                current["errors"].append(row)
            elif current is not None and kind == "sample":
                if row["sequence"] != current["begin"]["sequence"] or row["nonce"] != current["begin"]["nonce"]:
                    raise ValueError("raw census sequence/barrier mismatch")
                current["summary"] = row
                samples.append(current)
                current = None
    if current is not None or not ready or not samples:
        raise ValueError("missing/truncated raw counter evidence")
    rows = []
    for old, new in zip(samples, samples[1:]):
        counts = dict(stable_ticks=0, sinomo_ticks=0, observer_ticks=0)
        stable_threads = []
        unknown = []
        born = sorted(new["threads"].keys() - old["threads"].keys())
        exited = sorted(old["threads"].keys() - new["threads"].keys())
        daemons = []
        for key in new["threads"].keys() & old["threads"].keys():
            a, b = old["threads"][key], new["threads"][key]
            ap = old["processes"].get(key[0])
            bp = new["processes"].get(key[0])
            ae = old["process_ends"].get(key[0])
            be = new["process_ends"].get(key[0])
            identity = ("start_ticks", "process_epoch", "uid", "exe_identity", "exe", "argv", "sha256", "exe_errno", "argv_errno")
            if (any(p is None for p in (ap, bp, ae, be))
                    or any(any(p[k] != ap[k] for k in identity) for p in (bp, ae, be))
                    or old["summary"]["boot_id"] != new["summary"]["boot_id"]):
                unknown.append({"thread": key, "reason": "process/exec incarnation unavailable or changed"})
                continue
            deltas = [integer(v) - integer(u) for u, v in zip(a["cpu_ticks"], b["cpu_ticks"])]
            if len(deltas) != 2 or min(deltas) < 0:
                unknown.append({"thread": key, "reason": "thread counter reset"})
                continue
            delta = sum(deltas)
            counts["stable_ticks"] += delta
            times_a, times_b = a.get("read_ns"), b.get("read_ns")
            minimum = (span(times_b)[0] - span(times_a)[1]) if times_a and times_b else None
            stable_threads.append(dict(thread=key, comm=bp.get("comm"), uid=bp.get("uid"),
                                       delta_ticks=delta, minimum_interval_ns=minimum,
                                       percent_one_core_upper=(delta * 100 * SECOND / ready["hertz"] / minimum)
                                       if minimum and minimum > 0 else None))
            if key[0] == ready["pid"] and bp["start_ticks"] == ready["start_ticks"]:
                counts["observer_ticks"] += delta
            argv = bp["argv"]
            role = ("shadow" if argv[1:4] == ["daemon", "shadow", "run"] else
                    "main" if argv[1:3] == ["daemon", "run"] else None)
            matches = [e for e in expected_runtimes if e["role"] == role
                       and e["executable"] == bp["exe"] and e["sha256"] == bp["sha256"]
                       and e["pid"] == key[0] and e["start_ticks"] == bp["start_ticks"]
                       and e["boot_id"] == new["summary"]["boot_id"]]
            if len(matches) == 1:
                runtime = matches[0]
                if not all(runtime.get(k) for k in ("revision", "node_id", "data_dir")):
                    unknown.append({"thread": key, "reason": "runtime receipt incomplete"})
                    continue
                counts["sinomo_ticks"] += delta
                daemons.append({"thread": key, "boot_id": new["summary"]["boot_id"],
                                "process_start_ticks": bp["start_ticks"], "role": role,
                                "runtime": runtime, "delta_ticks": delta})
            elif role is not None or bp["candidate"] or bp["exe_errno"] or bp["argv_errno"]:
                unknown.append({"thread": key, "reason": "unverified runtime, denied identity or unclassified Sinomo candidate"})
        try:
            host = interval(new["summary"], old["summary"], ready["hertz"])
        except (ValueError, KeyError, TypeError) as e:
            host = {"error": str(e)}
        rows.append(dict(sequence=new["summary"]["sequence"], host=host, **counts,
                         stable_threads=stable_threads, sinomo_threads=daemons, unknown=unknown, born=born, exited=exited,
                         process_lifetime_coverage_complete=False,
                         namespace_bytes_are_process_attribution=False))
    return dict(schema="sinomo.native-thread-attribution.v1", boot_id=ready["boot_id"],
                hertz=ready["hertz"], acceptance=False, overhead_subtracted=False,
                qualification="Stable-thread CPU attribution only. Birth/exit/exec gaps, kernel/IRQ/steal and denied identities are explicit. Host CPU remains the gate. Namespace bytes cannot identify a daemon's traffic.", rows=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    capture = sub.add_parser("capture", help="fixed 120s diagnostic controller; never starts daemons")
    capture.add_argument("--config", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    explain = sub.add_parser("attribution")
    explain.add_argument("--raw", type=Path, required=True)
    explain.add_argument("--runtimes", type=Path, required=True)
    explain.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "capture":
        value = run(json.loads(args.config.read_text()), args.output)
        print(json.dumps(value))
        return 0 if value["acceptance"] else 2
    value = attribution(args.raw, json.loads(args.runtimes.read_text()))
    with args.output.open("x") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
