#!/usr/bin/env python3
"""Unprivileged local Sinomo ownership/counter collector; never starts a daemon.

An operator-pinned receipt supplies ownership intent. Kernel identity checks
validate current objects, not the receipt's provenance or completeness. Network,
unseen handoffs, final process I/O and delayed writeback remain unqualified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import select
import time

import daemon_scope as policy

SCHEMA = "sinomo.owned-counters.v1"
BINDINGS = ("purpose", "node_id", "boot_id", "source_revision", "data_dir",
            "data_identity", "groups", "owners", "delegations")
SECOND = 1_000_000_000


def digest(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def file_identity(stat):
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def object_identity(path):
    stat = path.stat()
    return [stat.st_dev, stat.st_ino]


def raise_walk_error(error):
    raise error


def start_ticks(text):
    end = text.rfind(")")
    policy.require(end >= 0, "invalid process stat")
    return policy.integer(int(text[end+2:].split()[19]))


def io_counters(text):
    policy.require(text.endswith("\n"), "truncated process I/O")
    values = {}
    for line in text.splitlines():
        key, value = line.split(": ")
        policy.require(key not in values and value.isascii() and value.isdecimal(), "invalid process I/O")
        values[key] = policy.integer(int(value))
    policy.require({"read_bytes", "write_bytes", "cancelled_write_bytes"} <= values.keys(), "missing process I/O")
    return values


class PidHandle:
    def __init__(self, pid):
        self.fd = os.pidfd_open(pid)

    def alive(self):
        poller = select.poll()
        poller.register(self.fd, select.POLLIN)
        return not poller.poll(0)

    def close(self):
        os.close(self.fd)


class Kernel:
    """Filesystem roots are injectable for unit tests; the CLI uses real roots."""
    def __init__(self, proc=Path("/proc"), cgroup=Path("/sys/fs/cgroup")):
        self.proc, self.cgroup = proc, cgroup

    def boot(self):
        return (self.proc / "sys/kernel/random/boot_id").read_text().strip()

    def group_path(self, name):
        return self.cgroup / name.lstrip("/")

    def pin(self, pid):
        return PidHandle(pid)


class Collector:
    def __init__(self, manifest_path, expected_sha256, kernel=None):
        self.kernel = kernel or Kernel()
        self.path = Path(manifest_path)
        self.sha256 = expected_sha256
        self.handles, self.group_fds, self.ledger, self.hashes = {}, {}, {}, {}
        self.last_cpu = None
        self.sticky_errors = []
        self.previous_usage = {}
        try:
            raw = self.path.read_bytes()
            policy.require(hashlib.sha256(raw).hexdigest() == expected_sha256, "manifest digest mismatch")
            self.manifest = json.loads(raw)
            m = self.manifest
            policy.require(m["schema"] == SCHEMA, "manifest schema mismatch")
            policy.require(m["purpose"] in ("runtime", "isolated-control"), "manifest purpose missing")
            for key in ("node_id", "boot_id"):
                policy.require(type(m[key]) is str and m[key], "manifest identity missing")
            policy.require(len(m["source_revision"]) == 40 and all(c in "0123456789abcdef" for c in m["source_revision"]), "exact source revision required")
            receipt = m["runtime_receipt"]
            receipt_path = Path(receipt["path"])
            policy.require(receipt_path.is_absolute(), "receipt path must be absolute")
            receipt_raw = receipt_path.read_bytes()
            policy.require(hashlib.sha256(receipt_raw).hexdigest() == receipt["sha256"], "runtime receipt digest mismatch")
            value = json.loads(receipt_raw)
            policy.require(all(value[k] == m[k] for k in BINDINGS), "runtime receipt binding mismatch")
            self.check_binding()
            roots = {key: dict(path=row["path"], incarnation=str(row["identity"]), source="cgroup-v2-cpu.stat")
                     for key, row in m["groups"].items()}
            policy.validate_roots(roots)
            for key, group in m["groups"].items():
                path = self.kernel.group_path(group["path"])
                policy.require(object_identity(path) == group["identity"], "cgroup identity mismatch")
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
                self.group_fds[key] = fd
                policy.require([os.fstat(fd).st_dev, os.fstat(fd).st_ino] == group["identity"], "cgroup changed while pinning")
            roles, pids = [], set()
            for owner in m["owners"]:
                identity = owner["id"]
                policy.require(type(identity) is str and identity and identity not in self.ledger, "duplicate owner")
                policy.require(owner["group"] in m["groups"] and owner["ownership_ref"], "unbound owner")
                pid = policy.integer(owner["pid"])
                policy.require(pid > 0 and pid not in pids, "duplicate/invalid owner PID")
                pids.add(pid)
                policy.require(owner["role"] in ("main", "shadow", "helper", "control"), "unknown owner role")
                roles.append(owner["role"])
                self.ledger[identity] = dict(owner=owner, state="unobserved", last_io=None, final_io=None)
                # Do not replace a pinned exited identity with a new PID occupant.
                self.handles[identity] = self.kernel.pin(pid)
                self.read_owner(owner)
            policy.require(roles, "empty ownership manifest")
            if m["purpose"] == "runtime":
                policy.require(roles.count("main") == roles.count("shadow") == 1 and "control" not in roles,
                               "runtime needs exactly one main and shadow")
            policy.require(isinstance(m["delegations"], list), "delegation inventory missing")
            for delegation in m["delegations"]:
                policy.require(delegation["owner_id"] in self.ledger and delegation["operation_id"]
                               and delegation["ownership_ref"], "unbound delegation")
        except Exception:
            self.close()
            raise

    def close(self):
        for handle in self.handles.values():
            handle.close()
        for fd in self.group_fds.values():
            os.close(fd)
        self.handles.clear()
        self.group_fds.clear()

    def check_binding(self):
        m = self.manifest
        policy.require(self.kernel.boot() == m["boot_id"], "boot identity mismatch")
        policy.require(digest(self.path) == self.sha256, "manifest changed during capture")
        policy.require(digest(m["runtime_receipt"]["path"]) == m["runtime_receipt"]["sha256"], "runtime receipt changed")
        data = Path(m["data_dir"])
        policy.require(data.is_absolute() and data.is_dir() and object_identity(data) == m["data_identity"],
                       "DATA root identity mismatch")

    def read_owner(self, owner):
        begin = time.monotonic_ns()
        handle = self.handles[owner["id"]]
        policy.require(handle.alive(), "process exited; final I/O unavailable")
        base = self.kernel.proc / str(owner["pid"])
        first = start_ticks((base / "stat").read_text())
        policy.require(first == owner["start_ticks"], "PID/start identity mismatch")
        argv = (base / "cmdline").read_bytes().split(b"\0")
        policy.require(argv[-1] == b"", "truncated command line")
        policy.require([x.decode() for x in argv[:-1]] == owner["argv"], "runtime command identity mismatch")
        exe = base / "exe"
        target = os.readlink(exe)
        policy.require(target == owner["executable"] and Path(target).is_absolute(), "executable path mismatch")
        before = file_identity(exe.stat())
        key = tuple(before)
        if key not in self.hashes:
            self.hashes[key] = digest(exe)
        policy.require(self.hashes[key] == owner["sha256"], "executable digest mismatch")
        memberships = [line[3:] for line in (base / "cgroup").read_text().splitlines() if line.startswith("0::")]
        policy.require(len(memberships) == 1, "unified cgroup membership unavailable")
        group = self.manifest["groups"][owner["group"]]["path"]
        actual = PurePosixPath(memberships[0])
        declared = PurePosixPath(group)
        moved = actual != declared and declared not in actual.parents
        io_error = None
        try:
            values = io_counters((base / "io").read_text())
        except (OSError, ValueError) as error:
            values, io_error = None, str(error)
        policy.require(handle.alive() and start_ticks((base / "stat").read_text()) == first,
                       "process exited/changed during read; final I/O unavailable")
        policy.require(file_identity(exe.stat()) == before and os.readlink(exe) == target,
                       "executable changed during read")
        policy.require((base / "cgroup").read_text().splitlines() == ["0::" + memberships[0]],
                       "membership changed during read")
        return dict(read_ns=[begin, time.monotonic_ns()], io=values, actual_cgroup=memberships[0],
                    moved=moved, io_error=io_error, executable_sha256=self.hashes[key])

    def sample(self, sequence):
        begin = time.monotonic_ns()
        errors, roots, members = [], {}, set()
        try:
            self.check_binding()
        except (OSError, ValueError) as error:
            errors.append(str(error))
        cpu_start = time.monotonic_ns()
        for key, group in self.manifest["groups"].items():
            path = self.kernel.group_path(group["path"])
            try:
                policy.require(object_identity(path) == group["identity"], "cgroup recreated/missing")
                usage = policy.parse_cpu_stat((path / "cpu.stat").read_text())
                policy.require(usage >= self.previous_usage.get(key, 0), "cgroup CPU counter reset")
                self.previous_usage[key] = usage
                # Read only the declared disjoint subtrees, never a host census.
                for directory, dirs, files in os.walk(path, onerror=raise_walk_error):
                    current = Path(directory)
                    identity = object_identity(current)
                    policy.require(not any((current / name).is_symlink() for name in dirs), "symlink in cgroup tree")
                    text = (current / "cgroup.procs").read_text()
                    for pid in text.splitlines():
                        policy.require(pid.isascii() and pid.isdecimal(), "invalid cgroup member")
                        members.add(int(pid))
                    policy.require(object_identity(current) == identity, "cgroup census changed")
                policy.require(object_identity(path) == group["identity"], "cgroup changed during read")
                roots[key] = dict(path=group["path"], incarnation=self.sha256+":"+key,
                                  source="cgroup-v2-cpu.stat", usage_usec=usage)
            except (OSError, ValueError) as error:
                errors.append(key + ": " + str(error))
        cpu_end = time.monotonic_ns()
        known_pids = {entry["owner"]["pid"] for entry in self.ledger.values()}
        unknown = sorted(members - known_pids)
        if unknown:
            errors.append("unmanifested cgroup members: " + ",".join(map(str, unknown)))
        io_errors = []
        for identity, entry in self.ledger.items():
            owner = entry["owner"]
            try:
                row = self.read_owner(owner)
                if row["io"] is not None and entry["last_io"] is not None:
                    policy.require(all(row["io"][key] >= entry["last_io"][key] for key in
                                       ("read_bytes", "write_bytes", "cancelled_write_bytes")), "process I/O counter reset")
                entry.update(state="moved" if row["moved"] else "observed", observation=row)
                if row["io"] is not None:
                    entry["last_io"] = row["io"]
                else:
                    io_errors.append(identity + ": I/O unavailable: " + row["io_error"])
                if row["moved"]:
                    errors.append(identity + ": moved outside declared root; CPU handoff missing")
                    io_errors.append(identity + ": retained I/O outside declared root; delegation unqualified")
            except (OSError, ValueError) as error:
                entry["state"] = "retired-unfinalized" if not self.handles[identity].alive() else "identity-or-counter-error"
                entry["error"] = str(error)
                io_errors.append(identity + ": " + str(error))
                if entry["state"] != "retired-unfinalized" or owner["role"] in ("main", "shadow"):
                    errors.append(identity + ": " + str(error))
                if entry["state"] == "retired-unfinalized" and owner["pid"] in members:
                    errors.append(identity + ": retired PID still listed; occupant incarnation unqualified")
        self.sticky_errors = sorted(set(self.sticky_errors + errors + io_errors))
        cpu = dict(scope=policy.SCOPE, boot_id=self.manifest["boot_id"], read_ns=[cpu_start, cpu_end],
                   roots=roots, collection_errors=errors)
        comparison = None
        if self.last_cpu is not None:
            try:
                comparison = policy.cpu_interval(cpu, self.last_cpu)
            except (ValueError, KeyError, TypeError) as error:
                comparison = dict(within=None, error=str(error))
        self.last_cpu = cpu
        all_seen = not io_errors and all(entry["last_io"] is not None for entry in self.ledger.values())
        observed = {key: sum(entry["last_io"][key] for entry in self.ledger.values()) if all_seen else None
                    for key in ("read_bytes", "write_bytes")}
        return dict(schema=SCHEMA, sequence=sequence, purpose=self.manifest["purpose"],
                    node_id=self.manifest["node_id"], source_revision=self.manifest["source_revision"],
                    manifest_sha256=self.sha256, begin_ns=begin, end_ns=time.monotonic_ns(),
                    cpu=cpu, cpu_interval=comparison, members=sorted(members), unknown_members=unknown,
                    owners=json.loads(json.dumps(self.ledger)),
                    disk=dict(scope=policy.SCOPE, owner_epoch=self.sha256, read_ns=[begin,time.monotonic_ns()],
                              semantics="observed-process-block-bytes-unfinalized", **observed,
                              errors=io_errors, final_counters_complete=False),
                    network=dict(available=False, reason="No complete owned socket/interface-equivalent source; namespace/RPC substitution forbidden"),
                    writeback=dict(complete=False, reason="Process observations do not prove delayed Sinomo writeback/drain"),
                    delegations=self.manifest["delegations"],
                    ownership_complete=False, errors=self.sticky_errors, acceptance=False)


def capture(manifest, manifest_sha256, output, samples=121):
    started = time.monotonic_ns()
    deadline = started + policy.POLICY["absolute_deadline_seconds"] * SECOND
    policy.require(type(samples) is int and 1 <= samples <= 121, "samples must be 1..121; reduced runs are diagnostic")
    output.mkdir(parents=True, exist_ok=False)
    receipt = dict(schema=SCHEMA, acceptance=False, verdict="BLOCKED", scope=policy.SCOPE,
                   whole_host_gate=False, started_ns=started, absolute_deadline_ns=deadline,
                   manifest_sha256=manifest_sha256, samples=0, requested_samples=samples,
                   reduced_diagnostic=samples < 121, collection_completed=False,
                   policy=dict(policy.POLICY), errors=[], gaps=["unseen helper/delegated lifetime ownership",
                   "final process I/O and Sinomo writeback", "complete owned network source/byte semantics"],
                   tool_sha256={p.name:digest(p) for p in (Path(__file__),Path(policy.__file__))})
    collector = None
    try:
        collector = Collector(manifest, manifest_sha256)
        receipt["purpose"] = collector.manifest["purpose"]
        receipt["source_revision"] = collector.manifest["source_revision"]
        tick = time.monotonic_ns()
        with (output / "samples.ndjson").open("x") as raw:
            for sequence in range(samples):
                now = time.monotonic_ns()
                if now >= deadline:
                    break
                if now < tick:
                    time.sleep((min(tick,deadline)-now)/SECOND)
                if time.monotonic_ns() >= deadline:
                    break
                sent = time.monotonic_ns()
                if sent-tick > 50_000_000:
                    receipt["errors"].append("one-second cadence missed")
                row = collector.sample(sequence)
                row.update(scheduled_ns=tick,sent_ns=sent,received_ns=time.monotonic_ns())
                if row["received_ns"] > deadline:
                    receipt["errors"].append("sample completed after original absolute deadline")
                raw.write(json.dumps(row)+"\n");raw.flush()
                receipt["samples"] += 1
                tick += SECOND
        receipt["errors"].extend(collector.sticky_errors)
        receipt["collection_completed"] = receipt["samples"] > 0 and not receipt["errors"]
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        receipt["errors"].append(str(error))
    finally:
        if collector:
            collector.close()
        receipt["finished_ns"] = time.monotonic_ns()
        with (output / "receipt.json").open("x") as stream:
            json.dump(receipt,stream,indent=2);stream.write("\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=121, help="bounded diagnostic sample count; no acceptance shortcut")
    args = parser.parse_args()
    value = capture(args.manifest,args.manifest_sha256,args.output,args.samples)
    print(json.dumps(value,indent=2))
    return 2  # Coverage remains incomplete, independently of observed CPU rates.


if __name__ == "__main__":
    raise SystemExit(main())
