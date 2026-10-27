#!/usr/bin/env python3
"""Read-only I/O counters for one stable, exclusively owned container cgroup.

The observation ends before container teardown. Exited helpers remain charged
to the live group, including supported inode writeback. Removal/replacement
during the observation is unavailable coverage, never a zero counter or a
bridge to another incarnation. This module creates/configures no cgroup.
"""
import os
from pathlib import Path
import select
import time


class IncompleteObservation(RuntimeError):
    pass


def key_values(raw):
    values = {}
    for line in raw.splitlines():
        key, value = line.split()
        if key in values or not value.isdecimal():
            raise IncompleteObservation("invalid or duplicate counter field")
        values[key] = int(value)
    return values


def io_values(raw):
    values = {}
    for line in raw.splitlines():
        device, *fields = line.split()
        if len(device.split(":")) != 2 or not all(p.isdecimal() for p in device.split(":")) or device in values:
            raise IncompleteObservation("invalid or duplicate I/O device")
        row = {}
        for field in fields:
            key, value = field.split("=", 1)
            if key in row or not value.isdecimal():
                raise IncompleteObservation("invalid or duplicate I/O field")
            row[key] = int(value)
        if not {"rbytes", "wbytes", "rios", "wios"} <= row.keys():
            raise IncompleteObservation("required I/O counter missing")
        values[device] = row
    return values


def device_identity(device):
    path = (Path("/sys/dev/block") / device).resolve(strict=True)
    # Accounting can name a partition or its containing disk. Never add two
    # views of the same physical device, or a stacked device and its slaves.
    physical = path.parent if (path / "partition").exists() else path
    slaves = list((physical / "slaves").iterdir())
    if slaves:
        leaves = set()
        for slave in slaves:
            leaves.update(device_identity((slave / "dev").read_text().strip())["leaves"])
    else:
        leaves = {(physical / "dev").read_text().strip()}
    return {
        "path": str(path), "physical_path": str(physical),
        "diskseq": (physical / "diskseq").read_text().strip(),
        "leaves": sorted(leaves),
    }


class OwnedCgroup:
    FILES = ("io.stat", "memory.stat", "memory.events", "memory.max",
             "memory.swap.max", "memory.swap.current", "memory.peak",
             "cgroup.events", "cgroup.procs")

    def __init__(self, path, owner_pid, expected_memory=2147483648):
        self.path = Path(path).resolve(strict=True)
        if not self.path.is_relative_to(Path("/sys/fs/cgroup")):
            raise IncompleteObservation("counter source is outside cgroupfs")
        self.owner_pid = owner_pid
        self.expected_memory = expected_memory
        self.root_fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY)
        self.fds = {}
        self.pidfd = None
        try:
            st = os.fstat(self.root_fd)
            self.identity = [st.st_dev, st.st_ino]
            self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            self.pidfd = os.pidfd_open(owner_pid)
            for key in self.FILES:
                self.fds[key] = os.open(key, os.O_RDONLY, dir_fd=self.root_fd)
        except BaseException:
            self.close()
            raise

    def sample(self):
        row = {"start_ns": time.monotonic_ns(), "path": str(self.path),
               "identity": self.identity, "boot_id": self.boot_id,
               "owner_pid": self.owner_pid, "raw": {}, "unavailable": {}}
        row["owner_alive"] = not bool(select.select([self.pidfd], [], [], 0)[0])
        try:
            st = self.path.stat()
            row["path_matches"] = [st.st_dev, st.st_ino] == self.identity
        except OSError:
            row["path_matches"] = False
        try:
            group = next(line[3:] for line in Path(f"/proc/{self.owner_pid}/cgroup").read_text().splitlines()
                         if line.startswith("0::"))
            row["owner_in_group"] = Path("/sys/fs/cgroup", group.lstrip("/")).resolve() == self.path
        except (OSError, StopIteration):
            row["owner_in_group"] = False
        for key, fd in self.fds.items():
            try:
                data = os.pread(fd, 131073, 0)
                if len(data) > 131072:
                    raise IncompleteObservation("counter response exceeds capture bound")
                row["raw"][key] = data.decode()
            except (OSError, UnicodeError, IncompleteObservation) as error:
                row["unavailable"][key] = str(error)
        # Capture device incarnations at each endpoint, not later when the two
        # endpoints are compared: a replaced device can reuse major:minor.
        try:
            row["devices"] = {key: device_identity(key) for key in io_values(row["raw"]["io.stat"])}
        except (OSError, ValueError, KeyError, IncompleteObservation) as error:
            row["unavailable"]["device_identity"] = str(error)
        row["finish_ns"] = time.monotonic_ns()
        return row

    def validate(self, row):
        if (row["identity"] != self.identity or row["boot_id"] != self.boot_id
                or row["owner_pid"] != self.owner_pid or row["path"] != str(self.path)
                or not row["owner_alive"] or not row["owner_in_group"]
                or not row["path_matches"] or row["unavailable"]):
            raise IncompleteObservation("owned cgroup/owner/counters unavailable or changed")
        if Path("/proc/sys/kernel/random/boot_id").read_text().strip() != self.boot_id:
            raise IncompleteObservation("boot changed")
        raw = row["raw"]
        if int(raw["memory.max"].strip()) != self.expected_memory or raw["memory.swap.max"].strip() != "0":
            raise IncompleteObservation("container memory/no-swap limits changed")
        events = key_values(raw["memory.events"])
        if not {"high", "max", "oom", "oom_kill", "oom_group_kill"} <= events.keys():
            raise IncompleteObservation("memory event coverage unavailable")
        if int(raw["memory.swap.current"].strip()) != 0 or any(events.values()):
            raise IncompleteObservation("memory/swap resource event")
        if int(raw["memory.peak"].strip()) > self.expected_memory:
            raise IncompleteObservation("container memory limit exceeded")
        if key_values(raw["cgroup.events"]).get("populated") != 1:
            raise IncompleteObservation("owned cgroup is not populated")
        memory = key_values(raw["memory.stat"])
        if not {"file_dirty", "file_writeback"} <= memory.keys():
            raise IncompleteObservation("dirty/writeback gauges unavailable")
        io = io_values(raw["io.stat"])
        if not io:
            raise IncompleteObservation("no device counter baseline available")
        devices = row["devices"]
        if devices.keys() != io.keys():
            raise IncompleteObservation("device identity coverage differs from I/O counters")
        seen = set()
        for device in devices.values():
            if seen.intersection(device["leaves"]):
                raise IncompleteObservation("overlapping device accounting cannot be summed")
            seen.update(device["leaves"])
        return {"io": io, "devices": devices,
                "dirty_bytes": memory["file_dirty"], "writeback_bytes": memory["file_writeback"]}

    def interval(self, before, after):
        a, b = self.validate(before), self.validate(after)
        if a["devices"] != b["devices"]:
            raise IncompleteObservation("I/O device set or identity changed; no zero baseline inferred")
        minimum_ns = after["start_ns"] - before["finish_ns"]
        if minimum_ns <= 0:
            raise IncompleteObservation("counter read brackets overlap")
        deltas = {}
        for device in a["io"]:
            if a["io"][device].keys() != b["io"][device].keys():
                raise IncompleteObservation("I/O field set changed")
            delta = {key: b["io"][device][key] - value for key, value in a["io"][device].items()}
            if any(value < 0 for value in delta.values()):
                raise IncompleteObservation("I/O counter reset")
            deltas[device] = delta
        return {"minimum_interval_ns": minimum_ns, "per_device": deltas,
                "physical_read_bytes": sum(d["rbytes"] for d in deltas.values()),
                "physical_write_bytes": sum(d["wbytes"] for d in deltas.values()),
                "dirty_bytes": [a["dirty_bytes"], b["dirty_bytes"]],
                "writeback_bytes": [a["writeback_bytes"], b["writeback_bytes"]]}

    def close(self):
        for fd in self.fds.values():
            os.close(fd)
        self.fds.clear()
        if self.pidfd is not None:
            os.close(self.pidfd)
            self.pidfd = None
        if self.root_fd is not None:
            os.close(self.root_fd)
            self.root_fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
