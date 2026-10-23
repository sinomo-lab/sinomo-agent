"""Diagnostic Linux block counters; never a whole-host disk acceptance gate.

Keep every layer separate: summing disks, partitions and device-mapper rows
would count the same I/O repeatedly. A major/minor/name is not a lifetime ID.
"""

SECOND = 1_000_000_000
MAX_GAP_NS = 5 * SECOND


def uint(value):
    if type(value) is not int or not 0 <= value < 2**64:
        raise ValueError("invalid disk counter")
    return value


def decimal(value):
    if not value or not value.isascii() or not value.isdecimal():
        raise ValueError("invalid disk counter text")
    return uint(int(value))


def parse_diskstats(text):
    if not isinstance(text, str) or not text.endswith("\n"):
        raise ValueError("diskstats unavailable or truncated")
    devices = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) not in (14, 18, 20):
            raise ValueError("unsupported or incomplete diskstats fields")
        major, minor = map(decimal, parts[:2])
        key = f"{major}:{minor}"
        if key in devices:
            raise ValueError("duplicate disk identity")
        devices[key] = {"name": parts[2], "counters": list(map(decimal, parts[3:]))}
    if not devices:
        raise ValueError("empty disk inventory")
    return devices


def settling(row):
    text = row.get("vmstat")
    if not isinstance(text, str) or not text.endswith("\n"):
        raise ValueError("dirty/writeback counters unavailable or truncated")
    fields = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] in fields:
            raise ValueError("invalid vmstat fields")
        fields[parts[0]] = decimal(parts[1])
    page = uint(row["page_size"])
    if not page or "nr_dirty" not in fields or "nr_writeback" not in fields:
        raise ValueError("dirty/writeback counters unavailable")
    return {"dirty_bytes": fields["nr_dirty"] * page,
            "writeback_bytes": fields["nr_writeback"] * page}


def interval(current, previous):
    """Conservative per-device deltas, independent of CPU/network availability."""
    if not current.get("boot_id") or current["boot_id"] != previous.get("boot_id"):
        raise ValueError("disk host boot changed or unavailable")
    if uint(current["sequence"]) != uint(previous["sequence"]) + 1:
        raise ValueError("disk sample sequence gap")
    spans = []
    inventories = []
    gauges = []
    for sample in (previous, current):
        disk = sample["disk"]
        if disk.get("read_errors") != []:
            raise ValueError("disk reader failed or error evidence missing")
        times = disk["read_ns"]
        if not isinstance(times, list) or len(times) != 2:
            raise ValueError("disk read bracket unavailable")
        start, end = map(uint, times)
        if not uint(sample["begin_ns"]) <= start <= end <= uint(sample["end_ns"]):
            raise ValueError("disk read outside sample")
        spans.append((start, end))
        inventories.append(parse_diskstats(disk.get("diskstats")))
        gauges.append(settling(disk))
    gap = spans[1][0] - spans[0][1]
    if not 0 < gap <= MAX_GAP_NS:
        raise ValueError("disk read overlap or gap")
    old, new = inventories
    if old.keys() != new.keys():
        raise ValueError("disk inventory changed")
    if previous["disk"]["page_size"] != current["disk"]["page_size"]:
        raise ValueError("page size changed")
    devices = {}
    for key, entry in new.items():
        before, after = old[key]["counters"], entry["counters"]
        if entry["name"] != old[key]["name"] or len(before) != len(after):
            raise ValueError("disk identity or counter schema changed")
        delta = [b - a for a, b in zip(before, after)]
        if any(d < 0 for i, d in enumerate(delta) if i != 8):
            raise ValueError("disk counter reset")
        # Kernel sector units are always 512 bytes, not device logical blocks.
        read_bytes, write_bytes = delta[2] * 512, delta[6] * 512
        devices[key] = dict(name=entry["name"], read_bytes=read_bytes,
                            write_bytes=write_bytes, reads_completed=delta[0],
                            writes_completed=delta[4],
                            read_Bps_upper=read_bytes * SECOND / gap,
                            write_Bps_upper=write_bytes * SECOND / gap,
                            read_write_IOPS_upper=(delta[0] + delta[4]) * SECOND / gap,
                            in_flight_before=before[8], in_flight_after=after[8],
                            read_ms=delta[3], write_ms=delta[7],
                            io_ms=delta[9], weighted_io_ms=delta[10],
                            discards_completed=delta[11] if len(delta) >= 15 else None,
                            discarded_bytes=delta[13] * 512 if len(delta) >= 15 else None,
                            flushes_completed=delta[15] if len(delta) == 17 else None,
                            flush_ms=delta[16] if len(delta) == 17 else None)
    return dict(acceptance=False, coverage_complete=False, verdict="DIAGNOSTIC",
                minimum_interval_ns=gap, devices=devices,
                settling_before=gauges[0], settling_after=gauges[1],
                aggregate=None, sync_syscalls=None, approved_native_limits=None,
                blockers=["block-device topology and continuous lifetime coverage unavailable",
                          "rows overlap; no whole-host aggregate or physical-layer selection",
                          "flush counters are not fsync/fdatasync calls; partition flush coverage unavailable",
                          "endpoint dirty/writeback gauges do not prove drain or absence of deferred work",
                          "optional host disk diagnostics apply no rollout limits"])
