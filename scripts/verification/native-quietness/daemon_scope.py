"""Sinomo process-family quietness policy and offline counter checks.

Whole-host diagnostics are not inputs. These functions do not authenticate an
ownership manifest or establish live acceptance from a supplied JSON fixture.
"""
from pathlib import PurePosixPath

from controller import integer, span

SCOPE = "sinomo-main-shadow-and-attributable-work"
POLICY = dict(scope=SCOPE, whole_host_limits=False, cpu_percent_one_core=10,
              network_combined_Bps=65_536, network_burst_bytes=0,
              quiet_seconds=30, absolute_deadline_seconds=120,
              disk_window_seconds=60, disk_read_Bps=10_240,
              disk_write_Bps=0, disk_write_bytes=0)
COVERAGE = ("main_shadow_identity", "helper_and_detached_work_ownership",
            "cpu_accounting", "disk_accounting", "network_accounting",
            "sinomo_writeback_and_deferred_work")
SECOND = 1_000_000_000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_cpu_stat(text):
    require(isinstance(text, str) and text.endswith("\n"), "cpu.stat unavailable/truncated")
    fields = {}
    for line in text.splitlines():
        key, value = line.split()
        require(key not in fields and value.isascii() and value.isdecimal(), "invalid cpu.stat")
        fields[key] = integer(int(value))
    require("usage_usec" in fields, "cpu.stat usage missing")
    return fields["usage_usec"]


def validate_roots(roots):
    require(isinstance(roots, dict) and roots, "owned cgroup roots unavailable")
    paths = []
    for name, row in roots.items():
        path = PurePosixPath(row["path"])
        # PurePosixPath preserves a leading //, but Linux cgroup paths resolve
        # it as /. Reject that alias before comparing roots for overlap.
        require(path.anchor == "/" and path != PurePosixPath("/") and ".." not in path.parts,
                "invalid or host-wide cgroup root")
        require(all(path != p and path not in p.parents and p not in path.parents for p in paths),
                "overlapping cgroups double-count descendants")
        paths.append(path)
        require(type(name) is str and name and type(row["incarnation"]) is str and row["incarnation"],
                "cgroup incarnation missing")
        require(row["source"] == "cgroup-v2-cpu.stat", "CPU counter source is not family scoped")


def cpu_interval(current, previous):
    """Sum disjoint owned cgroup subtrees, retaining exited children's CPU."""
    require(current["scope"] == previous["scope"] == SCOPE, "wrong quietness scope")
    require(not current.get("collection_errors") and not previous.get("collection_errors"),
            "CPU ownership/counter collection incomplete")
    require(current["boot_id"] and current["boot_id"] == previous["boot_id"], "host boot changed")
    old, new = previous["roots"], current["roots"]
    validate_roots(old)
    validate_roots(new)
    require(old.keys() == new.keys(), "owned cgroup inventory changed")
    elapsed = span(current["read_ns"])[0] - span(previous["read_ns"])[1]
    require(0 < elapsed <= 5 * SECOND, "CPU read overlap or stall")
    usage = 0
    for key in new:
        a, b = old[key], new[key]
        require(a["path"] == b["path"] and a["incarnation"] == b["incarnation"], "cgroup recreated")
        delta = integer(b["usage_usec"]) - integer(a["usage_usec"])
        require(delta >= 0, "cgroup CPU reset")
        usage += delta
    return dict(cpu_usec=usage, minimum_interval_ns=elapsed,
                within=usage * 1000 * 100 <= POLICY["cpu_percent_one_core"] * elapsed,
                percent_one_core_upper=usage * 1000 * 100 / elapsed,
                qualification="owned roots only; detached/delegated ownership requires separate evidence")


def network_interval(current, previous):
    """Reject namespace/shared-host and incomplete framed-RPC substitutions."""
    for row in (previous, current):
        require(row["scope"] == SCOPE, "network scope mismatch")
        require(row["semantics"] == "complete-owned-interface-equivalent-TX-RX",
                "network accounting scope/byte semantics unresolved")
        require(row["includes_loopback"] is True, "loopback omitted")
    require(current["owner_epoch"] and current["owner_epoch"] == previous["owner_epoch"],
            "network owner incarnation changed")
    elapsed = span(current["read_ns"])[0] - span(previous["read_ns"])[1]
    require(0 < elapsed <= 5 * SECOND, "network read overlap or stall")
    delta = [integer(current[k]) - integer(previous[k]) for k in ("tx_bytes", "rx_bytes")]
    require(min(delta) >= 0, "network counter reset")
    return dict(tx_bytes=delta[0], rx_bytes=delta[1], minimum_interval_ns=elapsed,
                within=sum(delta) * SECOND <= POLICY["network_combined_Bps"] * elapsed)


def disk_window(current, previous):
    """Preserve the separate legacy 60-second read/zero-write criterion."""
    for row in (previous, current):
        require(row["scope"] == SCOPE, "disk scope mismatch")
        require(row["semantics"] == "owned-process-block-bytes-with-retired-work",
                "disk accounting cannot substitute logical or whole-host bytes")
    require(current["owner_epoch"] and current["owner_epoch"] == previous["owner_epoch"],
            "disk owner incarnation changed")
    elapsed = span(current["read_ns"])[0] - span(previous["read_ns"])[1]
    require(elapsed >= POLICY["disk_window_seconds"] * SECOND, "legacy disk window shorter than 60 seconds")
    delta = [integer(current[k]) - integer(previous[k]) for k in ("read_bytes", "write_bytes")]
    require(min(delta) >= 0, "disk counter reset or lost retired work")
    return dict(read_bytes=delta[0], write_bytes=delta[1], minimum_interval_ns=elapsed,
                within=delta[0] * SECOND <= POLICY["disk_read_Bps"] * elapsed and delta[1] == 0)


def readiness(evidence):
    """Review recorded gates; host diagnostics, privileges and host disk policy do not gate.

    No live adapter is qualified here. A complete fixture can satisfy the review
    shape, but cannot grant rollout acceptance via a boolean in a document.
    """
    blockers = []
    coverage = evidence.get("coverage", {})
    for key in COVERAGE:
        value = coverage.get(key, {})
        if value.get("complete") is not True or not value.get("evidence_ref"):
            blockers.append(key)
    windows = evidence.get("windows", {})
    try:
        joint, disk, elapsed = [integer(windows[k]) for k in
                                ("cpu_network_joint_ns", "disk_ns", "elapsed_ns")]
    except (ValueError, KeyError, TypeError):
        joint, disk, elapsed = 0, 0, POLICY["absolute_deadline_seconds"] * SECOND + 1
        blockers.append("window timing unavailable or invalid")
    if joint < POLICY["quiet_seconds"] * SECOND:
        blockers.append("joint CPU/network window shorter than 30 seconds")
    if disk < POLICY["disk_window_seconds"] * SECOND:
        blockers.append("legacy disk window shorter than 60 seconds")
    if elapsed > POLICY["absolute_deadline_seconds"] * SECOND:
        blockers.append("original absolute deadline missing or exceeded")
    if max(joint, disk) > elapsed:
        blockers.append("claimed window exceeds the original run interval")
    if windows.get("continuous_cadence") is not True:
        blockers.append("continuous observations unproved")
    for key in ("cpu", "disk", "network"):
        if evidence.get("limits", {}).get(key) is not True:
            blockers.append(key + " limits failed or unmeasured")
    if evidence.get("scope") != SCOPE:
        blockers.append("process-family scope not established")
    return dict(scope=SCOPE, rollout_gate=True, whole_host_gate=False,
                policy=dict(POLICY), recorded_requirements_satisfied=not blockers,
                blockers=blockers, acceptance=False,
                live_verdict="NOT_QUALIFIED",
                qualification="Offline evidence review; live ownership/counter adapter not qualified")
