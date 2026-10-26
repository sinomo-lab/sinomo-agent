#!/usr/bin/env python3
"""One stable Docker I/O window, ending before container teardown.

Keeps existing per-daemon /proc checks and adds live cgroup block counters,
including exited helpers and Btrfs inode writeback. No settings are changed.
All in-container snapshot commands exit before the final cgroup endpoint.
"""
import argparse
import contextlib
import datetime
import json
import os
import signal
from pathlib import Path
import subprocess
import time

from owned_container_io import IncompleteObservation, OwnedCgroup


SNAPSHOT = r'''
set -eu
for comm in /proc/[0-9]*/comm; do
  IFS= read -r name < "$comm" || continue
  [ "$name" = sinomo ] || continue
  directory=${comm%/comm}
  pid=${directory##*/}
  cmd=$(tr '\000' ' ' </proc/$pid/cmdline) || exit 1
  case "$cmd" in
    *" daemon shadow run "*) role=shadow ;;
    *" daemon run "*) role=main ;;
    *) continue ;;
  esac
  IFS= read -r process_stat < /proc/$pid/stat
  stat_fields=${process_stat##*) }
  set -- $stat_fields
  [ "$#" -ge 20 ] || exit 1
  shift 19
  start=$1
  case "$start" in ''|*[!0-9]*) exit 1 ;; esac
  exe=$(stat -Lc '%d:%i:%s:%Y:%Z' /proc/$pid/exe)
  printf '%s %s %s %s\n' "$role" "$pid" "$start" "$exe"
  printf 'stdout %s %s\n' "$(readlink /proc/$pid/fd/1)" "$(stat -Lc '%f:%d:%i' /proc/$pid/fd/1)"
  printf 'stderr %s %s\n' "$(readlink /proc/$pid/fd/2)" "$(stat -Lc '%f:%d:%i' /proc/$pid/fd/2)"
  while IFS= read -r line; do printf '%s\n' "$line"; done < /proc/$pid/io
done
'''


def command(*args):
    return subprocess.check_output(args, text=True, timeout=10)


def inspect(project, nodes):
    ids = command("docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + project).split()
    if not ids:
        raise IncompleteObservation("no live owned containers")
    fmt = '{"id":{{json .Id}},"pid":{{.State.Pid}},"running":{{.State.Running}},"oom":{{.State.OOMKilled}},"restarts":{{.RestartCount}},"memory":{{.HostConfig.Memory}},"swap":{{.HostConfig.MemorySwap}},"node":{{json (index .Config.Labels "com.docker.compose.service")}},"image":{{json .Image}},"mounts":{{json .Mounts}}}'
    selected = {}
    for line in command("docker", "inspect", "--format", fmt, *ids).splitlines():
        row = json.loads(line)
        if row["node"] not in nodes:
            continue
        if row["node"] in selected or not row["running"] or row["oom"] or row["restarts"]:
            raise IncompleteObservation("missing, duplicate, restarted or OOM container")
        if row["memory"] != 2147483648 or row["swap"] != 2147483648:
            raise IncompleteObservation("Docker limits differ from 2 GiB/no swap")
        selected[row["node"]] = row
    if set(selected) != set(nodes):
        raise IncompleteObservation("selected node coverage incomplete")
    return selected


def daemon_snapshot(container):
    raw = command("docker", "exec", container, "sh", "-c", SNAPSHOT)
    rows = {}
    current = None
    for line in raw.splitlines():
        parts = line.split()
        if parts[0] in ("main", "shadow"):
            role, pid, start, exe = parts
            if role in rows:
                raise IncompleteObservation("multiple daemons in one role")
            current = rows[role] = {"identity": [pid, start, exe], "io": {}, "sinks": {}}
        elif parts[0] in ("stdout", "stderr"):
            name, path, identity = parts
            if current is None or name in current["sinks"]:
                raise IncompleteObservation("invalid/duplicate output sink")
            mode, device, inode = identity.split(":")
            if int(mode, 16) & 0o170000 != 0o100000 or not path.startswith("/data/") or path.endswith("(deleted)"):
                raise IncompleteObservation("daemon logging escapes the owned DATA I/O cohort; use the isolated entrypoint's owned-log mode")
            current["sinks"][name] = [path, device, inode]
        else:
            if current is None or len(parts) != 2 or not parts[1].isdecimal():
                raise IncompleteObservation("invalid daemon I/O snapshot")
            current["io"][parts[0].removesuffix(":")] = int(parts[1])
    if set(rows) != {"main", "shadow"} or any(
            not {"rchar", "wchar", "read_bytes", "write_bytes"} <= row["io"].keys()
            or set(row["sinks"]) != {"stdout", "stderr"} for row in rows.values()):
        raise IncompleteObservation("main/shadow I/O coverage incomplete")
    return {"raw": raw, "roles": rows}


def daemon_delta(before, after):
    delta = {}
    for role, row in before["roles"].items():
        end = after["roles"][role]
        if (row["identity"] != end["identity"] or row["io"].keys() != end["io"].keys()
                or row["sinks"] != end["sinks"]):
            raise IncompleteObservation("daemon process/executable/counter identity changed")
        # cancelled_write_bytes is a cumulative count, not a credit against
        # physical or proc writes. No observation overhead is subtracted.
        delta[role] = {key: end["io"][key] - value for key, value in row["io"].items()}
        if any(value < 0 for value in delta[role].values()):
            raise IncompleteObservation("daemon I/O counter reset")
    return delta


def collector_group_members(group):
    members = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) == group and fields[0] != "Z":
                members.append({"pid": int(proc.name), "start_ticks": int(fields[19])})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return members


def completed_legacy_collector(process, record):
    record["exit_code"] = process.wait()
    record["remaining_group_members"] = collector_group_members(process.pid)
    if record["remaining_group_members"]:
        raise IncompleteObservation("legacy idle collector left owned descendants")
    return process.returncode


def measure(args):
    report = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "project": args.project, "window_seconds": args.window, "nodes": {}, "samples": [],
              "qualification": "Stable live owned-container window only; physical cgroup bytes are distinct from logical/proc bytes. Exclusive owned DATA and supported Btrfs writeback required. Main/shadow snapshots and their exited collectors are inside the counter interval. No post-teardown accounting or whole-host acceptance claim."}
    code = 2
    legacy = None
    legacy_log = None
    try:
        with contextlib.ExitStack() as stack:
            owners = inspect(args.project, args.nodes)
            readers = {}
            for node, owner in owners.items():
                group = next(line[3:] for line in Path(f'/proc/{owner["pid"]}/cgroup').read_text().splitlines() if line.startswith("0::"))
                if owner["id"] not in group:
                    raise IncompleteObservation("init PID does not belong to selected Docker cgroup")
                readers[node] = stack.enter_context(OwnedCgroup(Path("/sys/fs/cgroup", group.lstrip("/")), owner["pid"]))
                report["nodes"][node] = {"owner": owner, "before": readers[node].sample()}
                readers[node].validate(report["nodes"][node]["before"])
            for node, owner in owners.items():
                report["nodes"][node]["daemon_before"] = daemon_snapshot(owner["id"])
            if getattr(args, "legacy_idle", False):
                args.output.parent.mkdir(parents=True, exist_ok=True)
                legacy_log = args.output.with_suffix(".legacy.log").open("x")
                environment = dict(os.environ, SINOMO_IDLE_OWNED_IO_CHILD="1")
                legacy_command = ["bash", str(Path(__file__).with_name("measure-idle.sh")), str(args.window)]
                legacy = subprocess.Popen(legacy_command, env=environment, stdout=legacy_log,
                                          stderr=subprocess.STDOUT, start_new_session=True)
                report["legacy_idle"] = {"command": legacy_command, "pid": legacy.pid,
                                         "collector_drain_seconds": 10}
            deadline = time.monotonic() + args.window
            buffered = set()
            while True:
                cut = {"at_monotonic_ns": time.monotonic_ns(), "nodes": {}}
                for node, reader in readers.items():
                    sample = reader.sample()
                    cut["nodes"][node] = sample
                    value = reader.validate(sample)
                    reader.interval(report["nodes"][node]["before"], sample)
                    if value["dirty_bytes"] or value["writeback_bytes"]:
                        buffered.add(node)
                report["samples"].append(cut)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if legacy is None or legacy.poll() is not None:
                        break
                    if time.monotonic() >= deadline + 10:
                        raise IncompleteObservation("legacy idle collector did not drain within ten seconds of the fixed window")
                time.sleep(min(1, max(.05, remaining)))
            if legacy is not None:
                completed_legacy_collector(legacy, report["legacy_idle"])
                legacy_log.close()
                legacy_log = None

            for node, owner in owners.items():
                report["nodes"][node]["daemon_after"] = daemon_snapshot(owner["id"])
            final_owners = inspect(args.project, args.nodes)
            if owners != final_owners:
                raise IncompleteObservation("container ownership changed during the observation")
            failures = []
            if legacy is not None and legacy.returncode != 0:
                failures.append("legacy idle thresholds failed; see retained collector log")
            for node, reader in readers.items():
                row = report["nodes"][node]
                row["after"] = reader.sample()
                row["cgroup_delta"] = reader.interval(row["before"], row["after"])
                row["daemon_delta"] = daemon_delta(row["daemon_before"], row["daemon_after"])
                delta = row["cgroup_delta"]
                if (delta["physical_write_bytes"] or any(delta["dirty_bytes"] + delta["writeback_bytes"])
                        or node in buffered or any(r["write_bytes"] for r in row["daemon_delta"].values())):
                    failures.append(node + ": writes or unsettled dirty/writeback")
                if args.max_read_bps is not None and delta["physical_read_bytes"] > args.max_read_bps * args.window:
                    failures.append(node + ": physical read budget exceeded")
            report["failures"] = failures
            report["complete"] = True
            code = 1 if failures else 0
    except Exception as error:
        report.update(complete=False, error=repr(error))
    finally:
        if legacy is not None and (legacy.poll() is None or collector_group_members(legacy.pid)):
            # Cancel only the collector group created by this observation.
            report["legacy_idle"]["cancelled_in_cleanup"] = True
            for sig, grace in [(signal.SIGTERM, 10), (signal.SIGKILL, 2)]:
                try:
                    os.killpg(legacy.pid, sig)
                except ProcessLookupError:
                    pass
                end = time.monotonic() + grace
                while collector_group_members(legacy.pid) and time.monotonic() < end:
                    time.sleep(.05)
                if not collector_group_members(legacy.pid):
                    break
            legacy.wait(timeout=2)
            report["legacy_idle"]["exit_code"] = legacy.returncode
            report["legacy_idle"]["remaining_group_members"] = collector_group_members(legacy.pid)
        if legacy_log is not None:
            legacy_log.close()
        report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["exit_code"] = code
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        print(json.dumps({"exit_code": code, "complete": report.get("complete"), "output": str(args.output),
                          "failures": report.get("failures"), "error": report.get("error")}))
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--nodes", nargs="+", default=[f"node{i}" for i in range(1, 7)])
    parser.add_argument("--window", type=int, required=True)
    parser.add_argument("--max-read-bps", type=int)
    parser.add_argument("--legacy-idle", action="store_true",
                        help="enclose the unchanged legacy disk/network sampler and its collector exit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.window <= 0 or args.window > 600 or len(set(args.nodes)) != len(args.nodes):
        parser.error("bounded positive window and unique nodes required")
    raise SystemExit(measure(args))
