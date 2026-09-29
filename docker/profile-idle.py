#!/usr/bin/env python3
"""Collect a bounded idle diagnostic using the standard Observe capture/report tools.

Does not start, stop or build the cluster. The separate whole-container quietness
gate remains the acceptance authority. Missing counters and trace loss remain
explicit in the generated report.
"""
import argparse
import concurrent.futures
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("write_profiles", ROOT / "docker/profile-dfs-writes.py")
profiles = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profiles)

PROCESS_SAMPLE = r'''
getconf CLK_TCK
cat /proc/uptime
for proc in /proc/[0-9]*; do
  IFS= read -r comm < "$proc/comm" 2>/dev/null || continue
  [ "$comm" = sinomo ] || continue
  cmd=$(tr "\000" " " < "$proc/cmdline") || continue
  case " $cmd " in
    *" daemon shadow run "*) role=shadow ;;
    *" daemon run "*) role=main ;;
    *) continue ;;
  esac
  printf '%s\n' "$role"
  cat "$proc/stat" "$proc/io" || exit 1
  printf '\n'
done
'''


def parse_process_sample(raw):
    lines = raw.splitlines()
    snapshot = {"ticks_per_second": int(lines[0]), "monotonic_seconds": float(lines[1].split()[0]), "processes": {}}
    for block in '\n'.join(lines[2:]).strip().split('\n\n'):
        rows = block.splitlines()
        if not rows:
            continue
        role, stat = rows[:2]
        fields = stat.rsplit(') ', 1)[1].split()
        if role in snapshot['processes']:
            raise ValueError(f"duplicate {role} process")
        snapshot['processes'][role] = {"pid": int(stat.split()[0]), "start_ticks": int(fields[19]),
            "cpu_ticks": int(fields[11]) + int(fields[12]),
            "io": {line.split(':')[0]: int(line.split(':')[1]) for line in rows[2:]}}
    if set(snapshot['processes']) != {'main', 'shadow'}:
        raise ValueError("expected exactly one main and one shadow")
    return snapshot


def process_window(root, clients, seconds):
    """Workload hook: capture both daemon roles around the same idle interval."""
    def snapshot(item, index):
        node, client = item
        try:
            result = subprocess.run(client[:3] + ["sh", "-c", PROCESS_SAMPLE],
                                    capture_output=True, text=True, timeout=10, check=True)
            (root / f"{node}-process-{index}.txt").write_text(result.stdout)
            value = parse_process_sample(result.stdout)
        except Exception as error:
            value = {"error": str(error)}
        profiles.save(root / f"{node}-process-{index}.json", value)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda item: snapshot(item, 0), clients.items()))
        time.sleep(seconds)
        list(pool.map(lambda item: snapshot(item, 1), clients.items()))


def process_deltas(before, after):
    elapsed = after.get('monotonic_seconds', 0) - before.get('monotonic_seconds', 0)
    rows = []
    for role in ('main', 'shadow'):
        first, last = before.get('processes', {}).get(role), after.get('processes', {}).get(role)
        row = {"role": role, "cpu_percent": None, "read_bytes": None, "write_bytes": None,
               "read_syscalls": None, "write_syscalls": None, "window_seconds": elapsed}
        if (first and last and elapsed > 0 and before.get('ticks_per_second', 0) > 0
                and before['ticks_per_second'] == after.get('ticks_per_second')
                and (first['pid'], first['start_ticks']) == (last['pid'], last['start_ticks'])):
            row.update(pid=last['pid'], start_ticks=last['start_ticks'])
            ticks = profiles.reporter.counter_delta(first['cpu_ticks'], last['cpu_ticks'])
            row['cpu_percent'] = ticks / before['ticks_per_second'] / elapsed * 100 if ticks is not None else None
            for field, source in [('read_bytes', 'read_bytes'), ('write_bytes', 'write_bytes'),
                                  ('read_syscalls', 'syscr'), ('write_syscalls', 'syscw')]:
                row[field] = profiles.reporter.counter_delta(first['io'].get(source), last['io'].get(source))
        rows.append(row)
    return rows


def main():
    if sys.argv[1:2] == ['--process-window']:
        root = Path(sys.argv[2])
        process_window(root, json.loads((root / 'clients.json').read_text()), int(sys.argv[3]))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=15)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 20:
        parser.error("seconds must be between 1 and 20 for the bounded CPU capture")
    args.artifacts = args.artifacts.resolve()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    run = profiles.latency.acceptance.Acceptance(args)
    result = {"schema": "sinomo.idle-diagnostics.v1", "project": args.project,
              "binary_sha256": run.binary_hash, "source_revision": args.source_revision,
              "profile": args.profile, "seconds": args.seconds, "complete": False,
              "classification": "diagnostic", "errors": [], "nodes": {}, "cpu_profiles": []}
    capture = run.artifacts / "idle"
    try:
        # Use observation-only identity reads, never a readiness/status command
        # which could initiate maintenance in the period being diagnosed.
        def identity(alias):
            command = [*run.compose, "exec", "-T", alias, "/usr/local/bin/sinomo",
                       "observe", "traces", "--target-prefix", "__idle_identity__", "--limit", "0", "--json"]
            completed = subprocess.run(command, env=run.env, capture_output=True, timeout=20, check=True)
            (run.artifacts / (alias + "-identity.json")).write_bytes(completed.stdout)
            return alias, json.loads(completed.stdout)["process"]["node_id"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            nodes = dict(pool.map(identity, [f"node{i}" for i in range(1, 7)]))
        result["nodes"] = nodes
        if len(set(nodes.values())) != 6:
            raise ValueError("expected six distinct node identities")
        clients = {node: ["docker", "exec", f"{args.project}-{alias}-1", "/usr/local/bin/sinomo"]
                   for alias, node in nodes.items()}
        profiles.save(run.artifacts / "clients.json", clients)
        command = ["python3", str(ROOT / "scripts/debug/capture-performance.py"), "--buffered",
                   "--cpu-profile", "--workload-kind", "idle", "--metrics-prefix", "sinomo.dfs.background", "--nodes", *nodes.values(),
                   "--clients-file", str(run.artifacts / "clients.json"), "--output", str(capture),
                   "--source-revision", args.source_revision, "--binary-sha256", run.binary_hash,
                   "--profile", args.profile, "--duration-seconds", str(args.seconds + 10),
                   "--", "python3", str(Path(__file__).resolve()), "--process-window",
                   str(run.artifacts), str(args.seconds)]
        profiles.save(run.artifacts / "capture.command.json", command)
        with (run.artifacts / "capture.log").open("w") as log:
            result["capture_exit"] = subprocess.run(command, env=run.env, stdout=log, stderr=subprocess.STDOUT).returncode
        report = profiles.reporter.summarize(capture)
        profiles.save(capture / "performance.json", report)
        (capture / "PERFORMANCE.md").write_text(profiles.reporter.markdown(report))
        result["cpu_profiles"] = profiles.collect_cpu_profiles(run, capture, "idle", nodes)
        result["process_windows"] = []
        for alias, node in nodes.items():
            endpoints = [run.artifacts / f"{node}-process-{index}.json" for index in (0, 1)]
            before, after = [json.loads(path.read_text()) if path.exists() else {} for path in endpoints]
            result["process_windows"].extend(dict(row, node=alias) for row in process_deltas(before, after))
        result["trace_complete"] = report["complete"]
        result["complete"] = (result["capture_exit"] == 0 and report["complete"]
                              and len(result["cpu_profiles"]) == 6
                              and all(row[field] is not None for row in result['process_windows']
                                      for field in ['cpu_percent', 'read_bytes', 'write_bytes', 'read_syscalls', 'write_syscalls']))
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        lines = ["# Cluster idle diagnostics", "",
                 f"Project: `{args.project}`. Source: `{args.source_revision}`. Binary: `{run.binary_hash}`. Profile: `{args.profile}`.", "",
                 "This bounded idle capture is diagnostic and does not establish quietness acceptance. "
                 "The verification harness runs it alongside the unchanged whole-container quietness gate. "
                 "CPU profiles cover main daemons; gate CPU also includes shadows and helpers. All observation "
                 "overhead remains charged to the gate. CPU sampling excludes the system-library ranges listed in each manifest.", "",
                 f"Capture coverage: **{'complete' if result['complete'] else 'partial / unavailable'}**. "
                 "[Standard stage/I/O, node-window, activity and coverage tables](idle/PERFORMANCE.md).", "",
                 "| Node | CPU samples | Profile |", "|---|---:|---|"]
        for row in result["cpu_profiles"]:
            links = f"[pprof]({row['artifact']})"
            if row.get("flamegraph"):
                links += f" / [flamegraph]({row['flamegraph']}) / [top]({row['top']})"
            lines.append(f"| {row['node']} | {row['sample_count']} | {links} |")
        lines += ["", "Main/shadow endpoint deltas use kernel process counters and stable PID/start ticks. "
                  "They exclude short-lived helpers and do not replace the whole-container gate. Node network "
                  "windows are in the standard report; they include all non-loopback namespace traffic.", "",
                  "| Node / role / PID | Window s | CPU % | Kernel read / write B | Read / write syscalls |",
                  "|---|---:|---:|---|---|"]
        number = profiles.reporter.number
        for row in result.get('process_windows', []):
            lines.append(f"| {row['node']} / {row['role']} / {row.get('pid', 'unavailable')} | "
                         f"{number(row['window_seconds'])} | {number(row['cpu_percent'])} | "
                         f"{number(row['read_bytes'])} / {number(row['write_bytes'])} | "
                         f"{number(row['read_syscalls'])} / {number(row['write_syscalls'])} |")
        if result["errors"]:
            lines += ["", "Unavailable evidence:", ""] + ["- " + error for error in result["errors"]]
        (run.artifacts / "REPORT.md").write_text("\n".join(lines) + "\n")
        profiles.save(run.artifacts / "report.json", result)
        for source in [Path(__file__), ROOT / "scripts/debug/capture-performance.py",
                       ROOT / "scripts/debug/report-performance.py"]:
            (run.artifacts / source.name).write_bytes(source.read_bytes())
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
