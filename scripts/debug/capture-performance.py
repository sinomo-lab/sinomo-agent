#!/usr/bin/env python3
"""Capture generic Observe stages on an explicit set of nodes around a workload.

Example: --client-command '["sinomo"]' --nodes ID1 ID2 --output target/capture
         -- COMMAND ARGS...
Uses normal authenticated Observe routes. Does not build/start/stop clusters.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_capture(args):
    if getattr(args, "buffered", False):
        return run_buffered(args)
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    manifest = {"schema_version": 1, "capture_id": "perf-" + uuid.uuid4().hex,
                "nodes": args.nodes, "client_command": args.client_command,
                "workload": args.workload, "classification": "diagnostic",
                "source_revision": args.source_revision, "binary_sha256": args.binary_sha256,
                "profile": args.profile, "errors": [], "workload_exit_code": None,
                "started_unix_ns": time.time_ns(), "poll_seconds": args.poll_seconds}
    write_json(root / "manifest.json", manifest)
    previous, attempted = {}, []
    cursors, processes = {}, {}
    clients = getattr(args, "clients", None) or {}
    manifest["clients"] = clients
    workload = None
    sample = 0
    def rpc(node, command):
        result = subprocess.run(clients.get(node, args.client_command) + ["observe"] + command + ["--target", node, "--json"],
                                capture_output=True, text=True, timeout=args.rpc_timeout)
        if result.returncode:
            raise RuntimeError(f"{node}: {command[0]} failed ({result.returncode}): {result.stderr[-2000:]}")
        return json.loads(result.stdout)

    def collect(node, index):
        after = cursors.get(node)
        command = ["traces", "--capture", manifest["capture_id"], "--limit", "0"]
        if after is not None:
            command += ["--after-sequence", str(after)]
        response = rpc(node, command)
        response["collection_after_sequence"] = after
        write_json(root / node / f"traces-{index:06}.json", response)
        if not response.get("capture_active"):
            raise RuntimeError(f"{node}: capture expired or ownership changed")
        process = response.get("process") or {}
        if process.get("node_id") != node:
            raise RuntimeError(f"{node}: unexpected trace process identity {process.get('node_id')}")
        identity = (process.get("process_id"), process.get("started_at"))
        if node in processes and processes[node] != identity:
            raise RuntimeError(f"{node}: daemon process changed during capture")
        processes[node] = identity
        if "last_sequence" in response:
            cursors[node] = response["last_sequence"]

    try:
        for node in args.nodes:
            (root / node).mkdir()
            control = rpc(node, ["control"])
            write_json(root / node / "control-before.json", control)
            if control.get("active_capture_id"):
                raise RuntimeError(f"{node}: capture already owned by {control['active_capture_id']}")
            previous[node] = control
        for node in args.nodes:
            # Save attempted nodes BEFORE a mutating RPC: timeout can mean applied.
            attempted.append(node)
            control = rpc(node, ["control", "--capture", manifest["capture_id"], "--clear-traces",
                                 "--filter", previous[node]["base_filter"] + ",sinomo.perf=trace"])
            write_json(root / node / "control-start.json", control)
            if control.get("active_capture_id") != manifest["capture_id"]:
                raise RuntimeError(f"{node}: capture identity mismatch")
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(args.nodes))) as pool:
            def snapshot():
                nonlocal sample
                results = [pool.submit(collect, node, sample) for node in args.nodes]
                sample += 1
                failures = []
                for result in results:
                    try:
                        result.result()
                    except Exception as error:
                        failures.append(str(error))
                if failures:
                    raise RuntimeError("; ".join(failures))
            snapshot()
            manifest["workload_started_unix_ns"] = time.time_ns()
            write_json(root / "manifest.json", manifest)
            with (root / "workload.stdout").open("w") as stdout, (root / "workload.stderr").open("w") as stderr:
                workload = subprocess.Popen(args.workload, stdout=stdout, stderr=stderr, start_new_session=True)
                deadline = time.monotonic() + args.duration_seconds
                while workload.poll() is None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("workload exceeded diagnostic time limit; inspect possibly applied mutations")
                    time.sleep(args.poll_seconds)
                    snapshot()
                manifest["workload_exit_code"] = workload.returncode
                manifest["workload_finished_unix_ns"] = time.time_ns()
                snapshot()
    except (Exception, KeyboardInterrupt) as error:
        manifest["errors"].append(str(error) or "capture interrupted")
    finally:
        if workload is not None and workload.poll() is None:
            # Only the launched client process group belongs to this capture.
            # Remote mutations may outlive it; the report retains the failure.
            import signal
            os.killpg(workload.pid, signal.SIGTERM)
            try:
                workload.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(workload.pid, signal.SIGKILL)
                workload.wait()
            manifest["workload_exit_code"] = workload.returncode
        for node in reversed(attempted):
            try:
                command = ["traces", "--capture", manifest["capture_id"], "--limit", "0"]
                after = cursors.get(node)
                if after is not None:
                    command += ["--after-sequence", str(after)]
                response = rpc(node, command)
                response["collection_after_sequence"] = after
                write_json(root / node / f"traces-{sample:06}.json", response)
            except Exception as error:
                manifest["errors"].append(f"final collection: {error}")
            try:
                # Server-side compare-and-clear protects a replacement owner,
                # even when an earlier status/start/collection RPC timed out.
                restored = rpc(node, ["control", "--clear-capture", "--expected-capture", manifest["capture_id"]])
                write_json(root / node / "control-restored.json", restored)
                if restored.get("active_capture_id") or restored["active_filter"] != previous[node]["active_filter"]:
                    raise RuntimeError(f"{node}: ownership/filter changed; cleanup did not overwrite it")
            except Exception as error:
                manifest["errors"].append(f"cleanup: {error}")
        manifest["finished_unix_ns"] = time.time_ns()
        write_json(root / "manifest.json", manifest)
    return 0 if not manifest["errors"] and manifest["workload_exit_code"] == 0 else 2


def run_buffered(args):
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    capture_id = "perf-" + uuid.uuid4().hex
    clients = getattr(args, "clients", None) or {}
    manifest = {"schema_version": 1, "capture_id": capture_id, "nodes": args.nodes,
                "client_command": args.client_command, "clients": clients, "workload": args.workload,
                "source_revision": args.source_revision, "binary_sha256": args.binary_sha256,
                "profile": args.profile, "classification": "diagnostic", "mode": "buffered",
                "errors": [], "workload_exit_code": None, "started_unix_ns": time.time_ns()}
    write_json(root / "manifest.json", manifest)
    previous, cursors, processes = {}, {}, {}
    attempted = []
    cpu_attempted = []
    workload = None

    def rpc(node, command):
        result = subprocess.run(clients.get(node, args.client_command) + ["observe"] + command + ["--target", node, "--json"],
                                capture_output=True, text=True, timeout=args.rpc_timeout)
        if result.returncode:
            raise RuntimeError(f"{node}: {command[0]} exit {result.returncode}: {result.stderr[-2000:]}")
        return json.loads(result.stdout)

    def parallel(operation, nodes):
        failures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
            futures = [pool.submit(operation, node) for node in nodes]
            for future in futures:
                try:
                    future.result()
                except Exception as error:
                    failures.append(str(error))
        return failures

    def before(node):
        (root / node).mkdir()
        control = rpc(node, ["control"])
        write_json(root / node / "control-before.json", control)
        if control.get("active_capture_id"):
            raise RuntimeError(f"{node}: another capture is active")
        previous[node] = control
        if getattr(args, "cpu_profile", False):
            profile = rpc(node, ["profile", "cpu", "status"])
            write_json(root / node / "cpu-before.json", profile)
            if not profile.get("supported") or profile.get("state") in ("starting", "running", "finalizing"):
                raise RuntimeError(f"{node}: CPU profiler unavailable or already owned")

    def start(node):
        control = rpc(node, ["control", "--capture", capture_id, "--clear-traces", "--filter", previous[node]["base_filter"] + ",sinomo.perf=trace"])
        write_json(root / node / "control-start.json", control)
        if control.get("active_capture_id") != capture_id:
            raise RuntimeError(f"{node}: capture identity mismatch")

    def cpu_start(node):
        profile = rpc(node, ["profile", "cpu", "start", "--capture", capture_id,
                            "--duration-seconds", "30", "--frequency-hz", "49"])
        deadline = time.monotonic() + 10
        while profile.get("state") == "starting" and time.monotonic() < deadline:
            time.sleep(.1)
            profile = rpc(node, ["profile", "cpu", "status"])
        write_json(root / node / "cpu-start.json", profile)
        if profile.get("state") != "running" or profile.get("capture_id") != capture_id:
            raise RuntimeError(f"{node}: CPU profile did not start: {profile}")

    def cpu_stop(node):
        profile = rpc(node, ["profile", "cpu", "stop", "--capture", capture_id])
        deadline = time.monotonic() + 15
        while profile.get("state") in ("starting", "running", "finalizing") and time.monotonic() < deadline:
            time.sleep(.1)
            profile = rpc(node, ["profile", "cpu", "status"])
        write_json(root / node / "cpu-final.json", profile)
        if profile.get("state") != "complete" or profile.get("capture_id") != capture_id:
            raise RuntimeError(f"{node}: CPU profile incomplete: {profile}")

    def counter_snapshot(node, index):
        response = rpc(node, ["traces", "--capture", capture_id, "--target-prefix", "__observe_counter_snapshot_only__", "--limit", "0"])
        response["process_window_sample"] = True
        response["collection_snapshot_only"] = True
        write_json(root / node / f"traces-{index:06}.json", response)
        process = response.get("process") or {}
        if not response.get("capture_active") or process.get("node_id") != node:
            raise RuntimeError(f"{node}: capture/process identity changed")
        if (response.get("capacity") or 0) < args.minimum_capacity:
            raise RuntimeError(f"{node}: trace capacity {response.get('capacity')} is below {args.minimum_capacity}")
        identity = (process.get("process_id"), process.get("started_at"))
        if index == 0:
            cursors[node] = response["last_sequence"]
            processes[node] = identity
        elif processes.get(node) != identity:
            raise RuntimeError(f"{node}: daemon restarted during workload")

    def stop(node):
        control = rpc(node, ["control", "--clear-capture", "--expected-capture", capture_id])
        write_json(root / node / "control-restored.json", control)
        if control.get("active_capture_id") or control["active_filter"] != previous[node]["active_filter"]:
            raise RuntimeError(f"{node}: prior filter not restored")

    def export(node):
        command = ["traces", "--capture", capture_id, "--limit", "0"]
        if node in cursors:
            command += ["--after-sequence", str(cursors[node])]
        response = rpc(node, command)
        response["process_window_sample"] = False
        response["collection_after_sequence"] = cursors.get(node)
        write_json(root / node / "traces-000002.json", response)
        if response.get("capture_active"):
            raise RuntimeError(f"{node}: capture still active during frozen export")
        process = response.get("process") or {}
        if node in processes and (process.get("process_id"), process.get("started_at")) != processes[node]:
            raise RuntimeError(f"{node}: process changed before export")

    try:
        errors = parallel(before, args.nodes)
        if errors:
            raise RuntimeError("; ".join(errors))
        attempted.extend(args.nodes)  # A timed-out start may already be applied.
        errors = parallel(start, args.nodes)
        if errors:
            raise RuntimeError("; ".join(errors))
        if getattr(args, "cpu_profile", False):
            cpu_attempted.extend(args.nodes)
            errors = parallel(cpu_start, args.nodes)
            if errors:
                raise RuntimeError("; ".join(errors))
            manifest["cpu_profile"] = {"frequency_hz": 49, "maximum_duration_seconds": 30}
        errors = parallel(lambda node: counter_snapshot(node, 0), args.nodes)
        if errors:
            raise RuntimeError("; ".join(errors))
        manifest["workload_started_unix_ns"] = time.time_ns()
        write_json(root / "manifest.json", manifest)
        with (root / "workload.stdout").open("w") as stdout, (root / "workload.stderr").open("w") as stderr:
            workload = subprocess.Popen(args.workload, stdout=stdout, stderr=stderr, start_new_session=True)
            manifest["workload_exit_code"] = workload.wait(timeout=args.duration_seconds)
        manifest["workload_finished_unix_ns"] = time.time_ns()
        manifest["errors"].extend(parallel(lambda node: counter_snapshot(node, 1), args.nodes))
    except (Exception, KeyboardInterrupt) as error:
        manifest["errors"].append(str(error) or "capture interrupted")
    finally:
        if workload is not None and workload.poll() is None:
            import signal
            os.killpg(workload.pid, signal.SIGTERM)
            try:
                workload.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(workload.pid, signal.SIGKILL)
                workload.wait()
            manifest["workload_exit_code"] = workload.returncode
        if attempted:
            manifest["errors"].extend("cleanup: " + error for error in parallel(stop, attempted))
            manifest["tracing_stopped_unix_ns"] = time.time_ns()
            if cpu_attempted:
                manifest["errors"].extend("CPU cleanup: " + error for error in parallel(cpu_stop, cpu_attempted))
            # Expensive serialization/transport occurs after filter restoration.
            manifest["errors"].extend("export: " + error for error in parallel(export, attempted))
        manifest["finished_unix_ns"] = time.time_ns()
        write_json(root / "manifest.json", manifest)
    return 0 if not manifest["errors"] and manifest["workload_exit_code"] == 0 else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-command", type=json.loads, default=["sinomo"], help="JSON argument array, optionally docker exec plus absolute sinomo path")
    parser.add_argument("--nodes", nargs="+", required=True)
    parser.add_argument("--buffered", action="store_true", help="stop tracing before exporting a short workload; process I/O window excludes export")
    parser.add_argument("--cpu-profile", action="store_true", help="also collect bounded 49 Hz CPU profiles on all nodes (buffered mode)")
    parser.add_argument("--minimum-capacity", type=int, default=0, help="require this many server-side trace slots before a buffered workload")
    parser.add_argument("--clients-file", type=Path, help="JSON map of node ID to client argument array; avoids routing all collection through one node")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--poll-seconds", type=float, default=2)
    parser.add_argument("--rpc-timeout", type=float, default=20)
    parser.add_argument("--duration-seconds", type=float, default=60)
    parser.add_argument("workload", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.cpu_profile and not args.buffered:
        parser.error("--cpu-profile requires --buffered")
    args.clients = json.loads(args.clients_file.read_text()) if args.clients_file else {}
    if args.clients and (not isinstance(args.clients, dict) or set(args.clients) != set(args.nodes)
                        or any(not isinstance(cmd, list) or not cmd or not all(isinstance(v, str) and v for v in cmd) for cmd in args.clients.values())):
        parser.error("clients-file must map every requested node to a nonempty argument array")
    if args.workload[:1] == ["--"]:
        args.workload.pop(0)
    if not args.workload or not isinstance(args.client_command, list) or not args.client_command or not all(isinstance(v, str) and v for v in args.client_command):
        parser.error("client-command must be a nonempty string array and workload is required")
    if len(set(args.nodes)) != len(args.nodes) or any(n in (".", "..") or "/" in n or "\\" in n for n in args.nodes):
        parser.error("node IDs must be unique safe directory names")
    if not 0 <= args.minimum_capacity <= 32768:
        parser.error("minimum-capacity must be 0..32768")
    if not (0 < args.duration_seconds <= 120 and .1 <= args.poll_seconds <= 10 and 0 < args.rpc_timeout <= 60):
        parser.error("duration 0..120s, poll .1..10s, RPC timeout 0..60s required")
    return run_capture(args)


if __name__ == "__main__":
    raise SystemExit(main())
