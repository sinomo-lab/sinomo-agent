#!/usr/bin/env python3
"""Hard resource checks for the six-node disposable Docker verifier.

Node allocations are 6 * 2 GiB, with no swap. The driver's current consumption
is already reflected in MemAvailable; its cgroup ceiling is not an allocation.
Compiler admission/caps are a separate concern. OOM, limit-charge failures and
unexpected container exits fail the run, including exits hidden by auto-restart.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time

NODE_LIMIT = 2 * 1024**3
NODES = {f"node{i}" for i in range(1, 7)}


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True)


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def require_limits(services):
    if set(services) != NODES:
        raise RuntimeError("runtime budget requires exactly node1 through node6")
    for node, config in services.items():
        if config.get("scale", 1) != 1 or config.get("deploy", {}).get("replicas", 1) != 1:
            raise RuntimeError(f"{node}: the runtime allocation permits exactly one container per node")
        # Compose's JSON rendering uses decimal byte strings on some versions.
        if str(config.get("mem_limit")) != str(NODE_LIMIT) or str(config.get("memswap_limit")) != str(NODE_LIMIT):
            raise RuntimeError(f"{node}: require exactly 2 GiB memory and 2 GiB memory+swap")
    return len(services) * NODE_LIMIT


def preflight(config, artifacts):
    required = require_limits(config["services"])
    memory = {line.split(":")[0]: int(line.split()[1]) * 1024
              for line in Path("/proc/meminfo").read_text().splitlines()
              if line.startswith(("MemTotal:", "MemAvailable:", "Shmem:", "SwapTotal:"))}
    paths = {Path("/tmp"), artifacts, Path(docker("info", "--format", "{{.DockerRootDir}}").strip())}
    capacity = {}
    for path in paths:
        stat = os.statvfs(path)
        capacity[str(path)] = {"free_bytes": stat.f_bavail * stat.f_frsize,
                               "free_inodes": stat.f_favail, "reports_inodes": bool(stat.f_files)}
    result = {"node_count": 6, "per_node_bytes": NODE_LIMIT, "node_allocation_bytes": required,
              "memory": memory, "filesystems_used": capacity,
              "host_accounting": "MemAvailable already reflects current host/driver use; no fixed 4 GiB driver allocation is added. Future host/driver growth remains observed overhead, not node capacity.",
              "build_limits": "Separate build guard; no compiler cap or build headroom change."}
    save(artifacts / "resource-preflight.json", result)
    if memory["MemAvailable"] < required:
        raise RuntimeError(f"insufficient available host memory for six 2 GiB nodes: {memory}")
    for path, value in capacity.items():
        if value["free_bytes"] < 5 * 1024**3 or (value["reports_inodes"] and value["free_inodes"] < 65536):
            raise RuntimeError(f"storage reserve unavailable at {path}: {value}")
    return result


def inspect_nodes(project):
    ids = docker("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}").split()
    return json.loads(docker("inspect", *ids)) if ids else []


def check_node(row):
    node = row["Config"]["Labels"]["com.docker.compose.service"]
    if node not in NODES:
        raise RuntimeError(f"unexpected service in the six-node project: {node}")
    host, state = row["HostConfig"], row["State"]
    if host["Memory"] != NODE_LIMIT or host["MemorySwap"] != NODE_LIMIT:
        raise RuntimeError(f"{node}: effective Docker limits differ from 2 GiB/no swap")
    if state["OOMKilled"] or row["RestartCount"]:
        raise RuntimeError(f"{node}: OOM or automatic restart: {state}, restarts={row['RestartCount']}")
    if not state["Running"]:
        return {"node": node, "running": False, "state": state}
    pid = state["Pid"]
    try:
        cgroup = next(line.split("::", 1)[1] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines() if line.startswith("0::"))
    except FileNotFoundError:
        # Explicit restarts may retire this PID between inspect and observation.
        # Docker's event stream still classifies its exit; this is missing data,
        # never a zero counter or an effective-limit PASS.
        return {"node": node, "running": False, "retired_pid": pid}
    if row["Id"] not in cgroup:
        raise RuntimeError(f"{node}: PID cgroup does not match the inspected container")
    root = Path("/sys/fs/cgroup") / cgroup.lstrip("/")
    try:
        values = {name: (root / name).read_text().strip() for name in
                  ["memory.max", "memory.swap.max", "memory.current", "memory.peak", "memory.events"]}
    except FileNotFoundError:
        if not Path(f"/proc/{pid}").exists():
            return {"node": node, "running": False, "retired_pid": pid}
        raise
    events = dict((key, int(value)) for key, value in (line.split() for line in values["memory.events"].splitlines()))
    if int(values["memory.max"]) != NODE_LIMIT or int(values["memory.swap.max"]) != 0:
        raise RuntimeError(f"{node}: effective cgroup limits incorrect: {values}")
    if any(events.get(key, 0) for key in ["max", "oom", "oom_kill", "oom_group_kill"]) or int(values["memory.peak"]) > NODE_LIMIT:
        raise RuntimeError(f"{node}: memory.max boundary or OOM condition requires debugging: {values}")
    return {"node": node, "container_id": row["Id"], "pid": pid, "cgroup": cgroup,
            "running": True, "limits_and_counters": values}


def expect_exit(nodes, reason):
    filename = os.environ.get("SINOMO_TEST_RESOURCE_INTENTS")
    if not filename:
        return
    if not set(nodes) <= NODES or not nodes:
        raise RuntimeError("planned exits require explicit node names")
    with open(filename, "a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps({"time_ns": time.time_ns(), "nodes": nodes, "reason": reason}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def expected_die(event, intents, consumed):
    node = event.get("Actor", {}).get("Attributes", {}).get("com.docker.compose.service")
    for index, intent in enumerate(intents):
        key = (index, node)
        if node in intent["nodes"] and intent["time_ns"] <= event["timeNano"] and key not in consumed:
            consumed.add(key)
            return True
    return False


def watch(project, artifacts, parent_pid):
    artifacts.mkdir(parents=True, exist_ok=True)
    parent = os.pidfd_open(parent_pid)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    command = ["docker", "events", "--since", str(int(time.time())), "--format", "{{json .}}",
               "--filter", f"label=com.docker.compose.project={project}",
               "--filter", "event=oom", "--filter", "event=die", "--filter", "event=start"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=(artifacts / "events.stderr").open("w"), text=True)
    messages = queue.Queue()
    def read_events():
        try:
            for line in process.stdout:
                messages.put(json.loads(line))
        except Exception as error:
            messages.put({"reader_error": str(error)})
    reader = threading.Thread(target=read_events, daemon=True)
    reader.start()
    consumed = set()
    last_sample = 0
    seen = set()
    code = 0
    try:
        with (artifacts / "events.jsonl").open("w") as events, (artifacts / "samples.jsonl").open("w") as samples:
            save(artifacts / "watch-ready.json", {"pid": os.getpid(), "parent_pid": parent_pid, "command": command})
            while True:
                if stopped.is_set() and process.poll() is None:
                    process.terminate()
                    process.wait(timeout=10)
                    reader.join(timeout=10)
                if stopped.is_set() and messages.empty():
                    break
                try:
                    event = messages.get(timeout=0.2)
                except queue.Empty:
                    event = None
                if event:
                    if "reader_error" in event:
                        raise RuntimeError(f"resource event decoding failed: {event}")
                    events.write(json.dumps(event) + "\n")
                    events.flush()
                    intents_path = os.environ.get("SINOMO_TEST_RESOURCE_INTENTS")
                    if intents_path and Path(intents_path).exists():
                        with open(intents_path) as stream:
                            fcntl.flock(stream, fcntl.LOCK_SH)
                            intents = [json.loads(line) for line in stream]
                    else:
                        intents = []
                    if event["Action"] == "oom" or (event["Action"] == "die" and not expected_die(event, intents, consumed)):
                        raise RuntimeError(f"hard container resource/exit failure: {event}")
                if process.poll() is not None and not stopped.is_set():
                    raise RuntimeError("Docker resource event stream ended unexpectedly")
                if time.monotonic() - last_sample >= 1:
                    rows = [check_node(row) for row in inspect_nodes(project)]
                    if len(rows) > 6 or len({row["node"] for row in rows}) != len(rows):
                        raise RuntimeError("more than one container per node exceeds the six-node allocation")
                    seen.update(row["node"] for row in rows)
                    samples.write(json.dumps({"time_ns": time.time_ns(), "nodes": rows}) + "\n")
                    samples.flush()
                    last_sample = time.monotonic()
            save(artifacts / "watch-complete.json", {"seen_nodes": sorted(seen), "planned_exits_consumed": sorted(consumed)})
    except Exception as error:
        code = 1
        save(artifacts / "HARD-FAILURE.json", {"time_ns": time.time_ns(), "error": str(error)})
        signal.pidfd_send_signal(parent, signal.SIGUSR1)
    finally:
        process.terminate()
        process.wait(timeout=10)
        reader.join(timeout=10)
        os.close(parent)
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preflight", "verify", "watch", "expect-exit"])
    parser.add_argument("--project")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--parent-pid", type=int)
    parser.add_argument("--nodes", nargs="+")
    parser.add_argument("--reason", default="explicit verifier restart")
    args = parser.parse_args()
    if args.action == "preflight":
        preflight(json.loads(args.config.read_text()), args.artifacts)
    elif args.action == "expect-exit":
        expect_exit(args.nodes, args.reason)
    elif args.action == "watch":
        raise SystemExit(watch(args.project, args.artifacts, args.parent_pid))
    else:
        rows = [check_node(row) for row in inspect_nodes(args.project)]
        save(args.artifacts / "effective-limits.json", rows)
        if len(rows) != 6 or {row["node"] for row in rows} != NODES or not all(row["running"] for row in rows):
            raise SystemExit("six live limited nodes required")
