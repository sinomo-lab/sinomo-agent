#!/usr/bin/env python3
"""Hard resource checks for the six-node disposable Docker verifier.

Node allocations are 6 * 2 GiB, with no swap. The driver's current consumption
is already reflected in MemAvailable; its cgroup ceiling is not an allocation.
Compiler admission/caps are a separate concern. OOM, limit-charge failures and
unexpected container exits fail the run, including exits hidden by auto-restart.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import threading
import time

NODE_LIMIT = 2 * 1024**3
NODES = {f"node{i}" for i in range(1, 7)}
LIFECYCLE_JOIN_SECONDS = 2  # Two sample periods; never extends a scenario deadline.


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


def process_start(pid):
    raw = Path(f"/proc/{pid}/stat").read_text()
    return int(raw[raw.rindex(")") + 2:].split()[19])


def sample_reused_nodes(project):
    rows = inspect_nodes(project)
    if len(rows) != 6 or len({row["Id"] for row in rows}) != 6 or {row["Config"]["Labels"]["com.docker.compose.service"] for row in rows} != NODES:
        raise RuntimeError("reuse admission requires the exact six existing owned nodes")
    result = {}
    for row in rows:
        if row["Config"]["Labels"].get("com.docker.compose.project") != project:
            raise RuntimeError("reuse admission cannot borrow another project's memory")
        checked = check_node(row)
        if not checked["running"]:
            raise RuntimeError("reuse admission requires live container ownership")
        pid = checked["pid"]
        start = process_start(pid)
        root = Path("/sys/fs/cgroup") / checked["cgroup"].lstrip("/")
        identity = root.stat()
        raw = (root / "memory.stat").read_text()
        stat = {key: int(value) for key, value in (line.split() for line in raw.splitlines())}
        current = int((root / "memory.current").read_text())
        swap = int((root / "memory.swap.current").read_text())
        if "anon" not in stat or stat["anon"] < 0 or current < 0 or swap:
            raise RuntimeError("reuse anonymous-memory coverage unavailable or swap present")
        after = root.stat()
        group = next(line.split("::", 1)[1] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines() if line.startswith("0::"))
        if (identity.st_dev, identity.st_ino) != (after.st_dev, after.st_ino) or process_start(pid) != start or group != checked["cgroup"]:
            raise RuntimeError("reuse memory owner changed during observation")
        result[checked["node"]] = {
            "container_id": row["Id"], "pid": pid, "start_ticks": start,
            "cgroup": checked["cgroup"], "cgroup_identity": [identity.st_dev, identity.st_ino],
            "anonymous_bytes": min(stat["anon"], current, NODE_LIMIT),
            "memory_current_bytes": current, "memory_stat": raw,
            "effective_limits_and_events": checked["limits_and_counters"],
        }
    return result


def reused_anonymous_credit(before, after):
    if set(before) != NODES or set(after) != NODES:
        raise RuntimeError("reuse samples must cover all six owned nodes")
    identity_fields = ("container_id", "pid", "start_ticks", "cgroup", "cgroup_identity")
    credit = 0
    for node in sorted(NODES):
        if any(before[node][key] != after[node][key] for key in identity_fields):
            raise RuntimeError(f"{node}: reuse ownership changed across host-memory sample")
        values = [before[node]["anonymous_bytes"], after[node]["anonymous_bytes"]]
        if any(not isinstance(value, int) or value < 0 for value in values):
            raise RuntimeError(f"{node}: anonymous-memory observation unavailable")
        credit += min(*values, NODE_LIMIT)
    return credit


def preflight(config, artifacts, reuse_project=None):
    required = require_limits(config["services"])
    reuse = None
    if reuse_project is not None and (not reuse_project or config.get("name") != reuse_project):
        raise RuntimeError("reuse project must match the resolved Compose configuration")
    before = sample_reused_nodes(reuse_project) if reuse_project else None
    memory = {line.split(":")[0]: int(line.split()[1]) * 1024
              for line in Path("/proc/meminfo").read_text().splitlines()
              if line.startswith(("MemTotal:", "MemAvailable:", "Shmem:", "SwapTotal:"))}
    credit = 0
    if reuse_project:
        after = sample_reused_nodes(reuse_project)
        credit = reused_anonymous_credit(before, after)
        reuse = {"project": reuse_project, "before": before, "after": after,
                 "already_resident_anonymous_bytes": credit,
                 "qualification": "Lower bracketing anonymous charge of the same live owned containers only. File cache, shared ancestors and unrelated workloads receive no credit. This is sampled admission, not an atomic host reservation or a resource-gate waiver."}
    additional = required - credit
    paths = {Path("/tmp"), artifacts, Path(docker("info", "--format", "{{.DockerRootDir}}").strip())}
    capacity = {}
    for path in paths:
        stat = os.statvfs(path)
        capacity[str(path)] = {"free_bytes": stat.f_bavail * stat.f_frsize,
                               "free_inodes": stat.f_favail, "reports_inodes": bool(stat.f_files)}
    result = {"node_count": 6, "per_node_bytes": NODE_LIMIT, "node_allocation_bytes": required,
              "memory": memory, "filesystems_used": capacity, "reuse": reuse,
              "additional_available_bytes_required": additional,
              "host_accounting": "Fresh launch requires the full12GiB. Explicit reuse credits only already resident owned anonymous memory, which MemAvailable has already excluded; remaining capacity must still be available. No file-cache/observer subtraction, fixed driver allocation, or whole-host acceptance limit.",
              "build_limits": "Separate build guard; no compiler cap or build headroom change."}
    save(artifacts / "resource-preflight.json", result)
    if memory["MemAvailable"] < additional:
        raise RuntimeError(f"insufficient available host memory for six 2 GiB nodes: additional_required={additional}, already_resident_anon={credit}, memory={memory}")
    for path, value in capacity.items():
        if value["free_bytes"] < 5 * 1024**3 or (value["reports_inodes"] and value["free_inodes"] < 65536):
            raise RuntimeError(f"storage reserve unavailable at {path}: {value}")
    return result


def inspect_nodes(project):
    ids = docker("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}").split()
    return json.loads(docker("inspect", *ids)) if ids else []


def inspection_evidence(row):
    """Retain classification inputs unchanged, without unrelated Docker config."""
    evidence = {key: row[key] for key in ("Id", "State", "RestartCount") if key in row}
    labels = row.get("Config", {}).get("Labels", {})
    evidence["Config"] = {"Labels": {key: labels[key] for key in
        ("com.docker.compose.project", "com.docker.compose.service") if key in labels}}
    host = row.get("HostConfig", {})
    evidence["HostConfig"] = {key: host[key] for key in ("Memory", "MemorySwap") if key in host}
    return evidence


def check_node(row, activation=None, now=None):
    node = row["Config"]["Labels"]["com.docker.compose.service"]
    if node not in NODES:
        raise RuntimeError(f"unexpected service in the six-node project: {node}")
    host, state = row["HostConfig"], row["State"]
    if host["Memory"] != NODE_LIMIT or host["MemorySwap"] != NODE_LIMIT:
        raise RuntimeError(f"{node}: effective Docker limits differ from 2 GiB/no swap")
    if state["OOMKilled"] or (activation is None and row["RestartCount"]):
        raise RuntimeError(f"{node}: OOM or automatic restart: {state}, restarts={row['RestartCount']}")
    if activation is not None:
        activation.observe(row, now)
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


def read_intents():
    filename = os.environ.get("SINOMO_TEST_RESOURCE_INTENTS")
    if not filename or not Path(filename).exists():
        return []
    with open(filename) as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        return [json.loads(line) for line in stream]


def expect_rollout_exit(project, node, candidate, plan_log, artifacts):
    """Record an actual fresh fixture claim before its one PID1 replacement."""
    filename = os.environ["SINOMO_TEST_RESOURCE_INTENTS"]
    ids = docker("ps", "-q", "--filter", f"label=com.docker.compose.project={project}",
                 "--filter", f"label=com.docker.compose.service={node}").split()
    if node not in NODES or len(ids) != 1:
        raise RuntimeError("activation intent requires one owned target")
    row = json.loads(docker("inspect", ids[0]))[0]
    plans = set(re.findall(r"rollout ([0-9a-z]+) saved;", plan_log.read_text()))
    if len(plans) != 1:
        raise RuntimeError("activation intent requires the original coordinator plan")
    plan = plans.pop()
    claims = docker("exec", row["Id"], "sh", "-c",
                    "for f in /data/rollout/staged/uploads/*/activation-started.sobj; "
                    "do test -f \"$f\" && sha256sum \"$f\"; done").splitlines()
    if len(claims) != 1:
        raise RuntimeError("activation intent requires one actual target claim")
    claim_hash, claim_path = claims[0].split(None, 1)
    if not re.fullmatch(r"[0-9a-f]{64}", claim_hash) or not re.fullmatch(
            re.escape(f"/data/rollout/staged/uploads/{plan}-") +
            r"[0-9a-z]+/activation-started\.sobj", claim_path):
        raise RuntimeError("activation claim does not match the original plan")
    with candidate.open("rb") as stream:
        candidate_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    after = json.loads(docker("inspect", row["Id"]))[0]
    for observed in (row, after):
        labels, state = observed["Config"]["Labels"], observed["State"]
        if (not re.fullmatch(r"[0-9a-f]{64}", observed["Id"]) or
                labels.get("com.docker.compose.project") != project or
                labels.get("com.docker.compose.service") != node or
                not state["Running"] or state["OOMKilled"] or state["Pid"] <= 0 or
                not state["StartedAt"] or observed["RestartCount"] != 0):
            raise RuntimeError("activation intent requires the original live container")
    if any(row[key] != after[key] for key in ("Id", "RestartCount")) or any(
            row["State"][key] != after["State"][key] for key in ("Pid", "StartedAt")):
        raise RuntimeError("activation container changed while recording its claim")
    record = {"kind": "rollout-shadow-replacement", "time_ns": time.time_ns(),
              "nodes": [node], "project": project, "container_id": row["Id"],
              "old_pid": row["State"]["Pid"], "old_started_at": row["State"]["StartedAt"],
              "old_restart_count": 0, "candidate_sha256": candidate_hash,
              "rollout_id": plan, "claim_path": claim_path, "claim_sha256": claim_hash}
    with open(filename, "a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(record) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    save(artifacts / f"{node}-planned-lifecycle.json", record)


class ActivationLifecycle:
    """Join this fixture's one claimed replacement with raw Docker evidence."""
    def __init__(self, project, candidate_hash, rows, started_ns):
        self.project, self.candidate_hash, self.started_ns = project, candidate_hash, started_ns
        self.initial = {row["Id"]: row for row in rows}
        self.latest = dict(self.initial)
        self.intents, self.claims, self.dies, self.starts, self.pending = [], {}, {}, {}, {}
        self.replacements = {}
        self.consumed = set()
        if len(rows) != 6 or len(self.initial) != 6 or {
                row["Config"]["Labels"]["com.docker.compose.service"] for row in rows} != NODES:
            raise RuntimeError("activation observation requires the exact six existing nodes")
        for row in rows:
            if (not re.fullmatch(r"[0-9a-f]{64}", row["Id"]) or
                    row["Config"]["Labels"].get("com.docker.compose.project") != project or
                    not row["State"]["Running"] or not row["State"]["StartedAt"] or
                    row["State"]["Pid"] <= 0 or row["RestartCount"] != 0):
                raise RuntimeError("activation baseline is not a fresh live owned container")

    def refresh(self, intents):
        if intents[:len(self.intents)] != self.intents:
            raise RuntimeError("activation intent history changed")
        for index in range(len(self.intents), len(intents)):
            intent = intents[index]
            cid = intent.get("container_id")
            old = self.initial.get(cid)
            if old is None:
                raise RuntimeError("activation intent has an unknown canonical container")
            node = old["Config"]["Labels"]["com.docker.compose.service"]
            plan = intent.get("rollout_id", "")
            if (cid in self.claims or intent.get("kind") != "rollout-shadow-replacement" or
                    intent.get("project") != self.project or intent.get("nodes") != [node] or
                    intent.get("candidate_sha256") != self.candidate_hash or
                    intent.get("old_restart_count") != 0 or
                    intent.get("old_pid") != old["State"]["Pid"] or
                    intent.get("old_started_at") != old["State"]["StartedAt"] or
                    not re.fullmatch(r"[0-9a-z]+", plan) or
                    not re.fullmatch(r"[0-9a-f]{64}", intent.get("claim_sha256", "")) or
                    not re.fullmatch(re.escape(f"/data/rollout/staged/uploads/{plan}-") +
                                     r"[0-9a-z]+/activation-started\.sobj", intent.get("claim_path", "")) or
                    not isinstance(intent.get("time_ns"), int) or intent["time_ns"] < self.started_ns):
                raise RuntimeError("invalid or repeated activation claim provenance")
            self.claims[cid] = (index, intent)
        self.intents = list(intents)

    def event(self, event, now):
        self.expire(now)
        actor, action = event["Actor"], event["Action"]
        cid, labels = actor["ID"], actor["Attributes"]
        old = self.initial.get(cid)
        if (old is None or labels.get("com.docker.compose.project") != self.project or
                labels.get("com.docker.compose.service") != old["Config"]["Labels"]["com.docker.compose.service"]):
            raise RuntimeError("foreign activation lifecycle event")
        if action == "start" and event["timeNano"] < self.started_ns:
            return  # Initial startup replayed by Docker's whole-second --since.
        claim = self.claims.get(cid)
        if claim is None or event["timeNano"] < claim[1]["time_ns"]:
            raise RuntimeError("container lifecycle event without a pre-exit activation claim")
        if action == "die":
            if labels.get("exitCode") != "0" or cid in self.dies:
                raise RuntimeError("nonzero or repeated activation exit")
            self.dies[cid] = event
            self.consumed.add((claim[0], labels["com.docker.compose.service"]))
        elif action == "start":
            if cid in self.starts:
                raise RuntimeError("repeated activation start")
            self.starts[cid] = event
        else:
            raise RuntimeError("unexpected activation lifecycle event")
        self.pending.setdefault(cid, now)
        self.reconcile(cid)

    def observe(self, row, now):
        self.expire(now)
        cid, count, state = row["Id"], row["RestartCount"], row["State"]
        old, last = self.initial.get(cid), self.latest.get(cid)
        if (old is None or row["Config"]["Labels"].get("com.docker.compose.project") != self.project or
                row["Config"]["Labels"].get("com.docker.compose.service") != old["Config"]["Labels"]["com.docker.compose.service"]):
            raise RuntimeError("activation container identity changed")
        if count not in (0, 1) or count < last["RestartCount"]:
            raise RuntimeError("extra or reset activation restart count")
        generation = (state["Pid"], state["StartedAt"])
        if cid in self.replacements and (count != 1 or not state["Running"] or
                                        generation != self.replacements[cid]):
            raise RuntimeError("extra activation process replacement")
        changed = (state["Pid"], state["StartedAt"]) != (old["State"]["Pid"], old["State"]["StartedAt"])
        if count or changed or not state["Running"]:
            if cid not in self.claims or (state["Running"] and not count):
                raise RuntimeError("undeclared container restart")
            if state.get("ExitCode", 0) != 0:
                raise RuntimeError("nonzero activation exit in inspect")
            self.pending.setdefault(cid, now)
        self.latest[cid] = row
        self.reconcile(cid)

    def reconcile(self, cid):
        row, old = self.latest[cid], self.initial[cid]
        if cid in self.dies and cid in self.starts:
            if self.starts[cid]["timeNano"] < self.dies[cid]["timeNano"]:
                raise RuntimeError("activation start precedes its exit")
            if (row["RestartCount"] == 1 and row["State"]["Running"] and
                    (row["State"]["Pid"], row["State"]["StartedAt"]) !=
                    (old["State"]["Pid"], old["State"]["StartedAt"])):
                self.pending.pop(cid, None)
                self.replacements[cid] = (row["State"]["Pid"], row["State"]["StartedAt"])

    def expire(self, now):
        if any(now - began >= LIFECYCLE_JOIN_SECONDS for began in self.pending.values()):
            raise RuntimeError("activation lifecycle evidence missing at fixed join deadline")

    def classification(self, cid):
        return {"observed_restart_count": self.latest[cid]["RestartCount"],
                "classified_rollout_exits": int(cid in self.dies),
                "lifecycle_pending": cid in self.pending,
                "lifecycle_pending_since_monotonic": self.pending.get(cid)}

    def finish(self, now):
        self.expire(now)
        if self.pending or set(self.claims) != set(self.dies) or set(self.dies) != set(self.starts):
            raise RuntimeError("activation observation ended with unresolved lifecycle evidence")
        if any(not row["State"]["Running"] or row["RestartCount"] != int(cid in self.dies)
               for cid, row in self.latest.items()):
            raise RuntimeError("activation observation ended without all live reconciled nodes")


def watch(project, artifacts, parent_pid, activation_candidate=None):
    artifacts.mkdir(parents=True, exist_ok=True)
    parent = os.pidfd_open(parent_pid)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    started_ns = time.time_ns()
    command = ["docker", "events", "--since", str(started_ns // 10**9), "--format", "{{json .}}",
               "--filter", f"label=com.docker.compose.project={project}",
               "--filter", "event=oom", "--filter", "event=die", "--filter", "event=start"]
    events_stderr = (artifacts / "events.stderr").open("w")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=events_stderr, text=True)
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
    activation = None
    stream_stopped = False
    stop_sampled = False
    def stop_stream():
        nonlocal stream_stopped
        process.terminate()
        process.wait(timeout=10)
        reader.join(timeout=10)
        if reader.is_alive():
            raise RuntimeError("Docker resource event reader did not drain")
        stream_stopped = True
    try:
        candidate_hash = None
        if activation_candidate is not None:
            with activation_candidate.open("rb") as candidate:
                candidate_hash = hashlib.file_digest(candidate, "sha256").hexdigest()
        with (artifacts / "events.jsonl").open("w") as events, \
                (artifacts / "samples.jsonl").open("w") as samples, \
                (artifacts / "inspections.jsonl").open("w") as inspections:
            def sample():
                nonlocal activation, last_sample
                raw = inspect_nodes(project)
                observed_ns = time.time_ns()
                # Preserve even failing inputs, without retaining unrelated config.
                inspections.write(json.dumps({"time_ns": observed_ns,
                                              "nodes": [inspection_evidence(row) for row in raw]}) + "\n")
                inspections.flush()
                if candidate_hash is not None:
                    if activation is None:
                        activation = ActivationLifecycle(project, candidate_hash, raw, started_ns)
                    activation.refresh(read_intents())
                    if len(raw) != 6 or {row["Id"] for row in raw} != set(activation.initial):
                        raise RuntimeError("activation lost its exact six-container coverage")
                rows = [check_node(row, activation, time.monotonic()) if activation is not None
                        else check_node(row) for row in raw]
                if len(rows) > 6 or len({row["node"] for row in rows}) != len(rows):
                    raise RuntimeError("more than one container per node exceeds the six-node allocation")
                if activation is not None:
                    for row, original in zip(rows, raw):
                        row.update(activation.classification(original["Id"]))
                seen.update(row["node"] for row in rows)
                samples.write(json.dumps({"time_ns": observed_ns, "nodes": rows}) + "\n")
                samples.flush()
                last_sample = time.monotonic()
                return rows

            checked = sample() if candidate_hash is not None else []
            if activation is not None and not all(row["running"] for row in checked):
                raise RuntimeError("activation baseline resource observation is unavailable")
            save(artifacts / "watch-ready.json", {"pid": os.getpid(), "parent_pid": parent_pid,
                                                 "command": command, "activation_candidate_sha256": candidate_hash})
            while True:
                if stopped.is_set() and activation is None and not stream_stopped:
                    stop_stream()
                if stopped.is_set() and activation is None and messages.empty():
                    break
                try:
                    event = messages.get(timeout=0.2)
                except queue.Empty:
                    event = None
                while event is not None:
                    if "reader_error" in event:
                        raise RuntimeError(f"resource event decoding failed: {event}")
                    events.write(json.dumps(event) + "\n")
                    events.flush()
                    intents = read_intents()
                    if event["Action"] == "oom":
                        raise RuntimeError(f"hard container resource/exit failure: {event}")
                    if activation is not None:
                        activation.refresh(intents)
                        activation.event(event, time.monotonic())
                    elif event["Action"] == "die" and not expected_die(event, intents, consumed):
                        raise RuntimeError(f"hard container resource/exit failure: {event}")
                    try:
                        event = messages.get_nowait()
                    except queue.Empty:
                        event = None
                if process.poll() is not None and not stream_stopped:
                    raise RuntimeError("Docker resource event stream ended unexpectedly")
                if activation is not None:
                    activation.expire(time.monotonic())
                if (time.monotonic() - last_sample >= 1 or
                        (activation is not None and stopped.is_set() and (not stop_sampled or stream_stopped))):
                    checked = sample()
                    stop_sampled = stopped.is_set()
                if activation is not None and stopped.is_set():
                    if activation.pending and not stream_stopped:
                        continue  # Same fixed join deadline; the event stream remains live.
                    activation.finish(time.monotonic())
                    if not all(row["running"] for row in checked):
                        raise RuntimeError("activation final resource observation is unavailable")
                    if not stream_stopped:
                        stop_stream()
                        continue  # Drain reader output and take one final inspect before PASS.
                    consumed = activation.consumed
                    break
            save(artifacts / "watch-complete.json", {"seen_nodes": sorted(seen), "planned_exits_consumed": sorted(consumed)})
    except Exception as error:
        code = 1
        save(artifacts / "HARD-FAILURE.json", {"time_ns": time.time_ns(), "error": str(error),
                                             "pending_lifecycle": activation.pending if activation else {}})
        signal.pidfd_send_signal(parent, signal.SIGUSR1)
    finally:
        process.terminate()
        process.wait(timeout=10)
        reader.join(timeout=10)
        events_stderr.close()
        os.close(parent)
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preflight", "verify", "watch", "expect-exit", "expect-rollout-exit"])
    parser.add_argument("--project")
    parser.add_argument("--reuse-project", help="preflight only: exact existing project; no new/recreated nodes")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--parent-pid", type=int)
    parser.add_argument("--nodes", nargs="+")
    parser.add_argument("--reason", default="explicit verifier restart")
    parser.add_argument("--activation-candidate", type=Path, help="opt in to exact claim-bound rollout observation")
    parser.add_argument("--plan-log", type=Path)
    args = parser.parse_args()
    if args.action == "preflight":
        preflight(json.loads(args.config.read_text()), args.artifacts, args.reuse_project)
    elif args.action == "expect-exit":
        expect_exit(args.nodes, args.reason)
    elif args.action == "expect-rollout-exit":
        if not args.nodes or len(args.nodes) != 1 or not args.activation_candidate or not args.plan_log:
            parser.error("expect-rollout-exit requires one node, activation candidate and original plan log")
        expect_rollout_exit(args.project, args.nodes[0], args.activation_candidate, args.plan_log, args.artifacts)
    elif args.action == "watch":
        raise SystemExit(watch(args.project, args.artifacts, args.parent_pid, args.activation_candidate))
    else:
        rows = [check_node(row) for row in inspect_nodes(args.project)]
        save(args.artifacts / "effective-limits.json", rows)
        if len(rows) != 6 or {row["node"] for row in rows} != NODES or not all(row["running"] for row in rows):
            raise SystemExit("six live limited nodes required")
