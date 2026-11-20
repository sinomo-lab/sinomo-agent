#!/usr/bin/env python3
"""Verify channels, roaming homes and indexed memory on an existing dev cluster.

The selected six-node project is retained throughout. This driver never resets
or deletes volumes. Mutations run once; failed effects and native receipts stay
in the artifact directory. Performance distributions are outside this lane.
"""

import argparse
import concurrent.futures
import contextlib
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

DOCKER = Path(__file__).resolve().parent
sys.path.insert(0, str(DOCKER))
spec = importlib.util.spec_from_file_location("sifs_consumer_history", DOCKER / "test-file-history.py")
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)
import runtime_resources

NODES = tuple(f"node{i}" for i in range(1, 7))
CONNECTOR = "sinomo.memory.sifs/v1"


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


class Consumers(history.Acceptance):
    def __init__(self, args):
        super().__init__(args)
        self.env["SINOMO_TEST_PROFILE"] = args.profile
        self.identity = history.operation_id()
        self.agent = "cluster-test-sifs-" + self.identity
        self.channel = self.identity
        self.common_token = "sinomoconsumer" + self.identity
        self.old_token = "sinomostale" + self.identity
        self.new_token = "sinomocurrent" + self.identity
        self.primary = "notes/primary.md"
        self.secondary = "nested/secondary.md"
        self.primary_body = f"# Cluster note\n{self.common_token} {self.old_token}\nDurable λ 文件.\n"
        self.secondary_body = f"# Another note\n{self.common_token} secondary{self.identity}\n"
        self.home_payload = f"roaming workspace {self.identity} λ 文件\n"
        self.product_home_payload = "product-home-" + self.identity
        self.product_home_name = "consumer-" + self.identity
        self.receipts = {}
        self.roots = {}
        self.message_receipts = []
        self.phases = []
        self.watch = None
        self.watch_stream = None
        self.topology_unknown = False

    def cli(self, node, label, *args, timeout=120, **kwargs):
        # Killing only the host Docker client can leave a mutation in progress.
        # Bound the actual CLI in its container and retain host cleanup grace.
        return self.run(label, [*self.compose, "exec", "-T", node, "timeout", "--foreground",
                               "-k", "5s", f"{timeout}s", "/usr/local/bin/sinomo", *args],
                        timeout=timeout + 10, **kwargs)

    def native(self, node, case, label, *, timeout=120, **params):
        args = ["test", "run", case, "--require-cluster", "--json"]
        for key, value in params.items():
            args += ["--param", f"{key.replace('_', '-')}={str(value).lower() if isinstance(value, bool) else value}"]
        try:
            raw = self.cli(node, label, *args, timeout=timeout)
        except BaseException:
            # Native failures normally print a complete JSON report before the
            # CLI exits nonzero. Keep it visible to the maintained reporter too.
            output = (self.artifacts / f"{self.index:03d}-{label}").with_suffix(".stdout")
            if output.is_file():
                try:
                    failed = json.loads(output.read_bytes())
                    if isinstance(failed, dict) and "case" in failed:
                        save(self.artifacts / f"probe-{label}.json", failed)
                except (ValueError, UnicodeError):
                    pass  # Original partial bytes and timeout receipt remain intact.
            raise
        report = json.loads(raw)
        save(self.artifacts / f"probe-{label}.json", report)
        require(report.get("status") == "passed", f"{label}: native case did not pass: {report}")
        return report["diagnostics"]

    def phase(self, name, action):
        row = {"name": name, "status": "running"}
        self.phases.append(row)
        save(self.artifacts / "phases.json", self.phases)
        try:
            action()
            row["status"] = "passed"
        except BaseException as error:
            row.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            save(self.artifacts / "phases.json", self.phases)

    def start_watch(self):
        config = self.run("resolved-compose", [*self.compose, "config", "--format", "json"])
        (self.artifacts / "compose-effective.json").write_bytes(config)
        rows = runtime_resources.inspect_nodes(self.args.project)
        bindings = []
        for row in rows:
            mounts = [mount for mount in row.get("Mounts", []) if mount.get("Destination") == "/usr/local/bin/sinomo"]
            require(len(mounts) == 1 and mounts[0].get("Type") == "bind" and mounts[0].get("RW") is False
                    and Path(mounts[0]["Source"]).resolve() == self.binary,
                    "existing container bind source differs from --binary; refusing possible recreation during restart")
            bindings.append({"id": row["Id"], "node": row["Config"]["Labels"].get("com.docker.compose.service"),
                             "binary_mount": mounts[0]})
        save(self.artifacts / "initial-binary-bindings.json", bindings)
        (self.artifacts / "resources").mkdir(parents=True, exist_ok=True)
        self.run("resource-admission", [sys.executable, str(DOCKER / "runtime_resources.py"),
                 "preflight", "--config", str(self.artifacts / "compose-effective.json"),
                 "--artifacts", str(self.artifacts / "resources"), "--reuse-project", self.args.project])
        intents = str(self.artifacts / "runtime-resource-intents.jsonl")
        os.environ["SINOMO_TEST_RESOURCE_INTENTS"] = intents
        self.env["SINOMO_TEST_RESOURCE_INTENTS"] = intents
        self.watch_stream = (self.artifacts / "resource-watch.log").open("wb")
        self.watch = subprocess.Popen([sys.executable, str(DOCKER / "runtime_resources.py"),
            "watch", "--project", self.args.project, "--artifacts", str(self.artifacts / "resources"),
            "--parent-pid", str(os.getpid())], cwd=self.root, env=self.env,
            stdout=self.watch_stream, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while not (self.artifacts / "resources/watch-ready.json").exists():
            require(self.watch.poll() is None and time.monotonic() < deadline,
                    "resource watcher did not establish coverage")
            time.sleep(.1)

    def stop_watch(self):
        if self.watch is None:
            return
        self.watch.terminate()
        try:
            code = self.watch.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.watch.kill()
            self.watch.wait(timeout=5)
            raise RuntimeError("resource watcher did not drain")
        finally:
            self.watch_stream.close()
        require(code == 0 and (self.artifacts / "resources/watch-complete.json").is_file()
                and not (self.artifacts / "resources/HARD-FAILURE.json").exists(),
                "resource coverage failed or was incomplete")

    def seed_channels(self):
        title = "DFS consumer " + self.identity
        receipt = self.native("node1", "cluster.agent.channel-write", "channel-seed",
                              channel=self.channel, message="seed-" + self.identity, title=title)
        self.message_receipts = [("seed-" + self.identity, receipt)]

        def append(node):
            # Separate maintained runners make concurrent command receipts and
            # counters independent, while both CLIs address the same DFS channel.
            child_args = copy.copy(self.args)
            child_args.artifacts = self.artifacts / "concurrent" / node
            child = Consumers(child_args)
            child.env = dict(self.env)
            text = "concurrent-" + node + "-" + self.identity
            started = time.monotonic_ns()
            record = child.native(node, "cluster.agent.channel-write", node + "-append",
                                  channel=self.channel, message=text, title=title)
            return text, record, {"node": node, "start_ns": started, "end_ns": time.monotonic_ns()}

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            replies = list(pool.map(append, ("node1", "node5")))
        self.message_receipts += [(text, record) for text, record, _ in replies]
        intervals = [interval for _, _, interval in replies]
        save(self.artifacts / "concurrent-channel-writers.json", intervals)
        require(max(row["start_ns"] for row in intervals) < min(row["end_ns"] for row in intervals),
                "concurrent channel writer invocations did not overlap")
        require(len({record["message_id"] for _, record in self.message_receipts}) == 3,
                "channel writers did not retain three distinct message identities")
        save(self.artifacts / "channel-receipts.json", self.message_receipts)
        self.verify_channels("initial")

    def verify_channels(self, phase):
        for node in NODES:
            for index, (text, receipt) in enumerate(self.message_receipts):
                self.native(node, "cluster.agent.channel-expect", f"{phase}-{node}-channel-{index}",
                    channel=self.channel, message=text, cleanup=False, message_count=3,
                    title="DFS consumer " + self.identity, message_id=receipt["message_id"],
                    source_node_id=receipt["source_node_id"], timeout_seconds=60)

    def notes_first_home(self):
        # Exercise the canonical backend before FUSE or the sandbox initializer
        # can create HOME. Both callers must agree on the same private directory.
        saved = {item["lane"]: item for item in self.layouts("notes-first-original-layout")}
        self.verify_fresh_remote_home_absent(saved, self.agent)
        body = "notes before sandbox HOME " + self.identity
        self.receipts["notes-first"] = self.native("node6", "cluster.sifs.memory-write",
            "notes-before-home-initializer", agent=self.agent, path="interop/first.md", markdown=body)
        home = self.native("node6", "cluster.sifs.agent-home-prepare",
            "notes-first-strict-product-home", agent=self.agent)
        self.verify_remote_home_attributes(saved, home, self.agent)
        for node in NODES:
            self.native(node, "cluster.sifs.memory-expect", f"notes-first-{node}-read",
                agent=self.agent, path="interop/first.md", markdown=body,
                version=self.receipts["notes-first"]["version"], timeout_seconds=60)
        save(self.artifacts / "notes-first-home-receipts.json", {
            "note": self.receipts["notes-first"], "home": home,
            "canonical_home": self.receipts["remote-home-canonical-attributes"]})

    def seed_storage(self):
        self.receipts["home"] = self.native("node1", "cluster.sifs.home-write", "home-seed",
            agent=self.agent, path="roaming.txt", payload=self.home_payload)
        self.receipts["primary-original"] = self.native("node5", "cluster.sifs.memory-write", "memory-primary-seed",
            agent=self.agent, path=self.primary, markdown=self.primary_body)
        self.receipts["primary"] = self.receipts["primary-original"]
        self.receipts["secondary"] = self.native("node5", "cluster.sifs.memory-write", "memory-secondary-seed",
            agent=self.agent, path=self.secondary, markdown=self.secondary_body)
        save(self.artifacts / "consumer-receipts.json", self.receipts)
        self.verify_storage("initial")

    def verify_storage(self, phase):
        for node in NODES:
            if "notes-first" in self.receipts:
                self.native(node, "cluster.sifs.memory-expect", f"{phase}-{node}-notes-first-read",
                    agent=self.agent, path="interop/first.md", markdown="notes before sandbox HOME " + self.identity,
                    version=self.receipts["notes-first"]["version"], timeout_seconds=60)
            self.native(node, "cluster.sifs.home-expect", f"{phase}-{node}-home",
                agent=self.agent, path="roaming.txt", payload=self.home_payload,
                object_id=self.receipts["home"]["object_id"], version=self.receipts["home"]["version"], timeout_seconds=60)
            for label, path, body in (("primary", self.primary, self.primary_body),
                                      ("secondary", self.secondary, self.secondary_body)):
                self.native(node, "cluster.sifs.memory-expect", f"{phase}-{node}-{label}-read",
                    agent=self.agent, path=path, markdown=body, version=self.receipts[label]["version"], timeout_seconds=60)
                search = self.native(node, "cluster.sifs.memory-search", f"{phase}-{node}-{label}-search",
                    agent=self.agent, query=self.common_token, path=path,
                    version=self.receipts[label]["version"], timeout_seconds=60)
                require(search["continuation_count"] > 0 and search["page_count"] > 1,
                        f"{phase}/{node}: search did not exercise an actual continuation")

    def concurrent_fuse_sessions(self):
        script = (DOCKER / "test-fuse-session-overlap.sh").read_text()
        self.run("concurrent-fuse-session-crash",
                 [*self.compose, "exec", "-T", "node1", "timeout", "--kill-after=5s",
                  "150s", "sh", "-c", script, "fuse-session-overlap", self.agent],
                 timeout=160)

    def product_home(self, phase, create=False):
        if create:
            self.native("node1", "cluster.sifs.agent-home-command", phase + "-node1-product-home-create",
                agent=self.agent, payload=self.product_home_payload, name=self.product_home_name, action="create")
        for node in ("node1", "node5"):
            self.native(node, "cluster.sifs.agent-home-command", f"{phase}-{node}-product-home-read",
                agent=self.agent, payload=self.product_home_payload, name=self.product_home_name, action="read")

    @staticmethod
    def native_identity(token):
        parts = token.split(":")
        require(len(parts) == 4 and parts[:2] == ["vfs", "v1"], "unexpected memory-note storage version token")
        return parts[2], parts[3]

    def memory_query(self, node, phase, token, *, expected):
        raw = self.cli(node, f"{phase}-{node}-query", "memory", "query", token,
                       "--max-records", "32", "--max-bytes", "262144")
        bundle = json.loads(raw)
        save(self.artifacts / f"memory-query-{phase}-{node}.json", bundle)
        matching = [item for item in bundle.get("evidence", []) if token in item.get("content", "")]
        if not expected:
            require(not matching, f"{node}: stale old-token evidence remains after source replacement")
            return
        require(matching, f"{node}: query returned no evidence containing the exact unique token")
        original, version = self.native_identity(self.receipts["primary"]["version"])
        root = self.roots[node]
        sources = [item["source"] for item in bundle.get("files", [])]
        source = next((source for source in sources
            if source["version"]["file_ref"]["root_id"] == root["root_id"]
            and source["version"]["file_ref"]["connector_id"] == CONNECTOR
            and source["version"]["file_ref"]["path_hint"] == self.primary), None)
        require(source is not None, f"{node}: evidence has no exact registered SIFS source")
        require(source["platform_identity"] == original
                and source["source_version_hint"].split("\0") == [original, version, str(len(self.primary_body.encode()))]
                and source["version"]["byte_length"] == len(self.primary_body.encode())
                and source["freshness"] == "current", f"{node}: evidence identity/version is not the current native source")
        require(any(item["anchor"]["source_version"] == source["version"] for item in matching),
                f"{node}: matching content is not anchored to the verified source version")

    def index_initial(self):
        memory_root = self.receipts["primary"]["root"]
        for node in ("node1", "node5"):
            root = json.loads(self.cli(node, f"{node}-register-memory-root", "memory", "root", "add",
                                       memory_root, "--filesystem", "sifs"))
            require(root["connector_id"] == CONNECTOR and root["config"]["path"] == memory_root,
                    f"{node}: memory registered a different filesystem source")
            self.roots[node] = root
            self.cli(node, f"{node}-index-initial", "memory", "index", root["root_id"])
            self.memory_query(node, "initial", self.old_token, expected=True)
        save(self.artifacts / "memory-roots.json", self.roots)

    def replace_memory(self):
        self.primary_body = f"# Updated cluster note\n{self.common_token} {self.new_token}\nCurrent λ 文件.\n"
        self.receipts["primary"] = self.native("node3", "cluster.sifs.memory-write", "memory-primary-cas-update",
            agent=self.agent, path=self.primary, markdown=self.primary_body,
            version=self.receipts["primary-original"]["version"])
        require(self.receipts["primary"]["created"] is False
                and self.receipts["primary"]["version"] != self.receipts["primary-original"]["version"],
                "memory replacement did not return a new exact native version")
        self.native("node6", "cluster.sifs.memory-expect", "memory-current-before-guards", agent=self.agent,
            path=self.primary, markdown=self.primary_body, version=self.receipts["primary"]["version"], timeout_seconds=60)
        self.native("node6", "cluster.sifs.memory-write-guards", "memory-create-cas-isolation-guards",
            agent=self.agent, path=self.primary, stale_version=self.receipts["primary-original"]["version"])
        for node, root in self.roots.items():
            self.cli(node, f"{node}-reindex-current", "memory", "index", root["root_id"])
            self.memory_query(node, "current", self.new_token, expected=True)
            self.memory_query(node, "stale-rejected", self.old_token, expected=False)
        save(self.artifacts / "consumer-receipts.json", self.receipts)

    def layouts(self, label):
        return json.loads(self.probe("node1", "shard-layout"))["diagnostics"]["shards"]

    def assignment(self, lane, label):
        return next(item for item in self.layouts(label) if item["lane"] == lane)

    def wait_assignment(self, lane, desired, label):
        deadline = time.monotonic() + 90
        while True:
            actual = self.assignment(lane, label)
            if (actual["primary_node_id"] == desired["primary_node_id"]
                    and actual["epoch"] == desired["epoch"]
                    and set(actual["replica_node_ids"]) == set(desired["replica_node_ids"])):
                return actual
            if time.monotonic() >= deadline:
                self.topology_unknown = True
                raise RuntimeError(f"{label}: exact desired assignment did not activate: {actual}")
            time.sleep(.5)

    def transfer_primary(self, lane, target=None):
        layouts = self.layouts("before-" + lane + "-handoff")
        before = next(item for item in layouts if item["lane"] == lane)
        if target is None:
            primaries = {item["primary_node_id"] for item in layouts}
            target = next(node for node in before["replica_node_ids"] if node not in primaries)
        require(target in before["replica_node_ids"], "promoted primary must be an actual assigned replica")
        self.topology_unknown = True
        requested = json.loads(self.cli("node1", "transfer-" + lane + "-consumer-primary", "dfs", "metadata",
            "shard", "transfer-primary", before["shard_id"], "--filesystem", before["file_system_id"],
            "--to", target, "--expected-epoch", str(before["epoch"]), "--json"))
        desired = requested["pending_handoff"]["desired_shard"]
        require(desired["primary_node_id"] == target and desired["epoch"] == before["epoch"] + 1,
                "handoff request changed expected primary/epoch")
        self.wait_assignment(lane, desired, lane + "-handoff")
        self.topology_unknown = False
        return before, requested

    def handoffs(self):
        records = {}
        for lane in ("root", "services"):
            records[lane] = self.transfer_primary(lane)
            if lane == "root":
                self.native("node1", "cluster.sifs.home-write", "activate-root-consumer-owner", agent=self.agent,
                    path="activation/root.txt", payload="new native home authority " + self.identity)
            else:
                self.native("node1", "cluster.agent.channel-write", "activate-services-consumer-owner",
                    channel="activation-" + self.identity, message="new native channel authority " + self.identity)
            self.wait_ready(lane + "-consumer-handoff")
        save(self.artifacts / "consumer-handoffs.json", records)
        self.verify_channels("handoff")
        self.verify_storage("handoff")
        self.product_home("handoff")
        for node in self.roots:
            self.memory_query(node, "handoff", self.new_token, expected=True)

    def restart_consumers(self):
        self.restart()
        self.verify_channels("restart")
        self.verify_storage("restart")
        self.product_home("restart")
        for node in self.roots:
            # Derived indices stay local and survive with the same DATA volume;
            # retrieval must still bind to the current canonical DFS source.
            self.memory_query(node, "restart", self.new_token, expected=True)
            self.memory_query(node, "restart-stale-rejected", self.old_token, expected=False)

    def set_replicas(self, lane, replicas, label):
        before = self.assignment(lane, label + "-before")
        require(before["primary_node_id"] not in replicas, "replica set must not duplicate its primary")
        argv = ["dfs", "metadata", "shard", "set-replicas", before["shard_id"], "--filesystem",
                before["file_system_id"], "--expected-epoch", str(before["epoch"])]
        for node in replicas:
            argv += ["--replica", node]
        self.topology_unknown = True
        requested = json.loads(self.cli("node1", label, *argv, "--json"))
        desired = requested["pending_handoff"]["desired_shard"]
        require(set(desired["replica_node_ids"]) == set(replicas)
                and desired["primary_node_id"] == before["primary_node_id"]
                and desired["epoch"] == before["epoch"] + 1,
                "membership request changed expected primary, epoch or exact replicas")
        self.wait_assignment(lane, desired, label + "-active")
        self.topology_unknown = False
        return before, requested

    def main_identity(self, node, label):
        script = r'''selected=""
for file in /proc/[0-9]*/cmdline; do
  test -r "$file" || continue
  set -- $(tr '\000' ' ' < "$file")
  test "$#" -ge 3 || continue
  test "$1" = /usr/local/bin/sinomo && test "$2" = daemon && test "$3" = run || continue
  test -z "$selected" || exit 1
  selected=${file%/cmdline}
done
test -n "$selected" || exit 0
pid=${selected##*/}
before=$(sed 's/^.*) //' "$selected/stat" | awk '{print $20}')
digest=$(dd if="$selected/exe" iflag=nocache bs=1M status=none | sha256sum) || exit 1
after=$(sed 's/^.*) //' "$selected/stat" | awk '{print $20}')
test "$before" = "$after" || exit 1
printf '%s %s %s\n' "$pid" "$before" "${digest%% *}"
'''
        raw = self.run(label, [*self.compose, "exec", "-T", node, "bash", "-o", "pipefail", "-c", script], timeout=15)
        if not raw.strip():
            return None
        values = raw.decode().split()
        require(len(values) == 3 and values[0].isdigit() and values[1].isdigit()
                and values[2] == self.binary_hash, f"{node}: Main identity/hash is not the tested artifact")
        return {"pid": int(values[0]), "start_ticks": int(values[1]), "sha256": values[2]}

    def shadow_identity(self, node, label):
        script = r'''set -- $(tr '\000' ' ' < /proc/1/cmdline)
test "$#" -ge 4 || exit 1
case "$1" in /usr/local/bin/sinomo|/usr/local/bin/sinomo-shadow) ;; *) exit 1 ;; esac
test "$2" = daemon && test "$3" = shadow && test "$4" = run || exit 1
before=$(sed 's/^.*) //' /proc/1/stat | awk '{print $20}')
digest=$(dd if=/proc/1/exe iflag=nocache bs=1M status=none | sha256sum) || exit 1
test "$(sed 's/^.*) //' /proc/1/stat | awk '{print $20}')" = "$before" || exit 1
printf '1 %s %s\n' "$before" "${digest%% *}"
'''
        raw = self.run(label, [*self.compose, "exec", "-T", node, "bash", "-o", "pipefail", "-c", script], timeout=15)
        values = raw.decode().split()
        require(len(values) == 3 and values[0] == "1" and values[1].isdigit()
                and values[2] == self.binary_hash, f"{node}: PID1 Shadow identity/hash differs from the owned artifact")
        return {"pid": 1, "start_ticks": int(values[1]), "sha256": values[2]}

    def restart_main(self, node, label):
        before = self.main_identity(node, label + "-before")
        require(before is not None, "targeted Main restart requires exactly one existing daemon run process")
        shadow_before = self.shadow_identity(node, label + "-shadow-before")
        # Revalidate the exact PID/start/executable and argv at the signal
        # boundary. PID1 shadow and the long-lived probe CLI are never targets.
        script = r'''pid=$1; expected_start=$2; expected_hash=$3
set -- $(tr '\000' ' ' < "/proc/$pid/cmdline")
test "$#" -ge 3 && test "$1" = /usr/local/bin/sinomo && test "$2" = daemon && test "$3" = run || exit 1
actual_start=$(sed 's/^.*) //' "/proc/$pid/stat" | awk '{print $20}')
digest=$(dd if="/proc/$pid/exe" iflag=nocache bs=1M status=none | sha256sum) || exit 1
test "$actual_start" = "$expected_start" && test "${digest%% *}" = "$expected_hash" || exit 1
test "$(sed 's/^.*) //' "/proc/$pid/stat" | awk '{print $20}')" = "$expected_start" || exit 1
kill -TERM "$pid"
'''
        self.run(label + "-terminate-exact-main", [*self.compose, "exec", "-T", node, "bash", "-o", "pipefail", "-c", script,
            "bash", str(before["pid"]), str(before["start_ticks"]), before["sha256"]], timeout=15)
        deadline, index, start_attempted = time.monotonic() + 90, 0, False
        while True:
            after = self.main_identity(node, f"{label}-after-{index}")
            if after is None and not start_attempted:
                require(self.shadow_identity(node, label + "-shadow-before-start") == shadow_before,
                        "PID1 Shadow changed before the controlled Main start")
                # An operator TERM is deliberately not a product crash/restart
                # request. Observe absence, then use the supported public start
                # once; a failed/unknown start is retained without resubmission.
                after = self.main_identity(node, label + "-before-public-start")
                if after is None:
                    remaining = deadline - time.monotonic()
                    require(remaining > 0, "controlled Main restart exhausted its original budget before start")
                    start_attempted = True
                    self.cli(node, label + "-public-main-start", "daemon", "start", "--host", "0.0.0.0",
                             "--port", "21353", "--no-port-fallback", timeout=min(30, remaining))
                    after = self.main_identity(node, label + "-after-public-start")
            if after is not None and (after["pid"], after["start_ticks"]) != (before["pid"], before["start_ticks"]):
                shadow_after = self.shadow_identity(node, label + "-shadow-after")
                require(shadow_after == shadow_before, "controlled Main restart changed PID1 Shadow")
                save(self.artifacts / (label + "-main-generation.json"), {"before": before, "after": after,
                    "shadow_before": shadow_before, "shadow_after": shadow_after,
                    "public_start_attempted": start_attempted,
                    "scope": "exact Main-only controlled stop and supported public start; PID1 Shadow, container, probe CLI and DATA retained"})
                # When CPU profiling is requested, the container entrypoint
                # profiles its initial Main. A public Main-only restart has its
                # own profiler, so establish the same bounded capture for this
                # actual replacement generation.
                if os.environ.get("SINOMO_TEST_CPU_PROFILE") != "1":
                    return
                config = json.loads((self.artifacts / "compose-effective.json").read_text())
                environment = config["services"][node]["environment"]
                duration = int(environment["SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS"])
                frequency = int(environment["SINOMO_TEST_FORMATION_OBSERVE_FREQUENCY_HZ"])
                require(1 <= duration <= 30 and 10 <= frequency <= 500,
                        "replacement Main profile requires the existing bounded formation settings")
                remaining = deadline - time.monotonic()
                require(remaining > 0, "Main restart budget expired before its CPU capture")
                capture = f"provider-{node}-{after['pid']}-{after['start_ticks']}"
                started = json.loads(self.cli(node, label + "-cpu-profile-start", "observe", "profile", "cpu", "start",
                    "--capture", capture, "--duration-seconds", str(duration), "--frequency-hz", str(frequency),
                    "--json", timeout=min(30, remaining)))
                require(isinstance(started, dict) and started.get("supported") is True
                        and started.get("state") in ("starting", "running", "finalizing", "complete")
                        and started.get("capture_id") == capture
                        and started.get("duration_seconds") == duration
                        and started.get("frequency_hz") == frequency
                        and started.get("error") is None,
                        "replacement Main did not accept the exact bounded CPU start; original response retained without resubmission")
                require(self.main_identity(node, label + "-cpu-profile-generation") == after,
                        "Main changed during its replacement CPU capture")
                save(self.artifacts / (label + "-cpu-profile.json"), {"main": after, "capture": capture,
                    "duration_seconds": duration, "frequency_hz": frequency, "start_response": started,
                    "scope": "one accepted bounded asynchronous CPU start on this Main; completion/window require exact native manifest and pprof; full observe gate unchanged"})
                return
            require(time.monotonic() < deadline, f"{node}: controlled restart did not produce a new Main within its original budget")
            index += 1
            time.sleep(.25)

    def verify_online_cluster(self, label):
        """Live health observation only; this does not establish DFS readiness."""
        expected = set()
        for alias in NODES:
            identity = json.loads(self.probe(alias, "shard-layout"))["diagnostics"]["node"]
            expected.add(identity)
        require(len(expected) == 6, "diagnostic must observe six distinct local DFS owners")
        raw = self.cli("node1", label + "-health", "cluster", "health", timeout=30)
        rows = raw.decode().splitlines()
        require(rows and rows[0].split("\t")[:5] == ["node", "process", "role", "member", "health"],
                "cluster health output has another schema")
        observed = {}
        for line in rows[1:]:
            values = line.split("\t")
            require(len(values) >= 5, "cluster health row is truncated")
            if values[0] in expected:
                observed.setdefault(values[0], {})[values[1].lower()] = values[4].lower()
        require(set(observed) == expected and all(roles.get("daemon-main") == "online"
                for roles in observed.values()), "six-node Main cluster health is not online")
        save(self.artifacts / (label + "-online-members.json"), observed)
        # The health projection contains Main rows. Prove both actual native
        # roles separately with the same stable PID/start/hash fence used by
        # the retained deployment verifier, rather than inventing shadow health.
        spec = importlib.util.spec_from_file_location("sifs_diagnostic_process_roles", DOCKER / "profile-idle.py")
        idle = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(idle)
        def roles(phase):
            return {node: idle.parse_process_sample(self.run(f"{label}-{phase}-{node}",
                [*self.compose, "exec", "-T", node, "sh", "-c", idle.PROCESS_SAMPLE], timeout=10).decode())
                for node in NODES}
        before = roles("roles-before")
        self.verify_processes(label + "-role-hashes")
        after = roles("roles-after")
        seals = {}
        for node in NODES:
            seals[node] = {}
            for role in ("main", "shadow"):
                first, last = before[node]["processes"][role], after[node]["processes"][role]
                require((first["pid"], first["start_ticks"]) == (last["pid"], last["start_ticks"]),
                        "diagnostic role changed during executable hash observation")
                seals[node][role] = {"pid": last["pid"], "start_ticks": last["start_ticks"], "sha256": self.binary_hash}
        save(self.artifacts / (label + "-main-shadow-seals.json"), seals)
        return expected

    @contextlib.contextmanager
    def withdrawn_dfs_provider(self, node6, saved, before_disable, before_restore, *,
                               require_nonprimary=False, full_readiness=True):
        """One owner for metadata duties, module lifecycle and known restoration."""
        require(callable(before_disable) and callable(before_restore),
                "provider transition requires its real observer lifecycle callbacks")
        save(self.artifacts / "provider-original-assignments.json", saved)
        if require_nonprimary:
            require(set(saved) == {"root", "services", "users"}, "diagnostic requires exactly three saved assignments")
            require(all(item["primary_node_id"] != node6 for item in saved.values()),
                    "diagnostic requires node6 to be nonprimary on every metadata shard before any effect")
        changed, disabled, module_unknown = [], False, False
        restoration = {"passed": False, "saved": saved, "changed": changed,
                       "full_readiness_checked": full_readiness}
        try:
            for lane in saved:
                current = self.assignment(lane, "provider-prepare-" + lane)
                if require_nonprimary:
                    require(current["primary_node_id"] == saved[lane]["primary_node_id"]
                            and current["epoch"] == saved[lane]["epoch"]
                            and set(current["replica_node_ids"]) == set(saved[lane]["replica_node_ids"]),
                            "diagnostic assignment changed after its exact saved cut")
                if current["primary_node_id"] == node6:
                    primaries = {item["primary_node_id"] for item in self.layouts("provider-move-primary")}
                    target = next(item for item in current["replica_node_ids"] if item not in primaries and item != node6)
                    self.transfer_primary(lane, target)
                    self.wait_ready("provider-primary-moved-" + lane)
                    current = self.assignment(lane, "provider-moved-" + lane)
                if node6 in current["replica_node_ids"]:
                    self.set_replicas(lane, [item for item in current["replica_node_ids"] if item != node6],
                                      "provider-remove-duty-" + lane)
                    changed.append(lane)
            before_disable()
            module_unknown = True
            raw = self.cli("node6", "disable-local-dfs-provider", "module", "native-rust", "disable", "module.dfs",
                           "--reason", "isolated provider withdrawal consumer acceptance")
            require(b"enabled: false" in raw, "DFS disable did not return a committed configuration result")
            disabled = True
            module_unknown = False
            self.restart_main("node6", "restart-withdrawn-provider")
            yield
        finally:
            original_error = sys.exc_info()[1]
            if original_error is not None:
                restoration["original_error"] = f"{type(original_error).__name__}: {original_error}"
            errors = []
            try:
                before_restore(restoration)
            except BaseException as error:
                errors.append("observer drain: " + str(error))
            if module_unknown:
                errors.append("module disable has no verified receipt; no replacement configuration mutation attempted")
            provider_restored = not module_unknown and not disabled
            if disabled:
                try:
                    raw = self.cli("node6", "restore-local-dfs-provider", "module", "native-rust", "enable", "module.dfs",
                                   "--reason", "restore isolated consumer fixture")
                    require(b"enabled: true" in raw, "DFS enable did not return committed configuration")
                    self.restart_main("node6", "restart-restored-provider")
                    provider_restored = True
                except BaseException as error:
                    errors.append(str(error))
            if self.topology_unknown:
                errors.append("an original topology transition is unresolved; no replacement mutation attempted")
            elif provider_restored:
                for lane in reversed(changed):
                    try:
                        current = self.assignment(lane, "provider-restore-" + lane)
                        if require_nonprimary:
                            require(current["primary_node_id"] == saved[lane]["primary_node_id"]
                                    and current["epoch"] == saved[lane]["epoch"] + 1
                                    and set(current["replica_node_ids"]) == set(saved[lane]["replica_node_ids"]) - {node6},
                                    "diagnostic no longer owns the exact removal assignment; no replacement mutation attempted")
                        members = [saved[lane]["primary_node_id"], *saved[lane]["replica_node_ids"]]
                        self.set_replicas(lane, [member for member in members if member != current["primary_node_id"]],
                                          "provider-restore-membership-" + lane)
                    except BaseException as error:
                        errors.append(str(error))
                        break
            if not errors:
                try:
                    actual = {item["lane"]: item for item in self.layouts("provider-restored-exact-layout")}
                    restoration["restored_assignments"] = actual
                    if require_nonprimary:
                        require(set(actual) == set(saved) and all(
                            actual[lane]["primary_node_id"] == saved[lane]["primary_node_id"]
                            and set(actual[lane]["replica_node_ids"]) == set(saved[lane]["replica_node_ids"])
                            and actual[lane]["epoch"] == saved[lane]["epoch"] + (2 if lane in changed else 0)
                            for lane in saved), "diagnostic did not restore the exact saved assignments at expected successor epochs")
                    if full_readiness:
                        self.wait_ready("provider-restored")
                    else:
                        self.verify_online_cluster("diagnostic-provider-restored")
                        restoration["qualification"] = "known module/membership/identity restoration only; full DFS readiness remains unqualified"
                    self.verify_processes("provider-restored")
                    restoration["passed"] = True
                except BaseException as error:
                    errors.append(str(error))
            restoration["errors"] = errors
            save(self.artifacts / "provider-restoration.json", restoration)
            require(not errors, "provider fixture restoration is incomplete: " + "; ".join(errors))

    def remote_root_alias(self, saved):
        owner = saved["root"]["primary_node_id"]
        aliases = {json.loads(self.probe(alias, "shard-layout"))["diagnostics"]["node"]: alias
                   for alias in NODES if alias != "node6"}
        require(owner in aliases, "saved root metadata owner is not a remote owned provider")
        return aliases[owner]

    def verify_fresh_remote_home_absent(self, saved, agent):
        # This isolated short safe identity is unchanged by the public product
        # agent_home_sifs_path_for_id mapping. No directory is seeded here.
        path = f"/sinomo/agents/{agent}/home"
        root = saved["root"]
        self.cli(self.remote_root_alias(saved), "remote-root-owner-fresh-home-absent",
                 "dfs", "path", path, "--filesystem", root["file_system_id"], "--json", timeout=30,
                 expected_success=False,
                 expected_error=f"DFS path {path} in filesystem {root['file_system_id']} has not been written".lower())
        self.receipts["remote-home-absent-before-initializer"] = {"agent": agent, "path": path,
            "observer": root["primary_node_id"], "typed_cli_outcome": "MissingArtifact"}

    def verify_remote_home_attributes(self, saved, home, agent):
        """Observe the created root through its actual remote metadata owner."""
        root = saved["root"]
        require(home["agent"] == agent and home["initializer"] == "prepare_simple_sandbox_agent_home"
                and home["fuse_mount_requested"] is False and home["sandbox_provisioned"] is False,
                "HOME diagnostic did not use exactly the normal metadata initializer")
        expected = home["home"]
        owner = root["primary_node_id"]
        alias = self.remote_root_alias(saved)
        require(self.receipts["remote-home-absent-before-initializer"]["path"] == expected,
                "initializer path differs from the exact previously absent fixture")
        raw = self.cli(alias, "remote-root-owner-home-attributes", "dfs", "path", expected,
                       "--filesystem", root["file_system_id"], "--json", timeout=30)
        actual = json.loads(raw)["object"]
        require(actual["path"] == expected and actual["file_system_id"] == root["file_system_id"]
                and actual["kind"] == "directory" and bool(actual["object_id"])
                and actual["mode"] == 0o700 and actual["uid"] == home["expected_uid"]
                and actual["gid"] == home["expected_gid"],
                "remote canonical HOME identity/owner/mode differs from the strict initializer")
        self.receipts["remote-home-canonical-attributes"] = {
            "observer": owner, "alias": alias, "object": actual,
            "qualification": "read-only actual root-primary observation after one fresh initializer; no mounted/native second store"}

    def provider_withdrawal(self, *, metadata_home_only=False):
        node6 = json.loads(self.probe("node6", "shard-layout"))["diagnostics"]["node"]
        saved = {item["lane"]: item for item in self.layouts("provider-original-layout")}
        process, began, stdout, stderr = None, None, None, None
        control_id = "provider-" + self.identity
        control_root = f"/shared/consumer-tests/{control_id}"

        def before_disable():
            nonlocal process, began, stdout, stderr
            args = ["test", "run", "cluster.agent.channel-provider-reopen", "--require-cluster", "--json",
                    "--param", f"control-id={control_id}", "--param", f"original-provider={node6}",
                    "--param", "timeout-seconds=240"]
            command = [*self.compose, "exec", "-T", "node6", "timeout", "--foreground", "-k", "5s",
                       "250s", "/usr/local/bin/sinomo", *args]
            save(self.artifacts / "provider-reopen.command.json", command)
            stdout = (self.artifacts / "provider-reopen.stdout").open("wb")
            stderr = (self.artifacts / "provider-reopen.stderr").open("wb")
            began = time.monotonic()
            process = subprocess.Popen(command, cwd=self.root, env=self.env, stdout=stdout, stderr=stderr)
            deadline = began + 60
            index = 0
            while True:
                require(process.poll() is None, "provider probe ended before its initial read-only open")
                raw = self.run(f"provider-ready-{index}", [*self.compose, "exec", "-T", "node6", "sh", "-c",
                    'if test -f "$1"; then cat "$1"; fi', "sh", control_root + "/ready.json"], timeout=10)
                if raw:
                    ready = json.loads(raw)
                    require(ready["control_id"] == control_id and ready["original_provider"] == node6,
                            "provider handshake names another fixture or owner")
                    break
                require(time.monotonic() < deadline, "provider probe did not publish its ready barrier")
                index += 1
                time.sleep(.25)

        def before_restore(restoration):
            if process is not None and process.poll() is None:
                # The probe itself performs reads only. Let its in-container
                # timeout drain it before restoring its selected provider;
                # disconnecting the Docker client would leave it unobserved.
                try:
                    process.wait(timeout=max(1, began + 265 - time.monotonic()))
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)
                    restoration["probe_drain"] = "host deadline exhausted; original probe coverage incomplete"
            for stream in (stdout, stderr):
                if stream is not None and not stream.closed:
                    stream.close()

        with self.withdrawn_dfs_provider(node6, saved, before_disable, before_restore,
                require_nonprimary=metadata_home_only, full_readiness=not metadata_home_only):
            release = json.dumps({"control_id": control_id, "original_provider": node6, "phase": "provider-withdrawn"})
            self.run("release-provider-probe", [*self.compose, "exec", "-T", "node6", "sh", "-c",
                'printf "%s" "$2" > "$1.pending" && mv "$1.pending" "$1"',
                "sh", control_root + "/release.json", release], timeout=15)
            process.wait(timeout=max(1, began + 265 - time.monotonic()))
            stdout.close()
            stderr.close()
            save(self.artifacts / "provider-reopen.timing.json", {"elapsed_ms": (time.monotonic() - began) * 1000,
                                                                 "returncode": process.returncode})
            require(process.returncode == 0, "same-instance provider reopen case failed")
            report = json.loads((self.artifacts / "provider-reopen.stdout").read_text())
            save(self.artifacts / "probe-provider-reopen.json", report)
            require(report.get("status") == "passed", "provider reopen case did not establish actual withdrawal")
            if metadata_home_only:
                self.verify_fresh_remote_home_absent(saved, self.agent + "-remote-home")
            self.receipts["remote-home-prepare"] = self.native("node6", "cluster.sifs.agent-home-prepare",
                "remote-provider-strict-home-prepare", agent=self.agent + "-remote-home")
            if metadata_home_only:
                self.verify_remote_home_attributes(saved, self.receipts["remote-home-prepare"], self.agent + "-remote-home")
            else:
                self.native("node6", "cluster.sifs.home-expect", "remote-provider-home-read", agent=self.agent,
                    path="roaming.txt", payload=self.home_payload, version=self.receipts["home"]["version"], timeout_seconds=60)
                self.native("node6", "cluster.sifs.home-write", "remote-provider-home-write", agent=self.agent,
                    path="provider/remote.txt", payload="remote home provider " + self.identity)
                remote = self.native("node6", "cluster.sifs.memory-write", "remote-provider-memory-write", agent=self.agent,
                    path="provider/remote.md", markdown="remote memory provider " + self.identity)
                self.native("node1", "cluster.sifs.memory-expect", "remote-provider-memory-visible", agent=self.agent,
                    path="provider/remote.md", markdown="remote memory provider " + self.identity,
                    version=remote["version"], timeout_seconds=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--provider-withdrawal", action="store_true",
                        help="also remove node6 metadata duties, withdraw its DFS provider, then restore it")
    parser.add_argument("--metadata-home-only", action="store_true",
                        help="diagnostic: withdraw nonprimary node6 DFS and invoke one fresh strict HOME initializer; no full readiness claim")
    parser.add_argument("--notes-home-only", action="store_true",
                        help="diagnostic: canonical notes first, strict private HOME, then real FUSE/sandbox HOME; no full readiness claim")
    args = parser.parse_args()
    if not __debug__:
        parser.error("correctness checks require ordinary Python execution without -O")
    if sum((args.metadata_home_only, args.notes_home_only, args.provider_withdrawal)) > 1:
        parser.error("HOME diagnostics and the full provider consumer workflow are separate modes")
    if args.artifacts.exists() and any(args.artifacts.iterdir()):
        parser.error("artifacts must be new; existing results cannot be overwritten")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error("--binary must be the exact executable mounted in this owned project")
    args.restart_stagger_seconds, args.max_write_retries = 0, 0
    run = Consumers(args)
    report = {"schema": "sinomo.sifs-consumer-verification.v1", "passed": False,
              "case": "SIFS channels, canonical agent homes and indexed memory consumers",
              "project": args.project, "binary": str(run.binary), "binary_sha256": run.binary_hash,
              "profile": args.profile, "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=run.root, text=True).strip(),
              "agent": run.agent, "channel": run.channel, "provider_withdrawal": args.provider_withdrawal,
              "performance_acceptance": False,
              "outage_coverage": "cooperative metadata handoff and all-node abrupt restart; this lane does not establish surviving writes during abrupt primary loss",
              "qualification": "Canonical daemon-backed consumers on all six nodes; real product FUSE/simple-sandbox HOME on node1 and node5. Full cluster release remains a separate gate.",
              "retention": "existing project, containers, volumes, test effects and failures retained; no reset/volume deletion"}
    if args.metadata_home_only:
        report.update(case="DIAGNOSTIC: strict HOME initializer with local DFS module disabled",
                      diagnostic_only=True, full_consumer_acceptance=False, readiness_acceptance=False,
                      outage_coverage="controlled nonprimary DFS provider withdrawal and exact known restoration",
                      qualification="One fresh metadata-only product HOME initializer on node6, actual provider-catalog withdrawal, and remote root-owner attributes; no FUSE, sandbox command, channel message persistence, full readiness or quietness claim")
    if args.notes_home_only:
        report.update(case="DIAGNOSTIC: notes-first canonical HOME and real sandbox interoperability",
                      diagnostic_only=True, full_consumer_acceptance=False, readiness_acceptance=False,
                      outage_coverage="none; all nodes remain online",
                      qualification="One canonical note before HOME initialization, strict remote native attributes, exact note reads on all six nodes, and real FUSE/simple-sandbox HOME on node1/node5; no full cluster readiness or handoff/restart claim")
    patch = subprocess.check_output(["git", "diff", "--binary"], cwd=run.root)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z", "--", "crates"],
                                        cwd=run.root).decode().split("\0")
    report["untracked_source_hashes"] = {}
    for relative in sorted(path for path in untracked if path.endswith(".rs") or Path(path).name == "Cargo.toml"):
        content = (run.root / relative).read_bytes()
        report["untracked_source_hashes"][relative] = hashlib.sha256(content).hexdigest()
        addition = subprocess.run(["git", "diff", "--binary", "--no-index", "/dev/null", relative],
                                  cwd=run.root, capture_output=True)
        require(addition.returncode in (0, 1), "could not archive untracked source " + relative)
        patch += addition.stdout
    (run.artifacts / "source.patch").write_bytes(patch)
    report["source_patch_sha256"] = hashlib.sha256(patch).hexdigest()
    verifier = Path(__file__).read_bytes()
    (run.artifacts / "verifier.py").write_bytes(verifier)
    report["verifier_sha256"] = hashlib.sha256(verifier).hexdigest()
    dependency = (DOCKER / "test-file-history.py").read_bytes()
    (run.artifacts / "history-verifier.py").write_bytes(dependency)
    report["history_verifier_sha256"] = hashlib.sha256(dependency).hexdigest()
    def resource_failure(*_):
        raise RuntimeError("hard runtime resource failure")
    old_signal = signal.signal(signal.SIGUSR1, resource_failure)
    failure = None
    try:
        run.start_watch()
        if args.metadata_home_only:
            run.phase("diagnostic-source-and-live-health", lambda: (
                run.verify_processes("initial"), run.verify_observe(), run.verify_online_cluster("diagnostic-initial")))
            run.phase("diagnostic-metadata-home-withdrawal-and-restoration",
                      lambda: run.provider_withdrawal(metadata_home_only=True))
        elif args.notes_home_only:
            run.phase("diagnostic-source-and-live-health", lambda: (
                run.verify_processes("initial"), run.verify_observe(), run.verify_online_cluster("diagnostic-initial")))
            run.phase("notes-first-private-home-interoperability", run.notes_first_home)
            run.phase("product-FUSE-sandbox-agent-home", lambda: run.product_home("initial", create=True))
            run.phase("concurrent-fuse-session-crash", run.concurrent_fuse_sessions)
        else:
            run.phase("source-and-runtime-identity", lambda: (run.verify_processes("initial"), run.verify_observe(), run.wait_ready("consumer-initial")))
            run.phase("channels-concurrent-and-all-node", run.seed_channels)
            run.phase("notes-first-private-home-interoperability", run.notes_first_home)
            run.phase("product-FUSE-sandbox-agent-home", lambda: run.product_home("initial", create=True))
            run.phase("concurrent-fuse-session-crash", run.concurrent_fuse_sessions)
            run.phase("canonical-home-and-memory-all-node", run.seed_storage)
            run.phase("independent-node-memory-indexes", run.index_initial)
            run.phase("memory-cas-isolation-and-index-refresh", run.replace_memory)
            run.phase("consumer-metadata-handoffs", run.handoffs)
            run.phase("abrupt-restart-consumer-durability", run.restart_consumers)
            if args.provider_withdrawal:
                run.phase("provider-withdrawal-and-restoration", run.provider_withdrawal)
            run.phase("final-invariants", lambda: [run.probe(node, "invariants", timeout_seconds=60) for node in NODES])
            run.phase("final-routed-status", lambda: run.probe("node1", "routed-status", timeout_seconds=90))
        run.verify_processes("final")
        report["passed"] = True
    except BaseException as error:
        report["error"] = f"{type(error).__name__}: {error}"
        failure = error
    finally:
        try:
            run.stop_watch()
        except BaseException as error:
            report["passed"] = False
            report["resource_error"] = f"{type(error).__name__}: {error}"
            failure = failure or error
        signal.signal(signal.SIGUSR1, old_signal)
        report["phases"] = run.phases
        report["completed_commands"] = run.completed
        report["receipts"] = run.receipts
        report["memory_roots"] = run.roots
        for name in ("result.json", "results.json"):
            save(run.artifacts / name, report)
        history.verification_report.write_report(run.artifacts)
    if failure:
        raise failure


if __name__ == "__main__":
    main()
