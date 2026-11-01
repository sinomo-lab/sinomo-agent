#!/usr/bin/env python3
"""DFS normal/history acceptance and CLI benchmarks on a ready six-node cluster.

Run docker/test-cluster.sh with SINOMO_TEST_DFS_READY_ONLY=1 and
SINOMO_TEST_KEEP=1 first. This verifier retains the project and its evidence.
--restart-cluster and --case restart/core-only permit killing all six containers with SIGKILL.
The core-only case toggles only its two feature modules and restores them before exit.
It tests durable completed history and retry receipts, not a crash at every WAL
boundary or incomplete-replication failover.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import time

report_spec = importlib.util.spec_from_file_location(
    "verification_report", Path(__file__).with_name("verification_report.py"))
verification_report = importlib.util.module_from_spec(report_spec)
report_spec.loader.exec_module(verification_report)


def operation_id():
    value = (time.time_ns() // 1_000_000 << 80) | secrets.randbits(80)
    alphabet = "0123456789abcdefghjkmnpqrstvwxyz"
    return "".join(alphabet[(value >> shift) & 31] for shift in range(125, -1, -5))


def summarize_samples(samples):
    groups = {}
    for sample in samples:
        key = (sample["profile"], sample["origin"], sample["operation"])
        groups.setdefault(key, []).append(sample["elapsed_ms"])
    return [{"profile": key[0], "origin": key[1], "operation": key[2],
             "count": len(values),
             "p50_ms": sorted(values)[math.ceil(len(values) * .50) - 1],
             "p95_ms": sorted(values)[math.ceil(len(values) * .95) - 1],
             "max_ms": max(values), "operations_per_second": len(values) * 1000 / sum(values)}
            for key, values in sorted(groups.items())]


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.root = Path(__file__).resolve().parent.parent
        self.artifacts = args.artifacts.resolve()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.binary = args.binary.resolve()
        self.env = dict(os.environ, SINOMO_DOCKER_BINARY=str(self.binary))
        self.compose = ["docker", "compose", "-p", args.project, "-f",
                        os.environ.get("COMPOSE_FILE", str(self.root / "docker/docker-compose.dev.yml"))]
        if os.environ.get("SINOMO_TEST_COMPOSE_OVERRIDE"):
            self.compose += ["-f", os.environ["SINOMO_TEST_COMPOSE_OVERRIDE"]]
        self.index = 0
        self.completed = []
        self.samples = []
        self.prefix = f"/services/cluster-tests/history-{operation_id()}"
        with self.binary.open("rb") as binary:
            self.binary_hash = hashlib.file_digest(binary, "sha256").hexdigest()

    def run(self, label, argv, *, expected_success=True, expected_error=None, timeout=120):
        if argv[:len(self.compose)] == self.compose and argv[len(self.compose)] in ("kill", "restart"):
            from runtime_resources import NODES, expect_exit
            expect_exit([arg for arg in argv[len(self.compose) + 1:] if arg in NODES] or sorted(NODES), label)
        self.index += 1
        stem = self.artifacts / f"{self.index:03d}-{label}"
        stem.with_suffix(".command.json").write_text(json.dumps(argv, indent=2) + "\n")
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=self.root, env=self.env, capture_output=True,
                                    timeout=timeout, check=False)
        except subprocess.TimeoutExpired as error:
            stem.with_suffix(".stdout").write_bytes(error.stdout or b"")
            stem.with_suffix(".stderr").write_bytes(error.stderr or b"")
            stem.with_suffix(".timing.json").write_text(json.dumps({
                "elapsed_ms": (time.monotonic() - started) * 1000, "timed_out": True,
            }) + "\n")
            raise
        stem.with_suffix(".timing.json").write_text(json.dumps({
            "elapsed_ms": (time.monotonic() - started) * 1000,
            "returncode": result.returncode, "timed_out": False,
        }) + "\n")
        stem.with_suffix(".stdout").write_bytes(result.stdout)
        stem.with_suffix(".stderr").write_bytes(result.stderr)
        if (result.returncode == 0) != expected_success:
            raise RuntimeError(f"{label}: exit {result.returncode}; see {stem}.stderr")
        expected_errors = ((expected_error,) if isinstance(expected_error, str) else expected_error)
        if expected_errors and not any(error in (result.stdout + result.stderr).decode().lower()
                                       for error in expected_errors):
            raise RuntimeError(f"{label}: missing expected error {expected_error!r}; see {stem}.stderr")
        print(f"ok: {label}", flush=True)
        self.completed.append(label)
        return result.stdout

    def cli(self, node, label, *args, **kwargs):
        return self.run(label, [*self.compose, "exec", "-T", node, "sinomo", *args], **kwargs)

    def probe(self, node, case, **params):
        args = ["test", "run", f"cluster.dfs.{case}", "--require-cluster", "--json"]
        for name, value in params.items():
            args += ["--param", f"{name.replace('_', '-')}={value}"]
        raw = self.cli(node, f"{node}-{case}", *args)
        report = json.loads(raw)
        if report.get("status") != "passed":
            raise RuntimeError(f"{node}-{case}: probe did not pass: {report}")
        return raw

    def write(self, node, path, payload):
        result = json.loads(self.probe(node, "write", path=path, payload=payload,
                           max_retries=self.args.max_write_retries, timeout_seconds=60))
        assert result["diagnostics"].get("wait_for_commit") is True, result

    def expect(self, node, path, payload):
        self.probe(node, "expect-file", path=path, payload=payload, timeout_seconds=60)

    def history(self, node, action, *args, expected_success=True, expected_error=None):
        raw = self.cli(node, f"{node}-history-{action}", "dfs", "history", action,
                       *args, "--json", expected_success=expected_success, expected_error=expected_error)
        return json.loads(raw) if expected_success else None

    def listing(self, node, path, object_id=None, limit=128):
        entries, cursor, seen, identities = [], None, set(), set()
        while True:
            args = [path, "--limit", str(limit)]
            if object_id:
                args += ["--object", object_id]
            if cursor:
                args += ["--cursor", cursor]
            page = self.history(node, "list", *args)
            for entry in page["entries"]:
                identity = json.dumps(entry["identity"], sort_keys=True)
                assert identity not in identities, "history pagination duplicated a version"
                identities.add(identity)
                entries.append(entry)
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                return entries
            assert next_cursor not in seen, "history cursor repeated"
            seen.add(next_cursor)
            cursor = next_cursor

    def read(self, node, identity, expected):
        data = self.cli(node, f"{node}-history-read", "dfs", "history", "read",
                        "--object", identity["object_id"], "--version", identity["version"])
        assert data == expected.encode(), (identity, data)

    def verify_processes(self, label):
        for node in [f"node{i}" for i in range(1, 7)]:
            # Debug executables can exceed a GiB. Hash every byte without
            # retaining the one-shot read in the daemon's capped file cache.
            script = ('for exe in /proc/[0-9]*/exe; do '
                      'target=$(readlink "$exe") || continue; '
                      'case "$target" in */sinomo|*/sinomo-shadow) '
                      'digest=$(dd if="$exe" iflag=nocache bs=1M status=none | sha256sum) || exit 1; '
                      'printf "%s  %s\\n" "${digest%% *}" "$exe";; esac; done')
            raw = self.run(f"{label}-{node}-process-hashes",
                           [*self.compose, "exec", "-T", node, "bash", "-o", "pipefail", "-c", script])
            hashes = [line.split()[0] for line in raw.decode().splitlines()]
            assert len(hashes) >= 2 and set(hashes) == {self.binary_hash}, (node, hashes)

    def verify_observe(self, require_cpu_capture=False):
        """All Docker gates use debug/trace-enabled processes and profiler support."""
        configurations = {}
        for node in (f"node{i}" for i in range(1, 7)):
            debug = self.run(
                f"{node}-observe-debug-env", [*self.compose, "exec", "-T", node,
                    "sh", "-c", 'printf "%s" "$SINOMO_DFS_PERF_TRACE"'])
            control = json.loads(self.cli(node, f"{node}-observe-control", "observe", "control", "--json"))
            profile = json.loads(self.cli(node, f"{node}-observe-cpu", "observe", "profile", "cpu", "status", "--json"))
            traces = json.loads(self.cli(node, f"{node}-observe-capacity", "observe", "traces",
                                        "--target-prefix", "__config_only__", "--limit", "0", "--json"))
            configurations[node] = {"debug": debug.decode(), "control": control, "profile": profile,
                                    "trace_capacity": traces.get("capacity")}
            (self.artifacts / "observe-configuration.json").write_text(json.dumps(configurations, indent=2) + "\n")
            if (debug.strip() != b"1" or "sinomo.perf=trace" not in control.get("active_filter", "")
                    or not profile.get("supported") or profile.get("state") == "failed"
                    or (require_cpu_capture and profile.get("state") not in ("running", "finalizing", "complete"))
                    or (traces.get("capacity") or 0) < 32768):
                raise RuntimeError(f"{node}: Docker gates require DFS debug, generic tracing, 32768 trace slots and CPU profiler support; "
                                   f"debug={debug!r}, CPU={profile.get('state')}, supported={profile.get('supported')}, "
                                   f"capacity={traces.get('capacity')}, startup_capture_required={require_cpu_capture}")

    def restart(self):
        phase = f"restart-{self.index}"
        nodes = [f"node{i}" for i in range(1, 7)]
        self.run("abrupt-cluster-stop", [*self.compose, "kill", "-s", "SIGKILL", *nodes])
        if self.args.restart_stagger_seconds:
            # Keep the AB/B bridge down briefly after its peers start. This
            # exercises ordinary connection refusals during cold recovery.
            early = [node for node in nodes if node != "node4"]
            self.run("restart-early-peers", [*self.compose, "up", "-d", "--no-build", "--no-deps", *early])
            self.run("restart-bridge-delay", ["sleep", str(self.args.restart_stagger_seconds)])
            self.run("restart-bridge", [*self.compose, "up", "-d", "--no-build", "--no-deps", "node4"])
        else:
            self.run("restart-existing-volumes", [*self.compose, "up", "-d", "--no-build", *nodes])
        self.wait_ready(phase)
        self.verify_processes("restarted")

    def wait_ready(self, phase):
        previous = self.env
        self.env = dict(previous, SINOMO_TEST_PROJECT=self.args.project, SINOMO_TEST_BUILD="0",
                        SINOMO_TEST_BINARY=str(self.binary), SINOMO_TEST_REUSE="1",
                        SINOMO_TEST_KEEP="1", SINOMO_TEST_DFS_READY_ONLY="1",
                        SINOMO_TEST_DFS_ONLY="0", SINOMO_TEST_DFS_BASELINE_ONLY="0",
                        SINOMO_TEST_NETWORK_CASES="",
                        SINOMO_TEST_ARTIFACTS=str(self.artifacts / phase))
        try:
            self.run(phase + "-readiness", ["bash", "docker/test-cluster.sh"], timeout=600)
        finally:
            self.env = previous

    def exercise_restart_readiness(self):
        self.verify_processes("initial")
        path = self.prefix + "/restart.txt"
        self.write("node1", path, "retained-before-restart")
        saved = self.history("node5", "capture", path, "--pin", "restart-check")["identity"]
        self.write("node6", path, "current-before-restart")
        for _ in range(self.args.restart_cycles):
            self.restart()
            self.expect("node1", path, "current-before-restart")
            self.expect("node5", path, "current-before-restart")
            self.read("node2", saved, "retained-before-restart")
            self.read("node6", saved, "retained-before-restart")

    def exercise(self):
        self.verify_processes("initial")
        path = self.prefix + "/file.txt"
        self.write("node1", path, "alpha")
        self.write("node5", path, "beta")
        assert self.listing("node2", path) == [], "default writes created history"
        self.probe("node6", "expect-replicated", path=path, timeout_seconds=60)
        saved = self.history("node5", "capture", path, "--pin", "checkpoint",
                             "--operation-id", operation_id())["identity"]
        self.write("node6", path, "gamma")
        self.read("node2", saved, "beta")
        self.expect("node4", path, "gamma")
        identity_args = ["--object", saved["object_id"], "--version", saved["version"]]
        self.history("node3", "pin", path, *identity_args, "--pin", "audit")
        self.history("node6", "unpin", path, *identity_args, "--pin", "checkpoint")
        entries = self.listing("node1", path)
        assert len(entries) == 1 and entries[0]["pins"] == ["audit"], entries
        if self.args.restart_cluster:
            self.restart()
            self.read("node5", saved, "beta")
            self.expect("node6", path, "gamma")
        before = self.history("node4", "capture", path, "--pin", "before-restore")["identity"]
        # Exercise one-record cursors on this object's family, without making
        # every path query traverse unrelated accumulated fixtures one row/RPC.
        paged = self.listing("node2", path, saved["object_id"], limit=1)
        assert {entry["identity"]["version"] for entry in paged} == {saved["version"], before["version"]}, paged
        restore_args = [path, *identity_args, "--expected-object", before["object_id"],
                        "--expected-version", before["version"], "--operation-id", operation_id()]
        restored = self.history("node6", "restore", *restore_args)
        assert restored["committed"]["version"] != saved["version"], restored
        self.expect("node1", path, "beta")
        self.write("node2", path, "after-restore")
        checkpoint = {"phase": "after-restore-write", "project": self.args.project,
                      "binary_sha256": self.binary_hash, "prefix": self.prefix,
                      "saved": saved, "restore_args": restore_args, "restored": restored}
        (self.artifacts / "checkpoint.json").write_text(json.dumps(checkpoint, indent=2) + "\n")
        if self.args.restart_cluster:
            self.restart()
        self.finish_after_restore(saved, restore_args, restored)

    def exercise_normal(self, *, history_enabled=True):
        """Sequential canonical mutations observed through every segment."""
        self.verify_processes("initial")
        nodes = [f"node{i}" for i in range(1, 7)]
        for origin, peer in [("node1", "node5"), ("node5", "node1")]:
            path = self.prefix + f"/normal-{origin}.txt"
            # Short and empty replacement detect stale tails; Unicode exercises
            # byte lengths independently from character counts.
            for writer, payload in [(origin, "long-body-" * 128), (peer, "short"),
                                    (origin, ""), (peer, "version-λ-文件\n")]:
                self.write(writer, path, payload)
                for observer in nodes:
                    self.expect(observer, path, payload)
            if history_enabled:
                assert self.listing(peer, path) == [], "ordinary writes retained versions"
            renamed = path + ".renamed"
            self.probe("node3", "rename", **{"from": path, "to": renamed})
            for observer in nodes:
                self.expect(observer, renamed, payload)
                self.probe(observer, "expect-absent", path=path, timeout_seconds=60)
            self.probe(peer, "remove", path=renamed)
            for observer in nodes:
                self.probe(observer, "expect-absent", path=renamed, timeout_seconds=60)
            self.write(origin, path, "new-object")
            self.expect(peer, path, "new-object")
            if history_enabled:
                assert self.listing(peer, path) == [], "path reuse acquired history"
            self.probe(peer, "remove", path=path)
        for node in nodes:
            self.probe(node, "invariants", timeout_seconds=60)

    def feature_routes_absent(self, node, path):
        # Rejection must identify the removed route; a permission, timeout, or
        # filesystem error is not evidence of logical withdrawal.
        for feature, args, route in [
            ("history", ["dfs", "history", "list", path, "--json"],
             "/daemon/dfs/browser/file-history/list-v1"),
            ("attachments", ["dfs", "attachment", "list", path],
             "/daemon/dfs/browser/attachments-v2"),
        ]:
            self.cli(node, f"{node}-{feature}-route-absent", *args,
                     expected_success=False,
                     expected_error=tuple(f"unknown {kind} route: {route}"
                                          for kind in ["service", "object rpc"]))

    def set_filesystem_features(self, enabled):
        action = "enable" if enabled else "disable"
        errors = []
        for node in (f"node{i}" for i in range(1, 7)):
            for module in ["module.dfs.attachments", "module.dfs.history"]:
                try:
                    raw = self.cli(node, f"{node}-{module}-{action}", "module", "native-rust",
                                   action, module, "--reason", "core-only filesystem acceptance")
                    assert f"enabled: {str(enabled).lower()}".encode() in raw, raw
                except Exception as error:
                    errors.append(f"{node}/{module}: {error}")
        if errors:
            raise RuntimeError(f"feature {action} incomplete: " + "; ".join(errors))

    def exercise_core_only(self):
        """Core operations and durable roots survive independently disabled features."""
        self.verify_processes("initial")
        nodes = [f"node{i}" for i in range(1, 7)]
        self.probe("node6", "semantic-lost-reply", timeout_seconds=120)
        source = self.prefix + "/preserved-source.txt"
        self.write("node1", source, "explicitly-retained-before-disable")
        saved = self.history("node5", "capture", source, "--pin", "core-only-preserved")["identity"]
        self.write("node6", source, "current-before-disable")
        payload = "semantic-attachment-preserved-λ".encode()
        upload = self.artifacts / "core-only-attachment.txt"
        upload.write_bytes(payload)
        container_upload = "/tmp/sinomo-core-only-" + operation_id()
        self.run("copy-core-only-attachment", [*self.compose, "cp", str(upload), f"node1:{container_upload}"])
        attachment_args = ["dfs", "attachment", "get", source, "--kind", "sinomo.test.core-only.v1", "--name", "preserved"]
        self.cli("node1", "create-core-only-attachment", "dfs", "attachment", "put", source,
                 "--kind", "sinomo.test.core-only.v1", "--name", "preserved", "--schema", "sinomo.test.core-only/v1",
                 "--media-type", "text/plain", "--input", container_upload)
        assert self.cli("node5", "read-core-only-attachment-before", *attachment_args) == payload
        checkpoint = {"source": source, "saved": saved, "attachment_sha256": hashlib.sha256(payload).hexdigest(),
                      "modules": ["module.dfs.attachments", "module.dfs.history"], "nodes": nodes,
                      "restored": False}
        (self.artifacts / "core-only.json").write_text(json.dumps(checkpoint, indent=2) + "\n")
        # Preserve configured state across the process restart; never recreate
        # volumes or replace the verified executable during the toggle test.
        try:
            self.set_filesystem_features(False)
            self.restart()
            self.verify_observe()
            for node in nodes:
                self.expect(node, source, "current-before-disable")
                self.feature_routes_absent(node, source)
            self.exercise_normal(history_enabled=False)
            self.write("node5", source, "core-overwrite-while-features-disabled")
            for node in nodes:
                self.expect(node, source, "core-overwrite-while-features-disabled")
                self.probe(node, "invariants", timeout_seconds=60)
            checkpoint["core_operations_passed"] = True
        finally:
            restore_errors = []
            try:
                self.set_filesystem_features(True)
            except Exception as error:
                restore_errors.append(str(error))
            try:
                self.restart()
                self.verify_observe()
            except Exception as error:
                restore_errors.append(str(error))
            if restore_errors:
                raise RuntimeError("feature restoration incomplete: " + "; ".join(restore_errors))
        for node in nodes:
            self.read(node, saved, "explicitly-retained-before-disable")
            assert self.cli(node, f"{node}-attachment-preserved-after", *attachment_args) == payload
            self.expect(node, source, "core-overwrite-while-features-disabled")
        checkpoint["restored"] = True
        (self.artifacts / "core-only.json").write_text(json.dumps(checkpoint, indent=2) + "\n")

    def measured(self, profile, origin, operation, measured, action):
        started = time.monotonic_ns()
        first_command = self.index + 1
        result = action()
        elapsed_ms = (time.monotonic_ns() - started) / 1_000_000
        if measured:
            self.samples.append({"profile": profile, "origin": origin,
                                 "operation": operation, "elapsed_ms": elapsed_ms,
                                 "first_command": first_command, "last_command": self.index})
        return result

    def benchmark(self):
        """CLI end-to-end distributions; validation is outside mutation timers."""
        self.verify_processes("initial")
        for origin, peer in [("node1", "node5"), ("node5", "node1")]:
            for profile in ["ordinary", "retention", "checkpoint"]:
                base = self.prefix + f"/bench/{origin}/{profile}"
                if profile == "retention":
                    self.history(origin, "retention", base, "--keep-last", "2")
                for index in range(self.args.warmup + self.args.iterations):
                    measured = index >= self.args.warmup
                    path = base + f"/{index}.txt"
                    payload = (f"{index}:" + "a" * self.args.payload_bytes)[:self.args.payload_bytes]
                    replacement = "b" * self.args.payload_bytes
                    def sample(operation, action):
                        return self.measured(profile, origin, operation, measured, action)
                    sample("create", lambda: self.write(origin, path, payload))
                    self.expect(peer, path, payload)
                    saved = None
                    if profile == "checkpoint":
                        saved = sample("capture", lambda: self.history(
                            peer, "capture", path, "--pin", "benchmark"))["identity"]
                    sample("overwrite", lambda: self.write(origin, path, replacement))
                    sample("current-read", lambda: self.expect(peer, path, replacement))
                    entries = sample("history-list", lambda: self.listing(peer, path))
                    if profile == "ordinary":
                        assert entries == [], entries
                    else:
                        assert len(entries) == 1, entries
                        identity = entries[0]["identity"]
                        if saved:
                            assert identity == saved, entries
                        sample("history-read", lambda: self.read(peer, identity, payload))
                        current = self.history(origin, "capture", path, "--pin", "before-restore")["identity"]
                        identity_args = ["--object", identity["object_id"], "--version", identity["version"]]
                        sample("restore", lambda: self.history(
                            origin, "restore", path, *identity_args,
                            "--expected-object", current["object_id"],
                            "--expected-version", current["version"], "--operation-id", operation_id()))
                        self.expect(peer, path, payload)
                        recovered = path + ".recovered"
                        sample("recover", lambda: self.history(peer, "recover", recovered, *identity_args))
                        self.expect(origin, recovered, payload)
                        self.probe(origin, "remove", path=recovered)
                    sample("remove", lambda: self.probe(origin, "remove", path=path))
                    self.probe(peer, "expect-absent", path=path, timeout_seconds=60)
        for node in [f"node{i}" for i in range(1, 7)]:
            self.probe(node, "invariants", timeout_seconds=60)

    def exercise_handoff(self):
        self.verify_processes("initial")
        path = self.prefix + "/handoff.txt"
        self.write("node6", path, "retained-across-handoff")
        self.probe("node1", "expect-replicated", path=path, timeout_seconds=60)
        saved = self.history("node5", "capture", path, "--pin", "handoff")["identity"]
        self.write("node3", path, "current-across-handoff")
        before = json.loads(self.probe("node1", "shard-layout"))["diagnostics"]["shards"]
        shard = next(item for item in before if item["lane"] == "services")
        primaries = {item["primary_node_id"] for item in before}
        target = next(node for node in shard["replica_node_ids"] if node not in primaries)
        target_container = next(
            node for node in [f"node{i}" for i in range(1, 7)]
            if json.loads(self.probe(node, "shard-layout"))["diagnostics"]["node"] == target)
        # Open this node's history mutation lane while it is still a replica.
        self.history(target_container, "pin", path, "--object", saved["object_id"],
                     "--version", saved["version"], "--pin", "before-primary-change")
        requested = json.loads(self.cli("node1", "transfer-services-primary", "dfs", "metadata", "shard",
                              "transfer-primary", "services", "--to", target,
                              "--expected-epoch", str(shard["epoch"]), "--json"))
        assert requested["pending_handoff"]["desired_shard"]["primary_node_id"] == target, requested
        assert requested["shard"]["primary_node_id"] == shard["primary_node_id"], requested
        deadline = time.monotonic() + 60
        while True:
            layouts = json.loads(self.probe("node1", "shard-layout"))["diagnostics"]["shards"]
            assigned = next(item for item in layouts if item["lane"] == "services")
            if assigned["primary_node_id"] == target and assigned["epoch"] == shard["epoch"] + 1:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"handoff assignment did not commit: {assigned}")
            time.sleep(1)
        # Exercise promoted-authority rebind through an ordinary committed
        # operation before requiring its outgoing replication counters to be
        # initialized. This operation still needs the full publication proof.
        self.write("node1", self.prefix + "/activation.txt", "new-primary-active")
        # The standard gate requires zero pending handoffs, exact topology and
        # zero replica lag across two observations, plus successful mutations.
        self.wait_ready("handoff")
        after = json.loads(self.probe("node1", "shard-layout"))["diagnostics"]["shards"]
        current = next(item for item in after if item["lane"] == "services")
        assert current["primary_node_id"] == target and current["epoch"] == shard["epoch"] + 1, current
        promoted_capture = self.history(target_container, "capture", path,
                                        "--pin", "after-primary-change")["identity"]
        assert promoted_capture["object_id"] == saved["object_id"] and promoted_capture["version"] != saved["version"], promoted_capture
        self.read("node6", saved, "retained-across-handoff")
        self.expect("node5", path, "current-across-handoff")
        recovered = self.prefix + "/handoff-recovered.txt"
        self.history("node3", "recover", recovered, "--object", saved["object_id"],
                     "--version", saved["version"])
        self.expect("node1", recovered, "retained-across-handoff")
        self.expect("node6", path, "current-across-handoff")
        (self.artifacts / "handoff.json").write_text(json.dumps({
            "before": shard, "requested": requested, "after": current, "retained_identity": saved,
            "promoted_capture": promoted_capture, "promoted_container": target_container,
        }, indent=2) + "\n")

    def resume(self, checkpoint_path):
        checkpoint = json.loads(checkpoint_path.read_text())
        assert checkpoint["phase"] == "after-restore-write", checkpoint
        assert checkpoint["project"] == self.args.project
        assert checkpoint["binary_sha256"] == self.binary_hash
        self.prefix = checkpoint["prefix"]
        assert self.prefix.startswith("/services/cluster-tests/history-")
        assert checkpoint["restore_args"][0] == self.prefix + "/file.txt"
        self.verify_processes("resumed")
        self.expect("node3", self.prefix + "/file.txt", "after-restore")
        self.read("node2", checkpoint["saved"], "beta")
        self.finish_after_restore(checkpoint["saved"], checkpoint["restore_args"], checkpoint["restored"])

    def finish_after_restore(self, saved, restore_args, restored):
        path = self.prefix + "/file.txt"
        identity_args = ["--object", saved["object_id"], "--version", saved["version"]]
        replayed = self.history("node6", "restore", *restore_args)
        assert replayed == restored, (replayed, restored)
        self.expect("node3", path, "after-restore")
        stale_restore = [*restore_args[:-1], operation_id()]
        self.history("node5", "restore", *stale_restore,
                     expected_success=False, expected_error="changed")
        self.expect("node2", path, "after-restore")
        self.probe("node3", "remove", path=path)
        assert len(self.listing("node5", path)) == 2
        self.write("node6", path, "reused-path")
        self.read("node2", saved, "beta")
        recovered_path = self.prefix + "/recovered.txt"
        self.history("node5", "recover", recovered_path, *identity_args)
        self.expect("node1", recovered_path, "beta")
        self.expect("node4", path, "reused-path")
        self.history("node2", "recover", recovered_path, *identity_args,
                     expected_success=False, expected_error="already exists")
        self.expect("node3", recovered_path, "beta")
        # Rename changes the namespace, never the retained object's identity.
        renamed = self.prefix + "/renamed.txt"
        self.probe("node4", "rename", **{"from": path, "to": renamed})
        reused = self.history("node5", "capture", renamed, "--pin", "path-reuse")["identity"]
        assert reused["object_id"] != saved["object_id"], (saved, reused)
        self.read("node1", saved, "beta")
        self.read("node2", reused, "reused-path")
        moved = renamed + ".again"
        self.probe("node6", "rename", **{"from": renamed, "to": moved})
        entries = self.listing("node3", moved, reused["object_id"])
        assert len(entries) == 1 and entries[0]["identity"] == reused, entries
        self.expect("node1", moved, "reused-path")
        self.probe("node5", "expect-absent", path=renamed, timeout_seconds=60)
        # Cross the history CLI's 64 KiB range boundary, then shorten the head.
        large_path = self.prefix + "/ranged-history.txt"
        large_payload = ("history-range-" * 6000)[:65537]
        self.write("node1", large_path, large_payload)
        large = self.history("node5", "capture", large_path, "--pin", "ranged")["identity"]
        self.write("node6", large_path, "short-head")
        self.read("node2", large, large_payload)
        large_recovery = large_path + ".recovered"
        self.history("node3", "recover", large_recovery,
                     "--object", large["object_id"], "--version", large["version"])
        self.expect("node6", large_recovery, large_payload)
        self.expect("node4", large_path, "short-head")
        rule_path = self.prefix + "/rules/file.txt"
        self.history("node5", "retention", self.prefix + "/rules", "--keep-last", "1")
        for payload in ["rule-a", "rule-b", "rule-c"]:
            self.write("node6", rule_path, payload)
        rule_entries = self.listing("node2", rule_path)
        assert len(rule_entries) == 1, rule_entries
        self.read("node3", rule_entries[0]["identity"], "rule-b")
        self.probe("node4", "remove", path=rule_path)
        deleted = self.listing("node5", rule_path)
        assert len(deleted) == 1, deleted
        self.read("node1", deleted[0]["identity"], "rule-c")
        self.history("node6", "unpin", path, *identity_args, "--pin", "audit")
        assert all(entry["identity"] != saved for entry in self.listing("node2", path))
        for node in [f"node{i}" for i in range(1, 7)]:
            self.probe(node, "invariants", timeout_seconds=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--restart-cluster", action="store_true")
    parser.add_argument("--case", choices=["normal", "history", "handoff", "benchmark", "restart", "observe", "core-only"], default="history",
                        help="handoff transfers the services primary; restart repeatedly SIGKILLs all six containers")
    parser.add_argument("--iterations", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--restart-cycles", type=int, default=3, help="cycles for --case restart")
    parser.add_argument("--restart-stagger-seconds", type=int, default=0,
                        help="delay node4 by 0..10 seconds after starting its peers")
    parser.add_argument("--resume-from", type=Path,
                        help="continue a saved post-restore checkpoint after readiness has been restored")
    parser.add_argument("--max-write-retries", type=int, default=0,
                        help="maximum observed write-probe retries; each probe still has a 60s deadline")
    args = parser.parse_args()
    if not __debug__:
        parser.error("Python optimization disables correctness assertions; omit -O/PYTHONOPTIMIZE")
    if not 1 <= args.iterations <= 1000 or not 0 <= args.warmup <= 100:
        parser.error("iterations must be 1..1000 and warmup 0..100")
    if not 1 <= args.payload_bytes <= 32768:
        parser.error("payload-bytes must be 1..32768 (CLI argument workload)")
    if args.case != "history" and (args.resume_from or args.restart_cluster):
        parser.error("resume/restart options apply only to --case history")
    if not 1 <= args.restart_cycles <= 20 or not 0 <= args.restart_stagger_seconds <= 10:
        parser.error("restart-cycles must be 1..20 and restart-stagger-seconds 0..10")
    if args.restart_stagger_seconds and args.case != "restart" and not args.restart_cluster:
        parser.error("restart staggering requires --case restart or --restart-cluster")
    if not 0 <= args.max_write_retries <= 2:
        parser.error("--max-write-retries must be in 0..=2")
    run = Acceptance(args)
    report = {"passed": False, "project": args.project, "binary": str(run.binary), "sha256": run.binary_hash,
              "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=run.root,
                                                       text=True).strip(),
              "path_prefix": run.prefix, "restart_cluster": args.restart_cluster or args.case in ("restart", "core-only"),
              "case": args.case,
              "restart_parameters": {"cycles": args.restart_cycles if args.case == "restart" else None,
                                     "stagger_seconds": args.restart_stagger_seconds},
              "benchmark_parameters": {"iterations": args.iterations, "warmup": args.warmup,
                                       "payload_bytes": args.payload_bytes},
              "max_write_retries": args.max_write_retries,
              "path_list_limit": 128, "object_pagination_limit": 1,
              "resume_from": str(args.resume_from.resolve()) if args.resume_from else None}
    verifier = Path(__file__).read_bytes()
    report["verifier_sha256"] = hashlib.sha256(verifier).hexdigest()
    (run.artifacts / "verifier.py").write_bytes(verifier)
    patch = subprocess.check_output(["git", "diff", "--binary"], cwd=run.root)
    # git diff alone omits newly added source modules. Preserve the exact
    # untracked Rust sources and manifests used by the candidate as reviewable additions.
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z", "--", "crates"],
        cwd=run.root).decode().split("\0")
    report["untracked_rust_sources"] = {}
    report["untracked_manifests"] = {}
    for relative in sorted(path for path in untracked
                           if path.endswith(".rs") or Path(path).name == "Cargo.toml"):
        content = (run.root / relative).read_bytes()
        bucket = "untracked_rust_sources" if relative.endswith(".rs") else "untracked_manifests"
        report[bucket][relative] = hashlib.sha256(content).hexdigest()
        addition = subprocess.run(["git", "diff", "--binary", "--no-index", "/dev/null", relative],
                                  cwd=run.root, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if addition.returncode not in (0, 1):
            raise RuntimeError(addition.stderr.decode())
        patch += addition.stdout
    (run.artifacts / "source.patch").write_bytes(patch)
    try:
        run.run("docker-context", ["docker", "context", "show"])
        run.run("compose-config", [*run.compose, "config"])
        run.verify_observe(require_cpu_capture=args.case == "observe")
        if args.case == "observe":
            run.verify_processes("initial")
        elif args.case == "restart":
            run.exercise_restart_readiness()
        elif args.case == "normal":
            run.exercise_normal()
        elif args.case == "core-only":
            run.exercise_core_only()
        elif args.case == "benchmark":
            run.benchmark()
        elif args.case == "handoff":
            run.exercise_handoff()
        elif args.resume_from:
            run.resume(args.resume_from)
        else:
            run.exercise()
        report["passed"] = True
    except Exception as error:
        report.update(passed=False, error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["path_prefix"] = run.prefix
        report["completed_commands"] = run.completed
        if args.case == "benchmark":
            (run.artifacts / "benchmark.json").write_text(json.dumps({
                "schema": "sinomo.dfs-cli-benchmark.v1", "passed": report.get("passed", False),
                "provenance": {key: report[key] for key in
                               ("project", "binary", "sha256", "source_head", "verifier_sha256")},
                "timing_scope": "host wall time including Docker exec, CLI startup and RPC; read probes may poll",
                "percentiles": "nearest rank", "parameters": report["benchmark_parameters"],
                "samples": run.samples, "summary": summarize_samples(run.samples),
            }, indent=2) + "\n")
        (run.artifacts / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        verification_report.write_report(run.artifacts)


if __name__ == "__main__":
    main()
