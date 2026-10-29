#!/usr/bin/env python3
import copy
import hashlib
import json
import os
from pathlib import Path
import queue
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import runtime_resources as resources


class RuntimeResources(unittest.TestCase):
    def config(self):
        return {node: {"mem_limit": resources.NODE_LIMIT, "memswap_limit": resources.NODE_LIMIT}
                for node in resources.NODES}

    def test_six_nodes_are_twelve_gib_and_limits_cannot_be_raised(self):
        self.assertEqual(resources.require_limits(self.config()), 12 * 1024**3)
        encoded = {node: {key: str(value) for key, value in row.items()} for node, row in self.config().items()}
        self.assertEqual(resources.require_limits(encoded), 12 * 1024**3)
        for field in ["mem_limit", "memswap_limit"]:
            bad = self.config()
            bad["node3"][field] *= 2
            with self.assertRaises(RuntimeError):
                resources.require_limits(bad)
        with self.assertRaises(RuntimeError):
            resources.require_limits({})
        scaled = self.config()
        scaled["node1"]["deploy"] = {"replicas": 2}
        with self.assertRaises(RuntimeError):
            resources.require_limits(scaled)

    def test_host_preflight_does_not_add_a_fictitious_four_gib_driver_allocation(self):
        original = Path.read_text
        def read(path, *args, **kwargs):
            if str(path) == "/proc/meminfo":
                return f"MemTotal: {32 * 1024**2} kB\nMemAvailable: {13 * 1024**2} kB\n"
            return original(path, *args, **kwargs)
        with tempfile.TemporaryDirectory(prefix="sinomo-resource-control-") as tmp:
            with patch.object(Path, "read_text", read), patch.object(resources, "docker", return_value=tmp):
                result = resources.preflight({"services": self.config()}, Path(tmp))
        self.assertEqual(result["node_allocation_bytes"], 12 * 1024**3)

    def test_exit_allowance_is_exact_one_use_and_does_not_match_old_events(self):
        event = {"timeNano": 20, "Actor": {"Attributes": {"com.docker.compose.service": "node2"}}}
        intents = [{"time_ns": 10, "nodes": ["node2"], "reason": "restart durability"}]
        consumed = set()
        self.assertTrue(resources.expected_die(event, intents, consumed))
        self.assertFalse(resources.expected_die(event, intents, consumed))
        old = copy.deepcopy(event)
        old["timeNano"] = 9
        self.assertFalse(resources.expected_die(old, intents, set()))
        wrong_node = copy.deepcopy(event)
        wrong_node["Actor"]["Attributes"]["com.docker.compose.service"] = "node3"
        self.assertFalse(resources.expected_die(wrong_node, intents, set()))

    def reused_sample(self, anonymous_bytes):
        return {node: {"container_id": f"container-{node}", "pid": i + 100,
                       "start_ticks": i + 1000, "cgroup": f"/docker-{node}",
                       "cgroup_identity": [1, i], "anonymous_bytes": anonymous_bytes,
                       "memory_current_bytes": resources.NODE_LIMIT}
                for i, node in enumerate(sorted(resources.NODES))}

    def with_available(self, available):
        original = Path.read_text
        def read(path, *args, **kwargs):
            if str(path) == "/proc/meminfo":
                return f"MemTotal: {32 * 1024**2} kB\nMemAvailable: {available // 1024} kB\nSwapTotal: 0 kB\n"
            return original(path, *args, **kwargs)
        return patch.object(Path, "read_text", read)

    def test_reuse_does_not_charge_already_resident_anonymous_memory_twice(self):
        # Actual failed nested-readiness MemAvailable; the owner snapshots here
        # are controlled inputs, not reconstructed historical cgroup evidence.
        available = 12623179776
        sample = self.reused_sample(512 * 1024**2)
        config = {"name": "owned", "services": self.config()}
        with tempfile.TemporaryDirectory(prefix="sinomo-reuse-admission-") as tmp:
            with self.with_available(available), patch.object(resources, "docker", return_value=tmp), \
                 patch.object(resources, "sample_reused_nodes", return_value=sample):
                result = resources.preflight(config, Path(tmp), reuse_project="owned")
                self.assertEqual(result["node_allocation_bytes"], 12 * 1024**3)
                self.assertEqual(result["additional_available_bytes_required"], 9 * 1024**3)
                with self.assertRaises(RuntimeError):
                    resources.preflight(config, Path(tmp))

    def test_reuse_never_credits_reclaimable_file_cache(self):
        sample = self.reused_sample(0)  # current charge is still2GiB per node
        with tempfile.TemporaryDirectory(prefix="sinomo-reuse-cache-") as tmp:
            with self.with_available(11 * 1024**3), patch.object(resources, "docker", return_value=tmp), \
                 patch.object(resources, "sample_reused_nodes", return_value=sample):
                with self.assertRaises(RuntimeError):
                    resources.preflight({"name": "owned", "services": self.config()}, Path(tmp), "owned")

    def test_reuse_credits_lower_bracketing_charge_and_rejects_changed_owners(self):
        before = self.reused_sample(512 * 1024**2)
        after = self.reused_sample(256 * 1024**2)
        self.assertEqual(resources.reused_anonymous_credit(before, after), 6 * 256 * 1024**2)
        for key in ["container_id", "pid", "start_ticks", "cgroup", "cgroup_identity"]:
            bad = copy.deepcopy(after)
            bad["node6"][key] = "changed"
            with self.assertRaises(RuntimeError):
                resources.reused_anonymous_credit(before, bad)
        bad = copy.deepcopy(after)
        del bad["node6"]
        with self.assertRaises(RuntimeError):
            resources.reused_anonymous_credit(before, bad)
        bad = copy.deepcopy(after)
        bad["node6"]["anonymous_bytes"] = None
        with self.assertRaises(RuntimeError):
            resources.reused_anonymous_credit(before, bad)

    def test_reuse_requires_exact_project_and_complete_counter_observation(self):
        with tempfile.TemporaryDirectory(prefix="sinomo-reuse-owner-") as tmp:
            with patch.object(resources, "sample_reused_nodes") as sample:
                with self.assertRaises(RuntimeError):
                    resources.preflight({"name": "other", "services": self.config()}, Path(tmp), "owned")
                sample.assert_not_called()
            with patch.object(resources, "sample_reused_nodes", side_effect=FileNotFoundError("missing memory.stat")):
                with self.assertRaises(FileNotFoundError):
                    resources.preflight({"name": "owned", "services": self.config()}, Path(tmp), "owned")

    def test_reuse_reader_binds_anon_to_live_identity_and_rejects_missing_or_swapped_data(self):
        rows = [{"Id": f"container-{node}", "Config": {"Labels": {
                    "com.docker.compose.project": "owned", "com.docker.compose.service": node}},
                 "pid": i + 100} for i, node in enumerate(sorted(resources.NODES))]
        values = {"memory.stat": "anon 256\nfile 768\n", "memory.current": "1024", "memory.swap.current": "0"}
        def checked(row):
            return {"node": row["Config"]["Labels"]["com.docker.compose.service"], "running": True,
                    "pid": row["pid"], "cgroup": f"/docker-{row['pid']}", "limits_and_counters": {}}
        def read(path, *args, **kwargs):
            if path.name == "cgroup":
                return f"0::/docker-{path.parent.name}\n"
            if path.name == "stat":
                fields = ["0"] * 20
                fields[0], fields[19] = "S", "12345"
                return f"{path.parent.name} (owned fixture) " + " ".join(fields)
            return values[path.name]
        def stat(path, *args, **kwargs):
            return SimpleNamespace(st_dev=1, st_ino=int(path.name.rsplit("-", 1)[1]))
        with patch.object(resources, "inspect_nodes", return_value=rows), \
             patch.object(resources, "check_node", side_effect=checked), \
             patch.object(Path, "read_text", read), patch.object(Path, "stat", stat):
            sample = resources.sample_reused_nodes("owned")
            self.assertEqual(set(sample), resources.NODES)
            self.assertTrue(all(row["anonymous_bytes"] == 256 for row in sample.values()))
            values["memory.stat"] = "file 1024\n"
            with self.assertRaises(RuntimeError):
                resources.sample_reused_nodes("owned")
            values["memory.stat"] = "anon 256\nfile 768\n"
            values["memory.swap.current"] = "1"
            with self.assertRaises(RuntimeError):
                resources.sample_reused_nodes("owned")
            values["memory.swap.current"] = "0"
            with patch.object(resources, "process_start", side_effect=[1, 2]):
                with self.assertRaises(RuntimeError):
                    resources.sample_reused_nodes("owned")

    def test_effective_limit_swap_oom_and_restart_fail_closed(self):
        row = {"Id": "container123", "Config": {"Labels": {"com.docker.compose.service": "node1"}},
               "HostConfig": {"Memory": resources.NODE_LIMIT, "MemorySwap": resources.NODE_LIMIT},
               "State": {"Running": True, "OOMKilled": False, "Pid": 123}, "RestartCount": 0}
        values = {"cgroup": "0::/docker-container123.scope\n", "memory.max": str(resources.NODE_LIMIT),
                  "memory.swap.max": "0", "memory.current": "1024", "memory.peak": "2048",
                  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"}
        def read(path, *args, **kwargs):
            return values[path.name]
        with patch.object(Path, "read_text", read):
            self.assertTrue(resources.check_node(row)["running"])
            for key, value in [("memory.swap.max", "1"), ("memory.max", str(4 * 1024**3)),
                               ("memory.events", "max 1\noom 0\noom_kill 0"),
                               ("memory.events", "max 0\noom 1\noom_kill 1")]:
                before = values[key]
                values[key] = value
                with self.assertRaises(RuntimeError):
                    resources.check_node(row)
                values[key] = before
            row["RestartCount"] = 1
            with self.assertRaises(RuntimeError):
                resources.check_node(row)

    def test_retired_pid_is_unavailable_not_zero_counters(self):
        row = {"Id": "container123", "Config": {"Labels": {"com.docker.compose.service": "node1"}},
               "HostConfig": {"Memory": resources.NODE_LIMIT, "MemorySwap": resources.NODE_LIMIT},
               "State": {"Running": True, "OOMKilled": False, "Pid": 123}, "RestartCount": 0}
        def read(path, *args, **kwargs):
            if path.name == "cgroup":
                return "0::/docker-container123.scope\n"
            raise FileNotFoundError(path)
        with patch.object(Path, "read_text", read), patch.object(Path, "exists", return_value=False):
            result = resources.check_node(row)
            self.assertFalse(result["running"])
            self.assertNotIn("limits_and_counters", result)
        with patch.object(Path, "read_text", read), patch.object(Path, "exists", return_value=True):
            with self.assertRaises(FileNotFoundError):
                resources.check_node(row)


class ActivationResources(unittest.TestCase):
    def setUp(self):
        self.rows = [{"Id": f"{i:064x}", "Config": {"Labels": {
                         "com.docker.compose.project": "owned", "com.docker.compose.service": f"node{i}"}},
                      "HostConfig": {"Memory": resources.NODE_LIMIT, "MemorySwap": resources.NODE_LIMIT},
                      "State": {"Running": True, "OOMKilled": False, "Pid": 100 + i,
                                "StartedAt": f"initial-{i}", "ExitCode": 0}, "RestartCount": 0}
                     for i in range(1, 7)]
        self.cid = self.rows[1]["Id"]
        self.intent = {"kind": "rollout-shadow-replacement", "time_ns": 110, "nodes": ["node2"],
                       "project": "owned", "container_id": self.cid, "old_pid": 102,
                       "old_started_at": "initial-2", "old_restart_count": 0,
                       "candidate_sha256": "a" * 64, "rollout_id": "plan1",
                       "claim_path": "/data/rollout/staged/uploads/plan1-target2/activation-started.sobj",
                       "claim_sha256": "b" * 64}
        self.new = copy.deepcopy(self.rows[1])
        self.new["RestartCount"] = 1
        self.new["State"].update(Pid=202, StartedAt="replacement-2")

    def lifecycle(self, claimed=True):
        result = resources.ActivationLifecycle("owned", "a" * 64, copy.deepcopy(self.rows), 100)
        if claimed:
            result.refresh([copy.deepcopy(self.intent)])
        return result

    def event(self, action):
        return {"Action": action, "timeNano": 120 if action == "die" else 130,
                "Actor": {"ID": self.cid, "Attributes": {"com.docker.compose.project": "owned",
                          "com.docker.compose.service": "node2", "exitCode": "0"}}}

    def complete(self, life):
        life.event(self.event("die"), 0)
        life.event(self.event("start"), 0.1)
        life.observe(copy.deepcopy(self.new), 0.2)

    def test_each_event_inspect_delivery_order_reconciles_without_rewriting_counts(self):
        import itertools
        for order in itertools.permutations(["die", "start", "inspect"]):
            with self.subTest(order=order):
                life = self.lifecycle()
                raw = copy.deepcopy(self.new)
                for i, action in enumerate(order):
                    if action == "inspect":
                        life.observe(raw, i * 0.1)
                        self.assertEqual(raw["RestartCount"], 1)
                        if i < 2:
                            self.assertTrue(life.classification(self.cid)["lifecycle_pending"])
                    else:
                        life.event(self.event(action), i * 0.1)
                life.finish(0.5)
                self.assertEqual(life.classification(self.cid), {
                    "observed_restart_count": 1, "classified_rollout_exits": 1, "lifecycle_pending": False,
                    "lifecycle_pending_since_monotonic": None})

    def test_missing_events_have_one_fixed_deadline_and_late_evidence_cannot_pass(self):
        life = self.lifecycle()
        life.observe(copy.deepcopy(self.new), 0)
        life.observe(copy.deepcopy(self.new), 1.9)
        self.assertEqual(life.pending[self.cid], 0)
        with self.assertRaisesRegex(RuntimeError, "fixed join deadline"):
            life.event(self.event("die"), resources.LIFECYCLE_JOIN_SECONDS)
        self.assertFalse(life.consumed)
        for first in ("die", "start"):
            life = self.lifecycle()
            life.event(self.event(first), 0)
            with self.assertRaisesRegex(RuntimeError, "fixed join deadline"):
                life.observe(copy.deepcopy(self.new), resources.LIFECYCLE_JOIN_SECONDS)

    def test_missing_claim_reset_extra_count_and_extra_generation_fail(self):
        with self.assertRaisesRegex(RuntimeError, "undeclared"):
            self.lifecycle(False).observe(copy.deepcopy(self.new), 0)
        for count in (0, 2):
            life = self.lifecycle()
            life.observe(copy.deepcopy(self.new), 0)
            row = copy.deepcopy(self.new)
            row["RestartCount"] = count
            with self.assertRaisesRegex(RuntimeError, "extra or reset"):
                life.observe(row, 0.1)
        life = self.lifecycle()
        self.complete(life)
        extra = copy.deepcopy(self.new)
        extra["State"]["Pid"] += 1
        with self.assertRaisesRegex(RuntimeError, "extra activation process"):
            life.observe(extra, 0.3)

    def test_provenance_and_append_only_intents_are_required(self):
        edits = [{"container_id": self.cid[:12]}, {"project": "foreign"}, {"nodes": ["node3"]},
                 {"candidate_sha256": "c" * 64}, {"old_restart_count": 1}, {"old_pid": 999},
                 {"old_started_at": "different"}, {"kind": "any-exit"}, {"rollout_id": "other"},
                 {"claim_path": "/unrelated/activation-started.sobj"}, {"claim_sha256": ""},
                 {"time_ns": 99}]
        for edit in edits:
            with self.subTest(edit=edit):
                intent = dict(self.intent, **edit)
                with self.assertRaises(RuntimeError):
                    self.lifecycle(False).refresh([intent])
        life = self.lifecycle()
        for changed in ([], [dict(self.intent, time_ns=111)], [self.intent, self.intent]):
            with self.assertRaises(RuntimeError):
                life.refresh(copy.deepcopy(changed))

    def test_foreign_old_nonzero_oom_repeated_and_unclaimed_events_fail(self):
        changes = [lambda e: e["Actor"].update(ID=self.cid[:12]),
                   lambda e: e["Actor"]["Attributes"].update({"com.docker.compose.project": "foreign"}),
                   lambda e: e["Actor"]["Attributes"].update({"com.docker.compose.service": "node3"}),
                   lambda e: e["Actor"]["Attributes"].update(exitCode="137"),
                   lambda e: e.update(timeNano=109), lambda e: e.update(Action="oom")]
        for change in changes:
            event = self.event("die")
            change(event)
            with self.assertRaises(RuntimeError):
                self.lifecycle().event(event, 0)
        with self.assertRaises(RuntimeError):
            self.lifecycle(False).event(self.event("die"), 0)
        for action in ("die", "start"):
            life = self.lifecycle()
            life.event(self.event(action), 0)
            with self.assertRaises(RuntimeError):
                life.event(self.event(action), 0.1)

    def test_initial_replay_is_bounded_and_finish_needs_all_evidence(self):
        life = self.lifecycle(False)
        event = self.event("start")
        event["timeNano"] = 99
        life.event(event, 0)
        life.finish(0)
        for actions in ((), ("die",), ("die", "start")):
            life = self.lifecycle()
            for action in actions:
                life.event(self.event(action), 0)
            with self.assertRaises(RuntimeError):
                life.finish(0.1)
        for rows in (self.rows[:-1], self.rows + [self.rows[0]]):
            with self.assertRaises(RuntimeError):
                resources.ActivationLifecycle("owned", "a" * 64, rows, 100)

    def test_overlapping_claims_remain_independent_and_inspect_identity_is_exact(self):
        life = self.lifecycle()
        second = copy.deepcopy(self.intent)
        row3 = self.rows[2]
        second.update(nodes=["node3"], container_id=row3["Id"], old_pid=103,
                      old_started_at="initial-3", rollout_id="plan2",
                      claim_path="/data/rollout/staged/uploads/plan2-target3/activation-started.sobj")
        life.refresh([copy.deepcopy(self.intent), second])
        life.observe(copy.deepcopy(self.new), 0)
        new3 = copy.deepcopy(row3)
        new3["RestartCount"] = 1
        new3["State"].update(Pid=203, StartedAt="replacement-3")
        life.observe(new3, 0.1)
        for action in ("die", "start"):
            event = self.event(action)
            event["Actor"]["ID"] = row3["Id"]
            event["Actor"]["Attributes"]["com.docker.compose.service"] = "node3"
            life.event(event, 0.2)
        self.assertEqual(set(life.pending), {self.cid})
        for action in ("die", "start"):
            life.event(self.event(action), 0.3)
        life.finish(0.4)
        for kind in ("container", "project", "service"):
            row = copy.deepcopy(self.new)
            if kind == "container":
                row["Id"] = "f" * 64
            else:
                row["Config"]["Labels"]["com.docker.compose." + kind] = "foreign"
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                self.lifecycle().observe(row, 0)

    def test_memory_oom_and_cgroup_failures_still_apply_while_join_is_pending(self):
        values = {"cgroup": f"0::/docker-{self.cid}.scope\n", "memory.max": str(resources.NODE_LIMIT),
                  "memory.swap.max": "0", "memory.current": "1024", "memory.peak": "2048",
                  "memory.events": "max 0\noom 0\noom_kill 0"}
        with patch.object(Path, "read_text", lambda p: values[p.name]):
            life = self.lifecycle()
            self.assertTrue(resources.check_node(copy.deepcopy(self.new), life, 0)["running"])
            self.assertTrue(life.pending)
            for name, bad in [("memory.max", "1"), ("memory.swap.max", "1"),
                              ("memory.events", "max 1\noom 0\noom_kill 0"),
                              ("memory.events", "max 0\noom 1\noom_kill 1"),
                              ("cgroup", "0::/foreign.scope\n")]:
                before = values[name]
                values[name] = bad
                with self.assertRaises(RuntimeError):
                    resources.check_node(copy.deepcopy(self.new), self.lifecycle(), 0)
                values[name] = before
            row = copy.deepcopy(self.new)
            row["State"]["OOMKilled"] = True
            with self.assertRaisesRegex(RuntimeError, "OOM"):
                resources.check_node(row, self.lifecycle(), 0)
            with self.assertRaises(RuntimeError):
                resources.check_node(self.new)

    def test_producer_records_canonical_identity_and_actual_claim_without_docker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate, log, intents = root / "candidate", root / "plan.log", root / "intents.jsonl"
            candidate.write_bytes(b"sealed candidate")
            log.write_text("rollout plan1 saved;\n")
            row = self.rows[1]
            def docker(*args):
                if args[0] == "ps":
                    return self.cid[:12] + "\n"
                if args[0] == "inspect":
                    return json.dumps([row])
                self.assertEqual(args[:2], ("exec", self.cid))
                return self.intent["claim_sha256"] + "  " + self.intent["claim_path"] + "\n"
            with patch.object(resources, "docker", side_effect=docker), \
                    patch.dict(os.environ, SINOMO_TEST_RESOURCE_INTENTS=str(intents)):
                resources.expect_rollout_exit("owned", "node2", candidate, log, root)
                record = json.loads(intents.read_text())
                self.assertEqual(record["container_id"], self.cid)
                self.assertEqual(record["old_started_at"], "initial-2")
                self.assertEqual(record["candidate_sha256"], hashlib.sha256(candidate.read_bytes()).hexdigest())
                self.assertEqual(record["claim_path"], self.intent["claim_path"])
                log.write_text("rollout different saved;\n")
                with self.assertRaisesRegex(RuntimeError, "original plan"):
                    resources.expect_rollout_exit("owned", "node2", candidate, log, root)
                self.assertEqual(len(intents.read_text().splitlines()), 1)

    def test_producer_refuses_a_retired_generation_and_leaves_no_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate, log, intents = root / "candidate", root / "plan.log", root / "intents.jsonl"
            candidate.write_bytes(b"candidate")
            log.write_text("rollout plan1 saved;\n")
            observations = [json.dumps([self.rows[1]]), json.dumps([self.new])]
            def docker(*args):
                if args[0] == "ps":
                    return self.cid[:12]
                if args[0] == "inspect":
                    return observations.pop(0)
                return self.intent["claim_sha256"] + "  " + self.intent["claim_path"]
            with patch.object(resources, "docker", side_effect=docker), \
                    patch.dict(os.environ, SINOMO_TEST_RESOURCE_INTENTS=str(intents)):
                with self.assertRaises(RuntimeError):
                    resources.expect_rollout_exit("owned", "node2", candidate, log, root)
            self.assertFalse(intents.exists())

    def run_watcher(self, root, deliver=True, extra_count=False, final_missing=False,
                    stream_ends=False, reader_error=False):
        # All Docker subprocesses, process handles, signal handlers and signals are
        # fakes. Only the observer's file receipts and pure lifecycle logic run.
        now, handler, calls = [0.0], {}, [0]
        pending = queue.Queue()
        process = SimpleNamespace(ended=False)
        process.poll = lambda: 0 if process.ended else None
        process.terminate = lambda: setattr(process, "ended", True)
        process.wait = lambda **_: 0
        reader = SimpleNamespace(start=lambda: None, join=lambda **_: None, is_alive=lambda: False)
        candidate = root / "candidate"
        candidate.write_bytes(b"candidate")
        intent = dict(self.intent, candidate_sha256=hashlib.sha256(b"candidate").hexdigest())
        intents = root / "intents.jsonl"
        intents.write_text(json.dumps(intent) + "\n")
        def inspect(_):
            calls[0] += 1
            rows = copy.deepcopy(self.rows)
            if calls[0] > 1:
                rows[1] = copy.deepcopy(self.new)
                if extra_count:
                    rows[1]["RestartCount"] = 2
                if calls[0] == 2 and deliver:
                    pending.put(self.event("die"))
                    pending.put(self.event("start"))
            if process.ended and final_missing:
                rows.pop()
            return rows
        class Delivery:
            def get(self, timeout):
                now[0] += 0.2
                if stream_ends:
                    process.ended = True
                if reader_error:
                    return {"reader_error": "invalid event JSON"}
                return pending.get_nowait()
            def get_nowait(self):
                event = pending.get_nowait()
                if event["Action"] == "start":
                    handler[resources.signal.SIGTERM]()
                return event
            def empty(self):
                return pending.empty()
        def checked(row, activation=None, now=None):
            if activation:
                activation.observe(row, now)
            return {"node": row["Config"]["Labels"]["com.docker.compose.service"], "running": True}
        with patch.object(resources, "inspect_nodes", side_effect=inspect), \
                patch.object(resources, "check_node", side_effect=checked), \
                patch.object(resources.subprocess, "Popen", return_value=process), \
                patch.object(resources.threading, "Thread", return_value=reader), \
                patch.object(resources.queue, "Queue", return_value=Delivery()), \
                patch.object(resources.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(resources.time, "time_ns", side_effect=lambda: 100 + int(now[0] * 10**9)), \
                patch.object(resources.signal, "signal", side_effect=lambda sig, fn: handler.update({sig: fn})), \
                patch.object(resources.signal, "pidfd_send_signal") as sent, \
                patch.object(resources.os, "pidfd_open", return_value=123), \
                patch.object(resources.os, "close"), \
                patch.dict(os.environ, SINOMO_TEST_RESOURCE_INTENTS=str(intents)):
            result = resources.watch("owned", root / "watch", 456, candidate)
            self.assertEqual(sent.call_count, int(result != 0))
        return result

    def test_watcher_retains_pending_raw_sample_then_reconciles_before_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(self.run_watcher(root), 0)
            samples = [json.loads(line) for line in (root / "watch/samples.jsonl").read_text().splitlines()]
            pending = [row for sample in samples for row in sample["nodes"] if row.get("lifecycle_pending")]
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["observed_restart_count"], 1)
            self.assertEqual(pending[0]["classified_rollout_exits"], 0)
            self.assertTrue((root / "watch/watch-complete.json").exists())
            self.assertEqual(len((root / "watch/events.jsonl").read_text().splitlines()), 2)

    def test_inspection_evidence_preserves_unmodified_inputs_and_omits_unrelated_config(self):
        row = copy.deepcopy(self.new)
        row["State"].update(Running=False, OOMKilled=True, Pid=0, ExitCode=137, Error="failure evidence")
        row["RestartCount"] = 3
        row["HostConfig"].update(Memory=123, MemorySwap=456, Privileged=True, Binds=["omit-mount"])
        row["Config"].update(Env=["omit-env"], Image="omit-image", Cmd=["omit-command"])
        row["Config"]["Labels"]["unrelated-label"] = "omit-label"
        row.update(Mounts=[{"Source": "omit-source"}], NetworkSettings={"unrelated": "omit-network"})
        before = copy.deepcopy(row)
        evidence = resources.inspection_evidence(row)
        self.assertEqual(evidence, {
            "Id": self.cid, "State": row["State"], "RestartCount": 3,
            "Config": {"Labels": {"com.docker.compose.project": "owned",
                                   "com.docker.compose.service": "node2"}},
            "HostConfig": {"Memory": 123, "MemorySwap": 456}})
        self.assertEqual(row, before)
        self.assertNotIn("omit-", json.dumps(evidence))
        missing = resources.inspection_evidence({"Id": self.cid, "Config": {"Labels": {}}})
        self.assertNotIn("State", missing)
        self.assertNotIn("RestartCount", missing)
        self.assertEqual(missing["HostConfig"], {})

    def test_failing_inspection_retains_counter_identity_and_state_without_env_or_mounts(self):
        self.new["Config"].update(Env=["UNRELATED_ENV_SENTINEL"], Image="UNRELATED_IMAGE_SENTINEL")
        self.new["HostConfig"]["Binds"] = ["UNRELATED_BIND_SENTINEL"]
        self.new["Mounts"] = [{"Source": "UNRELATED_MOUNT_SENTINEL"}]
        self.new["Config"]["Labels"]["unrelated"] = "UNRELATED_LABEL_SENTINEL"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(self.run_watcher(root, extra_count=True), 1)
            text = (root / "watch/inspections.jsonl").read_text()
            self.assertNotIn("UNRELATED_", text)
            failed = json.loads(text.splitlines()[-1])["nodes"][1]
            self.assertEqual(failed["Id"], self.cid)
            self.assertEqual(failed["RestartCount"], 2)
            self.assertEqual(failed["State"], self.new["State"])
            self.assertEqual(failed["HostConfig"], {"Memory": resources.NODE_LIMIT,
                                                 "MemorySwap": resources.NODE_LIMIT})
            self.assertEqual(failed["Config"]["Labels"], {"com.docker.compose.project": "owned",
                                                         "com.docker.compose.service": "node2"})
            self.assertTrue((root / "watch/HARD-FAILURE.json").exists())
            self.assertFalse((root / "watch/watch-complete.json").exists())

    def test_watcher_missing_event_extra_count_and_final_coverage_fail_with_raw_receipts(self):
        for arguments in ({"deliver": False}, {"extra_count": True}, {"final_missing": True},
                          {"stream_ends": True}, {"reader_error": True}):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.assertEqual(self.run_watcher(root, **arguments), 1)
                self.assertTrue((root / "watch/HARD-FAILURE.json").exists())
                self.assertFalse((root / "watch/watch-complete.json").exists())
                raw = [json.loads(line) for line in (root / "watch/inspections.jsonl").read_text().splitlines()]
                self.assertGreaterEqual(len(raw), 1 if arguments.get("stream_ends") or arguments.get("reader_error") else 2)
                if arguments.get("extra_count"):
                    self.assertEqual(raw[-1]["nodes"][1]["RestartCount"], 2)


if __name__ == "__main__":
    unittest.main()
