#!/usr/bin/env python3
import copy
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
