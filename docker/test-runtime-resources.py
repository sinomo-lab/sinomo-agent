#!/usr/bin/env python3
import copy
from pathlib import Path
import tempfile
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
