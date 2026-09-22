#!/usr/bin/env python3
"""Regression checks for the acceptance driver's failure and evidence boundaries."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("acceptance", Path(__file__).with_name("test-file-history.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class HarnessTests(unittest.TestCase):
    def runner(self):
        runner = driver.Acceptance.__new__(driver.Acceptance)
        runner.index = 0
        runner.completed = []
        runner.samples = []
        return runner

    def test_convergence_requires_authoritative_handoff_evidence(self):
        script = Path(__file__).with_name("test-cluster.sh").read_text()
        function = script.split("dfs_converged_epoch_signature() {", 1)[1].split("\n}\n", 1)[0]
        function = "dfs_converged_epoch_signature() {" + function + "\n}\n"
        probe = {"status": "passed", "diagnostics": {"shards": [
            {"lane": lane, "shard_id": lane, "file_system_id": "fs", "epoch": 3,
             "primary_node_id": lane, "replica_node_ids": list("abcde")}
            for lane in ["root", "services", "users"]]}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.json"
            for pending, expected in [(None, False), (["pending"], False), ([], True)]:
                if pending is not None:
                    probe["diagnostics"]["pending_handoff_keys"] = pending
                path.write_text(json.dumps(probe))
                result = subprocess.run(["bash", "-c", function +
                    'dfs_converged_epoch_signature "$1"', "test", str(path)], capture_output=True)
                self.assertEqual(result.returncode == 0, expected, result.stderr.decode())

    def test_bulk_requester_rejects_a_promoted_primary(self):
        script = Path(__file__).with_name("test-cluster.sh").read_text()
        function = script.split("bulk_requester_is_nonprimary() {", 1)[1].split("\n}\n", 1)[0]
        function = "bulk_requester_is_nonprimary() {" + function + "\n}\n"
        probe = {"status": "passed", "diagnostics": {"node": "node4", "shards": [
            {"primary_node_id": node} for node in ["node1", "node2", "node3"]]}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.json"
            for node, expected in [("node4", True), ("node2", False), (None, False)]:
                probe["diagnostics"]["node"] = node
                path.write_text(json.dumps(probe))
                result = subprocess.run(["bash", "-c", function +
                    'bulk_requester_is_nonprimary "$1"', "test", str(path)], capture_output=True)
                self.assertEqual(result.returncode == 0, expected, result.stderr.decode())

    def test_skipped_probe_is_not_acceptance(self):
        runner = self.runner()
        for status in ["skipped", "failed", None]:
            runner.cli = Mock(return_value=json.dumps({"status": status}).encode())
            with self.assertRaisesRegex(RuntimeError, "did not pass"):
                runner.probe("node5", "write")

    def test_cursor_cycle_fails_instead_of_looping(self):
        runner = self.runner()
        runner.history = Mock(side_effect=[
            {"entries": [], "next_cursor": cursor} for cursor in ["a", "b", "a"]])
        with self.assertRaisesRegex(AssertionError, "cursor repeated"):
            runner.listing("node1", "/fixture")

    def test_timeout_preserves_partial_output(self):
        runner = self.runner()
        with tempfile.TemporaryDirectory() as directory:
            runner.root = runner.artifacts = Path(directory)
            runner.env = {}
            error = subprocess.TimeoutExpired(["probe"], 1, output=b"partial", stderr=b"diagnostic")
            with patch.object(driver.subprocess, "run", side_effect=error):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.run("timeout", ["probe"])
            self.assertEqual((runner.artifacts / "001-timeout.stdout").read_bytes(), b"partial")
            self.assertEqual((runner.artifacts / "001-timeout.stderr").read_bytes(), b"diagnostic")
            self.assertTrue(json.loads((runner.artifacts / "001-timeout.timing.json").read_text())["timed_out"])
            self.assertEqual(runner.completed, [])

    def test_pagination_rejects_duplicate_versions(self):
        runner = self.runner()
        entry = {"identity": {"object_id": "object", "version": "v1", "file_system_id": "fs"}}
        runner.history = Mock(side_effect=[
            {"entries": [entry], "next_cursor": "next"},
            {"entries": [entry], "next_cursor": None}])
        with self.assertRaisesRegex(AssertionError, "duplicated a version"):
            runner.listing("node1", "/fixture")

    def test_warmup_and_failed_operations_are_not_samples(self):
        runner = self.runner()
        runner.measured("ordinary", "node1", "write", False, lambda: None)
        with self.assertRaisesRegex(RuntimeError, "failed"):
            runner.measured("ordinary", "node1", "write", True,
                            Mock(side_effect=RuntimeError("failed")))
        self.assertEqual(runner.samples, [])
        runner.measured("ordinary", "node1", "write", True, lambda: None)
        self.assertEqual(len(runner.samples), 1)

    def test_restart_readiness_does_not_reenter_dfs_suite(self):
        runner = self.runner()
        runner.env = {"SINOMO_TEST_DFS_ONLY": "1"}
        runner.args = SimpleNamespace(project="fixture")
        runner.binary = Path("/binary")
        runner.artifacts = Path("/artifacts")
        def readiness(*args, **kwargs):
            self.assertEqual(runner.env["SINOMO_TEST_DFS_ONLY"], "0")
            self.assertEqual(runner.env["SINOMO_TEST_DFS_READY_ONLY"], "1")
            self.assertEqual(runner.env["SINOMO_TEST_REUSE"], "1")
            raise RuntimeError("gate failed")
        runner.run = readiness
        with self.assertRaisesRegex(RuntimeError, "gate failed"):
            runner.wait_ready("restart")
        self.assertEqual(runner.env, {"SINOMO_TEST_DFS_ONLY": "1"})

    def test_percentiles_keep_profiles_and_directions_separate(self):
        samples = [{"profile": "ordinary", "origin": "node1", "operation": "write", "elapsed_ms": value}
                   for value in range(1, 21)]
        samples += [{"profile": "retention", "origin": "node5", "operation": "write", "elapsed_ms": 1000}]
        summary = driver.summarize_samples(samples)
        self.assertEqual(len(summary), 2)
        self.assertEqual((summary[0]["p50_ms"], summary[0]["p95_ms"], summary[0]["max_ms"]), (10, 19, 20))
        self.assertEqual(summary[1]["count"], 1)
        self.assertEqual(driver.summarize_samples([]), [])


if __name__ == "__main__":
    unittest.main()
