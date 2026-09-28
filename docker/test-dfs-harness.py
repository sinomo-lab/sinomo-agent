#!/usr/bin/env python3
"""Regression checks for the acceptance driver's failure and evidence boundaries."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("acceptance", Path(__file__).with_name("test-file-history.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
latency_spec = importlib.util.spec_from_file_location(
    "write_latency", Path(__file__).with_name("measure-dfs-write-latency.py"))
latency_driver = importlib.util.module_from_spec(latency_spec)
latency_spec.loader.exec_module(latency_driver)


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

    def test_convergence_probe_receives_the_remaining_command_budget(self):
        script = Path(__file__).with_name("test-cluster.sh").read_text()
        function = script.split("wait_for_dfs_converged() {", 1)[1].split("\n}\n", 1)[0]
        # Execute the real function with the clock and transports stubbed. The
        # probe must receive the bounded budget, not a shorter fixed deadline.
        prelude = r'''log() { :; }
command() { return 0; }
date() { printf '100\n'; }
deadline_command_timeout() { printf '7\n'; }
dfs_converged_epoch_signature() { printf 'stable\n'; }
jq() { return 0; }
sleep() { :; }
compose_exec_timeout() {
  if [[ "$*" == *cluster.dfs.invariants* ]]; then
    [[ "$*" == *timeout-seconds=7* ]] || exit 42
  fi
}
DFS_CONVERGENCE_WAIT=360
READINESS_COMMAND_TIMEOUT=10
POLL_INTERVAL=1
NODES=node1
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(["bash", "-c", prelude +
                '\nARTIFACTS="$1"\nwait_for_dfs_converged() {' + function +
                '\n}\nwait_for_dfs_converged', "test", directory], capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_staggered_restart_starts_bridge_before_readiness(self):
        runner = self.runner()
        runner.compose = ["docker", "compose"]
        runner.args = SimpleNamespace(restart_stagger_seconds=4)
        runner.run = Mock()
        runner.wait_ready = Mock()
        runner.verify_processes = Mock()
        runner.restart()
        calls = runner.run.call_args_list
        self.assertEqual([call.args[0] for call in calls], [
            "abrupt-cluster-stop", "restart-early-peers", "restart-bridge-delay", "restart-bridge"])
        self.assertEqual(calls[0].args[1][-6:], [f"node{i}" for i in range(1, 7)])
        self.assertNotIn("node4", calls[1].args[1])
        self.assertIn("--no-deps", calls[1].args[1])
        self.assertEqual(calls[2].args[1], ["sleep", "4"])
        self.assertEqual(calls[3].args[1][-1], "node4")
        runner.wait_ready.assert_called_once()
        runner.verify_processes.assert_called_once_with("restarted")

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

    def latency_report(self, samples, scenario="w1-n2"):
        policy = latency_driver.POLICIES[scenario]
        base = "/cluster-tests/runs/test/write-latency"
        for sample in samples:
            sample.update(placement_verified=True, placement_policy=policy.copy(),
                          placement_source_path=base)
        return {"status": "passed", "diagnostics": {
            "base": base, "replication_scenario": scenario,
            "requested_placement_policy": policy.copy(), "wait_for_commit": True,
            "correctness_passed": True, "samples": samples}}

    def test_write_matrix_runs_both_policies_for_every_origin_and_shard(self):
        self.check_write_matrix(False)

    def test_write_matrix_reuses_verified_fixtures_without_changing_the_probe(self):
        self.check_write_matrix(True)

    def check_write_matrix(self, reuse):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "sinomo"
            binary.write_bytes(b"test binary")
            artifacts = root / "artifacts"
            argv = ["measure-dfs-write-latency.py", "--project", "fixture", "--binary", str(binary),
                    "--artifacts", str(artifacts)]
            if reuse:
                fixture_groups = []
                for scenario, origin, shard in latency_driver.product(
                        latency_driver.POLICIES, latency_driver.PEERS, latency_driver.BASES):
                    base = f"{latency_driver.BASES[shard]}/runs/{scenario}-{origin}/write-latency"
                    sample = {"succeeded": True, "error": None, "placement_verified": True,
                              "placement_policy": latency_driver.POLICIES[scenario],
                              "placement_source_path": base}
                    fixture_groups.append({"replication_scenario": scenario, "origin": origin,
                                           "shard": shard, "base": base,
                                           "probe_diagnostics": {"base": base, "correctness_passed": True,
                                                                 "samples": [sample]}})
                fixture_path = root / "fixtures.json"
                fixture_path.write_text(json.dumps({"project": "fixture", "fixtures_retained": True,
                                                    "groups": fixture_groups}))
                argv += ["--fixtures", str(fixture_path)]
            observed = []
            def command(args, **kwargs):
                if any(str(arg).endswith("profile-dfs-writes.py") for arg in args):
                    output = Path(args[args.index("--artifacts") + 1])
                    output.mkdir()
                    (output / "report.json").write_text('{"complete": true}')
                    (output / "REPORT.md").write_text("Generated stage report\n")
                    return subprocess.CompletedProcess(args, 0, b"", b"")
                if "cluster.dfs.write-latency" not in args:
                    return subprocess.CompletedProcess(args, 0, b"", b"")
                params = dict(arg.split("=", 1) for arg in args if "=" in arg)
                scenario = params["replication"]
                self.assertEqual("prepared-base" in params, reuse)
                self.assertEqual("base" in params, not reuse)
                base = params["prepared-base" if reuse else "base"]
                prefix = next(value for value in latency_driver.BASES.values()
                              if base == value or base.startswith(value + "/"))
                observed.append((scenario, args[args.index("-T") + 1], prefix))
                samples = [{"operation": operation, "index": index, "measured": index >= 12,
                            "succeeded": True, "error": None, "elapsed_us": 1000,
                            "path": f"/fixture/{index}", "payload_seed": str(index)}
                           for index in range(112) for operation in ("create", "overwrite")]
                report = self.latency_report(samples, scenario)
                if reuse:
                    report["diagnostics"]["base"] = base
                    for sample in samples:
                        sample["placement_source_path"] = base
                return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"")
            with patch("sys.argv", argv), \
                 patch.object(latency_driver.subprocess, "check_output",
                              side_effect=lambda args, **kwargs: "source\n" if kwargs.get("text") else b""), \
                 patch.object(latency_driver.subprocess, "run", side_effect=command), \
                 patch.object(latency_driver.acceptance.Acceptance, "verify_processes"), \
                 patch.object(latency_driver.acceptance.Acceptance, "verify_observe"), \
                 patch.object(latency_driver.acceptance.Acceptance, "wait_ready"), \
                 patch.object(latency_driver.acceptance.Acceptance, "probe"):
                latency_driver.main()
            result = json.loads((artifacts / "write-latency.json").read_text())
            self.assertTrue(result["passed"])
            self.assertTrue(result["acceptance_scope"])
            self.assertTrue(result["stage_profile_passed"])
            if reuse:
                self.assertTrue(result["fixture_reuse"])
                self.assertEqual((artifacts / "input-fixtures.json").read_bytes(), fixture_path.read_bytes())
            self.assertEqual(set(observed), set(latency_driver.product(
                latency_driver.POLICIES, latency_driver.PEERS, latency_driver.BASES.values())))
            self.assertEqual(len(observed), 12)
            self.assertEqual(sum(len(group["summary"]) for group in result["groups"]), 24)
            generated = json.loads((artifacts / "report.json").read_text())
            self.assertTrue(generated["passed"])
            self.assertEqual(len(generated["acceptance_rows"]), 24)
            self.assertTrue(all(row["policy_verified"] for row in generated["acceptance_rows"]))
            self.assertTrue(all(row["p95_ms"] == 1 for row in generated["acceptance_rows"]))
            self.assertIsNone(generated["node_window_measurements"])
            before = (artifacts / "REPORT.md").read_bytes()
            latency_driver.acceptance.verification_report.write_report(artifacts)
            self.assertEqual((artifacts / "REPORT.md").read_bytes(), before)
            for group in result["groups"]:
                self.assertEqual(group["min_commit_replicas"],
                                 latency_driver.POLICIES[group["replication_scenario"]]["min_commit_replicas"])

    def test_reused_fixtures_reject_missing_policy_evidence_and_foreign_scope(self):
        base = "/cluster-tests/runs/fixture/write-latency"
        sample = {"succeeded": True, "error": None, "placement_verified": True,
                  "placement_policy": latency_driver.POLICIES["w1-n2"], "placement_source_path": base}
        valid = {"project": "fixture", "fixtures_retained": True, "passed": False,
                 "groups": [{"replication_scenario": "w1-n2", "origin": "node1", "shard": "root",
                             "base": base, "probe_diagnostics": {"base": base,
                             "correctness_passed": True, "samples": [sample]}}]}
        selected = [("w1-n2", "node1", "root")]
        self.assertEqual(len(latency_driver.verified_fixtures(valid, "fixture", selected)), 1)
        for change in ("project", "retained", "policy", "source", "duplicate", "traversal"):
            bad = json.loads(json.dumps(valid))
            if change == "project": bad["project"] = "another-project"
            if change == "retained": bad["fixtures_retained"] = False
            if change == "policy": bad["groups"][0]["probe_diagnostics"]["samples"][0]["placement_policy"] = latency_driver.POLICIES["w2-n3"]
            if change == "source": bad["groups"][0]["probe_diagnostics"]["samples"][0]["placement_source_path"] = "/elsewhere"
            if change == "duplicate": bad["groups"].append(bad["groups"][0])
            if change == "traversal": bad["groups"][0]["base"] = "/cluster-tests/../elsewhere"
            with self.subTest(change=change), self.assertRaises(ValueError):
                latency_driver.verified_fixtures(bad, "fixture", selected)
        with self.assertRaises(ValueError):
            latency_driver.verified_fixtures(valid, "fixture", [("w2-n3", "node1", "root")])

    def test_write_policy_evidence_is_required_and_must_match_each_sample(self):
        for scenario in latency_driver.POLICIES:
            samples = [{"operation": operation, "index": 0, "measured": True,
                        "succeeded": True, "error": None, "elapsed_us": 1000}
                       for operation in ("create", "overwrite")]
            report = self.latency_report(samples, scenario)
            self.assertEqual(len(latency_driver.validate_report(report, 1, 0, 0, scenario)), 2)
            samples[-1]["placement_policy"]["min_commit_replicas"] = 99
            with self.assertRaisesRegex(RuntimeError, "mismatched placement"):
                latency_driver.validate_report(report, 1, 0, 0, scenario)
            report = self.latency_report(samples, scenario)
            del report["diagnostics"]["requested_placement_policy"]
            with self.assertRaisesRegex(RuntimeError, "requested replication policy"):
                latency_driver.validate_report(report, 1, 0, 0, scenario)

    def test_committed_write_report_rejects_censored_and_failed_samples(self):
        samples = [{"operation": operation, "index": index, "measured": index >= 1,
                    "succeeded": True, "error": None, "elapsed_us": 20_000}
                   for index in range(21) for operation in ("create", "overwrite")]
        report = self.latency_report(samples)
        self.assertEqual(len(latency_driver.validate_report(report, 20, 1, 100_000, "w1-n2")), 2)
        samples[-1]["succeeded"] = False
        with self.assertRaisesRegex(RuntimeError, "failed operation"):
            latency_driver.validate_report(report, 20, 1, 100_000, "w1-n2")
        samples[-1]["succeeded"] = True
        samples.pop()
        with self.assertRaisesRegex(RuntimeError, "omitted operations"):
            latency_driver.validate_report(report, 20, 1, 100_000, "w1-n2")

    def test_committed_write_gate_keeps_operations_separate_and_limit_exclusive(self):
        samples = [{"operation": operation, "index": index, "measured": True,
                    "succeeded": True, "error": None,
                    "elapsed_us": 100_000 if operation == "overwrite" and index >= 18 else 1000}
                   for index in range(20) for operation in ("create", "overwrite")]
        report = self.latency_report(samples)
        with self.assertRaisesRegex(RuntimeError, "overwrite p95 100000us"):
            latency_driver.validate_report(report, 20, 0, 100_000, "w1-n2")
        self.assertEqual(len(latency_driver.validate_report(report, 20, 0, 0, "w1-n2")), 2)

    def test_failed_write_command_keeps_aggregate_evidence_and_failure(self):
        samples = [{"operation": "create", "index": 0, "measured": True,
                    "succeeded": True, "error": None, "elapsed_us": 101_000}]
        report = {"status": "failed", "summary": "create p95 is not below 100000us",
                  "diagnostics": {"base": "/cluster-tests/runs/failed",
                                  "samples": samples, "operation_latency_us": {
                                      "create": {"count": 1, "p95_us": 101_000}}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "sinomo"
            binary.write_bytes(b"test binary")
            artifacts = root / "artifacts"
            argv = ["measure-dfs-write-latency.py", "--project", "fixture", "--binary", str(binary),
                    "--artifacts", str(artifacts), "--origins", "node1", "--shards", "root",
                    "--skip-readiness"]
            def command(args, **kwargs):
                if "cluster.dfs.write-latency" in args:
                    return subprocess.CompletedProcess(args, 7, json.dumps(report).encode(), b"gate failed")
                return subprocess.CompletedProcess(args, 0, b"", b"")
            with patch("sys.argv", argv), \
                 patch.object(latency_driver.subprocess, "check_output",
                              side_effect=lambda args, **kwargs: "source\n" if kwargs.get("text") else b""), \
                 patch.object(latency_driver.subprocess, "run", side_effect=command), \
                 patch.object(latency_driver.acceptance.Acceptance, "verify_processes"), \
                 patch.object(latency_driver.acceptance.Acceptance, "verify_observe"), \
                 patch.object(latency_driver.acceptance.Acceptance, "probe"):
                with self.assertRaisesRegex(RuntimeError, "exit 7"):
                    latency_driver.main()
            result = json.loads((artifacts / "write-latency.json").read_text())
            self.assertFalse(result["passed"])
            self.assertEqual(result["parameters"]["max_p95_us"], 100_000)
            group = result["groups"][0]
            self.assertFalse(group["passed"])
            self.assertEqual(group["probe_status"], "failed")
            self.assertEqual(group["probe_summary"], report["summary"])
            self.assertEqual(group["samples"], samples)
            self.assertEqual(group["probe_diagnostics"], report["diagnostics"])
            self.assertIn("exit 7", group["error"])
            generated = json.loads((artifacts / "report.json").read_text())
            self.assertFalse(generated["passed"])
            self.assertEqual(generated["acceptance_rows"][0]["p95_ms"], 101)
            self.assertEqual(generated["acceptance_rows"][0]["status"], "failed")
            self.assertIsNone(generated["acceptance_rows"][1]["p95_ms"])
            self.assertEqual(json.loads((artifacts / "003-w1-n2-node1-root-write-latency.stdout").read_bytes()), report)
            self.assertEqual(json.loads((artifacts / "003-w1-n2-node1-root-write-latency.timing.json").read_text())["returncode"], 7)

    def test_latency_driver_refuses_to_overwrite_an_existing_result(self):
        with tempfile.TemporaryDirectory() as directory:
            result_path = Path(directory) / "write-latency.json"
            original = b'{"passed": false, "error": "saved failure"}\n'
            result_path.write_bytes(original)
            argv = ["measure-dfs-write-latency.py", "--project", "fixture", "--binary", "/unused",
                    "--artifacts", directory]
            with patch("sys.argv", argv), patch("sys.stderr"), \
                 patch.object(latency_driver.acceptance, "Acceptance") as runner:
                with self.assertRaises(SystemExit) as error:
                    latency_driver.main()
                self.assertEqual(error.exception.code, 2)
                runner.assert_not_called()
            self.assertEqual(result_path.read_bytes(), original)

    def test_readiness_failure_generates_every_unrun_distribution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = {"schema": "sinomo.dfs-write-latency.v2", "passed": False,
                      "error": "quiet gate failed", "groups": [], "acceptance_scope": True,
                      "parameters": {"policies": list(latency_driver.POLICIES),
                                     "origins": list(latency_driver.PEERS),
                                     "shards": list(latency_driver.BASES), "iterations": 100,
                                     "warmup": 12, "payload_bytes": 4096, "max_p95_us": 100_000}}
            (root / "write-latency.json").write_text(json.dumps(result))
            report = driver.verification_report.write_report(root)
            self.assertFalse(report["passed"])
            self.assertEqual(report["coverage"]["expected_distributions"], 24)
            self.assertEqual(report["coverage"]["observed_distributions"], 0)
            self.assertTrue(all(row["status"] == "not-run" and row["p95_ms"] is None
                                for row in report["acceptance_rows"]))
            self.assertIn("quiet gate failed", (root / "REPORT.md").read_text())

    def test_docker_exit_trap_generates_report_and_preserves_failure_exit(self):
        script = Path(__file__).with_name("test-cluster.sh").read_text()
        function = "teardown() {" + script.split("\nteardown() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        for exit_code in (0, 7):
            with tempfile.TemporaryDirectory() as directory:
                env = dict(os.environ, ARTIFACTS=directory, PROJECT="unit-report", BINARY="/unavailable",
                           SINOMO_DOCKER_DIR=str(Path(__file__).resolve().parent), FAILED_SCENARIOS="",
                           FORMATION_ONLY="0", DFS_READY_ONLY="1", DFS_ONLY="0", NETWORK_CASES="",
                           CLUSTER_KV_ONLY="0", PJDFSTEST_ONLY="0")
                command = ('teardown_cluster() { return 1; }\nlog() { :; }\n' + function
                           + f'prior_status() {{ return {exit_code}; }}\nprior_status\nteardown')
                completed = subprocess.run(["bash", "-c", command], env=env, capture_output=True)
                self.assertEqual(completed.returncode, exit_code, completed.stderr.decode())
                report = json.loads((Path(directory) / "report.json").read_text())
                self.assertEqual(report["passed"], exit_code == 0)
                self.assertTrue((Path(directory) / "REPORT.md").is_file())

    def test_verification_report_reuses_native_metrics_without_retiming(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "verification.json").write_text(json.dumps({"passed": False, "lane": "network"}))
            metrics = {"latency_p95_us": {"value": 321, "unit": "microseconds"}}
            (root / "probe-001-node1-network.json").write_text(json.dumps({
                "case": {"id": "network.example"}, "status": "passed", "duration_ms": 4000,
                "summary": "native result", "metrics": metrics}))
            (root / "probe-002-node2-network.json").write_text('{"partial":')
            report = driver.verification_report.write_report(root)
            self.assertEqual(report["native_probe_results"][0]["metrics"], metrics)
            self.assertEqual(report["native_probe_results"][0]["duration_ms"], 4000)
            self.assertEqual(report["native_probe_results"][1]["status"], "invalid / incomplete")
            self.assertFalse(report["passed"])

    def test_observe_configuration_rejects_unprofiled_or_untraced_nodes(self):
        for supported, active_filter, debug in [(False, "debug,sinomo.perf=trace", b"1"),
                                                 (True, "info", b"1"),
                                                 (True, "debug,sinomo.perf=trace", b"0")]:
            with tempfile.TemporaryDirectory() as directory:
                runner = self.runner()
                runner.artifacts = Path(directory)
                runner.compose = ["docker", "compose"]
                runner.run = Mock(return_value=debug)
                runner.cli = Mock(side_effect=[json.dumps(item).encode() for item in (
                    {"active_filter": active_filter}, {"supported": supported}, {"capacity": 32768})])
                with self.assertRaisesRegex(RuntimeError, "require DFS debug"):
                    runner.verify_observe()

    def test_formation_capture_handles_retained_pid_markers_once_per_generation(self):
        source = Path(__file__).with_name("entrypoint.sh").read_text()
        function = "start_formation_observation() {" + source.split("start_formation_observation() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, DATA=directory, NODE_NAME="node-test", FORMATION_OBSERVE="1")
            script = ('log() { :; }\nrun_formation_observation() { printf "%s\\n" "$2" >> "$DATA/scheduled"; }\n'
                      + function + '\nmkdir -p "$DATA/observe/formation"\n'
                      'printf "%s\\n" "$$" > "$DATA/observe/formation/current-daemon-pid"\n'
                      'start_formation_observation "$$"\nstart_formation_observation "$$"\nwait\n')
            completed = subprocess.run(["bash", "-c", script], env=env, capture_output=True)
            self.assertEqual(completed.returncode, 0, completed.stderr.decode())
            self.assertEqual(len((Path(directory) / "scheduled").read_text().splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
