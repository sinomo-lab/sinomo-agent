#!/usr/bin/env python3
"""Regression checks for the acceptance driver's failure and evidence boundaries."""
import importlib.util
import json
import os
from pathlib import Path
import shlex
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
idle_spec = importlib.util.spec_from_file_location("idle_profile", Path(__file__).with_name("profile-idle.py"))
idle_driver = importlib.util.module_from_spec(idle_spec)
idle_spec.loader.exec_module(idle_driver)
stage_spec = importlib.util.spec_from_file_location("write_stage_profile", Path(__file__).with_name("profile-dfs-writes.py"))
stage_driver = importlib.util.module_from_spec(stage_spec)
stage_spec.loader.exec_module(stage_driver)


class HarnessTests(unittest.TestCase):
    def test_idle_process_sampler_preserves_roles_with_a_distinct_shadow_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'uptime').write_text('10.00 20.00\n')

            def process(pid, name, argv, started):
                proc = root / str(pid)
                proc.mkdir(exist_ok=True)
                (proc / 'comm').write_text(name + '\n')
                (proc / 'cmdline').write_bytes(b'\0'.join(a.encode() for a in argv) + b'\0')
                fields = ['S'] + ['0'] * 49
                fields[11] = '100'
                fields[12] = '20'
                fields[19] = str(started)
                (proc / 'stat').write_text(f'{pid} ({name}) ' + ' '.join(fields) + '\n')
                (proc / 'io').write_text('read_bytes: 10\nwrite_bytes: 20\nsyscr: 2\nsyscw: 3\n')

            process(28, 'sinomo', ['/data/bin/sinomo', 'daemon', 'run'], 202)
            # The observer carries both daemon role strings in its script.
            # It must not become a daemon merely because those strings match.
            observer = ['sh', '-c', 'case daemon shadow run and daemon run']
            process(99, 'sh', observer, 303)
            program = idle_driver.PROCESS_SAMPLE.replace('/proc/', shlex.quote(str(root)) + '/')

            def sample():
                result = subprocess.run(['sh', '-c', program], check=True, text=True,
                                        capture_output=True, timeout=5)
                return idle_driver.parse_process_sample(result.stdout)

            for name in ['sinomo', 'sinomo-shadow']:
                process(1, name, [f'/data/bin/{name}', 'daemon', 'shadow', 'run'], 101)
                value = sample()
                self.assertEqual(set(value['processes']), {'main', 'shadow'})
                self.assertEqual(value['processes']['main']['pid'], 28)
                self.assertEqual(value['processes']['main']['start_ticks'], 202)
                self.assertEqual(value['processes']['shadow']['pid'], 1)
                self.assertEqual(value['processes']['shadow']['start_ticks'], 101)

            process(99, 'sinomo-shadow', ['/data/bin/sinomo-shadow', 'daemon', 'shadow', 'run'], 303)
            with self.assertRaisesRegex(ValueError, 'duplicate shadow process'):
                sample()
            process(99, 'sh', observer, 303)
            process(1, 'unrelated', ['/data/bin/sinomo-shadow', 'daemon', 'shadow', 'run'], 101)
            with self.assertRaisesRegex(ValueError, 'expected exactly one main and one shadow'):
                sample()

    def test_idle_process_deltas_reject_restarts_and_counter_resets(self):
        def snapshot(at, pid, ticks, read):
            return {'monotonic_seconds': at, 'ticks_per_second': 100, 'processes': {
                role: {'pid': pid, 'start_ticks': 5, 'cpu_ticks': ticks,
                       'io': {'read_bytes': read, 'write_bytes': 10, 'syscr': 2, 'syscw': 1}}
                for role in ['main', 'shadow']}}
        before = snapshot(10, 20, 100, 1000)
        rows = idle_driver.process_deltas(before, snapshot(20, 20, 150, 1200))
        self.assertEqual(rows[0]['cpu_percent'], 5)
        self.assertEqual(rows[0]['read_bytes'], 200)
        self.assertTrue(all(r['cpu_percent'] is None for r in
                            idle_driver.process_deltas(before, snapshot(20, 21, 150, 1200))))
        self.assertIsNone(idle_driver.process_deltas(before, snapshot(20, 20, 90, 999))[0]['cpu_percent'])
        self.assertIsNone(idle_driver.process_deltas(before, snapshot(20, 20, 90, 999))[0]['read_bytes'])

    def test_quiet_report_preserves_gate_verdict_and_whole_container_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'network-quiet-idle.summary.txt').write_text(
                'label=idle\nstatus=fail\nreason=deadline\nmax_cpu_percent=10\n'
                'required_quiet_duration_seconds=30\nelapsed_seconds=120\n'
                'samples_file=network-quiet-idle.samples.tsv\n')
            (root / 'network-quiet-idle.samples.tsv').write_text(
                'node\tcpu_percent\twindow_state\nnode1\t99%\tstream-warmup\n'
                'node1\t2%\tquiet\nnode1\t20%\tbusy\nnode2\t1%\tquiet\n')
            gates = driver.verification_report.quietness_results(root)
            self.assertEqual(gates[0]['summary']['status'], 'fail')
            self.assertEqual(gates[0]['nodes'][0], {
                'node': 'node1', 'samples': 2, 'mean_cpu_percent': 11,
                'p95_cpu_percent': 20, 'max_cpu_percent': 20, 'over_limit_samples': 1,
                'network_gate_recorded': False, 'network_intervals': 0,
                'network_max_combined_bytes_per_second': None,
                'network_over_limit_intervals': None, 'network_rx_bytes': None,
                'network_tx_bytes': None, 'network_unavailable_intervals': 0})
            with (root / 'network-quiet-idle.summary.txt').open('a') as output:
                output.write('container_count=3\n')
            missing = driver.verification_report.quietness_results(root)[0]['nodes'][2]
            self.assertEqual(missing['node'], 'node3')
            self.assertEqual(missing['samples'], 0)
            self.assertIsNone(missing['max_cpu_percent'])

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
DFS_FRESH_TOPOLOGY_REQUESTED=1
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

    def test_core_only_route_withdrawal_rejects_other_failures(self):
        runner = self.runner()
        with tempfile.TemporaryDirectory() as directory:
            runner.root = runner.artifacts = Path(directory)
            runner.env = {}
            for stderr, accepted in [
                (b"unknown service route: /daemon/dfs/browser/file-history/list-v1", True),
                (b"unknown object rpc route: /daemon/dfs/browser/file-history/list-v1", True),
                (b"permission denied for /daemon/dfs/browser/file-history/list-v1", False),
                (b"unknown service route: /different/route", False),
                (b"request timed out", False),
            ]:
                result = subprocess.CompletedProcess(["sinomo"], 1, b"", stderr)
                with patch.object(driver.subprocess, "run", return_value=result):
                    errors = ("unknown service route: /daemon/dfs/browser/file-history/list-v1",
                              "unknown object rpc route: /daemon/dfs/browser/file-history/list-v1")
                    if accepted:
                        runner.run("feature-route", ["sinomo"], expected_success=False, expected_error=errors)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "missing expected error"):
                            runner.run("feature-route", ["sinomo"], expected_success=False, expected_error=errors)

    def test_feature_restoration_attempts_every_node_after_one_toggle_failure(self):
        runner = self.runner()
        def toggle(node, *args, **kwargs):
            if node == "node1":
                raise RuntimeError("unavailable")
            return b"enabled: true"
        runner.cli = Mock(side_effect=toggle)
        with self.assertRaisesRegex(RuntimeError, "enable incomplete"):
            runner.set_filesystem_features(True)
        self.assertEqual(runner.cli.call_count, 12)
        self.assertEqual({call.args[0] for call in runner.cli.call_args_list},
                         {f"node{i}" for i in range(1, 7)})

    def test_core_only_requires_remote_retry_probe_before_feature_withdrawal(self):
        runner = self.runner()
        runner.verify_processes = Mock()
        runner.probe = Mock(side_effect=RuntimeError("remote retry failed"))
        runner.set_filesystem_features = Mock()
        with self.assertRaisesRegex(RuntimeError, "remote retry failed"):
            runner.exercise_core_only()
        runner.probe.assert_called_once_with("node6", "semantic-lost-reply", timeout_seconds=120)
        runner.set_filesystem_features.assert_not_called()

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
        runner.env = {"SINOMO_TEST_DFS_ONLY": "1", "SINOMO_TEST_DFS_BASELINE_ONLY": "1"}
        runner.args = SimpleNamespace(project="fixture")
        runner.binary = Path("/binary")
        runner.artifacts = Path("/artifacts")
        def readiness(*args, **kwargs):
            self.assertEqual(runner.env["SINOMO_TEST_DFS_ONLY"], "0")
            self.assertEqual(runner.env["SINOMO_TEST_DFS_BASELINE_ONLY"], "0")
            self.assertEqual(runner.env["SINOMO_TEST_DFS_READY_ONLY"], "1")
            self.assertEqual(runner.env["SINOMO_TEST_REUSE"], "1")
            raise RuntimeError("gate failed")
        runner.run = readiness
        with self.assertRaisesRegex(RuntimeError, "gate failed"):
            runner.wait_ready("restart")
        self.assertEqual(runner.env, {"SINOMO_TEST_DFS_ONLY": "1", "SINOMO_TEST_DFS_BASELINE_ONLY": "1"})

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

    def test_stage_fixture_partition_preserves_failed_case_and_profiles_verified_matrix(self):
        selected = list(latency_driver.product(latency_driver.POLICIES, latency_driver.PEERS, latency_driver.BASES))
        fixtures = {"project": "fixture", "fixtures_retained": True, "binary_sha256": "fixture-binary", "groups": []}
        for policy, origin, shard in selected:
            base = f"{latency_driver.BASES[shard]}/runs/{policy}-{origin}-{shard}/write-latency"
            sample = {"succeeded": True, "error": None, "placement_verified": True,
                      "placement_policy": latency_driver.POLICIES[policy], "placement_source_path": base}
            fixtures["groups"].append({"replication_scenario": policy, "origin": origin, "shard": shard,
                                       "base": base, "probe_diagnostics": {"base": base,
                                       "correctness_passed": True, "samples": [sample]}})
        failed_key = ("w2-n3", "node1", "services")
        failed_group = next(group for group in fixtures["groups"]
                            if (group["replication_scenario"], group["origin"], group["shard"]) == failed_key)
        failed_group.update(probe_status="failed", probe_summary="placement snapshot changed before publication",
                            error="original setup failure", probe_diagnostics={"base": failed_group["base"]})
        with self.assertRaises(ValueError):
            latency_driver.verified_fixtures(fixtures, "fixture", selected)
        verified, skipped = latency_driver.partition_verified_fixtures(fixtures, "fixture", selected)
        self.assertEqual(set(verified), set(selected) - {failed_key})
        self.assertEqual(set(skipped), {failed_key})
        self.assertEqual(skipped[failed_key]["fixture_base"], failed_group["base"])
        self.assertEqual(skipped[failed_key]["fixture_probe_error"], "original setup failure")
        self.assertIn(failed_group["probe_summary"], skipped[failed_key]["error"])
        cases = []
        for policy, origin, shard in selected:
            key = (policy, origin, shard)
            case = {"name": f"{policy}-{origin}-{shard}", "policy": policy, "origin": origin,
                    "shard": shard, "complete": key in verified, "workload_verified": key in verified}
            if key in skipped:
                case.update(skipped[key], skipped=True)
            cases.append(case)
        result = {"binary_sha256": "running-binary", "source_revision": "source", "profile": "debug+cpu-profiler",
                  "project": "fixture", "cases": cases, "operations": [], "complete": False,
                  "fixture_coverage": {"selected": 12, "verified": 11, "skipped": 1}}
        with tempfile.TemporaryDirectory() as root:
            stage_driver.render_index(Path(root), result)
            artifact = json.loads((Path(root) / "report.json").read_text())
            self.assertFalse(artifact["complete"])
            self.assertEqual(len(artifact["cases"]), 12)
            report = (Path(root) / "REPORT.md").read_text()
            self.assertIn("**11/12**", report)
            self.assertIn("skipped: missing verified fixture", report)
            self.assertIn(failed_group["probe_summary"], report)
        with self.assertRaises(ValueError):
            latency_driver.partition_verified_fixtures(fixtures, "another-project", selected)
        duplicated = json.loads(json.dumps(fixtures))
        duplicated["groups"].append(duplicated["groups"][0])
        with self.assertRaises(ValueError):
            latency_driver.partition_verified_fixtures(duplicated, "fixture", selected)

    def test_stage_clients_use_container_mount_and_keep_preworkload_capture_failure(self):
        host_binary = "/host/checkout/target/candidate02/sinomo"
        clients = stage_driver.container_clients("fixture", {"node1": "node-id-1", "node5": "node-id-5"})
        self.assertEqual(clients["node-id-1"], ["docker", "exec", "fixture-node1-1", "/usr/local/bin/sinomo"])
        self.assertEqual(clients["node-id-5"][-1], "/usr/local/bin/sinomo")
        self.assertTrue(all(host_binary not in client for client in clients.values()))
        original = "node-id-1: control exited 127: container command not found"
        case = {"name": "w1-n2-node1-root", "capture_exit": 2}
        with tempfile.TemporaryDirectory() as root:
            capture = Path(root)
            (capture / "manifest.json").write_text(json.dumps({"errors": [original], "workload_exit_code": None}))
            stage_driver.record_capture_manifest(capture, case)
            self.assertFalse(case["workload_started"])
            self.assertIsNone(case["workload_exit_code"])
            stage_driver.record_capture_failure(case, FileNotFoundError(str(capture / "workload.stdout")))
        self.assertEqual(case["capture_errors"], [original])
        self.assertIn(original, case["error"])
        self.assertNotIn("FileNotFoundError", case["error"])
        self.assertIn("FileNotFoundError", case["analysis_error"])

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


PERSISTENT_GENERATION_DOCKER = r'''#!/usr/bin/env python3
import os,sys,time
from pathlib import Path
assert sys.argv[1:3] == ['exec','-i'], sys.argv
container=sys.argv[3]
root=Path(os.environ['PROC_FIXTURES'])
with (root/'launches').open('a') as output:
    output.write(f'{container}\t{os.getpid()}\n')
mode=os.environ.get('PERSISTENT_GENERATION_DOCKER_MODE','normal')
if mode == 'die': sys.exit(7)
if mode == 'malformed':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t28\tbad\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'closed_stdin':
    os.close(0); time.sleep(10); sys.exit(0)
if mode == 'zero_pid':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t0\t202\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'same_pid':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t28\t202\t28\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'stale':
    request=sys.stdin.readline()
    print('0-0\t28\t202\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'stall':
    sys.stdin.readline(); time.sleep(10); sys.exit(0)
args=sys.argv[4:]
args[-2]=str(root/container)
os.execvp(args[0],args)
'''


class PersistentGenerationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / 'docker').write_text(PERSISTENT_GENERATION_DOCKER)
        (self.root / 'docker').chmod(0o755)
        source = Path(__file__).with_name("test-cluster.sh").read_text()
        marker = 'if [ "${SINOMO_TEST_TIMING_HELPERS_SELF_TEST:-0}" = 1 ]; then'
        self.assertEqual(source.count(marker), 1)
        (self.root / 'definitions.sh').write_text(source.split(marker)[0])
        (self.root / 'quiet-network-counters.py').write_text(Path(__file__).with_name('quiet-network-counters.py').read_text())
        for node in ('node1', 'node2', 'node3', 'node4', 'node5', 'node6'):
            self.proc(node, '1', 'sinomo-shadow', ['sinomo', 'daemon', 'shadow', 'run'], '101')
            self.proc(node, '28', 'sinomo', ['sinomo', 'daemon', 'run'], '202')
            # Observer argv contains role literals, but its comm is not a daemon.
            self.proc(node, '99', 'sh', ['sh', '-c', 'daemon shadow run and daemon run'], '303')
        self.env = dict(os.environ, PATH=str(self.root)+os.pathsep+os.environ['PATH'], PROC_FIXTURES=str(self.root))

    def tearDown(self):
        self.tmp.cleanup()

    def proc(self, node, pid, comm, args, start):
        path = self.root / node / pid
        path.mkdir(parents=True)
        (path / 'comm').write_text(comm+'\n')
        (path / 'cmdline').write_bytes(b'\0'.join(a.encode() for a in args)+b'\0')
        fields = ['S'] + ['0'] * 49
        fields[19] = start
        (path / 'stat').write_text(f'{pid} ({comm}) ' + ' '.join(fields) + '\n')

    def shell(self, body, mode='normal', extra_env=None):
        env = dict(self.env, PERSISTENT_GENERATION_DOCKER_MODE=mode, **(extra_env or {}))
        script = 'source "$PROC_FIXTURES/definitions.sh"\nset -e\n'+body
        value = subprocess.run(['bash', '-c', script], env=env, text=True, capture_output=True, timeout=8)
        self.assertEqual(value.returncode, 0, value.stdout+'\n'+value.stderr)
        self.assertNotIn('coproc', value.stderr)
        return value

    def test_reuses_one_session_and_refreshes_glob_on_each_request(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 5000 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        first="$(container_daemon_generation_snapshot node1 1)"
        [ "$first" = $'28\t202\t1\t101' ]
        mv "$PROC_FIXTURES/node1/28" "$PROC_FIXTURES/node1/42"
        second="$(container_daemon_generation_snapshot node1 1)"
        [ "$second" = $'42\t202\t1\t101' ]
        ! network_quiet_generation_matches "$first" "$second"
        cp -r "$PROC_FIXTURES/node1/42" "$PROC_FIXTURES/node1/43"
        ! container_daemon_generation_snapshot node1 1 >/dev/null
        ''')
        self.assertEqual(len((self.root/'launches').read_text().splitlines()),1)

    def test_network_reads_reuse_generation_worker_and_inherited_descriptors(self):
        self.shell(r'''
        NETWORK_QUIET_NET_DIRECTORY="$PROC_FIXTURES/counters"
        mkdir "$NETWORK_QUIET_NET_DIRECTORY"
        printf '%s\n' 'printf "net:[123]\\n28 202 1 101\\nInter-| Receive | Transmit\\nface | bytes | bytes\\nlo: 1 0 0 0 0 0 0 0 2 0 0 0 0 0 0 0\\n"' > "$NETWORK_QUIET_NET_DIRECTORY/counter-program.sh"
        deadline=$(( $(monotonic_millis) + 5000 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        python3 - "$BASHPID" "${NETWORK_QUIET_GENERATION_READ_FD[node1]}" "${NETWORK_QUIET_GENERATION_WRITE_FD[node1]}" "$deadline" "$PROC_FIXTURES/quiet-network-counters.py" <<'PYCOUNTER'
import importlib.util, sys
spec = importlib.util.spec_from_file_location('counter', sys.argv[5])
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
for _ in range(2):
    raw = module.read_owned_worker_counter(*map(int, sys.argv[1:5]))
    namespace, generations, interfaces = module.parse_counters(raw)
    assert namespace == 'net:[123]' and interfaces['lo']['tx_bytes'] == '2'
PYCOUNTER
        [ "$(container_daemon_generation_snapshot node1 1)" = $'28\t202\t1\t101' ]
        [ "$(wc -l < "$PROC_FIXTURES/launches")" = 1 ]
        ''')

    def test_multiple_sessions_close_all_inherited_writers_on_eof(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 5000 ))
        network_quiet_generation_sessions_start "$deadline" node1 node2 node3 node4 node5 node6
        trap network_quiet_generation_sessions_stop EXIT
        pids=("${NETWORK_QUIET_GENERATION_PID[@]}")
        for node in node1 node2 node3 node4 node5 node6; do
          [ "$(container_daemon_generation_snapshot "$node" 1)" = $'28\t202\t1\t101' ]
        done
        network_quiet_generation_sessions_stop
        for pid in "${pids[@]}"; do ! kill -0 "$pid" 2>/dev/null; done
        [ "${#NETWORK_QUIET_GENERATION_PID[@]}" = 0 ]
        network_quiet_generation_sessions_stop
        ''')
        self.assertEqual(len((self.root/'launches').read_text().splitlines()),6)

    def test_bad_or_stale_response_and_worker_eof_fail_closed(self):
        for mode in ('malformed', 'stale', 'die', 'zero_pid', 'same_pid'):
            with self.subTest(mode=mode):
                self.shell(r'''
                deadline=$(( $(monotonic_millis) + 2000 ))
                network_quiet_generation_sessions_start "$deadline" node1
                trap network_quiet_generation_sessions_stop EXIT
                ! container_daemon_generation_snapshot node1 0.3 >/dev/null
                ''', mode=mode)

    def test_broken_request_pipe_does_not_kill_caller(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 2000 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        sleep 0.1
        ! container_daemon_generation_snapshot node1 0.3 >/dev/null
        printf 'caller-survived\n'
        ''', mode='closed_stdin')

    def test_worker_death_and_unknown_container_fail_closed(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 2000 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        [ "$(container_daemon_generation_snapshot node1 1)" = $'28\t202\t1\t101' ]
        pid="${NETWORK_QUIET_GENERATION_PID[node1]}"
        kill -TERM "$pid"
        wait "$pid" || true
        ! container_daemon_generation_snapshot node1 0.3 >/dev/null
        ! container_daemon_generation_snapshot replacement-container 0.3 >/dev/null
        ''')

    def test_read_timeout_respects_original_absolute_deadline(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 200 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        before="$(monotonic_millis)"
        ! network_quiet_generation_snapshot_before_deadline node1 "$deadline" >/dev/null
        after="$(monotonic_millis)"
        [ "$((after - before))" -lt 500 ]
        ! network_quiet_generation_snapshot_before_deadline node1 "$before" >/dev/null
        ''', mode='stall')

    def test_container_watchdog_exits_even_if_host_stops_requesting(self):
        self.shell(r'''
        deadline=$(( $(monotonic_millis) + 300 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        pid="${NETWORK_QUIET_GENERATION_PID[node1]}"
        ! wait "$pid"
        ! container_daemon_generation_snapshot node1 0.3 >/dev/null
        ''')

    def test_fresh_missing_or_malformed_role_evidence_fails_closed(self):
        for change in ('rm -f "$PROC_FIXTURES/node1/1/comm"',
                       'printf "28 2 3\\n" > "$PROC_FIXTURES/node1/28/stat"'):
            with self.subTest(change=change):
                self.shell(r'''
                deadline=$(( $(monotonic_millis) + 2000 ))
                network_quiet_generation_sessions_start "$deadline" node1
                trap network_quiet_generation_sessions_stop EXIT
                '''+change+r'''
                ! container_daemon_generation_snapshot node1 1 >/dev/null
                ''')
                # Restore the fixture for the next independent evidence failure.
                (self.root/'node1/1/comm').write_text('sinomo-shadow\n')

    def mock_cleanup_gate(self, case):
        self.shell(r'''
        SINOMO_TEST_TIMING_HELPERS_SELF_TEST=1
        NODES=node1
        EXPECTED_MAINS=1
        ARTIFACTS="$PROC_FIXTURES/gate"
        mkdir "$ARTIFACTS"
        mock_now=100
        mock_index=0
        mock_stream_running=0
        mock_cleanup_count=0
        mock_cleanup_stream=unavailable
        times=(1000 2000 32000 34000 34500 35500 36500)
        monotonic_millis() { printf '%s\n' "$mock_now"; }
        network_quiet_container_for_node_before_deadline() { printf 'id-node1\n'; }
        network_quiet_container_name_before_deadline() { printf 'mock-node1\n'; }
        network_quiet_stats_stream_start() { mock_stream_running=1; NETWORK_QUIET_NET_SEALED=0; }
        network_quiet_sample_network_group() {
          NETWORK_QUIET_GROUP_NET_MIN_MS="$mock_now"
          NETWORK_QUIET_GROUP_NET_CLOSED_MS="$mock_now"
          NETWORK_QUIET_GROUP_NET_STATUS[node1]=within
        }
        network_quiet_stats_stream_stop() { mock_stream_running=0; }
        network_quiet_stream_restart_delay() { return 1; }
        network_quiet_establish_generation_baseline() { generation_baseline[node1]='id-node1|28 202 1 101'; }
        network_quiet_verify_generation_baseline() { NETWORK_QUIET_GENERATION_VERIFY_FINISHED_MS=33000; mock_now=33000; }
        network_quiet_generation_sessions_stop() {
          mock_cleanup_count=$((mock_cleanup_count + 1))
          mock_cleanup_stream="$mock_stream_running"
          if [ "$CLEANUP_TEST_CASE" = deadline ]; then mock_now=120100
          else mock_now=$((mock_now + 1000))
          fi
        }
        network_quiet_stats_stream_read_group() {
          local at cpu
          [ "$mock_index" -lt "${#times[@]}" ] || return 1
          at="${times[$mock_index]}"
          mock_index=$((mock_index + 1))
          if [ "$mock_now" -lt "$at" ]; then mock_now="$at"; fi
          cpu=1.00%
          NETWORK_QUIET_GROUP_REASON=''
          NETWORK_QUIET_GROUP_STRUCTURAL_ERROR=0
          NETWORK_QUIET_GROUP_MIN_MS="$at"
          NETWORK_QUIET_GROUP_MAX_MS="$at"
          NETWORK_QUIET_GROUP_CPU_STATUS[node1]=within
          if [ "$CLEANUP_TEST_CASE" = busy ] && [ "$at" = 34500 ]; then
            cpu=20.00%
            NETWORK_QUIET_GROUP_CPU_STATUS[node1]=above
            NETWORK_QUIET_GROUP_REASON='mock-node1=20.00%'
          fi
          NETWORK_QUIET_GROUP_CPU[node1]="$cpu"
          NETWORK_QUIET_GROUP_NAME[node1]=mock-node1
          NETWORK_QUIET_GROUP_OBSERVED_MS[node1]="$at"
        }
        case "$CLEANUP_TEST_CASE" in
          quiet)
            wait_for_network_quiet_gate cleanup
            [ "$mock_index" = 6 ]
            awk '/^status=pass$/ { found=1 } END { exit !found }' "$ARTIFACTS/network-quiet-cleanup.summary.txt"
            awk -F '\t' '$6 == 34500 && $15 == "verification-covering" { seen=1 } END { exit !seen }' "$ARTIFACTS/network-quiet-cleanup.samples.tsv"
            awk -F '\t' '$6 == 35500 && $15 == "pass" { seen=1 } END { exit !seen }' "$ARTIFACTS/network-quiet-cleanup.samples.tsv"
            ;;
          busy)
            ! wait_for_network_quiet_gate cleanup
            [ "$mock_index" = 5 ]
            awk '/^status=fail$/ { found=1 } END { exit !found }' "$ARTIFACTS/network-quiet-cleanup.summary.txt"
            awk -F '\t' '$6 == 34500 && $12 == "20.00%" && $15 == "verification-busy" { seen=1 } END { exit !seen }' "$ARTIFACTS/network-quiet-cleanup.samples.tsv"
            ;;
          deadline)
            ! wait_for_network_quiet_gate cleanup
            [ "$mock_index" = 4 ]
            awk '/^status=fail$/ { found=1 } END { exit !found }' "$ARTIFACTS/network-quiet-cleanup.summary.txt"
            awk '/^reason=quiet-window-deadline-exhausted$/ { found=1 } END { exit !found }' "$ARTIFACTS/network-quiet-cleanup.summary.txt"
            ;;
        esac
        [ "$mock_cleanup_count" = 1 ]
        [ "$mock_cleanup_stream" = 1 ]
        [ "$mock_stream_running" = 0 ]
        ''', extra_env={'CLEANUP_TEST_CASE': case})

    def test_quiet_coverage_must_reach_helper_cleanup_completion(self):
        self.mock_cleanup_gate('quiet')

    def test_helper_cleanup_cpu_resets_candidate(self):
        self.mock_cleanup_gate('busy')

    def test_helper_cleanup_cannot_extend_absolute_gate_deadline(self):
        self.mock_cleanup_gate('deadline')

    def test_final_verification_resolves_fresh_container_id_and_preserves_schema(self):
        value = self.shell(r'''
        deadline=$(( $(monotonic_millis) + 2000 ))
        network_quiet_generation_sessions_start "$deadline" node1
        trap network_quiet_generation_sessions_stop EXIT
        NODES=node1
        declare -A generation_baseline=()
        generation_baseline[node1]="node1|$(container_daemon_generation_snapshot node1 1)"
        network_quiet_container_for_node_before_deadline() { printf 'replacement-container\n'; }
        ! network_quiet_verify_generation_baseline "$deadline" 1 "$PROC_FIXTURES/generations.tsv"
        cat "$PROC_FIXTURES/generations.tsv"
        ''')
        fields = value.stdout.strip().split('\t')
        self.assertEqual(len(fields), 12)
        self.assertEqual(fields[:2], ['1', 'verify'])
        self.assertRegex(fields[2], r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')
        self.assertRegex(fields[3], r'^\d{13}$')
        self.assertEqual(fields[4:6], ['node1', 'replacement-container'])
        self.assertEqual(fields[6:10], ['', '', '', ''])
        self.assertEqual(fields[10:], ['false', 'changed'])



class DfsBaselineLaneTests(unittest.TestCase):
    @staticmethod
    def shell_function(name):
        script = Path(__file__).with_name('test-cluster.sh').read_text()
        return name + '() {' + script.split('\n' + name + '() {', 1)[1].split('\n}\n', 1)[0] + '\n}\n'

    def test_baseline_selector_rejects_combined_lanes_and_invalid_values(self):
        validator = self.shell_function('validate_dfs_suite_config')
        check = r'''require_bounded_uint() {
  [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -ge "$3" ] && [ "$2" -le "$4" ]
}
log() { :; }
''' + validator + '\nvalidate_dfs_suite_config'
        base = dict(os.environ, DFS_BASELINE_ONLY='1', DFS_ONLY='0', DFS_READY_ONLY='0',
                    FORMATION_ONLY='0', CLUSTER_KV_ONLY='0', PJDFSTEST_ONLY='0', QUIET_ONLY='0',
                    NETWORK_CASES='', SINOMO_TEST_NETWORK_ONLY='0', DFS_READY_REQUIRE_QUIET='0')
        self.assertEqual(subprocess.run(['bash','-c',check], env=base, capture_output=True).returncode, 0)
        for key in ('DFS_ONLY','DFS_READY_ONLY','FORMATION_ONLY','CLUSTER_KV_ONLY','PJDFSTEST_ONLY',
                    'QUIET_ONLY','SINOMO_TEST_NETWORK_ONLY','NETWORK_CASES'):
            with self.subTest(selector=key):
                env = dict(base, **{key:'1'})
                self.assertNotEqual(subprocess.run(['bash','-c',check], env=env, capture_output=True).returncode, 0)
        for value in ('2', 'unknown'):
            self.assertNotEqual(subprocess.run(['bash','-c',check], env=dict(base, DFS_BASELINE_ONLY=value),
                                               capture_output=True).returncode, 0)

    def test_baseline_keeps_every_final_check_after_a_real_scenario_failure_result(self):
        with tempfile.TemporaryDirectory() as directory:
            command = r'''FAILED_SCENARIOS=""
scenario() {
  local name="$1"
  printf '%s\t%s\n' "$name" "$*" >> "$ARTIFACTS/calls.tsv"
  if [ "$name" = dfs-normal-correctness ]; then FAILED_SCENARIOS="$FAILED_SCENARIOS $name"; fi
}
''' + self.shell_function('dfs_baseline_case') + self.shell_function('dfs_baseline_correctness') + '\ndfs_baseline_correctness'
            result = subprocess.run(['bash','-c',command], env=dict(os.environ, ARTIFACTS=directory), capture_output=True)
            self.assertNotEqual(result.returncode, 0, result.stderr.decode())
            scope = json.loads((Path(directory)/'dfs-baseline-scope.json').read_text())
            self.assertEqual(scope['status'], 'failed')
            self.assertEqual(len(scope['required_cases']), 15)
            self.assertEqual([case['name'] for case in scope['required_cases'] if case['status'] != 'passed'],
                             ['dfs-normal-correctness'])
            calls = (Path(directory)/'calls.tsv').read_text()
            self.assertIn('dfs-history-restart dfs_acceptance_case history --restart-cluster', calls)
            self.assertIn('dfs-native-handoff dfs_acceptance_case handoff', calls)
            for name in ('invariants-final','routed-status-final','baseline-final-convergence','baseline-final-joint-quiet'):
                self.assertIn(name, calls)
            self.assertNotIn('bulk-performance\t', calls)
            self.assertNotIn('dfs-cli-benchmarks\t', calls)
            self.assertTrue(all(case['status']=='not_run' for case in scope['performance_cases']))
            self.assertFalse(scope['performance_acceptance'])

    def test_report_preserves_not_run_performance_and_rejects_incomplete_claimed_baseline(self):
        names = ['invariants-baseline','read-your-write','cross-node','rename-across-prefixes',
                 'disk-damage-recovery','temporary-network-recovery','restart-durability',
                 'dfs-core-only-components','dfs-normal-correctness','dfs-history-restart',
                 'dfs-native-handoff','invariants-final','routed-status-final',
                 'baseline-final-convergence','baseline-final-joint-quiet']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'verification.json').write_text(json.dumps({'passed':True,'lane':'dfs-baseline=1'}))
            scope = {'schema':'sinomo.dfs-correctness-baseline.v1','status':'passed',
                     'required_cases':[{'name':name,'status':'passed'} for name in names],
                     'performance_cases':[{'name':'bulk-performance','status':'not_run','reason':'deferred'}],
                     'voter_coverage':'six containers do not prove multi-voter coverage'}
            (root/'dfs-baseline-scope.json').write_text(json.dumps(scope))
            report = driver.verification_report.write_report(root)
            self.assertTrue(report['passed'])
            self.assertFalse(report['performance_acceptance'])
            self.assertEqual(report['dfs_baseline']['performance_cases'][0]['status'], 'not_run')
            self.assertIn('NOT_RUN', (root/'REPORT.md').read_text())
            scope['required_cases'].pop()
            (root/'dfs-baseline-scope.json').write_text(json.dumps(scope))
            self.assertFalse(driver.verification_report.write_report(root)['passed'])
            (root/'dfs-baseline-scope.json').unlink()
            self.assertFalse(driver.verification_report.write_report(root)['passed'])


class AssignedParticipantInterruptionTests(unittest.TestCase):
    @staticmethod
    def partition_layout(primary="native-node4"):
        nodes = [f"native-node{i}" for i in range(1, 7)]
        return {"status": "passed", "diagnostics": {"pending_handoff_keys": [], "shards": [
            {"lane": lane, "file_system_id": "native-fs", "shard_id": lane, "epoch": 7,
             "primary_node_id": owner, "replica_node_ids": [node for node in nodes if node != owner]}
            for lane, owner in [("root", "native-node1"), ("services", primary), ("users", "native-node3")]]}}

    def test_partition_roles_follow_actual_primary_and_prefer_nonprimary_leaf6_else_leaf5(self):
        # Pure harness DATA control; no supplied observation grants authority.
        functions = (DfsBaselineLaneTests.shell_function('dfs_converged_epoch_signature') +
                     DfsBaselineLaneTests.shell_function('dfs_partition_participants_from_layout'))
        roster = [{"container": f"node{i}", "node_id": f"native-node{i}"} for i in range(1, 7)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout_file, roster_file = root/'layout.json', root/'roster.json'
            for primary, expected_writer, expected_leaf in [
                    ("native-node4", "node4", "node6"),
                    ("native-node6", "node6", "node5"),
                    ("native-node2", "node2", "node6")]:
                layout_file.write_text(json.dumps(self.partition_layout(primary)))
                roster_file.write_text(json.dumps(roster))
                result = subprocess.run(['bash', '-c', functions +
                    '\ndfs_partition_participants_from_layout "$1" "$2"', 'test', str(layout_file), str(roster_file)],
                    capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                roles = json.loads(result.stdout)
                self.assertEqual(roles['writer_container'], expected_writer)
                self.assertEqual(roles['writer_node_id'], primary)
                self.assertEqual(roles['unavailable_container'], expected_leaf)
                self.assertEqual(roles['unavailable_node_id'], 'native-' + expected_leaf)
                self.assertNotEqual(roles['writer_node_id'], roles['unavailable_node_id'])
            negatives = []
            negatives.append((self.partition_layout(), roster[:-1]))
            duplicate = [dict(row) for row in roster]
            duplicate[-1]['node_id'] = duplicate[0]['node_id']
            negatives.append((self.partition_layout(), duplicate))
            wrong = self.partition_layout()
            wrong['diagnostics']['shards'][1]['replica_node_ids'][-1] = 'foreign-node'
            negatives.append((wrong, roster))
            pending = self.partition_layout()
            pending['diagnostics']['pending_handoff_keys'] = ['actual-pending-transition']
            negatives.append((pending, roster))
            for layout, actual_roster in negatives:
                layout_file.write_text(json.dumps(layout))
                roster_file.write_text(json.dumps(actual_roster))
                result = subprocess.run(['bash', '-c', functions +
                    '\ndfs_partition_participants_from_layout "$1" "$2"', 'test', str(layout_file), str(roster_file)],
                    capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b'')

    def exercise_selected_interruption(self, writer, leaf, extra_network=False):
        # Side-effect-free transport stubs cover harness custody and sequencing;
        # these are explicitly not native partition or availability proofs.
        functions = ''.join(DfsBaselineLaneTests.shell_function(name) for name in
                            ['dfs_converged_epoch_signature', 'dfs_partition_participants_from_layout',
                             'temporary_network_recovery'])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'layout.json').write_text(json.dumps(self.partition_layout('native-' + writer)))
            prelude = r'''log() { :; }
cluster_local_node_id() { printf 'identity\t%s\n' "$1" >>"$ARTIFACTS/calls.tsv"; printf 'native-%s\n' "$1"; }
compose() {
  if [ "$1" = ps ] && [ "$2" = -q ]; then printf 'actual-container-%s\n' "$3";
  else printf 'compose\t%s\n' "$*" >>"$ARTIFACTS/calls.tsv"; fi
}
run_probe() {
  local node="$1" case_id="$2"; shift 2
  PROBE_INDEX=$((PROBE_INDEX + 1))
  printf '%s\t%s' "$node" "$case_id" >>"$ARTIFACTS/calls.tsv"
  printf '\t%s' "$@" >>"$ARTIFACTS/calls.tsv"
  printf '\n' >>"$ARTIFACTS/calls.tsv"
  local output="$ARTIFACTS/probe-$(printf '%03d' "$PROBE_INDEX")-$node-$case_id.json"
  if [ "$case_id" = cluster.dfs.shard-layout ]; then
    cp "$ARTIFACTS/layout.json" "$output"
  elif [ "$case_id" = cluster.dfs.partition-write ]; then
    printf '{"diagnostics":{"original":{"object_id":"actual-original"},"assignment":{"primary_node_id":"%s","epoch":7},"write_options":{"immutable":"actual-options"}}}\n' "$TEST_PRIMARY_NATIVE" >"$output"
  fi
}
docker() {
  printf 'docker\t%s\n' "$*" >>"$ARTIFACTS/calls.tsv"
  if [ "$1" = inspect ]; then
    if [ "$TEST_EXTRA_NETWORK" = yes ]; then printf '{"owned-retained_seg_b":{},"unexpected-network":{}}\n';
    else printf '{"owned-retained_seg_b":{}}\n'; fi
  fi
}
sleep() { :; }
wait_for_cluster_ready() { printf 'ready\t%s\n' "$*" >>"$ARTIFACTS/calls.tsv"; }
wait_for_dfs_ready() { :; }
wait_for_dfs_converged() { :; }
NODES='node1 node2 node3 node4 node5 node6'
PROJECT=owned-retained
EXPECT_TIMEOUT=60
NETWORK_PARTITION_SECONDS=1
PROBE_INDEX=0
'''
            script = prelude + functions + '\nARTIFACTS="$1"\nTEST_PRIMARY_NATIVE="$2"\nRUN_ID="retained-current-$3"\nTEST_EXTRA_NETWORK="$4"\ntemporary_network_recovery'
            result = subprocess.run(['bash', '-c', script, 'test', directory, 'native-' + writer,
                                     writer, 'yes' if extra_network else 'no'],
                                    capture_output=True, text=True, timeout=5)
            calls = [line.split('\t') for line in (root/'calls.tsv').read_text().splitlines()]
            if extra_network:
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(len(row) > 1 and row[1] == 'cluster.dfs.write' for row in calls))
                self.assertFalse(any(row[0] == 'docker' and row[1].startswith('network disconnect') for row in calls))
                return
            self.assertEqual(result.returncode, 0, result.stderr)
            partition = [row for row in calls if len(row) > 1 and row[1] == 'cluster.dfs.partition-write']
            self.assertEqual(len(partition), 3)
            self.assertEqual([row[0] for row in partition], [writer] * 3)
            self.assertEqual([next(arg for arg in row if arg.startswith('phase=')) for row in partition],
                             ['phase=baseline', 'phase=blocked', 'phase=recover'])
            for row in partition:
                self.assertIn('required-participants=6', row)
                self.assertIn(f'operation-id=network-heal-retained-current-{writer}', row)
                self.assertIn('unavailable-node-id=native-' + leaf, row)
            for row in partition[1:]:
                self.assertIn('original-object-json={"object_id":"actual-original"}', row)
                self.assertIn('original-assignment-json={"primary_node_id":"native-' + writer + '","epoch":7}', row)
                self.assertIn('original-options-json={"immutable":"actual-options"}', row)
            self.assertEqual([row[0] for row in calls if len(row) > 1 and row[1] == 'cluster.dfs.write'], [writer, writer])
            self.assertIn(['docker', f'network disconnect owned-retained_seg_b actual-container-{leaf}'], calls)
            self.assertIn(['docker', f'network connect --alias {leaf} owned-retained_seg_b actual-container-{leaf}'], calls)
            self.assertIn(['compose', f'restart {leaf}'], calls)
            restart_at = calls.index(['compose', f'restart {leaf}'])
            ready_at = calls.index(['ready', 'post-network-recovery-reopen'])
            post_restart_identity = [index for index, row in enumerate(calls)
                                     if index > restart_at and row == ['identity', leaf]]
            self.assertEqual(len(post_restart_identity), 1)
            self.assertLess(restart_at, ready_at)
            self.assertLess(ready_at, post_restart_identity[0])
            leaf_reads = [row for row in calls if row[0] == leaf and len(row) > 1 and row[1] == 'cluster.dfs.expect-file']
            self.assertEqual(len(leaf_reads), 5)
            receipt = json.loads((root/'network-heal-writer-selection.json').read_text())
            self.assertEqual(receipt['writer_container'], writer)
            self.assertEqual(receipt['unavailable_container'], leaf)
            self.assertEqual(receipt['unavailable_node_id'], 'native-' + leaf)
            self.assertEqual(receipt['unavailable_container_id'], 'actual-container-' + leaf)
            self.assertEqual(receipt['interrupted_network'], 'owned-retained_seg_b')

    def test_interruption_primary4_keeps_leaf6_and_original_through_heal_and_reopen(self):
        self.exercise_selected_interruption('node4', 'node6')

    def test_interruption_primary6_keeps_leaf5_and_original_through_heal_and_reopen(self):
        self.exercise_selected_interruption('node6', 'node5')

    def test_interruption_refuses_a_leaf_with_an_extra_network_before_any_seed(self):
        self.exercise_selected_interruption('node6', 'node5', extra_network=True)

    def test_cut_requires_exact_durable_witness_and_heal_reuses_original_inputs(self):
        body = DfsBaselineLaneTests.shell_function('temporary_network_recovery')
        self.assertLess(body.index('--param phase=baseline'), body.index('docker network disconnect'))
        self.assertLess(body.index('--param phase=blocked'), body.index('docker network connect'))
        self.assertLess(body.index('docker network connect'), body.index('--param phase=recover'))
        self.assertEqual(body.count('--param "operation-id=$operation_id"'), 3)
        self.assertEqual(body.count('--param required-participants=6'), 3)
        self.assertEqual(body.count('--param "original-options-json=$options_json"'), 2)
        self.assertIn('compose restart "$unavailable"', body)
        self.assertIn('new write after network heal', body)
        self.assertNotIn('expect-error', body)
        self.assertNotIn('grep', body)

    def test_failed_cut_is_retained_while_actual_heal_and_original_recovery_are_attempted(self):
        body = DfsBaselineLaneTests.shell_function('temporary_network_recovery')
        # The blocked probe must not return before the real link is healed.
        blocked = body.split('--param phase=blocked', 1)[1].split('--param phase=recover', 1)[0]
        self.assertIn('|| rc=1', blocked)
        self.assertNotIn('return 0', blocked)
        self.assertIn('docker network connect', blocked)
        self.assertNotIn('if [ "$rc" = 0 ]', body)

    def test_runtime_probe_does_not_accept_bare_errors_or_parse_rejection_strings(self):
        source = (Path(__file__).parents[1] / 'crates/sinomo-cli/src/commands/test_cases_dfs/partition_write.rs').read_text()
        for required in ('record.validate()', 'DfsPreparedOperationStatus::Prepared',
                         'original_intent_matches', 'actual.version != original.version',
                         'DfsStableWriteOperationReconciliation::Applied', 'receipt.target_version != target'):
            self.assertIn(required, source)
        self.assertNotIn('error.to_string().contains', source)
        self.assertNotIn('write_return.contains', source)


class DiskDamageSelectionTests(unittest.TestCase):
    @staticmethod
    def inputs(locations=('native-node6', 'native-node4')):
        # Explicit harness DATA only. No native row/ACK or physical copy is made.
        roster = [{'container': f'node{i}', 'node_id': f'native-node{i}'} for i in range(1, 7)]
        identity = {'file_system_id': 'native-fs', 'object_id': 'native-object',
                    'version': 'native-version', 'content_hash': 'blake3:' + 'a' * 64,
                    'size_bytes': 1048576, 'path': '/services/cluster-tests/test/disk-damage.bin'}
        view = {'object': {**{k: v for k, v in identity.items() if k != 'size_bytes'},
                           'kind': 'file', 'current_locations': list(locations)},
                'versions': [{'version': identity['version'], 'size_bytes': identity['size_bytes']}],
                'receipts': [{'receipt': {'file_system_id': identity['file_system_id'],
                    'object_id': identity['object_id'], 'committed_version': identity['version'],
                    'owner_node_id': 'native-node6', 'event_id': f'data-event-{node}'},
                    'replica_node_id': node, 'content_hash': identity['content_hash'],
                    'received_at': '1970-01-01T00:00:00Z'} for node in locations]}
        report = {'status': 'passed', 'diagnostics': {**{k: identity[k] for k in
                   ['object_id', 'version', 'content_hash', 'size_bytes']},
                   'current_locations': list(locations)}}
        return roster, view, report

    def select(self, roster, view, report):
        functions = ''.join(DfsBaselineLaneTests.shell_function(name) for name in
                            ['dfs_damage_roster_valid', 'dfs_damage_participants_from_observation'])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, data in [('roster', roster), ('view', view), ('report', report)]:
                (root/f'{name}.json').write_text(json.dumps(data))
            return subprocess.run(['bash', '-c', functions +
                '\ndfs_damage_participants_from_observation "$1/report.json" "$1/view.json" "$1/roster.json" "$2"',
                'test', directory, '/services/cluster-tests/test/disk-damage.bin'],
                capture_output=True, text=True, timeout=5)

    def test_actual_locations6_and4_never_choose_stale3(self):
        result = self.select(*self.inputs())
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = json.loads(result.stdout)
        self.assertEqual(actual['damage_container'], 'node4')
        self.assertEqual(actual['damage_node_id'], 'native-node4')
        self.assertEqual(actual['remaining_container'], 'node6')
        self.assertEqual(actual['remaining_node_id'], 'native-node6')
        self.assertEqual(actual['expected_identity']['node_id'], 'native-node4')
        self.assertEqual(actual['expected_identity']['size_bytes'], 1048576)
        self.assertIn('not local physical proof', actual['remaining_copy_qualification'])

    def test_preferred3_is_selected_only_when_it_is_current(self):
        result = self.select(*self.inputs(('native-node3', 'native-node4')))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['damage_container'], 'node3')

    def test_missing_mapping_single_copy_or_changed_observation_refuses_before_fault(self):
        roster, view, report = self.inputs()
        bad_roster = roster[:-1]
        changed_report = json.loads(json.dumps(report))
        changed_report['diagnostics']['version'] = 'another-generation'
        foreign = json.loads(json.dumps(view))
        foreign['object']['current_locations'][0] = 'foreign-node'
        single = self.inputs(('native-node6',))
        for data in [(bad_roster, view, report), (roster, view, changed_report),
                     (roster, foreign, report), single]:
            result = self.select(*data)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, '')

    def test_missing_or_mismatched_current_receipt_refuses_metadata_only_holder(self):
        roster, view, report = self.inputs()
        cases = []
        missing = json.loads(json.dumps(view))
        missing.pop('receipts')
        cases.append(('missing-receipt-array', missing))
        for label, receipts in [('empty-receipts', []), ('one-current-receipt', view['receipts'][:1])]:
            changed = json.loads(json.dumps(view))
            changed['receipts'] = receipts
            cases.append((label, changed))
        for field in ['file_system_id', 'object_id', 'committed_version', 'content_hash', 'replica_node_id']:
            changed = json.loads(json.dumps(view))
            receipt = changed['receipts'][0]
            if field in ['file_system_id', 'object_id', 'committed_version']:
                receipt['receipt'][field] = 'different-current-subject'
            else:
                receipt[field] = 'different-current-subject'
            cases.append((field, changed))
        for label, changed in cases:
            result = self.select(roster, changed, report)
            self.assertNotEqual(result.returncode, 0, label)
            self.assertEqual(result.stdout, '', label)

    def test_historical_receipts_do_not_select_a_current_damage_holder(self):
        roster, view, report = self.inputs()
        historical = json.loads(json.dumps(view['receipts'][0]))
        historical['receipt']['committed_version'] = 'historical-version'
        historical['replica_node_id'] = 'native-node3'
        view['receipts'].append(historical)
        result = self.select(roster, view, report)
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = json.loads(result.stdout)
        self.assertEqual(actual['damage_container'], 'node4')
        self.assertEqual(actual['remaining_container'], 'node6')
        # Even when the old recipient appears in current metadata/report, a
        # receipt for another generation cannot qualify it for this fault.
        view['object']['current_locations'].append('native-node3')
        report['diagnostics']['current_locations'].append('native-node3')
        result = self.select(roster, view, report)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['damage_container'], 'node4')

    def test_exact_identity_change_refuses_even_if_bytes_are_equal(self):
        roster, view, report = self.inputs()
        functions = DfsBaselineLaneTests.shell_function('dfs_damage_identity_matches')
        expected = {**{k: v for k, v in view['object'].items() if k not in ['kind', 'current_locations']},
                    'size_bytes': 1048576, 'node_id': 'native-node4'}
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory)/'view.json'
            file.write_text(json.dumps(view))
            argv = ['bash', '-c', functions + '\ndfs_damage_identity_matches "$1" "$2"',
                    'test', json.dumps(expected), str(file)]
            result = subprocess.run(argv, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            for field in ['object_id', 'version', 'content_hash', 'file_system_id', 'path']:
                changed = json.loads(json.dumps(view))
                changed['object'][field] = 'changed-identity'
                file.write_text(json.dumps(changed))
                result = subprocess.run(argv, capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0, field)

    def test_invalid_native_roster_refuses_before_seed_or_damage(self):
        functions = ''.join(DfsBaselineLaneTests.shell_function(name) for name in
                            ['dfs_damage_roster_valid', 'dfs_damage_participants_from_observation',
                             'dfs_damage_identity_matches', 'disk_damage_recovery'])
        prelude = r'''cluster_local_node_id() {
  if [ "$1" = node6 ]; then printf 'native-node1\n'; else printf 'native-%s\n' "$1"; fi
}
run_probe() { printf '%s\n' "$*" >>"$ARTIFACTS/forbidden-calls"; }
compose() { printf '%s\n' "$*" >>"$ARTIFACTS/forbidden-calls"; }
NODES='node1 node2 node3 node4 node5 node6'
RUN_ID=test
DAMAGE_BYTES=1048576
EXPECT_TIMEOUT=60
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(['bash', '-c', prelude + functions +
                '\nARTIFACTS="$1"\ndisk_damage_recovery', 'test', directory],
                capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((Path(directory)/'forbidden-calls').exists())

    def test_existing_fault_keeps_expected_tuple_and_read_side_zero_contract(self):
        body = DfsBaselineLaneTests.shell_function('disk_damage_recovery')
        self.assertLess(body.index('dfs_damage_roster_valid'), body.index('cluster.dfs.write'))
        self.assertLess(body.index('dfs_damage_identity_matches'), body.index('cluster.dfs.damage-local-loose-blob'))
        self.assertIn('--param "expected-object-json=$expected"', body)
        self.assertEqual(body.count('--param "max-proc-write-bytes=0"'), 2)
        self.assertIn('.diagnostics.unlink_completed == true', body)
        self.assertIn('fault_recipient_manifest.object_id == $expected.object_id', body)
        self.assertIn('run_probe "$damage" cluster.dfs.expect-file', body)
        self.assertNotIn('run_probe node3 cluster.dfs.damage-local-loose-blob', body)
        self.assertNotIn('expect-error', body)


if __name__ == "__main__":
    unittest.main()
