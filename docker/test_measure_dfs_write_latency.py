"""Bounded target execution and fail-closed matrix continuation controls."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "write_latency", Path(__file__).with_name("measure-dfs-write-latency.py"))
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class ProbeCompletionTests(unittest.TestCase):
    def complete_group(self):
        base = "/cluster-tests/retained/write-latency"
        samples = [{"operation":op, "index":i, "measured":i >= 1,
                    "elapsed_us":200000, "placement_verified":True,
                    "placement_policy":matrix.POLICIES["w1-n2"],
                    "placement_source_path":base, "succeeded":True, "error":None}
                   for op in ("create", "overwrite") for i in range(3)]
        return {"replication_scenario":"w1-n2", "origin":"node1", "shard":"root",
                "base":base, "command_timed_out":False,
                "probe_diagnostics":{"base":base, "samples":samples,
                    "correctness_passed":True, "wait_for_commit":True,
                    "replication_scenario":"w1-n2",
                    "requested_placement_policy":matrix.POLICIES["w1-n2"]}}

    def test_latency_only_failure_can_continue(self):
        self.assertTrue(matrix.probe_failure_allows_continuation(self.complete_group(), 2, 1))

    def test_incomplete_or_uncertain_cases_stop(self):
        for mutation in [
            lambda g: g.update(command_timed_out=True),
            lambda g: g["probe_diagnostics"].update(correctness_passed=False),
            lambda g: g["probe_diagnostics"].update(wait_for_commit=False),
            lambda g: g["probe_diagnostics"]["samples"].pop(),
            lambda g: g["probe_diagnostics"]["samples"][0].update(succeeded=False),
            lambda g: g["probe_diagnostics"]["samples"][0].update(placement_verified=False),
            lambda g: g["probe_diagnostics"]["samples"][0].update(error="PossiblyApplied"),
            lambda g: g["probe_diagnostics"]["samples"][0].update(placement_policy=matrix.POLICIES["w2-n3"]),
        ]:
            group = self.complete_group()
            mutation(group)
            self.assertFalse(matrix.probe_failure_allows_continuation(group, 2, 1), group)

    def test_timeout_terminates_target_and_its_child(self):
        # Execute the same target-side supervisor without Docker. Both parent
        # and child acknowledge TERM; the child would otherwise stay alive.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = ("import signal,time,pathlib; "
                     "signal.signal(signal.SIGTERM,lambda *_:"
                     "(pathlib.Path('child-ended').write_text('term'),exit(0))); "
                     "pathlib.Path('child-ready').touch(); time.sleep(30)")
            parent = ("import signal,subprocess,sys,time,pathlib; "
                      "signal.signal(signal.SIGTERM,lambda *_:"
                      "(pathlib.Path('parent-ended').write_text('term'),exit(0))); "
                      f"p=subprocess.Popen([sys.executable,'-c',{child!r}]); "
                      "pathlib.Path('child-pid').write_text(str(p.pid)); p.wait()")
            result = subprocess.run(matrix.bounded_probe_command([sys.executable, "-c", parent], 1),
                                    cwd=root, timeout=1 + matrix.PROBE_EXEC_DRAIN_SECONDS,
                                    capture_output=True)
            self.assertEqual(result.returncode, 124, result.stderr)
            self.assertTrue((root / "child-ready").exists())
            self.assertEqual((root / "parent-ended").read_text(), "term")
            self.assertEqual((root / "child-ended").read_text(), "term")

    def test_main_stops_before_next_case_on_target_or_host_timeout(self):
        for host_timeout in (False, True):
            calls = []

            class Run:
                def __init__(self, args):
                    self.root = Path(__file__).resolve().parent.parent
                    self.artifacts = args.artifacts
                    self.binary = args.binary
                    self.binary_hash = "retained-binary"
                    self.compose = ["docker", "compose", "-p", args.project]
                    self.env, self.index = {}, 0

                def verify_processes(self, *_): pass
                def verify_observe(self): pass
                def probe(self, *args, **_): calls.append(args)

                def run(self, label, argv, **kwargs):
                    self.index += 1
                    calls.append(label)
                    if label.endswith("-write-latency"):
                        # Simulate the existing runner's exact terminal evidence;
                        # no cluster or external process is launched by this test.
                        stem = self.artifacts / f"{self.index:03d}-{label}"
                        stem.with_suffix(".stdout").write_bytes(b"partial")
                        if host_timeout:
                            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                        stem.with_suffix(".timing.json").write_text(
                            json.dumps({"returncode":124, "timed_out":False}))
                        raise RuntimeError("target supervisor expired")
                    return b""

            with tempfile.TemporaryDirectory() as directory:
                args = ["measure", "--project", "owned-fixture", "--binary", "/unused",
                        "--artifacts", directory, "--skip-readiness"]
                with mock.patch.object(sys, "argv", args), \
                     mock.patch.object(matrix.acceptance, "Acceptance", Run), \
                     mock.patch.object(matrix.acceptance.verification_report, "write_report"):
                    with self.assertRaisesRegex(RuntimeError, "stop before later cases"):
                        matrix.main()
                result = json.loads((Path(directory) / "write-latency.json").read_text())
                self.assertEqual(len(result["groups"]), 1)
                self.assertTrue(result["groups"][0]["command_timed_out"])
                self.assertEqual(len(result["unrun_cases"]), 11)
                self.assertNotIn("stage-profiles", calls)
                self.assertEqual(sum(isinstance(x, str) and x.endswith("-write-latency")
                                     for x in calls), 1)


if __name__ == "__main__":
    unittest.main()
