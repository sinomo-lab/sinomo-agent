#!/usr/bin/env python3
"""Focused host-only controls; private roots are owned by run-with-test-fixtures.py."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


profiles = load(ROOT / "docker/profile-dfs-writes.py", "profile_defer_tests")
renderer = profiles.presentation
ENV = "SINOMO_TEST_DEFER_PPROF_PRESENTATION"
OWNED_CHILDREN = []


class PresentationTests(unittest.TestCase):
    def setUp(self):
        # Do not delete failure inputs before the maintained archive/compare step.
        self.root = Path(tempfile.mkdtemp(prefix="pprof-presentation-case-"))

    def raw(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        profile = directory / "cpu.pprof.pb"
        profile.write_bytes(renderer._synthetic_profile())
        response = {"state": "complete", "capture_id": "fixture-capture", "sample_count": 1,
                    "artifact_bytes": profile.stat().st_size,
                    "artifact_relative_path": "observe/profiles/fixture/cpu.pprof.pb",
                    "manifest_relative_path": "observe/profiles/fixture/manifest.json"}
        manifest = directory / "manifest.json"
        manifest.write_text(json.dumps({"process_id": 123, "response": response}))
        return profile, manifest, response

    def collector(self, deferred=True, manifest=True):
        profile, cpu_manifest, response = self.raw(self.root / "source")
        capture = self.root / "capture"
        (capture / "node-id").mkdir(parents=True)
        (capture / "node-id/cpu-final.json").write_text(json.dumps(response))
        calls = []

        def run_copy(label, command):
            calls.append((label, command))
            source = profile if command[-2].endswith("cpu.pprof.pb") else cpu_manifest
            if source == cpu_manifest and not manifest:
                raise RuntimeError("actual raw manifest copy failed")
            shutil.copy2(source, command[-1])

        run = types.SimpleNamespace(args=types.SimpleNamespace(project="fixture"),
                                    env={ENV: "1"} if deferred else {}, run=run_copy)
        return run, capture, calls

    def test_deferred_hashes_and_no_presentation_process_or_output(self):
        profile, manifest, response = self.raw(self.root)
        before = [p.read_bytes() for p in (profile, manifest)]
        with mock.patch.object(renderer, "_go_binary", side_effect=AssertionError("Go invoked")):
            record = renderer.record_deferred_presentation(
                profile, manifest, self.root / "presentation.json", [Path(__file__)], True)
        self.assertEqual(record["status"], "deferred")
        self.assertEqual(record["capture"], response)
        self.assertEqual(record["inputs"]["profile"]["sha256"], hashlib.sha256(before[0]).hexdigest())
        self.assertEqual(record["inputs"]["recorder"]["sha256"],
                         hashlib.sha256(Path(renderer.__file__).read_bytes()).hexdigest())
        self.assertEqual(record["presentation_outputs"], {})
        self.assertEqual(before, [p.read_bytes() for p in (profile, manifest)])
        self.assertFalse((self.root / "flamegraph.html").exists())
        self.assertFalse((self.root / "cpu-top.txt").exists())

    def test_missing_raw_is_unavailable_not_successful_deferral(self):
        _, manifest, _ = self.raw(self.root / "retained")
        output = self.root / "unavailable.json"
        with self.assertRaises(renderer.RenderError):
            renderer.record_deferred_presentation(self.root / "missing", manifest, output)
        record = json.loads(output.read_text())
        self.assertEqual(record["status"], "unavailable")
        self.assertTrue(record["errors"])
        self.assertEqual(record["presentation_outputs"], {})

    def test_mismatched_manifest_is_not_complete_raw_coverage(self):
        profile, manifest, _ = self.raw(self.root)
        data = json.loads(manifest.read_text())
        data["response"]["artifact_bytes"] += 1
        manifest.write_text(json.dumps(data))
        with self.assertRaises(renderer.RenderError):
            renderer.record_deferred_presentation(profile, manifest, self.root / "bad.json")
        self.assertEqual(json.loads((self.root / "bad.json").read_text())["status"], "unavailable")

    def test_idle_and_write_shared_collector_exports_raw_but_never_go(self):
        run, capture, calls = self.collector()
        with mock.patch.object(profiles.subprocess, "run", side_effect=AssertionError("presentation invoked")):
            rows = profiles.collect_cpu_profiles(run, capture, "idle", {"node1": "node-id"})
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["presentation_status"], "deferred")
        self.assertNotIn("top", rows[0])
        self.assertNotIn("flamegraph", rows[0])
        self.assertTrue((capture / "node-id/cpu-manifest.json").is_file())

    def test_failed_raw_export_still_fails_collector(self):
        run, capture, _ = self.collector(manifest=False)
        with mock.patch.object(profiles.subprocess, "run", side_effect=AssertionError("presentation invoked")):
            with self.assertRaisesRegex(RuntimeError, "manifest copy failed"):
                profiles.collect_cpu_profiles(run, capture, "idle", {"node1": "node-id"})
        self.assertFalse((capture / "node-id/pprof-presentation.json").exists())

    def test_default_collector_still_invokes_both_presentations(self):
        run, capture, _ = self.collector(deferred=False)
        with mock.patch.object(profiles.shutil, "which", return_value="/fixture/go"), \
             mock.patch.object(profiles.subprocess, "run", return_value=types.SimpleNamespace(returncode=0)) as invoked:
            rows = profiles.collect_cpu_profiles(run, capture, "idle", {"node1": "node-id"})
        self.assertEqual(invoked.call_count, 2)
        self.assertIn("-top", invoked.call_args_list[0].args[0])
        self.assertIn("render-pprof-flamegraph.py", invoked.call_args_list[1].args[0][1])
        self.assertNotIn("presentation_status", rows[0])

    def test_formation_nested_environment_preserves_raw_and_missing_status(self):
        observe = self.root / "formation"
        profile, manifest, _ = self.raw(observe / "node1/profile-artifact")
        original = profile.read_bytes(), manifest.read_bytes()
        for node in ("node1", "node2"):
            (observe / node / "network-timeline-artifacts").mkdir(parents=True)
        source = (ROOT / "docker/test-cluster.sh").read_text()
        function = source.split("render_formation_observation_artifacts() {", 1)[1].split(
            "\ncapture_formation_observation() {", 1)[0]
        shell = self.root / "formation-function.sh"
        shell.write_text("set -eu\nrender_formation_observation_artifacts() {" + function +
                         '\nrender_formation_observation_artifacts "$1"\n')
        sentinel = self.root / "go-was-invoked"
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        go = bin_dir / "go"
        go.write_text('#!/bin/sh\ntouch "$GO_SENTINEL"\nexit 77\n')
        go.chmod(0o700)
        environment = dict(os.environ, **{ENV: "1", "SINOMO_ROOT": str(ROOT), "NODES": "node1 node2",
                           "GO_SENTINEL": str(sentinel), "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"]})
        # Two real shell levels exercise ordinary inherited restart environment.
        subprocess.run(["bash", "-c", 'exec bash "$1" "$2"', "nested-restart", str(shell), str(observe)],
                       env=environment, check=True, timeout=10)
        good = json.loads((profile.parent / "pprof-presentation.json").read_text())
        missing = json.loads((observe / "node2/profile-artifact/pprof-presentation.json").read_text())
        self.assertEqual(good["status"], "deferred")
        self.assertEqual(missing["status"], "unavailable")
        self.assertEqual(original, (profile.read_bytes(), manifest.read_bytes()))
        self.assertFalse(sentinel.exists())
        self.assertFalse(list(observe.rglob("flamegraph.html")))

    def test_real_renderer_still_completes_and_drains_owned_launcher(self):
        profile, _, _ = self.raw(self.root)
        original_popen = subprocess.Popen
        children = []

        def track(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            children.append(process)
            return process

        with mock.patch.object(renderer.subprocess, "Popen", side_effect=track):
            written = renderer.render_profile(profile, self.root / "flamegraph.html")
        self.assertEqual(written, (self.root / "flamegraph.html").stat().st_size)
        self.assertTrue(children)
        for process in children:
            self.assertIsNotNone(process.poll(), "renderer must reap its owned launcher")
            OWNED_CHILDREN.append({"pid": process.pid, "returncode": process.returncode})


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(PresentationTests)
    count = suite.countTestCases()
    assert count == 8, count
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    evidence = os.environ.get("SINOMO_PPROF_TEST_EVIDENCE")
    if evidence:
        Path(evidence).write_text(json.dumps({"discovered": count, "executed": result.testsRun,
            "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
            "actual_renderer_launchers": OWNED_CHILDREN}, indent=2) + "\n")
    raise SystemExit(0 if result.wasSuccessful() else 1)
