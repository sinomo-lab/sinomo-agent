#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("fixtures", Path(__file__).with_name("run-with-test-fixtures.py"))
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


class FixtureLifecycle(unittest.TestCase):
    def setUp(self):
        durable = Path(__file__).resolve().parents[2] / "target" / "fixture-lifecycle-tests"
        durable.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="sinomo-lifecycle-control-")
        self.artifacts = tempfile.TemporaryDirectory(prefix="artifacts-", dir=durable)
        self.root = Path(self.temp.name)
        self.artifact_root = Path(self.artifacts.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.artifacts.cleanup)

    def test_completed_success_reclaims_only_its_private_root(self):
        sibling = self.root / "retained-other-run"
        sibling.mkdir()
        (sibling / "evidence").write_bytes(b"preserve")
        out = self.artifact_root / "success"
        code = fixtures.run([sys.executable, "-c", "import os,pathlib; pathlib.Path(os.environ['TMPDIR'],'bytes').write_bytes(b'ok')"], out, self.root)
        self.assertEqual(code, 0)
        receipt = json.loads((out / "result.json").read_text())
        self.assertTrue(receipt["reclaimed"])
        self.assertFalse(Path(receipt["temporary_root"]).exists())
        self.assertEqual((sibling / "evidence").read_bytes(), b"preserve")

    def test_failure_retains_exact_bytes_links_and_exit_status_in_durable_archive(self):
        out = self.artifact_root / "failure"
        script = "import os,pathlib; p=pathlib.Path(os.environ['TMPDIR']); (p/'evidence').write_bytes(b'failed-bytes'); os.link(p/'evidence',p/'hardlink'); (p/'symlink').symlink_to('evidence'); raise SystemExit(37)"
        self.assertEqual(fixtures.run([sys.executable, "-c", script], out, self.root), 37)
        receipt = json.loads((out / "result.json").read_text())
        original = Path(receipt["temporary_root"])
        self.assertFalse(original.exists())
        with tarfile.open(receipt["archive"]["archive"]) as archive:
            self.assertEqual(archive.extractfile(original.name + "/evidence").read(), b"failed-bytes")
            self.assertEqual(archive.extractfile(original.name + "/hardlink").read(), b"failed-bytes")
            self.assertEqual(archive.getmember(original.name + "/symlink").linkname, "evidence")

    def test_failed_archival_does_not_reclaim_original(self):
        out = self.artifact_root / "archive-failure"
        with patch.object(fixtures, "archive_verified", side_effect=RuntimeError("verification failed")):
            self.assertEqual(fixtures.run([sys.executable, "-c", "raise SystemExit(3)"], out, self.root), 3)
        receipt = json.loads((out / "result.json").read_text())
        self.assertFalse(receipt["reclaimed"])
        self.assertTrue(Path(receipt["temporary_root"]).is_dir())
        self.assertIn("verification failed", receipt["preservation_error"])

    def test_deferred_failure_keeps_original_until_separate_verified_finalization(self):
        out = self.artifact_root / "deferred"
        script = "import os,pathlib; pathlib.Path(os.environ['TMPDIR'],'evidence').write_bytes(b'keep-me'); raise SystemExit(37)"
        self.assertEqual(fixtures.run([sys.executable, "-c", script], out, self.root,
                                      defer_cleanup=True), 37)
        pending = json.loads((out / "pending-cleanup.json").read_text())
        root = Path(pending["temporary_root"])
        self.assertEqual((root / "evidence").read_bytes(), b"keep-me")
        self.assertFalse((out / "result.json").exists())
        receipt = fixtures.finalize(out)
        self.assertEqual(receipt["command_exit_code"], 37)
        self.assertTrue(receipt["reclaimed"])
        with tarfile.open(receipt["archive"]["archive"]) as archive:
            self.assertEqual(archive.extractfile(root.name + "/evidence").read(), b"keep-me")
        original_receipt = (out / "result.json").read_bytes()
        with self.assertRaisesRegex(RuntimeError, "finalization already recorded"):
            fixtures.finalize(out)
        self.assertEqual((out / "result.json").read_bytes(), original_receipt)

    def test_deferred_finalization_rejects_replacement_root_and_preserves_both(self):
        out = self.artifact_root / "replacement"
        self.assertEqual(fixtures.run([sys.executable, "-c", "pass"], out, self.root,
                                      defer_cleanup=True), 0)
        pending = json.loads((out / "pending-cleanup.json").read_text())
        root = Path(pending["temporary_root"])
        retained = root.with_name(root.name + "-original")
        root.rename(retained)
        root.mkdir()
        (root / "unrelated").write_bytes(b"preserve")
        receipt = fixtures.finalize(out)
        self.assertFalse(receipt["reclaimed"])
        self.assertIn("ownership changed", receipt["preservation_error"])
        self.assertTrue(retained.is_dir())
        self.assertEqual((root / "unrelated").read_bytes(), b"preserve")

    def test_deferred_finalization_checks_live_use_again(self):
        out = self.artifact_root / "live"
        self.assertEqual(fixtures.run([sys.executable, "-c", "pass"], out, self.root,
                                      defer_cleanup=True), 0)
        with patch.object(fixtures, "active_uses", return_value=[{"pid": 42}]):
            receipt = fixtures.finalize(out)
        self.assertFalse(receipt["reclaimed"])
        self.assertTrue(Path(receipt["temporary_root"]).is_dir())

    def test_active_process_reference_prevents_cleanup(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1)"], cwd=self.root)
        try:
            self.assertTrue(fixtures.active_uses(self.root))
        finally:
            proc.wait(timeout=5)

    def test_retained_container_mount_prevents_cleanup(self):
        def output(command, **kwargs):
            if command[:3] == ["docker", "ps", "-aq"]:
                return "fixture-container\n"
            return json.dumps([{"Source": str(self.root), "Destination": "/data"}]) + "\n"
        with patch.object(fixtures.shutil, "which", return_value="docker"), patch.object(fixtures.subprocess, "check_output", side_effect=output):
            uses = fixtures.active_uses(self.root)
        self.assertTrue(any(row.get("container") == "fixture-container" for row in uses))

    def test_removed_container_does_not_hide_another_retained_mount(self):
        def output(command, **kwargs):
            if command[:3] == ["docker", "ps", "-aq"]:
                return "removed-container\nretained-container\n"
            if command[-1] == "removed-container":
                raise subprocess.CalledProcessError(
                    1, command, stderr="Error: No such object: removed-container\n")
            return json.dumps([{"Source": str(self.root), "Destination": "/data"}])
        with patch.object(fixtures.shutil, "which", return_value="docker"), patch.object(
                fixtures.subprocess, "check_output", side_effect=output):
            uses = fixtures.active_uses(self.root)
        self.assertTrue(any(row.get("container") == "retained-container" for row in uses))
        self.assertFalse(any(row.get("container") == "removed-container" for row in uses))

    def test_unavailable_container_inspection_preserves_original(self):
        original_output = fixtures.subprocess.check_output
        def output(command, **kwargs):
            if command[:3] == ["docker", "ps", "-aq"]:
                return "unknown-container\n"
            if command[:2] == ["docker", "inspect"]:
                raise subprocess.CalledProcessError(
                    1, command, stderr="Cannot connect to the Docker daemon")
            return original_output(command, **kwargs)
        out = self.artifact_root / "docker-unavailable"
        with patch.object(fixtures.shutil, "which", return_value="docker"), patch.object(
                fixtures.subprocess, "check_output", side_effect=output):
            self.assertEqual(fixtures.run([sys.executable, "-c", "pass"], out, self.root), 1)
        receipt = json.loads((out / "result.json").read_text())
        self.assertEqual(receipt["command_exit_code"], 0)
        self.assertFalse(receipt["reclaimed"])
        self.assertTrue(Path(receipt["temporary_root"]).is_dir())


if __name__ == "__main__":
    unittest.main()
