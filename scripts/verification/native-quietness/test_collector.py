#!/usr/bin/env python3
"""Tests an already-built collector; no daemon control or build inside tests."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class Collector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        value = os.environ.get("SINOMO_NATIVE_COUNTER_BINARY")
        if not value:
            raise RuntimeError("build first and set SINOMO_NATIVE_COUNTER_BINARY to the reviewed collector")
        cls.binary = Path(value).resolve()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.raw = Path(self.temp.name) / "raw.ndjson"

    def run_commands(self, commands):
        r = subprocess.run([str(self.binary), "--raw", str(self.raw)], input=commands,
                           text=True, capture_output=True, timeout=10)
        return r, [json.loads(line) for line in r.stdout.splitlines()]

    def test_build_boot_and_runtime_identity(self):
        r, rows = self.run_commands("QUIT\n")
        self.assertEqual(r.returncode, 0, r.stderr)
        ready = rows[0]
        self.assertEqual(ready["collector_sha256"], hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.assertEqual(ready["boot_id"], Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        self.assertGreater(ready["start_ticks"], 0)
        self.assertFalse(ready["whole_host_scope_verified"])

    def test_live_threads_have_exact_pidfd_epochs_and_counter_bindings(self):
        r, rows = self.run_commands("SAMPLE 0 fixture-0\nQUIT\n")
        self.assertEqual(r.returncode, 0, r.stderr)
        data = [json.loads(line) for line in self.raw.read_text().splitlines()]
        threads = [x for x in data if x["type"] == "thread"]
        observer = [x for x in threads if x["tid"] == rows[0]["pid"]]
        self.assertEqual(len(observer), 1)
        self.assertGreater(observer[0]["thread_epoch"], 0)
        self.assertEqual(observer[0]["start_ticks"], rows[0]["start_ticks"])
        sample = rows[-1]
        self.assertEqual(sample["tasks"], len(threads))
        for row in threads:
            self.assertIn(":".join(map(str, row["counter_file"])), sample["groups"])
        self.assertTrue(all("lo" in g["interfaces"] for g in sample["groups"].values()))
        self.assertFalse(any(sample["coverage"].values()))
        self.assertEqual(self.raw.stat().st_mode & 0o777, 0o600)

    def test_cannot_overwrite_previous_evidence(self):
        self.raw.write_text("previous evidence\n")
        r, rows = self.run_commands("QUIT\n")
        self.assertEqual(r.returncode, 2)
        self.assertFalse(rows)
        self.assertEqual(self.raw.read_text(), "previous evidence\n")

    def test_duplicate_or_skipped_command_sequence_fails(self):
        for command in ("SAMPLE 1 fixture-1\n", "SAMPLE 0 fixture-0\nSAMPLE 0 fixture-0\n"):
            with self.subTest(command=command):
                self.raw.unlink(missing_ok=True)
                r, _ = self.run_commands(command)
                self.assertEqual(r.returncode, 2)
                self.assertIn("discontinuous", r.stderr)

    def test_disconnected_controller_is_incomplete(self):
        r, _ = self.run_commands("SAMPLE 0 fixture-0\n")
        self.assertEqual(r.returncode, 2)
        self.assertIn("incomplete", r.stderr)

    def test_malformed_command_is_not_silently_accepted(self):
        for command in ("SAMPLE 0 token extra\n", "SAMPLE 0 ../token\n", "SAMPLE 0\n"):
            with self.subTest(command=command):
                self.raw.unlink(missing_ok=True)
                r, _ = self.run_commands(command)
                self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main()
