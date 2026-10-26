#!/usr/bin/env python3
"""Exercise the real shell gate's retry decisions without launching Docker."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class FixedWindowTests(unittest.TestCase):
    def run_gate(self, statuses, strict, owned=True):
        source = Path(__file__).with_name("measure-final.sh").read_text()
        gate = source.split("enforce_idle_write_stability() {", 1)[1].split(
            "\nrun_validation_cycle() {", 1
        )[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = dict(os.environ, FIXTURE_ROOT=directory,
                               STATUSES=" ".join(map(str, statuses)))
            script = """
set -euo pipefail
QUIESCE_WAIT=10 QUIESCE_WINDOW=135 QUIESCE_TIMEOUT=600
MEASURE_NODES='node1 node2 node3 node4 node5 node6'
COMPOSE_PROJECT_NAME=fixture SINOMO_DOCKER_DIR=fixture
sleep() { echo "$1" >> "$FIXTURE_ROOT/sleeps"; }
date() { echo 1000; }
python3() {
  echo "$*" >> "$FIXTURE_ROOT/calls"
  local count status
  count=$(wc -l < "$FIXTURE_ROOT/calls")
  status=$(echo "$STATUSES" | cut -d ' ' -f "$count")
  [ -n "$status" ] || return 99
  return "$status"
}
node_daemon_write_snapshot() {
  echo "$1" >> "$FIXTURE_ROOT/snapshots"
  wc -l < "$FIXTURE_ROOT/snapshots"
}
daemon_snapshot_identity() { echo same-incarnation; }
daemon_snapshot_write_bytes() { echo "$1"; }
"""
            script += f"NO_QUIET_RETRY={int(strict)}\n"
            if owned:
                script += 'SINOMO_GATE_OWNED_IO_ARTIFACTS="$FIXTURE_ROOT/owned"\n'
            script += "enforce_idle_write_stability() {" + gate
            script += '\nenforce_idle_write_stability "fixture"\n'
            result = subprocess.run(["bash", "-c", script], env=environment,
                                    capture_output=True, text=True, timeout=5)
            calls = (root / "calls").read_text().splitlines() if (root / "calls").exists() else []
            sleeps = (root / "sleeps").read_text().splitlines()
            return result, calls, sleeps

    def test_first_failed_owned_window_stops_without_changing_its_bound(self):
        result, calls, sleeps = self.run_gate([1, 0], True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("--window 135", calls[0])
        self.assertIn("node1 node2 node3 node4 node5 node6", calls[0])
        self.assertEqual(sleeps, ["10"])

    def test_successful_first_window_passes(self):
        result, calls, _ = self.run_gate([0], True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 1)

    def test_incomplete_measurement_never_retries(self):
        for strict in [False, True]:
            result, calls, _ = self.run_gate([2, 0], strict)
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertEqual(len(calls), 1)

    def test_existing_default_retry_remains_explicit(self):
        result, calls, sleeps = self.run_gate([1, 0], False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, ["10"])

    def test_legacy_counter_failure_also_stops_at_first_window(self):
        result, calls, sleeps = self.run_gate([], True, owned=False)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(calls, [])
        self.assertEqual(sleeps, ["10", "135"])


if __name__ == "__main__":
    unittest.main()
