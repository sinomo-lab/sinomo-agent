#!/usr/bin/env python3
"""Counter rejection controls; actual helper/writeback controls are separate."""
import copy
import importlib.util
from pathlib import Path
import unittest
import subprocess
import sys
import os
import tempfile
from unittest.mock import patch

from owned_container_io import IncompleteObservation, OwnedCgroup, io_values

spec = importlib.util.spec_from_file_location("owned_window", Path(__file__).with_name("measure-owned-io.py"))
window = importlib.util.module_from_spec(spec)
spec.loader.exec_module(window)


class CounterTests(unittest.TestCase):
    def test_real_collector_failure_is_retained_after_actual_exit(self):
        process = subprocess.Popen([sys.executable, "-c", "raise SystemExit(7)"], start_new_session=True)
        record = {}
        self.assertEqual(window.completed_legacy_collector(process, record), 7)
        self.assertEqual(record["exit_code"], 7)
        self.assertEqual(record["remaining_group_members"], [])

    def test_collector_exit_does_not_hide_remaining_owned_descendants(self):
        process = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
        record = {}
        with patch.object(window, "collector_group_members", return_value=[{"pid": 42, "start_ticks": 100}]):
            with self.assertRaisesRegex(IncompleteObservation, "owned descendants"):
                window.completed_legacy_collector(process, record)
        self.assertEqual(record["exit_code"], 0)
        self.assertEqual(record["remaining_group_members"], [{"pid": 42, "start_ticks": 100}])

    def setUp(self):
        self.reader = OwnedCgroup.__new__(OwnedCgroup)
        self.reader.path = Path("/sys/fs/cgroup/test-only-no-cgroup-created")
        self.reader.identity = [1, 2]
        self.reader.owner_pid = 42
        self.reader.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        self.reader.expected_memory = 2147483648
        self.before = {
            "path": str(self.reader.path), "identity": [1, 2], "owner_pid": 42,
            "boot_id": self.reader.boot_id, "owner_alive": True, "owner_in_group": True,
            "path_matches": True, "unavailable": {}, "start_ns": 100, "finish_ns": 200,
            "devices": {"259:0": {"diskseq": "1", "leaves": ["259:0"]}},
            "raw": {"io.stat": "259:0 rbytes=10 wbytes=20 rios=1 wios=2\n",
                    "memory.max": "2147483648", "memory.swap.max": "0",
                    "memory.swap.current": "0", "memory.peak": "4096",
                    "memory.events": "high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
                    "memory.stat": "file_dirty 0\nfile_writeback 0\n",
                    "cgroup.events": "populated 1\n"}}
        self.after = copy.deepcopy(self.before)
        self.after.update(start_ns=1200, finish_ns=1300)

    def test_unchanged_live_cut_and_physical_write_delta(self):
        self.assertEqual(self.reader.interval(self.before, self.after)["physical_write_bytes"], 0)
        self.after["raw"]["io.stat"] = "259:0 rbytes=10 wbytes=4116 rios=1 wios=3\n"
        delta = self.reader.interval(self.before, self.after)
        self.assertEqual(delta["physical_write_bytes"], 4096)
        self.assertEqual(delta["minimum_interval_ns"], 1000)

    def test_reused_device_number_requires_same_captured_incarnation(self):
        self.after["devices"]["259:0"]["diskseq"] = "2"
        with self.assertRaisesRegex(IncompleteObservation, "identity changed"):
            self.reader.interval(self.before, self.after)

    def test_owner_loss_migration_replacement_missing_counter_and_reset_rejected(self):
        changes = [
            ("owner_alive", False), ("owner_in_group", False), ("path_matches", False),
            ("identity", [1, 3]), ("owner_pid", 43),
            ("unavailable", {"io.stat": "ENODEV"}),
        ]
        for field, value in changes:
            with self.subTest(field=field):
                after = copy.deepcopy(self.after)
                after[field] = value
                with self.assertRaises(IncompleteObservation):
                    self.reader.interval(self.before, after)
        self.after["raw"]["io.stat"] = "259:0 rbytes=10 wbytes=0 rios=1 wios=0\n"
        with self.assertRaisesRegex(IncompleteObservation, "reset"):
            self.reader.interval(self.before, self.after)

    def test_new_device_is_not_assumed_to_start_at_zero(self):
        self.after["raw"]["io.stat"] += "259:4 rbytes=0 wbytes=0 rios=0 wios=0\n"
        self.after["devices"]["259:4"] = {"diskseq": "2", "leaves": ["259:4"]}
        with self.assertRaisesRegex(IncompleteObservation, "device set"):
            self.reader.interval(self.before, self.after)

    def test_partition_and_disk_overlap_is_not_double_counted(self):
        self.after["raw"]["io.stat"] += "259:1 rbytes=0 wbytes=0 rios=0 wios=0\n"
        self.after["devices"]["259:1"] = {"diskseq": "1", "leaves": ["259:0"]}
        with self.assertRaisesRegex(IncompleteObservation, "overlapping"):
            self.reader.interval(self.before, self.after)

    def test_buffered_work_is_reported_even_without_physical_delta(self):
        self.after["raw"]["memory.stat"] = "file_dirty 4096\nfile_writeback 8192\n"
        delta = self.reader.interval(self.before, self.after)
        self.assertEqual(delta["physical_write_bytes"], 0)
        self.assertEqual(delta["dirty_bytes"], [0, 4096])
        self.assertEqual(delta["writeback_bytes"], [0, 8192])

    def test_missing_metric_duplicate_and_negative_io_rejected(self):
        for raw in ["259:0 rbytes=1 wbytes=1 rios=1", "259:0 rbytes=1 wbytes=-1 rios=1 wios=1",
                    "259:0 rbytes=1 wbytes=1 rios=1 wios=1 wios=2"]:
            with self.subTest(raw=raw), self.assertRaises(IncompleteObservation):
                io_values(raw)

    def test_resource_limit_event_remains_hard_failure(self):
        self.after["raw"]["memory.events"] = "high 0\nmax 1\noom 0\noom_kill 0\noom_group_kill 0\n"
        with self.assertRaisesRegex(IncompleteObservation, "resource event"):
            self.reader.interval(self.before, self.after)

    def test_missing_memory_events_is_not_zero(self):
        self.after["raw"]["memory.events"] = ""
        with self.assertRaisesRegex(IncompleteObservation, "coverage unavailable"):
            self.reader.interval(self.before, self.after)

    def test_external_logger_is_not_in_container_io_coverage(self):
        def record(role, pid, sink):
            return (f"{role} {pid} 10 36:20:100:1:1\n"
                    f"stdout {sink}\nstderr /data/daemon/{role}.stderr.log 8180:36:{pid}2\n"
                    "rchar: 0\nwchar: 0\nread_bytes: 0\nwrite_bytes: 0\n")
        main = record("main", 30, "/data/daemon/main.stdout.log 8180:36:301")
        escaped = main + record("shadow", 1, "pipe:[100] 1180:16:100")
        with patch.object(window, "command", return_value=escaped):
            with self.assertRaisesRegex(IncompleteObservation, "logging escapes"):
                window.daemon_snapshot("selected-container")
        owned = main + record("shadow", 1, "/data/daemon/entrypoint-shadow.stdout.log 8180:36:11")
        with patch.object(window, "command", return_value=owned):
            before = window.daemon_snapshot("selected-container")
        self.assertEqual(window.daemon_delta(before, before)["main"]["write_bytes"], 0)
        after = copy.deepcopy(before)
        after["roles"]["shadow"]["sinks"]["stdout"][2] = "12"
        with self.assertRaisesRegex(IncompleteObservation, "identity changed"):
            window.daemon_delta(before, after)


class MobileWindowDispatchTests(unittest.TestCase):
    def run_window(self, adapter_status, owned=True):
        source = Path(__file__).with_name("test-mobile-idle.sh").read_text()
        functions = source.split("node_write_bytes() {", 1)[1].split("declare -a TARGET_SERVICES=()", 1)[0]
        functions = "node_write_bytes() {" + functions
        with tempfile.TemporaryDirectory(prefix="mobile-window-control-") as root:
            environment = dict(os.environ, CONTROL_ROOT=root,
                               CONTROL_ADAPTER_STATUS=str(adapter_status),
                               SINOMO_DOCKER_DIR=str(Path(__file__).parent.resolve()),
                               COMPOSE_PROJECT_NAME="owned-window-control",
                               MEASURE_NODES="node4 node5 node6", QUIESCE_WAIT="10",
                               QUIESCE_WINDOW="135")
            if owned:
                environment["SINOMO_GATE_OWNED_IO_ARTIFACTS"] = root + "/evidence"
            else:
                environment.pop("SINOMO_GATE_OWNED_IO_ARTIFACTS", None)
            controls = r"""
sleep() { printf 'sleep %s\n' "$*" >> "$CONTROL_ROOT/calls"; }
python3() {
  printf 'adapter\n' >> "$CONTROL_ROOT/calls"
  printf '%s\n' "$@" > "$CONTROL_ROOT/adapter-args"
  return "$CONTROL_ADAPTER_STATUS"
}
node_write_bytes() {
  printf 'legacy %s\n' "$1" >> "$CONTROL_ROOT/calls"
  printf '17\n'
}
enforce_idle_write_stability "battery-constrained role transition"
printf 'continued\n' >> "$CONTROL_ROOT/calls"
"""
            result = subprocess.run(["bash", "-c", "set -euo pipefail\n" + functions + controls],
                                    env=environment, text=True, capture_output=True)
            calls = Path(root, "calls").read_text().splitlines()
            args_file = Path(root, "adapter-args")
            args = args_file.read_text().splitlines() if args_file.exists() else []
            stages = list(Path(root).glob("evidence/mobile-stability-*/stage.txt"))
            stage_text = [p.read_text().strip() for p in stages]
            return result, calls, args, stage_text

    def test_owned_mobile_window_preserves_one_stabilization_and_exact_window(self):
        result, calls, args, stages = self.run_window(0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["sleep 10", "adapter", "continued"])
        self.assertEqual(args[0], str(Path(__file__).with_name("measure-owned-io.py").resolve()))
        self.assertEqual(args[1:9], ["--project", "owned-window-control", "--nodes",
                                   "node4", "node5", "node6", "--window", "135"])
        self.assertEqual(args[9], "--output")
        self.assertTrue(args[10].endswith("/io.json"))
        self.assertEqual(stages, ["battery-constrained role transition"])

    def test_owned_mobile_writes_and_missing_coverage_both_stop_without_fallback(self):
        for status in (1, 2):
            with self.subTest(status=status):
                result, calls, args, stages = self.run_window(status)
                self.assertEqual(result.returncode, status)
                self.assertEqual(calls, ["sleep 10", "adapter"])
                self.assertEqual(stages, ["battery-constrained role transition"])
                self.assertIn("no retry or deadline reset", result.stdout)

    def test_nonowned_mobile_window_keeps_original_single_proc_window(self):
        result, calls, args, stages = self.run_window(0, owned=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["sleep 10", "legacy node4", "legacy node5", "legacy node6",
                                 "sleep 135", "legacy node4", "legacy node5", "legacy node6",
                                 "continued"])
        self.assertEqual(args, [])
        self.assertEqual(stages, [])


if __name__ == "__main__":
    unittest.main()
