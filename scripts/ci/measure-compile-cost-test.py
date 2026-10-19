#!/usr/bin/env python3
"""Fixture and small Python subprocess tests; never invoke a compiler or Docker."""

import contextlib
import copy
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("measure-compile-cost.py")
spec = importlib.util.spec_from_file_location("measure_compile_cost", SCRIPT)
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def stat_line(pid, name="cargo", *, ppid=1, session=100, start=123, rss_pages=4):
    # Fields after comm begin at Linux stat field 3 (state).
    fields = ["0"] * 22
    fields[0], fields[1], fields[3] = "S", str(ppid), str(session)
    fields[19], fields[20], fields[21] = str(start), "1048576", str(rss_pages)
    return f"{pid} ({name}) " + " ".join(fields) + "\n"


def snapshot(rss, pss, *, missing=0, scan_errors=0, count=2, available=1000):
    return {"tree_rss_bytes": rss, "tree_pss_bytes": pss,
            "pss_missing_processes": missing, "proc_scan_errors": scan_errors,
            "processes": [{} for _ in range(count)],
            "host_mem_available_bytes": available}


class ProcessAccountingTests(unittest.TestCase):
    def test_event_deltas_do_not_invent_zero_for_missing_or_reset_counters(self):
        self.assertIsNone(measure.counter_delta(None, "max 0\n"))
        self.assertIsNone(measure.counter_delta("", ""))
        self.assertIsNone(measure.counter_delta("max 1\n", "max 0\n"))
        self.assertIsNone(measure.counter_delta("max 0\n", "max 0\noom 0\n"))
        self.assertEqual(measure.counter_delta("max 4\noom 1\n", "max 7\noom 1\n"),
                         {"max": 3, "oom": 0})
    def test_gnu_time_hours_and_minutes_preserve_workload_values(self):
        for elapsed, seconds in (("1:02:03.45", 3723.45), ("2:03.45", 123.45), ("0:00.07", 0.07)):
            with self.subTest(elapsed=elapsed):
                text = ("\tCommand being timed: \"python -c print('literal: value')\"\n"
                        "\tUser time (seconds): 4.12\n"
                        "\tSystem time (seconds): 0.56\n"
                        f"\tElapsed (wall clock) time (h:mm:ss or m:ss): {elapsed}\n"
                        "\tMaximum resident set size (kbytes): 98765\n"
                        "\tExit status: 7\n")
                result = measure.parse_gnu_time(text)
                self.assertAlmostEqual(result["wall_time_s"], seconds)
                self.assertEqual(result["largest_child_max_rss_kib"], 98765)
                self.assertEqual(result["user_cpu_s"], 4.12)
                self.assertEqual(result["system_cpu_s"], 0.56)
                self.assertEqual(result["exit_code"], 7)

    def test_missing_gnu_time_values_are_unavailable_and_zero_is_measured(self):
        missing = measure.parse_gnu_time("Command exited with non-zero status 7\n")
        self.assertEqual(missing, {"wall_time_s": None, "largest_child_max_rss_kib": None,
                                   "user_cpu_s": None, "system_cpu_s": None, "exit_code": None})
        partial = measure.parse_gnu_time("\tUser time (seconds): 0.00\n\tExit status: 0\n")
        self.assertEqual(partial["user_cpu_s"], 0.0)
        self.assertEqual(partial["exit_code"], 0)
        self.assertIsNone(partial["wall_time_s"])
        self.assertIsNone(partial["largest_child_max_rss_kib"])

    def test_stat_identity_with_whitespace_and_closing_parenthesis(self):
        row = measure.parse_stat(stat_line(101, "rustc worker) with spaces",
                                          ppid=98, session=97, start=654321, rss_pages=17))
        self.assertEqual((row["pid"], row["name"], row["ppid"], row["session"], row["start_ticks"]),
                         (101, "rustc worker) with spaces", 98, 97, 654321))
        self.assertEqual(row["rss_bytes"], 17 * os.sysconf("SC_PAGE_SIZE"))

    def test_memory_units_are_not_mixed(self):
        values = measure.parse_kib_values("Pss: 3 kB\nRss: 7 kB\nThreads: 2\nName: rustc\n")
        self.assertEqual(values, {"Pss": 3072, "Rss": 7168, "Threads": 2})

    def test_session_reparenting_and_descendants_exclude_unrelated_processes(self):
        rows = {pid: measure.parse_stat(text) for pid, text in {
            100: stat_line(100),
            110: stat_line(110, ppid=100),
            120: stat_line(120, ppid=110, session=900),
            130: stat_line(130, ppid=1),
            200: stat_line(200, ppid=1, session=200),
            210: stat_line(210, ppid=200, session=200),
        }.items()}
        self.assertEqual({r["pid"] for r in measure.select_processes(100, rows)},
                         {100, 110, 120, 130})

    def write_process(self, root, pid, *, ppid, session, rss_pages, pss_kib):
        directory = root / str(pid)
        directory.mkdir()
        (directory / "stat").write_text(stat_line(pid, ppid=ppid, session=session,
                                                   rss_pages=rss_pages))
        (directory / "cmdline").write_bytes(b"fake-python\0literal argument\0")
        if pss_kib is not None:
            (directory / "smaps_rollup").write_text(f"Pss: {pss_kib} kB\n")

    def test_fixture_aggregate_and_missing_pss_remain_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            (proc / "meminfo").write_text("MemAvailable: 1234 kB\n")
            self.write_process(proc, 100, ppid=1, session=100, rss_pages=4, pss_kib=5)
            self.write_process(proc, 110, ppid=100, session=100, rss_pages=8, pss_kib=None)
            self.write_process(proc, 200, ppid=1, session=200, rss_pages=100, pss_kib=100)
            missing = measure.process_sample(100, 0.5, [], proc)
            self.assertEqual(missing["tree_rss_bytes"], 12 * os.sysconf("SC_PAGE_SIZE"))
            self.assertIsNone(missing["tree_pss_bytes"])
            self.assertEqual(missing["pss_known_bytes"], 5 * 1024)
            self.assertEqual(missing["pss_missing_processes"], 1)
            self.assertEqual(missing["host_mem_available_bytes"], 1234 * 1024)
            self.assertEqual(missing["processes"][0]["argv"], ["fake-python", "literal argument"])
            (proc / "110/smaps_rollup").write_text("Pss: 11 kB\n")
            complete = measure.process_sample(100, 0.6, [], proc)
            self.assertEqual(complete["tree_pss_bytes"], 16 * 1024)
            self.assertEqual(complete["pss_missing_processes"], 0)

    def test_exit_during_scan_is_an_error_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            self.write_process(proc, 100, ppid=1, session=100, rss_pages=4, pss_kib=1)
            (proc / "110").mkdir()  # A process exited after /proc enumeration.
            result = measure.process_sample(100, 0, [], proc)
            self.assertEqual(result["proc_scan_errors"], 1)
            self.assertEqual({row["pid"] for row in result["processes"]}, {100})

    def test_smaps_rss_is_preferred_to_approximate_stat_rss(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            self.write_process(proc, 100, ppid=1, session=100, rss_pages=100, pss_kib=9)
            (proc / "100/smaps_rollup").write_text("Rss: 13 kB\nPss: 9 kB\n")
            result = measure.process_sample(100, 0, [], proc)
            self.assertEqual(result["tree_rss_bytes"], 13 * 1024)
            self.assertEqual(result["tree_pss_bytes"], 9 * 1024)
            self.assertEqual(result["processes"][0]["rss_source"], "smaps_rollup")
            self.assertTrue(result["processes"][0]["identity_stable"])

    def test_pid_reuse_during_smaps_read_never_attributes_new_process_memory(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            self.write_process(proc, 100, ppid=1, session=100, rss_pages=4, pss_kib=1)
            real_read = measure.read_optional

            def reused(path):
                if path == proc / "100/smaps_rollup":
                    (proc / "100/stat").write_text(stat_line(100, "unrelated", session=999,
                                                              start=99999, rss_pages=1000))
                    return "Rss: 10000 kB\nPss: 9000 kB\n"
                return real_read(path)

            with mock.patch.object(measure, "read_optional", side_effect=reused):
                result = measure.process_sample(100, 0, [], proc)
            row = result["processes"][0]
            self.assertFalse(row["identity_stable"])
            self.assertIsNone(row["pss_bytes"])
            self.assertEqual(row["rss_bytes"], 4 * os.sysconf("SC_PAGE_SIZE"))
            self.assertEqual(row["rss_source"], "stat-before-identity-change-approximate")
            self.assertIsNone(result["tree_pss_bytes"])
            self.assertEqual(result["pss_missing_processes"], 1)
            self.assertLessEqual(result["tree_rss_bytes"], 4 * os.sysconf("SC_PAGE_SIZE"))

    def test_peaks_are_maxima_of_simultaneous_sums_with_coverage(self):
        result = measure.summarize_samples([
            snapshot(100, 70, available=1000),
            snapshot(130, None, missing=1, scan_errors=2, count=3, available=800),
            snapshot(90, 80, available=None),
        ])
        self.assertEqual(result["sampled_tree_peak_rss_bytes"], 130)
        self.assertEqual(result["sampled_tree_peak_pss_bytes"], 80)
        self.assertEqual(result["complete_pss_samples"], 2)
        self.assertEqual(result["pss_missing_process_observations"], 1)
        self.assertEqual(result["proc_scan_errors"], 2)
        self.assertEqual(result["process_count_peak"], 3)
        self.assertEqual(result["host_min_mem_available_bytes"], 800)
        unavailable = measure.summarize_samples([snapshot(12, None, missing=1)])
        self.assertIsNone(unavailable["sampled_tree_peak_pss_bytes"])
        empty = measure.summarize_samples([])
        self.assertIsNone(empty["sampled_tree_peak_rss_bytes"])
        self.assertIsNone(empty["sampled_tree_peak_pss_bytes"])

    def test_cgroup_ancestors_and_absent_counters_are_read_only_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc, cgroup = root / "proc", root / "cgroup"
            (proc / "self").mkdir(parents=True)
            (proc / "self/cgroup").write_text("0::/user.slice/build.scope\n")
            leaf = cgroup / "user.slice/build.scope"
            leaf.mkdir(parents=True)
            (leaf / "memory.current").write_text("50\n")
            (leaf / "memory.peak").write_text("10000000\n")
            (cgroup / "memory.max").write_text("999\n")
            paths = measure.cgroup_paths(proc, cgroup)
            self.assertEqual(paths, [leaf, cgroup / "user.slice", cgroup])
            records = measure.cgroup_snapshot(paths)
            self.assertEqual(records[0]["memory.current"], "50")
            self.assertEqual(records[0]["memory.peak"], "10000000")
            self.assertIsNone(records[0]["memory.max"])
            self.assertEqual(records[-1]["memory.max"], "999")
            self.assertEqual((leaf / "memory.peak").read_text(), "10000000\n")
            dedicated = measure.cgroup_snapshot(paths, dedicated_leaf=True)
            self.assertEqual(dedicated[0]["classification"], "dedicated-observer-and-workload")
            self.assertEqual([row["classification"] for row in dedicated[1:]], ["shared", "shared"])
            self.assertTrue(all(row["classification"] == "shared" for row in records))


class FakeCommandTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.output, self.target, self.lock = (self.root / name for name in ("output", "target", "lock"))
        # Git metadata is a fixture; only the measured command executes for real.
        self.identity = {"repo": str(self.repo), "head": "fixture-head", "branch": "fixture",
                         "tracked_patch_sha256": "fixture-patch", "status": "",
                         "lockfile_sha256": "fixture-lock", "root_manifest_sha256": "fixture-manifest"}
        self.observers = []
        self.owned_groups = []

    def tearDown(self):
        for group, members in self.owned_groups:
            for pid, start in members:
                try:
                    row = measure.parse_stat(Path(f"/proc/{pid}/stat").read_text())
                    if row["start_ticks"] == start and row["session"] == group:
                        os.killpg(group, signal.SIGKILL)
                        break
                except (ProcessLookupError, FileNotFoundError):
                    pass
        for observer in self.observers:
            if observer.poll() is None:
                observer.terminate()
                try:
                    observer.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    observer.kill()
                    observer.wait(timeout=1)
            observer.communicate()
        self.temporary.cleanup()

    def arguments(self, command, *, cache_state="unknown-warm", extra=()):
        return ["--repo", str(self.repo), "--label", "fake-command", "--kind", "custom",
                "--profile", "fixture", "--cache-state", cache_state,
                "--target-dir", str(self.target), "--output", str(self.output),
                "--lock", str(self.lock), "--interval", "0.01", "--skip-tool-versions",
                *extra, "--", *command]

    def invoke(self, command, *, cache_state="unknown-warm", identities=None, extra=()):
        args = self.arguments(command, cache_state=cache_state, extra=extra)
        source = identities or [copy.deepcopy(self.identity), copy.deepcopy(self.identity)]
        with mock.patch.object(measure, "source_identity", side_effect=source), \
                mock.patch.object(measure.subprocess, "run", return_value=subprocess.CompletedProcess(
                    ["git", "diff"], 0, stdout=b"")), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return measure.main(args)

    def summary(self):
        return json.loads((self.output / "summary.json").read_text())

    def spawn_observer(self, command):
        # Real observer signals must run outside unittest's own process. Git and
        # tool versions remain fixtures, and cancellation budgets are shortened.
        wrapper = ("import importlib.util,subprocess,sys; "
                   "spec=importlib.util.spec_from_file_location('observer',sys.argv[1]); "
                   "m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); "
                   "m.source_identity=lambda repo: " + repr(self.identity) + "; "
                   "m.subprocess.run=lambda *a,**kw: subprocess.CompletedProcess([],0,stdout=b''); "
                   "m.CANCEL_TERM_GRACE_S=0.15; m.CANCEL_KILL_WAIT_S=0.3; "
                   "sys.exit(m.main(sys.argv[2:]))")
        observer = subprocess.Popen([sys.executable, "-c", wrapper, str(SCRIPT), *self.arguments(command)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.observers.append(observer)
        return observer

    def wait_ready(self, observer, path):
        deadline = time.monotonic() + 2
        while True:
            if path.exists():
                try:
                    ready = json.loads(path.read_text())
                    break
                except json.JSONDecodeError:
                    pass  # The fake process may still be writing its small marker.
            if observer.poll() is not None:
                stdout, stderr = observer.communicate()
                self.fail(f"observer exited before workload ready: {stdout!r}, {stderr!r}")
            if time.monotonic() >= deadline:
                self.fail("fake Python workload did not become ready")
            time.sleep(0.01)
        members = []
        for key in ("pid", "descendant_pid"):
            if key in ready:
                pid = ready[key]
                row = measure.parse_stat(Path(f"/proc/{pid}/stat").read_text())
                members.append((pid, row["start_ticks"]))
        self.owned_groups.append((ready["pid"], members))
        return ready

    def test_success_keeps_evidence_and_literal_argv_without_a_shell(self):
        marker = self.root / "shell-must-not-run"
        literal = f"spaces; $(touch {marker}) `touch {marker}` $HOME\nsecond line"
        command = [sys.executable, "-c",
                   "import json,os,sys; print(json.dumps(sys.argv[1:])); "
                   "print(os.environ['CARGO_TARGET_DIR']); print('diagnostic', file=sys.stderr)",
                   literal, "--leading-option"]
        self.assertEqual(self.invoke(command, cache_state="empty-target"), 0)
        result = self.summary()
        lines = (self.output / "stdout.log").read_text().splitlines()
        self.assertEqual(json.loads(lines[0]), [literal, "--leading-option"])
        self.assertEqual(lines[1], str(self.target))
        self.assertIn("diagnostic", (self.output / "stderr.log").read_text())
        self.assertFalse(marker.exists())
        self.assertEqual(result["command"], command)
        self.assertEqual(result["cache_state"], "empty-target")
        self.assertTrue(result["tracked_source_stable"])
        self.assertIsNone(result["gnu_time"])
        self.assertEqual(result["cargo_timings"], [])
        self.assertGreater(result["wait4"]["largest_child_max_rss_kib"], 0)
        self.assertGreater(result["measurement"]["sample_count"], 0)
        self.assertNotIn("tools", result)

    def test_nonzero_exit_preserves_summary_and_releases_lock(self):
        self.assertEqual(self.invoke([sys.executable, "-c", "raise SystemExit(7)"]), 7)
        self.assertEqual(self.summary()["exit_code"], 7)
        with self.lock.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_signal_exit_is_preserved_and_mapped_for_shell(self):
        command = [sys.executable, "-c", "import os,signal; os.kill(os.getpid(), signal.SIGTERM)"]
        self.assertEqual(self.invoke(command), 128 + signal.SIGTERM)
        self.assertEqual(self.summary()["exit_code"], -signal.SIGTERM)

    def test_lock_contention_never_spawns_or_replaces_live_lock(self):
        self.lock.write_text("another owner\n")
        inode = self.lock.stat().st_ino
        with self.lock.open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with mock.patch.object(measure.subprocess, "Popen") as popen:
                self.assertEqual(self.invoke([sys.executable, "-c", "raise SystemExit(0)"]), 75)
                popen.assert_not_called()
        self.assertEqual(self.lock.stat().st_ino, inode)
        self.assertEqual(self.lock.read_text(), "another owner\n")
        self.assertFalse(self.output.exists())
        self.assertFalse(self.target.exists())

    def test_empty_target_validation_preserves_existing_artifacts(self):
        self.target.mkdir()
        artifact = self.target / "existing-artifact"
        artifact.write_bytes(b"retain me")
        with mock.patch.object(measure.subprocess, "Popen") as popen:
            with self.assertRaises(SystemExit) as error:
                self.invoke([sys.executable, "-c", "pass"], cache_state="empty-target")
            self.assertEqual(error.exception.code, 2)
            popen.assert_not_called()
        self.assertEqual(artifact.read_bytes(), b"retain me")
        self.assertFalse(self.output.exists())

    def test_existing_output_evidence_is_never_overwritten(self):
        self.output.mkdir()
        prior = self.output / "summary.json"
        prior.write_text("prior measurement\n")
        with mock.patch.object(measure.subprocess, "Popen") as popen:
            with self.assertRaises(SystemExit) as error:
                self.invoke([sys.executable, "-c", "pass"])
            self.assertEqual(error.exception.code, 2)
            popen.assert_not_called()
        self.assertEqual(prior.read_text(), "prior measurement\n")

    def test_missing_time_binary_is_rejected_before_command_or_evidence_creation(self):
        with mock.patch.object(measure.subprocess, "Popen") as popen:
            with self.assertRaises(SystemExit) as error:
                self.invoke([sys.executable, "-c", "pass"], extra=("--time-bin", str(self.root / "absent")))
            self.assertEqual(error.exception.code, 2)
            popen.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_tracked_source_drift_is_reported_without_hiding_command_success(self):
        before, after = copy.deepcopy(self.identity), copy.deepcopy(self.identity)
        after["tracked_patch_sha256"] = "changed-source"
        self.assertEqual(self.invoke([sys.executable, "-c", "pass"], identities=[before, after]), 0)
        self.assertFalse(self.summary()["tracked_source_stable"])

    def test_real_small_python_children_are_sampled_concurrently(self):
        child = "import time; a=bytearray(8*1024*1024); a[::4096]=b'x'*2048; time.sleep(0.25)"
        parent = ("import subprocess,sys; code=" + repr(child) + "; "
                  "children=[subprocess.Popen([sys.executable,'-c',code]) for _ in range(2)]; "
                  "[p.wait() for p in children]")
        self.assertEqual(self.invoke([sys.executable, "-c", parent]), 0)
        result = self.summary()
        self.assertGreaterEqual(result["measurement"]["process_count_peak"], 3)
        self.assertGreater(result["measurement"]["sampled_tree_peak_rss_bytes"], 16 * 1024 * 1024)
        samples = [json.loads(line) for line in (self.output / "process-samples.jsonl").read_text().splitlines()]
        self.assertTrue(any(len(sample["processes"]) >= 3 for sample in samples))
        self.assertEqual(result["measurement"]["sampled_tree_peak_rss_bytes"],
                         max(sample["tree_rss_bytes"] for sample in samples))

    def test_launch_failure_retains_unknown_workload_status_and_restores_handlers(self):
        before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
        self.assertEqual(self.invoke([str(self.root / "missing-executable")]), 1)
        result = self.summary()
        self.assertIsNone(result["exit_code"])
        self.assertEqual(result["observer_exit_code"], 1)
        self.assertIsNone(result["wait4"])
        self.assertFalse(result["measurement_complete"])
        self.assertEqual(result["errors"][0]["type"], "FileNotFoundError")
        self.assertEqual(before, {s: signal.getsignal(s) for s in before})

    def test_sampling_failure_kills_stubborn_child_and_retains_partial_evidence(self):
        ready = self.root / "ready"
        child = ("import os,signal,time; from pathlib import Path; "
                 "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                 f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(5)")

        def fail_after_ready(*_args):
            deadline = time.monotonic() + 2
            while not ready.exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError("fake child did not start")
                time.sleep(0.01)
            raise RuntimeError("fixture observer sampling failure")

        before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
        with mock.patch.object(measure, "process_sample", side_effect=fail_after_ready), \
                mock.patch.object(measure, "CANCEL_TERM_GRACE_S", 0.05), \
                mock.patch.object(measure, "CANCEL_KILL_WAIT_S", 0.3):
            self.assertEqual(self.invoke([sys.executable, "-c", child]), 1)
        result = self.summary()
        self.assertEqual(result["exit_code"], -signal.SIGKILL)
        self.assertEqual(result["observer_exit_code"], 1)
        self.assertFalse(result["measurement_complete"])
        self.assertTrue(result["cleanup"]["kill_sent"])
        self.assertTrue(result["cleanup"]["root_reaped"])
        self.assertIn("fixture observer sampling failure", result["errors"][0]["message"])
        self.assertIsNotNone(result["wait4"])
        self.assertTrue((self.output / "process-samples.jsonl").exists())
        self.assertEqual(before, {s: signal.getsignal(s) for s in before})
        with self.lock.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_observer_term_hup_int_cancel_workload_and_keep_summary(self):
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            with self.subTest(signum=signum):
                self.output = self.root / f"output-{signum}"
                ready_path = self.root / f"ready-{signum}"
                child = ("import json,os,time; from pathlib import Path; "
                         f"Path({str(ready_path)!r}).write_text(json.dumps({{'pid':os.getpid()}})); "
                         "time.sleep(5)")
                observer = self.spawn_observer([sys.executable, "-c", child])
                self.wait_ready(observer, ready_path)
                observer.send_signal(signum)
                self.assertEqual(observer.wait(timeout=3), 128 + signum)
                result = self.summary()
                self.assertEqual(result["interruption_signal"], signum)
                self.assertFalse(result["measurement_complete"])
                self.assertEqual(result["exit_code"], -signal.SIGTERM)
                self.assertTrue(result["cleanup"]["term_sent"])
                self.assertTrue(result["cleanup"]["root_reaped"])

    def test_stubborn_descendant_is_killed_even_after_immediate_child_exits(self):
        ready_path, descendant_ready = self.root / "ready", self.root / "descendant-ready"
        descendant = ("import os,signal,time; from pathlib import Path; "
                      "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                      f"Path({str(descendant_ready)!r}).write_text(str(os.getpid())); time.sleep(5)")
        child = ("import json,os,subprocess,sys,time; from pathlib import Path; "
                 f"p=subprocess.Popen([sys.executable,'-c',{descendant!r}],close_fds=False); "
                 f"marker=Path({str(descendant_ready)!r}); "
                 "exec('while not marker.exists(): time.sleep(0.01)'); "
                 f"Path({str(ready_path)!r}).write_text(json.dumps({{'pid':os.getpid(),'descendant_pid':p.pid}})); "
                 "time.sleep(5)")
        observer = self.spawn_observer([sys.executable, "-c", child])
        ready = self.wait_ready(observer, ready_path)
        observer.terminate()
        self.assertEqual(observer.wait(timeout=3), 128 + signal.SIGTERM)
        result = self.summary()
        self.assertEqual(result["exit_code"], -signal.SIGTERM)
        self.assertTrue(result["cleanup"]["kill_sent"])
        self.assertTrue(result["cleanup"]["root_reaped"])
        stat = Path(f"/proc/{ready['descendant_pid']}/stat")
        if stat.exists():
            fields = stat.read_text().rsplit(")", 1)[1].split()
            self.assertEqual(fields[0], "Z")  # A zombie has no running work or open lock FD.
        with self.lock.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_observer_sigkill_does_not_release_lock_held_by_live_workload(self):
        ready_path = self.root / "ready"
        child = ("import json,os,time; from pathlib import Path; "
                 "fds=[]; "
                 "exec('for fd in os.listdir(\"/proc/self/fd\"):\\n"
                 " try: fds.append(os.readlink(\"/proc/self/fd/\"+fd))\\n"
                 " except OSError: pass'); "
                 f"Path({str(ready_path)!r}).write_text(json.dumps({{'pid':os.getpid(),'fds':fds}})); "
                 "time.sleep(5)")
        observer = self.spawn_observer([sys.executable, "-c", child])
        ready = self.wait_ready(observer, ready_path)
        self.assertIn(str(self.lock), ready["fds"])
        observer.kill()
        self.assertEqual(observer.wait(timeout=2), -signal.SIGKILL)
        with self.lock.open("a") as lock:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertFalse((self.output / "summary.json").exists())
        metadata = json.loads((self.output / "metadata.json").read_text())
        self.assertTrue(metadata["workload_inherits_lock_fd"])
        os.killpg(ready["pid"], signal.SIGKILL)
        deadline = time.monotonic() + 1
        with self.lock.open("a") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self.fail("lock remained held after the fake workload was killed")
                    time.sleep(0.01)

    def test_dedicated_cgroup_is_labelled_separately_from_shared_ancestors(self):
        ancestor = self.root / "cgroup"
        leaf = ancestor / "fresh-workload.scope"
        leaf.mkdir(parents=True)
        (leaf / "memory.current").write_text("123\n")
        (leaf / "memory.peak").write_text("987654\n")
        (leaf / "memory.events").write_text("max 2\noom 0\n")
        (leaf / "memory.swap.current").write_text("0\n")
        (leaf / "memory.swap.peak").write_text("0\n")
        (ancestor / "memory.current").write_text("9000000\n")
        with mock.patch.object(measure, "cgroup_paths", return_value=[leaf, ancestor]):
            self.assertEqual(self.invoke([sys.executable, "-c", "pass"], extra=("--dedicated-cgroup",)), 0)
        result = self.summary()
        self.assertEqual(result["cgroups_after"][0]["classification"], "dedicated-observer-and-workload")
        self.assertEqual(result["cgroups_after"][1]["classification"], "shared")
        self.assertEqual(result["dedicated_cgroup_measurement"]["peak_bytes"], 987654)
        self.assertTrue(result["dedicated_cgroup_measurement"]["includes_observer_page_cache_and_kernel"])
        self.assertEqual(result["dedicated_cgroup_measurement"]["memory_events_delta"], {"max": 0, "oom": 0})
        self.assertEqual(result["dedicated_cgroup_measurement"]["swap_peak_bytes"], 0)
        samples = [json.loads(line) for line in (self.output / "process-samples.jsonl").read_text().splitlines()]
        self.assertEqual(samples[0]["cgroup_current_bytes"], ["123", "9000000"])
        self.assertNotIn("shared_cgroup_current_bytes", samples[0])
        self.assertEqual(samples[0]["dedicated_leaf"][0]["memory.events"], "max 2\noom 0")
        self.assertEqual((leaf / "memory.peak").read_text(), "987654\n")


if __name__ == "__main__":
    unittest.main()
