#!/usr/bin/env python3
"""Controlled evidence fixtures. Successful fixture checks are not host acceptance."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import controller as c


def sample(i, *, offset=0, coverage=True):
    base = offset + (i + 1) * c.SECOND
    return dict(type="sample", sequence=i, nonce=f"barrier-{i}", boot_id="boot-A",
                begin_ns=base, end_ns=base + 2_000_000,
                cpu_read_ns=[base + 1_500_000, base + 1_600_000],
                cpu_fields=[i, 0, 0, 100 * i, 0, 0, 0, 0, 0, 0],
                groups={"24:100": dict(netns=300, read_ns=[base + 100_000, base + 200_000],
                                      interface_indices={"lo": 1, "eth0": 2},
                                      interfaces={"lo": [1000 + i * 100, 1000 + i * 100],
                                                  "eth0": [i * 200, i * 300]})},
                errors=0, vanished=0, self_link_losses=0, self_link_events=0,
                self_link_reader=True, coverage={k: coverage for k in c.COVERAGE})


class Counters(unittest.TestCase):
    def test_idle_conservative_integer_bounds(self):
        row = c.interval(sample(1), sample(0), 100)
        self.assertTrue(row["counters_within"])
        self.assertEqual(row["rx_bytes"], 300)
        self.assertEqual(row["tx_bytes"], 400)
        self.assertEqual(row["cpu_minimum_interval_ns"], c.SECOND - 100_000)
        self.assertEqual(row["network_minimum_interval_ns"], c.SECOND - 100_000)

    def reject(self, change, text=None):
        old, new = sample(0), sample(1)
        change(new)
        with self.assertRaises(ValueError) as e:
            c.interval(new, old, 100)
        if text:
            self.assertIn(text, str(e.exception))

    def test_counter_reset(self):
        self.reject(lambda r: r["groups"]["24:100"]["interfaces"]["lo"].__setitem__(0, 999), "reset")

    def test_missing_loopback(self):
        self.reject(lambda r: r["groups"]["24:100"]["interfaces"].pop("lo"), "loopback")

    def test_interface_birth(self):
        self.reject(lambda r: r["groups"]["24:100"]["interfaces"].update(veth0=[0, 0]), "inventory")

    def test_interface_index_reuse_is_not_hidden_by_increasing_bytes(self):
        self.reject(lambda r: r["groups"]["24:100"]["interface_indices"].update(eth0=3), "lifecycle")

    def test_missing_foreign_interface_reader_blocks_full_coverage(self):
        a, b = sample(0), sample(1)
        for r in (a, b):
            r["groups"]["24:100"]["interface_indices"] = None
        value = c.interval(b, a, 100)
        self.assertTrue(value["counters_within"])
        self.assertFalse(value["coverage_complete"])

    def test_each_continuous_coverage_source_is_required(self):
        for key in c.COVERAGE:
            with self.subTest(key=key):
                a, b = sample(0), sample(1)
                b["coverage"][key] = False
                self.assertFalse(c.interval(b, a, 100)["coverage_complete"])

    def test_prior_missing_reader_needs_a_new_clean_baseline(self):
        a, b = sample(0), sample(1)
        a["errors"] = 1
        with self.assertRaisesRegex(ValueError, "reader"):
            c.interval(b, a, 100)
        self.assertTrue(c.interval(sample(2), b, 100)["coverage_complete"])

    def test_namespace_birth(self):
        self.reject(lambda r: r["groups"].update({"24:101": copy.deepcopy(r["groups"]["24:100"])}), "inventory")

    def test_namespace_id_changed(self):
        self.reject(lambda r: r["groups"]["24:100"].update(netns=301), "identity")

    def test_same_id_reused_with_reset(self):
        # A matching proc inode/netns is insufficient: the counters still reset.
        self.reject(lambda r: r["groups"]["24:100"].update(interfaces={"lo": [0, 0], "eth0": [0, 0]}), "reset")

    def test_disappearing_thread_invalidates_reader(self):
        self.reject(lambda r: r.update(vanished=1), "unexplained")

    def test_reader_permission_denial(self):
        self.reject(lambda r: r.update(errors=1), "reader")

    def test_namespace_symlink_denial_does_not_invent_zero(self):
        a, b = sample(0), sample(1)
        b["namespace_link_denials"] = 99
        b["coverage"]["continuous_host_namespaces"] = False
        result = c.interval(b, a, 100)
        self.assertTrue(result["counters_within"])
        self.assertFalse(result["coverage_complete"])

    def test_missing_counter_is_not_zero(self):
        self.reject(lambda r: r["groups"]["24:100"]["interfaces"].update(lo=[None, 0]), "counter")

    def test_float_boolean_negative_overflow_counters(self):
        for value in (1.0, True, -1, 2**64):
            with self.subTest(value=value):
                self.reject(lambda r: r["groups"]["24:100"]["interfaces"].update(lo=[value, 1000]), "counter")

    def test_boot_change(self):
        self.reject(lambda r: r.update(boot_id="boot-B"), "boot")

    def test_reader_restart_or_missing_sample(self):
        self.reject(lambda r: r.update(sequence=0), "reader")

    def test_missing_field(self):
        with self.assertRaises(KeyError):
            row = sample(1)
            row.pop("cpu_fields")
            c.interval(row, sample(0), 100)

    def test_cpu_counter_reset(self):
        a, b = sample(1), sample(2)
        b["cpu_fields"][0] = 0
        with self.assertRaisesRegex(ValueError, "reset"):
            c.interval(b, a, 100)

    def test_link_event_and_dropped_reader(self):
        for update in (dict(self_link_events=1), dict(self_link_losses=1), dict(self_link_reader=False)):
            with self.subTest(update=update):
                self.reject(lambda r: r.update(update), "interface")

    def test_read_spans_must_be_inside_census(self):
        self.reject(lambda r: r.update(cpu_read_ns=[0, 1]), "outside")

    def test_overlap_and_stall(self):
        self.reject(lambda r: r.update(begin_ns=1), "overlapping")
        self.reject(lambda r: r.update(begin_ns=9 * c.SECOND, end_ns=10 * c.SECOND), "outside")

    def test_all_interfaces_and_namespaces_are_summed(self):
        a, b = sample(0), sample(1)
        for row in (a, b):
            row["groups"]["24:101"] = copy.deepcopy(row["groups"]["24:100"])
        result = c.interval(b, a, 100)
        self.assertEqual(result["rx_bytes"] + result["tx_bytes"], 1400)

    def test_loopback_tx_and_rx_both_charged(self):
        a, b = sample(0), sample(1)
        b["groups"]["24:100"]["interfaces"]["lo"] = [41000, 41000]
        self.assertFalse(c.interval(b, a, 100)["network_within"])

    def test_one_cpu_not_percentage_of_host_processors(self):
        a, b = sample(0), sample(1)
        b["cpu_fields"][0] = 11  # 11% of one CPU exceeds 10%, on any host size.
        b["processors"] = 192
        self.assertFalse(c.interval(b, a, 100)["cpu_within"])

    def test_no_guest_double_count_and_all_busy_fields(self):
        a, b = sample(0), sample(1)
        b["cpu_fields"][8] = 100000  # already included in user time
        self.assertTrue(c.interval(b, a, 100)["cpu_within"])
        for field in c.CPU_FIELDS:
            r = sample(1)
            r["cpu_fields"][field] = 100
            self.assertFalse(c.interval(r, a, 100)["cpu_within"])

    def test_observer_overhead_is_not_subtracted(self):
        a, b = sample(0), sample(1)
        b["cpu_fields"][0] = 11
        b["observer_cpu_ns"] = c.SECOND
        self.assertFalse(c.interval(b, a, 100)["cpu_within"])

    def test_uncertainty_rejects_nominal_boundary(self):
        a, b = sample(0), sample(1)
        b["cpu_fields"][0] = 10  # exactly 10% at 1s, >10% over minimum interval
        self.assertFalse(c.interval(b, a, 100)["cpu_within"])
        for row in (a, b):
            row["groups"]["24:100"]["interfaces"] = {"lo": [1000, 1000]}
            row["groups"]["24:100"]["interface_indices"] = {"lo": 1}
        b["groups"]["24:100"]["interfaces"]["lo"][0] += 65536
        self.assertFalse(c.interval(b, a, 100)["network_within"])


class Barriers(unittest.TestCase):
    def setUp(self):
        self.nodes = ("A", "B")
        self.w = c.JointWindow(self.nodes, 0)

    def tick(self, i, *, good=True, coverage=True, end=False, latency=5_000_000, offset=0):
        sent = {n: (i + 1) * c.SECOND + offset for n in self.nodes}
        replies = {n: dict(received_ns=sent[n] + latency, sample=sample(i, offset=999*c.SECOND if n == "B" else 0))
                   for n in self.nodes}
        checks = {n: dict(counters_within=good, coverage_complete=coverage) for n in self.nodes} if i else {}
        return self.w.observe(i, f"barrier-{i}", sent, replies, checks, end=end)

    def test_continuous_fixture_requires_start_and_end_barriers(self):
        for i in range(31):
            self.assertFalse(self.tick(i, end=True)["covered_window"])
        # 31s needed here: baseline ACK latency cannot be counted toward 30s.
        result = self.tick(31, end=True)
        self.assertTrue(result["covered_window"])
        self.assertEqual(result["common_duration_ns"], 31*c.SECOND - 5_000_000)

    def test_independent_host_monotonic_offsets_are_never_subtracted(self):
        for i in range(32):
            result = self.tick(i, end=True)
        self.assertTrue(result["covered_window"])

    def test_30_seconds_without_end_barrier_is_insufficient(self):
        for i in range(40):
            result = self.tick(i, end=False)
        self.assertFalse(result["covered_window"])

    def test_missing_coverage_never_passes(self):
        for i in range(100):
            self.assertFalse(self.tick(i, coverage=False, end=True)["covered_window"])

    def test_one_bad_interval_breaks_the_joint_streak(self):
        for i in range(31):
            self.tick(i, good=i != 15, end=True)
        self.assertFalse(self.tick(31, end=True)["covered_window"])

    def test_end_ack_after_original_deadline_is_rejected(self):
        for i in range(118):
            self.tick(i)
        result = self.tick(118, end=True, latency=2*c.SECOND)
        self.assertFalse(result["covered_window"])
        self.assertIn("deadline", result["error"])

    def test_stalled_cadence_is_not_an_accepted_long_interval(self):
        self.tick(0)
        result = self.tick(1, offset=c.SECOND)
        self.assertIn("cadence", result["error"])

    def test_no_missing_or_restarted_reader(self):
        self.tick(0)
        self.assertIn("sequence", self.tick(2)["error"])

    def test_nonce_mismatch(self):
        self.tick(0)
        commands = {n: 2*c.SECOND for n in self.nodes}
        replies = {n: dict(received_ns=2*c.SECOND + 1, sample=sample(1)) for n in self.nodes}
        replies["B"]["sample"]["nonce"] = "wrong"
        result = self.w.observe(1, "barrier-1", commands, replies, {}, end=True)
        self.assertIn("barrier", result["error"])

    def test_one_missing_host_invalidates_joint_result(self):
        result = self.w.observe(0, "barrier-0", {"A": c.SECOND}, {}, {})
        self.assertIn("participant", result["error"])


def raw_fixture(path, *, argv=None, epoch=1, exec_change=False):
    runtime = dict(role="main", executable="/owned/bin/sinomo", sha256="a"*64,
                   pid=50, start_ticks=200, boot_id="boot-A", revision="revision-A",
                   node_id="node-A", data_dir="/owned/DATA")
    lines = [dict(type="ready", boot_id="boot-A", pid=60, start_ticks=300, hertz=100)]
    for i in range(2):
        summary = sample(i)
        lines.append(dict(type="begin", sequence=i, nonce=summary["nonce"]))
        p = dict(type="process", tgid=50, uid=1000, process_epoch=1, start_ticks=200,
                 exe=runtime["executable"], exe_identity=[24, 400, 100, 1, 0, 1, 0],
                 sha256=runtime["sha256"], candidate=True, exe_errno=0, argv_errno=0,
                 argv=argv or ["sinomo", "daemon", "run"])
        lines.append(p)
        for tid in (50, 51):
            lines.append(dict(type="thread", tgid=50, tid=tid, start_ticks=200,
                              thread_epoch=(epoch if i else 1) + tid,
                              cpu_ticks=[100 + i*3, 200 + i*2]))
        q = dict(p, type="process_end")
        if exec_change and i:
            q["sha256"] = "b"*64
        lines.extend([q, summary])
    path.write_text("\n".join(map(json.dumps, lines)) + "\n")
    return runtime


class Attribution(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.raw = Path(self.temp.name) / "raw.ndjson"

    def test_thread_totals_never_double_count_tgid_stat(self):
        runtime = raw_fixture(self.raw)
        report = c.attribution(self.raw, [runtime])
        self.assertFalse(report["acceptance"])
        self.assertEqual(report["rows"][0]["sinomo_ticks"], 10)
        self.assertFalse(report["rows"][0]["namespace_bytes_are_process_attribution"])

    def test_command_substring_does_not_classify_observer_as_daemon(self):
        runtime = raw_fixture(self.raw, argv=["sinomo", "debug", "script contains daemon run"])
        row = c.attribution(self.raw, [runtime])["rows"][0]
        self.assertEqual(row["sinomo_ticks"], 0)
        self.assertEqual(len(row["unknown"]), 2)

    def test_same_tick_pid_reuse_is_split_by_pidfd_epoch(self):
        runtime = raw_fixture(self.raw, epoch=2)
        row = c.attribution(self.raw, [runtime])["rows"][0]
        self.assertEqual(row["sinomo_ticks"], 0)
        self.assertTrue(row["born"] and row["exited"])

    def test_exec_changes_are_unknown(self):
        runtime = raw_fixture(self.raw, exec_change=True)
        row = c.attribution(self.raw, [runtime])["rows"][0]
        self.assertEqual(row["sinomo_ticks"], 0)
        self.assertEqual(len(row["unknown"]), 2)

    def test_hash_revision_and_pid_receipt_are_required(self):
        for key, value in (("sha256", "b"*64), ("revision", None), ("pid", 51), ("boot_id", "boot-B")):
            with self.subTest(key=key):
                runtime = raw_fixture(self.raw)
                runtime[key] = value
                self.assertEqual(c.attribution(self.raw, [runtime])["rows"][0]["sinomo_ticks"], 0)

    def test_truncated_raw_is_not_a_zero_sample(self):
        raw_fixture(self.raw)
        self.raw.write_text("\n".join(self.raw.read_text().splitlines()[:-1]) + "\n")
        with self.assertRaisesRegex(ValueError, "truncated"):
            c.attribution(self.raw, [])


if __name__ == "__main__":
    unittest.main()
