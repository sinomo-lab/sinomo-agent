"""Synthetic counter controls, never native-host acceptance or live I/O load."""
import copy
import unittest

import disk_metrics as d


def sample(sequence, fields=None):
    fields = fields if fields is not None else [100] * 17
    base = (sequence + 1) * d.SECOND
    return dict(boot_id="boot", sequence=sequence, begin_ns=base,
                end_ns=base + 400_000,
                disk=dict(read_ns=[base + 100_000, base + 200_000],
                          read_errors=[], page_size=4096,
                          diskstats="8 0 sda " + " ".join(map(str, fields)) + "\n",
                          vmstat="nr_dirty 2\nnr_writeback 3\n"))


class DiskMetrics(unittest.TestCase):
    def test_bytes_operations_flushes_and_gauges_remain_distinct(self):
        old, new = sample(0), sample(1, [101] * 17)
        result = d.interval(new, old)
        row = result["devices"]["8:0"]
        self.assertEqual(row["read_bytes"], 512)
        self.assertEqual(row["write_bytes"], 512)
        self.assertEqual(row["reads_completed"], 1)
        self.assertEqual(row["flushes_completed"], 1)
        self.assertEqual(row["flush_ms"], 1)
        self.assertEqual(row["discarded_bytes"], 512)
        self.assertEqual(result["settling_after"]["dirty_bytes"], 8192)
        self.assertEqual(result["settling_after"]["writeback_bytes"], 12288)
        self.assertEqual(result["minimum_interval_ns"], d.SECOND - 100_000)
        self.assertGreater(row["read_Bps_upper"], 512)
        self.assertIsNone(result["sync_syscalls"])

    def test_every_cumulative_counter_reset_rejects_but_in_flight_may_decrease(self):
        for index in range(17):
            fields = [100] * 17
            fields[index] = 99
            with self.subTest(index=index):
                if index == 8:
                    self.assertEqual(d.interval(sample(1, fields), sample(0))["devices"]["8:0"]["in_flight_after"], 99)
                else:
                    with self.assertRaisesRegex(ValueError, "reset"):
                        d.interval(sample(1, fields), sample(0))

    def test_older_counters_do_not_fabricate_discard_or_flush_zero(self):
        for count in (11, 15):
            result = d.interval(sample(1, [101] * count), sample(0, [100] * count))
            row = result["devices"]["8:0"]
            self.assertIsNone(row["flushes_completed"])
            if count == 11:
                self.assertIsNone(row["discarded_bytes"])

    def test_counter_layers_are_not_summed(self):
        a, b = sample(0), sample(1)
        for row in (a, b):
            row["disk"]["diskstats"] += row["disk"]["diskstats"].replace("8 0 sda", "8 1 sda1")
        result = d.interval(b, a)
        self.assertEqual(len(result["devices"]), 2)
        self.assertIsNone(result["aggregate"])

    def test_identity_inventory_and_schema_changes_reject(self):
        for old, new in (("sda", "sdb"), ("8 0", "8 1"), (" 100\n", "\n")):
            row = sample(1)
            row["disk"]["diskstats"] = row["disk"]["diskstats"].replace(old, new)
            with self.subTest(new=new), self.assertRaises(ValueError):
                d.interval(row, sample(0))

    def test_permission_failure_or_missing_errors_never_becomes_zero(self):
        for value in (None, [{"source": "/proc/diskstats", "errno": 13}]):
            row = sample(1)
            row["disk"]["read_errors"] = value
            with self.assertRaisesRegex(ValueError, "reader"):
                d.interval(row, sample(0))

    def test_malformed_missing_truncated_or_duplicate_counter_text(self):
        text = sample(0)["disk"]["diskstats"]
        for value in (None, "", text.rstrip(), text + text, text.replace("100", "-1"),
                      text.replace("100", "1.5"), text.replace("100", str(2**64))):
            with self.subTest(value=value), self.assertRaises(ValueError):
                d.parse_diskstats(value)

    def test_boot_and_sequence_changes_reject(self):
        for key, value in (("boot_id", "new-boot"), ("sequence", 4), ("sequence", True)):
            row = sample(1)
            row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                d.interval(row, sample(0))

    def test_read_outside_sample_overlap_and_stall_reject(self):
        for read_span in ([0, 1], [2_000_200_000, 2_000_100_000], [1, 2, 3]):
            row = sample(1)
            row["disk"]["read_ns"] = read_span
            with self.assertRaises(ValueError):
                d.interval(row, sample(0))
        old = sample(0)
        new = copy.deepcopy(old)
        new["sequence"] = 1
        with self.assertRaisesRegex(ValueError, "overlap"):
            d.interval(new, old)
        new = sample(9)
        new["sequence"] = 1
        with self.assertRaisesRegex(ValueError, "gap"):
            d.interval(new, old)

    def test_missing_writeback_or_invalid_page_size_reject(self):
        for key, value in (("vmstat", "nr_dirty 1\n"), ("page_size", 0),
                           ("page_size", True), ("vmstat", None)):
            row = sample(1)
            row["disk"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                d.interval(row, sample(0))

    def test_zero_delta_and_claimed_coverage_never_qualify_host(self):
        a, b = sample(0), sample(1)
        for row in (a, b):
            row["disk"].update(device_lifecycle_coverage=True, sync_syscall_coverage=True)
        result = d.interval(b, a)
        self.assertFalse(result["acceptance"])
        self.assertFalse(result["coverage_complete"])
        self.assertIsNone(result["approved_native_limits"])
        self.assertTrue(result["blockers"])

    def test_missing_disk_sample_is_unavailable(self):
        row = sample(1)
        del row["disk"]
        with self.assertRaises(KeyError):
            d.interval(row, sample(0))


if __name__ == "__main__":
    unittest.main()
