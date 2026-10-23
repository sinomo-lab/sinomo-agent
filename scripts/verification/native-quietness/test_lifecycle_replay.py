"""Synthetic loss/exit/drain controls; no kernel operations or host acceptance."""
import copy
import unittest

import lifecycle_replay as p


def fixture():
    binding = dict(host_id="host", boot_id="boot", source_id="source", source_epoch="epoch",
                   manifest_sha256="a" * 64, run_nonce="nonce", observer_id="observer:1",
                   owned_transients=["collector:1", "transport:1"])

    def entity(identity, kind="process", parent=None, owned=False):
        return dict(incarnation=identity, kind=kind, parent=parent, owned_transient=owned,
                    counters={k: 0 for k in p.COUNTERS[kind]})

    observer = entity("observer:1")
    collector = entity("collector:1", owned=True)
    transport = entity("transport:1", owned=True)
    netns = entity("netns:1", "netns")
    iface = entity("interface:1", "interface", "netns:1")
    rows = [dict(type="baseline", subscription_before_census=True, baseline_reconciled=True,
                 producers=["cpu0:1", "cpu1:1"], entities=[observer, collector, transport, netns, iface])]
    for i, item in enumerate([iface, netns, collector, transport], 1):
        rows.append(dict(type="exit", entity=copy.deepcopy(item), final_counters=True,
                         producer="cpu0:1", producer_sequence=i))
    rows.append(dict(type="terminal", sealed_producers=True,
                     watermarks={"cpu0:1": dict(emitted=4, lost=0, inflight=0),
                                 "cpu1:1": dict(emitted=0, lost=0, inflight=0)},
                     observer_id="observer:1", observer_continues=True,
                     entities=[copy.deepcopy(observer)], deferred={k: 0 for k in p.DEFERRED},
                     deferred_proof_id="fixture-only-no-real-drain-proof"))
    for seq, row in enumerate(rows):
        row.update({k: binding[k] for k in p.BINDING})
        row.update(schema=p.SCHEMA, sequence=seq, monotonic_ns=seq + 1, lost=0)
    rows[-1]["cut_ns"] = rows[-1]["monotonic_ns"]
    return binding, rows


class LifecycleReplay(unittest.TestCase):
    def reject(self, edit):
        binding, rows = fixture()
        edit(binding, rows)
        result = p.replay(rows, binding)
        self.assertFalse(result["replay_valid"], result)
        self.assertFalse(result["acceptance"])
        self.assertFalse(result["coverage_complete"])
        self.assertTrue(result["errors"])
        return result

    def test_valid_replay_never_grants_native_coverage_or_quietness(self):
        binding, rows = fixture()
        result = p.replay(rows, binding)
        self.assertTrue(result["replay_valid"], result)
        self.assertEqual(result["verdict"], "BLOCKED")
        self.assertFalse(result["acceptance"])
        self.assertFalse(result["coverage_complete"])
        self.assertTrue(result["blockers"])

    def test_every_source_run_binding_is_required(self):
        for key in p.BINDING:
            with self.subTest(key=key):
                self.reject(lambda b, r: r[2].update({key: "changed"}))

    def test_middle_loss_duplicate_and_reordering(self):
        self.reject(lambda b, r: r.pop(2))
        self.reject(lambda b, r: r.insert(2, copy.deepcopy(r[1])))
        self.reject(lambda b, r: r.__setitem__(slice(1, 3), list(reversed(r[1:3]))))

    def test_unreported_final_event_is_detected_by_producer_watermark(self):
        self.reject(lambda b, r: r[-1]["watermarks"]["cpu0:1"].update(emitted=5))

    def test_missing_inactive_producer_is_not_assumed_zero(self):
        self.reject(lambda b, r: r[-1]["watermarks"].pop("cpu1:1"))

    def test_reported_loss_or_inflight_at_cut_rejects(self):
        for key in ("lost", "inflight"):
            with self.subTest(key=key):
                self.reject(lambda b, r: r[-1]["watermarks"]["cpu1:1"].update({key: 1}))
        self.reject(lambda b, r: r[1].update(lost=1))

    def test_missing_terminal_or_data_after_cut_rejects(self):
        self.reject(lambda b, r: r.pop())
        self.reject(lambda b, r: r.append(copy.deepcopy(r[-1])))
        self.reject(lambda b, r: r.clear())

    def test_baseline_race_and_missing_observer(self):
        self.reject(lambda b, r: r[0].update(subscription_before_census=False))
        self.reject(lambda b, r: r[0].update(baseline_reconciled=False))
        self.reject(lambda b, r: r[0]["entities"].pop(0))

    def test_incarnation_reuse_is_not_a_new_process(self):
        self.reject(lambda b, r: r[1].update(type="birth", entity=copy.deepcopy(r[0]["entities"][1])))
        self.reject(lambda b, r: r[4].update(type="birth", entity=copy.deepcopy(r[3]["entity"])))

    def test_namespace_final_counters_cannot_precede_interface_exit(self):
        self.reject(lambda b, r: r[1].update(entity=copy.deepcopy(r[2]["entity"])))

    def test_missing_final_counters_and_counter_reset(self):
        self.reject(lambda b, r: r[1].update(final_counters=False))
        self.reject(lambda b, r: r[1]["entity"]["counters"].pop("rx_bytes"))
        self.reject(lambda b, r: r[0]["entities"][-1]["counters"].update(rx_bytes=1))

    def test_numeric_booleans_and_negative_fields_reject(self):
        for value in (True, -1, 2**64):
            with self.subTest(value=value):
                self.reject(lambda b, r: r[1].update(producer_sequence=value))
                self.reject(lambda b, r: r[-1].update(cut_ns=value))

    def test_process_or_interface_binding_changes_reject(self):
        self.reject(lambda b, r: r[1]["entity"].update(parent="netns:2"))
        self.reject(lambda b, r: r[3]["entity"].update(owned_transient=False))

    def test_all_owned_helpers_must_exit(self):
        self.reject(lambda b, r: b["owned_transients"].append("missing-helper:1"))
        self.reject(lambda b, r: r[4].update(type="sample"))

    def test_source_epoch_producer_gap_and_unknown_event(self):
        self.reject(lambda b, r: r[2].update(producer="cpu0:2"))
        self.reject(lambda b, r: r[2].update(producer_sequence=3))
        self.reject(lambda b, r: r[2].update(type="silently-rebind"))

    def test_terminal_inventory_and_observer_continuity(self):
        self.reject(lambda b, r: r[-1].update(entities=[]))
        self.reject(lambda b, r: r[-1].update(observer_continues=False))
        self.reject(lambda b, r: r[-1].update(sealed_producers=False))

    def test_every_deferred_work_family_requires_proof(self):
        for key in p.DEFERRED:
            with self.subTest(key=key):
                self.reject(lambda b, r: r[-1]["deferred"].update({key: 1}))
                self.reject(lambda b, r: r[-1]["deferred"].pop(key))
        self.reject(lambda b, r: r[-1].update(deferred_proof_id=""))

    def test_truncated_json_during_stream_replay_is_invalid(self):
        import json
        binding, rows = fixture()
        stream = [json.dumps(row) for row in rows]
        stream[-1] = stream[-1][:-3]
        result = p.replay((json.loads(line) for line in stream), binding)
        self.assertFalse(result["replay_valid"])


if __name__ == "__main__":
    unittest.main()
