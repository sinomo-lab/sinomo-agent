#!/usr/bin/env python3
"""Offline lifecycle receipt validation. No kernel access or acceptance authority.

This small protocol checks consistency of a supplied replay, not authenticity,
kernel hook coverage, counter correctness, disk policy or a quiet time window.
Even a valid replay always returns BLOCKED with acceptance/coverage false.
"""
import argparse
import json
from pathlib import Path

SCHEMA = "sinomo.lifecycle-replay.v1"
BINDING = ("host_id", "boot_id", "source_id", "source_epoch", "manifest_sha256", "run_nonce")
COUNTERS = {"process": {"cpu_ticks", "read_bytes", "write_bytes"},
            "netns": set(), "interface": {"rx_bytes", "tx_bytes"}}
DEFERRED = {"raw_dirty_bytes", "pending_block_requests", "queued_transport_bytes",
            "pending_export_bytes"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value):
    require(type(value) is int and 0 <= value < 2**64, "invalid counter or sequence")
    return value


def names(value):
    require(isinstance(value, list) and all(type(x) is str and x for x in value), "invalid identities")
    require(len(value) == len(set(value)), "duplicate identity")
    return set(value)


def counters(row, before=None):
    require(row["kind"] in COUNTERS, "unknown entity kind")
    values = row["counters"]
    require(isinstance(values, dict) and values.keys() == COUNTERS[row["kind"]], "missing counter fields")
    for key, value in values.items():
        require(integer(value) >= (before[key] if before is not None else 0), "counter reset")
    return dict(values)


def replay(records, expected):
    result = dict(schema=SCHEMA, replay_valid=False, verdict="BLOCKED", rollout_gate=False,
                  acceptance=False, coverage_complete=False, errors=[],
                  blockers=["no qualified authenticated kernel event source is connected",
                            "optional replay does not establish daemon quietness or live ownership",
                            "terminal drain assertions require independent source evidence"])
    live, seen, owned, exited = {}, set(), set(), set()
    producer_sequences = {}
    terminal = False
    last_time = 0
    count = 0

    def add(entity):
        identity = entity["incarnation"]
        require(type(identity) is str and identity and identity not in seen, "reused incarnation")
        require(type(entity["owned_transient"]) is bool, "missing transient ownership")
        if entity["kind"] == "interface":
            parent = live.get(entity["parent"])
            require(parent is not None and parent["kind"] == "netns", "interface has no live namespace")
        else:
            require(entity["parent"] is None, "unexpected parent binding")
        row = dict(entity)
        row["counters"] = counters(entity)
        live[identity] = row
        seen.add(identity)
        if entity["owned_transient"]:
            require(entity["kind"] == "process", "transient must be a process incarnation")
            owned.add(identity)

    def update(entity):
        identity = entity["incarnation"]
        require(identity in live, "event for unknown or exited incarnation")
        old = live[identity]
        require(all(entity[k] == old[k] for k in ("kind", "parent", "owned_transient")),
                "identity binding changed without lifecycle event")
        old["counters"] = counters(entity, old["counters"])

    try:
        require(all(type(expected[k]) is str and expected[k] for k in BINDING), "expected binding missing")
        require(type(expected["observer_id"]) is str and expected["observer_id"], "observer binding missing")
        required_owned = names(expected["owned_transients"])
        require(required_owned and expected["observer_id"] not in required_owned, "invalid observer/transient roles")
        for sequence, row in enumerate(records):
            require(not terminal, "record after terminal cut")
            require(row["schema"] == SCHEMA, "unsupported schema")
            require(all(row[k] == expected[k] for k in BINDING), "source/run binding changed")
            require(integer(row["sequence"]) == sequence, "lost, duplicate or reordered record")
            require(integer(row["lost"]) == 0, "source reports lost events")
            observed = integer(row["monotonic_ns"])
            require(observed >= last_time, "source time reversed")
            last_time = observed
            kind = row["type"]
            if sequence == 0:
                require(kind == "baseline", "missing startup baseline")
                require(row["subscription_before_census"] is True and row["baseline_reconciled"] is True,
                        "baseline has an uncovered census race")
                producers = names(row["producers"])
                require(producers, "missing producer inventory")
                producer_sequences = {p: 0 for p in producers}
                for entity in row["entities"]:
                    add(entity)
                observer = live.get(expected["observer_id"])
                require(observer and observer["kind"] == "process" and not observer["owned_transient"],
                        "persistent observer absent from measured inventory")
            elif kind in ("birth", "sample", "exit"):
                producer = row["producer"]
                require(producer in producer_sequences, "unknown producer or epoch change")
                require(integer(row["producer_sequence"]) == producer_sequences[producer] + 1,
                        "producer sequence gap")
                producer_sequences[producer] += 1
                entity = row["entity"]
                if kind == "birth":
                    add(entity)
                else:
                    update(entity)
                    if kind == "exit":
                        identity = entity["incarnation"]
                        require(not any(e["parent"] == identity for e in live.values()),
                                "namespace exited before interfaces' final counters")
                        require(row["final_counters"] is True, "exit lacks final counter receipt")
                        del live[identity]
                        exited.add(identity)
            elif kind == "terminal":
                require(row["sealed_producers"] is True, "producer cut not fenced")
                watermarks = row["watermarks"]
                require(watermarks.keys() == producer_sequences.keys(), "missing terminal producer")
                for p, last in producer_sequences.items():
                    mark = watermarks[p]
                    require(integer(mark["emitted"]) == last and integer(mark["lost"]) == 0
                            and integer(mark["inflight"]) == 0, "terminal tail loss or producer still active")
                require(integer(row["cut_ns"]) == observed, "terminal cut differs from source fence")
                require(required_owned <= owned and owned <= exited, "collector/transport/helper exit unproved")
                require(row["observer_id"] == expected["observer_id"] and row["observer_continues"] is True
                        and row["observer_id"] in live, "independent observer does not cover cut")
                entities = row["entities"]
                require(names([e["incarnation"] for e in entities]) == live.keys(), "terminal inventory incomplete")
                for entity in entities:
                    update(entity)
                deferred = row["deferred"]
                require(deferred.keys() == DEFERRED and all(integer(x) == 0 for x in deferred.values()),
                        "raw evidence, I/O or transport drain unproved")
                require(row["deferred_proof_id"] and type(row["deferred_proof_id"]) is str,
                        "missing deferred-work evidence reference")
                terminal = True
            else:
                raise ValueError("unknown event or duplicate baseline")
            count += 1
        require(terminal, "missing terminal receipt or truncated stream")
        result["replay_valid"] = True
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        result["errors"].append(str(error))
    result.update(records_checked=count, exited_incarnations=sorted(exited))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--records", type=Path, required=True)
    args = parser.parse_args()
    expected = json.loads(args.binding.read_text())
    # This is an offline replay file, not a live collector/transport command.
    with args.records.open() as stream:
        result = replay((json.loads(line) for line in stream), expected)
    print(json.dumps(result, indent=2))
    return 0 if result["replay_valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
