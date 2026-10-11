#!/usr/bin/env python3
"""Strict native shard trace preflight and actual Quint finite-boundary replay.

Exit 0 requires complete native evidence projected from held observations and
actual enabled Quint transition replay to the terminal cursor.
Exit 1 rejects invalid evidence or a disabled/mismatching Quint transition; exit 2
reports a missing dependency, missing native input, or an unsupported projection.
No model-generated trace is accepted as native input. Replay mechanics can be
checked independently with verify_quint_boundary(), without a conformance claim.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any

REPO = Path(__file__).resolve().parents[2]
SCHEMA_PATH = Path(__file__).with_name("shard-trace-v1.schema.json")
MODEL_PATH = REPO / "specs/quint/models/dfs/whole_shard.qnt"
CHECKS_PATH = REPO / "specs/quint/checks/dfs/whole_shard_safety.qnt"
MAX_U64 = (1 << 64) - 1
MODEL_MAX_INTEGER = (1 << 53) - 1


class Rejected(ValueError):
    """Evidence or transition is invalid; never a skipped/stuttering step."""


class Unavailable(RuntimeError):
    """Required native evidence, projection or tooling is not available."""


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def formal_source_hash(model: Path, checks: Path) -> str:
    """Bind both the transition relation and its invariant suite."""
    return digest_bytes(model.read_bytes() + b"\x00" + checks.read_bytes())


def decimal_u64(value: Any, where: str = "u64") -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise Rejected(f"{where}: native u64 requires a canonical decimal string")
    number = int(value)
    if number > MAX_U64:
        raise Rejected(f"{where}: native u64 overflow")
    return number


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise Rejected(f"duplicate JSON field {key!r}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(), object_pairs_hook=_pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(
                              Rejected(f"non-finite JSON number {value}")))
    except FileNotFoundError as exc:
        raise Unavailable(f"missing input: {path}") from exc
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise Rejected(f"invalid JSON in {path}: {exc}") from exc


def validate_schema(value: Any, node: dict[str, Any], root: dict[str, Any], where: str = "$") -> None:
    """Enforce the exact small draft-2020 schema vocabulary shipped with this checker."""
    if "$ref" in node:
        name = node["$ref"]
        if not name.startswith("#/$defs/") or name[8:] not in root["$defs"]:
            raise Rejected(f"unsupported schema reference {name}")
        validate_schema(value, root["$defs"][name[8:]], root, where)
    for branch in node.get("allOf", []):
        validate_schema(value, branch, root, where)
    for keyword, exact_one in (("anyOf", False), ("oneOf", True)):
        if keyword in node:
            matched = 0
            for branch in node[keyword]:
                try:
                    validate_schema(value, branch, root, where)
                    matched += 1
                except Rejected:
                    pass
            if matched == 0 or (exact_one and matched != 1):
                raise Rejected(f"{where}: {keyword} matched {matched} alternatives")
    if "not" in node:
        try:
            validate_schema(value, node["not"], root, where)
        except Rejected:
            pass
        else:
            raise Rejected(f"{where}: forbidden value")
    if "const" in node and (type(value) is not type(node["const"]) or value != node["const"]):
        raise Rejected(f"{where}: expected constant {node['const']!r}")
    if "enum" in node and value not in node["enum"]:
        raise Rejected(f"{where}: value outside enum")
    kind = node.get("type")
    types = {"object": dict, "array": list, "string": str, "integer": int, "boolean": bool, "null": type(None)}
    if kind is not None and (kind not in types or type(value) is not types[kind]):
        raise Rejected(f"{where}: expected {kind}")
    if kind == "object":
        missing = set(node.get("required", [])) - value.keys()
        if missing:
            raise Rejected(f"{where}: missing {sorted(missing)}")
        fields = node.get("properties", {})
        extra = node.get("additionalProperties", True)
        for key, item in value.items():
            if key in fields:
                validate_schema(item, fields[key], root, f"{where}.{key}")
            elif extra is False:
                raise Rejected(f"{where}: unknown field {key}")
            elif isinstance(extra, dict):
                validate_schema(item, extra, root, f"{where}.{key}")
    elif kind == "array":
        for i, item in enumerate(value):
            validate_schema(item, node["items"], root, f"{where}[{i}]")
    elif kind == "string":
        if len(value) < node.get("minLength", 0):
            raise Rejected(f"{where}: string too short")
        if "pattern" in node and not re.fullmatch(node["pattern"], value):
            raise Rejected(f"{where}: noncanonical or out-of-range string")
    elif kind == "integer":
        if value < node.get("minimum", value) or value > node.get("maximum", value):
            raise Rejected(f"{where}: integer outside bounds")


def verified_artifact_manifest(manifest_path: Path, source_manifest: Path,
                               executable: Path | None = None) -> dict[str, Any]:
    helper_path = Path(__file__).with_name("native-artifact-binding.py")
    spec = importlib.util.spec_from_file_location("native_artifact_binding", helper_path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    try:
        expected, _ = helper.verify_manifest(manifest_path, source_manifest, executable)
        return expected
    except OSError as exc:
        raise Unavailable(f"actual native artifact unavailable: {exc}") from exc
    except (ValueError, KeyError, TypeError) as exc:
        raise Rejected(f"native artifact binding: {exc}") from exc


def trace_artifact_binding(trace: dict[str, Any], expected: dict[str, Any] | None) -> dict[str, Any]:
    if expected is None:
        raise Unavailable("native conformance requires the verified executable/source artifact manifest")
    certificate = native_json_blob(trace["blobs"], trace["initial_cut"]["baseline_validation_blob"],
                                   "DfsNativeInitialCutCertificateV1")
    reference = certificate.get("source_artifact_binding_blob")
    if not reference:
        raise Unavailable("native initial cut lacks its actual artifact binding")
    actual = native_json_blob(trace["blobs"], reference, "NativeTraceArtifactBindingV1")
    if actual != expected:
        raise Rejected("native trace differs from the actual executable/source capture binding")
    for key in ("implementation_revision", "implementation_build_id", "model_sha256", "schema_sha256"):
        if actual[key] != trace[key]:
            raise Rejected(f"native artifact binding contradicts envelope {key}")
    return actual


def preflight_native(trace: dict[str, Any], *, schema: dict[str, Any], source_revision: str,
                     model_sha256: str, build_id: str, artifact_binding: dict[str, Any] | None = None) -> None:
    if any(key in trace for key in ("#meta", "states", "vars", "ITF")):
        raise Rejected("model-generated/ITF traces are not native conformance input")
    validate_schema(trace, schema, schema)
    for field, expected in (("implementation_revision", source_revision),
                            ("implementation_build_id", build_id),
                            ("model_sha256", model_sha256),
                            ("schema_sha256", digest_bytes(SCHEMA_PATH.read_bytes()))):
        if trace[field] != expected:
            raise Rejected(f"{field}: source/build/model/schema binding mismatch")
    terminal = trace["terminal"]
    for field in ("dropped_events", "incomplete_events", "discontinuities"):
        if decimal_u64(terminal[field], field) != 0:
            raise Rejected(f"capture incomplete: {field}")
    if not terminal["owners_drained"] or not terminal["subscriptions_closed"]:
        raise Rejected("native producers/observers did not close and drain")
    owners = {owner["owner_id"]: owner for owner in trace["owners"]}
    if len(owners) != len(trace["owners"]):
        raise Rejected("duplicate owner identity")
    writer_lanes: dict[str, str] = {}
    for owner in owners.values():
        previous = writer_lanes.setdefault(owner["node_id"], owner["lane_incarnation"])
        if previous != owner["lane_incarnation"]:
            raise Unavailable("model bound: more than one observed writer lane for a node")
    blobs = trace["blobs"]
    for name, blob in blobs.items():
        try:
            data = blob["data"].encode() if blob["encoding"] == "json-utf8" else base64.b64decode(blob["data"], validate=True)
        except ValueError as exc:
            raise Rejected(f"blob {name}: invalid base64") from exc
        if digest_bytes(data) != blob["sha256"]:
            raise Rejected(f"blob {name}: bytes do not match hash")
    snapshots = trace["snapshots"]
    if not snapshots:
        raise Unavailable("no complete native snapshots")
    for name, snapshot in snapshots.items():
        if snapshot["owner_id"] not in owners or snapshot["scope"] != trace["scope"]:
            raise Rejected(f"snapshot {name}: wrong owner/scope")
        if not snapshot["projection_continuous"] or not snapshot["kv_read"]["continuous"]:
            raise Rejected(f"snapshot {name}: discontinuous native projection")
        if not snapshot["kv_read"]["complete"]:
            raise Rejected(f"snapshot {name}: incomplete authoritative exact read")
        requested = snapshot["kv_read"]["requested_keys"]
        rows = snapshot["kv_read"]["rows"]
        if len(requested) != len(set(requested)) or [row["key"] for row in rows] != requested:
            raise Rejected(f"snapshot {name}: exact key order/cardinality mismatch")
        for config in (snapshot["committed_config"], snapshot["active_config"]):
            decimal_u64(config["epoch"], "config.epoch")
            for field in ("native_config_blob", "assignment_blob", "default_policy_blob"):
                if config[field] not in blobs:
                    raise Rejected(f"snapshot {name}: missing complete config evidence {field}")
            raw = blobs[config["native_config_blob"]]
            if raw["native_type"] != "DfsMetadataShardConfig" or raw["encoding"] != "json-utf8":
                raise Rejected(f"snapshot {name}: wrong native configuration encoding")
            actual = json.loads(raw["data"], object_pairs_hook=_pairs)
            current_fields = {"file_system_id", "shard_id", "primary_node_id", "replica_node_ids", "epoch", "assignment", "default_policy"}
            if not current_fields - {"replica_node_ids"} <= set(actual) or not set(actual) <= current_fields:
                raise Rejected(f"snapshot {name}: native config is not the complete current schema")
            for field in ("file_system_id", "shard_id", "primary_node_id", "replica_node_ids"):
                if actual.get(field, [] if field == "replica_node_ids" else None) != config[field]:
                    raise Rejected(f"snapshot {name}: config tuple mismatch {field}")
            if type(actual.get("epoch")) is not int or actual["epoch"] != decimal_u64(config["epoch"]):
                raise Rejected(f"snapshot {name}: native config epoch mismatch")
            for native_field, blob_field in (("assignment", "assignment_blob"), ("default_policy", "default_policy_blob")):
                evidence = blobs[config[blob_field]]
                if evidence["encoding"] != "json-utf8" or json.loads(evidence["data"], object_pairs_hook=_pairs) != actual.get(native_field):
                    raise Rejected(f"snapshot {name}: config {native_field} mismatch")
    seen: set[str] = set()
    sequence = {owner: 0 for owner in owners}
    for event in trace["events"]:
        if event["event_id"] in seen or event["owner_id"] not in owners:
            raise Rejected("duplicate event or unregistered owner")
        ordinal = decimal_u64(event["owner_sequence"], "owner_sequence")
        if ordinal != sequence[event["owner_id"]] + 1:
            raise Rejected("native owner ordinal gap/reorder")
        sequence[event["owner_id"]] = ordinal
        if not set(event["caused_by"]).issubset(seen):
            raise Rejected("missing/forward causal edge; supply a complete linearization")
        for field in ("before_snapshot", "after_snapshot"):
            if event[field] not in snapshots:
                raise Rejected(f"event {event['event_id']}: missing {field}")
            if snapshots[event[field]]["owner_id"] != event["owner_id"]:
                raise Rejected(f"event {event['event_id']}: borrowed snapshot from another owner/root")
        if event["action"]["kind"] == "capture-failure":
            raise Rejected("native capture failure")
        if event["action"]["kind"] == "kv-observation" and event["action"]["observation_kind"] == "discontinuity":
            raise Rejected("KV observation discontinuity")
        seen.add(event["event_id"])
    if set(terminal["final_owner_sequences"]) != set(owners):
        raise Rejected("terminal owner sequence coverage differs")
    for owner, end in terminal["final_owner_sequences"].items():
        if decimal_u64(end) != sequence[owner]:
            raise Rejected("terminal native ordinal does not match complete capture")
    if not trace["events"]:
        raise Unavailable("no observed native semantic events")
    trace_artifact_binding(trace, artifact_binding)
    cut = trace["initial_cut"]
    if not cut["baseline_snapshot_ids"] or any(key not in snapshots for key in cut["baseline_snapshot_ids"]):
        raise Rejected("missing certified native initial-cut snapshots")
    for field in ("baseline_validation_blob",):
        if cut[field] not in blobs:
            raise Rejected(f"missing native initial-cut evidence {field}")
    if set(cut["native_prefix_by_owner"]) != set(owners):
        raise Rejected("certified initial-cut owner coverage differs")



def native_uint64(value: Any, where: str) -> int:
    """Native serde JSON blobs are parsed losslessly by Python; no JS Number hop."""
    if type(value) is int and 0 <= value <= MAX_U64:
        return value
    return decimal_u64(value, where)


def native_json_blob(blobs: dict[str, Any], reference: str, native_type: str) -> dict[str, Any]:
    blob = blobs.get(reference)
    if blob is None:
        raise Unavailable(f"missing native {native_type} blob {reference}")
    if blob["encoding"] != "json-utf8" or blob["native_type"] != native_type:
        raise Unavailable(f"unsupported native encoding/type for {reference}")
    data = blob["data"].encode()
    if digest_bytes(data) != blob["sha256"]:
        raise Rejected(f"native blob integrity mismatch {reference}")
    value = json.loads(blob["data"], object_pairs_hook=_pairs)
    if not isinstance(value, dict):
        raise Rejected(f"native {native_type} must decode to an object")
    return value


VISIBLE_NATIVE_TABLES = (
    "objects", "file_system_objects", "paths", "inodes", "dirfrags", "dentries",
    "hardlinks", "compacted_tombstones", "versions", "version_pins",
    "cross_shard_pin_transfers", "visible_commits", "namespace_transactions",
)


def project_held_native_records(tables: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
    """Read actual v7 records. This storage fragment alone is not full conformance."""
    records = {}
    for record in tables.get("prepared_operations_v7", []):
        digest, payload = record["digest"], record["payload"]
        for field in ("protocol_version", "file_system_id", "shard_id", "owner_epoch", "operation_id"):
            if digest[field] != payload[field]:
                raise Rejected(f"native v7 payload/digest scope mismatch {field}")
        if digest["protocol_version"] != 7 or digest["file_system_id"] != scope["file_system_id"] or digest["shard_id"] != scope["shard_id"]:
            raise Rejected("foreign/unsupported native v7 record")
        prepare_sequence = native_uint64(digest["prepare_sequence"], "prepare_sequence")
        if prepare_sequence == 0 or not digest["payload_digest"]:
            raise Rejected("native v7 record has no exact durable preparation identity")
        operation = digest["operation_id"]
        if operation in records:
            raise Rejected("duplicate native prepared-operation identity")
        entry = record.get("committed_entry")
        status = record["status"]
        committed = status == "committed"
        if status not in ("prepared", "committed"):
            raise Unavailable("native v7 status outside bounded prepared/committed profile")
        if committed != (entry is not None):
            raise Rejected("native v7 terminal status lacks exact commit reference")
        if entry is not None:
            if set(entry) != {"shard_id", "sequence", "created_at", "digest"}:
                raise Rejected("native terminal reference has a different schema")
            if (entry["shard_id"] != digest["shard_id"] or native_uint64(entry["sequence"], "commit_sequence") <= prepare_sequence
                    or entry["digest"] != digest):
                raise Rejected("native v7 commit names a different prepared scope/payload/sequence")
        records[operation] = {"digest": digest, "payload": payload, "committed": committed,
                              "prepare_sequence": prepare_sequence, "commit_entry": entry}
    return records


def validate_prepare_visibility(before: dict[str, Any], after: dict[str, Any]) -> None:
    """Hidden prepare cannot mutate any actual visible native table footprint."""
    for table in VISIBLE_NATIVE_TABLES:
        if before.get(table, []) != after.get(table, []):
            raise Rejected(f"native hidden prepare changed visible table {table}")


def quint_literal(value: Any, depth: int = 0) -> str:
    if depth > 64:
        raise Rejected("finite projection nesting bound exceeded")
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        if abs(value) > MODEL_MAX_INTEGER:
            raise Rejected("finite model integer must be lossless/bounded; native u64 stays decimal")
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, dict):
        if set(value) == {"$set"}:
            values = value["$set"]
            if not isinstance(values, list) or len({json.dumps(x, sort_keys=True) for x in values}) != len(values):
                raise Rejected("invalid finite set")
            return "Set(" + ", ".join(quint_literal(x, depth + 1) for x in values) + ")"
        if set(value) == {"$map"}:
            pairs = value["$map"]
            if not isinstance(pairs, list) or not pairs:
                raise Unavailable("empty/untyped finite map not supported")
            keys = [pair[0] for pair in pairs if isinstance(pair, list) and len(pair) == 2]
            if len(keys) != len(pairs) or any(type(key) is not int for key in keys) or len(set(keys)) != len(keys):
                raise Rejected("finite map requires distinct integer keys")
            key = f"trace_key_{depth}"
            result = quint_literal(pairs[-1][1], depth + 1)
            for native_key, item in reversed(pairs[:-1]):
                result = f"if ({key} == {native_key}) {quint_literal(item, depth + 1)} else {result}"
            return f"Set({', '.join(map(str, keys))}).mapBy({key} => {result})"
        if any(not re.fullmatch(r"[A-Za-z][A-Za-z_0-9]*", key) for key in value):
            raise Rejected("invalid Quint record field")
        return "{ " + ", ".join(f"{key}: {quint_literal(item, depth + 1)}" for key, item in value.items()) + " }"
    raise Rejected("projection requires records and explicit $set/$map tags; no ITF or raw arrays")


def standard_actions(model_text: str) -> set[str]:
    declarations = set(re.findall(r"\baction\s+(\w+)", model_text))
    match = re.search(r"\baction\s+step\s*=([\s\S]*?)(?=\n\s*(?:action|def|pure)\b|\n}\s*$)", model_text)
    if not match:
        raise Unavailable("model has no extractable ordinary step relation")
    return declarations.intersection(re.findall(r"\b\w+\b", match.group(1))) - {"init", "step"}


def verify_quint_boundary(states: list[dict[str, Any]], transitions: list[dict[str, Any]],
                          *, model: Path, checks: Path, quint: str = "quint",
                          timeout: float = 45.0) -> dict[str, Any]:
    """Check imported Quint action enablement AND observed pre/post, never synthesize states."""
    if len(states) != len(transitions) + 1 or not transitions:
        raise Rejected("finite boundary needs exactly n+1 observed states and n transitions")
    if not model.exists() or not checks.exists():
        raise Unavailable("missing whole-shard model/checks")
    model_text = model.read_text()
    if not re.search(r"module\s+dfs_whole_shard\s*{", model_text):
        raise Rejected("wrong model module; expected dfs_whole_shard")
    try:
        version = subprocess.check_output([quint, "--version"], text=True, timeout=timeout).strip()
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise Unavailable(f"Quint version unavailable: {exc}") from exc
    if version != "0.32.0":
        raise Unavailable(f"Quint 0.32.0 required; observed {version}")
    allowed = standard_actions(model_text)
    literals = [quint_literal(state) for state in states]
    branches = []
    for index, transition in enumerate(transitions):
        if set(transition) != {"action", "arguments"} or transition["action"] not in allowed:
            raise Rejected(f"transition {index}: action outside ordinary model step")
        args = transition["arguments"]
        if not isinstance(args, list):
            raise Rejected("action arguments must be explicit finite values")
        call = transition["action"] + ("(" + ", ".join(quint_literal(arg) for arg in args) + ")" if args else "")
        branches.append(f"if (trace_cursor == {index}) all {{ s == observed_{index}, {call}, trace_cursor' = trace_cursor + 1 }}.expect(s == observed_{index + 1}) else ")
    source = 'module shard_trace_replay {\n'
    source += '  import dfs_whole_shard.* from ' + json.dumps(str(model.resolve().with_suffix(""))) + '\n'
    source += '  import dfs_whole_shard_safety from ' + json.dumps(str(checks.resolve().with_suffix(""))) + '\n'
    for index, literal in enumerate(literals):
        source += f"  pure val observed_{index}: State = {literal}\n"
    source += "  var trace_cursor: int\n"
    source += "  action traceInit = all { initFromCertifiedCut(observed_0.profile, observed_0.receipts, observed_0.bound, observed_0.online), trace_cursor' = 0 }.expect(s == observed_0)\n"
    source += "  action traceStep = " + "".join(branches) + "all { s' = s, trace_cursor' = trace_cursor }\n"
    source += f"  def terminalWitness: bool = trace_cursor == {len(transitions)} and s == observed_{len(transitions)}\n"
    source += "  def traceRefinementSafety: bool = dfs_whole_shard_safety::safety\n}\n"
    with tempfile.TemporaryDirectory(prefix="sinomo-shard-trace-") as temp:
        driver = Path(temp) / "shard_trace_replay.qnt"
        for imported in (model, checks):
            absolute = str(imported.resolve().with_suffix(""))
            relative = os.path.relpath(absolute, temp)
            source = source.replace(json.dumps(absolute), json.dumps(relative))
        driver.write_text(source)
        args = [quint, "run", str(driver), "--main=shard_trace_replay", "--init=traceInit", "--step=traceStep",
                "--backend=typescript", "--invariant=traceRefinementSafety", "--witnesses=terminalWitness", "--max-samples=1",
                f"--max-steps={len(transitions)}", "--seed=1", "--verbosity=1"]
        try:
            result = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                                    env={**os.environ, "NO_COLOR": "1"}, check=False)
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            raise Unavailable(f"Quint replay unavailable: {exc}") from exc
        output = result.stdout + result.stderr
        if result.returncode != 0 or not re.search(r"terminalWitness was witnessed in [1-9][0-9]* trace", output):
            raise Rejected("Quint transition disabled, post-state mismatched, invariant failed, or complete terminal not reached: " + output[-3000:])
        return {"quint_enabled_transition_replay": "verified", "transitions": len(transitions),
                "model_sha256": formal_source_hash(model, checks), "transition_model_sha256": digest_bytes(model.read_bytes()),
                "checks_sha256": digest_bytes(checks.read_bytes()),
                "classification": "finite observed-boundary replay; native conformance requires native projector"}


def check_trace(trace_path: Path, *, build_id: str, source_revision: str,
                model: Path = MODEL_PATH, checks: Path = CHECKS_PATH,
                projection_path: Path | None = None,
                artifact_manifest: Path | None = None, source_manifest: Path | None = None,
                executable: Path | None = None) -> dict[str, Any]:
    if not model.exists() or not checks.exists():
        raise Unavailable("whole-shard model/checks not installed")
    trace = read_json(trace_path)
    if not isinstance(trace, dict):
        raise Rejected("native trace envelope must be an object")
    schema = read_json(SCHEMA_PATH)
    if artifact_manifest is None or source_manifest is None:
        raise Unavailable("native conformance requires --artifact-manifest and --source-manifest")
    artifact_binding = verified_artifact_manifest(artifact_manifest, source_manifest, executable)
    preflight_native(trace, schema=schema, source_revision=source_revision,
                     model_sha256=formal_source_hash(model, checks), build_id=build_id,
                     artifact_binding=artifact_binding)
    if projection_path is not None:
        raise Rejected("externally supplied finite states are not native observation; projector derives states from source-bound native facts")
    path = Path(__file__).with_name("shard-native-projector.py")
    if not path.exists():
        raise Unavailable("native projector is missing")
    spec = importlib.util.spec_from_file_location("sinomo_native_shard_projector", path)
    if spec is None or spec.loader is None:
        raise Unavailable("native projector cannot be loaded")
    projector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(projector)
    try:
        result = projector.verify_native_trace(trace, verify_quint_boundary, model=model, checks=checks, artifact_binding=artifact_binding)
    except projector.Rejected as exc:
        raise Rejected(str(exc)) from exc
    except projector.Unavailable as exc:
        raise Unavailable(str(exc)) from exc
    return {"status": "pass", "native_conformance": "pass",
            "classification": "bounded native observed transition refinement; not a proof of Raft/storage/OS mechanisms",
            "implementation_revision": source_revision, "implementation_build_id": build_id,
            "artifact_binding": artifact_binding,
            "artifact_manifest_sha256": digest_bytes(artifact_manifest.read_bytes()),
            "native_trace_sha256": digest_bytes(trace_path.read_bytes()),
            "model_sha256": formal_source_hash(model, checks),
            "schema_sha256": digest_bytes(SCHEMA_PATH.read_bytes()),
            "profile": result["projection"]["profile"], "quint_replay": result["quint_replay"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--artifact-manifest", type=Path, required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--executable", type=Path, help="immutable copy of the exact captured executable")
    parser.add_argument("--model", type=Path, default=MODEL_PATH)
    parser.add_argument("--checks", type=Path, default=CHECKS_PATH)
    parser.add_argument("--projection", type=Path, help="source-bound native observed finite boundary sidecar; no ITF")
    args = parser.parse_args(argv)
    try:
        result = check_trace(args.trace, build_id=args.build_id, source_revision=args.source_revision,
                             model=args.model, checks=args.checks, projection_path=args.projection,
                             artifact_manifest=args.artifact_manifest, source_manifest=args.source_manifest,
                             executable=args.executable)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "pass" else 2
    except Rejected as exc:
        print(json.dumps({"status": "fail", "native_conformance": "fail", "error": str(exc)}, sort_keys=True))
        return 1
    except Unavailable as exc:
        print(json.dumps({"status": "unavailable", "native_conformance": "unavailable", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
