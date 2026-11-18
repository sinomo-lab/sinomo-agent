#!/usr/bin/env python3
"""Source-bound native storage/evidence to the finite whole-shard observation.

This module does not execute or predict model transitions. Every state is rebuilt
from held native tables, certified cuts and retained operational observations.
The driver must first validate the envelope/schema/source bindings, then call
project_native_trace, then its existing Quint action.expect replay. Missing facts
are Unavailable; contradictions are Rejected. Synthetic self-tests prove only
projector sensitivity. This module alone never claims native conformance.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
from typing import Any, Callable

MAX_U64 = (1 << 64) - 1
PROJECTION_VERSION = 1
NODES, OPERATIONS, SEQUENCES = range(1, 5), range(1, 4), range(1, 7)
VISIBLE_TABLES = ("objects", "file_system_objects", "paths", "inodes", "dirfrags",
                  "dentries", "hardlinks", "compacted_tombstones", "versions",
                  "version_pins", "cross_shard_pin_transfers")


class Rejected(ValueError):
    pass


class Unavailable(RuntimeError):
    pass


def u64(value: Any, label: str) -> int:
    # Native serde blobs use Python's exact integer parser. Envelope DTO fields
    # use decimal strings. Never accept floats, booleans or a JS Number bridge.
    if type(value) is int and 0 <= value <= MAX_U64:
        return value
    if isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        number = int(value)
        if number <= MAX_U64:
            return number
    raise Rejected(f"{label}: invalid lossless u64")


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise Rejected(f"duplicate native JSON field {key}")
        result[key] = value
    return result


def exact_json(data: bytes):
    try:
        return json.loads(data, object_pairs_hook=pairs,
                          parse_constant=lambda x: (_ for _ in ()).throw(Rejected(f"nonfinite {x}")))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise Rejected(f"invalid native JSON: {exc}") from exc


def finite_set(values):
    return {"$set": sorted(set(values))}


def finite_map(values):
    return {"$map": [[key, value] for key, value in sorted(values.items())]}


def injective(mapping, allowed, label):
    if not isinstance(mapping, dict) or any(type(x) is not int or x not in allowed for x in mapping.values()):
        raise Unavailable(f"{label}: finite bound exceeded")
    if len(mapping.values()) != len(set(mapping.values())):
        raise Rejected(f"{label}: aliases are not injective")
    return mapping


# Small independent BLAKE3 implementation avoids a subprocess or installed
# dependency at this pure boundary. Exact frame bytes never pass through JS.
_IV = [0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
       0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19]
_PERM = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8]


def _compress(cv, words, counter, length, flags):
    v = cv[:] + _IV[:4] + [counter & 0xffffffff, counter >> 32, length, flags]
    def g(a, b, c, d, x, y):
        def rot(value, count):
            return ((value >> count) | (value << (32-count))) & 0xffffffff
        v[a] = (v[a] + v[b] + x) & 0xffffffff; v[d] = rot(v[d] ^ v[a], 16)
        v[c] = (v[c] + v[d]) & 0xffffffff; v[b] = rot(v[b] ^ v[c], 12)
        v[a] = (v[a] + v[b] + y) & 0xffffffff; v[d] = rot(v[d] ^ v[a], 8)
        v[c] = (v[c] + v[d]) & 0xffffffff; v[b] = rot(v[b] ^ v[c], 7)
    m = words[:]
    for _ in range(7):
        g(0,4,8,12,m[0],m[1]); g(1,5,9,13,m[2],m[3]); g(2,6,10,14,m[4],m[5]); g(3,7,11,15,m[6],m[7])
        g(0,5,10,15,m[8],m[9]); g(1,6,11,12,m[10],m[11]); g(2,7,8,13,m[12],m[13]); g(3,4,9,14,m[14],m[15])
        m = [m[i] for i in _PERM]
    return [v[i] ^ v[i+8] for i in range(8)] + [v[i+8] ^ cv[i] for i in range(8)]


def _chunk(data, counter):
    cv = _IV[:]
    blocks = [data[i:i+64] for i in range(0, len(data), 64)] or [b""]
    for index, block in enumerate(blocks):
        words = [int.from_bytes(block.ljust(64, b"\0")[i:i+4], 'little') for i in range(0, 64, 4)]
        flags = (1 if index == 0 else 0) | (2 if index == len(blocks)-1 else 0)
        output = (cv, words, counter, len(block), flags)
        if index < len(blocks)-1:
            cv = _compress(*output)[:8]
    return output


def blake3(data: bytes) -> str:
    chunks = [data[i:i+1024] for i in range(0, len(data), 1024)] or [b""]
    stack = []
    for index, chunk in enumerate(chunks[:-1]):
        cv = _compress(*_chunk(chunk, index))[:8]
        count = index + 1
        while count % 2 == 0:
            cv = _compress(_IV[:], stack.pop() + cv, 0, 64, 4)[:8]
            count //= 2
        stack.append(cv)
    output = _chunk(chunks[-1], len(chunks)-1)
    while stack:
        output = (_IV[:], stack.pop() + _compress(*output)[:8], 0, 64, 4)
    root = _compress(output[0], output[1], 0, output[3], output[4] | 8)
    return b''.join(x.to_bytes(4, 'little') for x in root[:8]).hex()


def empty_entry():
    return dict(kind=0, op=0, digest=0, body=0, epoch=0)


def empty_prefix():
    return {n: empty_entry() for n in SEQUENCES}


def extend_native_prefix(previous, sequence, exact_frame):
    if previous.get('version') != 2 or not re.fullmatch(r'dfs-metadata-prefix-v2:blake3:[0-9a-f]{64}', previous.get('digest', '')):
        raise Unavailable('native prefix is not the current exact-frame chain')
    prior = u64(previous['journal_sequence'], 'native prior prefix')
    if sequence != prior + 1:
        raise Rejected('native prefix ancestry has a frame gap')
    parts = (b'journal-frame', (2).to_bytes(2, 'big'), prior.to_bytes(8, 'big'),
             previous['digest'].encode(), sequence.to_bytes(8, 'big'), exact_frame)
    raw = b'sinomo-dfs-metadata-prefix-proof\0v2\0'
    raw += b''.join(len(part).to_bytes(8, 'big') + part for part in parts)
    return dict(version=2, journal_sequence=sequence,
                digest='dfs-metadata-prefix-v2:blake3:' + blake3(raw))


def empty_prepare():
    return dict(present=False, op=0, digest=0, body=0, epoch=0, revision=0,
                primary=0, seq=0, base=0, target=0, receipts=finite_set([]), prefix=finite_map(empty_prefix()))






def empty_commit():
    return dict(present=False, op=0, digest=0, body=0, epoch=0, revision=0, primary=0,
                seq=0, base=0, target=0, members=finite_set([]), admittedEpoch=0,
                admittedRevision=0, admittedPrimary=0, admittedFrozen=False,
                admittedPending=False, receipts=finite_set([]), acks=finite_set([]),
                path=0)


def native_bytes(value, label):
    if isinstance(value, list) and all(type(x) is int and 0 <= x <= 255 for x in value):
        return bytes(value)
    raise Rejected(f'{label}: native serde byte vector required')


def native_digest_hex(value):
    if isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value):
        return value
    raw = native_bytes(value, 'KvCommandDigest')
    if len(raw) != 32:
        raise Rejected('native KvCommandDigest length differs')
    return raw.hex()


def select_primary_held_snapshot(config, peers, snapshots, owners, scope):
    primary = config['primary_node_id']
    peer = peers.get(primary)
    if peer is None or peer.get('held_snapshot_id') not in snapshots:
        raise Unavailable('current primary native held cut unavailable')
    snapshot = snapshots[peer['held_snapshot_id']]
    if (snapshot['owner_id'] != peer['owner_id'] or snapshot['scope'] != scope
            or owners[peer['owner_id']]['node_id'] != primary
            or snapshot['local_certified_prefix'] != peer.get('materialized_prefix', peer['prefix'])):
        raise Rejected('global primary cut borrowed from another actual node/root')
    return snapshot


def sealed_content_requirement(payload, digest, canonical):
    """Content W for this exact original sealed body; never metadata authority."""
    fields = {"protocol_version", "file_system_id", "shard_id", "owner_epoch",
        "operation_id", "journal_operation_id", "expected_journal_sequence",
        "expected_object_versions", "expected_path_occupants", "transaction",
        "metadata_effects", "content_receipts", "required_replica_node_ids",
        "prepared_intent", "terminal_intent", "completed_request",
        "completed_request_completion"}
    if not isinstance(payload, dict) or set(payload) != fields:
        raise Unavailable("sealed quorum needs the complete current payload shape")
    if not isinstance(digest, dict) or set(digest) != {
            "protocol_version", "file_system_id", "shard_id", "owner_epoch",
            "operation_id", "payload_digest", "prepare_sequence"}:
        raise Unavailable("sealed content requirement lacks its complete original digest")
    if u64(payload["owner_epoch"], "sealed payload epoch") != u64(digest["owner_epoch"], "sealed digest epoch"):
        raise Rejected("sealed content requirement changes its exact owner epoch")
    if exact_json(canonical) != payload:
        raise Rejected("sealed content classification differs from canonical bytes")
    if (payload["protocol_version"] != 7 or digest["protocol_version"] != 7
            or any(payload[k] != digest[k] for k in
                   ("file_system_id", "shard_id", "owner_epoch", "operation_id"))
            or u64(digest["prepare_sequence"], "sealed prepare sequence") == 0
            or digest["payload_digest"] != "dfs-prepared-operation-v7:blake3:" +
                blake3(b"sinomo.dfs.prepared-operation\0v7\0" + canonical)):
        raise Rejected("sealed content requirement changes its exact native digest")
    transaction = payload["transaction"]
    if (not isinstance(transaction, dict) or transaction.get("file_system_id") != payload["file_system_id"]
            or not isinstance(transaction.get("objects"), list)
            or not isinstance(transaction.get("versions", []), list)
            or not isinstance(payload["content_receipts"], list)
            or not isinstance(payload["required_replica_node_ids"], list)
            or not isinstance(payload["metadata_effects"], list)):
        raise Unavailable("sealed quorum needs actual typed transaction and receipt arrays")
    for effect in payload["metadata_effects"]:
        if not isinstance(effect, dict):
            raise Unavailable("sealed quorum needs typed metadata effect rows")
        if effect.get("type") == "put-intent":
            intent = effect.get("intent")
            if (not isinstance(intent, dict)
                    or not isinstance(intent.get("policy"), dict)):
                raise Unavailable("sealed quorum needs a typed metadata intent and policy")
        elif effect.get("type") == "put-receipt":
            record = effect.get("receipt")
            if (not isinstance(record, dict)
                    or not isinstance(record.get("receipt"), dict)):
                raise Unavailable("sealed quorum needs typed metadata receipt rows")
    prepared, terminal = payload["prepared_intent"], payload["terminal_intent"]
    if prepared is None and terminal is None:
        # Current transaction serde omits empty versions and optional extension
        # fields; only their declared empty representation is admitted here.
        if (payload["content_receipts"] or payload["required_replica_node_ids"]
                or transaction["objects"] or transaction.get("versions", [])
                or not payload["metadata_effects"]
                or payload["completed_request"] is not None
                or payload["completed_request_completion"] is not None
                or any(transaction.get(k) for k in ("advisory_lock_states",
                    "extension_expectations", "file_version_pins", "contribution_digest",
                    "content_reference_identity"))):
            raise Rejected("metadata-only quorum cannot hide content or namespace publication")
        for effect in payload["metadata_effects"]:
            kind = effect.get("type")
            if kind == "put-intent" and set(effect) == {"type", "intent"}:
                fs = effect["intent"].get("file_system_id")
            elif kind == "put-receipt" and set(effect) == {"type", "receipt"}:
                fs = effect["receipt"].get("receipt", {}).get("file_system_id")
            else:
                raise Unavailable("metadata-only quorum supports only actual intent/receipt effects")
            if fs != payload["file_system_id"]:
                raise Rejected("metadata-only effect crosses its actual filesystem")
        return dict(min_commit_replicas=0, content_replica_node_ids=[])
    if not isinstance(prepared, dict) or not isinstance(terminal, dict):
        raise Rejected("sealed content body has a partial preparation/terminal pair")
    identity = ("file_system_id", "object_id", "target_version", "intent_id", "owner_epoch",
                "content_hash", "desired_replica_node_ids", "policy", "coordinator_node_id",
                "operation_id", "operation_order_token", "publication")
    if (any(prepared.get(k) != terminal.get(k) for k in identity)
            or prepared.get("file_system_id") != payload["file_system_id"]
            or prepared.get("owner_epoch") != payload["owner_epoch"]
            or terminal.get("status") != "committed"):
        raise Rejected("sealed content quorum changes its immutable terminal identity")
    plan = prepared.get("desired_replica_node_ids")
    policy = prepared.get("policy", {})
    if not isinstance(policy, dict) or not isinstance(terminal.get("policy"), dict):
        raise Unavailable("sealed content quorum needs its typed actual policy")
    if (not isinstance(plan, list) or not plan
            or any(not isinstance(value, str) for value in plan)):
        raise Unavailable("sealed content quorum needs typed actual plan node IDs")
    if len(set(plan)) != len(plan):
        raise Rejected("sealed content quorum lacks its exact unique actual plan")
    w = u64(policy.get("min_commit_replicas"), "sealed content W")
    if not 1 <= w <= len(plan) or u64(policy.get("desired_replicas"), "sealed content N") != len(plan):
        raise Rejected("sealed content quorum has an invalid actual W/N plan")
    required = payload["required_replica_node_ids"]
    if any(not isinstance(value, str) for value in required):
        raise Unavailable("sealed content quorum needs typed required node IDs")
    if len(set(required)) != len(required) or not set(required) <= set(plan) or len(required) < w:
        raise Rejected("sealed content quorum lacks the actual required W holders")
    receipts = payload["content_receipts"]
    nodes = []
    for record in receipts:
        if (not isinstance(record, dict)
                or not isinstance(record.get("receipt"), dict)
                or not isinstance(record.get("replica_node_id"), str)):
            raise Unavailable("sealed content quorum needs typed actual receipt rows")
        receipt = record["receipt"]
        node = record["replica_node_id"]
        if (node not in plan or record.get("content_hash") != prepared.get("content_hash")
                or receipt.get("file_system_id") != payload["file_system_id"]
                or receipt.get("object_id") != prepared.get("object_id")
                or receipt.get("committed_version") != prepared.get("target_version")):
            raise Rejected("sealed content receipt differs from its exact immutable tuple")
        nodes.append(node)
    if len(set(nodes)) != len(nodes) or not set(required) <= set(nodes):
        raise Rejected("sealed content quorum lacks its exact receipt rows")
    return dict(min_commit_replicas=w, content_replica_node_ids=plan)


def exact_receipt_replacement(prior, replacement):
    if (not isinstance(prior,dict) or not isinstance(replacement,dict)
            or set(prior)!=set(replacement) or not isinstance(prior.get('receipt'),dict)
            or not isinstance(replacement.get('receipt'),dict)
            or set(prior['receipt'])!=set(replacement['receipt'])
            or 'received_at' not in prior or 'event_id' not in prior['receipt']):
        raise Unavailable('receipt replacement needs complete actual current rows')
    immutable=copy.deepcopy(replacement)
    immutable['received_at']=prior['received_at']
    immutable['receipt']['event_id']=prior['receipt']['event_id']
    return immutable==prior


def exact_intent_terminal(prior, payload):
    prepared, terminal = payload['prepared_intent'], payload['terminal_intent']
    if not isinstance(prepared,dict) or not isinstance(terminal,dict) or prepared!=prior:
        raise Rejected('intent supersession lacks its complete exact sealed preparation')
    object = terminal.get('object_metadata')
    if not isinstance(object,dict) or terminal.get('status')!='committed':
        raise Rejected('intent supersession has no complete committed terminal object')
    expected=copy.deepcopy(prepared)
    expected.update(status='committed',updated_at=terminal['updated_at'],object_metadata=object)
    if (expected!=terminal or object['file_system_id']!=terminal['file_system_id']
            or object['object_id']!=terminal['object_id'] or object['version']!=terminal['target_version']
            or object.get('content_hash')!=terminal['content_hash']
            or object not in payload['transaction']['objects']
            or not any(version['file_system_id']==terminal['file_system_id']
                and version['object_id']==terminal['object_id'] and version['version']==terminal['target_version']
                and version['content_hash']==terminal['content_hash']
                and version['size_bytes']==object['content_size_bytes']
                for version in payload['transaction'].get('versions',[]))):
        raise Rejected('intent terminalization changed its immutable tuple or sealed effects')
    return terminal


class NativeProjectorV1:
    """Pure adapter. The enclosing checker validates source/schema/build first."""
    def __init__(self, trace: dict[str, Any], *, artifact_binding: dict[str, Any]):
        if trace.get('schema_version') != 1 or trace.get('projection_version') != 1:
            raise Unavailable('unsupported native trace/projection version')
        if type(trace.get('metadata_protocol_version')) is not int or trace['metadata_protocol_version'] != 7:
            raise Unavailable('native trace requires the actual current PreparedV7 protocol')
        if any(k in trace for k in ('states', 'model_state', 'ITF', '#meta')):
            raise Rejected('model/predicted state cannot be native input')
        self.trace, self.blobs, self.scope = trace, trace['blobs'], trace['scope']
        self.owners = {x['owner_id']: x for x in trace['owners']}
        if len(self.owners) != len(trace['owners']):
            raise Rejected('duplicate owner')
        lanes = {}
        for owner in self.owners.values():
            if any(not owner.get(k) for k in ('root_lease_token', 'component_incarnation', 'storage_generation_token', 'lane_incarnation')):
                raise Unavailable('owner/root/component/lane incarnation not certified')
            key = owner['node_id']
            lane = (owner['component_incarnation'], owner['lane_incarnation'])
            if key in lanes and lanes[key] != lane:
                raise Unavailable('more than one observed writer lane per node')
            lanes[key] = lane
        d = trace['finite_dictionary']
        self.nodes = injective(d['node_aliases'], NODES, 'nodes')
        self.operations = injective(d['operation_aliases'], OPERATIONS, 'operations')
        # Payload tokens must be scoped by operation: two independent immutable
        # bodies may both have model body=1. Never identify their native bytes.
        self.payloads = d.get('operation_payload_aliases')
        if self.payloads is None:
            raise Unavailable('operation-scoped payload dictionary is required; global payload aliases cannot represent two body=1 operations')
        for op, values in self.payloads.items():
            if op not in self.operations:
                raise Rejected('payload dictionary has unknown operation')
            injective(values, range(1, 3), 'operation payloads')
        self.configs = injective(d['config_aliases'], range(1, 3), 'whole-config identities')
        if set(self.nodes) != {o['node_id'] for o in self.owners.values()}:
            raise Unavailable('each finite node needs an actual owner/certified-cut binding')
        self.cut_aliases = {}
        for cut in d['sequence_cuts']:
            key = self.prefix_key(cut['owner_id'], cut['native_prefix'])
            alias = cut['model_sequence']
            if type(alias) is not int or not 0 <= alias <= 6 or key in self.cut_aliases:
                raise Rejected('duplicate or out-of-bound native sequence cut')
            self.cut_aliases[key] = alias
        self.intervals = {}
        for interval in d['semantic_intervals']:
            eid = interval['action_event_id']
            if eid in self.intervals:
                raise Rejected('duplicate semantic interval')
            self.intervals[eid] = interval
        self.initial = self.blob(trace['initial_cut']['baseline_validation_blob'], 'DfsNativeInitialCutCertificateV1')
        self.require_certificate(self.initial, 'initial-cut')
        captured_binding = self.blob(self.initial.get('source_artifact_binding_blob'), 'NativeTraceArtifactBindingV1')
        if not artifact_binding or captured_binding != artifact_binding:
            raise Rejected('initial-cut executable/source binding differs from verified capture manifest')
        if not self.initial.get('complete_native_visibility'):
            raise Unavailable('initial cut is not a complete native visibility baseline')
        self.baseline_records = {}
        self.baseline_operations = set()
        self.initial_payloads = {}
        self.profile = self.derive_profile()
        self.epoch_aliases = {}
        self.prepare_witnesses = {}
        self.guard_witnesses = {}
        self.observed_flags = {}
        self.last = 'init'

    def blob_bytes(self, reference):
        blob = self.blobs.get(reference)
        if blob is None:
            raise Unavailable(f'missing native blob {reference}')
        try:
            raw = blob['data'].encode('utf-8') if blob['encoding'] == 'json-utf8' else base64.b64decode(blob['data'], validate=True)
        except (ValueError, UnicodeError) as exc:
            raise Rejected('invalid native blob encoding') from exc
        if hashlib.sha256(raw).hexdigest() != blob['sha256']:
            raise Rejected('native blob hash mismatch')
        return raw

    def blob(self, reference, native_type):
        if not reference:
            raise Unavailable(f'missing {native_type} observation')
        if self.blobs.get(reference, {}).get('native_type') != native_type:
            raise Unavailable(f'wrong/missing native type: {native_type}')
        value = exact_json(self.blob_bytes(reference))
        if not isinstance(value, dict):
            raise Rejected(f'{native_type} must be an object')
        if any(k in value for k in ('model_state', 'expected_state', 'quint_action', 'Case')):
            raise Rejected('predicted model fields in native observation')
        return value

    def require_certificate(self, certificate, label):
        if certificate.get('scope') != self.scope or certificate.get('implementation_revision') != self.trace['implementation_revision']:
            raise Rejected(f'{label}: source/scope binding differs')
        if not certificate.get('complete') or not certificate.get('continuous'):
            raise Unavailable(f'{label}: missing/discontinuous certification')
        bindings = certificate.get('owner_bindings')
        if bindings is None or bindings != list(self.owners.values()):
            raise Rejected(f'{label}: native root/incarnation ownership differs')

    def node(self, native):
        if native not in self.nodes:
            raise Unavailable('node outside finite dictionary')
        return self.nodes[native]

    def op(self, native):
        if native not in self.operations:
            raise Unavailable('operation outside finite dictionary')
        return self.operations[native]

    def node_set(self, native):
        if not isinstance(native, list) or len(set(native)) != len(native):
            raise Rejected('native node set has duplicates/invalid shape')
        return finite_set(self.node(n) for n in native)

    def prefix_key(self, owner, prefix):
        if owner not in self.owners:
            raise Rejected('prefix has no actual owner')
        if not isinstance(prefix, dict):
            raise Unavailable('actual native prefix evidence is absent')
        reference = prefix.get('native_prefix_blob')
        if reference:
            native = self.blob(reference, 'DfsMetadataPrefixProof')
            if (native.get('version') != prefix.get('version') or native.get('digest') != prefix.get('digest')
                    or u64(native.get('journal_sequence'), 'native prefix blob sequence') != u64(prefix.get('journal_sequence'), 'prefix sequence')):
                raise Rejected('prefix DTO differs from actual native prefix blob')
        return (owner, u64(prefix['journal_sequence'], 'prefix.sequence'), prefix['digest'], prefix['version'])

    def cut(self, owner, prefix):
        key = self.prefix_key(owner, prefix)
        if key not in self.cut_aliases:
            raise Unavailable('native prefix cut lacks certified finite alias')
        return self.cut_aliases[key]

    def config(self, tuple_value):
        raw = self.blob(tuple_value['native_config_blob'], 'DfsMetadataShardConfig')
        fingerprint = tuple_value['canonical_fingerprint']
        if fingerprint not in self.configs:
            raise Unavailable('full config identity outside finite dictionary')
        current_fields = {'file_system_id', 'shard_id', 'primary_node_id', 'replica_node_ids', 'epoch', 'assignment', 'default_policy'}
        if not current_fields - {'replica_node_ids'} <= set(raw) or not set(raw) <= current_fields:
            raise Rejected('native configuration is not the complete current schema')
        if 'namespace_prepare_codec' in tuple_value:
            raise Rejected('retired codec configuration cannot be projected as current')
        for key in ('file_system_id', 'shard_id', 'primary_node_id', 'replica_node_ids'):
            if raw.get(key, [] if key == 'replica_node_ids' else None) != tuple_value[key]:
                raise Rejected(f'full configuration mismatch: {key}')
        if u64(raw['epoch'], 'config.epoch') != u64(tuple_value['epoch'], 'config.epoch'):
            raise Rejected('native config epoch differs')
        for key, ref in (('assignment','assignment_blob'), ('default_policy','default_policy_blob')):
            if exact_json(self.blob_bytes(tuple_value[ref])) != raw[key]:
                raise Rejected(f'config {key} omitted/changed')
        if raw['file_system_id'] != self.scope['file_system_id'] or raw['shard_id'] != self.scope['shard_id']:
            raise Rejected('foreign native config')
        # Fingerprint validation is captured from the real canonical validator;
        # all actual current fields are compared, including assignment/policy.
        expected = self.initial['config_fingerprints'].get(fingerprint)
        if expected != raw:
            raise Rejected('config fingerprint does not bind complete native tuple')
        # Native serde omits an empty replica list; its declared current default
        # is still compared above against the explicit envelope tuple.
        raw.setdefault('replica_node_ids', [])
        alias = self.configs[fingerprint]
        epoch = u64(raw['epoch'], 'config.epoch')
        previous = self.epoch_aliases.setdefault(epoch, alias)
        if previous != alias:
            raise Unavailable('model cannot separate two whole configs at the same native epoch')
        return alias, raw

    def derive_profile(self):
        native = self.initial.get('profile')
        if native is None:
            raise Unavailable('certified actual node/voter/member/content/W profile absent')
        if native.get('standalone') is not False:
            raise Unavailable('native clustered profile requires explicit standalone=false; archived profile incompatible')
        if set(native['nodes']) != set(self.nodes):
            raise Rejected('profile node coverage differs')
        primary = native['initial_primary_node_id']
        if self.node(primary) != 1:
            raise Rejected('node 1 must bind actual retiring primary')
        required = u64(native['min_commit_replicas'], 'content W')
        replicas = native['content_replica_node_ids']
        if not 1 <= required <= len(set(replicas)):
            raise Unavailable('namespace-only/no-content or invalid content policy outside model')
        sealed = native.get('sealed_content_requirements')
        if not isinstance(sealed, dict) or set(sealed) != set(self.operations):
            raise Unavailable('each selected operation needs its original sealed content requirement')
        metadata = []
        for opid, row in sealed.items():
            ref = row['canonical_payload_blob']
            body = self.blob(ref, 'DfsPreparedOperationPayloadCanonicalV7')
            digest = row['native_digest']
            if (body['operation_id'] != opid or any(body[k] != self.scope[k] for k in ('file_system_id','shard_id'))
                    or self.payloads.get(opid, {}).get(digest['payload_digest']) != 1):
                raise Rejected('content requirement borrows a foreign or changed retry body')
            actual = sealed_content_requirement(body, digest, self.blob_bytes(ref))
            if row.get('requirement') != actual:
                raise Rejected('declared per-payload content quorum differs from exact sealed body')
            if actual['min_commit_replicas'] == 0:
                metadata.append(opid)
            elif actual['min_commit_replicas'] != required or actual['content_replica_node_ids'] != replicas:
                raise Unavailable('content body differs from actual finite content plan')
        return dict(standalone=False, nodes=self.node_set(native['nodes']), voters=self.node_set(native['raft_voter_node_ids']),
                    initialMembers=self.node_set(native['initial_metadata_members']),
                    replacementMembers=self.node_set(native['replacement_metadata_members']),
                    contentReplicas=self.node_set(replicas), requiredW=required,
                    metadataOnlyOperations=finite_set(self.op(op) for op in metadata))

    def verified_record(self, native, canonical_payload_reference=None):
        digest, payload = native['digest'], native['payload']
        for field in ('protocol_version', 'file_system_id', 'shard_id', 'owner_epoch', 'operation_id'):
            if digest[field] != payload[field]:
                raise Rejected('prepared payload/digest scope differs')
        if digest['protocol_version'] != 7 or any(digest[k] != self.scope[k] for k in ('file_system_id','shard_id')):
            raise Unavailable('foreign/legacy prepared record')
        # Re-encoding a nested serde body could change map order or integer
        # spelling. Require the actual canonical bytes used by the Rust digest.
        body_ref = canonical_payload_reference
        canonical = self.blob_bytes(body_ref) if body_ref else None
        if canonical is None:
            raise Unavailable('actual Rust canonical payload bytes absent')
        if exact_json(canonical) != payload:
            raise Rejected('canonical digest bytes differ from complete stored payload')
        expected = 'dfs-prepared-operation-v7:blake3:' + blake3(b'sinomo.dfs.prepared-operation\0v7\0' + canonical)
        if digest['payload_digest'] != expected:
            raise Rejected('canonical native payload digest mismatch')
        sequence = u64(digest['prepare_sequence'], 'prepare sequence')
        terminal = native.get('committed_entry')
        if (native['status'] == 'committed') != (terminal is not None):
            raise Rejected('committed record has no exact terminal reference')
        if native['status'] not in ('prepared','committed'):
            raise Unavailable('aborted/unsupported native prepared state')
        if terminal is not None:
            if set(terminal) != {'shard_id','sequence','created_at','digest'} or terminal.get('digest') != digest or terminal['shard_id'] != digest['shard_id'] or u64(terminal['sequence'], 'commit sequence') <= sequence:
                raise Rejected('digest-only commit reference mismatch')
        return payload, digest, terminal

    def record(self, native, canonical_payload_reference=None):
        payload, digest, terminal = self.verified_record(native, canonical_payload_reference)
        opid = digest['operation_id']; op = self.op(opid)
        if self.payloads.get(opid, {}).get(digest['payload_digest']) != 1:
            raise Unavailable('current immutable body must have operation-scoped alias 1')
        return op, payload, digest, terminal

    def certify_initial_baseline(self, facts):
        """Quotient only immutable committed rows at the actual held native cut.

        This is an observation boundary, not replay or authorization of historical
        operations. Every retained body and its original native coordinates remain
        in the certificate. Prepared rows and active dependencies cannot disappear.
        """
        cuts = self.initial.get('owner_cuts')
        if not isinstance(cuts, dict) or set(cuts) != set(self.owners):
            raise Unavailable('complete held per-owner initial-cut evidence absent')
        for peer in facts['peer_certified_cuts'].values():
            owner = peer['owner_id']; item = cuts[owner]
            if item.get('held_snapshot_id') != peer['held_snapshot_id'] or item.get('native_prefix') != peer['prefix']:
                raise Rejected('baseline owner cut differs from actual held peer cut')
            snapshot = self.trace['snapshots'][peer['held_snapshot_id']]
            tables, _ = self.held(snapshot)
            if item.get('held_tables_blob') != snapshot['held_tables_blob']:
                raise Rejected('baseline table evidence borrowed from another held cut')
            anchor = u64(peer['prefix']['journal_sequence'], 'baseline native anchor')
            if u64(snapshot['materialized_through_sequence'], 'baseline materialized') != anchor:
                raise Unavailable('initial baseline contains unmaterialized native work')
            candidates = item.get('terminal_records')
            if not isinstance(candidates, list):
                raise Unavailable('baseline terminal record inventory absent')
            certified = {}
            for witness in candidates:
                raw = self.blob(witness.get('record_blob'), 'DfsPreparedOperationRecord')
                payload, digest, terminal = self.verified_record(raw, witness.get('canonical_payload_blob'))
                opid = digest['operation_id']
                if opid in self.operations or opid in certified:
                    raise Rejected('baseline record overlaps finite operation or duplicates identity')
                if raw['status'] != 'committed' or terminal is None:
                    raise Unavailable('noncommitted prepared work cannot be excluded at initial cut')
                if u64(terminal['sequence'], 'baseline commit') > anchor:
                    raise Rejected('baseline commit lies beyond actual certified cut')
                if raw not in tables.get('prepared_operations_v7', []):
                    raise Rejected('baseline record is not present in the held native tables')
                commit = self.blob(witness.get('commit_reference_blob'), 'DfsPreparedOperationCommitReference')
                if commit != terminal:
                    raise Rejected('baseline exact commit reference differs')
                proof = self.blob(witness.get('prefix_ancestry_blob'), 'DfsNativeBaselinePrefixAncestryV1')
                self.require_certificate(proof, 'baseline prefix ancestry')
                self.validate_baseline_ancestry(proof, owner, peer['prefix'], commit)
                certified[opid] = raw
                self.baseline_operations.add(opid)
            for raw in tables.get('prepared_operations_v7', []):
                opid = raw['digest']['operation_id']
                if opid not in self.operations and certified.get(opid) != raw:
                    raise Unavailable('unmodeled prepared/terminal row lacks complete baseline evidence')
            if any(row['digest']['operation_id'] not in self.operations for row in tables.get('prepared_operation_evictions', [])):
                raise Unavailable('compacted baseline body/terminal evidence is outside the certified cut')
            self.baseline_records[owner] = certified
            self.validate_initial_dependencies(tables, item, facts['operation_observations'])
        if set(self.initial.get('unmodeled_operations', [])) != self.baseline_operations:
            raise Rejected('baseline unmodeled operation inventory differs from certified records')

    def validate_baseline_ancestry(self, proof, owner, anchor, commit):
        if proof.get('owner_id') != owner or self.prefix_key(owner, proof.get('native_anchor')) != self.prefix_key(owner, anchor) or proof.get('commit_reference') != commit:
            raise Rejected('baseline ancestry borrowed from another native owner/cut/commit')
        prior = proof.get('prior_prefix')
        frames = proof.get('frames')
        if not prior or not isinstance(frames, list) or not frames:
            raise Unavailable('baseline commit lacks retained exact-frame ancestry')
        if u64(prior['journal_sequence'], 'baseline prior') + 1 != u64(commit['sequence'], 'baseline commit'):
            raise Rejected('baseline ancestry does not begin at the exact commit frame')
        for index, item in enumerate(frames):
            raw = self.blob_bytes(item.get('exact_frame_blob'))
            entry = self.blob(item.get('decoded_entry_blob'), 'DfsMetadataJournalEntry')
            decode = self.blob(item.get('decoder_observation_blob'), 'DfsNativeJournalDecodeObservationV1')
            self.require_certificate(decode, 'baseline actual journal decoder')
            if decode.get('exact_frame_sha256') != hashlib.sha256(raw).hexdigest() or decode.get('decoded_entry_blob') != item['decoded_entry_blob']:
                raise Rejected('baseline native decoded entry differs from exact source frame')
            sequence = u64(entry['sequence'], 'baseline ancestry sequence')
            if entry['shard_id'] != self.scope['shard_id']:
                raise Rejected('baseline ancestry crosses physical shard')
            if index == 0:
                mutation = entry['mutation']
                if mutation.get('type') != 'commit-prepared-operation-v7' or mutation.get('digest') != commit['digest'] or mutation.get('file_system_id') != self.scope['file_system_id'] or mutation.get('operation_id') != commit['digest']['operation_id'] or entry['created_at'] != commit['created_at'] or sequence != u64(commit['sequence'], 'baseline commit'):
                    raise Rejected('baseline first retained frame differs from exact commit reference')
            prior = extend_native_prefix(prior, sequence, raw)
        if self.prefix_key(owner, prior) != self.prefix_key(owner, anchor):
            raise Rejected('retained native baseline frames do not reach the certified anchor')

    def validate_initial_dependencies(self, tables, owner_cut, observations):
        # The certificate is a complete row inventory, not a claim that an
        # unrelated pending intent/job/request is harmless to this shard.
        dependencies = owner_cut.get('active_dependencies')
        if not isinstance(dependencies, dict):
            raise Unavailable('initial active dependency inventory absent')
        selected = []
        for opid, observation in observations.items():
            self.op(opid)
            reference = observation.get('initial_payload_blob')
            if not reference:
                continue
            native_type = self.blobs.get(reference, {}).get('native_type')
            if native_type not in ('DfsPreparedOperationPayload', 'DfsPreparedOperationPayloadCanonicalV7'):
                raise Unavailable('initial selected body lacks its exact native payload/canonical bytes')
            payload = self.blob(reference, native_type)
            if payload.get('operation_id') != opid or any(payload.get(k) != self.scope[k] for k in ('file_system_id', 'shard_id')):
                raise Rejected('initial selected payload has a different native scope')
            expected = 'dfs-prepared-operation-v7:blake3:' + blake3(b'sinomo.dfs.prepared-operation\0v7\0' + self.blob_bytes(reference))
            if self.payloads.get(opid, {}).get(expected) != 1:
                raise Rejected('initial selected payload differs from exact body dictionary')
            if payload.get('protocol_version') != 7:
                raise Unavailable('initial selected payload is not the current protocol')
            prior = self.initial_payloads.setdefault(opid, payload)
            if prior != payload:
                raise Rejected('initial selected operation body differs across actual owners')
            if payload.get('prepared_intent'):
                selected.append(payload['prepared_intent'])
        statuses = {'intents': ({'staging', 'receipts-pending'}, {'committed', 'rejected', 'aborted', 'superseded'}),
                    'namespace_requests': ({'pending'}, {'completed'}),
                    'namespace_jobs': ({'running'}, {'completed', 'failed'})}
        for table, (pending, terminal) in statuses.items():
            rows = tables.get(table, [])
            if any(row.get('status') not in pending | terminal for row in rows):
                raise Unavailable('unsupported current native dependency status')
            active = [row for row in rows if row['status'] in pending]
            if dependencies.get(table) != active:
                raise Rejected('initial active dependency inventory omits or changes native rows')
            if table != 'intents' and active:
                raise Unavailable('active native request/job is outside finite initial-cut scope')
            if table == 'intents' and any(row not in selected for row in active):
                raise Unavailable('unrelated staged intent cannot be excluded at initial cut')

    def is_baseline_record(self, owner, raw):
        opid = raw['digest']['operation_id']
        retained = self.baseline_records.get(owner, {}).get(opid)
        if retained is None:
            return False
        if raw != retained:
            raise Rejected('immutable baseline prepared record changed after initial cut')
        return True

    def validate_baseline_rows(self, owner, tables):
        rows = tables.get('prepared_operations_v7', [])
        if any(raw not in rows for raw in self.baseline_records.get(owner, {}).values()):
            raise Unavailable('baseline record disappeared without retained native eviction evidence')
        for raw in rows:
            if raw['digest']['operation_id'] not in self.operations and not self.is_baseline_record(owner, raw):
                raise Unavailable('new unmodeled native operation after the certified cut')

    def validate_initial_receipts(self, tables, observations):
        for opid, observation in observations.items():
            if opid in self.baseline_operations:
                continue
            nodes = observation['durable_content_receipt_node_ids']
            if not nodes:
                continue
            payload = self.initial_payloads.get(opid)
            if payload is None:
                raise Unavailable('initial content receipts lack the actual selected pre-prepare body')
            receipts = [self.blob(ref, 'DfsReplicaReceiptRecord') for ref in observation['durable_content_receipt_blobs']]
            if len(receipts) != len(nodes) or {row['replica_node_id'] for row in receipts} != set(nodes):
                raise Rejected('initial content node set lacks exact receipt row coverage')
            for row in receipts:
                if row not in payload.get('content_receipts', []):
                    raise Rejected('initial content receipt differs from the immutable body')
                if row not in tables.get('receipts', []):
                    self.validate_initial_content_return(row, observation)
            intent = payload.get('prepared_intent')
            if intent:
                policy = intent['policy']
                if u64(policy['min_commit_replicas'], 'initial content W') != self.profile['requiredW'] or set(intent['desired_replica_node_ids']) != set(self.initial['profile']['content_replica_node_ids']):
                    raise Unavailable('initial body content plan differs from the certified finite profile')

    def validate_initial_content_return(self, row, observation, payload=None):
        refs = observation.get('initial_content_receipt_observation_blobs', [])
        for reference in refs:
            fact = self.blob(reference, 'DfsNativeContentReceiptObservationV1')
            self.require_certificate(fact, 'actual pre-prepare content durability')
            if fact.get('coordinator_receipt_record') != row:
                continue
            if fact.get('origin') not in ('local-verified-durable', 'authenticated-durable-response'):
                raise Unavailable('initial receipt lacks a normal persistence/response origin')
            owner = fact.get('data_owner_id')
            node = row['replica_node_id']
            if owner not in self.owners or self.owners[owner]['node_id'] != node:
                raise Rejected('initial content receipt belongs to another actual data owner')
            content = row['receipt']
            distributed = self.blob(fact.get('actual_distributed_receipt_blob'), 'DistributedReplicaReceipt')
            actual_intent = self.blob(fact.get('actual_distributed_intent_blob'), 'DistributedWriteIntent')
            if payload is None:
                payload = next((body for body in self.initial_payloads.values() if row in body.get('content_receipts', [])), None)
            if payload is not None and row not in payload.get('content_receipts', []):
                raise Rejected('receipt return is not in the exact successor immutable body')
            if payload is None or not payload.get('prepared_intent'):
                raise Unavailable('actual coordinator receipt lacks its immutable prepared intent')
            planned = payload['prepared_intent']
            expected = dict(intent_id=planned['intent_id'], file_system_id=content['file_system_id'],
                            object_id=content['object_id'], version=content['committed_version'],
                            replica_node_id=node, state='durable')
            if distributed != expected:
                raise Rejected('actual distributed Durable receipt differs from the immutable content tuple')
            for field, expected_value in dict(intent_id=planned['intent_id'], file_system_id=planned['file_system_id'],
                    object_id=planned['object_id'], target_version=planned['target_version'],
                    requesting_node_id=planned['coordinator_node_id'], owner_epoch=planned['owner_epoch'],
                    desired_replica_nodes=planned['desired_replica_node_ids'], policy=planned['policy'],
                    touch_parent_on_create=planned['touch_parent_on_create']).items():
                if actual_intent.get(field) != expected_value:
                    raise Rejected('actual coordinator intent differs from the sealed content placement')
            refs = fact.get('actual_raw_references')
            if not isinstance(refs, list):
                raise Unavailable('actual content source references are absent')
            if fact['origin'] == 'local-verified-durable':
                if (len(refs) != 2 or any(ref.get('actual_mount_label') != self.owners[owner]['component_incarnation'] for ref in refs)
                        or u64(refs[0].get('owner_ordinal'), 'actual persistence ordinal') >= u64(refs[1].get('owner_ordinal'), 'actual coordinator ordinal')):
                    raise Unavailable('actual local persistence/coordinator source ordering is absent')
            elif len(refs) != 5:
                raise Unavailable('remote durability needs receiver persistence/served return and source RPC start/return/coordinator references')
            identity = fact.get('verified_content_identity')
            if not isinstance(identity, dict) or any(identity.get(field) != value for field, value in {
                    'file_system_id': content['file_system_id'], 'object_id': content['object_id'],
                    'content_version': content['committed_version'], 'content_hash': row['content_hash']}.items()):
                raise Rejected('initial receipt differs from actual verified data generation')
            u64(identity['size_bytes'], 'actual verified content size')
            if fact['origin'] == 'authenticated-durable-response':
                if fact.get('authenticated_recipient_node_id') != node or not fact.get('native_request_id') or not fact.get('durable_response_blob'):
                    raise Unavailable('initial remote receipt lacks actual durable request/response correlation')
                reply = self.blob(fact['durable_response_blob'], 'DfsStageBlobResponse')
                rpc = self.blob(fact.get('actual_rpc_observation_blob'), 'DfsNativeAuthenticatedRpcReturnV1')
                self.require_certificate(rpc, 'actual authenticated content RPC return')
                expected_reply = dict(request_id=fact['native_request_id'], intent_id=planned['intent_id'],
                    file_system_id=content['file_system_id'], object_id=content['object_id'],
                    version=content['committed_version'], replica_node_id=node,
                    result={'durable': {'content_hash': row['content_hash']}})
                if reply != expected_reply or rpc['authenticated_target_node_id'] != node:
                    raise Rejected('remote Durable reply differs from actual prepared tuple/authenticated peer')
                source_owner = rpc['owner_id']
                if source_owner not in self.owners or self.owners[source_owner]['node_id'] != planned['coordinator_node_id']:
                    raise Rejected('remote receipt return is not owned by the actual coordinator')
                if (any(ref.get('actual_mount_label') != self.owners[owner]['component_incarnation'] for ref in refs[:2])
                        or any(ref.get('actual_mount_label') != self.owners[source_owner]['component_incarnation'] for ref in refs[2:])
                        or u64(refs[0]['owner_ordinal'], 'receiver persistence') >= u64(refs[1]['owner_ordinal'], 'receiver served return')
                        or rpc.get('actual_raw_references') != refs[2:4]
                        or u64(refs[2]['owner_ordinal'], 'source RPC start') >= u64(refs[3]['owner_ordinal'], 'source RPC return')
                        or u64(refs[3]['owner_ordinal'], 'source RPC return') >= u64(refs[4]['owner_ordinal'], 'coordinator receipt')):
                    raise Rejected('remote receipt changes receiver/source causal provenance')
                outputs = rpc['response']['outbound_messages']
                matched = [message for message in outputs if message['source_node_id'] == node
                    and message['target_node_id'] == planned['coordinator_node_id']
                    and message['message'] == {'stage-blob-response': reply}]
                command = rpc.get('command')
                routed = command.get('routed_message', {}) if isinstance(command, dict) else {}
                staged = routed.get('message', {}).get('stage-blob')
                if (len(matched) != 1 or staged is None
                        or rpc['response']['request_id'] != command['request_id']
                        or routed.get('source_node_id') != planned['coordinator_node_id']
                        or routed.get('target_node_id') != node
                        or staged.get('request_id') != fact['native_request_id']
                        or any(staged.get(field) != expected_reply[field] for field in
                               ('intent_id', 'file_system_id', 'object_id', 'version'))
                        or staged.get('content_hash') != row['content_hash']):
                    raise Rejected('actual served response lacks the exact correlated Durable message')
                served = self.blob(fact.get('actual_receiver_response_blob'), 'DfsNativeContentServedReturnV1')
                self.require_certificate(served, 'actual receiver content served return')
                command_identity = {field:staged[field] for field in
                    ('request_id','intent_id','file_system_id','object_id','version','content_hash')}
                if (served['owner_id'] != owner or served['phase'] != 'content-stage-durable-return'
                        or served['authenticated_requester_node_id'] != planned['coordinator_node_id']
                        or served['command'] != command_identity or served['response'] != reply):
                    raise Rejected('receiver persistence is not paired with its exact actual served call')
                manifest = self.blob(fact.get('actual_manifest_blob'), 'ContentManifest')
                if (manifest.get('object_id') != content['object_id'] or manifest.get('version') != content['committed_version']
                        or manifest.get('content_hash') != row['content_hash']
                        or u64(manifest['size_bytes'], 'receiver durable manifest size') != u64(identity['size_bytes'], 'verified size')):
                    raise Rejected('remote receipt differs from actual receiver persistence')
            else:
                manifest = self.blob(fact.get('actual_manifest_blob'), 'ContentManifest')
                if (manifest.get('object_id') != content['object_id'] or manifest.get('version') != content['committed_version']
                        or manifest.get('content_hash') != row['content_hash']
                        or u64(manifest['size_bytes'], 'actual durable manifest size') != u64(identity['size_bytes'], 'verified size')):
                    raise Rejected('local receipt differs from the normal durable manifest return')
            return
        raise Unavailable('initial receipt is absent from held metadata and actual persistence observations')

    def native_prefix(self, owner, prefix, certificate):
        through = self.cut(owner, prefix)
        output = empty_prefix()
        anchor = self.trace['initial_cut']['native_prefix_by_owner'][owner]
        if self.prefix_key(owner, certificate.get('native_initial_anchor')) != self.prefix_key(owner, anchor):
            raise Rejected('semantic prefix does not retain the certified initial native anchor')
        extended = anchor
        intervals = certificate.get('semantic_prefix')
        if intervals is None:
            raise Unavailable('complete native semantic prefix certificate absent')
        used = set()
        for item in intervals:
            slot = item['model_sequence']
            if type(slot) is not int or not 1 <= slot <= through or slot in used:
                raise Rejected('native semantic prefix slot duplicated/gapped')
            used.add(slot)
            entry = self.blob(item['native_entry_blob'], 'DfsMetadataJournalEntry')
            raw = self.blob_bytes(item.get('exact_frame_blob'))
            decoded = self.blob(item.get('decoder_observation_blob'), 'DfsNativeJournalDecodeObservationV1')
            self.require_certificate(decoded, 'actual journal frame decode')
            if decoded.get('exact_frame_sha256') != hashlib.sha256(raw).hexdigest() or decoded.get('decoded_entry_blob') != item['native_entry_blob']:
                raise Rejected('semantic journal entry does not bind the actual retained frame')
            # These are complete ordinary append slots after a certified cut,
            # not subtraction of a higher native index or a guessed genesis.
            if slot != len(used):
                raise Rejected('semantic prefix entries are out of native frame order')
            extended = extend_native_prefix(extended, u64(entry['sequence'], 'semantic native sequence'), raw)
            opid = item['operation_id']; op = self.op(opid)
            epoch = u64(item['owner_epoch'], 'entry epoch')
            if epoch not in self.epoch_aliases:
                raise Unavailable('journal epoch lacks exact full-config alias')
            mutation = entry['mutation']; kind = item['semantic_kind']
            if entry['shard_id'] != self.scope['shard_id'] or mutation.get('file_system_id') != self.scope['file_system_id'] or mutation.get('operation_id') != opid:
                raise Rejected('native prefix entry fs/shard/operation scope differs')
            if kind == 'prepare':
                if mutation.get('type') != 'prepare-operation-v7' or mutation['record']['digest']['operation_id'] != opid:
                    raise Rejected('prepare semantic slot differs from native opcode')
                prepared = mutation['record']
                self.record(prepared, item.get('canonical_payload_blob'))
                if u64(prepared['digest']['prepare_sequence'],'native prefix prepare slot') != u64(entry['sequence'],'native entry sequence') or u64(prepared['digest']['owner_epoch'],'native prefix epoch') != epoch or prepared['status'] != 'prepared':
                    raise Rejected('native prepare prefix slot/epoch/status differs')
                digest = prepared['digest']['payload_digest']; body = 1; tag = 1
            elif kind == 'commit':
                if mutation.get('type') != 'commit-prepared-operation-v7' or mutation['digest']['operation_id'] != opid:
                    raise Rejected('commit semantic slot differs from native opcode')
                committed = mutation['digest']
                if committed['file_system_id'] != self.scope['file_system_id'] or committed['shard_id'] != self.scope['shard_id'] or committed['protocol_version'] != 7 or u64(committed['owner_epoch'],'commit epoch') != epoch or u64(committed['prepare_sequence'],'commit dependency') >= u64(entry['sequence'],'commit slot'):
                    raise Rejected('native commit prefix scope/epoch/dependency differs')
                digest = committed['payload_digest']; body = 0; tag = 2
            else:
                raise Unavailable('unmodeled journal effects cannot be skipped')
            if self.payloads.get(opid, {}).get(digest) != 1:
                raise Rejected('prefix digest differs from exact immutable body dictionary')
            output[slot] = dict(kind=tag, op=op, digest=op*10+1, body=body, epoch=self.epoch_aliases[epoch])
        if used != set(range(1, through+1)):
            raise Rejected('noncontiguous finite semantic prefix')
        if certificate.get('native_prefix') != prefix:
            raise Rejected('semantic prefix certificate does not name actual native cut')
        if self.prefix_key(owner, extended) != self.prefix_key(owner, prefix):
            raise Rejected('semantic exact frames do not reach the actual native prefix')
        return through, finite_map(output)

    def held(self, snapshot):
        if not snapshot.get('projection_continuous'):
            raise Unavailable('discontinuous native observation')
        reference = snapshot.get('held_tables_blob')
        tables = self.blob(reference, 'DfsMetadataTableSnapshotTables')
        facts = self.blob(snapshot.get('native_operational_facts_blob'), 'DfsShardNativeOperationalFactsV1')
        self.require_certificate(facts, 'held operational facts')
        if facts['snapshot_owner_id'] != snapshot['owner_id'] or facts['snapshot_native_prefix'] != snapshot['local_certified_prefix']:
            raise Rejected('held facts borrowed from another native owner/cut')
        if not facts.get('visible_footprint_complete'):
            raise Unavailable('partial native visible footprint')
        return tables, facts

    def validate_peer_durable_cut(self, node_id, peer, snapshot, materialized):
        durable = peer['prefix']
        if self.prefix_key(peer['owner_id'], durable) == self.prefix_key(peer['owner_id'], materialized):
            return
        if snapshot.get('durable_journal_prefix') != durable:
            raise Unavailable('lagging replica lacks its separately observed durable journal cut')
        if self.cut(peer['owner_id'], materialized) > self.cut(peer['owner_id'], durable):
            raise Rejected('replica materialized prefix is beyond its actual durable prefix')
        fact = self.blob(peer.get('durable_ingest_observation_blob'), 'DfsNativeReplicaDurableIngestObservationV1')
        self.require_certificate(fact, 'actual durable replica ingest')
        if (fact.get('owner_id') != peer['owner_id'] or fact.get('recipient_node_id') != node_id
                or fact.get('after_held_snapshot_id') != peer['held_snapshot_id']
                or fact.get('durable_journal_prefix') != durable
                or fact.get('materialized_prefix') != materialized):
            raise Rejected('replica durable ingest borrowed another owner/cut/result')
        permit = fact['actual_ingest_permit_tuple']
        _, source = self.config(fact['actual_source_config'])
        allowed = set(source['replica_node_ids'] + [source['primary_node_id']])
        pending_id = permit.get('pending_primary_handoff_id')
        if pending_id is not None:
            pending = self.blob(fact.get('actual_pending_handoff_blob'), 'DfsMetadataShardMembershipHandoff')
            retiring, desired = pending['retiring_shard'], pending['desired_shard']
            if (pending.get('handoff_id') != pending_id or source not in (retiring, desired)
                    or any(cfg['file_system_id'] != self.scope['file_system_id'] or cfg['shard_id'] != self.scope['shard_id'] for cfg in (retiring, desired))):
                raise Rejected('replica pending ingest scope differs from actual handoff')
            allowed |= set(desired['replica_node_ids'] + [desired['primary_node_id']])
            allowed |= set(retiring['replica_node_ids'] + [retiring['primary_node_id']])
        if (permit['file_system_id'] != self.scope['file_system_id']
                or permit['shard_id'] != self.scope['shard_id']
                or permit['source_node_id'] != source['primary_node_id']
                or u64(permit['source_epoch'], 'actual replica source epoch') != u64(source['epoch'], 'source config epoch')
                or node_id not in allowed):
            raise Rejected('durable journal result differs from the normal scoped ingest permit')
        if not fact.get('materialization_pending'):
            raise Rejected('durable-only replica cut was mislabeled fully materialized')
        # Exact frame decoding/chain evidence is checked by native_prefix below.
        # A durable receiver report alone does not manufacture an ACK at source.
        refs = fact.get('validated_frame_decoder_observation_blobs')
        semantic = self.blob(peer['prefix_certificate_blob'], 'DfsNativeSemanticPrefixCertificateV1')
        expected = [item['decoder_observation_blob'] for item in semantic['semantic_prefix']]
        if not refs:
            raise Unavailable('durable replica result lacks actual validated frame decoder evidence')
        if refs != expected:
            raise Rejected('durable replica decoder evidence differs from exact semantic prefix frames')
        materialized_certificate = self.blob(peer.get('materialized_prefix_certificate_blob'), 'DfsNativeSemanticPrefixCertificateV1')
        self.require_certificate(materialized_certificate, 'actual replica materialized prefix')
        self.native_prefix(peer['owner_id'], materialized, materialized_certificate)

    def replica_freeze_read(self, reference):
        proof=self.blob(reference,'DfsNativeReplicaFreezeReadObservationV1')
        self.require_certificate(proof,'actual replica freeze READ')
        observed=proof['observed_freeze'];permit=proof['actual_read_fact']['permit']
        if proof['actual_read_fact']['observed_freeze']!=observed:
            raise Rejected('replica freeze classification changes its actual read result')
        if observed['observation']=='absent':kind=1
        elif observed['observation']=='present':
            marker=observed['value'];activated=marker.get('activated_epoch')
            admitted=(activated is not None and u64(permit['source_epoch'],'replica source epoch')>=u64(activated,'activated freeze epoch')
                or activated is None and permit.get('pending_primary_handoff_id')==marker['handoff_id']
                    and u64(permit['source_epoch'],'replica source epoch')==u64(marker['retiring_epoch'],'retiring freeze epoch'))
            kind=2 if admitted else 3
        else:raise Unavailable('Unknown cannot become a normal read observation')
        return proof,kind

    def project_snapshot(self, snapshot_id):
        snapshot = self.trace['snapshots'][snapshot_id]
        tables, facts = self.held(snapshot)
        self.config(snapshot['committed_config'])  # validate complete consensus tuple
        revision, config = self.config(snapshot['active_config'])
        primary = self.node(config['primary_node_id']); epoch = self.epoch_aliases[u64(config['epoch'], 'epoch')]
        peers = facts['peer_certified_cuts']
        if set(peers) != set(self.nodes):
            raise Unavailable('complete per-node certified cuts absent')
        heads, logs, bound = {}, {}, {}
        replica_read,replica_freeze,replica_ingest_read={},{},{}
        for node_id, peer in peers.items():
            owner_id = peer['owner_id']
            if self.owners[owner_id]['node_id'] != node_id:
                raise Rejected('peer cut uses a different native node owner')
            peer_snapshot_id = peer.get('held_snapshot_id')
            if peer_snapshot_id not in self.trace['snapshots']:
                raise Unavailable('peer cut needs a real held snapshot, not a certificate boolean')
            peer_snapshot = self.trace['snapshots'][peer_snapshot_id]
            materialized = peer.get('materialized_prefix', peer['prefix'])
            if peer_snapshot['owner_id'] != owner_id or peer_snapshot['scope'] != self.scope or peer_snapshot['local_certified_prefix'] != materialized:
                raise Rejected('peer cut differs from referenced native held snapshot')
            if u64(peer_snapshot['materialized_through_sequence'], 'peer materialized sequence') != u64(materialized['journal_sequence'], 'peer materialized prefix'):
                raise Rejected('peer held tables do not match their actual materialized prefix')
            peer_tables,peer_facts=self.held(peer_snapshot)
            gate=peer_facts['actual_operational_gate'];read_ref=gate.get('actual_replica_freeze_read_observation_blob')
            alias=self.node(node_id)
            replica_read[alias]=0;replica_freeze[alias]=0;replica_ingest_read[alias]=0
            if read_ref:
                proof,kind=self.replica_freeze_read(read_ref)
                asof=peer_facts['actual_cut_as_of_source'];read=proof['read_source']
                if (proof['owner_id']!=owner_id or read['owner_id']!=owner_id
                        or u64(read['owner_ordinal'],'READ ordinal')>u64(asof['owner_ordinal'],'READ as-of')):
                    raise Rejected('replica freeze observation borrows a future or foreign READ')
                replica_read[alias]=u64(read['owner_ordinal'],'READ ordinal');replica_freeze[alias]=kind
                accepted=gate.get('actual_replica_ingest_read_source')
                if accepted is not None:
                    if (accepted['owner_id']!=owner_id
                            or u64(accepted['owner_ordinal'],'accepted READ ordinal')>replica_read[alias]):
                        raise Rejected('replica accepted ingest substitutes a foreign or future normal READ')
                    replica_ingest_read[alias]=u64(accepted['owner_ordinal'],'accepted READ ordinal')
            self.validate_baseline_rows(owner_id, peer_tables)
            self.validate_peer_durable_cut(node_id, peer, peer_snapshot, materialized)
            cert = self.blob(peer['prefix_certificate_blob'], 'DfsNativeSemanticPrefixCertificateV1')
            self.require_certificate(cert, 'peer prefix')
            heads[self.node(node_id)], logs[self.node(node_id)] = self.native_prefix(owner_id, peer['prefix'], cert)
            binding = peer['writer_binding']
            if binding['owner_id'] != owner_id:
                raise Unavailable('process-local actual writer binding missing')
            if not binding['validated_under_real_writer']:
                custody=self.blob(binding.get('native_resident_custody_blob'),'DfsNativeResidentReplicaCustodyV1')
                self.require_certificate(custody,'actual resident replica lease/admission')
                actual=custody['actual_custody'];owner=self.owners[owner_id]
                if (custody['owner_id']!=owner_id or actual['actual_mount_label']!=owner['component_incarnation']
                        or actual['local_node_id']!=node_id or actual['backend_root']!=owner['canonical_root']
                        or any(actual[k]!=self.scope[k] for k in ('file_system_id','shard_id'))
                        or actual['admission_kind'] not in ('exact-shard','global-exclusive')
                        or actual['admission_scheduler_token']!=owner['lane_incarnation']
                        or not actual.get('resident_handle_token') or not actual.get('data_owner_token')):
                    raise Rejected('replica observed binding borrows another actual resident/admitted scope')
            native_epoch = binding['bound_epoch']
            actual_guard_ref=binding.get('actual_publication_guard_observation_blob')
            if native_epoch is not None:
                instance = binding['actual_backend_instance_authority_epoch']
                if instance != native_epoch:
                    raise Rejected('persistent binding differs from actual backend instance epoch')
                if actual_guard_ref:
                    actual_guard = self.blob(actual_guard_ref, 'DfsNativePublicationGuardObservationV1')
                    self.require_certificate(actual_guard, 'observed actual writer binding')
                    _, expected = self.config(actual_guard['expected_config'])
                    if (actual_guard.get('actual_physical_scope', {}).get('canonical_root') != self.owners[owner_id]['canonical_root']
                            or native_epoch != expected['epoch'] or expected['primary_node_id'] != node_id):
                        raise Rejected('observed binding borrows another guard/configuration/root')
                # A held instance epoch is persistent DATA, including after
                # release. It cannot replace the live author guard at commit.
            bound[self.node(node_id)] = 0 if native_epoch is None else self.epoch_aliases.get(u64(native_epoch, 'writer epoch'), -1)
            if bound[self.node(node_id)] < 0:
                raise Unavailable('writer epoch outside config dictionary')
        prefix_cert = self.blob(facts['local_prefix_certificate_blob'], 'DfsNativeSemanticPrefixCertificateV1')
        self.require_certificate(prefix_cert, 'local prefix')
        self.native_prefix(snapshot['owner_id'], snapshot['local_certified_prefix'], prefix_cert)
        # A receiver's local cut is a per-node observation. Global authority and
        # journal come from the current actual primary's certified held view.
        primary_peer = peers[config['primary_node_id']]
        primary_snapshot = select_primary_held_snapshot(config, peers, self.trace['snapshots'], self.owners, self.scope)
        primary_tables,primary_facts=self.held(primary_snapshot)
        _, primary_config = self.config(primary_snapshot['active_config'])
        if primary_config != config:
            raise Rejected('logical-shard cut combines different active configurations')
        tables = primary_tables
        # Global shard control/guard state belongs to this exact primary cut;
        # local receiver Unknown/read observations were retained separately.
        facts = primary_facts
        head, journal = heads[primary], logs[primary]
        if (primary_snapshot['local_certified_prefix'] != primary_peer['prefix']
                or u64(primary_snapshot['materialized_through_sequence'], 'primary materialized sequence') != u64(primary_peer['prefix']['journal_sequence'], 'primary durable sequence')):
            raise Unavailable('primary visibility needs an actual fully materialized logical cut')
        prepares = {o: empty_prepare() for o in OPERATIONS}; commits = {o: empty_commit() for o in OPERATIONS}
        receipts = {o: finite_set([]) for o in OPERATIONS}; acks = {o: finite_set([]) for o in OPERATIONS}
        definitions = facts['operation_observations']
        if head == 0:
            self.validate_initial_receipts(tables, definitions)
        for opid, native in definitions.items():
            if opid in self.baseline_operations:
                continue
            op = self.op(opid)
            receipts[op] = self.node_set(native['durable_content_receipt_node_ids'])
            acks[op] = self.node_set(native['authenticated_prepare_ack_node_ids'])
            for ref in native['durable_content_receipt_blobs']:
                receipt = self.blob(ref, 'DfsReplicaReceiptRecord')
                if receipt['replica_node_id'] not in native['durable_content_receipt_node_ids'] or receipt['receipt']['file_system_id'] != self.scope['file_system_id'] or receipt['receipt']['object_id'] != native['target_object_id'] or receipt['receipt']['committed_version'] != native['target_version'] or receipt['content_hash'] != native['content_hash']:
                    raise Rejected('content receipt does not bind exact native tuple')
            if len(native['durable_content_receipt_blobs']) != len(native['durable_content_receipt_node_ids']):
                raise Rejected('content receipt set lacks complete typed evidence')
        native_records = tables.get('prepared_operations_v7', [])
        for raw in native_records:
            if self.is_baseline_record(primary_snapshot['owner_id'], raw):
                continue
            opid = raw['digest']['operation_id']
            if opid not in definitions:
                raise Unavailable('stored operation lacks its native witness observation')
            op, payload, digest, terminal = self.record(raw, definitions[opid].get('canonical_payload_blob'))
            opid = digest['operation_id']; observation = definitions[opid]
            witness = observation['durable_prepare_observation']
            old_revision, old_config = self.config(witness['config'])
            old_epoch = self.epoch_aliases[u64(payload['owner_epoch'], 'payload epoch')]
            prepare_seq, prepare_prefix = self.native_prefix(witness['owner_id'], witness['prefix'], self.blob(witness['prefix_certificate_blob'], 'DfsNativeSemanticPrefixCertificateV1'))
            if u64(digest['prepare_sequence'], 'prepare slot') != u64(witness['prefix']['journal_sequence'], 'prepare prefix'):
                raise Rejected('prepare sequence differs from actual durable cut')
            chain = observation['visibility_chain_certificate']
            if not chain.get('complete_atomic_footprint') or chain['operation_id'] != opid or chain['native_payload_digest'] != digest['payload_digest']:
                raise Unavailable('operation lacks a complete native visibility-chain certificate')
            previous_commits = witness.get('native_visible_commit_operation_ids')
            if previous_commits is None or len(previous_commits) != len(set(previous_commits)):
                raise Unavailable('prepare held-cut visible operation identities absent')
            previous_commits = [previous for previous in previous_commits if previous not in self.baseline_operations]
            for previous in previous_commits:
                self.op(previous)
            base, target = len(previous_commits), len(previous_commits)+1
            if not 0 <= base < target <= 3:
                raise Unavailable('native effects are not one bounded sequential visibility lane')
            prepare_receipts = self.node_set(witness['durable_content_receipt_node_ids'])
            prepares[op] = dict(present=True, op=op, digest=op*10+1, body=1, epoch=old_epoch, revision=old_revision, primary=self.node(old_config['primary_node_id']), seq=prepare_seq, base=base, target=target, receipts=prepare_receipts, prefix=prepare_prefix)
            if observation.get('native_permit_observation_blob') is not None:
                raise Rejected('retired per-operation KV permit is incompatible with current native model')
            if terminal:
                admission = self.guard(observation['committed_guard_observation_blob'], digest)
                if not chain.get('all_effect_rows_materialized'):
                    raise Rejected('native commit label with partially visible transaction rows')
                self.validate_atomic_rows(tables, payload, observation['visibility_chain_certificate'], terminal, primary_snapshot)
                commits[op] = dict(present=True, op=op, digest=op*10+1, body=1, epoch=old_epoch, revision=old_revision, primary=prepares[op]['primary'], seq=prepare_seq, base=base, target=target, members=admission['members'], admittedEpoch=admission['epoch'], admittedRevision=admission['revision'], admittedPrimary=admission['primary'], admittedFrozen=admission['frozen'], admittedPending=admission['pending'], receipts=prepare_receipts, acks=admission['acks'], path=admission['path'])
        visible_commits = [c for c in commits.values() if c['present']]
        visible = max([c['target'] for c in visible_commits] or [0])
        gate = primary_facts['actual_operational_gate']
        if gate['writer_owner_id'] != primary_snapshot['owner_id'] or not gate['read_under_actual_owner']:
            raise Unavailable('native freeze/admission state has no owned operational read')
        handoff = facts.get('handoff')
        def cut_field(name):
            return self.cut(handoff['retiring_owner_id'], handoff[name]) if handoff else 0
        def prefix_field(name):
            if not handoff: return finite_map(empty_prefix())
            certificate = self.blob(handoff[name+'_certificate_blob'], 'DfsNativeSemanticPrefixCertificateV1')
            return self.native_prefix(handoff['retiring_owner_id'], handoff[name], certificate)[1]
        pending = primary_snapshot['pending_handoff_blob'] is not None
        if pending != bool(handoff and handoff['pending_record_present']):
            raise Rejected('pending native handoff facts differ from authoritative record')
        desired_members = self.profile['initialMembers']; desired_primary = 1
        if handoff:
            _, desired = self.config(handoff['desired_config'])
            _, retiring = self.config(handoff['retiring_config'])
            desired_primary = self.node(desired['primary_node_id'])
            desired_members = self.node_set(sorted(set(desired['replica_node_ids']+[desired['primary_node_id']])))
            old_members = set(retiring['replica_node_ids']+[retiring['primary_node_id']]); new_members = set(desired['replica_node_ids']+[desired['primary_node_id']])
            if retiring['primary_node_id'] != desired['primary_node_id'] and old_members != new_members:
                raise Rejected('mixed primary+membership handoff is outside actual gate')
        freeze_observation = gate.get('write_freeze_observation')
        if freeze_observation not in ('observed-absent','observed-present','observed-present-inactive'):
            raise Unavailable('freeze None/NotObserved is not a certified absence')
        if gate['durable_write_frozen'] != (freeze_observation == 'observed-present'):
            raise Rejected('current writer freeze differs from its actual operational gate')
        logical_frozen = freeze_observation != 'observed-absent'
        if logical_frozen and not gate.get('native_write_freeze_blob'):
            raise Unavailable('native observed durable freeze marker absent')
        if freeze_observation == 'observed-present-inactive':
            historical=gate.get('initial_completed_handoff_blob')
            if historical:
                certificate=self.blob(historical,'DfsNativeInitialCompletedHandoffV1')
                self.require_certificate(certificate,'actual completed initial handoff')
                completed=certificate['actual_completed_handoff']
                if completed!=self.initial.get('initial_completed_handoff') or handoff is not None:
                    raise Rejected('historical initial marker cannot replace a finite active handoff')
                # The raw marker remains PRESENT/INACTIVE. The finite interval
                # begins after this independently certified terminal history.
                logical_frozen=False
            marker = self.blob(gate['native_write_freeze_blob'], 'DfsMetadataShardWriteFreeze')
            rebound = self.blob(gate.get('actual_successor_rebind_observation_blob'), 'DfsNativeSuccessorRebindObservationV1')
            self.require_certificate(rebound, 'actual successor rebind')
            if not historical and (not handoff or not handoff['activation_committed']):
                raise Rejected('inactive marker has no actual completed native handoff')
            actual_epoch = u64(config['epoch'], 'active native epoch')
            proof = rebound['proof']; original = rebound['original_freeze']
            if (rebound['owner_id'] != primary_snapshot['owner_id'] or rebound['active'] != config
                    or proof['desired_shard'] != config or proof['handoff_id'] != marker['handoff_id']
                    or original['handoff_id'] != marker['handoff_id']
                    or original['retiring_epoch'] != marker['retiring_epoch']
                    or u64(marker['activated_epoch'], 'activated marker epoch') != actual_epoch
                    or u64(rebound['after_instance_epoch'], 'observed rebound instance epoch') != actual_epoch):
                raise Rejected('inactive marker borrows another actual owner/proof/instance generation')
        environment = facts['environment_binding']
        if environment.get('origin') not in ('native-fault-control','existential-environment-constrained-by-native-responses'):
            raise Unavailable('model online set has no explicit environment abstraction provenance')
        if not set(environment.get('authenticated_response_node_ids',[])).issubset(environment['online_node_ids']):
            raise Rejected('environment binding contradicts an authenticated response')
        if environment.get('absence_is_offline',False):
            raise Rejected('missing responses cannot certify an offline node')
        if any(key in facts for key in ('operation_floor_prefix', 'raft_watermark_prefix')):
            raise Rejected('retired ordinary KV clocks are incompatible with current native context')
        active_guard = facts.get('active_publication_guard_blob')
        admission = self.guard(active_guard, None) if active_guard else None
        retained = self.guard(facts['last_publication_guard_blob'], None) if facts.get('last_publication_guard_blob') else None
        if admission:
            if (bound[primary] != admission['epoch']
                    or primary_peer['writer_binding'].get('actual_publication_guard_observation_blob') != active_guard):
                raise Rejected('active writer binding differs from its exact admitted guard lease')
            # This cut follows admission: the peer binding already names the
            # same live lease. Its separately checked prior storage view remains
            # provenance for the mint, not the current capability binding.
            bound[primary] = admission['epoch']
        state = dict(primary=primary, epoch=epoch, revision=revision, profile=self.profile, members=self.node_set(sorted(set(config['replica_node_ids']+[config['primary_node_id']]))), online=self.node_set(facts['environment_binding']['online_node_ids']), bound=finite_map(bound), head=head, journal=journal, logs=finite_map(logs), heads=finite_map(heads), replicaRead=finite_map(replica_read), replicaFreeze=finite_map(replica_freeze), replicaIngestRead=finite_map(replica_ingest_read), prepares=finite_map(prepares), receipts=finite_map(receipts), acks=finite_map(acks), commits=finite_map(commits), visible=visible, visibleBodies=finite_map({o: 1 if commits[o]['present'] else 0 for o in OPERATIONS}), pending=pending, observedPending=gate['observed_pending_handoff'], frozen=logical_frozen, desiredPrimary=desired_primary, desiredMembers=desired_members, capturedHead=cut_field('pending_decision_local_prefix'), pendingJournalHead=cut_field('pending_decision_local_prefix'), capturedPrefix=prefix_field('pending_decision_local_prefix'), frozenHead=cut_field('frozen_prefix'), frozenPrefix=prefix_field('frozen_prefix'), handoffAcks=self.node_set(handoff['authenticated_exact_prefix_ack_nodes']) if handoff else finite_set([]), promotionMarker=bool(handoff and handoff['durable_promotion_marker_validated']), activated=bool(handoff and handoff['activation_committed']), lostReply=finite_set(self.observed_flags.get('lostReply',[])), replayed=finite_set(self.observed_flags.get('replayed',[])), changedRetryRejected=self.observed_flags.get('changedRetryRejected',False), minorityRejected=self.observed_flags.get('minorityRejected',False), staleEpochRejected=self.observed_flags.get('staleEpochRejected',False), partialActivationRejected=self.observed_flags.get('partialActivationRejected',False), partialRejected=self.observed_flags.get('partialRejected',False), frozenWriterRejected=self.observed_flags.get('frozenWriterRejected',False), guardOp=admission['op'] if admission else 0, guardPath=retained['path'] if retained else 0, guardAcks=retained['acks'] if retained else finite_set([]), guardPending=retained['pending'] if retained else False, wrongDigestRejected=self.observed_flags.get('wrongDigestRejected',False), divergentPrefixRejected=self.observed_flags.get('divergentPrefixRejected',False), lateWriteCaptured=bool(handoff and handoff['late_native_writes_captured']), activationReplyLost=self.observed_flags.get('activationReplyLost', False), last=self.last)
        return state

    def validate_effect_materialization(self, witness, payload, committed_entry, canonical_reference):
        if not isinstance(witness,dict) or set(witness)!={'event_id','snapshot_id','table','effect'} or witness['table'] not in ('intents','receipts'):
            raise Unavailable('complete actual intent materialization source unavailable')
        events=[event for event in self.trace['events'] if event['event_id']==witness['event_id']]
        if len(events)!=1:
            raise Unavailable('intent materialization event is missing or ambiguous')
        event=events[0]; action=event['action']; snapshot=self.trace['snapshots'].get(witness['snapshot_id'])
        if snapshot is None:
            raise Unavailable('actual intent materialization snapshot unavailable')
        prepared=committed_entry is None
        if prepared and witness['table']!='receipts':
            raise Rejected('prepared DATA cannot supersede a terminal intent or visible effect')
        if (action['kind']!=('prepare-payload' if prepared else 'commit-digest')
                or event['after_snapshot']!=witness['snapshot_id']
                or snapshot['owner_id']!=event['owner_id'] or snapshot['scope']!=self.scope
                or action['operation']['operation_id']!=payload['operation_id']
                or not prepared and self.blob(action['commit_record_blob'],'DfsPreparedOperationCommitReference')!=committed_entry):
            raise Rejected('effect materialization crosses its actual owner/prepare-or-commit/scope')
        tables,facts=self.held(snapshot)
        records=[row for row in tables.get('prepared_operations_v7',[]) if row['digest']['operation_id']==payload['operation_id']]
        if len(records)!=1:
            raise Unavailable('effect materialization lacks its full actual sealed operation')
        record=records[0]; _,body,digest,terminal=self.record(record,canonical_reference)
        sequence=digest['prepare_sequence'] if prepared else committed_entry['sequence']
        if (body!=payload or terminal!=committed_entry or record['status']!=('prepared' if prepared else 'committed')
                or action['operation']['native_digest']!=digest['payload_digest']
                or action['operation']['payload_blob']!=canonical_reference
                or u64(snapshot['materialized_through_sequence'],'effect materialized sequence')!=u64(sequence,'effect durable sequence')
                or u64(snapshot['local_certified_prefix']['journal_sequence'],'effect certified head')!=u64(sequence,'effect durable sequence')):
            raise Rejected('effect materialization changed the exact immutable body/durable cut')
        if prepared:
            source=facts.get('actual_memory_cut_source')
            if (self.blob(action['prepare_record_blob'],'DfsPreparedOperationRecord')!=record
                    or source is None or source['owner_id']!=event['owner_id'] or source['side']!='after'
                    or dict(actual_mount_label=source['owner_id'],owner_ordinal=source['owner_ordinal']) not in event['raw_references']
                    or witness['effect'] not in body['content_receipts']):
                raise Rejected('prepared receipt DATA lacks its SAME actual sealed prepare materialization')
        if tables.get(witness['table'],[]).count(witness['effect'])!=1:
            raise Rejected('effect is absent/duplicated/changed at its actual materialization cut')
        return witness['effect']

    def validate_receipt_return_frontier(self, reference, snapshot, materialization_source=None):
        # Ordinals are owner-local. The already verified source-closed peer cuts
        # bound every SAME actual persistence, served return and coordinator fact.
        fact=self.blob(reference,'DfsNativeContentReceiptObservationV1')
        _,facts=self.held(snapshot)
        peers=facts.get('peer_certified_cuts')
        if not isinstance(peers,dict):
            raise Unavailable('receipt DATA needs the exact current source-closed peer frontier')
        horizon={}
        for node,peer in peers.items():
            owner=peer['owner_id'];held=self.trace['snapshots'].get(peer['held_snapshot_id'])
            if owner not in self.owners or self.owners[owner]['node_id']!=node or held is None or held['owner_id']!=owner:
                raise Rejected('receipt DATA frontier borrows another actual peer owner')
            _,observed=self.held(held);asof=observed.get('actual_cut_as_of_source')
            if not asof or asof['owner_id']!=owner:
                raise Unavailable('receipt DATA peer lacks its actual owner-local as-of source')
            horizon[self.owners[owner]['component_incarnation']]=u64(asof['owner_ordinal'],'receipt peer cutoff')
        refs=fact.get('actual_raw_references')
        if not isinstance(refs,list) or not refs:
            raise Unavailable('receipt return lacks its actual raw source references')
        if materialization_source is not None:
            refs=refs+[dict(actual_mount_label=materialization_source['owner_id'],owner_ordinal=materialization_source['owner_ordinal'])]
        if any(
                ref['actual_mount_label'] not in horizon
                or u64(ref['owner_ordinal'],'receipt return ordinal')>horizon[ref['actual_mount_label']]
                for ref in refs):
            raise Rejected('receipt return is after or outside its exact source-closed frontier')

    def validate_atomic_rows(self, tables, payload, certificate, committed_entry=None, snapshot=None):
        # Exact current footprint, including absence, is part of the certificate.
        # Earlier committed effects may be superseded only by a separately
        # captured successor transaction; simple existence of a label is invalid.
        rows = certificate.get('actual_footprint_rows')
        if not isinstance(rows, dict):
            raise Unavailable('full current atomic footprint evidence unavailable')
        if (certificate.get('declared_transaction', payload['transaction']) != payload['transaction']
                or certificate.get('declared_metadata_effects', payload.get('metadata_effects', [])) != payload.get('metadata_effects', [])):
            raise Rejected('atomic certificate changes the complete immutable declared effects')
        superseded = []
        superseded_metadata = []
        for witness in certificate.get('superseded_effects', []):
            fields={'table','prior_effect','replacement_effect','successor_record_blob','successor_canonical_payload_blob'}
            if witness.get('table') in ('intents','receipts'): fields.add('successor_materialization')
            if witness.get('table')=='receipts': fields.add('successor_receipt_observation_blob')
            if set(witness)!=fields or witness['table'] not in ('objects','intents','receipts'):
                raise Unavailable('supersession requires an exact supported current effect chain')
            prior, replacement = witness['prior_effect'], witness['replacement_effect']
            successor = self.blob(witness['successor_record_blob'], 'DfsPreparedOperationRecord')
            if successor not in tables.get('prepared_operations_v7', []):
                raise Rejected('supersession borrows a record outside actual held native tables')
            self.verified_record(successor, witness['successor_canonical_payload_blob'])
            body = successor['payload']; commit = successor.get('committed_entry')
            prepared_receipt=(witness['table']=='receipts' and successor['status']=='prepared' and commit is None)
            later=u64(successor['digest']['prepare_sequence'],'successor prepare slot') if prepared_receipt else (u64(commit['sequence'],'successor commit slot') if commit else 0)
            if (not prepared_receipt and (successor['status']!='committed' or not commit)
                    or committed_entry is None or later<=u64(committed_entry['sequence'],'earlier commit slot')
                    or body['operation_id'] not in self.operations
                    or body['operation_id'] == payload['operation_id']
                    or body['file_system_id'] != payload['file_system_id']
                    or body['shard_id'] != payload['shard_id']):
                raise Rejected('supersession lacks a later exact committed operation in this scope')
            if witness['table'] in ('intents','receipts'):
                table=witness['table']; kind,field=('put-intent','intent') if table=='intents' else ('put-receipt','receipt')
                if (not any(effect['type']==kind and effect[field]==prior for effect in payload['metadata_effects'])
                        or replacement not in tables.get(table,[]) or replacement not in rows.get(table,[])):
                    raise Rejected('metadata supersession omits its original or actual sealed effect')
                if table=='intents':
                    if exact_intent_terminal(prior,body)!=replacement:
                        raise Rejected('intent supersession changes its complete sealed terminal')
                else:
                    if not exact_receipt_replacement(prior,replacement) or replacement not in body['content_receipts']:
                        raise Rejected('receipt supersession changes its tuple or omits its actual sealed row')
                    self.validate_initial_content_return(replacement,dict(
                        initial_content_receipt_observation_blobs=[witness['successor_receipt_observation_blob']]),body)
                    if prepared_receipt:
                        if snapshot is None:
                            raise Unavailable('prepared receipt DATA needs its actual current frontier')
                        materialized=self.trace['snapshots'][witness['successor_materialization']['snapshot_id']]
                        _,materialized_facts=self.held(materialized)
                        self.validate_receipt_return_frontier(witness['successor_receipt_observation_blob'],snapshot,
                            materialized_facts.get('actual_memory_cut_source'))
                        self.validate_receipt_return_frontier(witness['successor_receipt_observation_blob'],materialized)
                self.validate_effect_materialization(witness['successor_materialization'],body,commit,
                    witness['successor_canonical_payload_blob'])
                if (witness['successor_materialization']['effect']!=replacement
                        or witness['successor_materialization']['table']!=table
                        or any(item['type']==kind and item[field]==prior for item in superseded_metadata)):
                    raise Rejected('metadata supersession changes or duplicates its terminal materialization')
                superseded_metadata.append(dict(type=kind,**{field:prior}))
                continue
            if (prior not in payload['transaction']['objects']
                    or replacement not in body['transaction']['objects']
                    or prior['file_system_id'] != replacement['file_system_id']
                    or prior['object_id'] != replacement['object_id']
                    or replacement not in tables.get('objects', [])):
                raise Rejected('supersession changes or omits its exact native object effect')
            readset = [row for row in body['expected_object_versions'] if row['object_id'] == prior['object_id']]
            if len(readset) != 1 or readset[0]['expected_version'] != prior['version']:
                raise Rejected('successor did not capture the exact predecessor generation')
            if any(effect == prior for effect in superseded):
                raise Rejected('duplicate supersession witness for one native effect')
            if replacement not in rows.get('objects', []):
                raise Rejected('current atomic footprint omits the actual replacement object')
            superseded.append(prior)
        for table, expected in rows.items():
            if table not in (*VISIBLE_TABLES,'intents','receipts') or not isinstance(expected, list):
                raise Rejected('unsupported/partial atomic footprint table')
            actual = tables.get(table, [])
            for effect in expected:
                if effect not in actual:
                    raise Rejected(f'partially visible native transaction: {table}')
        for table, effects in (('objects',payload['transaction']['objects']), ('versions',payload['transaction'].get('versions',[]))):
            if any(effect not in rows.get(table,[]) and not (table == 'objects' and effect in superseded) for effect in effects):
                raise Rejected('native atomic certificate omits a complete payload effect')
        self.validate_declared_table_effects(tables, payload, certificate, committed_entry, superseded_metadata)
        for effect in payload['transaction']['objects']:
            if effect in superseded:
                effect = next(w['replacement_effect'] for w in certificate['superseded_effects'] if w['prior_effect'] == effect)
            self.validate_object_indexes(tables, effect)
        for absent in certificate.get('absent_native_rows',[]):
            table = absent['table']; key = absent['native_identity_fields']
            if table not in VISIBLE_TABLES or not key:
                raise Rejected('invalid native absence footprint')
            if any(all(row.get(field) == value for field,value in key.items()) for row in tables.get(table,[])):
                raise Rejected('native atomic footprint contains a row certified absent')
        terminal = payload.get('terminal_intent')
        if terminal:
            matches = [row for row in tables.get('intents',[]) if row['intent_id']==terminal['intent_id'] and row['object_id']==terminal['object_id']]
            receipts = [row for row in tables.get('intent_receipts',[]) if row['intent_id']==terminal['intent_id'] and row['object_id']==terminal['object_id']]
            if len(matches)!=1 or receipts:
                raise Rejected('sealed full terminal intent is missing/duplicated or prematurely compacted')
            # The sole current Rust commit projector retains the complete exact
            # owner for N repair/content-copy recovery. GC compaction is a later
            # separately observed effect, never inferred from commit visibility.
            if matches[0] != terminal:
                raise Rejected('retained full terminal intent differs from the complete sealed body')
            for required in ('publication', 'owner_epoch', 'touch_parent_on_create'):
                if required not in terminal:
                    raise Rejected(f'current terminal intent omits required {required}')
            if terminal.get('status') != 'committed':
                raise Rejected('sealed terminal write witness is not committed')
            if terminal.get('object_metadata') not in payload['transaction']['objects']:
                raise Rejected('terminal object body omitted from atomic transaction')
        completion = payload.get('completed_request') or payload.get('completed_request_completion')
        if completion:
            rows=tables.get('namespace_requests',[])+tables.get('namespace_request_receipts',[])
            matches=[row for row in rows if row['requester_node_id']==completion['requester_node_id'] and row['request_id']==completion['request_id']]
            if len(matches)!=1 or any(matches[0].get(k)!=completion.get(k) for k in ('file_system_id','operation_id','command_digest','result','authority_shard_id')):
                raise Rejected('sealed routed request outcome is absent or changed')
        if not certificate.get('parent_path_absence_checks_complete') or not certificate.get('terminal_intent_and_request_outcomes_complete'):
            raise Unavailable('native parent/path absence or terminal outcome footprint incomplete')

    def validate_object_indexes(self, tables, object):
        """Current metadata_object_index's linked row identities, independently."""
        fs, oid = object['file_system_id'], object['object_id']
        path = object['path'].strip('/')
        name = path.rsplit('/', 1)[-1] or '/'
        parent = path.rsplit('/', 1)[0] if '/' in path else ''
        parent_inode = 'path:'+fs+':'+blake3(parent.encode()) if parent else 'fs:'+fs+':root'
        inode = 'obj:'+oid
        entries = [row for row in tables.get('paths', []) if row['file_system_id'] == fs and row['path'] == path]
        if len(entries) != 1:
            raise Rejected('atomic object has no unique current path index')
        entry = entries[0]
        expected = dict(file_system_id=fs, path=path, object_id=oid, inode_id=inode,
            parent_inode_id=parent_inode, name=name, kind=object['kind'],
            name_hash=int.from_bytes(bytes.fromhex(blake3(name.encode()))[:8], 'big'))
        if any(entry.get(k) != v for k,v in expected.items()) or entry not in tables.get('file_system_objects', []):
            raise Rejected('atomic path/filesystem index differs from the declared object')
        inodes = [row for row in tables.get('inodes', []) if row['inode_id'] == inode]
        required = dict(inode_id=inode, file_system_id=fs, object_id=oid, kind=object['kind'],
            owner_node_id=object['owner_node_id'], policy=object['policy'], visible_version=object['version'])
        if len(inodes) != 1 or any(inodes[0].get(k) != v for k,v in required.items()):
            raise Rejected('atomic inode index differs from the declared object')
        for field, default in (('content_hash', None), ('current_locations', []),
                               ('created_by_event_id', None), ('last_modified_by_event_id', None)):
            expected_value = object.get(field, default)
            if field == 'current_locations':
                expected_value = list(dict.fromkeys(expected_value))
            if inodes[0].get(field, default) != expected_value:
                raise Rejected('atomic inode content/provenance differs from the declared object')
        dentries = tables.get('dentries', [])
        if object['kind'] == 'tombstone':
            if any(row['file_system_id'] == fs and row['path'] == path for row in dentries):
                raise Rejected('atomic tombstone remains visible as a dentry')
        else:
            if entry not in dentries:
                raise Rejected('atomic live object lacks its exact dentry')
            fragments = [row for row in tables.get('dirfrags', [])
                         if row['file_system_id'] == fs and row['dir_inode_id'] == parent_inode
                         and row['dirfrag_id'] == entry['dirfrag_id']]
            count = sum(row['parent_inode_id'] == parent_inode and row['dirfrag_id'] == entry['dirfrag_id'] for row in dentries)
            if len(fragments) != 1 or u64(fragments[0]['entry_count'], 'actual fragment count') != count:
                raise Rejected('atomic directory fragment differs from the exact dentry set')
        if object['kind'] == 'hardlink':
            hardlinks = [row for row in tables.get('hardlinks', [])
                         if row['target_object_id'] == object.get('link_target_object_id')
                         and row['hardlink_object_id'] == oid and row['path_record'] == entry]
            if len(hardlinks) != 1:
                raise Rejected('atomic hardlink lacks its exact target/path index')

    def validate_declared_table_effects(self, tables, payload, certificate=None, committed_entry=None, superseded_metadata=()):
        puts = {'put-file-system':('file_systems','file_system'), 'put-object':('objects','object'),
            'put-version':('versions','version'), 'put-version-pin':('version_pins','pin'),
            'put-receipt':('receipts','receipt'), 'put-intent-receipt':('intent_receipts','receipt'),
            'put-advisory-lock-state':('advisory_lock_states','state'),
            'put-namespace-transaction-receipt':('namespace_transaction_receipts','receipt'),
            'put-namespace-job':('namespace_jobs','job'), 'put-namespace-job-receipt':('namespace_job_receipts','receipt'),
            'put-namespace-request':('namespace_requests','request'), 'put-namespace-request-receipt':('namespace_request_receipts','receipt')}
        for effect in payload.get('metadata_effects', []):
            kind = effect['type']
            if kind in ('put-intent','put-receipt'):
                table,field=('intents','intent') if kind=='put-intent' else ('receipts','receipt')
                sources=[] if certificate is None else [row for row in certificate.get('effect_materializations',[])
                    if row.get('effect')==effect[field] and row.get('table')==table]
                if len(sources)!=1 or committed_entry is None:
                    raise Unavailable('metadata effect lacks its exact original commit materialization')
                event=next((event for event in self.trace['events'] if event['event_id']==sources[0]['event_id']),None)
                if event is None:
                    raise Unavailable('metadata original commit event unavailable')
                self.validate_effect_materialization(sources[0],payload,committed_entry,event['action']['operation']['payload_blob'])
                if effect[field] not in tables.get(table,[]) and effect not in superseded_metadata:
                    raise Unavailable('metadata replacement has no observed exact sealed lineage')
                continue
            if kind not in puts:
                raise Unavailable('current effect needs an exact table transformation refinement: '+kind)
            table, field = puts[kind]
            if effect[field] not in tables.get(table, []):
                raise Rejected('declared native metadata effect is absent: '+kind)
            if kind == 'put-object':
                self.validate_object_indexes(tables, effect[field])
        for pin in payload['transaction'].get('file_version_pins') or []:
            if pin not in tables.get('version_pins', []):
                raise Rejected('declared native version pin is absent')
        for lock in payload['transaction'].get('advisory_lock_states', []):
            if lock not in tables.get('advisory_lock_states', []):
                raise Rejected('declared native advisory state is absent')

    def decoded_native_kv_value(self, raw_value, native_type, evidence_refs):
        encoded = native_bytes(raw_value, 'native KV value')
        identity = hashlib.sha256(encoded).hexdigest()
        for ref in evidence_refs:
            evidence = self.blob(ref, 'DfsNativeKvValueDecodeObservationV1')
            self.require_certificate(evidence, 'actual native KV decode')
            if evidence['encoded_value_sha256'] == identity and evidence['decoded_native_type'] == native_type:
                if self.blob_bytes(evidence['encoded_value_blob']) != encoded:
                    raise Rejected('KV decode observation bytes differ from actual compare/effect')
                return self.blob(evidence['decoded_value_blob'], native_type)
        raise Unavailable('actual native KV compare/effect decoder evidence absent')







    def guard(self, reference, digest):
        fact = self.blob(reference, 'DfsNativePublicationGuardObservationV1')
        self.require_certificate(fact, 'publication guard')
        if digest is not None and fact['digest'] != digest:
            raise Rejected('guard authorizes a different immutable prepared operation')
        if not all(fact.get(key) is True for key in ('actual_writer_lock_retained',
                'raw_scoped_fence_retained', 'execution_and_data_owner_retained',
                'retained_config_and_missing_handoff_qualified')):
            raise Unavailable('actual retained configuration/writer/fence/custody evidence is incomplete')
        revision, config = self.config(fact['expected_config'])
        op = self.op(fact['digest']['operation_id'])
        members = sorted(set(config['replica_node_ids'] + [config['primary_node_id']]))
        path = fact['authorization_mode']
        if path == 'full-shard' and self.profile['standalone'] is False:
            if set(fact['authenticated_prepare_ack_node_ids']) != set(members):
                raise Rejected('FullShard guard lacks ALL actual assigned participants')
            acks = self.node_set(fact['authenticated_prepare_ack_node_ids']); number = 1
        elif path == 'local' and self.profile['standalone'] is True:
            acks = finite_set([]); number = 0
        else:
            raise Unavailable('authority mode is incompatible with explicit Local/clustered profile')
        if fact.get('native_permit_observation_blob') is not None:
            raise Rejected('retired per-operation KV fallback cannot authorize a current guard')
        return dict(op=op, epoch=revision, revision=revision, primary=self.node(config['primary_node_id']),
                    members=self.node_set(members), path=number, acks=acks,
                    pending=fact['admission_pending_handoff'], frozen=fact['admission_write_frozen'])


    def validate_interval(self, event):
        kind = event['action']['kind']
        if kind not in ('prepare-payload','commit-digest'):
            return
        if event['action']['outcome'] != 'applied':
            return
        interval = self.intervals.get(event['event_id'])
        if interval is None:
            raise Unavailable('semantic native append interval not certified')
        cert = self.blob(interval['complete_interval_certificate_blob'],'DfsNativeJournalIntervalCertificateV1')
        self.require_certificate(cert,'complete append interval')
        if cert['event_id'] != event['event_id'] or cert['owner_id'] != event['owner_id'] or cert['exact_frame_blobs'] != interval['exact_frame_blobs']:
            raise Rejected('append interval owner/event/frame identity differs')
        first,last = u64(interval['first_sequence'],'first sequence'),u64(interval['last_sequence'],'last sequence')
        if first > last or first != u64(interval['prior_prefix']['journal_sequence'],'prior cut')+1 or last != u64(interval['next_prefix']['journal_sequence'],'next cut'):
            raise Rejected('native append interval gaps/truncates its certified cuts')
        entries=cert['decoded_frames']
        if len(entries) != last-first+1 or len(entries) != len(interval['exact_frame_blobs']):
            raise Rejected('incomplete native interval frame coverage')
        for offset,item in enumerate(entries):
            raw=self.blob_bytes(interval['exact_frame_blobs'][offset])
            if blake3(raw) != item['exact_frame_blake3'] or u64(item['sequence'],'physical sequence') != first+offset:
                raise Rejected('native frame bytes/sequence mismatch')
            self.blob(item['native_entry_blob'],'DfsMetadataJournalEntry')
        if not cert['exact_prefix_chain_validated_by_native_decoder']:
            raise Unavailable('native SOBJ decode/exact-prefix chain certificate absent')
        if self.cut(event['owner_id'],interval['next_prefix']) != self.cut(event['owner_id'],interval['prior_prefix'])+1:
            raise Rejected('semantic interval does not preserve native cut ordering')

    def transition(self, event):
        action = event['action']; kind = action['kind']; op = None
        if kind == 'fallback-permit':
            raise Rejected('retired per-operation KV fallback action is incompatible')
        if 'operation' in action:
            op = self.op(action['operation']['operation_id'])
        # Kernel outcome is not reply delivery. An ambiguous source result must
        # be resolved by an actual terminal/effect observation, never relabeled.
        if 'outcome' in action and action['outcome'] != 'applied':
            raise Unavailable('non-applied native outcome requires actual typed effect/rejection grouping')
        mapping = {
            'prepare-payload': ('prepare', [op], 'prepare-payload'),
            'publication-guard-admitted': ('admitCommit', [op], 'publication-guard-admitted'),
            'freeze': ('freezeRetiring', [], 'retiring-prefix-frozen'),
            'activate': ('activate', [False], 'handoff-activated'),
            'rebind': ('bindSuccessor', [], 'successor-bound'),
            'replay': ('replay', [op, op*10+1] if op else [], 'exact-replay'),
        }
        if kind == 'commit-digest':
            if action['supplied_digest'] != action['operation']['native_digest']:
                raise Rejected('commit supplied digest differs from sealed body')
            mapping[kind] = ('commit', [op, op*10+1, False], 'commit-known')
        if kind == 'replay':
            self.observed_flags.setdefault('replayed', set()).add(op)
        if kind == 'handoff-begin':
            native = self.blob(action['handoff_blob'], 'DfsMetadataShardMembershipHandoff')
            _, retiring = self.config(action['retiring'])
            _, desired = self.config(action['desired'])
            if (native['handoff_id'] != action['handoff_id']
                    or native['retiring_shard'] != retiring or native['desired_shard'] != desired):
                raise Rejected('handoff action differs from its complete actual native record')
            if u64(desired['epoch'], 'desired epoch') != u64(retiring['epoch'], 'retiring epoch') + 1:
                raise Rejected('handoff does not advance the exact current epoch by one')
            if any(retiring[field] != desired[field] for field in ('file_system_id', 'shard_id', 'assignment', 'default_policy')):
                raise Rejected('handoff changes its actual assignment/policy scope')
            old_members = set(retiring['replica_node_ids'] + [retiring['primary_node_id']])
            new_members = set(desired['replica_node_ids'] + [desired['primary_node_id']])
            promote = retiring['primary_node_id'] != desired['primary_node_id']
            cause = native.get('cause')
            if not isinstance(cause, dict):
                raise Unavailable('actual typed handoff cause is missing')
            if cause == {'kind': 'membership'}:
                if promote and old_members != new_members or not promote and old_members == new_members:
                    raise Rejected('ordinary membership cause does not describe a supported topology change')
                mapping[kind] = ('beginHandoff', [promote], 'handoff-pending')
            else:
                raise Unavailable('unmodeled current native handoff cause')
        if kind == 'handoff-ack':
            mapping[kind] = ('acknowledgeHandoff', [self.node(action['authenticated_source_node'])], 'handoff-prefix-ack')
        if kind=='replica-freeze-read':
            proof,freeze=self.replica_freeze_read(action['native_freeze_read_observation_blob'])
            if proof['owner_id']!=event['owner_id'] or self.owners[event['owner_id']]['node_id']!=action['recipient_node_id']:
                raise Rejected('replica READ action substitutes another actual owner')
            mapping[kind]=('observeReplicaFreezeRead',[self.node(action['recipient_node_id']),
                u64(proof['read_source']['owner_ordinal'],'normal READ ordinal'),freeze],'replica-freeze-read')
        if kind == 'replica-ingest':
            witness = self.blob(action['native_ingest_observation_blob'], 'DfsNativeReplicaDurableIngestObservationV1')
            self.require_certificate(witness, 'actual receiver ingest')
            if (witness['owner_id'] != event['owner_id']
                    or witness['recipient_node_id'] != action['recipient_node_id']
                    or witness['durable_journal_prefix'] != action['prefix']
                    or witness['after_held_snapshot_id'] != event['after_snapshot']):
                raise Rejected('receiver ingest action borrows another actual owner/result cut')
            materialized=witness.get('actual_materialization_source')
            if materialized:
                mapping[kind]=('materializeReplicaPrefix',[self.node(action['recipient_node_id']),
                    self.cut(event['owner_id'],action['prefix'])],'replica-prefix-materialized')
            else:
                mapping[kind] = ('ingestPrefix', [self.node(action['recipient_node_id']),
                    self.cut(event['owner_id'], action['prefix'])], 'replica-prefix-durable')
        if kind == 'replica-progress':
            witness = self.blob(action.get('native_prepare_ack_blob'), 'DfsNativePreparePrefixAckObservationV1')
            mapping[kind] = ('acknowledgePrepare', [self.op(witness['operation_id']), self.node(witness['authenticated_node_id'])], 'prepare-prefix-ack')
        if kind == 'content-receipt':
            mapping[kind] = ('stage', [self.op(action['operation_id']), self.node(action['replica_node_id'])], 'content-receipt')
        if kind == 'environment-binding':
            mapping[kind] = ('changeConnectivity', [self.node_set(action['online_node_ids'])], 'connectivity')
        if kind == 'promotion-marker':
            mapping[kind] = ('persistPromotionMarker', [], 'promotion-marker')
        if kind == 'recovery':
            mapping[kind] = ('restart', [self.node(self.owners[event['owner_id']]['node_id'])], 'restart-unbound')
        if kind == 'publication-guard-released':
            mapping[kind] = ('releasePublicationGuard', [self.op(action['operation_id'])], 'publication-guard-released')
        if kind in ('reply', 'activation-reply'):
            snapshot = self.trace.get('snapshots', {}).get(event.get('after_snapshot'))
            if snapshot is None:
                raise Unavailable('reply lacks its actual native observation context')
            _, facts = self.held(snapshot)
            observed = self.blob(facts.get('actual_client_reply_observation_blob'), 'DfsNativeClientReplyObservationV1')
            self.require_certificate(observed, 'actual final client outcome')
            if (observed.get('phase') != 'client-operation-outcome'
                    or observed.get('owner_id') != event['owner_id']
                    or observed.get('delivery') != action['delivery']):
                raise Rejected('reply action changes its actual final client outcome')
            identity = 'operation_id' if kind == 'reply' else 'handoff_id'
            if observed.get(identity) != action[identity] or (kind == 'reply' and observed.get('attempt_id') != action['attempt_id']):
                raise Rejected('reply borrows another actual operation/handoff/attempt')
            if action['delivery'] == 'cancelled' and observed.get('actual_cancellation_observed') is not True:
                raise Unavailable('abandoned RPC future does not establish actual cancellation')
            delivery = action['delivery']
            if delivery not in ('known-accepted','unknown','cancelled','lost'):
                raise Unavailable('known rejection requires exact native rejected-effect mapping')
            unknown = delivery != 'known-accepted'
            if kind == 'reply':
                op = self.op(action['operation_id'])
                if unknown: self.observed_flags.setdefault('lostReply', set()).add(op)
                mapping[kind] = ('observeReply', [op, unknown], 'reply-observed')
            else:
                if unknown: self.observed_flags['activationReplyLost'] = True
                mapping[kind] = ('observeActivationReply', [unknown], 'activation-reply-observed')
        if kind in ('capture-failure','journal-append','kv-observation'):
            raise Unavailable(f'{kind}: explicit semantic grouping required; loss/effects cannot stutter')
        if kind not in mapping:
            raise Unavailable(f'unmodeled native action {kind}')
        name, args, self.last = mapping[kind]
        return dict(action=name, arguments=args)

    def project(self):
        terminal=self.trace['terminal']
        if any(u64(terminal[k],k) for k in ('dropped_events','incomplete_events','discontinuities')) or not terminal['owners_drained'] or not terminal['subscriptions_closed']:
            raise Unavailable('capture did not complete/drain without loss')
        ids=self.trace['initial_cut']['baseline_snapshot_ids']
        if len(ids)!=1:
            raise Unavailable('one certified complete logical-shard initial snapshot required')
        initial_snapshot = self.trace['snapshots'][ids[0]]
        _, initial_facts = self.held(initial_snapshot)
        anchors = self.trace['initial_cut']['native_prefix_by_owner']
        if set(anchors) != set(self.owners):
            raise Rejected('initial native prefix anchors omit an actual owner')
        for peer in initial_facts['peer_certified_cuts'].values():
            owner_id = peer['owner_id']
            if anchors[owner_id] != peer['prefix'] or self.cut(owner_id,peer['prefix']) != 0:
                raise Rejected('initial cut is not anchored to actual complete per-node held prefixes')
        self.certify_initial_baseline(initial_facts)
        states=[self.project_snapshot(ids[0])];transitions=[];previous=ids[0];seen=set();ordinals={x:0 for x in self.owners}
        for event in self.trace['events']:
            owner=event['owner_id'];ordinal=u64(event['owner_sequence'],'owner ordinal')
            if owner not in ordinals or ordinal!=ordinals[owner]+1 or event['event_id'] in seen or not set(event['caused_by']).issubset(seen):
                raise Rejected('native owner/causal discontinuity')
            ordinals[owner]=ordinal
            before=self.project_snapshot(event['before_snapshot'])
            if before!=states[-1]:
                raise Rejected('native before cut does not equal preceding observed logical shard cut')
            self.validate_interval(event)
            transition=self.transition(event)
            after=self.project_snapshot(event['after_snapshot'])
            if event['action']['kind']=='prepare-payload':
                old_tables,_=self.held(self.trace['snapshots'][event['before_snapshot']]);new_tables,_=self.held(self.trace['snapshots'][event['after_snapshot']])
                if any(old_tables.get(k,[])!=new_tables.get(k,[]) for k in VISIBLE_TABLES):
                    raise Rejected('hidden native prepare changed visible footprint')
            states.append(after);transitions.append(transition);seen.add(event['event_id']);previous=event['after_snapshot']
        if not transitions:
            raise Unavailable('no captured native semantic transitions')
        if {key:u64(value,'terminal ordinal') for key,value in terminal['final_owner_sequences'].items()}!=ordinals:
            raise Rejected('terminal owner coverage/ordinal differs')
        return dict(projection_version=1,origin='rust-native-derived-boundary-v1',profile=self.profile,states=states,transitions=transitions,
                    implementation_revision=self.trace['implementation_revision'],implementation_build_id=self.trace['implementation_build_id'],model_sha256=self.trace['model_sha256'],schema_sha256=self.trace['schema_sha256'])


def project_native_trace(trace, *, artifact_binding):
    try:
        return NativeProjectorV1(trace, artifact_binding=artifact_binding).project()
    except (KeyError, IndexError, TypeError) as exc:
        raise Unavailable(f'incomplete or unsupported native fact layout: {exc}') from exc


def verify_native_trace(trace, replay_verifier: Callable[...,dict], *, artifact_binding, **kwargs):
    """Driver-injected existing actual Quint verifier; never a local invariant substitute."""
    projection=project_native_trace(trace, artifact_binding=artifact_binding)
    result=replay_verifier(projection['states'],projection['transitions'],**kwargs)
    return dict(projection=projection,quint_replay=result)


def native_fragment_self_test():
    """Synthetic native bytes/rows test adapter sensitivity, not Rust conformance."""
    scope = {'file_system_id':'native-fs','shard_id':'root'}
    payload = {'protocol_version':7, **scope, 'owner_epoch':9007199254740993,
               'operation_id':'write:native-op', 'expected_object_versions':[],
               'expected_path_occupants':[], 'transaction':{'file_system_id':'native-fs',
               'objects':[{'object_id':'object','file_system_id':'native-fs','version':'target',
               'path':'/file','kind':'file','owner_node_id':'node1','policy':'replicated-eventual',
               'current_locations':['node1'],
               'mode':384,'module_metadata':{'vendor.example':{'payload':'x'*3073}}}],
               'versions':[]}, 'content_receipts':[], 'required_replica_node_ids':[],
               'prepared_intent':None,'terminal_intent':None,
               'completed_request':None,'completed_request_completion':None}
    canonical = json.dumps(payload, ensure_ascii=False, separators=(',',':')).encode()
    native_hash = 'dfs-prepared-operation-v7:blake3:' + blake3(b'sinomo.dfs.prepared-operation\0v7\0'+canonical)
    digest = {'protocol_version':7, **scope, 'owner_epoch':9007199254740993,
              'operation_id':'write:native-op','payload_digest':native_hash,'prepare_sequence':18}
    record = {'digest':digest,'payload':payload,'status':'prepared'}
    adapter = object.__new__(NativeProjectorV1)
    adapter.scope=scope;adapter.operations={'write:native-op':1}
    adapter.payloads={'write:native-op':{native_hash:1}}
    adapter.blobs={'canonical':{'native_type':'DfsPreparedOperationPayload','encoding':'json-utf8',
                              'data':canonical.decode(),'sha256':hashlib.sha256(canonical).hexdigest()}}
    assert adapter.record(record,'canonical')[0] == 1
    rejected = 0
    def rejects(action):
        nonlocal rejected
        try: action()
        except (Rejected,Unavailable): rejected += 1
        else: raise AssertionError('native negative fragment unexpectedly accepted')
    changed=copy.deepcopy(record);changed['payload']['transaction']['objects'][0]['mode']=420
    rejects(lambda:adapter.record(changed,'canonical'))
    changed_scope=copy.deepcopy(record);changed_scope['payload']['shard_id']='foreign'
    rejects(lambda:adapter.record(changed_scope,'canonical'))
    committed=copy.deepcopy(record);committed['status']='committed'
    committed['committed_entry']={'shard_id':'root','sequence':19,'created_at':'2026-10-06T00:00:00Z',
        'digest':copy.deepcopy(digest)}
    assert adapter.record(committed,'canonical')[3]['sequence']==19
    committed['committed_entry']['digest']['payload_digest']='changed'
    rejects(lambda:adapter.record(committed,'canonical'))
    obj=payload['transaction']['objects'][0]
    path=dict(file_system_id='native-fs',path='file',object_id='object',inode_id='obj:object',
        parent_inode_id='fs:native-fs:root',dirfrag_id='root',name='file',kind='file',
        name_hash=int.from_bytes(bytes.fromhex(blake3(b'file'))[:8],'big'))
    inode=dict(inode_id='obj:object',file_system_id='native-fs',object_id='object',kind='file',
        owner_node_id='node1',policy='replicated-eventual',visible_version='target',current_locations=['node1'])
    tables=dict(objects=[obj],versions=[],paths=[path],file_system_objects=[path],inodes=[inode],dentries=[path],
        dirfrags=[dict(file_system_id='native-fs',dir_inode_id='fs:native-fs:root',dirfrag_id='root',entry_count=1)])
    certificate={'actual_footprint_rows':{k:tables[k] for k in tables}, 'superseded_effects':[],
                 'parent_path_absence_checks_complete':True,'terminal_intent_and_request_outcomes_complete':True}
    adapter.validate_atomic_rows(tables,payload,certificate)
    rejects(lambda:adapter.validate_atomic_rows({'objects':[]},payload,certificate))
    absent=copy.deepcopy(certificate);absent['absent_native_rows']=[{'table':'objects','native_identity_fields':{'object_id':'object'}}]
    rejects(lambda:adapter.validate_atomic_rows(tables,payload,absent))
    terminal={'intent_id':'intent','file_system_id':'native-fs','object_id':'object','target_version':'target',
              'publication':{'kind':'current'},'owner_epoch':7,'touch_parent_on_create':False,
              'coordinator_node_id':'node1','policy':{'min_commit_replicas':1},'status':'committed',
              'created_at':'2026-10-06T00:00:00Z','updated_at':'2026-10-06T00:00:01Z','object_metadata':obj}
    receipt={k:v for k,v in terminal.items() if k not in ('updated_at','object_metadata')}
    receipt.update(terminal_at=terminal['updated_at'],retained_at='2026-10-06T00:00:02Z')
    terminal_payload=copy.deepcopy(payload);terminal_payload['terminal_intent']=terminal
    entry={'created_at':receipt['retained_at']}
    adapter.validate_atomic_rows(dict(tables,intents=[terminal]),terminal_payload,certificate,entry)
    rejects(lambda:adapter.validate_atomic_rows(dict(tables,intents=[]),terminal_payload,certificate,entry))
    rejects(lambda:adapter.validate_atomic_rows(dict(tables,intents=[terminal,terminal]),terminal_payload,certificate,entry))
    rejects(lambda:adapter.validate_atomic_rows(dict(tables,intent_receipts=[receipt]),terminal_payload,certificate,entry))
    rejects(lambda:adapter.validate_atomic_rows(dict(tables,intents=[terminal],intent_receipts=[receipt]),terminal_payload,certificate,entry))
    for required in ('publication','owner_epoch','touch_parent_on_create'):
        missing=copy.deepcopy(terminal);del missing[required]
        rejects(lambda:adapter.validate_atomic_rows(dict(tables,intents=[missing]),terminal_payload,certificate,entry))
    for field in ('updated_at','target_version','status','object_metadata'):
        changed=copy.deepcopy(terminal);changed[field]='changed-sealed-field'
        rejects(lambda:adapter.validate_atomic_rows(dict(tables,intents=[changed]),terminal_payload,certificate,entry))
    owners={'owner1':{'node_id':'node1'},'owner2':{'node_id':'node2'}}
    snapshots={'primary':{'owner_id':'owner1','scope':scope,'local_certified_prefix':{'digest':'primary-cut','journal_sequence':'19'}},
               'replica':{'owner_id':'owner2','scope':scope,'local_certified_prefix':{'digest':'replica-cut','journal_sequence':'18'}}}
    peers={'node1':{'owner_id':'owner1','held_snapshot_id':'primary','prefix':snapshots['primary']['local_certified_prefix']},
           'node2':{'owner_id':'owner2','held_snapshot_id':'replica','prefix':snapshots['replica']['local_certified_prefix']}}
    assert select_primary_held_snapshot({'primary_node_id':'node1'},peers,snapshots,owners,scope)['local_certified_prefix']['journal_sequence']=='19'
    bad_peers=copy.deepcopy(peers);bad_peers['node1']['held_snapshot_id']='replica'
    rejects(lambda:select_primary_held_snapshot({'primary_node_id':'node1'},bad_peers,snapshots,owners,scope))
    # Retired per-operation KV authority has no current projection action.
    rejects(lambda: adapter.transition({'action': {'kind': 'fallback-permit'}}))
    # Reply delivery changes no predicted commit flag. Its observed semantics
    # are separate and the real Quint action later checks committed prestate.
    adapter.observed_flags={};adapter.last='prepare-payload'
    event={'action':{'kind':'commit-digest','outcome':'applied','operation':{'operation_id':'write:native-op','native_digest':native_hash},'supplied_digest':native_hash}}
    assert adapter.transition(event)=={'action':'commit','arguments':[1,11,False]}
    assert 'lostReply' not in adapter.observed_flags
    ambiguous=copy.deepcopy(event);ambiguous['action']['outcome']='ambiguous'
    rejects(lambda:adapter.transition(ambiguous))
    adapter.trace={'snapshots':{}}
    reply={'action':{'kind':'reply','operation_id':'write:native-op','delivery':'lost'}}
    rejects(lambda:adapter.transition(reply))
    assert 'lostReply' not in adapter.observed_flags
    return {'native_negative_fragments':rejected,'complex_canonical_payload_bytes':len(canonical),
            'classification':'synthetic adapter sensitivity only'}


# Official BLAKE3-team 1.8.2 test_vectors/test_vectors.json; input byte i % 251.
# Source SHA256 dcb91ea8accc77e6d6e632af7cdc1a99a9f3ae78cf648da595c7d064db32f624.
OFFICIAL_BLAKE3_DEFAULT_VECTORS = [(0, 'af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262'), (1, '2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213'), (2, '7b7015bb92cf0b318037702a6cdd81dee41224f734684c2c122cd6359cb1ee63'), (3, 'e1be4d7a8ab5560aa4199eea339849ba8e293d55ca0a81006726d184519e647f'), (4, 'f30f5ab28fe047904037f77b6da4fea1e27241c5d132638d8bedce9d40494f32'), (5, 'b40b44dfd97e7a84a996a91af8b85188c66c126940ba7aad2e7ae6b385402aa2'), (6, '06c4e8ffb6872fad96f9aaca5eee1553eb62aed0ad7198cef42e87f6a616c844'), (7, '3f8770f387faad08faa9d8414e9f449ac68e6ff0417f673f602a646a891419fe'), (8, '2351207d04fc16ade43ccab08600939c7c1fa70a5c0aaca76063d04c3228eaeb'), (63, 'e9bc37a594daad83be9470df7f7b3798297c3d834ce80ba85d6e207627b7db7b'), (64, '4eed7141ea4a5cd4b788606bd23f46e212af9cacebacdc7d1f4c6dc7f2511b98'), (65, 'de1e5fa0be70df6d2be8fffd0e99ceaa8eb6e8c93a63f2d8d1c30ecb6b263dee'), (127, 'd81293fda863f008c09e92fc382a81f5a0b4a1251cba1634016a0f86a6bd640d'), (128, 'f17e570564b26578c33bb7f44643f539624b05df1a76c81f30acd548c44b45ef'), (129, '683aaae9f3c5ba37eaaf072aed0f9e30bac0865137bae68b1fde4ca2aebdcb12'), (1023, '10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11'), (1024, '42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7'), (1025, 'd00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444'), (2048, 'e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a'), (2049, '5f4d72f40d7a5f82b15ca2b2e44b1de3c2ef86c426c95c1af0b6879522563030'), (3072, 'b98cb0ff3623be03326b373de6b9095218513e64f1ee2edd2525c7ad1e5cffd2'), (3073, '7124b49501012f81cc7f11ca069ec9226cecb8a2c850cfe644e327d22d3e1cd3'), (4096, '015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969'), (4097, '9b4052b38f1c5fc8b1f9ff7ac7b27cd242487b3d890d15c96a1c25b8aa0fb995'), (5120, '9cadc15fed8b5d854562b26a9536d9707cadeda9b143978f319ab34230535833'), (5121, '628bd2cb2004694adaab7bbd778a25df25c47b9d4155a55f8fbd79f2fe154cff'), (6144, '3e2e5b74e048f3add6d21faab3f83aa44d3b2278afb83b80b3c35164ebeca205'), (6145, 'f1323a8631446cc50536a9f705ee5cb619424d46887f3c376c695b70e0f0507f'), (7168, '61da957ec2499a95d6b8023e2b0e604ec7f6b50e80a9678b89d2628e99ada77a'), (7169, 'a003fc7a51754a9b3c7fae0367ab3d782dccf28855a03d435f8cfe74605e7817'), (8192, 'aae792484c8efe4f19e2ca7d371d8c467ffb10748d8a5a1ae579948f718a2a63'), (8193, 'bab6c09cb8ce8cf459261398d2e7aef35700bf488116ceb94a36d0f5f1b7bc3b'), (16384, 'f875d6646de28985646f34ee13be9a576fd515f76b5b0a26bb324735041ddde4'), (31744, '62b6960e1a44bcc1eb1a611a8d6235b6b4b78f32e7abc4fb4c6cdcce94895c47'), (102400, 'bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085')]

def self_test():
    assert blake3(b'') == 'af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262'
    assert blake3(b'abc') == '6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85'
    for length, expected in OFFICIAL_BLAKE3_DEFAULT_VECTORS:
        assert blake3(bytes(i % 251 for i in range(length))) == expected, length
    assert u64(str(MAX_U64),'maximum') == MAX_U64
    for bad in ('00','-1',str(MAX_U64+1),True,1.5):
        try:u64(bad,'negative fixture')
        except Rejected:pass
        else:raise AssertionError('u64 rejection missing')
    for invalid in ({'a':1,'b':1},{'a':5}):
        try:injective(invalid,NODES,'negative dictionary')
        except (Rejected,Unavailable):pass
        else:raise AssertionError('injective finite-bound rejection missing')
    try:exact_json(b'{"x":1,"x":2}')
    except Rejected:pass
    else:raise AssertionError('duplicate JSON rejection missing')
    report = native_fragment_self_test()
    report['official_blake3_default_vectors'] = len(OFFICIAL_BLAKE3_DEFAULT_VECTORS)
    print(json.dumps(report,sort_keys=True))


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true')
    args=parser.parse_args()
    if args.self_test:self_test()
    else:parser.error('use imported project_native_trace with the source-bound driver')
