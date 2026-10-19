#!/usr/bin/env python3
"""Assemble source-bound native observations; never create model states.

The native header must come from the Rust capture utility. It supplies actual
owner/profile/control/peer-cut observations and a certified initial cut. Kernel
packets supply actual held cuts and exact returned frames. Missing header facts
exit 2; malformed/lost evidence exits 1. This does not run or claim conformance;
check-shard-trace.py performs the independent projection and Quint replay.
"""
from __future__ import annotations
import argparse
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('shard_trace_check', HERE/'check-shard-trace.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
SUBTREE_SPEC=importlib.util.spec_from_file_location('native_subtree',HERE/'native-json-subtree.py')
SUBTREE=importlib.util.module_from_spec(SUBTREE_SPEC)
SUBTREE_SPEC.loader.exec_module(SUBTREE)
Rejected, Unavailable = CHECK.Rejected, CHECK.Unavailable


def byte_vector(value: Any, label: str) -> bytes:
    if not isinstance(value, list) or any(type(x) is not int or not 0 <= x <= 255 for x in value):
        raise Rejected(f'{label}: expected exact native byte vector')
    return bytes(value)


def native_json(raw: bytes, label: str) -> Any:
    try:
        return json.loads(raw, object_pairs_hook=CHECK._pairs,
                          parse_float=lambda _: (_ for _ in ()).throw(Rejected(f'{label}: float forbidden')))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise Rejected(f'{label}: invalid native JSON') from exc


def reject_model_fields(value: Any) -> None:
    if isinstance(value, dict):
        if set(value) & {'model_state', 'expected_state', 'expected_poststate', 'quint_action', 'Case', 'ITF'}:
            raise Rejected('predicted/model state is not native input')
        for child in value.values():
            reject_model_fields(child)
    elif isinstance(value, list):
        for child in value:
            reject_model_fields(child)


def native_metadata_memory_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Compare held storage DATA across ordinary getter argument views.

    These copied arguments never establish a persistent writer binding. Exact
    guard or resident custody observations remain required independently.
    """
    argument_fields = {'writer_bound_epoch', 'requested_writer_epoch_view'}
    for held in (left, right):
        if not argument_fields.issubset(held) or 'backend_instance_authority_epoch' not in held:
            raise Rejected('native held memory omits explicit ordinary epoch-view arguments')
        for field in argument_fields | {'backend_instance_authority_epoch'}:
            value = held[field]
            if value is not None and (type(value) is not int or not 0 <= value <= 2**64-1):
                raise Rejected('native held memory has a malformed ordinary epoch-view argument')
        request = held['requested_writer_epoch_view']
        instance = held['backend_instance_authority_epoch']
        expected = request if request is not None and request == instance else None
        if held['writer_bound_epoch'] != expected:
            raise Rejected('native storage view differs from copied ordinary request/instance arguments')
    return ({key: value for key, value in left.items() if key not in argument_fields}
            == {key: value for key, value in right.items() if key not in argument_fields})


def journal_rpc_request_matches(fact, command, source_node, target_node):
    """Match actual direct or routed journal DATA without inventing envelopes."""
    phase = fact.get('phase')
    if fact.get('target_node_id') != target_node:
        return False
    if phase == 'direct-journal-rpc-attempt-started':
        if fact.get('command') != command:
            return False
        if fact.get('route') != '/dfs/metadata/journal-batch' or command.get('source_node_id') != source_node:
            raise Rejected('actual direct journal call changes its route/source identity')
        return True
    if phase == 'rpc-attempt-started':
        routed = fact['command']['routed_message']
        return (routed['source_node_id'] == source_node and routed['target_node_id'] == target_node
                and routed['message'].get('metadata-journal-batch') == command)
    return False


def journal_rpc_return_phase(start):
    return ('direct-journal-rpc-attempt-returned' if start['phase'] == 'direct-journal-rpc-attempt-started'
            else 'rpc-attempt-returned')


def verify_journal_rpc_return(start, returned, expected_response, source_node, target_node):
    if (returned.get('phase') != journal_rpc_return_phase(start)
            or returned.get('attempt_id') != start.get('attempt_id')
            or returned.get('error_detail') is not None or returned.get('delivery_state') is not None):
        raise Rejected('accepted journal ACK lacks its SAME successful actual transport attempt')
    reply = returned.get('response')
    if reply is None or reply.get('request_id') != start['command']['request_id']:
        raise Rejected('accepted journal ACK changes actual transport request identity')
    if start['phase'] == 'direct-journal-rpc-attempt-started':
        if reply != expected_response:
            raise Rejected('accepted journal ACK differs from actual typed direct response')
    elif not any(message['source_node_id'] == target_node and message['target_node_id'] == source_node
                 and message['message'].get('metadata-journal-batch-response') == expected_response
                 for message in reply['outbound_messages']):
        raise Rejected('accepted journal ACK differs from actual authenticated routed response')


def actual_journal_rpc_command(start, source_node, target_node):
    """Read the exact existing direct/routed typed command, without a wrapper."""
    if start.get('phase') == 'direct-journal-rpc-attempt-started':
        command = start.get('command')
    elif start.get('phase') == 'rpc-attempt-started':
        routed = start.get('command', {}).get('routed_message', {})
        command = routed.get('message', {}).get('metadata-journal-batch')
    else:
        return None
    if command is None:
        return None
    if not journal_rpc_request_matches(start, command, source_node, target_node):
        raise Rejected('native journal attempt changes actual source/target route')
    return command


def committed_ingest_group_sources(raw, source):
    """Classify from every actual referenced ingest, never a retargeted anchor."""
    anchor=(source['owner_id'],CHECK.decimal_u64(source['event_id'].rsplit(':',1)[1]))
    actual=raw.get(anchor)
    if actual is None:raise Rejected('ingest group lacks its actual anchored source')
    refs=[(ref['actual_mount_label'],CHECK.decimal_u64(ref['owner_ordinal'])) for ref in source['raw_references']]
    persisted=[key for key in refs if raw[key]['kind']=='boundary'
               and raw[key]['record']['facts']['phase']=='replica-journal-persisted']
    committed=[key for key in persisted if raw[key]['record']['facts']['command'].get('authority_probe') is None
               and raw[key]['record']['facts']['command'].get('durable_replication') is not None]
    phase=actual['record'].get('facts',{}).get('phase')
    if phase=='replica-materialization-completed':
        if persisted:raise Rejected('materialization cannot consume its separately modeled durable ingest')
        return None
    command=actual['record'].get('facts',{}).get('command',{})
    anchored=phase=='replica-journal-persisted' and command.get('authority_probe') is None and command.get('durable_replication') is not None
    if not committed and not anchored:return None
    if phase!='replica-journal-persisted' or persisted!=[anchor] or committed!=[anchor]:
        raise Rejected('committed ingest group omits, repeats or retargets its actual persisted boundary')
    accepted=[key for key in refs if raw[key]['kind']=='boundary'
              and raw[key]['record']['facts']['phase']=='source-journal-response-accepted']
    if len(accepted)!=1:raise Unavailable('committed ingest grouping lacks its actual accepted response')
    return anchor,accepted[0]


def verify_committed_replication(raw, headers, owners, scope, selected, receiver_key, source_key, *, group_refs=None, group_owner=None):
    """Join one actual terminal frame to its original writer and full RPC return.

    This is existing replica ingestion DATA, never a prepared-prefix ACK.
    Unknown batches, absent original materialization or missing returns fail closed.
    """
    number = SUBTREE.PROJECTOR.u64
    receiver = raw[receiver_key]['record']; accepted = raw[source_key]['record']['facts']
    fact = receiver['facts']; command = fact['command']; frames = command['batch']['journal_frames']
    source, recipient = owners[source_key[0]]['node_id'], owners[receiver_key[0]]['node_id']
    if (fact['phase'] != 'replica-journal-persisted' or len(frames) != 1
            or command.get('authority_probe') is not None or command.get('prefix_probe') is not None
            or command.get('verify_prefix_only') is not False):
        raise Unavailable('committed replication needs its single actual non-probe terminal frame')
    decodes = [row['decoded_entry'] for row in headers[receiver_key[0]].get('actual_received_frame_decodes', [])
               if number(row['source_owner_ordinal'], 'receiver decoder ordinal') == receiver_key[1]
               and row['frame_index'] == 0]
    if len(decodes) != 1:
        raise Unavailable('committed replication lacks its exact normal receiver decoder')
    entry = decodes[0]; mutation = entry['mutation']; digest = mutation.get('digest'); op = mutation.get('operation_id')
    if mutation.get('type') != 'commit-prepared-operation-v7' or op not in selected or not isinstance(digest, dict):
        raise Unavailable('committed replication is outside the selected exact terminal family')
    if (digest.get('protocol_version') != 7 or digest.get('operation_id') != op
            or any(digest.get(k) != scope[k] for k in ('file_system_id', 'shard_id'))
            or mutation.get('file_system_id') != scope['file_system_id'] or entry['shard_id'] != scope['shard_id']
            or number(entry['sequence'], 'terminal sequence') != number(frames[0]['sequence'], 'wire sequence')):
        raise Rejected('committed replication changes its original terminal digest/scope/sequence')
    def one(phase, kind='kernel'):
        found = [(key, item['record']) for key, item in raw.items() if key[0] == source_key[0]
                 and item['kind'] == kind and item['record'].get('phase') == phase
                 and item['record'].get('operation', item['record'].get('digest')) == digest]
        if len(found) != 1:
            raise Unavailable('committed replication lacks original ' + phase + ' source')
        return found[0]
    prepared_key, prepared = one('prepare-materialized')
    durable_key, durable = one('commit-durable')
    materialized_key, materialized = one('commit-materialized')
    guard_key, guard = one('admitted', 'guard')
    starts = [(key, item['record']) for key, item in raw.items() if key[0] == source_key[0]
              and item['kind'] == 'boundary' and journal_rpc_request_matches(item['record']['facts'], command, source, recipient)]
    if len(starts) != 1:
        raise Unavailable('committed replication lacks its unique actual source start')
    start_key, start = starts[0]
    returns = [(key, item['record']) for key, item in raw.items() if key[0] == source_key[0]
               and item['kind'] == 'boundary' and item['record']['facts'].get('phase') == journal_rpc_return_phase(start['facts'])
               and item['record']['facts'].get('attempt_id') == start['facts']['attempt_id']]
    constructed = [(key, item['record']) for key, item in raw.items() if key[0] == receiver_key[0] and key[1] > receiver_key[1]
                   and item['kind'] == 'boundary' and item['record']['facts'].get('phase') == 'replica-journal-response-constructed'
                   and item['record']['facts'].get('response') == accepted.get('response')]
    if len(returns) != 1 or len(constructed) != 1:
        raise Unavailable('committed replication lacks its exact constructed and returned response')
    return_key, returned = returns[0]; constructed_key, _ = constructed[0]
    if not prepared_key[1] < guard_key[1] < durable_key[1] < materialized_key[1] < start_key[1] < return_key[1] <= source_key[1]:
        raise Rejected('committed replication borrows a later or reordered original source')
    verify_journal_rpc_return(start['facts'], returned['facts'], accepted['response'], source, recipient)
    response = accepted['response']; purpose = command.get('durable_replication'); config = guard['facts']['authority'].get('expected')
    if (not isinstance(purpose, dict) or not isinstance(purpose.get('confirmation_id'), str) or not purpose['confirmation_id']
            or type(purpose.get('background_publication')) is not bool or not isinstance(config, dict)
            or command['file_system_id'] != scope['file_system_id'] or command['batch']['shard_id'] != scope['shard_id']
            or config != fact['actual_source_config'] or config['primary_node_id'] != source
            or any(config[k] != scope[k] for k in ('file_system_id','shard_id'))
            or guard['matches_actual_mounted_owner'] is not True or guard['facts']['actor_node_id'] != source
            or recipient not in config.get('replica_node_ids', []) or fact['local_node_id'] != recipient
            or command['source_node_id'] != source or number(command['authority_epoch'], 'command epoch') != number(config['epoch'], 'config epoch')
            or fact['actual_pending_handoff'] is not None
            or fact['permit'] != dict(file_system_id=scope['file_system_id'], shard_id=scope['shard_id'],
                                     source_node_id=source, source_epoch=config['epoch'], pending_primary_handoff_id=None)
            or accepted.get('phase') != 'source-journal-response-accepted' or accepted['authenticated_source_node_id'] != recipient
            or accepted['expected_target_node_id'] != recipient or response['request_id'] != command['request_id']
            or response['shard_id'] != scope['shard_id'] or response['result'] != 'applied'
            or response['verified_prefix_only'] is not False or accepted['expected_prefix_only'] is not False
            or accepted['expected_confirmation_id'] != purpose['confirmation_id'] or response['verified_authority_probe_id'] != purpose['confirmation_id']
            or number(response['next_after_sequence'], 'returned head') != number(entry['sequence'], 'terminal head')
            or number(accepted['expected_next_after_sequence'], 'accepted head') != number(entry['sequence'], 'terminal head')
            or number(command['batch']['next_after_sequence'], 'command head') != number(entry['sequence'], 'terminal head')
            or number(fact['applied_entries'], 'persisted count') != 1 or number(response['applied_entries'], 'returned count') != 1):
        raise Rejected('committed replication changes actual confirmation/config/permit/response identity')
    source_decodes = [row['decoded_entry'] for row in headers[source_key[0]].get('actual_frame_decodes', [])
                      if number(row['owner_ordinal'], 'source decoder ordinal') == durable_key[1]]
    if source_decodes != [entry] or durable['exact_entry'] != entry or durable['exact_frame'] != frames[0]['exact_bytes']:
        raise Rejected('committed replication differs from the original exact writer frame/decoder')
    before = native_json(byte_vector(receiver['before_held_json'], 'receiver before'), 'actual receiver before')
    after = native_json(byte_vector(receiver['after_held_json'], 'receiver after'), 'actual receiver after')
    held = native_json(byte_vector(materialized['after_held_json'], 'writer terminal cut'), 'actual writer terminal cut')
    old = native_json(byte_vector(prepared['after_held_json'], 'writer prepared cut'), 'actual writer prepared cut')
    for view in (held, old):
        if view['backend_root'] != owners[source_key[0]]['canonical_root'] or view['shard_id'] != scope['shard_id']:
            raise Rejected('committed replication borrows another original writer root')
    for key in (receiver_key, source_key, prepared_key, durable_key, materialized_key, guard_key, start_key, return_key, constructed_key):
        if raw[key]['record']['owner_label'] != key[0] or number(raw[key]['record']['owner_ordinal'], 'actual source ordinal') != key[1]:
            raise Rejected('committed replication changes its actual source owner/ordinal')
    for view in (before, after):
        if view['backend_root'] != owners[receiver_key[0]]['canonical_root'] or view['shard_id'] != scope['shard_id']:
            raise Rejected('committed replication borrows another receiver root')
    prior = before['certified_prefix']; through = SUBTREE.PROJECTOR.extend_native_prefix(prior, entry['sequence'], byte_vector(frames[0]['exact_bytes'], 'terminal wire frame'))
    if (native_prefix(through) != native_prefix(fact['durable_prefix']) or native_prefix(held['certified_prefix']) != native_prefix(through)
            or before['tables'] != after['tables'] or before['materialized_through_sequence'] != after['materialized_through_sequence']
            or before['certified_prefix'] != after['certified_prefix'] or fact['materialization_pending'] is not True):
        raise Rejected('committed replication changes its exact durable/materialized prefix or table cut')
    rows = [row for row in held['tables']['prepared_operations_v7'] if row['digest'] == digest]
    original = [row for row in old['tables']['prepared_operations_v7'] if row['digest'] == digest]
    reference = dict(shard_id=entry['shard_id'], sequence=entry['sequence'], created_at=entry['created_at'], digest=digest)
    if len(rows) != 1 or len(original) != 1 or rows[0]['status'] != 'committed' or rows[0]['committed_entry'] != reference or rows[0]['payload'] != original[0]['payload']:
        raise Rejected('committed replication lacks its whole original sealed body and terminal reference')
    index = held['tables']['prepared_operations_v7'].index(rows[0])
    canonical = SUBTREE.canonical_prepared_payload(byte_vector(materialized['after_held_json'], 'original terminal body'),
                                                 ['tables','prepared_operations_v7',index,'payload'], digest['payload_digest'])
    SUBTREE.PROJECTOR.sealed_content_requirement(rows[0]['payload'], digest, canonical)
    references = (receiver_key, start_key, return_key, source_key, constructed_key)
    if group_refs is not None:
        supplied = [(ref['actual_mount_label'], number(ref['owner_ordinal'], 'group source ordinal')) for ref in group_refs]
        if len(supplied) != len(references) or set(supplied) != set(references) or group_owner != receiver_key[0]:
            raise Rejected('committed ingest grouping changes its exact original five sources')
    return op, materialized_key, references


def verify_not_sent_journal_attempt(start_record, returned_record, *, source_node,
                                    target_node, digest, prepared_prefix,
                                    prepared_ordinal, guard_ordinal, members):
    """Certify only the actual transport's DefinitelyNotSent observation.

    This supplies no ACK, offline state, client reply or cancellation fact.
    Unknown delivery and abandonment are deliberately outside this refinement.
    """
    start, returned = start_record['facts'], returned_record['facts']
    command = actual_journal_rpc_command(start, source_node, target_node)
    if command is None:
        raise Unavailable('no-effect grouping requires an actual typed journal call')
    if (target_node == source_node or target_node not in members
            or start_record['owner_label'] != returned_record['owner_label']
            or not start.get('attempt_id')
            or returned.get('phase') != journal_rpc_return_phase(start)
            or returned.get('attempt_id') != start['attempt_id']):
        raise Rejected('not-sent journal pair changes actual owner/attempt/member')
    number = SUBTREE.PROJECTOR.u64
    if not (prepared_ordinal < number(start_record['owner_ordinal'], 'RPC start ordinal')
            < number(returned_record['owner_ordinal'], 'RPC return ordinal') < guard_ordinal):
        raise Rejected('not-sent journal attempt is outside its actual preparation/guard')
    if (returned.get('delivery_state') != 'definitely-not-sent'
            or returned.get('response') is not None
            or not isinstance(returned.get('error_detail'), str)
            or not returned['error_detail'].strip()):
        raise Unavailable('journal failure lacks actual DefinitelyNotSent/no-response/error')
    probe = command.get('authority_probe')
    operation = (probe or {}).get('operation', {})
    fields = ('operation_id', 'file_system_id', 'shard_id', 'owner_epoch',
              'prepare_sequence', 'payload_digest')
    for field in ('owner_epoch', 'prepare_sequence'):
        number(operation.get(field), 'not-sent operation ' + field)
        number(digest[field], 'sealed operation ' + field)
    if (operation.get('kind') != 'prepared-operation'
            or any(operation.get(field) != digest[field] for field in fields)
            or command.get('file_system_id') != digest['file_system_id']
            or command.get('source_node_id') != source_node
            or command.get('batch', {}).get('shard_id') != digest['shard_id']
            or number(command['batch']['next_after_sequence'], 'not-sent native head')
                != number(digest['prepare_sequence'], 'native prepared slot')
            or native_prefix(probe['prepared_prefix']) != native_prefix(prepared_prefix)):
        raise Rejected('not-sent journal command changes exact prepared tuple/prefix')
    return command


def selected_effect_in_bootstrap_cut(held, payloads, records, received_entries=()):
    """Recognize selected native work even after its prepared row was GC'd.

    A baseline-only peer may retain genuine unselected terminal bootstrap DATA;
    no selected append, receiver observation or complete sealed effect is hidden.
    """
    selected = set(payloads)
    tables = held['tables']
    for item in records:
        record = item['record']
        if item['kind'] in ('kernel', 'guard'):
            if record.get('operation', record.get('digest', {})).get('operation_id') in selected:
                return True
        elif item['kind'] == 'boundary':
            fact = record['facts']
            if fact.get('phase') in ('replica-journal-persisted', 'replica-prefix-probe-accepted'):
                probe = fact['command'].get('authority_probe') or {}
                if probe.get('operation', {}).get('operation_id') in selected:
                    return True
    for entry in received_entries:
        mutation = entry['mutation']
        operation = (mutation.get('operation_id') or mutation.get('digest', {}).get('operation_id')
                     or mutation.get('record', {}).get('digest', {}).get('operation_id'))
        if operation in selected:
            return True
    if any(row['digest']['operation_id'] in selected for row in tables['prepared_operations_v7']):
        return True
    for payload in payloads.values():
        transaction = payload['transaction']
        for table, expected in (('objects', transaction['objects']),
                                ('versions', transaction['versions']),
                                ('receipts', payload['content_receipts'])):
            if any(row in tables.get(table, []) for row in expected):
                return True
        for intent in (payload.get('prepared_intent'), payload.get('terminal_intent')):
            if intent and any(row.get('intent_id') == intent['intent_id']
                              and row.get('object_id') == intent['object_id']
                              for table in ('intents', 'intent_receipts') for row in tables.get(table, [])):
                return True
        completion = payload.get('completed_request') or payload.get('completed_request_completion')
        if completion and any(row.get('requester_node_id') == completion['requester_node_id']
                              and row.get('request_id') == completion['request_id']
                              for table in ('namespace_requests', 'namespace_request_receipts')
                              for row in tables.get(table, [])):
            return True
    return False


class Blobs:
    def __init__(self, native: dict[str, Any]):
        self.values = copy.deepcopy(native)
        for name, value in self.values.items():
            raw = value['data'].encode() if value['encoding']=='json-utf8' else base64.b64decode(value['data'], validate=True)
            if hashlib.sha256(raw).hexdigest() != value['sha256']:
                raise Rejected(f'{name}: native blob hash differs')

    def put(self, kind: str, raw: bytes, encoding: str='json-utf8') -> str:
        digest = hashlib.sha256(raw).hexdigest()
        name = f'{kind}:{digest}'
        value = {'native_type':kind, 'encoding':encoding,
                 'data':raw.decode() if encoding=='json-utf8' else base64.b64encode(raw).decode(),
                 'sha256':digest}
        if name in self.values and self.values[name] != value:
            raise Rejected('native blob identity collision')
        self.values[name] = value
        return name

    def json(self, kind: str, value: Any) -> str:
        # Only actual parsed facts are serialized here. Canonical payload bytes
        # are supplied separately by Rust; this generic re-encoding never hashes
        # a prepared operation or manufactures its digest.
        return self.put(kind, json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode())


def validate_capture(capture: dict[str, Any]) -> list[dict[str, Any]]:
    if capture.get('raw_observation_version') != 1:
        raise Unavailable('unsupported/missing actual native capture')
    if not capture.get('closed') or not capture.get('actual_owner_stopped'):
        raise Unavailable('native capture/mounted owner did not close and drain')
    for field in ('in_flight','dropped','incomplete'):
        if CHECK.decimal_u64(capture[field],field):
            raise Rejected(f'native capture has nonzero {field}')
    label = capture['actual_mount_label']
    combined=[]
    for kind, collection in (('kernel', 'packets'),('authoritative-read','authoritative_reads'),('guard','guard_events'),('content-receipt','content_receipts'),('replica-ingest','replica_packets'),('configuration-cut','configuration_cuts'),('metadata-cache-cut','metadata_cache_cuts'),('boundary','boundary_packets'),('control-commit','control_commits'),('original-control-commit','original_control_commits')):
        for record in capture.get(collection,[]):
            if record['owner_label'] != label:
                raise Rejected('native event borrows another mount owner')
            ordinal=CHECK.decimal_u64(record['owner_ordinal'],'native ordinal')
            combined.append({'kind':kind,'ordinal':ordinal,'record':record})
    combined.sort(key=lambda item:item['ordinal'])
    if [item['ordinal'] for item in combined] != list(range(1,len(combined)+1)):
        raise Rejected('native raw owner sequence is missing or duplicated')
    return combined



def verify_replica_freeze_read(raw, owners, scope, read_key):
    """Qualify a separate SAME normal read before its correlated durable ingest."""
    row = raw.get(read_key)
    if row is None or row['kind'] != 'boundary':
        raise Rejected('replica freeze read lacks its actual boundary source')
    record = row['record']; fact = record['facts']; owner_id, ordinal = read_key
    if fact.get('phase') != 'replica-write-freeze-read':
        raise Rejected('replica freeze read reference changes its actual phase')
    read_id = fact.get('read_id')
    if (not isinstance(read_id, str) or not read_id
            or record.get('normal_replica_freeze_read_id') != read_id
            or not isinstance(fact.get('actual_writer_token'), str) or not fact['actual_writer_token']):
        raise Unavailable('normal replica freeze read has no same-call held writer identity')
    ingests = [(key, item['record']) for key, item in raw.items()
               if key[0] == owner_id and item['kind'] == 'boundary'
               and item['record']['facts']['phase'] in ('replica-journal-persisted', 'replica-snapshot-accepted')
               and item['record'].get('normal_replica_freeze_read_id') == read_id]
    if len(ingests) != 1:
        raise Unavailable('normal replica freeze read lacks one exact correlated ingest')
    ingest_key, ingest = ingests[0]
    if ordinal >= ingest_key[1]:
        raise Rejected('normal replica freeze read was not observed before its ingest')
    if fact['permit'] != ingest['facts']['permit']:
        raise Rejected('normal replica freeze read changes its same ingest permit')
    permit = fact['permit']; owner = owners[owner_id]
    if any(permit[key] != scope[key] for key in ('file_system_id', 'shard_id')):
        raise Rejected('normal replica freeze read crosses its actual shard scope')
    custody = record.get('resident_custody'); later = ingest.get('resident_custody')
    if not isinstance(custody, dict) or not isinstance(later, dict):
        raise Unavailable('normal replica freeze read lacks same resident and admission custody')
    for current, n in ((custody, ordinal), (later, ingest_key[1])):
        if (current['actual_mount_label'] != owner_id or current['local_node_id'] != owner['node_id']
                or current['backend_root'] != owner['canonical_root']
                or current['observer_owner_ordinal'] != str(n)
                or any(current[key] != scope[key] for key in ('file_system_id', 'shard_id'))
                or current['admission_kind'] not in ('exact-shard', 'global-exclusive')
                or not all(current.get(key) for key in ('resident_handle_token', 'data_owner_token', 'admission_scheduler_token'))):
            raise Rejected('normal replica freeze read changes its actual owner or held scope')
        admitted = current['admitted_scope']
        if (admitted['kind'] != current['admission_kind']
                or admitted['kind'] not in ('exact-shard', 'global-exclusive')
                or admitted['kind'] == 'exact-shard' and any(admitted[key] != scope[key]
                    for key in ('file_system_id', 'shard_id'))):
            raise Rejected('normal replica freeze read changes its actual admitted scope')
    if ({key: value for key, value in custody.items() if key != 'observer_owner_ordinal'}
            != {key: value for key, value in later.items() if key != 'observer_owner_ordinal'}):
        raise Rejected('normal replica freeze read borrows another resident or data-owner handle')
    before = native_json(byte_vector(record['before_held_json'], 'freeze read before'), 'freeze read before')
    after = native_json(byte_vector(record['after_held_json'], 'freeze read after'), 'freeze read after')
    ingest_before = native_json(byte_vector(ingest['before_held_json'], 'freeze ingest before'), 'freeze ingest before')
    if before != after or not native_metadata_memory_equal(after, ingest_before):
        raise Rejected('normal replica freeze read changes or substitutes its actual held storage data')
    if after['backend_root'] != owner['canonical_root'] or after['shard_id'] != scope['shard_id']:
        raise Rejected('normal replica freeze read held data is outside its actual root')
    for (actor, n), item in raw.items():
        if actor != owner_id or not ordinal < n < ingest_key[1]:
            continue
        if item['kind'] in ('kernel', 'guard'):
            raise Unavailable('normal replica freeze read crosses another actual writer transition')
        if item['kind'] == 'boundary' and item['record']['facts']['phase'] == 'replica-write-freeze-read':
            raise Rejected('normal replica freeze read skips a newer same-call read')
        held = item['record'].get('after_held_json')
        if held is not None and not native_metadata_memory_equal(after,
                native_json(byte_vector(held, 'intervening freeze read held'), 'intervening freeze read held')):
            raise Unavailable('normal replica freeze read crosses changed held storage data')
    observed = fact.get('observed_freeze')
    if not isinstance(observed, dict) or observed.get('observation') not in ('absent', 'present'):
        raise Unavailable('normal replica freeze read did not retain its actual result')
    if observed['observation'] == 'absent' and set(observed) != {'observation'}:
        raise Rejected('normal absent freeze read contains a fabricated marker')
    if observed['observation'] == 'present' and (set(observed) != {'observation', 'value'}
            or not isinstance(observed['value'], dict)):
        raise Rejected('normal present freeze read has no complete actual marker')
    return dict(owner_id=owner_id, read_id=read_id, read_source=dict(owner_id=owner_id, owner_ordinal=str(ordinal)),
        ingest_source=dict(owner_id=owner_id, owner_ordinal=str(ingest_key[1])),
        actual_read_fact=fact, actual_custody=custody, observed_freeze=observed)



def verify_replica_freeze_gate(raw, owners, scope, blobs, context, revision):
    facts = native_json(blobs.values[context['native_operational_facts_blob']]['data'].encode(), 'freeze gate facts')
    gate = facts['actual_operational_gate']; reference = gate.get('actual_replica_freeze_read_observation_blob')
    cut = facts['actual_cut_as_of_source']; memory = facts['actual_memory_cut_source']
    if gate.get('write_freeze_observation')=='not-observed':
        owner_id=context['owner_id'];owner=owners[owner_id]
        memory_key=memory['owner_id'],CHECK.decimal_u64(memory['owner_ordinal'])
        actual=raw[memory_key]['record'].get('resident_custody')
        cfg=native_json(blobs.values[context['active_config']['native_config_blob']]['data'].encode(),'unknown peer config')
        if (reference is not None or gate.get('native_write_freeze_blob') is not None
                or gate.get('durable_write_frozen') is not None or gate.get('read_under_actual_owner') is not False
                or gate.get('actual_replica_ingest_read_source') is not None
                or cut['owner_id']!=owner_id or memory_key[0]!=owner_id
                or gate['writer_owner_id']!=owner_id or cfg['primary_node_id']==owner['node_id']
                or not isinstance(actual,dict)
                or actual['actual_mount_label']!=owner_id or actual['local_node_id']!=owner['node_id']
                or actual['backend_root']!=owner['canonical_root']
                or actual['observer_owner_ordinal']!=str(memory_key[1])
                or any(actual[field]!=scope[field] for field in ('file_system_id','shard_id'))
                or actual['admission_kind'] not in ('exact-shard','global-exclusive')
                or actual['admitted_scope']['kind']!=actual['admission_kind']
                or actual['admission_kind']=='exact-shard' and any(
                    actual['admitted_scope'][field]!=scope[field] for field in ('file_system_id','shard_id'))
                or owner['storage_generation_token']!='resident-slot:'+actual['resident_handle_token']
                or owner['root_lease_token']!='mounted-resident-owner:'+actual['data_owner_token']
                or owner['lane_incarnation']!=actual['admission_scheduler_token']):
            raise Rejected('Unknown freeze observation is not an actual nonauthor resident cut')
        encoded=raw[memory_key]['record'].get(memory['side']+'_held_json')
        held=native_json(byte_vector(encoded,'Unknown actual held bytes'),'Unknown actual held')
        if (held['write_freeze']!={'observation':'not-observed'} or held['backend_root']!=owner['canonical_root']
                or held['shard_id']!=scope['shard_id']):
            raise Rejected('Unknown changes the actual held freeze observation')
        for key,row in raw.items():
            if (key[0]!=owner_id or key[1]>CHECK.decimal_u64(cut['owner_ordinal']) or row['kind']!='boundary'
                    or row['record']['facts']['phase']!='replica-write-freeze-read'):continue
            qualified=verify_replica_freeze_read(raw,owners,scope,key)
            if qualified['actual_custody']['resident_handle_token']==actual['resident_handle_token']:
                raise Rejected('Unknown omits an already observed SAME resident freeze read')
        return
    if reference is None:
        return
    value = blobs.values[reference]
    if value['native_type'] != 'DfsNativeReplicaFreezeReadObservationV1':
        raise Rejected('replica freeze gate changes its actual read certificate type')
    proof = native_json(value['data'].encode(), 'replica freeze read gate')
    source = proof['read_source']; key = source['owner_id'], CHECK.decimal_u64(source['owner_ordinal'])
    qualified = verify_replica_freeze_read(raw, owners, scope, key)
    expected = dict(scope=scope, implementation_revision=revision, owner_bindings=list(owners.values()),
                    complete=True, continuous=True, **qualified)
    if proof != expected:
        raise Rejected('replica freeze gate changes its whole actual read or owner certificate')
    if (cut['owner_id'] != qualified['owner_id'] or context['owner_id'] != qualified['owner_id']
            or gate['writer_owner_id'] != qualified['owner_id']
            or CHECK.decimal_u64(qualified['read_source']['owner_ordinal']) > CHECK.decimal_u64(cut['owner_ordinal'])):
        raise Rejected('replica freeze gate borrows a future or foreign checked read')
    memory_key = memory['owner_id'], CHECK.decimal_u64(memory['owner_ordinal'])
    actual = raw[memory_key]['record'].get('resident_custody')
    original = qualified['actual_custody']
    if (memory_key[0] != qualified['owner_id'] or not isinstance(actual, dict)
            or {k: v for k, v in actual.items() if k != 'observer_owner_ordinal'}
                != {k: v for k, v in original.items() if k != 'observer_owner_ordinal'}):
        raise Rejected('replica freeze gate borrows another held resident or admission')
    for (owner_id, ordinal), event in raw.items():
        if owner_id != key[0] or not key[1] < ordinal <= CHECK.decimal_u64(cut['owner_ordinal']):
            continue
        if event['kind'] == 'boundary' and event['record']['facts']['phase'] in (
                'retiring-freeze-persisted', 'successor-rebound', 'handoff-activation-completed'):
            raise Unavailable('replica freeze read cannot cross a later normal marker transition')
        if event['kind']=='boundary' and event['record']['facts']['phase']=='replica-write-freeze-read':
            newer=verify_replica_freeze_read(raw,owners,scope,(owner_id,ordinal))
            if newer['actual_custody']['resident_handle_token']==original['resident_handle_token']:
                raise Rejected('replica freeze gate skips a newer SAME resident normal READ')
    accepted=[]
    for candidate,row in raw.items():
        if (candidate[0]!=key[0] or candidate[1]>CHECK.decimal_u64(cut['owner_ordinal'])
                or row['kind']!='boundary' or row['record']['facts']['phase']!='replica-write-freeze-read'):continue
        prior=verify_replica_freeze_read(raw,owners,scope,candidate)
        if ({k:v for k,v in prior['actual_custody'].items() if k!='observer_owner_ordinal'}
                =={k:v for k,v in original.items() if k!='observer_owner_ordinal'}
                and CHECK.decimal_u64(prior['ingest_source']['owner_ordinal'])<=CHECK.decimal_u64(cut['owner_ordinal'])):
            accepted.append((CHECK.decimal_u64(prior['ingest_source']['owner_ordinal']),prior['read_source']))
    expected_accepted=max(accepted,key=lambda row:row[0])[1] if accepted else None
    if gate.get('actual_replica_ingest_read_source')!=expected_accepted:
        raise Rejected('replica freeze observation changes its actual accepted ingest read')
    observed = qualified['observed_freeze']; label = 'observed-' + observed['observation']
    if gate['write_freeze_observation'] != label or gate['durable_write_frozen'] != (label == 'observed-present'):
        raise Rejected('replica freeze gate changes its actual normal read result')
    marker_ref = gate.get('native_write_freeze_blob')
    if observed['observation'] == 'absent':
        if marker_ref is not None:
            raise Rejected('absent replica freeze read borrows a marker')
    elif marker_ref is None or native_json(blobs.values[marker_ref]['data'].encode(), 'actual replica freeze marker') != observed['value']:
        raise Rejected('replica freeze gate changes its whole normal marker')


def validate_retained_configuration_cut(cut, header, scope, *, guard_id=None,
                                        expected=None):
    """Validate DATA from the SAME already-qualified retained configuration view.

    Its original three quorum proofs remain unchanged even when replay advances
    the accepted projection revision. This is not a fresh per-operation KV read.
    The source-bound Rust observation is emitted only after normal readiness.
    """
    fields = {'owner_label', 'owner_ordinal', 'requested_scope_prefixes', 'revision', 'accepted_authority_generation', 'scopes', 'authority', 'retained_projection_floor', 'guard_context_id'}
    if not isinstance(cut, dict) or 'guard_context_id' not in cut:
        raise Unavailable('retained configuration observation lacks current guard-context contract')
    if set(cut) != fields:
        raise Rejected('retained configuration observation has unknown or missing native fields')
    if cut.get('owner_label') != header.get('actual_mount_label'):
        raise Rejected('retained configuration cut borrows another mounted owner')
    ordinal = SUBTREE.PROJECTOR.u64(cut.get('owner_ordinal'), 'retained cut ordinal')
    if guard_id is not None and cut['guard_context_id'] != guard_id:
        raise Rejected('retained configuration cut borrows another guard context')
    prefixes = ['/dfs/filesystems/', '/dfs/services/content/', '/cluster/services/']
    if cut.get('requested_scope_prefixes') != prefixes:
        raise Rejected('retained configuration cut omits its exact three projection scopes')
    scopes, proofs = cut.get('scopes'), cut.get('authority')
    if not isinstance(scopes, list) or not isinstance(proofs, list) or len(scopes) != 3 or len(proofs) != 3:
        raise Rejected('retained configuration cut lacks complete scope/proof coverage')
    revision = SUBTREE.PROJECTOR.u64(cut.get('revision'), 'accepted projection revision')
    SUBTREE.PROJECTOR.u64(cut.get('accepted_authority_generation'), 'accepted authority generation')
    floor = cut.get('retained_projection_floor')
    if not isinstance(floor, dict) or not isinstance(floor.get('value'), dict):
        raise Unavailable('retained configuration cut has no actual native configuration floor')
    entry = floor['value']
    if entry.get('key') != '/cluster/raft/config' or not byte_vector(entry.get('value'), 'retained Raft config bytes'):
        raise Rejected('retained configuration floor changes its native configuration identity')
    created = SUBTREE.PROJECTOR.u64(entry.get('create_revision'), 'floor created')
    modified = SUBTREE.PROJECTOR.u64(entry.get('modify_revision'), 'floor modified')
    if not created <= modified <= SUBTREE.PROJECTOR.u64(floor.get('revision'), 'local floor revision') <= revision:
        raise Rejected('retained configuration floor crosses accepted projection revision')
    floor_term = SUBTREE.PROJECTOR.u64(floor.get('observed_term'), 'local floor term')
    SUBTREE.PROJECTOR.u64(floor.get('applied_index'), 'local floor applied index')
    voters = {row['node_id'] for row in header['normal_mounted_identity']['configured_members']
              if row['role'] == 'voter' and row['state'] == 'voter'}
    if not voters:
        raise Unavailable('native retained cut has no actual configured voter basis')
    terms = set()
    for prefix, rows, proof in zip(prefixes, scopes, proofs):
        if not isinstance(rows, list) or not isinstance(proof, dict) or set(proof) != {'term', 'commit_index', 'revision', 'basis'}:
            raise Rejected('retained scope/proof has an invalid native shape')
        term = SUBTREE.PROJECTOR.u64(proof.get('term'), 'original projection term')
        proof_revision = SUBTREE.PROJECTOR.u64(proof.get('revision'), 'original projection revision')
        if term == 0 or term < floor_term or not modified <= proof_revision <= revision:
            raise Rejected('retained projection authority contradicts its native floor/cut')
        if SUBTREE.PROJECTOR.u64(proof.get('commit_index'), 'original commit index') == 0:
            raise Rejected('retained projection proof has no current-term committed index')
        basis = proof.get('basis')
        if not isinstance(basis, dict) or set(basis) != {'current-term-quorum'}:
            raise Unavailable('retained projection proof is not current-term quorum evidence')
        quorum = basis['current-term-quorum']
        if not isinstance(quorum, dict) or set(quorum) != {'required_quorum', 'voter_acks'}:
            raise Rejected('retained quorum proof has malformed native fields')
        required = SUBTREE.PROJECTOR.u64(quorum.get('required_quorum'), 'projection quorum')
        acknowledged = SUBTREE.PROJECTOR.u64(quorum.get('voter_acks'), 'projection voter ACKs')
        if required != len(voters)//2+1 or not required <= acknowledged <= len(voters):
            raise Rejected('retained projection proof differs from actual native voter membership')
        terms.add(term)
        keys = []
        for row in rows:
            key = row.get('key')
            if not isinstance(key, str) or not key.startswith(prefix):
                raise Rejected('retained projection row is outside its complete scope')
            created = SUBTREE.PROJECTOR.u64(row.get('create_revision'), 'scope created')
            modified_row = SUBTREE.PROJECTOR.u64(row.get('modify_revision'), 'scope modified')
            if not created <= modified_row <= revision:
                raise Rejected('retained projection row is beyond its accepted cut')
            byte_vector(row.get('value'), 'retained scope entry bytes')
            keys.append(key)
        if keys != sorted(set(keys)):
            raise Rejected('retained projection has duplicate or unordered rows')
    if len(terms) != 1:
        raise Rejected('retained projection proofs cross native authority terms')
    prefix = '/dfs/filesystems/' + scope['file_system_id'] + '/'
    segment = scope['shard_id'].strip('/').replace('/', '_')
    kinds = [('metadata-shards/', 'DfsMetadataShardConfig'),
             ('metadata-membership-handoffs/', 'DfsMetadataShardMembershipHandoff')]
    keys = [prefix + family + segment for family, _ in kinds]
    entries = {row['key']: row for row in scopes[0]}
    decoded = []
    decodes = []
    for key, (_, kind) in zip(keys, kinds):
        row = entries.get(key)
        if row is None:
            decoded.append(None)
            continue
        found = [value for value in header['actual_control_decodes']
                 if SUBTREE.PROJECTOR.u64(value['source_owner_ordinal'], 'decoder ordinal') == ordinal
                 and value['exact_entry'] == row and value['native_type'] == kind]
        if len(found) != 1:
            raise Unavailable('retained configuration lacks its SAME ordinal/bytes typed decoder')
        decoded.append(found[0]['decoded_value']); decodes.append(found[0])
    config = decoded[0]
    if config is None:
        raise Unavailable('retained configuration cut has no active shard assignment')
    if any(config.get(key) != scope[key] for key in ('file_system_id', 'shard_id')):
        raise Rejected('retained configuration decoder describes another physical shard')
    members = config.get('replica_node_ids', []) + [config['primary_node_id']]
    if len(members) != len(set(members)):
        raise Rejected('retained configuration has duplicate assigned participants')
    if expected is not None and (config != expected or decoded[1] is not None):
        raise Rejected('guard retained configuration or missing handoff differs from actual mint')
    return dict(entries=scopes[0], cached_revision=str(revision), scope_prefix=prefixes[0],
                selected_keys=keys, decoded=decoded, typed_decodes=decodes,
                original_authority=proofs, retained_projection_floor=floor)


def validate_metadata_cache_cut(cut):
    """Independently fold actual accepted delta facts over the ORIGINAL read.

    Return that original authority unchanged, alongside the later cached rows
    and revision. A delta is never relabeled a new CurrentTermQuorum receipt.
    """
    if CHECK.decimal_u64(cut['provenance_loss_count'],'provenance loss'):
        raise Unavailable('native cache proof/delta ring lost observations')
    evidence=cut.get('original_native_provenance')
    if evidence is None:raise Unavailable('native cache has no retained original read provenance')
    prefix=cut['prefix'];read=evidence['original_read'];authority=read['authority']
    previous=CHECK.decimal_u64(str(authority['revision']),'original native revision')
    entries={}
    for entry in read['value']:
        if not entry['key'].startswith(prefix) or entry['key'] in entries:
            raise Rejected('original complete native read has foreign/duplicate row')
        entries[entry['key']]=entry
    for delta in evidence['deltas']:
        revision=CHECK.decimal_u64(str(delta['revision']),'actual delta revision')
        compacted=CHECK.decimal_u64(str(delta['compacted_revision']),'actual compaction')
        if revision<=previous or compacted>previous:
            raise Rejected('native accepted delta cannot extend its original cut')
        events=delta.get('events',[]);finals=delta.get('final_key_states',[])
        event_revisions={CHECK.decimal_u64(str(event['revision']),'native event revision') for event in events}
        if revision-previous>len(event_revisions) or not set(range(previous+1,revision+1))<=event_revisions:
            raise Rejected('native delta skips a real revision interval')
        if any(value>revision or value<=previous for value in event_revisions):
            raise Rejected('native delta event revision crosses its actual interval')
        keys=[state['key'] for state in finals]
        if len(keys)!=len(set(keys)):raise Rejected('native delta has duplicate final keys')
        relevant={event['key'] for event in events if event['key'].startswith(prefix)}
        if relevant!={key for key in keys if key.startswith(prefix)}:
            raise Rejected('native delta omits changed scope rows')
        for state in finals:
            entry=state['entry'];key=state['key']
            if entry is not None:
                if entry['key']!=key:raise Rejected('native delta changes a row key')
                created=CHECK.decimal_u64(str(entry['create_revision']),'created revision')
                modified=CHECK.decimal_u64(str(entry['modify_revision']),'modified revision')
                if created>modified or modified>revision:
                    raise Rejected('native delta row lies beyond its actual cut')
            if not key.startswith(prefix):continue
            if entry is None:entries.pop(key,None)
            else:entries[key]=entry
        previous=revision
    observed=CHECK.decimal_u64(cut['cached_revision'],'actual cached revision')
    if previous!=observed or observed<CHECK.decimal_u64(cut['required_revision'],'required cached revision'):
        raise Rejected('native cache rows/revision lack exact source continuity')
    rows=[entries[key] for key in sorted(entries)]
    if rows!=cut['entries']:
        raise Rejected('actual cache rows differ from the original read plus accepted delta chain')
    return {'original_read':read,'accepted_deltas':evidence['deltas'],
            'cached_revision':str(observed),'entries':rows,'scope_prefix':prefix}



def validate_metadata_cache_consumer_scope(cut, file_system_id, shard_id,
                                          selected_operation_ids=None):
    """Validate only actual preparation OR actual retained CP consumption DATA.

    This helper grants no authority. The caller separately validates the
    original complete native read/delta provenance and owner/horizon.
    """
    preparation = cut.get('actual_preparation_scope')
    consumption = cut.get('actual_consumption_scope')
    if (preparation is None) == (consumption is None):
        raise Rejected('native cache scope must have exactly one actual consumer')
    if preparation is not None:
        if (not isinstance(preparation, dict)
                or set(preparation) != {'file_system_id', 'shard_id', 'owner_epoch', 'operation_id'}):
            raise Rejected('native preparation consumer has malformed scope')
        if (preparation.get('file_system_id') != file_system_id
                or preparation.get('shard_id') != shard_id):
            raise Rejected('native control projection is scoped elsewhere')
        if (selected_operation_ids is not None
                and preparation.get('operation_id') not in selected_operation_ids):
            raise Rejected('initial cache context belongs to another selected operation')
        return
    if not isinstance(consumption, dict):
        raise Rejected('native retained consumer has malformed scope')
    prefix = '/dfs/filesystems/'
    if cut.get('prefix') != prefix:
        raise Rejected('native retained consumer does not use the complete metadata prefix')
    kind = consumption.get('kind')
    if kind == 'retained-prefix-seed':
        if set(consumption) != {'kind', 'prefix'} or consumption['prefix'] != prefix:
            raise Rejected('native retained seed has another actual prefix')
    elif kind == 'retained-prefix-returned':
        if (set(consumption) != {'kind', 'prefix', 'entries'}
                or consumption['prefix'] != prefix
                or consumption['entries'] != cut['entries']):
            raise Rejected('native prefix return differs from its exact retained seed')
    elif kind == 'retained-keys-returned':
        if set(consumption) != {'kind', 'keys', 'values'}:
            raise Rejected('native retained key return has unsupported fields')
        keys, values = consumption['keys'], consumption['values']
        if (not isinstance(keys, list) or not keys
                or any(not isinstance(key, str) or not key.startswith(prefix) for key in keys)
                or len(keys) != len(set(keys)) or not isinstance(values, list)
                or len(keys) != len(values)):
            raise Rejected('native retained key return has malformed correlation')
        segment = shard_id.strip('/').replace('/', '_')
        selected = {prefix + file_system_id + '/' + family + segment for family in (
            'metadata-shards/', 'metadata-membership-handoffs/',
            )}
        if not selected.intersection(keys):
            raise Rejected('native retained key return is scoped elsewhere')
        entries = {entry['key']: entry for entry in cut['entries']}
        if any(entries.get(key) != value for key, value in zip(keys, values)):
            raise Rejected('native key return differs from its exact retained seed')
    else:
        raise Rejected('native retained consumer kind is unknown')

def native_prefix(prefix):
    if prefix is None:raise Unavailable('actual held native prefix absent')
    return {'version':prefix['version'],'journal_sequence':str(prefix['journal_sequence']),'digest':prefix['digest']}


def derive_terminal_baseline(owners, scope, revision, raw_headers, raw, initial_cuts, operation_ids, blobs):
    """Certify terminal rows by actual typed body/decode/frame ancestry.

    Every initial cut is an actual captured held JSON byte vector. This helper
    certifies no configuration, writer binding, floor, voter, or predicted state.
    """
    result={};excluded=set();retained_identities={};used=set()
    for owner in owners:
        owner_id=owner['owner_id'];label=owner['component_incarnation']
        native=raw_headers[label]
        if native['actual_mount_label']!=label:raise Rejected('initial source header mount mismatch')
        cut=initial_cuts[owner_id]
        held_bytes=byte_vector(cut['held_json'],'actual initial held cut')
        held=native_json(held_bytes,'actual initial held cut')
        if held['backend_root']!=owner['canonical_root'] or held['shard_id']!=scope['shard_id']:
            raise Rejected('initial held cut belongs to another actual physical owner/scope')
        anchor=native_prefix(held['certified_prefix'])
        if int(anchor['journal_sequence'])!=held['materialized_through_sequence']:
            raise Unavailable('terminal baseline cut has deferred materialization')
        decodes={CHECK.decimal_u64(item['owner_ordinal']):item['decoded_entry'] for item in native['actual_frame_decodes']}
        frames={}
        for key,item in raw.items():
            if key[0]!=label or item['kind']!='kernel' or (cut.get('owner_ordinal') is not None and item['ordinal']>=cut['owner_ordinal']):continue
            packet=item['record']
            if packet['exact_frame'] is None:continue
            entry=packet['exact_entry'];exact=byte_vector(packet['exact_frame'],'actual frame')
            if decodes.get(item['ordinal'])!=entry:
                raise Unavailable('retained frame lacks its actual ordinary native decode equality')
            sequence=entry['sequence']
            if sequence in frames:raise Rejected('initial native physical frame slot duplicated')
            before=native_json(byte_vector(packet['before_held_json'],'frame prior'),'actual frame prior')
            after=native_json(byte_vector(packet['after_held_json'],'frame next'),'actual frame next')
            prior=native_prefix(before['certified_prefix']);next_cut=native_prefix(after['certified_prefix'])
            observed=SUBTREE.PROJECTOR.extend_native_prefix(prior,sequence,exact)
            if native_prefix(observed)!=next_cut:
                raise Rejected('actual retained frame does not reach its observed native prefix')
            if entry['shard_id']!=scope['shard_id'] or entry['mutation']['file_system_id']!=scope['file_system_id']:
                raise Rejected('baseline native frame crosses scope')
            frame_ref=blobs.put('DfsMetadataExactJournalFrame',exact,'exact-frame-base64')
            entry_ref=blobs.json('DfsMetadataJournalEntry',entry)
            decoder_ref=blobs.json('DfsNativeJournalDecodeObservationV1',{
                'scope':scope,'implementation_revision':revision,'owner_bindings':owners,
                'complete':True,'continuous':True,'owner_id':owner_id,
                'actual_owner_ordinal':str(item['ordinal']),
                'exact_frame_sha256':hashlib.sha256(exact).hexdigest(),'decoded_entry_blob':entry_ref})
            frames[sequence]=(key,entry,prior,next_cut,{'exact_frame_blob':frame_ref,
                'decoded_entry_blob':entry_ref,'decoder_observation_blob':decoder_ref})
        # Replica baseline ancestry comes from the SAME persisted wire frames,
        # paired with the ordinary receiver decoder. A copied terminal row alone
        # never proves its original commit or permits exclusion of pending work.
        for key,item in raw.items():
            if key[0]!=label or item['kind']!='boundary' or item['record']['facts']['phase']!='replica-journal-persisted' or (cut.get('owner_ordinal') is not None and item['ordinal']>=cut['owner_ordinal']):continue
            packet=item['record'];fact=packet['facts']
            before=native_json(byte_vector(packet['before_held_json'],'receiver frame prior'),'actual receiver frame prior')
            prior=native_prefix(before['certified_prefix'])
            for index,wire in enumerate(fact['command']['batch']['journal_frames']):
                decoded=[row['decoded_entry'] for row in native.get('actual_received_frame_decodes',[])
                    if CHECK.decimal_u64(row['source_owner_ordinal'])==item['ordinal'] and row['frame_index']==index]
                if len(decoded)!=1:raise Unavailable('baseline receiver frame lacks SAME ordinary decoder')
                entry=decoded[0];exact=byte_vector(wire['exact_bytes'],'baseline received frame');sequence=entry['sequence']
                if entry['shard_id']!=scope['shard_id'] or entry['mutation']['file_system_id']!=scope['file_system_id']:
                    raise Rejected('baseline received frame crosses actual source scope')
                if sequence<=int(prior['journal_sequence']):
                    known=frames.get(sequence)
                    if known and (known[1]!=entry or blobs.values[known[4]['exact_frame_blob']]['data']!=base64.b64encode(exact).decode()):
                        raise Rejected('baseline repeated receiver frame differs from its durable history')
                    continue
                next_cut=native_prefix(SUBTREE.PROJECTOR.extend_native_prefix(prior,sequence,exact))
                frame_ref=blobs.put('DfsMetadataExactJournalFrame',exact,'exact-frame-base64')
                entry_ref=blobs.json('DfsMetadataJournalEntry',entry)
                decoder_ref=blobs.json('DfsNativeJournalDecodeObservationV1',dict(scope=scope,implementation_revision=revision,
                    owner_bindings=owners,complete=True,continuous=True,owner_id=owner_id,
                    actual_owner_ordinal=str(item['ordinal']),actual_received_frame_index=index,
                    exact_frame_sha256=hashlib.sha256(exact).hexdigest(),decoded_entry_blob=entry_ref))
                candidate=(key,entry,prior,next_cut,dict(exact_frame_blob=frame_ref,decoded_entry_blob=entry_ref,decoder_observation_blob=decoder_ref))
                if sequence in frames and frames[sequence][1:4]!=candidate[1:4]:
                    raise Rejected('baseline receiver retained divergent exact frame ancestry')
                frames.setdefault(sequence,candidate);prior=next_cut
            if prior!=native_prefix(fact['durable_prefix']):
                raise Unavailable('receiver baseline frames do not reach the actual durable prefix')
        witnesses=[]
        rows=held['tables'].get('prepared_operations_v7',[])
        for index,record in enumerate(rows):
            opid=record['digest']['operation_id']
            if opid in operation_ids:continue
            if record['status']!='committed' or not record.get('committed_entry'):
                raise Unavailable('pending or noncommitted baseline record cannot be excluded')
            previous=retained_identities.setdefault(opid,record)
            if previous!=record:raise Rejected('baseline replicas retain different original operation outcomes')
            commit=record['committed_entry'];start=commit['sequence'];end=int(anchor['journal_sequence'])
            if start>end or any(sequence not in frames for sequence in range(start,end+1)):
                raise Unavailable('baseline exact commit-to-anchor physical ancestry absent')
            expected=frames[start][2];chain=[]
            for sequence in range(start,end+1):
                key,entry,prior,next_cut,item=frames[sequence]
                if prior!=expected:raise Rejected('baseline retained frame chain is discontinuous')
                expected=next_cut;chain.append(item);used.add(key)
            if expected!=anchor:raise Rejected('baseline actual frame ancestry ends at another cut')
            first=frames[start][1]
            if (first['mutation'].get('type')!='commit-prepared-operation-v7'
                    or first['mutation'].get('digest')!=commit['digest'] or first['created_at']!=commit['created_at']):
                raise Rejected('baseline commit reference differs from actual native commit frame')
            canonical=SUBTREE.canonical_prepared_payload(held_bytes,['tables','prepared_operations_v7',index,'payload'],record['digest']['payload_digest'])
            ancestry={'scope':scope,'implementation_revision':revision,'owner_bindings':owners,
                'complete':True,'continuous':True,'owner_id':owner_id,'native_anchor':anchor,
                'commit_reference':commit,'prior_prefix':frames[start][2],'frames':chain}
            witnesses.append({'record_blob':blobs.json('DfsPreparedOperationRecord',record),
                'canonical_payload_blob':blobs.put('DfsPreparedOperationPayloadCanonicalV7',canonical),
                'commit_reference_blob':blobs.json('DfsPreparedOperationCommitReference',commit),
                'prefix_ancestry_blob':blobs.json('DfsNativeBaselinePrefixAncestryV1',ancestry)})
            excluded.add(opid)
        if held['tables'].get('prepared_operation_evictions'):
            raise Unavailable('evicted baseline lacks full immutable body evidence')
        result[owner_id]={'held_snapshot_id':cut['snapshot_id'],
            'held_tables_blob':blobs.json('DfsMetadataTableSnapshotTables',held['tables']),
            'native_prefix':anchor,'terminal_records':witnesses,
            'active_dependencies':{name:held['tables'].get(name,[]) for name in ('intents','namespace_requests','namespace_jobs')}}
    return result,sorted(excluded),used


def derive_remote_content_return(header, raw, blobs, owners, prepare, intent, row):
    """Match actual receiver persistence, authenticated return and coordinator.

    Ordinals order only one owner. Cross-owner causality is the exact normal
    StageBlob request/reply tuple, never wall-clock order or a holder hint.
    """
    content, node = row['receipt'], row['replica_node_id']
    source = prepare['record']['owner_label']
    source_owner = owners.get(source)
    if source_owner is None or source_owner['node_id'] != intent['coordinator_node_id']:
        raise Rejected('native prepare is not owned by its actual content coordinator')
    expected = dict(intent_id=intent['intent_id'], file_system_id=content['file_system_id'],
                    object_id=content['object_id'], version=content['committed_version'],
                    replica_node_id=node, state='durable')
    matches=[]
    for coordinator_key, item in raw.items():
        if item['kind'] != 'content-receipt' or coordinator_key[0] != source or item['ordinal'] >= prepare['ordinal']:
            continue
        coordinator=item['record']['facts']
        if coordinator.get('origin') != 'coordinator-durable' or coordinator['receipt'] != expected:
            continue
        actual_intent=coordinator['intent']
        immutable=('intent_id','file_system_id','object_id','base_version','target_version','operation_id',
                   'operation_order_token','owner_epoch','policy','touch_parent_on_create','create_predecessor_object_id')
        if (any(actual_intent.get(k) != intent.get(k) for k in immutable)
                or actual_intent.get('publication', {'kind':'current'}) != intent['publication']
                or actual_intent['requesting_node_id'] != intent['coordinator_node_id']
                or actual_intent['desired_replica_nodes'] != intent['desired_replica_node_ids']):
            raise Rejected('remote coordinator return changes the immutable placement/intent')
        for returned_key, returned_item in raw.items():
            if returned_item['kind'] != 'boundary' or returned_key[0] != source or returned_item['ordinal'] >= item['ordinal']:
                continue
            returned=returned_item['record']['facts']
            if returned.get('phase') != 'rpc-attempt-returned' or returned.get('response') is None:
                continue
            started=[(key,value) for key,value in raw.items() if value['kind']=='boundary' and key[0]==source
                     and value['ordinal']<returned_item['ordinal']
                     and value['record']['facts'].get('phase')=='rpc-attempt-started'
                     and value['record']['facts']['attempt_id']==returned['attempt_id']]
            if len(started)!=1:
                raise Rejected('actual returned content RPC lacks its unique source attempt')
            started_key,started_item=started[0]; call=started_item['record']['facts']; command=call['command']
            routed=command['routed_message']; stage=routed['message'].get('stage-blob')
            if stage is None or call['target_node_id'] != node:
                continue
            if (routed['source_node_id']!=source_owner['node_id'] or routed['target_node_id']!=node
                    or returned['response']['request_id']!=command['request_id']):
                raise Rejected('actual content RPC request/response changes authenticated routing identity')
            reply=dict(request_id=stage['request_id'],intent_id=intent['intent_id'],
                file_system_id=content['file_system_id'],object_id=content['object_id'],
                version=content['committed_version'],replica_node_id=node,
                result={'durable':{'content_hash':row['content_hash']}})
            if (any(stage.get(k)!=reply[k] for k in ('intent_id','file_system_id','object_id','version'))
                    or stage.get('content_hash')!=row['content_hash']):
                continue
            outputs=[message for message in returned['response']['outbound_messages']
                     if message['source_node_id']==node and message['target_node_id']==source_owner['node_id']
                     and message['message']=={'stage-blob-response':reply}]
            if len(outputs)!=1:
                continue
            for manifest_key, manifest_item in raw.items():
                if manifest_item['kind']!='content-receipt':
                    continue
                receiver=owners.get(manifest_key[0]); fact=manifest_item['record']['facts']
                if not receiver or receiver['node_id']!=node or fact.get('origin')!='local-persisted':
                    continue
                manifest=fact['manifest']
                if (manifest_item['record']['actual_mounted_node_id']!=node
                        or fact['file_system_id']!=content['file_system_id']
                        or manifest['object_id']!=content['object_id'] or manifest['version']!=content['committed_version']
                        or manifest['content_hash']!=row['content_hash']):
                    continue
                if fact['persistence'] not in ('blob-only','manifest-bound'):
                    raise Rejected('unknown actual receiver persistence result')
                command_identity={field:stage[field] for field in
                    ('request_id','intent_id','file_system_id','object_id','version','content_hash')}
                served=[(key,value) for key,value in raw.items() if value['kind']=='boundary'
                        and key[0]==manifest_key[0] and value['ordinal']>manifest_item['ordinal']
                        and value['record']['facts'].get('phase')=='content-stage-durable-return'
                        and value['record']['facts'].get('authenticated_requester_node_id')==source_owner['node_id']
                        and value['record']['facts'].get('command')==command_identity
                        and value['record']['facts'].get('response')==reply]
                if not served:
                    # Matching immutable bytes do not place a detached receiver
                    # return before this source reply. Require SAME served call.
                    continue
                if len(served)!=1:
                    raise Rejected('receiver content return has ambiguous exact request provenance')
                served_key,served_item=served[0]
                matches.append((manifest_key,receiver,manifest,fact['persistence'],started_key,returned_key,coordinator_key,command,returned['response'],reply,coordinator,served_key,served_item))
    if not matches:
        return None,set()
    identities={(match[1]['owner_id'],match[2]['content_hash'],str(match[2]['size_bytes'])) for match in matches}
    if len(identities)!=1:
        raise Rejected('actual receiver persistence returns disagree on owner/hash/length')
    manifest_key,receiver,manifest,persistence,started_key,returned_key,coordinator_key,command,response,reply,coordinator,served_key,served_item=matches[-1]
    common=dict(scope=header['scope'],implementation_revision=header['envelope_binding']['implementation_revision'],
                owner_bindings=header['owners'],complete=True,continuous=True)
    ref=lambda key:dict(actual_mount_label=key[0],owner_ordinal=str(key[1]))
    rpc=blobs.json('DfsNativeAuthenticatedRpcReturnV1',dict(common,owner_id=source_owner['owner_id'],
        authenticated_target_node_id=node,command=command,response=response,
        actual_raw_references=[ref(started_key),ref(returned_key)]))
    evidence=dict(common,origin='authenticated-durable-response',data_owner_id=receiver['owner_id'],
        coordinator_receipt_record=row,authenticated_recipient_node_id=node,native_request_id=reply['request_id'],
        verified_content_identity=dict(file_system_id=content['file_system_id'],object_id=content['object_id'],
            content_version=content['committed_version'],content_hash=manifest['content_hash'],size_bytes=manifest['size_bytes']),
        actual_manifest_blob=blobs.json('ContentManifest',manifest),actual_persistence_kind=persistence,
        actual_distributed_receipt_blob=blobs.json('DistributedReplicaReceipt',coordinator['receipt']),
        actual_distributed_intent_blob=blobs.json('DistributedWriteIntent',coordinator['intent']),
        actual_rpc_observation_blob=rpc,durable_response_blob=blobs.json('DfsStageBlobResponse',reply),
        actual_receiver_response_blob=blobs.json('DfsNativeContentServedReturnV1',dict(common,
            owner_id=receiver['owner_id'],**served_item['record']['facts'])),
        actual_raw_references=[ref(manifest_key),ref(served_key),ref(started_key),ref(returned_key),ref(coordinator_key)])
    return evidence,{manifest_key,served_key,started_key,returned_key,coordinator_key}


def derive_initial_local_receipts(header, raw, blobs):
    """Correlate actual persistence/coordinator returns with exact sealed rows.

    Event IDs/times remain the actual payload row. No content location, payload
    receipt alone, or observation timestamp supplies durability evidence.
    """
    contexts=copy.deepcopy(header['snapshot_contexts'])
    owners={owner['component_incarnation']:owner for owner in header['owners']}
    used=set()
    for context in contexts.values():
        reference=context['native_operational_facts_blob']
        fact_blob=blobs.values[reference]
        if fact_blob['encoding']!='json-utf8':
            raise Rejected('native operational facts require exact native JSON')
        facts=native_json(fact_blob['data'].encode(),'native operational facts')
        source_cut=facts.get('actual_memory_cut_source')
        if source_cut is not None:
            cfg=native_json(blobs.values[context['active_config']['native_config_blob']]['data'].encode(),'actual primary config')
            primary_peer=facts['peer_certified_cuts'][cfg['primary_node_id']]
            primary_context=contexts[primary_peer['held_snapshot_id']]
            primary_facts=native_json(blobs.values[primary_context['native_operational_facts_blob']]['data'].encode(),'actual primary facts')
            source_cut=primary_facts['actual_memory_cut_source']
        changed=False
        for opid, observation in facts.get('operation_observations',{}).items():
            payload_ref=observation.get('initial_payload_blob')
            if not payload_ref or observation.get('initial_content_receipt_observation_blobs'):
                continue
            body=blobs.values[payload_ref]
            if body['native_type']!='DfsPreparedOperationPayloadCanonicalV7' or body['encoding']!='json-utf8':
                raise Rejected('initial body is not actual canonical V7 bytes')
            payload=native_json(body['data'].encode(),'selected immutable body')
            if payload['operation_id']!=opid:raise Rejected('initial body names another operation')
            prepares=[item for item in raw.values() if item['kind']=='kernel'
                      and item['record']['phase']=='prepare-durable'
                      and item['record']['operation']['operation_id']==opid]
            if len(prepares)!=1:raise Unavailable('initial content evidence needs the actual first native prepare')
            first_prepare=prepares[0]
            if source_cut is not None:
                if source_cut['owner_id']!=owners[first_prepare['record']['owner_label']]['owner_id']:
                    # Later primaries obtain receipts from their actual held
                    # replicated rows; another owner's ordinal grants no cut.
                    continue
                cutoff=CHECK.decimal_u64(str(source_cut['owner_ordinal']),'actual content observation cut')
            elif len(header['owners'])==1 and len(header['finite_dictionary']['operation_aliases'])==1:
                # Preserved original singleton adapter: its one content body
                # was already sealed before the selected first prepare.
                cutoff=first_prepare['ordinal']-1
            else:
                raise Unavailable('pre-prepare receipts need exact actual memory-cut attribution')
            SUBTREE.canonical_prepared_payload(body['data'].encode(),[],first_prepare['record']['operation']['payload_digest'])
            intent=payload.get('prepared_intent')
            if not intent:continue
            derived=[]
            derived_rows=[]
            for row in payload.get('content_receipts',[]):
                content=row['receipt'];node=row['replica_node_id']
                matches=[]
                for key, item in raw.items():
                    if item['kind']!='content-receipt':continue
                    event=item['record'];owner=owners.get(event['owner_label'])
                    # This local path is same-owner ordered. Remote receipt
                    # correlation requires its authenticated response evidence.
                    if (event['owner_label']!=first_prepare['record']['owner_label']
                            or item['ordinal']>=first_prepare['ordinal'] or item['ordinal']>cutoff):
                        continue
                    if not owner or owner['node_id']!=node or event['actual_mounted_node_id']!=node:
                        continue
                    if event['shard_id']!=header['scope']['shard_id']:
                        raise Rejected('native content receipt crosses selected shard')
                    observed=event['facts']
                    if observed['origin']!='coordinator-durable':continue
                    receipt=observed['receipt'];native_intent=observed['intent']
                    expected={'intent_id':intent['intent_id'],'file_system_id':content['file_system_id'],
                              'object_id':content['object_id'],'version':content['committed_version'],
                              'replica_node_id':node,'state':'durable'}
                    if receipt!=expected:continue
                    fields={'requesting_node_id':'coordinator_node_id','desired_replica_nodes':'desired_replica_node_ids'}
                    immutable=('intent_id','file_system_id','object_id','base_version','target_version',
                               'operation_id','operation_order_token','owner_epoch','policy',
                               'touch_parent_on_create','create_predecessor_object_id')
                    if any(native_intent.get(field)!=intent.get(field) for field in immutable) or any(native_intent.get(a)!=intent.get(b) for a,b in fields.items()):
                        raise Rejected('coordinator receipt belongs to a different immutable intent/plan')
                    if native_intent.get('publication',{'kind':'current'})!=intent['publication']:
                        raise Rejected('coordinator publication differs from the sealed intent')
                    manifests=[]
                    for manifest_key, candidate in raw.items():
                        if candidate['kind']!='content-receipt' or manifest_key[0]!=key[0] or candidate['ordinal']>=item['ordinal']:
                            continue
                        actual=candidate['record'];returned=actual['facts']
                        if returned['origin']!='local-persisted':continue
                        manifest=returned['manifest']
                        if (actual['shard_id']==event['shard_id'] and actual['actual_mounted_node_id']==node
                                and returned['file_system_id']==content['file_system_id']
                                and manifest['object_id']==content['object_id'] and manifest['version']==content['committed_version']
                                and manifest['content_hash']==row['content_hash']):
                            if returned['persistence'] not in ('blob-only','manifest-bound'):
                                raise Rejected('unknown actual content persistence result')
                            manifests.append((manifest_key,manifest,returned['persistence']))
                    if not manifests:continue
                    identities={(manifest['content_hash'],CHECK.decimal_u64(str(manifest['size_bytes']),'manifest size')) for _,manifest,_ in manifests}
                    if len(identities)!=1:raise Rejected('actual content persistence returns disagree')
                    matches.append((key,owner,manifests))
                if not matches:
                    remote, remote_sources=derive_remote_content_return(header,raw,blobs,owners,first_prepare,intent,row)
                    if remote is not None:
                        coordinator_ref=remote['actual_raw_references'][-1]
                        if CHECK.decimal_u64(coordinator_ref['owner_ordinal'])<=cutoff:
                            derived.append(blobs.json('DfsNativeContentReceiptObservationV1',remote))
                            derived_rows.append(row)
                            used.update(remote_sources)
                    continue
                if len({owner['owner_id'] for _,owner,_ in matches})!=1:
                    raise Rejected('local receipt borrowed another data owner')
                coordinator_key,owner,manifests=matches[-1]
                manifest_key,manifest,persistence=manifests[-1]
                receipt_fact={'scope':header['scope'],
                    'implementation_revision':header['envelope_binding']['implementation_revision'],
                    'owner_bindings':header['owners'],'complete':True,'continuous':True,
                    'origin':'local-verified-durable','data_owner_id':owner['owner_id'],
                    'coordinator_receipt_record':row,
                    'verified_content_identity':{'file_system_id':content['file_system_id'],
                        'object_id':content['object_id'],'content_version':content['committed_version'],
                        'content_hash':manifest['content_hash'],'size_bytes':manifest['size_bytes']},
                    'actual_manifest_blob':blobs.json('ContentManifest',manifest),
                    'actual_persistence_kind':persistence,
                    'actual_distributed_receipt_blob':blobs.json('DistributedReplicaReceipt',raw[coordinator_key]['record']['facts']['receipt']),
                    'actual_distributed_intent_blob':blobs.json('DistributedWriteIntent',raw[coordinator_key]['record']['facts']['intent']),
                    'actual_raw_references':[{'actual_mount_label':key[0],'owner_ordinal':str(key[1])}
                        for key in sorted({key for key,_,_ in manifests}|{key for key,_,_ in matches})]}

                derived.append(blobs.json('DfsNativeContentReceiptObservationV1',receipt_fact))
                derived_rows.append(row)
                used.update(key for key,_,_ in manifests)
                used.update(key for key,_,_ in matches)
            if derived:
                observation['initial_content_receipt_observation_blobs']=derived
                actual_rows=[native_json(blobs.values[ref]['data'].encode(),'actual durable receipt')
                    for ref in observation['durable_content_receipt_blobs']]
                for row in derived_rows:
                    if row not in actual_rows:actual_rows.append(row)
                observation['durable_content_receipt_blobs']=[blobs.json('DfsReplicaReceiptRecord',row) for row in actual_rows]
                observation['durable_content_receipt_node_ids']=[row['replica_node_id'] for row in actual_rows]
                changed=True
        if changed:
            context['native_operational_facts_blob']=blobs.json(fact_blob['native_type'],facts)
    return contexts,used


def initial_native_verifiers(scope,owners,raw):
    """Share the SAME parser and authenticated edge matcher for initial DATA."""
    import types
    spec=importlib.util.spec_from_file_location('actual_bootstrap',HERE/'shard-native-bootstrap.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.configure(types.SimpleNamespace(Rejected=Rejected,Unavailable=Unavailable,CHECK=CHECK,
        SUBTREE=SUBTREE,native_prefix=native_prefix,native_json=native_json,byte_vector=byte_vector,
        validate_request_value_decode=validate_request_value_decode,
        journal_rpc_request_matches=journal_rpc_request_matches,
        journal_rpc_return_phase=journal_rpc_return_phase,
        verify_journal_rpc_return=verify_journal_rpc_return))
    context_spec=importlib.util.spec_from_file_location('bootstrap_ack_context',HERE/'shard-native-context.py')
    context=importlib.util.module_from_spec(context_spec);context_spec.loader.exec_module(context)
    actual_index=types.SimpleNamespace(scope=scope,owners={row['owner_id']:row for row in owners},
        events={(owner,n):dict(collection='boundary_packets',record=item['record'])
                for (owner,n),item in raw.items() if item['kind']=='boundary'})
    def verify_edge(edge):
        try:return context.NativeContexts.verified_edge(actual_index,edge)
        except context.Rejected as error:raise Rejected(str(error)) from error
        except context.Unavailable as error:raise Unavailable(str(error)) from error
    return module,verify_edge


def verify_initial_completed_handoff(scope,owners,headers,raw,initial_prefixes,initial_ordinals,correlations):
    """Re-derive historical handoff DATA from actual raw/header evidence."""
    module,verify_edge=initial_native_verifiers(scope,owners,raw)
    return module.verify_completed_handoff(scope,owners,headers,raw,initial_prefixes,initial_ordinals,
        correlations,verify_edge)



def validate_request_value_decode(header, record, observation):
    """Verify one normal Rust decode against its SAME actual request field.

    This uses the export's ordinary current decoder result. It creates no
    persisted Entry/revision and serializes no replacement SOBJ value.
    """
    kinds={'metadata-shards':'DfsMetadataShardConfig',
           'metadata-membership-handoffs':'DfsMetadataShardMembershipHandoff',
           'metadata-membership-handoff-proofs':'DfsMetadataShardMembershipHandoffProof',
           'metadata-operation-floors':'DfsMetadataShardOperationFloor',
           'metadata-head-watermarks':'DfsMetadataShardHeadWatermark'}
    if observation not in header.get('actual_request_value_decodes',[]):
        raise Rejected('request value decode is absent from its actual native header')
    if CHECK.decimal_u64(observation['source_owner_ordinal'])!=CHECK.decimal_u64(record['owner_ordinal']):
        raise Rejected('request value decode borrows another actual owner ordinal')
    section=observation['request_section'];index=observation['field_index']
    if section not in ('compare','success') or type(index)is not int or index<0:
        raise Rejected('request value decode field identity is invalid')
    fields=record['commit']['request']['txn']['compares' if section=='compare' else 'success']
    if index>=len(fields):raise Rejected('request value decode field is outside actual transaction')
    tag='ValueEquals' if section=='compare' else 'Put';field=fields[index]
    if set(field)!={tag}:raise Rejected('request value decode is not an actual encoded value field')
    actual=field[tag]
    if observation['key']!=actual['key'] or byte_vector(observation['exact_value'],'decoded request bytes')!=byte_vector(actual['value'],'actual request bytes'):
        raise Rejected('request value decode changed actual field key/bytes')
    parts=actual['key'].split('/')
    if len(parts)!=6 or parts[:3]!=['','dfs','filesystems'] or not parts[3] or not parts[5] or kinds.get(parts[4])!=observation['native_type']:
        raise Rejected('request value decode current type does not match its exact key grammar')
    return observation['decoded_value']


def validate_initial_sources(header, raw, raw_headers, blobs):
    """Bind certified initial evidence to complete actual raw observations."""
    used=set();baseline={}
    for value in blobs.values.values():
        if value['native_type']!='DfsNativeKvValueDecodeObservationV1':continue
        evidence=native_json(value['data'].encode(),'actual request value decode')
        source=evidence.get('actual_request_source')
        if source is None:continue  # Older independently supplied decoder family.
        key=(source['owner_id'],CHECK.decimal_u64(source['owner_ordinal']))
        item=raw.get(key)
        headers=[h for h in raw_headers if h['actual_mount_label']==key[0]]
        if item is None or item['kind']!='control-commit' or len(headers)!=1:
            raise Rejected('request value decode lacks its actual native control call/header')
        observation=evidence['actual_request_value_decode']
        decoded=validate_request_value_decode(headers[0],item['record'],observation)
        if (evidence['decoded_native_type']!=observation['native_type']
                or blobs.values[evidence['decoded_value_blob']]['native_type']!=observation['native_type']
                or native_json(blobs.values[evidence['decoded_value_blob']]['data'].encode(),'actual typed value')!=decoded
                or blobs.values[evidence['encoded_value_blob']]['encoding']!='exact-frame-base64'
                or base64.b64decode(blobs.values[evidence['encoded_value_blob']]['data'],validate=True)!=byte_vector(observation['exact_value'],'actual typed request bytes')
                or hashlib.sha256(byte_vector(observation['exact_value'],'actual typed request bytes')).hexdigest()!=evidence['encoded_value_sha256']):
            raise Rejected('request value certificate differs from actual decoded source')
    initial=native_json(blobs.values[header['initial_cut']['baseline_validation_blob']]['data'].encode(),'initial certificate')
    for owner,cut in initial['owner_cuts'].items():
        for witness in cut['terminal_records']:
            record=native_json(blobs.values[witness['record_blob']]['data'].encode(),'actual baseline')
            if record['status']!='committed':raise Unavailable('initial effect is not terminal')
            baseline[record['digest']['operation_id']]=record
    source_cuts={}
    for owner,cut in header['owner_cuts'].items():
        source=header['snapshot_blueprints'][cut['held_snapshot_id']]['native_source']
        key=(source['owner_id'],CHECK.decimal_u64(source['owner_ordinal']))
        if key[0]!=owner or key not in raw or source['side'] not in ('before','after'):
            raise Rejected('baseline held source differs from actual owner packet')
        encoded=raw[key]['record'].get(source['side']+'_held_json')
        if encoded is None:raise Unavailable('baseline held source bytes absent')
        horizon = key[1] + (source['side'] == 'after')
        modeled_owners = {event['owner_id'] for event in header['native_timeline']}
        if owner not in modeled_owners:
            selected = set(header['finite_dictionary']['operation_aliases'])
            payloads = {}
            for value in blobs.values.values():
                if value['native_type'] == 'DfsPreparedOperationPayloadCanonicalV7':
                    payload = native_json(value['data'].encode(), 'selected canonical baseline body')
                    if payload['operation_id'] in selected:
                        previous = payloads.setdefault(payload['operation_id'], payload)
                        if previous != payload:
                            raise Rejected('baseline exclusion changes a selected canonical body')
            if set(payloads) != selected:
                raise Unavailable('baseline-only peer lacks every actual selected body for exclusion')
            prior = [item for (actor, ordinal), item in raw.items()
                     if actor == owner and ordinal < horizon]
            headers = [item for item in raw_headers if item['actual_mount_label'] == owner]
            if len(headers) != 1:
                raise Unavailable('baseline-only peer lacks its actual normal decoder header')
            received = [item['decoded_entry'] for item in headers[0].get('actual_received_frame_decodes', [])
                        if CHECK.decimal_u64(item['source_owner_ordinal']) < horizon]
            if selected_effect_in_bootstrap_cut(native_json(byte_vector(encoded, 'bootstrap held cut'),
                                                           'bootstrap held cut'), payloads, prior, received):
                raise Rejected('baseline-only peer hides selected effects or compacted outcomes')
        source_cuts[owner]=dict(held_json=encoded,snapshot_id=cut['held_snapshot_id'],owner_ordinal=horizon)
    derived,excluded,_=derive_terminal_baseline(header['owners'],header['scope'],header['envelope_binding']['implementation_revision'],
        {row['actual_mount_label']:row for row in raw_headers},raw,source_cuts,
        header['finite_dictionary']['operation_aliases'],blobs)
    if excluded!=header['unmodeled_operations']:raise Rejected('initial terminal operation inventory differs from original held sources')
    for owner,actual in derived.items():
        claimed=initial['owner_cuts'][owner]
        if (native_prefix(actual['native_prefix'])!=native_prefix(claimed['native_prefix'])
                or any(actual[field]!=claimed[field] for field in ('held_snapshot_id','held_tables_blob','terminal_records','active_dependencies'))):
            raise Rejected('initial terminal baseline differs from independently retained original frame/body evidence')
    actual_decodes={}
    for native in raw_headers:
        label=native['actual_mount_label']
        for item in native.get('actual_control_decodes',[]):
            actual_decodes.setdefault((label,int(item['source_owner_ordinal'])),[]).append(item)
    # Every copied cache cut must be the exact captured source, not a detached
    # self-declared certificate. Typed decoding is paired with the raw header.
    control_keys=set()
    initial_control_refs=set()
    for name in header['initial_cut']['baseline_snapshot_ids']:
        context=header['snapshot_contexts'][name]
        facts=native_json(blobs.values[context['native_operational_facts_blob']]['data'].encode(),'initial operational facts')
        if facts.get('native_control_cut_blob'):initial_control_refs.add(facts['native_control_cut_blob'])
    for reference,value in list(blobs.values.items()):
        if value['native_type'] == 'DfsNativeRetainedConfigurationCutEvidenceV1':
            fact = native_json(value['data'].encode(), 'retained configuration evidence')
            cut = fact['actual_configuration_cut']; key = (cut['owner_label'], CHECK.decimal_u64(cut['owner_ordinal']))
            if key not in raw or raw[key]['kind'] != 'configuration-cut' or raw[key]['record'] != cut:
                raise Rejected('retained certificate differs from actual raw owner/ordinal/cut')
            headers = [item for item in raw_headers if item['actual_mount_label'] == key[0]]
            if len(headers) != 1:
                raise Unavailable('retained certificate has no exact actual source header')
            qualified = validate_retained_configuration_cut(cut, headers[0], header['scope'])
            entries = {row['key']: row for row in qualified['entries']}
            if (fact['selected_control_keys'] != qualified['selected_keys']
                    or fact['selected_entries'] != [entries.get(k) for k in qualified['selected_keys']]
                    or fact['actual_typed_decodes'] != qualified['typed_decodes']):
                raise Rejected('retained certificate changes its exact selected keys/bytes/typed rows')
            if reference in initial_control_refs: control_keys.add(key)
            continue
        if value['native_type']!='DfsNativeCacheControlCutEvidenceV1':continue
        fact=native_json(value['data'].encode(),'native cache evidence');cut=fact['actual_cache_cut']
        key=(cut['owner_label'],CHECK.decimal_u64(cut['owner_ordinal']))
        if key not in raw or raw[key]['kind']!='metadata-cache-cut' or raw[key]['record']!=cut:
            raise Rejected('cache certificate differs from actual raw owner/ordinal/cut')
        if not raw_headers:raise Unavailable('actual typed decoder source header is required')
        scope=header['scope'];segment=scope['shard_id'].strip('/').replace('/','_')
        expected_keys=[f"/dfs/filesystems/{scope['file_system_id']}/{family}/{segment}" for family in
                       ('metadata-shards','metadata-membership-handoffs')]
        if fact['selected_control_keys']!=expected_keys:
            raise Rejected('initial control certificate changes the exact selected scope/key set')
        expected_decodes=[item for item in actual_decodes.get(key,[]) if item['exact_entry']['key'] in expected_keys]
        if fact['actual_typed_decodes']!=expected_decodes:
            raise Rejected('typed cache decode certificate differs from actual native header')
        validate_metadata_cache_cut(cut)
        if reference in initial_control_refs:control_keys.add(key)
    completed=header.get('initial_completed_handoff')
    header_map={row['actual_mount_label']:row for row in raw_headers}
    anchors={owner:cut['native_prefix'] for owner,cut in header['owner_cuts'].items()}
    ordinals={owner:source_cuts[owner]['owner_ordinal'] for owner in header['owner_cuts']}
    correlations=header.get('initial_actual_request_response_correlations',[])
    if completed is not None:
        if not correlations:raise Unavailable('completed bootstrap exact ACK correlations are absent')
        actual=verify_initial_completed_handoff(header['scope'],header['owners'],header_map,raw,anchors,ordinals,correlations)
        if completed!=actual:raise Rejected('initial completed handoff certificate differs from independent actual evidence')
    module,verify_edge=initial_native_verifiers(header['scope'],header['owners'],raw)
    boundary_witnesses=module.verify_initial_boundaries(header['scope'],header['owners'],header_map,raw,
        anchors,ordinals,correlations,baseline,completed,verify_edge)
    boundary_sources={(item['owner_id'],CHECK.decimal_u64(item['owner_ordinal'])) for item in boundary_witnesses}
    first_by_owner={}
    for event in header['native_timeline']:
        for ref in event['raw_references']:
            label=ref['actual_mount_label'];ordinal=CHECK.decimal_u64(ref['owner_ordinal'])
            first_by_owner[label]=min(first_by_owner.get(label,ordinal),ordinal)
    for ref in header.get('initial_source_observations',[]):
        key=(ref['actual_mount_label'],CHECK.decimal_u64(ref['owner_ordinal']))
        if (key not in raw or key in used
                or key[1]>=first_by_owner.get(key[0],max(k[1] for k in raw if k[0]==key[0])+1)
                or raw[key]['kind']!=ref['native_kind']):
            raise Rejected('initial native source coverage overlaps or changes actual observations')
        item=raw[key];record=item['record'];kind=item['kind']
        if kind=='metadata-cache-cut':
            validate_metadata_cache_cut(record)
            validate_metadata_cache_consumer_scope(record, scope['file_system_id'],
                scope['shard_id'], header['finite_dictionary']['operation_aliases'])
        elif kind=='kernel':
            opid=record['operation']['operation_id'];stored=baseline.get(opid)
            if not stored or stored['digest']!=record['operation']:
                raise Unavailable('initial kernel effect lacks exact retained terminal baseline')
            if record['exact_entry']:
                entry=record['exact_entry'];frame=byte_vector(record['exact_frame'],'initial frame')
                before=native_json(byte_vector(record['before_held_json'],'initial prior'),'initial prior')
                after=native_json(byte_vector(record['after_held_json'],'initial next'),'initial next')
                if native_prefix(SUBTREE.PROJECTOR.extend_native_prefix(native_prefix(before['certified_prefix']),entry['sequence'],frame))!=native_prefix(after['certified_prefix']):
                    raise Rejected('initial physical kernel effect does not match actual prefix')
        elif kind=='guard':
            stored=baseline.get(record['digest']['operation_id'])
            if not stored or record['digest']!=stored['digest']:raise Rejected('initial guard belongs to another terminal baseline')
            if record['phase']=='admitted' and (not record['matches_actual_mounted_owner'] or record['held'] is None):
                raise Unavailable('initial baseline guard lacks actual mounted custody')
        elif kind == 'authoritative-read':
            if record.get('guard_context_id') is not None:
                raise Rejected('retired per-operation authoritative guard read is incompatible')
            if key not in boundary_sources:
                raise Unavailable('initial global authoritative read needs exact configuration/handoff source evidence')
        elif kind == 'configuration-cut':
            validate_retained_configuration_cut(record, header_map[key[0]], header['scope'])
        elif kind=='content-receipt':
            references=[]
            for value in blobs.values.values():
                if value['native_type']=='DfsNativeContentReceiptObservationV1':
                    references+=native_json(value['data'].encode(),'actual receipt')['actual_raw_references']
            if not any((r['actual_mount_label'],int(r['owner_ordinal']))==key for r in references):
                raise Unavailable('initial content return lacks verified exact receipt provenance')
        elif kind=='boundary':
            if record['facts']['phase']=='content-stage-durable-return':
                references=[]
                for value in blobs.values.values():
                    if value['native_type']=='DfsNativeContentReceiptObservationV1':
                        references+=native_json(value['data'].encode(),'actual content return')['actual_raw_references']
                if not any((r['actual_mount_label'],int(r['owner_ordinal']))==key for r in references):
                    raise Unavailable('initial content-stage return lacks actual receipt/call provenance')
            elif key not in boundary_sources:
                raise Unavailable('initial native receiver/lifecycle event lacks independent exact evidence')
        elif kind=='control-commit':
            if key not in boundary_sources:validate_initial_bootstrap_commit(header,raw,raw_headers,blobs,key,record,baseline)
        elif kind=='original-control-commit':
            module.verify_original_observation(header['scope'],header['owners'],header_map,raw,key)
        else:raise Unavailable('unmodeled native initial observation cannot be discarded')
        used.add(key)
    if not control_keys<=used:raise Rejected('certified initial cache source is outside raw coverage')
    return used


def validate_initial_bootstrap_commit(header,raw,raw_headers,blobs,key,record,baseline):
    """Account an actual native genesis/configuration CAS before finite writes.

    This is part of the certified POST-bootstrap initial cut, not a model
    stutter: its first hidden body, exact native config publication and terminal
    commit are all retained and checked as initial evidence.
    """
    scope=header['scope'];native=record['commit'];request=native['request'];result=native['result']
    authority=result['authority'];response=result.get('txn_response');basis=authority['basis']
    if (request['request_id']!=result['request_id'] or response is None or response.get('succeeded') is not True
            or SUBTREE.PROJECTOR.u64(authority['revision'],'bootstrap authority revision')!=SUBTREE.PROJECTOR.u64(response['revision'],'bootstrap response revision')
            or SUBTREE.PROJECTOR.u64(authority['term'],'bootstrap term')==0
            or SUBTREE.PROJECTOR.u64(authority['commit_index'],'bootstrap commit index')==0
            or not isinstance(basis,dict) or set(basis)!={'current-term-quorum'}):
        raise Rejected('initial native configuration lacks its actual fresh committed-majority receipt')
    quorum=basis['current-term-quorum']
    required=SUBTREE.PROJECTOR.u64(quorum['required_quorum'],'bootstrap required quorum')
    acknowledged=SUBTREE.PROJECTOR.u64(quorum['voter_acks'],'bootstrap ACK count')
    if (not 1<=required<=acknowledged
            or result.get('submitted_command_digest') is None
            or SUBTREE.PROJECTOR.native_digest_hex(result['submitted_command_digest'])!=SUBTREE.PROJECTOR.native_digest_hex(result['command_digest'])):
        raise Rejected('initial native configuration changed its actual digest/quorum')
    segment=scope['shard_id'].strip('/').replace('/','_');prefix='/dfs/filesystems/'+scope['file_system_id']+'/'
    keys=[prefix+family+'/'+segment for family in
          ('metadata-shards','metadata-membership-handoffs','metadata-operation-floors','metadata-head-watermarks')]
    txn=request['txn'];success=txn['success']
    if (len(success)!=1 or set(success[0])!={'Put'} or txn.get('failure')
            or success[0]['Put']['key']!=keys[0] or success[0]['Put'].get('lease_id') is not None):
        raise Unavailable('initial native control effect is not the exact genesis configuration publication')
    effect=success[0]['Put'];missing=[compare['Missing']['key'] for compare in txn['compares'] if set(compare)=={'Missing'}]
    if sorted(missing)!=sorted(keys):
        raise Rejected('genesis configuration lacks exact absent shard/handoff/floor/head guards')
    owner=next((owner for owner in header['owners'] if owner['component_incarnation']==key[0]),None)
    if owner is None:raise Rejected('initial native configuration has no actual owner')
    extra=[compare for compare in txn['compares'] if set(compare)!={'Missing'}]
    expected_extra={'/cluster/members/'+owner['node_id'],'/cluster/policy/capabilities/'+owner['node_id']}
    if (len(extra)!=2 or any(set(compare)!={'ValueEquals'} for compare in extra)
            or {compare['ValueEquals']['key'] for compare in extra}!=expected_extra
            or any(not compare['ValueEquals']['value'] for compare in extra)):
        raise Rejected('genesis configuration omits its actual member/capability policy compares')
    headers=[native for native in raw_headers if native['actual_mount_label']==key[0]]
    if len(headers)!=1:raise Unavailable('initial native owner header unavailable')
    mounted=headers[0]['normal_mounted_identity']
    voters={row['node_id'] for row in mounted['configured_members'] if row['role']=='voter' and row['state']=='voter'}
    if required!=len(voters)//2+1 or not acknowledged<=len(voters):
        raise Rejected('bootstrap native quorum count differs from actual configured voter membership')
    decodes=[row for row in headers[0]['actual_control_decodes']
             if row['native_type']=='DfsMetadataShardConfig' and row['exact_entry']['key']==keys[0]
             and row['exact_entry']['value']==effect['value']]
    decoded=[row['decoded_value'] for row in decodes]
    if not decoded:raise Unavailable('original genesis config bytes lack their ordinary native decoder observation')
    if any(cfg!=decoded[0] for cfg in decoded):raise Rejected('original config bytes have inconsistent typed native decodes')
    cfg=decoded[0]
    if (any(cfg[field]!=scope[field] for field in ('file_system_id','shard_id'))
            or cfg['primary_node_id']!=owner['node_id'] or cfg.get('replica_node_ids')
            or SUBTREE.PROJECTOR.u64(cfg['epoch'],'bootstrap native epoch')!=1):
        raise Rejected('initial native genesis is not the actual owned singleton epoch-one configuration')
    candidates=[]
    for opid,stored in baseline.items():
        digest=stored['digest'];body=stored['payload']
        if (digest['file_system_id']!=scope['file_system_id'] or digest['shard_id']!=scope['shard_id']
                or digest['owner_epoch']!=cfg['epoch'] or digest['prepare_sequence']!=1
                or stored['status']!='committed' or body.get('prepared_intent') is not None):continue
        effects=body.get('metadata_effects',[])
        if not any(effect.get('type')=='put-file-system' and effect['file_system']['file_system_id']==scope['file_system_id'] for effect in effects):continue
        prepares=[item for (label,_),item in raw.items() if label==key[0] and item['kind']=='kernel'
                  and item['record']['phase']=='prepare-durable' and item['record']['operation']==digest]
        commits=[item for (label,_),item in raw.items() if label==key[0] and item['kind']=='kernel'
                 and item['record']['phase']=='commit-materialized' and item['record']['operation']==digest]
        if len(prepares)==1 and len(commits)==1 and prepares[0]['ordinal']<key[1]<commits[0]['ordinal']:
            candidates.append(stored)
    if len(candidates)!=1:
        raise Unavailable('initial configuration CAS lacks its exact captured hidden-genesis/terminal-prefix ancestry')


def assemble(header: dict[str, Any], captures: list[dict[str, Any]], raw_headers: list[dict[str, Any]] | None=None,
             artifact_manifests: list[dict[str, Any]] | None=None,
             expected_binding: dict[str, Any] | None=None) -> dict[str, Any]:
    reject_model_fields(header)
    if header.get('native_header_type') != 'DfsNativeShardHeaderV1':
        raise Unavailable('actual Rust native header is missing')
    required=('envelope_binding','scope','owners','initial_cut','finite_dictionary',
              'blobs','snapshot_contexts','native_timeline','environment')
    missing=[key for key in required if key not in header]
    if missing:
        raise Unavailable('native header omits '+', '.join(missing))
    if expected_binding is None or not artifact_manifests or not raw_headers:
        raise Unavailable('assembly requires verified actual executable/source manifest and raw headers')
    if header['envelope_binding'] != expected_binding:
        raise Rejected('assembled header differs from verified actual artifact binding')
    for native in raw_headers:
        if native.get('artifact_binding') != expected_binding:
            raise Rejected('raw native header differs from verified actual artifact binding')
    manifest_captures={}
    for manifest in artifact_manifests:
        if manifest.get('artifact_binding') != expected_binding:
            raise Rejected('capture manifest differs from verified actual artifact binding')
        for capture in manifest['captures']:
            root=capture['actual_data_root']
            if root in manifest_captures:
                raise Rejected('duplicate actual data root in capture manifests')
            manifest_captures[root]=capture
    supplied_roots={capture['actual_mount_root'] for capture in captures}
    if supplied_roots != set(manifest_captures):
        raise Rejected('supplied raw captures do not exactly match actual manifest roster')
    if {native['actual_mount_label'] for native in raw_headers} != {capture['actual_mount_label'] for capture in captures}:
        raise Rejected('raw header/capture mount coverage differs')
    owners={owner['owner_id']:owner for owner in header['owners']}
    if len(owners)!=len(header['owners']):
        raise Rejected('duplicate native owner binding')
    by_label={owner['component_incarnation']:owner for owner in owners.values()}
    raw={}
    for capture in captures:
        label=capture['actual_mount_label']
        if label not in by_label or capture['actual_mount_root'] not in header.get('actual_data_roots',[]):
            raise Rejected('capture is not bound to actual native header mount/root')
        for item in validate_capture(capture):
            key=(label,item['ordinal'])
            if key in raw:raise Rejected('duplicate raw capture event')
            raw[key]=item
    if not raw:raise Unavailable('no actual raw native observations')
    blobs=Blobs(header['blobs'])
    native_contexts,initial_evidence=derive_initial_local_receipts(header,raw,blobs)
    certified_initial_sources=validate_initial_sources(header,raw,raw_headers or [],blobs)
    snapshots={}
    # Native contexts contain operational facts/peer references from Rust,
    # never finite states. Replace only the held-table view with exact packet
    # bytes and independently cross-check native cut identity.
    def snapshot(reference: str, held_bytes: bytes | None=None) -> str:
        context=native_contexts.get(reference)
        if context is None:raise Unavailable(f'missing native snapshot context {reference}')
        result=copy.deepcopy(context)
        verify_replica_freeze_gate(raw,owners,header['scope'],blobs,result,expected_binding['implementation_revision'])
        if held_bytes is not None:
            held=native_json(held_bytes,'held view')
            if held['shard_id'] != header['scope']['shard_id']:
                raise Rejected('held physical shard differs')
            prefix=held['certified_prefix']
            if prefix is None:raise Unavailable('held native prefix was not certified')
            if int(result['local_certified_prefix']['journal_sequence']) != prefix['journal_sequence'] or result['local_certified_prefix']['digest'] != prefix['digest']:
                raise Rejected('context borrows a different held native prefix')
            if int(result['materialized_through_sequence']) != held['materialized_through_sequence']:
                raise Rejected('context materialization cursor differs')
            expected_tables=native_json(blobs.values[result['held_tables_blob']]['data'].encode(),'context held tables')
            if expected_tables != held['tables']:
                raise Rejected('native context differs from actual held tables')
            result['held_tables_blob']=blobs.json('DfsMetadataTableSnapshotTables',held['tables'])
            result['complete_visible_rows_blob']=result['held_tables_blob']
        if reference in snapshots and snapshots[reference]!=result:
            raise Rejected('same snapshot name denotes different actual held views')
        snapshots[reference]=result
        return reference
    for reference in header['initial_cut']['baseline_snapshot_ids']:
        snapshot(reference)
    # Peer certified cuts reference their own real held snapshots; they must
    # survive assembly even when no local semantic action selects that view.
    for reference in native_contexts:
        snapshot(reference)
    used=set(initial_evidence)|certified_initial_sources;events=[];ordinals={owner:0 for owner in owners}
    for source in header['native_timeline']:
        references=source['raw_references']
        if not references:raise Rejected('native semantic event has no actual raw source')
        records=[]
        for ref in references:
            key=(ref['actual_mount_label'],CHECK.decimal_u64(ref['owner_ordinal'],'source ordinal'))
            if key not in raw:raise Rejected('timeline references missing native observation')
            if key in used:raise Rejected('raw native effect represented more than once')
            used.add(key);records.append(raw[key])
        action=copy.deepcopy(source['action'])
        if action['kind'] not in ('handoff-begin','activate','kv-observation') and any(item['kind']=='original-control-commit' for item in records):
            raise Rejected('original KV proof cannot be consumed by another semantic group')
        owner=source['owner_id']
        if owner not in owners:raise Rejected('timeline owner is not native-bound')
        if action['kind'] in ('prepare-payload','commit-digest'):
            materialized='prepare-materialized' if action['kind']=='prepare-payload' else 'commit-materialized'
            durable='prepare-durable' if action['kind']=='prepare-payload' else 'commit-durable'
            matches=[item['record'] for item in records if item['kind']=='kernel']
            before=[item for item in matches if item['phase']==durable]
            after=[item for item in matches if item['phase']==materialized]
            if len(before)!=1 or len(after)!=1 or before[0]['operation']!=after[0]['operation']:
                raise Unavailable('semantic append requires exact durable+materialized native pair')
            expected=action['operation']
            digest=before[0]['operation']
            if expected['operation_id']!=digest['operation_id'] or expected['native_digest']!=digest['payload_digest'] or CHECK.decimal_u64(expected['owner_epoch'],'operation epoch')!=digest['owner_epoch']:
                raise Rejected('native action differs from actual immutable operation')
            snapshot(source['before_snapshot'],byte_vector(before[0]['before_held_json'],'native before'))
            actual_after=byte_vector(after[0]['after_held_json'],'native after')
            snapshot(source['after_snapshot'],actual_after)
            held_after=native_json(actual_after,'actual materialized held cut')
            candidates=[(index,record) for index,record in enumerate(held_after['tables']['prepared_operations_v7']) if record['digest']==digest]
            if len(candidates)!=1:raise Rejected('materialized native operation body absent or duplicated')
            index,record=candidates[0]
            canonical=SUBTREE.canonical_prepared_payload(actual_after,['tables','prepared_operations_v7',index,'payload'],digest['payload_digest'])
            actual_payload_ref=blobs.put('DfsPreparedOperationPayloadCanonicalV7',canonical)
            expected_payload_ref=action['operation']['payload_blob']
            if expected_payload_ref in blobs.values:
                old=blobs.values[expected_payload_ref]
                if old['encoding']!='json-utf8' or old['data'].encode()!=canonical:
                    raise Rejected('header canonical payload differs from exact actual Rust bytes')
            action['operation']['payload_blob']=actual_payload_ref
            if action['kind']=='prepare-payload':
                action['prepare_record_blob']=blobs.json('DfsPreparedOperationRecord',record)
            frame=byte_vector(before[0]['exact_frame'],'actual append frame')
            blobs.put('DfsMetadataExactJournalFrame',frame,'exact-frame-base64')
            blobs.json('DfsMetadataJournalEntry',before[0]['exact_entry'])
        elif action['kind']=='replica-freeze-read':
            keys=[(ref['actual_mount_label'],CHECK.decimal_u64(ref['owner_ordinal'])) for ref in source['raw_references']]
            if len(keys)!=1 or keys[0][0]!=owner:
                raise Rejected('normal freeze READ group changes its exact actual source')
            qualified=verify_replica_freeze_read(raw,owners,header['scope'],keys[0])
            expected=dict(scope=header['scope'],implementation_revision=expected_binding['implementation_revision'],
                owner_bindings=header['owners'],complete=True,continuous=True,**qualified)
            proof=native_json(blobs.values[action['native_freeze_read_observation_blob']]['data'].encode(),'actual READ group')
            if proof!=expected or action['recipient_node_id']!=owners[owner]['node_id']:
                raise Rejected('normal READ action changes its whole actual read/permit/custody fact')
            snapshot(source['before_snapshot']);snapshot(source['after_snapshot'])
        elif action['kind']=='replica-ingest':
            matched=committed_ingest_group_sources(raw,source)
            if matched is not None:
                receiver_key,accepted_key=matched;fact=raw[receiver_key]['record']['facts']
                verify_committed_replication(raw,{row['actual_mount_label']:row for row in raw_headers},owners,
                    header['scope'],set(header['finite_dictionary']['operation_aliases']),receiver_key,accepted_key,
                    group_refs=source['raw_references'],group_owner=source['owner_id'])
                witness=native_json(blobs.values[action['native_ingest_observation_blob']]['data'].encode(),'actual committed ingest')
                cfg=witness['actual_source_config']
                if (action['recipient_node_id']!=fact['local_node_id'] or native_prefix(action['prefix'])!=native_prefix(fact['durable_prefix'])
                        or witness['actual_boundary_source']!={'owner_id':receiver_key[0],'owner_ordinal':str(receiver_key[1])}
                        or witness['actual_ingest_permit_tuple']!=fact['permit']
                        or native_json(blobs.values[cfg['native_config_blob']]['data'].encode(),'actual source config')!=fact['actual_source_config']
                        or witness['actual_pending_handoff_blob'] is not None or witness['materialization_pending'] is not fact['materialization_pending']):
                    raise Rejected('committed ingest witness changes its actual durable source/permit/config')
            snapshot(source['before_snapshot']);snapshot(source['after_snapshot'])
        elif action['kind'] in ('publication-guard-admitted','publication-guard-released'):
            phase='admitted' if action['kind']=='publication-guard-admitted' else 'released'
            matches=[item['record'] for item in records if item['kind']=='guard' and item['record']['phase']==phase]
            if len(matches)!=1 or matches[0]['guard_id']!=action['guard_id']:
                raise Rejected('guard action differs from actual native guard lifetime')
            observed=matches[0]
            if phase=='admitted':
                if not observed['matches_actual_mounted_owner']:
                    raise Unavailable('guard did not retain the exact mounted native owner')
                if observed['digest']['operation_id']!=action['operation']['operation_id'] or observed['digest']['payload_digest']!=action['operation']['native_digest']:
                    raise Rejected('guard action changes its actual sealed digest')
                facts=observed['facts'];native_authority=facts['authority']
                no_effect_records = [item['record'] for item in records if item['kind'] == 'boundary']
                if no_effect_records:
                    prepared = [item['record'] for (actor, _), item in raw.items()
                                if actor == owner and item['kind'] == 'kernel'
                                and item['record']['phase'] == 'prepare-materialized'
                                and item['record']['operation'] == observed['digest']]
                    if len(prepared) != 1:
                        raise Unavailable('not-sent grouping lacks one actual materialized prepare')
                    prepared_held = native_json(byte_vector(prepared[0]['after_held_json'],
                                                           'not-sent prepared held cut'),
                                                'not-sent prepared held cut')
                    starts = [row for row in no_effect_records if row['facts']['phase'] in
                              ('rpc-attempt-started', 'direct-journal-rpc-attempt-started')]
                    assigned = set()
                    config = native_authority['expected']
                    for start in starts:
                        returns = [row for row in no_effect_records
                                   if row['facts']['phase'] == journal_rpc_return_phase(start['facts'])
                                   and row['facts'].get('attempt_id') == start['facts'].get('attempt_id')]
                        if len(returns) != 1:
                            raise Unavailable('not-sent grouping lacks its SAME normal terminal return')
                        verify_not_sent_journal_attempt(start, returns[0],
                            source_node=facts['actor_node_id'], target_node=start['facts']['target_node_id'],
                            digest=observed['digest'], prepared_prefix=prepared_held['certified_prefix'],
                            prepared_ordinal=CHECK.decimal_u64(prepared[0]['owner_ordinal']),
                            guard_ordinal=CHECK.decimal_u64(observed['owner_ordinal']),
                            members=[config['primary_node_id']] + config.get('replica_node_ids', []))
                        for row in (start, returns[0]):
                            ordinal = CHECK.decimal_u64(row['owner_ordinal'])
                            if ordinal in assigned or row['owner_label'] != owner:
                                raise Rejected('not-sent grouping repeats or borrows a raw source')
                            assigned.add(ordinal)
                    if len(assigned) != len(no_effect_records):
                        raise Unavailable('guard grouping includes unknown/abandoned native transport facts')
                expected_config=native_authority.get('expected')
                if expected_config is None:raise Unavailable('local-author guard is outside clustered model')
                config_blob=action['expected_config']['native_config_blob']
                if native_json(blobs.values[config_blob]['data'].encode(),'guard config')!=expected_config:
                    raise Rejected('guard envelope config differs from actual validated native config')
                guard_blob=blobs.values[action['authorization_blob']]
                if guard_blob['native_type']!='DfsNativePublicationGuardObservationV1':
                    raise Rejected('guard authorization blob has wrong native type')
                guard_fact=native_json(guard_blob['data'].encode(),'guard fact')
                actual_mode=native_authority['mode']
                if guard_fact['digest']!=observed['digest'] or guard_fact['authorization_mode']!=actual_mode:
                    raise Rejected('guard envelope authority differs from actual guard')
                if set(guard_fact['authenticated_prepare_ack_node_ids'])!=set(native_authority['authenticated_acknowledged_node_ids']):
                    raise Rejected('guard envelope invents or drops authenticated native ACKs')
                if actual_mode != 'full-shard':
                    raise Unavailable('clustered native guard rejects retired fallback/local authority')
                correlated = [item['record'] for item in records if item['kind'] == 'configuration-cut'
                              and item['record'].get('guard_context_id') == observed['guard_id']]
                if len(correlated) != 1:
                    raise Unavailable('guard lacks its SAME consumed retained configuration cut')
                actual_cut = correlated[0]
                header_for_owner = next(value for value in raw_headers if value['actual_mount_label'] == owner)
                qualified = validate_retained_configuration_cut(actual_cut, header_for_owner,
                    header['scope'], guard_id=observed['guard_id'], expected=expected_config)
                exact = action['retained_configuration_cut']
                if exact['requested_keys'] != qualified['selected_keys'] or CHECK.decimal_u64(exact['revision']) != CHECK.decimal_u64(qualified['cached_revision']):
                    raise Rejected('guard cut differs from actual accepted retained projection')
                evidence = native_json(blobs.values[exact['native_authority_evidence_blob']]['data'].encode(), 'retained cut evidence')
                if evidence != actual_cut or len(exact['rows']) != 2:
                    raise Rejected('guard cut substitutes actual retained evidence/row coverage')
                entries = {row['key']: row for row in qualified['entries']}
                for key, row in zip(qualified['selected_keys'], exact['rows']):
                    if row['key'] != key:
                        raise Rejected('guard retained row key changes actual requested scope')
                    actual = entries.get(key)
                    value = native_json(blobs.values[row['entry_blob']]['data'].encode(), 'retained KV row') if row['entry_blob'] else None
                    if value != actual:
                        raise Rejected('guard retained row differs from SAME consumed cut')
                snapshot(source['before_snapshot'])
                if observed['held'] is None:raise Unavailable('guard admission lacks actual held native cut')
                snapshot(source['after_snapshot'],json.dumps(observed['held'],separators=(',',':')).encode())
            else:
                if observed['digest']['operation_id']!=action['operation_id']:
                    raise Rejected('guard release names another actual operation')
                snapshot(source['before_snapshot']);snapshot(source['after_snapshot'])
        elif action['kind'] == 'fallback-permit':
            raise Rejected('retired per-operation KV fallback is incompatible with current pipeline')
        elif action['kind'] == 'replay':
            matches = [item['record'] for item in records if item['kind'] == 'kernel'
                       and item['record']['phase'] == 'exact-committed-replay']
            if len(matches) != 1:
                raise Unavailable('replay requires its actual committed native lookup boundary')
            observed = matches[0]; digest = observed['operation']; expected = action['operation']
            if digest['operation_id'] != expected['operation_id'] or digest['payload_digest'] != expected['native_digest']:
                raise Rejected('replay borrows another actual immutable operation')
            snapshot(source['before_snapshot'],byte_vector(observed['before_held_json'],'replay prior'))
            snapshot(source['after_snapshot'],byte_vector(observed['after_held_json'],'replay next'))
        elif action['kind'] == 'handoff-begin':
            calls = [item for item in records if item['kind'] == 'control-commit']
            if len(calls) != 1:
                raise Unavailable('finite pending decision requires its exact global transaction receipt')
            call = calls[0]; key = (owner, call['ordinal'])
            native_header = next(value for value in raw_headers if value['actual_mount_label'] == owner)
            voters = {member['node_id'] for member in native_header['normal_mounted_identity']['configured_members']
                      if member['role'] == 'voter' and member['state'] == 'voter'}
            module, _ = initial_native_verifiers(header['scope'], header['owners'], raw)
            txn = module.native_receipt(call['record'], voters, scope=header['scope'], owner=owners[owner],
                header=native_header, raw=raw, key=key)
            decoded = module.request_decodes(native_header, call['record'])
            pending = native_json(blobs.values[action['handoff_blob']]['data'].encode(), 'actual pending handoff')
            retiring = native_json(blobs.values[action['retiring']['native_config_blob']]['data'].encode(), 'retiring config')
            desired = native_json(blobs.values[action['desired']['native_config_blob']]['data'].encode(), 'desired config')
            pending_key = '/dfs/filesystems/' + header['scope']['file_system_id'] + '/metadata-membership-handoffs/' + header['scope']['shard_id'].strip('/').replace('/', '_')
            effects = [value for (section, _), value in decoded.items() if section == 'success' and value[0] == pending_key]
            if len(effects) != 1 or effects[0][1] != 'DfsMetadataShardMembershipHandoff' or effects[0][2] != pending:
                raise Rejected('finite handoff decision does not publish the exact observed pending body')
            if pending['handoff_id'] != action['handoff_id'] or pending['retiring_shard'] != retiring or pending['desired_shard'] != desired:
                raise Rejected('finite handoff action changes its original tuple/body')
            # These are genuine global configuration CAS constraints, not
            # ordinary metadata effect authority or model permit state.
            module.exact_scope_guards(txn, decoded, header['scope'], retiring, pending_key,
                None, pending.get('required_head_watermark'))
            before = snapshots[snapshot(source['before_snapshot'])]
            snapshot(source['after_snapshot'])
            prefix = action['pending_begin_local_prefix']
            if before['owner_id'] != owner or before['local_certified_prefix'] != prefix or CHECK.decimal_u64(action['pending_begin_local_head']) != CHECK.decimal_u64(prefix['journal_sequence']):
                raise Rejected('pending decision substitutes a freeze/height for its actual original local prefix')
        elif action['kind'] in ('freeze', 'activate', 'rebind'):
            phase = {'freeze':'retiring-freeze-persisted', 'activate':'handoff-activation-completed',
                     'rebind':'successor-rebound'}[action['kind']]
            matched = [item['record'] for item in records if item['kind'] == 'boundary'
                       and item['record']['facts']['phase'] == phase]
            if len(matched) != 1:
                raise Unavailable('handoff transition lacks its actual normal boundary packet')
            observed = matched[0]
            if observed['before_held_json'] is None or observed['after_held_json'] is None:
                raise Unavailable('handoff transition lacks actual cached held cuts')
            snapshot(source['before_snapshot'],byte_vector(observed['before_held_json'],'handoff prior'))
            snapshot(source['after_snapshot'],byte_vector(observed['after_held_json'],'handoff next'))
        elif action['kind'] in ('reply', 'activation-reply'):
            matched=[item['record'] for item in records if item['kind']=='boundary'
                     and item['record']['facts'].get('phase')=='client-operation-outcome']
            if len(matched)!=1:
                raise Unavailable('reply delivery requires its actual final client outcome, not an abandoned transport future')
            observed=matched[0]['facts'];identity='operation_id' if action['kind']=='reply' else 'handoff_id'
            if (observed[identity]!=action[identity] or observed['delivery']!=action['delivery']
                    or action['kind']=='reply' and observed['attempt_id']!=action['attempt_id']):
                raise Rejected('reply action changes its actual final operation/handoff/attempt/delivery')
            snapshot(source['before_snapshot']);snapshot(source['after_snapshot'])
        else:
            snapshot(source['before_snapshot']);snapshot(source['after_snapshot'])
        ordinals[owner]+=1
        events.append({'event_id':source['event_id'],'owner_id':owner,'owner_sequence':str(ordinals[owner]),
                       'turn_id':source['turn_id'],'caused_by':source['caused_by'],
                       'before_snapshot':source['before_snapshot'],'after_snapshot':source['after_snapshot'],'action':action})
    # Auxiliary read/phase records must be explicitly assigned to a semantic
    # event or certified normal no-effect observation. Effects cannot disappear.
    for reference in header.get('native_auxiliary_observations',[]):
        key=(reference['actual_mount_label'],CHECK.decimal_u64(reference['owner_ordinal']))
        if key not in raw or key in used:raise Rejected('invalid/duplicate auxiliary source reference')
        if raw[key]['kind']=='metadata-cache-cut':
            validate_metadata_cache_cut(raw[key]['record'])
        elif raw[key]['kind']=='boundary' and raw[key]['record']['facts']['phase']=='replica-write-freeze-read':
            verify_replica_freeze_read(raw, owners, header['scope'], key)
        elif raw[key]['kind']=='boundary' and raw[key]['record']['facts']['phase']=='successor-rebound':
            module,_=initial_native_verifiers(header['scope'],header['owners'],raw)
            verified=module.verify_auxiliary_rebind(header['scope'],header['owners'],
                {row['actual_mount_label']:row for row in raw_headers},raw,key,header.get('initial_completed_handoff'))
            blobs.json('DfsNativeFreshHandleCompletedDataObservationV1',verified)
        elif raw[key]['kind']=='original-control-commit':
            module,_=initial_native_verifiers(header['scope'],header['owners'],raw)
            module.verify_original_observation(header['scope'],header['owners'],
                {row['actual_mount_label']:row for row in raw_headers},raw,key)
        elif raw[key]['kind'] == 'authoritative-read':
            if raw[key]['record'].get('guard_context_id') is not None:
                raise Rejected('retired per-operation authoritative read cannot be auxiliary')
        elif raw[key]['kind'] == 'configuration-cut':
            if 'guard_context_id' not in raw[key]['record']:
                raise Unavailable('archived retained cut lacks current context contract')
            if raw[key]['record']['guard_context_id'] is not None:
                raise Unavailable('guard-bound retained cut cannot disappear into auxiliary observations')
        elif raw[key]['kind'] not in ('authoritative-read','configuration-cut'):
            raise Rejected('durable kernel/control effects and guards cannot be labeled auxiliary')
        used.add(key)
    if used!=set(raw):raise Unavailable('native capture has ungrouped observations/effects')
    binding=header['envelope_binding']
    # Preserve the entire actual runner binding as a native blob; top-level
    # fields follow the frozen versioned envelope schema.
    blobs.json('NativeTraceArtifactBindingV1',binding)
    top_binding={key:binding[key] for key in ('implementation_revision','implementation_build_id','schema_sha256','model_sha256')}
    top_binding['run_id']=binding.get('run_id',binding['implementation_build_id']+':'+header['native_timeline'][0]['turn_id'])
    return {**top_binding,'schema_version':1,'projection_version':1,'metadata_protocol_version':7,
            'scope':header['scope'],'owners':header['owners'],'blobs':blobs.values,
            'snapshots':snapshots,'environment':header['environment'],'events':events,
            'initial_cut':header['initial_cut'],'finite_dictionary':{'payload_aliases':{},**header['finite_dictionary'],'sequence_cuts':[{'compaction_identity_blob':header['initial_cut']['baseline_validation_blob'],**cut} for cut in header['finite_dictionary']['sequence_cuts']]},
            'terminal':{'dropped_events':'0','incomplete_events':'0','discontinuities':'0',
                        'owners_drained':True,'subscriptions_closed':True,
                        'final_owner_sequences':{owner:str(ordinal) for owner,ordinal in ordinals.items()}}}


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-header',type=Path,required=True)
    parser.add_argument('--capture',type=Path,action='append',required=True)
    parser.add_argument('--native-raw-header',type=Path,action='append',default=[])
    parser.add_argument('--artifact-manifest',type=Path,action='append',required=True)
    parser.add_argument('--source-manifest',type=Path,required=True)
    parser.add_argument('--executable',type=Path,help='immutable copy of the exact captured executable')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    try:
        expected=[CHECK.verified_artifact_manifest(path,args.source_manifest,args.executable) for path in args.artifact_manifest]
        if any(binding != expected[0] for binding in expected):
            raise Rejected('native manifests bind different executable/source artifacts')
        manifests=[CHECK.read_json(path) for path in args.artifact_manifest]
        actual_raw={}
        actual_headers={}
        for path,manifest in zip(args.artifact_manifest,manifests):
            for item in manifest['captures']:
                raw=CHECK.read_json(path.parent/item['raw_file'])
                native=CHECK.read_json(path.parent/item['header_file'])
                label=raw['actual_mount_label']
                if label in actual_raw:raise Rejected('duplicate actual capture across manifests')
                actual_raw[label]=raw;actual_headers[label]=native
        supplied_raw=[CHECK.read_json(path) for path in args.capture]
        supplied_headers=[CHECK.read_json(path) for path in args.native_raw_header]
        if {item['actual_mount_label']:item for item in supplied_raw}!=actual_raw:
            raise Rejected('supplied raw packets differ from actual manifest capture files')
        if {item['actual_mount_label']:item for item in supplied_headers}!=actual_headers:
            raise Rejected('supplied raw headers differ from actual manifest header files')
        value=assemble(CHECK.read_json(args.native_header),supplied_raw,supplied_headers,manifests,expected[0])
        schema=CHECK.read_json(HERE/'shard-trace-v1.schema.json')
        CHECK.validate_schema(value,schema,schema)
        args.output.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    except Unavailable as exc:
        print(json.dumps({'status':'unavailable','reason':str(exc)}));return 2
    except (Rejected,KeyError,TypeError,ValueError) as exc:
        print(json.dumps({'status':'rejected','reason':str(exc)}));return 1
    print(json.dumps({'status':'assembled-native-envelope','output':str(args.output),'conformance':'not yet checked'}))
    return 0


if __name__=='__main__':raise SystemExit(main())
