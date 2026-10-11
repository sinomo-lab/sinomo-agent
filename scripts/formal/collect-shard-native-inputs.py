#!/usr/bin/env python3
"""Package actual finite multi-owner observations; produce no model state."""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve()
while not (ROOT / 'scripts/formal/native-artifact-binding.py').is_file():
    if ROOT == ROOT.parent:
        raise RuntimeError('workspace formal verifier unavailable')
    ROOT = ROOT.parent

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value

BINDING = module('native_binding_inputs', ROOT/'scripts/formal/native-artifact-binding.py')
SUBTREE = module('native_subtree_inputs', ROOT/'scripts/formal/native-json-subtree.py')
COLLECTIONS = ('packets', 'authoritative_reads', 'guard_events', 'content_receipts',
               'configuration_cuts', 'metadata_cache_cuts', 'boundary_packets', 'control_commits', 'original_control_commits')
MAX_FILE_BYTES = 128 * 1024 * 1024

def read(path):
    path = Path(path)
    if path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError('native observation artifact exceeds bounded file size')
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=BINDING.pairs)
    return raw, value

def child(root, name):
    name = Path(name)
    if name.is_absolute() or len(name.parts) != 1 or name.name in ('', '.', '..'):
        raise ValueError('native capture file is not an exact sibling filename')
    return root/name

def ordinal(value):
    if not isinstance(value, str) or not value.isdecimal() or value.startswith('0') or int(value) > 2**64-1:
        raise ValueError('native owner ordinal is not canonical positive u64')
    return int(value)

def held(raw, record, key):
    value = record.get(key)
    if value is None:
        return None
    if isinstance(value, list):
        if any(type(byte) is not int or not 0 <= byte <= 255 for byte in value):
            raise ValueError('native held JSON bytes malformed')
        exact = bytes(value)
        decoded = json.loads(exact, object_pairs_hook=BINDING.pairs)
    elif isinstance(value, dict):
        # Guard's typed held field is an actual JSON subtree, never reencoded.
        raise ValueError('typed held snapshot needs its explicit raw subtree path')
    else:
        raise ValueError('native held bytes absent or malformed')
    return {'sha256': hashlib.sha256(exact).hexdigest(), 'exact_json_utf8': exact.decode(), 'value': decoded}

def collect_native_inputs(manifest_path, expected_binding, selected_operation_ids, *, source_manifest_path, executable=None):
    expected_binding = BINDING.binding(expected_binding)
    verified, manifest = BINDING.verify_manifest(manifest_path, source_manifest_path, executable)
    if verified != expected_binding:
        raise ValueError('expected native artifact binding differs from actual manifest/source/executable')
    selected = list(selected_operation_ids)
    if not 1 <= len(selected) <= 3 or len(set(selected)) != len(selected) or any(not isinstance(v, str) or not v for v in selected):
        raise ValueError('select one to three distinct actual immutable operation IDs')
    captures = manifest['captures']
    if not 1 <= len(captures) <= 4:
        raise ValueError('native input requires one to four actual capture handles')
    root = Path(manifest_path).resolve().parent
    owners = []; events = []; cuts = {}; bodies = {key: [] for key in selected}
    receivers = {}; acknowledgements = []; missing = []; scope = None; nodes = set(); labels = set()
    for capture in captures:
        raw_bytes, raw = read(child(root,capture['raw_file']))
        header_bytes, header = read(child(root,capture['header_file']))
        label = raw['actual_mount_label']; node = capture['node_id']; shard = raw['shard']
        if raw.get('raw_observation_version') != 1 or header.get('native_header_type') != 'DfsNativeShardHeaderV1':
            raise ValueError('unsupported native raw/header observation version')
        if node in nodes or label in labels:
            raise ValueError('duplicate actual node or mount capture')
        nodes.add(node); labels.add(label)
        if header['artifact_binding'] != expected_binding or header['shard'] != shard or header['actual_mount_label'] != label:
            raise ValueError('header borrows another binding/mount/shard')
        mounted = header.get('normal_mounted_identity')
        if not mounted or mounted['local_node_id'] != node:
            raise ValueError('actual mounted node identity is unavailable or inconsistent')
        if raw['closed'] is not True or raw['actual_owner_stopped'] is not True or any(raw[k] != '0' for k in ('in_flight','dropped','incomplete')):
            raise ValueError('actual capture did not drain without lost/uncertain observations')
        owners.append({'owner_id': label, 'node_id': node, 'actual_data_root':capture['actual_data_root'],
                       'native_mount_identity':mounted, 'raw_path':str(child(root,capture['raw_file'])),
                       'raw_sha256':hashlib.sha256(raw_bytes).hexdigest(),
                       'header_path':str(child(root,capture['header_file'])), 'header_sha256':hashlib.sha256(header_bytes).hexdigest()})
        if ('original_control_commits' in raw) != ('actual_original_control_commits' in header):
            raise ValueError('original native certificate family is present in only raw or header')
        if raw.get('original_control_commits', []) != header.get('actual_original_control_commits', []):
            raise ValueError('original native certificate raw/header copies differ')
        local = []; seen = set(); memory = []
        for collection in COLLECTIONS:
            if collection not in raw:
                missing.append({'owner_id':label,'collection':collection,'reason':'collector family not present in this actual build'})
                continue
            for index, record in enumerate(raw[collection]):
                number = ordinal(record['owner_ordinal'])
                if record['owner_label'] != label or number in seen:
                    raise ValueError('native packet borrows another owner or repeats its ordinal')
                seen.add(number)
                event = {'owner_id':label,'owner_ordinal':number,'collection':collection,'index':index,'record':record}
                local.append(event)
                for key, side in (('before_held_json','before'),('after_held_json','after')):
                    snapshot = held(raw_bytes,record,key)
                    if snapshot:
                        if snapshot['value']['shard_id'] != shard:
                            raise ValueError('memory cut names another physical shard')
                        memory.append({'owner_ordinal':number,'side':side,'collection':collection,'index':index,**snapshot})
                if collection == 'guard_events' and record.get('held') is not None:
                    exact = SUBTREE.subtree(raw_bytes,[collection,index,'held'])
                    memory.append({'owner_ordinal':number,'side':'admission','collection':collection,'index':index,
                                   'sha256':hashlib.sha256(exact).hexdigest(),'exact_json_utf8':exact.decode(),'value':record['held']})
                entry = record.get('exact_entry') if collection == 'packets' else None
                mutation = (entry or {}).get('mutation',{})
                if mutation.get('type') == 'prepare-operation-v7':
                    prepared = mutation['record']; digest=prepared['digest']; op=digest['operation_id']
                    if op in bodies:
                        actual_scope = {'file_system_id':digest['file_system_id'],'shard_id':digest['shard_id']}
                        if scope is None: scope=actual_scope
                        if actual_scope != scope or digest['shard_id'] != shard:
                            raise ValueError('selected actual operations cross filesystem/shard scopes')
                        encoded = SUBTREE.canonical_prepared_payload(raw_bytes,[collection,index,'exact_entry','mutation','record','payload'],digest['payload_digest'])
                        bodies[op].append({'owner_id':label,'owner_ordinal':number,'native_digest':digest,
                                          'canonical_payload_json_utf8':encoded.decode(),'payload_sha256':hashlib.sha256(encoded).hexdigest()})
                facts = record.get('facts',{})
                phase = facts.get('phase')
                if phase in ('replica-journal-persisted','replica-prefix-probe-accepted','replica-snapshot-accepted'):
                    command = facts['command']; request_id=command['request_id']
                    source_node = command['source_node_id']
                    receivers.setdefault(request_id,[]).append({'receiver_owner_id':label,'receiver_node_id':node,
                        'receiver_ordinal':number,'source_node_id':source_node,'phase':phase,'command':command,
                        'response':facts.get('response'),'durable_prefix':facts.get('durable_prefix')})
                elif phase == 'source-journal-response-accepted':
                    acknowledgements.append({'source_owner_id':label,'source_node_id':node,'source_ordinal':number,'facts':facts})
        local.sort(key=lambda event:event['owner_ordinal']); events.extend(local)
        if [event['owner_ordinal'] for event in local] != list(range(1,len(local)+1)):
            raise ValueError('native owner-local observations are missing or discontinuous')
        memory.sort(key=lambda item:(item['owner_ordinal'], {'before':0,'admission':1,'after':2}[item['side']]))
        cuts[label]={'initial':memory[0] if memory else None,'terminal':memory[-1] if memory else None,
                     'all_memory_cuts':memory,'unavailable':None if memory else 'no actual memory-cut observation in this capture'}
    if scope is None or any(not rows for rows in bodies.values()):
        raise ValueError('selected operation has no actual immutable native Prepare body')
    for op, rows in bodies.items():
        if len({(row['native_digest']['payload_digest'],row['canonical_payload_json_utf8']) for row in rows}) != 1:
            raise ValueError('selected operation repeats different actual immutable bodies')
    edges=[]
    for ack in acknowledgements:
        facts=ack['facts']; response=facts['response']; request_id=response['request_id']
        matches=[receiver for receiver in receivers.get(request_id,[])
                 if receiver['receiver_node_id'] == facts['authenticated_source_node_id']
                 and receiver['source_node_id'] == ack['source_node_id']]
        if not matches:
            missing.append({'owner_id':ack['source_owner_id'],'request_id':request_id,'reason':'accepted source ACK lacks actual receiver observation'})
            continue
        for receiver in matches:
            command=receiver['command']; boundary=command.get('batch',command)
            if response['shard_id'] != boundary['shard_id'] or response['next_after_sequence'] != boundary['next_after_sequence']:
                raise ValueError('accepted source ACK differs from actual receiver command scope/cut')
            edges.append({'request_id':request_id, 'source_owner_id':ack['source_owner_id'], 'source_ordinal':ack['source_ordinal'],
                          'receiver_owner_id':receiver['receiver_owner_id'],'receiver_ordinal':receiver['receiver_ordinal'],
                          'receiver_phase':receiver['phase'],'actual_source_response':response})
    return {'native_operational_inputs_version':2,'classification':'actual native input packaging; context/projection/replay not yet checked',
            'artifact_binding':expected_binding,'artifact_source_inputs':{'manifest_path':str(Path(manifest_path).resolve()),'source_manifest_path':str(Path(source_manifest_path).resolve()),'executable':str(Path(executable).resolve()) if executable else None},'scope':scope,'owners':owners,'selected_operation_ids':selected,
            'operation_bodies':bodies,'owner_memory_cuts':cuts,'owner_local_events':events,
            'actual_request_response_correlations':edges,'missing_observation_families':missing,
            'cross_owner_total_order':None}

def write_native_inputs(path, value):
    path = Path(path)
    written = 0
    try:
        with path.open('w', encoding='utf-8') as output:
            for chunk in json.JSONEncoder(separators=(',', ':'), ensure_ascii=False).iterencode(value):
                written += len(chunk.encode('utf-8'))
                if written >= MAX_FILE_BYTES:
                    raise ValueError('native input package exceeds bounded file size')
                output.write(chunk)
            output.write('\n')
    except (ValueError, OSError):
        path.unlink(missing_ok=True)
        raise

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--manifest',required=True);parser.add_argument('--binding',required=True)
    parser.add_argument('--source-manifest',required=True);parser.add_argument('--executable');parser.add_argument('--operation-id',action='append',required=True)
    parser.add_argument('--output',required=True);args=parser.parse_args()
    try:
        expected=json.loads(Path(args.binding).read_text(),object_pairs_hook=BINDING.pairs)
        result=collect_native_inputs(args.manifest,expected,args.operation_id,source_manifest_path=args.source_manifest,executable=args.executable)
        write_native_inputs(args.output, result)
    except FileNotFoundError as exc:
        print(json.dumps(dict(status='unavailable',reason=str(exc))));return 2
    except (ValueError,KeyError,TypeError) as exc:
        print(json.dumps(dict(status='rejected',reason=str(exc))));return 1
    print(json.dumps(dict(status='packaged-actual-native-inputs',output=args.output,conformance='not yet checked')))
    return 0
if __name__=='__main__':raise SystemExit(main())
