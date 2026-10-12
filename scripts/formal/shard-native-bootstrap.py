"""Verify actual initial handoff evidence without constructing model state."""
import hashlib
import importlib.util
from pathlib import Path

A = None
Rejected, Unavailable = ValueError, RuntimeError

def configure(assembler):
    global A, Rejected, Unavailable
    A = assembler
    Rejected, Unavailable = assembler.Rejected, assembler.Unavailable

def number(value, label):
    try: return A.SUBTREE.PROJECTOR.u64(value, label)
    except A.SUBTREE.PROJECTOR.Rejected as error: raise Rejected(str(error)) from error

def native_cut(value):
    parsed=A.native_prefix(value)
    if parsed["version"]!=2:raise Rejected("completed bootstrap requires actual native v2 prefix")
    return dict(version=2,journal_sequence=number(parsed["journal_sequence"],"bootstrap native prefix"),digest=parsed["digest"])

def scoped(config, scope):
    return all(config.get(field) == scope[field] for field in ("file_system_id", "shard_id"))

def members(config):
    result = [config["primary_node_id"]] + config.get("replica_node_ids", [])
    if len(result) != len(set(result)):
        raise Rejected("bootstrap config duplicates actual member identity")
    return set(result)

def quorum(authority, voters):
    basis=authority.get('basis')
    if (number(authority['term'],'native term')==0 or number(authority['commit_index'],'native index')==0
            or not isinstance(basis,dict) or set(basis)!={'current-term-quorum'}):
        raise Rejected('bootstrap request lacks its exact current-term committed receipt')
    proof=basis['current-term-quorum']
    if not isinstance(proof,dict) or set(proof)!={'voter_acks','required_quorum'}:
        raise Rejected('original native quorum fields malformed')
    required=number(proof['required_quorum'],'native quorum')
    acknowledged=number(proof['voter_acks'],'native ACK count')
    if required!=len(voters)//2+1 or not required<=acknowledged<=len(voters):
        raise Rejected('bootstrap atomic KV receipt differs from actual configured voters')


def original_fact(scope, owner, header, record):
    from pathlib import PurePosixPath, PureWindowsPath
    if (not isinstance(record,dict) or set(record)!={'owner_label','owner_ordinal','original'}
            or record['owner_label']!=owner['owner_id'] or header['actual_mount_label']!=owner['owner_id']):
        raise Rejected('original certificate borrows another actual owner')
    fact=record['original']
    if not isinstance(fact,dict) or set(fact)!={'root','node_id','request','committed','authority'}:
        raise Rejected('original native certificate fields malformed')
    root=header['actual_data_root']
    if not isinstance(root,str) or not isinstance(fact['root'],str):
        raise Rejected('original certificate lacks an actual path identity')
    path=PureWindowsPath if '\\' in root else PurePosixPath
    if (fact['root']!=str(path(root)/'cluster') or fact['node_id']!=owner['node_id']
            or header['normal_mounted_identity']['local_node_id']!=owner['node_id']):
        raise Rejected('original certificate changes its actual KV root/node')
    request,committed,authority=fact['request'],fact['committed'],fact['authority']
    if (not isinstance(request,dict) or set(request)!={'request_id','command'}
            or not isinstance(request['command'],dict) or set(request['command'])!={'Txn'}
            or not isinstance(committed,dict)
            or set(committed)!={'request_id','command_digest','term','log_index','revision','txn_response'}
            or not isinstance(authority,dict) or set(authority)!={'term','commit_index','revision','basis'}
            or request['request_id']!=committed['request_id']):
        raise Rejected('original certificate lacks full exact request/result identity')
    txn=request['command']['Txn']; config_key='/dfs/filesystems/'+scope['file_system_id']+'/metadata-shards/'+scope['shard_id'].strip('/').replace('/','_')
    if (not isinstance(txn,dict) or not isinstance(txn.get('compares'),list)
            or not any(isinstance(compare,dict) and set(compare) in ({'ValueEquals'},{'Missing'})
                and isinstance(next(iter(compare.values())),dict)
                and next(iter(compare.values())).get('key')==config_key for compare in txn['compares'])):
        raise Rejected('original certificate is outside the exact filesystem/shard interest')
    response=committed['txn_response']
    if (not isinstance(response,dict) or type(response.get('succeeded')) is not bool
            or number(response['revision'],'original receipt revision')!=number(committed['revision'],'original record revision')
            or number(authority['term'],'original authority term')!=number(committed['term'],'original record term')
            or number(authority['commit_index'],'original certified head')<number(committed['log_index'],'original request index')
            or number(authority['revision'],'original certified revision')<number(committed['revision'],'original request revision')):
        raise Rejected('original certificate changes its durable record/result coordinates')
    A.SUBTREE.PROJECTOR.native_digest_hex(committed['command_digest'])
    number(record['owner_ordinal'],'original observer ordinal')
    return fact


def original_matches_call(fact, record):
    request,result=record['commit']['request'],record['commit']['result']
    committed,authority=fact['committed'],result['authority']
    return (fact['request']==dict(request_id=request['request_id'],command={'Txn':request['txn']})
        and result['request_id']==committed['request_id']
        and result.get('txn_response')==committed['txn_response']
        and all(number(authority[a],'caller authority '+a)==number(committed[b],'original record '+b)
                for a,b in (('term','term'),('commit_index','log_index'),('revision','revision')))
        and result.get('submitted_command_digest') is not None
        and A.SUBTREE.PROJECTOR.native_digest_hex(result['submitted_command_digest'])
            ==A.SUBTREE.PROJECTOR.native_digest_hex(result['command_digest'])
            ==A.SUBTREE.PROJECTOR.native_digest_hex(committed['command_digest']))


def original_for_replay(scope, owner, header, raw, key, record, voters, *, required=True):
    if (key[0]!=owner['owner_id'] or record['owner_label']!=key[0]
            or number(record['owner_ordinal'],'replay caller ordinal')!=key[1]):
        raise Rejected('replay association changes its actual caller source')
    matches=[]
    for original_key,item in raw.items():
        if (original_key[0]!=key[0] or original_key[1]>=key[1]
                or item['kind']!='original-control-commit'
                or item['record'].get('original',{}).get('request',{}).get('request_id')!=record['commit']['request']['request_id']):
            continue
        if number(item['record']['owner_ordinal'],'original observer ordinal')!=original_key[1]:
            raise Rejected('original association changes its actual observer ordinal')
        fact=original_fact(scope,owner,header,item['record'])
        if not original_matches_call(fact,record):
            raise Rejected('exact replay differs from its original full native command/result')
        quorum(fact['authority'],voters)
        if record['commit']['result']['authority']['basis']!='exact-committed-request':
            # The normal caller's own proof and the enclosing producer prefix
            # are independently genuine. Neither is relabelled as the other.
            quorum(record['commit']['result']['authority'],voters)
        matches.append(original_key)
    if not matches and not required: return None
    if len(matches)!=1:
        raise Unavailable('exact replay lacks one earlier actual original current-term certificate')
    return matches[0]


def verify_original_observation(scope, owners, headers, raw, key):
    owner=next((owner for owner in owners if owner['owner_id']==key[0]),None)
    if owner is None: raise Rejected('original observation has no actual owner')
    if number(raw[key]['record']['owner_ordinal'],'original source ordinal')!=key[1]:
        raise Rejected('original observation changes its actual source ordinal')
    fact=original_fact(scope,owner,headers[key[0]],raw[key]['record'])
    voters={row['node_id'] for row in headers[key[0]]['normal_mounted_identity']['configured_members']
            if row['role']=='voter' and row['state']=='voter'}
    quorum(fact['authority'],voters)
    if fact['committed']['txn_response']['succeeded'] is False:
        raise Unavailable('actual original CAS-false receipt requires its exact no-effect/failure-branch coverage')
    matches=[(call_key,item['record']) for call_key,item in raw.items()
        if call_key[0]==key[0] and call_key[1]>key[1] and item['kind']=='control-commit'
        and item['record']['commit']['request']['request_id']==fact['request']['request_id']]
    if not matches: raise Unavailable('original certificate lacks its later actual native caller result')
    for call_key,record in matches:
        if record['owner_label']!=call_key[0] or number(record['owner_ordinal'],'original matching caller ordinal')!=call_key[1]:
            raise Rejected('original observation changes its actual caller source')
        if not original_matches_call(fact,record):
            raise Rejected('original observation differs from its actual caller full command/result')
        basis=record['commit']['result']['authority']['basis']
        if basis=='exact-committed-request':
            if record['commit']['result'].get('replayed_committed_request') is not True:
                raise Rejected('original association changes the actual replay classification')
        else:
            quorum(record['commit']['result']['authority'],voters)
    return dict(owner_id=key[0],owner_ordinal=str(key[1]),request_id=fact['request']['request_id'],
                caller_owner_ordinals=[str(call_key[1]) for call_key,_ in matches])


def native_receipt(record, voters, *, scope=None, owner=None, header=None, raw=None, key=None, original_sources=None):
    commit=record['commit'];request,result=commit['request'],commit['result']
    response,authority=result.get('txn_response'),result['authority']
    if (request['request_id']!=result['request_id'] or response is None or response.get('succeeded') is not True
            or number(response['revision'],'receipt revision')!=number(authority['revision'],'authority revision')):
        raise Rejected('bootstrap request lacks its exact current-term committed receipt')
    submitted=result.get('submitted_command_digest')
    if submitted is None or A.SUBTREE.PROJECTOR.native_digest_hex(submitted)!=A.SUBTREE.PROJECTOR.native_digest_hex(result['command_digest']):
        raise Rejected('bootstrap submitted/committed digest differs')
    if authority['basis']=='exact-committed-request':
        if result.get('replayed_committed_request') is not True:
            raise Rejected('exact replay basis lacks actual replay classification')
        if any(value is None for value in (scope,owner,header,raw,key)):
            raise Unavailable('exact replay lacks its original native source inputs')
        original=original_for_replay(scope,owner,header,raw,key,record,voters)
        if original_sources is not None: original_sources.append(dict(owner_id=original[0],owner_ordinal=str(original[1])))
    else:
        quorum(authority,voters)
        if all(value is not None for value in (scope,owner,header,raw,key)):
            original=original_for_replay(scope,owner,header,raw,key,record,voters,required=False)
            if original is not None and original_sources is not None:
                original_sources.append(dict(owner_id=original[0],owner_ordinal=str(original[1])))
    return request['txn']

def request_decodes(header, record):
    values = {}
    for observed in header.get("actual_request_value_decodes", []):
        if number(observed["source_owner_ordinal"], "decode source") != number(record["owner_ordinal"], "commit source"):
            continue
        value = A.validate_request_value_decode(header, record, observed)
        identity = (observed["request_section"], observed["field_index"])
        if identity in values:
            raise Rejected("bootstrap request field decoded twice")
        values[identity] = (observed["key"], observed["native_type"], value)
    if not values:
        raise Unavailable("bootstrap exact request-value normal decodes were not captured")
    return values

def frame_chain(owner, start, finish, raw, header, *, maximum_ordinal=None, scope=None):
    """Extend retained EXACT original native frames, not sequence subtraction."""
    start = native_cut(start); finish = native_cut(finish)
    frames = {}
    for (actor, ordinal), item in raw.items():
        if actor != owner or (maximum_ordinal is not None and ordinal >= maximum_ordinal):
            continue
        record = item["record"]
        if item["kind"] == "kernel" and record.get("exact_frame") is not None:
            entry = record["exact_entry"]
            decodes = [row["decoded_entry"] for row in header.get("actual_frame_decodes", [])
                       if number(row["owner_ordinal"], "decode ordinal") == ordinal]
            if decodes != [entry]:
                raise Rejected("bootstrap frame differs from its ordinary native decoder")
            candidates = [(entry, record["exact_frame"])]
        elif item["kind"] == "boundary" and record["facts"]["phase"] == "replica-journal-persisted":
            candidates = []
            for index, frame in enumerate(record["facts"]["command"]["batch"]["journal_frames"]):
                rows = [row["decoded_entry"] for row in header.get("actual_received_frame_decodes", [])
                        if number(row["source_owner_ordinal"], "received decoder ordinal") == ordinal and row["frame_index"] == index]
                if len(rows) != 1:
                    raise Unavailable("bootstrap receiver exact native frame decode absent")
                candidates.append((rows[0], frame["exact_bytes"]))
        else:
            continue
        for entry, exact in candidates:
            if scope is not None and (entry["shard_id"] != scope["shard_id"] or entry["mutation"]["file_system_id"] != scope["file_system_id"]):
                raise Rejected("bootstrap ancestry contains another native scope")
            sequence = number(entry["sequence"], "native frame slot")
            if not start["journal_sequence"] < sequence <= finish["journal_sequence"]:
                continue
            value = (entry, A.byte_vector(exact, "bootstrap exact frame"), ordinal)
            if sequence in frames and frames[sequence][:2] != value[:2]:
                raise Rejected("bootstrap owner retains conflicting frame at exact slot")
            frames.setdefault(sequence, value)
    prefix = start; witnesses = []
    for sequence in range(start["journal_sequence"] + 1, finish["journal_sequence"] + 1):
        if sequence not in frames:
            raise Unavailable("bootstrap freeze-to-initial anchor lacks retained frame")
        entry, exact, ordinal = frames[sequence]
        prefix = A.SUBTREE.PROJECTOR.extend_native_prefix(prefix, sequence, exact)
        witnesses.append(dict(owner_id=owner, owner_ordinal=str(ordinal), sequence=str(sequence),
                              exact_frame_sha256=hashlib.sha256(exact).hexdigest(), decoded_entry=entry))
    if native_cut(prefix) != finish:
        raise Rejected("bootstrap retained exact frames end at another initial anchor")
    return witnesses

def exact_scope_guards(txn,decoded,scope,retiring,pending_key,pending_expected,head_expected):
    """Check the normal four-key CAS boundary, including floor/head fences."""
    segment=scope['shard_id'].strip('/').replace('/','_');base='/dfs/filesystems/'+scope['file_system_id']+'/'
    config_key=base+'metadata-shards/'+segment;floor_key=base+'metadata-operation-floors/'+segment;head_key=base+'metadata-head-watermarks/'+segment
    fields={}
    for index,compare in enumerate(txn['compares']):
        if len(compare)!=1:raise Rejected('bootstrap CAS compare has no unique native operation')
        kind,body=next(iter(compare.items()));key=body['key']
        if key in (config_key,pending_key,floor_key,head_key):
            if key in fields:raise Rejected('bootstrap scope CAS repeats an exact key')
            fields[key]=(kind,body,decoded.get(('compare',index)))
    if set(fields)!={config_key,pending_key,floor_key,head_key}:
        raise Rejected('bootstrap CAS omits original configuration/reservation/floor/head guard')
    cfg=fields[config_key]
    if cfg[0]!='ValueEquals' or cfg[2] is None or cfg[2][2]!=retiring:
        raise Rejected('bootstrap CAS does not compare exact retiring configuration')
    pending=fields[pending_key]
    if pending_expected is None:
        if pending[0]!='Missing':raise Rejected('bootstrap initial reservation is not genuinely absent')
    elif pending[0]!='ValueEquals' or pending[2] is None or pending[2][2]!=pending_expected:
        raise Rejected('bootstrap CAS uses another original pending body')
    floor=fields[floor_key];floor_sequence=0
    if floor[0]=='ValueEquals':
        if floor[2] is None or floor[2][1]!='DfsMetadataShardOperationFloor':raise Unavailable('bootstrap floor lacks actual normal request decoder')
        value=floor[2][2];floor_sequence=number(value['prepared_sequence'],'guarded native operation floor')
        if number(value['authority_epoch'],'guarded floor epoch')>number(retiring['epoch'],'source epoch'):
            raise Rejected('bootstrap guarded operation floor belongs to a later source epoch')
    elif floor[0]!='Missing':raise Rejected('bootstrap floor lacks exact value/absence guard')
    head=fields[head_key]
    if head_expected is None:
        if head[0]!='Missing':raise Rejected('bootstrap replaces absent required watermark with another head')
    elif head[0]!='ValueEquals' or head[2] is None or head[2][2]!=head_expected:
        raise Rejected('bootstrap guarded original watermark differs from captured pending reservation')
    return floor_sequence


def verified_call(edge, owners, raw, verify_actual_edge):
    """Bind accepted progress to its SAME real authenticated served RPC return."""
    by_owner={row['owner_id']:row for row in owners}
    source,receiver,accepted,received=verify_actual_edge(edge)
    calls=[]
    for key,item in raw.items():
        if key[0]!=source[0] or key[1]>=source[1] or item['kind']!='boundary':continue
        fact=item['record']['facts']
        if A.journal_rpc_request_matches(fact,received['command'],by_owner[source[0]]['node_id'],by_owner[receiver[0]]['node_id']):calls.append((key,fact))
    if len(calls)!=1:raise Unavailable('initial native ACK lacks SAME actual source RPC-start')
    start,call=calls[0]
    returned=[(key,item['record']['facts']) for key,item in raw.items()
        if key[0]==source[0] and start[1]<key[1]<=source[1] and item['kind']=='boundary'
        and item['record']['facts']['phase']==A.journal_rpc_return_phase(call)
        and item['record']['facts']['attempt_id']==call['attempt_id']]
    if len(returned)!=1 or returned[0][1]['response'] is None:
        raise Unavailable('initial native ACK lacks SAME completed source RPC-return')
    returned_key,response=returned[0]
    A.verify_journal_rpc_return(call,response,accepted['response'],by_owner[source[0]]['node_id'],by_owner[receiver[0]]['node_id'])
    constructed=[key for key,item in raw.items() if key[0]==receiver[0] and key[1]>receiver[1] and item['kind']=='boundary'
        and item['record']['facts']['phase']=='replica-journal-response-constructed'
        and item['record']['facts']['response']==accepted['response']]
    if received['phase']=='replica-journal-persisted' and len(constructed)!=1:
        raise Unavailable('initial persisted receiver lacks unique actual response construction')
    return source,receiver,accepted,received,start,returned_key,constructed

def verify_initial_boundaries(scope,owners,headers,raw,initial_prefixes,initial_ordinals,correlations,baseline,completed,verify_actual_edge):
    """Certify each pre-finite receiver/lifecycle observation; never discard it."""
    covered={};proof=completed['proof'] if completed else None
    lifecycle=[]
    if completed:
        fields=('stage_native_commit_source','fence_native_commit_source','persisted_freeze_source',
                'activation_native_commit_source','activation_source')
        lifecycle=[completed[field] for field in fields]+completed['successor_rebind_sources']
        for ref in lifecycle:
            key=(ref['owner_id'],number(ref['owner_ordinal'],'initial lifecycle ordinal'))
            covered[key]=dict(reason='independently verified completed handoff',source=ref)
    for edge in correlations:
        source=(edge['source_owner_id'],number(edge['source_ordinal'],'initial ACK ordinal'))
        receiver=(edge['receiver_owner_id'],number(edge['receiver_ordinal'],'initial receiver ordinal'))
        if source[1]>=initial_ordinals[source[0]]:continue
        if receiver[1]>=initial_ordinals[receiver[0]]:
            raise Rejected('initial source ACK uses a receiver cut after its declared initial state')
        source,receiver,ack,recv,start,returned,constructed=verified_call(edge,owners,raw,verify_actual_edge)
        command=recv['command'];probe=command.get('authority_probe');pending=recv.get('actual_pending_handoff')
        actual_handoff=proof is not None and pending is not None and pending['handoff_id']==proof['handoff_id']
        if actual_handoff:
            if pending['retiring_shard']!=proof['retiring_shard'] or pending['desired_shard']!=proof['desired_shard'] or pending['cause']!=proof['cause']:
                raise Rejected('initial ACK uses another original pending handoff body')
        elif probe is not None:
            operation=probe['operation'];retained=baseline.get(operation.get('operation_id'))
            fields=('file_system_id','shard_id','owner_epoch','prepare_sequence','payload_digest','operation_id')
            if (operation.get('kind')!='prepared-operation' or retained is None
                    or any(operation.get(field)!=retained['digest'][field] for field in fields)):
                raise Rejected('initial receiver probe lacks its typed immutable committed baseline operation')
        elif recv['phase']!='replica-journal-persisted':
            raise Unavailable('initial prefix observation lacks verified handoff or baseline operation binding')
        if recv['phase']=='replica-journal-persisted':
            for index,wire in enumerate(command['batch']['journal_frames']):
                decoded=[row['decoded_entry'] for row in headers[receiver[0]].get('actual_received_frame_decodes',[])
                    if number(row['source_owner_ordinal'],'initial received ordinal')==receiver[1] and row['frame_index']==index]
                if len(decoded)!=1:raise Unavailable('initial receiver lacks exact normal frame decoder')
                mutation=decoded[0]['mutation'];digest=mutation.get('digest')
                if mutation.get('type') not in ('prepare-operation-v7','commit-prepared-operation-v7'):
                    raise Unavailable('initial journal contains a non-prepared current operation family')
                opid=mutation.get('operation_id') or (digest or {}).get('operation_id')
                retained=baseline.get(opid)
                if retained is None or (digest is not None and digest!=retained['digest']) or (mutation.get('type')=='prepare-operation-v7' and (mutation['record']['digest']!=retained['digest'] or mutation['record']['payload']!=retained['payload'])):
                    raise Rejected('initial receiver frame lacks exact committed baseline identity')
            prior=A.native_json(A.byte_vector(raw[receiver]['record']['before_held_json'],'initial receiver prior'),'actual initial receiver prior')
            frame_chain(receiver[0],prior['certified_prefix'],recv['durable_prefix'],raw,headers[receiver[0]],
                        maximum_ordinal=receiver[1]+1,scope=scope)
        refs=[source,receiver,start,returned]+constructed
        for key in refs:
            if key[1]>=initial_ordinals[key[0]]:raise Rejected('initial served response belongs after finite initial cut')
            covered[key]=dict(reason='same actual baseline/handoff journal RPC',request_id=edge['request_id'])
    for key,item in raw.items():
        if item['kind']!='boundary' or key[1]>=initial_ordinals[key[0]] or item['record']['facts']['phase']!='replica-materialization-completed':continue
        fact=item['record']['facts']
        if fact['file_system_id']!=scope['file_system_id'] or fact['shard_id']!=scope['shard_id']:
            raise Rejected('initial materialization crosses scope')
        before=A.native_json(A.byte_vector(item['record']['before_held_json'],'initial materialization prior'),'actual materialization prior')
        after=A.native_json(A.byte_vector(item['record']['after_held_json'],'initial materialization next'),'actual materialization next')
        prior=number(before['materialized_through_sequence'],'initial materialized prior');next_=number(after['materialized_through_sequence'],'initial materialized next')
        if next_<prior or number(fact['actual_materialized_entries'],'actual entry count')!=next_-prior:
            raise Rejected('initial materialization count differs from actual bounded table advancement')
        for row in after['tables'].get('prepared_operations_v7',[]):
            retained=baseline.get(row['digest']['operation_id'])
            if retained is None or retained['digest']!=row['digest'] or retained['payload']!=row['payload']:
                raise Rejected('initial materialization introduces unbound or changed immutable work')
        frame_chain(key[0],before['certified_prefix'],after['certified_prefix'],raw,headers[key[0]],maximum_ordinal=key[1],scope=scope)
        covered[key]=dict(reason='actual bounded materialization of retained baseline frames')
    return [dict(owner_id=key[0],owner_ordinal=str(key[1]),**value) for key,value in sorted(covered.items())]

def verify_frozen_ack(scope, handoff, pre_fence_pending, prefix, accepted, received, receiver_record):
    """Validate a typed actual ACK; only a prefix probe witnesses handoff."""
    command, response = received["command"], accepted["response"]
    retiring = handoff["retiring_shard"]
    if (received.get("actual_pending_handoff") not in (handoff, pre_fence_pending)
            or received.get("actual_source_config") != retiring
            or command["file_system_id"] != scope["file_system_id"]
            or command["source_node_id"] != retiring["primary_node_id"]
            or command["batch"]["shard_id"] != scope["shard_id"]
            or number(command["authority_epoch"], "ACK source epoch") != number(retiring["epoch"], "retiring epoch")):
        raise Rejected("bootstrap ACK lacks exact actual pending/source binding")
    probe, durable = command.get("prefix_probe"), command.get("durable_replication")
    prefix_only = command["verify_prefix_only"]
    if prefix_only is True:
        if (received["phase"] != "replica-prefix-probe-accepted"
                or received["actual_pending_handoff"] != handoff or probe is None
                or command.get("authority_probe") is not None or durable is not None
                or command["batch"]["journal_frames"]
                or probe["file_system_id"] != scope["file_system_id"] or probe["shard_id"] != scope["shard_id"]
                or number(probe["authority_epoch"], "probe source epoch") != number(retiring["epoch"], "retiring epoch")
                or number(probe["journal_sequence"], "probe prefix") != prefix["journal_sequence"]
                or probe["state_fingerprint"] != prefix["digest"]):
            raise Rejected("bootstrap prefix ACK lacks exact actual pending/source/prefix binding")
        confirmation = probe["primary_handoff_id"]
        if confirmation is not None and confirmation != handoff["handoff_id"]:
            raise Rejected("bootstrap primary-only confirmation names another handoff")
    elif prefix_only is False:
        if (received["phase"] != "replica-journal-persisted" or probe is not None
                or command.get("authority_probe") is not None or not isinstance(durable, dict)
                or not isinstance(durable.get("confirmation_id"), str) or not durable["confirmation_id"]
                or not command["batch"]["journal_frames"] or received.get("durable_prefix") is None):
            raise Rejected("bootstrap journal ACK lacks its exact durable confirmation purpose")
        confirmation = durable["confirmation_id"]
    else:
        raise Rejected("bootstrap ACK has no actual boolean confirmation purpose")
    held = A.native_json(A.byte_vector(receiver_record["after_held_json"], "bootstrap receiver held cut"),
                         "actual bootstrap receiver cut")
    receiver_prefix = received.get("durable_prefix") or held["certified_prefix"]
    if (accepted["expected_confirmation_id"] != confirmation
            or response["verified_authority_probe_id"] != confirmation
            or accepted["expected_prefix_only"] is not prefix_only
            or response["verified_prefix_only"] is not prefix_only
            or number(command["batch"]["next_after_sequence"], "command ACK prefix") != prefix["journal_sequence"]
            or number(response["next_after_sequence"], "ACK prefix") != prefix["journal_sequence"]
            or native_cut(receiver_prefix) != prefix):
        raise Rejected("bootstrap ACK does not attest its exact original frozen prefix")
    return prefix_only


def verify_completed_handoff(scope, owners, headers, raw, initial_prefixes, initial_ordinals, correlations, verify_actual_edge):
    """Return verified historical DATA for an initial cut; no absence/permission."""
    by_owner = {owner["owner_id"]: owner for owner in owners}
    candidates = []
    for key, item in raw.items():
        if item["kind"] != "boundary" or key[1] >= initial_ordinals[key[0]]:
            continue
        if item["record"]["facts"]["phase"] == "handoff-activation-completed":
            candidates.append((key, item["record"]["facts"]))
    if len(candidates) != 1:
        raise Unavailable("initial completed bootstrap requires one exact actual activation")
    (source, activation_ordinal), activation = candidates[0]
    handoff, proof = activation["handoff"], activation["proof"]
    retiring, desired = handoff["retiring_shard"], handoff["desired_shard"]
    if not scoped(retiring, scope) or not scoped(desired, scope) or any(proof[field] != handoff[field] for field in ("cause", "handoff_id", "retiring_shard", "desired_shard")):
        raise Rejected("bootstrap activation proof borrows another scope/body/cause")
    if number(retiring["epoch"], "retiring epoch") + 1 != number(desired["epoch"], "desired epoch"):
        raise Rejected("bootstrap activation did not advance exact source epoch")
    if by_owner[source]["node_id"] != retiring["primary_node_id"]:
        raise Rejected("bootstrap native activation has another actual source owner")
    old, new = members(retiring), members(desired)
    ack = set(proof["acknowledged_node_ids"])
    if (len(ack) != len(proof["acknowledged_node_ids"]) or not ack <= old|new
            or len(ack&old) < len(old)//2+1 or len(ack&new) < len(new)//2+1
            or not {retiring["primary_node_id"], desired["primary_node_id"]} <= ack):
        raise Rejected("bootstrap lacks exact both-majority and both-primary proof")
    prefix = dict(version=2, journal_sequence=number(proof["journal_sequence"], "frozen prefix"), digest=proof["state_fingerprint"])
    freezes = [(key, item["record"]["facts"]) for key,item in raw.items()
               if key[0] == source and key[1] < activation_ordinal and item["kind"] == "boundary"
               and item["record"]["facts"]["phase"] == "retiring-freeze-persisted"
               and item["record"]["facts"]["pending"]["handoff_id"] == handoff["handoff_id"]]
    if len(freezes) != 1:
        raise Unavailable("bootstrap actual persisted freeze missing/ambiguous")
    freeze_key, persisted = freezes[0]; freeze = persisted["freeze"]
    prior_pending=dict(persisted['pending']);current_pending=dict(handoff)
    prior_fence=prior_pending.pop('retiring_primary_fence',None);active_fence=current_pending.pop('retiring_primary_fence',None)
    expected_fence=dict(handoff_id=handoff['handoff_id'],primary_node_id=retiring['primary_node_id'],
        journal_sequence=prefix['journal_sequence'],state_fingerprint=prefix['digest'])
    if prior_fence is not None or prior_pending!=current_pending or active_fence!=expected_fence:
        raise Rejected('bootstrap fencing changed original reservation or lacks exact persisted source fence')
    if number(handoff['required_prepared_sequence'],'required prepared prefix')>prefix['journal_sequence']:
        raise Rejected('bootstrap persisted prefix is below its original captured requirement')
    # The normal persisted-freeze boundary owns this marker fact. The held
    # table getter may explicitly mark the on-disk marker NotObserved while
    # the writer is held; keep that observation, never convert it to absence.
    observed_after=A.native_json(A.byte_vector(raw[freeze_key]['record']['after_held_json'],'persisted freeze held source'),'actual persisted freeze held source')
    if (observed_after['backend_root']!=by_owner[source]['canonical_root'] or observed_after['shard_id']!=scope['shard_id']
            or observed_after['write_freeze'] not in ({'observation':'not-observed'},{'observation':'present','value':freeze})
            or native_cut(observed_after['certified_prefix'])!=prefix):
        raise Rejected('bootstrap freeze differs from its actual persisted held marker/cut')
    if (any(persisted["pending"][field] != handoff[field] for field in ("cause", "handoff_id", "retiring_shard", "desired_shard"))
            or freeze["handoff_id"] != handoff["handoff_id"]
            or number(freeze["retiring_epoch"], "freeze epoch") != number(retiring["epoch"], "old epoch")
            or native_cut(freeze["rebind_prefix_proof"]) != prefix):
        raise Rejected("bootstrap persisted freeze differs from exact activated cut")
    rebound = [(key,item["record"]["facts"]) for key,item in raw.items()
               if key[0] == source and activation_ordinal < key[1] < initial_ordinals[source]
               and item["kind"] == "boundary" and item["record"]["facts"]["phase"] == "successor-rebound"
               and item["record"]["facts"]["proof"] == proof]
    if not rebound:
        raise Unavailable("bootstrap successor was not actually rebound before initial cut")
    for key,fact in rebound:
        marker = fact["original_freeze"]
        after=A.native_json(A.byte_vector(raw[key]['record']['after_held_json'],'actual successor held cut'),'actual rebound held cut')
        if (after['backend_root']!=by_owner[source]['canonical_root'] or after['shard_id']!=scope['shard_id']
                or number(after['backend_instance_authority_epoch'],'actual rebound instance epoch')!=number(desired['epoch'],'activated epoch')
                or after['write_freeze'] not in ({'observation':'not-observed'},{'observation':'present','value':marker})):
            raise Rejected('bootstrap successor observation lacks matching actual held instance/marker')
        if (fact["active"] != desired or marker["handoff_id"] != handoff["handoff_id"]
                or number(marker["activated_epoch"], "rebound epoch") != number(desired["epoch"], "desired epoch")
                or native_cut(marker["rebind_prefix_proof"]) != prefix):
            raise Rejected("bootstrap rebind belongs to another marker/configuration/prefix")
    # The ordinary shared context matcher independently verifies each exact
    # authenticated source/receiver request-response tuple and served return.
    # Owner ordinals are local; only these real call edges order other owners.
    ack_sources = []
    witnessed = {retiring["primary_node_id"]}
    for edge in correlations:
        if edge["source_owner_id"] != source:
            continue
        source_ordinal = number(edge["source_ordinal"], "source ACK ordinal")
        if not freeze_key[1] < source_ordinal < activation_ordinal:
            continue
        source_key, receiver_key, accepted, received, call_key, return_key, _ = verified_call(edge,owners,raw,verify_actual_edge)
        command = received["command"]; response = accepted["response"]
        pending = received.get("actual_pending_handoff")
        if pending not in (handoff, persisted["pending"]):
            if pending is not None and pending.get("handoff_id") == handoff["handoff_id"]:
                raise Rejected("bootstrap ACK changes its original pending handoff")
            continue
        prefix_only = verify_frozen_ack(scope, handoff, persisted["pending"], prefix,
                                        accepted, received, raw[receiver_key]["record"])
        if call_key[1] <= freeze_key[1]:
            raise Rejected('bootstrap confirming RPC began before its actual persisted source freeze')
        if not prefix_only:
            # Catch-up has its own durable confirmation; it is not a handoff
            # prefix witness. Retain and verify its SAME normal decoded frames.
            prior = A.native_json(A.byte_vector(raw[receiver_key]["record"]["before_held_json"],
                "bootstrap journal receiver prior"), "actual bootstrap journal receiver prior")
            frame_chain(receiver_key[0], prior["certified_prefix"], received["durable_prefix"],
                        raw, headers[receiver_key[0]], maximum_ordinal=receiver_key[1]+1, scope=scope)
            continue
        witnessed.add(accepted["authenticated_source_node_id"])
        ack_sources.append(dict(source_owner_id=source_key[0],source_ordinal=str(source_key[1]),
                                receiver_owner_id=receiver_key[0],receiver_ordinal=str(receiver_key[1]),
                                request_id=edge["request_id"],rpc_start_ordinal=str(call_key[1]),rpc_return_ordinal=str(return_key[1])))
    if not ack <= witnessed:
        raise Unavailable("bootstrap proof contains ACK identities without actual correlated owner returns")
    segment = scope["shard_id"].strip("/").replace("/", "_")
    base = "/dfs/filesystems/" + scope["file_system_id"] + "/"
    config_key = base+"metadata-shards/"+segment
    pending_key = base+"metadata-membership-handoffs/"+segment
    proof_key = base+"metadata-membership-handoff-proofs/"+segment
    head_key = base+"metadata-head-watermarks/"+segment
    required_head = persisted["pending"].get("required_head_watermark")
    voter_rows = headers[source]["normal_mounted_identity"]["configured_members"]
    voters = {row["node_id"] for row in voter_rows if row["role"] == "voter" and row["state"] == "voter"}
    commits = []; stages = []; fences = []; original_sources=[]
    for key,item in raw.items():
        if key[0] != source or key[1] >= activation_ordinal or item["kind"] != "control-commit":
            continue
        record = item["record"]; txn = native_receipt(record,voters,scope=scope,owner=by_owner[source],
            header=headers[source],raw=raw,key=key,original_sources=original_sources)
        decoded = request_decodes(headers[source],record)
        effects = {key: value for (section,_),(key,_,value) in decoded.items() if section == "success"}
        pending_effect = effects.get(pending_key)
        compared = {key:value for (section,_),(key,_,value) in decoded.items() if section == "compare"}
        if pending_effect == persisted["pending"]:
            if (set(effects) != {pending_key} or len(txn["success"]) != 1
                    or compared.get(config_key) != retiring
                    or not any(set(compare) == {"Missing"} and compare["Missing"]["key"] == pending_key
                               for compare in txn["compares"]) or txn.get("failure")):
                raise Rejected("bootstrap stage lacks exact old configuration/absent reservation CAS")
            floor = exact_scope_guards(txn,decoded,scope,retiring,pending_key,None,required_head)
            required=max(floor,number(required_head["journal_sequence"],"original required head") if required_head else 0)
            if required!=number(persisted["pending"]["required_prepared_sequence"],"captured required cut"):
                raise Rejected("bootstrap staged required cut differs from guarded original floor/head")
            stages.append(key)
        if pending_effect == handoff:
            if (set(effects) != {pending_key} or len(txn["success"]) != 1
                    or compared.get(config_key) != retiring or compared.get(pending_key) != persisted["pending"]
                    or txn.get("failure")):
                raise Rejected("bootstrap freeze request lacks exact original stage/current configuration CAS")
            floor=exact_scope_guards(txn,decoded,scope,retiring,pending_key,persisted["pending"],required_head)
            if floor>prefix["journal_sequence"]:raise Rejected("bootstrap frozen cut is below the guarded operation floor")
            fences.append(key)
        if effects.get(config_key) == desired and effects.get(proof_key) == proof:
            if (set(effects) != {config_key,proof_key,head_key}
                    or len(txn["success"]) != 4
                    or {op["Delete"]["key"] for op in txn["success"] if set(op)=={"Delete"}} != {pending_key}
                    or txn.get("failure")):
                raise Rejected("bootstrap activation request has another complete effect set")
            compared = {key:value for (section,_),(key,_,value) in decoded.items() if section == "compare"}
            if compared.get(config_key) != retiring or compared.get(pending_key) != handoff:
                raise Rejected("bootstrap activation lacks exact current source/pending CAS")
            watermark=effects[head_key]
            if (any(watermark[field]!=scope[field] for field in ("file_system_id","shard_id"))
                    or watermark["primary_node_id"] != desired["primary_node_id"]
                    or watermark["journal_sequence"] != proof["journal_sequence"]
                    or watermark["state_fingerprint"] != proof["state_fingerprint"]):
                raise Rejected("bootstrap activated watermark differs from actual frozen proof")
            floor=exact_scope_guards(txn,decoded,scope,retiring,pending_key,handoff,required_head)
            if floor>prefix["journal_sequence"]:raise Rejected("bootstrap activation is below the guarded operation floor")
            if number(watermark["epoch"],"activated watermark epoch")!=number(desired["epoch"],"desired epoch") or set(watermark["acknowledged_node_ids"])!=ack&new:
                raise Rejected("bootstrap watermark changes desired epoch or actual ACK identities")
            commits.append(key)
    if len(commits) != 1 or len(stages) != 1 or len(fences) != 1:
        raise Unavailable("bootstrap stage/fence/activation lacks unique exact native KV request/results")
    if not stages[0][1] < freeze_key[1] < fences[0][1] < commits[0][1] < activation_ordinal:
        raise Rejected("bootstrap native stage/freeze/fence/activation owner order differs")
    ancestry = {owner: frame_chain(owner,prefix,initial_prefixes[owner],raw,headers[owner],
                                  maximum_ordinal=initial_ordinals[owner],scope=scope)
                for owner in by_owner}
    return dict(scope=scope, historical_only=True, original_marker_not_absent=True,
                source_owner_id=source, authenticated_ack_sources=ack_sources,
                original_native_commit_sources=original_sources,
                stage_native_commit_source=dict(owner_id=source,owner_ordinal=str(stages[0][1])),
                fence_native_commit_source=dict(owner_id=source,owner_ordinal=str(fences[0][1])),
                persisted_freeze_source=dict(owner_id=freeze_key[0],owner_ordinal=str(freeze_key[1])),
                activation_source=dict(owner_id=source,owner_ordinal=str(activation_ordinal)),
                activation_native_commit_source=dict(owner_id=source,owner_ordinal=str(commits[0][1])),
                successor_rebind_sources=[dict(owner_id=key[0],owner_ordinal=str(key[1])) for key,_ in rebound],
                proof=proof, frozen_prefix=prefix, retained_ancestry_by_owner=ancestry)


def verify_auxiliary_rebind(scope,owners,headers,raw,key,completed):
    """A fresh handle can adopt already completed DATA, never author capability."""
    if completed is None:raise Unavailable('fresh-handle adoption lacks independently verified prior native completion')
    item=raw[key]
    if item['kind']!='boundary' or item['record']['facts']['phase']!='successor-rebound':
        raise Rejected('auxiliary rebind certificate points to another actual boundary')
    fact=item['record']['facts'];proof=completed['proof'];desired=proof['desired_shard']
    owner=next(row for row in owners if row['owner_id']==key[0])
    if fact['proof']!=proof or fact['active']!=desired or owner['node_id']!=desired['primary_node_id']:
        raise Rejected('fresh-handle adoption changes the exact previously completed configuration/proof/owner')
    before=A.native_json(A.byte_vector(item['record']['before_held_json'],'fresh-handle prior'),'actual fresh-handle prior')
    after=A.native_json(A.byte_vector(item['record']['after_held_json'],'fresh-handle next'),'actual fresh-handle next')
    epoch=number(desired['epoch'],'completed native epoch');prior=before['backend_instance_authority_epoch']
    if (fact['prior_instance_epoch']!=prior or (prior is not None and number(prior,'prior instance epoch')!=epoch)
            or number(after['backend_instance_authority_epoch'],'adopted instance epoch')!=epoch):
        raise Rejected('auxiliary handle adoption changes a live former-author instance rather than fresh/current DATA')
    if (before['backend_root']!=owner['canonical_root'] or before['shard_id']!=scope['shard_id']
            or {k:v for k,v in before.items() if k!='backend_instance_authority_epoch'}
                !={k:v for k,v in after.items() if k!='backend_instance_authority_epoch'}):
        raise Rejected('auxiliary handle adoption changed native visibility/prefix/writer custody or another held fact')
    marker=fact['original_freeze']
    if (marker['handoff_id']!=proof['handoff_id'] or number(marker['retiring_epoch'],'original retiring epoch')!=number(proof['retiring_shard']['epoch'],'retired source epoch')
            or number(marker['activated_epoch'],'original activated epoch')!=epoch
            or marker['journal_sequence']!=proof['journal_sequence'] or marker['state_fingerprint']!=proof['state_fingerprint']):
        raise Rejected('auxiliary handle adoption borrows another original freeze/activation')
    witnesses=frame_chain(key[0],completed['frozen_prefix'],after['certified_prefix'],raw,headers[key[0]],maximum_ordinal=key[1],scope=scope)
    marker_prefix=native_cut(marker['rebind_prefix_proof'])
    if marker_prefix!=native_cut(completed['frozen_prefix']) and marker_prefix!=native_cut(after['certified_prefix']):
        raise Unavailable('auxiliary marker has an intermediate extension without separately retained prefix evidence')
    return dict(owner_id=key[0],owner_ordinal=str(key[1]),historical_only=True,no_writer_capability_minted=True,
        actual_boundary_fact=fact,retained_ancestry=witnesses)
