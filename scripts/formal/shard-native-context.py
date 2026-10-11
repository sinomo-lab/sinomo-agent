#!/usr/bin/env python3
"""Construct bounded operational facts from actual, drained Rust observations.

This adapter creates no model states. The normal checker independently projects
its native cuts and asks Quint to validate every transition. Up to four actual
owners and two immutable operations are selected from complete native captures.
"""
import argparse
import copy
import importlib.util
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
while not (HERE/'assemble-shard-native-trace.py').is_file():
    workspace = HERE
    while workspace != workspace.parent and not (workspace/'scripts/formal/assemble-shard-native-trace.py').is_file():
        workspace = workspace.parent
    if workspace == workspace.parent:
        raise RuntimeError('native formal verifier is unavailable')
    HERE = workspace/'scripts/formal'

SPEC = importlib.util.spec_from_file_location('native_assembler', HERE/'assemble-shard-native-trace.py')
ASSEMBLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ASSEMBLER)
Rejected, Unavailable = ASSEMBLER.Rejected, ASSEMBLER.Unavailable
CAUSAL_SPEC = importlib.util.spec_from_file_location('context_causality', HERE/'shard-native-causality.py')
CAUSAL = importlib.util.module_from_spec(CAUSAL_SPEC)
CAUSAL_SPEC.loader.exec_module(CAUSAL)


def decimal(value, label):
    if type(value) is int:
        if not 0 <= value <= 2**64-1: raise Rejected(f'{label}: u64 outside native range')
        return value
    return ASSEMBLER.CHECK.decimal_u64(value,label)


class NativeContexts:
    """Build facts from actual scoped packets; never select a predicted poststate."""
    def __init__(self, inputs, skeleton):
        self.inputs=inputs; self.header=copy.deepcopy(skeleton)
        artifact_source=inputs.get('artifact_source_inputs')
        if artifact_source is None:
            raise Unavailable('actual capture/source-manifest/executable verification inputs missing')
        binding_spec=importlib.util.spec_from_file_location('context_native_binding',HERE/'native-artifact-binding.py')
        binding_module=importlib.util.module_from_spec(binding_spec);binding_spec.loader.exec_module(binding_module)
        verified,manifest=binding_module.verify_manifest(artifact_source['manifest_path'],artifact_source['source_manifest_path'],artifact_source.get('executable'))
        if verified!=inputs['artifact_binding']:
            raise Rejected('actual capture executable/source manifest changed after packaging')
        self.owners={row['owner_id']:row for row in self.header['owners']}
        self.scope=self.header['scope']; self.blobs=ASSEMBLER.Blobs(self.header['blobs'])
        self.operations=self.header['finite_dictionary']['operation_aliases']
        if not 1<=len(self.owners)<=4 or not 1<=len(self.operations)<=3:
            raise Unavailable('native bound is one to four owners and one to three actual operations')
        if set(self.operations)!=set(inputs['selected_operation_ids']) or inputs['scope']!=self.scope:
            raise Rejected('actual selection differs from finite operation/scope dictionary')
        actual={row['owner_id']:row for row in inputs['owners']}
        captured={row['node_id']:row for row in manifest['captures']}
        manifest_root=Path(artifact_source['manifest_path']).parent
        for row in actual.values():
            original=captured.get(row['node_id'])
            if original is None or original['actual_data_root']!=row['actual_data_root']:
                raise Rejected('packaged owner is absent from actual manifest roster')
            if (Path(row['raw_path']).resolve()!=(manifest_root/original['raw_file']).resolve()
                    or Path(row['header_path']).resolve()!=(manifest_root/original['header_file']).resolve()):
                raise Rejected('packaged owner borrows files outside its actual capture manifest')
        if set(actual)!=set(self.owners): raise Rejected('actual capture roster differs from owner dictionary')
        self.artifact=self.native(self.header['source_artifact_binding_blob'])
        if self.artifact!=inputs['artifact_binding']:
            raise Rejected('current full six-field binary/source binding differs')
        for oid,owner in self.owners.items():
            row=actual[oid]
            if owner['component_incarnation']!=oid or owner['node_id']!=row['node_id']:
                raise Rejected('owner alias borrows another observed mount/node')
            if not owner['canonical_root'].startswith(row['actual_data_root']+'/'):
                raise Rejected('owner physical backend is outside the actual mounted root')
        self.raw_bytes={oid:Path(row['raw_path']).read_bytes() for oid,row in actual.items()}
        self.raw_headers={oid:ASSEMBLER.CHECK.read_json(Path(row['header_path'])) for oid,row in actual.items()}
        self.raw_captures={oid:ASSEMBLER.CHECK.read_json(Path(row['raw_path'])) for oid,row in actual.items()}
        import hashlib
        for oid,row in actual.items():
            if (hashlib.sha256(Path(row['header_path']).read_bytes()).hexdigest()!=row['header_sha256']
                    or hashlib.sha256(Path(row['raw_path']).read_bytes()).hexdigest()!=row['raw_sha256']):
                raise Rejected('actual packet/header file changed after packaging')
            header=self.raw_headers[oid]; raw=self.raw_captures[oid]
            if header['artifact_binding']!=self.artifact or header['actual_mount_label']!=oid or raw['actual_mount_label']!=oid:
                raise Rejected('raw input borrows another build/owner')
            if not raw['closed'] or not raw['actual_owner_stopped'] or any(decimal(raw[k],k) for k in ('dropped','incomplete','in_flight')):
                raise Unavailable('actual native owner did not drain with complete observations')
        self.events={}
        for event in inputs['owner_local_events']:
            oid=event['owner_id']; n=decimal(event['owner_ordinal'],'owner ordinal')
            actual_row=self.raw_captures[oid][event['collection']][event['index']]
            if actual_row!=event['record'] or actual_row['owner_label']!=oid or decimal(actual_row['owner_ordinal'],'raw ordinal')!=n:
                raise Rejected('packaged event differs from actual captured packet')
            if (oid,n) in self.events: raise Rejected('duplicate owner-local native observation')
            self.events[oid,n]=event
        for oid in self.owners:
            observed=sorted(n for owner,n in self.events if owner==oid)
            actual_ordinals={decimal(row['owner_ordinal'],'actual raw ordinal')
                for collection in ('packets','authoritative_reads','guard_events','content_receipts',
                    'configuration_cuts','metadata_cache_cuts','boundary_packets','control_commits','original_control_commits')
                for row in self.raw_captures[oid].get(collection,[])}
            if observed!=list(range(1,len(observed)+1)) or set(observed)!=actual_ordinals:
                raise Rejected('actual owner-local native observations are missing/discontinuous')
        self.cut_refs={}; self.memory_sources={}; self.control={}; self.configs={}; self.contexts={}
        self.prefix_items=self.header.get('semantic_prefix_items_by_owner')
        if self.prefix_items is None:
            if len(self.owners)!=1: raise Unavailable('multi-owner cuts require exact per-owner frame dictionaries')
            self.prefix_items={next(iter(self.owners)):self.header['semantic_prefix_items']}
        self.bodies={}
        for op,rows in inputs['operation_bodies'].items():
            if not rows: raise Unavailable('selected operation lacks actual native body')
            body=rows[0]; encoded=body['canonical_payload_json_utf8'].encode()
            ASSEMBLER.SUBTREE.canonical_prepared_payload(encoded,[],body['native_digest']['payload_digest'])
            payload=ASSEMBLER.native_json(encoded,'actual selected payload')
            if payload['operation_id']!=op or any(payload[k]!=self.scope[k] for k in ('file_system_id','shard_id')):
                raise Rejected('selected immutable payload crosses actual scope')
            if any(row['native_digest']!=body['native_digest'] or row['canonical_payload_json_utf8']!=body['canonical_payload_json_utf8'] for row in rows):
                raise Rejected('selected immutable payload changed across actual captures')
            ASSEMBLER.SUBTREE.PROJECTOR.sealed_content_requirement(payload, body['native_digest'], encoded)
            self.bodies[op]=(payload,body['native_digest'],self.blobs.put('DfsPreparedOperationPayloadCanonicalV7',encoded))

    def verified_edge(self,edge):
        source=(edge['source_owner_id'],decimal(edge['source_ordinal'],'source ACK ordinal'))
        recipient=(edge['receiver_owner_id'],decimal(edge['receiver_ordinal'],'receiver ordinal'))
        sent=self.events.get(source);received=self.events.get(recipient)
        if sent is None or received is None or sent['collection']!='boundary_packets' or received['collection']!='boundary_packets':
            raise Rejected('causal edge lacks actual source/receiver boundary packets')
        ack=sent['record']['facts'];recv=received['record']['facts']
        if ack['phase']!='source-journal-response-accepted' or recv['phase'] not in ('replica-journal-persisted','replica-prefix-probe-accepted'):
            raise Rejected('causal edge pairs incompatible actual phases')
        response=ack['response'];command=recv['command'];batch=command['batch']
        if (edge['request_id']!=response['request_id'] or response['request_id']!=command['request_id']
                or edge['actual_source_response']!=response or response['result']!='applied'
                or self.owners[recipient[0]]['node_id']!=ack['authenticated_source_node_id']
                or self.owners[source[0]]['node_id']!=command['source_node_id']
                or command['file_system_id']!=self.scope['file_system_id']
                or response['shard_id']!=self.scope['shard_id'] or batch['shard_id']!=self.scope['shard_id']
                or decimal(response['next_after_sequence'],'response head')!=decimal(batch['next_after_sequence'],'command head')
                or ack['expected_target_node_id']!=ack['authenticated_source_node_id']
                or decimal(ack['expected_next_after_sequence'],'expected head')!=decimal(response['next_after_sequence'],'response head')
                or ack['expected_prefix_only']!=response['verified_prefix_only']
                or ack['expected_confirmation_id']!=response['verified_authority_probe_id']):
            raise Rejected('causal edge changed actual native request/recipient/scope/cut/proof identity')
        if recv['phase']=='replica-prefix-probe-accepted':
            if recv['response']!=response: raise Rejected('source ACK differs from exact accepted receiver prefix response')
        else:
            constructed=[event['record']['facts']['response'] for (oid,n),event in self.events.items()
                if oid==recipient[0] and n>recipient[1] and event['collection']=='boundary_packets'
                and event['record']['facts']['phase']=='replica-journal-response-constructed'
                and event['record']['facts']['response']['request_id']==command['request_id']]
            if response not in constructed: raise Unavailable('durable receiver report lacks its actual ACK-floor-complete response construction')
        return source,recipient,ack,recv

    def native(self, reference):
        item=self.blobs.values[reference]
        if item['encoding']!='json-utf8': raise Rejected('native context fact is not exact JSON')
        return ASSEMBLER.native_json(item['data'].encode(),item['native_type'])

    def certificate(self, **fields):
        return dict(scope=self.scope,implementation_revision=self.artifact['implementation_revision'],
                    owner_bindings=list(self.owners.values()),complete=True,continuous=True,**fields)

    def prefix(self, value):
        if value is None: raise Unavailable('actual native prefix missing; empty genesis may not be inferred')
        result=dict(version=value['version'],journal_sequence=str(decimal(value['journal_sequence'],'native sequence')),digest=value['digest'])
        exact=dict(version=value['version'],journal_sequence=decimal(value['journal_sequence'],'native sequence'),digest=value['digest'])
        result['native_prefix_blob']=self.blobs.json('DfsMetadataPrefixProof',exact)
        return result

    def config(self, cfg, encoded=None):
        import hashlib
        allowed={'file_system_id','shard_id','primary_node_id','replica_node_ids','epoch','assignment','default_policy'}
        if set(cfg)-allowed or not allowed-{'replica_node_ids'}<=set(cfg):
            raise Rejected('normal config decoder did not supply complete current schema')
        if any(cfg[k]!=self.scope[k] for k in ('file_system_id','shard_id')):
            raise Rejected('actual config belongs to another scope')
        # Without an exact SOBJ row, use an existing dictionary tuple only; no guessed fingerprint.
        if encoded is None:
            for value in self.header.get('config_tuples',[]) + [self.header.get('actual_config_tuple',{})]:
                if value and self.native(value['native_config_blob'])==cfg:
                    self.configs[value['canonical_fingerprint']]=cfg
                    return value
            raise Unavailable('actual complete config lacks exact encoded row/dictionary identity')
        fp='native-config-sha256:'+hashlib.sha256(ASSEMBLER.byte_vector(encoded,'config bytes')).hexdigest()
        for known in self.header.get('config_tuples',[]) + [self.header.get('actual_config_tuple',{})]:
            if known and known['canonical_fingerprint']==fp and self.native(known['native_config_blob'])!=cfg:
                raise Rejected('normal typed configuration differs from the exact recorded dictionary value')
        value=dict(file_system_id=cfg['file_system_id'],shard_id=cfg['shard_id'],primary_node_id=cfg['primary_node_id'],
                   replica_node_ids=cfg.get('replica_node_ids',[]),epoch=str(decimal(cfg['epoch'],'config epoch')),
                   assignment_blob=self.blobs.json('DfsMetadataShardAssignment',cfg['assignment']),
                   default_policy_blob=self.blobs.json('DfsObjectPolicy',cfg['default_policy']),
                   native_config_blob=self.blobs.json('DfsMetadataShardConfig',cfg),canonical_fingerprint=fp)
        if fp not in self.header['finite_dictionary']['config_aliases']:
            raise Unavailable('actual whole config is absent from finite dictionary')
        self.configs[fp]=cfg
        return value

    def raw_memory(self,oid,n,side):
        event=self.events.get((oid,n))
        if event is None: raise Rejected('held snapshot has no actual owner-local source packet')
        record=event['record']
        if side in ('before','after'):
            field=side+'_held_json'
            value=record.get(field)
            if value is None: raise Unavailable('actual packet has no requested held memory cut')
            encoded=ASSEMBLER.byte_vector(value,'actual held snapshot bytes')
        elif side=='admission':
            if event['collection']!='guard_events' or record.get('phase')!='admitted' or record.get('held') is None:
                raise Rejected('admission held cut does not come from an actual guard mint')
            encoded=ASSEMBLER.SUBTREE.subtree(self.raw_bytes[oid],[event['collection'],event['index'],'held'])
        else: raise Rejected('unknown held snapshot side')
        return encoded,ASSEMBLER.native_json(encoded,'actual held source snapshot')

    def snapshot_source(self,name,blueprint):
        source=blueprint.get('native_source')
        if source is None:
            matches=[]
            for (oid,n),event in self.events.items():
                sides=('admission',) if event['collection']=='guard_events' and event['record'].get('phase')=='admitted' else ('before','after')
                for side in sides:
                    try: encoded,held=self.raw_memory(oid,n,side)
                    except (Unavailable,Rejected): continue
                    if held==blueprint['held']: matches.append(dict(owner_id=oid,owner_ordinal=n,side=side))
            if len(matches)!=1: raise Unavailable('exact owner/ordinal/side reference required; equal held rows do not identify time')
            source=matches[0]
        oid=source['owner_id']; source_n=decimal(source['owner_ordinal'],'memory source ordinal')
        encoded,held=self.raw_memory(oid,source_n,source['side'])
        if held!=blueprint['held']: raise Rejected('snapshot blueprint differs from exact raw packet/subtree bytes')
        packed=[cut for cut in self.inputs['owner_memory_cuts'][oid]['all_memory_cuts']
                if decimal(cut['owner_ordinal'],'packaged cut ordinal')==source_n and cut['side']==source['side']]
        if len(packed)!=1 or packed[0]['value']!=held:
            raise Rejected('packaged held cut differs from actual raw packet')
        import hashlib
        if packed[0].get('exact_json_utf8')!=encoded.decode() or packed[0].get('sha256')!=hashlib.sha256(encoded).hexdigest():
            raise Rejected('packaged held bytes/hash differ from actual native source bytes')
        owner=self.owners[oid]
        if held['backend_root']!=owner['canonical_root'] or held['shard_id']!=self.scope['shard_id']:
            raise Rejected('held snapshot belongs to another physical root/shard')
        asof=blueprint.get('as_of_source',source)
        n=decimal(asof['owner_ordinal'],'as-of ordinal')
        if asof['owner_id']!=oid or n<source_n or (oid,n) not in self.events:
            raise Rejected('logical cut lacks actual same-owner as-of observation')
        side = asof.get('side', 'before' if source['side']=='before' and n==source_n else 'after')
        if side not in ('before', 'after') or (side=='before' and n==source_n and source['side']!='before'):
            raise Rejected('logical before cut borrows its held after-state')
        query_n = n if side=='after' else max((ordinal for actor,ordinal in self.events if actor==oid and ordinal<n), default=0)
        first = source_n if source['side']=='before' else source_n+1
        for (actor,ordinal),event in self.events.items():
            if actor!=oid or not first<=ordinal<=query_n: continue
            if ordinal==source_n and source['side']=='before' and (
                    event['collection']=='packets' or event['collection']=='boundary_packets'
                    and event['record']['facts']['phase'] in
                    ('replica-journal-persisted','replica-prefix-probe-accepted','replica-snapshot-accepted')):
                raise Unavailable('held-before cut crosses its actual durable/floor transition')
            after=event['record'].get('after_held_json')
            if after is not None and not ASSEMBLER.native_metadata_memory_equal(ASSEMBLER.native_json(ASSEMBLER.byte_vector(after,'intervening held cut'),'intervening held cut'),held):
                raise Unavailable('reused held view crossed an actual native memory mutation')
        self.memory_sources[name]=(oid,source_n,source['side'])
        self.cut_refs[name]=(oid,query_n,source['side'],held)
        return oid,query_n,held

    def guard_source(self,reference,oid,n,active=False):
        if not reference: return None
        fact=self.native(reference); raw=self.native(fact.get('actual_native_guard_blob'))
        guards=[event['record'] for (owner,ordinal),event in self.events.items()
                if owner==oid and ordinal<=n and event['collection']=='guard_events'
                and event['record'].get('phase')=='admitted' and event['record'].get('guard_id')==raw.get('guard_id')]
        if len(guards)!=1 or guards[0]!=raw or not raw.get('matches_actual_mounted_owner'):
            raise Rejected('guard wrapper lacks its exact successful normal owner-bound admission packet')
        physical=raw['facts']['physical_scope']; owner=self.owners[oid]
        if (physical['canonical_root']!=owner['canonical_root'] or any(physical[k]!=self.scope[k] for k in ('file_system_id','shard_id'))
                or raw['facts']['actor_node_id']!=owner['node_id'] or raw['facts'].get('execution') is None):
            raise Rejected('actual guard ownership/admission/scope differs')
        if fact.get('digest')!=raw['digest'] or fact.get('actual_physical_scope')!=physical or fact.get('actual_execution')!=raw['facts']['execution']:
            raise Rejected('guard wrapper changed actual native body/physical/execution facts')
        expected=self.native(fact['expected_config']['native_config_blob'])
        authority=raw['facts']['authority']
        if authority.get('expected')!=expected or fact['authorization_mode']!=authority['mode']:
            raise Rejected('guard wrapper changed exact native authority configuration/mode')
        expected_acks=authority.get('authenticated_acknowledged_node_ids',[])
        if fact.get('authenticated_prepare_ack_node_ids')!=expected_acks:
            raise Rejected('guard wrapper changed actual acknowledged member identities')
        if not all(fact.get(field) is True for field in ('actual_writer_lock_retained','raw_scoped_fence_retained','execution_and_data_owner_retained','exact_config_and_missing_handoff_read')):
            raise Unavailable('guard wrapper omits actual retained writer/fence/execution proof')
        admitted=next(ordinal for (actor,ordinal),event in self.events.items()
            if actor==oid and event['collection']=='guard_events' and event['record'].get('guard_id')==raw['guard_id'] and event['record']['phase']=='admitted')
        previous=[event['record'] for (actor,ordinal),event in self.events.items()
            if actor==oid and ordinal<admitted and event['collection']=='guard_events' and event['record']['phase']=='admitted'
            and not any(other['record'].get('phase')=='released' and other['record'].get('guard_id')==event['record']['guard_id']
                        for (peer,number),other in self.events.items() if peer==oid and number<admitted and other['collection']=='guard_events')]
        if previous:raise Unavailable('prior actual publication lease is not absent at this mint')
        if fact.get('prior_writer_bound_epoch') is not None or fact.get('actual_prior_storage_epoch_view')!=raw['held']['writer_bound_epoch']:
            raise Rejected('guard wrapper confuses the requested storage view with publication binding')
        reads=[event['record'] for (actor,ordinal),event in self.events.items()
               if actor==oid and ordinal<=n and event['collection']=='authoritative_reads'
               and event['record'].get('guard_context_id')==raw['guard_id']]
        if len(reads)!=1 or self.native(fact['same_call_native_read_blob'])!=reads[0]['read']:
            raise Unavailable('guard has no exact same-call final authoritative read correlation')
        if active and any(event['record'].get('phase')=='released' and event['record'].get('guard_id')==raw['guard_id']
                for (actor,ordinal),event in self.events.items() if actor==oid and ordinal<=n and event['collection']=='guard_events'):
            raise Rejected('active guard references an already released actual lease')
        return raw

    def require_owned_cut(self,name,blueprint):
        oid,n,side,held=self.cut_refs[name]; source=self.memory_sources[name]
        event=self.events[(source[0],source[1])]
        if event['collection']=='packets' and event['record']['phase'] in ('prepare-durable','prepare-materialized','commit-durable','commit-materialized','exact-prepared-replay','exact-committed-replay'):
            return
        if source[2]=='admission':
            raw=event['record']
            if not raw.get('matches_actual_mounted_owner') or raw['facts'].get('execution') is None:
                raise Unavailable('actual admission/data-owner custody missing')
            return
        interval=blueprint.get('materialization_interval')
        if interval:
            if not isinstance(interval,dict) or set(interval)!={'persisted','completed'}:
                raise Rejected('materialization interval changes its exact raw references')
            keys=[]
            for field in ('persisted','completed'):
                ref=interval[field]
                if not isinstance(ref,dict) or set(ref)!={'owner_id','owner_ordinal'}:
                    raise Rejected('materialization interval lacks its exact owner/ordinal')
                keys.append((ref['owner_id'],decimal(ref['owner_ordinal'],'materialization ordinal')))
            if keys[1]!=(source[0],source[1]):
                raise Rejected('materialized held cut borrows another completion source')
            CAUSAL.qualified_materialization(ASSEMBLER,self.events,self.owners,self.scope,*keys)
        resident_ref=blueprint.get('native_resident_custody_blob')
        if resident_ref:
            fact=self.native(resident_ref);actual=event['record'].get('resident_custody')
            if (event['collection']!='boundary_packets' or actual is None
                    or fact.get('owner_id')!=oid or fact.get('actual_custody')!=actual
                    or fact.get('actual_boundary_source')!={'owner_id':oid,'owner_ordinal':str(source[1])}):
                raise Rejected('resident lease proof is not the exact actual held-source packet')
            owner=self.owners[oid]
            if (actual['actual_mount_label']!=owner['component_incarnation']
                    or actual['local_node_id']!=owner['node_id'] or actual['backend_root']!=held['backend_root']
                    or any(actual[k]!=self.scope[k] for k in ('file_system_id','shard_id'))
                    or actual['observer_owner_ordinal']!=str(source[1])
                    or actual['admission_scheduler_token']!=owner['lane_incarnation']
                    or not all(actual.get(k) for k in ('resident_handle_token','data_owner_token'))):
                raise Rejected('resident custody changes its actual mount/scope/slot/admission')
            admitted=actual['admitted_scope']
            if (actual['admission_kind']!=admitted['kind']
                    or admitted['kind'] not in ('exact-shard','global-exclusive')
                    or admitted['kind']=='exact-shard' and any(admitted[k]!=self.scope[k] for k in ('file_system_id','shard_id'))):
                raise Rejected('replica custody lacks its actual admitted scope')
            if owner['storage_generation_token'].startswith('resident-slot:'):
                if (owner['storage_generation_token']!='resident-slot:'+actual['resident_handle_token']
                        or owner['root_lease_token']!='mounted-resident-owner:'+actual['data_owner_token']):
                    raise Rejected('replica owner binding substitutes another actual resident/data-owner handle')
            # This proves the actual resident lease and admission at the held
            # callback. It grants no author permission and claims no writer FD
            # retained after the normal persistence method returned.
            return
        reference=blueprint.get('actual_writer_admission_guard_blob')
        raw=self.guard_source(reference,oid,n,active=True) if reference else None
        if raw is None: raise Unavailable('replica/read-only held cut needs SAME existing resident/admission/writer custody facts; held getter alone is not a lease proof')
        if raw['held']!=held: raise Rejected('borrowed writer proof describes another native memory cut')

    def decode_entry(self,oid,cut,key,entry,kind):
        if entry is None: return None
        rows=[row for row in self.raw_headers[oid]['actual_control_decodes']
              if row['source_owner_ordinal']==cut['owner_ordinal'] and row['exact_entry']==entry and row['native_type']==kind]
        if len(rows)!=1: raise Unavailable(f'{key}: SAME normal typed decoder correlation missing')
        return rows[0]['decoded_value']

    def control_at(self,oid,n,blueprint):
        candidates=[e['record'] for (owner,ordinal),e in self.events.items()
                    if owner==oid and ordinal<=n and e['collection']=='metadata_cache_cuts']
        if not candidates: raise Unavailable(f'{oid}:{n}: complete already-used native control projection missing')
        cut=max(candidates,key=lambda row:decimal(row['owner_ordinal'],'cache ordinal'))
        folded=ASSEMBLER.validate_metadata_cache_cut(cut)
        ASSEMBLER.validate_metadata_cache_consumer_scope(cut,
            self.scope['file_system_id'], self.scope['shard_id'])
        prefix=f"/dfs/filesystems/{self.scope['file_system_id']}/"
        segment=self.scope['shard_id'].strip('/').replace('/','_')
        kinds=[('metadata-shards/','DfsMetadataShardConfig'),('metadata-membership-handoffs/','DfsMetadataShardMembershipHandoff'),
               ('metadata-operation-floors/','DfsMetadataShardOperationFloor'),('metadata-head-watermarks/','DfsMetadataShardHeadWatermark')]
        keys=[prefix+family+segment for family,_ in kinds]
        if any(not key.startswith(folded['scope_prefix']) for key in keys): raise Rejected('native complete prefix omits control scope')
        entries={entry['key']:entry for entry in folded['entries']}
        decoded=[self.decode_entry(oid,cut,key,entries.get(key),kind) for key,(_,kind) in zip(keys,kinds)]
        if decoded[0] is None: raise Unavailable('actual active native configuration absent')
        cfg=self.config(decoded[0],entries[keys[0]]['value'])
        decodes=[row for row in self.raw_headers[oid]['actual_control_decodes']
                 if row['source_owner_ordinal']==cut['owner_ordinal'] and row['exact_entry'] in [entries.get(key) for key in keys]]
        ref=self.blobs.json('DfsNativeCacheControlCutEvidenceV1',self.certificate(actual_cache_cut=cut,
            actual_typed_decodes=decodes,selected_control_keys=keys,selected_entries=[entries.get(key) for key in keys]))
        kv=dict(requested_keys=keys,rows=[dict(key=key,entry_blob=self.blobs.json('KvEntry',entries[key]) if key in entries else None) for key in keys],
                revision=folded['cached_revision'],applied_index=None,native_authority_evidence_blob=ref,complete=True,continuous=True)
        read_ref=blueprint.get('actual_authoritative_read_blob')
        read_keys=blueprint.get('actual_authoritative_read_keys')
        guard_ref=blueprint.get('active_guard_blob') or blueprint.get('last_guard_blob')
        if guard_ref:
            wrapper=self.native(guard_ref)
            if read_ref and read_ref!=wrapper.get('same_call_native_read_blob'):
                raise Rejected('context admission read borrows another native guard call')
            read_ref=wrapper.get('same_call_native_read_blob')
            guards=self.guard_source(guard_ref,oid,n,active=bool(blueprint.get('active_guard_blob')))
            read_rows=[event['record'] for (actor,ordinal),event in self.events.items()
                if actor==oid and ordinal<=n and event['collection']=='authoritative_reads'
                and event['record'].get('guard_context_id')==guards['guard_id']]
            read_keys=read_rows[0]['requested_keys']
        if read_ref:
            read=self.native(read_ref)
            matching=[event['record'] for (actor,ordinal),event in self.events.items()
                if actor==oid and ordinal<=n and event['collection']=='authoritative_reads'
                and event['record']['read']==read and event['record']['requested_keys']==read_keys]
            if not matching or read_keys is None or len(read_keys)!=len(read['value']):
                raise Rejected('context read lacks the exact actual requested-key/result correlation')
            if decimal(read['authority']['revision'],'guard revision')!=decimal(folded['cached_revision'],'cache revision'):
                raise Unavailable('actual admission read crossed retained complete control projection; SAME complete cut hook required')
            for key,entry in zip(read_keys,read['value']):
                if key not in keys or entries.get(key)!=entry: raise Rejected('actual guard read differs from complete native cut')
        horizon={oid:n}
        for reference in blueprint.get('peer_cut_refs',{}).values():
            actor,ordinal,_,_=self.cut_refs[reference];horizon[actor]=max(horizon.get(actor,0),ordinal)
        for (actor,ordinal),event in self.events.items():
            if actor not in horizon or ordinal>horizon[actor]: continue
            if event['collection']=='control_commits':
                response=event['record']['commit']['result'].get('txn_response')
            elif event['collection']=='original_control_commits':
                response=event['record']['original']['committed'].get('txn_response')
            else: continue
            if response is not None and response.get('succeeded') is True and decimal(response['revision'],'observed commit revision')>decimal(folded['cached_revision'],'complete cache revision'):
                raise Unavailable('actual committed control transaction is newer than complete retained cut; fresh SAME complete projection required')
        self.control[oid,n]=(cut,folded,cfg,decoded,ref,kv)
        return self.control[oid,n]

    def semantic_prefix(self,oid,prefix):
        anchor=self.prefix(self.header['owner_cuts'][oid]['native_prefix'])
        wanted=self.prefix(prefix); items=self.prefix_items.get(oid)
        if items is None: raise Unavailable('actual owner semantic frame dictionary absent')
        count=decimal(wanted['journal_sequence'],'native sequence')-decimal(anchor['journal_sequence'],'anchor')
        if count<0 or count>len(items): raise Unavailable('actual owner suffix does not extend certified anchor')
        chosen=items[:count]
        if chosen:
            # Projector independently extends the exact captured frame chain.
            previous=anchor
            for item in chosen:
                entry=self.native(item['native_entry_blob']); exact=ASSEMBLER.byte_vector(
                    list(__import__('base64').b64decode(self.blobs.values[item['exact_frame_blob']]['data'])), 'exact frame')
                decode=self.native(item['decoder_observation_blob'])
                if decode.get('owner_id')!=oid:
                    raise Rejected('semantic frame decoder certificate borrows another physical owner')
                ordinal=decimal(decode['actual_owner_ordinal'],'decoder native ordinal')
                event=self.events.get((oid,ordinal))
                if event is None: raise Rejected('semantic frame lacks actual owner-local native packet')
                actual_frames=[]
                if event['collection']=='packets':
                    packet=event['record']
                    if packet.get('exact_frame') is not None:
                        actual_frames.append((ASSEMBLER.byte_vector(packet['exact_frame'],'actual kernel frame'),packet['exact_entry']))
                    decodes=[row['decoded_entry'] for row in self.raw_headers[oid]['actual_frame_decodes'] if decimal(row['owner_ordinal'],'native frame ordinal')==ordinal]
                elif event['collection']=='boundary_packets' and event['record']['facts']['phase']=='replica-journal-persisted':
                    command=event['record']['facts']['command']
                    decodes=[]
                    for index,frame in enumerate(command['batch']['journal_frames']):
                        observed=[row['decoded_entry'] for row in self.raw_headers[oid].get('actual_received_frame_decodes',[])
                                  if decimal(row['source_owner_ordinal'],'received frame ordinal')==ordinal and row['frame_index']==index]
                        if len(observed)!=1: raise Unavailable('normal current received-frame decoder correlation missing')
                        actual_frames.append((ASSEMBLER.byte_vector(frame['exact_bytes'],'actual received frame'),observed[0]));decodes.extend(observed)
                else: raise Rejected('semantic frame points to a non-journal boundary')
                if (exact,entry) not in actual_frames or entry not in decodes:
                    raise Rejected('semantic frame/digest input differs from actual normal decoder/bytes')
                previous=ASSEMBLER.SUBTREE.PROJECTOR.extend_native_prefix(previous,entry['sequence'],exact)
            if ASSEMBLER.native_prefix(previous)!=ASSEMBLER.native_prefix(wanted): raise Rejected('actual suffix frame chain does not reach captured cut')
        elif ASSEMBLER.native_prefix(anchor)!=ASSEMBLER.native_prefix(wanted): raise Rejected('empty semantic suffix changes native cut')
        return wanted,self.blobs.json('DfsNativeSemanticPrefixCertificateV1',self.certificate(native_initial_anchor=anchor,native_prefix=wanted,semantic_prefix=chosen))

    def effect_materialization_source(self, record, table, effect, frontier):
        # Only receipt DATA can come from a nonterminal prepare. Visible object
        # and terminal intent effects still require their actual commit cut.
        prepared = record['status']=='prepared' and record.get('committed_entry') is None
        if prepared and table!='receipts':
            raise Rejected('nonterminal preparation cannot supersede an intent or visible effect')
        phase,kind=('prepare-materialized','prepare-payload') if prepared else ('commit-materialized','commit-digest')
        sources = [(oid,n,event) for (oid,n),event in self.events.items()
            if event['collection']=='packets' and event['record']['phase']==phase
            and event['record']['operation']==record['digest']]
        if len(sources)!=1:
            raise Unavailable('PutIntent needs one actual original materialization source')
        oid,n,event = sources[0]
        if oid not in frontier or n>frontier[oid]:
            raise Rejected('effect materialization is after the selected source-closed frontier')
        _,held = self.raw_memory(oid,n,'after')
        if record not in held['tables'].get('prepared_operations_v7',[]) or effect not in held['tables'].get(table,[]):
            raise Rejected('actual intent materialization differs from its sealed record/effect')
        rows = [row for row in self.header['native_timeline']
            if row['owner_id']==oid and row['action']['kind']==kind
            and row['action']['operation']['operation_id']==record['digest']['operation_id']
            and row['action']['operation']['native_digest']==record['digest']['payload_digest']
            and dict(actual_mount_label=oid,owner_ordinal=str(n)) in row['raw_references']]
        if len(rows)!=1:
            raise Unavailable('intent materialization lacks its exact native commit event')
        row=rows[0]; blueprint=self.header['snapshot_blueprints'][row['after_snapshot']]
        if blueprint['native_source']!=dict(owner_id=oid,owner_ordinal=str(n),side='after') or blueprint['held']!=held:
            raise Rejected('intent materialization borrows another actual held source')
        return dict(event_id=row['event_id'],snapshot_id=row['after_snapshot'],table=table,effect=effect)

    def receipt_observation_matches_source(self, reference, successor, materialization, frontier):
        # Re-run the EXISTING receipt matcher against the same verified raw
        # events and actual materialization cut. Never certify supplied refs.
        snapshot_id=materialization['snapshot_id']
        blueprint=self.header['snapshot_blueprints'][snapshot_id]
        source=blueprint['native_source'];owner=source['owner_id']
        ordinal=decimal(source['owner_ordinal'],'receipt materialization ordinal')
        horizon={owner:ordinal}
        for node,cut in blueprint.get('peer_cut_refs',{}).items():
            actor,n,_,_=self.cut_refs[cut]
            if self.owners[actor]['node_id']!=node or actor not in frontier or n>frontier[actor]:
                raise Rejected('receipt materialization borrows a foreign or future source-closed cut')
            horizon[actor]=n
        if owner not in frontier or ordinal>frontier[owner]:
            raise Rejected('receipt materialization is outside the actual current frontier')
        if successor['status']=='prepared':
            # Use the SAME earlier normal control projection only to correlate
            # returned DATA. A future admitted publication guard grants nothing.
            cfg=self.native(self.control_at(owner,ordinal,{})[2]['native_config_blob'])
            if (cfg['primary_node_id']!=self.owners[owner]['node_id']
                    or decimal(cfg['epoch'],'receipt config epoch')!=decimal(successor['digest']['owner_epoch'],'receipt prepared epoch')):
                raise Rejected('prepared receipt DATA belongs to another actual source configuration')
        else:
            guards=[event for (actor,n),event in self.events.items() if actor in horizon and n<=horizon[actor]
                and event['collection']=='guard_events' and event['record']['phase']=='admitted'
                and event['record']['digest']==successor['digest']]
            if len(guards)!=1: raise Unavailable('receipt successor lacks its actual admitted guard source')
            cfg=guards[0]['record']['facts']['authority']['expected']
        refs=self.native(reference).get('actual_raw_references')
        if (not isinstance(refs,list) or not refs or any(
                ref['actual_mount_label'] not in horizon
                or decimal(ref['owner_ordinal'],'receipt source ordinal')>horizon[ref['actual_mount_label']]
                for ref in refs)):
            raise Unavailable('receipt return is outside the selected actual materialization frontier')
        canonical=self.bodies[successor['digest']['operation_id']][2]
        facts=dict(actual_memory_cut_source=source,
            peer_certified_cuts={cfg['primary_node_id']:dict(held_snapshot_id=snapshot_id)},
            operation_observations={successor['digest']['operation_id']:dict(initial_payload_blob=canonical,
                initial_content_receipt_observation_blobs=[],durable_content_receipt_blobs=[])})
        candidate=dict(native_operational_facts_blob=self.blobs.json('DfsShardNativeOperationalFactsV1',facts),
            active_config=dict(native_config_blob=self.blobs.json('DfsMetadataShardConfig',cfg)))
        kinds={'packets':'kernel','content_receipts':'content-receipt','boundary_packets':'boundary'}
        raw={key:dict(kind=kinds[event['collection']],ordinal=key[1],record=event['record'])
            for key,event in self.events.items() if event['collection'] in kinds
            and key[0] in horizon and key[1]<=horizon[key[0]]}
        contexts,_=ASSEMBLER.derive_initial_local_receipts(dict(scope=self.scope,envelope_binding=self.artifact,
            owners=list(self.owners.values()),snapshot_contexts={snapshot_id:candidate}),raw,self.blobs)
        derived=self.native(contexts[snapshot_id]['native_operational_facts_blob'])['operation_observations'][successor['digest']['operation_id']]
        references=derived.get('initial_content_receipt_observation_blobs',[])
        return any(self.native(actual)==self.native(reference) for actual in references)

    def declared_footprint(self,payload,record,held,oid,n,blueprint):
        # Declare the original complete input; never substitute observed rows
        # for expected effects. The projector checks current index derivation.
        result=dict(complete_atomic_footprint=True,operation_id=record['digest']['operation_id'],
            native_payload_digest=record['digest']['payload_digest'],
            all_effect_rows_materialized=record['status']=='committed',
            actual_footprint_rows={key:held['tables'].get(key,[]) for key in ASSEMBLER.SUBTREE.PROJECTOR.VISIBLE_TABLES},
            declared_transaction=payload['transaction'],declared_metadata_effects=payload.get('metadata_effects',[]),
            absent_native_rows=[],superseded_effects=[],effect_materializations=[],parent_path_absence_checks_complete=True,
            terminal_intent_and_request_outcomes_complete=True)
        if record['status']!='committed': return result
        frontier={oid:n}
        for node,cut in blueprint.get('peer_cut_refs',{}).items():
            actor,ordinal,_,_=self.cut_refs[cut]
            if self.owners[actor]['node_id']!=node or actor==oid and ordinal!=n:
                raise Rejected('atomic footprint changes the actual owner/source-closed frontier')
            frontier[actor]=ordinal
        old_commit=record.get('committed_entry')
        if old_commit is None: raise Rejected('actual committed record lacks native commit reference')
        for effect in payload['metadata_effects']:
            if effect['type'] not in ('put-intent','put-receipt'): continue
            table,field=('intents','intent') if effect['type']=='put-intent' else ('receipts','receipt')
            prior=effect[field]
            result['effect_materializations'].append(self.effect_materialization_source(record,table,prior,frontier))
            result['actual_footprint_rows'][table]=held['tables'].get(table,[])
            if prior in held['tables'].get(table,[]): continue
            matches=[]
            for successor in held['tables'].get('prepared_operations_v7',[]):
                prepared_receipt=(table=='receipts' and successor['status']=='prepared'
                    and successor.get('committed_entry') is None)
                if prepared_receipt:
                    sequence=successor['digest']['prepare_sequence']
                elif successor['status']=='committed' and successor.get('committed_entry'):
                    sequence=successor['committed_entry']['sequence']
                else: continue
                if decimal(sequence,'successor materialization')<=decimal(old_commit['sequence'],'earlier commit'): continue
                op=successor['digest']['operation_id']
                if op not in self.bodies: continue
                body,digest,reference=self.bodies[op]
                if digest!=successor['digest'] or body!=successor['payload']:
                    raise Rejected('effect successor differs from its captured immutable preparation')
                if table=='intents':
                    if body['prepared_intent']!=prior: continue
                    replacement=ASSEMBLER.SUBTREE.PROJECTOR.exact_intent_terminal(prior,body)
                    candidates=[replacement] if replacement in held['tables'].get(table,[]) else []
                else:
                    candidates=[row for row in body['content_receipts'] if row in held['tables'].get(table,[])
                        and ASSEMBLER.SUBTREE.PROJECTOR.exact_receipt_replacement(prior,row)]
                for replacement in candidates:
                    witness=dict(table=table,prior_effect=prior,replacement_effect=replacement,
                        successor_record_blob=self.blobs.json('DfsPreparedOperationRecord',successor),
                        successor_canonical_payload_blob=reference,
                        successor_materialization=self.effect_materialization_source(successor,table,replacement,frontier))
                    if table=='receipts':
                        references=self.header.get('initial_content_receipt_observation_blobs_by_operation',{}).get(op,[])
                        proven=[ref for ref in references if self.native(ref).get('coordinator_receipt_record')==replacement]
                        if len(proven)!=1: continue
                        if not self.receipt_observation_matches_source(proven[0],successor,witness['successor_materialization'],frontier):
                            raise Unavailable('sealed receipt has no exact observed persistence/coordinator return')
                        fact=self.native(proven[0]); raw_refs=fact.get('actual_raw_references')
                        if (fact.get('origin') not in ('local-verified-durable','authenticated-durable-response')
                                or not isinstance(raw_refs,list) or not raw_refs
                                or any((ref['actual_mount_label'],decimal(ref['owner_ordinal'],'receipt source')) not in self.events for ref in raw_refs)):
                            raise Unavailable('receipt successor lacks actual persistence/coordinator sources')
                        witness['successor_receipt_observation_blob']=proven[0]
                    matches.append(witness)
            if len(matches)!=1:
                raise Unavailable('superseded metadata effect lacks one exact observed sealed successor')
            result['superseded_effects'].append(matches[0])
        for prior in payload['transaction']['objects']:
            current=[row for row in held['tables'].get('objects',[])
                     if row['file_system_id']==prior['file_system_id'] and row['object_id']==prior['object_id']]
            if prior in current: continue
            matches=[]
            for successor in held['tables'].get('prepared_operations_v7',[]):
                if successor['status']!='committed' or not successor.get('committed_entry'): continue
                if decimal(successor['committed_entry']['sequence'],'successor commit')<=decimal(old_commit['sequence'],'earlier commit'): continue
                op=successor['digest']['operation_id']
                if op not in self.bodies: continue
                body,digest,reference=self.bodies[op]
                if digest!=successor['digest'] or body!=successor['payload']: raise Rejected('successor retained body differs from captured immutable preparation')
                readset=body['expected_object_versions']
                if not any(row['object_id']==prior['object_id'] and row.get('expected_version')==prior['version'] for row in readset): continue
                for replacement in body['transaction']['objects']:
                    if replacement['file_system_id']==prior['file_system_id'] and replacement['object_id']==prior['object_id'] and replacement in current:
                        matches.append(dict(table='objects',prior_effect=prior,replacement_effect=replacement,
                            successor_record_blob=self.blobs.json('DfsPreparedOperationRecord',successor),
                            successor_canonical_payload_blob=reference))
            if len(matches)!=1: raise Unavailable('superseded native object needs one actual committed successor with exact prior-version readset')
            result['superseded_effects'].append(matches[0])
        return result

    def request_value_decode_refs(self,oid,ordinal,record):
        references=[];header=self.raw_headers[oid]
        for observed in header.get('actual_request_value_decodes',[]):
            if decimal(observed['source_owner_ordinal'],'actual request decode ordinal')!=ordinal:continue
            decoded=ASSEMBLER.validate_request_value_decode(header,record,observed)
            encoded=ASSEMBLER.byte_vector(observed['exact_value'],'actual request value')
            kind=observed['native_type']
            evidence=self.certificate(encoded_value_sha256=hashlib.sha256(encoded).hexdigest(),
                encoded_value_blob=self.blobs.put('DfsNativeKvEncodedValue',encoded,'exact-frame-base64'),
                decoded_native_type=kind,decoded_value_blob=self.blobs.json(kind,decoded),
                actual_request_source=dict(owner_id=oid,owner_ordinal=str(ordinal)),
                actual_request_value_decode=observed)
            references.append(self.blobs.json('DfsNativeKvValueDecodeObservationV1',evidence))
        return references

    def permit_at(self,oid,n,digest):
        candidates=[]
        for (owner,ordinal),event in self.events.items():
            if owner!=oid or ordinal>n or event['collection']!='boundary_packets': continue
            fact=event['record']['facts']
            if fact['phase']!='recovery-permit-issued': continue
            operation=fact['operation'];permit=fact['permit']
            if operation.get('kind')!='prepared-operation': raise Rejected('permit boundary is not a current native prepared operation')
            if any(operation[field]!=digest[field] for field in ('operation_id','file_system_id','shard_id','owner_epoch','prepare_sequence','payload_digest')): continue
            if (permit['file_system_id']!=digest['file_system_id'] or permit['shard_id']!=digest['shard_id']
                    or decimal(permit['owner_epoch'],'permit epoch')!=decimal(digest['owner_epoch'],'prepared epoch')
                    or decimal(permit['prepare_sequence'],'permit sequence')!=decimal(digest['prepare_sequence'],'prepared slot')
                    or permit['operation_digest']!=digest['payload_digest']):
                raise Rejected('actual permit differs from exact captured preparation')
            native=permit.get('native_commit')
            if native is None: raise Unavailable('actual returned operation permit lacks native request/result')
            calls=[(number,row['record']) for (actor,number),row in self.events.items()
                if actor==oid and number<=ordinal and row['collection']=='control_commits'
                and row['record']['commit']==native]
            if len(calls)!=1: raise Unavailable('permit lacks unique SAME native control transaction receipt packet')
            number,call=calls[0];commit=call['commit'];result=commit['result'];response=result.get('txn_response')
            if (response is None or response.get('succeeded') is not True
                    or commit['request']['request_id']!=result['request_id']
                    or decimal(response['revision'],'receipt revision')!=decimal(result['authority']['revision'],'authority revision')):
                raise Rejected('same-call permit receipt has invalid success/request/revision identity')
            raw={(actor,n):dict(kind={'control_commits':'control-commit','original_control_commits':'original-control-commit'}.get(event['collection'],event['collection']),
                    ordinal=n,record=event['record']) for (actor,n),event in self.events.items()}
            module,_=ASSEMBLER.initial_native_verifiers(self.scope,list(self.owners.values()),raw)
            voters={member['node_id'] for member in self.raw_headers[oid]['normal_mounted_identity']['configured_members']
                if member['role']=='voter' and member['state']=='voter'}
            original=module.original_for_replay(self.scope,self.owners[oid],self.raw_headers[oid],raw,
                (oid,number),call,voters,required=False)
            proof=self.certificate(owner_id=oid,actual_owner_ordinal=str(number),commit=commit,
                refinement_boundary='verified-r0-atomic-kv')
            if original is not None:
                proof['original_native_commit_source']=dict(owner_id=oid,owner_ordinal=str(original[1]))
            basis=result['authority']['basis']
            quorum=basis.get('current-term-quorum') if isinstance(basis,dict) else None
            if quorum is None: raise Unavailable('operation permit has no actual fresh current-term quorum basis')
            value=self.certificate(digest=digest,permit_id=permit['permit_id'],expected_config=self.config(fact['expected']),
                native_permit_blob=self.blobs.json('DfsMetadataOperationPermit',permit),
                native_same_call_commit_observation_blob=self.blobs.json('DfsNativeControlCommitObservationV1',proof),
                acknowledged_voter_count=str(decimal(quorum['voter_acks'],'native voter acknowledgements')),
                authenticated_recovery_prefix_ack_node_ids=fact['authenticated_recovery_node_ids'],
                kv_value_decode_evidence_blobs=self.request_value_decode_refs(oid,number,call),
                actual_permit_boundary_source=dict(owner_id=oid,owner_ordinal=str(ordinal)))
            candidates.append((ordinal,self.blobs.json('DfsNativeOperationPermitObservationV1',value)))
        return max(candidates,key=lambda row:row[0])[1] if candidates else None

    def operations_at(self,oid,n,held,blueprint):
        result={}
        for op,(payload,digest,body_ref) in self.bodies.items():
            intent=payload.get('prepared_intent')
            receipts=payload['content_receipts']; nodes=[row['replica_node_id'] for row in receipts]
            if len(nodes)!=len(set(nodes)): raise Rejected('actual sealed body duplicates receipt node')
            initial_refs=self.header.get('initial_content_receipt_observation_blobs_by_operation',{}).get(op,
                         self.header.get('initial_content_receipt_observation_blobs',[]) if len(self.bodies)==1 else [])
            available=[]; available_refs=[]
            for row in receipts:
                if row in held['tables'].get('receipts',[]):
                    available.append(row); continue
                for reference in initial_refs:
                    evidence=self.native(reference)
                    if evidence.get('coordinator_receipt_record')!=row: continue
                    refs=evidence.get('actual_raw_references',[])
                    if evidence.get('origin')=='authenticated-durable-response':
                        coordinator=evidence.get('actual_coordinator_source')
                        if coordinator is None: continue
                        actor=coordinator['owner_id'];ordinal=decimal(coordinator['owner_ordinal'],'actual coordinator ordinal')
                        event=self.events.get((actor,ordinal))
                        if actor!=oid or ordinal>n or event is None or event['collection']!='content_receipts' or event['record']['facts']['origin']!='coordinator-durable': continue
                        actual=event['record']['facts']['receipt']
                        if actual!=self.native(evidence['actual_distributed_receipt_blob']): raise Rejected('remote receipt coordinator source differs from actual stored durable return')
                        # Receiver ordinals belong to another owner and are
                        # ordered only by exact authenticated call correlation.
                        available.append(row);available_refs.append(reference);break
                    if refs and all(ref['actual_mount_label']==oid and decimal(ref['owner_ordinal'],'content return ordinal')<=n for ref in refs):
                        available.append(row); available_refs.append(reference); break
            value=dict(initial_payload_blob=body_ref,canonical_payload_blob=body_ref,
                durable_content_receipt_node_ids=[row['replica_node_id'] for row in available],
                durable_content_receipt_blobs=[self.blobs.json('DfsReplicaReceiptRecord',row) for row in available],
                initial_content_receipt_observation_blobs=available_refs,authenticated_prepare_ack_node_ids=[])
            if intent is not None:
                value.update(target_object_id=intent['object_id'], target_version=intent['target_version'],
                             content_hash=intent['content_hash'])
            starts=[e for (owner,ordinal),e in self.events.items() if e['collection']=='packets'
                    and e['record']['phase']=='prepare-durable' and e['record']['operation']['operation_id']==op]
            if len(starts)!=1: raise Unavailable('actual first native preparation is not uniquely captured')
            start=starts[0]; source=start['owner_id']; start_n=decimal(start['owner_ordinal'],'prepare ordinal')
            start_packet=start['record']; before=ASSEMBLER.native_json(ASSEMBLER.byte_vector(start_packet['before_held_json'],'prepare before'),'prepare before')
            after=ASSEMBLER.native_json(ASSEMBLER.byte_vector(start_packet['after_held_json'],'prepare after'),'prepare after')
            for expected in payload['expected_object_versions']:
                rows=[row for row in before['tables'].get('objects',[]) if row['file_system_id']==self.scope['file_system_id'] and row['object_id']==expected['object_id']]
                if len(rows)>1 or (rows[0]['version'] if rows else None)!=expected.get('expected_version'):
                    raise Rejected('actual immutable prepare readset differs from captured prior head')
            for expected in payload['expected_path_occupants']:
                rows=[row for row in before['tables'].get('paths',[]) if row['file_system_id']==self.scope['file_system_id'] and row['path']==expected['path']]
                if len(rows)>1 or (rows[0]['object_id'] if rows else None)!=expected.get('expected_object_id'):
                    raise Rejected('actual immutable prepare path readset differs from captured prior occupant')
            prepare_cfg=self.control_at(source,start_n,{})[2]
            prepare_prefix,prepare_cert=self.semantic_prefix(source,after['certified_prefix'])
            records=[row for row in held['tables'].get('prepared_operations_v7',[]) if row['digest']['operation_id']==op]
            if records:
                value['durable_prepare_observation']=dict(config=prepare_cfg,owner_id=source,prefix=prepare_prefix,prefix_certificate_blob=prepare_cert,
                    durable_content_receipt_node_ids=nodes,native_visible_commit_operation_ids=[row['digest']['operation_id'] for row in before['tables'].get('prepared_operations_v7',[]) if row['status']=='committed'])
                committed=records[0]['status']=='committed'
                value['visibility_chain_certificate']=self.declared_footprint(payload,records[0],held,oid,n,blueprint)
                if committed:
                    guards=self.header.get('guard_observation_blobs_by_operation',{})
                    ref=guards.get(op,self.header.get('actual_guard_observation_blob') if len(self.bodies)==1 else None)
                    if not ref: raise Unavailable('real committed body lacks its actual admitted guard observation')
                    wrapper=self.native(ref); raw_guard=self.native(wrapper.get('actual_native_guard_blob'))
                    author=raw_guard.get('owner_label')
                    if author not in self.owners:
                        raise Rejected('committed operation guard names an unobserved author owner')
                    author_n=n
                    if author!=oid:
                        # These exact peer frontiers were source-closed by the
                        # verified global DAG before constructing operation views.
                        selected=blueprint.get('peer_cut_refs',{}).get(self.owners[author]['node_id'])
                        if selected not in self.cut_refs:
                            raise Unavailable('committed operation lacks its selected actual author frontier')
                        selected_owner,author_n,_,_=self.cut_refs[selected]
                        if selected_owner!=author:
                            raise Rejected('committed author frontier substitutes another observed owner')
                    actual_guard=self.guard_source(ref,author,author_n)
                    if actual_guard['digest']!=digest:
                        raise Rejected('committed author guard differs from the exact original operation digest')
                    value['committed_guard_observation_blob']=ref
            acknowledged=set()
            if (source==oid and start_n<=n
                    and decimal(held['certified_prefix']['journal_sequence'],'actual acknowledged source cut')
                        >= decimal(digest['prepare_sequence'],'actual prepare slot')):
                acknowledged.add(self.owners[source]['node_id'])
            for (owner,ordinal),event in self.events.items():
                if owner!=oid or ordinal>n or event['collection']!='boundary_packets': continue
                fact=event['record']['facts']
                if fact['phase']!='source-journal-response-accepted': continue
                for edge in self.inputs['actual_request_response_correlations']:
                    if edge['source_owner_id']==oid and decimal(edge['source_ordinal'],'source ACK ordinal')==ordinal:
                        _,_,_,receive=self.verified_edge(edge)
                        probe=receive['command'].get('authority_probe')
                        if probe and probe['operation'].get('operation_id')==op:
                            operation=probe['operation']
                            fields=('file_system_id','shard_id','owner_epoch','prepare_sequence','payload_digest','operation_id')
                            if operation.get('kind')!='prepared-operation' or any(operation.get(field)!=digest[field] for field in fields):
                                raise Rejected('native prefix ACK changes the typed immutable prepared-operation tuple')
                            acknowledged.add(fact['authenticated_source_node_id'])
            value['authenticated_prepare_ack_node_ids']=sorted(acknowledged)
            permit_ref=self.permit_at(oid,n,digest)
            if permit_ref is not None: value['native_permit_observation_blob']=permit_ref
            result[op]=value
        return result

    def freeze_at(self,oid,n,held,cfg):
        raw=held['write_freeze']; observed=raw.get('observation'); marker=raw.get('value')
        persisted=[]; rebind=[]
        for (owner,ordinal),event in self.events.items():
            if owner!=oid or ordinal>n or event['collection']!='boundary_packets': continue
            fact=event['record']['facts']
            if fact['phase']=='retiring-freeze-persisted': persisted.append((ordinal,fact))
            if fact['phase']=='successor-rebound': rebind.append((ordinal,fact))
        provenance=None
        if persisted:
            fact=persisted[-1][1]
            matching=[row for row in rebind if row[0]>persisted[-1][0] and row[1]['proof']['handoff_id']==fact['pending']['handoff_id']]
            original=matching[-1][1]['original_freeze'] if matching else fact['freeze']
            if observed=='not-observed':
                # Preserve NotObserved in held bytes. This separate gate fact
                # comes from the actual normal persisted/rebound return, not a
                # guessed on-disk marker or a claim that the marker is absent.
                marker=original;observed='present'
                provenance=self.blobs.json('DfsNativePersistedFreezeObservationV1',self.certificate(owner_id=oid,actual_boundary_fact=fact,actual_owner_ordinal=str(persisted[-1][0])))
            elif observed!='present':
                raise Rejected('actual observed marker absence contradicts retained freeze without a normal removal observation')
            elif marker!=original:
                immutable={key:value for key,value in marker.items() if key!='rebind_prefix_proof'}
                if immutable!={key:value for key,value in original.items() if key!='rebind_prefix_proof'}:
                    raise Rejected('actual held freeze changed its original handoff/activation identity')
                kinds={'packets':'kernel','boundary_packets':'boundary','control_commits':'control-commit'}
                raw={(owner,ordinal):dict(kind=kinds[event['collection']],ordinal=ordinal,record=event['record'])
                     for (owner,ordinal),event in self.events.items() if event['collection'] in kinds}
                module,_=ASSEMBLER.initial_native_verifiers(self.scope,list(self.owners.values()),raw)
                module.frame_chain(oid,original['rebind_prefix_proof'],marker['rebind_prefix_proof'],raw,self.raw_headers[oid],maximum_ordinal=n+1,scope=self.scope)
            # A newer actual activated/extended marker is never overwritten by
            # the earlier retiring-freeze value just because their bytes differ.
        if observed=='not-observed' and not persisted:
            kinds={'packets':'kernel','guard_events':'guard','boundary_packets':'boundary'}
            raw_index={(owner,ordinal):dict(kind=kinds[event['collection']],ordinal=ordinal,record=event['record'])
                       for (owner,ordinal),event in self.events.items() if event['collection'] in kinds}
            candidates=[]
            for key,event in self.events.items():
                if (key[0]!=oid or key[1]>n or event['collection']!='boundary_packets'
                        or event['record']['facts']['phase']!='replica-write-freeze-read'):
                    continue
                qualified=ASSEMBLER.verify_replica_freeze_read(raw_index,self.owners,self.scope,key)
                actual=qualified['actual_custody'];owner=self.owners[oid]
                if (owner['storage_generation_token']=='resident-slot:'+actual['resident_handle_token']
                        and owner['root_lease_token']=='mounted-resident-owner:'+actual['data_owner_token']
                        and owner['lane_incarnation']==actual['admission_scheduler_token']):
                    candidates.append((key[1],qualified))
            if candidates:
                _,qualified=max(candidates,key=lambda row:row[0])
                # Preserve the getter's NotObserved bytes. The separate earlier
                # actual read result supplies this later gate, never a new read.
                actual=qualified['observed_freeze'];observed=actual['observation'];marker=actual.get('value')
                provenance=self.blobs.json('DfsNativeReplicaFreezeReadObservationV1',
                    self.certificate(**qualified))
        if observed=='not-observed' and not persisted:
            active=self.native(cfg['native_config_blob']);owner=self.owners[oid]
            if (active['primary_node_id']==owner['node_id']
                    or not owner['storage_generation_token'].startswith('resident-slot:')
                    or marker is not None):
                raise Unavailable('primary authority requires its actual known freeze observation')
            # Exact resident/held-custody validation still runs for this cut.
            # Unknown is an observation state and cannot admit any ingest.
            return dict(writer_owner_id=oid,read_under_actual_owner=False,
                observed_pending_handoff=False,durable_write_frozen=None,
                write_freeze_observation='not-observed',actual_replica_ingest_read_source=None)
        if observed not in ('absent','present'): raise Unavailable('actual normal write-freeze observation/result missing')
        gate=dict(writer_owner_id=oid,read_under_actual_owner=True,observed_pending_handoff=False,
                  durable_write_frozen=observed=='present',write_freeze_observation='observed-'+observed)
        if marker is not None:
            gate['native_write_freeze_blob']=self.blobs.json('DfsMetadataShardWriteFreeze',marker)
        if provenance:
            if self.blobs.values[provenance]['native_type']=='DfsNativeReplicaFreezeReadObservationV1':
                gate['actual_replica_freeze_read_observation_blob']=provenance
                accepted=[(decimal(proof['ingest_source']['owner_ordinal'],'accepted R ordinal'),proof)
                    for _,proof in candidates
                    if decimal(proof['ingest_source']['owner_ordinal'],'accepted R ordinal')<=n]
                gate['actual_replica_ingest_read_source']=max(accepted,key=lambda row:row[0])[1]['read_source'] if accepted else None
            else:gate['actual_persisted_freeze_observation_blob']=provenance
        if rebind and marker is not None and rebind[-1][1]['proof']['handoff_id']==marker['handoff_id']:
            fact=rebind[-1][1]; after_epoch=held['backend_instance_authority_epoch']
            active=self.native(cfg['native_config_blob'])
            if fact['active']==active and after_epoch==active['epoch']:
                if marker is None or marker.get('activated_epoch')!=active['epoch']:
                    raise Unavailable('SAME successful activated marker write result is missing; backend handoff134–168 copy hook required')
                evidence=self.certificate(owner_id=oid,active=active,proof=fact['proof'],original_freeze=fact['original_freeze'],after_instance_epoch=after_epoch)
                gate['actual_successor_rebind_observation_blob']=self.blobs.json('DfsNativeSuccessorRebindObservationV1',evidence)
                gate['durable_write_frozen']=False;gate['write_freeze_observation']='observed-present-inactive'
        return gate

    def construct(self):
        blueprints=self.header['snapshot_blueprints']
        for name,blueprint in blueprints.items(): self.snapshot_source(name,blueprint)
        basis = self.header.get('global_observer_basis')
        if basis is None:
            raise Unavailable('actual global observer basis is missing')
        for name, blueprint in basis['snapshot_blueprints'].items():
            self.snapshot_source(name, blueprint)
        dag = CAUSAL.NativeExecutionDag(ASSEMBLER, self.events, self.owners, self.scope,
                                       self.inputs['actual_request_response_correlations'], self.verified_edge, self.raw_headers)
        CAUSAL.validate_globalized(dag, self.header)
        for row in self.header['native_timeline']:
            action=row['action'];oid=row['owner_id']
            if action['kind']=='fallback-permit':
                n=decimal(row['event_id'].rsplit(':',1)[1],'actual permit return ordinal');op=action['operation']['operation_id']
                permit_ref=self.permit_at(oid,n,self.bodies[op][1])
                if permit_ref is None:raise Unavailable('fallback action lacks SAME independently checked actual native permit')
                action['native_quorum_result_blob']=permit_ref
            if action['kind']=='publication-guard-admitted':
                reference=action['authorization_blob'];value=self.native(reference)
                if value['authorization_mode']!='recovery':continue
                op=action['operation']['operation_id'];digest=self.bodies[op][1]
                n=decimal(row['event_id'].rsplit(':',1)[1],'actual recovery guard ordinal')
                permit_ref=self.permit_at(oid,n,digest)
                source=blueprints[row['after_snapshot']].get('recovery_guard_prepare_source')
                if permit_ref is None or source is None or source['owner_id']!=oid:raise Unavailable('recovery guard lacks exact actual permit/preparation source')
                prepared=self.cut_refs[source['snapshot_id']][3]
                if decimal(prepared['certified_prefix']['journal_sequence'],'actual prepared prefix')!=decimal(digest['prepare_sequence'],'actual prepared slot'):raise Rejected('recovery guard prepare prefix differs from original native slot')
                value['native_permit_observation_blob']=permit_ref;value['prepare_owner_id']=oid
                value['prepare_prefix']=self.prefix(prepared['certified_prefix'])
                replacement=self.blobs.json('DfsNativePublicationGuardObservationV1',value)
                action['authorization_blob']=replacement
                self.header['guard_observation_blobs_by_operation'][op]=replacement
                for blueprint in blueprints.values():
                    for key in ('active_guard_blob','last_guard_blob'):
                        if blueprint.get(key)==reference:blueprint[key]=replacement
        peers_by_name={}
        for name,blueprint in blueprints.items():
            oid,n,_,held=self.cut_refs[name]; cut,folded,cfg,decoded,control_ref,kv=self.control_at(oid,n,blueprint)
            prefix,prefix_ref=self.semantic_prefix(oid,held['certified_prefix'])
            resident_ref=blueprint.get('native_resident_custody_blob')
            actual_guard=blueprint.get('active_guard_blob')
            bound=None
            if 'requested_writer_epoch_view' in held:
                request=held['requested_writer_epoch_view'];instance=held['backend_instance_authority_epoch']
                expected=request if request is not None and request==instance else None
                if held['writer_bound_epoch']!=expected:raise Rejected('normal epoch view does not match its actual getter argument/instance')
            if actual_guard:
                actual=self.guard_source(actual_guard,oid,n,active=True)
                expected=actual['facts']['authority']['expected']['epoch'];instance=held['backend_instance_authority_epoch']
                if instance is not None and instance!=expected:raise Rejected('actual guard lease targets another observed instance epoch')
                # An explicitly unbound store remains storage DATA. Only this
                # actual validated native guard supplies its writer capability.
                bound=expected
            bindings=dict(owner_id=oid,bound_epoch=bound,validated_under_real_writer=resident_ref is None,
                actual_backend_instance_authority_epoch=held['backend_instance_authority_epoch'],
                actual_storage_epoch_view=held['writer_bound_epoch'],
                actual_publication_guard_observation_blob=actual_guard)
            if resident_ref:bindings['native_resident_custody_blob']=resident_ref
            self.require_owned_cut(name,blueprint)
            self.guard_source(blueprint.get('active_guard_blob'),oid,n,active=True)
            self.guard_source(blueprint.get('last_guard_blob'),oid,n)
            peers_by_name[name]=dict(owner_id=oid,prefix=prefix,held_snapshot_id=name,prefix_certificate_blob=prefix_ref,writer_binding=bindings)
            durable_event=blueprint.get('durable_ingest_source')
            durable_prefix=None
            if durable_event:
                key=(durable_event['owner_id'],decimal(durable_event['owner_ordinal'],'ingest ordinal'))
                event=self.events.get(key)
                if key[0]!=oid or key[1]>n or event is None or event['collection']!='boundary_packets':
                    raise Rejected('replica durable cut borrows another owner/future packet')
                fact=event['record']['facts']
                if fact['phase']!='replica-journal-persisted' or fact['local_node_id']!=self.owners[oid]['node_id']:
                    raise Rejected('replica durable cut did not come from actual validated receiver persistence')
                durable_prefix,durable_ref=self.semantic_prefix(oid,fact['durable_prefix'])
                if decimal(durable_prefix['journal_sequence'],'durable prefix')<decimal(prefix['journal_sequence'],'materialized prefix'):
                    raise Rejected('materialized replica cut exceeds observed journal durability')
                if ASSEMBLER.native_prefix(durable_prefix)!=ASSEMBLER.native_prefix(prefix):
                    if not fact['materialization_pending']:
                        raise Rejected('receiver durable report falsely labels a lagging table cut materialized')
                    semantic=self.native(durable_ref)
                    ingest=self.certificate(owner_id=oid,recipient_node_id=self.owners[oid]['node_id'],
                        after_held_snapshot_id=name,durable_journal_prefix=durable_prefix,materialized_prefix=prefix,
                        actual_ingest_permit_tuple=fact['permit'],actual_source_config=self.config(fact['actual_source_config']),
                        actual_pending_handoff_blob=self.blobs.json('DfsMetadataShardMembershipHandoff',fact['actual_pending_handoff']) if fact['actual_pending_handoff'] else None,
                        materialization_pending=True,actual_boundary_source=dict(owner_id=oid,owner_ordinal=str(key[1])),
                        validated_frame_decoder_observation_blobs=[item['decoder_observation_blob'] for item in semantic['semantic_prefix']])
                    peers_by_name[name].update(prefix=durable_prefix,prefix_certificate_blob=durable_ref,
                        materialized_prefix=prefix,materialized_prefix_certificate_blob=prefix_ref,
                        durable_ingest_observation_blob=self.blobs.json('DfsNativeReplicaDurableIngestObservationV1',ingest))
            gate=self.freeze_at(oid,n,held,cfg);gate['observed_pending_handoff']=decoded[1] is not None
            completed_initial=self.header.get('initial_completed_handoff')
            if gate['write_freeze_observation']=='observed-present-inactive' and completed_initial:
                proof=completed_initial['proof'];marker=self.native(gate['native_write_freeze_blob'])
                if (proof['desired_shard']!=self.native(cfg['native_config_blob'])
                        or marker['handoff_id']!=proof['handoff_id'] or decoded[1] is not None):
                    raise Rejected('historical inactive marker differs from certified initial active cut')
                gate['initial_completed_handoff_blob']=self.blobs.json('DfsNativeInitialCompletedHandoffV1',self.certificate(actual_completed_handoff=completed_initial))
            operations=self.operations_at(oid,n,held,blueprint)
            typedrefs=[self.blobs.json(kind,value) if value is not None else None for kind,value in zip(
                ('DfsMetadataShardConfig','DfsMetadataShardMembershipHandoff','DfsMetadataShardOperationFloor','DfsMetadataShardHeadWatermark'),decoded)]
            table_ref=self.blobs.json('DfsMetadataTableSnapshotTables',held['tables'])
            memory_owner,memory_n,memory_side=self.memory_sources[name]
            facts=self.certificate(snapshot_owner_id=oid,snapshot_native_prefix=prefix,visible_footprint_complete=True,
                local_prefix_certificate_blob=prefix_ref,
                actual_memory_cut_source=dict(owner_id=memory_owner,owner_ordinal=str(memory_n),side=memory_side),
                actual_cut_as_of_source=dict(owner_id=oid,owner_ordinal=str(n)),
                operation_observations=operations,actual_operational_gate=gate,native_control_cut_blob=control_ref,
                active_publication_guard_blob=blueprint.get('active_guard_blob'),last_publication_guard_blob=blueprint.get('last_guard_blob'))
            self.contexts[name]=dict(owner_id=oid,scope=self.scope,active_config=cfg,committed_config=cfg,
                local_certified_prefix=prefix,materialized_through_sequence=str(held['materialized_through_sequence']),
                prepare_record_blobs=[],commit_record_blobs=[],complete_visible_rows_blob=table_ref,held_tables_blob=table_ref,
                pending_handoff_blob=typedrefs[1],operation_floor_blob=typedrefs[2],raft_head_watermark_blob=typedrefs[3],
                retiring_fence_blob=None,local_marker_blob=gate.get('native_write_freeze_blob'),
                local_writer_binding_blob=self.blobs.json('DfsNativeWriterBindingV1',bindings),kv_read=kv,projection_continuous=True,
                _facts=facts)
            if durable_prefix is not None:
                self.contexts[name]['durable_journal_prefix']=durable_prefix
        nodes=[row['node_id'] for row in self.owners.values()]
        actual_operation_views={name:copy.deepcopy(context['_facts']['operation_observations']) for name,context in self.contexts.items()}
        for name,context in self.contexts.items():
            blueprint=blueprints[name]; selection=blueprint.get('peer_cut_refs')
            if selection is None:
                if len(nodes)==1: selection={nodes[0]:name}
                else: raise Unavailable(f'{name}: complete actual cross-owner peer-cut references absent')
            if set(selection)!=set(nodes): raise Rejected('actual peer cut does not cover every finite owner')
            peers={}
            for node,reference in selection.items():
                if reference not in peers_by_name or self.owners[peers_by_name[reference]['owner_id']]['node_id']!=node:
                    raise Rejected('peer cut references a different actual node/owner')
                selected_owner,selected_n,_,_=self.cut_refs[reference]
                local_owner,local_n,_,_=self.cut_refs[name]
                if selected_owner==local_owner and selected_n>local_n:
                    raise Rejected('peer view borrows a later local observation')
                # Global peer states are the independently reconstructed observer
                # frontier. Actual author/source ACK/W facts stay validated in
                # their existing native guard and receipt paths.
                peers[node]=copy.deepcopy(peers_by_name[reference])
            fact=context.pop('_facts');fact['peer_certified_cuts']=peers
            actual_primary=self.native(context['active_config']['native_config_blob'])['primary_node_id']
            if actual_primary not in selection: raise Rejected('actual primary has no captured peer cut')
            fact['operation_observations']=copy.deepcopy(actual_operation_views[selection[actual_primary]])
            environment=blueprint.get('environment_binding')
            if environment is None:
                if len(nodes)!=1: raise Unavailable('multi-owner cut needs actual response/fault constrained online-set selection')
                environment=dict(origin='existential-environment-constrained-by-native-responses',online_node_ids=nodes,authenticated_response_node_ids=nodes,absence_is_offline=False,actual_receipt_observation_blobs=self.header.get('initial_content_receipt_observation_blobs',[]))
            if environment.get('absence_is_offline'): raise Rejected('missing ACK cannot certify offline node')
            fact['environment_binding']=environment
            # Nonempty control clocks require exact physical-prefix attribution, never owner-label guessing.
            for control_field,prefix_field,owner_field in [('operation_floor_blob','operation_floor_prefix','operation_floor_source_owner_id'),('raft_head_watermark_blob','raft_watermark_prefix','raft_watermark_source_owner_id')]:
                if context[control_field] is not None:
                    attribution=blueprint.get(prefix_field+'_attribution')
                    if attribution is None: raise Unavailable(f'{prefix_field}: actual source-owner prefix attribution missing')
                    snapshot=attribution['snapshot_id'];peer=peers_by_name[snapshot]
                    clock=self.native(context[control_field]);source_owner,source_n,_,source_held=self.cut_refs[snapshot]
                    horizon={context['owner_id']:self.cut_refs[name][1]}
                    for peer_name in selection.values():
                        actor,ordinal,_,_=self.cut_refs[peer_name];horizon[actor]=max(horizon.get(actor,0),ordinal)
                    if source_owner not in horizon or source_n>horizon[source_owner]:
                        raise Rejected('control clock prefix borrows a future or unselected owner cut')
                    materialized=source_held['certified_prefix']
                    sequence=clock['prepared_sequence'] if prefix_field=='operation_floor_prefix' else clock['journal_sequence']
                    if decimal(materialized['journal_sequence'],'attributed prefix')!=decimal(sequence,'actual control clock'):
                        raise Rejected('control clock differs from attributed exact native prefix')
                    if prefix_field=='operation_floor_prefix':
                        records=[row for row in source_held['tables'].get('prepared_operations_v7',[])
                            if row['digest']['payload_digest']==clock['operation_digest']
                            and decimal(row['digest']['prepare_sequence'],'prepare slot')==decimal(sequence,'floor slot')
                            and decimal(row['digest']['owner_epoch'],'record epoch')==decimal(clock['authority_epoch'],'floor epoch')]
                        if len(records)!=1: raise Rejected('floor does not bind the actual captured original preparation')
                    else:
                        if (clock['file_system_id']!=self.scope['file_system_id'] or clock['shard_id']!=self.scope['shard_id']
                                or clock['state_fingerprint']!=materialized['digest']
                                or self.owners[source_owner]['node_id']!=clock['primary_node_id']):
                            raise Rejected('watermark identity/fingerprint differs from actual source owner prefix')
                    fact[prefix_field]=self.prefix(materialized);fact[owner_field]=source_owner
                    if prefix_field=='raft_watermark_prefix': fact['raft_watermark_certificate_blob']=peer['prefix_certificate_blob']
            if context['pending_handoff_blob'] is not None or blueprint.get('handoff_observation'):
                observation=blueprint.get('handoff_observation')
                if observation is None: raise Unavailable('actual pending handoff needs frozen cut/ACK/activation source references')
                source_refs=observation.get('actual_boundary_sources')
                if not source_refs: raise Unavailable('handoff facts need exact actual freeze/ACK/activation boundary references')
                actual=[]
                for source in source_refs:
                    key=(source['owner_id'],decimal(source['owner_ordinal'],'handoff source ordinal'))
                    event=self.events.get(key)
                    if event is None or event['collection']!='boundary_packets': raise Rejected('handoff source is not an actual boundary packet')
                    horizon={context['owner_id']:self.cut_refs[name][1]}
                    for selected_name in selection.values():
                        actor,ordinal,_,_=self.cut_refs[selected_name];horizon[actor]=max(horizon.get(actor,0),ordinal)
                    if key[0] not in horizon or key[1]>horizon[key[0]]:
                        raise Rejected('handoff source borrows a future or unselected owner transition')
                    actual.append(event['record']['facts'])
                pending=self.native(context['pending_handoff_blob']) if context['pending_handoff_blob'] else None
                native_handoffs=[row.get('pending',row.get('handoff')) for row in actual if row['phase'] in ('retiring-freeze-persisted','handoff-activation-completed')]
                if pending is not None and pending not in native_handoffs:
                    raise Rejected('handoff observation differs from normal typed pending record')
                source_handoff=pending or next((row for row in native_handoffs if row is not None),None)
                if source_handoff is None: raise Unavailable('complete actual handoff identity/body missing')
                if self.native(observation['retiring_config']['native_config_blob'])!=source_handoff['retiring_shard'] or self.native(observation['desired_config']['native_config_blob'])!=source_handoff['desired_shard']:
                    raise Rejected('handoff full configurations differ from actual native record')
                if bool(observation['pending_record_present'])!=(pending is not None): raise Rejected('handoff pending-presence flag differs from exact typed control cut')
                activations=[row for row in actual if row['phase']=='handoff-activation-completed']
                for row in activations:
                    proof=row['proof']
                    if row['handoff']!=source_handoff or any(proof[field]!=source_handoff[field] for field in ('cause','handoff_id','retiring_shard','desired_shard')):
                        raise Rejected('activation source belongs to another actual handoff/configuration/cause')
                activated=bool(activations)
                if bool(observation['activation_committed'])!=activated: raise Rejected('handoff activation flag lacks actual successful normal boundary')
                freezes=[]
                for row in actual:
                    if row['phase']!='retiring-freeze-persisted': continue
                    if (row['pending']!=source_handoff or row['freeze']['handoff_id']!=source_handoff['handoff_id']
                            or decimal(row['freeze']['retiring_epoch'],'freeze epoch')!=decimal(source_handoff['retiring_shard']['epoch'],'retiring epoch')):
                        raise Rejected('freeze source belongs to another exact handoff/scope/epoch')
                    freezes.append(row['freeze'])
                if freezes:
                    freeze=freezes[-1]
                    if decimal(observation['frozen_prefix']['journal_sequence'],'frozen prefix')!=decimal(freeze['journal_sequence'],'actual frozen sequence') or observation['frozen_prefix']['digest']!=freeze['state_fingerprint']:
                        raise Rejected('handoff frozen prefix differs from actual persisted freeze return')
                elif decimal(observation['frozen_prefix']['journal_sequence'],'frozen prefix'):
                    raise Unavailable('handoff frozen cut lacks actual persisted freeze result')
                observed_nodes=set()
                for row in actual:
                    if row['phase']=='source-journal-response-accepted' and row['response'].get('verified_authority_probe_id')==source_handoff['handoff_id']:
                        observed_nodes.add(row['authenticated_source_node_id'])
                self_ack=set()
                if freezes:
                    for source in source_refs:
                        actor=source['owner_id'];ordinal=decimal(source['owner_ordinal'],'freeze source ordinal')
                        event=self.events[actor,ordinal]
                        if event['record']['facts']['phase']=='retiring-freeze-persisted' and self.owners[actor]['node_id']==source_handoff['retiring_shard']['primary_node_id']:
                            self_ack.add(self.owners[actor]['node_id'])
                if not set(observation['authenticated_exact_prefix_ack_nodes']).issubset(observed_nodes|self_ack):
                    raise Rejected('handoff ACKs lack actual correlated source responses or owned exact retiring freeze')
                observation['cause']=source_handoff['cause']
                fact['handoff']=observation
            context['native_operational_facts_blob']=self.blobs.json('DfsShardNativeOperationalFactsV1',fact)
        initial_names=[row['held_snapshot_id'] for row in self.header['owner_cuts'].values()]
        initial_cfg=self.contexts[initial_names[0]]['active_config']; cfg=self.native(initial_cfg['native_config_blob'])
        members=set(cfg.get('replica_node_ids',[])+[cfg['primary_node_id']])
        mounted=[h['normal_mounted_identity'] for h in self.raw_headers.values()]
        roster=mounted[0]['configured_members']
        if any(m['cluster_id']!=mounted[0]['cluster_id'] or m['configured_members']!=roster for m in mounted):
            raise Unavailable('actual mounted membership cuts disagree; exact same native voter cut required')
        voters=[m['node_id'] for m in roster if m['role']=='voter' and m['state']=='voter']
        content = [p['prepared_intent'] for p,_,_ in self.bodies.values() if p['prepared_intent'] is not None]
        if not content: raise Unavailable('finite profile requires one genuine sealed content plan')
        first=content[0];plan=first['desired_replica_node_ids'];w=first['policy']['min_commit_replicas']
        if any(intent['desired_replica_node_ids']!=plan or intent['policy']['min_commit_replicas']!=w for intent in content):
            raise Unavailable('selected content bodies require one actual finite content policy/plan')
        replacement=members
        for context in self.contexts.values():
            pending=context['pending_handoff_blob']
            if pending:
                desired=self.native(pending)['desired_shard'];replacement=set(desired.get('replica_node_ids',[])+[desired['primary_node_id']])
        profile=dict(nodes=nodes,raft_voter_node_ids=voters,initial_primary_node_id=cfg['primary_node_id'],
            initial_metadata_members=sorted(members),replacement_metadata_members=sorted(replacement),
            content_replica_node_ids=plan,min_commit_replicas=w,
            sealed_content_requirements={op:dict(canonical_payload_blob=ref,native_digest=digest,
                requirement=ASSEMBLER.SUBTREE.PROJECTOR.sealed_content_requirement(
                    payload,digest,self.blobs.values[ref]['data'].encode()))
                for op,(payload,digest,ref) in self.bodies.items()})
        owner_cuts=copy.deepcopy(self.header['owner_cuts'])
        for cut in owner_cuts.values():cut['native_prefix']=self.prefix(cut['native_prefix'])
        baseline=self.certificate(complete_native_visibility=True,profile=profile,config_fingerprints=self.configs,
            initial_completed_handoff=self.header.get('initial_completed_handoff'),
            owner_cuts=owner_cuts,unmodeled_operations=self.header['unmodeled_operations'],
            actual_initial_source_observations=self.header['initial_source_observations'],source_artifact_binding_blob=self.header['source_artifact_binding_blob'])
        self.header['initial_cut']=dict(baseline_snapshot_ids=initial_names,native_prefix_by_owner={oid:self.prefix(row['native_prefix']) for oid,row in self.header['owner_cuts'].items()},
            baseline_validation_blob=self.blobs.json('DfsNativeInitialCutCertificateV1',baseline),compaction_identity_blobs=[])
        self.header['snapshot_contexts']=self.contexts;self.header['blobs']=self.blobs.values
        return self.header


def construct(inputs,skeleton):
    if inputs.get('native_operational_inputs_version')!=2:
        raise Unavailable('current native contexts require independently packaged V2 inputs')
    return NativeContexts(inputs,skeleton).construct()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs',type=Path,required=True)
    parser.add_argument('--skeleton',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    try:
        value=construct(ASSEMBLER.CHECK.read_json(args.inputs),ASSEMBLER.CHECK.read_json(args.skeleton))
        args.output.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    except Unavailable as exc:
        print(json.dumps({'status':'unavailable','reason':str(exc)}));return 2
    except (Rejected,KeyError,TypeError,ValueError) as exc:
        print(json.dumps({'status':'rejected','reason':str(exc)}));return 1
    print(json.dumps({'status':'constructed-native-contexts','output':str(args.output),'conformance':'not yet checked'}))
    return 0


if __name__=='__main__':raise SystemExit(main())
