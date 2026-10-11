#!/usr/bin/env python3
"""Build a finite actual owner/event skeleton; no predicted model state."""
from __future__ import annotations
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path

HERE=Path(__file__).resolve().parent

def load(name,file):
    spec=importlib.util.spec_from_file_location(name,HERE/file);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

A=load('skeleton_assembler','assemble-shard-native-trace.py')
COLLECT=load('skeleton_inputs','collect-shard-native-inputs.py')
CAUSAL=load('skeleton_causality','shard-native-causality.py')
Rejected,Unavailable=A.Rejected,A.Unavailable
KINDS={'packets':'kernel','authoritative_reads':'authoritative-read','guard_events':'guard',
       'content_receipts':'content-receipt','configuration_cuts':'configuration-cut','metadata_cache_cuts':'metadata-cache-cut',
       'boundary_packets':'boundary','control_commits':'control-commit','original_control_commits':'original-control-commit'}

def u64(value,label):
    if type(value)is int:
        if not 0<=value<=2**64-1:raise Rejected(label+' outside u64')
        return value
    return A.CHECK.decimal_u64(value,label)

def build_skeleton(inputs):
    """Verify manifest/executable/source anew, then build the current skeleton."""
    if inputs.get('native_operational_inputs_version')!=2:
        raise Unavailable('current event builder requires independently packaged V2 native inputs')
    source=inputs.get('artifact_source_inputs')
    if source is None:raise Unavailable('actual manifest/source/executable inputs missing')
    actual=COLLECT.collect_native_inputs(source['manifest_path'],inputs['artifact_binding'],inputs['selected_operation_ids'],
        source_manifest_path=source['source_manifest_path'],executable=source.get('executable'))
    if actual!=inputs:raise Rejected('V2 inputs differ from independently reread actual capture files')
    return _build_verified_capture(actual)


def _build_verified_capture(inputs):
    """Internal actual-data builder; public entry verifies complete artifacts."""
    return FiniteCaptureBuilder(inputs).build()


class FiniteCaptureBuilder:
    """Select finite actual cuts and causality; never impose a native global clock."""
    def __init__(self, inputs):
        self.inputs = inputs
        self.scope = inputs['scope']
        self.binding = inputs['artifact_binding']
        self.selected = inputs['selected_operation_ids']
        if not 1 <= len(inputs['owners']) <= 4 or not 1 <= len(self.selected) <= 3:
            raise Unavailable('finite native bounds require 1–4 owners and 1–3 actual operations')
        self.captured = {row['owner_id']: row for row in inputs['owners']}
        self.raw_bytes = {oid: Path(row['raw_path']).read_bytes() for oid,row in self.captured.items()}
        self.raw = {oid: A.native_json(data,'actual capture') for oid,data in self.raw_bytes.items()}
        self.headers = {oid: A.CHECK.read_json(Path(row['header_path'])) for oid,row in self.captured.items()}
        self.events = {}
        for oid,row in self.captured.items():
            if (hashlib.sha256(self.raw_bytes[oid]).hexdigest()!=row['raw_sha256']
                    or hashlib.sha256(Path(row['header_path']).read_bytes()).hexdigest()!=row['header_sha256']
                    or self.headers[oid]['artifact_binding']!=self.binding):
                raise Rejected('finite capture raw/header binding changed')
            for event in inputs['owner_local_events']:
                if event['owner_id']!=oid: continue
                n=u64(event['owner_ordinal'],'owner ordinal'); key=(oid,n)
                if key in self.events or self.raw[oid][event['collection']][event['index']]!=event['record']:
                    raise Rejected('finite capture packet differs or repeats its actual source')
                self.events[key]=event
            actual=A.validate_capture(self.raw[oid])
            if {n for owner,n in self.events if owner==oid}!={r['ordinal'] for r in actual}:
                raise Rejected('finite input omits an actual owner-local observation')
        self.blobs=A.Blobs({}); self.owners=[]; self.owner_map={}; self.custody={}
        for oid,row in self.captured.items():
            guards=[e for (owner,n),e in self.events.items() if owner==oid and e['collection']=='guard_events'
                    and e['record']['phase']=='admitted' and e['record']['matches_actual_mounted_owner']]
            if guards:
                selected_guards=[e for e in guards if e['record']['digest']['operation_id'] in self.selected]
                # The finite interval begins at the selected actual operation.
                # Terminal bootstrap may have used an earlier actual scheduler;
                # its complete facts remain independently certified initial data.
                guard=min(selected_guards or guards,key=lambda e:e['owner_ordinal'])['record'];physical=guard['facts']['physical_scope'];execution=guard['facts'].get('execution')
                if execution is None or physical.get('cached_root_identity') is None:
                    raise Unavailable('actual author guard omits native physical/execution identity')
                root=physical['canonical_root']; generation='native-directory:'+':'.join(physical['cached_root_identity']);lane=execution['admission_scheduler_handle'];lease='mounted-owner:'+oid
            else:
                packets=[e for (owner,n),e in self.events.items() if owner==oid and e['record'].get('resident_custody')]
                if not packets:
                    raise Unavailable(oid+': actual peer resident/admission custody missing; SAME resident return collector hook required')
                # The checked interval begins with the selected operation.
                # Bootstrap can use an earlier real resident slot; it remains
                # initial raw history, not the selected interval's held owner.
                selected_packets = []
                for event in packets:
                    facts = event['record']['facts']
                    if facts['phase'] not in ('replica-journal-persisted', 'replica-prefix-probe-accepted'):
                        continue
                    command = facts['command']
                    operation = (command.get('authority_probe') or {}).get('operation', {})
                    decoded = [row['decoded_entry'] for row in self.headers[oid].get('actual_received_frame_decodes', [])
                               if u64(row['source_owner_ordinal'], 'receiver decoder ordinal') == event['owner_ordinal']]
                    if (operation.get('operation_id') in self.selected
                            or any(row['mutation'].get('operation_id') in self.selected for row in decoded)):
                        selected_packets.append(event)
                packet=min(selected_packets or packets,key=lambda e:e['owner_ordinal']);custody=packet['record']['resident_custody'];self.validate_custody(custody,packet)
                root=custody['backend_root'];generation='resident-slot:'+custody['resident_handle_token'];lane=custody['admission_scheduler_token'];lease='mounted-resident-owner:'+custody['data_owner_token'];self.custody[oid]=custody
            if not root.startswith(row['actual_data_root']+'/') or not lane:
                raise Rejected('actual finite owner scope is outside its mounted data root')
            owner=dict(owner_id=oid,node_id=row['node_id'],canonical_root=root,root_lease_token=lease,component_incarnation=oid,storage_generation_token=generation,lane_incarnation=lane)
            self.owners.append(owner);self.owner_map[oid]=owner
        self.nodes={owner['node_id']:owner['owner_id'] for owner in self.owners}
        if len(self.nodes)!=len(self.owners):raise Rejected('finite capture repeats actual node identity')
        self.blueprints={};self.cuts={};self.initial={};self.operations={};self.semantic={oid:[] for oid in self.captured};self.intervals=[];self.timeline=[];self.graph={};self.covered=set();self.configs={};self.guards={}

    def cert(self,**fields):
        return dict(scope=self.scope,implementation_revision=self.binding['implementation_revision'],owner_bindings=self.owners,complete=True,continuous=True,**fields)

    def validate_custody(self,custody,event):
        row=self.captured[event['owner_id']]
        if (custody['actual_mount_label']!=event['owner_id'] or custody['local_node_id']!=row['node_id']
                or any(custody[k]!=self.scope[k] for k in ('file_system_id','shard_id'))
                or custody['admission_kind'] not in ('exact-shard','global-exclusive')
                or not all(custody.get(k) for k in ('resident_handle_token','data_owner_token','admission_scheduler_token'))
                or custody['observer_owner_ordinal']!=str(event['owner_ordinal'])):
            raise Rejected('peer resident custody borrows another actual owner/scope/packet')
        admitted=custody['admitted_scope']
        if admitted['kind']=='exact-shard' and any(admitted[k]!=self.scope[k] for k in ('file_system_id','shard_id')):
            raise Rejected('actual resident admission names another finite shard')
        if admitted['kind'] not in ('exact-shard','global-exclusive'):
            raise Rejected('read-only/namespace hint cannot replace actual replica admission')

    def memory(self,event,side):
        record=event['record'];oid=event['owner_id']
        encoded=A.SUBTREE.subtree(self.raw_bytes[oid],[event['collection'],event['index'],'held']) if side=='admission' else A.byte_vector(record[side+'_held_json'],'actual finite held cut')
        held=A.native_json(encoded,'actual finite held cut')
        if held['backend_root']!=self.owner_map[oid]['canonical_root'] or held['shard_id']!=self.scope['shard_id']:
            raise Rejected('finite held source belongs to another actual root/shard')
        return encoded,held

    def cut(self,event,side,*,asof=None,guard=None,last=None):
        oid=event['owner_id'];n=event['owner_ordinal'];end=(asof or event)['owner_ordinal'];name=oid+':cut:'+str(n)+':'+side+':asof:'+str(end)
        data,held=self.memory(event,side)
        value=dict(held=held,native_source=dict(owner_id=oid,owner_ordinal=str(n),side=side),as_of_source=dict(owner_id=oid,owner_ordinal=str(end),side='before' if side=='before' and end==n else 'after'),active_guard_blob=guard,last_guard_blob=last)
        if event['collection']=='boundary_packets' and event['record']['facts']['phase'] in ('replica-journal-persisted','replica-prefix-probe-accepted','replica-materialization-completed','replica-write-freeze-read'):
            custody=event['record'].get('resident_custody')
            if custody is None:raise Unavailable('actual replica cut lacks SAME resident/admission custody packet')
            self.validate_custody(custody,event)
            value['native_resident_custody_blob']=self.blobs.json('DfsNativeResidentReplicaCustodyV1',self.cert(owner_id=oid,actual_boundary_source=dict(owner_id=oid,owner_ordinal=str(n)),actual_custody=custody))
            if side=='after' and event['record']['facts']['phase']=='replica-journal-persisted':value['durable_ingest_source']=dict(owner_id=oid,owner_ordinal=str(n))
        if name in self.blueprints and self.blueprints[name]!=value:raise Rejected('one actual finite cut acquired contradictory contexts')
        self.blueprints[name]=value;self.cuts[name]=(event,side,end,data,held);return name

    def prefix(self,value):
        answer=A.native_prefix(value);answer['native_prefix_blob']=self.blobs.json('DfsMetadataPrefixProof',value);return answer

    def config(self,cfg,entry):
        if any(cfg[k]!=self.scope[k] for k in ('file_system_id','shard_id')):raise Rejected('finite config crosses actual scope')
        members=cfg.get('replica_node_ids',[])+[cfg['primary_node_id']]
        if len(set(members))!=len(members) or not set(members)<=set(self.nodes):raise Unavailable('complete current metadata member capture coverage missing')
        fp='native-config-sha256:'+hashlib.sha256(A.byte_vector(entry['value'],'actual config entry')).hexdigest()
        result=dict(file_system_id=cfg['file_system_id'],shard_id=cfg['shard_id'],primary_node_id=cfg['primary_node_id'],replica_node_ids=cfg.get('replica_node_ids',[]),epoch=str(cfg['epoch']),assignment_blob=self.blobs.json('DfsMetadataShardAssignment',cfg['assignment']),default_policy_blob=self.blobs.json('DfsObjectPolicy',cfg['default_policy']),native_config_blob=self.blobs.json('DfsMetadataShardConfig',cfg),canonical_fingerprint=fp)
        if fp in self.configs and self.configs[fp]!=result:raise Rejected('same original config bytes have different typed result')
        self.configs[fp]=result;return result

    def one(self,oid,collection,phase,op):
        found=[e for (owner,n),e in self.events.items() if owner==oid and e['collection']==collection and e['record'].get('phase')==phase and e['record'].get('operation',e['record'].get('digest',{})).get('operation_id')==op]
        if len(found)!=1:raise Unavailable('finite operation needs unique actual '+phase+' boundary')
        return found[0]

    def guard(self,event):
        raw=event['record'];oid=event['owner_id'];facts=raw['facts'];authority=facts['authority']
        physical=facts['physical_scope'];execution=facts.get('execution');owner=self.owner_map[oid]
        if (raw['matches_actual_mounted_owner'] is not True or raw.get('held') is None or execution is None
                or physical['canonical_root']!=owner['canonical_root'] or any(physical[k]!=self.scope[k] for k in ('file_system_id','shard_id'))
                or facts['actor_node_id']!=owner['node_id'] or physical.get('cached_root_identity') is None
                or not all(execution.get(k) for k in ('execution_owner_handle','admission_scheduler_handle','execution_scope_debug'))
                or execution['admission_scheduler_handle']!=owner['lane_incarnation']):
            raise Unavailable('selected guard lacks matching actual owner/physical/admission custody')
        if authority['mode'] not in ('full-shard','recovery'):raise Unavailable('finite clustered builder needs full-shard or recovery native authority')
        if authority['mode']=='recovery':
            expected=authority['expected'];members=set(expected['replica_node_ids']+[expected['primary_node_id']]);acks=set(authority['authenticated_acknowledged_node_ids'])
            if (authority['required_metadata_quorum']!=len(members)//2+1 or not acks<=members
                    or expected['primary_node_id'] not in acks or len(acks)<authority['required_metadata_quorum']):
                raise Rejected('actual recovery guard lacks exact configured metadata majority')
        reads=[e for (owner,n),e in self.events.items() if owner==oid and n<event['owner_ordinal'] and e['collection']=='authoritative_reads' and e['record'].get('guard_context_id')==raw['guard_id']]
        if len(reads)!=1:raise Unavailable('finite author guard lacks unique SAME normal final read')
        read_event=reads[0];read=read_event['record'];key=f"/dfs/filesystems/{self.scope['file_system_id']}/metadata-shards/{self.scope['shard_id'].strip('/').replace('/','_')}";handoff=key.replace('/metadata-shards/','/metadata-membership-handoffs/')
        if read['requested_keys']!=[key,handoff] or len(read['read']['value'])!=2 or read['read']['value'][1] is not None:raise Unavailable('guard lacks exact configuration/missing handoff read')
        entry=read['read']['value'][0];cfg=authority['expected']
        decodes=[r for r in self.headers[oid]['actual_control_decodes'] if r['exact_entry']==entry and r['native_type']=='DfsMetadataShardConfig' and r['decoded_value']==cfg]
        if not decodes or cfg['primary_node_id']!=self.owner_map[oid]['node_id']:raise Rejected('native guard config lacks normal decoder/source-primary match')
        config=self.config(cfg,entry);read_ref=self.blobs.json('KvAuthoritativeRead',read['read'])
        freeze=raw['held']['write_freeze']
        if freeze!={'observation':'absent'}:
            marker=freeze.get('value')
            if (freeze.get('observation')!='present' or not marker
                    or marker.get('activated_epoch')!=cfg['epoch']
                    or marker['retiring_epoch']>=cfg['epoch']
                    or raw['held']['backend_instance_authority_epoch']!=cfg['epoch']):
                raise Unavailable('finite guard has active/unproved retiring freeze; explicit handoff actions required')
            # This candidate remains a PRESENT marker. The complete initial
            # bootstrap validator below must bind actual activation/rebind/ACKs.
        prior_guards=[e for e in self.events.values() if e['owner_id']==oid and e['collection']=='guard_events'
            and e['record']['phase']=='admitted' and e['owner_ordinal']<event['owner_ordinal']
            and not any(r['owner_id']==oid and r['collection']=='guard_events' and r['record']['phase']=='released'
                        and r['record']['guard_id']==e['record']['guard_id'] and r['owner_ordinal']<event['owner_ordinal'] for r in self.events.values())]
        if prior_guards:raise Unavailable('nested actual publication leases need their explicit source interval')
        witness=self.cert(digest=raw['digest'],guard_id=raw['guard_id'],authorization_mode=authority['mode'],expected_config=config,authenticated_prepare_ack_node_ids=authority['authenticated_acknowledged_node_ids'],actual_writer_lock_retained=True,raw_scoped_fence_retained=True,execution_and_data_owner_retained=True,exact_config_and_missing_handoff_read=True,admission_pending_handoff=False,admission_write_frozen=False,prior_writer_bound_epoch=None,actual_prior_storage_epoch_view=raw['held']['writer_bound_epoch'],actual_native_guard_blob=self.blobs.json('NativeGuardEventV1',raw),actual_physical_scope=facts['physical_scope'],actual_execution=facts['execution'],same_call_native_read_blob=read_ref)
        exact=dict(requested_keys=read['requested_keys'],rows=[dict(key=k,entry_blob=self.blobs.json('KvEntry',e) if e else None) for k,e in zip(read['requested_keys'],read['read']['value'])],revision=str(read['read']['authority']['revision']),applied_index=read['read']['authority'].get('applied_index'),native_authority_evidence_blob=read_ref,complete=True,continuous=True)
        return self.blobs.json('DfsNativePublicationGuardObservationV1',witness),config,exact,read_event

    def action(self,event,before,after,action,refs,dependencies=()):
        key=(event['owner_id'],event['owner_ordinal']);eid=key[0]+':actual-event:'+str(key[1])
        if eid in self.graph:raise Rejected('actual native transition is grouped twice')
        value=dict(event_id=eid,owner_id=key[0],turn_id=action.get('operation',{}).get('operation_id',action.get('operation_id',action.get('kind'))),caused_by=[],before_snapshot=before,after_snapshot=after,action=action,raw_references=[dict(actual_mount_label=e['owner_id'],owner_ordinal=str(e['owner_ordinal'])) for e in refs])
        self.graph[eid]=set(dependencies);self.timeline.append(value)
        for ref in refs:
            source=(ref['owner_id'],ref['owner_ordinal'])
            if source in self.covered:raise Rejected('actual packet was assigned twice')
            self.covered.add(source)
        return eid

    def prior_selected_guard(self, owner, before):
        admissions = [event for event in self.events.values()
            if event['owner_id'] == owner and event['owner_ordinal'] < before
            and event['collection'] == 'guard_events' and event['record']['phase'] == 'admitted'
            and event['record']['digest']['operation_id'] in self.selected]
        if not admissions:
            return None
        admitted = max(admissions, key=lambda event: event['owner_ordinal'])
        released = [event for event in self.events.values()
            if event['owner_id'] == owner and admitted['owner_ordinal'] < event['owner_ordinal'] < before
            and event['collection'] == 'guard_events' and event['record']['phase'] == 'released'
            and event['record']['guard_id'] == admitted['record']['guard_id']
            and event['record']['digest'] == admitted['record']['digest']]
        if len(released) != 1:
            raise Unavailable('prior selected guard lacks its exact actual release before preparation')
        return self.guard(admitted)[0]

    def select_operations(self):
        for op in self.selected:
            sources=[e for e in self.events.values() if e['collection']=='packets' and e['record']['phase']=='prepare-durable' and e['record']['operation']['operation_id']==op]
            if len(sources)!=1:raise Unavailable('selected body lacks unique original native writer prepare')
            pd=sources[0];oid=pd['owner_id'];pm=self.one(oid,'packets','prepare-materialized',op);gd=self.one(oid,'guard_events','admitted',op);cd=self.one(oid,'packets','commit-durable',op);cm=self.one(oid,'packets','commit-materialized',op);gr=self.one(oid,'guard_events','released',op);digest=pd['record']['operation']
            stages=[pd,pm,gd,cd,cm,gr]
            if any(e['record'].get('operation',e['record'].get('digest'))!=digest for e in stages) or [e['owner_ordinal'] for e in stages]!=sorted(e['owner_ordinal'] for e in stages):raise Rejected('native body/digest/lifetime changes between finite boundaries')
            data,held=self.memory(pm,'after');rows=[(i,r) for i,r in enumerate(held['tables']['prepared_operations_v7']) if r['digest']==digest]
            if len(rows)!=1:raise Rejected('actual prepared table cut lacks selected exact body')
            index,record=rows[0];body=self.blobs.put('DfsPreparedOperationPayloadCanonicalV7',A.SUBTREE.canonical_prepared_payload(data,['tables','prepared_operations_v7',index,'payload'],digest['payload_digest']));operation=dict(operation_id=op,owner_epoch=str(digest['owner_epoch']),native_digest=digest['payload_digest'],payload_blob=body)
            guard_ref,config,exact,read=self.guard(gd);self.guards[op]=guard_ref
            prior=self.memory(gd,'admission')[1];preappend=self.memory(cd,'before')[1]
            if prior!=preappend:
                if (cd['owner_ordinal']!=gd['owner_ordinal']+1
                        or not A.native_metadata_memory_equal(prior,preappend)):
                    raise Unavailable('actual guard/preappend difference lacks copied normal view-argument provenance')
            prior_guard=self.prior_selected_guard(oid,pd['owner_ordinal'])
            before=self.cut(pd,'before',last=prior_guard);prepared=self.cut(pm,'after',last=prior_guard);admitted=self.cut(gd,'admission',guard=guard_ref,last=guard_ref);preappend_cut=self.cut(cd,'before',guard=guard_ref,last=guard_ref);committed=self.cut(cm,'after',guard=guard_ref,last=guard_ref);released=self.cut(cm,'after',asof=gr,last=guard_ref)

            final=self.memory(cm,'after')[1];records=[r for r in final['tables']['prepared_operations_v7'] if r['digest']==digest]
            if len(records)!=1 or records[0]['status']!='committed':raise Rejected('finite body has no actual terminal commit')
            prepare_id=self.action(pm,before,prepared,dict(kind='prepare-payload',operation=operation,outcome='applied',prepare_record_blob=self.blobs.json('DfsPreparedOperationRecord',record)),[pd,pm])
            authority=gd['record']['facts']['authority'];guard_dependencies=[prepare_id];guard_before=prepared
            if authority['mode']=='recovery':
                issued=[e for e in self.events.values() if e['owner_id']==oid and e['collection']=='boundary_packets'
                    and e['record']['facts']['phase']=='recovery-permit-issued'
                    and pm['owner_ordinal']<e['owner_ordinal']<gd['owner_ordinal']
                    and e['record']['facts']['permit']==authority['native_permit']]
                if len(issued)!=1:raise Unavailable('recovery guard lacks one SAME actual returned operation permit')
                permit_event=issued[0];fact=permit_event['record']['facts'];native=fact['permit'].get('native_commit')
                if fact['expected']!=authority['expected'] or set(fact['authenticated_recovery_node_ids'])!=set(authority['authenticated_acknowledged_node_ids']):raise Rejected('recovery guard changes its actual permit-return scope/ACKs')
                calls=[e for e in self.events.values() if e['owner_id']==oid and e['collection']=='control_commits'
                    and pm['owner_ordinal']<e['owner_ordinal']<permit_event['owner_ordinal'] and e['record']['commit']==native]
                if native is None or len(calls)!=1:raise Unavailable('fallback lacks SAME actual native transaction request/result observation')
                module,_=A.initial_native_verifiers(self.scope,self.owners,
                    {(owner,n):dict(kind=KINDS[event['collection']],ordinal=n,record=event['record']) for (owner,n),event in self.events.items()})
                voters={member['node_id'] for member in self.headers[oid]['normal_mounted_identity']['configured_members']
                    if member['role']=='voter' and member['state']=='voter'}
                original=module.original_for_replay(self.scope,self.owner_map[oid],self.headers[oid],
                    {(owner,n):dict(kind=KINDS[event['collection']],ordinal=n,record=event['record']) for (owner,n),event in self.events.items()},
                    (oid,calls[0]['owner_ordinal']),calls[0]['record'],voters,required=False)
                effect_ordinal=original[1] if original is not None else calls[0]['owner_ordinal']
                refs=([self.events[original]] if original is not None else [])+[calls[0],permit_event]
                earlier=[e for e in self.events.values() if e['owner_id']==oid and pm['owner_ordinal']<=e['owner_ordinal']<effect_ordinal]
                if not earlier:raise Unavailable('fallback native transaction lacks actual pre-original source cut')
                prior_event=max(earlier,key=lambda e:e['owner_ordinal'])
                before_permit=self.cut(pm,'after',asof=prior_event,last=prior_guard);after_permit=self.cut(permit_event,'after',last=prior_guard)
                for cut_name in (before_permit,after_permit):
                    if not A.native_metadata_memory_equal(self.cuts[prepared][4],self.cuts[cut_name][4]):raise Unavailable('fallback metadata cut changes outside grouped exact preparation')
                for cut_name in (before_permit,after_permit,admitted,preappend_cut,committed,released):
                    self.blueprints[cut_name]['operation_floor_prefix_attribution']=dict(snapshot_id=prepared)
                permit_id=self.action(permit_event,before_permit,after_permit,dict(kind='fallback-permit',operation=operation,
                    permit_id=fact['permit']['permit_id'],expected_config=config,native_quorum_result_blob=None,outcome='applied'),refs,[prepare_id])
                guard_dependencies.append(permit_id);guard_before=after_permit
                self.blueprints[admitted]['recovery_guard_prepare_source']=dict(owner_id=oid,snapshot_id=prepared)
            guard_id=self.action(gd,guard_before,admitted,dict(kind='publication-guard-admitted',operation=operation,guard_id=gd['record']['guard_id'],authorization_blob=guard_ref,expected_config=config,authoritative_exact_read=exact,outcome='applied'),[read,gd],guard_dependencies)
            commit_id=self.action(cm,preappend_cut,committed,dict(kind='commit-digest',operation=operation,guard_id=gd['record']['guard_id'],supplied_digest=digest['payload_digest'],commit_record_blob=self.blobs.json('DfsPreparedOperationCommitReference',records[0]['committed_entry']),outcome='applied'),[cd,cm],[guard_id])
            self.action(gr,committed,released,dict(kind='publication-guard-released',operation_id=op,guard_id=gd['record']['guard_id'],outcome='applied'),[gr],[commit_id])
            self.operations[op]=dict(source_owner_id=oid,digest=digest,payload_blob=body,operation=operation,prepared_source=pm,initial_source=pd,prior_guard=prior_guard,prepare_event_id=prepare_id,guard_event_id=guard_id,commit_event_id=commit_id,committed_source=cm,config=config)

    def verify_edge(self,edge):
        source=self.events.get((edge['source_owner_id'],u64(edge['source_ordinal'],'source ordinal')));receiver=self.events.get((edge['receiver_owner_id'],u64(edge['receiver_ordinal'],'receiver ordinal')))
        if not source or not receiver:raise Rejected('finite request edge lacks actual packets')
        sent=source['record']['facts'];received=receiver['record']['facts'];response=sent['response'];command=received['command'];probe=command.get('authority_probe')
        if (sent['phase']!='source-journal-response-accepted' or received['phase'] not in ('replica-journal-persisted','replica-prefix-probe-accepted')
                or edge['request_id']!=response['request_id'] or response['request_id']!=command['request_id'] or edge['actual_source_response']!=response
                or response['result']!='applied' or sent['authenticated_source_node_id']!=self.owner_map[receiver['owner_id']]['node_id']
                or command['source_node_id']!=self.owner_map[source['owner_id']]['node_id'] or command['file_system_id']!=self.scope['file_system_id']
                or command['batch']['shard_id']!=self.scope['shard_id'] or response['shard_id']!=self.scope['shard_id']
                or response['next_after_sequence']!=command['batch']['next_after_sequence'] or sent['expected_target_node_id']!=sent['authenticated_source_node_id']
                or sent['expected_next_after_sequence']!=response['next_after_sequence'] or sent['expected_prefix_only']!=response['verified_prefix_only']
                or sent['expected_confirmation_id']!=response['verified_authority_probe_id']):raise Rejected('finite ACK edge changes authenticated identity/head/request/proof')
        if probe is None:
            command=received['command'];frames=command['batch']['journal_frames']
            decoded=[row['decoded_entry'] for row in self.headers[receiver['owner_id']].get('actual_received_frame_decodes',[])
                if u64(row['source_owner_ordinal'],'receiver decoder ordinal')==receiver['owner_ordinal']]
            if not any(row['mutation'].get('operation_id') in self.operations for row in decoded):return None
            raw={(oid,n):dict(kind=KINDS[event['collection']],record=event['record']) for (oid,n),event in self.events.items()}
            op,_,refs=A.verify_committed_replication(raw,self.headers,self.owner_map,self.scope,set(self.operations),
                (receiver['owner_id'],receiver['owner_ordinal']),(source['owner_id'],source['owner_ordinal']))
            return op,source,receiver,self.events[refs[-1]]
        if probe['operation'].get('kind')!='prepared-operation':raise Rejected('finite native receiver probe has another typed operation kind')
        if probe['operation']['operation_id'] not in self.operations:return None
        op=probe['operation']['operation_id'];actual=self.operations[op]
        if actual['source_owner_id']!=source['owner_id'] or any(probe['operation'][k]!=actual['digest'][k] for k in ('file_system_id','shard_id','owner_epoch','prepare_sequence','payload_digest','operation_id')):raise Rejected('actual receiver probe binds another immutable prepare')
        if received['phase']=='replica-prefix-probe-accepted':
            if received['response']!=response:raise Rejected('prefix probe reply changed at source')
        else:
            constructed=[e for e in self.events.values() if e['owner_id']==receiver['owner_id'] and e['owner_ordinal']>receiver['owner_ordinal'] and e['collection']=='boundary_packets' and e['record']['facts']['phase']=='replica-journal-response-constructed' and e['record']['facts']['response']==response]
            if len(constructed)!=1:raise Unavailable('persisted receiver lacks exact ACK-floor-complete response return')
        if A.native_prefix(probe['prepared_prefix'])!=A.native_prefix(received.get('durable_prefix',probe['prepared_prefix'])):raise Rejected('receiver durable prefix differs from exact prepare probe')
        return op,source,receiver,constructed[0] if received['phase']=='replica-journal-persisted' else None

    def select_not_sent_journal_attempts(self):
        self.not_sent_starts = set()
        for (oid, n), event in sorted(self.events.items()):
            if event['collection'] != 'boundary_packets' or (oid, n) in self.covered:
                continue
            start = event['record']['facts']
            if start['phase'] not in ('rpc-attempt-started', 'direct-journal-rpc-attempt-started'):
                continue
            target = start.get('target_node_id')
            command = A.actual_journal_rpc_command(start, self.owner_map[oid]['node_id'], target)
            if command is None:
                continue
            operation = (command.get('authority_probe') or {}).get('operation', {})
            actual = self.operations.get(operation.get('operation_id'))
            if actual is None or actual['source_owner_id'] != oid:
                continue
            returns = [row for row in self.events.values()
                       if row['owner_id'] == oid and row['collection'] == 'boundary_packets'
                       and row['record']['facts']['phase'] == A.journal_rpc_return_phase(start)
                       and row['record']['facts'].get('attempt_id') == start.get('attempt_id')]
            if len(returns) != 1 or returns[0]['record']['facts'].get('delivery_state') != 'definitely-not-sent':
                continue
            returned = returns[0]
            guard = self.one(oid, 'guard_events', 'admitted', actual['digest']['operation_id'])
            config = guard['record']['facts']['authority']['expected']
            A.verify_not_sent_journal_attempt(event['record'], returned['record'],
                source_node=self.owner_map[oid]['node_id'], target_node=target,
                digest=actual['digest'], prepared_prefix=self.memory(actual['prepared_source'], 'after')[1]['certified_prefix'],
                prepared_ordinal=actual['prepared_source']['owner_ordinal'], guard_ordinal=guard['owner_ordinal'],
                members=[config['primary_node_id']] + config.get('replica_node_ids', []))
            row = next(row for row in self.timeline if row['event_id'] == actual['guard_event_id'])
            for source in (event, returned):
                key = (oid, source['owner_ordinal'])
                if key in self.covered:
                    raise Rejected('not-sent journal attempt repeats a grouped actual source')
                self.covered.add(key)
                row['raw_references'].append(dict(actual_mount_label=oid, owner_ordinal=str(key[1])))
            self.not_sent_starts.add((oid, n))

    def select_initial_cuts(self):
        for oid in self.captured:
            candidates = [(name, item) for name, item in self.cuts.items()
                          if item[0]['owner_id'] == oid and item[1] == 'before']
            if candidates:
                first_name,first=min(candidates,key=lambda pair:pair[1][0]['owner_ordinal'])
                if first[0]['collection']=='boundary_packets' and first[0]['record']['facts']['phase']=='replica-write-freeze-read':
                    # The initial observation precedes this genuine read. Choose
                    # a captured earlier SAME resident cut; its Unknown remains
                    # Unknown and no future read/source-call enters the frontier.
                    custody=first[0]['record']['resident_custody']
                    earlier=[event for (owner,n),event in self.events.items() if owner==oid
                        and n<first[0]['owner_ordinal'] and event['collection']=='boundary_packets'
                        and event['record']['facts']['phase'] in ('replica-journal-persisted','replica-prefix-probe-accepted','replica-materialization-completed')
                        and event['record'].get('after_held_json') is not None
                        and isinstance(event['record'].get('resident_custody'),dict)
                        and {k:v for k,v in event['record']['resident_custody'].items() if k!='observer_owner_ordinal'}
                            =={k:v for k,v in custody.items() if k!='observer_owner_ordinal'}]
                    if not earlier:raise Unavailable('initial Unknown peer needs an earlier actual SAME resident held cut')
                    first_name=self.cut(max(earlier,key=lambda event:event['owner_ordinal']),'after')
                self.initial[oid]=first_name
                continue
            # A faulted configured member can retain only genuine bootstrap
            # resident cuts. Its terminal table/journal baseline is checked by
            # derive_terminal_baseline below, not fabricated as an empty peer.
            candidates = []
            for (owner, _), event in self.events.items():
                if owner != oid or event['collection'] != 'boundary_packets':
                    continue
                record = event['record']
                if (record['facts']['phase'] not in ('replica-journal-persisted', 'replica-prefix-probe-accepted')
                        or not record.get('resident_custody') or record.get('after_held_json') is None):
                    continue
                _, held = self.memory(event, 'after')
                payloads = {op:A.native_json(self.blobs.values[actual['payload_blob']]['data'].encode(),
                                             'actual selected canonical body')
                            for op,actual in self.operations.items()}
                prior = [dict(kind=KINDS[row['collection']], record=row['record'])
                         for (actor,n),row in self.events.items()
                         if actor == oid and n <= event['owner_ordinal']]
                received = [row['decoded_entry'] for row in self.headers[oid].get('actual_received_frame_decodes', [])
                            if u64(row['source_owner_ordinal'], 'actual received decoder ordinal') <= event['owner_ordinal']]
                if A.selected_effect_in_bootstrap_cut(held, payloads, prior, received):
                    continue
                candidates.append(event)
            if not candidates:
                raise Unavailable(oid + ': actual terminal bootstrap resident cut missing')
            event = max(candidates, key=lambda row: row['owner_ordinal'])
            self.initial[oid] = self.cut(event, 'after')

    def select_receivers(self):
        self.links=[];ingests={}
        for edge in self.inputs['actual_request_response_correlations']:
            matched=self.verify_edge(edge)
            if matched is None:continue
            op,source,receiver,response_return=matched;actual=self.operations[op];fact=receiver['record']['facts'];key=(receiver['owner_id'],receiver['owner_ordinal'])
            calls=[]
            for event in self.events.values():
                if event['owner_id']!=source['owner_id'] or event['owner_ordinal']>=source['owner_ordinal'] or event['collection']!='boundary_packets':continue
                call=event['record']['facts']
                if ((event['owner_id'], event['owner_ordinal']) not in self.not_sent_starts
                        and A.journal_rpc_request_matches(call,fact['command'],self.owner_map[source['owner_id']]['node_id'],self.owner_map[receiver['owner_id']]['node_id'])):calls.append(event)
            if len(calls)!=1:raise Unavailable('received journal request lacks exact source normal RPC-start provenance; source send copy hook required')
            call=calls[0]
            postcommit=fact['command'].get('authority_probe') is None
            required_source=actual['committed_source'] if postcommit else actual['prepared_source']
            if call['owner_ordinal']<=required_source['owner_ordinal']:raise Rejected('source journal request preceded its actual materialized operation')
            returned=[event for event in self.events.values() if event['owner_id']==source['owner_id'] and event['collection']=='boundary_packets'
                and event['record']['facts']['phase']==A.journal_rpc_return_phase(call['record']['facts']) and event['record']['facts']['attempt_id']==call['record']['facts']['attempt_id']]
            if len(returned)!=1 or not call['owner_ordinal']<returned[0]['owner_ordinal']<=source['owner_ordinal']:
                raise Unavailable('accepted source ACK lacks actual completed RPC attempt correlation')
            A.verify_journal_rpc_return(call['record']['facts'],returned[0]['record']['facts'],source['record']['facts']['response'],self.owner_map[source['owner_id']]['node_id'],self.owner_map[receiver['owner_id']]['node_id'])
            before=self.cut(receiver,'before');after=self.cut(receiver,'after')
            if key not in ingests:
                if fact['phase']=='replica-journal-persisted':
                    durable=self.prefix(fact['durable_prefix']);materialized=self.prefix(self.cuts[after][4]['certified_prefix'])
                    witness=self.cert(owner_id=receiver['owner_id'],recipient_node_id=self.owner_map[receiver['owner_id']]['node_id'],after_held_snapshot_id=after,durable_journal_prefix=durable,materialized_prefix=materialized,actual_ingest_permit_tuple=fact['permit'],actual_source_config=actual['config'],actual_pending_handoff_blob=self.blobs.json('DfsMetadataShardMembershipHandoff',fact['actual_pending_handoff']) if fact['actual_pending_handoff'] else None,materialization_pending=fact['materialization_pending'],actual_boundary_source=dict(owner_id=receiver['owner_id'],owner_ordinal=str(receiver['owner_ordinal'])))
                    ingest_ref=self.blobs.json('DfsNativeReplicaDurableIngestObservationV1',witness)
                    refs=[receiver]+([call,returned[0],source,response_return] if postcommit else [])
                    raw={(oid,n):dict(kind=KINDS[event['collection']],ordinal=n,record=event['record'])
                         for (oid,n),event in self.events.items()}
                    reads=[key for key,row in raw.items() if key[0]==receiver['owner_id']
                        and row['kind']=='boundary' and row['record']['facts']['phase']=='replica-write-freeze-read'
                        and row['record']['facts']['read_id']==receiver['record'].get('normal_replica_freeze_read_id')]
                    if len(reads)!=1:raise Unavailable('selected ingest lacks its unique SAME normal freeze read')
                    qualified=A.verify_replica_freeze_read(raw,self.owner_map,self.scope,reads[0])
                    if qualified['ingest_source']!={'owner_id':receiver['owner_id'],'owner_ordinal':str(receiver['owner_ordinal'])}:
                        raise Rejected('selected normal freeze read links another actual ingest')
                    read_event=self.events[reads[0]]
                    read_before=self.cut(read_event,'before');read_after=self.cut(read_event,'after')
                    read_id=self.action(read_event,read_before,read_after,dict(kind='replica-freeze-read',
                        recipient_node_id=fact['local_node_id'],
                        native_freeze_read_observation_blob=self.blobs.json('DfsNativeReplicaFreezeReadObservationV1',self.cert(**qualified))),
                        [read_event],[actual['commit_event_id'] if postcommit else actual['prepare_event_id']])
                    ingest_id=self.action(receiver,before,after,dict(kind='replica-ingest',recipient_node_id=fact['local_node_id'],prefix=durable,native_ingest_observation_blob=ingest_ref),refs,[read_id])
                else:
                    # Prefix-only requests create no new journal data. They
                    # can prove a source ACK, but do not fabricate ingestPrefix.
                    ingest_id=None
                ingests[key]=(ingest_id,before,after)
            ingest_id,_,peer_after=ingests[key]
            if postcommit:
                self.links.append(dict(edge=edge,op=op,ingest_event_id=ingest_id,ack_event_id=None,source=source,receiver=receiver,request_source=call,peer_after=peer_after))
                continue
            prepared_source=actual['prepared_source'];before_source=self.cut(prepared_source,'after',asof=source,last=actual['prior_guard']);after_source=before_source
            prefix=self.prefix(fact.get('durable_prefix',fact['command']['authority_probe']['prepared_prefix']))
            ack=self.cert(owner_id=source['owner_id'],operation_id=op,digest=actual['digest'],authenticated_node_id=self.owner_map[receiver['owner_id']]['node_id'],prefix=prefix,actual_source_reference=dict(owner_id=source['owner_id'],owner_ordinal=str(source['owner_ordinal'])),actual_receiver_reference=dict(owner_id=receiver['owner_id'],owner_ordinal=str(receiver['owner_ordinal'])),actual_command=fact['command'],actual_response=source['record']['facts']['response'])
            refs=[call,returned[0],source]+([response_return] if response_return else [])+([receiver] if ingest_id is None else [])
            ack_id=self.action(source,before_source,after_source,dict(kind='replica-progress',ingest_permit_blob=self.blobs.json('DfsMetadataReplicaIngestPermitObservedTuple',fact['permit']) if fact.get('permit') else self.blobs.json('DfsMetadataAuthorityProbe',fact['command']['authority_probe']),verified_prefix=prefix,replacement=False,outcome='journal-durable-materialization-pending' if fact.get('materialization_pending') else 'applied',native_prepare_ack_blob=self.blobs.json('DfsNativePreparePrefixAckObservationV1',ack)),refs,[ingest_id or actual['prepare_event_id']])
            self.graph[actual['guard_event_id']].add(ack_id)
            for permit_action in self.timeline:
                if permit_action['action']['kind']=='fallback-permit' and permit_action['action']['operation']['operation_id']==op:self.graph[permit_action['event_id']].add(ack_id)
            self.links.append(dict(edge=edge,op=op,ingest_event_id=ingest_id,ack_event_id=ack_id,source=source,receiver=receiver,request_source=call,peer_after=peer_after))
        for op,actual in self.operations.items():
            guard=A.native_json(self.blobs.values[self.guards[op]]['data'].encode(),'actual guard');needed=set(guard['authenticated_prepare_ack_node_ids'])-{self.owner_map[actual['source_owner_id']]['node_id']}
            witnessed={self.owner_map[link['receiver']['owner_id']]['node_id'] for link in self.links if link['op']==op and link['ack_event_id'] is not None}
            if not needed<=witnessed:raise Unavailable('actual full-shard guard ACKs lack complete captured receiver/source causal edges')

    def select_materializations(self):
        seen = set()
        for link in self.links:
            receiver = link['receiver']; owner, n = receiver['owner_id'], receiver['owner_ordinal']
            fact = receiver['record']['facts']
            if fact['phase'] != 'replica-journal-persisted' or not fact['materialization_pending'] or (owner,n) in seen:
                continue
            seen.add((owner,n))
            later = [event for (actor, ordinal), event in self.events.items()
                if actor == owner and ordinal > n and event['collection'] == 'boundary_packets'
                and event['record']['facts']['phase'] == 'replica-materialization-completed']
            if not later:
                raise Unavailable('actual pending ingest lacks its observed materialization completion')
            completed = min(later, key=lambda event: event['owner_ordinal'])
            key = owner, completed['owner_ordinal']
            CAUSAL.qualified_materialization(A, self.events, self.owner_map, self.scope, (owner,n), key)
            before, after = self.cut(completed,'before'), self.cut(completed,'after')
            source = dict(owner_id=owner,owner_ordinal=str(n))
            for name in (before,after):
                self.blueprints[name]['durable_ingest_source'] = source
                self.blueprints[name]['materialization_interval'] = dict(persisted=source,
                    completed=dict(owner_id=owner,owner_ordinal=str(key[1])))
            original = next(row for row in self.timeline if row['event_id']==link['ingest_event_id'])
            witness = A.native_json(self.blobs.values[original['action']['native_ingest_observation_blob']]['data'].encode(),'actual persisted ingest')
            witness = dict(witness,after_held_snapshot_id=after,
                materialized_prefix=self.prefix(self.cuts[after][4]['certified_prefix']),
                actual_materialization_source=dict(owner_id=owner,owner_ordinal=str(key[1])))
            # The pending flag still describes the original R persistence.
            # M is independently copied and qualified; it creates no ACK.
            self.action(completed,before,after,dict(kind='replica-ingest',
                recipient_node_id=fact['local_node_id'],prefix=original['action']['prefix'],
                native_ingest_observation_blob=self.blobs.json('DfsNativeReplicaDurableIngestObservationV1',witness)),
                [completed],[link['ingest_event_id']])

    def initial_horizon(self, oid):
        event, side, _, _, _ = self.cuts[self.initial[oid]]
        # Existing ancestry helpers use an exclusive ordinal horizon. The SAME
        # packet is included for an after-cut, never for its before-cut.
        return event['owner_ordinal'] + (side == 'after')

    def group_timeline(self, raw_index):
        _, verify_edge = A.initial_native_verifiers(self.scope, self.owners, raw_index)
        dag = CAUSAL.NativeExecutionDag(A, self.events, self.owner_map, self.scope,
                                       self.inputs['actual_request_response_correlations'], verify_edge, self.headers)
        # Every actual return before a grouped source ACK contributes its
        # receiver predecessor, including a sibling target's completed call.
        # The same raw DAG owns these edges; no timestamp or peer link is inferred.
        for row in self.timeline:
            key = (row['owner_id'], u64(row['event_id'].rsplit(':', 1)[1], 'event ordinal'))
            horizon = dag.close({oid: key[1] if oid == key[0] else 0 for oid in self.captured})
            for predecessor in self.timeline:
                other = (predecessor['owner_id'], u64(predecessor['event_id'].rsplit(':', 1)[1], 'event ordinal'))
                if other != key and other[1] <= horizon[other[0]]:
                    self.graph[row['event_id']].add(predecessor['event_id'])
        # Preserve every source-local ordering edge. The deterministic order
        # below is only a linearization of this captured causal DAG, not time.
        for oid in self.captured:
            local=sorted([row for row in self.timeline if row['owner_id']==oid],key=lambda r:u64(r['event_id'].rsplit(':',1)[1],'event ordinal'))
            for prior,next_ in zip(local,local[1:]):self.graph[next_['event_id']].add(prior['event_id'])
        by_id={row['event_id']:row for row in self.timeline};done=set();timeline=[]
        while len(done)<len(by_id):
            ready=sorted(key for key in by_id if key not in done and self.graph[key]<=done)
            if not ready:raise Rejected('actual owner-local/request-response causality contains a cycle')
            eid=ready[0];row=by_id[eid];row['caused_by']=sorted(self.graph[eid]);timeline.append(row);done.add(eid)
        self.populate_frames(timeline)
        return dag,timeline

    def build(self):
        self.select_operations();self.select_not_sent_journal_attempts();self.select_receivers();self.select_materializations()
        self.select_initial_cuts()
        raw_index={(oid,n):dict(kind=KINDS[e['collection']],ordinal=n,record=e['record']) for (oid,n),e in self.events.items()}
        cuts,excluded,_=A.derive_terminal_baseline(self.owners,self.scope,self.binding['implementation_revision'],self.headers,raw_index,{oid:dict(held_json=list(self.cuts[name][3]),snapshot_id=name,owner_ordinal=self.initial_horizon(oid)) for oid,name in self.initial.items()},self.selected,self.blobs)
        completed_initial=None
        initial_markers=[self.cuts[name][4]['write_freeze'] for name in self.initial.values()]
        if any(marker!={'observation':'absent'} for marker in initial_markers):
            completed_initial=A.verify_initial_completed_handoff(self.scope,self.owners,self.headers,raw_index,
                {oid:self.cuts[name][4]['certified_prefix'] for oid,name in self.initial.items()},
                {oid:self.initial_horizon(oid) for oid in self.initial},
                self.inputs['actual_request_response_correlations'])
        receipt_refs=self.select_content_returns(raw_index)
        dag,timeline=self.group_timeline(raw_index)
        ingest_refs={row['action']['native_ingest_observation_blob'] for row in timeline
            if row['action'].get('native_ingest_observation_blob')}
        global_basis = dict(snapshot_blueprints=copy.deepcopy(self.blueprints),
                            initial=dict(self.initial), native_timeline=copy.deepcopy(timeline),
                            ingest_observation_blobs={ref:copy.deepcopy(self.blobs.values[ref]) for ref in ingest_refs})
        self.blueprints, self.initial, timeline = CAUSAL.globalize(
            dag, self.blueprints, self.initial, timeline, self.blobs)
        self.cuts = {}
        for name, blueprint in self.blueprints.items():
            source = blueprint['native_source']
            event = self.events[source['owner_id'], u64(source['owner_ordinal'], 'held source')]
            data, held = self.memory(event, source['side'])
            self.cuts[name] = (event, source['side'],
                              u64(blueprint['as_of_source']['owner_ordinal'], 'as-of source'), data, held)
        for oid, name in self.initial.items():
            cuts[oid]['held_snapshot_id'] = name
        initial_sources=[];auxiliary=[]
        for (oid,n),event in self.events.items():
            if (oid,n) in self.covered:continue
            reference=dict(actual_mount_label=oid,owner_ordinal=str(n))
            first=min((e['owner_ordinal'] for e in self.events.values() if e['owner_id']==oid and (oid,e['owner_ordinal']) in self.covered),
                      default=self.cuts[self.initial[oid]][2]+1)
            if event['collection']=='boundary_packets' and event['record']['facts']['phase']=='replica-write-freeze-read':
                # Bootstrap reads remain explicit qualified no-effect sources;
                # they cannot be hidden in the pre-interval effect baseline.
                A.verify_replica_freeze_read(raw_index,self.owner_map,self.scope,(oid,n))
                auxiliary.append(reference)
            elif n<first:initial_sources.append(dict(reference,native_kind=KINDS[event['collection']]))
            elif event['collection']=='boundary_packets' and event['record']['facts']['phase']=='successor-rebound':
                module,_=A.initial_native_verifiers(self.scope,self.owners,raw_index)
                module.verify_auxiliary_rebind(self.scope,self.owners,self.headers,raw_index,(oid,n),completed_initial)
                auxiliary.append(reference)
            elif event['collection']=='original_control_commits':
                module,_=A.initial_native_verifiers(self.scope,self.owners,raw_index)
                module.verify_original_observation(self.scope,self.owners,self.headers,raw_index,(oid,n))
                auxiliary.append(reference)
            elif event['collection'] in ('metadata_cache_cuts','configuration_cuts','authoritative_reads'):auxiliary.append(reference)
            else:raise Unavailable('finite raw transition remains ungrouped: '+oid+':'+str(n)+':'+event['collection'])
        result=dict(classification='finite actual event skeleton; native context/projection/replay not yet checked',native_header_type='DfsNativeShardHeaderV1',envelope_binding=self.binding,source_artifact_binding_blob=self.blobs.json('NativeTraceArtifactBindingV1',self.binding),scope=self.scope,owners=self.owners,actual_data_roots=[r['actual_data_root'] for r in self.captured.values()],snapshot_blueprints=self.blueprints,native_timeline=timeline,semantic_prefix_items_by_owner=self.semantic,finite_dictionary=dict(node_aliases={node:i+1 for i,node in enumerate(self.nodes)},operation_aliases={op:i+1 for i,op in enumerate(self.selected)},operation_payload_aliases={op:{r['digest']['payload_digest']:1} for op,r in self.operations.items()},config_aliases={fp:i+1 for i,fp in enumerate(self.configs)},sequence_cuts=self.sequence_cuts,semantic_intervals=self.intervals),config_tuples=list(self.configs.values()),owner_cuts=cuts,unmodeled_operations=excluded,initial_source_observations=initial_sources,native_auxiliary_observations=auxiliary,guard_observation_blobs_by_operation=self.guards,initial_content_receipt_observation_blobs_by_operation={},blobs=self.blobs.values,environment=[])
        result['global_observer_basis'] = global_basis
        result['initial_completed_handoff']=completed_initial
        result['initial_actual_request_response_correlations']=self.inputs['actual_request_response_correlations']
        result['initial_content_receipt_observation_blobs_by_operation']=receipt_refs
        result['initial_content_receipt_observation_blobs']=list(dict.fromkeys(
            reference for references in receipt_refs.values() for reference in references))
        result['blobs']=self.blobs.values;return result

    def select_content_returns(self,raw_index):
        # Reuse the existing exact manifest/call/coordinator matcher. Its
        # cutoff is the actual pre-Prepare source packet, never another owner's
        # ordinal or a receipt inferred from a placement location.
        result=dict(scope=self.scope,owners=self.owners,envelope_binding=self.binding,
            finite_dictionary=dict(operation_aliases={op:i+1 for i,op in enumerate(self.selected)}))
        seed_contexts={}
        for op,actual in self.operations.items():
            source=actual['initial_source'];fact=self.cert(actual_memory_cut_source=dict(owner_id=source['owner_id'],owner_ordinal=str(source['owner_ordinal']),side='before'),peer_certified_cuts={self.owner_map[actual['source_owner_id']]['node_id']:dict(held_snapshot_id='receipts:'+op)},operation_observations={op:dict(initial_payload_blob=actual['payload_blob'],initial_content_receipt_observation_blobs=[],durable_content_receipt_blobs=[])})
            seed_contexts['receipts:'+op]=dict(active_config=actual['config'],native_operational_facts_blob=self.blobs.json('DfsShardNativeOperationalFactsV1',fact))
        result['snapshot_contexts']=seed_contexts;derived,_=A.derive_initial_local_receipts(result,raw_index,self.blobs);references={}
        for op in self.selected:
            fact=A.native_json(self.blobs.values[derived['receipts:'+op]['native_operational_facts_blob']]['data'].encode(),'actual finite receipts');refs=fact['operation_observations'][op]['initial_content_receipt_observation_blobs'];payload=A.native_json(self.blobs.values[self.operations[op]['payload_blob']]['data'].encode(),'actual finite body')
            if len(refs)!=len(payload['content_receipts']):raise Unavailable('finite sealed W receipts lack exact actual persistence/call/coordinator provenance')
            references[op]=refs
            for reference in refs:
                evidence=A.native_json(self.blobs.values[reference]['data'].encode(),'actual content persistence provenance')
                sources=[self.events[r['actual_mount_label'],u64(r['owner_ordinal'],'content return ordinal')] for r in evidence['actual_raw_references']]
                coordinator=[event for event in sources if event['owner_id']==self.operations[op]['source_owner_id'] and event['collection']=='content_receipts'
                    and event['record']['facts']['origin']=='coordinator-durable']
                if len(coordinator)!=1:raise Rejected('content evidence lacks its exact local coordinator return')
                event=coordinator[0];oid=event['owner_id'];n=event['owner_ordinal']
                if n<self.cuts[self.initial[oid]][0]['owner_ordinal']:continue
                available=[item for item in self.cuts.values() if item[0]['owner_id']==oid and item[0]['owner_ordinal']<n and item[1] in ('after','admission')]
                if not available:raise Unavailable('content transition has no preceding actual held memory source')
                memory=max(available,key=lambda item:item[0]['owner_ordinal']);prior_event=self.events.get((oid,n-1))
                if prior_event is None:raise Rejected('content return has no preceding actual owner-local event')
                last_guard=None
                prior_guards=[(actual['prepared_source']['owner_ordinal'],self.guards[other]) for other,actual in self.operations.items()
                    if actual['source_owner_id']==oid and actual['prepared_source']['owner_ordinal']<n]
                if prior_guards:last_guard=max(prior_guards)[1]
                before=self.cut(memory[0],memory[1],asof=prior_event,last=last_guard);after=self.cut(memory[0],memory[1],asof=event,last=last_guard)
                row=evidence['coordinator_receipt_record'];self.action(event,before,after,dict(kind='content-receipt',operation_id=op,replica_node_id=row['replica_node_id'],
                    native_receipt_blob=self.blobs.json('DfsReplicaReceiptRecord',row),authenticated_response_blob=reference),sources)
        return references

    def populate_frames(self,timeline):
        self.sequence_cuts=[]
        for oid in self.captured:
            anchor=self.cuts[self.initial[oid]][4]['certified_prefix'];number=u64(anchor['journal_sequence'],'initial native head');frames={}
            for (owner,n),event in self.events.items():
                if owner!=oid:continue
                record=event['record'];actual=[]
                if event['collection']=='packets' and record.get('exact_frame') is not None:
                    decoded=[r['decoded_entry'] for r in self.headers[oid]['actual_frame_decodes'] if u64(r['owner_ordinal'],'decode ordinal')==n]
                    if decoded!=[record['exact_entry']]:raise Rejected('finite kernel frame lacks normal exact decode')
                    actual=[(A.byte_vector(record['exact_frame'],'kernel frame'),record['exact_entry'])]
                elif event['collection']=='boundary_packets' and record['facts']['phase']=='replica-journal-persisted':
                    for index,frame in enumerate(record['facts']['command']['batch']['journal_frames']):
                        decoded=[r['decoded_entry'] for r in self.headers[oid].get('actual_received_frame_decodes',[]) if u64(r['source_owner_ordinal'],'decode ordinal')==n and r['frame_index']==index]
                        if len(decoded)!=1:raise Unavailable('finite replica frame lacks exact normal current decoder return')
                        actual.append((A.byte_vector(frame['exact_bytes'],'receiver frame'),decoded[0]))
                for exact,entry in actual:
                    seq=u64(entry['sequence'],'actual frame sequence')
                    if seq<=number:continue
                    if seq in frames and frames[seq][:2]!=(exact,entry):raise Rejected('finite owner repeats divergent exact frame slot')
                    frames.setdefault(seq,(exact,entry,n))
            prior=anchor;self.sequence_cuts.append(dict(owner_id=oid,native_prefix=self.prefix(prior),model_sequence=0))
            for index,seq in enumerate(sorted(frames)):
                if seq!=number+index+1:raise Unavailable('finite exact native suffix has a missing frame')
                exact,entry,n=frames[seq];mutation=entry['mutation'];op=mutation.get('operation_id');kind=mutation['type']
                if op not in self.operations or kind not in ('prepare-operation-v7','commit-prepared-operation-v7'):raise Unavailable('unselected native suffix effect needs explicit baseline/action coverage')
                next_=A.SUBTREE.PROJECTOR.extend_native_prefix(prior,seq,exact);frame_ref=self.blobs.put('DfsMetadataExactJournalFrame',exact,'exact-frame-base64');entry_ref=self.blobs.json('DfsMetadataJournalEntry',entry);decoder=self.blobs.json('DfsNativeJournalDecodeObservationV1',self.cert(owner_id=oid,actual_owner_ordinal=str(n),exact_frame_sha256=hashlib.sha256(exact).hexdigest(),decoded_entry_blob=entry_ref));item=dict(model_sequence=index+1,operation_id=op,owner_epoch=str(self.operations[op]['digest']['owner_epoch']),semantic_kind='prepare' if kind=='prepare-operation-v7' else 'commit',native_entry_blob=entry_ref,exact_frame_blob=frame_ref,decoder_observation_blob=decoder,canonical_payload_blob=self.operations[op]['payload_blob'] if kind=='prepare-operation-v7' else None);self.semantic[oid].append(item)
                self.sequence_cuts.append(dict(owner_id=oid,native_prefix=self.prefix(next_),model_sequence=index+1));prior=next_
            for name,(event,side,end,data,held) in self.cuts.items():
                if event['owner_id']!=oid:continue
                target=held['certified_prefix'];through=u64(target['journal_sequence'],'actual materialized head')-number
                if not 0<=through<=len(self.semantic[oid]):raise Unavailable('finite actual held cut is outside retained exact native suffix')
                check=anchor
                for seq in sorted(frames)[:through]:check=A.SUBTREE.PROJECTOR.extend_native_prefix(check,seq,frames[seq][0])
                if A.native_prefix(check)!=A.native_prefix(target):raise Rejected('finite exact frame ancestry differs from observed native held prefix')
        # Bind each real append/ingest interval to its own physical owner's
        # frames and normal decoder observations. A peer frame is never copied
        # out of the primary's encoder packet.
        for row in timeline:
            kind=row['action']['kind']
            if kind not in ('prepare-payload','commit-digest','replica-ingest'):continue
            oid=row['owner_id'];before=self.cuts[row['before_snapshot']][4]['certified_prefix']
            after=self.cuts[row['after_snapshot']][4]['certified_prefix']
            if kind=='replica-ingest':
                after=row['action']['prefix'];source_n=self.cuts[row['after_snapshot']][2]
                prior=[e for (owner,n),e in self.events.items() if owner==oid and n<source_n and e['collection']=='boundary_packets'
                    and e['record']['facts']['phase']=='replica-journal-persisted']
                if prior:
                    event=max(prior,key=lambda e:e['owner_ordinal']);before=event['record']['facts']['durable_prefix']
                    self.blueprints[row['before_snapshot']]['durable_ingest_source']=dict(owner_id=oid,owner_ordinal=str(event['owner_ordinal']))
                witness_ref=row['action']['native_ingest_observation_blob'];witness=A.native_json(self.blobs.values[witness_ref]['data'].encode(),'actual receiver ingest')
                witness['validated_frame_decoder_observation_blobs']=[item['decoder_observation_blob'] for item in self.semantic[oid]
                    if u64(A.native_json(self.blobs.values[item['native_entry_blob']]['data'].encode(),'actual receiver frame')['sequence'],'frame sequence')<=u64(after['journal_sequence'],'durable receiver head')]
                row['action']['native_ingest_observation_blob']=self.blobs.json('DfsNativeReplicaDurableIngestObservationV1',witness)
            start=u64(before['journal_sequence'],'interval prior');end=u64(after['journal_sequence'],'interval next')
            if end<start:raise Rejected('actual finite native interval regresses')
            items=[item for item in self.semantic[oid] if start<u64(A.native_json(self.blobs.values[item['native_entry_blob']]['data'].encode(),'actual interval frame')['sequence'],'frame sequence')<=end]
            if len(items)!=end-start:raise Unavailable('actual native interval lacks every exact owner-local frame')
            if end==start:continue
            frames=[item['exact_frame_blob'] for item in items];decoded=[]
            for item in items:
                entry=A.native_json(self.blobs.values[item['native_entry_blob']]['data'].encode(),'actual interval entry')
                exact=__import__('base64').b64decode(self.blobs.values[item['exact_frame_blob']]['data'],validate=True)
                decoded.append(dict(sequence=str(entry['sequence']),exact_frame_blake3=A.SUBTREE.PROJECTOR.blake3(exact),native_entry_blob=item['native_entry_blob'],decoder_observation_blob=item['decoder_observation_blob']))
            cert=self.cert(owner_id=oid,event_id=row['event_id'],exact_frame_blobs=frames,decoded_frames=decoded,exact_prefix_chain_validated_by_native_decoder=True)
            self.intervals.append(dict(owner_id=oid,action_event_id=row['event_id'],prior_prefix=self.prefix(before),next_prefix=self.prefix(after),first_sequence=str(start+1),last_sequence=str(end),exact_frame_blobs=frames,complete_interval_certificate_blob=self.blobs.json('DfsNativeJournalIntervalCertificateV1',cert)))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--inputs',type=Path,required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    try:value=build_skeleton(A.CHECK.read_json(args.inputs));args.output.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    except (Unavailable,OSError) as error:print(json.dumps({'status':'unavailable','reason':str(error)}));return 2
    except (Rejected,ValueError,KeyError,TypeError) as error:print(json.dumps({'status':'rejected','reason':str(error)}));return 1
    print(json.dumps({'status':'constructed-actual-event-skeleton','output':str(args.output),'native_conformance':'not checked'}));return 0
if __name__=='__main__':raise SystemExit(main())
