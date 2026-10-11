"""Actual owner-local execution DAG and observed global metadata cut vectors.

These DATA facts describe an observer's linearization, never actor knowledge.
Author guards, immutable preparation and W receipts keep their native validators.
"""
import copy
import heapq


class NativeExecutionDag:
    def __init__(self, api, events, owners, scope, correlations, verify_edge, headers=None):
        self.api, self.events, self.owners, self.scope = api, events, owners, scope
        self.headers=headers
        self.number = lambda value: value if type(value) is int and 0 <= value <= 2**64-1 else api.CHECK.decimal_u64(value, "actual DAG ordinal")
        self.by_node = {row["node_id"]: oid for oid, row in owners.items()}
        if len(self.by_node) != len(owners):
            raise api.Rejected("actual DAG repeats a native node owner")
        self.local = {oid: sorted(n for owner, n in events if owner == oid) for oid in owners}
        self.parents = {key: set() for key in events}
        starts, returns, receivers, accepted = [], {}, [], set()
        for key, event in events.items():
            oid, n = key
            record = event["record"]
            if (oid not in owners or event["owner_id"] != oid
                    or self.number(event["owner_ordinal"]) != n or record["owner_label"] != oid
                    or self.number(record["owner_ordinal"]) != n):
                raise api.Rejected("actual DAG borrows an owner or raw ordinal")
            if event["collection"] != "boundary_packets":
                continue
            fact = record["facts"]
            phase = fact["phase"]
            if phase in ("rpc-attempt-started", "direct-journal-rpc-attempt-started"):
                target = fact.get("target_node_id")
                command = api.actual_journal_rpc_command(fact, owners[oid]["node_id"], target)
                if command is not None:
                    starts.append((key, fact, command))
            elif phase in ("rpc-attempt-returned", "direct-journal-rpc-attempt-returned"):
                returns.setdefault((oid, fact.get("attempt_id")), []).append((key, fact))
            elif phase in ("replica-journal-persisted", "replica-prefix-probe-accepted"):
                receivers.append((key, fact))
            elif phase == "source-journal-response-accepted":
                accepted.add(key)
        for oid, numbers in self.local.items():
            for before, after in zip(numbers, numbers[1:]):
                self.parents[oid, after].add((oid, before))
        self.request_sources, self.receiver_returns = {}, {}
        self.unresolved_returns = set()
        for key, fact in receivers:
            command = fact["command"]
            if (command["file_system_id"] != scope["file_system_id"]
                    or command["batch"]["shard_id"] != scope["shard_id"]):
                raise api.Rejected("actual receiver DAG command crosses its scope")
            source = self.by_node.get(command["source_node_id"])
            if source is None:
                raise api.Unavailable("actual receiver has no captured source owner")
            matches = [(start, call) for start, call, value in starts
                       if start[0] == source and value == command
                       and api.journal_rpc_request_matches(
                           call, command, owners[source]["node_id"], owners[key[0]]["node_id"])]
            if len(matches) != 1:
                raise api.Unavailable("actual receiver lacks its unique target-qualified source start")
            start, call = matches[0]
            self.parents[key].add(start)
            self.request_sources[key] = (start, call)
        consumed = set()
        for edge in correlations:
            source, receiver, ack, received = verify_edge(edge)
            if source in consumed:
                raise api.Rejected("actual DAG repeats an accepted source return")
            consumed.add(source)
            if source not in accepted or receiver not in self.request_sources:
                raise api.Rejected("actual DAG edge lacks its captured acceptance or receiver")
            start, call = self.request_sources[receiver]
            terminal = returns.get((start[0], call["attempt_id"]), [])
            if len(terminal) != 1:
                raise api.Unavailable("actual accepted ACK lacks its SAME unique transport return")
            returned, fact = terminal[0]
            if not start[1] < returned[1] <= source[1]:
                raise api.Rejected("actual transport start/return/ACK order changed")
            api.verify_journal_rpc_return(call, fact, ack["response"],
                                         owners[source[0]]["node_id"], owners[receiver[0]]["node_id"])
            if received["phase"] == "replica-prefix-probe-accepted":
                constructed = receiver
            else:
                matches = [key for key, event in events.items()
                           if key[0] == receiver[0] and key[1] > receiver[1]
                           and event["collection"] == "boundary_packets"
                           and event["record"]["facts"].get("phase") == "replica-journal-response-constructed"
                           and event["record"]["facts"].get("response") == ack["response"]]
                if len(matches) != 1:
                    raise api.Unavailable("actual accepted ACK lacks its unique constructed receiver return")
                constructed = matches[0]
            if constructed != receiver:
                self.parents[constructed].add(receiver)
            self.parents[returned].add(constructed)
            self.parents[source].add(returned)
            self.receiver_returns[receiver] = (constructed, returned, source)
        if consumed != accepted:
            raise api.Unavailable("actual DAG omits an accepted source/receiver return correlation")
        # A real decoded return can be consumed without minting an accepted ACK.
        # Preserve that exact transport relationship; uncertain/missing returns
        # cannot certify a frontier beyond the receiver.
        for receiver, (start, call) in self.request_sources.items():
            if receiver in self.receiver_returns:
                continue
            terminal = returns.get((start[0], call["attempt_id"]), [])
            if len(terminal) != 1:
                self.unresolved_returns.add(receiver)
                continue
            returned, fact = terminal[0]
            received = events[receiver]["record"]["facts"]
            expected = received.get("response")
            constructed = receiver
            if received["phase"] == "replica-journal-persisted":
                candidates = [(key, event["record"]["facts"]["response"])
                    for key, event in events.items() if key[0] == receiver[0] and key[1] > receiver[1]
                    and event["collection"] == "boundary_packets"
                    and event["record"]["facts"].get("phase") == "replica-journal-response-constructed"
                    and event["record"]["facts"].get("response", {}).get("request_id") == received["command"]["request_id"]]
                if len(candidates) != 1:
                    self.unresolved_returns.add(receiver)
                    continue
                constructed, expected = candidates[0]
            if fact.get("error_detail") is not None or fact.get("delivery_state") is not None or expected is None:
                self.unresolved_returns.add(receiver)
                continue
            if not start[1] < returned[1]:
                raise api.Rejected("actual unaccepted transport return precedes its source start")
            api.verify_journal_rpc_return(call, fact, expected, owners[start[0]]["node_id"], owners[receiver[0]]["node_id"])
            if constructed != receiver:
                self.parents[constructed].add(receiver)
            self.parents[returned].add(constructed)
            self.receiver_returns[receiver] = (constructed, returned, None)
        # A normal replica marker read is part of the same received call.
        # Its unique linked ingest already owns the actual source-start edge;
        # the read precedes that ingest and cannot precede the source call.
        read_raw = {key: dict(kind='boundary' if event['collection']=='boundary_packets'
                             else 'kernel' if event['collection']=='packets'
                             else 'guard' if event['collection']=='guard_events'
                             else event['collection'], ordinal=key[1], record=event['record'])
                    for key,event in events.items()}
        for key,event in events.items():
            if (event['collection']!='boundary_packets'
                    or event['record']['facts']['phase']!='replica-write-freeze-read'):
                continue
            qualified = api.verify_replica_freeze_read(read_raw, owners, scope, key)
            source = qualified['ingest_source']
            ingest = source['owner_id'], self.number(source['owner_ordinal'])
            if ingest not in self.request_sources:
                raise api.Unavailable('normal replica freeze read has no actual source-call start')
            start,_ = self.request_sources[ingest]
            self.parents[key].add(start)
        self.require_acyclic()

    def require_acyclic(self):
        degrees = {key: len(value) for key, value in self.parents.items()}
        children = {key: [] for key in self.parents}
        for key, parents in self.parents.items():
            for parent in parents:
                if parent not in children:
                    raise self.api.Rejected("actual DAG references an uncaptured predecessor")
                children[parent].append(key)
        ready = [key for key, degree in degrees.items() if not degree]
        heapq.heapify(ready)
        seen = 0
        while ready:
            key = heapq.heappop(ready)
            seen += 1
            for child in children[key]:
                degrees[child] -= 1
                if not degrees[child]:
                    heapq.heappush(ready, child)
        if seen != len(degrees):
            raise self.api.Rejected("actual owner-local/transport DAG contains a cycle")

    def horizon(self, blueprint):
        source = blueprint["native_source"]
        asof = blueprint.get("as_of_source", source)
        oid, n = asof["owner_id"], self.number(asof["owner_ordinal"])
        if oid != source["owner_id"] or (oid, n) not in self.events:
            raise self.api.Rejected("global cut has no actual same-owner as-of source")
        side = asof.get("side", "before" if source["side"] == "before"
                        and self.number(source["owner_ordinal"]) == n else "after")
        if side not in ("before", "after"):
            raise self.api.Rejected("global cut has an unknown as-of side")
        if n < self.number(source["owner_ordinal"]) or (n == self.number(source["owner_ordinal"])
                and side == "before" and source["side"] != "before"):
            raise self.api.Rejected("global cut borrows a future held source or its after-state")
        return oid, n if side == "after" else max((p for p in self.local[oid] if p < n), default=0)

    def close(self, vector):
        answer = dict(vector)
        changed = True
        while changed:
            changed = False
            for (oid, n), parents in self.parents.items():
                if n > answer[oid]:
                    continue
                for actor, ordinal in parents:
                    if answer[actor] < ordinal:
                        answer[actor] = ordinal
                        changed = True
        return answer

    def require_closed(self, vector):
        if set(vector) != set(self.owners) or any(type(n) is not int or n < 0 for n in vector.values()):
            raise self.api.Rejected("global frontier does not cover the exact actual owners")
        if any(n <= vector[owner] for owner, n in self.unresolved_returns):
            raise self.api.Unavailable("global frontier crosses an unresolved actual receiver/return relationship")
        if self.close(vector) != vector:
            raise self.api.Rejected("global frontier omits an actual causal predecessor")

    def rebase(self, blueprint, ordinal, side="after"):
        result = copy.deepcopy(blueprint)
        source = result["native_source"]
        oid, source_n = source["owner_id"], self.number(source["owner_ordinal"])
        if (oid, ordinal) not in self.events or ordinal < source_n:
            raise self.api.Rejected("global frontier borrows a future held source")
        old = self.horizon(blueprint)[1]
        horizon = ordinal if side == "after" else max((n for n in self.local[oid] if n < ordinal), default=0)
        first = source_n if source["side"] == "before" else source_n + 1
        for (actor, n), event in self.events.items():
            if actor != oid or not first <= n <= horizon:
                continue
            record = event["record"]
            if (n > old and event['collection']=='original_control_commits'
                    and (actor,n) not in getattr(self,'original_effect_boundaries',{})):
                raise self.api.Unavailable('global as-of reuse crosses an unmatched actual original KV effect')
            if n == source_n and source["side"] == "before" and (
                    event["collection"] == "packets" or
                    event["collection"] == "boundary_packets" and record["facts"]["phase"] in
                    ("replica-journal-persisted", "replica-prefix-probe-accepted", "replica-snapshot-accepted")):
                raise self.api.Unavailable("held-before source cannot cross its actual durable/floor transition")
            value = record.get("after_held_json")
            if value is not None:
                held = self.api.native_json(self.api.byte_vector(value, "intervening global held"), "intervening global held")
                if not self.api.native_metadata_memory_equal(held, result["held"]):
                    raise self.api.Unavailable("global as-of reuse crossed an actual metadata mutation")
            if n > old and event["collection"] == "guard_events":
                raise self.api.Unavailable("global as-of reuse crossed an unmodeled actual guard transition")
        result["as_of_source"] = dict(owner_id=oid, owner_ordinal=str(ordinal), side=side)
        return result


def global_vectors(dag, blueprints, initial, timeline):
    """Independently derive source-bound vectors; no supplied graph is trusted."""
    api = dag.api
    for blueprint in blueprints.values():
        source, held = blueprint["native_source"], blueprint["held"]
        if (source["owner_id"] not in dag.owners
                or held["backend_root"] != dag.owners[source["owner_id"]]["canonical_root"]
                or held["shard_id"] != dag.scope["shard_id"]):
            raise api.Rejected("global held source crosses its actual root or shard")
        event = dag.events.get((source['owner_id'],dag.number(source['owner_ordinal'])))
        if event is not None and event['collection']=='boundary_packets' and event['record']['facts']['phase']=='replica-materialization-completed':
            interval=blueprint.get('materialization_interval')
            if not isinstance(interval,dict) or set(interval)!={'persisted','completed'}:
                raise api.Unavailable('materialized cut lacks its exact persisted/completed interval')
            keys=[]
            for label in ('persisted','completed'):
                ref=interval[label]
                if not isinstance(ref,dict) or set(ref)!={'owner_id','owner_ordinal'}:
                    raise api.Rejected('materialization interval changes its exact raw references')
                keys.append((ref['owner_id'],dag.number(ref['owner_ordinal'])))
            if keys[1]!=(source['owner_id'],dag.number(source['owner_ordinal'])):
                raise api.Rejected('materialized cut borrows another actual completion')
            qualified_materialization(api,dag.events,dag.owners,dag.scope,*keys)
    current = {oid: copy.deepcopy(blueprints[name]) for oid, name in initial.items()}
    dag.require_closed({oid: dag.horizon(cut)[1] for oid, cut in current.items()})
    vectors = {}
    initial_horizon={oid:dag.horizon(cut)[1] for oid,cut in current.items()}
    dag.original_effect_boundaries={}
    for row in timeline:
        terminal=(row['owner_id'],dag.number(row['event_id'].rsplit(':',1)[1]))
        references=[(ref['actual_mount_label'],dag.number(ref['owner_ordinal'])) for ref in row['raw_references']]
        originals=[key for key in references if dag.events.get(key,{}).get('collection')=='original_control_commits']
        if originals:
            if row.get('action',{}).get('kind')!='fallback-permit':
                raise api.Rejected('original KV source is outside its actual fallback-permit group')
            calls=[key for key in references if dag.events.get(key,{}).get('collection')=='control_commits']
            if len(originals)!=1 or len(calls)!=1 or originals[0][0]!=terminal[0] or calls[0][0]!=terminal[0]:
                raise api.Rejected('fallback group changes its exact original/caller source set')
            if dag.headers is None:
                raise api.Unavailable('original effect boundary lacks its actual source headers')
            raw={key:dict(kind={'original_control_commits':'original-control-commit','control_commits':'control-commit'}.get(event['collection'],event['collection']),
                          ordinal=key[1],record=event['record']) for key,event in dag.events.items()}
            module,_=api.initial_native_verifiers(dag.scope,list(dag.owners.values()),raw)
            owner=terminal[0]; header=dag.headers[owner]
            voters={member['node_id'] for member in header['normal_mounted_identity']['configured_members']
                if member['role']=='voter' and member['state']=='voter'}
            matched=module.original_for_replay(dag.scope,dag.owners[owner],header,raw,
                calls[0],dag.events[calls[0]]['record'],voters)
            if matched!=originals[0]:
                raise api.Rejected('fallback group changes its independently matched original boundary')
            if matched in dag.original_effect_boundaries:
                raise api.Rejected('actual original KV effect represented by multiple transitions')
            dag.original_effect_boundaries[matched]=terminal
    transitions = {(row["owner_id"], dag.number(row["event_id"].rsplit(":", 1)[1])) for row in timeline}
    completed = set()
    for row in timeline:
        oid, end = row["owner_id"], dag.number(row["event_id"].rsplit(":", 1)[1])
        if (oid, end) not in dag.events:
            raise api.Rejected("global transition lacks its actual raw event")
        effect = end
        for ref in row["raw_references"]:
            key = ref["actual_mount_label"], dag.number(ref["owner_ordinal"])
            event = dag.events.get(key)
            if event is None:
                raise api.Rejected("global transition borrows an uncaptured raw source")
            if key[0] == oid and (event["collection"] in ("control_commits", "original_control_commits")
                    or event["collection"] == "packets"
                    and event["record"].get("phase") in ("prepare-durable", "commit-durable")):
                effect = min(effect, key[1])
        for side, field in (("before", "before_snapshot"), ("after", "after_snapshot")):
            actor = copy.deepcopy(blueprints[row[field]])
            if side == "before":
                source = actor["native_source"]
                if source["side"] == "before" and dag.number(source["owner_ordinal"]) == effect:
                    actor = dag.rebase(actor, effect, "before")
                else:
                    previous = max((n for n in dag.local[oid] if n < effect), default=0)
                    actor = dag.rebase(actor, previous)
            else:
                actor = dag.rebase(actor, end)
            wanted = dict(current)
            wanted[oid] = actor
            horizon = dag.close({owner: dag.horizon(cut)[1] for owner, cut in wanted.items()})
            for owner, n in horizon.items():
                if n > dag.horizon(wanted[owner])[1]:
                    wanted[owner] = dag.rebase(wanted[owner], n)
            if any(n <= horizon[actor] and key not in completed
                   for key in transitions for actor, n in [key]
                   if key != (oid, end) or side == "before"):
                raise api.Unavailable("global frontier crosses an unprocessed native transition")
            for key,event in dag.events.items():
                if (event['collection']!='original_control_commits'
                        or not initial_horizon[key[0]]<key[1]<=horizon[key[0]]): continue
                terminal=dag.original_effect_boundaries.get(key)
                if terminal is None or (terminal not in completed and (terminal!=(oid,end) or side=='before')):
                    raise api.Unavailable('global frontier crosses an unrepresented/unprocessed actual original KV effect')
            dag.require_closed(horizon)
            if side == "before" and any(not api.native_metadata_memory_equal(current[owner]["held"], wanted[owner]["held"]) or any(current[owner].get(k) != wanted[owner].get(k) for k in ("active_guard_blob", "last_guard_blob")) for owner in current):
                raise api.Unavailable("global before-cut crosses an unmodeled native state change")
            for owner in current:
                if owner != oid and (not api.native_metadata_memory_equal(current[owner]["held"], wanted[owner]["held"])
                        or any(current[owner].get(k) != wanted[owner].get(k)
                               for k in ("active_guard_blob", "last_guard_blob"))):
                    raise api.Rejected("global transition changes a non-actor native cut")
            vectors[row["event_id"], side] = wanted
        current = vectors[row["event_id"], "after"]
        completed.add((oid, end))
    return vectors


def globalize(dag, blueprints, initial, timeline, blobs=None):
    """Name each composite observer cut separately; native memory stays copied."""
    vectors = global_vectors(dag, blueprints, initial, timeline)
    output, renamed = {}, copy.deepcopy(timeline)
    initial_names = {oid: initial[oid] + ":observer-initial" for oid in initial}
    initial_selection = {dag.owners[oid]["node_id"]: name for oid, name in initial_names.items()}
    environment = dict(origin="existential-environment-constrained-by-native-responses",
                       online_node_ids=list(dag.by_node), authenticated_response_node_ids=[],
                       absence_is_offline=False, actual_receipt_observation_blobs=[])
    for oid, name in initial_names.items():
        output[name] = copy.deepcopy(blueprints[initial[oid]])
        output[name]["peer_cut_refs"] = dict(initial_selection)
        output[name]["environment_binding"] = copy.deepcopy(environment)
    for row in renamed:
        for side, field in (("before", "before_snapshot"), ("after", "after_snapshot")):
            vector = vectors[row["event_id"], side]
            names = {oid: cut["native_source"]["owner_id"] + ":observer:" + row["event_id"] + ":" + side
                     for oid, cut in vector.items()}
            selection = {dag.owners[oid]["node_id"]: name for oid, name in names.items()}
            for oid, name in names.items():
                output[name] = copy.deepcopy(vector[oid])
                output[name]["peer_cut_refs"] = dict(selection)
                output[name]["environment_binding"] = copy.deepcopy(environment)
            row[field] = names[row["owner_id"]]
        reference=row.get('action',{}).get('native_ingest_observation_blob')
        if reference:
            if blobs is None:
                raise dag.api.Unavailable('actual ingest reassociation lacks its original blob registry')
            original=blobs.values.get(reference)
            if not isinstance(original,dict) or original.get('native_type')!='DfsNativeReplicaDurableIngestObservationV1' or original.get('encoding')!='json-utf8':
                raise dag.api.Rejected('ingest reassociation changes its original fact type')
            data=original['data'].encode()
            import hashlib
            if hashlib.sha256(data).hexdigest()!=original.get('sha256'):
                raise dag.api.Rejected('ingest reassociation changes its original fact bytes')
            witness=dag.api.native_json(data,'original actual ingest fact')
            original_row=next(value for value in timeline if value['event_id']==row['event_id'])
            if witness.get('after_held_snapshot_id')!=original_row['after_snapshot'] or witness.get('owner_id')!=row['owner_id']:
                raise dag.api.Rejected('ingest fact does not name its exact original held result')
            original_cut=blueprints[original_row['after_snapshot']]
            rebound=output[row['after_snapshot']]
            if any(original_cut[key]!=rebound[key] for key in ('native_source','held')):
                raise dag.api.Rejected('ingest reassociation changes actual source or held tables')
            replacement=dict(witness,after_held_snapshot_id=row['after_snapshot'])
            row['action']['native_ingest_observation_blob']=blobs.json('DfsNativeReplicaDurableIngestObservationV1',replacement)
    return output, initial_names, renamed


def validate_globalized(dag, header):
    """Recompute from actual DATA, never accept a supplied causal graph or state."""
    basis = header.get("global_observer_basis")
    if not isinstance(basis, dict) or set(basis) != {"snapshot_blueprints", "initial", "native_timeline", "ingest_observation_blobs"}:
        raise dag.api.Unavailable("complete actual global observer basis is missing")
    registry=dag.api.Blobs(copy.deepcopy(basis['ingest_observation_blobs']))
    expected, initial, timeline = globalize(dag, basis["snapshot_blueprints"],
                                            basis["initial"], basis["native_timeline"], registry)
    for row in timeline:
        reference=row.get('action',{}).get('native_ingest_observation_blob')
        if reference and header.get('blobs',{}).get(reference)!=registry.values[reference]:
            raise dag.api.Rejected('reassociated ingest fact differs from independently derived actual bytes')
    if header["snapshot_blueprints"] != expected or header["native_timeline"] != timeline:
        raise dag.api.Rejected("global observer vectors differ from independently derived raw cuts")
    actual_initial = {oid: cut["held_snapshot_id"] for oid, cut in header["owner_cuts"].items()}
    if actual_initial != initial:
        raise dag.api.Rejected("global observer initial cut changes its certified actual owner sources")
    return expected


def qualified_materialization(api, events, owners, scope, persisted_key, completed_key):
    def held(event, side):
        value = event['record'].get(side + '_held_json')
        if value is None:
            raise api.Unavailable('interval lacks an actual held ' + side + ' cut')
        return api.native_json(api.byte_vector(value, 'actual interval held'), 'actual interval held')
    def number(value):
        if type(value) is int and 0 <= value < 2**64:
            return value
        return api.CHECK.decimal_u64(value, 'actual interval ordinal')
    r, m = events.get(persisted_key), events.get(completed_key)
    if r is None or m is None:
        raise api.Unavailable('interval lacks its actual persisted/completed event')
    owner = persisted_key[0]
    if (owner not in owners or completed_key[0] != owner
            or not persisted_key[1] < completed_key[1]
            or any(e['owner_id'] != owner or e['record']['owner_label'] != owner
                or number(e['owner_ordinal']) != key[1]
                or number(e['record']['owner_ordinal']) != key[1]
                for e, key in ((r, persisted_key), (m, completed_key)))):
        raise api.Rejected('materialization changes its exact owner or interval ordinal')
    if r['collection'] != 'boundary_packets' or m['collection'] != 'boundary_packets':
        raise api.Rejected('materialization sources are not actual receiver boundaries')
    rf, mf = r['record']['facts'], m['record']['facts']
    if (rf['phase'] != 'replica-journal-persisted' or rf.get('materialization_pending') is not True
            or mf['phase'] != 'replica-materialization-completed'):
        raise api.Rejected('interval changes its actual persisted/completed phase')
    command, permit = rf['command'], rf['permit']
    cfg = rf['actual_source_config']
    if (rf['local_node_id'] != owners[owner]['node_id']
            or cfg['file_system_id'] != scope['file_system_id'] or cfg['shard_id'] != scope['shard_id']
            or mf.get('file_system_id') != scope['file_system_id'] or mf.get('shard_id') != scope['shard_id']
            or command['file_system_id'] != scope['file_system_id']
            or command['batch']['shard_id'] != scope['shard_id']
            or permit['file_system_id'] != scope['file_system_id'] or permit['shard_id'] != scope['shard_id']
            or command['source_node_id'] != cfg['primary_node_id']
            or permit['source_node_id'] != cfg['primary_node_id']
            or number(permit['source_epoch']) != number(cfg['epoch'])):
        raise api.Rejected('materialization changes its exact source scope or permit')
    rb, ra, mb, ma = held(r, 'before'), held(r, 'after'), held(m, 'before'), held(m, 'after')
    if any(x['backend_root'] != owners[owner]['canonical_root'] or x['shard_id'] != scope['shard_id']
            for x in (rb, ra, mb, ma)):
        raise api.Rejected('materialization borrows another actual root or shard')
    if not api.native_metadata_memory_equal(ra, mb):
        raise api.Unavailable('materialization before cut differs from actual persisted after cut')
    durable = rf.get('durable_prefix')
    if durable is None or durable != ma['certified_prefix']:
        raise api.Rejected('materialization changes its exact full durable prefix')
    end, start = number(ma['materialized_through_sequence']), number(mb['materialized_through_sequence'])
    if (end != number(durable['journal_sequence']) or end <= start
            or number(mf['actual_materialized_entries']) != end - start):
        raise api.Rejected('materialization actual sequence/count is inconsistent')
    fixed = dict(ma, tables=ra['tables'], certified_prefix=ra['certified_prefix'],
        materialized_through_sequence=ra['materialized_through_sequence'])
    if not api.native_metadata_memory_equal(ra, fixed):
        raise api.Rejected('materialization changed unrelated native authority or fence state')
    for (actor, n), event in events.items():
        if actor != owner or not persisted_key[1] < n < completed_key[1]:
            continue
        if event['collection'] == 'guard_events':
            raise api.Unavailable('materialization interval crosses an actual guard transition')
        if event['collection'] == 'boundary_packets':
            fact = event['record']['facts']
            if fact['phase'] == 'replica-journal-response-constructed':
                reply = fact['response']
                if (reply['request_id'] != command['request_id'] or reply['shard_id'] != scope['shard_id']
                        or reply['result'] != 'applied'
                        or number(reply['next_after_sequence']) != number(durable['journal_sequence'])):
                    raise api.Rejected('materialization interval crosses another constructed reply')
            elif fact['phase'] in ('replica-journal-persisted', 'replica-prefix-probe-accepted',
                    'replica-snapshot-accepted', 'replica-materialization-completed',
                    'recovery-permit-issued', 'retiring-freeze-persisted', 'handoff-activation-completed'):
                raise api.Unavailable('materialization interval has another actual receiver/authority effect')
        value = event['record'].get('after_held_json')
        if value is not None and not api.native_metadata_memory_equal(ra,
                api.native_json(api.byte_vector(value, 'intervening held'), 'intervening held')):
            raise api.Unavailable('materialization interval crosses unaccounted native memory')
    rc, mc = r['record'].get('resident_custody'), m['record'].get('resident_custody')
    if not isinstance(rc, dict) or not isinstance(mc, dict):
        raise api.Unavailable('materialization lacks its OWN qualified resident custody')
    for event, custody, ordinal in ((r, rc, persisted_key[1]), (m, mc, completed_key[1])):
        if (custody['actual_mount_label'] != owner or custody['local_node_id'] != owners[owner]['node_id']
                or custody['backend_root'] != owners[owner]['canonical_root']
                or custody['file_system_id'] != scope['file_system_id'] or custody['shard_id'] != scope['shard_id']
                or number(custody['observer_owner_ordinal']) != ordinal
                or custody['admission_kind'] not in ('exact-shard', 'global-exclusive')):
            raise api.Rejected('materialization custody changes actual owner/root/scope')
        admitted = custody['admitted_scope']
        if (admitted['kind'] != custody['admission_kind']
                or admitted['kind'] == 'exact-shard' and any(admitted[k] != scope[k]
                    for k in ('file_system_id', 'shard_id'))):
            raise api.Rejected('materialization custody has another actual admission scope')
    for field in ('resident_handle_token', 'data_owner_token', 'admission_scheduler_token'):
        if not rc.get(field) or mc.get(field) != rc[field]:
            raise api.Rejected('materialization substitutes another actual resident/data/admission owner')
    # Production integration must STILL invoke existing independent owned-cut,
    # frame/prefix/semantic-decoder and original transport/ACK validators.
    # This proposal fragment alone grants no authorization or native acceptance.
    return persisted_key, completed_key
