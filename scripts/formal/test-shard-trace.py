#!/usr/bin/env python3
"""Checker sensitivity tests; synthetic states never establish native conformance."""
from __future__ import annotations
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('shard_trace_checker', Path(__file__).with_name('check-shard-trace.py'))
assert SPEC and SPEC.loader
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)
PROJECTOR_SPEC = importlib.util.spec_from_file_location('native_projector', Path(__file__).with_name('shard-native-projector.py'))
assert PROJECTOR_SPEC and PROJECTOR_SPEC.loader
projector = importlib.util.module_from_spec(PROJECTOR_SPEC)
PROJECTOR_SPEC.loader.exec_module(projector)


def finite(value):
    if isinstance(value, list):
        return [finite(x) for x in value]
    if isinstance(value, dict):
        if set(value) == {'#bigint'}:
            return int(value['#bigint'])
        if set(value) == {'#set'}:
            return {'$set': [finite(x) for x in value['#set']]}
        if set(value) == {'#map'}:
            return {'$map': [[finite(k), finite(v)] for k, v in value['#map']]}
        return {k: finite(v) for k, v in value.items() if k != '#meta'}
    return value


class EvidenceTests(unittest.TestCase):
    def test_exact_u64(self):
        self.assertEqual(checker.decimal_u64(str((1 << 64) - 1)), (1 << 64) - 1)
        for value in ['00', '-1', str(1 << 64), 1.0, 9007199254740993]:
            with self.subTest(value=value), self.assertRaises(checker.Rejected):
                checker.decimal_u64(value)

    def test_duplicate_json_rejected(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / 'duplicate.json'
            path.write_text('{"kind": "kv-observation", "kind": "contiguous"}')
            with self.assertRaises(checker.Rejected):
                checker.read_json(path)

    def test_kv_discriminator_is_separate(self):
        schema = checker.read_json(checker.SCHEMA_PATH)
        event = {'kind': 'kv-observation', 'observation_kind': 'contiguous',
                 'previous_revision': '1', 'revision': '2', 'applied_index': '2', 'payload_blob': 'native'}
        checker.validate_schema(event, schema['$defs']['NativeActionV1'], schema)
        event.pop('observation_kind')
        with self.assertRaises(checker.Rejected):
            checker.validate_schema(event, schema['$defs']['NativeActionV1'], schema)

    def test_itf_not_native(self):
        with self.assertRaises(checker.Rejected):
            checker.preflight_native({'#meta': {}, 'states': []}, schema={},
                                     source_revision='x', model_sha256='x', build_id='x')

    def test_current_config_has_no_retired_codec(self):
        schema = checker.read_json(checker.SCHEMA_PATH)
        current = {'file_system_id': 'fs', 'shard_id': 'root', 'primary_node_id': 'node',
                   'replica_node_ids': [], 'epoch': '1', 'assignment_blob': 'assignment',
                   'default_policy_blob': 'policy', 'native_config_blob': 'config',
                   'canonical_fingerprint': 'current-whole-config'}
        checker.validate_schema(current, schema['$defs']['ConfigTuple'], schema)
        for retired in ('legacy', 'packed-v1', 'packed-v6'):
            invalid = {**current, 'namespace_prepare_codec': retired}
            with self.subTest(retired=retired), self.assertRaises(checker.Rejected):
                checker.validate_schema(invalid, schema['$defs']['ConfigTuple'], schema)

    def test_trace_requires_explicit_current_metadata_protocol(self):
        schema = checker.read_json(checker.SCHEMA_PATH)
        protocol = schema['$defs']['TraceEnvelopeV1']['properties']['metadata_protocol_version']
        self.assertIn('metadata_protocol_version', schema['$defs']['TraceEnvelopeV1']['required'])
        checker.validate_schema(7, protocol, schema)
        for invalid in (6, '7', None, True):
            with self.subTest(invalid=invalid), self.assertRaises(checker.Rejected):
                checker.validate_schema(invalid, protocol, schema)


class NativeStorageFragmentTests(unittest.TestCase):
    def record(self):
        identity = {"protocol_version": 7, "file_system_id": "fs", "shard_id": "root",
                    "owner_epoch": 7, "operation_id": "operation"}
        return {"digest": {**identity, "payload_digest": "native-canonical-digest", "prepare_sequence": 18},
                "payload": identity, "status": "prepared"}

    def test_actual_visible_rows_unchanged_on_hidden_prepare(self):
        before = {"objects": [{"object_id": "object", "current_version": "before"}]}
        after = copy.deepcopy(before)
        after['prepared_operations_v7'] = [self.record()]
        checker.validate_prepare_visibility(before, after)
        projected = checker.project_held_native_records(after, {"file_system_id": "fs", "shard_id": "root"})
        self.assertFalse(projected['operation']['committed'])

    def current_index_rows(self):
        object = dict(file_system_id='fs', object_id='object', path='/parent/leaf',
            kind='file', owner_node_id='node', version='version', policy='replicated-eventual',
            content_hash='content', current_locations=['node'])
        parent = 'path:fs:'+projector.blake3(b'parent')
        entry = dict(file_system_id='fs', object_id='object', path='parent/leaf', kind='file',
            inode_id='obj:object', parent_inode_id=parent, name='leaf', dirfrag_id='root',
            name_hash=int.from_bytes(bytes.fromhex(projector.blake3(b'leaf'))[:8], 'big'))
        inode = dict(inode_id='obj:object', file_system_id='fs', object_id='object',
            kind='file', owner_node_id='node', policy='replicated-eventual', visible_version='version',
            content_hash='content', current_locations=['node'])
        tables = dict(paths=[entry], file_system_objects=[copy.deepcopy(entry)],
            inodes=[inode], dentries=[copy.deepcopy(entry)],
            dirfrags=[dict(file_system_id='fs', dir_inode_id=parent, dirfrag_id='root', entry_count=1)])
        return object, tables

    def test_complete_native_derived_indexes_and_missing_rows(self):
        instance = object.__new__(projector.NativeProjectorV1)
        declared, tables = self.current_index_rows()
        instance.validate_object_indexes(tables, declared)
        for table in ('paths', 'file_system_objects', 'inodes', 'dentries', 'dirfrags'):
            altered = copy.deepcopy(tables); altered[table] = []
            with self.subTest(table=table), self.assertRaises(projector.Rejected):
                instance.validate_object_indexes(altered, declared)

    def test_indexes_bind_exact_object_generation_content_and_fragment_count(self):
        instance = object.__new__(projector.NativeProjectorV1)
        declared, tables = self.current_index_rows()
        for table, field, wrong in (('paths', 'object_id', 'other'), ('inodes', 'visible_version', 'later'),
                ('inodes', 'content_hash', 'other-bytes'), ('dirfrags', 'entry_count', 2)):
            altered = copy.deepcopy(tables); altered[table][0][field] = wrong
            with self.subTest(field=field), self.assertRaises(projector.Rejected):
                instance.validate_object_indexes(altered, declared)

    def test_native_metadata_effect_cannot_hide_behind_complete_footprint_label(self):
        instance = object.__new__(projector.NativeProjectorV1)
        payload = dict(metadata_effects=[dict(type='put-version', version=dict(object_id='object', version='version'))],
            transaction=dict(file_version_pins=None, advisory_lock_states=[]))
        with self.assertRaises(projector.Rejected):
            instance.validate_declared_table_effects(dict(versions=[]), payload)
        instance.validate_declared_table_effects(dict(versions=[payload['metadata_effects'][0]['version']]), payload)
        payload['metadata_effects'] = [dict(type='merge-intent-receipt-retention', retention={})]
        with self.assertRaises(projector.Unavailable):
            instance.validate_declared_table_effects({}, payload)

    def test_partial_visible_rows_rejected(self):
        before = {"objects": [{"object_id": "object", "current_version": "before"}]}
        after = {"objects": [{"object_id": "object", "current_version": "after"}]}
        with self.assertRaises(checker.Rejected):
            checker.validate_prepare_visibility(before, after)

    def test_changed_native_payload_scope_rejected(self):
        record = self.record()
        record['payload']['owner_epoch'] = 8
        with self.assertRaises(checker.Rejected):
            checker.project_held_native_records({"prepared_operations_v7": [record]}, {"file_system_id": "fs", "shard_id": "root"})

    def test_commit_reference_binds_exact_digest(self):
        record = self.record()
        record['status'] = 'committed'
        record['committed_entry'] = {"shard_id": "root", "sequence": 19,
            "created_at": "2026-10-07T00:00:00Z", "digest": copy.deepcopy(record['digest'])}
        result = checker.project_held_native_records({"prepared_operations_v7": [record]}, {"file_system_id": "fs", "shard_id": "root"})
        self.assertTrue(result['operation']['committed'])
        record['committed_entry']['digest']['payload_digest'] = 'changed'
        with self.assertRaises(checker.Rejected):
            checker.project_held_native_records({"prepared_operations_v7": [record]}, {"file_system_id": "fs", "shard_id": "root"})


class InitialCutSensitivityTests(unittest.TestCase):
    """Synthetic byte/row sensitivity; these fixtures are not Rust captures."""
    def setUp(self):
        import hashlib
        self.scope = {'file_system_id': 'fs', 'shard_id': 'root'}
        self.adapter = object.__new__(projector.NativeProjectorV1)
        self.adapter.scope = self.scope
        self.adapter.owners = {'owner': {'owner_id': 'owner', 'node_id': 'node'}}
        self.adapter.trace = {'implementation_revision': 'revision'}
        self.adapter.operations = {'selected': 1}
        self.adapter.payloads = {}
        self.adapter.baseline_records = {}
        self.adapter.baseline_operations = set()
        self.adapter.initial_payloads = {}
        self.adapter.blobs = {}
        self.prior = {'version': 2, 'journal_sequence': 17,
                      'digest': 'dfs-metadata-prefix-v2:blake3:' + 'a' * 64}
        self.commit = {'shard_id': 'root', 'sequence': 18, 'created_at': '2026-10-07T00:00:00Z',
                       'digest': {**self.scope, 'operation_id': 'bootstrap'}}
        self.entry = {**{key: value for key, value in self.commit.items() if key != 'digest'},
                      'mutation': {'type': 'commit-prepared-operation-v7', 'digest': self.commit['digest'],
                                   'file_system_id': 'fs', 'operation_id': 'bootstrap'}}
        self.frame = b'captured native frame'
        self.anchor = projector.extend_native_prefix(self.prior, 18, self.frame)
        def blob(key, kind, value):
            raw = json.dumps(value, separators=(',', ':')).encode()
            self.adapter.blobs[key] = {'native_type': kind, 'encoding': 'json-utf8',
                                      'data': raw.decode(), 'sha256': hashlib.sha256(raw).hexdigest()}
        self.blob = blob
        self.adapter.blobs['frame'] = {'encoding': 'json-utf8', 'data': self.frame.decode(),
                                       'sha256': hashlib.sha256(self.frame).hexdigest()}
        blob('entry', 'DfsMetadataJournalEntry', self.entry)
        blob('decode', 'DfsNativeJournalDecodeObservationV1', {
            'scope': self.scope, 'implementation_revision': 'revision', 'complete': True,
            'continuous': True, 'owner_bindings': list(self.adapter.owners.values()),
            'exact_frame_sha256': hashlib.sha256(self.frame).hexdigest(), 'decoded_entry_blob': 'entry'})
        self.proof = {'owner_id': 'owner', 'native_anchor': self.anchor,
                      'commit_reference': self.commit, 'prior_prefix': self.prior,
                      'frames': [{'exact_frame_blob': 'frame', 'decoded_entry_blob': 'entry',
                                  'decoder_observation_blob': 'decode'}]}

    def test_exact_native_baseline_chain_reaches_anchor(self):
        self.adapter.validate_baseline_ancestry(self.proof, 'owner', self.anchor, self.commit)

    def test_missing_altered_and_foreign_baseline_chain_fail(self):
        import hashlib
        for field in ('missing', 'altered', 'foreign', 'gap'):
            proof = copy.deepcopy(self.proof)
            if field == 'missing':
                proof['frames'] = []
            elif field == 'altered':
                self.adapter.blobs['frame']['data'] = 'altered native frame'
                self.adapter.blobs['frame']['sha256'] = hashlib.sha256(b'altered native frame').hexdigest()
            elif field == 'foreign':
                proof['owner_id'] = 'other'
            else:
                proof['prior_prefix']['journal_sequence'] = 16
            with self.subTest(field=field), self.assertRaises((projector.Rejected, projector.Unavailable)):
                self.adapter.validate_baseline_ancestry(proof, 'owner', self.anchor, self.commit)
            self.adapter.blobs['frame']['data'] = self.frame.decode()
            self.adapter.blobs['frame']['sha256'] = hashlib.sha256(self.frame).hexdigest()

    def test_pending_rows_cannot_be_excluded_as_baseline(self):
        tables = {'intents': [{'status': 'receipts-pending', 'intent_id': 'unrelated'}]}
        cuts = {'active_dependencies': {'intents': tables['intents'], 'namespace_requests': [], 'namespace_jobs': []}}
        with self.assertRaises(projector.Unavailable):
            self.adapter.validate_initial_dependencies(tables, cuts, {})
        cuts['active_dependencies']['intents'] = []
        with self.assertRaises(projector.Rejected):
            self.adapter.validate_initial_dependencies(tables, cuts, {})

    def test_initial_receipt_requires_actual_body_and_held_row(self):
        receipt = {'replica_node_id': 'node', 'receipt': {'object_id': 'object'}}
        self.blob('receipt', 'DfsReplicaReceiptRecord', receipt)
        observed = {'selected': {'durable_content_receipt_node_ids': ['node'],
                                'durable_content_receipt_blobs': ['receipt']}}
        with self.assertRaises(projector.Unavailable):
            self.adapter.validate_initial_receipts({'receipts': [receipt]}, observed)
        self.adapter.initial_payloads['selected'] = {'content_receipts': [receipt]}
        self.adapter.validate_initial_receipts({'receipts': [receipt]}, observed)
        with self.assertRaises(projector.Unavailable):
            self.adapter.validate_initial_receipts({'receipts': []}, observed)


class NativeMemoryViewTests(unittest.TestCase):
    """Pure current-format argument views cannot alter storage or grant custody."""
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('native_memory_assembler', Path(__file__).with_name('assemble-shard-native-trace.py'))
        cls.assembler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.assembler)

    def views(self):
        before = dict(writer_bound_epoch=2, requested_writer_epoch_view=2,
            backend_instance_authority_epoch=2, backend_root='/actual/native/root',
            certified_prefix={'journal_sequence': 17, 'digest': 'exact-native-prefix'},
            tables={'objects': [{'object_id': 'object', 'version': 'version'}]})
        after = copy.deepcopy(before)
        after['writer_bound_epoch'] = None
        after['requested_writer_epoch_view'] = None
        return before, after

    def test_explicit_derived_argument_views_preserve_exact_storage_data(self):
        before, after = self.views()
        self.assertTrue(self.assembler.native_metadata_memory_equal(before, after))

    def test_instance_metadata_prefix_and_owner_changes_are_unequal(self):
        before, after = self.views()
        for field, value in [('backend_instance_authority_epoch', 3), ('tables', {'objects': []}),
                ('certified_prefix', {'journal_sequence': 17, 'digest': 'changed-prefix'}),
                ('backend_root', '/different/native/root')]:
            altered = copy.deepcopy(after)
            altered[field] = value
            with self.subTest(field=field):
                self.assertFalse(self.assembler.native_metadata_memory_equal(before, altered))

    def test_missing_invalid_or_malformed_argument_views_are_rejected(self):
        before, after = self.views()
        missing = copy.deepcopy(after)
        missing.pop('requested_writer_epoch_view')
        invalid = {**after, 'writer_bound_epoch': 2}
        malformed = {**after, 'requested_writer_epoch_view': True}
        for altered in (missing, invalid, malformed):
            with self.subTest(altered=altered), self.assertRaises(self.assembler.Rejected):
                self.assembler.native_metadata_memory_equal(before, altered)


class EnabledTransitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='sinomo-shard-checker-selftest-')
        path = Path(cls.scratch.name)
        driver = path / 'observed_selftest.qnt'
        imported = json.dumps(os.path.relpath(checker.MODEL_PATH.with_suffix(''), path))
        driver.write_text('module observed_selftest {\n'
            f'  import dfs_whole_shard.* from {imported}\n'
            '  var cursor: int\n'
            '  action initialize = all { init, cursor\' = 0 }\n'
            '  action advance = if (cursor == 0) all { stage(1, 1), cursor\' = 1 }\n'
            '    else all { stutter, cursor\' = cursor }\n}\n')
        output = path / 'selftest.itf.json'
        command = ['quint', 'run', str(driver), '--main=observed_selftest', '--init=initialize',
                   '--step=advance', '--backend=typescript', '--max-samples=1', '--max-steps=1',
                   '--seed=1', '--verbosity=0', '--out-itf=' + str(output)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=45)
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)
        cls.states = [finite(state['s']) for state in json.loads(output.read_text())['states']]
        cls.transition = [{'action': 'stage', 'arguments': [1, 1]}]

    @classmethod
    def tearDownClass(cls):
        cls.scratch.cleanup()

    def test_enabled_action_and_observed_poststate(self):
        result = checker.verify_quint_boundary(self.states, self.transition,
            model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)
        self.assertEqual(result['quint_enabled_transition_replay'], 'verified')
        self.assertIn('native projector', result['classification'])

    def test_changed_observed_poststate_rejected(self):
        states = copy.deepcopy(self.states)
        states[1]['visible'] = 1
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary(states, self.transition,
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_invariant_preserving_wrong_poststate_rejected(self):
        states = copy.deepcopy(self.states)
        states[1]['last'] = 'fabricated-post-state'
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary(states, self.transition,
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_disabled_action_cannot_pass_on_safe_prefix(self):
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary(self.states, [{'action': 'commit', 'arguments': [1, 11, False]}],
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_unknown_action_rejected(self):
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary(self.states, [{'action': 'inventAuthority', 'arguments': []}],
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_initialization_cannot_erase_pending_native_work(self):
        states = copy.deepcopy(self.states)
        states[0]['pending'] = True
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary(states, self.transition,
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)


def actual_causal_data_fixture(path, api, helper):
    A, H = api, helper
    INPUT = Path(path)
    DATA=json.loads(INPUT.read_text())
    EVENTS={(r["owner_id"],int(r["owner_ordinal"])):r for r in DATA["owner_local_events"]}
    OWNERS={r["owner_id"]:dict(r) for r in DATA["owners"]}
    for oid,owner in OWNERS.items():
        owner["canonical_root"]=DATA["owner_memory_cuts"][oid]["all_memory_cuts"][0]["value"]["backend_root"]
    def dag(events=EVENTS,links=None):
        _,verify=A.initial_native_verifiers(DATA["scope"],list(OWNERS.values()),
            {k:dict(kind="boundary",record=v["record"]) for k,v in events.items() if v["collection"]=="boundary_packets"})
        return H.NativeExecutionDag(A,events,OWNERS,DATA["scope"],
            DATA["actual_request_response_correlations"] if links is None else links,verify)
    DAG=dag()
    OP=DATA["selected_operation_ids"][0]
    def one(phase):
        return next(k for k,v in EVENTS.items() if v["collection"]=="packets"
            and v["record"]["phase"]==phase and v["record"]["operation"]["operation_id"]==OP)
    PD,PM=one("prepare-durable"),one("prepare-materialized")
    BLUEPRINTS={}
    def cut(key,side,asof=None):
        held=A.native_json(A.byte_vector(EVENTS[key]["record"][side+"_held_json"],"actual DATA cut"),"actual DATA cut")
        name=key[0]+":"+str(key[1])+":"+side+":"+str(asof or key[1])
        BLUEPRINTS[name]=dict(native_source=dict(owner_id=key[0],owner_ordinal=str(key[1]),side=side),
            as_of_source=dict(owner_id=key[0],owner_ordinal=str(asof or key[1]),side="before" if side=="before" and not asof else "after"),held=held)
        return name
    INITIAL={PD[0]:cut(PD,"before")}
    PREPARED=cut(PM,"after")
    def row(key,before,after,refs):
        return dict(event_id=key[0]+":actual-event:"+str(key[1]),owner_id=key[0],before_snapshot=before,after_snapshot=after,
            raw_references=[dict(actual_mount_label=k[0],owner_ordinal=str(k[1])) for k in refs])
    ROWS=[row(PM,INITIAL[PD[0]],PREPARED,[PD,PM])]
    for link in DATA["actual_request_response_correlations"]:
        key=link["receiver_owner_id"],int(link["receiver_ordinal"])
        cmd=EVENTS[key]["record"]["facts"]["command"]
        if ((cmd.get("authority_probe") or {}).get("operation") or {}).get("operation_id")!=OP:continue
        INITIAL[key[0]]=cut(key,"before")
        ROWS.append(row(key,INITIAL[key[0]],cut(key,"after"),[key]))
        ack=link["source_owner_id"],int(link["source_ordinal"])
        ref=cut(PM,"after",ack[1]);ROWS.append(row(ack,ref,ref,[ack]))
    # Only actual native vertices and their independently reconstructed raw edges
    # define this DATA control's partial linearization; no model action is executed.
    bykey={(r["owner_id"],int(r["event_id"].rsplit(":",1)[1])):r for r in ROWS}
    def ancestors(key):
        todo=list(DAG.parents[key]);seen=set()
        while todo:
            item=todo.pop()
            if item in seen:continue
            seen.add(item);todo.extend(DAG.parents[item])
        return seen
    deps={key:ancestors(key)&set(bykey) for key in bykey};done=set();ordered=[]
    while len(done)<len(bykey):
        ready=sorted(key for key in bykey if key not in done and deps[key]<=done)
        if not ready:raise A.Rejected("actual DATA partial order has a cycle")
        key=ready[0];ordered.append(bykey[key]);done.add(key)
    ROWS=ordered
    return dict(DATA=DATA, EVENTS=EVENTS, OWNERS=OWNERS, DAG=DAG,
                BLUEPRINTS=BLUEPRINTS, INITIAL=INITIAL, ROWS=ROWS, dag=dag)

class NativeCausalFrontierDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.environ.get("SINOMO_NATIVE_CAUSAL_INPUTS")
        if not path:
            raise unittest.SkipTest("actual saved native capture DATA is required")
        here = Path(__file__).resolve().parent
        cls.api = importlib.util.module_from_spec(spec := importlib.util.spec_from_file_location(
            "causal_test_api", here/"assemble-shard-native-trace.py"))
        spec.loader.exec_module(cls.api)
        cls.helper = importlib.util.module_from_spec(spec := importlib.util.spec_from_file_location(
            "causal_test_helper", here/"shard-native-causality.py"))
        spec.loader.exec_module(cls.helper)
        cls.fixture = actual_causal_data_fixture(path, cls.api, cls.helper)

    def test_actual_complete_target_qualified_dag_and_parallel_global_vectors(self):
        f = self.fixture
        self.assertEqual(len(f["DAG"].request_sources), len(f["DATA"]["actual_request_response_correlations"]))
        vectors = self.helper.global_vectors(f["DAG"], f["BLUEPRINTS"], f["INITIAL"], f["ROWS"])
        self.assertEqual(len(vectors), 2*len(f["ROWS"]))
        output, initial, rows = self.helper.globalize(f["DAG"], f["BLUEPRINTS"], f["INITIAL"], f["ROWS"])
        header = dict(snapshot_blueprints=output, native_timeline=rows,
            owner_cuts={oid:dict(held_snapshot_id=name) for oid,name in initial.items()},
            global_observer_basis=dict(snapshot_blueprints=f["BLUEPRINTS"], initial=f["INITIAL"], native_timeline=f["ROWS"], ingest_observation_blobs={}))
        self.helper.validate_globalized(f["DAG"], header)

    def test_actual_ingest_enrichment_is_bound_before_globalization(self):
        f = self.fixture
        blueprints = copy.deepcopy(f["BLUEPRINTS"])
        rows = copy.deepcopy(f["ROWS"])
        receiver = next(row for row in rows
            if f["EVENTS"][row["owner_id"], int(row["event_id"].rsplit(":", 1)[1])]["collection"] == "boundary_packets"
            and f["EVENTS"][row["owner_id"], int(row["event_id"].rsplit(":", 1)[1])]["record"]["facts"]["phase"] == "replica-journal-persisted")
        event = f["EVENTS"][receiver["owner_id"], int(receiver["event_id"].rsplit(":", 1)[1])]
        blobs=self.api.Blobs({})
        witness=dict(copy.deepcopy(event["record"]["facts"]),owner_id=receiver["owner_id"],
            after_held_snapshot_id=receiver["after_snapshot"])
        ref=blobs.json('DfsNativeReplicaDurableIngestObservationV1',witness)
        receiver["action"] = dict(native_ingest_observation_blob=ref)
        originals=copy.deepcopy(blobs.values)
        blueprints[receiver["before_snapshot"]]["durable_ingest_source"] = dict(
            owner_id=receiver["owner_id"], owner_ordinal=str(event["owner_ordinal"]))
        output, initial, timeline = self.helper.globalize(
            f["DAG"], blueprints, f["INITIAL"], rows,blobs)
        header = dict(snapshot_blueprints=output, native_timeline=timeline,blobs=blobs.values,
            owner_cuts={oid:dict(held_snapshot_id=name) for oid,name in initial.items()},
            global_observer_basis=dict(snapshot_blueprints=blueprints,
                initial=f["INITIAL"], native_timeline=rows,ingest_observation_blobs=originals))
        self.helper.validate_globalized(f["DAG"], header)
        rebound=timeline[rows.index(receiver)]["action"]["native_ingest_observation_blob"]
        result=json.loads(blobs.values[rebound]['data'])
        self.assertEqual(result,dict(witness,after_held_snapshot_id=timeline[rows.index(receiver)]['after_snapshot']))
        self.assertEqual(originals[ref],header['global_observer_basis']['ingest_observation_blobs'][ref])
        changed = copy.deepcopy(header)
        changed['blobs'][rebound]['data']=changed['blobs'][rebound]['data'].replace('command','foreign-command',1)
        with self.assertRaises(self.api.Rejected):
            self.helper.validate_globalized(f["DAG"], changed)
        wrong=copy.deepcopy(rows)
        bad=dict(witness,after_held_snapshot_id='foreign-cut')
        wrong[rows.index(receiver)]['action']['native_ingest_observation_blob']=blobs.json('DfsNativeReplicaDurableIngestObservationV1',bad)
        with self.assertRaises(self.api.Rejected):
            self.helper.globalize(f['DAG'],blueprints,f['INITIAL'],wrong,blobs)

    def test_missing_actual_source_return_is_unavailable(self):
        f = self.fixture
        returned = next(iter(f["DAG"].receiver_returns.values()))[1]
        events = dict(f["EVENTS"]); events.pop(returned)
        with self.assertRaises(self.api.Unavailable):
            f["dag"](events)

    def test_foreign_physical_root_is_rejected(self):
        f = self.fixture; blueprints = copy.deepcopy(f["BLUEPRINTS"])
        next(iter(blueprints.values()))["held"]["backend_root"] += "/foreign"
        with self.assertRaises(self.api.Rejected):
            self.helper.global_vectors(f["DAG"], blueprints, f["INITIAL"], f["ROWS"])

    def test_changed_actual_prepared_command_digest_is_unavailable(self):
        f = self.fixture
        key = next(key for key,value in f["EVENTS"].items()
            if value["collection"]=="boundary_packets" and value["record"]["facts"]["phase"]=="replica-journal-persisted"
            and (value["record"]["facts"]["command"].get("authority_probe") or {}).get("operation",{}).get("kind")=="prepared-operation")
        events = dict(f["EVENTS"]); changed = copy.deepcopy(events[key])
        changed["record"]["facts"]["command"]["authority_probe"]["operation"]["payload_digest"] += "-changed"
        events[key] = changed
        with self.assertRaises(self.api.Unavailable):
            f["dag"](events)

    def test_cycle_in_actual_derived_order_is_rejected(self):
        dag = copy.copy(self.fixture["DAG"])
        dag.parents = {key:set(value) for key,value in dag.parents.items()}
        child = next(key for key,value in dag.parents.items() if value)
        parent = next(iter(dag.parents[child])); dag.parents[parent].add(child)
        with self.assertRaises(self.api.Rejected):
            dag.require_acyclic()

    def test_frontier_missing_actual_predecessor_is_rejected(self):
        dag = self.fixture["DAG"]
        vector = {oid:0 for oid in dag.owners}
        receiver = next(iter(dag.request_sources))
        vector[receiver[0]] = receiver[1]
        with self.assertRaises(self.api.Rejected):
            dag.require_closed(vector)

    def test_after_held_cannot_be_borrowed_by_same_before_frontier(self):
        f = self.fixture
        blueprint = next(value for value in f["BLUEPRINTS"].values() if value["native_source"]["side"]=="after")
        changed = copy.deepcopy(blueprint)
        changed["as_of_source"] = dict(owner_id=blueprint["native_source"]["owner_id"],
            owner_ordinal=blueprint["native_source"]["owner_ordinal"], side="before")
        with self.assertRaises(self.api.Rejected):
            f["DAG"].horizon(changed)

    def test_actual_before_source_cannot_claim_its_mutated_after_cut(self):
        f = self.fixture
        for blueprint in f["BLUEPRINTS"].values():
            source = blueprint["native_source"]
            if source["side"] != "before":
                continue
            with self.subTest(source=source), self.assertRaises(self.api.Unavailable):
                f["DAG"].rebase(blueprint, int(source["owner_ordinal"]), "after")

    def test_unaccepted_completed_return_keeps_exact_edge_without_ack(self):
        f = self.fixture
        edge = f["DATA"]["actual_request_response_correlations"][0]
        key = edge["source_owner_id"], int(edge["source_ordinal"])
        events = dict(f["EVENTS"]); events.pop(key)
        links = [e for e in f["DATA"]["actual_request_response_correlations"] if e is not edge]
        dag = f["dag"](events, links)
        receiver = edge["receiver_owner_id"], int(edge["receiver_ordinal"])
        constructed, returned, accepted = dag.receiver_returns[receiver]
        self.assertIsNone(accepted)
        self.assertIn(constructed, dag.parents[returned])

    def test_global_vector_or_nonactor_cut_tampering_is_rejected(self):
        f = self.fixture
        output, initial, rows = self.helper.globalize(f["DAG"], f["BLUEPRINTS"], f["INITIAL"], f["ROWS"])
        header = dict(snapshot_blueprints=output, native_timeline=rows,
            owner_cuts={oid:dict(held_snapshot_id=name) for oid,name in initial.items()},
            global_observer_basis=dict(snapshot_blueprints=f["BLUEPRINTS"], initial=f["INITIAL"], native_timeline=f["ROWS"], ingest_observation_blobs={}))
        name = rows[0]["after_snapshot"]
        header["snapshot_blueprints"][name]["peer_cut_refs"] = dict(
            header["snapshot_blueprints"][name]["peer_cut_refs"])
        target = next(node for node,ref in header["snapshot_blueprints"][name]["peer_cut_refs"].items()
            if ref!=name)
        header["snapshot_blueprints"][name]["peer_cut_refs"][target] = name
        with self.assertRaises(self.api.Rejected):
            self.helper.validate_globalized(f["DAG"], header)

    def test_asof_ordinal_is_strict_u64(self):
        f = self.fixture
        for bad in [True, 1.5, "-1", "01", str(2**64)]:
            changed = copy.deepcopy(next(iter(f["BLUEPRINTS"].values())))
            changed["as_of_source"]["owner_ordinal"] = bad
            with self.subTest(value=bad), self.assertRaises(self.api.Rejected):
                f["DAG"].horizon(changed)


class MetadataContentModelTests(unittest.TestCase):
    """Same whole-shard model machinery; synthetic witness is not native proof."""
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='sinomo-shard-metadata-content-')
        output = Path(cls.scratch.name) / 'metadata-content.itf.json'
        scenario = ROOT / 'specs/quint/scenarios/dfs/whole_shard_metadata_content.qnt'
        command = ['quint', 'run', str(scenario),
            '--main=dfs_whole_shard_metadata_content_scenario', '--backend=typescript',
            '--invariant=safety', '--witnesses=terminalWitness', '--max-samples=1',
            '--max-steps=20', '--seed=1', '--verbosity=0', '--out-itf=' + str(output)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=45)
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)
        cls.states = [finite(state['s']) for state in json.loads(output.read_text())['states']]

    @classmethod
    def tearDownClass(cls):
        cls.scratch.cleanup()

    def test_metadata_zero_w_still_commits_only_through_metadata_ack_and_guard(self):
        self.assertEqual(len(self.states), 21)
        final = self.states[-1]
        self.assertEqual(final['head'], 6)
        self.assertEqual(final['visible'], 3)
        commits = dict(final['commits']['$map'])
        for op in (1, 2):
            self.assertEqual(commits[op]['receipts'], {'$set': []})
            self.assertEqual(commits[op]['acks'], {'$set': [1, 2, 3]})
        self.assertEqual(commits[3]['receipts'], {'$set': [1, 3]})

    def test_metadata_minority_cannot_admit(self):
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary([self.states[1], self.states[4]],
                [{'action': 'admitCommit', 'arguments': [1]}],
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_metadata_without_guard_cannot_commit(self):
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary([self.states[3], self.states[5]],
                [{'action': 'commit', 'arguments': [1, 11, False]}],
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)

    def test_content_missing_w2_cannot_prepare(self):
        with self.assertRaises(checker.Rejected):
            checker.verify_quint_boundary([self.states[13], self.states[15]],
                [{'action': 'prepare', 'arguments': [3]}],
                model=checker.MODEL_PATH, checks=checker.CHECKS_PATH)


def actual_put_intent_data_fixture(path, context, api, projector):
    # This is a saved-DATA fragment, not a native execution or author proof.
    data=json.loads(Path(path).read_text())
    events={(row['owner_id'],int(row['owner_ordinal'])):row for row in data['events']}
    materialized=[row for row in data['events'] if row['collection']=='packets'
        and row['record']['phase']=='commit-materialized']
    materialized.sort(key=lambda row:int(row['owner_ordinal']))
    blobs=api.Blobs({}); ctx=object.__new__(context.NativeContexts)
    ctx.events=events; ctx.artifact=data['artifact_binding']; ctx.scope=data['scope']
    ctx.owners={row['owner_id']:{**row,'component_incarnation':row['owner_id']} for row in data['owners']}; ctx.blobs=blobs
    ctx.bodies={}; timeline=[]; snapshots={}; blueprints={}; records={}; helds={}
    for op,body in data['operation_bodies'].items():
        canonical=body['canonical_payload_json_utf8'].encode()
        if hashlib.sha256(canonical).hexdigest()!=body['canonical_sha256']:
            raise projector.Rejected('actual canonical DATA bytes changed')
        payload=projector.exact_json(canonical); digest=body['native_digest']
        projector.sealed_content_requirement(payload,digest,canonical)
        reference=blobs.put('DfsPreparedOperationPayloadCanonicalV7',canonical)
        ctx.bodies[op]=(payload,digest,reference)
    for event in materialized:
        oid=event['owner_id']; n=int(event['owner_ordinal']); record=event['record']
        held=api.native_json(api.byte_vector(record['after_held_json'],'actual materialization'), 'actual materialization')
        op=record['operation']['operation_id']; payload,digest,reference=ctx.bodies[op]
        stored=next(row for row in held['tables']['prepared_operations_v7'] if row['digest']==digest)
        if stored['payload']!=payload: raise projector.Rejected('actual materialization changed canonical body')
        records[op]=stored; helds[op]=held
        name=oid+':cut:'+str(n)+':after:asof:'+str(n)
        action=dict(kind='commit-digest',operation=dict(operation_id=op,native_digest=digest['payload_digest'],
            payload_blob=reference),commit_record_blob=blobs.json('DfsPreparedOperationCommitReference',stored['committed_entry']))
        event_id=oid+':actual-event:'+str(n)
        timeline.append(dict(event_id=event_id,owner_id=oid,after_snapshot=name,action=action,
            raw_references=[dict(actual_mount_label=oid,owner_ordinal=str(n))]))
        blueprints[name]=dict(native_source=dict(owner_id=oid,owner_ordinal=str(n),side='after'),held=held)
        facts=ctx.certificate(snapshot_owner_id=oid,snapshot_native_prefix=held['certified_prefix'],visible_footprint_complete=True)
        snapshots[name]=dict(owner_id=oid,scope=data['scope'],projection_continuous=True,
            materialized_through_sequence=held['materialized_through_sequence'],local_certified_prefix=held['certified_prefix'],
            held_tables_blob=blobs.json('DfsMetadataTableSnapshotTables',held['tables']),
            native_operational_facts_blob=blobs.json('DfsShardNativeOperationalFactsV1',facts))
    # Reuse the ordinary local persistence/coordinator matcher. This fragment
    # certifies only the local row needed by supersession, never a complete W.
    content_op=next(op for op,(payload,_,_) in ctx.bodies.items() if payload['prepared_intent'] is not None)
    body,digest,reference=ctx.bodies[content_op]
    prepare=next(row for row in data['events'] if row['collection']=='packets'
        and row['record']['phase']=='prepare-durable' and row['record']['operation']==digest)
    guard=next(row for row in data['events'] if row['collection']=='guard_events'
        and row['record']['phase']=='admitted' and row['record']['digest']==digest)
    actual_cfg=guard['record']['facts']['authority']['expected']
    name='receipts:'+content_op
    fact=ctx.certificate(actual_memory_cut_source=dict(owner_id=prepare['owner_id'],
        owner_ordinal=str(prepare['owner_ordinal']),side='before'),
        peer_certified_cuts={actual_cfg['primary_node_id']:dict(held_snapshot_id=name)},
        operation_observations={content_op:dict(initial_payload_blob=reference,
            initial_content_receipt_observation_blobs=[],durable_content_receipt_blobs=[])})
    seed=dict(scope=data['scope'],owners=list(ctx.owners.values()),envelope_binding=data['artifact_binding'],
        finite_dictionary=dict(operation_aliases={op:i+1 for i,op in enumerate(ctx.bodies)}),
        snapshot_contexts={name:dict(active_config=dict(
            native_config_blob=blobs.json('DfsMetadataShardConfig',actual_cfg)),
            native_operational_facts_blob=blobs.json('DfsShardNativeOperationalFactsV1',fact))})
    raw_index={key:dict(kind='kernel' if row['collection']=='packets' else
        'content-receipt' if row['collection']=='content_receipts' else 'guard',
        ordinal=int(row['owner_ordinal']),record=row['record']) for key,row in events.items()}
    derived,_=api.derive_initial_local_receipts(seed,raw_index,blobs)
    source=api.native_json(blobs.values[derived[name]['native_operational_facts_blob']]['data'].encode(),'actual derived receipt')
    refs=source['operation_observations'][content_op]['initial_content_receipt_observation_blobs']
    ctx.header=dict(native_timeline=timeline,snapshot_blueprints=blueprints,
        initial_content_receipt_observation_blobs_by_operation={content_op:refs})

    adapter=object.__new__(projector.NativeProjectorV1)
    adapter.scope=data['scope']; adapter.owners=ctx.owners; adapter.blobs=blobs.values
    adapter.trace=dict(events=timeline,snapshots=snapshots,implementation_revision=data['artifact_binding']['implementation_revision'])
    latest=helds[materialized[-1]['record']['operation']['operation_id']]['tables']
    actual_operations=[row['digest']['operation_id'] for row in latest['prepared_operations_v7']
        if not row['digest']['operation_id'].startswith('genesis:')]
    adapter.operations={op:index+1 for index,op in enumerate(sorted(actual_operations))}
    adapter.payloads={op:{digest['payload_digest']:1} for op,(_,digest,_) in ctx.bodies.items()}
    adapter.initial_payloads={op:payload for op,(payload,_,_) in ctx.bodies.items()}
    original=materialized[0]['record']['operation']['operation_id']
    return dict(ctx=ctx,adapter=adapter,original=original,records=records,helds=helds,data=data)


class NativePutIntentEffectDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=os.environ.get('SINOMO_NATIVE_INTENT_EFFECT_DATA')
        if not path: raise unittest.SkipTest('actual saved PutIntent materialization DATA is required')
        here=Path(__file__).resolve().parent
        cls.projector=importlib.util.module_from_spec(spec:=importlib.util.spec_from_file_location('intent_projector',here/'shard-native-projector.py'))
        spec.loader.exec_module(cls.projector)
        cls.context=importlib.util.module_from_spec(spec:=importlib.util.spec_from_file_location('intent_context',here/'shard-native-context.py'))
        spec.loader.exec_module(cls.context)
        cls.api=cls.context.ASSEMBLER
        # Wire the same proposed stateless DATA dependency in this isolated test.
        cls.api.SUBTREE.PROJECTOR=cls.projector
        cls.fixture=actual_put_intent_data_fixture(path,cls.context,cls.api,cls.projector)

    def setUp(self):
        f=self.fixture
        self.ctx=copy.copy(f['ctx']); self.ctx.events=dict(f['ctx'].events)
        self.adapter=copy.copy(f['adapter']); self.adapter.trace=copy.deepcopy(f['adapter'].trace)
        self.payload,self.digest,self.reference=self.ctx.bodies[f['original']]
        self.record=f['records'][f['original']]
        self.tables=copy.deepcopy(list(f['helds'].values())[-1]['tables'])
        self.certificate=self.ctx.declared_footprint(self.payload,self.record,list(f['helds'].values())[-1])

    def validate(self):
        self.adapter.validate_atomic_rows(self.tables,self.payload,self.certificate,self.record['committed_entry'])

    def test_original_actual_materialization_and_later_exact_terminal_are_validated(self):
        self.assertEqual(len(self.certificate['effect_materializations']),2)
        self.assertEqual(self.certificate['superseded_effects'][0]['table'],'intents')
        self.validate()

    def test_original_effect_still_present_requires_actual_materialization(self):
        held=self.fixture['helds'][self.fixture['original']]
        certificate=self.ctx.declared_footprint(self.payload,self.record,held)
        self.adapter.validate_atomic_rows(held['tables'],self.payload,certificate,self.record['committed_entry'])

    def test_dropped_original_source_packet_is_unavailable(self):
        self.ctx.events={key:row for key,row in self.ctx.events.items()
            if not (row['collection']=='packets' and row['record']['phase']=='commit-materialized'
                and row['record']['operation']==self.digest)}
        with self.assertRaises(self.context.Unavailable):
            self.ctx.declared_footprint(self.payload,self.record,list(self.fixture['helds'].values())[-1])

    def test_dropped_original_materialization_reference_is_unavailable(self):
        self.certificate['effect_materializations']=[]
        with self.assertRaises(self.projector.Unavailable): self.validate()

    def test_dropped_original_snapshot_is_unavailable(self):
        name=self.certificate['effect_materializations'][0]['snapshot_id']
        self.adapter.trace['snapshots'].pop(name)
        with self.assertRaises(self.projector.Unavailable): self.validate()

    def test_wrong_prior_intent_id_version_or_hash_is_rejected(self):
        original=copy.deepcopy(self.certificate)
        for field in ('intent_id','target_version','content_hash'):
            self.certificate=copy.deepcopy(original)
            self.certificate['superseded_effects'][0]['prior_effect'][field]+='-wrong'
            with self.subTest(field=field),self.assertRaises(self.projector.Rejected): self.validate()

    def test_changed_actual_terminal_row_is_rejected(self):
        self.tables['intents'][0]['updated_at']='2026-10-10T00:00:00Z'
        with self.assertRaises(self.projector.Rejected): self.validate()

    def test_changed_sealed_terminal_identity_is_rejected(self):
        prior=self.certificate['superseded_effects'][0]['prior_effect']
        successor=self.fixture['records'][next(op for op in self.fixture['records'] if op!=self.fixture['original'])]
        body=copy.deepcopy(successor['payload']);body['terminal_intent']['coordinator_node_id']='foreign'
        with self.assertRaises(self.projector.Rejected): self.projector.exact_intent_terminal(prior,body)

    def test_foreign_later_digest_is_rejected(self):
        witness=self.certificate['superseded_effects'][0]
        record=self.adapter.blob(witness['successor_record_blob'],'DfsPreparedOperationRecord')
        record['digest']['payload_digest']+='-foreign'
        witness['successor_record_blob']=self.ctx.blobs.json('DfsPreparedOperationRecord',record)
        with self.assertRaises(self.projector.Rejected): self.validate()

    def test_original_cut_cannot_substitute_for_later_materialization(self):
        witness=self.certificate['superseded_effects'][0]
        witness['successor_materialization']=copy.deepcopy(self.certificate['effect_materializations'][0])
        with self.assertRaises(self.projector.Rejected): self.validate()

    def test_receipt_wrong_full_tuple_is_rejected(self):
        original=copy.deepcopy(self.certificate)
        for field in ('replica_node_id','file_system_id','object_id','committed_version','content_hash','owner_node_id'):
            self.certificate=copy.deepcopy(original)
            witness=next(w for w in self.certificate['superseded_effects'] if w['table']=='receipts')
            row=witness['replacement_effect']
            target=row if field in ('replica_node_id','content_hash') else row['receipt']
            target[field]+='-foreign'
            with self.subTest(field=field),self.assertRaises(self.projector.Rejected):self.validate()

    def test_receipt_missing_original_materialization_is_unavailable(self):
        self.certificate['effect_materializations']=[w for w in self.certificate['effect_materializations'] if w['table']!='receipts']
        with self.assertRaises(self.projector.Unavailable):self.validate()

    def test_receipt_unknown_persistence_origin_is_unavailable(self):
        witness=next(w for w in self.certificate['superseded_effects'] if w['table']=='receipts')
        evidence=self.adapter.blob(witness['successor_receipt_observation_blob'],'DfsNativeContentReceiptObservationV1')
        evidence['origin']='unobserved'
        witness['successor_receipt_observation_blob']=self.ctx.blobs.json('DfsNativeContentReceiptObservationV1',evidence)
        with self.assertRaises(self.projector.Unavailable):self.validate()

    def test_receipt_unobserved_later_row_has_no_inferred_replacement(self):
        held=copy.deepcopy(list(self.fixture['helds'].values())[-1]);held['tables']['receipts']=[]
        with self.assertRaises(self.context.Unavailable):
            self.ctx.declared_footprint(self.payload,self.record,held)

    def test_missing_current_terminal_has_no_inferred_replacement(self):
        held=copy.deepcopy(list(self.fixture['helds'].values())[-1]);held['tables']['intents']=[]
        with self.assertRaises(self.context.Unavailable): self.ctx.declared_footprint(self.payload,self.record,held)



class SealedPayloadTypedDataTests(unittest.TestCase):
    """Malformed saved canonical DATA must fail typed, never classify as W0."""
    @classmethod
    def setUpClass(cls):
        path = os.environ.get('SINOMO_NATIVE_MATERIALIZATION_DATA')
        if not path:
            raise unittest.SkipTest('requires original saved materialization DATA')
        data = json.loads(Path(path).read_text())
        cls.bodies = []
        for row in data['operation_bodies'].values():
            canonical = row['canonical_payload_json_utf8'].encode()
            if hashlib.sha256(canonical).hexdigest() != row['canonical_sha256']:
                raise projector.Rejected('original saved canonical DATA bytes changed')
            cls.bodies.append((projector.exact_json(canonical), row['native_digest'], canonical))
        cls.metadata = next(row for row in cls.bodies if row[0]['prepared_intent'] is None)
        cls.content = next(row for row in cls.bodies if row[0]['prepared_intent'] is not None)

    def malformed(self, original, edit):
        payload, digest, _ = original
        payload, digest = copy.deepcopy(payload), copy.deepcopy(digest)
        edit(payload)
        canonical = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode()
        digest['payload_digest'] = 'dfs-prepared-operation-v7:blake3:' + projector.blake3(
            b'sinomo.dfs.prepared-operation\0v7\0' + canonical)
        # Matching negative canonical bytes reach the type predicate rather
        # than failing an unrelated old digest. They never establish native proof.
        with self.assertRaises((projector.Rejected, projector.Unavailable)):
            projector.sealed_content_requirement(payload, digest, canonical)

    def test_actual_original_bodies_keep_their_quorums(self):
        values = [projector.sealed_content_requirement(*row)['min_commit_replicas']
            for row in self.bodies]
        self.assertEqual(sorted(values), [0, 2])

    def test_metadata_nondict_effect_is_typed_unavailable(self):
        for bad in (None, [], 'effect', 1):
            with self.subTest(value=bad):
                self.malformed(self.metadata,
                    lambda p: p['metadata_effects'].__setitem__(0, bad))

    def test_metadata_nondict_intent_and_policy_are_typed_unavailable(self):
        for bad in (None, [], 'intent', 1):
            with self.subTest(field='intent', value=bad):
                self.malformed(self.metadata, lambda p: p.update(
                    metadata_effects=[dict(type='put-intent', intent=bad)]))
            with self.subTest(field='policy', value=bad):
                self.malformed(self.metadata, lambda p: p.update(metadata_effects=[dict(
                    type='put-intent', intent=dict(file_system_id=p['file_system_id'], policy=bad))]))

    def test_metadata_nondict_receipt_rows_are_typed_unavailable(self):
        for bad in (None, [], 'receipt', 1):
            with self.subTest(field='record', value=bad):
                self.malformed(self.metadata, lambda p: p.update(
                    metadata_effects=[dict(type='put-receipt', receipt=bad)]))
            with self.subTest(field='receipt', value=bad):
                self.malformed(self.metadata, lambda p: p.update(metadata_effects=[dict(
                    type='put-receipt', receipt=dict(receipt=bad))]))

    def test_content_nondict_prepared_terminal_and_policy_are_typed(self):
        for bad in (None, [], 'intent', 1):
            for field in ('prepared_intent', 'terminal_intent'):
                with self.subTest(field=field, value=bad):
                    self.malformed(self.content, lambda p: p.__setitem__(field, bad))
            with self.subTest(field='policy', value=bad):
                def edit(p):
                    p['prepared_intent']['policy'] = bad
                    p['terminal_intent']['policy'] = bad
                self.malformed(self.content, edit)

    def test_content_nondict_receipt_rows_are_typed_unavailable(self):
        for bad in (None, [], 'receipt', 1):
            with self.subTest(field='record', value=bad):
                self.malformed(self.content,
                    lambda p: p['content_receipts'].__setitem__(0, bad))
            with self.subTest(field='receipt', value=bad):
                self.malformed(self.content,
                    lambda p: p['content_receipts'][0].__setitem__('receipt', bad))

    def test_content_nondict_node_ids_do_not_reach_unchecked_set(self):
        for bad in (None, [], {}, True, 1):
            with self.subTest(field='plan', value=bad):
                def edit(p):
                    p['prepared_intent']['desired_replica_node_ids'][0] = bad
                    p['terminal_intent']['desired_replica_node_ids'][0] = bad
                self.malformed(self.content, edit)
            with self.subTest(field='required', value=bad):
                self.malformed(self.content,
                    lambda p: p['required_replica_node_ids'].__setitem__(0, bad))
            with self.subTest(field='receipt', value=bad):
                self.malformed(self.content,
                    lambda p: p['content_receipts'][0].__setitem__('replica_node_id', bad))


class NativeReplicaFreezeReadDataTests(unittest.TestCase):
    """SAME future actual read/ingest DATA; no invented marker or Rust execution."""
    @classmethod
    def setUpClass(cls):
        path = os.environ.get('SINOMO_NATIVE_GROUPED_INPUTS')
        skeleton_path = os.environ.get('SINOMO_NATIVE_SELECTED_CUSTODY_SKELETON')
        if not path or not skeleton_path:
            raise unittest.SkipTest('requires actual read-boundary input and its same selected skeleton')
        data = json.loads(Path(path).read_text())
        skeleton = json.loads(Path(skeleton_path).read_text())
        if data['artifact_binding'] != skeleton['envelope_binding'] or data['scope'] != skeleton['scope']:
            raise AssertionError('actual freeze read DATA changes its capture binding')
        here = Path(__file__).resolve().parent
        spec = importlib.util.spec_from_file_location('actual_replica_freeze_read_data',
            here / 'assemble-shard-native-trace.py')
        cls.api = importlib.util.module_from_spec(spec); spec.loader.exec_module(cls.api)
        kinds = dict(packets='kernel', guard_events='guard', boundary_packets='boundary')
        cls.raw = {(e['owner_id'], e['owner_ordinal']): dict(kind=kinds.get(e['collection'], e['collection']),
            ordinal=e['owner_ordinal'], record=e['record']) for e in data['owner_local_events']}
        cls.owners = {o['owner_id']: o for o in skeleton['owners']}; cls.scope = data['scope']
        cls.events = {(e['owner_id'],e['owner_ordinal']):e for e in data['owner_local_events']}
        cls.correlations = data['actual_request_response_correlations']
        cls.headers = {o['owner_id']:json.loads(Path(o['header_path']).read_text()) for o in data['owners']}
        spec = importlib.util.spec_from_file_location('actual_replica_freeze_read_causality',
            here / 'shard-native-causality.py')
        cls.causal = importlib.util.module_from_spec(spec); spec.loader.exec_module(cls.causal)
        cls.reads = [k for k, r in cls.raw.items() if r['kind'] == 'boundary'
            and r['record']['facts']['phase'] == 'replica-write-freeze-read']
        if not cls.reads:
            raise unittest.SkipTest('saved actual capture predates normal replica freeze read observations')

    def verify(self, raw, key):
        return self.api.verify_replica_freeze_read(raw, self.owners, self.scope, key)

    def test_actual_prior_read_is_linked_without_rewriting_earlier_getter_bytes(self):
        for key in self.reads:
            with self.subTest(owner=key[0], ordinal=key[1]):
                result = self.verify(self.raw, key)
                self.assertLess(key[1], int(result['ingest_source']['owner_ordinal']))
                self.assertEqual(result['actual_read_fact'], self.raw[key]['record']['facts'])
                self.assertEqual(self.raw[key]['record']['before_held_json'],
                    self.raw[key]['record']['after_held_json'])
                self.assertIn(result['observed_freeze']['observation'], ('absent', 'present'))

    def test_actual_source_call_is_a_predecessor_of_read_and_ingest(self):
        _,verify_edge=self.api.initial_native_verifiers(self.scope,list(self.owners.values()),self.raw)
        dag=self.causal.NativeExecutionDag(self.api,self.events,self.owners,self.scope,
            self.correlations,verify_edge,self.headers)
        for key in self.reads:
            qualified=self.verify(self.raw,key);source=qualified['ingest_source']
            ingest=source['owner_id'],int(source['owner_ordinal']);start,_=dag.request_sources[ingest]
            self.assertIn(start,dag.parents[key])
            self.assertLess(key[1],ingest[1])
            horizon=dag.close({oid:key[1]if oid==key[0]else 0 for oid in self.owners})
            self.assertGreaterEqual(horizon[start[0]],start[1],
                'read frontier cannot precede the actual same-call source start')

    def test_missing_future_foreign_and_changed_read_facts_remain_rejected(self):
        key = self.reads[0]; original = self.verify(self.raw, key)
        ingest = original['ingest_source']['owner_id'], int(original['ingest_source']['owner_ordinal'])
        for case in ('missing-writer', 'missing-ingest-link', 'wrong-read-id', 'wrong-permit',
                     'foreign-resident', 'not-observed-result', 'changed-held-data', 'future-read',
                     'admitted-kind-mismatch', 'admitted-foreign-shard'):
            raw = copy.deepcopy(self.raw); read = raw[key]['record']
            if case == 'missing-writer': read['facts']['actual_writer_token'] = ''
            elif case == 'missing-ingest-link': raw[ingest]['record']['normal_replica_freeze_read_id'] = None
            elif case == 'wrong-read-id': read['facts']['read_id'] += '-foreign'
            elif case == 'wrong-permit': read['facts']['permit']['source_epoch'] += 1
            elif case == 'foreign-resident': read['resident_custody']['resident_handle_token'] += '-foreign'
            elif case == 'admitted-kind-mismatch':
                for current in (read, raw[ingest]['record']):
                    current['resident_custody']['admitted_scope'] = dict(kind='global-exclusive')
                    current['resident_custody']['admission_kind'] = 'exact-shard'
            elif case == 'admitted-foreign-shard':
                for current in (read, raw[ingest]['record']):
                    current['resident_custody']['admission_kind'] = 'exact-shard'
                    current['resident_custody']['admitted_scope'] = dict(kind='exact-shard',
                        file_system_id=self.scope['file_system_id'], shard_id=self.scope['shard_id']+'-foreign')
            elif case == 'not-observed-result': read['facts']['observed_freeze'] = dict(observation='not-observed')
            elif case == 'changed-held-data':
                held = json.loads(bytes(read['after_held_json']))
                held['journal_next_sequence'] += 1
                read['after_held_json'] = list(json.dumps(held, separators=(',', ':')).encode())
            elif case == 'future-read':
                raw.pop(key); moved = (key[0], ingest[1] + 1); raw[moved] = dict(kind='boundary', record=read)
                key_for_case = moved
            with self.subTest(case=case), self.assertRaises((self.api.Rejected, self.api.Unavailable)):
                self.verify(raw, key_for_case if case == 'future-read' else key)


class NativeReplicaFreezeObservationDataTests(unittest.TestCase):
    """Saved actual DATA checks; no model engine or whole trace acceptance."""
    @classmethod
    def setUpClass(cls):
        path=os.environ.get('SINOMO_NATIVE_GROUPED_INPUTS')
        if not path:raise unittest.SkipTest('requires genuine saved normal READ capture DATA')
        cls.inputs=json.loads(Path(path).read_text())
        spec=importlib.util.spec_from_file_location('actual_freeze_observation_builder',
            Path(__file__).resolve().parent/'build-shard-native-skeleton.py')
        cls.builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.builder)
        cls.data=cls.builder.FiniteCaptureBuilder(cls.inputs)
        # Isolated DATA selection, never build/globalize/context/model/replay.
        cls.data.select_operations();cls.data.select_not_sent_journal_attempts()
        cls.data.select_receivers();cls.data.select_materializations();cls.data.select_initial_cuts()
        cls.raw={(oid,n):dict(kind=cls.builder.KINDS[event['collection']],ordinal=n,record=event['record'])
            for (oid,n),event in cls.data.events.items()}

    def test_genuine_unknown_initial_peer_cuts_are_source_closed_and_keep_pending_records(self):
        data=self.data;api=self.builder.A
        _,edge=api.initial_native_verifiers(data.scope,data.owners,self.raw)
        dag=self.builder.CAUSAL.NativeExecutionDag(api,data.events,data.owner_map,data.scope,
            self.inputs['actual_request_response_correlations'],edge,data.headers)
        vector={oid:dag.horizon(data.blueprints[name])[1] for oid,name in data.initial.items()}
        dag.require_closed(vector)
        peers=0
        for oid,name in data.initial.items():
            if oid not in data.custody:continue
            peers+=1;event,side,_,_,held=data.cuts[name]
            self.assertEqual(side,'after');self.assertEqual(held['write_freeze'],{'observation':'not-observed'})
            self.assertEqual(event['record']['resident_custody']['resident_handle_token'],
                data.custody[oid]['resident_handle_token'])
            for read in data.timeline:
                if read['owner_id']==oid and read['action']['kind']=='replica-freeze-read':
                    self.assertLess(vector[oid],int(read['event_id'].rsplit(':',1)[1]))
            # Initial DATA must retain every row; pending rows remain pending
            # and are never relabeled terminal by this observation boundary.
            actual=json.loads(bytes(event['record']['after_held_json']))
            self.assertEqual(held['tables'],actual['tables'])
        self.assertGreater(peers,0)

    def test_unknown_gate_keeps_actual_bytes_and_rejects_invented_absence_or_primary_authority(self):
        data=self.data;api=self.builder.A
        for oid,name in data.initial.items():
            if oid not in data.custody:continue
            blueprint=data.blueprints[name];source=blueprint['native_source']
            gate=dict(writer_owner_id=oid,read_under_actual_owner=False,
                observed_pending_handoff=False,durable_write_frozen=None,
                write_freeze_observation='not-observed',actual_replica_ingest_read_source=None)
            facts=dict(actual_operational_gate=gate,actual_cut_as_of_source=blueprint['as_of_source'],
                actual_memory_cut_source=source)
            config=next(iter(data.operations.values()))['config']
            context=dict(owner_id=oid,active_config=config,
                native_operational_facts_blob=data.blobs.json('DfsShardNativeOperationalFactsV1',facts))
            api.verify_replica_freeze_gate(self.raw,data.owner_map,data.scope,data.blobs,context,
                data.binding['implementation_revision'])
            for case in ('invented-absent','claimed-owned-read','foreign-writer','primary-authority',
                         'admitted-kind','foreign-admitted-shard'):
                changed=copy.deepcopy(facts);blobs=api.Blobs(copy.deepcopy(data.blobs.values));ctx=copy.deepcopy(context)
                raw=copy.deepcopy(self.raw)
                if case=='invented-absent':changed['actual_operational_gate']['durable_write_frozen']=False
                elif case=='claimed-owned-read':changed['actual_operational_gate']['read_under_actual_owner']=True
                elif case=='foreign-writer':changed['actual_operational_gate']['writer_owner_id']=next(k for k in data.owner_map if k!=oid)
                elif case in ('admitted-kind','foreign-admitted-shard'):
                    key=source['owner_id'],int(source['owner_ordinal']);custody=raw[key]['record']['resident_custody']
                    if case=='admitted-kind':custody['admitted_scope']={'kind':'global-exclusive'}
                    else:custody['admitted_scope']['shard_id']=data.scope['shard_id']+'-foreign'
                else:
                    native=json.loads(blobs.values[config['native_config_blob']]['data'])
                    native['primary_node_id']=data.owner_map[oid]['node_id']
                    ctx['active_config']=dict(config,native_config_blob=blobs.json('DfsMetadataShardConfig',native))
                ctx['native_operational_facts_blob']=blobs.json('DfsShardNativeOperationalFactsV1',changed)
                with self.subTest(owner=oid,case=case),self.assertRaises((api.Rejected,api.Unavailable)):
                    api.verify_replica_freeze_gate(raw,data.owner_map,data.scope,blobs,ctx,
                        data.binding['implementation_revision'])

    def test_genuine_read_actions_keep_exact_ordinal_and_unique_ingest_dependency(self):
        data=self.data;api=self.builder.A
        reads=[row for row in data.timeline if row['action']['kind']=='replica-freeze-read']
        self.assertTrue(reads)
        for row in reads:
            key=row['owner_id'],int(row['event_id'].rsplit(':',1)[1])
            qualified=api.verify_replica_freeze_read(self.raw,data.owner_map,data.scope,key)
            proof=json.loads(data.blobs.values[row['action']['native_freeze_read_observation_blob']]['data'])
            self.assertEqual(proof,data.cert(**qualified))
            self.assertEqual(row['raw_references'],[dict(actual_mount_label=key[0],owner_ordinal=str(key[1]))])
            source=qualified['ingest_source'];ingest_id=source['owner_id']+':actual-event:'+source['owner_ordinal']
            self.assertIn(row['event_id'],data.graph[ingest_id])
            self.assertLess(key[1],int(source['owner_ordinal']))
            # Claiming the genuine result at an earlier ordinal still violates
            # the exact raw-source/link proof; no Unknown-to-absence shortcut.
            wrong=copy.deepcopy(self.raw);wrong[key]['record']['facts']['observed_freeze']={'observation':'not-observed'}
            with self.assertRaises((api.Rejected,api.Unavailable)):
                api.verify_replica_freeze_read(wrong,data.owner_map,data.scope,key)


class NativeSelectedResidentCustodyDataTests(unittest.TestCase):
    """Actual saved receiver ownership DATA; no whole context/model engine."""
    @classmethod
    def setUpClass(cls):
        path = os.environ.get('SINOMO_NATIVE_GROUPED_INPUTS')
        if not path:
            raise unittest.SkipTest('requires complete saved grouped native capture DATA')
        here = Path(__file__).resolve().parent
        cls.input_bytes = Path(path).read_bytes()
        cls.input_sha256 = hashlib.sha256(cls.input_bytes).hexdigest()
        data = json.loads(cls.input_bytes)
        cls.inputs = data
        spec = importlib.util.spec_from_file_location('selected_custody_data_builder',
            here / 'build-shard-native-skeleton.py')
        cls.builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.builder)
        cls.actor = cls.builder.FiniteCaptureBuilder(data)
        skeleton_path = os.environ.get('SINOMO_NATIVE_SELECTED_CUSTODY_SKELETON')
        if not skeleton_path:
            raise unittest.SkipTest('requires the same saved selected receiver blueprints')
        skeleton = json.loads(Path(skeleton_path).read_text())
        cls.skeleton = skeleton
        cls.skeleton_sha256 = hashlib.sha256(Path(skeleton_path).read_bytes()).hexdigest()
        if (skeleton['envelope_binding'] != data['artifact_binding']
                or skeleton['scope'] != data['scope']
                or set(skeleton['finite_dictionary']['operation_aliases']) != set(data['selected_operation_ids'])):
            raise AssertionError('saved custody blueprints differ from their actual input binding')
        cls.interval_sources = {}
        for blueprint in skeleton['global_observer_basis']['snapshot_blueprints'].values():
            reference = blueprint.get('native_resident_custody_blob')
            if not reference:
                continue
            fact = json.loads(skeleton['blobs'][reference]['data'])
            source = blueprint['native_source']
            key = source['owner_id'], int(source['owner_ordinal'])
            actual = cls.actor.events[key]['record']['resident_custody']
            if fact['actual_custody'] != actual or fact['actual_boundary_source'] != dict(
                    owner_id=key[0], owner_ordinal=str(key[1])):
                raise AssertionError('saved selected cut retargets its actual custody source')
            cls.interval_sources.setdefault(key[0], set()).add(key[1])
        spec = importlib.util.spec_from_file_location('selected_custody_data_context',
            here / 'shard-native-context.py')
        cls.context_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.context_module)
        cls.peers = []
        for oid, owner in cls.actor.owner_map.items():
            if not owner['storage_generation_token'].startswith('resident-slot:'):
                continue
            custody = cls.actor.custody[oid]
            event = cls.actor.events[oid, int(custody['observer_owner_ordinal'])]
            bootstrap = min((row for row in cls.actor.events.values()
                if row['owner_id'] == oid and row['record'].get('resident_custody')),
                key=lambda row: row['owner_ordinal'])
            cls.peers.append((oid, owner, event, bootstrap))
        if not cls.peers:
            raise AssertionError('saved actual receiver custody is absent')

    def context(self, oid, owner, event):
        actor = object.__new__(self.context_module.NativeContexts)
        ordinal = event['owner_ordinal']
        name = 'actual-selected-receiver-cut'
        actor.cut_refs = {name: (oid, ordinal, 'before',
            {'backend_root': event['record']['resident_custody']['backend_root']})}
        actor.memory_sources = {name: (oid, ordinal, 'before')}
        actor.events = {(oid, ordinal): event}
        actor.owners = {oid: owner}
        actor.scope = self.actor.scope
        fact = dict(owner_id=oid, actual_custody=event['record']['resident_custody'],
            actual_boundary_source=dict(owner_id=oid, owner_ordinal=str(ordinal)))
        actor.native = lambda reference: fact
        return actor, name, dict(native_resident_custody_blob='actual-custody-data')

    def test_committed_replica_view_requires_its_exact_selected_author_guard(self):
        # Exercise the normal context consumer with complete saved raw DATA.
        # A replica holding the original committed record owns no author guard.
        api = self.context_module
        actor = api.NativeContexts(self.inputs, self.skeleton)
        for source in (actor.header['snapshot_blueprints'],
                       actor.header['global_observer_basis']['snapshot_blueprints']):
            for name, blueprint in source.items():
                actor.snapshot_source(name, blueprint)
        dag = api.CAUSAL.NativeExecutionDag(api.ASSEMBLER, actor.events, actor.owners,
            actor.scope, actor.inputs['actual_request_response_correlations'],
            actor.verified_edge, actor.raw_headers)
        api.CAUSAL.validate_globalized(dag, actor.header)
        candidates = []
        for name, blueprint in actor.header['snapshot_blueprints'].items():
            oid, ordinal, _, held = actor.cut_refs[name]
            if not actor.owners[oid]['storage_generation_token'].startswith('resident-slot:'):
                continue
            records = [row for row in held['tables'].get('prepared_operations_v7', [])
                if row['status'] == 'committed' and row['digest']['operation_id'] in actor.bodies]
            if len(records) >= 2:
                candidates.append((ordinal, name, blueprint, records))
        self.assertTrue(candidates, 'requires actual replica DATA holding two selected commits')
        _, name, blueprint, records = max(candidates, key=lambda row: row[0])
        oid, ordinal, _, held = actor.cut_refs[name]
        actor.require_owned_cut(name, blueprint)
        actual = actor.operations_at(oid, ordinal, held, blueprint)
        op = records[-1]['digest']['operation_id']
        reference = actor.header['guard_observation_blobs_by_operation'][op]
        wrapper = actor.native(reference); raw = actor.native(wrapper['actual_native_guard_blob'])
        author = raw['owner_label']; author_node = actor.owners[author]['node_id']
        self.assertNotEqual(author, oid)
        self.assertEqual(actual[op]['committed_guard_observation_blob'], reference)
        selected = actor.cut_refs[blueprint['peer_cut_refs'][author_node]]
        self.assertEqual(selected[0], author)
        self.assertLessEqual(int(raw['owner_ordinal']), selected[1])
        self.assertEqual(raw['digest'], records[-1]['digest'])
        for case in ('missing-author', 'foreign-frontier', 'earlier-author'):
            changed = copy.deepcopy(blueprint)
            if case == 'missing-author':
                del changed['peer_cut_refs'][author_node]
            elif case == 'foreign-frontier':
                changed['peer_cut_refs'][author_node] = name
            else:
                changed['peer_cut_refs'][author_node] = actor.header['owner_cuts'][author]['held_snapshot_id']
            with self.subTest(case=case), self.assertRaises((api.Rejected, api.Unavailable)):
                actor.operations_at(oid, ordinal, held, changed)
        other = next(row['digest']['operation_id'] for row in records
                     if row['digest']['operation_id'] != op)
        original = actor.header['guard_observation_blobs_by_operation'][op]
        try:
            actor.header['guard_observation_blobs_by_operation'][op] = actor.header['guard_observation_blobs_by_operation'][other]
            with self.assertRaisesRegex(api.Rejected, 'exact original operation digest'):
                actor.operations_at(oid, ordinal, held, blueprint)
        finally:
            actor.header['guard_observation_blobs_by_operation'][op] = original

    def test_selected_receiver_binding_uses_its_same_actual_resident_handle(self):
        changed_from_bootstrap = 0
        for oid, owner, event, bootstrap in self.peers:
            actual = event['record']['resident_custody']
            with self.subTest(owner=oid, ordinal=event['owner_ordinal']):
                # Initial/probe/READ cuts retain actual custody, but the binding
                # begins at the first selected persist or authority-probe call.
                selected=[]
                for (actor,ordinal),observed in self.actor.events.items():
                    if actor!=oid or not observed['record'].get('resident_custody'): continue
                    facts=observed['record']['facts']
                    if facts['phase'] not in ('replica-journal-persisted','replica-prefix-probe-accepted'): continue
                    operation=(facts['command'].get('authority_probe') or {}).get('operation',{})
                    decoded=[row['decoded_entry'] for row in self.actor.headers[oid].get('actual_received_frame_decodes',[])
                             if int(row['source_owner_ordinal'])==ordinal]
                    if (operation.get('operation_id') in self.actor.selected
                            or any(row['mutation'].get('operation_id') in self.actor.selected for row in decoded)):
                        selected.append(ordinal)
                self.assertTrue(selected,'actual selected persist/probe receiver coverage required')
                self.assertEqual(event['owner_ordinal'],min(selected),
                    'binding must use the first same actual selected persist/probe, not a later handle')
                for ordinal in self.interval_sources[oid]:
                    self.assertEqual(self.actor.events[oid, ordinal]['record']['resident_custody'],
                        dict(actual, observer_owner_ordinal=str(ordinal)),
                        'every modeled interval cut retains the same actual handles and admission scope')
                self.assertEqual(owner['storage_generation_token'],
                    'resident-slot:' + actual['resident_handle_token'])
                self.assertEqual(owner['root_lease_token'],
                    'mounted-resident-owner:' + actual['data_owner_token'])
                actor, name, blueprint = self.context(oid, owner, event)
                actor.require_owned_cut(name, blueprint)
                if actual['resident_handle_token'] != bootstrap['record']['resident_custody']['resident_handle_token']:
                    changed_from_bootstrap += 1
        self.assertGreater(changed_from_bootstrap, 0,
            'DATA must exercise an actual resident replacement before the selected interval')

    def test_bootstrap_and_retargeted_resident_tokens_are_still_rejected(self):
        checked_bootstrap = 0
        for oid, owner, event, bootstrap in self.peers:
            actual = event['record']['resident_custody']
            old = bootstrap['record']['resident_custody']
            for token in (old['resident_handle_token'], 'retargeted-resident-data-control'):
                if token == actual['resident_handle_token']:
                    continue
                changed = dict(owner, storage_generation_token='resident-slot:' + token)
                actor, name, blueprint = self.context(oid, changed, event)
                with self.subTest(owner=oid, token=token), self.assertRaisesRegex(
                        self.context_module.Rejected,
                        'replica owner binding substitutes another actual resident/data-owner handle'):
                    actor.require_owned_cut(name, blueprint)
                checked_bootstrap += token == old['resident_handle_token']
        self.assertGreater(checked_bootstrap, 0)


    def test_prepared_receipt_replacement_requires_earlier_exact_return_at_current_frontier(self):
        # Positive DATA is the actual sealed prepare and already returned bytes,
        # not a synthetic terminal record, guard, W threshold or model state.
        api=self.context_module
        actor=api.NativeContexts(self.inputs,self.skeleton)
        for blueprints in (actor.header['snapshot_blueprints'],
                           actor.header['global_observer_basis']['snapshot_blueprints']):
            for name,blueprint in blueprints.items(): actor.snapshot_source(name,blueprint)
        dag=api.CAUSAL.NativeExecutionDag(api.ASSEMBLER,actor.events,actor.owners,actor.scope,
            actor.inputs['actual_request_response_correlations'],actor.verified_edge,actor.raw_headers)
        api.CAUSAL.validate_globalized(dag,actor.header)
        cases=[]
        for name,blueprint in actor.header['snapshot_blueprints'].items():
            source=blueprint['native_source'];oid,n,_,held=actor.cut_refs[name]
            event=actor.events[oid,int(source['owner_ordinal'])]
            if (source['side']!='after' or event['collection']!='packets'
                    or event['record']['phase']!='prepare-materialized'): continue
            successors=[row for row in held['tables']['prepared_operations_v7']
                if row['digest']==event['record']['operation'] and row['status']=='prepared']
            if len(successors)!=1: continue
            successor=successors[0]
            for record in held['tables']['prepared_operations_v7']:
                op=record['digest']['operation_id']
                if record['status']!='committed' or op not in actor.bodies: continue
                payload=actor.bodies[op][0]
                replaced=[effect['receipt'] for effect in payload['metadata_effects']
                    if effect['type']=='put-receipt' and effect['receipt'] not in held['tables']['receipts']
                    and any(projector.exact_receipt_replacement(effect['receipt'],row)
                        for row in successor['payload']['content_receipts'] if row in held['tables']['receipts'])]
                if replaced: cases.append((n,name,blueprint,oid,held,record,payload,successor))
        self.assertTrue(cases,'requires genuine prepared receipt replacement before publication DATA')
        n,name,blueprint,oid,held,record,payload,successor=min(cases,key=lambda row:row[0])
        before=api.ASSEMBLER.native_json(api.ASSEMBLER.byte_vector(
            actor.events[oid,n]['record']['before_held_json'],'actual prepare before'),'actual prepare before')
        for table in projector.VISIBLE_TABLES:
            self.assertEqual(before['tables'].get(table,[]),held['tables'].get(table,[]))
        certificate=actor.declared_footprint(payload,record,held,oid,n,blueprint)
        witnesses=[row for row in certificate['superseded_effects'] if row['table']=='receipts']
        self.assertTrue(witnesses)
        frontier={actor.cut_refs[cut][0]:actor.cut_refs[cut][1]
                  for cut in blueprint['peer_cut_refs'].values()}
        self.assertEqual(frontier[oid],n)
        for witness in witnesses:
            self.assertEqual(actor.native(witness['successor_record_blob']),successor)
            self.assertIsNone(successor.get('committed_entry'))
            self.assertTrue(actor.receipt_observation_matches_source(
                witness['successor_receipt_observation_blob'],successor,
                witness['successor_materialization'],frontier))
            fact=actor.native(witness['successor_receipt_observation_blob'])
            for ref in fact['actual_raw_references']:
                self.assertLessEqual(int(ref['owner_ordinal']),frontier[ref['actual_mount_label']])
            saved=actor.events
            try:
                last=fact['actual_raw_references'][-1]
                actor.events={key:event for key,event in saved.items()
                    if key!=(last['actual_mount_label'],int(last['owner_ordinal']))}
                with self.assertRaises((api.Rejected,api.Unavailable)):
                    actor.declared_footprint(payload,record,held,oid,n,blueprint)
            finally: actor.events=saved
            future=dict(frontier);last=fact['actual_raw_references'][-1]
            future[last['actual_mount_label']]=int(last['owner_ordinal'])-1
            with self.assertRaises((api.Rejected,api.Unavailable)):
                actor.receipt_observation_matches_source(witness['successor_receipt_observation_blob'],
                    successor,witness['successor_materialization'],future)
        for case in ('changed-tuple','foreign-prepared-body'):
            changed=copy.deepcopy(held)
            if case=='changed-tuple':
                replacement=witnesses[0]['replacement_effect']
                changed['tables']['receipts'][changed['tables']['receipts'].index(replacement)]['content_hash']='changed-actual-data-negative'
            else:
                candidate=changed['tables']['prepared_operations_v7'][changed['tables']['prepared_operations_v7'].index(successor)]
                candidate['payload']['content_receipts'][0]['content_hash']='foreign-sealed-body-negative'
            with self.subTest(case=case),self.assertRaises((api.Rejected,api.Unavailable)):
                actor.declared_footprint(payload,record,changed,oid,n,blueprint)
        # Exercise the independent pure projector validators on these actual
        # tables, event actions and source-cut facts. No full model run occurs.
        adapter=object.__new__(projector.NativeProjectorV1)
        adapter.scope=actor.scope;adapter.owners=actor.owners
        adapter.operations=actor.header['finite_dictionary']['operation_aliases']
        adapter.payloads=actor.header['finite_dictionary']['operation_payload_aliases']
        adapter.initial_payloads={op:body[0] for op,body in actor.bodies.items()}
        snapshots={}
        for sid,bp in actor.header['snapshot_blueprints'].items():
            owner,cut,_,actual=actor.cut_refs[sid];memory=bp['native_source']
            prefix=actor.prefix(actual['certified_prefix'])
            peers={node:dict(owner_id=actor.cut_refs[ref][0],held_snapshot_id=ref)
                   for node,ref in bp['peer_cut_refs'].items()}
            facts=actor.certificate(snapshot_owner_id=owner,snapshot_native_prefix=prefix,
                visible_footprint_complete=True,actual_memory_cut_source=memory,
                actual_cut_as_of_source=dict(owner_id=owner,owner_ordinal=str(cut)),peer_certified_cuts=peers)
            snapshots[sid]=dict(owner_id=owner,scope=actor.scope,local_certified_prefix=prefix,
                materialized_through_sequence=str(actual['materialized_through_sequence']),projection_continuous=True,
                held_tables_blob=actor.blobs.json('DfsMetadataTableSnapshotTables',actual['tables']),
                native_operational_facts_blob=actor.blobs.json('DfsShardNativeOperationalFactsV1',facts))
        adapter.trace=dict(implementation_revision=actor.artifact['implementation_revision'],
            snapshots=snapshots,events=actor.header['native_timeline'])
        adapter.blobs=actor.blobs.values
        adapter.validate_atomic_rows(held['tables'],payload,certificate,record['committed_entry'],snapshots[name])
        for witness in witnesses:
            materialization=witness['successor_materialization']
            canonical=witness['successor_canonical_payload_blob']
            self.assertEqual(adapter.verified_record(successor,canonical)[2],None)
            adapter.validate_effect_materialization(materialization,successor['payload'],None,canonical)
            intent=copy.deepcopy(materialization);intent['table']='intents'
            with self.assertRaises(projector.Rejected):
                adapter.validate_effect_materialization(intent,successor['payload'],None,canonical)
            fact=actor.native(witness['successor_receipt_observation_blob'])
            last=fact['actual_raw_references'][-1];owner=last['actual_mount_label']
            changed=copy.deepcopy(snapshots[name]);_,current=adapter.held(changed)
            peer=next(value for value in current['peer_certified_cuts'].values() if value['owner_id']==owner)
            old=snapshots[peer['held_snapshot_id']]['native_operational_facts_blob']
            modified=copy.deepcopy(actor.native(old));modified['actual_cut_as_of_source']['owner_ordinal']=str(int(last['owner_ordinal'])-1)
            try:
                snapshots[peer['held_snapshot_id']]['native_operational_facts_blob']=actor.blobs.json('DfsShardNativeOperationalFactsV1',modified)
                with self.assertRaises(projector.Rejected):
                    adapter.validate_receipt_return_frontier(witness['successor_receipt_observation_blob'],changed)
            finally: snapshots[peer['held_snapshot_id']]['native_operational_facts_blob']=old


class NativeGroupedDagOrderDataTests(unittest.TestCase):
    """Real grouped DATA ordering; stops before skeleton output or model replay."""
    @classmethod
    def setUpClass(cls):
        path = os.environ.get('SINOMO_NATIVE_GROUPED_INPUTS')
        if not path:
            raise unittest.SkipTest('requires complete saved grouped native capture DATA')
        here = Path(__file__).resolve().parent
        spec = importlib.util.spec_from_file_location('grouped_data_builder',
            here / 'build-shard-native-skeleton.py')
        cls.builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.builder)
        data = json.loads(Path(path).read_text())
        cls.input_sha256 = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        # This existing control exercises only the source-bound ordering
        # prefix. Full production build additionally requires every qualified M.
        cls.capture_builder = cls.builder.FiniteCaptureBuilder(data)
        actor=cls.capture_builder
        actor.select_operations(); actor.select_not_sent_journal_attempts(); actor.select_receivers()
        actor.select_initial_cuts()
        raw={(oid,n):dict(kind=cls.builder.KINDS[event['collection']],ordinal=n,record=event['record'])
            for (oid,n),event in actor.events.items()}
        dag,timeline=actor.group_timeline(raw)
        cls.fixture=dict(dag=dag,blueprints=actor.blueprints,initial=actor.initial,
            timeline=timeline,selected=data['selected_operation_ids'])

    def test_actual_grouped_prefix_preserves_all_selected_transitions(self):
        f = self.fixture
        rows = f['timeline']
        self.assertEqual(len(f['selected']), 3)
        self.assertEqual(sum(r['action']['kind'] == 'prepare-payload' for r in rows), 3)
        self.assertEqual(sum(r['action']['kind'] == 'commit-digest' for r in rows), 3)
        # Verify the reordered prefix through the second genuine preparation.
        # Later unowned materialization remains outside this fragment claim.
        second = [r for r in rows if r['action']['kind'] == 'prepare-payload'][1]
        prefix = rows[:rows.index(second) + 1]
        vectors = self.builder.CAUSAL.global_vectors(f['dag'], f['blueprints'], f['initial'], prefix)
        self.assertEqual(len(vectors), 2 * len(prefix))

    def test_each_grouped_predecessor_is_an_actual_raw_dag_ancestor(self):
        f = self.fixture
        rows, dag = f['timeline'], f['dag']
        indexes = {r['event_id']: i for i, r in enumerate(rows)}
        sibling = False
        for row in rows:
            key = row['owner_id'], dag.number(row['event_id'].rsplit(':', 1)[1])
            horizon = dag.close({oid: key[1] if oid == key[0] else 0 for oid in dag.owners})
            peers = []
            for prior in rows:
                other = prior['owner_id'], dag.number(prior['event_id'].rsplit(':', 1)[1])
                if other != key and other[1] <= horizon[other[0]]:
                    self.assertIn(prior['event_id'], row['caused_by'])
                    self.assertLess(indexes[prior['event_id']], indexes[row['event_id']])
                    if prior['action']['kind'] == 'replica-ingest':
                        peers.append(prior['owner_id'])
            if row['action']['kind'] == 'replica-progress' and len(set(peers)) >= 2:
                sibling = True
        self.assertTrue(sibling, 'actual parallel completed calls require both target predecessors')

    def test_moving_actual_sibling_ingest_after_ack_keeps_held_before_barrier(self):
        f = self.fixture
        rows = copy.deepcopy(f['timeline'])
        dag = f['dag']
        for index, row in enumerate(rows):
            if row['action']['kind'] != 'replica-progress':
                continue
            peers = [i for i, prior in enumerate(rows[:index])
                if prior['event_id'] in row['caused_by'] and prior['action']['kind'] == 'replica-ingest']
            if len({rows[i]['owner_id'] for i in peers}) < 2:
                continue
            moved = rows.pop(peers[-1])
            after = next(i for i, current in enumerate(rows) if current['event_id'] == row['event_id'])
            rows.insert(after + 1, moved)
            with self.assertRaisesRegex(self.builder.Unavailable,
                    r'^(held-before source cannot cross its actual durable/floor transition|global as-of reuse crossed an actual metadata mutation)$'):
                self.builder.CAUSAL.global_vectors(dag, f['blueprints'], f['initial'], rows)
            return
        self.fail('actual sibling completed-call witness is missing')

    def test_dropped_prior_guard_context_cannot_cross_second_prepare(self):
        f = self.fixture
        changed = copy.deepcopy(f['blueprints'])
        rows = [row for row in f['timeline'] if row['action']['kind'] == 'prepare-payload']
        self.assertIsNotNone(changed[rows[1]['before_snapshot']]['last_guard_blob'])
        changed[rows[1]['before_snapshot']]['last_guard_blob'] = None
        with self.assertRaisesRegex(self.builder.Unavailable,
                'global before-cut crosses an unmodeled native state change'):
            self.builder.CAUSAL.global_vectors(f['dag'], changed, f['initial'], f['timeline'])

    def test_prior_selected_guard_requires_exact_actual_release(self):
        row = next(row for row in self.fixture['timeline']
            if row['action']['kind'] == 'prepare-payload'
            and self.fixture['blueprints'][row['before_snapshot']]['last_guard_blob'] is not None)
        source = self.fixture['blueprints'][row['before_snapshot']]['native_source']
        owner, before = source['owner_id'], int(source['owner_ordinal'])
        actor = copy.copy(self.capture_builder)
        actor.events = dict(actor.events)
        admitted = max((event for event in actor.events.values()
            if event['owner_id'] == owner and event['owner_ordinal'] < before
            and event['collection'] == 'guard_events' and event['record']['phase'] == 'admitted'
            and event['record']['digest']['operation_id'] in actor.selected),
            key=lambda event: event['owner_ordinal'])
        release = next(key for key, event in actor.events.items()
            if event['owner_id'] == owner and event['owner_ordinal'] < before
            and event['collection'] == 'guard_events' and event['record']['phase'] == 'released'
            and event['record']['guard_id'] == admitted['record']['guard_id'])
        actual = actor.events.pop(release)
        with self.assertRaises(self.builder.Unavailable):
            actor.prior_selected_guard(owner, before)
        changed = copy.deepcopy(actual)
        changed['record']['digest']['payload_digest'] += '-foreign'
        actor.events[release] = changed
        with self.assertRaises(self.builder.Unavailable):
            actor.prior_selected_guard(owner, before)


class NativeMaterializedIntervalDataTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  path=os.environ.get('SINOMO_NATIVE_MATERIALIZATION_CAPTURE')
  if not path:raise unittest.SkipTest('requires fresh original closed native materialization capture')
  here=Path(__file__).resolve().parent
  def load(name,file):
   spec=importlib.util.spec_from_file_location(name,here/file);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
  cls.api=load('materialization_test_api','assemble-shard-native-trace.py')
  cls.helper=load('materialization_test_helper','shard-native-causality.py')
  manifest_path=Path(path);manifest=json.loads(manifest_path.read_text());cls.fixtures=[]
  for row in manifest['captures']:
   raw_path=manifest_path.parent/row['raw_file'];raw=json.loads(raw_path.read_text())
   header=json.loads((manifest_path.parent/row['header_file']).read_text())
   if header['artifact_binding']!=manifest['artifact_binding']:
    raise cls.api.Rejected('fresh interval borrows another actual source/binary/model/schema')
   if not raw['closed'] or not raw['actual_owner_stopped'] or any(raw[k]!='0' for k in ('dropped','incomplete','in_flight')):
    raise cls.api.Unavailable('fresh interval actual capture is incomplete or live')
   cls.api.validate_capture(raw)
   owner=raw['actual_mount_label']
   events={(owner,int(record['owner_ordinal'])):dict(owner_id=owner,owner_ordinal=int(record['owner_ordinal']),collection='boundary_packets',record=record) for record in raw['boundary_packets']}
   for key,event in events.items():
    if event['record']['facts']['phase']!='replica-materialization-completed':continue
    prior=[(k,e) for k,e in events.items() if k[1]<key[1] and e['record']['facts']['phase']=='replica-journal-persisted']
    if not prior:raise cls.api.Unavailable('actual completion has no actual receiver persistence')
    rk,re=max(prior,key=lambda pair:pair[0][1]);custody=re['record']['resident_custody']
    owners={owner:dict(node_id=row['node_id'],canonical_root=custody['backend_root'])}
    scope=dict(file_system_id=re['record']['facts']['command']['file_system_id'],shard_id=re['record']['facts']['command']['batch']['shard_id'])
    interval={k:e for k,e in events.items() if rk[1]<=k[1]<=key[1]}
    cls.fixtures.append((interval,owners,scope,rk,key))
 def setUp(self):
  events,owners,scope,r,m=self.fixtures[-1];self.events=copy.deepcopy(events);self.owners=owners;self.scope=scope;self.r=r;self.m=m
 def verify(self):return self.helper.qualified_materialization(self.api,self.events,self.owners,self.scope,self.r,self.m)
 def test_every_genuine_owned_interval_qualifies_without_data_changes(self):
  self.assertEqual(len(self.fixtures),10)
  for events,owners,scope,r,m in self.fixtures:
   before=copy.deepcopy(events)
   self.assertEqual(self.helper.qualified_materialization(self.api,events,owners,scope,r,m),(r,m))
   self.assertEqual(events,before)
 def test_missing_own_completion_custody_is_unavailable(self):
  self.events[self.m]['record']['resident_custody']=None
  with self.assertRaises(self.api.Unavailable):self.verify()
 def test_foreign_actual_custody_is_rejected(self):
  original=copy.deepcopy(self.events)
  for field in ('actual_mount_label','local_node_id','backend_root','file_system_id','shard_id','resident_handle_token','data_owner_token','admission_scheduler_token'):
   self.events=copy.deepcopy(original);self.events[self.m]['record']['resident_custody'][field]+='-foreign'
   with self.subTest(field=field),self.assertRaises(self.api.Rejected):self.verify()
 def test_wrong_actual_admission_scope_is_rejected(self):
  self.events[self.m]['record']['resident_custody']['admitted_scope']['shard_id']+='-foreign'
  with self.assertRaises(self.api.Rejected):self.verify()






class NativeOriginalKvCertificateDataTests(unittest.TestCase):
    """Saved normal caller DATA sensitivity; no invented original capture PASS."""
    @classmethod
    def setUpClass(cls):
        path=os.environ.get('SINOMO_NATIVE_ORIGINAL_CALL_DATA')
        if not path: raise unittest.SkipTest('requires unchanged saved actual caller DATA')
        cls.data=json.loads(Path(path).read_text())
        if cls.data['classification']!='genuine unchanged saved caller DATA; no new original producer fact':
            raise RuntimeError('original certificate controls require actual caller DATA provenance')
        spec=importlib.util.spec_from_file_location('original_certificate_test_assembler',Path(__file__).with_name('assemble-shard-native-trace.py'))
        cls.api=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.api)
        spec=importlib.util.spec_from_file_location('original_certificate_test_bootstrap',Path(__file__).with_name('shard-native-bootstrap.py'))
        cls.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.module)
        cls.module.configure(cls.api)

    def setUp(self):
        d=self.data;self.owner=copy.deepcopy(d['owner']);self.scope=copy.deepcopy(d['scope'])
        self.header=dict(actual_mount_label=self.owner['owner_id'],actual_data_root=self.owner['actual_data_root'],
            normal_mounted_identity=self.owner['native_mount_identity'])
        self.calls={(e['owner_id'],e['owner_ordinal']):dict(kind='control-commit',ordinal=e['owner_ordinal'],record=copy.deepcopy(e['record'])) for e in d['events']}
        self.current=next(row['record'] for row in self.calls.values() if isinstance(row['record']['commit']['result']['authority']['basis'],dict))
        self.replay=next(row['record'] for row in self.calls.values() if row['record']['commit']['result']['authority']['basis']=='exact-committed-request')
        request,result=self.current['commit']['request'],self.current['commit']['result']
        # Comparison DTO only, copied from a genuine normal caller result. This
        # is NEVER inserted as a positive original capture or certificate source.
        self.candidate=dict(owner_label=self.current['owner_label'],owner_ordinal=self.current['owner_ordinal'],original=dict(
            root=str(Path(self.owner['actual_data_root'])/'cluster'),node_id=self.owner['node_id'],
            request=dict(request_id=request['request_id'],command={'Txn':copy.deepcopy(request['txn'])}),
            committed=dict(request_id=result['request_id'],command_digest=copy.deepcopy(result['command_digest']),
                term=result['authority']['term'],log_index=result['authority']['commit_index'],revision=result['authority']['revision'],
                txn_response=copy.deepcopy(result['txn_response'])),authority=copy.deepcopy(result['authority'])))
        self.voters={r['node_id'] for r in self.owner['native_mount_identity']['configured_members'] if r['role']=='voter' and r['state']=='voter'}

    def test_genuine_normal_caller_and_missing_original_replay(self):
        self.module.native_receipt(self.current,self.voters)
        key=(self.owner['owner_id'],int(self.replay['owner_ordinal']))
        with self.assertRaises(self.api.Unavailable):
            self.module.native_receipt(self.replay,self.voters,scope=self.scope,owner=self.owner,header=self.header,raw=self.calls,key=key)

    def test_whole_actual_comparison_tuple_mutations(self):
        original=self.candidate['original']
        self.assertTrue(self.module.original_matches_call(original,self.current))
        edits=[lambda f:f['request'].update(request_id=self.replay['commit']['request']['request_id']),
            lambda f:f['request']['command']['Txn']['success'].clear(),
            lambda f:f['committed'].update(request_id=self.replay['commit']['request']['request_id']),
            lambda f:f['committed']['command_digest'].__setitem__(0,f['committed']['command_digest'][0]^1),
            lambda f:f['committed']['txn_response'].update(succeeded=False),
            lambda f:f['committed']['txn_response']['responses'].clear()]
        for field in ('term','log_index','revision'):
            edits.append(lambda f,field=field:f['committed'].__setitem__(field,int(f['committed'][field])+1))
        for edit in edits:
            fact=copy.deepcopy(original);edit(fact)
            with self.subTest(fact=fact):self.assertFalse(self.module.original_matches_call(fact,self.current))

    def test_actual_root_node_owner_and_filesystem_cannot_be_borrowed(self):
        for field,value in (('root',self.candidate['original']['root']+'-foreign'),('node_id','foreign-node')):
            row=copy.deepcopy(self.candidate);row['original'][field]=value
            with self.subTest(field=field),self.assertRaises(self.api.Rejected):
                self.module.original_fact(self.scope,self.owner,self.header,row)
        row=copy.deepcopy(self.candidate);row['owner_label']+='-foreign'
        with self.assertRaises(self.api.Rejected):self.module.original_fact(self.scope,self.owner,self.header,row)
        scope=dict(self.scope,file_system_id=self.scope['file_system_id']+'-foreign')
        with self.assertRaises(self.api.Rejected):self.module.original_fact(scope,self.owner,self.header,self.candidate)

    def test_actual_quorum_count_and_type_mutations(self):
        self.module.quorum(self.candidate['original']['authority'],self.voters)
        for field,value in (('voter_acks',0),('required_quorum',len(self.voters)+1),('voter_acks',True)):
            authority=copy.deepcopy(self.candidate['original']['authority']);authority['basis']['current-term-quorum'][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(self.api.Rejected):self.module.quorum(authority,self.voters)

    def test_future_higher_prefix_and_other_real_requests_cannot_replace_missing_origin(self):
        key=(self.owner['owner_id'],int(self.replay['owner_ordinal']))
        for actual_key,item in self.calls.items():
            if not isinstance(item['record']['commit']['result']['authority']['basis'],dict):continue
            request,result=item['record']['commit']['request'],item['record']['commit']['result']
            candidate=copy.deepcopy(self.candidate);candidate['owner_ordinal']=item['record']['owner_ordinal']
            candidate['original'].update(request=dict(request_id=request['request_id'],command={'Txn':request['txn']}),
                committed=dict(request_id=result['request_id'],command_digest=result['command_digest'],term=result['authority']['term'],
                    log_index=result['authority']['commit_index'],revision=result['authority']['revision'],txn_response=result['txn_response']),
                authority=result['authority'])
            # Negative collection-tag mutation of a genuine different/future call.
            raw=dict(self.calls);raw[actual_key]=dict(kind='original-control-commit',ordinal=actual_key[1],record=candidate)
            with self.subTest(actual_key=actual_key),self.assertRaises(self.api.Unavailable):
                self.module.original_for_replay(self.scope,self.owner,self.header,raw,key,self.replay,self.voters)

    def test_original_cas_false_and_unassociated_calls_stay_unavailable(self):
        row=copy.deepcopy(self.candidate);key=(self.owner['owner_id'],int(row['owner_ordinal']))
        raw={key:dict(kind='original-control-commit',ordinal=key[1],record=row)}
        with self.assertRaises(self.api.Unavailable):self.module.verify_original_observation(self.scope,[self.owner],{key[0]:self.header},raw,key)
        row['original']['committed']['txn_response']['succeeded']=False
        with self.assertRaises(self.api.Unavailable):self.module.verify_original_observation(self.scope,[self.owner],{key[0]:self.header},raw,key)

    def test_original_certified_prefix_cannot_precede_its_actual_request(self):
        # These are negative DTO mutations of the saved real caller only. They
        # never supply a positive original producer or a later borrowed proof.
        original=self.candidate['original']
        for field,value in (('term',int(original['committed']['term'])+1),
                            ('commit_index',int(original['committed']['log_index'])-1),
                            ('revision',int(original['committed']['revision'])-1)):
            row=copy.deepcopy(self.candidate);row['original']['authority'][field]=value
            with self.subTest(field=field),self.assertRaises(self.api.Rejected):
                self.module.original_fact(self.scope,self.owner,self.header,row)

    def test_missing_fields_and_source_ordinal_mutation_are_rejected(self):
        for field in ('request','committed','authority','root','node_id'):
            row=copy.deepcopy(self.candidate);del row['original'][field]
            with self.subTest(field=field),self.assertRaises(self.api.Rejected):self.module.original_fact(self.scope,self.owner,self.header,row)
        key=(self.owner['owner_id'],int(self.replay['owner_ordinal'])+1)
        with self.assertRaises(self.api.Rejected):
            self.module.original_for_replay(self.scope,self.owner,self.header,self.calls,key,self.replay,self.voters)



class NativeOriginalKvFrontierDataTests(unittest.TestCase):
    """Negative tags on genuine saved events; never a positive capture claim."""
    @classmethod
    def setUpClass(cls):
        NativeCausalFrontierDataTests.setUpClass.__func__(cls)
    def original_event(self):
        f=self.fixture
        source=f['BLUEPRINTS'][f['ROWS'][0]['before_snapshot']]['native_source']
        pd=(source['owner_id'],int(source['owner_ordinal']))
        return next((key,event) for key,event in f['EVENTS'].items()
            if event['collection']=='control_commits' and isinstance(event['record']['commit']['result']['authority']['basis'],dict)
            and key[0]==pd[0] and key[1]<pd[1])

    def test_original_cannot_be_consumed_by_prepare_group(self):
        f=self.fixture;key,_=self.original_event();events=copy.deepcopy(f['EVENTS'])
        events[key]['collection']='original_control_commits'
        dag=f['dag'](events)
        rows=copy.deepcopy(f['ROWS']);rows[0]['raw_references'].append(dict(actual_mount_label=key[0],owner_ordinal=str(key[1])))
        with self.assertRaisesRegex(self.api.Rejected,'outside a genuine global configuration/handoff group'):
            self.helper.global_vectors(dag,f['BLUEPRINTS'],f['INITIAL'],rows)

    def test_unmatched_original_cannot_be_crossed_by_asof(self):
        f=self.fixture;key,_=self.original_event();events=copy.deepcopy(f['EVENTS'])
        prior=[(source,event) for source,event in events.items() if source[0]==key[0] and source[1]<key[1]
            and event['record'].get('after_held_json') is not None]
        self.assertTrue(prior,'saved actual origin must have an earlier genuine held source')
        source,event=max(prior,key=lambda pair:pair[0][1]);held=self.api.native_json(
            self.api.byte_vector(event['record']['after_held_json'],'actual pre-origin held'),'actual pre-origin held')
        events[key]['collection']='original_control_commits';dag=f['dag'](events)
        blueprint=dict(native_source=dict(owner_id=source[0],owner_ordinal=str(source[1]),side='after'),
            as_of_source=dict(owner_id=source[0],owner_ordinal=str(source[1]),side='after'),held=held)
        with self.assertRaisesRegex(self.api.Unavailable,'unmatched actual original KV effect'):
            dag.rebase(blueprint,key[1])

    def test_post_original_held_source_cannot_be_borrowed_as_before(self):
        f=self.fixture;key,_=self.original_event()
        blueprint=f['BLUEPRINTS'][f['ROWS'][0]['before_snapshot']]
        with self.assertRaisesRegex(self.api.Rejected,'future held source'):
            f['DAG'].rebase(blueprint,key[1]-1)

    def test_extra_original_reference_cannot_mark_effect_processed(self):
        f=self.fixture;key,_=self.original_event();events=copy.deepcopy(f['EVENTS'])
        events[key]['collection']='original_control_commits';dag=f['dag'](events)
        rows=copy.deepcopy(f['ROWS']);rows[0]['action']=dict(kind='handoff-begin')
        ref=dict(actual_mount_label=key[0],owner_ordinal=str(key[1]))
        rows[0]['raw_references'] += [ref,copy.deepcopy(ref)]
        with self.assertRaisesRegex(self.api.Rejected,'exact original/caller source set'):
            self.helper.global_vectors(dag,f['BLUEPRINTS'],f['INITIAL'],rows)

def actual_stage_receipt_data_fixture(path, api):
    data=json.loads(Path(path).read_text())
    if data['source_input_sha256']!='168b9d79648214ea9567dad2183dea07c2cb6c8b3360230b2b42840d845f8e2f':
        raise api.Rejected('Stage DATA is not the saved actual r482 source')
    for name in ('body','receipt_body'):
        body=data[name];encoded=body['canonical_payload_json_utf8'].encode()
        api.SUBTREE.canonical_prepared_payload(encoded,[],body['native_digest']['payload_digest'])
    payload=api.native_json(data['body']['canonical_payload_json_utf8'].encode(),'actual W2 body')
    metadata=api.native_json(data['receipt_body']['canonical_payload_json_utf8'].encode(),'actual receipt-only body')
    if api.SUBTREE.PROJECTOR.sealed_content_requirement(payload,data['body']['native_digest'],data['body']['canonical_payload_json_utf8'].encode())['min_commit_replicas']!=2:
        raise api.Rejected('actual saved content body is not W2')
    if api.SUBTREE.PROJECTOR.sealed_content_requirement(metadata,data['receipt_body']['native_digest'],data['receipt_body']['canonical_payload_json_utf8'].encode())['min_commit_replicas']!=0:
        raise api.Rejected('actual saved receipt-only body changed classification')
    raw={(e['owner_id'],int(e['owner_ordinal'])):dict(kind='content-receipt' if e['collection']=='content_receipts' else 'boundary',ordinal=int(e['owner_ordinal']),record=e['record']) for e in data['events']}
    prepare=data['prepare'];prepared=dict(kind='kernel',ordinal=int(prepare['owner_ordinal']),record=prepare['record'])
    return data,payload,metadata,raw,prepared


class NativeStageReceiptSourceDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=os.environ.get('SINOMO_NATIVE_STAGE_RECEIPT_DATA')
        if not path:raise unittest.SkipTest('actual saved Stage receipt DATA is required')
        here=Path(__file__).resolve().parent
        cls.api=importlib.util.module_from_spec(spec:=importlib.util.spec_from_file_location('actual_stage_assembler',here/'assemble-shard-native-trace.py'))
        spec.loader.exec_module(cls.api)
        cls.fixture=actual_stage_receipt_data_fixture(path,cls.api)

    def setUp(self):
        self.data,self.payload,self.metadata,self.raw,self.prepare=copy.deepcopy(self.fixture)
        self.header=dict(scope=self.data['scope'],owners=self.data['owners'],envelope_binding=self.data['artifact_binding'])
        self.owners={o['component_incarnation']:o for o in self.data['owners']}
        self.blobs=self.api.Blobs({})

    def match(self):
        return self.api.derive_remote_content_return(self.header,self.raw,self.blobs,self.owners,self.prepare,self.payload['prepared_intent'],self.data['row'])

    def key(self,phase):
        return next(k for k,r in self.raw.items() if r['kind']=='boundary' and r['record']['facts']['phase']==phase)

    def adapter(self):
        adapter=object.__new__(projector.NativeProjectorV1)
        adapter.scope=self.data['scope'];adapter.owners={o['owner_id']:o for o in self.data['owners']}
        adapter.trace=dict(implementation_revision=self.data['artifact_binding']['implementation_revision'])
        adapter.blobs=self.blobs.values;adapter.initial_payloads={self.payload['operation_id']:self.payload}
        return adapter

    def validate(self,fact):
        ref=self.blobs.json('DfsNativeContentReceiptObservationV1',fact)
        self.adapter().validate_initial_content_return(self.data['row'],dict(initial_content_receipt_observation_blobs=[ref]))

    def test_actual_start_return_receiver_and_coordinator_are_all_grouped(self):
        fact,used=self.match();self.assertIsNotNone(fact)
        refs=fact['actual_raw_references'];self.assertEqual(len(refs),5)
        self.assertEqual({(r['actual_mount_label'],int(r['owner_ordinal'])) for r in refs},used)
        nested=self.api.native_json(self.blobs.values[fact['actual_rpc_observation_blob']]['data'].encode(),'actual RPC')
        self.assertEqual(nested['actual_raw_references'],refs[2:4]);self.validate(fact)

    def test_actual_receipt_only_and_full_body_preserve_whole_rows(self):
        row=self.data['row'];old=next(e['receipt'] for e in self.metadata['metadata_effects'] if e['type']=='put-receipt')
        for field in ('file_system_id','object_id','committed_version','owner_node_id'):
            self.assertEqual(old['receipt'][field],row['receipt'][field])
        for field in ('replica_node_id','content_hash'):self.assertEqual(old[field],row[field])
        old_fields=copy.deepcopy(old);new_fields=copy.deepcopy(row)
        for value in (old_fields,new_fields):value['receipt'].pop('event_id');value.pop('received_at')
        self.assertEqual(old_fields,new_fields);self.assertNotEqual(old,row)
        self.assertLess(int(self.key('rpc-attempt-returned')[1]),int(self.data['receipt_body']['owner_ordinal']))

    def test_missing_source_start_rejected(self):
        del self.raw[self.key('rpc-attempt-started')]
        with self.assertRaisesRegex(self.api.Rejected,'unique source attempt'):self.match()

    def test_missing_source_return_unavailable(self):
        del self.raw[self.key('rpc-attempt-returned')];self.assertIsNone(self.match()[0])

    def test_missing_receiver_persistence_unavailable(self):
        key=next(k for k,r in self.raw.items() if r['kind']=='content-receipt' and r['record']['facts']['origin']=='local-persisted')
        del self.raw[key];self.assertIsNone(self.match()[0])

    def test_wrong_actual_target_unavailable(self):
        self.raw[self.key('rpc-attempt-started')]['record']['facts']['target_node_id']=next(o['node_id'] for o in self.data['owners'] if o['node_id'] not in (self.payload['prepared_intent']['coordinator_node_id'],self.data['row']['replica_node_id']))
        self.assertIsNone(self.match()[0])

    def test_wrong_actual_response_hash_unavailable(self):
        response=self.raw[self.key('rpc-attempt-returned')]['record']['facts']['response']
        response['outbound_messages'][0]['message']['stage-blob-response']['result']['durable']['content_hash']='blake3:'+'0'*64
        self.assertIsNone(self.match()[0])

    def test_wrong_receiver_filesystem_unavailable(self):
        key=next(k for k,r in self.raw.items() if r['kind']=='content-receipt' and r['record']['facts']['origin']=='local-persisted')
        self.raw[key]['record']['facts']['file_system_id']='foreign-fs';self.assertIsNone(self.match()[0])

    def test_changed_immutable_intent_rejected(self):
        self.payload['prepared_intent']['owner_epoch']+=1
        with self.assertRaisesRegex(self.api.Rejected,'immutable placement/intent'):self.match()

    def test_omitted_outer_start_unavailable(self):
        fact,_=self.match();del fact['actual_raw_references'][2]
        with self.assertRaisesRegex(projector.Unavailable,'source RPC start/return'):self.validate(fact)

    def test_nested_pair_cannot_borrow_another_source_cut(self):
        fact,_=self.match();nested=self.api.native_json(self.blobs.values[fact['actual_rpc_observation_blob']]['data'].encode(),'actual RPC')
        nested['actual_raw_references'][0]['owner_ordinal']='205'
        fact['actual_rpc_observation_blob']=self.blobs.json('DfsNativeAuthenticatedRpcReturnV1',nested)
        with self.assertRaisesRegex(projector.Rejected,'causal provenance'):self.validate(fact)

    def test_after_return_cannot_claim_a_before_start(self):
        fact,_=self.match();fact['actual_raw_references'][2:4]=list(reversed(fact['actual_raw_references'][2:4]))
        nested=self.api.native_json(self.blobs.values[fact['actual_rpc_observation_blob']]['data'].encode(),'actual RPC')
        nested['actual_raw_references']=copy.deepcopy(fact['actual_raw_references'][2:4])
        fact['actual_rpc_observation_blob']=self.blobs.json('DfsNativeAuthenticatedRpcReturnV1',nested)
        with self.assertRaisesRegex(projector.Rejected,'causal provenance'):self.validate(fact)

    def test_foreign_actual_root_certificate_rejected(self):
        fact,_=self.match();fact=copy.deepcopy(fact);fact['owner_bindings'][0]['canonical_root']+='-foreign'
        with self.assertRaisesRegex(projector.Rejected,'root/incarnation ownership'):self.validate(fact)


class NativeCommittedReplicationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=os.environ.get('SINOMO_NATIVE_COMMITTED_REPLICATION_DATA')
        if not path:raise unittest.SkipTest('actual saved committed replication DATA is required')
        cls.data=json.loads(Path(path).read_text())
        digest=hashlib.sha256()
        with Path(cls.data['input']).open('rb') as source:
            for block in iter(lambda:source.read(1024*1024),b''):digest.update(block)
        if digest.hexdigest()!=cls.data['input_sha256']:raise RuntimeError('original actual input changed')
        here=Path(__file__).resolve().parent
        cls.api=importlib.util.module_from_spec(spec:=importlib.util.spec_from_file_location('actual_commit_assembler',here/'assemble-shard-native-trace.py'))
        spec.loader.exec_module(cls.api)
        cls.causal=importlib.util.module_from_spec(spec:=importlib.util.spec_from_file_location('actual_commit_causality',here/'shard-native-causality.py'))
        spec.loader.exec_module(cls.causal)

    def setUp(self):
        kinds={'packets':'kernel','guard_events':'guard','boundary_packets':'boundary'}
        self.raw={(e['owner_id'],e['owner_ordinal']):dict(kind=kinds[e['collection']],record=copy.deepcopy(e['record'])) for e in self.data['events']}
        self.headers=copy.deepcopy(self.data['headers']);self.scope=self.data['scope']
        self.owners={row['owner_id']:dict(row,canonical_root=json.loads(bytes(next(e['record']['after_held_json'] for e in self.data['events']
            if e['owner_id']==row['owner_id'] and e['record'].get('after_held_json'))))['backend_root']) for row in self.data['owners']}
        edge=self.data['correlations'][0];self.receiver=edge['receiver_owner_id'],edge['receiver_ordinal'];self.source=edge['source_owner_id'],edge['source_ordinal']
        self.original=self.api.verify_committed_replication(self.raw,self.headers,self.owners,self.scope,set(self.data['selected_operation_ids']),self.receiver,self.source)

    def verify(self,**kwargs):
        return self.api.verify_committed_replication(self.raw,self.headers,self.owners,self.scope,set(self.data['selected_operation_ids']),self.receiver,self.source,**kwargs)

    def phase(self,phase):
        return next(key for key,row in self.raw.items() if key[0]==self.source[0] and row['record'].get('phase',row['record'].get('facts',{}).get('phase'))==phase)

    def refs(self):return [dict(actual_mount_label=key[0],owner_ordinal=str(key[1])) for key in self.original[2]]

    def test_both_actual_terminal_calls_have_their_whole_five_sources(self):
        self.assertEqual(len(self.data['correlations']),2)
        for edge in self.data['correlations']:
            receiver=edge['receiver_owner_id'],edge['receiver_ordinal'];source=edge['source_owner_id'],edge['source_ordinal']
            op,cm,refs=self.api.verify_committed_replication(self.raw,self.headers,self.owners,self.scope,set(self.data['selected_operation_ids']),receiver,source)
            self.assertEqual(op,self.data['selected_operation_ids'][-1]);self.assertEqual(cm[1],236)
            self.assertEqual(len(set(refs)),5);self.assertIn(refs[1][1],(241,242))
            self.assertEqual(self.api.verify_committed_replication(self.raw,self.headers,self.owners,self.scope,set(self.data['selected_operation_ids']),receiver,source,
                group_refs=[dict(actual_mount_label=k[0],owner_ordinal=str(k[1])) for k in refs],group_owner=receiver[0])[2],refs)

    def test_actual_same_ingest_materialization_requires_its_own_custody(self):
        events={key:dict(owner_id=key[0],owner_ordinal=key[1],collection='boundary_packets' if row['kind']=='boundary' else 'packets' if row['kind']=='kernel' else 'guard_events',record=row['record']) for key,row in self.raw.items()}
        completed=min(key for key,row in self.raw.items() if key[0]==self.receiver[0] and key[1]>self.receiver[1]
            and row['record'].get('facts',{}).get('phase')=='replica-materialization-completed')
        self.assertEqual(self.causal.qualified_materialization(self.api,events,self.owners,self.scope,self.receiver,completed),(self.receiver,completed))
        events[completed]['record']['resident_custody']=None
        with self.assertRaises(self.api.Unavailable):self.causal.qualified_materialization(self.api,events,self.owners,self.scope,self.receiver,completed)

    def test_missing_original_commit_materialization_is_unavailable(self):
        del self.raw[self.phase('commit-materialized')]
        with self.assertRaises(self.api.Unavailable):self.verify()

    def test_future_original_commit_cannot_be_borrowed(self):
        key=self.phase('commit-materialized');row=self.raw.pop(key);late=self.source[1]+1
        row['record']['owner_ordinal']=str(late);self.raw[key[0],late]=row
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_missing_normal_receiver_decoder_is_unavailable(self):
        self.headers[self.receiver[0]]['actual_received_frame_decodes']=[]
        with self.assertRaises(self.api.Unavailable):self.verify()

    def test_changed_original_frame_rejected(self):
        self.raw[self.phase('commit-durable')]['record']['exact_frame'][0]^=1
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_changed_terminal_digest_rejected(self):
        self.headers[self.receiver[0]]['actual_received_frame_decodes'][0]['decoded_entry']['mutation']['digest']['payload_digest']='foreign-terminal-digest'
        with self.assertRaises((self.api.Rejected,self.api.Unavailable)):self.verify()

    def test_changed_sealed_original_body_rejected(self):
        key=self.phase('commit-materialized');record=self.raw[key]['record'];held=json.loads(bytes(record['after_held_json']))
        row=next(row for row in held['tables']['prepared_operations_v7'] if row['digest']['operation_id']==self.original[0])
        row['payload']['prepared_intent']['content_hash']='blake3:'+'0'*64
        record['after_held_json']=list(json.dumps(held,separators=(',',':')).encode())
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_foreign_receiver_root_rejected(self):
        self.owners[self.receiver[0]]['canonical_root']+='-foreign'
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_wrong_source_epoch_permit_rejected(self):
        self.raw[self.receiver]['record']['facts']['permit']['source_epoch']+=1
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_wrong_whole_configuration_rejected(self):
        self.raw[self.receiver]['record']['facts']['actual_source_config']['epoch']+=1
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_wrong_confirmation_rejected(self):
        self.raw[self.source]['record']['facts']['expected_confirmation_id']='foreign-confirmation'
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_changed_actual_return_rejected(self):
        self.raw[self.original[2][2]]['record']['facts']['response']['verified_authority_probe_id']='foreign-confirmation'
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_missing_actual_return_is_unavailable(self):
        del self.raw[self.original[2][2]]
        with self.assertRaises(self.api.Unavailable):self.verify()

    def test_missing_actual_constructed_reply_is_unavailable(self):
        del self.raw[self.original[2][4]]
        with self.assertRaises(self.api.Unavailable):self.verify()

    def test_omitted_table_row_rejected(self):
        record=self.raw[self.receiver]['record'];held=json.loads(bytes(record['after_held_json']))
        held['tables']['prepared_operations_v7'].pop();record['after_held_json']=list(json.dumps(held,separators=(',',':')).encode())
        with self.assertRaises(self.api.Rejected):self.verify()

    def test_missing_grouped_start_rejected(self):
        refs=self.refs();del refs[1]
        with self.assertRaises(self.api.Rejected):self.verify(group_refs=refs,group_owner=self.receiver[0])

    def test_extra_grouped_other_actual_source_rejected(self):
        refs=self.refs();key=self.phase('commit-materialized');refs.append(dict(actual_mount_label=key[0],owner_ordinal=str(key[1])))
        with self.assertRaises(self.api.Rejected):self.verify(group_refs=refs,group_owner=self.receiver[0])

    def grouped_source(self,refs):
        return dict(owner_id=self.receiver[0],event_id='actual-ingest:'+str(self.receiver[1]),raw_references=refs)

    def test_assembler_classifies_the_actual_original_ingest(self):
        self.assertEqual(self.api.committed_ingest_group_sources(self.raw,self.grouped_source(self.refs())),(self.receiver,self.source))

    def test_assembler_extra_persisted_boundary_cannot_bypass_exact_group(self):
        other=next(key for key,row in self.raw.items() if key!=self.receiver and row['record'].get('facts',{}).get('phase')=='replica-journal-persisted')
        refs=self.refs()+[dict(actual_mount_label=other[0],owner_ordinal=str(other[1]))]
        with self.assertRaises(self.api.Rejected):self.api.committed_ingest_group_sources(self.raw,self.grouped_source(refs))

    def test_assembler_missing_original_ingest_cannot_bypass_exact_group(self):
        with self.assertRaises(self.api.Rejected):self.api.committed_ingest_group_sources(self.raw,self.grouped_source(self.refs()[1:]))

    def test_materialization_group_cannot_hide_original_durable_ingest(self):
        completed=min(key for key,row in self.raw.items() if key[0]==self.receiver[0] and key[1]>self.receiver[1]
            and row['record'].get('facts',{}).get('phase')=='replica-materialization-completed')
        source=dict(owner_id=completed[0],event_id='actual-materialization:'+str(completed[1]),raw_references=[
            dict(actual_mount_label=key[0],owner_ordinal=str(key[1])) for key in (completed,self.receiver)])
        with self.assertRaises(self.api.Rejected):self.api.committed_ingest_group_sources(self.raw,source)

    def test_actual_constructed_anchor_cannot_hide_committed_ingest(self):
        source=self.grouped_source(self.refs());source['event_id']='actual-constructed:'+str(self.original[2][4][1])
        with self.assertRaises(self.api.Rejected):self.api.committed_ingest_group_sources(self.raw,source)

    def test_actual_constructed_anchor_with_extra_ingest_rejected(self):
        source=self.grouped_source(self.refs());source['event_id']='actual-constructed:'+str(self.original[2][4][1])
        other=next(key for key,row in self.raw.items() if key!=self.receiver and row['record'].get('facts',{}).get('phase')=='replica-journal-persisted')
        source['raw_references'].append(dict(actual_mount_label=other[0],owner_ordinal=str(other[1])))
        with self.assertRaises(self.api.Rejected):self.api.committed_ingest_group_sources(self.raw,source)

    def test_foreign_group_owner_rejected(self):
        with self.assertRaises(self.api.Rejected):self.verify(group_refs=self.refs(),group_owner=self.source[0])




class RetainedConfigurationContractTests(unittest.TestCase):
    """Source-shaped DTO sensitivity; this is not a native execution fixture."""
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).with_name('assemble-shard-native-trace.py')
        spec = importlib.util.spec_from_file_location('retained_configuration_sensitivity', path)
        cls.api = importlib.util.module_from_spec(spec); spec.loader.exec_module(cls.api)

    def setUp(self):
        self.scope = dict(file_system_id='fs', shard_id='services')
        self.config = dict(file_system_id='fs', shard_id='services', primary_node_id='n1',
                           replica_node_ids=['n2'], epoch=3, assignment={}, default_policy={})
        self.entry = dict(key='/dfs/filesystems/fs/metadata-shards/services', value=[7, 8],
                          create_revision=3, modify_revision=7)
        floor = dict(key='/cluster/raft/config', value=[1, 2], create_revision=1, modify_revision=2)
        proof = dict(term=2, commit_index=4, revision=5,
                     basis={'current-term-quorum': dict(required_quorum=1, voter_acks=1)})
        self.cut = dict(owner_label='owner1', owner_ordinal='12', guard_context_id='guard1',
            requested_scope_prefixes=['/dfs/filesystems/', '/dfs/services/content/', '/cluster/services/'],
            revision='7', accepted_authority_generation='4', scopes=[[self.entry], [], []],
            authority=[copy.deepcopy(proof) for _ in range(3)],
            retained_projection_floor=dict(value=floor, revision=5, observed_term=2, applied_index=4))
        self.header = dict(actual_mount_label='owner1', normal_mounted_identity=dict(configured_members=[
            dict(node_id='n1', role='voter', state='voter')]), actual_control_decodes=[
            dict(source_owner_ordinal='12', exact_entry=self.entry,
                 native_type='DfsMetadataShardConfig', decoded_value=self.config)])

    def validate(self, cut=None, header=None):
        return self.api.validate_retained_configuration_cut(cut or self.cut, header or self.header,
            self.scope, guard_id='guard1', expected=self.config)

    def test_original_authority_stays_at_its_revision_after_accepted_replay(self):
        before = copy.deepcopy(self.cut)
        result = self.validate()
        self.assertEqual(result['cached_revision'], '7')
        self.assertEqual([proof['revision'] for proof in result['original_authority']], [5, 5, 5])
        self.assertEqual(self.cut, before)
        self.assertEqual(result['selected_keys'], [self.entry['key'],
            '/dfs/filesystems/fs/metadata-membership-handoffs/services'])
        self.assertEqual(result['decoded'], [self.config, None])

    def test_guard_owner_scope_floor_and_quorum_mutations_fail_closed(self):
        changes = [lambda x: x.update(owner_label='foreign'),
            lambda x: x.update(unrecognized_permit='retired'),
            lambda x: x.update(guard_context_id='another'),
            lambda x: x.pop('guard_context_id'),
            lambda x: x['requested_scope_prefixes'].pop(),
            lambda x: x['scopes'].pop(), lambda x: x['authority'].pop(),
            lambda x: x['authority'][0].update(term=3),
            lambda x: x['authority'][0].update(basis='exact-committed-request'),
            lambda x: x['authority'][0]['basis']['current-term-quorum'].update(voter_acks=0),
            lambda x: x['retained_projection_floor'].update(value=None),
            lambda x: x['retained_projection_floor']['value'].update(key='/foreign/config'),
            lambda x: x['scopes'][0][0].update(modify_revision=8),
            lambda x: x['scopes'][0].append(copy.deepcopy(x['scopes'][0][0]))]
        for change in changes:
            cut = copy.deepcopy(self.cut); change(cut)
            with self.subTest(change=change), self.assertRaises((self.api.Rejected, self.api.Unavailable)):
                self.validate(cut)

    def test_same_decoder_ordinal_full_entry_type_and_config_required(self):
        for field, value in [('source_owner_ordinal', '11'), ('native_type', 'DfsObject'),
                             ('exact_entry', dict(self.entry, value=[9]))]:
            header = copy.deepcopy(self.header); header['actual_control_decodes'][0][field] = value
            with self.subTest(field=field), self.assertRaises((self.api.Rejected, self.api.Unavailable)):
                self.validate(header=header)
        header = copy.deepcopy(self.header)
        header['actual_control_decodes'][0]['decoded_value']['epoch'] += 1
        with self.assertRaises(self.api.Rejected): self.validate(header=header)

    def test_missing_handoff_is_an_observed_complete_scope_absence(self):
        cut = copy.deepcopy(self.cut); header = copy.deepcopy(self.header)
        row = dict(self.entry, key='/dfs/filesystems/fs/metadata-membership-handoffs/services')
        cut['scopes'][0].append(row); cut['scopes'][0].sort(key=lambda entry: entry['key'])
        header['actual_control_decodes'].append(dict(source_owner_ordinal='12', exact_entry=row,
            native_type='DfsMetadataShardMembershipHandoff', decoded_value={'handoff_id': 'pending'}))
        with self.assertRaises(self.api.Rejected): self.validate(cut, header)

    def test_retired_fallback_schema_and_old_guard_read_are_incompatible(self):
        schema = checker.read_json(checker.SCHEMA_PATH)
        self.assertNotIn('operation_floor_blob', schema['$defs']['ShardSnapshot']['properties'])
        self.assertNotIn('raft_head_watermark_blob', schema['$defs']['ShardSnapshot']['properties'])
        with self.assertRaises(checker.Rejected):
            checker.validate_schema({'kind': 'fallback-permit'}, schema['$defs']['NativeActionV1'], schema)
        guard = next(action for action in schema['$defs']['NativeActionV1']['oneOf']
                     if action.get('properties', {}).get('kind', {}).get('const') == 'publication-guard-admitted')
        self.assertIn('retained_configuration_cut', guard['required'])
        self.assertNotIn('authoritative_exact_read', guard['properties'])

    def test_full_assigned_guard_rejects_partial_even_for_one_native_voter(self):
        adapter = object.__new__(projector.NativeProjectorV1)
        adapter.profile = dict(standalone=False)
        fact = dict(digest={'operation_id': 'op'}, expected_config={'config': True},
            authorization_mode='full-shard', authenticated_prepare_ack_node_ids=['n1'],
            actual_writer_lock_retained=True, raw_scoped_fence_retained=True,
            execution_and_data_owner_retained=True, retained_config_and_missing_handoff_qualified=True,
            admission_pending_handoff=False, admission_write_frozen=False)
        adapter.blob = lambda ref, kind: fact
        adapter.require_certificate = lambda fact, label: None
        adapter.config = lambda value: (1, self.config)
        adapter.op = lambda op: 1
        adapter.node = lambda node: {'n1': 1, 'n2': 2}[node]
        adapter.node_set = lambda nodes: projector.finite_set(adapter.node(node) for node in nodes)
        with self.assertRaises(projector.Rejected): adapter.guard('fact', None)
        fact['authenticated_prepare_ack_node_ids'] = ['n1', 'n2']
        result = adapter.guard('fact', None)
        self.assertEqual(result['path'], 1); self.assertEqual(result['acks'], {'$set': [1, 2]})
        fact['authorization_mode'] = 'local'
        with self.assertRaises(projector.Unavailable): adapter.guard('fact', None)
        fact['authorization_mode'] = 'recovery'
        with self.assertRaises(projector.Unavailable): adapter.guard('fact', None)


if __name__ == '__main__':
    unittest.main()
