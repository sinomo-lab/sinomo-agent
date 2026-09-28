#!/usr/bin/env python3
"""Behavioral tests for attribution, censoring, reset detection and cleanup."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import argparse
import subprocess


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = load('report-performance')
capture = load('capture-performance')


def context(span, stage):
    return {"span_id": span, "name": stage, "target": "sinomo.perf", "fields": {"stage": stage}}


def record(sequence, span, stage, close, duration=1000000, ancestors=(), **fields):
    return {"sequence": sequence, "capture_id": "c", "spans": list(ancestors) + [context(span, stage)],
            "fields": {"record_kind": "span_closed" if close else "span_started", "span_id": span,
                       "stage": stage, "duration_ns": duration, "started_monotonic_ns": 1,
                       "capture_complete": True, **fields}}


def process(at, read_bytes, pid=1):
    return {"node_id": "n", "process_role": "main", "process_id": pid, "started_at": str(pid),
            "observed_monotonic_ns": at, "io": {"read_bytes": read_bytes}}


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / 'n').mkdir()
        (self.root / 'manifest.json').write_text(json.dumps({"nodes": ["n"], "capture_id": "c", "workload_exit_code": 0}))

    def tearDown(self):
        self.tmp.cleanup()

    def snapshot(self, index, records, at=10, read_bytes=100, pid=1, **extra):
        (self.root / 'n' / f'traces-{index}.json').write_text(json.dumps({"process": process(at, read_bytes, pid), "records": records, **extra}))

    def test_repeated_snapshots_do_not_double_count_child_io(self):
        records = [record(1, 1, 'write', False), record(2, 2, 'storage', False, ancestors=[context(1, 'write')]),
                   record(3, 2, 'storage', True, ancestors=[context(1, 'write')], io_kind='disk.write', io_bytes=16, outcome='ok'),
                   record(4, 1, 'write', True, duration=3000000)]
        self.snapshot(1, records[:3]); self.snapshot(2, records, at=100, read_bytes=104)
        result = report.summarize(self.root)
        self.assertTrue(result['complete'], result['issues'])
        stages = {r['stage']: r for r in result['stages']}
        self.assertEqual(stages['write']['io_inclusive']['disk.write.bytes'], 16)
        self.assertEqual(stages['write']['io_direct'], {})
        self.assertEqual(stages['storage/disk.write']['io_direct']['disk.write.bytes'], 16)
        self.assertEqual(stages['write']['p95_ms'], 3)
        self.assertEqual(result['processes'][0]['io_delta']['read_bytes'], 4)
        self.assertEqual(result['coverage'][0]['records'], 4)

    def test_observation_rpc_is_not_an_unfinished_workload(self):
        observer = record(3, 99, 'network.server', False)
        marked = context(100, 'network.rpc'); marked['fields']['observation'] = True
        marker = {"sequence": 4, "capture_id": "c", "spans": [context(99, 'network.server'), marked], "fields": {"observation": True}}
        records = [record(1, 1, 'work', False), record(2, 1, 'work', True), observer, marker]
        self.snapshot(1, records); self.snapshot(2, records, at=200)
        result = report.summarize(self.root)
        self.assertTrue(result['complete'], result['issues'])
        self.assertEqual(result['coverage'][0]['observer_records_excluded'], 2)
        self.assertEqual(len(result['stages']), 1)

    def test_polling_can_preserve_records_later_evicted_from_ring(self):
        self.snapshot(1, [record(1, 1, 'work', False)])
        self.snapshot(2, [record(2, 1, 'work', True)], at=100, dropped_records=1, truncated=True)
        result = report.summarize(self.root)
        self.assertTrue(result['complete'], result['issues'])
        self.assertEqual(result['coverage'][0]['ring_evictions'], 1)
        self.assertEqual(result['coverage'][0]['complete_spans'], 1)

    def test_open_receiver_is_joined_and_reported_as_censored(self):
        manifest = {"nodes": ["n", "b"], "capture_id": "c", "workload_exit_code": 0}
        (self.root / 'manifest.json').write_text(json.dumps(manifest))
        client = [record(1, 1, 'write', False), record(2, 2, 'network.service_rpc', False, ancestors=[context(1, 'write')]),
                  record(3, 2, 'network.service_rpc', True, ancestors=[context(1, 'write')]), record(4, 1, 'write', True)]
        for row in client[1:3]: row['spans'][-1]['fields']['rpc_request_id'] = 'rpc'
        self.snapshot(0, [], at=1); self.snapshot(1, client, at=1000)
        (self.root / 'b').mkdir()
        receiver = record(1, 10, 'network.server', False)
        parent = context(10, 'network.server'); parent['fields']['rpc_request_id'] = 'rpc'
        waiting = record(2, 11, 'dfs.admission', False, ancestors=[parent], phase='initial-scheduler')
        for index, rows, at in [(0, [], 1), (1, [receiver, waiting], 1000)]:
            proc = process(at, 0); proc['node_id'] = 'b'
            (self.root / 'b' / f'traces-{index}.json').write_text(json.dumps({'process': proc, 'records': rows}))
        result = report.operation_view(report.summarize(self.root), 'write', 1)
        self.assertEqual(result['joined_rpcs'], 1)
        self.assertFalse(any('unmatched' in issue for issue in result['issues']))
        self.assertEqual(len(result['censored_workload_spans']), 2)
        self.assertTrue(all(row['elapsed_lower_bound_ms'] == .000999 for row in result['censored_workload_spans']))

    def test_censored_and_missing_nodes_stay_visible(self):
        self.snapshot(1, [record(1, 1, 'write', False), record(2, 1, 'write', True, capture_complete=False)])
        manifest = json.loads((self.root / 'manifest.json').read_text()); manifest['nodes'].append('missing')
        (self.root / 'manifest.json').write_text(json.dumps(manifest))
        result = report.summarize(self.root)
        self.assertFalse(result['complete']); self.assertEqual(result['stages'], [])
        self.assertEqual(len(result['coverage']), 2)
        self.assertTrue(any('missing: node not observed' in x for x in result['issues']))

    def test_restarts_and_counter_resets_are_not_traffic(self):
        self.snapshot(1, [], read_bytes=100, pid=1)
        self.snapshot(2, [], at=200, read_bytes=5, pid=2)
        result = report.summarize(self.root)
        self.assertEqual(len(result['processes']), 2)
        self.assertTrue(all(r['io_delta']['read_bytes'] is None for r in result['processes']))
        self.assertIsNone(report.counter_delta(100, 5))

    def test_quantiles_are_from_samples_and_absence_is_not_zero(self):
        self.assertEqual(report.percentile([1, 2, 3, 100], .95), 100)
        self.assertEqual(report.io_value({}, 'disk.read', 'bytes'), 'n/o')
        self.assertEqual(report.io_value({'disk.read.ops': 1, 'disk.read.bytes': 0}, 'disk.read', 'bytes'), '0')

    def test_truncation_and_unfinished_work_are_reported(self):
        self.snapshot(1, [record(1, 1, 'write', False)], truncated=True, dropped_records=3)
        result = report.summarize(self.root)
        self.assertFalse(result['complete'])
        self.assertIn('n: unfinished span(s)', result['issues'])
        self.assertIn('n: trace ring/response truncated', result['issues'])


class OperationViewTests(unittest.TestCase):
    def test_cross_node_rpc_join_excludes_background_and_preserves_node_io(self):
        def span(node, sid, stage, ancestors, rpc=None, io=None):
            return {"node": node, "process": [node, "main", 1, node], "span_id": sid,
                    "stage": stage, "ancestors": ancestors, "rpc_request_id": rpc,
                    "duration_ns": 1000000, "io_direct": io or {}, "io_inclusive": io or {},
                    "outcome": "ok", "peers": []}
        spans = [span('a', 1, 'network.server', [], 'outer'),
                 span('a', 2, 'dfs.browser.write', [1]),
                 span('a', 3, 'network.service_rpc', [1, 2], 'inner'),
                 span('b', 4, 'network.server', [], 'inner'),
                 span('b', 5, 'storage.io/disk.write', [4], io={'disk.write.bytes': 32, 'disk.write.ops': 1}),
                 span('b', 6, 'background', [])]
        source = {'spans': spans, 'coverage': [], 'issues': [], 'complete': True, 'stages': []}
        selected = report.operation_view(source, 'dfs.browser.write', 1)
        self.assertTrue(selected['complete'], selected['issues'])
        self.assertEqual(selected['joined_rpcs'], 1)
        self.assertNotIn('background', [s['stage'] for s in selected['spans']])
        io = next(s for s in selected['stages'] if s['stage'] == 'storage.io/disk.write')
        self.assertEqual(io['node'], 'b'); self.assertEqual(io['io_direct']['disk.write.bytes'], 32)
        source['spans'] = spans[:3]
        missing = report.operation_view(source, 'dfs.browser.write', 1)
        self.assertFalse(missing['complete'])
        self.assertTrue(any('unmatched' in issue for issue in missing['issues']))

    def test_expected_roots_and_censored_descendant_fail_coverage(self):
        process = ['a', 'main', 1, 'start']
        root = {'node': 'a', 'process': process, 'span_id': 1, 'stage': 'write', 'ancestors': [],
                'duration_ns': 100, 'io_direct': {}, 'io_inclusive': {}, 'outcome': 'unknown', 'peers': []}
        source = {'spans': [root], 'coverage': [{'censored_spans': [{'process': process, 'span_id': 2, 'ancestors': [1]}]}],
                  'issues': ['a: unfinished span(s)'], 'complete': False}
        selected = report.operation_view(source, 'write', 2)
        self.assertFalse(selected['complete'])
        self.assertTrue(any('expected 2' in issue for issue in selected['issues']))
        self.assertTrue(any('incomplete spans' in issue for issue in selected['issues']))


class CaptureCleanupTests(unittest.TestCase):
    def test_buffered_capture_stops_before_export_and_excludes_export_io(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(output=Path(tmp) / 'capture', nodes=['n'], client_command=['sinomo'], clients={},
                                      workload=['true'], source_revision='test', binary_sha256='test', profile='test',
                                      poll_seconds=.1, rpc_timeout=1, duration_seconds=1, buffered=True, minimum_capacity=32768)
            state = {'owner': None, 'capture': None, 'snapshots': 0}; calls = []
            def run(command, **kwargs):
                calls.append(command)
                if 'control' in command:
                    if '--capture' in command:
                        state['owner'] = state['capture'] = command[command.index('--capture')+1]
                    if '--clear-capture' in command:
                        state['owner'] = None
                    body = {'active_capture_id': state['owner'], 'base_filter': 'warn', 'active_filter': 'warn'}
                else:
                    counter = '--target-prefix' in command
                    if counter:
                        state['snapshots'] += 1
                        sample = process(state['snapshots']*10, 100 if state['snapshots']==1 else 104)
                        records = []
                    else:
                        self.assertIsNone(state['owner'], 'export must happen after stop')
                        sample = process(1000, 999999)
                        records = [record(11, 1, 'write', False), record(12, 1, 'write', True)]
                        for row in records: row['capture_id'] = state['capture']
                    body = {'process': sample, 'records': records, 'capacity': 32768,
                            'last_sequence': 10 if state['snapshots']==1 else 20,
                            'capture_active': bool(state['owner']), 'truncated': False, 'dropped_records': 0}
                return subprocess.CompletedProcess(command, 0, json.dumps(body), '')
            child = mock.Mock(); child.wait.return_value = 0; child.poll.return_value = 0
            with mock.patch.object(capture.subprocess, 'run', side_effect=run), mock.patch.object(capture.subprocess, 'Popen', return_value=child):
                self.assertEqual(capture.run_capture(args), 0)
            result = report.summarize(args.output)
            self.assertTrue(result['complete'], result['issues'])
            self.assertEqual(result['processes'][0]['io_delta']['read_bytes'], 4)
            self.assertEqual(result['processes'][0]['window_ms'], .00001)

    def test_timed_out_start_is_guardedly_cleaned_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(output=Path(tmp) / 'capture', nodes=['n'], client_command=['sinomo'], workload=['true'],
                                      source_revision='test', binary_sha256='test', profile='test', poll_seconds=.1, rpc_timeout=1, duration_seconds=1)
            owner = None
            calls = []
            def run(command, **kwargs):
                nonlocal owner
                calls.append(command)
                if '--capture' in command and 'control' in command:
                    owner = command[command.index('--capture') + 1]
                    raise subprocess.TimeoutExpired(command, 1)
                if '--clear-capture' in command:
                    self.assertEqual(command[command.index('--expected-capture') + 1], owner)
                    owner = None
                body = {"active_capture_id": owner, "base_filter": "warn", "active_filter": "warn", "records": []}
                return subprocess.CompletedProcess(command, 0, json.dumps(body), '')
            with mock.patch.object(capture.subprocess, 'run', side_effect=run):
                self.assertEqual(capture.run_capture(args), 2)
            self.assertIsNone(owner)
            self.assertTrue(any('--clear-capture' in cmd for cmd in calls))
            self.assertTrue(json.loads((args.output / 'manifest.json').read_text())['errors'])

    def test_existing_capture_is_not_mutated(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(output=Path(tmp) / 'capture', nodes=['n'], client_command=['sinomo'], workload=['true'], source_revision='test', binary_sha256='test', profile='test', poll_seconds=.1, rpc_timeout=1, duration_seconds=1)
            with mock.patch.object(capture.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps({"active_capture_id": "someone-else"}), '')) as run:
                self.assertEqual(capture.run_capture(args), 2)
                self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
