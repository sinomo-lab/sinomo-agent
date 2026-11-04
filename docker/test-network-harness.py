#!/usr/bin/env python3
"""No-Docker regressions for network acceptance helpers."""
import shutil
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('network_checks', Path(__file__).parent/'lib/network_checks.py')
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)


def report(targets=('a', 'b'), status='passed'):
    return {'case': {'id': 'cluster.network.ping-benchmark'}, 'status': status,
            'diagnostics': {'targets': list(targets), 'target_count': len(targets),
                            'total_requests': len(targets)*4, 'success_count': len(targets)*4,
                            'failure_count': 0, 'successes_by_target': {t: 4 for t in targets}}}


class Tests(unittest.TestCase):
    def test_missing_duplicate_wrong_targets_or_skipped_do_not_pass(self):
        for value in [report(('a',)), report(('a','a')), report(('a','c')), report(status='skipped')]:
            self.assertFalse(checks.validate_probe(value, ['a','b'], 4))
        self.assertTrue(checks.validate_probe(report(), ['a','b'], 4))

    def test_count_mismatch_and_request_errors_fail(self):
        for field, value in [('success_count', 7), ('failure_count', 1), ('total_requests', 0),
                             ('successes_by_target', {'a': 8})]:
            r=report(); r['diagnostics'][field]=value
            self.assertFalse(checks.validate_probe(r, ['a','b'], 4))

    def test_partition_requires_completed_failed_rpc_results(self):
        self.assertFalse(checks.validate_probe({'status':'failed'}, ['a'], 4, False))
        r=report(('a',), 'failed'); r['diagnostics'].update(success_count=0, failure_count=4, failures_by_target={'a':4})
        self.assertTrue(checks.validate_probe(r, ['a'], 4, False))

    def test_case_selection_is_explicit_and_deduplicated(self):
        self.assertEqual(checks.select_cases('smoke,ready,bridge'), ['ready','bridge'])
        with self.assertRaises(ValueError): checks.select_cases('everything')

    def test_earlier_origin_must_remain_healthy_and_sweeps_consecutive(self):
        with tempfile.TemporaryDirectory() as d:
            c=checks.Checks(SimpleNamespace(artifacts=d, project='test', compose_file=[], wait=120, requests=4))
            c.ids={n:n for n in checks.NODES}
            calls=[]
            def probe(node, targets, deadline, *args):
                calls.append((c.sweep,node))
                return not (c.sweep==2 and node=='node1')
            with patch.object(c, 'probe', side_effect=probe), patch.object(checks.time, 'sleep'):
                c.ready()
            self.assertEqual(c.sweep, 5)
            self.assertEqual(len(calls), 30)

    def test_timeout_preserves_partial_output(self):
        import subprocess
        with tempfile.TemporaryDirectory() as d:
            c=checks.Checks(SimpleNamespace(artifacts=d, project='test', compose_file=[]))
            with patch.object(checks.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['x'], 1, b'partial', b'error')):
                rc, raw=c.run('failure',['x'],1)
            self.assertEqual((rc,raw),(124,b'partial'))
            self.assertEqual((Path(d)/'failure.stderr').read_bytes(),b'error')
            with self.assertRaises(RuntimeError): c.run('failure',['x'])

    def test_bridge_is_restored_when_partition_assertion_fails(self):
        with tempfile.TemporaryDirectory() as d:
            c=checks.Checks(SimpleNamespace(artifacts=d, project='test', compose_file=[], wait=1))
            (Path(d)/'inspect-node4.stdout').write_text(json.dumps([{'NetworkSettings':{'Networks':{'test_seg_ab':{}}}}]))
            commands=[]
            def run(label, argv, budget=20):
                commands.append(label)
                return (0, b'[{"NetworkSettings":{"Networks":{}}}]' if label in ('verify-disconnect','bridge-cleanup-state') else b'container')
            with patch.object(c,'run',side_effect=run), patch.object(c,'ready',side_effect=[None,RuntimeError('partition failure')]):
                with self.assertRaisesRegex(RuntimeError,'partition failure'): c.bridge()
            self.assertEqual(commands[-1],'bridge-reconnect')

    def test_bridge_restores_cut_when_disconnect_acknowledgement_is_interrupted(self):
        with tempfile.TemporaryDirectory() as d:
            c=checks.Checks(SimpleNamespace(artifacts=d, project='test', compose_file=[], wait=1))
            (Path(d)/'inspect-node4.stdout').write_text(json.dumps([{'NetworkSettings':{'Networks':{'test_seg_ab':{}}}}]))
            commands=[]
            def run(label, argv, budget=20):
                commands.append(label)
                if label=='bridge-disconnect': raise KeyboardInterrupt()
                return (0, b'[{"NetworkSettings":{"Networks":{}}}]' if label=='bridge-cleanup-state' else b'container')
            with patch.object(c,'run',side_effect=run), patch.object(c,'ready'):
                with self.assertRaises(KeyboardInterrupt): c.bridge()
            self.assertEqual(commands[-1],'bridge-reconnect')

    def test_preflight_rejects_wrong_running_binary_before_traffic(self):
        with tempfile.TemporaryDirectory() as d:
            binary=Path(d)/'sinomo'; binary.write_bytes(b'candidate')
            c=checks.Checks(SimpleNamespace(artifacts=d, project='test', compose_file=[], binary=str(binary)))
            with patch.object(c,'run',return_value=(0,b'wrong /proc/1/exe\nwrong /proc/2/exe')) as run:
                with self.assertRaisesRegex(RuntimeError,'running binary'): c.preflight()
            self.assertEqual(run.call_count,1)

    def test_plan_and_missing_binary_cannot_start_docker_or_build(self):
        import os
        import subprocess
        with tempfile.TemporaryDirectory() as d:
            directory=Path(d)
            for name in ['docker','cargo']:
                path=directory/name
                shutil.copy2(Path(__file__).with_name("fixtures") / "record-invocation.sh", path)
            env=dict(os.environ, PATH=d+':'+os.environ['PATH'], SINOMO_TEST_NETWORK_CASES='ready',
                     SINOMO_TEST_NETWORK_PLAN='1', SINOMO_TEST_ARTIFACTS=d+'/artifacts')
            script=Path(__file__).parent/'test-cluster.sh'
            p=subprocess.run(['bash',str(script)],env=env,capture_output=True)
            self.assertEqual(p.returncode,0,p.stderr)
            self.assertFalse((directory/'invoked').exists())
            env.update(SINOMO_TEST_NETWORK_PLAN='0', SINOMO_TEST_BINARY=d+'/absent',
                       SINOMO_DOCKER_BINARY=d+'/absent', SINOMO_TEST_BUILD='0', SINOMO_TEST_ALLOW_OTHER_COMPOSE='1')
            p=subprocess.run(['bash',str(script)],env=env,capture_output=True)
            self.assertNotEqual(p.returncode,0)
            self.assertIn(b'prebuilt binary missing',p.stdout)
            self.assertFalse((directory/'invoked').exists())


if __name__=='__main__': unittest.main()
