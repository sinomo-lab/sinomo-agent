#!/usr/bin/env python3
import importlib.util
import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('quiet_network', Path(__file__).with_name('quiet-network-counters.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def frame(start, closed, received=0, sent=0):
    return {'container_id': 'a' * 64, 'container_started_at': 'fixed-start',
            'namespace': 'net:[123]', 'generations': ['1', '100', '2', '101'],
            'capture_started_ms': start, 'capture_closed_ms': closed,
            'interfaces': {'lo': {'rx_bytes': str(received), 'tx_bytes': str(sent)}}}


class NetworkCounters(unittest.TestCase):
    def test_all_interfaces_loopback_and_values_above_float_precision(self):
        n = 2**53 + 41
        row = lambda name, rx, tx: name + ': ' + ' '.join(map(str, [rx] + [0]*7 + [tx] + [0]*7))
        raw = 'net:[123]\n1\t100\t2\t101\nInter-| Receive | Transmit\nface |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n' + row('lo', n, n+1) + '\n' + row('eth0', 7, 11) + '\n'
        namespace, generation, interfaces = MODULE.parse_counters(raw)
        self.assertEqual(namespace, 'net:[123]')
        self.assertEqual(generation, ('1', '100', '2', '101'))
        self.assertEqual(int(interfaces['lo']['rx_bytes']), n)
        self.assertEqual(int(interfaces['lo']['tx_bytes']), n+1)
        self.assertEqual(len(interfaces), 2)
        old = frame(0, 1000, n, n)
        new = frame(2000, 2010, n+65536, n)
        self.assertTrue(MODULE.counter_delta(new, old)['within_limit'])
        new['interfaces']['lo']['rx_bytes'] = str(n+65537)
        self.assertFalse(MODULE.counter_delta(new, old)['within_limit'])

    def test_receive_and_transmit_are_combined_without_rounding(self):
        old = frame(0, 1000)
        self.assertTrue(MODULE.counter_delta(frame(2000, 2010, 32768, 32768), old)['within_limit'])
        self.assertFalse(MODULE.counter_delta(frame(2000, 2010, 32768, 32769), old)['within_limit'])
        self.assertEqual(MODULE.counter_delta(frame(1000, 1010), None)['status'], 'baseline')

    def test_uncertainty_is_not_subtracted_to_hide_a_rate(self):
        old = frame(0, 1010)
        self.assertFalse(MODULE.counter_delta(frame(2000, 2500, 65536), old)['within_limit'])

    def test_namespace_interface_generation_reset_and_gap_are_unavailable(self):
        old = frame(0, 1000, 100, 100)
        changes = [lambda f: f.update(namespace='net:[124]'),
                   lambda f: f.update(generations=['1','100','3','102']),
                   lambda f: f['interfaces'].update(eth0={'rx_bytes':'0','tx_bytes':'0'}),
                   lambda f: f['interfaces']['lo'].update(rx_bytes='99'),
                   lambda f: f.update(capture_started_ms=1000),
                   lambda f: f.update(capture_started_ms=6001)]
        for change in changes:
            candidate = frame(2000, 2010, 100, 100)
            change(candidate)
            with self.assertRaises(ValueError): MODULE.counter_delta(candidate, old)
        self.assertTrue(MODULE.counter_delta(frame(2000,2010,100,100), json.loads(json.dumps(old)))['within_limit'])

    def test_one_unavailable_container_keeps_other_raw_rows_and_does_not_supply_zero(self):
        members = [{'node':'node1','container_id':'a'*64}, {'node':'node2','container_id':'b'*64}]
        def read(member, *_):
            if member['node'] == 'node2':
                raise ValueError('unsupported proc counters')
            return frame(1000,1010,7,11)
        with patch.object(MODULE, 'read_counter', side_effect=read):
            output = MODULE.sample_group(members, 'own-project', 120000, {}, 'exact-shared-scanner')
        self.assertEqual(output['members']['node1']['interfaces']['lo']['rx_bytes'], '7')
        self.assertEqual(output['members']['node2']['status'], 'unavailable')
        self.assertIsNone(output['members']['node2']['within_limit'])
        self.assertNotIn('interfaces', output['members']['node2'])

    def test_persistent_reader_checks_nonce_completion_and_fragmented_frames(self):
        for mode in ('valid', 'stale', 'failed', 'partial'):
            reply_read, reply_write = os.pipe()
            request_read, request_write = os.pipe()
            def worker():
                request = os.read(request_read, 1024).decode().strip()
                if mode == 'stale': request = 'net-other-request'
                body = (request + '\nnetwork-counter-data\n' + request + '\t').encode()
                os.write(reply_write, body)
                time.sleep(0.01)
                if mode != 'partial': os.write(reply_write, b'0\n' if mode != 'failed' else b'1\n')
                os.close(reply_write); os.close(request_read)
            thread = threading.Thread(target=worker); thread.start()
            try:
                if mode == 'valid':
                    self.assertEqual(MODULE.read_worker_counter(reply_read, request_write, MODULE.monotonic_ms()+1000), 'network-counter-data\n')
                else:
                    with self.assertRaises(ValueError):
                        MODULE.read_worker_counter(reply_read, request_write, MODULE.monotonic_ms()+1000)
            finally:
                thread.join(); os.close(reply_read); os.close(request_write)

    def test_batched_namespace_scan_rejects_unreadable_or_foreign_live_processes(self):
        real_run = subprocess.run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            proc = root / 'proc'
            for pid in ('1', '2', '3'):
                (proc / pid / 'ns').mkdir(parents=True)
                (proc / pid / 'ns/net').touch()
                (proc / pid / 'stat').write_text(f'{pid} (test process) S 0\n')
            (proc / 'self/ns').mkdir(parents=True)
            (proc / 'self/ns/net').symlink_to('net:[123]')
            (proc / 'net').mkdir()
            (proc / 'net/dev').write_text('Inter-| Receive | Transmit\nface | bytes | bytes\nlo: ' + ' '.join(['0']*16) + '\n')
            stat = root / 'stat'
            stat.write_text('#!/bin/sh\ncat "$QUIET_TEST_STAT_ROWS"\n')
            stat.chmod(0o755)
            rows = root / 'rows'
            identity = dict(id='a'*64, project='test', running=True, started_at='start', network_mode='bridge', pid_mode='')
            def run(argv, **kwargs):
                if argv[1] == 'inspect':
                    return subprocess.CompletedProcess(argv, 0, json.dumps(identity), '')
                program = argv[5].replace('/proc', str(proc))
                env = dict(os.environ, PATH=str(root)+':'+os.environ['PATH'], QUIET_TEST_STAT_ROWS=str(rows))
                return real_run(['sh', '-c', program, 'sh', argv[7]], env=env, **kwargs)
            scanner = "printf '1 100 2 101\\n'"
            def collect():
                with patch.object(MODULE.subprocess, 'run', side_effect=run):
                    return MODULE.read_counter({'container_id':'a'*64}, 'test', MODULE.monotonic_ms()+5000, scanner)
            def set_rows(entries):
                rows.write_text(''.join(f'{proc}/{pid}/ns/net:{inode}\n' for pid,inode in entries))
            set_rows([(1,123),(2,123),(3,123)])
            self.assertEqual(collect()['namespace'], 'net:[123]')
            for entries in [[(1,123),(2,123),(3,124)], [(1,123),(2,123)], [(1,123),(3,123)]]:
                set_rows(entries)
                with self.assertRaises(ValueError): collect()
            # A zombie has no namespace; a live unreadable process above fails.
            (proc / '3/stat').write_text('3 (test process) Z 0\n')
            set_rows([(1,123),(2,123)])
            self.assertEqual(collect()['namespace'], 'net:[123]')
            # An absent short-lived helper contributes no second namespace.
            (proc / '3/ns/net').unlink()
            (proc / '3/ns').rmdir()
            (proc / '3/stat').unlink()
            (proc / '3').rmdir()
            set_rows([(1,123),(2,123)])
            self.assertEqual(collect()['namespace'], 'net:[123]')

    def test_malformed_or_missing_loopback_counters_are_not_zero(self):
        for raw in ['unavailable', 'net:[123]\n1 100 2 101\nReceive Transmit\nheader\neth0: invalid']:
            with self.assertRaises(ValueError): MODULE.parse_counters(raw)


if __name__ == '__main__':
    unittest.main()
