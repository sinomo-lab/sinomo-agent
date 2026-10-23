"""Isolated process-family policy controls, not live rollout acceptance."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import controller
import daemon_scope as d


def cpu(index):
    return dict(scope=d.SCOPE, boot_id="boot", read_ns=[index*d.SECOND]*2,
                roots={name: dict(path='/owned/'+name, incarnation=name+':epoch',
                                  source='cgroup-v2-cpu.stat', usage_usec=index*20_000)
                       for name in ('main','shadow','detached-helper')})


def network(index):
    return dict(scope=d.SCOPE, semantics='complete-owned-interface-equivalent-TX-RX',
                owner_epoch='owned:epoch', includes_loopback=True,
                read_ns=[index*d.SECOND]*2, rx_bytes=index*200, tx_bytes=index*300)


def disk(index):
    return dict(scope=d.SCOPE, semantics='owned-process-block-bytes-with-retired-work',
                owner_epoch='owned:epoch', read_ns=[index*60*d.SECOND]*2,
                read_bytes=index*614400, write_bytes=0)


def evidence():
    return dict(scope=d.SCOPE,
                coverage={k:dict(complete=True,evidence_ref='fixture:'+k) for k in d.COVERAGE},
                windows=dict(cpu_network_joint_ns=30*d.SECOND,disk_ns=60*d.SECOND,
                             elapsed_ns=61*d.SECOND,continuous_cadence=True),
                limits=dict(cpu=True,disk=True,network=True))


class ProcessScope(unittest.TestCase):
    def test_cgroup_aggregate_includes_main_shadow_and_detached_helper(self):
        result=d.cpu_interval(cpu(1),cpu(0))
        self.assertEqual(result['cpu_usec'],60000)
        self.assertTrue(result['within'])
        busy=cpu(1);busy['roots']['detached-helper']['usage_usec']=90000
        self.assertFalse(d.cpu_interval(busy,cpu(0))['within'])

    def test_unrelated_host_load_is_not_a_gate_input(self):
        a=evidence();b=copy.deepcopy(a)
        b['whole_host']={'cpu':9999,'network':2**60,'disk':2**60,'coverage':False}
        b['privileged_host_observer']=None
        self.assertEqual(d.readiness(a),d.readiness(b))
        self.assertTrue(d.readiness(b)['recorded_requirements_satisfied'])

    def test_fixture_satisfaction_never_grants_live_acceptance(self):
        r=d.readiness(evidence())
        self.assertTrue(r['rollout_gate'])
        self.assertFalse(r['whole_host_gate'])
        self.assertFalse(r['acceptance'])
        self.assertEqual(r['live_verdict'],'NOT_QUALIFIED')

    def test_parent_and_child_cgroups_cannot_be_summed(self):
        row=cpu(1);row['roots']['shadow']['path']='/owned/main/child'
        with self.assertRaisesRegex(ValueError,'overlapping'):d.cpu_interval(row,cpu(0))

    def test_host_cgroup_and_cpu_source_rejected(self):
        for key,value in [('path','/'),('source','proc-stat-host'),('path','/owned/../host')]:
            row=cpu(1);row['roots']['main'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):d.cpu_interval(row,cpu(0))

    def test_double_slash_alias_cannot_hide_host_or_overlapping_roots(self):
        for parent,child in [('//','/owned/shadow'),('//owned/main','/owned/main/child')]:
            row=cpu(1)
            row['roots']['main']['path']=parent
            row['roots']['shadow']['path']=child
            with self.subTest(parent=parent),self.assertRaises(ValueError):
                d.validate_roots(row['roots'])

    def test_cgroup_retains_exit_usage_but_not_removal_or_recreation(self):
        a,b=cpu(0),cpu(1)
        a['processes']=[1,2,3];b['processes']=[1,2]
        self.assertEqual(d.cpu_interval(b,a)['cpu_usec'],60000)
        b['roots']['detached-helper']['incarnation']='recreated'
        with self.assertRaisesRegex(ValueError,'recreated'):d.cpu_interval(b,a)
        del b['roots']['detached-helper']
        with self.assertRaisesRegex(ValueError,'inventory'):d.cpu_interval(b,a)

    def test_cpu_reset_missing_or_stalled_data_is_not_zero(self):
        a,b=cpu(1),cpu(2);b['roots']['main']['usage_usec']=0
        with self.assertRaisesRegex(ValueError,'reset'):d.cpu_interval(b,a)
        b=cpu(2);b['read_ns']=[8*d.SECOND]*2
        with self.assertRaisesRegex(ValueError,'stall'):d.cpu_interval(b,a)
        for text in [None,'','user_usec 0\n','usage_usec -1\n','usage_usec 0']:
            with self.subTest(text=text),self.assertRaises(ValueError):d.parse_cpu_stat(text)
        self.assertEqual(d.parse_cpu_stat('usage_usec 123\nuser_usec 30\nsystem_usec 93\n'),123)

    def test_cpu_uncertainty_charged_and_no_machine_cpu_normalization(self):
        a,b=cpu(0),cpu(1)
        b['roots']['main']['usage_usec']=60000
        self.assertTrue(d.cpu_interval(b,a)['within'])
        a['read_ns'][1]=100000
        self.assertFalse(d.cpu_interval(b,a)['within'])

    def test_namespace_framed_rpc_and_ip_bytes_are_not_equivalent(self):
        for semantics in ['proc-PID-net-dev','framed-rpc-bytes','systemd-IP-bytes']:
            row=network(1);row['semantics']=semantics
            with self.subTest(semantics=semantics),self.assertRaisesRegex(ValueError,'semantics'):
                d.network_interval(row,network(0))

    def test_combined_network_and_no_burst_or_loopback_exemption(self):
        row=network(1);row.update(rx_bytes=32768,tx_bytes=32768)
        self.assertTrue(d.network_interval(row,network(0))['within'])
        row['tx_bytes']+=1
        self.assertFalse(d.network_interval(row,network(0))['within'])
        row['tx_bytes']=4194304
        self.assertFalse(d.network_interval(row,network(0))['within'])
        row['includes_loopback']=False
        with self.assertRaisesRegex(ValueError,'loopback'):d.network_interval(row,network(0))

    def test_legacy_read_limit_and_zero_write_policy_preserved(self):
        self.assertTrue(d.disk_window(disk(1),disk(0))['within'])
        row=disk(1);row['read_bytes']+=1
        self.assertFalse(d.disk_window(row,disk(0))['within'])
        row=disk(1);row['write_bytes']=1
        self.assertFalse(d.disk_window(row,disk(0))['within'])

    def test_disk_window_is_not_silently_shortened_to_30_seconds(self):
        row=disk(1);row['read_ns']=[30*d.SECOND]*2
        with self.assertRaisesRegex(ValueError,'60 seconds'):d.disk_window(row,disk(0))

    def test_disk_retired_work_reset_or_new_counter_layer_rejects(self):
        a,b=disk(0),disk(1);a['write_bytes']=1
        with self.assertRaisesRegex(ValueError,'reset'):d.disk_window(b,a)
        b['semantics']='whole-host-diskstats'
        with self.assertRaisesRegex(ValueError,'substitute'):d.disk_window(b,a)

    def test_each_sinomo_coverage_gap_remains_explicit(self):
        for key in d.COVERAGE:
            row=evidence();del row['coverage'][key]
            with self.subTest(key=key):self.assertIn(key,d.readiness(row)['blockers'])

    def test_no_parent_escape_or_deferred_work_exemption(self):
        for key in ['helper_and_detached_work_ownership','sinomo_writeback_and_deferred_work']:
            row=evidence();row['coverage'][key]=dict(complete=True,evidence_ref='')
            self.assertFalse(d.readiness(row)['recorded_requirements_satisfied'])

    def test_original_deadline_and_both_windows_are_required(self):
        for key,value in [('elapsed_ns',121*d.SECOND),('disk_ns',59*d.SECOND),
                          ('cpu_network_joint_ns',29*d.SECOND),('elapsed_ns',1),
                          ('elapsed_ns',True),('continuous_cadence',False)]:
            row=evidence();row['windows'][key]=value
            with self.subTest(key=key):self.assertFalse(d.readiness(row)['recorded_requirements_satisfied'])

    def test_optional_host_capture_error_never_becomes_rollout_blocker(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(controller,'Reader',side_effect=OSError('fixture')):
            r=controller.run({'nodes':[{'name':'host','command':[]}]},Path(tmp)/'capture')
        self.assertFalse(r['rollout_gate'])
        self.assertEqual(r['verdict'],'DIAGNOSTIC')
        self.assertTrue(r['diagnostic_gaps'])
        self.assertNotIn('blockers',r)
        self.assertNotIn('limits',r)


if __name__=='__main__':unittest.main()
