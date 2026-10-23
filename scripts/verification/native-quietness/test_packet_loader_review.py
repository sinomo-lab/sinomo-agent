"""Pure synthetic root/program/map/link controls. No BPF syscalls or sockets."""
import copy
import unittest

import packet_loader_review as r


def fixture():
    m=dict(purpose='runtime',node_id='node',boot_id='boot',source_revision='a'*40,
           review_valid_ns=[0,120_000_000_000],groups={},owners=[])
    s=dict(node_id='node',boot_id='boot',now_ns=1_000_000_000,groups={},owners={})
    for i,(role,unit) in enumerate([('main','sinomo-daemon.service'),('shadow','sinomo-shadow-daemon.service')],1):
        group=dict(path='/user.slice/user-1000.slice/user@1000.service/app.slice/'+unit,
                   unit=unit,identity=[12,100+i],kernel_id=1000+i)
        m['groups'][role]=group
        owner=dict(id=role,role=role,pid=100+i,start_ticks=200+i,executable='/bin/sinomo',
                   sha256='c'*64,argv=['sinomo','daemon',role],group=role,ownership_ref='pinned-receipt')
        m['owners'].append(owner);s['owners'][role]=copy.deepcopy(owner)
        s['groups'][role]=dict(group,query_complete=True,effective_ingress_ids=[],effective_egress_ids=[],
                              revision=3,owner_ids=[role])
    a=dict(programs=dict(r.PROGRAMS),map=dict(r.MAP),object_sha256='b'*64,semantics=r.SEMANTICS)
    return m,a,s


def bundles(plan):
    return {key:dict(object_sha256=plan['object_sha256'],root_kernel_id=value['kernel_id'],
                     map=dict(r.MAP),map_id=20+i,program_ids={'ingress':30+2*i,'egress':31+2*i},
                     program_type='cgroup_skb') for i,(key,value) in enumerate(plan['roots'].items())}


def counters(i):
    return dict(boot_id='boot',object_sha256='b'*64,root_kernel_id=1001,map_id=20,
                program_ids={'ingress':30,'egress':31},read_ns=[i*1_000_000_000,i*1_000_000_000+10],
                per_cpu={str(cpu):{d:dict(skb_bytes=i*100,skb_packets=i,overflow=0)
                                  for d in r.PROGRAMS} for cpu in [0,1]})


class PacketReview(unittest.TestCase):
    def reject(self,edit):
        m,a,s=fixture();edit(m,a,s)
        with self.assertRaises((ValueError,KeyError,TypeError)):r.plan(m,a,s)

    def test_plan_is_never_authority_to_execute_or_accept(self):
        p=r.plan(*fixture())
        self.assertTrue(p['review_valid'])
        self.assertFalse(p['execution_ready']);self.assertFalse(p['acceptance'])
        self.assertFalse(p['kernel_actions_performed']);self.assertFalse(p['replace_existing'])
        self.assertFalse(p['counter_reset']);self.assertEqual(p['allow_verdict'],1)

    def test_node_boot_and_stale_observations_reject(self):
        for key,value in [('boot_id','new'),('node_id','other'),('now_ns',121_000_000_000)]:
            with self.subTest(key=key):self.reject(lambda m,a,s:s.update({key:value}))

    def test_broad_root_and_session_fallback_reject(self):
        for path in ['/','/user.slice','/user.slice/user-1000.slice/user@1000.service','/user.slice/session-4.scope']:
            def edit(m,a,s):
                m['groups']['main'].update(path=path,unit=path.split('/')[-1]);s['groups']['main'].update(m['groups']['main'])
            with self.subTest(path=path):self.reject(edit)

    def test_overlap_and_cgroup_incarnation_reject(self):
        self.reject(lambda m,a,s:m['groups']['shadow'].update(path=m['groups']['main']['path']+'/child.scope'))
        self.reject(lambda m,a,s:s['groups']['main'].update(identity=[12,999]))
        self.reject(lambda m,a,s:s['groups']['main'].update(kernel_id=999))

    def test_existing_and_inherited_attachment_is_not_replaced(self):
        for d in ['effective_ingress_ids','effective_egress_ids']:
            with self.subTest(direction=d):self.reject(lambda m,a,s:s['groups']['main'].update({d:[77]}))
        self.reject(lambda m,a,s:s['groups']['main'].update(query_complete=False))

    def test_identity_and_unclassified_helpers_reject(self):
        self.reject(lambda m,a,s:s['owners']['main'].update(sha256='d'*64))
        self.reject(lambda m,a,s:s['owners']['main'].update(start_ticks=999))
        self.reject(lambda m,a,s:s['groups']['main']['owner_ids'].append('unclassified'))
        self.reject(lambda m,a,s:m['owners'][1].update(pid=m['owners'][0]['pid']))

    def test_unowned_extra_root_and_missing_shadow_reject(self):
        self.reject(lambda m,a,s:m['groups'].update(extra=dict(m['groups']['main'])))
        self.reject(lambda m,a,s:m['owners'].pop())

    def test_wrong_object_contract_and_semantics_reject(self):
        self.reject(lambda m,a,s:a['map'].update(flags=0))
        self.reject(lambda m,a,s:a.update(semantics='interface-bytes'))
        self.reject(lambda m,a,s:a['programs'].update(ingress='drop_packets'))

    def test_bundle_identifiers_immutable_and_per_root(self):
        p=r.plan(*fixture());b=bundles(p);r.validate_bundles(p,b)
        altered=copy.deepcopy(b);altered['main']['program_ids']['ingress']=1234
        with self.assertRaisesRegex(ValueError,'immutable'):r.validate_bundles(p,altered,b)
        altered=copy.deepcopy(b);altered['shadow']['map_id']=b['main']['map_id']
        with self.assertRaisesRegex(ValueError,'shared'):r.validate_bundles(p,altered)

    def test_exact_own_link_cleanup_never_detaches_peer(self):
        owned=[dict(handle='own-fd',link_id=8,program_id=30,root_kernel_id=1001,direction='ingress')]
        observed={'own-fd':copy.deepcopy(owned[0]),'peer-fd':dict(link_id=77)}
        actions=r.cleanup_actions(owned,observed)
        self.assertEqual(actions,[dict(operation='close_owned_link_fd',handle='own-fd',expected_link_id=8)])
        self.assertIn('peer-fd',observed)
        observed['own-fd']['link_id']=88
        with self.assertRaisesRegex(ValueError,'identity'):r.cleanup_actions(owned,observed)

    def test_missing_link_identity_preserves_unknown_outcome(self):
        owned=[dict(handle='own-fd',link_id=8,program_id=30,root_kernel_id=1001,direction='ingress')]
        with self.assertRaises(ValueError):r.cleanup_actions(owned,{})
        with self.assertRaises(ValueError):r.cleanup_actions(owned*2,{'own-fd':owned[0]})

    def test_directions_and_all_cpu_slots_counted_without_gate_conversion(self):
        v=r.counter_delta(counters(1),counters(0))
        self.assertEqual(v['directions']['ingress']['skb_bytes'],200)
        self.assertEqual(v['directions']['egress']['skb_bytes'],200)
        self.assertEqual(v['minimum_interval_ns'],999_999_990)
        self.assertFalse(v['numeric_gate_applied']);self.assertFalse(v['interface_equivalent'])

    def test_map_restart_overflow_and_counter_reset_reject(self):
        a,b=counters(1),counters(2)
        for field,value in [('map_id',99),('boot_id','new')]:
            changed=copy.deepcopy(b);changed[field]=value
            with self.assertRaises(ValueError):r.counter_delta(changed,a)
        b['per_cpu']['0']['ingress']['overflow']=1
        with self.assertRaisesRegex(ValueError,'overflow'):r.counter_delta(b,a)
        b=counters(2);b['per_cpu']['0']['egress']['skb_bytes']=0
        with self.assertRaisesRegex(ValueError,'reset'):r.counter_delta(b,a)

    def test_cpu_slot_gap_and_counter_read_stall_reject(self):
        a,b=counters(0),counters(1);del b['per_cpu']['1']
        with self.assertRaisesRegex(ValueError,'inventory'):r.counter_delta(b,a)
        b=counters(10)
        with self.assertRaisesRegex(ValueError,'stall'):r.counter_delta(b,a)


if __name__=='__main__':unittest.main()
