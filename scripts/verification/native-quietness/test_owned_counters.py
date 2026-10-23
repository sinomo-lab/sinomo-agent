"""Local filesystem/kernel fixtures for manifest-bound collection failures."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import daemon_scope as policy
import owned_counters as c


class FakeHandle:
    def __init__(self, kernel, pid):
        self.kernel, self.pid, self.closed = kernel, pid, False

    def alive(self):
        return self.kernel.alive[self.pid]

    def close(self):
        self.closed = True


class FakeKernel(c.Kernel):
    def __init__(self, root):
        super().__init__(root/'proc',root/'cgroup')
        self.alive = {123:True,124:True,125:True}
        self.pins = []

    def pin(self, pid):
        handle = FakeHandle(self,pid)
        self.pins.append(handle)
        return handle


class OwnedCounters(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.kernel = FakeKernel(self.root)
        boot = self.kernel.proc/'sys/kernel/random/boot_id'
        boot.parent.mkdir(parents=True);boot.write_text('boot-A\n')
        data = self.root/'DATA';data.mkdir()
        exe = self.root/'binary';exe.write_bytes(b'fixture executable identity')
        self.manifest = dict(schema=c.SCHEMA,purpose='runtime',node_id='node-A',boot_id='boot-A',
                             source_revision='a'*40,data_dir=str(data),data_identity=c.object_identity(data),
                             groups={},owners=[],delegations=[])
        for key,pids in [('main',[123,125]),('shadow',[124])]:
            path=self.kernel.cgroup/'owned'/key;path.mkdir(parents=True)
            (path/'cpu.stat').write_text('usage_usec 100\nuser_usec 60\nsystem_usec 40\n')
            (path/'cgroup.procs').write_text(''.join(str(p)+'\n' for p in pids))
            self.manifest['groups'][key]=dict(path='/owned/'+key,identity=c.object_identity(path))
        for pid,role,group in [(123,'main','main'),(124,'shadow','shadow'),(125,'helper','main')]:
            p=self.kernel.proc/str(pid);p.mkdir()
            self.write_stat(pid,pid*10)
            (p/'cmdline').write_bytes(b'fixture\0'+role.encode()+b'\0')
            (p/'exe').symlink_to(exe)
            (p/'cgroup').write_text('0::/owned/'+group+'\n')
            (p/'io').write_text('read_bytes: 100\nwrite_bytes: 200\ncancelled_write_bytes: 0\n')
            self.manifest['owners'].append(dict(id=role,role=role,group=group,pid=pid,start_ticks=pid*10,
                                                executable=str(exe),sha256=c.digest(exe),argv=['fixture',role],
                                                ownership_ref='fixture-runtime-receipt'))
        self.path=self.root/'manifest.json'
        self.write_manifest()

    def write_stat(self,pid,ticks):
        fields=['0']*20;fields[0]='S';fields[19]=str(ticks)
        (self.kernel.proc/str(pid)/'stat').write_text(str(pid)+' (name with ) spaces) '+' '.join(fields)+'\n')

    def write_manifest(self):
        receipt=self.root/'runtime-receipt.json'
        receipt.write_text(json.dumps({k:self.manifest[k] for k in c.BINDINGS})+'\n')
        self.manifest['runtime_receipt']=dict(path=str(receipt),sha256=c.digest(receipt))
        self.path.write_text(json.dumps(self.manifest)+'\n')

    def collector(self):
        obj=c.Collector(self.path,c.digest(self.path),self.kernel)
        self.addCleanup(obj.close)
        return obj

    def test_collects_disjoint_cgroup_cpu_and_process_io(self):
        row=self.collector().sample(0)
        self.assertEqual(set(row['cpu']['roots']),{'main','shadow'})
        self.assertEqual(row['disk']['read_bytes'],300)
        self.assertEqual(row['disk']['write_bytes'],600)
        self.assertFalse(row['acceptance'])
        self.assertFalse(row['network']['available'])
        self.assertFalse(row['writeback']['complete'])

    def test_manifest_hash_and_receipt_binding_rejected(self):
        with self.assertRaisesRegex(ValueError,'manifest digest'):c.Collector(self.path,'0'*64,self.kernel)
        self.manifest['node_id']='changed';self.path.write_text(json.dumps(self.manifest))
        with self.assertRaisesRegex(ValueError,'binding'):self.collector()

    def test_boot_mismatch(self):
        (self.kernel.proc/'sys/kernel/random/boot_id').write_text('new-boot\n')
        with self.assertRaisesRegex(ValueError,'boot'):self.collector()

    def test_pid_start_mismatch(self):
        self.write_stat(123,99999)
        with self.assertRaisesRegex(ValueError,'PID/start'):self.collector()

    def test_executable_digest_and_command_identity(self):
        (self.root/'binary').write_bytes(b'other bytes')
        with self.assertRaisesRegex(ValueError,'digest'):self.collector()
        self.manifest['owners'][0]['sha256']=c.digest(self.root/'binary');self.write_manifest()
        (self.kernel.proc/'123/cmdline').write_bytes(b'fixture\0different\0')
        with self.assertRaisesRegex(ValueError,'command'):self.collector()

    def test_data_root_identity(self):
        self.manifest['data_identity']=[0,0];self.write_manifest()
        with self.assertRaisesRegex(ValueError,'DATA'):self.collector()

    def test_root_overlap_and_host_root_forbidden(self):
        self.manifest['groups']['shadow']['path']='/owned/main/child';self.write_manifest()
        with self.assertRaisesRegex(ValueError,'overlapping'):self.collector()
        self.manifest['groups']['shadow']['path']='/';self.write_manifest()
        with self.assertRaisesRegex(ValueError,'host-wide'):self.collector()

    def test_cgroup_identity_and_duplicate_pid(self):
        old=copy.deepcopy(self.manifest)
        self.manifest['groups']['main']['identity']=[0,0];self.write_manifest()
        with self.assertRaisesRegex(ValueError,'cgroup identity'):self.collector()
        self.manifest=old;self.manifest['owners'][1]['pid']=123;self.write_manifest()
        with self.assertRaisesRegex(ValueError,'duplicate'):self.collector()

    def test_runtime_requires_main_and_shadow(self):
        self.manifest['owners']=self.manifest['owners'][:1];self.write_manifest()
        with self.assertRaisesRegex(ValueError,'main and shadow'):self.collector()
        self.assertTrue(all(p.closed for p in self.kernel.pins))

    def test_cpu_reset_invalidates_policy_comparison(self):
        collector=self.collector();old=collector.sample(0)
        (self.kernel.cgroup/'owned/main/cpu.stat').write_text('usage_usec 0\n')
        new=collector.sample(1)
        self.assertTrue(any('reset' in e for e in new['errors']))
        with self.assertRaisesRegex(ValueError,'collection incomplete'):policy.cpu_interval(new['cpu'],old['cpu'])

    def test_exited_helper_retains_last_io_without_claiming_final(self):
        collector=self.collector();old=collector.sample(0)
        self.kernel.alive[125]=False
        (self.kernel.cgroup/'owned/main/cgroup.procs').write_text('123\n')
        (self.kernel.cgroup/'owned/main/cpu.stat').write_text('usage_usec 125\n')
        new=collector.sample(1)
        self.assertEqual(new['owners']['helper']['last_io'],old['owners']['helper']['last_io'])
        self.assertEqual(new['owners']['helper']['state'],'retired-unfinalized')
        self.assertIsNone(new['owners']['helper']['final_io'])
        self.assertIsNone(new['disk']['read_bytes'])
        self.assertEqual(new['cpu']['roots']['main']['usage_usec'],125)
        self.assertEqual(new['cpu']['collection_errors'],[])

    def test_reused_pid_is_not_new_owner(self):
        collector=self.collector();collector.sample(0)
        self.kernel.alive[125]=False
        self.write_stat(125,9999)
        row=collector.sample(1)
        self.assertTrue(any('occupant incarnation' in e for e in row['errors']))
        self.assertEqual(row['owners']['helper']['last_io']['read_bytes'],100)

    def test_moved_owner_keeps_io_and_requires_cpu_handoff(self):
        collector=self.collector();collector.sample(0)
        (self.kernel.proc/'125/cgroup').write_text('0::/other/service\n')
        (self.kernel.cgroup/'owned/main/cgroup.procs').write_text('123\n')
        row=collector.sample(1)
        self.assertEqual(row['owners']['helper']['state'],'moved')
        self.assertEqual(row['owners']['helper']['last_io']['write_bytes'],200)
        self.assertTrue(any('handoff missing' in e for e in row['errors']))

    def test_unknown_child_is_not_assumed_owned_or_ignored(self):
        collector=self.collector()
        (self.kernel.cgroup/'owned/main/cgroup.procs').write_text('123\n125\n999\n')
        row=collector.sample(0)
        self.assertEqual(row['unknown_members'],[999])
        self.assertTrue(row['cpu']['collection_errors'])

    def test_process_io_reset_does_not_publish_zero(self):
        collector=self.collector();collector.sample(0)
        (self.kernel.proc/'125/io').write_text('read_bytes: 0\nwrite_bytes: 0\ncancelled_write_bytes: 0\n')
        row=collector.sample(1)
        self.assertIsNone(row['disk']['read_bytes'])
        self.assertTrue(any('I/O counter reset' in e for e in row['errors']))
        self.assertEqual(row['owners']['helper']['last_io']['read_bytes'],100)

    def test_io_denial_preserves_available_cpu(self):
        collector=self.collector();original=Path.read_text
        def read(path,*args,**kwargs):
            if path==self.kernel.proc/'125/io':raise PermissionError('denied fixture I/O')
            return original(path,*args,**kwargs)
        with patch.object(Path,'read_text',read):row=collector.sample(0)
        self.assertEqual(len(row['cpu']['roots']),2)
        self.assertIsNone(row['disk']['write_bytes'])
        self.assertTrue(any('denied' in e for e in row['errors']))

    def test_receipt_or_manifest_drift_is_sticky(self):
        collector=self.collector();collector.sample(0)
        self.path.write_text(self.path.read_text()+' ')
        row=collector.sample(1)
        self.assertTrue(any('manifest changed' in e for e in row['errors']))
        self.write_manifest()
        self.assertTrue(collector.sample(2)['errors'])

    def test_claimed_coverage_and_delegation_completion_cannot_grant_pass(self):
        self.manifest['coverage_complete']=True
        self.manifest['delegations']=[dict(owner_id='helper',operation_id='operation-A',
                                            ownership_ref='receipt-A',complete=True)]
        self.write_manifest();row=self.collector().sample(0)
        self.assertFalse(row['ownership_complete'])
        self.assertFalse(row['acceptance'])
        self.assertFalse(row['network']['available'])
        self.assertFalse(row['writeback']['complete'])
        with self.assertRaisesRegex(ValueError,'substitute'):policy.disk_window(row['disk'],row['disk'])

    def test_namespace_network_file_never_read(self):
        collector=self.collector();original=Path.read_text
        def read(path,*args,**kwargs):
            if 'net/dev' in str(path):raise AssertionError('namespace byte substitution')
            return original(path,*args,**kwargs)
        with patch.object(Path,'read_text',read):row=collector.sample(0)
        self.assertFalse(row['network']['available'])

    def test_no_counter_cancel_subtraction(self):
        (self.kernel.proc/'125/io').write_text('read_bytes: 100\nwrite_bytes: 200\ncancelled_write_bytes: 150\n')
        row=self.collector().sample(0)
        self.assertEqual(row['disk']['write_bytes'],600)

    def test_capture_charges_initialization_to_original_deadline(self):
        clock=[0]
        class SlowCollector:
            def __init__(self,*args):
                clock[0]=119*c.SECOND
                self.manifest={'purpose':'isolated-control','source_revision':'a'*40}
                self.sticky_errors=[]
            def sample(self,sequence):
                clock[0]+=2*c.SECOND
                return {'sequence':sequence}
            def close(self):pass
        with patch.object(c,'Collector',SlowCollector),patch.object(c.time,'monotonic_ns',lambda:clock[0]):
            result=c.capture(self.path,c.digest(self.path),self.root/'deadline-output',samples=4)
        self.assertEqual(result['absolute_deadline_ns'],120*c.SECOND)
        self.assertEqual(result['samples'],1)
        self.assertTrue(any('original absolute deadline' in x for x in result['errors']))
        self.assertFalse(result['collection_completed'])

    def test_capture_reports_cadence_gap_without_filling_samples(self):
        clock=[0]
        class SlowCollector:
            def __init__(self,*args):
                self.manifest={'purpose':'isolated-control','source_revision':'a'*40}
                self.sticky_errors=[]
            def sample(self,sequence):
                clock[0]+=1_200_000_000
                return {'sequence':sequence}
            def close(self):pass
        with patch.object(c,'Collector',SlowCollector),patch.object(c.time,'monotonic_ns',lambda:clock[0]):
            result=c.capture(self.path,c.digest(self.path),self.root/'cadence-output',samples=2)
        self.assertEqual(result['samples'],2)
        self.assertIn('one-second cadence missed',result['errors'])
        self.assertFalse(result['acceptance'])

    def test_output_and_bad_manifest_fail_without_overwriting_evidence(self):
        output=self.root/'previous';output.mkdir();(output/'receipt.json').write_text('preserved')
        with self.assertRaises(FileExistsError):c.capture(self.path,'0'*64,output,samples=1)
        self.assertEqual((output/'receipt.json').read_text(),'preserved')
        result=c.capture(self.path,'0'*64,self.root/'bad-manifest',samples=1)
        self.assertEqual(result['samples'],0)
        self.assertTrue(any('manifest digest' in x for x in result['errors']))
        self.assertFalse(result['acceptance'])


if __name__=='__main__':unittest.main()
