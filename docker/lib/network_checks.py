#!/usr/bin/env python3
"""Shared traffic/evidence checks for existing Docker harnesses; never builds a cluster."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import threading
import time

NODES = [f"node{i}" for i in range(1, 7)]
CASES = {
    "ready": "Three complete 30-pair traffic sweeps; no DFS prerequisite",
    "bridge": "Disconnect/heal node4 bridge; verify partition traffic and recovery",
    "restart": "Three simultaneous crashes; DFS current and retained-byte sentinel",
    "restart-staggered": "Three crashes delaying node4 four seconds; DFS sentinel",
    "latency": "Existing stabilized direct/relay latency gates; DFS convergence prerequisite",
    "payload": "Existing typed-payload throughput gate; DFS convergence prerequisite",
    "attachments": "Existing attachment throughput gate; DFS convergence prerequisite",
}
PROFILES = {"smoke": ["ready"], "recovery": ["ready", "bridge", "restart", "restart-staggered"],
            "performance": ["latency", "payload", "attachments"]}


def select_cases(selection):
    result = []
    for name in selection.split(','):
        names = PROFILES.get(name, [name])
        for case in names:
            if case not in CASES:
                raise ValueError(f"unknown network case: {case}")
            if case not in result:
                result.append(case)
    return result


def validate_probe(report, targets, requests, reachable=True, case_id="cluster.network.ping-benchmark"):
    d = report.get('diagnostics', {})
    expected = set(targets)
    if (report.get('case', {}).get('id') != case_id
            or set(d.get('targets', [])) != expected
            or d.get('target_count') != len(expected)
            or len(d.get('targets', [])) != len(expected)
            or d.get('total_requests') != requests * len(expected)):
        return False
    if reachable:
        return (report.get('status') == 'passed' and d.get('failure_count') == 0
                and d.get('success_count') == requests * len(expected)
                and d.get('successes_by_target') == {target: requests for target in expected})
    # A skipped probe, JSON error, or host timeout is not evidence of a partition.
    failures = d.get('failures_by_target', {})
    return (report.get('status') == 'failed' and d.get('success_count') == 0
            and d.get('failure_count') == requests * len(expected)
            and failures == {target: requests for target in expected})


class Checks:
    def __init__(self, args):
        self.args = args
        self.artifacts = Path(args.artifacts)
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.compose = ['docker', 'compose', '-p', args.project]
        for path in args.compose_file:
            self.compose += ['-f', path]
        self.lock = threading.Lock()
        self.started = time.monotonic()
        self.ids = {}
        self.sweep = 0

    def event(self, **fields):
        with self.lock, (self.artifacts / 'events.jsonl').open('a') as output:
            output.write(json.dumps(dict(elapsed_ms=(time.monotonic()-self.started)*1000, **fields))+'\n')

    def run(self, label, argv, budget=20):
        if budget <= 0:
            raise TimeoutError('absolute deadline exhausted')
        stem = self.artifacts / label
        if stem.with_suffix('.command.json').exists():
            raise RuntimeError(f'refusing to overwrite evidence: {stem}')
        stem.with_suffix('.command.json').write_text(json.dumps(argv))
        started = time.monotonic()
        try:
            p = subprocess.run(argv, capture_output=True, timeout=budget)
            code, out, err = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            code, out, err = 124, e.stdout or b'', e.stderr or b''
        stem.with_suffix('.stdout').write_bytes(out)
        stem.with_suffix('.stderr').write_bytes(err)
        self.event(command=label, returncode=code, duration_ms=(time.monotonic()-started)*1000)
        return code, out

    def cli(self, label, node, arguments, budget=20):
        return self.run(label, [*self.compose, 'exec', '-T', node, 'timeout',
                               str(max(.01, budget-.1)), 'sinomo', *arguments], budget)

    def identities(self):
        for node in NODES:
            code, raw = self.cli('identity-'+node, node, ['cluster', 'status'])
            match = re.search(rb'^local node:\s+(\S+)', raw, re.M)
            if code or not match:
                raise RuntimeError(f'cannot establish identity for {node}')
            self.ids[node] = match[1].decode()
        if len(set(self.ids.values())) != 6:
            raise RuntimeError('duplicate node identities')
        (self.artifacts/'identities.json').write_text(json.dumps(self.ids, indent=2))

    def preflight(self):
        binary = Path(self.args.binary)
        if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError('an executable absolute --binary path is required; no implicit build')
        with binary.open('rb') as source:
            digest = hashlib.file_digest(source, 'sha256').hexdigest()
        network_sets = []
        # Keep full executable verification from filling the capped daemon's
        # file cache with a debug binary that can exceed a GiB.
        script = ('for exe in /proc/[0-9]*/exe; do target=$(readlink "$exe") || continue; '
                  'case "$target" in */sinomo|*/sinomo-shadow) '
                  'digest=$(dd if="$exe" iflag=nocache bs=1M status=none | sha256sum) || exit 1; '
                  'printf "%s  %s\\n" "${digest%% *}" "$exe";; esac; done')
        for node in NODES:
            code, raw = self.run('processes-'+node,
                                 [*self.compose, 'exec', '-T', node, 'bash', '-o', 'pipefail', '-c', script])
            hashes = [line.split()[0] for line in raw.decode().splitlines()]
            if code or len(hashes) != 2 or set(hashes) != {digest}:
                raise RuntimeError(f'{node}: expected exactly main/shadow running binary {digest}, got {hashes}')
            code, raw = self.run('container-'+node, [*self.compose, 'ps', '-q', node])
            if code or not raw.strip():
                raise RuntimeError(f'missing container {node}')
            code, raw = self.run('inspect-'+node, ['docker', 'inspect', raw.decode().strip()])
            info = json.loads(raw)[0]
            if code or info['Config']['Labels']['com.docker.compose.project'] != self.args.project:
                raise RuntimeError('container ownership mismatch')
            if 'SINOMO_DISABLE_IROH=1' not in info['Config']['Env']:
                raise RuntimeError('segmented network cases require Iroh disabled')
            network_sets.append(set(info['NetworkSettings']['Networks']))
        expected = [{0,1,2}, {2,3}, {3,4,5}]
        networks = set.union(*network_sets)
        actual = [{i for i, names in enumerate(network_sets) if net in names} for net in networks]
        if sorted(map(sorted, actual)) != sorted(map(sorted, expected)):
            raise RuntimeError(f'not the six-node segmented topology: {actual}')
        code, raw = self.cli('available-probes', 'node1', ['test', 'list', '--json'])
        if code or not any(case.get('id') == 'cluster.network.connectivity' for case in json.loads(raw)):
            raise RuntimeError('binary does not provide cluster.network.connectivity')
        self.identities()
        (self.artifacts/'manifest.json').write_text(json.dumps({
            'project': self.args.project, 'binary': str(binary), 'sha256': digest,
            'compose_files': self.args.compose_file, 'identities': self.ids,
            'command': self.args.command, 'deadline_seconds': self.args.wait,
            'baseline_seconds': self.args.baseline_wait, 'outage_seconds': self.args.outage_wait,
            'requests_per_target': self.args.requests,
            'route_evidence': 'existing probe reports predicted paths; not executed route traces'}, indent=2))

    def probe(self, node, targets, deadline, target=None, reachable=True):
        label = f'sweep-{self.sweep:03d}-{node}' + (f'-{target}' if target else '')
        budget = min(20, deadline-time.monotonic())
        params = {'requests-per-target': self.args.requests, 'warmup-requests-per-target': 0,
                  'concurrency': self.args.concurrency, 'timeout-ms': self.args.timeout_ms, 'expected-targets': len(targets),
                  'include-unobserved-targets': 'true', 'max-failures': 0,
                  'max-p50-ms': 0, 'max-p95-ms': 0, 'max-p50-us': 0, 'max-p95-us': 0,
                  'min-requests-per-second': 0, 'payload-bytes': 1024,
                  'response-payload-bytes': 1024}
        if target:
            params['target-node-id'] = self.ids[target]
        args = ['test', 'run', 'cluster.network.connectivity', '--require-cluster', '--json']
        for key, value in params.items():
            args += ['--param', f'{key}={value}']
        try:
            code, raw = self.cli(label, node, args, budget)
            report = json.loads(raw)
            valid = code != 124 and (code == 0 if reachable else code != 0) and validate_probe(
                report, targets, self.args.requests, reachable, "cluster.network.connectivity")
        except (ValueError, TimeoutError):
            valid = False
        self.event(probe=label, origin=node, targets=targets, expected_reachable=reachable, passed=valid)
        return valid

    def ready(self, phase='ready', partition=False, wait=None):
        wait = self.args.wait if wait is None else wait
        start = time.monotonic()
        deadline = start + wait
        consecutive = 0
        while time.monotonic() < deadline:
            self.sweep += 1
            jobs = []
            with ThreadPoolExecutor(max_workers=6) as pool:
                for origin in NODES:
                    if partition:
                        for target in NODES:
                            if target != origin:
                                reachable = (origin in NODES[:3]) == (target in NODES[:3])
                                jobs.append(pool.submit(self.probe, origin, [self.ids[target]], deadline, target, reachable))
                    else:
                        jobs.append(pool.submit(self.probe, origin,
                            [self.ids[n] for n in NODES if n != origin], deadline))
                passed = all([job.result() for job in jobs]) and time.monotonic() <= deadline
            consecutive = consecutive + 1 if passed else 0
            self.event(phase=phase, sweep=self.sweep, passed=passed, consecutive=consecutive)
            print(f'{phase}: sweep {self.sweep}: {consecutive}/3 complete', flush=True)
            if consecutive == 3:
                return
            time.sleep(min(2, max(0, deadline-time.monotonic())))
        raise RuntimeError(f'{phase}: no three complete traffic sweeps within {wait}s')

    def bridge(self):
        self.ready('baseline', wait=getattr(self.args, 'baseline_wait', 120))
        code, raw = self.run('bridge-container', [*self.compose, 'ps', '-q', 'node4'])
        container = raw.decode().strip()
        if code or not container:
            raise RuntimeError('node4 container missing')
        info = json.loads((self.artifacts/'inspect-node4.stdout').read_text())[0]
        networks = [n for n in info['NetworkSettings']['Networks'] if n.endswith('_seg_ab')]
        if len(networks) != 1:
            raise RuntimeError('bridge network not uniquely identified')
        network = networks[0]
        try:
            code, _ = self.run('bridge-disconnect', ['docker', 'network', 'disconnect', network, container])
            if code:
                raise RuntimeError('bridge fault not activated')
            code, raw = self.run('verify-disconnect', ['docker', 'inspect', container])
            if code or network in json.loads(raw)[0]['NetworkSettings']['Networks']:
                raise RuntimeError('bridge still connected after fault injection')
            self.ready('partition', partition=True, wait=getattr(self.args, 'outage_wait', 45))
        finally:
            # A signal can arrive after Docker applied the cut but before its
            # acknowledgement reaches us. Inspect reality even if disconnect
            # raised; never infer fault ownership solely from its return code.
            code, raw = self.run('bridge-cleanup-state', ['docker', 'inspect', container])
            if code:
                raise RuntimeError('cannot inspect bridge during cleanup')
            if network not in json.loads(raw)[0]['NetworkSettings']['Networks']:
                code, _ = self.run('bridge-reconnect', ['docker', 'network', 'connect', network, container])
                if code:
                    raise RuntimeError('bridge cleanup failed; inspect retained project')
        self.ready('recovery')


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['list', 'select', 'finish', 'preflight', 'ready', 'bridge'])
    parser.add_argument('--cases', default='smoke')
    parser.add_argument('--exit-code', type=int, default=0)
    parser.add_argument('--project')
    parser.add_argument('--compose-file', action='append', default=[])
    parser.add_argument('--artifacts')
    parser.add_argument('--binary')
    parser.add_argument('--wait', type=int, default=120)
    parser.add_argument('--baseline-wait', type=int, default=120)
    parser.add_argument('--outage-wait', type=int, default=45)
    parser.add_argument('--requests', type=int, default=4)
    parser.add_argument('--timeout-ms', type=int, default=2000)
    parser.add_argument('--concurrency', type=int, default=4)
    args = parser.parse_args()
    if args.command == 'list':
        print(json.dumps({'cases': CASES, 'profiles': PROFILES}, indent=2))
        return
    if args.command == 'select':
        print(' '.join(select_cases(args.cases)))
        return
    if not args.project or not args.artifacts or not args.compose_file or not all(1 <= value <= 360 for value in (args.wait, args.baseline_wait, args.outage_wait)) or not 1 <= args.requests <= 100 or not 1 <= args.concurrency <= 64 or not 1 <= args.timeout_ms <= 20000:
        parser.error('project, compose-file, artifacts and bounded wait/requests are required')
    if args.command == 'finish':
        root = Path(__file__).resolve().parents[2]
        artifacts = Path(args.artifacts)
        artifacts.mkdir(parents=True, exist_ok=True)
        rows = []
        path = artifacts/'network-cases.tsv'
        if path.exists():
            rows = [line.split('\t') for line in path.read_text().splitlines()]
        selected = args.cases.split()
        status = 'passed' if (args.exit_code == 0 and [row[0] for row in rows] == selected
                              and all(row[1] == '0' for row in rows)) else 'failed'
        (artifacts/'result.json').write_text(json.dumps({
            'status': status, 'exit_code': args.exit_code, 'selected': selected,
            'completed': [{'case': name, 'exit_code': int(code)} for name, code in rows],
            'project': args.project, 'binary': args.binary,
            'checkout_head': subprocess.check_output(['git','rev-parse','HEAD'], cwd=root, text=True).strip(),
            'binary_source_note': 'Prebuilt binary identity is recorded by hash; checkout revision is harness provenance, not a build attestation.'}, indent=2)+'\n')
        (artifacts/'source.patch').write_bytes(subprocess.check_output(['git','diff','--binary'], cwd=root))
        directory = artifacts/'harness'
        directory.mkdir(exist_ok=True)
        for path in ['docker/lib/network_checks.py', 'docker/test-cluster.sh',
                     'docker/test-bridge-recovery.sh', 'docker/test-file-history.py']:
            shutil.copyfile(root/path, directory/Path(path).name)
        return
    if (Path(args.artifacts)/'result.json').exists():
        parser.error('choose a fresh artifact directory; earlier results are immutable')
    checks = Checks(args)
    result = {'status': 'incomplete', 'command': args.command}
    try:
        if args.command in ('preflight', 'bridge'):
            checks.preflight()
        else:
            checks.identities()
        if args.command == 'ready':
            checks.ready()
        elif args.command == 'bridge':
            checks.bridge()
        result['status'] = 'passed'
    except KeyboardInterrupt as error:
        result.update(status='incomplete', error=str(error))
        raise
    except Exception as error:
        result.update(status='failed', error=str(error))
        raise
    finally:
        result['elapsed_seconds'] = time.monotonic()-checks.started
        (checks.artifacts/'result.json').write_text(json.dumps(result, indent=2)+'\n')


if __name__ == '__main__':
    main()
