#!/usr/bin/env python3
"""Measure persistent-client DFS RPC latency on an existing disposable cluster.

First build `cargo build -p sinomo-client --example attachment_latency` and pass
an explicit project already checked by docker/test-cluster.sh. This script
creates scoped DFS fixtures and retains them for inspection; it never resets a
cluster. Per-sample timers live inside the Rust client, excluding Docker exec,
CLI startup, fixture creation, and verification. Tracing must be disabled for
acceptance timings. Run identical arguments against each tested daemon binary.
"""
import argparse
import datetime
import json
import pathlib
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project', required=True)
    p.add_argument('--artifacts', type=pathlib.Path, required=True)
    p.add_argument('--node', choices=[f'node{i}' for i in range(1, 7)], default='node5')
    p.add_argument('--client', type=pathlib.Path, default=pathlib.Path('target/debug/examples/attachment_latency'))
    p.add_argument('--count', type=int, default=200)
    p.add_argument('--warmup', type=int, default=20)
    p.add_argument('--bytes', type=int, nargs='+', default=[128, 4096])
    p.add_argument('--modes', nargs='+', choices=['attachment', 'ordinary', 'reads'], default=['attachment', 'ordinary', 'reads'])
    p.add_argument('--timeout', type=int, default=1800)
    p.add_argument('--max-write-p95-us', type=int, default=100_000,
                   help='Write p95 acceptance target; 0 records timings without a latency gate')
    args = p.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    index = 0

    def run(label, command):
        nonlocal index
        index += 1
        stem = args.artifacts / f'{index:03d}-{label}'
        stem.with_suffix('.command.json').write_text(json.dumps(command))
        with stem.with_suffix('.stdout').open('wb') as out, stem.with_suffix('.stderr').open('wb') as err:
            subprocess.run(command, stdout=out, stderr=err, timeout=args.timeout, check=True)
        return stem.with_suffix('.stdout').read_text()

    containers = run('container', ['docker', 'ps', '--filter', f'label=com.docker.compose.project={args.project}',
                     '--filter', f'label=com.docker.compose.service={args.node}', '--format', '{{.ID}}']).split()
    if len(containers) != 1:
        raise RuntimeError('Expected one running container for the selected project/service')
    container = containers[0]
    run('identity', ['docker', 'exec', container, 'sh', '-c',
        'for p in /proc/[0-9]*; do if [ "$(cat "$p/comm" 2>/dev/null)" = sinomo ]; then sha256sum "$p/exe"; tr "\\000" " " < "$p/cmdline"; echo; fi; done'])
    run('client-copy', ['docker', 'cp', str(args.client.resolve()), f'{container}:/tmp/attachment_latency'])
    run_name = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S')
    report = {'status': 'running', 'project': args.project, 'node': args.node,
              'method': 'One persistent Rust client/runtime; per-RPC timings, setup and verification excluded',
              'cases': []}
    destination = args.artifacts / 'result.json'
    try:
        for mode in args.modes:
            for size in args.bytes:
                output = run(f'{mode}-{size}', ['docker', 'exec', container, '/tmp/attachment_latency',
                    str(args.count), str(args.warmup), str(size), f'{run_name}-{mode}-{size}', mode])
                records = [json.loads(line) for line in output.splitlines() if line.strip()]
                summaries = [r for r in records if r.get('summary')]
                if len(summaries) != 1 or not summaries[0].get('verified') or len(summaries[0]['samples_us']) != args.count:
                    raise RuntimeError('Incomplete or unverified workload')
                report['cases'].append(summaries[0])
                destination.write_text(json.dumps(report, indent=2) + '\n')
                print(mode, size, {k: summaries[0][k] for k in ['p50_us', 'p95_us', 'p99_us']}, flush=True)
        slow = [case for case in report['cases'] if case['mode'] != 'reads'
                and args.max_write_p95_us > 0 and case['p95_us'] > args.max_write_p95_us]
        report['max_write_p95_us'] = args.max_write_p95_us
        report['latency_target_met'] = not slow
        if slow:
            raise RuntimeError(f'{len(slow)} write workload(s) exceeded the p95 target')
        report['status'] = 'passed'
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        raise
    finally:
        destination.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
