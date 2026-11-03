#!/usr/bin/env python3
"""Run an installed disposable codec fixture and retain bounded model-free comparisons."""
import argparse
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--serial', required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--modes', nargs='+', default=['images-full', 'images', 'run', 'sparse'],
                    choices=['images-full', 'images', 'images-baseline', 'run', 'sparse'])
parser.add_argument('--timeout', type=float, default=150)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
adb = ['adb', '-s', args.serial]
package = 'net.sinomo.codecs.probe'
manifest = {'serial': args.serial, 'modes': args.modes, 'outcomes': {}}

def call(*command, check=True):
    return subprocess.run(adb + list(command), check=check, capture_output=True, text=True, timeout=15)

try:
    for mode in args.modes:
        call('shell', 'am', 'force-stop', package)
        call('shell', 'run-as', package, 'rm', '-f', 'files/comparison/results.jsonl')
        launch = call('shell', 'am', 'start', '-n', package + '/net.sinomo.probe.CodecActivity',
                      '--es', 'mode', mode, '--ei', 'frames', '10' if mode == 'sparse' else '35')
        (args.output / (mode + '-launch.txt')).write_text(launch.stdout + launch.stderr)
        deadline = time.monotonic() + args.timeout
        while True:
            result = call('exec-out', 'run-as', package, 'cat', 'files/comparison/results.jsonl', check=False)
            rows = []
            if result.returncode == 0:
                (args.output / (mode + '.jsonl')).write_text(result.stdout)
                for line in result.stdout.splitlines():
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # The producer may be completing the last line.
            fatal = [row for row in rows if row['kind'] == 'fatal']
            summary = [row for row in rows if row['kind'] in ('summary', 'image-complete')]
            if fatal:
                raise RuntimeError(f'{mode}: {fatal[-1]}')
            if summary:
                verdict = summary[-1]
                manifest['outcomes'][mode] = verdict
                if verdict.get('failures', 0) or verdict.get('exit_code', 0):
                    raise RuntimeError(f'{mode}: {verdict}')
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{mode}: fixture did not complete within {args.timeout}s')
            time.sleep(.5)
        if mode.startswith('images'):
            result = call('exec-out', 'run-as', package, 'cat', f'files/comparison/{mode}.txt')
            (args.output / (mode + '.txt')).write_text(result.stdout)
        print(f'{mode}: complete', flush=True)
finally:
    call('shell', 'am', 'force-stop', package, check=False)
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
