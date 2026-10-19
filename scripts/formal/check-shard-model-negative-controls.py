#!/usr/bin/env python3
"""Exercise deliberately invalid transitions in the whole-shard target model.

These are model sensitivity checks, never native Rust conformance evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
MODEL = Path('specs/quint/models/dfs/whole_shard.qnt')
CHECK = Path('specs/quint/checks/dfs/whole_shard_safety.qnt')
CONTRACT = Path('specs/quint/contracts/dfs/whole_shard_authority.qnt')
SEED = '0x51a020261006'
CONTROLS = {
    'content_w_without_metadata_authority': (
        'publicationProofAllowed(s.profile.standalone, s.primary, s.members,\n        if (s.profile.standalone) Set() else s.acks.get(op)),', 'true,',
        ['stage(1, 1)', 'prepare(1)', 'admitCommit(1)', 'commit(1, digest(1, 1), false)']),
    'unilateral_handoff_activation': (
        's.pending, s.frozen, jointProof,',
        's.pending, s.frozen,',
        ['beginHandoff(false)', 'freezeRetiring', 'acknowledgeHandoff(1)', 'activate(false)']),
    'metadata_majority_without_full_assignment': (
        'publicationProofAllowed(s.profile.standalone, s.primary, s.members,\n        if (s.profile.standalone) Set() else s.acks.get(op)),', 'true,',
        ['stage(1, 1)', 'prepare(1)', 'changeConnectivity(Set(1, 2))', 'acknowledgePrepare(1, 2)',
         'admitCommit(1)', 'commit(1, digest(1, 1), false)']),
    'frozen_writer_admission': (
        'not(s.observedPending), not(s.frozen) or (s.activated and s.bound.get(s.primary) == s.epoch),',
        'true,',
        ['stage(1, 1)', 'prepare(1)', 'acknowledgePrepare(1, 2)', 'acknowledgePrepare(1, 3)',
         'beginHandoff(false)', 'freezeRetiring', 'admitCommit(1)']),
    'mismatched_commit_digest': (
        'suppliedDigest == p.digest, s.guardOp == op, writable, current(p),',
        's.guardOp == op, writable, current(p),',
        ['stage(1, 1)', 'prepare(1)', 'acknowledgePrepare(1, 2)', 'acknowledgePrepare(1, 3)',
         'admitCommit(1)', 'commit(1, digest(1, 2), false)']),
}


def driver(actions: list[str]) -> str:
    lines = [
        'module dfs_whole_shard_negative_scenario {',
        '  import dfs_whole_shard from "../../models/dfs/whole_shard"',
        '  import dfs_whole_shard_safety from "../../checks/dfs/whole_shard_safety"',
        '  var phase: int',
        "  action init = all { dfs_whole_shard::init, phase' = 0 }",
        '  action step =',
    ]
    for phase, action in enumerate(actions):
        action = action.replace('digest(', 'dfs_whole_shard::digest(')
        prefix = 'if' if phase == 0 else '} else if'
        lines += [f'    {prefix} (phase == {phase}) {{',
                  f"      all {{ dfs_whole_shard::{action}, phase' = phase + 1 }}"]
    lines += ["    } else { all { dfs_whole_shard::stutter, phase' = phase } }",
              '  def safety: bool = dfs_whole_shard_safety::safety', '}']
    return '\n'.join(lines) + '\n'


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, help='optional artifact report; no generated repository files')
    parser.add_argument('--timeout', type=int, default=60, help='seconds per Quint invocation')
    args = parser.parse_args()
    version = subprocess.check_output(['quint', '--version'], text=True).strip()
    if version != '0.32.0':
        raise SystemExit(f'Expected Quint 0.32.0; observed {version}')
    model = (ROOT / MODEL).read_text()
    checks = (ROOT / CHECK).read_text()
    results = []
    with tempfile.TemporaryDirectory(prefix='sinomo-whole-shard-negative-') as temporary:
        scratch = Path(temporary)
        (scratch / MODEL).parent.mkdir(parents=True)
        (scratch / CHECK).parent.mkdir(parents=True)
        scenario = scratch / 'specs/quint/scenarios/dfs/whole_shard_negative.qnt'
        scenario.parent.mkdir(parents=True)
        (scratch / CHECK).write_text(checks)
        (scratch / CONTRACT).parent.mkdir(parents=True)
        (scratch / CONTRACT).write_bytes((ROOT / CONTRACT).read_bytes())
        for name, (original, replacement, actions) in CONTROLS.items():
            if model.count(original) != 1:
                raise SystemExit(f'Expected exactly one {name} mutation site; model changed')
            (scratch / MODEL).write_text(model.replace(original, replacement, 1))
            scenario.write_text(driver(actions))
            command = ['quint', 'run', str(scenario), '--main=dfs_whole_shard_negative_scenario',
                       '--backend=typescript', '--invariant=safety', '--max-samples=1',
                       f'--max-steps={len(actions) + 2}', f'--seed={SEED}', '--verbosity=1']
            try:
                run = subprocess.run(command, text=True, capture_output=True, timeout=args.timeout)
                output = run.stdout + run.stderr
                passed = run.returncode == 1 and '[violation]' in output and 'Invariant violated' in output
                result = {'name': name, 'passed': passed, 'exitCode': run.returncode, 'output': output}
            except subprocess.TimeoutExpired:
                result = {'name': name, 'passed': False, 'exitCode': 124, 'output': 'Quint timed out'}
            results.append(result)
            print(f"{'ok' if result['passed'] else 'FAILED'} expected counterexample: {name}", flush=True)
    report = {'classification': 'model negative controls; not Rust conformance',
              'quintVersion': version, 'seed': SEED,
              'modelSha256': hashlib.sha256((ROOT / MODEL).read_bytes()).hexdigest(),
              'checksSha256': hashlib.sha256((ROOT / CHECK).read_bytes()).hexdigest(),
              'allExpectedViolations': all(item['passed'] for item in results), 'results': results}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['allExpectedViolations'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
