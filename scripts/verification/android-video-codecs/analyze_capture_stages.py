#!/usr/bin/env python3
"""Join received frames to source Observe/Android timing records; preserve missing coverage."""
import argparse
import importlib.util
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--frames', type=Path, required=True)
parser.add_argument('--traces', type=Path, required=True)
parser.add_argument('--native-log', type=Path)
parser.add_argument('--clock-before', type=Path, required=True)
parser.add_argument('--clock-after', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('reporter', repo / 'docker/verification_report.py')
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)

def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]

def key(row):
    return row['sequence'], row['captured_at_unix_ms']

brackets = []
for path in [args.clock_before, args.clock_after]:
    candidates = [row for row in lines(path) if row['response_bytes'] == 0 and not row['warmup']]
    row = min(candidates, key=lambda r: r['completed_unix_ns'] - r['sent_unix_ns'])
    brackets.append(((row['sent_unix_ns'] - row['remote_observed_unix_ns']) / 1000,
                     (row['completed_unix_ns'] - row['remote_observed_unix_ns']) / 1000))
lower = min(bound[0] for bound in brackets)
upper = max(bound[1] for bound in brackets)
offset = (lower + upper) / 2
uncertainty = (upper - lower) / 2
trace = json.loads(args.traces.read_text())
source = {}
for record in trace['records']:
    row = record.get('fields', {})
    if row.get('message') == 'source frame stages':
        assert key(row) not in source, 'ambiguous source frame identity'
        source[key(row)] = row
native = {}
if args.native_log:
    for row in lines(args.native_log):
        if 'sequence' in row and 'captured_at_unix_ms' in row:
            native[row.get('kind', 'java'), key(row)] = row
frames = [row for row in lines(args.frames) if 'sample' in row and not row['warmup']]
values, missing = {}, []
for frame in frames:
    identity = key(frame['metadata'])
    if identity not in source:
        missing.append(identity)
        continue
    row = source[identity]
    stage = {
        'acquisition_to_native_us': row['native_ready_unix_us'] - identity[1] * 1000,
        'pull_us': row['pull_us'],
        'encode_us': row['encode_us'],
        'acquisition_to_encoded_us': row['encoded_ready_unix_us'] - identity[1] * 1000,
        'post_encode_delivery_us': frame['wire_received_unix_us'] - offset - row['encoded_ready_unix_us'],
        'receiver_work_us': frame['ready_unix_us'] - frame['wire_received_unix_us'],
        'acquisition_to_pixels_us': frame['ready_unix_us'] - offset - identity[1] * 1000,
    }
    for kind, fields in {
        'java': ['copy_us', 'java_queue_us', 'socket_write_us'],
        'capture': ['read_us', 'normalize_us', 'publish_us'],
        'module': ['call_next_us', 'prepare_us', 'serialize_us', 'host_callback_us'],
    }.items():
        if (kind, identity) in native:
            stage.update({kind + '.' + field: native[kind, identity][field] for field in fields})
    for name, value in stage.items():
        values.setdefault(name, []).append(value)
summary = {name: reporter.latency_summary(samples) for name, samples in values.items()}
for item in summary.values():
    item.pop('serial_api_ops_per_second', None)
result = {
    'clock': {'host_minus_source_us': offset, 'uncertainty_us': uncertainty,
              'frame_timestamp_quantization_us': 1000,
              'assumption': 'Offset stays inside the before/after RPC brackets; no clock step.'},
    'coverage': {'expected_measured_frames': len(frames), 'matched': len(frames) - len(missing),
                 'missing_source_frames': missing, 'source_trace_process': trace['process'],
                 'dropped_records': trace.get('dropped_records'), 'truncated': trace.get('truncated')},
    'stages': summary,
    'notes': ['Stages overlap; do not sum their percentiles.',
              'Post-encode delivery includes source queues, RPC/proxying and receiver record parsing, not just USB.',
              'Android trace node_id may be unavailable; retain independent daemon/PID binding evidence.'],
}
args.output.write_text(json.dumps(result, indent=2) + '\n')
print('\n'.join(reporter.table(['Stage', 'Samples', 'p50 ms', 'p95 ms', 'max ms'],
    [[name, item['count'], item['p50_us']/1000, item['p95_us']/1000, item['max_us']/1000]
     for name, item in summary.items()])))
if missing:
    raise SystemExit('incomplete source frame coverage')
