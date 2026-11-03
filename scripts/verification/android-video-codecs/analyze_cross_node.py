#!/usr/bin/env python3
"""Validate and summarize real-node media_e2e video-latency records."""
import argparse
import importlib.util
import json
from pathlib import Path

repo = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('verification_report', repo / 'docker/verification_report.py')
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--run', type=Path, required=True)
parser.add_argument('--samples', type=int, default=105)
parser.add_argument('--lane', action='append', help='Lane name:transport, e.g. surface:h264; repeat for comparisons')
args = parser.parse_args()
root = args.run
best = [min(json.loads((root / f'clock-measure-{phase}.json').read_text()),
            key=lambda row: row['uncertainty_ns']) for phase in ['before', 'after']]
offset_us = sum(row['host_minus_phone_ns'] for row in best) / 2000
uncertainty_us = max(abs(row['host_minus_phone_ns'] / 1000 - offset_us)
                     + row['uncertainty_ns'] / 1000 for row in best) + 1000
summary = {'clock': {'host_minus_phone_us': offset_us, 'uncertainty_us': uncertainty_us,
                    'endpoint_offset_change_us': abs(best[0]['host_minus_phone_ns'] - best[1]['host_minus_phone_ns']) / 1000,
                    'assumption': 'Clock offset remains between the bracket observations; includes 1 ms source quantization.'},
           'lanes': {}}
rows = []
for lane_spec in args.lane or ['image:image', 'h264:h264']:
    lane, transport = lane_spec.split(':', 1)
    assert transport in ['image', 'h264'], transport
    records = [json.loads(line) for line in (root / f'cross-{lane}.jsonl').read_text().splitlines()]
    frames = [row for row in records if 'sample' in row]
    assert len(frames) == args.samples, (lane, len(frames), args.samples)
    assert [row['sample'] for row in frames] == list(range(args.samples))
    assert all(row['metadata']['transport'] == transport for row in frames)
    assert all((row['decoded_width'], row['decoded_height']) ==
               (row['metadata']['width'], row['metadata']['height']) for row in frames)
    assert all(a['metadata']['sequence'] < b['metadata']['sequence'] for a, b in zip(frames, frames[1:]))
    if transport == 'h264':
        assert [row['metadata']['packet_sequence'] for row in frames] == list(range(args.samples))
    measured = [row for row in frames if not row['warmup']]
    assert len(measured) == args.samples - 5
    ages = [row['ready_unix_us'] - row['metadata']['captured_at_unix_ms'] * 1000 - offset_us for row in measured]
    assert min(ages) >= -uncertainty_us, 'Clock correlation does not bound negative frame age'
    age = reporter.latency_summary(ages)
    gaps = reporter.latency_summary([row['gap_ms'] * 1000 for row in measured])
    item = {'capture_formats': sorted({row['metadata'].get('capture_format') or 'unreported' for row in frames}),
            'output_dimensions': sorted({(row['decoded_width'], row['decoded_height']) for row in frames}),
            'native_dimensions': sorted({(row['metadata']['source_width'], row['metadata']['source_height']) for row in frames}),
            'frames': len(frames), 'warmup': 5, 'measured': len(measured),
            'startup_ms': frames[0]['elapsed_ms'], 'acquisition_to_pixels_us': age,
            'gap_us': gaps, 'mean_encoded_payload_bytes': sum(row['wire_bytes'] for row in measured) / len(measured),
            'decode_us': reporter.latency_summary([row['video_decode_us'] + row['image_decode_us'] for row in measured]),
            'delivered_fps': (len(measured) - 1) * 1000 / (measured[-1]['elapsed_ms'] - measured[0]['elapsed_ms']),
            'p95_age_upper_bound_ms': (age['p95_us'] + uncertainty_us) / 1000,
            'startup_under_3000_ms': frames[0]['elapsed_ms'] < 3000,
            'gap_p95_under_500_ms': gaps['p95_us'] < 500000}
    # Frame age and decoding overlap with capture; they are not serial API throughput.
    for metric in ['acquisition_to_pixels_us', 'gap_us', 'decode_us']:
        item[metric].pop('serial_api_ops_per_second', None)
    summary['lanes'][lane] = item
    rows.append([lane, len(measured), item['startup_ms'], age['p50_us'] / 1000,
                 age['p95_us'] / 1000, age['p99_us'] / 1000, age['max_us'] / 1000,
                 gaps['p95_us'] / 1000, item['delivered_fps'], item['mean_encoded_payload_bytes']])
(root / 'cross-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
(root / 'CROSS_TABLE.md').write_text('\n'.join(reporter.table(
    ['Lane', 'Measured frames', 'Stream startup ms', 'Age p50 ms', 'p95 ms', 'p99 ms', 'max ms',
     'Gap p95 ms', 'Delivered fps', 'Mean encoded payload B'], rows)) + '\n')
print((root / 'CROSS_TABLE.md').read_text())
print('Clock uncertainty ms:', uncertainty_us / 1000)
