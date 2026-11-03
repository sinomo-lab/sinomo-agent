#!/usr/bin/env python3
"""Summarize codec fixture samples with Sinomo's maintained percentile reducer."""
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
args = parser.parse_args()
summary = {'lanes': {}, 'cold': {}, 'coverage': {}}

for mode in ['images', 'images-full']:
    rows = [json.loads(line.split('IMAGE_CODEC_SAMPLE ', 1)[1])
            for line in (args.run / (mode + '.txt')).read_text().splitlines()
            if 'IMAGE_CODEC_SAMPLE ' in line]
    summary['coverage'][mode] = len(rows)
    for lane in sorted({row['lane'] for row in rows}):
        samples = [row for row in rows if row['lane'] == lane and not row['warmup']]
        assert len(samples) == 30, (lane, len(samples))
        item = {key: reporter.latency_summary([row[key] for row in samples])
                for key in ['encode_us', 'decode_us', 'encode_decode_us', 'source_gap_us']}
        item['mean_bytes'] = sum(row['bytes'] for row in samples) / len(samples)
        item['mean_luma_psnr_db'] = (sum(row['luma_psnr_db'] for row in samples) / len(samples)
                                     if 'luma_psnr_db' in samples[0] else None)
        summary['lanes'][lane] = item

for mode in ['run', 'sparse']:
    rows = [json.loads(line) for line in (args.run / (mode + '.jsonl')).read_text().splitlines()]
    assert not [row for row in rows if row['kind'] in ['failure', 'fatal']]
    assert rows[-1]['kind'] == 'summary' and rows[-1]['failures'] == 0
    summary['coverage'][mode] = sum(row['kind'] == 'sample' for row in rows)
    for lane in sorted({row['lane'] for row in rows if row['kind'] == 'sample'}):
        all_samples = [row for row in rows if row['kind'] == 'sample' and row['lane'] == lane]
        samples = [row for row in all_samples if not row['warmup']]
        if not samples:
            continue
        assert len(samples) == (5 if mode == 'sparse' else 30), (lane, len(samples))
        item = {key.replace('_ms', '_us'): reporter.latency_summary([row[key] * 1000 for row in samples])
                for key in ['encode_ms', 'decode_ms', 'encode_decode_ms', 'source_gap_ms']}
        item['mean_bytes'] = sum(row['bytes'] for row in samples) / len(samples)
        item['mean_luma_psnr_db'] = sum(row['luma_psnr_db'] for row in samples) / len(samples)
        item['keyframes'] = sum(row['keyframe'] for row in samples)
        item['codec_config_bytes'] = all_samples[0]['codec_config_bytes']
        summary['lanes'][lane] = item
    if mode == 'run':
        for codec in ['h264', 'h265']:
            cold = [row for row in rows if row['kind'] == 'sample' and row['frame'] == 0
                    and row['lane'].startswith(codec + '-1000000-independent-')
                    and 'gpu-scale' not in row['lane']]
            assert len(cold) == 4
            summary['cold'][codec] = reporter.latency_summary([row['cold_first_frame_ms'] * 1000 for row in cold])

(args.run / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
rows = []
for lane, item in summary['lanes'].items():
    rows.append([lane, item['encode_decode_us']['count'], item['encode_decode_us']['p50_us'] / 1000,
                 item['encode_decode_us']['p95_us'] / 1000, item['mean_bytes'], item['mean_luma_psnr_db']])
(args.run / 'TABLE.md').write_text('\n'.join(reporter.table(
    ['Lane', 'Measured frames', 'Encode+decode p50 ms', 'p95 ms', 'Mean payload bytes', 'Mean luma PSNR dB'], rows)) + '\n')
print(json.dumps(summary['coverage']))
