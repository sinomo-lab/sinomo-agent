# SWE-bench Verified Benchmark

This directory contains the initial SWE-bench support for Sinomo's coding agent harness.

The checked-in `verified_10.json` suite selects 10 tasks from SWE-bench Verified by taking the first task from each of the first 10 distinct repositories in the official `princeton-nlp/SWE-bench_Verified` test split. This avoids a one-repository smoke test while staying small enough for provider and harness iteration.

## Setup

```sh
python3 -m pip install mini-swe-agent swebench
cargo build -p sinomo-benchmarks --bin sinomo-swebench-agent
sinomo --local providers test deepseek
```

Docker is required for official SWE-bench evaluation and for the mini-SWE-agent baseline.

## Plan

```sh
python3 crates/sinomo-benchmarks/swebench/run_swebench_verified.py --mode plan
```

## Sinomo Harness Run

```sh
python3 crates/sinomo-benchmarks/swebench/run_swebench_verified.py \
  --mode sinomo \
  --provider deepseek \
  --model deepseek-v4-flash \
  --max-steps 20
```

The Sinomo path prepares local Git checkouts, runs `sinomo-swebench-agent` inside each checkout with local workspace tools, writes per-instance reports under `benchmark-results/swebench/sinomo`, and writes `predictions.jsonl` in SWE-bench prediction format.

## mini-SWE-agent Baseline

```sh
python3 crates/sinomo-benchmarks/swebench/run_swebench_verified.py \
  --mode mini \
  --baseline-model deepseek/deepseek-v4-flash \
  --workers 2
```

The baseline delegates to `mini-extra swebench --subset verified --split test --filter <suite-regex>` so it uses mini-SWE-agent's normal SWE-bench Docker workflow on the same 10 instance IDs.

## Evaluation

After generating Sinomo predictions:

```sh
python -m swebench.harness.run_evaluation \
  --dataset_name princeton-nlp/SWE-bench_Verified \
  --predictions_path benchmark-results/swebench/sinomo/predictions.jsonl \
  --max_workers 2 \
  --run_id sinomo-deepseek-v4-flash-verified-10
```

The Python runner can run this command after the Sinomo pass with `--evaluate`.
