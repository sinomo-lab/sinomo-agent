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
When the system Python is newer than the SWE-bench package supports, use a repo-local Python 3.11 environment:

```sh
uv venv --python python3.11 target/tmp/swebench-venv
uv pip install --python target/tmp/swebench-venv/bin/python swebench
```

Evolution benchmark gates use `SINOMO_SWEBENCH_PYTHON` when it is set. If it is not set, they automatically prefer `target/tmp/swebench-venv/bin/python` when that file exists, then fall back to `python3`.

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
  --limit 1 \
  --max-steps 20 \
  --summary-out benchmark-results/swebench/summary.json \
  --progress-out benchmark-results/swebench/progress.jsonl
```

The Sinomo path prepares local Git checkouts, runs `sinomo-swebench-agent` inside each checkout with local workspace tools, writes per-instance reports, full `*.run.json` traces, live agent `*.trace.jsonl` records, post-patch syntax-check results, and bounded repair-pass counters under `benchmark-results/swebench/sinomo`, and writes `predictions.jsonl` in SWE-bench prediction format. The repair pass is used when a patch fails the post-patch syntax check or when the initial pass exhausts its step budget without leaving a patch. For Python indentation syntax failures, the harness also runs a narrow deterministic cleanup that restores base-file indentation on changed lines before rerunning the syntax check.

Use `--limit 1` for a first smoke run or `--instance-id <id>` to rerun a specific checked-in instance.
Use `--progress-out` to write newline-delimited JSON progress events while the run is active.
Use `--repair-steps 0` to disable the bounded repair turn when a post-patch syntax check fails.

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
When `--evaluate` is used, SWE-bench summary files and per-instance logs are written under `<results-dir>/evaluation`; run the Python runner through the repo-local venv if SWE-bench is installed there.
