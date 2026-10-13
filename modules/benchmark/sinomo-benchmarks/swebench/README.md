# SWE-bench Verified Benchmark

This directory contains the initial SWE-bench support for Sinomo's coding agent harness.

The checked-in `verified_10.json` suite selects 10 representative tasks from SWE-bench Verified. It keeps 10 distinct repositories, covers Django, SymPy, Sphinx, scientific Python, testing/linting, and HTTP-client behavior, and mirrors the Verified difficulty distribution at this size with 4 easy tasks, 5 medium tasks, and 1 harder task. The cases were selected on 2026-06-04 from the official `princeton-nlp/SWE-bench_Verified` test split with moderate patch and test sizes where possible, so the suite stays useful for repeated provider and harness iteration rather than only final leaderboard evaluation.

## Setup

```sh
python3 -m pip install mini-swe-agent swebench
sinomo providers test deepseek
```

The provider check uses the running main daemon; the removed `--local` flag is
rejected.

Docker is required for official SWE-bench evaluation and for the mini-SWE-agent baseline.
When the system Python is newer than the SWE-bench package supports, use a repo-local Python 3.11 environment:

```sh
uv venv --python python3.11 target/tmp/swebench-venv
uv pip install --python target/tmp/swebench-venv/bin/python swebench
```

Evolution benchmark gates use `SINOMO_SWEBENCH_PYTHON` when it is set. If it is not set, they automatically prefer `target/tmp/swebench-venv/bin/python` when that file exists, then fall back to `python3`.

The single-node compose file builds the Sinomo image with `SINOMO_BENCH_DEPS=1`, which installs the `swebench` Python package and Docker CLI for benchmark evaluation, and mounts `/var/run/docker.sock` so the official harness can launch its per-instance containers. If `--evaluate` is requested without those prerequisites, the runner writes `<results-dir>/evaluation/blocked.json` and records `official_evaluation.parse_status = "blocked"` in the summary instead of failing with an ambiguous import or Docker error.

## Plan

```sh
python3 modules/benchmark/sinomo-benchmarks/swebench/run_swebench_verified.py --mode plan
```

## Sinomo Harness Run

```sh
python3 modules/benchmark/sinomo-benchmarks/swebench/run_swebench_verified.py \
  --mode sinomo \
  --provider deepseek \
  --model deepseek-v4-flash \
  --limit 1 \
  --summary-out benchmark-results/swebench/summary.json \
  --progress-out benchmark-results/swebench/progress.jsonl
```

The Sinomo path prepares local Git checkouts, runs `sinomo-swebench-agent` inside each checkout with local workspace tools, writes per-instance reports, full `*.run.json` traces, live agent `*.trace.jsonl` records, post-patch syntax-check results, and bounded repair-pass counters under `benchmark-results/swebench/sinomo`, and writes `predictions.jsonl` in SWE-bench prediction format. By default the main agent pass has no fixed model-turn cap; pass `--max-steps <n>` only when a benchmark run intentionally needs a finite turn cap. The repair pass is used when a patch fails the post-patch syntax check or when an explicitly capped initial pass exhausts its step budget without leaving a patch. For Python indentation syntax failures, the harness also runs a narrow deterministic cleanup that restores base-file indentation on changed lines before rerunning the syntax check.

By default, the runner rebuilds `target/debug/sinomo-swebench-agent` with `cargo build -p sinomo-benchmarks --bin sinomo-swebench-agent` before every Sinomo benchmark run so retries cannot silently use a stale agent binary. If `--sinomo-agent-bin <path>` is supplied, that explicit binary is used without rebuilding and its path, size, mtime, and SHA-256 are recorded in the benchmark summary.

If an agent subprocess fails before writing a prediction, the runner writes a per-instance report with `status: "harness-blocked"` and continues with the next instance. Official SWE-bench evaluation is blocked when predictions are incomplete so provider, credential, binary, or host-environment failures do not get counted as model outcomes.

Use `--limit 1` for a first smoke run or `--instance-id <id>` to rerun a specific checked-in instance.
Use `--progress-out` to write newline-delimited JSON progress events while the run is active.
Use `--max-steps <n>` to opt into a finite model-turn cap for the main pass.
Use `--repair-steps 0` to disable the bounded repair turn when a post-patch syntax check fails.
Plan and summary JSON include a `host_dependencies` probe for command availability, Python imports, Docker reachability, agent-binary status, and blocking host issues. Plan mode records those issues without failing; `sinomo`, `mini`, and `both` modes stop before launching subprocesses when the selected run/evaluation path has blocking host issues, write `harness_status: "host-dependency-blocked"` in the summary, and emit a `host-dependency-blocked` progress event. Per-instance reports include combined `agent_model_metrics`, compact coding-state labels, the first successful patch turn when a patch was applied, session event/item counts, tool call/result item counts, and orphan tool call/result counts; suite summaries aggregate parse failures, model repair attempts, strict/text/native/vendor decision paths, validation status counts, stop-recommendation counts, validation failure-kind counts, repair token usage, first-patch-turn min/max/sum, orphan tool-pair counts, and final decision-source counts for calibration.

## mini-SWE-agent Baseline

```sh
python3 modules/benchmark/sinomo-benchmarks/swebench/run_swebench_verified.py \
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
