# Agent memory benchmarks

The agent benchmark package contains complementary memory evaluations:

- `run_memory_eval.py` is the small, checked-in synthetic evaluation. It is
  useful for fast fixture checks and model-facing prompt behavior.
- `run_longmemeval_s.py` runs the cleaned LongMemEval-S corpus through either
  Sinomo's indexed-file path or the explicit history-search and memory-note
  tools, with an optional agent reader for the indexed-file path.

LongMemEval-S is an external dataset and is not vendored in this repository.
Benchmark outputs belong under the gitignored `benchmark-results/` directory.

## LongMemEval-S

The default LongMemEval-S run is retrieval-only. It does not call a model or
copy provider configuration. Passing `--reader sinomo` adds an optional reader
run and produces hypotheses for the upstream LongMemEval answer evaluator.

### Inspect the pinned plan

Plan mode performs no download, indexing, or model call:

```bash
python3 modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py \
  --mode plan
```

The plan records the dataset revision, byte count, SHA-256 digest, retrieval
limits, result schema, and containment contract.

### Acquire the dataset explicitly

The runner never downloads data implicitly. Acquire the exact cleaned dataset
revision into a gitignored location:

```bash
mkdir -p benchmark-results/datasets
curl --location --fail --show-error \
  --output benchmark-results/datasets/longmemeval_s_cleaned.json \
  https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/98d7416c24c778c2fee6e6f3006e7a073259d48f/longmemeval_s_cleaned.json
```

The pinned artifact is:

```text
revision  98d7416c24c778c2fee6e6f3006e7a073259d48f
bytes     277383467
sha256    d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442
```

The run command verifies both byte count and digest before creating a benchmark
case. An intentional local fixture or alternate dataset revision requires
`--allow-unpinned-dataset`; results from that mode must not be reported as the
pinned LongMemEval-S baseline.

### Retrieval-only baseline

Build the CLI used by the benchmark, then start with the fixed development
subset:

```bash
cargo build -p sinomo
python3 modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py \
  --mode run \
  --dataset benchmark-results/datasets/longmemeval_s_cleaned.json \
  --reader none \
  --subset development
```

The development subset contains two fixed, non-abstention questions from each
official question category, so repeated debugging runs cover a stable mix
without paying for all 500 cases:

| Category | Question IDs |
|---|---|
| `single-session-user` | `e47becba`, `118b2229` |
| `single-session-assistant` | `7161e7e2`, `c4f10528` |
| `single-session-preference` | `8a2466db`, `06878be2` |
| `temporal-reasoning` | `gpt4_59149c77`, `gpt4_f49edff3` |
| `knowledge-update` | `6a1eabeb`, `6aeb4375` |
| `multi-session` | `0a995998`, `6d550036` |

The preset is intentionally fixed and cannot be combined with explicit IDs,
type filters, or `--limit`. Use those existing selectors for ad hoc runs;
`--limit` continues to select a deterministic dataset prefix.

Omit `--subset development` for all 500 questions. Selection is deterministic
and can also be narrowed with repeatable `--question-id ID` and
`--question-type TYPE` arguments. The supported question types are printed by
`--help`.

The canonical retrieval-only baseline is therefore:

```bash
python3 modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py \
  --mode run \
  --dataset benchmark-results/datasets/longmemeval_s_cleaned.json \
  --reader none
```

Retrieval uses one indexed source file per history session and one dated,
role-labelled lexical anchor per turn. Gold answers, evidence-session labels,
and `has_answer` labels are kept outside the indexed corpus. The report includes
session- and turn-level MRR, any/all recall, and nDCG at the configured cutoffs.
The `recall_any`, `recall_all`, and `ndcg_any` calculations intentionally match
LongMemEval's reference retrieval evaluator, including its rank-discount
convention, so those fields are directly comparable with upstream retrieval
logs. MRR is an additional diagnostic.
Session relevance comes from `answer_session_ids`; turn relevance comes from
the independently supplied `has_answer` labels, which are not complete for
every gold session in the cleaned dataset. The harness does not infer missing
turn labels from session labels. As in the upstream benchmark, abstention
questions are excluded from retrieval scoring.

The default indexed-file retrieval budget is 50 records and 1 MiB. Use
`--max-records` and `--max-bytes` to create a separately named operating point;
record those values when comparing results. The memory-tools baseline uses the
same `--max-records` value to bound each reported ranking; `--max-bytes` is
specific to indexed-file evidence.

### Memory-tools baseline

The memory-tools baseline uses the same cases, gold labels, development subset,
and retrieval metrics. It seeds one typed background activity per turn and one
Markdown note per session, then calls the production `session_history_search`,
`memory_write`, and `memory_search` tools. All storage is a fresh in-memory VFS,
so this baseline does not require DFS, SIFS, a daemon, or operator state.

```bash
cargo build --release -p sinomo-benchmarks \
  --features memory-tools-bench \
  --bin sinomo-memory-tools-bench
python3 modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py \
  --mode run \
  --retriever memory-tools \
  --dataset benchmark-results/datasets/longmemeval_s_cleaned.json \
  --reader none \
  --subset development
```

The driver derives a bounded deterministic set of query terms from each
question. Reports retain separate history-search and memory-search rankings as
well as a combined ranking ordered by query-term coverage, source coverage, and
reciprocal-rank fusion. Use the combined ranking for the direct indexed-file
comparison, and the per-source rankings to diagnose which tool contributed.
The memory-tools path is retrieval-only; `--reader sinomo` is intentionally
rejected because that reader consumes indexed-file evidence.

### Optional Sinomo reader

The reader uses the same isolated index created for the question and records
the exact evidence that reached the model, after the agent's own context limits:

```bash
python3 modules/benchmark/sinomo-benchmarks/agent/run_longmemeval_s.py \
  --mode run \
  --dataset benchmark-results/datasets/longmemeval_s_cleaned.json \
  --reader sinomo \
  --provider codex \
  --model gpt-5.2-codex \
  --limit 10
```

Use `--account NAME` when selecting a configured provider account. The provider
configuration source defaults to the operator's current `SINOMO_DATA_DIR`, or
`~/.sinomo` when that variable is unset. It can be selected explicitly with
`--config-source-data-dir PATH`. This path is a read-only source for the narrow
copy allowlist described below; child processes never use it as their data
directory.

Environment-backed provider credentials are not inherited automatically. Pass
an individual required variable explicitly, for example
`--pass-env OPENAI_API_KEY`. The option is repeatable. Sinomo data-directory,
provider, credential-path, agent-home, workspace, sandbox, cluster, daemon, and
permission variables are all rejected: every `SINOMO_*` child setting is owned
by the containment harness. Path-bearing overrides such as `CODEX_HOME`,
`AWS_CONFIG_FILE`, and `PYTHONPATH` are also rejected; use copied provider state
or value-only API-key and endpoint variables.

Reader mode writes upstream-compatible JSONL records of the form
`{"question_id":"...","hypothesis":"..."}`. Answer correctness is deliberately
not guessed by this harness. Evaluate `predictions.jsonl` with a separately
pinned checkout of the official
[LongMemEval evaluator](https://github.com/xiaowu0162/longmemeval); its LLM judge
dependencies and credentials are independent of the Sinomo run.

## Outputs

The default output directory is
`benchmark-results/agent-memory/longmemeval-s/`:

- `summary.json` records the dataset and binary hashes, Git revision,
  configuration, aggregate retrieval and timing metrics, and failures.
- `retrieval.jsonl` contains one detailed retrieval record per question,
  including ranked session/turn IDs, coverage, query trace, indexing/query
  timing, and the containment manifest.
- `predictions.jsonl` is created only with `--reader sinomo` and contains the
  official hypothesis interchange records.
- `progress.jsonl` is an append-style event stream for monitoring a long run.

Use `--summary-out`, `--retrieval-out`, `--predictions-out`, and
`--progress-out` to choose different paths. Each new run initializes its JSONL
outputs, so concurrent or comparative runs must use distinct output paths.
`--fail-fast` stops after the first failed case; otherwise failures are recorded
and the remaining cases continue. A `passed` run status means the harness
completed without execution or data-contract failures; retrieval quality is
reported as metrics and is not converted into an arbitrary pass threshold.
With `--retriever memory-tools`, the default directory is
`benchmark-results/agent-memory/memory-tools-longmemeval-s/` and the detailed
records additionally contain per-tool rankings, query terms, call counts, and
seeded-object counts.

## Containment contract

Every LongMemEval question represents a different synthetic user. Reusing
normal Sinomo state, or even state from another question, would both bias the
measurement and contaminate operational history. The harness therefore applies
the following fail-closed contract to every case:

1. It creates a fresh disposable root containing an independent
   `SINOMO_DATA_DIR`, corpus, working directory, `HOME`, XDG directories, and
   temporary directory. No case root is reused.
2. Sinomo subprocesses run from the disposable workspace with an allowlisted
   environment. They cannot discover the operator's data directory through an
   inherited Sinomo variable.
3. Retrieval-only mode copies nothing from the operator's Sinomo directory.
   Reader mode may copy only these regular, non-symlink provider files:

   ```text
   configs/lm/providers.json (rewritten to the selected --account only)
   configs/lm/credentials/<selected-provider>.json
   or providers/<selected-provider>/credentials.json (legacy fallback)
   ```

   The provider configuration is not copied at all when `--account` is absent;
   unrelated providers and accounts (including inline API keys) never enter a
   case. Token refreshes therefore modify only the disposable credential copy.
4. Cluster membership, node identity, DFS/SIFS state, local permission grants,
   chats, channels, sessions, activities, traces, memory sources, indexes,
   agent homes, skills, and sandbox state are never copied. In particular, the
   temporary node has no remote-management or unsafe-tool permission grant.
5. Corpus files contain history turns only. The standalone question, answer
   field, `answer_session_ids`, and `has_answer` labels remain out of band, so
   retrieval cannot see the answer key as benchmark metadata.
6. Cleanup runs after every case, including command failure and timeout. A
   sentinel and path-overlap checks prevent cleanup from targeting the dataset,
   result files, configuration source, or any non-generated directory.

`--scratch-root PATH` may place disposable roots on a faster or larger local
filesystem, but it must not overlap the operational Sinomo data directory.
Disposable roots are intentionally not retainable; use the structured outputs
and progress log for diagnosis.
