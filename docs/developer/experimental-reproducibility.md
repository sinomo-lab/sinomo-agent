# Experimental Reproducibility

This guide defines the minimum experiment record for Sinomo. Individual
benchmark READMEs can impose stricter dataset, containment, or reporting
requirements.

## Record the Source

Before building, capture:

```sh
git rev-parse HEAD
git status --short --branch
rustc --version --verbose
cargo --version
```

Record the lockfile and any local patch. A commit hash does not describe a dirty
worktree. If source is imported into Sinomo's managed rollout checkout, record
both the operator checkout and immutable source/build identity reported by the
build service.

Build with locked dependencies:

```sh
cargo build -p sinomo --locked
sha256sum target/debug/sinomo
```

Use the release profile for performance claims and state the profile
explicitly. Do not compare a debug binary with a release baseline. Docker
acceptance gates default to the host-built `target/debug/sinomo` described in
the [Docker guide](../../docker/README.md) and keep debugging, tracing, and
profiling enabled at unchanged thresholds. Record the profile with their results
and do not present debug-profile timings as release performance. DFS and
cluster performance reports follow the
[performance analysis skill](../../skills/sinomo-performance-analysis/SKILL.md).

## Isolate Runtime State

Every independent experimental condition should have an isolated data
directory, workspace, temporary root, and output directory:

```sh
export SINOMO_DATA_DIR=/absolute/path/to/experiment/state
```

Do not copy normal cluster identity, sessions, memory, skills, permissions, or
credentials into an evaluation unless the protocol explicitly requires them.
When a live provider is required, copy only the narrow credential/account state
allowed by that harness and record the procedure without publishing secrets.

For user- or question-level memory evaluations, never reuse state between
cases. Reuse can leak answer evidence and invalidate retrieval measurements.

## Record the Environment

At minimum, retain:

- operating system, kernel, architecture, and filesystem;
- CPU model/count, memory, accelerators, and relevant power limits;
- node count, node roles, consensus authority set, and Sinomo versions;
- storage medium, mount options, free space, and cache/warmup policy;
- network topology, bandwidth/latency shaping, relays, and failure injection;
- provider, account class, model identifier, endpoint, and model options;
- dataset name, upstream revision, size, digest, and selection filters;
- benchmark configuration, seeds, concurrency, limits, thresholds, and timeout;
- background workload and collection start/end timestamps in UTC.

Avoid publishing API keys, join invites, identity private keys, raw private
prompts, or personal workspace content.

## Formal Runs

The harness defaults encode pinned tool versions and deterministic seeds. Keep
the complete command and output:

```sh
QUINT_CASE_FILTER=raft-kv QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage

QUINT_CASE_FILTER=raft-kv QUINT_VERIFY_MAX_STEPS=5 \
  bash scripts/formal/check-quint.sh verify-fast
```

Report:

- evidence class: refinement, abstract contract, or target hardening;
- model/check/scenario files and main/step relation;
- Quint and Apalache versions;
- bounds, samples, seed, timeout, and backend;
- every required witness and its observed count;
- pass/failure and elapsed time;
- implementation anchors and matching Rust conformance tests;
- important omitted mechanics.

A safety pass with no witness coverage may describe an unreachable invariant.
A randomized witness is coverage evidence, not a symbolic proof. Report both
when both are part of the claim.

## Cluster Runs

For Docker or physical clusters, collect a before and after baseline:

```sh
sinomo cluster health
sinomo cluster doctor
sinomo dfs status --cluster --json
sinomo rollout status
```

Retain per-node stdout/stderr, topology, container/image hashes, configuration,
operation IDs, failures injected, restart points, and convergence criteria.
Do not report a multi-node result from only the initiating client's exit code.

Separate durable correctness runs from tmpfs/algorithm-only performance runs.
State whether caches are cold, warm, or intentionally retained and whether the
measured interval includes namespace, transfer, flush, quorum, repair, or
application startup work.

## Model and Agent Runs

Model-backed results are nondeterministic and provider behavior can change
without a repository commit. Preserve:

- exact prompts after template expansion, with privacy-safe redaction rules;
- tool/skill/agent package versions and context limits;
- provider/model/account class and reasoning/output options;
- conversation/session reset policy;
- retries, failures, quota/rate-limit events, and excluded cases;
- raw response and tool evidence needed by the evaluator;
- evaluator version, rubric, model, and blinding procedure;
- repeated-run variance and statistical method.

Separate retrieval quality from reader/answer quality. A retrieval harness that
produces hypotheses does not validate answer correctness unless the named
evaluator is also run.

## Result Bundle

A durable result bundle should contain:

```text
manifest.json        source, binary, data, environment, and command hashes
configuration.json   normalized experiment settings
events.jsonl         progress and structured failures
raw/                 immutable machine-readable per-case output
summary.json         aggregate metrics with sample counts and uncertainty
README.md             claim, protocol, deviations, and reproduction steps
```

Large or sensitive bundles can live in an external artifact store. Preserve a
content digest and access policy in the reviewed report. Do not commit ordinary
run output under `benchmark-results/`.

## Minimum Report Template

```text
Claim:
Commit and dirty state:
Binary/profile/hash:
Environment and topology:
Dataset/input revision and hash:
Provider/model/options, if any:
Exact command:
Warmup, repetitions, seed, and bounds:
Primary metric and uncertainty:
Baselines and ablations:
Failures and excluded cases:
Evidence artifact and digest:
Known limitations:
```

A result is ready to compare only when another developer or experiment author
can reconstruct its inputs and measurement boundary. A result is ready to
generalize only after the chosen baselines, repetitions, and environments
support that broader claim.
