# Research and Evaluation

Sinomo is both a working runtime and a research platform for local-first,
distributed, continuously operating AI systems. This developer-facing guide
helps contributors and experiment authors separate implemented mechanisms,
architectural requirements, executable evidence, and open research questions.

For a non-code introduction, read the [system overview](../system-overview.md).
For normative boundaries, read the [architecture overview](../architecture.md).
For experiment reporting, use the
[experimental reproducibility guide](experimental-reproducibility.md).

## Research Position

Sinomo explores a system in which models, agents, perception, memory, tools,
files, devices, and software updates operate across a trusted personal cluster
without relying on one cloud coordinator as the hidden source of authority.

The system is designed around several separations that are research-relevant:

- stale local availability versus quorum-authoritative control state;
- discovery and routing versus identity, trust, and final effect authority;
- visible conversations versus internal model sessions and bounded context;
- durable source evidence versus rebuildable memory indexes and summaries;
- immutable artifact production versus rollout and node-local activation;
- canonical distributed history versus opt-in degraded/offline work;
- reactive scheduling versus interactive and deliberative model work.

These are architectural commitments. Their presence does not by itself
establish a publishable performance, quality, availability, or safety result.

## How to Read Evidence

Sinomo documentation and artifacts use different evidence classes:

| Class | What it supports | What it does not support |
|---|---|---|
| Architecture invariant | Reviewed ownership and behavior required of implementations | A measured result or proof that every path conforms |
| Rust unit/integration test | Concrete behavior for the exercised implementation and environment | Exhaustive protocol behavior or untested failures |
| Current-behavior Quint refinement | Bounded state-space evidence for a named implementation decision surface | Unmodeled code, unbounded liveness, or complete system correctness |
| Abstract Quint contract | A compositional guarantee under explicit assumptions | A claim about hidden lower-layer mechanics |
| Target-hardening model | A precise desired property and counterexample boundary | Current production conformance |
| Docker or real-cluster harness | Process, disk, route, restart, and topology behavior in the recorded setup | Other platforms, arbitrary scale, or exhaustive faults |
| Benchmark | A measured operating point with recorded inputs and environment | A general claim without baselines, variance, and replication |
| Research note | Literature and design evidence | Adopted Sinomo behavior |
| Plan | Intended work or retained implementation history | Current behavior unless independently verified |

The [formal verification guide](../formal/cluster-verification.md) documents
model classification and omitted mechanics. Plans and research notes should
never be cited as proof that a feature exists.

## Current Research and Evaluation Surfaces

Status last reviewed: 2026-07-19. Inspect the referenced code and tests at the
exact commit used by an experiment.

| Surface | Implemented foundation | Existing evidence | Important limitation |
|---|---|---|---|
| Reactive KV/Raft for intermittent clusters | Explicit local/authoritative reads, current-term proofs, stable request IDs, bounded durable outcome recovery, restart-safe config changes, and wake-coalesced maintenance | KV and cluster tests; Raft/KV and membership Quint refinements | Needs external baseline comparison and long-running intermittent-device evaluation |
| Proof-oriented DFS authority | Sharded metadata authority, canonical visible history, content receipts, repair, replication, handoff, fencing, and POSIX-facing adapters | Extensive DFS Quint slices, integration tests, Docker cluster gates, SIFS/FUSE harness | Opt-in local-first branch/reconciliation and some POSIX/scale behavior remain partial or target hardening |
| Indexed-file memory | Version-identified, verified current-only text/Markdown snapshots, immutable lexical index generations, bounded evidence queries, and agent integration | Unit/integration tests, synthetic evaluation, pinned LongMemEval-S harness, literature map | Historical source bytes are not retained across source mutation/restart; multimodal, federated, vector/graph, and learned-relation behavior is not end-to-end implemented |
| Bounded multi-agent channels | Agent/channel architecture, typed events, durable sessions, cancellation, and useful-silence design questions | Literature review and runtime components | No canonical useful-silence benchmark result yet; treat as a research proposal |
| Controlled self-evolution | Candidate jobs, review artifacts, evaluation hooks, approval/rejection, immutable build/rollout separation, and recovery boundaries | Rust tests, architecture, replay/evaluation surfaces | Prototype-level orchestration; not evidence of safe autonomous recursive improvement |
| Realtime multimodal interaction | Audio/video/device surfaces, realtime provider calls, MiniCPM/PyTorch integration, and MCU/MineStudio adapters | Runtime tests and benchmark-specific vision/action/duplex harnesses | Latency bands are targets, not measured service-level guarantees; benchmark adapters are not a complete perception stack |
| Capability-safe modules and sandboxes | Typed service routes, node-local permissions, module lifecycle, VFS boundary, a development simple-native backend, and the WASIX runtime module | Backend tests, toolchain verifiers, module replay, policy/architecture tests | Simple-native is not a containment boundary; native dylibs remain trusted; a component-model Wasm backend is future work |

The broad “Sinomo architecture” is too large for one falsifiable research
claim. A strong study selects one row, states a narrow hypothesis, compares
against credible baselines, and links every result to an exact artifact.

## Formal Methods

The Quint tree is layered by responsibility:

```text
specs/quint/lib          finite shared vocabulary
specs/quint/models       transition systems
specs/quint/contracts    reusable guarantees
specs/quint/checks       invariant entry points
specs/quint/scenarios    composed randomized and deterministic traces
```

The registered suite covers KV/Raft, membership, network mechanism and retry,
integrated system guarantees, and many DFS authority, metadata, replication,
repair, handoff, POSIX, and degraded-operation slices. The harness pins Quint
and Apalache versions, supports fixed seeds and focused filters, and records
witness coverage separately from symbolic safety checks.

Useful entry points:

```sh
bash scripts/formal/check-quint.sh typecheck
bash scripts/formal/check-quint.sh fast
bash scripts/formal/check-quint.sh smoke

QUINT_CASE_FILTER=raft-kv QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage
```

Passing these checks is bounded, compositional evidence. It does not prove the
byte-level WAL, cryptography, real network scheduler, arbitrary cluster size,
fairness, or every higher-layer composition.

## Evaluation Surfaces

The repository includes opt-in evaluation infrastructure rather than one
aggregate score:

| Area | Location | Purpose |
|---|---|---|
| Multi-node cluster | `docker/` | Formation, routing, migration, failure, restart, DFS, and idle-resource gates |
| Agent and memory | `modules/benchmark/sinomo-benchmarks/agent/` | Synthetic memory checks and pinned LongMemEval-S retrieval/reader runs |
| SIFS/DFS | `crates/sinomo-dfs/benches/`, `modules/benchmark/sinomo-benchmarks/sifs/` | Algorithm scale plus real mounted correctness/performance |
| Coding agents | `modules/benchmark/sinomo-benchmarks/swebench/` | Contained SWE-bench execution and official evaluation integration |
| Realtime embodied tasks | `modules/benchmark/sinomo-benchmarks/mcu/` and `minestudio/` | Vision/action and full-duplex task adapters |
| Runtime tests | `sinomo test list` | Discoverable product checks with explicit live/cluster requirements |

External datasets are not silently downloaded or vendored. Individual harness
READMEs define pinned revisions, containment, credentials, and result schemas.
Raw output belongs under the gitignored `benchmark-results/` directory.

The repository does not currently ship one canonical, checked-in result
manifest for every evaluation. A paper or report must therefore preserve its
own immutable result bundle and should not infer a baseline from the existence
of a harness.

## Literature and Design Evidence

The [research-notes index](../research_notes/README.md) covers indexed-file and
semantic-filesystem memory, channel/multi-agent evaluation, real-time
multimodal systems, streaming perception, long-horizon agents, and interactive
computer use.

Research notes can motivate a hypothesis or baseline choice. A recommendation
becomes a Sinomo decision only when adopted by architecture or implemented and
tested. Older notes may preserve options that the current architecture later
rejected; cite both the note and the adopted decision when discussing that
history.

## Known Claim Boundaries

Do not describe these as established system-wide results without new evidence:

- the architecture's reactive, interactive, and deliberative latency ranges;
- continuous availability under arbitrary partitions or device sleep;
- autonomous safe self-improvement;
- full multimodal or federated long-term memory;
- complete POSIX behavior for every DFS policy and failure mode;
- unbounded formal correctness or liveness;
- production security certification;
- linear performance scaling with nodes, cores, shards, or models;
- provider-independent answer quality;
- completed remote placement of all agent sessions and devices.

The [plans index](../plans/README.md) is the explicit backlog/history boundary.
When a planned mechanism is implemented, update its plan status, architecture,
conformance tests, and this maturity matrix together.

## Referencing Sinomo

There is no repository-defined DOI or canonical academic citation at present.
For reproducibility, identify:

- the repository and exact Git commit;
- any dirty changes or patch bundle;
- the architecture/formal documents used to define the claim;
- binary and dataset hashes;
- model/provider versions and options;
- the machine, cluster topology, storage, and network setup;
- the complete command and result artifact location.

See [experimental reproducibility](experimental-reproducibility.md) for a
practical record template.
