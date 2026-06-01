# Self-Evolution Implementation Roadmap

## Purpose

This roadmap updates the existing self-evolution plans now that Sinomo has a working coding agent loop and an initial SWE-bench benchmark harness. The goal is to turn the current local supervised evolution path into a serious implementation path without jumping prematurely to autonomous production rollout.

The immediate target is:

```text
objective
  -> scoped evolution job
  -> candidate worktree
  -> agent-driven mutation
  -> command and benchmark evaluation
  -> durable candidate bundle
  -> human approval or rejection
  -> managed-source fast-forward and explicit rollout
```

This preserves the direction from the self-evolution architecture while making the first implementation concrete: the agent loop is the mutation engine, evaluation evidence is the authority, and human approval remains the bootstrap checkpoint.

## Design Review Summary

The existing documents establish three compatible layers:

- `self-evolution-architecture.md` defines the long-term controlled-mutation system: trust rings, candidate bundles, eval gates, policy, registry, rollout, and rollback.
- `r0-r1-self-evolution-bootstrap-plan.md` defines the local supervised bootstrap loop for improving the R0/R1 foundation in a disposable environment.
- `r0-r1-bootstrap-technical-architecture.md` defines the first executable shape: job, mutation packet, worktree, verification, bundle, approval.
- `human-guided-long-horizon-evolution.md` adds campaigns, stages, resources, scheduling, and adaptation above the one-candidate loop.

The code has already implemented more than the older docs assume:

- R0 contracts exist for `EvolutionJob`, `EditScope`, `VerificationPlan`, `MutationPacket`, `CandidateBundleManifest`, `EvaluationScorecard`, and approval decisions.
- `sinomo-evolution` has `GitWorkspaceAdapter`, `LocalCandidateBundleStore`, file-backed job/campaign records, campaign dry-runs, scheduler dry-runs, stage evaluation, and a first `EvolutionOrchestrator`.
- `sinomo-eval` has a real `LocalCommandRunner`, command control, inactivity notifications, and command-based scorecards.
- `sinomo-cli` exposes `evolve propose`, `approve`, `reject`, `restore`, `status`, `show`, `verify`, `schedule`, `iterate`, `evaluate`, and `tick`.
- `sinomo-agent` now has the stronger coding harness: dynamic tools, standalone Codex patch parser, provider-aware native tool support, session store, compaction, tool evidence, sanitized model-visible tool results, and benchmark-proven local workspace behavior.
- `sinomo-benchmarks` has a SWE-bench Verified 10-task suite, mini-SWE-agent baseline path, official SWE-bench evaluation integration, and traced Sinomo runs.

The main mismatch is that `sinomo-evolution` still has its own smaller mutation bridge and tool registry. That bridge was useful for bootstrap, but it duplicates and lags the real agent loop. The serious implementation should make `sinomo-agent` the canonical mutation worker.

The first artifact-integrity step is also now in place: candidate bundles carry a stable content manifest, aggregate bundle hash, and in-toto-style attestation that records the job, producer, mutation worker, source revision, verification outcome, changed files, and evidence artifact references. `sinomo-registry` can now recompute the bundle hash, validate the content manifest and attestation, and register a verified candidate-bundle provenance record. Approval writes that record back into the candidate bundle as `candidate-bundle-registry-record.json` when integrity passes and persists a registry-owned copy under `registry/candidate-bundles/<job_id>.json`. The node-local module loader now rejects policy-allowed artifacts that do not carry supported trust material: a supported content hash plus either a verified local Ed25519 signature or registry/attestation evidence. `sinomo-registry` now also has durable trusted artifact-signer records under `registry/trusted-artifact-signers.json`, cluster KV helper records under `/cluster/registry/trusted-artifact-signers/`, authorization checks for artifact signature public keys by target ring and module, and `sinomo evolve trusted-signers list|publish|hydrate` for node-local/cluster trust synchronization. `sinomo-rollout` can prepare a verified-candidate rollout only when the artifact has a valid local Ed25519 signature from an authorized trusted signer and the rollout plan, policy decision, and verified candidate-bundle registry record agree, and only after the node-local loader accepts the request. Accepted verified preparations and subsequent rollout decisions now persist resumable runtime state under `rollout/runtime-state.sobj`. Rollout decisions can also be advanced from that persisted state through `sinomo rollout advance`, persisted as transition records under rollout history, emitted as matching `rollout.transition.completed` event-log entries, and applied into node-local module loader runtime state under `module/loader-state/<module_id>.sobj`. `sinomo-module` can now route module invocations deterministically from that loader state and execute the selected artifact through a pluggable artifact runner, including shadow mirroring and canary bucket selection without letting shadow output override primary output. The first runners are a durable local-command registry for exercising the boundary end-to-end and a sandbox-backed WASI/module-component registry that verifies artifact bytes before execution. This is not yet connected to a converted low-risk production WIT component module or binary hot-swap engine, but it gives later rollout work concrete runtime state and execution decisions to consume.

## Implementation Principles

1. The active Sinomo checkout must not be mutated directly. Every attempt uses a candidate worktree under `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/`.
2. `sinomo-agent` is the mutation harness. Do not continue growing a second coding-agent loop inside `sinomo-evolution`.
3. Evaluation evidence gates approval. Model summaries and self-review are useful evidence, not authority.
4. Public SWE-bench is a harness regression signal, not the final measure of capability. Use it to compare runs and catch regressions, then add private trace-derived evals later.
5. Human approval remains required for now. Automatic live rollout, distributed canaries, and R1 self-promotion come after local candidate quality is reliable.
6. Every candidate must be inspectable after the fact: mutation packet, full agent run, tool evidence, diff, command logs, benchmark logs, scorecards, and approval decision.

## Phase 1: Use The Agent Loop As The Mutation Worker

Add an agent-backed mutation worker in `sinomo-evolution`.

Proposed shape:

```rust
pub struct EvolutionAgentMutationModel {
    pub provider: String,
    pub model: String,
    pub data_dir: SinomoDataDir,
    pub max_steps: Option<usize>,
}
```

Responsibilities:

- Render the existing `MutationPacket` into an `AgentLoopInput`.
- Populate `AgentEvolutionContext` with the job, packet, and candidate bundle.
- Run `sinomo_agent::Agent` in the candidate worktree with `AgentToolRegistry::local_workspace_with_config`.
- Use the same local workspace tools as the benchmark harness.
- Persist the full `AgentRun` into the candidate bundle as `agent-run.json`.
- Persist model turns, tool results, agent evidence, final text, and usage metrics.
- Return a `ModelMutationResult` or a new `AgentMutationResult` that the current `EvolutionOrchestrator` can consume.

Acceptance criteria:

- `sinomo evolve propose "..."`
  can run through the agent loop instead of the legacy mutation bridge.
- Candidate bundles contain `agent-run.json`.
- The resulting diff is collected from the candidate worktree, not trusted from model text.
- Existing `cargo test -p sinomo-agent` and `cargo test -p sinomo-evolution` pass.

Implementation note: `sinomo-evolution` and `sinomo-agent` are both R2 crates, so a direct dependency is acceptable under the current ring tests. If that coupling starts to feel too broad, extract a smaller R2 mutation-worker facade later.

## Phase 2: Retire Or Narrow The Legacy Evolution Tool Registry

The current `EvolutionToolRegistry` should not remain a second full coding harness.

Near-term handling:

- Keep it only as a compatibility path for existing tests and simple non-agent providers.
- Stop adding major behavior to it.
- Route normal model mutation through the agent-backed worker.
- Reuse the agent `apply_patch` implementation instead of the current evolution-side `git apply`/small V4A parser path.
- Reuse agent model-visible result sanitation and evidence containers.

Acceptance criteria:

- New self-evolution runs use the same `read_file`, `search`, `apply_patch`, `run_command`, and command-session behavior as SWE-bench runs.
- The candidate bundle records which mutation worker was used: `agent-loop` or `legacy-bridge`.
- Legacy bridge failures do not block progress on the agent-backed path.

## Phase 3: Add Benchmark-Aware Evaluation Gates

Extend evaluation so a self-evolution candidate can be judged by both ordinary verification commands and benchmark suites.

First benchmark gate:

```text
SWE-bench smoke:
  suite: verified_10
  selection: one instance by default
  provider/model: same as candidate or explicit benchmark model
  baseline: previous main run or mini-SWE-agent reference
  pass condition: selected instance resolves or no regression against stored baseline
```

Implementation approach:

- Keep `sinomo-benchmarks` outside production runtime dependencies.
- Let `sinomo-evolution` call benchmark runners as command gates first.
- Add scorecard adapters that parse benchmark output JSON into `EvaluationMetric` values:
  - `swebench_total_instances`
  - `swebench_resolved_instances`
  - `swebench_error_instances`
  - `agent_model_turns`
  - `agent_tool_results`
  - `agent_input_tokens`
  - `agent_output_tokens`
- Copy or reference benchmark result artifacts from `benchmark-results/` into the candidate bundle.

CLI shape:

```text
sinomo evolve propose "improve agent context packing" \
  --allow "crates/sinomo-agent/**" \
  --verify "cargo test -p sinomo-agent" \
  --benchmark "swebench:psf__requests-1142"
```

Acceptance criteria:

- One self-evolution candidate can run cargo verification plus one SWE-bench smoke gate.
- The bundle records benchmark command, run directory, summary JSON, and scorecard metrics.
- Benchmark failures mark the candidate scorecard failed or inconclusive, not silently successful.

## Phase 4: Strengthen Approval And Promotion

`evolve approve` already records a decision and fast-forwards the source repository. Make that approval path stricter before it becomes routine.

Required improvements:

- Refuse approval by default if required verification failed.
- Refuse approval by default if the candidate worktree has no diff and the result kind is expected to be a patch.
- Refuse approval by default if the source repo cannot fast-forward cleanly.
- Add `--override` with a required note for intentional exceptions.
- Record approval preflight results in `approval-decision.json`.
- After approval, write a short `candidate-summary.md` into the bundle for human and model review.

Acceptance criteria:

- Bad candidates remain inspectable but cannot be accidentally promoted.
- Approval decisions include objective, diff summary, verification outcome, benchmark outcome, override state, and recovery note.

## Phase 5: Connect Campaign Iteration To Real Mutation

The campaign layer now supports proposal, scheduling dry-runs, iteration dry-runs, stage evaluation, background tick planning, and an initial non-dry-run execution path for one selected task. The remaining work is to make adaptation richer after repeated failed iterations.

Implementation:

- Convert a `NearTermEvolutionTask` into an `EvolutionRequest`.
- Dispatch the agent-backed mutation worker.
- Run command and benchmark gates.
- Attach the resulting `EvolutionReport` to `EvolutionIterationResult`.
- Run `evaluate_campaign_stage` automatically after candidate verification.
- If an iteration fails, create an adaptation record using the failure class and scorecard evidence.
- Persist non-dry-run iteration plan, request, dispatch, result, stage scorecard, and adaptation artifacts in the campaign bundle.

Acceptance criteria:

- `sinomo evolve iterate --campaign <id> --task <id>` can produce a real candidate bundle.
- Stage scorecards update from candidate evidence.
- After repeated failures, the campaign proposes an easier, more diagnostic, or more foundational next task. This remains the next hardening step.

## Phase 6: Resource Usage And Long-Running Operation

Use the agent run and benchmark traces as the first real resource ledger inputs. The initial implementation now writes candidate-level resource summaries and feeds prior campaign iteration usage back into scheduler dry-runs; richer provider cost models and cluster persistence can build on that.

Implementation:

- Extract model usage from `AgentRun.model_events`.
- Record command wall time, benchmark wall time, and estimated local compute cost.
- Attach usage confidence: exact, provider-reported, estimated, or unknown.
- Feed usage into the existing resource scheduler dry-run model.
- Let `evolve tick --dry-run` plan around real previous usage.
- Only after the dry-run path is reliable, allow a background tick to dispatch one approved low-risk iteration.
- Store scheduler-facing usage on `EvolutionIterationResult` so later ticks can account for work already spent in the current resource window.

Acceptance criteria:

- Candidate bundles include resource usage summaries.
- Campaign decisions reference resource context.
- The scheduler can pause a campaign because a resource lease cannot be granted.

## Phase 7: Rollout Stays Explicit

For now, approval should fast-forward managed source and stop there. Runtime rollout remains explicit:

```text
sinomo rollout apply
```

Later, a campaign may request rollout as a separate approved step, but not as part of the first serious mutation path.

Do not implement yet:

- autonomous cluster-wide rollout from self-evolution,
- R3 Wasm module conversion,
- automatic policy mutation,
- formal capability registry as a required state machine,
- live canary promotion without human approval.

## First Implementation Slice

The next concrete engineering batch should be:

1. Add `sinomo-agent` as a dependency of `sinomo-evolution`.
2. Implement `EvolutionAgentMutationModel`.
3. Add `agent-run.json` and agent summary paths to candidate bundles.
4. Switch `evolve propose` to use the agent-backed mutation worker by default.
5. Keep the legacy bridge available behind an explicit fallback flag or test helper.
6. Add a focused test that runs a fake model through the agent-backed mutation worker and produces a candidate diff.
7. Add a second test that verifies the bundle contains mutation packet, agent run, changed files, diff, command logs, and scorecard.

After that batch, add the SWE-bench smoke gate as the first benchmark-backed evaluation path.

Current status: the first implementation batch now routes normal `evolve propose` mutation through `sinomo-agent`, records `agent-run.json` and `agent-summary.json` in candidate bundles, and keeps the legacy bridge as a compatibility/test path. The second batch has started: `evolve propose` accepts benchmark specs, runs them as command-backed `PrivateRegressionEvals`, records `benchmark-results.json`, and parses benchmark summary JSON into scorecard metrics. SWE-bench specs now request official evaluation by default, write parsed resolved/unresolved/error counts into the summary, and fail the candidate scorecard unless all selected instances resolve without errors. Approval now runs preflight checks for verification outcome, candidate diff, candidate bundle integrity, clean source checkout, and fast-forward viability; `--override` requires a note and records the failed checks in `approval-decision.json`, and successful approvals write `candidate-summary.md`. Campaign iteration can dispatch real agent-backed mutation, evaluate the candidate, persist iteration artifacts, record adaptation evidence, and feed candidate resource usage into scheduler planning. The SE-2/SE-4 bridge now has local bundle integrity verification, registry-side candidate bundle records, approval blocking for tampered or incomplete bundles, persisted registry records for integrity-verified approved candidates under the registry data-dir, loader trust-material checks with local Ed25519 artifact-signature verification, durable registry trusted-signer authorization, cluster KV serialization and CLI publish/hydrate support for trusted signer records, verified-candidate rollout preparation that requires a valid artifact signature from an authorized signer, durable rollout runtime state, a `sinomo rollout advance` path that consumes persisted runtime state plus scorecard/incident/module-replay/live-shadow evidence and explicit rollback triggers, durable rollout transition records for promotion/rollback audit, rollout transition event-log appends, a local rollout executor that applies recorded transitions into module loader runtime state with traffic weights, deterministic module invocation routing for shadow/canary selection, the first checked-in `sinomo-module-v1` WIT boundary, and a pluggable artifact execution boundary with durable local-command and sandbox-backed WASI/module-component runners that mirror shadow invocations while preserving primary output. Module replay now has first-class fixtures and reports: `ModuleReplayFixture` drives the same routed invocation path, `ModuleReplayReport` captures per-case primary/shadow outcomes, missing routes, errors, and shadow mismatches, emits `module.shadow.decision` events, converts the result into replay and shadow-mode scorecard gates, and can be passed directly to `sinomo rollout advance --module-replay-report <path>` for live promotion or rollback decisions. Live routed invocation can append `module.shadow.decision` telemetry into the event log, `ModuleShadowTelemetryReport` summarizes recent matching events into shadow-mode scorecards, `sinomo rollout advance --shadow-telemetry [--shadow-telemetry-limit <n>]` consumes that live evidence from the persisted rollout state, and live routed invocation also emits `module.invocation.decision` so canary-stage daemon advancement evaluates candidate-primary traffic before increasing traffic weight. The main daemon maintenance loop now consumes stage-appropriate event-log telemetry through the rollout controller while checkpointing consumed event sequences and telemetry kind under `rollout/telemetry-advance-state.sobj`, and publishes reference-shaped module rollout desired-state records under `/cluster/module-rollouts/desired/<module>/<rollout>` and each node's current module rollout stage, traffic weight, artifact ids, hot-swap state, and telemetry checkpoint under `/cluster/module-rollouts/nodes/<node>/<module>`. Automatic promotion gates are durable under `rollout/telemetry-gates.sobj`, configurable through `sinomo rollout telemetry-gates show|set`, and enforce minimum event counts, scan limits, automatic promotion enablement, maximum automatic traffic weight, and optional candidate-primary p95 latency bounds without blocking failed-scorecard rollback. Rollout observations map failed scorecards, incidents, user-correction spikes, and explicit `--rollback-trigger <trigger>` telemetry through the rollout plan's `RollbackPolicy` before rolling back. `VerificationPlan` now carries module replay report references, `sinomo-eval` ingests those report JSON files into replay/shadow scorecard gates, mutation prompts expose them as requested evidence, and `sinomo evolve propose|iterate` accept `--module-replay-report [name=]<path>`. Sandbox-backed WASI artifacts can now declare `sinomo-module-v1`; the sandbox runner verifies registered bytes, executes active plus shadow artifacts through the replay route, validates the typed `SinomoModuleV1Result` JSON contract, and has a low-risk silence-policy replay fixture that proves the path. The sandbox runtime also enables Wasmtime component-model support for artifacts that opt into `component_model`: it rejects malformed components whose `sinomo:module-v1/module.run` export does not match the WIT signature, instantiates valid components with a fuel budget, invokes typed `run`, and adapts the typed module result back into the existing JSON contract. A component-model silence-policy trigger fixture now runs through the same active-plus-shadow replay path, so the first low-risk WIT component target is covered by executable evidence. The sandbox CLI exposes the operational loop through `sinomo sandbox module-artifact register` and `sinomo sandbox module-replay run`, with registered artifact bytes and replay reports persisted under the data-dir storage contract. The next implementation batch should add daemon reconciliation of cluster module rollout desired-state and richer user-correction canary gates.

Follow-up update: the desired-state reconciliation part of that next batch is now implemented. Each daemon reconciles active local module rollout targets into `/cluster/module-rollouts/reconcile/<node>/<module>/<rollout>` as `matched`, `material-available`, or `blocked`, while still leaving artifact trust and verified rollout preparation as local node responsibilities.

Follow-up update: user-correction canary gates are now implemented. `module.user.correction` events are folded into `ModuleCanaryTelemetryReport`, emitted as canary scorecard metrics, and passed to the rollout controller as `UserCorrectionSpike` evidence so candidate corrections can trigger rollback even before the minimum candidate-primary event count is reached.

Follow-up update: runtime failure canary rollback is now implemented. `module.crash`, `module.timeout`, and `module.policy.blocked` events are consumed with live canary telemetry, filtered to the current module and candidate artifact, recorded as panic/timeout/policy-blocked canary metrics, and mapped to the rollout policy's `Panic`, `Timeout`, and `PolicyIncident` triggers.

Follow-up update: local verified rollout material is now the bridge from cluster desired state into node-local rollout preparation. Approved material can be persisted under `registry/verified-rollout-material/<candidate-bundle-job>.json`; daemon desired-state reconciliation verifies that record against the desired rollout plan, candidate bundle record, bundle hash, local trusted signer set, policy decision, and artifact manifest before calling the normal verified rollout preparation path and publishing `prepared` reconciliation state. The next gap is producing and hydrating this verified material automatically from the approval/module-registration flow for the first low-risk module family.

Follow-up update: approval now materializes verified rollout material automatically when an approved candidate bundle contains `verified-rollout-material.json`. The approval preflight treats that file as optional for normal source candidates, but when it is present it validates the candidate bundle reference, bundle integrity, artifact/policy/rollout consistency, non-empty evidence refs, and authorized local artifact signature before writing the registry-owned record under `registry/verified-rollout-material/<candidate-bundle-job>.json`. The next gap is wiring sandbox module-artifact registration to emit this bundle material for the first low-risk module family instead of requiring the bundle file to be assembled by hand.

Follow-up update: sandbox module-artifact registration can now emit candidate-bundle rollout material directly. `sinomo sandbox module-artifact register` accepts `--candidate-bundle <bundle-dir> --module <module-id> --active-artifact <artifact-id>` plus optional rollout/policy ids, module version/owner, and evidence refs; it copies the Wasm bytes into the local sandbox registry as before, signs an R3 artifact manifest with the node identity, and writes `verified-rollout-material.json` into the candidate bundle for later approval materialization. The next gap is exercising this in a full low-risk module-family flow that includes replay evidence, approval, cluster desired-state publication, daemon preparation, shadow telemetry, and canary rollback.

Follow-up update: the local low-risk source-to-rollout flow is now covered by `self_evolution_module_candidate_flow_advances_to_canary_and_rolls_back`. The test creates a candidate bundle through an agent-backed evolution proposal, has the fake model patch the low-risk candidate module source, compiles the active and candidate WAT sources into registered sandbox artifacts, evaluates a module replay report into `scorecard.json`, registers sandbox module rollout material, refreshes the bundle integrity after the material is added, trusts the local artifact signer, approves the candidate, verifies registry materialization, publishes cluster desired state, confirms daemon reconciliation writes shadow rollout runtime state, advances to canary from live shadow telemetry, invokes an intentionally bad candidate artifact through the routed module execution path, records the timed-out live invocation decision with structured `primary_error_kind = "timeout"` telemetry, and rolls back from that timeout evidence. The default targeted smoke command, `cargo test -p sinomo-cli self_evolution_module_candidate_flow_advances_to_canary_and_rolls_back`, now pulls `sinomo-sandbox-runtime` only as a dev dependency, derives replay evidence through `run_sandbox_wasm_module_replay_fixture`, and executes the bad canary candidate through `SandboxWasmModuleArtifactRunner`; the production CLI/library build still keeps the heavy runtime behind the `sandbox` feature, but this E2E no longer accepts handcrafted replay or local-command fallback evidence. The rollout controller now treats the module artifact content hash and candidate bundle content hash as distinct hashes bound by signed material and registry validation, timed-out candidate invocation decisions are mapped into the rollout policy's `Timeout` trigger, and bundle integrity treats mutable `job.json`/`record.json` bookkeeping as control metadata instead of provenance payload. The focused local smoke command now covers the source-to-rollout flow, the registry bundle-integrity metadata regression, formatting, and `cargo check -p sinomo-cli` as operator-run validation. The next gap is broadening this into stronger reproducible local regression evidence.

Follow-up update: the first SE-5 telemetry-to-opportunity bridge is now implemented. `detect_evolution_opportunities_from_events` accepts live/module telemetry events and detection policy, recognizes module timeouts, runtime failures, policy-blocked incidents, user corrections, metric regressions, replay failures, and stale scorecards, then produces bounded `EvolutionOpportunity` records with observed-event evidence, target ring/module, workspace, path scope, and priority. The existing `EvolutionPlanner` can immediately scope those detected opportunities into candidate jobs, so the remaining SE-5 work is wiring this detector into daemon/background tick dispatch with autonomous budget and evidence gates.

## Suggested First Dogfood Target

Use self-evolution to improve itself in a narrow, low-risk way:

```text
objective:
  Improve candidate bundle evidence summaries for agent-backed evolution runs.

allowed paths:
  crates/sinomo-evolution/**
  crates/sinomo-agent/**
  crates/sinomo-cli/src/commands/evolve.rs

verification:
  cargo test -p sinomo-evolution
  cargo test -p sinomo-agent

benchmark:
  swebench:psf__requests-1142
```

This target exercises the real loop without requiring live rollout or broad architectural changes. It also produces evidence that is immediately useful for subsequent self-evolution attempts.
