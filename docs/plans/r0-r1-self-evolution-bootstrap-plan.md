# R0/R1 Self-Evolution Bootstrap Plan

## Purpose

This document defines the minimal R0/R1 core Sinomo needs in order to start evolving and expanding its own foundation.

The bootstrap goal is not to build a full autonomous module ecosystem first. The goal is to build a barebone local R0/R1 loop, then hand further improvement of that loop to Sinomo itself under human supervision in a controlled development environment.

For this bootstrap phase, production security is intentionally out of scope. The system is expected to run inside a disposable VM or similarly restorable environment. If an experiment destroys the workspace or host environment, the recovery mechanism is to restore the VM snapshot and continue. The important bootstrap properties are steerability, observability, repeatability, and fast recovery, not hardened containment.

In short:

```text
human objective
  -> scoped evolution job
  -> allowed workspace
  -> coding LLM or tool execution
  -> tests, evals, and logs
  -> steering decision
  -> candidate record
  -> human review
  -> keep, revise, or restore snapshot
```

## Guiding Position

The bootstrap's purpose is to grow R0/R1 from a minimal barebone core into a stronger self-evolution foundation.

R0/R1 are not merely a protective shell around future R3 modules. They are the first living substrate Sinomo needs to improve: the event log, job model, tool substrate, command runner, evaluation records, candidate bundle format, model bridge, and human review loop.

Without a minimally working R0/R1 foundation, R3 modules do not have a meaningful place to live. For example, a tool-use prompt is not useful until there is a real tool substrate that defines available tools, runs commands, captures outputs, reports failures, and lets Sinomo iterate on its own tool-use behavior.

Therefore the immediate target is:

> Sinomo can improve its own R0/R1 evolution machinery in a supervised VM, using evidence and human review to decide the next iteration.

The first version should be intentionally permissive. It should let Sinomo try real changes to its own foundation, observe what happened, and recover cheaply when those changes are bad. R3 should come later, after the local R0/R1 loop can scope, mutate, evaluate, record, review, and iterate reliably.

## Bootstrap Lab Assumptions

This plan assumes a controlled lab environment:

- Sinomo runs inside a disposable VM or equivalent sandbox.
- Human supervision is present for each bootstrap iteration.
- VM snapshots are the primary recovery mechanism.
- Heavy security boundaries are deferred.
- R0/R1 changes are allowed and expected.
- The system optimizes for learning speed, trace quality, and repairability.

The bootstrap loop should still track scope, commands, changed files, evidence, and review decisions, but these are primarily cognitive and operational aids. They help Sinomo and the human understand what happened. They are not intended to be hardened security boundaries in this phase.

R0 should start tiny and understandable. R1 should start practical and local-first. The first self-evolution loop should improve R0/R1 with human guidance and rapid snapshot recovery.

## Phase 0: Bootstrap Contract

Define the canonical contract every self-evolution attempt must satisfy.

Each evolution attempt must include:

- objective
- target ring
- allowed paths
- required verification commands
- expected experiment scope
- expected artifacts
- human review checkpoint
- recovery note

Current supporting crates:

- `sinomo-core`
- `sinomo-events`
- `sinomo-evolution`
- `sinomo-eval`
- `sinomo-policy`
- `sinomo-registry`
- `sinomo-sandbox`
- `sinomo-module`
- `sinomo-rollout`

Acceptance criteria:

- A serialized `EvolutionJob` can describe a real repository change.
- Every job can describe the intended edit scope.
- Every job has required verification commands.
- Every job produces enough evidence for a human and Sinomo to decide the next iteration.

## Phase 1: R0 Continuity Root

R0 owns only the smallest contracts and vocabulary required for the self-evolution loop to remain understandable and restartable.

R0 responsibilities:

- ID contracts
- event envelope
- append-only event log semantics
- trust ring definitions
- policy decision shape
- artifact and provenance shape
- evaluation scorecard shape
- recovery and snapshot vocabulary
- edit-scope description rules

Near-term implementation work:

- Make edit-scope description a first-class primitive.
- Add explicit states for `HumanReviewRequested`, `HumanAccepted`, and `HumanRejected`.
- Add structured recovery metadata to every evolution job.
- Record whether an iteration requires VM snapshot restore, ordinary git revert, or another repair path.

Acceptance criteria:

- R0 data structures are small enough to inspect manually.
- A failed bootstrap iteration can be understood from recorded job, command, diff, and review data.
- R0 contracts help Sinomo resume after a failed experiment rather than trying to prevent every possible failure.

## Phase 2: R1 Local Evolution Orchestrator

R1 should provide the first practical bootstrap engine.

The orchestrator wires the existing pieces together:

```text
EvolutionOpportunity
  -> EvolutionJob
  -> CandidateWorkspace
  -> MutationPacket
  -> LLM or tool run
  -> Sandbox/eval run
  -> SteeringDecision
  -> CandidateRecord
  -> HumanReviewRequest
```

Minimum responsibilities:

- create scoped jobs
- create candidate branch or workspace
- generate mutation packets for coding LLMs
- run verification commands
- collect changed files
- produce scorecards
- produce a steering decision
- register candidate metadata
- summarize candidates for human review

Acceptance criteria:

- One command can run an end-to-end dry-run evolution attempt.
- The loop can make real R0/R1 changes in the disposable environment.
- Outputs are auditable as JSON records and events.

Suggested first CLI shape:

```powershell
cargo run -p sinomo -- evolve propose `
  --objective "improve evolution job state coverage" `
  --allow "crates/sinomo-evolution/**" `
  --verify "cargo test -p sinomo-evolution"
```

## Phase 3: Tool Execution Substrate

Before Sinomo can make useful tool-use prompts, it needs real tools and real feedback.

Minimal tool registry:

- `read_file`
- `list_files`
- `search`
- `apply_patch`
- `run_command`
- `git_diff`
- `git_status`
- `format`
- `test`

Each tool should declare:

- name
- input schema
- output shape
- command or filesystem effects when relevant
- evidence captured from the call

Acceptance criteria:

- Mutation workers use tools through a registry so Sinomo can inspect and improve tool-use behavior.
- Tool calls are logged as events.
- Command output, exit codes, and changed files become evidence.
- The registry is simple enough for Sinomo to extend during bootstrap.

## Phase 4: Mutation Packet And Coding LLM Bridge

This is the first point where Sinomo asks a powerful coding LLM to improve Sinomo.

A mutation packet should include:

- job ID
- objective
- target ring
- allowed paths
- relevant source excerpts
- current failing tests or desired improvement
- required verification commands
- output contract
- experiment notes
- recovery notes

The output contract should ask for:

- changed files
- patch summary
- verification notes
- experiment notes
- recovery notes

The first LLM bridge can be simple:

- generate a prompt from the mutation packet
- call an existing provider from `sinomo-lm`
- receive a patch or structured implementation notes
- optionally hand off to a process coding agent such as a Codex CLI or Claude Code style provider

Acceptance criteria:

- The LLM sees a focused packet that helps it work on the intended part of R0/R1.
- The LLM result is treated as a candidate, not truth.
- The candidate must produce enough evidence for the next human-guided iteration.

## Phase 5: Evidence And Candidate Bundles

Every candidate should produce a durable local bundle.

Example bundle layout:

```text
$SINOMO_DATA_DIR/evolution/jobs/evojob_x/
  record.json
  job.json
  mutation-packet.json
  changed-files.json
  diff.patch
  command-log-cargo-test.txt
  scorecard.json
  policy-decision.json
  provenance.json
  human-review.md
  worktree/
```

Acceptance criteria:

- A human can inspect exactly what happened.
- Sinomo can replay why a candidate was accepted or rejected.
- Failed candidates become learning material instead of discarded noise.

## Phase 6: Human Review And Keep/Restore

For bootstrap, human review is the iteration mechanism.

Initial state flow:

```text
scoped
  -> candidate-created
  -> verified
  -> steering-reviewed
  -> awaiting-human-review
  -> kept
  -> revised-or-restored
```

Review should show:

- objective
- changed files
- diff summary
- tests run
- scorecard
- steering decision
- experiment notes
- recovery note

Acceptance criteria:

- Keeping a candidate is explicit.
- Reverting a candidate or restoring a snapshot is documented.
- R0/R1 can be improved directly during bootstrap.

## First Milestones

### Milestone 1: Local Evidence Loop

Implement a local, no-LLM evolution dry run.

Deliverables:

- scope job
- run command eval
- produce scorecard
- register candidate record
- emit or store audit records

No candidate mutation is required yet.

### Milestone 2: Scoped LLM Patch Loop

Add a scoped coding LLM path.

Deliverables:

- generate mutation packet
- ask coding LLM for patch or implementation
- apply in candidate workspace
- run tests
- produce review bundle

### Milestone 3: Guided Self-Improvement

Use Sinomo to improve one small part of its own R1 harness.

Good first targets:

- better eval command execution
- better scorecard fields
- better edit-scope descriptions
- better candidate bundle format
- better human review summary

## Deferred Work

Defer these until the local R0/R1 bootstrap loop works:

- Wasm R3 module ecosystem
- distributed rollout
- live hot swapping
- canary promotion
- automatic R1 promotion
- production security hardening
- strict autonomous policy mutation controls
- registry signatures beyond local hashes
- complex private replay suites

## Recommended Implementation Shape

Prefer a library-first orchestrator with a thin CLI wrapper.

The CLI should make the bootstrap loop easy to run manually, while the library keeps the same loop reusable later by the API, app runtime, scheduled evolution jobs, and eventually Sinomo itself.

Suggested crate ownership:

- `sinomo-core`: stable R0 data contracts.
- `sinomo-events`: append/replay event substrate.
- `sinomo-evolution`: orchestrator, job state, mutation packets, candidate lifecycle.
- `sinomo-eval`: command execution and scorecard production.
- `sinomo-policy`: steering decisions and review requirements.
- `sinomo-registry`: candidate and artifact records.
- `sinomo-sandbox`: execution requests/results, with hardening deferred.
- `sinomo-cli` or `sinomo`: thin user-facing command wrapper.

The first implementation should aim for a local, inspectable, human-guided self-improvement loop rather than distributed autonomy or production-grade safety.
