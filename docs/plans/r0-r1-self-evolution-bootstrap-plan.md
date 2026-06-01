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
  -> verification evidence
  -> candidate record
  -> approval decision
  -> approve, reject, or restore snapshot
```

## Guiding Position

The bootstrap's purpose is to grow R0/R1 from a minimal barebone core into a stronger self-evolution foundation.

R0/R1 are not merely a protective shell around future R3 modules. They are the first living substrate Sinomo needs to improve: the event log, job model, tool substrate, command runner, evaluation records, candidate bundle format, model bridge, and approval loop.

Without a minimally working R0/R1 foundation, R3 modules do not have a meaningful place to live. For example, a tool-use prompt is not useful until there is a real tool substrate that defines available tools, runs commands, captures outputs, reports failures, and lets Sinomo iterate on its own tool-use behavior.

Therefore the immediate target is:

> Sinomo can improve its own R0/R1 evolution machinery in a supervised VM, using evidence and human approval to decide the next iteration.

The first version should be intentionally permissive. It should let Sinomo try real changes to its own foundation, observe what happened, and recover cheaply when those changes are bad. R3 should come later, after the local R0/R1 loop can scope, mutate, evaluate, record, approve or reject, and iterate reliably.

## Bootstrap Lab Assumptions

This plan assumes a controlled lab environment:

- Sinomo runs inside a disposable VM or equivalent sandbox.
- Human supervision is present for each bootstrap iteration.
- VM snapshots are the primary recovery mechanism.
- Heavy security boundaries are deferred.
- R0/R1 changes are allowed and expected.
- The system optimizes for learning speed, trace quality, and repairability.

The bootstrap loop should still track scope, commands, changed files, evidence, and approval decisions, but these are primarily cognitive and operational aids. They help Sinomo and the human understand what happened. They are not intended to be hardened security boundaries in this phase.

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
- approval checkpoint
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
- Add explicit states for `AwaitingApproval`, `Approved`, and `Rejected`.
- Add structured recovery metadata to every evolution job.
- Record whether an iteration requires VM snapshot restore, ordinary git revert, or another repair path.

Acceptance criteria:

- R0 data structures are small enough to inspect manually.
- A failed bootstrap iteration can be understood from recorded job, command, diff, and approval data.
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
  -> VerificationEvidence
  -> CandidateRecord
  -> ApprovalRequest
```

Minimum responsibilities:

- create scoped jobs
- create candidate branch or workspace
- generate mutation packets for coding LLMs
- run verification commands
- collect changed files
- produce scorecards
- produce verification evidence
- register candidate metadata
- summarize candidates for approval

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
- `search`
- `apply_patch`
- `run_command`

Long-running command control can be exposed as session tools or explicit interrupt/kill controls depending on the runtime. `request_user_input` belongs to interactive sessions once a user bridge is attached.

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
- optional tool calls when the model needs Sinomo to inspect files, run commands, or apply intermediate edits

The first LLM bridge should be solid enough to support both process coding agents and direct API models:

- generate a prompt from the mutation packet
- call an existing provider from `sinomo-lm`
- include the candidate workspace path and tool registry descriptors
- for direct API models, accept structured JSON with `tool_calls`
- execute tool calls through the R1 tool registry and feed results back into the next model turn
- receive a final patch or structured implementation notes
- apply final patches as candidate edits through `apply_patch`
- record raw output, model turns, tool results, and parsed summaries as bundle evidence
- for process coding agents such as Codex CLI or Claude Code, let the provider edit the candidate worktree directly behind the `sinomo-lm` abstraction
- treat target ring as metadata only until the VM/ring separation exists

Acceptance criteria:

- The LLM sees a focused packet that helps it work on the intended part of R0/R1.
- The LLM result is treated as a candidate, not truth.
- The candidate must produce enough evidence for the next human-guided iteration.
- Non-agent API models can use Sinomo tools instead of requiring an external coding-agent CLI.
- Patch application failures are explicit candidate failures, not silent successes.

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
  approval-decision.json
  worktree/
```

Acceptance criteria:

- A human can inspect exactly what happened.
- Sinomo can replay why a candidate was accepted or rejected.
- Failed candidates become learning material instead of discarded noise.

## Phase 6: Approval And Keep/Restore

For bootstrap, human approval or rejection is the iteration mechanism. The human does not write a review artifact back into Sinomo; they inspect the candidate externally and record only the decision.

Initial state flow:

```text
scoped
  -> candidate-created
  -> verified
  -> awaiting-approval
  -> approved
  -> rejected-or-restored
```

Candidate evidence should show:

- objective
- changed files
- diff summary
- tests run
- scorecard
- experiment notes
- recovery note
- approval decision, when one has been recorded

Acceptance criteria:

- Approving a candidate is explicit.
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
- produce evidence bundle

### Milestone 3: Guided Self-Improvement

Use Sinomo to improve one small part of its own R1 harness.

Good first targets:

- better eval command execution
- better scorecard fields
- better edit-scope descriptions
- better candidate bundle format
- better candidate evidence summary

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

- `sinomo-core`: stable low-level R0 data contracts. It should not contain self-evolution job, mutation packet, candidate bundle, or approval lifecycle schemas.
- `sinomo-runtime`: explicit R0/R1 boundary facade; aggregates only R0/R1 crates and must not depend on R2 services or applications.
- `sinomo-events`: append/replay event substrate.
- `sinomo-evolution`: R2 evolution service; orchestrator, model-driven proposal flow, candidate lifecycle, and human-facing bootstrap behavior. Evolution-specific job, packet, candidate bundle, campaign, and approval contracts live here. Shared scorecard, artifact, policy, rollout, registry evidence, and sandbox workload contracts stay below it as generic R0/R1 functionality.
- `sinomo-eval`: command execution and scorecard production.
- `sinomo-policy`: approval requirements and eventual autonomous promotion policy.
- `sinomo-registry`: generic artifact, artifact-evidence, release-material, and signer records.
- `sinomo-sandbox`: generic execution workload requests/results, with hardening deferred.
- `sinomo-cli` or `sinomo`: thin user-facing command wrapper.

Every workspace crate should declare `[package.metadata.sinomo] ring = "..."`. Architecture tests should keep the cut mechanical: R0/R1 crates cannot depend on R2 or application crates, R2/application crates must cross through `sinomo-runtime` instead of directly depending on R0/R1 crates, and `sinomo-runtime` can aggregate only R0/R1 crates.

The first implementation should aim for a local, inspectable, human-guided self-improvement loop rather than distributed autonomy or production-grade safety.
