# Sinomo Evolution Framework Architecture

## Purpose

Evolution is Sinomo's generic, goal-directed improvement framework. A goal may
describe software, a prompt, a model, a dataset, a design, a policy, or any other
object whose revisions can establish immutable content identity and produce
evaluatable artifacts. The framework repeatedly proposes revisions, evaluates
their artifacts against explicit metrics and guardrails, and uses the evidence to
choose the next branch of work.

Self-evolution is one application of this framework. When the target is Sinomo or
another repository, the same machinery isolates workspaces, evaluates replayable
evidence, and may hand approved artifacts to the separate rollout authority.

This document defines the stable architecture and invariants. Current
implementation status is tracked by the code, readiness gates, rollout
operations, and test evidence rather than by separate plan documents.

Evolution complements the event-centric distributed runtime in
[architecture.md](architecture.md). It is another control plane: it consumes
events, traces, metrics, workspaces, scorecards, policy decisions, and rollout
telemetry, then emits candidate bundles, artifacts, desired-state changes when
applicable, and audit events. Sinomo self-mutation is the default target when no
repository is supplied; `sinomo evolve ... --repo <path>` uses the same
candidate, verification, and evidence lifecycle for another project. Non-git
targets provide an equivalent immutable content identity and evaluator contract.

## Goal And Revision Model

An evolution goal is the durable unit of intent. It owns:

- the user objective, non-goals, constraints, and approval checkpoints;
- metric definitions, directions, targets, and absolute guardrails;
- a revision directed acyclic graph (DAG);
- tasks, subgoals, dependencies, and agent assignments;
- artifact-bound metric observations;
- resource and monetary cost records with provenance and confidence;
- the attributable event history used for audit, replay, and interaction.

Every graph node is a revision. A revision is immutable once admitted to the
graph and identifies its content using one of these forms:

- a repository plus an immutable commit identity;
- a frozen directory snapshot plus its digest and optional manifest;
- inline raw contents plus media type and digest;
- a composite identity binding an immutable base revision to a content-addressed
  patch artifact.

A mutable path alone is not a revision identity. The system must snapshot its
contents, store the raw contents, or bind it to another immutable identity before
evaluation evidence can be attributed to it.

Revision parent links form the DAG. Multiple children represent competing
candidates; multiple parents represent an explicit merge. Selection never
rewrites graph history. Rejected, failed, superseded, and rolled-back revisions
remain inspectable with their evidence.

Artifacts belong to exact revisions and are the only inputs from which metric
observations are established. Examples include evaluation datasets, result
tables, scorecards, traces, patches, binaries, images, and provenance manifests.
Each observation names the metric, evaluator, artifact, sample count when
applicable, time, and outcome. Missing or incomparable observations stay missing;
the governor does not impute them.

## Runtime Boundary

The implementation is the independently built native-dylib module
`module.evolution`, located at `modules/evolution/sinomo-evolution`. Its module
descriptor owns the evolution service interface, app and CLI routes, settings,
storage identity, imported and exported capabilities, and lifecycle admission.
The signed default module distribution profile builds and activates it as a
separate artifact on supported desktop/server platforms.

The `sinomo` binary, daemon/CLI host, and graphical app depend only on the
serializable evolution contract and generic module clients. They do not link
`sinomo-evolution`, call its Rust functions, or register a host-side evolution
service. App operations and `sinomo evolve` cross the authenticated local daemon
boundary and resolve the active module routes. Missing, disabled, incompatible,
or failed evolution modules therefore make only evolution unavailable; they do
not prevent the daemon, chat, or unrelated modules from starting.

## Design Goals

- Keep Sinomo running while it changes itself.
- Reuse the candidate/evidence lifecycle for external projects without assuming
  Sinomo-specific rollout material.
- Make every mutation scoped, replayable, inspectable, and reversible.
- Let models propose and implement changes, but never grant them direct
  authority over the live runtime.
- Prefer the evolution R3 sandboxed-module class as the first autonomous
  mutation target.
- Preserve R0/R1 continuity with standby, handoff, checkpoints, and rollback
  before any trusted-runtime mutation can activate.
- Treat evaluation evidence, policy, provenance, and node-local loader checks as
  the authority for promotion.

## Non-Goals

- No direct in-place patching of trusted running code by a model.
- No promotion based only on model self-review or generated tests.
- No weakening of sandbox, policy, registry, or rollback gates to make a
  candidate easier to ship.
- No assumption that a public benchmark is sufficient evidence for real
  improvement.

## Evolution Risk Rings

Self-evolution uses the `TrustRing` domain model to classify mutation risk and
required containment. This is distinct from the Cargo dependency rings in
package metadata. Evolution R0 through R2 align conceptually with the
foundation, trusted runtime, and trusted services, but they are not dependency
tags. Evolution R3 through R5 classify artifacts, disposable workspaces, and
model providers and have no Cargo dependency meaning.

Lower evolution rings have more continuity responsibility and therefore
require stronger gates.

| Evolution ring | Contents | Mutation Rule |
|---|---|---|
| R0 continuity root | event envelope, ID contracts, artifact verifier, policy verifier, rollback loader, emergency stop | Mutate only through dual-root or equivalent standby handoff with strongest continuity gates. |
| R1 trusted runtime | node supervisor, event store, DFS metadata, sandbox host, rollout controller | Mutate through standby execution, state handoff, health checks, activation checkpoints, and rollback checkpoints. |
| R2 trusted services | perception, memory, trigger, agent, deliberation, action services | Mutate through strong evaluation, canary, and rollback gates. |
| R3 sandboxed modules | skills, tools, evaluators, adapters, prompts, planners, local policies | Primary autonomous mutation target. |
| R4 disposable workspaces | candidate branches, test sandboxes, generated fixtures, traces | Fully automated, isolated, and disposable. |
| R5 model providers | local or remote LLMs used for proposal, implementation, critique, review | No direct authority; can only produce artifacts or judgments. |

## Agent-Governed Control Plane

The evolution agent is the goal governor. It interprets the objective, maintains
the DAG and metric contract, creates subgoals and tasks, allocates bounded work,
reviews evidence, and recommends the next experiment. It delegates proposal,
implementation, and evaluation to separately identified subagents. Those
subagents receive only their task inputs and allowed capabilities, and cannot
select or promote their own outputs.

The control plane is an iterative evidence loop.

```text
goal / user interaction / events / metrics / incidents
  -> evolution governor
  -> subgoal and task DAG
  -> proposal and implementation subagents
  -> immutable revision + evaluatable artifacts
  -> evaluation subagents and trusted harnesses
  -> metric comparison + resource/cost ledger
  -> next branch, user checkpoint, completion, or policy handoff

software target only, after approval:
  -> artifact registry -> rollout controller -> node-local loader
  -> telemetry -> rollback evidence -> evolution governor
```

Core responsibilities:

- Detect improvement opportunities from incidents, regressions, replay failures,
  stale scorecards, and user corrections.
- Convert each opportunity into a scoped evolution job.
- Maintain immutable revision identities, parent edges, artifacts, metrics, and
  task/agent/resource state for every goal.
- Keep the governor, implementers, and evaluators as separate attributable agent
  runs with explicit parent-child relationships.
- Run mutation in an isolated candidate workspace.
- Preserve complete candidate evidence.
- Evaluate candidate behavior against fixed and trace-derived gates.
- Approve or deny promotion through policy.
- Register approved artifacts with provenance.
- Roll out by module and node under local policy.
- Convert failures into rollback, new replay cases, and future opportunities.

## Campaign Proposals

A long-horizon campaign groups repeated evolution jobs under one human-provided
objective. The campaign owns staged acceptance targets, a near-term task
backlog, resource and reserve policy, approval checkpoints, and an auditable
decision history. It can redirect work toward a prerequisite when evidence
shows that the prerequisite blocks the original objective.

Phase zero is proposal-only. It persists an inspectable campaign bundle with the
interpreted objective, assumptions, non-goals, stages, metrics, tasks, budget
preview, and approval state without dispatching implementation agents or
activating candidates. When policy requires human approval, scheduled execution
cannot begin until that checkpoint is recorded.

After approval, each concrete iteration still uses the normal scoped job,
candidate bundle, evaluation, policy, registry, rollout, and rollback lifecycle.
Campaign scheduling chooses what to try next; it does not weaken the evidence or
authority required to ship a change.

## Lifecycle

The goal lifecycle is iterative rather than a single linear delivery state
machine:

```text
plan goal + metrics
  -> establish baseline revision
  -> create subgoal/task
  -> spawn bounded proposal/implementation work
  -> record candidate revision and artifacts
  -> evaluate and compare
  -> select next branch / merge / retry / ask user / complete
```

Within one revision attempt, the existing candidate state machine remains:

```text
observed
  -> scoped
  -> proposed
  -> implemented
  -> built
  -> evaluated
  -> policy-approved
  -> staged
  -> shadowed
  -> canary
  -> promoted
```

Failure paths are normal and must preserve evidence:

```text
proposed -> rejected
implemented -> build-failed
evaluated -> eval-failed
policy-approved -> denied
shadowed -> rolled-back
canary -> rolled-back
promoted -> reverted
```

Every state transition appends an event. The event log is both the audit trail
and the debugging substrate.

## User Interface

The application and CLI consume the stable evolution contract rather than
linking the module implementation. The interface contract covers:

- a goal list with status, governor, active task, and budget state;
- a revision DAG with branch, merge, selected, failed, and rollback
  states;
- per-revision content identity, artifacts, metric observations, and comparable
  deltas;
- task/subgoal dependencies and live governor/subagent activity;
- resource quantities, token/compute/wall-time usage, and monetary cost where
  known;
- the attributable event and conversation timeline;
- policy-checked controls to pause, resume, cancel, approve, reject, retry,
  branch, merge, or reprioritize work.

Reads may be eventually refreshed projections. Mutations cross the authenticated
daemon/module service route and remain subject to the same policy and authority
checks as CLI operations.

The current graphical slice renders a live goal list and revision-edge view,
metric/artifact summaries, task and agent activity, costs, recent events, and
pause/resume/complete/cancel controls. Rich graph navigation and additional
branch/merge/reprioritization controls can evolve behind the same contract.

## Candidate Bundle

A candidate bundle is the immutable evidence package for one attempted change.
It should contain or reference:

- objective and mutation packet,
- target ring and module,
- allowed and forbidden edit scope,
- candidate workspace identity and source revision,
- model/provider metadata and agent trace,
- changed files and diff,
- build logs and command logs,
- benchmark and replay artifacts,
- scorecards,
- resource usage,
- manifest and capability diffs,
- policy decision,
- provenance attestations,
- rollout material when applicable,
- recovery and rollback notes.

Candidate bundles are supply-chain artifacts. They need stable content hashes,
attestations, and registry-verifiable provenance. Mutable bookkeeping should not
invalidate the provenance payload.

## Mutation Protocol

The mutation worker receives a mutation packet, not general authority over the
repository or runtime.

A mutation packet includes:

- target ring and module,
- objective and success criteria,
- allowed workspace and write set,
- forbidden paths and capabilities,
- relevant traces, incidents, examples, and failing evals,
- baseline scorecard,
- required verification,
- output contract and risk notes.

Mutation workers must:

1. Work only in the candidate workspace.
2. Edit only within the allowed scope.
3. Use available tools under the active sandbox.
4. Capture tool, command, and model evidence.
5. Run required checks when practical.
6. Produce a candidate bundle.

Mutation workers must not:

- write the live registry,
- change active loader state,
- bypass policy,
- self-approve,
- directly roll out their own artifact.

## Module Capsule

Evolvable runtime behavior should converge on module capsules:

```text
capsule =
  executable artifact
  + manifest
  + capability declaration
  + resource limits
  + compatibility contract
  + provenance
  + rollout policy
```

The manifest is part of the signed artifact. A candidate cannot grant itself new
powers by editing code; capability changes are explicit manifest changes and
must pass policy.

The capsule lifecycle is backend-neutral. Sandboxed processes, native modules,
declarative resources, and Wasm components may all participate when they meet
the same manifest, capability, evaluation, and rollback contracts. Wasm and
typed WIT interfaces are preferred when their portability and isolation fit the
module, but they are not a requirement for every evolution R3 boundary.

## Evaluation

Evaluation is layered from cheap local checks to live rollout evidence.

| Gate | Purpose | Blocks Promotion |
|---|---|---|
| Manifest validation | schema, capability diff, compatibility | yes |
| Static checks | formatting, linting, dependency and forbidden API scans | yes |
| Unit and integration tests | local and boundary correctness | yes |
| Replay tests | old traces and incidents still behave | yes |
| Private regression evals | prevent overfitting to public benchmarks | yes |
| Public benchmark smoke | coarse harness regression signal | usually |
| Generated adversarial tests | candidate-specific weakness discovery | usually |
| Shadow mode | compare candidate decisions without acting | yes for risky modules |
| Canary analysis | limited live traffic and node subset | yes |
| Post-promotion audit | delayed regressions and corrections | triggers revert |

Scorecards should capture:

- pass/fail gates,
- correctness metrics,
- latency p50/p95/p99,
- resource usage,
- action success and failure rates,
- sandbox block rate,
- crash and timeout counts,
- user correction rate,
- policy incidents,
- module-specific quality metrics,
- sample count or confidence.

Promotion requires absolute safety gates plus relative improvement or maintained
quality. A candidate that improves one metric while regressing policy, resource
usage, or rollback readiness should not promote automatically.

## Policy

Policy decisions are made by trusted policy code, not by candidate code.

Policy inputs include:

- target ring and module,
- manifest and capability diff,
- source and dependency diff summary,
- scorecards,
- rollout plan,
- node trust domain and role,
- module blast radius,
- recent incidents,
- requested autonomy level.

Policy outputs include:

- allow, deny, defer, quarantine, require more evidence, or emergency stop,
- maximum rollout weight,
- allowed node roles,
- required additional evals,
- required shadow duration,
- rollback thresholds.

Default posture:

- Evolution R0/R1 changes need continuity-specific gates and standby handoff.
- Evolution R2 changes need strong evaluation and canary rollback.
- Evolution R3 changes may auto-promote only when scorecards, manifests,
  capabilities, and rollout telemetry stay inside policy.
- New filesystem, network, process, secret, or device capability requests need
  extra evidence.
- Changes touching policy, registry trust, artifact verification, rollback, or
  sandbox host code are treated as high-risk trusted-runtime changes.

## Rollout And Rollback

Rollout is local-authority-first. A registry can approve an artifact, but each
node decides whether it can load and run that artifact under local policy.

Rollout rules:

- Keep the previous stable artifact available for rollback.
- Start with shadow mode when candidate output can be compared safely.
- Canary action-capable modules more slowly than pure analysis modules.
- Promote independently by module, node role, trust domain, device class, or
  user/session cohort.
- Pause or roll back on degraded scorecards, policy incidents, crashes,
  timeouts, user correction spikes, resource regressions, or node health
  degradation.

A rollback is a signed desired-state change back to a known-good artifact.
Rollback events must include the failed artifact, restored artifact, triggering
evidence, affected scope, and whether rollback was local, session-wide,
trust-domain-wide, or global.

Disconnected nodes keep running their last stable approved artifact. They may
apply missed mutations after reconnecting only through the same policy, health,
compatibility, and rollback checks as online nodes.

## Distributed Operation

Self-evolution should use the cluster deliberately:

- Workstation nodes run heavy local builds, UI replays, and desktop simulations.
- Server nodes run long evals, private benchmark rotation, registry, and
  provenance storage.
- Mobile nodes participate only in relevant mobile shadow/canary paths.
- Sensor nodes receive conservative stable artifacts.

Node-local loaders enforce final authority for artifact compatibility, local
resource limits, local trust state, and rollback readiness. This keeps a single
bad approval from forcing every node into the same failure mode.

## Event Families

The event model remains open. Event kinds use typed dotted names and carry
enough structured evidence to reconstruct why a candidate was created, why it
was approved or denied, what changed during rollout, and why promotion or
rollback happened. The module emits these families:

- goal history: `evolution.goal.created`, `evolution.goal.paused`,
  `evolution.goal.resumed`, `evolution.goal.completed`, and
  `evolution.goal.cancelled`;
- revisions: `evolution.revision.proposed`, `evolution.revision.evaluated`,
  `evolution.revision.awaiting-decision`, `evolution.revision.selected`,
  `evolution.revision.failed`, and `evolution.revision.recovered`;
- orchestration: `evolution.task.updated`, `evolution.agent.updated`, and
  `evolution.resource.recorded`;
- opportunity planning: `evolution.job.scoped`, scoped from
  `evolution.metric.regression`, `evolution.replay.failure`,
  `evolution.scorecard.stale`, and module signals;
- tool use: `evolution.tool.executed`.

Module and rollout infrastructure supply the lifecycle facts that evolution
treats as evidence: `module.shadow.decision`, `module.invocation.decision`,
`module.user.correction`, `module.crash`, `module.timeout`,
`module.policy.blocked`, and `rollout.transition.completed`.

Candidate decisions, scorecards, and lessons are stored with the job and in
lesson memory and surface as revision status events; artifact registration and
rollout report through the registry and rollout authorities. Dedicated
evolution stage families added later follow the same
`evolution.<object>.<transition>` shape.

## Lessons And Memory

Failed candidates are useful. The system should preserve:

- minimized reproductions,
- replay fixtures,
- root cause summaries,
- failed assumptions,
- policy denial reasons,
- rollback evidence,
- suggested smaller or prerequisite follow-up tasks.

Repeated failures should create procedural memory and future evaluation gates.
Self-evolution should improve both runtime behavior and its own ability to
debug, evaluate, and schedule future attempts.
