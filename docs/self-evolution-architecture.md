# Sinomo Self-Evolution Architecture

## Purpose

Self-evolution is Sinomo's controlled live-mutation loop. The same machinery can
target Sinomo itself or another git-backed project. The system observes runtime
behavior, proposes bounded improvements, implements candidates in isolated
workspaces, evaluates them against replayable evidence, rolls approved artifacts
out progressively when the target supports rollout, and rolls back when evidence
degrades.

This document defines the stable architecture and invariants. Current
implementation status is tracked by the code, readiness gates, rollout
operations, and test evidence rather than by separate plan documents.

Self-evolution complements the event-centric distributed runtime in
[architecture.md](architecture.md). It is another control plane: it consumes
events, traces, metrics, workspaces, scorecards, policy decisions, and rollout
telemetry, then emits candidate bundles, artifacts, desired-state changes when
applicable, and audit events. Sinomo self-mutation is the default target when no
repository is supplied; `sinomo evolve ... --repo <path>` uses the same
candidate, verification, and evidence lifecycle for another project.

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

## Control Plane

The self-evolution control plane is a delivery pipeline.

```text
events / metrics / incidents / traces / scorecards
  -> opportunity detector
  -> evolution planner
  -> mutation worker
  -> build and evaluation harness
  -> policy gate
  -> artifact registry
  -> rollout controller
  -> node-local loader
  -> telemetry and rollback
```

Core responsibilities:

- Detect improvement opportunities from incidents, regressions, replay failures,
  stale scorecards, and user corrections.
- Convert each opportunity into a scoped evolution job.
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

Each candidate moves through a small state machine:

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

The event model should remain open, but these families are foundational:

- `evolution.opportunity.detected`
- `evolution.job.scoped`
- `evolution.proposal.created`
- `evolution.candidate.implemented`
- `evolution.eval.started`
- `evolution.eval.completed`
- `evolution.policy.approved`
- `evolution.policy.denied`
- `evolution.artifact.registered`
- `evolution.rollout.started`
- `evolution.rollout.progressed`
- `evolution.rollout.promoted`
- `evolution.rollout.paused`
- `evolution.rollback.started`
- `evolution.rollback.completed`
- `evolution.lesson.recorded`
- `module.shadow.decision`
- `module.invocation.decision`
- `module.user.correction`
- `module.crash`
- `module.timeout`
- `module.policy.blocked`
- `rollout.transition.completed`

Events should include enough structured evidence to reconstruct why a candidate
was created, why it was approved or denied, what changed during rollout, and why
promotion or rollback happened.

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
