# Sinomo Self-Evolution Architecture

## Purpose

Sinomo should be able to improve its own modules with coding LLMs without letting the live runtime become an unsafe self-modifying process. The design below treats self-evolution as a controlled, replayable, policy-gated software delivery loop: observe behavior, propose bounded improvements, implement candidates in isolated workspaces, evaluate them against verifiable gates, roll them out module-by-module and node-by-node, and roll back automatically when evidence gets worse.

This complements the existing event-centric distributed architecture in [architecture.md](architecture.md). The self-evolution system is another background control plane that consumes events, workspaces, traces, evaluation results, and policy incidents, then emits versioned artifacts and rollout events.

## Research Takeaways

- WebAssembly is a good default isolation boundary for extension modules because Wasm guests only access host functionality through explicit imports, and WASI adds capability-oriented filesystem access. Wasmtime documents memory isolation, checked control flow, explicit imports/exports, and WASI capability-based filesystem access as core sandbox properties. See [Wasmtime security](https://docs.wasmtime.dev/security.html).
- The WebAssembly Component Model fits Sinomo's modularity goal because it uses typed interfaces and WIT worlds to define what a component provides and depends on. See [Bytecode Alliance Component Model concepts](https://component-model.bytecodealliance.org/design/component-model-concepts.html).
- Iterative LLM improvement works best when feedback is grounded. Self-Refine uses generate-feedback-refine loops without model training, while Reflexion stores language reflections from task feedback in memory for later attempts. See [Self-Refine](https://arxiv.org/abs/2303.17651) and [Reflexion](https://arxiv.org/abs/2303.11366).
- Self-evolving code agents are moving toward generation-verification loops rather than unconstrained rewriting. ReVeal emphasizes iterative generation-verification with tool feedback, and Live-SWE-agent explores runtime scaffold evolution while solving software tasks. See [ReVeal](https://www.microsoft.com/en-us/research/publication/reveal-self-evolving-code-agents-via-reliable-self-verification/) and [Live-SWE-agent](https://huggingface.co/papers/2511.13646).
- Evaluation must stay fresh and task-relevant. SWE-bench demonstrates reproducible software-agent evaluation with Docker, while OpenAI's 2026 analysis warns that SWE-bench Verified no longer measures frontier coding progress well because of contamination and test issues. Use fixed public benchmarks only as coarse regression signals; Sinomo needs private, rotating, replay-derived evals. See [SWE-bench](https://www.swebench.com/SWE-bench/) and [OpenAI on SWE-bench Verified](https://openai.com/index/why-we-no-longer-evaluate-swe-bench-verified/).
- Rollout should borrow from progressive delivery. Kubernetes rolling updates use availability budgets and revisions; Argo Rollouts can abort canaries when analysis fails. Sinomo should apply the same idea at module and node granularity. See [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) and [Argo canary analysis](https://argo-rollouts.readthedocs.io/en/stable/features/canary/).
- Artifact integrity matters because an autonomous loop is now part of the supply chain. SLSA provides a framework for provenance and supply-chain integrity, and in-toto attestations define verifiable claims about how software was produced. See [SLSA](https://slsa.dev/) and [in-toto attestations](https://github.com/in-toto/attestation).
- Policy should be externalized from the mutating code. OPA's policy model decouples policy decisions from enforcement and evaluates structured input against policy as code. Sinomo does not need to adopt OPA immediately, but should use the same separation. See [Open Policy Agent](https://www.openpolicyagent.org/docs).

## Design Position

The system should not let arbitrary runtime code rewrite the trusted control plane. Instead, split Sinomo into immutable or slowly changing trust rings:

| Ring | Contents | Evolution rule |
|---|---|---|
| R0 immutable root | event envelope, ID contracts, artifact verifier, policy verifier, rollback loader, emergency stop | human-reviewed only |
| R1 trusted runtime | node supervisor, event store, DFS metadata, sandbox host, rollout controller | human approval for now; future gated auto-proposals only |
| R2 trusted services | perception, memory, trigger, agent, deliberation, action services | candidate auto-evolution after strong evaluation |
| R3 sandboxed modules | skills, tools, evaluators, adapters, prompts, planners, local policies | primary auto-evolution target |
| R4 disposable workspaces | mutation branches, test sandboxes, generated fixtures, traces | fully automated but isolated |
| R5 model providers | local or remote LLMs used for proposal, implementation, review, critique | no direct authority; only produce artifacts or judgments |

Early self-evolution should target R3 first: prompts, skills, evaluators, tool adapters, planning policies, and low-risk action modules. R2 evolution can come later once the evaluation harness and rollback machinery have earned trust. R0 and R1 are deliberately boring; boring is a feature here.

## Runtime Components

```text
Event Log / Metrics / Incidents / Workspaces
  -> Evolution Orchestrator
  -> Candidate Planner
  -> Mutation Worker Pool
  -> Sandbox Build + Test Harness
  -> Evaluation Board
  -> Policy Gate
  -> Artifact Registry
  -> Rollout Controller
  -> Node-local Module Loaders
```

### `sinomo-evolution`

Owns the lifecycle of evolution jobs.

Responsibilities:

- Detect improvement opportunities from metrics, incidents, replay failures, user corrections, code TODOs, and stale module scorecards.
- Create `EvolutionJob` records with strict scope, allowed write set, target module, evaluation suite, and rollback policy.
- Coordinate proposal, implementation, review, build, evaluation, approval, rollout, rollback, and archival.
- Emit `evolution.*` events for every state transition.

### `sinomo-registry`

Stores versioned module artifacts and provenance.

Responsibilities:

- Track module manifests, semantic versions, content hashes, signatures, compatibility constraints, and node rollout state.
- Store in-toto-like attestations for who or what produced the artifact, which source revision was used, which tests ran, and which policies approved it.
- Expose immutable artifact references to node-local loaders.

### `sinomo-sandbox`

Executes candidate modules and test jobs in constrained environments.

Responsibilities:

- Run Wasm components through Wasmtime for R3 modules.
- Run non-Wasm build/test jobs in disposable process or container sandboxes until they can be narrowed into Wasm.
- Enforce CPU, memory, wall-clock, filesystem, network, secret, and device capability limits.
- Redact terminal control sequences and normalize logs before showing them to LLMs or operators.

### `sinomo-eval`

Owns evaluation definitions and scorecards.

Responsibilities:

- Run static checks, unit tests, integration tests, replay tests, simulation tests, regression suites, red-team probes, and canary analysis.
- Keep private rotating evals derived from Sinomo's own event traces and failure cases.
- Prevent candidate-generated tests from being the only gate. LLM-generated tests are useful as evidence, not authority.
- Produce machine-readable scorecards with pass/fail gates and weighted metrics.

### `sinomo-rollout`

Promotes approved artifacts across modules and nodes.

Responsibilities:

- Support shadow, canary, blue/green, rolling, and immediate rollback strategies.
- Roll out by module, node role, trust domain, device class, session, or user opt-in group.
- Keep old versions hot enough for fast rollback.
- Stop promotion when canary analysis, policy incidents, crash rate, latency, or user corrections regress.

## Module Capsule

Every evolvable module should be packaged as a capsule: code artifact plus manifest plus policy plus provenance.

```toml
[module]
id = "module.trigger.silence_policy"
kind = "wasm-component"
version = "0.3.7"
api_version = "sinomo-module-v1"
owner = "sinomo-trigger"

[artifact]
content_hash = "sha256:..."
signature = "sigstore-or-local-dev-signature"
source_workspace = "workspace_repo_sinomo_agent"
source_revision = "git:..."

[capabilities]
imports = [
  "event.read.session",
  "memory.read.hot",
  "metrics.emit",
]
exports = [
  "trigger.evaluate",
]

[limits]
cpu_ms = 50
memory_mb = 64
wall_time_ms = 100
network = "deny"
filesystem = "none"

[rollout]
strategy = "canary"
initial_weight = 0.05
promotion_steps = [0.05, 0.25, 0.50, 1.0]
rollback_on = ["panic", "latency_p95_regression", "policy_incident", "score_regression"]
```

The manifest is part of the signed artifact. A candidate cannot grant itself new powers by editing code; capability requests are visible manifest changes and must pass policy.

## Component Protocol

R3 modules should converge on Wasm Component Model interfaces. Exact WIT can evolve, but the boundary should be typed and capability-oriented.

```wit
package sinomo:module-v1;

interface events {
  record event-ref {
    event-id: string,
    kind: string,
  }

  record event-envelope {
    event-id: string,
    origin-node-id: string,
    session-id: option<string>,
    kind: string,
    payload-json: string,
  }
}

interface module {
  record module-context {
    node-id: string,
    module-id: string,
    module-version: string,
    budget-ms: u64,
    capabilities-json: string,
  }

  record module-result {
    status: string,
    events-json: list<string>,
    metrics-json: string,
  }

  run: func(context: module-context, input-json: string) -> module-result;
}
```

Use JSON inside the first WIT boundary only where schemas are still unstable. As contracts settle, promote frequent payloads into typed records.

## Evolution State Machine

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

Failure paths:

```text
proposed -> rejected
implemented -> build-failed
evaluated -> eval-failed
policy-approved -> denied
shadowed -> rolled-back
canary -> rolled-back
promoted -> reverted
```

Each transition appends an event. The event log is the audit trail and the debugging substrate.

## Event Families

Add these open event families to the existing event model:

- `evolution.opportunity.detected`
- `evolution.job.scoped`
- `evolution.proposal.created`
- `evolution.proposal.reviewed`
- `evolution.candidate.implemented`
- `evolution.candidate.built`
- `evolution.eval.started`
- `evolution.eval.completed`
- `evolution.policy.approved`
- `evolution.policy.denied`
- `evolution.artifact.registered`
- `evolution.rollout.started`
- `evolution.rollout.progressed`
- `evolution.rollout.paused`
- `evolution.rollout.promoted`
- `evolution.rollback.started`
- `evolution.rollback.completed`
- `evolution.lesson.recorded`

Example payload:

```json
{
  "job_id": "task_01...",
  "target_module_id": "module.trigger.silence_policy",
  "target_ring": "r3-sandboxed-module",
  "scope": {
    "allowed_paths": ["crates/sinomo-trigger/**", "modules/trigger/**"],
    "forbidden_paths": ["crates/sinomo-core/**", "docs/security/**"]
  },
  "objective": "Reduce false positive interruptions during low-salience screen changes",
  "baseline": {
    "scorecard_id": "score_2026_04_24_trigger_baseline",
    "latency_p95_ms": 41,
    "false_interrupt_rate": 0.081
  },
  "gates": [
    "cargo-test-workspace",
    "trigger-replay-private-v3",
    "latency-no-regression",
    "policy-no-new-capabilities"
  ]
}
```

## Mutation Protocol

The mutation protocol is intentionally narrow. A coding LLM receives a mutation packet, not general authority over the repo.

Mutation packet:

- Target module and ring.
- Problem statement and objective metric.
- Allowed files or DFS workspace mount.
- Forbidden files and capabilities.
- Relevant traces, incidents, failing evals, and examples.
- Baseline scorecard.
- Required verification commands.
- Output contract: patch, manifest diff, self-review, test notes, risk notes.

Mutation worker steps:

1. Create a DFS snapshot or branch workspace.
2. Generate a proposal before editing.
3. Apply edits only inside the allowed write set.
4. Add or update tests when useful.
5. Run required local checks in a sandbox.
6. Produce a structured self-review and risk summary.
7. Emit an immutable candidate bundle.

No mutation worker can directly update the live registry or active module loader.

## Debugging Protocol

Every candidate must be debugged through replayable evidence:

- Build logs and test logs are captured as artifacts.
- Failed evals attach minimized reproduction inputs when possible.
- Event replay tests reference event ranges, not opaque screenshots or transcripts alone.
- LLM reviewers receive reduced traces, relevant code, scorecard deltas, and policy output.
- Debugging attempts create child candidates rather than rewriting history.
- Repeated failures produce `evolution.lesson.recorded` events for procedural memory.

For runtime failures:

- The node-local loader emits `module.crash`, `module.timeout`, or `module.policy.blocked`.
- The rollout controller pauses or rolls back depending on policy.
- The evaluator creates a replay test from the failing live trace if privacy allows.

## Evaluation Gates

Evaluation should be layered from cheap to expensive:

| Gate | Purpose | Blocks promotion? |
|---|---|---|
| Manifest validation | schema, capability diff, compatibility | yes |
| Static checks | formatting, linting, dependency audit, forbidden API scan | yes |
| Unit tests | local correctness | yes |
| Integration tests | service boundary correctness | yes |
| Replay tests | old event traces still behave | yes |
| Private regression evals | prevent overfitting to public benchmarks | yes |
| Generated adversarial tests | find candidate-specific weaknesses | usually |
| Shadow mode | compare candidate decisions without acting | yes for risky modules |
| Canary analysis | live traffic subset and node subset | yes |
| Post-promotion audit | delayed regressions and user corrections | triggers revert |

Scorecards should include:

- correctness pass/fail,
- latency p50/p95/p99,
- memory and CPU budget use,
- action success/failure rates,
- sandbox block rate,
- crash and timeout counts,
- user correction rate,
- policy incidents,
- module-specific quality metrics,
- confidence interval or sample count.

Promotion requires absolute gates plus relative improvement. For example: no new policy incidents, no correctness regression, latency p95 within budget, and at least one target metric improves beyond noise.

## Rollout and Rollback

Rollout is local-authority-first, matching Sinomo's distributed safety model.

Rules:

- A home registry may approve an artifact, but each node decides whether it can load it under local policy.
- A node keeps the previous stable version until the new version passes local health checks.
- R3 modules support shadow mode by default when their output can be compared without action.
- Action-capable modules canary more slowly than pure analysis modules.
- Mobile, desktop, server, and sensor nodes can have different promotion schedules.
- A rollback is just a new signed desired-state event pointing back to a known-good artifact.

Rollback triggers:

- build or load failure,
- module panic,
- timeout threshold,
- resource budget violation,
- new denied capability request,
- policy incident,
- canary score regression,
- operator emergency stop,
- user correction spike,
- node health degradation correlated with the rollout.

Rollback events must include the failed artifact, restored artifact, triggering evidence, affected nodes, and whether the rollback is local, session-wide, trust-domain-wide, or global.

## Policy Model

Policy decisions should be made by a trusted policy engine, not by candidate code.

Policy inputs:

- target ring,
- target module,
- manifest diff,
- capability diff,
- source diff summary,
- dependency diff,
- evaluation scorecard,
- rollout plan,
- node trust domain,
- module blast radius,
- recent incidents.

Policy outputs:

- allow, deny, or require human review,
- maximum rollout weight,
- allowed node roles,
- required additional evals,
- required shadow duration,
- rollback thresholds.

Default policy:

- R0/R1 changes always require human review.
- R2 changes require human review until private evals and canary machinery are mature.
- R3 changes may auto-promote only with no new capabilities and passing scorecards.
- Any new filesystem, network, process, secret, or device capability requires explicit review.
- Any mutation touching policy code, artifact verification, registry trust, rollback logic, or sandbox host code requires human review.

## Distributed Operation

Self-evolution should exploit Sinomo's distributed shape:

- Workstation nodes run heavy builds, local UI replays, and desktop action simulations.
- Server nodes run long evals, private benchmark rotation, artifact registry, and provenance storage.
- Mobile nodes run only modules relevant to their role and can participate in shadow/canary for mobile-specific behavior.
- Sensor nodes receive conservative updates and mostly run signed stable artifacts.

Node-by-node flow:

1. Registry publishes `evolution.rollout.started` with desired artifact and rollout plan.
2. Candidate nodes check artifact signature, manifest compatibility, local policy, and resource limits.
3. Nodes load the candidate beside the stable module.
4. Shadow decisions are emitted as `module.shadow.decision` events.
5. Canary nodes route a small percentage of eligible work to the candidate.
6. Rollout controller compares stable and candidate scorecards.
7. Promotion advances, pauses, or rolls back.

Disconnected nodes keep running their last stable approved artifact. They do not auto-apply missed mutations when reconnecting; they reconcile desired state through the same policy and health checks as online nodes.

## Data Model Additions

Initial types can live in `sinomo-core` as serializable placeholders, then move behavior into service crates.

```rust
pub struct ModuleId(String);
pub struct ArtifactId(String);
pub struct EvolutionJobId(TaskId);

pub struct ModuleManifest {
    pub module_id: ModuleId,
    pub version: String,
    pub kind: ModuleKind,
    pub api_version: String,
    pub imports: Vec<CapabilityName>,
    pub exports: Vec<CapabilityName>,
    pub limits: ResourceLimits,
}

pub struct EvolutionJob {
    pub job_id: TaskId,
    pub target_module_id: ModuleId,
    pub target_ring: TrustRing,
    pub objective: String,
    pub status: EvolutionStatus,
}

pub struct EvaluationScorecard {
    pub scorecard_id: String,
    pub artifact_id: ArtifactId,
    pub gates: Vec<EvaluationGateResult>,
    pub metrics: serde_json::Value,
}

pub struct RolloutPlan {
    pub artifact_id: ArtifactId,
    pub strategy: RolloutStrategy,
    pub target_nodes: Vec<NodeId>,
    pub rollback_policy: RollbackPolicy,
}
```

Use open string-backed IDs if these need to settle before hard prefix decisions.

## MVP Implementation Plan

### Phase SE-0: Documentation and Contracts

- Add this design document.
- Add `evolution.*` and `module.*` event examples to docs.
- Add placeholder domain types for module manifests, scorecards, and evolution jobs.

### Phase SE-1: Offline Evolution Harness

- Add `sinomo-eval` with local command-based scorecards.
- Add private replay fixture format using existing `EventEnvelope`.
- Add `sinomo-evolution` job records backed by the in-memory event store first.
- Make candidate generation manual or operator-triggered; no auto-apply.

### Phase SE-2: Registry and Provenance

- Add artifact registry metadata.
- Store candidate bundles with content hashes.
- Add in-toto-style attestation JSON.
- Add signature verification in the loader path.

### Phase SE-3: Sandboxed R3 Modules

- Add `sinomo-sandbox` with Wasmtime.
- Define the first WIT module interface for a low-risk module.
- Convert one policy-like or evaluator-like module to a Wasm component.
- Run it in shadow mode against replay traces.

### Phase SE-4: Progressive Rollout

- Add rollout desired-state records and node-local loader state.
- Support shadow and canary for one module family.
- Implement automatic rollback on timeout, panic, score regression, or policy incident.

### Phase SE-5: Automated Proposal Loop

- Allow the evolution orchestrator to open candidate jobs from metrics and incidents.
- Keep implementation automated but promotion conservative.
- Require human approval for new capabilities, R2 changes, and policy-sensitive diffs.

## Near-Term Recommendation

Start with R3 modules and private replay evals. The first useful demo should be:

1. A low-risk trigger or evaluator module packaged as a Wasm component.
2. A replay suite built from synthetic or captured `EventEnvelope` fixtures.
3. A mutation job that improves the module or its prompt/config.
4. A scorecard showing baseline versus candidate.
5. Shadow rollout on one local node.
6. Automatic rollback when an intentionally bad candidate is introduced.

That proves the core mechanism without giving the system authority over its own foundation too early.
