# Human-Guided Long-Horizon Evolution Design

## Purpose

This document refines the first practical self-evolution mechanism for Sinomo: a human-guided, long-running evolution campaign started from a concrete objective such as:

```text
sinomo evolve propose --objective "speedrun minecraft"
```

or:

```text
sinomo evolve propose --objective "make a 15 minute video explaining X using available video generation model APIs"
```

The important shift is that an evolution task is not a one-shot coding request. It is a scheduled campaign with an explicit objective, staged evaluation targets, resource budgets, repeated implementation/evaluation/rollout iterations, and adaptation when attempted changes fail.

The framework should remain flexible. The objective gives direction, but it should not over-constrain the implementation. If the implementation agent discovers that the best next step is to add a tool, rewrite a subsystem, create a new evaluator, change the distributed system foundation, improve scheduling, build an artifact-inspection pipeline, or abandon a proposed abstraction, the campaign should be able to absorb that discovery.

Self-evolution is therefore not defined as “implementing capabilities.” It is defined as:

```text
inspect the current system and objective
  -> choose a useful next improvement
  -> implement or propose the improvement
  -> evaluate it with evidence
  -> roll it out or reject it
  -> adapt the next step
  -> repeat
```

New abilities may emerge during a campaign, but the framework should not require every improvement to be modeled as a formal capability. A capability system can be useful as a note-taking or evaluation aid. It should not be the foundation of the mechanism.

The first version should be human-guided but system-driven:

- the human supplies the objective and optional constraints;
- Sinomo inspects the current state and available resources;
- Sinomo proposes measurable staged targets;
- Sinomo proposes near-term tasks;
- Sinomo schedules work around resource budgets and reset windows;
- Sinomo calls planning, scouting, implementation, evaluation, review, and rollout agents;
- Sinomo evaluates candidate completeness and performance;
- Sinomo rolls out accepted improvements;
- Sinomo learns from failed rollout attempts and proposes smaller, easier, broader, or more foundational next tasks;
- Sinomo continues until the final objective is achieved, cancelled, superseded, paused by budget/policy, or replaced by a better objective interpretation.

## Relationship To Existing Self-Evolution Docs

This plan extends the existing R0/R1 bootstrap and self-evolution architecture:

- `docs/self-evolution-architecture.md` describes controlled live mutation, evidence gates, registry, rollout, and rollback.
- `docs/plans/r0-r1-self-evolution-bootstrap-plan.md` describes the local supervised loop for improving the R0/R1 foundation.
- `docs/plans/r0-r1-bootstrap-technical-architecture.md` describes the current executable bootstrap shape: objective, job, mutation packet, workspace, verification, bundle, and approval.

This document adds the missing long-horizon campaign layer above those primitives. A campaign owns the goal, schedule, staged evals, budget policy, near-term backlog, and adaptation policy. Each concrete candidate still uses the existing evolution job, candidate bundle, eval scorecard, registry, rollout, and rollback machinery.

The campaign layer should not replace lower-level evidence gates. It decides what to try next. It does not decide that an unverified change is safe.

## Design Position

The first self-evolution mechanism should be a guided campaign manager, not a fully autonomous open-ended optimizer.

The campaign manager is responsible for choosing what to try next and when to spend resources. It does not dictate the exact technical solution. Every concrete change still becomes a scoped evolution job with a candidate workspace, mutation packet, verification commands, scorecard, policy decision, artifact record, and rollout state.

The campaign should optimize for useful progress, not for fitting a predefined ontology.

A good campaign can produce many kinds of improvements:

```text
code patch
configuration change
new test or benchmark
new tool or adapter
new artifact pipeline
new prompt or workflow
new evaluator
new logging or trace collection
new distributed-system primitive
new scheduler behavior
new rollout guard
new documentation or operator guidance
new proposal that changes the campaign plan
```

The framework should allow the implementation agent to say:

```text
The selected task is not the right next step.
The real blocker is lower-level infrastructure.
I propose this smaller/foundational change instead.
```

That proposal is then evaluated by the campaign manager, review agent, resource policy, and human approval policy.

## Capability System Policy

The previous revision made “capability cards” central. This version deliberately removes that requirement.

A campaign may still record lightweight ability notes when useful:

```text
Sinomo can now sample frames from a generated video.
Sinomo can now launch a local Minecraft fixture.
Sinomo can now pause and resume a scheduled evolution job.
```

But these notes are optional. They are diagnostic memory, not the core data model.

Rules:

1. No campaign state transition should depend on a formal capability registry in the first version.
2. No implementation agent should be forced to express its solution as a capability.
3. No stage should fail merely because the result does not fit a capability taxonomy.
4. If lightweight ability notes become confusing or unhelpful, the campaign can drop them.
5. The source of truth remains evidence: scorecards, artifacts, tests, traces, logs, rollout records, and human approval.

This keeps the design general enough for tasks where the useful change is not a new tool at all. For example, the best next step may be to improve the distributed scheduler, change the storage layout, add retry semantics, improve context packing, or replace a brittle abstraction.

## Objective As Compass, Not Cage

The campaign is goal-oriented, but it should not be narrowly goal-limited.

The objective provides:

```text
priority
success direction
acceptance metrics
resource justification
human intent
```

It should not forbid the system from improving foundations that are blocking the objective.

Examples:

- If `speedrun minecraft` reveals that the agent runtime cannot handle continuous observation/action loops, a valid near-term task may be to improve the runtime loop or event bus.
- If `make a 15 minute video` reveals that artifact storage cannot track hundreds of generated clips, a valid near-term task may be to improve artifact indexing.
- If a campaign repeatedly fails because implementation packets are too poorly scoped, a valid task may be to improve context collection or mutation-packet generation.
- If scheduling around resource reset windows is unreliable, a valid task may be to update the distributed system foundation before pursuing the original task again.

The campaign should remain anchored to the original objective, but it can temporarily work on prerequisite infrastructure when evidence shows that this is the best path.

A useful rule:

```text
A foundation-improvement task is valid when it explains which campaign blocker it removes and how it will be evaluated.
```

Foundation work also needs a return-to-objective check:

```text
After a foundation-improvement task completes, the campaign re-runs or re-scores the blocked objective stage. If the foundation work does not improve the blocker, similar foundation work loses priority until new evidence justifies it.
```

This prevents foundation repair from becoming an infinite escape hatch while still allowing the campaign to improve prerequisites when they are genuinely blocking progress.

## Core Vocabulary

| Term | Meaning |
|---|---|
| Objective | Human-provided goal, written in natural language. |
| Campaign | Long-running background job created from one objective. |
| Stage | A measurable milestone toward the objective. |
| Evaluation target | A stage-level scorecard definition with acceptance thresholds. |
| Near-term task | A small piece of work intended to improve one metric, unblock one blocker, add instrumentation, or repair one foundation issue. |
| Iteration | One concrete attempt to implement, evaluate, and optionally roll out a near-term task. |
| Candidate | The artifact, patch, config, workflow, evaluator, or proposal produced by an iteration. |
| Resource budget | Time-windowed allowance for model calls, coding-agent sessions, local compute, network APIs, paid APIs, and human attention. |
| Resource lease | A temporary reservation granted to an agent or iteration. |
| Adaptation | The process of changing the next step after failure or discovery. The next step may become easier, narrower, better-instrumented, more foundational, or differently framed. |
| Ability note | Optional informal record that Sinomo appears to have gained or improved some ability. Not a required registry entry. |
| Campaign decision | Auditable record of why the campaign selected a task, rejected alternatives, spent resources, paused, resumed, promoted a stage, or changed direction. |

## Command Shape

The human-facing entry point should accept an objective and optional constraints, then create a campaign proposal before executing substantial work.

Example:

```text
sinomo evolve propose \
  --objective "speedrun minecraft" \
  --horizon "30 days" \
  --budget-policy "free-and-prepaid" \
  --reserve "coding-plan-weekly:30%" \
  --reserve "free-api-openrouter:40%" \
  --approval "stage-rollout" \
  --dry-run
```

Another example:

```text
sinomo evolve propose \
  --objective "make a 15 minute video explaining the history of topic X using available video generation APIs" \
  --horizon "14 days" \
  --budget-policy "free-and-approved-video-api" \
  --approval "every-paid-api-family" \
  --dry-run
```

Initial command modes:

| Mode | Behavior |
|---|---|
| `--dry-run` | Inspect state and resources, then write a campaign proposal without executing agents. |
| `--approve` | Mark the proposed campaign as approved for scheduled execution. |
| `--pause-after-proposal` | Default safe mode: produce proposal and wait for human approval. |
| `--max-wall-time` | Bound the first planning run. |
| `--budget-policy` | Select a resource policy profile. |
| `--approval` | Choose approval checkpoints: proposal-only, stage-rollout, every-rollout, or manual-only. |

The command should write a campaign bundle and emit events even in dry-run mode. This makes the proposal inspectable and resumable.

## Campaign Lifecycle

```text
objective submitted
  -> current state inspected
  -> resources inventoried
  -> objective clarified into measurable outcomes
  -> baseline measured or estimated
  -> stages proposed
  -> evaluation targets proposed
  -> near-term backlog proposed
  -> resource schedule proposed
  -> campaign awaits approval
  -> campaign scheduled
  -> iteration lease acquired
  -> implementation agent receives a goal-oriented packet
  -> implementation agent produces a candidate or proposes a better next task
  -> candidate evaluated
  -> candidate reviewed and policy checked
  -> candidate rolled out or rejected
  -> stage scorecard updated
  -> next near-term task selected or adapted
  -> repeat until final objective is done, paused, cancelled, superseded, or exhausted
```

Campaign state machine:

```text
Draft
  -> AwaitingApproval
  -> Scheduled
  -> Running
  -> PausedBudget
  -> PausedPolicy
  -> PausedHuman
  -> StageComplete
  -> Complete
  -> Cancelled
  -> Superseded
  -> FailedExhausted
```

Iteration state machine:

```text
Proposed
  -> ResourceLeaseRequested
  -> ResourceLeaseGranted
  -> AgentRunning
  -> CandidateProduced
  -> Evaluating
  -> AwaitingRolloutApproval
  -> RollingOut
  -> RolledOut
  -> Rejected
  -> RolledBack
  -> Adapted
```

A candidate can also be a task-replacement proposal rather than a patch. This is important when the implementation agent discovers that the selected task is not the right next step.

## Objective Refinement

A raw objective is not executable. The first campaign planner turns it into a structured objective card.

Objective card fields:

```text
objective_id
human_text
interpreted_goal
non_goals
assumptions
constraints
success_metrics
failure_conditions
safety_notes
resource_policy
approval_policy
initial_horizon
assumptions_needing_human_review
```

For ambiguous goals, the first version should make assumptions explicit rather than blocking on repeated clarification. The proposal can include `assumptions_needing_human_review`. If a missing answer materially changes safety, spending, privacy, legality, or target environment, the campaign remains `AwaitingApproval` until reviewed.

Example for `speedrun minecraft`:

```text
interpreted_goal:
  Build and improve the Sinomo system until it can autonomously make progress
  toward Minecraft speedrunning objectives in a controlled local environment.

non_goals:
  No public-server automation.
  No anti-cheat bypass.
  No account-risk behavior.
  No paid API overage.

initial staged targets:
  1. controlled game setup and baseline observation/action experiment
  2. reliable low-level interaction or an alternative controlled interface
  3. primitive survival tasks in scripted or fixed worlds
  4. route execution toward an intermediate objective
  5. speed and reliability improvement over repeated attempts
```

Example for a 15-minute video:

```text
interpreted_goal:
  Build and improve a repeatable workflow that can plan, generate, inspect,
  revise, and assemble a 15-minute video from shorter generated segments.

non_goals:
  No unapproved paid API overage.
  No unreviewed public publishing.
  No use of copyrighted assets unless explicitly approved.

initial staged targets:
  1. generate and save one short video clip
  2. inspect generated clips through sampled frames or another practical method
  3. generate several clips from a scene plan and select/retry weak outputs
  4. compose clips into a short draft
  5. scale the pipeline to a full 15-minute draft
  6. perform final QA and export checks
```

## Staged Evaluation Targets

Each stage must be falsifiable. A stage is not just a task list; it is a metric checkpoint.

Stage fields:

```text
stage_id
name
purpose
baseline
acceptance_metrics
regression_gates
required_artifacts
allowed_task_types
eval_suite
estimated_budget
promotion_policy
```

Good stages should be monotonic when possible: passing Stage 3 should continue to imply Stage 1 and Stage 2 still pass. If a later stage requires a different environment, the old stages become regression gates.

Stage design should stay outcome-oriented. It should not prescribe a capability taxonomy. For example:

```text
Bad:
  Implement capability class VideoInspectionCapability.

Better:
  Given three generated clips and their scene intents, produce a QA report
  that correctly identifies at least two intentionally bad clips and saves
  evidence for the decision.
```

Generic stage pattern:

| Stage type | Purpose | Example acceptance metric |
|---|---|---|
| Access/setup | Reach the relevant environment, provider, project, artifact, or runtime. | Minimal operation succeeds and is recorded. |
| Observation/inspection | Inspect the relevant state or artifact well enough to make decisions. | Output includes metadata, evidence, and useful labels. |
| Action/generation | Produce, modify, or control something in a bounded way. | One minimal output is created and verified. |
| Evaluation | Judge whether outputs or behaviors satisfy intent. | Scorecard distinguishes pass/fail on known examples. |
| Composition | Combine smaller successful pieces into larger workflows. | Multi-step result is produced without manual glue. |
| Reliability | Repeat the workflow with acceptable success, quality, cost, and runtime. | Threshold passes over several samples or fixtures. |
| Optimization | Improve speed, cost, quality, or robustness without regression. | Target metric improves while regression gates remain green. |
| Foundation repair | Improve the underlying evolution/runtime/distributed system when it blocks progress. | The blocker is reproduced before the patch and absent after the patch. |

The `Foundation repair` stage type is intentionally included. Long-horizon evolution will expose weaknesses in the mechanism itself. Fixing those weaknesses is part of self-evolution.

## Near-Term Task Selection

The campaign manager should keep a rolling backlog of near-term tasks. Tasks should be small enough that a failed rollout teaches something useful.

Task selection inputs:

- current stage target;
- latest scorecard;
- failed eval logs;
- rollout incidents;
- available resources in the current budget window;
- expected task cost;
- expected metric impact;
- risk and blast radius;
- prerequisite blockers;
- implementation-agent feedback;
- human priorities.

Task priority score can start simple:

```text
priority = expected_metric_gain
         * confidence
         * prerequisite_unblock_value
         / (estimated_resource_cost * rollout_risk * task_complexity)
```

The exact formula should remain explainable. Every selected task should include why it was chosen and why the campaign did not choose the top rejected alternatives. This explanation should be stored as a campaign decision record, not only as transient reasoning in an agent transcript.

The campaign should not force all next steps to be implementation patches. Valid near-term tasks include:

```text
add instrumentation
write an eval fixture
inspect previous failures
reduce ambiguity in a metric
improve context collection
repair scheduler/runtime/storage/event bus
create one minimal tool integration
try a cheap prototype
write a design sketch for a risky subsystem
run a baseline measurement
produce an artifact for human review
```

Task selection order should usually be:

```text
1. If nothing can be measured, add measurement.
2. If failures are opaque, add logs/traces/artifact capture.
3. If the selected task is too broad, split it.
4. If the infrastructure blocks execution, repair the infrastructure.
5. If a minimal operation is missing, implement the smallest useful version.
6. If the minimal operation works once, improve reliability.
7. If pieces work independently, compose them.
8. If the workflow works end to end, optimize.
```

This ordering is a heuristic, not a hard ontology. The implementation agent may challenge it with evidence.

## Implementation Agent Freedom

The implementation agent should receive a goal-oriented implementation packet, not a rigid capability contract.

Implementation packet fields:

```text
task_id
campaign_id
stage_id
objective_summary
selected_task_intent
why_this_task_was_selected
current_evidence
known_failures
required_verification
resource_lease
allowed_paths_or_artifact_scope
forbidden_actions
approval_boundaries
expected_output_shape
```

The implementation agent may return one of several result types:

```text
PatchCandidate
ArtifactCandidate
WorkflowCandidate
ConfigCandidate
EvalCandidate
InstrumentationCandidate
FoundationRepairCandidate
TaskReplacementProposal
DesignReviewOnly
NoOpWithReason
```

This is the main flexibility point.

The implementation agent is allowed to decide that:

- the selected task is too high-level;
- the selected task is too low-level;
- the proposed technical approach is wrong;
- an evaluator is needed before implementation;
- the distributed system foundation must be repaired first;
- a different tool/API/interface would satisfy the stage better;
- the campaign should update its stage definition or backlog.

The implementation agent is not allowed to bypass:

- resource leases;
- forbidden actions;
- rollout policy;
- safety constraints;
- approval boundaries;
- required verification for rollout.

A good implementation packet says:

```text
Here is the outcome we need, here is the evidence we currently have,
here are the constraints, here are the checks that will judge the result.
Find the best scoped change or explain why a different next step is better.
```

It should not say:

```text
You must implement this exact capability interface even if the interface is wrong.
```

## Resource Budget Model

The resource scheduler is part of the self-evolution mechanism. Without it, a long-running campaign will either stall or consume all available model/API quota immediately.

Track resources as time-windowed buckets:

| Resource class | Examples | Budget behavior |
|---|---|---|
| Prepaid coding plan | Fixed-price coding assistant sessions resetting every few hours or weekly | Use steadily, reserve a floor for urgent work, avoid burst exhaustion. |
| Free API models | Free OpenRouter/HF/provider models | Prefer for scouting, summarization, critique, cheap eval labeling; respect rate limits. |
| Local compute | Workstation/server CPU/GPU, disk, network | Schedule heavy jobs during idle windows; cap concurrency. |
| External generation APIs | Video/image/audio generation providers | Disabled or capped unless explicitly allowed. |
| Paid on-demand APIs | Any metered paid model/tool | Disabled by default for the first mechanism unless explicitly allowed. |
| Human attention | approval/review prompts | Batch prompts at proposal, stage boundaries, and policy-sensitive rollouts. |

Budget policy fields:

```text
policy_id
allowed_resource_classes
forbidden_resource_classes
per_campaign_limits
per_window_limits
reserve_floors
reset_windows
burst_limits
idle_window_rules
priority_overrides
usage_confidence_policy
budget_debt_policy
emergency_stop_rules
```

A resource lease should include:

```text
lease_id
campaign_id
iteration_id
resource_kind
provider
model_or_tool
estimated_cost
hard_limit
expires_at
reservation_reason
usage_confidence
budget_debt_limit
```

Track usage confidence explicitly:

```text
Exact             # measured locally or provider returns exact units/cost
ProviderReported  # provider reports usage, but timing/rounding may lag
Estimated         # inferred from tokens, wall time, file size, or heuristic
Unknown           # resource was consumed but accounting could not be trusted
```

Also track budget debt: actual usage beyond the estimate but still inside the hard limit. Repeated debt should lower confidence in future estimates and may force smaller leases or stronger approval.

The campaign manager can only dispatch an agent after acquiring leases for the resources that agent may consume. Agents should report actual usage back to the ledger. If a provider cannot report exact token/cost usage, the scheduler records an estimate with lower confidence.

## Scheduling Policy

The scheduler should gradually consume renewable resources instead of spending each reset window immediately.

Initial rules:

1. Keep reserve floors. Example: keep 30% of a weekly coding plan and 40% of free API quota unallocated for non-campaign operations unless the human overrides.
2. Smooth usage across the reset window. A 5-hour window should have per-hour suggested spend, not only a hard cap.
3. Preserve cross-campaign fairness. No campaign can consume the full allocatable portion of a shared renewable resource unless it has an explicit priority override. Default scheduling should leave room for at least one urgent non-campaign operation and one other approved campaign.
4. Prefer cheap scouting before expensive implementation. Use free models for status summaries, eval log clustering, candidate critique, and task decomposition when quality is sufficient.
5. Use expensive coding agents for narrow implementation packets with clear verification commands.
6. Pause campaigns automatically when leases cannot be acquired without violating reserve floors.
7. Resume campaigns automatically after reset windows or idle windows open.
8. Apply failure penalties. If a task family repeatedly fails, reduce its budget priority until the campaign proposes instrumentation, smaller scope, or a prerequisite task.
9. Limit concurrent mutations. The first version should run one rollout-affecting iteration per campaign at a time.
10. Permit analysis-only or proposal-only iterations when implementation resources are unavailable.

The scheduler should produce a visible plan:

```text
next_24h:
  - 02:00 local: run free-model eval-log clustering
  - 03:00 local: run one coding-plan implementation lease, max 45 minutes
  - 03:50 local: run local tests and write scorecard
  - after reset: reserve 30%, allocate at most two near-term tasks
```

## Campaign Functions

The first mechanism can call existing or external agents, but these are functions, not required separate personas or services. The first implementation may combine several functions in one process, one model call, or one local command. Split them only when evidence shows that separation improves reliability, cost, safety, or observability.

| Function | Responsibility | Preferred resources |
|---|---|---|
| Campaign planning | Refine objective, maintain stages, choose next task, adapt backlog after evidence. | Strong reasoning model when budget allows; cheaper model for routine updates. |
| State scouting | Inspect repo/runtime/status/eval logs/artifacts and summarize current state. | Free models plus local tools. |
| Evaluation design | Convert stage goals into scorecards, fixtures, verification plans, and measurable checks. | Strong model for initial design; local compute for validation. |
| Implementation | Modify code/config/prompts/workflows, produce artifacts, or propose a better next step in a candidate workspace. | Prepaid coding plan or local coding model. |
| Review | Check spec compliance, risk, code quality, artifact quality, and rollout readiness. | Free/cheap model first; escalate on high risk. |
| Rollout supervision | Apply registry/rollout steps and watch metrics. | Local runtime services. |
| Budget management | Allocate leases and pause/resume work. | Deterministic local service, not LLM. |

Functions are workers, not authorities. Authority stays with:

```text
campaign state machine
policy engine
evaluation board
resource ledger
rollout gates
human approval policy
```

The implementation agent should have the broadest local discretion inside the approved task scope. The campaign manager chooses what outcome is worth pursuing. The implementation agent chooses how to pursue it.

## Evaluation And Evidence

Evaluation is the anchor that keeps the system flexible without becoming chaotic.

Because the implementation agent is free to choose the technical path, the campaign must be strict about evidence.

A candidate should include one or more of:

```text
verification command output
scorecard
artifact sample
before/after comparison
trace or log reference
failure reproduction
resource usage record
human-readable summary
rollback instructions
```

Evaluation should handle more than code tests. A long-horizon campaign may need to evaluate:

```text
source code behavior
scheduler behavior
distributed system behavior
UI interaction traces
generated images/videos/audio
documents and exports
API responses
workflow reliability
cost and latency
human review decisions
```

Evaluation should happen at three levels:

| Level | Question | Example evidence |
|---|---|---|
| Task evaluation | Did this candidate do what it claimed? | Tests, artifacts, command output, traces, review notes. |
| Stage evaluation | Did the candidate improve the current milestone? | Stage scorecard, baseline comparison, regression gates. |
| Campaign evaluation | Is the overall objective closer, unchanged, or drifting? | Progress summary, stage movement, resource burn, repeated blocker analysis. |

Default rule:

```text
If a candidate cannot be evaluated, the next task should usually add evaluation or inspection before trying to improve behavior.
```

A second default rule handles drift:

```text
If task scorecards pass but the campaign objective is not getting closer, the campaign should pause for objective/stage review instead of optimizing the local proxy further.
```

This rule is general. It applies equally to Minecraft screen interaction, generated video clips, code patches, and scheduler changes.

## Failure Adaptation

Failed rollout should not just retry the same task. The campaign manager should create an adaptation record.

Failure classes:

| Failure | Adaptation |
|---|---|
| Build/test failure | Split task into smaller code change, add focused repro, or ask implementation agent to fix only the failure. |
| Eval regression | Narrow objective to the regressed metric, add diagnostic logging, or keep candidate as a negative example. |
| Artifact quality failure | Add better inspection, labeled examples, clearer intent, or reduce artifact size before retrying. |
| Rollout failure | Try shadow-only rollout, canary smaller subset, lower blast radius, or add health checks before retry. |
| Resource exhaustion | Replan with cheaper agents, defer expensive work until reset, or switch to analysis-only tasks. |
| Repeated implementation failure | Add prerequisite task, improve context packet, reduce allowed file scope, or ask a stronger model only for design. |
| Foundation blocker | Create a foundation-repair task with a repro and post-fix verification. |
| Ambiguous metric | Pause stage and ask for human decision, or propose clearer proxy metrics. |
| Wrong task selection | Accept a task-replacement proposal and update the backlog. |

Adaptation state:

```text
failed_iteration_id
failure_class
root_cause_summary
what_was_learned
next_task_simplification
new_eval_or_diagnostic
foundation_issue_detected
replacement_task_candidate
budget_adjustment
human_attention_needed
```

A useful rule:

```text
After two failed attempts at the same near-term task family,
the next task must be easier, more diagnostic, or more foundational.
```

A task family is the same when it targets the same blocker with the same basic approach, even if the prompt wording, branch name, or implementation agent changes. This prevents the campaign from bypassing adaptation by renaming retries.

Examples of easier or better-adapted tasks:

- add instrumentation instead of changing behavior;
- write an eval fixture instead of implementing the feature;
- improve context collection instead of asking another coding agent to guess;
- reduce the target module set;
- convert an end-to-end target into one primitive task;
- run shadow-only instead of canary;
- ask for a design review instead of a patch;
- fix the scheduler/runtime/storage layer if it is the real blocker.

## Rollout Policy For The First Mechanism

The first human-guided mechanism should use conservative rollout checkpoints:

- no live rollout without passing required verification commands;
- no campaign stage promotion without a stage scorecard;
- no broad rollout after a single success sample unless the stage target explicitly allows it;
- every rollout writes a rollback plan;
- failed rollout emits an adaptation record;
- human approval is required for first rollout of each stage and for policy-sensitive expansions;
- routine same-scope patches can be auto-rolled out only if the campaign approval policy allows it.

Policy-sensitive expansions include:

```text
new paid resource class
new external provider with spending or privacy implications
new public posting/distribution path
new UI/control channel that can affect external systems
new autonomous loop that can spend quota repeatedly
changes to rollout safety policy
changes to resource reserve floors
```

For bootstrap, rollout may simply mean merging a candidate branch or switching a local config in a disposable VM. The same state machine should still be used so later registry/module rollout can reuse it.

## Data Model Additions

Initial contracts should live in `sinomo-core` when they are pure data. Orchestration behavior belongs in `sinomo-evolution`; evaluation execution belongs in `sinomo-eval`; model/provider selection and metering should integrate with `sinomo-lm`; rollout execution belongs in `sinomo-rollout`.

This version intentionally does not require a formal capability data model.

Sketch:

```rust
pub struct EvolutionCampaign {
    pub campaign_id: EvolutionCampaignId,
    pub objective: ObjectiveCard,
    pub status: EvolutionCampaignStatus,
    pub stages: Vec<EvolutionStage>,
    pub current_stage_id: Option<EvolutionStageId>,
    pub budget_policy: ResourceBudgetPolicy,
    pub approval_policy: CampaignApprovalPolicy,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

pub struct ObjectiveCard {
    pub objective_id: ObjectiveId,
    pub human_text: String,
    pub interpreted_goal: String,
    pub non_goals: Vec<String>,
    pub assumptions: Vec<String>,
    pub constraints: Vec<String>,
    pub success_metrics: Vec<String>,
    pub failure_conditions: Vec<String>,
    pub safety_notes: Vec<String>,
    pub initial_horizon: Option<Duration>,
}

pub struct EvolutionStage {
    pub stage_id: EvolutionStageId,
    pub name: String,
    pub purpose: String,
    pub baseline: Option<EvaluationScorecardRef>,
    pub acceptance_metrics: Vec<EvaluationMetricTarget>,
    pub regression_gates: Vec<EvaluationGate>,
    pub required_artifacts: Vec<RequiredArtifactSpec>,
    pub eval_suite_id: Option<String>,
    pub estimated_budget: ResourceEstimate,
    pub status: EvolutionStageStatus,
}

pub struct NearTermEvolutionTask {
    pub task_id: EvolutionTaskId,
    pub campaign_id: EvolutionCampaignId,
    pub stage_id: EvolutionStageId,
    pub objective: String,
    pub rationale: String,
    pub expected_metric_impact: Vec<EvaluationMetricTarget>,
    pub allowed_paths: Vec<String>,
    pub allowed_artifact_scopes: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub required_verification: VerificationPlan,
    pub estimated_budget: ResourceEstimate,
    pub adaptation_parent: Option<EvolutionTaskId>,
}

pub enum CandidateResultKind {
    PatchCandidate,
    ArtifactCandidate,
    WorkflowCandidate,
    ConfigCandidate,
    EvalCandidate,
    InstrumentationCandidate,
    FoundationRepairCandidate,
    TaskReplacementProposal,
    DesignReviewOnly,
    NoOpWithReason,
    Other(String),
}

pub struct EvolutionIterationResult {
    pub iteration_id: EvolutionIterationId,
    pub task_id: EvolutionTaskId,
    pub result_kind: CandidateResultKind,
    pub candidate_ref: Option<ArtifactRef>,
    pub replacement_task: Option<NearTermEvolutionTask>,
    pub scorecard: Option<EvaluationScorecardRef>,
    pub resource_usage: ResourceUsageSummary,
    pub rollout_recommendation: RolloutRecommendation,
    pub summary: String,
}

pub struct CampaignDecision {
    pub decision_id: CampaignDecisionId,
    pub campaign_id: EvolutionCampaignId,
    pub stage_id: Option<EvolutionStageId>,
    pub selected_task_id: Option<EvolutionTaskId>,
    pub decision_kind: CampaignDecisionKind,
    pub reason: String,
    pub alternatives_considered: Vec<String>,
    pub evidence_refs: Vec<ArtifactRef>,
    pub resource_context: ResourceUsageSummary,
}

pub struct ResourceBudgetPolicy {
    pub allowed_classes: Vec<ResourceClass>,
    pub forbidden_classes: Vec<ResourceClass>,
    pub reserve_floors: Vec<ResourceReserveFloor>,
    pub per_window_limits: Vec<ResourceWindowLimit>,
    pub burst_limits: Vec<ResourceBurstLimit>,
}

pub struct ResourceLease {
    pub lease_id: ResourceLeaseId,
    pub campaign_id: EvolutionCampaignId,
    pub iteration_id: Option<EvolutionIterationId>,
    pub resource_class: ResourceClass,
    pub provider: Option<String>,
    pub hard_limit: ResourceAmount,
    pub estimated_cost: ResourceAmount,
    pub budget_debt_limit: Option<ResourceAmount>,
    pub expires_at: OffsetDateTime,
}

pub enum UsageConfidence {
    Exact,
    ProviderReported,
    Estimated,
    Unknown,
}

pub struct ResourceUsageSummary {
    pub estimated: ResourceAmount,
    pub actual: Option<ResourceAmount>,
    pub confidence: UsageConfidence,
    pub budget_debt: Option<ResourceAmount>,
}
```

`CandidateResultKind` is a coarse routing hint, not a closed ontology. Unknown or new result kinds should be preserved through `Other(String)` and reviewed rather than rejected merely because the campaign manager does not yet know the label.

Optional ability notes can be a JSONL sidecar, not a core model:

```text
ability-notes.jsonl
```

Example entry:

```json
{
  "observed_at": "...",
  "campaign_id": "campaign_x",
  "iteration_id": "iter_012",
  "note": "Generated clips can now be sampled into frame strips for visual review.",
  "evidence_ref": "iterations/iter_012/scorecard.json"
}
```

Nothing should break if this file is absent.

## Event Families

Add campaign-level events above existing `evolution.job.*`, `evolution.eval.*`, and `evolution.rollout.*` events:

```text
evolution.campaign.proposed
evolution.campaign.approved
evolution.campaign.scheduled
evolution.campaign.paused
evolution.campaign.resumed
evolution.campaign.completed
evolution.stage.proposed
evolution.stage.started
evolution.stage.scorecard.updated
evolution.stage.completed
evolution.task.proposed
evolution.task.selected
evolution.task.replaced
evolution.resource.lease.requested
evolution.resource.lease.granted
evolution.resource.lease.denied
evolution.resource.usage.recorded
evolution.campaign.decision.recorded
evolution.human.checkpoint.requested
evolution.human.checkpoint.resolved
evolution.iteration.started
evolution.iteration.completed
evolution.iteration.failed
evolution.adaptation.recorded
evolution.foundation.blocker.detected
evolution.rollout.blocked
evolution.rollout.rollback.requested
evolution.rollout.rollback.completed
evolution.ability.note.recorded
```

Every event should include campaign ID, stage ID when available, resource usage estimate when relevant, and enough provenance to reconstruct why the campaign chose the next task.

## Persistent Bundle Layout

Campaigns live outside the repo under `SINOMO_DATA_DIR`.

```text
$SINOMO_DATA_DIR/evolution/campaigns/campaign_x/
  campaign.json
  objective-card.json
  budget-policy.json
  resource-ledger.jsonl
  schedule.json
  decisions.jsonl
  ability-notes.jsonl                 # optional
  stages/
    stage_001.json
    stage_001-scorecards.jsonl
  tasks/
    task_001.json
    task_002.json
  iterations/
    iter_001/
      job.json
      mutation-packet.json
      resource-leases.json
      agent-transcripts/
      candidate-bundle-ref.json
      scorecard.json
      rollout-attempt.json
      adaptation.json
      replacement-task.json           # only if result is TaskReplacementProposal
  events.jsonl
```

Candidate bundles can continue using the existing job-level layout. The campaign bundle references them instead of duplicating all artifacts.

## Example Campaign: Speedrun Minecraft

The Minecraft example is still useful because it illustrates a task where Sinomo may need to grow from a coding/conversation agent into an interactive system. The design should not hardcode Minecraft-specific capability classes, but the campaign still needs staged evals that expose the real blockers.

Objective:

```text
sinomo evolve propose --objective "speedrun minecraft"
```

Possible objective interpretation:

```text
Build and improve the Sinomo system until it can autonomously make progress
on Minecraft speedrunning-style objectives in a controlled local environment.
```

Important assumptions:

```text
controlled local environment
pinned game version if needed
fixed seeds for early benchmarks
no public server automation
no anti-cheat bypass
no account-risk behavior
human approval for new external tools or risky control channels
```

Example ruleset card:

```text
ruleset:
  game_version: pinned by proposal or recorded as undecided
  category: controlled local benchmark, not official speedrun submission
  allowed_interfaces:
    - screen/input automation
    - local server/mod API if approved
    - logs/traces
  forbidden:
    - public server automation
    - anti-cheat bypass
    - account-risk behavior
  seed_policy:
    - fixed seeds for early evals
    - held-out seeds for generalization
```

### Why this is more than route planning

A naive plan might focus on questions like:

```text
How do we get wood?
How do we craft tools?
How do we build a portal?
```

Those are necessary later, but they are not the first blockers.

At the start, Sinomo may not be able to:

```text
observe the game continuously
interact with the game reliably
record what happened after an action
recover from bad states
react quickly to real-time events
measure success or failure of a primitive action
run repeated attempts under comparable conditions
```

The campaign should let the implementation agent decide the best way to address those blockers. It might use screen capture and keyboard/mouse input. It might use a controlled local mod or API if that is safer and more reliable. It might first improve Sinomo’s runtime event loop, artifact tracing, or scheduler. The stage target defines the outcome; it does not require a specific capability abstraction.

### Possible staged targets

| Stage | Purpose | Example acceptance metrics |
|---|---|---|
| Controlled setup | Establish a reproducible local environment for attempts. | Launch/reset works; environment metadata recorded; no public-server dependency. |
| Baseline observation/action | Demonstrate that Sinomo can observe the game and perform one bounded action or equivalent controlled interaction. | Before/after evidence saved; action effect can be inspected; emergency stop or safe abort exists. |
| Interaction trace | Record enough evidence to diagnose why attempts succeed or fail. | Observation/action/event logs are stored; failed attempts include inspectable traces. |
| Primitive tasks | Complete small tasks in controlled fixtures. | Movement, camera/view adjustment, block breaking, item pickup, crafting/inventory interaction, or equivalent primitives pass thresholds. |
| Survival progression | Chain primitive tasks into early-game progress. | Collect wood, craft tools, mine basic resources, manage food/health, survive first night across fixed seeds. |
| Intermediate route execution | Execute a route toward an intermediate speedrun objective. | Reach a defined milestone such as Nether preparation or Nether entry with bounded retries. |
| Speed/reliability optimization | Improve over repeated attempts. | Median time improves without reducing success rate; regressions on earlier stages remain below threshold. |
| Generalization | Avoid overfitting to one fixture. | Performance remains acceptable across held-out seeds or small environment variations. |

### Example near-term backlog

```text
task_001:
  objective: Create a controlled Minecraft benchmark fixture or explain why a different local interface is safer.
  expected evidence: launch/reset record, environment metadata, first baseline scorecard.

 task_002:
  objective: Produce before/after evidence for one bounded in-game action.
  expected evidence: screenshot/log/trace before action, action record, screenshot/log/trace after action.

 task_003:
  objective: Add diagnostic recording for failed interaction attempts.
  expected evidence: failed attempt can be replayed or summarized well enough to identify the first bad step.

 task_004:
  objective: Complete one primitive block-interaction task in a controlled world or propose a better primitive benchmark.
  expected evidence: success rate over repeated runs, failures labeled.

 task_005:
  objective: Complete a collect-wood microbenchmark.
  expected evidence: logs/traces, inventory or state proof, success rate, median time.

 task_006:
  objective: Compose early survival workflow from passing primitives.
  expected evidence: wood -> planks -> crafting table -> tool or equivalent stage scorecard.

 task_007:
  objective: Add fast-reaction or recovery behavior for a real-time failure class if traces show it is blocking progress.
  expected evidence: event detection/recovery benchmark or a cheaper diagnostic if not ready.
```

The exact implementation path is intentionally open. The implementation agent may decide that the next useful work is not “play better,” but “make traces inspectable,” “repair the scheduler so repeated attempts can run,” or “replace the proposed input method.”

### Example implementation-agent discretion

Selected task:

```text
Complete one primitive block-interaction task in a controlled world.
```

Acceptable implementation-agent responses:

```text
- Patch: implement a screen/input based primitive and eval.
- EvalCandidate: add the fixture/eval first because no reliable benchmark exists.
- InstrumentationCandidate: record before/after traces because failures are opaque.
- FoundationRepairCandidate: fix runtime cancellation because stuck input cannot be safely tested.
- TaskReplacementProposal: use a local mod/API for early benchmarks instead of brittle desktop automation.
```

The campaign should judge these by evidence and policy, not by whether they match a predeclared capability card.

## Example Campaign: 15-Minute Video Generation

The video example shows a different kind of long-horizon evolution. The issue is not real-time control. The issue is that current video generation APIs may generate short clips, while the objective asks for a coherent 15-minute artifact.

Objective:

```text
sinomo evolve propose \
  --objective "make a 15 minute video explaining topic X using available video generation model APIs"
```

Possible objective interpretation:

```text
Build and improve a repeatable workflow that plans a long video as scenes/cuts,
generates shorter clips, inspects the outputs, retries weak segments, stitches
clips together, and verifies the final artifact.
```

### Why this is more than prompting a video model

A naive plan might ask one model for a 15-minute video. If providers cannot generate that directly, Sinomo needs a workflow.

Likely blockers:

```text
choosing or connecting to a provider
planning a long video as scenes/cuts
tracking many generated clips and prompts
inspecting video outputs
sampling frames or creating contact sheets/frame strips
using vision models to check multiple moments at once
retrying weak clips
combining clips consistently
checking duration, format, continuity, captions/audio, and final export
controlling generation cost
```

Again, the framework should not require these to be formal capabilities. They are simply blockers and possible next tasks.

### Possible staged targets

| Stage | Purpose | Example acceptance metrics |
|---|---|---|
| Minimal generation | Generate and save one short clip. | Clip exists, metadata recorded, cost tracked, prompt saved. |
| Artifact inspection | Inspect generated clips well enough to catch obvious mismatch or artifacts. | Sampled frames/contact sheet/QA report identifies known bad examples. |
| Scene planning | Break the 15-minute intent into manageable scenes/cuts. | Plan covers requested topic, target duration, narrative order, and generation prompts. |
| Clip retry loop | Improve weak clips using inspection feedback. | At least one failed clip is detected, revised, regenerated, and re-evaluated. |
| Short assembly | Stitch several clips into a 1-2 minute draft. | Export succeeds; duration/format checks pass; scene order matches plan. |
| Full draft assembly | Generate, inspect, retry, and stitch all clips into a 15-minute draft. | Final artifact duration near target; required scene coverage present; metadata complete. |
| Final QA | Verify the finished artifact. | Duration, format, scene coverage, visible defects, audio/captions if required, and export integrity checked. |
| Optimization | Reduce cost/time or improve quality. | Better QA score, lower cost, or faster runtime without stage regressions. |

### Example near-term backlog

```text
task_001:
  objective: Generate and save one short clip using an approved provider or propose a provider plan.
  expected evidence: clip file, prompt, provider metadata, cost/usage record.

 task_002:
  objective: Produce a QA summary for one generated clip.
  expected evidence: sampled frames or contact sheet, model/human-readable inspection report.

 task_003:
  objective: Create a scene/cut plan for a 2-minute pilot version.
  expected evidence: scene list with target durations, prompts, dependencies, and success checks.

 task_004:
  objective: Stitch 3-5 clips into a short draft.
  expected evidence: output video, duration check, scene order check, export metadata.

 task_005:
  objective: Add retry behavior for clips that fail inspection.
  expected evidence: failed clip label, revised prompt, regenerated clip, comparison report.

 task_006:
  objective: Scale from pilot plan to 15-minute plan.
  expected evidence: full scene/cut plan with budget estimate and risk notes.
```

The implementation agent might choose to implement frame sampling, contact sheets, FFmpeg integration, artifact indexing, a provider adapter, or a cheaper manual review step first. The campaign should accept whichever path best improves the stage scorecard under policy and budget.

## First Demo Recommendation

Use a small Sinomo-internal objective before an ambitious external game or media objective.

Recommended first objective:

```text
sinomo evolve propose --objective "improve the self-evolution scheduler so it can pause and resume around resource budgets"
```

Concrete first-demo success shape:

```text
sinomo evolve propose --dry-run ...
  -> writes a campaign bundle
  -> writes a resource schedule preview
  -> writes staged evaluation targets
  -> writes a near-term task list
  -> writes an approval checkpoint

sinomo evolve show <campaign>
  -> renders the proposal, current state, budget preview, stages, tasks, decisions, and approval status
```

Why this is a good first campaign:

- It improves the mechanism itself.
- Metrics are easy to define: proposal completeness, lease correctness, pause/resume behavior, persisted bundle shape, and tests.
- Rollout can be limited to the disposable development VM.
- Failure adaptation is easy to observe.
- It exercises the principle that self-evolution can update the foundation when the foundation is the blocker.

After the campaign manager can handle its own scheduling improvements, use `speedrun minecraft` as a richer long-horizon interactive campaign and the 15-minute video objective as a richer artifact-generation campaign.

## Initial Implementation Phases

### Phase 0: Campaign Proposal Only

Goal: make `sinomo evolve propose --dry-run` produce an inspectable long-horizon campaign proposal.

Deliverables:

- `EvolutionCampaign`, `ObjectiveCard`, `EvolutionStage`, `NearTermEvolutionTask`, `EvolutionIterationResult`, `CampaignDecision`, and budget policy data contracts.
- CLI args for objective, horizon, budget policy, reserve floors, approval policy, and dry-run.
- Proposal writer under `SINOMO_DATA_DIR/evolution/campaigns`.
- No agent execution yet.

Acceptance criteria:

- A human can inspect objective assumptions, staged eval targets, near-term tasks, and budget schedule.
- The proposal includes explicit metrics and failure conditions.
- The proposal can be approved, rejected, or edited later.
- No formal capability system is required.

### Phase 1: Resource Ledger And Scheduler Dry Run

Goal: make resource availability a first-class gate before dispatching agents.

Deliverables:

- Resource class definitions.
- Reset-window and reserve-floor config.
- Lease request/grant/deny records.
- Scheduler preview for the next 24 hours and current reset window.
- Pause/resume decisions based on budget availability.

Acceptance criteria:

- The campaign does not dispatch work when reserve floors would be violated.
- The scheduler explains why a task is running now, delayed, or paused.
- Actual and estimated usage are recorded.

### Phase 2: One Scheduled Iteration

Goal: let an approved campaign run one near-term implementation iteration through the existing candidate job machinery.

Deliverables:

- Convert selected `NearTermEvolutionTask` into `EvolutionJob` and `MutationPacket`.
- Acquire resource lease before model/agent use.
- Dispatch one implementation agent in a candidate worktree.
- Allow the implementation agent to produce a patch, artifact, evaluator, instrumentation, foundation repair, or task-replacement proposal.
- Run required verification commands.
- Attach resulting candidate bundle to the campaign.

Acceptance criteria:

- One campaign can produce one candidate without mutating the active checkout.
- The scorecard and resource usage are visible in the campaign bundle.
- Failed implementation becomes an adaptation record instead of a silent retry.
- A task-replacement proposal can update the backlog when accepted.

### Phase 3: Staged Evaluation And Adaptation

Goal: make campaign progress depend on stage scorecards, not just successful patches.

Deliverables:

- Stage scorecard update after each iteration.
- Stage promotion checks.
- Failure classification.
- Automatic easier, more diagnostic, or more foundational task proposal after repeated failure.
- Human checkpoint for ambiguous metric or first rollout of a stage.

Acceptance criteria:

- A stage cannot complete without meeting its declared metrics.
- Failed rollout causes narrower, easier, or better-instrumented next-task proposals.
- Foundation blockers can become explicit near-term tasks.
- The campaign can explain what changed in the backlog after failure.

### Phase 4: Background Scheduling

Goal: make campaigns durable scheduled background jobs.

Deliverables:

- Campaign runner service or cron-compatible command.
- Resume from persisted campaign bundle.
- Timer events for reset windows and idle windows.
- Concurrency cap across campaigns.
- Notification or status summary for human checkpoints.

Acceptance criteria:

- A campaign can pause for budget, resume after reset, and continue from disk.
- Multiple campaigns cannot exhaust shared resources accidentally.
- Human attention is requested only at configured checkpoints or policy incidents.

### Phase 5: Optional Ability Notes

Goal: allow campaigns to record learned abilities without making them central.

Deliverables:

- Optional `ability-notes.jsonl` sidecar.
- Event `evolution.ability.note.recorded`.
- UI/status display for informal learned abilities.

Acceptance criteria:

- Campaigns run correctly if the file is absent.
- No rollout or stage promotion depends on an ability note.
- Notes link to evidence when available.

## Non-Goals For The First Version

- No open-ended autonomous spending.
- No paid on-demand API usage unless explicitly enabled.
- No bypassing policy/evaluation gates because a campaign is long-running.
- No direct mutation of the active Sinomo checkout.
- No broad live rollout without stage scorecards.
- No hard dependency on a formal capability registry.
- No requirement that implementation agents use a predefined capability taxonomy.
- No attempt to solve model training or reinforcement learning infrastructure yet.
- No public-server game automation or anti-cheat bypass in the Minecraft example.
- No unapproved public posting or paid overage in the video example.

## Core Invariants

1. A campaign is durable, inspectable, and resumable.
2. Every stage has measurable evaluation targets.
3. Every iteration has a resource lease before using scarce resources.
4. Every candidate has evidence before rollout.
5. Every rollout has rollback information.
6. Every failed rollout produces an adaptation record.
7. Repeated failure makes the next task easier, more diagnostic, or more foundational, not merely retried.
8. Resource reserves protect other Sinomo operations.
9. Free and prepaid resources are scheduled gradually over their reset windows.
10. Human guidance defines goals and approval policy; Sinomo drives routine planning, execution, evidence collection, and adaptation inside that envelope.
11. The implementation agent may challenge the selected task and propose a better next step.
12. The objective is a compass, not a cage: foundation repairs and broader system improvements are allowed when they remove campaign blockers.
13. Capability tracking is optional. Evidence is mandatory.
