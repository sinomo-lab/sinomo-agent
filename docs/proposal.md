# Sinomo System Architecture Proposal

This proposal captures the high-level design intent for Sinomo. The detailed, current architecture lives in [architecture.md](architecture.md). When this proposal and the architecture differ, the architecture is the source of truth.

## Vision

Sinomo should be built as an individual AI entity runtime, not as a single monolithic model loop or a thin chat surface. The runtime should support continuous perception, durable memory, situated action, slower reflection, and controlled self-evolution without letting any one path block the others.

The core design idea is a three-speed, event-centric multimodal system:

- keep live ingestion and perception always on,
- convert raw signals into timestamped structured events,
- let a fast reactive loop handle timing-sensitive decisions,
- let an interactive loop handle ordinary conversation and small actions,
- escalate only selected work into a slower deliberative loop,
- improve running behavior through scoped mutation jobs, evaluation gates, policy decisions, artifact registration, rollout, and rollback,
- store memory through explicit promotion policies rather than dumping whole streams into context.

## Runtime Shape

Sinomo should run first on a serious local workstation or small server, with the same architecture able to expand across nearby trusted nodes. A single-machine deployment should be a special case of the distributed node model, not a separate design.

The core runtime should be realistic for self-hosted hardware:

- continuous audio, video, screen, text, telemetry, and event ingestion,
- local hot state and low-latency policy decisions,
- durable event and memory storage,
- node-local sandbox and action enforcement,
- optional use of local or remote model backends behind stable interfaces.

Model providers and module implementations can change over time. The event contract, memory policy, node/device provenance, artifact provenance, rollout state, and runtime supervision model should remain stable.

## Design Principles

1. **Perception is always on.** Ingestion and perception continue while Sinomo thinks, speaks, runs tools, or executes long jobs.
2. **Events are the canonical substrate.** Services communicate through structured events with time, provenance, confidence, salience, evidence, and routing metadata.
3. **Timing and content are separate decisions.** The system decides when to speak, interrupt, alert, wait, or escalate separately from what to say or do.
4. **Reasoning runs at multiple speeds.** Fast reactive behavior, ordinary interaction, and slow deliberation should be separate cooperating loops.
5. **Memory is hierarchical.** Hot, warm, cold, and procedural memory are managed explicitly, with selective promotion from events.
6. **Distribution is native.** Nodes, devices, sessions, workspaces, events, and memory should carry identity and locality from the beginning.
7. **Safety is enforced by the runtime.** Sandboxes, policy checks, resource limits, and incident events protect action paths without putting human approval on the hot path.
8. **Evolution is evidence-gated.** Self-evolution moves through scoped workspaces, manifests, scorecards, policy decisions, signed artifacts, shadow/canary rollout, and rollback.
9. **Implementation stays local-first.** Early versions should avoid unnecessary infrastructure and prove the contracts before adding heavier distributed services.

## Conceptual Planes

```text
Inputs
  -> Distributed Foundation Plane
  -> Ingestion Plane
  -> Perception Plane
  -> Event / World-State Plane
  -> Agent Control Plane
  -> Evolution / Delivery Plane
  -> Action / Generation Plane
  -> Outputs
```

The event/world-state plane is the architectural center. Perception produces events; memory, triggers, agents, deliberation, actions, supervision, and self-evolution consume and emit events. This keeps live behavior, replay, debugging, long-horizon memory, and autonomous improvement grounded in the same substrate.

## Self-Evolution Direction

Sinomo should improve herself through controlled live mutation rather than direct rewriting of trusted runtime code. The first auto-evolution target should be low-risk R3 modules, while R0/R1 contracts are designed from the beginning for continuity checks, standby handoff, and rollback. Human-guided long-horizon evolution should be framed as flexible campaign execution: starting from an objective, Sinomo proposes staged evaluations, delegates open-ended implementation agents, evaluates candidate bundles, rolls out evidence-backed changes, adapts failed work into easier or diagnostic tasks, and may improve its own foundation when the objective exposes deeper blockers.

The evolution loop should be explicit:

- detect an opportunity from traces, incidents, metrics, replay failures, or user corrections,
- create a scoped evolution job with an allowed write set and rollback policy,
- produce a candidate manifest and artifact,
- run evaluation scorecards and replay gates,
- pass through a trusted policy decision,
- register immutable artifact metadata,
- shadow, canary, promote, pause, or roll back through events.

## Control Loops

| Loop | Target | Responsibility |
|---|---:|---|
| Fast reactive | 50-150 ms | interruption, wake/no-wake, safety triggers, urgent acknowledgement, alert suppression |
| Interactive | 200 ms-2 s | dialogue, bounded retrieval, short tool calls, compact responses, task state |
| Deliberative | 2 s+ | planning, reflection, branch comparison, memory consolidation, long-running generation |

The fast path should stay small and deterministic. The interactive path should be bounded and user-facing. The deliberative path should run as asynchronous jobs that can be cancelled or superseded when new events change the situation.

## Memory Direction

Sinomo should not rely on one giant context window. The event log provides the replayable timeline, while memory tiers provide different working sets:

- **hot memory** for immediate live context,
- **warm episodic memory** for session continuity,
- **cold semantic memory** for long-term knowledge and summaries,
- **procedural memory** for reusable skills, workflows, policies, and recovery patterns.

Memory writes should be selective. Promote events when they are salient, repeated, unresolved, user-important, linked to task outcomes, or needed for future planning.

## Safety Direction

Sinomo should act freely inside explicit runtime envelopes. The node or device affected by an action is responsible for final local enforcement.

The runtime should record both successful and blocked actions as structured events. Supervisors can review incidents after the fact and adjust policy, but ordinary low-latency behavior should not depend on inline approval prompts.

## MVP Direction

The first useful MVP should prove the event-centric, three-speed runtime on local-first infrastructure:

- shared contracts for IDs, events, routing, node/device provenance, and capabilities,
- trust-ring, module manifest, artifact, policy decision, scorecard, rollout, and rollback contracts,
- event validation, persistence interfaces, and replay,
- basic local node/device metadata,
- simulated perception events,
- a simple trigger path,
- a bounded interactive loop,
- one asynchronous deliberation worker,
- enough observability to replay decisions and inspect traces.

Detailed crate responsibilities, storage choices, distributed synchronization, DFS policy, runtime flows, and implementation phases are maintained in [architecture.md](architecture.md).
