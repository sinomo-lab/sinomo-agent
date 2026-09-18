# Evolution Goals

Sinomo Evolution manages iterative improvement as a goal with explicit metrics
and an immutable revision graph. It can evolve source repositories, but the
framework does not require git: a revision may also be a frozen directory
snapshot, inline contents, or an immutable base plus patch artifact.

The evolution module must be active. The CLI and app call it through the local
daemon; they do not contain a second evolution implementation.
The active module publishes `evolve`, including its help and completion data.
It appears in `sinomo help` while available and is removed from discovery after
unload. If the catalog changes between discovery and invocation, the command
fails before execution; run it again to discover the current owner. Bootstrap
commands such as `sinomo module` remain available without the Evolution module.
When upgrading from a build that used the CLI's dedicated Evolution proxy,
rebuild and roll out the Evolution native module too: older artifacts do not
publish the new command contribution.

## Create And Inspect A Generic Goal

```sh
sinomo evolve goal create "Improve answer quality without increasing latency" \
  --metric quality:maximize:points \
  --metric latency:minimize:milliseconds

sinomo evolve graph
sinomo evolve graph --goal GOAL_ID --json
```

The graphical app's **Evolution** tab refreshes while visible. It shows goal
status, the revision DAG and heads, content identity kinds, artifact and metric
counts, task and agent activity, resources, and the latest attributed event.

## Record A Revision

`sinomo evolve revision record` accepts a JSON request. This keeps arbitrary
artifact schemas and content identities out of shell quoting. For example:

```json
{
  "goal_id": "GOAL_ID",
  "revision_id": "answer-v1",
  "parent_revision_ids": [],
  "content": {
    "kind": "inline-content",
    "media_type": "text/plain",
    "content": "The exact frozen answer"
  },
  "artifacts": [
    {
      "id": "answer-v1-eval",
      "kind": "evaluation-results",
      "uri": "sifs:/experiments/answer-v1/results.json",
      "content_hash": "blake3:REPLACE_WITH_64_LOWERCASE_HEX_DIGITS",
      "media_type": "application/json",
      "schema": "example.answer-eval/v1",
      "role": "evaluation-output"
    }
  ],
  "metrics": [
    {
      "metric_id": "quality",
      "value": { "type": "integer", "value": 82 },
      "sample_count": 100,
      "outcome": "passed",
      "evaluator": "answer-eval-v1",
      "artifact_id": "answer-v1-eval"
    }
  ],
  "status": "baseline",
  "summary": "Initial measured answer"
}
```

```sh
sinomo evolve revision record revision.json
```

Metric observations must name a metric declared by the goal and an artifact
attached to the same revision. Inline-content digests are computed and checked
by the module. Directory, artifact, and composite identities require an
explicit BLAKE3 digest.

## Repository Evolution

The existing repository workflow now also projects its candidates into goal
revision graphs:

```sh
sinomo evolve propose "Reduce parser latency" --repo PATH \
  --verify "cargo test -p parser"
sinomo evolve status
sinomo evolve graph
```

Candidate work remains isolated. A candidate enters the DAG only after its
source commit and recorded patch establish frozen content identity. Approval,
policy, artifact registration, and rollout remain separate authority gates.

## Control A Goal

```sh
sinomo evolve goal pause GOAL_ID --note "reviewing evidence"
sinomo evolve goal resume GOAL_ID
sinomo evolve goal complete GOAL_ID --note "targets met"
sinomo evolve goal cancel GOAL_ID --note "objective superseded"
```

The Evolution tab provides the same pause/resume and cancel controls. For a
campaign-backed goal, pause, resume, and cancel also update campaign scheduling
state. These controls do not bypass a running operation's cancellation or
rollout authority.

The `evolution-agent` is the governor. It owns planning, task assignment,
evidence review, and next-step recommendations. The separately attributable
`evolution-implementer` and `evolution-evaluator` packages do bounded work and
cannot approve or promote their own revisions.

Governors and integrations can persist orchestration state in one typed update:

```sh
sinomo evolve workflow update workflow-update.json
```

The request contains `goal_id` plus `tasks`, `agent_runs`, and `resources`
arrays. Tasks may name parent and dependency task IDs, agent runs may name their
parent governor run and assigned task, and resource records may bind tokens,
compute, wall time, or monetary microunits to a revision/task/agent run. Every
reference is validated before the graph is written, and every accepted update
adds an attributed `evolution.task.updated`, `evolution.agent.updated`, or
`evolution.resource.recorded` event.
