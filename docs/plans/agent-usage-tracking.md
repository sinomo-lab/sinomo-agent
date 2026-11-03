# Agent Usage Tracking

Status: proposed.

This plan adds durable, attributable accounting of agent work and a Usage view
in the app's Cluster surface. It covers model provider requests, token and other
billable units, tool calls, skill activations, skill version changes, provider
quotas, cost estimates, and display-only budgets. It extends the
[observability architecture](../observability-architecture.md) and the
[agent system architecture](../agent-system-architecture.md); it does not
replace their contracts.

## Goal and Completion Criteria

The work is complete when:

- every model provider request made on behalf of an agent, a background
  service, or a hosted tool produces one attributable usage record, including
  requests that are not text completions, such as image generation and hosted
  web search;
- token counts distinguish input, output, cache-read, cache-write, and reasoning
  tokens wherever the provider reports them, and records state when the
  provider reported nothing;
- tool calls, skill activations, and skill version publications produce usage
  records linked to the session, turn, and tool call that caused them;
- provider quota and credit snapshots are retained as history rather than only
  as the latest value;
- usage records survive restarts and are retained according to a cluster-wide
  setting that defaults to keeping everything;
- the Cluster surface has a sixth view, **Usage**, that answers cluster-wide and
  per-node questions over an arbitrary time range and drills down to individual
  records and their chat or session evidence;
- estimated cost is derived from an editable price catalog, re-prices existing
  history when prices change, and reports how much of the usage it could price;
  and
- budgets are visible against actual spend and never block, delay, or alter a
  request.

## Scope

In scope:

- requests to model providers, regardless of modality: text and structured
  completions, streaming completions, image generation and editing, hosted web
  search, and any later provider operation such as embeddings or speech;
- tools invoked through the agent tool orchestrator, including MCP tools and
  detached calls;
- skill activation through `/skill`, `skill_activate`, and resource reads, and
  skill version publication through `skill_update`, catalog import or
  activation, and provider reconciliation;
- provider quota and credit snapshots already returned as
  `LanguageModelProviderUsage`.

Out of scope:

- outbound HTTP made by tools or sandboxed programs; it is not a model provider
  request;
- enforcing budgets or quotas; budgets are display-only;
- billing reconciliation against provider invoices; costs are estimates; and
- storing prompt text, model output, tool arguments, or tool results in usage
  records. Records refer to the existing session evidence instead.

## Event Contract

Usage is recorded from typed events, following the
[event contract](../architecture.md#event-contract). Every usage event carries
the standard provenance plus a usage attribution:

| Attribution field | Meaning |
|---|---|
| Node | The node that issued the request or executed the tool. |
| Agent and agent session | The agent package key and model session, if any. |
| Parent session | The delegating session for subagent work. |
| Chat or channel | The user-visible conversation, if any. |
| Task and operation | The task and operation identity that links to activity and cancellation. |
| Origin | Interactive chat, background agent, subagent, evolution, evaluation, or a named runtime service. |
| Purpose | The reason for a model request: agent turn, decision repair, compaction, memory maintenance, hosted tool, or another named purpose. |

Event families:

| Event | Records |
|---|---|
| `lm.request.completed` | Provider, provider account, model, operation (completion, image generation, web search, ...), status and error class, retry count, latency, time to first output for streaming requests, and the billable units described below. Failed and cancelled requests are recorded too, because providers may bill them. |
| `agent.tool.call.completed` | Tool name, tool call id, status (succeeded, failed, cancelled, denied), duration, whether it was detached, the executing runtime and node, and the MCP server for MCP tools. This extends the existing `agent.loop.tool.completed` payload instead of adding a parallel event. |
| `skill.activated` | Skill name, version, content hash, trigger (explicit `/skill`, model `skill_activate`, or resource read), and the resource path for resource reads. It subsumes `interaction.skill.activated`. |
| `skill.version.published` | Skill name, previous and new version, previous and new content hash, source (provider, user, agent, evolution), and the authoring session when an agent wrote it. |
| `lm.provider.quota.sampled` | The provider account and the `LanguageModelProviderUsage` snapshot. |

### Billable units

`LanguageModelUsage` grows from input and output tokens to an explicit unit
breakdown. Each unit is optional so that "not reported" and "zero" stay
distinct:

- input tokens, cache-read input tokens, cache-write input tokens;
- output tokens and the reasoning subset of output tokens;
- generated images with their size and quality tier;
- hosted searches; and
- a generic named-unit list for provider operations without a dedicated field.

Each provider adapter maps its native usage report into this breakdown. A
request whose provider reports no usage is recorded with `reported = false`;
the UI never invents token counts for it.

### Recording boundary

Model requests are recorded where host runtimes invoke a provider through the
configured request path, not inside the agent turn loop. That captures agent
turns, decision repair, compaction, memory maintenance, hosted tools, and
non-agent services through one mechanism. Callers supply attribution and
purpose; the recording path supplies timing, status, and units. Phase 1
inventories every provider invocation site and routes any direct
`LanguageModelProvider` call through the recording path.

Recording sits outside the correctness path, as the
[observability boundary](../architecture.md#observability) requires. A failure
to append a usage record is logged and counted; it never fails or delays the
model request or tool call.

## Usage Ledger

The usage ledger belongs to the agent runtime and is registered through the
shared storage registry like other agent state. It follows the "events and
traces" state class: append-oriented, durable, and node-local.

- Each node appends the records for work it performed. Records are never written
  to cluster KV, which holds only compact configuration.
- The ledger keeps raw records and time-bucketed rollups keyed by the dimensions
  the UI filters on (node, provider, account, model, operation, purpose, agent,
  chat, tool, skill). Range queries read rollups and only scan raw records for
  drill-down.
- Rollups are rebuildable from raw records. Raw records are the source of truth
  until retention removes them; after that, rollups remain the record.
- A torn final record after a crash is ignored, matching the session log rule
  in the [agent storage contract](../agent-system-architecture.md#storage-and-evidence).

### Retention

Retention is a distributed setting stored in cluster KV and edited in Settings.
It has two independent values:

| Setting | Default | Effect |
|---|---|---|
| Raw record retention | Forever | Age after which a node may delete raw records. |
| Rollup retention | Forever | Age after which a node may delete rollup buckets. |

Each node applies the current setting to its own ledger. Shortening retention
removes eligible data on that node's next maintenance pass; lengthening it
cannot restore deleted data. Settings shows each node's ledger size so that
operators can judge the cost of keeping everything. Rollup retention cannot be
shorter than raw record retention.

### Query API

The app-private protocol gains a `usage` domain module in `sinomo-api::app`,
with a typed client in `sinomo-client`. The GUI never opens the ledger.

A query names a time range, a node scope (cluster or selected nodes), filters,
a grouping, and a bucket width. The connected daemon fans the query out to
authorized nodes using the same bounded concurrency and per-node failure
reporting as log collection, merges rollups, and returns:

- grouped series and totals;
- the nodes that answered, failed, or were omitted; and
- for drill-down, a paged list of raw records.

Partial results stay visible as partial; they are never presented as complete.

### Live metrics

Observe also receives bounded counters, such as model requests by provider and
status, tokens by provider and unit, and tool calls by status. These feed the
Cluster Overview's agent activity charts and Explore. Agent, chat, model, tool,
and skill breakdowns are not metric labels; those dimensions stay in the ledger.

## Cost Estimation

Cost is computed at query time from usage units and a price catalog; it is not
stored in records. Editing a price therefore re-prices all retained history.

- `sinomo-lm` ships a default price catalog. Entries are keyed by provider,
  model pattern, and operation, and give a price per unit (per million tokens of
  each token kind, per image tier, per search) with an effective-from time.
- Operator overrides and additions are stored in cluster KV alongside the model
  configuration and take precedence over the built-in catalog.
- Each provider account has a billing mode:

| Billing mode | Display |
|---|---|
| Metered | Estimated cost in the catalog currency. |
| Subscription | "Included in plan", with quota consumption from quota snapshots. |
| Local | Zero cost, with request time shown instead. |

- A record is unpriced when its provider reported no units, or when no catalog
  entry matches it. Totals state the priced share, for example "at least $4.12,
  97% of requests priced", and the UI lists the unpriced models so an operator
  can add prices.

## Budgets

Budgets are display-only and stored in cluster KV. A budget has a scope
(cluster, provider account, model, or agent), a period (day, week, or month), and
an amount. The Usage view shows spend against each budget and a projection for
the period. Budgets do not emit enforcement events, do not affect placement or
model selection, and never block a request.

## Cluster Usage View

`ClusterSubview` gains a sixth view, **Usage**, after Logs. It queries the
ledger only while visible, refreshes on a slower interval than live metrics, and
preserves its controls and scroll position across refreshes.

A shared filter bar selects the time range (preset ranges and custom), node
scope, agent, chat, provider, account, model, and operation. Filters persist
while switching between the view's sections:

- **Summary** shows tiles for estimated cost, tokens by kind, model requests and
  error rate, tool calls and failure rate, skill activations, and skill version
  publications. Charts show cost over time by provider, units by model, usage by
  purpose, top agents and chats by cost, and budget progress. Selecting a tile
  or chart segment applies it as a filter.
- **Providers** groups by provider, account, and model, with requests, errors,
  latency percentiles, units, estimated cost, and price source. Quota cards show
  current remaining limits and reset times, plus quota history from the sampled
  snapshots. The Settings provider rows link here.
- **Tools** groups by tool with call counts by status, duration percentiles,
  detached calls, and executing runtime. Expanding a tool lists recent calls;
  each opens the chat activity at that tool call.
- **Skills** groups by skill with activations split by trigger, the agents that
  used it, and when it was last used. A version timeline shows each publication
  with its source, authoring session, and a link to the version in the skill
  catalog.
- **Records** is a virtualized, filterable list of raw records, like the Logs
  view. Each row links to its chat, session, turn, or tool call.

Node failures and partial coverage appear in the view header, matching the Logs
view. Other surfaces link into the view: the Cluster Overview's agent activity
charts, the provider rows in Settings, and a per-chat usage indicator in chat
that opens Usage filtered to that chat.

## Settings

A Settings section for usage owns:

- raw record and rollup retention, with per-node ledger size;
- billing mode per provider account;
- price overrides and the effective built-in prices; and
- budgets.

All of these are distributed settings in cluster KV and require the same
authority as other model configuration changes.

## Phases

1. **Contract.** Extend `LanguageModelUsage` with the unit breakdown and update
   each provider adapter, including image generation and hosted search. Add the
   usage attribution and events. Route every provider invocation through the
   recording path.
2. **Ledger and query.** Implement the node-local ledger, rollups, retention
   maintenance, the `app::usage` query with node fan-out, the typed client, and
   the Observe counters.
3. **Pricing and settings.** Ship the default price catalog, billing modes,
   price overrides, budgets, and retention settings.
4. **Usage view.** Add the Cluster Usage view with Summary, Providers, and
   Tools.
5. **Skills, records, and links.** Add the Skills and Records sections, the chat
   usage indicator, and links from Overview and Settings.

## Verification

- Provider adapter tests map each provider's native usage report, including
  cache, reasoning, image, and search units, and a request that reports nothing.
- Agent integration tests run turns with tool calls, decision repair,
  compaction, a subagent, a skill activation, and a `skill_update`, and check
  that each produces exactly one correctly attributed record.
- Ledger tests cover torn final records, rollup rebuild, retention changes, and
  a usage append failure that must not fail the request.
- Cluster integration tests query across nodes with one node offline and check
  that totals are marked partial.
- Pricing tests cover re-pricing after an override, subscription and local
  accounts, and the priced-share calculation.
- Headless app renders cover the Usage view sections with empty, partial, and
  populated data.
