# Usage and Cost Estimates

Open **Cluster → Usage** to inspect work recorded by Observe. The view retains
its filters when switching between Summary, Providers, Tools, Skills and Records.
Use the range presets or enter a UTC date or timestamp for an exact range. The
end of a custom range is exclusive. Leaving the node filter empty includes the
cluster; enter one node ID, or comma-separated IDs, to select particular nodes.

## Understand the Results

- **Summary** shows request, tool and skill counts, reported tokens, estimated
  costs, trends and budget progress. Select a tile or chart entry to narrow the
  view. Selecting a point on the cost chart selects its time bucket.
- **Providers** groups requests by provider, account and model, including errors,
  latency, units and price sources. Quota snapshots show remaining limits, reset
  times and credit history. They are observations, not quota enforcement.
- **Tools** shows outcomes, duration, detached calls and execution location.
  Inspect a tool to page through its calls.
- **Skills** shows activation triggers and users, plus version publications and
  their authors. Open a published version in the skill catalog.
- **Records** pages through individual events. Open the existing activity evidence
  for an operation or tool call, or navigate to its conversation. Background
  service work may have no conversation; unavailable evidence is shown explicitly.

A partial result is useful but not complete. The header and detail messages
identify failed or omitted nodes and any grouping limits. Narrow filters or select Providers, Tools or Skills for a coarser grouping. Records use a stable cursor so paging is
not limited to the first collection window. The raw evidence page and the
aggregate totals can cover different amounts of history after retention.

Missing units mean **unreported**, not zero. Cache-read and cache-write counts
are subsets of total input; reasoning is a subset of output. Do not add these
subsets to their totals. Retries count additional dispatched Sinomo transport attempts;
opaque provider command harnesses may not expose their internal retry counts.

## Configure Accounting

Open **Settings → Usage** and edit the accounting fields. Changes use the same
configuration authority as other cluster settings and require an explicit Save.

**Retention:** blank values keep history forever. Raw records and aggregate
projections have separate durations in seconds. Aggregate retention must be at
least raw retention. Maintenance applies each node's current policy. Shortening
retention deletes eligible history; lengthening it cannot restore deleted data.
Refresh the storage card to inspect each node's retained size and any collection
failures.

**Billing mode:** choose Metered, Subscription or Local for each provider account.
Subscription work displays as included in the plan; quota snapshots show its
reported consumption. Local work has zero estimated monetary cost. Request
durations remain available in both cases.

**Prices:** the catalog lists built-in rates and operator overrides. An entry
matches a provider, model pattern and operation from its effective time onward.
A trailing `*` matches a model prefix. Token rates are per million tokens; image,
search and named-unit rates are per unit. The effective time uses Unix
milliseconds in the configuration editor. Separate currencies are never added
together. Editing rates re-prices retained history without rewriting usage events.

A request is unpriced when no reported units or applicable prices are available.
The view lists unpriced models and the priced share. An estimate with missing
prices is a lower bound, not a provider invoice or a complete spending total.

**Budgets:** configure a scope, currency, amount and UTC day, Monday-based week
or calendar month. Progress and projections use retained actual work. Budgets
are display-only: they never stop requests, change model selection, alter
placement or emit enforcement actions.

## Recording and Failure Visibility

Observe retains usage on the node that performed the work. Only compact settings
live in cluster KV. Usage events contain attribution, status, timing and billable
units; prompts, model output and tool content remain in their existing evidence
stores rather than being copied into accounting history.

Observe exposes live usage counters and histograms through its usual Metrics
and Explore surfaces. Queue, transport, storage and maintenance failures appear
in Observe logs and delivery counters. Recording failures do not fail the work
being observed. If Observe is unavailable on a node, a successful model response
is not proof that its accounting event was retained.
