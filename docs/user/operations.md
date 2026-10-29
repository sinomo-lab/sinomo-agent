# Operating Sinomo

This guide covers routine lifecycle, health, observation, data protection, and
incident handling. Cluster membership and rollout have dedicated guides because
they change distributed authority or installed binaries.

## Know Which Installation You Are Operating

Before any state-changing command, confirm the data directory and daemon:

```sh
printf '%s\n' "${SINOMO_DATA_DIR:-$HOME/.sinomo}"
sinomo daemon status
sinomo cluster status
```

The CLI, main daemon, and shadow daemon must use the same persistent directory.
A surprising empty cluster or missing provider account often means the command
selected a different directory, not that state was deleted.

## Process Lifecycle

`sinomo install` performs initial setup, registers per-user startup and starts
main and shadow daemons by default. It does not invoke the graphical installer
or replace an evolved release. `--no-start` stages registration without launching
new processes. The CLI and graphical uninstaller both stop the deployment and
remove its recorded startup integration, retaining data unless deletion is
explicitly requested.

For legacy deployments outside the shared installer,
`sinomo install --runtime-service` explicitly selects a redeployment transaction.
It first holds new
agent turns and waits up to five minutes for running turns to finish, failing
rather than interrupting them. It may then stop both daemons, replace installed
binaries, advance managed source from the invoking checkout, build and install
native modules, install the desktop app when run from a desktop release,
rewrite boot integration, and restart. `--no-start` suppresses only the final
start. Verify the data directory, checkout and commit, desired binary, and
expected outage first.

For ordinary process recovery with an already installed known-good binary, use
`sinomo daemon start` rather than redeploying with `install --runtime-service`.

For direct control:

```sh
sinomo daemon start
sinomo daemon status
sinomo daemon stop

sinomo daemon shadow start
sinomo daemon shadow status
sinomo daemon shadow stop
```

Use `sinomo daemon run` or `sinomo daemon shadow run` for a foreground process
whose logs should remain attached to the terminal. Do not run two main daemons
against one data directory; its lock exists to prevent split ownership.

The default main and shadow ports (`21353` and `21354`) are preferred ports.
When an automatic start cannot bind one because it is occupied or reserved by
the operating system, the daemon selects an OS-assigned port, logs a warning,
writes the actual address to its status file, and advertises that endpoint to
the cluster. CLI and graphical-app clients discover the local address from the
same status path; they must not assume the preferred port after startup.

Use `--no-port-fallback` to make a default-port start strict. Supplying
`--port` is always strict as well. `SINOMO_DAEMON_PORT` and
`SINOMO_SHADOW_DAEMON_PORT` provide strict environment-selected ports;
`SINOMO_DAEMON_PORT_FALLBACK` and `SINOMO_SHADOW_DAEMON_PORT_FALLBACK` accept
`true` or `false` to control fallback when no explicit port is supplied.

`sinomo uninstall` removes boot entries and stops daemons but retains the data
directory. Read its current help before using `--no-stop`.

## Routine Health Baseline

Use several independent views rather than trusting one summary:

```sh
sinomo daemon status
sinomo daemon shadow status
sinomo cluster health
sinomo cluster doctor
sinomo dfs status --cluster --json
sinomo rollout status
```

The views answer different questions:

| View | Evidence |
|---|---|
| Daemon status | Local process lock, listener, identity, and version |
| Cluster health | Authorized member reachability, platform, and reported rollout state |
| Cluster doctor | Endpoint advertisement, stale observations, identity, and routing anomalies |
| DFS status | Metadata, replication, pending work, repair, and conflict state |
| Rollout status | Candidate, activation, success, or failure recorded for this installation |

During a partition, a local daemon can be healthy while authoritative cluster
operations fail. Report both facts instead of collapsing them into “the node is
down.”

## Observe Runtime Behavior

Read recent operational evidence locally or across authorized members:

```sh
TARGET_NODE=your-node-id
sinomo observe events --limit 100
sinomo observe logs --limit 100
sinomo observe metrics --limit 200
sinomo observe traces --limit 200

sinomo observe events --all --limit 100 --json
sinomo observe metrics --target "$TARGET_NODE" --json
sinomo observe metrics --name sinomo.kv. --all --limit 0 --watch 5
sinomo observe metrics --name sinomo.kv. --all --limit 0 --watch 5 --samples 12 --json

# Cached OpenMetrics for a local Prometheus/Grafana deployment.
sinomo observe prometheus serve --all
# Then scrape http://127.0.0.1:9464/metrics (default refresh: 15 seconds).
```

Narrow results by event kind, component, operation ID, metric name, traceparent,
or target rather than enabling broad debug output indefinitely.
Metric watch output reports counter rates and interval histogram percentiles;
`--samples` bounds the capture, while an omitted sample count runs until
interrupted. Streaming JSON is JSONL with one object per interval.

The Prometheus bridge is loopback-only and keeps only its latest encoded
snapshot. Repeated HTTP scrapes read that cache rather than repeating cluster
RPC and snapshot-exporter work. Use `--name`, `--target`, `--all`, `--refresh`,
and `--listen` to scope it; use Prometheus/Grafana for retention and alerting.
The graphical app's Cluster tab provides a curated Overview, a generic Explore
browser for every registered metric, Topology, and Activity for on-demand live
operation without requiring Prometheus. Use an Overview card's **Inspect**
action to open its exact metric query, or use Explore directly to search by
name/help, filter by metric kind, node, and labels, and select counter or
histogram transforms. Explore's 1/5/15-minute windows use local collection time
and leave visible gaps across resets, restarts, or unavailable samples. It
retains only a short bounded history for the active query; use
Prometheus/Grafana when history, alerting, or durable dashboards are required.

Live trace controls are leased and explicit:

```sh
sinomo observe control
sinomo observe control --filter 'sinomo_network=debug'
sinomo observe control --reset
```

CPU profiles and network timelines are bounded diagnostic captures. Their help
shows duration and target limits:

```sh
sinomo observe help
```

Tracing and profiling can expose prompts, paths, endpoints, and workload
timing. Protect exported diagnostics accordingly.

## Protect Local State

The data directory contains several authority classes in one tree: node and
cluster identity, Raft state, credentials, DFS content and metadata, agent
sessions, configuration, logs, managed source, builds, rollout recovery, and
evolution evidence. Ownership is documented in the
[storage architecture](../storage-architecture.md).

For a raw filesystem backup:

1. Record the Sinomo version, node ID, cluster ID, and data-directory path.
2. Stop the main and shadow daemons so the copy is point-in-time consistent.
3. Copy the complete data directory while preserving permissions and links.
4. Restart the daemons and run the routine health baseline.
5. Store the backup as sensitive material because it can contain credentials
   and identity keys.

Do not restore one member's data directory while the original member is still
active. That duplicates node identity and durable protocol state. A backup is
for recovery of that installation, not a shortcut for adding another node;
use a join invite for a new installation.

DFS durability depends on each object's policy and current replica state. A
backup of one node does not prove that every cluster file is current or fully
replicated. Inspect DFS status before and after maintenance.

## Add or Change Nodes

Use the [cluster operations guide](cluster-operations.md) for init, invite,
join, role, state authority, migration, leave, and removal. Before changing
authority membership:

- confirm the current authority set and quorum health;
- keep a reliable odd-sized voter set when practical;
- verify that a member is caught up before promotion;
- remove state authority before removing a node;
- preserve the existing cluster identity during recovery.

Node role is placement metadata. It is not consensus authority and does not
grant host permissions.

## Update the System

Use the [rollout operations guide](rollout-operations.md) for local or cluster
updates. A safe update cycle is:

```sh
sinomo cluster health
sinomo rollout import-source --repo /path/to/reviewed/checkout
sinomo rollout cluster
sinomo rollout status
sinomo cluster health
sinomo rollout diagnostics
```

Select targets explicitly for a canary or partial update. A build artifact is
not active until rollout and the receiving node complete their checks.

## Manage Host and Device Authority

Remote management is disabled until a person runs a local command on the
receiving node:

```sh
sinomo node permissions show --json
sinomo node permissions allow remote-management --json
sinomo node permissions deny remote-management --json
```

An enabled remote shell is equivalent to local administrator access. Agents'
external MCP servers need the separate `external-integrations` permission on
the node that runs them:

```sh
sinomo node permissions allow external-integrations --json
sinomo node permissions deny external-integrations --json
```

Device and resource exposure is narrower and names the operation:

```sh
RESOURCE=your-resource-id
OPERATION=your-operation-id
sinomo node permissions expose "$RESOURCE" "$OPERATION" --json
sinomo node permissions hide "$RESOURCE" "$OPERATION" --json
sinomo node permissions hide "$RESOURCE" --json
```

There is no supported remote permission-management command. Do not work around
that boundary by editing another node's files through an unrelated service.

## Incident Order of Operations

1. Stop issuing state-changing retries and record the exact failing command.
2. Confirm the data directory, local process status, and current binary version.
3. Capture cluster health, doctor, DFS status, rollout status, and relevant
   observation records as JSON where supported.
4. Determine whether the failure is local, routing-related, quorum-related,
   storage-related, permission-related, or an activation failure.
5. Preserve logs, operation IDs, invites, manifests, and failed candidate
   evidence before restarting a process.
6. Apply the narrowest documented recovery action and re-run the full baseline.

Never delete cluster, KV, DFS, or rollout state merely because it appears
stale. Those files may be the only evidence needed to preserve identity or
recover a committed operation. Continue with [troubleshooting](troubleshooting.md)
for symptom-specific checks.
