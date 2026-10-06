# Troubleshooting

This guide starts with safe, read-only checks. It does not replace the command
help or subsystem architecture when a repair changes authority.

## Collect a Baseline

Record the command, timestamp, host, Sinomo version, and selected data directory.
Start with the [routine health baseline](operations.md#routine-health-baseline),
then capture the additional evidence that remains available:

```sh
sinomo cluster status
sinomo rollout diagnostics
sinomo observe events --limit 200 --json
sinomo observe logs --limit 200 --json
```

If a command fails, keep its exact output. A sequence showing which surfaces
still work is more useful than repeatedly retrying the final failed mutation.

## The CLI Shows an Empty or Different Installation

Likely cause: the shell and installed daemon use different data directories.

```sh
printf '%s\n' "${SINOMO_DATA_DIR:-$HOME/.sinomo}"
sinomo daemon status
```

Check the environment used by service startup and recent commands. Do not copy
individual identity or Raft files between directories. Select the intended
complete directory and restart the matching daemon.

## The Main Daemon Is Unreachable

Check both process roles:

```sh
sinomo daemon status
sinomo daemon shadow status
sinomo observe logs --component daemon --limit 100
```

If no rollout is in progress and the installed binary is known good, a normal
`sinomo daemon start` is the narrow first recovery. If activation failed, keep
the candidate and rollout evidence and follow
[rollout recovery](rollout-operations.md#recover-a-failed-rollout).

A stale lock, listener failure, and process crash are different conditions.
Do not remove a lock file until the status output and process inspection prove
that no owner remains.

If the preferred port is occupied, an automatic start should report a warning
and publish a different control address in `sinomo daemon status`. An explicit
`--port` or `SINOMO_DAEMON_PORT` remains fail-closed. If automatic fallback also
fails, inspect OS listeners and reserved/excluded port ranges rather than
deleting daemon state.

## Cluster Writes Fail but Reads Still Work

Likely cause: the node has local committed state but cannot establish current
quorum or leadership evidence.

```sh
sinomo cluster health
sinomo cluster peers
sinomo cluster doctor
sinomo observe events --kind cluster. --limit 200 --json
sinomo observe metrics --name sinomo.kv. --all --json
sinomo observe metrics --name sinomo.kv. --all --limit 0 --watch 5 --samples 12
```

Restore reachability to the existing authority members. Do not initialize a
new cluster or report a local/stale read as an authoritative result. Membership
repair is appropriate only when its help matches the observed same-cluster
condition.

## Join or Invite Fails

On the existing member, confirm the main daemon is reachable and the introducer
address is usable from the joining machine. Create a new short-lived invite if
the old one expired:

```sh
INTRODUCER=192.168.1.10:21353
sinomo cluster status
sinomo cluster invite --introducer "$INTRODUCER" --output join.txt
```

On the new machine, verify that its data directory is the intended new
installation and that its daemon is running. If it is already settled in
another cluster, inspect `sinomo cluster migrate plan` instead of forcing a
fresh join.

Invites are credentials. Do not paste their contents into public logs or issue
reports.

## Provider Authentication or Model Calls Fail

Separate credential, catalog, account, quota, and transport checks:

```sh
PROVIDER=your-provider-id
sinomo providers test "$PROVIDER"
sinomo models list --provider "$PROVIDER"
sinomo models options show --provider "$PROVIDER"
sinomo usage --provider "$PROVIDER" --json
```

Confirm that the daemon and CLI use the same data directory and that the
selected account can access the exact model ID. Provider outages and rate
limits do not justify copying credentials into cluster configuration.

## DFS Is Degraded or a File Is Missing

Inspect before repairing:

```sh
SIFS_PATH=/sinomo/path/to/file
sinomo dfs status --cluster --json
sinomo dfs path "$SIFS_PATH" --json
sinomo dfs repair --json
sinomo dfs conflicts --json
sinomo dfs services --json
```

Distinguish a missing replica, lagging metadata, unavailable primary, policy
violation, and explicit conflict record. The current `dfs repair` command is an
inspection surface that lists inferred missing-replica work; it does not apply
repairs. Conflict resolution commands deliberately choose or remove history and
should be run only after inspecting stored versions.

If missing replicas remain but no repair attempts are queued, an administrator
can explicitly reconstruct the work through the running DFS owner:

```sh
sinomo dfs repair retry --json
sinomo dfs repair retry --target NODE_ID --json
sinomo dfs status --cluster --json
```

The retry command requests metadata reconciliation; it does not report completed
replicas. The normal repair worker verifies exact versions and content and keeps
the configured replication requirements. The command requires a running main
daemon and does not accept an offline `--metadata-root`.

Old terminal namespace jobs can be compacted into exact retry receipts through
the same administrator path. Name each job explicitly; the owner retains jobs
younger than seven days, running jobs, and jobs needed by pending requests:

```sh
sinomo dfs repair retry --target NODE_ID --compact-namespace-job JOB_ID --json
sinomo dfs status --cluster --json
```

Repeat `--compact-namespace-job` to select several jobs. This does not retry a
rejected restore or overwrite its destination. It preserves the terminal result
and removes only the selected eligible job bodies. The response acknowledges
the accepted scope; subsequent DFS status verifies completion.

A FUSE mount blocked on mounted-mutation handoff waits for the daemon to finish
durable recovery. Inspect `sinomo dfs mount --status --json` and allow bounded
reconciliation to complete. If repeated mount attempts interfere with recovery,
temporarily run `sinomo dfs mount --disable --json`, request `dfs repair retry`,
then restore the configured mount with `sinomo dfs mount --background`. Preserve
its original root, mount point and synchronization options when restoring it.

Never delete content-store or metadata files directly. A byte that appears
unreferenced locally may still be required by a committed version, repair, or
another member.

## A Windows Cloud Files Mount Is Slow or Busy While Idle

Start with the live supervisor and DFS state:

```powershell
sinomo dfs mount --status --json
sinomo dfs status --cluster --json
sinomo dfs conflicts --json
```

Inspect `worker_ready`, pending/retrying/failed operations, conflicts, attention
records, provider connection state, and restart count. Startup reconciliation,
an earlier local write, a committed remote namespace change, or a retry can
legitimately perform finite work without a new Explorer access. Once those
obligations settle, the Cloud Files layer has no periodic scan, heartbeat,
config poll, or scheduled disk I/O. Persistent settled-idle I/O or Explorer
latency outside the projected tree is a defect; record the exact build,
`SINOMO_DATA_DIR`, mount status, relevant process I/O counters, and the affected
paths and timestamps.

To isolate the provider without losing its durable identity, use:

```powershell
sinomo dfs mount --disable --json
```

Do not manually delete the sync root, registration journal, queue, manifest,
staged registration, pending unregister, provider bundle, or cursor-recovery
files. Their presence and exact temporary successors distinguish safe replay
from replacement or corruption. If status reports retained cursor-only state,
inspect it without applying changes:

```powershell
sinomo dfs mount --recover-cursor-only-state --json
```

Applying that recovery is a break-glass reconciliation step requiring the exact
returned token and explicit acknowledgement. Preserve the inspection output
first. See the
[Windows Cloud Files architecture](../cloud-files-architecture.md) for the
ownership and recovery state machine.

## Windows Explorer Does Not Show a Sinomo File Status

The standard Cloud Files availability and in-sync badges come from CFAPI. The
additional Sinomo labels appear only for outstanding local writeback, retry,
conflict, or failure state; a healthy completed file deliberately has no custom
label. Add the **Status** column in Explorer when the current layout does not
show it.

If a pending operation is visible in `sinomo dfs mount --status --json` but no
custom label appears, inspect the daemon log for `Cloud Files Explorer custom
status` or `Cloud Files Explorer status update`. Custom-property registration
is ancillary, so its failure does not stop hydration or writeback and Sinomo
does not poll or repeatedly write merely to recover decoration. Record the
Windows version, exact build, mount path, affected item, and log message before
remounting with a corrected build.

## A Rollout Is Stuck or Failed

```sh
sinomo rollout status
sinomo rollout diagnostics
sinomo cluster health
sinomo daemon shadow status
```

Determine whether the failure occurred during source import, build resolution,
artifact transfer, staging, candidate startup, or health observation. Preserve
build and rollout evidence before retrying. `rollout clear-status` clears the
recorded status only; it is not a repair and does not delete pending candidates.

## Remote Management Is Denied

Membership and reachability are insufficient. On the receiving node, a person
must inspect and enable the permission locally:

```sh
sinomo node permissions show --json
sinomo node permissions allow remote-management --json
```

If the permission is already present, use the target node ID with
`sinomo remote info --target NODE_ID` and cluster diagnostics to distinguish
policy rejection from route failure.
There is intentionally no remote command that enables this permission.

## A Sandbox Command Fails

Inspect the sandbox, backend, pinned toolchain, and policy separately:

```sh
SANDBOX_ID=your-sandbox-id
sinomo sandbox status "$SANDBOX_ID"
sinomo sandbox config "$SANDBOX_ID"
sinomo sandbox doctor "$SANDBOX_ID"
sinomo sandbox logs "$SANDBOX_ID"
sinomo sandbox policy show
```

Do not switch to an unsandboxed host command merely to bypass a denied
capability. Confirm that the requested mount, toolchain command, memory limit,
wall-time limit, and network policy are part of the intended sandbox contract.

## When Reporting a Problem

Include:

- the Git commit or installed version and platform;
- whether the worktree was dirty when the binary was built;
- the command and complete error text;
- local node and cluster IDs, redacted only where necessary;
- the selected data-directory path without its credential contents;
- relevant operation, trace, rollout, request, object, or session IDs;
- health and doctor output from more than one member when possible;
- the smallest reproduction and whether it survives a process restart.

Remove API keys, invite material, identity private keys, raw prompts, private
file content, and access tokens. Preserve structural IDs and timestamps when
they are needed to correlate the evidence.
