# Rollout Operations

This guide is for operators and developers updating installed Sinomo daemons
and modules. Building produces immutable artifacts; rollout distributes,
activates, observes, and, when necessary, rolls them back. For the service
boundary, see the [build service architecture](../build-service-architecture.md)
and the
[architecture overview](../architecture.md#modules-and-controlled-evolution).

Command syntax can change with the installed binary. Use
`sinomo rollout help` as the final reference.

## Choose a Workflow

| Goal | Workflow |
|---|---|
| Update a mixed-platform cluster from source | Cluster build and rollout |
| Update only the current machine from managed source | Local apply |
| Update compatible nodes from an existing artifact | Prebuilt artifact rollout |
| Refresh installation and boot integration | Install |

Before rollout, commit or otherwise preserve the intended source, run the
relevant tests, and confirm the current cluster is healthy.

## Cluster Build and Rollout

Cluster build and rollout is the normal source-based workflow. Rollout discovers
the selected nodes' explicit target requirements, groups compatible targets,
and asks the cluster build service for one verified artifact per group. A cache
hit is distributed immediately. Otherwise the service queues one build for
matching requests and selects a live compatible worker using toolchain support,
available CPU, memory and disk headroom, current load, queue pressure, and
transfer locality.

The selected worker need not be the rollout target. For example, a server with
the correct cross-toolchain should build a Raspberry Pi artifact when it can
finish and deliver it substantially sooner. The client verifies the streamed
artifact against its BLAKE3 digest and build manifest, and every target checks
the rollout manifest's operating system and architecture before activation.
Build resolution for a rollout wave completes before that wave starts staging.

Without `--target`, `sinomo rollout cluster` selects the local daemon and the
authorized remote members discoverable from cluster state. Treat it as a
cluster-wide operation. Use explicit `--target` values and `--skip-local` when
the change should reach only selected nodes.

Import the reviewed checkout into managed source, then request cluster build
resolution and rollout:

```sh
sinomo rollout import-source --repo "$PWD"
sinomo rollout cluster
sinomo cluster health
```

To update one target and skip the local daemon:

```sh
sinomo rollout cluster \
  --skip-local \
  --target 192.168.1.154:21353
```

The default command uses the cluster build service. `--remote-build` is retained
as a compatibility spelling and now selects the same service. For a
mixed-version recovery that must use a checkout already present on the target,
combine it with `--remote-repo`; this bypasses cluster placement but still uses
the same build-owned executor, cache, resource admission, and artifact
verification. `--force-source` applies only while importing a managed source
checkout.

Each build server advertises its installed host target automatically. To let a
server build for additional installed Rust targets, set a comma-separated list
before starting its daemon, for example:

```sh
SINOMO_BUILD_TARGETS=aarch64-unknown-linux-gnu,x86_64-unknown-linux-musl \
  sinomo daemon run
```

Every listed target must already be installed and have the required linker or
cross-toolchain. An invalid configured target makes that server's build
capability unavailable instead of advertising a target it cannot compile.

## Local Apply

Build and activate the current machine's managed source:

```sh
sinomo rollout apply
```

Local apply remains a bootstrap and development convenience. It submits the
managed checkout to the same build-owned planner and executor locally, uses a
release profile, reuses the content-addressed artifact cache and incremental
target directory, and then hands the verified outputs to rollout. A clustered
shared update should use the cluster workflow so other compatible nodes can
reuse the same build result.

Import an external checkout and apply it in one step:

```sh
sinomo rollout apply-external --repo /path/to/sinomo
```

Use `sinomo install` instead when the goal is to refresh the installed binary,
boot entries, daemon bind settings, and managed source together:

```sh
cargo run -p sinomo -- install
```

## Prebuilt Artifact Rollout

Prebuilt rollout is the fastest path when a verified compatible binary or
module already exists. Compatibility is determined by the artifact manifest
and the target contract, not by which node performed the build. Let the CLI
resolve the matching cached artifact or schedule it if missing:

```sh
sinomo rollout cluster --target 192.168.1.154:21353
```

Or provide a binary explicitly:

```sh
cargo build -p sinomo
sinomo rollout cluster \
  --binary target/debug/sinomo \
  --target 192.168.1.154:21353
```

An explicitly supplied artifact is hashed into the rollout manifest and is
restricted to targets reporting the local operating system and architecture.
Unknown or mismatched target information blocks activation; it does not
silently trigger use of the wrong binary.

## Queueing and Fallback

Matching build requests on one server share one durable queued or running job.
If a selected builder becomes unreachable before resolution completes, the CLI
tries the next ranked compatible server. If no worker advertises the required
target and artifact support, the command fails before activating any rollout
target.

The rollout target is only a build candidate when it advertises the required
capability. It receives no local-first preference, so a much faster compatible
server normally wins a cache miss. The command does not silently fall back to
target-local source. An explicit `--remote-repo` is required for that recovery
behavior.

## Activation Safety

Producing or publishing an artifact never activates it. Rollout authority
selects the target set and lane, and the target stages candidate files before
stopping the main daemon. When a shadow daemon is available, it starts the
candidate, waits for health, and restores the previous main binary if activation
fails. A main-only installation can complete a staged self-restart, but it has
less recovery coverage.

The main daemon remains the normal rollout endpoint. The shadow endpoint is a
repair surface for cases where the main control path is unavailable.

## Verify a Rollout

Check both rollout state and live node health:

```sh
sinomo rollout status
sinomo cluster health
sinomo rollout diagnostics
```

For a remote target:

```sh
sinomo rollout diagnostics --target 192.168.1.154:21353
```

Diagnostics include rollout state, the latest local build-service log, and
candidate output tails. Cluster-build job records, source bundles, logs,
workspaces, reusable target directories, and verified cache entries live under
`$SINOMO_DATA_DIR/build/` on each build server.
The main local artifacts are under `$SINOMO_DATA_DIR/rollout/`; use the
diagnostics command before inspecting those files directly.

## Recover a Failed Rollout

First establish which process is reachable:

```sh
sinomo daemon status
sinomo daemon shadow status
sinomo rollout diagnostics
```

If the installed binary is known good and no activation is in progress, start
the existing deployment without replacing it:

```sh
sinomo daemon start
```

Use `install` only when the deployment itself must be replaced. Verify the
selected data directory, invoking checkout and commit, expected binary/module
set, and outage first; installation may stop both daemons, advance managed
source, rebuild native modules, rewrite boot entries, and restart processes:

```sh
cargo run -p sinomo -- install
```

When the main daemon is unavailable but the shadow daemon is reachable, use the
shadow's reported endpoint for diagnostics or a repair rollout:

```sh
SHADOW_ENDPOINT=shadow-host.example:21354
sinomo rollout diagnostics --target "$SHADOW_ENDPOINT"
sinomo rollout cluster \
  --remote-build \
  --remote-repo /path/to/sinomo \
  --skip-local \
  --target "$SHADOW_ENDPOINT"
```

Clear stale status only after independently confirming that the daemon is
healthy:

```sh
sinomo rollout clear-status
```

This clears the recorded status; it does not delete pending candidate files.

## Managed Source

The build service owns the managed source checkout at
`$SINOMO_DATA_DIR/source/sinomo`. Import merges reviewed external changes into
that checkout and removes external remotes so later builds do not depend on a
public forge. Cluster builds identify an immutable source input from this
reviewed state; workers materialize that input instead of depending on an
independently mutable checkout on every target.

To bring self-evolution changes back to an external checkout:

```sh
sinomo rollout export-source --repo /path/to/sinomo
```
