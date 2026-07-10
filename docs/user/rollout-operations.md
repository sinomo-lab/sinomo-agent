# Rollout Operations

This guide is for operators and developers updating installed Sinomo daemons.
For the supervision and artifact model, see the
[architecture overview](../architecture.md#modules-and-controlled-evolution).

Command syntax can change with the installed binary. Use
`sinomo rollout help` as the final reference.

## Choose a Workflow

| Goal | Workflow |
|---|---|
| Update a mixed-platform cluster from source | Remote build |
| Update only the current machine from managed source | Local apply |
| Update compatible development nodes quickly | Binary rollout |
| Refresh installation and boot integration | Install |

Before rollout, commit or otherwise preserve the intended source, run the
relevant tests, and confirm the current cluster is healthy.

## Remote Build

Remote build is the normal cluster workflow. Each target builds from its own
managed source checkout, so nodes may use different operating systems or CPU
architectures.

Without `--target`, `sinomo rollout cluster` selects the local daemon and the
authorized remote members discoverable from cluster state. Treat it as a
cluster-wide operation. Use explicit `--target` values and `--skip-local` when
the change should reach only selected nodes.

Import the reviewed checkout into the local managed source, then request a
remote build:

```sh
sinomo rollout import-source --repo "$PWD"
sinomo rollout cluster --remote-build
sinomo cluster health
```

To update one target and skip the local daemon:

```sh
sinomo rollout cluster \
  --remote-build \
  --skip-local \
  --target 192.168.1.154:21353
```

If a target uses a non-default source location, `--remote-repo` is interpreted
on that target:

```sh
sinomo rollout cluster \
  --remote-build \
  --target 192.168.1.154:21353 \
  --remote-repo 'D:\src\sinomo'
```

## Local Apply

Build and activate the current machine's managed source:

```sh
sinomo rollout apply
```

Import an external checkout and apply it in one step:

```sh
sinomo rollout apply-external --repo /path/to/sinomo
```

Use `sinomo install` instead when the goal is to refresh the installed binary,
boot entries, daemon bind settings, and managed source together:

```sh
cargo run -p sinomo -- install
```

## Binary Rollout

Binary rollout is appropriate only when the target platform matches the build
platform. Let the CLI build and select the binary:

```sh
sinomo rollout cluster --target 192.168.1.154:21353
```

Or provide one explicitly:

```sh
cargo build -p sinomo
sinomo rollout cluster \
  --binary target/debug/sinomo \
  --target 192.168.1.154:21353
```

Use remote build when the target reports a different or unknown operating
system or architecture.

## Activation Safety

The target stages candidate files before stopping the main daemon. When a
shadow daemon is available, it starts the candidate, waits for health, and
restores the previous main binary if activation fails. A main-only installation
can complete a staged self-restart, but it has less recovery coverage.

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

Diagnostics include rollout state, the build log, and candidate output tails.
The main local artifacts are under `$SINOMO_DATA_DIR/rollout/`; use the
diagnostics command before inspecting those files directly.

## Recover a Failed Rollout

First establish which process is reachable:

```sh
sinomo daemon status
sinomo daemon shadow status
sinomo rollout diagnostics
```

If the installed binary is known good, refresh and restart the installation:

```sh
sinomo install
```

If it cannot read current local state, run install from a current checkout:

```sh
cargo run -p sinomo -- install
```

When the main daemon is unavailable but the shadow daemon is reachable, use the
shadow's reported endpoint for diagnostics or a repair rollout:

```sh
sinomo rollout diagnostics --target '<shadow-endpoint>'
sinomo rollout cluster \
  --remote-build \
  --skip-local \
  --target '<shadow-endpoint>'
```

Clear stale status only after independently confirming that the daemon is
healthy:

```sh
sinomo rollout clear-status
```

This clears the recorded status; it does not delete pending candidate files.

## Managed Source

The managed source checkout lives under `$SINOMO_DATA_DIR/source/sinomo`. Import
merges reviewed external changes into that checkout and removes external remotes
so later builds do not depend on a public forge.

To bring self-evolution changes back to an external checkout:

```sh
sinomo rollout export-source --repo /path/to/sinomo
```
