# Rollout Operations

This document describes the current Sinomo rollout workflow for updating installed daemons from source or from a prebuilt binary.

## Concepts

Sinomo installs two daemon binaries under `$SINOMO_DATA_DIR/bin/`:

- `sinomo`: the main daemon binary.
- `sinomo-shadow`: the shadow daemon binary.

The main daemon serves normal local and cluster control traffic. The shadow daemon supervises the main daemon and owns rollout recovery, but it is not required for every deployment. When a local shadow supervisor is running, Sinomo uses it for safer candidate main activation and recovery. When only the main daemon is available, the main daemon can stage a candidate, stop after replying to the rollout request, and let the staged candidate complete activation by itself.

The default data directory is `~/.sinomo`. Important rollout paths are:

- `~/.sinomo/source/sinomo`: managed source checkout.
- `~/.sinomo/bin/sinomo`: installed main binary.
- `~/.sinomo/bin/sinomo-shadow`: installed shadow binary.
- `~/.sinomo/rollout/build.log`: last build log.
- `~/.sinomo/rollout/state.sobj`: last rollout state as a binary Sinomo object.
- `~/.sinomo/rollout/last-main-exit.sobj`: last supervised main process exit as a binary Sinomo object.
- `~/.sinomo/rollout/logs/`: candidate stdout, stderr, and diagnostics.

The managed source checkout is intentionally local. `sinomo install`, `sinomo rollout init-source`, and `sinomo rollout import-source` merge source into `~/.sinomo/source/sinomo` and remove external remotes, so a node can build later without depending on GitHub or another upstream service.

## Safety Model

Cluster rollout is staged through the running main daemon:

1. The target main daemon receives the rollout request.
2. It builds or receives candidate bytes.
3. It writes pending rollout files under `$SINOMO_DATA_DIR/rollout/`.
4. It exits after the accepted response has been flushed.
5. If the local shadow daemon is running, the shadow starts the candidate main binary, waits for health checks, and lets the healthy main daemon finalize the shadow binary update.
6. If the local shadow daemon is not running, the staged candidate starts directly and installs itself as the stable main binary without shadow supervision.

If the candidate main cannot start or does not become healthy, the shadow records failure in `rollout/state.sobj`, restores the previous main binary, and keeps supervising. If the candidate shadow fails after main is healthy, the main daemon attempts to restart the previous shadow binary and records the recovery result.

## Normal Source Rollout

Use this path for normal cluster updates, especially when nodes may run different operating systems or CPU architectures.

```sh
git status --short
cargo test -p sinomo-core
cargo test -p sinomo-cli cluster_
git add <changed files>
git commit -m "Describe the change"
sinomo rollout import-source --repo /path/to/sinomo
sinomo rollout cluster --remote-build
sinomo cluster health
```

`rollout import-source` updates the local managed source checkout. `rollout cluster --remote-build` asks each target to build from its own managed source checkout, usually `~/.sinomo/source/sinomo`.

After a successful rollout, `sinomo cluster health` should show the main daemon member online, with shadow reachability reported through the main node's supervision endpoints and rollout status such as:

```text
accepted (cluster-remote-build)
```

## Installing or Refreshing a Node

Run install from the current source checkout to create or refresh the local installation:

```sh
cargo run --bin sinomo -- install
```

`install` copies the current executable to the installed main and shadow binary paths, exposes the main binary as `~/.local/bin/sinomo` on Unix-like systems, updates boot entries, starts both daemons by default, and imports the current git checkout into the managed source repo. For a LAN-reachable node, the default install bind is `0.0.0.0`.

Useful variants:

```sh
sinomo install --host 127.0.0.1
sinomo install --host 0.0.0.0 --port 21353 --shadow-port 21354
sinomo install --no-start
```

## Local Source Apply

Use local apply when you only want to update the current machine from source:

```sh
sinomo rollout apply
```

To import an external checkout first and then apply it:

```sh
sinomo rollout apply-external --repo /path/to/sinomo
```

Equivalent explicit two-step form:

```sh
sinomo rollout import-source --repo /path/to/sinomo
sinomo rollout apply
```

`rollout apply` builds the managed source with `cargo build -p sinomo`, stages main and shadow binaries, restarts local shadow supervision when available, and verifies that the main daemon becomes healthy.

## Cluster Targets

Without `--target`, `sinomo rollout cluster` targets the local daemon and any cluster member endpoints known from local cluster state.
Target discovery is member-oriented: each authorized remote main daemon member is considered even when its main control endpoint is missing, and Sinomo can choose that member's shadow daemon repair endpoint, including Iroh, as the rollout route.

For explicit rollout targets:

```sh
sinomo rollout cluster --remote-build --target 192.168.1.154:21353
```

To skip the local daemon and update only explicit remote targets:

```sh
sinomo rollout cluster --remote-build --skip-local --target 192.168.1.154:21353
```

If a target stores its source somewhere other than its managed source checkout, pass the target-local path:

```sh
sinomo rollout cluster --remote-build --target 192.168.1.154:21353 --remote-repo D:\src\sinomo
```

`--remote-repo` is interpreted on the target node. Relative paths are resolved under that target's data directory.

## Binary Rollout

Binary rollout is useful for same-platform development clusters.

Build locally and let the CLI ship the compatible binary:

```sh
sinomo rollout cluster --target 192.168.1.154:21353
```

Use an explicit binary:

```sh
cargo build -p sinomo
sinomo rollout cluster --binary target/debug/sinomo --target 192.168.1.154:21353
```

Binary rollout is platform-specific. Targets report `target_os` and `target_arch` in daemon status; incompatible or unknown platforms are skipped or should use `--remote-build`.

## Status and Diagnostics

Show rollout paths and managed source state:

```sh
sinomo rollout status
```

Check cluster-level health and per-node rollout summaries:

```sh
sinomo cluster health
```

Fetch rollout diagnostics from a daemon:

```sh
sinomo rollout diagnostics --target 127.0.0.1:21353
sinomo rollout diagnostics --target 192.168.1.154:21353
```

Diagnostics include the decoded rollout state, build logs, and candidate stdout/stderr tails. If a rollout fails, inspect:

```sh
sinomo rollout diagnostics
cat ~/.sinomo/rollout/build.log
ls -lt ~/.sinomo/rollout/logs
```

Clear a stale rollout status after independently verifying the daemon process is healthy:

```sh
sinomo rollout clear-status
sinomo rollout clear-status --target 192.168.1.154:21353
```

This removes only `rollout/state.sobj`. It does not delete pending rollout artifacts.

## Recovery

If local rollout leaves the main daemon stopped but the shadow daemon is still running, first check status:

```sh
sinomo daemon status
sinomo daemon shadow status
```

If the installed binary is good, restart the installed daemons:

```sh
sinomo install
```

If the installed binary cannot read current local state, run install from a current checkout:

```sh
cargo run --bin sinomo -- install
```

If a remote rollout request fails with a transient daemon control read error, check health and retry the main daemon endpoint:

```sh
sinomo cluster health
sinomo rollout cluster --remote-build --skip-local --target 192.168.1.154:21353
```

Do not target the remote shadow daemon for normal rollout while the main daemon is healthy. The normal rollout handler runs on the main daemon. It uses the target's local shadow daemon for supervised activation when that daemon is running, but it can also complete a main-only activation without the shadow daemon.

If the main daemon or cluster control path is broken but the shadow daemon is still alive, use the shadow daemon as the repair surface. `sinomo daemon shadow status` prints the local shadow Iroh endpoint, and remote operators can use that endpoint directly for diagnostics or a repair rollout:

```sh
sinomo rollout diagnostics --target 'iroh://<shadow-public-key>?relay=<relay-url>'
sinomo rollout cluster --remote-build --skip-local --target 'iroh://<shadow-public-key>?relay=<relay-url>'
```

The shadow daemon must keep its control listener and Iroh proxy independent of KV quorum. Lost quorum should make cluster KV writes read-only, but it must not stop shadow diagnostics, rollout staging, or supervised main recovery.

## Common Workflows

Commit and roll out a source change to the cluster:

```sh
cargo fmt --check
cargo test -p sinomo-core
cargo test -p sinomo-cli cluster_
git add crates docs
git commit -m "Update cluster behavior"
sinomo rollout import-source --repo "$PWD"
sinomo rollout cluster --remote-build
sinomo cluster health
```

Roll out only to one remote node:

```sh
sinomo rollout import-source --repo "$PWD"
sinomo rollout cluster --remote-build --skip-local --target 192.168.1.154:21353
sinomo rollout diagnostics --target 192.168.1.154:21353
```

Update local install after pulling source:

```sh
git pull --ff-only
cargo run --bin sinomo -- install
sinomo cluster status
```

Export managed self-evolution changes back to an external checkout:

```sh
sinomo rollout export-source --repo /path/to/sinomo
```
