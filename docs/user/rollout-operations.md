# Rollout Operations

This guide is for operators and developers updating installed Sinomo daemons
and modules. Building produces immutable artifacts; rollout distributes,
activates, observes, and, when necessary, rolls them back. For the service
boundary, see the [build service architecture](../build-service-architecture.md)
and the
[architecture overview](../architecture.md#controlled-evolution).

Command syntax can change with the installed binary. Use
`sinomo rollout help` as the final reference.

## Choose a Workflow

| Goal | Workflow |
|---|---|
| Update a mixed-platform cluster from source | Cluster build and rollout |
| Update only the current machine from managed source | Local apply |
| Update compatible nodes from an existing artifact | Prebuilt artifact rollout |
| Initial deployment and daemon startup | CLI or graphical installer |
| Redeploy a legacy installation outside the shared installer | Install with `--runtime-service` |
| Build or update the desktop client independently | Desktop app rollout |

Before rollout, commit or otherwise preserve the intended source, run the
relevant tests, and confirm the current cluster is healthy.

## Desktop App Rollout

A desktop release contains `sinomo-app`, `sinomo-ui-extension-host`, optional
runtime libraries, and `desktop-release.json`. It can be delivered independently
of the daemon and native modules. The initial installer also ships the CLI and
offline default-module/source payload; the CLI hosts local desktop supervision. Running `sinomo install` from that release installs the
desktop app automatically. A runtime-only release remains headless. The explicit
forms are:

```sh
sinomo install --desktop --app-bundle /path/to/desktop-release --add-to-path
sinomo install --headless --add-to-path
# Launch through PATH after starting a new login session:
sinomo-app
# Equivalent supervised launch through the CLI:
sinomo rollout app launch
```

Previously installed desktop bundles can still restore their verified GUI;
the current CLI supplies their daemon host. The launcher always selects the canonical installed CLI rather than a daemon
copy in an old desktop bundle. Retired daemon helpers are never launched.
Approved desktop files are published to `<data>/bin` after the previous UI exits;
readiness failure restores the previous release through the same location.
A remote GUI's connection and pairing configuration is a separate client feature.

Build the local desktop platform through the existing cluster build service:

```sh
sinomo rollout app build --repo /path/to/prepared-source --directory /path/to/desktop-release
sinomo rollout app stage --directory /path/to/desktop-release
sinomo rollout app status
```

To build for another node's desktop platform and offer the update there:

```sh
sinomo rollout app build --repo /path/to/prepared-source --target NODE_ID --directory /path/to/desktop-release
sinomo rollout app stage --directory /path/to/desktop-release --target NODE_ID
```

Staging accepts repeated explicit `--target` values for compatible nodes. It uses
Sinomo's authenticated routed streams, checks each file's length and digest, and
publishes the offer only after the complete bundle is durable. Interrupted
transfers can be retried with the same bundle. Publication receipts prevent an
old retry from replacing a newer offer. Remote staging requires the target's
existing node-local remote-management consent; it never grants that consent or
authorizes an automatic GUI restart.

Build uses committed prepared source, compatible builder discovery, immutable
build keys, and verified artifact downloads. It does not activate either the
daemon or the app. Without `--target`, staging affects only the local
installation: it verifies and copies the complete bundle into the data
directory, then notifies the running app. Daemon/cluster rollout commands keep
their existing target scope. App staging does not remotely launch a GUI or
restart other nodes.

For binaries already produced by a reviewed build, create the manifest with:

```sh
sinomo rollout app package --directory /path/to/desktop-release --revision FULL_COMMIT
```

Cross-build packaging additionally accepts `--target-os` and `--target-arch`.
CI's `scripts/ci/build-release-artifacts.sh` retains its headless default; set
`SINOMO_RELEASE_PROFILE=desktop` to include the GUI, helper, and manifest in
separately named desktop archives. Desktop targets require the corresponding GUI
and native-device development libraries as well as the Rust/linker toolchain.
The Linux x86-64 desktop CI job installs those libraries and builds this profile
using the published SUI release locked in `Cargo.lock`.

The app offers **Restart and restore**, **When idle**, and **Later**. Its launcher
keeps an app-owned daemon alive across GUI replacement. Chat work continues in
the daemon; the new GUI restores the selected chat, drafts, attachments, visible
tool progress, event cursor, and window size and position where supported by the
window manager. An active voice/video call or file picker must finish before
restarting because media devices are app-local.

Candidates must use the supported daemon protocol and workspace schema. The
launcher verifies immutable bytes before execution and waits up to 90 seconds
for a readiness acknowledgement after connection, restoration, and an initial
rendering health window. Failed startup
or an interrupted activation returns to the previous release. Staging and
readiness are distinct states; inspect `rollout app status` for the active and
previous release identities and any rollback detail.

Planned daemon replacement waits up to five minutes for leased chat work to reach
a safe boundary. If work cannot drain, activation is deferred with an error;
retry the existing rollout after that work completes. Accepted pending commands
are recovered automatically by the replacement daemon. Unexpected process loss
can leave a started external effect ambiguous: its request ID is retained and the
runtime requires reconciliation instead of executing it again. An app update
alone does not interrupt the daemon or require this recovery path.

Desktop artifacts and activation records live under
`$SINOMO_DATA_DIR/rollout/desktop/`; the presentation snapshot is
`$SINOMO_DATA_DIR/configs/app/workspace.json`. Durable chat command receipts live
under the agent-owned `events/agent/chat-hosts/` subtree. These files are owned by
their services and must not be manually edited to force an activation.

## Prepare Committed Dependencies

Rollout source must be self-contained. SUI uses published crates.io packages
selected by workspace dependencies and locked with checksums in `Cargo.lock`.
`rollout-source.json` lists any external Git dependencies that need embedding;
the list is empty for the normal registry build. Prepare a deployment checkout
from the committed source:

```sh
sinomo rollout prepare-source --repo "$PWD" --output /path/to/new-deployment-source \
  --target x86_64-unknown-linux-gnu --target x86_64-pc-windows-msvc
sinomo rollout import-source --repo /path/to/new-deployment-source
```

The output directory must be new. Preparation excludes uncommitted source and
dependency edits, embeds any listed pinned Git trees, rewrites their dependency
paths inside the isolated checkout, and resolves the full dependency graph with
locked Cargo metadata for each requested target. A dependency selection that
disagrees with the committed lockfile fails preparation before a prepared snapshot
is committed. It records the source and dependency provenance in
`deployment-source.json` and commits the prepared snapshot. Builds reject external
path dependencies and inconsistent prepared pins. Update and commit dependency
declarations and the matching lockfile before preparing a different version.
Preparation never publishes a repository.

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
sinomo rollout import-source --repo /path/to/new-deployment-source
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
as a compatibility spelling and selects the same service. For a mixed-version
recovery that must use a checkout already present on the target, combine it
with `--remote-repo`; this bypasses cluster placement but still uses the same
build-owned executor, cache, resource admission, and artifact verification.
`--force-source` allows an import to replace a dirty managed checkout. On
`rollout cluster` it also selects that compatibility path instead of cluster
build placement: the CLI builds locally for targets that match its platform and
asks other targets to import the source bundle into their own managed checkout
and build it there.

When omitted, `--repo` resolves to the managed source checkout, independently of
the invoking current directory. An explicit target list is the complete target
set; `--skip-local` affects automatic discovery. Each selected node retains its
existing main and shadow ports. Port flags provide connection/bootstrap defaults;
they do not relocate an existing listener. `--host` remains an explicit bind-host
change and is checked after activation.

Each build server advertises its installed host target automatically. GNU Linux
builders running on x86-64 or AArch64 also offer managed cross compilation for:

- `aarch64-unknown-linux-gnu` and `x86_64-unknown-linux-gnu`;
- `armv7-unknown-linux-gnueabihf`;
- `aarch64-unknown-linux-musl` and `x86_64-unknown-linux-musl`;
- `armv7-unknown-linux-musleabihf`.

These are compatibility choices, not capacity classes. An ARM cloud VM can
serve other architectures; a small x86 machine receives the same resource
checks as a small ARM machine. Placement uses measured resources and cache
availability rather than treating AArch64 as synonymous with an SBC.

The first admitted job for a managed target downloads pinned Zig,
cargo-zigbuild, and host libclang archives over HTTPS, checks their SHA-256 digests, installs the
Rust standard library for the selected compiler through rustup if needed, and
qualifies executable and native-library linking. Preparation is visible in the
build log and failures fail that job explicitly. Capability discovery does not
install tools. Tools and qualification evidence live under
`$SINOMO_DATA_DIR/build/toolchains/managed-cross`; system libraries are not
modified. The build key includes the compiler, SDK, exact target triple, and
GNU glibc baseline of 2.17. A target requiring an older glibc needs an explicitly
installed compatible SDK instead. Musl builds use dynamic libc so the binary
and its native modules share the target runtime.

Builders need `curl` and either rustup or the requested Rust target already
installed. Set `SINOMO_BUILD_CROSS_TARGETS` to a comma-separated subset of the
managed targets before starting the daemon, or to `off` to disable automatic
preparation. An installed SDK for the same exact target takes precedence.

Rollout uses each daemon's reported Rust target triple to distinguish GNU,
musl, and ARM ABI variants. For a legacy daemon that reports only OS and
architecture, verify its ABI locally before using an override for one target:

```sh
sinomo rollout cluster --repo /path/to/prepared-source \
  --target arm-cloud.example:21353 --rust-target aarch64-unknown-linux-gnu
```

An override cannot replace a triple reported by a newer daemon. Without an
exact triple, managed cross compilation is excluded from selection.

To let a server build for additional operator-installed Rust targets, set a
comma-separated list before starting its daemon, for example:

```sh
SINOMO_BUILD_TARGETS=aarch64-unknown-linux-gnu,x86_64-unknown-linux-musl \
  sinomo daemon run
```

Every listed target must already be installed and have the required linker or
cross-toolchain. An invalid configured target makes that server's build
capability unavailable instead of advertising a target it cannot compile.

An x86-64 Linux build server also advertises `aarch64-unknown-linux-gnu`
automatically when the Rust target is installed and a complete
`aarch64-linux-gnu` GCC toolchain and sysroot are present in
`$SINOMO_DATA_DIR/build/toolchains/aarch64-linux-gnu/usr/bin`, or in the
directory named by `SINOMO_BUILD_AARCH64_GNU_BIN`. The cross toolchain's
identity is part of that target's build key; an unusable toolchain is ignored
rather than advertised.

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

For legacy deployments outside the shared installer, use
`sinomo install --runtime-service` when the goal is to refresh the installed binary,
boot entries, daemon bind settings, and managed source together:

```sh
cargo run -p sinomo -- install --runtime-service
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

For containers where shadow is PID 1, the updated entrypoint launches an
authorized staged shadow after restart. Rollout-owned boot recovery promotes it
atomically, retains the previous binary, consumes the stage, and re-executes from
the stable path with the recorded listener settings. Container images must carry
that entrypoint; a binary-only rollout does not update image scripts.

## Verify a Rollout

Cluster source and binary rollouts save their exact targets, upload manifests,
artifact paths, and progress under `$SINOMO_DATA_DIR/rollout/coordinator/` before
uploading. The printed rollout ID identifies that plan. The local coordinator
stays last even when targets use different platforms.

```sh
sinomo rollout verify                 # inspect every target in the latest plan
sinomo rollout resume                 # reconcile and continue that same plan
sinomo rollout resume --id ROLLOUT_ID
```

Resume checks completed targets again, observes an activation already in
progress, and reuses the original upload identity when transfer must continue.
Receiver-side activation claims and receipts prevent duplicate activation. An
unknown outcome on an older daemon without upload-status support fails closed;
it is never treated as proof that the request was not sent. Another voter cannot
advance until the preceding voter has regained current-term quorum-backed KV
readiness for five seconds. A failed or ambiguous activation leaves the plan
unfinished and blocks starting a replacement cluster plan.

Use `sinomo rollout cancel --id ROLLOUT_ID` when deliberately abandoning a settled
plan. Cancellation requires terminal receipts, explicit proof that no replacements
are pending, and stable readiness of the affected nodes. It retires the local plan;
it does not stop an active deployment or undo completed updates. Missing evidence,
including an older daemon that cannot report pending state, fails closed. A new
plan can begin once the old one is complete or safely cancelled.

For a deployment made before saved plans were available, verification can use
explicit targets and an expected source commit:

```sh
sinomo rollout verify --target NODE_ID --commit FULL_COMMIT --repo /path/to/source
```

Matching full commit IDs do not require a local source checkout. `--repo` is
needed to resolve abbreviated IDs reported by older binaries.

These commands perform the normal identity, runtime revision, finalized handoff,
and Raft-readiness checks through Sinomo RPC. Verification does not activate or
clear rollout state. Explicit target-local source recovery (`--remote-repo`) and
the local `apply` handoff retain their existing recovery paths.

Check both rollout state and live node health:

```sh
sinomo rollout status
sinomo cluster health
sinomo rollout diagnostics
```

For a remote target:

```sh
sinomo rollout diagnostics --target 192.168.1.154:21353
sinomo rollout diagnostics --target NODE_ID --json
```

Diagnostics return per-target runtime observations, observation timestamps,
rollout reports, and errors. The command exits nonzero if a requested target's
diagnostics or runtime status cannot be read. Cluster health displays published
observations, which can briefly lag a restarted process; direct diagnostics read
the actual daemon.

Compiler output is written to the build job's log while the process runs. Use
the build server and job ID printed during resolution to inspect that job:

```sh
sinomo rollout build-log --target BUILDER_NODE_ID --job JOB_ID --follow
```

Log reads are bounded, support cursors across worker restarts, and require the
build log's administrator capability. They use the build service and do not
require remote shell or filesystem consent. A log from an older local build in
the diagnostic report does not describe a current cluster build job.

Diagnostics include rollout state, the latest local build-service log, and
candidate output tails. Cluster-build job records, source bundles, logs,
workspaces, reusable target directories, and verified cache entries live under
`$SINOMO_DATA_DIR/build/` on each build server.
The main local artifacts are under `$SINOMO_DATA_DIR/rollout/`; use the
diagnostics command before inspecting those files directly.

### Artifact and Temporary-File Retention

Cleanup runs at operation boundaries and build-service startup. It does not
scan storage while the cluster is idle. Cancellation and ordinary failures
remove operation-owned partial files, download directories, and build scratch.
Git packs marked read-only on Windows are removed with their discarded
workspaces.

| Owner | Retained data | Cleanup boundary |
|---|---|---|
| Rollout coordinator | Artifacts for every unfinished plan, the latest plan, and four recent settled plans | Starting a new coordinator, completing or cancelling a plan |
| Daemon rollout | Canonical rollback binaries and four numbered recovery generations | Successful main/shadow finalization |
| Rollout logs | Sixteen recent process generations, plus logs owned by either live daemon | Successful main/shadow finalization |
| Build service | Four recently used artifact sets per target, profile, and output selection; active jobs and a 24-hour transfer grace period can retain more | Startup, cache hits, and completed builds |
| Build job records | Sixty-four recent terminal jobs, plus the transfer grace period and all active jobs | Build-service maintenance |
| Desktop rollout | Four recent bundles, plus the active, previous, and pending releases | Staging, readiness acknowledgement, or rollback |

Coordinator artifacts are independent of temporary downloads, so interruption
still preserves the exact inputs required by `rollout resume`. Upload receipts
and activation claims remain durable to prevent replay from activating the same
request twice. An unresolved activation claim is preserved rather than expired
by age. Unclaimed daemon upload payloads and legacy transfer scratch become
eligible for removal after 24 hours when no receiver or activation owns them.

Desktop uploads discard their duplicate payloads after publication while keeping
the publication receipt. Workspace restoration backups remain until readiness
or rollback commits. Reusable incremental build directories, toolchains, module
loader state, and unrelated files are outside rollout scratch cleanup.

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

For legacy deployments outside the shared installer, use `install --runtime-service`
only when the deployment itself must be replaced. Verify the
selected data directory, invoking checkout and commit, expected binary/module
set, and outage first; installation may stop both daemons, advance managed
source, rebuild native modules, rewrite boot entries, and restart processes:

```sh
cargo run -p sinomo -- install --runtime-service
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
