# Rollout command selection

Examples use Bash and must run in the selected installation/data-directory
context. Set variables to the already-authorized source and target; command
bundles are alternatives, not a sequence to execute in full. For PowerShell,
use its variable/environment syntax while preserving each command's flags.
Check `sinomo rollout help` on the invoking binary for the current contract.

## Read-only preflight and diagnosis

```bash
sinomo rollout help
sinomo daemon status
sinomo daemon shadow status
sinomo rollout status
sinomo cluster health
sinomo rollout diagnostics
```

For a selected remote endpoint:

```bash
: "${SINOMO_ROLLOUT_TARGET:?Set the authorized host:port endpoint}"
sinomo rollout diagnostics --target "$SINOMO_ROLLOUT_TARGET"
```

For a direct runtime check, use `sinomo daemon status --host HOST --port PORT`.
Confirm the expected node/cluster identity, main process, commit and rollout
phase. Cluster health is a published observation and can briefly retain the
previous PID/revision after the new daemon has accepted its rollout.

Use reported endpoints, not an assumed default port when automatic bind fallback
or a shadow repair path is involved. For remote diagnostics, use a routable
endpoint from `sinomo cluster connectivity`, not another node's reported
`127.0.0.1` control address. Current diagnostics resolve node IDs too and support
`--json`; requested target failures return nonzero. Older versions accepted only
endpoints and could print `failed:` while returning zero.
Keep the data-directory context consistent; individual commands do not all
accept the same host/port options.
Current cluster plans record each node's actual listener ports. Existing listeners
are preserved; `--port` and `--shadow-port` supply connection/bootstrap defaults.
Older clients used group-wide defaults for manifests and restart probes, so
preserve custom ports explicitly when working through one of those clients.

## Prepare immutable source and dependencies

Choose an explicit full Sinomo Git commit ID before a rollout. Normal builds
use the published SUI v0.4.1 registry packages selected by workspace dependencies
and the reviewed lockfile. Cargo also supports local-only patch configuration
for debug builds; see its
[override documentation](https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html).
Ensure a rollout does not inherit an unversioned local override.

The committed `rollout-source.json` names any external Git dependency revisions
and repositories that need embedding; its list is empty for the normal registry
build. Such dependencies require fixed full commit IDs. Use the built-in
preparation command with a new output directory:

```bash
: "${SINOMO_SOURCE_REPO:?Set the committed operator checkout}"
: "${SINOMO_PREPARED_REPO:?Set a new isolated output directory}"
sinomo rollout prepare-source --repo "$SINOMO_SOURCE_REPO" --output "$SINOMO_PREPARED_REPO" \
  --target x86_64-pc-windows-msvc --target x86_64-unknown-linux-gnu \
  --target aarch64-unknown-linux-gnu
```

Select the actual target triples for the deployment. The tool clones committed
source, embeds any listed pinned dependency trees without working-tree edits,
rewrites their dependency paths only in that output, and validates locked metadata.
It writes `deployment-source.json` and commits the prepared snapshot. Pin/lockfile
changes must already be committed in the input. An already prepared clean source
can be built directly without preparing it again.

Builds reject external path dependencies and inconsistent prepared provenance.
They bundle committed Git content with `--locked`; untracked files, dirty edits,
and an external `../sui` directory are not deployment inputs. Source preparation
and metadata validation establish input identity, not successful compilation.

## Update selected remote nodes from source

```bash
: "${SINOMO_SOURCE_REPO:?Set the reviewed external checkout path}"
: "${SINOMO_MANAGED_REPO:?Set the managed source path reported by rollout status}"
: "${SINOMO_ROLLOUT_TARGET:?Set the authorized host:port endpoint}"
sinomo rollout import-source --repo "$SINOMO_SOURCE_REPO"
sinomo rollout cluster --repo "$SINOMO_MANAGED_REPO" --target "$SINOMO_ROLLOUT_TARGET"
sinomo rollout diagnostics --target "$SINOMO_ROLLOUT_TARGET"
sinomo cluster health
```

Repeat `--target` for additional intended endpoints. Omitting the target changes
the scope to automatic cluster discovery, which includes the local installation
unless `--skip-local` is set. An explicit list selects only those targets;
`--skip-local` does not remove an explicitly listed local node. Cluster/module
rollout and current diagnostics resolve authorized node IDs. Record the full
resolved target set, including node identities and discovered endpoints.

Managed source lives at `$SINOMO_DATA_DIR/source/sinomo`. Preserve intended
external changes before import. Current import fetches the source HEAD, saves the
previous managed HEAD as `sinomo-managed-source-backup`, and aligns the managed
checkout to the fetched commit. It does not import dirty source contents.
A dirty managed checkout rejects by default; `--force-source` stashes it before
replacement. Use that option only when replacing the managed state is intended.
The build manifest/source bundle, rather than an unrelated checkout's HEAD,
identifies what remote workers compiled.

The default resource policy uses every available core on the build server.
Never add Cargo `--jobs` flags or `CARGO_BUILD_JOBS` overrides to address resource
issues or work around compiler concurrency bugs. Diagnose and fix the cause,
or select a compatible build server through the existing placement/admission
path. Keep compiler concurrency at the all-cores default.

Current build/rollout commands default to the managed checkout. Use `--repo` to
select another prepared checkout explicitly. Older clients defaulted to the
operator current directory. For an authorized whole-cluster rollout:

```bash
: "${SINOMO_MANAGED_REPO:?Set the clean managed source checkout}"
sinomo rollout cluster --repo "$SINOMO_MANAGED_REPO"
```

The automatic target path keeps the local coordinator last. Preserve the
per-target authority/readiness gates; do not parallelize manual voter replacement
or skip a voter whose activation outcome is still unknown.

`--remote-build` is currently an alias for build-service scheduling. A supplied
`--remote-repo` selects the explicit target-local source recovery path; it does
not discover a better builder. No compatible builder is a reported failure,
not permission to silently change source or target.

## Local source update

```bash
: "${SINOMO_MANAGED_REPO:?Set the clean managed source checkout}"
sinomo rollout apply --repo "$SINOMO_MANAGED_REPO"
```

Or import and apply an external checkout locally:

```bash
: "${SINOMO_SOURCE_REPO:?Set the reviewed external checkout path}"
sinomo rollout apply-external --repo "$SINOMO_SOURCE_REPO"
```

The thin Docker dev image cannot perform these source builds. Use a full
toolchain-bearing image or the appropriate cluster build/prebuilt path. This
distinction also matters when testing rollout changes in the debug cluster.

## Existing binary or a selected native module

```bash
: "${SINOMO_ROLLOUT_BINARY:?Set the verified compatible artifact path}"
: "${SINOMO_ROLLOUT_TARGET:?Set the authorized host:port endpoint}"
sinomo rollout cluster --binary "$SINOMO_ROLLOUT_BINARY" \
  --skip-local --target "$SINOMO_ROLLOUT_TARGET"
```

The current CLI restricts explicit binaries to targets reporting its local OS
and architecture. For other target groups, use the build-service workflow and
its explicit target requirements instead of bypassing manifest verification.

For a native module, confirm its selector and target platform in current help:

```bash
: "${SINOMO_MODULE_SELECTOR:?Set the intended native module selector}"
: "${SINOMO_SOURCE_REPO:?Set the reviewed source checkout path}"
: "${SINOMO_ROLLOUT_TARGET:?Set the authorized host:port endpoint}"
sinomo rollout module native-dylib --module "$SINOMO_MODULE_SELECTOR" \
  --repo "$SINOMO_SOURCE_REPO" --skip-local --target "$SINOMO_ROLLOUT_TARGET"
```

`--skip-native-modules` belongs to the cluster bootstrap path; it is not a generic
flag for `apply-external`. Use the module workflow for a module-only update and
verify its installed artifact, active catalog entry and exported behavior.

## Failed activation or unavailable main daemon

1. Inspect main/shadow status and rollout diagnostics. Identify any queued build,
   candidate, activation, rollback, or pending shadow replacement before retrying.
   If a node-ID/automatic upload fails during connection establishment with
   `DefinitelyNotSent`, but an authorized concrete endpoint remains reachable,
   verify the old runtime and absence of a matching pending rollout. A bounded
   retry of only that node with explicit `--target ENDPOINT` can reuse the verified
   build cache while retaining normal identity and authority checks. Do not apply
   this shortcut to an ambiguous final activation or skip the failed voter.
2. If a known-good installed deployment is simply stopped and activation is not
   in progress, use `sinomo daemon start` in that installation's context.
3. If main is unavailable but shadow is reachable, use its reported endpoint for
   diagnostics and an authorized repair rollout. A documented source recovery is:

```bash
: "${SINOMO_SHADOW_ENDPOINT:?Set the reported shadow repair endpoint}"
: "${SINOMO_REMOTE_SOURCE_REPO:?Set the verified checkout on the repair target}"
sinomo rollout diagnostics --target "$SINOMO_SHADOW_ENDPOINT"
sinomo rollout cluster --remote-build --remote-repo "$SINOMO_REMOTE_SOURCE_REPO" \
  --skip-local --target "$SINOMO_SHADOW_ENDPOINT"
```

4. Use `cargo run -p sinomo -- install` only for the intended installation repair;
   verify data root, source, module set and expected outage first. It may stop both
   daemons, rebuild modules and rewrite boot entries.
5. Clear stale status only after independently confirming health. Do not delete
   candidate files or use a guessed rollback subcommand to bypass the state machine.

A reset during the final activation call can occur before the client observes
any matching progress, while the remote main/shadow state machine completes the
update. Match diagnostics to the current request time and source, confirm
accepted/shadow-finalized with no pending main/shadow replacement, and check the
actual new main process. Do not re-upload merely to obtain a zero client exit.

Source and prebuilt cluster rollouts persist a coordinator plan before upload.
After a client interruption, use the printed ID (or the latest saved plan):

```bash
sinomo rollout verify
sinomo rollout resume
```

`--id ID` selects a specific saved plan. Resume verifies completed targets,
observes the original activation, and reuses the original manifest/upload ID
when transfer must continue. The target's persistent activation claim prevents
another activation from a replay. An ambiguous request to an older daemon that
cannot report upload status remains blocked; lack of that route is not proof
that nothing was sent.

For a previous deployment without a saved plan:

```bash
: "${SINOMO_ROLLOUT_TARGET:?Set the authorized target node or endpoint}"
: "${SINOMO_EXPECTED_COMMIT:?Set the expected runtime commit}"
: "${SINOMO_SOURCE_REPO:?Set the repository containing that commit}"
sinomo rollout verify --target "$SINOMO_ROLLOUT_TARGET" \
  --commit "$SINOMO_EXPECTED_COMMIT" --repo "$SINOMO_SOURCE_REPO"
```

Verification requires the actual main identity and finalized rollout, the
expected source commit when available, and current-term quorum-backed Raft
readiness for the production stable window (five seconds). Do not substitute a
cached KV file or a generic health row. New cluster plans remain blocked while
an earlier saved plan is unfinished. Local `apply` and explicit target-local
source recovery keep their existing handoff paths.

To deliberately retire settled work, use `rollout cancel --id ID`. The command
requires terminal receipts, explicit absence of pending replacements, and stable
node readiness. It leaves completed node updates intact and refuses uncertain
outcomes. A cancelled plan cannot resume; start a new plan for the new intent.

Build jobs, bundles, logs, reusable targets and caches belong under the build
server's `$SINOMO_DATA_DIR/build/`; rollout artifacts/state belong under
`$SINOMO_DATA_DIR/rollout/`. Prefer diagnostics before direct read-only inspection.
Cluster jobs have their own `build/jobs/JOB_ID/build.log`; the `build/local.log`
shown by rollout diagnostics can describe an older local build. Correlate the
job ID, source revision and request time instead of treating an old accepted
rollout or an old successful build log as progress for the current request.
Current workers write compiler output directly to the job log as it runs. Read
or follow the job through the build service:

```bash
: "${SINOMO_BUILD_SERVER:?Set the selected build server node or endpoint}"
: "${SINOMO_BUILD_JOB:?Set the job ID printed during build resolution}"
sinomo rollout build-log --target "$SINOMO_BUILD_SERVER" --job "$SINOMO_BUILD_JOB" --follow
```

Reads are bounded and cursored, with restart-generation handling. They require
the build log's administrator capability. Older workers lack this route and may
buffer output until completion; absence of streaming output is not a retry
signal. Remote filesystem/shell access still requires separate node-local
consent and is unnecessary for the new log route.

Completion evidence includes actual target/runtime identity and the exact feature
probe, with successful, failed, pending and untouched targets accounted for.
Container bootstrap hashes alone are insufficient after a rollout because the
[entrypoint](../../../../docker/entrypoint.sh) can prefer an authorized pending
shadow stage or installed `/data/bin` copies over the host bind mount. PID 1
recovery requires the updated entrypoint as well as the new shadow binary.
