---
name: sinomo-rollout
description: Prepare, perform, verify, or recover Sinomo daemon and native-module rollouts. Use for managed-source import, cluster build selection, targeted deployment, prebuilt artifacts, main/shadow activation, and rollout diagnostics.
---

# Sinomo rollout

Read the [rollout guide](../../../docs/user/rollout-operations.md) and use the
actual invoking binary's `sinomo rollout help` before execution. The current
[CLI implementation](../../../crates/sinomo-cli/src/commands/rollout.rs) is the
source reference; an installed older binary may expose a different interface.
Use the [runbook](references/runbook.md) for workflow-specific command forms.

## Establish source, artifact and target scope

Honor the rollout intent and targets already authorized in the task. Skill
activation alone is not a deployment request. For preparation or diagnosis,
complete the candidate/evidence work within that scope; execute activation,
installation or recovery only when that action and its target are authorized.
Clarify a genuinely missing target rather than inventing one or repeatedly
asking about an already-authorized action.

Identify the invoking executable, external versus managed checkout, source
revision/dirty content, `SINOMO_DATA_DIR`, main/shadow endpoints, target node
identities and platform requirements, and intended daemon/module set. Determine
the currently active artifact and recovery path before replacement. A CLI's
version or a rebuilt checkout does not identify the running daemon.

**`sinomo rollout cluster` without `--target` selects the local daemon and
discoverable authorized remote members. Treat it as cluster-wide.** In the
current implementation an explicit target list is the complete selection;
`--skip-local` controls automatic discovery and does not subtract a node explicitly
listed. Current cluster/module rollout, diagnostics and verification resolve
authorized node IDs or concrete endpoints. Older diagnostics versions accepted
only endpoints; verify the invoking version.
Remote health rows may show that node's loopback control address; obtain routable
endpoints from cluster connectivity instead of copying that address blindly.

## Freeze the dependency versions

A rollout must name a fixed Git commit for Sinomo and use its committed lockfile.
SUI normally uses published crates.io packages with exact workspace versions and
locked checksums. Local debug builds may use a sibling working-tree override, but
that override is not an acceptable deployment source. Do not snapshot uncommitted
dependency edits or resolve a moving branch afresh for each target.

Use the committed `rollout-source.json` pins and `sinomo rollout prepare-source`
to create a new isolated deployment checkout. For any listed external Git pins,
the tool embeds exact dependency trees, records commit/tree provenance, and
rewrites local paths only in the output. It validates locked metadata before
committing the snapshot. Builds reject external paths and inconsistent prepared
pins. An exact Cargo `rev` with a
consistent lockfile is also suitable for dependencies fetched by builders. Do
not publish code or substitute another revision to work around a missing pin.

Validate the prepared workspace with locked Cargo metadata for every target
triple before submitting builds. A source bundle includes committed Git content,
not untracked files, dirty changes, or an external `../sui` directory. Preserve
the developer's local debug overrides and report the prepared snapshot's
relationship to the original Sinomo commit and locked dependency versions.

## Choose the build and activation path

- Cluster source rollout uses reviewed managed source and the cluster build
  service. Builders are selected by compatible capabilities/resources/cache;
  the target need not build its own artifact. Build resolution precedes staging.
- Pass `--repo` explicitly when selecting a prepared checkout. Current cluster
  commands default to managed source; older clients selected the current directory.
  `apply-external --repo` imports committed source and applies the managed result.
  Import aligns managed HEAD to the fetched commit and preserves its previous
  clean HEAD in a backup branch; it is not a content merge of dirty source trees.
- Explicit `--binary` is governed by the CLI's manifest/target restrictions;
  do not treat it as an arbitrary cross-platform binary delivery escape hatch.
  Use cluster build resolution for mixed-platform targets.
- Native modules have a dedicated `rollout module native-dylib` path. Keep module
  identity, interface/ABI, platform and content-addressed artifact provenance
  consistent with the [module contract](../../../docs/module-lifecycle-architecture.md).
- `install` also changes boot integration and managed source and may replace both
  daemons. Select it when installation repair is intended, not as a routine
  substitute for starting an existing healthy deployment.

Builds and artifact publication do not grant activation authority. Keep
[build-service](../../../docs/build-service-architecture.md) state separate from
rollout state and local loader observations. Inspect a compatibility or policy
failure; do not weaken gates to make an artifact activate.

## Verify and recover through the supported path

Record the artifact digest/manifest and resolved source identity, selected target
set, rollout state, and main/shadow health. Verify every intended node reached the
expected runtime revision/artifact, then run the specific behavior that motivated
the update. Check relevant module offers/catalog entries when modules changed.
A successful build or staging response is not deployment completion.

After timeout or partial success, inspect diagnostics and the candidate/build
identity before retrying. Preserve active jobs and previous artifacts while an
outcome is ambiguous. The main endpoint is the ordinary control surface; the
shadow's reported endpoint is a repair path when main is unavailable. Use the
existing health/rollback machinery rather than editing rollout files or copying
an executable over a running installation.

A client connection reset during activation can leave the remote state machine
running successfully. Correlate the request time/source with accepted,
shadow-finalized diagnostics, absent pending replacements, and direct main-daemon
status at the expected revision. Cluster health can briefly retain an older
PID/revision. Use `rollout resume --id ID` to reconcile the saved operation;
completed targets are verified without another activation. Receiver claims and
receipts protect replay of the same upload identity.
When the authorized intent abandons a settled plan, use `rollout cancel --id ID`.
It requires terminal receipts, no pending replacements, and stable readiness;
it cannot force an unknown operation aside or undo completed node updates.

Use a current CLI outside the installation being replaced when the installed
client lacks required source/identity checks. Preserve the rollout coordinator
until remote targets have converged. Do not advance to another Raft voter after
an indeterminate activation; establish the previous voter's authoritative
readiness first. Run `rollout verify --id ID`, or use explicit `--target` and
`--commit` for an earlier deployment without a saved plan. These reuse the
identity-bound main-daemon and current-term quorum-backed KV checks, including
the target's applied state and stable readiness window. Reachability or an
accepted rollout record alone does not prove voter rejoin. Diagnostics now
support `--json` and return nonzero for target failures; older clients printed
failure sections while returning zero.

`clear-status` only clears a record and leaves pending files; it is not recovery.
Use it after independently establishing the real deployment is healthy. Retain
the diagnostics, failing phase and remaining target states if recovery cannot
complete. Do not report success for targets that were merely reachable, staged,
or omitted after a failure.
