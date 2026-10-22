# Testing and Validation

Sinomo uses several evidence layers because a unit test cannot establish
cluster behavior and a live cluster run cannot exhaustively explore a protocol
state machine. Choose the smallest relevant layer while iterating, then add the
layers required by the changed boundary.

## Fast Local Loop

```sh
CRATE=sinomo-cluster
TEST_FILTER=your_test_name
cargo test -p "$CRATE" "$TEST_FILTER"
cargo check -p "$CRATE"
cargo build -p sinomo
git diff --check
```

Use a precise test filter during iteration. Before handoff, run the complete
test target or crate that owns the change so a filter does not hide adjacent
regressions. Replace the example values with real package, test, or runtime
identifiers.

Format Rust changes with:

```sh
cargo fmt --all
cargo fmt --all -- --check
```

If the workspace has pre-existing formatting drift outside the patch, do not
format unrelated code silently. Format the touched files, report the baseline
failure, and keep the contribution scoped.

## Crate and Integration Tests

Representative distributed test targets are:

```sh
cargo test -p sinomo-kv --test kv
cargo test -p sinomo-cluster --test control_plane
cargo test -p sinomo-dfs --test networked_dfs
```

Use unit tests for pure parsing, validation, state machines, ordering, and
selection. Prefer integration tests for cross-crate network, KV, cluster, DFS,
daemon, persistence, sandbox, and restart behavior.

Filesystem tests should use `sinomo-testdir::TestDir` when possible. Successful
owned roots are cleaned on drop; panic evidence and explicitly retained roots
remain available. Constructors never sweep other tests' roots by age.

For bounded Linux runs, use `scripts/ci/run-with-test-fixtures.py --artifacts ABSOLUTE_NEW_DIR
-- COMMAND...` inside the normal shared build/resource guard. It gives the
invocation a private TMPDIR and retains fixtures until the outcome is known.
After processes drain, success reclaims that private directory; failure first
creates, flushes and compares a durable tar archive, then reclaims the originals.
Logs, exit status, archive digest and recovery instructions stay in the artifact
directory. Failed verification or a live user of the fixture blocks reclamation.
The wrapper never sweeps shared or previous-run directories. Use
`--retention failures` for the testdir crate's own Drop-behavior tests.

Do not globally export `SINOMO_TEST_KEEP_TMP=1` for routine development. Bare
test runs killed before cleanup still require an explicit ownership/evidence
audit; age alone is not permission to remove their fixtures. Keep permanent
receipts and unique failure evidence on a durable filesystem, not in `/tmp`.

## Trust-Ring Gate

Run this for every manifest, facade, feature, crate, or cross-ring import
change:

```sh
cargo test -p sinomo-core --test workspace_ring_boundaries
```

It checks ring metadata, dependency direction, facade shape, ABI exceptions,
native-module default membership, the network/KV production split, and the
separation of the app, daemon, and product components.

## DFS/KV Boundary Lint

Run this for DFS, DFS history/attachment, or KV-facing changes:

```sh
bash scripts/ci/check-dfs-kv-boundary.sh
```

The Dylint rule rejects implicit KV access from ordinary DFS operations. It
needs a pinned nightly and `cargo-dylint`; see the
[lint README](../../lints/sinomo-architecture/README.md) for installation,
boundary markers, and its limits.

## Broad Rust Gates

```sh
# Default members only.
cargo test

# Every workspace member with default features.
cargo test --workspace
cargo check --workspace

# Lint all workspace targets.
cargo clippy --workspace --all-targets
```

Plain `cargo test` is not every workspace member. Platform-specific and opt-in
module packages may require their own feature and environment.

## Runtime Tests

Runtime tests exercise registered product behavior rather than Cargo test
functions:

```sh
cargo run -p sinomo -- test list
cargo run -p sinomo -- test list --tag cluster-dfs
TEST_ID=your-test-id
cargo run -p sinomo -- test run "$TEST_ID" --json
```

Tests that contact providers or require a formed cluster stay opt-in through
`--live` and `--require-cluster`. Point them at an isolated
`SINOMO_DATA_DIR`; never let a test mutate an operator's normal installation.

## Agent Tests Without a Model

The `scripted` provider (`sinomo-lm`, feature `scripted-provider`) replays a
`ModelScript` behind the normal `LanguageModelProvider` contract, so request
rendering, streaming, tool orchestration, and cancellation run unchanged. Each
exchange can check the request it receives and replies with text, native tool
calls, usage, failures, gated replies, or a wait for cancellation. Unexpected
requests, failed checks, and calls to tools the request did not offer fail the
request and are reported by `assert_finished`. The script file format is
documented in `crates/sinomo-lm/src/scripted/mod.rs`.

| Tier | Where | Use |
|---|---|---|
| Inline scripts | `crates/sinomo-agent/tests/*.rs` | Turn loop, cancellation, pause, detached calls |
| Recorded scenarios | `crates/sinomo-agent/tests/scenarios/` | Realistic sessions against real workspace tools |
| Daemon end to end | `crates/sinomo/tests/scripted_chat.rs` | Chat RPC, persistence, and restart through a real daemon |

```sh
cargo test -p sinomo-agent --test scenarios
SINOMO_UPDATE_SCENARIOS=1 cargo test -p sinomo-agent --test scenarios
cargo test -p sinomo --features scripted-provider --test scripted_chat
```

A scenario run is compared with its `.outline.txt` (items, tool statuses, and
event kinds per turn). Rewrite outlines only for intended behavior changes and
review their diff. To add a scenario, add its JSON file and its name to the
`scenarios!` list.

A daemon selects the provider with `sinomo models default --provider scripted
--model scripted` and replays the script named by
`SINOMO_SCRIPTED_PROVIDER_SCRIPT`. It writes `<script>.report.json` after each
request and opens a gated reply when `<script>.gates/<gate>` exists.

To record real interactions, set `SINOMO_RECORD_MODEL_SCRIPT=<file>` on a
feature build, or re-record scenarios against a configured provider:

```sh
SINOMO_RERECORD_SCENARIOS=<provider>/<model> cargo test -p sinomo-agent --test scenarios
```

Re-recording writes `<name>.recorded.json` beside each scenario and is never
replayed. Recorded strings are redacted (home directory, secret-looking
environment values, and credential-shaped tokens) and reasoning and provider
continuation state are dropped, but review every recording before keeping any
of it. Recordings expect only the outputs of the previous reply's tool calls;
tighten their checks by hand.

## Live Desktop Agent Verification (Windows)

Build the opt-in fixture host, then run the real agent against owned test windows:

```powershell
cargo build -p sinomo --features desktop-e2e
.\scripts\test-desktop-agent.ps1 -Model gpt-6.1-sol -UseInteractiveDesktop
# Run just one case:
.\scripts\test-desktop-agent.ps1 -Model gpt-6.1-sol -UseInteractiveDesktop -Case agent.desktop.app-drag-resize
```

The launcher requires `ffmpeg` and `ffprobe` on PATH and an existing Sinomo Codex
credential. `-Credentials` can select another stored credential file. It reads
the token without copying it into the result bundle, checks expiry before each
model request, disables credential refresh, and stops the suite on missing,
expired or rejected credentials. The requested model is sent unchanged; absence
from a provider's menu does not trigger model substitution.

Each case creates a fresh data directory, local resource grants, a component
host with only its desktop module, and a real `AgentSessionRuntime`. The normal
module tool projection and service dispatch handle actual model tool calls.
Local object-RPC uses production body framing and ownership through the network
test-support adapter. No daemon, cluster, operator workspace or distributed
settings are required or changed. This exercises the local agent-to-device
path; it does not establish cross-node routing behavior.

The agent receives native image inputs sampled from the continuous, shared
Sinomo media stream. Each new observation waits for a frame captured after the
preceding tool completion. This is a visual tool loop with a recorded video,
not evidence of a realtime provider's native video API. The separate
`sinomo voice call --computer-stream/--app-stream` path supplies such consumers.

| Case | Independent acceptance condition |
|---|---|
| `agent.desktop.app-form` | Submit a fresh code, set counter to 3 and toggle ON while another window retains focus and the pointer stays unchanged |
| `agent.desktop.app-drag-resize` | Resize, drag into a target, scroll exactly three ticks and submit the code in an occluded background window |
| `agent.desktop.computer-form` | Complete the form through raw input while the owned full-screen fixture retains foreground |
| `agent.desktop.gamepad-capability` | Attempt native neutral gamepad injection and close the input lease; unsupported hosts are explicitly skipped |
| `agent.desktop.gamepad-state` | Observe A, stick and trigger values through XInput or Windows.Gaming.Input, then observe neutral before closing the lease; unsupported hosts are explicitly skipped |

`-UseInteractiveDesktop` explicitly allows the owned windows on the current
desktop. Setup may click an owned, hit-tested window to establish foreground,
restoring the pointer before measurements begin. Raw tests occupy the primary
display and move the pointer. Avoid user input during the run. Foreground and desktop checks stop raw input and frame
forwarding when ownership changes; a hidden watchdog stops only the exact owned
child on timeout or launcher failure and restores the previous desktop, pointer
and foreground. These guards are test isolation, not a security sandbox.
Without that flag, the launcher uses a separate Win32 desktop. On the verified
Windows host, WGC returned a zero-sized capture item there even after painting
and activating the window; interactive fixture mode was required. The harness
does not silently switch modes. `-FixtureOnly` records a short setup diagnostic
and returns **skipped**, explicitly stating that no model ran.

Results go into a new `benchmark-results/desktop-agent-*` directory. Each case
retains its MP4, frame timeline, exact model observations, agent trace, RPC
receipts, independent application-state assertions and `ffprobe` validation.
The report attaches files with MIME type, size and BLAKE3 digest, including on
failure. `source.json` records the binary hash, Git revision and dirty-source
hashes. MP4 recording uses FFmpeg only as an artifact sink after Sinomo's shared
encoder and media transport; repeated latest frames preserve recording time
and are declared in its metadata. Failed runs are retained, and reusing an
existing run directory is rejected.

## Formal Verification

The Quint harness pins Quint 0.32.0 and Apalache 0.56.1. It classifies models
as current-behavior refinements, abstract contracts, or target hardening. Read
the [formal verification guide](../formal/cluster-verification.md) before
interpreting a pass.

```sh
bash scripts/formal/check-quint.sh typecheck
bash scripts/formal/check-quint.sh fast

QUINT_CASE_FILTER=raft-kv QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage

QUINT_CASE_FILTER=raft-kv \
  bash scripts/formal/check-quint.sh verify-fast
```

Use a focused case while iterating, then the relevant tier. Record tool
versions, steps, samples, seed, timeout, witnesses, and elapsed time. Bounded
model checking is compositional evidence, not a proof of unmodeled WAL,
cryptographic, scheduling, or real-network behavior.

## Real Multi-Node Validation

The Docker harnesses include a segmented six-node development cluster plus
separate routing, migration, and single-node paths. Together they exercise
formation, routing, migration, DFS, failure, restart, rollout recovery,
mounted-filesystem compatibility, and idle-resource gates. Start with
[the Docker guide](../../docker/README.md).

Common harnesses include:

```sh
docker/test-cluster.sh
docker/benchmark-cluster-kv.sh
docker/test-routing-scenario.sh
docker/test-cluster-migration.sh
docker/test-pjdfstest.sh --smoke --report
docker/measure-final.sh 60 90
```

`docker/test-rollout-recovery.sh` additionally needs compatible baseline and
candidate Linux binaries; see the Docker guide for its inputs.

These commands can take substantial time and create containers, volumes, and
artifacts. Read their options, use a distinct Compose project where supported,
and preserve the generated failure bundle before teardown.

A passing in-process cluster test does not replace real transport, process,
disk, restart, or platform evidence for a change that affects those paths.

## Sandbox and WASIX Verification

Checking the contract marker is useful but insufficient for runtime changes:

```sh
cargo check -p sinomo-sandbox --features wasix-runtime
cargo check -p sinomo-sandbox-wasix-module --features wasix-runtime
```

Run the real verifier matching the changed toolchain:

```sh
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh
crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
```

Verifier output and generated toolchain `work-*`/`out-*` directories are not
source artifacts and should not be committed.

## Benchmarks and Experimental Evaluation

Benchmarks live under `modules/benchmark/sinomo-benchmarks/` and are opt-in.
Available surfaces include indexed-file memory and LongMemEval-S, agent channel,
compaction, and skill evaluations, SIFS/FUSE, SWE-bench, MCU/MineStudio, tau3
voice, and environment adapters. The cluster-KV benchmark is a Docker harness.
For DFS or cluster performance reports, apply the
[performance analysis skill](../../skills/sinomo-performance-analysis/SKILL.md).

Use [research and evaluation](research-and-evaluation.md) to classify the claim,
evidence, and current implementation boundary. Use
[experimental reproducibility](experimental-reproducibility.md) as the
canonical checklist for source, environment, datasets, model configuration,
commands, raw results, uncertainty, and result-bundle contents. A benchmark
README may impose stricter protocol-specific requirements.

Write run output under the gitignored `benchmark-results/` directory. Curated
claims belong in a reviewed evidence document that links or describes the
immutable result artifact; do not commit an unreviewed output dump.

## Current CI Boundary

The GitLab pipeline runs only on `main`. It runs the DFS/KV boundary lint and
builds locked release CLI artifacts for configured Linux and Windows targets
plus a Linux x86_64 desktop bundle. It does not currently run merge-request
formatting, Clippy, unit tests, the trust-ring test, formal checks, real-cluster
tests, or benchmarks.

Contributors must therefore report local validation explicitly. A future CI
gate can automate a command, but it does not change which evidence the
architecture requires.

## Validation Matrix

| Change | Minimum additional evidence |
|---|---|
| Manifest or facade | Trust-ring test and workspace check |
| KV/Raft or membership | Crate tests, cluster integration, relevant Quint refinement |
| Network routing or transport | Network tests, routed multi-node scenario, relevant Quint refinement |
| DFS metadata/content | DFS integration, DFS/KV boundary lint, restart/repair scenario, relevant formal slice |
| Windows Cloud Files | API and platform-neutral state-machine tests, Windows target `--no-run`, and focused Windows execution for CFAPI, Explorer, notification, idle-I/O, or power behavior |
| Daemon lifecycle or rollout | Daemon/rollout tests and process-level activation/recovery scenario (`docker/test-rollout-recovery.sh`) |
| Sandbox/toolchain | Actual backend build and matching verifier |
| Provider or agent protocol | Scripted-provider tests or scenarios; opt-in live smoke only when required |
| Performance optimization | Correctness gates plus before/after benchmark on a recorded environment; DFS/cluster work uses the performance analysis skill's standard tables |
| User command | Parser/help tests, domain tests, and updated user guide |
| Documentation only | `python3 scripts/docs/check_links.py` and `git diff --check` |
