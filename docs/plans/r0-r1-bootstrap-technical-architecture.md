# R0/R1 Bootstrap Technical Architecture

## Purpose

This document turns the R0/R1 bootstrap direction into an implementation proposal.

The goal is to build the smallest real self-evolution core that can help Sinomo evolve and expand its own R0/R1 foundation under human supervision in a disposable VM. This is called bootstrap because it is the first phase, but the core interfaces should be general and expandable from the beginning. Production hardening, strict containment, distributed rollout, and R3 module infrastructure are deferred.

The first useful system should be able to:

```text
accept objective
  -> create evolution job
  -> generate mutation packet
  -> run a coding LLM or tool loop
  -> run verification commands
  -> collect changed files and logs
  -> write candidate bundle
  -> produce approval evidence
  -> approve, reject, or restore VM snapshot
```

## Existing Foundation

The repository already has most of the broad shapes:

- `sinomo-core`: IDs, events, trust rings, evolution jobs, scorecards, artifacts, policy decisions.
- `sinomo-runtime`: explicit R0/R1 boundary facade that aggregates only foundation crates.
- `sinomo-events`: in-memory append/replay event store.
- `sinomo-evolution`: R2 evolution service with opportunity-to-job planning and candidate workspace behavior.
- `sinomo-eval`: command scorecard skeleton.
- `sinomo-lm`: R2 HTTP and process language model providers, including Codex CLI and Claude Code style providers.
- `sinomo-policy`: policy decision skeleton.
- `sinomo-registry`: candidate and artifact record skeleton.
- `sinomo-sandbox`: sandbox run request/result skeleton.
- `sinomo-cli`: provider auth/test commands.

The bootstrap work should avoid replacing this foundation. Instead, turn the existing broad shapes into a minimal executable core. Temporary scaffolding is allowed where useful, but stable data types should be designed for the long-term self-evolution system and placed in R0/R1 crates when they fit. R2 services can depend on R0/R1, but R0/R1 crates must not depend back on R2 or application crates.

## Architecture Shape

Use three layers:

```text
R0 self-evolution contracts
  stable data structures, event names, evidence bundle schema

R1 local self-evolution services
  orchestrator, workspace adapter, command runner, tool registry, model bridge, bundle writer

CLI/API entry point
  human-facing command that runs one local supervised self-evolution iteration
```

R0 should contain minimal durable data contracts. R1 should contain trusted runtime behavior. The explicit Rust boundary is `sinomo-runtime`, which re-exports only R0/R1 crates and is checked by workspace architecture tests. The CLI should be thin.

## Design Decisions

These choices guide the first implementation:

1. Do not create a separate disposable bootstrap data model. Self-evolution is a core concept, so stable interfaces should be designed for the long-term system and moved directly into R0/R1 crates when they fit.
2. `evolve propose` must mutate a separate repository checkout or git worktree from day one. The active Sinomo instance should not build or test against files being mutated underneath it.
3. All language-model calls go through `sinomo-lm` using a common provider flag. Process coding agents such as Codex CLI or Claude Code are implementations behind the `sinomo-lm` abstraction.
4. Candidate bundles live under `SINOMO_DATA_DIR`, not inside the repository.
5. Verification commands stay shell-based during bootstrap, with exact command text, working directory, exit code, stdout, stderr, and duration captured as evidence.

## R0 Minimal Interfaces

R0 should define the shared vocabulary needed to understand and resume a self-evolution attempt. Types that are stable enough should live in `sinomo-core`; scaffolding and orchestration-only helpers can remain in `sinomo-evolution`.

### `EvolutionJob`

Existing `EvolutionJob` is useful but should be expanded toward the real minimal core instead of wrapped by a separate bootstrap-only model. The stable shape belongs in `sinomo-core`; `sinomo-evolution` can provide builders and orchestration helpers.

Proposed shape:

```rust
pub struct EvolutionJob {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    pub edit_scope: EditScope,
    pub verification: VerificationPlan,
    pub recovery: RecoveryPlan,
    pub status: EvolutionJobStatus,
}
```

### `EditScope`

Scope is a focus aid during bootstrap, not a hardened security boundary.

```rust
pub struct EditScope {
    pub workspace_root: String,
    pub intended_paths: Vec<String>,
    pub notes: String,
}
```

Use `intended_paths` for the first interface because scope is an experiment focus aid during the supervised VM phase. Long term, the same type can grow stricter enforcement fields without changing the job model.

### `VerificationPlan`

```rust
pub struct VerificationPlan {
    pub commands: Vec<VerificationCommand>,
}

pub struct VerificationCommand {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    pub gate: EvaluationGate,
}
```

The first implementation can run commands through PowerShell on Windows and ordinary process execution elsewhere. Later, command parsing can become structured.

### `RecoveryPlan`

Recovery is explicit because the VM snapshot is the real bootstrap safety net.

```rust
pub struct RecoveryPlan {
    pub strategy: RecoveryStrategy,
    pub note: String,
}

pub enum RecoveryStrategy {
    GitRevert,
    GitBranchReset,
    VmSnapshotRestore,
    ManualRepair,
}
```

### `EvolutionJobStatus`

Use statuses that match the lab loop:

```rust
pub enum EvolutionJobStatus {
    Created,
    PacketWritten,
    MutationStarted,
    MutationFinished,
    Verified,
    AwaitingApproval,
    Approved,
    Rejected,
    Revised,
    Restored,
    Failed,
}
```

These can extend or replace the current broader `EvolutionJobStatus` variants. The important part is to represent the local self-evolution loop directly instead of maintaining a parallel bootstrap state machine.

### `MutationPacket`

This is the main R0/R1 interface between Sinomo and a coding model.

```rust
pub struct MutationPacket {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    pub edit_scope: EditScope,
    pub relevant_files: Vec<FileExcerpt>,
    pub verification: VerificationPlan,
    pub output_contract: MutationOutputContract,
    pub recovery: RecoveryPlan,
}

pub struct FileExcerpt {
    pub path: String,
    pub content: String,
}

pub struct MutationOutputContract {
    pub expected_sections: Vec<String>,
    pub prefer_patch: bool,
    pub notes: String,
}
```

For the first version, file excerpts can be empty or manually selected. Automatic context collection comes later.

### `CandidateBundleManifest`

Every run writes a local bundle:

```rust
pub struct CandidateBundleManifest {
    pub job_id: EvolutionJobId,
    pub bundle_path: String,
    pub created_at: OffsetDateTime,
    pub mutation_packet_path: String,
    pub changed_files_path: Option<String>,
    pub diff_path: Option<String>,
    pub scorecard_path: Option<String>,
    pub command_logs: Vec<CommandLogRef>,
}

pub struct CommandLogRef {
    pub name: String,
    pub command: String,
    pub log_path: String,
    pub exit_code: Option<i32>,
}
```

This should live in `sinomo-core` if it remains a pure data contract. File-writing behavior belongs in `sinomo-evolution` or a local storage crate.

## R1 Minimal Services

R1 is the executable local bootstrap layer.

### 1. `EvolutionOrchestrator`

Crate: `sinomo-evolution`

Main responsibility: run one local self-evolution iteration.

Proposed API:

```rust
pub struct EvolutionOrchestrator<W, M, E, B> {
    pub workspace: W,
    pub model: M,
    pub evaluator: E,
    pub bundles: B,
}

impl<W, M, E, B> EvolutionOrchestrator<W, M, E, B> {
    pub fn propose(&self, request: EvolutionRequest)
        -> Result<EvolutionReport, EvolutionError>;
}
```

Request:

```rust
pub struct EvolutionRequest {
    pub objective: String,
    pub target_ring: TrustRing,
    pub intended_paths: Vec<String>,
    pub verification_commands: Vec<VerificationCommand>,
    pub model_provider: Option<String>,
    pub dry_run: bool,
}
```

Report:

```rust
pub struct EvolutionReport {
    pub job: EvolutionJob,
    pub bundle: CandidateBundleManifest,
    pub scorecard: Option<EvaluationScorecard>,
    pub changed_files: Vec<String>,
}
```

### 2. `WorkspaceAdapter`

Crate: `sinomo-evolution` initially. It can move to `sinomo-dfs` later.

Responsibilities:

- find source workspace root
- create a candidate repository checkout or git worktree
- collect changed files
- produce diff
- report current git status

Proposed trait:

```rust
pub trait WorkspaceAdapter {
    fn root(&self) -> &Path;
    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError>;
    fn changed_files(&self, workspace: &CandidateWorkspace) -> Result<Vec<String>, WorkspaceError>;
    fn diff(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
    fn status(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
}
```

First implementation:

- `GitWorkspaceAdapter`
- create a separate git worktree or cloned checkout from day one
- candidate path should live outside the active runtime checkout
- candidate metadata should record source revision and candidate branch/worktree path

### 3. `CommandRunner`

Crate: `sinomo-eval`

Current `LocalEvalHarness` accepts observed exit codes. The bootstrap needs actual command execution.

Proposed trait:

```rust
pub trait CommandRunner {
    fn run(&self, command: &VerificationCommand, cwd: &Path) -> CommandRunResult;
}

pub struct CommandRunResult {
    pub name: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
}
```

Then add:

```rust
pub struct ExecutingEvalHarness<R> {
    pub runner: R,
}

impl<R: CommandRunner> ExecutingEvalHarness<R> {
    pub fn run_verification_plan(
        &self,
        plan: VerificationPlan,
        cwd: &Path,
    ) -> Result<(EvaluationScorecard, Vec<CommandRunResult>), EvalError>;
}
```

First commands:

- `cargo fmt --check`
- `cargo test -p sinomo-evolution`
- `cargo test`

### 4. `EvolutionToolRegistry`

Crate: `sinomo-evolution` or new crate `sinomo-tools` later.

The first version does not need a full tool protocol. It needs a typed registry so Sinomo can observe and improve tool-use behavior.

Minimal tool descriptor:

```rust
pub struct EvolutionToolDescriptor {
    pub name: String,
    pub input_schema_json: serde_json::Value,
    pub output_schema_json: serde_json::Value,
    pub effects: Vec<ToolEffect>,
}

pub enum ToolEffect {
    ReadFile,
    WriteFile,
    RunCommand,
    ModelCall,
}
```

Minimal tools:

- `read_file`
- `search`
- `apply_patch`
- `run_command`

The concrete runtime can add command-session or interrupt/kill controls for long-running commands. `request_user_input` is an interactive-session bridge, not a default autonomous evolution tool. Milestone 1 can document these descriptors without exposing them to the model. Milestone 2 can include them in the mutation packet.

### 5. `ModelMutationBridge`

Crate: `sinomo-evolution`, using `sinomo-lm`.

Responsibilities:

- render `MutationPacket` into a prompt
- call selected `LanguageModelProvider`
- save raw model output
- parse structured mutation output when available
- run model-requested tool calls through `EvolutionToolRegistry`
- feed tool results back into later model turns
- apply the final returned patch through `apply_patch` for non-agent providers
- record raw output, parsed output, model turns, and tool results

Proposed trait:

```rust
pub trait MutationModel {
    fn mutate(&self, packet: &MutationPacket, workspace: &CandidateWorkspace) -> Result<ModelMutationResult, MutationModelError>;
}

pub struct ModelMutationResult {
    pub provider: String,
    pub model: String,
    pub raw_output: String,
    pub parsed_patch: Option<String>,
    pub summary: Option<String>,
    pub structured_output: Option<StructuredMutationOutput>,
    pub tool_results: Vec<EvolutionToolResult>,
    pub model_turns: Vec<MutationModelTurn>,
}

pub struct StructuredMutationOutput {
    pub tool_calls: Vec<EvolutionToolCall>,
    pub changed_files: Vec<String>,
    pub patch: Option<String>,
    pub patch_summary: Option<String>,
    pub verification_notes: Option<String>,
    pub experiment_notes: Option<String>,
    pub recovery_notes: Option<String>,
}
```

First provider path:

- all model calls are routed through `sinomo-lm`
- the CLI selects the provider through a common provider flag
- direct HTTP/API providers can request Sinomo tools, inspect tool results, and return a final patch
- process coding-agent providers can perform real code editing in the candidate worktree
- target ring is carried as metadata but ignored by the bridge until VM/ring separation exists

Important design choice:

Process coding agents such as Codex CLI or Claude Code should still be hidden behind the `sinomo-lm` provider abstraction. The bundle records what changed afterward using git diff from the candidate worktree.

### 6. `CandidateBundleStore`

Crate: `sinomo-evolution`

Responsibilities:

- create candidate bundles inside per-job folders under `SINOMO_DATA_DIR`
- write JSON artifacts
- write command logs
- write diff
- write durable evidence artifacts for later approval, replay, and learning

Proposed trait:

```rust
pub trait CandidateBundleStore {
    fn create(&self, job: &EvolutionJob) -> Result<CandidateBundleManifest, BundleError>;
    fn write_json<T: Serialize>(&self, manifest: &CandidateBundleManifest, name: &str, value: &T) -> Result<(), BundleError>;
    fn write_text(&self, manifest: &CandidateBundleManifest, name: &str, value: &str) -> Result<(), BundleError>;
}
```

Bundle layout:

```text
%SINOMO_DATA_DIR%/evolution/jobs/evojob_x/
  manifest.json
  job.json
  record.json
  mutation-packet.json
  model-output.txt
  changed-files.json
  diff.patch
  scorecard.json
  approval-decision.json
  command-logs/
    cargo-test.txt
  worktree/
```

On Unix-like systems, the same layout is:

```text
$SINOMO_DATA_DIR/evolution/jobs/evojob_x/
```

## Event Families

Use the existing open namespaced event model.

Self-evolution bootstrap events:

- `bootstrap.evolution.job.created`
- `bootstrap.evolution.packet.written`
- `bootstrap.evolution.mutation.started`
- `bootstrap.evolution.mutation.finished`
- `bootstrap.evolution.verification.started`
- `bootstrap.evolution.verification.finished`
- `bootstrap.evolution.bundle.written`
- `bootstrap.evolution.awaiting_approval`
- `bootstrap.evolution.approved`
- `bootstrap.evolution.rejected`
- `bootstrap.evolution.restored`

Payloads should include `job_id`, `bundle_path`, `objective`, and relevant artifact paths. The first implementation can append to an in-memory event store and write bundle files; durable event storage can come later.

## CLI Workflow

The CLI is the first minimal user interface for the self-evolution core. It should feel like a small steering wheel, not an operator cockpit.

After model setup, the minimal loop should be:

```powershell
sinomo evolve propose "implement evolve propose"

# human inspects the candidate in a normal editor or git tool

sinomo evolve approve
```

Or, if the candidate is bad:

```powershell
sinomo evolve reject
```

Or, if the candidate requires snapshot recovery:

```powershell
sinomo evolve restore --note "restored VM snapshot before retrying"
```

If there is exactly one active evolution job, `--job` can be omitted. If multiple jobs exist, `approve`, `reject`, and `restore` require `--job`.

### CLI Principles

- The human provides a natural-language prompt, not a structured job form.
- Sinomo creates the job, worktree, bundle, verification plan, and evidence records automatically.
- Verification is automatic. The minimal first verifier can be only `cargo check` or `cargo test --no-run`.
- Human supervision happens outside the CLI, using normal editor, git, and diff tools.
- The human-facing decision is only `approve` or `reject`.
- `restore` records the recovery decision when a candidate requires VM snapshot or manual repair.
- The CLI still preserves the job model. Advanced commands can inspect and operate on jobs explicitly.

### Active Job Resolution

Most commands that operate on an existing job accept `--job <evojob_id>`, but the flag should be optional when Sinomo can infer the active job.

Resolution rule:

1. If `--job` is provided, use that job.
2. If exactly one active job is awaiting approval, use it.
3. If exactly one active job exists in any non-terminal state, use it for read-only commands such as `status` and `show`.
4. If zero jobs match, print a short message saying there is no active job.
5. If multiple jobs match, print their IDs and ask the user to rerun with `--job`.

Terminal states:

- `Approved`
- `Rejected`
- `Restored`
- `Failed`

This keeps the normal loop tiny:

```powershell
sinomo evolve propose "implement evolve propose"
sinomo evolve approve
```

while preserving explicit job control for parallel or historical work:

```powershell
sinomo evolve approve --job evojob_...
```

### Default Paths

- data directory: `SINOMO_DATA_DIR`, falling back to the existing `sinomo-local` default.
- source repo: current working directory unless `--repo <path>` is provided.
- job folders: `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/`.
- candidate worktrees: `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/worktree/`.
- candidate bundle artifacts: `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/`.
- job records: `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/record.json`.

Every command should print the job ID, candidate worktree path, bundle path, and current state.

### `evolve propose "<prompt>"`

Runs one evolution attempt.

Example:

```powershell
sinomo evolve propose "implement evolve propose"
```

Slightly more explicit example:

```powershell
sinomo evolve propose "add real command execution to sinomo-eval" --provider codex-cli
```

Minimal flags:

- `--provider <provider>` chooses a `sinomo-lm` provider.
- `--model <model>` optionally overrides the provider default.
- `--repo <path>` overrides the configured source repo.
- `--verify <command>` overrides the default verification command.

Default behavior:

1. Create an `EvolutionJob` from the prompt.
2. Initialize the evolution data layout if needed.
3. Create a per-job folder under `SINOMO_DATA_DIR/evolution/jobs/<job_id>/`.
4. Create a separate git worktree at `SINOMO_DATA_DIR/evolution/jobs/<job_id>/worktree/`.
5. Write a mutation packet into the candidate bundle.
6. Invoke the selected model provider through `sinomo-lm`.
7. Run verification automatically in the candidate worktree.
8. Write the candidate bundle and job record.
9. Mark the job as awaiting approval.
10. Print where the human can inspect the worktree and bundle.

Minimal verification:

```powershell
cargo check
```

or:

```powershell
cargo test --no-run
```

The default can start with `cargo check` because the goal of the first verifier is simply to catch obvious compile failures.

Output:

```text
job: evojob_...
status: awaiting-approval
worktree: C:\Users\IX\.sinomo\evolution\jobs\evojob_...\worktree
bundle: C:\Users\IX\.sinomo\evolution\jobs\evojob_...
verification: passed
next: inspect the worktree, then run `sinomo evolve approve` or `sinomo evolve reject`
```

### `evolve approve`

Approves the active candidate.

Example:

```powershell
sinomo evolve approve
```

With multiple active jobs:

```powershell
sinomo evolve approve --job evojob_...
```

First implementation:

- mark the job as approved
- record `approval-decision.json` in the bundle
- print the candidate worktree path

Whether `approve` also merges the candidate into the source repo can be decided later. The minimal first version can leave merging manual because the human may prefer to inspect and apply the code with external tools.

### `evolve reject`

Rejects the active candidate and preserves the bundle as learning material.

Example:

```powershell
sinomo evolve reject
```

With multiple active jobs:

```powershell
sinomo evolve reject --job evojob_...
```

First implementation:

- mark the job as rejected
- record `approval-decision.json` in the bundle
- keep the bundle
- optionally leave or delete the worktree depending on a later cleanup policy

### `evolve restore`

Records that the active candidate requires recovery.

Example:

```powershell
sinomo evolve restore --note "restored VM snapshot before retrying"
```

First implementation:

- mark the job as restored
- record `approval-decision.json` with recovery strategy and note
- keep the bundle as learning material
- leave actual VM snapshot restore or manual repair to the supervised bootstrap environment

### Advanced Job Commands

The minimal interface is `propose`, `approve`, and `reject`, with `restore` available for recovery documentation. The architecture should keep the richer job workflow available.

These commands should support the active job resolution rule, so `--job` is only needed when there is ambiguity.

#### `evolve job create`

Creates an `EvolutionJob` without invoking the model.

Example:

```powershell
sinomo evolve job create "add real command execution to sinomo-eval"
```

Useful when the human or Sinomo wants to prepare a job before running it.

#### `evolve packet`

Writes or prints the mutation packet for a job.

Example:

```powershell
sinomo evolve packet
```

With an explicit job:

```powershell
sinomo evolve packet --job evojob_...
```

Useful for prompt inspection before model execution.

#### `evolve verify`

Runs or reruns automatic verification for the active candidate.

Example:

```powershell
sinomo evolve verify
```

Useful after the human manually edits the candidate worktree.

#### `evolve status`

Shows current evolution jobs and their latest state.

Examples:

```powershell
sinomo evolve status
sinomo evolve status --job evojob_...
```

Output should include:

- job ID
- status
- prompt/objective
- candidate worktree path
- bundle path
- verification outcome
- approval state

#### `evolve show`

Displays a specific artifact from the active candidate bundle.

Examples:

```powershell
sinomo evolve show --diff
sinomo evolve show --scorecard
sinomo evolve show --decision
sinomo evolve show --packet
```

With an explicit job:

```powershell
sinomo evolve show --job evojob_... --diff
```

This is optional because the human can inspect the worktree directly, but it is useful when operating from a terminal.

#### `evolve cleanup`

Removes a candidate job folder and its worktree.

Example:

```powershell
sinomo evolve cleanup --job evojob_...
```

Cleanup is explicit because it deletes the local job record, bundle artifacts, and candidate worktree. Failed candidates can still be kept as learning material by leaving the job folder in place.

## CLI Output Contract

Human-readable output is enough for the first version. Keep it short:

```text
job: evojob_...
status: awaiting-approval
worktree: C:\Users\IX\.sinomo\evolution\jobs\evojob_...\worktree
bundle: C:\Users\IX\.sinomo\evolution\jobs\evojob_...
verification: passed
```

JSON output can be added later when the CLI itself becomes an agent-facing interface.

## First Operator Loop

The first real bootstrapping session should feel like this:

```powershell
sinomo evolve propose "implement evolve propose" --repo "D:\Workspace\sinomo-agent"

# inspect generated candidate worktree externally

sinomo evolve approve
```

If the result is not good:

```powershell
sinomo evolve reject
sinomo evolve propose "try a smaller implementation of evolve propose"
```

This keeps the human loop tiny while Sinomo owns the repetitive mechanics: job creation, worktree setup, model invocation, compile check, bundle writing, and decision recording.

## Coding Plan

### Step 1: Expand Core Self-Evolution Data Types

Files:

- `crates/sinomo-core/src/domain.rs`
- `crates/sinomo-evolution/src/lib.rs`

Add:

- durable fields to `EvolutionJob`
- `EvolutionRequest`
- `EvolutionReport`
- `EditScope`
- `VerificationPlan`
- `VerificationCommand`
- `RecoveryPlan`
- `RecoveryStrategy`
- local-loop `EvolutionJobStatus` variants
- `MutationPacket`
- `FileExcerpt`
- `MutationOutputContract`
- `CandidateBundleManifest`
- `CommandLogRef`

Tests:

- job serializes to readable JSON
- request creates job with target ring and intended paths
- mutation packet includes verification commands

### Step 2: Add Candidate Bundle Store

Files:

- `crates/sinomo-evolution/src/lib.rs`

Add:

- `CandidateBundleStore` trait
- `LocalCandidateBundleStore`

Behavior:

- writes `$SINOMO_DATA_DIR/evolution/jobs/<job_id>/`
- writes pretty JSON
- writes text files
- returns manifest paths

Tests:

- creates bundle directory under temp path
- writes `job.json` and `mutation-packet.json`
- manifest references written files

### Step 3: Add Real Command Runner

Files:

- `crates/sinomo-eval/src/lib.rs`

Add:

- `CommandRunner` trait
- `LocalCommandRunner`
- `CommandRunResult`
- `ExecutingEvalHarness`

Behavior:

- run commands in workspace root
- capture stdout/stderr
- record duration
- create `EvaluationScorecard`

Tests:

- `exit 0` command passes
- nonzero command fails
- stdout/stderr are captured

On Windows, tests should use PowerShell-compatible commands.

### Step 4: Add Workspace Adapter

Files:

- `crates/sinomo-evolution/src/lib.rs`

Add:

- `WorkspaceAdapter` trait
- `GitWorkspaceAdapter`
- `CandidateWorkspace`

Behavior:

- create candidate worktree or separate checkout
- `git status --short`
- `git diff --binary`
- changed file extraction from `git status --porcelain`

Tests:

- parsing `git status --porcelain` yields paths
- adapter can be unit-tested with fixture strings before invoking git

### Step 5: Add Mutation Packet Renderer

Files:

- `crates/sinomo-evolution/src/lib.rs`

Add:

- `MutationPacketRenderer`
- `render_mutation_prompt(packet: &MutationPacket) -> String`

Prompt should include:

- objective
- intended paths
- verification commands
- recovery note
- requested output shape

Tests:

- prompt contains objective
- prompt contains every verification command
- prompt asks for changed files, verification notes, and recovery notes

### Step 6: Add Model Bridge

Files:

- `crates/sinomo-evolution/src/lib.rs`
- maybe `crates/sinomo-cli/src/lib.rs`

Add:

- `MutationModel` trait
- `LanguageModelMutationBridge<P: LanguageModelProvider>`

Behavior:

- converts mutation packet into `LanguageModelRequest`
- invokes the selected provider through `sinomo-lm`
- stores raw model output in bundle
- does not require structured parsing in first version

Tests:

- fake provider receives rendered prompt
- raw output is returned and written

### Step 7: Add Orchestrator

Files:

- `crates/sinomo-evolution/src/lib.rs`

Add:

- `EvolutionOrchestrator`
- `EvolutionError`

First orchestration path:

```text
request
  -> job
  -> packet
  -> bundle
  -> candidate worktree
  -> optional model mutation
  -> git diff/status
  -> command eval
  -> approval evidence
  -> report
```

Tests:

- dry run writes packet and bundle without model call
- verification-only run creates scorecard
- fake model output is captured

### Step 8: Add CLI Entry Point

Files:

- `crates/sinomo-cli/src/lib.rs`

Add:

- parse `evolve propose`
- parse `evolve approve`
- parse `evolve reject`
- parse `evolve restore`
- implement active job resolution for omitted `--job`
- parse advanced commands after the minimal loop works:
  - `evolve job create`
  - `evolve packet`
  - `evolve verify`
  - `evolve status`
  - `evolve show`
  - `evolve cleanup`
- print bundle path and scorecard summary

Tests:

- parser accepts `evolve propose "<prompt>"`
- parser accepts optional `--provider`, `--model`, `--repo`, and `--verify`
- parser accepts `evolve approve`, `evolve reject`, and `evolve restore` with optional `--job`
- active job resolution selects the only job awaiting approval
- active job resolution reports ambiguity when multiple jobs are awaiting approval
- parser accepts advanced job commands
- usage includes evolve commands

### Step 9: First Self-Improvement Trial

Use the new CLI to improve a small part of the bootstrap harness.

First target:

```powershell
sinomo evolve propose "improve command evaluation scorecard details"
```

Expected result:

- candidate bundle written
- changed files captured
- tests run
- approval evidence written
- human decides approve/reject/restore

## Minimal Milestone Cut

If we want the smallest possible first implementation, do only:

1. Core self-evolution data types.
2. Candidate bundle store.
3. Real command runner.
4. Git worktree candidate workspace.
5. `evolve propose "<prompt>"` that initializes the job folder, invokes the model, writes the bundle, and runs minimal verification.
6. `evolve approve`.
7. `evolve reject`.
8. `evolve restore`.
9. Active job resolution so `approve`, `reject`, and `restore` can omit `--job`.

This gives Sinomo the smallest model-driven local evolution loop.

After the smallest loop works, add:

1. `evolve status`
2. `evolve show`
3. `evolve verify`
4. `evolve job create`
5. `evolve packet`
6. `evolve cleanup`

## Settled Design Questions

1. Stable self-evolution types should go into `sinomo-core` when they fit. Avoid a separate bootstrap-only data model.
2. `evolve propose` should use a separate repository checkout or git worktree from day one.
3. Model calls should use a common provider flag and route through `sinomo-lm`.
4. Candidate bundles should live under `SINOMO_DATA_DIR`.
5. Verification commands should remain shell-based during bootstrap.

## Success Criteria

The architecture is working when Sinomo can run this loop against its own R1 code:

```text
objective -> packet -> model/tool attempt -> tests -> bundle -> approval decision -> next iteration
```

The first version does not need to be autonomous. It needs to make each iteration clear enough that the human and Sinomo can jointly decide what to try next.
