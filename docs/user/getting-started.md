# Getting Started

This guide builds Sinomo from the repository, installs it for the current user,
creates a one-node cluster, configures a model provider, and runs a first
interaction.

## 1. Check the Requirements

The workspace requires Rust 1.95 or newer and Git. The cross-platform CLI does
not require the graphical application's desktop libraries.

```sh
rustc --version
cargo --version
git --version
```

The graphical app additionally uses SUI/wgpu plus native audio and video
backends, so its platform development libraries must be present. Docker is
optional unless you want the containerized cluster, cross-build, or Android
workflows.

## 2. Choose the Persistent Data Directory

Sinomo uses `~/.sinomo` by default. To keep an evaluation or development
installation separate, set an absolute path before installing:

```sh
export SINOMO_DATA_DIR="$HOME/.sinomo-dev"
```

Use the same value for the installed client and daemons. Different values
create different installations with different identity, credentials, cluster
state, and files.

Do not point `SINOMO_DATA_DIR` at the repository, a temporary directory that
will disappear unexpectedly, or another machine's live data directory.

## 3. Build and Install

From the repository root:

```sh
cargo build -p sinomo
cargo run -p sinomo -- install
```

`install` is a deployment transaction. It creates or updates the user-local
installation, adds daemon startup integration, exposes the client in the user
binary directory, builds and installs the current native modules, and starts
the main and shadow daemons. On an existing installation it may first stop both
daemons, replace binaries, advance managed source from the current checkout,
rewrite boot entries, and then restart. Verify the data directory, current
repository and commit, and expected outage before running it. `--no-start`
suppresses the final start; it does not make the preceding deployment steps a
dry run.

If `sinomo` is not immediately on `PATH`, start a new shell or continue with
`cargo run -p sinomo -- <command>` from the repository.

Verify both processes:

```sh
sinomo daemon status
sinomo daemon shadow status
```

For a foreground development process instead of boot integration, use
`sinomo daemon run`. Do not run a foreground and installed main daemon against
the same data directory at the same time.

## 4. Create the One-Node Cluster

Only initialize a data directory that has no existing cluster identity:

```sh
sinomo cluster init --name workstation
sinomo cluster status
sinomo cluster health
```

If the directory was used before, run `sinomo cluster status` first. Do not
initialize a replacement identity to work around an unhealthy existing
cluster. To join another installation, follow the
[cluster operations guide](cluster-operations.md#add-a-node).

## 5. Configure a Model Provider

List supported providers and authenticate the one you intend to use:

```sh
PROVIDER=your-provider-id
sinomo providers help
sinomo providers auth "$PROVIDER"
sinomo providers test "$PROVIDER"
```

Provider authentication may use an interactive login, a provider CLI, or an
API key depending on the adapter. Relevant environment variables are listed by
`sinomo help`. Sinomo-owned credential material lives in the selected data
directory. A provider CLI or operating-system credential store may retain
additional state elsewhere; back up or revoke it through that owning system.
Never commit credentials or copy them into a shared SIFS path.

Inspect available models and choose the default:

```sh
MODEL=your-model-id
sinomo models list --provider "$PROVIDER"
sinomo models default --provider "$PROVIDER" --model "$MODEL"
sinomo models
```

Use an identifier returned by `models list`; model catalogs and account access
can vary by provider.

## 6. Run a First Interaction

Open the terminal interface:

```sh
sinomo
```

Inside the TUI, enter ordinary messages directly. Prefix a CLI command with
`/`, for example `/models list`, and use `/quit` or `/exit` to leave.

For one non-interactive agent turn:

```sh
sinomo agent run --prompt "Summarize what this workspace contains"
```

Run that command from the workspace the agent is meant to see. The built-in
coding agent rejects unsafe roots and restricts file-changing tools to the
explicit current workspace.

To open the graphical application from the repository:

```sh
cargo build -p sinomo -p sinomo-ui-extension-host -p sinomo-app
cargo run -p sinomo-app
```

The app and CLI use the same selected data directory and daemon state. If no
daemon is running, the desktop launcher starts the sibling CLI with
`sinomo daemon run --app-owned` and holds its lifetime lease across app updates.
An already-running daemon is merely attached and is not stopped when the window closes.
Custom module previews additionally use the sibling
`sinomo-ui-extension-host`; it is started only for a selected custom surface
and is terminated with that surface or the app.

## 7. Verify the Installation

The normal healthy baseline is:

```sh
sinomo daemon status
sinomo cluster health
sinomo cluster doctor
```

On a fresh one-node installation, the output should identify one authorized
main member, reachable local services, and no unexplained DFS or endpoint
errors. A command can report a warning that deserves investigation without the
entire runtime being unusable; use the [troubleshooting guide](troubleshooting.md)
to collect evidence. The canonical multi-surface checklist is the
[routine health baseline](operations.md#routine-health-baseline).

## Next Steps

- [Everyday use](everyday-use.md) covers models, agents, memory, skills, files,
  and voice.
- [Cluster operations](cluster-operations.md) adds trusted devices.
- [Operations](operations.md) covers health, observation, backups, and routine
  maintenance.
- [Rollout operations](rollout-operations.md) updates installed nodes safely.
