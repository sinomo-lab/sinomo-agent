# Getting Started

## Graphical Installer

For a prebuilt desktop installation, use the Windows setup executable or Linux
installer AppImage from the CI `dist/installers/` artifacts. Windows registers
Sinomo in Installed Apps. On Linux, make the AppImage executable in your file
manager and open it; the installer uses native desktop dialogs and requires a
working desktop portal. No terminal setup is needed after opening the installer.

Select an empty application folder and a separate persistent data folder. Windows
offers a directory entry and Browse button; Linux uses folder choosers, which can
create new folders. PATH integration and app desktop/menu shortcuts are optional.
The CLI and app use the selected data folder; an explicit `SINOMO_DATA_DIR`
environment variable takes precedence. Linux PATH integration writes a removable
block to `~/.profile` and applies to future login sessions that read that file.
Desktop environments may ask you to trust a newly created desktop shortcut.

Both setup frontends register and start the main and shadow daemons by default.
Windows uses current-user sign-in startup, without a machine-wide service or
administrator requirement. Linux registers enabled systemd user units. If no
user service manager is running, setup reports that condition and starts the
daemons directly for the current session. No GUI is needed for `sinomo install`.

Setup is for initial installation, not upstream updates. It refuses a nonempty
application destination. When reusing a data directory with an active desktop
release, setup preserves that release. Evolution and upstream source merges stay
under the cluster's normal rollout system; no binary auto-updater is installed.

To uninstall, quit the app and use Installed Apps on Windows or **Uninstall
Sinomo** in the Linux applications menu. Linux also provides `sinomo-setup` in
the application folder. Data is kept by default. Permanent data deletion requires
explicit confirmation and is available only for the selected data folder bound
to this installation. Setup does not take deletion ownership from another
installation. Either the CLI or graphical uninstaller can remove either setup
type. Both stop shadow recovery before main, remove daemon startup integration,
and remove owned app files while preserving other files added to the app folder.

## Source Installation

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
cargo run -p sinomo -- install --add-to-path
```

`sinomo install` uses the same initial-setup engine as the graphical installer.
It requires an empty application destination, binds the CLI and app to the
selected data directory, registers removal integration, and preserves an active
evolved release. It registers per-user daemon startup and verifies main and
shadow are running before reporting success. It does not import source, build
native modules, or offer a replacement upstream binary. `--no-start` registers
the deployment without starting new daemon processes.

Use `--install-dir PATH` and `--data-dir PATH` to choose locations. The defaults
are `%LOCALAPPDATA%/Programs/Sinomo` on Windows and `~/.local/opt/sinomo` on Linux,
with the data directory selected by `SINOMO_DATA_DIR` or `~/.sinomo`.
`--add-to-path`, `--desktop-shortcut` and `--menu-shortcut` opt into shell
integration. The menu option covers Start Menu on Windows and the applications
menu on Linux. App shortcuts require a verified desktop bundle.

A source-built CLI without a sibling desktop manifest installs only the CLI.
Use `--app-bundle DIRECTORY` to install a verified desktop release; `--headless`
explicitly installs only the CLI. See [desktop app rollout](rollout-operations.md#desktop-app-rollout)
for desktop bundle preparation.

`sinomo uninstall` removes an initial installation and retains data by default.
Use `--install-dir PATH` to select another installation, or add `--delete-data`
to explicitly request permanent removal of its associated data. The same path
and ownership checks apply to both graphical and command-line removal.

If `sinomo` is not immediately on `PATH`, start a new shell or continue with
`cargo run -p sinomo -- <command>` from the repository.

Daemon host/port options work with normal installation. The default service bind
host is `0.0.0.0`, with preferred ports `21353` and `21354`; supplying a port makes
it strict, while default ports permit an OS-assigned fallback.

`--runtime-service` is a legacy operator redeployment workflow for installations
outside the shared installer. It can import source, build modules and replace
daemon images. It is not needed to register services, and is rejected for data
directories owned by the shared installer; use the normal rollout workflow for
those deployments. `uninstall --runtime-service` remains available to remove
legacy startup entries, with `--no-stop` to leave legacy daemons running.

Verify both processes after installation:

```sh
sinomo daemon status
sinomo daemon shadow status
```

For a foreground development process instead of boot integration, use
`sinomo daemon run`. Do not run a foreground and installed main daemon against
the same data directory at the same time.

### Minimal Containers

The standalone CLI and its install/uninstall path do not require a display,
desktop portal, graphical installer, GPU or desktop libraries. A container with
no systemd user manager receives enabled unit files and directly started daemon
processes, with that fallback reported explicitly. As with all background
processes, they cannot outlive the container's PID 1.

For an image build, use `sinomo install --headless --no-start`. At container
runtime, run the daemon in the foreground as the container entry point, for
example `sinomo daemon run`, using the same persistent data directory. A normal
install inside an already running container starts both daemon processes.

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

`sinomo models help` lists the model provider IDs. Authenticate the one you
intend to use; local Ollama and LM Studio runtimes need no credentials:

```sh
PROVIDER=your-provider-id
sinomo models help
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
`sinomo daemon run --app-owned` and holds its lifetime lease across app
updates. An already-running daemon is merely attached and is not stopped when
the window closes. Custom module previews additionally use the sibling
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
