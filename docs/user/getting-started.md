# Getting Started

## Graphical Installer

For a prebuilt desktop installation, use the Windows setup executable or Linux
installer AppImage from the CI `dist/installers/` artifacts. Windows registers
Sinomo in Installed Apps. On Linux, make the AppImage executable in your file
manager and open it; the installer uses native desktop dialogs and requires a
working desktop portal. No terminal setup is needed after opening the installer.

Select an empty setup folder and a separate persistent data folder. The setup
folder holds removal metadata; executable components live only in `<data>/bin`. Windows
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
release, setup preserves that release, its binaries, modules and managed source.
The complete distribution includes every default platform-compatible native module,
including evolution, and an offline source checkout. Fresh deployments receive
those defaults; retained deployments preserve their evolved module selection.
The evolution agent merges and verifies upstream changes; rollout replaces each
selected node's source with the approved snapshot. No binary auto-updater is installed.

For an older installation with executable copies in the setup folder, first
update its daemon through the approved rollout workflow. Then uninstall while
keeping data and reinstall from the complete distribution using that data folder.
This rebuilds OS integration around the canonical binaries without replacing the
evolved deployment. Setup does not perform an implicit runtime upgrade.

To uninstall, use Installed Apps on Windows or **Uninstall Sinomo** in the Linux
applications menu. Linux also provides `<data>/bin/sinomo-setup`. Data is kept by
default, including evolved binaries, native modules and source history. Permanent data deletion requires
explicit confirmation and is available only for the selected data folder bound
to this installation. Setup does not take deletion ownership from another
installation. Either the CLI or graphical uninstaller can remove either setup
type. Both remove startup/PATH/shortcut integration and stop the deployment. A persistent
removal fence prevents background recovery from reviving it. Shutdown has a bounded
grace period; stuck work or rollout cannot veto removal, which can terminate owned
processes. Explicit reinstall validates and re-enables retained deployment data.

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
# Linux x86-64 example; use x86_64-pc-windows-msvc and .exe on Windows.
cargo build --locked -p sinomo --target x86_64-unknown-linux-gnu
cargo run --locked -p sinomo-installer --bin sinomo-package -- . target/x86_64-unknown-linux-gnu/debug target/x86_64-unknown-linux-gnu/debug x86_64-unknown-linux-gnu dev
target/x86_64-unknown-linux-gnu/debug/sinomo install --add-to-path
```

`sinomo install` and the graphical installer use the same setup, verification,
configuration and removal engine. The ordinary CLI archive and graphical package
carry the same default desktop profile. Use `--headless` for a CLI-only deployment.
For a new deployment, use a complete distribution or prepare the offline payload
from a clean committed source checkout as above. Keep `initial-payload.tar` beside
the CLI. Payload and ownership checks finish before setup claims the installation
or changes deployment enablement. Installation does not compile, download, or
modify system libraries.

Setup binds executable components to `<data>/bin`, registers per-user daemon
startup and verifies main and shadow readiness before reporting success.
`--no-start` suppresses immediate daemon startup while still configuring future
startup. `--no-startup` disables boot/login startup without stopping processes
that are already running.

An existing installation is verified instead of overwritten. Running either
frontend after the other preserves evolved binaries, modules, source, data and
omitted integration settings. Healthy daemons are not restarted. Missing owned
registration or integration files are repaired; conflicting ownership is reported.
The graphical installer offers the same PATH, startup and shortcut choices as
the CLI:

```sh
sinomo install
sinomo install --add-to-path --startup --menu-shortcut
sinomo install --no-add-to-path --no-startup --no-menu-shortcut --no-desktop-shortcut
```

Use `--install-dir PATH` and `--data-dir PATH` to choose locations for a new
installation. The defaults are `%LOCALAPPDATA%/Programs/Sinomo` on Windows and
`~/.local/opt/sinomo` on Linux, with data selected by `SINOMO_DATA_DIR` or
`~/.sinomo`. Existing installation bindings are reused when those options are
omitted. Optional PATH and shortcuts are disabled on a fresh installation;
startup is enabled. Either frontend can subsequently enable or disable them.
App shortcuts require a desktop profile. Component upgrades use rollout rather
than setup.

A source-built CLI without a sibling desktop manifest installs only the CLI.
Use `--app-bundle DIRECTORY` to install a verified desktop release; `--headless`
explicitly installs only the CLI. See [desktop app rollout](rollout-operations.md#desktop-app-rollout)
for desktop bundle preparation.

Both uninstall frontends stop the same main/shadow deployment and remove its
PATH, boot/login startup, shortcuts and operating-system registration. Data is
retained by default, including binaries, managed source, modules and user state;
a durable removal fence prevents retained launchers from restarting it.
Use `--install-dir PATH` to select another installation. `--delete-data`, or the
explicit graphical deletion choice, removes the complete owned data directory
as well as installation metadata. Files unrelated to the installation are not
removed from a custom metadata folder. Reinstallation verifies and re-enables
retained components without replacing them.

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

The complete CLI distribution and its install/uninstall path do not require a
display, desktop portal, graphical installer or GPU. Required native runtime
libraries are bundled; installed perception modules do not grant device consent. A container with
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
