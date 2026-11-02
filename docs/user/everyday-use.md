# Everyday Use

This guide introduces the main user-facing surfaces after the daemon, cluster,
and provider are configured. It is a map, not an exhaustive command reference;
use `sinomo <command> help` for the installed version.

## Choose an Interaction Surface

| Surface | Best for | Start command |
|---|---|---|
| Terminal UI | Conversation plus occasional CLI commands | `sinomo` |
| Graphical app | Chat, SIFS files, cluster state, and settings | `sinomo-app` or `cargo run -p sinomo-app` |
| One-shot agent | Scripts, focused tasks, and reproducible prompts | `sinomo agent run --prompt <text>` |
| Direct CLI | Configuration, inspection, automation, and operations | `sinomo <command>` |

The TUI connects to the local daemon when available. Within it, `/models list`
is a CLI command while text without `/` is a user message.

Use F1–F4 or click the tabs to switch between Chat, Files, Sessions, and
Settings. Tabs shorten automatically in narrow terminals. The chat footer shows
shortcuts for the current action: Enter sends a message, Tab or Enter accepts a
completion, and Ctrl+C interrupts (pauses) active work. Use Page Up/Page Down
to scroll and Alt+S to select transcript text. Esc clears a chat selection or
cancels an edit first; otherwise it exits chat or returns from another view to
chat.

The `sinomo-app` launcher comes from a desktop release: `sinomo install` run
from that release, or with `--desktop --app-bundle`, places it in
the chosen application directory. An existing active release in the selected
data directory is preserved. A source-checkout or CLI-only install is headless.
See [desktop app rollout](rollout-operations.md#desktop-app-rollout).

## Settings

Graphical Settings groups its pages by task:

- **General**: **Appearance** and **System** (connection status, app version,
  and diagnostics).
- **AI**: **Providers**, **Agents**, and **Skills**.
- **This device**: **Storage** (SIFS host mounts, natively or over NFS) and
  **Host access**.
- **Cluster**: **Membership** and **Modules**.
- **Advanced**: **Configuration**, a browser for every setting key with a
  **Cluster** or **This device** scope.

Storage, Host access, Modules, and device-scoped Configuration have a **Node**
selector that chooses the node being managed.

Ordinary remote configuration requires cluster configuration authority, without
granting remote control of the host system. Host filesystem mount changes still
require remote-management consent on the selected node. Grant or revoke host
access locally on that node; selecting it in Settings does not grant permission.

**Host access → Device sharing** controls access to this node's default
microphone, speaker, camera, and motion/environment sensors. Change these switches on the
device that owns the hardware. Sharing is separate from unrestricted remote
management and is off by default. Other authenticated cluster nodes still need
the matching media capability. Turning sharing off revokes access; it does not
revoke Android's permission for local calls. Sensor capture depends on a platform
adapter; Android supplies motion, rotation, magnetic field, light, and pressure
readings where the hardware is available.

On Android, **Set up App Use** opens the local display-sharing and accessibility
controls. App Use also requires its packaged OS provider and exact-share consent;
see [App Use setup](computer-and-app-use.md#installation-and-local-consent).

## Models and Providers

Show the current default, list models, and change the selection:

```sh
PROVIDER=your-provider-id
MODEL=your-model-id
sinomo models
sinomo models list --provider "$PROVIDER"
sinomo models default --provider "$PROVIDER" --model "$MODEL"
```

Provider-specific reasoning, output, account, and vendor options are explicit:

```sh
sinomo models options show --provider "$PROVIDER"
sinomo models options set --provider "$PROVIDER" --reasoning-effort high
```

Codex model discovery includes GPT-6.1 Sol, GPT-6 Astra/Sol/Luna, and
GPT-5.6 Sol, Terra, and Luna when
available to your signed-in account. Astra, Sol, and Terra accept `max` and
`ultra` reasoning; Luna accepts up to `max`. Set options per model:

```sh
sinomo models list --provider codex
sinomo models options set --provider codex --model gpt-6-astra --reasoning-effort ultra --vendor-option service_tier=priority
sinomo models options set --provider codex --model gpt-6.1-sol --reasoning-effort max --vendor-option service_tier=priority
```

Service tiers affect account usage. Use `service_tier=default` to return to the
default tier. In the direct provider, `ultra` selects Codex's underlying model
effort (normally `max`); Sinomo controls agent delegation. See
[Codex provider compatibility](../developer/provider-compatibility.md#codex)
for the source comparison and supported request formats.

The other providers also discover models from their configured endpoints.
OpenAI, Codex, and direct GitHub Copilot now default to `gpt-6.1-sol`;
Anthropic/Claude to `claude-sonnet-5-5`; Gemini to `gemini-3.8-flash`;
and DeepSeek to `deepseek-flash`. Explicit account/model selections are retained.
Claude 5.5 models reject forced tool selection. Sonnet's lowest thinking setting
is `between_tools`; Opus always uses adaptive thinking.
Native tool calls and their continuation state are supported across the HTTP
providers. See [provider compatibility](../developer/provider-compatibility.md)
for reasoning controls, current defaults, and protocol details.

Native model runtime locations and endpoints are cluster-scoped settings. Read
their current values before changing them:

```sh
sinomo models runtime
sinomo models help
```

Do not place provider credentials in cluster KV. Authenticate on the node that
will use the provider with `sinomo providers auth <provider>`.

The graphical app's **Settings > Providers** page shows each account's API and
tool capabilities, including Responses, realtime voice, hosted search/image
tools, MCP, and local model discovery. Ollama and LM Studio can be added there
without credentials; their endpoint and model remain editable, and connection
testing discovers the locally served model catalog.

## Agents and Durable Sessions

Run the built-in agent or select an imported package:

```sh
sinomo agent run --prompt "Inspect the project and explain its entry points"
AGENT_ID=your-agent-id
sinomo agent run --agent "$AGENT_ID" --prompt "Continue the analysis"
```

Use `--provider`, `--model`, or `--account` for an explicit per-run selection.
Use `--session <session-id>` to continue a persisted session when the package
and provider support the intended workflow.

Inspect packages and readiness before relying on a custom agent:

```sh
sinomo agent inspect
sinomo agent validate
sinomo agent readiness
```

In the graphical app, open **Settings > Agents** to manage the same package
surface. The tool picker includes provider-backed web search and image
generation, and lists whether every packaged tool works in chat, realtime
voice, or both. Its **External integrations** area manages per-agent Streamable
HTTP MCP servers. Each server has a **Protocol** setting that defaults to
detecting whether the endpoint speaks the stateless MCP revision (2026-07-28) or
the older `initialize` handshake, and can be pinned to either when an endpoint
mishandles the probe. MCP credentials are environment-variable references rather
than stored secret values. Granting or revoking the required node-local
`external-integrations` permission is an explicit, confirmed action in the same
area; after saving an agent, **Test readiness** connects to enabled servers and
reports the negotiated protocol version, the server that answered, and the
discovered tool names. Realtime voice can also hand coding, research, or longer
work to the same regular agent and bring the result back into the live
conversation.

For a coding task in **Chat**, the default **main-agent** can use the complete
coding toolset; **coding-agent** remains available as a dedicated workspace
persona. Open the **Workspace** control in the composer and use **Add folder**
to grant the conversation a project directory. A conversation can hold several
folders: commands start in the default (first) folder, **Make default** changes
it, each folder's close button removes it, and **Remove all** revokes every
grant. Workspaces cannot change while a turn is active. Choose a model, then
describe the implementation and validation outcome you want. File reads,
edits, patches, and audited command execution use the granted folders as their
roots, while tool calls and captured command output appear in the
conversation's **Tool activity** expander.

The main agent can also inspect and update registered Sinomo configuration,
read cluster health, and use available module, integration, and remote-node
tools. Operations that affect another node still require that node's explicit
local permission; selecting the main agent does not bypass those checks.

A host-local folder remains on the node that owns it and is not implicitly
copied into SIFS. A workspace selected from a live SIFS filesystem mount uses
that mount directly, so changes synchronize continuously without a copy-back
or checkpoint phase. Dependency directories such as `node_modules` and Python
virtual environments therefore stay local unless the workspace itself is a
SIFS-mounted directory.

An agent can start temporary subagents for parallel or focused subtasks. They
inherit the parent's model, tools, workspace, and permissions, cannot be given
more, are cancelled with their parent, and do not survive a process restart.

A chat runs on one node at a time. Other windows and nodes that open it share
the same conversation and turn; if the hosting node stops, another node takes
the chat over after about 45 seconds and resumes from the last saved session.
Deleting a chat cancels its live work and releases its automatic sandbox; its
stored content is purged 30 days later.

Live sessions expose explicit control commands:

```sh
SESSION_ID=your-session-id
sinomo agent session inspect "$SESSION_ID"
sinomo agent session progress "$SESSION_ID"
sinomo agent session steer "$SESSION_ID" "Recheck the failing test first"
sinomo agent session interrupt "$SESSION_ID" "Wait for my review"
sinomo agent session unpause "$SESSION_ID"
sinomo agent session cancel "$SESSION_ID" "The task is no longer needed"
```

Steering and follow-up queue input for a live runtime. Interrupt pauses the
session: no new model request starts, running tools and subagents finish and
keep their results, and the next message (or `unpause`) continues the work.
Cancel stops the turn now, including running tools and processes, and may lose
unfinished work. In the app, Esc or **Interrupt** pauses and **Cancel
response** stops immediately. Neither erases already committed files or
external effects.

## Indexed-File Memory

Register a directory of Markdown or text sources, build an index generation,
and retrieve evidence:

```sh
sinomo memory root add /absolute/path/to/notes
# Alternatively, select an existing SIFS directory through a running daemon.
sinomo memory root add /services/shared-notes --filesystem sifs
sinomo memory root list
ROOT_ID=your-root-id
sinomo memory index "$ROOT_ID"
sinomo memory query "where did we decide the retention policy?"
```

The source files remain the durable evidence. Indexes are derived and can be
rebuilt. SIFS source files are shared through DFS; root registrations and derived
indexes belong to the node running these commands. Register and index the source
on each node that needs indexed retrieval. Run `memory index` again to reconcile
source changes and deletions; queries recheck the source version and do not serve
changed or unavailable source content as current. Agent memory-note tools do not
automatically register or index their SIFS home.
Joining a different cluster does not rebind an existing SIFS index root;
register the intended source in the new cluster explicitly.

Keep secrets out of a registered root unless its storage and scope
policy are appropriate for those secrets.

## Skills

Sinomo uses the open
[Agent Skills format](https://agentskills.io/home). A skill is a directory with
a required `SKILL.md` and optional `scripts/`, `references/`, `assets/`, or
other resources:

```sh
SKILL_ID=your-skill-id
SCOPE=your-scope
sinomo skill list
sinomo skill import /path/to/skill-name
sinomo skill show "$SKILL_ID" --text
sinomo skill activate "$SKILL_ID" --scope "$SCOPE"
sinomo skill export "$SKILL_ID" --output /path/to/exports
```

Inspect imported instructions and resources before activation. A skill can
guide an agent but cannot grant host, network, filesystem, device, secret, or
rollout permissions. In particular, the experimental `allowed-tools`
frontmatter field does not override Sinomo authority checks.

## Shared Files and SIFS

The graphical app exposes the SIFS namespace in **Files**. Use **Locations** to
move between Sinomo and this device, then use the visible **New**, **Import**,
and item-action controls for everyday work. Copy, move, and download dialogs
offer common Sinomo destinations or the native folder picker, while retaining
an editable exact path for less common destinations.

Use **File view** in the toolbar to choose **Details**, **Small icons**, **Medium
icons**, **Large icons**, or **Flow**. Flow arranges image previews in variable-height
columns, like Pinterest; folders and other files use compact icon cards. JPEG, PNG,
and WebP previews load as you scroll. Selection, marked files, sorting, and file
actions work across views. Single-click to select and double-click to open; use the
arrow keys to move between cards, Space to mark an item, and Enter to open it.

Select a Sinomo file or folder to manage its availability. The primary choices
are to inherit the folder default, keep copies available across devices, or
wait for every configured copy before publishing. Replica thresholds, zones,
node constraints, and policy impact stay under **Advanced settings**; every
change still requires a fresh preview before it can be applied.

For a selected file, **History & issues** saves named versions of its content
and lists the versions it keeps. From there you can preview, restore, or save a
version as a new file, add or release names, and review missing-copy records.
For a folder, the same sheet, or **Saved versions** in the folder overview,
lists versions saved from files in that folder, including deleted files that
can be recovered; **Show file** opens a listed file's current location.
Internal commit generations are not saved versions. See
[file content history](file-history.md#use-the-app) for details and the
equivalent commands; automatic retention rules are configured with the CLI.
**Run maintenance and refresh** requests ordinary DFS maintenance; it does not
claim that the client performed a path-scoped repair.

Use **Storage health** for the cluster-wide view. It keeps object/version
counts, pending writes, a bounded missing-copy list, and links to the Cluster
view, mount settings, and reconciliation activity out of the everyday file
list. Disaster-recovery operations such as rebuilding a lost repair queue or
clearing fenced placement authority remain explicit operator commands; they
are not ordinary Files actions.

On supported desktop platforms, an operator can also mount a subtree through
the host filesystem:

```sh
sinomo dfs mount /path/to/mount-point --root /sinomo
```

The mount command stays attached while mounted. Use `--read-only` when write
access is unnecessary, and choose a narrower SIFS root for everyday use. The
[SIFS reference](../sifs.md) explains the namespace and placement policies;
`sinomo dfs help` lists inspection, pinning, history, attachment, mount, and
repair commands.

On Windows, Cloud Files mounts are supervised by the main daemon and writable
synchronization must be explicit:

```powershell
sinomo dfs mount 'D:\sinomo' --root / --background --sync-local-changes
sinomo dfs mount --status --json
```

The same node-local mapping is available in the graphical app under
**Settings > Storage**. In **Mount settings**, set **Sync root** to the
absolute host directory (for example `D:\sinomo`) and **SIFS root** to the DFS
subtree to project (for example `/`). Enable **Durable writable cloud sync**
for a writable mount. The current supervisor supports one such mapping per
node, and the read-only live mount status appears on the same page.

File Explorer continues to show Windows' standard Cloud Files availability and
in-sync badges. For writable mounts it also shows **Waiting to upload**,
**Uploading**, **Retrying**, **Conflict**, or **Sync failed** while Sinomo has
matching durable work or attention evidence. A successfully completed item
returns to the normal Windows in-sync status. These properties are pushed only
when state changes; opening an Explorer folder does not query SIFS or start a
background status scan.

Disable the mount through `sinomo dfs mount --disable` rather than deleting the
projected directory or unregistering it through Windows tools. The supervisor
preserves registration identity, pending local evidence, and the exact provider
needed after restart. The
[Cloud Files architecture](../cloud-files-architecture.md) describes the
Windows data paths and recovery model.

## Android attachments and media

Use Chat's attachment chooser to select images or files in Android's document
picker. The picker grants access to the selected documents; broad storage access
is not required. Sinomo copies selections into private cache before validating
and attaching them. Each selection is limited to 32 files and 128 MiB in total.
Cancelling the picker leaves the draft unchanged.

Enabling the microphone or camera for setup or a call requests the corresponding
Android permission. A denial leaves that feature unavailable and shows an error;
allow access in Android's app settings and retry. Local calls do not enable
cluster device sharing. Camera capture uses the front camera when available and
adjusts frames for display rotation. Android's foreground and privacy controls
still govern capture, including capture requested by another node.

## Voice and Realtime Models

List local microphones, cameras, and speakers:

```sh
sinomo voice devices
sinomo voice help
```

`sinomo voice call` supports provider streams, native devices, recorded media,
optional video frames, and explicit input/output formats. Begin with a bounded
`--call-ms` and confirm the provider's audio format and privacy policy before
enabling native microphone or camera capture.

## Runtime Tests and Usage

The runtime has discoverable, opt-in checks:

```sh
sinomo test list
TEST_ID=your-test-id
sinomo test run "$TEST_ID"
sinomo usage --json
```

Tests marked live can contact providers, consume quota, or require a cluster.
They run live only when requested with the relevant flags.

## Automation

Prefer `--json` where the command advertises it. JSON makes node IDs, operation
IDs, errors, and evidence easier to preserve than parsing display output.

Automation should:

- set `SINOMO_DATA_DIR` explicitly;
- use stable IDs instead of display names;
- retain command version, target, and operation identity with results;
- apply timeouts outside commands that do not expose one;
- treat partial cluster fan-out as a partial result, not success for every
  node;
- avoid permission-changing, membership, and destructive recovery commands
  without an operator checkpoint.

For system health and incident workflows, continue with
[operations](operations.md).
