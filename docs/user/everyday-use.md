# Everyday Use

This guide introduces the main user-facing surfaces after the daemon, cluster,
and provider are configured. It is a map, not an exhaustive command reference;
use `sinomo <command> help` for the installed version.

## Choose an Interaction Surface

| Surface | Best for | Start command |
|---|---|---|
| Terminal UI | Conversation plus occasional CLI commands | `sinomo` |
| Graphical app | Chat, SIFS files, cluster state, and settings | `cargo run -p sinomo-app` |
| One-shot agent | Scripts, focused tasks, and reproducible prompts | `sinomo agent run --prompt <text>` |
| Direct CLI | Configuration, inspection, automation, and operations | `sinomo <command>` |

The TUI connects to the local daemon when available. Within it, `/models list`
is a CLI command while text without `/` is a user message.

The bare `sinomo-app` command is available only when the graphical binary was
separately built and installed; `sinomo install` installs the main `sinomo`
client/daemon binary, not the graphical app.

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

Native model runtime locations and endpoints are cluster-scoped settings. Read
their current values before changing them:

```sh
sinomo models runtime
sinomo models help
```

Do not place provider credentials in cluster KV. Authenticate on the node that
will use the provider with `sinomo providers auth <provider>`.

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

Live sessions expose explicit control commands:

```sh
SESSION_ID=your-session-id
sinomo agent session inspect "$SESSION_ID"
sinomo agent session progress "$SESSION_ID"
sinomo agent session steer "$SESSION_ID" "Recheck the failing test first"
sinomo agent session interrupt "$SESSION_ID" "The task is no longer needed"
```

Steering and follow-up queue input for a live runtime. Interrupt requests
cancellation; it does not erase already committed files or external effects.

## Indexed-File Memory

Register a directory of Markdown or text sources, build an index generation,
and retrieve evidence:

```sh
sinomo memory root add /absolute/path/to/notes
sinomo memory root list
ROOT_ID=your-root-id
sinomo memory index "$ROOT_ID"
sinomo memory query "where did we decide the retention policy?"
```

The source files remain the durable evidence. Indexes are derived and can be
rebuilt. Keep secrets out of a registered root unless its storage and scope
policy are appropriate for those secrets.

## Skills

Skills are portable instruction packages based on a `SKILL.md` file:

```sh
SKILL_ID=your-skill-id
SCOPE=your-scope
sinomo skill list
sinomo skill import /path/to/skill-directory
sinomo skill show "$SKILL_ID" --text
sinomo skill activate "$SKILL_ID" --scope "$SCOPE"
sinomo skill export "$SKILL_ID" --output exported-skill.md
```

Inspect imported instructions and resources before activation. A skill can
guide an agent but cannot grant host, network, filesystem, device, secret, or
rollout permissions.

## Shared Files and SIFS

The graphical app exposes the SIFS namespace directly. On supported desktop
platforms, an operator can also mount a subtree through the host filesystem:

```sh
sinomo dfs mount /path/to/mount-point --root /sinomo
```

The mount command stays attached while mounted. Use `--read-only` when write
access is unnecessary, and choose a narrower SIFS root for everyday use. The
[SIFS reference](../sifs.md) explains the namespace and placement policies;
`sinomo dfs help` lists inspection, pinning, exposure, conflict, and repair
commands.

Local host-path exposure is explicit and node-owned:

```sh
sinomo dfs expose init
sinomo dfs expose list
sinomo dfs expose add --source /local/path --dfs-path /sinomo/exposed/name --read-only
```

Review symlink and write behavior before exposing a directory. Cluster
membership does not expose host paths automatically.

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
