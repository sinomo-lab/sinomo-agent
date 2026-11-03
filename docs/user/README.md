# User and Operator Guide

This section explains how to install, use, and operate the current Sinomo
system. It assumes no knowledge of the Rust crates.

## Choose a Starting Point

| Goal | Guide |
|---|---|
| Install a one-node system and run the first model-backed interaction | [Getting started](getting-started.md) |
| Use the TUI, app, agents, models, memory, skills, files, and voice surfaces | [Everyday use](everyday-use.md) |
| Capture, retain, restore, or recover file content | [File history](file-history.md) |
| Stream a desktop/app to a realtime model and send input | [App Use: background and raw control](computer-and-app-use.md) |
| Inspect agent usage, quotas, estimated costs and budgets | [Usage and cost estimates](usage.md) |
| Keep an installation healthy and collect operational evidence | [Operations](operations.md) |
| Create and monitor a metric-driven evolution goal | [Evolution goals](evolution.md) |
| Create, join, inspect, migrate, or recover a cluster | [Cluster operations](cluster-operations.md) |
| Build, stage, activate, diagnose, or recover an update | [Rollout operations](rollout-operations.md) |
| Diagnose common local, cluster, model, DFS, and permission failures | [Troubleshooting](troubleshooting.md) |

Read the [system overview](../system-overview.md) for the concepts behind these
workflows. The [glossary](../glossary.md) defines terms used by the CLI and the
architecture documents.

## Command Help Is Authoritative

The installed binary is the final source of truth for syntax:

```sh
sinomo help
COMMAND=cluster
sinomo "$COMMAND" help
```

Some commands expose nested help, such as `sinomo agent session help` or
`sinomo dfs history help`. Use the help shown by the exact installed version
before a destructive, cluster-wide, or permission-changing operation.

Shell examples use uppercase variables for values that you must choose. Values
in angle brackets in prose or syntax summaries are placeholders; never type the
brackets or square-bracketed optional notation literally into a shell.

## Safety Conventions

- Check `SINOMO_DATA_DIR` before changing cluster, rollout, or local permission
  state. The default is `~/.sinomo`.
- Treat join invites, provider credentials, private keys, and diagnostic output
  containing paths or prompts as sensitive.
- Observe first. Do not delete Raft, DFS, rollout, or daemon files to resolve an
  error.
- Cluster membership never grants remote shell or device exposure. Those
  permissions must be enabled manually on the receiving node.
- A shadow daemon is a recovery endpoint for the same installation, not a
  second cluster member.

Sinomo is under active development. Back up important state, keep the binary
and documentation from the same revision, and preserve diagnostic evidence
when reporting a problem.
