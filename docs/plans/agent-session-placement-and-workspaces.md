# Agent Session Placement and Workspace Projection

Status: Active implementation plan
Last reviewed: 2026-07-15

The durable ownership model is now represented in `sinomo-agent`:

- homes are keyed by agent id and are independent of sessions and roles;
- chats and channels carry ordered workspace grants, with the first grant as
  cwd;
- agent definitions may select a home server for new sessions;
- sessions record their active server and placement generation, plus a
  reference to one independently managed sandbox resource;
- manual migration invalidates session-host-local resources without copying
  them and does not move the sandbox.
- normal local chat/CLI setup provisions the default persistent sandbox before
  tools are exposed and persists the automatic attachment on the session;
- the command tool fails with `sandbox_not_attached` when the resource is
  missing and never creates one during tool execution.

The remaining work is runtime projection and distributed routing:

1. Replace the compatibility `AgentSession.workspace_binding` reads with the
   conversation workspace list throughout TUI and app resume paths, then remove
   the legacy field after persisted snapshots have aged out.
2. Replace the current local-backend automatic provisioning adapter with the
   node/cluster sandbox service route, including remote placement. Explicit
   attachment must accept an existing, potentially shared sandbox and must
   never provision a replacement implicitly.
3. Route every agent tool through the attached sandbox service, including file
   tools; tools must not bypass the sandbox filesystem or process runtime.
4. Extend the tool runtime from a primary root to an ordered access set. Mount
   each workspace into the persistent sandbox before exposing tools, with the
   first mount as cwd and the agent home mounted by default.
5. Persist the authoritative session-server record in cluster-visible storage
   and add a compare-and-swap placement-generation update for migration.
6. Route chat/channel ingress and live controls to the active session server.
   Stale senders refresh placement and retry; they never auto-migrate.
7. Add explicit migration commands and reports showing unavailable local
   resources and the unchanged sandbox placement.
8. Add multi-node acceptance coverage: two agents in one channel on different
   servers, cross-device ingress routed to the current host, SIFS workspace
   continuity, local-workspace loss after migration, remote sandbox execution,
   and rejection of non-portable sandbox migration.

This plan must not introduce local file copying, let tool calls create
sandboxes, or couple session placement to sandbox lifecycle.
