---
name: remote-management
description: Bounded remote shell and system observation guidance.
---

# Remote Management Compute Use

Use the remote-management routes for bounded host inspection and command
execution on a Sinomo node.

## Local authorization boundary

Installing, rolling out, or activating this module does not grant remote
control. The target node must first have the `remote-management` permission
enabled locally by its operator. That permission cannot be enabled, disabled,
or otherwise edited through any supported remote-management or configuration
RPC. A granted shell is technically full host access and could rewrite local
state, but doing so would violate this authority boundary; never use it to
change node-local permissions.

If a route is denied because the local permission is absent, stop and tell the
operator that manual local enablement is required. Do not retry through shell,
configuration, or another tool to bypass the denial. Shell execution is
equivalent to full host access once permission is granted, and unsandboxed or
unbounded computer-use tools are subject to the same local permission.

## Workflow

1. Call `/remote-management/system/observe` before making changes when current
   load, memory, disks, network interfaces, or processes could affect the task.
   Request only the optional sections you need and keep process counts bounded.
2. Call `/remote-management/system/info` for stable host identity, OS,
   architecture, current directory, executable path, shell, or explicitly
   selected non-secret environment variables.
3. Call `/remote-management/shell/run` only for the narrow command needed.
   Supply a working directory when relevant, use an explicit timeout and output
   limit, and inspect exit code, timeout, truncation, stdout, and stderr before
   deciding the next action.
4. Prefer typed filesystem and configuration routes over shell commands when a
   route already represents the operation.

System observation keeps verified device inventory and node-local resource
policy separate. `devices` contains only inventory verified by the target host
(and is empty when no inventory provider is active); `resource_exposure_policies`
contains local policy declarations and does not prove that a named resource
exists or is currently available. Never infer missing devices from other
observation fields. Any future tool that actually operates on a resource must
re-check the node-local permission store's fail-closed
`resource_operation_exposed` guard at final dispatch.

The current interface does not provide screen capture or interactive GUI
control.
