# Remote Management Compute Use

Use the remote-management module when you need direct machine interaction
through admin-authorized tools.

Start with `module_rpc_remote_management_remote_management_system_info_read`
when host identity, OS, architecture, current directory, executable path, shell,
or selected environment variables matter.

Use `module_rpc_remote_management_remote_management_shell_run` for short,
bounded commands. Set a working directory when the command depends on local
files, keep timeouts explicit for risky commands, and inspect stdout, stderr,
exit code, and timeout status before taking follow-up action.

Use filesystem routes for precise reads, writes, and listings. Prefer read and
list before write. For writes, include exact bytes and set `create_parent` only
when creating missing directories is intended.

This module does not provide screen capture yet. Do not infer visual state from
these tools.
