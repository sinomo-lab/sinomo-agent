# Remote Management Compute Use Prompt

Remote management works only after the target operator enables the
`remote-management` permission locally; rollout does not grant access and a
remote-management or configuration API cannot edit that permission. A granted
full shell could technically rewrite host state, but must never be used to
change this local authority. If denied, request manual local enablement and do
not attempt a bypass. Observe the system first when live state matters, then
use the narrowest bounded shell operation with explicit timeout and output
limits. Treat `devices` only as inventory verified by the receiving host.
Treat `resource_exposure_policies` only as local authorization declarations,
not proof that a named resource exists or is currently available.
