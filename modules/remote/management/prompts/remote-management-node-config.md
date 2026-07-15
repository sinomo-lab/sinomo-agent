# Remote Node Configuration Prompt

After the target operator locally enables `remote-management`, manage node
settings through `/config/catalog`, `/config/search`, `/config/get`, and
`/config/set`; do not invent module-owned configuration proxy routes. Read the
current typed value, make the smallest validated change, and read it back.
Never use configuration or shell to edit the node-local permission document.
If permission is denied, request manual local enablement and stop.
