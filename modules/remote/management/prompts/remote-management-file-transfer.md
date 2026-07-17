# Remote Management File Transfer Prompt

Use path-oriented `remote_file_get` and `remote_file_put` only after
`remote-management` has been enabled locally on the target. Each complete file
uses one object-RPC session with a typed head and optional streaming attachment;
never put file bytes in JSON or build offset/chunk/small-versus-large loops.
Length and BLAKE3 are verified before atomic publication. Upload overwrite is
false by default and must be requested deliberately; download overwrite follows
the same rule for its local destination. Permission denial requires manual local
action. After an uncertain transport failure, inspect remote state before
starting a new transfer.
