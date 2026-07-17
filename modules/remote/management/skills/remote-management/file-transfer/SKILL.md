# Remote Management File Transfer

Use the path-oriented `remote_file_get` and `remote_file_put` tools for complete
files. Each transfer is one object-RPC session with a typed head and an optional
streaming attachment; file bytes never belong in JSON, offsets, or chunk loops.
There is no application file-size ceiling.

All routes require the target operator to enable `remote-management` locally.
Module rollout alone is not authorization. No supported remote API can grant
that permission, and an authorized shell must never be used to rewrite it. On
permission denial, stop and request manual local enablement.

## Download

1. Call `remote_file_get` with `remote_path` and `local_path`. Set
   `create_parent` only when creating missing local directories is intended.
   `overwrite` defaults to false; set it only when replacing an existing local
   file is explicitly intended.
2. The tool streams `/remote-management/fs/read` directly into a same-directory
   temporary file, verifies the typed `length` and BLAKE3 digest, syncs it, and
   atomically installs it without clobbering by default. A failed integrity
   check or no-clobber conflict leaves the destination unchanged and removes
   the temporary file.
3. Use `remote_file_stat` before or after only when separate discovery or
   observation is useful; it is not part of the transfer protocol.

## Upload

1. Call `remote_file_put` with `local_path` and `remote_path`. The tool hashes
   the local file and sends its expected length and BLAKE3 digest in the signed
   head while streaming the file as that session's request attachment.
2. `overwrite` defaults to false. Set it to true only when replacing an existing
   remote file is explicitly intended. Set `create_parent` only when creating
   missing remote directories is intended.
3. The receiving host verifies the signed head, staged attachment descriptor,
   and copied bytes before atomically publishing the file. The response repeats
   the committed length and digest for end-to-end verification.

A transport failure can have an uncertain application outcome. Inspect the
remote path before deciding whether to start a new transfer. Do not recreate
offset, append, truncate, chunk, or small-versus-large transfer protocols.
`remote_file_list` remains bounded directory discovery; honor its `truncated`
flag.
