# Remote Management File Transfer

Use the typed filesystem routes for bounded, retry-safe transfers. All routes
require the target operator to enable `remote-management` locally; module
rollout alone is not authorization. No supported remote API can grant that
permission, and an already-authorized shell must never be used to rewrite it.
On permission denial, stop and request manual local enablement.

## Download

1. Call `/remote-management/fs/stat` to confirm the source is a regular file
   and record its length. Request the BLAKE3 digest when end-to-end verification
   is useful.
2. Call `/remote-management/fs/read` repeatedly with explicit offsets and a
   bounded `max_bytes`. Advance by the number of bytes actually returned, not
   by the requested limit, and stop when `truncated` is false.
3. Verify the received byte count and, when requested, the BLAKE3 digest.

## Upload

1. Split the payload into chunks no larger than the protocol write limit.
2. Send the first `/remote-management/fs/write` request with `offset: 0` and
   `truncate: true`. Set `create_parent` only when creating missing directories
   is intended.
3. Send later chunks at their exact byte offsets without truncation. Use the
   returned `next_offset` to prepare the following request.
4. A failed or lost response can be retried with the identical offset and
   bytes: an offset write overwrites the same range instead of duplicating it.
   Do not use append mode when retry safety matters.
5. Call `/remote-management/fs/stat` after completion and compare length and
   BLAKE3 digest. Use `/remote-management/fs/list` only for bounded directory
   discovery and honor its `truncated` flag.

Do not confuse an incomplete transfer with permission denial. Transfer errors
may be retried carefully; local-policy denial requires action on the node.
