# Remote Management File Transfer Prompt

Use bounded filesystem routes only after `remote-management` has been enabled
locally on the target; permission denial requires manual local action. For an
upload, write the first chunk at offset zero with truncation, write subsequent
chunks at exact offsets without truncation, and retry a lost response with the
identical offset and bytes. Avoid append mode for retry-safe transfers. Verify
the final length and BLAKE3 digest with file stat. Download with bounded offset
reads until `truncated` is false.
