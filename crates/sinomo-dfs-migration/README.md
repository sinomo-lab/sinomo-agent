# sinomo-dfs-migration

This R1 component owns retained DFS format decoding and incremental conversion.
The current DFS crate contains the current execution and storage path.

Main and Embedded hosts register the migration provider before opening DFS
storage. The provider advances bounded work when a retained root is touched;
Shadow hosts do not convert or publish. Progress, original source bytes and
source digests survive restart. Source adaptation follows original validation.

Conversion builds a hidden current candidate. Native installation binds the
actual source cut, current configuration, physical storage ownership and exact
KV comparisons. Original reservations, jobs and proofs remain data until the
current authority path establishes their replacements. Standalone conversion
uses explicit local ownership evidence.

Current metadata snapshots use schema 9; their operation journal uses the V7
prepare/commit family. Older formats are handled here, and incompatible daemon
versions must be upgraded.
