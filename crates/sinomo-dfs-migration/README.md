# sinomo-dfs-migration

This R1 component owns retained DFS format decoding and incremental conversion.
The current DFS crate contains the current execution and storage path.

Main and Embedded hosts register the migration provider before opening DFS
storage. The provider advances bounded work when a retained root is touched;
Shadow hosts do not convert or publish. Progress, original source bytes and
source digests survive restart. Source adaptation follows original validation.
Preparation that is still pending when the component mounts resumes in the
initial startup-recovery generation under the same data owner; it does not
fail the mount.

Each metadata shard converts on its own, driven by its primary from its own
copy. A shard with no storage record is old storage that this component owns,
and it stays closed until its conversion activates; the filesystem's other
shards keep serving. The primary records the attempt Converting in the shard's
storage record before it reads what the other shards contribute to its counts.
It builds a hidden current candidate, seals it into the attempt, installs it on
the members the shard's install rule names and activates it with one
transaction on the shard's own keys. Native installation binds the actual
source cut, the attempt's serving record, physical storage ownership and exact
comparisons of the shard's own keys; nothing writes the filesystem
configuration or advances its catalog generation. A serving change supersedes
the attempt, and the next primary converts again from its own copy. When a
closed shard's primary is lost, a member converts from its own copy in its
place, and its activation also makes it primary at the next epoch. Original
reservations, jobs and proofs remain data until the current authority path
establishes their replacements.

Current metadata snapshots use storage format 12; their operation journal uses
the V7 prepare/commit family. Older formats are read only here, and
incompatible daemon versions must be upgraded.
