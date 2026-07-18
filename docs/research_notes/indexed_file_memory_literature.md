# Indexed File Memory Literature

Research date: 2026-07-05

## Question

Sinomo's current memory design is record-centric. The proposed redesign has one
core concept: memories are indexed files. A memory should be a durable file or
file-like object, while chunks, embeddings, summaries, entities, and
heterogeneous relations are derived indexes over those files.

The closest motivating implementation is
[codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp), but the
target Sinomo design should generalize beyond code to Markdown, logs,
transcripts, screenshots, PDFs, structured data, configuration, and future media
types. It should also allow relation types to be heterogeneous and model
defined, not only a fixed code-structure edge taxonomy.

## Short Answer

I did not find a single system that combines all of these requirements:

- file objects are the source of truth for agent memory;
- indexes are rebuildable derived artifacts over those files;
- file types are generic, including non-code and multimodal artifacts;
- relations are heterogeneous, provenance-carrying, and model defined;
- the system is writable by agents as durable long-term memory.

Several lines of work cover major pieces of the idea:

- Semantic file systems provide the historical precedent for indexing files by
  extracted meaning rather than only path hierarchy.
- Code intelligence systems provide mature patterns for analyzer-generated graph
  indexes over source files.
- Graph-RAG systems show how to extract and retrieve relation graphs from
  document corpora.
- Agent memory systems such as Graphiti/Zep and Cognee show temporal,
  provenance-rich, graph-based memory for agents.
- Multimodal RAG systems such as RAG-Anything show how file-derived entities and
  cross-modal relations can work beyond text.

The most promising Sinomo shape is: keep memory files as the durable substrate,
store indexes as derived and rebuildable artifacts, and make every relation edge
carry source anchors, extractor identity, model/version metadata, confidence,
validity, and visibility.

## Working Definition

For this note, an indexed file memory system has these objects:

- `MemoryFile`: durable content such as Markdown, JSON, YAML, source code,
  transcript, log, PDF, screenshot, image, audio transcript, video transcript, or
  a model-generated artifact. It has content identity, version identity, scope,
  ownership, and visibility.
- `MemoryAnchor`: a stable pointer into a memory file. Examples include byte
  ranges, line ranges, Markdown heading paths, JSON pointers, PDF page regions,
  image regions, timestamps, or transcript utterance ids.
- `MemoryIndex`: derived search structures over files, such as lexical tokens,
  chunks, embeddings, summaries, entities, topics, tables, and media-derived
  descriptors.
- `MemoryRelation`: a typed edge whose endpoints may be files, anchors,
  extracted entities, agent sessions, tasks, tools, people, projects, or other
  relations. Relation labels can be built in, user declared, or model proposed.
- `MemoryEvidenceBundle`: the retrieval result passed to an agent. It should
  contain file references, snippets or regions, relation paths, and enough
  provenance for the agent to cite or audit the memory.

The key constraint is that indexes are not the memory source of truth. They are
cacheable, compactable, invalidatable, and rebuildable from files plus extractor
versions.

## Literature Map

| Work | Primary substrate | Relation or index style | Useful idea for Sinomo | Main gap |
| --- | --- | --- | --- | --- |
| [Semantic File Systems](https://web.mit.edu/6.826/www/notes/HO13.pdf) | Files | File-type transducers extract attributes; virtual directories are queries | Treat files as objects with automatically extracted semantic attributes | Pre-LLM, mostly attribute search rather than rich agent memory |
| [Haystack](https://ceur-ws.org/Vol-55/huynh.pdf) and [Semex](https://lunadong.com/publication/semex_cidr.pdf) | Personal information objects | RDF-like objects and typed associations across documents, email, contacts, tasks | Generic personal memory graph across heterogeneous artifacts | Older ontology-driven systems, not modern model-derived relation extraction |
| [codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp) and [Codebase-Memory paper](https://arxiv.org/html/2603.27277v1) | Source files | Tree-sitter and LSP-backed code graph exposed through MCP | Strong implementation pattern for file indexing, incremental updates, and graph queries | Code-oriented nodes and edges; relation schema is mostly fixed by program structure |
| [Joern code property graph](https://docs.joern.io/code-property-graph/), [SCIP](https://github.com/scip-code/scip), [Kythe](https://kythe.io/docs/kythe-storage.html) | Source files | Analyzer-produced graph/index artifacts for code navigation and analysis | Stable source anchors, analyzer outputs, portable index artifacts, forward/reverse edge handling | Code-only and deterministic-analyzer centric |
| [AIOS-LSFS](https://github.com/agiresearch/AIOS-LSFS) / [paper](https://openreview.net/forum?id=2G021ZqUEZ) | Files in an AI OS | Semantic indexes, LLM-generated update logs, semantic retrieval and rollback syscalls | Closest modern "semantic file system" for LLM agents | Focuses on file operations and semantic retrieval, not a heterogeneous relation graph memory |
| [Microsoft GraphRAG](https://microsoft.github.io/graphrag/) | Text corpus | LLM-extracted graph plus community summaries | Graph and summary layers help answer global questions that chunk search misses | Corpus/chunk centric; files are input documents rather than durable agent-written memory |
| [LlamaIndex PropertyGraphIndex](https://developers.llamaindex.ai/python/framework/module_guides/indexing/lpg_index_guide/) | Documents/chunks | LLM extractors produce entities and relation triples | Practical library pattern for model-defined relations over arbitrary documents | Index primitive, not a full memory substrate or filesystem-level design |
| [LightRAG](https://arxiv.org/abs/2410.05779) and [PathRAG](https://arxiv.org/abs/2502.14902) | Text corpus | Lightweight graph plus vector retrieval; path-based retrieval | Retrieval should return compact relation paths, not only top-k chunks | Mostly RAG over corpora, not durable agent memory files |
| [Graphiti](https://github.com/getzep/graphiti) / [Zep paper](https://arxiv.org/html/2501.13956v1) | Episodes and business data | Temporal knowledge graph with provenance, validity, learned/prescribed ontology | Strong pattern for time-aware memory edges, provenance, and incremental graph updates | Memory source objects are episodes/data records, not necessarily files as first-class objects |
| [Cognee](https://github.com/topoteretes/cognee) | Arbitrary data sources | Knowledge graph plus vector memory, ontology generation, traceability | Closest open-source generic "any data to graph memory" system | File versioning and rebuildable file-index contract are less central |
| [RAG-Anything](https://arxiv.org/html/2510.12323v1) | Multimodal documents | Dual graph over textual semantics and cross-modal structure | Good precedent for generic file types, layout-aware anchors, and cross-modal relations | A RAG framework, not an agent-writable memory filesystem |
| [Mem0](https://github.com/mem0ai/mem0) | User/session/agent memory items | API-level long-term memory and hybrid retrieval | Useful product baseline and evaluation references | Less aligned with file-as-source-of-truth memory |

## Annotated Sources

### Semantic File Systems

[Semantic File Systems](https://web.mit.edu/6.826/www/notes/HO13.pdf)
(Gifford et al., SOSP 1991) is the cleanest historical ancestor of indexed file
memory. The system keeps ordinary files, runs file-type-specific transducers to
extract attributes, and exposes query-like virtual directories. For Sinomo, the
important lesson is the separation between durable file content and derived
semantic access paths. The modern version would replace only-path hierarchy with
file anchors, embeddings, relation edges, and model-derived metadata.

This also argues against making the relation graph the only durable memory. The
file remains auditable, portable, and re-indexable; the graph is an access layer.

The focused follow-up note
[Semantic Filesystem Catalogs for Indexed Memory](semantic_filesystem_catalogs_for_indexed_memory.md)
compares filesystem-integrated, sidecar, and hybrid catalog designs and proposes
the low-level DFS catalog, logical change feed, and exact-revision boundary for
Sinomo.

### Haystack and Semex

[Haystack](https://ceur-ws.org/Vol-55/huynh.pdf) modeled personal information
with RDF so agents could query across documents, email, appointments, tasks, and
other desktop objects without being trapped inside application-specific silos.
[Semex](https://lunadong.com/publication/semex_cidr.pdf) similarly built a
personal-information platform around objects and automatically discovered
associations between them.

These systems are directly relevant because they are not code-specific. Their
core assumption is that a user's memory should cross application boundaries and
should be navigable by semantic associations. The gap is that their semantics are
mostly ontology and extractor driven; they predate LLM-based relation proposal,
confidence scoring, and natural-language retrieval.

### Codebase-Memory MCP

[codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp) is the
closest implementation to the user's motivating example. It builds a persistent
SQLite graph from source files, exposes graph tools through MCP, supports
incremental indexing, uses a compressed graph artifact for team sharing, and
relies on the MCP client or coding agent as the intelligence layer. The related
[Codebase-Memory paper](https://arxiv.org/html/2603.27277v1) frames the problem
as repeated grep/file-reading waste and reports large token savings from a
queryable code graph.

For Sinomo, this is the best concrete pattern for:

- a derived graph artifact that lives beside or near the source corpus;
- cheap structural queries before reading raw files;
- incremental re-indexing by content hash and file watcher;
- graph tools that return relevant source snippets rather than dumping files.

The main limitation is domain specificity. The node and edge model is centered
on software: files, modules, classes, functions, imports, calls, routes, tests,
and related code facts. A Sinomo memory system should keep this as one extractor
family, not as the universal memory schema.

### Code Intelligence Indexes

[Joern's code property graph](https://docs.joern.io/code-property-graph/) merges
multiple program views into an edge-labeled attributed multigraph for analysis.
[SCIP](https://github.com/scip-code/scip) is a language-agnostic protocol for
source-code indexes used by code navigation tools. [Kythe's storage
model](https://kythe.io/docs/kythe-storage.html) is especially useful because it
distinguishes persistent storage entries from serving indexes, uses stable node
identifiers, and treats schema labels as extensible.

The transfer to Sinomo is architectural, not domain-specific:

- analyzers emit appendable, portable index artifacts;
- source anchors matter as much as extracted nodes;
- forward edges can be stored canonically while reverse edges are derived for
  serving;
- persistent storage can be optimized for correctness and portability, while
  serving indexes can be optimized separately.

### AIOS-LSFS

[AIOS-LSFS](https://github.com/agiresearch/AIOS-LSFS) is a modern LLM-based
semantic file system for AI agents. The paper describes semantic retrieval,
file-update summarization, rollback, semantic grouping, semantic sharing, and
LLM-assisted file operations inside an AI OS.

This is the closest work to "files are the substrate" in a modern LLM setting.
It supports the idea that file operations themselves should become semantic and
agent-friendly. However, LSFS appears more focused on semantic file management
and retrieval APIs than on a durable heterogeneous relation graph over arbitrary
memory files. Sinomo can borrow the file-system API framing while pushing the
relation model further.

### GraphRAG, LightRAG, and PathRAG

[Microsoft GraphRAG](https://microsoft.github.io/graphrag/) extracts a graph and
community summaries from a text corpus so that retrieval can answer broad,
global questions that naive vector search misses. [LightRAG](https://arxiv.org/abs/2410.05779)
combines graph and vector retrieval in a lightweight incremental design.
[PathRAG](https://arxiv.org/abs/2502.14902) emphasizes retrieving key relational
paths rather than redundant graph neighborhoods.

These systems are relevant to the retrieval side of indexed file memory. They
suggest that a memory query should not only return top-k chunks. It should be
able to return:

- a small set of candidate memory files;
- exact anchors inside those files;
- relation paths connecting the query to the evidence;
- summaries at different graph scopes.

The gap is that these systems usually treat files as source documents for an
offline corpus. They do not make "agent-writable file memory with rebuildable
indexes" the central storage abstraction.

### LlamaIndex PropertyGraphIndex

[LlamaIndex PropertyGraphIndex](https://developers.llamaindex.ai/python/framework/module_guides/indexing/lpg_index_guide/)
is useful as a practical relation-extraction pattern. It can attach entity and
relation nodes to chunks, use LLM extractors for triples, preserve implicit
chunk-to-document paths, and persist graph/vector stores.

The lesson for Sinomo is to make model-defined relations a first-class extractor
output, but to avoid letting the library's chunk abstraction become the durable
memory model. Chunks should be anchors or index entries over files.

### Graphiti and Zep

[Graphiti](https://github.com/getzep/graphiti) builds temporal context graphs
for AI agents. It tracks facts over time, keeps provenance to source episodes,
supports prescribed and learned ontologies, and performs incremental graph
updates. The [Zep paper](https://arxiv.org/html/2501.13956v1) describes a
dynamic temporal knowledge graph with episode, semantic entity, and community
subgraphs.

This is one of the strongest precedents for Sinomo's relation layer. It points
to several required fields for memory relations:

- source provenance;
- valid-time and supersession semantics;
- confidence and contradiction handling;
- learned relation/entity types under governance;
- hybrid retrieval over graph, text, and time.

The difference is substrate. In Sinomo's proposed design, the source episode
should usually be a memory file or file anchor, not an opaque memory object.

### Cognee and Mem0

[Cognee](https://github.com/topoteretes/cognee) is an open-source AI memory
platform that ingests data from many formats, builds a knowledge graph, combines
vector search with graph reasoning, and emphasizes traceability. It is one of
the closest open-source systems to "generic data to graph memory."

[Mem0](https://github.com/mem0ai/mem0) is a product-oriented memory layer with
user, session, and agent memories. It is less aligned with file-index memory,
but it is useful as a baseline for what application developers expect from a
memory API and for pointers to recent memory evaluations.

### RAG-Anything

[RAG-Anything](https://arxiv.org/html/2510.12323v1) is important because it
treats multimodal documents as interconnected knowledge entities instead of
flattening everything into text chunks. It uses a dual-graph design for textual
semantics and cross-modal structure, with attention to tables, figures,
equations, captions, layout, and surrounding context.

This is the best match for Sinomo's "generic file types beyond code" requirement.
For indexed file memory, it implies that anchors should be typed by medium:

- line ranges for text;
- JSON pointers for structured data;
- page regions for PDFs;
- table cells or ranges for tabular content;
- image regions and OCR spans for screenshots;
- time ranges for audio/video transcripts.

## Design Implications for Sinomo

### 1. File as Source of Truth

The durable unit should be a memory file or file-like object. Existing
record-style memory can remain as a compatibility layer, but records should
either become memory files themselves or become index entries that cite memory
file anchors.

This gives Sinomo a clean rebuild story: if an extractor improves, the system can
recompute chunks, embeddings, entities, summaries, and relation edges without
rewriting the source memory.

### 2. Derived Indexes, Not Primary Memory

Indexes should be explicitly derived artifacts. Each index entry needs at least:

- source memory file id and version;
- source anchor;
- extractor id and version;
- model id, prompt id, and decoding settings when model-derived;
- creation time;
- confidence or quality score when applicable;
- visibility/scope inherited from the source anchor;
- invalidation status.

The index can be compacted or replicated independently, but it should never be
the only location where the memory content exists.

### 3. Heterogeneous Relation Edge Model

Relation edges should not be a single closed enum. A workable edge shape is:

```text
MemoryRelation {
  relation_id,
  relation_type_id,
  relation_type_label,
  relation_schema_source,   // builtin | user | model | module
  subject_ref,
  object_ref,
  source_anchor_refs,
  extractor_id,
  model_id,
  confidence,
  valid_time,
  observed_time,
  supersedes_relation_ids,
  visibility,
}
```

The edge label can be model defined, but governance should distinguish:

- built-in relation types that carry runtime semantics;
- user or module-declared relation types;
- model-proposed relation types that are queryable but not trusted for policy;
- promoted relation types that have survived validation or repeated use.

### 4. Extractor Families

Sinomo should treat extractors as plugins or modules with typed outputs.
Suggested initial families:

- text and Markdown: headings, sections, links, TODOs, summaries, entities;
- JSON/YAML/TOML: path anchors, key/value semantics, schema hints;
- code: tree-sitter symbols, imports, definitions, calls, tests;
- logs/transcripts: events, timestamps, speakers, tasks, decisions;
- PDFs: page text, layout regions, tables, captions;
- screenshots/images: OCR, UI elements, regions, visual labels;
- model relation extractor: candidate triples and relation labels over text or
  multimodal anchors.

Deterministic extractors should run first. Model extractors can then propose
higher-level entities and relations using deterministic anchors as evidence.

### 5. Retrieval Should Return Evidence Bundles

The agent should not receive a bag of memory snippets with no structure. A
retrieval result should include the relation path and file evidence:

```text
MemoryEvidenceBundle {
  query,
  candidate_files,
  anchors,
  relation_paths,
  summaries,
  retrieval_modes_used,
  omitted_due_to_budget,
}
```

Useful retrieval modes include lexical, vector, graph-local, graph-global,
path-pruned, temporal, and scope-filtered retrieval. For context assembly, graph
paths should help explain why a file is being included.

### 6. Rebuild, Invalidation, and Audit

Every index entry should be tied to a file content version and extractor version.
When a file changes, the system should mark derived entries as stale or superseded
instead of silently mutating them. This supports:

- reproducible retrieval audits;
- relation contradiction detection;
- rollback;
- compacted snapshots;
- cluster replication of source files separately from derived indexes.

### 7. Policy and Privacy

Relations can leak source content. For example, an edge
`user_prefers_bank: X` can reveal private information even if the source sentence
is hidden. Relation visibility should therefore be no broader than the most
restrictive source anchor used to derive the relation. Model-proposed relation
labels should not bypass policy.

## Recommended MVP

1. Define a `MemoryFile` namespace and metadata model, backed by SIFS or the
   existing durable storage path.
2. Keep current durable memory records as a compatibility layer while making new
   memories file-first.
3. Implement deterministic indexing for Markdown/text and JSON/YAML/TOML first.
4. Add a small relation store with open relation labels, source anchors,
   extractor provenance, confidence, and visibility.
5. Add one model-based relation extractor for text/Markdown that emits strict
   JSON and never writes trusted policy edges.
6. Make retrieval return `MemoryEvidenceBundle` objects containing file refs,
   snippets, and relation paths.
7. Treat graph/vector indexes as rebuildable derived artifacts. Store enough
   metadata to invalidate by file version and extractor version.

This MVP would prove the core concept without requiring full multimodal parsing
or a large ontology up front.

## Open Research Questions

- Should model-defined relation labels be fully open, or should they be mapped
  into a controlled namespace before becoming durable?
- Should relation schemas be user-visible editable files, module-owned metadata,
  or both?
- What is the right storage split between SOBJ append logs, SQLite, and a graph
  serving index?
- How should contradictions be represented: relation supersession, file-version
  supersession, or explicit contradiction edges?
- How can the system evaluate relation quality without overfitting to retrieval
  benchmarks that only test answer accuracy?
- How should memory files, derived indexes, and private relation edges replicate
  across nodes with different trust scopes?

## Sources Consulted

- [codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp)
- [Codebase-Memory paper](https://arxiv.org/html/2603.27277v1)
- [Semantic File Systems](https://web.mit.edu/6.826/www/notes/HO13.pdf)
- [Haystack RDF personal information management](https://ceur-ws.org/Vol-55/huynh.pdf)
- [Semex](https://lunadong.com/publication/semex_cidr.pdf)
- [AIOS-LSFS](https://github.com/agiresearch/AIOS-LSFS)
- [LSFS OpenReview paper](https://openreview.net/forum?id=2G021ZqUEZ)
- [Microsoft GraphRAG](https://microsoft.github.io/graphrag/)
- [LlamaIndex PropertyGraphIndex](https://developers.llamaindex.ai/python/framework/module_guides/indexing/lpg_index_guide/)
- [Graph-based Agent Memory survey](https://arxiv.org/html/2602.05665v1)
- [Awesome Graph Memory](https://github.com/DEEP-PolyU/Awesome-GraphMemory)
- [Graphiti](https://github.com/getzep/graphiti)
- [Zep paper](https://arxiv.org/html/2501.13956v1)
- [Cognee](https://github.com/topoteretes/cognee)
- [Mem0](https://github.com/mem0ai/mem0)
- [LightRAG](https://arxiv.org/abs/2410.05779)
- [PathRAG](https://arxiv.org/abs/2502.14902)
- [RAG-Anything](https://arxiv.org/html/2510.12323v1)
- [Joern code property graph](https://docs.joern.io/code-property-graph/)
- [SCIP](https://github.com/scip-code/scip)
- [Kythe storage model](https://kythe.io/docs/kythe-storage.html)
