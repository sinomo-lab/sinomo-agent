//! Retrieval-only LongMemEval case driver for Sinomo's explicit memory tools.
//!
//! The driver seeds an isolated in-memory VFS, writes one background activity
//! projection per turn, writes one Markdown memory note per session through
//! `memory_write`, and queries both `session_history_search` and
//! `memory_search`. It never opens DFS, a daemon, or operator state.

use serde::{Deserialize, Serialize};
use sinomo_agent::tools::memory_notes;
use sinomo_agent::{
    AgentTaskActivityProjection, AgentTaskActivityProjectionStore, AgentTaskLifecycleStatus,
    AgentToolCall, AgentToolRegistry, AgentToolStatus, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, MemoryNoteAgentToolConfig, TaskId, VfsMemoryNoteBackend,
};
use sinomo_cli::register_agent_history_search_tool_with_vfs;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::vfs::VfsHandle;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const INPUT_SCHEMA: &str = "sinomo.benchmark.memory-tools-case.v1";
const OUTPUT_SCHEMA: &str = "sinomo.benchmark.memory-tools-result.v1";
const AGENT_KEY: &str = "longmemeval-agent";
const HISTORY_TOOL: &str = "session_history_search";
const MEMORY_SEARCH_TOOL: &str = "memory_search";
const MEMORY_WRITE_TOOL: &str = "memory_write";
const MAX_PAGES_PER_QUERY: usize = 10_000;
const RRF_K: f64 = 60.0;

#[derive(Debug, Deserialize)]
struct CaseInput {
    schema: String,
    case_id: String,
    queries: Vec<String>,
    max_ranked_results: usize,
    sessions: Vec<CaseSession>,
}

#[derive(Debug, Deserialize)]
struct CaseSession {
    session_id: String,
    date: String,
    turns: Vec<CaseTurn>,
}

#[derive(Debug, Deserialize)]
struct CaseTurn {
    role: String,
    content: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TurnLocator {
    session_id: String,
    turn_id: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum RetrievalSource {
    SessionHistory,
    MemoryNotes,
}

#[derive(Clone, Debug, Default)]
struct HitScore {
    source_scores: BTreeMap<RetrievalSource, SourceHitScore>,
    first_seen: usize,
}

#[derive(Clone, Debug, Default)]
struct SourceHitScore {
    query_indexes: BTreeSet<usize>,
    reciprocal_rank: f64,
}

#[derive(Debug, Default, Serialize)]
struct SourceStats {
    tool_calls: usize,
    returned_matches: usize,
    partial_pages: usize,
    truncated_pages: usize,
}

#[derive(Debug, Serialize)]
struct RankingOutput {
    ranked_session_ids: Vec<String>,
    ranked_turn_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CaseOutput {
    schema: &'static str,
    case_id: String,
    query_terms: Vec<String>,
    seeded_sessions: usize,
    seeded_turns: usize,
    seeded_memory_notes: usize,
    max_ranked_results: usize,
    rankings: BTreeMap<&'static str, RankingOutput>,
    source_stats: BTreeMap<&'static str, SourceStats>,
    seed_seconds: f64,
    query_seconds: f64,
}

struct SeededCase {
    registry: AgentToolRegistry,
    interaction: InteractionSession,
    activity_turns: HashMap<String, TurnLocator>,
    note_turns: HashMap<String, Vec<TurnLocator>>,
    seeded_turns: usize,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let input_path = parse_input_path()?;
    let bytes = fs::read(&input_path)
        .map_err(|source| format!("read memory-tools case {}: {source}", input_path.display()))?;
    let input: CaseInput = serde_json::from_slice(&bytes)
        .map_err(|source| format!("parse memory-tools case {}: {source}", input_path.display()))?;
    validate_input(&input)?;

    let seed_started = Instant::now();
    let seeded = seed_case(&input)?;
    let seed_seconds = seed_started.elapsed().as_secs_f64();

    let query_started = Instant::now();
    let (scores, history_stats, memory_stats) = query_tools(&input, &seeded)?;
    let query_seconds = query_started.elapsed().as_secs_f64();

    let mut rankings = BTreeMap::new();
    rankings.insert(
        "combined",
        rankings_for_source(&scores, None, input.max_ranked_results),
    );
    rankings.insert(
        "session_history_search",
        rankings_for_source(
            &scores,
            Some(RetrievalSource::SessionHistory),
            input.max_ranked_results,
        ),
    );
    rankings.insert(
        "memory_search",
        rankings_for_source(
            &scores,
            Some(RetrievalSource::MemoryNotes),
            input.max_ranked_results,
        ),
    );
    let mut source_stats = BTreeMap::new();
    source_stats.insert("session_history_search", history_stats);
    source_stats.insert("memory_search", memory_stats);

    let output = CaseOutput {
        schema: OUTPUT_SCHEMA,
        case_id: input.case_id,
        query_terms: input.queries,
        seeded_sessions: input.sessions.len(),
        seeded_turns: seeded.seeded_turns,
        seeded_memory_notes: seeded.note_turns.len(),
        max_ranked_results: input.max_ranked_results,
        rankings,
        source_stats,
        seed_seconds,
        query_seconds,
    };
    println!(
        "{}",
        serde_json::to_string(&output)
            .map_err(|source| format!("encode memory-tools result: {source}"))?
    );
    Ok(())
}

fn parse_input_path() -> Result<PathBuf, String> {
    let mut args = env::args_os().skip(1);
    let mut input = None;
    while let Some(argument) = args.next() {
        if argument == "--input" {
            input = Some(
                args.next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "--input requires a path".to_string())?,
            );
        } else {
            return Err(format!(
                "unexpected argument {}",
                argument.to_string_lossy()
            ));
        }
    }
    input.ok_or_else(|| "usage: sinomo-memory-tools-bench --input <case.json>".to_string())
}

fn validate_input(input: &CaseInput) -> Result<(), String> {
    if input.schema != INPUT_SCHEMA {
        return Err(format!(
            "unsupported memory-tools case schema {}",
            input.schema
        ));
    }
    if input.case_id.trim().is_empty() {
        return Err("memory-tools case id is empty".to_string());
    }
    if input.queries.is_empty() || input.queries.iter().any(|query| query.trim().is_empty()) {
        return Err("memory-tools case requires non-empty query terms".to_string());
    }
    if input.max_ranked_results == 0 {
        return Err("memory-tools case requires a positive ranking limit".to_string());
    }
    if input.sessions.is_empty() {
        return Err("memory-tools case contains no sessions".to_string());
    }
    if input
        .sessions
        .iter()
        .any(|session| session.turns.is_empty())
    {
        return Err("memory-tools case contains an empty session".to_string());
    }
    Ok(())
}

fn seed_case(input: &CaseInput) -> Result<SeededCase, String> {
    let vfs = VfsHandle::memory();
    let activity_store = AgentTaskActivityProjectionStore::from_vfs(vfs.clone());
    let memory_backend = VfsMemoryNoteBackend::from_vfs(vfs.clone(), AGENT_KEY)
        .map_err(|source| format!("configure memory-note VFS: {source}"))?;
    let registry = AgentToolRegistry::new();
    memory_notes::register_tools(
        &registry,
        MemoryNoteAgentToolConfig::from_backend(memory_backend),
    );
    register_agent_history_search_tool_with_vfs(&registry, vfs, AGENT_KEY)
        .map_err(|source| format!("register history search tool: {source}"))?;
    let interaction = InteractionSession::new(
        SessionId::from_legacy_named(format!("memory-tools:session:{}", input.case_id))
            .map_err(|source| format!("create benchmark session id: {source}"))?,
        NodeId::from_legacy_named("memory-tools-benchmark-node")
            .map_err(|source| format!("create benchmark node id: {source}"))?,
        InteractionSurface::new(
            "memory-tools-benchmark",
            InteractionSurfaceKind::CommandLine,
        ),
    );

    let mut activity_turns = HashMap::new();
    let mut note_turns = HashMap::new();
    let mut sequence = 0_i64;
    for (session_index, session) in input.sessions.iter().enumerate() {
        let note_path = format!("sessions/session-{session_index:04}.md");
        let mut note_lines = Vec::with_capacity(session.turns.len());
        let mut locators = Vec::with_capacity(session.turns.len());
        for (turn_index, turn) in session.turns.iter().enumerate() {
            let locator = TurnLocator {
                session_id: session.session_id.clone(),
                turn_id: format!("{}::turn-{turn_index:06}", session.session_id),
            };
            let activity_id = TaskId::from_legacy_named(format!(
                "memory-tools:{}:{session_index}:{turn_index}",
                input.case_id
            ))
            .map_err(|source| format!("create activity id: {source}"))?;
            let marker = format!("[[longmemeval:{session_index}:{turn_index}]]");
            let content = flatten_text(&turn.content);
            let summary = truncate_utf8(
                &format!("{marker} [{}] {}: {content}", session.date, turn.role),
                512,
            );
            let activity = AgentTaskActivityProjection {
                activity_id: activity_id.clone(),
                owner_agent_key: AGENT_KEY.to_string(),
                activity_kind: "longmemeval_turn".to_string(),
                summary,
                status: AgentTaskLifecycleStatus::Completed,
                progress_summary: Some("completed".to_string()),
                updated_at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(sequence),
            };
            activity_store
                .save_activity(&activity)
                .map_err(|source| format!("seed historical activity: {source}"))?;
            activity_turns.insert(activity_id.to_string(), locator.clone());
            note_lines.push(format!(
                "{marker} [{}] {}: {content}",
                session.date, turn.role
            ));
            locators.push(locator);
            sequence = sequence.saturating_add(1);
        }
        let markdown = format!("{}\n", note_lines.join("\n"));
        execute_tool(
            &registry,
            &interaction,
            MEMORY_WRITE_TOOL,
            serde_json::json!({"path": note_path, "markdown": markdown}),
        )?;
        note_turns.insert(note_path, locators);
    }

    Ok(SeededCase {
        registry,
        interaction,
        activity_turns,
        note_turns,
        seeded_turns: sequence as usize,
    })
}

fn query_tools(
    input: &CaseInput,
    seeded: &SeededCase,
) -> Result<(HashMap<TurnLocator, HitScore>, SourceStats, SourceStats), String> {
    let mut scores = HashMap::new();
    let mut next_seen = 0_usize;
    let mut history_stats = SourceStats::default();
    let mut memory_stats = SourceStats::default();
    for (query_index, query) in input.queries.iter().enumerate() {
        query_history(
            seeded,
            query,
            query_index,
            &mut scores,
            &mut next_seen,
            &mut history_stats,
        )?;
        query_memory_notes(
            seeded,
            query,
            query_index,
            &mut scores,
            &mut next_seen,
            &mut memory_stats,
        )?;
    }
    Ok((scores, history_stats, memory_stats))
}

fn query_history(
    seeded: &SeededCase,
    query: &str,
    query_index: usize,
    scores: &mut HashMap<TurnLocator, HitScore>,
    next_seen: &mut usize,
    stats: &mut SourceStats,
) -> Result<(), String> {
    let mut cursor: Option<String> = None;
    let mut rank = 0_usize;
    for _ in 0..MAX_PAGES_PER_QUERY {
        let mut input = serde_json::json!({"query": query, "limit": 50});
        if let Some(cursor) = &cursor {
            input["cursor"] = serde_json::Value::String(cursor.clone());
        }
        let output = execute_tool(&seeded.registry, &seeded.interaction, HISTORY_TOOL, input)?;
        stats.tool_calls = stats.tool_calls.saturating_add(1);
        let matches = output
            .get("matches")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "session_history_search output has no matches array".to_string())?;
        stats.returned_matches = stats.returned_matches.saturating_add(matches.len());
        for matched in matches {
            let Some(item_id) = matched.get("item_id").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(locator) = seeded.activity_turns.get(item_id) else {
                continue;
            };
            rank = rank.saturating_add(1);
            record_hit(
                scores,
                locator,
                query_index,
                RetrievalSource::SessionHistory,
                rank,
                next_seen,
            );
        }
        if output.get("partial").and_then(serde_json::Value::as_bool) == Some(true) {
            stats.partial_pages = stats.partial_pages.saturating_add(1);
        }
        let complete = output
            .get("complete")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        cursor = output
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if complete {
            return Ok(());
        }
        if cursor.is_none() {
            return Err(
                "session_history_search returned an incomplete page without a cursor".into(),
            );
        }
    }
    Err("session_history_search exceeded its pagination bound".to_string())
}

fn query_memory_notes(
    seeded: &SeededCase,
    query: &str,
    query_index: usize,
    scores: &mut HashMap<TurnLocator, HitScore>,
    next_seen: &mut usize,
    stats: &mut SourceStats,
) -> Result<(), String> {
    let mut cursor: Option<String> = None;
    let mut rank = 0_usize;
    for _ in 0..MAX_PAGES_PER_QUERY {
        let mut input = serde_json::json!({"query": query, "max_results": 100});
        if let Some(cursor) = &cursor {
            input["cursor"] = serde_json::Value::String(cursor.clone());
        }
        let output = execute_tool(
            &seeded.registry,
            &seeded.interaction,
            MEMORY_SEARCH_TOOL,
            input,
        )?;
        stats.tool_calls = stats.tool_calls.saturating_add(1);
        let matches = output
            .get("matches")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "memory_search output has no matches array".to_string())?;
        stats.returned_matches = stats.returned_matches.saturating_add(matches.len());
        for matched in matches {
            let Some(path) = matched.get("path").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(line) = matched.get("line").and_then(serde_json::Value::as_u64) else {
                continue;
            };
            let Some(locator) = seeded.note_turns.get(path).and_then(|turns| {
                line.checked_sub(1)
                    .and_then(|index| turns.get(index as usize))
            }) else {
                continue;
            };
            rank = rank.saturating_add(1);
            record_hit(
                scores,
                locator,
                query_index,
                RetrievalSource::MemoryNotes,
                rank,
                next_seen,
            );
        }
        if output.get("truncated").and_then(serde_json::Value::as_bool) == Some(true) {
            stats.truncated_pages = stats.truncated_pages.saturating_add(1);
        }
        cursor = output
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if cursor.is_none() {
            return Ok(());
        }
    }
    Err("memory_search exceeded its pagination bound".to_string())
}

fn execute_tool(
    registry: &AgentToolRegistry,
    interaction: &InteractionSession,
    tool: &str,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let outcome = registry.execute(
        interaction,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: tool.to_string(),
            input,
            r#async: false,
            summary: tool.to_string(),
        },
    );
    if outcome.status != AgentToolStatus::Succeeded {
        return Err(format!(
            "tool {tool} returned {:?}: {} ({})",
            outcome.status, outcome.summary, outcome.output
        ));
    }
    Ok(outcome.output)
}

fn record_hit(
    scores: &mut HashMap<TurnLocator, HitScore>,
    locator: &TurnLocator,
    query_index: usize,
    source: RetrievalSource,
    rank: usize,
    next_seen: &mut usize,
) {
    let score = scores.entry(locator.clone()).or_insert_with(|| {
        let first_seen = *next_seen;
        *next_seen = (*next_seen).saturating_add(1);
        HitScore {
            first_seen,
            ..HitScore::default()
        }
    });
    let source_score = score.source_scores.entry(source).or_default();
    source_score.query_indexes.insert(query_index);
    source_score.reciprocal_rank += 1.0 / (RRF_K + rank as f64);
}

fn rankings_for_source(
    scores: &HashMap<TurnLocator, HitScore>,
    source: Option<RetrievalSource>,
    max_results: usize,
) -> RankingOutput {
    let mut turns = scores
        .iter()
        .filter(|(_, score)| source.is_none_or(|source| score.source_scores.contains_key(&source)))
        .collect::<Vec<_>>();
    turns.sort_by(|(left_locator, left), (right_locator, right)| {
        let left_queries = query_indexes_for_source(left, source);
        let right_queries = query_indexes_for_source(right, source);
        let left_sources = source_count(left, source);
        let right_sources = source_count(right, source);
        let left_rrf = reciprocal_rank_for_source(left, source);
        let right_rrf = reciprocal_rank_for_source(right, source);
        right_queries
            .len()
            .cmp(&left_queries.len())
            .then_with(|| right_sources.cmp(&left_sources))
            .then_with(|| right_rrf.partial_cmp(&left_rrf).unwrap_or(Ordering::Equal))
            .then_with(|| left.first_seen.cmp(&right.first_seen))
            .then_with(|| left_locator.turn_id.cmp(&right_locator.turn_id))
    });
    let ranked_turn_ids = turns
        .iter()
        .take(max_results)
        .map(|(locator, _)| locator.turn_id.clone())
        .collect::<Vec<_>>();
    let mut seen_sessions = BTreeSet::new();
    let ranked_session_ids = turns
        .into_iter()
        .filter_map(|(locator, _)| {
            if seen_sessions.insert(locator.session_id.clone()) {
                Some(locator.session_id.clone())
            } else {
                None
            }
        })
        .take(max_results)
        .collect();
    RankingOutput {
        ranked_session_ids,
        ranked_turn_ids,
    }
}

fn query_indexes_for_source(score: &HitScore, source: Option<RetrievalSource>) -> BTreeSet<usize> {
    match source {
        Some(source) => score
            .source_scores
            .get(&source)
            .map(|score| score.query_indexes.clone())
            .unwrap_or_default(),
        None => score
            .source_scores
            .values()
            .flat_map(|score| score.query_indexes.iter().copied())
            .collect(),
    }
}

fn source_count(score: &HitScore, source: Option<RetrievalSource>) -> usize {
    match source {
        Some(source) => usize::from(score.source_scores.contains_key(&source)),
        None => score.source_scores.len(),
    }
}

fn reciprocal_rank_for_source(score: &HitScore, source: Option<RetrievalSource>) -> f64 {
    match source {
        Some(source) => score
            .source_scores
            .get(&source)
            .map_or(0.0, |score| score.reciprocal_rank),
        None => score
            .source_scores
            .values()
            .map(|score| score.reciprocal_rank)
            .sum(),
    }
}

fn flatten_text(value: &str) -> String {
    value
        .replace('\0', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_ranking_prefers_more_query_terms_then_more_sources() {
        let first = TurnLocator {
            session_id: "s1".to_string(),
            turn_id: "s1:0".to_string(),
        };
        let second = TurnLocator {
            session_id: "s2".to_string(),
            turn_id: "s2:0".to_string(),
        };
        let mut scores = HashMap::new();
        let mut next_seen = 0;
        record_hit(
            &mut scores,
            &first,
            0,
            RetrievalSource::MemoryNotes,
            1,
            &mut next_seen,
        );
        record_hit(
            &mut scores,
            &second,
            0,
            RetrievalSource::MemoryNotes,
            1,
            &mut next_seen,
        );
        record_hit(
            &mut scores,
            &second,
            1,
            RetrievalSource::SessionHistory,
            4,
            &mut next_seen,
        );
        let ranking = rankings_for_source(&scores, None, 50);
        assert_eq!(ranking.ranked_turn_ids, ["s2:0", "s1:0"]);
        assert_eq!(ranking.ranked_session_ids, ["s2", "s1"]);
        let bounded = rankings_for_source(&scores, None, 1);
        assert_eq!(bounded.ranked_turn_ids, ["s2:0"]);
        assert_eq!(bounded.ranked_session_ids, ["s2"]);
    }

    #[test]
    fn source_rankings_only_use_that_sources_scores() {
        let first = TurnLocator {
            session_id: "s1".to_string(),
            turn_id: "s1:0".to_string(),
        };
        let second = TurnLocator {
            session_id: "s2".to_string(),
            turn_id: "s2:0".to_string(),
        };
        let mut scores = HashMap::new();
        let mut next_seen = 0;
        record_hit(
            &mut scores,
            &first,
            0,
            RetrievalSource::SessionHistory,
            1,
            &mut next_seen,
        );
        record_hit(
            &mut scores,
            &first,
            1,
            RetrievalSource::MemoryNotes,
            1,
            &mut next_seen,
        );
        record_hit(
            &mut scores,
            &second,
            0,
            RetrievalSource::SessionHistory,
            2,
            &mut next_seen,
        );
        record_hit(
            &mut scores,
            &second,
            1,
            RetrievalSource::SessionHistory,
            3,
            &mut next_seen,
        );

        let ranking = rankings_for_source(&scores, Some(RetrievalSource::SessionHistory), 50);
        assert_eq!(ranking.ranked_turn_ids, ["s2:0", "s1:0"]);
    }

    #[test]
    fn driver_queries_real_memory_tools_over_an_in_memory_vfs() {
        let input = CaseInput {
            schema: INPUT_SCHEMA.to_string(),
            case_id: "driver-smoke".to_string(),
            queries: vec!["cobalt".to_string()],
            max_ranked_results: 50,
            sessions: vec![
                CaseSession {
                    session_id: "answer-session".to_string(),
                    date: "2026/01/03".to_string(),
                    turns: vec![CaseTurn {
                        role: "user".to_string(),
                        content: "We selected cobalt for the launch banner.".to_string(),
                    }],
                },
                CaseSession {
                    session_id: "noise-session".to_string(),
                    date: "2026/01/04".to_string(),
                    turns: vec![CaseTurn {
                        role: "assistant".to_string(),
                        content: "The launch remains scheduled for Friday.".to_string(),
                    }],
                },
            ],
        };

        let seeded = seed_case(&input).expect("seed VFS-backed memory tools");
        let (scores, history_stats, memory_stats) =
            query_tools(&input, &seeded).expect("query VFS-backed memory tools");
        let combined = rankings_for_source(&scores, None, input.max_ranked_results);
        let history = rankings_for_source(
            &scores,
            Some(RetrievalSource::SessionHistory),
            input.max_ranked_results,
        );
        let memory = rankings_for_source(
            &scores,
            Some(RetrievalSource::MemoryNotes),
            input.max_ranked_results,
        );

        assert_eq!(
            combined.ranked_session_ids.first(),
            Some(&"answer-session".to_string())
        );
        assert_eq!(
            combined.ranked_turn_ids.first(),
            Some(&"answer-session::turn-000000".to_string())
        );
        assert_eq!(history.ranked_session_ids, ["answer-session"]);
        assert_eq!(memory.ranked_session_ids, ["answer-session"]);
        assert!(history_stats.tool_calls > 0);
        assert!(memory_stats.tool_calls > 0);
    }
}
