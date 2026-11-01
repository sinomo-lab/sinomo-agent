//! Runtime checks for real agent-home and memory-note consumers of daemon SIFS.
//! Test agents have isolated roaming homes; failures retain their original data.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sinomo_agent::{
    AgentCancellationToken, MAX_MEMORY_NOTE_READ_BYTES, MAX_MEMORY_NOTE_SEARCH_OUTPUT_BYTES,
    MemoryNoteBackend, MemoryNoteBackendError, MemoryNoteDocument, MemoryNotePath,
    MemoryNoteReadRequest, MemoryNoteSearchRequest, MemoryNoteVersion, MemoryNoteWritePrecondition,
    MemoryNoteWriteRequest, VfsMemoryNoteBackend, agent_home_sifs_path_for_id,
};
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use sinomo_runtime::vfs::{VfsHandle, VfsWriteOptions};

const READ_WAIT: Duration = Duration::from_millis(100);
const MAX_SEARCH_PAGES: usize = 256;

pub fn register_sifs_consumer_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(memory_write())
        .register(memory_expect())
        .register(memory_search())
        .register(memory_write_guards())
        .register(home_write())
        .register(home_expect())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn memory_write(ctx) {
        id: "cluster.sifs.memory-write",
        summary: "Write an isolated agent memory note through its normal daemon SIFS backend",
        tags: ["cluster-sifs-consumers", "memory-notes", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_memory_write(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn memory_expect(ctx) {
        id: "cluster.sifs.memory-expect",
        summary: "Read exact memory-note bytes and version through the normal agent backend",
        tags: ["cluster-sifs-consumers", "memory-notes", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_memory_expect(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn memory_search(ctx) {
        id: "cluster.sifs.memory-search",
        summary: "Search real agent memory notes with advancing bounded continuation pages",
        tags: ["cluster-sifs-consumers", "memory-notes", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_memory_search(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn memory_write_guards(ctx) {
        id: "cluster.sifs.memory-write-guards",
        summary: "Verify typed memory create/CAS rejection, unchanged data and another agent's isolation",
        tags: ["cluster-sifs-consumers", "memory-notes", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_memory_write_guards(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn home_write(ctx) {
        id: "cluster.sifs.home-write",
        summary: "Create one exclusive versioned file in an isolated roaming agent workspace",
        tags: ["cluster-sifs-consumers", "agent-home", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_home_write(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn home_expect(ctx) {
        id: "cluster.sifs.home-expect",
        summary: "Read exact roaming workspace bytes, object identity and version through daemon SIFS",
        tags: ["cluster-sifs-consumers", "agent-home", "dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_home_expect(ctx) }
    }
}

fn invalid(name: &str, detail: impl Into<String>) -> SinomoTestError {
    SinomoTestError::InvalidParameter {
        name: name.into(),
        detail: detail.into(),
    }
}

fn isolated_agent(value: &str) -> Result<String, SinomoTestError> {
    if !value.starts_with("cluster-test-")
        || value.len() <= "cluster-test-".len()
        || value.len() > 120
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(invalid(
            "agent",
            "must be an isolated cluster-test-* lowercase alphanumeric/hyphen identity of at most 120 bytes",
        ));
    }
    Ok(value.to_string())
}

fn memory_path(ctx: &SinomoTestContext) -> Result<MemoryNotePath, SinomoTestError> {
    MemoryNotePath::new(ctx.required_parameter("path")?.to_string())
        .map_err(|error| invalid("path", error.to_string()))
}

fn version(value: &str, name: &str) -> Result<MemoryNoteVersion, SinomoTestError> {
    MemoryNoteVersion::new(value.to_string()).map_err(|error| invalid(name, error.to_string()))
}

fn optional_version(ctx: &SinomoTestContext) -> Result<Option<MemoryNoteVersion>, SinomoTestError> {
    ctx.parameter("version")
        .map(|value| version(value, "version"))
        .transpose()
}

fn bounded_text(ctx: &SinomoTestContext, name: &str) -> Result<String, SinomoTestError> {
    let value = ctx.required_parameter(name)?.to_string();
    if value.len() >= MAX_MEMORY_NOTE_READ_BYTES {
        return Err(invalid(
            name,
            format!(
                "must be smaller than {MAX_MEMORY_NOTE_READ_BYTES} bytes for complete bounded verification"
            ),
        ));
    }
    Ok(value)
}

fn read_deadline(ctx: &SinomoTestContext) -> Result<Instant, SinomoTestError> {
    let seconds = ctx
        .parameter("timeout-seconds")
        .unwrap_or("30")
        .parse::<u64>()
        .map_err(|error| invalid("timeout-seconds", error.to_string()))?;
    if !(1..=300).contains(&seconds) {
        return Err(invalid(
            "timeout-seconds",
            "must be between 1 and 300 seconds",
        ));
    }
    Ok(Instant::now() + Duration::from_secs(seconds))
}

fn backend(ctx: &mut SinomoTestContext) -> Result<VfsMemoryNoteBackend, SinomoTestError> {
    let agent = isolated_agent(ctx.required_parameter("agent")?)?;
    let backend = VfsMemoryNoteBackend::new(&SinomoDataDir::new(ctx.data_dir()), &agent)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    ctx.record("agent", agent);
    ctx.record("root", backend.root());
    ctx.record("fixture_retained", true);
    Ok(backend)
}

fn read_note(
    backend: &VfsMemoryNoteBackend,
    path: &MemoryNotePath,
) -> Result<MemoryNoteDocument, MemoryNoteBackendError> {
    backend.read(
        MemoryNoteReadRequest {
            path: path.clone(),
            offset: 0,
            max_bytes: MAX_MEMORY_NOTE_READ_BYTES,
            expected_version: None,
        },
        &AgentCancellationToken::new(),
    )
}

fn record_note(ctx: &mut SinomoTestContext, document: &MemoryNoteDocument) {
    ctx.record("path", document.path.as_str());
    ctx.record("version", document.version.as_str());
    ctx.record("bytes", document.markdown.len());
    ctx.record(
        "content_blake3",
        blake3::hash(document.markdown.as_bytes())
            .to_hex()
            .to_string(),
    );
    ctx.record("truncated", document.truncated);
}

fn pause_read(deadline: Instant) -> bool {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return false;
    }
    std::thread::sleep(READ_WAIT.min(remaining));
    true
}

fn run_memory_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = memory_path(ctx)?;
    let markdown = bounded_text(ctx, "markdown")?;
    let precondition = match optional_version(ctx)? {
        Some(expected_version) => {
            MemoryNoteWritePrecondition::ReplaceIfVersion { expected_version }
        }
        None => MemoryNoteWritePrecondition::CreateNew,
    };
    let backend = backend(ctx)?;
    let receipt = backend
        .write(
            MemoryNoteWriteRequest {
                path,
                markdown,
                precondition,
            },
            &AgentCancellationToken::new(),
        )
        .map_err(|error| {
            SinomoTestError::failed(format!(
                "memory write failed; no retry was attempted: {error}"
            ))
        })?;
    ctx.record("path", receipt.path.as_str());
    ctx.record("version", receipt.version.as_str());
    ctx.record("created", receipt.created);
    Ok(SinomoTestOutcome::passed(
        "normal agent memory write returned a versioned SIFS receipt",
    ))
}

fn run_memory_expect(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = memory_path(ctx)?;
    let markdown = bounded_text(ctx, "markdown")?;
    let expected_version = optional_version(ctx)?;
    let deadline = read_deadline(ctx)?;
    let backend = backend(ctx)?;
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        let detail = match read_note(&backend, &path) {
            Ok(document) => {
                record_note(ctx, &document);
                if !document.truncated
                    && document.markdown == markdown
                    && expected_version
                        .as_ref()
                        .is_none_or(|expected| expected == &document.version)
                    && Instant::now() <= deadline
                {
                    ctx.record("read_attempts", attempts);
                    return Ok(SinomoTestOutcome::passed(
                        "exact agent memory bytes and version are readable through daemon SIFS",
                    ));
                }
                "memory bytes/version differ or complete read exceeded original deadline"
                    .to_string()
            }
            Err(error) => error.to_string(),
        };
        if !pause_read(deadline) {
            ctx.record("read_attempts", attempts);
            ctx.record("last_read_state", &detail);
            return Ok(SinomoTestOutcome::failed(format!(
                "memory read did not converge within its original deadline: {detail}"
            )));
        }
    }
}

fn run_memory_search(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = memory_path(ctx)?;
    let query = ctx.required_parameter("query")?.to_string();
    if query.trim().is_empty() {
        return Err(invalid("query", "must not be empty"));
    }
    let expected_version = optional_version(ctx)?;
    let deadline = read_deadline(ctx)?;
    let backend = backend(ctx)?;
    let cancellation = AgentCancellationToken::new();
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    let mut found = false;
    let mut pages = Vec::new();
    loop {
        if Instant::now() >= deadline || pages.len() >= MAX_SEARCH_PAGES {
            ctx.record("pages", &pages);
            return Ok(SinomoTestOutcome::failed(
                "memory search exceeded its original deadline or bounded page count",
            ));
        }
        let page = match backend.search(
            MemoryNoteSearchRequest {
                query: query.clone(),
                cursor: cursor.clone(),
                max_results: 1,
                max_output_bytes: MAX_MEMORY_NOTE_SEARCH_OUTPUT_BYTES,
            },
            &cancellation,
        ) {
            Ok(page) => page,
            Err(error) => {
                if cursor.is_some() || !pause_read(deadline) {
                    return Err(SinomoTestError::failed(format!(
                        "memory search failed without replacing its continuation: {error}"
                    )));
                }
                continue;
            }
        };
        if page.matches.len() > 1 {
            return Ok(SinomoTestOutcome::failed(
                "memory search ignored its one-result page bound",
            ));
        }
        for hit in &page.matches {
            if hit.path == path
                && expected_version
                    .as_ref()
                    .is_none_or(|expected| expected == &hit.version)
            {
                found = true;
            }
        }
        pages.push(serde_json::json!({ "matches": page.matches.iter().map(|hit| serde_json::json!({
            "path": hit.path.as_str(), "version": hit.version.as_str(), "line": hit.line,
        })).collect::<Vec<_>>(), "has_next": page.next_cursor.is_some(), "truncated": page.truncated }));
        match page.next_cursor {
            Some(next) => {
                if !seen.insert(next.as_str().to_string()) {
                    ctx.record("pages", &pages);
                    return Ok(SinomoTestOutcome::failed(
                        "memory search repeated a continuation instead of making progress",
                    ));
                }
                cursor = Some(next);
            }
            None => {
                ctx.record("pages", &pages);
                ctx.record("page_count", pages.len());
                ctx.record("continuation_count", seen.len());
                return Ok(if found && Instant::now() <= deadline {
                    SinomoTestOutcome::passed(
                        "bounded memory search reached the requested note and exhausted advancing continuations",
                    )
                } else {
                    SinomoTestOutcome::failed(
                        "memory search finished without the requested note/version",
                    )
                });
            }
        }
    }
}

fn run_memory_write_guards(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = memory_path(ctx)?;
    let stale_version = version(ctx.required_parameter("stale-version")?, "stale-version")?;
    let agent = isolated_agent(ctx.required_parameter("agent")?)?;
    let other = isolated_agent(
        &ctx.parameter("other-agent")
            .map(str::to_string)
            .unwrap_or_else(|| format!("{agent}-other")),
    )?;
    if agent == other {
        return Err(invalid(
            "other-agent",
            "must differ from the original agent",
        ));
    }
    let backend = backend(ctx)?;
    let before =
        read_note(&backend, &path).map_err(|error| SinomoTestError::failed(error.to_string()))?;
    record_note(ctx, &before);
    ctx.record("stale_version", stale_version.as_str());
    if before.truncated || before.version == stale_version {
        return Err(invalid(
            "stale-version",
            "must name an actual earlier version different from the complete current note",
        ));
    }
    let cancellation = AgentCancellationToken::new();
    let duplicate = backend.write(
        MemoryNoteWriteRequest {
            path: path.clone(),
            markdown: "duplicate create must not overwrite".into(),
            precondition: MemoryNoteWritePrecondition::CreateNew,
        },
        &cancellation,
    );
    if !matches!(duplicate, Err(MemoryNoteBackendError::AlreadyExists { path: ref rejected }) if rejected == &path)
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "duplicate memory create did not return typed AlreadyExists: {duplicate:?}"
        )));
    }
    let stale = backend.write(
        MemoryNoteWriteRequest {
            path: path.clone(),
            markdown: "stale CAS must not overwrite".into(),
            precondition: MemoryNoteWritePrecondition::ReplaceIfVersion {
                expected_version: stale_version,
            },
        },
        &cancellation,
    );
    if !matches!(stale, Err(MemoryNoteBackendError::Conflict { path: ref rejected, current_version: ref current }) if rejected == &path && current.as_ref().is_none_or(|version| version == &before.version))
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "stale memory replacement did not return exact typed Conflict: {stale:?}"
        )));
    }
    let after =
        read_note(&backend, &path).map_err(|error| SinomoTestError::failed(error.to_string()))?;
    if before != after {
        return Ok(SinomoTestOutcome::failed(
            "rejected memory writes changed bytes or version",
        ));
    }
    let other_backend = VfsMemoryNoteBackend::new(&SinomoDataDir::new(ctx.data_dir()), &other)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let isolated = read_note(&other_backend, &path);
    if !matches!(isolated, Err(MemoryNoteBackendError::NotFound { path: ref rejected }) if rejected == &path)
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "another agent did not receive typed note absence: {isolated:?}"
        )));
    }
    record_note(ctx, &after);
    ctx.record("other_agent", other);
    ctx.record("other_root", other_backend.root());
    ctx.record("duplicate_create", "AlreadyExists");
    ctx.record("stale_replace", "Conflict");
    ctx.record("unchanged", true);
    Ok(SinomoTestOutcome::passed(
        "duplicate create and stale CAS reject without mutation, and another agent cannot read this note",
    ))
}

fn home_path(ctx: &SinomoTestContext) -> Result<String, SinomoTestError> {
    let path = ctx.required_parameter("path")?;
    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['\\', '\0'])
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(invalid(
            "path",
            "must be a normalized relative workspace file path",
        ));
    }
    Ok(format!("workspace/{path}"))
}

fn home(ctx: &mut SinomoTestContext, create: bool) -> Result<VfsHandle, SinomoTestError> {
    let agent = isolated_agent(ctx.required_parameter("agent")?)?;
    let root = agent_home_sifs_path_for_id(&agent);
    let client =
        DaemonApiClient::connect().map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let filesystem = sinomo_runtime::vfs::discovered_daemon_sifs(
        SinomoDataDir::new(ctx.data_dir()),
        client.address(),
    )
    .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    if create {
        filesystem
            .create_dir_all_beneath_no_links(&root)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    }
    let scoped = filesystem
        .scoped(&root)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    ctx.record("agent", agent);
    ctx.record("root", root);
    ctx.record("transport", scoped.backend_name());
    ctx.record("fixture_retained", true);
    Ok(scoped)
}

fn run_home_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = home_path(ctx)?;
    let payload = bounded_text(ctx, "payload")?;
    let scoped = home(ctx, true)?;
    let parent = path.rsplit_once('/').unwrap().0;
    scoped
        .create_dir_all_beneath_no_links(parent)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let receipt = scoped
        .create_new_versioned_beneath_no_links(
            &path,
            payload.as_bytes(),
            VfsWriteOptions::default().create_new(),
        )
        .map_err(|error| {
            SinomoTestError::failed(format!(
                "home write failed; no retry was attempted: {error}"
            ))
        })?;
    if receipt.object_id.is_none() || receipt.version.is_none() {
        return Ok(SinomoTestOutcome::failed(
            "roaming home write returned no committed object identity/version",
        ));
    }
    ctx.record("path", path);
    ctx.record("object_id", receipt.object_id);
    ctx.record("version", receipt.version);
    ctx.record("bytes", payload.len());
    Ok(SinomoTestOutcome::passed(
        "exclusive roaming workspace create returned its actual DFS object/version",
    ))
}

fn run_home_expect(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = home_path(ctx)?;
    let payload = bounded_text(ctx, "payload")?;
    let expected_version = ctx.parameter("version").map(str::to_string);
    let expected_object = ctx.parameter("object-id").map(str::to_string);
    let deadline = read_deadline(ctx)?;
    let scoped = home(ctx, false)?;
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        let detail = match scoped.read_prefix_beneath_no_links(&path, payload.len() as u64 + 1) {
            Ok(read) => {
                ctx.record("path", &path);
                ctx.record("object_id", &read.metadata.object_id);
                ctx.record("version", &read.metadata.version);
                ctx.record("bytes", read.bytes.len());
                ctx.record("truncated", read.truncated);
                if !read.truncated
                    && read.bytes == payload.as_bytes()
                    && read.metadata.object_id.is_some()
                    && read.metadata.version.is_some()
                    && expected_version
                        .as_ref()
                        .is_none_or(|version| read.metadata.version.as_ref() == Some(version))
                    && expected_object
                        .as_ref()
                        .is_none_or(|object| read.metadata.object_id.as_ref() == Some(object))
                    && Instant::now() <= deadline
                {
                    ctx.record("read_attempts", attempts);
                    return Ok(SinomoTestOutcome::passed(
                        "exact roaming workspace bytes and object/version are visible through daemon SIFS",
                    ));
                }
                "home payload/object/version differ or read exceeded original deadline".to_string()
            }
            Err(error) => error.to_string(),
        };
        if !pause_read(deadline) {
            ctx.record("read_attempts", attempts);
            ctx.record("last_read_state", &detail);
            return Ok(SinomoTestOutcome::failed(format!(
                "home read did not converge within its original deadline: {detail}"
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_probes_reject_product_agents_and_ambient_paths() {
        assert!(isolated_agent("cluster-test-dfs-consumer").is_ok());
        for agent in [
            "main-agent",
            "cluster-test-",
            "cluster-test-../foreign",
            "cluster-test-UPPER",
        ] {
            assert!(isolated_agent(agent).is_err(), "{agent}");
        }
        for path in [
            "/outside",
            "../outside",
            "part/../outside",
            "part//note",
            "part\\note",
        ] {
            let ctx = SinomoTestContext::new("test-data").with_parameter("path", path);
            assert!(home_path(&ctx).is_err(), "{path}");
        }
        let ctx = SinomoTestContext::new("test-data").with_parameter("path", "notes/roaming.txt");
        assert_eq!(home_path(&ctx).unwrap(), "workspace/notes/roaming.txt");
    }
}
