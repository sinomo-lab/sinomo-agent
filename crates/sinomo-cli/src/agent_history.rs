use crate::app_api::{
    AgentChannelHistoryMetadata, SIFS_AGENT_CHANNELS_ROOT, SifsAgentChannelStore,
};
use crate::error::CliError;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sinomo_agent::{
    AgentCancellationToken, AgentChannelAuthor, AgentChannelKind, AgentChannelStoreError,
    AgentTaskActivityProjection, AgentTaskActivityProjectionStore, AgentTaskLifecycleStatus,
    AgentToolRegistry, FileAgentSessionStore, SessionHistoryActivityKind,
    SessionHistorySearchBackend, SessionHistorySearchConsistency, SessionHistorySearchError,
    SessionHistorySearchMatch, SessionHistorySearchPage, SessionHistorySearchPrincipal,
    SessionHistorySearchRequest, SessionHistorySearchStats, register_session_history_search_tool,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{VfsDirectoryCursor, VfsError, VfsHandle, VfsObjectKind, mounted_sifs};
use std::sync::{Arc, Mutex};

// One immutable directory entry per page gives a continuation for the exact
// next record even when the current result page fills or an oversized body is
// skipped without being read.
const CHANNEL_MESSAGE_PAGE_SIZE: usize = 1;

#[cfg(test)]
std::thread_local! {
    static CHANNEL_HISTORY_AFTER_BODY_READ_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
struct ChannelHistoryAfterBodyReadHookGuard;

#[cfg(test)]
impl Drop for ChannelHistoryAfterBodyReadHookGuard {
    fn drop(&mut self) {
        CHANNEL_HISTORY_AFTER_BODY_READ_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
fn install_channel_history_after_body_read_hook(
    hook: impl FnOnce() + 'static,
) -> ChannelHistoryAfterBodyReadHookGuard {
    CHANNEL_HISTORY_AFTER_BODY_READ_HOOK.with(|slot| {
        let previous = slot.borrow_mut().replace(Box::new(hook));
        assert!(
            previous.is_none(),
            "channel-history test hook already installed"
        );
    });
    ChannelHistoryAfterBodyReadHookGuard
}

#[cfg(test)]
fn run_channel_history_after_body_read_hook() {
    CHANNEL_HISTORY_AFTER_BODY_READ_HOOK.with(|slot| {
        let hook = slot.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
    });
}

#[derive(Clone)]
pub(crate) struct VfsAgentHistorySearchBackend {
    sifs_data_dir: Option<SinomoDataDir>,
    history_vfs: Arc<Mutex<Option<VfsHandle>>>,
}

impl VfsAgentHistorySearchBackend {
    pub(crate) fn new(data_dir: SinomoDataDir) -> Self {
        Self {
            sifs_data_dir: Some(data_dir),
            history_vfs: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn from_vfs(history_vfs: VfsHandle) -> Self {
        Self {
            sifs_data_dir: None,
            history_vfs: Arc::new(Mutex::new(Some(history_vfs))),
        }
    }

    /// Open the roaming history view on first use rather than while assembling
    /// the agent registry. Recall tools remain part of every normal agent's
    /// default surface during a DFS outage and fail closed at invocation; a
    /// later call can retry after SIFS recovers.
    fn history_vfs(&self) -> Result<VfsHandle, SessionHistorySearchError> {
        let mut cached =
            self.history_vfs
                .lock()
                .map_err(|_| SessionHistorySearchError::Unavailable {
                    detail: "visible activity history is unavailable".to_string(),
                })?;
        if let Some(history_vfs) = cached.as_ref() {
            return Ok(history_vfs.clone());
        }
        let data_dir =
            self.sifs_data_dir
                .as_ref()
                .ok_or_else(|| SessionHistorySearchError::Unavailable {
                    detail: "injected visible activity-history VFS is unavailable".to_string(),
                })?;
        let history_vfs = mounted_sifs(Some(data_dir.root().to_path_buf())).map_err(|_| {
            SessionHistorySearchError::Unavailable {
                detail: "visible activity history is unavailable".to_string(),
            }
        })?;
        *cached = Some(history_vfs.clone());
        Ok(history_vfs)
    }
}

pub(crate) fn register_agent_history_search_tool(
    registry: &AgentToolRegistry,
    data_dir: &SinomoDataDir,
    agent_key: &str,
) -> Result<(), CliError> {
    let principal =
        SessionHistorySearchPrincipal::new(agent_key).map_err(|source| CliError::Parse {
            detail: format!("configure session-history tool principal: {source}"),
        })?;
    let backend = VfsAgentHistorySearchBackend::new(data_dir.clone());
    register_session_history_search_tool(registry, principal, Arc::new(backend));
    Ok(())
}

pub fn register_agent_history_search_tool_with_vfs(
    registry: &AgentToolRegistry,
    history_vfs: VfsHandle,
    agent_key: &str,
) -> Result<(), CliError> {
    let principal =
        SessionHistorySearchPrincipal::new(agent_key).map_err(|source| CliError::Parse {
            detail: format!("configure session-history tool principal: {source}"),
        })?;
    let backend = VfsAgentHistorySearchBackend::from_vfs(history_vfs);
    register_session_history_search_tool(registry, principal, Arc::new(backend));
    Ok(())
}

impl SessionHistorySearchBackend for VfsAgentHistorySearchBackend {
    fn search(
        &self,
        principal: &SessionHistorySearchPrincipal,
        request: &SessionHistorySearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<SessionHistorySearchPage, SessionHistorySearchError> {
        if cancellation.is_cancelled() {
            return Err(SessionHistorySearchError::Cancelled);
        }
        let history_vfs = self.history_vfs()?;
        let binding = cursor_binding(principal.agent_key(), &request.query);
        let mut position = parse_cursor(request.cursor.as_deref(), &binding)?;
        let mut matches = Vec::new();
        let mut scanned = SessionHistorySearchStats::default();
        let mut partial = false;
        let query = request.query.to_lowercase();

        if let HistoryCursor::Channel {
            mut channel_id,
            mut channel_cursor,
            mut record_source,
            mut record_cursor,
            mut record_entry,
            mut record_object_id,
            mut record_version,
            mut record_offset,
            mut byte_offset,
            mut discarding_line,
        } = position.clone()
        {
            let mut channel_after = decode_vfs_cursor(channel_cursor.as_deref())?;
            let mut record_after = decode_vfs_cursor(record_cursor.as_deref())?;
            'channels: loop {
                if cancellation.is_cancelled() {
                    return Err(SessionHistorySearchError::Cancelled);
                }
                if scanned.activities >= request.max_scanned_activities
                    || scanned.items >= request.max_scanned_items
                    || scanned.bytes >= request.max_scanned_bytes
                {
                    return continued_page(
                        matches,
                        HistoryCursor::Channel {
                            channel_id,
                            channel_cursor,
                            record_source,
                            record_cursor,
                            record_entry,
                            record_object_id,
                            record_version,
                            record_offset,
                            byte_offset,
                            discarding_line,
                        },
                        &binding,
                        scanned,
                        partial,
                    );
                }
                let channel_page = history_vfs
                    .list_page_beneath_no_links_if_exists(
                        SIFS_AGENT_CHANNELS_ROOT,
                        channel_after.as_ref(),
                        1,
                    )
                    .map_err(channel_vfs_history_error)?;
                let Some(entry) = channel_page.entries.first() else {
                    position = HistoryCursor::Background {
                        activity_cursor: None,
                    };
                    break 'channels;
                };
                let next_channel_cursor = encode_vfs_cursor(channel_page.next_cursor.as_ref())?;
                let parsed_channel_id = (entry.kind == VfsObjectKind::Directory)
                    .then(|| entry.name.parse::<SessionId>().ok())
                    .flatten();
                let Some(current_channel_id) = parsed_channel_id else {
                    scanned.activities = scanned.activities.saturating_add(1);
                    scanned.items = scanned.items.saturating_add(1);
                    scanned.omitted_items = scanned.omitted_items.saturating_add(1);
                    partial = true;
                    let Some(next) = channel_page.next_cursor else {
                        position = HistoryCursor::Background {
                            activity_cursor: None,
                        };
                        break 'channels;
                    };
                    channel_after = Some(next);
                    channel_cursor = next_channel_cursor;
                    channel_id = None;
                    record_source = ChannelMessageSource::Legacy;
                    record_after = None;
                    record_cursor = None;
                    record_entry = None;
                    record_object_id = None;
                    record_version = None;
                    record_offset = 0;
                    byte_offset = 0;
                    discarding_line = false;
                    continue;
                };
                if channel_id
                    .as_ref()
                    .is_some_and(|expected| expected != &current_channel_id)
                {
                    return Err(invalid_cursor(
                        "channel continuation no longer identifies the expected channel",
                    ));
                }
                if channel_id.is_none() {
                    record_source = ChannelMessageSource::Legacy;
                    record_after = None;
                    record_cursor = None;
                    record_entry = None;
                    record_object_id = None;
                    record_version = None;
                    record_offset = 0;
                    byte_offset = 0;
                    discarding_line = false;
                }
                channel_id = Some(current_channel_id.clone());
                scanned.activities = scanned.activities.saturating_add(1);
                let metadata_started_after_prior_scan = scanned.items > 0 || scanned.bytes > 0;
                let metadata = SifsAgentChannelStore::load_channel_metadata_for_history_from_vfs(
                    &history_vfs,
                    SIFS_AGENT_CHANNELS_ROOT,
                    &current_channel_id,
                    request.max_scanned_items.saturating_sub(scanned.items),
                    request.max_scanned_bytes.saturating_sub(scanned.bytes),
                )
                .map_err(channel_store_history_error)?;
                scanned.items = scanned.items.saturating_add(metadata.scanned_items);
                scanned.bytes = scanned.bytes.saturating_add(metadata.scanned_bytes);
                scanned.omitted_items = scanned
                    .omitted_items
                    .saturating_add(metadata.omitted_records);
                partial |= !metadata.complete || metadata.omitted_records > 0;
                let Some(mut current) = metadata.channel else {
                    // A proof that only failed after earlier channels consumed
                    // this page's budget may fit when retried with a fresh page
                    // budget. Preserve the cursor before this channel once;
                    // when it is already first on a fresh page, fail closed and
                    // advance so an intrinsically oversized proof cannot loop.
                    if !metadata.complete && metadata_started_after_prior_scan {
                        return continued_page(
                            matches,
                            HistoryCursor::Channel {
                                channel_id: channel_id.clone(),
                                channel_cursor: channel_cursor.clone(),
                                record_source: ChannelMessageSource::Legacy,
                                record_cursor: None,
                                record_entry: None,
                                record_object_id: None,
                                record_version: None,
                                record_offset: 0,
                                byte_offset: 0,
                                discarding_line: false,
                            },
                            &binding,
                            scanned,
                            partial,
                        );
                    }
                    let Some(next) = channel_page.next_cursor else {
                        position = HistoryCursor::Background {
                            activity_cursor: None,
                        };
                        break 'channels;
                    };
                    channel_after = Some(next);
                    channel_cursor = next_channel_cursor;
                    channel_id = None;
                    record_source = ChannelMessageSource::Legacy;
                    record_after = None;
                    record_cursor = None;
                    record_entry = None;
                    record_object_id = None;
                    record_version = None;
                    record_offset = 0;
                    byte_offset = 0;
                    discarding_line = false;
                    continue;
                };
                // Membership can be revoked after the metadata listing. Check
                // it again immediately before every message-body page read.
                if !agent_can_read_channel(&current, principal.agent_key()) {
                    let Some(next) = channel_page.next_cursor else {
                        position = HistoryCursor::Background {
                            activity_cursor: None,
                        };
                        break 'channels;
                    };
                    channel_after = Some(next);
                    channel_cursor = next_channel_cursor;
                    channel_id = None;
                    record_source = ChannelMessageSource::Legacy;
                    record_after = None;
                    record_cursor = None;
                    record_entry = None;
                    record_object_id = None;
                    record_version = None;
                    record_offset = 0;
                    byte_offset = 0;
                    discarding_line = false;
                    continue;
                }
                let mut authorization_proof_items = metadata.scanned_items;
                let mut authorization_proof_bytes = metadata.scanned_bytes;
                loop {
                    let remaining_items = request.max_scanned_items.saturating_sub(scanned.items);
                    let remaining_bytes = request.max_scanned_bytes.saturating_sub(scanned.bytes);
                    let body_budget = reserve_post_read_authorization_budget(
                        remaining_items,
                        remaining_bytes,
                        authorization_proof_items,
                        authorization_proof_bytes,
                    );
                    let Some((body_items, body_bytes)) = body_budget else {
                        // If this page already did useful work, retry the same
                        // record with a fresh page budget. If the authorization
                        // proof alone consumes a fresh page, fail closed and
                        // advance so an intrinsically oversized channel cannot
                        // create a non-advancing cursor loop.
                        if scanned.items > authorization_proof_items
                            || scanned.bytes > authorization_proof_bytes
                        {
                            return continued_page(
                                matches,
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source,
                                    record_cursor: record_cursor.clone(),
                                    record_entry: record_entry.clone(),
                                    record_object_id: record_object_id.clone(),
                                    record_version: record_version.clone(),
                                    record_offset,
                                    byte_offset,
                                    discarding_line,
                                },
                                &binding,
                                scanned,
                                partial,
                            );
                        }
                        partial = true;
                        let Some(next) = channel_page.next_cursor else {
                            position = HistoryCursor::Background {
                                activity_cursor: None,
                            };
                            break 'channels;
                        };
                        channel_after = Some(next);
                        channel_cursor = next_channel_cursor.clone();
                        channel_id = None;
                        record_source = ChannelMessageSource::Legacy;
                        record_after = None;
                        record_cursor = None;
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        record_offset = 0;
                        byte_offset = 0;
                        discarding_line = false;
                        continue 'channels;
                    };
                    let fresh_body_budget = reserve_post_read_authorization_budget(
                        request
                            .max_scanned_items
                            .saturating_sub(authorization_proof_items),
                        request
                            .max_scanned_bytes
                            .saturating_sub(authorization_proof_bytes),
                        authorization_proof_items,
                        authorization_proof_bytes,
                    );
                    let allow_discard_partial_line = record_source == ChannelMessageSource::Legacy
                        && fresh_body_budget
                            .is_some_and(|(_, fresh_bytes)| body_bytes >= fresh_bytes);
                    let page = load_channel_history_message_page(
                        &history_vfs,
                        &current.channel_id,
                        record_source,
                        record_after.as_ref(),
                        record_entry.as_deref(),
                        record_object_id.as_deref(),
                        record_version.as_deref(),
                        record_offset,
                        byte_offset,
                        discarding_line,
                        allow_discard_partial_line,
                        body_items,
                        body_bytes,
                    )
                    .map_err(channel_store_history_error)?;
                    if let Some(required_bytes) = page.required_bytes {
                        if fresh_body_budget
                            .is_some_and(|(items, bytes)| items > 0 && required_bytes <= bytes)
                        {
                            // Earlier work exhausted this invocation. Retry the
                            // same immutable record with a fresh hard budget
                            // instead of traversal-order-dependent omission.
                            return continued_page(
                                matches,
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source,
                                    record_cursor: record_cursor.clone(),
                                    record_entry: record_entry.clone(),
                                    record_object_id: record_object_id.clone(),
                                    record_version: record_version.clone(),
                                    record_offset,
                                    byte_offset,
                                    discarding_line,
                                },
                                &binding,
                                scanned,
                                partial,
                            );
                        }

                        // The immutable record cannot fit even on a fresh
                        // invocation with this request's hard scan budget.
                        scanned.items = scanned.items.saturating_add(1);
                        scanned.omitted_items = scanned.omitted_items.saturating_add(1);
                        partial = true;
                        record_offset = record_offset.saturating_add(1);
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        byte_offset = 0;
                        discarding_line = false;
                        if let Some(next) = page.next_cursor {
                            record_cursor = encode_vfs_cursor(Some(&next))?;
                            record_after = Some(next);
                            continue;
                        }
                        if record_source == ChannelMessageSource::Legacy {
                            record_source = ChannelMessageSource::Immutable;
                            record_cursor = None;
                            record_after = None;
                            record_offset = 0;
                            continue;
                        }
                        break;
                    }
                    scanned.items = scanned.items.saturating_add(page.consumed_entries);
                    scanned.omitted_items =
                        scanned.omitted_items.saturating_add(page.omitted_records);
                    partial |= page.omitted_records > 0;
                    partial |= page.next_discarding_line;
                    scanned.bytes = scanned.bytes.saturating_add(page.read_bytes);
                    // No body was materialized, so there is nothing to release
                    // and no post-read authorization proof is necessary.
                    if page.records.is_empty() {
                        record_offset = record_offset.saturating_add(page.consumed_entries);
                        if page.deferred_for_fresh_budget {
                            return continued_page(
                                matches,
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source,
                                    record_cursor: record_cursor.clone(),
                                    record_entry: page.current_entry_name,
                                    record_object_id: page.current_object_id,
                                    record_version: page.current_version,
                                    record_offset,
                                    byte_offset: page.next_byte_offset,
                                    discarding_line: page.next_discarding_line,
                                },
                                &binding,
                                scanned,
                                partial,
                            );
                        }
                        if page.next_byte_offset > 0 {
                            record_entry = page.current_entry_name;
                            record_object_id = page.current_object_id;
                            record_version = page.current_version;
                            byte_offset = page.next_byte_offset;
                            discarding_line = page.next_discarding_line;
                            continue;
                        }
                        byte_offset = 0;
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        discarding_line = false;
                        if let Some(next) = page.next_cursor {
                            record_cursor = encode_vfs_cursor(Some(&next))?;
                            record_after = Some(next);
                            continue;
                        }
                        if record_source == ChannelMessageSource::Legacy {
                            record_source = ChannelMessageSource::Immutable;
                            record_after = None;
                            record_cursor = None;
                            record_entry = None;
                            record_object_id = None;
                            record_version = None;
                            record_offset = 0;
                            byte_offset = 0;
                            discarding_line = false;
                            continue;
                        }
                        break;
                    }
                    #[cfg(test)]
                    run_channel_history_after_body_read_hook();
                    // Close the revocation race around the body read. The
                    // first check avoids unauthorized I/O; this second check
                    // ensures a concurrent disable/delete cannot release the
                    // bodies we just read into model-visible output.
                    let metadata_after_read =
                        SifsAgentChannelStore::load_channel_metadata_for_history_from_vfs(
                            &history_vfs,
                            SIFS_AGENT_CHANNELS_ROOT,
                            &current.channel_id,
                            request.max_scanned_items.saturating_sub(scanned.items),
                            request.max_scanned_bytes.saturating_sub(scanned.bytes),
                        )
                        .map_err(channel_store_history_error)?;
                    scanned.items = scanned
                        .items
                        .saturating_add(metadata_after_read.scanned_items);
                    scanned.bytes = scanned
                        .bytes
                        .saturating_add(metadata_after_read.scanned_bytes);
                    scanned.omitted_items = scanned
                        .omitted_items
                        .saturating_add(metadata_after_read.omitted_records);
                    partial |=
                        !metadata_after_read.complete || metadata_after_read.omitted_records > 0;
                    if !metadata_after_read.complete {
                        // The reserved proof changed or grew while the body was
                        // being read. Discard that body and retry this exact
                        // immutable record with a fresh bounded page rather
                        // than skipping the rest of the channel.
                        return continued_page(
                            matches,
                            HistoryCursor::Channel {
                                channel_id: channel_id.clone(),
                                channel_cursor: channel_cursor.clone(),
                                record_source,
                                record_cursor: record_cursor.clone(),
                                record_entry: record_entry.clone(),
                                record_object_id: record_object_id.clone(),
                                record_version: record_version.clone(),
                                record_offset,
                                byte_offset,
                                discarding_line,
                            },
                            &binding,
                            scanned,
                            partial,
                        );
                    }
                    let Some(authorized_after_read) = metadata_after_read.channel else {
                        let Some(next) = channel_page.next_cursor else {
                            position = HistoryCursor::Background {
                                activity_cursor: None,
                            };
                            break 'channels;
                        };
                        channel_after = Some(next);
                        channel_cursor = next_channel_cursor.clone();
                        channel_id = None;
                        record_source = ChannelMessageSource::Legacy;
                        record_after = None;
                        record_cursor = None;
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        record_offset = 0;
                        byte_offset = 0;
                        discarding_line = false;
                        continue 'channels;
                    };
                    if !agent_can_read_channel(&authorized_after_read, principal.agent_key()) {
                        let Some(next) = channel_page.next_cursor else {
                            position = HistoryCursor::Background {
                                activity_cursor: None,
                            };
                            break 'channels;
                        };
                        channel_after = Some(next);
                        channel_cursor = next_channel_cursor.clone();
                        channel_id = None;
                        record_source = ChannelMessageSource::Legacy;
                        record_after = None;
                        record_cursor = None;
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        record_offset = 0;
                        byte_offset = 0;
                        discarding_line = false;
                        continue 'channels;
                    }
                    for record in page.records {
                        let next_record_offset = record.record_offset.saturating_add(1);
                        let next_byte_offset = record.next_byte_offset;
                        let message = record.message;
                        if cancellation.is_cancelled() {
                            return Err(SessionHistorySearchError::Cancelled);
                        }
                        if message.content.to_lowercase().contains(&query) {
                            let (excerpt, excerpt_truncated) = matching_excerpt(
                                &message.content,
                                &query,
                                request.max_excerpt_bytes,
                                false,
                            );
                            matches.push(SessionHistorySearchMatch {
                                activity_id: authorized_after_read.channel_id.clone(),
                                activity_kind: channel_activity_kind(&authorized_after_read),
                                item_id: message.message_id.clone(),
                                sequence: Some(message.sequence),
                                title: truncate_utf8(
                                    &authorized_after_read.title,
                                    request.max_title_bytes,
                                )
                                .0,
                                author: truncate_utf8(
                                    &channel_author_label(&message.author),
                                    request.max_author_bytes,
                                )
                                .0,
                                created_at: message.created_at,
                                excerpt,
                                excerpt_truncated,
                            });
                        }
                        if matches.len() >= request.limit {
                            let next_record_cursor = encode_vfs_cursor(page.next_cursor.as_ref())?;
                            let next_position = if next_byte_offset > 0 {
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source,
                                    record_cursor: record_cursor.clone(),
                                    record_entry: page.current_entry_name.clone(),
                                    record_object_id: page.current_object_id.clone(),
                                    record_version: page.current_version.clone(),
                                    record_offset: next_record_offset,
                                    byte_offset: next_byte_offset,
                                    discarding_line: false,
                                }
                            } else if next_record_cursor.is_some() {
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source,
                                    record_cursor: next_record_cursor,
                                    record_entry: None,
                                    record_object_id: None,
                                    record_version: None,
                                    record_offset: next_record_offset,
                                    byte_offset: 0,
                                    discarding_line: false,
                                }
                            } else if record_source == ChannelMessageSource::Legacy {
                                HistoryCursor::Channel {
                                    channel_id: channel_id.clone(),
                                    channel_cursor: channel_cursor.clone(),
                                    record_source: ChannelMessageSource::Immutable,
                                    record_cursor: None,
                                    record_entry: None,
                                    record_object_id: None,
                                    record_version: None,
                                    record_offset: 0,
                                    byte_offset: 0,
                                    discarding_line: false,
                                }
                            } else if channel_page.next_cursor.is_some() {
                                HistoryCursor::Channel {
                                    channel_id: None,
                                    channel_cursor: next_channel_cursor.clone(),
                                    record_source: ChannelMessageSource::Legacy,
                                    record_cursor: None,
                                    record_entry: None,
                                    record_object_id: None,
                                    record_version: None,
                                    record_offset: 0,
                                    byte_offset: 0,
                                    discarding_line: false,
                                }
                            } else {
                                HistoryCursor::Background {
                                    activity_cursor: None,
                                }
                            };
                            return continued_page(
                                matches,
                                next_position,
                                &binding,
                                scanned,
                                partial,
                            );
                        }
                    }
                    authorization_proof_items = metadata_after_read.scanned_items;
                    authorization_proof_bytes = metadata_after_read.scanned_bytes;
                    current = authorized_after_read;
                    record_offset = record_offset.saturating_add(page.consumed_entries);
                    if page.next_byte_offset > 0 {
                        record_entry = page.current_entry_name;
                        record_object_id = page.current_object_id;
                        record_version = page.current_version;
                        byte_offset = page.next_byte_offset;
                        discarding_line = page.next_discarding_line;
                        continue;
                    }
                    record_entry = None;
                    record_object_id = None;
                    record_version = None;
                    byte_offset = 0;
                    discarding_line = false;
                    if let Some(next) = page.next_cursor {
                        record_cursor = encode_vfs_cursor(Some(&next))?;
                        record_after = Some(next);
                        continue;
                    }
                    if record_source == ChannelMessageSource::Legacy {
                        record_source = ChannelMessageSource::Immutable;
                        record_cursor = None;
                        record_after = None;
                        record_entry = None;
                        record_object_id = None;
                        record_version = None;
                        record_offset = 0;
                        byte_offset = 0;
                        discarding_line = false;
                        continue;
                    }
                    break;
                }

                let Some(next) = channel_page.next_cursor else {
                    position = HistoryCursor::Background {
                        activity_cursor: None,
                    };
                    break 'channels;
                };
                channel_after = Some(next);
                channel_cursor = next_channel_cursor;
                channel_id = None;
                record_source = ChannelMessageSource::Legacy;
                record_after = None;
                record_cursor = None;
                record_entry = None;
                record_object_id = None;
                record_version = None;
                record_offset = 0;
                byte_offset = 0;
                discarding_line = false;
            }
        }

        let HistoryCursor::Background {
            mut activity_cursor,
        } = position
        else {
            unreachable!("channel history transitions to background history")
        };
        let store = AgentTaskActivityProjectionStore::from_vfs(history_vfs);
        if let Some(data_dir) = &self.sifs_data_dir {
            let local_activity_store = FileAgentSessionStore::for_data_dir(data_dir);
            partial |= local_activity_store
                .has_pending_task_activity_mirrors(principal.agent_key())
                .unwrap_or(true);
        }
        let mut activity_after = decode_vfs_cursor(activity_cursor.as_deref())?;
        loop {
            if cancellation.is_cancelled() {
                return Err(SessionHistorySearchError::Cancelled);
            }
            if scanned.activities >= request.max_scanned_activities
                || scanned.items >= request.max_scanned_items
                || scanned.bytes >= request.max_scanned_bytes
            {
                return continued_page(
                    matches,
                    HistoryCursor::Background { activity_cursor },
                    &binding,
                    scanned,
                    partial,
                );
            }
            let remaining_entries = request
                .max_scanned_activities
                .saturating_sub(scanned.activities)
                .min(request.max_scanned_items.saturating_sub(scanned.items));
            let remaining_bytes = request.max_scanned_bytes.saturating_sub(scanned.bytes);
            let page = store
                .load_page(
                    principal.agent_key(),
                    activity_after.as_ref(),
                    remaining_entries.min(1),
                    remaining_bytes,
                )
                .map_err(|_| SessionHistorySearchError::Backend {
                    // Neither private model-session ids nor storage paths are
                    // part of the model-visible search surface.
                    detail: "cannot read typed background activity projection".to_string(),
                })?;
            if !page.available {
                // There is deliberately no legacy raw-session fallback. An
                // agent upgraded from the old format gets an explicit partial
                // result until new typed projections have been written.
                partial = true;
                break;
            }
            if let Some(required_bytes) = page.required_bytes {
                if page.deferred_for_fresh_budget && required_bytes <= request.max_scanned_bytes {
                    // Channel traversal consumed this invocation's remaining
                    // body budget. Retry the exact typed activity with a fresh
                    // hard budget instead of traversal-order-dependent loss.
                    return continued_page(
                        matches,
                        HistoryCursor::Background {
                            activity_cursor: encode_vfs_cursor(page.deferred_cursor.as_ref())?,
                        },
                        &binding,
                        scanned,
                        partial,
                    );
                }

                // A single typed projection cannot fit this request's fresh
                // hard budget. Account for the explicit partial omission and
                // advance only via the guarded cursor after that record.
                scanned.activities = scanned.activities.saturating_add(1);
                scanned.items = scanned.items.saturating_add(1);
                scanned.omitted_items = scanned.omitted_items.saturating_add(1);
                partial = true;
                let next_activity_cursor = encode_vfs_cursor(page.next_cursor.as_ref())?;
                let Some(next) = page.next_cursor else {
                    break;
                };
                activity_cursor = next_activity_cursor;
                activity_after = Some(next);
                continue;
            }
            scanned.activities = scanned.activities.saturating_add(page.consumed_entries);
            scanned.items = scanned.items.saturating_add(page.consumed_entries);
            scanned.omitted_items = scanned.omitted_items.saturating_add(page.omitted_entries);
            scanned.bytes = scanned.bytes.saturating_add(page.read_bytes);
            partial |= page.omitted_entries > 0;
            let next_activity_cursor = encode_vfs_cursor(page.next_cursor.as_ref())?;
            let Some(activity) = page.activities.first() else {
                let Some(next) = page.next_cursor else {
                    break;
                };
                activity_cursor = next_activity_cursor;
                activity_after = Some(next);
                continue;
            };
            // The projection loader already validates the owner in the body,
            // but keep this last release-site guard next to model-visible use.
            if !agent_owns_background_activity(activity, principal.agent_key()) {
                partial = true;
            } else {
                let text = background_activity_text(activity);
                if text.to_lowercase().contains(&query) {
                    let (excerpt, excerpt_truncated) =
                        matching_excerpt(&text, &query, request.max_excerpt_bytes, false);
                    matches.push(SessionHistorySearchMatch {
                        activity_id: activity.activity_id.clone(),
                        activity_kind: SessionHistoryActivityKind::BackgroundActivity,
                        item_id: activity.activity_id.clone(),
                        sequence: None,
                        title: truncate_utf8(&activity.summary, request.max_title_bytes).0,
                        author: truncate_utf8(principal.agent_key(), request.max_author_bytes).0,
                        created_at: activity.updated_at,
                        excerpt,
                        excerpt_truncated,
                    });
                }
            }
            if matches.len() >= request.limit && page.next_cursor.is_some() {
                return continued_page(
                    matches,
                    HistoryCursor::Background {
                        activity_cursor: next_activity_cursor,
                    },
                    &binding,
                    scanned,
                    partial,
                );
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            activity_cursor = next_activity_cursor;
            activity_after = Some(next);
        }

        Ok(SessionHistorySearchPage {
            matches,
            next_cursor: None,
            complete: true,
            partial: partial || scanned.omitted_items > 0,
            consistency: SessionHistorySearchConsistency::BestEffort,
            scanned,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HistoryCursor {
    Channel {
        channel_id: Option<SessionId>,
        /// Paged-directory cursor immediately before `channel_id`.
        channel_cursor: Option<String>,
        record_source: ChannelMessageSource,
        /// Paged-directory cursor immediately before `record_offset`.
        record_cursor: Option<String>,
        /// Legacy JSONL entry containing `byte_offset`, when paging a chunk.
        record_entry: Option<String>,
        /// Exact immutable object opened for a legacy range continuation.
        record_object_id: Option<String>,
        /// Exact immutable version opened for a legacy range continuation.
        record_version: Option<String>,
        record_offset: usize,
        byte_offset: usize,
        /// The cursor is scanning to the end of one oversized JSONL record.
        discarding_line: bool,
    },
    Background {
        /// Paged-directory cursor immediately before the next typed activity.
        activity_cursor: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChannelMessageSource {
    Legacy,
    Immutable,
}

struct ChannelHistoryMessageRecord {
    record_offset: usize,
    next_byte_offset: usize,
    message: sinomo_agent::AgentChannelMessage,
}

struct ChannelHistoryMessagePage {
    records: Vec<ChannelHistoryMessageRecord>,
    next_cursor: Option<VfsDirectoryCursor>,
    current_entry_name: Option<String>,
    current_object_id: Option<String>,
    current_version: Option<String>,
    next_byte_offset: usize,
    next_discarding_line: bool,
    deferred_for_fresh_budget: bool,
    consumed_entries: usize,
    read_bytes: u64,
    omitted_records: usize,
    required_bytes: Option<u64>,
}

fn cursor_binding(agent_key: &str, query: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(agent_key.as_bytes());
    hasher.update(&[0]);
    hasher.update(query.to_lowercase().as_bytes());
    let digest = hasher.finalize().to_hex();
    digest.as_str()[..24].to_string()
}

fn parse_cursor(
    cursor: Option<&str>,
    expected_binding: &str,
) -> Result<HistoryCursor, SessionHistorySearchError> {
    let Some(cursor) = cursor else {
        return Ok(HistoryCursor::Channel {
            channel_id: None,
            channel_cursor: None,
            record_source: ChannelMessageSource::Legacy,
            record_cursor: None,
            record_entry: None,
            record_object_id: None,
            record_version: None,
            record_offset: 0,
            byte_offset: 0,
            discarding_line: false,
        });
    };
    let parts = cursor.split(':').collect::<Vec<_>>();
    if parts.first().copied() != Some("h6") || parts.get(1).copied() != Some(expected_binding) {
        return Err(invalid_cursor(
            "cursor belongs to a different agent or query",
        ));
    }
    match parts.as_slice() {
        [
            _,
            _,
            "c",
            channel_id,
            channel_cursor,
            record_source,
            record_cursor,
            record_entry,
            record_object_id,
            record_version,
            record_offset,
            byte_offset,
            discarding_line,
        ] => Ok(HistoryCursor::Channel {
            channel_id: parse_channel_cursor_id(channel_id)?,
            channel_cursor: parse_optional_cursor_component(channel_cursor),
            record_source: parse_channel_message_source(record_source)?,
            record_cursor: parse_optional_cursor_component(record_cursor),
            record_entry: parse_optional_cursor_component(record_entry),
            record_object_id: parse_encoded_cursor_component(record_object_id)?,
            record_version: parse_encoded_cursor_component(record_version)?,
            record_offset: parse_cursor_number(record_offset)?,
            byte_offset: parse_cursor_number(byte_offset)?,
            discarding_line: parse_cursor_bool(discarding_line)?,
        }),
        [_, _, "b", activity_cursor] => Ok(HistoryCursor::Background {
            activity_cursor: parse_optional_cursor_component(activity_cursor),
        }),
        _ => Err(invalid_cursor("unrecognized cursor shape")),
    }
}

fn encode_cursor(position: &HistoryCursor, binding: &str) -> String {
    match position {
        HistoryCursor::Channel {
            channel_id,
            channel_cursor,
            record_source,
            record_cursor,
            record_entry,
            record_object_id,
            record_version,
            record_offset,
            byte_offset,
            discarding_line,
        } => format!(
            "h6:{binding}:c:{}:{}:{}:{}:{}:{}:{}:{record_offset}:{byte_offset}:{}",
            session_cursor_id_component(channel_id.as_ref()),
            optional_cursor_component(channel_cursor.as_deref()),
            channel_message_source_component(*record_source),
            optional_cursor_component(record_cursor.as_deref()),
            optional_cursor_component(record_entry.as_deref()),
            encoded_cursor_component(record_object_id.as_deref()),
            encoded_cursor_component(record_version.as_deref()),
            if *discarding_line { "1" } else { "0" },
        ),
        HistoryCursor::Background { activity_cursor } => format!(
            "h6:{binding}:b:{}",
            optional_cursor_component(activity_cursor.as_deref())
        ),
    }
}

fn parse_channel_message_source(
    value: &str,
) -> Result<ChannelMessageSource, SessionHistorySearchError> {
    match value {
        "l" => Ok(ChannelMessageSource::Legacy),
        "i" => Ok(ChannelMessageSource::Immutable),
        _ => Err(invalid_cursor("invalid channel message source")),
    }
}

fn channel_message_source_component(source: ChannelMessageSource) -> &'static str {
    match source {
        ChannelMessageSource::Legacy => "l",
        ChannelMessageSource::Immutable => "i",
    }
}

fn parse_channel_cursor_id(value: &str) -> Result<Option<SessionId>, SessionHistorySearchError> {
    if value == "-" {
        return Ok(None);
    }
    value
        .parse::<SessionId>()
        .map(Some)
        .map_err(|_| invalid_cursor("invalid activity id cursor component"))
}

fn parse_optional_cursor_component(value: &str) -> Option<String> {
    (value != "-").then(|| value.to_string())
}

fn optional_cursor_component(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

fn encoded_cursor_component(value: Option<&str>) -> String {
    value
        .map(|value| URL_SAFE_NO_PAD.encode(value.as_bytes()))
        .unwrap_or_else(|| "-".to_string())
}

fn parse_encoded_cursor_component(
    value: &str,
) -> Result<Option<String>, SessionHistorySearchError> {
    if value == "-" {
        return Ok(None);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| invalid_cursor("invalid encoded cursor component"))?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| invalid_cursor("invalid encoded cursor component"))
}

fn parse_cursor_bool(value: &str) -> Result<bool, SessionHistorySearchError> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(invalid_cursor("invalid boolean cursor component")),
    }
}

fn session_cursor_id_component(value: Option<&SessionId>) -> &str {
    value.map(SessionId::as_str).unwrap_or("-")
}

fn decode_vfs_cursor(
    value: Option<&str>,
) -> Result<Option<VfsDirectoryCursor>, SessionHistorySearchError> {
    value
        .map(VfsDirectoryCursor::from_opaque_string)
        .transpose()
        .map_err(|source| invalid_cursor(format!("invalid filesystem continuation: {source}")))
}

fn encode_vfs_cursor(
    value: Option<&VfsDirectoryCursor>,
) -> Result<Option<String>, SessionHistorySearchError> {
    value
        .map(VfsDirectoryCursor::to_opaque_string)
        .transpose()
        .map_err(|source| SessionHistorySearchError::Backend {
            detail: format!("cannot encode filesystem continuation: {source}"),
        })
}

fn parse_cursor_number(value: &str) -> Result<usize, SessionHistorySearchError> {
    value
        .parse()
        .map_err(|_| invalid_cursor("invalid numeric cursor component"))
}

/// Leave enough of this tool call's hard scan budget to repeat the most recent
/// complete authorization proof after one immutable body is read.
fn reserve_post_read_authorization_budget(
    remaining_items: usize,
    remaining_bytes: u64,
    proof_items: usize,
    proof_bytes: u64,
) -> Option<(usize, u64)> {
    let body_items = remaining_items.checked_sub(proof_items)?;
    let body_bytes = remaining_bytes.checked_sub(proof_bytes)?;
    (body_items > 0 && body_bytes > 0).then_some((body_items, body_bytes))
}

#[allow(clippy::too_many_arguments)]
fn load_channel_history_message_page(
    history_vfs: &VfsHandle,
    channel_id: &SessionId,
    source: ChannelMessageSource,
    after: Option<&VfsDirectoryCursor>,
    expected_entry_name: Option<&str>,
    expected_object_id: Option<&str>,
    expected_version: Option<&str>,
    record_offset: usize,
    byte_offset: usize,
    discarding_line: bool,
    allow_discard_partial_line: bool,
    max_items: usize,
    max_bytes: u64,
) -> Result<ChannelHistoryMessagePage, AgentChannelStoreError> {
    match source {
        ChannelMessageSource::Legacy => {
            let page = SifsAgentChannelStore::load_legacy_message_record_page_from_vfs(
                history_vfs,
                SIFS_AGENT_CHANNELS_ROOT,
                channel_id,
                after,
                expected_entry_name,
                expected_object_id,
                expected_version,
                record_offset,
                byte_offset,
                discarding_line,
                allow_discard_partial_line,
                max_items,
                max_bytes,
            )?;
            Ok(ChannelHistoryMessagePage {
                records: page
                    .records
                    .into_iter()
                    .map(|record| ChannelHistoryMessageRecord {
                        record_offset: record.record_offset,
                        next_byte_offset: record.next_byte_offset,
                        message: record.message,
                    })
                    .collect(),
                next_cursor: page.next_cursor,
                current_entry_name: page.current_entry_name,
                current_object_id: page.current_object_id,
                current_version: page.current_version,
                next_byte_offset: page.next_byte_offset,
                next_discarding_line: page.next_discarding_line,
                deferred_for_fresh_budget: page.deferred_for_fresh_budget,
                consumed_entries: page.consumed_entries,
                read_bytes: page.read_bytes,
                omitted_records: page.omitted_records,
                required_bytes: None,
            })
        }
        ChannelMessageSource::Immutable => {
            if byte_offset != 0
                || discarding_line
                || expected_entry_name.is_some()
                || expected_object_id.is_some()
                || expected_version.is_some()
            {
                return Err(AgentChannelStoreError::Io {
                    path: channel_id.to_string(),
                    detail: "invalid immutable message cursor state".to_string(),
                });
            }
            let page = SifsAgentChannelStore::load_immutable_message_record_page_from_vfs(
                history_vfs,
                channel_id,
                after,
                record_offset,
                max_items.min(CHANNEL_MESSAGE_PAGE_SIZE),
                max_bytes,
            )?;
            Ok(ChannelHistoryMessagePage {
                records: page
                    .records
                    .into_iter()
                    .map(|record| ChannelHistoryMessageRecord {
                        record_offset: record.record_offset,
                        next_byte_offset: 0,
                        message: record.message,
                    })
                    .collect(),
                next_cursor: page.next_cursor,
                current_entry_name: None,
                current_object_id: None,
                current_version: None,
                next_byte_offset: 0,
                next_discarding_line: false,
                deferred_for_fresh_budget: false,
                consumed_entries: page.consumed_entries,
                read_bytes: page.read_bytes,
                omitted_records: page.omitted_records,
                required_bytes: page.required_bytes,
            })
        }
    }
}

fn continued_page(
    matches: Vec<SessionHistorySearchMatch>,
    position: HistoryCursor,
    binding: &str,
    scanned: SessionHistorySearchStats,
    partial: bool,
) -> Result<SessionHistorySearchPage, SessionHistorySearchError> {
    Ok(SessionHistorySearchPage {
        matches,
        next_cursor: Some(encode_cursor(&position, binding)),
        complete: false,
        partial: partial || scanned.omitted_items > 0,
        consistency: SessionHistorySearchConsistency::BestEffort,
        scanned,
    })
}

fn invalid_cursor(detail: impl Into<String>) -> SessionHistorySearchError {
    SessionHistorySearchError::InvalidCursor {
        detail: detail.into(),
    }
}

fn channel_vfs_history_error(error: VfsError) -> SessionHistorySearchError {
    if matches!(&error, VfsError::InvalidCursor { .. }) {
        return invalid_cursor("filesystem continuation does not match visible channel history");
    }
    scrubbed_channel_backend_error()
}

fn channel_store_history_error(error: AgentChannelStoreError) -> SessionHistorySearchError {
    if matches!(
        &error,
        AgentChannelStoreError::Io { detail, .. }
            if detail.starts_with("invalid VFS directory cursor:")
                || detail.starts_with("invalid legacy message cursor:")
                || detail.starts_with("invalid immutable message cursor")
    ) {
        return invalid_cursor("filesystem continuation does not match visible channel history");
    }
    scrubbed_channel_backend_error()
}

fn scrubbed_channel_backend_error() -> SessionHistorySearchError {
    SessionHistorySearchError::Backend {
        detail: "visible channel history storage operation failed".to_string(),
    }
}

fn agent_can_read_channel(channel: &AgentChannelHistoryMetadata, agent_key: &str) -> bool {
    !channel.is_deleted()
        && channel
            .participants
            .iter()
            .any(|participant| participant.enabled && participant.agent_key == agent_key)
}

fn channel_activity_kind(channel: &AgentChannelHistoryMetadata) -> SessionHistoryActivityKind {
    match channel.channel_kind {
        AgentChannelKind::Chat => SessionHistoryActivityKind::Chat,
        AgentChannelKind::Channel => SessionHistoryActivityKind::Channel,
    }
}

fn channel_author_label(author: &AgentChannelAuthor) -> String {
    match author {
        AgentChannelAuthor::User { display_name, .. } => display_name.clone(),
        AgentChannelAuthor::Agent {
            agent_name,
            agent_key,
            ..
        } => {
            if agent_name.trim().is_empty() {
                agent_key.clone()
            } else {
                agent_name.clone()
            }
        }
        AgentChannelAuthor::System => "System".to_string(),
    }
}

fn background_activity_text(activity: &AgentTaskActivityProjection) -> String {
    let status = match &activity.status {
        AgentTaskLifecycleStatus::Pending => "pending",
        AgentTaskLifecycleStatus::Running => "running",
        AgentTaskLifecycleStatus::Completed => "completed",
        AgentTaskLifecycleStatus::Failed => "failed",
        AgentTaskLifecycleStatus::Cancelled => "cancelled",
        AgentTaskLifecycleStatus::TimedOut => "timed out",
        AgentTaskLifecycleStatus::Paused => "paused",
    };
    [
        activity.summary.as_str(),
        activity.activity_kind.as_str(),
        status,
        activity.progress_summary.as_deref().unwrap_or_default(),
    ]
    .into_iter()
    .filter(|part| !part.trim().is_empty())
    .collect::<Vec<_>>()
    .join("\n")
}

fn agent_owns_background_activity(activity: &AgentTaskActivityProjection, agent_key: &str) -> bool {
    activity.owner_agent_key == agent_key
}

fn matching_excerpt(
    chunk: &str,
    lower_query: &str,
    max_bytes: usize,
    source_truncated: bool,
) -> (String, bool) {
    let candidate = chunk
        .lines()
        .find(|line| line.to_lowercase().contains(lower_query))
        .unwrap_or(chunk)
        .trim();
    if candidate.len() <= max_bytes {
        return (candidate.to_string(), source_truncated);
    }

    // Prefer a window around the match instead of returning a long line's
    // unrelated prefix. Unicode case folding can change byte positions, so
    // only reuse the lowercase offset when it is also a boundary in the
    // original string; otherwise the safe prefix fallback still applies.
    let lower_candidate = candidate.to_lowercase();
    let match_offset = lower_candidate
        .find(lower_query)
        .filter(|offset| *offset <= candidate.len() && candidate.is_char_boundary(*offset))
        .unwrap_or(0);
    let mut start = match_offset.saturating_sub(max_bytes / 3);
    while start > 0 && !candidate.is_char_boundary(start) {
        start -= 1;
    }
    if candidate.len().saturating_sub(start) < max_bytes {
        start = candidate.len().saturating_sub(max_bytes);
        while start > 0 && !candidate.is_char_boundary(start) {
            start -= 1;
        }
    }
    let mut end = start.saturating_add(max_bytes).min(candidate.len());
    while end > start && !candidate.is_char_boundary(end) {
        end -= 1;
    }
    (
        candidate[start..end].to_string(),
        source_truncated || start > 0 || end < candidate.len(),
    )
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_backend_searches_stable_chat_and_background_activity() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-visible-activity-history");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel =
            sinomo_agent::AgentChannel::new(channel_id.clone(), "Stable visible chat");
        channel.ensure_participant("main-agent", "Main");
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized chat");
        let message = channel.append_user_message(
            "app-user",
            "User",
            "recall-needle in a visible chat",
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, &message)
            .expect("append stable chat message");

        let activity_store = AgentTaskActivityProjectionStore::from_vfs(
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open activity SIFS"),
        );
        let activity = AgentTaskActivityProjection {
            activity_id: sinomo_agent::TaskId::generate(),
            owner_agent_key: "main-agent".to_string(),
            activity_kind: "maintenance".to_string(),
            summary: "recall-needle background activity".to_string(),
            status: AgentTaskLifecycleStatus::Completed,
            progress_summary: Some("completed".to_string()),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        activity_store
            .save_activity(&activity)
            .expect("save typed background activity");

        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let page = backend
            .search(
                &SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
                &SessionHistorySearchRequest {
                    query: "recall-needle".to_string(),
                    cursor: None,
                    limit: 10,
                    max_scanned_activities: 16,
                    max_scanned_items: 64,
                    max_scanned_bytes: 1024 * 1024,
                    max_excerpt_bytes: 256,
                    max_title_bytes: 256,
                    max_author_bytes: 128,
                },
                &AgentCancellationToken::new(),
            )
            .expect("search stable visible activity");

        assert!(page.complete);
        assert_eq!(page.matches.len(), 2, "{:#?}", page.matches);
        assert!(
            page.matches
                .iter()
                .any(|matched| matched.activity_kind == SessionHistoryActivityKind::Chat)
        );
        assert!(page.matches.iter().any(|matched| {
            matched.activity_kind == SessionHistoryActivityKind::BackgroundActivity
        }));
    }

    #[test]
    fn history_backend_searches_an_injected_memory_vfs_without_dfs() {
        let history_vfs = VfsHandle::memory();
        let activity_store = AgentTaskActivityProjectionStore::from_vfs(history_vfs.clone());
        let activity = AgentTaskActivityProjection {
            activity_id: sinomo_agent::TaskId::generate(),
            owner_agent_key: "main-agent".to_string(),
            activity_kind: "benchmark".to_string(),
            summary: "in-memory-history-needle".to_string(),
            status: AgentTaskLifecycleStatus::Completed,
            progress_summary: Some("completed".to_string()),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        activity_store
            .save_activity(&activity)
            .expect("save in-memory activity");

        let page = VfsAgentHistorySearchBackend::from_vfs(history_vfs)
            .search(
                &SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
                &SessionHistorySearchRequest {
                    query: "in-memory-history-needle".to_string(),
                    cursor: None,
                    limit: 10,
                    max_scanned_activities: 16,
                    max_scanned_items: 64,
                    max_scanned_bytes: 1024 * 1024,
                    max_excerpt_bytes: 256,
                    max_title_bytes: 256,
                    max_author_bytes: 128,
                },
                &AgentCancellationToken::new(),
            )
            .expect("search injected VFS history");

        assert!(page.complete, "{page:#?}");
        assert_eq!(page.matches.len(), 1, "{page:#?}");
        assert_eq!(page.matches[0].item_id, activity.activity_id);
        assert_eq!(
            page.matches[0].activity_kind,
            SessionHistoryActivityKind::BackgroundActivity
        );
    }

    #[test]
    fn background_activity_retries_after_channel_scan_consumes_remaining_bytes() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-background-fresh-budget");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel =
            sinomo_agent::AgentChannel::new(channel_id.clone(), "Channel before activity");
        channel.ensure_participant("main-agent", "Main");
        let channel_message = channel.append_user_message(
            "app-user",
            "User",
            format!("channel nonmatch {}", "c".repeat(512)),
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        sinomo_agent::AgentChannelStore::append_message(
            &channel_store,
            &channel_id,
            &channel_message,
        )
        .expect("append stable channel message");

        let backend = VfsAgentHistorySearchBackend::new(data_dir.clone());
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let baseline = backend
            .search(
                &principal,
                &SessionHistorySearchRequest {
                    query: "background-fresh-budget-needle".to_string(),
                    cursor: None,
                    limit: 10,
                    max_scanned_activities: 16,
                    max_scanned_items: 64,
                    max_scanned_bytes: 1024 * 1024,
                    max_excerpt_bytes: 256,
                    max_title_bytes: 256,
                    max_author_bytes: 128,
                },
                &AgentCancellationToken::new(),
            )
            .expect("measure channel scan cost");
        assert!(baseline.complete, "{baseline:#?}");
        assert!(baseline.scanned.bytes > 0, "{baseline:#?}");

        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open activity SIFS");
        let activity_store = AgentTaskActivityProjectionStore::from_vfs(history_vfs.clone());
        let activity = AgentTaskActivityProjection {
            activity_id: sinomo_agent::TaskId::generate(),
            owner_agent_key: "main-agent".to_string(),
            activity_kind: "maintenance".to_string(),
            summary: format!("background-fresh-budget-needle {}", "b".repeat(1_024)),
            status: AgentTaskLifecycleStatus::Completed,
            progress_summary: Some("completed".to_string()),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        activity_store
            .save_activity(&activity)
            .expect("save typed background activity");
        let activity_directory = format!(
            "{}/{}",
            sinomo_agent::AGENT_TASK_ACTIVITY_SIFS_ROOT,
            blake3::hash(b"main-agent").to_hex()
        );
        let activity_path = format!("{activity_directory}/{}.json", activity.activity_id);
        let activity_bytes = history_vfs
            .stat_beneath_no_links(&activity_path)
            .expect("stat typed activity")
            .size_bytes
            .expect("typed activity size");
        let max_scanned_bytes = baseline
            .scanned
            .bytes
            .saturating_add(activity_bytes)
            .saturating_sub(1);
        assert!(max_scanned_bytes >= activity_bytes);

        let mut request = SessionHistorySearchRequest {
            query: "background-fresh-budget-needle".to_string(),
            cursor: None,
            limit: 10,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes,
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let first_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("channel then deferred background page");
        assert!(!first_page.complete, "{first_page:#?}");
        assert!(first_page.matches.is_empty(), "{first_page:#?}");
        assert_eq!(first_page.scanned.omitted_items, 0, "{first_page:#?}");
        request.cursor = first_page.next_cursor;
        let second_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("fresh background continuation");
        assert!(
            second_page
                .matches
                .iter()
                .any(|matched| matched.item_id == activity.activity_id),
            "{second_page:#?}"
        );
    }

    #[test]
    fn history_backend_pages_legacy_stable_chunks_and_deduplicates_immutable_records() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-legacy-visible-history");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel =
            sinomo_agent::AgentChannel::new(channel_id.clone(), "Legacy visible chat");
        channel.ensure_participant("main-agent", "Main");
        let legacy_only = channel.append_user_message(
            "app-user",
            "User",
            "legacy-recall-needle first",
            sinomo_agent::AgentChannelSource::app(),
        );
        let migrated = channel.append_user_message(
            "app-user",
            "User",
            "legacy-recall-needle migrated",
            sinomo_agent::AgentChannelSource::app(),
        );
        channel.append_user_message(
            "app-user",
            "User",
            "snapshot-only-secret must stay out of historical search",
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized snapshot");
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, &migrated)
            .expect("append migrated immutable record");

        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open history SIFS");
        let legacy_dir = format!("{SIFS_AGENT_CHANNELS_ROOT}/{channel_id}/messages");
        history_vfs
            .create_dir_all(&legacy_dir)
            .expect("create legacy message directory");
        let mut legacy_bytes = serde_json::to_vec(&legacy_only).expect("serialize legacy message");
        legacy_bytes.push(b'\n');
        legacy_bytes.extend(serde_json::to_vec(&migrated).expect("serialize migrated message"));
        legacy_bytes.push(b'\n');
        history_vfs
            .write(format!("{legacy_dir}/00000000.jsonl"), legacy_bytes)
            .expect("write bounded legacy chunk");

        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let request = |query: &str| SessionHistorySearchRequest {
            query: query.to_string(),
            cursor: None,
            limit: 10,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes: 1024 * 1024,
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let page = backend
            .search(
                &principal,
                &request("legacy-recall-needle"),
                &AgentCancellationToken::new(),
            )
            .expect("search legacy stable history");
        assert!(page.complete, "{page:#?}");
        let ids = page
            .matches
            .iter()
            .map(|matched| matched.item_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(page.matches.len(), 2, "{:#?}", page.matches);
        assert_eq!(ids.len(), 2, "migrated message must not be duplicated");
        assert!(ids.contains(&legacy_only.message_id));
        assert!(ids.contains(&migrated.message_id));

        let snapshot_only = backend
            .search(
                &principal,
                &request("snapshot-only-secret"),
                &AgentCancellationToken::new(),
            )
            .expect("search must ignore snapshot transcript");
        assert!(snapshot_only.matches.is_empty(), "{snapshot_only:#?}");
    }

    #[test]
    fn legacy_chunk_retries_after_remaining_page_budget_is_exhausted() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-legacy-history-budget-retry");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = sinomo_agent::AgentChannel::new(channel_id.clone(), "Legacy budget");
        channel.ensure_participant("main-agent", "Main");
        let first = channel.append_user_message(
            "app-user",
            "User",
            format!("first nonmatch {}", "a".repeat(256)),
            sinomo_agent::AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            "app-user",
            "User",
            format!("fresh-budget-needle {}", "b".repeat(4096)),
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open history SIFS");
        let legacy_dir = format!("{SIFS_AGENT_CHANNELS_ROOT}/{channel_id}/messages");
        history_vfs
            .create_dir_all(&legacy_dir)
            .expect("create legacy directory");
        let mut first_bytes = serde_json::to_vec(&first).expect("serialize first");
        first_bytes.push(b'\n');
        let mut second_bytes = serde_json::to_vec(&second).expect("serialize second");
        second_bytes.push(b'\n');
        history_vfs
            .write(format!("{legacy_dir}/00000000.jsonl"), &first_bytes)
            .expect("write first chunk");
        history_vfs
            .write(format!("{legacy_dir}/00000001.jsonl"), &second_bytes)
            .expect("write second chunk");

        let proof = channel_store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, usize::MAX, u64::MAX)
            .expect("measure authorization proof");
        assert!(proof.complete);
        let max_scanned_bytes = proof
            .scanned_bytes
            .saturating_mul(2)
            .saturating_add(second_bytes.len() as u64)
            .saturating_add(1);
        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let mut request = SessionHistorySearchRequest {
            query: "fresh-budget-needle".to_string(),
            cursor: None,
            limit: 10,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes,
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let first_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("first bounded page");
        let found_first = first_page
            .matches
            .iter()
            .any(|matched| matched.item_id == second.message_id);
        if !found_first {
            assert!(!first_page.complete, "{first_page:#?}");
            request.cursor = first_page.next_cursor;
            let second_page = backend
                .search(&principal, &request, &AgentCancellationToken::new())
                .expect("fresh-budget continuation");
            assert!(
                second_page
                    .matches
                    .iter()
                    .any(|matched| matched.item_id == second.message_id),
                "{second_page:#?}"
            );
        }
    }

    #[test]
    fn legacy_partial_tail_retries_at_its_own_fresh_range() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-legacy-history-partial-tail");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = sinomo_agent::AgentChannel::new(channel_id.clone(), "Legacy tail");
        channel.ensure_participant("main-agent", "Main");
        let first = channel.append_user_message(
            "app-user",
            "User",
            "small first record",
            sinomo_agent::AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            "app-user",
            "User",
            format!("partial-tail-needle {}", "b".repeat(8_192)),
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open history SIFS");
        let legacy_dir = format!("{SIFS_AGENT_CHANNELS_ROOT}/{channel_id}/messages");
        history_vfs
            .create_dir_all(&legacy_dir)
            .expect("create legacy directory");
        let mut first_bytes = serde_json::to_vec(&first).expect("serialize first");
        first_bytes.push(b'\n');
        let mut second_bytes = serde_json::to_vec(&second).expect("serialize second");
        second_bytes.push(b'\n');
        let mut chunk = first_bytes.clone();
        chunk.extend_from_slice(&second_bytes);
        history_vfs
            .write(format!("{legacy_dir}/00000000.jsonl"), chunk)
            .expect("write mixed-size legacy chunk");

        let proof = channel_store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, usize::MAX, u64::MAX)
            .expect("measure authorization proof");
        assert!(proof.complete);
        assert!(first_bytes.len() < second_bytes.len());
        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let mut request = SessionHistorySearchRequest {
            query: "partial-tail-needle".to_string(),
            cursor: None,
            limit: 10,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes: proof
                .scanned_bytes
                .saturating_mul(2)
                .saturating_add(second_bytes.len() as u64),
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let first_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("partial-tail page");
        assert!(!first_page.complete, "{first_page:#?}");
        assert!(first_page.matches.is_empty(), "{first_page:#?}");
        request.cursor = first_page.next_cursor;
        let second_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("fresh tail continuation");
        assert!(
            second_page
                .matches
                .iter()
                .any(|matched| matched.item_id == second.message_id),
            "{second_page:#?}"
        );
    }

    #[test]
    fn immutable_record_retries_after_remaining_body_budget_is_exhausted() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-immutable-history-budget-retry");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = sinomo_agent::AgentChannel::new(channel_id.clone(), "Immutable budget");
        channel.ensure_participant("main-agent", "Main");
        let left = channel.append_user_message(
            "app-user",
            "User",
            "immutable placeholder left",
            sinomo_agent::AgentChannelSource::app(),
        );
        let right = channel.append_user_message(
            "app-user",
            "User",
            "immutable placeholder right",
            sinomo_agent::AgentChannelSource::app(),
        );
        let (mut first, mut second) = if left.message_id < right.message_id {
            (left, right)
        } else {
            (right, left)
        };
        first.content = format!("first immutable nonmatch {}", "a".repeat(256));
        second.content = format!("immutable-budget-needle {}", "b".repeat(8_192));
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, &first)
            .expect("append first immutable record");
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, &second)
            .expect("append second immutable record");
        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open history SIFS");
        let proof = channel_store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, usize::MAX, u64::MAX)
            .expect("measure authorization proof");
        let immutable_dir = format!("{SIFS_AGENT_CHANNELS_ROOT}/{channel_id}/messages/records");
        let immutable_page = history_vfs
            .list_page_beneath_no_links_if_exists(&immutable_dir, None, 2)
            .expect("list immutable records in guarded traversal order");
        let immutable_entries = immutable_page.entries;
        assert_eq!(immutable_entries.len(), 2, "{immutable_entries:#?}");
        // Bind the small/large roles to the backend's actual traversal order;
        // the budget regression must not assume an ascending directory sort.
        let first_file_name = format!("{}.json", first.message_id);
        let (mut first, mut second) = if immutable_entries[0].name == first_file_name {
            (first, second)
        } else {
            (second, first)
        };
        first.content = format!("first immutable nonmatch {}", "a".repeat(256));
        second.content = format!("immutable-budget-needle {}", "b".repeat(8_192));
        for message in [&first, &second] {
            history_vfs
                .write(
                    format!("{immutable_dir}/{}.json", message.message_id),
                    serde_json::to_vec_pretty(message).expect("serialize ordered fixture"),
                )
                .expect("replace ordered immutable fixture");
        }
        let first_bytes = serde_json::to_vec_pretty(&first)
            .expect("serialize first")
            .len() as u64;
        let second_bytes = serde_json::to_vec_pretty(&second)
            .expect("serialize second")
            .len() as u64;
        assert!(
            second_bytes > first_bytes.saturating_add(proof.scanned_bytes),
            "test must leave a positive but insufficient second-record budget"
        );
        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let mut request = SessionHistorySearchRequest {
            query: "immutable-budget-needle".to_string(),
            cursor: None,
            limit: 10,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes: proof
                .scanned_bytes
                .saturating_mul(2)
                .saturating_add(second_bytes),
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let first_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("remaining-budget page");
        assert!(!first_page.complete, "{first_page:#?}");
        assert!(first_page.matches.is_empty(), "{first_page:#?}");
        assert_eq!(first_page.scanned.omitted_items, 0, "{first_page:#?}");
        request.cursor = first_page.next_cursor;
        let second_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("fresh immutable continuation");
        assert!(
            second_page
                .matches
                .iter()
                .any(|matched| matched.item_id == second.message_id),
            "{second_page:#?}"
        );
    }

    #[test]
    fn immutable_limit_one_continuation_does_not_skip_the_next_record() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-immutable-history-limit-one");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = sinomo_agent::AgentChannel::new(channel_id.clone(), "Immutable paging");
        channel.ensure_participant("main-agent", "Main");
        let first = channel.append_user_message(
            "app-user",
            "User",
            "limit-one-needle alpha",
            sinomo_agent::AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            "app-user",
            "User",
            "limit-one-needle beta",
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        for message in [&first, &second] {
            sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, message)
                .expect("append immutable record");
        }
        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let mut request = SessionHistorySearchRequest {
            query: "limit-one-needle".to_string(),
            cursor: None,
            limit: 1,
            max_scanned_activities: 16,
            max_scanned_items: 64,
            max_scanned_bytes: 1024 * 1024,
            max_excerpt_bytes: 256,
            max_title_bytes: 256,
            max_author_bytes: 128,
        };
        let first_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("first result page");
        assert_eq!(first_page.matches.len(), 1, "{first_page:#?}");
        request.cursor = first_page.next_cursor;
        let second_page = backend
            .search(&principal, &request, &AgentCancellationToken::new())
            .expect("second result page");
        assert_eq!(second_page.matches.len(), 1, "{second_page:#?}");
        let actual = [
            first_page.matches[0].item_id.clone(),
            second_page.matches[0].item_id.clone(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let expected = [first.message_id, second.message_id]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn authorized_channel_cannot_import_an_unauthorized_record_by_hardlink() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-history-cross-channel-hardlink");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");

        let authorized_id = SessionId::generate();
        let mut authorized =
            sinomo_agent::AgentChannel::new(authorized_id.clone(), "Authorized channel");
        authorized.ensure_participant("main-agent", "Main");
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &authorized)
            .expect("save authorized channel");

        let unauthorized_id = SessionId::generate();
        let mut unauthorized =
            sinomo_agent::AgentChannel::new(unauthorized_id.clone(), "Private channel");
        unauthorized.ensure_participant("other-agent", "Other");
        let secret = unauthorized.append_user_message(
            "app-user",
            "User",
            "cross-channel-hardlink-secret",
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &unauthorized)
            .expect("save private channel");
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &unauthorized_id, &secret)
            .expect("append private message");

        let history_vfs =
            mounted_sifs(Some(data_dir.root().to_path_buf())).expect("open history SIFS");
        let authorized_records =
            format!("{SIFS_AGENT_CHANNELS_ROOT}/{authorized_id}/messages/records");
        history_vfs
            .create_dir_all(&authorized_records)
            .expect("create authorized record directory");
        let private_record = format!(
            "{SIFS_AGENT_CHANNELS_ROOT}/{unauthorized_id}/messages/records/{}.json",
            secret.message_id
        );
        let imported_record = format!("{authorized_records}/{}.json", secret.message_id);
        let (sync, asynchronous) = crate::native_test_fixture::peer_transports(&data_dir).unwrap();
        let mut opened =
            sinomo_runtime::dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
                data_dir.clone(), sync, asynchronous,
            ).expect("open SIFS service through the actual adversarial fixture owner");
        opened
            .service
            .posix_hardlink(&private_record, &imported_record)
            .expect("hardlink private record under authorized channel");

        let backend = VfsAgentHistorySearchBackend::new(data_dir);
        let page = backend
            .search(
                &SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
                &SessionHistorySearchRequest {
                    query: "cross-channel-hardlink-secret".to_string(),
                    cursor: None,
                    limit: 10,
                    max_scanned_activities: 16,
                    max_scanned_items: 64,
                    max_scanned_bytes: 1024 * 1024,
                    max_excerpt_bytes: 256,
                    max_title_bytes: 256,
                    max_author_bytes: 128,
                },
                &AgentCancellationToken::new(),
            )
            .expect("search guarded channel history");
        assert!(page.matches.is_empty(), "{page:#?}");
        assert!(page.partial, "guard rejection must be explicit: {page:#?}");
    }

    #[test]
    fn membership_revoked_after_body_read_cannot_release_history_match() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-history-post-read-revocation");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel =
            sinomo_agent::AgentChannel::new(channel_id.clone(), "Revocation race channel");
        let participant_id = channel.ensure_participant("main-agent", "Main");
        sinomo_agent::AgentChannelStore::save_channel(&channel_store, &channel)
            .expect("save authorized channel");
        let message = channel.append_user_message(
            "app-user",
            "User",
            "post-read-revocation-secret",
            sinomo_agent::AgentChannelSource::app(),
        );
        sinomo_agent::AgentChannelStore::append_message(&channel_store, &channel_id, &message)
            .expect("append stable message");

        let hook_data_dir = data_dir.clone();
        let hook_channel_id = channel_id.clone();
        let hook_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_ran_in_callback = Arc::clone(&hook_ran);
        let _hook_guard = install_channel_history_after_body_read_hook(move || {
            let store =
                SifsAgentChannelStore::open(&hook_data_dir).expect("reopen channel for revocation");
            let mut current =
                sinomo_agent::AgentChannelStore::load_channel(&store, &hook_channel_id)
                    .expect("load channel for revocation")
                    .expect("channel still exists");
            assert!(current.disable_participant(&participant_id));
            sinomo_agent::AgentChannelStore::save_channel(&store, &current)
                .expect("persist post-read revocation");
            hook_ran_in_callback.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let page = VfsAgentHistorySearchBackend::new(data_dir)
            .search(
                &SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
                &SessionHistorySearchRequest {
                    query: "post-read-revocation-secret".to_string(),
                    cursor: None,
                    limit: 10,
                    max_scanned_activities: 16,
                    max_scanned_items: 64,
                    max_scanned_bytes: 1024 * 1024,
                    max_excerpt_bytes: 256,
                    max_title_bytes: 256,
                    max_author_bytes: 128,
                },
                &AgentCancellationToken::new(),
            )
            .expect("search stable history across revocation");

        assert!(
            hook_ran.load(std::sync::atomic::Ordering::SeqCst),
            "test must revoke membership after materializing the message body"
        );
        assert!(
            page.matches.is_empty(),
            "post-read proof must discard the revoked body: {page:#?}"
        );
    }

    #[test]
    fn history_backend_is_constructed_lazily_when_sifs_is_unavailable() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-history-lazy-sifs");
        let blocked_root = root.join("not-a-directory");
        std::fs::write(&blocked_root, b"blocked").expect("create unusable data root");
        let backend = VfsAgentHistorySearchBackend::new(SinomoDataDir::new(&blocked_root));

        assert!(
            backend
                .history_vfs
                .lock()
                .expect("history VFS cache")
                .is_none(),
            "registry construction must not eagerly open SIFS"
        );
        assert_eq!(
            backend.history_vfs().expect_err("SIFS open must fail"),
            SessionHistorySearchError::Unavailable {
                detail: "visible activity history is unavailable".to_string(),
            }
        );
        assert!(
            backend
                .history_vfs
                .lock()
                .expect("history VFS cache")
                .is_none(),
            "a failed lazy open must remain retryable"
        );
    }

    #[test]
    fn cursor_is_bound_to_agent_and_query() {
        let binding = cursor_binding("main-agent", "Release");
        let channel_id =
            SessionId::from_legacy_named("history_cursor_channel").expect("channel id");
        let cursor = encode_cursor(
            &HistoryCursor::Channel {
                channel_id: Some(channel_id.clone()),
                channel_cursor: Some("outer-cursor".to_string()),
                record_source: ChannelMessageSource::Legacy,
                record_cursor: Some("record-cursor".to_string()),
                record_entry: Some("00000001.jsonl".to_string()),
                record_object_id: Some("object:with:separators".to_string()),
                record_version: Some("version:with:separators".to_string()),
                record_offset: 42,
                byte_offset: 3,
                discarding_line: true,
            },
            &binding,
        );
        assert_eq!(
            parse_cursor(Some(&cursor), &binding).expect("cursor"),
            HistoryCursor::Channel {
                channel_id: Some(channel_id),
                channel_cursor: Some("outer-cursor".to_string()),
                record_source: ChannelMessageSource::Legacy,
                record_cursor: Some("record-cursor".to_string()),
                record_entry: Some("00000001.jsonl".to_string()),
                record_object_id: Some("object:with:separators".to_string()),
                record_version: Some("version:with:separators".to_string()),
                record_offset: 42,
                byte_offset: 3,
                discarding_line: true,
            }
        );
        assert!(parse_cursor(Some(&cursor), &cursor_binding("other", "Release")).is_err());
        assert!(parse_cursor(Some(&cursor), &cursor_binding("main-agent", "other")).is_err());
    }

    #[test]
    fn background_cursor_round_trips_the_vfs_continuation() {
        let binding = cursor_binding("main-agent", "Release");
        let position = HistoryCursor::Background {
            activity_cursor: Some("opaque-continuation".to_string()),
        };
        let cursor = encode_cursor(&position, &binding);

        assert_eq!(
            parse_cursor(Some(&cursor), &binding).expect("background cursor"),
            position
        );
    }

    #[test]
    fn post_read_authorization_reservation_is_bounded() {
        assert_eq!(
            reserve_post_read_authorization_budget(10, 1_000, 3, 250),
            Some((7, 750))
        );
        assert_eq!(
            reserve_post_read_authorization_budget(3, 1_000, 3, 250),
            None
        );
        assert_eq!(
            reserve_post_read_authorization_budget(10, 250, 3, 250),
            None
        );
    }

    #[test]
    fn channel_storage_errors_are_scrubbed_but_invalid_cursors_stay_typed() {
        let storage = channel_store_history_error(AgentChannelStoreError::Io {
            path: "/services/private/channel.json".to_string(),
            detail: "device secret".to_string(),
        });
        assert_eq!(
            storage,
            SessionHistorySearchError::Backend {
                detail: "visible channel history storage operation failed".to_string(),
            }
        );

        let cursor = channel_vfs_history_error(VfsError::InvalidCursor {
            detail: "cursor leaks /services/private/channel.json".to_string(),
        });
        assert_eq!(
            cursor,
            SessionHistorySearchError::InvalidCursor {
                detail: "filesystem continuation does not match visible channel history"
                    .to_string(),
            }
        );

        let nested_cursor = channel_store_history_error(AgentChannelStoreError::Io {
            path: "/services/private/channel.json".to_string(),
            detail: "invalid VFS directory cursor: cursor belongs to a private path".to_string(),
        });
        assert_eq!(cursor, nested_cursor);

        let backend = channel_vfs_history_error(VfsError::Backend {
            backend: "mounted-sifs".to_string(),
            path: "/services/private/channel.json".to_string(),
            detail: "device secret".to_string(),
        });
        assert_eq!(storage, backend);
    }

    #[test]
    fn background_activity_projection_is_stable_and_model_session_free() {
        let activity = AgentTaskActivityProjection {
            activity_id: sinomo_agent::TaskId::generate(),
            owner_agent_key: "main-agent".to_string(),
            activity_kind: "maintenance".to_string(),
            summary: "Compact observations".to_string(),
            status: AgentTaskLifecycleStatus::Completed,
            progress_summary: Some("wrote the bounded snapshot".to_string()),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        let text = background_activity_text(&activity);
        assert!(text.contains("Compact observations"));
        assert!(text.contains("completed"));
        assert!(text.contains("bounded snapshot"));
        assert!(!text.contains("private://"));
        assert!(agent_owns_background_activity(&activity, "main-agent"));
        assert!(!agent_owns_background_activity(&activity, "coding-agent"));
    }

    #[test]
    fn bounded_excerpt_keeps_a_late_match_visible() {
        let content = format!("{}needle{}", "a".repeat(200), "z".repeat(200));
        let (excerpt, truncated) = matching_excerpt(&content, "needle", 64, false);

        assert!(excerpt.contains("needle"), "{excerpt:?}");
        assert!(truncated);
        assert!(excerpt.len() <= 64);
    }
}
