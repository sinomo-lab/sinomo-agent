use super::*;
use sinomo_runtime::api::app::chat::{
    ChatHistoryLoadMode, ChatHistoryLoadRequest, ChatHistorySources,
};

fn message_set_hash(ids: &BTreeSet<String>) -> String {
    let mut hash = blake3::Hasher::new();
    for id in ids {
        hash.update(&(id.len() as u64).to_le_bytes());
        hash.update(id.as_bytes());
    }
    hash.finalize().to_hex().to_string()
}

impl SifsAgentChannelStore {
    /// Read the current transcript-free projection when available. A changed
    /// metadata directory, a withdrawn source or an older layout requires the
    /// ordinary metadata fold. The desktop cursor never grants visibility.
    pub fn transcript_channel_kind(
        &self,
        id: &SessionId,
    ) -> Result<Option<AgentChannelKind>, AgentChannelStoreError> {
        let path = self.history_metadata_projection_path(id);
        if let Ok(bytes) = self.browser.read_file(&path)
            && let Ok(projection) =
                serde_json::from_slice::<SifsAgentChannelHistoryMetadataProjection>(&bytes)
            && projection.source_record_id.len() == 64
            && projection
                .source_record_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            && let Ok(current) = self.browser.stat(&self.metadata_records_dir(id))
            && projection.matches_revision(id, Some(&current.object_id), Some(&current.version))
            && self
                .browser
                .stat(&self.metadata_record_path(id, &projection.source_record_id))
                .is_ok()
        {
            return Ok(
                (!projection.channel.is_deleted()).then_some(projection.channel.channel_kind)
            );
        }
        Ok(self
            .load_channel_metadata_only(id)?
            .filter(|channel| !channel.is_deleted())
            .map(|channel| channel.channel_kind))
    }

    // Unlike immutable records, a legacy JSONL chunk can gain messages under
    // the same name. Only skip it if the current bytes match the last read.
    fn changed_chunks(
        &self,
        paths: &[String],
        previous: &ChatHistorySources,
        sources: &mut ChatHistorySources,
    ) -> Result<Vec<String>, AgentChannelStoreError> {
        let mut changed = Vec::new();
        for path in paths {
            if let Some(hash) = previous.legacy_chunks.get(path) {
                match self.browser.stat(path) {
                    Ok(stat) if stat.content_hash.as_ref() == Some(hash) => {
                        sources.legacy_chunks.insert(path.clone(), hash.clone());
                        continue;
                    }
                    Ok(_) => {}
                    Err(detail) if sifs_error_is_not_found(&detail) => continue,
                    Err(detail) => return Err(channel_store_io(path, detail)),
                }
            }
            changed.push(path.clone());
        }
        Ok(changed)
    }

    fn read_history_chunks<T: DeserializeOwned>(
        &self,
        paths: &[String],
        sources: &mut ChatHistorySources,
    ) -> Result<Vec<T>, AgentChannelStoreError> {
        let mut records = Vec::new();
        for (path, bytes) in self.record_bytes(paths)? {
            records.extend(parse_json_lines::<T>(&bytes)?);
            sources
                .legacy_chunks
                .insert(path, format!("blake3:{}", blake3::hash(&bytes)));
        }
        Ok(records)
    }

    fn transcript_turns(
        &self,
        id: &SessionId,
        previous: &ChatHistorySources,
        sources: &mut ChatHistorySources,
    ) -> Result<Vec<AgentChannelTurn>, AgentChannelStoreError> {
        let chunks = self.paths_in_dir_with_suffix(&self.turn_chunks_dir(id), ".jsonl")?;
        let changed = self.changed_chunks(&chunks, previous, sources)?;
        let paths = self.paths_in_dir_with_suffix(&self.turn_records_dir(id), ".json")?;
        // Mixed layouts can contain the same turn in both formats. When either
        // changes, check their bodies together for conflicting turn identities.
        let check_mixed = !chunks.is_empty()
            && (!changed.is_empty()
                || paths.iter().any(|p| {
                    !json_record_id_from_path(p)
                        .is_some_and(|id| previous.turn_records.contains(id))
                }));
        let mut turns =
            self.read_history_chunks(if check_mixed { &chunks } else { &changed }, sources)?;
        let mut unread = Vec::new();
        for path in paths {
            if let Some(turn_id) = json_record_id_from_path(&path)
                && !check_mixed
                && previous.turn_records.contains(turn_id)
            {
                sources.turn_records.insert(turn_id.to_string());
            } else {
                unread.push(path);
            }
        }
        for (path, turn) in self.load_json_documents_with_paths::<AgentChannelTurn>(&unread)? {
            if turn.channel_id != *id
                || json_record_id_from_path(&path) != Some(turn.turn_id.as_str())
            {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!("turn identity differs from record {path}"),
                });
            }
            sources.turn_records.insert(turn.turn_id.to_string());
            turns.push(turn);
        }
        canonicalize_channel_turns(turns)
    }

    pub fn read_transcript(
        &self,
        id: &SessionId,
        request: &ChatHistoryLoadRequest,
    ) -> Result<AgentChannelTranscript, AgentChannelStoreError> {
        use ChatHistoryLoadMode::*;
        let known = request
            .known_message_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let delta = matches!(request.mode, Initial | ReconcileLatest) && !known.is_empty();
        let full = !delta || request.include_overview;
        let empty = ChatHistorySources::default();
        let previous = if full {
            &empty
        } else {
            request.known_sources.as_ref().unwrap_or(&empty)
        };
        let mut sources = ChatHistorySources::default();

        // Observe committed turns before the per-message directory. A turn may
        // be the only durable copy of its outbox after a writer crashes.
        let turns = self.transcript_turns(id, previous, &mut sources)?;
        let chunks = self.paths_in_dir_with_suffix(&self.message_chunks_dir(id), ".jsonl")?;
        let previous_chunks = if previous
            .legacy_message_set
            .as_ref()
            .is_some_and(|h| h == &message_set_hash(&known))
        {
            previous
        } else {
            &empty
        };
        let changed = self.changed_chunks(&chunks, previous_chunks, &mut sources)?;
        let mut messages =
            self.read_history_chunks::<AgentChannelMessage>(&changed, &mut sources)?;
        let paths: Vec<_> = self
            .paths_in_dir_with_suffix(&self.message_records_dir(id), ".json")?
            .into_iter()
            .filter(|p| json_record_id_from_path(p).is_some_and(|mid| full || !known.contains(mid)))
            .collect();
        let mut outbox = BTreeMap::new();
        for message in turns.into_iter().flat_map(|turn| turn.outbox_messages) {
            let path = self.message_record_path(id, &message.message_id);
            match outbox.entry(path) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(message);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &message => {
                    return Err(AgentChannelStoreError::Serialization {
                        detail: format!("conflicting turn outboxes for {}", entry.key()),
                    });
                }
                _ => {}
            }
        }
        for (path, bytes) in self.message_record_bytes(&paths)? {
            // Match the immutable writer's byte-for-byte conflict check using
            // the body already fetched for this page.
            let message: AgentChannelMessage = if let Some(expected) = outbox.remove(&path) {
                let encoded = serde_json::to_vec_pretty(&expected).map_err(|e| {
                    AgentChannelStoreError::Serialization {
                        detail: e.to_string(),
                    }
                })?;
                if bytes != encoded {
                    return Err(channel_store_io(
                        &path,
                        "immutable channel record already exists with different content".into(),
                    ));
                }
                expected
            } else {
                Self::decode_record(&path, &bytes)?
            };
            if json_record_id_from_path(&path) != Some(message.message_id.as_str()) {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!("message identity differs from record {path}"),
                });
            }
            messages.push(message);
        }
        for (_, message) in outbox {
            // Also checks a known ID claimed by a newly discovered turn.
            // Another writer may already have materialized the record.
            self.append_message_record(id, &message)?;
            messages.push(message);
        }
        let mut overview = None;
        let reconciliation = if delta {
            if request.include_overview {
                let ordered = canonicalize_channel_messages(messages.clone())?;
                overview = Some(Self::overview_from_messages(id, &ordered));
            }
            messages.retain(|m| !known.contains(m.message_id.as_str()));
            Self::delta_from_messages(id, messages, &known)?
        } else {
            let ordered = canonicalize_channel_messages(messages)?;
            overview = request
                .include_overview
                .then(|| Self::overview_from_messages(id, &ordered));
            let total = ordered.len() as u64;
            let mut discovered_message_ids = Vec::new();
            let page = match request.mode {
                Initial | ReconcileLatest => {
                    discovered_message_ids =
                        ordered.iter().map(|m| m.message_id.to_string()).collect();
                    discovered_message_ids.sort();
                    let start = total.saturating_sub(request.limit as u64).saturating_add(1);
                    Self::message_page_from_records(id, ordered, start, total, request.limit)
                }
                Before => Self::page_before_messages(
                    id,
                    ordered,
                    request.sequence.unwrap_or(1),
                    request.limit,
                ),
                After => Self::page_from_messages(
                    id,
                    ordered,
                    request.sequence.unwrap_or(0).saturating_add(1),
                    request.limit,
                ),
            };
            AgentChannelMessageReconciliation {
                page,
                discovered_message_ids,
            }
        };
        if !chunks.is_empty() && matches!(request.mode, Initial | ReconcileLatest) {
            let mut acknowledged = known;
            acknowledged.extend(reconciliation.discovered_message_ids.iter().cloned());
            sources.legacy_message_set = Some(message_set_hash(&acknowledged));
        }
        Ok(AgentChannelTranscript {
            reconciliation,
            overview,
            sources,
        })
    }
}
