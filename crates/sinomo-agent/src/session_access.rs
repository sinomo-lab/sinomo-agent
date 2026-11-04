//! Generic scoped access to canonical sessions and their activity journals.
use crate::{
    AgentChannelStore, AgentSessionStore, FileAgentChannelStore, FileAgentSessionStore, SessionId,
};
use sinomo_runtime::{
    local::SinomoDataDir, module::sessions::SessionAccess, module_api::sessions::*,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

pub struct AgentSessionAccess {
    store: Arc<dyn AgentSessionStore>,
    messages: Vec<ConversationMessage>,
    cancellation: Option<crate::AgentCancellationToken>,
    roots: BTreeMap<String, String>,
    members_cache: Mutex<Option<BTreeMap<String, SessionInfo>>>,
    commands:
        Option<Arc<dyn Fn(SessionCommand) -> Result<serde_json::Value, String> + Send + Sync>>,
}
impl AgentSessionAccess {
    pub fn new(store: Arc<dyn AgentSessionStore>, roots: BTreeMap<String, String>) -> Self {
        Self {
            store,
            messages: vec![],
            cancellation: None,
            roots,
            commands: None,
            members_cache: Mutex::new(None),
        }
    }
    pub fn with_cancellation(mut self, cancellation: crate::AgentCancellationToken) -> Self {
        self.cancellation = Some(cancellation);
        self
    }
    pub fn with_commands(
        mut self,
        commands: impl Fn(SessionCommand) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    ) -> Self {
        self.commands = Some(Arc::new(commands));
        self
    }
    pub fn for_conversation(
        data_dir: &SinomoDataDir,
        conversation_id: &str,
    ) -> Result<Self, String> {
        let id: SessionId = conversation_id
            .parse()
            .map_err(|_| "invalid conversation id")?;
        let store = Arc::new(FileAgentSessionStore::for_data_dir(data_dir));
        let mut roots = BTreeMap::new();
        let mut messages = Vec::new();
        if let Some(channel) = FileAgentChannelStore::for_data_dir(data_dir)
            .load_channel(&id)
            .map_err(|e| e.to_string())?
        {
            if channel.deleted_at.is_some() {
                return Err("conversation was deleted".into());
            }
            for message in &channel.transcript {
                let sender = match &message.author {
                    crate::AgentChannelAuthor::Agent {
                        agent_session_id, ..
                    } => Some(agent_session_id.to_string()),
                    _ => None,
                };
                let recipients = channel
                    .participants
                    .iter()
                    .filter(|p| match &message.audience {
                        crate::AgentChannelAudience::Everyone => true,
                        crate::AgentChannelAudience::Participants { participant_ids } => {
                            participant_ids.contains(&p.participant_id)
                        }
                    })
                    .map(|p| p.agent_session_id.to_string())
                    .filter(|id| Some(id) != sender.as_ref())
                    .collect();
                messages.push(ConversationMessage {
                    message_id: message.message_id.to_string(),
                    sequence: message.sequence,
                    recorded_at_unix_ms: (message.created_at.unix_timestamp_nanos() / 1_000_000)
                        as i64,
                    sender_session_id: sender,
                    recipients,
                    operation_id: message.produced_by.as_ref().map(|r| r.op_id.to_string()),
                    reply_to: message.reply_to.as_ref().map(|id| id.to_string()),
                    content: message.content.chars().take(4096).collect(),
                });
            }
            for participant in channel.participants {
                roots.insert(
                    participant.agent_session_id.to_string(),
                    participant.agent_name,
                );
            }
        } else if store
            .load_session(&id)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            roots.insert(id.to_string(), "Agent".into());
        } else {
            return Err("conversation not found".into());
        }
        let mut access = Self::new(store, roots);
        access.messages = messages;
        Ok(access)
    }
    fn members(&self) -> Result<BTreeMap<String, SessionInfo>, String> {
        if let Some(members) = self
            .members_cache
            .lock()
            .map_err(|_| "session scope cache poisoned")?
            .as_ref()
        {
            return Ok(members.clone());
        }
        let sessions = self.store.list_sessions().map_err(|e| e.to_string())?;
        let mut allowed: BTreeSet<_> = self.roots.keys().cloned().collect();
        loop {
            let previous = allowed.len();
            for session in &sessions {
                if session_relationship(session)
                    .as_ref()
                    .is_some_and(|r| allowed.contains(&r.parent_session_id))
                {
                    allowed.insert(session.session_id.to_string());
                }
            }
            if allowed.len() == previous {
                break;
            }
        }
        let mut result = BTreeMap::new();
        for session in sessions {
            let id = session.session_id.to_string();
            if !allowed.contains(&id) {
                continue;
            }
            let relationship = session_relationship(&session);
            result.insert(
                id.clone(),
                SessionInfo {
                    label: self
                        .roots
                        .get(&id)
                        .cloned()
                        .or_else(|| {
                            session
                                .managed
                                .as_ref()
                                .map(|s| s.name.clone())
                                .filter(|s| !s.is_empty())
                        })
                        .or(session.agent_definition_key)
                        .unwrap_or_else(|| "Agent".into()),
                    aliases: session
                        .managed
                        .as_ref()
                        .map(|s| vec![s.id.to_string()])
                        .unwrap_or_default(),
                    session_id: id,
                    relationship,
                    archived: session.managed.as_ref().is_some_and(|s| s.archived),
                },
            );
        }
        for (id, label) in &self.roots {
            result.entry(id.clone()).or_insert(SessionInfo {
                session_id: id.clone(),
                label: label.clone(),
                aliases: vec![],
                relationship: None,
                archived: false,
            });
        }
        *self
            .members_cache
            .lock()
            .map_err(|_| "session scope cache poisoned")? = Some(result.clone());
        Ok(result)
    }
}
impl SessionAccess for AgentSessionAccess {
    fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
    }
    fn request(&self, request: SessionRequest) -> Result<serde_json::Value, String> {
        if self.is_cancelled() {
            return Err("session invocation cancelled".into());
        }
        if let SessionRequest::Command { command } = request {
            let result = self
                .commands
                .as_ref()
                .ok_or("this session grant is read-only")?(command)?;
            *self
                .members_cache
                .lock()
                .map_err(|_| "session scope cache poisoned")? = None;
            return Ok(result);
        }
        let members = self.members()?;
        match request {
            SessionRequest::Messages {
                after_sequence,
                limit,
            } => {
                if limit == 0 || limit > MAX_SESSION_PAGE {
                    return Err("invalid message page limit".into());
                }
                let messages: Vec<_> = if let Some(after) = after_sequence {
                    self.messages
                        .iter()
                        .filter(|message| message.sequence > after)
                        .take(limit)
                        .collect()
                } else {
                    self.messages
                        .iter()
                        .rev()
                        .take(limit)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect()
                };
                serde_json::to_value(messages).map_err(|e| e.to_string())
            }
            SessionRequest::List {
                include_archived,
                include_related,
            } => {
                let entries: Vec<_> = members
                    .into_values()
                    .filter(|s| {
                        (include_archived || !s.archived)
                            && (include_related || self.roots.contains_key(&s.session_id))
                    })
                    .collect();
                if entries.len() > MAX_SESSION_PAGE {
                    return Err("session selection exceeds page limit".into());
                }
                serde_json::to_value(entries).map_err(|e| e.to_string())
            }
            SessionRequest::Inspect { session_id } => {
                if !members.contains_key(&session_id) {
                    return Err("session is outside the invocation grant".into());
                }
                let id = session_id.parse().map_err(|_| "invalid session id")?;
                serde_json::to_value(self.store.load_session(&id).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())
            }
            SessionRequest::Activity {
                session_id,
                after_sequence,
                tail,
                limit,
            } => {
                if !members.contains_key(&session_id) {
                    return Err("session is outside the invocation grant".into());
                }
                if limit == 0 || limit > MAX_SESSION_PAGE {
                    return Err("invalid activity page limit".into());
                }
                let id = session_id.parse().map_err(|_| "invalid session id")?;
                let mut records = self
                    .store
                    .load_item_records(&id)
                    .map_err(|e| e.to_string())?;
                let earlier = tail && records.len() > limit;
                if tail {
                    let start = records.len().saturating_sub(limit);
                    records = records.split_off(start);
                }
                let mut eligible = records
                    .into_iter()
                    .filter(|r| after_sequence.is_none_or(|after| r.seq > after));
                let mut page = SessionActivityPage {
                    records: vec![],
                    next_sequence: None,
                    has_more: false,
                };
                for record in eligible.by_ref().take(limit) {
                    let mut item = serde_json::to_value(&record.item).map_err(|e| e.to_string())?;
                    if bound_payload(&mut item) {
                        item["projection_truncated"] = serde_json::Value::Bool(true);
                    }
                    page.next_sequence = Some(record.seq);
                    page.records.push(SessionActivity {
                        session_id: session_id.clone(),
                        sequence: record.seq,
                        recorded_at_unix_ms: (record.recorded_at.unix_timestamp_nanos() / 1_000_000)
                            as i64,
                        operation_id: record.op_id.map(|v| v.to_string()),
                        tool_call_id: record.tool_call_id.map(|v| v.to_string()),
                        item,
                    });
                }
                page.has_more = earlier || eligible.next().is_some();
                serde_json::to_value(page).map_err(|e| e.to_string())
            }
            SessionRequest::Command { .. } => unreachable!(),
        }
    }
}
fn bound_payload(value: &mut serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) if s.len() > 4096 => {
            let mut end = 4096;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
            s.push_str(" [truncated]");
            true
        }
        serde_json::Value::Array(values) => {
            let truncated = values.len() > 32;
            values.truncate(32);
            values.iter_mut().fold(truncated, |truncated, value| {
                bound_payload(value) | truncated
            })
        }
        serde_json::Value::Object(values) => values
            .values_mut()
            .fold(false, |truncated, value| bound_payload(value) | truncated),
        _ => false,
    }
}

// Existing subagent archives predate the generic relationship field. Project
// their authoritative typed parent record without rewriting history.
fn session_relationship(session: &crate::AgentSession) -> Option<SessionRelationship> {
    session.relationship.clone().or_else(|| {
        session.managed.as_ref().and_then(|child| {
            child
                .parent_session_id
                .as_ref()
                .map(|parent| SessionRelationship {
                    parent_session_id: parent.to_string(),
                    kind: "delegation".into(),
                })
        })
    })
}
