//! Persistent Realtime transport for the ordinary agent model/tool loop.
//! The agent runtime retains tool execution, policy, cancellation and history.
//! One provider instance owns one fresh conversation; failed transports are not
//! reconnected or replayed because tool effects may already have happened.
use crate::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct RealtimeModelVideoObservation {
    pub frame: RealtimeEncodedVideoFrameInput,
    pub context: Option<String>,
}

pub trait RealtimeModelVideoSource: Send + Sync {
    /// Before a response, wait for evidence after preceding tool effects. During
    /// a response, return the newest available frame without blocking capture.
    fn observe(
        &self,
        before_response: bool,
    ) -> Result<Option<RealtimeModelVideoObservation>, String>;
}

type Observer = dyn Fn(&serde_json::Value) + Send + Sync;
#[derive(Default)]
struct WireState {
    updated: u64,
    terminal: Option<serde_json::Value>,
}
struct Conversation {
    runtime: tokio::runtime::Runtime,
    session: Option<RealtimeAudioSession>,
    messages: BTreeMap<usize, String>,
    outputs: BTreeMap<String, serde_json::Value>,
    outstanding: BTreeSet<String>,
    names: BTreeMap<String, String>,
    last_frame: Option<u64>,
    failure: Option<String>,
}

pub struct RealtimeModelProvider {
    config: RealtimeAudioSessionConfig,
    state: Mutex<Conversation>,
    wire: Arc<Mutex<WireState>>,
    video: Option<Arc<dyn RealtimeModelVideoSource>>,
    observer: Arc<Observer>,
}
impl RealtimeModelProvider {
    pub fn new(config: RealtimeAudioSessionConfig) -> Result<Self, LanguageModelError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| LanguageModelError::ProviderInvocationFailed {
                provider: config.provider.provider_id().into(),
                model: config.model.clone(),
                detail: e.to_string(),
            })?;
        Ok(Self {
            config,
            state: Mutex::new(Conversation {
                runtime,
                session: None,
                messages: BTreeMap::new(),
                outputs: BTreeMap::new(),
                outstanding: BTreeSet::new(),
                names: BTreeMap::new(),
                last_frame: None,
                failure: None,
            }),
            wire: Arc::new(Mutex::new(WireState::default())),
            video: None,
            observer: Arc::new(|_| {}),
        })
    }
    pub fn with_video_source(mut self, source: Arc<dyn RealtimeModelVideoSource>) -> Self {
        self.video = Some(source);
        self
    }
    pub fn with_observer(
        mut self,
        observer: impl Fn(&serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        self.observer = Arc::new(observer);
        self
    }
    pub fn close(&self) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state
            .failure
            .get_or_insert_with(|| "Realtime conversation is closed".into());
        if let Some(mut session) = state.session.take() {
            state.runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), session.close())
                    .await
                    .map_err(|_| "Realtime close timed out".to_string())?
                    .map_err(|e| e.to_string())
            })?;
        }
        Ok(())
    }
    fn error(&self, detail: String) -> LanguageModelError {
        let lower = detail.to_ascii_lowercase();
        if lower.contains("401")
            || lower.contains("invalid_api_key")
            || lower.contains("authentication")
        {
            LanguageModelError::AuthenticationRequired {
                provider: self.config.provider.provider_id().into(),
                detail,
            }
        } else {
            LanguageModelError::ProviderInvocationFailed {
                provider: self.config.provider.provider_id().into(),
                model: self.config.model.clone(),
                detail,
            }
        }
    }
    async fn frame(
        &self,
        session: &mut RealtimeAudioSession,
        last: &mut Option<u64>,
        before: bool,
    ) -> Result<(), String> {
        if let Some(source) = &self.video
            && let Some(mut observation) = source.observe(before)?
        {
            if let Some(config) = &self.config.input_video {
                observation.frame.detail = config.detail;
            }
            if before || *last != Some(observation.frame.sequence) {
                if let Some(context) = &observation.context {
                    session
                        .send_text(context)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                session
                    .send_encoded_video_frame(&observation.frame)
                    .await
                    .map_err(|e| e.to_string())?;
                *last = Some(observation.frame.sequence);
                (self.observer)(
                    &serde_json::json!({"type":"sinomo.realtime.video_sent","sequence":observation.frame.sequence,"detail":observation.frame.detail,"before_response":before,"context":observation.context}),
                );
            }
        }
        Ok(())
    }
    fn invoke(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        emit: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if request.model != self.config.model
            || request.provider != self.config.provider.provider_id()
        {
            return Err(
                self.error("Realtime provider is bound to one provider/model conversation".into())
            );
        }
        let mut state = self.state.lock().unwrap();
        if let Some(failure) = &state.failure {
            return Err(self.error(failure.clone()));
        }
        let Conversation {
            runtime,
            session,
            messages,
            outputs,
            outstanding,
            names,
            last_frame,
            ..
        } = &mut *state;
        let mut tools = Vec::new();
        for tool in &request.tools {
            let next = names.len();
            let wire_name = names
                .entry(tool.name.clone())
                .or_insert_with(|| format!("agent_tool_{next}"));
            tools.push(RealtimeAudioToolDefinition {
                name: wire_name.clone(),
                description: Some(format!("{}\n{}", tool.name, tool.description)),
                parameters: tool.input_schema.clone(),
            });
        }
        let instructions = request
            .messages
            .iter()
            .filter(|m| m.role == LanguageModelRole::System)
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        let items = if request.items.is_empty() {
            request
                .messages
                .iter()
                .map(|m| LanguageModelRequestItem::message(m.role.clone(), m.content.clone()))
                .collect()
        } else {
            request.items.clone()
        };
        emit(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let result=runtime.block_on(async {
            let expected_update=self.wire.lock().unwrap().updated+1;
            if session.is_none() {
                let mut config=self.config.clone();config.instructions=instructions.clone();config.tools=tools.clone();
                let wire=self.wire.clone();let observer=self.observer.clone();
                let connected=RealtimeAudioSession::connect(config).await.map_err(|e|e.to_string())?.with_server_event_observer(move |event|{
                    {let mut state=wire.lock().unwrap();if event["type"]=="session.updated"{state.updated+=1;}else if event["type"]=="response.done"{state.terminal=Some(event["response"].clone());}}
                    observer(event);
                });
                *session=Some(connected);
            }else {session.as_mut().unwrap().update_agent_context(&instructions,&tools,request.max_output_tokens).await.map_err(|e|e.to_string())?;}
            let session=session.as_mut().unwrap();
            let deadline=Instant::now()+self.config.receive_timeout();
            while self.wire.lock().unwrap().updated<expected_update {
                if cancellation.is_cancelled(){return Err("Realtime agent request cancelled".into());}
                if Instant::now()>deadline{return Err("Realtime session update timed out".into());}
                match session.receive_event_with_timeout(Duration::from_millis(100)).await {
                    Ok(RealtimeAudioEvent::Error{code,message})=>return Err(format!("Realtime {code}: {message}")),
                    Ok(_)|Err(RealtimeAudioError::Timeout{..})=>{},Err(error)=>return Err(error.to_string())
                }
            }
            for (index,item) in items.iter().enumerate() {
                match item {
                    LanguageModelRequestItem::Message{role:LanguageModelRole::User,content}=>{
                        if messages.get(&index)!=Some(content){session.send_text(content).await.map_err(|e|e.to_string())?;messages.insert(index,content.clone());}
                    }
                    LanguageModelRequestItem::Context{label,content}=>{
                        let text=format!("{label}\n{content}");if messages.get(&index)!=Some(&text){session.send_text(&text).await.map_err(|e|e.to_string())?;messages.insert(index,text);}
                    }
                    LanguageModelRequestItem::ToolCallOutput{call_id,output,..}=>{
                        if let Some(previous)=outputs.get(call_id) {if previous!=output{return Err("tool output changed after it was delivered".into());}continue;}
                        if !outstanding.remove(call_id){return Err(format!("unknown Realtime tool call id {call_id}; cannot replay effects into a new conversation"));}
                        session.send_tool_result(call_id,&output.to_string(),false).await.map_err(|e|e.to_string())?;
                        outputs.insert(call_id.clone(),output.clone());
                        (self.observer)(&serde_json::json!({"type":"sinomo.realtime.tool_result_sent","call_id":call_id,"output":output}));
                    }
                    _=>{}
                }
            }
            if !outstanding.is_empty(){return Err("agent did not supply every outstanding Realtime tool result".into());}
            self.frame(session,last_frame,true).await?;
            self.wire.lock().unwrap().terminal=None;
            session.request_response().await.map_err(|e|e.to_string())?;
            let deadline=Instant::now()+self.config.receive_timeout();
            let interval=self.config.input_video.as_ref().and_then(RealtimeVideoInputConfig::model_frame_interval).unwrap_or(Duration::from_millis(500));
            let mut next_frame=Instant::now()+interval;
            loop {
                if cancellation.is_cancelled(){return Err("Realtime agent request cancelled".into());}
                if Instant::now()>deadline{return Err("Realtime model response timed out".into());}
                if Instant::now()>=next_frame{self.frame(session,last_frame,false).await?;next_frame=Instant::now()+interval;}
                match session.receive_event_with_timeout(Duration::from_millis(50)).await {
                    Ok(RealtimeAudioEvent::OutputTranscriptDelta{delta,..})=>emit(LanguageModelStreamEvent::TextDelta{delta}),
                    Ok(RealtimeAudioEvent::Error{code,message})=>return Err(format!("Realtime {code}: {message}")),
                    Ok(RealtimeAudioEvent::Closed{reason})=>return Err(format!("Realtime session closed: {reason}")),
                    Ok(_)|Err(RealtimeAudioError::Timeout{..})=>{},Err(error)=>return Err(error.to_string())
                }
                let terminal=self.wire.lock().unwrap().terminal.take();
                if let Some(terminal)=terminal {
                    let response=completed_response(&terminal,names,&request.provider,&request.model)?;
                    for call in &response.tool_calls {
                        let id=call.call_id.as_ref().unwrap();
                        if outputs.contains_key(id)||!outstanding.insert(id.clone()){return Err("Realtime repeated a previously executed tool call id".into());}
                        emit(LanguageModelStreamEvent::ToolCallCompleted{call_id:call.call_id.clone(),name:call.name.clone(),input:call.input.clone()});
                    }
                    if let Some(usage)=&response.usage {emit(LanguageModelStreamEvent::Usage{usage:usage.clone()});}
                    emit(LanguageModelStreamEvent::Finished{finish_reason:Some("completed".into())});
                    return Ok(response);
                }
            }
        });
        if let Err(detail) = &result {
            state.failure = Some(detail.clone());
            if let Some(mut session) = state.session.take() {
                let _ = state.runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), session.close()).await
                });
            }
        }
        result.map_err(|e| self.error(e))
    }
}
impl Drop for RealtimeModelProvider {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl LanguageModelProvider for RealtimeModelProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.config.provider.provider_id().into(),
            default_model: self.config.model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: LanguageModelProviderCapabilities {
                realtime_websocket: true,
                ..Default::default()
            },
            supports_json_object: false,
            supports_tool_permissions: false,
        }
    }
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.invoke(request, &LanguageModelCancellation::default(), &mut |_| {})
    }
    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        emit: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.invoke(request, cancellation, emit)
    }
}

fn completed_response(
    value: &serde_json::Value,
    names: &BTreeMap<String, String>,
    provider: &str,
    model: &str,
) -> Result<LanguageModelResponse, String> {
    if value["status"] != "completed" {
        return Err(format!(
            "Realtime response did not complete: {} {}",
            value["status"], value["status_details"]
        ));
    }
    let mut text = String::new();
    let mut calls = Vec::new();
    for item in value["output"]
        .as_array()
        .ok_or("Realtime response output is absent")?
    {
        if item["type"] == "function_call" {
            let wire = item["name"].as_str().ok_or("function name absent")?;
            let name = names
                .iter()
                .find_map(|(name, id)| (id == wire).then(|| name.clone()))
                .ok_or("function was not advertised")?;
            let call_id = item["call_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or("function call id absent")?
                .to_string();
            let input = serde_json::from_str(
                item["arguments"]
                    .as_str()
                    .ok_or("function arguments absent")?,
            )
            .map_err(|e| format!("invalid function JSON: {e}"))?;
            calls.push(LanguageModelToolCall {
                call_id: Some(call_id),
                name,
                input,
                summary: None,
            });
        } else if item["type"] == "message"
            && let Some(parts) = item["content"].as_array()
        {
            for part in parts {
                if let Some(content) = part["transcript"]
                    .as_str()
                    .or_else(|| part["text"].as_str())
                {
                    text.push_str(content);
                }
            }
        }
    }
    let usage = crate::usage::openai(value);
    let items = calls
        .iter()
        .cloned()
        .map(|call| LanguageModelResponseItem::ToolCall { call })
        .collect();
    Ok(LanguageModelResponse {
        provider: provider.into(),
        model: model.into(),
        content: text,
        items,
        tool_calls: calls,
        usage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn realtime_tools_require_successful_terminal_status_and_preserve_call_ids() {
        let names = BTreeMap::from([("module.control".into(), "agent_tool_0".into())]);
        let mut reply = serde_json::json!({"status":"completed","output":[{"type":"function_call","name":"agent_tool_0","call_id":"call_7","arguments":"{\"x\":4}"}],"usage":{"input_tokens":15,"output_tokens":8}});
        let result = completed_response(&reply, &names, "codex", "gpt-realtime-2").unwrap();
        assert_eq!(result.tool_calls[0].name, "module.control");
        assert_eq!(result.tool_calls[0].call_id.as_deref(), Some("call_7"));
        assert_eq!(result.usage.unwrap().input_tokens, Some(15));
        for status in ["cancelled", "failed", "incomplete"] {
            reply["status"] = status.into();
            assert!(completed_response(&reply, &names, "codex", "gpt-realtime-2").is_err());
        }
    }
}
