//! Conversation routing and compatibility with earlier client-owned preferences.
use super::*;
use crate::models::AgentKind;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChatConfiguration {
    pub selection: Option<ModelSelection>,
    pub agent: AgentKind,
    #[serde(default)]
    pub approval_mode: Option<crate::models::ApprovalMode>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ClientWorkspaceSettings {
    pub selection: Option<ModelSelection>,
    pub dispatch_settings: dispatch::DispatchSettings,
}

pub(super) fn chat_scope_key(query: &ScopeQuery) -> String {
    if let Some(id) = &query.conversation_id {
        return conversations::key(id);
    }
    match &query.client_id {
        Some(id) => format!(
            "client:{:x}:{}",
            Sha256::digest(id.as_bytes()),
            scope_key(query)
        ),
        None => scope_key(query),
    }
}

pub(super) fn settings(memory: &Memory, query: &ScopeQuery) -> ChatConfiguration {
    if let Some(conversation) = query
        .conversation_id
        .as_ref()
        .and_then(|id| memory.conversations.get(id))
    {
        return conversation.configuration.clone();
    }
    if let Some(settings) = query
        .client_id
        .as_ref()
        .and_then(|id| memory.clients.get(id))
    {
        ChatConfiguration {
            selection: settings.selection.clone(),
            agent: settings.dispatch_settings.agent,
            approval_mode: None,
        }
    } else {
        ChatConfiguration {
            selection: memory.model_selection.clone(),
            agent: memory.dispatch_settings.agent,
            approval_mode: None,
        }
    }
}

// Call only after normal Bridge authentication, which binds this ID to its token.
pub(super) async fn client_scope(
    headers: &HeaderMap,
    state: &AppState,
    mut query: ScopeQuery,
) -> Result<ScopeQuery, ApiError> {
    if headers
        .get("x-omni-code-workspace-scope")
        .and_then(|value| value.to_str().ok())
        == Some("conversation")
    {
        return conversations::scope(headers, state, query).await;
    }
    if headers
        .get("x-omni-code-workspace-scope")
        .and_then(|v| v.to_str().ok())
        != Some("client")
    {
        return Ok(query);
    }
    let id = headers
        .get("x-omni-code-client-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| failure("Missing authenticated workspace client".into()))?
        .to_owned();
    query.client_id = Some(id.clone());
    let mut memory = state.workspace_agent.memory.lock().await;
    if !memory.clients.contains_key(&id) {
        let mut next = memory.clone();
        // The first upgraded client keeps the pre-isolation conversation and tasks.
        // Later clients start their own conversation without copying another client's history.
        if next.clients.is_empty() {
            for (project_id, entries) in memory
                .chats
                .iter()
                .filter(|(key, _)| !key.starts_with("client:") && !key.starts_with("conversation:"))
            {
                let scope = ScopeQuery {
                    conversation_id: None,
                    project_id: (!project_id.is_empty()).then(|| project_id.clone()),
                    session_id: None,
                    client_id: Some(id.clone()),
                };
                next.chats.insert(chat_scope_key(&scope), entries.clone());
            }
            for task in &mut next.dispatch_tasks {
                if task.client_id.is_none() && task.conversation_id.is_none() {
                    task.client_id = Some(id.clone());
                }
            }
            for request in &mut next.dispatch_requests {
                if request.client_id.is_none() && request.conversation_id.is_none() {
                    request.client_id = Some(id.clone());
                    request.scope_key = chat_scope_key(&ScopeQuery {
                        conversation_id: None,
                        project_id: (!request.scope_key.is_empty())
                            .then(|| request.scope_key.clone()),
                        session_id: None,
                        client_id: Some(id.clone()),
                    });
                }
            }
        }
        next.clients.insert(
            id,
            ClientWorkspaceSettings {
                selection: memory.model_selection.clone(),
                dispatch_settings: memory.dispatch_settings.clone(),
            },
        );
        state.workspace_agent.save(&next).await.map_err(failure)?;
        *memory = next;
    }
    Ok(query)
}

pub(super) async fn chat_lock(state: &AppState, query: &ScopeQuery) -> Option<Arc<Mutex<()>>> {
    if query.conversation_id.is_none() && query.client_id.is_none() {
        return None;
    }
    Some(
        state
            .workspace_agent
            .client_chat_locks
            .lock()
            .await
            .entry(chat_scope_key(query))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(),
    )
}

pub(super) async fn validate_configuration(
    state: &AppState,
    config: &ChatConfiguration,
) -> Result<(), ApiError> {
    if let Some(selection) = &config.selection {
        if selection.model.trim().is_empty() || selection.model.chars().count() > 200 {
            return Err(failure("Model must contain 1–200 characters".into()));
        }
        if !state
            .bridge_settings()
            .await
            .model_providers
            .iter()
            .any(|provider| {
                provider.id == selection.provider_id
                    && provider.enabled
                    && provider.format != ApiFormat::Acp
            })
        {
            return Err(failure("The selected model provider is unavailable".into()));
        }
    }
    Ok(())
}
