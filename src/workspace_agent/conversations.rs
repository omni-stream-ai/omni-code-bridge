//! Main-agent conversations are resumable; task progress and durable knowledge are shared.
use super::*;
use axum::routing::delete;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Conversation {
    id: String,
    title: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    pub configuration: ChatConfiguration,
    #[serde(default)]
    pub active_project_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct MemoryNote {
    id: String,
    content: String,
    project_id: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MemoryUpdate {
    #[serde(default)]
    id: Option<String>,
    content: String,
}

pub(super) fn key(id: &str) -> String {
    format!("conversation:{id}")
}

fn title(entries: &[ChatEntry]) -> String {
    let title: String = entries
        .iter()
        .find(|entry| entry.role == "user")
        .map(|entry| {
            entry
                .content
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
        .chars()
        .take(80)
        .collect();
    if title.is_empty() {
        "Attachment".into()
    } else {
        title
    }
}

// Preserve old shared/client-scoped chats as selectable history, never as a new app's context.
fn migrate(memory: &mut Memory) {
    if memory.conversation_history_migrated {
        return;
    }
    let mut old = memory
        .chats
        .iter()
        .filter(|(scope, entries)| !scope.starts_with("conversation:") && !entries.is_empty())
        .map(|(scope, entries)| (scope.clone(), entries.clone()))
        .collect::<Vec<_>>();
    old.sort_by_key(|(scope, _)| !scope.starts_with("client:"));
    let mut copies = HashMap::<String, String>::new();
    let mut scopes = HashMap::<String, String>::new();
    for (scope, mut entries) in old {
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&entries).unwrap_or_default())
        );
        let project = if scope.starts_with("client:") {
            scope.splitn(3, ':').nth(2).unwrap_or_default()
        } else {
            scope.as_str()
        };
        for entry in &mut entries {
            if entry.proposal.is_some() {
                entry.proposal_project_id = (!project.is_empty()).then(|| project.to_string());
            }
        }
        let id = if let Some(id) = copies.get(&digest) {
            id.clone()
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            let settings = memory
                .clients
                .iter()
                .find(|(client, _)| {
                    scope.starts_with(&format!("client:{:x}:", Sha256::digest(client.as_bytes())))
                })
                .map(|(_, settings)| ChatConfiguration {
                    selection: settings.selection.clone(),
                    agent: settings.dispatch_settings.agent,
                    approval_mode: None,
                })
                .unwrap_or_else(|| ChatConfiguration {
                    selection: memory.model_selection.clone(),
                    agent: memory.dispatch_settings.agent,
                    approval_mode: None,
                });
            memory.conversations.insert(
                id.clone(),
                Conversation {
                    id: id.clone(),
                    title: title(&entries),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    configuration: settings,
                    active_project_id: (!project.is_empty()).then(|| project.to_string()),
                },
            );
            memory.chats.insert(key(&id), entries);
            copies.insert(digest, id.clone());
            id
        };
        scopes.insert(scope, id);
    }
    for request in &mut memory.dispatch_requests {
        if let Some(id) = scopes.get(&request.scope_key) {
            request.conversation_id = Some(id.clone());
            request.scope_key = key(id);
            for task in memory
                .dispatch_tasks
                .iter_mut()
                .filter(|task| task.request_id == request.id)
            {
                task.conversation_id = Some(id.clone());
            }
        }
    }
    memory.conversation_history_migrated = true;
}

pub(super) async fn scope(
    headers: &HeaderMap,
    state: &AppState,
    mut query: ScopeQuery,
) -> Result<ScopeQuery, ApiError> {
    let id = headers
        .get("x-omni-code-conversation-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .ok_or_else(|| failure("A valid main conversation ID is required".into()))?
        .to_string();
    query.conversation_id = Some(id.clone());
    query.client_id = None;
    let default_mode = state.bridge_settings().await.default_approval_mode;
    let mut memory = state.workspace_agent.memory.lock().await;
    if !memory.conversation_history_migrated || !memory.conversations.contains_key(&id) {
        let mut next = memory.clone();
        migrate(&mut next);
        next.conversations
            .entry(id.clone())
            .or_insert_with(|| Conversation {
                id,
                title: String::new(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                configuration: ChatConfiguration {
                    selection: memory.model_selection.clone(),
                    agent: memory.dispatch_settings.agent,
                    approval_mode: Some(default_mode),
                },
                active_project_id: None,
            });
        state.workspace_agent.save(&next).await.map_err(failure)?;
        *memory = next;
    }
    Ok(query)
}

pub(super) fn touch(memory: &mut Memory, query: &ScopeQuery, message: &str) {
    if let Some(conversation) = query
        .conversation_id
        .as_ref()
        .and_then(|id| memory.conversations.get_mut(id))
    {
        if conversation.title.is_empty() {
            conversation.title = message
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .chars()
                .take(80)
                .collect();
            if conversation.title.is_empty() {
                conversation.title = "Attachment".into();
            }
        }
        conversation.updated_at = Utc::now();
    }
}

pub(super) fn memories(memory: &Memory, query: &ScopeQuery) -> Value {
    json!(
        memory
            .long_term_memory
            .iter()
            .filter(|note| query
                .project_id
                .as_ref()
                .is_none_or(|id| note.project_id.as_ref().is_none_or(|project| project == id)))
            .collect::<Vec<_>>()
    )
}

pub(super) fn apply_memories(
    memory: &mut Memory,
    query: &ScopeQuery,
    updates: Vec<MemoryUpdate>,
) -> Result<(), ApiError> {
    if updates.len() > 8 {
        return Err(failure("Remember at most 8 facts per message".into()));
    }
    for update in updates {
        let content = update.content.trim().to_string();
        if content.chars().count() > 1000 {
            return Err(failure(
                "A remembered fact is limited to 1000 characters".into(),
            ));
        }
        if let Some(id) = update.id {
            let index = memory
                .long_term_memory
                .iter()
                .position(|note| {
                    note.id == id
                        && query
                            .project_id
                            .as_ref()
                            .is_none_or(|project| note.project_id.as_ref() == Some(project))
                })
                .ok_or_else(|| failure("Remembered fact not found in this scope".into()))?;
            if content.is_empty() {
                memory.long_term_memory.remove(index);
            } else {
                memory.long_term_memory[index].content = content;
                memory.long_term_memory[index].updated_at = Utc::now();
            }
        } else if !content.is_empty()
            && !memory
                .long_term_memory
                .iter()
                .any(|note| note.project_id == query.project_id && note.content == content)
        {
            if memory.long_term_memory.len() >= 200 {
                return Err(failure(
                    "Long-term memory is full; remove outdated facts first".into(),
                ));
            }
            memory.long_term_memory.push(MemoryNote {
                id: uuid::Uuid::new_v4().to_string(),
                content,
                project_id: query.project_id.clone(),
                updated_at: Utc::now(),
            });
        }
    }
    Ok(())
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/workspace-agent/conversations", get(history))
        .route(
            "/workspace-agent/approval-mode",
            get(approval_mode).put(save_approval_mode),
        )
        .route("/workspace-agent/memories", get(get_memories))
        .route("/workspace-agent/memories/{id}", delete(forget))
}

async fn approval_mode(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let query = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let mode = client_scope::settings(&*state.workspace_agent.memory.lock().await, &query)
        .approval_mode
        .unwrap_or_default();
    Ok(Json(ApiResponse {
        data: json!({"approval_mode": mode}),
    }))
}

#[derive(Deserialize)]
struct ApprovalModeInput {
    approval_mode: crate::models::ApprovalMode,
}

async fn save_approval_mode(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<ApprovalModeInput>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let query = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let mut memory = state.workspace_agent.memory.lock().await;
    let mut next = memory.clone();
    let conversation = query
        .conversation_id
        .as_ref()
        .and_then(|id| next.conversations.get_mut(id))
        .ok_or_else(|| failure("Select an assistant conversation first".into()))?;
    conversation.configuration.approval_mode = Some(input.approval_mode);
    state.workspace_agent.save(&next).await.map_err(failure)?;
    if let Err(error) = state.remember_approval_mode(input.approval_mode).await {
        let _ = state.workspace_agent.save(&memory).await;
        return Err(failure(error));
    }
    *memory = next;
    Ok(Json(ApiResponse {
        data: json!({"approval_mode": input.approval_mode}),
    }))
}

pub(super) async fn history(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let _ = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let memory = state.workspace_agent.memory.lock().await;
    let mut conversations = memory
        .conversations
        .values()
        .filter(|conversation| !conversation.title.is_empty())
        .collect::<Vec<_>>();
    conversations.sort_by_key(|conversation| std::cmp::Reverse(conversation.updated_at));
    Ok(Json(ApiResponse { data: json!(conversations.iter().map(|conversation| {
        let entries = memory.chats.get(&key(&conversation.id));
        json!({"id":conversation.id, "title":conversation.title, "created_at":conversation.created_at,
            "updated_at":conversation.updated_at, "message_count":entries.map_or(0, Vec::len),
            "preview":entries.and_then(|entries| entries.last()).map(|entry| entry.content.chars().take(180).collect::<String>())})
    }).collect::<Vec<_>>()) }))
}

async fn get_memories(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ScopeQuery>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let memory = state.workspace_agent.memory.lock().await;
    Ok(Json(ApiResponse {
        data: memories(&memory, &query),
    }))
}

pub(super) async fn forget(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let mut memory = state.workspace_agent.memory.lock().await;
    let mut next = memory.clone();
    next.long_term_memory.retain(|note| note.id != id);
    state.workspace_agent.save(&next).await.map_err(failure)?;
    *memory = next;
    Ok(Json(ApiResponse {
        data: json!({"deleted":true}),
    }))
}

#[cfg(test)]
mod approval_tests {
    use super::*;
    use crate::models::ApprovalMode;

    fn headers(fixture: &crate::acceptance_test_support::Fixture, id: &str) -> HeaderMap {
        let mut headers = fixture.headers.clone();
        headers.insert(
            "x-omni-code-workspace-scope",
            "conversation".parse().unwrap(),
        );
        headers.insert("x-omni-code-conversation-id", id.parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn approval_mode_save_failure_preserves_existing_session_and_assistant_choices() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let state = fixture.state.clone();
        let id = uuid::Uuid::new_v4().to_string();
        let _ = approval_mode(headers(&fixture, &id), State(state.clone()))
            .await
            .unwrap();
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Failed save".into(),
                root_path: fixture.root.to_string_lossy().into(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                client_session_id: uuid::Uuid::new_v4().to_string(),
                project_id: project.id,
                title: None,
                agent: crate::models::AgentKind::Custom,
                brief_reply_mode: false,
                approval_mode: None,
                model: None,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .unwrap();
        let path = fixture.root.join("settings.json");
        let _ = std::fs::remove_file(&path);
        std::fs::create_dir(&path).unwrap();
        assert!(
            state
                .update_session_approval_mode(&session.id, ApprovalMode::Auto)
                .await
                .is_err()
        );
        assert_eq!(
            state.find_session(&session.id).await.unwrap().approval_mode,
            ApprovalMode::Smart
        );
        assert!(
            save_approval_mode(
                headers(&fixture, &id),
                State(state.clone()),
                Json(ApprovalModeInput {
                    approval_mode: ApprovalMode::Auto
                })
            )
            .await
            .is_err()
        );
        assert_eq!(
            approval_mode(headers(&fixture, &id), State(state.clone()))
                .await
                .unwrap()
                .0
                .data["approval_mode"],
            "smart"
        );
        let restored = AppState::new_with_paths(
            fixture.root.join("settings.json"),
            fixture.root.join("runtime.json"),
            fixture.root.join("metadata.json"),
        )
        .await;
        assert_eq!(
            restored
                .find_session(&session.id)
                .await
                .unwrap()
                .approval_mode,
            ApprovalMode::Smart
        );
        let query = ScopeQuery {
            conversation_id: Some(id),
            ..Default::default()
        };
        assert_eq!(
            client_scope::settings(&*restored.workspace_agent.memory.lock().await, &query)
                .approval_mode,
            Some(ApprovalMode::Smart)
        );
    }

    #[tokio::test]
    async fn assistant_approval_modes_are_isolated_and_new_conversations_remember_last_choice() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let state = fixture.state.clone();
        let a = uuid::Uuid::new_v4().to_string();
        let b = uuid::Uuid::new_v4().to_string();
        let read = |id: &str| approval_mode(headers(&fixture, id), State(state.clone()));
        assert_eq!(read(&a).await.unwrap().0.data["approval_mode"], "smart");
        assert_eq!(read(&b).await.unwrap().0.data["approval_mode"], "smart");
        let _ = save_approval_mode(
            headers(&fixture, &a),
            State(state.clone()),
            Json(ApprovalModeInput {
                approval_mode: ApprovalMode::Auto,
            }),
        )
        .await
        .unwrap();
        assert_eq!(read(&a).await.unwrap().0.data["approval_mode"], "auto");
        assert_eq!(read(&b).await.unwrap().0.data["approval_mode"], "smart");
        let c = uuid::Uuid::new_v4().to_string();
        assert_eq!(read(&c).await.unwrap().0.data["approval_mode"], "auto");
        let _ = save_approval_mode(
            headers(&fixture, &b),
            State(state.clone()),
            Json(ApprovalModeInput {
                approval_mode: ApprovalMode::Manual,
            }),
        )
        .await
        .unwrap();
        assert_eq!(read(&a).await.unwrap().0.data["approval_mode"], "auto");
        assert_eq!(read(&c).await.unwrap().0.data["approval_mode"], "auto");
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Shared default".into(),
                root_path: fixture.root.to_string_lossy().into(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                client_session_id: uuid::Uuid::new_v4().to_string(),
                project_id: project.id,
                title: None,
                agent: crate::models::AgentKind::Custom,
                brief_reply_mode: false,
                approval_mode: None,
                model: None,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .unwrap();
        assert_eq!(session.approval_mode, ApprovalMode::Manual);
        state
            .update_session_approval_mode(&session.id, ApprovalMode::Smart)
            .await
            .unwrap();
        let d = uuid::Uuid::new_v4().to_string();
        assert_eq!(read(&d).await.unwrap().0.data["approval_mode"], "smart");
        assert_eq!(read(&a).await.unwrap().0.data["approval_mode"], "auto");
        assert_eq!(read(&b).await.unwrap().0.data["approval_mode"], "manual");
        let restored = Arc::new(
            AppState::new_with_paths(
                fixture.root.join("settings.json"),
                fixture.root.join("runtime.json"),
                fixture.root.join("metadata.json"),
            )
            .await,
        );
        // Load persisted workspace memory through its normal conversation scope.
        let query = ScopeQuery {
            conversation_id: Some(a),
            ..Default::default()
        };
        assert_eq!(
            client_scope::settings(&*restored.workspace_agent.memory.lock().await, &query)
                .approval_mode,
            Some(ApprovalMode::Auto)
        );
        assert_eq!(
            restored.bridge_settings().await.default_approval_mode,
            ApprovalMode::Smart
        );
    }
}
