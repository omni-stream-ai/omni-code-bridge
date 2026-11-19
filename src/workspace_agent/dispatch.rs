//! Durable scheduling of project sessions, independent of assistant chat.
use super::*;
mod reporting;
use crate::models::{AgentKind, CreateSessionInput, InputMode, SendMessageInput, SessionStatus};
use crate::session_domain::{CreateTurnCommand, TurnStatus};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct DispatchSettings {
    pub(super) agent: AgentKind,
}
impl Default for DispatchSettings {
    fn default() -> Self {
        Self {
            agent: AgentKind::Pi,
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlannedTask {
    pub(super) project_id: String,
    title: String,
    instruction: String,
    #[serde(default)]
    depends_on: Vec<usize>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Queued,
    Running,
    AwaitingApproval,
    Paused,
    Cancelled,
    AwaitingReview,
    Blocked,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct DispatchTask {
    #[serde(default)]
    pub(super) conversation_id: Option<String>,
    #[serde(default)]
    pub(super) client_id: Option<String>,
    id: String,
    pub(super) request_id: String,
    project_id: String,
    title: String,
    instruction: String,
    session_id: String,
    turn_id: String,
    agent: AgentKind,
    #[serde(default)]
    approval_mode: crate::models::ApprovalMode,
    #[serde(default)]
    model_selection: Option<ModelSelection>,
    dependencies: Vec<String>,
    status: Status,
    summary: String,
    attachments: Vec<Attachment>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    attempt: u32,
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/workspace-agent/dispatch/settings",
            get(settings).put(save_settings),
        )
        .route("/workspace-agent/dispatch/{id}/control", post(control))
}
async fn settings(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let query = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let memory = state.workspace_agent.memory.lock().await;
    let configuration = client_scope::settings(&memory, &query);
    let agent = configuration.agent;
    let approval_mode = configuration.approval_mode.unwrap_or_default();
    Ok(Json(ApiResponse {
        data: json!({"agent":agent, "approval_mode": approval_mode}),
    }))
}
async fn save_settings(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<DispatchSettings>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let query = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let mut memory = state.workspace_agent.memory.lock().await;
    let mut next = memory.clone();
    if let Some(id) = &query.client_id {
        next.clients.get_mut(id).unwrap().dispatch_settings = input.clone();
    } else if let Some(id) = &query.conversation_id {
        next.conversations.get_mut(id).unwrap().configuration.agent = input.agent;
    } else {
        next.dispatch_settings = input.clone();
    }
    state.workspace_agent.save(&next).await.map_err(failure)?;
    *memory = next;
    Ok(Json(ApiResponse { data: json!(input) }))
}

pub(super) fn validate_plan(
    plan: &[PlannedTask],
    projects: &HashSet<String>,
    scope: Option<&str>,
) -> Result<(), String> {
    if plan.len() > 12 {
        return Err("Split a request into at most 12 tasks".into());
    }
    for (index, task) in plan.iter().enumerate() {
        if !projects.contains(&task.project_id) || scope.is_some_and(|id| id != task.project_id) {
            return Err("The scheduled project is outside the selected workspace scope".into());
        }
        if task.title.trim().is_empty()
            || task.title.chars().count() > 160
            || task.instruction.trim().is_empty()
            || task.instruction.chars().count() > 16000
        {
            return Err("Invalid scheduled task title or instruction".into());
        }
        if task
            .depends_on
            .iter()
            .any(|&dependency| dependency >= index)
        {
            return Err("Task dependencies must refer to earlier tasks".into());
        }
    }
    Ok(())
}
pub(super) fn preserve_references(
    plan: &mut [PlannedTask],
    references: &[references::ComposerReference],
) {
    for task in plan {
        for reference in references
            .iter()
            .filter(|reference| reference.project_id() == task.project_id)
        {
            match reference {
                references::ComposerReference::Skill { name, .. } => {
                    let marker = format!("${name}");
                    if task.instruction.split_whitespace().next() != Some(marker.as_str()) {
                        task.instruction = format!("{marker} {}", task.instruction);
                    }
                }
                references::ComposerReference::File { path, .. } => {
                    if !task.instruction.contains(path) {
                        task.instruction
                            .push_str(&format!("\nReferenced project file: @{path}"));
                    }
                }
            }
        }
    }
}

pub(super) fn append_plan(
    memory: &mut Memory,
    plan: Vec<PlannedTask>,
    request_id: &str,
    attachments: &[Attachment],
) {
    let ids = plan
        .iter()
        .map(|_| uuid::Uuid::new_v4().to_string())
        .collect::<Vec<_>>();
    for (index, task) in plan.into_iter().enumerate() {
        let now = Utc::now();
        memory.dispatch_tasks.push(DispatchTask {
            conversation_id: None,
            client_id: None,
            id: ids[index].clone(),
            request_id: request_id.into(),
            project_id: task.project_id,
            title: task.title,
            instruction: task.instruction,
            session_id: uuid::Uuid::new_v4().to_string(),
            turn_id: uuid::Uuid::new_v4().to_string(),
            agent: memory.dispatch_settings.agent,
            approval_mode: Default::default(),
            model_selection: memory.model_selection.clone(),
            dependencies: task.depends_on.iter().map(|&i| ids[i].clone()).collect(),
            status: Status::Queued,
            summary: String::new(),
            attachments: attachments.to_vec(),
            created_at: now,
            updated_at: now,
            attempt: 0,
        });
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ParentRequest {
    #[serde(default)]
    pub(super) conversation_id: Option<String>,
    #[serde(default)]
    pub(super) client_id: Option<String>,
    pub(super) id: String,
    pub(super) scope_key: String,
    message: String,
    model_selection: Option<ModelSelection>,
    #[serde(default)]
    reported: HashMap<String, String>,
    #[serde(default)]
    retry_at: Option<DateTime<Utc>>,
    #[serde(default)]
    failures: u32,
    #[serde(default)]
    error: Option<String>,
}
pub(super) fn link_parent(
    memory: &mut Memory,
    id: &str,
    query: &ScopeQuery,
    message: &str,
    model_selection: Option<ModelSelection>,
    agent: AgentKind,
    approval_mode: crate::models::ApprovalMode,
) {
    for task in memory
        .dispatch_tasks
        .iter_mut()
        .filter(|task| task.request_id == id)
    {
        task.model_selection = model_selection.clone();
        task.agent = agent;
        task.approval_mode = approval_mode;
        task.client_id = query.client_id.clone();
        task.conversation_id = query.conversation_id.clone();
    }
    memory.dispatch_requests.push(ParentRequest {
        conversation_id: query.conversation_id.clone(),
        client_id: query.client_id.clone(),
        id: id.into(),
        scope_key: chat_scope_key(query),
        message: message.into(),
        model_selection,
        reported: HashMap::new(),
        retry_at: None,
        failures: 0,
        error: None,
    });
}
pub(super) fn report_snapshot(memory: &Memory, query: &ScopeQuery) -> Value {
    json!(
        memory
            .dispatch_requests
            .iter()
            .filter(|request| query.conversation_id.is_some() || request.client_id == query.client_id)
            .filter(|request| query
                .project_id
                .as_ref()
                .is_none_or(|id| &request.scope_key == id
                    || memory
                        .dispatch_tasks
                        .iter()
                        .any(|task| task.request_id == request.id && &task.project_id == id)))
            .map(|request| json!({"request_id":request.id, "error":request.error, "pending":!reporting::pending(request, &memory.dispatch_tasks).is_empty()}))
            .collect::<Vec<_>>()
    )
}

pub(super) fn snapshot(memory: &Memory, query: &ScopeQuery) -> Value {
    json!(
        memory
            .dispatch_tasks
            .iter()
            .filter(|task| query.conversation_id.is_some() || task.client_id == query.client_id)
            .filter(|task| query
                .project_id
                .as_ref()
                .is_none_or(|id| id == &task.project_id)
                && query
                    .session_id
                    .as_ref()
                    .is_none_or(|id| id == &task.session_id))
            .collect::<Vec<_>>()
    )
}

// One scheduler per Bridge; no strong state reference while sleeping.
pub(crate) fn start(state: &Arc<AppState>) {
    if state
        .workspace_agent
        .dispatch_started
        .swap(true, Ordering::AcqRel)
    {
        return;
    }
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else {
                break;
            };
            if let Err(error) = tick(&state).await {
                eprintln!("[workspace dispatch] {error}");
            }
            reporting::enqueue(&state).await;
            drop(state);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}
async fn persist_task(state: &AppState, mut task: DispatchTask) -> Result<(), String> {
    let mut memory = state.workspace_agent.memory.lock().await;
    let mut next = memory.clone();
    let target = next
        .dispatch_tasks
        .iter_mut()
        .find(|entry| entry.id == task.id)
        .ok_or("Scheduled task not found")?;
    // The first client can claim legacy work while the scheduler is awaiting I/O.
    task.client_id = target.client_id.clone();
    task.conversation_id = target.conversation_id.clone();
    *target = task;
    state.workspace_agent.save(&next).await?;
    *memory = next;
    Ok(())
}
async fn launch(state: &Arc<AppState>, task: &DispatchTask) -> Result<(), String> {
    let session = state
        .create_session(CreateSessionInput {
            client_session_id: task.session_id.clone(),
            project_id: task.project_id.clone(),
            title: Some(task.title.clone()),
            agent: task.agent,
            brief_reply_mode: false,
            approval_mode: Some(task.approval_mode),
            provider_id: Some(
                task.model_selection
                    .as_ref()
                    .map(|selection| selection.provider_id.clone())
                    .unwrap_or_else(|| "AUTO".into()),
            ),
            reasoning_effort: None,
            model: task
                .model_selection
                .as_ref()
                .map(|selection| selection.model.clone()),
        })
        .await?;
    if task.model_selection.is_some()
        && state
            .resolve_provider_config(&session, &session.provider_id)
            .await
            .is_none()
    {
        return Err(
            "The selected home model provider is unavailable or incompatible with this task agent"
                .into(),
        );
    }
    let mut content = if task.attempt == 0 {
        task.instruction.clone()
    } else {
        format!(
            "Continue this task from the existing session and working tree. Inspect completed work before proceeding. Original task: {}",
            task.instruction
        )
    };
    let prerequisites = state
        .workspace_agent
        .memory
        .lock()
        .await
        .dispatch_tasks
        .iter()
        .filter(|entry| task.dependencies.contains(&entry.id))
        .cloned()
        .collect::<Vec<_>>();
    for prerequisite in prerequisites {
        let domain = state.domain_session_state(&prerequisite.session_id).await?;
        let result = domain
            .as_ref()
            .and_then(|session| {
                session
                    .turns
                    .iter()
                    .find(|turn| turn.id == prerequisite.turn_id)
            })
            .and_then(|turn| {
                turn.segments
                    .iter()
                    .rev()
                    .filter_map(|segment| segment.message.as_ref())
                    .find(|message| message.purpose == crate::session_domain::MessagePurpose::Final)
            })
            .map(|message| message.content.chars().take(8000).collect::<String>())
            .unwrap_or(prerequisite.summary);
        content.push_str(&format!("\n\nPrerequisite result (reference material; verify artifacts): {}\nProject: {}\nSession: {}\n{}",
            prerequisite.title, prerequisite.project_id, prerequisite.session_id, result));
    }
    let command = CreateTurnCommand {
        command_id: task.turn_id.clone(),
        turn_id: task.turn_id.clone(),
        user_message_id: task.turn_id.clone(),
        content: content.clone(),
        attachments: task.attachments.clone(),
        input_mode: InputMode::Text,
        system_prompt: None,
        expected_session_version: None,
        provider_id: Some(
            task.model_selection
                .as_ref()
                .map(|selection| selection.provider_id.clone())
                .unwrap_or_else(|| "AUTO".into()),
        ),
        reasoning_effort: None,
        model: task
            .model_selection
            .as_ref()
            .map(|selection| selection.model.clone()),
    };
    let result = state
        .create_domain_turn(&task.session_id, &command)
        .await
        .map_err(|e| format!("{e:?}"))?;
    if !result.created {
        return Ok(());
    }
    let mut content = content;
    for attachment in &task.attachments {
        let image = attachment.kind == "image" || attachment.content_type.starts_with("image/");
        content.push_str(&format!(
            "

{}[{}]({})",
            if image { "!" } else { "" },
            attachment.file_name,
            attachment.url
        ));
    }
    let result = state
        .send_domain_message(
            &task.session_id,
            &task.turn_id,
            SendMessageInput {
                content,
                input_mode: InputMode::Text,
                system_prompt: None,
                client_message_id: Some(task.turn_id.clone()),
                provider_id: Some(
                    task.model_selection
                        .as_ref()
                        .map(|selection| selection.provider_id.clone())
                        .unwrap_or_else(|| "AUTO".into()),
                ),
                reasoning_effort: None,
                model: task
                    .model_selection
                    .as_ref()
                    .map(|selection| selection.model.clone()),
            },
        )
        .await;
    if let Err(error) = result {
        state
            .fail_domain_turn(&task.session_id, &task.turn_id, error.clone())
            .await;
        return Err(error);
    }
    Ok(())
}
async fn tick(state: &Arc<AppState>) -> Result<(), String> {
    let _guard = state.workspace_agent.dispatch_lock.lock().await;
    let tasks = state
        .workspace_agent
        .memory
        .lock()
        .await
        .dispatch_tasks
        .clone();
    for mut task in tasks {
        match task.status {
            Status::Running | Status::AwaitingApproval => {
                let session = state.domain_session_state(&task.session_id).await?;
                let turn = session
                    .as_ref()
                    .and_then(|s| s.turns.iter().find(|t| t.id == task.turn_id));
                let new_status = match turn.map(|t| t.status) {
                    Some(TurnStatus::Completed) => Status::AwaitingReview,
                    Some(TurnStatus::Failed) | None => Status::Blocked,
                    Some(TurnStatus::Cancelled) => Status::Paused,
                    Some(TurnStatus::AwaitingApproval) => Status::AwaitingApproval,
                    _ => Status::Running,
                };
                let summary = turn
                    .and_then(|turn| {
                        turn.segments.iter().rev().find_map(|segment| {
                            segment
                                .message
                                .as_ref()
                                .filter(|m| {
                                    m.purpose != crate::session_domain::MessagePurpose::User
                                        && !m.content.trim().is_empty()
                                })
                                .map(|m| m.content.clone())
                                .or_else(|| {
                                    segment
                                        .activities
                                        .last()
                                        .map(|activity| activity.title.clone())
                                })
                        })
                    })
                    .unwrap_or_default()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .chars()
                    .take(160)
                    .collect::<String>();
                if task.status != new_status || task.summary != summary {
                    task.status = new_status;
                    task.summary = summary;
                    task.updated_at = Utc::now();
                    persist_task(state, task).await?;
                }
            }
            Status::Queued => {
                let memory = state.workspace_agent.memory.lock().await;
                let dependencies_ready = task.dependencies.iter().all(|id| {
                    memory
                        .dispatch_tasks
                        .iter()
                        .any(|t| &t.id == id && t.status == Status::AwaitingReview)
                });
                // Serialize writes to a project; independent projects can run concurrently.
                let project_busy = memory.dispatch_tasks.iter().any(|other| {
                    other.id != task.id
                        && other.project_id == task.project_id
                        && matches!(other.status, Status::Running | Status::AwaitingApproval)
                });
                drop(memory);
                if !dependencies_ready || project_busy {
                    continue;
                }
                task.status = Status::Running;
                task.updated_at = Utc::now();
                persist_task(state, task.clone()).await?;
                if let Err(error) = launch(state, &task).await {
                    task.status = Status::Blocked;
                    task.summary = error.chars().take(500).collect();
                    persist_task(state, task).await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    action: String,
}
async fn control(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Control>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let query = client_scope(&headers, &state, ScopeQuery::default()).await?;
    let _guard = state.workspace_agent.dispatch_lock.lock().await;
    let mut task = state
        .workspace_agent
        .memory
        .lock()
        .await
        .dispatch_tasks
        .iter()
        .find(|t| t.id == id && (query.conversation_id.is_some() || t.client_id == query.client_id))
        .cloned()
        .ok_or_else(|| failure("Scheduled task not found".into()))?;
    match input.action.as_str() {
        "pause" | "cancel" => {
            let target = if input.action == "pause" {
                Status::Paused
            } else {
                Status::Cancelled
            };
            if task.status == target {
                return Ok(Json(ApiResponse { data: json!(task) }));
            }
            if matches!(task.status, Status::Cancelled | Status::AwaitingReview) {
                return Err((
                    StatusCode::CONFLICT,
                    "This task has already stopped".to_string(),
                )
                    .into());
            }
            if matches!(task.status, Status::Running | Status::AwaitingApproval) {
                state
                    .cancel_turn_if_current(&task.session_id, Some(&task.turn_id))
                    .await
                    .map_err(failure)?;
            }
            task.status = target;
        }
        "resume" => {
            if task.status == Status::Queued {
                return Ok(Json(ApiResponse { data: json!(task) }));
            }
            if !matches!(task.status, Status::Paused | Status::Blocked) {
                return Err((
                    StatusCode::CONFLICT,
                    "Only paused or blocked tasks can resume".to_string(),
                )
                    .into());
            }
            if state
                .list_sessions()
                .await
                .into_iter()
                .find(|s| s.id == task.session_id)
                .is_some_and(|s| {
                    matches!(
                        s.status,
                        SessionStatus::Running | SessionStatus::AwaitingApproval
                    )
                })
            {
                return Err((
                    StatusCode::CONFLICT,
                    "The session is still running".to_string(),
                )
                    .into());
            }
            task.attempt += 1;
            task.turn_id = uuid::Uuid::new_v4().to_string();
            task.status = Status::Queued;
            task.summary.clear();
        }
        _ => return Err(failure("Use pause, resume or cancel".into())),
    }
    task.updated_at = Utc::now();
    persist_task(&state, task.clone()).await.map_err(failure)?;
    start(&state);
    Ok(Json(ApiResponse { data: json!(task) }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{AgentProvider, ProviderRegistry};
    use crate::models::ClientAuthRequestInput;
    use crate::models::{CreateProjectInput, ReasoningEffort, ResolvedProviderConfig};
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct ReturnModel {
        inputs: Mutex<Vec<Value>>,
        fail_returns: AtomicBool,
        hold_returns: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        hold_chats: AtomicBool,
        chat_entered: tokio::sync::Notify,
        chat_release: tokio::sync::Notify,
    }
    async fn return_model(
        State(mock): State<Arc<ReturnModel>>,
        Json(request): Json<Value>,
    ) -> impl axum::response::IntoResponse {
        let input: Value = serde_json::from_str(
            request["messages"].as_array().unwrap().last().unwrap()["content"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        mock.inputs
            .lock()
            .await
            .push(json!({"input":input,"model":request["model"]}));
        if input["latest_user_message"] == "Hold this request"
            && mock.hold_chats.load(Ordering::SeqCst)
        {
            mock.chat_entered.notify_one();
            mock.chat_release.notified().await;
        }
        let output = if let Some(original) = input["original_request"].as_str() {
            mock.entered.notify_one();
            if mock.hold_returns.load(Ordering::SeqCst) {
                mock.release.notified().await;
            }
            if mock.fail_returns.load(Ordering::SeqCst) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [("content-type", "application/json")],
                    "{\"error\":{\"message\":\"Temporary model failure\"}}".into(),
                );
            }
            let results = input["subtasks"].as_array().unwrap();
            assert!(results.iter().any(|task| task["result_truncated"] == true));
            assert!(
                results
                    .iter()
                    .map(|task| task["result"].as_str().unwrap().chars().count())
                    .sum::<usize>()
                    < 101_000
            );
            assert!(
                results.iter().any(|task| task["result"]
                    .as_str()
                    .unwrap()
                    .contains("Actual answer: export uses UTF-8 CSV with three columns.")),
                "the main task needs the actual answer beyond the brief card preview"
            );
            let complete = results
                .iter()
                .all(|task| task["status"] == "awaiting_review");
            json!({"reply":format!("Answer to {original}: export uses UTF-8 CSV with three columns. {}", if complete { "All subtasks returned." } else { "Other subtasks are pending." })})
        } else if input["latest_user_message"] == "Change scope" {
            json!({"reply":"Proposed.","change":{"analysis_scope":{"include_other_sessions":true,"recent_days":7,"max_sessions":15}}})
        } else if input["latest_user_message"] == "Remember my preference" {
            json!({"reply":"Remembered.","memory_updates":[{"content":"Prefer concise Chinese replies."}]})
        } else if input["latest_user_message"] == "progress?" {
            json!({"reply":"The earlier work continues."})
        } else {
            let project = input["available_projects"][0]["id"].as_str().unwrap();
            let tasks = if input["latest_user_message"]
                .as_str()
                .unwrap()
                .starts_with("Split")
            {
                vec![plan(project, vec![]), plan(project, vec![0])]
            } else {
                vec![plan(project, vec![])]
            };
            json!({"reply":"Scheduled.","dispatch":tasks})
        };
        let chunk = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":{"role":"assistant","content":output.to_string()},"finish_reason":null}]});
        let finish = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
        (
            StatusCode::OK,
            [("content-type", "text/event-stream")],
            format!("data: {chunk}\n\ndata: {finish}\n\ndata: [DONE]\n\n"),
        )
    }
    async fn configure_return_model(
        state: &Arc<AppState>,
    ) -> (Arc<ReturnModel>, tokio::task::JoinHandle<()>) {
        use crate::{bridge_settings::BridgeSettingsInput, models::ModelProviderConfig};
        let mock = Arc::new(ReturnModel::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/chat/completions", post(return_model))
            .with_state(mock.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        state
            .update_bridge_settings(BridgeSettingsInput {
                model_providers: Some(vec![
                    ModelProviderConfig {
                        id: "automatic".into(),
                        name: "Automatic".into(),
                        base_url: url.clone(),
                        api_key: "test".into(),
                        model: Some("automatic-model".into()),
                        format: ApiFormat::OpenAiCompatible,
                        enabled: true,
                        priority: 0,
                    },
                    ModelProviderConfig {
                        id: "home-provider".into(),
                        name: "Home".into(),
                        base_url: url,
                        api_key: "test".into(),
                        model: Some("provider-default".into()),
                        format: ApiFormat::OpenAiCompatible,
                        enabled: true,
                        priority: 10,
                    },
                ]),
                ai_approval: None,
                acp_servers: None,
            })
            .await
            .unwrap();
        state.workspace_agent.memory.lock().await.model_selection = Some(ModelSelection {
            provider_id: "home-provider".into(),
            model: "home-model".into(),
        });
        (mock, server)
    }
    async fn submit_request(
        state: &Arc<AppState>,
        headers: &HeaderMap,
        project: Option<&str>,
        message: &str,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(ScopeQuery {
                conversation_id: None,
                client_id: None,
                project_id: project.map(str::to_owned),
                session_id: None,
            }),
            Json(ChatInput {
                references: Vec::new(),
                configuration: None,
                request_id: Some(id.clone()),
                message: message.into(),
                attachments: vec![],
            }),
        )
        .await
        .unwrap();
        id
    }
    async fn reconcile_status(state: &Arc<AppState>, id: &str, status: Status) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tick(state).await.unwrap();
                if state
                    .workspace_agent
                    .memory
                    .lock()
                    .await
                    .dispatch_tasks
                    .iter()
                    .any(|task| task.id == id && task.status == status)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn wait_return(state: &Arc<AppState>, id: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let memory = state.workspace_agent.memory.lock().await;
                let returned = memory
                    .chats
                    .values()
                    .flatten()
                    .filter(|entry| {
                        entry.dispatch_request_id.as_deref() == Some(id)
                            && entry.id.starts_with("dispatch-return-")
                    })
                    .count();
                drop(memory);
                if returned == count {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn child_inherits_frozen_home_model_and_returns_to_parent_without_blocking_chat() {
        let (state, worker, headers, project, root) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let id = submit_request(&state, &headers, Some(&project), "Explain export").await;
        state
            .workspace_agent
            .memory
            .lock()
            .await
            .model_selection
            .as_mut()
            .unwrap()
            .model = "later-model".into();
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let task = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
        assert_eq!(
            worker.selections.lock().await[0],
            (
                Some("home-provider".into()),
                Some("home-model".into()),
                Some("home-model".into())
            )
        );
        let session = state
            .list_sessions()
            .await
            .into_iter()
            .find(|session| session.id == task.session_id)
            .unwrap();
        assert_eq!(session.model.as_deref(), Some("home-model"));
        assert_eq!(session.provider_id.as_deref(), Some("home-provider"));
        let domain = state
            .domain_session_state(&task.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(domain.session.config.model.as_deref(), Some("home-model"));
        worker.finish.notify_one();
        reconcile_status(&state, &task.id, Status::AwaitingReview).await;
        mock.hold_returns.store(true, Ordering::SeqCst);
        reporting::enqueue(&state).await;
        reporting::enqueue(&state).await;
        tokio::time::timeout(Duration::from_secs(3), mock.entered.notified())
            .await
            .unwrap();
        let second = tokio::time::timeout(
            Duration::from_secs(2),
            submit_request(&state, &headers, Some(&project), "Another request"),
        )
        .await
        .unwrap();
        let queued = state
            .workspace_agent
            .memory
            .lock()
            .await
            .dispatch_tasks
            .iter()
            .find(|task| task.request_id == second)
            .unwrap()
            .clone();
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            control(
                headers.clone(),
                State(state.clone()),
                Path(queued.id),
                Json(Control {
                    action: "pause".into(),
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            chat(
                headers,
                State(state.clone()),
                Query(ScopeQuery {
                    conversation_id: None,
                    client_id: None,
                    project_id: Some(project.clone()),
                    session_id: None,
                }),
                Json(ChatInput {
                    references: Vec::new(),
                    configuration: None,
                    request_id: None,
                    message: "progress?".into(),
                    attachments: vec![],
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        mock.hold_returns.store(false, Ordering::SeqCst);
        mock.release.notify_one();
        wait_return(&state, &id, 1).await;
        {
            let memory = state.workspace_agent.memory.lock().await;
            let reply = memory.chats[&project]
                .iter()
                .find(|entry| entry.id.starts_with("dispatch-return-"))
                .unwrap();
            assert!(
                reply
                    .content
                    .contains("Answer to Explain export: export uses UTF-8 CSV")
            );
            assert_eq!(reply.dispatch_request_id.as_deref(), Some(id.as_str()));
            assert!(memory.chats.get("").is_none());
            assert_eq!(
                memory
                    .dispatch_requests
                    .iter()
                    .find(|request| request.id == id)
                    .unwrap()
                    .reported
                    .len(),
                1
            );
        }
        let inputs = mock.inputs.lock().await;
        let returned = inputs
            .iter()
            .filter(|entry| entry["input"]["original_request"].is_string())
            .collect::<Vec<_>>();
        assert_eq!(returned.len(), 1);
        assert_eq!(returned[0]["model"], "home-model");
        drop(inputs);
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        *state.workspace_agent.memory.lock().await = restored.memory.lock().await.clone();
        reporting::report(&state, &id).await.unwrap();
        assert_eq!(
            mock.inputs
                .lock()
                .await
                .iter()
                .filter(|entry| entry["input"]["original_request"].is_string())
                .count(),
            1,
            "restoring memory must not repeat the main reply"
        );
        server.abort();
    }

    #[tokio::test]
    async fn split_results_resume_main_task_and_failed_returns_retry_after_restart() {
        let (state, worker, headers, _, root) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let id = submit_request(&state, &headers, None, "Split export delivery").await;
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let tasks = state
            .workspace_agent
            .memory
            .lock()
            .await
            .dispatch_tasks
            .clone();
        worker.finish.notify_one();
        reconcile_status(&state, &tasks[0].id, Status::AwaitingReview).await;
        wait_calls(&worker, 2).await;
        reporting::report(&state, &id).await.unwrap();
        {
            let memory = state.workspace_agent.memory.lock().await;
            assert!(
                memory.chats[""]
                    .last()
                    .unwrap()
                    .content
                    .contains("Other subtasks are pending")
            );
        }
        worker.finish.notify_one();
        reconcile_status(&state, &tasks[1].id, Status::AwaitingReview).await;
        mock.fail_returns.store(true, Ordering::SeqCst);
        reporting::enqueue(&state).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state.workspace_agent.memory.lock().await.dispatch_requests[0]
                    .error
                    .is_some()
                    && state
                        .workspace_agent
                        .dispatch_reporting
                        .lock()
                        .await
                        .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        let mut saved = restored.memory.lock().await.clone();
        assert!(saved.dispatch_requests[0].error.is_some());
        assert_eq!(
            saved.dispatch_requests[0].reported.len(),
            1,
            "a failed main answer must not acknowledge the second result"
        );
        saved.dispatch_requests[0].retry_at = None;
        *state.workspace_agent.memory.lock().await = saved;
        mock.fail_returns.store(false, Ordering::SeqCst);
        reporting::enqueue(&state).await;
        wait_return(&state, &id, 2).await;
        let memory = state.workspace_agent.memory.lock().await;
        assert!(memory.dispatch_requests[0].error.is_none());
        assert_eq!(memory.dispatch_requests[0].reported.len(), 2);
        assert!(
            memory.chats[""]
                .last()
                .unwrap()
                .content
                .contains("All subtasks returned")
        );
        server.abort();
    }

    #[tokio::test]
    async fn chat_dispatch_is_idempotent_and_new_requests_do_not_interrupt_workers() {
        use crate::{bridge_settings::BridgeSettingsInput, models::ModelProviderConfig};
        async fn model(Json(request): Json<Value>) -> impl axum::response::IntoResponse {
            let input: Value = serde_json::from_str(
                request["messages"].as_array().unwrap().last().unwrap()["content"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            let tasks = if input["latest_user_message"] == "progress?" {
                vec![]
            } else if input["latest_user_message"] == "Split change" {
                let project = input["available_projects"][0]["id"].as_str().unwrap();
                vec![plan(project, vec![]), plan(project, vec![0])]
            } else {
                vec![plan(
                    input["available_projects"][0]["id"].as_str().unwrap(),
                    vec![],
                )]
            };
            let content = json!({"reply":"Tasks are tracked here", "dispatch": tasks}).to_string();
            let chunk = json!({"id":"mock","object":"chat.completion.chunk","model":"mock-model","choices":[{"index":0,"delta":{"role":"assistant","content":content},"finish_reason":null}]});
            let finish = json!({"id":"mock","object":"chat.completion.chunk","model":"mock-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
            (
                [("content-type", "text/event-stream")],
                format!("data: {chunk}\n\ndata: {finish}\n\ndata: [DONE]\n\n"),
            )
        }
        let (state, worker, headers, _, root) = fixture().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new().route("/chat/completions", post(model)),
            )
            .into_future(),
        );
        state
            .update_bridge_settings(BridgeSettingsInput {
                model_providers: Some(vec![ModelProviderConfig {
                    id: "mock".into(),
                    name: "Mock".into(),
                    base_url,
                    api_key: "test".into(),
                    model: Some("mock-model".into()),
                    format: ApiFormat::OpenAiCompatible,
                    enabled: true,
                    priority: 0,
                }]),
                ai_approval: None,
                acp_servers: None,
            })
            .await
            .unwrap();
        let request_id = uuid::Uuid::new_v4().to_string();
        let input = || ChatInput {
            references: Vec::new(),
            configuration: None,
            request_id: Some(request_id.clone()),
            message: "Implement it".into(),
            attachments: vec![],
        };
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
            Json(input()),
        )
        .await
        .unwrap();
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
            Json(input()),
        )
        .await
        .unwrap();
        assert_eq!(
            state
                .workspace_agent
                .memory
                .lock()
                .await
                .dispatch_tasks
                .len(),
            1
        );
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
            Json(ChatInput {
                references: Vec::new(),
                configuration: None,
                request_id: None,
                message: "progress?".into(),
                attachments: vec![],
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state
                .workspace_agent
                .memory
                .lock()
                .await
                .dispatch_tasks
                .len(),
            1
        );
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
            Json(ChatInput {
                references: Vec::new(),
                configuration: None,
                request_id: Some(uuid::Uuid::new_v4().to_string()),
                message: "Split change".into(),
                attachments: vec![],
            }),
        )
        .await
        .unwrap();
        let tasks = state
            .workspace_agent
            .memory
            .lock()
            .await
            .dispatch_tasks
            .clone();
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].status, Status::Running);
        assert_eq!(tasks[1].status, Status::Queued);
        assert_eq!(
            state
                .domain_session_state(&tasks[0].session_id)
                .await
                .unwrap()
                .unwrap()
                .turns[0]
                .status,
            TurnStatus::Running
        );
        let mut conflicting = input();
        conflicting.message = "different".into();
        assert!(
            chat(
                headers,
                State(state.clone()),
                Query(ScopeQuery::default()),
                Json(conflicting)
            )
            .await
            .is_err()
        );
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        assert!(
            restored
                .memory
                .lock()
                .await
                .chat_receipts
                .iter()
                .any(|r| r.id == request_id)
        );
        state.cancel_turn(&tasks[0].session_id).await.unwrap();
        server.abort();
    }

    #[derive(Default)]
    struct Worker {
        calls: AtomicUsize,
        finish: tokio::sync::Notify,
        selections: Mutex<Vec<(Option<String>, Option<String>, Option<String>)>>,
    }
    #[async_trait::async_trait]
    impl AgentProvider for Worker {
        async fn run_session(
            &self,
            state: Arc<AppState>,
            session: SessionSummary,
            _: ChatMessage,
            _: Option<String>,
            reply: ChatMessage,
            provider: Option<ResolvedProviderConfig>,
            _: Option<ReasoningEffort>,
        ) -> anyhow::Result<()> {
            self.selections.lock().await.push((
                session.provider_id.clone(),
                session.model.clone(),
                provider.and_then(|p| p.model),
            ));
            self.calls.fetch_add(1, Ordering::SeqCst);
            state
                .emit_assistant_message_snapshot(
                    &session.id,
                    &reply.id,
                    "Implementing the requested change",
                )
                .await;
            self.finish.notified().await;
            state
                .emit_assistant_message_snapshot(
                    &session.id,
                    &reply.id,
                    &format!("Implemented and tested; ready for review. {} Actual answer: export uses UTF-8 CSV with three columns.", "Detailed verification. ".repeat(1500)),
                )
                .await;
            state
                .finish_assistant_message(&session.id, &reply.id)
                .await
                .map_err(anyhow::Error::msg)?;
            Ok(())
        }
    }
    async fn fixture() -> (Arc<AppState>, Arc<Worker>, HeaderMap, String, PathBuf) {
        let root = std::env::temp_dir().join(format!("dispatch-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let worker = Arc::new(Worker::default());
        let mut state = AppState::new_with_paths_and_providers(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
            ProviderRegistry::from_map(HashMap::from([
                (AgentKind::Pi, worker.clone() as Arc<dyn AgentProvider>),
                (AgentKind::Codex, worker.clone() as Arc<dyn AgentProvider>),
            ])),
        )
        .await;
        state
            .set_client_auth_path_for_test(root.join("auth.json"))
            .await;
        let state = Arc::new(state);
        // Tests explicitly advance the scheduler, so no background ticks race assertions.
        state
            .workspace_agent
            .dispatch_started
            .store(true, Ordering::Release);
        let project = state
            .create_project(CreateProjectInput {
                name: "Test project".into(),
                root_path: root.to_string_lossy().into(),
            })
            .await;
        let auth = state
            .request_client_auth(ClientAuthRequestInput {
                client_id: "dispatch-test".into(),
                device_name: None,
            })
            .await
            .unwrap();
        let token = state
            .approve_client_auth_for_test(&auth.request_id)
            .await
            .unwrap()
            .token
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-omni-code-client-id", "dispatch-test".parse().unwrap());
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        (state, worker, headers, project.id, root)
    }
    fn plan(project: &str, dependencies: Vec<usize>) -> PlannedTask {
        PlannedTask {
            project_id: project.into(),
            title: "Implement change".into(),
            instruction: "Implement and test the change".into(),
            depends_on: dependencies,
        }
    }
    async fn wait_calls(worker: &Worker, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while worker.calls.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn plans_require_real_scoped_projects_and_acyclic_dependencies() {
        let projects = HashSet::from(["p".into(), "q".into()]);
        assert!(
            validate_plan(
                &[plan("p", vec![]), plan("p", vec![0])],
                &projects,
                Some("p")
            )
            .is_ok()
        );
        assert!(validate_plan(&[plan("missing", vec![])], &projects, None).is_err());
        assert!(validate_plan(&[plan("q", vec![])], &projects, Some("p")).is_err());
        assert!(validate_plan(&[plan("p", vec![0])], &projects, None).is_err());
        assert!(validate_plan(&vec![plan("p", vec![]); 13], &projects, None).is_err());
    }

    #[tokio::test]
    async fn scheduling_runs_real_turns_pause_resume_and_dependencies() {
        let (state, worker, headers, project, root) = fixture().await;
        {
            let mut memory = state.workspace_agent.memory.lock().await;
            append_plan(
                &mut memory,
                vec![plan(&project, vec![]), plan(&project, vec![0])],
                "request",
                &[],
            );
        }
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let tasks = state
            .workspace_agent
            .memory
            .lock()
            .await
            .dispatch_tasks
            .clone();
        let first = tasks[0].clone();
        assert_eq!(tasks[0].status, Status::Running);
        assert_eq!(tasks[1].status, Status::Queued);
        assert_eq!(state.list_sessions().await.len(), 1);
        assert!(
            control(
                HeaderMap::new(),
                State(state.clone()),
                Path(first.id.clone()),
                Json(Control {
                    action: "pause".into()
                })
            )
            .await
            .is_err()
        );
        let _ = control(
            headers.clone(),
            State(state.clone()),
            Path(first.id.clone()),
            Json(Control {
                action: "pause".into(),
            }),
        )
        .await
        .unwrap();
        let domain = state
            .domain_session_state(&first.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(domain.turns[0].status, TurnStatus::Cancelled);
        assert_eq!(
            state.workspace_agent.memory.lock().await.dispatch_tasks[0].status,
            Status::Paused
        );
        tick(&state).await.unwrap();
        assert_eq!(worker.calls.load(Ordering::SeqCst), 1);
        let _ = control(
            headers.clone(),
            State(state.clone()),
            Path(first.id.clone()),
            Json(Control {
                action: "resume".into(),
            }),
        )
        .await
        .unwrap();
        tick(&state).await.unwrap();
        wait_calls(&worker, 2).await;
        let resumed = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
        assert_eq!(resumed.session_id, first.session_id);
        assert_ne!(resumed.turn_id, first.turn_id);
        assert_eq!(state.list_sessions().await.len(), 1);
        worker.finish.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tick(&state).await.unwrap();
                if state.workspace_agent.memory.lock().await.dispatch_tasks[0].status
                    == Status::AwaitingReview
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tick(&state).await.unwrap();
        wait_calls(&worker, 3).await;
        assert_eq!(state.list_sessions().await.len(), 2);
        let second = state.workspace_agent.memory.lock().await.dispatch_tasks[1].clone();
        assert!(
            state
                .domain_session_state(&second.session_id)
                .await
                .unwrap()
                .unwrap()
                .turns[0]
                .user_message
                .content
                .contains("Implemented and tested")
        );
        let _ = control(
            headers.clone(),
            State(state.clone()),
            Path(second.id.clone()),
            Json(Control {
                action: "cancel".into(),
            }),
        )
        .await
        .unwrap();
        // Cancellation is idempotent and cannot start a new turn.
        let _ = control(
            headers,
            State(state.clone()),
            Path(second.id.clone()),
            Json(Control {
                action: "cancel".into(),
            }),
        )
        .await
        .unwrap();
        tick(&state).await.unwrap();
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        let memory = restored.memory.lock().await;
        assert_eq!(memory.dispatch_tasks[0].status, Status::AwaitingReview);
        assert_eq!(memory.dispatch_tasks[1].status, Status::Cancelled);
        assert!(
            memory.dispatch_tasks[0]
                .summary
                .contains("ready for review")
        );
    }

    #[tokio::test]
    async fn background_scheduler_launches_without_an_open_chat_and_recovers_interrupted_turns() {
        let (state, worker, _, project, root) = fixture().await;
        {
            let mut memory = state.workspace_agent.memory.lock().await;
            append_plan(&mut memory, vec![plan(&project, vec![])], "background", &[]);
            state.workspace_agent.save(&memory).await.unwrap();
        }
        state
            .workspace_agent
            .dispatch_started
            .store(false, Ordering::Release);
        start(&state);
        start(&state);
        wait_calls(&worker, 1).await;
        assert_eq!(worker.calls.load(Ordering::SeqCst), 1);
        let task = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
        state.cancel_turn(&task.session_id).await.unwrap();
        // Reopen the durable store before its next reconciliation, as on restart.
        drop(state);
        let recovered = Arc::new(
            AppState::new_with_paths_and_providers(
                root.join("settings.json"),
                root.join("runtime.json"),
                root.join("metadata.json"),
                ProviderRegistry::from_map(HashMap::from([(
                    AgentKind::Pi,
                    worker.clone() as Arc<dyn AgentProvider>,
                )])),
            )
            .await,
        );
        tick(&recovered).await.unwrap();
        let memory = recovered.workspace_agent.memory.lock().await;
        assert_eq!(memory.dispatch_tasks[0].status, Status::Paused);
        assert_eq!(memory.dispatch_tasks[0].session_id, task.session_id);
        assert_eq!(
            worker.calls.load(Ordering::SeqCst),
            1,
            "recovery must not replay the original turn"
        );
    }

    async fn scoped_headers(state: &Arc<AppState>, id: &str) -> HeaderMap {
        let auth = state
            .request_client_auth(ClientAuthRequestInput {
                client_id: id.into(),
                device_name: None,
            })
            .await
            .unwrap();
        let token = state
            .approve_client_auth_for_test(&auth.request_id)
            .await
            .unwrap()
            .token
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-omni-code-client-id", id.parse().unwrap());
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        headers.insert("x-omni-code-workspace-scope", "client".parse().unwrap());
        headers
    }

    async fn choose_client_model(
        state: &Arc<AppState>,
        headers: &HeaderMap,
        provider: &str,
        model: &str,
    ) {
        let _ = update_model_selection(
            headers.clone(),
            State(state.clone()),
            Json(ModelSelectionInput {
                selection: Some(ModelSelection {
                    provider_id: provider.into(),
                    model: model.into(),
                }),
            }),
        )
        .await
        .unwrap();
    }

    fn conversation_headers(headers: &HeaderMap, id: &str) -> HeaderMap {
        let mut headers = headers.clone();
        headers.insert(
            "x-omni-code-workspace-scope",
            "conversation".parse().unwrap(),
        );
        headers.insert("x-omni-code-conversation-id", id.parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn main_conversations_share_progress_and_memory_but_keep_context_and_models_separate() {
        let (state, _, auth, project, root) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let id_a = uuid::Uuid::new_v4().to_string();
        let id_b = uuid::Uuid::new_v4().to_string();
        let a = conversation_headers(&auth, &id_a);
        let b = conversation_headers(&auth, &id_b);
        choose_client_model(&state, &a, "home-provider", "model-a").await;
        choose_client_model(&state, &b, "automatic", "model-b").await;
        let _ = save_settings(
            b.clone(),
            State(state.clone()),
            Json(DispatchSettings {
                agent: AgentKind::Codex,
            }),
        )
        .await
        .unwrap();
        submit_request(&state, &a, None, "Remember my preference").await;
        let request_a = submit_request(&state, &a, None, "Work for A").await;
        submit_request(&state, &b, None, "Work for B").await;
        {
            let inputs = mock.inputs.lock().await;
            let context = &inputs.last().unwrap()["input"]["context"];
            assert!(context["chat"].as_array().unwrap().is_empty());
            assert_eq!(
                context["long_term_memory"][0]["content"],
                "Prefer concise Chinese replies."
            );
            assert_eq!(context["dispatch_tasks"][0]["request_id"], request_a);
        }
        let snap_a = get_snapshot(
            a.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        let snap_b = get_snapshot(
            b.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(snap_a["chat"][0]["content"], "Remember my preference");
        assert_eq!(snap_a["conversation"]["active_project_id"], project);
        assert_eq!(snap_a["project_context"]["project"]["id"], project);
        assert_eq!(snap_b["chat"][0]["content"], "Work for B");
        assert_eq!(snap_b["dispatch_tasks"].as_array().unwrap().len(), 2);
        assert_eq!(snap_b["dispatch_tasks"][0]["conversation_id"], id_a);
        assert_eq!(
            snap_b["dispatch_tasks"][0]["model_selection"]["model"],
            "model-a"
        );
        assert_eq!(
            snap_b["dispatch_tasks"][1]["model_selection"]["model"],
            "model-b"
        );
        assert_eq!(snap_b["dispatch_tasks"][1]["agent"], "codex");
        let task_id = snap_b["dispatch_tasks"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let _ = control(
            b.clone(),
            State(state.clone()),
            Path(task_id),
            Json(Control {
                action: "pause".into(),
            }),
        )
        .await
        .unwrap();
        // New chats keep complete persisted history beyond the legacy 40-message window.
        for _ in 0..21 {
            submit_request(&state, &a, None, "progress?").await;
        }
        let history = conversations::history(b.clone(), State(state.clone()))
            .await
            .unwrap()
            .0
            .data;
        assert_eq!(history.as_array().unwrap().len(), 2);
        assert!(
            history
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["id"] == id_a && entry["message_count"] == 46)
        );
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        let memory = restored.memory.lock().await;
        assert_eq!(memory.conversations.len(), 2);
        assert_eq!(
            memory.conversations[&id_a]
                .configuration
                .selection
                .as_ref()
                .unwrap()
                .model,
            "model-a"
        );
        assert_eq!(
            memory.conversations[&id_b].configuration.agent,
            AgentKind::Codex
        );
        assert_eq!(memory.chats[&conversations::key(&id_a)].len(), 46);
        assert_eq!(memory.long_term_memory.len(), 1);
        drop(memory);
        let scope = ScopeQuery {
            project_id: Some(project.clone()),
            ..Default::default()
        };
        let proposal = chat(
            a.clone(),
            State(state.clone()),
            Query(scope.clone()),
            Json(ChatInput {
                references: Vec::new(),
                configuration: None,
                request_id: None,
                message: "Change scope".into(),
                attachments: vec![],
            }),
        )
        .await
        .unwrap()
        .0
        .data;
        let proposal_id = proposal.as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(
            apply_change(
                a.clone(),
                State(state.clone()),
                Query(ScopeQuery::default()),
                Path(proposal_id.clone())
            )
            .await
            .is_err()
        );
        assert!(
            !state
                .workspace_agent
                .memory
                .lock()
                .await
                .scopes
                .contains_key(&project)
        );
        let _ = apply_change(a, State(state.clone()), Query(scope), Path(proposal_id))
            .await
            .unwrap();
        assert_eq!(
            state.workspace_agent.memory.lock().await.scopes[&project].recent_days,
            7
        );
        let note_id = snap_a["long_term_memory"][0]["id"].as_str().unwrap();
        let _ = conversations::forget(b, State(state.clone()), Path(note_id.to_owned()))
            .await
            .unwrap();
        assert!(
            state
                .workspace_agent
                .memory
                .lock()
                .await
                .long_term_memory
                .is_empty()
        );
        server.abort();
    }

    #[tokio::test]
    async fn fresh_conversation_can_reopen_legacy_history_and_inflight_task_returns() {
        let (state, worker, auth, _, _) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let request_id = submit_request(&state, &auth, None, "Previous work").await;
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let task_id = state.workspace_agent.memory.lock().await.dispatch_tasks[0]
            .id
            .clone();
        worker.finish.notify_one();
        reconcile_status(&state, &task_id, Status::AwaitingReview).await;
        mock.hold_returns.store(true, Ordering::SeqCst);
        reporting::enqueue(&state).await;
        tokio::time::timeout(Duration::from_secs(3), mock.entered.notified())
            .await
            .unwrap();
        let new_id = uuid::Uuid::new_v4().to_string();
        let fresh = conversation_headers(&auth, &new_id);
        let snapshot = get_snapshot(
            fresh.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        assert!(snapshot["chat"].as_array().unwrap().is_empty());
        assert_eq!(snapshot["dispatch_tasks"].as_array().unwrap().len(), 1);
        let history = conversations::history(fresh.clone(), State(state.clone()))
            .await
            .unwrap()
            .0
            .data;
        assert_eq!(history.as_array().unwrap().len(), 1);
        let old_id = history[0]["id"].as_str().unwrap();
        assert_ne!(old_id, new_id);
        mock.hold_returns.store(false, Ordering::SeqCst);
        mock.release.notify_one();
        wait_return(&state, &request_id, 1).await;
        let reopened = get_snapshot(
            conversation_headers(&auth, old_id),
            State(state.clone()),
            Query(ScopeQuery::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(reopened["chat"][0]["content"], "Previous work");
        assert!(reopened["chat"].as_array().unwrap().iter().any(|entry| {
            entry["id"]
                .as_str()
                .unwrap()
                .starts_with("dispatch-return-")
        }));
        assert!(
            get_snapshot(fresh, State(state.clone()), Query(ScopeQuery::default()))
                .await
                .unwrap()
                .0
                .data["chat"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        server.abort();
    }

    #[tokio::test]
    async fn client_workspaces_isolate_choices_conversations_tasks_and_restart() {
        let (state, _, _, project, root) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let a = scoped_headers(&state, "client-a").await;
        let b = scoped_headers(&state, "client-b").await;
        choose_client_model(&state, &a, "home-provider", "model-a").await;
        choose_client_model(&state, &b, "automatic", "model-b").await;
        let _ = save_settings(
            a.clone(),
            State(state.clone()),
            Json(DispatchSettings {
                agent: AgentKind::Pi,
            }),
        )
        .await
        .unwrap();
        let _ = save_settings(
            b.clone(),
            State(state.clone()),
            Json(DispatchSettings {
                agent: AgentKind::Codex,
            }),
        )
        .await
        .unwrap();
        let request_a = submit_request(&state, &a, Some(&project), "Work for A").await;
        let request_b = submit_request(&state, &b, Some(&project), "Work for B").await;
        let query = ScopeQuery {
            project_id: Some(project.clone()),
            ..Default::default()
        };
        let snapshot_a = get_snapshot(a.clone(), State(state.clone()), Query(query.clone()))
            .await
            .unwrap()
            .0
            .data;
        let snapshot_b = get_snapshot(b.clone(), State(state.clone()), Query(query))
            .await
            .unwrap()
            .0
            .data;
        assert_eq!(snapshot_a["chat"][0]["content"], "Work for A");
        assert_eq!(snapshot_b["chat"][0]["content"], "Work for B");
        assert_eq!(snapshot_a["dispatch_tasks"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot_b["dispatch_tasks"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot_a["dispatch_tasks"][0]["agent"], "pi");
        assert_eq!(snapshot_b["dispatch_tasks"][0]["agent"], "codex");
        assert_eq!(snapshot_a["dispatch_reports"][0]["request_id"], request_a);
        assert_eq!(snapshot_b["dispatch_reports"][0]["request_id"], request_b);
        let a_task = snapshot_a["dispatch_tasks"][0]["id"].as_str().unwrap();
        assert!(
            control(
                b.clone(),
                State(state.clone()),
                Path(a_task.into()),
                Json(Control {
                    action: "cancel".into()
                })
            )
            .await
            .is_err()
        );
        assert_eq!(
            get_model_selection(a.clone(), State(state.clone()))
                .await
                .unwrap()
                .0
                .data["selection"]["model"],
            "model-a"
        );
        assert_eq!(
            settings(b.clone(), State(state.clone()))
                .await
                .unwrap()
                .0
                .data["agent"],
            "codex"
        );
        assert_eq!(mock.inputs.lock().await[0]["model"], "model-a");
        assert_eq!(mock.inputs.lock().await[1]["model"], "model-b");
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        let memory = restored.memory.lock().await;
        assert_eq!(
            memory.clients["client-a"].selection.as_ref().unwrap().model,
            "model-a"
        );
        assert_eq!(
            memory.clients["client-b"].dispatch_settings.agent,
            AgentKind::Codex
        );
        assert_eq!(
            memory.dispatch_tasks[0].client_id.as_deref(),
            Some("client-a")
        );
        assert_eq!(memory.model_selection.as_ref().unwrap().model, "home-model");
        assert_eq!(
            memory
                .chats
                .values()
                .filter(|entries| !entries.is_empty())
                .count(),
            2
        );
        server.abort();
    }

    #[tokio::test]
    async fn client_request_freezes_its_configuration_and_other_clients_do_not_wait() {
        for by_conversation in [false, true] {
            let (state, worker, auth, _, _) = fixture().await;
            let (mock, server) = configure_return_model(&state).await;
            let a = if by_conversation {
                conversation_headers(&auth, &uuid::Uuid::new_v4().to_string())
            } else {
                scoped_headers(&state, "frozen-a").await
            };
            let b = if by_conversation {
                conversation_headers(&auth, &uuid::Uuid::new_v4().to_string())
            } else {
                scoped_headers(&state, "frozen-b").await
            };
            choose_client_model(&state, &a, "home-provider", "saved-a").await;
            choose_client_model(&state, &b, "automatic", "saved-b").await;
            mock.hold_chats.store(true, Ordering::SeqCst);
            let request_id = uuid::Uuid::new_v4().to_string();
            let id = request_id.clone();
            let copied = state.clone();
            let headers = a.clone();
            let pending = tokio::spawn(async move {
                chat(
                    headers,
                    State(copied),
                    Query(ScopeQuery::default()),
                    Json(ChatInput {
                        references: Vec::new(),
                        request_id: Some(id),
                        message: "Hold this request".into(),
                        attachments: vec![],
                        configuration: Some(ChatConfiguration {
                            approval_mode: Some(crate::models::ApprovalMode::Auto),
                            selection: Some(ModelSelection {
                                provider_id: "home-provider".into(),
                                model: "request-model".into(),
                            }),
                            agent: AgentKind::Pi,
                        }),
                    }),
                )
                .await
                .unwrap()
            });
            tokio::time::timeout(Duration::from_secs(3), mock.chat_entered.notified())
                .await
                .unwrap();
            choose_client_model(&state, &a, "automatic", "changed-a").await;
            let _ = save_settings(
                a.clone(),
                State(state.clone()),
                Json(DispatchSettings {
                    agent: AgentKind::Codex,
                }),
            )
            .await
            .unwrap();
            tokio::time::timeout(
                Duration::from_secs(2),
                submit_request(&state, &b, None, "progress?"),
            )
            .await
            .unwrap();
            mock.chat_release.notify_one();
            let _ = pending.await.unwrap();
            tick(&state).await.unwrap();
            wait_calls(&worker, 1).await;
            let task = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
            assert_eq!(task.agent, AgentKind::Pi);
            assert_eq!(task.approval_mode, crate::models::ApprovalMode::Auto);
            assert_eq!(state.find_session(&task.session_id).await.unwrap().approval_mode, crate::models::ApprovalMode::Auto);
            assert_eq!(
                task.model_selection.as_ref().unwrap().model,
                "request-model"
            );
            assert_eq!(
                worker.selections.lock().await[0].1.as_deref(),
                Some("request-model")
            );
            worker.finish.notify_one();
            reconcile_status(&state, &task.id, Status::AwaitingReview).await;
            reporting::enqueue(&state).await;
            wait_return(&state, &request_id, 1).await;
            let result_a = get_snapshot(a, State(state.clone()), Query(ScopeQuery::default()))
                .await
                .unwrap()
                .0
                .data;
            let result_b = get_snapshot(b, State(state.clone()), Query(ScopeQuery::default()))
                .await
                .unwrap()
                .0
                .data;
            assert!(result_a["chat"].as_array().unwrap().iter().any(|entry| {
                entry["id"]
                    .as_str()
                    .unwrap()
                    .starts_with("dispatch-return-")
            }));
            assert!(!result_b["chat"].as_array().unwrap().iter().any(|entry| {
                entry["id"]
                    .as_str()
                    .unwrap()
                    .starts_with("dispatch-return-")
            }));
            assert_eq!(
                mock.inputs.lock().await.last().unwrap()["model"],
                "request-model"
            );
            server.abort();
        }
    }

    #[tokio::test]
    async fn first_scoped_client_keeps_legacy_work_without_copying_it_to_other_clients() {
        let (state, worker, legacy, _, _) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let request_id = submit_request(&state, &legacy, None, "Existing work").await;
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        let task_id = state.workspace_agent.memory.lock().await.dispatch_tasks[0]
            .id
            .clone();
        worker.finish.notify_one();
        reconcile_status(&state, &task_id, Status::AwaitingReview).await;
        let stale_task = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
        mock.hold_returns.store(true, Ordering::SeqCst);
        reporting::enqueue(&state).await;
        tokio::time::timeout(Duration::from_secs(3), mock.entered.notified())
            .await
            .unwrap();
        let a = scoped_headers(&state, "upgraded-a").await;
        let b = scoped_headers(&state, "new-b").await;
        let old = get_snapshot(
            a.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        let fresh = get_snapshot(b, State(state.clone()), Query(ScopeQuery::default()))
            .await
            .unwrap()
            .0
            .data;
        assert_eq!(old["chat"][0]["content"], "Existing work");
        assert_eq!(old["dispatch_tasks"].as_array().unwrap().len(), 1);
        assert!(fresh["chat"].as_array().unwrap().is_empty());
        assert!(fresh["dispatch_tasks"].as_array().unwrap().is_empty());
        // Scheduler writes and a return that started before migration keep the new owner.
        persist_task(&state, stale_task).await.unwrap();
        mock.hold_returns.store(false, Ordering::SeqCst);
        mock.release.notify_one();
        wait_return(&state, &request_id, 1).await;
        let returned = get_snapshot(a, State(state.clone()), Query(ScopeQuery::default()))
            .await
            .unwrap()
            .0
            .data;
        assert_eq!(returned["dispatch_tasks"].as_array().unwrap().len(), 1);
        assert!(returned["chat"].as_array().unwrap().iter().any(|entry| {
            entry["id"]
                .as_str()
                .unwrap()
                .starts_with("dispatch-return-")
        }));
        assert_eq!(returned["dispatch_reports"][0]["pending"], false);
        let parsed: ScopeQuery = serde_json::from_value(json!({"client_id":"upgraded-a"})).unwrap();
        assert!(
            parsed.client_id.is_none(),
            "request parameters cannot impersonate an authenticated client"
        );
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_client_request_id_collisions_cannot_reassign_tasks() {
        let (state, _, _, _, _) = fixture().await;
        let (mock, server) = configure_return_model(&state).await;
        let a = scoped_headers(&state, "collision-a").await;
        let b = scoped_headers(&state, "collision-b").await;
        let request_id = uuid::Uuid::new_v4().to_string();
        mock.hold_chats.store(true, Ordering::SeqCst);
        let input = ChatInput {
            references: Vec::new(),
            request_id: Some(request_id.clone()),
            message: "Hold this request".into(),
            attachments: vec![],
            configuration: None,
        };
        let copied = state.clone();
        let a_headers = a.clone();
        let pending = tokio::spawn(async move {
            chat(
                a_headers,
                State(copied),
                Query(ScopeQuery::default()),
                Json(input),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(3), mock.chat_entered.notified())
            .await
            .unwrap();
        let _ = chat(
            b.clone(),
            State(state.clone()),
            Query(ScopeQuery::default()),
            Json(ChatInput {
                references: Vec::new(),
                request_id: Some(request_id),
                message: "Work for B".into(),
                attachments: vec![],
                configuration: None,
            }),
        )
        .await
        .unwrap();
        mock.chat_release.notify_one();
        let error = pending.await.unwrap().err().unwrap();
        assert_eq!(error.status, StatusCode::CONFLICT);
        let result_a = get_snapshot(a, State(state.clone()), Query(ScopeQuery::default()))
            .await
            .unwrap()
            .0
            .data;
        let result_b = get_snapshot(b, State(state.clone()), Query(ScopeQuery::default()))
            .await
            .unwrap()
            .0
            .data;
        assert!(result_a["chat"].as_array().unwrap().is_empty());
        assert!(result_a["dispatch_tasks"].as_array().unwrap().is_empty());
        assert_eq!(result_b["chat"][0]["content"], "Work for B");
        assert_eq!(result_b["dispatch_tasks"].as_array().unwrap().len(), 1);
        assert_eq!(
            state
                .workspace_agent
                .memory
                .lock()
                .await
                .dispatch_tasks
                .len(),
            1
        );
        server.abort();
    }

    #[tokio::test]
    async fn saved_agent_applies_to_new_requests_and_queue_controls_are_durable() {
        let (state, worker, headers, project, root) = fixture().await;
        assert_eq!(
            state
                .workspace_agent
                .memory
                .lock()
                .await
                .dispatch_settings
                .agent,
            AgentKind::Pi
        );
        let _ = save_settings(
            headers.clone(),
            State(state.clone()),
            Json(DispatchSettings {
                agent: AgentKind::Codex,
            }),
        )
        .await
        .unwrap();
        {
            let mut memory = state.workspace_agent.memory.lock().await;
            append_plan(&mut memory, vec![plan(&project, vec![])], "first", &[]);
        }
        let id = state.workspace_agent.memory.lock().await.dispatch_tasks[0]
            .id
            .clone();
        let _ = control(
            headers.clone(),
            State(state.clone()),
            Path(id.clone()),
            Json(Control {
                action: "pause".into(),
            }),
        )
        .await
        .unwrap();
        tick(&state).await.unwrap();
        assert_eq!(worker.calls.load(Ordering::SeqCst), 0);
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        assert_eq!(
            restored.memory.lock().await.dispatch_settings.agent,
            AgentKind::Codex
        );
        let _ = control(
            headers,
            State(state.clone()),
            Path(id),
            Json(Control {
                action: "resume".into(),
            }),
        )
        .await
        .unwrap();
        tick(&state).await.unwrap();
        wait_calls(&worker, 1).await;
        assert_eq!(state.list_sessions().await[0].agent, AgentKind::Codex);
        let task = state.workspace_agent.memory.lock().await.dispatch_tasks[0].clone();
        state.cancel_turn(&task.session_id).await.unwrap();
    }
}
