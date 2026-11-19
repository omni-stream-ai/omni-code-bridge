//! Explicit task-state edits from assistant chat use the same durable actions as the task list.
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChatTaskAction {
    status: TaskStatus,
    #[serde(default)]
    all: bool,
    #[serde(default)]
    from_status: Option<TaskStatus>,
    #[serde(default)]
    from_statuses: Vec<TaskStatus>,
    #[serde(default)]
    tasks: Vec<TaskTarget>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct AppliedTaskAction {
    pub id: Option<String>,
    pub affected_tasks: usize,
    pub status: TaskStatus,
    pub undone: bool,
}
impl AppliedTaskAction {
    pub(super) fn notice(&self, message: &str) -> String {
        let chinese = message
            .chars()
            .any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c));
        if self.affected_tasks == 0 {
            return if chinese {
                "\n\n当前范围没有符合条件的任务，未修改任何任务。".into()
            } else {
                "\n\nNo matching tasks in the current scope. No task records changed.".into()
            };
        }
        let state = match self.status {
            TaskStatus::Completed => {
                if chinese {
                    "已完成"
                } else {
                    "completed"
                }
            }
            TaskStatus::Cancelled => {
                if chinese {
                    "已取消"
                } else {
                    "cancelled"
                }
            }
            TaskStatus::Pending => {
                if chinese {
                    "待继续"
                } else {
                    "pending"
                }
            }
            TaskStatus::AwaitingReview => {
                if chinese {
                    "待验收"
                } else {
                    "awaiting review"
                }
            }
            TaskStatus::InProgress => {
                if chinese {
                    "进行中"
                } else {
                    "in progress"
                }
            }
            TaskStatus::Blocked => {
                if chinese {
                    "待解决"
                } else {
                    "blocked"
                }
            }
        };
        if chinese {
            format!(
                "\n\n已将当前范围内 {} 项任务标记为“{}”，可撤销。",
                self.affected_tasks, state
            )
        } else {
            format!(
                "\n\nMarked {} task records in the current scope as {state}. You can undo this change.",
                self.affected_tasks
            )
        }
    }
}

pub(super) fn apply(
    memory: &mut Memory,
    action: ChatTaskAction,
    context: &[Value],
    _message: &str,
) -> Result<AppliedTaskAction, ApiError> {
    if action.all == !action.tasks.is_empty() {
        return Err(failure("Choose all tasks or specific task IDs".into()));
    }
    let matches_source = |status: &TaskStatus| {
        action
            .from_status
            .as_ref()
            .is_none_or(|filter| filter == status)
            && (action.from_statuses.is_empty() || action.from_statuses.contains(status))
    };
    let available = context
        .iter()
        .map(|item| {
            let target = TaskTarget {
                session_id: item["session_id"].as_str().unwrap_or_default().into(),
                task_id: item["task"]["id"].as_str().unwrap_or_default().into(),
                expected_status: serde_json::from_value(item["task"]["status"].clone()).ok(),
            };
            (target, item["archived"] == true)
        })
        .collect::<Vec<_>>();
    let mut targets = Vec::new();
    if action.all {
        for (target, archived) in available {
            let Some(status) = target.expected_status.as_ref() else {
                continue;
            };
            if matches_source(status) && (*status != action.status || archived) {
                targets.push(target);
            }
        }
    } else {
        for requested in action.tasks {
            let target = available
                .iter()
                .find(|(target, _)| {
                    target.session_id == requested.session_id && target.task_id == requested.task_id
                })
                .ok_or_else(|| failure("The requested task is outside the current scope".into()))?
                .0
                .clone();
            if target
                .expected_status
                .as_ref()
                .is_none_or(|status| !matches_source(status))
            {
                return Err(task_conflict(
                    "The requested task does not have the specified status",
                ));
            }
            if target.expected_status.as_ref() != Some(&action.status) {
                targets.push(target);
            }
        }
    }
    let count = targets.len();
    let id = if targets.is_empty() {
        None
    } else {
        let id = uuid::Uuid::new_v4().to_string();
        apply_task_status(
            memory,
            &TaskStatusInput {
                request_id: id.clone(),
                expected_status: TaskStatus::Pending,
                tasks: targets,
                status: action.status.clone(),
            },
            None,
        )?;
        Some(id)
    };
    Ok(AppliedTaskAction {
        id,
        affected_tasks: count,
        status: action.status,
        undone: false,
    })
}

pub(super) fn mark_undone(memory: &mut Memory, id: &str) {
    for message in memory.chats.values_mut().flatten() {
        if let Some(action) = &mut message.task_action {
            if action.id.as_deref() == Some(id) {
                action.undone = true;
            }
        }
    }
    for receipt in &mut memory.chat_receipts {
        if let Some(messages) = receipt.response.as_array_mut() {
            for message in messages {
                if message["task_action"]["id"].as_str() == Some(id) {
                    message["task_action"]["undone"] = json!(true);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        adapter::ProviderRegistry,
        bridge_settings::BridgeSettingsInput,
        models::{
            AgentKind, ClientAuthRequestInput, CreateProjectInput, CreateSessionInput,
            ModelProviderConfig,
        },
    };

    fn task(id: &str, status: TaskStatus) -> Task {
        Task {
            id: id.into(),
            title: id.into(),
            summary: "Task summary".into(),
            created_at: Some(Utc::now() - chrono::Duration::hours(1)),
            updated_at: Some(Utc::now()),
            completed_at: matches!(status, TaskStatus::Completed | TaskStatus::Cancelled)
                .then(Utc::now),
            progress: None,
            group: None,
            status,
            source_message_id: "u1".into(),
            evidence_message_id: "a1".into(),
        }
    }
    fn analysis(tasks: Vec<Task>) -> Analysis {
        serde_json::from_value(json!({"updated_at":Utc::now(),"analyzed_at":Utc::now(),"tasks":tasks,"truncated":false})).unwrap()
    }
    fn context(memory: &Memory, session: &str) -> Vec<Value> {
        memory.analyses[session]
            .tasks
            .iter()
            .map(|task| json!({"session_id":session,"task":task,"archived":false}))
            .collect()
    }
    fn all(status: TaskStatus) -> ChatTaskAction {
        ChatTaskAction {
            status,
            all: true,
            from_status: None,
            from_statuses: vec![],
            tasks: vec![],
        }
    }

    #[test]
    fn direct_actions_allow_every_state_transition_and_keep_times_consistent() {
        let states = [
            TaskStatus::Pending,
            TaskStatus::InProgress,
            TaskStatus::AwaitingReview,
            TaskStatus::Blocked,
            TaskStatus::Completed,
            TaskStatus::Cancelled,
        ];
        for from in &states {
            for to in &states {
                let mut memory = Memory::default();
                memory
                    .analyses
                    .insert("s".into(), analysis(vec![task("t", from.clone())]));
                let created = memory.analyses["s"].tasks[0].created_at;
                let target = TaskTarget {
                    session_id: "s".into(),
                    task_id: "t".into(),
                    expected_status: Some(from.clone()),
                };
                let input = TaskStatusInput {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    tasks: vec![target.clone()],
                    expected_status: from.clone(),
                    status: to.clone(),
                };
                apply_task_status(&mut memory, &input, Some(500)).unwrap();
                let updated = effective_task(&memory, &target).unwrap();
                assert_eq!(&updated.status, to, "{from:?} -> {to:?}");
                assert_eq!(updated.created_at, created);
                assert_eq!(
                    updated.completed_at.is_some(),
                    matches!(to, TaskStatus::Completed | TaskStatus::Cancelled)
                );
                let before = serde_json::to_value(&memory).unwrap();
                apply_task_status(&mut memory, &input, Some(500)).unwrap();
                assert_eq!(
                    serde_json::to_value(&memory).unwrap(),
                    before,
                    "retries must not change task times"
                );
            }
        }
    }

    #[test]
    fn chat_edits_are_scoped_atomic_and_cover_more_than_a_model_context() {
        let mut memory = Memory::default();
        memory.analyses.insert(
            "s".into(),
            analysis(
                (0..501)
                    .map(|i| task(&format!("t{i}"), TaskStatus::InProgress))
                    .collect(),
            ),
        );
        memory.analyses.insert(
            "outside".into(),
            analysis(vec![task("other", TaskStatus::Pending)]),
        );
        let captured = context(&memory, "s");
        let result = apply(
            &mut memory,
            all(TaskStatus::Completed),
            &captured,
            "all complete",
        )
        .unwrap();
        assert_eq!(result.affected_tasks, 501);
        assert!(!memory.manual_task_states.contains_key("outside"));
        assert_eq!(memory.task_actions.len(), 1);
        let outside: ChatTaskAction = serde_json::from_value(
            json!({"status":"blocked","tasks":[{"session_id":"outside","task_id":"other"}]}),
        )
        .unwrap();
        assert!(apply(&mut memory, outside, &captured, "block other").is_err());
        let before = serde_json::to_value(&memory).unwrap();
        assert!(
            apply(
                &mut memory,
                all(TaskStatus::Blocked),
                &captured,
                "block all"
            )
            .is_err(),
            "stale expected statuses must reject the entire edit"
        );
        assert_eq!(serde_json::to_value(&memory).unwrap(), before);
    }

    #[tokio::test]
    async fn chat_marks_all_scoped_tasks_complete_with_persistence_dedup_and_undo() {
        async fn model(Json(request): Json<Value>) -> impl axum::response::IntoResponse {
            let input: Value = serde_json::from_str(
                request["messages"].as_array().unwrap().last().unwrap()["content"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            let action = if input["latest_user_message"] == "所有任务都标记完成" {
                assert_eq!(input["context"]["tasks"].as_array().unwrap().len(), 120);
                assert_eq!(input["context"]["task_context_truncated"], true);
                json!({"status":"completed","all":true})
            } else {
                Value::Null
            };
            let output = json!({"reply":"好的。","task_action":action}).to_string();
            let chunk = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":{"role":"assistant","content":output},"finish_reason":null}]});
            let finish = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
            (
                [("content-type", "text/event-stream")],
                format!("data: {chunk}\n\ndata: {finish}\n\ndata: [DONE]\n\n"),
            )
        }
        let root = std::env::temp_dir().join(format!("task-chat-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut state = AppState::new_with_paths_and_providers(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
            ProviderRegistry::from_map(HashMap::new()),
        )
        .await;
        state
            .set_client_auth_path_for_test(root.join("auth.json"))
            .await;
        let state = Arc::new(state);
        let mut projects = Vec::new();
        let mut sessions = Vec::new();
        for index in 0..2 {
            let project = state
                .create_project(CreateProjectInput {
                    name: format!("P{index}"),
                    root_path: root.join(format!("p{index}")).to_string_lossy().into(),
                })
                .await;
            let session = state
                .create_session(CreateSessionInput {
                    client_session_id: uuid::Uuid::new_v4().to_string(),
                    project_id: project.id.clone(),
                    title: Some("Work".into()),
                    agent: AgentKind::Pi,
                    brief_reply_mode: false,
                    approval_mode: Default::default(),
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                })
                .await
                .unwrap();
            projects.push(project);
            sessions.push(session);
        }
        let states = [
            TaskStatus::Pending,
            TaskStatus::InProgress,
            TaskStatus::AwaitingReview,
            TaskStatus::Blocked,
            TaskStatus::Completed,
            TaskStatus::Cancelled,
        ];
        {
            let mut memory = state.workspace_agent.memory.lock().await;
            memory.analyses.insert(
                sessions[0].id.clone(),
                analysis(
                    (0..150)
                        .map(|i| task(&format!("t{i}"), states[i % 6].clone()))
                        .collect(),
                ),
            );
            memory.analyses.insert(
                sessions[1].id.clone(),
                analysis(vec![task("outside", TaskStatus::Pending)]),
            );
        }
        let auth = state
            .request_client_auth(ClientAuthRequestInput {
                client_id: "task-chat".into(),
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
        headers.insert("x-omni-code-client-id", "task-chat".parse().unwrap());
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
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
                    base_url: url,
                    api_key: "test".into(),
                    model: Some("mock".into()),
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
        let query = ScopeQuery {
            conversation_id: None,
            client_id: None,
            project_id: Some(projects[0].id.clone()),
            session_id: None,
        };
        let input = || ChatInput {
            references: Vec::new(),
            configuration: None,
            request_id: Some(request_id.clone()),
            message: "所有任务都标记完成".into(),
            attachments: vec![],
        };
        let response = chat(
            headers.clone(),
            State(state.clone()),
            Query(query.clone()),
            Json(input()),
        )
        .await
        .unwrap()
        .0
        .data;
        let response_again = chat(
            headers.clone(),
            State(state.clone()),
            Query(query.clone()),
            Json(input()),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(response, response_again);
        let action = &response.as_array().unwrap().last().unwrap()["task_action"];
        assert_eq!(action["affected_tasks"], 125);
        let action_id = action["id"].as_str().unwrap().to_string();
        let restored = WorkspaceAgent::open(root.join("metadata.workspace-agent.json")).unwrap();
        assert_eq!(
            restored.memory.lock().await.manual_task_states[&sessions[0].id].len(),
            125
        );
        assert!(
            !restored
                .memory
                .lock()
                .await
                .manual_task_states
                .contains_key(&sessions[1].id)
        );
        assert_eq!(
            state.list_sessions().await.len(),
            2,
            "a task-record edit must not create an execution session"
        );
        let _ = chat(
            headers.clone(),
            State(state.clone()),
            Query(query.clone()),
            Json(ChatInput {
                references: Vec::new(),
                configuration: None,
                request_id: None,
                message: "现在什么进度？".into(),
                attachments: vec![],
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state.workspace_agent.memory.lock().await.task_actions.len(),
            1
        );
        let _ = undo_task_status(headers, State(state.clone()), Path(action_id))
            .await
            .unwrap();
        let memory = state.workspace_agent.memory.lock().await;
        assert!(memory.task_actions[0].undone);
        assert!(
            memory
                .chat_receipts
                .iter()
                .flat_map(|receipt| receipt.response.as_array().unwrap())
                .filter(|message| message["task_action"]["id"].is_string())
                .all(|message| message["task_action"]["undone"] == true)
        );
        assert!(
            memory
                .chats
                .values()
                .flatten()
                .filter_map(|entry| entry.task_action.as_ref())
                .all(|action| action.undone)
        );
        for task in &memory.analyses[&sessions[0].id].tasks {
            let target = TaskTarget {
                session_id: sessions[0].id.clone(),
                task_id: task.id.clone(),
                expected_status: None,
            };
            assert_eq!(
                effective_task(&memory, &target).unwrap().status,
                task.status
            );
        }
        server.abort();
    }
}
