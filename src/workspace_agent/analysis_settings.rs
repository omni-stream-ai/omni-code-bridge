use super::*;

const MAX_PROMPT_CHARS: usize = 12_000;
pub(super) const DEFAULT_PROMPT: &str = "只识别用户提出或确认的、有明确交付目标的软件开发任务，包括功能实现、缺陷修复、重构、UI 与交互调整、测试、构建部署、项目配置、开发环境排障，以及与项目交付直接相关的文档维护。以疑问句提出的明确实现或修复需求也属于开发任务。仅询问知识、解释代码或原理、讨论方案、查询信息、闲聊、翻译，以及没有明确开发交付目标的讨论不算任务。出现技术词汇、处于项目会话中或助手提出建议，本身都不构成任务。已有开发任务的补充、澄清、进展询问和验收反馈应归入原任务，不另建任务。没有符合条件的开发请求时返回空任务列表。";

pub(super) fn prompt(memory: &Memory) -> &str {
    memory
        .task_analysis_prompt
        .as_deref()
        .unwrap_or(DEFAULT_PROMPT)
}

pub(super) fn prompt_hash(prompt: &str) -> String {
    format!("{:x}", Sha256::digest(prompt.as_bytes()))
}

fn settings(memory: &Memory) -> Value {
    json!({
        "prompt": prompt(memory),
        "default_prompt": DEFAULT_PROMPT,
    })
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new().route(
        "/workspace-agent/analysis-settings",
        get(get_settings).put(update_settings),
    )
}

async fn get_settings(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    Ok(Json(ApiResponse {
        data: settings(&*state.workspace_agent.memory.lock().await),
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptInput {
    prompt: String,
}

async fn update_settings(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<PromptInput>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let prompt = input.prompt.trim();
    if prompt.chars().count() > MAX_PROMPT_CHARS {
        return Err(failure(format!(
            "Task analysis prompt must be at most {MAX_PROMPT_CHARS} characters"
        )));
    }
    let mut memory = state.workspace_agent.memory.lock().await;
    let mut next = memory.clone();
    next.task_analysis_prompt =
        (!prompt.is_empty() && prompt != DEFAULT_PROMPT).then(|| prompt.to_string());
    state.workspace_agent.save(&next).await.map_err(failure)?;
    *memory = next;
    Ok(Json(ApiResponse {
        data: settings(&memory),
    }))
}

// Only an explicit criteria change may remove previously tracked tasks. Require
// the model to name every excluded ID so accidental omissions still fail.
pub(super) fn validate_selection(
    tasks: &[Task],
    ids: &HashSet<String>,
    previous: &[Task],
    excluded: &[String],
    criteria_changed: bool,
) -> Result<(), String> {
    if !criteria_changed && !excluded.is_empty() {
        return Err("Task exclusions require changed analysis criteria".into());
    }
    let mut unique = HashSet::new();
    for id in excluded {
        if !unique.insert(id)
            || !previous.iter().any(|task| &task.id == id)
            || tasks.iter().any(|task| &task.id == id)
        {
            return Err("Invalid excluded task ID".into());
        }
    }
    let retained = previous
        .iter()
        .filter(|task| !unique.contains(&task.id))
        .cloned()
        .collect::<Vec<_>>();
    validate_tasks(tasks, ids, &retained)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn criteria_changes_require_explicit_valid_exclusions_and_preserve_evidence() {
        let task: Task = serde_json::from_value(json!({
            "id": "explanation", "title": "Explain a concept",
            "summary": "The explanation was delivered.", "status": "completed",
            "source_message_id": "u1", "evidence_message_id": "a1",
        }))
        .unwrap();
        let prior = vec![task];
        let ids = HashSet::new();
        let excluded = vec!["explanation".into()];
        assert!(validate_selection(&[], &ids, &prior, &excluded, true).is_ok());
        assert!(validate_selection(&[], &ids, &prior, &excluded, false).is_err());
        assert!(
            validate_selection(&[], &ids, &prior, &[], true).is_err(),
            "an unexplained omission is still rejected"
        );
        assert!(validate_selection(&[], &ids, &prior, &["unknown".into()], true).is_err());
        assert!(
            validate_selection(
                &[],
                &ids,
                &prior,
                &["explanation".into(), "explanation".into()],
                true
            )
            .is_err()
        );
        assert!(validate_selection(&prior, &ids, &prior, &excluded, true).is_err());
        let mut changed = prior.clone();
        changed[0].status = TaskStatus::Pending;
        assert!(
            validate_selection(&changed, &ids, &prior, &[], true).is_err(),
            "changing criteria cannot invent status evidence"
        );
    }
}
