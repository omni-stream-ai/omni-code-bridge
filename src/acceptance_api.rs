use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    acceptance::{AcceptanceResult, AcceptanceRun, AcceptanceTestCase, CreateAcceptance},
    app_state::AppState,
    models::{ApiError, ApiResponse},
    project_environment::{EnvironmentConfig, EnvironmentMode, EnvironmentSettings},
    runtime_plugin::{MANIFEST, PluginInstallation},
};

#[path = "acceptance_analysis.rs"]
mod analysis;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/v2/runtime-plugins/x11box-acceptance",
            get(plugin_status)
                .post(install_plugin)
                .delete(uninstall_plugin),
        )
        .route("/v2/acceptance", get(list_runs).post(create_run))
        .route(
            "/v2/projects/{project_id}/environment",
            get(project_environment).post(save_project_environment),
        )
        .route(
            "/v2/projects/{project_id}/environment/detect",
            post(detect_project_environment),
        )
        .route(
            "/v2/projects/{project_id}/acceptance-environment",
            get(project_environment).post(save_project_environment),
        )
        .route(
            "/v2/projects/{project_id}/acceptance-environment/detect",
            post(detect_project_environment),
        )
        .route("/v2/acceptance/{id}", get(get_run))
        .route("/v2/acceptance/{id}/analyze", post(analysis::analyze))
        .route("/v2/acceptance/{id}/retry", post(retry_automation))
        .route("/v2/acceptance/{id}/rejudge", post(rejudge))
        .route("/v2/acceptance/{id}/test-plan", post(edit_test_plan))
        .route("/v2/acceptance/{id}/test-plan/retry", post(retry_test_plan))
        .route(
            "/v2/acceptance/{id}/test-plan/messages",
            post(revise_test_plan),
        )
        .route(
            "/v2/acceptance/{id}/test-plan/confirm",
            post(confirm_test_plan),
        )
        .route("/v2/acceptance/{id}/control", post(control))
        .route("/v2/acceptance/{id}/instructions", post(instruction))
        .route("/v2/acceptance/{id}/screenshots", post(screenshot))
        .route("/v2/acceptance/{id}/evidence", post(save_evidence))
        .route("/v2/acceptance/{id}/preview", get(preview))
        .route("/v2/acceptance/{id}/stop", post(stop))
        .route("/v2/acceptance/{id}/artifacts/{name}", get(artifact))
}

async fn require_project(state: &AppState, id: &str) -> Result<(), ApiError> {
    if !state
        .list_projects()
        .await
        .iter()
        .any(|project| project.id == id)
    {
        return Err(bad_request("project not found"));
    }
    Ok(())
}

async fn project_environment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<ApiResponse<EnvironmentSettings>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    require_project(&state, &project_id).await?;
    let project = state
        .find_project(&project_id)
        .await
        .ok_or_else(|| bad_request("project not found"))?;
    let mut settings = state
        .acceptance
        .environments
        .settings(&project_id)
        .map_err(internal)?;
    settings.detection = Some(crate::project_detection::detect(std::path::Path::new(
        &project.root_path,
    )));
    state
        .project_runtimes
        .prepare_detection(&state, &project, settings.detection.as_mut().unwrap())
        .map_err(internal)?;
    if uri.path().ends_with("/environment") {
        settings.config.build_targets = state
            .project_runtimes
            .targets(&state, &project)
            .map_err(internal)?;
        if settings
            .current
            .as_ref()
            .is_some_and(|current| current.config != settings.config)
        {
            settings.current = None;
        }
    }
    Ok(Json(ApiResponse { data: settings }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveEnvironment {
    #[serde(default)]
    build: Option<crate::project_detection::BuildConfiguration>,
    #[serde(default)]
    build_targets: Vec<crate::project_detection::BuildTarget>,
    #[serde(default)]
    build_reasoning: String,
    #[serde(default)]
    mode: EnvironmentMode,
    #[serde(default)]
    dockerfile: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    rebuild: bool,
}

async fn save_project_environment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(input): Json<SaveEnvironment>,
) -> Result<Json<ApiResponse<EnvironmentSettings>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    require_project(&state, &project_id).await?;
    let _profiles = state.project_runtimes.profiles_lock.lock().await;
    let excluded = state
        .project_runtimes
        .preferences(&project_id)
        .map_err(internal)?
        .excluded;
    let mut config = EnvironmentConfig {
        build: input.build,
        build_targets: input.build_targets,
        build_reasoning: input.build_reasoning,
        mode: input.mode,
        dockerfile: input.dockerfile,
        image: input.image,
        ..Default::default()
    };
    let previous_count = config.build_targets.len();
    config
        .build_targets
        .retain(|target| !excluded.contains(&target.directory));
    if previous_count != config.build_targets.len() {
        config.build = config
            .build_targets
            .first()
            .map(|target| target.configuration.clone());
    }
    config.validate().map_err(operation_error)?;
    // Retain per-directory profiles when a later acceptance run selects only a
    // subset. Manual runtime settings must not disappear with that test scope.
    let previous = state
        .acceptance
        .environments
        .config(&project_id)
        .map_err(internal)?;
    state
        .project_runtimes
        .remember_targets(&project_id, &previous.build_targets)
        .await
        .map_err(internal)?;
    let settings = state
        .acceptance
        .environments
        .save_config(&project_id, config, input.rebuild)
        .map_err(operation_error)?;
    state
        .project_runtimes
        .remember_targets(&project_id, &settings.config.build_targets)
        .await
        .map_err(internal)?;
    Ok(Json(ApiResponse { data: settings }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DetectEnvironment {
    #[serde(default)]
    agent: bool,
    #[serde(default)]
    all_targets: bool,
    #[serde(default)]
    current: crate::project_detection::BuildConfiguration,
    #[serde(default)]
    target_directory: Option<String>,
    #[serde(default)]
    requirements: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    current_targets: Vec<crate::project_detection::BuildTarget>,
}
async fn detect_project_environment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(input): Json<DetectEnvironment>,
) -> Result<Json<ApiResponse<crate::project_detection::Detection>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let project = state
        .find_project(&project_id)
        .await
        .ok_or_else(|| bad_request("project not found"))?;
    input.current.validate().map_err(operation_error)?;
    if input.all_targets && input.target_directory.is_some() {
        return Err(bad_request("全项目配置不能限定单个子项目"));
    }
    let root = std::path::Path::new(&project.root_path);
    let mut detection = crate::project_detection::detect(root);
    state
        .project_runtimes
        .prepare_detection(&state, &project, &mut detection)
        .map_err(internal)?;
    if let Some(directory) = &input.target_directory {
        let candidate = detection
            .candidates
            .iter()
            .find(|candidate| candidate.directory == *directory)
            .ok_or_else(|| bad_request("验收目标未在识别结果中找到，请刷新后重试"))?;
        detection.configuration = candidate.configuration.clone();
    }
    if input.all_targets || input.agent {
        let identified = if input.all_targets {
            let excluded = state
                .project_runtimes
                .preferences(&project_id)
                .map_err(internal)?
                .excluded;
            let current_targets: Vec<_> = input
                .current_targets
                .iter()
                .filter(|target| !excluded.contains(&target.directory))
                .cloned()
                .collect();
            crate::project_detection::configure_all_targets(
                &state,
                &project_id,
                root,
                &detection,
                &current_targets,
                input.agent,
            )
            .await
        } else {
            crate::project_detection::identify_with_agent(
                &state,
                &project_id,
                root,
                &input.current,
                &detection,
                input.target_directory.as_deref(),
                &crate::project_detection::IdentificationContext {
                    all_targets: input.all_targets,
                    requirements: input.requirements,
                    session_id: input.session_id,
                    current_targets: input.current_targets,
                },
            )
            .await
        }
        .map_err(operation_error)?;
        detection.configuration = identified
            .targets
            .first()
            .map(|target| target.configuration.clone())
            .unwrap_or_default();
        detection.targets = identified.targets;
        detection.reasoning = identified.reasoning;
    }
    Ok(Json(ApiResponse { data: detection }))
}

async fn list_runs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<AcceptanceRun>>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let mut runs = state.acceptance.list().await.map_err(internal)?;
    for run in &mut runs {
        project_pending_approvals(&state, run).await;
    }
    Ok(Json(ApiResponse { data: runs }))
}

async fn project_pending_approvals(state: &AppState, run: &mut AcceptanceRun) {
    run.pending_approvals.clear();
    if matches!(
        run.phase,
        crate::acceptance::RunPhase::Completed | crate::acceptance::RunPhase::Failed
    ) {
        return;
    }
    let mut sessions = run.requirement_agent_session_ids.clone();
    sessions.extend(run.agent_session_id.clone());
    sessions.extend(run.plan_agent_session_id.clone());
    sessions.extend(
        run.environment
            .as_ref()
            .and_then(|env| env.agent_session_id.clone()),
    );
    sessions.sort();
    sessions.dedup();
    for session_id in sessions {
        if let Some(request) = state.pending_session_approval(&session_id).await {
            run.pending_approvals
                .push(crate::acceptance::AcceptanceApproval {
                    session_id,
                    request,
                });
        }
    }
    if !run.pending_approvals.is_empty() {
        run.automation_progress = format!(
            "等待验收操作审批（{} 项），环境准备或执行尚未完成",
            run.pending_approvals.len()
        );
    }
}

async fn create_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<CreateAcceptance>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let project = state
        .list_projects()
        .await
        .into_iter()
        .find(|p| p.id == input.project_id)
        .ok_or_else(|| bad_request("project not found"))?;
    let mut context_messages = Vec::new();
    if let Some(session_id) = &input.session_id {
        let session = state
            .get_session(session_id)
            .await
            .ok_or_else(|| bad_request("session not found"))?;
        if session.session.project_id != project.id {
            return Err(bad_request("session belongs to another project"));
        }
        if input.automatic {
            context_messages = state
                .list_messages(session_id)
                .await
                .ok_or_else(|| bad_request("无法读取当前会话的验收需求"))?;
        }
    }
    let client = client_id(&headers)?;
    let data = state
        .acceptance
        .create(
            input,
            std::path::Path::new(&project.root_path),
            client,
            context_messages,
            Some(Arc::clone(&state)),
        )
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data })))
}

async fn get_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<AcceptanceRun>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let mut run = state
        .acceptance
        .snapshot(&id)
        .await
        .map_err(operation_error)?;
    project_pending_approvals(&state, &mut run).await;
    Ok(Json(ApiResponse { data: run }))
}

async fn rejudge(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let run = state
        .acceptance
        .rejudge(Arc::clone(&state), &id)
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data: run })))
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RetryAutomation {
    #[serde(default)]
    startup: Option<crate::acceptance::StartupDecision>,
    #[serde(default)]
    use_current_environment: bool,
}

async fn retry_automation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<RetryAutomation>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let run = state
        .acceptance
        .retry_with_environment(
            Arc::clone(&state),
            &id,
            input.startup,
            input.use_current_environment,
        )
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data: run })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditTestPlan {
    version: u64,
    cases: Vec<AcceptanceTestCase>,
}

async fn retry_test_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state
        .acceptance
        .retry_test_plan(Arc::clone(&state), &id)
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data })))
}

async fn edit_test_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<EditTestPlan>,
) -> Result<Json<ApiResponse<AcceptanceRun>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .edit_test_plan(&id, input.version, input.cases)
            .await
            .map_err(operation_error)?,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviseTestPlan {
    version: u64,
    message: String,
}

async fn revise_test_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<ReviseTestPlan>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state
        .acceptance
        .revise_test_plan(Arc::clone(&state), &id, input.version, input.message)
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmTestPlan {
    version: u64,
}

async fn confirm_test_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<ConfirmTestPlan>,
) -> Result<(StatusCode, Json<ApiResponse<AcceptanceRun>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state
        .acceptance
        .confirm_test_plan(Arc::clone(&state), &id, input.version, client_id(&headers)?)
        .await
        .map_err(operation_error)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlInput {
    acquire: bool,
}

async fn control(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<ControlInput>,
) -> Result<Json<ApiResponse<AcceptanceRun>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state
        .acceptance
        .control(&id, client_id(&headers)?, input.acquire)
        .await
        .map_err(operation_error)?;
    Ok(Json(ApiResponse { data }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstructionInput {
    instruction: String,
    app: Option<String>,
}

async fn instruction(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<InstructionInput>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state
        .acceptance
        .instruction(&id, &input.instruction, input.app.as_deref())
        .await
        .map_err(operation_error)?;
    Ok(Json(ApiResponse { data }))
}

async fn screenshot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<crate::acceptance::Artifact>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .screenshot(&id)
            .await
            .map_err(operation_error)?,
    }))
}

async fn preview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    crate::api::authorize_request(&headers, &state).await?;
    let bytes = state
        .acceptance
        .preview(&id)
        .await
        .map_err(operation_error)?;
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "image/png"),
            (axum::http::header::CACHE_CONTROL, "private, no-store"),
        ],
        bytes,
    )
        .into_response())
}

async fn save_evidence(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<AcceptanceRun>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let run = state
        .acceptance
        .snapshot(&id)
        .await
        .map_err(operation_error)?;
    if run.automatic
        && !matches!(
            run.automation,
            crate::acceptance::AutomationPhase::AwaitingConfirmation
                | crate::acceptance::AutomationPhase::Completed
                | crate::acceptance::AutomationPhase::Failed
        )
    {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "Automatic acceptance is still running".into(),
        });
    }
    let data = state
        .acceptance
        .save_evidence(&id)
        .await
        .map_err(operation_error)?;
    Ok(Json(ApiResponse { data }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopInput {
    passed: Option<bool>,
    #[serde(default)]
    notes: String,
}

async fn stop(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<StopInput>,
) -> Result<Json<ApiResponse<AcceptanceRun>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let submitted_by = client_id(&headers)?.to_owned();
    let result = input.passed.map(|passed| AcceptanceResult {
        passed,
        notes: input.notes,
        submitted_by,
        submitted_at: chrono::Utc::now(),
    });
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .stop(&id, result)
            .await
            .map_err(operation_error)?,
    }))
}

async fn artifact(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, name)): Path<(String, String)>,
) -> Result<axum::response::Response, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    artifact_response(&state, &id, &name, &headers, false).await
}

pub(crate) async fn artifact_response(
    state: &AppState,
    id: &str,
    name: &str,
    headers: &HeaderMap,
    download: bool,
) -> Result<axum::response::Response, ApiError> {
    use axum::{http::header, response::IntoResponse};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let (path, artifact) = state
        .acceptance
        .artifact(id, name)
        .await
        .map_err(operation_error)?;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| internal(e.into()))?;
    let length = file.metadata().await.map_err(|e| internal(e.into()))?.len();
    let range = match byte_range(
        headers.get(header::RANGE).and_then(|v| v.to_str().ok()),
        length,
    ) {
        Ok(range) => range,
        Err(()) => {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{length}"))],
            )
                .into_response());
        }
    };
    let (start, count, status) = match range {
        Some((start, end)) => (start, end - start + 1, StatusCode::PARTIAL_CONTENT),
        None => (0, length, StatusCode::OK),
    };
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|e| internal(e.into()))?;
    let stream = futures_util::stream::try_unfold(file.take(count), |mut file| async move {
        let mut buffer = vec![0; 64 * 1024];
        let n = file.read(&mut buffer).await?;
        buffer.truncate(n);
        Ok::<_, std::io::Error>(if n == 0 { None } else { Some((buffer, file)) })
    });
    let mut response = (
        status,
        [
            (header::CONTENT_TYPE, artifact.content_type),
            (header::CONTENT_LENGTH, count.to_string()),
            (header::ACCEPT_RANGES, "bytes".into()),
            (header::CACHE_CONTROL, "private, no-store".into()),
            (header::REFERRER_POLICY, "no-referrer".into()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".into()),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response();
    if let Some((start, end)) = range {
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{length}").parse().unwrap(),
        );
    }
    if download {
        // Artifact names are generated by Bridge; still encode unsafe characters.
        let safe: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{safe}\"").parse().unwrap(),
        );
    }
    Ok(response)
}

fn byte_range(value: Option<&str>, length: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(value) = value.strip_prefix("bytes=") else {
        return Ok(None);
    };
    // Multiple ranges are optional. Send the whole resource instead.
    if value.contains(',') {
        return Ok(None);
    }
    if length == 0 {
        return Err(());
    }
    let (start, end) = value.split_once('-').ok_or(())?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        return Ok(Some((length.saturating_sub(suffix), length - 1)));
    }
    let start = start.parse::<u64>().map_err(|_| ())?;
    let end = if end.is_empty() {
        length - 1
    } else {
        end.parse::<u64>().map_err(|_| ())?.min(length - 1)
    };
    if start >= length || end < start {
        return Err(());
    }
    Ok(Some((start, end)))
}

fn client_id(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get("x-omni-code-client-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| bad_request("missing client id"))
}

fn bad_request(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

fn operation_error(error: anyhow::Error) -> ApiError {
    let message = format!("{error:#}");
    let status = if message.contains("not found") {
        StatusCode::NOT_FOUND
    } else if message.contains("control")
        || message.contains("not ready")
        || message.contains("not running")
        || message.contains("already exists")
        || message.contains("已更新")
        || message.contains("当前不能")
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    ApiError { status, message }
}

async fn plugin_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let installation = state.runtime_plugins.status().await.map_err(internal)?;
    Ok(Json(ApiResponse {
        data: json!({
            "manifest": serde_json::from_str::<Value>(MANIFEST).map_err(|e| internal(e.into()))?,
            "installation": installation,
        }),
    }))
}

async fn install_plugin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<ApiResponse<PluginInstallation>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let data = state.runtime_plugins.install().await.map_err(internal)?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data })))
}

fn internal(error: anyhow::Error) -> ApiError {
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("{error:#}"),
    }
}

async fn uninstall_plugin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    state
        .acceptance
        .uninstall()
        .await
        .map_err(operation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_responses_show_unsaved_failure_instead_of_stale_build_progress() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let cached = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        let path = fixture
            .state
            .runtime_plugins
            .root
            .join("runs")
            .join(&fixture.id)
            .join("run.json");
        {
            let mut run = cached.lock().await;
            run.phase = crate::acceptance::RunPhase::Starting;
            run.automation_progress = "正在构建项目环境".into();
            crate::runtime_plugin::atomic_json(&path, &*run).unwrap();
            run.phase = crate::acceptance::RunPhase::Failed;
            run.automation = crate::acceptance::AutomationPhase::Failed;
            run.automation_progress = "验收状态保存失败".into();
            run.error = Some("磁盘空间不足：No space left on device (os error 28)".into());
        }
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        for suffix in [format!("/{}", fixture.id), String::new()] {
            let response = client
                .get(format!("{origin}/v2/acceptance{suffix}"))
                .headers(fixture.headers.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body: Value = response.json().await.unwrap();
            let run = if suffix.is_empty() {
                &body["data"][0]
            } else {
                &body["data"]
            };
            assert_eq!(run["phase"], "failed");
            assert_eq!(run["automation"], "failed");
            assert!(
                run["error"]
                    .as_str()
                    .unwrap()
                    .contains("No space left on device")
            );
            assert_eq!(run["automation_progress"], "验收状态保存失败");
        }
        server.abort();
    }

    #[tokio::test]
    async fn run_responses_project_live_internal_approvals_without_persisting_them() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let request: crate::models::ApprovalRequest = serde_json::from_value(json!({
            "request_id":"0", "kind":"command_execution", "command":"build app",
            "allow_accept_for_session":false, "allow_cancel":true, "resolvable":true
        }))
        .unwrap();
        for session in [
            "acceptance-environment",
            "acceptance-execution",
            "unrelated-session",
        ] {
            fixture.state.raise_approval(session, request.clone()).await;
        }
        let path = fixture
            .state
            .runtime_plugins
            .root
            .join("runs")
            .join(&fixture.id)
            .join("run.json");
        let mut run: AcceptanceRun =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        run.phase = crate::acceptance::RunPhase::Starting;
        run.agent_session_id = Some("acceptance-execution".into());
        run.environment = Some(
            serde_json::from_value(json!({
                "id":"a".repeat(64), "project_id":run.project_id, "base_image_id":"base",
                "config":{}, "source_key":"source", "phase":"identifying", "name":"Agent",
                "description":"", "dockerfile":"", "verify_command":"", "progress":"preparing",
                "agent_session_id":"acceptance-environment", "updated_at":chrono::Utc::now()
            }))
            .unwrap(),
        );
        crate::runtime_plugin::atomic_json(&path, &run).unwrap();
        // Populate the cache before snapshot recovery can interpret this as a restart.
        let cached = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        *cached.lock().await = run.clone();
        crate::runtime_plugin::atomic_json(&path, &run).unwrap();
        let Json(response) = get_run(
            State(Arc::clone(&fixture.state)),
            fixture.headers.clone(),
            Path(fixture.id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(response.data.pending_approvals.len(), 2);
        assert!(
            response
                .data
                .automation_progress
                .contains("等待验收操作审批")
        );
        assert!(
            response
                .data
                .pending_approvals
                .iter()
                .any(|a| a.session_id == "acceptance-environment")
        );
        assert!(
            !std::fs::read_to_string(&path)
                .unwrap()
                .contains("pending_approvals")
        );
        fixture
            .state
            .resolve_approval(
                "acceptance-environment",
                "0",
                crate::models::ApprovalChoice::Accept,
            )
            .await;
        let Json(response) = list_runs(State(Arc::clone(&fixture.state)), fixture.headers.clone())
            .await
            .unwrap();
        assert_eq!(response.data[0].pending_approvals.len(), 1);
        assert_eq!(
            response.data[0].pending_approvals[0].session_id,
            "acceptance-execution"
        );
        let mut terminal = response.data[0].clone();
        terminal.phase = crate::acceptance::RunPhase::Completed;
        project_pending_approvals(&fixture.state, &mut terminal).await;
        assert!(terminal.pending_approvals.is_empty());
    }

    #[tokio::test]
    async fn project_environments_are_authenticated_isolated_and_validated() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let a = fixture
            .state
            .create_project(crate::models::CreateProjectInput {
                name: "A".into(),
                root_path: fixture.root.join("a").to_string_lossy().into_owned(),
            })
            .await;
        let b = fixture
            .state
            .create_project(crate::models::CreateProjectInput {
                name: "B".into(),
                root_path: fixture.root.join("b").to_string_lossy().into_owned(),
            })
            .await;
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let url = format!("{origin}/v2/projects/{}/acceptance-environment", a.id);
        assert_eq!(
            client.get(&url).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let response = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"mode":"dockerfile","dockerfile":"FROM {{BASE_IMAGE}}","rebuild":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let data: Value = response.json().await.unwrap();
        assert_eq!(data["data"]["config"]["mode"], "dockerfile");
        assert!(
            !data["data"]["config"]["generation"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        let response = client
            .get(format!(
                "{origin}/v2/projects/{}/acceptance-environment",
                b.id
            ))
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap();
        let data: Value = response.json().await.unwrap();
        assert_eq!(data["data"]["config"]["mode"], "auto");
        let response = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"mode":"image","image":"--privileged"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = client
            .get(format!(
                "{origin}/v2/projects/missing/acceptance-environment"
            ))
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        server.abort();
    }

    #[tokio::test]
    async fn build_detection_is_read_only_and_reviewed_edits_survive_reload() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let root = fixture.root.join("detected");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("package.json"),
            r#"{"packageManager":"pnpm@9","scripts":{"build":"vite build"}}"#,
        )
        .unwrap();
        let project = fixture
            .state
            .create_project(crate::models::CreateProjectInput {
                name: "Detected".into(),
                root_path: root.to_string_lossy().into_owned(),
            })
            .await;
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let url = format!("{origin}/v2/projects/{}/acceptance-environment", project.id);
        let detection_url = format!("{url}/detect");
        assert_eq!(
            client
                .post(&detection_url)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let detected: Value = client
            .get(&url)
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            detected["data"]["detection"]["configuration"]["build_command"],
            "pnpm run build"
        );
        assert!(detected["data"]["config"]["build"].is_null());
        let reviewed = json!({"language":"typescript", "package_manager":"pnpm", "install_command":"", "build_command":"pnpm run custom", "start_command":"", "artifact_directory":""});
        let saved: Value = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"mode":"auto", "build": reviewed}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(saved["data"]["config"]["build"], reviewed);
        let detected: Value = client
            .post(&detection_url)
            .headers(fixture.headers.clone())
            .json(&json!({"agent":false, "current":reviewed}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            detected["data"]["configuration"]["build_command"],
            "pnpm run build"
        );
        let restored: Value = client
            .get(&url)
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(restored["data"]["config"]["build"], reviewed);
        let reloaded = crate::project_environment::ProjectEnvironmentStore::new(Arc::clone(
            &fixture.state.runtime_plugins,
        ));
        assert_eq!(
            reloaded
                .config(&project.id)
                .unwrap()
                .build
                .unwrap()
                .build_command,
            "pnpm run custom"
        );
        let invalid = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"build":{"build_command":"x".repeat(8001)}}))
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        server.abort();
    }

    #[tokio::test]
    async fn build_detection_all_targets_uses_program_results_without_a_model() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let root = fixture.root.join("program-detection");
        for (directory, manifest, content) in [
            (
                "api",
                "Cargo.toml",
                "[package]\nname='api'\nversion='0.1.0'",
            ),
            (
                "web",
                "package.json",
                r#"{"packageManager":"npm@10","scripts":{"start":"vite"}}"#,
            ),
        ] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
            std::fs::write(root.join(directory).join(manifest), content).unwrap();
        }
        let project = fixture
            .state
            .create_project(crate::models::CreateProjectInput {
                name: "Program detection".into(),
                root_path: root.to_string_lossy().into_owned(),
            })
            .await;
        let sessions_before = fixture
            .state
            .list_project_sessions(&project.id)
            .await
            .unwrap()
            .len();
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let url = format!("{origin}/v2/projects/{}/environment/detect", project.id);
        let response = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({
                "all_targets": true,
                "agent": true,
                "current_targets": [{"directory": "api", "configuration": {
                    "start_command": "./custom-api", "build_command": "./custom-build"
                }}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        let targets = body["data"]["targets"].as_array().unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0]["configuration"]["start_command"], "./custom-api");
        assert_eq!(
            targets[0]["configuration"]["build_command"],
            "./custom-build"
        );
        assert_eq!(
            targets[0]["configuration"]["install_command"],
            "cd 'api' && cargo fetch"
        );
        assert_eq!(
            targets[1]["configuration"]["start_command"],
            "cd 'web' && npm run start"
        );
        assert_eq!(targets[1]["configuration"]["build_command"], "");
        assert_eq!(targets[1]["configuration"]["artifact_directory"], "");
        assert_eq!(
            body["data"]["reasoning"],
            "已优先使用程序识别结果并保留已有配置。"
        );
        assert_eq!(
            fixture
                .state
                .list_project_sessions(&project.id)
                .await
                .unwrap()
                .len(),
            sessions_before
        );

        // Explicit program-only requests also return incomplete profiles without
        // starting an Agent or requiring model configuration.
        let response = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({
                "all_targets": true, "agent": false
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(
            body["data"]["targets"][0]["configuration"]["start_command"],
            ""
        );
        assert_eq!(
            body["data"]["targets"][1]["configuration"]["start_command"],
            "cd 'web' && npm run start"
        );
        assert_eq!(
            fixture
                .state
                .list_project_sessions(&project.id)
                .await
                .unwrap()
                .len(),
            sessions_before
        );
        assert!(
            fixture
                .state
                .acceptance
                .environments
                .config(&project.id)
                .unwrap()
                .build_targets
                .is_empty()
        );
        server.abort();
    }

    #[tokio::test]
    async fn build_detection_exposes_targets_and_validates_explicit_selection() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let root = fixture.root.join("multi-project");
        for (directory, manifest, content) in [
            (
                "client",
                "pubspec.yaml",
                "dependencies:\n  flutter:\n    sdk: flutter",
            ),
            (
                "bridge",
                "Cargo.toml",
                "[package]\nname = 'bridge'\nversion = '0.1.0'",
            ),
        ] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
            std::fs::write(root.join(directory).join(manifest), content).unwrap();
        }
        let project = fixture
            .state
            .create_project(crate::models::CreateProjectInput {
                name: "Multiple targets".into(),
                root_path: root.to_string_lossy().into_owned(),
            })
            .await;
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let url = format!("{origin}/v2/projects/{}/acceptance-environment", project.id);
        let detected: Value = client
            .get(&url)
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            detected["data"]["detection"]["configuration"]["language"],
            ""
        );
        assert_eq!(
            detected["data"]["detection"]["candidates"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let selected: Value = client
            .post(format!("{url}/detect"))
            .headers(fixture.headers.clone())
            .json(&json!({"target_directory": "client"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(selected["data"]["configuration"]["language"], "flutter");
        assert_eq!(
            selected["data"]["configuration"]["install_command"],
            "cd 'client' && flutter pub get"
        );
        assert!(
            fixture
                .state
                .acceptance
                .environments
                .config(&project.id)
                .unwrap()
                .build
                .is_none()
        );
        let targets = json!([
            {"directory":"client","role":"客户端","reason":"展示登录结果",
             "configuration":{"language":"flutter","package_manager":"flutter pub","install_command":"","build_command":"cd 'client' && custom-build","start_command":"","artifact_directory":""}},
            {"directory":"bridge","role":"后端服务","reason":"提供登录 API",
             "configuration":{"language":"rust","package_manager":"cargo","install_command":"","build_command":"cd 'bridge' && cargo build","start_command":"cd 'bridge' && ./api","artifact_directory":""}}
        ]);
        let saved = client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"build_targets": targets, "build_reasoning": "登录前后端联动"}))
            .send()
            .await
            .unwrap();
        assert_eq!(saved.status(), StatusCode::OK);
        let restored: Value = client
            .get(&url)
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(restored["data"]["config"]["build_targets"], targets);
        assert_eq!(
            restored["data"]["config"]["build_reasoning"],
            "登录前后端联动"
        );
        for invalid in ["../outside", "client/../bridge", "missing"] {
            let response = client
                .post(format!("{url}/detect"))
                .headers(fixture.headers.clone())
                .json(&json!({"agent": true, "target_directory": invalid}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        for invalid in [
            json!({"all_targets": true, "target_directory": "client"}),
            json!({"agent": true, "all_targets": true, "target_directory": "client"}),
        ] {
            let response = client
                .post(format!("{url}/detect"))
                .headers(fixture.headers.clone())
                .json(&invalid)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        server.abort();
    }

    #[tokio::test]
    async fn test_plan_http_edits_are_persisted_and_old_confirmation_is_rejected() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let path = fixture
            .state
            .runtime_plugins
            .root
            .join("runs")
            .join(&fixture.id)
            .join("run.json");
        let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["phase"] = json!("planning");
        value["automatic"] = json!(true);
        value["automation"] = json!("awaiting_plan_approval");
        let cases = json!([{"id":"login","title":"Login errors","steps":["Enter an invalid password"],"expected":"Show an error"}]);
        value["test_plan"] = json!({"version":1,"cases":cases});
        crate::runtime_plugin::atomic_json(&path, &value).unwrap();
        let (url, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let saved = client
            .post(format!("{url}/v2/acceptance/{}/test-plan", fixture.id))
            .headers(fixture.headers.clone())
            .json(&json!({"version":1,"cases":cases}))
            .send()
            .await
            .unwrap();
        assert_eq!(saved.status(), StatusCode::OK);
        let saved: Value = saved.json().await.unwrap();
        assert_eq!(saved["data"]["test_plan"]["version"], 2);
        assert!(saved["data"]["desktop_port"].is_null());
        let stale = client
            .post(format!(
                "{url}/v2/acceptance/{}/test-plan/confirm",
                fixture.id
            ))
            .headers(fixture.headers.clone())
            .json(&json!({"version":1}))
            .send()
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        server.abort();
    }

    #[test]
    fn media_ranges_cover_open_suffix_clamped_and_unsatisfiable_requests() {
        assert_eq!(byte_range(Some("bytes=2-5"), 10), Ok(Some((2, 5))));
        assert_eq!(byte_range(Some("bytes=7-"), 10), Ok(Some((7, 9))));
        assert_eq!(byte_range(Some("bytes=-3"), 10), Ok(Some((7, 9))));
        assert_eq!(byte_range(Some("bytes=-30"), 10), Ok(Some((0, 9))));
        assert_eq!(byte_range(Some("bytes=2-50"), 10), Ok(Some((2, 9))));
        for value in ["bytes=10-", "bytes=8-2", "bytes=-0", "bytes=a-2"] {
            assert!(byte_range(Some(value), 10).is_err());
        }
        assert_eq!(byte_range(None, 0), Ok(None));
        assert!(byte_range(Some("bytes=0-"), 0).is_err());
        assert_eq!(byte_range(Some("bytes=0-1,4-5"), 10), Ok(None));
    }

    #[tokio::test]
    async fn evidence_ranges_stream_only_requested_bytes() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let (url, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let url = format!("{url}/v2/acceptance/{}/artifacts/recording.mp4", fixture.id);
        let response = client
            .get(&url)
            .headers(fixture.headers.clone())
            .header("range", "bytes=2-5")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-range"], "bytes 2-5/10");
        assert_eq!(response.headers()["content-length"], "4");
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"2345");
        let response = client
            .get(&url)
            .headers(fixture.headers.clone())
            .header("range", "bytes=10-")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()["content-range"], "bytes */10");
        let response = client
            .head(&url)
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-length"], "10");
        assert!(response.bytes().await.unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn runtime_routes_reject_unauthorized_requests_before_side_effects() {
        let root = std::env::temp_dir().join(format!("acceptance-auth-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let state = Arc::new(
            AppState::new_with_paths(
                root.join("settings.json"),
                root.join("runtime.json"),
                root.join("metadata.json"),
            )
            .await,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = router()
            .merge(crate::desktop_gateway::router())
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let id = uuid::Uuid::new_v4();
        for (method, path, body) in [
            (
                reqwest::Method::GET,
                "/v2/runtime-plugins/x11box-acceptance".into(),
                None,
            ),
            (
                reqwest::Method::POST,
                "/v2/runtime-plugins/x11box-acceptance".into(),
                None,
            ),
            (
                reqwest::Method::DELETE,
                "/v2/runtime-plugins/x11box-acceptance".into(),
                None,
            ),
            (reqwest::Method::GET, "/v2/acceptance".into(), None),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/test-plan"),
                Some(json!({"version":1,"cases":[]})),
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/test-plan/messages"),
                Some(json!({"version":1,"message":"change cases"})),
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/test-plan/confirm"),
                Some(json!({"version":1})),
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/retry"),
                Some(json!({})),
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/rejudge"),
                None,
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/test-plan/retry"),
                None,
            ),
            (
                reqwest::Method::GET,
                "/v2/projects/missing/acceptance-environment".into(),
                None,
            ),
            (
                reqwest::Method::POST,
                "/v2/projects/missing/acceptance-environment".into(),
                Some(json!({"mode":"base"})),
            ),
            (
                reqwest::Method::GET,
                format!("/v2/acceptance/{id}/preview"),
                None,
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/control"),
                Some(json!({"acquire":true})),
            ),
            (
                reqwest::Method::POST,
                format!("/v2/acceptance/{id}/desktop-ticket"),
                None,
            ),
            (
                reqwest::Method::GET,
                format!("/v2/acceptance/{id}/desktop/"),
                None,
            ),
            (
                reqwest::Method::GET,
                format!("/v2/acceptance/{id}/artifacts/report.json"),
                None,
            ),
        ] {
            let mut request = client.request(method, format!("http://{address}{path}"));
            if let Some(body) = body {
                request = request.json(&body);
            }
            let response = request.send().await.unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ),
                "{path}: {}",
                response.status()
            );
        }
        assert!(!state.runtime_plugins.root.exists());
        server.abort();
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }
}
