use std::{
    convert::Infallible,
    io,
    path::{Path as StdPath, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::Multipart,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post, put},
};
use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    adapter,
    app_state::{AppState, EventReplay, MarkSessionReadError, SequencedSessionEvent},
    bridge_settings::{AiApprovalPromptInput, BridgeSettingsInput, ProjectAiApprovalInput},
    models::{
        AcpAgentDiagnosticResponse, AcpHandshakeProbe, AgentCommandForwarding, AgentCommandSummary,
        AgentCommandsSummary, AgentInstallInput, AgentKind, AgentReadiness, AgentSummary, ApiError,
        ApiResponse, AppUpdateManifest, ApprovalDecisionInput, CancelSessionReplyResult,
        ClientAuthRequestInput, CreateProjectInput, CreateSessionInput, FileCompletionItem,
        FileCompletionQuery, MarkSessionReadInput, MessageListPage, MessageListQuery,
        RegisterPushDeviceInput, ReplySummary, SendMessageInput, SessionEvent, SummarizeReplyInput,
        TriggerClientMessageInput, UpdateSessionInput, UploadedFileResponse,
    },
};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/files", get(get_file_by_path))
        .route("/files/completions", get(list_file_completions))
        .route("/uploads", post(upload_file))
        .route("/uploads/{id}", get(download_uploaded_file))
        .route("/app-update/manifest", get(app_update_manifest))
        .route("/app-update/apk", get(download_app_update_apk))
        .route("/client-auth/requests", post(request_client_auth))
        .route(
            "/client-auth/requests/{request_id}",
            get(get_client_auth_request),
        )
        .route("/settings", get(get_settings).put(update_settings))
        .route(
            "/settings/ai-approval-prompt",
            get(get_ai_approval_prompt).put(update_ai_approval_prompt),
        )
        .route("/projects", get(list_projects).post(create_project))
        .route(
            "/projects/{id}/ai-approval",
            get(get_project_ai_approval).put(update_project_ai_approval),
        )
        .route("/devices/register", post(register_push_device))
        .route("/client/messages", post(trigger_client_message))
        .route("/projects/{id}/sessions", get(list_project_sessions))
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{id}/cancel", post(cancel_session_reply))
        .route(
            "/sessions/{id}/messages",
            get(list_messages).post(send_message),
        )
        .route("/sessions/{id}", get(get_session).patch(update_session))
        .route("/sessions/{id}/read-state", put(mark_session_read))
        .route("/sessions/{id}/summary", post(summarize_reply))
        .route("/agents", get(list_agents))
        .route("/agents/acp/diagnostic", get(get_acp_agent_diagnostic))
        .route("/agents/commands", get(list_agent_commands))
        .route("/agents/install", post(install_agent_handler))
        .route(
            "/sessions/{id}/approvals/{request_id}",
            post(resolve_approval),
        )
        .route("/sessions/{id}/events", get(session_events))
}

#[derive(Debug, Deserialize)]
struct FileQuery {
    path: String,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AcpDiagnosticQuery {
    #[serde(default)]
    refresh: bool,
    #[serde(default)]
    provider_id: Option<String>,
    #[serde(default)]
    all: bool,
    #[serde(default)]
    probe: bool,
}

async fn app_update_manifest() -> Result<Json<AppUpdateManifest>, ApiError> {
    let Some(apk_path) = find_mobile_apk() else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let version_name =
        read_mobile_version_name().unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
    let version_code = apk_modified_version_code(&apk_path);
    let manifest = AppUpdateManifest {
        version_name,
        version_code,
        apk_url: "/app-update/apk".to_string(),
        release_notes: format!("Bridge 自动提供的 APK：{}", apk_path.display()),
        force: false,
    };
    Ok(Json(manifest))
}

async fn download_app_update_apk() -> Result<impl IntoResponse, ApiError> {
    let apk_path = find_mobile_apk().ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "mobile apk has not been built".to_string(),
    })?;
    let file_name = apk_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("omni-code.apk")
        .to_string();
    let bytes = tokio::fs::read(&apk_path)
        .await
        .map_err(|error| (StatusCode::NOT_FOUND, error.to_string()))?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.android.package-archive"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{file_name}\""))
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?,
    );

    Ok((StatusCode::OK, headers, Body::from(bytes)))
}

fn find_mobile_apk() -> Option<PathBuf> {
    let client_repo_root = find_client_repo_root()?;
    let candidates = [
        "build/app/outputs/flutter-apk/app-release.apk",
        "build/app/outputs/flutter-apk/app-profile.apk",
        "build/app/outputs/flutter-apk/app-debug.apk",
        "build/app/outputs/apk/release/app-release.apk",
        "build/app/outputs/apk/profile/app-profile.apk",
        "build/app/outputs/apk/debug/app-debug.apk",
    ];

    candidates
        .into_iter()
        .map(|path| client_repo_root.join(path))
        .filter(|path| path.is_file())
        .max_by_key(|path| {
            path.metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
        })
}

fn read_mobile_version_name() -> Option<String> {
    let pubspec = std::fs::read_to_string(find_client_repo_root()?.join("pubspec.yaml")).ok()?;
    let raw = pubspec
        .lines()
        .find_map(|line| line.trim().strip_prefix("version:"))?
        .trim();
    let version_name = raw.split_once('+').map(|(name, _)| name).unwrap_or(raw);
    Some(version_name.trim().to_string())
}

fn apk_modified_version_code(path: &std::path::Path) -> u64 {
    path.metadata()
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(1)
}

fn find_client_repo_root() -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    loop {
        if current.join("pubspec.yaml").is_file() && current.join("lib/main.dart").is_file() {
            return Some(current);
        }
        if current.join("omni-code/pubspec.yaml").is_file()
            && current.join("omni-code/lib/main.dart").is_file()
        {
            return Some(current.join("omni-code"));
        }
        if current.join("../omni-code/pubspec.yaml").is_file()
            && current.join("../omni-code/lib/main.dart").is_file()
        {
            return current.join("../omni-code").canonicalize().ok();
        }
        if !current.pop() {
            return None;
        }
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({
        "ok": true,
        "service": "omni-code-bridge",
    }))
}

async fn get_file_by_path(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<FileQuery>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let file_path = resolve_authorized_file_path(&state, &query).await?;
    let bytes = tokio::fs::read(&file_path)
        .await
        .map_err(|error| map_file_resolution_error(&query.path, error))?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type_for_path(&file_path)),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    Ok((StatusCode::OK, headers, Body::from(bytes)))
}

async fn list_file_completions(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<FileCompletionQuery>,
) -> Result<Json<ApiResponse<Vec<FileCompletionItem>>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let root = resolve_completion_root(&state, &query).await?;
    let items = list_completion_items(&root, &query)?;
    Ok(Json(ApiResponse { data: items }))
}

async fn upload_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;

    let mut uploaded = None;
    while let Some(field) = multipart.next_field().await.map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: format!("invalid multipart payload: {error}"),
    })? {
        let field_name = field.name().map(ToString::to_string);
        if field_name.as_deref() != Some("file") {
            continue;
        }

        let original_file_name = field
            .file_name()
            .map(ToString::to_string)
            .unwrap_or_else(|| "upload.bin".to_string());
        let content_type = field
            .content_type()
            .map(ToString::to_string)
            .unwrap_or_else(|| content_type_for_upload_name(&original_file_name).to_string());
        let bytes = field.bytes().await.map_err(|error| ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!("failed to read uploaded file: {error}"),
        })?;
        uploaded = Some((original_file_name, content_type, bytes));
        break;
    }

    let Some((original_file_name, content_type, bytes)) = uploaded else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "missing multipart field 'file'".to_string(),
        });
    };

    let id = uuid::Uuid::new_v4().to_string().replace('-', "");
    let file_name = sanitize_upload_file_name(&original_file_name);
    let stored_name = format!("{id}-{file_name}");
    let upload_dir = uploads_dir();
    tokio::fs::create_dir_all(&upload_dir)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let path = upload_dir.join(&stored_name);
    tokio::fs::write(&path, &bytes)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let url = format!("/uploads/{stored_name}");
    let absolute_url = absolute_url_from_headers(&headers, &url);
    let local_path = path.display().to_string();
    Ok((
        StatusCode::CREATED,
        Json(ApiResponse {
            data: UploadedFileResponse {
                id: stored_name,
                file_name,
                content_type,
                size_bytes: bytes.len() as u64,
                url,
                absolute_url,
                local_path,
            },
        }),
    ))
}

async fn download_uploaded_file(Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let file_name = sanitize_upload_lookup_id(&id).ok_or(ApiError {
        status: StatusCode::BAD_REQUEST,
        message: "invalid upload id".to_string(),
    })?;
    let path = uploads_dir().join(&file_name);
    let bytes = tokio::fs::read(&path).await.map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            ApiError {
                status: StatusCode::NOT_FOUND,
                message: "uploaded file not found".to_string(),
            }
        } else {
            ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: error.to_string(),
            }
        }
    })?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type_for_path(&path)),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=86400"),
    );
    Ok((StatusCode::OK, headers, Body::from(bytes)))
}

async fn get_settings(headers: HeaderMap, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    Json(ApiResponse {
        data: state.bridge_settings().await,
    })
    .into_response()
}

async fn update_settings(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<BridgeSettingsInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let settings = state
        .update_bridge_settings(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok(Json(ApiResponse { data: settings }))
}

async fn get_ai_approval_prompt(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let prompt = state.bridge_settings().await.ai_approval.prompt;
    Ok(Json(ApiResponse {
        data: AiApprovalPromptInput { prompt },
    }))
}

async fn update_ai_approval_prompt(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<AiApprovalPromptInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let prompt = state
        .update_ai_approval_prompt(input.prompt)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(ApiResponse {
        data: AiApprovalPromptInput { prompt },
    }))
}

async fn get_project_ai_approval(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let settings = state
        .project_ai_approval_settings(&id)
        .await
        .map_err(|error| (StatusCode::NOT_FOUND, error))?;
    Ok(Json(ApiResponse { data: settings }))
}

async fn update_project_ai_approval(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<ProjectAiApprovalInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let settings = state
        .update_project_ai_approval_settings(&id, input)
        .await
        .map_err(|error| (StatusCode::NOT_FOUND, error))?;
    Ok(Json(ApiResponse { data: settings }))
}

async fn list_sessions(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    Json(ApiResponse {
        data: state.list_sessions().await,
    })
    .into_response()
}

async fn list_projects(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    Json(ApiResponse {
        data: state.list_projects().await,
    })
    .into_response()
}

async fn create_session(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<CreateSessionInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let session = state
        .create_session(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: session })))
}

async fn create_project(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<CreateProjectInput>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let project = state.create_project(input).await;
    (StatusCode::CREATED, Json(ApiResponse { data: project })).into_response()
}

async fn register_push_device(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<RegisterPushDeviceInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let client_id = read_client_id(&headers)?;
    let device = state.register_push_device(client_id, input).await;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: device })))
}

async fn list_project_sessions(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Vec<crate::models::SessionSummary>>>, ApiError> {
    authorize_request_status(&headers, &state).await?;
    let sessions = state.list_project_sessions(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "project not found".to_string(),
    })?;
    Ok(Json(ApiResponse { data: sessions }))
}

async fn get_session(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<crate::models::SessionDetail>>, ApiError> {
    authorize_request_status(&headers, &state).await?;
    let session = state.get_session(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "session not found".to_string(),
    })?;
    Ok(Json(ApiResponse { data: session }))
}

async fn trigger_client_message(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<TriggerClientMessageInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let result = state
        .trigger_client_message(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    Ok((StatusCode::CREATED, Json(ApiResponse { data: result })))
}

async fn list_messages(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<MessageListQuery>,
) -> Result<Json<ApiResponse<MessageListPage>>, ApiError> {
    authorize_request_status(&headers, &state).await?;
    let messages = state.list_messages(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "session not found".to_string(),
    })?;
    let page = paginate_messages(messages, query)?;
    Ok(Json(ApiResponse { data: page }))
}

fn paginate_messages(
    messages: Vec<crate::models::ChatMessage>,
    query: MessageListQuery,
) -> Result<MessageListPage, ApiError> {
    if query.before_id.is_some() && query.after_id.is_some() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "before_id and after_id cannot be used together".to_string(),
        });
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let mut start = 0usize;
    let mut end = messages.len();

    let uses_after_cursor = query.after_id.is_some();

    if let Some(before_id) = query.before_id {
        let cursor_index = messages
            .iter()
            .position(|message| message.id == before_id)
            .ok_or(ApiError {
                status: StatusCode::BAD_REQUEST,
                message: format!("unknown before_id: {before_id}"),
            })?;
        end = message_unit_start(&messages, cursor_index);
    }

    if let Some(after_id) = query.after_id {
        let cursor_index = messages
            .iter()
            .position(|message| message.id == after_id)
            .ok_or(ApiError {
                status: StatusCode::BAD_REQUEST,
                message: format!("unknown after_id: {after_id}"),
            })?;
        start = message_unit_end(&messages, cursor_index, end);
    }

    if start > end {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "after_id must refer to a message before before_id".to_string(),
        });
    }

    let (skip, take_len, has_more) = if uses_after_cursor {
        let take_end = forward_message_window_end(&messages, start, end, limit);
        (start, take_end.saturating_sub(start), take_end < end)
    } else {
        let (skip, take_end) = trailing_message_window_bounds(&messages, start, end, limit);
        (skip, take_end.saturating_sub(skip), skip > start)
    };

    let page_messages = messages
        .into_iter()
        .skip(skip)
        .take(take_len)
        .collect::<Vec<_>>();
    let next_cursor = if has_more && uses_after_cursor {
        page_messages.last().map(|message| message.id.clone())
    } else if has_more {
        page_messages.first().map(|message| message.id.clone())
    } else {
        None
    };

    Ok(MessageListPage {
        messages: page_messages,
        has_more,
        next_cursor,
    })
}

fn is_user_message(message: &crate::models::ChatMessage) -> bool {
    matches!(message.role, crate::models::MessageRole::User)
}

fn message_unit_start(messages: &[crate::models::ChatMessage], index: usize) -> usize {
    if is_user_message(&messages[index]) {
        return index;
    }

    let mut start = index;
    while start > 0 && !is_user_message(&messages[start - 1]) {
        start -= 1;
    }
    start
}

fn message_unit_end(
    messages: &[crate::models::ChatMessage],
    index: usize,
    max_end: usize,
) -> usize {
    if is_user_message(&messages[index]) {
        return index + 1;
    }

    let mut end = index + 1;
    while end < max_end && !is_user_message(&messages[end]) {
        end += 1;
    }
    end
}

fn previous_message_unit_start(
    messages: &[crate::models::ChatMessage],
    start: usize,
    before: usize,
) -> usize {
    let mut unit_start = before - 1;
    if is_user_message(&messages[unit_start]) {
        return unit_start;
    }
    while unit_start > start && !is_user_message(&messages[unit_start - 1]) {
        unit_start -= 1;
    }
    unit_start
}

fn trailing_message_window_bounds(
    messages: &[crate::models::ChatMessage],
    start: usize,
    end: usize,
    limit: usize,
) -> (usize, usize) {
    let mut counted = 0usize;
    let mut window_start = end;
    while window_start > start && counted < limit {
        window_start = previous_message_unit_start(messages, start, window_start);
        counted += 1;
    }
    (window_start, end)
}

fn forward_message_window_end(
    messages: &[crate::models::ChatMessage],
    start: usize,
    end: usize,
    limit: usize,
) -> usize {
    let mut counted = 0usize;
    let mut window_end = start;
    while window_end < end && counted < limit {
        if is_user_message(&messages[window_end]) {
            window_end += 1;
        } else {
            while window_end < end && !is_user_message(&messages[window_end]) {
                window_end += 1;
            }
        }
        counted += 1;
    }
    window_end
}

async fn send_message(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SendMessageInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let (user_message, pending_reply) =
        state
            .send_message(&id, input)
            .await
            .map_err(|err| ApiError {
                status: StatusCode::NOT_FOUND,
                message: err,
            })?;

    Ok((
        StatusCode::CREATED,
        Json(ApiResponse {
            data: serde_json::json!({
                "user_message": user_message,
                "reply": pending_reply,
            }),
        }),
    ))
}

async fn summarize_reply(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SummarizeReplyInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let text = state
        .summarize_reply(&id, input.content)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    Ok((
        StatusCode::OK,
        Json(ApiResponse {
            data: ReplySummary { text },
        }),
    ))
}

async fn update_session(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<UpdateSessionInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let session = state
        .update_session_settings(&id, input.provider_id, input.reasoning_effort, input.model)
        .await
        .map_err(|err| ApiError {
            status: StatusCode::NOT_FOUND,
            message: err,
        })?;
    Ok(Json(ApiResponse { data: session }))
}

async fn mark_session_read(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<MarkSessionReadInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let session = state
        .mark_session_read(&id, &input.last_message_id)
        .await
        .map_err(|error| match error {
            MarkSessionReadError::NotFound(message) => ApiError {
                status: StatusCode::NOT_FOUND,
                message,
            },
            MarkSessionReadError::Persistence(message) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        })?;
    Ok(Json(ApiResponse { data: session }))
}

async fn cancel_session_reply(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let cancelled = state
        .cancel_turn(&id)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok(Json(ApiResponse {
        data: CancelSessionReplyResult { cancelled },
    }))
}

async fn list_agents(headers: HeaderMap, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let agents = vec![
        agent_summary(&state, AgentKind::Codex).await,
        agent_summary(&state, AgentKind::ClaudeCode).await,
        agent_summary(&state, AgentKind::OpenCode).await,
        agent_summary(&state, AgentKind::Acp).await,
    ];
    Json(ApiResponse { data: agents }).into_response()
}

async fn list_agent_commands(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let commands = vec![
        agent_commands_summary(AgentKind::Codex),
        agent_commands_summary(AgentKind::ClaudeCode),
        agent_commands_summary(AgentKind::OpenCode),
        agent_commands_summary(AgentKind::Acp),
    ];
    Json(ApiResponse { data: commands }).into_response()
}

async fn get_acp_agent_diagnostic(
    headers: HeaderMap,
    Query(query): Query<AcpDiagnosticQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let _refresh = query.refresh;
    let settings = state.bridge_settings().await;
    if query.all && query.provider_id.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                data: serde_json::json!({
                    "error": "query parameters `all=true` and `provider_id` cannot be used together"
                }),
            }),
        )
            .into_response();
    }
    let default_selected_provider_id = settings
        .acp_servers
        .iter()
        .filter(|server| server.enabled)
        .min_by_key(|server| server.priority)
        .map(|server| server.id.clone());
    if let Some(provider_id) = query
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        && !settings
            .acp_servers
            .iter()
            .any(|server| server.id == provider_id)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                data: serde_json::json!({
                    "error": format!(
                        "configured ACP server `{provider_id}` was not found in settings"
                    )
                }),
            }),
        )
            .into_response();
    }
    let probed_at = chrono::Utc::now().to_rfc3339();
    if query.all && query.provider_id.is_none() {
        let provider_ids = settings
            .acp_servers
            .iter()
            .map(|server| server.id.clone())
            .collect::<Vec<_>>();
        let mut items = Vec::with_capacity(provider_ids.len());
        for provider_id in provider_ids {
            let is_default_selected = default_selected_provider_id
                .as_deref()
                .map(|selected| selected == provider_id)
                .unwrap_or(false);
            let summary =
                adapter::summarize_acp_agent_for_provider(&settings, Some(provider_id.as_str()))
                    .await;
            let probe = if query.probe {
                adapter::probe_acp_handshake_for_provider(&settings, Some(provider_id.as_str()))
                    .await
            } else {
                None
            };
            items.push(acp_diagnostic_response_from_summary(
                summary,
                Some(provider_id),
                is_default_selected,
                probed_at.clone(),
                probe,
            ));
        }
        return Json(ApiResponse { data: items }).into_response();
    }

    let summary =
        adapter::summarize_acp_agent_for_provider(&settings, query.provider_id.as_deref()).await;
    Json(ApiResponse {
        data: acp_diagnostic_response_from_summary(
            summary,
            query.provider_id.clone(),
            query
                .provider_id
                .as_deref()
                .map(|provider_id| {
                    default_selected_provider_id
                        .as_deref()
                        .map(|selected| selected == provider_id)
                        .unwrap_or(false)
                })
                .unwrap_or(true),
            probed_at,
            if query.probe {
                adapter::probe_acp_handshake_for_provider(&settings, query.provider_id.as_deref())
                    .await
            } else {
                None
            },
        ),
    })
    .into_response()
}

fn acp_diagnostic_response_from_summary(
    summary: adapter::AcpAgentStatus,
    provider_id: Option<String>,
    is_default_selected: bool,
    probed_at: String,
    handshake_probe: Option<AcpHandshakeProbe>,
) -> AcpAgentDiagnosticResponse {
    let resolved_provider_id = provider_id.or_else(|| {
        summary
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.configured_server_id.clone())
    });
    let readiness = if !summary.installed {
        AgentReadiness::NotInstalled
    } else if summary.readiness_message.is_some() {
        AgentReadiness::AttentionRequired
    } else {
        AgentReadiness::Ready
    };
    AcpAgentDiagnosticResponse {
        provider_id: resolved_provider_id,
        is_default_selected,
        installed: summary.installed,
        installed_path: summary.installed_path,
        readiness,
        readiness_message: summary.readiness_message,
        source: "live_probe".to_string(),
        probed_at,
        handshake_probe,
        diagnostic: summary.diagnostic,
    }
}

async fn install_agent_handler(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<AgentInstallInput>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let result = adapter::install_agent(input.agent).await;
    Json(ApiResponse { data: result }).into_response()
}

async fn agent_summary(state: &Arc<AppState>, kind: AgentKind) -> AgentSummary {
    let (id, label, aliases, selectable, default_selected, compatible_formats, binary_name) =
        agent_descriptor(kind);
    if matches!(kind, AgentKind::Custom) {
        return AgentSummary {
            kind,
            id: id.to_string(),
            label: label.to_string(),
            aliases: aliases.into_iter().map(ToString::to_string).collect(),
            selectable,
            default_selected,
            compatible_formats,
            installed: false,
            installed_path: None,
            readiness: AgentReadiness::NotInstalled,
            readiness_message: None,
            acp_diagnostic: None,
            install_hint: "Custom agent does not support auto-install".to_string(),
        };
    }
    let (installed, installed_path, readiness_message, acp_diagnostic) =
        if matches!(kind, AgentKind::Acp) {
            let settings = state.bridge_settings().await;
            let summary = adapter::summarize_acp_agent(&settings).await;
            (
                summary.installed,
                summary.installed_path,
                summary.readiness_message,
                summary.diagnostic,
            )
        } else {
            let installed_path = adapter::find_executable_in_path(binary_name);
            let readiness_message = adapter::agent_readiness_message(kind).await;
            (
                installed_path.is_some(),
                installed_path.map(|p| p.display().to_string()),
                readiness_message,
                None,
            )
        };
    let readiness = if !installed {
        AgentReadiness::NotInstalled
    } else if readiness_message.is_some() {
        AgentReadiness::AttentionRequired
    } else {
        AgentReadiness::Ready
    };
    AgentSummary {
        kind,
        id: id.to_string(),
        label: label.to_string(),
        aliases: aliases.into_iter().map(ToString::to_string).collect(),
        selectable,
        default_selected,
        compatible_formats,
        installed,
        installed_path,
        readiness,
        readiness_message,
        acp_diagnostic,
        install_hint: adapter::manual_install_hint(kind),
    }
}

fn agent_descriptor(
    kind: AgentKind,
) -> (
    &'static str,
    &'static str,
    Vec<&'static str>,
    bool,
    bool,
    Vec<crate::models::ApiFormat>,
    &'static str,
) {
    match kind {
        AgentKind::Codex => (
            "codex",
            "Codex",
            vec!["codex"],
            true,
            true,
            vec![crate::models::ApiFormat::Codex],
            "codex",
        ),
        AgentKind::ClaudeCode => (
            "claude_code",
            "Claude Code",
            vec!["claude_code", "claudecode"],
            true,
            false,
            vec![crate::models::ApiFormat::AnthropicMessages],
            "claude",
        ),
        AgentKind::OpenCode => (
            "open_code",
            "OpenCode",
            vec!["open_code"],
            true,
            false,
            vec![
                crate::models::ApiFormat::OpenAiCompatible,
                crate::models::ApiFormat::AnthropicMessages,
                crate::models::ApiFormat::Codex,
            ],
            "opencode",
        ),
        AgentKind::Acp => (
            "acp",
            "ACP",
            vec!["acp"],
            true,
            false,
            vec![crate::models::ApiFormat::Acp],
            "",
        ),
        AgentKind::Custom => (
            "custom",
            "Agent",
            Vec::new(),
            false,
            false,
            vec![crate::models::ApiFormat::OpenAiCompatible],
            "",
        ),
    }
}

fn agent_commands_summary(kind: AgentKind) -> AgentCommandsSummary {
    let commands = match kind {
        AgentKind::Codex => vec![
            AgentCommandSummary {
                name: "/compact".to_string(),
                args_hint: None,
                description: "Compact current thread context".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/review".to_string(),
                args_hint: Some("[instructions]".to_string()),
                description: "Review uncommitted changes or run a custom review".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/rename".to_string(),
                args_hint: Some("<title>".to_string()),
                description: "Rename current session".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/goal".to_string(),
                args_hint: Some("<objective>".to_string()),
                description: "Set and run a goal with automatic continuation".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/clear-goal".to_string(),
                args_hint: None,
                description: "Clear the current thread goal".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/model".to_string(),
                args_hint: Some("<model-id>".to_string()),
                description: "Switch the active model for this thread".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
        ],
        AgentKind::ClaudeCode => vec![
            AgentCommandSummary {
                name: "/clear".to_string(),
                args_hint: None,
                description: "Clear the current Claude session context".to_string(),
                forwarding: AgentCommandForwarding::Wrapped,
            },
            AgentCommandSummary {
                name: "/skill-creator".to_string(),
                args_hint: Some("<task>".to_string()),
                description: "Invoke the Claude skill creator slash command".to_string(),
                forwarding: AgentCommandForwarding::Wrapped,
            },
        ],
        AgentKind::OpenCode => vec![
            AgentCommandSummary {
                name: "/clear".to_string(),
                args_hint: None,
                description: "Clear the current OpenCode session context".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/compact".to_string(),
                args_hint: None,
                description: "Compact the current OpenCode session context".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/undo".to_string(),
                args_hint: None,
                description: "Undo the last OpenCode edit".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/redo".to_string(),
                args_hint: None,
                description: "Redo the last undone OpenCode edit".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/init".to_string(),
                args_hint: None,
                description: "Initialize an AGENTS.md file for the current project".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/share".to_string(),
                args_hint: None,
                description: "Share the current OpenCode session".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/unshare".to_string(),
                args_hint: None,
                description: "Stop sharing the current OpenCode session".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/model".to_string(),
                args_hint: Some("<provider/model>".to_string()),
                description: "Switch the active model for this session".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/rename".to_string(),
                args_hint: Some("<title>".to_string()),
                description: "Rename current session".to_string(),
                forwarding: AgentCommandForwarding::Bridge,
            },
        ],
        AgentKind::Acp | AgentKind::Custom => Vec::new(),
    };
    AgentCommandsSummary { kind, commands }
}

async fn resolve_approval(
    Path((id, request_id)): Path<(String, String)>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<ApprovalDecisionInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    state
        .submit_approval(&id, &request_id, input.choice)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn session_events(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize_request_status(&headers, &state).await?;
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let subscription = state.subscribe_with_replay(last_event_id);
    let detail = state.get_session(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "session not found".to_string(),
    })?;

    let replay_session_id = id.clone();
    let initial_stream = match last_event_id {
        Some(_) => match subscription.replay {
            EventReplay::Events(events) => stream::iter(
                events
                    .into_iter()
                    .filter(move |event| event_belongs_to_session(&event.event, &replay_session_id))
                    .filter_map(|event| sse_event_for_session_event(&event)),
            )
            .boxed(),
            EventReplay::SyncRequired => stream::once(async { sync_required_event() }).boxed(),
        },
        None => {
            let initial_event = SessionEvent::SessionSnapshot(detail.session);
            let high_watermark = subscription.high_watermark;
            stream::once(
                async move { sse_event_for_initial_snapshot(&initial_event, high_watermark) },
            )
            .boxed()
        }
    };

    let high_watermark = subscription.high_watermark;
    let broadcast_stream = BroadcastStream::new(subscription.receiver).filter_map(move |item| {
        let session_id = id.clone();
        async move {
            match item {
                Ok(event)
                    if event.id > high_watermark
                        && event_belongs_to_session(&event.event, &session_id) =>
                {
                    sse_event_for_session_event(&event)
                }
                Err(_) => Some(sync_required_event()),
                _ => None,
            }
        }
    });
    let stream = initial_stream.chain(broadcast_stream);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn sse_event_for_initial_snapshot(
    event: &SessionEvent,
    event_id: u64,
) -> Result<Event, Infallible> {
    let (name, json) = encode_session_event(event).expect("session snapshot serializes");
    Ok(Event::default()
        .id(event_id.to_string())
        .event(name)
        .data(json))
}

fn sse_event_for_session_event(event: &SequencedSessionEvent) -> Option<Result<Event, Infallible>> {
    let (name, json) = encode_session_event(&event.event)?;
    Some(Ok(Event::default()
        .id(event.id.to_string())
        .event(name)
        .data(json)))
}

fn sync_required_event() -> Result<Event, Infallible> {
    Ok(Event::default()
        .event("sync.required")
        .data(r#"{"type":"sync_required","payload":{}}"#))
}

fn encode_session_event(event: &SessionEvent) -> Option<(&'static str, String)> {
    let json = serde_json::to_string(event).ok()?;
    Some((event_name(event), json))
}

async fn request_client_auth(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ClientAuthRequestInput>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .request_client_auth(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: record })))
}

async fn get_client_auth_request(
    Path(request_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .get_client_auth_request(&request_id)
        .await
        .ok_or(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "auth request not found".to_string(),
        })?;
    Ok(Json(ApiResponse { data: record }))
}

async fn authorize_request(headers: &HeaderMap, state: &AppState) -> Result<(), ApiError> {
    let client_id = read_client_id(headers)?;
    let runtime_allowed = state.is_runtime_client_id_allowed(client_id).await;
    if !runtime_allowed {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "client id is not allowed".to_string(),
        });
    }

    let actual = bearer_token(headers);
    let actual = actual.ok_or(ApiError {
        status: StatusCode::UNAUTHORIZED,
        message: "missing bearer token".to_string(),
    })?;
    if state.client_token_matches(client_id, actual).await {
        return Ok(());
    }

    Err(ApiError {
        status: StatusCode::FORBIDDEN,
        message: "invalid bearer token".to_string(),
    })
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn read_client_id(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get("x-omni-code-client-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing client id".to_string(),
        })
}

async fn authorize_request_status(headers: &HeaderMap, state: &AppState) -> Result<(), ApiError> {
    authorize_request(headers, state).await
}

fn event_belongs_to_session(event: &SessionEvent, session_id: &str) -> bool {
    match event {
        SessionEvent::SessionSnapshot(session) => session.id == session_id,
        SessionEvent::SessionStatus(status) => status.session_id == session_id,
        SessionEvent::MessageCreated(message) => message.session_id == session_id,
        SessionEvent::MessageDelta(delta) => delta.session_id == session_id,
        SessionEvent::MessageSnapshot(snapshot) => snapshot.session_id == session_id,
        SessionEvent::AgentError(error) => error.session_id == session_id,
        SessionEvent::ApprovalRequested(event) => event.session_id == session_id,
        SessionEvent::ApprovalResolved(event) => event.session_id == session_id,
    }
}

fn event_name(event: &SessionEvent) -> &'static str {
    match event {
        SessionEvent::SessionSnapshot(_) => "session.snapshot",
        SessionEvent::SessionStatus(_) => "session.status",
        SessionEvent::MessageCreated(_) => "message.created",
        SessionEvent::MessageDelta(_) => "message.delta",
        SessionEvent::MessageSnapshot(_) => "message.snapshot",
        SessionEvent::AgentError(_) => "agent.error",
        SessionEvent::ApprovalRequested(_) => "approval.requested",
        SessionEvent::ApprovalResolved(_) => "approval.resolved",
    }
}

async fn resolve_authorized_file_path(
    state: &AppState,
    query: &FileQuery,
) -> Result<PathBuf, ApiError> {
    let requested_path = query.path.trim();
    if requested_path.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "path is required".to_string()).into());
    }

    let project_id = query
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let session_id = query
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if project_id.is_some() && session_id.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "project_id and session_id cannot be used together".to_string(),
        )
            .into());
    }

    let requested_path_buf = PathBuf::from(requested_path);
    if let Some(session_id) = session_id {
        let project_root = state
            .project_root_path_for_session(session_id)
            .await
            .map_err(|error| (StatusCode::NOT_FOUND, error))?;
        let project_root = canonicalize_local_directory(&project_root).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("session {session_id} is not backed by a local project directory"),
            )
        })?;
        return resolve_path_within_root(&project_root, &requested_path_buf)
            .map_err(|error| map_file_resolution_error(requested_path, error));
    }

    if let Some(project_id) = project_id {
        let project_root = state
            .list_projects()
            .await
            .into_iter()
            .find(|project| project.id == project_id)
            .ok_or((
                StatusCode::NOT_FOUND,
                format!("unknown project: {project_id}"),
            ))?
            .root_path;
        let project_root = canonicalize_local_directory(&project_root).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("project {project_id} is not backed by a local directory"),
            )
        })?;
        return resolve_path_within_root(&project_root, &requested_path_buf)
            .map_err(|error| map_file_resolution_error(requested_path, error));
    }

    if requested_path_buf.is_absolute() {
        return canonicalize_existing_file(&requested_path_buf)
            .map_err(|error| map_file_resolution_error(requested_path, error));
    }

    Err((
        StatusCode::BAD_REQUEST,
        "relative path requires project_id or session_id".to_string(),
    )
        .into())
}

async fn resolve_completion_root(
    state: &AppState,
    query: &FileCompletionQuery,
) -> Result<PathBuf, ApiError> {
    let project_id = query
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let session_id = query
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if project_id.is_some() && session_id.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "project_id and session_id cannot be used together".to_string(),
        )
            .into());
    }

    if let Some(session_id) = session_id {
        let project_root = state
            .project_root_path_for_session(session_id)
            .await
            .map_err(|error| (StatusCode::NOT_FOUND, error))?;
        return canonicalize_local_directory(&project_root).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("session {session_id} is not backed by a local project directory"),
            )
                .into()
        });
    }

    if let Some(project_id) = project_id {
        let project = state
            .list_projects()
            .await
            .into_iter()
            .find(|project| project.id == project_id)
            .ok_or(ApiError {
                status: StatusCode::NOT_FOUND,
                message: format!("unknown project: {project_id}"),
            })?;
        return canonicalize_local_directory(&project.root_path).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("project {project_id} is not backed by a local directory"),
            )
                .into()
        });
    }

    Err((
        StatusCode::BAD_REQUEST,
        "file completion requires project_id or session_id".to_string(),
    )
        .into())
}

fn list_completion_items(
    root: &StdPath,
    query: &FileCompletionQuery,
) -> Result<Vec<FileCompletionItem>, ApiError> {
    let prefix = query.prefix.trim();
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let normalized_prefix = normalize_completion_prefix(prefix)?;
    let (search_dir, file_prefix) = completion_search_scope(root, &normalized_prefix)?;

    let entries = std::fs::read_dir(&search_dir).map_err(map_completion_dir_error)?;
    let mut items = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let file_name = entry.file_name();
            let file_name = file_name.to_str()?;
            if !file_name.starts_with(file_prefix) {
                return None;
            }
            let path = entry.path();
            let is_dir = path.is_dir();
            let relative = path
                .strip_prefix(root)
                .ok()?
                .to_string_lossy()
                .replace('\\', "/");
            let path = if is_dir {
                format!("{relative}/")
            } else {
                relative
            };
            Some(FileCompletionItem { path, is_dir })
        })
        .collect::<Vec<_>>();

    items.sort_by(|a, b| a.path.cmp(&b.path));
    items.truncate(limit);
    Ok(items)
}

fn normalize_completion_prefix(prefix: &str) -> Result<String, ApiError> {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return Ok(String::new());
    }
    if prefix.starts_with('/') {
        return Err((
            StatusCode::BAD_REQUEST,
            "absolute prefix is not supported for file completion".to_string(),
        )
            .into());
    }

    let mut normalized = String::new();
    for component in StdPath::new(prefix).components() {
        match component {
            std::path::Component::Normal(segment) => {
                if !normalized.is_empty() {
                    normalized.push('/');
                }
                normalized.push_str(&segment.to_string_lossy());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "prefix cannot traverse outside the project root".to_string(),
                )
                    .into());
            }
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid prefix for file completion".to_string(),
                )
                    .into());
            }
        }
    }

    if prefix.ends_with('/') && !normalized.is_empty() {
        normalized.push('/');
    }
    Ok(normalized)
}

fn completion_search_scope<'a>(
    root: &'a StdPath,
    prefix: &'a str,
) -> Result<(PathBuf, &'a str), ApiError> {
    let (dir_part, file_prefix) = match prefix.rsplit_once('/') {
        Some((dir, tail)) => (dir, tail),
        None => ("", prefix),
    };
    let search_dir = if dir_part.is_empty() {
        root.to_path_buf()
    } else {
        resolve_directory_within_root(root, StdPath::new(dir_part))
            .map_err(map_completion_dir_error)?
    };
    Ok((search_dir, file_prefix))
}

fn resolve_directory_within_root(root: &StdPath, requested_path: &StdPath) -> io::Result<PathBuf> {
    let candidate = root.join(requested_path);
    let candidate = std::fs::canonicalize(candidate)?;
    if !candidate.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }
    if candidate.starts_with(root) {
        return Ok(candidate);
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "path is outside allowed project roots",
    ))
}

fn map_completion_dir_error(error: io::Error) -> ApiError {
    let (status, message) = match error.kind() {
        io::ErrorKind::NotFound => (
            StatusCode::NOT_FOUND,
            "completion directory not found".to_string(),
        ),
        io::ErrorKind::PermissionDenied => (
            StatusCode::FORBIDDEN,
            "completion path is outside allowed project roots".to_string(),
        ),
        io::ErrorKind::InvalidInput => (
            StatusCode::BAD_REQUEST,
            "completion path does not point to a directory".to_string(),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to enumerate files: {error}"),
        ),
    };
    ApiError { status, message }
}

fn canonicalize_local_directory(path: impl AsRef<StdPath>) -> io::Result<PathBuf> {
    let path = std::fs::canonicalize(path)?;
    if !path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }
    Ok(path)
}

fn canonicalize_existing_file(path: impl AsRef<StdPath>) -> io::Result<PathBuf> {
    let path = std::fs::canonicalize(path)?;
    if !path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a file",
        ));
    }
    Ok(path)
}

fn resolve_path_within_root(root: &StdPath, requested_path: &StdPath) -> io::Result<PathBuf> {
    let candidate = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        root.join(requested_path)
    };
    let candidate = canonicalize_existing_file(candidate)?;
    if candidate.starts_with(root) {
        return Ok(candidate);
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "path is outside allowed project roots",
    ))
}

fn map_file_resolution_error(requested_path: &str, error: io::Error) -> ApiError {
    let (status, message) = match error.kind() {
        io::ErrorKind::NotFound => (
            StatusCode::NOT_FOUND,
            format!("file not found: {requested_path}"),
        ),
        io::ErrorKind::PermissionDenied => (
            StatusCode::FORBIDDEN,
            format!("path is outside allowed project roots: {requested_path}"),
        ),
        io::ErrorKind::InvalidInput => (
            StatusCode::BAD_REQUEST,
            format!("path does not point to a file: {requested_path}"),
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    ApiError { status, message }
}

fn content_type_for_path(path: &StdPath) -> &'static str {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());

    match extension.as_deref() {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("bmp") => "image/bmp",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("avif") => "image/avif",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("ogg") => "audio/ogg",
        Some("m4a") => "audio/mp4",
        Some("flac") => "audio/flac",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mov") => "video/quicktime",
        Some("pdf") => "application/pdf",
        Some("json") => "application/json; charset=utf-8",
        Some("toml") => "application/toml; charset=utf-8",
        Some("xml") => "application/xml; charset=utf-8",
        Some("js") | Some("mjs") | Some("cjs") => "application/javascript; charset=utf-8",
        Some("zip") => "application/zip",
        Some("gz") => "application/gzip",
        Some("tar") => "application/x-tar",
        Some("md") => "text/markdown; charset=utf-8",
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("csv") => "text/csv; charset=utf-8",
        Some("yaml") | Some("yml") => "text/yaml; charset=utf-8",
        Some("txt") | Some("rs") | Some("ts") | Some("tsx") | Some("jsx") | Some("py")
        | Some("java") | Some("kt") | Some("swift") | Some("go") | Some("c") | Some("cc")
        | Some("cpp") | Some("h") | Some("hpp") | Some("sh") | Some("bash") | Some("zsh")
        | Some("fish") | Some("sql") | Some("log") | Some("dart") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn content_type_for_upload_name(file_name: &str) -> &'static str {
    content_type_for_path(StdPath::new(file_name))
}

fn uploads_dir() -> PathBuf {
    crate::bridge_settings::settings_path()
        .parent()
        .map(StdPath::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(".omni-code"))
        .join("uploads")
}

fn absolute_url_from_headers(headers: &HeaderMap, path: &str) -> String {
    let forwarded = headers
        .get("forwarded")
        .and_then(|value| value.to_str().ok());
    let forwarded_proto = forwarded
        .and_then(|value| forwarded_param(value, "proto"))
        .or_else(|| {
            headers
                .get("x-forwarded-proto")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "http".to_string());
    let forwarded_host = forwarded
        .and_then(|value| forwarded_param(value, "host"))
        .or_else(|| {
            headers
                .get("x-forwarded-host")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "127.0.0.1:8787".to_string());

    format!("{forwarded_proto}://{forwarded_host}{path}")
}

fn forwarded_param(header_value: &str, key: &str) -> Option<String> {
    header_value
        .split(',')
        .next()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| {
            if name.trim().eq_ignore_ascii_case(key) {
                let value = value.trim().trim_matches('"');
                (!value.is_empty()).then(|| value.to_string())
            } else {
                None
            }
        })
}

fn sanitize_upload_file_name(file_name: &str) -> String {
    let base = StdPath::new(file_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("upload.bin");
    let sanitized = base
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('.')
        .trim_matches('_')
        .to_string();
    if sanitized.is_empty() {
        "upload.bin".to_string()
    } else {
        sanitized
    }
}

fn sanitize_upload_lookup_id(id: &str) -> Option<String> {
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || !id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
    {
        return None;
    }
    Some(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        AcpDiagnosticQuery, absolute_url_from_headers, agent_commands_summary, agent_summary,
        completion_search_scope, content_type_for_path, content_type_for_upload_name,
        encode_session_event, get_acp_agent_diagnostic, list_completion_items,
        normalize_completion_prefix, paginate_messages, resolve_path_within_root,
        sanitize_upload_file_name, sanitize_upload_lookup_id, uploads_dir,
    };
    use crate::{
        app_state::AppState,
        bridge_settings::{AiApprovalSettings, BridgeSettingsInput},
        models::{
            AcpProfile, AcpServerConfig, AgentKind, AgentReadiness, ChatMessage,
            ClientAuthRequestInput, FileCompletionQuery, HeaderKeyValue, MessageListQuery,
            MessageRole, SessionEvent,
        },
    };
    use axum::{
        Json,
        body::to_bytes,
        extract::{Query, State},
        http::{HeaderMap, StatusCode, header},
        response::IntoResponse,
        routing::{get, post},
    };
    use serde_json::Value;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn test_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-api-{prefix}-{unique}.json"))
    }

    async fn test_state(prefix: &str) -> Arc<AppState> {
        let settings_path = test_path(&format!("{prefix}-settings"));
        let runtime_path = test_path(&format!("{prefix}-runtime"));
        let metadata_path = test_path(&format!("{prefix}-metadata"));
        Arc::new(AppState::new_with_paths(settings_path, runtime_path, metadata_path).await)
    }

    async fn authorized_headers(state: &Arc<AppState>) -> HeaderMap {
        let record = state
            .request_client_auth(ClientAuthRequestInput {
                client_id: "test-client".to_string(),
                device_name: Some("API tests".to_string()),
            })
            .await
            .expect("client auth request should succeed");
        let token = if let Some(token) = record.token.clone() {
            token
        } else {
            state
                .approve_client_auth_for_test(&record.request_id)
                .await
                .expect("client auth approval should succeed")
                .token
                .unwrap_or_default()
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-omni-code-client-id", "test-client".parse().unwrap());
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    fn mock_kiro_acp_script_path() -> PathBuf {
        let path = std::env::temp_dir().join("omni-code-bridge-api-mock-kiro-acp.py");
        std::fs::write(
            &path,
            r#"#!/usr/bin/env python3
import json
import sys

scenario = sys.argv[1] if len(sys.argv) > 1 else "probe-success"

def write(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    value = json.loads(line)
    request_id = value.get("id")
    method = value.get("method", "")

    if method == "initialize":
        write({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": 1, "serverInfo": {"name": "mock-kiro-acp", "version": "test"}}})
    elif method == "session/new":
        write({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "kiro-probe-session"}})
    elif method == "session/prompt" and scenario == "probe-success":
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "probe ok"}}})
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "TurnEnd", "status": "completed"}})
        write({"jsonrpc": "2.0", "id": request_id, "result": {}})
        sys.exit(0)
    else:
        write({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"unsupported mock method: {method}"}})
"#,
        )
        .expect("mock Kiro ACP script should be written");
        path
    }

    #[test]
    fn resolves_file_inside_root() {
        let root = test_dir("file-api-inside");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let file_path = nested.join("note.txt");
        fs::write(&file_path, "hello").unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let resolved =
            resolve_path_within_root(&canonical_root, Path::new("nested/note.txt")).unwrap();

        assert_eq!(resolved, fs::canonicalize(file_path).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn blocks_parent_path_escape() {
        let base = test_dir("file-api-scope");
        let root = base.join("project");
        fs::create_dir_all(&root).unwrap();
        let outside = base.join("secret.txt");
        fs::write(&outside, "secret").unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let error =
            resolve_path_within_root(&canonical_root, Path::new("../secret.txt")).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn file_completion_normalizes_relative_prefix() {
        assert_eq!(normalize_completion_prefix(" src/api").unwrap(), "src/api");
        assert_eq!(
            normalize_completion_prefix("./src/api.rs").unwrap(),
            "src/api.rs"
        );
        assert_eq!(normalize_completion_prefix("src/").unwrap(), "src/");
    }

    #[test]
    fn file_completion_rejects_absolute_and_parent_prefixes() {
        let absolute = normalize_completion_prefix("/tmp").unwrap_err();
        assert_eq!(absolute.status, StatusCode::BAD_REQUEST);

        let parent = normalize_completion_prefix("../secret").unwrap_err();
        assert_eq!(parent.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn file_completion_search_scope_uses_prefix_parent_directory() {
        let root = test_dir("file-api-completion-scope");
        let src = root.join("src");
        fs::create_dir_all(&src).unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let (search_dir, file_prefix) = completion_search_scope(&canonical_root, "src/ap").unwrap();

        assert_eq!(search_dir, fs::canonicalize(src).unwrap());
        assert_eq!(file_prefix, "ap");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn file_completion_lists_matching_files_and_directories() {
        let root = test_dir("file-api-completion-list");
        let src = root.join("src");
        let docs = root.join("docs");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&docs).unwrap();
        fs::write(src.join("api.rs"), "").unwrap();
        fs::write(src.join("app.rs"), "").unwrap();
        fs::write(src.join("main.rs"), "").unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let items = list_completion_items(
            &canonical_root,
            &FileCompletionQuery {
                prefix: "src/ap".to_string(),
                project_id: None,
                session_id: None,
                limit: Some(10),
            },
        )
        .unwrap();

        let paths = items
            .iter()
            .map(|item| item.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["src/api.rs", "src/app.rs"]);
        assert!(items.iter().all(|item| !item.is_dir));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn file_completion_appends_slash_for_directories() {
        let root = test_dir("file-api-completion-dir");
        let src = root.join("src");
        fs::create_dir_all(src.join("api")).unwrap();
        fs::write(src.join("app.rs"), "").unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let items = list_completion_items(
            &canonical_root,
            &FileCompletionQuery {
                prefix: "src/ap".to_string(),
                project_id: None,
                session_id: None,
                limit: Some(10),
            },
        )
        .unwrap();

        let paths = items
            .iter()
            .map(|item| item.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["src/api/", "src/app.rs"]);
        assert!(
            items
                .iter()
                .any(|item| item.is_dir && item.path == "src/api/")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_text_and_image_content_types() {
        assert_eq!(
            content_type_for_path(Path::new("/tmp/readme.md")),
            "text/markdown; charset=utf-8"
        );
        assert_eq!(
            content_type_for_path(Path::new("/tmp/image.png")),
            "image/png"
        );
        assert_eq!(
            content_type_for_path(Path::new("/tmp/archive.bin")),
            "application/octet-stream"
        );
        assert_eq!(content_type_for_upload_name("photo.webp"), "image/webp");
    }

    #[test]
    fn upload_file_names_are_sanitized() {
        assert_eq!(
            sanitize_upload_file_name("../unsafe photo.png"),
            "unsafe_photo.png"
        );
        assert_eq!(sanitize_upload_file_name("..."), "upload.bin");
        assert_eq!(
            sanitize_upload_lookup_id("abc-123_file.png").as_deref(),
            Some("abc-123_file.png")
        );
        assert!(sanitize_upload_lookup_id("../secret").is_none());
        assert!(sanitize_upload_lookup_id("nested/file.png").is_none());
    }

    #[test]
    fn upload_urls_and_directory_are_stable() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "127.0.0.1:8787".parse().unwrap());
        assert_eq!(
            absolute_url_from_headers(&headers, "/uploads/file.png"),
            "http://127.0.0.1:8787/uploads/file.png"
        );
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "bridge.example.com".parse().unwrap());
        assert_eq!(
            absolute_url_from_headers(&headers, "/uploads/file.png"),
            "https://bridge.example.com/uploads/file.png"
        );
        assert_eq!(
            uploads_dir().file_name().and_then(|value| value.to_str()),
            Some("uploads")
        );
        let stored_path = uploads_dir().join("file.png");
        assert!(stored_path.ends_with("uploads/file.png"));
    }

    #[test]
    fn session_snapshot_can_be_encoded_as_sse_initial_event() {
        let event = SessionEvent::SessionSnapshot(crate::models::SessionSummary {
            id: "session-1".to_string(),
            project_id: "project-1".to_string(),
            title: "Existing session".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: crate::models::SessionStatus::Interrupted,
            updated_at: chrono::Utc::now(),
            unread_count: 0,
            last_message_preview: Some("hello".to_string()),
            pending_approval: None,
            runtime_session_ref: Some("thread-1".to_string()),
            provider_id: None,
            reasoning_effort: None,
            model: None,
        });

        let (name, body) = encode_session_event(&event).expect("snapshot should encode");

        assert_eq!(name, "session.snapshot");
        assert!(body.contains("\"type\":\"session_snapshot\""));
        assert!(body.contains("\"id\":\"session-1\""));
    }

    #[test]
    fn agent_commands_summary_exposes_supported_slash_commands() {
        let codex = agent_commands_summary(AgentKind::Codex);
        assert!(
            codex
                .commands
                .iter()
                .any(|command| command.name == "/compact")
        );
        assert!(codex.commands.iter().any(|command| command.name == "/goal"));
        assert!(
            codex
                .commands
                .iter()
                .any(|command| command.name == "/model")
        );

        let claude = agent_commands_summary(AgentKind::ClaudeCode);
        assert!(
            claude
                .commands
                .iter()
                .any(|command| command.name == "/clear")
        );

        let opencode = agent_commands_summary(AgentKind::OpenCode);
        assert!(
            opencode
                .commands
                .iter()
                .any(|command| command.name == "/clear")
        );
        assert!(
            opencode
                .commands
                .iter()
                .any(|command| command.name == "/compact")
        );
        assert!(
            opencode
                .commands
                .iter()
                .any(|command| command.name == "/rename")
        );
    }

    fn test_message(id: &str, role: MessageRole, content: &str) -> ChatMessage {
        ChatMessage {
            id: id.to_string(),
            session_id: "session".to_string(),
            role,
            content: content.to_string(),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn list_messages_supports_limit_and_cursors() {
        let messages = vec![
            test_message("first", MessageRole::User, "first"),
            test_message("second", MessageRole::Assistant, "second"),
            test_message("third", MessageRole::User, "third"),
        ];

        let page = paginate_messages(
            messages.clone(),
            MessageListQuery {
                limit: Some(2),
                before_id: None,
                after_id: None,
            },
        )
        .expect("request should succeed");
        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second", "third"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("second"));

        let page = paginate_messages(
            messages.clone(),
            MessageListQuery {
                limit: Some(2),
                before_id: Some("third".to_string()),
                after_id: None,
            },
        )
        .expect("before_id request should succeed");
        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert!(!page.has_more);
        assert_eq!(page.next_cursor, None);

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(1),
                before_id: None,
                after_id: Some("first".to_string()),
            },
        )
        .expect("after_id request should succeed");
        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("second"));
    }

    #[test]
    fn list_messages_default_page_includes_active_reply() {
        let messages = vec![
            test_message("old-user", MessageRole::User, "old"),
            test_message("new-user", MessageRole::User, "new"),
            test_message("active-reply", MessageRole::Assistant, "partial reply"),
        ];

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(2),
                before_id: None,
                after_id: None,
            },
        )
        .expect("request should succeed");

        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-user", "active-reply"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("new-user"));
        assert_eq!(page.messages[1].content, "partial reply");
    }

    #[test]
    fn list_messages_before_id_returns_adjacent_previous_page() {
        let messages = vec![
            test_message("first", MessageRole::User, "first"),
            test_message("second", MessageRole::Assistant, "second"),
            test_message("third", MessageRole::User, "third"),
            test_message("fourth", MessageRole::Assistant, "fourth"),
            test_message("fifth", MessageRole::User, "fifth"),
        ];

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(2),
                before_id: Some("fifth".to_string()),
                after_id: None,
            },
        )
        .expect("before_id request should succeed");

        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["third", "fourth"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("third"));
    }

    #[test]
    fn list_messages_rejects_invalid_cursor_combinations() {
        let messages = vec![test_message("only", MessageRole::User, "only")];

        let error = paginate_messages(
            messages.clone(),
            MessageListQuery {
                limit: Some(10),
                before_id: Some("a".to_string()),
                after_id: Some("b".to_string()),
            },
        )
        .expect_err("conflicting cursors should fail");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("cannot be used together"));

        let error = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(10),
                before_id: Some("missing".to_string()),
                after_id: None,
            },
        )
        .expect_err("unknown before_id should fail");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("unknown before_id"));
    }

    #[test]
    fn list_messages_limit_counts_agent_reply_segment_as_one_message() {
        let messages = vec![
            test_message("user-1", MessageRole::User, "u1"),
            test_message("assistant-1", MessageRole::Assistant, "a1"),
            test_message("system-1", MessageRole::System, "s1"),
            test_message("system-2", MessageRole::System, "s2"),
            test_message("user-2", MessageRole::User, "u2"),
            test_message("assistant-2", MessageRole::Assistant, "a2"),
        ];

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(2),
                before_id: None,
                after_id: None,
            },
        )
        .expect("request should succeed");

        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["user-2", "assistant-2"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("user-2"));
    }

    #[test]
    fn list_messages_limit_keeps_system_messages_in_agent_reply_segment() {
        let messages = vec![
            test_message("user-1", MessageRole::User, "u1"),
            test_message("assistant-1", MessageRole::Assistant, "a1"),
            test_message("user-2", MessageRole::User, "u2"),
            test_message("assistant-2", MessageRole::Assistant, "a2"),
            test_message("system-1", MessageRole::System, "s1"),
            test_message("system-2", MessageRole::System, "s2"),
        ];

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(2),
                before_id: None,
                after_id: None,
            },
        )
        .expect("request should keep trailing system messages");

        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["user-2", "assistant-2", "system-1", "system-2"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("user-2"));
    }

    #[test]
    fn list_messages_before_id_inside_agent_reply_does_not_split_segment() {
        let messages = vec![
            test_message("user-1", MessageRole::User, "u1"),
            test_message("assistant-1", MessageRole::Assistant, "a1"),
            test_message("system-1", MessageRole::System, "s1"),
            test_message("system-2", MessageRole::System, "s2"),
            test_message("user-2", MessageRole::User, "u2"),
            test_message("assistant-2", MessageRole::Assistant, "a2"),
            test_message("system-3", MessageRole::System, "s3"),
            test_message("system-4", MessageRole::System, "s4"),
            test_message("assistant-3", MessageRole::Assistant, "a3"),
            test_message("system-5", MessageRole::System, "s5"),
        ];

        let page = paginate_messages(
            messages,
            MessageListQuery {
                limit: Some(2),
                before_id: Some("system-5".to_string()),
                after_id: None,
            },
        )
        .expect("before_id should not split agent reply segments");

        assert_eq!(
            page.messages
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["assistant-1", "system-1", "system-2", "user-2"]
        );
        assert!(page.has_more);
        assert_eq!(page.next_cursor.as_deref(), Some("assistant-1"));
    }

    #[tokio::test]
    async fn agent_summary_exposes_descriptor_metadata() {
        let state = test_state("agent-summary-metadata").await;
        let codex = agent_summary(&state, AgentKind::Codex).await;
        assert_eq!(codex.id, "codex");
        assert_eq!(codex.label, "Codex");
        assert!(codex.default_selected);
        assert_eq!(codex.compatible_formats.len(), 1);
        assert!(matches!(
            codex.readiness,
            AgentReadiness::Ready
                | AgentReadiness::AttentionRequired
                | AgentReadiness::NotInstalled
        ));

        let claude = agent_summary(&state, AgentKind::ClaudeCode).await;
        assert!(claude.aliases.contains(&"claudecode".to_string()));
        assert!(!claude.default_selected);

        let acp = agent_summary(&state, AgentKind::Acp).await;
        assert_eq!(acp.id, "acp");
        assert_eq!(acp.compatible_formats.len(), 1);
        assert!(matches!(
            acp.readiness,
            AgentReadiness::Ready
                | AgentReadiness::AttentionRequired
                | AgentReadiness::NotInstalled
        ));
        assert!(acp.acp_diagnostic.is_none());

        let custom = agent_summary(&state, AgentKind::Custom).await;
        assert!(!custom.selectable);
        assert!(matches!(custom.readiness, AgentReadiness::NotInstalled));
        assert!(custom.acp_diagnostic.is_none());
    }

    #[tokio::test]
    async fn acp_agent_summary_reports_unconfigured_when_no_enabled_servers_exist() {
        let state = test_state("acp-summary-unconfigured").await;

        let acp = agent_summary(&state, AgentKind::Acp).await;

        assert!(matches!(acp.readiness, AgentReadiness::NotInstalled));
        assert!(!acp.installed);
        assert!(acp.installed_path.is_none());
        assert!(
            acp.readiness_message
                .as_deref()
                .unwrap_or_default()
                .contains("no enabled ACP servers")
        );
    }

    #[tokio::test]
    async fn acp_agent_summary_uses_enabled_generic_http_server() {
        let state = test_state("acp-summary-generic-http").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-acp".to_string(),
                    name: "Generic ACP".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some("https://acp.example.test".to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let acp = agent_summary(&state, AgentKind::Acp).await;

        assert!(matches!(acp.readiness, AgentReadiness::Ready));
        assert!(acp.installed);
        assert!(acp.installed_path.is_none());
        assert!(acp.readiness_message.is_none());
        let diagnostic = acp.acp_diagnostic.expect("diagnostic should exist");
        assert_eq!(diagnostic.configured_server_id, "generic-acp");
        assert_eq!(diagnostic.configured_server_name, "Generic ACP");
        assert!(matches!(diagnostic.profile, AcpProfile::GenericHttp));
        assert_eq!(
            diagnostic.endpoint.as_deref(),
            Some("https://acp.example.test")
        );
        assert!(!diagnostic.auth_configured);
        assert_eq!(diagnostic.default_model, None);
        assert_eq!(diagnostic.header_count, 0);
        assert_eq!(diagnostic.env_count, 0);
        assert_eq!(
            diagnostic.turn_url_candidates,
            vec![
                "https://acp.example.test/turns".to_string(),
                "https://acp.example.test/turn".to_string(),
                "https://acp.example.test/sessions/{session_id}/turns".to_string(),
                "https://acp.example.test".to_string(),
            ]
        );
        assert_eq!(diagnostic.approval_reply_url_templates.len(), 6);
        assert_eq!(diagnostic.cancel_url_templates.len(), 5);
        assert_eq!(diagnostic.enabled_server_count, 1);
    }

    #[tokio::test]
    async fn acp_agent_summary_prefers_highest_priority_enabled_server() {
        let state = test_state("acp-summary-priority").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![
                    AcpServerConfig {
                        id: "kiro-primary".to_string(),
                        name: "Kiro Primary".to_string(),
                        profile: AcpProfile::Stdio,
                        endpoint: None,
                        command: Some("/definitely/missing/kiro-cli".to_string()),
                        args: vec!["acp".to_string()],
                        auth_token: String::new(),
                        default_model: None,
                        enabled: true,
                        priority: 0,
                        headers: Vec::new(),
                        env: Vec::new(),
                    },
                    AcpServerConfig {
                        id: "generic-fallback".to_string(),
                        name: "Generic Fallback".to_string(),
                        profile: AcpProfile::GenericHttp,
                        endpoint: Some("https://acp.example.test".to_string()),
                        command: None,
                        args: Vec::new(),
                        auth_token: String::new(),
                        default_model: None,
                        enabled: true,
                        priority: 10,
                        headers: vec![HeaderKeyValue {
                            key: "Authorization".to_string(),
                            value: "Bearer token".to_string(),
                        }],
                        env: Vec::new(),
                    },
                ]),
            })
            .await
            .expect("settings update should succeed");

        let acp = agent_summary(&state, AgentKind::Acp).await;

        assert!(matches!(acp.readiness, AgentReadiness::NotInstalled));
        assert!(!acp.installed);
        assert!(acp.installed_path.is_none());
        assert!(
            acp.readiness_message
                .as_deref()
                .unwrap_or_default()
                .contains("kiro-primary")
        );
        let diagnostic = acp.acp_diagnostic.expect("diagnostic should exist");
        assert_eq!(diagnostic.configured_server_id, "kiro-primary");
        assert_eq!(diagnostic.configured_server_name, "Kiro Primary");
        assert!(matches!(diagnostic.profile, AcpProfile::Stdio));
        assert_eq!(
            diagnostic.command.as_deref(),
            Some("/definitely/missing/kiro-cli")
        );
        assert_eq!(diagnostic.args, vec!["acp".to_string()]);
        assert!(!diagnostic.auth_configured);
        assert_eq!(diagnostic.default_model, None);
        assert_eq!(diagnostic.header_count, 0);
        assert_eq!(diagnostic.env_count, 0);
        assert_eq!(diagnostic.enabled_server_count, 2);
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_returns_selected_server_details() {
        let state = test_state("acp-diagnostic-endpoint").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-acp".to_string(),
                    name: "Generic ACP".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some("https://acp.example.test".to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response =
            get_acp_agent_diagnostic(headers, Query(AcpDiagnosticQuery::default()), State(state))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["provider_id"],
            Value::String("generic-acp".to_string())
        );
        assert_eq!(json["data"]["is_default_selected"], Value::Bool(true));
        assert_eq!(json["data"]["installed"], Value::Bool(true));
        assert_eq!(
            json["data"]["readiness"],
            Value::String("ready".to_string())
        );
        assert!(json["data"]["readiness_message"].is_null());
        assert_eq!(
            json["data"]["source"],
            Value::String("live_probe".to_string())
        );
        assert!(json["data"]["probed_at"].as_str().is_some());
        assert_eq!(
            json["data"]["diagnostic"]["configured_server_id"],
            Value::String("generic-acp".to_string())
        );
        assert_eq!(json["data"]["diagnostic"]["enabled"], Value::Bool(true));
        assert_eq!(
            json["data"]["diagnostic"]["endpoint"],
            Value::String("https://acp.example.test".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["auth_configured"],
            Value::Bool(false)
        );
        assert_eq!(json["data"]["diagnostic"]["default_model"], Value::Null);
        assert_eq!(
            json["data"]["diagnostic"]["header_count"],
            Value::Number(0.into())
        );
        assert_eq!(
            json["data"]["diagnostic"]["env_count"],
            Value::Number(0.into())
        );
        assert_eq!(
            json["data"]["diagnostic"]["turn_url_candidates"][0],
            Value::String("https://acp.example.test/turns".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["approval_reply_url_templates"][0],
            Value::String("https://acp.example.test/approvals/{request_id}/reply".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["cancel_url_templates"][0],
            Value::String("https://acp.example.test/sessions/{session_ref}/cancel".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["enabled_server_count"],
            Value::Number(1.into())
        );
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_can_target_disabled_server_by_provider_id() {
        let state = test_state("acp-diagnostic-provider-id").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![
                    AcpServerConfig {
                        id: "generic-primary".to_string(),
                        name: "Generic Primary".to_string(),
                        profile: AcpProfile::GenericHttp,
                        endpoint: Some("https://acp-primary.example.test".to_string()),
                        command: None,
                        args: Vec::new(),
                        auth_token: "secret-token".to_string(),
                        default_model: None,
                        enabled: true,
                        priority: 0,
                        headers: vec![HeaderKeyValue {
                            key: "X-ACP-Client".to_string(),
                            value: "omni-code-bridge".to_string(),
                        }],
                        env: Vec::new(),
                    },
                    AcpServerConfig {
                        id: "generic-disabled".to_string(),
                        name: "Generic Disabled".to_string(),
                        profile: AcpProfile::GenericHttp,
                        endpoint: Some("https://acp-disabled.example.test".to_string()),
                        command: None,
                        args: Vec::new(),
                        auth_token: String::new(),
                        default_model: None,
                        enabled: false,
                        priority: 100,
                        headers: Vec::new(),
                        env: Vec::new(),
                    },
                ]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: Some("generic-disabled".to_string()),
                all: false,
                probe: false,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["provider_id"],
            Value::String("generic-disabled".to_string())
        );
        assert_eq!(json["data"]["is_default_selected"], Value::Bool(false));
        assert_eq!(
            json["data"]["diagnostic"]["configured_server_id"],
            Value::String("generic-disabled".to_string())
        );
        assert_eq!(json["data"]["diagnostic"]["enabled"], Value::Bool(false));
        assert_eq!(
            json["data"]["diagnostic"]["endpoint"],
            Value::String("https://acp-disabled.example.test".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["auth_configured"],
            Value::Bool(false)
        );
        assert_eq!(
            json["data"]["diagnostic"]["header_count"],
            Value::Number(0.into())
        );
        assert_eq!(
            json["data"]["readiness"],
            Value::String("attention_required".to_string())
        );
        assert!(
            json["data"]["readiness_message"]
                .as_str()
                .unwrap_or_default()
                .contains("disabled in settings")
        );
        assert_eq!(
            json["data"]["diagnostic"]["enabled_server_count"],
            Value::Number(1.into())
        );
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_rejects_unknown_provider_id() {
        let state = test_state("acp-diagnostic-missing-provider").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-primary".to_string(),
                    name: "Generic Primary".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some("https://acp-primary.example.test".to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: Some("missing-provider".to_string()),
                all: false,
                probe: false,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["data"]["error"]
                .as_str()
                .unwrap_or_default()
                .contains("missing-provider")
        );
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_can_return_all_servers() {
        let state = test_state("acp-diagnostic-all").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![
                    AcpServerConfig {
                        id: "generic-primary".to_string(),
                        name: "Generic Primary".to_string(),
                        profile: AcpProfile::GenericHttp,
                        endpoint: Some("https://acp-primary.example.test".to_string()),
                        command: None,
                        args: Vec::new(),
                        auth_token: String::new(),
                        default_model: None,
                        enabled: true,
                        priority: 0,
                        headers: Vec::new(),
                        env: Vec::new(),
                    },
                    AcpServerConfig {
                        id: "generic-disabled".to_string(),
                        name: "Generic Disabled".to_string(),
                        profile: AcpProfile::GenericHttp,
                        endpoint: Some("https://acp-disabled.example.test".to_string()),
                        command: None,
                        args: Vec::new(),
                        auth_token: String::new(),
                        default_model: None,
                        enabled: false,
                        priority: 10,
                        headers: Vec::new(),
                        env: Vec::new(),
                    },
                ]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: None,
                all: true,
                probe: false,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let items = json["data"].as_array().expect("data should be an array");
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]["provider_id"],
            Value::String("generic-primary".to_string())
        );
        assert_eq!(items[0]["is_default_selected"], Value::Bool(true));
        assert_eq!(items[0]["diagnostic"]["enabled"], Value::Bool(true));
        assert_eq!(
            items[1]["provider_id"],
            Value::String("generic-disabled".to_string())
        );
        assert_eq!(items[1]["is_default_selected"], Value::Bool(false));
        assert_eq!(items[1]["diagnostic"]["enabled"], Value::Bool(false));
        assert_eq!(
            items[1]["readiness"],
            Value::String("attention_required".to_string())
        );
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_rejects_conflicting_all_and_provider_id() {
        let state = test_state("acp-diagnostic-conflict").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-primary".to_string(),
                    name: "Generic Primary".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some("https://acp-primary.example.test".to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: Some("generic-primary".to_string()),
                all: true,
                probe: false,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["data"]["error"]
                .as_str()
                .unwrap_or_default()
                .contains("cannot be used together")
        );
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_reports_turn_probe_for_generic_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let app = axum::Router::new()
            .route(
                "/turns",
                post(|| async {
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "application/json")],
                        Json(serde_json::json!({
                            "session_id": "acp-probe-session",
                            "output_text": "probe ok"
                        })),
                    )
                }),
            )
            .route(
                "/",
                get(|| async {
                    (
                        StatusCode::METHOD_NOT_ALLOWED,
                        [(header::CONTENT_TYPE, "application/json")],
                        Json(serde_json::json!({
                            "error": "POST required for ACP turn creation"
                        })),
                    )
                }),
            )
            .route(
                "/",
                post(|| async {
                    (
                        StatusCode::METHOD_NOT_ALLOWED,
                        [(header::CONTENT_TYPE, "application/json")],
                        Json(serde_json::json!({
                            "error": "Use /turns for ACP turn creation"
                        })),
                    )
                }),
            );
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = test_state("acp-diagnostic-generic-probe").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-acp".to_string(),
                    name: "Generic ACP".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some(format!("http://{}", address)),
                    command: None,
                    args: Vec::new(),
                    auth_token: "secret-token".to_string(),
                    default_model: Some("acp-probe-model".to_string()),
                    enabled: true,
                    priority: 0,
                    headers: vec![HeaderKeyValue {
                        key: "X-ACP-Client".to_string(),
                        value: "omni-code-bridge".to_string(),
                    }],
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: None,
                all: false,
                probe: true,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["handshake_probe"]["attempted"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["success"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["mode"],
            Value::String("generic_http_turn_create".to_string())
        );
        assert_eq!(
            json["data"]["handshake_probe"]["stage"],
            Value::String("turn_post".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["auth_configured"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["diagnostic"]["default_model"],
            Value::String("acp-probe-model".to_string())
        );
        assert_eq!(
            json["data"]["diagnostic"]["header_count"],
            Value::Number(1.into())
        );
        assert_eq!(
            json["data"]["diagnostic"]["env_count"],
            Value::Number(0.into())
        );
        assert!(
            json["data"]["handshake_probe"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("accepted a probe request")
        );

        server.abort();
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_reports_sse_turn_probe_for_generic_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let app = axum::Router::new().route(
            "/turns",
            post(|| async {
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    "data: {\"session_id\":\"acp-probe-session\",\"delta\":{\"text\":\"stream \"}}\n\n\
data: {\"delta\":{\"text\":\"ok\"}}\n\n\
data: {\"type\":\"done\"}\n\n",
                )
            }),
        );
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = test_state("acp-diagnostic-generic-sse-probe").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-acp-sse".to_string(),
                    name: "Generic ACP SSE".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some(format!("http://{}", address)),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: None,
                all: false,
                probe: true,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["handshake_probe"]["attempted"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["success"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["mode"],
            Value::String("generic_http_turn_create".to_string())
        );
        assert_eq!(
            json["data"]["handshake_probe"]["stage"],
            Value::String("turn_post".to_string())
        );
        assert!(
            json["data"]["handshake_probe"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("SSE stream completed normally")
        );
        assert!(
            json["data"]["handshake_probe"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("stream ok")
        );

        server.abort();
    }

    #[tokio::test]
    async fn acp_agent_diagnostic_endpoint_reports_successful_kiro_probe() {
        let state = test_state("acp-diagnostic-kiro-probe").await;
        let script = mock_kiro_acp_script_path();
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "kiro-mock".to_string(),
                    name: "Kiro Mock".to_string(),
                    profile: AcpProfile::Stdio,
                    endpoint: None,
                    command: Some("python3".to_string()),
                    args: vec![
                        script.to_string_lossy().to_string(),
                        "probe-success".to_string(),
                    ],
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                }]),
            })
            .await
            .expect("settings update should succeed");

        let headers = authorized_headers(&state).await;
        let response = get_acp_agent_diagnostic(
            headers,
            Query(AcpDiagnosticQuery {
                refresh: true,
                provider_id: None,
                all: false,
                probe: true,
            }),
            State(state),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["handshake_probe"]["attempted"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["success"],
            Value::Bool(true)
        );
        assert_eq!(
            json["data"]["handshake_probe"]["mode"],
            Value::String("stdio_json_rpc_handshake".to_string())
        );
        assert_eq!(
            json["data"]["handshake_probe"]["stage"],
            Value::String("session/prompt".to_string())
        );
        assert!(
            json["data"]["handshake_probe"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("kiro-probe-session")
        );
        assert!(
            json["data"]["handshake_probe"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("probe turn")
        );
    }

    fn test_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}"))
    }
}
