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
    routing::{get, post},
};
use futures_util::stream::{Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    app_state::AppState,
    asr,
    bridge_settings::BridgeSettings,
    models::{
        ApiResponse, AppUpdateManifest, ApprovalDecisionInput, AudioSpeechInput,
        AudioTranscription, ClientAuthRequestInput, CreateProjectInput, CreateSessionInput,
        RegisterPushDeviceInput, ReplySummary, SendMessageInput, SessionEvent, SummarizeReplyInput,
        TriggerClientMessageInput,
    },
    tts,
};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/files", get(get_file_by_path))
        .route("/app-update/manifest", get(app_update_manifest))
        .route("/app-update/apk", get(download_app_update_apk))
        .route("/client-auth/requests", post(request_client_auth))
        .route(
            "/client-auth/requests/{request_id}",
            get(get_client_auth_request),
        )
        .route("/settings", get(get_settings).put(update_settings))
        .route("/projects", get(list_projects).post(create_project))
        .route("/devices/register", post(register_push_device))
        .route("/client/messages", post(trigger_client_message))
        .route("/projects/{id}/sessions", get(list_project_sessions))
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{id}/cancel", post(cancel_session_reply))
        .route("/audio/transcriptions", post(transcribe_audio))
        .route("/audio/speech", post(synthesize_speech))
        .route(
            "/sessions/{id}/messages",
            get(list_messages).post(send_message),
        )
        .route("/sessions/{id}/summary", post(summarize_reply))
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

async fn app_update_manifest() -> Result<Json<AppUpdateManifest>, StatusCode> {
    let Some(apk_path) = find_mobile_apk() else {
        return Err(StatusCode::NOT_FOUND);
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

async fn download_app_update_apk() -> Result<impl IntoResponse, (StatusCode, String)> {
    let apk_path = find_mobile_apk().ok_or((
        StatusCode::NOT_FOUND,
        "mobile apk has not been built".to_string(),
    ))?;
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
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
    Json(input): Json<BridgeSettings>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    let settings = state
        .save_bridge_settings(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
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
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    let client_id = read_client_id(&headers)?;
    let device = state.register_push_device(client_id, input).await;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: device })))
}

async fn list_project_sessions(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Vec<crate::models::SessionSummary>>>, StatusCode> {
    authorize_request_status(&headers, &state).await?;
    let sessions = state
        .list_project_sessions(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ApiResponse { data: sessions }))
}

async fn transcribe_audio(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    let mut file_bytes = None;
    let mut file_name = "speech.wav".to_string();
    let mut content_type = None;

    while let Some(field) = multipart.next_field().await.map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid multipart payload: {error}"),
        )
    })? {
        if field.name() != Some("file") {
            continue;
        }

        if let Some(name) = field.file_name() {
            file_name = name.to_string();
        }
        content_type = field.content_type().map(ToString::to_string);
        file_bytes = Some(
            field
                .bytes()
                .await
                .map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("failed to read audio file: {error}"),
                    )
                })?
                .to_vec(),
        );
        break;
    }

    let bytes = file_bytes.ok_or((
        StatusCode::BAD_REQUEST,
        "missing multipart field 'file'".to_string(),
    ))?;

    let text = asr::transcribe_audio(bytes, file_name, content_type)
        .await
        .map_err(|error| (StatusCode::BAD_GATEWAY, error))?;

    Ok((
        StatusCode::CREATED,
        Json(ApiResponse {
            data: AudioTranscription { text },
        }),
    ))
}

async fn synthesize_speech(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<AudioSpeechInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    let (audio_bytes, content_type) = tts::synthesize_speech(
        input.input,
        input.voice,
        input.speed,
        input.volume,
        input.response_format,
    )
    .await
    .map_err(|error| (StatusCode::BAD_GATEWAY, error))?;

    Ok((
        StatusCode::OK,
        [("content-type", content_type)],
        Body::from(audio_bytes),
    ))
}

async fn trigger_client_message(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<TriggerClientMessageInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
) -> Result<Json<ApiResponse<Vec<crate::models::ChatMessage>>>, StatusCode> {
    authorize_request_status(&headers, &state).await?;
    let messages = state
        .list_messages(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ApiResponse { data: messages }))
}

async fn send_message(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SendMessageInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    let (user_message, pending_reply) = state
        .send_message(&id, input)
        .await
        .map_err(|err| (StatusCode::NOT_FOUND, err))?;

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
) -> Result<impl IntoResponse, (StatusCode, String)> {
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

async fn cancel_session_reply(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    authorize_request(&headers, &state).await?;
    state
        .cancel_turn(&id)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resolve_approval(
    Path((id, request_id)): Path<(String, String)>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<ApprovalDecisionInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    authorize_request_status(&headers, &state).await?;
    if state.list_messages(&id).await.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let stream = BroadcastStream::new(state.subscribe()).filter_map(move |item| {
        let session_id = id.clone();
        async move {
            match item {
                Ok(event) if event_belongs_to_session(&event, &session_id) => {
                    let json = serde_json::to_string(&event).ok()?;
                    Some(Ok(Event::default().event(event_name(&event)).data(json)))
                }
                _ => None,
            }
        }
    });

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn request_client_auth(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ClientAuthRequestInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let record = state
        .request_client_auth(input)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: record })))
}

async fn get_client_auth_request(
    Path(request_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let record = state
        .get_client_auth_request(&request_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ApiResponse { data: record }))
}

async fn authorize_request(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<(), (StatusCode, String)> {
    let client_id = read_client_id(headers)?;
    let runtime_allowed = state.is_runtime_client_id_allowed(client_id).await;
    if !runtime_allowed {
        return Err((
            StatusCode::FORBIDDEN,
            "client id is not allowed".to_string(),
        ));
    }

    let actual = bearer_token(headers);
    let actual = actual.ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    if state.client_token_matches(client_id, actual).await {
        return Ok(());
    }

    Err((StatusCode::FORBIDDEN, "invalid bearer token".to_string()))
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

fn read_client_id(headers: &HeaderMap) -> Result<&str, (StatusCode, String)> {
    headers
        .get("x-omni-code-client-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or((StatusCode::UNAUTHORIZED, "missing client id".to_string()))
}

async fn authorize_request_status(headers: &HeaderMap, state: &AppState) -> Result<(), StatusCode> {
    authorize_request(headers, state)
        .await
        .map_err(|(status, _)| status)
}

fn event_belongs_to_session(event: &SessionEvent, session_id: &str) -> bool {
    match event {
        SessionEvent::SessionSnapshot(session) => session.id == session_id,
        SessionEvent::SessionStatus(status) => status.session_id == session_id,
        SessionEvent::MessageCreated(message) => message.session_id == session_id,
        SessionEvent::MessageDelta(delta) => delta.session_id == session_id,
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
        SessionEvent::AgentError(_) => "agent.error",
        SessionEvent::ApprovalRequested(_) => "approval.requested",
        SessionEvent::ApprovalResolved(_) => "approval.resolved",
    }
}

async fn resolve_authorized_file_path(
    state: &AppState,
    query: &FileQuery,
) -> Result<PathBuf, (StatusCode, String)> {
    let requested_path = query.path.trim();
    if requested_path.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "path is required".to_string()));
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
        ));
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
    ))
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

fn map_file_resolution_error(requested_path: &str, error: io::Error) -> (StatusCode, String) {
    match error.kind() {
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
    }
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

#[cfg(test)]
mod tests {
    use super::{content_type_for_path, resolve_path_within_root};
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

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
    }

    fn test_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}"))
    }
}
