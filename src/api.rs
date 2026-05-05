use std::{convert::Infallible, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::Multipart,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures_util::stream::{Stream, StreamExt};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    app_state::AppState,
    asr,
    bridge_settings::BridgeSettings,
    models::{
        ApiResponse, AppUpdateManifest, ApprovalDecisionInput, AudioSpeechInput,
        AudioTranscription, CreateProjectInput, CreateSessionInput, RegisterPushDeviceInput,
        ReplySummary, SendMessageInput, SessionEvent, SummarizeReplyInput,
        TriggerClientMessageInput,
    },
    tts,
};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/app-update/manifest", get(app_update_manifest))
        .route("/app-update/apk", get(download_app_update_apk))
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

async fn get_settings(headers: HeaderMap, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state) {
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
    authorize_request(&headers, &state)?;
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
    if let Err(error) = authorize_request(&headers, &state) {
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
    if let Err(error) = authorize_request(&headers, &state) {
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
    authorize_request(&headers, &state)?;
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
    if let Err(error) = authorize_request(&headers, &state) {
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
    authorize_request(&headers, &state)?;
    let client_id = read_client_id(&headers)?;
    let device = state.register_push_device(client_id, input).await;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: device })))
}

async fn list_project_sessions(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<Vec<crate::models::SessionSummary>>>, StatusCode> {
    authorize_request_status(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request_status(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request(&headers, &state)?;
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
    authorize_request_status(&headers, &state)?;
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

fn authorize_request(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let client_id = read_client_id(headers)?;

    if !state.is_client_id_allowed(client_id) {
        return Err((
            StatusCode::FORBIDDEN,
            "client id is not allowed".to_string(),
        ));
    }

    if let Some(expected) = state.bridge_token() {
        let actual = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
        if actual != expected {
            return Err((StatusCode::FORBIDDEN, "invalid bearer token".to_string()));
        }
    }

    Ok(())
}

fn read_client_id(headers: &HeaderMap) -> Result<&str, (StatusCode, String)> {
    headers
        .get("x-omni-code-client-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or((StatusCode::UNAUTHORIZED, "missing client id".to_string()))
}

fn authorize_request_status(headers: &HeaderMap, state: &AppState) -> Result<(), StatusCode> {
    authorize_request(headers, state).map_err(|(status, _)| status)
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
