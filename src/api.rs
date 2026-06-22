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
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    adapter,
    app_state::AppState,
    asr,
    bridge_settings::{BridgeSettings, BridgeSettingsInput},
    models::{
        AgentCommandForwarding, AgentCommandSummary, AgentCommandsSummary, AgentInstallInput,
        AgentKind, AgentSummary, ApiError, ApiResponse, AppUpdateManifest, ApprovalDecisionInput,
        AudioSpeechStreamResponse, CancelSessionReplyResult, ClientAuthRequestInput,
        CreateProjectInput, CreateSessionInput, FileCompletionItem, FileCompletionQuery,
        OpenAiAudioSpeechRequest, OpenAiErrorDetail, OpenAiErrorResponse, OpenAiModel,
        OpenAiModelList, OpenAiTranscriptionResponse, OpenAiVerboseTranscriptionResponse,
        OpenAiVerboseTranscriptionSegment, RegisterPushDeviceInput, ReplySummary, SendMessageInput,
        SessionEvent, SpeakerFilterSettingsInput, SpeechModelDownloadInput, SpeechModelKind,
        SpeechProfile, SpeechProfileSelectionInput, SpeechVoiceSelectionInput, SummarizeReplyInput,
        TriggerClientMessageInput, UpdateSessionInput, UploadedFileResponse,
    },
    realtime, speaker,
    speech::{
        self, profile_slug, set_profile_model, set_tts_model_voice, validate_tts_model_voice,
    },
    tts,
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
        .route("/projects", get(list_projects).post(create_project))
        .route("/devices/register", post(register_push_device))
        .route("/client/messages", post(trigger_client_message))
        .route("/projects/{id}/sessions", get(list_project_sessions))
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{id}/cancel", post(cancel_session_reply))
        .route("/v1/models", get(list_openai_models))
        .route("/v1/audio/transcriptions", post(transcribe_audio))
        .route("/v1/audio/speech", post(synthesize_speech))
        .route(
            "/v1/audio/speech/streams/{token}",
            get(stream_synthesized_speech).head(head_synthesized_speech),
        )
        .route("/speech", get(get_speech_status))
        .route("/speech/realtime", get(get_speech_realtime_descriptor))
        .route("/speech/realtime/ws", get(connect_speech_realtime))
        .route("/speech/models", get(list_speech_models))
        .route("/speech/models/downloads", post(create_speech_download))
        .route("/speech/speakers", get(list_speakers).post(enroll_speaker))
        .route("/speech/speakers/{speaker_id}", delete(delete_speaker))
        .route(
            "/speech/speaker-filter",
            get(get_speaker_filter).put(update_speaker_filter),
        )
        .route(
            "/speech/models/downloads/{task_id}",
            get(get_speech_download),
        )
        .route(
            "/speech/models/{model_id}/voice",
            get(get_speech_model_voice).put(update_speech_model_voice),
        )
        .route(
            "/speech/profiles/{profile}/model",
            get(get_speech_profile_model).put(update_speech_profile_model),
        )
        .route(
            "/sessions/{id}/messages",
            get(list_messages).post(send_message),
        )
        .route("/sessions/{id}", get(get_session).patch(update_session))
        .route("/sessions/{id}/summary", post(summarize_reply))
        .route("/agents", get(list_agents))
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

async fn transcribe_audio(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let mut file_bytes = None;
    let mut file_name = "speech.wav".to_string();
    let mut content_type = None;
    let mut model = None;
    let mut language = None;
    let mut prompt = None;
    let mut response_format = None;
    let mut stream = None;
    let mut timestamp_granularities = Vec::new();

    while let Some(field) = match multipart.next_field().await {
        Ok(field) => field,
        Err(error) => {
            return openai_error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid multipart payload: {error}"),
                "invalid_request_error",
                None,
                None,
            )
            .into_response();
        }
    } {
        let field_name = field.name().map(ToString::to_string);
        if field_name.as_deref() == Some("model") {
            model = Some(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read model field: {error}"),
                        "invalid_request_error",
                        Some("model"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }
        if field_name.as_deref() == Some("language") {
            language = Some(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read language field: {error}"),
                        "invalid_request_error",
                        Some("language"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }
        if field_name.as_deref() == Some("prompt") {
            prompt = Some(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read prompt field: {error}"),
                        "invalid_request_error",
                        Some("prompt"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }
        if field_name.as_deref() == Some("response_format") {
            response_format = Some(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read response_format field: {error}"),
                        "invalid_request_error",
                        Some("response_format"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }
        if field_name.as_deref() == Some("stream") {
            stream = Some(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read stream field: {error}"),
                        "invalid_request_error",
                        Some("stream"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }
        if field_name.as_deref() == Some("timestamp_granularities[]") {
            timestamp_granularities.push(match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    return openai_error_response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read timestamp_granularities[] field: {error}"),
                        "invalid_request_error",
                        Some("timestamp_granularities[]"),
                        None,
                    )
                    .into_response();
                }
            });
            continue;
        }

        if field_name.as_deref() != Some("file") {
            continue;
        }

        if let Some(name) = field.file_name() {
            file_name = name.to_string();
        }
        content_type = field.content_type().map(ToString::to_string);
        file_bytes = Some(match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(error) => {
                return openai_error_response(
                    StatusCode::BAD_REQUEST,
                    format!("failed to read audio file: {error}"),
                    "invalid_request_error",
                    Some("file"),
                    None,
                )
                .into_response();
            }
        });
    }

    let Some(bytes) = file_bytes else {
        return openai_error_response(
            StatusCode::BAD_REQUEST,
            "missing multipart field 'file'",
            "invalid_request_error",
            Some("file"),
            None,
        )
        .into_response();
    };

    let requested_model = model
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let normalized_language = match normalize_transcription_language(language.as_deref()) {
        Ok(language) => language,
        Err(error) => {
            return openai_error_response(
                StatusCode::BAD_REQUEST,
                error,
                "invalid_request_error",
                Some("language"),
                None,
            )
            .into_response();
        }
    };
    let normalized_response_format =
        normalize_transcription_response_format(response_format.as_deref());

    if let Err(error) = validate_transcription_options(
        requested_model.as_deref(),
        normalized_language.as_deref(),
        prompt.as_deref(),
        normalized_response_format,
        stream.as_deref(),
        &timestamp_granularities,
    ) {
        return openai_error_response(
            StatusCode::BAD_REQUEST,
            error,
            "invalid_request_error",
            None,
            None,
        )
        .into_response();
    }

    let settings = state.bridge_settings().await;
    let model_id =
        match resolve_asr_request_model(&state, &settings, requested_model.as_deref()).await {
            Ok(model_id) => model_id,
            Err(error) => {
                return openai_error_response(
                    StatusCode::PRECONDITION_FAILED,
                    error,
                    "invalid_request_error",
                    Some("model"),
                    None,
                )
                .into_response();
            }
        };

    let transcription = match asr::transcribe_audio(
        state.speech(),
        settings.speech_profiles,
        &model_id,
        bytes,
        file_name,
        content_type,
        normalized_language.clone(),
        settings.speaker_filter,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return openai_error_response(
                StatusCode::BAD_GATEWAY,
                error,
                "server_error",
                None,
                None,
            )
            .into_response();
        }
    };

    transcription_response(
        normalized_response_format,
        normalized_language.as_deref(),
        transcription.text,
        transcription.duration_secs,
    )
    .into_response()
}

async fn synthesize_speech(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<OpenAiAudioSpeechRequest>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let model = input
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let response_format = input
        .response_format
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let stream = input.stream.unwrap_or(false);
    if let Err(error) = validate_speech_options(
        model,
        input.input.as_str(),
        input.voice.as_deref(),
        input.instructions.as_deref(),
        response_format.as_deref(),
        input.speed,
    ) {
        return openai_error_response(
            StatusCode::BAD_REQUEST,
            error,
            "invalid_request_error",
            None,
            None,
        )
        .into_response();
    }
    let settings = state.bridge_settings().await;
    let model_id = match resolve_tts_request_model(&state, &settings, model).await {
        Ok(model_id) => model_id,
        Err(error) => {
            return openai_error_response(
                StatusCode::PRECONDITION_FAILED,
                error,
                "invalid_request_error",
                Some("model"),
                None,
            )
            .into_response();
        }
    };
    let voice = input
        .voice
        .as_deref()
        .is_some_and(|voice| !voice.trim().is_empty())
        .then_some(input.voice)
        .flatten()
        .or_else(|| settings.speech_voices.tts_by_model.get(&model_id).cloned());
    if stream {
        let session = state
            .create_tts_stream_session(
                model_id,
                input.input,
                voice,
                input.speed,
                response_format,
                "audio/wav".to_string(),
            )
            .await;
        return (
            StatusCode::OK,
            Json(ApiResponse {
                data: AudioSpeechStreamResponse {
                    stream_url: format!("/v1/audio/speech/streams/{}", session.token),
                    content_type: session.content_type,
                },
            }),
        )
            .into_response();
    }
    let (audio_bytes, content_type) = match tts::synthesize_speech(
        state.speech(),
        &model_id,
        input.input,
        voice,
        input.speed,
        response_format,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return openai_error_response(
                StatusCode::BAD_GATEWAY,
                error,
                "server_error",
                None,
                None,
            )
            .into_response();
        }
    };

    (
        StatusCode::OK,
        [("content-type", content_type)],
        Body::from(audio_bytes),
    )
        .into_response()
}

async fn stream_synthesized_speech(
    Path(token): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(session) = state.get_tts_stream_session(&token).await else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if headers.get(header::RANGE).is_some() {
        return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    }

    let (body, content_type) = match tts::synthesize_speech_stream(
        state.speech(),
        &session.model_id,
        session.input,
        session.voice,
        session.speed,
        session.response_format,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return openai_error_response(
                StatusCode::BAD_GATEWAY,
                error,
                "server_error",
                None,
                None,
            )
            .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            ("content-type", content_type),
            ("cache-control", "no-store".to_string()),
        ],
        body,
    )
        .into_response()
}

async fn head_synthesized_speech(
    Path(token): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let Some(session) = state.get_tts_stream_session(&token).await else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let mut response = StatusCode::OK.into_response();
    let headers = response.headers_mut();
    insert_header(headers, header::CONTENT_TYPE, &session.content_type);
    insert_header(headers, header::CACHE_CONTROL, "no-store");
    response
}

fn insert_header(headers: &mut HeaderMap, name: header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

async fn list_openai_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }

    let data = state
        .speech()
        .list_models(state.bridge_settings().await.speech_profiles)
        .await
        .into_iter()
        .filter(|model| {
            model.installed
                && match model.kind {
                    SpeechModelKind::Asr => model.capabilities.batch_asr,
                    SpeechModelKind::Tts => model.capabilities.speech_synthesis,
                    SpeechModelKind::Vad => false,
                    SpeechModelKind::Speaker => false,
                }
        })
        .map(|model| OpenAiModel {
            id: model.id,
            object: "model".to_string(),
            created: 0,
            owned_by: "omni-code-bridge".to_string(),
        })
        .collect();

    Json(OpenAiModelList {
        object: "list".to_string(),
        data,
    })
    .into_response()
}

async fn get_speech_status(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let settings = state.bridge_settings().await;
    Json(ApiResponse {
        data: state
            .speech()
            .status(settings.speech_profiles, settings.speech_voices)
            .await,
    })
    .into_response()
}

async fn get_speech_realtime_descriptor(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }

    Json(ApiResponse {
        data: realtime::descriptor(&state).await,
    })
    .into_response()
}

async fn connect_speech_realtime(
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }

    ws.on_upgrade(move |socket| realtime::handle_socket(socket, state))
        .into_response()
}

async fn list_speech_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    let settings = state.bridge_settings().await;
    Json(ApiResponse {
        data: state.speech().list_models(settings.speech_profiles).await,
    })
    .into_response()
}

async fn create_speech_download(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SpeechModelDownloadInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let task = state
        .speech()
        .queue_download(&input.model_id)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: task })))
}

async fn get_speech_download(
    Path(task_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let task = state
        .speech()
        .get_download(&task_id)
        .await
        .ok_or(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "speech download task not found".to_string(),
        })?;
    Ok(Json(ApiResponse { data: task }))
}

async fn list_speakers(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let speakers = speaker::list_speakers()
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(ApiResponse { data: speakers }))
}

async fn enroll_speaker(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let mut file_bytes = None;
    let mut file_name = "speaker.wav".to_string();
    let mut content_type = None;
    let mut name = None;

    while let Some(field) = multipart.next_field().await.map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid multipart payload: {error}"),
        )
    })? {
        let field_name = field.name().map(ToString::to_string);
        if field_name.as_deref() == Some("name") {
            name = Some(field.text().await.map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("failed to read name field: {error}"),
                )
            })?);
            continue;
        }

        if field_name.as_deref() != Some("file") {
            continue;
        }

        if let Some(value) = field.file_name() {
            file_name = value.to_string();
        }
        content_type = field.content_type().map(ToString::to_string);
        file_bytes = Some(
            field
                .bytes()
                .await
                .map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("failed to read speaker audio: {error}"),
                    )
                })?
                .to_vec(),
        );
    }

    let bytes = file_bytes.ok_or((
        StatusCode::BAD_REQUEST,
        "missing multipart field 'file'".to_string(),
    ))?;
    let name = name
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or((StatusCode::BAD_REQUEST, "missing speaker name".to_string()))?;
    let result = speaker::enroll_speaker(state.speech(), name, bytes, file_name, content_type)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok((StatusCode::CREATED, Json(ApiResponse { data: result })))
}

async fn delete_speaker(
    Path(speaker_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    speaker::delete_speaker(&speaker_id)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let current = state.bridge_settings().await.speaker_filter;
    if current.speaker_id.as_deref() == Some(speaker_id.as_str()) {
        let mut updated = current;
        updated.enabled = false;
        updated.speaker_id = None;
        state
            .update_speaker_filter_settings(speaker::normalize_speaker_filter(updated))
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn get_speaker_filter(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    Ok(Json(ApiResponse {
        data: state.bridge_settings().await.speaker_filter,
    }))
}

async fn update_speaker_filter(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SpeakerFilterSettingsInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let settings = speaker::normalize_speaker_filter(crate::models::SpeakerFilterSettings {
        enabled: input.enabled,
        speaker_id: input.speaker_id,
        threshold: input.threshold,
    });
    if let Some(speaker_id) = settings.speaker_id.as_deref() {
        let speakers = speaker::list_speakers()
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        if !speakers.iter().any(|speaker| speaker.id == speaker_id) {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                message: "unknown speaker_id".to_string(),
            });
        }
    }
    let settings = state
        .update_speaker_filter_settings(settings)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?
        .speaker_filter;
    Ok(Json(ApiResponse { data: settings }))
}

async fn get_speech_model_voice(
    Path(model_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    validate_tts_model_voice(&state.speech(), &model_id, None)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let settings = state.bridge_settings().await;
    Ok(Json(ApiResponse {
        data: serde_json::json!({
            "model_id": model_id,
            "voice": settings.speech_voices.tts_by_model.get(&model_id),
        }),
    }))
}

async fn update_speech_model_voice(
    Path(model_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SpeechVoiceSelectionInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let voices = set_tts_model_voice(
        state.settings_store(),
        &state.speech(),
        &model_id,
        input.voice,
    )
    .await
    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(ApiResponse { data: voices }))
}

async fn get_speech_profile_model(
    Path(profile): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let profile = speech::profile_from_slug(&profile).ok_or(ApiError {
        status: StatusCode::BAD_REQUEST,
        message: "unknown speech profile".to_string(),
    })?;
    let settings = state.bridge_settings().await;
    Ok(Json(ApiResponse {
        data: serde_json::json!({
            "profile": profile_slug(profile),
            "model_id": settings.speech_profiles.model_for_profile(profile),
        }),
    }))
}

async fn update_speech_profile_model(
    Path(profile): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(input): Json<SpeechProfileSelectionInput>,
) -> Result<impl IntoResponse, ApiError> {
    authorize_request(&headers, &state).await?;
    let profile = speech::profile_from_slug(&profile).ok_or(ApiError {
        status: StatusCode::BAD_REQUEST,
        message: "unknown speech profile".to_string(),
    })?;
    let profiles = set_profile_model(
        state.settings_store(),
        &state.speech(),
        profile,
        input.model_id,
    )
    .await
    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(ApiResponse { data: profiles }))
}

async fn resolve_asr_request_model(
    state: &Arc<AppState>,
    settings: &BridgeSettings,
    requested_model: Option<&str>,
) -> Result<String, String> {
    if let Some(model_id) = requested_model.filter(|value| !value.trim().is_empty()) {
        state.speech().resolve_model_by_id(model_id.trim()).await?;
        validate_requested_model_for_profile(&state, model_id.trim(), SpeechProfile::AsrBatch)?;
        return Ok(model_id.trim().to_string());
    }

    state
        .speech()
        .verify_profile(
            &settings.speech_profiles,
            crate::models::SpeechProfile::AsrBatch,
        )
        .await
        .map(|_| {
            settings
                .speech_profiles
                .asr_batch
                .clone()
                .expect("asr_batch must exist after verify_profile succeeds")
        })
}

async fn resolve_tts_request_model(
    state: &Arc<AppState>,
    settings: &BridgeSettings,
    requested_model: Option<&str>,
) -> Result<String, String> {
    if let Some(model_id) = requested_model.filter(|value| !value.trim().is_empty()) {
        state.speech().resolve_model_by_id(model_id.trim()).await?;
        validate_requested_model_kind(&state, model_id.trim(), SpeechModelKind::Tts)?;
        return Ok(model_id.trim().to_string());
    }

    match state
        .speech()
        .verify_profile(
            &settings.speech_profiles,
            crate::models::SpeechProfile::TtsDefault,
        )
        .await
    {
        Ok(_) => Ok(settings
            .speech_profiles
            .tts_default
            .clone()
            .expect("tts_default must exist after verify_profile succeeds")),
        Err(_) => infer_installed_tts_model(state, settings)
            .await
            .ok_or_else(|| {
                format!(
                    "configure speech profile {} by calling PUT /speech/profiles/{}/model",
                    profile_slug(crate::models::SpeechProfile::TtsDefault),
                    profile_slug(crate::models::SpeechProfile::TtsDefault)
                )
            }),
    }
}

async fn infer_installed_tts_model(
    state: &Arc<AppState>,
    settings: &BridgeSettings,
) -> Option<String> {
    let models = state
        .speech()
        .list_models(settings.speech_profiles.clone())
        .await;
    let mut candidates = models
        .into_iter()
        .filter(|model| {
            model.installed
                && model
                    .supports_profiles
                    .contains(&crate::models::SpeechProfile::TtsDefault)
        })
        .collect::<Vec<_>>();

    candidates.sort_by_key(|model| if model.id.contains("kokoro") { 0 } else { 1 });
    candidates.into_iter().map(|model| model.id).next()
}

fn validate_transcription_options(
    model: Option<&str>,
    language: Option<&str>,
    _prompt: Option<&str>,
    response_format: Option<&str>,
    stream: Option<&str>,
    timestamp_granularities: &[String],
) -> Result<(), String> {
    if let Some(language) = language {
        if !supported_sensevoice_languages().contains(&language) {
            return Err(format!(
                "unsupported language '{}'; supported values are auto, zh, en, yue, ja, ko",
                language
            ));
        }
    }

    if let Some(format) = response_format {
        let allowed = ["json", "text", "verbose_json"];
        if !allowed.contains(&format.trim()) {
            return Err(format!(
                "unsupported response_format '{}'; supported values are json, text, verbose_json",
                format.trim()
            ));
        }
    }

    if let Some(stream) = stream {
        let value = stream.trim().to_ascii_lowercase();
        if value == "true" || value == "1" {
            return Err("stream=true is not supported on /v1/audio/transcriptions yet".to_string());
        }
    }

    for granularity in timestamp_granularities {
        let value = granularity.trim();
        match value {
            "" | "segment" => {}
            "word" => {
                return Err(
                    "timestamp_granularities[]=word is not supported by the current local ASR backend"
                        .to_string(),
                );
            }
            _ => {
                return Err(format!(
                    "unsupported timestamp_granularities[] value '{}'; supported values are segment",
                    value
                ));
            }
        }
    }

    if let Some(model) = model {
        if model.trim().is_empty() {
            return Err("model must not be empty".to_string());
        }
    }

    Ok(())
}

fn normalize_transcription_language(language: Option<&str>) -> Result<Option<String>, String> {
    let Some(language) = language.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let normalized = language.to_ascii_lowercase().replace('_', "-");
    let normalized = match normalized.as_str() {
        "auto" => "auto",
        "zh" | "zh-cn" | "zh-hans" | "cmn" => "zh",
        "en" | "en-us" | "en-gb" => "en",
        "yue" | "zh-hk" | "zh-hant" => "yue",
        "ja" | "jp" => "ja",
        "ko" | "kr" => "ko",
        _ => {
            return Err(format!(
                "unsupported language '{}'; supported values are auto, zh, en, yue, ja, ko",
                language
            ));
        }
    };
    Ok(Some(normalized.to_string()))
}

fn supported_sensevoice_languages() -> &'static [&'static str] {
    &["auto", "zh", "en", "yue", "ja", "ko"]
}

fn validate_speech_options(
    model: Option<&str>,
    input: &str,
    voice: Option<&str>,
    instructions: Option<&str>,
    response_format: Option<&str>,
    speed: Option<f32>,
) -> Result<(), String> {
    if let Some(model) = model {
        if model.trim().is_empty() {
            return Err("model must not be empty".to_string());
        }
    }

    if input.trim().is_empty() {
        return Err("input is required".to_string());
    }

    if let Some(voice) = voice.map(str::trim).filter(|value| !value.is_empty()) {
        if voice.parse::<i32>().is_err() {
            return Err("voice must be a numeric speaker id".to_string());
        }
    }

    if let Some(speed) = speed {
        if !(0.25..=4.0).contains(&speed) {
            return Err("speed must be between 0.25 and 4.0".to_string());
        }
    }

    if let Some(format) = response_format {
        let value = format.trim();
        if value != "wav" {
            return Err(format!(
                "unsupported response_format '{}'; local TTS currently supports wav only",
                value
            ));
        }
    }

    if instructions
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
    {
        return Err("instructions is not supported by the current local TTS backend".to_string());
    }

    Ok(())
}

fn normalize_transcription_response_format(response_format: Option<&str>) -> Option<&str> {
    response_format
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn transcription_response(
    response_format: Option<&str>,
    language: Option<&str>,
    text: String,
    duration_secs: f32,
) -> impl IntoResponse {
    match response_format.unwrap_or("json") {
        "text" => (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            Body::from(text),
        )
            .into_response(),
        "verbose_json" => {
            let response = OpenAiVerboseTranscriptionResponse {
                task: "transcribe".to_string(),
                language: language
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("auto")
                    .to_string(),
                duration: duration_secs,
                segments: vec![OpenAiVerboseTranscriptionSegment {
                    id: 0,
                    seek: 0,
                    start: 0.0,
                    end: duration_secs,
                    text: text.clone(),
                }],
                text,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        _ => (StatusCode::OK, Json(OpenAiTranscriptionResponse { text })).into_response(),
    }
}

fn validate_requested_model_for_profile(
    state: &Arc<AppState>,
    model_id: &str,
    profile: SpeechProfile,
) -> Result<(), String> {
    let kind = match state.speech().model_kind(model_id) {
        Some(kind) => kind,
        None => return Err(format!("unknown speech model: {model_id}")),
    };

    if kind != SpeechModelKind::Asr {
        return Err(format!(
            "model {model_id} is not an ASR model and cannot be used for /v1/audio/transcriptions"
        ));
    }

    if !state.speech().supports_profile(model_id, profile) {
        return Err(format!(
            "model {model_id} does not support batch transcription; choose a model compatible with profile {}",
            profile_slug(profile)
        ));
    }

    Ok(())
}

fn validate_requested_model_kind(
    state: &Arc<AppState>,
    model_id: &str,
    expected_kind: SpeechModelKind,
) -> Result<(), String> {
    match state.speech().model_kind(model_id) {
        Some(kind) if kind == expected_kind => Ok(()),
        Some(_) => Err(format!("model {model_id} cannot be used for this endpoint")),
        None => Err(format!("unknown speech model: {model_id}")),
    }
}

fn openai_error_response(
    status: StatusCode,
    message: impl Into<String>,
    error_type: &str,
    param: Option<&str>,
    code: Option<&str>,
) -> impl IntoResponse {
    (
        status,
        Json(OpenAiErrorResponse {
            error: OpenAiErrorDetail {
                message: message.into(),
                error_type: error_type.to_string(),
                param: param.map(ToString::to_string),
                code: code.map(ToString::to_string),
            },
        }),
    )
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
) -> Result<Json<ApiResponse<Vec<crate::models::ChatMessage>>>, ApiError> {
    authorize_request_status(&headers, &state).await?;
    let messages = state.list_messages(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "session not found".to_string(),
    })?;
    Ok(Json(ApiResponse { data: messages }))
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
        .update_session_settings(&id, input.provider_id, input.reasoning_effort)
        .await
        .map_err(|err| ApiError {
            status: StatusCode::NOT_FOUND,
            message: err,
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
        agent_summary(AgentKind::Codex),
        agent_summary(AgentKind::ClaudeCode),
        agent_summary(AgentKind::OpenCode),
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
    ];
    Json(ApiResponse { data: commands }).into_response()
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

fn agent_summary(kind: AgentKind) -> AgentSummary {
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
            install_hint: "Custom agent does not support auto-install".to_string(),
        };
    }
    let installed_path = adapter::find_executable_in_path(binary_name);
    AgentSummary {
        kind,
        id: id.to_string(),
        label: label.to_string(),
        aliases: aliases.into_iter().map(ToString::to_string).collect(),
        selectable,
        default_selected,
        compatible_formats,
        installed: installed_path.is_some(),
        installed_path: installed_path.map(|p| p.display().to_string()),
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
                description: "Set the current thread goal".to_string(),
                forwarding: AgentCommandForwarding::Native,
            },
            AgentCommandSummary {
                name: "/clear-goal".to_string(),
                args_hint: None,
                description: "Clear the current thread goal".to_string(),
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
        AgentKind::OpenCode | AgentKind::Custom => Vec::new(),
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
    let detail = state.get_session(&id).await.ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        message: "session not found".to_string(),
    })?;

    let initial_event = SessionEvent::SessionSnapshot(detail.session);
    let initial_stream = stream::once(async move { sse_event_for_session_event(&initial_event) })
        .filter_map(std::future::ready);

    let broadcast_stream = BroadcastStream::new(state.subscribe()).filter_map(move |item| {
        let session_id = id.clone();
        async move {
            match item {
                Ok(event) if event_belongs_to_session(&event, &session_id) => {
                    sse_event_for_session_event(&event)
                }
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

fn sse_event_for_session_event(event: &SessionEvent) -> Option<Result<Event, Infallible>> {
    let (name, json) = encode_session_event(event)?;
    Some(Ok(Event::default().event(name).data(json)))
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
        absolute_url_from_headers, agent_commands_summary, agent_summary, completion_search_scope,
        content_type_for_path, content_type_for_upload_name, encode_session_event,
        list_completion_items, normalize_completion_prefix, normalize_transcription_language,
        normalize_transcription_response_format, resolve_path_within_root,
        sanitize_upload_file_name, sanitize_upload_lookup_id, transcription_response, uploads_dir,
        validate_speech_options, validate_transcription_options,
    };
    use crate::models::{AgentKind, FileCompletionQuery, SessionEvent};
    use axum::{
        body::to_bytes,
        http::{HeaderMap, StatusCode, header},
        response::IntoResponse,
    };
    use serde_json::Value;
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
    fn transcription_options_allow_missing_model_for_profile_fallback() {
        assert!(validate_transcription_options(None, None, None, None, None, &[]).is_ok());
    }

    #[test]
    fn transcription_options_reject_word_timestamps() {
        let error = validate_transcription_options(
            None,
            None,
            None,
            Some("verbose_json"),
            None,
            &["word".to_string()],
        )
        .unwrap_err();
        assert!(error.contains("timestamp_granularities[]=word"));
    }

    #[test]
    fn transcription_language_normalizes_common_sensevoice_aliases() {
        assert_eq!(
            normalize_transcription_language(Some(" zh-CN ")).unwrap(),
            Some("zh".to_string())
        );
        assert_eq!(
            normalize_transcription_language(Some("zh_HK")).unwrap(),
            Some("yue".to_string())
        );
        assert_eq!(normalize_transcription_language(Some("   ")).unwrap(), None);
    }

    #[test]
    fn transcription_options_reject_unsupported_language() {
        let error =
            normalize_transcription_language(Some("fr")).expect_err("fr should be unsupported");
        assert!(error.contains("unsupported language 'fr'"));
    }

    #[test]
    fn speech_options_allow_missing_model_for_profile_fallback() {
        assert!(validate_speech_options(None, "hello", None, None, None, None).is_ok());
    }

    #[test]
    fn speech_options_require_non_empty_input() {
        let error = validate_speech_options(None, "   ", None, None, None, None).unwrap_err();
        assert_eq!(error, "input is required");
    }

    #[test]
    fn speech_options_reject_non_numeric_voice() {
        let error =
            validate_speech_options(None, "hello", Some("alloy"), None, None, None).unwrap_err();
        assert_eq!(error, "voice must be a numeric speaker id");
    }

    #[test]
    fn speech_options_allow_numeric_voice_for_model_level_fallback() {
        assert!(validate_speech_options(None, "hello", Some("48"), None, None, None).is_ok());
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
            provider_id: None,
            reasoning_effort: None,
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

        let claude = agent_commands_summary(AgentKind::ClaudeCode);
        assert!(
            claude
                .commands
                .iter()
                .any(|command| command.name == "/clear")
        );

        let opencode = agent_commands_summary(AgentKind::OpenCode);
        assert!(opencode.commands.is_empty());
    }

    #[test]
    fn agent_summary_exposes_descriptor_metadata() {
        let codex = agent_summary(AgentKind::Codex);
        assert_eq!(codex.id, "codex");
        assert_eq!(codex.label, "Codex");
        assert!(codex.default_selected);
        assert_eq!(codex.compatible_formats.len(), 1);

        let claude = agent_summary(AgentKind::ClaudeCode);
        assert!(claude.aliases.contains(&"claudecode".to_string()));
        assert!(!claude.default_selected);

        let custom = agent_summary(AgentKind::Custom);
        assert!(!custom.selectable);
    }

    #[tokio::test]
    async fn transcription_response_returns_text_body() {
        let response =
            transcription_response(Some("text"), None, "hello".to_string(), 1.25).into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "hello");
    }

    #[tokio::test]
    async fn transcription_response_returns_verbose_json_body() {
        let response =
            transcription_response(Some("verbose_json"), Some("zh"), "你好".to_string(), 2.5)
                .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["task"], "transcribe");
        assert_eq!(json["language"], "zh");
        assert_eq!(json["text"], "你好");
        assert_eq!(json["duration"], 2.5);
        assert_eq!(json["segments"][0]["start"], 0.0);
        assert_eq!(json["segments"][0]["end"], 2.5);
    }

    #[test]
    fn normalize_transcription_response_format_trims_empty_values() {
        assert_eq!(
            normalize_transcription_response_format(Some(" json ")),
            Some("json")
        );
        assert_eq!(normalize_transcription_response_format(Some("   ")), None);
    }

    fn test_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}"))
    }
}
