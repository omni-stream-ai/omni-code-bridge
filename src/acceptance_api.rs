use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
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
            "/v2/projects/{project_id}/acceptance-environment",
            get(project_environment).post(save_project_environment),
        )
        .route("/v2/acceptance/{id}", get(get_run))
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
) -> Result<Json<ApiResponse<EnvironmentSettings>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    require_project(&state, &project_id).await?;
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .environments
            .settings(&project_id)
            .map_err(internal)?,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveEnvironment {
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
    let config = EnvironmentConfig {
        mode: input.mode,
        dockerfile: input.dockerfile,
        image: input.image,
        ..Default::default()
    };
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .environments
            .save_config(&project_id, config, input.rebuild)
            .map_err(operation_error)?,
    }))
}

async fn list_runs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<AcceptanceRun>>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    Ok(Json(ApiResponse {
        data: state.acceptance.list().await.map_err(internal)?,
    }))
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
    Ok(Json(ApiResponse {
        data: state
            .acceptance
            .snapshot(&id)
            .await
            .map_err(operation_error)?,
    }))
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
