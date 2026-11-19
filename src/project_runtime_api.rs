use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;

use super::{Location, Overview, Runtime};
use crate::{
    app_state::AppState,
    models::{ApiError, ApiResponse, ProjectSummary},
};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/v2/projects/{project_id}/runtime",
            get(overview).post(save_location),
        )
        .route("/v2/projects/{project_id}/runtime/start", post(start))
        .route("/v2/projects/{project_id}/runtime/stop", post(stop))
        .route("/v2/projects/{project_id}/runtime/logs", post(logs))
        .route(
            "/v2/projects/{project_id}/runtime/targets",
            post(save_target),
        )
        .route(
            "/v2/projects/{project_id}/runtime/targets/rename",
            post(rename_target),
        )
        .route(
            "/v2/projects/{project_id}/runtime/targets/delete",
            post(delete_target),
        )
}
pub(crate) fn error(error: impl std::fmt::Display) -> ApiError {
    let message = error.to_string();
    ApiError {
        status: if ["占用", "已启动", "租约", "无法确认", "先停止"]
            .iter()
            .any(|text| message.contains(text))
        {
            StatusCode::CONFLICT
        } else {
            StatusCode::BAD_REQUEST
        },
        message,
    }
}
async fn project(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
) -> Result<ProjectSummary, ApiError> {
    crate::api::authorize_request(headers, state).await?;
    state
        .find_project(id)
        .await
        .ok_or_else(|| error("项目不存在"))
}
async fn overview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Overview>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    Ok(Json(ApiResponse {
        data: state
            .project_runtimes
            .overview(&state, &project)
            .await
            .map_err(error)?,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveTarget {
    target: crate::project_detection::BuildTarget,
    name: String,
    #[serde(default)]
    create: bool,
}
async fn save_target(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<SaveTarget>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    state
        .project_runtimes
        .save_target(&state, &project, input.target, input.name, input.create)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: serde_json::json!({"saved": true}),
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameTarget {
    directory: String,
    name: String,
}
async fn rename_target(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<RenameTarget>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    state
        .project_runtimes
        .rename_target(&state, &project, &input.directory, &input.name)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: serde_json::json!({"saved": true}),
    }))
}

async fn delete_target(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Target>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    state
        .project_runtimes
        .delete_target(&state, &project, &input.directory)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: serde_json::json!({"deleted": true}),
    }))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    directory: String,
    location: Location,
}
async fn save_location(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Selection>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    if !state
        .project_runtimes
        .targets(&state, &project)
        .map_err(error)?
        .iter()
        .any(|target| target.directory == input.directory)
    {
        return Err(error("子项目不存在，请重新读取配置"));
    }
    state
        .project_runtimes
        .save_location(&id, &input.directory, input.location)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: serde_json::json!({"saved":true}),
    }))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    directory: String,
    location: Option<Location>,
}
async fn start(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Start>,
) -> Result<Json<ApiResponse<Runtime>>, ApiError> {
    let project = project(&state, &headers, &id).await?;
    let client = headers
        .get("x-omni-code-client-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| error("client ID required"))?
        .to_owned();
    Ok(Json(ApiResponse {
        data: state
            .project_runtimes
            .start(
                state.clone(),
                project,
                input.directory,
                input.location,
                client,
            )
            .await
            .map_err(error)?,
    }))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    directory: String,
}
async fn stop(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Target>,
) -> Result<Json<ApiResponse<Runtime>>, ApiError> {
    project(&state, &headers, &id).await?;
    Ok(Json(ApiResponse {
        data: state
            .project_runtimes
            .stop(&state, &id, &input.directory)
            .await
            .map_err(error)?,
    }))
}
async fn logs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Target>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    project(&state, &headers, &id).await?;
    Ok(Json(ApiResponse {
        data: serde_json::json!({"log":state.project_runtimes.logs(&state, &id, &input.directory).await.map_err(error)?}),
    }))
}
