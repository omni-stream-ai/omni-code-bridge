use crate::{
    api::authorize_request,
    app_state::AppState,
    mcp::{self, Scope},
    models::{AgentKind, ApiError, ApiResponse},
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v2/mcp/config", get(config))
        .route("/v2/mcp/servers/{name}", put(save).delete(remove))
        .route("/v2/mcp/status", get(status))
        .route("/v2/mcp/inventory", get(inventory))
        .route("/v2/mcp/rpc", post(rpc))
        .route("/v2/mcp/requests", get(requests))
        .route("/v2/mcp/requests/{id}", post(respond))
        .route("/v2/mcp/disconnect", post(disconnect))
        .route("/v2/mcp/oauth/complete", post(complete_oauth))
}
fn error(error: impl std::fmt::Display) -> ApiError {
    let message = error.to_string();
    ApiError {
        status: if message.contains("Configuration changed") {
            StatusCode::CONFLICT
        } else {
            StatusCode::BAD_REQUEST
        },
        message,
    }
}
async fn scope(state: &AppState, query: Scope) -> Result<(String, PathBuf), ApiError> {
    if let Some(id) = query.session_id {
        let detail = state
            .get_session(&id)
            .await
            .ok_or_else(|| error("Session not found"))?;
        if detail.session.agent != AgentKind::Codex {
            return Err(error(
                "This agent does not expose Codex MCP management. Its native MCP configuration remains in effect.",
            ));
        }
        let root = state
            .project_root_path_for_session(&id)
            .await
            .map_err(error)?;
        return Ok((
            detail
                .session
                .profile_id
                .unwrap_or_else(|| "default".into()),
            PathBuf::from(root),
        ));
    }
    let profile = if query.profile_id.is_empty() {
        "default".into()
    } else {
        query.profile_id
    };
    mcp::config_path(&profile).map_err(error)?;
    Ok((profile, std::env::current_dir().map_err(error)?))
}
async fn config(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, _) = scope(&state, query).await?;
    let value = mcp::read_profile_config(&profile).map_err(error)?;
    Ok(Json(ApiResponse { data: value }))
}
async fn save(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<Scope>,
    Json(input): Json<mcp::ConfigUpdate>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, _) = scope(&state, query).await?;
    let expected = input.expected_revision.clone();
    let value = mcp::save_config(
        &mcp::config_path(&profile).map_err(error)?,
        &name,
        Some(input),
        &expected,
    )
    .await
    .map_err(error)?;
    mcp::invalidate(&profile).await.map_err(error)?;
    Ok(Json(ApiResponse { data: value }))
}
#[derive(Deserialize)]
struct Remove {
    expected_revision: String,
}
async fn remove(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<Scope>,
    Json(input): Json<Remove>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, _) = scope(&state, query).await?;
    let value = mcp::save_config(
        &mcp::config_path(&profile).map_err(error)?,
        &name,
        None,
        &input.expected_revision,
    )
    .await
    .map_err(error)?;
    mcp::invalidate(&profile).await.map_err(error)?;
    Ok(Json(ApiResponse { data: value }))
}
async fn status(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    load_status(headers, state, query, false).await
}
async fn inventory(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    load_status(headers, state, query, true).await
}
async fn load_status(
    headers: HeaderMap,
    state: Arc<AppState>,
    query: Scope,
    full: bool,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let session_runtime = mcp::session_runtime(query.session_id.as_deref());
    let (profile, cwd) = scope(&state, query).await?;
    let peer = mcp::peer(&profile, &cwd).await.map_err(error)?;
    let thread = peer.thread_id().await.map_err(error)?;
    let mut data = Vec::new();
    let mut cursor = Value::Null;
    loop {
        let page = peer
            .rpc(
                "mcpServerStatus/list",
                json!({"threadId":thread,"cursor":cursor,"limit":100,
                    "detail":if full {"full"} else {"toolsAndAuthOnly"}}),
            )
            .await
            .map_err(error)?;
        data.extend(page["data"].as_array().cloned().unwrap_or_default());
        let next = page.get("nextCursor").cloned().unwrap_or(Value::Null);
        if next.is_null() {
            break;
        }
        if next == cursor || data.len() > 10000 {
            return Err(error("Invalid MCP pagination response"));
        }
        cursor = next;
    }
    Ok(Json(ApiResponse {
        data: json!({"servers":data,"connection":peer.snapshot().await, "source":"managementProbe", "sessionRuntime":session_runtime,"inventoryComplete":full}),
    }))
}
#[derive(Deserialize)]
struct Rpc {
    method: String,
    #[serde(default)]
    params: Value,
}
async fn rpc(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
    Json(input): Json<Rpc>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    if !matches!(
        input.method.as_str(),
        "mcpServer/oauth/login"
            | "config/mcpServer/reload"
            | "mcpServer/resource/read"
            | "mcpServer/tool/call"
    ) {
        return Err(error("Unsupported MCP management method"));
    }
    let (profile, cwd) = scope(&state, query).await?;
    let peer = mcp::peer(&profile, &cwd).await.map_err(error)?;
    let mut params = input.params.as_object().cloned().unwrap_or_default();
    if input.method != "config/mcpServer/reload" {
        let thread = peer.thread_id().await.map_err(error)?;
        params.insert("threadId".into(), json!(thread));
    }
    let value = peer
        .rpc(&input.method, Value::Object(params))
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse { data: value }))
}
async fn requests(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, cwd) = scope(&state, query).await?;
    let peer = mcp::peer(&profile, &cwd).await.map_err(error)?;
    Ok(Json(ApiResponse {
        data: peer.snapshot().await,
    }))
}
async fn respond(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<Scope>,
    Json(input): Json<Value>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, cwd) = scope(&state, query).await?;
    mcp::peer(&profile, &cwd)
        .await
        .map_err(error)?
        .respond(&id, input)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: json!({"resolved":true}),
    }))
}
async fn disconnect(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, cwd) = scope(&state, query).await?;
    mcp::disconnect(&profile, &cwd).await.map_err(error)?;
    Ok(Json(ApiResponse {
        data: json!({"disconnected":true}),
    }))
}

#[derive(Deserialize)]
struct OAuthCallback {
    callback_url: String,
}
async fn complete_oauth(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<Scope>,
    Json(input): Json<OAuthCallback>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize_request(&headers, &state).await?;
    let (profile, cwd) = scope(&state, query).await?;
    mcp::peer(&profile, &cwd)
        .await
        .map_err(error)?
        .complete_oauth(&input.callback_url)
        .await
        .map_err(error)?;
    Ok(Json(ApiResponse {
        data: json!({"completed":true}),
    }))
}
