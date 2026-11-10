use crate::{api::authorize_request, app_state::AppState};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::any,
};
use omni_relay_connector::{Host, UPSTREAM_CLIENT_ID};
use std::sync::Arc;

pub fn host(port: u16) -> anyhow::Result<Arc<Host>> {
    let root = crate::bridge_settings::settings_path()
        .parent()
        .ok_or_else(|| anyhow::anyhow!("settings directory missing"))?
        .join("relay");
    Ok(Arc::new(Host::open(
        &root,
        "omni-code",
        &format!("http://127.0.0.1:{port}"),
        "Omni Code Bridge",
    )?))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/relay/{*operation}", any(control))
}
fn is_tunnel(headers: &HeaderMap) -> bool {
    headers
        .get("x-omni-code-client-id")
        .is_some_and(|value| value == UPSTREAM_CLIENT_ID)
}
pub async fn guard(request: Request, next: Next) -> Response {
    let path = request.uri().path();
    if is_tunnel(request.headers())
        && (path == "/relay"
            || path.starts_with("/relay/")
            || path == "/client-auth"
            || path.starts_with("/client-auth/"))
    {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error":"Relay clients cannot manage host credentials or remote access"}))).into_response();
    }
    next.run(request).await
}
async fn control(
    State(state): State<Arc<AppState>>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = authorize_request(&headers, &state).await {
        return error.into_response();
    }
    if is_tunnel(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(host) = &state.relay else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match host.request(method.as_str(), uri.path(), &body).await {
        Ok(value) => Json(value).into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"Relay operation failed; check login, network and configuration"}))).into_response(),
    }
}
