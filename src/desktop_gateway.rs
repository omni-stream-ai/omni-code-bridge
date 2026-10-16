//! Short-lived browser tickets exchanged for scoped HttpOnly desktop cookies.
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{Message as UpstreamMessage, client::IntoClientRequest};
use uuid::Uuid;

use crate::{
    acceptance::{RunPhase, has_controller},
    app_state::AppState,
    models::{ApiError, ApiResponse},
};

#[derive(Clone)]
struct Grant {
    run_id: String,
    client_id: String,
    auth: HeaderMap,
    generation: u64,
    expires: DateTime<Utc>,
    ticket: bool,
}

#[derive(Default)]
pub struct DesktopGateway {
    grants: Mutex<HashMap<String, Grant>>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v2/acceptance/{id}/desktop-ticket", post(issue_ticket))
        .route("/v2/acceptance/{id}/desktop/", get(desktop_root))
        .route("/v2/acceptance/{id}/desktop/{*path}", get(desktop_asset))
}

fn error(status: StatusCode, message: impl Into<String>) -> ApiError {
    ApiError {
        status,
        message: message.into(),
    }
}

fn secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

async fn issue_ticket(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let client = headers
        .get("x-omni-code-client-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "missing client id"))?;
    let run = state
        .acceptance
        .get(&id)
        .await
        .map_err(|e| error(StatusCode::NOT_FOUND, e.to_string()))?;
    let run = run.lock().await;
    if run.phase != RunPhase::Running
        || !has_controller(&run, Utc::now())
        || run.controller.as_deref() != Some(client)
    {
        return Err(error(
            StatusCode::CONFLICT,
            "Acquire desktop control before opening the desktop",
        ));
    }
    let mut auth = HeaderMap::new();
    for key in [header::AUTHORIZATION.as_str(), "x-omni-code-client-id"] {
        if let Some(value) = headers.get(key) {
            auth.insert(
                axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                value.clone(),
            );
        }
    }
    let grant = Grant {
        run_id: id.clone(),
        client_id: client.into(),
        auth,
        generation: run.control_generation,
        expires: Utc::now() + chrono::Duration::seconds(30),
        ticket: true,
    };
    let token = secret();
    let mut grants = state.desktop_gateway.grants.lock().await;
    grants.retain(|_, grant| grant.expires > Utc::now());
    if grants.len() >= 1024 {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many desktop sessions",
        ));
    }
    grants.insert(token.clone(), grant);
    Ok(Json(ApiResponse {
        data: json!({"url": format!("/v2/acceptance/{id}/desktop/?ticket={token}"), "expires_in": 30}),
    }))
}

#[derive(Deserialize, Default)]
struct DesktopQuery {
    ticket: Option<String>,
}

async fn desktop_root(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<DesktopQuery>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Result<Response, ApiError> {
    if let Some(token) = query.ticket {
        let mut grant = state
            .desktop_gateway
            .grants
            .lock()
            .await
            .remove(&token)
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Invalid or used desktop ticket"))?;
        if !grant.ticket || grant.run_id != id || grant.expires <= Utc::now() {
            return Err(error(StatusCode::UNAUTHORIZED, "Expired desktop ticket"));
        }
        validate_grant(&state, &grant).await?;
        grant.ticket = false;
        grant.expires = Utc::now() + chrono::Duration::hours(1);
        let cookie = secret();
        state
            .desktop_gateway
            .grants
            .lock()
            .await
            .insert(cookie.clone(), grant);
        let base = format!("/v2/acceptance/{id}/desktop/");
        let location =
            format!("{base}index.html?path=v2%2Facceptance%2F{id}%2Fdesktop%2F&autoconnect=true");
        let secure = headers
            .get("x-forwarded-proto")
            .is_some_and(|v| v == "https");
        let cookie = format!(
            "omni_desktop={cookie}; Path={base}; HttpOnly; SameSite=Strict; Max-Age=3600{}",
            if secure { "; Secure" } else { "" }
        );
        return Ok((
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, location),
                (header::SET_COOKIE, cookie),
                (header::CACHE_CONTROL, "no-store".into()),
                (header::REFERRER_POLICY, "no-referrer".into()),
            ],
        )
            .into_response());
    }
    proxy(state, id, "index.html".into(), headers, ws.ok()).await
}

async fn desktop_asset(
    State(state): State<Arc<AppState>>,
    Path((id, path)): Path<(String, String)>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Result<Response, ApiError> {
    proxy(state, id, path, headers, ws.ok()).await
}

async fn browser_grant(state: &AppState, id: &str, headers: &HeaderMap) -> Result<Grant, ApiError> {
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(';')
                .find_map(|part| part.trim().strip_prefix("omni_desktop="))
        })
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Desktop cookie is required"))?;
    let grant = state
        .desktop_gateway
        .grants
        .lock()
        .await
        .get(cookie)
        .cloned()
        .ok_or_else(|| {
            error(
                StatusCode::UNAUTHORIZED,
                "Desktop session expired; reopen it from Omni Code",
            )
        })?;
    if grant.ticket || grant.run_id != id || grant.expires <= Utc::now() {
        return Err(error(StatusCode::UNAUTHORIZED, "Desktop session expired"));
    }
    // Reject cross-origin WebSocket use even when a browser supplies cookies.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin
            .to_str()
            .ok()
            .and_then(|s| reqwest::Url::parse(s).ok());
        let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
        if origin
            .as_ref()
            .map(|url| url[url::Position::BeforeHost..url::Position::AfterPort].to_owned())
            .as_deref()
            != host
        {
            return Err(error(StatusCode::FORBIDDEN, "Desktop origin mismatch"));
        }
    }
    Ok(grant)
}

async fn validate_grant(state: &AppState, grant: &Grant) -> Result<u16, ApiError> {
    if grant.expires <= Utc::now() {
        return Err(error(StatusCode::UNAUTHORIZED, "Desktop session expired"));
    }
    crate::api::authorize_request(&grant.auth, state).await?;
    let shared = state
        .acceptance
        .get(&grant.run_id)
        .await
        .map_err(|e| error(StatusCode::NOT_FOUND, e.to_string()))?;
    let run = shared.lock().await;
    if run.phase != RunPhase::Running
        || run.expires_at <= Utc::now()
        || !has_controller(&run, Utc::now())
        || run.controller.as_ref() != Some(&grant.client_id)
        || run.control_generation != grant.generation
    {
        return Err(error(
            StatusCode::CONFLICT,
            "Desktop control was released or expired",
        ));
    }
    run.desktop_port
        .ok_or_else(|| error(StatusCode::CONFLICT, "Desktop is not ready"))
}

fn validate_asset_path(path: &str) -> Result<(), ApiError> {
    if path.starts_with('/')
        || path.contains('\\')
        || path.contains('%')
        || path.contains('?')
        || path.contains('#')
        || path.split('/').any(|part| part == ".." || part == ".")
    {
        return Err(error(StatusCode::BAD_REQUEST, "Invalid desktop asset path"));
    }
    Ok(())
}

async fn proxy(
    state: Arc<AppState>,
    id: String,
    path: String,
    headers: HeaderMap,
    ws: Option<WebSocketUpgrade>,
) -> Result<Response, ApiError> {
    validate_asset_path(&path)?;
    let grant = browser_grant(&state, &id, &headers).await?;
    let port = validate_grant(&state, &grant).await?;
    if let Some(ws) = ws {
        let mut request = format!("ws://127.0.0.1:{port}/")
            .into_client_request()
            .map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("binary"));
        let (upstream, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(request),
        )
        .await
        .map_err(|_| error(StatusCode::GATEWAY_TIMEOUT, "Desktop connection timed out"))?
        .map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
        return Ok(ws
            .protocols(["binary"])
            .max_message_size(16 * 1024 * 1024)
            .on_upgrade(move |socket| relay(state, grant, socket, upstream))
            .into_response());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let response = client
        .get(format!("http://127.0.0.1:{port}/{path}"))
        .send()
        .await
        .map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or(HeaderValue::from_static("application/octet-stream"));
    let mut output = Response::new(Body::from_stream(response.bytes_stream()));
    *output.status_mut() = status;
    output
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    output.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    output.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    output.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    Ok(output)
}

async fn relay(
    state: Arc<AppState>,
    grant: Grant,
    mut client: WebSocket,
    mut upstream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if validate_grant(&state, &grant).await.is_err() { break; }
                if state.acceptance.renew_control(&grant.run_id, &grant.client_id, grant.generation).await.is_err() { break; }
            }
            message = client.next() => {
                // Revalidate every input frame. UI flags alone never grant control.
                if validate_grant(&state, &grant).await.is_err() { break; }
                let message = match message {
                    Some(Ok(Message::Binary(data))) => UpstreamMessage::Binary(data),
                    Some(Ok(Message::Text(data))) => UpstreamMessage::Text(data.to_string().into()),
                    Some(Ok(Message::Ping(data))) => UpstreamMessage::Ping(data),
                    Some(Ok(Message::Pong(data))) => UpstreamMessage::Pong(data),
                    _ => break,
                };
                let shared = match state.acceptance.get(&grant.run_id).await { Ok(run) => run, Err(_) => break };
                let run = shared.lock().await;
                if run.phase != RunPhase::Running || !has_controller(&run, Utc::now())
                    || run.controller.as_ref() != Some(&grant.client_id) || run.control_generation != grant.generation { break; }
                if !matches!(tokio::time::timeout(Duration::from_secs(2), upstream.send(message)).await, Ok(Ok(()))) { break; }
            }
            message = upstream.next() => {
                let message = match message {
                    Some(Ok(UpstreamMessage::Binary(data))) => Message::Binary(data),
                    Some(Ok(UpstreamMessage::Text(data))) => Message::Text(data.to_string().into()),
                    Some(Ok(UpstreamMessage::Ping(data))) => Message::Ping(data),
                    Some(Ok(UpstreamMessage::Pong(data))) => Message::Pong(data),
                    Some(Ok(UpstreamMessage::Frame(_))) => continue,
                    _ => break,
                };
                if !matches!(tokio::time::timeout(Duration::from_secs(2), client.send(message)).await, Ok(Ok(()))) { break; }
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), client.close()).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), upstream.close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gateway_paths_cannot_escape_the_upstream_root() {
        for path in [
            "../status",
            "%2e%2e/status",
            "a/../b",
            "/control",
            "a?url=other",
            "a\\b",
        ] {
            assert!(validate_asset_path(path).is_err());
        }
        assert!(validate_asset_path("js/Client.js").is_ok());
    }
}
