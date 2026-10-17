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
    artifact: Option<String>,
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
        .route(
            "/v2/acceptance/{id}/evidence-ticket/{name}",
            post(issue_evidence_ticket),
        )
        .route("/v2/acceptance/{id}/evidence/{name}", get(evidence))
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
        artifact: None,
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
        // A fresh ticket replaces any cookie from an earlier desktop session.
        let mut grant = state
            .desktop_gateway
            .grants
            .lock()
            .await
            .remove(&token)
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Invalid or used desktop ticket"))?;
        if !grant.ticket
            || grant.artifact.is_some()
            || grant.run_id != id
            || grant.expires <= Utc::now()
        {
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
        let secure = headers
            .get("x-forwarded-proto")
            .is_some_and(|v| v == "https");
        let set_cookie = format!(
            "omni_desktop={cookie}; Path={base}; HttpOnly; SameSite=Strict; Max-Age=3600{}",
            if secure { "; Secure" } else { "" }
        );
        let location = format!("{base}viewer.html");
        return Ok((
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, location),
                (header::SET_COOKIE, set_cookie),
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
    if grant.ticket || grant.artifact.is_some() || grant.run_id != id || grant.expires <= Utc::now()
    {
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

async fn issue_evidence_ticket(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    state
        .acceptance
        .artifact(&id, &name)
        .await
        .map_err(|e| error(StatusCode::NOT_FOUND, e.to_string()))?;
    let mut auth = HeaderMap::new();
    for key in [header::AUTHORIZATION.as_str(), "x-omni-code-client-id"] {
        if let Some(value) = headers.get(key) {
            auth.insert(
                axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                value.clone(),
            );
        }
    }
    let token = secret();
    let mut grants = state.desktop_gateway.grants.lock().await;
    grants.retain(|_, grant| grant.expires > Utc::now());
    if grants.len() >= 1024 {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many browser sessions",
        ));
    }
    grants.insert(
        token.clone(),
        Grant {
            run_id: id.clone(),
            client_id: String::new(),
            auth,
            generation: 0,
            expires: Utc::now() + chrono::Duration::seconds(30),
            ticket: true,
            artifact: Some(name.clone()),
        },
    );
    let name: String = url::form_urlencoded::byte_serialize(name.as_bytes()).collect();
    Ok(Json(ApiResponse {
        data: json!({"url":format!("/v2/acceptance/{id}/evidence/{name}?ticket={token}"),"expires_in":30}),
    }))
}

#[derive(Deserialize, Default)]
struct EvidenceQuery {
    ticket: Option<String>,
    #[serde(default)]
    download: bool,
}

async fn evidence(
    State(state): State<Arc<AppState>>,
    Path((id, name)): Path<(String, String)>,
    Query(query): Query<EvidenceQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(ticket) = query.ticket {
        let mut grant = state
            .desktop_gateway
            .grants
            .lock()
            .await
            .remove(&ticket)
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Invalid or used evidence ticket"))?;
        if !grant.ticket
            || grant.run_id != id
            || grant.artifact.as_deref() != Some(&name)
            || grant.expires <= Utc::now()
        {
            return Err(error(StatusCode::UNAUTHORIZED, "Invalid evidence ticket"));
        }
        crate::api::authorize_request(&grant.auth, &state).await?;
        grant.ticket = false;
        grant.expires = Utc::now() + chrono::Duration::hours(1);
        let cookie = secret();
        state
            .desktop_gateway
            .grants
            .lock()
            .await
            .insert(cookie.clone(), grant);
        let base = format!("/v2/acceptance/{id}/evidence/");
        let name: String = url::form_urlencoded::byte_serialize(name.as_bytes()).collect();
        let location = format!("{base}{name}?download={}", query.download);
        let secure = headers
            .get("x-forwarded-proto")
            .is_some_and(|v| v == "https");
        // A per-file cookie allows concurrent playback/download of different artifacts.
        let cookie = format!(
            "omni_evidence={cookie}; Path={base}{name}; HttpOnly; SameSite=Strict; Max-Age=3600{}",
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
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(';')
                .find_map(|v| v.trim().strip_prefix("omni_evidence="))
        })
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Evidence cookie is required"))?;
    let grant = state
        .desktop_gateway
        .grants
        .lock()
        .await
        .get(cookie)
        .cloned()
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "Evidence session expired"))?;
    if grant.ticket
        || grant.run_id != id
        || grant.artifact.as_deref() != Some(&name)
        || grant.expires <= Utc::now()
    {
        return Err(error(StatusCode::UNAUTHORIZED, "Evidence session expired"));
    }
    crate::api::authorize_request(&grant.auth, &state).await?;
    crate::acceptance_api::artifact_response(&state, &id, &name, &headers, query.download).await
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
    if path == "viewer.html" {
        let shared = state
            .acceptance
            .get(&id)
            .await
            .map_err(|e| error(StatusCode::NOT_FOUND, e.to_string()))?;
        let run = shared.lock().await;
        let (width, height) = match run.size.as_str() {
            "mobile" => (390, 844),
            "tablet" => (834, 1112),
            "laptop" => (1366, 768),
            _ => (1440, 900),
        };
        let html = include_str!("../plugins/x11box-acceptance/desktop-viewer.html")
            .replace("__DESKTOP_WIDTH__", &width.to_string())
            .replace("__DESKTOP_HEIGHT__", &height.to_string());
        return Ok((
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "private, no-store"),
                (header::REFERRER_POLICY, "no-referrer"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            html,
        )
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

    #[tokio::test]
    async fn fresh_desktop_ticket_replaces_stale_cookie_and_stays_single_use() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let shared = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        {
            let mut run = shared.lock().await;
            run.phase = RunPhase::Running;
            run.desktop_port = Some(1);
        }
        fixture
            .state
            .acceptance
            .control(&fixture.id, "acceptance-test", true)
            .await
            .unwrap();
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let base = format!("{origin}/v2/acceptance/{}", fixture.id);
        let mut cookie = "omni_desktop=stale".to_string();
        for _ in 0..2 {
            let issued: Value = client
                .post(format!("{base}/desktop-ticket"))
                .headers(fixture.headers.clone())
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            let url = format!("{origin}{}", issued["data"]["url"].as_str().unwrap());
            let exchange = client
                .get(&url)
                .header(header::COOKIE, &cookie)
                .send()
                .await
                .unwrap();
            assert_eq!(exchange.status(), StatusCode::SEE_OTHER);
            assert!(
                exchange.headers()[header::LOCATION]
                    .to_str()
                    .unwrap()
                    .ends_with("/desktop/viewer.html")
            );
            let next_cookie = exchange.headers()[header::SET_COOKIE].to_str().unwrap();
            assert!(next_cookie.contains("HttpOnly; SameSite=Strict"));
            let next_cookie = next_cookie.split(';').next().unwrap().to_string();
            assert_ne!(next_cookie, cookie);
            let viewer = format!("{base}/desktop/viewer.html");
            assert_eq!(
                client.get(&viewer).send().await.unwrap().status(),
                StatusCode::UNAUTHORIZED
            );
            let response = client
                .get(&viewer)
                .header(header::COOKIE, &next_cookie)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            let html = response.text().await.unwrap();
            assert!(html.contains("const width = 390;"));
            assert!(!html.contains("__DESKTOP_"));
            assert_eq!(
                client
                    .get(&url)
                    .header(header::COOKIE, &next_cookie)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
            // Closing and reopening must exchange the new ticket, even when
            // the WebView still holds the previous session's cookie.
            fixture
                .state
                .acceptance
                .control(&fixture.id, "acceptance-test", false)
                .await
                .unwrap();
            assert_eq!(
                client
                    .get(&viewer)
                    .header(header::COOKIE, &next_cookie)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::CONFLICT
            );
            fixture
                .state
                .acceptance
                .control(&fixture.id, "acceptance-test", true)
                .await
                .unwrap();
            cookie = next_cookie;
        }
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires X11BOX_TEST_IMAGE, Playwright and a Chromium executable"]
    async fn real_mobile_gateway_touch_input_and_revocation() {
        use crate::{
            acceptance::{AcceptanceStore, CreateAcceptance},
            runtime_plugin::{
                InstallPhase, RuntimeConfig, RuntimePluginStore, atomic_json, run_bounded,
            },
        };
        use std::path::Path as FsPath;
        let mut fixture = crate::acceptance_test_support::Fixture::new().await;
        let runtime = Arc::new(RuntimePluginStore::new(
            fixture.root.join("browser-runtime"),
            RuntimeConfig {
                image: Some(std::env::var("X11BOX_TEST_IMAGE").expect("set X11BOX_TEST_IMAGE")),
                source_dir: None,
                docker: "docker".into(),
            },
        ));
        let state = Arc::get_mut(&mut fixture.state).unwrap();
        state.runtime_plugins = Arc::clone(&runtime);
        state.acceptance = Arc::new(AcceptanceStore::new(Arc::clone(&runtime)));
        runtime.install().await.unwrap();
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                let status = runtime.status().await.unwrap();
                if status.phase == InstallPhase::Ready {
                    break;
                }
                assert_ne!(status.phase, InstallPhase::Failed, "{:?}", status.error);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap();
        let project = fixture.root.join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("index.html"),r#"<!doctype html><meta charset="utf-8"><title>Mobile fixture</title><style>body{margin:0}input{box-sizing:border-box;width:100%;height:80vh;font-size:24px;background:#e4f7ff}</style><input autofocus placeholder="点击并输入中文" onclick="document.title='Touched'" oninput="document.title='Input:'+this.value">"#).unwrap();
        for args in [
            vec!["init", "--quiet"],
            vec!["add", "index.html"],
            vec![
                "-c",
                "user.name=Acceptance Test",
                "-c",
                "user.email=acceptance@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "fixture",
            ],
        ] {
            let output = tokio::process::Command::new("git")
                .arg("-C")
                .arg(&project)
                .args(args)
                .output()
                .await
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let id = Uuid::new_v4().to_string();
        fixture
            .state
            .acceptance
            .create(
                CreateAcceptance {
                    id: id.clone(),
                    project_id: "browser-fixture".into(),
                    session_id: None,
                    command: "chromium --no-sandbox --new-window file:///workspace/index.html"
                        .into(),
                    size: "pc".into(),
                },
                &project,
                "acceptance-test",
            )
            .await
            .unwrap();
        let shared = fixture.state.acceptance.get(&id).await.unwrap();
        let running = tokio::time::timeout(Duration::from_secs(150), async {
            loop {
                let run = shared.lock().await.clone();
                if run.phase != RunPhase::Starting {
                    break run;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(running.phase, RunPhase::Running, "{:?}", running.error);
        let (origin, server) = fixture.server().await;
        let config = fixture.root.join("browser.json");
        let headers: HashMap<String, String> = fixture
            .headers
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        atomic_json(
            &config,
            &json!({"origin":origin,"base":format!("{origin}/v2/acceptance/{id}"),
            "headers":headers,"screenshot":"/tmp/omni-acceptance-mobile-proof.png"}),
        )
        .unwrap();
        let result = run_bounded(
            FsPath::new("node"),
            &["tests/browser/mobile-gateway.cjs", config.to_str().unwrap()],
            Duration::from_secs(90),
        )
        .await;
        let stopped = fixture.state.acceptance.stop(&id, None).await;
        server.abort();
        assert!(result.is_ok(), "{result:?}");
        assert!(stopped.unwrap().error.is_none());
    }

    #[tokio::test]
    async fn evidence_tickets_are_single_use_scoped_expiring_and_revocable() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let (url, server) = fixture.server().await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let issued: Value = client
            .post(format!(
                "{url}/v2/acceptance/{}/evidence-ticket/recording.mp4",
                fixture.id
            ))
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ticket_url = format!("{url}{}", issued["data"]["url"].as_str().unwrap());
        let exchange = client.get(&ticket_url).send().await.unwrap();
        assert_eq!(exchange.status(), StatusCode::SEE_OTHER);
        let cookie = exchange.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let evidence_url = format!("{url}{}", exchange.headers()["location"].to_str().unwrap());
        assert_eq!(
            client.get(&ticket_url).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let response = client
            .get(&evidence_url)
            .header("cookie", &cookie)
            .header("range", "bytes=-3")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"789");
        let other = evidence_url.replace("recording.mp4", "report.json");
        assert_eq!(
            client
                .get(other)
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let other_run = evidence_url.replace(&fixture.id, &Uuid::new_v4().to_string());
        assert_eq!(
            client
                .get(other_run)
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let desktop = format!("{url}/v2/acceptance/{}/desktop/", fixture.id);
        assert_eq!(
            client
                .get(desktop)
                .header("cookie", cookie.replace("omni_evidence", "omni_desktop"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let token = cookie.strip_prefix("omni_evidence=").unwrap();
        {
            let mut grants = fixture.state.desktop_gateway.grants.lock().await;
            grants.get_mut(token).unwrap().expires = Utc::now() - chrono::Duration::seconds(1);
        }
        assert_eq!(
            client
                .get(&evidence_url)
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        {
            let mut grants = fixture.state.desktop_gateway.grants.lock().await;
            let grant = grants.get_mut(token).unwrap();
            grant.expires = Utc::now() + chrono::Duration::hours(1);
            grant.auth.insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer revoked"),
            );
        }
        assert_eq!(
            client
                .get(evidence_url)
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        server.abort();
    }
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
