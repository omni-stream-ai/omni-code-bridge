use crate::{
    protocol::{forward_headers, Frame, Headers, Open, MAX_CHUNK, MAX_HEAD},
    util::validate_relay_url,
};
use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{Message, WebSocketConfig},
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub relay: String,
    pub service_id: String,
    pub connector_token: String,
    pub kind: String,
    pub target: String,
    pub upstream_token: Option<String>,
    pub upstream_client_id: Option<String>,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                "connector config contains credentials; use chmod 600"
            );
        }
        let config: Self = toml::from_str(&std::fs::read_to_string(path)?).map_err(|_| {
            anyhow::anyhow!("invalid connector configuration; check TOML syntax and fields")
        })?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        validate_relay_url(&self.relay)?;
        let target = url::Url::parse(&self.target)?;
        let local = target.host_str().is_some_and(|h| {
            h == "localhost"
                || h.parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        ensure!(
            target.scheme() == "http"
                && local
                && target.username().is_empty()
                && target.password().is_none()
                && target.query().is_none()
                && target.fragment().is_none()
                && target.path() == "/",
            "target must be an HTTP loopback origin without credentials or path"
        );
        ensure!(
            matches!(self.kind.as_str(), "omnistream" | "omni-code" | "http"),
            "invalid service kind"
        );
        if self.kind != "http" {
            ensure!(
                self.upstream_token.as_ref().is_some_and(|v| !v.is_empty()),
                "Core/Bridge requires an upstream credential"
            );
        }
        if self.kind == "omni-code" {
            ensure!(
                self.upstream_client_id
                    .as_ref()
                    .is_some_and(|v| !v.is_empty()),
                "Omni Code requires upstream_client_id"
            );
        }
        Ok(())
    }
    fn websocket_request(&self, path: &str) -> Result<http::Request<()>> {
        let mut url = validate_relay_url(&self.relay)?.join(path)?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme).unwrap();
        let mut request = url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", self.connector_token).parse()?,
        );
        request
            .headers_mut()
            .insert("x-relay-protocol", "2".parse()?);
        Ok(request)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorState {
    Connecting,
    Online,
    Backoff,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorStatus {
    pub service_id: String,
    pub state: ConnectorState,
    pub retry_in_ms: Option<u64>,
}

pub struct ConnectorHandle {
    cancel: CancellationToken,
    status: watch::Receiver<ConnectorStatus>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl ConnectorHandle {
    pub fn subscribe(&self) -> watch::Receiver<ConnectorStatus> {
        self.status.clone()
    }
    pub fn status(&self) -> ConnectorStatus {
        self.status.borrow().clone()
    }
    pub fn stop(&self) {
        self.cancel.cancel();
    }
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for ConnectorHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Start using the caller's Tokio runtime; no global runtime or signal handler is installed.
pub fn start(config: Config) -> Result<ConnectorHandle> {
    config.validate()?;
    ensure!(
        !config.service_id.is_empty() && !config.connector_token.is_empty(),
        "service enrollment is required"
    );
    let runtime =
        tokio::runtime::Handle::try_current().context("connector requires a Tokio runtime")?;
    let initial = ConnectorStatus {
        service_id: config.service_id.clone(),
        state: ConnectorState::Connecting,
        retry_in_ms: None,
    };
    let (status_tx, status) = watch::channel(initial);
    let cancel = CancellationToken::new();
    let child = cancel.clone();
    let task = runtime.spawn(async move {
        tokio::select! { _ = child.cancelled() => {}, _ = run(config, &status_tx) => {} }
        status_tx.send_modify(|s| {
            s.state = ConnectorState::Stopped;
            s.retry_in_ms = None;
        });
    });
    Ok(ConnectorHandle {
        cancel,
        status,
        task: Some(task),
    })
}

async fn run(config: Config, status: &watch::Sender<ConnectorStatus>) {
    let config = Arc::new(config);
    let mut delay = 1_u64;
    loop {
        let started = Instant::now();
        status.send_modify(|s| {
            s.state = ConnectorState::Connecting;
            s.retry_in_ms = None;
        });
        let _ = connected(config.clone(), status).await;
        if started.elapsed() > Duration::from_secs(60) {
            delay = 1;
        }
        // No requests are replayed after reconnection.
        let wait =
            Duration::from_secs(delay) + Duration::from_millis(rand::random::<u16>() as u64 % 1000);
        status.send_modify(|s| {
            s.state = ConnectorState::Backoff;
            s.retry_in_ms = Some(wait.as_millis() as u64);
        });
        tokio::time::sleep(wait).await;
        delay = (delay * 2).min(30);
    }
}

async fn connected(config: Arc<Config>, status: &watch::Sender<ConnectorStatus>) -> Result<()> {
    let request = config.websocket_request("/v1/tunnel/control")?;
    let (socket, _) =
        tokio::time::timeout(Duration::from_secs(15), connect_async(request)).await??;
    status.send_modify(|s| {
        s.state = ConnectorState::Online;
        s.retry_in_ms = None;
    });
    let (mut tx, mut rx) = socket.split();
    let mut tasks = tokio::task::JoinSet::new();
    let permits = Arc::new(tokio::sync::Semaphore::new(64));
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    let mut last_seen = Instant::now();
    loop {
        tokio::select! {
            _=heartbeat.tick()=>{
                if last_seen.elapsed()>Duration::from_secs(45){break}
                if tx.send(Message::Ping(Vec::new().into())).await.is_err(){break}
            },
            _=tasks.join_next(),if !tasks.is_empty()=>{},
            incoming=rx.next()=>match incoming {
                Some(Ok(Message::Text(text)))=>{
                    last_seen=Instant::now();
                    let open:Open=serde_json::from_str(&text)?;
                    if let Ok(permit)=permits.clone().try_acquire_owned() {
                        let config=config.clone();
                        tasks.spawn(async move {let _permit=permit;let _ = transfer(&config,&open.id).await;});
                    }
                },
                Some(Ok(Message::Ping(_)|Message::Pong(_)))=>last_seen=Instant::now(),
                _=>break,
            }
        }
    }
    tasks.abort_all();
    Ok(())
}

async fn transfer(config: &Config, id: &str) -> Result<()> {
    let mut request = config.websocket_request("/v1/tunnel/data")?;
    request
        .headers_mut()
        .insert("x-relay-stream-id", id.parse()?);
    let mut limits = WebSocketConfig::default();
    limits.max_message_size = Some(MAX_CHUNK);
    limits.max_frame_size = Some(MAX_CHUNK);
    let (socket, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async_with_config(request, Some(limits), false),
    )
    .await??;
    let (mut tx, mut rx) = socket.split();
    let first = tokio::time::timeout(Duration::from_secs(15), rx.next())
        .await?
        .context("missing request")??;
    let Message::Text(text) = first else {
        bail!("invalid request")
    };
    ensure!(text.len() <= MAX_HEAD, "request head too large");
    let Frame::Request {
        method,
        path,
        headers,
        websocket,
    } = serde_json::from_str::<Frame>(&text)?
    else {
        bail!("missing request head")
    };
    ensure!(
        path.starts_with('/') && !path.starts_with("//") && !path.contains('#'),
        "invalid upstream path"
    );
    let mut url = url::Url::parse(&format!("{}{}", config.target.trim_end_matches('/'), path))?;
    let headers = upstream_headers(config, headers)?;
    if websocket {
        url.set_scheme("ws").unwrap();
        let mut request = url.as_str().into_client_request()?;
        for (name, value) in headers.iter() {
            request.headers_mut().append(name, value.clone());
        }
        let upstream = tokio::time::timeout(
            Duration::from_secs(15),
            tokio_tungstenite::connect_async_with_config(request, Some(limits), false),
        )
        .await?;
        let (upstream, response) = match upstream {
            Ok(result) => result,
            Err(_) => {
                tx.send(Message::Text(serde_json::to_string(&Frame::Error)?.into()))
                    .await?;
                return Ok(());
            }
        };
        let head = Frame::Response {
            status: 101,
            headers: forward_headers(response.headers(), false),
        };
        tx.send(Message::Text(serde_json::to_string(&head)?.into()))
            .await?;
        let (mut up_tx, mut up_rx) = upstream.split();
        loop {
            tokio::select! {
                m=rx.next()=>match m {Some(Ok(m))=>{let close=m.is_close();if up_tx.send(m).await.is_err()||close{break}},_=>break},
                m=up_rx.next()=>match m {Some(Ok(m))=>{let close=m.is_close();if tx.send(m).await.is_err()||close{break}},_=>break},
            }
        }
        return Ok(());
    }
    // Chunked request bodies and responses keep uploads and SSE bounded in memory.
    let tx = Arc::new(tokio::sync::Mutex::new(tx));
    let reader_tx = tx.clone();
    let window = Arc::new(tokio::sync::Semaphore::new(4));
    let reader_window = window.clone();
    let (body_tx, mut body_rx) = tokio::sync::mpsc::channel(4);
    let closed = tokio_util::sync::CancellationToken::new();
    let reader_closed = closed.clone();
    let reader = tokio::spawn(async move {
        let mut body_tx = Some(body_tx);
        while let Some(Ok(message)) = rx.next().await {
            match message {
                Message::Binary(bytes) => match &body_tx {
                    Some(sender) => {
                        if sender.send(bytes).await.is_err() {
                            break;
                        }
                        if reader_tx
                            .lock()
                            .await
                            .send(Message::Text(
                                serde_json::to_string(&Frame::Ack).unwrap().into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    None => break,
                },
                Message::Text(text)
                    if serde_json::from_str::<Frame>(&text)
                        .is_ok_and(|f| matches!(f, Frame::End)) =>
                {
                    body_tx.take();
                }
                Message::Text(text)
                    if serde_json::from_str::<Frame>(&text)
                        .is_ok_and(|f| matches!(f, Frame::Ack)) =>
                {
                    if reader_window.available_permits() >= 4 {
                        break;
                    }
                    reader_window.add_permits(1);
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => break,
            }
        }
        reader_closed.cancel();
    });
    let _reader = AbortOnDrop(reader.abort_handle());
    let response_complete = std::sync::atomic::AtomicBool::new(false);
    let upstream = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        let request = client
            .request(method.parse::<reqwest::Method>()?, url)
            .headers(headers);
        // Core's current HTTP server requires Content-Length. It already
        // buffers requests with a 32 MiB limit; preserve that bound here.
        // Bridge continues streaming uploads with bounded chunk queues.
        let request = if config.kind == "omnistream" {
            let mut body = Vec::new();
            while let Some(bytes) = body_rx.recv().await {
                ensure!(
                    body.len() + bytes.len() <= 32 * 1024 * 1024,
                    "Core request too large"
                );
                body.extend_from_slice(&bytes);
            }
            request.body(body)
        } else {
            let body = async_stream::stream! {
                while let Some(bytes)=body_rx.recv().await {yield Ok::<_,std::io::Error>(bytes);}
            };
            request.body(reqwest::Body::wrap_stream(body))
        };
        let response = request.send().await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tx.lock()
                    .await
                    .send(Message::Text(serde_json::to_string(&Frame::Error)?.into()))
                    .await?;
                return Ok(());
            }
        };
        let head = Frame::Response {
            status: response.status().as_u16(),
            headers: forward_headers(response.headers(), false),
        };
        ensure!(
            serde_json::to_vec(&head)?.len() <= MAX_HEAD,
            "response head too large"
        );
        tx.lock()
            .await
            .send(Message::Text(serde_json::to_string(&head)?.into()))
            .await?;
        let mut stream = response.bytes_stream();
        while let Some(bytes) = stream.next().await {
            for chunk in bytes?.chunks(MAX_CHUNK) {
                window.acquire().await?.forget();
                tx.lock()
                    .await
                    .send(Message::Binary(chunk.to_vec().into()))
                    .await?;
            }
        }
        tx.lock()
            .await
            .send(Message::Text(serde_json::to_string(&Frame::End)?.into()))
            .await?;
        response_complete.store(true, std::sync::atomic::Ordering::Release);
        // The relay owns the response drain. Keep the carrier alive until it
        // closes after End and buffered delivery, or cancels the client lease.
        closed.cancelled().await;
        Ok(())
    };
    tokio::select! {
        _=closed.cancelled()=>{
            if response_complete.load(std::sync::atomic::Ordering::Acquire) { Ok(()) }
            else { bail!("relay disconnected") }
        },
        result=upstream=>result,
    }
}

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn upstream_headers(config: &Config, headers: Headers) -> Result<http::HeaderMap> {
    let mut parsed = http::HeaderMap::new();
    for (name, value) in headers {
        parsed.append(
            http::HeaderName::try_from(name)?,
            http::HeaderValue::try_from(value)?,
        );
    }
    let sanitized = forward_headers(&parsed, true);
    let mut headers = http::HeaderMap::new();
    for (name, value) in sanitized {
        headers.append(http::HeaderName::try_from(name)?, value.parse()?);
    }
    if let Some(token) = &config.upstream_token {
        headers.insert("authorization", format!("Bearer {token}").parse()?);
    }
    // Browser Origin is checked by Relay. Core/Bridge receive a native request
    // authenticated with their dedicated credential, without browser cookies.
    if config.kind != "http" {
        headers.remove("origin");
    }
    if let Some(id) = &config.upstream_client_id {
        headers.insert("x-omni-code-client-id", id.parse()?);
    }
    Ok(headers)
}
