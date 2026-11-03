//! Codex MCP management. Peers stay alive across HTTP requests (notably OAuth).
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Mutex, mpsc, oneshot},
};

// Observations from the app-server actually executing chat turns, never from
// the independent management/test peer. Keep a bounded last-turn snapshot.
fn session_runtimes() -> &'static std::sync::Mutex<HashMap<String, Value>> {
    static RUNTIMES: OnceLock<std::sync::Mutex<HashMap<String, Value>>> = OnceLock::new();
    RUNTIMES.get_or_init(Default::default)
}
pub fn record_session_runtime(session: &str, servers: Value) {
    let mut runtimes = session_runtimes().lock().unwrap_or_else(|e| e.into_inner());
    if runtimes.len() >= 128 && !runtimes.contains_key(session) {
        if let Some(key) = runtimes.keys().next().cloned() {
            runtimes.remove(&key);
        }
    }
    runtimes.insert(
        session.to_owned(),
        json!({"servers": servers,
        "observedAt": chrono::Utc::now().to_rfc3339()}),
    );
}
pub fn session_runtime(session: Option<&str>) -> Value {
    session
        .and_then(|id| {
            session_runtimes()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(id)
                .cloned()
        })
        .unwrap_or(Value::Null)
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct Scope {
    #[serde(default = "default_profile")]
    pub profile_id: String,
    pub session_id: Option<String>,
}
fn default_profile() -> String {
    "default".into()
}

pub fn config_path(profile: &str) -> Result<PathBuf> {
    if !crate::session_store::codex_profile_summaries()
        .iter()
        .any(|p| p.id == profile)
    {
        bail!("Unknown Codex profile: {profile}");
    }
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"));
    Ok(home.join(if profile == "default" {
        "config.toml".into()
    } else {
        format!("{profile}.config.toml")
    }))
}
fn revision(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn read_config(path: &Path) -> Result<Value> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    let doc = std::str::from_utf8(&bytes)?.parse::<toml::Table>()?;
    Ok(
        json!({"servers": doc.get("mcp_servers").map(serde_json::to_value).transpose()?.unwrap_or(json!({})), "revision": revision(&bytes)}),
    )
}

pub fn read_profile_config(profile: &str) -> Result<Value> {
    let mut config = read_config(&config_path(profile)?)?;
    if profile != "default" {
        let base = read_config(&config_path("default")?)?;
        let own = config["servers"]
            .as_object()
            .context("mcp_servers must be a table")?
            .clone();
        let mut merged = base["servers"]
            .as_object()
            .context("mcp_servers must be a table")?
            .clone();
        let inherited = merged
            .keys()
            .filter(|name| !own.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        for (name, overrides) in own {
            if let (Some(base), Some(overrides)) = (
                merged.get_mut(&name).and_then(Value::as_object_mut),
                overrides.as_object(),
            ) {
                base.extend(overrides.clone());
            } else {
                merged.insert(name, overrides);
            }
        }
        config["servers"] = Value::Object(merged);
        config["inherited"] = json!(inherited);
    }
    Ok(config)
}

#[derive(Deserialize)]
pub struct ConfigUpdate {
    pub config: Value,
    pub expected_revision: String,
}

fn validate_server(name: &str, config: &Value) -> Result<()> {
    if name.trim().is_empty() || name.len() > 128 {
        bail!("A server name is required (max 128 characters)");
    }
    let object = config
        .as_object()
        .context("Server configuration must be an object")?;
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let url = object.get("url").and_then(Value::as_str);
    if command.is_some() == url.is_some() {
        bail!("Configure exactly one of command (stdio) or url (HTTP)");
    }
    if let Some(url) = url {
        let parsed = url::Url::parse(url)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("MCP URL must use HTTP or HTTPS");
        }
    }
    for field in ["command", "url", "cwd", "bearer_token_env_var"] {
        if let Some(value) = object.get(field) {
            if !value.is_string() || value.as_str().is_some_and(|v| v.trim().is_empty()) {
                bail!("{field} must be a nonempty string");
            }
        }
    }
    for field in ["args", "env_vars", "enabled_tools", "disabled_tools"] {
        if let Some(values) = object.get(field) {
            if !values
                .as_array()
                .is_some_and(|a| a.iter().all(Value::is_string))
            {
                bail!("{field} must be an array of strings");
            }
        }
    }
    for field in ["env", "http_headers", "env_http_headers"] {
        if let Some(values) = object.get(field) {
            if !values
                .as_object()
                .is_some_and(|a| a.values().all(Value::is_string))
            {
                bail!("{field} must map names to strings");
            }
        }
    }
    for field in ["startup_timeout_sec", "tool_timeout_sec"] {
        if let Some(value) = object.get(field) {
            if !value.as_f64().is_some_and(|v| v > 0.0 && v.is_finite()) {
                bail!("{field} must be positive");
            }
        }
    }
    if let Some(value) = object.get("enabled") {
        if !value.is_boolean() {
            bail!("enabled must be a boolean");
        }
    }
    toml::Value::try_from(config).context("Configuration must contain TOML-compatible values")?;
    Ok(())
}

/// Preserve unrelated TOML and reject stale editors, including edits by the CLI.
pub async fn save_config(
    path: &Path,
    name: &str,
    update: Option<ConfigUpdate>,
    expected: &str,
) -> Result<Value> {
    static WRITES: Mutex<()> = Mutex::const_new(());
    let _guard = WRITES.lock().await;
    let resolved = match std::fs::canonicalize(path) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => path.to_path_buf(),
        Err(error) => return Err(error.into()),
    };
    let path = resolved.as_path();
    let bytes = std::fs::read(path).or_else(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Ok(Vec::new())
        } else {
            Err(e)
        }
    })?;
    if revision(&bytes) != expected {
        bail!("Configuration changed. Refresh before saving.");
    }
    let mut doc = std::str::from_utf8(&bytes)?.parse::<toml_edit::DocumentMut>()?;
    if doc
        .get("mcp_servers")
        .is_some_and(|item| !item.is_table_like())
    {
        bail!("mcp_servers must be a TOML table");
    }
    if let Some(update) = update {
        validate_server(name, &update.config)?;
        let table: toml::Table = [("server".into(), toml::Value::try_from(&update.config)?)]
            .into_iter()
            .collect();
        let source = toml::to_string(&table)?.parse::<toml_edit::DocumentMut>()?;
        if doc.get("mcp_servers").is_none() {
            doc["mcp_servers"] = toml_edit::Item::Table(toml_edit::Table::new());
        }
        doc["mcp_servers"][name] = source["server"].clone();
    } else if let Some(servers) = doc
        .get_mut("mcp_servers")
        .and_then(toml_edit::Item::as_table_like_mut)
    {
        servers.remove(name);
    }
    let parent = path.parent().context("Missing config directory")?;
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".omni-mcp-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        use std::io::Write;
        if let Ok(metadata) = std::fs::metadata(path) {
            file.set_permissions(metadata.permissions())?;
        }
        file.write_all(doc.to_string().as_bytes())?;
        file.sync_all()?;
        // Check again just before rename; never silently overwrite a detected CLI edit.
        let current = std::fs::read(path).or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(Vec::new())
            } else {
                Err(error)
            }
        })?;
        if revision(&current) != expected {
            bail!("Configuration changed. Refresh before saving.");
        }
        std::fs::rename(&temp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result?;
    read_config(path)
}

type RpcReply = oneshot::Sender<std::result::Result<Value, String>>;
#[derive(Default)]
struct PeerState {
    pending: HashMap<String, RpcReply>,
    requests: HashMap<String, Value>,
    events: VecDeque<Value>,
    closed: bool,
    oauth_callbacks: HashMap<String, url::Url>,
}
fn remember(state: &mut PeerState, value: Value) {
    state.events.push_back(value);
    while state.events.len() > 50 {
        state.events.pop_front();
    }
}
pub struct Peer {
    outgoing: mpsc::Sender<Value>,
    state: Arc<Mutex<PeerState>>,
    task: tokio::task::AbortHandle,
    writer: tokio::task::AbortHandle,
    thread: Mutex<Option<String>>,
    pub cwd: PathBuf,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
        self.writer.abort();
    }
}
impl Peer {
    async fn spawn(profile: &str, cwd: &Path) -> Result<Arc<Self>> {
        let child = crate::adapter::spawn_codex_mcp_server(profile, cwd)?;
        Self::from_child(child, cwd).await
    }
    async fn from_child(mut child: tokio::process::Child, cwd: &Path) -> Result<Arc<Self>> {
        let mut stdin = child.stdin.take().context("Missing Codex stdin")?;
        let stdout = child.stdout.take().context("Missing Codex stdout")?;
        let stderr = child.stderr.take().context("Missing Codex stderr")?;
        let state = Arc::new(Mutex::new(PeerState::default()));
        let (outgoing, mut receiver) = mpsc::channel::<Value>(64);
        let writer = tokio::spawn(async move {
            while let Some(value) = receiver.recv().await {
                let mut bytes = serde_json::to_vec(&value).unwrap();
                bytes.push(b'\n');
                if stdin.write_all(&bytes).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        })
        .abort_handle();
        // Drain stderr without exposing configuration values or credentials through the API.
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(_)) = lines.next_line().await {}
        });
        let reader_state = Arc::clone(&state);
        let reply_tx = outgoing.clone();
        let task = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(value) = serde_json::from_str::<Value>(&line) else { continue; };
                let mut state = reader_state.lock().await;
                if let Some(id) = value.get("id") {
                    let key = id.to_string();
                    if value.get("method").is_some() {
                        if value["method"] == "mcpServer/elicitation/request" {
                            state.requests.insert(key, value);
                        } else {
                            drop(state);
                            let _ = reply_tx.send(json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32601,"message":"Unsupported management request"}})).await;
                        }
                    } else if let Some(reply) = state.pending.remove(&key) {
                        let result = if let Some(error) = value.get("error") { Err(error.to_string()) } else { Ok(value.get("result").cloned().unwrap_or(Value::Null)) };
                        let _ = reply.send(result);
                    }
                } else if value.get("method").is_some() {
                    if value["method"] == "serverRequest/resolved" {
                        if let Some(id) = value.pointer("/params/requestId") { state.requests.remove(&id.to_string()); }
                    }
                    remember(&mut state, value);
                }
            }
            let mut state = reader_state.lock().await;
            state.closed = true;
            for (_, reply) in state.pending.drain() { let _ = reply.send(Err("Codex MCP process exited".into())); }
            state.requests.clear();
            stderr_task.abort();
            let _ = child.kill().await;
        }).abort_handle();
        let peer = Arc::new(Self {
            outgoing,
            state,
            task,
            writer,
            thread: Mutex::new(None),
            cwd: cwd.to_path_buf(),
        });
        peer.rpc("initialize", json!({"clientInfo":{"name":"codex-tui","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
        peer.outgoing
            .send(json!({"jsonrpc":"2.0","method":"initialized"}))
            .await?;
        Ok(peer)
    }
    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let key = json!(id).to_string();
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            if state.closed {
                bail!("Codex MCP process exited");
            }
            state.pending.insert(key.clone(), sender);
        }
        if self
            .outgoing
            .send(json!({"jsonrpc":"2.0", "id":id,"method":method,"params":params}))
            .await
            .is_err()
        {
            self.state.lock().await.pending.remove(&key);
            bail!("Codex MCP connection closed");
        }
        let result = tokio::time::timeout(
            Duration::from_secs(if method == "initialize" { 20 } else { 180 }),
            receiver,
        )
        .await;
        self.state.lock().await.pending.remove(&key);
        match result {
            Ok(Ok(Ok(value))) => {
                if method == "mcpServer/oauth/login" {
                    if let Some(authorization_url) = value
                        .get("authorizationUrl")
                        .and_then(Value::as_str)
                        .and_then(|url| url::Url::parse(url).ok())
                    {
                        let query: HashMap<_, _> =
                            authorization_url.query_pairs().into_owned().collect();
                        if let (Some(state), Some(redirect)) =
                            (query.get("state"), query.get("redirect_uri"))
                        {
                            if let Ok(redirect) = url::Url::parse(redirect) {
                                self.state
                                    .lock()
                                    .await
                                    .oauth_callbacks
                                    .insert(state.clone(), redirect);
                            }
                        }
                    }
                }
                Ok(value)
            }
            Ok(Ok(Err(error))) => bail!("{error}"),
            _ => bail!("MCP request timed out or connection closed"),
        }
    }
    /// Remote/mobile browsers cannot reach the host's loopback OAuth listener.
    /// Forward only an exact callback issued by this peer, matched by OAuth state.
    pub async fn complete_oauth(&self, callback: &str) -> Result<()> {
        let url = url::Url::parse(callback)?;
        let state_value = url
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, v)| v.into_owned())
            .context("Missing OAuth state")?;
        let expected = self
            .state
            .lock()
            .await
            .oauth_callbacks
            .get(&state_value)
            .cloned()
            .context("No matching pending OAuth login")?;
        validate_oauth_callback(&expected, &url)?;
        let response = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()?
            .get(url)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("OAuth callback rejected (HTTP {})", response.status());
        }
        self.state.lock().await.oauth_callbacks.remove(&state_value);
        Ok(())
    }
    pub async fn thread_id(&self) -> Result<String> {
        let mut thread = self.thread.lock().await;
        if let Some(id) = &*thread {
            return Ok(id.clone());
        }
        let response = self
            .rpc("thread/start", json!({"cwd":self.cwd,"ephemeral":true}))
            .await?;
        let id = response
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .context("Codex did not return a thread id")?
            .to_string();
        *thread = Some(id.clone());
        Ok(id)
    }
    pub async fn snapshot(&self) -> Value {
        let state = self.state.lock().await;
        json!({"requests":state.requests.iter().map(|(id,v)| json!({"request_id":id,"params":v["params"]})).collect::<Vec<_>>(),"events":state.events,"closed":state.closed})
    }
    pub async fn respond(&self, id: &str, value: Value) -> Result<()> {
        validate_elicitation_response(&value)?;
        let mut state = self.state.lock().await;
        let request = state
            .requests
            .get(id)
            .context("MCP request has expired or was already answered")?;
        if value["action"] == "accept"
            && request.pointer("/params/mode").and_then(Value::as_str) == Some("form")
            && !value["content"].is_object()
        {
            bail!("Form responses require an object of answers");
        }
        self.outgoing
            .send(json!({"jsonrpc":"2.0","id":request["id"],"result":value}))
            .await?;
        state.requests.remove(id);
        Ok(())
    }
}
pub fn validate_elicitation_response(value: &Value) -> Result<()> {
    if !matches!(
        value.get("action").and_then(Value::as_str),
        Some("accept" | "decline" | "cancel")
    ) {
        bail!("Invalid elicitation action");
    }
    if value["action"] == "accept"
        && !value
            .get("content")
            .is_none_or(|v| v.is_null() || v.is_object())
    {
        bail!("Elicitation content must be an object");
    }
    Ok(())
}

fn validate_oauth_callback(expected: &url::Url, callback: &url::Url) -> Result<()> {
    let loopback = expected.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !loopback
        || expected.scheme() != "http"
        || expected.origin() != callback.origin()
        || expected.path() != callback.path()
        || !callback.username().is_empty()
        || callback.password().is_some()
    {
        bail!("Callback must match the pending login's local redirect URL");
    }
    if !callback
        .query_pairs()
        .any(|(key, _)| key == "code" || key == "error")
    {
        bail!("OAuth callback contains no code or error");
    }
    Ok(())
}

struct Entry {
    peer: Arc<Peer>,
    touched: std::time::Instant,
}
fn peers() -> &'static Mutex<HashMap<String, Entry>> {
    static PEERS: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    PEERS.get_or_init(|| Mutex::new(HashMap::new()))
}
pub async fn peer(profile: &str, cwd: &Path) -> Result<Arc<Peer>> {
    let key = format!(
        "{}:{}:{}",
        config_path(profile)?.display(),
        profile,
        cwd.display()
    );
    let mut peers = peers().lock().await;
    peers.retain(|_, e| e.touched.elapsed() < Duration::from_secs(1800));
    if let Some(entry) = peers.get_mut(&key) {
        if !entry.peer.state.lock().await.closed {
            entry.touched = std::time::Instant::now();
            return Ok(Arc::clone(&entry.peer));
        }
    }
    if peers.len() >= 16 {
        bail!("Too many MCP management connections; disconnect an unused scope first");
    }
    let peer = Peer::spawn(profile, cwd).await?;
    peers.insert(
        key,
        Entry {
            peer: Arc::clone(&peer),
            touched: std::time::Instant::now(),
        },
    );
    Ok(peer)
}
pub async fn disconnect(profile: &str, cwd: &Path) -> Result<()> {
    let key = format!(
        "{}:{}:{}",
        config_path(profile)?.display(),
        profile,
        cwd.display()
    );
    peers().lock().await.remove(&key);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_runtime_observations_are_isolated_and_unknown_by_default() {
        let id = uuid::Uuid::new_v4().to_string();
        assert!(session_runtime(Some(&id)).is_null());
        record_session_runtime(
            &id,
            json!([{"name":"fixture","runtimeStatus":"failed","tools":{}}]),
        );
        assert_eq!(
            session_runtime(Some(&id))["servers"][0]["runtimeStatus"],
            "failed"
        );
        assert!(session_runtime(None).is_null());
    }
    #[test]
    fn validates_transports_and_types() {
        assert!(
            validate_server(
                "local",
                &json!({"command":"node","args":["server.js"],"env":{"TOKEN":"value"}})
            )
            .is_ok()
        );
        assert!(
            validate_server(
                "remote",
                &json!({"url":"https://example.test/mcp","enabled":false})
            )
            .is_ok()
        );
        for config in [
            json!({}),
            json!({"url":"file:///tmp/server"}),
            json!({"command":"x","url":"https://example.test"}),
            json!({"command":"x","args":"bad"}),
            json!({"command":"x","tool_timeout_sec":-1}),
        ] {
            assert!(validate_server("test", &config).is_err());
        }
    }
    #[tokio::test]
    async fn config_changes_preserve_other_settings_and_reject_stale_edits() {
        let dir = std::env::temp_dir().join(format!("omni-mcp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "# keep this comment\nmodel = \"test\"\n[mcp_servers.old]\ncommand = \"old\"\n",
        )
        .unwrap();
        let before = read_config(&path).unwrap();
        let rev = before["revision"].as_str().unwrap();
        let after = save_config(
            &path,
            "work.api",
            Some(ConfigUpdate {
                config: json!({"url":"https://example.test/mcp","enabled":true}),
                expected_revision: rev.into(),
            }),
            rev,
        )
        .await
        .unwrap();
        assert_eq!(after["servers"]["work.api"]["enabled"], true);
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains("# keep this comment")
        );
        assert!(save_config(&path, "old", None, rev).await.is_err());
        save_config(&path, "old", None, after["revision"].as_str().unwrap())
            .await
            .unwrap();
        assert!(read_config(&path).unwrap()["servers"].get("old").is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

pub async fn invalidate(profile: &str) -> Result<()> {
    if profile == "default" {
        peers().lock().await.clear();
        return Ok(());
    }
    let prefix = format!("{}:{}:", config_path(profile)?.display(), profile);
    peers()
        .lock()
        .await
        .retain(|key, _| !key.starts_with(&prefix));
    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires OMNI_MCP_TEST_CODEX and OMNI_MCP_TEST_PYTHON; uses an isolated CODEX_HOME"]
    async fn real_codex_mcp_inventory_tools_resources_and_elicitation() {
        let binary = std::env::var("OMNI_MCP_TEST_CODEX").expect("Codex executable");
        let python = std::env::var("OMNI_MCP_TEST_PYTHON").expect("Python executable");
        let dir =
            std::env::temp_dir().join(format!("omni-mcp-integration-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp_server.py");
        let config = json!({"model":"gpt-5.4", "mcp_servers":{"fixture":{"command":python,"args":[fixture],"startup_timeout_sec":30}}});
        std::fs::write(dir.join("config.toml"), toml::to_string(&config).unwrap()).unwrap();
        let mut command = tokio::process::Command::new(binary);
        command
            .args(["app-server", "--listen", "stdio://"])
            .env("CODEX_HOME", &dir)
            .current_dir(&dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let peer = Peer::from_child(command.spawn().unwrap(), &dir)
            .await
            .unwrap();
        let thread = peer.thread_id().await.unwrap();
        let inventory = peer
            .rpc("mcpServerStatus/list", json!({"threadId":thread}))
            .await
            .unwrap();
        let server = inventory["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == "fixture")
            .expect("fixture inventory");
        assert!(server["tools"].get("echo").is_some(), "{inventory}");
        let result=peer.rpc("mcpServer/tool/call",json!({"threadId":thread,"server":"fixture","tool":"echo","arguments":{"text":"round trip"}})).await.unwrap();
        assert_eq!(result["content"][0]["text"], "round trip");
        let resource = peer
            .rpc(
                "mcpServer/resource/read",
                json!({"threadId":thread,"server":"fixture","uri":"fixture://notes"}),
            )
            .await
            .unwrap();
        assert_eq!(resource["contents"][0]["text"], "Fixture resource");
        let form_peer = Arc::clone(&peer);
        let form_thread = thread.clone();
        let call = tokio::spawn(async move {
            form_peer
                .rpc(
                    "mcpServer/tool/call",
                    json!({"threadId":form_thread,"server":"fixture","tool":"form","arguments":{}}),
                )
                .await
        });
        let request = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let snapshot = peer.snapshot().await;
                if let Some(request) = snapshot["requests"].as_array().and_then(|r| r.first()) {
                    break request.clone();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("MCP form request");
        peer.respond(
            request["request_id"].as_str().unwrap(),
            json!({"action":"accept","content":{"count":3},"_meta":null}),
        )
        .await
        .unwrap();
        let result = call.await.unwrap().unwrap();
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("count")
        );
        assert!(
            peer.snapshot().await["requests"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        drop(peer);
        // Dropping the peer aborts its IO tasks; allow the killed Codex process
        // to finish its final database writes before deleting the isolated home.
        for attempt in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) if attempt == 19 => panic!("fixture cleanup: {error}"),
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod oauth_tests {
    use super::*;
    #[test]
    fn callback_forwarding_rejects_other_hosts_ports_and_paths() {
        let expected = url::Url::parse("http://127.0.0.1:12345/callback").unwrap();
        assert!(
            validate_oauth_callback(
                &expected,
                &url::Url::parse("http://127.0.0.1:12345/callback?code=x&state=s").unwrap()
            )
            .is_ok()
        );
        for url in [
            "http://example.com:12345/callback?code=x",
            "http://127.0.0.1:12346/callback?code=x",
            "http://127.0.0.1:12345/other?code=x",
            "http://127.0.0.1:12345/callback",
        ] {
            assert!(validate_oauth_callback(&expected, &url::Url::parse(url).unwrap()).is_err());
        }
    }
    #[tokio::test]
    #[ignore = "requires OMNI_MCP_TEST_CODEX and OMNI_MCP_TEST_PYTHON; loopback OAuth fixture only"]
    async fn real_codex_mcp_oauth_and_remote_callback_round_trip() {
        let binary = std::env::var("OMNI_MCP_TEST_CODEX").expect("Codex executable");
        let python = std::env::var("OMNI_MCP_TEST_PYTHON").expect("Python executable");
        let dir = std::env::temp_dir().join(format!("omni-mcp-oauth-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp_oauth_server.py");
        let mut fixture = tokio::process::Command::new(python)
            .arg(fixture)
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let port = BufReader::new(fixture.stdout.take().unwrap())
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let config = json!({"model":"gpt-5.4","mcp_servers":{"oauth_fixture":{"url":format!("http://127.0.0.1:{port}/mcp"),"startup_timeout_sec":10}}});
        std::fs::write(dir.join("config.toml"), toml::to_string(&config).unwrap()).unwrap();
        let child = tokio::process::Command::new(binary)
            .args(["app-server", "--listen", "stdio://"])
            .env("CODEX_HOME", &dir)
            .current_dir(&dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let peer = Peer::from_child(child, &dir).await.unwrap();
        let thread = peer.thread_id().await.unwrap();
        let login = peer
            .rpc(
                "mcpServer/oauth/login",
                json!({"name":"oauth_fixture","threadId":thread,"timeoutSecs":60}),
            )
            .await
            .unwrap();
        let authorization = login["authorizationUrl"].as_str().unwrap();
        // Simulate the phone browser reaching the redirect, but not following it.
        let browser = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let redirect = browser.get(authorization).send().await.unwrap();
        let callback = redirect.headers()[reqwest::header::LOCATION]
            .to_str()
            .unwrap();
        let mut wrong = url::Url::parse(callback).unwrap();
        wrong.set_query(Some("state=wrong&code=fixture-code"));
        assert!(peer.complete_oauth(wrong.as_str()).await.is_err());
        peer.complete_oauth(callback).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let snapshot = peer.snapshot().await;
                if snapshot["events"].as_array().unwrap().iter().any(|event| {
                    event["method"] == "mcpServer/oauthLogin/completed"
                        && event["params"]["success"] == true
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("OAuth completed event");
        assert!(
            peer.complete_oauth(callback).await.is_err(),
            "callback cannot be replayed"
        );
        peer.rpc("config/mcpServer/reload", json!({}))
            .await
            .unwrap();
        let inventory = peer
            .rpc("mcpServerStatus/list", json!({"threadId":thread}))
            .await
            .unwrap();
        assert!(
            inventory["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|server| server["tools"].get("authorized_echo").is_some()),
            "{inventory}"
        );
        drop(peer);
        fixture.kill().await.unwrap();
        // Dropping the peer aborts its IO tasks; allow the killed Codex process
        // to finish its final database writes before deleting the isolated home.
        for attempt in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) if attempt == 19 => panic!("fixture cleanup: {error}"),
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod startup_catalog_tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires real Codex and Python; model traffic captured by local fixture"]
    async fn real_codex_slow_mcp_is_in_first_turn_model_catalog() {
        let binary = std::env::var("OMNI_MCP_TEST_CODEX").unwrap();
        let python = std::env::var("OMNI_MCP_TEST_PYTHON").unwrap();
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        // Demonstrate the failure and its fix with the same two-second startup.
        for (grace, expected) in [(1000, false), (30000, true)] {
            let dir =
                std::env::temp_dir().join(format!("omni-mcp-catalog-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let capture = dir.join("request.json");
            let mut model = tokio::process::Command::new(&python)
                .arg(fixtures.join("mcp_model_capture.py"))
                .arg(&capture)
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let port = BufReader::new(model.stdout.take().unwrap())
                .lines()
                .next_line()
                .await
                .unwrap()
                .unwrap();
            let config = json!({"model":"gpt-5.4","model_provider":"fixture",
                "mcp_optional_startup_grace_ms":grace,
                "model_providers":{"fixture":{"name":"Local capture","base_url":format!("http://127.0.0.1:{port}/v1"),"wire_api":"responses","requires_openai_auth":false}},
                "mcp_servers":{"slow_fixture":{"command":python,"args":[fixtures.join("mcp_server.py")],
                    "env":{"OMNI_MCP_FIXTURE_DELAY":"2"},"startup_timeout_sec":10}}});
            std::fs::write(dir.join("config.toml"), toml::to_string(&config).unwrap()).unwrap();
            let child = tokio::process::Command::new(&binary)
                .args(["app-server", "--listen", "stdio://"])
                .env("CODEX_HOME", &dir)
                .current_dir(&dir)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let peer = Peer::from_child(child, &dir).await.unwrap();
            let thread = peer.thread_id().await.unwrap();
            peer.rpc("turn/start",json!({"threadId":thread,"input":[{"type":"text","text":"Use slow_fixture echo","text_elements":[]}]})).await.unwrap();
            tokio::time::timeout(Duration::from_secs(30), async {
                while !capture.exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("local model request captured");
            let request: Value = serde_json::from_slice(&std::fs::read(&capture).unwrap()).unwrap();
            let tools = request["tools"].to_string();
            assert_eq!(
                tools.contains("slow_fixture") && tools.contains("\"name\":\"echo\""),
                expected,
                "grace={grace}; tools={tools}"
            );
            drop(peer);
            model.kill().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}
