use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::mpsc,
    time::sleep,
};

use crate::{
    ai_approval::{self, AiApprovalDecisionKind},
    app_state::AppState,
    approval_policy::{self, AutoApprovalDecision},
    claude_hook::{
        ClaudeHookStatusEvent, ClaudePermissionRequest, append_always_allow_command,
        claude_state_dir, ensure_runtime_dirs, request_path, response_from_choice, response_path,
    },
    claude_store::{load_claude_archive_summary, load_claude_messages},
    models::{
        AgentKind, ApprovalChoice, ApprovalKind, ApprovalRequest, ChatMessage, ProjectSummary,
        SessionSummary,
    },
    session_store::{load_session_archive_summary, load_session_messages},
};

#[async_trait]
pub trait AgentProvider: Send + Sync {
    async fn list_projects(&self) -> HashMap<String, ProjectSummary> {
        HashMap::new()
    }

    async fn list_sessions(&self) -> HashMap<String, SessionSummary> {
        HashMap::new()
    }

    async fn list_messages(&self, _session_id: &str) -> Option<Vec<ChatMessage>> {
        None
    }

    async fn default_runtime_ref(&self, _session_id: &str) -> Option<String> {
        None
    }

    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        input: ChatMessage,
        reply: ChatMessage,
    ) -> Result<()>;

    async fn summarize_reply(
        &self,
        _state: Arc<AppState>,
        _session: SessionSummary,
        _content: String,
    ) -> Result<String> {
        bail!("summary is not supported by this agent")
    }
}

pub struct ProviderRegistry {
    providers: HashMap<AgentKind, Arc<dyn AgentProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        let mut providers: HashMap<AgentKind, Arc<dyn AgentProvider>> = HashMap::new();
        providers.insert(AgentKind::Codex, Arc::new(CodexProvider::new()));
        providers.insert(AgentKind::ClaudeCode, Arc::new(ClaudeCodeProvider::new()));
        providers.insert(
            AgentKind::OpenCode,
            Arc::new(StubProvider::new(AgentKind::OpenCode)),
        );
        providers.insert(
            AgentKind::Custom,
            Arc::new(StubProvider::new(AgentKind::Custom)),
        );
        Self { providers }
    }

    pub fn get(&self, kind: AgentKind) -> Option<Arc<dyn AgentProvider>> {
        self.providers.get(&kind).cloned()
    }

    pub fn all(&self) -> Vec<Arc<dyn AgentProvider>> {
        self.providers.values().cloned().collect()
    }
}

struct StubProvider {
    kind: AgentKind,
}

impl StubProvider {
    fn new(kind: AgentKind) -> Self {
        Self { kind }
    }
}

#[async_trait]
impl AgentProvider for StubProvider {
    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        input: ChatMessage,
        reply: ChatMessage,
    ) -> Result<()> {
        let prefix = match self.kind {
            AgentKind::ClaudeCode => "ClaudeCode",
            AgentKind::OpenCode => "OpenCode",
            AgentKind::Custom => "CustomAgent",
            AgentKind::Codex => "Codex",
        };

        let chunks = [
            format!("{prefix} 已接收任务。"),
            "当前 provider 架构已就位，这个 agent 还没有接入真实 CLI。".to_string(),
            format!("任务内容：{}", input.content),
        ];

        for chunk in chunks {
            sleep(Duration::from_millis(120)).await;
            state
                .emit_message_delta(&session.id, &reply.id, &chunk)
                .await;
        }

        state
            .finish_assistant_message(&session.id, &reply.id)
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(())
    }

    async fn summarize_reply(
        &self,
        _state: Arc<AppState>,
        _session: SessionSummary,
        content: String,
    ) -> Result<String> {
        let summary = content.chars().take(60).collect::<String>();
        Ok(summary)
    }
}

#[derive(Clone)]
struct CachedMessageSet {
    fingerprint: u64,
    messages: Vec<ChatMessage>,
}

#[derive(Clone)]
struct CachedCodexArchive {
    checked_at: Instant,
    fingerprint: u64,
    projects: HashMap<String, ProjectSummary>,
    sessions: HashMap<String, SessionSummary>,
    session_files: HashMap<String, PathBuf>,
    messages: HashMap<String, CachedMessageSet>,
}

struct CodexProvider {
    cache: Mutex<Option<CachedCodexArchive>>,
}

impl CodexProvider {
    const ARCHIVE_CACHE_TTL: Duration = Duration::from_secs(2);

    fn new() -> Self {
        Self {
            cache: Mutex::new(None),
        }
    }

    fn ensure_archive(&self) -> CachedCodexArchive {
        {
            let cache = self.cache.lock().expect("codex cache poisoned");
            if let Some(existing) = cache.as_ref()
                && existing.checked_at.elapsed() < Self::ARCHIVE_CACHE_TTL
            {
                return existing.clone();
            }
        }

        let summary = load_session_archive_summary();
        let mut cache = self.cache.lock().expect("codex cache poisoned");
        if let Some(existing) = cache.as_mut()
            && existing.fingerprint == summary.fingerprint
        {
            existing.checked_at = Instant::now();
            return existing.clone();
        }

        let messages = cache
            .as_ref()
            .map(|existing| existing.messages.clone())
            .unwrap_or_default();
        let archive = CachedCodexArchive {
            checked_at: Instant::now(),
            fingerprint: summary.fingerprint,
            projects: summary.projects,
            sessions: summary.sessions,
            session_files: summary.session_files,
            messages,
        };
        *cache = Some(archive.clone());
        archive
    }

    fn load_messages_for_session(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        let mut archive = self.ensure_archive();
        let path = archive.session_files.get(session_id)?.clone();
        let loaded = load_session_messages(&path)?;
        if let Some(existing) = archive.messages.get(session_id)
            && existing.fingerprint == loaded.fingerprint
        {
            return Some(existing.messages.clone());
        }

        archive.messages.insert(
            session_id.to_string(),
            CachedMessageSet {
                fingerprint: loaded.fingerprint,
                messages: loaded.messages.clone(),
            },
        );
        let mut cache = self.cache.lock().expect("codex cache poisoned");
        *cache = Some(archive);
        Some(loaded.messages)
    }
}

#[async_trait]
impl AgentProvider for CodexProvider {
    async fn list_projects(&self) -> HashMap<String, ProjectSummary> {
        self.ensure_archive().projects
    }

    async fn list_sessions(&self) -> HashMap<String, SessionSummary> {
        self.ensure_archive().sessions
    }

    async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        self.load_messages_for_session(session_id)
    }

    async fn default_runtime_ref(&self, session_id: &str) -> Option<String> {
        self.ensure_archive()
            .sessions
            .contains_key(session_id)
            .then(|| session_id.to_string())
    }

    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        input: ChatMessage,
        reply: ChatMessage,
    ) -> Result<()> {
        run_codex(state, &session, &input, &reply).await
    }

    async fn summarize_reply(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        content: String,
    ) -> Result<String> {
        summarize_with_codex(state, &session, &content).await
    }
}

#[derive(Clone)]
struct CachedClaudeArchive {
    checked_at: Instant,
    fingerprint: u64,
    projects: HashMap<String, ProjectSummary>,
    sessions: HashMap<String, SessionSummary>,
    session_files: HashMap<String, PathBuf>,
    messages: HashMap<String, CachedMessageSet>,
}

struct ClaudeCodeProvider {
    cache: Mutex<Option<CachedClaudeArchive>>,
}

impl ClaudeCodeProvider {
    const ARCHIVE_CACHE_TTL: Duration = Duration::from_secs(2);

    fn new() -> Self {
        Self {
            cache: Mutex::new(None),
        }
    }

    fn ensure_archive(&self) -> CachedClaudeArchive {
        {
            let cache = self.cache.lock().expect("claude cache poisoned");
            if let Some(existing) = cache.as_ref()
                && existing.checked_at.elapsed() < Self::ARCHIVE_CACHE_TTL
            {
                return existing.clone();
            }
        }

        let summary = load_claude_archive_summary();
        let mut cache = self.cache.lock().expect("claude cache poisoned");
        if let Some(existing) = cache.as_mut()
            && existing.fingerprint == summary.fingerprint
        {
            existing.checked_at = Instant::now();
            return existing.clone();
        }

        let messages = cache
            .as_ref()
            .map(|existing| existing.messages.clone())
            .unwrap_or_default();
        let archive = CachedClaudeArchive {
            checked_at: Instant::now(),
            fingerprint: summary.fingerprint,
            projects: summary.projects,
            sessions: summary.sessions,
            session_files: summary.session_files,
            messages,
        };
        *cache = Some(archive.clone());
        archive
    }

    fn load_messages_for_session(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        let mut archive = self.ensure_archive();
        let path = archive.session_files.get(session_id)?.clone();
        let loaded = load_claude_messages(&path)?;
        if let Some(existing) = archive.messages.get(session_id)
            && existing.fingerprint == loaded.fingerprint
        {
            return Some(existing.messages.clone());
        }

        archive.messages.insert(
            session_id.to_string(),
            CachedMessageSet {
                fingerprint: loaded.fingerprint,
                messages: loaded.messages.clone(),
            },
        );
        let mut cache = self.cache.lock().expect("claude cache poisoned");
        *cache = Some(archive);
        Some(loaded.messages)
    }
}

#[async_trait]
impl AgentProvider for ClaudeCodeProvider {
    async fn list_projects(&self) -> HashMap<String, ProjectSummary> {
        self.ensure_archive().projects
    }

    async fn list_sessions(&self) -> HashMap<String, SessionSummary> {
        self.ensure_archive().sessions
    }

    async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        self.load_messages_for_session(session_id)
    }

    async fn default_runtime_ref(&self, session_id: &str) -> Option<String> {
        self.ensure_archive()
            .sessions
            .contains_key(session_id)
            .then(|| session_id.to_string())
    }

    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        input: ChatMessage,
        reply: ChatMessage,
    ) -> Result<()> {
        run_claude_code(state, &session, &input, &reply).await
    }

    async fn summarize_reply(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        content: String,
    ) -> Result<String> {
        summarize_with_claude_code(state, &session, &content).await
    }
}

fn spawn_codex_app_server(cwd: &Path) -> Result<Child> {
    let binary = codex_binary_path();
    Command::new(&binary)
        .args(["app-server", "--listen", "stdio://"])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn `{}`", binary.display()))
}

fn brief_reply_developer_prompt(session: &SessionSummary) -> Option<&'static str> {
    session.brief_reply_mode.then_some(
        "回复要求：请简短说明你做了什么和结果，尽量不超过 50 个汉字。只保留关键动作、结果或结论，避免展开解释。",
    )
}

fn codex_binary_path() -> PathBuf {
    if let Some(path) = std::env::var_os("ECHO_MATE_CODEX_BIN")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        return path;
    }

    if let Some(path) = find_executable_in_path("codex") {
        return path;
    }

    let mut candidates = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        candidates.push(home.join(".bun/bin/codex"));
        candidates.push(home.join(".local/bin/codex"));

        let node_versions = home.join(".nvm/versions/node");
        if let Ok(entries) = std::fs::read_dir(node_versions) {
            let mut codex_bins = entries
                .filter_map(Result::ok)
                .map(|entry| entry.path().join("bin/codex"))
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            codex_bins.sort();
            candidates.extend(codex_bins.into_iter().rev());
        }
    }
    candidates.push(PathBuf::from("/usr/local/bin/codex"));
    candidates.push(PathBuf::from("/usr/bin/codex"));

    candidates
        .into_iter()
        .find(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from("codex"))
}

fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

async fn run_codex(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    reply: &ChatMessage,
) -> Result<()> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let mut child =
        spawn_codex_app_server(&project_root).context("failed to spawn `codex app-server`")?;

    let mut stdin = child
        .stdin
        .take()
        .context("codex process did not expose stdin")?;
    let stdout = child
        .stdout
        .take()
        .context("codex process did not expose stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("codex process did not expose stderr")?;

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = reader.next_line().await? {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
        }
        Ok::<_, std::io::Error>(output)
    });

    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let _ = stdout_tx.send(Ok(line));
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = stdout_tx.send(Err(error.to_string()));
                    break;
                }
            }
        }
    });

    let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
    state.set_approval_sender(&session.id, approval_tx).await;

    let mut next_request_id = 1_u64;
    let mut raw_stdout = String::new();
    let mut parsed = CodexAppServerStreamingState::default();

    send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "initialize",
        serde_json::json!({
            "clientInfo": {
                "name": "omni-code-bridge",
                "title": "omni-code-bridge",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "experimentalApi": true,
            }
        }),
    )
    .await?;
    wait_for_json_rpc_response(&mut stdout_rx, &mut raw_stdout, next_request_id - 1).await?;

    let existing_runtime_ref = state.provider_session_ref(&session.id).await;
    let developer_instructions = brief_reply_developer_prompt(session);
    let thread_request_id = if let Some(thread_id) = existing_runtime_ref.as_deref() {
        send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "thread/resume",
            serde_json::json!({
                "threadId": thread_id,
                "persistExtendedHistory": false,
                "developerInstructions": developer_instructions,
            }),
        )
        .await?
    } else {
        send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "thread/start",
            serde_json::json!({
                "cwd": state
                    .project_root_path_for_session(&session.id)
                    .await
                    .map_err(anyhow::Error::msg)?,
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": "workspace-write",
                "persistExtendedHistory": false,
                "developerInstructions": developer_instructions,
            }),
        )
        .await?
    };
    let thread_response =
        wait_for_json_rpc_response(&mut stdout_rx, &mut raw_stdout, thread_request_id).await?;
    let thread_id = thread_response
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            thread_response
                .pointer("/thread/threadId")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or(existing_runtime_ref)
        .context("codex app-server did not provide a thread id")?;
    state
        .set_provider_session_ref(&session.id, Some(thread_id.clone()))
        .await;
    parsed.session_id = Some(thread_id.clone());

    let turn_request_id = send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "turn/start",
        serde_json::json!({
            "threadId": thread_id,
            "input": [{
                "type": "text",
                "text": input.content,
                "text_elements": [],
            }],
        }),
    )
    .await?;

    let idle_deadline = Duration::from_secs(15);
    let idle_sleep = tokio::time::sleep(idle_deadline);
    tokio::pin!(idle_sleep);
    let mut pending_approval: Option<PendingApproval> = None;
    let mut turn_finished = false;
    let mut last_rendered = String::new();

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                let Some(line) = maybe_line else {
                    break;
                };
                let line = line.map_err(anyhow::Error::msg)?;
                idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_deadline);
                if !raw_stdout.is_empty() {
                    raw_stdout.push('\n');
                }
                raw_stdout.push_str(&line);

                let value = serde_json::from_str::<Value>(&line)
                    .unwrap_or_else(|_| serde_json::json!({ "raw_line": line }));
                if value.get("id").and_then(jsonrpc_id_to_string).as_deref()
                    == Some(&turn_request_id.to_string())
                {
                    if let Some(error) = value.get("error") {
                        let message = error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("codex turn/start failed");
                        bail!("{message}");
                    }
                    continue;
                }

                let previous_status = parsed.current_status().map(ToString::to_string);
                match parsed.ingest_value(&value) {
                    CodexAppServerEvent::None => {}
                    CodexAppServerEvent::Content(text) => {
                        push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
                    }
                    CodexAppServerEvent::Status(status) => {
                        parsed.latest_status = Some(status);
                    }
                    CodexAppServerEvent::ApprovalRequested(pending) => {
                        if let Some(choice) = auto_approve_codex_request(&pending.request, &project_root).await? {
                            send_json_rpc_response(
                                &mut stdin,
                                &pending.request.request_id,
                                approval_result_json(&choice, &pending.request.kind),
                            )
                            .await?;
                        } else {
                            pending_approval = Some(pending.clone());
                            state.raise_approval(&session.id, pending.request).await;
                        }
                    }
                    CodexAppServerEvent::ApprovalResolved { request_id } => {
                        if let Some(pending) = pending_approval.take() {
                            let choice = pending.last_choice.unwrap_or(ApprovalChoice::Accept);
                            state.resolve_approval(&session.id, &request_id, choice).await;
                        } else {
                            state.resolve_approval(&session.id, &request_id, ApprovalChoice::Accept).await;
                        }
                    }
                    CodexAppServerEvent::TurnCompleted => {
                        turn_finished = true;
                        break;
                    }
                    CodexAppServerEvent::TurnFailed(message) => {
                        bail!("{message}");
                    }
                }
                let next_status = parsed.current_status().map(ToString::to_string);
                if next_status.is_some() && next_status != previous_status {
                    state
                        .emit_system_message(&session.id, next_status.unwrap_or_default())
                        .await;
                }
            }
            Some(choice) = approval_rx.recv(), if pending_approval.is_some() => {
                let request = pending_approval
                    .as_mut()
                    .context("approval state disappeared unexpectedly")?;
                request.last_choice = Some(choice.clone());
                send_json_rpc_response(
                    &mut stdin,
                    &request.request.request_id,
                    approval_result_json(&choice, &request.request.kind),
                )
                .await?;
            }
            _ = &mut idle_sleep => {
                let previous_status = parsed.current_status().map(ToString::to_string);
                if let Some(text) = parsed.mark_idle_waiting() {
                    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
                }
                let next_status = parsed.current_status().map(ToString::to_string);
                if next_status.is_some() && next_status != previous_status {
                    state
                        .emit_system_message(&session.id, next_status.unwrap_or_default())
                        .await;
                }
                idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_deadline);
            }
        }
    }

    let text = parsed
        .finish_text()
        .or_else(|| fallback_text(&raw_stdout))
        .context("codex response did not include assistant text")?;
    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;

    if turn_finished {
        stderr_task.abort();
        state
            .finish_assistant_message(&session.id, &reply.id)
            .await
            .map_err(anyhow::Error::msg)?;
        return Ok(());
    }

    let status = child.wait().await?;
    let stderr = stderr_task
        .await
        .context("failed to join codex stderr reader")??;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("codex exited with status {code}");
        } else {
            bail!("codex exited with status {code}: {stderr}");
        }
    }

    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn run_claude_code(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    reply: &ChatMessage,
) -> Result<()> {
    let state_dir = claude_state_dir();
    ensure_runtime_dirs(&state_dir).await?;
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let existing_runtime_ref = state.provider_session_ref(&session.id).await;
    let runtime_ref = existing_runtime_ref
        .clone()
        .unwrap_or_else(|| session.id.clone());
    state
        .set_provider_session_ref(&session.id, Some(runtime_ref.clone()))
        .await;
    let run_id = uuid::Uuid::new_v4().to_string();
    let current_exe = std::env::current_exe().context("failed to resolve current executable")?;
    let hook_command = format!(
        "{} claude-permission-hook --state-dir {} --session-id {} --run-id {}",
        shell_quote(current_exe.to_string_lossy().as_ref()),
        shell_quote(state_dir.display().to_string().as_str()),
        shell_quote(session.id.as_str()),
        shell_quote(run_id.as_str()),
    );
    let hook_command = format!(
        "{hook_command} --project-root {}",
        shell_quote(project_root.display().to_string().as_str())
    );
    let settings = serde_json::json!({
        "permissions": {
            "allow": ["Bash(*)", "WebSearch(*)", "WebFetch(*)"],
            "defaultMode": "acceptEdits"
        },
        "hooks": {
            "PreToolUse": [{
                "hooks": [{"type": "command", "command": hook_command}]
            }],
            "PostToolUse": [{
                "hooks": [{"type": "command", "command": hook_command}]
            }]
        }
    });

    let mut command = Command::new("claude");
    command
        .arg("-p")
        .arg("--verbose")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--settings")
        .arg(settings.to_string());
    if let Some(system_prompt) = brief_reply_developer_prompt(session) {
        command.arg("--append-system-prompt").arg(system_prompt);
    }
    if existing_runtime_ref.is_some() {
        command.arg("-r").arg(&runtime_ref);
    } else {
        command.arg("--session-id").arg(&runtime_ref);
    }
    command
        .arg(&input.content)
        .current_dir(project_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().context("failed to spawn `claude`")?;
    let stdout = child
        .stdout
        .take()
        .context("claude process did not expose stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("claude process did not expose stderr")?;

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = reader.next_line().await? {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
        }
        Ok::<_, std::io::Error>(output)
    });

    let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
    state.set_approval_sender(&session.id, approval_tx).await;

    let mut last_rendered = String::new();
    let mut parsed = ClaudeStreamingState::default();
    let mut final_text = None;
    let mut reader = BufReader::new(stdout).lines();
    let mut pending_approval: Option<String> = None;
    let mut seen_permission_requests = std::collections::HashSet::new();
    let mut seen_status_events = std::collections::HashSet::new();
    let mut permission_poll = tokio::time::interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            maybe_line = reader.next_line() => {
                let Some(line) = maybe_line? else {
                    break;
                };
                let clean_line = strip_ansi(&line);
                if clean_line.trim().is_empty() {
                    continue;
                }
                let previous_status = parsed.current_status().map(ToString::to_string);
                if let Some(text) = parsed.ingest_line(&clean_line)? {
                    final_text = parsed.finish_text();
                    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
                }
                let next_status = parsed.current_status().map(ToString::to_string);
                if next_status.is_some() && next_status != previous_status {
                    state
                        .emit_system_message(&session.id, next_status.unwrap_or_default())
                        .await;
                }
            }
            _ = permission_poll.tick() => {
                if let Some(event) = next_claude_status_event(&state_dir, &run_id, &mut seen_status_events).await? {
                    let previous_status = parsed.current_status().map(ToString::to_string);
                    if let Some(text) = parsed.ingest_external_status(event.summary) {
                        push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
                    }
                    let next_status = parsed.current_status().map(ToString::to_string);
                    if next_status.is_some() && next_status != previous_status {
                        state
                            .emit_system_message(&session.id, next_status.unwrap_or_default())
                            .await;
                    }
                }
                if pending_approval.is_none() {
                    if let Some(request) = next_claude_permission_request(&state_dir, &run_id, &mut seen_permission_requests).await? {
                        let approval = request.as_approval_request();
                        pending_approval = Some(approval.request_id.clone());
                        state.raise_approval(&session.id, approval).await;
                    }
                }
            }
            Some(choice) = approval_rx.recv(), if pending_approval.is_some() => {
                let request_id = pending_approval.clone().context("approval state disappeared unexpectedly")?;
                let mut response = response_from_choice(&choice);
                response.request_id = request_id.clone();
                if matches!(choice, ApprovalChoice::AlwaysAllow) {
                    let runtime_request_path = request_path(&state_dir, &request_id);
                    if let Ok(body) = tokio::fs::read(&runtime_request_path).await {
                        if let Ok(request) = serde_json::from_slice::<ClaudePermissionRequest>(&body) {
                            if let Some(command) = request.as_approval_request().command {
                                append_always_allow_command(&state_dir, &command).await?;
                            }
                        }
                    }
                }
                tokio::fs::write(
                    response_path(&state_dir, &request_id),
                    serde_json::to_vec_pretty(&response)?,
                )
                .await?;
                state.resolve_approval(&session.id, &request_id, choice).await;
                pending_approval = None;
            }
        }
    }

    let status = child.wait().await?;
    let stderr = stderr_task
        .await
        .context("failed to join claude stderr reader")??;

    if !status.success() {
        let code = status.code().unwrap_or(1);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("claude exited with status {code}");
        } else {
            bail!("claude exited with status {code}: {stderr}");
        }
    }

    final_text = final_text.or_else(|| parsed.finish_text());
    if final_text.is_none() {
        bail!("claude response did not include assistant text");
    }

    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn summarize_with_codex(
    state: Arc<AppState>,
    session: &SessionSummary,
    content: &str,
) -> Result<String> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let mut child = spawn_codex_app_server(&project_root)
        .context("failed to spawn `codex app-server` for summary")?;

    let mut stdin = child
        .stdin
        .take()
        .context("codex summary process did not expose stdin")?;
    let stdout = child
        .stdout
        .take()
        .context("codex summary process did not expose stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("codex summary process did not expose stderr")?;

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = reader.next_line().await? {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
        }
        Ok::<_, std::io::Error>(output)
    });

    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let _ = stdout_tx.send(Ok(line));
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = stdout_tx.send(Err(error.to_string()));
                    break;
                }
            }
        }
    });

    let mut next_request_id = 1_u64;
    let mut raw_stdout = String::new();
    let mut parsed = CodexAppServerStreamingState::default();

    send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "initialize",
        serde_json::json!({
            "clientInfo": {
                "name": "omni-code-bridge",
                "title": "omni-code-bridge",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "experimentalApi": true,
            }
        }),
    )
    .await?;
    wait_for_json_rpc_response(&mut stdout_rx, &mut raw_stdout, next_request_id - 1).await?;

    let thread_request_id = send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "thread/start",
        serde_json::json!({
            "cwd": state
                .project_root_path_for_session(&session.id)
                .await
                .map_err(anyhow::Error::msg)?,
            "approvalPolicy": "on-request",
            "approvalsReviewer": "user",
            "sandbox": "workspace-write",
            "persistExtendedHistory": false,
        }),
    )
    .await?;
    let thread_response =
        wait_for_json_rpc_response(&mut stdout_rx, &mut raw_stdout, thread_request_id).await?;
    let thread_id = thread_response
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            thread_response
                .pointer("/thread/threadId")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .context("codex summary thread did not provide a thread id")?;

    let turn_request_id = send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "turn/start",
        serde_json::json!({
            "threadId": thread_id,
            "input": [{
                "type": "text",
                "text": summary_prompt(content),
                "text_elements": [],
            }],
        }),
    )
    .await?;

    loop {
        let Some(line) = stdout_rx.recv().await else {
            break;
        };
        let line = line.map_err(anyhow::Error::msg)?;
        if !raw_stdout.is_empty() {
            raw_stdout.push('\n');
        }
        raw_stdout.push_str(&line);

        let value = serde_json::from_str::<Value>(&line)
            .unwrap_or_else(|_| serde_json::json!({ "raw_line": line }));
        if value.get("id").and_then(jsonrpc_id_to_string).as_deref()
            == Some(&turn_request_id.to_string())
        {
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex summary failed");
                bail!("{message}");
            }
            continue;
        }

        match parsed.ingest_value(&value) {
            CodexAppServerEvent::TurnCompleted => break,
            CodexAppServerEvent::TurnFailed(message) => bail!("{message}"),
            CodexAppServerEvent::ApprovalRequested(_) => {
                bail!("codex summary unexpectedly requested approval")
            }
            _ => {}
        }
    }

    let status = child.wait().await?;
    let stderr = stderr_task
        .await
        .context("failed to join codex summary stderr reader")??;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("codex summary exited with status {code}");
        } else {
            bail!("codex summary exited with status {code}: {stderr}");
        }
    }

    let text = parsed
        .finish_text()
        .or_else(|| fallback_text(&raw_stdout))
        .context("codex summary did not include assistant text")?;
    Ok(normalize_summary_output(&text))
}

async fn summarize_with_claude_code(
    state: Arc<AppState>,
    session: &SessionSummary,
    content: &str,
) -> Result<String> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;

    let mut child = Command::new("claude");
    child
        .arg("-p")
        .arg("--output-format")
        .arg("stream-json")
        .arg(summary_prompt(content))
        .current_dir(project_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = child
        .spawn()
        .context("failed to spawn `claude` for summary")?;
    let stdout = child
        .stdout
        .take()
        .context("claude summary process did not expose stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("claude summary process did not expose stderr")?;

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = reader.next_line().await? {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
        }
        Ok::<_, std::io::Error>(output)
    });

    let mut parsed = ClaudeStreamingState::default();
    let mut final_text = None;
    let mut reader = BufReader::new(stdout).lines();

    while let Some(line) = reader.next_line().await? {
        let clean_line = strip_ansi(&line);
        if clean_line.trim().is_empty() {
            continue;
        }
        if parsed.ingest_line(&clean_line)?.is_some() {
            final_text = parsed.finish_text();
        }
    }

    let status = child.wait().await?;
    let stderr = stderr_task
        .await
        .context("failed to join claude summary stderr reader")??;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("claude summary exited with status {code}");
        } else {
            bail!("claude summary exited with status {code}: {stderr}");
        }
    }

    let text = final_text
        .or_else(|| parsed.finish_text())
        .context("claude summary did not include assistant text")?;
    Ok(normalize_summary_output(&text))
}

fn summary_prompt(content: &str) -> String {
    format!(
        "请把下面这段 AI 回复压缩成 50 到 60 个汉字，只输出最终摘要，不要标题、不要引号、不要列表、不要解释、不要补充新信息。\n\n原文如下：\n{content}"
    )
}

fn normalize_summary_output(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
        .trim_matches('"')
        .trim_matches('“')
        .trim_matches('”')
        .trim()
        .to_string()
}

async fn push_incremental_text(
    state: &AppState,
    session_id: &str,
    message_id: &str,
    last_rendered: &mut String,
    next_text: &str,
) {
    if let Some(delta) = next_text.strip_prefix(last_rendered.as_str()) {
        if !delta.is_empty() {
            state
                .emit_message_delta(session_id, message_id, delta)
                .await;
            *last_rendered = next_text.to_string();
        }
    }
}

#[derive(Default)]
struct CodexAppServerStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    partial_agent_messages: HashMap<String, String>,
    latest_status: Option<String>,
    session_id: Option<String>,
}

enum CodexAppServerEvent {
    None,
    Content(String),
    Status(String),
    ApprovalRequested(PendingApproval),
    ApprovalResolved { request_id: String },
    TurnCompleted,
    TurnFailed(String),
}

#[derive(Clone)]
struct PendingApproval {
    request: ApprovalRequest,
    last_choice: Option<ApprovalChoice>,
}

impl CodexAppServerStreamingState {
    fn current_status(&self) -> Option<&str> {
        self.latest_status.as_deref()
    }

    fn ingest_value(&mut self, value: &Value) -> CodexAppServerEvent {
        let Some(map) = value.as_object() else {
            return CodexAppServerEvent::None;
        };

        if map.get("id").is_some() && map.get("method").is_some() {
            return self.ingest_app_server_request(value);
        }

        if let Some(method) = map.get("method").and_then(Value::as_str) {
            let params = map.get("params").unwrap_or(&Value::Null);
            return match method {
                "thread/started" => {
                    if self.session_id.is_none() {
                        self.session_id = params
                            .pointer("/thread/id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                    }
                    CodexAppServerEvent::None
                }
                "mcpServer/startupStatus/updated" => render_codex_startup_status(params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or(CodexAppServerEvent::None),
                "thread/status/changed" => render_codex_thread_status(params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or(CodexAppServerEvent::None),
                "turn/started" => CodexAppServerEvent::Status("[turn] started".to_string()),
                "turn/plan/updated" => render_codex_plan_update(params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or(CodexAppServerEvent::None),
                "item/fileChange/outputDelta" => render_codex_file_change_delta(params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or(CodexAppServerEvent::None),
                "turn/diff/updated" => render_codex_turn_diff(params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or(CodexAppServerEvent::None),
                "thread/tokenUsage/updated" | "account/rateLimits/updated" => {
                    CodexAppServerEvent::None
                }
                "item/agentMessage/delta" => {
                    let item_id = params
                        .get("itemId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let delta = params
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if item_id.is_empty() || delta.is_empty() {
                        CodexAppServerEvent::None
                    } else {
                        let entry = self.partial_agent_messages.entry(item_id).or_default();
                        entry.push_str(delta);
                        self.render_assistant_text()
                            .map(CodexAppServerEvent::Content)
                            .unwrap_or(CodexAppServerEvent::None)
                    }
                }
                "item/started" | "item/completed" => self
                    .ingest_app_server_item(params.get("item").unwrap_or(&Value::Null), method)
                    .map(CodexAppServerEvent::Content)
                    .unwrap_or(CodexAppServerEvent::None),
                "serverRequest/resolved" => {
                    let request_id = params
                        .get("requestId")
                        .and_then(jsonrpc_id_to_string)
                        .unwrap_or_default();
                    if request_id.is_empty() {
                        CodexAppServerEvent::None
                    } else {
                        self.latest_status = None;
                        CodexAppServerEvent::ApprovalResolved { request_id }
                    }
                }
                "turn/completed" => {
                    let status = params
                        .pointer("/turn/status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    match status {
                        "completed" | "interrupted" => CodexAppServerEvent::TurnCompleted,
                        "failed" => {
                            let message = params
                                .pointer("/turn/error/message")
                                .and_then(Value::as_str)
                                .unwrap_or("codex turn failed")
                                .to_string();
                            CodexAppServerEvent::TurnFailed(message)
                        }
                        _ => CodexAppServerEvent::None,
                    }
                }
                "error" => {
                    let message = params
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("codex app-server error")
                        .to_string();
                    CodexAppServerEvent::TurnFailed(message)
                }
                _ => CodexAppServerEvent::Status(format!(
                    "[debug:codex:method] unhandled method={method}"
                )),
            };
        }

        CodexAppServerEvent::None
    }

    fn ingest_app_server_request(&mut self, value: &Value) -> CodexAppServerEvent {
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let request_id = value
            .get("id")
            .and_then(jsonrpc_id_to_string)
            .unwrap_or_default();
        let params = value.get("params").unwrap_or(&Value::Null);
        if request_id.is_empty() {
            return CodexAppServerEvent::None;
        }

        let request = match method {
            "item/commandExecution/requestApproval" => Some(PendingApproval {
                request: ApprovalRequest {
                    request_id: request_id.clone(),
                    kind: ApprovalKind::CommandExecution,
                    command: params
                        .get("command")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    reason: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    allow_accept_for_session: approval_decisions_contain(
                        params.get("availableDecisions"),
                        "acceptForSession",
                    ),
                    allow_cancel: approval_decisions_contain(
                        params.get("availableDecisions"),
                        "cancel",
                    ),
                    resolvable: true,
                },
                last_choice: None,
            }),
            "execCommandApproval" => Some(PendingApproval {
                request: ApprovalRequest {
                    request_id: request_id.clone(),
                    kind: ApprovalKind::ExecCommand,
                    command: params
                        .get("command")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            parts
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(" ")
                        })
                        .filter(|command| !command.is_empty()),
                    reason: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    allow_accept_for_session: true,
                    allow_cancel: true,
                    resolvable: true,
                },
                last_choice: None,
            }),
            "item/permissions/requestApproval" => Some(PendingApproval {
                request: ApprovalRequest {
                    request_id: request_id.clone(),
                    kind: ApprovalKind::Permissions,
                    command: None,
                    reason: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    allow_accept_for_session: false,
                    allow_cancel: true,
                    resolvable: true,
                },
                last_choice: None,
            }),
            _ => None,
        };

        if let Some(request) = request {
            self.latest_status = Some(render_approval_summary(&request.request));
            CodexAppServerEvent::ApprovalRequested(request)
        } else {
            CodexAppServerEvent::Status(format!(
                "[debug:codex:request] unhandled request method={method}"
            ))
        }
    }

    fn ingest_app_server_item(&mut self, item: &Value, method: &str) -> Option<String> {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match item_type {
            "agentMessage" if method == "item/completed" => {
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                let text = item.get("text").and_then(Value::as_str)?.trim().to_string();
                if text.is_empty() {
                    return None;
                }
                self.partial_agent_messages.remove(item_id);
                self.display_blocks.push(text.clone());
                self.assistant_blocks.push(text);
                self.render_assistant_text()
            }
            "agentMessage" => None,
            "userMessage" => None,
            "reasoning" => {
                self.latest_status = Some(if method == "item/started" {
                    "[reasoning] thinking".to_string()
                } else {
                    "[reasoning] complete".to_string()
                });
                None
            }
            "commandExecution" => {
                self.latest_status = Some(render_command_summary(
                    item.get("command")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    match item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                    {
                        "inProgress" => "running",
                        other => other,
                    },
                    item.get("exitCode").and_then(Value::as_i64),
                ));
                None
            }
            "webSearch" => {
                self.latest_status = Some(render_web_search_summary(
                    item.get("query")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    method,
                ));
                None
            }
            _ => {
                self.latest_status =
                    render_generic_item_summary(item_type, item, method).or_else(|| {
                        Some(format!(
                            "[debug:codex:item] unhandled item_type={} phase={}",
                            item_type,
                            if method == "item/started" {
                                "started"
                            } else {
                                "completed"
                            }
                        ))
                    });
                None
            }
        }
    }

    fn mark_idle_waiting(&mut self) -> Option<String> {
        let waiting = "[status] waiting for approval or blocked by sandbox/network";
        if self.latest_status.as_deref() == Some(waiting) {
            None
        } else {
            self.latest_status = Some(waiting.to_string());
            None
        }
    }

    fn finish_text(&self) -> Option<String> {
        let text = self.assistant_blocks.join("\n\n");
        if text.is_empty() { None } else { Some(text) }
    }

    fn render_assistant_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        if let Some((_, partial)) = self.partial_agent_messages.iter().last() {
            let partial = partial.trim();
            if !partial.is_empty() {
                blocks.push(partial.to_string());
            }
        }
        let text = blocks.join("\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

async fn send_json_rpc_request(
    writer: &mut (impl AsyncWrite + Unpin),
    next_request_id: &mut u64,
    method: &str,
    params: Value,
) -> Result<u64> {
    let request_id = *next_request_id;
    *next_request_id += 1;
    write_json_line(
        writer,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }),
    )
    .await?;
    Ok(request_id)
}

async fn send_json_rpc_response(
    writer: &mut (impl AsyncWrite + Unpin),
    request_id: &str,
    result: Value,
) -> Result<()> {
    let id = request_id
        .parse::<u64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::String(request_id.to_string()));
    write_json_line(
        writer,
        &serde_json::json!({
            "id": id,
            "result": result,
        }),
    )
    .await
}

async fn write_json_line(writer: &mut (impl AsyncWrite + Unpin), value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn wait_for_json_rpc_response(
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    request_id: u64,
) -> Result<Value> {
    loop {
        let line = stdout_rx
            .recv()
            .await
            .context("codex app-server closed before responding")?
            .map_err(anyhow::Error::msg)?;
        if !raw_stdout.is_empty() {
            raw_stdout.push('\n');
        }
        raw_stdout.push_str(&line);
        let value: Value = serde_json::from_str(&line)
            .with_context(|| "codex app-server produced invalid JSON")?;
        if value.get("id").and_then(jsonrpc_id_to_string).as_deref()
            == Some(&request_id.to_string())
        {
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex app-server request failed");
                bail!("{message}");
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
    }
}

fn fallback_text(raw_stdout: &str) -> Option<String> {
    raw_stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|value| {
            value
                .pointer("/params/item/text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(ToString::to_string)
        })
        .next_back()
}

fn jsonrpc_id_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn approval_decisions_contain(value: Option<&Value>, expected: &str) -> bool {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items.iter().any(|item| {
                item.as_str() == Some(expected)
                    || item
                        .as_object()
                        .map(|map| map.contains_key(expected))
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn render_approval_summary(request: &ApprovalRequest) -> String {
    let command = request
        .command
        .as_deref()
        .filter(|command| !command.is_empty())
        .unwrap_or("unknown command");
    let reason = request.reason.as_deref().unwrap_or("等待用户审批");
    format!("[approval] {reason}: {command}")
}

fn render_command_summary(command: &str, status: &str, exit_code: Option<i64>) -> String {
    let command = if command.trim().is_empty() {
        "command"
    } else {
        command
    };
    match exit_code {
        Some(code) => format!("[command:{status}] {command} (exit {code})"),
        None => format!("[command:{status}] {command}"),
    }
}

fn render_web_search_summary(query: &str, method: &str) -> String {
    let query = if query.trim().is_empty() {
        "search"
    } else {
        query
    };
    match method {
        "item/started" => format!("[web] searching: {query}"),
        _ => format!("[web] finished: {query}"),
    }
}

fn render_codex_startup_status(params: &Value) -> Option<String> {
    let status = params
        .get("status")
        .or_else(|| params.pointer("/startupStatus/status"))
        .and_then(Value::as_str)?;
    match status {
        "ready" | "completed" => None,
        other => Some(format!("[mcp] startup {other}")),
    }
}

fn render_codex_thread_status(params: &Value) -> Option<String> {
    let status = params
        .get("status")
        .or_else(|| params.pointer("/thread/status"))
        .and_then(Value::as_str)?;
    let summary = match status {
        "running" => "[thread] running",
        "waiting" => "[thread] waiting",
        "idle" => return None,
        other => return Some(format!("[thread] {other}")),
    };
    Some(summary.to_string())
}

fn render_codex_plan_update(params: &Value) -> Option<String> {
    let steps = params
        .get("steps")
        .or_else(|| params.pointer("/plan/steps"))
        .or_else(|| params.get("items"))
        .or_else(|| params.pointer("/plan/items"))
        .and_then(Value::as_array)?;
    let previews = steps
        .iter()
        .filter_map(|step| {
            step.get("title")
                .or_else(|| step.get("step"))
                .or_else(|| step.get("content"))
                .or_else(|| step.get("text"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(ToString::to_string)
        })
        .take(3)
        .collect::<Vec<_>>();
    let count = steps.len();
    if previews.is_empty() {
        Some(format!("[plan] updated ({count} steps)"))
    } else {
        Some(format!("[plan] {} ({count} steps)", previews.join(" | ")))
    }
}

fn render_codex_file_change_delta(params: &Value) -> Option<String> {
    let files = collect_file_paths(params);
    let preview = if files.is_empty() {
        params
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| params.get("output").and_then(Value::as_str))
            .or_else(|| params.get("text").and_then(Value::as_str))
            .map(|text| truncate_tool_text(text, 120))
            .filter(|text| !text.is_empty())?
    } else {
        format_file_preview(&files)
    };
    Some(format!("[file:delta] {preview}"))
}

fn render_codex_turn_diff(params: &Value) -> Option<String> {
    let files = collect_file_paths(params);
    let mut segments = Vec::new();
    if !files.is_empty() {
        segments.push(format_file_preview(&files));
    }

    let added = find_first_i64(
        params,
        &[
            "added",
            "insertions",
            "linesAdded",
            "additions",
            "totalAdded",
        ],
    );
    let removed = find_first_i64(
        params,
        &[
            "removed",
            "deletions",
            "linesRemoved",
            "totalRemoved",
            "deletionsCount",
        ],
    );

    match (added, removed) {
        (Some(added), Some(removed)) => segments.push(format!("+{added} -{removed}")),
        (Some(added), None) => segments.push(format!("+{added}")),
        (None, Some(removed)) => segments.push(format!("-{removed}")),
        (None, None) => {}
    }

    let summary = params
        .get("summary")
        .or_else(|| params.get("title"))
        .or_else(|| params.get("description"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 80))
        .filter(|text| !text.is_empty());
    if let Some(summary) = summary {
        segments.push(summary);
    }

    if segments.is_empty() {
        None
    } else {
        Some(format!("[diff] {}", segments.join(" | ")))
    }
}

fn render_generic_item_summary(item_type: &str, item: &Value, method: &str) -> Option<String> {
    let normalized = item_type.trim();
    if normalized.is_empty() {
        return None;
    }

    let phase = if method == "item/started" {
        "running"
    } else {
        "done"
    };

    if normalized.to_ascii_lowercase().contains("todo") {
        let todo_items = item
            .get("todos")
            .or_else(|| item.get("items"))
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|entry| {
                        entry
                            .get("content")
                            .or_else(|| entry.get("text"))
                            .or_else(|| entry.get("title"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                            .map(ToString::to_string)
                    })
                    .take(3)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let count = item
            .get("todos")
            .or_else(|| item.get("items"))
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(todo_items.len());
        return Some(if todo_items.is_empty() {
            format!("[todo:{phase}] {count} items")
        } else {
            format!("[todo:{phase}] {} ({count} items)", todo_items.join(" | "))
        });
    }

    let files = collect_file_paths(item);
    if !files.is_empty() {
        let preview = files.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
        return Some(format!(
            "[file:{phase}] {}{}",
            preview,
            if files.len() > 3 {
                format!(" +{} more", files.len() - 3)
            } else {
                String::new()
            }
        ));
    }

    let output = item
        .get("stdout")
        .or_else(|| item.get("stderr"))
        .or_else(|| item.get("output"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 120));
    if let Some(output) = output.filter(|text| !text.is_empty()) {
        return Some(format!("[{normalized}:{phase}] {output}"));
    }

    let title = item
        .get("title")
        .or_else(|| item.get("description"))
        .or_else(|| item.get("name"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 120));
    title.map(|title| format!("[{normalized}:{phase}] {title}"))
}

fn collect_file_paths(value: &Value) -> Vec<String> {
    let mut results = Vec::new();
    collect_file_paths_inner(value, &mut results);
    results.sort();
    results.dedup();
    results
}

fn format_file_preview(files: &[String]) -> String {
    let preview = files.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
    if files.len() > 3 {
        format!("{preview} +{} more", files.len() - 3)
    } else {
        preview
    }
}

fn find_first_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(number) = map.get(*key).and_then(Value::as_i64) {
                    return Some(number);
                }
            }
            for nested in map.values() {
                if let Some(number) = find_first_i64(nested, keys) {
                    return Some(number);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|item| find_first_i64(item, keys)),
        _ => None,
    }
}

fn collect_file_paths_inner(value: &Value, results: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for key in ["filePath", "file_path", "path", "target_file"] {
                if let Some(path) = map.get(key).and_then(Value::as_str) {
                    let path = path.trim();
                    if !path.is_empty() && looks_like_file_path(path) {
                        results.push(path.to_string());
                    }
                }
            }
            for nested in map.values() {
                collect_file_paths_inner(nested, results);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_file_paths_inner(item, results);
            }
        }
        _ => {}
    }
}

fn looks_like_file_path(path: &str) -> bool {
    path.contains('/') || path.contains('.') || path.contains('\\')
}

fn truncate_tool_text(text: &str, max_chars: usize) -> String {
    let text = text.trim().replace('\n', " ");
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

#[derive(Default)]
struct ClaudeStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    latest_status: Option<String>,
    partial_text: Option<String>,
}

impl ClaudeStreamingState {
    fn current_status(&self) -> Option<&str> {
        self.latest_status.as_deref()
    }

    fn ingest_line(&mut self, line: &str) -> Result<Option<String>> {
        let value = serde_json::from_str::<Value>(line)
            .with_context(|| "Claude stream produced invalid JSON")?;
        let Some(map) = value.as_object() else {
            return Ok(None);
        };

        match map.get("type").and_then(Value::as_str).unwrap_or_default() {
            "assistant" => Ok(self.ingest_assistant_text(
                map.get("message")
                    .and_then(extract_text_from_json)
                    .or_else(|| map.get("content").and_then(extract_text_from_json))
                    .or_else(|| map.get("text").and_then(extract_text_from_json)),
            )),
            "system" => {
                self.latest_status = map
                    .get("subtype")
                    .and_then(Value::as_str)
                    .map(|subtype| format!("[claude] {subtype}"));
                Ok(None)
            }
            "result" => {
                if map
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    let message = map
                        .get("result")
                        .and_then(extract_text_from_json)
                        .unwrap_or_else(|| "claude run failed".to_string());
                    bail!("{message}");
                }
                if let Some(text) = map
                    .get("result")
                    .and_then(extract_text_from_json)
                    .or_else(|| map.get("output_text").and_then(extract_text_from_json))
                    .or_else(|| map.get("text").and_then(extract_text_from_json))
                {
                    self.partial_text = None;
                    if self.assistant_blocks.last() != Some(&text) {
                        self.display_blocks.push(text.clone());
                        self.assistant_blocks.push(text);
                    }
                    self.latest_status = None;
                    Ok(self.render_assistant_text())
                } else {
                    Ok(None)
                }
            }
            other => {
                self.latest_status = Some(format!("[debug:claude:type] unhandled type={other}"));
                Ok(None)
            }
        }
    }

    fn ingest_assistant_text(&mut self, text: Option<String>) -> Option<String> {
        let text = text?.trim().to_string();
        if text.is_empty() {
            return None;
        }
        self.partial_text = Some(text);
        self.latest_status = None;
        self.render_assistant_text()
    }

    fn ingest_external_status(&mut self, summary: String) -> Option<String> {
        self.latest_status = Some(summary);
        None
    }

    fn finish_text(&self) -> Option<String> {
        if let Some(text) = self.partial_text.as_ref() {
            let text = text.trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
        let text = self.assistant_blocks.join("\n\n");
        if text.trim().is_empty() {
            None
        } else {
            Some(text)
        }
    }

    fn render_assistant_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        if let Some(text) = self.partial_text.as_ref() {
            let text = text.trim();
            if !text.is_empty() {
                blocks.push(text.to_string());
            }
        }
        let text = blocks.join("\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

fn extract_text_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| match item.get("type").and_then(Value::as_str) {
                    Some("text") => item
                        .get("text")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    _ => extract_text_from_json(item),
                })
                .collect::<Vec<_>>()
                .join("");
            let text = text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        Value::Object(map) => map
            .get("content")
            .and_then(extract_text_from_json)
            .or_else(|| map.get("message").and_then(extract_text_from_json))
            .or_else(|| map.get("text").and_then(extract_text_from_json))
            .or_else(|| map.get("result").and_then(extract_text_from_json))
            .or_else(|| map.get("output_text").and_then(extract_text_from_json)),
        _ => None,
    }
}

fn strip_ansi(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if matches!(chars.peek(), Some('[') | Some(']')) {
                chars.next();
                while let Some(next) = chars.next() {
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
                continue;
            }
        }
        result.push(ch);
    }
    result
}

async fn next_claude_permission_request(
    state_dir: &std::path::Path,
    run_id: &str,
    seen: &mut std::collections::HashSet<String>,
) -> Result<Option<ClaudePermissionRequest>> {
    let mut entries = match tokio::fs::read_dir(state_dir.join("requests")).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if seen.contains(stem) {
            continue;
        }
        let body = tokio::fs::read(&path).await?;
        let request: ClaudePermissionRequest = serde_json::from_slice(&body)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        if request.run_id != run_id {
            continue;
        }
        seen.insert(request.request_id.clone());
        return Ok(Some(request));
    }
    Ok(None)
}

async fn next_claude_status_event(
    state_dir: &std::path::Path,
    run_id: &str,
    seen: &mut std::collections::HashSet<String>,
) -> Result<Option<ClaudeHookStatusEvent>> {
    let mut entries = match tokio::fs::read_dir(state_dir.join("events")).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if seen.contains(stem) {
            continue;
        }
        let body = tokio::fs::read(&path).await?;
        let event: ClaudeHookStatusEvent = serde_json::from_slice(&body)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        if event.run_id != run_id {
            continue;
        }
        seen.insert(event.event_id.clone());
        let _ = tokio::fs::remove_file(&path).await;
        return Ok(Some(event));
    }
    Ok(None)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

async fn auto_approve_codex_request(
    request: &ApprovalRequest,
    project_root: &std::path::Path,
) -> Result<Option<ApprovalChoice>> {
    match request.kind {
        ApprovalKind::CommandExecution | ApprovalKind::ExecCommand => {
            let Some(command) = request.command.as_deref() else {
                return Ok(None);
            };
            if let Some(decision) = approval_policy::should_auto_approve(command, project_root) {
                return Ok(match decision {
                    AutoApprovalDecision::Accept => Some(ApprovalChoice::Accept),
                });
            }
            let decision = match ai_approval::review_request(request, project_root).await {
                Ok(decision) => decision,
                Err(error) => {
                    eprintln!(
                        "AI approval review failed; falling back to user approval: {error:?}"
                    );
                    None
                }
            };
            Ok(match decision.map(|decision| decision.decision) {
                Some(AiApprovalDecisionKind::Accept) => Some(ApprovalChoice::Accept),
                Some(AiApprovalDecisionKind::Decline) => Some(ApprovalChoice::Decline),
                Some(AiApprovalDecisionKind::AskUser) | None => None,
            })
        }
        ApprovalKind::Permissions => Ok(None),
    }
}

fn approval_result_json(choice: &ApprovalChoice, kind: &ApprovalKind) -> Value {
    match kind {
        ApprovalKind::CommandExecution => serde_json::json!({
            "decision": match choice {
                ApprovalChoice::Accept | ApprovalChoice::AlwaysAllow => Value::String("accept".to_string()),
                ApprovalChoice::AcceptForSession => Value::String("acceptForSession".to_string()),
                ApprovalChoice::Decline => Value::String("decline".to_string()),
                ApprovalChoice::Cancel => Value::String("cancel".to_string()),
            }
        }),
        ApprovalKind::ExecCommand => serde_json::json!({
            "decision": match choice {
                ApprovalChoice::Accept | ApprovalChoice::AlwaysAllow => Value::String("approved".to_string()),
                ApprovalChoice::AcceptForSession => Value::String("approved_for_session".to_string()),
                ApprovalChoice::Decline => Value::String("denied".to_string()),
                ApprovalChoice::Cancel => Value::String("abort".to_string()),
            }
        }),
        ApprovalKind::Permissions => match choice {
            ApprovalChoice::Accept
            | ApprovalChoice::AcceptForSession
            | ApprovalChoice::AlwaysAllow => serde_json::json!({
                "permissions": {
                    "network": { "enabled": true }
                },
                "scope": if matches!(choice, ApprovalChoice::AcceptForSession) {
                    "session"
                } else {
                    "turn"
                },
            }),
            ApprovalChoice::Decline | ApprovalChoice::Cancel => serde_json::json!({
                "permissions": {},
                "scope": "turn",
            }),
        },
    }
}
