use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::mpsc,
    time::sleep,
};

#[cfg(test)]
use tokio::io::AsyncReadExt;

use crate::{
    ai_approval::{self, AiApprovalDecisionKind},
    app_state::AppState,
    approval_policy::{self, AutoApprovalDecision},
    bridge_settings::BridgeSettings,
    claude_hook::{
        ClaudeHookStatusEvent, ClaudePermissionRequest, append_always_allow_command,
        claude_state_dir, ensure_runtime_dirs, request_path, response_from_choice, response_path,
    },
    claude_store::{load_claude_archive_summary, load_claude_messages},
    models::{
        AcpAgentDiagnostic, AcpHandshakeProbe, AcpProfile, AgentKind, ApprovalChoice, ApprovalKind,
        ApprovalRequest, ChatMessage, ProjectSummary, ReasoningEffort, ResolvedProviderConfig,
        SessionSummary,
    },
    session_store::{load_session_archive_summary, load_session_messages},
};

const CODEX_IDLE_TICK_SECONDS: u64 = 15;
const CODEX_COMMAND_SOFT_RECOVERY_IDLE_TICKS: u32 = 8;
const CODEX_COMMAND_STALLED_IDLE_TICKS: u32 = 20;
const ACP_JSON_RPC_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const ACP_JSON_RPC_CANCEL_TIMEOUT: Duration = Duration::from_secs(5);
const ACP_READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const AGENT_READINESS_CACHE_TTL: Duration = Duration::from_secs(5);
#[cfg(test)]
static ACP_JSON_RPC_HANDSHAKE_TIMEOUT_TEST_MS: AtomicU64 = AtomicU64::new(0);

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
        system_prompt: Option<String>,
        reply: ChatMessage,
        provider_config: Option<ResolvedProviderConfig>,
        reasoning_effort: Option<ReasoningEffort>,
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
        providers.insert(AgentKind::OpenCode, Arc::new(OpenCodeProvider::new()));
        providers.insert(AgentKind::Acp, Arc::new(AcpProvider::new()));
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
        _system_prompt: Option<String>,
        reply: ChatMessage,
        _provider_config: Option<ResolvedProviderConfig>,
        _reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<()> {
        let prefix = match self.kind {
            AgentKind::ClaudeCode => "ClaudeCode",
            AgentKind::OpenCode => "OpenCode",
            AgentKind::Acp => "ACP",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge_settings::{AiApprovalSettings, BridgeSettingsInput};
    use crate::models::{
        AUTO_PROVIDER_ID, AcpProfile, AcpServerConfig, HeaderKeyValue, InputMode, MessageRole,
        SendMessageInput, SessionStatus,
    };
    use std::time::Duration;

    #[test]
    fn provider_registry_has_provider_for_every_agent_kind() {
        let registry = ProviderRegistry::new();

        for kind in AgentKind::ALL {
            assert!(
                registry.get(kind).is_some(),
                "missing provider for {kind:?}"
            );
        }
    }

    #[test]
    fn codex_streaming_state_parses_content_status_and_completion() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "method": "item/agentMessage/delta",
            "params": {
                "itemId": "msg-1",
                "delta": "hello"
            }
        })) {
            CodexAppServerEvent::Content(text) => assert_eq!(text, "hello"),
            _ => panic!("expected content delta"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "msg-1",
                    "type": "agentMessage",
                    "text": "hello world"
                }
            }
        })) {
            CodexAppServerEvent::Content(text) => assert_eq!(text, "hello world"),
            _ => panic!("expected completed content"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "turn/completed",
            "params": {
                "turn": {
                    "status": "completed"
                }
            }
        })) {
            CodexAppServerEvent::TurnCompleted => {}
            _ => panic!("expected turn completion"),
        }

        assert_eq!(state.finish_text().as_deref(), Some("hello world"));
    }

    #[test]
    fn codex_streaming_state_keeps_partial_text_when_turn_completes_early() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "method": "item/agentMessage/delta",
            "params": {
                "itemId": "msg-1",
                "delta": "partial answer"
            }
        })) {
            CodexAppServerEvent::Content(text) => assert_eq!(text, "partial answer"),
            _ => panic!("expected content delta"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "turn/completed",
            "params": {
                "turn": {
                    "status": "completed"
                }
            }
        })) {
            CodexAppServerEvent::TurnCompleted => {}
            _ => panic!("expected turn completion"),
        }

        assert_eq!(state.finish_text().as_deref(), Some("partial answer"));
    }

    #[test]
    fn codex_streaming_state_renders_multiple_partial_blocks_in_order() {
        let mut state = CodexAppServerStreamingState::default();

        let _ = state.ingest_value(&serde_json::json!({
            "method": "item/agentMessage/delta",
            "params": {
                "itemId": "msg-1",
                "delta": "first"
            }
        }));
        let rendered = state.ingest_value(&serde_json::json!({
            "method": "item/agentMessage/delta",
            "params": {
                "itemId": "msg-2",
                "delta": "second"
            }
        }));

        match rendered {
            CodexAppServerEvent::Content(text) => assert_eq!(text, "first\n\n---\n\nsecond"),
            _ => panic!("expected combined partial content"),
        }
    }

    #[test]
    fn codex_streaming_state_maps_approval_requests() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "id": 7,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "command": "cargo test",
                "reason": "run tests",
                "availableDecisions": ["accept", "acceptForSession", "cancel"]
            }
        })) {
            CodexAppServerEvent::ApprovalRequested(pending) => {
                assert_eq!(pending.request.request_id, "7");
                assert_eq!(pending.request.command.as_deref(), Some("cargo test"));
                assert_eq!(pending.request.reason.as_deref(), Some("run tests"));
                assert!(pending.request.allow_accept_for_session);
                assert!(pending.request.allow_cancel);
                assert!(pending.request.resolvable);
            }
            _ => panic!("expected approval request"),
        }
    }

    #[test]
    fn codex_streaming_state_maps_file_change_approval_requests() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "id": 8,
            "method": "item/fileChange/requestApproval",
            "params": {
                "reason": "Would you like to make the following edits?",
                "availableDecisions": ["accept", "acceptForSession", "cancel"],
                "fileChanges": [
                    { "path": "src/adapter.rs" },
                    { "path": "src/models.rs" }
                ]
            }
        })) {
            CodexAppServerEvent::ApprovalRequested(pending) => {
                assert_eq!(pending.request.request_id, "8");
                assert!(matches!(pending.request.kind, ApprovalKind::FileChange));
                assert_eq!(
                    pending.request.command.as_deref(),
                    Some("edit files: src/adapter.rs, src/models.rs")
                );
                assert_eq!(
                    pending.request.reason.as_deref(),
                    Some("Would you like to make the following edits?")
                );
                assert!(pending.request.allow_accept_for_session);
                assert!(pending.request.allow_cancel);
                assert!(pending.request.resolvable);
            }
            _ => panic!("expected file change approval request"),
        }
    }

    #[test]
    fn codex_streaming_state_maps_legacy_apply_patch_approval_requests() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "id": "patch-1",
            "method": "applyPatchApproval",
            "params": {
                "files": ["src/adapter.rs"]
            }
        })) {
            CodexAppServerEvent::ApprovalRequested(pending) => {
                assert_eq!(pending.request.request_id, "patch-1");
                assert!(matches!(pending.request.kind, ApprovalKind::ApplyPatch));
                assert_eq!(
                    pending.request.command.as_deref(),
                    Some("edit files: src/adapter.rs")
                );
                assert!(pending.request.allow_accept_for_session);
                assert!(pending.request.allow_cancel);
                assert!(pending.request.resolvable);
            }
            _ => panic!("expected apply patch approval request"),
        }
    }

    #[test]
    fn codex_streaming_state_maps_tool_process_status_events() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "method": "thread/name/updated",
            "params": {
                "threadId": "thread-1",
                "threadName": "Investigate provider switching"
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[thread] renamed: Investigate provider switching")
            }
            _ => panic!("expected thread rename status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "item/commandExecution/outputDelta",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "cmd-1",
                "delta": "running tests"
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[command:output] running tests")
            }
            _ => panic!("expected command output status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "command/exec/outputDelta",
            "params": {
                "processId": "proc-1",
                "stream": "stdout",
                "deltaBase64": "b2sK",
                "capReached": false
            }
        })) {
            CodexAppServerEvent::Status(status) => assert_eq!(status, "[exec]:stdout ok"),
            _ => panic!("expected exec output status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "process/exited",
            "params": {
                "processHandle": "proc-1",
                "exitCode": 0,
                "stdout": "done",
                "stderr": "",
                "stdoutCapReached": false,
                "stderrCapReached": false
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[process] exit 0 | stdout: done")
            }
            _ => panic!("expected process exit status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "warning",
            "params": {
                "threadId": "thread-1",
                "message": "network unavailable"
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[warning] network unavailable")
            }
            _ => panic!("expected warning status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "fs/changed",
            "params": {
                "watchId": "watch-1",
                "changedPaths": ["/tmp/a.txt", "/tmp/b.txt"]
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[fs] changed /tmp/a.txt, /tmp/b.txt")
            }
            _ => panic!("expected fs status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "thread/realtime/transcript/done",
            "params": {
                "threadId": "thread-1",
                "role": "user",
                "text": "hello realtime"
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[realtime:user:done] hello realtime")
            }
            _ => panic!("expected realtime transcript status"),
        }

        match state.ingest_value(&serde_json::json!({
            "method": "account/login/completed",
            "params": {
                "loginId": "login-1",
                "success": false,
                "error": "denied"
            }
        })) {
            CodexAppServerEvent::Status(status) => {
                assert_eq!(status, "[account] login failed: denied")
            }
            _ => panic!("expected account login status"),
        }
    }

    #[test]
    fn codex_streaming_state_tracks_stalled_commands() {
        let mut state = CodexAppServerStreamingState::default();

        match state.ingest_value(&serde_json::json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "command": "flutter test",
                    "status": "inProgress"
                }
            }
        })) {
            CodexAppServerEvent::None => {}
            _ => panic!("expected command start to produce no content event"),
        }
        assert_eq!(
            state
                .running_command
                .as_ref()
                .map(|item| item.command.as_str()),
            Some("flutter test")
        );

        for _ in 1..CODEX_COMMAND_SOFT_RECOVERY_IDLE_TICKS {
            assert!(state.mark_idle_waiting(false).is_none());
            assert_eq!(
                state.current_status(),
                Some("[status] command still running")
            );
        }

        match state.mark_idle_waiting(false) {
            Some(CommandWatchdogAction::SoftRecovery { command }) => {
                assert_eq!(command, "flutter test");
            }
            _ => panic!("expected soft recovery"),
        }
        assert_eq!(
            state.current_status(),
            Some("[command:stalled] flutter test (no output yet; waiting once more)")
        );

        for _ in (CODEX_COMMAND_SOFT_RECOVERY_IDLE_TICKS + 1)..CODEX_COMMAND_STALLED_IDLE_TICKS {
            assert!(state.mark_idle_waiting(false).is_none());
        }

        match state.mark_idle_waiting(false) {
            Some(CommandWatchdogAction::Stalled { command }) => {
                assert_eq!(command, "flutter test");
            }
            _ => panic!("expected stalled command"),
        }
    }

    #[test]
    fn codex_streaming_state_clears_completed_commands() {
        let mut state = CodexAppServerStreamingState::default();

        let _ = state.ingest_value(&serde_json::json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "command": "cargo test",
                    "status": "inProgress"
                }
            }
        }));
        assert!(state.running_command.is_some());

        let _ = state.ingest_value(&serde_json::json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "command": "cargo test",
                    "status": "completed",
                    "exitCode": 0
                }
            }
        }));
        assert!(state.running_command.is_none());
    }

    #[test]
    fn codex_approval_result_serializes_file_change_decisions() {
        assert_eq!(
            approval_result_json(&ApprovalChoice::Accept, &ApprovalKind::FileChange),
            serde_json::json!({ "decision": "accept" })
        );
        assert_eq!(
            approval_result_json(&ApprovalChoice::AcceptForSession, &ApprovalKind::FileChange),
            serde_json::json!({ "decision": "acceptForSession" })
        );
        assert_eq!(
            approval_result_json(&ApprovalChoice::AcceptForSession, &ApprovalKind::ApplyPatch),
            serde_json::json!({ "decision": "approved_for_session" })
        );
    }

    #[test]
    fn claude_streaming_state_parses_assistant_result_and_errors() {
        let mut state = ClaudeStreamingState::default();

        let rendered = state
            .ingest_line(
                r#"{"type":"assistant","message":{"content":[{"type":"text","text":"partial"}]}}"#,
            )
            .expect("valid assistant event");
        assert_eq!(rendered.as_deref(), Some("partial"));
        assert_eq!(state.finish_text().as_deref(), Some("partial"));

        let rendered = state
            .ingest_line(r#"{"type":"result","result":"final answer","is_error":false}"#)
            .expect("valid result event");
        assert_eq!(rendered.as_deref(), Some("final answer"));
        assert_eq!(state.finish_text().as_deref(), Some("final answer"));

        let error = state
            .ingest_line(r#"{"type":"result","result":"failed","is_error":true}"#)
            .expect_err("error result should fail");
        assert!(error.to_string().contains("failed"));
    }

    #[test]
    fn claude_streaming_state_parses_stream_events() {
        let mut state = ClaudeStreamingState::default();

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"message_start","message":{"model":"claude-sonnet-4"}}}"#,
            )
            .expect("valid message start");
        assert_eq!(rendered, None);
        assert_eq!(
            state.current_status(),
            Some("[claude] message started: claude-sonnet-4")
        );

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}"#,
            )
            .expect("valid content block start");
        assert_eq!(rendered, None);

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}}"#,
            )
            .expect("valid content block delta");
        assert_eq!(rendered.as_deref(), Some("hello"));
        assert_eq!(state.finish_text().as_deref(), Some("hello"));

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}}"#,
            )
            .expect("valid second content block delta");
        assert_eq!(rendered.as_deref(), Some("hello world"));

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"content_block_stop","index":0}}"#,
            )
            .expect("valid content block stop");
        assert_eq!(rendered.as_deref(), Some("hello world"));
        assert_eq!(state.finish_text().as_deref(), Some("hello world"));

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}}}}"#,
            )
            .expect("valid tool block start");
        assert_eq!(rendered, None);
        assert_eq!(
            state.current_status(),
            Some("[claude:Bash] started: cargo test")
        );

        let rendered = state
            .ingest_line(
                r#"{"type":"stream_event","event":{"type":"unknown_new_event","value":true}}"#,
            )
            .expect("valid unknown stream event");
        assert_eq!(rendered, None);
        assert_eq!(
            state.current_status(),
            Some("[debug:claude:event] unhandled type=unknown_new_event")
        );
    }

    #[test]
    fn context_migration_instructions_include_recent_history_without_current_turn() {
        let current_input = ChatMessage {
            id: "input-now".to_string(),
            session_id: "session".to_string(),
            role: MessageRole::User,
            content: "最新问题".to_string(),
            created_at: chrono::Utc::now(),
        };
        let pending_reply = ChatMessage {
            id: "reply-now".to_string(),
            session_id: "session".to_string(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: chrono::Utc::now(),
        };
        let messages = vec![
            ChatMessage {
                id: "old-user".to_string(),
                session_id: "session".to_string(),
                role: MessageRole::User,
                content: "先看 provider 配置".to_string(),
                created_at: chrono::Utc::now(),
            },
            ChatMessage {
                id: "old-assistant".to_string(),
                session_id: "session".to_string(),
                role: MessageRole::Assistant,
                content: "已确认请求体发的是 gpt-5.5".to_string(),
                created_at: chrono::Utc::now(),
            },
            current_input.clone(),
            pending_reply.clone(),
        ];

        let instructions =
            build_context_migration_instructions(&messages, &current_input, &pending_reply)
                .expect("history should be generated");

        assert!(instructions.contains("先看 provider 配置"));
        assert!(instructions.contains("已确认请求体发的是 gpt-5.5"));
        assert!(!instructions.contains("最新问题"));
    }

    #[test]
    fn claude_runtime_ref_selection_respects_resume_and_model_switch() {
        let session_id = "local-session";
        let existing_runtime_ref = "claude-runtime";

        assert_eq!(
            select_claude_runtime_ref(session_id, None, false),
            session_id
        );
        assert_eq!(
            select_claude_runtime_ref(session_id, Some(existing_runtime_ref), true),
            existing_runtime_ref
        );
        assert_eq!(
            select_claude_runtime_ref(session_id, Some(existing_runtime_ref), false),
            existing_runtime_ref
        );
    }

    #[test]
    fn context_migration_instructions_keep_recent_messages_when_history_is_long() {
        let current_input = ChatMessage {
            id: "input-now".to_string(),
            session_id: "session".to_string(),
            role: MessageRole::User,
            content: "当前输入".to_string(),
            created_at: chrono::Utc::now(),
        };
        let pending_reply = ChatMessage {
            id: "reply-now".to_string(),
            session_id: "session".to_string(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: chrono::Utc::now(),
        };
        let messages = (0..15)
            .map(|index| ChatMessage {
                id: format!("msg-{index}"),
                session_id: "session".to_string(),
                role: if index % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Assistant
                },
                content: format!("历史消息 {index}"),
                created_at: chrono::Utc::now(),
            })
            .collect::<Vec<_>>();

        let instructions =
            build_context_migration_instructions(&messages, &current_input, &pending_reply)
                .expect("history should be generated");

        assert!(!instructions.contains("历史消息 0"));
        assert!(!instructions.contains("历史消息 2"));
        assert!(instructions.contains("历史消息 14"));
    }

    #[test]
    fn codex_thread_action_resumes_when_provider_matches_even_if_model_changes() {
        assert_eq!(
            decide_codex_thread_action(Some("thread-1"), Some("provider-a"), Some("provider-a")),
            CodexThreadDecision::Resume
        );
    }

    #[test]
    fn codex_thread_action_migrates_when_provider_changes() {
        assert_eq!(
            decide_codex_thread_action(Some("thread-1"), Some("provider-a"), Some("provider-b")),
            CodexThreadDecision::StartWithMigration
        );
        assert_eq!(
            decide_codex_thread_action(None, Some("provider-a"), Some("provider-a")),
            CodexThreadDecision::StartFresh
        );
    }

    #[test]
    fn codex_thread_action_resumes_when_provider_metadata_is_missing() {
        assert_eq!(
            decide_codex_thread_action(Some("thread-1"), None, Some("provider-a")),
            CodexThreadDecision::Resume
        );
        assert_eq!(
            decide_codex_thread_action(Some("thread-1"), Some("provider-a"), None),
            CodexThreadDecision::Resume
        );
    }

    #[test]
    fn parse_slash_command_extracts_name_and_args() {
        let command = parse_slash_command("/model gpt-5").expect("should parse");
        assert_eq!(command.name, "model");
        assert_eq!(command.args, "gpt-5");

        let no_args = parse_slash_command("  /clear  ").expect("should parse");
        assert_eq!(no_args.name, "clear");
        assert_eq!(no_args.args, "");
    }

    #[test]
    fn parse_slash_command_rejects_plain_text() {
        assert!(parse_slash_command("hello").is_none());
        assert!(parse_slash_command("/").is_none());
        assert!(parse_slash_command("//bad").is_none());
    }

    #[test]
    fn claude_prompt_input_wraps_slash_commands() {
        assert_eq!(
            claude_prompt_input("/clear"),
            "<command-name>/clear</command-name>\n<command-message>clear</command-message>\n<command-args></command-args>"
        );
        assert_eq!(
            claude_prompt_input("/skill-creator 写一个 <skill>"),
            "<command-name>/skill-creator</command-name>\n<command-message>skill-creator</command-message>\n<command-args>写一个 &lt;skill&gt;</command-args>"
        );
    }

    #[test]
    fn claude_prompt_input_keeps_plain_text_unchanged() {
        assert_eq!(claude_prompt_input("fix this bug"), "fix this bug");
    }

    #[test]
    fn classify_codex_slash_command_maps_supported_commands() {
        assert_eq!(
            classify_codex_slash_command("/compact"),
            Some(CodexSlashAction::Compact)
        );
        assert_eq!(
            classify_codex_slash_command("/review"),
            Some(CodexSlashAction::ReviewUncommittedChanges)
        );
        assert_eq!(
            classify_codex_slash_command("/review check auth flow"),
            Some(CodexSlashAction::ReviewCustom {
                instructions: "check auth flow"
            })
        );
        assert_eq!(
            classify_codex_slash_command("/rename New Session"),
            Some(CodexSlashAction::Rename {
                title: "New Session"
            })
        );
        assert_eq!(
            classify_codex_slash_command("/goal finish bridge slash forwarding"),
            Some(CodexSlashAction::GoalSet {
                objective: "finish bridge slash forwarding"
            })
        );
        assert_eq!(
            classify_codex_slash_command("/clear-goal"),
            Some(CodexSlashAction::GoalClear)
        );
        assert_eq!(classify_codex_slash_command("/model gpt-5"), None);
    }

    #[tokio::test]
    async fn stub_provider_completes_when_called_directly() {
        let state = Arc::new(AppState::new().await);
        let provider = StubProvider::new(AgentKind::Custom);
        let session = SessionSummary {
            id: "stub-session".to_string(),
            project_id: "project".to_string(),
            title: "Stub".to_string(),
            agent: AgentKind::Custom,
            brief_reply_mode: false,
            status: SessionStatus::Running,
            updated_at: chrono::Utc::now(),
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            provider_id: None,
            reasoning_effort: None,
        };
        let input = ChatMessage {
            id: "input".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::User,
            content: "ping".to_string(),
            created_at: chrono::Utc::now(),
        };
        let reply = ChatMessage {
            id: "reply".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: chrono::Utc::now(),
        };

        let result = provider
            .run_session(Arc::clone(&state), session, input, None, reply, None, None)
            .await
            .expect_err("direct call without seeded reply should expose state contract");
        assert!(result.to_string().contains("unknown message"));
    }

    #[tokio::test]
    async fn kiro_acp_provider_completes_prompt_and_permission_flow() {
        let _guard = kiro_acp_test_lock();
        let state = Arc::new(test_state("kiro-acp-permission").await);
        configure_mock_kiro_acp(&state, "permission").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        let approval = wait_for_pending_approval(&state, &session.id).await;
        assert_eq!(approval.command.as_deref(), Some("rm -rf /"));
        state
            .submit_approval(&session.id, &approval.request_id, ApprovalChoice::Accept)
            .await
            .expect("approval should submit");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant = messages
            .iter()
            .rev()
            .find(|message| matches!(message.role, MessageRole::Assistant))
            .expect("assistant reply should exist");
        assert_eq!(assistant.content, "hello approved");
        assert_eq!(
            state.provider_session_ref(&session.id).await.as_deref(),
            Some("kiro-session-1")
        );
    }

    #[tokio::test]
    async fn kiro_acp_provider_sends_session_cancel() {
        let _guard = kiro_acp_test_lock();
        let state = Arc::new(test_state("kiro-acp-cancel").await);
        configure_mock_kiro_acp(&state, "cancel").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Cancel Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP Cancel".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "wait".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_cancel_sender(&state, &session.id).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let cancelled = state
            .cancel_turn(&session.id)
            .await
            .expect("cancel should succeed");
        assert!(cancelled);

        wait_for_session_status(&state, &session.id, SessionStatus::Interrupted)
            .await
            .expect("session should become interrupted");
    }

    #[tokio::test]
    async fn kiro_acp_provider_handles_secret_storage_requests() {
        let _guard = kiro_acp_test_lock();
        let state = Arc::new(test_state("kiro-acp-secret-storage").await);
        configure_mock_kiro_acp(&state, "secret-storage").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Secret Storage Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP Secret Storage".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle");
        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant = messages
            .iter()
            .rev()
            .find(|message| matches!(message.role, MessageRole::Assistant))
            .expect("assistant reply should exist");
        assert_eq!(assistant.content, "secret ok");
        assert_eq!(
            state.secret_store().get("kiro/auth-token").await.as_deref(),
            Some("stored-secret")
        );
    }

    #[tokio::test]
    async fn kiro_acp_provider_falls_back_when_saved_session_cannot_load() {
        let _guard = kiro_acp_test_lock();
        let state = Arc::new(test_state("kiro-acp-load-fails").await);
        configure_mock_kiro_acp(&state, "load-fails").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Load Fallback Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP Load Fallback".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");
        state
            .set_provider_session_ref(&session.id, Some("missing-kiro-session".to_string()))
            .await;

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle");
        assert_eq!(
            state.provider_session_ref(&session.id).await.as_deref(),
            Some("kiro-session-1")
        );
        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant = messages
            .iter()
            .rev()
            .find(|message| matches!(message.role, MessageRole::Assistant))
            .expect("assistant reply should exist");
        assert_eq!(assistant.content, "hello approved");
        assert!(
            messages
                .iter()
                .any(|message| message.content.contains("failed to load ACP session"))
        );
    }

    #[tokio::test]
    async fn generic_http_acp_provider_completes_json_turn_and_reuses_session_ref() {
        let request_log = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let request_log_for_server = Arc::clone(&request_log);
        let app = axum::Router::new().route(
            "/turns",
            axum::routing::post(move |axum::Json(payload): axum::Json<Value>| {
                let request_log = Arc::clone(&request_log_for_server);
                async move {
                    request_log.lock().await.push(payload.clone());
                    (
                        axum::http::StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        axum::Json(serde_json::json!({
                            "session_id": "generic-http-session-1",
                            "output_text": format!(
                                "json:{}",
                                payload
                                    .get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                            )
                        })),
                    )
                }
            }),
        );
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(test_state("generic-http-json").await);
        configure_mock_generic_http_acp(&state, &format!("http://{address}")).await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Generic HTTP JSON".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Generic HTTP JSON".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello-json".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle after first turn");
        wait_for_assistant_message_count(&state, &session.id, 1)
            .await
            .expect("first assistant reply should be persisted");
        wait_for_turn_completion(&state, &session.id)
            .await
            .expect("first turn should be fully released");
        assert_eq!(
            state.provider_session_ref(&session.id).await.as_deref(),
            Some("generic-http-session-1")
        );

        send_message_with_retry(
            &state,
            &session.id,
            SendMessageInput {
                content: "hello-again".to_string(),
                input_mode: InputMode::Text,
                system_prompt: None,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            },
        )
        .await
        .expect("second message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle after second turn");
        wait_for_assistant_message_count(&state, &session.id, 2)
            .await
            .expect("second assistant reply should be persisted");
        wait_for_turn_completion(&state, &session.id)
            .await
            .expect("second turn should be fully released");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant_replies = messages
            .iter()
            .filter(|message| matches!(message.role, MessageRole::Assistant))
            .map(|message| message.content.clone())
            .collect::<Vec<_>>();
        assert!(
            assistant_replies
                .iter()
                .any(|text| text == "json:hello-json")
        );
        assert!(
            assistant_replies
                .iter()
                .any(|text| text == "json:hello-again")
        );

        let requests = request_log.lock().await.clone();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].get("session_id").and_then(Value::as_str),
            Some(session.id.as_str())
        );
        assert_eq!(
            requests[1].get("session_id").and_then(Value::as_str),
            Some("generic-http-session-1")
        );

        server.abort();
    }

    #[tokio::test]
    async fn generic_http_acp_provider_completes_sse_turn() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let app = axum::Router::new().route(
            "/turns",
            axum::routing::post(|| async {
                (
                    axum::http::StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    "data: {\"session_id\":\"generic-http-sse-session\",\"delta\":{\"text\":\"hello \"}}\n\n\
data: {\"delta\":{\"text\":\"stream\"}}\n\n\
data: {\"type\":\"done\"}\n\n",
                )
            }),
        );
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(test_state("generic-http-sse").await);
        configure_mock_generic_http_acp(&state, &format!("http://{address}")).await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Generic HTTP SSE".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Generic HTTP SSE".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "stream-me".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant = messages
            .iter()
            .rev()
            .find(|message| matches!(message.role, MessageRole::Assistant))
            .expect("assistant reply should exist");
        assert_eq!(assistant.content, "hello stream");
        assert_eq!(
            state.provider_session_ref(&session.id).await.as_deref(),
            Some("generic-http-sse-session")
        );

        server.abort();
    }

    #[tokio::test]
    async fn generic_http_acp_provider_replies_to_approval_requests() {
        let approvals = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let turn_started = Arc::new(tokio::sync::Notify::new());
        let pending_turn_stream = Arc::new(tokio::sync::Mutex::new(None::<tokio::net::TcpStream>));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let approvals_for_server = Arc::clone(&approvals);
        let turn_started_for_server = Arc::clone(&turn_started);
        let pending_turn_stream_for_server = Arc::clone(&pending_turn_stream);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(value) => value,
                    Err(_) => break,
                };
                let approvals = Arc::clone(&approvals_for_server);
                let turn_started = Arc::clone(&turn_started_for_server);
                let pending_turn_stream = Arc::clone(&pending_turn_stream_for_server);
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 8192];
                    let size = match stream.read(&mut buffer).await {
                        Ok(size) => size,
                        Err(_) => return,
                    };
                    let request = String::from_utf8_lossy(&buffer[..size]).to_string();
                    let first_line = request.lines().next().unwrap_or_default().to_string();

                    if first_line.starts_with("POST /turns") {
                        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\ndata: {\"type\":\"approval_request\",\"id\":\"approval-1\",\"command\":\"rm -rf /\"}\n\n";
                        if stream.write_all(head.as_bytes()).await.is_err() {
                            return;
                        }
                        let _ = stream.flush().await;
                        *pending_turn_stream.lock().await = Some(stream);
                        turn_started.notify_waiters();
                        return;
                    }

                    let response = if first_line.starts_with("POST /approvals/approval-1/reply") {
                        let body = request
                            .split("\r\n\r\n")
                            .nth(1)
                            .unwrap_or_default()
                            .to_string();
                        approvals.lock().await.push(body);
                        if stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let _ = stream.shutdown().await;
                        if let Some(mut turn_stream) = pending_turn_stream.lock().await.take() {
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                let tail = "data: {\"delta\":{\"text\":\"after-approval\"}}\n\ndata: {\"type\":\"done\"}\n\n";
                                let _ = turn_stream.write_all(tail.as_bytes()).await;
                                let _ = turn_stream.flush().await;
                                let _ = turn_stream.shutdown().await;
                            });
                        }
                        return;
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nnot found".to_string()
                    };

                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        let state = Arc::new(test_state("generic-http-approval").await);
        configure_mock_generic_http_acp(&state, &format!("http://{address}")).await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Generic HTTP Approval".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Generic HTTP Approval".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "need-approval".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        turn_started.notified().await;
        let approval = wait_for_pending_approval(&state, &session.id).await;
        assert_eq!(approval.request_id, "approval-1");
        state
            .submit_approval(&session.id, &approval.request_id, ApprovalChoice::Accept)
            .await
            .expect("approval should submit");

        wait_for_session_status(&state, &session.id, SessionStatus::Idle)
            .await
            .expect("session should become idle");
        wait_for_assistant_message_count(&state, &session.id, 1)
            .await
            .expect("assistant reply should be persisted");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("session messages should exist");
        let assistant = messages
            .iter()
            .rev()
            .find(|message| matches!(message.role, MessageRole::Assistant))
            .expect("assistant reply should exist");
        assert_eq!(assistant.content, "after-approval");

        let approvals = approvals.lock().await.clone();
        assert_eq!(approvals.len(), 1);
        assert!(approvals[0].contains("\"decision\":\"approved\""));

        server.abort();
    }

    #[tokio::test]
    async fn generic_http_acp_provider_sends_cancel_request() {
        let cancels = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let turn_started = Arc::new(tokio::sync::Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should expose addr");
        let cancels_for_server = Arc::clone(&cancels);
        let turn_started_for_server = Arc::clone(&turn_started);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(value) => value,
                    Err(_) => break,
                };
                let cancels = Arc::clone(&cancels_for_server);
                let turn_started = Arc::clone(&turn_started_for_server);
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 8192];
                    let size = match stream.read(&mut buffer).await {
                        Ok(size) => size,
                        Err(_) => return,
                    };
                    let request = String::from_utf8_lossy(&buffer[..size]).to_string();
                    let first_line = request.lines().next().unwrap_or_default().to_string();
                    let body = request
                        .split("\r\n\r\n")
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();

                    if first_line.starts_with("POST /turns") {
                        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\ndata: {\"session_id\":\"generic-http-cancel-session\",\"delta\":{\"text\":\"waiting\"}}\n\n";
                        if stream.write_all(head.as_bytes()).await.is_err() {
                            return;
                        }
                        turn_started.notify_waiters();
                        loop {
                            let size = match stream.read(&mut buffer).await {
                                Ok(size) => size,
                                Err(_) => return,
                            };
                            if size == 0 {
                                return;
                            }
                            let followup = String::from_utf8_lossy(&buffer[..size]).to_string();
                            let followup_first_line =
                                followup.lines().next().unwrap_or_default().to_string();
                            let followup_body = followup
                                .split("\r\n\r\n")
                                .nth(1)
                                .unwrap_or_default()
                                .to_string();
                            if followup_first_line
                                .starts_with("POST /sessions/generic-http-cancel-session/cancel")
                            {
                                cancels.lock().await.push(followup_body);
                                let _ = stream.shutdown().await;
                                return;
                            }
                        }
                    }

                    let response = if first_line
                        .starts_with("POST /sessions/generic-http-cancel-session/cancel")
                    {
                        cancels.lock().await.push(body);
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}".to_string()
                    } else if first_line
                        .starts_with("POST /sessions/generic-http-cancel-session/cancel")
                    {
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}".to_string()
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nnot found".to_string()
                    };

                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        let state = Arc::new(test_state("generic-http-cancel").await);
        configure_mock_generic_http_acp(&state, &format!("http://{address}")).await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "Generic HTTP Cancel".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Generic HTTP Cancel".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "cancel-me".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        turn_started.notified().await;

        wait_for_cancel_sender(&state, &session.id).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let cancelled = state
            .cancel_turn(&session.id)
            .await
            .expect("cancel should succeed");
        assert!(cancelled);

        wait_for_session_status(&state, &session.id, SessionStatus::Interrupted)
            .await
            .expect("session should become interrupted");

        let cancels = cancels.lock().await.clone();
        assert_eq!(cancels.len(), 1);
        assert!(cancels[0].contains("generic-http-cancel-session"));

        server.abort();
    }

    #[tokio::test]
    async fn kiro_acp_initialize_timeout_fails_session() {
        let _guard = kiro_acp_test_lock();
        let _timeout = AcpTimeoutOverride::new(Duration::from_millis(200));
        let state = Arc::new(test_state("kiro-acp-init-timeout").await);
        configure_mock_kiro_acp(&state, "hang-initialize").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Timeout Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP Timeout".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Failed)
            .await
            .expect("session should fail after initialize timeout");
        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");
        assert!(
            detail
                .session
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .contains("timed out waiting for response id=1")
        );
    }

    #[tokio::test]
    async fn kiro_acp_initialize_exit_surfaces_stderr_message() {
        let _guard = kiro_acp_test_lock();
        let state = Arc::new(test_state("kiro-acp-init-exit").await);
        configure_mock_kiro_acp(&state, "exit-initialize-error").await;
        let project = state
            .create_project(crate::models::CreateProjectInput {
                name: "ACP Exit Test".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(crate::models::CreateSessionInput {
                project_id: project.id,
                title: Some("Kiro ACP Exit".to_string()),
                agent: AgentKind::Acp,
                brief_reply_mode: false,
                provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "hello".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    provider_id: Some(AUTO_PROVIDER_ID.to_string()),
                    reasoning_effort: None,
                },
            )
            .await
            .expect("message should start");

        wait_for_session_status(&state, &session.id, SessionStatus::Failed)
            .await
            .expect("session should fail after initialize exit");
        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");
        let preview = detail
            .session
            .last_message_preview
            .as_deref()
            .unwrap_or_default();
        assert!(preview.contains("exited before responding to initialize"));
        assert!(preview.contains("You are not logged in"));
        assert!(preview.contains("kiro-cli login"));
    }

    struct AcpTimeoutOverride;

    impl AcpTimeoutOverride {
        fn new(timeout: Duration) -> Self {
            ACP_JSON_RPC_HANDSHAKE_TIMEOUT_TEST_MS.store(
                timeout.as_millis().try_into().unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            Self
        }
    }

    impl Drop for AcpTimeoutOverride {
        fn drop(&mut self) {
            ACP_JSON_RPC_HANDSHAKE_TIMEOUT_TEST_MS.store(0, Ordering::SeqCst);
        }
    }

    fn kiro_acp_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn configure_mock_kiro_acp(state: &Arc<AppState>, scenario: &str) {
        let script = mock_kiro_acp_script_path();
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: AiApprovalSettings::default(),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "kiro-mock".to_string(),
                    name: "Kiro Mock".to_string(),
                    profile: AcpProfile::Kiro,
                    endpoint: None,
                    command: Some("python3".to_string()),
                    args: vec![script.to_string_lossy().to_string(), scenario.to_string()],
                    auth_token: String::new(),
                    default_model: Some("claude-sonnet-4".to_string()),
                    enabled: true,
                    priority: 0,
                    headers: Vec::<HeaderKeyValue>::new(),
                    env: Vec::<HeaderKeyValue>::new(),
                }]),
                speech_profiles: None,
                speech_voices: None,
                speaker_filter: None,
            })
            .await
            .expect("mock ACP settings should update");
    }

    async fn configure_mock_generic_http_acp(state: &Arc<AppState>, endpoint: &str) {
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: AiApprovalSettings::default(),
                model_providers: None,
                acp_servers: Some(vec![AcpServerConfig {
                    id: "generic-http-mock".to_string(),
                    name: "Generic HTTP Mock".to_string(),
                    profile: AcpProfile::GenericHttp,
                    endpoint: Some(endpoint.to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: String::new(),
                    default_model: None,
                    enabled: true,
                    priority: 0,
                    headers: Vec::<HeaderKeyValue>::new(),
                    env: Vec::<HeaderKeyValue>::new(),
                }]),
                speech_profiles: None,
                speech_voices: None,
                speaker_filter: None,
            })
            .await
            .expect("mock generic HTTP ACP settings should update");
    }

    async fn test_state(prefix: &str) -> AppState {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let root = std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}"));
        AppState::new_with_paths(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
        )
        .await
    }

    fn mock_kiro_acp_script_path() -> PathBuf {
        let path = std::env::temp_dir().join("omni-code-bridge-mock-kiro-acp.py");
        std::fs::write(
            &path,
            r#"#!/usr/bin/env python3
import json
import sys

scenario = sys.argv[1] if len(sys.argv) > 1 else "permission"
prompt_id = None
permission_requested = False

def write(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    value = json.loads(line)
    request_id = value.get("id")
    method = value.get("method", "")

    if method == "initialize":
        if scenario == "hang-initialize":
            continue
        if scenario == "exit-initialize-error":
            sys.stderr.write("error: You are not logged in, please log in with kiro-cli login\n")
            sys.stderr.flush()
            sys.exit(1)
        write({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": 1, "serverInfo": {"name": "mock-kiro-acp", "version": "test"}}})
    elif method == "session/load" and scenario == "load-fails":
        write({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32001, "message": "session not found"}})
    elif method in ("session/new", "session/load"):
        write({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "kiro-session-1"}})
    elif method == "session/set_model":
        write({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/prompt" and scenario == "cancel":
        prompt_id = request_id
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "waiting"}}})
    elif method == "session/prompt" and scenario == "secret-storage":
        prompt_id = request_id
        write({"jsonrpc": "2.0", "id": 88, "method": "secretStorage/set", "params": {"key": "kiro/auth-token", "value": "stored-secret"}})
    elif method == "session/prompt" and scenario == "load-fails":
        prompt_id = request_id
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "hello approved"}}})
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "TurnEnd", "status": "completed"}})
        if prompt_id is not None:
            write({"jsonrpc": "2.0", "id": prompt_id, "result": {}})
        sys.exit(0)
    elif method == "session/prompt":
        prompt_id = request_id
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "hello "}}})
        write({"jsonrpc": "2.0", "id": 77, "method": "session/request_permission", "params": {"toolCall": {"toolName": "execute_command", "arguments": {"command": "rm -rf /"}}}})
        permission_requested = True
    elif method == "session/cancel":
        write({"jsonrpc": "2.0", "id": request_id, "result": {}})
        if prompt_id is not None:
            write({"jsonrpc": "2.0", "id": prompt_id, "result": {}})
        sys.exit(0)
    elif permission_requested and value.get("id") == 77:
        result = value.get("result", {})
        outcome = result.get("outcome", {})
        if outcome.get("outcome") != "selected":
            raise SystemExit(f"unexpected permission outcome variant: {result}")
        if outcome.get("optionId") != "allow_once":
            raise SystemExit("unexpected permission outcome")
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "approved"}}})
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "TurnEnd", "status": "completed"}})
        if prompt_id is not None:
            write({"jsonrpc": "2.0", "id": prompt_id, "result": {}})
        sys.exit(0)
    elif scenario == "secret-storage" and value.get("id") == 88:
        if value.get("result") != {}:
            raise SystemExit("unexpected secret storage set result")
        write({"jsonrpc": "2.0", "id": 89, "method": "secretStorage/get", "params": {"key": "kiro/auth-token"}})
    elif scenario == "secret-storage" and value.get("id") == 89:
        if value.get("result", {}).get("value") != "stored-secret":
            raise SystemExit("unexpected secret storage get result")
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "AgentMessageChunk", "chunk": {"text": "secret ok"}}})
        write({"jsonrpc": "2.0", "method": "session/notification", "params": {"type": "TurnEnd", "status": "completed"}})
        if prompt_id is not None:
            write({"jsonrpc": "2.0", "id": prompt_id, "result": {}})
        sys.exit(0)
    else:
        write({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"unsupported mock method: {method}"}})
"#,
        )
        .expect("mock Kiro ACP script should be written");
        path
    }

    async fn wait_for_pending_approval(state: &Arc<AppState>, session_id: &str) -> ApprovalRequest {
        for _ in 0..50 {
            if let Some(detail) = state.get_session(session_id).await {
                if let Some(request) = detail.session.pending_approval {
                    return request;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for ACP approval request");
    }

    async fn wait_for_assistant_message_count(
        state: &Arc<AppState>,
        session_id: &str,
        expected_count: usize,
    ) -> std::result::Result<(), String> {
        for _ in 0..100 {
            let count = state
                .list_messages(session_id)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|message| matches!(message.role, MessageRole::Assistant))
                .count();
            if count >= expected_count {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(format!(
            "timed out waiting for {expected_count} assistant messages"
        ))
    }

    async fn send_message_with_retry(
        state: &Arc<AppState>,
        session_id: &str,
        input: SendMessageInput,
    ) -> std::result::Result<(), String> {
        for _ in 0..20 {
            match state.send_message(session_id, input.clone()).await {
                Ok(_) => return Ok(()),
                Err(error) if error.contains("already processing a message") => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err("timed out waiting for session to accept another message".to_string())
    }

    async fn wait_for_turn_completion(
        state: &Arc<AppState>,
        session_id: &str,
    ) -> std::result::Result<(), String> {
        for _ in 0..100 {
            if !state.turn_in_flight_for_test(session_id).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err("timed out waiting for turn_in_flight to clear".to_string())
    }

    async fn wait_for_cancel_sender(state: &Arc<AppState>, session_id: &str) {
        for _ in 0..50 {
            if state.has_cancel_sender_for_test(session_id).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for ACP cancel sender");
    }

    async fn wait_for_session_status(
        state: &Arc<AppState>,
        session_id: &str,
        expected: SessionStatus,
    ) -> std::result::Result<(), String> {
        for _ in 0..100 {
            if let Some(detail) = state.get_session(session_id).await {
                if std::mem::discriminant(&detail.session.status)
                    == std::mem::discriminant(&expected)
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(format!("timed out waiting for session status {expected:?}"))
    }

    #[test]
    fn opencode_streaming_state_parses_session_text_tools_and_errors() {
        let mut state = OpenCodeStreamingState::default();

        match state
            .ingest_line(
                r#"{"type":"step_start","timestamp":1767036059338,"sessionID":"ses_123","part":{"type":"step-start"}}"#,
            )
            .expect("valid step start")
        {
            OpenCodeStreamEvent::Session(session_id) => assert_eq!(session_id, "ses_123"),
            _ => panic!("expected session event"),
        }

        match state
            .ingest_line(
                r#"{"type":"text","timestamp":1767036064268,"sessionID":"ses_123","part":{"type":"text","text":"hello"}}"#,
            )
            .expect("valid text")
        {
            OpenCodeStreamEvent::Content(text) => assert_eq!(text, "hello"),
            _ => panic!("expected text content"),
        }

        match state
            .ingest_line(
                r#"{"type":"tool_use","timestamp":1767036064269,"sessionID":"ses_123","part":{"tool":"bash","status":"completed","input":{"command":"cargo test"}}}"#,
            )
            .expect("valid tool use")
        {
            OpenCodeStreamEvent::Status(status) => assert!(status.contains("cargo test")),
            _ => panic!("expected tool status"),
        }

        match state
            .ingest_line(
                r#"{"type":"step_finish","timestamp":1767036064273,"sessionID":"ses_123","part":{"type":"step-finish","reason":"stop"}}"#,
            )
            .expect("valid step finish")
        {
            OpenCodeStreamEvent::None => {}
            _ => panic!("expected no content on final step"),
        }
        assert_eq!(state.finish_text().as_deref(), Some("hello"));

        match state
            .ingest_line(
                r#"{"type":"error","timestamp":1767036065000,"sessionID":"ses_123","error":{"name":"APIError","data":{"message":"rate limited"}}}"#,
            )
            .expect("valid error")
        {
            OpenCodeStreamEvent::Error(message) => assert!(message.contains("rate limited")),
            _ => panic!("expected error event"),
        }

        assert_eq!(
            render_opencode_event_status(
                "message.part.updated",
                &serde_json::json!({
                    "part": {
                        "type": "tool",
                        "tool": "bash",
                        "state": {
                            "status": "running",
                            "input": { "command": "cargo check" }
                        }
                    }
                })
            )
            .as_deref(),
            Some("[opencode:bash:running] cargo check")
        );

        assert_eq!(
            render_opencode_event_status(
                "permission.asked",
                &serde_json::json!({
                    "permission": "bash",
                    "metadata": { "command": "git status" }
                })
            )
            .as_deref(),
            Some("[opencode:permission:asked] bash: git status")
        );

        assert_eq!(
            render_opencode_event_status("unknown.event", &serde_json::json!({})).as_deref(),
            Some("[debug:opencode:event] unhandled type=unknown.event")
        );
    }

    #[test]
    fn kiro_acp_notification_parses_agent_message_chunks() {
        let params = serde_json::json!({
            "type": "AgentMessageChunk",
            "chunk": {
                "text": "hello"
            }
        });

        assert_eq!(
            extract_kiro_acp_message_chunk(&params).as_deref(),
            Some("hello")
        );

        let session_update = serde_json::json!({
            "sessionUpdate": "agent_message_chunk",
            "content": {
                "type": "text",
                "text": "world"
            }
        });
        assert_eq!(
            extract_kiro_acp_message_chunk(&session_update).as_deref(),
            Some("world")
        );
    }

    #[test]
    fn kiro_acp_notification_renders_tool_status_and_turn_end() {
        assert_eq!(
            render_kiro_acp_notification_status(&serde_json::json!({
                "type": "ToolCall",
                "toolCall": {
                    "toolName": "execute_command"
                }
            }))
            .as_deref(),
            Some("[kiro:execute_command] started")
        );
        assert_eq!(
            render_kiro_acp_notification_status(&serde_json::json!({
                "type": "TurnEnd",
                "status": "completed"
            }))
            .as_deref(),
            Some("[kiro] turn completed")
        );
        assert_eq!(
            render_kiro_acp_notification_status(&serde_json::json!({
                "sessionUpdate": "tool_call",
                "title": "Running: pwd"
            }))
            .as_deref(),
            Some("[kiro:Running: pwd] started")
        );
        assert!(kiro_acp_turn_end(&serde_json::json!({
            "sessionUpdate": "completed"
        })));
    }

    #[test]
    fn kiro_acp_permission_request_maps_command_approval() {
        let request = kiro_acp_permission_request_to_approval(
            "permission-1",
            &serde_json::json!({
            "toolCall": {
                "toolName": "execute_command",
                "arguments": {
                    "command": "cargo test"
                }
            }
            }),
        );

        assert_eq!(request.request_id, "permission-1");
        assert_eq!(request.command.as_deref(), Some("cargo test"));
        assert!(matches!(request.kind, ApprovalKind::ExecCommand));
        assert!(request.allow_accept_for_session);
    }

    #[test]
    fn kiro_acp_permission_result_maps_user_choices() {
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::Accept)["outcome"]["outcome"],
            "selected"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::Accept)["outcome"]["optionId"],
            "allow_once"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::AcceptForSession)["outcome"]["outcome"],
            "selected"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::AcceptForSession)["outcome"]["optionId"],
            "allow_always"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::Decline)["outcome"]["outcome"],
            "selected"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::Decline)["outcome"]["optionId"],
            "reject_once"
        );
        assert_eq!(
            kiro_acp_permission_result_json(&ApprovalChoice::Cancel)["outcome"]["outcome"],
            "cancelled"
        );
    }

    #[test]
    fn kiro_runtime_exit_error_adds_login_hint() {
        let message = format_kiro_runtime_exit_error(
            "initialize",
            1,
            "1",
            "error: You are not logged in, please log in with kiro-cli login",
        );
        assert!(message.contains("exited before responding to initialize"));
        assert!(message.contains("Run `kiro-cli login` and try again."));
    }

    #[tokio::test]
    async fn acp_readiness_message_reports_not_logged_in() {
        let message = acp_readiness_message_for_command_with_args(
            Path::new("/bin/sh"),
            &["-c", "printf \"Not logged in\\n\"; exit 1"],
        )
        .await
        .expect("mock readiness should return a message");
        assert!(message.contains("not ready"));
        assert!(message.contains("Not logged in"));
    }

    #[tokio::test]
    async fn acp_readiness_message_times_out() {
        let message =
            acp_readiness_message_for_command_with_args(Path::new("/bin/sh"), &["-c", "sleep 10"])
                .await
                .expect("timeout readiness should return a message");
        assert!(message.contains("timed out"));
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
        system_prompt: Option<String>,
        reply: ChatMessage,
        provider_config: Option<ResolvedProviderConfig>,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<()> {
        run_codex(
            state,
            &session,
            &input,
            system_prompt.as_deref(),
            &reply,
            provider_config,
            reasoning_effort,
        )
        .await
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
        system_prompt: Option<String>,
        reply: ChatMessage,
        provider_config: Option<ResolvedProviderConfig>,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<()> {
        run_claude_code(
            state,
            &session,
            &input,
            system_prompt.as_deref(),
            &reply,
            provider_config,
            reasoning_effort,
        )
        .await
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

struct OpenCodeProvider;

impl OpenCodeProvider {
    fn new() -> Self {
        Self
    }
}

struct AcpProvider;

impl AcpProvider {
    fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlashCommand<'a> {
    name: &'a str,
    args: &'a str,
}

fn parse_slash_command(input: &str) -> Option<SlashCommand<'_>> {
    let trimmed = input.trim();
    if !trimmed.starts_with('/') || trimmed.len() <= 1 {
        return None;
    }

    let without_slash = &trimmed[1..];
    let command_end = without_slash
        .find(char::is_whitespace)
        .unwrap_or(without_slash.len());
    let name = without_slash[..command_end].trim();
    if name.is_empty() || name.contains('/') {
        return None;
    }

    let args = without_slash[command_end..].trim();
    Some(SlashCommand { name, args })
}

fn escape_command_wrapper_text(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn claude_prompt_input(text: &str) -> String {
    let Some(command) = parse_slash_command(text) else {
        return text.to_string();
    };

    format!(
        "<command-name>/{}</command-name>\n<command-message>{}</command-message>\n<command-args>{}</command-args>",
        escape_command_wrapper_text(command.name),
        escape_command_wrapper_text(command.name),
        escape_command_wrapper_text(command.args),
    )
}

#[async_trait]
impl AgentProvider for OpenCodeProvider {
    async fn list_projects(&self) -> HashMap<String, ProjectSummary> {
        match OpenCodeHttpClient::start().await {
            Ok(mut client) => {
                let result = client.list_projects().await.unwrap_or_default();
                client.shutdown().await;
                result
            }
            Err(_) => HashMap::new(),
        }
    }

    async fn list_sessions(&self) -> HashMap<String, SessionSummary> {
        match OpenCodeHttpClient::start().await {
            Ok(mut client) => {
                let result = client.list_sessions().await.unwrap_or_default();
                client.shutdown().await;
                result
            }
            Err(_) => HashMap::new(),
        }
    }

    async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        let mut client = OpenCodeHttpClient::start().await.ok()?;
        let result = client.list_messages(session_id).await.ok();
        client.shutdown().await;
        result
    }

    async fn default_runtime_ref(&self, _session_id: &str) -> Option<String> {
        None
    }

    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        input: ChatMessage,
        system_prompt: Option<String>,
        reply: ChatMessage,
        provider_config: Option<ResolvedProviderConfig>,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<()> {
        let _ = reasoning_effort;
        run_opencode(
            state,
            &session,
            &input,
            system_prompt.as_deref(),
            &reply,
            provider_config,
        )
        .await
    }

    async fn summarize_reply(
        &self,
        state: Arc<AppState>,
        session: SessionSummary,
        content: String,
    ) -> Result<String> {
        summarize_with_opencode(state, &session, &content).await
    }
}

#[async_trait]
impl AgentProvider for AcpProvider {
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
        system_prompt: Option<String>,
        reply: ChatMessage,
        provider_config: Option<ResolvedProviderConfig>,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<()> {
        run_acp(
            state,
            &session,
            &input,
            system_prompt.as_deref(),
            &reply,
            provider_config,
            reasoning_effort,
        )
        .await
    }

    async fn summarize_reply(
        &self,
        _state: Arc<AppState>,
        _session: SessionSummary,
        content: String,
    ) -> Result<String> {
        Ok(content.chars().take(120).collect())
    }
}

fn spawn_codex_app_server(cwd: &Path) -> Result<Child> {
    spawn_codex_app_server_with_config(cwd, None, None, None)
}

fn spawn_codex_app_server_with_config(
    cwd: &Path,
    provider_config: Option<&ResolvedProviderConfig>,
    codex_provider_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<Child> {
    let binary = codex_binary_path();
    let mut command = Command::new(&binary);
    command
        .args(["app-server", "--listen", "stdio://"])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut c_overrides: Vec<String> = Vec::new();
    let push_arg = |cmd: &mut Command, vec: &mut Vec<String>, kv: String| {
        cmd.args(["-c", &kv]);
        vec.push(kv);
    };
    let toml_string = |value: &str| serde_json::to_string(value).expect("string serializes");

    // Force manual approval routing for app-server clients. `approval_policy`
    // alone is not enough when Codex config enables an automatic reviewer.
    push_arg(
        &mut command,
        &mut c_overrides,
        "approval_policy=\"on-request\"".to_string(),
    );
    push_arg(
        &mut command,
        &mut c_overrides,
        "approvals_reviewer=\"user\"".to_string(),
    );
    if let Some(reasoning_effort) = reasoning_effort {
        push_arg(
            &mut command,
            &mut c_overrides,
            format!(
                "model_reasoning_effort={}",
                toml_string(reasoning_effort.as_str())
            ),
        );
    }

    // Apply provider configuration via -c config overrides while preserving the
    // user's CODEX_HOME for MCP, trust, and other Codex settings.
    if let Some(config) = provider_config {
        let name = codex_provider_name.unwrap_or("omni-bridge");
        let display_name =
            find_codex_provider_display_name(name).unwrap_or_else(|| name.to_string());

        push_arg(
            &mut command,
            &mut c_overrides,
            format!("model_provider={}", toml_string(name)),
        );
        push_arg(
            &mut command,
            &mut c_overrides,
            format!("model_providers.{name}.name={}", toml_string(&display_name)),
        );
        push_arg(
            &mut command,
            &mut c_overrides,
            format!(
                "model_providers.{name}.base_url={}",
                toml_string(&config.base_url)
            ),
        );
        push_arg(
            &mut command,
            &mut c_overrides,
            format!("model_providers.{name}.requires_openai_auth=false"),
        );
        push_arg(
            &mut command,
            &mut c_overrides,
            format!("model_providers.{name}.wire_api=\"responses\""),
        );
        if !config.api_key.is_empty() {
            let auth_env_var = "OMNI_CODEX_PROVIDER_KEY";
            command.env(auth_env_var, &config.api_key);
            push_arg(
                &mut command,
                &mut c_overrides,
                format!(
                    "model_providers.{name}.env_key={}",
                    toml_string(auth_env_var)
                ),
            );
        }
        if let Some(ref model) = config.model {
            push_arg(
                &mut command,
                &mut c_overrides,
                format!("model={}", toml_string(model)),
            );
            push_arg(
                &mut command,
                &mut c_overrides,
                format!("model_providers.{name}.model={}", toml_string(model)),
            );
        }
    }
    eprintln!("[codex] full -c args: {c_overrides:#?}");

    command
        .spawn()
        .with_context(|| format!("failed to spawn `{}`", binary.display()))
}

fn find_codex_provider_name() -> Option<String> {
    let path = codex_home_dir().join("config.toml");
    let content = std::fs::read_to_string(path).ok()?;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("model_provider") && trimmed.contains('=') {
            let value = trimmed.splitn(2, '=').nth(1)?.trim();
            return Some(value.trim_matches('"').trim_matches('\'').to_string());
        }
    }
    None
}

/// Read the `name` field from a model_providers section in config.toml.
fn find_codex_provider_display_name(provider_key: &str) -> Option<String> {
    let path = codex_home_dir().join("config.toml");
    let content = std::fs::read_to_string(path).ok()?;
    let section_header = format!("[model_providers.{provider_key}]");
    let mut in_section = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == section_header {
            in_section = true;
            continue;
        }
        if in_section {
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                break;
            }
            if trimmed.starts_with("name") && trimmed.contains('=') {
                let value = trimmed.splitn(2, '=').nth(1)?.trim();
                return Some(value.trim_matches('"').trim_matches('\'').to_string());
            }
        }
    }
    None
}

/// Read the model_provider from codex's session data file for a given session ID.
/// Session files are stored at ~/.codex/sessions/YYYY/MM/DD/rollout-*-<session_id>.jsonl
fn read_codex_session_provider(session_id: &str) -> Option<String> {
    let sessions_dir = codex_home_dir().join("sessions");
    find_session_file(&sessions_dir, session_id).and_then(|path| {
        let content = std::fs::read_to_string(path).ok()?;
        let first_line = content.lines().next()?;
        let meta: serde_json::Value = serde_json::from_str(first_line).ok()?;
        meta.pointer("/payload/model_provider")
            .and_then(|v| v.as_str())
            .map(String::from)
    })
}

fn find_session_file(dir: &Path, session_id: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_session_file(&path, session_id) {
                return Some(found);
            }
        } else {
            let name = path.file_name()?.to_string_lossy();
            if name.contains(session_id) && name.ends_with(".jsonl") {
                return Some(path);
            }
        }
    }
    None
}

/// Path to the codex home directory (~/.codex)
fn codex_home_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        home.join(".codex")
    } else {
        PathBuf::from(".codex")
    }
}

async fn start_codex_thread(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
    next_request_id: &mut u64,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    state: &AppState,
    session: &SessionSummary,
    developer_instructions: &Option<String>,
) -> Result<serde_json::Value> {
    let request_id = send_json_rpc_request(
        stdin,
        next_request_id,
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
    .await?;
    wait_for_json_rpc_response(stdout_rx, raw_stdout, request_id).await
}

async fn fork_codex_thread(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
    next_request_id: &mut u64,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    source_thread_id: &str,
    cwd: &Path,
    model_provider: Option<&str>,
    model: Option<&str>,
    developer_instructions: &Option<String>,
) -> Result<serde_json::Value> {
    let request_id = send_json_rpc_request(
        stdin,
        next_request_id,
        "thread/fork",
        serde_json::json!({
            "threadId": source_thread_id,
            "cwd": cwd.display().to_string(),
            "approvalPolicy": "on-request",
            "approvalsReviewer": "user",
            "sandbox": "workspace-write",
            "modelProvider": model_provider,
            "model": model,
            "developerInstructions": developer_instructions,
        }),
    )
    .await?;
    wait_for_json_rpc_response(stdout_rx, raw_stdout, request_id).await
}

async fn archive_codex_thread(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
    next_request_id: &mut u64,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    thread_id: &str,
) -> Result<serde_json::Value> {
    let request_id = send_json_rpc_request(
        stdin,
        next_request_id,
        "thread/archive",
        serde_json::json!({
            "threadId": thread_id,
        }),
    )
    .await?;
    wait_for_json_rpc_response(stdout_rx, raw_stdout, request_id).await
}

fn brief_reply_developer_prompt(session: &SessionSummary) -> Option<&'static str> {
    session.brief_reply_mode.then_some(
        "回复要求：请简短说明你做了什么和结果，尽量不超过 50 个汉字。只保留关键动作、结果或结论，避免展开解释。",
    )
}

fn log_codex_thread_response(session_id: &str, response: &Value) {
    let thread_id = response
        .pointer("/thread/id")
        .or_else(|| response.pointer("/thread/threadId"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let approval_policy = response
        .pointer("/thread/approvalPolicy")
        .or_else(|| response.pointer("/thread/approval_policy"))
        .or_else(|| response.pointer("/approvalPolicy"))
        .or_else(|| response.pointer("/approval_policy"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let approvals_reviewer = response
        .pointer("/thread/approvalsReviewer")
        .or_else(|| response.pointer("/thread/approvals_reviewer"))
        .or_else(|| response.pointer("/approvalsReviewer"))
        .or_else(|| response.pointer("/approvals_reviewer"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    eprintln!(
        "[codex] thread response session={session_id} thread={thread_id} approvalPolicy={approval_policy} approvalsReviewer={approvals_reviewer}"
    );
}

fn turn_system_prompt<'a>(
    session: &SessionSummary,
    system_prompt: Option<&'a str>,
) -> Option<String> {
    let mut prompts = Vec::new();
    if let Some(prompt) = brief_reply_developer_prompt(session) {
        prompts.push(prompt.to_string());
    }
    if let Some(prompt) = system_prompt
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        prompts.push(prompt.to_string());
    }
    (!prompts.is_empty()).then(|| prompts.join("\n\n"))
}

fn combine_developer_instructions(
    base: Option<String>,
    migration: Option<String>,
) -> Option<String> {
    match (base, migration) {
        (Some(base), Some(migration)) => Some(format!("{base}\n\n{migration}")),
        (Some(base), None) => Some(base),
        (None, Some(migration)) => Some(migration),
        (None, None) => None,
    }
}

fn build_context_migration_instructions(
    messages: &[ChatMessage],
    current_input: &ChatMessage,
    pending_reply: &ChatMessage,
) -> Option<String> {
    const MAX_MESSAGES: usize = 12;
    const MAX_CHARS_PER_MESSAGE: usize = 600;
    const MAX_TOTAL_CHARS: usize = 4000;

    let mut selected = messages
        .iter()
        .filter(|message| message.id != current_input.id && message.id != pending_reply.id)
        .filter_map(|message| {
            let content = message.content.trim();
            if content.is_empty() {
                return None;
            }
            let role = match message.role {
                crate::models::MessageRole::User => "用户",
                crate::models::MessageRole::Assistant => "助手",
                crate::models::MessageRole::System => "系统",
            };
            let truncated = truncate_chars(content, MAX_CHARS_PER_MESSAGE);
            Some(format!("{role}: {truncated}"))
        })
        .collect::<Vec<_>>();

    if selected.is_empty() {
        return None;
    }

    if selected.len() > MAX_MESSAGES {
        selected = selected.split_off(selected.len() - MAX_MESSAGES);
    }

    let mut body = String::new();
    for line in selected {
        let extra_len = if body.is_empty() {
            line.chars().count()
        } else {
            line.chars().count() + 1
        };
        if body.chars().count() + extra_len > MAX_TOTAL_CHARS {
            break;
        }
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&line);
    }

    if body.is_empty() {
        return None;
    }

    Some(format!(
        "这是一次模型或提供方切换后的新线程。请把下面历史对话当作延续上下文，仅用于理解先前约束、决策和未完成事项，不要重复复述给用户。\n\n历史对话摘录：\n{body}"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexThreadDecision {
    StartFresh,
    Resume,
    StartWithMigration,
}

fn decide_codex_thread_action(
    existing_runtime_ref: Option<&str>,
    stored_provider_name: Option<&str>,
    current_provider_name: Option<&str>,
) -> CodexThreadDecision {
    if existing_runtime_ref.is_none() {
        return CodexThreadDecision::StartFresh;
    }

    match (stored_provider_name, current_provider_name) {
        (Some(stored), Some(current)) if stored != current => {
            CodexThreadDecision::StartWithMigration
        }
        _ => CodexThreadDecision::Resume,
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut result = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index >= max_chars {
            result.push_str("...");
            break;
        }
        result.push(ch);
    }
    result
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

fn opencode_binary_path() -> PathBuf {
    if let Some(path) = std::env::var_os("ECHO_MATE_OPENCODE_BIN")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        return path;
    }

    find_executable_in_path("opencode").unwrap_or_else(|| PathBuf::from("opencode"))
}

fn opencode_config_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        home.join(".config/opencode/opencode.json")
    } else {
        PathBuf::from(".config/opencode/opencode.json")
    }
}

/// Create a temporary opencode config file with the given provider's base_url.
/// Returns the path to the temp config file.
fn write_opencode_overlay_config(config: &ResolvedProviderConfig) -> Result<PathBuf> {
    let original_path = opencode_config_path();
    let original = std::fs::read_to_string(&original_path)
        .unwrap_or_else(|_| r#"{"provider":{}}"#.to_string());
    let modified = modify_opencode_config(&original, config);
    let overlay = crate::bridge_settings::project_tmp_dir("opencode-overlay");
    std::fs::create_dir_all(&overlay)?;
    let path = overlay.join("opencode.json");
    std::fs::write(&path, &modified)?;
    Ok(path)
}

/// Modify opencode config: find a provider with matching base_url AND npm package, and update it,
/// or create a new provider if none found.
fn modify_opencode_config(original: &str, config: &ResolvedProviderConfig) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(original) else {
        return original.to_string();
    };

    let providers = value.get_mut("provider").and_then(|v| v.as_object_mut());
    let Some(providers) = providers else {
        return original.to_string();
    };

    let npm = match config.format {
        crate::models::ApiFormat::AnthropicMessages => "@ai-sdk/anthropic",
        _ => "@ai-sdk/openai-compatible",
    };

    // Find provider to update: must match both base_url AND npm package
    // This prevents updating an OpenAI-compatible provider when we need Anthropic
    let target_key = providers
        .iter()
        .find(|(_, v)| {
            let base_url_match = v
                .get("options")
                .and_then(|o| o.get("baseURL"))
                .and_then(|u| u.as_str())
                == Some(&config.base_url);
            let npm_match = v.get("npm").and_then(|n| n.as_str()) == Some(npm);
            base_url_match && npm_match
        })
        .map(|(k, _)| k.clone());

    if let Some(key) = target_key {
        // Update existing provider with matching base_url and npm
        if let Some(provider) = providers.get_mut(&key).and_then(|v| v.as_object_mut()) {
            let options = provider
                .entry("options")
                .or_insert_with(|| serde_json::json!({}));
            if let Some(obj) = options.as_object_mut() {
                obj.insert("baseURL".to_string(), serde_json::json!(config.base_url));
                if !config.api_key.is_empty() {
                    obj.insert("apiKey".to_string(), serde_json::json!(config.api_key));
                }
            }
            provider.insert("npm".to_string(), serde_json::json!(npm));
        }
    } else {
        // Create new provider with a unique name based on npm package
        let provider_name = match config.format {
            crate::models::ApiFormat::AnthropicMessages => "omni-bridge-anthropic",
            crate::models::ApiFormat::Codex => "omni-bridge-codex",
            _ => "omni-bridge",
        };
        let mut options = serde_json::json!({ "baseURL": config.base_url });
        if !config.api_key.is_empty() {
            options["apiKey"] = serde_json::json!(config.api_key);
        }
        providers.insert(
            provider_name.to_string(),
            serde_json::json!({ "npm": npm, "options": options }),
        );
    }

    serde_json::to_string_pretty(&value).unwrap_or_else(|_| original.to_string())
}

struct OpenCodeHttpClient {
    base_url: String,
    http: reqwest::Client,
    child: Child,
    stderr_task: tokio::task::JoinHandle<std::result::Result<String, std::io::Error>>,
}

impl OpenCodeHttpClient {
    async fn start() -> Result<Self> {
        Self::start_with_config(None).await
    }

    async fn start_with_config(provider_config: Option<&ResolvedProviderConfig>) -> Result<Self> {
        let mut command = Command::new(opencode_binary_path());
        command
            .arg("serve")
            .arg("--hostname")
            .arg("127.0.0.1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Apply provider configuration via OPENCODE_CONFIG overlay file
        if let Some(config) = provider_config {
            match write_opencode_overlay_config(config) {
                Ok(path) => {
                    command.env("OPENCODE_CONFIG", &path);
                    eprintln!(
                        "[opencode] using overlay config: base_url={}",
                        config.base_url
                    );
                }
                Err(err) => {
                    eprintln!("[opencode] warning: failed to create overlay config: {err}");
                }
            }
            if let Some(ref model) = config.model {
                command.env("OPENCODE_MODEL", model);
            }
        }

        let mut child = command
            .spawn()
            .context("failed to spawn `opencode serve`")?;
        let stdout = child
            .stdout
            .take()
            .context("opencode server did not expose stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("opencode server did not expose stderr")?;
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            let mut output = String::new();
            while let Some(line) = reader.next_line().await? {
                if !output.is_empty() {
                    output.push('\n');
                }
                output.push_str(&strip_ansi(&line));
            }
            Ok::<_, std::io::Error>(output)
        });
        let base_url = read_opencode_server_url(stdout).await?;
        Ok(Self {
            base_url,
            http: reqwest::Client::new(),
            child,
            stderr_task,
        })
    }

    async fn shutdown(&mut self) {
        let _ = self.child.kill().await;
        self.stderr_task.abort();
    }

    async fn list_projects(&self) -> Result<HashMap<String, ProjectSummary>> {
        let sessions = self.get_json("/session").await?;
        let mut projects = HashMap::new();
        for session in sessions.as_array().into_iter().flatten() {
            let Some(directory) = session.get("directory").and_then(Value::as_str) else {
                continue;
            };
            let project_id = crate::session_store::project_id_for_path(directory);
            let updated_at = opencode_time(session.pointer("/time/updated"));
            let preview = session
                .get("title")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            projects
                .entry(project_id.clone())
                .and_modify(|project: &mut ProjectSummary| {
                    project.session_count += 1;
                    if updated_at > project.updated_at {
                        project.updated_at = updated_at;
                        project.last_session_preview = preview.clone();
                    }
                })
                .or_insert_with(|| ProjectSummary {
                    id: project_id,
                    name: project_name_from_path(directory),
                    root_path: directory.to_string(),
                    updated_at,
                    session_count: 1,
                    last_session_preview: preview,
                    git_branch: None,
                    git_status: None,
                });
        }
        Ok(projects)
    }

    async fn list_sessions(&self) -> Result<HashMap<String, SessionSummary>> {
        let sessions = self.get_json("/session").await?;
        Ok(sessions
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(opencode_session_summary)
            .map(|session| (session.id.clone(), session))
            .collect())
    }

    async fn list_messages(&self, session_id: &str) -> Result<Vec<ChatMessage>> {
        let value = self
            .get_json(&format!("/session/{}/message", url_path_escape(session_id)))
            .await?;
        Ok(opencode_messages_from_value(session_id, &value))
    }

    async fn latest_assistant_text(&self, session_id: &str, project_root: &Path) -> Result<String> {
        let value = self
            .get_json_with_query(
                &format!("/session/{}/message", url_path_escape(session_id)),
                &[("directory", project_root.display().to_string())],
            )
            .await?;
        Ok(opencode_messages_from_value(session_id, &value)
            .into_iter()
            .rev()
            .find(|message| matches!(message.role, crate::models::MessageRole::Assistant))
            .map(|message| message.content)
            .unwrap_or_default())
    }

    async fn create_session(&self, project_root: &Path, title: &str) -> Result<String> {
        let value = self
            .post_json_with_query(
                "/session",
                &[("directory", project_root.display().to_string())],
                serde_json::json!({
                    "title": title,
                    "permission": [{
                        "permission": "bash",
                        "pattern": "*",
                        "action": "ask",
                    }],
                }),
            )
            .await?;
        value
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .context("opencode session create did not return id")
    }

    async fn prompt_async(
        &self,
        session_id: &str,
        project_root: &Path,
        prompt: &str,
        system: Option<&str>,
        model: Option<&str>,
        provider_id: Option<&str>,
    ) -> Result<()> {
        let mut body = serde_json::json!({
            "parts": [{
                "type": "text",
                "text": prompt,
            }],
        });
        if let Some(system) = system {
            body["system"] = serde_json::Value::String(system.to_string());
        }
        if let Some(model) = model {
            // opencode expects model as an object with provider and model fields
            // Use the provider_id if provided, otherwise default to "omni-bridge"
            // Note: The provider name must match what modify_opencode_config creates
            let provider = provider_id.unwrap_or("omni-bridge");
            body["model"] = serde_json::json!({
                "provider": provider,
                "model": model,
            });
        }
        self.post_json_with_query(
            &format!("/session/{}/prompt_async", url_path_escape(session_id)),
            &[("directory", project_root.display().to_string())],
            body,
        )
        .await?;
        Ok(())
    }

    async fn reply_permission(&self, request_id: &str, choice: &ApprovalChoice) -> Result<()> {
        let reply = match choice {
            ApprovalChoice::Accept => "once",
            ApprovalChoice::AcceptForSession | ApprovalChoice::AlwaysAllow => "always",
            ApprovalChoice::Decline | ApprovalChoice::Cancel => "reject",
        };
        self.post_json(
            &format!("/permission/{}/reply", url_path_escape(request_id)),
            serde_json::json!({ "reply": reply }),
        )
        .await?;
        Ok(())
    }

    async fn event_stream(
        &self,
        project_root: &Path,
    ) -> Result<mpsc::UnboundedReceiver<Result<String>>> {
        let response = self
            .http
            .get(format!("{}/event", self.base_url))
            .query(&[("directory", project_root.display().to_string())])
            .send()
            .await
            .context("failed to connect opencode event stream")?
            .error_for_status()
            .context("opencode event stream failed")?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut stream = response.bytes_stream();
            let mut buffer = Vec::<u8>::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        buffer.extend_from_slice(&bytes);
                        while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
                            let line = buffer.drain(..=index).collect::<Vec<_>>();
                            let line = String::from_utf8_lossy(&line).trim().to_string();
                            let _ = tx.send(Ok(line));
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error.into()));
                        return;
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        self.get_json_with_query(path, &[]).await
    }

    async fn get_json_with_query(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .query(query)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    async fn post_json(&self, path: &str, body: Value) -> Result<Value> {
        self.post_json_with_query(path, &[], body).await
    }

    async fn post_json_with_query(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: Value,
    ) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let response = self.http.post(&url).query(query).json(&body).send().await?;
        let status = response.status();
        if status.is_success() {
            Ok(response.json().await.unwrap_or(Value::Null))
        } else {
            let body_text = response.text().await.unwrap_or_default();
            eprintln!("[opencode] HTTP {status} {url} body={body_text}");
            bail!("opencode HTTP {status}: {body_text}");
        }
    }
}

async fn read_opencode_server_url(stdout: impl AsyncRead + Unpin) -> Result<String> {
    let mut lines = BufReader::new(stdout).lines();
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else {
                    bail!("opencode server exited before printing URL");
                };
                if let Some(url) = extract_first_http_url(&strip_ansi(&line)) {
                    return Ok(url);
                }
            }
            _ = &mut deadline => {
                bail!("timed out waiting for opencode server URL");
            }
        }
    }
}

fn extract_first_http_url(text: &str) -> Option<String> {
    text.split_whitespace()
        .find(|part| part.starts_with("http://") || part.starts_with("https://"))
        .map(|part| part.trim_end_matches('/').to_string())
}

pub fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

async fn run_codex(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    provider_config: Option<ResolvedProviderConfig>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    // Resolve the codex provider name for this session.
    // When the bridge explicitly resolved a provider_id (message-level or session-level),
    // use it directly as the codex provider name — the -c overrides will create the
    // matching model_providers entry. This avoids mismatches where the fallback chain
    // picks a different provider name than what the bridge actually configured.
    //
    // Fallback chain when the bridge did not inject a provider config:
    // 1. Codex session data file (model_provider in session_meta)
    // 2. Current config.toml's model_provider
    // 3. No override
    let codex_provider_name = match provider_config.as_ref().and_then(|c| c.provider_id.clone()) {
        Some(id) => Some(id),
        None => read_codex_session_provider(&session.id).or_else(find_codex_provider_name),
    };
    let mut child = spawn_codex_app_server_with_config(
        &project_root,
        provider_config.as_ref(),
        codex_provider_name.as_deref(),
        reasoning_effort,
    )
    .context("failed to spawn `codex app-server`")?;

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
            eprintln!("[codex:stderr] {line}");
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

    let current_model = provider_config.as_ref().and_then(|c| c.model.clone());
    let stored_model = state.codex_model(&session.id).await;
    let existing_runtime_ref = state.provider_session_ref(&session.id).await;
    let stored_provider_name = state.codex_provider_name(&session.id).await;
    let thread_decision = decide_codex_thread_action(
        existing_runtime_ref.as_deref(),
        stored_provider_name.as_deref(),
        codex_provider_name.as_deref(),
    );
    let base_developer_instructions = turn_system_prompt(session, system_prompt);
    let developer_instructions = base_developer_instructions.clone();

    if thread_decision == CodexThreadDecision::StartWithMigration {
        eprintln!(
            "[codex] forking thread for session={}: provider changed old_provider={:?} new_provider={:?} old_model={:?} new_model={:?}",
            session.id, stored_provider_name, codex_provider_name, stored_model, current_model,
        );
    } else if thread_decision == CodexThreadDecision::Resume && stored_model != current_model {
        eprintln!(
            "[codex] attempting thread/resume across model change for session={}: provider={:?} old_model={:?} new_model={:?}",
            session.id, codex_provider_name, stored_model, current_model,
        );
    }
    let thread_response = if thread_decision == CodexThreadDecision::StartWithMigration {
        let source_thread_id = existing_runtime_ref
            .as_deref()
            .context("missing source thread for codex fork")?;
        match fork_codex_thread(
            &mut stdin,
            &mut next_request_id,
            &mut stdout_rx,
            &mut raw_stdout,
            source_thread_id,
            &project_root,
            codex_provider_name.as_deref(),
            current_model.as_deref(),
            &developer_instructions,
        )
        .await
        {
            Ok(resp) => {
                if let Err(error) = archive_codex_thread(
                    &mut stdin,
                    &mut next_request_id,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    source_thread_id,
                )
                .await
                {
                    eprintln!(
                        "[codex] failed to archive source thread after fork session={} source_thread={}: {error}",
                        session.id, source_thread_id,
                    );
                }
                resp
            }
            Err(error) => {
                eprintln!(
                    "[codex] thread/fork failed, falling back to thread/start with migrated context: {error}"
                );
                state.set_provider_session_ref(&session.id, None).await;
                state.set_codex_provider_name(&session.id, None).await;
                state.set_codex_model(&session.id, None).await;
                let fallback_developer_instructions = combine_developer_instructions(
                    base_developer_instructions.clone(),
                    state.list_messages(&session.id).await.and_then(|messages| {
                        build_context_migration_instructions(&messages, input, reply)
                    }),
                );
                start_codex_thread(
                    &mut stdin,
                    &mut next_request_id,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    &state,
                    &session,
                    &fallback_developer_instructions,
                )
                .await?
            }
        }
    } else if let Some(thread_id) = existing_runtime_ref.as_deref() {
        let resume_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "thread/resume",
            serde_json::json!({
                "threadId": thread_id,
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": "workspace-write",
                "persistExtendedHistory": false,
                "developerInstructions": developer_instructions,
            }),
        )
        .await?;
        match wait_for_json_rpc_response(&mut stdout_rx, &mut raw_stdout, resume_id).await {
            Ok(resp) if resp.get("error").is_none() => resp,
            Ok(resp) => {
                eprintln!(
                    "[codex] thread/resume failed, falling back to thread/start: {}",
                    resp
                );
                state.set_codex_provider_name(&session.id, None).await;
                state.set_codex_model(&session.id, None).await;
                let fallback_developer_instructions = combine_developer_instructions(
                    base_developer_instructions.clone(),
                    state.list_messages(&session.id).await.and_then(|messages| {
                        build_context_migration_instructions(&messages, input, reply)
                    }),
                );
                start_codex_thread(
                    &mut stdin,
                    &mut next_request_id,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    &state,
                    &session,
                    &fallback_developer_instructions,
                )
                .await?
            }
            Err(e) => {
                eprintln!("[codex] thread/resume failed, falling back to thread/start: {e}");
                state.set_codex_provider_name(&session.id, None).await;
                state.set_codex_model(&session.id, None).await;
                let fallback_developer_instructions = combine_developer_instructions(
                    base_developer_instructions.clone(),
                    state.list_messages(&session.id).await.and_then(|messages| {
                        build_context_migration_instructions(&messages, input, reply)
                    }),
                );
                start_codex_thread(
                    &mut stdin,
                    &mut next_request_id,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    &state,
                    &session,
                    &fallback_developer_instructions,
                )
                .await?
            }
        }
    } else {
        start_codex_thread(
            &mut stdin,
            &mut next_request_id,
            &mut stdout_rx,
            &mut raw_stdout,
            &state,
            &session,
            &developer_instructions,
        )
        .await?
    };
    log_codex_thread_response(&session.id, &thread_response);
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
    // Store the provider name only for bridge-managed provider sessions.
    if provider_config.is_some() {
        let name = codex_provider_name.unwrap_or_else(|| "omni-bridge".to_string());
        state.set_codex_provider_name(&session.id, Some(name)).await;
        state.set_codex_model(&session.id, current_model).await;
    }
    parsed.session_id = Some(thread_id.clone());

    if let Some(response_text) = handle_codex_immediate_slash_command(
        &state,
        session,
        &mut stdin,
        &mut next_request_id,
        &mut stdout_rx,
        &mut raw_stdout,
        &thread_id,
        input,
    )
    .await?
    {
        let mut last_rendered = String::new();
        push_incremental_text(
            &state,
            &session.id,
            &reply.id,
            &mut last_rendered,
            &response_text,
        )
        .await;
        stderr_task.abort();
        state
            .finish_assistant_message(&session.id, &reply.id)
            .await
            .map_err(anyhow::Error::msg)?;
        return Ok(());
    }

    let (turn_request_id, slash_status_message) = match classify_codex_slash_command(&input.content)
    {
        Some(CodexSlashAction::Compact) => (
            send_json_rpc_request(
                &mut stdin,
                &mut next_request_id,
                "thread/compact/start",
                serde_json::json!({
                    "threadId": thread_id,
                }),
            )
            .await?,
            Some("[codex] running /compact".to_string()),
        ),
        Some(CodexSlashAction::ReviewUncommittedChanges) => (
            send_json_rpc_request(
                &mut stdin,
                &mut next_request_id,
                "review/start",
                serde_json::json!({
                    "threadId": thread_id,
                    "target": {
                        "type": "uncommittedChanges"
                    },
                    "delivery": "inline",
                }),
            )
            .await?,
            Some("[codex] running /review".to_string()),
        ),
        Some(CodexSlashAction::ReviewCustom { instructions }) => (
            send_json_rpc_request(
                &mut stdin,
                &mut next_request_id,
                "review/start",
                serde_json::json!({
                    "threadId": thread_id,
                    "target": {
                        "type": "custom",
                        "instructions": instructions,
                    },
                    "delivery": "inline",
                }),
            )
            .await?,
            Some(format!("[codex] running /review {}", instructions)),
        ),
        Some(CodexSlashAction::Rename { .. })
        | Some(CodexSlashAction::GoalSet { .. })
        | Some(CodexSlashAction::GoalClear) => unreachable!("handled above"),
        None => (
            send_json_rpc_request(
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
            .await?,
            None,
        ),
    };
    if let Some(status_message) = slash_status_message {
        state.emit_system_message(&session.id, status_message).await;
    }

    let idle_deadline = Duration::from_secs(CODEX_IDLE_TICK_SECONDS);
    let idle_sleep = tokio::time::sleep(idle_deadline);
    tokio::pin!(idle_sleep);
    let mut pending_approval: Option<PendingApproval> = None;
    let mut queued_approvals: VecDeque<PendingApproval> = VecDeque::new();
    let mut turn_finished = false;
    let mut last_rendered = String::new();

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                let Some(line) = maybe_line else {
                    break;
                };
                let line = line.map_err(anyhow::Error::msg)?;
                eprintln!("[codex:stdout] {line}");
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
                            .unwrap_or("codex request failed");
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
                            if pending_approval.is_some() {
                                queued_approvals.push_back(pending);
                            } else {
                                state.raise_approval(&session.id, pending.request.clone()).await;
                                pending_approval = Some(pending);
                            }
                        }
                    }
                    CodexAppServerEvent::ApprovalResolved { request_id } => {
                        let resolved_current = pending_approval
                            .as_ref()
                            .map(|pending| pending.request.request_id == request_id)
                            .unwrap_or(false);
                        if resolved_current {
                            let pending = pending_approval
                                .take()
                                .context("approval state disappeared unexpectedly")?;
                            let choice = pending.last_choice.unwrap_or(ApprovalChoice::Accept);
                            state.resolve_approval(&session.id, &request_id, choice).await;
                            if let Some(next) = queued_approvals.pop_front() {
                                state.raise_approval(&session.id, next.request.clone()).await;
                                pending_approval = Some(next);
                            }
                        } else if pending_approval.is_none() {
                            state.resolve_approval(&session.id, &request_id, ApprovalChoice::Accept).await;
                        } else {
                            eprintln!(
                                "[approval] ignoring resolved request for non-current approval session={} request={}",
                                session.id, request_id
                            );
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
                let watchdog_action = parsed.mark_idle_waiting(pending_approval.is_some());
                match watchdog_action {
                    Some(CommandWatchdogAction::SoftRecovery { command }) => {
                        eprintln!(
                            "[codex:watchdog] session={} command stalled; waiting once more: {}",
                            session.id, command
                        );
                    }
                    Some(CommandWatchdogAction::Stalled { command }) => {
                        let message = format!(
                            "codex command stalled without a completion event: {command}"
                        );
                        eprintln!("[codex:watchdog] session={} {message}", session.id);
                        bail!(message);
                    }
                    None => {}
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
            // stderr may contain retry messages like "Reconnecting... 1/5".
            // Use the last line as the actual error message.
            let last_line = stderr.lines().last().unwrap_or(stderr);
            bail!("codex exited with status {code}: {last_line}");
        }
    }

    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn handle_codex_immediate_slash_command(
    state: &AppState,
    session: &SessionSummary,
    stdin: &mut (impl AsyncWrite + Unpin),
    next_request_id: &mut u64,
    stdout_rx: &mut mpsc::UnboundedReceiver<Result<String, String>>,
    raw_stdout: &mut String,
    thread_id: &str,
    input: &ChatMessage,
) -> Result<Option<String>> {
    let Some(command) = classify_codex_slash_command(&input.content) else {
        return Ok(None);
    };

    let (method, params, response_text, updated_title) = match command {
        CodexSlashAction::Rename { title } => (
            "thread/name/set",
            serde_json::json!({
                "threadId": thread_id,
                "name": title,
            }),
            format!("Session renamed to: {title}"),
            Some(title.to_string()),
        ),
        CodexSlashAction::GoalSet { objective } => (
            "thread/goal/set",
            serde_json::json!({
                "threadId": thread_id,
                "objective": objective,
                "status": "active",
            }),
            format!("Goal set: {objective}"),
            None,
        ),
        CodexSlashAction::GoalClear => (
            "thread/goal/clear",
            serde_json::json!({
                "threadId": thread_id,
            }),
            "Goal cleared.".to_string(),
            None,
        ),
        CodexSlashAction::Compact
        | CodexSlashAction::ReviewUncommittedChanges
        | CodexSlashAction::ReviewCustom { .. } => return Ok(None),
    };

    let request_id = send_json_rpc_request(stdin, next_request_id, method, params).await?;
    let response = wait_for_json_rpc_response(stdout_rx, raw_stdout, request_id).await?;
    if let Some(error) = response.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("codex request failed");
        bail!("{message}");
    }

    if let Some(title) = updated_title {
        state
            .update_session_title(&session.id, title)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    Ok(Some(response_text))
}

async fn run_claude_code(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    provider_config: Option<ResolvedProviderConfig>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    let state_dir = claude_state_dir();
    ensure_runtime_dirs(&state_dir).await?;
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let current_provider_id = provider_config.as_ref().and_then(|c| c.provider_id.clone());
    let current_model = provider_config.as_ref().and_then(|c| c.model.clone());
    let stored_provider_id = state.claude_provider_id(&session.id).await;
    let stored_model = state.claude_model(&session.id).await;
    let existing_runtime_ref = state.provider_session_ref(&session.id).await;
    let can_resume_existing_session = existing_runtime_ref.is_some()
        && stored_provider_id == current_provider_id
        && stored_model == current_model;
    let should_fork_existing_session =
        existing_runtime_ref.is_some() && !can_resume_existing_session;
    if should_fork_existing_session {
        eprintln!(
            "[claude] forking session for session={}: provider/model changed old_provider={:?} new_provider={:?} old_model={:?} new_model={:?}",
            session.id, stored_provider_id, current_provider_id, stored_model, current_model,
        );
    }
    let runtime_ref = select_claude_runtime_ref(
        &session.id,
        existing_runtime_ref.as_deref(),
        can_resume_existing_session,
    );
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
    // Build settings, including env overrides to suppress global ~/.claude/settings.json env
    let mut settings_value = serde_json::json!({
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

    // Apply provider configuration
    if let Some(ref config) = provider_config {
        // Inject env overrides into settings so they take precedence over
        // ~/.claude/settings.json env (ANTHROPIC_AUTH_TOKEN, ANTHROPIC_BASE_URL, etc.)
        let mut env_overrides = serde_json::Map::new();
        if !config.api_key.is_empty() {
            env_overrides.insert(
                "ANTHROPIC_AUTH_TOKEN".to_string(),
                serde_json::json!(config.api_key),
            );
            env_overrides.insert(
                "ANTHROPIC_API_KEY".to_string(),
                serde_json::json!(config.api_key),
            );
        }
        if !config.base_url.is_empty() {
            env_overrides.insert(
                "ANTHROPIC_BASE_URL".to_string(),
                serde_json::json!(config.base_url),
            );
        }
        if !env_overrides.is_empty() {
            settings_value["env"] = serde_json::Value::Object(env_overrides);
        }
    }

    let mut command = Command::new("claude");
    command
        .arg("-p")
        .arg("--verbose")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--settings")
        .arg(settings_value.to_string());
    if let Some(reasoning_effort) = reasoning_effort {
        command.arg("--effort").arg(reasoning_effort.as_str());
    }

    // Also set env vars directly on the process as a fallback
    if let Some(ref config) = provider_config {
        if !config.api_key.is_empty() {
            command.env("ANTHROPIC_AUTH_TOKEN", &config.api_key);
            command.env("ANTHROPIC_API_KEY", &config.api_key);
        }
        if !config.base_url.is_empty() {
            command.env("ANTHROPIC_BASE_URL", &config.base_url);
        }
        if let Some(ref model) = config.model {
            command.arg("--model").arg(model);
        }
    }

    let combined_system_prompt = {
        let base = turn_system_prompt(session, system_prompt);
        base
    };
    if let Some(system_prompt) = combined_system_prompt {
        command.arg("--append-system-prompt").arg(system_prompt);
    }
    if should_fork_existing_session {
        command.arg("-r").arg(&runtime_ref).arg("--fork-session");
    } else if can_resume_existing_session {
        command.arg("-r").arg(&runtime_ref);
    } else {
        command.arg("--session-id").arg(&runtime_ref);
    }
    command
        .arg(claude_prompt_input(&input.content))
        .current_dir(&project_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().context("failed to spawn `claude`")?;
    state
        .set_claude_provider_id(&session.id, current_provider_id)
        .await;
    state.set_claude_model(&session.id, current_model).await;
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
                        if let Some(choice) = auto_approve_codex_request(&approval, &project_root).await? {
                            let mut response = response_from_choice(&choice);
                            response.request_id = approval.request_id.clone();
                            tokio::fs::write(
                                response_path(&state_dir, &approval.request_id),
                                serde_json::to_vec_pretty(&response)?,
                            )
                            .await?;
                            state.resolve_approval(&session.id, &approval.request_id, choice).await;
                        } else {
                            pending_approval = Some(approval.request_id.clone());
                            state.raise_approval(&session.id, approval).await;
                        }
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

    if let Some(runtime_session_id) = parsed.session_id.clone() {
        if runtime_session_id != runtime_ref {
            state
                .set_provider_session_ref(&session.id, Some(runtime_session_id))
                .await;
        }
    }

    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn run_opencode(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    provider_config: Option<ResolvedProviderConfig>,
) -> Result<()> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;

    let mut client = OpenCodeHttpClient::start_with_config(provider_config.as_ref()).await?;
    let opencode_session_id =
        if let Some(session_id) = state.provider_session_ref(&session.id).await {
            session_id
        } else {
            let session_id = client
                .create_session(&project_root, &session.title)
                .await
                .context("failed to create opencode session")?;
            state
                .set_provider_session_ref(&session.id, Some(session_id.clone()))
                .await;
            session_id
        };

    let mut last_rendered = String::new();
    let mut full_text = String::new();
    let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
    state.set_approval_sender(&session.id, approval_tx).await;
    let system = turn_system_prompt(session, system_prompt);
    let model = provider_config.as_ref().and_then(|c| c.model.as_deref());
    // Use opencode_provider_name which matches what modify_opencode_config creates
    let opencode_provider = provider_config
        .as_ref()
        .and_then(|c| c.opencode_provider_name.as_deref());
    client
        .prompt_async(
            &opencode_session_id,
            &project_root,
            &input.content,
            system.as_deref(),
            model,
            opencode_provider,
        )
        .await
        .context("failed to send opencode prompt")?;
    let mut event_stream = client
        .event_stream(&project_root)
        .await
        .context("failed to subscribe to opencode events")?;
    let mut pending_approval: Option<PendingApproval> = None;

    loop {
        tokio::select! {
            maybe_line = event_stream.recv() => {
                let Some(line) = maybe_line else {
                    break;
                };
                let line = line?;
                let Some(event) = parse_sse_json_event(&line) else {
                    continue;
                };
                let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
                let properties = event.get("properties").unwrap_or(&Value::Null);
                let event_session_id = properties
                    .get("sessionID")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !event_session_id.is_empty() && event_session_id != opencode_session_id {
                    continue;
                }

                match event_type {
                    "message.part.delta" => {
                        let delta = properties.get("delta").and_then(Value::as_str).unwrap_or_default();
                        if properties.get("field").and_then(Value::as_str).unwrap_or("text") == "text"
                            && !delta.is_empty()
                        {
                            full_text.push_str(delta);
                            push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &full_text).await;
                        }
                    }
                    "permission.asked" => {
                        let request = opencode_permission_to_approval(properties);
                        if let Some(choice) = auto_approve_opencode_request(&request, &project_root).await? {
                            client.reply_permission(&request.request_id, &choice).await?;
                            state.resolve_approval(&session.id, &request.request_id, choice).await;
                        } else {
                            pending_approval = Some(PendingApproval {
                                request: request.clone(),
                                last_choice: None,
                            });
                            state.raise_approval(&session.id, request).await;
                        }
                    }
                    "permission.replied" => {
                        let request_id = properties
                            .get("requestID")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if let Some(pending) = pending_approval.take() {
                            let choice = pending.last_choice.unwrap_or(ApprovalChoice::Accept);
                            state.resolve_approval(&session.id, request_id, choice).await;
                        }
                    }
                    "session.error" => {
                        bail!("{}", render_opencode_error(properties));
                    }
                    "session.idle" => break,
                    _ => {
                        if let Some(status) = render_opencode_event_status(event_type, properties) {
                            state.emit_system_message(&session.id, status).await;
                        }
                    }
                }
            }
            Some(choice) = approval_rx.recv(), if pending_approval.is_some() => {
                let request = pending_approval
                    .as_mut()
                    .context("approval state disappeared unexpectedly")?;
                request.last_choice = Some(choice.clone());
                client.reply_permission(&request.request.request_id, &choice).await?;
            }
        }
    }

    if full_text.trim().is_empty() {
        full_text = client
            .latest_assistant_text(&opencode_session_id, &project_root)
            .await
            .unwrap_or_default();
    }
    let text = (!full_text.trim().is_empty())
        .then(|| full_text.trim().to_string())
        .context("opencode response did not include assistant text")?;
    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
    client.shutdown().await;
    // Clean up overlay config if we created one
    if provider_config.is_some() {
        let overlay = crate::bridge_settings::project_tmp_dir("opencode-overlay");
        let _ = std::fs::remove_dir_all(&overlay);
    }
    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn run_acp(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    provider_config: Option<ResolvedProviderConfig>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    let Some(config) = provider_config else {
        bail!("ACP agent requires an ACP server configuration");
    };
    if matches!(config.acp_profile, Some(AcpProfile::Kiro)) {
        run_kiro_acp(
            state,
            session,
            input,
            system_prompt,
            reply,
            config,
            reasoning_effort,
        )
        .await
    } else {
        run_acp_http(
            state,
            session,
            input,
            system_prompt,
            reply,
            config,
            reasoning_effort,
        )
        .await
    }
}

async fn run_acp_http(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    config: ResolvedProviderConfig,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let client = reqwest::Client::new();
    let session_ref = state.provider_session_ref(&session.id).await;
    let headers = build_acp_http_headers(&config.api_key, &config.extra_headers)?;
    let base_url = config.base_url.clone();

    let combined_system = turn_system_prompt(session, system_prompt);
    let mut body = serde_json::json!({
        "session_id": session_ref.clone().unwrap_or_else(|| session.id.clone()),
        "thread_id": session_ref.clone().unwrap_or_else(|| session.id.clone()),
        "conversation_id": session_ref.clone().unwrap_or_else(|| session.id.clone()),
        "cwd": project_root.display().to_string(),
        "project_root": project_root.display().to_string(),
        "input": input.content,
        "message": input.content,
        "prompt": input.content,
    });
    if let Some(system) = combined_system {
        body["system"] = serde_json::json!(system);
    }
    if let Some(model) = &config.model {
        body["model"] = serde_json::json!(model);
    }
    if let Some(reasoning_effort) = reasoning_effort {
        body["reasoning_effort"] = serde_json::json!(reasoning_effort.as_str());
    }

    let candidate_urls = acp_http_candidate_urls(&config.base_url, &session.id);
    let mut response = None;
    let mut last_error = None;
    for url in candidate_urls {
        let request = client
            .post(&url)
            .headers(headers.clone())
            .header(
                reqwest::header::ACCEPT,
                "text/event-stream, application/json",
            )
            .json(&body)
            .send()
            .await;
        match request {
            Ok(resp) if resp.status().is_success() => {
                response = Some(resp);
                break;
            }
            Ok(resp) => {
                let status = resp.status();
                let body_text = resp.text().await.unwrap_or_default();
                last_error = Some(format!("ACP HTTP {status} {url}: {body_text}"));
            }
            Err(error) => {
                last_error = Some(format!("ACP request failed for {url}: {error}"));
            }
        }
    }
    let response = response.ok_or_else(|| {
        anyhow::anyhow!(last_error.unwrap_or_else(|| "ACP request failed".to_string()))
    })?;

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
    state.set_approval_sender(&session.id, approval_tx).await;
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
    state.set_cancel_sender(&session.id, cancel_tx).await;
    let mut last_rendered = String::new();
    let mut full_text = String::new();
    let mut pending_approval: Option<PendingApproval> = None;

    if content_type.contains("application/json") {
        let value: Value = response
            .json()
            .await
            .context("failed to decode ACP JSON response")?;
        if let Some(runtime_ref) = acp_extract_session_ref(&value) {
            state
                .set_provider_session_ref(&session.id, Some(runtime_ref))
                .await;
        }
        if let Some(text) = acp_extract_text(&value) {
            full_text = text;
        }
        if let Some(status) = acp_extract_status(&value) {
            state.emit_system_message(&session.id, status).await;
        }
    } else {
        let mut buffer = Vec::<u8>::new();
        let mut stream = response.bytes_stream();
        let mut stream_completed = false;
        while !stream_completed {
            tokio::select! {
                maybe_chunk = stream.next() => {
                    let Some(chunk) = maybe_chunk else {
                        break;
                    };
                    let chunk = chunk.context("failed to read ACP event stream")?;
                    buffer.extend_from_slice(&chunk);
                    while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
                        let line = String::from_utf8_lossy(&buffer.drain(..=index).collect::<Vec<_>>())
                            .trim()
                            .to_string();
                        let Some(event) = parse_sse_json_event(&line) else {
                            continue;
                        };
                        if let Some(runtime_ref) = acp_extract_session_ref(&event) {
                            state
                                .set_provider_session_ref(&session.id, Some(runtime_ref))
                                .await;
                        }
                        if let Some(request) = acp_event_to_approval(&event) {
                            if let Some(choice) =
                                auto_approve_codex_request(&request, &project_root).await?
                            {
                                send_acp_http_approval_decision(
                                    &client,
                                    &base_url,
                                    &headers,
                                    &request.request_id,
                                    &choice,
                                    &request.kind,
                                )
                                .await?;
                                state
                                    .resolve_approval(&session.id, &request.request_id, choice)
                                    .await;
                            } else {
                                pending_approval = Some(PendingApproval {
                                    request: request.clone(),
                                    last_choice: None,
                                });
                                state.raise_approval(&session.id, request).await;
                            }
                        }
                        if let Some(text) = acp_extract_text(&event) {
                            full_text.push_str(&text);
                            push_incremental_text(
                                &state,
                                &session.id,
                                &reply.id,
                                &mut last_rendered,
                                &full_text,
                            )
                            .await;
                        }
                        if let Some(status) = acp_extract_status(&event) {
                            state.emit_system_message(&session.id, status).await;
                        }
                        if acp_event_is_error(&event) {
                            bail!("{}", acp_extract_error(&event));
                        }
                        if acp_event_is_done(&event) {
                            stream_completed = true;
                            break;
                        }
                    }
                }
                Some(choice) = approval_rx.recv(), if pending_approval.is_some() => {
                    let request = pending_approval
                        .as_mut()
                        .context("approval state disappeared unexpectedly")?;
                    request.last_choice = Some(choice.clone());
                    send_acp_http_approval_decision(
                        &client,
                        &base_url,
                        &headers,
                        &request.request.request_id,
                        &choice,
                        &request.request.kind,
                    )
                    .await?;
                    state
                        .resolve_approval(&session.id, &request.request.request_id, choice)
                        .await;
                    pending_approval = None;
                }
                Some(()) = cancel_rx.recv() => {
                    let runtime_session_ref = state.provider_session_ref(&session.id).await;
                    let _ = send_acp_http_cancel(
                        &client,
                        &base_url,
                        &headers,
                        runtime_session_ref.as_deref(),
                        &session.id,
                    )
                    .await;
                    return Ok(());
                }
            }
        }
    }

    let text = full_text.trim().to_string();
    if text.is_empty() {
        bail!("ACP response did not include assistant text");
    }
    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
    state
        .finish_assistant_message(&session.id, &reply.id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn run_kiro_acp(
    state: Arc<AppState>,
    session: &SessionSummary,
    input: &ChatMessage,
    system_prompt: Option<&str>,
    reply: &ChatMessage,
    config: ResolvedProviderConfig,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let command_name = config
        .acp_command
        .clone()
        .unwrap_or_else(|| "kiro-cli".to_string());
    let args = if config.acp_args.is_empty() {
        vec!["acp".to_string()]
    } else {
        config.acp_args.clone()
    };

    let mut command = Command::new(&command_name);
    command
        .args(&args)
        .current_dir(&project_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in &config.acp_env {
        command.env(key, value);
    }
    if !config.api_key.is_empty() {
        command.env("KIRO_API_KEY", &config.api_key);
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn ACP runtime `{command_name}`"))?;
    let mut stdin = child
        .stdin
        .take()
        .context("ACP runtime did not expose stdin")?;
    let stdout = child
        .stdout
        .take()
        .context("ACP runtime did not expose stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("ACP runtime did not expose stderr")?;

    let stderr_buffer = Arc::new(tokio::sync::Mutex::new(String::new()));
    let stderr_buffer_task = Arc::clone(&stderr_buffer);
    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = reader.next_line().await? {
            let line = strip_ansi(&line);
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
            let mut shared = stderr_buffer_task.lock().await;
            if !shared.is_empty() {
                shared.push('\n');
            }
            shared.push_str(&line);
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
    let init_request_id = send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "initialize",
        serde_json::json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": {
                    "readTextFile": true,
                    "writeTextFile": true
                },
                "terminal": true,
                "secretStorage": true
            },
            "clientInfo": {
                "name": "omni-code-bridge",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
    )
    .await?;
    wait_for_kiro_json_rpc_response(
        &mut child,
        &mut stdout_rx,
        &mut raw_stdout,
        &stderr_buffer,
        init_request_id,
        acp_json_rpc_handshake_timeout(),
        "initialize",
    )
    .await?;

    let existing_runtime_ref = state.provider_session_ref(&session.id).await;
    let session_response = if let Some(runtime_ref) = existing_runtime_ref.as_deref() {
        let load_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "session/load",
            serde_json::json!({
                "sessionId": runtime_ref,
            }),
        )
        .await?;
        match wait_for_kiro_json_rpc_response(
            &mut child,
            &mut stdout_rx,
            &mut raw_stdout,
            &stderr_buffer,
            load_request_id,
            acp_json_rpc_handshake_timeout(),
            "session/load",
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                let message = format!(
                    "[kiro] failed to load ACP session {runtime_ref}; starting a new session: {error}"
                );
                state.emit_system_message(&session.id, message).await;
                state.set_provider_session_ref(&session.id, None).await;
                let new_request_id = send_json_rpc_request(
                    &mut stdin,
                    &mut next_request_id,
                    "session/new",
                    serde_json::json!({
                        "cwd": project_root.display().to_string(),
                        "mcpServers": [],
                    }),
                )
                .await?;
                wait_for_kiro_json_rpc_response(
                    &mut child,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    &stderr_buffer,
                    new_request_id,
                    acp_json_rpc_handshake_timeout(),
                    "session/new",
                )
                .await?
            }
        }
    } else {
        let new_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "session/new",
            serde_json::json!({
                "cwd": project_root.display().to_string(),
                "mcpServers": [],
            }),
        )
        .await?;
        wait_for_kiro_json_rpc_response(
            &mut child,
            &mut stdout_rx,
            &mut raw_stdout,
            &stderr_buffer,
            new_request_id,
            acp_json_rpc_handshake_timeout(),
            "session/new",
        )
        .await?
    };
    let runtime_session_id = session_response
        .get("sessionId")
        .or_else(|| session_response.pointer("/session/id"))
        .or_else(|| session_response.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| session.id.clone());
    state
        .set_provider_session_ref(&session.id, Some(runtime_session_id.clone()))
        .await;

    if let Some(model) = config.model.as_deref() {
        let mut set_model_params = serde_json::json!({
            "sessionId": runtime_session_id,
        });
        add_kiro_model_id_fields(&mut set_model_params, Some(model));
        let set_model_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "session/set_model",
            set_model_params,
        )
        .await?;
        let _ = wait_for_kiro_json_rpc_response(
            &mut child,
            &mut stdout_rx,
            &mut raw_stdout,
            &stderr_buffer,
            set_model_request_id,
            acp_json_rpc_handshake_timeout(),
            "session/set_model",
        )
        .await;
    }

    let mut content = vec![serde_json::json!({
        "type": "text",
        "text": input.content,
    })];
    if let Some(system) = turn_system_prompt(session, system_prompt) {
        content.insert(
            0,
            serde_json::json!({
                "type": "text",
                "text": format!("System instructions:\n{system}"),
            }),
        );
    }
    if let Some(reasoning_effort) = reasoning_effort {
        content.push(serde_json::json!({
            "type": "text",
            "text": format!("Reasoning effort: {}", reasoning_effort.as_str()),
        }));
    }
    let mut prompt_params = serde_json::json!({
        "sessionId": runtime_session_id,
        "prompt": content.clone(),
        "input": content.clone(),
        "content": content,
    });
    add_kiro_model_id_fields(&mut prompt_params, config.model.as_deref());
    let prompt_request_id = send_json_rpc_request(
        &mut stdin,
        &mut next_request_id,
        "session/prompt",
        prompt_params,
    )
    .await?;

    let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
    state.set_approval_sender(&session.id, approval_tx).await;
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
    state.set_cancel_sender(&session.id, cancel_tx).await;
    let mut pending_approval: Option<PendingApproval> = None;
    let mut last_rendered = String::new();
    let mut full_text = String::new();
    let mut turn_finished = false;
    let mut cancelled = false;

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                let Some(line) = maybe_line else {
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
                    == Some(&prompt_request_id.to_string())
                {
                    if let Some(error) = value.get("error") {
                        let message = error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("Kiro ACP prompt failed");
                        bail!("{message}");
                    }
                    // Some Kiro ACP builds finish a turn by resolving the prompt request
                    // without emitting a separate TurnEnd/session completed update.
                    turn_finished = true;
                    break;
                }

                if value.get("method").and_then(Value::as_str) == Some("session/notification") {
                    let params = value.get("params").unwrap_or(&Value::Null);
                    if let Some(status) = render_kiro_acp_notification_status(params) {
                        state.emit_system_message(&session.id, status).await;
                    }
                    if let Some(delta) = extract_kiro_acp_message_chunk(params) {
                        full_text.push_str(&delta);
                        push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &full_text).await;
                    }
                    if kiro_acp_turn_end(params) {
                        turn_finished = true;
                        break;
                    }
                } else if value.get("method").and_then(Value::as_str) == Some("session/update") {
                    let params = value
                        .pointer("/params/update")
                        .or_else(|| value.get("params"))
                        .unwrap_or(&Value::Null);
                    if let Some(status) = render_kiro_acp_notification_status(params) {
                        state.emit_system_message(&session.id, status).await;
                    }
                    if let Some(delta) = extract_kiro_acp_message_chunk(params) {
                        full_text.push_str(&delta);
                        push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &full_text).await;
                    }
                    if kiro_acp_turn_end(params) {
                        turn_finished = true;
                        break;
                    }
                } else if value.get("method").and_then(Value::as_str) == Some("session/request_permission") {
                    let request_id = value
                        .get("id")
                        .and_then(jsonrpc_id_to_string)
                        .context("Kiro ACP permission request did not include id")?;
                    let params = value.get("params").unwrap_or(&Value::Null);
                    let request = kiro_acp_permission_request_to_approval(&request_id, params);
                    if let Some(choice) = auto_approve_codex_request(&request, &project_root).await? {
                        send_json_rpc_response(
                            &mut stdin,
                            &request_id,
                            kiro_acp_permission_result_json(&choice),
                        )
                        .await?;
                        state.resolve_approval(&session.id, &request.request_id, choice).await;
                    } else {
                        pending_approval = Some(PendingApproval {
                            request: request.clone(),
                            last_choice: None,
                        });
                        state.raise_approval(&session.id, request).await;
                    }
                } else if let Some(method) = value.get("method").and_then(Value::as_str) {
                    let request_id = value.get("id").and_then(jsonrpc_id_to_string);
                    let params = value.get("params").cloned().unwrap_or(Value::Null);
                    if let Some(request_id) = request_id {
                        handle_kiro_client_method_request(&state, &mut stdin, &request_id, method, params)
                            .await?;
                    }
                }
            }
            Some(choice) = approval_rx.recv(), if pending_approval.is_some() => {
                if let Some(pending) = pending_approval.as_mut() {
                    pending.last_choice = Some(choice.clone());
                    send_json_rpc_response(
                        &mut stdin,
                        &pending.request.request_id,
                        kiro_acp_permission_result_json(&choice),
                    )
                    .await?;
                    state.resolve_approval(&session.id, &pending.request.request_id, choice).await;
                }
                pending_approval = None;
            }
            Some(()) = cancel_rx.recv() => {
                let cancel_request_id = send_json_rpc_request(
                    &mut stdin,
                    &mut next_request_id,
                    "session/cancel",
                    serde_json::json!({
                        "sessionId": runtime_session_id,
                    }),
                )
                .await?;
                let _ = wait_for_kiro_json_rpc_response(
                    &mut child,
                    &mut stdout_rx,
                    &mut raw_stdout,
                    &stderr_buffer,
                    cancel_request_id,
                    ACP_JSON_RPC_CANCEL_TIMEOUT,
                    "session/cancel",
                ).await;
                cancelled = true;
                break;
            }
        }
    }

    drop(stdin);
    let status = match tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            child.kill().await?;
            child.wait().await?
        }
    };
    let stderr = stderr_task
        .await
        .context("failed to join ACP stderr reader")??;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("ACP runtime exited with status {code}");
        } else {
            bail!("ACP runtime exited with status {code}: {stderr}");
        }
    }
    if cancelled {
        return Ok(());
    }
    if !turn_finished && full_text.trim().is_empty() {
        bail!("Kiro ACP response did not include assistant text");
    }
    let text = full_text.trim().to_string();
    if text.is_empty() {
        bail!("Kiro ACP response did not include assistant text");
    }
    push_incremental_text(&state, &session.id, &reply.id, &mut last_rendered, &text).await;
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

async fn summarize_with_opencode(
    state: Arc<AppState>,
    session: &SessionSummary,
    content: &str,
) -> Result<String> {
    let project_root = state
        .project_root_path_for_session(&session.id)
        .await
        .map(PathBuf::from)
        .map_err(anyhow::Error::msg)?;
    let mut client = OpenCodeHttpClient::start().await?;
    let session_id = client
        .create_session(&project_root, "omni-code summary")
        .await
        .context("failed to create opencode summary session")?;
    client
        .prompt_async(
            &session_id,
            &project_root,
            &summary_prompt(content),
            None,
            None,
            None,
        )
        .await
        .context("failed to send opencode summary prompt")?;
    wait_for_opencode_session_idle(&mut client, &session_id, &project_root).await?;
    let text = client
        .latest_assistant_text(&session_id, &project_root)
        .await
        .context("opencode summary did not include assistant text")?;
    client.shutdown().await;
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

fn build_acp_http_headers(
    api_key: &str,
    extra_headers: &[(String, String)],
) -> Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    if !api_key.is_empty() {
        let value = format!("Bearer {}", api_key);
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&value)
                .context("invalid ACP authorization header")?,
        );
    }
    for (key, value) in extra_headers {
        headers.insert(
            reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .context("invalid ACP header name")?,
            reqwest::header::HeaderValue::from_str(value).context("invalid ACP header value")?,
        );
    }
    Ok(headers)
}

fn acp_http_candidate_urls(endpoint: &str, session_id: &str) -> Vec<String> {
    let endpoint = endpoint.trim_end_matches('/').to_string();
    vec![
        format!("{endpoint}/turns"),
        format!("{endpoint}/turn"),
        format!("{endpoint}/sessions/{}/turns", url_path_escape(session_id)),
        endpoint,
    ]
}

fn acp_http_turn_url_templates(endpoint: &str) -> Vec<String> {
    let endpoint = endpoint.trim_end_matches('/').to_string();
    vec![
        format!("{endpoint}/turns"),
        format!("{endpoint}/turn"),
        format!("{endpoint}/sessions/{{session_id}}/turns"),
        endpoint,
    ]
}

fn acp_http_approval_reply_urls(base_url: &str, request_id: &str) -> Vec<String> {
    let base_url = base_url.trim_end_matches('/').to_string();
    let request_id = url_path_escape(request_id);
    vec![
        format!("{base_url}/approvals/{request_id}/reply"),
        format!("{base_url}/approval/{request_id}/reply"),
        format!("{base_url}/permissions/{request_id}/reply"),
        format!("{base_url}/permission/{request_id}/reply"),
        format!("{base_url}/approvals/{request_id}"),
        format!("{base_url}/approval/{request_id}"),
    ]
}

fn acp_http_approval_reply_url_templates(base_url: &str) -> Vec<String> {
    let base_url = base_url.trim_end_matches('/').to_string();
    vec![
        format!("{base_url}/approvals/{{request_id}}/reply"),
        format!("{base_url}/approval/{{request_id}}/reply"),
        format!("{base_url}/permissions/{{request_id}}/reply"),
        format!("{base_url}/permission/{{request_id}}/reply"),
        format!("{base_url}/approvals/{{request_id}}"),
        format!("{base_url}/approval/{{request_id}}"),
    ]
}

fn acp_http_cancel_urls(
    base_url: &str,
    runtime_session_ref: Option<&str>,
    fallback_session_id: &str,
) -> Vec<String> {
    let base_url = base_url.trim_end_matches('/').to_string();
    let session_ref = runtime_session_ref.unwrap_or(fallback_session_id);
    let session_ref = url_path_escape(session_ref);
    vec![
        format!("{base_url}/sessions/{session_ref}/cancel"),
        format!("{base_url}/session/{session_ref}/cancel"),
        format!("{base_url}/turns/cancel"),
        format!("{base_url}/turn/cancel"),
        format!("{base_url}/cancel"),
    ]
}

fn acp_http_cancel_url_templates(base_url: &str) -> Vec<String> {
    let base_url = base_url.trim_end_matches('/').to_string();
    vec![
        format!("{base_url}/sessions/{{session_ref}}/cancel"),
        format!("{base_url}/session/{{session_ref}}/cancel"),
        format!("{base_url}/turns/cancel"),
        format!("{base_url}/turn/cancel"),
        format!("{base_url}/cancel"),
    ]
}

async fn send_acp_http_approval_decision(
    client: &reqwest::Client,
    base_url: &str,
    headers: &reqwest::header::HeaderMap,
    request_id: &str,
    choice: &ApprovalChoice,
    kind: &ApprovalKind,
) -> Result<()> {
    let body = approval_result_json(choice, kind);
    let mut last_error = None;
    for url in acp_http_approval_reply_urls(base_url, request_id) {
        match client
            .post(&url)
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let status = response.status();
                let body_text = response.text().await.unwrap_or_default();
                last_error = Some(format!("ACP approval reply {status} {url}: {body_text}"));
            }
            Err(error) => {
                last_error = Some(format!("ACP approval reply failed for {url}: {error}"));
            }
        }
    }
    bail!(
        "{}",
        last_error.unwrap_or_else(|| "ACP approval reply failed".to_string())
    )
}

async fn send_acp_http_cancel(
    client: &reqwest::Client,
    base_url: &str,
    headers: &reqwest::header::HeaderMap,
    runtime_session_ref: Option<&str>,
    fallback_session_id: &str,
) -> Result<()> {
    let body = serde_json::json!({
        "session_id": runtime_session_ref.unwrap_or(fallback_session_id),
        "thread_id": runtime_session_ref.unwrap_or(fallback_session_id),
        "conversation_id": runtime_session_ref.unwrap_or(fallback_session_id),
    });
    let mut last_error = None;
    for url in acp_http_cancel_urls(base_url, runtime_session_ref, fallback_session_id) {
        match client
            .post(&url)
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let status = response.status();
                let body_text = response.text().await.unwrap_or_default();
                last_error = Some(format!("ACP cancel {status} {url}: {body_text}"));
            }
            Err(error) => {
                last_error = Some(format!("ACP cancel failed for {url}: {error}"));
            }
        }
    }
    bail!(
        "{}",
        last_error.unwrap_or_else(|| "ACP cancel failed".to_string())
    )
}

async fn read_acp_probe_response(
    response: reqwest::Response,
) -> Result<(String, Option<String>, Option<String>, bool)> {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    if content_type.contains("text/event-stream") {
        let mut full_text = String::new();
        let mut session_ref = None;
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::<u8>::new();
        let mut done = false;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed to read ACP probe event stream")?;
            buffer.extend_from_slice(&chunk);
            while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
                let line = String::from_utf8_lossy(&buffer.drain(..=index).collect::<Vec<_>>())
                    .trim()
                    .to_string();
                let Some(event) = parse_sse_json_event(&line) else {
                    continue;
                };
                if session_ref.is_none() {
                    session_ref = acp_extract_session_ref(&event);
                }
                if let Some(text) = acp_extract_text(&event) {
                    full_text.push_str(&text);
                }
                if acp_event_is_error(&event) {
                    bail!("{}", acp_extract_error(&event));
                }
                if acp_event_is_done(&event) {
                    done = true;
                    break;
                }
            }
            if done {
                break;
            }
        }

        return Ok((content_type, session_ref, Some(full_text), done));
    }

    let body_text = response.text().await.unwrap_or_default();
    let trimmed_body = body_text.trim().to_string();
    let parsed_json = serde_json::from_str::<Value>(&trimmed_body).ok();
    let session_ref = parsed_json.as_ref().and_then(acp_extract_session_ref);
    let text = parsed_json
        .as_ref()
        .and_then(acp_extract_text)
        .or_else(|| (!trimmed_body.is_empty()).then_some(trimmed_body));
    Ok((content_type, session_ref, text, false))
}

#[derive(Default)]
struct CodexAppServerStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    partial_agent_messages: BTreeMap<String, String>,
    latest_status: Option<String>,
    session_id: Option<String>,
    running_command: Option<RunningCommand>,
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
struct RunningCommand {
    item_id: String,
    command: String,
    idle_ticks: u32,
    recovery_requested: bool,
}

#[derive(Clone)]
struct PendingApproval {
    request: ApprovalRequest,
    last_choice: Option<ApprovalChoice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodexSlashAction<'a> {
    Compact,
    ReviewUncommittedChanges,
    ReviewCustom { instructions: &'a str },
    Rename { title: &'a str },
    GoalSet { objective: &'a str },
    GoalClear,
}

fn classify_codex_slash_command(input: &str) -> Option<CodexSlashAction<'_>> {
    let command = parse_slash_command(input)?;
    match command.name {
        "compact" if command.args.is_empty() => Some(CodexSlashAction::Compact),
        "review" if command.args.is_empty() => Some(CodexSlashAction::ReviewUncommittedChanges),
        "review" => Some(CodexSlashAction::ReviewCustom {
            instructions: command.args,
        }),
        "rename" if !command.args.is_empty() => Some(CodexSlashAction::Rename {
            title: command.args,
        }),
        "goal" if !command.args.is_empty() => Some(CodexSlashAction::GoalSet {
            objective: command.args,
        }),
        "clear-goal" if command.args.is_empty() => Some(CodexSlashAction::GoalClear),
        _ => None,
    }
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
                "thread/tokenUsage/updated" | "account/rateLimits/updated" => {
                    CodexAppServerEvent::None
                }
                "item/commandExecution/outputDelta" => {
                    self.mark_command_activity(params.get("itemId").and_then(Value::as_str));
                    render_codex_status_notification(method, params)
                        .map(CodexAppServerEvent::Status)
                        .unwrap_or(CodexAppServerEvent::None)
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
                "item/started" | "item/completed" => {
                    self.ingest_app_server_item(params.get("item").unwrap_or(&Value::Null), method)
                }
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
                    let error = &params["error"];
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("codex app-server error");
                    let details = error
                        .get("additionalDetails")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let full = if details.is_empty() {
                        message.to_string()
                    } else {
                        format!("{message}: {details}")
                    };
                    CodexAppServerEvent::TurnFailed(full)
                }
                _ => render_codex_status_notification(method, params)
                    .map(CodexAppServerEvent::Status)
                    .unwrap_or_else(|| {
                        CodexAppServerEvent::Status(format!(
                            "[debug:codex:method] unhandled method={method}"
                        ))
                    }),
            };
        }

        CodexAppServerEvent::None
    }

    fn mark_command_activity(&mut self, item_id: Option<&str>) {
        let Some(running) = self.running_command.as_mut() else {
            return;
        };
        if item_id.is_none_or(|item_id| item_id.is_empty() || item_id == running.item_id) {
            running.idle_ticks = 0;
            running.recovery_requested = false;
        }
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
            "item/fileChange/requestApproval" => Some(PendingApproval {
                request: ApprovalRequest {
                    request_id: request_id.clone(),
                    kind: ApprovalKind::FileChange,
                    command: render_file_change_approval_command(params),
                    reason: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .or_else(|| Some("Apply proposed edits".to_string())),
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
            "applyPatchApproval" => Some(PendingApproval {
                request: ApprovalRequest {
                    request_id: request_id.clone(),
                    kind: ApprovalKind::ApplyPatch,
                    command: render_file_change_approval_command(params),
                    reason: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .or_else(|| Some("Apply proposed edits".to_string())),
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

    fn ingest_app_server_item(&mut self, item: &Value, method: &str) -> CodexAppServerEvent {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match item_type {
            "agentMessage" if method == "item/completed" => {
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                let Some(text) = item.get("text").and_then(Value::as_str) else {
                    return CodexAppServerEvent::None;
                };
                let text = text.trim().to_string();
                if text.is_empty() {
                    return CodexAppServerEvent::None;
                }
                self.partial_agent_messages.remove(item_id);
                self.display_blocks.push(text.clone());
                self.assistant_blocks.push(text);
                self.render_assistant_text()
                    .map(CodexAppServerEvent::Content)
                    .unwrap_or(CodexAppServerEvent::None)
            }
            "agentMessage" => CodexAppServerEvent::None,
            "userMessage" => CodexAppServerEvent::None,
            "reasoning" => {
                self.latest_status = Some(if method == "item/started" {
                    "[reasoning] thinking".to_string()
                } else {
                    "[reasoning] complete".to_string()
                });
                CodexAppServerEvent::None
            }
            "commandExecution" => {
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                let command = item
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let status = item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if method == "item/started" || status == "inProgress" {
                    if !item_id.is_empty() {
                        self.running_command = Some(RunningCommand {
                            item_id: item_id.to_string(),
                            command: command.to_string(),
                            idle_ticks: 0,
                            recovery_requested: false,
                        });
                    }
                } else if method == "item/completed" {
                    let completed_running = self
                        .running_command
                        .as_ref()
                        .map(|running| item_id.is_empty() || running.item_id == item_id)
                        .unwrap_or(false);
                    if completed_running {
                        self.running_command = None;
                    }
                }
                self.latest_status = Some(render_command_summary(
                    command,
                    match status {
                        "inProgress" => "running",
                        other => other,
                    },
                    item.get("exitCode").and_then(Value::as_i64),
                ));
                CodexAppServerEvent::None
            }
            "webSearch" => {
                self.latest_status = Some(render_web_search_summary(
                    item.get("query")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    method,
                ));
                CodexAppServerEvent::None
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
                CodexAppServerEvent::None
            }
        }
    }

    fn mark_idle_waiting(&mut self, pending_approval: bool) -> Option<CommandWatchdogAction> {
        let waiting = if pending_approval {
            "[status] waiting for approval"
        } else if let Some(running) = self.running_command.as_mut() {
            running.idle_ticks += 1;
            if running.idle_ticks == 1 {
                "[status] command still running"
            } else if running.idle_ticks >= CODEX_COMMAND_SOFT_RECOVERY_IDLE_TICKS
                && !running.recovery_requested
            {
                running.recovery_requested = true;
                self.latest_status = Some(render_command_stalled_summary(
                    &running.command,
                    "no output yet; waiting once more",
                ));
                return Some(CommandWatchdogAction::SoftRecovery {
                    command: running.command.clone(),
                });
            } else if running.idle_ticks >= CODEX_COMMAND_STALLED_IDLE_TICKS {
                self.latest_status = Some(render_command_stalled_summary(
                    &running.command,
                    "no completion event after recovery wait",
                ));
                return Some(CommandWatchdogAction::Stalled {
                    command: running.command.clone(),
                });
            } else {
                "[status] command still running"
            }
        } else {
            "[status] waiting for Codex response"
        };
        if self.latest_status.as_deref() == Some(waiting) {
            None
        } else {
            self.latest_status = Some(waiting.to_string());
            None
        }
    }

    fn finish_text(&self) -> Option<String> {
        let mut blocks = self.assistant_blocks.clone();
        for partial in self.partial_agent_messages.values() {
            let partial = partial.trim();
            if !partial.is_empty() {
                blocks.push(partial.to_string());
            }
        }
        let text = blocks.join("\n\n---\n\n");
        if text.is_empty() { None } else { Some(text) }
    }

    fn render_assistant_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        for partial in self.partial_agent_messages.values() {
            let partial = partial.trim();
            if !partial.is_empty() {
                blocks.push(partial.to_string());
            }
        }
        let text = blocks.join("\n\n---\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

enum CommandWatchdogAction {
    SoftRecovery { command: String },
    Stalled { command: String },
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
    eprintln!("[codex:jsonrpc:send] id={request_id} method={method}");
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
    .await?;
    eprintln!("[codex:jsonrpc:send-response] id={request_id}");
    Ok(())
}

async fn send_json_rpc_error(
    writer: &mut (impl AsyncWrite + Unpin),
    request_id: &str,
    code: i64,
    message: &str,
) -> Result<()> {
    let id = request_id
        .parse::<u64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::String(request_id.to_string()));
    write_json_line(
        writer,
        &serde_json::json!({
            "id": id,
            "error": {
                "code": code,
                "message": message,
            },
        }),
    )
    .await?;
    eprintln!("[codex:jsonrpc:send-error] id={request_id} code={code} message={message}");
    Ok(())
}

async fn handle_kiro_client_method_request(
    state: &AppState,
    writer: &mut (impl AsyncWrite + Unpin),
    request_id: &str,
    method: &str,
    params: Value,
) -> Result<()> {
    match method {
        "secretStorage/get" | "secretStorage/getSecret" | "secrets/get" => {
            let key = secret_storage_key_from_params(&params)?;
            let value = state.secret_store().get(&key).await;
            send_json_rpc_response(
                writer,
                request_id,
                serde_json::json!({
                    "value": value,
                }),
            )
            .await
        }
        "secretStorage/set" | "secretStorage/setSecret" | "secrets/set" => {
            let (key, value) = secret_storage_key_value_from_params(&params)?;
            state.secret_store().set(key, value).await?;
            send_json_rpc_response(writer, request_id, serde_json::json!({})).await
        }
        "secretStorage/delete" | "secretStorage/deleteSecret" | "secrets/delete" => {
            let key = secret_storage_key_from_params(&params)?;
            state.secret_store().delete(&key).await?;
            send_json_rpc_response(writer, request_id, serde_json::json!({})).await
        }
        _ => send_json_rpc_error(writer, request_id, -32601, &format!("Method not found: {method}"))
            .await,
    }
}

fn secret_storage_key_from_params(params: &Value) -> Result<String> {
    params
        .get("key")
        .or_else(|| params.get("name"))
        .or_else(|| params.pointer("/secret/key"))
        .or_else(|| params.pointer("/secret/name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .context("secret storage request did not include a key")
}

fn secret_storage_key_value_from_params(params: &Value) -> Result<(String, String)> {
    let key = secret_storage_key_from_params(params)?;
    let value = params
        .get("value")
        .or_else(|| params.pointer("/secret/value"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .context("secret storage set request did not include a value")?;
    Ok((key, value))
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
            .context("JSON-RPC runtime closed before responding")?
            .map_err(anyhow::Error::msg)?;
        if !raw_stdout.is_empty() {
            raw_stdout.push('\n');
        }
        raw_stdout.push_str(&line);
        eprintln!("[codex:jsonrpc:recv] {line}");
        let value: Value = serde_json::from_str(&line)
            .with_context(|| "JSON-RPC runtime produced invalid JSON")?;
        if value.get("id").and_then(jsonrpc_id_to_string).as_deref()
            == Some(&request_id.to_string())
        {
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("JSON-RPC runtime request failed");
                bail!("{message}");
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
    }
}

async fn wait_for_json_rpc_response_with_timeout(
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    request_id: u64,
    timeout: Duration,
) -> Result<Value> {
    match tokio::time::timeout(
        timeout,
        wait_for_json_rpc_response(stdout_rx, raw_stdout, request_id),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => bail!(
            "JSON-RPC runtime timed out waiting for response id={request_id} after {}ms",
            timeout.as_millis()
        ),
    }
}

async fn wait_for_kiro_json_rpc_response(
    child: &mut Child,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    stderr_buffer: &Arc<tokio::sync::Mutex<String>>,
    request_id: u64,
    timeout: Duration,
    request_name: &str,
) -> Result<Value> {
    match wait_for_json_rpc_response_with_timeout(stdout_rx, raw_stdout, request_id, timeout).await
    {
        Ok(value) => Ok(value),
        Err(error) => {
            let stderr = stderr_buffer.lock().await.clone();
            if let Some(status) = child.try_wait()? {
                let code = status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "signal".to_string());
                bail!(
                    "{}",
                    format_kiro_runtime_exit_error(request_name, request_id, &code, stderr.trim())
                );
            }
            let stderr = stderr.trim();
            if stderr.is_empty() {
                Err(error)
            } else {
                Err(error.context(format!(
                    "Kiro ACP runtime stderr while waiting for {request_name}: {stderr}"
                )))
            }
        }
    }
}

async fn wait_for_stdout_line(
    child: &mut Child,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    stderr_buffer: &Arc<tokio::sync::Mutex<String>>,
    timeout: Duration,
    request_name: &str,
) -> Result<String> {
    match tokio::time::timeout(timeout, stdout_rx.recv()).await {
        Ok(Some(Ok(line))) => Ok(line),
        Ok(Some(Err(error))) => {
            bail!("Kiro ACP runtime stdout error during {request_name}: {error}")
        }
        Ok(None) => {
            let status = child
                .try_wait()
                .context("failed to poll Kiro ACP runtime while reading stdout")?;
            let status_code = status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "running".to_string());
            let stderr = stderr_buffer.lock().await.clone();
            bail!(
                "{}",
                format_kiro_runtime_exit_error(request_name, 0, &status_code, stderr.trim())
            );
        }
        Err(_) => bail!(
            "Kiro ACP runtime timed out waiting for {request_name} output after {}ms",
            timeout.as_millis()
        ),
    }
}

fn format_kiro_runtime_exit_error(
    request_name: &str,
    request_id: u64,
    status_code: &str,
    stderr: &str,
) -> String {
    let mut message = if stderr.is_empty() {
        format!(
            "Kiro ACP runtime exited before responding to {request_name} (id={request_id}, status={status_code})"
        )
    } else {
        format!(
            "Kiro ACP runtime exited before responding to {request_name} (id={request_id}, status={status_code}): {stderr}"
        )
    };
    let stderr_lower = stderr.to_ascii_lowercase();
    if stderr_lower.contains("not logged in") || stderr_lower.contains("please log in") {
        message.push_str(". Run `kiro-cli login` and try again.");
    }
    message
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

fn render_file_change_approval_command(params: &Value) -> Option<String> {
    let files = params
        .get("files")
        .or_else(|| params.get("fileChanges"))
        .or_else(|| params.get("changes"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(file_change_path)
                .take(5)
                .collect::<Vec<_>>()
        })
        .filter(|paths| !paths.is_empty());

    if let Some(paths) = files {
        let suffix = if paths.len() == 5 { "..." } else { "" };
        return Some(format!("edit files: {}{}", paths.join(", "), suffix));
    }

    params
        .get("path")
        .or_else(|| params.get("file"))
        .or_else(|| params.pointer("/file/path"))
        .and_then(Value::as_str)
        .map(|path| format!("edit file: {path}"))
        .or_else(|| Some("apply proposed edits".to_string()))
}

fn file_change_path(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("path")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("file")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("relativePath")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
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

fn render_command_stalled_summary(command: &str, reason: &str) -> String {
    let command = if command.trim().is_empty() {
        "command"
    } else {
        command
    };
    format!("[command:stalled] {command} ({reason})")
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

fn render_codex_status_notification(method: &str, params: &Value) -> Option<String> {
    match method {
        "mcpServer/startupStatus/updated" => render_codex_startup_status(params),
        "thread/status/changed" => render_codex_thread_status(params),
        "turn/started" => Some("[turn] started".to_string()),
        "turn/plan/updated" => render_codex_plan_update(params),
        "item/fileChange/outputDelta" | "item/fileChange/patchUpdated" => {
            render_codex_file_change_delta(params)
        }
        "turn/diff/updated" => render_codex_turn_diff(params),
        "thread/archived" => render_codex_thread_lifecycle(params, "archived"),
        "thread/deleted" => render_codex_thread_lifecycle(params, "deleted"),
        "thread/unarchived" => render_codex_thread_lifecycle(params, "unarchived"),
        "thread/closed" => render_codex_thread_lifecycle(params, "closed"),
        "skills/changed" => Some("[skills] changed".to_string()),
        "thread/name/updated" => render_codex_thread_name(params),
        "thread/goal/updated" => render_codex_thread_goal(params, "updated"),
        "thread/goal/cleared" => Some("[goal] cleared".to_string()),
        "thread/settings/updated" => Some("[thread] settings updated".to_string()),
        "thread/compacted" => Some("[context] compacted".to_string()),
        "hook/started" => Some("[hook] started".to_string()),
        "hook/completed" => Some("[hook] completed".to_string()),
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
            render_codex_delta_status(params, "[reasoning]", "delta")
        }
        "item/reasoning/summaryPartAdded" => Some("[reasoning] summary updated".to_string()),
        "item/plan/delta" => render_codex_delta_status(params, "[plan]", "delta"),
        "command/exec/outputDelta" => render_codex_base64_output_status(params, "[exec]"),
        "process/outputDelta" => render_codex_base64_output_status(params, "[process]"),
        "process/exited" => render_codex_process_exit(params),
        "item/commandExecution/outputDelta" => {
            render_codex_delta_status(params, "[command:output]", "delta")
        }
        "item/commandExecution/terminalInteraction" => {
            Some("[command] terminal interaction".to_string())
        }
        "item/autoApprovalReview/started" => Some("[approval] auto review started".to_string()),
        "item/autoApprovalReview/completed" => Some("[approval] auto review completed".to_string()),
        "rawResponseItem/completed" => None,
        "mcpServer/oauthLogin/completed" => Some("[mcp] oauth login completed".to_string()),
        "account/updated" => render_codex_account_updated(params),
        "app/list/updated" => render_codex_app_list_updated(params),
        "remoteControl/status/changed" => render_codex_remote_control_status(params),
        "externalAgentConfig/import/completed" => {
            Some("[external-agent] config import completed".to_string())
        }
        "fs/changed" => render_codex_fs_changed(params),
        "item/mcpToolCall/progress" => render_codex_mcp_tool_progress(params),
        "model/rerouted" => render_codex_model_reroute(params),
        "model/verification" => render_codex_model_verification(params),
        "turn/moderationMetadata" => Some("[moderation] metadata updated".to_string()),
        "warning" | "guardianWarning" | "deprecationNotice" | "configWarning" => {
            render_codex_notice(params, method)
        }
        "fuzzyFileSearch/sessionUpdated" => render_codex_fuzzy_search(params, "updated"),
        "fuzzyFileSearch/sessionCompleted" => render_codex_fuzzy_search(params, "completed"),
        "thread/realtime/started" => Some("[realtime] started".to_string()),
        "thread/realtime/itemAdded" => render_codex_realtime_item(params),
        "thread/realtime/transcript/delta" => render_codex_realtime_transcript(params, "delta"),
        "thread/realtime/transcript/done" => render_codex_realtime_transcript(params, "done"),
        "thread/realtime/outputAudio/delta" => Some("[realtime] output audio".to_string()),
        "thread/realtime/sdp" => Some("[realtime] sdp received".to_string()),
        "thread/realtime/error" => render_codex_notice(params, "warning")
            .map(|message| message.replacen("[warning]", "[realtime:error]", 1)),
        "thread/realtime/closed" => render_codex_realtime_closed(params),
        "windows/worldWritableWarning" => render_codex_windows_world_writable(params),
        "windowsSandbox/setupCompleted" => render_codex_windows_sandbox(params),
        "account/login/completed" => render_codex_account_login(params),
        _ => None,
    }
}

fn render_codex_thread_lifecycle(params: &Value, action: &str) -> Option<String> {
    let thread_id = params
        .get("threadId")
        .or_else(|| params.pointer("/thread/id"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 16));
    Some(match thread_id {
        Some(thread_id) if !thread_id.is_empty() => format!("[thread] {action}: {thread_id}"),
        _ => format!("[thread] {action}"),
    })
}

fn render_codex_thread_name(params: &Value) -> Option<String> {
    let name = params
        .get("threadName")
        .or_else(|| params.pointer("/thread/name"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 80))
        .filter(|text| !text.is_empty())?;
    Some(format!("[thread] renamed: {name}"))
}

fn render_codex_thread_goal(params: &Value, action: &str) -> Option<String> {
    let goal = params
        .pointer("/goal/text")
        .or_else(|| params.pointer("/goal/title"))
        .or_else(|| params.get("goal"))
        .and_then(extract_text_from_json)
        .map(|text| truncate_tool_text(&text, 100))
        .filter(|text| !text.is_empty())?;
    Some(format!("[goal] {action}: {goal}"))
}

fn render_codex_delta_status(params: &Value, label: &str, key: &str) -> Option<String> {
    let text = params
        .get(key)
        .and_then(Value::as_str)
        .or_else(|| params.get("text").and_then(Value::as_str))
        .or_else(|| params.get("message").and_then(Value::as_str))
        .map(|text| truncate_tool_text(text, 120))
        .filter(|text| !text.is_empty())?;
    Some(format!("{label} {text}"))
}

fn render_codex_base64_output_status(params: &Value, label: &str) -> Option<String> {
    use base64::Engine;

    let stream = params
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or("output");
    let encoded = params.get("deltaBase64").and_then(Value::as_str)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .map(|text| truncate_tool_text(&text, 120))
        .filter(|text| !text.is_empty())?;
    Some(format!("{label}:{stream} {decoded}"))
}

fn render_codex_process_exit(params: &Value) -> Option<String> {
    let exit_code = params.get("exitCode").and_then(Value::as_i64)?;
    let mut segments = vec![format!("exit {exit_code}")];
    for key in ["stdout", "stderr"] {
        if let Some(text) = params
            .get(key)
            .and_then(Value::as_str)
            .map(|text| truncate_tool_text(text, 80))
            .filter(|text| !text.is_empty())
        {
            segments.push(format!("{key}: {text}"));
        }
    }
    Some(format!("[process] {}", segments.join(" | ")))
}

fn render_codex_mcp_tool_progress(params: &Value) -> Option<String> {
    params
        .get("message")
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 120))
        .filter(|text| !text.is_empty())
        .map(|message| format!("[mcp:tool] {message}"))
}

fn render_codex_account_updated(params: &Value) -> Option<String> {
    let auth = params
        .get("authMode")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let plan = params.get("planType").and_then(Value::as_str);
    Some(match plan {
        Some(plan) => format!("[account] updated auth={auth} plan={plan}"),
        None => format!("[account] updated auth={auth}"),
    })
}

fn render_codex_app_list_updated(params: &Value) -> Option<String> {
    let count = params
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    Some(format!("[apps] list updated ({count})"))
}

fn render_codex_remote_control_status(params: &Value) -> Option<String> {
    let status = params
        .get("status")
        .and_then(extract_text_from_json)
        .unwrap_or_else(|| "updated".to_string());
    let server = params
        .get("serverName")
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 60));
    Some(match server {
        Some(server) if !server.is_empty() => format!("[remote-control] {status}: {server}"),
        _ => format!("[remote-control] {status}"),
    })
}

fn render_codex_fs_changed(params: &Value) -> Option<String> {
    let paths = params
        .get("changedPaths")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(if paths.is_empty() {
        "[fs] changed".to_string()
    } else {
        format!("[fs] changed {}", format_file_preview(&paths))
    })
}

fn render_codex_model_reroute(params: &Value) -> Option<String> {
    let from = params
        .get("fromModel")
        .and_then(Value::as_str)
        .unwrap_or("model");
    let to = params
        .get("toModel")
        .and_then(Value::as_str)
        .unwrap_or("model");
    let reason = params
        .get("reason")
        .and_then(extract_text_from_json)
        .map(|text| truncate_tool_text(&text, 80))
        .filter(|text| !text.is_empty());
    Some(match reason {
        Some(reason) => format!("[model] rerouted {from} -> {to}: {reason}"),
        None => format!("[model] rerouted {from} -> {to}"),
    })
}

fn render_codex_model_verification(params: &Value) -> Option<String> {
    let count = params
        .get("verifications")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    Some(if count == 0 {
        "[model] verification updated".to_string()
    } else {
        format!("[model] verification updated ({count})")
    })
}

fn render_codex_notice(params: &Value, method: &str) -> Option<String> {
    let label = match method {
        "guardianWarning" => "[guardian]",
        "deprecationNotice" => "[deprecation]",
        "configWarning" => "[config]",
        _ => "[warning]",
    };
    let text = params
        .get("message")
        .or_else(|| params.get("summary"))
        .or_else(|| params.get("details"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 160))
        .filter(|text| !text.is_empty())?;
    Some(format!("{label} {text}"))
}

fn render_codex_fuzzy_search(params: &Value, action: &str) -> Option<String> {
    let query = params
        .get("query")
        .or_else(|| params.get("pattern"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 80));
    Some(match query {
        Some(query) if !query.is_empty() => format!("[file-search] {action}: {query}"),
        _ => format!("[file-search] {action}"),
    })
}

fn render_codex_realtime_item(params: &Value) -> Option<String> {
    let item_type = params
        .pointer("/item/type")
        .and_then(Value::as_str)
        .unwrap_or("item");
    Some(format!("[realtime] item added: {item_type}"))
}

fn render_codex_realtime_transcript(params: &Value, phase: &str) -> Option<String> {
    let role = params
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("audio");
    let text = params
        .get(if phase == "done" { "text" } else { "delta" })
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 120))
        .filter(|text| !text.is_empty())?;
    Some(format!("[realtime:{role}:{phase}] {text}"))
}

fn render_codex_realtime_closed(params: &Value) -> Option<String> {
    let reason = params
        .get("reason")
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 80))
        .filter(|text| !text.is_empty());
    Some(match reason {
        Some(reason) => format!("[realtime] closed: {reason}"),
        None => "[realtime] closed".to_string(),
    })
}

fn render_codex_windows_world_writable(params: &Value) -> Option<String> {
    let extra = params
        .get("extraCount")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let paths = params
        .get("samplePaths")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let failed_scan = params
        .get("failedScan")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if failed_scan {
        return Some("[windows] world-writable path scan failed".to_string());
    }
    Some(if paths.is_empty() {
        format!("[windows] world-writable paths detected (+{extra})")
    } else {
        format!(
            "[windows] world-writable paths: {} (+{extra})",
            format_file_preview(&paths)
        )
    })
}

fn render_codex_windows_sandbox(params: &Value) -> Option<String> {
    let mode = params
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("setup");
    let success = params
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let error = params
        .get("error")
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 100))
        .filter(|text| !text.is_empty());
    Some(match (success, error) {
        (true, _) => format!("[windows-sandbox] {mode} ready"),
        (false, Some(error)) => format!("[windows-sandbox] {mode} failed: {error}"),
        (false, None) => format!("[windows-sandbox] {mode} failed"),
    })
}

fn render_codex_account_login(params: &Value) -> Option<String> {
    let success = params
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let error = params
        .get("error")
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 100))
        .filter(|text| !text.is_empty());
    Some(match (success, error) {
        (true, _) => "[account] login completed".to_string(),
        (false, Some(error)) => format!("[account] login failed: {error}"),
        (false, None) => "[account] login failed".to_string(),
    })
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
    stream_text: String,
    session_id: Option<String>,
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
            "stream_event" => {
                Ok(self.ingest_stream_event(map.get("event").or_else(|| map.get("stream_event"))))
            }
            "system" => {
                if self.session_id.is_none() {
                    self.session_id = map
                        .get("session_id")
                        .or_else(|| map.get("sessionId"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                }
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

    fn ingest_stream_event(&mut self, event: Option<&Value>) -> Option<String> {
        let Some(event_value) = event else {
            return None;
        };
        let Some(event) = event_value.as_object() else {
            return None;
        };

        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match event_type {
            "content_block_start" => {
                if let Some(text) = event
                    .get("content_block")
                    .and_then(extract_text_from_json)
                    .or_else(|| event.get("block").and_then(extract_text_from_json))
                {
                    self.stream_text.push_str(&text);
                    self.partial_text = Some(self.stream_text.clone());
                    self.latest_status = None;
                    return self.render_assistant_text();
                }
                self.latest_status = render_claude_stream_event_status(event_type, event_value);
                None
            }
            "content_block_delta" => {
                let text = event
                    .get("delta")
                    .and_then(extract_stream_delta_text)
                    .or_else(|| {
                        event
                            .get("text")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })?;
                self.stream_text.push_str(&text);
                self.partial_text = Some(self.stream_text.clone());
                self.latest_status = None;
                self.render_assistant_text()
            }
            "content_block_stop" | "message_stop" => {
                let text = self.stream_text.trim().to_string();
                if !text.is_empty() && self.assistant_blocks.last() != Some(&text) {
                    self.display_blocks.push(text.clone());
                    self.assistant_blocks.push(text);
                }
                self.partial_text = None;
                self.stream_text.clear();
                self.latest_status = None;
                self.render_assistant_text()
            }
            "message_start" | "message_delta" => {
                self.latest_status = render_claude_stream_event_status(event_type, event_value);
                None
            }
            _ => {
                self.latest_status = render_claude_stream_event_status(event_type, event_value)
                    .or_else(|| Some(format!("[debug:claude:event] unhandled type={event_type}")));
                None
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
        let text = self.assistant_blocks.join("\n\n---\n\n");
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
        let text = blocks.join("\n\n---\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

fn render_claude_stream_event_status(event_type: &str, event: &Value) -> Option<String> {
    match event_type {
        "message_start" => {
            let model = event
                .pointer("/message/model")
                .or_else(|| event.get("model"))
                .and_then(Value::as_str)
                .map(|text| truncate_tool_text(text, 80));
            Some(match model {
                Some(model) if !model.is_empty() => format!("[claude] message started: {model}"),
                _ => "[claude] message started".to_string(),
            })
        }
        "message_delta" => {
            let stop_reason = event
                .pointer("/delta/stop_reason")
                .or_else(|| event.get("stop_reason"))
                .and_then(Value::as_str)
                .map(|text| truncate_tool_text(text, 80));
            Some(match stop_reason {
                Some(reason) if !reason.is_empty() => format!("[claude] stop reason: {reason}"),
                _ => "[claude] message updated".to_string(),
            })
        }
        "content_block_start" => render_claude_content_block_status(
            event
                .get("content_block")
                .or_else(|| event.get("block"))
                .unwrap_or(&Value::Null),
            "started",
        ),
        "content_block_delta" => render_claude_delta_status(event),
        "content_block_stop" => Some("[claude] content block completed".to_string()),
        "message_stop" => Some("[claude] message completed".to_string()),
        "ping" => Some("[claude] ping".to_string()),
        "error" => render_claude_error_status(event),
        _ => None,
    }
}

fn render_claude_content_block_status(block: &Value, phase: &str) -> Option<String> {
    let block_type = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("content");
    match block_type {
        "tool_use" => {
            let tool = block.get("name").and_then(Value::as_str).unwrap_or("tool");
            let command = block
                .get("input")
                .and_then(extract_command_like_text)
                .map(|text| truncate_tool_text(&text, 120));
            Some(match command.filter(|text| !text.is_empty()) {
                Some(command) => format!("[claude:{tool}] {phase}: {command}"),
                None => format!("[claude:{tool}] {phase}"),
            })
        }
        "thinking" | "redacted_thinking" => Some(format!("[claude] thinking {phase}")),
        "text" => None,
        other => Some(format!("[claude:{other}] {phase}")),
    }
}

fn render_claude_delta_status(event: &Value) -> Option<String> {
    let delta = event.get("delta").unwrap_or(&Value::Null);
    let delta_type = delta.get("type").and_then(Value::as_str).unwrap_or("delta");
    match delta_type {
        "text_delta" => None,
        "thinking_delta" => delta
            .get("thinking")
            .or_else(|| delta.get("text"))
            .and_then(Value::as_str)
            .map(|text| truncate_tool_text(text, 120))
            .filter(|text| !text.is_empty())
            .map(|text| format!("[claude:thinking] {text}")),
        "signature_delta" => Some("[claude:thinking] signature updated".to_string()),
        "input_json_delta" => delta
            .get("partial_json")
            .and_then(Value::as_str)
            .map(|text| truncate_tool_text(text, 120))
            .filter(|text| !text.is_empty())
            .map(|text| format!("[claude:tool-input] {text}")),
        other => Some(format!("[claude:{other}] delta")),
    }
}

fn render_claude_error_status(event: &Value) -> Option<String> {
    let text = event
        .pointer("/error/message")
        .or_else(|| event.pointer("/error/type"))
        .or_else(|| event.get("message"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 160))
        .filter(|text| !text.is_empty())?;
    Some(format!("[claude:error] {text}"))
}

#[cfg(test)]
#[derive(Default)]
struct OpenCodeStreamingState {
    session_id: Option<String>,
    assistant_text: String,
    latest_status: Option<String>,
    finished: bool,
}

#[cfg(test)]
enum OpenCodeStreamEvent {
    None,
    Content(String),
    Status(String),
    Session(String),
    Error(String),
}

#[cfg(test)]
impl OpenCodeStreamingState {
    fn ingest_line(&mut self, line: &str) -> Result<OpenCodeStreamEvent> {
        let value = serde_json::from_str::<Value>(line)
            .with_context(|| "opencode produced invalid JSON")?;
        let Some(map) = value.as_object() else {
            return Ok(OpenCodeStreamEvent::None);
        };

        if self.session_id.is_none()
            && let Some(session_id) = value.get("sessionID").and_then(Value::as_str)
        {
            self.session_id = Some(session_id.to_string());
            return Ok(OpenCodeStreamEvent::Session(session_id.to_string()));
        }

        match map.get("type").and_then(Value::as_str).unwrap_or_default() {
            "text" => {
                let text = value
                    .pointer("/part/text")
                    .or_else(|| value.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if text.is_empty() {
                    return Ok(OpenCodeStreamEvent::None);
                }
                self.assistant_text.push_str(text);
                Ok(OpenCodeStreamEvent::Content(self.assistant_text.clone()))
            }
            "step_start" | "step-start" => Ok(OpenCodeStreamEvent::Status(
                "[opencode] step started".to_string(),
            )),
            "step_finish" | "step-finish" => {
                self.finished = true;
                self.latest_status = None;
                Ok(OpenCodeStreamEvent::None)
            }
            "tool_use" | "tool-use" => render_opencode_tool_use_status(&value)
                .map(OpenCodeStreamEvent::Status)
                .map(Ok)
                .unwrap_or(Ok(OpenCodeStreamEvent::None)),
            "error" => Ok(OpenCodeStreamEvent::Error(render_opencode_error(&value))),
            "session" => value
                .get("id")
                .or_else(|| value.get("sessionID"))
                .and_then(Value::as_str)
                .map(|session_id| {
                    self.session_id = Some(session_id.to_string());
                    OpenCodeStreamEvent::Session(session_id.to_string())
                })
                .map(Ok)
                .unwrap_or(Ok(OpenCodeStreamEvent::None)),
            other if other.is_empty() => Ok(OpenCodeStreamEvent::None),
            other => Ok(OpenCodeStreamEvent::Status(format!(
                "[debug:opencode:type] unhandled type={other}"
            ))),
        }
    }

    fn finish_text(&self) -> Option<String> {
        let text = self.assistant_text.trim();
        if text.is_empty() {
            None
        } else {
            Some(text.to_string())
        }
    }
}

#[cfg(test)]
fn render_opencode_tool_use_status(value: &Value) -> Option<String> {
    let part = value.get("part").unwrap_or(value);
    let mut normalized = part.clone();
    if normalized.get("type").is_none() {
        normalized["type"] = Value::String("tool".to_string());
    }
    if normalized.pointer("/state/status").is_none() {
        let status = part
            .get("status")
            .or_else(|| value.get("status"))
            .cloned()
            .unwrap_or_else(|| Value::String("running".to_string()));
        normalized["state"]["status"] = status;
    }
    if normalized.pointer("/state/input").is_none() {
        if let Some(input) = part.get("input").or_else(|| part.get("args")) {
            normalized["state"]["input"] = input.clone();
        }
    }
    if normalized.get("tool").is_none() {
        if let Some(tool) = part.get("name").or_else(|| value.get("tool")) {
            normalized["tool"] = tool.clone();
        }
    }
    render_opencode_part_status(&normalized)
}

fn render_opencode_error(value: &Value) -> String {
    value
        .pointer("/error/data/message")
        .or_else(|| value.pointer("/error/message"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("opencode run failed")
        .to_string()
}

fn acp_extract_session_ref(value: &Value) -> Option<String> {
    value
        .pointer("/session/id")
        .or_else(|| value.pointer("/session_id"))
        .or_else(|| value.pointer("/thread/id"))
        .or_else(|| value.pointer("/thread_id"))
        .or_else(|| value.pointer("/conversation/id"))
        .or_else(|| value.pointer("/conversation_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn acp_extract_text(value: &Value) -> Option<String> {
    value
        .pointer("/delta/text")
        .or_else(|| value.pointer("/message/delta"))
        .or_else(|| value.pointer("/message/text"))
        .or_else(|| value.pointer("/text"))
        .or_else(|| value.pointer("/output_text"))
        .or_else(|| value.pointer("/content"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn acp_extract_status(value: &Value) -> Option<String> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)?;
    let summary = value
        .pointer("/status")
        .or_else(|| value.pointer("/state"))
        .or_else(|| value.pointer("/message"))
        .and_then(extract_text_from_json)
        .unwrap_or_default();
    Some(if summary.is_empty() {
        format!("[acp] {event_type}")
    } else {
        format!("[acp:{event_type}] {}", truncate_tool_text(&summary, 120))
    })
}

fn acp_event_is_done(value: &Value) -> bool {
    matches!(
        value.get("type").and_then(Value::as_str),
        Some("done" | "turn.completed" | "response.completed" | "session.idle")
    ) || matches!(
        value.get("event").and_then(Value::as_str),
        Some("done" | "turn.completed" | "response.completed" | "session.idle")
    )
}

fn acp_event_is_error(value: &Value) -> bool {
    matches!(
        value.get("type").and_then(Value::as_str),
        Some("error" | "turn.failed" | "response.failed")
    ) || value.get("error").is_some()
}

fn acp_extract_error(value: &Value) -> String {
    value
        .pointer("/error/message")
        .or_else(|| value.pointer("/message"))
        .or_else(|| value.pointer("/error"))
        .and_then(extract_text_from_json)
        .unwrap_or_else(|| "ACP run failed".to_string())
}

fn acp_event_to_approval(value: &Value) -> Option<ApprovalRequest> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)?;
    if !event_type.contains("approval") && !event_type.contains("permission") {
        return None;
    }
    let request_id = value
        .pointer("/approval/id")
        .or_else(|| value.pointer("/request_id"))
        .or_else(|| value.pointer("/id"))
        .and_then(Value::as_str)
        .unwrap_or("acp-approval")
        .to_string();
    let command = value
        .pointer("/approval/command")
        .or_else(|| value.pointer("/command"))
        .or_else(|| value.pointer("/tool/input/command"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    Some(ApprovalRequest {
        request_id,
        kind: if command.is_some() {
            ApprovalKind::ExecCommand
        } else {
            ApprovalKind::Permissions
        },
        command,
        reason: Some("ACP 请求执行审批".to_string()),
        allow_accept_for_session: true,
        allow_cancel: true,
        resolvable: true,
    })
}

fn extract_kiro_acp_message_chunk(params: &Value) -> Option<String> {
    if let Some("agent_message_chunk") = params.get("sessionUpdate").and_then(Value::as_str) {
        return params
            .pointer("/content/text")
            .or_else(|| params.pointer("/delta/text"))
            .or_else(|| params.pointer("/text"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
    }
    let notification_type = params
        .get("type")
        .or_else(|| params.get("notification"))
        .and_then(Value::as_str)?;
    if notification_type != "AgentMessageChunk" {
        return None;
    }
    params
        .pointer("/chunk/text")
        .or_else(|| params.pointer("/text"))
        .or_else(|| params.pointer("/delta"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn kiro_acp_turn_end(params: &Value) -> bool {
    if matches!(
        params.get("sessionUpdate").and_then(Value::as_str),
        Some("completed")
            | Some("turn_completed")
            | Some("end_turn")
            | Some("finished")
            | Some("error")
    ) {
        return true;
    }
    matches!(
        params
            .get("type")
            .or_else(|| params.get("notification"))
            .and_then(Value::as_str),
        Some("TurnEnd")
    )
}

fn render_kiro_acp_notification_status(params: &Value) -> Option<String> {
    if let Some(update_kind) = params.get("sessionUpdate").and_then(Value::as_str) {
        return match update_kind {
            "tool_call" | "tool_call_chunk" => {
                let tool = params
                    .pointer("/title")
                    .or_else(|| params.pointer("/kind"))
                    .or_else(|| params.pointer("/tool/name"))
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                Some(format!("[kiro:{}] started", truncate_tool_text(tool, 80)))
            }
            "completed" | "turn_completed" | "end_turn" | "finished" => {
                Some("[kiro] turn completed".to_string())
            }
            "error" => {
                let status = params
                    .pointer("/message")
                    .and_then(extract_text_from_json)
                    .unwrap_or_else(|| "failed".to_string());
                Some(format!("[kiro] turn {}", truncate_tool_text(&status, 80)))
            }
            _ => None,
        };
    }
    match params
        .get("type")
        .or_else(|| params.get("notification"))
        .and_then(Value::as_str)?
    {
        "ToolCall" => {
            let tool = params
                .pointer("/toolCall/toolName")
                .or_else(|| params.pointer("/tool/name"))
                .or_else(|| params.pointer("/name"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            Some(format!("[kiro:{tool}] started"))
        }
        "ToolCallUpdate" => {
            let tool = params
                .pointer("/toolCall/toolName")
                .or_else(|| params.pointer("/tool/name"))
                .or_else(|| params.pointer("/name"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let status = params
                .pointer("/status")
                .or_else(|| params.pointer("/message"))
                .and_then(extract_text_from_json)
                .unwrap_or_else(|| "updated".to_string());
            Some(format!(
                "[kiro:{tool}] {}",
                truncate_tool_text(&status, 120)
            ))
        }
        "TurnEnd" => {
            let status = params
                .pointer("/result")
                .or_else(|| params.pointer("/status"))
                .and_then(extract_text_from_json)
                .unwrap_or_else(|| "completed".to_string());
            Some(format!("[kiro] turn {}", truncate_tool_text(&status, 80)))
        }
        _ => None,
    }
}

fn kiro_acp_permission_request_to_approval(request_id: &str, params: &Value) -> ApprovalRequest {
    let tool_name = params
        .pointer("/toolCall/title")
        .or_else(|| params.pointer("/toolCall/name"))
        .or_else(|| params.pointer("/tool/title"))
        .or_else(|| params.pointer("/tool/name"))
        .or_else(|| params.pointer("/toolCall/toolName"))
        .and_then(Value::as_str)
        .unwrap_or("tool")
        .to_string();
    let command = params
        .pointer("/toolCall/arguments/command")
        .or_else(|| params.pointer("/toolCall/input/command"))
        .or_else(|| params.pointer("/tool/input/command"))
        .or_else(|| params.pointer("/arguments/command"))
        .and_then(Value::as_str)
        .map(ToString::to_string);

    ApprovalRequest {
        request_id: request_id.to_string(),
        kind: if command.is_some() {
            ApprovalKind::ExecCommand
        } else {
            ApprovalKind::Permissions
        },
        command,
        reason: Some(format!("Kiro 请求执行 {tool_name}")),
        allow_accept_for_session: true,
        allow_cancel: true,
        resolvable: true,
    }
}

fn kiro_acp_permission_result_json(choice: &ApprovalChoice) -> Value {
    match choice {
        ApprovalChoice::Accept => serde_json::json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "allow_once",
            },
        }),
        ApprovalChoice::AlwaysAllow | ApprovalChoice::AcceptForSession => serde_json::json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "allow_always",
            },
        }),
        ApprovalChoice::Decline => serde_json::json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "reject_once",
            },
        }),
        ApprovalChoice::Cancel => serde_json::json!({
            "outcome": {
                "outcome": "cancelled",
            },
        }),
    }
}

fn add_kiro_model_id_fields(params: &mut Value, model: Option<&str>) {
    let Some(model) = model.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    let Some(object) = params.as_object_mut() else {
        return;
    };
    object.insert("model".to_string(), Value::String(model.to_string()));
    object.insert("modelId".to_string(), Value::String(model.to_string()));
}

fn acp_json_rpc_handshake_timeout() -> Duration {
    #[cfg(test)]
    {
        let override_ms = ACP_JSON_RPC_HANDSHAKE_TIMEOUT_TEST_MS.load(Ordering::SeqCst);
        if override_ms > 0 {
            return Duration::from_millis(override_ms);
        }
    }
    ACP_JSON_RPC_HANDSHAKE_TIMEOUT
}

fn parse_sse_json_event(line: &str) -> Option<Value> {
    line.strip_prefix("data:")
        .map(str::trim)
        .filter(|data| !data.is_empty() && *data != "[DONE]")
        .and_then(|data| serde_json::from_str::<Value>(data).ok())
}

fn opencode_session_summary(value: &Value) -> Option<SessionSummary> {
    let id = value.get("id").and_then(Value::as_str)?.to_string();
    let directory = value.get("directory").and_then(Value::as_str)?;
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or("opencode session")
        .to_string();
    Some(SessionSummary {
        id,
        project_id: crate::session_store::project_id_for_path(directory),
        title: title.clone(),
        agent: AgentKind::OpenCode,
        brief_reply_mode: false,
        status: crate::models::SessionStatus::Idle,
        updated_at: opencode_time(value.pointer("/time/updated")),
        unread_count: 0,
        last_message_preview: Some(title),
        pending_approval: None,
        provider_id: None,
        reasoning_effort: None,
    })
}

fn opencode_messages_from_value(session_id: &str, value: &Value) -> Vec<ChatMessage> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let info = entry.get("info")?;
            let role = match info.get("role").and_then(Value::as_str)? {
                "user" => crate::models::MessageRole::User,
                "assistant" => crate::models::MessageRole::Assistant,
                _ => return None,
            };
            let content = opencode_parts_text(entry.get("parts").unwrap_or(&Value::Null));
            if content.trim().is_empty() {
                return None;
            }
            Some(ChatMessage {
                id: info
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                session_id: session_id.to_string(),
                role,
                content,
                created_at: opencode_time(info.pointer("/time/created")),
            })
        })
        .collect()
}

fn opencode_parts_text(parts: &Value) -> String {
    parts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| match part.get("type").and_then(Value::as_str) {
            Some("text") => part.get("text").and_then(Value::as_str),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
        .trim()
        .to_string()
}

fn opencode_time(value: Option<&Value>) -> chrono::DateTime<chrono::Utc> {
    let millis = value.and_then(Value::as_i64).unwrap_or(0);
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis).unwrap_or_else(chrono::Utc::now)
}

fn project_name_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

fn select_claude_runtime_ref(
    session_id: &str,
    existing_runtime_ref: Option<&str>,
    can_resume_existing_session: bool,
) -> String {
    if can_resume_existing_session {
        existing_runtime_ref
            .map(ToString::to_string)
            .unwrap_or_else(|| session_id.to_string())
    } else if existing_runtime_ref.is_some() {
        existing_runtime_ref
            .map(ToString::to_string)
            .unwrap_or_else(|| session_id.to_string())
    } else {
        session_id.to_string()
    }
}

fn opencode_permission_to_approval(value: &Value) -> ApprovalRequest {
    let request_id = value
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let permission = value
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or("permission");
    let patterns = value
        .get("patterns")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|text| !text.is_empty());
    let metadata_command = value
        .pointer("/metadata/command")
        .or_else(|| value.pointer("/metadata/description"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let command = metadata_command.or(patterns);
    ApprovalRequest {
        request_id,
        kind: if command.is_some() {
            ApprovalKind::ExecCommand
        } else {
            ApprovalKind::Permissions
        },
        command,
        reason: Some(format!("OpenCode 请求 {permission} 权限")),
        allow_accept_for_session: true,
        allow_cancel: true,
        resolvable: true,
    }
}

async fn auto_approve_opencode_request(
    request: &ApprovalRequest,
    project_root: &Path,
) -> Result<Option<ApprovalChoice>> {
    match auto_approve_codex_request(request, project_root).await? {
        Some(ApprovalChoice::Accept) => Ok(Some(ApprovalChoice::Accept)),
        Some(other) => Ok(Some(other)),
        None => Ok(None),
    }
}

fn render_opencode_part_status(part: &Value) -> Option<String> {
    match part.get("type").and_then(Value::as_str)? {
        "tool" => {
            let tool = part.get("tool").and_then(Value::as_str).unwrap_or("tool");
            let status = part
                .pointer("/state/status")
                .and_then(Value::as_str)
                .unwrap_or("running");
            let command = part
                .pointer("/state/input/command")
                .or_else(|| part.pointer("/state/title"))
                .and_then(Value::as_str)
                .map(|text| truncate_tool_text(text, 120));
            Some(match command.filter(|text| !text.is_empty()) {
                Some(command) => format!("[opencode:{tool}:{status}] {command}"),
                None => format!("[opencode:{tool}:{status}]"),
            })
        }
        "step-start" => Some("[opencode] step started".to_string()),
        "step-finish" => Some("[opencode] step finished".to_string()),
        "patch" => render_opencode_patch_status(part),
        _ => None,
    }
}

fn render_opencode_event_status(event_type: &str, properties: &Value) -> Option<String> {
    match event_type {
        "message.part.updated" => {
            render_opencode_part_status(properties.get("part").unwrap_or(&Value::Null))
        }
        "message.updated" => render_opencode_message_status(properties),
        "message.removed" => Some("[opencode] message removed".to_string()),
        "message.part.delta" => None,
        "permission.asked" => render_opencode_permission_status(properties, "asked"),
        "permission.replied" => render_opencode_permission_status(properties, "replied"),
        "session.status" => {
            let status = properties
                .pointer("/status/type")
                .or_else(|| properties.get("status"))
                .and_then(extract_text_from_json)
                .map(|text| truncate_tool_text(&text, 80))
                .filter(|text| !text.is_empty())?;
            (status != "idle").then(|| format!("[opencode] {status}"))
        }
        "session.updated" => Some("[opencode] session updated".to_string()),
        "session.deleted" => Some("[opencode] session deleted".to_string()),
        "session.error" => Some(format!(
            "[opencode:error] {}",
            render_opencode_error(properties)
        )),
        "session.idle" => None,
        "storage.write" => properties
            .get("key")
            .and_then(Value::as_str)
            .map(|key| format!("[opencode:storage] write {}", truncate_tool_text(key, 80)))
            .or_else(|| Some("[opencode:storage] write".to_string())),
        "file.edited" | "file.watcher.updated" => {
            render_opencode_file_event_status(event_type, properties)
        }
        other if other.is_empty() => None,
        other => Some(format!("[debug:opencode:event] unhandled type={other}")),
    }
}

fn render_opencode_message_status(properties: &Value) -> Option<String> {
    let role = properties
        .pointer("/info/role")
        .or_else(|| properties.pointer("/message/info/role"))
        .or_else(|| properties.get("role"))
        .and_then(Value::as_str)
        .unwrap_or("message");
    let status = properties
        .pointer("/info/status")
        .or_else(|| properties.pointer("/message/info/status"))
        .or_else(|| properties.get("status"))
        .and_then(extract_text_from_json)
        .map(|text| truncate_tool_text(&text, 80));
    Some(match status {
        Some(status) if !status.is_empty() => format!("[opencode:{role}] {status}"),
        _ => format!("[opencode:{role}] updated"),
    })
}

fn render_opencode_permission_status(properties: &Value, phase: &str) -> Option<String> {
    let permission = properties
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or("permission");
    let command = properties
        .pointer("/metadata/command")
        .or_else(|| properties.pointer("/metadata/description"))
        .or_else(|| properties.get("patterns"))
        .and_then(extract_text_from_json)
        .map(|text| truncate_tool_text(&text, 120));
    Some(match command.filter(|text| !text.is_empty()) {
        Some(command) => format!("[opencode:permission:{phase}] {permission}: {command}"),
        None => format!("[opencode:permission:{phase}] {permission}"),
    })
}

fn render_opencode_file_event_status(event_type: &str, properties: &Value) -> Option<String> {
    let path = properties
        .get("path")
        .or_else(|| properties.get("file"))
        .and_then(Value::as_str)
        .map(|text| truncate_tool_text(text, 120));
    let label = match event_type {
        "file.edited" => "[opencode:file] edited",
        "file.watcher.updated" => "[opencode:file] watcher updated",
        _ => "[opencode:file] updated",
    };
    Some(match path {
        Some(path) if !path.is_empty() => format!("{label}: {path}"),
        _ => label.to_string(),
    })
}

fn render_opencode_patch_status(part: &Value) -> Option<String> {
    let files = part.get("files").and_then(Value::as_array)?;
    let paths = files
        .iter()
        .filter_map(Value::as_str)
        .take(3)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        None
    } else {
        Some(format!("[opencode:patch] {}", paths.join(", ")))
    }
}

async fn wait_for_opencode_session_idle(
    client: &mut OpenCodeHttpClient,
    session_id: &str,
    project_root: &Path,
) -> Result<()> {
    let mut stream = client.event_stream(project_root).await?;
    while let Some(line) = stream.recv().await {
        let line = line?;
        let Some(event) = parse_sse_json_event(&line) else {
            continue;
        };
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let properties = event.get("properties").unwrap_or(&Value::Null);
        let event_session_id = properties
            .get("sessionID")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !event_session_id.is_empty() && event_session_id != session_id {
            continue;
        }
        match event_type {
            "session.idle" => return Ok(()),
            "session.error" => bail!("{}", render_opencode_error(properties)),
            _ => {}
        }
    }
    bail!("opencode event stream closed before session became idle")
}

fn url_path_escape(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            other => format!("%{other:02X}").chars().collect::<Vec<_>>(),
        })
        .collect()
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

fn extract_command_like_text(value: &Value) -> Option<String> {
    value
        .get("command")
        .or_else(|| value.get("cmd"))
        .or_else(|| value.get("query"))
        .or_else(|| value.get("url"))
        .or_else(|| value.get("file_path"))
        .or_else(|| value.get("path"))
        .or_else(|| value.get("description"))
        .or_else(|| value.get("pattern"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
}

fn extract_stream_delta_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(extract_stream_delta_text)
                .collect::<Vec<_>>()
                .join("");
            Some(text)
        }
        Value::Object(map) => map
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| map.get("delta").and_then(extract_stream_delta_text))
            .or_else(|| map.get("content").and_then(extract_stream_delta_text)),
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
        if body.is_empty() {
            continue;
        }
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
        if body.is_empty() {
            let _ = tokio::fs::remove_file(&path).await;
            continue;
        }
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
        ApprovalKind::FileChange | ApprovalKind::ApplyPatch | ApprovalKind::Permissions => Ok(None),
    }
}

fn approval_result_json(choice: &ApprovalChoice, kind: &ApprovalKind) -> Value {
    match kind {
        ApprovalKind::CommandExecution | ApprovalKind::FileChange => serde_json::json!({
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
        ApprovalKind::ApplyPatch => serde_json::json!({
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

fn is_macos() -> bool {
    cfg!(target_os = "macos")
}

fn is_windows() -> bool {
    cfg!(target_os = "windows")
}

async fn command_exists(name: &str) -> bool {
    tokio::process::Command::new(if cfg!(target_os = "windows") {
        "where"
    } else {
        "which"
    })
    .arg(name)
    .output()
    .await
    .map(|o| o.status.success())
    .unwrap_or(false)
}

pub async fn agent_readiness_message(kind: AgentKind) -> Option<String> {
    match kind {
        AgentKind::Acp => acp_readiness_message().await,
        _ => None,
    }
}

async fn acp_readiness_message() -> Option<String> {
    let kiro_path = find_executable_in_path("kiro-cli")?;
    let cache = acp_readiness_cache();
    {
        let cached = cache.lock().await;
        if let Some(cached) = cached.as_ref()
            && cached.command_path == kiro_path
            && cached.checked_at.elapsed() < AGENT_READINESS_CACHE_TTL
        {
            return cached.message.clone();
        }
    }

    let message = acp_readiness_message_for_command(&kiro_path).await;
    let mut cached = cache.lock().await;
    *cached = Some(CachedAgentReadiness {
        checked_at: Instant::now(),
        command_path: kiro_path,
        message: message.clone(),
    });
    message
}

fn acp_readiness_cache() -> &'static tokio::sync::Mutex<Option<CachedAgentReadiness>> {
    static CACHE: OnceLock<tokio::sync::Mutex<Option<CachedAgentReadiness>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(None))
}

async fn acp_readiness_message_for_command(command_path: &Path) -> Option<String> {
    acp_readiness_message_for_command_with_args(command_path, &["whoami", "--format", "plain"])
        .await
}

async fn acp_readiness_message_for_command_with_args(
    command_path: &Path,
    args: &[&str],
) -> Option<String> {
    let output = match tokio::time::timeout(
        ACP_READINESS_TIMEOUT,
        tokio::process::Command::new(command_path)
            .args(args)
            .output(),
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return Some(format!(
                "Kiro ACP is installed at {} but readiness check failed: {}",
                command_path.display(),
                error
            ));
        }
        Err(_) => {
            return Some(format!(
                "Kiro ACP is installed at {} but readiness check timed out after {}s while running `{}`.",
                command_path.display(),
                ACP_READINESS_TIMEOUT.as_secs(),
                std::iter::once(command_path.display().to_string())
                    .chain(args.iter().map(|value| value.to_string()))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
    };
    if output.status.success() {
        return None;
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("exit status {}", output.status)
    };
    let mut message = format!(
        "Kiro ACP is installed at {} but is not ready: {}",
        command_path.display(),
        detail
    );
    let detail_lower = detail.to_ascii_lowercase();
    if detail_lower.contains("not logged in") || detail_lower.contains("please log in") {
        message.push_str(". Run `kiro-cli login` if authentication is required.");
    }
    Some(message)
}

struct CachedAgentReadiness {
    checked_at: Instant,
    command_path: PathBuf,
    message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AcpAgentStatus {
    pub installed: bool,
    pub installed_path: Option<String>,
    pub readiness_message: Option<String>,
    pub diagnostic: Option<AcpAgentDiagnostic>,
}

pub async fn summarize_acp_agent(settings: &BridgeSettings) -> AcpAgentStatus {
    summarize_acp_agent_for_provider(settings, None).await
}

pub async fn probe_acp_handshake_for_provider(
    settings: &BridgeSettings,
    provider_id: Option<&str>,
) -> Option<AcpHandshakeProbe> {
    let provider_id = provider_id.map(str::trim).filter(|value| !value.is_empty());
    let server = if let Some(provider_id) = provider_id {
        settings
            .acp_servers
            .iter()
            .find(|server| server.id == provider_id)
    } else {
        settings
            .acp_servers
            .iter()
            .filter(|server| server.enabled)
            .min_by_key(|server| server.priority)
    }?;

    match server.profile {
        AcpProfile::Kiro => Some(probe_kiro_acp_handshake(server).await),
        AcpProfile::GenericHttp => Some(probe_generic_http_acp_connectivity(server).await),
    }
}

pub async fn summarize_acp_agent_for_provider(
    settings: &BridgeSettings,
    provider_id: Option<&str>,
) -> AcpAgentStatus {
    let enabled_servers = settings
        .acp_servers
        .iter()
        .filter(|server| server.enabled)
        .collect::<Vec<_>>();
    let enabled_server_count = enabled_servers.len();
    let server =
        if let Some(provider_id) = provider_id.map(str::trim).filter(|value| !value.is_empty()) {
            settings
                .acp_servers
                .iter()
                .find(|server| server.id == provider_id)
        } else {
            enabled_servers
                .into_iter()
                .min_by_key(|server| server.priority)
        };
    let Some(server) = server else {
        if let Some(provider_id) = provider_id.map(str::trim).filter(|value| !value.is_empty()) {
            return AcpAgentStatus {
                installed: false,
                installed_path: None,
                readiness_message: Some(format!(
                    "ACP agent is not ready: configured ACP server `{provider_id}` was not found in settings."
                )),
                diagnostic: None,
            };
        }
        return AcpAgentStatus {
            installed: false,
            installed_path: None,
            readiness_message: Some(
                "ACP agent is not ready: no enabled ACP servers are configured in settings."
                    .to_string(),
            ),
            diagnostic: None,
        };
    };
    let diagnostic = AcpAgentDiagnostic {
        configured_server_id: server.id.clone(),
        configured_server_name: server.name.clone(),
        enabled: server.enabled,
        profile: server.profile,
        auth_configured: !server.auth_token.trim().is_empty(),
        command: server.command.clone(),
        args: server.args.clone(),
        endpoint: server.endpoint.clone(),
        default_model: server.default_model.clone(),
        header_count: server.headers.len(),
        env_count: server.env.len(),
        turn_url_candidates: server
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(acp_http_turn_url_templates)
            .unwrap_or_default(),
        approval_reply_url_templates: server
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(acp_http_approval_reply_url_templates)
            .unwrap_or_default(),
        cancel_url_templates: server
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(acp_http_cancel_url_templates)
            .unwrap_or_default(),
        enabled_server_count,
    };

    match server.profile {
        AcpProfile::Kiro => {
            let command = server.command.as_deref().unwrap_or("kiro-cli");
            let installed_path = resolve_command_path(command);
            let readiness_message = match installed_path.as_ref() {
                Some(path) => acp_readiness_message_for_command(path).await,
                None => Some(format!(
                    "ACP server `{}` is configured for Kiro but command `{}` was not found in PATH.",
                    server.id, command
                )),
            };
            let readiness_message = if !server.enabled {
                Some(format!(
                    "ACP server `{}` is configured but disabled in settings.",
                    server.id
                ))
                .or(readiness_message)
            } else {
                readiness_message
            };
            AcpAgentStatus {
                installed: installed_path.is_some(),
                installed_path: installed_path.map(|path| path.display().to_string()),
                readiness_message,
                diagnostic: Some(diagnostic),
            }
        }
        AcpProfile::GenericHttp => {
            let installed = server
                .endpoint
                .as_deref()
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .is_some();
            let readiness_message = if !server.enabled {
                Some(format!(
                    "ACP server `{}` is configured but disabled in settings.",
                    server.id
                ))
            } else {
                None
            };
            AcpAgentStatus {
                installed,
                installed_path: None,
                readiness_message,
                diagnostic: Some(diagnostic),
            }
        }
    }
}

fn resolve_command_path(command: &str) -> Option<PathBuf> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = Path::new(trimmed);
    if path.components().count() > 1 || path.is_absolute() {
        if path.exists() {
            return Some(path.to_path_buf());
        }
        return None;
    }
    find_executable_in_path(trimmed)
}

async fn probe_kiro_acp_handshake(server: &crate::models::AcpServerConfig) -> AcpHandshakeProbe {
    let command_name = server
        .command
        .clone()
        .unwrap_or_else(|| "kiro-cli".to_string());
    let args = if server.args.is_empty() {
        vec!["acp".to_string()]
    } else {
        server.args.clone()
    };

    let mut command = Command::new(&command_name);
    command
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for entry in &server.env {
        command.env(&entry.key, &entry.value);
    }
    if !server.auth_token.is_empty() {
        command.env("KIRO_API_KEY", &server.auth_token);
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return AcpHandshakeProbe {
                attempted: true,
                success: false,
                mode: "kiro_stdio_handshake".to_string(),
                stage: Some("spawn".to_string()),
                message: Some(format!(
                    "Failed to spawn Kiro ACP runtime `{command_name}`: {error}"
                )),
            };
        }
    };

    let result = async {
        let mut stdin = child
            .stdin
            .take()
            .context("ACP runtime did not expose stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("ACP runtime did not expose stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("ACP runtime did not expose stderr")?;

        let stderr_buffer = Arc::new(tokio::sync::Mutex::new(String::new()));
        let stderr_buffer_task = Arc::clone(&stderr_buffer);
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            let mut output = String::new();
            while let Some(line) = reader.next_line().await? {
                let line = strip_ansi(&line);
                if !output.is_empty() {
                    output.push('\n');
                }
                output.push_str(&line);
                let mut shared = stderr_buffer_task.lock().await;
                if !shared.is_empty() {
                    shared.push('\n');
                }
                shared.push_str(&line);
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
        let init_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "initialize",
        serde_json::json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": {
                    "readTextFile": true,
                    "writeTextFile": true
                },
                "terminal": true,
                "secretStorage": true
            },
            "clientInfo": {
                "name": "omni-code-bridge-diagnostic",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
        )
        .await?;
        wait_for_kiro_json_rpc_response(
            &mut child,
            &mut stdout_rx,
            &mut raw_stdout,
            &stderr_buffer,
            init_request_id,
            acp_json_rpc_handshake_timeout(),
            "initialize",
        )
        .await?;

        let session_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "session/new",
            serde_json::json!({
                "cwd": std::env::current_dir()?.display().to_string(),
                "mcpServers": [],
            }),
        )
        .await?;
        let response = wait_for_kiro_json_rpc_response(
            &mut child,
            &mut stdout_rx,
            &mut raw_stdout,
            &stderr_buffer,
            session_request_id,
            acp_json_rpc_handshake_timeout(),
            "session/new",
        )
        .await?;
        let session_id = response
            .get("sessionId")
            .or_else(|| response.pointer("/session/id"))
            .or_else(|| response.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        let mut prompt_params = serde_json::json!({
            "sessionId": session_id,
            "prompt": [{
                "type": "text",
                "text": "healthcheck",
            }],
            "input": [{
                "type": "text",
                "text": "healthcheck",
            }],
            "content": [{
                "type": "text",
                "text": "healthcheck",
            }],
        });
        add_kiro_model_id_fields(&mut prompt_params, server.default_model.as_deref());
        let prompt_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "session/prompt",
            prompt_params,
        )
        .await?;
        let mut probe_text = String::new();
        loop {
            let line = wait_for_stdout_line(
                &mut child,
                &mut stdout_rx,
                &stderr_buffer,
                acp_json_rpc_handshake_timeout(),
                "session/prompt",
            )
            .await?;
            raw_stdout.push_str(&line);
            raw_stdout.push('\n');

            let value = serde_json::from_str::<Value>(&line)
                .with_context(|| format!("Kiro ACP probe emitted invalid JSON: {line}"))?;
            if value.get("id") == Some(&Value::from(prompt_request_id)) {
                if let Some(error) = value.get("error") {
                    bail!(
                        "{}",
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("Kiro ACP probe prompt failed")
                    );
                }
                break;
            }

            if value.get("method").and_then(Value::as_str) == Some("session/notification") {
                let params = value.get("params").unwrap_or(&Value::Null);
                if let Some(text) = extract_kiro_acp_message_chunk(params) {
                    probe_text.push_str(&text);
                }
                if kiro_acp_turn_end(params) {
                    continue;
                }
            } else if value.get("method").and_then(Value::as_str) == Some("session/update") {
                let params = value
                    .pointer("/params/update")
                    .or_else(|| value.get("params"))
                    .unwrap_or(&Value::Null);
                if let Some(text) = extract_kiro_acp_message_chunk(params) {
                    probe_text.push_str(&text);
                }
                if kiro_acp_turn_end(params) {
                    continue;
                }
            } else if value.get("method").and_then(Value::as_str) == Some("session/request_permission") {
                let request_id = value
                    .get("id")
                    .and_then(jsonrpc_id_to_string)
                    .context("Kiro ACP probe permission request did not include id")?;
                send_json_rpc_response(
                    &mut stdin,
                    &request_id,
                    kiro_acp_permission_result_json(&ApprovalChoice::Accept),
                )
                .await?;
            }
        }

        drop(stdin);
        let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
        let _ = stderr_task.await;
        Ok::<_, anyhow::Error>((session_id.to_string(), probe_text))
    }
    .await;

    let _ = child.kill().await;

    match result {
        Ok((session_id, probe_text)) => AcpHandshakeProbe {
            attempted: true,
            success: true,
            mode: "kiro_stdio_handshake".to_string(),
            stage: Some("session/prompt".to_string()),
            message: Some(format!(
                "Kiro ACP handshake succeeded, created session `{session_id}`, and completed a probe turn{}.",
                if probe_text.trim().is_empty() {
                    String::new()
                } else {
                    format!(
                        " with text preview: {}",
                        truncate_tool_text(probe_text.trim(), 80)
                    )
                }
            )),
        },
        Err(error) => AcpHandshakeProbe {
            attempted: true,
            success: false,
            mode: "kiro_stdio_handshake".to_string(),
            stage: None,
            message: Some(error.to_string()),
        },
    }
}

async fn probe_generic_http_acp_connectivity(
    server: &crate::models::AcpServerConfig,
) -> AcpHandshakeProbe {
    let Some(endpoint) = server
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return AcpHandshakeProbe {
            attempted: false,
            success: false,
            mode: "generic_http_turn_create".to_string(),
            stage: Some("configuration".to_string()),
            message: Some(
                "Generic HTTP ACP probe requires a configured endpoint, but none was provided."
                    .to_string(),
            ),
        };
    };

    let extra_headers = server
        .headers
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect::<Vec<_>>();
    let headers = match build_acp_http_headers(&server.auth_token, &extra_headers) {
        Ok(headers) => headers,
        Err(error) => {
            return AcpHandshakeProbe {
                attempted: false,
                success: false,
                mode: "generic_http_turn_create".to_string(),
                stage: Some("configuration".to_string()),
                message: Some(format!(
                    "Generic HTTP ACP probe could not build headers: {error}"
                )),
            };
        }
    };

    let client = reqwest::Client::new();
    let cwd = std::env::current_dir()
        .map(|value| value.display().to_string())
        .unwrap_or_else(|_| ".".to_string());
    let body = serde_json::json!({
        "session_id": "acp-probe-session",
        "thread_id": "acp-probe-session",
        "conversation_id": "acp-probe-session",
        "cwd": cwd,
        "project_root": std::env::current_dir()
            .map(|value| value.display().to_string())
            .unwrap_or_else(|_| ".".to_string()),
        "input": "healthcheck",
        "message": "healthcheck",
        "prompt": "healthcheck",
    });
    let mut failures = Vec::new();

    for url in acp_http_candidate_urls(endpoint, "acp-probe-session") {
        let response = client
            .post(&url)
            .headers(headers.clone())
            .header(
                reqwest::header::ACCEPT,
                "text/event-stream, application/json",
            )
            .timeout(ACP_READINESS_TIMEOUT)
            .json(&body)
            .send()
            .await;

        match response {
            Ok(response) => {
                let status = response.status();
                let header_content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let compatible_content_type = header_content_type.contains("application/json")
                    || header_content_type.contains("text/event-stream")
                    || header_content_type.contains("application/problem+json");

                if status.is_success() {
                    let probe_result = read_acp_probe_response(response).await;
                    let (content_type, session_ref, text_hint, saw_done) = match probe_result {
                        Ok((content_type, session_ref, text_hint, saw_done)) => {
                            (content_type, session_ref, text_hint, saw_done)
                        }
                        Err(error) => {
                            failures.push(format!(
                                "successful HTTP {status} from `{url}` but probe response parsing failed: {error}"
                            ));
                            continue;
                        }
                    };
                    let extracted_session = session_ref.unwrap_or_else(|| "unknown".to_string());
                    let mut message = format!(
                        "Generic HTTP ACP turn endpoint `{url}` accepted a probe request (HTTP {status})"
                    );
                    if !content_type.is_empty() {
                        message.push_str(&format!(" with content-type `{content_type}`"));
                    }
                    if extracted_session != "unknown" {
                        message.push_str(&format!(" and returned session `{extracted_session}`"));
                    }
                    if let Some(text) = text_hint.filter(|text| !text.trim().is_empty()) {
                        message.push_str(&format!(
                            ", text preview: {}",
                            truncate_tool_text(&text, 80)
                        ));
                    }
                    if content_type.contains("text/event-stream") {
                        if saw_done {
                            message.push_str(", SSE stream completed normally");
                        } else {
                            message.push_str(
                                ", SSE stream was accepted but did not emit an explicit done event before closing",
                            );
                        }
                    }
                    return AcpHandshakeProbe {
                        attempted: true,
                        success: true,
                        mode: "generic_http_turn_create".to_string(),
                        stage: Some("turn_post".to_string()),
                        message: Some(message),
                    };
                }

                let body_text = response.text().await.unwrap_or_default();
                let trimmed_body = body_text.trim();
                let mut failure = format!("HTTP {status} from `{url}`");
                if !header_content_type.is_empty() {
                    failure.push_str(&format!(" content-type `{header_content_type}`"));
                }
                if !compatible_content_type {
                    failure.push_str(" (not ACP-like)");
                }
                if !trimmed_body.is_empty() {
                    failure.push_str(&format!(": {trimmed_body}"));
                }
                failures.push(failure);
            }
            Err(error) => failures.push(format!("request to `{url}` failed: {error}")),
        }
    }

    AcpHandshakeProbe {
        attempted: true,
        success: false,
        mode: "generic_http_turn_create".to_string(),
        stage: Some("turn_post".to_string()),
        message: Some(format!(
            "Generic HTTP ACP probe could not create a turn via any candidate endpoint for `{endpoint}`: {}",
            failures.join(" | ")
        )),
    }
}

async fn try_install_with_npm(
    agent: AgentKind,
    npm_package: &str,
    binary_name: &str,
) -> Option<crate::models::AgentInstallResult> {
    if !command_exists("npm").await {
        return None;
    }

    let result = tokio::process::Command::new("npm")
        .args(["install", "-g", npm_package])
        .output()
        .await;

    match result {
        Ok(output) if output.status.success() => {
            let path =
                find_executable_in_path(binary_name).unwrap_or_else(|| PathBuf::from(binary_name));
            Some(crate::models::AgentInstallResult {
                agent,
                success: true,
                message: Some(format!("installed successfully via npm ({npm_package})")),
                installed_path: Some(path.display().to_string()),
            })
        }
        _ => None,
    }
}

async fn try_install_with_brew(
    agent: AgentKind,
    brew_package: &str,
    binary_name: &str,
) -> Option<crate::models::AgentInstallResult> {
    if !is_macos() || !command_exists("brew").await {
        return None;
    }

    let result = tokio::process::Command::new("brew")
        .args(["install", brew_package])
        .output()
        .await;

    match result {
        Ok(output) if output.status.success() => {
            let path =
                find_executable_in_path(binary_name).unwrap_or_else(|| PathBuf::from(binary_name));
            Some(crate::models::AgentInstallResult {
                agent,
                success: true,
                message: Some(format!("installed successfully via brew ({brew_package})")),
                installed_path: Some(path.display().to_string()),
            })
        }
        _ => None,
    }
}

async fn try_install_with_script(
    agent: AgentKind,
    unix_script_url: &str,
    windows_script_url: Option<&str>,
    binary_name: &str,
) -> Option<crate::models::AgentInstallResult> {
    let result = if is_windows() {
        let Some(url) = windows_script_url else {
            return None;
        };
        tokio::process::Command::new("powershell")
            .args(["-Command", &format!("irm {url} | iex")])
            .output()
            .await
    } else {
        tokio::process::Command::new("sh")
            .args(["-c", &format!("curl -fsSL {unix_script_url} | sh")])
            .output()
            .await
    };

    let used_url = if is_windows() {
        windows_script_url.unwrap_or(unix_script_url)
    } else {
        unix_script_url
    };

    match result {
        Ok(output) if output.status.success() => {
            let path =
                find_executable_in_path(binary_name).unwrap_or_else(|| PathBuf::from(binary_name));
            Some(crate::models::AgentInstallResult {
                agent,
                success: true,
                message: Some(format!("installed successfully via script ({used_url})")),
                installed_path: Some(path.display().to_string()),
            })
        }
        _ => None,
    }
}

pub fn manual_install_hint(agent: AgentKind) -> String {
    match agent {
        AgentKind::Codex => "Please install manually:\n  \
             npm: npm install -g @openai/codex\n  \
             brew: brew install --cask codex\n  \
             script: curl -fsSL https://chatgpt.com/codex/install.sh | sh\n  \
             Windows: powershell -Command \"irm https://chatgpt.com/codex/install.ps1 | iex\""
            .to_string(),
        AgentKind::ClaudeCode => "Please install manually:\n  \
             script: curl -fsSL https://claude.ai/install.sh | bash\n  \
             brew: brew install --cask claude-code\n  \
             Windows: powershell -Command \"irm https://claude.ai/install.ps1 | iex\"\n  \
             npm (deprecated): npm install -g @anthropic-ai/claude-code"
            .to_string(),
        AgentKind::OpenCode => "Please install manually:\n  \
             npm: npm i -g opencode-ai@latest\n  \
             brew: brew install anomalyco/tap/opencode\n  \
             script: curl -fsSL https://opencode.ai/install | bash\n  \
             Windows: scoop install opencode"
            .to_string(),
        AgentKind::Acp => "ACP agent is configured in bridge settings.\n  \
             Kiro (local stdio): install `kiro-cli`, run `kiro-cli login`, then configure `profile: kiro` with `command: kiro-cli` and `args: [\"acp\"]`\n  \
             Generic HTTP: configure `profile: generic_http` with an ACP-compatible endpoint URL"
            .to_string(),
        AgentKind::Custom => "Custom agent does not support auto-install".to_string(),
    }
}

pub async fn install_agent(agent: AgentKind) -> crate::models::AgentInstallResult {
    let (binary_name, npm_package, brew_package, unix_script, windows_script) = match agent {
        AgentKind::Codex => (
            "codex",
            "@openai/codex",
            "codex",
            "https://chatgpt.com/codex/install.sh",
            Some("https://chatgpt.com/codex/install.ps1"),
        ),
        AgentKind::ClaudeCode => (
            "claude",
            "@anthropic-ai/claude-code",
            "claude-code",
            "https://claude.ai/install.sh",
            Some("https://claude.ai/install.ps1"),
        ),
        AgentKind::OpenCode => (
            "opencode",
            "opencode-ai",
            "anomalyco/tap/opencode",
            "https://opencode.ai/install",
            None,
        ),
        AgentKind::Acp => {
            return crate::models::AgentInstallResult {
                agent,
                success: false,
                message: Some(
                    "ACP agent does not support auto-install. For Kiro, install `kiro-cli` and run `kiro-cli login`; for generic HTTP ACP, configure the endpoint in settings.".to_string(),
                ),
                installed_path: None,
            };
        }
        AgentKind::Custom => {
            return crate::models::AgentInstallResult {
                agent,
                success: false,
                message: Some("Custom agent does not support auto-install".to_string()),
                installed_path: None,
            };
        }
    };

    if let Some(result) = try_install_with_npm(agent, npm_package, binary_name).await {
        return result;
    }
    if let Some(result) = try_install_with_brew(agent, brew_package, binary_name).await {
        return result;
    }
    if let Some(result) =
        try_install_with_script(agent, unix_script, windows_script, binary_name).await
    {
        return result;
    }

    crate::models::AgentInstallResult {
        agent,
        success: false,
        message: Some(manual_install_hint(agent)),
        installed_path: None,
    }
}
