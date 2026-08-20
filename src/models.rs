use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de::Visitor};
use std::marker::PhantomData;

pub const AUTO_PROVIDER_ID: &str = "AUTO";

/// API format for model providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiFormat {
    /// OpenAI-compatible API (default for most providers)
    #[serde(alias = "openai")]
    OpenAiCompatible,
    /// Anthropic Messages API (required for Claude Code)
    #[serde(alias = "anthropic")]
    AnthropicMessages,
    /// Codex API (uses JSON-RPC protocol, internally calls OpenAI-compatible API)
    #[serde(alias = "codex")]
    Codex,
    /// Experimental ACP-compatible agent endpoint
    #[serde(alias = "acp")]
    Acp,
}

impl Default for ApiFormat {
    fn default() -> Self {
        Self::OpenAiCompatible
    }
}

/// Configuration for a model provider (base_url + api_key + optional model)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelProviderConfig {
    /// Unique identifier for this provider config
    pub id: String,
    /// Display name
    pub name: String,
    /// API base URL (e.g., "https://api.openai.com/v1")
    pub base_url: String,
    /// API key
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_key: String,
    /// Default model for this provider (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// API format (openai-compatible or anthropic-messages)
    #[serde(default)]
    pub format: ApiFormat,
    /// Whether this provider is active
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Priority (lower number = higher priority)
    #[serde(default)]
    pub priority: i32,
}

fn default_enabled() -> bool {
    true
}

/// Resolved provider configuration for a specific agent execution
#[derive(Debug, Clone)]
pub struct ResolvedProviderConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: Option<String>,
    pub format: ApiFormat,
    pub acp_profile: Option<AcpProfile>,
    /// The bridge provider ID that was explicitly specified (message-level or session-level).
    /// None when auto-selected from the provider list by priority.
    pub provider_id: Option<String>,
    /// The opencode provider name to use in prompt_async (e.g., "omni-bridge", "omni-bridge-anthropic").
    /// This is determined by the format and must match what modify_opencode_config creates.
    pub opencode_provider_name: Option<String>,
    /// Additional headers required by non-model agent runtimes such as ACP.
    pub extra_headers: Vec<(String, String)>,
    /// Command used to launch local ACP runtimes such as `kiro-cli acp`.
    pub acp_command: Option<String>,
    /// Arguments for launching a local ACP runtime.
    pub acp_args: Vec<String>,
    /// Environment variables for launching a local ACP runtime.
    pub acp_env: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    Codex,
    ClaudeCode,
    OpenCode,
    Acp,
    Pi,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ReasoningEffort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

#[cfg(test)]
impl AgentKind {
    pub const ALL: [AgentKind; 6] = [
        AgentKind::Codex,
        AgentKind::ClaudeCode,
        AgentKind::OpenCode,
        AgentKind::Acp,
        AgentKind::Pi,
        AgentKind::Custom,
    ];
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpServerConfig {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub profile: AcpProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub headers: Vec<HeaderKeyValue>,
    #[serde(default)]
    pub env: Vec<HeaderKeyValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AcpProfile {
    #[default]
    #[serde(alias = "kiro")]
    Stdio,
    GenericHttp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderKeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    AwaitingApproval,
    Interrupted,
    Waiting,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub agent: AgentKind,
    #[serde(default)]
    pub brief_reply_mode: bool,
    pub status: SessionStatus,
    pub updated_at: DateTime<Utc>,
    pub unread_count: u32,
    pub last_message_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<ApprovalRequest>,
    /// Runtime/provider-native session or thread id used to resume the upstream agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_session_ref: Option<String>,
    /// Default provider for this session (references provider config id)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Default reasoning effort for this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Default model override for this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub session: SessionSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_status: Option<GitStatusDetail>,
    #[serde(default)]
    pub diffs: Vec<SessionDiffEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelSessionReplyResult {
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectGitStatus {
    Clean,
    Dirty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitStatusDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub dirty: bool,
    pub staged: bool,
    pub unstaged: bool,
    pub untracked: bool,
    pub changed_count: u32,
    pub staged_count: u32,
    pub unstaged_count: u32,
    pub untracked_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ahead: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behind: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: String,
    pub session_id: String,
    pub role: MessageRole,
    pub content: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageListQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageListPage {
    pub messages: Vec<ChatMessage>,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionInput {
    /// Client-generated idempotency key. When present it is also the stable,
    /// canonical session id exposed by the bridge.
    pub client_session_id: String,
    pub project_id: String,
    pub title: Option<String>,
    pub agent: AgentKind,
    #[serde(default)]
    pub brief_reply_mode: bool,
    /// Default provider for this session.
    /// Omit to avoid bridge-side provider resolution; use "AUTO" to enable auto-selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Default reasoning effort for this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Model selected for this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSessionInput {
    /// Update the session-level provider selection.
    /// `Some(id)` uses a specific provider, `Some("AUTO")` enables auto-selection,
    /// and `None` clears provider resolution.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_patch_value"
    )]
    pub provider_id: Option<Option<String>>,
    /// Update the session-level reasoning effort.
    /// `Some(level)` sets the default, and `None` clears the default.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_patch_value"
    )]
    pub reasoning_effort: Option<Option<ReasoningEffort>>,
    /// Update the session-level model override.
    /// `Some(name)` sets the model, and `None` clears the override.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_patch_value"
    )]
    pub model: Option<Option<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarkSessionReadInput {
    pub last_message_id: String,
}

fn deserialize_patch_value<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct PatchValueVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for PatchValueVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Option<Option<T>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("null or a valid field value")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(Some(None))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(Some(None))
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            T::deserialize(deserializer).map(|value| Some(Some(value)))
        }
    }

    deserializer.deserialize_option(PatchValueVisitor(PhantomData))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub root_path: String,
    pub updated_at: DateTime<Utc>,
    pub session_count: u32,
    pub last_session_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_status: Option<ProjectGitStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProjectInput {
    pub name: String,
    pub root_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputMode {
    Text,
    Voice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageInput {
    pub content: String,
    pub input_mode: InputMode,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    /// Override provider for this message.
    /// Omit to avoid bridge-side provider resolution; use "AUTO" to enable auto-selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Override reasoning effort for this message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Override model for this message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerClientMessageInput {
    pub content: String,
    pub project_name: Option<String>,
    pub project_id: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TriggerClientMessageResult {
    pub project: ProjectSummary,
    pub session: SessionSummary,
    pub message: ChatMessage,
}

#[cfg(test)]
mod tests {
    use super::{ReasoningEffort, UpdateSessionInput};

    #[test]
    fn update_session_input_distinguishes_null_from_missing_reasoning_effort() {
        let missing: UpdateSessionInput =
            serde_json::from_str("{}").expect("missing payload should parse");
        assert!(missing.reasoning_effort.is_none());

        let explicit_null: UpdateSessionInput =
            serde_json::from_str(r#"{"reasoning_effort":null}"#)
                .expect("null payload should parse");
        assert_eq!(explicit_null.reasoning_effort, Some(None));

        let explicit_value: UpdateSessionInput =
            serde_json::from_str(r#"{"reasoning_effort":"medium"}"#)
                .expect("value payload should parse");
        assert_eq!(
            explicit_value.reasoning_effort,
            Some(Some(ReasoningEffort::Medium))
        );
    }

    #[test]
    fn update_session_input_distinguishes_null_from_missing_provider_id() {
        let missing: UpdateSessionInput =
            serde_json::from_str("{}").expect("missing payload should parse");
        assert!(missing.provider_id.is_none());

        let explicit_null: UpdateSessionInput =
            serde_json::from_str(r#"{"provider_id":null}"#).expect("null payload should parse");
        assert_eq!(explicit_null.provider_id, Some(None));

        let explicit_value: UpdateSessionInput =
            serde_json::from_str(r#"{"provider_id":"AUTO"}"#).expect("value payload should parse");
        assert_eq!(explicit_value.provider_id, Some(Some("AUTO".to_string())));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummarizeReplyInput {
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStatusEvent {
    pub session_id: String,
    pub status: SessionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDeltaEvent {
    pub session_id: String,
    pub message_id: String,
    pub delta: String,
}

/// A complete replacement for an in-flight assistant message's content.
/// Clients must replace, rather than append, the `content` value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSnapshotEvent {
    pub session_id: String,
    pub message_id: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDiffEvent {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_turn_id: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentErrorEvent {
    pub session_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalKind {
    CommandExecution,
    ExecCommand,
    FileChange,
    ApplyPatch,
    Permissions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalChoice {
    Accept,
    AcceptForSession,
    AlwaysAllow,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub request_id: String,
    pub kind: ApprovalKind,
    pub command: Option<String>,
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approval_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approval_reason_kind: Option<String>,
    pub allow_accept_for_session: bool,
    pub allow_cancel: bool,
    pub resolvable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequestEvent {
    pub session_id: String,
    pub request: ApprovalRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResolvedEvent {
    pub session_id: String,
    pub request_id: String,
    pub choice: ApprovalChoice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDecisionInput {
    pub choice: ApprovalChoice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionUiRequest {
    pub request_id: String,
    pub method: String,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionUiRequestEvent {
    pub session_id: String,
    pub request: PiExtensionUiRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionUiResponseInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(default)]
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionUiResolvedEvent {
    pub session_id: String,
    pub request_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionCommand {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiExtensionCommandsEvent {
    pub session_id: String,
    pub commands: Vec<PiExtensionCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SessionEvent {
    SessionSnapshot(SessionSummary),
    SessionStatus(SessionStatusEvent),
    MessageCreated(ChatMessage),
    MessageDelta(MessageDeltaEvent),
    MessageSnapshot(MessageSnapshotEvent),
    SessionDiff(SessionDiffEvent),
    AgentError(AgentErrorEvent),
    ApprovalRequested(ApprovalRequestEvent),
    ApprovalResolved(ApprovalResolvedEvent),
    PiExtensionUiRequested(PiExtensionUiRequestEvent),
    PiExtensionUiResolved(PiExtensionUiResolvedEvent),
    PiExtensionCommandsUpdated(PiExtensionCommandsEvent),
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiResponse<T> {
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCompletionQuery {
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCompletionItem {
    pub path: String,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedFileResponse {
    pub id: String,
    pub file_name: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub url: String,
    pub absolute_url: String,
    pub local_path: String,
}

/// Unified JSON error response: `{"error": "message"}`
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

impl From<(StatusCode, String)> for ApiError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self { status, message }
    }
}

impl From<StatusCode> for ApiError {
    fn from(status: StatusCode) -> Self {
        Self {
            status,
            message: status.canonical_reason().unwrap_or("error").to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AppUpdateManifest {
    pub version_name: String,
    pub version_code: u64,
    pub apk_url: String,
    pub release_notes: String,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplySummary {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuthStatus {
    Pending,
    Approved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAuthRequestInput {
    pub client_id: String,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAuthRecord {
    pub request_id: String,
    pub client_id: String,
    pub device_name: Option<String>,
    pub status: ClientAuthStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushDeviceRegistration {
    pub client_id: String,
    pub platform: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub app_version: Option<String>,
    pub fcm_token: Option<String>,
    pub mi_push_reg_id: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPushDeviceInput {
    pub platform: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub app_version: Option<String>,
    pub fcm_token: Option<String>,
    pub mi_push_reg_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentInstallResult {
    pub agent: AgentKind,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentInstallInput {
    pub agent: AgentKind,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentReadiness {
    Ready,
    NotInstalled,
    AttentionRequired,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentSummary {
    pub kind: AgentKind,
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    pub selectable: bool,
    pub default_selected: bool,
    pub compatible_formats: Vec<ApiFormat>,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_path: Option<String>,
    pub readiness: AgentReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_diagnostic: Option<AcpAgentDiagnostic>,
    pub install_hint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AcpAgentDiagnostic {
    pub configured_server_id: String,
    pub configured_server_name: String,
    pub enabled: bool,
    pub profile: AcpProfile,
    #[serde(default)]
    pub auth_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub header_count: usize,
    #[serde(default)]
    pub env_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turn_url_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approval_reply_url_templates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cancel_url_templates: Vec<String>,
    pub enabled_server_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AcpAgentDiagnosticResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    pub is_default_selected: bool,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_path: Option<String>,
    pub readiness: AgentReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_message: Option<String>,
    pub source: String,
    pub probed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handshake_probe: Option<AcpHandshakeProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<AcpAgentDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AcpHandshakeProbe {
    pub attempted: bool,
    pub success: bool,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCommandForwarding {
    Native,
    Wrapped,
    Bridge,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentCommandSummary {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_hint: Option<String>,
    pub description: String,
    pub forwarding: AgentCommandForwarding,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentCommandsSummary {
    pub kind: AgentKind,
    pub commands: Vec<AgentCommandSummary>,
}
