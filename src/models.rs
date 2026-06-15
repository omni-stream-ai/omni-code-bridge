use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    /// The bridge provider ID that was explicitly specified (message-level or session-level).
    /// None when auto-selected from the provider list by priority.
    pub provider_id: Option<String>,
    /// The opencode provider name to use in prompt_async (e.g., "omni-bridge", "omni-bridge-anthropic").
    /// This is determined by the format and must match what modify_opencode_config creates.
    pub opencode_provider_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    Codex,
    ClaudeCode,
    OpenCode,
    Custom,
}

#[cfg(test)]
impl AgentKind {
    pub const ALL: [AgentKind; 4] = [
        AgentKind::Codex,
        AgentKind::ClaudeCode,
        AgentKind::OpenCode,
        AgentKind::Custom,
    ];
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
    /// Default provider for this session (references provider config id)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub session: SessionSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_status: Option<GitStatusDetail>,
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
pub struct CreateSessionInput {
    pub project_id: String,
    pub title: Option<String>,
    pub agent: AgentKind,
    #[serde(default)]
    pub brief_reply_mode: bool,
    /// Default provider for this session.
    /// Omit to avoid bridge-side provider resolution; use "AUTO" to enable auto-selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSessionInput {
    /// Update the session-level provider selection.
    /// `Some(id)` uses a specific provider, `Some("AUTO")` enables auto-selection,
    /// and `None` clears provider resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<Option<String>>,
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
    /// Override provider for this message.
    /// Omit to avoid bridge-side provider resolution; use "AUTO" to enable auto-selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
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
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SessionEvent {
    SessionSnapshot(SessionSummary),
    SessionStatus(SessionStatusEvent),
    MessageCreated(ChatMessage),
    MessageDelta(MessageDeltaEvent),
    AgentError(AgentErrorEvent),
    ApprovalRequested(ApprovalRequestEvent),
    ApprovalResolved(ApprovalResolvedEvent),
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
pub struct OpenAiModel {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub owned_by: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiModelList {
    pub object: String,
    pub data: Vec<OpenAiModel>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiTranscriptionResponse {
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiVerboseTranscriptionSegment {
    pub id: usize,
    pub seek: usize,
    pub start: f32,
    pub end: f32,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiVerboseTranscriptionResponse {
    pub task: String,
    pub language: String,
    pub duration: f32,
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<OpenAiVerboseTranscriptionSegment>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiErrorDetail {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiErrorResponse {
    pub error: OpenAiErrorDetail,
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
pub struct OpenAiAudioSpeechRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioSpeechStreamResponse {
    pub stream_url: String,
    pub content_type: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechModelKind {
    Asr,
    Tts,
    Vad,
    Speaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechRuntime {
    Offline,
    Streaming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechComputeBackend {
    Cpu,
    Onnx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechProfile {
    AsrBatch,
    AsrRealtime,
    TtsDefault,
    VadDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeechProfileSelection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asr_batch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asr_realtime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tts_default: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vad_default: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeechModelCapabilities {
    pub streaming: bool,
    pub realtime_asr: bool,
    pub batch_asr: bool,
    pub speech_synthesis: bool,
    pub vad: bool,
    pub speaker_embedding: bool,
    pub endpointing: bool,
    pub punctuation: bool,
    pub inverse_text_normalization: bool,
    pub multilingual: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeechModelSummary {
    pub id: String,
    pub kind: SpeechModelKind,
    pub display_name: String,
    pub description: String,
    pub languages: Vec<String>,
    pub runtime: SpeechRuntime,
    pub backend: SpeechComputeBackend,
    pub capabilities: SpeechModelCapabilities,
    pub features: Vec<String>,
    pub supports_profiles: Vec<SpeechProfile>,
    pub recommended_profiles: Vec<SpeechProfile>,
    pub download_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_size_mb: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate_hz: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_voice: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voices: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voice_details: Vec<SpeechVoiceSummary>,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_path: Option<String>,
    pub selected_by: Vec<SpeechProfile>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeechVoiceSummary {
    pub id: String,
    pub name: String,
    pub language: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gender: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechDownloadStatus {
    Queued,
    Downloading,
    Extracting,
    Verifying,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechDownloadTask {
    pub task_id: String,
    pub model_id: String,
    pub status: SpeechDownloadStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeechStatus {
    pub root_dir: String,
    pub profiles: SpeechProfileSelection,
    pub voices: SpeechVoiceSelection,
    pub models: Vec<SpeechModelSummary>,
    pub downloads: Vec<SpeechDownloadTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechModelDownloadInput {
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechProfileSelectionInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeechVoiceSelection {
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub tts_by_model: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechVoiceSelectionInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeakerFilterSettings {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker_id: Option<String>,
    #[serde(default)]
    pub threshold: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeakerRecord {
    pub id: String,
    pub name: String,
    pub embedding_model_id: String,
    pub embedding_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeakerEnrollmentResult {
    pub speaker: SpeakerRecord,
    pub sample_duration_secs: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeakerFilterSettingsInput {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f32>,
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
    pub install_hint: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCommandForwarding {
    Native,
    Wrapped,
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
