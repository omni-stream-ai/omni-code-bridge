use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    Codex,
    ClaudeCode,
    OpenCode,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    AwaitingApproval,
    Waiting,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub root_path: String,
    pub updated_at: DateTime<Utc>,
    pub session_count: u32,
    pub last_session_preview: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
pub struct AppUpdateManifest {
    pub version_name: String,
    pub version_code: u64,
    pub apk_url: String,
    pub release_notes: String,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioTranscription {
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplySummary {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioSpeechInput {
    pub input: String,
    pub voice: Option<String>,
    pub speed: Option<f32>,
    pub volume: Option<f32>,
    pub response_format: Option<String>,
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
