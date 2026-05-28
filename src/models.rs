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
    #[serde(default)]
    pub system_prompt: Option<String>,
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
