use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::{AgentKind, InputMode, ReasoningEffort};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainSessionStatus {
    Idle,
    Running,
    AwaitingApproval,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Accepted,
    Running,
    AwaitingApproval,
    Completed,
    Cancelled,
    Failed,
}

impl TurnStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentKind {
    AssistantMessage,
    Execution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagePurpose {
    User,
    Commentary,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityState {
    Pending,
    Running,
    AwaitingApproval,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    Reasoning,
    Progress,
    ToolCall,
    ToolResult,
    Approval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    TurnCumulativeDiff,
    GeneratedFile,
    OtherResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub id: String,
    pub kind: String,
    pub file_name: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_reference: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainMessage {
    pub id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub revision: i64,
    pub purpose: MessagePurpose,
    pub state: EntityState,
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub id: String,
    pub turn_id: String,
    pub segment_id: String,
    pub sequence: i64,
    pub revision: i64,
    pub kind: ActivityKind,
    pub state: EntityState,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    #[serde(default)]
    pub secondary: Vec<String>,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub revision: i64,
    pub kind: SegmentKind,
    pub state: EntityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<DomainMessage>,
    #[serde(default)]
    pub activities: Vec<Activity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_activity_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_segment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_activity_id: Option<String>,
    pub sequence: i64,
    pub revision: i64,
    pub kind: ArtifactKind,
    pub state: EntityState,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub id: String,
    pub session_id: String,
    pub sequence: i64,
    pub version: i64,
    pub status: TurnStatus,
    pub input_mode: InputMode,
    pub user_message: DomainMessage,
    #[serde(default)]
    pub segments: Vec<Segment>,
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_assistant_message_id: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub brief_reply_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkOrigin {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_sequence: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSession {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub agent: AgentKind,
    pub status: DomainSessionStatus,
    pub version: i64,
    pub config: SessionConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ForkOrigin>,
    pub unread_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_preview: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub session: DomainSession,
    pub turns: Vec<Turn>,
    pub cursor: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDomainEvent {
    pub event_id: i64,
    pub session_id: String,
    pub session_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_revision: Option<i64>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTurnCommand {
    pub command_id: String,
    pub turn_id: String,
    pub user_message_id: String,
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    pub input_mode: InputMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_session_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTurnResult {
    pub turn: Turn,
    pub cursor: i64,
    pub created: bool,
    pub request_fingerprint: String,
}

#[derive(Debug, Clone)]
pub enum AgentProjection {
    AssistantMessage {
        message_id: String,
        purpose: MessagePurpose,
        state: EntityState,
        content: String,
    },
    Activity {
        activity_id: String,
        kind: ActivityKind,
        state: EntityState,
        title: String,
        primary: Option<String>,
        secondary: Vec<String>,
        payload: Value,
    },
    Artifact {
        artifact_id: String,
        kind: ArtifactKind,
        state: EntityState,
        source_activity_id: Option<String>,
        payload: Value,
    },
    TurnStatus {
        status: TurnStatus,
        error: Option<String>,
    },
}
