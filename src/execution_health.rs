use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::session_domain::{ActivityKind, AgentProjection, EntityState, Turn, TurnStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPhase {
    Starting,
    Thinking,
    RunningCommand,
    AwaitingApproval,
    Retrying,
    WaitingForTool,
    Background,
    Unconfirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHealth {
    pub phase: ExecutionPhase,
    pub phase_started_at: DateTime<Utc>,
    pub last_progress_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_error: Option<String>,
}

impl ExecutionHealth {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            phase: ExecutionPhase::Starting,
            phase_started_at: now,
            last_progress_at: now,
            last_checked_at: None,
            check_started_at: None,
            check_error: None,
        }
    }

    pub fn set_phase(&mut self, phase: ExecutionPhase, now: DateTime<Utc>) {
        if self.phase != phase {
            self.phase = phase;
            self.phase_started_at = now;
        }
    }

    pub fn needs_attention(&self, now: DateTime<Utc>) -> bool {
        let seconds = match self.phase {
            ExecutionPhase::AwaitingApproval => return false,
            ExecutionPhase::Starting => 60,
            ExecutionPhase::Unconfirmed => 30,
            ExecutionPhase::WaitingForTool | ExecutionPhase::Retrying => 120,
            ExecutionPhase::Thinking => 180,
            ExecutionPhase::RunningCommand | ExecutionPhase::Background => 300,
        };
        now.signed_duration_since(self.last_progress_at)
            .num_seconds()
            >= seconds
            && now
                .signed_duration_since(self.phase_started_at)
                .num_seconds()
                >= seconds
    }

    pub fn observe(turn: &Turn, event: &AgentProjection, now: DateTime<Utc>) -> Self {
        let mut health = turn
            .execution
            .clone()
            .unwrap_or_else(|| Self::new(turn.created_at));
        let mut progress = false;
        let phase = match event {
            AgentProjection::AssistantMessage {
                message_id,
                content,
                ..
            } => {
                progress = !content.trim().is_empty()
                    && !turn.segments.iter().any(|segment| {
                        segment.message.as_ref().is_some_and(|message| {
                            &message.id == message_id && &message.content == content
                        })
                    });
                progress.then_some(ExecutionPhase::Thinking)
            }
            AgentProjection::Activity {
                activity_id,
                kind,
                state,
                title,
                payload,
                ..
            } => {
                if title.starts_with("[codex:retry]") {
                    Some(ExecutionPhase::Retrying)
                } else if title == "[status] waiting for background tasks" {
                    Some(ExecutionPhase::Background)
                } else if is_observation(title) {
                    None
                } else {
                    progress = matches!(
                        kind,
                        ActivityKind::Reasoning
                            | ActivityKind::ToolCall
                            | ActivityKind::ToolResult
                            | ActivityKind::Approval
                    ) && !turn
                        .segments
                        .iter()
                        .flat_map(|segment| &segment.activities)
                        .any(|activity| {
                            &activity.id == activity_id
                                && &activity.state == state
                                && &activity.title == title
                                && &activity.payload == payload
                        });
                    match kind {
                        ActivityKind::Approval if *state == EntityState::AwaitingApproval => {
                            Some(ExecutionPhase::AwaitingApproval)
                        }
                        ActivityKind::ToolCall
                            if !matches!(
                                state,
                                EntityState::Completed
                                    | EntityState::Failed
                                    | EntityState::Cancelled
                            ) =>
                        {
                            Some(
                                if payload
                                    .get("process_id")
                                    .and_then(|value| value.as_str())
                                    .is_some()
                                {
                                    ExecutionPhase::RunningCommand
                                } else {
                                    ExecutionPhase::WaitingForTool
                                },
                            )
                        }
                        ActivityKind::Progress => None,
                        _ => progress.then_some(ExecutionPhase::Thinking),
                    }
                }
            }
            AgentProjection::Artifact {
                artifact_id,
                payload,
                ..
            } => {
                progress = !turn
                    .artifacts
                    .iter()
                    .any(|artifact| &artifact.id == artifact_id && &artifact.payload == payload);
                None
            }
            AgentProjection::TurnStatus {
                status: TurnStatus::AwaitingApproval,
                ..
            } => Some(ExecutionPhase::AwaitingApproval),
            _ => None,
        };
        if let Some(phase) = phase {
            health.set_phase(phase, now);
        }
        if progress {
            health.last_progress_at = now;
            health.check_error = None;
        }
        health
    }
}

pub fn is_observation(title: &str) -> bool {
    [
        "[status]",
        "[thread]",
        "[debug:",
        "[moderation]",
        "[codex:watchdog]",
        "[command:stalled]",
    ]
    .iter()
    .any(|prefix| title.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use serde_json::json;

    fn turn() -> Turn {
        serde_json::from_value(json!({
            "id": "turn", "session_id": "session", "sequence": 1, "version": 1,
            "status": "running", "input_mode": "text", "segments": [], "artifacts": [],
            "created_at": "2026-10-30T00:00:00Z",
            "user_message": { "id": "user", "turn_id": "turn", "sequence": 1, "revision": 1,
                "purpose": "user", "state": "completed", "content": "Build APK", "attachments": [],
                "created_at": "2026-10-30T00:00:00Z", "updated_at": "2026-10-30T00:00:00Z" }
        }))
        .unwrap()
    }

    fn activity(title: &str) -> AgentProjection {
        AgentProjection::Activity {
            activity_id: "activity".to_string(),
            kind: ActivityKind::ToolCall,
            state: EntityState::Running,
            title: title.to_string(),
            primary: None,
            secondary: vec![],
            payload: json!({}),
        }
    }

    #[test]
    fn execution_health_observations_and_retries_do_not_reset_progress() {
        let mut turn = turn();
        for title in [
            "[status] command still running",
            "[thread] active",
            "[moderation] metadata updated",
            "[command:stalled] curl (waiting once more)",
            "[codex:retry] Reconnecting... 1/5",
            "[codex:retry] Reconnecting... 2/5",
        ] {
            turn.execution = Some(ExecutionHealth::observe(
                &turn,
                &activity(title),
                turn.created_at + TimeDelta::minutes(10),
            ));
            assert_eq!(
                turn.execution.as_ref().unwrap().last_progress_at,
                turn.created_at
            );
        }
        assert_eq!(
            turn.execution.as_ref().unwrap().phase,
            ExecutionPhase::Retrying
        );
    }

    #[test]
    fn execution_health_stage_deadlines_do_not_expire_approval_or_active_output() {
        let now = turn().created_at;
        let mut health = ExecutionHealth::new(now);
        health.set_phase(ExecutionPhase::WaitingForTool, now);
        assert!(!health.needs_attention(now + TimeDelta::seconds(119)));
        assert!(health.needs_attention(now + TimeDelta::seconds(120)));
        health.set_phase(ExecutionPhase::AwaitingApproval, now);
        assert!(!health.needs_attention(now + TimeDelta::days(1)));
        health.set_phase(ExecutionPhase::RunningCommand, now);
        health.last_progress_at = now + TimeDelta::minutes(59);
        assert!(!health.needs_attention(now + TimeDelta::hours(1)));
        assert!(health.needs_attention(now + TimeDelta::minutes(64)));
    }
}
