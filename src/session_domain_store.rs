use std::{path::Path, sync::Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use crate::{
    models::{ChatMessage, MessageRole, SessionDiffEvent, SessionSummary},
    session_domain::{
        AgentProjection, CreateTurnCommand, CreateTurnResult, DomainMessage, DomainSession,
        DomainSessionStatus, EntityState, MessagePurpose, Segment, SegmentKind, SessionConfig,
        SessionDomainEvent, SessionState, Turn, TurnStatus,
    },
};
use uuid::Uuid;

pub struct SessionDomainStore {
    connection: Mutex<Connection>,
    event_tx: broadcast::Sender<SessionDomainEvent>,
}

impl SessionDomainStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;
                PRAGMA synchronous = NORMAL;

                CREATE TABLE IF NOT EXISTS domain_sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS domain_sessions_project_updated
                    ON domain_sessions(project_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS domain_turns (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES domain_sessions(id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE INDEX IF NOT EXISTS domain_turns_session_sequence
                    ON domain_turns(session_id, sequence);

                CREATE TABLE IF NOT EXISTS domain_events (
                    event_id INTEGER NOT NULL,
                    session_id TEXT NOT NULL REFERENCES domain_sessions(id) ON DELETE CASCADE,
                    session_version INTEGER NOT NULL,
                    turn_id TEXT,
                    event_type TEXT NOT NULL,
                    entity_id TEXT,
                    entity_revision INTEGER,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, event_id)
                );
                CREATE INDEX IF NOT EXISTS domain_events_session_event
                    ON domain_events(session_id, event_id);

                CREATE TABLE IF NOT EXISTS domain_command_results (
                    command_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    command_type TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_bindings (
                    provider TEXT NOT NULL,
                    provider_entity_type TEXT NOT NULL,
                    provider_entity_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    turn_id TEXT,
                    entity_id TEXT,
                    PRIMARY KEY(provider, provider_entity_type, provider_entity_id)
                );
                "#,
            )
            .map_err(|error| error.to_string())?;
        let (event_tx, _) = broadcast::channel(1024);
        Ok(Self {
            connection: Mutex::new(connection),
            event_tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionDomainEvent> {
        self.event_tx.subscribe()
    }

    pub fn ensure_session(&self, legacy: &SessionSummary) -> Result<DomainSession, String> {
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        if let Some(session) = read_session(&transaction, &legacy.id)? {
            return Ok(session);
        }
        let now = Utc::now();
        let session = DomainSession {
            id: legacy.id.clone(),
            project_id: legacy.project_id.clone(),
            title: legacy.title.clone(),
            agent: legacy.agent,
            status: DomainSessionStatus::Idle,
            version: 1,
            config: SessionConfig {
                provider_id: legacy.provider_id.clone(),
                reasoning_effort: legacy.reasoning_effort,
                model: legacy.model.clone(),
                brief_reply_mode: legacy.brief_reply_mode,
            },
            active_turn_id: None,
            pending_approval_id: None,
            forked_from: None,
            unread_count: legacy.unread_count,
            last_message_preview: legacy.last_message_preview.clone(),
            created_at: legacy.updated_at,
            updated_at: now,
        };
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            None,
            "session.created",
            Some(&session.id),
            Some(session.version),
            json!({ "session": session }),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(session)
    }

    pub fn session_state(&self, session_id: &str) -> Result<Option<SessionState>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let Some(session) = read_session(&connection, session_id)? else {
            return Ok(None);
        };
        let mut statement = connection
            .prepare("SELECT state_json FROM domain_turns WHERE session_id = ?1 ORDER BY sequence")
            .map_err(|error| error.to_string())?;
        let turns = statement
            .query_map([session_id], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?
            .map(|row| decode(&row.map_err(|error| error.to_string())?))
            .collect::<Result<Vec<Turn>, String>>()?;
        let cursor = latest_cursor(&connection, session_id)?;
        Ok(Some(SessionState {
            session,
            turns,
            cursor,
        }))
    }

    pub fn import_message_history(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> Result<(), String> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let mut session = read_session(&transaction, session_id)?
            .ok_or_else(|| "session not found".to_string())?;
        let existing: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM domain_turns WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if existing > 0 {
            return Ok(());
        }

        let mut turns = Vec::<Turn>::new();
        for message in messages {
            if message.role == MessageRole::User || turns.is_empty() {
                let sequence = turns.len() as i64 + 1;
                let is_user = message.role == MessageRole::User;
                turns.push(Turn {
                    id: format!("imported-turn-{}", Uuid::new_v4()),
                    session_id: session_id.to_string(),
                    sequence,
                    version: 1,
                    status: TurnStatus::Completed,
                    input_mode: crate::models::InputMode::Text,
                    user_message: DomainMessage {
                        id: if is_user {
                            message.id.clone()
                        } else {
                            format!("imported-user-{}", Uuid::new_v4())
                        },
                        turn_id: String::new(),
                        sequence: 1,
                        revision: 1,
                        purpose: MessagePurpose::User,
                        state: EntityState::Completed,
                        content: if is_user {
                            message.content.clone()
                        } else {
                            String::new()
                        },
                        attachments: Vec::new(),
                        created_at: message.created_at,
                        updated_at: message.created_at,
                    },
                    segments: Vec::new(),
                    artifacts: Vec::new(),
                    final_assistant_message_id: None,
                    created_at: message.created_at,
                    started_at: Some(message.created_at),
                    completed_at: Some(message.created_at),
                });
                let turn = turns.last_mut().expect("imported turn");
                turn.user_message.turn_id = turn.id.clone();
                if is_user {
                    continue;
                }
            }

            let turn = turns.last_mut().expect("imported turn");
            let segment_id = format!("imported-segment-{}", Uuid::new_v4());
            let segment_sequence = turn.segments.len() as i64 + 1;
            if message.role == MessageRole::Assistant {
                turn.final_assistant_message_id = Some(message.id.clone());
                turn.segments.push(Segment {
                    id: segment_id,
                    turn_id: turn.id.clone(),
                    sequence: segment_sequence,
                    revision: 1,
                    kind: SegmentKind::AssistantMessage,
                    state: EntityState::Completed,
                    message: Some(DomainMessage {
                        id: message.id.clone(),
                        turn_id: turn.id.clone(),
                        sequence: 1,
                        revision: 1,
                        purpose: MessagePurpose::Final,
                        state: EntityState::Completed,
                        content: message.content.clone(),
                        attachments: Vec::new(),
                        created_at: message.created_at,
                        updated_at: message.created_at,
                    }),
                    activities: Vec::new(),
                    latest_activity_id: None,
                    created_at: message.created_at,
                    updated_at: message.created_at,
                });
            } else {
                let activity_id = message.id.clone();
                turn.segments.push(Segment {
                    id: segment_id.clone(),
                    turn_id: turn.id.clone(),
                    sequence: segment_sequence,
                    revision: 1,
                    kind: SegmentKind::Execution,
                    state: EntityState::Completed,
                    message: None,
                    activities: vec![crate::session_domain::Activity {
                        id: activity_id.clone(),
                        turn_id: turn.id.clone(),
                        segment_id,
                        sequence: 1,
                        revision: 1,
                        kind: crate::session_domain::ActivityKind::Progress,
                        state: EntityState::Completed,
                        title: message.content.clone(),
                        primary: None,
                        secondary: Vec::new(),
                        payload: json!({ "source": "legacy_import" }),
                        created_at: message.created_at,
                        updated_at: message.created_at,
                    }],
                    latest_activity_id: Some(activity_id),
                    created_at: message.created_at,
                    updated_at: message.created_at,
                });
            }
        }
        for turn in &turns {
            write_turn(&transaction, turn)?;
        }
        session.version += 1;
        session.updated_at = Utc::now();
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            None,
            "session.history_imported",
            Some(&session.id),
            Some(session.version),
            json!({ "session": session }),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(())
    }

    pub fn import_diff_history(
        &self,
        session_id: &str,
        diffs: &[SessionDiffEvent],
    ) -> Result<(), String> {
        if diffs.is_empty() {
            return Ok(());
        }
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let mut session = read_session(&transaction, session_id)?
            .ok_or_else(|| "session not found".to_string())?;
        let mut statement = transaction
            .prepare("SELECT state_json FROM domain_turns WHERE session_id = ?1 ORDER BY sequence")
            .map_err(|error| error.to_string())?;
        let mut turns = statement
            .query_map([session_id], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?
            .map(|row| decode(&row.map_err(|error| error.to_string())?))
            .collect::<Result<Vec<Turn>, String>>()?;
        drop(statement);
        if turns.is_empty() {
            return Ok(());
        }

        let mut changed = false;
        for diff in diffs {
            let artifact_id = format!(
                "imported-diff-{:x}",
                Sha256::digest(encode(diff)?.as_bytes())
            );
            if turns.iter().any(|turn| {
                turn.artifacts
                    .iter()
                    .any(|artifact| artifact.id == artifact_id)
            }) {
                continue;
            }
            let target_index = diff
                .conversation_turn_id
                .as_deref()
                .and_then(|id| turns.iter().position(|turn| turn.user_message.id == id))
                .unwrap_or(turns.len() - 1);
            let turn = &mut turns[target_index];
            let now = turn.completed_at.unwrap_or(turn.created_at);
            turn.artifacts.push(crate::session_domain::Artifact {
                id: artifact_id,
                turn_id: turn.id.clone(),
                source_segment_id: None,
                source_activity_id: None,
                sequence: turn.artifacts.len() as i64 + 1,
                revision: 1,
                kind: crate::session_domain::ArtifactKind::TurnCumulativeDiff,
                state: EntityState::Completed,
                payload: serde_json::to_value(diff).map_err(|error| error.to_string())?,
                created_at: now,
                updated_at: now,
            });
            turn.version += 1;
            changed = true;
        }
        if !changed {
            return Ok(());
        }
        for turn in &turns {
            write_turn(&transaction, turn)?;
        }
        session.version += 1;
        session.updated_at = Utc::now();
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            None,
            "session.diff_history_imported",
            Some(&session.id),
            Some(session.version),
            json!({ "session": session }),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(())
    }

    pub fn sync_session_metadata(&self, legacy: &SessionSummary) -> Result<(), String> {
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let Some(mut session) = read_session(&transaction, &legacy.id)? else {
            return Ok(());
        };
        let changed = session.title != legacy.title
            || session.config.provider_id != legacy.provider_id
            || session.config.reasoning_effort != legacy.reasoning_effort
            || session.config.model != legacy.model
            || session.config.brief_reply_mode != legacy.brief_reply_mode;
        if !changed {
            return Ok(());
        }
        session.title = legacy.title.clone();
        session.config.provider_id = legacy.provider_id.clone();
        session.config.reasoning_effort = legacy.reasoning_effort;
        session.config.model = legacy.model.clone();
        session.config.brief_reply_mode = legacy.brief_reply_mode;
        session.version += 1;
        session.updated_at = Utc::now();
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            None,
            "session.metadata_changed",
            Some(&session.id),
            Some(session.version),
            json!({ "session": session }),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(())
    }

    pub fn create_turn(
        &self,
        session_id: &str,
        command: &CreateTurnCommand,
    ) -> Result<CreateTurnResult, DomainStoreError> {
        validate_id("command_id", &command.command_id)?;
        validate_id("turn_id", &command.turn_id)?;
        validate_id("user_message_id", &command.user_message_id)?;
        let content = command.content.trim();
        if content.is_empty() && command.attachments.is_empty() {
            return Err(DomainStoreError::Invalid(
                "content or attachments are required".to_string(),
            ));
        }
        let request_fingerprint = command_fingerprint(command)?;

        let mut connection = self
            .connection
            .lock()
            .map_err(|error| DomainStoreError::Storage(error.to_string()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| DomainStoreError::Storage(error.to_string()))?;

        if let Some(result) = read_command_result::<CreateTurnResult>(
            &transaction,
            session_id,
            &command.command_id,
            "create_turn",
        )? {
            if result.request_fingerprint != request_fingerprint {
                return Err(DomainStoreError::Conflict(
                    "command_id was already used with a different request".to_string(),
                ));
            }
            return Ok(CreateTurnResult {
                created: false,
                ..result
            });
        }

        let mut session = read_session(&transaction, session_id)?
            .ok_or_else(|| DomainStoreError::NotFound("session not found".to_string()))?;
        if let Some(expected) = command.expected_session_version
            && expected != session.version
        {
            return Err(DomainStoreError::Conflict(format!(
                "session version mismatch: expected {expected}, actual {}",
                session.version
            )));
        }
        if session.active_turn_id.is_some() {
            return Err(DomainStoreError::Conflict(
                "session already has an active turn".to_string(),
            ));
        }
        let turn_sequence: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM domain_turns WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .map_err(|error| DomainStoreError::Storage(error.to_string()))?;
        let now = Utc::now();
        let user_message = DomainMessage {
            id: command.user_message_id.clone(),
            turn_id: command.turn_id.clone(),
            sequence: 1,
            revision: 1,
            purpose: MessagePurpose::User,
            state: EntityState::Completed,
            content: content.to_string(),
            attachments: command.attachments.clone(),
            created_at: now,
            updated_at: now,
        };
        let turn = Turn {
            id: command.turn_id.clone(),
            session_id: session_id.to_string(),
            sequence: turn_sequence,
            version: 1,
            status: TurnStatus::Accepted,
            input_mode: command.input_mode.clone(),
            user_message,
            segments: Vec::new(),
            artifacts: Vec::new(),
            final_assistant_message_id: None,
            created_at: now,
            started_at: None,
            completed_at: None,
        };
        session.version += 1;
        session.status = DomainSessionStatus::Running;
        session.active_turn_id = Some(turn.id.clone());
        session.last_message_preview = Some(content.to_string());
        session.updated_at = now;
        write_session(&transaction, &session)?;
        write_turn(&transaction, &turn)?;
        let event = insert_event(
            &transaction,
            &session,
            Some(&turn.id),
            "turn.accepted",
            Some(&turn.id),
            Some(turn.version),
            json!({}),
        )?;
        let result = CreateTurnResult {
            turn,
            cursor: event.event_id,
            created: true,
            request_fingerprint,
        };
        write_command_result(
            &transaction,
            session_id,
            &command.command_id,
            "create_turn",
            &result,
        )?;
        transaction
            .commit()
            .map_err(|error| DomainStoreError::Storage(error.to_string()))?;
        let _ = self.event_tx.send(event);
        Ok(result)
    }

    pub fn events_after(
        &self,
        session_id: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<SessionDomainEvent>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT event_id, session_id, session_version, turn_id, event_type, entity_id, \
                 entity_revision, '{}', created_at FROM domain_events \
                 WHERE session_id = ?1 AND event_id > ?2 ORDER BY event_id LIMIT ?3",
            )
            .map_err(|error| error.to_string())?;
        statement
            .query_map(
                params![session_id, after, limit.clamp(1, 10_000) as i64],
                event_from_row,
            )
            .map_err(|error| error.to_string())?
            .map(|row| row.map_err(|error| error.to_string()))
            .collect()
    }

    pub fn event_snapshot(
        &self,
        session_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Option<(DomainSession, Option<Turn>)>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let Some(session) = read_session(&connection, session_id)? else {
            return Ok(None);
        };
        let turn = turn_id
            .map(|id| read_turn(&connection, id))
            .transpose()?
            .flatten();
        Ok(Some((session, turn)))
    }

    pub fn project_agent_event(
        &self,
        session_id: &str,
        expected_turn_id: &str,
        projection: AgentProjection,
    ) -> Result<(), String> {
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let mut session = read_session(&transaction, session_id)?
            .ok_or_else(|| "session not found".to_string())?;
        let turn_id = session
            .active_turn_id
            .clone()
            .ok_or_else(|| "session has no active turn".to_string())?;
        if turn_id != expected_turn_id {
            return Err(format!(
                "turn mismatch: expected {expected_turn_id}, active {turn_id}"
            ));
        }
        let mut turn = read_turn(&transaction, &turn_id)?
            .ok_or_else(|| "active turn not found".to_string())?;
        let now = Utc::now();
        let (event_type, entity_id, entity_revision) = match projection {
            AgentProjection::AssistantMessage {
                message_id,
                purpose,
                state,
                content,
            } => {
                if purpose == MessagePurpose::Final && state == EntityState::Completed {
                    turn.final_assistant_message_id = Some(message_id.clone());
                    session.last_message_preview = Some(content.clone());
                }
                let existing_index = turn.segments.iter().position(|segment| {
                    segment
                        .message
                        .as_ref()
                        .is_some_and(|message| message.id == message_id)
                });
                let revision = if let Some(index) = existing_index {
                    let mut segment = turn.segments.remove(index);
                    let message = segment.message.as_mut().expect("message segment");
                    message.revision += 1;
                    message.purpose = purpose;
                    message.state = state;
                    message.content = content;
                    message.updated_at = now;
                    segment.revision += 1;
                    segment.state = state;
                    segment.updated_at = now;
                    let revision = message.revision;
                    turn.segments.push(segment);
                    for (index, segment) in turn.segments.iter_mut().enumerate() {
                        segment.sequence = index as i64 + 1;
                    }
                    revision
                } else {
                    let sequence = turn.segments.len() as i64 + 1;
                    turn.segments.push(Segment {
                        id: Uuid::new_v4().to_string(),
                        turn_id: turn.id.clone(),
                        sequence,
                        revision: 1,
                        kind: SegmentKind::AssistantMessage,
                        state,
                        message: Some(DomainMessage {
                            id: message_id.clone(),
                            turn_id: turn.id.clone(),
                            sequence: sequence + 1,
                            revision: 1,
                            purpose,
                            state,
                            content,
                            attachments: Vec::new(),
                            created_at: now,
                            updated_at: now,
                        }),
                        activities: Vec::new(),
                        latest_activity_id: None,
                        created_at: now,
                        updated_at: now,
                    });
                    1
                };
                if purpose == MessagePurpose::Final && state == EntityState::Completed {
                    turn.final_assistant_message_id = turn
                        .segments
                        .last()
                        .and_then(|segment| segment.message.as_ref())
                        .map(|message| message.id.clone());
                }
                ("message.updated", Some(message_id), Some(revision))
            }
            AgentProjection::Activity {
                activity_id,
                kind,
                state,
                title,
                primary,
                secondary,
                payload,
            } => {
                if kind == crate::session_domain::ActivityKind::Approval {
                    session.pending_approval_id = if state == EntityState::AwaitingApproval {
                        Some(activity_id.clone())
                    } else {
                        None
                    };
                }
                let needs_segment = turn
                    .segments
                    .last()
                    .is_none_or(|segment| segment.kind != SegmentKind::Execution);
                if needs_segment {
                    let sequence = turn.segments.len() as i64 + 1;
                    turn.segments.push(Segment {
                        id: Uuid::new_v4().to_string(),
                        turn_id: turn.id.clone(),
                        sequence,
                        revision: 1,
                        kind: SegmentKind::Execution,
                        state: EntityState::Running,
                        message: None,
                        activities: Vec::new(),
                        latest_activity_id: None,
                        created_at: now,
                        updated_at: now,
                    });
                }
                let segment = turn.segments.last_mut().expect("execution segment");
                let revision = if let Some(activity) = segment
                    .activities
                    .iter_mut()
                    .find(|activity| activity.id == activity_id)
                {
                    activity.revision += 1;
                    activity.kind = kind;
                    activity.state = state;
                    activity.title = title;
                    activity.primary = primary;
                    activity.secondary = secondary;
                    merge_json_object(&mut activity.payload, payload);
                    activity.updated_at = now;
                    activity.revision
                } else {
                    segment.activities.push(crate::session_domain::Activity {
                        id: activity_id.clone(),
                        turn_id: turn.id.clone(),
                        segment_id: segment.id.clone(),
                        sequence: segment.activities.len() as i64 + 1,
                        revision: 1,
                        kind,
                        state,
                        title,
                        primary,
                        secondary,
                        payload,
                        created_at: now,
                        updated_at: now,
                    });
                    1
                };
                segment.latest_activity_id = Some(activity_id.clone());
                segment.revision += 1;
                segment.state = aggregate_segment_state(&segment.activities);
                segment.updated_at = now;
                ("activity.updated", Some(activity_id), Some(revision))
            }
            AgentProjection::Artifact {
                artifact_id,
                kind,
                state,
                source_activity_id,
                payload,
            } => {
                let source_segment_id = source_activity_id.as_ref().and_then(|id| {
                    turn.segments.iter().find_map(|segment| {
                        segment
                            .activities
                            .iter()
                            .any(|activity| &activity.id == id)
                            .then(|| segment.id.clone())
                    })
                });
                let revision = if let Some(artifact) = turn
                    .artifacts
                    .iter_mut()
                    .find(|artifact| artifact.id == artifact_id)
                {
                    artifact.revision += 1;
                    artifact.kind = kind;
                    artifact.state = state;
                    artifact.source_activity_id = source_activity_id;
                    artifact.source_segment_id = source_segment_id;
                    artifact.payload = payload;
                    artifact.updated_at = now;
                    artifact.revision
                } else {
                    turn.artifacts.push(crate::session_domain::Artifact {
                        id: artifact_id.clone(),
                        turn_id: turn.id.clone(),
                        source_segment_id,
                        source_activity_id,
                        sequence: turn.artifacts.len() as i64 + 1,
                        revision: 1,
                        kind,
                        state,
                        payload,
                        created_at: now,
                        updated_at: now,
                    });
                    1
                };
                ("artifact.updated", Some(artifact_id), Some(revision))
            }
            AgentProjection::TurnStatus { status, error } => {
                if turn.status.is_terminal() {
                    return Ok(());
                }
                turn.status = status;
                if status == TurnStatus::Running && turn.started_at.is_none() {
                    turn.started_at = Some(now);
                }
                if status.is_terminal() {
                    turn.completed_at = Some(now);
                    session.active_turn_id = None;
                    session.pending_approval_id = None;
                    let terminal_state = match status {
                        TurnStatus::Completed => EntityState::Completed,
                        TurnStatus::Cancelled => EntityState::Cancelled,
                        TurnStatus::Failed => EntityState::Failed,
                        _ => unreachable!("terminal status matched above"),
                    };
                    for segment in &mut turn.segments {
                        if !matches!(
                            segment.state,
                            EntityState::Completed | EntityState::Cancelled | EntityState::Failed
                        ) {
                            segment.state = terminal_state;
                            segment.revision += 1;
                            segment.updated_at = now;
                        }
                        for activity in &mut segment.activities {
                            if !matches!(
                                activity.state,
                                EntityState::Completed
                                    | EntityState::Cancelled
                                    | EntityState::Failed
                            ) {
                                activity.state = terminal_state;
                                activity.revision += 1;
                                activity.updated_at = now;
                            }
                        }
                    }
                    for artifact in &mut turn.artifacts {
                        if !matches!(
                            artifact.state,
                            EntityState::Completed | EntityState::Cancelled | EntityState::Failed
                        ) {
                            artifact.state = terminal_state;
                            artifact.revision += 1;
                            artifact.updated_at = now;
                        }
                    }
                }
                if status == TurnStatus::Completed {
                    session.unread_count = 1;
                }
                session.status = match status {
                    TurnStatus::AwaitingApproval => DomainSessionStatus::AwaitingApproval,
                    TurnStatus::Failed => DomainSessionStatus::Failed,
                    status if status.is_terminal() => DomainSessionStatus::Idle,
                    _ => DomainSessionStatus::Running,
                };
                if let Some(error) = error {
                    session.last_message_preview = Some(error);
                }
                (
                    "turn.status_changed",
                    Some(turn.id.clone()),
                    Some(turn.version + 1),
                )
            }
        };
        turn.version += 1;
        session.version += 1;
        session.updated_at = now;
        write_turn(&transaction, &turn)?;
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            Some(&turn.id),
            event_type,
            entity_id.as_deref(),
            entity_revision,
            json!({}),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(())
    }

    pub fn mark_read(&self, session_id: &str) -> Result<(), String> {
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let mut session = read_session(&transaction, session_id)?
            .ok_or_else(|| "session not found".to_string())?;
        if session.unread_count == 0 {
            return Ok(());
        }
        session.unread_count = 0;
        session.version += 1;
        session.updated_at = Utc::now();
        write_session(&transaction, &session)?;
        let event = insert_event(
            &transaction,
            &session,
            None,
            "session.read_state_changed",
            Some(&session.id),
            Some(session.version),
            json!({ "session": session }),
        )?;
        transaction.commit().map_err(|error| error.to_string())?;
        let _ = self.event_tx.send(event);
        Ok(())
    }

    pub fn interrupt_active_turns(&self) -> Result<Vec<String>, String> {
        let session_ids = {
            let connection = self.connection.lock().map_err(|error| error.to_string())?;
            let mut statement = connection
                .prepare("SELECT id FROM domain_sessions")
                .map_err(|error| error.to_string())?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?
                .map(|row| row.map_err(|error| error.to_string()))
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut interrupted = Vec::new();
        for session_id in session_ids {
            let active_turn_id = self
                .session_state(&session_id)?
                .and_then(|state| state.session.active_turn_id);
            if let Some(turn_id) = active_turn_id {
                self.project_agent_event(
                    &session_id,
                    &turn_id,
                    AgentProjection::TurnStatus {
                        status: TurnStatus::Cancelled,
                        error: Some("Bridge restarted before the turn completed".to_string()),
                    },
                )?;
                interrupted.push(session_id);
            }
        }
        Ok(interrupted)
    }
}

fn aggregate_segment_state(activities: &[crate::session_domain::Activity]) -> EntityState {
    if activities
        .iter()
        .any(|activity| matches!(activity.state, EntityState::AwaitingApproval))
    {
        return EntityState::AwaitingApproval;
    }
    if activities
        .iter()
        .any(|activity| matches!(activity.state, EntityState::Running | EntityState::Pending))
    {
        return EntityState::Running;
    }
    if activities
        .iter()
        .any(|activity| matches!(activity.state, EntityState::Failed))
    {
        return EntityState::Failed;
    }
    if activities
        .iter()
        .any(|activity| matches!(activity.state, EntityState::Cancelled))
    {
        return EntityState::Cancelled;
    }
    EntityState::Completed
}

#[derive(Debug)]
pub enum DomainStoreError {
    NotFound(String),
    Invalid(String),
    Conflict(String),
    Storage(String),
}

impl From<String> for DomainStoreError {
    fn from(value: String) -> Self {
        Self::Storage(value)
    }
}

fn validate_id(name: &str, value: &str) -> Result<(), DomainStoreError> {
    if value.trim().is_empty() {
        return Err(DomainStoreError::Invalid(format!("{name} cannot be empty")));
    }
    Ok(())
}

fn merge_json_object(existing: &mut Value, incoming: Value) {
    match (existing.as_object_mut(), incoming) {
        (Some(existing), Value::Object(incoming)) => existing.extend(incoming),
        (_, incoming) => *existing = incoming,
    }
}

fn command_fingerprint(command: &CreateTurnCommand) -> Result<String, DomainStoreError> {
    let encoded = encode(command).map_err(DomainStoreError::Storage)?;
    Ok(format!(
        "sha256:v1:{:x}",
        Sha256::digest(encoded.as_bytes())
    ))
}

fn encode<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| error.to_string())
}

fn decode<T: DeserializeOwned>(value: &str) -> Result<T, String> {
    serde_json::from_str(value).map_err(|error| error.to_string())
}

fn read_session(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<DomainSession>, String> {
    connection
        .query_row(
            "SELECT state_json FROM domain_sessions WHERE id = ?1",
            [session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .map(|value| decode(&value))
        .transpose()
}

fn write_session(connection: &Connection, session: &DomainSession) -> Result<(), String> {
    connection
        .execute(
            "INSERT INTO domain_sessions(id, project_id, version, state_json, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(id) DO UPDATE SET project_id = excluded.project_id, \
             version = excluded.version, state_json = excluded.state_json, \
             updated_at = excluded.updated_at",
            params![
                session.id,
                session.project_id,
                session.version,
                encode(session)?,
                session.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_turn(connection: &Connection, turn: &Turn) -> Result<(), String> {
    connection
        .execute(
            "INSERT INTO domain_turns(id, session_id, sequence, version, state_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(id) DO UPDATE SET version = excluded.version, state_json = excluded.state_json",
            params![
                turn.id,
                turn.session_id,
                turn.sequence,
                turn.version,
                encode(turn)?,
                turn.created_at.to_rfc3339(),
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn read_turn(connection: &Connection, turn_id: &str) -> Result<Option<Turn>, String> {
    connection
        .query_row(
            "SELECT state_json FROM domain_turns WHERE id = ?1",
            [turn_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .map(|value| decode(&value))
        .transpose()
}

fn latest_cursor(connection: &Connection, session_id: &str) -> Result<i64, String> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(event_id), 0) FROM domain_events WHERE session_id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())
}

fn insert_event(
    connection: &Connection,
    session: &DomainSession,
    turn_id: Option<&str>,
    event_type: &str,
    entity_id: Option<&str>,
    entity_revision: Option<i64>,
    payload: Value,
) -> Result<SessionDomainEvent, String> {
    let created_at = Utc::now();
    let event_id: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(event_id), 0) + 1 FROM domain_events WHERE session_id = ?1",
            [&session.id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    connection
        .execute(
            "INSERT INTO domain_events(event_id, session_id, session_version, turn_id, event_type, \
             entity_id, entity_revision, payload_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                event_id,
                session.id,
                session.version,
                turn_id,
                event_type,
                entity_id,
                entity_revision,
                payload.to_string(),
                created_at.to_rfc3339(),
            ],
        )
        .map_err(|error| error.to_string())?;
    if event_id % 256 == 0 {
        connection
            .execute(
                "DELETE FROM domain_events WHERE session_id = ?1 AND event_id <= ?2",
                params![session.id, event_id - 2_048],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(SessionDomainEvent {
        event_id,
        session_id: session.id.clone(),
        session_version: session.version,
        turn_id: turn_id.map(ToOwned::to_owned),
        event_type: event_type.to_string(),
        entity_id: entity_id.map(ToOwned::to_owned),
        entity_revision,
        payload,
        created_at,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionDomainEvent> {
    let payload: String = row.get(7)?;
    let created_at: String = row.get(8)?;
    Ok(SessionDomainEvent {
        event_id: row.get(0)?,
        session_id: row.get(1)?,
        session_version: row.get(2)?,
        turn_id: row.get(3)?,
        event_type: row.get(4)?,
        entity_id: row.get(5)?,
        entity_revision: row.get(6)?,
        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
        created_at: DateTime::parse_from_rfc3339(&created_at)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
    })
}

fn read_command_result<T: DeserializeOwned>(
    connection: &Connection,
    session_id: &str,
    command_id: &str,
    command_type: &str,
) -> Result<Option<T>, DomainStoreError> {
    let row = connection
        .query_row(
            "SELECT session_id, command_type, result_json FROM domain_command_results WHERE command_id = ?1",
            [command_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
        )
        .optional()
        .map_err(|error| DomainStoreError::Storage(error.to_string()))?;
    let Some((stored_session_id, stored_type, result)) = row else {
        return Ok(None);
    };
    if stored_session_id != session_id || stored_type != command_type {
        return Err(DomainStoreError::Conflict(
            "command_id was already used for another command".to_string(),
        ));
    }
    decode(&result).map(Some).map_err(DomainStoreError::Storage)
}

fn write_command_result<T: Serialize>(
    connection: &Connection,
    session_id: &str,
    command_id: &str,
    command_type: &str,
    result: &T,
) -> Result<(), DomainStoreError> {
    connection
        .execute(
            "INSERT INTO domain_command_results(command_id, session_id, command_type, result_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                command_id,
                session_id,
                command_type,
                encode(result).map_err(DomainStoreError::Storage)?,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|error| DomainStoreError::Storage(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::SessionDomainStore;
    use crate::{
        models::{AgentKind, InputMode, SessionStatus, SessionSummary},
        session_domain::{
            AgentProjection, CreateTurnCommand, EntityState, MessagePurpose, SegmentKind,
            TurnStatus,
        },
    };

    fn test_store() -> SessionDomainStore {
        let path = std::env::temp_dir().join(format!("omni-domain-{}.sqlite3", Uuid::new_v4()));
        SessionDomainStore::open(&path).expect("domain store should open")
    }

    fn session(id: &str) -> SessionSummary {
        SessionSummary {
            id: id.to_string(),
            project_id: "project".to_string(),
            title: "Session".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            runtime_session_ref: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        }
    }

    #[test]
    fn create_turn_is_durable_and_idempotent() {
        let store = test_store();
        store.ensure_session(&session("session-1")).unwrap();
        let command = CreateTurnCommand {
            command_id: "command-1".to_string(),
            turn_id: "turn-1".to_string(),
            user_message_id: "user-1".to_string(),
            content: "hello".to_string(),
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        let first = store.create_turn("session-1", &command).unwrap();
        let duplicate = store.create_turn("session-1", &command).unwrap();
        assert!(first.created);
        assert!(!duplicate.created);
        assert_eq!(first.turn.id, duplicate.turn.id);
        assert_eq!(first.cursor, duplicate.cursor);
        let state = store.session_state("session-1").unwrap().unwrap();
        assert_eq!(state.turns.len(), 1);
        assert_eq!(state.cursor, 2);

        let mut conflicting = command.clone();
        conflicting.content = "different".to_string();
        assert!(matches!(
            store.create_turn("session-1", &conflicting),
            Err(super::DomainStoreError::Conflict(_))
        ));
    }

    #[test]
    fn projection_updates_one_assistant_entity_and_completes_turn() {
        let store = test_store();
        store.ensure_session(&session("session-2")).unwrap();
        store
            .create_turn(
                "session-2",
                &CreateTurnCommand {
                    command_id: "command-2".to_string(),
                    turn_id: "turn-2".to_string(),
                    user_message_id: "user-2".to_string(),
                    content: "hello".to_string(),
                    attachments: Vec::new(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    expected_session_version: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .unwrap();
        for content in ["hel", "hello"] {
            store
                .project_agent_event(
                    "session-2",
                    "turn-2",
                    AgentProjection::AssistantMessage {
                        message_id: "assistant-2".to_string(),
                        purpose: MessagePurpose::Final,
                        state: EntityState::Running,
                        content: content.to_string(),
                    },
                )
                .unwrap();
        }
        store
            .project_agent_event(
                "session-2",
                "turn-2",
                AgentProjection::AssistantMessage {
                    message_id: "assistant-2".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Completed,
                    content: "hello".to_string(),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-2",
                "turn-2",
                AgentProjection::TurnStatus {
                    status: TurnStatus::Completed,
                    error: None,
                },
            )
            .unwrap();
        let state = store.session_state("session-2").unwrap().unwrap();
        assert_eq!(state.turns[0].segments.len(), 1);
        let message = state.turns[0].segments[0].message.as_ref().unwrap();
        assert_eq!(message.content, "hello");
        assert_eq!(message.revision, 3);
        assert_eq!(state.session.active_turn_id, None);
        assert_eq!(state.session.unread_count, 1);
        assert_eq!(state.cursor, 6);
        assert_eq!(
            store.events_after("session-2", 4, 10).unwrap()[0].event_id,
            5
        );
    }

    #[test]
    fn restart_interrupts_active_turn_and_allows_another_turn() {
        let store = test_store();
        store.ensure_session(&session("session-restart")).unwrap();
        let mut command = CreateTurnCommand {
            command_id: "command-before-restart".to_string(),
            turn_id: "turn-before-restart".to_string(),
            user_message_id: "user-before-restart".to_string(),
            content: "before restart".to_string(),
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        store.create_turn("session-restart", &command).unwrap();

        assert_eq!(
            store.interrupt_active_turns().unwrap(),
            vec!["session-restart".to_string()]
        );
        let state = store.session_state("session-restart").unwrap().unwrap();
        assert_eq!(state.turns[0].status, TurnStatus::Cancelled);
        assert_eq!(state.session.active_turn_id, None);

        command.command_id = "command-after-restart".to_string();
        command.turn_id = "turn-after-restart".to_string();
        command.user_message_id = "user-after-restart".to_string();
        assert!(store.create_turn("session-restart", &command).is_ok());
    }

    #[test]
    fn late_projection_cannot_mutate_the_next_turn() {
        let store = test_store();
        store.ensure_session(&session("session-late")).unwrap();
        let command = |command_id: &str, turn_id: &str, user_id: &str| CreateTurnCommand {
            command_id: command_id.to_string(),
            turn_id: turn_id.to_string(),
            user_message_id: user_id.to_string(),
            content: "hello".to_string(),
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        store
            .create_turn("session-late", &command("command-1", "turn-1", "user-1"))
            .unwrap();
        store
            .project_agent_event(
                "session-late",
                "turn-1",
                AgentProjection::TurnStatus {
                    status: TurnStatus::Cancelled,
                    error: None,
                },
            )
            .unwrap();
        store
            .create_turn("session-late", &command("command-2", "turn-2", "user-2"))
            .unwrap();

        let error = store
            .project_agent_event(
                "session-late",
                "turn-1",
                AgentProjection::AssistantMessage {
                    message_id: "late-message".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Completed,
                    content: "late".to_string(),
                },
            )
            .unwrap_err();
        assert!(error.contains("turn mismatch"));
        let state = store.session_state("session-late").unwrap().unwrap();
        assert!(state.turns[1].segments.is_empty());
    }

    #[test]
    fn execution_between_updates_keeps_one_stable_assistant_entity() {
        use crate::session_domain::ActivityKind;

        let store = test_store();
        store.ensure_session(&session("session-segments")).unwrap();
        store
            .create_turn(
                "session-segments",
                &CreateTurnCommand {
                    command_id: "command-segments".to_string(),
                    turn_id: "turn-segments".to_string(),
                    user_message_id: "user-segments".to_string(),
                    content: "inspect".to_string(),
                    attachments: Vec::new(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    expected_session_version: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-segments",
                "turn-segments",
                AgentProjection::AssistantMessage {
                    message_id: "provider-message".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Running,
                    content: "I will inspect it.".to_string(),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-segments",
                "turn-segments",
                AgentProjection::Activity {
                    activity_id: "activity".to_string(),
                    kind: ActivityKind::ToolCall,
                    state: EntityState::Running,
                    title: "Inspecting".to_string(),
                    primary: None,
                    secondary: Vec::new(),
                    payload: json!({}),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-segments",
                "turn-segments",
                AgentProjection::Activity {
                    activity_id: "activity-2".to_string(),
                    kind: ActivityKind::ToolResult,
                    state: EntityState::Completed,
                    title: "Inspected".to_string(),
                    primary: None,
                    secondary: Vec::new(),
                    payload: json!({}),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-segments",
                "turn-segments",
                AgentProjection::AssistantMessage {
                    message_id: "provider-message".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Completed,
                    content: "Inspection complete.".to_string(),
                },
            )
            .unwrap();

        let state = store.session_state("session-segments").unwrap().unwrap();
        assert_eq!(state.turns[0].segments.len(), 2);
        assert_eq!(state.turns[0].segments[0].kind, SegmentKind::Execution);
        assert_eq!(state.turns[0].segments[0].activities.len(), 2);
        assert_eq!(
            state.turns[0].segments[0].latest_activity_id.as_deref(),
            Some("activity-2")
        );
        assert_eq!(
            state.turns[0].segments[1].message.as_ref().unwrap().purpose,
            MessagePurpose::Final
        );
        assert_eq!(
            state.turns[0].segments[1].message.as_ref().unwrap().content,
            "Inspection complete."
        );
    }

    #[test]
    fn projection_events_persist_only_lightweight_metadata() {
        let store = test_store();
        store
            .ensure_session(&session("session-light-events"))
            .unwrap();
        store
            .create_turn(
                "session-light-events",
                &CreateTurnCommand {
                    command_id: "command-light".to_string(),
                    turn_id: "turn-light".to_string(),
                    user_message_id: "user-light".to_string(),
                    content: "inspect".to_string(),
                    attachments: Vec::new(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    expected_session_version: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-light-events",
                "turn-light",
                AgentProjection::AssistantMessage {
                    message_id: "message-light".to_string(),
                    purpose: MessagePurpose::Commentary,
                    state: EntityState::Running,
                    content: "x".repeat(100_000),
                },
            )
            .unwrap();

        let events = store.events_after("session-light-events", 0, 10).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events.last().unwrap().payload, json!({}));
        assert!(serde_json::to_string(events.last().unwrap()).unwrap().len() < 512);
    }

    #[test]
    fn cumulative_assistant_snapshot_after_activity_replaces_same_entity() {
        let store = test_store();
        store
            .ensure_session(&session("session-cumulative"))
            .unwrap();
        store
            .create_turn(
                "session-cumulative",
                &CreateTurnCommand {
                    command_id: "command-cumulative".to_string(),
                    turn_id: "turn-cumulative".to_string(),
                    user_message_id: "user-cumulative".to_string(),
                    content: "inspect".to_string(),
                    attachments: Vec::new(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    expected_session_version: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-cumulative",
                "turn-cumulative",
                AgentProjection::AssistantMessage {
                    message_id: "assistant".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Running,
                    content: "first update".to_string(),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-cumulative",
                "turn-cumulative",
                AgentProjection::Activity {
                    activity_id: "tool".to_string(),
                    kind: crate::session_domain::ActivityKind::ToolCall,
                    state: EntityState::Completed,
                    title: "tool".to_string(),
                    primary: None,
                    secondary: Vec::new(),
                    payload: json!({}),
                },
            )
            .unwrap();
        store
            .project_agent_event(
                "session-cumulative",
                "turn-cumulative",
                AgentProjection::AssistantMessage {
                    message_id: "assistant".to_string(),
                    purpose: MessagePurpose::Final,
                    state: EntityState::Running,
                    content: "first update\n\nsecond update".to_string(),
                },
            )
            .unwrap();

        let state = store.session_state("session-cumulative").unwrap().unwrap();
        assert_eq!(state.turns[0].segments.len(), 2);
        let assistant = state.turns[0]
            .segments
            .last()
            .and_then(|segment| segment.message.as_ref())
            .expect("second assistant segment");
        assert_eq!(assistant.content, "first update\n\nsecond update");
    }

    #[test]
    fn execution_segment_remains_running_until_all_activities_finish() {
        use crate::session_domain::ActivityKind;

        let store = test_store();
        store
            .ensure_session(&session("session-parallel-tools"))
            .unwrap();
        let command = CreateTurnCommand {
            command_id: "command-parallel".to_string(),
            turn_id: "turn-parallel".to_string(),
            user_message_id: "user-parallel".to_string(),
            content: "run both".to_string(),
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        store
            .create_turn("session-parallel-tools", &command)
            .unwrap();
        for (id, state) in [
            ("tool-a", EntityState::Running),
            ("tool-b", EntityState::Running),
            ("tool-a", EntityState::Completed),
        ] {
            store
                .project_agent_event(
                    "session-parallel-tools",
                    "turn-parallel",
                    AgentProjection::Activity {
                        activity_id: id.to_string(),
                        kind: ActivityKind::ToolCall,
                        state,
                        title: id.to_string(),
                        primary: None,
                        secondary: Vec::new(),
                        payload: json!({}),
                    },
                )
                .unwrap();
        }
        let state = store
            .session_state("session-parallel-tools")
            .unwrap()
            .unwrap();
        assert_eq!(state.turns[0].segments[0].state, EntityState::Running);
    }
}

use chrono::DateTime;
