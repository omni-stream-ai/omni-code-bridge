use std::{
    collections::HashMap,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    models::{AgentKind, ChatMessage, MessageRole, ProjectSummary, SessionStatus, SessionSummary},
    session_store::{project_id_for_path, truncate_preview},
};

pub struct ClaudeArchiveSummary {
    pub fingerprint: u64,
    pub projects: HashMap<String, ProjectSummary>,
    pub sessions: HashMap<String, SessionSummary>,
    pub session_files: HashMap<String, PathBuf>,
}

pub struct ClaudeMessages {
    pub fingerprint: u64,
    pub messages: Vec<ChatMessage>,
}

struct ParsedSessionSummaryRecord {
    project: ProjectSummary,
    session: SessionSummary,
    session_file: PathBuf,
}

pub fn load_claude_archive_summary() -> ClaudeArchiveSummary {
    let root = claude_projects_root();
    let (paths, fingerprint) = match collect_jsonl_files(&root) {
        Ok(result) => result,
        Err(_) => {
            return ClaudeArchiveSummary {
                fingerprint: 0,
                projects: HashMap::new(),
                sessions: HashMap::new(),
                session_files: HashMap::new(),
            };
        }
    };

    let mut archive = ClaudeArchiveSummary {
        fingerprint,
        projects: HashMap::new(),
        sessions: HashMap::new(),
        session_files: HashMap::new(),
    };

    for path in paths {
        let Some(record) = parse_session_summary_file(&path) else {
            continue;
        };
        archive
            .session_files
            .insert(record.session.id.clone(), record.session_file.clone());
        archive
            .projects
            .entry(record.project.id.clone())
            .and_modify(|project| {
                project.session_count += 1;
                if record.project.updated_at > project.updated_at {
                    project.updated_at = record.project.updated_at;
                    project.last_session_preview = record.project.last_session_preview.clone();
                }
            })
            .or_insert(record.project);
        archive
            .sessions
            .insert(record.session.id.clone(), record.session);
    }

    archive
}

pub fn load_claude_messages(path: &Path) -> Option<ClaudeMessages> {
    let fingerprint = file_fingerprint(path)?;
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut messages = Vec::new();
    let mut pending_blocks: Vec<String> = Vec::new();
    let mut pending_timestamp: DateTime<Utc> = Utc::now();
    let mut pending_id: Option<String> = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now);

        if session_id.is_none() {
            session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(ToString::to_string);
        }

        match line_type {
            "user" if is_tool_result(&value) => {}
            "user" => {
                flush_pending_claude_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_blocks,
                    &mut pending_id,
                    pending_timestamp,
                );
                if let Some(text) = extract_user_text(&value) {
                    messages.push(ChatMessage {
                        id: value
                            .get("uuid")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::User,
                        content: text,
                        created_at: timestamp,
                    });
                }
            }
            "assistant" => {
                if let Some(text) = extract_assistant_text(&value) {
                    if pending_blocks.is_empty() {
                        pending_timestamp = timestamp;
                        pending_id = value
                            .get("uuid")
                            .and_then(Value::as_str)
                            .or_else(|| value.pointer("/message/id").and_then(Value::as_str))
                            .map(ToString::to_string);
                    }
                    pending_blocks.push(text);
                }
            }
            _ => {}
        }
    }

    flush_pending_claude_assistant(
        &mut messages,
        &session_id.clone().unwrap_or_default(),
        &mut pending_blocks,
        &mut pending_id,
        pending_timestamp,
    );

    let session_id = session_id?;
    for message in &mut messages {
        message.session_id = session_id.clone();
    }

    Some(ClaudeMessages {
        fingerprint,
        messages,
    })
}

fn flush_pending_claude_assistant(
    messages: &mut Vec<ChatMessage>,
    session_id: &str,
    pending_blocks: &mut Vec<String>,
    pending_id: &mut Option<String>,
    timestamp: DateTime<Utc>,
) {
    if pending_blocks.is_empty() {
        return;
    }
    let content = pending_blocks
        .drain(..)
        .map(|block| block.trim().to_string())
        .filter(|block| !block.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    if content.is_empty() {
        return;
    }
    let id = pending_id
        .take()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    messages.push(ChatMessage {
        id,
        session_id: session_id.to_string(),
        role: MessageRole::Assistant,
        content,
        created_at: timestamp,
    });
}

fn parse_session_summary_file(path: &Path) -> Option<ParsedSessionSummaryRecord> {
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut cwd = None;
    let mut updated_at = None;
    let mut first_user_message = None;
    let mut last_preview = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp);

        if session_id.is_none() {
            session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(ToString::to_string);
        }
        if cwd.is_none() {
            cwd = value
                .get("cwd")
                .and_then(Value::as_str)
                .map(ToString::to_string);
        }

        match line_type {
            "user" => {
                if let Some(text) = extract_summary_user_text(&value) {
                    if first_user_message.is_none() {
                        first_user_message = Some(text.clone());
                    }
                    last_preview = Some(text);
                }
            }
            "assistant" => {
                if let Some(text) = extract_summary_assistant_text(&value) {
                    last_preview = Some(text);
                }
            }
            _ => {}
        }

        if let Some(timestamp) = timestamp {
            updated_at =
                Some(updated_at.map_or(timestamp, |current: DateTime<Utc>| current.max(timestamp)));
        }
    }

    let session_id = session_id?;
    let cwd = cwd?;
    let updated_at = updated_at.unwrap_or_else(Utc::now);
    let project_id = project_id_for_path(&cwd);
    let session_title = first_user_message
        .as_deref()
        .map(|text| truncate_preview(text, 32))
        .unwrap_or_else(|| {
            path.file_stem()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "claude-session".to_string())
        });

    Some(ParsedSessionSummaryRecord {
        project: ProjectSummary {
            id: project_id.clone(),
            name: project_name_from_path(&cwd),
            root_path: cwd.clone(),
            updated_at,
            session_count: 1,
            last_session_preview: last_preview.clone(),
            git_branch: None,
            git_status: None,
        },
        session: SessionSummary {
            id: session_id.clone(),
            project_id,
            title: session_title,
            agent: AgentKind::ClaudeCode,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at,
            unread_count: 0,
            last_message_preview: last_preview,
            pending_approval: None,
            runtime_session_ref: Some(session_id),
            provider_id: None,
            reasoning_effort: None,
        },
        session_file: path.to_path_buf(),
    })
}

fn is_tool_result(value: &Value) -> bool {
    value
        .pointer("/message/content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .all(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"))
        })
        .unwrap_or(false)
}

fn extract_user_text(value: &Value) -> Option<String> {
    let content = value.pointer("/message/content")?;
    extract_text_blocks(content)
}

fn extract_assistant_text(value: &Value) -> Option<String> {
    let content = value.pointer("/message/content")?;
    extract_text_blocks(content)
}

fn extract_summary_user_text(value: &Value) -> Option<String> {
    if value
        .get("isMeta")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }

    let text = extract_user_text(value)?;
    sanitize_summary_user_text(&text, value)
}

fn extract_summary_assistant_text(value: &Value) -> Option<String> {
    let text = extract_assistant_text(value)?;
    (!should_ignore_assistant_text_for_preview(&text, value)).then_some(text)
}

fn extract_text_blocks(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => normalize_text(text),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| match item.get("type").and_then(Value::as_str) {
                    Some("text") => item.get("text").and_then(Value::as_str),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            normalize_text(&text)
        }
        _ => None,
    }
}

fn normalize_text(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn sanitize_summary_user_text(text: &str, value: &Value) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty()
        || trimmed == "[Request interrupted by user]"
        || trimmed.starts_with("<local-command-caveat>")
        || trimmed.starts_with("<task-notification>")
        || is_task_notification_message(value)
        || is_compact_summary_message(value)
    {
        return None;
    }

    if is_command_wrapper_message(trimmed) {
        return extract_tag_text(trimmed, "command-args").and_then(normalize_text);
    }

    Some(trimmed.to_string())
}

fn should_ignore_assistant_text_for_preview(text: &str, value: &Value) -> bool {
    let trimmed = text.trim();
    trimmed.is_empty()
        || trimmed == "No response requested."
        || (is_synthetic_assistant_message(value)
            && (value
                .get("isApiErrorMessage")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || value.get("error").is_some()
                || trimmed.starts_with("API Error:")))
}

fn is_compact_summary_message(value: &Value) -> bool {
    value
        .get("isCompactSummary")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || value
            .get("isVisibleInTranscriptOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn is_task_notification_message(value: &Value) -> bool {
    value.pointer("/origin/kind").and_then(Value::as_str) == Some("task-notification")
}

fn is_command_wrapper_message(text: &str) -> bool {
    text.contains("<command-name>") || text.contains("<command-message>")
}

fn is_synthetic_assistant_message(value: &Value) -> bool {
    value.pointer("/message/model").and_then(Value::as_str) == Some("<synthetic>")
}

fn extract_tag_text<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = text.find(&start_tag)? + start_tag.len();
    let end = text[start..].find(&end_tag)? + start;
    Some(&text[start..end])
}

fn collect_jsonl_files(root: &Path) -> std::io::Result<(Vec<PathBuf>, u64)> {
    let mut files = Vec::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if !root.exists() {
        return Ok((files, 0));
    }
    collect_jsonl_files_inner(root, &mut files, &mut hasher)?;
    files.sort();
    Ok((files, hasher.finish()))
}

fn collect_jsonl_files_inner(
    root: &Path,
    files: &mut Vec<PathBuf>,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files_inner(&path, files, hasher)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            if let Some(fingerprint) = file_fingerprint(&path) {
                path.hash(hasher);
                fingerprint.hash(hasher);
            }
            files.push(path);
        }
    }
    Ok(())
}

fn file_fingerprint(path: &Path) -> Option<u64> {
    let metadata = fs::metadata(path).ok()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metadata.len().hash(&mut hasher);
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis()
        .hash(&mut hasher);
    Some(hasher.finish())
}

fn claude_projects_root() -> PathBuf {
    std::env::var("ECHO_MATE_CLAUDE_PROJECTS_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".claude/projects")))
        .unwrap_or_else(|_| PathBuf::from(".claude/projects"))
}

fn project_name_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_claude_messages_parses_jsonl_archive() {
        let file = temp_jsonl_file(
            "claude-session",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-1","cwd":"/tmp/project","uuid":"u1","message":{"content":"hello claude"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-1","cwd":"/tmp/project","uuid":"a1","message":{"content":[{"type":"text","text":"hello "},{"type":"text","text":"user"}]}}"#,
            ],
        );

        let messages = load_claude_messages(&file)
            .expect("claude archive should parse")
            .messages;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].session_id, "claude-1");
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "hello claude");
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "hello user");
    }

    #[test]
    fn parse_session_summary_file_builds_claude_project_and_session() {
        let file = temp_jsonl_file(
            "claude-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-2","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"fix bug"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-2","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"fixed"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.id, "claude-2");
        assert_eq!(record.session.agent, AgentKind::ClaudeCode);
        assert_eq!(record.session.title, "fix bug");
        assert_eq!(
            record.session.last_message_preview.as_deref(),
            Some("fixed")
        );
        assert_eq!(record.project.name, "claude-app");
        assert_eq!(record.project.session_count, 1);
    }

    #[test]
    fn parse_session_summary_file_ignores_local_command_caveat_meta_message() {
        let file = temp_jsonl_file(
            "claude-summary-meta",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-3","cwd":"/tmp/claude-app","uuid":"u0","isMeta":true,"message":{"content":"<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"user","sessionId":"claude-3","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"real prompt"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-3","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real prompt");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
    }

    #[test]
    fn parse_session_summary_file_ignores_slash_command_wrapper_messages() {
        let file = temp_jsonl_file(
            "claude-summary-command",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-5","cwd":"/tmp/claude-app","uuid":"u0","message":{"content":"<command-name>/clear</command-name>\n<command-message>clear</command-message>\n<command-args></command-args>"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"user","sessionId":"claude-5","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"actual request"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-5","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "actual request");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
    }

    #[test]
    fn parse_session_summary_file_uses_slash_command_args_as_title() {
        let file = temp_jsonl_file(
            "claude-summary-command-args",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-7","cwd":"/tmp/claude-app","uuid":"u0","message":{"content":"<command-message>skill-creator</command-message>\n<command-name>/skill-creator</command-name>\n<command-args>根据当前 cli, 写一个 skill</command-args>"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-7","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "根据当前 cli, 写一个 skill");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
    }

    #[test]
    fn parse_session_summary_file_ignores_compact_summary_messages() {
        let file = temp_jsonl_file(
            "claude-summary-compact",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-6","cwd":"/tmp/claude-app","uuid":"u0","isCompactSummary":true,"isVisibleInTranscriptOnly":true,"message":{"content":"This session is being continued from a previous conversation..."}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"user","sessionId":"claude-6","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"real follow-up"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-6","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real follow-up");
    }

    #[test]
    fn parse_session_summary_file_ignores_interrupted_request_messages() {
        let file = temp_jsonl_file(
            "claude-summary-interrupted",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-8","cwd":"/tmp/claude-app","uuid":"u0","message":{"content":[{"type":"text","text":"[Request interrupted by user]"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"user","sessionId":"claude-8","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"real follow-up"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-8","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real follow-up");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
    }

    #[test]
    fn parse_session_summary_file_ignores_task_notification_messages() {
        let file = temp_jsonl_file(
            "claude-summary-task-notification",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-9","cwd":"/tmp/claude-app","uuid":"u0","origin":{"kind":"task-notification"},"message":{"content":"<task-notification>\n<task-id>bq7yrfohe</task-id>\n<summary>Background command \"bun dev &amp;\" completed (exit code 0)</summary>\n</task-notification>"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"user","sessionId":"claude-9","cwd":"/tmp/claude-app","uuid":"u1","message":{"content":"real follow-up"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-9","cwd":"/tmp/claude-app","uuid":"a1","message":{"content":[{"type":"text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real follow-up");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
    }

    #[test]
    fn parse_session_summary_file_ignores_no_response_requested_preview() {
        let file = temp_jsonl_file(
            "claude-summary-no-response-requested",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-10","cwd":"/tmp/claude-app","uuid":"u0","message":{"content":"real prompt"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-10","cwd":"/tmp/claude-app","uuid":"a1","message":{"model":"<synthetic>","content":[{"type":"text","text":"No response requested."}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real prompt");
        assert_eq!(
            record.session.last_message_preview.as_deref(),
            Some("real prompt")
        );
    }

    #[test]
    fn parse_session_summary_file_ignores_synthetic_api_error_preview() {
        let file = temp_jsonl_file(
            "claude-summary-synthetic-error",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-11","cwd":"/tmp/claude-app","uuid":"u0","message":{"content":"real prompt"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-11","cwd":"/tmp/claude-app","uuid":"a0","message":{"content":[{"type":"text","text":"working result"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-11","cwd":"/tmp/claude-app","uuid":"a1","isApiErrorMessage":true,"error":"server_error","message":{"model":"<synthetic>","content":[{"type":"text","text":"API Error: 529 {\"type\":\"error\"}"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "real prompt");
        assert_eq!(
            record.session.last_message_preview.as_deref(),
            Some("working result")
        );
    }

    #[test]
    fn load_claude_messages_parses_array_user_content_and_text_blocks() {
        let file = temp_jsonl_file(
            "claude-array-user",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-4","cwd":"/tmp/project","uuid":"u1","message":{"content":[{"type":"text","text":"[Request interrupted by user]"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-4","cwd":"/tmp/project","uuid":"a1","message":{"content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"visible"}]}}"#,
            ],
        );

        let messages = load_claude_messages(&file)
            .expect("claude archive should parse")
            .messages;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "[Request interrupted by user]");
        assert_eq!(messages[1].content, "visible");
    }

    #[test]
    fn load_claude_messages_merges_consecutive_assistant_blocks() {
        let file = temp_jsonl_file(
            "claude-merge",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-merge","cwd":"/tmp/project","uuid":"u1","message":{"content":"hello"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-merge","cwd":"/tmp/project","uuid":"a1","message":{"content":[{"type":"text","text":"first block"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-merge","cwd":"/tmp/project","uuid":"a2","message":{"content":[{"type":"text","text":"second block"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"user","sessionId":"claude-merge","cwd":"/tmp/project","uuid":"u2","message":{"content":"follow up"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"assistant","sessionId":"claude-merge","cwd":"/tmp/project","uuid":"a3","message":{"content":[{"type":"text","text":"third block"}]}}"#,
            ],
        );

        let messages = load_claude_messages(&file)
            .expect("claude archive should parse")
            .messages;

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "hello");
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "first block\n\n---\n\nsecond block");
        assert_eq!(messages[2].role, MessageRole::User);
        assert_eq!(messages[2].content, "follow up");
        assert_eq!(messages[3].role, MessageRole::Assistant);
        assert_eq!(messages[3].content, "third block");
    }

    #[test]
    fn load_claude_messages_merges_across_tool_results() {
        let file = temp_jsonl_file(
            "claude-tool-merge",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"user","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"u1","message":{"content":"do something"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"assistant","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"a1","message":{"content":[{"type":"thinking","thinking":"hidden"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"assistant","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"a2","message":{"content":[{"type":"text","text":"let me check"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"assistant","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"a3","message":{"content":[{"type":"tool_use","id":"tu1","name":"bash","input":{}}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"user","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"u2","message":{"content":[{"type":"tool_result","tool_use_id":"tu1","content":"ok"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"assistant","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"a4","message":{"content":[{"type":"thinking","thinking":"hidden"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:06Z","type":"assistant","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"a5","message":{"content":[{"type":"text","text":"looks good"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:07Z","type":"user","sessionId":"claude-tr","cwd":"/tmp/project","uuid":"u3","message":{"content":"thanks"}}"#,
            ],
        );

        let messages = load_claude_messages(&file)
            .expect("claude archive should parse")
            .messages;

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "do something");
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "let me check\n\n---\n\nlooks good");
        assert_eq!(messages[2].role, MessageRole::User);
        assert_eq!(messages[2].content, "thanks");
    }

    fn temp_jsonl_file(name: &str, lines: &[&str]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "omni-code-bridge-test-{}-{}",
            name,
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join(format!("{name}.jsonl"));
        std::fs::write(&path, lines.join("\n")).expect("write jsonl");
        path
    }
}
