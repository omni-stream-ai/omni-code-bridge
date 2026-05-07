use std::{
    collections::HashMap,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::models::{
    AgentKind, ChatMessage, MessageRole, ProjectSummary, SessionStatus, SessionSummary,
};

pub struct SessionArchiveSummary {
    pub fingerprint: u64,
    pub projects: HashMap<String, ProjectSummary>,
    pub sessions: HashMap<String, SessionSummary>,
    pub session_files: HashMap<String, PathBuf>,
}

pub struct SessionMessages {
    pub fingerprint: u64,
    pub messages: Vec<ChatMessage>,
}

struct PendingAssistantMessage {
    created_at: DateTime<Utc>,
    blocks: Vec<String>,
}

struct ParsedSessionSummaryRecord {
    project: ProjectSummary,
    session: SessionSummary,
    session_file: PathBuf,
}

pub fn load_session_archive_summary() -> SessionArchiveSummary {
    let root = sessions_root();
    let (paths, fingerprint) = match collect_jsonl_files(&root) {
        Ok(result) => result,
        Err(_) => {
            return SessionArchiveSummary {
                fingerprint: 0,
                projects: HashMap::new(),
                sessions: HashMap::new(),
                session_files: HashMap::new(),
            };
        }
    };

    let mut archive = SessionArchiveSummary {
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

pub fn load_session_messages(path: &Path) -> Option<SessionMessages> {
    let fingerprint = file_fingerprint(path)?;
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut messages = Vec::new();
    let mut pending_assistant: Option<PendingAssistantMessage> = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now);

        match line_type {
            "session_meta" => {
                session_id = payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_started") => {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_complete") => {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("user_message") => {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
                let text = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(ToString::to_string);
                if let Some(text) = text {
                    messages.push(ChatMessage {
                        id: format!(
                            "{}-user-{}",
                            session_id.as_deref().unwrap_or("unknown"),
                            messages.len()
                        ),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::User,
                        content: text,
                        created_at: timestamp,
                    });
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("assistant") =>
            {
                if let Some(text) = extract_assistant_text(payload) {
                    let pending =
                        pending_assistant.get_or_insert_with(|| PendingAssistantMessage {
                            created_at: timestamp,
                            blocks: Vec::new(),
                        });
                    pending.blocks.push(text);
                }
            }
            _ => {}
        }
    }

    flush_pending_assistant(
        &mut messages,
        &session_id.clone().unwrap_or_default(),
        &mut pending_assistant,
    );

    let session_id = session_id?;
    for message in &mut messages {
        message.session_id = session_id.clone();
    }

    Some(SessionMessages {
        fingerprint,
        messages,
    })
}

fn flush_pending_assistant(
    messages: &mut Vec<ChatMessage>,
    session_id: &str,
    pending_assistant: &mut Option<PendingAssistantMessage>,
) {
    let Some(pending) = pending_assistant.take() else {
        return;
    };
    let content = pending
        .blocks
        .into_iter()
        .map(|block| block.trim().to_string())
        .filter(|block| !block.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    if content.is_empty() {
        return;
    }
    messages.push(ChatMessage {
        id: format!("{}-assistant-{}", session_id, messages.len()),
        session_id: session_id.to_string(),
        role: MessageRole::Assistant,
        content,
        created_at: pending.created_at,
    });
}

fn parse_session_summary_file(path: &Path) -> Option<ParsedSessionSummaryRecord> {
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut cwd = None;
    let mut updated_at = None;
    let mut user_message_candidates = Vec::new();
    let mut last_preview = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp);

        match line_type {
            "session_meta" => {
                session_id = payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                cwd = payload
                    .get("cwd")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                updated_at = timestamp.or_else(|| {
                    payload
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .and_then(parse_timestamp)
                });
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("user_message") => {
                let text = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(ToString::to_string);
                if let Some(text) = text {
                    user_message_candidates.push(text.clone());
                    last_preview = Some(text);
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("assistant") =>
            {
                if let Some(text) = extract_assistant_text(payload) {
                    last_preview = Some(text);
                }
            }
            _ => {}
        }

        if let Some(timestamp) = timestamp {
            updated_at = Some(updated_at.map_or(timestamp, |current| current.max(timestamp)));
        }
    }

    let session_id = session_id?;
    let cwd = cwd?;
    let updated_at = updated_at.unwrap_or_else(Utc::now);
    let project_id = project_id_for_path(&cwd);
    let session_title = preferred_session_title(&user_message_candidates)
        .map(|text| truncate_preview(&text, 32))
        .unwrap_or_else(|| file_stem_or_unknown(path));

    Some(ParsedSessionSummaryRecord {
        project: ProjectSummary {
            id: project_id.clone(),
            name: project_name_from_path(&cwd),
            root_path: cwd.clone(),
            updated_at,
            session_count: 1,
            last_session_preview: last_preview.clone(),
        },
        session: SessionSummary {
            id: session_id,
            project_id,
            title: session_title,
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Waiting,
            updated_at,
            unread_count: 0,
            last_message_preview: last_preview,
            pending_approval: None,
        },
        session_file: path.to_path_buf(),
    })
}

fn preferred_session_title(messages: &[String]) -> Option<String> {
    let mut fallback = None;

    for message in messages {
        let normalized = normalize_title_candidate(message);
        if normalized.is_empty() {
            continue;
        }
        if fallback.is_none() {
            fallback = Some(normalized.clone());
        }
        if !is_generic_title_candidate(&normalized) {
            return Some(normalized);
        }
    }

    fallback
}

fn normalize_title_candidate(text: &str) -> String {
    text.replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '`')
        .to_string()
}

fn is_generic_title_candidate(text: &str) -> bool {
    matches!(
        text,
        "继续"
            | "继续。"
            | "继续做"
            | "继续吧"
            | "继续一下"
            | "继续处理"
            | "继续帮我做"
            | "继续执行"
    )
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

fn extract_assistant_text(payload: &Value) -> Option<String> {
    payload
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| match item.get("type").and_then(Value::as_str) {
                    Some("output_text") => item.get("text").and_then(Value::as_str),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn sessions_root() -> PathBuf {
    std::env::var("ECHO_MATE_CODEX_SESSIONS_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".codex/sessions")))
        .unwrap_or_else(|_| PathBuf::from(".codex/sessions"))
}

pub fn project_id_for_path(path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn project_name_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

fn file_stem_or_unknown(path: &Path) -> String {
    path.file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("codex-session")
        .to_string()
}

pub fn truncate_preview(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        trimmed.to_string()
    } else {
        chars[..max_chars].iter().collect::<String>()
    }
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
    fn load_session_messages_parses_codex_jsonl_archive() {
        let file = temp_jsonl_file(
            "codex-session",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-1","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"hello codex"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello "},{"type":"output_text","text":"user"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete"}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].session_id, "codex-1");
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "hello codex");
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "hello user");
    }

    #[test]
    fn parse_session_summary_file_builds_codex_project_and_session() {
        let file = temp_jsonl_file(
            "codex-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-2","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"implement feature"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.id, "codex-2");
        assert_eq!(record.session.agent, AgentKind::Codex);
        assert_eq!(record.session.title, "implement feature");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
        assert_eq!(record.project.name, "example-app");
        assert_eq!(record.project.session_count, 1);
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
