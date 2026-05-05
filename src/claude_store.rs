use std::{
    collections::HashMap,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde_json::Value;

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
            "user" => {
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
                    messages.push(ChatMessage {
                        id: value
                            .get("uuid")
                            .and_then(Value::as_str)
                            .or_else(|| value.pointer("/message/id").and_then(Value::as_str))
                            .unwrap_or_default()
                            .to_string(),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::Assistant,
                        content: text,
                        created_at: timestamp,
                    });
                }
            }
            _ => {}
        }
    }

    let session_id = session_id?;
    for message in &mut messages {
        message.session_id = session_id.clone();
    }

    Some(ClaudeMessages {
        fingerprint,
        messages,
    })
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
                if let Some(text) = extract_user_text(&value) {
                    if first_user_message.is_none() {
                        first_user_message = Some(text.clone());
                    }
                    last_preview = Some(text);
                }
            }
            "assistant" => {
                if let Some(text) = extract_assistant_text(&value) {
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
        },
        session: SessionSummary {
            id: session_id,
            project_id,
            title: session_title,
            agent: AgentKind::ClaudeCode,
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

fn extract_user_text(value: &Value) -> Option<String> {
    let content = value.pointer("/message/content")?;
    if let Some(text) = content.as_str() {
        let text = text.trim();
        return (!text.is_empty()).then(|| text.to_string());
    }
    None
}

fn extract_assistant_text(value: &Value) -> Option<String> {
    let content = value.pointer("/message/content")?.as_array()?;
    let text = content
        .iter()
        .filter_map(|item| match item.get("type").and_then(Value::as_str) {
            Some("text") => item.get("text").and_then(Value::as_str),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
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
