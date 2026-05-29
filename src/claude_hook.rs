use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::models::{ApprovalChoice, ApprovalKind, ApprovalRequest};
use crate::approval_policy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudePermissionRequest {
    pub request_id: String,
    pub run_id: String,
    pub session_id: String,
    pub tool_name: Option<String>,
    pub tool_input: Value,
    pub permission_suggestions: Option<Value>,
    pub created_at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudePermissionResponse {
    pub request_id: String,
    pub behavior: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeHookStatusEvent {
    pub event_id: String,
    pub run_id: String,
    pub session_id: String,
    pub summary: String,
    pub created_at_unix_ms: u128,
}

pub fn claude_state_dir() -> PathBuf {
    env::temp_dir()
        .join("omni-code-bridge")
        .join("claude-permissions")
}

fn requests_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("requests")
}

fn responses_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("responses")
}

fn events_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("events")
}

pub fn request_path(state_dir: &Path, request_id: &str) -> PathBuf {
    requests_dir(state_dir).join(format!("{request_id}.json"))
}

pub fn response_path(state_dir: &Path, request_id: &str) -> PathBuf {
    responses_dir(state_dir).join(format!("{request_id}.json"))
}

pub fn event_path(state_dir: &Path, event_id: &str) -> PathBuf {
    events_dir(state_dir).join(format!("{event_id}.json"))
}

pub async fn ensure_runtime_dirs(state_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(requests_dir(state_dir)).await?;
    tokio::fs::create_dir_all(responses_dir(state_dir)).await?;
    tokio::fs::create_dir_all(events_dir(state_dir)).await?;
    Ok(())
}

pub async fn append_always_allow_command(state_dir: &Path, command: &str) -> Result<()> {
    let path = state_dir.join("always_allow_commands.json");
    let mut entries = match tokio::fs::read(&path).await {
        Ok(body) => serde_json::from_slice::<Vec<String>>(&body).unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    let command = command.trim().to_string();
    if !command.is_empty() && !entries.contains(&command) {
        entries.push(command);
        tokio::fs::write(path, serde_json::to_vec_pretty(&entries)?).await?;
    }
    Ok(())
}

async fn load_always_allow_commands(state_dir: &Path) -> Vec<String> {
    let path = state_dir.join("always_allow_commands.json");
    match tokio::fs::read(&path).await {
        Ok(body) => serde_json::from_slice::<Vec<String>>(&body).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

pub async fn run_permission_hook(
    state_dir: PathBuf,
    session_id: String,
    run_id: String,
    project_root: PathBuf,
) -> Result<()> {
    ensure_runtime_dirs(&state_dir).await?;

    let mut body = Vec::new();
    tokio::io::stdin().read_to_end(&mut body).await?;
    let payload = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice::<Value>(&body).context("failed to parse Claude hook input JSON")?
    };

    let hook_event_name = payload
        .get("hook_event_name")
        .and_then(Value::as_str)
        .or_else(|| payload.get("hookEventName").and_then(Value::as_str))
        .unwrap_or_default();
    let tool_name = payload
        .get("tool_name")
        .and_then(Value::as_str)
        .or_else(|| payload.get("toolName").and_then(Value::as_str));

    if let Some(summary) = summarize_hook_status_event(hook_event_name, tool_name, &payload) {
        let event = ClaudeHookStatusEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            summary,
            created_at_unix_ms: now_unix_ms(),
        };
        tokio::fs::write(
            event_path(&state_dir, &event.event_id),
            serde_json::to_vec_pretty(&event)?,
        )
        .await?;
    }

    if hook_event_name == "PostToolUse" {
        return Ok(());
    }

    let tool_input = payload
        .get("tool_input")
        .cloned()
        .or_else(|| payload.get("toolInput").cloned())
        .unwrap_or_else(|| payload.clone());
    let command = extract_command_from_value(&tool_input).unwrap_or_default();
    let auto_allow = load_always_allow_commands(&state_dir).await;
    if tool_name != Some("Bash")
        || is_auto_allowed_bash_command(&command, &auto_allow)
        || approval_policy::should_auto_approve(&command, &project_root).is_some()
    {
        write_hook_decision("allow", "Allowed by omni-code Claude hook").await?;
        return Ok(());
    }

    let request_id = uuid::Uuid::new_v4().to_string();
    let request = ClaudePermissionRequest {
        request_id: request_id.clone(),
        run_id,
        session_id,
        tool_name: tool_name.map(ToString::to_string),
        tool_input,
        permission_suggestions: payload.get("permission_suggestions").cloned(),
        created_at_unix_ms: now_unix_ms(),
    };
    tokio::fs::write(
        request_path(&state_dir, &request_id),
        serde_json::to_vec_pretty(&request)?,
    )
    .await?;

    let response = wait_for_permission_response(&state_dir, &request_id).await?;
    let _ = tokio::fs::remove_file(request_path(&state_dir, &request_id)).await;
    let _ = tokio::fs::remove_file(response_path(&state_dir, &request_id)).await;
    write_hook_response(&response).await?;
    Ok(())
}

async fn write_hook_decision(decision: &str, reason: &str) -> Result<()> {
    let payload = serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": decision,
            "permissionDecisionReason": reason,
        }
    });
    tokio::io::stdout()
        .write_all(serde_json::to_string(&payload)?.as_bytes())
        .await?;
    tokio::io::stdout().flush().await?;
    Ok(())
}

async fn write_hook_response(response: &ClaudePermissionResponse) -> Result<()> {
    let reason = match response.behavior.as_str() {
        "allow" if response.message.as_deref() == Some("accept_for_session") => {
            "Approved for this session"
        }
        "allow" => "Approved by omni-code",
        _ => response.message.as_deref().unwrap_or("Denied by omni-code"),
    };
    let decision = if response.behavior == "allow" {
        "allow"
    } else {
        "deny"
    };
    write_hook_decision(decision, reason).await
}

async fn wait_for_permission_response(
    state_dir: &Path,
    request_id: &str,
) -> Result<ClaudePermissionResponse> {
    let path = response_path(state_dir, request_id);
    loop {
        match tokio::fs::read(&path).await {
            Ok(body) => {
                return serde_json::from_slice(&body)
                    .with_context(|| format!("failed to parse {}", path.display()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub fn response_from_choice(choice: &ApprovalChoice) -> ClaudePermissionResponse {
    ClaudePermissionResponse {
        request_id: String::new(),
        behavior: match choice {
            ApprovalChoice::Accept
            | ApprovalChoice::AcceptForSession
            | ApprovalChoice::AlwaysAllow => "allow".to_string(),
            ApprovalChoice::Decline | ApprovalChoice::Cancel => "deny".to_string(),
        },
        message: match choice {
            ApprovalChoice::AcceptForSession => Some("accept_for_session".to_string()),
            ApprovalChoice::Decline | ApprovalChoice::Cancel => {
                Some("Denied by client approval".to_string())
            }
            _ => None,
        },
    }
}

impl ClaudePermissionRequest {
    pub fn as_approval_request(&self) -> ApprovalRequest {
        let command = extract_command_from_value(&self.tool_input);
        let reason = self
            .tool_input
            .get("message")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                self.tool_input
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .or_else(|| {
                self.tool_name
                    .as_ref()
                    .map(|tool_name| format!("Claude 请求执行 {tool_name}"))
            });

        ApprovalRequest {
            request_id: self.request_id.clone(),
            kind: if command.is_some() {
                ApprovalKind::ExecCommand
            } else {
                ApprovalKind::Permissions
            },
            command,
            reason,
            allow_accept_for_session: self
                .permission_suggestions
                .as_ref()
                .and_then(Value::as_array)
                .map(|items| {
                    items.iter().any(|item| {
                        item.get("destination")
                            .and_then(Value::as_str)
                            .map(|value| value == "session")
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false),
            allow_cancel: true,
            resolvable: true,
        }
    }
}


fn summarize_hook_status_event(
    hook_event_name: &str,
    tool_name: Option<&str>,
    payload: &Value,
) -> Option<String> {
    let tool_name = tool_name?;
    let input = payload
        .get("tool_input")
        .or_else(|| payload.get("toolInput"))
        .unwrap_or(payload);
    let phase = if hook_event_name == "PreToolUse" {
        "running"
    } else {
        "done"
    };
    match tool_name {
        "WebSearch" => extract_command_from_value(input)
            .map(|query| format!("[search] {phase}: {}", truncate(&query, 120))),
        "WebFetch" => extract_command_from_value(input)
            .map(|query| format!("[fetch] {phase}: {}", truncate(&query, 120))),
        "Bash" => summarize_bash_tool(input, phase),
        "TodoWrite" => summarize_todo_tool(input, phase),
        "Edit" | "MultiEdit" | "Write" => summarize_edit_tool(tool_name, input, phase),
        "Read" | "Glob" | "Grep" | "LS" | "Task" => summarize_generic_tool(tool_name, input, phase),
        _ => summarize_generic_tool(tool_name, input, phase),
    }
}

fn summarize_bash_tool(input: &Value, phase: &str) -> Option<String> {
    extract_command_from_value(input)
        .map(|command| format!("[command] {phase}: {}", truncate(&command, 120)))
}

fn summarize_todo_tool(input: &Value, phase: &str) -> Option<String> {
    let todos = input
        .get("todos")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("content")
                        .or_else(|| item.get("text"))
                        .or_else(|| item.get("title"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|text| !text.is_empty())
                        .map(ToString::to_string)
                })
                .take(3)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let count = input
        .get("todos")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(todos.len());
    Some(if todos.is_empty() {
        format!("[todo] {phase}: {count} items")
    } else {
        format!("[todo] {phase}: {} ({count} items)", todos.join(" | "))
    })
}

fn summarize_edit_tool(tool_name: &str, input: &Value, phase: &str) -> Option<String> {
    let path = input
        .get("file_path")
        .or_else(|| input.get("path"))
        .or_else(|| input.get("filePath"))
        .and_then(Value::as_str)
        .map(|path| truncate(path, 120));
    path.map(|path| format!("[{}] {phase}: {path}", tool_name.to_ascii_lowercase()))
}

fn summarize_generic_tool(tool_name: &str, input: &Value, phase: &str) -> Option<String> {
    let details = extract_command_from_value(input)
        .or_else(|| {
            input
                .get("file_path")
                .or_else(|| input.get("path"))
                .or_else(|| input.get("pattern"))
                .or_else(|| input.get("url"))
                .or_else(|| input.get("prompt"))
                .and_then(extract_command_from_value)
        })
        .map(|value| truncate(&value, 120))
        .unwrap_or_else(|| "working".to_string());
    Some(format!(
        "[{}] {phase}: {details}",
        tool_name.to_ascii_lowercase()
    ))
}

fn extract_command_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        Value::Array(items) => {
            let parts = items.iter().filter_map(Value::as_str).collect::<Vec<_>>();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(" "))
            }
        }
        Value::Object(map) => map
            .get("query")
            .and_then(extract_command_from_value)
            .or_else(|| map.get("url").and_then(extract_command_from_value))
            .or_else(|| map.get("command").and_then(extract_command_from_value))
            .or_else(|| map.get("commands").and_then(extract_command_from_value)),
        _ => None,
    }
}

fn truncate(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn prefix_matches(command: &str, pattern: &str) -> bool {
    if !pattern.contains('*') {
        return command == pattern || command.starts_with(&format!("{pattern} "));
    }
    let mut segments = pattern.split('*');
    let first = segments.next().unwrap_or("");
    if !command.starts_with(first) {
        return false;
    }
    let mut pos = first.len();
    for segment in segments {
        if segment.is_empty() {
            continue;
        }
        match command[pos..].find(segment) {
            Some(offset) => pos += offset + segment.len(),
            None => return false,
        }
    }
    let remaining = &command[pos..];
    remaining.is_empty() || remaining.starts_with(' ')
}

fn is_auto_allowed_bash_command(command: &str, dynamic_allow: &[String]) -> bool {
    const ALLOW_PREFIXES: &[&str] = &["date", "pwd"];
    ALLOW_PREFIXES.iter().any(|p| prefix_matches(command, p))
        || dynamic_allow.iter().any(|p| prefix_matches(command, p))
}

fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
