use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{
    claude_store::load_claude_archive_summary,
    models::{AgentKind, SessionSummary},
    session_store::load_session_archive_summary,
};

#[derive(Debug, Clone)]
struct SessionTraceTarget {
    session: SessionSummary,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct TraceEntry {
    timestamp: DateTime<Utc>,
    request: Value,
    response: Option<Value>,
}

pub fn print_session_trace(query: &str, limit: usize) -> Result<()> {
    let target = find_session_target(query)?;
    let entries = match target.session.agent {
        AgentKind::Codex => parse_codex_trace_entries(&target.path)?,
        AgentKind::ClaudeCode => parse_claude_trace_entries(&target.path)?,
        AgentKind::OpenCode => bail!("session trace is not supported for OpenCode sessions yet"),
        AgentKind::Acp => bail!("session trace is not supported for ACP sessions yet"),
        AgentKind::Custom => bail!("session trace is not supported for Custom sessions"),
    };

    let requested = limit.max(1);
    let responded = entries
        .iter()
        .filter(|entry| entry.response.is_some())
        .cloned()
        .collect::<Vec<_>>();
    let display_entries = if responded.is_empty() {
        entries
    } else {
        responded
    };
    let count = display_entries.len().min(requested);
    let start = display_entries.len().saturating_sub(count);

    println!(
        "Session: {} [{}] agent={:?}",
        target.session.title, target.session.id, target.session.agent
    );
    println!("Transcript: {}", target.path.display());
    println!("Showing last {count} command/response pair(s)\n");

    for (index, entry) in display_entries.into_iter().skip(start).enumerate() {
        println!("#{} {}", index + 1, entry.timestamp.to_rfc3339());
        println!("command:");
        println!("{}", serde_json::to_string_pretty(&entry.request)?);
        println!("response:");
        match entry.response {
            Some(response) => println!("{}", serde_json::to_string_pretty(&response)?),
            None => println!("null"),
        }
        println!();
    }

    Ok(())
}

fn find_session_target(query: &str) -> Result<SessionTraceTarget> {
    let normalized = query.trim();
    if normalized.is_empty() {
        bail!("session query must not be empty");
    }

    let mut sessions = Vec::new();

    let codex = load_session_archive_summary();
    for (id, session) in codex.sessions {
        if let Some(path) = codex.session_files.get(&id) {
            sessions.push(SessionTraceTarget {
                session,
                path: path.clone(),
            });
        }
    }

    let claude = load_claude_archive_summary();
    for (id, session) in claude.sessions {
        if let Some(path) = claude.session_files.get(&id) {
            sessions.push(SessionTraceTarget {
                session,
                path: path.clone(),
            });
        }
    }

    if let Some(target) = sessions.iter().find(|item| item.session.id == normalized) {
        return Ok(target.clone());
    }

    let lower = normalized.to_ascii_lowercase();
    let exact_title_matches = sessions
        .iter()
        .filter(|item| item.session.title.to_ascii_lowercase() == lower)
        .cloned()
        .collect::<Vec<_>>();
    if exact_title_matches.len() == 1 {
        return Ok(exact_title_matches[0].clone());
    }
    if exact_title_matches.len() > 1 {
        return Err(ambiguous_session_error(normalized, &exact_title_matches));
    }

    let fuzzy_matches = sessions
        .iter()
        .filter(|item| item.session.title.to_ascii_lowercase().contains(&lower))
        .cloned()
        .collect::<Vec<_>>();
    if fuzzy_matches.len() == 1 {
        return Ok(fuzzy_matches[0].clone());
    }
    if fuzzy_matches.len() > 1 {
        return Err(ambiguous_session_error(normalized, &fuzzy_matches));
    }

    bail!("no session found for query: {normalized}")
}

fn ambiguous_session_error(query: &str, matches: &[SessionTraceTarget]) -> anyhow::Error {
    let mut lines = Vec::new();
    lines.push(format!("multiple sessions matched query: {query}"));
    lines.push("candidates:".to_string());
    for item in matches.iter().take(10) {
        lines.push(format!(
            "  - [{}] {:?} {}",
            item.session.id, item.session.agent, item.session.title
        ));
    }
    if matches.len() > 10 {
        lines.push(format!("  ... and {} more", matches.len() - 10));
    }
    anyhow::anyhow!(lines.join("\n"))
}

fn parse_codex_trace_entries(path: &Path) -> Result<Vec<TraceEntry>> {
    let content = fs::read_to_string(path)?;
    let mut pending = VecDeque::<(String, DateTime<Utc>, Value)>::new();
    let mut entries = Vec::new();

    for line in content.lines() {
        let value: Value = serde_json::from_str(line)?;
        if value.get("type").and_then(Value::as_str) != Some("response_item") {
            continue;
        }
        let Some(payload) = value.get("payload") else {
            continue;
        };
        let timestamp = parse_timestamp(value.get("timestamp"))
            .unwrap_or_else(|| parse_timestamp(payload.get("timestamp")).unwrap_or_else(Utc::now));
        match payload.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
                    pending.push_back((call_id.to_string(), timestamp, payload.clone()));
                }
            }
            Some("function_call_output") => {
                let call_id = payload
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let request_index = pending.iter().position(|(id, _, _)| id == call_id);
                let request = request_index
                    .and_then(|index| pending.remove(index))
                    .map(|(_, request_timestamp, request)| (request_timestamp, request));
                entries.push(TraceEntry {
                    timestamp: request
                        .as_ref()
                        .map(|(request_timestamp, _)| *request_timestamp)
                        .unwrap_or(timestamp),
                    request: request.map(|(_, request)| request).unwrap_or(Value::Null),
                    response: Some(payload.clone()),
                });
            }
            _ => {}
        }
    }

    while let Some((_, timestamp, request)) = pending.pop_front() {
        entries.push(TraceEntry {
            timestamp,
            request,
            response: None,
        });
    }

    Ok(entries)
}

fn parse_claude_trace_entries(path: &Path) -> Result<Vec<TraceEntry>> {
    let content = fs::read_to_string(path)?;
    let mut pending = VecDeque::<Value>::new();
    let mut entries = Vec::new();

    for line in content.lines() {
        let value: Value = serde_json::from_str(line)?;
        let timestamp = parse_timestamp(value.get("timestamp")).unwrap_or_else(Utc::now);
        match value.get("type").and_then(Value::as_str) {
            Some("tool_use") => pending.push_back(value),
            Some("tool_result") => {
                let request_index = pending.iter().position(|request| {
                    request.get("tool_name") == value.get("tool_name")
                        && request.get("tool_input") == value.get("tool_input")
                });
                let request = request_index
                    .and_then(|index| pending.remove(index))
                    .unwrap_or(Value::Null);
                entries.push(TraceEntry {
                    timestamp: parse_timestamp(request.get("timestamp")).unwrap_or(timestamp),
                    request,
                    response: Some(value),
                });
            }
            _ => {}
        }
    }

    while let Some(request) = pending.pop_front() {
        entries.push(TraceEntry {
            timestamp: parse_timestamp(request.get("timestamp")).unwrap_or_else(Utc::now),
            request,
            response: None,
        });
    }

    Ok(entries)
}

fn parse_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_codex_trace_entries_pairs_calls_with_outputs() {
        let path = test_file(
            "codex-trace",
            &[
                r#"{"timestamp":"2026-06-10T00:00:00Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}","call_id":"call-1"}}"#,
                r#"{"timestamp":"2026-06-10T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"ok"}}"#,
            ],
        );

        let entries = parse_codex_trace_entries(&path).expect("trace should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].request["name"], "exec_command");
        assert_eq!(
            entries[0]
                .response
                .as_ref()
                .and_then(|value| value.get("output"))
                .and_then(Value::as_str),
            Some("ok")
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_claude_trace_entries_pairs_tool_use_and_result() {
        let path = test_file(
            "claude-trace",
            &[
                r#"{"type":"tool_use","timestamp":"2026-06-10T00:00:00Z","tool_name":"grep","tool_input":{"pattern":"foo"}}"#,
                r#"{"type":"tool_result","timestamp":"2026-06-10T00:00:01Z","tool_name":"grep","tool_input":{"pattern":"foo"},"tool_output":{"truncated":false}}"#,
            ],
        );

        let entries = parse_claude_trace_entries(&path).expect("trace should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].request["tool_name"], "grep");
        assert_eq!(
            entries[0]
                .response
                .as_ref()
                .and_then(|value| value.get("tool_output"))
                .and_then(|value| value.get("truncated"))
                .and_then(Value::as_bool),
            Some(false)
        );

        let _ = fs::remove_file(path);
    }

    fn test_file(prefix: &str, lines: &[&str]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "omni-code-bridge-{prefix}-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        fs::write(&path, lines.join("\n")).expect("write temp trace");
        path
    }
}
