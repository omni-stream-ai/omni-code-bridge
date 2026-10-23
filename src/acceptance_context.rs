//! Bounded, chronological inputs for the acceptance Agent's requirement extraction.
use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    acceptance::{AcceptanceCheck, parse_json_value},
    models::{ChatMessage, MessageRole},
};

const CHUNK_CHARS: usize = 18_000;
// Even JSON-escaped control characters fit comfortably inside a chunk.
const USER_PART_CHARS: usize = 2_000;
const ASSISTANT_CHARS: usize = 1_200;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptanceContext {
    pub chunks: Vec<String>,
    pub source_message_ids: HashSet<String>,
    pub user_message_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequirementBrief {
    pub requirements: Vec<AcceptanceRequirement>,
    #[serde(default)]
    pub notes: String,
    #[serde(default)]
    pub source_message_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptanceRequirement {
    pub id: String,
    pub description: String,
    pub source_message_ids: Vec<String>,
}

/// User messages are kept in full. Long inputs are split across bounded Agent
/// turns, rather than dropping earlier requirements. Assistant replies are
/// supporting context only; executable tool transcripts are not requirements.
pub fn build_context(messages: Vec<ChatMessage>, explicit: Option<&str>) -> AcceptanceContext {
    let mut messages = messages;
    messages.sort_by_key(|message| message.created_at);
    let mut context = AcceptanceContext {
        chunks: Vec::new(),
        source_message_ids: HashSet::new(),
        user_message_count: 0,
    };
    let mut current = String::new();
    for message in messages {
        let content = message.content.trim();
        if content.is_empty() || message.role == MessageRole::System {
            continue;
        }
        let (role, text) = match message.role {
            MessageRole::User => {
                context.source_message_ids.insert(message.id.clone());
                context.user_message_count += 1;
                ("user", content.to_string())
            }
            MessageRole::Assistant => {
                let text = assistant_excerpt(content);
                if text.is_empty() {
                    continue;
                }
                ("assistant_supporting_context", text)
            }
            MessageRole::System => continue,
        };
        for (part, text) in split_text(&text, USER_PART_CHARS).into_iter().enumerate() {
            let entry = serde_json::json!({
                "message_id": message.id,
                "role": role,
                "created_at": message.created_at,
                "part": part + 1,
                "text": text,
            });
            append_chunk(&mut context.chunks, &mut current, &entry.to_string());
        }
    }
    if let Some(explicit) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        context.source_message_ids.insert("acceptance-input".into());
        for (part, text) in split_text(explicit, USER_PART_CHARS)
            .into_iter()
            .enumerate()
        {
            let entry = serde_json::json!({
                "message_id": "acceptance-input", "role": "user",
                "priority": "explicit_acceptance_scope", "part": part + 1, "text": text,
            });
            append_chunk(&mut context.chunks, &mut current, &entry.to_string());
        }
    }
    if !current.is_empty() {
        context.chunks.push(current);
    }
    context
}

fn append_chunk(chunks: &mut Vec<String>, current: &mut String, entry: &str) {
    if !current.is_empty() && current.chars().count() + entry.chars().count() + 1 > CHUNK_CHARS {
        chunks.push(std::mem::take(current));
    }
    current.push_str(entry);
    current.push('\n');
}

fn split_text(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut chars = text.chars();
    loop {
        let chunk = chars.by_ref().take(limit).collect::<String>();
        if chunk.is_empty() {
            break;
        }
        chunks.push(chunk);
    }
    chunks
}

fn assistant_excerpt(text: &str) -> String {
    let text = text
        .split("\n\n---\n\n")
        .filter(|block| !block.trim_start().starts_with("[tool:"))
        .collect::<Vec<_>>()
        .join("\n\n");
    let chars = text.chars().collect::<Vec<_>>();
    if chars.len() <= ASSISTANT_CHARS {
        return text;
    }
    format!(
        "{}\n[Agent 回复中段省略；仅供参考，不作为用户需求或通过证据]\n{}",
        chars[..300].iter().collect::<String>(),
        chars[chars.len() - 900..].iter().collect::<String>()
    )
}

pub fn parse_brief(text: &str, context: &AcceptanceContext) -> Result<RequirementBrief> {
    let mut brief: RequirementBrief = serde_json::from_value(parse_json_value(text)?)
        .context("Agent returned invalid requirement summary")?;
    if brief.requirements.len() > 32 || serde_json::to_string(&brief)?.chars().count() > 16_000 {
        bail!("需求摘要过长，请合并同一功能的要求，最多 32 项且 JSON 不超过 16000 字符");
    }
    let mut ids = HashSet::new();
    for requirement in &brief.requirements {
        if requirement.id.trim().is_empty()
            || requirement.id.len() > 64
            || !ids.insert(&requirement.id)
            || requirement.description.trim().is_empty()
            || requirement.description.chars().count() > 2_000
            || requirement.source_message_ids.is_empty()
            || requirement
                .source_message_ids
                .iter()
                .any(|id| !context.source_message_ids.contains(id))
        {
            bail!("验收需求缺少有效的用户消息来源或包含重复编号");
        }
    }
    if brief.notes.chars().count() > 2_000 {
        bail!("验收需求备注过长");
    }
    brief.source_message_count = context.user_message_count;
    Ok(brief)
}

pub fn validate_coverage(checks: &[AcceptanceCheck], brief: &RequirementBrief) -> Result<()> {
    let requirements = brief
        .requirements
        .iter()
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    let mut covered = HashSet::new();
    for check in checks {
        if check.requirement_ids.is_empty() {
            bail!("验收项 {} 未关联用户需求 requirement_ids", check.id);
        }
        for id in &check.requirement_ids {
            if !requirements.contains(id.as_str()) {
                bail!("验收项 {} 引用了不存在的需求 {}", check.id, id);
            }
            covered.insert(id.as_str());
        }
    }
    let mut missing = requirements
        .difference(&covered)
        .copied()
        .collect::<Vec<_>>();
    missing.sort_unstable();
    if !missing.is_empty() {
        bail!("验收计划遗漏了用户需求：{}", missing.join(", "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeDelta, Utc};

    fn message(id: &str, role: MessageRole, content: &str, offset: i64) -> ChatMessage {
        ChatMessage {
            id: id.into(),
            session_id: "source".into(),
            role,
            content: content.into(),
            created_at: Utc::now() + TimeDelta::seconds(offset),
        }
    }

    #[test]
    fn preserves_early_requirements_and_late_corrections_in_chronological_order() {
        let mut messages = vec![message("early", MessageRole::User, "需要登录和导出 CSV", 0)];
        for index in 1..50 {
            messages.push(message(
                &format!("reply-{index}"),
                MessageRole::Assistant,
                "正在实现",
                index,
            ));
        }
        messages.push(message(
            "correction",
            MessageRole::User,
            "取消 CSV，改成导出 PDF",
            60,
        ));
        let context = build_context(messages, Some("重点验证导出权限"));
        let text = context.chunks.join("\n");
        assert_eq!(context.user_message_count, 2);
        assert!(text.contains("需要登录和导出 CSV"));
        assert!(text.find("需要登录和导出 CSV").unwrap() < text.find("取消 CSV").unwrap());
        assert!(text.find("取消 CSV").unwrap() < text.find("重点验证导出权限").unwrap());
    }

    #[test]
    fn splits_large_unicode_user_messages_without_dropping_any_text() {
        let text = format!("原始目标{}末尾要求", "长需求🖥".repeat(12_000));
        let context = build_context(vec![message("long", MessageRole::User, &text, 0)], None);
        assert!(context.chunks.len() > 2);
        let restored = context
            .chunks
            .iter()
            .flat_map(|chunk| chunk.lines())
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).unwrap()["text"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<String>();
        assert_eq!(restored, text);
        assert!(
            context
                .chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= CHUNK_CHARS)
        );
    }

    #[test]
    fn json_escaping_cannot_exceed_the_context_chunk_budget() {
        let text = "\u{1}中文\"\\\n".repeat(5_000);
        let context = build_context(vec![message("escaped", MessageRole::User, &text, 0)], None);
        assert!(
            context
                .chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= CHUNK_CHARS)
        );
        let restored = context
            .chunks
            .iter()
            .flat_map(|chunk| chunk.lines())
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).unwrap()["text"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<String>();
        // Boundary whitespace is normalized once; the remaining content is complete.
        assert_eq!(restored, text.trim());
    }

    #[test]
    fn excludes_system_and_tool_noise_but_keeps_user_and_assistant_conclusion() {
        let context = build_context(
            vec![
                message("s", MessageRole::System, "internal-system-prompt", 0),
                message("u", MessageRole::User, "检查保存功能", 1),
                message(
                    "a",
                    MessageRole::Assistant,
                    "[tool:output]\nnoise\n\n---\n\n保存功能已实现",
                    2,
                ),
            ],
            None,
        );
        let text = context.chunks.join("\n");
        assert!(!text.contains("noise"));
        assert!(!text.contains("internal-system-prompt"));
        assert!(text.contains("保存功能已实现"));
        assert!(!context.source_message_ids.contains("a"));
    }

    #[test]
    fn rejects_invented_sources_and_uncovered_requirements() {
        let context = build_context(
            vec![message("u", MessageRole::User, "检查保存和重新打开", 0)],
            None,
        );
        assert!(parse_brief(r#"{"requirements":[{"id":"save","description":"保存","source_message_ids":["invented"]}]}"#, &context).is_err());
        let brief = parse_brief(r#"{"requirements":[{"id":"save","description":"保存","source_message_ids":["u"]},{"id":"open","description":"重新打开","source_message_ids":["u"]}]}"#, &context).unwrap();
        let mut checks = crate::acceptance::parse_plan(r#"{"checks":[{"id":"save","title":"保存","assertion":"保存成功","requirement_ids":["save"],"actions":[{"instruction":"observe"}]}]}"#).unwrap();
        assert!(validate_coverage(&checks, &brief).is_err());
        checks[0].requirement_ids.push("open".into());
        assert!(validate_coverage(&checks, &brief).is_ok());
    }
}
