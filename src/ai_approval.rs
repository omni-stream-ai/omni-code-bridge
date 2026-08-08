use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::{
    bridge_settings::{AiApprovalSettings, BridgeSettings, settings_path},
    models::{ApprovalKind, ApprovalRequest},
};

const DEFAULT_MODEL: &str = "gpt-4.1-mini";
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const AI_APPROVAL_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiApprovalDecisionKind {
    Accept,
    Decline,
    AskUser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AiApprovalRisk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AiApprovalReasonKind {
    #[default]
    AiReview,
    RiskThreshold,
    HardBlock,
}

impl AiApprovalReasonKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AiReview => "ai_review",
            Self::RiskThreshold => "risk_threshold",
            Self::HardBlock => "hard_block",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AiApprovalDecision {
    pub decision: AiApprovalDecisionKind,
    pub risk: AiApprovalRisk,
    pub reason: String,
    #[serde(skip, default)]
    pub reason_kind: AiApprovalReasonKind,
}

#[derive(Debug, Clone)]
struct AiApprovalConfig {
    enabled: bool,
    base_url: String,
    api_key: String,
    model: String,
    max_auto_risk: AiApprovalRisk,
    prompt: String,
    project_prompt: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessageRequest<'a>>,
    temperature: f32,
    response_format: ResponseFormat,
}

#[derive(Debug, Serialize)]
struct ChatMessageRequest<'a> {
    role: &'a str,
    content: String,
}

#[derive(Debug, Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatCompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionMessage,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionMessage {
    content: String,
}

pub fn is_enabled() -> bool {
    load_file_settings()
        .map(|settings| settings.ai_approval.enabled)
        .unwrap_or_else(|| env_bool("OMNI_CODE_AI_APPROVAL"))
}

pub async fn review_request(
    request: &ApprovalRequest,
    project_root: &Path,
) -> Result<Option<AiApprovalDecision>> {
    let Some(command) = request
        .command
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if !is_enabled() {
        return Ok(None);
    }
    if is_hard_blocked(command, project_root) {
        return Ok(Some(AiApprovalDecision {
            decision: AiApprovalDecisionKind::AskUser,
            risk: AiApprovalRisk::High,
            reason: "Command matched a hard safety block".to_string(),
            reason_kind: AiApprovalReasonKind::HardBlock,
        }));
    }

    let config = AiApprovalConfig::from_settings_or_env(project_root)?;
    if !config.enabled {
        return Ok(None);
    }
    let decision = call_openai_compatible(&config, request, command, project_root).await?;
    if decision.decision == AiApprovalDecisionKind::Accept && decision.risk > config.max_auto_risk {
        return Ok(Some(AiApprovalDecision {
            decision: AiApprovalDecisionKind::AskUser,
            risk: decision.risk,
            reason: decision.reason,
            reason_kind: AiApprovalReasonKind::RiskThreshold,
        }));
    }
    Ok(Some(decision))
}

impl AiApprovalConfig {
    fn from_settings_or_env(project_root: &Path) -> Result<Self> {
        if let Some(settings) = load_file_settings() {
            let project = settings
                .project_ai_approval
                .get(&project_root.to_string_lossy().to_string())
                .cloned()
                .unwrap_or_default();
            return Self::from_ai_settings(settings.ai_approval, project);
        }
        Self::from_env()
    }

    fn from_env() -> Result<Self> {
        let api_key = std::env::var("OMNI_CODE_AI_APPROVAL_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .context(
                "OMNI_CODE_AI_APPROVAL_API_KEY or OPENAI_API_KEY is required for AI approval",
            )?;
        let base_url = std::env::var("OMNI_CODE_AI_APPROVAL_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let model = std::env::var("OMNI_CODE_AI_APPROVAL_MODEL")
            .unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        let max_auto_risk = parse_risk_env("OMNI_CODE_AI_APPROVAL_MAX_RISK", AiApprovalRisk::Low)?;
        Ok(Self {
            enabled: env_bool("OMNI_CODE_AI_APPROVAL"),
            base_url,
            api_key,
            model,
            max_auto_risk,
            prompt: String::new(),
            project_prompt: String::new(),
        })
    }

    fn from_ai_settings(
        settings: AiApprovalSettings,
        project: crate::bridge_settings::ProjectAiApprovalSettings,
    ) -> Result<Self> {
        Ok(Self {
            enabled: settings.enabled,
            base_url: settings.base_url.trim().trim_end_matches('/').to_string(),
            api_key: settings.api_key.trim().to_string(),
            model: settings.model.trim().to_string(),
            max_auto_risk: parse_risk_value(&settings.max_risk)?,
            prompt: settings.prompt,
            project_prompt: project.prompt,
        })
    }
}

fn load_file_settings() -> Option<BridgeSettings> {
    let body = std::fs::read_to_string(settings_path()).ok()?;
    serde_json::from_str(&body).ok()
}

fn parse_risk_env(name: &str, default: AiApprovalRisk) -> Result<AiApprovalRisk> {
    let Ok(value) = std::env::var(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "low" => Ok(AiApprovalRisk::Low),
        "medium" => Ok(AiApprovalRisk::Medium),
        "high" => Ok(AiApprovalRisk::High),
        other => bail!("invalid {name}: {other}"),
    }
}

fn parse_risk_value(value: &str) -> Result<AiApprovalRisk> {
    match value.trim().to_ascii_lowercase().as_str() {
        "low" => Ok(AiApprovalRisk::Low),
        "medium" => Ok(AiApprovalRisk::Medium),
        "high" => Ok(AiApprovalRisk::High),
        other => bail!("invalid AI approval max risk: {other}"),
    }
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

async fn call_openai_compatible(
    config: &AiApprovalConfig,
    request: &ApprovalRequest,
    command: &str,
    project_root: &Path,
) -> Result<AiApprovalDecision> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(AI_APPROVAL_TIMEOUT)
        .build()
        .context("failed to build AI approval HTTP client")?;
    let body = ChatCompletionRequest {
        model: &config.model,
        messages: vec![
            ChatMessageRequest {
                role: "system",
                content: system_prompt(config),
            },
            ChatMessageRequest {
                role: "user",
                content: user_prompt(request, command, project_root),
            },
        ],
        temperature: 0.0,
        response_format: ResponseFormat {
            kind: "json_object",
        },
    };

    let response = client
        .post(format!("{}/chat/completions", config.base_url))
        .header(AUTHORIZATION, format!("Bearer {}", config.api_key))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .context("failed to call AI approval endpoint")?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read AI approval response")?;
    if !status.is_success() {
        bail!("AI approval request failed with {status}: {text}");
    }

    let completion: ChatCompletionResponse = serde_json::from_str(&text)
        .context("failed to parse AI approval chat completion response")?;
    let content = completion
        .choices
        .first()
        .map(|choice| choice.message.content.trim())
        .filter(|content| !content.is_empty())
        .context("AI approval response did not include message content")?;
    let decision: AiApprovalDecision =
        serde_json::from_str(content).context("failed to parse AI approval decision JSON")?;
    Ok(normalize_decision(decision))
}

fn normalize_decision(mut decision: AiApprovalDecision) -> AiApprovalDecision {
    if decision.reason.trim().is_empty() {
        decision.reason = "No reason provided".to_string();
    }
    decision
}

fn system_prompt(config: &AiApprovalConfig) -> String {
    let mut prompt = "You are a conservative command approval reviewer for a coding agent. Return only JSON with keys decision, risk, reason. decision must be one of accept, decline, ask_user. risk must be one of low, medium, high. Accept only commands that are clearly project-local, reversible, and routine for software development, and assign their actual risk level. Ask the user for anything ambiguous, destructive, credential-related, system-level, network-installing, or outside the project. Decline commands that are clearly malicious or destructive. Hard safety rules cannot be overridden. Global instructions supplement these rules. Project instructions take precedence over conflicting global instructions, but cannot override hard safety rules. Write reason as a concise, natural, user-facing explanation with no labels or boilerplate. Use the same language as the request Reason when that language is detectable.".to_string();
    if !config.prompt.trim().is_empty() {
        prompt.push_str("\n\nGlobal approval instructions:\n");
        prompt.push_str(config.prompt.trim());
    }
    if !config.project_prompt.trim().is_empty() {
        prompt.push_str("\n\nProject approval instructions:\n");
        prompt.push_str(config.project_prompt.trim());
    }
    prompt
}

fn user_prompt(request: &ApprovalRequest, command: &str, project_root: &Path) -> String {
    let kind = match request.kind {
        ApprovalKind::CommandExecution => "command_execution",
        ApprovalKind::ExecCommand => "exec_command",
        ApprovalKind::FileChange => "file_change",
        ApprovalKind::ApplyPatch => "apply_patch",
        ApprovalKind::Permissions => "permissions",
    };
    format!(
        "Review this approval request.\nProject root: {}\nKind: {}\nReason: {}\nCommand: {}\n\nReturn JSON only, for example: {{\"decision\":\"ask_user\",\"risk\":\"medium\",\"reason\":\"...\"}}",
        project_root.display(),
        kind,
        request.reason.as_deref().unwrap_or(""),
        command
    )
}

pub fn is_hard_blocked(command: &str, project_root: &Path) -> bool {
    let lower = command.to_ascii_lowercase();
    if [
        "sudo",
        "su ",
        "chmod 777",
        "chown ",
        "mkfs",
        "dd if=",
        "private_key",
        "id_rsa",
        "authorized_keys",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        return true;
    }

    if lower.contains("rm -rf /")
        || lower.contains("rm -fr /")
        || rm_mentions_path_escape(command, project_root)
    {
        return true;
    }

    if (lower.contains("curl ") || lower.contains("wget "))
        && (lower.contains(" | sh")
            || lower.contains(" | bash")
            || lower.contains("bash -c")
            || lower.contains("sh -c"))
    {
        return true;
    }

    mentions_absolute_path_outside_project(command, project_root)
}

fn rm_mentions_path_escape(command: &str, project_root: &Path) -> bool {
    let mut tokens = command.split_whitespace();
    let Some(program) = tokens.next() else {
        return false;
    };
    if program != "rm" {
        return false;
    }

    tokens
        .filter(|token| !token.starts_with('-'))
        .any(|token| path_escapes_project(token, project_root))
}

fn path_escapes_project(raw: &str, project_root: &Path) -> bool {
    let trimmed = raw.trim_matches(|ch: char| {
        matches!(ch, '\'' | '"' | ',' | ')' | '(' | '[' | ']' | '{' | '}')
    });
    if trimmed.is_empty() {
        return false;
    }
    let path = PathBuf::from(trimmed);
    let candidate = if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    };
    normalize_without_fs(&candidate)
        .map(|normalized| !normalized.starts_with(project_root))
        .unwrap_or(true)
}

fn normalize_without_fs(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::RootDir => normalized.push(std::path::MAIN_SEPARATOR.to_string()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            std::path::Component::Normal(part) => normalized.push(part),
            std::path::Component::Prefix(_) => return None,
        }
    }

    Some(normalized)
}

fn mentions_absolute_path_outside_project(command: &str, project_root: &Path) -> bool {
    command.split_whitespace().any(|token| {
        let trimmed = token.trim_matches(|ch: char| {
            matches!(ch, '\'' | '"' | ',' | ')' | '(' | '[' | ']' | '{' | '}')
        });
        if !trimmed.starts_with('/') {
            return false;
        }
        let path = PathBuf::from(trimmed);
        let Some(normalized_root) = normalize_without_fs(project_root) else {
            return true;
        };
        let Some(normalized_path) = normalize_without_fs(&path) else {
            return true;
        };
        !normalized_path.starts_with(normalized_root)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_prompt_combines_global_and_project_instructions() {
        let config = AiApprovalConfig {
            enabled: true,
            base_url: String::new(),
            api_key: String::new(),
            model: String::new(),
            max_auto_risk: AiApprovalRisk::Low,
            prompt: "Follow company policy".to_string(),
            project_prompt: "Allow repository checks unless a hard safety block applies"
                .to_string(),
        };

        let prompt = system_prompt(&config);
        assert!(prompt.contains("Follow company policy"));
        assert!(prompt.contains("Allow repository checks"));
        assert!(prompt.contains("unless a hard safety block applies"));
        assert!(prompt.contains("concise, natural, user-facing explanation"));
        assert!(prompt.contains("same language as the request Reason"));
        assert!(prompt.contains("Project instructions take precedence"));
    }

    #[test]
    fn model_decision_defaults_to_ai_review_reason_kind() {
        let decision: AiApprovalDecision = serde_json::from_value(serde_json::json!({
            "decision": "ask_user",
            "risk": "medium",
            "reason": "这个操作会访问项目外部资源。"
        }))
        .unwrap();

        assert_eq!(decision.reason_kind, AiApprovalReasonKind::AiReview);
        assert_eq!(decision.reason, "这个操作会访问项目外部资源。");
    }

    #[test]
    fn normalization_preserves_medium_and_high_accept_risk() {
        let medium = normalize_decision(AiApprovalDecision {
            decision: AiApprovalDecisionKind::Accept,
            risk: AiApprovalRisk::Medium,
            reason: "Routine but moderate risk".to_string(),
            reason_kind: AiApprovalReasonKind::AiReview,
        });
        let high = normalize_decision(AiApprovalDecision {
            decision: AiApprovalDecisionKind::Accept,
            risk: AiApprovalRisk::High,
            reason: "High risk".to_string(),
            reason_kind: AiApprovalReasonKind::AiReview,
        });

        assert_eq!(medium.decision, AiApprovalDecisionKind::Accept);
        assert_eq!(high.decision, AiApprovalDecisionKind::Accept);
    }

    #[test]
    fn hard_blocks_detect_dangerous_commands_before_auto_approval() {
        let root = Path::new("/workspace/project");

        assert!(is_hard_blocked("sudo rm -rf /tmp", root));
        assert!(is_hard_blocked("cat /etc/passwd", root));
        assert!(!is_hard_blocked("cat ./README.md", root));
    }
}
