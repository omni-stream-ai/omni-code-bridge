use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::{
    bridge_settings::{AiApprovalSettings, BridgeSettings, settings_path},
    models::{ApprovalKind, ApprovalRequest},
};

const DEFAULT_MODEL: &str = "gpt-4.1-mini";
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

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

#[derive(Debug, Clone, Deserialize)]
pub struct AiApprovalDecision {
    pub decision: AiApprovalDecisionKind,
    pub risk: AiApprovalRisk,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct AiApprovalConfig {
    enabled: bool,
    base_url: String,
    api_key: String,
    model: String,
    max_auto_risk: AiApprovalRisk,
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
    AiApprovalConfig::from_settings_or_env()
        .map(|config| config.enabled)
        .unwrap_or(false)
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
    if has_hard_block(command, project_root) {
        return Ok(Some(AiApprovalDecision {
            decision: AiApprovalDecisionKind::AskUser,
            risk: AiApprovalRisk::High,
            reason: "Command matched a hard safety block".to_string(),
        }));
    }

    let config = AiApprovalConfig::from_settings_or_env()?;
    if !config.enabled {
        return Ok(None);
    }
    let decision = call_openai_compatible(&config, request, command, project_root).await?;
    if decision.decision == AiApprovalDecisionKind::Accept && decision.risk > config.max_auto_risk {
        return Ok(Some(AiApprovalDecision {
            decision: AiApprovalDecisionKind::AskUser,
            risk: decision.risk,
            reason: format!(
                "AI rated risk above auto-approval threshold: {}",
                decision.reason
            ),
        }));
    }
    Ok(Some(decision))
}

impl AiApprovalConfig {
    fn from_settings_or_env() -> Result<Self> {
        if let Some(settings) = load_file_settings() {
            return Self::from_ai_settings(settings.ai_approval);
        }
        Self::from_env()
    }

    fn from_env() -> Result<Self> {
        let api_key = std::env::var("ECHO_MATE_AI_APPROVAL_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .context(
                "ECHO_MATE_AI_APPROVAL_API_KEY or OPENAI_API_KEY is required for AI approval",
            )?;
        let base_url = std::env::var("ECHO_MATE_AI_APPROVAL_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let model = std::env::var("ECHO_MATE_AI_APPROVAL_MODEL")
            .unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        let max_auto_risk = parse_risk_env("ECHO_MATE_AI_APPROVAL_MAX_RISK", AiApprovalRisk::Low)?;
        Ok(Self {
            enabled: env_bool("ECHO_MATE_AI_APPROVAL"),
            base_url,
            api_key,
            model,
            max_auto_risk,
        })
    }

    fn from_ai_settings(settings: AiApprovalSettings) -> Result<Self> {
        Ok(Self {
            enabled: settings.enabled,
            base_url: settings.base_url.trim().trim_end_matches('/').to_string(),
            api_key: settings.api_key.trim().to_string(),
            model: settings.model.trim().to_string(),
            max_auto_risk: parse_risk_value(&settings.max_risk)?,
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
    let client = reqwest::Client::new();
    let body = ChatCompletionRequest {
        model: &config.model,
        messages: vec![
            ChatMessageRequest {
                role: "system",
                content: system_prompt(),
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
    if decision.decision == AiApprovalDecisionKind::Accept && decision.risk != AiApprovalRisk::Low {
        decision.decision = AiApprovalDecisionKind::AskUser;
    }
    decision
}

fn system_prompt() -> String {
    "You are a conservative command approval reviewer for a coding agent. Return only JSON with keys decision, risk, reason. decision must be one of accept, decline, ask_user. risk must be one of low, medium, high. Only accept low-risk commands that are clearly project-local, reversible, and routine for software development. Ask the user for anything ambiguous, destructive, credential-related, system-level, network-installing, or outside the project. Decline commands that are clearly malicious or destructive.".to_string()
}

fn user_prompt(request: &ApprovalRequest, command: &str, project_root: &Path) -> String {
    let kind = match request.kind {
        ApprovalKind::CommandExecution => "command_execution",
        ApprovalKind::ExecCommand => "exec_command",
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

fn has_hard_block(command: &str, project_root: &Path) -> bool {
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
        "/etc/",
        "/var/",
        "/usr/",
        "/bin/",
        "/sbin/",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        return true;
    }

    if lower.contains("rm -rf /") || lower.contains("rm -fr /") {
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

fn mentions_absolute_path_outside_project(command: &str, project_root: &Path) -> bool {
    command.split_whitespace().any(|token| {
        let trimmed = token.trim_matches(|ch: char| {
            matches!(ch, '\'' | '"' | ',' | ')' | '(' | '[' | ']' | '{' | '}')
        });
        if !trimmed.starts_with('/') {
            return false;
        }
        let path = PathBuf::from(trimmed);
        !path.starts_with(project_root)
    })
}
