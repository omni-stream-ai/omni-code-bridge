use pi::sdk::{AssistantMessage, ContentBlock, StopReason};
use serde_json::Value;

pub(super) const REPAIR_PROMPT: &str = "Your previous response could not be parsed as a complete JSON object. Answer the original request again using the JSON schema in the system instructions. Return exactly one complete JSON object, without Markdown fences, explanations or tool calls. Keep text fields concise. Escape newlines, quotation marks and backslashes inside JSON strings.";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OutputIssue {
    Empty,
    Truncated,
    InvalidJson { line: usize, column: usize },
    NotObject,
    Incomplete,
    Refused,
}

impl OutputIssue {
    pub(super) fn retryable(&self) -> bool {
        !matches!(self, Self::Incomplete | Self::Refused)
    }

    pub(super) fn message(&self) -> &'static str {
        match self {
            Self::Empty => {
                "Model returned no response text after retry; try again or choose another model"
            }
            Self::Truncated => {
                "Model response exceeded the output token limit after retry; reduce the request or choose another model"
            }
            Self::InvalidJson { .. } | Self::NotObject => {
                "Model returned invalid JSON after retry; try again or choose another model"
            }
            Self::Incomplete => "Model could not finish the response",
            Self::Refused => "Model declined the request",
        }
    }
}

pub(super) fn parse(reply: &AssistantMessage) -> Result<Value, OutputIssue> {
    match reply.stop_reason {
        // Even syntactically valid JSON from an unfinished response must not
        // apply settings, update task records or dispatch execution sessions.
        StopReason::Length => return Err(OutputIssue::Truncated),
        StopReason::Refusal => return Err(OutputIssue::Refused),
        StopReason::Stop => {}
        _ => return Err(OutputIssue::Incomplete),
    }
    let text = reply
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<String>();
    let text = text.trim();
    if text.is_empty() {
        return Err(OutputIssue::Empty);
    }
    // Accept only a complete JSON document or a single Markdown code block.
    // Never extract a nested object from prose or try to complete broken JSON.
    let text = text
        .strip_prefix("```")
        .and_then(|fenced| fenced.strip_suffix("```"))
        .and_then(|fenced| {
            let (language, body) = fenced.split_once('\n')?;
            (language.trim().is_empty() || language.trim().eq_ignore_ascii_case("json"))
                .then_some(body.trim())
        })
        .unwrap_or(text);
    let value: Value = serde_json::from_str(text).map_err(|error| OutputIssue::InvalidJson {
        line: error.line(),
        column: error.column(),
    })?;
    if !value.is_object() {
        return Err(OutputIssue::NotObject);
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
