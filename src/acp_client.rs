use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub use agent_client_protocol::schema::ProtocolVersion;

pub use agent_client_protocol::schema::v1::{
    CancelNotification, ClientCapabilities, ContentBlock, ContentChunk, FileSystemCapabilities,
    Implementation, InitializeRequest, LoadSessionRequest, NewSessionRequest, PromptRequest,
    PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};

pub use agent_client_protocol::schema::v1::{RequestPermissionRequest, RequestPermissionResponse};

#[derive(Debug)]
#[allow(dead_code)]
pub struct AcpTransport {
    next_request_id: u64,
}

#[allow(dead_code)]
impl AcpTransport {
    pub fn new() -> Self {
        Self { next_request_id: 1 }
    }

    pub async fn send_request(
        &mut self,
        writer: &mut (impl AsyncWrite + Unpin),
        method: &str,
        params: &serde_json::Value,
    ) -> Result<u64> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });
        write_json_line(writer, &payload).await?;
        Ok(request_id)
    }

    pub async fn send_notification(
        &mut self,
        writer: &mut (impl AsyncWrite + Unpin),
        method: &str,
        params: &serde_json::Value,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        write_json_line(writer, &payload).await
    }

    pub async fn send_response(
        &mut self,
        writer: &mut (impl AsyncWrite + Unpin),
        request_id: &str,
        result: &serde_json::Value,
    ) -> Result<()> {
        let id: serde_json::Value = request_id
            .parse::<u64>()
            .map(serde_json::Value::from)
            .unwrap_or_else(|_| serde_json::Value::String(request_id.to_string()));
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        });
        write_json_line(writer, &payload).await
    }
}

#[derive(Debug)]
pub struct AcpMessageParser;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawJsonRpcMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AcpIncomingMessage {
    Response {
        id: String,
        result: serde_json::Value,
    },
    Error {
        id: Option<String>,
        code: i64,
        message: String,
    },
    Notification {
        method: String,
        params: serde_json::Value,
    },
    Request {
        id: String,
        method: String,
        params: serde_json::Value,
    },
}

impl AcpMessageParser {
    #[allow(dead_code)]
    pub fn parse(line: &str) -> Result<AcpIncomingMessage> {
        let raw: RawJsonRpcMessage =
            serde_json::from_str(line).context("invalid JSON-RPC message")?;

        if let Some(method) = &raw.method {
            if raw.id.is_some() {
                Ok(AcpIncomingMessage::Request {
                    id: json_value_to_id_string(raw.id.as_ref().unwrap()),
                    method: method.clone(),
                    params: raw.params.unwrap_or(serde_json::Value::Null),
                })
            } else {
                Ok(AcpIncomingMessage::Notification {
                    method: method.clone(),
                    params: raw.params.unwrap_or(serde_json::Value::Null),
                })
            }
        } else if let Some(error) = raw.error {
            Ok(AcpIncomingMessage::Error {
                id: raw.id.as_ref().map(json_value_to_id_string),
                code: error.get("code").and_then(|v| v.as_i64()).unwrap_or(-1),
                message: error
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string(),
            })
        } else if let Some(id) = raw.id {
            Ok(AcpIncomingMessage::Response {
                id: json_value_to_id_string(&id),
                result: raw.result.unwrap_or(serde_json::Value::Null),
            })
        } else {
            bail!("unrecognized JSON-RPC message");
        }
    }

    pub fn parse_session_update(params: &serde_json::Value) -> Result<SessionNotification> {
        serde_json::from_value::<SessionNotification>(params.clone())
            .context("failed to parse session/update notification")
    }

    #[allow(dead_code)]
    pub fn parse_permission_request(
        params: &serde_json::Value,
    ) -> Result<RequestPermissionRequest> {
        serde_json::from_value::<RequestPermissionRequest>(params.clone())
            .context("failed to parse permission request")
    }

    pub fn parse_prompt_response(result: &serde_json::Value) -> Result<PromptResponse> {
        serde_json::from_value::<PromptResponse>(result.clone())
            .context("failed to parse prompt response")
    }
}

pub fn build_initialize_request(client_name: &str, client_version: &str) -> InitializeRequest {
    let fs = FileSystemCapabilities::new()
        .read_text_file(true)
        .write_text_file(true);
    let caps = ClientCapabilities::new().fs(fs).terminal(true);
    InitializeRequest::new(ProtocolVersion::V1)
        .client_capabilities(caps)
        .client_info(Implementation::new(client_name, client_version))
}

pub fn build_new_session_request(cwd: &str) -> NewSessionRequest {
    NewSessionRequest::new(cwd.to_string())
}

pub fn build_load_session_request(session_id: &str, cwd: &str) -> LoadSessionRequest {
    LoadSessionRequest::new(SessionId::new(session_id), cwd.to_string())
}

pub fn build_prompt_request(session_id: &str, text: &str) -> PromptRequest {
    PromptRequest::new(
        SessionId::new(session_id),
        vec![ContentBlock::Text(TextContent::new(text))],
    )
}

pub fn build_prompt_with_blocks(session_id: &str, blocks: Vec<ContentBlock>) -> PromptRequest {
    PromptRequest::new(SessionId::new(session_id), blocks)
}

pub fn make_text_block(text: impl Into<String>) -> ContentBlock {
    ContentBlock::Text(TextContent::new(text))
}

#[allow(dead_code)]
pub fn text_from_content_blocks(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(tc) => Some(tc.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

pub fn build_cancel_notification(session_id: &str) -> CancelNotification {
    CancelNotification::new(SessionId::new(session_id))
}

#[allow(dead_code)]
pub fn build_permission_response(
    outcome: agent_client_protocol::schema::v1::RequestPermissionOutcome,
) -> RequestPermissionResponse {
    RequestPermissionResponse::new(outcome)
}

pub fn acp_stop_reason_description(reason: &StopReason) -> &'static str {
    match *reason {
        StopReason::EndTurn => "Turn completed normally",
        StopReason::MaxTokens => "Token limit reached",
        StopReason::MaxTurnRequests => "Max turn requests exceeded",
        StopReason::Refusal => "Agent refused to continue",
        StopReason::Cancelled => "Turn was cancelled",
        _ => "Unknown stop reason",
    }
}

pub fn extract_text_from_update(update: &SessionUpdate) -> Option<String> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => text_from_content_chunk(chunk),
        SessionUpdate::UserMessageChunk(chunk) => text_from_content_chunk(chunk),
        SessionUpdate::AgentThoughtChunk(chunk) => text_from_content_chunk(chunk),
        _ => None,
    }
}

fn text_from_content_chunk(chunk: &ContentChunk) -> Option<String> {
    match &chunk.content {
        ContentBlock::Text(tc) => Some(tc.text.clone()),
        _ => None,
    }
}

pub fn serialize_to_json_value<T: Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).context("failed to serialize ACP message")
}

#[allow(dead_code)]
async fn write_json_line(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &serde_json::Value,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[allow(dead_code)]
fn json_value_to_id_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}
