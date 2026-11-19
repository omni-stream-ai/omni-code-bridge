use super::*;
use crate::{models::ApiFormat, workspace_agent::complete_route};
use axum::{Json, Router, extract::State, routing::post};
use pi::sdk::TextContent;
use serde_json::json;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::Mutex;

fn reply(text: &str, stop_reason: StopReason) -> AssistantMessage {
    AssistantMessage {
        content: vec![ContentBlock::Text(TextContent::new(text))],
        stop_reason,
        ..Default::default()
    }
}

#[test]
fn accepts_complete_objects_and_single_json_fences() {
    for text in [
        r#" {"reply":"你好\n世界"} "#,
        "```json\n{\"reply\":\"你好\\n世界\"}\n```",
        "```JSON\r\n{\"reply\":\"你好\\n世界\"}\r\n```",
        "```\n{\"reply\":\"你好\\n世界\"}\n```",
    ] {
        assert_eq!(
            parse(&reply(text, StopReason::Stop)).unwrap(),
            json!({"reply":"你好\n世界"})
        );
    }
}

#[test]
fn rejects_prose_partial_json_and_multiple_documents() {
    for text in [
        r#"Here is the result: {"dispatch":[{"instruction":"do work"}]}"#,
        "```json\n{\"reply\":\"unfinished\"}",
        r#"{"reply":"unfinished"#,
        r#"{"reply":"one"} {"dispatch":[]}"#,
        "{\"reply\":\"literal\nnewline\"}",
    ] {
        assert!(matches!(
            parse(&reply(text, StopReason::Stop)),
            Err(OutputIssue::InvalidJson { .. })
        ));
    }
    for text in ["null", "[]", "42", r#""an answer""#] {
        assert_eq!(
            parse(&reply(text, StopReason::Stop)),
            Err(OutputIssue::NotObject)
        );
    }
}

#[test]
fn rejects_unfinished_or_refused_responses_even_when_json_is_valid() {
    for (stop, issue) in [
        (StopReason::Length, OutputIssue::Truncated),
        (StopReason::Error, OutputIssue::Incomplete),
        (StopReason::Aborted, OutputIssue::Incomplete),
        (StopReason::PauseTurn, OutputIssue::Incomplete),
        (StopReason::ToolUse, OutputIssue::Incomplete),
        (StopReason::Refusal, OutputIssue::Refused),
    ] {
        assert_eq!(parse(&reply(r#"{"dispatch":[{}]}"#, stop)), Err(issue));
    }
    assert_eq!(
        parse(&reply("  ", StopReason::Stop)),
        Err(OutputIssue::Empty)
    );
}

struct ScriptedReply {
    text: &'static str,
    truncated: bool,
}

#[derive(Default)]
struct ScriptedModel {
    replies: Mutex<VecDeque<ScriptedReply>>,
    requests: Mutex<Vec<Value>>,
    repair_delay: Duration,
}

async fn serve_reply(
    State(model): State<Arc<ScriptedModel>>,
    Json(request): Json<Value>,
) -> ([(&'static str, &'static str); 1], String) {
    let mut requests = model.requests.lock().await;
    let repair = !requests.is_empty();
    requests.push(request.clone());
    drop(requests);
    if repair {
        tokio::time::sleep(model.repair_delay).await;
    }
    let scripted = model
        .replies
        .lock()
        .await
        .pop_front()
        .expect("unexpected model attempt");
    let text = scripted.text;
    let events = if request.get("input").is_some() {
        vec![
            json!({"type":"response.output_text.delta","item_id":"msg_1","content_index":0,"delta":text}),
            json!({"type":if scripted.truncated { "response.incomplete" } else { "response.completed" },"response":{
                "incomplete_details":if scripted.truncated { json!({"reason":"max_output_tokens"}) } else { Value::Null },
                "usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}
            }}),
        ]
    } else if request.get("max_tokens").is_some() && request.get("system").is_some() {
        vec![
            json!({"type":"message_start","message":{"usage":{"input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":if scripted.truncated {"max_tokens"} else {"end_turn"}},"usage":{"output_tokens":1}}),
            json!({"type":"message_stop"}),
        ]
    } else {
        vec![
            json!({"id":"mock","object":"chat.completion.chunk","model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":text},"finish_reason":null}]}),
            json!({"id":"mock","object":"chat.completion.chunk","model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":if scripted.truncated {"length"} else {"stop"}}]}),
        ]
    };
    let mut body = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    body.push_str("data: [DONE]\n\n");
    ([("content-type", "text/event-stream")], body)
}

struct Server {
    url: String,
    model: Arc<ScriptedModel>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(replies: Vec<ScriptedReply>, repair_delay: Duration) -> Self {
        let model = Arc::new(ScriptedModel {
            replies: Mutex::new(replies.into()),
            repair_delay,
            ..Default::default()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/chat/completions", post(serve_reply))
            .route("/responses", post(serve_reply))
            .route("/v1/messages", post(serve_reply))
            .with_state(model.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, model, task }
    }

    async fn complete(
        &self,
        format: ApiFormat,
        deadline: tokio::time::Instant,
        attachments: &[ContentBlock],
    ) -> Result<Value, String> {
        complete_route(
            "Return only JSON {\"reply\":\"answer\"}.",
            json!({"latest_user_message":"original request"}),
            self.url.clone(),
            "test-key".into(),
            "test-model".into(),
            format,
            deadline,
            attachments,
        )
        .await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn repair_keeps_original_context_attachments_and_selected_model_for_all_formats() {
    for format in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ] {
        let server = Server::start(
            vec![
                ScriptedReply {
                    text: "an answer without JSON",
                    truncated: false,
                },
                ScriptedReply {
                    text: r#"{"reply":"repaired"}"#,
                    truncated: false,
                },
            ],
            Duration::ZERO,
        )
        .await;
        let output = server
            .complete(
                format,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &[ContentBlock::Text(TextContent::new("attachment context"))],
            )
            .await
            .unwrap();
        assert_eq!(output, json!({"reply":"repaired"}));
        let requests = server.model.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["model"], "test-model");
        assert_eq!(requests[1]["model"], "test-model");
        let repair = requests[1].to_string();
        assert!(repair.contains("original request"));
        assert!(repair.contains("attachment context"));
        assert!(repair.contains(REPAIR_PROMPT));
        assert!(requests.iter().all(|request| {
            request
                .get("tools")
                .is_none_or(|tools| tools.as_array().is_some_and(Vec::is_empty))
        }));
    }
}

#[tokio::test]
async fn truncated_output_retries_with_larger_budget_for_all_formats() {
    for format in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ] {
        let server = Server::start(
            vec![
                ScriptedReply {
                    text: r#"{"reply":"partial"}"#,
                    truncated: true,
                },
                ScriptedReply {
                    text: r#"{"reply":"complete"}"#,
                    truncated: false,
                },
            ],
            Duration::ZERO,
        )
        .await;
        let output = server
            .complete(
                format,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(output["reply"], "complete");
        let requests = server.model.requests.lock().await;
        assert_eq!(requests.len(), 2);
        let field = if format == ApiFormat::Codex {
            "max_output_tokens"
        } else {
            "max_tokens"
        };
        assert_eq!(requests[0][field], 6000);
        assert_eq!(requests[1][field], 12000);
    }
}

#[tokio::test]
async fn repeated_invalid_output_stops_after_one_repair() {
    for (text, truncated, expected) in [
        ("still invalid", false, "invalid JSON after retry"),
        ("", false, "no response text after retry"),
        (
            r#"{"reply":"partial"}"#,
            true,
            "output token limit after retry",
        ),
    ] {
        let server = Server::start(
            vec![
                ScriptedReply { text, truncated },
                ScriptedReply { text, truncated },
            ],
            Duration::ZERO,
        )
        .await;
        let error = server
            .complete(
                ApiFormat::Codex,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &[],
            )
            .await
            .unwrap_err();
        assert!(error.contains(expected), "{error}");
        assert_eq!(server.model.requests.lock().await.len(), 2);
    }
}

#[tokio::test]
async fn valid_first_response_does_not_retry() {
    let server = Server::start(
        vec![ScriptedReply {
            text: r#"{"reply":"done"}"#,
            truncated: false,
        }],
        Duration::ZERO,
    )
    .await;
    assert_eq!(
        server
            .complete(
                ApiFormat::Codex,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &[]
            )
            .await
            .unwrap()["reply"],
        "done"
    );
    assert_eq!(server.model.requests.lock().await.len(), 1);
}

#[tokio::test]
async fn repair_shares_the_original_deadline() {
    let server = Server::start(
        vec![
            ScriptedReply {
                text: "invalid",
                truncated: false,
            },
            ScriptedReply {
                text: r#"{"reply":"too late"}"#,
                truncated: false,
            },
        ],
        Duration::from_secs(2),
    )
    .await;
    let error = server
        .complete(
            ApiFormat::Codex,
            tokio::time::Instant::now() + Duration::from_millis(500),
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(error, "Model request timed out");
    assert_eq!(server.model.requests.lock().await.len(), 2);
}
