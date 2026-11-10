//! Real Pi SDK + bridge provider configuration + local HTTP mock.
//! Re-exec isolates SDK global config/environment from the parallel test suite.
use super::*;
use crate::models::ApiFormat;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Server(Child, String);
impl Server {
    fn start() -> Self {
        Self::start_with(&[])
    }

    fn start_with(args: &[&str]) -> Self {
        let mut child = Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/llm_mock.py"
            ))
            .args(args)
            .stdout(Stdio::piped())
            .spawn()
            .expect("python3 is required for model API integration tests");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let value: Value = serde_json::from_str(&line).expect("mock startup JSON");
        Self(child, value["base_url"].as_str().unwrap().to_string())
    }

    async fn plan(&self, key: &str, steps: Value) {
        reqwest::Client::new()
            .post(format!("{}/__mock/plan", self.1.trim_end_matches("/v1")))
            .json(&serde_json::json!({"key":key,"steps":steps}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    async fn records(&self) -> Vec<Value> {
        reqwest::get(format!(
            "{}/__mock/requests",
            self.1.trim_end_matches("/v1")
        ))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
    }

    async fn agent(
        &self,
        root: &Path,
        session: &str,
        key: &str,
        model: &str,
        format: ApiFormat,
    ) -> pi_agent_rust::sdk::AgentSessionHandle {
        pi_agent_rust::sdk::create_agent_session(self.options(root, session, key, model, format))
            .await
            .unwrap()
    }

    fn options(
        &self,
        root: &Path,
        session: &str,
        key: &str,
        model: &str,
        format: ApiFormat,
    ) -> pi_agent_rust::sdk::SessionOptions {
        let config = ResolvedProviderConfig {
            base_url: if matches!(format, ApiFormat::AnthropicMessages) {
                self.1.trim_end_matches("/v1").into()
            } else {
                self.1.clone()
            },
            api_key: key.into(),
            model: Some(model.into()),
            format,
            acp_profile: None,
            provider_id: Some("mock".into()),
            opencode_provider_name: None,
            extra_headers: vec![],
            acp_command: None,
            acp_args: vec![],
            acp_env: vec![],
        };
        let provider = configure_pi_provider(&config, session, Some(model)).unwrap();
        ensure_pi_session_file(
            &root.join(format!("{session}.jsonl")),
            root,
            Some(&provider),
            Some(model),
        )
        .unwrap();
        pi_agent_rust::sdk::SessionOptions {
            provider: Some(provider),
            model: Some(model.into()),
            api_key: Some(key.into()),
            working_directory: Some(root.into()),
            no_session: false,
            session_path: Some(root.join(format!("{session}.jsonl"))),
            enabled_tools: Some(vec!["read".into()]),
            system_prompt: Some("Follow the mock model instructions.".into()),
            max_tool_iterations: 3,
            ..Default::default()
        }
    }
}

#[path = "llm_extended_tests.rs"]
mod extended;
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn prompt(handle: &mut pi_agent_rust::sdk::AgentSessionHandle, input: &str) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(12), handle.prompt(input, |_| {}))
        .await
        .expect("agent prompt exceeded test deadline")
        .expect("agent prompt failed");
    serde_json::to_value(message).unwrap()
}

#[tokio::test]
async fn model_api_mock_agent_matrix() {
    const CHILD: &str = "OMNI_LLM_MOCK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let root = std::env::temp_dir().join(format!("omni-llm-mock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "adapter::llm_mock_tests::model_api_mock_agent_matrix",
                "--nocapture",
            ])
            .env(CHILD, &root)
            .env("PI_CODING_AGENT_DIR", root.join("pi"))
            .env("PI_HTTP_REQUEST_TIMEOUT_SECS", "2")
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env_remove("PI_EXTENSION_PATHS")
            .output()
            .unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            result.status.success(),
            "isolated agent tests failed:\n{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let root = PathBuf::from(std::env::var_os(CHILD).unwrap());
    std::fs::write(root.join("hello.txt"), "tool-result-unique-marker").unwrap();
    let first = Server::start();
    let second = Server::start();
    // Protocol switching recreates the SDK handle as the bridge does each turn,
    // preserving the same on-disk session and checking the actual wire request.
    for (index, format) in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ]
    .into_iter()
    .enumerate()
    {
        let server = if index == 1 { &second } else { &first };
        let key = format!("fake-key-{index}");
        let model = format!("mock-model-{index}");
        server
            .plan(
                &key,
                serde_json::json!([
                    {"tool":{"name":"read", "arguments":{"path":"hello.txt"}}},
                    {"text":format!("tool-finished-{index}")}
                ]),
            )
            .await;
        let mut agent = server.agent(&root, "shared", &key, &model, format).await;
        let result = prompt(&mut agent, &format!("turn-{index}: read hello.txt")).await;
        assert!(
            result
                .to_string()
                .contains(&format!("tool-finished-{index}")),
            "{result}"
        );
        let records = server.records().await;
        let last = records.last().unwrap();
        assert_eq!(last["body"]["model"], model);
        assert!(
            last["body"]
                .to_string()
                .contains("tool-result-unique-marker"),
            "tool result missing: {last}"
        );
        if index > 0 {
            assert!(
                last["body"].to_string().contains("turn-0"),
                "history lost during switch: {last}"
            );
        }
        let expected_path = match format {
            ApiFormat::Codex => "/v1/responses",
            ApiFormat::AnthropicMessages => "/v1/messages",
            _ => "/v1/chat/completions",
        };
        assert_eq!(last["path"], expected_path);
        use sha2::{Digest, Sha256};
        assert_eq!(
            last["key_sha256"],
            format!("{:x}", Sha256::digest(key.as_bytes()))
        );
        assert!(last["headers"].get("authorization").is_none());
        assert!(last["headers"].get("x-api-key").is_none());
        // A failing tool is returned to the model and does not end the turn.
        server
            .plan(
                &key,
                serde_json::json!([
                    {"tool":{"name":"read","arguments":{"path":"does-not-exist.txt"}}},
                    {"text":"handled-tool-error"}
                ]),
            )
            .await;
        let failed_tool = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&failed_tool);
        let result = tokio::time::timeout(
            Duration::from_secs(12),
            agent.prompt("exercise a failing tool", move |event| {
                if matches!(
                    event,
                    pi_agent_rust::sdk::AgentEvent::ToolExecutionEnd { is_error: true, .. }
                ) {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(failed_tool.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            serde_json::to_value(result)
                .unwrap()
                .to_string()
                .contains("handled-tool-error")
        );
    }
    // Two live agents must retain independent configuration, even while an old
    // request is awaiting its first response and another session changes API.
    first
        .plan(
            "parallel-old",
            serde_json::json!([{"delay":0.25,"text":"old-session-answer"}]),
        )
        .await;
    second
        .plan(
            "parallel-new",
            serde_json::json!([{"text":"new-session-answer"}]),
        )
        .await;
    let mut old = first
        .agent(
            &root,
            "parallel-a",
            "parallel-old",
            "old-model",
            ApiFormat::OpenAiCompatible,
        )
        .await;
    let mut new = second
        .agent(
            &root,
            "parallel-b",
            "parallel-new",
            "new-model",
            ApiFormat::AnthropicMessages,
        )
        .await;
    let (old_result, new_result) = tokio::join!(
        prompt(&mut old, "old-only-marker"),
        prompt(&mut new, "new-only-marker")
    );
    assert!(old_result.to_string().contains("old-session-answer"));
    assert!(new_result.to_string().contains("new-session-answer"));
    let old_record = first.records().await.pop().unwrap();
    let new_record = second.records().await.pop().unwrap();
    assert_eq!(old_record["body"]["model"], "old-model");
    assert_eq!(new_record["body"]["model"], "new-model");
    assert!(!old_record["body"].to_string().contains("new-only-marker"));
    assert!(!new_record["body"].to_string().contains("old-only-marker"));
    // Fatal HTTP failures, malformed data and disconnected streams must not
    // produce successful assistant output. Recovery uses the same session id.
    for (index, step) in [
        serde_json::json!({"status":401}),
        serde_json::json!({"status":403}),
        serde_json::json!({"status":404}),
        serde_json::json!({"status":400}),
        serde_json::json!({"status":408}),
        serde_json::json!({"status":409}),
        serde_json::json!({"status":413}),
        serde_json::json!({"status":422}),
        serde_json::json!({"status":429}),
        serde_json::json!({"status":500}),
        serde_json::json!({"status":502}),
        serde_json::json!({"status":503}),
        serde_json::json!({"status":504}),
        serde_json::json!({"malformed":true}),
        serde_json::json!({"disconnect":true}),
        serde_json::json!({"delay":3}),
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("failure-{index}");
        // Repeat failures so an SDK retry cannot accidentally consume success.
        first.plan(&key, Value::Array(vec![step; 20])).await;
        let session = format!("failure-{index}");
        let mut agent = first
            .agent(&root, &session, &key, "mock", ApiFormat::OpenAiCompatible)
            .await;
        let outcome = tokio::time::timeout(Duration::from_secs(8), agent.prompt("fail", |_| {}))
            .await
            .expect("agent failed to return a bounded error");
        assert!(
            outcome.is_err()
                || outcome.as_ref().is_ok_and(|message| matches!(
                    message.stop_reason,
                    pi_agent_rust::model::StopReason::Error
                        | pi_agent_rust::model::StopReason::Aborted
                )),
            "failure unexpectedly succeeded: {index}"
        );
        drop(agent);
        first
            .plan(&key, serde_json::json!([{"text":"recovered"}]))
            .await;
        let mut agent = first
            .agent(&root, &session, &key, "mock", ApiFormat::OpenAiCompatible)
            .await;
        assert!(
            prompt(&mut agent, "recover")
                .await
                .to_string()
                .contains("recovered")
        );
    }
    // The current SDK intentionally returns accumulated partial text on EOF.
    // Characterize that behavior separately from fatal transport failures.
    first
        .plan(
            "partial",
            serde_json::json!([{"text":"partial-output","truncate_after":1}]),
        )
        .await;
    let mut partial = first
        .agent(
            &root,
            "partial",
            "partial",
            "mock",
            ApiFormat::OpenAiCompatible,
        )
        .await;
    let result = prompt(&mut partial, "truncated stream").await;
    assert!(result.to_string().contains("partial"));
    assert!(!result.to_string().contains("partial-output"));
    first
        .plan(
            "partial",
            serde_json::json!([{"text":"complete-after-partial"}]),
        )
        .await;
    assert!(
        prompt(&mut partial, "try again")
            .await
            .to_string()
            .contains("complete-after-partial")
    );
    bridge_switch_matrix(&root, &first, &second).await;
}

async fn settings(state: &AppState, server: &Server, key: &str, model: &str, format: ApiFormat) {
    state
        .update_bridge_settings(crate::bridge_settings::BridgeSettingsInput {
            ai_approval: None,
            acp_servers: None,
            model_providers: Some(vec![crate::models::ModelProviderConfig {
                id: "bridge-mock".into(),
                name: "Mock".into(),
                base_url: if matches!(format, ApiFormat::AnthropicMessages) {
                    server.1.trim_end_matches("/v1").into()
                } else {
                    server.1.clone()
                },
                api_key: key.into(),
                model: Some(model.into()),
                format,
                enabled: true,
                priority: 0,
            }]),
        })
        .await
        .unwrap();
}

async fn send(state: &Arc<AppState>, session: &str, content: &str) {
    let turn_id = uuid::Uuid::new_v4().to_string();
    let message_id = uuid::Uuid::new_v4().to_string();
    state
        .create_domain_turn(
            session,
            &crate::session_domain::CreateTurnCommand {
                command_id: uuid::Uuid::new_v4().to_string(),
                turn_id: turn_id.clone(),
                user_message_id: message_id.clone(),
                content: content.into(),
                attachments: vec![],
                input_mode: crate::models::InputMode::Text,
                system_prompt: None,
                expected_session_version: None,
                provider_id: Some("bridge-mock".into()),
                reasoning_effort: None,
                model: None,
            },
        )
        .await
        .unwrap();
    state
        .send_domain_message(
            session,
            &turn_id,
            crate::models::SendMessageInput {
                content: content.into(),
                input_mode: crate::models::InputMode::Text,
                system_prompt: None,
                client_message_id: Some(message_id),
                provider_id: Some("bridge-mock".into()),
                reasoning_effort: None,
                model: None,
            },
        )
        .await
        .unwrap();
}

async fn settled(state: &Arc<AppState>, session: &str) -> crate::models::SessionStatus {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = state.get_session(session).await.unwrap().session.status;
            if !matches!(status, crate::models::SessionStatus::Running)
                && !state.turn_in_flight_for_test(session).await
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bridge turn failed to settle")
}

async fn bridge_switch_matrix(root: &Path, first: &Server, second: &Server) {
    use crate::models::{CreateProjectInput, CreateSessionInput, SessionStatus};
    let state = Arc::new(
        AppState::new_with_paths(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
        )
        .await,
    );
    state.clear_push_devices_for_test().await;
    let project = state
        .create_project(CreateProjectInput {
            name: "LLM mock".into(),
            root_path: root.to_string_lossy().into(),
        })
        .await;
    settings(
        &state,
        first,
        "bridge-key-0",
        "bridge-model-0",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    let session = state
        .create_session(CreateSessionInput {
            client_session_id: uuid::Uuid::new_v4().to_string(),
            project_id: project.id,
            title: Some("LLM integration".into()),
            agent: AgentKind::Pi,
            brief_reply_mode: false,
            provider_id: Some("bridge-mock".into()),
            reasoning_effort: None,
            model: None,
        })
        .await
        .unwrap();
    for (index, format) in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ]
    .into_iter()
    .enumerate()
    {
        let server = if index == 1 { second } else { first };
        let key = format!("bridge-key-{index}");
        let model = format!("bridge-model-{index}");
        settings(&state, server, &key, &model, format).await;
        server
            .plan(
                &key,
                serde_json::json!([{"text":format!("bridge-reply-{index}")}]),
            )
            .await;
        send(&state, &session.id, &format!("bridge-turn-{index}")).await;
        assert!(matches!(
            settled(&state, &session.id).await,
            SessionStatus::Idle
        ));
        let records = server.records().await;
        let last = records.last().unwrap();
        assert_eq!(last["body"]["model"], model);
        assert!(last["body"].to_string().contains("bridge-turn-0"));
        let messages: Vec<_> = state
            .list_messages(&session.id)
            .await
            .unwrap()
            .into_iter()
            .filter(|message| matches!(message.role, crate::models::MessageRole::Assistant))
            .collect();
        assert!(
            messages
                .last()
                .unwrap()
                .content
                .contains(&format!("bridge-reply-{index}")),
            "bridge turn {index}: assistant messages={messages:?}"
        );
    }
    // Session model override and explicit clearing must affect actual requests.
    state
        .update_session_settings(&session.id, None, None, Some(Some("override-model".into())))
        .await
        .unwrap();
    send(&state, &session.id, "override model").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert_eq!(
        first.records().await.last().unwrap()["body"]["model"],
        "override-model"
    );
    state
        .update_session_settings(&session.id, None, None, Some(None))
        .await
        .unwrap();
    send(&state, &session.id, "clear override").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert_eq!(
        first.records().await.last().unwrap()["body"]["model"],
        "bridge-model-2"
    );

    // Configuration changes while a request is live apply to the next turn;
    // the in-flight request completes against its original provider snapshot.
    first
        .plan(
            "bridge-key-2",
            serde_json::json!([{"delay":0.4,"text":"inflight-old"}]),
        )
        .await;
    let count = first.records().await.len();
    send(&state, &session.id, "inflight switch").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while first.records().await.len() == count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    second
        .plan("bridge-next", serde_json::json!([{"text":"next-turn-new"}]))
        .await;
    settings(
        &state,
        second,
        "bridge-next",
        "next-model",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert!(
        state
            .list_messages(&session.id)
            .await
            .unwrap()
            .into_iter()
            .rev()
            .find(|message| matches!(message.role, crate::models::MessageRole::Assistant))
            .unwrap()
            .content
            .contains("inflight-old")
    );
    send(&state, &session.id, "after switch").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert_eq!(
        second.records().await.last().unwrap()["body"]["model"],
        "next-model"
    );

    // Authentication failure is visible at bridge level, then switching back
    // restores the same conversation without losing its earlier history.
    settings(
        &state,
        first,
        "unregistered-key",
        "bad-model",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    send(&state, &session.id, "invalid credentials").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Failed
    ));
    settings(
        &state,
        second,
        "bridge-next",
        "next-model",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    send(&state, &session.id, "recovered after auth failure").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert!(
        second.records().await.last().unwrap()["body"]
            .to_string()
            .contains("bridge-turn-0")
    );

    // Actual bridge rate-limit retry, not a manually retried HTTP request.
    second.plan("bridge-next", serde_json::json!([{"status":429,"headers":{"Retry-After":"0"}},{"text":"retry-succeeded"}])).await;
    let before = second.records().await.len();
    send(&state, &session.id, "retry me").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert!(second.records().await.len() >= before + 2);
    assert!(
        state
            .list_messages(&session.id)
            .await
            .unwrap()
            .into_iter()
            .rev()
            .find(|message| matches!(message.role, crate::models::MessageRole::Assistant))
            .unwrap()
            .content
            .contains("retry-succeeded")
    );
    second
        .plan(
            "bridge-next",
            serde_json::json!([{"delay":0.6,"text":"cancelled-answer-must-not-win"}]),
        )
        .await;
    let before = second.records().await.len();
    send(&state, &session.id, "cancel this request").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while second.records().await.len() == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(state.cancel_turn(&session.id).await.unwrap());
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Interrupted
    ));
    second
        .plan(
            "bridge-next",
            serde_json::json!([{"text":"after-cancel-success"}]),
        )
        .await;
    send(&state, &session.id, "continue after cancel").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        state
            .list_messages(&session.id)
            .await
            .unwrap()
            .into_iter()
            .rev()
            .find(|message| matches!(message.role, crate::models::MessageRole::Assistant))
            .unwrap()
            .content
            .contains("after-cancel-success")
    );

    second
        .plan(
            "bridge-next",
            serde_json::json!([{"status":404,"expect":{"model":"missing-model"}}]),
        )
        .await;
    state
        .update_session_settings(&session.id, None, None, Some(Some("missing-model".into())))
        .await
        .unwrap();
    send(&state, &session.id, "invalid model").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Failed
    ));
    state
        .update_session_settings(&session.id, None, None, Some(None))
        .await
        .unwrap();
    send(&state, &session.id, "recover model default").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert_eq!(
        second.records().await.last().unwrap()["body"]["model"],
        "next-model"
    );

    // Invalid configuration cannot replace the working provider.
    for base_url in ["", "ftp://invalid.test"] {
        let invalid = crate::models::ModelProviderConfig {
            id: "bridge-mock".into(),
            name: "Invalid".into(),
            base_url: base_url.into(),
            api_key: "fake".into(),
            model: Some("mock".into()),
            format: ApiFormat::OpenAiCompatible,
            enabled: true,
            priority: 0,
        };
        assert!(
            state
                .update_bridge_settings(crate::bridge_settings::BridgeSettingsInput {
                    ai_approval: None,
                    acp_servers: None,
                    model_providers: Some(vec![invalid]),
                })
                .await
                .is_err()
        );
    }
    send(&state, &session.id, "working configuration retained").await;
    assert!(matches!(
        settled(&state, &session.id).await,
        SessionStatus::Idle
    ));
    assert_eq!(
        second.records().await.last().unwrap()["body"]["model"],
        "next-model"
    );
}
