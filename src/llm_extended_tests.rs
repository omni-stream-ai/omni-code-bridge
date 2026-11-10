//! Fault and HTTP lifecycle tests. Every case re-execs with isolated agent homes.
use super::*;
use crate::models::{CreateProjectInput, CreateSessionInput, SessionStatus};
use std::time::Instant;

async fn isolated(case: &str) -> Option<PathBuf> {
    if std::env::var("OMNI_LLM_EXTENDED_CASE").as_deref() == Ok(case) {
        return Some(std::env::var_os("OMNI_LLM_EXTENDED_ROOT").unwrap().into());
    }
    let root = std::env::temp_dir().join(format!(
        "omni-llm-{}-{}",
        case.replace("::", "-"),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(root.join("codex")).unwrap();
    std::fs::write(root.join("codex/config.toml"), "check_for_update_on_startup = false\n[features]\nunbounded_connection_retries = false\n[model_providers.bridge-mock]\nname = \"Local mock\"\nrequest_max_retries = 0\nstream_max_retries = 0\nstream_idle_timeout_ms = 1500\n").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(300),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("adapter::llm_mock_tests::extended::{case}"),
                "--nocapture",
            ])
            .env("OMNI_LLM_EXTENDED_CASE", case)
            .env("OMNI_LLM_EXTENDED_ROOT", &root)
            .env("PI_CODING_AGENT_DIR", root.join("pi"))
            .env("PI_HTTP_REQUEST_TIMEOUT_SECS", "2")
            .env("CODEX_HOME", root.join("codex"))
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .env_remove("PI_EXTENSION_PATHS")
            .env("RUST_LOG", "error")
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy")
            .env_remove("OMNI_CODE_FCM_SERVICE_ACCOUNT_JSON")
            .env_remove("OMNI_CODE_FCM_SERVICE_ACCOUNT_PATH")
            .env_remove("OMNI_CODE_XIAOMI_APP_SECRET")
            .kill_on_drop(true)
            .output(),
    )
    .await;
    let _ = std::fs::remove_dir_all(root);
    let output = result
        .expect("isolated matrix exceeded five minute deadline")
        .unwrap();
    assert!(
        output.status.success(),
        "{case} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    None
}

#[path = "llm_http_tests.rs"]
mod http;

async fn state(root: &Path) -> Arc<AppState> {
    let state = Arc::new(
        AppState::new_with_paths(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
        )
        .await,
    );
    state.clear_push_devices_for_test().await;
    state
}

async fn session(state: &Arc<AppState>, root: &Path, agent: AgentKind) -> String {
    let project = state
        .create_project(CreateProjectInput {
            name: "fault matrix".into(),
            root_path: root.to_string_lossy().into(),
        })
        .await;
    state
        .create_session(CreateSessionInput {
            client_session_id: uuid::Uuid::new_v4().to_string(),
            project_id: project.id,
            title: Some("mock fault test".into()),
            agent,
            brief_reply_mode: false,
            provider_id: Some("bridge-mock".into()),
            reasoning_effort: None,
            model: None,
        })
        .await
        .unwrap()
        .id
}

async fn wait(state: &Arc<AppState>, session: &str, timeout: u64) -> SessionStatus {
    tokio::time::timeout(Duration::from_secs(timeout), async {
        loop {
            let snapshot = state.domain_session_state(session).await.unwrap().unwrap();
            if snapshot.session.active_turn_id.is_none()
                && !state.turn_in_flight_for_test(session).await
            {
                return state.get_session(session).await.unwrap().session.status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("turn exceeded deadline")
}

async fn seen(server: &Server, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while server.records().await.len() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("model request was not received");
}

fn failed(result: pi_agent_rust::sdk::Result<pi_agent_rust::model::AssistantMessage>) -> bool {
    result.is_err()
        || result.as_ref().is_ok_and(|m| {
            matches!(
                m.stop_reason,
                pi_agent_rust::model::StopReason::Error | pi_agent_rust::model::StopReason::Aborted
            )
        })
}

#[tokio::test]
async fn model_api_pi_network_faults() {
    let Some(root) = isolated("model_api_pi_network_faults").await else {
        return;
    };
    let mut server = Server::start();
    let tls = Server::start_with(&["--tls"]);
    // The reserved listener remains bound but never accepts, exercising header
    // timeout without a race over a temporarily released port.
    let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let refused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let refused_address = refused.local_addr().unwrap();
    drop(refused);
    for (index, url) in [
        "http://model-does-not-exist.invalid/v1".to_string(),
        format!("http://{refused_address}/v1"),
        format!("http://{}/v1", stalled.local_addr().unwrap()),
        server.1.replace("http://", "https://"),
        tls.1.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        let original = std::mem::replace(&mut server.1, url);
        let options = server.options(
            &root,
            &format!("network-{index}"),
            "fake",
            "mock",
            ApiFormat::OpenAiCompatible,
        );
        server.1 = original;
        let mut agent = pi_agent_rust::sdk::create_agent_session(options)
            .await
            .unwrap();
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            agent.prompt("network fault", |_| {}),
        )
        .await
        .unwrap_or_else(|_| panic!("network fault {index} did not return within timeout"));
        assert!(
            failed(result),
            "network fault {index} unexpectedly succeeded"
        );
        assert!(start.elapsed() < Duration::from_secs(8));
    }
    for (index, format) in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("idle-{index}");
        server
            .plan(
                &key,
                serde_json::json!([{"text":"must-not-complete", "stall_at":1,"stall_seconds":3}]),
            )
            .await;
        let mut agent = server.agent(&root, &key, &key, "mock", format).await;
        let result =
            tokio::time::timeout(Duration::from_secs(8), agent.prompt("idle stream", |_| {}))
                .await
                .unwrap();
        assert!(failed(result), "idle stream should time out for {format:?}");
        server
            .plan(&key, serde_json::json!([{"text":"idle-recovered"}]))
            .await;
        assert!(
            prompt(&mut agent, "recover")
                .await
                .to_string()
                .contains("idle-recovered")
        );
    }
}

#[tokio::test]
async fn model_api_pi_tool_faults() {
    let Some(root) = isolated("model_api_pi_tool_faults").await else {
        return;
    };
    std::fs::write(root.join("a.txt"), "file-a-marker").unwrap();
    std::fs::write(root.join("b.txt"), "file-b-marker").unwrap();
    let server = Server::start();
    for (index, format) in [
        ApiFormat::OpenAiCompatible,
        ApiFormat::Codex,
        ApiFormat::AnthropicMessages,
    ]
    .into_iter()
    .enumerate()
    {
        for (name, tool) in [
            (
                "unknown",
                serde_json::json!({"name":"tool_that_does_not_exist","arguments":{}}),
            ),
            (
                "schema",
                serde_json::json!({"name":"read","arguments":{"path":42}}),
            ),
            (
                "json",
                serde_json::json!({"name":"read","arguments_json":"{broken"}),
            ),
            (
                "timeout",
                serde_json::json!({"name":"bash","arguments":{"command":"python3 -c 'import time; time.sleep(3)'","timeout":1}}),
            ),
        ] {
            let key = format!("{index}-{name}");
            server
                .plan(
                    &key,
                    serde_json::json!([{"tool":tool},{"text":"continued-after-tool-fault"}]),
                )
                .await;
            let mut options = server.options(&root, &key, &key, "mock", format);
            options.enabled_tools = Some(vec!["read".into(), "bash".into()]);
            let mut agent = pi_agent_rust::sdk::create_agent_session(options)
                .await
                .unwrap();
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            let captured = Arc::clone(&events);
            let result = tokio::time::timeout(
                Duration::from_secs(12),
                agent.prompt("tool fault", move |event| {
                    if let pi_agent_rust::sdk::AgentEvent::ToolExecutionEnd {
                        is_error,
                        result,
                        ..
                    } = event
                    {
                        captured
                            .lock()
                            .unwrap()
                            .push((is_error, serde_json::to_value(result).unwrap()));
                    }
                }),
            )
            .await
            .expect("tool fault hung");
            // Invalid wire arguments can be rejected by the provider parser;
            // otherwise the agent must report a tool error and continue.
            if let Ok(message) = result {
                let value = serde_json::to_value(message).unwrap();
                assert!(
                    value.to_string().contains("continued-after-tool-fault")
                        || value["stopReason"] == "error",
                    "{name}/{format:?}: {value}"
                );
                if name != "json" {
                    assert!(
                        events.lock().unwrap().iter().any(|(error, _)| *error),
                        "missing failed tool event: {name}/{format:?}"
                    );
                }
            } else {
                assert_eq!(name, "json", "unexpected fatal error for {name}/{format:?}");
            }
        }
        let key = format!("parallel-tools-{index}");
        server.plan(&key, serde_json::json!([{"tools":[
            {"name":"read","arguments":{"path":"a.txt"}}, {"name":"read","arguments":{"path":"b.txt"}}
        ]},{"text":"both-tools-finished"}])).await;
        let mut agent = server.agent(&root, &key, &key, "mock", format).await;
        assert!(
            prompt(&mut agent, "read both")
                .await
                .to_string()
                .contains("both-tools-finished")
        );
        let body = server.records().await.last().unwrap()["body"].to_string();
        assert!(
            body.contains("file-a-marker") && body.contains("file-b-marker"),
            "parallel tool results lost: {format:?}"
        );
        let key = format!("loop-{index}");
        server
            .plan(
                &key,
                Value::Array(vec![
                    serde_json::json!({"tool":{"name":"read","arguments":{"path":"a.txt"}}});
                    10
                ]),
            )
            .await;
        let mut agent = server.agent(&root, &key, &key, "mock", format).await;
        let result = tokio::time::timeout(Duration::from_secs(12), agent.prompt("loop", |_| {}))
            .await
            .unwrap();
        assert!(failed(result), "tool iteration limit should fail");
        let digest = format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes())
        );
        assert!(
            server
                .records()
                .await
                .iter()
                .filter(|r| r["key_sha256"] == digest)
                .count()
                <= 4
        );
    }
}

#[tokio::test]
async fn model_api_pi_retry_boundaries() {
    let Some(root) = isolated("model_api_pi_retry_boundaries").await else {
        return;
    };
    let state = state(&root).await;
    let old = Server::start();
    let new = Server::start();
    settings(
        &state,
        &old,
        "retry",
        "old-model",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    let id = session(&state, &root, AgentKind::Pi).await;
    // The pinned SDK does not expose Retry-After to the bridge; the supported
    // contract is fixed 5/15/30 second backoff, bounded to three retries.
    old.plan(
        "retry",
        Value::Array(vec![
            serde_json::json!({"status":429,"headers":{"Retry-After":"0"}});
            4
        ]),
    )
    .await;
    send(&state, &id, "exhausted-user-marker").await;
    assert!(matches!(wait(&state, &id, 80).await, SessionStatus::Failed));
    let records = old.records().await;
    assert_eq!(
        records.len(),
        4,
        "initial request plus exactly three retries"
    );
    for (pair, minimum) in records.windows(2).zip([4.8, 14.8, 29.8]) {
        assert!(
            pair[1]["at"].as_f64().unwrap() - pair[0]["at"].as_f64().unwrap() >= minimum,
            "backoff was too short"
        );
        let messages = pair[1]["body"]["messages"].as_array().unwrap();
        assert_eq!(
            messages
                .iter()
                .filter(|m| m["role"] == "user"
                    && m["content"].to_string().contains("exhausted-user-marker"))
                .count(),
            1
        );
    }
    // Changing configuration during backoff must not redirect an old turn.
    old.plan(
        "retry",
        serde_json::json!([{"status":429},{"text":"old-retry-success"}]),
    )
    .await;
    let before = old.records().await.len();
    send(&state, &id, "switch-during-backoff").await;
    seen(&old, before + 1).await;
    new.plan("next", serde_json::json!([{"text":"next-provider"}]))
        .await;
    settings(
        &state,
        &new,
        "next",
        "new-model",
        ApiFormat::OpenAiCompatible,
    )
    .await;
    assert!(matches!(wait(&state, &id, 15).await, SessionStatus::Idle));
    assert!(new.records().await.is_empty());
    assert_eq!(
        old.records().await.last().unwrap()["body"]["model"],
        "old-model"
    );
    send(&state, &id, "new turn").await;
    assert!(matches!(wait(&state, &id, 10).await, SessionStatus::Idle));
    assert_eq!(
        new.records().await.last().unwrap()["body"]["model"],
        "new-model"
    );
    // Cancel while sleeping; no timer is allowed to send a later retry.
    new.plan(
        "next",
        serde_json::json!([{"status":429},{"text":"must-not-arrive"}]),
    )
    .await;
    let before = new.records().await.len();
    send(&state, &id, "cancel backoff").await;
    seen(&new, before + 1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(state.cancel_turn(&id).await.unwrap());
    assert!(matches!(
        wait(&state, &id, 5).await,
        SessionStatus::Interrupted
    ));
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(new.records().await.len(), before + 1);
    new.plan("next", serde_json::json!([{"status":429},{"status":403}]))
        .await;
    send(&state, &id, "retry becomes fatal").await;
    assert!(matches!(wait(&state, &id, 15).await, SessionStatus::Failed));
    assert_eq!(new.records().await.len(), before + 3);
    new.plan("next", serde_json::json!([{"text":"retry-recovered"}]))
        .await;
    send(&state, &id, "recover after retry failure").await;
    assert!(matches!(wait(&state, &id, 10).await, SessionStatus::Idle));
}

#[tokio::test]
async fn model_api_settings_atomic_reads() {
    let Some(root) = isolated("model_api_settings_atomic_reads").await else {
        return;
    };
    let path = root.join("settings.json");
    let store = crate::bridge_settings::BridgeSettingsStore::load_from_path(path.clone()).await;
    store.update(|_| {}).await.unwrap();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = Arc::clone(&finished);
    let reader = tokio::task::spawn_blocking(move || {
        let mut reads = 0;
        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
            let bytes = std::fs::read(&path).unwrap();
            serde_json::from_slice::<crate::bridge_settings::BridgeSettings>(&bytes)
                .expect("another process observed partially written provider configuration");
            reads += 1;
            std::thread::sleep(Duration::from_micros(100));
        }
        reads
    });
    for i in 0..20 {
        store
            .update(|settings| {
                settings.ai_approval.prompt = format!("{i}:{}", "test-marker".repeat(20000))
            })
            .await
            .unwrap();
    }
    finished.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(reader.await.unwrap() > 0);
}

#[tokio::test]
async fn model_api_real_codex_bridge_retry_boundaries() {
    let Some(root) = isolated("model_api_real_codex_bridge_retry_boundaries").await else {
        return;
    };
    assert!(codex_binary_path().is_file(), "real Codex is required");
    let state = state(&root).await;
    let old = Server::start();
    let new = Server::start();
    settings(&state, &old, "codex-retry", "mock-codex", ApiFormat::Codex).await;
    let id = session(&state, &root, AgentKind::Codex).await;
    // Exhaust both the real app-server transport and bridge retry budgets.
    // CLI retry budgets are zero in this profile, so exactly eight model
    // requests must be observable (initial + seven bridge retries).
    old.plan(
        "codex-retry",
        Value::Array(vec![serde_json::json!({"disconnect":true}); 12]),
    )
    .await;
    send(&state, &id, "codex-exhaustion").await;
    assert!(matches!(
        wait(&state, &id, 200).await,
        SessionStatus::Failed
    ));
    let records = old.records().await;
    assert_eq!(
        records.len(),
        8,
        "bridge network retry budget ended early or was exceeded"
    );
    for (pair, minimum) in records
        .windows(2)
        .zip([1.8, 4.8, 9.8, 29.8, 29.8, 29.8, 29.8])
    {
        assert!(pair[1]["at"].as_f64().unwrap() - pair[0]["at"].as_f64().unwrap() >= minimum);
    }
    old.plan(
        "codex-retry",
        serde_json::json!([{"disconnect":true},{"text":"old-config-retry"}]),
    )
    .await;
    let before = old.records().await.len();
    send(&state, &id, "codex-switch-during-retry").await;
    seen(&old, before + 1).await;
    new.plan("codex-next", serde_json::json!([])).await;
    settings(
        &state,
        &new,
        "codex-next",
        "new-codex-model",
        ApiFormat::Codex,
    )
    .await;
    assert!(matches!(wait(&state, &id, 20).await, SessionStatus::Idle));
    assert!(
        new.records().await.is_empty(),
        "retry moved to a newly edited provider"
    );
    send(&state, &id, "codex-next-turn").await;
    assert!(matches!(wait(&state, &id, 20).await, SessionStatus::Idle));
    assert_eq!(
        new.records().await.last().unwrap()["body"]["model"],
        "new-codex-model"
    );
    new.plan(
        "codex-next",
        Value::Array(vec![serde_json::json!({"disconnect":true}); 12]),
    )
    .await;
    let before = new.records().await.len();
    send(&state, &id, "cancel-codex-backoff").await;
    seen(&new, before + 1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(state.cancel_turn(&id).await.unwrap());
    assert!(matches!(
        wait(&state, &id, 5).await,
        SessionStatus::Interrupted
    ));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        new.records().await.len(),
        before + 1,
        "cancelled retry timer still issued a request"
    );
    new.plan(
        "codex-next",
        serde_json::json!([{"text":"after-codex-retry-cancel"}]),
    )
    .await;
    send(&state, &id, "recover-codex-backoff").await;
    assert!(matches!(wait(&state, &id, 20).await, SessionStatus::Idle));
}
