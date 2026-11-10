//! Independent native agent -> HTTP client -> native agent -> HTTP client.
use super::*;

async fn native_codex(
    root: &Path,
    server: &Server,
    model: &str,
    marker: &str,
    resume: Option<&str>,
) -> String {
    let mut command = tokio::process::Command::new(codex_binary_path());
    command.arg("exec");
    if let Some(id) = resume {
        command.args(["resume", id]);
    }
    command.args([
        "--json",
        "--skip-git-repo-check",
        "-c",
        "model_provider=\"native\"",
        "-c",
        &format!("model={}", json!(model)),
        "-c",
        "model_providers.native.name=\"Native mock\"",
        "-c",
        &format!("model_providers.native.base_url={}", json!(server.1)),
        "-c",
        "model_providers.native.wire_api=\"responses\"",
        "-c",
        "model_providers.native.env_key=\"HANDOFF_FAKE_KEY\"",
        "-c",
        "model_providers.native.request_max_retries=0",
        "-c",
        "model_providers.native.stream_max_retries=0",
        "-c",
        "approval_policy=\"never\"",
        marker,
    ]);
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        command
            .current_dir(root)
            .env("HANDOFF_FAKE_KEY", "native-key")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "native Codex: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|v| v["type"] == "thread.started")
        .unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .into()
}

async fn snapshot(api: &Http, id: &str, markers: &[&str]) -> Value {
    // Exercise the actual client read path repeatedly, without restarting bridge.
    let value = api
        .json(Method::GET, &format!("/v2/sessions/{id}/state"), None, 200)
        .await;
    let text = value.to_string();
    for marker in markers {
        assert!(text.contains(marker), "client missing {marker}: {value}");
    }
    value
}

fn assert_history(record: &Value, model: &str, markers: &[&str]) {
    assert_eq!(record["body"]["model"], model);
    let text = record["body"].to_string();
    for marker in markers {
        assert!(text.contains(marker), "agent missing {marker}: {record}");
    }
}

async fn watch_native(api: &Http, id: &str) -> (Response, i64) {
    let snapshot = snapshot(api, id, &[]).await;
    let cursor = snapshot["cursor"].as_i64().unwrap();
    let response = api
        .request(
            Method::GET,
            &format!("/v2/sessions/{id}/events?after={cursor}"),
            None,
        )
        .await;
    assert_eq!(response.status(), 200);
    (response, cursor)
}

async fn expect_native_notification(watch: (Response, i64)) {
    let (response, cursor) = watch;
    let mut stream = response.bytes_stream();
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk.unwrap());
            while let Some(end) = buffer.windows(2).position(|bytes| bytes == b"\n\n") {
                let bytes = buffer.drain(..end + 2).collect::<Vec<_>>();
                let frame = String::from_utf8(bytes).unwrap();
                for line in frame.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        let event: Value = serde_json::from_str(data).unwrap();
                        if event["event_type"] == "stream.reset_required"
                            && event["event_id"].as_i64().unwrap() > cursor
                        {
                            return;
                        }
                    }
                }
            }
        }
        panic!("native update event stream ended");
    })
    .await
    .expect("open client did not receive native history update without a refresh");
}

#[tokio::test]
async fn model_api_codex_native_client_handoff() {
    let Some(root) = isolated("http::handoff::model_api_codex_native_client_handoff").await else {
        return;
    };
    let a = Server::start();
    let b = Server::start();
    let config_path = root.join("codex/config.toml");
    let config = std::fs::read_to_string(&config_path).unwrap();
    std::fs::write(&config_path, format!("{config}\n[model_providers.native]\nname = \"Native mock\"\nrequest_max_retries = 0\nstream_max_retries = 0\n")).unwrap();
    a.plan("native-key", json!([{"text":"native-answer-1"}]))
        .await;
    let id = native_codex(&root, &a, "native-model-1", "native-question-1", None).await;
    let api = Http::start(&root).await;
    a.plan("bridge-key", json!([])).await;
    b.plan("other-key", json!([])).await;
    api.configure(json!([
        provider(
            "bridge-mock",
            &a,
            "bridge-key",
            "client-model-1",
            "codex",
            0
        ),
        provider("other", &b, "other-key", "client-model-2", "codex", 1)
    ]))
    .await;
    snapshot(&api, &id, &["native-question-1", "native-answer-1"]).await;
    api.json(
        Method::PATCH,
        &format!("/v2/sessions/{id}"),
        Some(json!({"provider_id":"bridge-mock"})),
        200,
    )
    .await;
    a.plan("bridge-key", json!([{"text":"client-answer-1"}]))
        .await;
    let turn = api.turn(&id, "client-question-1", None).await;
    api.finished(&id, &turn, "completed").await;
    assert_single_codex_session(&api, &id).await;
    assert_history(
        a.records().await.last().unwrap(),
        "client-model-1",
        &["native-question-1", "native-answer-1"],
    );
    let watch = watch_native(&api, &id).await;
    b.plan("native-key",json!([{"tool":{"name":"__mock_shell__","command":"printf handoff-tool-marker"}},{"text":"native-answer-2"}])).await;
    assert_eq!(
        native_codex(&root, &b, "native-model-2", "native-question-2", Some(&id)).await,
        id
    );
    expect_native_notification(watch).await;
    assert_history(
        b.records().await.last().unwrap(),
        "native-model-2",
        &[
            "native-question-1",
            "client-question-1",
            "client-answer-1",
            "handoff-tool-marker",
        ],
    );
    let before = snapshot(
        &api,
        &id,
        &["client-question-1", "native-question-2", "native-answer-2"],
    )
    .await;
    let again = snapshot(&api, &id, &["native-question-2"]).await;
    assert_eq!(
        before["cursor"], again["cursor"],
        "read duplicated external history"
    );
    b.plan("other-key", json!([{"text":"client-answer-2"}]))
        .await;
    let turn = api.turn(&id, "client-question-2", Some("other")).await;
    api.finished(&id, &turn, "completed").await;
    assert_single_codex_session(&api, &id).await;
    assert_history(
        b.records().await.last().unwrap(),
        "client-model-2",
        &[
            "native-question-1",
            "client-question-1",
            "native-question-2",
            "native-answer-2",
            "handoff-tool-marker",
        ],
    );
    snapshot(
        &api,
        &id,
        &[
            "native-question-1",
            "client-question-1",
            "native-question-2",
            "client-question-2",
        ],
    )
    .await;
    // External changes must not silently override the client's next selection,
    // even when bridge metadata still says the previous model is selected.
    a.plan("native-key", json!([{"text":"native-answer-3"}]))
        .await;
    assert_eq!(
        native_codex(&root, &a, "native-model-3", "native-question-3", Some(&id)).await,
        id
    );
    snapshot(
        &api,
        &id,
        &["native-question-3", "native-answer-3", "client-answer-2"],
    )
    .await;
    api.json(
        Method::PATCH,
        &format!("/v2/sessions/{id}"),
        Some(json!({"model":"session-handoff-override"})),
        200,
    )
    .await;
    b.plan("other-key", json!([{"text":"client-answer-3"}]))
        .await;
    let turn = api.turn(&id, "client-question-3", Some("other")).await;
    api.finished(&id, &turn, "completed").await;
    assert_single_codex_session(&api, &id).await;
    assert_history(
        b.records().await.last().unwrap(),
        "session-handoff-override",
        &[
            "native-question-3",
            "native-answer-3",
            "handoff-tool-marker",
        ],
    );
    api.json(
        Method::PATCH,
        &format!("/v2/sessions/{id}"),
        Some(json!({"model":null})),
        200,
    )
    .await;
    // A failed provider switch must retain the shared transcript for recovery.
    a.plan("bridge-key", json!([{"status":401}])).await;
    let turn = api.turn(&id, "failed-switch-question", None).await;
    api.finished(&id, &turn, "failed").await;
    a.plan("bridge-key", json!([{"text":"recovered-handoff"}]))
        .await;
    let turn = api.turn(&id, "recover-switch-question", None).await;
    api.finished(&id, &turn, "completed").await;
    assert_history(
        a.records().await.last().unwrap(),
        "client-model-1",
        &[
            "native-question-1",
            "client-question-2",
            "native-question-3",
            "client-answer-3",
        ],
    );
    drop(api);
    let api = Http::start(&root).await;
    assert_single_codex_session(&api, &id).await;
    snapshot(
        &api,
        &id,
        &[
            "native-question-1",
            "native-question-2",
            "native-question-3",
            "client-question-3",
            "recovered-handoff",
        ],
    )
    .await;
}

async fn assert_single_codex_session(api: &Http, id: &str) {
    let sessions = api.json(Method::GET, "/v2/sessions", None, 200).await;
    let codex = sessions
        .as_array()
        .unwrap()
        .iter()
        .filter(|session| session["agent"] == "codex")
        .collect::<Vec<_>>();
    assert_eq!(
        codex.len(),
        1,
        "continuation created duplicate sessions: {sessions}"
    );
    assert_eq!(codex[0]["id"], id);
    assert_eq!(
        api.state.provider_session_ref(id).await.as_deref(),
        Some(id)
    );
}

#[tokio::test]
async fn model_api_pi_native_client_handoff() {
    let Some(root) = isolated("http::handoff::model_api_pi_native_client_handoff").await else {
        return;
    };
    let a = Server::start();
    let b = Server::start();
    let id = uuid::Uuid::new_v4().to_string();
    // Pi is embedded in this product. Use an independent real SDK handle,
    // sharing only its native transcript path with the bridge.
    let dir = root.join("pi-sessions");
    std::fs::create_dir_all(&dir).unwrap();
    a.plan("native-key", json!([{"text":"native-pi-answer-1"}]))
        .await;
    let mut native = a
        .agent(
            &dir,
            &id,
            "native-key",
            "native-pi-1",
            ApiFormat::OpenAiCompatible,
        )
        .await;
    prompt(&mut native, "native-pi-question-1").await;
    drop(native);
    let api = Http::start(&root).await;
    a.plan("bridge-key", json!([])).await;
    api.configure(json!([provider(
        "bridge-mock",
        &a,
        "bridge-key",
        "client-pi-1",
        "open-ai-compatible",
        0
    )]))
    .await;
    assert_eq!(
        api.create_with_id(&root, AgentKind::Pi, "bridge-mock", &id)
            .await,
        id
    );
    assert_eq!(
        api.state.pi_session_path(&id),
        dir.join(format!("{id}.jsonl"))
    );
    snapshot(&api, &id, &["native-pi-question-1", "native-pi-answer-1"]).await;
    a.plan("bridge-key", json!([{"text":"client-pi-answer-1"}]))
        .await;
    let turn = api.turn(&id, "client-pi-question-1", None).await;
    api.finished(&id, &turn, "completed").await;
    assert_history(
        a.records().await.last().unwrap(),
        "client-pi-1",
        &["native-pi-question-1", "native-pi-answer-1"],
    );
    std::fs::write(dir.join("hello.txt"), "pi-handoff-tool-marker").unwrap();
    let watch = watch_native(&api, &id).await;
    b.plan("native-key",json!([{"tool":{"name":"read","arguments":{"path":"hello.txt"}}},{"text":"native-pi-answer-2"}])).await;
    let mut native = b
        .agent(
            &dir,
            &id,
            "native-key",
            "native-pi-2",
            ApiFormat::AnthropicMessages,
        )
        .await;
    prompt(&mut native, "native-pi-question-2").await;
    drop(native);
    expect_native_notification(watch).await;
    assert_history(
        b.records().await.last().unwrap(),
        "native-pi-2",
        &[
            "client-pi-question-1",
            "client-pi-answer-1",
            "pi-handoff-tool-marker",
        ],
    );
    let before = snapshot(&api, &id, &["native-pi-question-2", "native-pi-answer-2"]).await;
    assert_eq!(
        before["cursor"],
        snapshot(&api, &id, &["native-pi-question-2"]).await["cursor"]
    );
    b.plan("bridge-key", json!([{"text":"client-pi-answer-2"}]))
        .await;
    api.configure(json!([provider(
        "bridge-mock",
        &b,
        "bridge-key",
        "client-pi-2",
        "codex",
        0
    )]))
    .await;
    let turn = api.turn(&id, "client-pi-question-2", None).await;
    api.finished(&id, &turn, "completed").await;
    assert_history(
        b.records().await.last().unwrap(),
        "client-pi-2",
        &[
            "native-pi-question-1",
            "client-pi-question-1",
            "native-pi-question-2",
            "native-pi-answer-2",
            "pi-handoff-tool-marker",
        ],
    );
    snapshot(
        &api,
        &id,
        &[
            "native-pi-question-1",
            "client-pi-question-1",
            "native-pi-question-2",
            "client-pi-question-2",
        ],
    )
    .await;
    a.plan("native-key", json!([{"text":"native-pi-answer-3"}]))
        .await;
    let mut native = a
        .agent(
            &dir,
            &id,
            "native-key",
            "native-pi-3",
            ApiFormat::OpenAiCompatible,
        )
        .await;
    prompt(&mut native, "native-pi-question-3").await;
    drop(native);
    snapshot(
        &api,
        &id,
        &[
            "native-pi-question-3",
            "native-pi-answer-3",
            "client-pi-answer-2",
        ],
    )
    .await;
    b.plan("bridge-key", json!([{"text":"client-pi-answer-3"}]))
        .await;
    let turn = api.turn(&id, "client-pi-question-3", None).await;
    api.finished(&id, &turn, "completed").await;
    assert_history(
        b.records().await.last().unwrap(),
        "client-pi-2",
        &[
            "native-pi-question-3",
            "native-pi-answer-3",
            "pi-handoff-tool-marker",
        ],
    );
    drop(api);
    let api = Http::start(&root).await;
    snapshot(
        &api,
        &id,
        &[
            "native-pi-question-1",
            "native-pi-question-2",
            "native-pi-question-3",
            "client-pi-answer-3",
        ],
    )
    .await;
}
