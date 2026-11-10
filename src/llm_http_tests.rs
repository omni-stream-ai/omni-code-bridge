//! Actual authenticated HTTP routes -> bridge -> real model clients -> mock.
use super::*;
use futures_util::StreamExt;
use reqwest::{Client, Method, Response};
use serde_json::json;

struct Http {
    state: Arc<AppState>,
    task: tokio::task::JoinHandle<()>,
    client: Client,
    url: String,
}

impl Http {
    async fn start(root: &Path) -> Self {
        let mut state = AppState::new_with_paths(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
        )
        .await;
        state
            .set_client_auth_path_for_test(root.join("auth.json"))
            .await;
        state.clear_push_devices_for_test().await;
        let state = Arc::new(state);
        let auth = state
            .request_client_auth(crate::models::ClientAuthRequestInput {
                client_id: "model-api-test".into(),
                device_name: None,
            })
            .await
            .unwrap();
        let token = if let Some(token) = auth.token {
            token
        } else {
            state
                .approve_client_auth_for_test(&auth.request_id)
                .await
                .unwrap()
                .token
                .unwrap()
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-omni-code-client-id", "model-api-test".parse().unwrap());
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(90))
            .build()
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::api::router().with_state(Arc::clone(&state));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            state,
            task,
            client,
            url,
        }
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Response {
        let request = self.client.request(method, format!("{}{path}", self.url));
        let request = if let Some(value) = body {
            request.json(&value)
        } else {
            request
        };
        request.send().await.unwrap()
    }

    async fn json(&self, method: Method, path: &str, body: Option<Value>, expected: u16) -> Value {
        let response = self.request(method, path, body).await;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap();
        let value: Value = serde_json::from_str(&body)
            .unwrap_or_else(|error| panic!("{path}: HTTP {status}, invalid JSON {error}: {body}"));
        assert_eq!(status, expected, "{path}: {value}");
        value["data"].clone()
    }

    async fn configure(&self, providers: Value) {
        self.json(
            Method::PUT,
            "/settings",
            Some(json!({"model_providers":providers})),
            200,
        )
        .await;
    }

    async fn create(&self, root: &Path, kind: AgentKind, provider: &str) -> String {
        self.create_with_id(root, kind, provider, &uuid::Uuid::new_v4().to_string())
            .await
    }

    async fn create_with_id(
        &self,
        root: &Path,
        kind: AgentKind,
        provider: &str,
        id: &str,
    ) -> String {
        let project = self
            .json(
                Method::POST,
                "/projects",
                Some(json!({"name":"HTTP mock", "root_path":root})),
                201,
            )
            .await;
        let session = self.json(Method::POST, "/v2/sessions", Some(json!({
            "client_session_id":id,"project_id":project["id"],"title":"HTTP regression", "agent":kind,"provider_id":provider
        })), 201).await;
        assert_eq!(session["id"], id);
        id.to_string()
    }

    fn command(marker: &str, provider: Option<&str>) -> Value {
        json!({"command_id":uuid::Uuid::new_v4().to_string(),"turn_id":uuid::Uuid::new_v4().to_string(),
            "user_message_id":uuid::Uuid::new_v4().to_string(), "content":marker,"attachments":[],
            "input_mode":"text","provider_id":provider})
    }

    async fn turn(&self, id: &str, marker: &str, provider: Option<&str>) -> Value {
        let command = Self::command(marker, provider);
        self.json(
            Method::POST,
            &format!("/v2/sessions/{id}/turns"),
            Some(command.clone()),
            202,
        )
        .await;
        command
    }

    async fn finished(&self, id: &str, turn: &Value, expected: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let snapshot = self
                    .json(Method::GET, &format!("/v2/sessions/{id}/state"), None, 200)
                    .await;
                let turns = snapshot["turns"].as_array().unwrap();
                if let Some(last) = turns.iter().find(|t| t["id"] == turn["turn_id"]) {
                    if ["completed", "failed", "cancelled"]
                        .iter()
                        .any(|status| last["status"] == *status)
                    {
                        assert_eq!(last["status"], expected, "turn outcome: {last}");
                        return snapshot;
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("HTTP turn did not settle")
    }
}

#[path = "llm_handoff_tests.rs"]
mod handoff;

impl Drop for Http {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn provider(
    id: &str,
    server: &Server,
    key: &str,
    model: &str,
    format: &str,
    priority: i32,
) -> Value {
    json!({"id":id,"name":id,"base_url":server.1,"api_key":key,"model":model,"format":format,"enabled":true,"priority":priority})
}

async fn exercise(root: &Path, kind: AgentKind) {
    let first = Server::start();
    let second = Server::start();
    first.plan("key-a", json!([])).await;
    second.plan("key-b", json!([])).await;
    let api = Http::start(root).await;
    let format = if kind == AgentKind::Codex {
        "codex"
    } else {
        "open-ai-compatible"
    };
    let a = provider("bridge-mock", &first, "key-a", "mock-a", format, 0);
    let b = provider("provider-b", &second, "key-b", "mock-b", format, 10);
    api.configure(json!([a, b])).await;

    // Authentication failures cannot mutate settings or execute a model turn.
    let denied = Client::new()
        .put(format!("{}/settings", api.url))
        .json(&json!({"model_providers":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    let denied = Client::new()
        .put(format!("{}/settings", api.url))
        .header("x-omni-code-client-id", "model-api-test")
        .bearer_auth("wrong-token")
        .json(&json!({"model_providers":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    assert_eq!(
        api.json(Method::GET, "/settings", None, 200).await["model_providers"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(first.records().await.is_empty() && second.records().await.is_empty());
    let one = api.create(root, kind, "bridge-mock").await;
    let two = api.create(root, kind, "provider-b").await;
    let turn = api.turn(&one, "http-first-history", None).await;
    let snapshot = api.finished(&one, &turn, "completed").await;
    assert!(snapshot.to_string().contains("mock-ok"));
    assert_eq!(
        first.records().await.last().unwrap()["body"]["model"],
        "mock-a"
    );

    // Replaying the same command must not create a second model request.
    let before = first.records().await.len();
    let replay = api
        .json(
            Method::POST,
            &format!("/v2/sessions/{one}/turns"),
            Some(turn.clone()),
            200,
        )
        .await;
    assert_eq!(replay["created"], false);
    assert_eq!(first.records().await.len(), before);
    // Read a real SSE replay and reconnect using the returned event cursor.
    let cursor = snapshot["cursor"].as_i64().unwrap();
    let response = api
        .request(
            Method::GET,
            &format!("/v2/sessions/{one}/events?after={}", cursor - 1),
            None,
        )
        .await;
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut stream = response.bytes_stream();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&event).contains(&format!("id: {cursor}")));
    drop(stream);
    assert_eq!(
        api.request(
            Method::GET,
            &format!("/v2/sessions/{one}/events?after={}", cursor + 999),
            None
        )
        .await
        .status(),
        409
    );

    // Both live sessions have distinct endpoints; a message override takes
    // precedence without changing the other session's pinned provider.
    let t1 = api.turn(&one, "parallel-one", None).await;
    let t2 = api.turn(&two, "parallel-two", None).await;
    tokio::join!(
        api.finished(&one, &t1, "completed"),
        api.finished(&two, &t2, "completed")
    );
    assert!(
        !first.records().await.last().unwrap()["body"]
            .to_string()
            .contains("parallel-two")
    );
    assert!(
        !second.records().await.last().unwrap()["body"]
            .to_string()
            .contains("parallel-one")
    );
    let overridden = api
        .turn(&one, "message-provider-override", Some("provider-b"))
        .await;
    api.finished(&one, &overridden, "completed").await;
    assert_eq!(
        second.records().await.last().unwrap()["body"]["model"],
        "mock-b"
    );
    api.json(
        Method::PATCH,
        &format!("/v2/sessions/{one}"),
        Some(json!({"model":"session-override"})),
        200,
    )
    .await;
    let t = api.turn(&one, "model override", None).await;
    api.finished(&one, &t, "completed").await;
    assert_eq!(
        first.records().await.last().unwrap()["body"]["model"],
        "session-override"
    );
    api.json(
        Method::PATCH,
        &format!("/v2/sessions/{one}"),
        Some(json!({"model":null})),
        200,
    )
    .await;

    first
        .plan("key-a", json!([{"delay":0.8,"text":"old-snapshot-answer"}]))
        .await;
    let before = first.records().await.len();
    let t = api
        .turn(&one, "switch HTTP configuration in flight", None)
        .await;
    seen(&first, before + 1).await;
    let changed = provider(
        "bridge-mock",
        &second,
        "key-b",
        "changed-in-flight",
        format,
        0,
    );
    api.configure(json!([changed, b])).await;
    api.finished(&one, &t, "completed").await;
    assert_eq!(
        first.records().await.last().unwrap()["body"]["model"],
        "mock-a"
    );
    let t = api.turn(&one, "new snapshot after switch", None).await;
    api.finished(&one, &t, "completed").await;
    assert_eq!(
        second.records().await.last().unwrap()["body"]["model"],
        "changed-in-flight"
    );

    // Concurrent writes may finish in any order; each saved provider must be a
    // complete configuration, and the persisted winner must match GET settings.
    let variants: Vec<_> = (0..8)
        .map(|i| {
            provider(
                "bridge-mock",
                if i % 2 == 0 { &first } else { &second },
                if i % 2 == 0 { "key-a" } else { "key-b" },
                &format!("race-{i}"),
                format,
                0,
            )
        })
        .collect();
    futures_util::future::join_all(variants.iter().map(|p| api.configure(json!([p, b])))).await;
    let saved = api.json(Method::GET, "/settings", None, 200).await;
    let winner = saved["model_providers"][0].clone();
    assert!(variants.contains(&winner));
    let persisted: Value =
        serde_json::from_slice(&std::fs::read(root.join("settings.json")).unwrap()).unwrap();
    assert_eq!(persisted["model_providers"], saved["model_providers"]);
    let t = api.turn(&one, "configuration-race-winner", None).await;
    api.finished(&one, &t, "completed").await;
    let selected = if winner["api_key"] == "key-a" {
        &first
    } else {
        &second
    };
    assert_eq!(
        selected.records().await.last().unwrap()["body"]["model"],
        winner["model"]
    );

    // Validation failures preserve the last working configuration.
    for change in [
        json!({"base_url":""}),
        json!({"base_url":"ftp://bad"}),
        json!({"base_url":"http://"}),
        json!({"base_url":"http://bad host"}),
        json!({"id":""}),
        json!({"name":""}),
    ] {
        let mut invalid = winner.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        assert_eq!(
            api.request(
                Method::PUT,
                "/settings",
                Some(json!({"model_providers":[invalid,b]}))
            )
            .await
            .status(),
            400
        );
    }
    assert_eq!(
        api.json(Method::GET, "/settings", None, 200).await["model_providers"],
        saved["model_providers"]
    );
    // Rebuild AppState and the HTTP listener from the persisted files. The next
    // real model request must retain history, provider, and cleared overrides.
    drop(api);
    let api = Http::start(root).await;
    assert_eq!(
        api.json(Method::GET, "/settings", None, 200).await["model_providers"],
        saved["model_providers"]
    );
    let restored = api
        .json(Method::GET, &format!("/v2/sessions/{one}"), None, 200)
        .await;
    assert!(restored["session"]["model"].is_null() || restored["model"].is_null());
    let t = api.turn(&one, "after-restart-marker", None).await;
    api.finished(&one, &t, "completed").await;
    let body = selected.records().await.last().unwrap()["body"].clone();
    assert_eq!(body["model"], winner["model"]);
    assert!(
        body.to_string().contains("http-first-history"),
        "history lost on restart"
    );

    // A concurrent turn is rejected while the current request is in flight.
    let key = winner["api_key"].as_str().unwrap();
    selected
        .plan(key, json!([{"delay":0.8,"text":"slow-finished"}]))
        .await;
    let before = selected.records().await.len();
    let t = api.turn(&one, "slow HTTP turn", None).await;
    seen(selected, before + 1).await;
    assert_eq!(
        api.request(
            Method::POST,
            &format!("/v2/sessions/{one}/turns"),
            Some(Http::command("overlap", None))
        )
        .await
        .status(),
        409
    );
    api.finished(&one, &t, "completed").await;
    // Cancel through the real turn route, then ensure a late response cannot
    // overwrite the next turn's result or mutate its terminal domain state.
    selected
        .plan(key, json!([{"delay":1.0,"text":"stale-cancelled-text"}]))
        .await;
    let before = selected.records().await.len();
    let t = api.turn(&one, "HTTP cancellation", None).await;
    seen(selected, before + 1).await;
    let response = api
        .request(
            Method::POST,
            &format!(
                "/v2/sessions/{one}/turns/{}/cancel",
                t["turn_id"].as_str().unwrap()
            ),
            None,
        )
        .await;
    assert!(response.status().is_success());
    api.finished(&one, &t, "cancelled").await;
    selected
        .plan(key, json!([{"text":"after-http-cancel"}]))
        .await;
    let t = api.turn(&one, "after cancellation", None).await;
    let snapshot = api.finished(&one, &t, "completed").await;
    let last = snapshot["turns"].as_array().unwrap().last().unwrap();
    assert!(last.to_string().contains("after-http-cancel"));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let snapshot = api
        .json(Method::GET, &format!("/v2/sessions/{one}/state"), None, 200)
        .await;
    assert!(
        !snapshot["turns"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .to_string()
            .contains("stale-cancelled-text")
    );
}

#[tokio::test]
async fn model_api_http_pi_lifecycle() {
    let Some(root) = isolated("http::model_api_http_pi_lifecycle").await else {
        return;
    };
    exercise(&root, AgentKind::Pi).await;
}

#[tokio::test]
async fn model_api_http_real_codex_lifecycle() {
    let Some(root) = isolated("http::model_api_http_real_codex_lifecycle").await else {
        return;
    };
    let binary = codex_binary_path();
    assert!(
        binary.is_file(),
        "real Codex binary is required; set OMNI_CODE_CODEX_BIN"
    );
    exercise(&root, AgentKind::Codex).await;
}

#[cfg(unix)]
#[tokio::test]
async fn model_api_http_real_codex_consumes_queued_followup() {
    // The subprocess must inherit the real executable, not a concurrent mock's env.
    let _lock = crate::adapter::tests::codex_test_lock().await;
    let Some(root) = isolated("http::model_api_http_real_codex_consumes_queued_followup").await
    else {
        return;
    };
    assert!(codex_binary_path().is_file(), "real Codex is required");
    let server = Server::start();
    server
        .plan(
            "steering-local-key",
            json!([
                {"delay":1.0,"text":"original response"},
                {"text":"handled-native-followup"}
            ]),
        )
        .await;
    let api = Http::start(&root).await;
    api.configure(json!([provider(
        "bridge-mock",
        &server,
        "steering-local-key",
        "mock-codex",
        "codex",
        0
    )]))
    .await;
    let id = api.create(&root, AgentKind::Codex, "bridge-mock").await;
    let turn = api.turn(&id, "continue the original work", None).await;
    seen(&server, 1).await;
    let command = json!({
        "command_id":"native-steer-command",
        "user_message_id":"native-steered-user",
        "content":"native-steered-marker",
        "attachments":[]
    });
    let path = format!(
        "/v2/sessions/{id}/turns/{}/steer",
        turn["turn_id"].as_str().unwrap()
    );
    let queued = api
        .json(Method::POST, &path, Some(command.clone()), 202)
        .await;
    assert_eq!(queued["message"]["state"], "pending");
    let duplicate = api.json(Method::POST, &path, Some(command), 202).await;
    assert_eq!(duplicate["created"], false);
    let snapshot = api.finished(&id, &turn, "completed").await;
    assert_eq!(snapshot["turns"].as_array().unwrap().len(), 1, "{snapshot}");
    let messages = snapshot["turns"][0]["segments"].as_array().unwrap();
    let input = messages
        .iter()
        .find_map(|segment| {
            segment
                .get("message")
                .filter(|message| message["id"] == "native-steered-user")
        })
        .expect("steered input retained");
    assert_eq!(input["state"], "completed");
    let requests = server.records().await;
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests[1]["body"]
            .to_string()
            .contains("native-steered-marker")
    );
    assert!(snapshot.to_string().contains("handled-native-followup"));
}

#[tokio::test]
async fn model_api_http_settings_save_failure() {
    let Some(root) = isolated("http::model_api_http_settings_save_failure").await else {
        return;
    };
    let server = Server::start();
    let api = Http::start(&root).await;
    let original = provider("bridge-mock", &server, "old-key", "old-model", "codex", 0);
    api.configure(json!([original])).await;
    let path = root.join("settings.json");
    let backup = root.join("saved-settings.json");
    std::fs::rename(&path, &backup).unwrap();
    std::fs::create_dir(&path).unwrap();
    let replacement = provider("bridge-mock", &server, "new-key", "new-model", "codex", 0);
    assert!(
        !api.request(
            Method::PUT,
            "/settings",
            Some(json!({"model_providers":[replacement]}))
        )
        .await
        .status()
        .is_success()
    );
    assert_eq!(
        api.json(Method::GET, "/settings", None, 200).await["model_providers"],
        json!([original]),
        "a failed save must not change active credentials or routing"
    );
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(&backup, &path).unwrap();
    drop(api);
    let api = Http::start(&root).await;
    assert_eq!(
        api.json(Method::GET, "/settings", None, 200).await["model_providers"],
        json!([original])
    );
}
