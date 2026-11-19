use crate::{
    acceptance::{AcceptanceRun, Artifact, RunPhase},
    app_state::AppState,
    models::ClientAuthRequestInput,
};
use axum::http::{HeaderMap, header};
use chrono::Utc;
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

pub struct Fixture {
    pub root: PathBuf,
    pub state: Arc<AppState>,
    pub headers: HeaderMap,
    pub id: String,
}

impl Fixture {
    pub async fn with_provider(provider: Arc<dyn crate::adapter::AgentProvider>) -> Self {
        let mut fixture = Self::new().await;
        let registry = crate::adapter::ProviderRegistry::from_map(std::collections::HashMap::from(
            [(crate::models::AgentKind::Codex, provider)],
        ));
        let mut state = AppState::new_with_paths_and_providers(
            fixture.root.join("settings.json"),
            fixture.root.join("runtime.json"),
            fixture.root.join("metadata.json"),
            registry,
        )
        .await;
        state
            .set_client_auth_path_for_test(fixture.root.join("auth.json"))
            .await;
        state.clear_push_devices_for_test().await;
        fixture.state = Arc::new(state);
        fixture
    }
    pub async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("acceptance-http-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut state = AppState::new_with_paths(
            root.join("settings.json"),
            root.join("runtime.json"),
            root.join("metadata.json"),
        )
        .await;
        state
            .set_client_auth_path_for_test(root.join("auth.json"))
            .await;
        let state = Arc::new(state);
        let record = state
            .request_client_auth(ClientAuthRequestInput {
                client_id: "acceptance-test".into(),
                device_name: None,
            })
            .await
            .unwrap();
        let token = state
            .approve_client_auth_for_test(&record.request_id)
            .await
            .unwrap()
            .token
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-omni-code-client-id", "acceptance-test".parse().unwrap());
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let run = AcceptanceRun {
            recovery_epoch: Uuid::new_v4().to_string(),
            recovery: None,
            pending_approvals: Vec::new(),
            id: id.clone(),
            project_id: "test-project".into(),
            session_id: None,
            agent_session_id: None,
            plan_agent_session_id: None,
            revision: "test-commit".into(),
            image_id: "test-image".into(),
            environment: None,
            environment_reused: false,
            command: "test".into(),
            size: "mobile".into(),
            created_by: "acceptance-test".into(),
            phase: RunPhase::Completed,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::hours(1),
            error: None,
            desktop_port: None,
            controller: None,
            control_expires_at: None,
            control_generation: 0,
            result: None,
            artifacts: vec![Artifact {
                name: "recording.mp4".into(),
                content_type: "video/mp4".into(),
                bytes: 10,
                created_at: now,
            }],
            automation: crate::acceptance::AutomationPhase::Pending,
            automation_progress: String::new(),
            checklist: vec![],
            auto_verdict: None,
            automation_error: None,
            requirements: None,
            requirement_brief: None,
            requirement_agent_session_ids: Vec::new(),
            change_summary: String::new(),
            startup: None,
            startup_overridden: false,
            automatic: false,
            test_plan: None,
            plan_messages: Vec::new(),
        };
        let run_dir = state.runtime_plugins.root.join("runs").join(&id);
        std::fs::create_dir_all(run_dir.join("artifacts")).unwrap();
        std::fs::write(run_dir.join("artifacts/recording.mp4"), b"0123456789").unwrap();
        crate::runtime_plugin::atomic_json(&run_dir.join("run.json"), &run).unwrap();
        Self {
            root,
            state,
            headers,
            id,
        }
    }

    pub async fn server(&self) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::acceptance_api::router()
            .merge(crate::desktop_gateway::router())
            .with_state(Arc::clone(&self.state));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, task)
    }
}

pub struct RepairProvider {
    pub reply: String,
    pub requests: tokio::sync::Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl crate::adapter::AgentProvider for RepairProvider {
    async fn run_session(
        &self,
        state: Arc<AppState>,
        session: crate::models::SessionSummary,
        input: crate::models::ChatMessage,
        _system_prompt: Option<String>,
        reply: crate::models::ChatMessage,
        _provider: Option<crate::models::ResolvedProviderConfig>,
        _reasoning: Option<crate::models::ReasoningEffort>,
    ) -> anyhow::Result<()> {
        self.requests
            .lock()
            .await
            .push((session.id.clone(), input.content));
        state
            .emit_assistant_message_snapshot(&session.id, &reply.id, &self.reply)
            .await;
        state
            .finish_assistant_message(&session.id, &reply.id)
            .await
            .map_err(anyhow::Error::msg)
    }
}

pub async fn repair_fixture(
    reply: String,
) -> (Fixture, Arc<RepairProvider>, crate::models::SessionSummary) {
    use crate::models::{AgentKind, CreateProjectInput, CreateSessionInput};
    let provider = Arc::new(RepairProvider {
        reply,
        requests: tokio::sync::Mutex::new(Vec::new()),
    });
    let fixture = Fixture::with_provider(provider.clone()).await;
    let project = fixture
        .state
        .create_project(CreateProjectInput {
            name: "Repair fixture".into(),
            root_path: fixture.root.to_string_lossy().into(),
        })
        .await;
    let session = fixture
        .state
        .create_session(CreateSessionInput {
            client_session_id: format!("acceptance-repair-fixture-{}", Uuid::new_v4()),
            project_id: project.id.clone(),
            title: Some("Repair fixture".into()),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            approval_mode: Default::default(),
            provider_id: None,
            reasoning_effort: None,
            model: None,
        })
        .await
        .unwrap();
    let shared = fixture.state.acceptance.get(&fixture.id).await.unwrap();
    let mut run = shared.lock().await;
    run.project_id = project.id;
    run.session_id = Some(session.id.clone());
    crate::runtime_plugin::atomic_json(
        &fixture
            .state
            .runtime_plugins
            .root
            .join("runs")
            .join(&fixture.id)
            .join("run.json"),
        &*run,
    )
    .unwrap();
    drop(run);
    (fixture, provider, session)
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
