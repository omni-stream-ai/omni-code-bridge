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
            id: id.clone(),
            project_id: "test-project".into(),
            session_id: None,
            revision: "test-commit".into(),
            image_id: "test-image".into(),
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

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
