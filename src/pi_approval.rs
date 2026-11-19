//! Apply the session's approval policy before Pi's built-in tools execute.
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use pi::sdk::{
    Config, Tool, ToolFactory, ToolOutput, ToolRegistry, ToolUpdate, default_tool_registry,
};
use pi::tools::ToolEffects;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::{
    app_state::AppState,
    models::{ApprovalChoice, ApprovalKind, ApprovalMode, ApprovalRequest},
};

pub struct ApprovalToolFactory {
    pub state: Arc<AppState>,
    pub session_id: String,
}

impl ToolFactory for ApprovalToolFactory {
    fn create_tool_registry(&self, enabled: &[&str], cwd: &Path, config: &Config) -> ToolRegistry {
        let gate = Arc::new(Mutex::new(()));
        let tools = default_tool_registry(enabled, cwd, config)
            .into_tools()
            .into_iter()
            .map(|inner| {
                Box::new(ApprovalTool {
                    inner,
                    state: self.state.clone(),
                    session_id: self.session_id.clone(),
                    cwd: cwd.to_path_buf(),
                    gate: gate.clone(),
                }) as Box<dyn Tool>
            })
            .collect();
        ToolRegistry::from_tools(tools)
    }
}

struct ApprovalTool {
    inner: Box<dyn Tool>,
    state: Arc<AppState>,
    session_id: String,
    cwd: PathBuf,
    // The application's approval card represents one pending tool at a time.
    gate: Arc<Mutex<()>>,
}

impl ApprovalTool {
    fn request(&self, input: &Value) -> ApprovalRequest {
        let bash = self.name() == "bash";
        let command = if bash {
            input
                .get("command")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            Some(format!("{} {}", self.name(), input))
        };
        ApprovalRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            cwd: Some(self.cwd.to_string_lossy().into_owned()),
            kind: if bash {
                ApprovalKind::ExecCommand
            } else {
                ApprovalKind::FileChange
            },
            command,
            reason: Some(format!("Pi: {}", self.label())),
            auto_approval_reason: None,
            auto_approval_reason_kind: None,
            allow_accept_for_session: false,
            allow_cancel: true,
            resolvable: true,
        }
    }

    fn safe_read(&self, input: &Value) -> bool {
        if !matches!(self.name(), "read" | "grep" | "find" | "ls") {
            return false;
        }
        let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
        if path.contains("://") || path.starts_with('~') {
            return false;
        }
        let path = self.cwd.join(path).to_string_lossy().into_owned();
        let quoted = format!("'{}'", path.replace('\'', "'\\''"));
        crate::approval_policy::should_auto_approve(&format!("cat -- {quoted}"), &self.cwd)
            .is_some()
    }

    async fn approve(&self, input: &Value) -> pi::PiResult<bool> {
        let _gate = self.gate.lock().await;
        let mode = self.state.session_approval_mode(&self.session_id).await;
        if mode == ApprovalMode::Smart && self.safe_read(input) {
            return Ok(true);
        }
        let mut request = self.request(input);
        let automatic = crate::adapter::auto_approve_codex_request_for_state(
            &self.state,
            &self.session_id,
            &mut request,
            &self.cwd,
        )
        .await
        .map_err(|error| pi::Error::tool(self.name(), error.to_string()))?;
        let choice = if let Some(choice) = automatic {
            choice
        } else {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            self.state
                .set_approval_sender(&self.session_id, sender)
                .await;
            self.state
                .raise_approval(&self.session_id, request.clone())
                .await;
            let choice = receiver.recv().await.unwrap_or(ApprovalChoice::Cancel);
            self.state
                .resolve_approval(&self.session_id, &request.request_id, choice.clone())
                .await;
            self.state.clear_approval_sender(&self.session_id).await;
            choice
        };
        Ok(matches!(
            choice,
            ApprovalChoice::Accept | ApprovalChoice::AcceptForSession | ApprovalChoice::AlwaysAllow
        ))
    }
}

#[async_trait]
impl Tool for ApprovalTool {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn label(&self) -> &str {
        self.inner.label()
    }
    fn description(&self) -> &str {
        self.inner.description()
    }
    fn parameters(&self) -> Value {
        self.inner.parameters()
    }
    fn effects(&self) -> ToolEffects {
        self.inner.effects()
    }

    async fn execute(
        &self,
        tool_call_id: &str,
        input: Value,
        on_update: Option<Box<dyn Fn(ToolUpdate) + Send + Sync>>,
    ) -> pi::PiResult<ToolOutput> {
        if !self.approve(&input).await? {
            return Err(pi::Error::tool(
                self.name(),
                "Tool execution was not approved",
            ));
        }
        self.inner.execute(tool_call_id, input, on_update).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{AgentKind, CreateProjectInput, CreateSessionInput};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountedTool(Arc<AtomicUsize>);
    #[async_trait]
    impl Tool for CountedTool {
        fn name(&self) -> &str {
            "write"
        }
        fn label(&self) -> &str {
            "Write"
        }
        fn description(&self) -> &str {
            "Count execution without changing files"
        }
        fn parameters(&self) -> Value {
            json!({})
        }
        async fn execute(
            &self,
            _: &str,
            _: Value,
            _: Option<Box<dyn Fn(ToolUpdate) + Send + Sync>>,
        ) -> pi::PiResult<ToolOutput> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput {
                content: vec![],
                details: None,
                is_error: false,
            })
        }
    }

    #[tokio::test]
    async fn pi_tools_wait_for_manual_approval_and_auto_mode_releases_pending_tools() {
        let fixture = crate::acceptance_test_support::Fixture::new().await;
        let state = fixture.state.clone();
        state.clear_push_devices_for_test().await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Pi approvals".into(),
                root_path: fixture.root.to_string_lossy().into(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: uuid::Uuid::new_v4().to_string(),
                project_id: project.id,
                title: None,
                agent: AgentKind::Pi,
                brief_reply_mode: false,
                approval_mode: Some(ApprovalMode::Manual),
                provider_id: None,
                model: None,
                reasoning_effort: None,
            })
            .await
            .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let tool = Arc::new(ApprovalTool {
            inner: Box::new(CountedTool(count.clone())),
            state: state.clone(),
            session_id: session.id.clone(),
            cwd: fixture.root.clone(),
            gate: Arc::new(Mutex::new(())),
        });
        for allow in [false, true] {
            let runner = tool.clone();
            let pending = tokio::spawn(async move {
                runner
                    .execute("write", json!({"path":"note.txt","content":"test"}), None)
                    .await
            });
            let request = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let Some(request) = state.pending_session_approval(&session.id).await {
                        break request;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(count.load(Ordering::SeqCst), 0);
            if allow {
                state
                    .update_session_approval_mode(&session.id, ApprovalMode::Auto)
                    .await
                    .unwrap();
                assert!(pending.await.unwrap().is_ok());
            } else {
                state
                    .submit_approval(&session.id, &request.request_id, ApprovalChoice::Decline)
                    .await
                    .unwrap();
                assert!(pending.await.unwrap().is_err());
            }
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        tool.execute("write-again", json!({"path":"other.txt"}), None)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
