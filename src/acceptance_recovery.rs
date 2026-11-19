//! Retain a bounded repair conversation and hand control back to the user.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptanceRecovery {
    #[serde(default)]
    pub epoch: String,
    pub stage: String,
    pub session_id: Option<String>,
    pub attempt: usize,
    pub max_attempts: usize,
    pub waiting_for_user: bool,
    #[serde(default)]
    pub failures: Vec<RecoveryFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryFailure {
    pub attempt: usize,
    pub at: DateTime<Utc>,
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<Value>,
}

impl AcceptanceRecovery {
    pub(crate) fn begin(stage: &str, max_attempts: usize, previous: Option<&Self>) -> Self {
        Self {
            epoch: String::new(),
            stage: stage.into(),
            session_id: previous.and_then(|p| p.session_id.clone()),
            attempt: 0,
            max_attempts,
            waiting_for_user: false,
            failures: previous.map(|p| p.failures.clone()).unwrap_or_default(),
        }
    }

    pub(crate) fn for_run(
        stage: &str,
        max_attempts: usize,
        run: &AcceptanceRun,
        previous: Option<&Self>,
    ) -> Self {
        let epoch = if run.recovery_epoch.is_empty() {
            &run.id
        } else {
            &run.recovery_epoch
        };
        let mut recovery = Self::begin(stage, max_attempts, previous);
        recovery.epoch = epoch.clone();
        if let Some(previous) = previous.filter(|p| p.epoch == *epoch && p.stage == stage) {
            recovery.attempt = previous.attempt;
            recovery.waiting_for_user = previous.waiting_for_user;
        }
        recovery
    }

    pub(crate) fn failed(&mut self, error: &str, configuration: Option<Value>) {
        self.failures.push(RecoveryFailure {
            attempt: self.attempt,
            at: Utc::now(),
            error: error
                .chars()
                .rev()
                .take(16_000)
                .collect::<String>()
                .chars()
                .rev()
                .collect(),
            configuration,
        });
        if self.failures.len() > 20 {
            self.failures.drain(..self.failures.len() - 20);
        }
    }
}

pub(crate) fn repair_limit(value: Option<&str>) -> usize {
    value
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(3)
        .clamp(1, 10)
}

impl AcceptanceStore {
    pub(super) async fn stop_previous_startup(&self, id: &str) -> Result<()> {
        const STOP: &str = r#"
import os, pathlib, signal, time
state = pathlib.Path('/var/lib/x11box/state/x11box')
try:
    pid = int((state / 'start-app.pid').read_text().strip())
    if pid < 2:
        raise ValueError('invalid startup pid')
    command = pathlib.Path('/proc', str(pid), 'cmdline').read_bytes()
    if b'/tmp/omni-acceptance-start.sh' not in command:
        raise SystemExit(0)
    os.kill(pid, signal.SIGTERM)
except (FileNotFoundError, ProcessLookupError):
    raise SystemExit(0)
for _ in range(60):
    try:
        status = pathlib.Path('/proc', str(pid), 'status').read_text()
        if any(line.startswith('State:') and 'Z' in line for line in status.splitlines()):
            raise SystemExit(0)
    except FileNotFoundError:
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit('上一轮应用启动进程尚未退出，停止重复启动')
"#;
        self.runtime
            .docker(&["exec", &container_name(id), "python3", "-c", STOP], 5)
            .await?;
        Ok(())
    }

    pub(super) async fn start_recognized_app(
        &self,
        state: &Arc<AppState>,
        id: &str,
        session_id: &str,
        mut decision: StartupDecision,
        mut initial_failure: Option<anyhow::Error>,
    ) -> Result<Value> {
        let run = self.snapshot(id).await?;
        let max_attempts = repair_limit(
            std::env::var("OMNI_CODE_X11BOX_STARTUP_MAX_ATTEMPTS")
                .ok()
                .as_deref(),
        );
        let mut recovery =
            AcceptanceRecovery::for_run("startup", max_attempts, &run, run.recovery.as_ref());
        recovery.session_id = Some(session_id.into());
        if recovery.waiting_for_user || recovery.attempt >= max_attempts {
            recovery.waiting_for_user = true;
            self.update_automation(id, |run| run.recovery = Some(recovery))
                .await?;
            bail!("自动启动修复次数已用尽，请继续与 Agent 沟通或手动调整后重试");
        }
        let mut last_error: Option<anyhow::Error> = if recovery.attempt > 0 {
            recovery
                .failures
                .last()
                .map(|failure| anyhow::anyhow!(failure.error.clone()))
        } else {
            None
        };
        for attempt in recovery.attempt + 1..=max_attempts {
            require_running(&self.snapshot(id).await?)?;
            recovery.attempt = attempt;
            self.update_automation(id, |run| {
                run.recovery = Some(recovery.clone());
                run.automation = AutomationPhase::StartingApp;
                run.automation_progress =
                    format!("正在启动并验证应用（第 {attempt}/{max_attempts} 轮）");
            })
            .await?;
            let started = if let Some(error) = initial_failure.take() {
                Err(error)
            } else {
                async {
                    if let Some(error) = last_error.as_ref() {
                        decision = self
                            .request_startup_repair(state, id, session_id, &decision, error)
                            .await?;
                    }
                    require_running(&self.snapshot(id).await?)?;
                    self.update_automation(id, |run| run.startup = Some(decision.clone()))
                        .await?;
                    self.launch_start_app(id, &decision).await?;
                    self.wait_for_app_ready(id, Some(&decision)).await
                }
                .await
            };
            match started {
                Ok(observation) => {
                    self.update_automation(id, |run| {
                        run.command = decision.command.clone();
                        run.recovery = Some(recovery.clone());
                    })
                    .await?;
                    return Ok(observation);
                }
                Err(error) => {
                    require_running(&self.snapshot(id).await?)?;
                    let log = self.startup_log(id).await.unwrap_or_default();
                    let diagnostic = format!("{error:#}\n启动日志：\n{log}");
                    recovery.failed(&diagnostic, Some(serde_json::to_value(&decision)?));
                    recovery.waiting_for_user = attempt == max_attempts;
                    self.update_automation(id, |run| {
                        run.recovery = Some(recovery.clone());
                        run.automation_progress = if recovery.waiting_for_user {
                            "自动修复次数已用尽，等待你继续与 Agent 沟通或手动调整".into()
                        } else {
                            format!("启动验证失败，Agent 正在分析第 {attempt} 轮错误")
                        };
                    })
                    .await?;
                    state.emit_system_message(session_id, format!(
                        "应用启动第 {attempt}/{max_attempts} 轮未通过。以下内容是实际诊断数据：\n{diagnostic}\n{}",
                        if recovery.waiting_for_user { "自动修复已停止。你可以在此会话继续与 Agent 修复，或手动调整后返回验收页重试。" } else { "Bridge 将把此错误反馈给当前 Agent 会话继续修复。" }
                    )).await;
                    last_error = Some(error);
                }
            }
        }
        let error = last_error.context("startup failed without a diagnostic")?;
        let run = self.snapshot(id).await?;
        if let Some(environment) = &run.environment {
            if let Err(invalidation) = self
                .environments
                .invalidate_runtime(&environment.id, &run.image_id, &format!("{error:#}"))
                .await
            {
                eprintln!("Failed to invalidate acceptance environment: {invalidation:#}");
            }
        }
        Err(error)
    }

    async fn request_startup_repair(
        &self,
        state: &Arc<AppState>,
        id: &str,
        session_id: &str,
        decision: &StartupDecision,
        error: &anyhow::Error,
    ) -> Result<StartupDecision> {
        let runtime = self.observe_runtime_environment(id).await?;
        let run = self.snapshot(id).await?;
        let log = self.startup_log(id).await.unwrap_or_default();
        let prompt = format!(
            "应用启动或就绪验证失败，请沿当前会话继续修复，结合之前的失败和用户补充，不重复已失败方案。\n实际容器：{}；镜像：{}；冻结源码：{}。\n运行观测（数据）：{runtime}\n当前方案（数据）：{}\n实际错误（数据）：{error:#}\n启动日志（数据）：{log}\n已准备环境（数据）：{}\n{RUNTIME_STARTUP_CONTRACT}\n允许在本轮指定容器内诊断和修复运行配置、启动命令、应用专用图形库路径、可逆的测试前置条件；可以修正本轮独立构建目录中的启动包装脚本。保留已有有效产物及测试数据，不修改受测源码、宿主系统或其他容器，不借用宿主凭据，不伪造成功。重启时保留相同 HOME、X11 和 D-Bus 环境，不能留下多个旧实例。必须在返回的启动方案中完整保留已验证的修复设置，使其可重复执行，不能只在临时诊断命令里设置后丢失。镜像缺少依赖且需要重新构建时明确说明，交由环境准备流程处理。最终只返回完整启动 JSON：原生 kind=desktop、command、app；网页 kind=web、command、url，均保留必要 services 和 prepare_command。容器工作目录 /workspace，不使用登录 shell 覆盖 PATH。",
            container_name(id),
            run.image_id,
            run.revision,
            serde_json::to_string(decision)?,
            run.environment
                .as_ref()
                .map(|e| format!("{}；{}", e.name, e.description))
                .unwrap_or_default(),
        );
        let (_, reply) = send_automation_message(state, session_id, SendMessageInput {
            content: prompt, input_mode: InputMode::Text,
            system_prompt: Some("你负责本轮隔离验收的运行修复，保留真实诊断和会话上下文。与用户沟通时正常解释方案；Bridge 明确要求启动配置时才输出完整 JSON。".into()),
            client_message_id: Some(format!("acceptance-startup-repair-{id}-{}", Uuid::new_v4())),
            provider_id: None, reasoning_effort: None, model: None,
        }).await.map_err(anyhow::Error::msg)?;
        parse_startup_decision(
            &wait_for_reply(state, session_id, &reply.id, Duration::from_secs(600)).await?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn startup_errors_reach_same_agent_and_exhaustion_survives_restart() {
        use crate::{acceptance_test_support::repair_fixture, runtime_plugin::RuntimeConfig};
        use std::os::unix::fs::PermissionsExt;
        let reply = r#"{"kind":"desktop","command":"./fixed-app","app":"Fixture"}"#;
        let (fixture, provider, session) = repair_fixture(reply.into()).await;
        let docker = fixture.root.join("docker");
        std::fs::write(
            &docker,
            "#!/bin/sh\necho startup-failed-fixture >&2\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let runtime = Arc::new(RuntimePluginStore::new(
            fixture.state.runtime_plugins.root.clone(),
            RuntimeConfig {
                image: None,
                source_dir: None,
                docker,
            },
        ));
        let store = Arc::new(AcceptanceStore::new(runtime.clone()));
        let mut run = store.snapshot(&fixture.id).await.unwrap();
        run.phase = RunPhase::Running;
        run.agent_session_id = Some(session.id.clone());
        store.persist(&run).unwrap();
        *store.get(&fixture.id).await.unwrap().lock().await = run;
        let error = store
            .start_recognized_app(
                &fixture.state,
                &fixture.id,
                &session.id,
                parse_startup_decision(reply).unwrap(),
                None,
            )
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("startup-failed-fixture"));
        store
            .set_automation_error(&fixture.id, format!("{error:#}"))
            .await
            .unwrap();
        let retained = store.snapshot(&fixture.id).await.unwrap().recovery.unwrap();
        assert!(retained.waiting_for_user);
        assert_eq!(retained.failures.len(), retained.max_attempts);
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), retained.max_attempts - 1);
        assert!(
            requests.iter().all(
                |(id, content)| id == &session.id && content.contains("startup-failed-fixture")
            )
        );
        drop(requests);
        let transcript = fixture.state.list_messages(&session.id).await.unwrap();
        assert!(
            transcript
                .iter()
                .any(|m| m.content.contains("自动修复已停止"))
        );
        let restored_store = AcceptanceStore::new(runtime);
        let restored = restored_store.snapshot(&fixture.id).await.unwrap();
        assert!(restored.recovery.unwrap().waiting_for_user);
        let requests_before_resume = provider.requests.lock().await.len();
        restored_store
            .start_recognized_app(
                &fixture.state,
                &fixture.id,
                &session.id,
                parse_startup_decision(reply).unwrap(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(provider.requests.lock().await.len(), requests_before_resume);
    }
    #[test]
    fn repair_history_is_bounded_and_manual_retry_keeps_diagnostics() {
        let mut recovery = AcceptanceRecovery::begin("startup", 3, None);
        for i in 1..=25 {
            recovery.attempt = i;
            recovery.failed(&format!("actual failure {i}"), None);
        }
        recovery.waiting_for_user = true;
        let next = AcceptanceRecovery::begin("startup", 3, Some(&recovery));
        assert_eq!(next.failures.len(), 20);
        assert_eq!(next.failures.last().unwrap().error, "actual failure 25");
        assert_eq!(next.attempt, 0);
        assert!(!next.waiting_for_user);
        assert_eq!(repair_limit(None), 3);
        assert_eq!(repair_limit(Some("99")), 10);
        assert_eq!(repair_limit(Some("0")), 1);
    }
}
