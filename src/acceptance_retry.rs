//! Retry retained acceptance sources and approved cases after runtime failures.
use super::*;

impl AcceptanceStore {
    pub async fn retry_automation(
        self: &Arc<Self>,
        state: Arc<AppState>,
        id: &str,
        startup: Option<StartupDecision>,
    ) -> Result<AcceptanceRun> {
        self.retry_with_environment(state, id, startup, false).await
    }

    pub async fn retry_with_environment(
        self: &Arc<Self>,
        state: Arc<AppState>,
        id: &str,
        startup: Option<StartupDecision>,
        use_current_environment: bool,
    ) -> Result<AcceptanceRun> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        if self.auto_started.lock().await.contains(id)
            || self
                .preparing_sources
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(id)
        {
            bail!("当前不能重试：上一轮任务仍在执行或清理，请稍后重试");
        }
        let shared = self.get(id).await?;
        let mut live = shared.lock().await;
        let previous = self
            .failed_saves
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned()
            .unwrap_or_else(|| live.clone());
        let restart_desktop = previous.phase == RunPhase::Failed || use_current_environment;
        if previous.result.is_some()
            || !(previous.phase == RunPhase::Failed
                || (previous.phase == RunPhase::Running
                    && (previous.automation == AutomationPhase::Failed
                        || (previous.automation == AutomationPhase::AwaitingConfirmation
                            && previous.auto_verdict.as_ref().is_some_and(|v| !v.passed)))))
        {
            bail!("当前不能重试：仅支持环境准备失败或尚未人工确认的失败验收");
        }
        for session_id in [
            previous
                .recovery
                .as_ref()
                .and_then(|r| r.session_id.as_deref()),
            previous
                .environment
                .as_ref()
                .and_then(|e| e.agent_session_id.as_deref()),
        ]
        .into_iter()
        .flatten()
        {
            if state.get_session(session_id).await.is_some_and(|detail| {
                matches!(
                    detail.session.status,
                    crate::models::SessionStatus::Running
                        | crate::models::SessionStatus::AwaitingApproval
                )
            }) {
                bail!("修复 Agent 仍在处理消息，请等待当前对话或审批完成后再重试");
            }
        }
        if previous.automatic || !restart_desktop {
            if previous.test_plan.is_none() {
                bail!("请先确认测试用例；没有已确认用例的旧任务请重新创建验收");
            }
            plan::confirmed_requirements(&previous)?;
        }
        let runtime_phase = self.runtime.status().await?.phase;
        let can_restore = self.runtime.config.image.is_some()
            || self.runtime.config.source_dir.is_some()
            || runtime_phase.in_progress();
        if runtime_phase != InstallPhase::Ready && !(restart_desktop && can_restore) {
            bail!("x11box plugin is not ready，请先恢复验收运行环境后重试");
        }
        let mut next = previous.clone();
        next.recovery_epoch = Uuid::new_v4().to_string();
        if use_current_environment {
            let archive = self.run_dir(id)?.join("source.tar");
            let previous_environment = previous
                .environment
                .as_ref()
                .context("原验收环境配置缺失")?;
            let mut environment = self.environments.describe_with_config(
                &previous.project_id,
                &previous_environment.base_image_id,
                &archive,
                self.environments.config(&previous.project_id)?,
            )?;
            if environment.agent_session_id.is_none() {
                environment.agent_session_id = previous_environment.agent_session_id.clone();
                environment.failure_history = previous_environment.failure_history.clone();
                environment.recovery = previous_environment.recovery.clone();
            }
            next.environment = Some(environment);
            next.startup = None;
            next.startup_overridden = false;
        }
        if let Some(startup) = startup {
            next.startup = Some(parse_startup_decision(&serde_json::to_string(&startup)?)?);
            next.command = "__OMNI_AGENT_START__".into();
            next.startup_overridden = true;
        }
        if restart_desktop {
            let active = self
                .list()
                .await?
                .iter()
                .filter(|run| {
                    run.id != id && !matches!(run.phase, RunPhase::Failed | RunPhase::Completed)
                })
                .count();
            anyhow::ensure!(active < 4, "当前不能重试：最多同时运行四个验收任务");
            let integrity = retained_source_integrity(
                &self.run_dir(id)?.join("source.tar"),
                &previous.revision,
            )?;
            anyhow::ensure!(
                integrity["available"] == true,
                "原冻结源码快照已缺失，请重新创建验收；原失败记录和证据已保留"
            );
            anyhow::ensure!(
                integrity["matches_revision"] != false,
                "原冻结源码快照校验失败，请重新创建验收；原失败记录和证据已保留"
            );
            let environment = next
                .environment
                .as_mut()
                .context("原验收环境配置缺失，请重新创建验收")?;
            next.image_id = environment.base_image_id.clone();
            environment.phase = crate::project_environment::EnvironmentPhase::Pending;
            environment.progress = "等待重试项目环境准备".into();
            environment.error = None;
            environment.updated_at = Utc::now();
            next.phase = RunPhase::Starting;
            next.desktop_port = None;
            next.environment_reused = false;
            // Automatic runs launch through the dependency supervisor again.
            if next.automatic || next.startup.is_some() {
                next.command = "__OMNI_AGENT_START__".into();
            }
            next.automation_progress = "正在使用原源码快照和已确认用例重新准备验收环境".into();
        } else {
            require_running(&previous)?;
            next.automation_progress = "正在使用已确认用例和已有环境重试验收".into();
        }
        next.error = None;
        next.automation = AutomationPhase::Pending;
        next.automation_error = None;
        next.auto_verdict = None;
        next.checklist.clear();
        next.controller = None;
        next.control_expires_at = None;
        next.control_generation += 1;
        next.pending_approvals.clear();
        if let Some(recovery) = &mut next.recovery {
            recovery.waiting_for_user = false;
        }
        next.updated_at = Utc::now();
        next.expires_at = next.updated_at + chrono::Duration::hours(4);
        // Each attempt must collect a fresh report and video. Preserve the
        // overwritten files and original failure before changing live state.
        self.archive_retry(&previous)?;
        next.artifacts
            .retain(|a| a.name != "recording.mp4" && a.name != "report.json");
        self.persist(&next)?;
        *live = next.clone();
        self.cleaned_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        drop(live);
        if restart_desktop {
            self.start_desktop(Some(state), next.clone());
        } else {
            self.start_automation(state, id).await?;
        }
        Ok(next)
    }

    fn archive_retry(&self, run: &AcceptanceRun) -> Result<()> {
        let root = self.run_dir(&run.id)?;
        let backup = root.join("attempts").join(Uuid::new_v4().to_string());
        atomic_json(&backup.join("run.json"), run).context("保存原失败记录失败，尚未开始重试")?;
        for name in ["recording.mp4", "report.json"] {
            let old = root.join("artifacts").join(name);
            if old.is_file() {
                std::fs::copy(old, backup.join(name))
                    .context("保存原验收证据失败，尚未开始重试")?;
            }
        }
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_string_lossy();
            if entry.file_type()?.is_file()
                && text.starts_with("visual-check-")
                && text.ends_with(".json")
            {
                std::fs::copy(entry.path(), backup.join(name))?;
            }
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        acceptance_test_support::Fixture,
        project_environment::{EnvironmentConfig, EnvironmentMode},
        runtime_plugin::{PluginInstallation, RuntimeConfig},
    };
    use axum::http::StatusCode;
    use std::os::unix::fs::PermissionsExt;

    async fn failed_run(automatic: bool) -> Fixture {
        let mut fixture = Fixture::new().await;
        let docker = fixture.root.join("docker");
        std::fs::write(
            &docker,
            "#!/bin/sh\nsleep 0.2\necho simulated-runtime-unavailable >&2\nexit 1\n",
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
        atomic_json(
            &runtime.root.join("installation.json"),
            &PluginInstallation {
                phase: InstallPhase::Ready,
                image_id: Some("test-image".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let store = Arc::new(AcceptanceStore::new(Arc::clone(&runtime)));
        let state = Arc::get_mut(&mut fixture.state).unwrap();
        state.runtime_plugins = runtime;
        state.acceptance = Arc::clone(&store);
        let mut run = store.snapshot(&fixture.id).await.unwrap();
        let source = store.run_dir(&run.id).unwrap().join("source.tar");
        let mut archive = tar::Builder::new(std::fs::File::create(&source).unwrap());
        let bytes = b"frozen acceptance source";
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, "README.md", &bytes[..])
            .unwrap();
        archive.finish().unwrap();
        drop(archive);
        store
            .environments
            .save_config(
                &run.project_id,
                EnvironmentConfig {
                    mode: EnvironmentMode::Base,
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        run.environment = Some(
            store
                .environments
                .describe(&run.project_id, "test-image", &source)
                .unwrap(),
        );
        run.phase = RunPhase::Failed;
        run.automation = AutomationPhase::Failed;
        run.automatic = automatic;
        run.error = Some("No space left on device (os error 28)".into());
        run.expires_at = Utc::now() - chrono::Duration::hours(1);
        if automatic {
            run.command = "__OMNI_AGENT_START__".into();
            run.test_plan = Some(
                serde_json::from_value(serde_json::json!({
                    "version":3,"approved_version":3,"approved_by":"acceptance-test",
                    "approved_at":Utc::now(),"cases":[{
                        "id":"original-case","title":"Original confirmed case",
                        "steps":["Open the project"],"expected":"Show its sessions"
                    }]
                }))
                .unwrap(),
            );
        }
        store.persist(&run).unwrap();
        let shared = store.get(&run.id).await.unwrap();
        *shared.lock().await = run;
        fixture
    }

    async fn wait_for_preparation(store: &AcceptanceStore, id: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !store.preparing_sources.lock().unwrap().contains(id) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retry preparation must finish");
    }

    #[tokio::test]
    async fn manual_environment_settings_are_used_only_when_requested() {
        for apply in [false, true] {
            let fixture = failed_run(true).await;
            let store = &fixture.state.acceptance;
            let before = store.snapshot(&fixture.id).await.unwrap();
            let config = EnvironmentConfig {
                mode: EnvironmentMode::Dockerfile,
                dockerfile: "FROM {{BASE_IMAGE}}\nUSER x11box\nWORKDIR /workspace".into(),
                ..Default::default()
            };
            store
                .environments
                .save_config(&before.project_id, config.clone(), false)
                .unwrap();
            let next = store
                .retry_with_environment(fixture.state.clone(), &fixture.id, None, apply)
                .await
                .unwrap();
            assert_eq!(
                next.environment.unwrap().config,
                if apply {
                    config
                } else {
                    before.environment.unwrap().config
                }
            );
            assert_eq!(next.revision, before.revision);
            assert_eq!(next.test_plan.unwrap().approved_version, Some(3));
            wait_for_preparation(store, &fixture.id).await;
        }
    }

    #[tokio::test]
    async fn manual_startup_override_is_retained_when_environment_restarts() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let startup = parse_startup_decision(
            r#"{"kind":"desktop","command":"env GRAPHICS=correct ./app","app":"Fixture"}"#,
        )
        .unwrap();
        let next = store
            .retry_automation(fixture.state.clone(), &fixture.id, Some(startup))
            .await
            .unwrap();
        assert!(next.startup_overridden);
        assert_eq!(next.startup.unwrap().command, "env GRAPHICS=correct ./app");
        wait_for_preparation(store, &fixture.id).await;
        assert!(
            store
                .snapshot(&fixture.id)
                .await
                .unwrap()
                .startup_overridden
        );
    }

    #[tokio::test]
    async fn failed_run_manual_repair_authorization_requires_retained_confirmed_scope() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let session_id = "acceptance-manual-repair-fixture";
        let shared = store.get(&fixture.id).await.unwrap();
        {
            let mut run = shared.lock().await;
            run.expires_at = Utc::now() + chrono::Duration::hours(1);
            let mut recovery = AcceptanceRecovery::begin("environment", 3, None);
            recovery.session_id = Some(session_id.into());
            recovery.waiting_for_user = true;
            run.environment.as_mut().unwrap().recovery = Some(recovery);
            // An earlier startup repair must not hide this environment handoff.
            run.recovery = Some(AcceptanceRecovery::begin("startup", 3, None));
            store.persist(&run).unwrap();
        }
        assert_eq!(
            store.approval_context(session_id).await.unwrap().phase,
            "repairing"
        );
        assert!(
            store
                .approval_context("acceptance-unrelated-fixture")
                .await
                .is_none()
        );
        {
            let mut run = shared.lock().await;
            run.test_plan.as_mut().unwrap().approved_version = None;
            store.persist(&run).unwrap();
        }
        assert!(store.approval_context(session_id).await.is_none());
    }

    #[tokio::test]
    async fn runtime_preflight_failure_stops_before_creating_a_project_agent() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let sessions = fixture.state.list_sessions().await.len();
        store
            .retry_automation(Arc::clone(&fixture.state), &fixture.id, None)
            .await
            .unwrap();
        wait_for_preparation(store, &fixture.id).await;
        let failed = store.snapshot(&fixture.id).await.unwrap();
        assert_eq!(failed.phase, RunPhase::Failed);
        assert!(failed.error.unwrap().contains("验收基础运行时不可用"));
        assert!(failed.environment.unwrap().agent_session_id.is_none());
        assert_eq!(fixture.state.list_sessions().await.len(), sessions);
    }

    #[tokio::test]
    async fn polling_does_not_replace_runtime_recovery_with_an_old_environment_failure() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let shared = store.get(&fixture.id).await.unwrap();
        let mut run = shared.lock().await;
        let mut old = run.environment.clone().unwrap();
        old.phase = crate::project_environment::EnvironmentPhase::Failed;
        old.updated_at = Utc::now() - chrono::Duration::hours(1);
        old.progress = "上一轮环境准备失败".into();
        let path = store
            .runtime
            .root
            .join("environments/images")
            .join(&old.id)
            .join("environment.json");
        atomic_json(&path, &old).unwrap();
        run.phase = RunPhase::Starting;
        run.automation_progress = "正在检查并恢复验收基础运行时".into();
        run.environment.as_mut().unwrap().updated_at = Utc::now();
        drop(run);
        assert_eq!(
            store
                .snapshot(&fixture.id)
                .await
                .unwrap()
                .automation_progress,
            "正在检查并恢复验收基础运行时"
        );
        old.updated_at = Utc::now();
        old.progress = "本轮正在准备项目环境".into();
        atomic_json(&path, &old).unwrap();
        assert_eq!(
            store
                .snapshot(&fixture.id)
                .await
                .unwrap()
                .automation_progress,
            "本轮正在准备项目环境"
        );
    }

    #[tokio::test]
    async fn restored_runtime_rebases_environment_without_changing_frozen_scope() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let recovered = format!("sha256:{}", "b".repeat(64));
        atomic_json(
            &store.runtime.root.join("installation.json"),
            &PluginInstallation {
                phase: InstallPhase::Ready,
                image_id: Some(recovered.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        std::fs::write(
            &store.runtime.config.docker,
            format!("#!/bin/sh\necho {recovered}\n"),
        )
        .unwrap();
        let shared = store.get(&fixture.id).await.unwrap();
        let mut initial = shared.lock().await.clone();
        initial.phase = RunPhase::Starting;
        initial.startup = Some(
            parse_startup_decision(
                r#"{"kind":"desktop","command":"old-client","app":"Old Client"}"#,
            )
            .unwrap(),
        );
        *shared.lock().await = initial.clone();
        let old = initial.environment.clone().unwrap();
        let source = store.run_dir(&fixture.id).unwrap().join("source.tar");
        let source_bytes = std::fs::read(&source).unwrap();
        // Settings edited later must not replace this run's frozen configuration.
        store
            .environments
            .save_config(&initial.project_id, EnvironmentConfig::default(), false)
            .unwrap();
        let (environment, _) = store
            .prepare_run_environment(None, &shared, &mut initial, &source)
            .await
            .unwrap();
        assert_ne!(environment.id, old.id);
        assert_eq!(environment.base_image_id, recovered);
        assert_eq!(environment.config, old.config);
        assert_eq!(environment.source_key, old.source_key);
        assert_eq!(std::fs::read(&source).unwrap(), source_bytes);
        let updated = shared.lock().await.clone();
        assert_eq!(updated.revision, initial.revision);
        assert_eq!(
            serde_json::to_value(&updated.test_plan).unwrap(),
            serde_json::to_value(&initial.test_plan).unwrap()
        );
        assert!(updated.startup.is_none());
        assert!(!updated.environment_reused);
    }

    #[tokio::test]
    async fn failed_environment_http_retry_preserves_source_and_approval_and_rejects_double_submit()
    {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let before = store.snapshot(&fixture.id).await.unwrap();
        let root = store.run_dir(&fixture.id).unwrap();
        let source = std::fs::read(root.join("source.tar")).unwrap();
        store
            .cleaned_failures
            .lock()
            .unwrap()
            .insert(fixture.id.clone());
        let (origin, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{origin}/v2/acceptance/{}/retry", fixture.id))
            .headers(fixture.headers.clone())
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = response.json().await.unwrap();
        let run = &body["data"];
        assert_eq!(run["phase"], "starting");
        assert_eq!(run["automation"], "pending");
        assert!(run["error"].is_null());
        assert_eq!(run["id"], fixture.id);
        assert_eq!(run["revision"], before.revision);
        assert_eq!(
            run["test_plan"],
            serde_json::to_value(&before.test_plan).unwrap()
        );
        assert!(run["artifacts"].as_array().unwrap().is_empty());
        assert!(
            DateTime::parse_from_rfc3339(run["expires_at"].as_str().unwrap()).unwrap() > Utc::now()
        );
        assert_eq!(std::fs::read(root.join("source.tar")).unwrap(), source);
        assert!(!store.failure_cleaned_up(&fixture.id));
        let backup = std::fs::read_dir(root.join("attempts"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let archived: AcceptanceRun =
            serde_json::from_slice(&std::fs::read(backup.join("run.json")).unwrap()).unwrap();
        assert_eq!(archived.error, before.error);
        assert_eq!(
            std::fs::read(backup.join("recording.mp4")).unwrap(),
            b"0123456789"
        );
        let duplicate = client
            .post(format!("{origin}/v2/acceptance/{}/retry", fixture.id))
            .headers(fixture.headers.clone())
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        wait_for_preparation(store, &fixture.id).await;
        assert!(
            store
                .snapshot(&fixture.id)
                .await
                .unwrap()
                .error
                .unwrap()
                .contains("simulated-runtime-unavailable")
        );
        server.abort();
    }

    #[tokio::test]
    async fn manual_failed_environment_can_restart_without_creating_or_approving_test_cases() {
        let fixture = failed_run(false).await;
        let store = &fixture.state.acceptance;
        let retried = store
            .retry_automation(Arc::clone(&fixture.state), &fixture.id, None)
            .await
            .unwrap();
        assert_eq!(retried.phase, RunPhase::Starting);
        assert!(retried.test_plan.is_none());
        assert!(!retried.automatic);
        wait_for_preparation(store, &fixture.id).await;
    }

    #[tokio::test]
    async fn invalid_retry_preconditions_leave_original_failure_unchanged() {
        for reason in [
            "missing-source",
            "changed-source",
            "unapproved-plan",
            "completed",
            "plugin-unavailable",
        ] {
            let fixture = failed_run(true).await;
            let store = &fixture.state.acceptance;
            let shared = store.get(&fixture.id).await.unwrap();
            {
                let mut run = shared.lock().await;
                match reason {
                    "missing-source" => {
                        std::fs::remove_file(store.run_dir(&fixture.id).unwrap().join("source.tar"))
                            .unwrap()
                    }
                    "changed-source" => {
                        run.revision = format!("snapshot-sha256:{}", "0".repeat(64))
                    }
                    "unapproved-plan" => run.test_plan.as_mut().unwrap().approved_version = None,
                    "completed" => run.phase = RunPhase::Completed,
                    "plugin-unavailable" => {
                        std::fs::remove_file(store.runtime.root.join("installation.json")).unwrap()
                    }
                    _ => unreachable!(),
                }
                store.persist(&run).unwrap();
            }
            let before = store.snapshot(&fixture.id).await.unwrap();
            let error = store
                .retry_automation(Arc::clone(&fixture.state), &fixture.id, None)
                .await
                .unwrap_err();
            let expected = match reason {
                "missing-source" => "快照已缺失",
                "changed-source" => "快照校验失败",
                "unapproved-plan" => "确认当前版本",
                "completed" => "当前不能重试",
                _ => "plugin is not ready",
            };
            assert!(error.to_string().contains(expected), "{reason}: {error:#}");
            let after = store.snapshot(&fixture.id).await.unwrap();
            assert_eq!(after.error, before.error);
            assert_eq!(after.phase, before.phase);
            assert!(
                !store
                    .run_dir(&fixture.id)
                    .unwrap()
                    .join("attempts")
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn maintenance_does_not_clean_a_new_retry_using_a_stale_failed_snapshot() {
        let fixture = failed_run(true).await;
        let store = &fixture.state.acceptance;
        let shared = store.get(&fixture.id).await.unwrap();
        {
            let mut run = shared.lock().await;
            run.phase = RunPhase::Starting;
            run.error = None;
            run.expires_at = Utc::now() + chrono::Duration::hours(4);
            store.persist(&run).unwrap();
        }
        let kept = store.stop_internal(&fixture.id, None, true).await.unwrap();
        assert_eq!(kept.phase, RunPhase::Starting);
        assert!(kept.error.is_none());
        assert!(!store.failure_cleaned_up(&fixture.id));
        assert!(
            store
                .run_dir(&fixture.id)
                .unwrap()
                .join("source.tar")
                .is_file()
        );
    }
}
