//! Human-readable test cases are approved before any environment is prepared.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceTestCase {
    pub id: String,
    pub title: String,
    pub steps: Vec<String>,
    pub expected: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptanceTestPlan {
    pub version: u64,
    pub cases: Vec<AcceptanceTestCase>,
    #[serde(default)]
    pub notes: String,
    pub approved_version: Option<u64>,
    pub approved_by: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanMessage {
    pub role: MessageRole,
    pub content: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct AgentPlan {
    cases: Vec<AcceptanceTestCase>,
    #[serde(default)]
    notes: String,
    #[serde(default)]
    reply: String,
}

fn validate_cases(cases: &[AcceptanceTestCase]) -> Result<()> {
    if cases.is_empty() || cases.len() > 32 {
        bail!("请保留 1 到 32 条测试用例");
    }
    let mut ids = HashSet::new();
    for case in cases {
        if case.id.is_empty()
            || case.id.len() > 64
            || !ids.insert(&case.id)
            || !case
                .id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            || case.title.trim().is_empty()
            || case.title.chars().count() > 160
            || case.expected.trim().is_empty()
            || case.expected.chars().count() > 2000
            || case.steps.is_empty()
            || case.steps.len() > 12
            || case
                .steps
                .iter()
                .any(|s| s.trim().is_empty() || s.chars().count() > 1000)
        {
            bail!("测试用例需要唯一编号、简洁标题、操作步骤和预期结果");
        }
    }
    if serde_json::to_vec(cases)?.len() > 64_000 {
        bail!("测试用例总长度不能超过 64000 字节");
    }
    Ok(())
}

fn require_draft(run: &AcceptanceRun, version: u64) -> Result<()> {
    if run.phase != RunPhase::Planning || run.automation != AutomationPhase::AwaitingPlanApproval {
        bail!("当前不能修改或确认测试用例，请等待 Agent 完成调整");
    }
    let plan = run.test_plan.as_ref().context("测试用例尚未生成")?;
    if plan.version != version || plan.approved_version.is_some() {
        bail!("测试用例已更新，请刷新后确认最新版本");
    }
    Ok(())
}

impl AcceptanceStore {
    pub async fn retry_test_plan(
        self: &Arc<Self>,
        state: Arc<AppState>,
        id: &str,
    ) -> Result<AcceptanceRun> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        if run.phase != RunPhase::Planning
            || run.test_plan.is_some()
            || run.automation != AutomationPhase::Failed
        {
            bail!("当前不能重新生成测试用例");
        }
        run.automation = AutomationPhase::Pending;
        run.automation_error = None;
        run.automation_progress = "正在重新生成测试用例".into();
        self.persist(&run)?;
        let snapshot = run.clone();
        drop(run);
        self.start_automation(state, id).await?;
        Ok(snapshot)
    }

    async fn ask_test_plan(&self, state: &Arc<AppState>, run: &AcceptanceRun) -> Result<AgentPlan> {
        let session_id = self.ensure_acceptance_agent(state, run).await?;
        let mut recent_messages = run.plan_messages.iter().rev().take(12).collect::<Vec<_>>();
        recent_messages.reverse();
        let prompt = format!(
            r#"请生成或调整一份供用户确认的简洁测试用例。这是验收的计划阶段，不要启动应用、安装环境、执行命令或执行测试。
用户需求摘要：{}
项目背景：{}
当前测试用例（包含用户手动编辑的最新版本）：{}
计划沟通记录（按顺序，以用户最新调整为准）：{}
只返回 JSON：{{"cases":[{{"id":"case_1","title":"简洁中文标题","steps":["用户能看懂的操作步骤"],"expected":"可验证的预期结果"}}],"notes":"不确定或不在范围内的事项","reply":"简短说明本次如何调整，或提出需要用户澄清的问题"}}。
每项包含 1 至 6 个简洁步骤和明确预期，总数 1 至 32 项，优先合并相近场景。步骤描述用户行为，不包含 x11box 命令、选择器、代码或内部实现细节。保留未被用户取消的要求，用户明确删除或手改的用例应作为最新范围。保持已有用例编号稳定；不能凭 README 扩展用户需求。没有图像内容时不得臆测。无法确认的前置条件写入 notes，用户可以继续沟通。不要声称已经通过或已经执行。"#,
            serde_json::to_string(&run.requirement_brief)?
                .chars()
                .take(20_000)
                .collect::<String>(),
            run.change_summary.chars().take(18_000).collect::<String>(),
            serde_json::to_string(&run.test_plan)?,
            serde_json::to_string(&recent_messages)?,
        );
        let (_, reply) = send_automation_message(
            state,
            &session_id,
            SendMessageInput {
                content: prompt,
                input_mode: InputMode::Text,
                system_prompt: Some("只生成可供用户审阅的测试用例 JSON，不执行工具或测试。".into()),
                client_message_id: Some(format!(
                    "acceptance-test-plan-{}-{}",
                    run.id,
                    Uuid::new_v4()
                )),
                provider_id: None,
                reasoning_effort: None,
                model: None,
            },
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let text = wait_for_reply(state, &session_id, &reply.id, Duration::from_secs(180)).await?;
        let plan: AgentPlan = serde_json::from_value(parse_json_value(&text)?)?;
        validate_cases(&plan.cases)?;
        if plan.notes.chars().count() > 4000 || plan.reply.chars().count() > 4000 {
            bail!("Agent 的测试用例说明过长，请重试");
        }
        Ok(plan)
    }

    pub(super) async fn generate_initial_test_plan(
        &self,
        state: &Arc<AppState>,
        id: &str,
    ) -> Result<()> {
        let run = self.snapshot(id).await?;
        let session_id = self.ensure_acceptance_agent(state, &run).await?;
        if run.requirement_brief.is_none()
            && (run.session_id.is_some() || run.requirements.is_some())
        {
            self.prepare_requirement_brief(state, id, &session_id)
                .await?;
        }
        self.update_automation(id, |run| {
            run.automation = AutomationPhase::Planning;
            run.automation_progress = "Agent 正在生成简洁测试用例，等待你确认".into();
        })
        .await?;
        let run = self.snapshot(id).await?;
        if run.phase != RunPhase::Planning {
            return Ok(());
        }
        let generated = self.ask_test_plan(state, &run).await?;
        self.apply_generated_plan(id, 0, generated).await
    }

    async fn apply_generated_plan(
        &self,
        id: &str,
        version: u64,
        generated: AgentPlan,
    ) -> Result<()> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        if run.phase != RunPhase::Planning
            || run.test_plan.as_ref().map_or(0, |p| p.version) != version
        {
            return Ok(());
        }
        run.test_plan = Some(AcceptanceTestPlan {
            version: version + 1,
            cases: generated.cases,
            notes: generated.notes,
            approved_version: None,
            approved_by: None,
            approved_at: None,
        });
        run.plan_messages.push(PlanMessage {
            role: MessageRole::Assistant,
            content: if generated.reply.trim().is_empty() {
                "测试用例已更新，请检查并确认。".into()
            } else {
                generated.reply
            },
            created_at: Utc::now(),
        });
        run.automation = AutomationPhase::AwaitingPlanApproval;
        run.automation_progress = "测试用例已就绪，确认后才会准备环境并执行".into();
        run.automation_error = None;
        run.updated_at = Utc::now();
        self.persist(&run)
    }

    pub async fn edit_test_plan(
        &self,
        id: &str,
        version: u64,
        cases: Vec<AcceptanceTestCase>,
    ) -> Result<AcceptanceRun> {
        validate_cases(&cases)?;
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        require_draft(&run, version)?;
        let plan = run.test_plan.as_mut().unwrap();
        plan.cases = cases;
        plan.version += 1;
        run.plan_messages.push(PlanMessage {
            role: MessageRole::User,
            content: "已手动编辑测试用例，请以当前版本为准。".into(),
            created_at: Utc::now(),
        });
        run.automation_error = None;
        run.updated_at = Utc::now();
        self.persist(&run)?;
        Ok(run.clone())
    }

    pub async fn revise_test_plan(
        self: &Arc<Self>,
        state: Arc<AppState>,
        id: &str,
        version: u64,
        message: String,
    ) -> Result<AcceptanceRun> {
        let message = message.trim();
        if message.is_empty() || message.chars().count() > 4000 {
            bail!("请填写 1 到 4000 字的调整要求");
        }
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        require_draft(&run, version)?;
        if run.plan_messages.len() >= 80 {
            bail!("本次沟通记录过长，请直接编辑测试用例");
        }
        run.plan_messages.push(PlanMessage {
            role: MessageRole::User,
            content: message.to_string(),
            created_at: Utc::now(),
        });
        run.automation = AutomationPhase::RevisingPlan;
        run.automation_error = None;
        run.automation_progress = "Agent 正在根据你的意见调整测试用例".into();
        run.updated_at = Utc::now();
        self.persist(&run)?;
        let snapshot = run.clone();
        drop(run);
        let store = Arc::clone(self);
        let id = id.to_string();
        let pending = snapshot.clone();
        tokio::spawn(async move {
            let result = store.ask_test_plan(&state, &pending).await;
            match result {
                Ok(generated) => {
                    let _ = store.apply_generated_plan(&id, version, generated).await;
                }
                Err(error) => {
                    if let Ok(shared) = store.get(&id).await {
                        let mut run = shared.lock().await;
                        if run.phase == RunPhase::Planning
                            && run.automation == AutomationPhase::RevisingPlan
                        {
                            run.automation = AutomationPhase::AwaitingPlanApproval;
                            run.automation_progress =
                                "调整失败，原测试用例已保留，可继续沟通或手动编辑".into();
                            run.automation_error = Some(format!("{error:#}"));
                            run.updated_at = Utc::now();
                            let _ = store.persist(&run);
                        }
                    }
                }
            }
        });
        Ok(snapshot)
    }

    pub async fn confirm_test_plan(
        self: &Arc<Self>,
        state: Arc<AppState>,
        id: &str,
        version: u64,
        client: &str,
    ) -> Result<AcceptanceRun> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        if run
            .test_plan
            .as_ref()
            .is_some_and(|p| p.approved_version == Some(version))
        {
            return Ok(run.clone());
        }
        require_draft(&run, version)?;
        validate_cases(&run.test_plan.as_ref().unwrap().cases)?;
        if self.runtime.status().await?.phase != InstallPhase::Ready {
            bail!("x11box plugin is not ready");
        }
        let plan = run.test_plan.as_mut().unwrap();
        plan.approved_version = Some(version);
        plan.approved_by = Some(client.to_string());
        plan.approved_at = Some(Utc::now());
        run.phase = RunPhase::Starting;
        run.automation = AutomationPhase::Pending;
        run.automation_error = None;
        run.automation_progress = "测试用例已确认，正在准备验收环境".into();
        run.updated_at = Utc::now();
        run.expires_at = Utc::now() + chrono::Duration::hours(4);
        self.persist(&run)?;
        let snapshot = run.clone();
        drop(run);
        self.start_desktop(Some(state), snapshot.clone());
        Ok(snapshot)
    }
}

pub(super) fn confirmed_requirements(run: &AcceptanceRun) -> Result<Option<RequirementBrief>> {
    let Some(plan) = &run.test_plan else {
        return Ok(None);
    };
    if plan.approved_version != Some(plan.version) {
        bail!("请先确认当前版本的测试用例");
    }
    Ok(Some(RequirementBrief {
        requirements: plan
            .cases
            .iter()
            .map(|case| acceptance_context::AcceptanceRequirement {
                id: case.id.clone(),
                description: format!(
                    "{}；步骤：{}；预期：{}",
                    case.title,
                    case.steps.join("；"),
                    case.expected
                ),
                source_message_ids: vec![format!("confirmed-test-plan-v{}", plan.version)],
            })
            .collect(),
        notes: plan.notes.clone(),
        source_message_count: 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acceptance_test_support::Fixture;

    fn cases() -> Vec<AcceptanceTestCase> {
        vec![AcceptanceTestCase {
            id: "login".into(),
            title: "登录失败提示".into(),
            steps: vec!["输入错误密码".into(), "点击登录".into()],
            expected: "显示错误提示，停留在登录页".into(),
        }]
    }

    async fn draft(fixture: &Fixture) {
        let shared = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        let mut run = shared.lock().await;
        run.phase = RunPhase::Planning;
        run.automatic = true;
        run.automation = AutomationPhase::AwaitingPlanApproval;
        run.test_plan = Some(AcceptanceTestPlan {
            version: 1,
            cases: cases(),
            notes: String::new(),
            approved_version: None,
            approved_by: None,
            approved_at: None,
        });
        fixture.state.acceptance.persist(&run).unwrap();
    }

    #[test]
    fn case_validation_rejects_empty_duplicate_and_unverifiable_cases() {
        assert!(validate_cases(&cases()).is_ok());
        assert!(validate_cases(&[]).is_err());
        let mut duplicate = cases();
        duplicate.extend(cases());
        assert!(validate_cases(&duplicate).is_err());
        let mut invalid = cases();
        invalid[0].expected.clear();
        assert!(validate_cases(&invalid).is_err());
    }

    #[tokio::test]
    async fn edits_persist_and_stale_versions_cannot_edit_or_confirm() {
        let fixture = Fixture::new().await;
        draft(&fixture).await;
        let store = &fixture.state.acceptance;
        let mut edited = cases();
        edited[0].expected = "显示明确错误，不跳转".into();
        let updated = store.edit_test_plan(&fixture.id, 1, edited).await.unwrap();
        assert_eq!(updated.test_plan.as_ref().unwrap().version, 2);
        assert!(updated.desktop_port.is_none());
        assert!(store.edit_test_plan(&fixture.id, 1, cases()).await.is_err());
        assert!(
            store
                .confirm_test_plan(Arc::clone(&fixture.state), &fixture.id, 1, "client")
                .await
                .is_err()
        );
        assert!(confirmed_requirements(&updated).is_err());
        let reloaded = AcceptanceStore::new(Arc::clone(&fixture.state.runtime_plugins));
        let restored = reloaded.snapshot(&fixture.id).await.unwrap();
        assert_eq!(restored.phase, RunPhase::Planning);
        assert_eq!(
            restored.test_plan.unwrap().cases[0].expected,
            "显示明确错误，不跳转"
        );
    }

    #[tokio::test]
    async fn revision_blocks_confirmation_and_restart_preserves_original_cases() {
        let fixture = Fixture::new().await;
        draft(&fixture).await;
        let shared = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        {
            let mut run = shared.lock().await;
            run.automation = AutomationPhase::RevisingPlan;
            fixture.state.acceptance.persist(&run).unwrap();
            assert!(require_draft(&run, 1).is_err());
        }
        let reloaded = AcceptanceStore::new(Arc::clone(&fixture.state.runtime_plugins));
        let restored = reloaded.snapshot(&fixture.id).await.unwrap();
        assert_eq!(restored.automation, AutomationPhase::AwaitingPlanApproval);
        assert_eq!(restored.test_plan.unwrap().cases[0].title, "登录失败提示");
        assert!(restored.automation_error.is_some());
    }

    #[tokio::test]
    async fn stopped_drafts_ignore_late_agent_output_and_never_touch_docker() {
        let fixture = Fixture::new().await;
        draft(&fixture).await;
        let store = &fixture.state.acceptance;
        let stopped = store.stop(&fixture.id, None).await.unwrap();
        assert_eq!(stopped.phase, RunPhase::Completed);
        store
            .apply_generated_plan(
                &fixture.id,
                1,
                AgentPlan {
                    cases: cases(),
                    notes: String::new(),
                    reply: "late reply".into(),
                },
            )
            .await
            .unwrap();
        let stopped = store.snapshot(&fixture.id).await.unwrap();
        assert_eq!(stopped.test_plan.unwrap().version, 1);
        assert!(stopped.plan_messages.is_empty());
    }

    #[tokio::test]
    async fn execution_uses_confirmed_cases_instead_of_cancelled_original_requirements() {
        let fixture = Fixture::new().await;
        draft(&fixture).await;
        let mut run = fixture
            .state
            .acceptance
            .snapshot(&fixture.id)
            .await
            .unwrap();
        let plan = run.test_plan.as_mut().unwrap();
        plan.approved_version = Some(plan.version);
        let brief = confirmed_requirements(&run).unwrap().unwrap();
        assert_eq!(brief.requirements.len(), 1);
        assert_eq!(brief.requirements[0].id, "login");
        assert!(brief.requirements[0].description.contains("点击登录"));
        let checks = parse_plan(r#"{"checks":[{"id":"login-failure","title":"登录失败","requirement_ids":["login"],"assertion":"显示错误","actions":[{"instruction":"observe"}]}]}"#).unwrap();
        assert!(acceptance_context::validate_coverage(&checks, &brief).is_ok());
    }

    #[tokio::test]
    async fn automatic_creation_without_git_stops_before_environment_preparation() {
        use crate::runtime_plugin::{PluginInstallation, RuntimeConfig};
        let root = std::env::temp_dir().join(format!("plan-gate-{}", Uuid::new_v4()));
        let project = root.join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("README.md"), "Fixture application").unwrap();
        let runtime = Arc::new(RuntimePluginStore::new(
            root.join("runtime"),
            RuntimeConfig {
                image: None,
                source_dir: None,
                docker: root.join("docker-must-not-be-called"),
            },
        ));
        atomic_json(
            &runtime.root.join("installation.json"),
            &PluginInstallation {
                phase: InstallPhase::Ready,
                image_id: Some(format!("sha256:{}", "a".repeat(64))),
                ..Default::default()
            },
        )
        .unwrap();
        let store = Arc::new(AcceptanceStore::new(runtime));
        let run = store
            .create(
                CreateAcceptance {
                    id: Uuid::new_v4().to_string(),
                    project_id: "fixture".into(),
                    session_id: None,
                    command: String::new(),
                    size: "pc".into(),
                    automatic: true,
                    requirements: Some("验证登录失败提示".into()),
                },
                &project,
                "client",
                Vec::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(run.phase, RunPhase::Planning);
        assert!(run.revision.starts_with("snapshot-sha256:"));
        assert!(!project.join(".git").exists());
        let mut source = tar::Archive::new(
            std::fs::File::open(store.run_dir(&run.id).unwrap().join("source.tar")).unwrap(),
        );
        assert_eq!(
            source
                .entries()
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path()
                .unwrap(),
            Path::new("README.md")
        );
        tokio::task::yield_now().await;
        let run = store.snapshot(&run.id).await.unwrap();
        assert_eq!(run.phase, RunPhase::Planning);
        assert!(run.desktop_port.is_none());
        assert!(run.error.is_none());
        store
            .apply_generated_plan(
                &run.id,
                0,
                AgentPlan {
                    cases: cases(),
                    notes: String::new(),
                    reply: "请确认".into(),
                },
            )
            .await
            .unwrap();
        let fixture = Fixture::new().await;
        let approved = store
            .confirm_test_plan(Arc::clone(&fixture.state), &run.id, 1, "client")
            .await
            .unwrap();
        assert_eq!(approved.phase, RunPhase::Starting);
        assert_eq!(
            approved.test_plan.as_ref().unwrap().approved_version,
            Some(1)
        );
        let repeated = store
            .confirm_test_plan(Arc::clone(&fixture.state), &run.id, 1, "client")
            .await
            .unwrap();
        assert_eq!(repeated.test_plan.unwrap().approved_version, Some(1));
        assert!(store.edit_test_plan(&run.id, 1, cases()).await.is_err());
        let _ = std::fs::remove_dir_all(root);
    }
}
