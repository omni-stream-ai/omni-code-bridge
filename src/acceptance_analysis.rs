//! Send a durable acceptance report to an ordinary user conversation.
use super::*;
use crate::{
    acceptance::{AutomationPhase, CheckStatus, RunPhase},
    models::{AgentKind, CreateSessionInput, InputMode, ReasoningEffort, SessionSummary},
    session_domain::CreateTurnCommand,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AnalyzeRequest {
    request_id: Uuid,
    #[serde(default)]
    new_session: bool,
    agent: Option<AgentKind>,
    provider_id: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<ReasoningEffort>,
}

#[derive(Serialize, Deserialize)]
struct AnalysisRequest {
    new_session: bool,
    session_id: String,
    content: String,
}

pub(super) async fn analyze(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<AnalyzeRequest>,
) -> Result<(StatusCode, Json<ApiResponse<SessionSummary>>), ApiError> {
    crate::api::authorize_request(&headers, &state).await?;
    let run = state
        .acceptance
        .snapshot(&id)
        .await
        .map_err(operation_error)?;
    let root = state.runtime_plugins.root.join("runs").join(&run.id);
    let request_path = root
        .join("analyses")
        .join(format!("{}.json", input.request_id));
    let request = if request_path.is_file() {
        let saved: AnalysisRequest =
            serde_json::from_slice(&std::fs::read(&request_path).map_err(|e| internal(e.into()))?)
                .map_err(|e| internal(e.into()))?;
        if saved.new_session != input.new_session {
            return Err(bad_request("Analysis request target changed"));
        }
        saved
    } else {
        if !matches!(run.phase, RunPhase::Completed | RunPhase::Failed)
            && !matches!(
                run.automation,
                AutomationPhase::AwaitingConfirmation
                    | AutomationPhase::Failed
                    | AutomationPhase::Completed
            )
        {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message: "验收仍在执行，请等待结果后再分析".into(),
            });
        }
        let source = match run.session_id.as_deref() {
            Some(id) => state
                .get_session(id)
                .await
                .filter(|detail| detail.session.project_id == run.project_id),
            None => None,
        };
        let session_id = if input.new_session {
            let source = source.as_ref().map(|detail| &detail.session);
            let session = state
                .create_session(CreateSessionInput {
                    client_session_id: format!("result-analysis-{}", input.request_id),
                    project_id: run.project_id.clone(),
                    title: Some(format!("验收结果分析 · {}", &run.id[..8.min(run.id.len())])),
                    agent: source
                        .map(|s| s.agent)
                        .or(input.agent)
                        .unwrap_or(AgentKind::Pi),
                    brief_reply_mode: source.is_some_and(|s| s.brief_reply_mode),
                    approval_mode: Default::default(),
                    provider_id: source
                        .map(|s| s.provider_id.clone())
                        .unwrap_or(input.provider_id),
                    reasoning_effort: source
                        .map(|s| s.reasoning_effort)
                        .unwrap_or(input.reasoning_effort),
                    model: source.map(|s| s.model.clone()).unwrap_or(input.model),
                })
                .await
                .map_err(|message| ApiError {
                    status: StatusCode::CONFLICT,
                    message,
                })?;
            if session.agent == AgentKind::Codex
                && session.provider_id.is_none()
                && let Some(profile) = source.and_then(|s| s.profile_id.as_deref())
            {
                state
                    .update_session_profile(&session.id, profile)
                    .await
                    .map_err(|message| ApiError {
                        status: StatusCode::CONFLICT,
                        message,
                    })?;
            }
            session.id
        } else {
            let session = source
                .ok_or_else(|| bad_request("原会话不存在，请新开会话分析验收结果"))?
                .session;
            if session.agent == AgentKind::Codex {
                let home = std::env::var_os("CODEX_HOME")
                    .map(std::path::PathBuf::from)
                    .or_else(|| {
                        std::env::var_os("HOME").map(|p| std::path::PathBuf::from(p).join(".codex"))
                    });
                if home.as_deref().is_some_and(|home| {
                    codex_writer_busy(
                        home,
                        session
                            .runtime_session_ref
                            .as_deref()
                            .unwrap_or(&session.id),
                    )
                }) {
                    return Err(ApiError { status: StatusCode::CONFLICT,
                        message: "原 Codex 会话正在其他窗口或进程中打开，请关闭对应会话后重试，或选择新建会话分析。".into() });
                }
            }
            session.id
        };
        let saved = AnalysisRequest {
            new_session: input.new_session,
            session_id,
            content: analysis_prompt(
                &state,
                &run,
                &root
                    .join("analyses")
                    .join(format!("{}.report.json", input.request_id)),
            )
            .await
            .map_err(internal)?,
        };
        crate::runtime_plugin::atomic_json(&request_path, &saved).map_err(internal)?;
        saved
    };
    let session = state
        .get_session(&request.session_id)
        .await
        .filter(|detail| detail.session.project_id == run.project_id)
        .ok_or_else(|| bad_request("分析会话不存在，请重新打开验收页后重试"))?
        .session;
    crate::api::submit_domain_turn(
        Arc::clone(&state),
        session.id.clone(),
        CreateTurnCommand {
            command_id: format!("acceptance-analysis-{}", input.request_id),
            turn_id: format!("analysis-{}", input.request_id),
            user_message_id: format!("analysis-input-{}", input.request_id),
            content: request.content,
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        },
        &headers,
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { data: session })))
}

fn tail(value: &str, limit: usize) -> String {
    value
        .chars()
        .rev()
        .take(limit)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn codex_writer_busy(home: &std::path::Path, thread_id: &str) -> bool {
    let Ok(id) = Uuid::parse_str(thread_id) else {
        return false;
    };
    let path = home.join("thread-writer-locks").join(format!("{id}.lock"));
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        return false;
    };
    // A stale lock file is harmless; only a live exclusive owner blocks resume.
    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
}

fn brief(value: &str, limit: usize) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = value.chars();
    let mut result = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}

async fn analysis_prompt(
    state: &AppState,
    run: &AcceptanceRun,
    report_path: &std::path::Path,
) -> anyhow::Result<String> {
    let root = state.runtime_plugins.root.join("runs").join(&run.id);
    let mut evidence = Vec::new();
    for artifact in &run.artifacts {
        let path = state
            .acceptance
            .artifact(&run.id, &artifact.name)
            .await
            .ok()
            .map(|(path, _)| path);
        evidence.push(json!({
            "name": artifact.name, "content_type": artifact.content_type,
            "path": path, "available": path.is_some(), "bytes": artifact.bytes,
        }));
    }
    let environment = run.environment.as_ref().map(|env| {
        json!({
            "id":env.id, "name":env.name, "phase":env.phase, "error":env.error,
            "log_tail":tail(&env.log, 16000), "agent_session_id":env.agent_session_id,
        })
    });
    let diagnostics = run
        .recovery
        .as_ref()
        .into_iter()
        .chain(run.environment.as_ref().and_then(|e| e.recovery.as_ref()))
        .map(|recovery| {
            json!({
                "stage":recovery.stage, "session_id":recovery.session_id,
                "attempt":recovery.attempt, "max_attempts":recovery.max_attempts,
                "waiting_for_user":recovery.waiting_for_user,
                "recent_failures":recovery.failures.iter().rev().take(3).map(|f| json!({
                    "attempt":f.attempt, "at":f.at, "error":tail(&f.error, 4000),
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let context = json!({
        "run_id":run.id, "project_id":run.project_id, "source_session_id":run.session_id,
        "revision":run.revision, "created_at":run.created_at, "updated_at":run.updated_at,
        "phase":run.phase, "automation":run.automation, "error":run.error,
        "automation_error":run.automation_error, "requirements":run.requirements,
        "requirement_brief":run.requirement_brief, "confirmed_test_plan":run.test_plan,
        "checks":run.checklist, "automatic_verdict":run.auto_verdict, "human_result":run.result,
        "environment":environment, "startup":run.startup, "repair_history":diagnostics,
        "evidence":evidence, "full_record_path":root.join("run.json"),
    });
    // Keep the full snapshot available without copying it into the conversation.
    crate::runtime_plugin::atomic_json(report_path, &context)?;
    let human = match run.result.as_ref().map(|r| r.passed) {
        Some(true) => "人工确认通过",
        Some(false) => "人工确认不通过",
        None => "尚未人工确认",
    };
    let automatic = match run.auto_verdict.as_ref().map(|r| r.passed) {
        Some(true) => "自动建议通过",
        Some(false) => "自动建议不通过",
        None => "未生成自动结论",
    };
    let count = |status: CheckStatus| run.checklist.iter().filter(|c| c.status == status).count();
    let mut summary = format!(
        "请分析这次应用验收，说明原因、证据和建议修复步骤。先分析，再由我决定是否修改或重跑。\n验收：{}；版本：{}\n结论：{human}；{automatic}。\n检查：共 {} 项，通过 {}，失败 {}，阻塞 {}，未完成 {}。",
        run.id,
        brief(&run.revision, 80),
        run.checklist.len(),
        count(CheckStatus::Passed),
        count(CheckStatus::Failed),
        count(CheckStatus::Blocked),
        count(CheckStatus::Pending) + count(CheckStatus::Running),
    );
    for check in run
        .checklist
        .iter()
        .filter(|c| matches!(c.status, CheckStatus::Failed | CheckStatus::Blocked))
        .take(3)
    {
        summary.push_str(&format!(
            "\n- {}（{}）：{}",
            brief(&check.title, 60),
            if check.status == CheckStatus::Blocked {
                "阻塞"
            } else {
                "失败"
            },
            brief(&check.notes, 120)
        ));
    }
    if let Some(error) = run
        .error
        .as_deref()
        .or(run.automation_error.as_deref())
        .or_else(|| run.environment.as_ref().and_then(|e| e.error.as_deref()))
    {
        summary.push_str(&format!("\n错误：{}", brief(error, 180)));
    }
    if let Some(result) = &run.result
        && !result.notes.trim().is_empty()
    {
        summary.push_str(&format!("\n人工备注：{}", brief(&result.notes, 120)));
    }
    summary.push_str(&format!("\n完整结果：{}\n按需读取该文件中的检查明细、诊断和证据路径，核对冻结版本；未实际查看的截图或录像请勿当作已验证证据。", report_path.display()));
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acceptance_test_support::{Fixture, repair_fixture};

    #[test]
    fn writer_probe_distinguishes_an_active_owner_from_a_stale_file() {
        let home = std::env::temp_dir().join(format!("analysis-writer-{}", Uuid::new_v4()));
        let id = Uuid::new_v4().to_string();
        std::fs::create_dir_all(home.join("thread-writer-locks")).unwrap();
        assert!(!codex_writer_busy(&home, &id));
        let file =
            std::fs::File::create(home.join("thread-writer-locks").join(format!("{id}.lock")))
                .unwrap();
        file.lock().unwrap();
        assert!(codex_writer_busy(&home, &id));
        drop(file);
        assert!(!codex_writer_busy(&home, &id));
        assert!(!codex_writer_busy(&home, "../../somewhere"));
        std::fs::remove_dir_all(home).unwrap();
    }

    async fn set_run(fixture: &Fixture, update: impl FnOnce(&mut AcceptanceRun)) {
        let shared = fixture.state.acceptance.get(&fixture.id).await.unwrap();
        let mut run = shared.lock().await;
        update(&mut run);
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
    }

    #[tokio::test]
    async fn analysis_reaches_source_or_new_session_once_with_evidence_and_inherited_settings() {
        for new_session in [false, true] {
            let (fixture, provider, source) = repair_fixture("Analysis complete".into()).await;
            fixture
                .state
                .update_session_profile(&source.id, "default")
                .await
                .unwrap();
            set_run(&fixture, |run| {
                run.automatic = true;
                run.automation = AutomationPhase::Completed;
                run.automation_error = Some("actual-runtime-error".into());
                run.checklist = vec![
                    serde_json::from_value(json!({
                        "id":"focus", "title":"Focus survives navigation", "status":"failed",
                        "notes":"focus-lost-fixture", "evidence":["missing.png"]
                    }))
                    .unwrap(),
                ];
                run.auto_verdict = Some(
                    serde_json::from_value(json!({"passed":false, "notes":"agent-diagnosis"}))
                        .unwrap(),
                );
                run.result = Some(AcceptanceResult {
                    passed: true,
                    notes: "human-override".into(),
                    submitted_by: "tester".into(),
                    submitted_at: chrono::Utc::now(),
                });
                run.artifacts.push(crate::acceptance::Artifact {
                    name: "missing.png".into(),
                    content_type: "image/png".into(),
                    bytes: 1,
                    created_at: chrono::Utc::now(),
                });
            })
            .await;
            let before = fixture
                .state
                .acceptance
                .snapshot(&fixture.id)
                .await
                .unwrap();
            let (url, server) = fixture.server().await;
            let client = reqwest::Client::new();
            let input = json!({"request_id":Uuid::new_v4(), "new_session":new_session,
                "agent":"pi", "provider_id":"must-not-override-source", "model":"must-not-override-source"});
            let mut target = String::new();
            for _ in 0..2 {
                let response = client
                    .post(format!("{url}/v2/acceptance/{}/analyze", fixture.id))
                    .headers(fixture.headers.clone())
                    .json(&input)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::ACCEPTED,
                    "{}",
                    response.text().await.unwrap()
                );
                let data: Value = response.json::<Value>().await.unwrap()["data"].clone();
                assert_eq!(data["agent"], "codex");
                assert_eq!(data["provider_id"], json!(source.provider_id));
                assert!(data["model"].is_null());
                assert_eq!(data["profile_id"], "default");
                if !target.is_empty() {
                    assert_eq!(data["id"], target);
                }
                target = data["id"].as_str().unwrap().into();
            }
            assert_eq!(target == source.id, !new_session);
            if new_session {
                assert!(!crate::models::is_internal_acceptance_session_id(&target));
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while provider.requests.lock().await.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let requests = provider.requests.lock().await;
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].0, target);
            for expected in [
                "test-commit",
                "focus-lost-fixture",
                "actual-runtime-error",
                "human-override",
            ] {
                assert!(requests[0].1.contains(expected), "missing {expected}");
            }
            assert!(requests[0].1.chars().count() < 1500);
            assert!(!requests[0].1.contains("content_type"));
            let report_path = fixture
                .state
                .runtime_plugins
                .root
                .join("runs")
                .join(&fixture.id)
                .join("analyses")
                .join(format!(
                    "{}.report.json",
                    input["request_id"].as_str().unwrap()
                ));
            assert!(requests[0].1.contains(report_path.to_str().unwrap()));
            let report: Value =
                serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
            assert_eq!(report["automatic_verdict"]["notes"], "agent-diagnosis");
            assert_eq!(report["human_result"]["notes"], "human-override");
            assert_eq!(report["evidence"][0]["name"], "recording.mp4");
            assert_eq!(report["evidence"][1]["available"], false);
            drop(requests);
            let after = fixture
                .state
                .acceptance
                .snapshot(&fixture.id)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(&before).unwrap(),
                serde_json::to_value(&after).unwrap()
            );
            server.abort();
        }
    }

    #[tokio::test]
    async fn project_runs_can_open_new_analysis_without_a_source_conversation() {
        let (fixture, provider, _) = repair_fixture("Analysis".into()).await;
        set_run(&fixture, |run| run.session_id = None).await;
        let (url, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let endpoint = format!("{url}/v2/acceptance/{}/analyze", fixture.id);
        let response = client
            .post(&endpoint)
            .headers(fixture.headers.clone())
            .json(&json!({"request_id":Uuid::new_v4()}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(provider.requests.lock().await.is_empty());
        let response = client
            .post(&endpoint)
            .headers(fixture.headers.clone())
            .json(&json!({"request_id":Uuid::new_v4(),"new_session":true,"agent":"codex"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        server.abort();
    }

    #[tokio::test]
    async fn large_results_are_referenced_by_a_bounded_summary() {
        let fixture = Fixture::new().await;
        set_run(&fixture, |run| {
            run.error = Some("long-environment-error ".repeat(3000));
            run.checklist = (0..40)
                .map(|index| {
                    serde_json::from_value(json!({
                        "id":format!("check-{index}"), "title":format!("Case {index}"),
                        "status":"blocked", "notes":"detailed-diagnostic ".repeat(2000),
                    }))
                    .unwrap()
                })
                .collect();
        })
        .await;
        let run = fixture
            .state
            .acceptance
            .snapshot(&fixture.id)
            .await
            .unwrap();
        let path = fixture.root.join("report.json");
        let summary = analysis_prompt(&fixture.state, &run, &path).await.unwrap();
        assert!(
            summary.chars().count() < 1500,
            "summary length: {}",
            summary.chars().count()
        );
        assert!(summary.contains("阻塞 40"));
        assert!(!summary.contains("Case 39"));
        let report: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(report["checks"].as_array().unwrap().len(), 40);
        assert_eq!(report["checks"][39]["notes"], run.checklist[39].notes);
    }

    #[tokio::test]
    async fn analysis_requires_authentication_and_a_settled_result() {
        let (fixture, provider, _) = repair_fixture("Analysis".into()).await;
        set_run(&fixture, |run| {
            run.phase = RunPhase::Running;
            run.automation = AutomationPhase::Executing;
        })
        .await;
        let (url, server) = fixture.server().await;
        let client = reqwest::Client::new();
        let endpoint = format!("{url}/v2/acceptance/{}/analyze", fixture.id);
        let input = json!({"request_id":Uuid::new_v4(), "new_session":true, "agent":"codex"});
        let response = client.post(&endpoint).json(&input).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = client
            .post(&endpoint)
            .headers(fixture.headers.clone())
            .json(&input)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(provider.requests.lock().await.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn analyzing_in_an_active_source_session_returns_conflict_without_sending() {
        let (fixture, provider, source) = repair_fixture("Analysis".into()).await;
        fixture
            .state
            .create_domain_turn(
                &source.id,
                &CreateTurnCommand {
                    command_id: "existing-command".into(),
                    turn_id: "existing-turn".into(),
                    user_message_id: "existing-input".into(),
                    content: "Existing work".into(),
                    attachments: Vec::new(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    expected_session_version: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .await
            .unwrap();
        let (url, server) = fixture.server().await;
        let response = reqwest::Client::new()
            .post(format!("{url}/v2/acceptance/{}/analyze", fixture.id))
            .headers(fixture.headers.clone())
            .json(&json!({"request_id":Uuid::new_v4()}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(provider.requests.lock().await.is_empty());
        let domain = fixture
            .state
            .domain_session_state(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(domain.turns.len(), 1);
        assert_eq!(
            domain.session.active_turn_id.as_deref(),
            Some("existing-turn")
        );
        server.abort();
    }
}
