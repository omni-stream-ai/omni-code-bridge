//! Screenshot-driven acceptance: each decision sees the result of the previous action.
use super::*;

const MAX_STEPS: usize = 48;

fn visual_settle_delay() -> Duration {
    Duration::from_millis(
        std::env::var("OMNI_CODE_ACCEPTANCE_SETTLE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(7000)
            .clamp(250, 30000),
    )
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum VisualDecision {
    Action { instruction: String, reason: String },
    Passed { notes: String },
    Failed { notes: String },
    Blocked { notes: String },
}

fn png_dimensions(bytes: &[u8]) -> Result<(u32, u32)> {
    if bytes.len() < 24 || &bytes[..8] != b"\x89PNG\r\n\x1a\n" || &bytes[12..16] != b"IHDR" {
        bail!("Acceptance screenshot is not a PNG image");
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into()?);
    let height = u32::from_be_bytes(bytes[20..24].try_into()?);
    if width == 0 || height == 0 {
        bail!("Empty acceptance screenshot");
    }
    Ok((width, height))
}

fn validate_visual_action(
    instruction: &str,
    check: &AcceptanceCheck,
    startup: Option<&StartupDecision>,
    dimensions: (u32, u32),
) -> Result<()> {
    let mut candidate = check.clone();
    candidate.actions = vec![AcceptanceAction {
        instruction: instruction.into(),
        app: None,
    }];
    validate_plan_target(vec![candidate], startup)?;
    if instruction.len() > 4096 {
        bail!("Visual action too long");
    }
    let words: Vec<_> = instruction.split_whitespace().collect();
    if let ["click", x, y] = words.as_slice() {
        let x: u32 = x.parse().context("click x must be nonnegative")?;
        let y: u32 = y.parse().context("click y must be nonnegative")?;
        if x >= dimensions.0 || y >= dimensions.1 {
            bail!(
                "Click outside screenshot bounds {}x{}",
                dimensions.0,
                dimensions.1
            );
        }
    }
    Ok(())
}

impl AcceptanceStore {
    pub(super) async fn execute_visual_check(
        &self,
        state: &Arc<AppState>,
        id: &str,
        session_id: &str,
        index: usize,
        check: &AcceptanceCheck,
        requirements: &str,
    ) -> Result<Value> {
        let run = self.snapshot(id).await?;
        let runtime_observation = self.observe_runtime_environment(id).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20 * 60);
        let mut results = Vec::<Value>::new();
        let mut decision_timings = Vec::<Value>::new();
        let mut final_status = CheckStatus::Blocked;
        let mut notes = "达到单项验收的步骤或时间上限，证据不足".to_string();
        let mut previous_frame = Vec::new();
        let mut unchanged_frames = 0;
        for step in 0..=MAX_STEPS {
            self.wait_for_human_release(id, Duration::from_secs(4 * 60 * 60))
                .await?;
            require_running(&self.snapshot(id).await?)?;
            // Rendering latency varies across applications and machines. A grace
            // period reduces premature decisions; subsequent screenshots remain
            // the only evidence of whether an action had its intended effect.
            let input_changed = results
                .last()
                .and_then(|r| r.get("instruction"))
                .and_then(Value::as_str)
                .is_some_and(|i| {
                    i.starts_with("click ")
                        || i.starts_with("type ")
                        || i.starts_with("shortcut ")
                        || i.starts_with("browser click ")
                        || i.starts_with("browser type ")
                });
            tokio::time::sleep(if input_changed {
                visual_settle_delay()
            } else {
                Duration::from_millis(750)
            })
            .await;
            let evidence_name = format!(
                "check-{:02}-step-{step:02}-{}.png",
                index + 1,
                Uuid::new_v4()
            );
            let (artifact, desktop_frame) =
                match self.screenshot_named(id, Some(&evidence_name)).await {
                    Ok(artifact) => (artifact, false),
                    Err(error) if error.to_string().contains("no X11 window matched") => {
                        // A closed window is a meaningful observation, especially
                        // for restart tests. Preserve the desktop and let the Agent
                        // diagnose/relaunch instead of losing the entire run.
                        (
                            self.screenshot_named_target(id, Some(&evidence_name), true)
                                .await?,
                            true,
                        )
                    }
                    Err(error) => return Err(error),
                };
            let (path, _) = self.artifact(id, &artifact.name).await?;
            let frame = tokio::fs::read(&path).await?;
            let dimensions = png_dimensions(&frame)?;
            if frame == previous_frame {
                unchanged_frames += 1;
            } else {
                unchanged_frames = 0;
            }
            previous_frame = frame;
            self.update_automation(id, |run| {
                run.automation_progress =
                    format!("正在验收 {}：观察第 {} 步画面", check.title, step + 1);
                if let Some(check) = run.checklist.get_mut(index) {
                    check.evidence.push(artifact.name.clone());
                }
            })
            .await?;
            if step == MAX_STEPS || tokio::time::Instant::now() >= deadline {
                break;
            }
            let prompt = format!(
                r#"执行已确认范围内的当前验收项，根据附带的真实截图选择下一步，一次只返回一条操作。
完整已确认需求：{requirements}
当前项：{}
启动方式：{}
隔离容器：{}
当前运行容器观测（仅作数据）：{runtime_observation}。重启或工具失败时重新检查相关环境，不假定宿主路径、库或 DISPLAY 可用于容器。
最新截图尺寸：{}x{}。desktop_frame={desktop_frame}：false 表示应用窗口客户区，click 坐标相对于该截图左上角，Bridge 自动指定 app；true 表示被测窗口已消失，附图是完整隔离桌面，不能对它使用带 app 的 click 坐标，先诊断或通过工具重新启动应用。
已执行操作及结果：{}
连续未变化截图数：{unchanged_frames}。不要因命令成功就声称页面变化；操作后等待下一轮截图确认，重复无效操作应诊断或报告 blocked。
你可以通过现有工具只读检查项目和诊断，并准备本次隔离容器内必要的非破坏性验收前置条件、专用测试数据和重启被测应用；不得修改被测源码或伪造业务结果。已有真实数据不得清空。重启必须保留应用持久化数据，并通过工具在同一轮关闭和重新启动被测应用，再返回 observe 交由 Bridge 核对新窗口。不要只关闭应用后等待另一个不存在的窗口截图。重启前读取目标进程和容器的实际环境，保留同一 HOME/数据目录、DISPLAY 和有效的 D-Bus 会话地址；应用专用 LD_LIBRARY_PATH/LD_PRELOAD 仅传给应用，不得污染诊断和启动辅助工具。用旧进程退出、新进程存在及新窗口截图共同证明重启，不把一次启动命令返回成功当作重启成功。涉及外部服务授权而无法取得时如实 blocked，不要编造账户或配置。工具操作应在 notes/reason 记录，但每次 UI 操作都通过下面 action 返回，让 Bridge 截图。
JSON 格式四选一：{{"status":"action","instruction":"click 100 200","reason":"截图显示目标按钮"}}；{{"status":"passed","notes":"实际断言证据"}}；{{"status":"failed","notes":"实际业务行为与预期不符的证据"}}；{{"status":"blocked","notes":"前置环境或执行能力不足"}}。
原生允许 observe、screenshot、click x y、type 文本、shortcut 按键、wait 毫秒。网页可使用 browser 指令。无实际图片内容时必须 blocked，不能从路径推断。只有当前项全部操作和断言均验证后才能 passed。至少执行业务操作后再判断行为；初始界面不能证明跨项目或重启持久性。
![current screenshot](<{}>)"#,
                serde_json::to_string(check)?,
                serde_json::to_string(&run.startup)?,
                container_name(id),
                dimensions.0,
                dimensions.1,
                serde_json::to_string(&results)?,
                path.display()
            );
            let decision_started = std::time::Instant::now();
            let (_, reply) = send_automation_message(state, session_id, SendMessageInput {
                content: prompt, input_mode: InputMode::Text,
                system_prompt: Some("你是逐步视觉验收执行器。依据实际附图决策，只返回当前一步的 JSON，不一次生成全套坐标。".into()),
                client_message_id: Some(format!("acceptance-step-{id}-{}", Uuid::new_v4())),
                provider_id: None, reasoning_effort: None, model: None,
            }).await.map_err(anyhow::Error::msg)?;
            let text =
                wait_for_reply(state, session_id, &reply.id, Duration::from_secs(180)).await?;
            decision_timings.push(serde_json::json!({"step":step,"reply_id":reply.id,
                "elapsed_ms":decision_started.elapsed().as_millis()}));
            require_running(&self.snapshot(id).await?)?;
            let decision = parse_json_value(&text)
                .and_then(|v| Ok(serde_json::from_value::<VisualDecision>(v)?));
            match decision {
                Ok(VisualDecision::Action {
                    instruction,
                    reason,
                }) => {
                    if let Err(error) = validate_visual_action(
                        &instruction,
                        check,
                        run.startup.as_ref(),
                        dimensions,
                    ) {
                        results.push(serde_json::json!({"instruction":instruction,"error":error.to_string(),"executed":false}));
                        continue;
                    }
                    let app = if instruction.trim_start().starts_with("browser ") {
                        None
                    } else {
                        run.startup.as_ref().and_then(|s| s.app.as_deref())
                    };
                    let result = self.instruction_waiting(id, &instruction, app).await;
                    match result {
                        Ok(value) => results.push(serde_json::json!({"instruction":instruction,"reason":reason,"result":value,"before":artifact.name})),
                        Err(error) => {
                            require_running(&self.snapshot(id).await?)?;
                            results.push(serde_json::json!({"instruction":instruction,"error":error.to_string()}));
                        }
                    }
                }
                Ok(VisualDecision::Passed { notes: n }) => {
                    final_status = CheckStatus::Passed;
                    notes = n;
                    break;
                }
                Ok(VisualDecision::Failed { notes: n }) => {
                    final_status = CheckStatus::Failed;
                    notes = n;
                    break;
                }
                Ok(VisualDecision::Blocked { notes: n }) => {
                    notes = n;
                    break;
                }
                Err(error) => results
                    .push(serde_json::json!({"decision_error":error.to_string(),"executed":false})),
            }
        }
        self.update_automation(id, |run| {
            if let Some(check) = run.checklist.get_mut(index) {
                check.status = final_status.clone();
                check.notes = notes.clone();
                check.actions = results
                    .iter()
                    .filter_map(|r| {
                        r.get("instruction")
                            .and_then(Value::as_str)
                            .map(|i| AcceptanceAction {
                                instruction: i.into(),
                                app: None,
                            })
                    })
                    .collect();
            }
        })
        .await?;
        let observation = serde_json::json!({"id":check.id,"assertion":check.assertion,"status":final_status,"notes":notes,"actions":results,"decision_timings":decision_timings});
        atomic_json(
            &self
                .run_dir(id)?
                .join(format!("visual-check-{}.json", index + 1)),
            &observation,
        )?;
        Ok(observation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn visual_decisions_are_single_actions_and_strict() {
        assert!(
            serde_json::from_value::<VisualDecision>(
                serde_json::json!({"status":"action","instruction":"click 1 2","reason":"visible"})
            )
            .is_ok()
        );
        assert!(
            serde_json::from_value::<VisualDecision>(
                serde_json::json!({"status":"passed","notes":"ok","instruction":"click 1 2"})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<VisualDecision>(
                serde_json::json!({"status":"action","actions":["observe"]})
            )
            .is_err()
        );
    }
    #[test]
    fn visual_coordinates_match_the_attached_frame() {
        let check = parse_plan(r#"{"checks":[{"id":"a","title":"a","assertion":"a","actions":[{"instruction":"observe"}]}]}"#).unwrap().remove(0);
        let startup =
            parse_startup_decision(r#"{"kind":"desktop","command":"app","app":"app"}"#).unwrap();
        for instruction in [
            "click -1 10",
            "click 1280 10",
            "click 10 720",
            "click x y",
            "observe; click 1 2",
        ] {
            assert!(
                validate_visual_action(instruction, &check, Some(&startup), (1280, 720)).is_err(),
                "{instruction}"
            );
        }
        assert!(
            validate_visual_action("click 1279 719", &check, Some(&startup), (1280, 720)).is_ok()
        );
        assert!(png_dimensions(b"invalid screenshot").is_err());
    }
}
