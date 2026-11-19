//! Start required services before the UI, and supervise their lifetime together.
use super::*;

pub(crate) const RUNTIME_STARTUP_CONTRACT: &str = r#"业务验收范围不等于运行依赖范围。即使只验收客户端界面、焦点或快捷键，也必须从 README、连接配置和源码判断实际页面依赖哪些 API、后端、数据库、认证和测试数据。没有要求验收后端业务，不代表可以省略客户端运行所必需的后端。已保存的单客户端配置或旧选择理由不能替代这项依赖检查；补齐必要运行依赖不新增业务验收项。用户明确禁止必要依赖且无法运行时，应报告环境前置条件不足。
必须在本次隔离容器内基于冻结源码准备并启动所需服务，不能假设容器的 localhost 指向宿主服务，不能借用宿主现有服务作为冻结源码的运行产物。只能使用本轮专用状态目录、临时认证和测试数据，不读取或复制宿主的账号令牌、模型密钥、用户会话数据。服务可用、客户端连接与认证配置正确、已确认用例所需的最小测试数据可用后，才启动界面并执行用例；不能用空壳 HTTP 服务或伪造业务响应替代真实后端。缺少必要的合法凭据时明确报告前置条件不足。
启动 JSON 增加 services 数组，按依赖顺序列出真实的后台服务：{"name":"服务名称","command":"服务前台启动命令","ready_command":"容器内实际就绪检查命令","timeout_seconds":60}。command 不要自行后台化或串联阻塞式启动多个服务；Bridge 负责后台启动、逐一等待就绪、记录日志和退出清理。ready_command 必须只读、可重复、有界，检查实际服务/API 可用性，不能只检查进程存在或文件存在。默认等待 60 秒，允许 1–180 秒；所有服务就绪等待合计最多 180 秒。无依赖时明确返回 services:[]。
prepare_command 在所有依赖服务就绪后、客户端启动前执行，用于配置本轮专用客户端连接、认证和最小测试数据，并检查客户端实际使用的 API 能返回所需业务数据。无额外准备时返回空字符串。Bridge 会等待 prepare_command 成功再启动主 command；主 command 仅以前台方式运行真实客户端。不能把仅出现窗口当作应用及其后端已就绪。所有服务和客户端命令相对 /workspace 执行，保留各自正确的环境，不使用登录 shell 覆盖 PATH。"#;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupService {
    pub name: String,
    pub command: String,
    pub ready_command: String,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    60
}

pub(super) fn validate_services(services: &[StartupService]) -> Result<()> {
    anyhow::ensure!(services.len() <= 16, "最多配置 16 个运行依赖服务");
    let mut names = HashSet::new();
    for service in services {
        anyhow::ensure!(
            !service.name.trim().is_empty()
                && service.name.len() <= 200
                && names.insert(service.name.trim())
                && !service.name.contains('\0'),
            "运行依赖服务名称不能为空、重复或过长"
        );
        for (value, limit) in [(&service.command, 16_384), (&service.ready_command, 8192)] {
            anyhow::ensure!(
                !value.trim().is_empty() && value.len() <= limit && !value.contains('\0'),
                "运行依赖服务必须提供启动命令和有界的就绪检查"
            );
        }
        anyhow::ensure!(
            (1..=180).contains(&service.timeout_seconds),
            "运行依赖服务就绪超时必须在 1–180 秒之间"
        );
    }
    Ok(())
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

pub(super) fn render_script(decision: &StartupDecision, workspace: &Path, logs: &Path) -> String {
    let mut script = format!(
        "#!/bin/sh\nset -eu\ncd {}\n",
        quote(&workspace.to_string_lossy())
    );
    script.push_str(&format!("mkdir -p {}\n", quote(&logs.to_string_lossy())));
    let marker = quote(&logs.join("omni-acceptance-runtime.ready").to_string_lossy());
    script.push_str(&format!(
        "omni_ready_marker={marker}\nrm -f \"$omni_ready_marker\"\n"
    ));
    script.push_str(
        r#"command -v setsid >/dev/null || { echo '运行依赖服务需要 setsid' >&2; exit 1; }
omni_pids=''
omni_cleanup() {
    trap - EXIT HUP INT TERM
    rm -f "$omni_ready_marker"
    # env selects the external utility; dash's kill builtin rejects -- here.
    for omni_pid in $omni_pids; do env kill -TERM -- "-$omni_pid" 2>/dev/null || true; done
    sleep 0.1
    for omni_pid in $omni_pids; do
        env kill -KILL -- "-$omni_pid" 2>/dev/null || true
        wait "$omni_pid" 2>/dev/null || true
    done
}
trap omni_cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap 'exit 129' HUP
omni_deadline=$(($(date +%s) + 180))
"#,
    );
    for (index, service) in decision.services.iter().enumerate() {
        let log = quote(
            &logs
                .join(format!("omni-acceptance-service-{index}.log"))
                .to_string_lossy(),
        );
        let ready_log = quote(
            &logs
                .join(format!("omni-acceptance-service-{index}-ready.log"))
                .to_string_lossy(),
        );
        let failed = quote(&format!("运行依赖服务 {} 提前退出", service.name));
        let timed_out = quote(&format!(
            "运行依赖服务 {} 就绪检查超时：{}",
            service.name, service.ready_command
        ));
        script.push_str(&format!(
            r#"setsid /bin/sh -c {command} >{log} 2>&1 &
omni_service_{index}=$!
omni_pids="$omni_service_{index} $omni_pids"
omni_service_deadline=$(($(date +%s) + {timeout}))
while :; do
    if ! kill -0 "$omni_service_{index}" 2>/dev/null; then
        printf '%s\n' {failed} >&2
        tail -c 4000 {log} >&2 || true
        exit 1
    fi
    if timeout --kill-after=1s 5s /bin/sh -c {ready} >{ready_log} 2>&1; then break; fi
    omni_now=$(date +%s)
    if [ "$omni_now" -ge "$omni_service_deadline" ] || [ "$omni_now" -ge "$omni_deadline" ]; then
        printf '%s\n' {timed_out} >&2
        tail -c 4000 {ready_log} >&2 || true
        tail -c 4000 {log} >&2 || true
        exit 1
    fi
    sleep 1
done
printf '%s\n' {ready_message}
"#,
            command = quote(&service.command),
            ready = quote(&service.ready_command),
            timeout = service.timeout_seconds,
            ready_message = quote(&format!("运行依赖服务 {} 已就绪", service.name))
        ));
    }
    if !decision.prepare_command.trim().is_empty() {
        script.push_str(&format!(
            "printf '%s\n' '正在准备客户端连接、认证和测试数据'\nif timeout --kill-after=2s 180s /bin/sh -c {}; then :; else\nprintf '%s\n' '客户端连接或测试数据准备失败，停止验收启动' >&2\nexit 1\nfi\n",
            quote(&decision.prepare_command)
        ));
    }
    script.push_str("printf 'ready\\n' >\"$omni_ready_marker\"\n");
    let command = if decision.command.trim().is_empty() {
        "exec sleep 2147483647"
    } else {
        &decision.command
    };
    script.push_str(&format!("setsid /bin/sh -c {} &\nomni_application_pid=$!\nomni_pids=\"$omni_application_pid $omni_pids\"\nwhile kill -0 \"$omni_application_pid\" 2>/dev/null; do\n", quote(command)));
    for (index, service) in decision.services.iter().enumerate() {
        let log = quote(
            &logs
                .join(format!("omni-acceptance-service-{index}.log"))
                .to_string_lossy(),
        );
        let failed = quote(&format!("运行依赖服务 {} 在应用运行期间退出", service.name));
        script.push_str(&format!("if ! kill -0 \"$omni_service_{index}\" 2>/dev/null; then\nprintf '%s\\n' {failed} >&2\ntail -c 4000 {log} >&2 || true\nexit 1\nfi\n"));
    }
    script.push_str("sleep 1\ndone\nomni_exit=0\nwait \"$omni_application_pid\" || omni_exit=$?\nexit \"$omni_exit\"\n");
    script
}

pub(super) fn has_preparation(decision: &StartupDecision) -> bool {
    !decision.services.is_empty() || !decision.prepare_command.trim().is_empty()
}

pub(super) fn readiness_script(
    decision: &StartupDecision,
    workspace: &Path,
    logs: &Path,
) -> Option<String> {
    if !has_preparation(decision) {
        return None;
    }
    let marker = quote(&logs.join("omni-acceptance-runtime.ready").to_string_lossy());
    let mut probe = format!(
        "test -s {marker} || {{ echo '运行依赖或客户端前置准备尚未完成' >&2; exit 1; }}\ncd {}\n",
        quote(&workspace.to_string_lossy())
    );
    for service in &decision.services {
        let diagnostic = quote(&format!("运行依赖服务 {} 不可用", service.name));
        probe.push_str(&format!("timeout --kill-after=1s 5s /bin/sh -c {} || {{ printf '%s\\n' {diagnostic} >&2; exit 1; }}\n",quote(&service.ready_command)));
    }
    Some(probe)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("startup 'fixture-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        async fn run(&self, decision: &StartupDecision) -> std::process::Output {
            let script = self.0.join("startup.sh");
            std::fs::write(&script, render_script(decision, &self.0, &self.0)).unwrap();
            tokio::process::Command::new("timeout")
                .args(["--kill-after=1s", "8s", "/bin/sh"])
                .arg(script)
                .output()
                .await
                .unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn decision(service: &str, ready: &str, prepare: &str) -> StartupDecision {
        parse_startup_decision(&serde_json::json!({
            "kind":"desktop", "app":"fixture", "command":"test -f prepared && touch frontend",
            "services":[{"name":"真实 API 后端", "command":service, "ready_command":ready,"timeout_seconds":2}],
            "prepare_command":prepare
        }).to_string()).unwrap()
    }

    #[tokio::test]
    async fn services_become_ready_before_preparing_data_and_starting_the_frontend() {
        let fixture = Fixture::new();
        let d = decision(
            "echo $$ >backend.pid; sleep 0.2; touch backend-ready; exec sleep 6",
            "test -f backend-ready",
            "test -f backend-ready && touch prepared",
        );
        let output = fixture.run(&d).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(fixture.0.join("frontend").is_file());
        assert!(!fixture.0.join("omni-acceptance-runtime.ready").exists());
        #[cfg(target_os = "linux")]
        {
            let pid = std::fs::read_to_string(fixture.0.join("backend.pid")).unwrap();
            assert!(
                !Path::new("/proc").join(pid.trim()).exists(),
                "backend leaked after frontend exited"
            );
        }
    }

    #[tokio::test]
    async fn failed_service_reports_its_name_and_log_without_starting_frontend() {
        let fixture = Fixture::new();
        let d = decision(
            "echo backend-crash-fixture >&2; exit 23",
            "false",
            "touch prepared",
        );
        let output = fixture.run(&d).await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("真实 API 后端"), "{error}");
        assert!(error.contains("backend-crash-fixture"), "{error}");
        assert!(!fixture.0.join("frontend").exists());
    }

    #[tokio::test]
    async fn unavailable_service_times_out_with_diagnostics() {
        let fixture = Fixture::new();
        let mut d = decision(
            "exec sleep 6",
            "echo connection-refused-fixture >&2; exit 7",
            "touch prepared",
        );
        d.services[0].timeout_seconds = 1;
        let output = fixture.run(&d).await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("就绪检查超时"), "{error}");
        assert!(error.contains("connection-refused-fixture"), "{error}");
        assert!(!fixture.0.join("frontend").exists());
    }

    #[tokio::test]
    async fn failed_client_auth_or_data_preparation_blocks_frontend() {
        let fixture = Fixture::new();
        let d = decision(
            "exec sleep 6",
            "true",
            "echo missing-auth-fixture >&2; exit 17",
        );
        let output = fixture.run(&d).await;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("missing-auth-fixture"));
        assert!(!fixture.0.join("frontend").exists());
        assert!(!fixture.0.join("omni-acceptance-runtime.ready").exists());
    }

    #[tokio::test]
    async fn service_exit_while_frontend_runs_stops_the_application() {
        let fixture = Fixture::new();
        let mut d = decision(
            "touch backend-ready; sleep 0.5; echo runtime-crash-fixture >&2; exit 23",
            "test -f backend-ready",
            "touch prepared",
        );
        d.command = "touch frontend; exec sleep 6".into();
        let output = fixture.run(&d).await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("runtime-crash-fixture"), "{error}");
        assert!(!fixture.0.join("omni-acceptance-runtime.ready").exists());
    }

    #[test]
    fn dependency_readiness_requires_both_preparation_and_live_service_checks() {
        let fixture = Fixture::new();
        let d = decision("exec sleep 6", "test -f api-healthy", "touch prepared");
        let probe = readiness_script(&d, &fixture.0, &fixture.0).unwrap();
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &probe])
                .output()
                .unwrap()
        };
        assert!(!run().status.success());
        std::fs::write(fixture.0.join("omni-acceptance-runtime.ready"), "ready").unwrap();
        assert!(!run().status.success());
        std::fs::write(fixture.0.join("api-healthy"), "yes").unwrap();
        assert!(run().status.success());
    }

    #[test]
    fn dependency_contract_rejects_missing_checks_and_preserves_legacy_startups() {
        let mut value = serde_json::to_value(decision(
            "server",
            "curl --fail localhost",
            "prepare-fixture",
        ))
        .unwrap();
        for field in ["command", "ready_command"] {
            let mut invalid = value.clone();
            invalid["services"][0][field] = "".into();
            assert!(parse_startup_decision(&invalid.to_string()).is_err());
        }
        value["services"][0]["timeout_seconds"] = 0.into();
        assert!(parse_startup_decision(&value.to_string()).is_err());
        value["services"][0]["timeout_seconds"] = 60.into();
        let duplicate = value["services"][0].clone();
        value["services"].as_array_mut().unwrap().push(duplicate);
        assert!(parse_startup_decision(&value.to_string()).is_err());
        let old = parse_startup_decision(r#"{"command":"exec app", "app":"fixture"}"#).unwrap();
        assert!(old.services.is_empty());
        assert!(readiness_script(&old, Path::new("/workspace"), Path::new("/tmp")).is_none());
    }
}
