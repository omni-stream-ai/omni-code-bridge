//! A discovered X11 window can still be unmapped while Flutter waits for a frame.
use super::*;

const SESSION_ENV: &str = "/var/lib/x11box/state/x11box/env";
// Read only the display settings; never source the file or inherit the app's
// Nix library path into Debian's X11 tools.
const VISIBLE_WINDOW_PROBE: &str = r#"
import os, pathlib, subprocess, sys
env = os.environ.copy()
env['LC_ALL'] = 'C'
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    key, sep, value = line.removeprefix('export ').partition('=')
    if sep and key in ('DISPLAY', 'XAUTHORITY') and value:
        env[key] = value
for window in sys.argv[2:]:
    try:
        int(window, 16 if window.startswith('0x') else 10)
    except ValueError:
        continue
    result = subprocess.run(['xwininfo', '-id', window], env=env,
                            capture_output=True, text=True, timeout=3)
    if result.returncode == 0 and 'Map State: IsViewable' in result.stdout:
        print(window)
        sys.exit(0)
sys.exit('应用窗口已创建但尚未显示，等待首帧渲染')
"#;

impl AcceptanceStore {
    pub(super) async fn check_desktop_frame(
        &self,
        id: &str,
        startup: &StartupDecision,
        observation: &Value,
    ) -> Result<()> {
        let windows = observation["windows"]
            .as_array()
            .context("没有应用窗口观测")?;
        let ids: Vec<_> = windows
            .iter()
            .filter(|window| startup.matches_window(window))
            .filter_map(|window| window["id"].as_str())
            .take(32)
            .collect();
        anyhow::ensure!(!ids.is_empty(), "应用窗口缺少可验证的 X11 ID");
        let name = container_name(id);
        let mut args = vec![
            "exec",
            &name,
            "python3",
            "-c",
            VISIBLE_WINDOW_PROBE,
            SESSION_ENV,
        ];
        args.extend(ids);
        self.runtime
            .docker(&args, 10)
            .await
            .context("应用窗口尚不可见")?;
        // Capture through the same app selector that the executor will use.
        // A title alone does not prove that its client area can be captured.
        let frame = self
            .screenshot_named(id, None)
            .await
            .context("应用窗口截图尚未就绪")?;
        let (path, _) = self.artifact(id, &frame.name).await?;
        let image =
            image::load_from_memory(&std::fs::read(path)?).context("应用窗口截图不是有效图片")?;
        anyhow::ensure!(image.width() > 1 && image.height() > 1, "应用窗口截图为空");
        Ok(())
    }

    pub(super) async fn startup_log(&self, id: &str) -> Result<String> {
        self.runtime
            .docker(
                &[
                    "exec",
                    &container_name(id),
                    "sh",
                    "-c",
                    "tail -c 6000 /var/lib/x11box/state/x11box/start-app.log 2>/dev/null || true",
                ],
                10,
            )
            .await
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{acceptance_test_support::Fixture, runtime_plugin::RuntimeConfig};
    use std::{os::unix::fs::PermissionsExt, process::Command};

    const DOCKER: &str = r#"#!/usr/bin/env python3
import json, pathlib, shutil, sys
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
if args[:1] == ['exec'] and args[2:3] == ['timeout']:
    args = args[:2] + args[5:]
mode = (root / 'mode').read_text()
def count(name):
    p = root / name
    n = int(p.read_text()) + 1 if p.exists() else 1
    p.write_text(str(n))
    return n
if args[0] == 'cp':
    shutil.copyfile(root / 'frame.png', args[2])
elif args[2:5] == ['x11box', 'do', 'observe']:
    count('observations')
    print(json.dumps({'ok':True, 'windows':[{'id':'10485762','title':'Omni Code'}]}))
elif args[2:4] == ['python3', '-c']:
    n = count('visibility')
    if mode == 'gl-failure' or n == 1:
        sys.exit('window is unmapped')
elif args[2:5] == ['x11box', 'do', 'screenshot']:
    if count('captures') <= (6 if mode == 'delayed-capture' else 1):
        sys.exit('capture-not-ready-fixture')
    print(json.dumps({'screenshot':{'path':'/tmp/frame.png'}}))
elif args[2:4] == ['sh', '-c'] and 'start-app.pid' not in args[4]:
    if mode in ('gl-failure', 'delayed-capture'):
        print('Failed to create OpenGL context: No available configurations for the given pixel format')
"#;

    async fn fixture(mode: &str) -> (Fixture, Arc<AcceptanceStore>, StartupDecision) {
        let fixture = Fixture::new().await;
        let docker = fixture.root.join("docker");
        std::fs::write(&docker, DOCKER).unwrap();
        std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(fixture.root.join("mode"), mode).unwrap();
        image::RgbImage::new(20, 20)
            .save(fixture.root.join("frame.png"))
            .unwrap();
        let store = Arc::new(AcceptanceStore::new(Arc::new(RuntimePluginStore::new(
            fixture.state.runtime_plugins.root.clone(),
            RuntimeConfig {
                image: None,
                source_dir: None,
                docker,
            },
        ))));
        let startup =
            parse_startup_decision(r#"{"kind":"desktop","command":"./app","app":"Omni Code"}"#)
                .unwrap();
        let mut run = store.snapshot(&fixture.id).await.unwrap();
        run.phase = RunPhase::Running;
        run.startup = Some(startup.clone());
        store.persist(&run).unwrap();
        *store.get(&fixture.id).await.unwrap().lock().await = run;
        (fixture, store, startup)
    }

    #[tokio::test]
    async fn desktop_readiness_waits_for_visible_window_and_successful_capture() {
        let (fixture, store, startup) = fixture("delayed-frame").await;
        tokio::time::timeout(
            Duration::from_secs(10),
            store.wait_for_app_ready(&fixture.id, Some(&startup)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("captures")).unwrap(),
            "2"
        );
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("visibility")).unwrap(),
            "3"
        );
        assert_eq!(
            store.snapshot(&fixture.id).await.unwrap().artifacts.len(),
            2
        );
    }

    #[tokio::test]
    async fn visible_window_capture_retry_ignores_historical_graphics_warning() {
        let (fixture, store, startup) = fixture("delayed-capture").await;
        tokio::time::timeout(
            Duration::from_secs(20),
            store.wait_for_app_ready(&fixture.id, Some(&startup)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("captures")).unwrap(),
            "7"
        );
    }

    #[tokio::test]
    async fn desktop_readiness_reports_live_process_graphics_failure() {
        let (fixture, store, startup) = fixture("gl-failure").await;
        let error = tokio::time::timeout(
            Duration::from_secs(12),
            store.wait_for_app_ready(&fixture.id, Some(&startup)),
        )
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
        assert!(error.contains("Failed to create OpenGL context"), "{error}");
        assert!(error.contains("应用窗口未能显示"), "{error}");
        assert!(!fixture.root.join("captures").exists());
    }

    #[test]
    fn visibility_probe_rejects_unmapped_window_and_uses_session_display() {
        let root = std::env::temp_dir().join(format!("acceptance-visible-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let xwininfo = root.join("xwininfo");
        std::fs::write(&xwininfo, "#!/bin/sh\n[ \"$DISPLAY\" = :91 ] || exit 2\n[ \"$XAUTHORITY\" = /isolated/auth ] || exit 3\ncat window-info\n").unwrap();
        std::fs::set_permissions(&xwininfo, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            root.join("env"),
            "DISPLAY=:91\nXAUTHORITY=/isolated/auth\nLD_LIBRARY_PATH=/wrong-driver\n",
        )
        .unwrap();
        let run = || {
            Command::new("python3")
                .args(["-c", VISIBLE_WINDOW_PROBE])
                .arg(root.join("env"))
                .arg("0x00a00002")
                .current_dir(&root)
                .env(
                    "PATH",
                    format!("{}:{}", root.display(), std::env::var("PATH").unwrap()),
                )
                .output()
                .unwrap()
        };
        std::fs::write(root.join("window-info"), "Map State: IsUnMapped\n").unwrap();
        assert!(!run().status.success());
        std::fs::write(root.join("window-info"), "Map State: IsViewable\n").unwrap();
        let result = run();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            String::from_utf8(result.stdout).unwrap().trim(),
            "0x00a00002"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
