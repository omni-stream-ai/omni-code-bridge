//! Durable acceptance runs; all mutating operations serialize on the run lock.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::runtime_plugin::{InstallPhase, RuntimePluginStore, atomic_json, path_str, run_bounded};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Starting,
    Running,
    Stopping,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptanceRun {
    pub id: String,
    pub project_id: String,
    pub session_id: Option<String>,
    pub revision: String,
    pub image_id: String,
    pub command: String,
    pub size: String,
    pub created_by: String,
    pub phase: RunPhase,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub error: Option<String>,
    pub desktop_port: Option<u16>,
    pub controller: Option<String>,
    pub control_expires_at: Option<DateTime<Utc>>,
    pub control_generation: u64,
    pub artifacts: Vec<Artifact>,
    pub result: Option<AcceptanceResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub name: String,
    pub content_type: String,
    pub bytes: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptanceResult {
    pub passed: bool,
    pub notes: String,
    pub submitted_by: String,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAcceptance {
    /// Client-generated UUID is also the idempotency key.
    pub id: String,
    pub project_id: String,
    pub session_id: Option<String>,
    pub command: String,
    #[serde(default = "default_size")]
    pub size: String,
}

fn default_size() -> String {
    "pc".into()
}

pub struct AcceptanceStore {
    runtime: Arc<RuntimePluginStore>,
    runs: Mutex<HashMap<String, Arc<Mutex<AcceptanceRun>>>>,
}

impl AcceptanceStore {
    pub fn new(runtime: Arc<RuntimePluginStore>) -> Self {
        Self {
            runtime,
            runs: Mutex::new(HashMap::new()),
        }
    }

    fn run_dir(&self, id: &str) -> Result<PathBuf> {
        validate_id(id)?;
        Ok(self.runtime.root.join("runs").join(id))
    }

    fn persist(&self, run: &AcceptanceRun) -> Result<()> {
        atomic_json(&self.run_dir(&run.id)?.join("run.json"), run)
    }

    pub async fn get(&self, id: &str) -> Result<Arc<Mutex<AcceptanceRun>>> {
        let dir = self.run_dir(id)?;
        let mut runs = self.runs.lock().await;
        if let Some(run) = runs.get(id) {
            return Ok(Arc::clone(run));
        }
        let bytes = std::fs::read(dir.join("run.json")).context("acceptance run not found")?;
        let mut run: AcceptanceRun = serde_json::from_slice(&bytes)?;
        if run.id != id {
            bail!("acceptance run identity mismatch");
        }
        // A disconnected controller must explicitly reacquire control.
        run.controller = None;
        run.control_expires_at = None;
        run.control_generation += 1;
        if matches!(run.phase, RunPhase::Starting | RunPhase::Stopping) {
            run.phase = RunPhase::Failed;
            run.error =
                Some("Operation interrupted by Bridge restart; stop this run to clean up".into());
        }
        self.persist(&run)?;
        let run = Arc::new(Mutex::new(run));
        runs.insert(id.into(), Arc::clone(&run));
        Ok(run)
    }

    pub async fn list(&self) -> Result<Vec<AcceptanceRun>> {
        let root = self.runtime.root.join("runs");
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };
        let mut runs = vec![];
        for entry in entries {
            let entry = entry?;
            let id = entry.file_name().to_string_lossy().to_string();
            if entry.file_type()?.is_dir()
                && validate_id(&id).is_ok()
                && entry.path().join("run.json").is_file()
            {
                runs.push(self.snapshot(&id).await?);
            }
        }
        runs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(runs)
    }

    /// Reads the last committed state even while a bounded runtime action is in
    /// flight. Polling must not wait behind a container build or shutdown.
    pub async fn snapshot(&self, id: &str) -> Result<AcceptanceRun> {
        self.get(id).await?;
        serde_json::from_slice(&std::fs::read(self.run_dir(id)?.join("run.json"))?)
            .context("read acceptance snapshot")
    }

    pub async fn renew_control(&self, id: &str, client: &str, generation: u64) -> Result<()> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        require_running(&run)?;
        if !has_controller(&run, Utc::now())
            || run.controller.as_deref() != Some(client)
            || run.control_generation != generation
        {
            bail!("desktop lease was revoked");
        }
        run.control_expires_at = Some(Utc::now() + chrono::Duration::seconds(90));
        self.persist(&run)
    }

    pub async fn create(
        self: &Arc<Self>,
        input: CreateAcceptance,
        project: &Path,
        client: &str,
    ) -> Result<AcceptanceRun> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        validate_id(&input.id)?;
        if input.command.trim().is_empty()
            || input.command.len() > 8192
            || input.command.contains('\0')
        {
            bail!("application command must contain 1 to 8192 bytes");
        }
        if !["mobile", "tablet", "laptop", "pc"].contains(&input.size.as_str()) {
            bail!("invalid window size");
        }
        let dir = self.run_dir(&input.id)?;
        if dir.join("run.json").exists() {
            let existing = self.get(&input.id).await?.lock().await.clone();
            if existing.project_id != input.project_id
                || existing.command != input.command
                || existing.size != input.size
                || existing.session_id != input.session_id
                || existing.created_by != client
            {
                bail!("acceptance id already exists with different parameters");
            }
            return Ok(existing);
        }
        let existing_runs = self.list().await?;
        if existing_runs
            .iter()
            .filter(|run| {
                matches!(
                    run.phase,
                    RunPhase::Starting | RunPhase::Running | RunPhase::Stopping
                )
            })
            .count()
            >= 4
        {
            bail!("maximum of four active acceptance runs reached");
        }
        let mut runs = self.runs.lock().await;
        let installation = self.runtime.status().await?;
        if installation.phase != InstallPhase::Ready {
            bail!("x11box plugin is not ready");
        }
        let image_id = installation.image_id.context("runtime image is missing")?;
        let project = project
            .canonicalize()
            .context("resolve project directory")?;
        let revision = run_bounded(
            Path::new("git"),
            &[
                "-C",
                path_str(&project)?,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
            ],
            Duration::from_secs(15),
        )
        .await?
        .trim()
        .to_owned();
        std::fs::create_dir_all(dir.join("artifacts"))?;
        let archive = dir.join("source.tar");
        // Archive the exact recorded commit. Host credentials and untracked files
        // are not mounted into the desktop container.
        run_bounded(
            Path::new("git"),
            &[
                "-C",
                path_str(&project)?,
                "archive",
                "--format=tar",
                "--output",
                path_str(&archive)?,
                &revision,
            ],
            Duration::from_secs(120),
        )
        .await?;
        let now = Utc::now();
        let run = AcceptanceRun {
            id: input.id.clone(),
            project_id: input.project_id,
            session_id: input.session_id,
            revision,
            image_id,
            command: input.command,
            size: input.size,
            created_by: client.into(),
            phase: RunPhase::Starting,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::hours(4),
            error: None,
            desktop_port: None,
            controller: None,
            control_expires_at: None,
            control_generation: 0,
            artifacts: vec![],
            result: None,
        };
        self.persist(&run)?;
        let shared = Arc::new(Mutex::new(run.clone()));
        runs.insert(run.id.clone(), Arc::clone(&shared));
        drop(runs);
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut run = shared.lock().await;
            match store.start_container(&run, &archive).await {
                Ok(port) => {
                    run.desktop_port = Some(port);
                    run.phase = RunPhase::Running;
                }
                Err(error) => {
                    run.phase = RunPhase::Failed;
                    run.error = Some(format!("{error:#}"));
                    let _ = store
                        .runtime
                        .docker(&["rm", "--force", &container_name(&run.id)], 30)
                        .await;
                }
            }
            run.updated_at = Utc::now();
            if let Err(error) = store.persist(&run) {
                run.phase = RunPhase::Failed;
                run.error = Some(format!("Cannot persist run state: {error}"));
                let _ = store
                    .runtime
                    .docker(&["rm", "--force", &container_name(&run.id)], 30)
                    .await;
            }
        });
        Ok(run)
    }

    async fn start_container(&self, run: &AcceptanceRun, archive: &Path) -> Result<u16> {
        let name = container_name(&run.id);
        self.runtime
            .docker(
                &[
                    "network",
                    "create",
                    "--label",
                    "ai.omnistream.acceptance=run",
                    &name,
                ],
                30,
            )
            .await?;
        self.runtime
            .docker(
                &[
                    "run",
                    "--detach",
                    "--name",
                    &name,
                    "--label",
                    "ai.omnistream.acceptance=run",
                    "--init",
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges",
                    "--pids-limit=512",
                    "--memory=2g",
                    "--cpus=2",
                    "--shm-size=256m",
                    "--publish",
                    "127.0.0.1::14500",
                    "--network",
                    &name,
                    &run.image_id,
                ],
                60,
            )
            .await?;
        self.runtime
            .docker(
                &[
                    "cp",
                    path_str(archive)?,
                    &format!("{name}:/workspace/source.tar"),
                ],
                120,
            )
            .await?;
        self.runtime
            .docker(
                &[
                    "exec",
                    &name,
                    "tar",
                    "--extract",
                    "--file",
                    "/workspace/source.tar",
                    "--directory",
                    "/workspace",
                    "--no-same-owner",
                ],
                120,
            )
            .await?;
        self.runtime
            .docker(
                &[
                    "exec",
                    &name,
                    "x11box",
                    "x11",
                    "start",
                    "--desktop",
                    "--web",
                    "--bind-tcp",
                    "0.0.0.0:14500",
                    "--size",
                    &run.size,
                    "--start-app",
                    &run.command,
                    "--session-mode",
                    "ephemeral",
                ],
                120,
            )
            .await?;
        self.runtime
            .docker(
                &[
                    "exec",
                    &name,
                    "x11box",
                    "x11",
                    "record",
                    "start",
                    "--fps",
                    "8",
                    "--output",
                    "/artifacts/recording.mp4",
                ],
                30,
            )
            .await?;
        let port = self
            .runtime
            .docker(&["port", &name, "14500/tcp"], 15)
            .await?;
        parse_loopback_port(&port)
    }

    pub async fn control(&self, id: &str, client: &str, acquire: bool) -> Result<AcceptanceRun> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        require_running(&run)?;
        let now = Utc::now();
        if has_controller(&run, now) && run.controller.as_deref() != Some(client) {
            bail!("another client holds desktop control");
        }
        if acquire {
            // Renewing a live lease preserves open connections.
            if !has_controller(&run, now) {
                run.control_generation += 1;
            }
            run.controller = Some(client.into());
            run.control_expires_at = Some(now + chrono::Duration::seconds(90));
        } else {
            run.controller = None;
            run.control_expires_at = None;
            run.control_generation += 1;
        }
        run.updated_at = now;
        self.persist(&run)?;
        Ok(run.clone())
    }

    pub async fn instruction(
        &self,
        id: &str,
        instruction: &str,
        app: Option<&str>,
    ) -> Result<Value> {
        if instruction.is_empty() || instruction.len() > 16384 {
            bail!("invalid instruction length");
        }
        let shared = self.get(id).await?;
        let run = shared.lock().await;
        require_running(&run)?;
        if has_controller(&run, Utc::now()) {
            bail!("automation paused while a client controls the desktop");
        }
        let name = container_name(&run.id);
        let mut args = vec![
            "exec",
            &name,
            "timeout",
            "--kill-after=2s",
            "20s",
            "x11box",
            "do",
            instruction,
            "--timeout-ms",
            "15000",
        ];
        if let Some(app) = app {
            args.extend(["--app", app]);
        }
        let result = self.runtime.docker(&args, 30).await?;
        let value: Value =
            serde_json::from_str(&result).context("invalid x11box instruction response")?;
        let log = self
            .run_dir(id)?
            .join(format!("action-{}.json", Uuid::new_v4()));
        atomic_json(
            &log,
            &serde_json::json!({"at": Utc::now(), "instruction": instruction, "app": app, "result": value}),
        )?;
        Ok(value)
    }

    pub async fn screenshot(&self, id: &str) -> Result<Artifact> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        require_running(&run)?;
        let name = container_name(id);
        let output = self
            .runtime
            .docker(&["exec", &name, "x11box", "do", "screenshot"], 30)
            .await?;
        let value: Value = serde_json::from_str(&output)?;
        let path = value["screenshot"]["path"]
            .as_str()
            .context("screenshot response has no path")?;
        let artifact_name = format!("screenshot-{}.png", Uuid::new_v4());
        let target = self.run_dir(id)?.join("artifacts").join(&artifact_name);
        self.runtime
            .docker(&["cp", &format!("{name}:{path}"), path_str(&target)?], 30)
            .await?;
        let artifact = Artifact {
            name: artifact_name,
            content_type: "image/png".into(),
            bytes: target.metadata()?.len(),
            created_at: Utc::now(),
        };
        run.artifacts.push(artifact.clone());
        run.updated_at = Utc::now();
        self.persist(&run)?;
        Ok(artifact)
    }

    pub async fn stop(&self, id: &str, result: Option<AcceptanceResult>) -> Result<AcceptanceRun> {
        let shared = self.get(id).await?;
        let mut run = shared.lock().await;
        if run.phase == RunPhase::Completed {
            return Ok(run.clone());
        }
        if result.as_ref().is_some_and(|r| r.notes.len() > 64 * 1024) {
            bail!("result notes are too long");
        }
        run.phase = RunPhase::Stopping;
        run.controller = None;
        run.control_expires_at = None;
        run.control_generation += 1;
        if result.is_some() {
            run.result = result;
        }
        self.persist(&run)?;
        let name = container_name(id);
        let recording = self
            .runtime
            .docker(&["exec", &name, "x11box", "x11", "record", "stop"], 30)
            .await;
        let target = self.run_dir(id)?.join("artifacts/recording.mp4");
        let copied = self
            .runtime
            .docker(
                &[
                    "cp",
                    &format!("{name}:/artifacts/recording.mp4"),
                    path_str(&target)?,
                ],
                60,
            )
            .await;
        if copied.is_ok() {
            run.artifacts.retain(|a| a.name != "recording.mp4");
            run.artifacts.push(Artifact {
                name: "recording.mp4".into(),
                content_type: "video/mp4".into(),
                bytes: target.metadata()?.len(),
                created_at: Utc::now(),
            });
        }
        let removed = self.runtime.docker(&["rm", "--force", &name], 30).await;
        let network_removed = self.runtime.docker(&["network", "rm", &name], 30).await;
        // Missing containers are already cleaned up; distinguish daemon failures.
        let removal_error = removed
            .err()
            .filter(|e| !e.to_string().contains("No such container"))
            .or_else(|| {
                network_removed
                    .err()
                    .filter(|e| !e.to_string().contains("not found"))
            });
        run.phase = if removal_error.is_some() {
            RunPhase::Failed
        } else {
            RunPhase::Completed
        };
        run.desktop_port = None;
        run.updated_at = Utc::now();
        run.error = removal_error
            .map(|e| format!("Container cleanup failed: {e:#}"))
            .or_else(|| {
                recording
                    .err()
                    .map(|e| format!("Recording stop failed: {e:#}"))
            })
            .or_else(|| {
                copied
                    .err()
                    .map(|e| format!("Recording could not be saved: {e:#}"))
            });
        self.persist(&run)?;
        atomic_json(&self.run_dir(id)?.join("artifacts/report.json"), &*run)?;
        let report_bytes = self
            .run_dir(id)?
            .join("artifacts/report.json")
            .metadata()?
            .len();
        run.artifacts.retain(|a| a.name != "report.json");
        run.artifacts.push(Artifact {
            name: "report.json".into(),
            content_type: "application/json".into(),
            bytes: report_bytes,
            created_at: Utc::now(),
        });
        self.persist(&run)?;
        Ok(run.clone())
    }

    pub async fn artifact(&self, id: &str, name: &str) -> Result<(PathBuf, Artifact)> {
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            bail!("invalid artifact name");
        }
        let run = self.get(id).await?;
        let run = run.lock().await;
        let artifact = run
            .artifacts
            .iter()
            .find(|a| a.name == name)
            .context("artifact not found")?
            .clone();
        let root = self.run_dir(id)?.join("artifacts").canonicalize()?;
        let path = root.join(name).canonicalize()?;
        if !path.starts_with(&root) || !path.is_file() {
            bail!("invalid artifact path");
        }
        Ok((path, artifact))
    }

    pub async fn uninstall(&self) -> Result<()> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        // Prevent new runs before taking the list to drain. Evidence is retained.
        self.runtime.mark_uninstalled().await?;
        let mut failures = vec![];
        for run in self.list().await? {
            if run.phase != RunPhase::Completed {
                match self.stop(&run.id, None).await {
                    Ok(stopped) if stopped.phase == RunPhase::Completed => (),
                    Ok(stopped) => failures.push(format!("{}: {:?}", run.id, stopped.error)),
                    Err(error) => failures.push(format!("{}: {error:#}", run.id)),
                }
            }
        }
        if !failures.is_empty() {
            bail!(
                "Plugin disabled; cleanup needs retry: {}",
                failures.join("; ")
            );
        }
        Ok(())
    }

    pub fn start_maintenance(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let Some(store) = weak.upgrade() else {
                    break;
                };
                let runs = match store.list().await {
                    Ok(runs) => runs,
                    Err(error) => {
                        eprintln!("acceptance maintenance: {error:#}");
                        continue;
                    }
                };
                for run in runs {
                    if run.phase == RunPhase::Completed {
                        continue;
                    }
                    if run.expires_at <= Utc::now() || run.phase == RunPhase::Failed {
                        if let Err(error) = store.stop(&run.id, None).await {
                            eprintln!("acceptance cleanup {}: {error:#}", run.id);
                        }
                    }
                }
            }
        });
    }
}

pub fn validate_id(id: &str) -> Result<()> {
    let uuid = Uuid::parse_str(id).context("acceptance id must be a UUID")?;
    if uuid.to_string() != id {
        bail!("acceptance id must be a canonical lowercase UUID");
    }
    Ok(())
}

pub fn container_name(id: &str) -> String {
    format!("omni-acceptance-{id}")
}

pub fn has_controller(run: &AcceptanceRun, now: DateTime<Utc>) -> bool {
    run.controller.is_some() && run.control_expires_at.is_some_and(|expires| expires > now)
}

fn require_running(run: &AcceptanceRun) -> Result<()> {
    if run.phase != RunPhase::Running {
        bail!("acceptance run is not running");
    }
    if run.expires_at <= Utc::now() {
        bail!("acceptance run expired");
    }
    Ok(())
}

fn parse_loopback_port(output: &str) -> Result<u16> {
    let port = output
        .trim()
        .strip_prefix("127.0.0.1:")
        .context("desktop port must bind only to loopback")?;
    let port = port.parse::<u16>().context("invalid desktop port")?;
    if port == 0 {
        bail!("invalid desktop port");
    }
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ids_cannot_escape_run_storage() {
        for id in [
            "../other",
            "/tmp",
            "",
            "AABBCCDD-0000-0000-0000-000000000000",
        ] {
            assert!(validate_id(id).is_err());
        }
        assert!(validate_id(&Uuid::new_v4().to_string()).is_ok());
    }
    #[test]
    fn rejects_public_or_ambiguous_port_mappings() {
        assert_eq!(parse_loopback_port("127.0.0.1:12345\n").unwrap(), 12345);
        for value in [
            "0.0.0.0:12345",
            "[::]:12345",
            "127.0.0.1:0",
            "127.0.0.1:1\n127.0.0.1:2",
        ] {
            assert!(parse_loopback_port(value).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "requires Docker and X11BOX_TEST_IMAGE containing the acceptance image ID"]
    async fn real_container_acceptance_lifecycle() {
        use crate::runtime_plugin::RuntimeConfig;
        let image = std::env::var("X11BOX_TEST_IMAGE").expect("set X11BOX_TEST_IMAGE");
        let root = std::env::temp_dir().join(format!("acceptance-e2e-{}", Uuid::new_v4()));
        let project = root.join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("index.html"), "<!doctype html><html><head><title>Acceptance fixture</title></head><body><button id='test' onclick=\"document.title='Clicked'\">验收按钮</button></body></html>").unwrap();
        let git = |args: &[&str]| {
            let result = std::process::Command::new("git")
                .arg("-C")
                .arg(&project)
                .args(args)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        };
        git(&["init", "--quiet"]);
        git(&["add", "index.html"]);
        git(&[
            "-c",
            "user.name=Acceptance Test",
            "-c",
            "user.email=acceptance@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ]);
        let runtime = Arc::new(RuntimePluginStore::new(
            root.join("runtime"),
            RuntimeConfig {
                image: Some(image),
                source_dir: None,
                docker: "docker".into(),
            },
        ));
        let installation = runtime.install().await.unwrap();
        let repeated = runtime.install().await.unwrap();
        assert_eq!(installation.operation_id, repeated.operation_id);
        tokio::time::timeout(Duration::from_secs(240), async {
            loop {
                let status = runtime.status().await.unwrap();
                if status.phase == InstallPhase::Ready {
                    break;
                }
                assert_ne!(status.phase, InstallPhase::Failed, "{:?}", status.error);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .expect("installation timed out");
        let store = Arc::new(AcceptanceStore::new(Arc::clone(&runtime)));
        let id = Uuid::new_v4().to_string();
        let created = store
            .create(
                CreateAcceptance {
                    id: id.clone(),
                    project_id: "fixture".into(),
                    session_id: None,
                    command: "chromium --no-sandbox --new-window file:///workspace/index.html"
                        .into(),
                    size: "mobile".into(),
                },
                &project,
                "test-client",
            )
            .await
            .unwrap();
        assert_eq!(created.phase, RunPhase::Starting);
        let run = store.get(&id).await.unwrap();
        let running = tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                let snapshot = run.lock().await.clone();
                if snapshot.phase != RunPhase::Starting {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(running.phase, RunPhase::Running, "{:?}", running.error);
        let screenshot = store.screenshot(&id).await.unwrap();
        assert!(screenshot.bytes > 0);
        let (path, _) = store.artifact(&id, &screenshot.name).await.unwrap();
        assert!(std::fs::read(path).unwrap().starts_with(b"\x89PNG"));
        store.control(&id, "test-client", true).await.unwrap();
        assert!(store.control(&id, "other-client", true).await.is_err());
        assert!(
            store
                .instruction(&id, "browser eval document.title", None)
                .await
                .is_err()
        );
        store.control(&id, "test-client", false).await.unwrap();
        let observed = store
            .instruction(&id, "browser eval document.title", None)
            .await
            .unwrap();
        assert!(
            observed.to_string().contains("Acceptance fixture"),
            "{observed}"
        );
        store
            .instruction(&id, "browser click '#test'", None)
            .await
            .unwrap();
        let clicked = store
            .instruction(&id, "browser eval document.title", None)
            .await
            .unwrap();
        assert!(clicked.to_string().contains("Clicked"), "{clicked}");
        let restored = AcceptanceStore::new(Arc::clone(&runtime));
        assert_eq!(
            restored.get(&id).await.unwrap().lock().await.revision,
            running.revision
        );
        let stopped = restored
            .stop(
                &id,
                Some(AcceptanceResult {
                    passed: true,
                    notes: "Browser click verified".into(),
                    submitted_by: "test-client".into(),
                    submitted_at: Utc::now(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(stopped.phase, RunPhase::Completed);
        assert!(stopped.error.is_none(), "{:?}", stopped.error);
        assert!(
            stopped
                .artifacts
                .iter()
                .any(|a| a.name == "recording.mp4" && a.bytes > 0)
        );
        assert!(stopped.artifacts.iter().any(|a| a.name == "report.json"));
        assert!(
            runtime
                .docker(&["inspect", &container_name(&id)], 10)
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
