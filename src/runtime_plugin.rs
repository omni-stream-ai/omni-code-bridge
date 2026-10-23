//! Provisioned Bridge plugins. Installation is independent of agent turns.
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::Mutex,
};
use uuid::Uuid;

pub const PLUGIN_ID: &str = "x11box-acceptance";
pub const MANIFEST: &str = include_str!("../plugins/x11box-acceptance/manifest.json");
const OUTPUT_LIMIT: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallPhase {
    NotInstalled,
    Checking,
    Preparing,
    Verifying,
    Ready,
    Failed,
}

impl InstallPhase {
    pub fn in_progress(&self) -> bool {
        matches!(self, Self::Checking | Self::Preparing | Self::Verifying)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginInstallation {
    pub id: String,
    pub version: String,
    pub phase: InstallPhase,
    pub operation_id: Option<String>,
    pub image_id: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
}

impl Default for PluginInstallation {
    fn default() -> Self {
        Self {
            id: PLUGIN_ID.into(),
            version: "1.0.0".into(),
            phase: InstallPhase::NotInstalled,
            operation_id: None,
            image_id: None,
            updated_at: Utc::now(),
            error: None,
        }
    }
}

/// These settings belong to the host administrator, never to a remote request.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub image: Option<String>,
    pub source_dir: Option<PathBuf>,
    pub docker: PathBuf,
}

impl RuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            image: std::env::var("OMNI_CODE_X11BOX_IMAGE")
                .ok()
                .filter(|s| !s.is_empty()),
            source_dir: std::env::var_os("OMNI_CODE_X11BOX_SOURCE").map(PathBuf::from),
            docker: std::env::var_os("OMNI_CODE_DOCKER_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("docker")),
        }
    }
}

pub struct RuntimePluginStore {
    pub root: PathBuf,
    pub config: RuntimeConfig,
    installation: Mutex<Option<PluginInstallation>>,
    pub lifecycle: Mutex<()>,
}

impl RuntimePluginStore {
    pub fn new(root: PathBuf, config: RuntimeConfig) -> Self {
        Self {
            root,
            config,
            installation: Mutex::new(None),
            lifecycle: Mutex::new(()),
        }
    }

    fn read_installation(&self) -> Result<PluginInstallation> {
        let path = self.root.join("installation.json");
        let mut value: PluginInstallation = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("invalid runtime plugin state")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
            Err(e) => return Err(e).context("read runtime plugin state"),
        };
        // A new process cannot own the old asynchronous installation operation.
        if value.phase.in_progress() {
            value.phase = InstallPhase::Failed;
            value.error =
                Some("Installation interrupted by Bridge restart; retry installation".into());
            value.updated_at = Utc::now();
            atomic_json(&path, &value)?;
        }
        Ok(value)
    }

    pub async fn status(&self) -> Result<PluginInstallation> {
        let mut state = self.installation.lock().await;
        if state.is_none() {
            *state = Some(self.read_installation()?);
        }
        Ok(state.as_ref().unwrap().clone())
    }

    /// Persist before spawning, so concurrent installs join the same operation.
    pub async fn install(self: &Arc<Self>) -> Result<PluginInstallation> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut state = self.installation.lock().await;
        if state.is_none() {
            *state = Some(self.read_installation()?);
        }
        let current = state.as_ref().unwrap();
        if current.phase.in_progress() || current.phase == InstallPhase::Ready {
            return Ok(current.clone());
        }
        let value = PluginInstallation {
            phase: InstallPhase::Checking,
            operation_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        atomic_json(&self.root.join("installation.json"), &value)?;
        *state = Some(value.clone());
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let result = store.provision().await;
            let mut state = store.installation.lock().await;
            let mut value = state.as_ref().unwrap().clone();
            match result {
                Ok(image_id) => {
                    value.phase = InstallPhase::Ready;
                    value.image_id = Some(image_id);
                }
                Err(error) => {
                    value.phase = InstallPhase::Failed;
                    value.error = Some(format!("{error:#}"));
                }
            }
            value.updated_at = Utc::now();
            if let Err(error) = atomic_json(&store.root.join("installation.json"), &value) {
                value.phase = InstallPhase::Failed;
                value.error = Some(format!("Cannot persist installation result: {error}"));
            }
            *state = Some(value);
        });
        Ok(value)
    }

    pub async fn mark_uninstalled(&self) -> Result<PluginInstallation> {
        let mut state = self.installation.lock().await;
        if state.is_none() {
            *state = Some(self.read_installation()?);
        }
        if state.as_ref().unwrap().phase.in_progress() {
            bail!("installation is still in progress");
        }
        let value = PluginInstallation::default();
        atomic_json(&self.root.join("installation.json"), &value)?;
        *state = Some(value.clone());
        Ok(value)
    }

    async fn phase(&self, phase: InstallPhase) -> Result<()> {
        let mut state = self.installation.lock().await;
        let mut value = state.as_ref().context("installation not started")?.clone();
        value.phase = phase;
        value.updated_at = Utc::now();
        atomic_json(&self.root.join("installation.json"), &value)?;
        *state = Some(value);
        Ok(())
    }

    async fn provision(&self) -> Result<String> {
        self.docker(&["info", "--format", "{{.OSType}}"], 30)
            .await
            .context("Docker must be installed and accessible to Bridge")?
            .trim()
            .eq("linux")
            .then_some(())
            .context("x11box requires Linux containers")?;
        self.phase(InstallPhase::Preparing).await?;
        let image = if let Some(image) = &self.config.image {
            validate_image_reference(image)?;
            if image.starts_with("sha256:") {
                self.docker(&["image", "inspect", image], 30).await?;
            } else {
                self.docker(&["pull", image], 1800).await?;
            }
            image.clone()
        } else if let Some(source) = &self.config.source_dir {
            let source = source
                .canonicalize()
                .context("resolve x11box source directory")?;
            if !source.join("src/main.rs").is_file() || !source.join("Cargo.lock").is_file() {
                bail!("x11box source must contain src/main.rs and Cargo.lock");
            }
            let dockerfile = self.root.join("x11box.Dockerfile");
            std::fs::create_dir_all(&self.root)?;
            std::fs::write(&dockerfile, include_str!("../deploy/x11box.Dockerfile"))?;
            let tag = format!("omni-x11box-install:{}", Uuid::new_v4().simple());
            self.docker(
                &[
                    "build",
                    "--file",
                    path_str(&dockerfile)?,
                    "--tag",
                    &tag,
                    path_str(&source)?,
                ],
                3600,
            )
            .await?;
            tag
        } else {
            bail!(
                "Configure OMNI_CODE_X11BOX_IMAGE with an immutable image digest, or OMNI_CODE_X11BOX_SOURCE with the x11box source directory"
            );
        };
        let image_id = self
            .docker(&["image", "inspect", "--format", "{{.Id}}", &image], 30)
            .await?
            .trim()
            .to_owned();
        validate_image_reference(&image_id)?;
        self.phase(InstallPhase::Verifying).await?;
        self.verify_image(&image_id).await?;
        Ok(image_id)
    }

    pub(crate) async fn verify_image(&self, image: &str) -> Result<()> {
        self.docker(
            &[
                "run", "--rm", "--network=none", "--cap-drop=ALL",
                "--security-opt=no-new-privileges", "--entrypoint=timeout", image,
                "--kill-after=2s", "25s", "sh", "-c",
                "test \"$(id -u)\" = 1000 && test -w /workspace && test -w /var/lib/x11box && command -v x11box && command -v ffmpeg",
            ], 30,
        ).await.context("image must retain the writable x11box user environment")?;
        let name = format!("omni-x11box-check-{}", Uuid::new_v4().simple());
        let result = async {
            self.docker(
                &[
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &name,
                    "--label",
                    "ai.omnistream.acceptance=healthcheck",
                    "--init",
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges",
                    "--pids-limit=256",
                    "--memory=1g",
                    "--shm-size=256m",
                    "--network=none",
                    "--entrypoint=timeout",
                    image,
                    "--kill-after=5s",
                    "240s",
                    "x11box",
                    "agent",
                    "serve",
                    "--listen",
                    "127.0.0.1:27811",
                ],
                60,
            )
            .await?;
            self.docker(
                &[
                    "exec",
                    &name,
                    "x11box",
                    "x11",
                    "start",
                    "--desktop",
                    "--web",
                    "--bind-tcp",
                    "127.0.0.1:14500",
                    "--size",
                    "mobile",
                ],
                120,
            )
            .await?;
            self.docker(
                &[
                    "exec",
                    &name,
                    "curl",
                    "--fail",
                    "--max-time",
                    "10",
                    "http://127.0.0.1:27811/healthz",
                ],
                20,
            )
            .await?;
            let html = self
                .docker(
                    &[
                        "exec",
                        &name,
                        "curl",
                        "--fail",
                        "--max-time",
                        "10",
                        "http://127.0.0.1:14500/",
                    ],
                    20,
                )
                .await?;
            if !html.to_lowercase().contains("<html") {
                bail!("Xpra HTML5 client is unavailable");
            }
            let screenshot = self
                .docker(&["exec", &name, "x11box", "do", "screenshot"], 30)
                .await?;
            let shot: serde_json::Value =
                serde_json::from_str(&screenshot).context("invalid screenshot response")?;
            if shot["ok"] != true {
                bail!("x11box screenshot health check failed");
            }
            Ok(())
        }
        .await;
        // Also remove partially created containers when startup or checks fail.
        let cleanup = self.docker(&["rm", "--force", &name], 30).await;
        result?;
        cleanup.context("remove installation health-check container")?;
        Ok(())
    }

    pub async fn docker(&self, args: &[&str], seconds: u64) -> Result<String> {
        run_bounded(&self.config.docker, args, Duration::from_secs(seconds)).await
    }
}

pub fn validate_image_reference(value: &str) -> Result<()> {
    let digest = value
        .strip_prefix("sha256:")
        .or_else(|| value.split_once("@sha256:").map(|(_, digest)| digest));
    if value.starts_with('-')
        || value.bytes().any(|b| b.is_ascii_whitespace())
        || !digest.is_some_and(|d| d.len() == 64 && d.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        bail!("Image must be an immutable repository@sha256:digest or local sha256:image-id");
    }
    Ok(())
}

pub fn path_str(path: &Path) -> Result<&str> {
    path.to_str().context("path is not valid UTF-8")
}

/// Atomic replacement with file and directory sync for crash recovery.
pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    use std::io::Write;
    let parent = path.parent().context("state path needs a parent")?;
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".state-{}", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

async fn read_tail(mut reader: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut buf = [0; 8192];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(tail);
        }
        tail.extend_from_slice(&buf[..n]);
        if tail.len() > OUTPUT_LIMIT {
            tail.drain(..tail.len() - OUTPUT_LIMIT);
        }
    }
}

pub async fn run_bounded(program: &Path, args: &[&str], deadline: Duration) -> Result<String> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", program.display()))?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let result = tokio::time::timeout(deadline, async {
        tokio::try_join!(child.wait(), read_tail(stdout), read_tail(stderr))
    })
    .await;
    let (status, stdout, stderr) = match result {
        Ok(result) => result?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            bail!(
                "{} operation timed out after {} seconds",
                program.display(),
                deadline.as_secs()
            );
        }
    };
    if !status.success() {
        bail!(
            "{} exited with {}: {}",
            program.display(),
            status,
            String::from_utf8_lossy(&stderr)
        );
    }
    String::from_utf8(stdout).context("command response was not UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> RuntimePluginStore {
        RuntimePluginStore::new(
            std::env::temp_dir().join(format!("runtime-plugin-{}", Uuid::new_v4())),
            RuntimeConfig {
                image: None,
                source_dir: None,
                docker: PathBuf::from("docker"),
            },
        )
    }

    #[test]
    fn requires_immutable_images() {
        for image in ["latest", "repo:1", "--privileged", "repo@sha256:abc", ""] {
            assert!(validate_image_reference(image).is_err());
        }
        assert!(validate_image_reference(&format!("repo@sha256:{}", "a".repeat(64))).is_ok());
        assert!(validate_image_reference(&format!("sha256:{}", "0".repeat(64))).is_ok());
    }

    #[tokio::test]
    async fn restart_marks_incomplete_installation_retryable() {
        let store = test_store();
        atomic_json(
            &store.root.join("installation.json"),
            &PluginInstallation {
                phase: InstallPhase::Verifying,
                ..Default::default()
            },
        )
        .unwrap();
        let status = store.status().await.unwrap();
        assert_eq!(status.phase, InstallPhase::Failed);
        assert!(status.error.unwrap().contains("restart"));
        std::fs::remove_dir_all(store.root).unwrap();
    }

    #[tokio::test]
    async fn corrupt_state_is_not_silently_reset() {
        let store = test_store();
        std::fs::create_dir_all(&store.root).unwrap();
        std::fs::write(store.root.join("installation.json"), "bad state").unwrap();
        assert!(store.status().await.is_err());
        std::fs::remove_dir_all(store.root).unwrap();
    }

    #[tokio::test]
    async fn output_is_drained_but_memory_bounded() {
        let input = vec![b'a'; OUTPUT_LIMIT * 4];
        assert_eq!(
            read_tail(input.as_slice()).await.unwrap().len(),
            OUTPUT_LIMIT
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commands_have_deadlines_and_nonzero_errors() {
        let result = run_bounded(Path::new("sleep"), &["10"], Duration::from_millis(50)).await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
        assert!(
            run_bounded(Path::new("false"), &[], Duration::from_secs(1))
                .await
                .is_err()
        );
    }
}
