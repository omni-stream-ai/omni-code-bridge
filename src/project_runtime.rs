//! Project services, independent of acceptance planning and test execution.
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use uuid::Uuid;

use crate::{
    acceptance::{CreateAcceptance, RunPhase},
    app_state::AppState,
    models::ProjectSummary,
    project_detection::{BuildCandidate, BuildTarget, Detection, detect},
    runtime_plugin::{RuntimePluginStore, atomic_json},
};

#[path = "project_runtime_agent.rs"]
pub(crate) mod agent;
#[path = "project_runtime_api.rs"]
pub(crate) mod api;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Location {
    #[default]
    Host,
    Docker,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Preferences {
    pub locations: BTreeMap<String, Location>,
    pub targets: BTreeMap<String, BuildTarget>,
    pub names: BTreeMap<String, String>,
    pub excluded: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Unknown,
}
impl Phase {
    pub fn active(self) -> bool {
        !matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub id: String,
    pub session_id: String,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub busy_until: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Runtime {
    pub id: String,
    pub project_id: String,
    pub directory: String,
    pub location: Location,
    pub phase: Phase,
    pub command: String,
    pub working_directory: String,
    pub pid: Option<u32>,
    // Linux boot ID + process creation time prevent acting on a recycled PID.
    pub host_identity: Option<String>,
    pub acceptance_run_id: Option<String>,
    pub revision: Option<String>,
    pub lease: Option<Lease>,
    pub error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

pub(crate) struct Entry {
    record: Runtime,
    child: Option<Child>,
    launching: bool,
}

impl Drop for Entry {
    fn drop(&mut self) {
        // Child::kill_on_drop only kills the shell; clean up its descendants too.
        #[cfg(unix)]
        if self.child.is_some() {
            if let Some(pid) = self.record.pid {
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", "--", &format!("-{pid}")])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

#[derive(Serialize)]
pub struct Overview {
    pub preferences: Preferences,
    pub targets: Vec<BuildTarget>,
    pub runtimes: Vec<Runtime>,
}

pub struct ProjectRuntimeStore {
    root: PathBuf,
    entries: Mutex<HashMap<String, Arc<Mutex<Entry>>>>,
    preferences_lock: Mutex<()>,
    pub(crate) profiles_lock: Mutex<()>,
    pub(crate) agent_server: Mutex<Option<agent::Server>>,
}

impl ProjectRuntimeStore {
    pub fn new(runtime: &RuntimePluginStore) -> Self {
        Self {
            root: runtime.root.join("project-runtimes"),
            entries: Mutex::new(HashMap::new()),
            preferences_lock: Mutex::new(()),
            profiles_lock: Mutex::new(()),
            agent_server: Mutex::new(None),
        }
    }

    fn project_dir(&self, project: &str) -> PathBuf {
        self.root.join(key(project))
    }
    fn record_path(&self, project: &str, directory: &str) -> PathBuf {
        self.project_dir(project)
            .join(format!("{}.json", key(directory)))
    }
    fn log_path(&self, record: &Runtime) -> PathBuf {
        self.project_dir(&record.project_id)
            .join(format!("{}.log", record.id))
    }
    fn persist(&self, entry: &Entry) -> Result<()> {
        atomic_json(
            &self.record_path(&entry.record.project_id, &entry.record.directory),
            &entry.record,
        )
    }
    pub fn preferences(&self, project: &str) -> Result<Preferences> {
        match std::fs::read(self.project_dir(project).join("preferences.json")) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Preferences::default()),
            Err(e) => Err(e.into()),
        }
    }
    pub async fn save_location(
        &self,
        project: &str,
        directory: &str,
        location: Location,
    ) -> Result<()> {
        validate_directory(directory)?;
        let _guard = self.preferences_lock.lock().await;
        let mut preferences = self.preferences(project)?;
        ensure!(
            !preferences.excluded.contains(directory),
            "子项目已删除，请重新读取配置"
        );
        preferences.locations.insert(directory.into(), location);
        ensure!(
            preferences.locations.len() <= 128,
            "最多保存 128 个子项目的运行位置"
        );
        atomic_json(
            &self.project_dir(project).join("preferences.json"),
            &preferences,
        )
    }
    pub async fn remember_targets(&self, project: &str, targets: &[BuildTarget]) -> Result<()> {
        crate::project_detection::validate_targets(targets)?;
        let _guard = self.preferences_lock.lock().await;
        let mut preferences = self.preferences(project)?;
        for target in targets {
            if preferences.excluded.contains(&target.directory) {
                continue;
            }
            preferences
                .targets
                .insert(target.directory.clone(), target.clone());
        }
        ensure!(
            preferences.targets.len() <= 128,
            "最多保存 128 个子项目配置"
        );
        atomic_json(
            &self.project_dir(project).join("preferences.json"),
            &preferences,
        )
    }
    pub fn targets(&self, state: &AppState, project: &ProjectSummary) -> Result<Vec<BuildTarget>> {
        let config = state.acceptance.environments.config(&project.id)?;
        let preferences = self.preferences(&project.id)?;
        let mut targets = preferences.targets;
        for target in &config.build_targets {
            targets.insert(target.directory.clone(), target.clone());
        }
        if !targets.is_empty() {
            return Ok(targets
                .into_values()
                .filter(|target| !preferences.excluded.contains(&target.directory))
                .collect());
        }
        if let Some(configuration) = config.build {
            return Ok(vec![BuildTarget {
                directory: ".".into(),
                configuration,
                ..Default::default()
            }]
            .into_iter()
            .filter(|target| !preferences.excluded.contains(&target.directory))
            .collect());
        }
        Ok(detect(Path::new(&project.root_path))
            .candidates
            .into_iter()
            .filter(|candidate| !preferences.excluded.contains(&candidate.directory))
            .map(|candidate| BuildTarget {
                directory: candidate.directory,
                configuration: candidate.configuration,
                ..Default::default()
            })
            .collect())
    }

    pub fn prepare_detection(
        &self,
        state: &AppState,
        project: &ProjectSummary,
        detection: &mut Detection,
    ) -> Result<()> {
        let preferences = self.preferences(&project.id)?;
        let previous_count = detection.candidates.len();
        detection
            .candidates
            .retain(|candidate| !preferences.excluded.contains(&candidate.directory));
        let mut changed = previous_count != detection.candidates.len();
        detection
            .targets
            .retain(|target| !preferences.excluded.contains(&target.directory));
        for target in self.targets(state, project)? {
            if !detection
                .candidates
                .iter()
                .any(|candidate| candidate.directory == target.directory)
            {
                detection.candidates.push(BuildCandidate {
                    directory: target.directory,
                    configuration: target.configuration,
                    evidence: Vec::new(),
                    warnings: Vec::new(),
                });
                changed = true;
            }
        }
        if changed {
            detection.configuration = if detection.candidates.len() == 1 {
                detection.candidates[0].configuration.clone()
            } else {
                Default::default()
            };
        }
        Ok(())
    }

    pub async fn save_target(
        &self,
        state: &AppState,
        project: &ProjectSummary,
        mut target: BuildTarget,
        name: String,
        create: bool,
    ) -> Result<()> {
        validate_directory(&target.directory)?;
        validate_name(&name)?;
        let root = Path::new(&project.root_path)
            .canonicalize()
            .context("项目目录不可用")?;
        let path = root
            .join(&target.directory)
            .canonicalize()
            .context("子项目目录不存在")?;
        ensure!(
            path.is_dir() && path.starts_with(&root),
            "子项目目录必须为项目内已有目录"
        );
        target.directory = path.strip_prefix(&root)?.to_string_lossy().into_owned();
        if target.directory.is_empty() {
            target.directory = ".".into();
        }
        crate::project_detection::validate_targets(std::slice::from_ref(&target))?;
        let _profiles = self.profiles_lock.lock().await;
        let previous = self.targets(state, project)?;
        let exists = previous
            .iter()
            .any(|value| value.directory == target.directory);
        ensure!(!create || !exists, "此目录的子项目已存在，请编辑现有配置");
        ensure!(create || exists, "子项目不存在，请重新读取配置");
        let _preferences = self.preferences_lock.lock().await;
        let mut preferences = self.preferences(&project.id)?;
        for value in previous {
            preferences.targets.insert(value.directory.clone(), value);
        }
        preferences.excluded.remove(&target.directory);
        preferences
            .names
            .insert(target.directory.clone(), name.trim().into());
        preferences
            .targets
            .insert(target.directory.clone(), target.clone());
        ensure!(
            preferences.targets.len() <= 128,
            "最多保存 128 个子项目配置"
        );
        let mut config = state.acceptance.environments.config(&project.id)?;
        config.build_targets.retain(|value| {
            value.directory != target.directory && !preferences.excluded.contains(&value.directory)
        });
        config.build_targets.push(target);
        config.build = config
            .build_targets
            .first()
            .map(|value| value.configuration.clone());
        config.validate()?;
        // Shared settings and profiles are serialized with acceptance saves.
        state
            .acceptance
            .environments
            .save_config(&project.id, config, false)?;
        atomic_json(
            &self.project_dir(&project.id).join("preferences.json"),
            &preferences,
        )
    }

    pub async fn rename_target(
        &self,
        state: &AppState,
        project: &ProjectSummary,
        directory: &str,
        name: &str,
    ) -> Result<()> {
        validate_directory(directory)?;
        validate_name(name)?;
        let _profiles = self.profiles_lock.lock().await;
        ensure!(
            self.targets(state, project)?
                .iter()
                .any(|target| target.directory == directory),
            "子项目不存在，请重新读取配置"
        );
        let _preferences = self.preferences_lock.lock().await;
        let mut preferences = self.preferences(&project.id)?;
        preferences
            .names
            .insert(directory.into(), name.trim().into());
        atomic_json(
            &self.project_dir(&project.id).join("preferences.json"),
            &preferences,
        )
    }

    pub async fn delete_target(
        &self,
        state: &AppState,
        project: &ProjectSummary,
        directory: &str,
    ) -> Result<()> {
        validate_directory(directory)?;
        let _profiles = self.profiles_lock.lock().await;
        let previous = self.targets(state, project)?;
        let shared = self.entry(&project.id, directory).await?;
        ensure!(
            previous.iter().any(|target| target.directory == directory) || shared.is_some(),
            "子项目不存在，请重新读取配置"
        );
        let mut entry = match &shared {
            Some(entry) => Some(entry.lock().await),
            None => None,
        };
        if let Some(entry) = &mut entry {
            self.refresh(state, entry).await?;
            ensure!(
                !entry.launching && !entry.record.phase.active() && !test_busy(&entry.record),
                "子项目仍在运行或状态尚未确认，请先停止后删除"
            );
        }
        let _preferences = self.preferences_lock.lock().await;
        let mut preferences = self.preferences(&project.id)?;
        for value in previous {
            preferences.targets.insert(value.directory.clone(), value);
        }
        preferences.targets.remove(directory);
        preferences.names.remove(directory);
        preferences.locations.remove(directory);
        preferences.excluded.insert(directory.into());
        ensure!(
            preferences.excluded.len() <= 128,
            "最多保留 128 个已删除子项目"
        );
        let mut config = state.acceptance.environments.config(&project.id)?;
        config
            .build_targets
            .retain(|value| value.directory != directory);
        config.build = config
            .build_targets
            .first()
            .map(|value| value.configuration.clone());
        state
            .acceptance
            .environments
            .save_config(&project.id, config, false)?;
        atomic_json(
            &self.project_dir(&project.id).join("preferences.json"),
            &preferences,
        )
    }
    async fn entry(&self, project: &str, directory: &str) -> Result<Option<Arc<Mutex<Entry>>>> {
        let id = format!("{}:{}", key(project), key(directory));
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(&id) {
            return Ok(Some(entry.clone()));
        }
        let bytes = match std::fs::read(self.record_path(project, directory)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let record: Runtime = serde_json::from_slice(&bytes)?;
        ensure!(
            record.project_id == project && record.directory == directory,
            "运行记录不匹配"
        );
        let entry = Arc::new(Mutex::new(Entry {
            record,
            child: None,
            launching: false,
        }));
        entries.insert(id, entry.clone());
        Ok(Some(entry))
    }
    async fn refresh(&self, state: &AppState, entry: &mut Entry) -> Result<()> {
        if entry
            .record
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at <= Utc::now())
        {
            entry.record.lease = None;
        }
        if !entry.record.phase.active() {
            return Ok(());
        }
        match entry.record.location {
            Location::Host => {
                if let Some(child) = &mut entry.child {
                    if let Some(status) = child.try_wait()? {
                        // The service may have spawned descendants; reap its whole group.
                        if let Some(pid) = entry.record.pid {
                            terminate_group(pid).await?;
                        }
                        entry.record.phase = if status.success() {
                            Phase::Stopped
                        } else {
                            Phase::Failed
                        };
                        entry.record.error =
                            (!status.success()).then(|| format!("启动命令已退出：{status}"));
                        entry.child = None;
                    }
                } else if entry.record.phase != Phase::Starting {
                    match host_alive(&entry.record) {
                        Some(true) => entry.record.phase = Phase::Running,
                        Some(false) => {
                            entry.record.phase = Phase::Stopped;
                        }
                        None => {
                            entry.record.phase = Phase::Unknown;
                            entry.record.error =
                                Some("Bridge 重启后无法确认原进程身份，请核对原服务".into());
                        }
                    }
                } else if !entry.launching {
                    entry.record.phase = Phase::Failed;
                    entry.record.error = Some("Bridge 重启中断了服务启动，请重新启动".into());
                }
            }
            Location::Docker => {
                if let Some(id) = &entry.record.acceptance_run_id {
                    match state.acceptance.snapshot(id).await {
                        Ok(run) => {
                            entry.record.phase = match run.phase {
                                RunPhase::Planning | RunPhase::Starting => Phase::Starting,
                                RunPhase::Running => Phase::Running,
                                RunPhase::Stopping => Phase::Stopping,
                                RunPhase::Completed => Phase::Stopped,
                                RunPhase::Failed => Phase::Failed,
                            };
                            entry.record.revision = Some(run.revision);
                            entry.record.error = run.error.or(run.automation_error);
                            if entry.record.phase == Phase::Failed
                                && !state.acceptance.failure_cleaned_up(id)
                            {
                                entry.record.phase = Phase::Unknown;
                                entry.record.error.get_or_insert_with(|| {
                                    "Docker 运行失败，尚未确认容器已清理，请停止后重试".into()
                                });
                            }
                            // A persisted run is not proof that its container still exists.
                            if entry.record.phase == Phase::Running {
                                let name = crate::acceptance::container_name(id);
                                match state
                                    .runtime_plugins
                                    .docker(
                                        &["inspect", "--format", "{{.State.Running}}", &name],
                                        10,
                                    )
                                    .await
                                {
                                    Ok(value) if value.trim() == "true" => {}
                                    Ok(_) => entry.record.phase = Phase::Stopped,
                                    Err(error) => {
                                        entry.record.phase = Phase::Unknown;
                                        entry.record.error = Some(error.to_string());
                                    }
                                }
                            }
                        }
                        Err(error) if !entry.launching => {
                            entry.record.phase = Phase::Unknown;
                            entry.record.error = Some(error.to_string());
                        }
                        Err(_) => {}
                    }
                }
            }
        }
        if !entry.record.phase.active() && !test_busy(&entry.record) {
            entry.record.lease = None;
        }
        entry.record.updated_at = Utc::now();
        self.persist(entry)
    }
    pub async fn overview(&self, state: &AppState, project: &ProjectSummary) -> Result<Overview> {
        let targets = self.targets(state, project)?;
        let preferences = self.preferences(&project.id)?;
        let mut directories: Vec<String> = targets
            .iter()
            .map(|target| target.directory.clone())
            .collect();
        // Keep removed targets visible while their services are still running.
        if let Ok(files) = std::fs::read_dir(self.project_dir(&project.id)) {
            for file in files.flatten() {
                if file.path().extension().is_some_and(|ext| ext == "json")
                    && file.file_name() != "preferences.json"
                {
                    let record: Runtime = serde_json::from_slice(&std::fs::read(file.path())?)?;
                    if record.project_id == project.id
                        && !preferences.excluded.contains(&record.directory)
                        && !directories.contains(&record.directory)
                    {
                        directories.push(record.directory);
                    }
                }
            }
        }
        let mut runtimes = Vec::new();
        for directory in directories {
            if let Some(entry) = self.entry(&project.id, &directory).await? {
                let mut entry = entry.lock().await;
                self.refresh(state, &mut entry).await?;
                runtimes.push(entry.record.clone());
            }
        }
        Ok(Overview {
            preferences,
            targets,
            runtimes,
        })
    }

    pub async fn start(
        self: &Arc<Self>,
        state: Arc<AppState>,
        project: ProjectSummary,
        directory: String,
        location: Option<Location>,
        client: String,
    ) -> Result<Runtime> {
        let profiles = self.profiles_lock.lock().await;
        validate_directory(&directory)?;
        let target = self
            .targets(&state, &project)?
            .into_iter()
            .find(|target| target.directory == directory)
            .context("请先配置此子项目")?;
        let command = target.configuration.start_command.trim().to_owned();
        ensure!(!command.is_empty(), "请先为子项目配置启动命令");
        target.configuration.validate()?;
        // A manual run freezes its own selected profile, independently of the
        // subset currently selected for an automatic acceptance run.
        let mut environment_config = state.acceptance.environments.config(&project.id)?;
        environment_config.build = Some(target.configuration.clone());
        environment_config.build_targets = vec![target];
        environment_config.build_reasoning = format!("用户手动启动子项目 {directory}");
        let root = Path::new(&project.root_path)
            .canonicalize()
            .context("项目目录不可用")?;
        let target_root = root
            .join(&directory)
            .canonicalize()
            .context("子项目目录不可用")?;
        ensure!(
            target_root.starts_with(&root),
            "子项目目录不能越出项目根目录"
        );
        let location = location.unwrap_or(
            self.preferences(&project.id)?
                .locations
                .get(&directory)
                .copied()
                .unwrap_or_default(),
        );
        let record = Runtime {
            id: Uuid::new_v4().to_string(),
            project_id: project.id.clone(),
            directory: directory.clone(),
            location,
            phase: Phase::Starting,
            command,
            working_directory: if location == Location::Host {
                root.to_string_lossy().into_owned()
            } else {
                "/workspace".into()
            },
            pid: None,
            host_identity: None,
            acceptance_run_id: (location == Location::Docker).then(|| Uuid::new_v4().to_string()),
            revision: None,
            lease: None,
            error: None,
            updated_at: Utc::now(),
        };
        let map_key = format!("{}:{}", key(&project.id), key(&directory));
        // Install the entry once. All lifecycle and lease changes then use its lock.
        let existing = self.entry(&project.id, &directory).await?;
        let entry = {
            let mut entries = self.entries.lock().await;
            entries
                .entry(map_key)
                .or_insert_with(|| {
                    existing.unwrap_or_else(|| {
                        Arc::new(Mutex::new(Entry {
                            record: record.clone(),
                            child: None,
                            launching: false,
                        }))
                    })
                })
                .clone()
        };
        let mut guard = entry.lock().await;
        if guard.record.id != record.id {
            self.refresh(&state, &mut guard).await?;
            ensure!(!guard.record.phase.active(), "此子项目已启动或状态尚未确认");
            ensure!(!test_busy(&guard.record), "上一条测试命令尚未确认结束");
            if guard.record.location == Location::Docker {
                if let Some(id) = &guard.record.acceptance_run_id {
                    stop_docker(&state, id).await?;
                }
            }
        }
        self.save_location(&project.id, &directory, location)
            .await?;
        guard.record = record.clone();
        if let Err(error) = self.persist(&guard) {
            guard.record.phase = Phase::Failed;
            guard.record.error = Some(format!("保存运行状态失败：{error:#}"));
            guard.launching = false;
            return Err(error);
        }
        guard.launching = true;
        drop(guard);
        drop(profiles);
        let store = self.clone();
        let worker_record = record.clone();
        tokio::spawn(async move {
            let result = store
                .launch(
                    &state,
                    &project,
                    &entry,
                    &worker_record,
                    &client,
                    environment_config,
                )
                .await;
            let mut entry = entry.lock().await;
            if entry.record.id == worker_record.id {
                entry.launching = false;
            }
            if let Err(error) = result {
                if entry.record.id == worker_record.id && entry.record.phase.active() {
                    entry.record.phase = Phase::Failed;
                    entry.record.error = Some(format!("{error:#}"));
                    let _ = store.persist(&entry);
                }
            }
        });
        Ok(record)
    }
    async fn launch(
        &self,
        state: &Arc<AppState>,
        project: &ProjectSummary,
        shared: &Mutex<Entry>,
        record: &Runtime,
        client: &str,
        environment_config: crate::project_environment::EnvironmentConfig,
    ) -> Result<()> {
        match record.location {
            Location::Host => {
                let mut entry = shared.lock().await;
                if entry.record.id != record.id || entry.record.phase != Phase::Starting {
                    return Ok(());
                }
                std::fs::create_dir_all(self.project_dir(&project.id))?;
                let log = std::fs::File::create(self.log_path(record))?;
                let mut command = shell(&record.command);
                command
                    .current_dir(&record.working_directory)
                    .stdin(Stdio::null())
                    .stdout(log.try_clone()?)
                    .stderr(log)
                    .kill_on_drop(true);
                #[cfg(unix)]
                command.process_group(0);
                let child = command.spawn().context("启动项目服务")?;
                let pid = child.id().context("服务未提供进程 ID")?;
                entry.record.pid = Some(pid);
                entry.record.host_identity = host_identity(pid);
                entry.record.phase = Phase::Running;
                entry.child = Some(child);
                if let Err(error) = self.persist(&entry) {
                    let _ = terminate_group(pid).await;
                    if let Some(child) = &mut entry.child {
                        let _ = child.kill().await;
                    }
                    return Err(error);
                }
                Ok(())
            }
            Location::Docker => {
                let id = record
                    .acceptance_run_id
                    .clone()
                    .context("容器运行 ID 缺失")?;
                let run = state
                    .acceptance
                    .create_with_environment(
                        CreateAcceptance {
                            id: id.clone(),
                            project_id: project.id.clone(),
                            session_id: None,
                            command: record.command.clone(),
                            size: "pc".into(),
                            automatic: false,
                            requirements: None,
                        },
                        Path::new(&project.root_path),
                        client,
                        vec![],
                        Some(state.clone()),
                        Some(environment_config),
                    )
                    .await?;
                let mut entry = shared.lock().await;
                if entry.record.id != record.id
                    || matches!(entry.record.phase, Phase::Stopping | Phase::Stopped)
                {
                    drop(entry);
                    state.acceptance.stop(&id, None).await?;
                } else {
                    entry.record.revision = Some(run.revision);
                    self.persist(&entry)?;
                }
                Ok(())
            }
        }
    }
    pub async fn stop(&self, state: &AppState, project: &str, directory: &str) -> Result<Runtime> {
        let shared = self
            .entry(project, directory)
            .await?
            .context("此子项目尚未启动")?;
        let mut entry = shared.lock().await;
        self.refresh(state, &mut entry).await?;
        ensure!(
            entry.record.lease.is_none(),
            "环境正被 Agent 占用，请等待释放或租约到期"
        );
        if !entry.record.phase.active() && entry.record.location == Location::Host {
            return Ok(entry.record.clone());
        }
        entry.record.phase = Phase::Stopping;
        self.persist(&entry)?;
        let result = match entry.record.location {
            Location::Host => {
                if let Some(pid) = entry.record.pid {
                    ensure!(
                        entry.child.is_some() || host_alive(&entry.record) == Some(true),
                        "无法确认原进程身份，拒绝终止可能已被复用的进程"
                    );
                    terminate_group(pid).await?;
                    if let Some(child) = &mut entry.child {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                    }
                }
                entry.child = None;
                Ok(())
            }
            Location::Docker => {
                if let Some(id) = &entry.record.acceptance_run_id {
                    // A start cancelled before source capture is stopped again by launch().
                    stop_docker(state, id).await
                } else {
                    Ok(())
                }
            }
        };
        match result {
            Ok(()) => {
                entry.record.phase = Phase::Stopped;
                entry.record.error = None;
            }
            Err(error) => {
                entry.record.phase = Phase::Unknown;
                entry.record.error = Some(error.to_string());
            }
        }
        entry.record.updated_at = Utc::now();
        self.persist(&entry)?;
        ensure!(
            entry.record.phase == Phase::Stopped,
            "{}",
            entry.record.error.as_deref().unwrap_or("停止失败")
        );
        Ok(entry.record.clone())
    }

    pub async fn claim(
        &self,
        state: &AppState,
        project: &str,
        directory: &str,
        session: &str,
        lease_id: Option<&str>,
    ) -> Result<Lease> {
        let entry = self
            .entry(project, directory)
            .await?
            .context("此子项目尚未启动")?;
        let mut entry = entry.lock().await;
        self.refresh(state, &mut entry).await?;
        ensure!(
            entry.record.phase == Phase::Running,
            "环境尚未运行或无法确认状态"
        );
        if let Some(lease) = &entry.record.lease {
            ensure!(
                lease.session_id == session && Some(lease.id.as_str()) == lease_id,
                "环境已被 Agent 占用"
            );
        } else {
            ensure!(lease_id.is_none(), "租约已失效，请重新申请");
        }
        let lease = Lease {
            id: lease_id
                .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            session_id: session.into(),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            busy_until: entry
                .record
                .lease
                .as_ref()
                .and_then(|lease| lease.busy_until),
        };
        entry.record.lease = Some(lease.clone());
        self.persist(&entry)?;
        Ok(lease)
    }
    pub async fn release(
        &self,
        project: &str,
        directory: &str,
        session: &str,
        lease_id: &str,
    ) -> Result<()> {
        let entry = self
            .entry(project, directory)
            .await?
            .context("此子项目尚未启动")?;
        let mut entry = entry.lock().await;
        require_lease(&entry.record, session, lease_id)?;
        ensure!(
            !entry
                .record
                .lease
                .as_ref()
                .unwrap()
                .busy_until
                .is_some_and(|until| until > Utc::now()),
            "上一条测试命令尚未确认结束，请稍后释放"
        );
        entry.record.lease = None;
        self.persist(&entry)
    }
    pub async fn execute(
        &self,
        state: &AppState,
        project: &str,
        directory: &str,
        session: &str,
        lease_id: &str,
        command: &str,
    ) -> Result<serde_json::Value> {
        ensure!(
            !command.trim().is_empty() && command.len() <= 8192 && !command.contains('\0'),
            "测试命令为空或过长"
        );
        let shared = self
            .entry(project, directory)
            .await?
            .context("此子项目尚未启动")?;
        let mut entry = shared.lock().await;
        self.refresh(state, &mut entry).await?;
        require_lease(&entry.record, session, lease_id)?;
        ensure!(entry.record.phase == Phase::Running, "环境未运行");
        ensure!(
            !entry
                .record
                .lease
                .as_ref()
                .unwrap()
                .busy_until
                .is_some_and(|until| until > Utc::now()),
            "上一条测试命令尚未确认结束"
        );
        entry.record.lease.as_mut().unwrap().expires_at = Utc::now() + chrono::Duration::minutes(5);
        entry.record.lease.as_mut().unwrap().busy_until =
            Some(Utc::now() + chrono::Duration::seconds(135));
        self.persist(&entry)?;
        let record = entry.record.clone();
        drop(entry);
        // Persist the in-flight deadline before unlocking. Status stays responsive,
        // while renew/release/start cannot transfer a running test to another Agent.
        let result = match record.location {
            Location::Host => {
                agent::execute_host(
                    &record.working_directory,
                    command,
                    &self.log_path(&record).with_extension("test.log"),
                )
                .await
            }
            Location::Docker => {
                let id = record
                    .acceptance_run_id
                    .as_deref()
                    .context("容器运行 ID 缺失")?;
                state
                    .runtime_plugins
                    .docker(
                        &[
                            "exec",
                            "--workdir",
                            "/workspace",
                            &crate::acceptance::container_name(id),
                            "timeout",
                            "--signal=KILL",
                            "120s",
                            "sh",
                            "-lc",
                            command,
                        ],
                        130,
                    )
                    .await
            }
        };
        let mut entry = shared.lock().await;
        ensure!(entry.record.id == record.id, "运行环境已被替换");
        let lease = entry.record.lease.as_mut().context("测试租约已失效")?;
        ensure!(
            lease.id == lease_id && lease.session_id == session,
            "测试租约已被替换"
        );
        lease.expires_at = Utc::now() + chrono::Duration::minutes(5);
        // Transport failure does not prove a container command has stopped.
        // Its in-container timeout bounds execution; keep its lease meanwhile.
        if result.is_ok() || entry.record.location == Location::Host {
            entry.record.lease.as_mut().unwrap().busy_until = None;
        }
        self.persist(&entry)?;
        Ok(match result {
            Ok(output) => serde_json::json!({"success":true,"output":output}),
            Err(error) => serde_json::json!({"success":false,"error":format!("{error:#}")}),
        })
    }
    pub async fn logs(&self, state: &AppState, project: &str, directory: &str) -> Result<String> {
        let entry = self
            .entry(project, directory)
            .await?
            .context("此子项目尚未启动")?;
        let entry = entry.lock().await;
        if entry.record.location == Location::Host {
            return tail(&self.log_path(&entry.record));
        }
        let run = state
            .acceptance
            .snapshot(
                entry
                    .record
                    .acceptance_run_id
                    .as_deref()
                    .context("容器运行 ID 缺失")?,
            )
            .await?;
        Ok(format!(
            "{}\n{}\n{}",
            run.automation_progress,
            run.error.unwrap_or_default(),
            run.environment.map(|env| env.log).unwrap_or_default()
        ))
    }
}

fn key(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
async fn stop_docker(state: &AppState, id: &str) -> Result<()> {
    // create() persists its run before it can launch a container. A failed or
    // cancelled request that never created that record has nothing to stop.
    match std::fs::metadata(
        state
            .runtime_plugins
            .root
            .join("runs")
            .join(id)
            .join("run.json"),
    ) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let run = state.acceptance.stop(id, None).await?;
    ensure!(
        run.phase == RunPhase::Completed || state.acceptance.failure_cleaned_up(id),
        "{}",
        run.error.as_deref().unwrap_or("Docker 资源清理失败")
    );
    Ok(())
}
fn test_busy(record: &Runtime) -> bool {
    record
        .lease
        .as_ref()
        .and_then(|lease| lease.busy_until)
        .is_some_and(|until| until > Utc::now())
}
fn validate_name(value: &str) -> Result<()> {
    ensure!(
        !value.trim().is_empty()
            && value.chars().count() <= 100
            && !value.chars().any(char::is_control),
        "子项目名称不能为空、包含控制字符或超过 100 个字符"
    );
    Ok(())
}

fn validate_directory(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 4096
            && !value.contains('\0')
            && Path::new(value).components().all(|part| matches!(
                part,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )),
        "子项目目录必须为项目内相对路径"
    );
    Ok(())
}
fn require_lease(record: &Runtime, session: &str, id: &str) -> Result<()> {
    ensure!(
        record.lease.as_ref().is_some_and(|lease| lease.id == id
            && lease.session_id == session
            && lease.expires_at > Utc::now()),
        "租约不属于当前 Agent 或已过期"
    );
    Ok(())
}
fn shell(script: &str) -> Command {
    #[cfg(unix)]
    {
        let mut command = Command::new("sh");
        command.args(["-lc", script]);
        command
    }
    #[cfg(not(unix))]
    {
        let mut command = Command::new("cmd");
        command.args(["/C", script]);
        command
    }
}
fn host_identity(pid: u32) -> Option<String> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    if fields.first().copied() == Some("Z") {
        return None;
    }
    Some(format!("{}:{}", boot.trim(), fields.get(19)?))
}
fn host_alive(record: &Runtime) -> Option<bool> {
    let pid = record.pid?;
    if cfg!(target_os = "linux") && !Path::new(&format!("/proc/{pid}")).exists() {
        return Some(false);
    }
    Some(host_identity(pid)? == *record.host_identity.as_ref()?)
}
async fn terminate_group(pid: u32) -> Result<()> {
    #[cfg(unix)]
    {
        let status = Command::new("kill")
            .args(["-KILL", "--", &format!("-{pid}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;
        if !status.success() {
            let alive = Command::new("kill")
                .args(["-0", "--", &format!("-{pid}")])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            ensure!(!alive.success(), "无法停止服务进程组");
        }
    }
    #[cfg(not(unix))]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .await?;
        ensure!(status.success(), "无法停止服务进程树");
    }
    Ok(())
}
pub(crate) fn tail(path: &Path) -> Result<String> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(e) => return Err(e.into()),
    };
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(24 * 1024)))?;
    let mut bytes = Vec::new();
    file.take(24 * 1024).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
#[path = "project_runtime_tests.rs"]
mod tests;
