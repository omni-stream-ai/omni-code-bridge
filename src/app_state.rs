use std::{
    collections::HashMap,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, RwLock, broadcast, mpsc},
    task::AbortHandle,
};
use uuid::Uuid;

use crate::{
    adapter::ProviderRegistry,
    bridge_settings::{BridgeSettings, BridgeSettingsStore},
    client_auth_store::ClientAuthStore,
    device_store::{load_device_registrations, save_device_registrations},
    models::{
        AUTO_PROVIDER_ID, AgentErrorEvent, AgentKind, ApiFormat, ApprovalChoice, ApprovalRequest,
        ApprovalRequestEvent, ApprovalResolvedEvent, ChatMessage, ClientAuthRecord,
        ClientAuthRequestInput, ClientAuthStatus, CreateProjectInput, CreateSessionInput,
        GitStatusDetail, InputMode, MessageDeltaEvent, MessageRole, ModelProviderConfig,
        ProjectGitStatus, ProjectSummary, PushDeviceRegistration, RegisterPushDeviceInput,
        ResolvedProviderConfig, SendMessageInput, SessionDetail, SessionEvent, SessionStatus,
        SessionStatusEvent, SessionSummary, SpeakerFilterSettings, TriggerClientMessageInput,
        TriggerClientMessageResult,
    },
    push::PushService,
    session_store::project_id_for_path,
    speech::SpeechService,
};

#[derive(Default)]
struct SessionRuntimeState {
    provider_session_ref: Option<String>,
    /// The codex model_provider name used when creating the current thread.
    /// Stored so we can resume with the same provider (codex binds threads to providers).
    codex_provider_name: Option<String>,
    /// The model used when creating the current Codex thread.
    /// Stored so model changes do not accidentally resume an incompatible thread.
    codex_model: Option<String>,
    /// The provider ID used when creating the current Claude session.
    claude_provider_id: Option<String>,
    /// The model used when creating the current Claude session.
    claude_model: Option<String>,
    pending_approval: Option<ApprovalRequest>,
    last_resolved_approval_request_id: Option<String>,
    approval_tx: Option<mpsc::UnboundedSender<ApprovalChoice>>,
    turn_abort: Option<AbortHandle>,
    turn_in_flight: bool,
    interrupted: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedSessionRuntimeState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_session_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    codex_provider_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    codex_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claude_provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claude_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_approval: Option<ApprovalRequest>,
    #[serde(default, skip_serializing_if = "is_false")]
    interrupted: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedSessionMetadata {
    #[serde(default)]
    sessions: HashMap<String, PersistedSessionMetadataEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedSessionMetadataEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl SessionRuntimeState {
    fn from_persisted(value: PersistedSessionRuntimeState) -> Self {
        let pending_approval = value.pending_approval.map(|mut request| {
            request.resolvable = false;
            request.allow_accept_for_session = false;
            request.reason = request.reason.or_else(|| {
                Some(
                    "审批请求已从持久化状态恢复，但原运行通道不可用，请重新发送消息继续。"
                        .to_string(),
                )
            });
            request
        });
        Self {
            provider_session_ref: value.provider_session_ref,
            codex_provider_name: value.codex_provider_name,
            codex_model: value.codex_model,
            claude_provider_id: value.claude_provider_id,
            claude_model: value.claude_model,
            pending_approval,
            last_resolved_approval_request_id: None,
            approval_tx: None,
            turn_abort: None,
            turn_in_flight: false,
            interrupted: value.interrupted,
        }
    }

    fn to_persisted(&self) -> PersistedSessionRuntimeState {
        PersistedSessionRuntimeState {
            provider_session_ref: self.provider_session_ref.clone(),
            codex_provider_name: self.codex_provider_name.clone(),
            codex_model: self.codex_model.clone(),
            claude_provider_id: self.claude_provider_id.clone(),
            claude_model: self.claude_model.clone(),
            pending_approval: self.pending_approval.clone(),
            interrupted: self.interrupted,
        }
    }
}

#[derive(Clone)]
struct AggregatedListCache {
    checked_at: Instant,
    git_fingerprint: u64,
    projects: Vec<ProjectSummary>,
    sessions: Vec<SessionSummary>,
    project_sessions: HashMap<String, Vec<SessionSummary>>,
    projects_by_id: HashMap<String, ProjectSummary>,
    sessions_by_id: HashMap<String, SessionSummary>,
}

#[derive(Debug, Clone)]
pub struct TtsStreamSession {
    pub token: String,
    pub model_id: String,
    pub input: String,
    pub voice: Option<String>,
    pub speed: Option<f32>,
    pub response_format: Option<String>,
    pub content_type: String,
    pub expires_at: Instant,
}

pub struct AppState {
    projects: RwLock<HashMap<String, ProjectSummary>>,
    sessions: RwLock<HashMap<String, SessionSummary>>,
    session_title_overrides: RwLock<HashMap<String, String>>,
    messages: RwLock<HashMap<String, Vec<ChatMessage>>>,
    devices: RwLock<HashMap<String, PushDeviceRegistration>>,
    list_cache: Mutex<Option<AggregatedListCache>>,
    runtime: Mutex<HashMap<String, SessionRuntimeState>>,
    event_tx: broadcast::Sender<SessionEvent>,
    providers: ProviderRegistry,
    settings: BridgeSettingsStore,
    speech: Arc<SpeechService>,
    client_auth: ClientAuthStore,
    push: PushService,
    tts_stream_sessions: Mutex<HashMap<String, TtsStreamSession>>,
    runtime_store_path: PathBuf,
    session_metadata_store_path: PathBuf,
}

impl AppState {
    const LIST_CACHE_TTL: Duration = Duration::from_secs(5);

    pub async fn new() -> Self {
        let settings_path = crate::bridge_settings::settings_path();
        let runtime_store_path = runtime_store_path();
        let session_metadata_store_path = session_metadata_store_path();
        Self::new_with_paths(
            settings_path,
            runtime_store_path,
            session_metadata_store_path,
        )
        .await
    }

    async fn new_with_paths(
        settings_path: PathBuf,
        runtime_store_path: PathBuf,
        session_metadata_store_path: PathBuf,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        let persisted_runtime = load_persisted_runtime(&runtime_store_path).await;
        let persisted_metadata =
            load_persisted_session_metadata(&session_metadata_store_path).await;
        Self {
            projects: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            session_title_overrides: RwLock::new(
                persisted_metadata
                    .sessions
                    .into_iter()
                    .filter_map(|(session_id, entry)| {
                        entry
                            .title
                            .map(|title| title.trim().to_string())
                            .filter(|title| !title.is_empty())
                            .map(|title| (session_id, title))
                    })
                    .collect(),
            ),
            messages: RwLock::new(HashMap::new()),
            devices: RwLock::new(load_device_registrations()),
            list_cache: Mutex::new(None),
            runtime: Mutex::new(
                persisted_runtime
                    .into_iter()
                    .map(|(session_id, value)| {
                        (session_id, SessionRuntimeState::from_persisted(value))
                    })
                    .collect(),
            ),
            event_tx,
            providers: ProviderRegistry::new(),
            settings: BridgeSettingsStore::load_from_path(settings_path).await,
            speech: Arc::new(SpeechService::load().await),
            client_auth: ClientAuthStore::load().await,
            push: PushService::new(),
            tts_stream_sessions: Mutex::new(HashMap::new()),
            runtime_store_path,
            session_metadata_store_path,
        }
    }

    pub async fn create_tts_stream_session(
        &self,
        model_id: String,
        input: String,
        voice: Option<String>,
        speed: Option<f32>,
        response_format: Option<String>,
        content_type: String,
    ) -> TtsStreamSession {
        const TTS_STREAM_TTL: Duration = Duration::from_secs(120);
        let mut sessions = self.tts_stream_sessions.lock().await;
        let now = Instant::now();
        sessions.retain(|_, session| session.expires_at > now);
        let token = Uuid::new_v4().to_string().replace('-', "");
        let session = TtsStreamSession {
            token: token.clone(),
            model_id,
            input,
            voice,
            speed,
            response_format,
            content_type,
            expires_at: now + TTS_STREAM_TTL,
        };
        sessions.insert(token, session.clone());
        session
    }

    pub async fn get_tts_stream_session(&self, token: &str) -> Option<TtsStreamSession> {
        let mut sessions = self.tts_stream_sessions.lock().await;
        let now = Instant::now();
        sessions.retain(|_, session| session.expires_at > now);
        sessions.get(token).cloned()
    }

    pub async fn is_runtime_client_id_allowed(&self, client_id: &str) -> bool {
        self.client_auth.has_approved_client_id(client_id).await
    }

    pub async fn client_token_matches(&self, client_id: &str, token: &str) -> bool {
        self.client_auth.token_matches(client_id, token).await
    }

    pub async fn request_client_auth(
        &self,
        input: ClientAuthRequestInput,
    ) -> Result<ClientAuthRecord, String> {
        let client_id = input.client_id.trim();
        if client_id.is_empty() {
            return Err("client_id is required".to_string());
        }

        if let Some(record) = self.client_auth.find_approved_by_client_id(client_id).await {
            return Ok(record);
        }

        let now = Utc::now();
        let record = ClientAuthRecord {
            request_id: Uuid::new_v4().to_string(),
            client_id: client_id.to_string(),
            device_name: input
                .device_name
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            status: ClientAuthStatus::Pending,
            token: None,
            created_at: now,
            updated_at: now,
        };
        self.client_auth
            .upsert(record)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn get_client_auth_request(&self, request_id: &str) -> Option<ClientAuthRecord> {
        self.client_auth.get(request_id).await
    }

    pub async fn bridge_settings(&self) -> BridgeSettings {
        self.settings.get().await
    }

    pub async fn update_bridge_settings(
        &self,
        input: crate::bridge_settings::BridgeSettingsInput,
    ) -> Result<BridgeSettings, String> {
        // Validate model_providers before applying
        if let Some(ref providers) = input.model_providers {
            crate::bridge_settings::validate_model_providers(providers)?;
        }

        self.settings
            .update(|settings| {
                settings.ai_approval = input.ai_approval;
                if let Some(model_providers) = input.model_providers {
                    settings.model_providers = model_providers;
                }
                if let Some(speech_profiles) = input.speech_profiles {
                    settings.speech_profiles = speech_profiles;
                }
                if let Some(speech_voices) = input.speech_voices {
                    settings.speech_voices = speech_voices;
                }
                if let Some(speaker_filter) = input.speaker_filter {
                    settings.speaker_filter =
                        crate::speaker::normalize_speaker_filter(speaker_filter);
                }
            })
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn update_speaker_filter_settings(
        &self,
        input: SpeakerFilterSettings,
    ) -> Result<BridgeSettings, String> {
        self.settings
            .update(|settings| {
                settings.speaker_filter = crate::speaker::normalize_speaker_filter(input);
            })
            .await
            .map_err(|error| error.to_string())
    }

    pub fn speech(&self) -> Arc<SpeechService> {
        Arc::clone(&self.speech)
    }

    pub fn settings_store(&self) -> &BridgeSettingsStore {
        &self.settings
    }

    pub async fn list_projects(&self) -> Vec<ProjectSummary> {
        self.ensure_list_cache().await.projects
    }

    pub async fn list_sessions(&self) -> Vec<SessionSummary> {
        self.with_runtime_approvals(
            self.overlay_persisted_session_titles(self.ensure_list_cache().await.sessions)
                .await,
        )
        .await
    }

    pub async fn list_project_sessions(&self, project_id: &str) -> Option<Vec<SessionSummary>> {
        let cache = self.ensure_list_cache().await;
        let mut items = cache
            .project_sessions
            .get(project_id)
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            let project_exists = cache.projects_by_id.contains_key(project_id);
            if !project_exists {
                return None;
            }
        }
        items.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        let items = self.overlay_persisted_session_titles(items).await;
        Some(self.with_runtime_approvals(items).await)
    }

    async fn with_runtime_approvals(
        &self,
        mut sessions: Vec<SessionSummary>,
    ) -> Vec<SessionSummary> {
        let runtime = self.runtime.lock().await;
        for session in &mut sessions {
            Self::apply_runtime_overlay(session, runtime.get(&session.id));
        }
        sessions
    }

    async fn with_runtime_approval(&self, mut session: SessionSummary) -> SessionSummary {
        session = self.overlay_persisted_session_title(session).await;
        let runtime = self.runtime.lock().await;
        let session_id = session.id.clone();
        Self::apply_runtime_overlay(&mut session, runtime.get(&session_id));
        session
    }

    async fn overlay_persisted_session_titles(
        &self,
        sessions: Vec<SessionSummary>,
    ) -> Vec<SessionSummary> {
        let mut updated = Vec::with_capacity(sessions.len());
        for session in sessions {
            updated.push(self.overlay_persisted_session_title(session).await);
        }
        updated
    }

    async fn overlay_persisted_session_title(&self, mut session: SessionSummary) -> SessionSummary {
        if let Some(title) = self.persisted_session_title(&session.id).await {
            session.title = title;
        }
        session
    }

    async fn persisted_session_title(&self, session_id: &str) -> Option<String> {
        self.session_title_overrides
            .read()
            .await
            .get(session_id)
            .cloned()
    }

    fn apply_runtime_overlay(session: &mut SessionSummary, runtime: Option<&SessionRuntimeState>) {
        session.pending_approval = runtime.and_then(|entry| entry.pending_approval.clone());
        if session.pending_approval.is_some() {
            session.status = SessionStatus::AwaitingApproval;
        } else if runtime.map(Self::is_recoverable_runtime).unwrap_or(false)
            || (matches!(session.status, SessionStatus::Running)
                && runtime.map(Self::is_detached_running).unwrap_or(false))
        {
            session.status = SessionStatus::Interrupted;
        }
    }

    fn is_recoverable_runtime(entry: &SessionRuntimeState) -> bool {
        entry.interrupted && !entry.turn_in_flight
    }

    fn is_detached_running(entry: &SessionRuntimeState) -> bool {
        !entry.turn_in_flight
            && entry.turn_abort.is_none()
            && entry.approval_tx.is_none()
            && entry.pending_approval.is_none()
            && entry.provider_session_ref.is_some()
    }

    pub async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        if let Some(messages) = self.messages.read().await.get(session_id).cloned() {
            return Some(messages);
        }
        let session = self.find_session(session_id).await?;
        let provider = self.provider_for_agent(session.agent)?;
        provider.list_messages(session_id).await
    }

    pub async fn get_session(&self, session_id: &str) -> Option<SessionDetail> {
        let session = self.find_session(session_id).await?;
        let session = self.with_runtime_approval(session).await;
        let git_status = self
            .find_project(&session.project_id)
            .await
            .and_then(|project| {
                git_status_for_project(&project.root_path).map(|state| state.detail)
            });
        Some(SessionDetail {
            session,
            git_status,
        })
    }

    pub async fn create_project(&self, input: CreateProjectInput) -> ProjectSummary {
        let project = ProjectSummary {
            id: project_id_for_path(&input.root_path),
            name: input.name,
            root_path: input.root_path,
            updated_at: Utc::now(),
            session_count: 0,
            last_session_preview: Some("项目已创建".to_string()),
            git_branch: None,
            git_status: None,
        };

        self.projects
            .write()
            .await
            .insert(project.id.clone(), project.clone());
        self.invalidate_list_cache().await;

        project
    }

    pub async fn create_session(
        &self,
        input: CreateSessionInput,
    ) -> Result<SessionSummary, String> {
        let project = self
            .find_project(&input.project_id)
            .await
            .ok_or_else(|| format!("unknown project: {}", input.project_id))?;

        let session = SessionSummary {
            id: Uuid::new_v4().to_string(),
            project_id: project.id,
            title: input
                .title
                .unwrap_or_else(|| format!("{} 会话", project.name)),
            agent: input.agent,
            brief_reply_mode: input.brief_reply_mode,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: Some("新会话已创建".to_string()),
            pending_approval: None,
            provider_id: input.provider_id,
            reasoning_effort: input.reasoning_effort,
        };

        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        self.messages
            .write()
            .await
            .insert(session.id.clone(), Vec::new());
        self.runtime.lock().await.insert(
            session.id.clone(),
            SessionRuntimeState {
                provider_session_ref: None,
                codex_provider_name: None,
                codex_model: None,
                claude_provider_id: None,
                claude_model: None,
                pending_approval: None,
                last_resolved_approval_request_id: None,
                approval_tx: None,
                turn_abort: None,
                turn_in_flight: false,
                interrupted: false,
            },
        );
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&session.project_id).await;

        let _ = self
            .event_tx
            .send(SessionEvent::SessionSnapshot(session.clone()));

        Ok(session)
    }

    pub async fn trigger_client_message(
        &self,
        input: TriggerClientMessageInput,
    ) -> Result<TriggerClientMessageResult, String> {
        let content = input.content.trim().to_string();
        if content.is_empty() {
            return Err("content cannot be empty".to_string());
        }

        let now = Utc::now();
        let project_id = input
            .project_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "client-inbox".to_string());
        let project = {
            let mut projects = self.projects.write().await;
            let entry = projects
                .entry(project_id.clone())
                .or_insert_with(|| ProjectSummary {
                    id: project_id.clone(),
                    name: input
                        .project_name
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("客户端消息")
                        .to_string(),
                    root_path: format!("omni-code://{project_id}"),
                    updated_at: now,
                    session_count: 0,
                    last_session_preview: None,
                    git_branch: None,
                    git_status: None,
                });
            if let Some(name) = input
                .project_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                entry.name = name.to_string();
            }
            entry.updated_at = now;
            entry.last_session_preview = Some(content.clone());
            entry.clone()
        };

        let title = input
            .title
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| message_title(&content));
        let session = SessionSummary {
            id: Uuid::new_v4().to_string(),
            project_id: project.id.clone(),
            title,
            agent: AgentKind::Custom,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: now,
            unread_count: 1,
            last_message_preview: Some(content.clone()),
            pending_approval: None,
            provider_id: None,
            reasoning_effort: None,
        };
        let message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: content.clone(),
            created_at: now,
        };

        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        self.messages
            .write()
            .await
            .insert(session.id.clone(), vec![message.clone()]);
        self.runtime
            .lock()
            .await
            .insert(session.id.clone(), SessionRuntimeState::default());
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&session.project_id).await;

        let _ = self
            .event_tx
            .send(SessionEvent::SessionSnapshot(session.clone()));
        let _ = self
            .event_tx
            .send(SessionEvent::MessageCreated(message.clone()));

        let devices = self
            .devices
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.push
            .send_assistant_reply(session.clone(), content, devices)
            .await;

        let project = self
            .find_project(&session.project_id)
            .await
            .unwrap_or(project);
        Ok(TriggerClientMessageResult {
            project,
            session,
            message,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.event_tx.subscribe()
    }

    /// Resolve provider configuration for a message based on priority:
    /// 1. Message-level provider_id
    /// 2. Session-level provider_id
    /// 3. Project-level provider (highest priority enabled provider matching agent format)
    /// 4. Global provider (highest priority enabled provider matching agent format)
    pub async fn resolve_provider_config(
        &self,
        session: &SessionSummary,
        message_provider_id: &Option<String>,
    ) -> Option<ResolvedProviderConfig> {
        let settings = self.bridge_settings().await;
        let agent = session.agent;
        let requested_provider_id = message_provider_id
            .clone()
            .or_else(|| session.provider_id.clone());

        // Helper: check if a format is compatible with the agent
        let is_format_compatible = |format: &ApiFormat| match agent {
            AgentKind::ClaudeCode => *format == ApiFormat::AnthropicMessages,
            AgentKind::Codex => *format == ApiFormat::Codex,
            AgentKind::OpenCode => {
                *format == ApiFormat::OpenAiCompatible
                    || *format == ApiFormat::AnthropicMessages
                    || *format == ApiFormat::Codex
            }
            AgentKind::Custom => *format == ApiFormat::OpenAiCompatible,
        };

        // Helper: find best provider from a list
        let find_best_provider =
            |providers: &[ModelProviderConfig]| -> Option<ResolvedProviderConfig> {
                providers
                    .iter()
                    .filter(|p| p.enabled)
                    .filter(|p| is_format_compatible(&p.format))
                    .min_by_key(|p| p.priority)
                    .map(|p| ResolvedProviderConfig {
                        base_url: p.base_url.clone(),
                        api_key: p.api_key.clone(),
                        model: p.model.clone(),
                        format: p.format,
                        provider_id: Some(p.id.clone()),
                        opencode_provider_name: match p.format {
                            ApiFormat::AnthropicMessages => {
                                Some("omni-bridge-anthropic".to_string())
                            }
                            ApiFormat::Codex => Some("omni-bridge-codex".to_string()),
                            _ => Some("omni-bridge".to_string()),
                        },
                    })
            };

        // Message/session provider handling:
        // - missing provider_id => do not resolve any bridge provider
        // - "AUTO" => use priority-based auto-selection
        // - explicit id => direct lookup, no fallback
        if let Some(provider_id) = requested_provider_id {
            if provider_id != AUTO_PROVIDER_ID {
                eprintln!("[provider] looking up explicit provider_id={provider_id}");
                if let Some(config) = self
                    .find_provider_by_id(&provider_id, agent, &settings)
                    .await
                {
                    eprintln!(
                        "[provider] resolved explicit provider: base_url={} model={:?} format={:?}",
                        config.base_url, config.model, config.format
                    );
                    return Some(config);
                }
                eprintln!("[provider] error: provider_id={provider_id} not found in settings");
                return None;
            }
            eprintln!("[provider] explicit AUTO provider requested");
        } else {
            eprintln!(
                "[provider] no provider_id requested for session={}; skipping bridge provider resolution",
                session.id
            );
            return None;
        }

        // 3. Project-level providers
        if let Some(project_providers) = self.load_project_providers(&session.project_id).await {
            if let Some(config) = find_best_provider(&project_providers) {
                eprintln!(
                    "[provider] using project-level provider for session={}: base_url={} format={:?}",
                    session.id, config.base_url, config.format
                );
                return Some(config);
            }
        }

        // 4. Global providers (sorted by priority)
        let config = find_best_provider(&settings.model_providers);
        if let Some(ref c) = config {
            eprintln!(
                "[provider] using global provider: base_url={} model={:?} format={:?}",
                c.base_url, c.model, c.format
            );
        }
        config
    }

    /// Load project-level provider configuration from .omni-code/providers.json
    async fn load_project_providers(&self, project_id: &str) -> Option<Vec<ModelProviderConfig>> {
        let project_root = self.find_project(project_id).await?.root_path;
        let config_path = std::path::PathBuf::from(&project_root)
            .join(".omni-code")
            .join("providers.json");

        if !config_path.exists() {
            return None;
        }

        match tokio::fs::read_to_string(&config_path).await {
            Ok(body) => match serde_json::from_str::<Vec<ModelProviderConfig>>(&body) {
                Ok(providers) => {
                    eprintln!(
                        "[provider] loaded {} project-level providers from {}",
                        providers.len(),
                        config_path.display()
                    );
                    Some(providers)
                }
                Err(error) => {
                    eprintln!(
                        "[provider] warning: failed to parse {}: {}",
                        config_path.display(),
                        error
                    );
                    None
                }
            },
            Err(error) => {
                eprintln!(
                    "[provider] warning: failed to read {}: {}",
                    config_path.display(),
                    error
                );
                None
            }
        }
    }

    /// Find a provider by ID and validate it's compatible with the agent
    async fn find_provider_by_id(
        &self,
        provider_id: &str,
        agent: AgentKind,
        settings: &BridgeSettings,
    ) -> Option<ResolvedProviderConfig> {
        let provider = settings
            .model_providers
            .iter()
            .find(|p| p.id == provider_id)?;

        // Validate format compatibility
        let is_compatible = match agent {
            AgentKind::ClaudeCode => provider.format == ApiFormat::AnthropicMessages,
            AgentKind::Codex => provider.format == ApiFormat::Codex,
            AgentKind::OpenCode => {
                provider.format == ApiFormat::OpenAiCompatible
                    || provider.format == ApiFormat::AnthropicMessages
                    || provider.format == ApiFormat::Codex
            }
            AgentKind::Custom => provider.format == ApiFormat::OpenAiCompatible,
        };

        if !is_compatible {
            eprintln!(
                "[provider] warning: provider {} format {:?} is not compatible with agent {:?}",
                provider_id, provider.format, agent
            );
            // Allow it anyway but log warning
        }

        Some(ResolvedProviderConfig {
            base_url: provider.base_url.clone(),
            api_key: provider.api_key.clone(),
            model: provider.model.clone(),
            format: provider.format,
            provider_id: Some(provider_id.to_string()),
            opencode_provider_name: match provider.format {
                ApiFormat::AnthropicMessages => Some("omni-bridge-anthropic".to_string()),
                ApiFormat::Codex => Some("omni-bridge-codex".to_string()),
                _ => Some("omni-bridge".to_string()),
            },
        })
    }

    pub async fn send_message(
        self: &Arc<Self>,
        session_id: &str,
        input: SendMessageInput,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        let mut already_processing = false;
        self.update_runtime_state(session_id, |entry| {
            if entry.turn_in_flight {
                already_processing = true;
                return;
            }
            entry.turn_in_flight = true;
            entry.interrupted = false;
        })
        .await;
        if already_processing {
            return Err("session is already processing a message".to_string());
        }

        let session_snapshot = self
            .find_session(session_id)
            .await
            .ok_or_else(|| format!("unknown session: {session_id}"));
        let session_snapshot = match session_snapshot {
            Ok(session) => session,
            Err(error) => {
                self.finish_turn(session_id).await;
                return Err(error);
            }
        };

        if matches!(session_snapshot.agent, AgentKind::Custom) {
            let user_message = ChatMessage {
                id: Uuid::new_v4().to_string(),
                session_id: session_id.to_string(),
                role: MessageRole::User,
                content: decorate_user_content(&input),
                created_at: Utc::now(),
            };
            self.ensure_inbox_seeded(session_id, session_snapshot.agent)
                .await;
            self.push_message(user_message.clone()).await;
            self.patch_session(
                session_id,
                SessionStatus::Idle,
                Some(user_message.content.clone()),
            )
            .await;
            self.finish_turn(session_id).await;
            let _ = self
                .event_tx
                .send(SessionEvent::MessageCreated(user_message.clone()));
            let _ = self
                .event_tx
                .send(SessionEvent::SessionStatus(SessionStatusEvent {
                    session_id: session_id.to_string(),
                    status: SessionStatus::Idle,
                    error_message: None,
                }));
            return Ok((user_message.clone(), user_message));
        }

        let user_message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::User,
            content: decorate_user_content(&input),
            created_at: Utc::now(),
        };
        let system_prompt = input
            .system_prompt
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        eprintln!(
            "[messages] send session={session_id} input_mode={:?} system_prompt_present={} system_prompt_len={} message_provider_id={:?} session_provider_id={:?} reasoning_effort={:?}",
            input.input_mode,
            system_prompt.is_some(),
            system_prompt.as_ref().map(|value| value.len()).unwrap_or(0),
            input.provider_id,
            session_snapshot.provider_id,
            input.reasoning_effort.or(session_snapshot.reasoning_effort),
        );
        let pending_reply = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: Utc::now(),
        };

        self.ensure_inbox_seeded(session_id, session_snapshot.agent)
            .await;
        self.push_message(user_message.clone()).await;
        self.push_message(pending_reply.clone()).await;
        self.patch_session(
            session_id,
            SessionStatus::Running,
            Some(user_message.content.clone()),
        )
        .await;
        self.clear_pending_approval(session_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::MessageCreated(user_message.clone()));
        let _ = self
            .event_tx
            .send(SessionEvent::MessageCreated(pending_reply.clone()));
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Running,
                error_message: None,
            }));

        // Resolve provider configuration
        let provider_config = self
            .resolve_provider_config(&session_snapshot, &input.provider_id)
            .await;
        if let Some(ref config) = provider_config {
            eprintln!(
                "[provider] resolved provider for session={session_id}: base_url={} model={:?} format={:?}",
                config.base_url, config.model, config.format
            );
        } else {
            self.set_codex_provider_name(session_id, None).await;
            self.set_codex_model(session_id, None).await;
            self.set_claude_provider_id(session_id, None).await;
            self.set_claude_model(session_id, None).await;
        }

        let state = Arc::clone(self);
        let reasoning_effort = input.reasoning_effort.or(session_snapshot.reasoning_effort);
        let provider = self
            .provider_for_agent(session_snapshot.agent)
            .ok_or_else(|| format!("unsupported agent: {:?}", session_snapshot.agent))?;
        let session_id = session_id.to_string();
        let session_id_for_task = session_id.clone();
        let user_message_for_task = user_message.clone();
        let pending_reply_for_task = pending_reply.clone();
        let handle = tokio::spawn(async move {
            let result = provider
                .run_session(
                    state.clone(),
                    session_snapshot,
                    user_message_for_task,
                    system_prompt,
                    pending_reply_for_task,
                    provider_config,
                    reasoning_effort,
                )
                .await;
            if let Err(error) = result {
                eprintln!(
                    "[session] session={} failed: {:#}",
                    session_id_for_task, error
                );
                state
                    .fail_session(&session_id_for_task, error.to_string())
                    .await;
            }
            state.clear_approval_sender(&session_id_for_task).await;
            state.finish_turn(&session_id_for_task).await;
        });
        let abort_handle = handle.abort_handle();
        self.set_turn_abort(&session_id, Some(abort_handle)).await;

        Ok((user_message, pending_reply))
    }

    pub async fn cancel_turn(&self, session_id: &str) -> Result<bool, String> {
        let session_snapshot = self
            .find_session(session_id)
            .await
            .ok_or_else(|| format!("unknown session: {session_id}"))?;
        let stale_running_without_runtime = matches!(
            session_snapshot.status,
            SessionStatus::Running | SessionStatus::AwaitingApproval
        );
        let (abort_handle, should_interrupt) = {
            let mut runtime = self.runtime.lock().await;
            let runtime_existed = runtime.contains_key(session_id);
            let entry = runtime
                .entry(session_id.to_string())
                .or_insert_with(SessionRuntimeState::default);
            let abort_handle = entry.turn_abort.take();
            let had_active_turn = entry.turn_in_flight || abort_handle.is_some();
            let had_detached_running = Self::is_detached_running(entry);
            let should_interrupt = had_active_turn
                || had_detached_running
                || (!runtime_existed && stale_running_without_runtime);
            entry.turn_in_flight = false;
            entry.approval_tx = None;
            entry.pending_approval = None;
            if should_interrupt {
                entry.interrupted = true;
            }
            let snapshot = runtime
                .iter()
                .map(|(session_id, state)| (session_id.clone(), state.to_persisted()))
                .collect::<HashMap<_, _>>();
            drop(runtime);
            if let Err(error) = write_persisted_runtime(&self.runtime_store_path, &snapshot).await {
                eprintln!(
                    "failed to persist session runtime metadata at {}: {error}",
                    self.runtime_store_path.display()
                );
            }
            (abort_handle, should_interrupt)
        };

        if let Some(abort_handle) = abort_handle {
            abort_handle.abort();
        }

        if should_interrupt {
            let preview = self
                .latest_assistant_preview(session_id)
                .await
                .or(Some("本次运行已中断，可继续发送消息恢复。".to_string()));
            self.patch_session(session_id, SessionStatus::Interrupted, preview)
                .await;
            let _ = self
                .event_tx
                .send(SessionEvent::SessionStatus(SessionStatusEvent {
                    session_id: session_id.to_string(),
                    status: SessionStatus::Interrupted,
                    error_message: None,
                }));
        }
        Ok(should_interrupt)
    }

    pub async fn submit_approval(
        &self,
        session_id: &str,
        request_id: &str,
        choice: ApprovalChoice,
    ) -> Result<(), String> {
        eprintln!("[approval] submit session={session_id} request={request_id} choice={choice:?}");
        let sender = {
            let runtime = self.runtime.lock().await;
            let session = runtime
                .get(session_id)
                .ok_or_else(|| format!("unknown session: {session_id}"))?;
            let already_resolved =
                session.last_resolved_approval_request_id.as_deref() == Some(request_id);
            let pending = match session.pending_approval.as_ref() {
                Some(pending) => pending,
                None if already_resolved => return Ok(()),
                None => return Err("no pending approval for this session".to_string()),
            };
            if pending.request_id != request_id {
                if already_resolved {
                    return Ok(());
                }
                return Err(format!("unknown approval request: {request_id}"));
            }
            if !pending.resolvable {
                return Err("this approval request cannot be resolved from client".to_string());
            }
            session.approval_tx.clone().ok_or_else(|| {
                if already_resolved {
                    "approval already resolved".to_string()
                } else {
                    "approval channel is not available".to_string()
                }
            })?
        };

        if sender.send(choice).is_err() {
            let runtime = self.runtime.lock().await;
            let session = runtime
                .get(session_id)
                .ok_or_else(|| format!("unknown session: {session_id}"))?;
            let already_resolved =
                session.last_resolved_approval_request_id.as_deref() == Some(request_id);
            let still_pending = session
                .pending_approval
                .as_ref()
                .map(|pending| pending.request_id.as_str() == request_id)
                .unwrap_or(false);
            if !already_resolved && still_pending {
                return Err("failed to forward approval to provider process".to_string());
            }
        }

        Ok(())
    }

    pub async fn summarize_reply(
        self: &Arc<Self>,
        session_id: &str,
        content: String,
    ) -> Result<String, String> {
        let session = self
            .find_session(session_id)
            .await
            .ok_or_else(|| format!("unknown session: {session_id}"))?;
        let provider = self
            .provider_for_agent(session.agent)
            .ok_or_else(|| format!("unsupported agent: {:?}", session.agent))?;

        provider
            .summarize_reply(Arc::clone(self), session, content)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn register_push_device(
        &self,
        client_id: &str,
        input: RegisterPushDeviceInput,
    ) -> PushDeviceRegistration {
        let device = PushDeviceRegistration {
            client_id: client_id.to_string(),
            platform: input.platform,
            manufacturer: input.manufacturer,
            model: input.model,
            app_version: input.app_version,
            fcm_token: input
                .fcm_token
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            mi_push_reg_id: input
                .mi_push_reg_id
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            updated_at: Utc::now(),
        };

        let snapshot = {
            let mut devices = self.devices.write().await;
            devices.insert(client_id.to_string(), device.clone());
            devices.clone()
        };
        save_device_registrations(&snapshot);
        device
    }

    pub async fn emit_message_delta(&self, session_id: &str, message_id: &str, delta: &str) {
        {
            let mut messages = self.messages.write().await;
            if let Some(message) = messages
                .get_mut(session_id)
                .and_then(|items| items.iter_mut().find(|item| item.id == message_id))
            {
                message.content.push_str(delta);
            }
        }

        let _ = self
            .event_tx
            .send(SessionEvent::MessageDelta(MessageDeltaEvent {
                session_id: session_id.to_string(),
                message_id: message_id.to_string(),
                delta: delta.to_string(),
            }));
    }

    pub async fn emit_system_message(&self, session_id: &str, content: impl Into<String>) {
        let content = content.into().trim().to_string();
        if content.is_empty() {
            return;
        }

        let message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::System,
            content,
            created_at: Utc::now(),
        };
        self.push_message(message.clone()).await;
        let _ = self.event_tx.send(SessionEvent::MessageCreated(message));
    }

    pub async fn finish_assistant_message(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<(), String> {
        let content = {
            let messages = self.messages.read().await;
            messages
                .get(session_id)
                .and_then(|items| items.iter().find(|item| item.id == message_id))
                .map(|message| message.content.clone())
                .ok_or_else(|| format!("unknown message: {message_id}"))?
        };

        self.patch_session(session_id, SessionStatus::Idle, Some(content.clone()))
            .await;
        self.clear_interrupted(session_id).await;
        self.clear_pending_approval(session_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Idle,
                error_message: None,
            }));
        if let Some(session) = self.find_session(session_id).await {
            let devices = self
                .devices
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            self.push
                .send_assistant_reply(session, content, devices)
                .await;
        }
        Ok(())
    }

    pub async fn fail_session(&self, session_id: &str, message: String) {
        self.patch_session(session_id, SessionStatus::Failed, Some(message.clone()))
            .await;
        self.clear_interrupted(session_id).await;
        self.clear_pending_approval(session_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::AgentError(AgentErrorEvent {
                session_id: session_id.to_string(),
                message: message.clone(),
            }));
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Failed,
                error_message: Some(message.clone()),
            }));
    }

    pub async fn set_provider_session_ref(&self, session_id: &str, session_ref: Option<String>) {
        self.update_runtime_state(session_id, |entry| {
            entry.provider_session_ref = session_ref;
        })
        .await;
    }

    pub async fn set_codex_provider_name(&self, session_id: &str, name: Option<String>) {
        self.update_runtime_state(session_id, |entry| {
            entry.codex_provider_name = name;
        })
        .await;
    }

    pub async fn codex_provider_name(&self, session_id: &str) -> Option<String> {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .and_then(|item| item.codex_provider_name.clone())
    }

    pub async fn set_codex_model(&self, session_id: &str, model: Option<String>) {
        self.update_runtime_state(session_id, |entry| {
            entry.codex_model = model;
        })
        .await;
    }

    pub async fn codex_model(&self, session_id: &str) -> Option<String> {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .and_then(|item| item.codex_model.clone())
    }

    pub async fn set_claude_provider_id(&self, session_id: &str, provider_id: Option<String>) {
        self.update_runtime_state(session_id, |entry| {
            entry.claude_provider_id = provider_id;
        })
        .await;
    }

    pub async fn claude_provider_id(&self, session_id: &str) -> Option<String> {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .and_then(|item| item.claude_provider_id.clone())
    }

    pub async fn set_claude_model(&self, session_id: &str, model: Option<String>) {
        self.update_runtime_state(session_id, |entry| {
            entry.claude_model = model;
        })
        .await;
    }

    pub async fn claude_model(&self, session_id: &str) -> Option<String> {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .and_then(|item| item.claude_model.clone())
    }

    pub async fn provider_session_ref(&self, session_id: &str) -> Option<String> {
        let runtime = self.runtime.lock().await;
        if let Some(item) = runtime
            .get(session_id)
            .and_then(|item| item.provider_session_ref.clone())
        {
            return Some(item);
        }
        drop(runtime);
        let session = self.find_session(session_id).await?;
        let provider = self.provider_for_agent(session.agent)?;
        provider.default_runtime_ref(session_id).await
    }

    async fn update_runtime_state<F>(&self, session_id: &str, update: F)
    where
        F: FnOnce(&mut SessionRuntimeState),
    {
        let snapshot = {
            let mut runtime = self.runtime.lock().await;
            let entry = runtime
                .entry(session_id.to_string())
                .or_insert_with(SessionRuntimeState::default);
            update(entry);
            runtime
                .iter()
                .map(|(session_id, state)| (session_id.clone(), state.to_persisted()))
                .collect::<HashMap<_, _>>()
        };
        if let Err(error) = write_persisted_runtime(&self.runtime_store_path, &snapshot).await {
            eprintln!(
                "failed to persist session runtime metadata at {}: {error}",
                self.runtime_store_path.display()
            );
        }
    }

    pub async fn project_root_path_for_session(&self, session_id: &str) -> Result<String, String> {
        let session = self
            .find_session(session_id)
            .await
            .ok_or_else(|| format!("unknown session: {session_id}"))?;
        let project = self
            .find_project(&session.project_id)
            .await
            .ok_or_else(|| format!("unknown project: {}", session.project_id))?;
        Ok(project.root_path)
    }

    pub async fn set_approval_sender(
        &self,
        session_id: &str,
        sender: mpsc::UnboundedSender<ApprovalChoice>,
    ) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        entry.approval_tx = Some(sender);
    }

    pub async fn clear_approval_sender(&self, session_id: &str) {
        let mut runtime = self.runtime.lock().await;
        if let Some(entry) = runtime.get_mut(session_id) {
            entry.approval_tx = None;
        }
    }

    pub async fn finish_turn(&self, session_id: &str) {
        self.update_runtime_state(session_id, |entry| {
            entry.turn_in_flight = false;
            entry.turn_abort = None;
        })
        .await;
    }

    pub async fn set_turn_abort(&self, session_id: &str, abort_handle: Option<AbortHandle>) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        entry.turn_abort = abort_handle;
    }

    pub async fn raise_approval(&self, session_id: &str, request: ApprovalRequest) {
        self.update_runtime_state(session_id, |entry| {
            entry.pending_approval = Some(request.clone());
            entry.last_resolved_approval_request_id = None;
            entry.interrupted = false;
        })
        .await;
        self.patch_session(
            session_id,
            SessionStatus::AwaitingApproval,
            request
                .reason
                .clone()
                .or_else(|| request.command.clone())
                .or(Some("等待审批".to_string())),
        )
        .await;
        if let Some(session) = self.find_session(session_id).await {
            let mut session = session;
            session.pending_approval = Some(request.clone());
            let devices = self
                .devices
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            self.push
                .send_approval_request(session, request.clone(), devices)
                .await;
        }
        let _ = self
            .event_tx
            .send(SessionEvent::ApprovalRequested(ApprovalRequestEvent {
                session_id: session_id.to_string(),
                request,
            }));
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::AwaitingApproval,
                error_message: None,
            }));
    }

    pub async fn resolve_approval(
        &self,
        session_id: &str,
        request_id: &str,
        choice: ApprovalChoice,
    ) {
        eprintln!(
            "[approval] resolved session={session_id} request={request_id} choice={choice:?}"
        );
        let preview = approval_choice_preview(&choice).to_string();
        self.update_runtime_state(session_id, |entry| {
            entry.pending_approval = None;
            entry.last_resolved_approval_request_id = Some(request_id.to_string());
            entry.interrupted = false;
        })
        .await;
        self.patch_session(session_id, SessionStatus::Running, Some(preview.clone()))
            .await;
        self.emit_system_message(session_id, preview.clone()).await;
        let _ = self
            .event_tx
            .send(SessionEvent::ApprovalResolved(ApprovalResolvedEvent {
                session_id: session_id.to_string(),
                request_id: request_id.to_string(),
                choice,
            }));
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Running,
                error_message: None,
            }));
    }

    async fn clear_pending_approval(&self, session_id: &str) {
        self.update_runtime_state(session_id, |entry| {
            entry.pending_approval = None;
        })
        .await;
    }

    async fn clear_interrupted(&self, session_id: &str) {
        self.update_runtime_state(session_id, |entry| {
            entry.interrupted = false;
        })
        .await;
    }

    async fn push_message(&self, message: ChatMessage) {
        let mut messages = self.messages.write().await;
        messages
            .entry(message.session_id.clone())
            .or_default()
            .push(message);
    }

    /// If the in-memory message cache has no entry for this session yet,
    /// pre-seed it from the provider's archive so that later `list_messages`
    /// calls don't shadow the historical conversation.
    async fn ensure_inbox_seeded(&self, session_id: &str, agent: AgentKind) {
        if self.messages.read().await.contains_key(session_id) {
            return;
        }
        let Some(provider) = self.provider_for_agent(agent) else {
            return;
        };
        if let Some(existing) = provider.list_messages(session_id).await {
            self.messages
                .write()
                .await
                .insert(session_id.to_string(), existing);
        }
    }

    async fn latest_assistant_preview(&self, session_id: &str) -> Option<String> {
        self.messages
            .read()
            .await
            .get(session_id)
            .and_then(|items| {
                items
                    .iter()
                    .rev()
                    .find(|item| matches!(item.role, MessageRole::Assistant))
            })
            .map(|message| message.content.trim().to_string())
            .filter(|content| !content.is_empty())
    }

    async fn patch_session(
        &self,
        session_id: &str,
        status: SessionStatus,
        last_message_preview: Option<String>,
    ) {
        let mut sessions = self.sessions.write().await;
        let mut session = sessions.remove(session_id);
        drop(sessions);
        if session.is_none() {
            session = self.find_session(session_id).await;
        }
        if let Some(current) = session.as_mut() {
            current.status = status;
            current.updated_at = Utc::now();
            current.last_message_preview = last_message_preview;
        }
        if let Some(current) = session {
            let project_id = current.project_id.clone();
            self.sessions
                .write()
                .await
                .insert(current.id.clone(), current);
            self.invalidate_list_cache().await;
            self.refresh_project_summary(&project_id).await;
        }
    }

    pub async fn update_session_settings(
        &self,
        session_id: &str,
        provider_id: Option<Option<String>>,
        reasoning_effort: Option<Option<crate::models::ReasoningEffort>>,
    ) -> Result<SessionSummary, String> {
        let mut sessions = self.sessions.write().await;
        let mut session = sessions.remove(session_id);
        drop(sessions);
        if session.is_none() {
            session = self.find_session(session_id).await;
        }
        let mut current = session.ok_or_else(|| format!("unknown session: {session_id}"))?;
        if let Some(provider_id) = provider_id {
            current.provider_id = provider_id;
        }
        if let Some(reasoning_effort) = reasoning_effort {
            current.reasoning_effort = reasoning_effort;
        }
        current.updated_at = Utc::now();
        let project_id = current.project_id.clone();
        let summary = current.clone();
        self.sessions
            .write()
            .await
            .insert(current.id.clone(), current);
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&project_id).await;
        Ok(summary)
    }

    pub async fn update_session_title(
        &self,
        session_id: &str,
        title: String,
    ) -> Result<SessionSummary, String> {
        let title = title.trim().to_string();
        if title.is_empty() {
            return Err("session title cannot be empty".to_string());
        }

        let mut sessions = self.sessions.write().await;
        let mut session = sessions.remove(session_id);
        drop(sessions);
        if session.is_none() {
            session = self.find_session(session_id).await;
        }
        let mut current = session.ok_or_else(|| format!("unknown session: {session_id}"))?;
        current.title = title;
        current.updated_at = Utc::now();
        let project_id = current.project_id.clone();
        let summary = current.clone();
        self.sessions
            .write()
            .await
            .insert(current.id.clone(), current);
        self.persist_session_title_override(session_id, &summary.title)
            .await?;
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&project_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::SessionSnapshot(summary.clone()));
        Ok(summary)
    }

    async fn persist_session_title_override(
        &self,
        session_id: &str,
        title: &str,
    ) -> Result<(), String> {
        self.session_title_overrides
            .write()
            .await
            .insert(session_id.to_string(), title.to_string());
        let entries = self.session_title_overrides.read().await.clone();
        let metadata = PersistedSessionMetadata {
            sessions: entries
                .into_iter()
                .map(|(session_id, title)| {
                    (
                        session_id,
                        PersistedSessionMetadataEntry { title: Some(title) },
                    )
                })
                .collect(),
        };
        write_persisted_session_metadata(&self.session_metadata_store_path, &metadata)
            .await
            .map_err(|error| {
                format!(
                    "failed to persist session metadata at {}: {error}",
                    self.session_metadata_store_path.display()
                )
            })
    }

    async fn refresh_project_summary(&self, project_id: &str) {
        let cache = self.ensure_list_cache().await;
        let sessions = cache
            .project_sessions
            .get(project_id)
            .cloned()
            .unwrap_or_default();
        let base_project = cache.projects_by_id.get(project_id).cloned();
        let mut projects = self.projects.write().await;
        let project = if let Some(project) = projects.get_mut(project_id) {
            project
        } else if let Some(project) = base_project {
            projects.entry(project_id.to_string()).or_insert(project)
        } else {
            drop(projects);
            self.invalidate_list_cache().await;
            return;
        };
        project.updated_at = sessions
            .first()
            .map(|session| session.updated_at)
            .unwrap_or_else(Utc::now);
        project.session_count = sessions.len() as u32;
        project.last_session_preview = sessions
            .first()
            .and_then(|session| session.last_message_preview.clone());
        drop(projects);
        self.invalidate_list_cache().await;
    }

    async fn find_project(&self, project_id: &str) -> Option<ProjectSummary> {
        if let Some(project) = self.projects.read().await.get(project_id).cloned() {
            return Some(project);
        }
        self.ensure_list_cache()
            .await
            .projects_by_id
            .get(project_id)
            .cloned()
    }

    async fn find_session(&self, session_id: &str) -> Option<SessionSummary> {
        let session = if let Some(session) = self.sessions.read().await.get(session_id).cloned() {
            session
        } else {
            self.ensure_list_cache()
                .await
                .sessions_by_id
                .get(session_id)
                .cloned()?
        };
        Some(self.with_runtime_approval(session).await)
    }

    async fn invalidate_list_cache(&self) {
        *self.list_cache.lock().await = None;
    }

    async fn ensure_list_cache(&self) -> AggregatedListCache {
        {
            let cache = self.list_cache.lock().await;
            if let Some(existing) = cache.as_ref()
                && existing.checked_at.elapsed() < Self::LIST_CACHE_TTL
                && git_fingerprint_for_projects(&existing.projects) == existing.git_fingerprint
            {
                return existing.clone();
            }
        }

        let mut merged_projects = HashMap::new();
        let mut merged_sessions = HashMap::new();
        for provider in self.providers.all() {
            merged_projects.extend(provider.list_projects().await);
            merged_sessions.extend(provider.list_sessions().await);
        }

        {
            let projects = self.projects.read().await;
            for (id, project) in projects.iter() {
                merged_projects.insert(id.clone(), project.clone());
            }
        }
        {
            let sessions = self.sessions.read().await;
            for (id, session) in sessions.iter() {
                merged_sessions.insert(id.clone(), session.clone());
            }
        }

        let project_git_states = merged_projects
            .iter()
            .map(|(project_id, project)| {
                (project_id.clone(), git_project_state(&project.root_path))
            })
            .collect::<HashMap<_, _>>();
        for (project_id, project) in &mut merged_projects {
            if let Some(state) = project_git_states.get(project_id) {
                project.git_branch = state.branch.clone();
                project.git_status = state.status.clone();
            }
        }

        let local_session_ids = {
            let sessions = self.sessions.read().await;
            sessions
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
        };
        let runtime_refs = {
            let runtime = self.runtime.lock().await;
            runtime
                .iter()
                .filter_map(|(session_id, state)| {
                    state
                        .provider_session_ref
                        .as_ref()
                        .map(|session_ref| (session_id.clone(), session_ref.clone()))
                })
                .collect::<HashMap<_, _>>()
        };
        for (session_id, session_ref) in runtime_refs {
            if session_id == session_ref {
                continue;
            }
            if !local_session_ids.contains(&session_id) {
                continue;
            }
            let Some(local_session) = merged_sessions.get(&session_id).cloned() else {
                continue;
            };
            let Some(provider_session) = merged_sessions.remove(&session_ref) else {
                continue;
            };
            merged_sessions.insert(
                session_id,
                Self::merge_linked_sessions(local_session, provider_session),
            );
        }

        let mut sessions = merged_sessions.into_values().collect::<Vec<_>>();
        sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

        let mut project_sessions = HashMap::<String, Vec<SessionSummary>>::new();
        for session in &sessions {
            project_sessions
                .entry(session.project_id.clone())
                .or_default()
                .push(session.clone());
        }

        for (project_id, project) in &mut merged_projects {
            if let Some(project_items) = project_sessions.get(project_id)
                && let Some(latest_session) = project_items.first()
            {
                project.updated_at = latest_session.updated_at;
                project.session_count = project_items.len() as u32;
                project.last_session_preview = latest_session.last_message_preview.clone();
            }
        }

        let mut projects = merged_projects.into_values().collect::<Vec<_>>();
        projects.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

        let cache = AggregatedListCache {
            checked_at: Instant::now(),
            git_fingerprint: project_git_states
                .values()
                .map(|state| state.fingerprint)
                .fold(0, |acc, fingerprint| acc ^ fingerprint),
            projects_by_id: projects
                .iter()
                .cloned()
                .map(|project| (project.id.clone(), project))
                .collect(),
            sessions_by_id: sessions
                .iter()
                .cloned()
                .map(|session| (session.id.clone(), session))
                .collect(),
            projects,
            sessions,
            project_sessions,
        };

        let mut slot = self.list_cache.lock().await;
        *slot = Some(cache.clone());
        cache
    }

    fn merge_linked_sessions(
        mut local_session: SessionSummary,
        provider_session: SessionSummary,
    ) -> SessionSummary {
        local_session.status =
            Self::preferred_session_status(local_session.status, provider_session.status);

        if provider_session.updated_at > local_session.updated_at {
            local_session.updated_at = provider_session.updated_at;
            local_session.unread_count = provider_session.unread_count;
        } else {
            local_session.unread_count = local_session
                .unread_count
                .max(provider_session.unread_count);
        }

        if (local_session.title.trim().is_empty() || local_session.title.ends_with(" 会话"))
            && !provider_session.title.trim().is_empty()
            && !provider_session.title.ends_with(" 会话")
        {
            local_session.title = provider_session.title;
        }

        if local_session
            .last_message_preview
            .as_deref()
            .is_none_or(|preview| preview.trim().is_empty())
        {
            local_session.last_message_preview = provider_session.last_message_preview;
        }

        local_session.pending_approval = local_session
            .pending_approval
            .or(provider_session.pending_approval);
        local_session
    }

    fn preferred_session_status(
        local_status: SessionStatus,
        provider_status: SessionStatus,
    ) -> SessionStatus {
        let rank = |status: &SessionStatus| match status {
            SessionStatus::AwaitingApproval => 6,
            SessionStatus::Interrupted => 5,
            SessionStatus::Running => 4,
            SessionStatus::Waiting => 3,
            SessionStatus::Failed => 2,
            SessionStatus::Idle => 1,
        };

        if rank(&local_status) >= rank(&provider_status) {
            local_status
        } else {
            provider_status
        }
    }

    fn provider_for_agent(
        &self,
        agent: crate::models::AgentKind,
    ) -> Option<Arc<dyn crate::adapter::AgentProvider>> {
        self.providers.get(agent)
    }
}

#[derive(Debug, Clone, Default)]
struct GitProjectState {
    branch: Option<String>,
    status: Option<ProjectGitStatus>,
    fingerprint: u64,
}

#[derive(Debug, Clone)]
struct GitStatusState {
    branch: Option<String>,
    project_status: ProjectGitStatus,
    detail: GitStatusDetail,
}

fn git_fingerprint_for_projects(projects: &[ProjectSummary]) -> u64 {
    projects
        .iter()
        .map(|project| git_project_state(&project.root_path).fingerprint)
        .fold(0, |acc, fingerprint| acc ^ fingerprint)
}

fn git_project_state(root_path: &str) -> GitProjectState {
    if root_path.contains("://") {
        return GitProjectState::default();
    }

    let root = Path::new(root_path);
    let Some(git_dir) = resolve_git_dir(root) else {
        return GitProjectState::default();
    };
    let head_path = git_dir.join("HEAD");
    let head = match fs::read_to_string(&head_path) {
        Ok(value) => value.trim().to_string(),
        Err(_) => return GitProjectState::default(),
    };

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root_path.hash(&mut hasher);
    head.hash(&mut hasher);
    file_fingerprint_for_cache(&head_path).hash(&mut hasher);

    file_fingerprint_for_cache(&git_dir.join("index")).hash(&mut hasher);
    file_fingerprint_for_cache(&git_dir.join("packed-refs")).hash(&mut hasher);
    file_fingerprint_for_cache(&git_dir.join("FETCH_HEAD")).hash(&mut hasher);

    let branch_from_head = head
        .strip_prefix("ref:")
        .map(str::trim)
        .and_then(|reference| {
            file_fingerprint_for_cache(&git_dir.join(reference)).hash(&mut hasher);
            reference
                .strip_prefix("refs/heads/")
                .map(ToString::to_string)
        });
    let status = git_status_for_project(root_path);

    GitProjectState {
        branch: status
            .as_ref()
            .and_then(|state| state.branch.clone())
            .or(branch_from_head),
        status: status.map(|state| state.project_status),
        fingerprint: hasher.finish(),
    }
}

fn git_status_for_project(root_path: &str) -> Option<GitStatusState> {
    if root_path.contains("://") {
        return None;
    }

    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "--branch"])
        .current_dir(root_path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    parse_git_status_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_git_status_output(output: &str) -> Option<GitStatusState> {
    let mut branch = None;
    let mut staged = false;
    let mut unstaged = false;
    let mut untracked = false;
    let mut staged_count = 0u32;
    let mut unstaged_count = 0u32;
    let mut untracked_count = 0u32;
    let mut ahead = None;
    let mut behind = None;

    for line in output.lines() {
        if let Some(header) = line.strip_prefix("## ") {
            parse_git_branch_header(header, &mut branch, &mut ahead, &mut behind);
            continue;
        }

        if line.starts_with("??") {
            untracked = true;
            untracked_count += 1;
            continue;
        }

        let mut chars = line.chars();
        let index_status = chars.next().unwrap_or(' ');
        let worktree_status = chars.next().unwrap_or(' ');
        if index_status != ' ' {
            staged = true;
            staged_count += 1;
        }
        if worktree_status != ' ' {
            unstaged = true;
            unstaged_count += 1;
        }
    }

    let dirty = staged || unstaged || untracked;
    let detail_branch = branch.clone();
    Some(GitStatusState {
        branch,
        project_status: if dirty {
            ProjectGitStatus::Dirty
        } else {
            ProjectGitStatus::Clean
        },
        detail: GitStatusDetail {
            branch: detail_branch,
            dirty,
            staged,
            unstaged,
            untracked,
            changed_count: staged_count + unstaged_count + untracked_count,
            staged_count,
            unstaged_count,
            untracked_count,
            ahead,
            behind,
        },
    })
}

fn parse_git_branch_header(
    header: &str,
    branch: &mut Option<String>,
    ahead: &mut Option<u32>,
    behind: &mut Option<u32>,
) {
    let (name, tracking) = header
        .split_once("...")
        .map_or((header, None), |(name, rest)| (name, Some(rest)));
    let name = name.trim();
    if !name.is_empty() && name != "HEAD (no branch)" {
        *branch = Some(name.to_string());
    }

    if let Some(tracking) = tracking
        && let Some(start) = tracking.find('[')
        && let Some(end) = tracking[start + 1..].find(']')
    {
        for item in tracking[start + 1..start + 1 + end].split(',') {
            let item = item.trim();
            if let Some(value) = item.strip_prefix("ahead ") {
                *ahead = value.parse().ok();
            } else if let Some(value) = item.strip_prefix("behind ") {
                *behind = value.parse().ok();
            }
        }
    }
}

fn resolve_git_dir(root: &Path) -> Option<PathBuf> {
    let git_path = root.join(".git");
    if git_path.is_dir() {
        return Some(git_path);
    }

    let content = fs::read_to_string(&git_path).ok()?;
    let path = content.trim().strip_prefix("gitdir:")?.trim();
    let git_dir = PathBuf::from(path);
    Some(if git_dir.is_absolute() {
        git_dir
    } else {
        root.join(git_dir)
    })
}

fn file_fingerprint_for_cache(path: &Path) -> Option<(u64, u128)> {
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis();
    Some((metadata.len(), modified))
}

async fn load_persisted_runtime(path: &PathBuf) -> HashMap<String, PersistedSessionRuntimeState> {
    match tokio::fs::read_to_string(path).await {
        Ok(body) => serde_json::from_str::<HashMap<String, PersistedSessionRuntimeState>>(&body)
            .unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

async fn write_persisted_runtime(
    path: &PathBuf,
    runtime: &HashMap<String, PersistedSessionRuntimeState>,
) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let body = serde_json::to_string_pretty(runtime)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::fs::write(path, body).await
}

async fn load_persisted_session_metadata(path: &PathBuf) -> PersistedSessionMetadata {
    match tokio::fs::read_to_string(path).await {
        Ok(body) => serde_json::from_str::<PersistedSessionMetadata>(&body).unwrap_or_default(),
        Err(_) => PersistedSessionMetadata::default(),
    }
}

async fn write_persisted_session_metadata(
    path: &PathBuf,
    metadata: &PersistedSessionMetadata,
) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let body = serde_json::to_string_pretty(metadata)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::fs::write(path, body).await
}

fn runtime_store_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".omni-code")
        .join("session-runtime.json")
}

fn session_metadata_store_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".omni-code")
        .join("session-metadata.json")
}

fn approval_choice_preview(choice: &ApprovalChoice) -> &'static str {
    match choice {
        ApprovalChoice::Accept => "审批已允许",
        ApprovalChoice::AcceptForSession => "当前会话已允许后续同类请求",
        ApprovalChoice::AlwaysAllow => "已永久允许此类请求",
        ApprovalChoice::Decline => "审批已拒绝",
        ApprovalChoice::Cancel => "审批已取消",
    }
}

fn decorate_user_content(input: &SendMessageInput) -> String {
    match input.input_mode {
        InputMode::Text => input.content.clone(),
        InputMode::Voice => format!("[voice] {}", input.content),
    }
}

fn message_title(content: &str) -> String {
    let title = content
        .lines()
        .find_map(|line| {
            let trimmed = line.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .unwrap_or("移动端消息");
    title.chars().take(24).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bridge_settings::{AiApprovalSettings, BridgeSettingsInput},
        models::{AUTO_PROVIDER_ID, AgentKind, ApiFormat, ModelProviderConfig, SessionSummary},
    };
    use chrono::Utc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}.json"))
    }

    async fn test_state(prefix: &str) -> AppState {
        let settings_path = test_path(&format!("{prefix}-settings"));
        let runtime_path = test_path(&format!("{prefix}-runtime"));
        let metadata_path = test_path(&format!("{prefix}-metadata"));
        AppState::new_with_paths(settings_path, runtime_path, metadata_path).await
    }

    #[tokio::test]
    async fn tts_stream_session_can_be_read_multiple_times_until_ttl() {
        let state = test_state("tts-stream-session").await;
        let session = state
            .create_tts_stream_session(
                "model".to_string(),
                "hello".to_string(),
                Some("48".to_string()),
                None,
                None,
                "audio/wav".to_string(),
            )
            .await;

        let first = state.get_tts_stream_session(&session.token).await;
        let second = state.get_tts_stream_session(&session.token).await;

        assert_eq!(
            first.as_ref().map(|value| value.token.as_str()),
            Some(session.token.as_str())
        );
        assert_eq!(
            second.as_ref().map(|value| value.token.as_str()),
            Some(session.token.as_str())
        );
    }

    #[test]
    fn git_project_state_tracks_branch_changes() {
        let root =
            std::env::temp_dir().join(format!("omni-code-bridge-git-state-{}", std::process::id()));
        let git_dir = root.join(".git");
        std::fs::create_dir_all(git_dir.join("refs/heads")).expect("create git refs");
        std::fs::write(git_dir.join("HEAD"), "ref: refs/heads/main\n").expect("write head");
        std::fs::write(git_dir.join("refs/heads/main"), "main-sha\n").expect("write main");

        let main = git_project_state(root.to_str().expect("utf8 temp path"));
        std::fs::write(git_dir.join("HEAD"), "ref: refs/heads/feature\n").expect("switch head");
        std::fs::write(git_dir.join("refs/heads/feature"), "feature-sha\n").expect("write feature");
        let feature = git_project_state(root.to_str().expect("utf8 temp path"));

        assert_eq!(main.branch.as_deref(), Some("main"));
        assert_eq!(feature.branch.as_deref(), Some("feature"));
        assert_ne!(main.fingerprint, feature.fingerprint);
    }

    #[test]
    fn parse_git_status_output_detects_dirty_detail() {
        let status = parse_git_status_output(
            "## feature...origin/feature [ahead 2, behind 1]\nM  staged.txt\n M unstaged.txt\n?? new.txt\n",
        )
        .expect("git status should parse");

        assert_eq!(status.branch.as_deref(), Some("feature"));
        assert_eq!(status.project_status, ProjectGitStatus::Dirty);
        assert_eq!(status.detail.branch.as_deref(), Some("feature"));
        assert!(status.detail.dirty);
        assert!(status.detail.staged);
        assert!(status.detail.unstaged);
        assert!(status.detail.untracked);
        assert_eq!(status.detail.changed_count, 3);
        assert_eq!(status.detail.staged_count, 1);
        assert_eq!(status.detail.unstaged_count, 1);
        assert_eq!(status.detail.untracked_count, 1);
        assert_eq!(status.detail.ahead, Some(2));
        assert_eq!(status.detail.behind, Some(1));
    }

    #[test]
    fn parse_git_status_output_detects_clean_project() {
        let status =
            parse_git_status_output("## main...origin/main\n").expect("git status should parse");

        assert_eq!(status.branch.as_deref(), Some("main"));
        assert_eq!(status.project_status, ProjectGitStatus::Clean);
        assert_eq!(status.detail.branch.as_deref(), Some("main"));
        assert!(!status.detail.dirty);
        assert!(!status.detail.staged);
        assert!(!status.detail.unstaged);
        assert!(!status.detail.untracked);
        assert_eq!(status.detail.changed_count, 0);
        assert_eq!(status.detail.staged_count, 0);
        assert_eq!(status.detail.unstaged_count, 0);
        assert_eq!(status.detail.untracked_count, 0);
        assert_eq!(status.detail.ahead, None);
        assert_eq!(status.detail.behind, None);
    }

    #[test]
    fn linked_session_merge_requires_live_local_session() {
        let live_session_id = "live-local".to_string();
        let archived_session_id = "archived-local".to_string();
        let provider_session_id = "provider-session".to_string();
        let now = Utc::now();

        let local_session_ids = std::collections::HashSet::from([live_session_id.clone()]);
        let runtime_refs = std::collections::HashMap::from([
            (live_session_id.clone(), provider_session_id.clone()),
            (archived_session_id.clone(), provider_session_id.clone()),
        ]);
        let mut merged_sessions = std::collections::HashMap::from([
            (
                live_session_id.clone(),
                SessionSummary {
                    id: live_session_id.clone(),
                    project_id: "project".to_string(),
                    title: "local".to_string(),
                    agent: AgentKind::Codex,
                    brief_reply_mode: false,
                    status: SessionStatus::Idle,
                    updated_at: now,
                    unread_count: 0,
                    last_message_preview: Some("local".to_string()),
                    pending_approval: None,
                    provider_id: None,
                    reasoning_effort: None,
                },
            ),
            (
                archived_session_id.clone(),
                SessionSummary {
                    id: archived_session_id.clone(),
                    project_id: "project".to_string(),
                    title: "archived".to_string(),
                    agent: AgentKind::Codex,
                    brief_reply_mode: false,
                    status: SessionStatus::Idle,
                    updated_at: now,
                    unread_count: 0,
                    last_message_preview: Some("archived".to_string()),
                    pending_approval: None,
                    provider_id: None,
                    reasoning_effort: None,
                },
            ),
            (
                provider_session_id.clone(),
                SessionSummary {
                    id: provider_session_id.clone(),
                    project_id: "project".to_string(),
                    title: "provider".to_string(),
                    agent: AgentKind::Codex,
                    brief_reply_mode: false,
                    status: SessionStatus::Running,
                    updated_at: now,
                    unread_count: 1,
                    last_message_preview: Some("provider".to_string()),
                    pending_approval: None,
                    provider_id: None,
                    reasoning_effort: None,
                },
            ),
        ]);

        for (session_id, session_ref) in runtime_refs {
            if session_id == session_ref {
                continue;
            }
            if !local_session_ids.contains(&session_id) {
                continue;
            }
            let Some(local_session) = merged_sessions.get(&session_id).cloned() else {
                continue;
            };
            let Some(provider_session) = merged_sessions.remove(&session_ref) else {
                continue;
            };
            merged_sessions.insert(
                session_id,
                AppState::merge_linked_sessions(local_session, provider_session),
            );
        }

        assert!(merged_sessions.contains_key(&live_session_id));
        assert!(merged_sessions.contains_key(&archived_session_id));
        assert!(!merged_sessions.contains_key(&provider_session_id));
    }

    #[tokio::test]
    async fn resolve_provider_config_skips_when_provider_id_missing() {
        let state = test_state("provider-config-missing").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: AiApprovalSettings::default(),
                model_providers: Some(vec![ModelProviderConfig {
                    id: "codex-primary".to_string(),
                    name: "Codex Primary".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "key".to_string(),
                    model: Some("gpt-4.1".to_string()),
                    format: ApiFormat::Codex,
                    enabled: true,
                    priority: 0,
                }]),
                speech_profiles: None,
                speech_voices: None,
                speaker_filter: None,
            })
            .await
            .expect("settings update should succeed");

        let session = SessionSummary {
            id: "session-1".to_string(),
            project_id: "project-1".to_string(),
            title: "Session".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            provider_id: None,
            reasoning_effort: None,
        };

        let resolved = state.resolve_provider_config(&session, &None).await;
        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn get_session_applies_detached_running_overlay() {
        let state = test_state("detached-running-detail").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .patch_session(
                &session.id,
                SessionStatus::Running,
                Some("running".to_string()),
            )
            .await;
        state
            .set_provider_session_ref(&session.id, Some("codex-thread".to_string()))
            .await;

        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");

        assert!(matches!(detail.session.status, SessionStatus::Interrupted));
    }

    #[tokio::test]
    async fn update_session_settings_clears_reasoning_effort_for_list_and_detail() {
        let state = test_state("clear-reasoning-effort").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: Some(crate::models::ReasoningEffort::Medium),
            })
            .await
            .expect("session should be created");

        let updated = state
            .update_session_settings(&session.id, None, Some(None))
            .await
            .expect("session should update");
        assert_eq!(updated.reasoning_effort, None);

        let listed = state.list_sessions().await;
        let listed_session = listed
            .into_iter()
            .find(|item| item.id == session.id)
            .expect("session should appear in list");
        assert_eq!(listed_session.reasoning_effort, None);

        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");
        assert_eq!(detail.session.reasoning_effort, None);
    }

    #[tokio::test]
    async fn update_session_title_persists_override_metadata() {
        let settings_path = test_path("persist-title-settings");
        let runtime_path = test_path("persist-title-runtime");
        let metadata_path = test_path("persist-title-metadata");
        let state =
            AppState::new_with_paths(settings_path, runtime_path, metadata_path.clone()).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Old".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .update_session_title(&session.id, "Renamed".to_string())
            .await
            .expect("rename should persist");

        let body = tokio::fs::read_to_string(metadata_path)
            .await
            .expect("metadata should be written");
        let metadata: PersistedSessionMetadata =
            serde_json::from_str(&body).expect("metadata should parse");
        assert_eq!(
            metadata
                .sessions
                .get(&session.id)
                .and_then(|entry| entry.title.as_deref()),
            Some("Renamed")
        );
    }

    #[tokio::test]
    async fn persisted_title_override_survives_restart() {
        let settings_path = test_path("restore-title-settings");
        let runtime_path = test_path("restore-title-runtime");
        let metadata_path = test_path("restore-title-metadata");
        let state = AppState::new_with_paths(
            settings_path.clone(),
            runtime_path.clone(),
            metadata_path.clone(),
        )
        .await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Original".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");
        state
            .update_session_title(&session.id, "Restarted Title".to_string())
            .await
            .expect("rename should persist");

        let restarted = AppState::new_with_paths(settings_path, runtime_path, metadata_path).await;
        let detail = restarted
            .with_runtime_approval(SessionSummary {
                id: session.id.clone(),
                project_id: session.project_id.clone(),
                title: "Derived Again".to_string(),
                agent: session.agent,
                brief_reply_mode: session.brief_reply_mode,
                status: SessionStatus::Idle,
                updated_at: Utc::now(),
                unread_count: 0,
                last_message_preview: None,
                pending_approval: None,
                provider_id: None,
                reasoning_effort: None,
            })
            .await;

        assert_eq!(detail.title, "Restarted Title");
    }

    #[tokio::test]
    async fn cancel_turn_interrupts_detached_running_session() {
        let state = test_state("cancel-detached-running").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .patch_session(
                &session.id,
                SessionStatus::Running,
                Some("running".to_string()),
            )
            .await;
        state
            .set_provider_session_ref(&session.id, Some("codex-thread".to_string()))
            .await;

        let cancelled = state
            .cancel_turn(&session.id)
            .await
            .expect("cancel should succeed");
        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");

        assert!(cancelled);
        assert!(matches!(detail.session.status, SessionStatus::Interrupted));
    }

    #[tokio::test]
    async fn cancel_turn_interrupts_running_session_missing_runtime_entry() {
        let state = test_state("cancel-missing-runtime").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
            })
            .await
            .expect("session should be created");

        state
            .patch_session(
                &session.id,
                SessionStatus::Running,
                Some("running".to_string()),
            )
            .await;
        state.runtime.lock().await.remove(&session.id);

        let cancelled = state
            .cancel_turn(&session.id)
            .await
            .expect("cancel should succeed for persisted sessions");
        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");

        assert!(cancelled);
        assert!(matches!(detail.session.status, SessionStatus::Interrupted));
    }

    #[tokio::test]
    async fn resolve_provider_config_auto_uses_priority_provider() {
        let state = test_state("provider-config-auto").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: AiApprovalSettings::default(),
                model_providers: Some(vec![
                    ModelProviderConfig {
                        id: "codex-secondary".to_string(),
                        name: "Codex Secondary".to_string(),
                        base_url: "https://secondary.test/v1".to_string(),
                        api_key: "key-2".to_string(),
                        model: Some("gpt-4.1-mini".to_string()),
                        format: ApiFormat::Codex,
                        enabled: true,
                        priority: 10,
                    },
                    ModelProviderConfig {
                        id: "codex-primary".to_string(),
                        name: "Codex Primary".to_string(),
                        base_url: "https://primary.test/v1".to_string(),
                        api_key: "key-1".to_string(),
                        model: Some("gpt-4.1".to_string()),
                        format: ApiFormat::Codex,
                        enabled: true,
                        priority: 0,
                    },
                ]),
                speech_profiles: None,
                speech_voices: None,
                speaker_filter: None,
            })
            .await
            .expect("settings update should succeed");

        let session = SessionSummary {
            id: "session-2".to_string(),
            project_id: "project-1".to_string(),
            title: "Session".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            provider_id: Some(AUTO_PROVIDER_ID.to_string()),
            reasoning_effort: None,
        };

        let resolved = state.resolve_provider_config(&session, &None).await;
        let resolved = resolved.expect("AUTO should resolve provider");
        assert_eq!(resolved.base_url, "https://primary.test/v1");
        assert_eq!(resolved.provider_id.as_deref(), Some("codex-primary"));
    }

    #[tokio::test]
    async fn resolve_provider_config_explicit_message_provider_overrides_auto() {
        let state = test_state("provider-config-explicit").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: AiApprovalSettings::default(),
                model_providers: Some(vec![
                    ModelProviderConfig {
                        id: "codex-primary".to_string(),
                        name: "Codex Primary".to_string(),
                        base_url: "https://primary.test/v1".to_string(),
                        api_key: "key-1".to_string(),
                        model: Some("gpt-4.1".to_string()),
                        format: ApiFormat::Codex,
                        enabled: true,
                        priority: 0,
                    },
                    ModelProviderConfig {
                        id: "codex-explicit".to_string(),
                        name: "Codex Explicit".to_string(),
                        base_url: "https://explicit.test/v1".to_string(),
                        api_key: "key-explicit".to_string(),
                        model: Some("gpt-4.1-explicit".to_string()),
                        format: ApiFormat::Codex,
                        enabled: true,
                        priority: 20,
                    },
                ]),
                speech_profiles: None,
                speech_voices: None,
                speaker_filter: None,
            })
            .await
            .expect("settings update should succeed");

        let session = SessionSummary {
            id: "session-3".to_string(),
            project_id: "project-1".to_string(),
            title: "Session".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            provider_id: Some(AUTO_PROVIDER_ID.to_string()),
            reasoning_effort: None,
        };

        let resolved = state
            .resolve_provider_config(&session, &Some("codex-explicit".to_string()))
            .await;
        let resolved = resolved.expect("explicit provider should resolve");
        assert_eq!(resolved.base_url, "https://explicit.test/v1");
        assert_eq!(resolved.provider_id.as_deref(), Some("codex-explicit"));
    }
}
