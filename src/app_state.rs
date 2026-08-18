use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, RwLock, broadcast, mpsc},
    task::AbortHandle,
};
use uuid::Uuid;

tokio::task_local! {
    pub(crate) static DOMAIN_TURN_ID: String;
}

tokio::task_local! {
    pub(crate) static APPROVAL_LANGUAGE: String;
}

use crate::{
    adapter::ProviderRegistry,
    bridge_settings::{BridgeSettings, BridgeSettingsStore},
    client_auth_store::ClientAuthStore,
    debug_log,
    device_store::{load_device_registrations, save_device_registrations},
    message_projection::MessageProjection,
    models::{
        AUTO_PROVIDER_ID, AgentErrorEvent, AgentKind, ApiFormat, ApprovalChoice, ApprovalRequest,
        ApprovalRequestEvent, ApprovalResolvedEvent, ChatMessage, ClientAuthRecord,
        ClientAuthRequestInput, ClientAuthStatus, CreateProjectInput, CreateSessionInput,
        GitStatusDetail, InputMode, MessageRole, MessageSnapshotEvent, ModelProviderConfig,
        ProjectGitStatus, ProjectSummary, PushDeviceRegistration, RegisterPushDeviceInput,
        ResolvedProviderConfig, SendMessageInput, SessionDetail, SessionDiffEvent, SessionEvent,
        SessionStatus, SessionStatusEvent, SessionSummary, TriggerClientMessageInput,
        TriggerClientMessageResult,
    },
    push::PushService,
    secret_store::SecretStore,
    session_domain::{
        ActivityKind, AgentProjection, ArtifactKind, CreateTurnCommand, CreateTurnResult,
        DomainSessionStatus, EntityState, MessagePurpose, SessionDomainEvent, SessionState,
        TurnStatus,
    },
    session_domain_store::{DomainStoreError, SessionDomainStore},
    session_store::project_id_for_path,
};

#[derive(Default)]
struct SessionRuntimeState {
    current_turn_id: Option<String>,
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
    cancel_tx: Option<mpsc::UnboundedSender<()>>,
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
    #[serde(default)]
    projects: HashMap<String, ProjectSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedSessionMetadataEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session: Option<SessionSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_unread_message_id: Option<String>,
    #[serde(default)]
    diffs: Vec<SessionDiffEvent>,
    #[serde(default)]
    system_messages: Vec<ChatMessage>,
    #[serde(default)]
    user_messages: Vec<ChatMessage>,
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
            current_turn_id: None,
            provider_session_ref: value.provider_session_ref,
            codex_provider_name: value.codex_provider_name,
            codex_model: value.codex_model,
            claude_provider_id: value.claude_provider_id,
            claude_model: value.claude_model,
            pending_approval,
            last_resolved_approval_request_id: None,
            approval_tx: None,
            cancel_tx: None,
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

fn provider_session_ref_chain_from_runtime(
    runtime: &HashMap<String, SessionRuntimeState>,
    session_id: &str,
) -> Vec<String> {
    let refs = runtime
        .iter()
        .filter_map(|(id, state)| {
            state
                .provider_session_ref
                .as_ref()
                .map(|session_ref| (id.clone(), session_ref.clone()))
        })
        .collect::<HashMap<_, _>>();
    provider_session_ref_chain_from_refs(&refs, session_id)
}

fn provider_session_ref_chain_from_refs(
    refs: &HashMap<String, String>,
    session_id: &str,
) -> Vec<String> {
    let mut chain = Vec::new();
    let mut visited = std::collections::HashSet::from([session_id.to_string()]);
    let mut current = session_id;
    while let Some(next) = refs.get(current) {
        if !visited.insert(next.clone()) {
            break;
        }
        chain.push(next.clone());
        current = next;
    }
    chain
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

pub struct AppState {
    projects: RwLock<HashMap<String, ProjectSummary>>,
    sessions: RwLock<HashMap<String, SessionSummary>>,
    session_title_overrides: RwLock<HashMap<String, String>>,
    unread_message_ids: RwLock<HashMap<String, String>>,
    messages: RwLock<HashMap<String, Vec<ChatMessage>>>,
    session_diffs: RwLock<HashMap<String, Vec<SessionDiffEvent>>>,
    metadata_write_lock: Mutex<()>,
    client_message_results: RwLock<HashMap<(String, String), (ChatMessage, ChatMessage)>>,
    devices: RwLock<HashMap<String, PushDeviceRegistration>>,
    list_cache: Mutex<Option<AggregatedListCache>>,
    runtime: Mutex<HashMap<String, SessionRuntimeState>>,
    event_tx: broadcast::Sender<SequencedSessionEvent>,
    event_stream: StdMutex<EventStreamState>,
    providers: ProviderRegistry,
    settings: BridgeSettingsStore,
    client_auth: ClientAuthStore,
    secret_store: SecretStore,
    push: PushService,
    session_domain: Arc<SessionDomainStore>,
    pub(crate) pi_plugins: crate::pi_plugin_store::PiPluginStore,
    runtime_store_path: PathBuf,
    session_metadata_store_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SequencedSessionEvent {
    pub id: u64,
    pub event: SessionEvent,
}

struct EventStreamState {
    next_event_id: u64,
    log: VecDeque<SequencedSessionEvent>,
}

pub enum EventReplay {
    Events(Vec<SequencedSessionEvent>),
    SyncRequired,
}

pub struct EventSubscription {
    pub receiver: broadcast::Receiver<SequencedSessionEvent>,
    pub replay: EventReplay,
    pub high_watermark: u64,
}

#[derive(Debug)]
pub enum MarkSessionReadError {
    NotFound(String),
    Persistence(String),
}

impl AppState {
    const LIST_CACHE_TTL: Duration = Duration::from_secs(5);
    const MAX_PERSISTED_DIFF_CHARS: usize = 200_000;
    const STALE_RUNNING_SESSION_DAYS: i64 = 7;

    #[allow(dead_code)]
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

    pub async fn new_strict() -> anyhow::Result<Self> {
        let settings_path = crate::bridge_settings::settings_path();
        let runtime_store_path = runtime_store_path();
        let session_metadata_store_path = session_metadata_store_path();
        Self::new_with_paths_strict(
            settings_path,
            runtime_store_path,
            session_metadata_store_path,
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn new_with_paths(
        settings_path: PathBuf,
        runtime_store_path: PathBuf,
        session_metadata_store_path: PathBuf,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(512);
        let persisted_runtime = load_persisted_runtime(&runtime_store_path).await;
        let persisted_metadata =
            load_persisted_session_metadata(&session_metadata_store_path).await;
        let session_domain = Arc::new(
            SessionDomainStore::open(&domain_store_path(&session_metadata_store_path))
                .expect("session domain store should open"),
        );
        let state = Self {
            projects: RwLock::new(persisted_metadata.projects.clone()),
            sessions: RwLock::new(
                persisted_metadata
                    .sessions
                    .values()
                    .filter_map(|entry| entry.session.clone())
                    .map(|session| (session.id.clone(), session))
                    .collect(),
            ),
            session_title_overrides: RwLock::new(
                persisted_metadata
                    .sessions
                    .clone()
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
            unread_message_ids: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(session_id, entry)| {
                        entry
                            .last_unread_message_id
                            .as_ref()
                            .map(|message_id| (session_id.clone(), message_id.clone()))
                    })
                    .collect(),
            ),
            messages: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(id, entry)| {
                        let mut messages = entry.user_messages.clone();
                        messages.extend(entry.system_messages.clone());
                        (!messages.is_empty()).then(|| (id.clone(), messages))
                    })
                    .collect(),
            ),
            session_diffs: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .map(|(id, entry)| (id.clone(), entry.diffs.clone()))
                    .collect(),
            ),
            metadata_write_lock: Mutex::new(()),
            client_message_results: RwLock::new(HashMap::new()),
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
            event_stream: StdMutex::new(EventStreamState {
                next_event_id: 1,
                log: VecDeque::new(),
            }),
            providers: ProviderRegistry::new(),
            settings: BridgeSettingsStore::load_from_path(settings_path).await,
            client_auth: ClientAuthStore::load().await,
            secret_store: SecretStore::load().await,
            push: PushService::new(),
            session_domain,
            pi_plugins: crate::pi_plugin_store::PiPluginStore::new(pi_plugin_store_path(
                &session_metadata_store_path,
            )),
            runtime_store_path,
            session_metadata_store_path,
        };
        state.ensure_domain_sessions().await;
        if let Err(error) = state.recover_domain_active_turns().await {
            debug_log!("[domain] failed to recover active turns: {error}");
        }
        state.interrupt_stale_running_sessions(Utc::now()).await;
        state
    }

    #[cfg(test)]
    pub(crate) async fn new_with_paths_and_providers(
        settings_path: PathBuf,
        runtime_store_path: PathBuf,
        session_metadata_store_path: PathBuf,
        providers: ProviderRegistry,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(512);
        let persisted_runtime = load_persisted_runtime(&runtime_store_path).await;
        let persisted_metadata =
            load_persisted_session_metadata(&session_metadata_store_path).await;
        let session_domain = Arc::new(
            SessionDomainStore::open(&domain_store_path(&session_metadata_store_path))
                .expect("session domain store should open"),
        );
        let state = Self {
            projects: RwLock::new(persisted_metadata.projects.clone()),
            sessions: RwLock::new(
                persisted_metadata
                    .sessions
                    .values()
                    .filter_map(|entry| entry.session.clone())
                    .map(|session| (session.id.clone(), session))
                    .collect(),
            ),
            session_title_overrides: RwLock::new(
                persisted_metadata
                    .sessions
                    .clone()
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
            unread_message_ids: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(session_id, entry)| {
                        entry
                            .last_unread_message_id
                            .as_ref()
                            .map(|message_id| (session_id.clone(), message_id.clone()))
                    })
                    .collect(),
            ),
            messages: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(id, entry)| {
                        let mut messages = entry.user_messages.clone();
                        messages.extend(entry.system_messages.clone());
                        (!messages.is_empty()).then(|| (id.clone(), messages))
                    })
                    .collect(),
            ),
            session_diffs: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .map(|(id, entry)| (id.clone(), entry.diffs.clone()))
                    .collect(),
            ),
            metadata_write_lock: Mutex::new(()),
            client_message_results: RwLock::new(HashMap::new()),
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
            event_stream: StdMutex::new(EventStreamState {
                next_event_id: 1,
                log: VecDeque::new(),
            }),
            providers,
            settings: BridgeSettingsStore::load_from_path(settings_path).await,
            client_auth: ClientAuthStore::load().await,
            secret_store: SecretStore::load().await,
            push: PushService::new(),
            session_domain,
            pi_plugins: crate::pi_plugin_store::PiPluginStore::new(pi_plugin_store_path(
                &session_metadata_store_path,
            )),
            runtime_store_path,
            session_metadata_store_path,
        };
        state.ensure_domain_sessions().await;
        if let Err(error) = state.recover_domain_active_turns().await {
            debug_log!("[domain] failed to recover active turns: {error}");
        }
        state.interrupt_stale_running_sessions(Utc::now()).await;
        state
    }

    pub(crate) async fn new_with_paths_strict(
        settings_path: PathBuf,
        runtime_store_path: PathBuf,
        session_metadata_store_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let (event_tx, _) = broadcast::channel(512);
        let persisted_runtime = load_persisted_runtime(&runtime_store_path).await;
        let persisted_metadata =
            load_persisted_session_metadata(&session_metadata_store_path).await;
        let session_domain = Arc::new(
            SessionDomainStore::open(&domain_store_path(&session_metadata_store_path))
                .map_err(anyhow::Error::msg)?,
        );
        let state = Self {
            projects: RwLock::new(persisted_metadata.projects.clone()),
            sessions: RwLock::new(
                persisted_metadata
                    .sessions
                    .values()
                    .filter_map(|entry| entry.session.clone())
                    .map(|session| (session.id.clone(), session))
                    .collect(),
            ),
            session_title_overrides: RwLock::new(
                persisted_metadata
                    .sessions
                    .clone()
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
            unread_message_ids: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(session_id, entry)| {
                        entry
                            .last_unread_message_id
                            .as_ref()
                            .map(|message_id| (session_id.clone(), message_id.clone()))
                    })
                    .collect(),
            ),
            messages: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .filter_map(|(id, entry)| {
                        let mut messages = entry.user_messages.clone();
                        messages.extend(entry.system_messages.clone());
                        (!messages.is_empty()).then(|| (id.clone(), messages))
                    })
                    .collect(),
            ),
            session_diffs: RwLock::new(
                persisted_metadata
                    .sessions
                    .iter()
                    .map(|(id, entry)| (id.clone(), entry.diffs.clone()))
                    .collect(),
            ),
            metadata_write_lock: Mutex::new(()),
            client_message_results: RwLock::new(HashMap::new()),
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
            event_stream: StdMutex::new(EventStreamState {
                next_event_id: 1,
                log: VecDeque::new(),
            }),
            providers: ProviderRegistry::new(),
            settings: BridgeSettingsStore::load_from_path_strict(settings_path).await?,
            client_auth: ClientAuthStore::load().await,
            secret_store: SecretStore::load().await,
            push: PushService::new(),
            session_domain,
            pi_plugins: crate::pi_plugin_store::PiPluginStore::new(pi_plugin_store_path(
                &session_metadata_store_path,
            )),
            runtime_store_path,
            session_metadata_store_path,
        };
        state.ensure_domain_sessions().await;
        state
            .recover_domain_active_turns()
            .await
            .map_err(anyhow::Error::msg)?;
        state.interrupt_stale_running_sessions(Utc::now()).await;
        Ok(state)
    }

    pub async fn is_runtime_client_id_allowed(&self, client_id: &str) -> bool {
        self.client_auth.has_approved_client_id(client_id).await
    }

    async fn ensure_domain_sessions(&self) {
        let sessions = self
            .sessions
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for session in sessions {
            if let Err(error) = self.session_domain.ensure_session(&session) {
                debug_log!("[domain] failed to import session={}: {error}", session.id);
            } else if let Err(error) = self.session_domain.sync_session_metadata(&session) {
                debug_log!("[domain] failed to sync session={}: {error}", session.id);
            }
        }
    }

    async fn recover_domain_active_turns(&self) -> Result<(), String> {
        let interrupted = self
            .session_domain
            .interrupt_active_turns()?
            .into_iter()
            .collect::<HashSet<_>>();
        let legacy_active_session_ids = self
            .sessions
            .read()
            .await
            .values()
            .filter(|session| {
                matches!(
                    session.status,
                    SessionStatus::Running
                        | SessionStatus::AwaitingApproval
                        | SessionStatus::Waiting
                ) || session.pending_approval.is_some()
            })
            .map(|session| session.id.clone())
            .collect::<Vec<_>>();
        for session_id in legacy_active_session_ids {
            let Some(domain_state) = self.session_domain.session_state(&session_id)? else {
                continue;
            };
            if domain_state.session.active_turn_id.is_some() {
                continue;
            }
            let recovered_status = if interrupted.contains(&session_id) {
                SessionStatus::Interrupted
            } else {
                match domain_state.session.status {
                    DomainSessionStatus::Idle => SessionStatus::Idle,
                    DomainSessionStatus::Failed => SessionStatus::Failed,
                    DomainSessionStatus::Running | DomainSessionStatus::AwaitingApproval => {
                        continue;
                    }
                }
            };
            let was_interrupted = matches!(recovered_status, SessionStatus::Interrupted);
            let summary = {
                let mut sessions = self.sessions.write().await;
                let Some(session) = sessions.get_mut(&session_id) else {
                    continue;
                };
                session.status = recovered_status;
                session.pending_approval = None;
                session.last_message_preview = domain_state.session.last_message_preview;
                session.updated_at = Utc::now();
                session.clone()
            };
            self.update_runtime_state(&session_id, |runtime| {
                runtime.interrupted = was_interrupted;
                runtime.turn_in_flight = false;
                runtime.turn_abort = None;
                runtime.cancel_tx = None;
                runtime.approval_tx = None;
                runtime.pending_approval = None;
            })
            .await;
            self.persist_canonical_session(&summary).await?;
            self.publish_event(SessionEvent::SessionSnapshot(summary));
        }
        self.invalidate_list_cache().await;
        Ok(())
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

    #[cfg(test)]
    pub async fn approve_client_auth_for_test(
        &self,
        request_id: &str,
    ) -> Result<ClientAuthRecord, String> {
        self.client_auth
            .approve(request_id)
            .await
            .map_err(|error| error.to_string())
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
        if let Some(ref servers) = input.acp_servers {
            crate::bridge_settings::validate_acp_servers(servers)?;
        }

        self.settings
            .update(|settings| {
                if let Some(ai_approval) = input.ai_approval {
                    settings.ai_approval = ai_approval;
                }
                if let Some(model_providers) = input.model_providers {
                    settings.model_providers = model_providers;
                }
                if let Some(acp_servers) = input.acp_servers {
                    settings.acp_servers = acp_servers;
                }
            })
            .await
            .map_err(|error| error.to_string())
    }

    pub fn settings_store(&self) -> &BridgeSettingsStore {
        &self.settings
    }

    pub async fn update_ai_approval_prompt(&self, prompt: String) -> Result<String, String> {
        let saved_prompt = prompt.trim().to_string();
        let result = saved_prompt.clone();
        self.settings
            .update(|settings| settings.ai_approval.prompt = saved_prompt)
            .await
            .map_err(|error| error.to_string())?;
        Ok(result)
    }

    pub async fn project_ai_approval_settings(
        &self,
        project_id: &str,
    ) -> Result<crate::bridge_settings::ProjectAiApprovalSettings, String> {
        let project = self
            .find_project(project_id)
            .await
            .ok_or_else(|| format!("unknown project: {project_id}"))?;
        Ok(self
            .settings
            .get()
            .await
            .project_ai_approval
            .get(&project.root_path)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn update_project_ai_approval_settings(
        &self,
        project_id: &str,
        input: crate::bridge_settings::ProjectAiApprovalInput,
    ) -> Result<crate::bridge_settings::ProjectAiApprovalSettings, String> {
        let project = self
            .find_project(project_id)
            .await
            .ok_or_else(|| format!("unknown project: {project_id}"))?;
        let project_root = project.root_path;
        let lookup_root = project_root.clone();
        let updated = self
            .settings
            .update(|settings| {
                let project_settings = settings
                    .project_ai_approval
                    .entry(project_root)
                    .or_default();
                project_settings.prompt = input.prompt;
            })
            .await
            .map_err(|error| error.to_string())?;
        Ok(updated
            .project_ai_approval
            .get(&lookup_root)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn remember_ai_approval_rule(
        &self,
        project_root: &std::path::Path,
        command: &str,
    ) -> Result<(), String> {
        let project_root = project_root.to_string_lossy().to_string();
        let command = command.trim().to_string();
        if command.is_empty() {
            return Ok(());
        }
        self.settings
            .update(|settings| {
                let project = settings
                    .project_ai_approval
                    .entry(project_root)
                    .or_default();
                let rule = always_allow_prompt_rule(&command);
                if !project.prompt.lines().any(|line| line.trim() == rule) {
                    if !project.prompt.trim().is_empty() {
                        project.prompt.push_str("\n\n");
                    }
                    project.prompt.push_str(&rule);
                }
            })
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn secret_store(&self) -> &SecretStore {
        &self.secret_store
    }

    pub async fn list_projects(&self) -> Vec<ProjectSummary> {
        self.ensure_list_cache().await.projects
    }

    async fn interrupt_stale_running_sessions(&self, now: DateTime<Utc>) -> usize {
        let cutoff = now - chrono::TimeDelta::days(Self::STALE_RUNNING_SESSION_DAYS);
        let stale_session_ids = self
            .ensure_list_cache()
            .await
            .sessions
            .into_iter()
            .filter(|session| {
                matches!(session.status, SessionStatus::Running) && session.updated_at < cutoff
            })
            .map(|session| session.id)
            .collect::<Vec<_>>();
        if stale_session_ids.is_empty() {
            return 0;
        }

        let snapshot = {
            let mut runtime = self.runtime.lock().await;
            for session_id in &stale_session_ids {
                let entry = runtime
                    .entry(session_id.clone())
                    .or_insert_with(SessionRuntimeState::default);
                entry.interrupted = true;
                entry.turn_in_flight = false;
                entry.turn_abort = None;
                entry.cancel_tx = None;
                entry.approval_tx = None;
                entry.pending_approval = None;
            }
            runtime
                .iter()
                .map(|(session_id, state)| (session_id.clone(), state.to_persisted()))
                .collect::<HashMap<_, _>>()
        };
        if let Err(error) = write_persisted_runtime(&self.runtime_store_path, &snapshot).await {
            eprintln!(
                "failed to persist stale session interruption state at {}: {error}",
                self.runtime_store_path.display()
            );
        }
        self.invalidate_list_cache().await;
        debug_log!(
            "[startup] interrupted {} session(s) still running after more than {} days",
            stale_session_ids.len(),
            Self::STALE_RUNNING_SESSION_DAYS
        );
        stale_session_ids.len()
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
        let local_session_ids = self
            .sessions
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let runtime = self.runtime.lock().await;
        for session in &mut sessions {
            Self::apply_runtime_overlay(
                session,
                runtime.get(&session.id),
                local_session_ids.contains(&session.id),
            );
        }
        sessions
    }

    async fn with_runtime_approval(&self, mut session: SessionSummary) -> SessionSummary {
        session = self.overlay_persisted_session_title(session).await;
        let is_local_session = self.sessions.read().await.contains_key(&session.id);
        let runtime = self.runtime.lock().await;
        let session_id = session.id.clone();
        Self::apply_runtime_overlay(&mut session, runtime.get(&session_id), is_local_session);
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

    fn apply_runtime_overlay(
        session: &mut SessionSummary,
        runtime: Option<&SessionRuntimeState>,
        is_local_session: bool,
    ) {
        session.pending_approval = runtime.and_then(|entry| entry.pending_approval.clone());
        session.runtime_session_ref = runtime
            .and_then(|entry| entry.provider_session_ref.clone())
            .or_else(|| (!is_local_session).then(|| session.id.clone()));
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
        let session = self.find_session(session_id).await?;
        let provider_messages = if let Some(provider) = self.provider_for_agent(session.agent) {
            let mut provider_session_ids = vec![session_id.to_string()];
            provider_session_ids.extend(self.provider_session_ref_chain(session_id).await);
            let mut combined = Vec::new();
            let mut found = false;
            for provider_session_id in provider_session_ids {
                if let Some(messages) = provider.list_messages(&provider_session_id).await {
                    found = true;
                    combined = MessageProjection::from_sources(session_id, messages, combined);
                }
            }
            found.then(|| MessageProjection::normalize_provider(session_id, combined))
        } else {
            None
        };
        let mut message_cache = self.messages.write().await;
        let cached = message_cache.get(session_id).cloned();

        let messages = match (cached, provider_messages) {
            (Some(local), Some(remote)) => {
                let merged = MessageProjection::from_sources(session_id, remote, local);
                message_cache.insert(session_id.to_string(), merged.clone());
                merged
            }
            (Some(local), None) => local,
            (None, Some(remote)) => {
                message_cache.insert(session_id.to_string(), remote.clone());
                remote
            }
            (None, None) => Vec::new(),
        };
        Some(sort_messages(messages))
    }

    pub async fn codex_turn_can_retry(&self, session_id: &str, message_id: &str) -> bool {
        let no_message_progress = self
            .messages
            .read()
            .await
            .get(session_id)
            .and_then(|messages| {
                let reply_index = messages
                    .iter()
                    .position(|message| message.id == message_id)?;
                Some(
                    messages[reply_index].content.trim().is_empty()
                        && messages.len() == reply_index + 1,
                )
            })
            .unwrap_or(false);
        if !no_message_progress {
            return false;
        }
        self.runtime
            .lock()
            .await
            .get(session_id)
            .is_some_and(|runtime| runtime.pending_approval.is_none())
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
            diffs: dedupe_session_diffs(
                self.session_diffs
                    .read()
                    .await
                    .get(session_id)
                    .cloned()
                    .unwrap_or_default(),
            ),
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
        let canonical_id = input.client_session_id.trim().to_string();
        if canonical_id.is_empty() {
            return Err("client_session_id is required".to_string());
        }
        if canonical_id.len() > 128 {
            return Err("client_session_id is too long".to_string());
        }
        if let Some(existing) = self.find_session(&canonical_id).await {
            if existing.project_id != input.project_id || existing.agent != input.agent {
                return Err("client_session_id is already used by another session".to_string());
            }
            return Ok(existing);
        }
        let project = self
            .find_project(&input.project_id)
            .await
            .ok_or_else(|| format!("unknown project: {}", input.project_id))?;

        let session = SessionSummary {
            id: canonical_id,
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
            runtime_session_ref: None,
            provider_id: input.provider_id,
            reasoning_effort: input.reasoning_effort,
            model: input.model,
        };

        {
            let mut sessions = self.sessions.write().await;
            if let Some(existing) = sessions.get(&session.id) {
                if existing.project_id != session.project_id || existing.agent != session.agent {
                    return Err("client_session_id is already used by another session".to_string());
                }
                return Ok(existing.clone());
            }
            sessions.insert(session.id.clone(), session.clone());
        }
        self.messages
            .write()
            .await
            .insert(session.id.clone(), Vec::new());
        self.runtime.lock().await.insert(
            session.id.clone(),
            SessionRuntimeState {
                current_turn_id: None,
                provider_session_ref: None,
                codex_provider_name: None,
                codex_model: None,
                claude_provider_id: None,
                claude_model: None,
                pending_approval: None,
                last_resolved_approval_request_id: None,
                approval_tx: None,
                cancel_tx: None,
                turn_abort: None,
                turn_in_flight: false,
                interrupted: false,
            },
        );
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&session.project_id).await;
        self.persist_canonical_session(&session).await?;
        self.session_domain.ensure_session(&session)?;

        self.publish_event(SessionEvent::SessionSnapshot(session.clone()));

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
            runtime_session_ref: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        let message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: content.clone(),
            created_at: now,
        };

        self.unread_message_ids
            .write()
            .await
            .insert(session.id.clone(), message.id.clone());
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
        self.persist_canonical_session(&session).await?;
        self.invalidate_list_cache().await;
        self.refresh_project_summary(&session.project_id).await;

        self.publish_event(SessionEvent::SessionSnapshot(session.clone()));
        self.publish_event(SessionEvent::MessageCreated(message.clone()));

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

    #[cfg(test)]
    pub fn subscribe(&self) -> broadcast::Receiver<SequencedSessionEvent> {
        self.event_tx.subscribe()
    }

    pub fn subscribe_with_replay(&self, event_id: Option<u64>) -> EventSubscription {
        let Ok(stream) = self.event_stream.lock() else {
            return EventSubscription {
                receiver: self.event_tx.subscribe(),
                replay: EventReplay::SyncRequired,
                high_watermark: 0,
            };
        };
        let receiver = self.event_tx.subscribe();
        let high_watermark = stream.next_event_id.saturating_sub(1);
        let replay = event_id
            .map(|event_id| replay_events_from_stream(&stream, event_id))
            .unwrap_or_else(|| EventReplay::Events(Vec::new()));
        EventSubscription {
            receiver,
            replay,
            high_watermark,
        }
    }

    pub async fn domain_session_state(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionState>, String> {
        if let Some(state) = self.session_domain.session_state(session_id)?
            && !state.turns.is_empty()
            && !self
                .session_domain
                .needs_legacy_history_reimport(session_id)?
        {
            if state.session.agent == AgentKind::Codex
                && state.session.status == DomainSessionStatus::Idle
                && let Some(messages) = self.list_messages(session_id).await
                && completed_codex_history_is_ready(&state, &messages)
            {
                self.session_domain
                    .refresh_completed_assistant_history(session_id, &messages)?;
            }
            let diffs = self
                .session_diffs
                .read()
                .await
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            self.session_domain
                .import_diff_history(session_id, &diffs)?;
            return self.session_domain.session_state(session_id);
        }
        let session = if let Some(session) = self.find_session(session_id).await {
            session
        } else {
            self.invalidate_list_cache().await;
            let Some(session) = self.find_session(session_id).await else {
                return Ok(None);
            };
            session
        };
        self.session_domain.ensure_session(&session)?;
        self.session_domain.sync_session_metadata(&session)?;
        if let Some(messages) = self.list_messages(session_id).await {
            self.session_domain
                .import_message_history(session_id, &messages)?;
        }
        let diffs = self
            .session_diffs
            .read()
            .await
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        self.session_domain
            .import_diff_history(session_id, &diffs)?;
        self.session_domain.session_state(session_id)
    }

    pub async fn list_domain_session_summaries(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<SessionSummary>, String> {
        // The domain list is used as the synchronization snapshot. Refresh the
        // provider-backed aggregate so sessions discovered after startup are included.
        self.invalidate_list_cache().await;
        let legacy_sessions = self.list_sessions().await;
        for session in &legacy_sessions {
            self.session_domain.ensure_session(&session)?;
            self.session_domain.sync_session_metadata(&session)?;
        }
        let domain_sessions = self.session_domain.list_sessions(project_id)?;
        let legacy = legacy_sessions
            .into_iter()
            .map(|session| (session.id.clone(), session))
            .collect::<HashMap<_, _>>();
        let runtime = self.runtime.lock().await;
        let mut summaries = domain_sessions
            .into_iter()
            .filter_map(|domain| {
                let base = legacy.get(&domain.id)?;
                let pending_approval = runtime
                    .get(&domain.id)
                    .and_then(|entry| entry.pending_approval.clone());
                Some(SessionSummary {
                    id: domain.id,
                    project_id: domain.project_id,
                    title: domain.title,
                    agent: domain.agent,
                    brief_reply_mode: domain.config.brief_reply_mode,
                    status: match domain.status {
                        DomainSessionStatus::Idle => SessionStatus::Idle,
                        DomainSessionStatus::Running => SessionStatus::Running,
                        DomainSessionStatus::AwaitingApproval => SessionStatus::AwaitingApproval,
                        DomainSessionStatus::Failed => SessionStatus::Failed,
                    },
                    // The provider/canonical summary is the source of truth for
                    // archived-session activity. Domain timestamps can reflect a
                    // local import or projection rather than a conversation update.
                    updated_at: base.updated_at,
                    unread_count: domain.unread_count,
                    last_message_preview: domain.last_message_preview,
                    pending_approval,
                    runtime_session_ref: base.runtime_session_ref.clone(),
                    provider_id: domain.config.provider_id,
                    reasoning_effort: domain.config.reasoning_effort,
                    model: domain.config.model,
                })
            })
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(summaries)
    }

    pub async fn create_domain_turn(
        &self,
        session_id: &str,
        command: &CreateTurnCommand,
    ) -> Result<CreateTurnResult, DomainStoreError> {
        self.domain_session_state(session_id)
            .await
            .map_err(DomainStoreError::Storage)?
            .ok_or_else(|| DomainStoreError::NotFound("session not found".to_string()))?;
        self.session_domain.create_turn(session_id, command)
    }

    pub async fn mark_domain_session_read(&self, session_id: &str) -> Result<(), String> {
        self.domain_session_state(session_id)
            .await?
            .ok_or_else(|| "session not found".to_string())?;
        self.clear_session_unread(session_id).await
    }

    pub fn domain_events_after(
        &self,
        session_id: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<SessionDomainEvent>, String> {
        self.session_domain.events_after(session_id, after, limit)
    }

    pub fn subscribe_domain_events(&self) -> broadcast::Receiver<SessionDomainEvent> {
        self.session_domain.subscribe()
    }

    pub fn domain_event_snapshot(
        &self,
        session_id: &str,
        turn_id: Option<&str>,
    ) -> Result<
        Option<(
            crate::session_domain::DomainSession,
            Option<crate::session_domain::Turn>,
        )>,
        String,
    > {
        self.session_domain.event_snapshot(session_id, turn_id)
    }

    fn try_project_agent_event(
        &self,
        session_id: &str,
        event: AgentProjection,
    ) -> Result<(), String> {
        let turn_id = DOMAIN_TURN_ID.try_with(Clone::clone);
        let Ok(turn_id) = turn_id else {
            return Ok(());
        };
        self.session_domain
            .project_agent_event(session_id, &turn_id, event)
    }

    fn project_agent_event(&self, session_id: &str, event: AgentProjection) {
        if let Err(error) = self.try_project_agent_event(session_id, event) {
            debug_log!("[domain] failed to project event for session={session_id}: {error}");
        }
    }

    fn bound_domain_turn_is_active(&self, session_id: &str) -> bool {
        let Ok(expected_turn_id) = DOMAIN_TURN_ID.try_with(Clone::clone) else {
            return true;
        };
        let active_turn_id = self
            .session_domain
            .session_state(session_id)
            .ok()
            .flatten()
            .and_then(|state| state.session.active_turn_id);
        let is_active = active_turn_id.as_deref() == Some(expected_turn_id.as_str());
        if !is_active {
            debug_log!(
                "[domain] ignored stale provider callback for session={session_id}, turn={expected_turn_id}"
            );
        }
        is_active
    }

    fn project_active_turn_event(&self, session_id: &str, event: AgentProjection) {
        let turn_id = self
            .session_domain
            .session_state(session_id)
            .ok()
            .flatten()
            .and_then(|state| state.session.active_turn_id);
        let Some(turn_id) = turn_id else {
            return;
        };
        if let Err(error) = self
            .session_domain
            .project_agent_event(session_id, &turn_id, event)
        {
            debug_log!(
                "[domain] failed to project control event for session={session_id}: {error}"
            );
        }
    }

    pub fn publish_event(&self, event: SessionEvent) {
        const EVENT_LOG_CAPACITY: usize = 512;
        let Ok(mut stream) = self.event_stream.lock() else {
            return;
        };
        let sequenced = SequencedSessionEvent {
            id: stream.next_event_id,
            event,
        };
        stream.next_event_id += 1;
        stream.log.push_back(sequenced.clone());
        while stream.log.len() > EVENT_LOG_CAPACITY {
            stream.log.pop_front();
        }
        let _ = self.event_tx.send(sequenced);
    }

    #[cfg(test)]
    pub fn replay_events_after(&self, event_id: u64) -> EventReplay {
        let Ok(stream) = self.event_stream.lock() else {
            return EventReplay::SyncRequired;
        };
        replay_events_from_stream(&stream, event_id)
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
                    || *format == ApiFormat::Acp
            }
            AgentKind::Acp => *format == ApiFormat::Acp,
            AgentKind::Pi => {
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
                    .map(|p| {
                        let mut config = ResolvedProviderConfig {
                            base_url: p.base_url.clone(),
                            api_key: p.api_key.clone(),
                            model: p.model.clone(),
                            format: p.format,
                            acp_profile: None,
                            provider_id: Some(p.id.clone()),
                            extra_headers: Vec::new(),
                            acp_command: None,
                            acp_args: Vec::new(),
                            acp_env: Vec::new(),
                            opencode_provider_name: match p.format {
                                ApiFormat::AnthropicMessages => {
                                    Some("omni-bridge-anthropic".to_string())
                                }
                                ApiFormat::Codex => Some("omni-bridge-codex".to_string()),
                                ApiFormat::Acp => Some("omni-bridge-acp".to_string()),
                                _ => Some("omni-bridge".to_string()),
                            },
                        };
                        if let Some(model) = &session.model {
                            config.model = Some(model.clone());
                        }
                        config
                    })
            };

        let find_best_acp_server = || -> Option<ResolvedProviderConfig> {
            settings
                .acp_servers
                .iter()
                .filter(|server| server.enabled)
                .min_by_key(|server| server.priority)
                .map(|server| {
                    let mut config = ResolvedProviderConfig {
                        base_url: server.endpoint.clone().unwrap_or_default(),
                        api_key: server.auth_token.clone(),
                        model: server.default_model.clone(),
                        format: ApiFormat::Acp,
                        acp_profile: Some(server.profile),
                        provider_id: Some(server.id.clone()),
                        extra_headers: server
                            .headers
                            .iter()
                            .map(|header| (header.key.clone(), header.value.clone()))
                            .collect(),
                        acp_command: server.command.clone(),
                        acp_args: server.args.clone(),
                        acp_env: server
                            .env
                            .iter()
                            .map(|entry| (entry.key.clone(), entry.value.clone()))
                            .collect(),
                        opencode_provider_name: Some("omni-bridge-acp".to_string()),
                    };
                    if let Some(model) = &session.model {
                        config.model = Some(model.clone());
                    }
                    config
                })
        };

        // Message/session provider handling:
        // - missing provider_id => do not resolve any bridge provider
        // - "AUTO" => use priority-based auto-selection
        // - explicit id => direct lookup, no fallback
        if let Some(provider_id) = requested_provider_id {
            if provider_id != AUTO_PROVIDER_ID {
                debug_log!("[provider] looking up explicit provider_id={provider_id}");
                if agent == AgentKind::Acp {
                    if let Some(config) = settings
                        .acp_servers
                        .iter()
                        .find(|server| server.id == provider_id)
                        .map(|server| {
                            let mut config = ResolvedProviderConfig {
                                base_url: server.endpoint.clone().unwrap_or_default(),
                                api_key: server.auth_token.clone(),
                                model: server.default_model.clone(),
                                format: ApiFormat::Acp,
                                acp_profile: Some(server.profile),
                                provider_id: Some(server.id.clone()),
                                extra_headers: server
                                    .headers
                                    .iter()
                                    .map(|header| (header.key.clone(), header.value.clone()))
                                    .collect(),
                                acp_command: server.command.clone(),
                                acp_args: server.args.clone(),
                                acp_env: server
                                    .env
                                    .iter()
                                    .map(|entry| (entry.key.clone(), entry.value.clone()))
                                    .collect(),
                                opencode_provider_name: Some("omni-bridge-acp".to_string()),
                            };
                            if let Some(model) = &session.model {
                                config.model = Some(model.clone());
                            }
                            config
                        })
                    {
                        return Some(config);
                    }
                }
                if let Some(mut config) = self
                    .find_provider_by_id(&provider_id, agent, &settings)
                    .await
                {
                    if let Some(model) = &session.model {
                        config.model = Some(model.clone());
                    }
                    debug_log!(
                        "[provider] resolved explicit provider: base_url={} model={:?} format={:?}",
                        config.base_url,
                        config.model,
                        config.format
                    );
                    return Some(config);
                }
                eprintln!("[provider] error: provider_id={provider_id} not found in settings");
                return None;
            }
            debug_log!("[provider] explicit AUTO provider requested");
        } else {
            debug_log!(
                "[provider] no provider_id requested for session={}; skipping bridge provider resolution",
                session.id
            );
            return None;
        }

        if agent == AgentKind::Acp {
            return find_best_acp_server();
        }

        // 3. Project-level providers
        if let Some(project_providers) = self.load_project_providers(&session.project_id).await {
            if let Some(config) = find_best_provider(&project_providers) {
                debug_log!(
                    "[provider] using project-level provider for session={}: base_url={} format={:?}",
                    session.id,
                    config.base_url,
                    config.format
                );
                return Some(config);
            }
        }

        // 4. Global providers (sorted by priority)
        let config = find_best_provider(&settings.model_providers);
        if let Some(ref c) = config {
            debug_log!(
                "[provider] using global provider: base_url={} model={:?} format={:?}",
                c.base_url,
                c.model,
                c.format
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
                    debug_log!(
                        "[provider] loaded {} project-level providers from {}",
                        providers.len(),
                        config_path.display()
                    );
                    Some(providers)
                }
                Err(error) => {
                    debug_log!(
                        "[provider] warning: failed to parse {}: {}",
                        config_path.display(),
                        error
                    );
                    None
                }
            },
            Err(error) => {
                debug_log!(
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
                    || provider.format == ApiFormat::Acp
            }
            AgentKind::Acp => provider.format == ApiFormat::Acp,
            AgentKind::Pi => {
                provider.format == ApiFormat::OpenAiCompatible
                    || provider.format == ApiFormat::AnthropicMessages
                    || provider.format == ApiFormat::Codex
            }
            AgentKind::Custom => provider.format == ApiFormat::OpenAiCompatible,
        };

        if !is_compatible {
            debug_log!(
                "[provider] warning: provider {} format {:?} is not compatible with agent {:?}",
                provider_id,
                provider.format,
                agent
            );
            // Allow it anyway but log warning
        }

        Some(ResolvedProviderConfig {
            base_url: provider.base_url.clone(),
            api_key: provider.api_key.clone(),
            model: provider.model.clone(),
            format: provider.format,
            acp_profile: None,
            provider_id: Some(provider_id.to_string()),
            extra_headers: Vec::new(),
            acp_command: None,
            acp_args: Vec::new(),
            acp_env: Vec::new(),
            opencode_provider_name: match provider.format {
                ApiFormat::AnthropicMessages => Some("omni-bridge-anthropic".to_string()),
                ApiFormat::Codex => Some("omni-bridge-codex".to_string()),
                ApiFormat::Acp => Some("omni-bridge-acp".to_string()),
                _ => Some("omni-bridge".to_string()),
            },
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn send_message(
        self: &Arc<Self>,
        session_id: &str,
        input: SendMessageInput,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        self.send_message_with_approval_language(session_id, input, None)
            .await
    }

    pub async fn send_message_with_approval_language(
        self: &Arc<Self>,
        session_id: &str,
        input: SendMessageInput,
        approval_language: Option<String>,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        let domain_turn_id = DOMAIN_TURN_ID.try_with(Clone::clone).ok();
        let client_message_id = input
            .client_message_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if let Some(client_message_id) = &client_message_id {
            if let Some(result) = self
                .client_message_results
                .read()
                .await
                .get(&(session_id.to_string(), client_message_id.clone()))
                .cloned()
            {
                return Ok(result);
            }
        }
        let mut already_processing = false;
        self.update_runtime_state(session_id, |entry| {
            if entry.turn_in_flight {
                already_processing = true;
                return;
            }
            entry.turn_in_flight = true;
            entry.interrupted = false;
            entry.current_turn_id = domain_turn_id.clone();
        })
        .await;
        if already_processing {
            return Err("session is already processing a message".to_string());
        }

        let selected_model = input
            .model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if let Some(model) = selected_model.clone()
            && let Err(error) = self
                .update_session_settings(session_id, None, None, Some(Some(model)))
                .await
        {
            self.finish_turn(session_id).await;
            return Err(error);
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
        if let Err(error) = self.clear_session_unread(session_id).await {
            self.finish_turn(session_id).await;
            return Err(error);
        }

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
            if let Some(client_message_id) = client_message_id {
                self.client_message_results.write().await.insert(
                    (session_id.to_string(), client_message_id),
                    (user_message.clone(), user_message.clone()),
                );
            }
            self.publish_event(SessionEvent::MessageCreated(user_message.clone()));
            self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Idle,
                error_message: None,
            }));
            self.project_agent_event(
                session_id,
                AgentProjection::TurnStatus {
                    status: TurnStatus::Completed,
                    error: None,
                },
            );
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
        debug_log!(
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
        if let Some(client_message_id) = client_message_id {
            self.client_message_results.write().await.insert(
                (session_id.to_string(), client_message_id),
                (user_message.clone(), pending_reply.clone()),
            );
        }
        self.patch_session(
            session_id,
            SessionStatus::Running,
            Some(user_message.content.clone()),
        )
        .await;
        self.clear_pending_approval(session_id).await;
        self.publish_event(SessionEvent::MessageCreated(user_message.clone()));
        self.publish_event(SessionEvent::MessageCreated(pending_reply.clone()));
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::Running,
            error_message: None,
        }));
        self.project_agent_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::Running,
                error: None,
            },
        );
        // Resolve provider configuration
        let provider_config = self
            .resolve_provider_config(&session_snapshot, &input.provider_id)
            .await;
        if let Some(ref config) = provider_config {
            debug_log!(
                "[provider] resolved provider for session={session_id}: base_url={} model={:?} format={:?}",
                config.base_url,
                config.model,
                config.format
            );
        } else {
            self.set_codex_provider_name(session_id, None).await;
            self.set_codex_model(session_id, None).await;
            self.set_claude_provider_id(session_id, None).await;
            self.set_claude_model(session_id, None).await;
        }

        let state = Arc::clone(self);
        let reasoning_effort = input.reasoning_effort.or(session_snapshot.reasoning_effort);
        let Some(provider) = self.provider_for_agent(session_snapshot.agent) else {
            let error = format!("unsupported agent: {:?}", session_snapshot.agent);
            self.fail_session(session_id, error.clone()).await;
            self.finish_turn(session_id).await;
            return Err(error);
        };
        let session_id = session_id.to_string();
        let session_id_for_task = session_id.clone();
        let user_message_for_task = user_message.clone();
        let pending_reply_for_task = pending_reply.clone();
        let domain_turn_id_for_runtime = domain_turn_id.clone();
        let worker_state = Arc::clone(&state);
        let worker_turn_id = domain_turn_id.clone();
        let worker_approval_language = approval_language.clone();
        let worker = tokio::spawn(async move {
            let run = async {
                provider
                    .run_session(
                        worker_state.clone(),
                        session_snapshot,
                        user_message_for_task,
                        system_prompt,
                        pending_reply_for_task,
                        provider_config,
                        reasoning_effort,
                    )
                    .await
            };
            let run = async {
                if let Some(turn_id) = worker_turn_id {
                    DOMAIN_TURN_ID.scope(turn_id, run).await
                } else {
                    run.await
                }
            };
            if let Some(language) = worker_approval_language {
                APPROVAL_LANGUAGE.scope(language, run).await
            } else {
                run.await
            }
        });
        let abort_handle = worker.abort_handle();
        tokio::spawn(async move {
            let cleanup = async {
                match worker.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let message = format!("{error:#}");
                        eprintln!(
                            "[session] session={} failed: {}",
                            session_id_for_task, message
                        );
                        state.fail_session(&session_id_for_task, message).await;
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        let message = format!("provider task terminated unexpectedly: {error}");
                        eprintln!(
                            "[session] session={} failed: {message}",
                            session_id_for_task
                        );
                        state.fail_session(&session_id_for_task, message).await;
                    }
                }
                state.finish_turn(&session_id_for_task).await;
                state.clear_approval_sender(&session_id_for_task).await;
            };
            if let Some(turn_id) = domain_turn_id {
                DOMAIN_TURN_ID.scope(turn_id, cleanup).await;
            } else {
                cleanup.await;
            }
        });
        if let Some(turn_id) = domain_turn_id_for_runtime {
            self.set_turn_abort_for_turn(&session_id, &turn_id, Some(abort_handle))
                .await;
        } else {
            self.set_turn_abort(&session_id, Some(abort_handle)).await;
        }

        Ok((user_message, pending_reply))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn send_domain_message(
        self: &Arc<Self>,
        session_id: &str,
        turn_id: &str,
        input: SendMessageInput,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        DOMAIN_TURN_ID
            .scope(turn_id.to_string(), self.send_message(session_id, input))
            .await
    }

    pub async fn send_domain_message_with_approval_language(
        self: &Arc<Self>,
        session_id: &str,
        turn_id: &str,
        input: SendMessageInput,
        approval_language: Option<String>,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        DOMAIN_TURN_ID
            .scope(
                turn_id.to_string(),
                self.send_message_with_approval_language(session_id, input, approval_language),
            )
            .await
    }

    pub async fn cancel_turn(&self, session_id: &str) -> Result<bool, String> {
        let canonical_session_id = self.resolve_session_id(session_id).await;
        let session_snapshot = self
            .find_session(&canonical_session_id)
            .await
            .ok_or_else(|| format!("unknown session: {session_id}"))?;
        let runtime_session_ref = self.provider_session_ref(&canonical_session_id).await;
        let stale_running_without_runtime = matches!(
            session_snapshot.status,
            SessionStatus::Running | SessionStatus::AwaitingApproval
        );
        let (abort_handle, cancel_tx, should_interrupt, should_try_provider_cancel) = {
            let mut runtime = self.runtime.lock().await;
            let entry = runtime
                .entry(canonical_session_id.clone())
                .or_insert_with(SessionRuntimeState::default);
            let abort_handle = entry.turn_abort.take();
            let cancel_tx = entry.cancel_tx.take();
            let had_active_turn = entry.turn_in_flight || abort_handle.is_some();
            let had_detached_running = Self::is_detached_running(entry);
            let should_interrupt =
                had_active_turn || had_detached_running || stale_running_without_runtime;
            let should_try_provider_cancel = runtime_session_ref.is_some() && !had_active_turn;
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
            (
                abort_handle,
                cancel_tx,
                should_interrupt,
                should_try_provider_cancel,
            )
        };

        if let Some(cancel_tx) = cancel_tx {
            let _ = cancel_tx.send(());
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        if let Some(abort_handle) = abort_handle {
            abort_handle.abort();
        }

        let provider_cancelled = if should_try_provider_cancel {
            match self.provider_for_agent(session_snapshot.agent) {
                Some(provider) => provider
                    .cancel_session(&canonical_session_id, runtime_session_ref.as_deref())
                    .await
                    .map_err(|error| error.to_string())?,
                None => false,
            }
        } else {
            false
        };

        if should_interrupt || provider_cancelled {
            let preview = self
                .latest_assistant_preview(&canonical_session_id)
                .await
                .or(Some("本次运行已中断，可继续发送消息恢复。".to_string()));
            self.patch_session(&canonical_session_id, SessionStatus::Interrupted, preview)
                .await;
            self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: canonical_session_id.clone(),
                status: SessionStatus::Interrupted,
                error_message: None,
            }));
            self.project_active_turn_event(
                &canonical_session_id,
                AgentProjection::TurnStatus {
                    status: TurnStatus::Cancelled,
                    error: None,
                },
            );
        }
        Ok(should_interrupt || provider_cancelled)
    }

    pub async fn submit_approval(
        &self,
        session_id: &str,
        request_id: &str,
        choice: ApprovalChoice,
    ) -> Result<(), String> {
        debug_log!("[approval] submit session={session_id} request={request_id} choice={choice:?}");
        let (sender, always_allow_command) = {
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
            let sender = session.approval_tx.clone().ok_or_else(|| {
                if already_resolved {
                    "approval already resolved".to_string()
                } else {
                    "approval channel is not available".to_string()
                }
            })?;
            let command = matches!(choice, ApprovalChoice::AlwaysAllow)
                .then(|| pending.command.clone())
                .flatten();
            (sender, command)
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

        if let Some(command) = always_allow_command {
            let project_root = self.project_root_path_for_session(session_id).await?;
            self.remember_ai_approval_rule(std::path::Path::new(&project_root), &command)
                .await?;
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

    pub async fn emit_assistant_message_snapshot(
        &self,
        session_id: &str,
        message_id: &str,
        content: &str,
    ) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        let content = content.to_string();
        {
            let mut messages = self.messages.write().await;
            if let Some(message) = messages
                .get_mut(session_id)
                .and_then(|items| items.iter_mut().find(|item| item.id == message_id))
            {
                message.content = content.clone();
            }
        }

        self.publish_event(SessionEvent::MessageSnapshot(MessageSnapshotEvent {
            session_id: session_id.to_string(),
            message_id: message_id.to_string(),
            content: content.clone(),
        }));
        self.project_agent_event(
            session_id,
            AgentProjection::AssistantMessage {
                message_id: message_id.to_string(),
                purpose: MessagePurpose::Final,
                state: EntityState::Running,
                content: content.to_string(),
            },
        );
    }

    pub async fn emit_system_message(&self, session_id: &str, content: impl Into<String>) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        let content = content.into().trim().to_string();
        if content.is_empty() {
            return;
        }

        self.emit_activity(
            session_id,
            classify_activity_kind(&content),
            content.clone(),
            None,
            Vec::new(),
            serde_json::json!({ "raw": content }),
        )
        .await;

        let message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::System,
            content: content.clone(),
            created_at: Utc::now(),
        };
        self.push_message(message.clone()).await;
        if let Err(error) = self.write_session_metadata().await {
            eprintln!("failed to persist session activity: {error}");
        }
        self.publish_event(SessionEvent::MessageCreated(message));
    }

    pub async fn emit_progress(&self, session_id: &str, content: impl Into<String>) {
        let activity = crate::adapter::provider_activity_from_status(content);
        self.emit_provider_activity(session_id, activity).await;
    }

    pub async fn emit_provider_activity(
        &self,
        session_id: &str,
        activity: crate::adapter::ProviderActivity,
    ) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        let content = activity.title.trim().to_string();
        if content.is_empty() {
            return;
        }
        self.emit_activity_with_state(
            session_id,
            activity.correlation_key.as_deref().map(|key| {
                let turn_id = DOMAIN_TURN_ID.try_with(Clone::clone).unwrap_or_default();
                let digest = Sha256::digest(format!("{session_id}\0{turn_id}\0{key}").as_bytes());
                format!("provider:{digest:x}")
            }),
            activity.kind,
            activity.state,
            content.clone(),
            None,
            Vec::new(),
            activity.payload,
        )
        .await;
        let message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::System,
            content,
            created_at: Utc::now(),
        };
        self.push_message(message.clone()).await;
        if let Err(error) = self.write_session_metadata().await {
            eprintln!("failed to persist session activity: {error}");
        }
        self.publish_event(SessionEvent::MessageCreated(message));
    }

    pub async fn emit_activity(
        &self,
        session_id: &str,
        kind: ActivityKind,
        title: impl Into<String>,
        primary: Option<String>,
        secondary: Vec<String>,
        payload: serde_json::Value,
    ) {
        self.emit_activity_with_state(
            session_id,
            None,
            kind,
            EntityState::Running,
            title,
            primary,
            secondary,
            payload,
        )
        .await;
    }

    async fn emit_activity_with_state(
        &self,
        session_id: &str,
        activity_id: Option<String>,
        kind: ActivityKind,
        state: EntityState,
        title: impl Into<String>,
        primary: Option<String>,
        secondary: Vec<String>,
        payload: serde_json::Value,
    ) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        let title = title.into().trim().to_string();
        if title.is_empty() {
            return;
        }
        self.project_agent_event(
            session_id,
            AgentProjection::Activity {
                activity_id: activity_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                kind,
                state,
                title,
                primary,
                secondary,
                payload,
            },
        );
    }

    pub async fn emit_session_diff(&self, mut event: SessionDiffEvent) {
        if !self.bound_domain_turn_is_active(&event.session_id) {
            return;
        }
        if let Some(patch) = event.patch.as_mut() {
            if patch.chars().count() > Self::MAX_PERSISTED_DIFF_CHARS {
                *patch = format!(
                    "{}\n\n[diff truncated by omni-code-bridge]",
                    patch
                        .chars()
                        .take(Self::MAX_PERSISTED_DIFF_CHARS)
                        .collect::<String>()
                );
            }
        }
        let mut diffs = self.session_diffs.write().await;
        let entries = diffs.entry(event.session_id.clone()).or_default();
        let key = session_diff_key(&event);
        entries.retain(|item| session_diff_key(item) != key);
        entries.push(event.clone());
        drop(diffs);
        if let Err(error) = self.write_session_metadata().await {
            eprintln!("failed to persist session diff: {error}");
        }
        self.publish_event(SessionEvent::SessionDiff(event.clone()));
        let artifact_owner = DOMAIN_TURN_ID.try_with(Clone::clone).unwrap_or_else(|_| {
            event
                .conversation_turn_id
                .clone()
                .unwrap_or_else(|| event.session_id.clone())
        });
        let artifact_id = format!("{artifact_owner}:turn-diff");
        self.project_agent_event(
            &event.session_id,
            AgentProjection::Artifact {
                artifact_id,
                kind: ArtifactKind::TurnCumulativeDiff,
                state: EntityState::Running,
                source_activity_id: None,
                payload: serde_json::to_value(&event).unwrap_or_default(),
            },
        );
    }

    pub async fn finish_assistant_message(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<(), String> {
        if !self.bound_domain_turn_is_active(session_id) {
            return Ok(());
        }
        let mut assistant_message = {
            let mut messages = self.messages.write().await;
            let message = messages
                .get(session_id)
                .and_then(|items| items.iter().find(|item| item.id == message_id))
                .cloned()
                .ok_or_else(|| format!("unknown message: {message_id}"))?;
            if let Some(stored) = messages
                .get_mut(session_id)
                .and_then(|items| items.iter_mut().find(|item| item.id == message_id))
            {
                stored.clone()
            } else {
                message
            }
        };
        if let Some(provider_message) = self
            .refresh_provider_final_assistant_message(session_id, &assistant_message)
            .await
        {
            assistant_message = provider_message;
        }
        let content = assistant_message.content.clone();
        let assistant_message_id = assistant_message.id.clone();

        // Metadata persistence must not keep a completed provider turn alive.
        // `patch_session` updates the in-memory session before awaiting disk I/O,
        // so a later metadata retry can safely converge the durable copy.
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            self.patch_session(session_id, SessionStatus::Idle, Some(content.clone())),
        )
        .await;
        self.clear_interrupted(session_id).await;
        self.clear_pending_approval(session_id).await;
        self.publish_event(SessionEvent::MessageCreated(assistant_message));
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::Idle,
            error_message: None,
        }));
        self.try_project_agent_event(
            session_id,
            AgentProjection::AssistantMessage {
                message_id: assistant_message_id.clone(),
                purpose: MessagePurpose::Final,
                state: EntityState::Completed,
                content: content.clone(),
            },
        )?;
        self.try_project_agent_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::Completed,
                error: None,
            },
        )?;
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            self.mark_session_unread(session_id, &assistant_message_id),
        )
        .await;
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

    async fn refresh_provider_final_assistant_message(
        &self,
        session_id: &str,
        current: &ChatMessage,
    ) -> Option<ChatMessage> {
        let session = self.find_session(session_id).await?;
        let provider = self.provider_for_agent(session.agent)?;
        let provider_session_id = self
            .provider_session_ref(session_id)
            .await
            .unwrap_or_else(|| session_id.to_string());
        let provider_messages = provider.list_messages(&provider_session_id).await?;
        let provider_messages =
            MessageProjection::normalize_provider(session_id, provider_messages);
        let provider_final = provider_messages
            .iter()
            .rev()
            .find(|message| Self::is_final_assistant_candidate(current, message))
            .cloned()?;
        if provider_final.content.len() < current.content.len()
            && !Self::is_assistant_message_aggregate(current, &provider_final)
        {
            return None;
        }
        let final_message = ChatMessage {
            id: current.id.clone(),
            session_id: current.session_id.clone(),
            role: current.role.clone(),
            content: provider_final.content,
            // Never let provider clock skew move the reply before the bridge's
            // turn anchor, while retaining provider order for multi-part replies.
            created_at: provider_final.created_at.max(current.created_at),
        };
        let local_messages = self.messages.read().await.get(session_id).cloned()?;
        let merged = local_messages
            .into_iter()
            .map(|message| {
                if message.id == current.id {
                    final_message.clone()
                } else {
                    message
                }
            })
            .collect::<Vec<_>>();
        self.messages
            .write()
            .await
            .insert(session_id.to_string(), merged);
        Some(final_message)
    }

    fn is_final_assistant_candidate(current: &ChatMessage, candidate: &ChatMessage) -> bool {
        if current.role != MessageRole::Assistant || candidate.role != MessageRole::Assistant {
            return false;
        }
        if current.session_id != candidate.session_id {
            return false;
        }
        if current.id == candidate.id {
            return true;
        }
        let seconds_apart = current
            .created_at
            .signed_duration_since(candidate.created_at)
            .num_seconds()
            .abs();
        if seconds_apart > 10 * 60 {
            return false;
        }
        let current_content = current.content.trim();
        let candidate_content = candidate.content.trim();
        if current_content.is_empty() || candidate_content.is_empty() {
            return false;
        }
        candidate_content.starts_with(current_content)
            || current_content.starts_with(candidate_content)
            || current_content == candidate_content
            || Self::is_assistant_message_aggregate(current, candidate)
    }

    fn is_assistant_message_aggregate(current: &ChatMessage, candidate: &ChatMessage) -> bool {
        let current_content = current.content.trim();
        let candidate_content = candidate.content.trim();
        current_content.len() > candidate_content.len()
            && current_content.contains(candidate_content)
            && current_content.contains("\n\n---\n\n")
    }

    pub async fn fail_session(&self, session_id: &str, message: String) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        self.patch_session(session_id, SessionStatus::Failed, Some(message.clone()))
            .await;
        self.clear_interrupted(session_id).await;
        self.clear_pending_approval(session_id).await;
        self.publish_event(SessionEvent::AgentError(AgentErrorEvent {
            session_id: session_id.to_string(),
            message: message.clone(),
        }));
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::Failed,
            error_message: Some(message.clone()),
        }));
        self.project_agent_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::Failed,
                error: Some(message),
            },
        );
    }

    /// Ends the current turn when the provider lost its completion signal.
    /// This deliberately does not send a provider cancellation: without a live
    /// process handle, the bridge cannot safely claim ownership of external work.
    pub async fn interrupt_turn_for_recovery(&self, session_id: &str, message: String) {
        if !self.bound_domain_turn_is_active(session_id) {
            return;
        }
        self.update_runtime_state(session_id, |entry| {
            entry.interrupted = true;
            entry.approval_tx = None;
            entry.pending_approval = None;
        })
        .await;
        let preview = self
            .latest_assistant_preview(session_id)
            .await
            .or(Some(message));
        self.patch_session(session_id, SessionStatus::Interrupted, preview)
            .await;
        self.clear_pending_approval(session_id).await;
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::Interrupted,
            error_message: None,
        }));
        self.project_agent_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::Cancelled,
                error: None,
            },
        );
    }

    pub async fn fail_domain_turn(&self, session_id: &str, turn_id: &str, message: String) {
        DOMAIN_TURN_ID
            .scope(turn_id.to_string(), self.fail_session(session_id, message))
            .await;
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
        if let Some(session_ref) = self
            .provider_session_ref_chain(session_id)
            .await
            .into_iter()
            .last()
        {
            return Some(session_ref);
        }
        let session = self.find_session(session_id).await?;
        let provider = self.provider_for_agent(session.agent)?;
        provider.default_runtime_ref(session_id).await
    }

    async fn provider_session_ref_chain(&self, session_id: &str) -> Vec<String> {
        let runtime = self.runtime.lock().await;
        provider_session_ref_chain_from_runtime(&runtime, session_id)
    }

    async fn resolve_session_id(&self, session_id: &str) -> String {
        if self.sessions.read().await.contains_key(session_id) {
            return session_id.to_string();
        }

        let runtime = self.runtime.lock().await;
        if let Some(local_session_id) = runtime.iter().find_map(|(candidate_id, state)| {
            (state.provider_session_ref.as_deref() == Some(session_id))
                .then(|| candidate_id.clone())
        }) {
            drop(runtime);
            if self.sessions.read().await.contains_key(&local_session_id) {
                return local_session_id;
            }
            return session_id.to_string();
        }

        session_id.to_string()
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
        // Runtime provider links affect session aggregation. Do not serve a
        // stale list while a model/provider migration is being established.
        self.invalidate_list_cache().await;
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
        let allowed = DOMAIN_TURN_ID
            .try_with(Clone::clone)
            .map(|turn| entry.current_turn_id.as_deref() == Some(turn.as_str()))
            .unwrap_or(true);
        if allowed {
            entry.approval_tx = Some(sender);
        }
    }

    pub async fn clear_approval_sender(&self, session_id: &str) {
        let mut runtime = self.runtime.lock().await;
        if let Some(entry) = runtime.get_mut(session_id) {
            let allowed = DOMAIN_TURN_ID
                .try_with(Clone::clone)
                .map(|turn| entry.current_turn_id.as_deref() == Some(turn.as_str()))
                .unwrap_or(true);
            if allowed {
                entry.approval_tx = None;
            }
        }
    }

    pub async fn finish_turn(&self, session_id: &str) {
        self.update_runtime_state(session_id, |entry| {
            let allowed = DOMAIN_TURN_ID
                .try_with(Clone::clone)
                .map(|turn| entry.current_turn_id.as_deref() == Some(turn.as_str()))
                .unwrap_or(true);
            if allowed {
                entry.turn_in_flight = false;
                entry.turn_abort = None;
                entry.cancel_tx = None;
                entry.current_turn_id = None;
            }
        })
        .await;
    }

    pub async fn set_cancel_sender(&self, session_id: &str, sender: mpsc::UnboundedSender<()>) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        let allowed = DOMAIN_TURN_ID
            .try_with(Clone::clone)
            .map(|turn| entry.current_turn_id.as_deref() == Some(turn.as_str()))
            .unwrap_or(true);
        if allowed {
            entry.cancel_tx = Some(sender);
        }
    }

    #[cfg(test)]
    pub async fn has_cancel_sender_for_test(&self, session_id: &str) -> bool {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .and_then(|entry| entry.cancel_tx.as_ref())
            .is_some()
    }

    #[cfg(test)]
    pub async fn turn_in_flight_for_test(&self, session_id: &str) -> bool {
        self.runtime
            .lock()
            .await
            .get(session_id)
            .map(|entry| entry.turn_in_flight)
            .unwrap_or(false)
    }

    pub async fn set_turn_abort(&self, session_id: &str, abort_handle: Option<AbortHandle>) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        entry.turn_abort = abort_handle;
    }

    async fn set_turn_abort_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        abort_handle: Option<AbortHandle>,
    ) {
        let mut runtime = self.runtime.lock().await;
        if let Some(entry) = runtime.get_mut(session_id)
            && entry.current_turn_id.as_deref() == Some(turn_id)
        {
            entry.turn_abort = abort_handle;
        }
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
        self.publish_event(SessionEvent::ApprovalRequested(ApprovalRequestEvent {
            session_id: session_id.to_string(),
            request: request.clone(),
        }));
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::AwaitingApproval,
            error_message: None,
        }));
        self.project_agent_event(
            session_id,
            AgentProjection::Activity {
                activity_id: request.request_id.clone(),
                kind: ActivityKind::Approval,
                state: EntityState::AwaitingApproval,
                title: "等待审批".to_string(),
                primary: request.command.clone().or_else(|| request.reason.clone()),
                secondary: Vec::new(),
                payload: serde_json::to_value(&request).unwrap_or_default(),
            },
        );
        self.project_agent_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::AwaitingApproval,
                error: None,
            },
        );
    }

    pub async fn resolve_approval(
        &self,
        session_id: &str,
        request_id: &str,
        choice: ApprovalChoice,
    ) {
        debug_log!(
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
        self.publish_event(SessionEvent::ApprovalResolved(ApprovalResolvedEvent {
            session_id: session_id.to_string(),
            request_id: request_id.to_string(),
            choice: choice.clone(),
        }));
        self.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: session_id.to_string(),
            status: SessionStatus::Running,
            error_message: None,
        }));
        self.project_active_turn_event(
            session_id,
            AgentProjection::Activity {
                activity_id: request_id.to_string(),
                kind: ActivityKind::Approval,
                state: EntityState::Completed,
                title: preview,
                primary: None,
                secondary: Vec::new(),
                payload: serde_json::json!({ "choice": choice }),
            },
        );
        self.project_active_turn_event(
            session_id,
            AgentProjection::TurnStatus {
                status: TurnStatus::Running,
                error: None,
            },
        );
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
        if self
            .messages
            .read()
            .await
            .get(session_id)
            .is_some_and(|messages| !messages.is_empty())
        {
            return;
        }
        let Some(provider) = self.provider_for_agent(agent) else {
            return;
        };
        let provider_session_id = self
            .provider_session_ref(session_id)
            .await
            .unwrap_or_else(|| session_id.to_string());
        if let Some(existing) = provider.list_messages(&provider_session_id).await {
            let existing = MessageProjection::normalize_provider(session_id, existing);
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
            let summary = current.clone();
            self.sessions
                .write()
                .await
                .insert(current.id.clone(), current);
            let _ = self.persist_canonical_session(&summary).await;
            self.invalidate_list_cache().await;
            self.refresh_project_summary(&project_id).await;
        }
    }

    /// Only replies completed by this bridge reach this method. Provider-only
    /// archive updates therefore never create unread state.
    async fn mark_session_unread(&self, session_id: &str, message_id: &str) -> Result<(), String> {
        let mut unread_message_ids = self.unread_message_ids.write().await;
        let mut sessions = self.sessions.write().await;
        let Some(session) = sessions.get_mut(session_id) else {
            return Ok(());
        };
        session.unread_count = 1;
        let summary = session.clone();
        unread_message_ids.insert(session_id.to_string(), message_id.to_string());
        drop(sessions);
        drop(unread_message_ids);
        self.persist_canonical_session(&summary).await?;
        self.invalidate_list_cache().await;
        self.publish_event(SessionEvent::SessionSnapshot(summary));
        Ok(())
    }

    pub async fn mark_session_read(
        &self,
        session_id: &str,
        last_message_id: &str,
    ) -> Result<SessionSummary, MarkSessionReadError> {
        let expected_message_id_before = self
            .unread_message_ids
            .read()
            .await
            .get(session_id)
            .cloned();
        let acknowledges_visible_reply = self
            .message_is_at_or_after(
                session_id,
                expected_message_id_before.as_deref(),
                last_message_id,
            )
            .await;
        let mut unread_message_ids = self.unread_message_ids.write().await;
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            MarkSessionReadError::NotFound(format!("unknown bridge session: {session_id}"))
        })?;
        let expected_message_id = unread_message_ids.get(session_id).cloned();
        let changed = (expected_message_id.is_none()
            || expected_message_id.as_deref() == Some(last_message_id)
            || (expected_message_id == expected_message_id_before && acknowledges_visible_reply))
            && (session.unread_count != 0 || expected_message_id.is_some());
        if changed {
            session.unread_count = 0;
            unread_message_ids.remove(session_id);
        }
        let summary = session.clone();
        drop(sessions);
        drop(unread_message_ids);
        if changed {
            self.persist_canonical_session(&summary)
                .await
                .map_err(MarkSessionReadError::Persistence)?;
            self.invalidate_list_cache().await;
            self.publish_event(SessionEvent::SessionSnapshot(summary.clone()));
            self.session_domain
                .mark_read(session_id)
                .map_err(MarkSessionReadError::Persistence)?;
        }
        Ok(summary)
    }

    async fn clear_session_unread(&self, session_id: &str) -> Result<(), String> {
        let mut unread_message_ids = self.unread_message_ids.write().await;
        let mut sessions = self.sessions.write().await;
        let Some(session) = sessions.get_mut(session_id) else {
            return Ok(());
        };
        let changed = session.unread_count != 0 || unread_message_ids.contains_key(session_id);
        if !changed {
            return Ok(());
        }
        session.unread_count = 0;
        unread_message_ids.remove(session_id);
        let summary = session.clone();
        drop(sessions);
        drop(unread_message_ids);
        self.persist_canonical_session(&summary).await?;
        self.invalidate_list_cache().await;
        self.publish_event(SessionEvent::SessionSnapshot(summary));
        self.session_domain.mark_read(session_id)?;
        Ok(())
    }

    async fn message_is_at_or_after(
        &self,
        session_id: &str,
        expected_message_id: Option<&str>,
        last_message_id: &str,
    ) -> bool {
        let Some(expected_message_id) = expected_message_id else {
            return false;
        };
        let messages = self.messages.read().await;
        let Some(messages) = messages.get(session_id) else {
            return false;
        };
        let Some(expected_message) = messages
            .iter()
            .find(|message| message.id == expected_message_id)
        else {
            return false;
        };
        let Some(last_message) = messages
            .iter()
            .find(|message| message.id == last_message_id)
        else {
            return false;
        };
        last_message.created_at >= expected_message.created_at
    }

    pub async fn update_session_settings(
        &self,
        session_id: &str,
        provider_id: Option<Option<String>>,
        reasoning_effort: Option<Option<crate::models::ReasoningEffort>>,
        model: Option<Option<String>>,
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
        if let Some(model) = model {
            current.model = model;
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
        let _ = self.persist_canonical_session(&summary).await;
        self.session_domain.sync_session_metadata(&summary)?;
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
        self.publish_event(SessionEvent::SessionSnapshot(summary.clone()));
        self.session_domain.sync_session_metadata(&summary)?;
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
        self.write_session_metadata().await
    }

    async fn persist_canonical_session(&self, session: &SessionSummary) -> Result<(), String> {
        debug_assert!(self.sessions.read().await.contains_key(&session.id));
        self.write_session_metadata().await
    }

    async fn write_session_metadata(&self) -> Result<(), String> {
        let _write_guard = self.metadata_write_lock.lock().await;
        let entries = self.session_title_overrides.read().await.clone();
        let unread_message_ids = self.unread_message_ids.read().await.clone();
        let session_diffs = self.session_diffs.read().await.clone();
        let system_messages = self.messages.read().await.clone();
        let canonical_sessions = self.sessions.read().await.clone();
        let mut metadata = PersistedSessionMetadata {
            sessions: HashMap::new(),
            projects: self.projects.read().await.clone(),
        };
        for (session_id, session) in canonical_sessions {
            metadata.sessions.insert(
                session_id.clone(),
                PersistedSessionMetadataEntry {
                    title: entries.get(&session_id).cloned(),
                    session: Some(session),
                    last_unread_message_id: unread_message_ids.get(&session_id).cloned(),
                    diffs: session_diffs.get(&session_id).cloned().unwrap_or_default(),
                    system_messages: system_messages
                        .get(&session_id)
                        .into_iter()
                        .flatten()
                        .filter(|message| matches!(message.role, MessageRole::System))
                        .cloned()
                        .collect(),
                    user_messages: system_messages
                        .get(&session_id)
                        .into_iter()
                        .flatten()
                        .filter(|message| matches!(message.role, MessageRole::User))
                        .cloned()
                        .collect(),
                },
            );
        }
        for (session_id, title) in entries {
            metadata.sessions.entry(session_id).or_default().title = Some(title);
        }
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
        let visible_ids = merged_sessions.keys().cloned().collect::<HashSet<_>>();
        let visible_descendants = runtime_refs
            .iter()
            .filter(|(source, target)| {
                visible_ids.contains(*source) && visible_ids.contains(*target)
            })
            .map(|(_, target)| target.clone())
            .collect::<HashSet<_>>();
        let visible_roots = visible_ids
            .iter()
            .filter(|session_id| {
                runtime_refs.contains_key(*session_id) && !visible_descendants.contains(*session_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for session_id in visible_roots {
            let Some(local_session) = merged_sessions.get(&session_id).cloned() else {
                continue;
            };
            let linked_refs = provider_session_ref_chain_from_refs(&runtime_refs, &session_id);
            let merged = linked_refs
                .into_iter()
                .filter_map(|session_ref| merged_sessions.remove(&session_ref))
                .fold(local_session, Self::merge_linked_sessions);
            merged_sessions.insert(session_id, merged);
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
        let provider_is_newer = provider_session.updated_at > local_session.updated_at;
        if local_session.runtime_session_ref.is_none() {
            local_session.runtime_session_ref = provider_session.runtime_session_ref.clone();
        }
        local_session.status =
            Self::preferred_session_status(local_session.status, provider_session.status);

        if provider_is_newer {
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

        if (provider_is_newer
            || local_session
                .last_message_preview
                .as_deref()
                .is_none_or(|preview| preview.trim().is_empty()))
            && provider_session
                .last_message_preview
                .as_deref()
                .is_some_and(|preview| !preview.trim().is_empty())
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

fn completed_codex_history_is_ready(state: &SessionState, messages: &[ChatMessage]) -> bool {
    let Some(domain_final) = state
        .turns
        .iter()
        .rev()
        .flat_map(|turn| turn.segments.iter().rev())
        .filter_map(|segment| segment.message.as_ref())
        .find(|message| message.purpose == MessagePurpose::Final)
    else {
        return false;
    };
    messages.iter().rev().find_map(|message| {
        (message.role == MessageRole::Assistant).then_some(message.content.trim())
    }) == Some(domain_final.content.trim())
}

fn classify_activity_kind(content: &str) -> ActivityKind {
    let normalized = content.to_ascii_lowercase();
    if normalized.starts_with("[reasoning]") {
        ActivityKind::Reasoning
    } else if normalized.contains("completed")
        || normalized.contains("finished")
        || normalized.contains(" exited")
        || normalized.contains(":output]")
    {
        ActivityKind::ToolResult
    } else if normalized.starts_with("[exec]")
        || normalized.starts_with("[process]")
        || normalized.starts_with("[command")
        || normalized.starts_with("[acp:")
        || normalized.contains("tool_call")
    {
        ActivityKind::ToolCall
    } else {
        ActivityKind::Progress
    }
}

fn session_diff_key(event: &SessionDiffEvent) -> String {
    format!(
        "{}:{}",
        event.conversation_turn_id.as_deref().unwrap_or_default(),
        event.files.first().cloned().unwrap_or_default()
    )
}

fn dedupe_session_diffs(diffs: Vec<SessionDiffEvent>) -> Vec<SessionDiffEvent> {
    let mut deduped = Vec::new();
    for diff in diffs {
        let key = session_diff_key(&diff);
        deduped.retain(|item| session_diff_key(item) != key);
        deduped.push(diff);
    }
    deduped
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

fn domain_store_path(metadata_path: &Path) -> PathBuf {
    let file_name = metadata_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("sessions-metadata.json");
    metadata_path.with_file_name(format!("{file_name}.v2.sqlite3"))
}

fn pi_plugin_store_path(metadata_path: &Path) -> PathBuf {
    metadata_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(".omni-code"))
        .join("pi-plugins")
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

fn always_allow_prompt_rule(command: &str) -> String {
    let quoted_command =
        serde_json::to_string(command).unwrap_or_else(|_| format!("\"{command}\""));
    format!(
        "Always allow commands that perform the same kind of operation as {quoted_command}, unless a hard safety block applies."
    )
}

fn decorate_user_content(input: &SendMessageInput) -> String {
    match input.input_mode {
        InputMode::Text => input.content.clone(),
        InputMode::Voice => format!("[voice] {}", input.content),
    }
}

fn sort_messages(mut messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    messages.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    messages
}

fn replay_events_from_stream(stream: &EventStreamState, event_id: u64) -> EventReplay {
    let high_watermark = stream.next_event_id.saturating_sub(1);
    if event_id > high_watermark {
        return EventReplay::SyncRequired;
    }
    let Some(oldest) = stream.log.front() else {
        return EventReplay::Events(Vec::new());
    };
    if event_id.saturating_add(1) < oldest.id {
        return EventReplay::SyncRequired;
    }
    EventReplay::Events(
        stream
            .log
            .iter()
            .filter(|event| event.id > event_id)
            .cloned()
            .collect(),
    )
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
        adapter::AgentProvider,
        bridge_settings::{AiApprovalSettings, BridgeSettingsInput},
        models::{
            AUTO_PROVIDER_ID, AgentKind, ApiFormat, ChatMessage, MessageRole, ModelProviderConfig,
            SessionSummary,
        },
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::{Mutex as TokioMutex, mpsc};

    #[test]
    fn always_allow_rule_expands_to_similar_operations_and_keeps_hard_blocks() {
        let rule = always_allow_prompt_rule("docker compose ps");
        assert!(rule.contains("same kind of operation"));
        assert!(rule.contains("docker compose ps"));
        assert!(rule.contains("unless a hard safety block applies"));
    }

    struct TestMessageProvider {
        sessions: TokioMutex<HashMap<String, SessionSummary>>,
        messages: TokioMutex<HashMap<String, Vec<ChatMessage>>>,
        cancelled: TokioMutex<Vec<(String, Option<String>)>>,
    }

    impl TestMessageProvider {
        fn new(messages: HashMap<String, Vec<ChatMessage>>) -> Self {
            Self {
                sessions: TokioMutex::new(HashMap::new()),
                messages: TokioMutex::new(messages),
                cancelled: TokioMutex::new(Vec::new()),
            }
        }

        async fn set_session(&self, session: SessionSummary) {
            self.sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
        }

        async fn set_messages(&self, session_id: impl Into<String>, messages: Vec<ChatMessage>) {
            self.messages
                .lock()
                .await
                .insert(session_id.into(), messages);
        }

        async fn cancelled_sessions(&self) -> Vec<(String, Option<String>)> {
            self.cancelled.lock().await.clone()
        }
    }

    #[async_trait]
    impl AgentProvider for TestMessageProvider {
        async fn list_sessions(&self) -> HashMap<String, SessionSummary> {
            self.sessions.lock().await.clone()
        }

        async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
            self.messages.lock().await.get(session_id).cloned()
        }

        async fn cancel_session(
            &self,
            session_id: &str,
            runtime_session_ref: Option<&str>,
        ) -> Result<bool> {
            self.cancelled.lock().await.push((
                session_id.to_string(),
                runtime_session_ref.map(str::to_string),
            ));
            Ok(true)
        }

        async fn run_session(
            &self,
            _state: Arc<AppState>,
            _session: SessionSummary,
            _input: ChatMessage,
            _system_prompt: Option<String>,
            _reply: ChatMessage,
            _provider_config: Option<ResolvedProviderConfig>,
            _reasoning_effort: Option<crate::models::ReasoningEffort>,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct PanickingProvider;

    #[async_trait]
    impl AgentProvider for PanickingProvider {
        async fn run_session(
            &self,
            _state: Arc<AppState>,
            _session: SessionSummary,
            _input: ChatMessage,
            _system_prompt: Option<String>,
            _reply: ChatMessage,
            _provider_config: Option<ResolvedProviderConfig>,
            _reasoning_effort: Option<crate::models::ReasoningEffort>,
        ) -> Result<()> {
            panic!("simulated provider task panic");
        }
    }

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

    async fn test_state_with_providers(prefix: &str, providers: ProviderRegistry) -> AppState {
        let settings_path = test_path(&format!("{prefix}-settings"));
        let runtime_path = test_path(&format!("{prefix}-runtime"));
        let metadata_path = test_path(&format!("{prefix}-metadata"));
        AppState::new_with_paths_and_providers(
            settings_path,
            runtime_path,
            metadata_path,
            providers,
        )
        .await
    }

    fn test_message(
        id: &str,
        session_id: &str,
        role: MessageRole,
        content: &str,
        created_at: chrono::DateTime<Utc>,
    ) -> ChatMessage {
        ChatMessage {
            id: id.to_string(),
            session_id: session_id.to_string(),
            role,
            content: content.to_string(),
            created_at,
        }
    }

    #[tokio::test]
    async fn strict_state_load_fails_for_invalid_settings_file() {
        let settings_path = test_path("strict-invalid-settings");
        let runtime_path = test_path("strict-invalid-runtime");
        let metadata_path = test_path("strict-invalid-metadata");

        tokio::fs::write(&settings_path, "{not-json")
            .await
            .expect("invalid settings fixture should be written");

        let error = AppState::new_with_paths_strict(settings_path, runtime_path, metadata_path)
            .await
            .err()
            .expect("strict state load should fail for invalid settings");
        assert!(
            error
                .to_string()
                .contains("failed to parse bridge settings")
        );
    }

    #[tokio::test]
    async fn create_session_is_idempotent_for_client_session_id() {
        let state = test_state("create-session-idempotent").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let input = CreateSessionInput {
            client_session_id: "client-stable-session".to_string(),
            project_id: project.id,
            title: Some("Session".to_string()),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            provider_id: None,
            reasoning_effort: None,
            model: Some("gpt-session-a".to_string()),
        };

        let first = state.create_session(input.clone()).await.unwrap();
        let second = state.create_session(input).await.unwrap();

        assert_eq!(first.id, "client-stable-session");
        assert_eq!(second.id, first.id);
        assert_eq!(first.model.as_deref(), Some("gpt-session-a"));
        assert_eq!(second.model, first.model);
        assert_eq!(state.sessions.read().await.len(), 1);
    }

    #[tokio::test]
    async fn canonical_session_survives_state_reconstruction() {
        let settings_path = test_path("canonical-restart-settings");
        let runtime_path = test_path("canonical-restart-runtime");
        let metadata_path = test_path("canonical-restart-metadata");
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
        let created = state
            .create_session(CreateSessionInput {
                client_session_id: "client-persisted-session".to_string(),
                project_id: project.id,
                title: Some("Persistent session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .unwrap();
        drop(state);

        let restored = AppState::new_with_paths(settings_path, runtime_path, metadata_path).await;
        let session = restored.find_session(&created.id).await.unwrap();
        assert_eq!(session.id, "client-persisted-session");
        assert_eq!(session.title, "Persistent session");
        let project_sessions = restored
            .list_project_sessions(&created.project_id)
            .await
            .expect("canonical project should also be restored");
        assert_eq!(project_sessions.len(), 1);
        assert_eq!(project_sessions[0].id, created.id);
    }

    #[tokio::test]
    async fn list_messages_refreshes_provider_messages_even_with_cached_session_messages() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Custom,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("refresh-messages", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Custom,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        provider
            .set_messages(
                session.id.clone(),
                vec![
                    test_message("u1", &session.id, MessageRole::User, "hello", now),
                    test_message(
                        "a1",
                        &session.id,
                        MessageRole::Assistant,
                        "new remote reply",
                        now + chrono::TimeDelta::seconds(1),
                    ),
                ],
            )
            .await;
        state.messages.write().await.insert(
            session.id.clone(),
            vec![test_message(
                "u1",
                &session.id,
                MessageRole::User,
                "hello",
                now,
            )],
        );

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].id, "a1");
        assert_eq!(messages[1].content, "new remote reply");
    }

    #[tokio::test]
    async fn list_messages_returns_empty_page_when_session_has_no_history() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Custom,
            provider as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("empty-message-history", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Custom,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("existing session should return a message page");
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn list_messages_uses_provider_session_ref_for_remote_history() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("remote-provider-session-ref", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;

        provider
            .set_messages(
                "codex-thread-1",
                vec![
                    test_message("u1", "codex-thread-1", MessageRole::User, "hello", now),
                    test_message(
                        "a1",
                        "codex-thread-1",
                        MessageRole::Assistant,
                        "latest remote reply",
                        now + chrono::TimeDelta::seconds(1),
                    ),
                ],
            )
            .await;
        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");
        let cached = state
            .messages
            .read()
            .await
            .get(&session.id)
            .cloned()
            .expect("messages should be cached");

        assert_eq!(messages.len(), 2);
        assert!(
            messages
                .iter()
                .all(|message| message.session_id == session.id)
        );
        assert_eq!(messages[1].content, "latest remote reply");
        assert_eq!(cached.len(), messages.len());
        assert!(
            cached
                .iter()
                .all(|message| message.session_id == session.id)
        );
        assert_eq!(cached[1].content, "latest remote reply");
    }

    #[tokio::test]
    async fn list_messages_follows_transitive_provider_session_refs() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("transitive-provider-history", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("old-thread".to_string()))
            .await;
        state
            .set_provider_session_ref("old-thread", Some("goal-thread".to_string()))
            .await;
        provider
            .set_messages(
                "old-thread",
                vec![test_message(
                    "old-user",
                    "old-thread",
                    MessageRole::User,
                    "previous history",
                    now,
                )],
            )
            .await;
        provider
            .set_messages(
                "goal-thread",
                vec![test_message(
                    "goal-user",
                    "goal-thread",
                    MessageRole::User,
                    "/goal finish task",
                    now + chrono::TimeDelta::seconds(1),
                )],
            )
            .await;

        assert_eq!(
            state.provider_session_ref(&session.id).await.as_deref(),
            Some("goal-thread")
        );
        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "previous history");
        assert_eq!(messages[1].content, "/goal finish task");
        assert!(
            messages
                .iter()
                .all(|message| message.session_id == session.id)
        );
    }

    #[test]
    fn provider_session_ref_chain_stops_at_cycles() {
        let refs = HashMap::from([
            ("local".to_string(), "old-thread".to_string()),
            ("old-thread".to_string(), "goal-thread".to_string()),
            ("goal-thread".to_string(), "old-thread".to_string()),
        ]);

        assert_eq!(
            provider_session_ref_chain_from_refs(&refs, "local"),
            vec!["old-thread".to_string(), "goal-thread".to_string()]
        );
    }

    #[tokio::test]
    async fn domain_state_imports_provider_only_session_on_demand() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("domain-provider-import", registry).await;
        let provider_session = SessionSummary {
            id: "provider-only-session".to_string(),
            project_id: "provider-project".to_string(),
            title: "Provider history".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now() - chrono::TimeDelta::days(7),
            unread_count: 0,
            last_message_preview: Some("history".to_string()),
            pending_approval: None,
            runtime_session_ref: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        provider.set_session(provider_session.clone()).await;
        let now = Utc::now();
        provider
            .set_messages(
                provider_session.id.clone(),
                vec![
                    test_message(
                        "provider-user",
                        &provider_session.id,
                        MessageRole::User,
                        "old question",
                        now,
                    ),
                    test_message(
                        "provider-assistant",
                        &provider_session.id,
                        MessageRole::Assistant,
                        "old answer",
                        now + chrono::TimeDelta::seconds(1),
                    ),
                ],
            )
            .await;
        state.session_diffs.write().await.insert(
            "provider-only-session".to_string(),
            vec![SessionDiffEvent {
                session_id: "provider-only-session".to_string(),
                conversation_turn_id: Some("provider-user".to_string()),
                files: vec!["src/main.rs".to_string()],
                added: Some(2),
                removed: Some(1),
                patch: Some("@@ -1 +1 @@".to_string()),
                summary: Some("updated main".to_string()),
            }],
        );
        state
            .session_domain
            .ensure_session(&provider_session)
            .expect("simulate an already imported empty domain session");
        assert!(
            state
                .session_domain
                .session_state(&provider_session.id)
                .unwrap()
                .is_some_and(|snapshot| snapshot.turns.is_empty())
        );
        let snapshot = state
            .domain_session_state(&provider_session.id)
            .await
            .unwrap()
            .expect("provider-only session should be imported on demand");

        assert_eq!(snapshot.session.id, provider_session.id);
        assert_eq!(snapshot.session.title, provider_session.title);
        assert_eq!(snapshot.turns.len(), 1);
        assert_eq!(snapshot.turns[0].user_message.content, "old question");
        assert_eq!(
            snapshot.turns[0].segments[0]
                .message
                .as_ref()
                .map(|message| message.content.as_str()),
            Some("old answer")
        );
        assert_eq!(snapshot.turns[0].artifacts.len(), 1);
        assert_eq!(
            snapshot.turns[0].artifacts[0]
                .payload
                .get("conversation_turn_id")
                .and_then(serde_json::Value::as_str),
            Some("provider-user")
        );
        assert_eq!(snapshot.cursor, 3);
    }

    #[tokio::test]
    async fn domain_session_list_includes_provider_only_sessions() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("domain-provider-list", registry).await;
        let provider_session = SessionSummary {
            id: "provider-only-list-session".to_string(),
            project_id: "provider-project".to_string(),
            title: "Provider history".to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at: Utc::now(),
            unread_count: 0,
            last_message_preview: Some("history".to_string()),
            pending_approval: None,
            runtime_session_ref: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        provider.set_session(provider_session.clone()).await;

        let sessions = state
            .list_domain_session_summaries(Some("provider-project"))
            .await
            .expect("domain session list should load");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, provider_session.id);
        assert_eq!(sessions[0].title, provider_session.title);
        assert_eq!(sessions[0].updated_at, provider_session.updated_at);
    }

    #[tokio::test]
    async fn list_sessions_collapses_transitive_provider_threads_into_local_session() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("transitive-session-list", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let local = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Original conversation".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        let mut old_thread = local.clone();
        old_thread.id = "old-thread".to_string();
        old_thread.title = "Old imported thread".to_string();
        let mut goal_thread = local.clone();
        goal_thread.id = "goal-thread".to_string();
        goal_thread.title = "/goal finish task".to_string();
        goal_thread.last_message_preview = Some("/goal finish task".to_string());
        goal_thread.updated_at += chrono::TimeDelta::seconds(2);
        provider.set_session(old_thread).await;
        provider.set_session(goal_thread).await;

        // Prime the aggregated list before the runtime fork links are known.
        // Updating those links must invalidate this cached three-session view.
        assert_eq!(state.list_sessions().await.len(), 3);
        state
            .set_provider_session_ref(&local.id, Some("old-thread".to_string()))
            .await;
        state
            .set_provider_session_ref("old-thread", Some("goal-thread".to_string()))
            .await;

        let sessions = state.list_sessions().await;
        assert_eq!(sessions.len(), 1);
        assert!(sessions.iter().any(|session| session.id == local.id));
        assert!(!sessions.iter().any(|session| session.id == "old-thread"));
        assert!(!sessions.iter().any(|session| session.id == "goal-thread"));
        assert_eq!(
            sessions[0].last_message_preview.as_deref(),
            Some("/goal finish task")
        );
    }

    #[tokio::test]
    async fn list_sessions_collapses_provider_threads_after_local_session_is_gone() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("archived-transitive-session-list", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let template = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Original conversation".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        let mut old_thread = template.clone();
        old_thread.id = "old-thread".to_string();
        old_thread.title = "Previous conversation".to_string();
        let mut goal_thread = template.clone();
        goal_thread.id = "goal-thread".to_string();
        goal_thread.title = "/goal finish task".to_string();
        goal_thread.last_message_preview = Some("/goal finish task".to_string());
        goal_thread.updated_at += chrono::TimeDelta::seconds(2);
        provider.set_session(old_thread).await;
        provider.set_session(goal_thread).await;
        state.sessions.write().await.remove(&template.id);
        state
            .set_provider_session_ref(&template.id, Some("old-thread".to_string()))
            .await;
        state
            .set_provider_session_ref("old-thread", Some("goal-thread".to_string()))
            .await;

        let sessions = state.list_sessions().await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "old-thread");
        assert_eq!(
            sessions[0].last_message_preview.as_deref(),
            Some("/goal finish task")
        );
    }

    #[tokio::test]
    async fn startup_interrupts_only_running_sessions_older_than_seven_days() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("stale-running-startup", registry).await;
        let now = Utc::now();
        let session = |id: &str, status: SessionStatus, updated_at| SessionSummary {
            id: id.to_string(),
            project_id: "project".to_string(),
            title: id.to_string(),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status,
            updated_at,
            unread_count: 0,
            last_message_preview: None,
            pending_approval: None,
            runtime_session_ref: Some(id.to_string()),
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        provider
            .set_session(session(
                "stale-running",
                SessionStatus::Running,
                now - chrono::TimeDelta::days(8),
            ))
            .await;
        provider
            .set_session(session(
                "recent-running",
                SessionStatus::Running,
                now - chrono::TimeDelta::days(6),
            ))
            .await;
        provider
            .set_session(session(
                "stale-approval",
                SessionStatus::AwaitingApproval,
                now - chrono::TimeDelta::days(8),
            ))
            .await;
        state.invalidate_list_cache().await;

        assert_eq!(state.interrupt_stale_running_sessions(now).await, 1);
        let sessions = state
            .list_sessions()
            .await
            .into_iter()
            .map(|session| (session.id, session.status))
            .collect::<HashMap<_, _>>();
        assert!(matches!(
            sessions.get("stale-running"),
            Some(SessionStatus::Interrupted)
        ));
        assert!(matches!(
            sessions.get("recent-running"),
            Some(SessionStatus::Running)
        ));
        assert!(matches!(
            sessions.get("stale-approval"),
            Some(SessionStatus::AwaitingApproval)
        ));
        let persisted = load_persisted_runtime(&state.runtime_store_path).await;
        assert!(
            persisted
                .get("stale-running")
                .is_some_and(|entry| entry.interrupted)
        );
    }

    #[tokio::test]
    async fn list_messages_deduplicates_equivalent_remote_and_local_messages() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("merge-equivalent-messages", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;

        provider
            .set_messages(
                "codex-thread-1",
                vec![
                    test_message(
                        "remote-user",
                        "codex-thread-1",
                        MessageRole::User,
                        "Repeat me once",
                        now,
                    ),
                    test_message(
                        "remote-assistant",
                        "codex-thread-1",
                        MessageRole::Assistant,
                        "Only one assistant bubble",
                        now + chrono::TimeDelta::seconds(1),
                    ),
                ],
            )
            .await;

        state.messages.write().await.insert(
            session.id.clone(),
            vec![
                test_message(
                    "local-user",
                    &session.id,
                    MessageRole::User,
                    "Repeat me once",
                    now + chrono::TimeDelta::seconds(2),
                ),
                test_message(
                    "local-assistant",
                    &session.id,
                    MessageRole::Assistant,
                    "Only one assistant bubble",
                    now + chrono::TimeDelta::seconds(3),
                ),
            ],
        );

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, "local-user");
        assert_eq!(messages[1].id, "local-assistant");
        assert!(
            messages
                .iter()
                .all(|message| message.session_id == session.id)
        );
    }

    #[tokio::test]
    async fn list_messages_keeps_repeated_content_when_messages_are_not_close_in_time() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("merge-repeated-content", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;

        provider
            .set_messages(
                "codex-thread-1",
                vec![test_message(
                    "remote-user",
                    "codex-thread-1",
                    MessageRole::User,
                    "continue",
                    now,
                )],
            )
            .await;

        state.messages.write().await.insert(
            session.id.clone(),
            vec![test_message(
                "local-user",
                &session.id,
                MessageRole::User,
                "continue",
                now + chrono::TimeDelta::minutes(30),
            )],
        );

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, "remote-user");
        assert_eq!(messages[1].id, "local-user");
    }

    #[tokio::test]
    async fn list_messages_preserves_in_flight_assistant_id_for_finish() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("preserve-in-flight-assistant", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;
        let local_assistant = test_message(
            "local-assistant",
            &session.id,
            MessageRole::Assistant,
            "I will inspect the workspace and then commit.",
            now,
        );
        state.messages.write().await.insert(
            session.id.clone(),
            vec![
                test_message("local-user", &session.id, MessageRole::User, "commit", now),
                local_assistant.clone(),
            ],
        );
        provider
            .set_messages(
                "codex-thread-1",
                vec![
                    test_message("remote-user", "codex-thread-1", MessageRole::User, "commit", now),
                    test_message(
                        "remote-assistant",
                        "codex-thread-1",
                        MessageRole::Assistant,
                        "I will inspect the workspace and then commit. Current branch is feat/desktop.",
                        now + chrono::TimeDelta::seconds(3),
                    ),
                ],
            )
            .await;

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");
        let assistant = messages
            .iter()
            .find(|message| message.role == MessageRole::Assistant)
            .expect("assistant should exist");
        assert_eq!(assistant.id, local_assistant.id);
        assert_eq!(
            assistant.content,
            "I will inspect the workspace and then commit. Current branch is feat/desktop."
        );

        state
            .finish_assistant_message(&session.id, &local_assistant.id)
            .await
            .expect("original in-flight assistant id should still finish");
    }

    #[tokio::test]
    async fn send_message_deduplicates_retries_by_client_message_id() {
        let state = Arc::new(test_state("client-message-id").await);
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Custom,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        let input = SendMessageInput {
            content: "hello".to_string(),
            input_mode: InputMode::Text,
            system_prompt: None,
            client_message_id: Some("local-user-123".to_string()),
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        let (user_message, echoed_reply) = state
            .send_message(&session.id, input.clone())
            .await
            .expect("message should send");

        let (retried_user_message, retried_reply) = state
            .send_message(&session.id, input)
            .await
            .expect("retry should reuse result");

        assert_ne!(user_message.id, "local-user-123");
        assert_eq!(echoed_reply.id, user_message.id);
        assert_eq!(retried_user_message.id, user_message.id);
        assert_eq!(retried_reply.id, echoed_reply.id);
        assert_eq!(
            state
                .list_messages(&session.id)
                .await
                .expect("messages should exist")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn event_replay_returns_only_events_after_cursor() {
        let state = test_state("event-replay").await;
        state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: "session-1".to_string(),
            status: SessionStatus::Running,
            error_message: None,
        }));
        state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: "session-1".to_string(),
            status: SessionStatus::Idle,
            error_message: None,
        }));

        let EventReplay::Events(events) = state.replay_events_after(1) else {
            panic!("recent event cursor should replay events");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 2);
    }

    #[tokio::test]
    async fn event_replay_requires_sync_for_future_cursor() {
        let state = test_state("event-replay-future").await;
        state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: "session-1".to_string(),
            status: SessionStatus::Running,
            error_message: None,
        }));

        assert!(matches!(
            state.replay_events_after(10),
            EventReplay::SyncRequired
        ));
    }

    #[tokio::test]
    async fn subscription_replay_high_watermark_prevents_duplicate_delivery() {
        let state = test_state("event-subscription-watermark").await;
        state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: "session-1".to_string(),
            status: SessionStatus::Running,
            error_message: None,
        }));

        let mut subscription = state.subscribe_with_replay(Some(0));
        let EventReplay::Events(replayed) = subscription.replay else {
            panic!("cursor should be replayable");
        };
        assert_eq!(subscription.high_watermark, 1);
        assert_eq!(
            replayed.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![1]
        );

        state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
            session_id: "session-1".to_string(),
            status: SessionStatus::Idle,
            error_message: None,
        }));

        let live = subscription
            .receiver
            .recv()
            .await
            .expect("new event should be broadcast");
        assert_eq!(live.id, 2);
        assert!(live.id > subscription.high_watermark);
    }

    #[tokio::test]
    async fn concurrent_event_publication_preserves_event_id_delivery_order() {
        use std::sync::{Arc as StdArc, Barrier};

        let state = Arc::new(test_state("event-order").await);
        let mut events = state.subscribe();
        let worker_count = 16;
        let barrier = StdArc::new(Barrier::new(worker_count));
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let state = Arc::clone(&state);
            let barrier = StdArc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                state.publish_event(SessionEvent::SessionStatus(SessionStatusEvent {
                    session_id: format!("session-{index}"),
                    status: SessionStatus::Running,
                    error_message: None,
                }));
            }));
        }
        for worker in workers {
            worker.join().expect("publisher thread should finish");
        }

        let mut received_ids = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            received_ids.push(events.recv().await.expect("event should be broadcast").id);
        }

        assert_eq!(received_ids, (1..=worker_count as u64).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn ensure_inbox_seeded_uses_provider_session_ref_and_normalizes_messages() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("seed-provider-session-ref", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;

        provider
            .set_messages(
                "codex-thread-1",
                vec![test_message(
                    "u1",
                    "codex-thread-1",
                    MessageRole::User,
                    "hello",
                    now,
                )],
            )
            .await;

        state
            .ensure_inbox_seeded(&session.id, AgentKind::Codex)
            .await;

        let cached = state
            .messages
            .read()
            .await
            .get(&session.id)
            .cloned()
            .expect("messages should be seeded");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].session_id, session.id);
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
                    runtime_session_ref: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
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
                    runtime_session_ref: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
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
                    runtime_session_ref: Some(provider_session_id.clone()),
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
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
                ai_approval: Some(AiApprovalSettings::default()),
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
                acp_servers: None,
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
            runtime_session_ref: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
        assert_eq!(
            detail.session.runtime_session_ref.as_deref(),
            Some("codex-thread")
        );
    }

    #[tokio::test]
    async fn finish_assistant_message_broadcasts_complete_message_snapshot() {
        let state = test_state("finish-assistant-snapshot").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        let assistant_message = ChatMessage {
            id: "assistant-message".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: Utc::now(),
        };
        state.push_message(assistant_message.clone()).await;
        let mut events = state.subscribe();

        state
            .emit_assistant_message_snapshot(&session.id, &assistant_message.id, "hello ")
            .await;
        state
            .emit_assistant_message_snapshot(&session.id, &assistant_message.id, "hello world")
            .await;
        state
            .finish_assistant_message(&session.id, &assistant_message.id)
            .await
            .expect("assistant message should finish");

        let mut saw_complete_message = false;
        let mut saw_idle_status_after_complete_message = false;
        for _ in 0..4 {
            match events
                .recv()
                .await
                .expect("event should be broadcast")
                .event
            {
                SessionEvent::MessageCreated(message) if message.id == assistant_message.id => {
                    assert_eq!(message.content, "hello world");
                    saw_complete_message = true;
                }
                SessionEvent::SessionStatus(event)
                    if event.session_id == session.id
                        && matches!(event.status, SessionStatus::Idle)
                        && saw_complete_message =>
                {
                    saw_idle_status_after_complete_message = true;
                    break;
                }
                _ => {}
            }
        }

        assert!(saw_complete_message);
        assert!(saw_idle_status_after_complete_message);
    }

    #[tokio::test]
    async fn completed_assistant_unread_state_stays_in_sync_with_domain_read_state() {
        let state = test_state("unread-domain-sync").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .unwrap();
        let assistant = ChatMessage {
            id: "assistant-unread".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: "completed reply".to_string(),
            created_at: Utc::now(),
        };
        let turn_id = Uuid::new_v4().to_string();
        state
            .create_domain_turn(
                &session.id,
                &CreateTurnCommand {
                    command_id: Uuid::new_v4().to_string(),
                    turn_id: turn_id.clone(),
                    user_message_id: Uuid::new_v4().to_string(),
                    content: "question".to_string(),
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
        state.push_message(assistant.clone()).await;
        DOMAIN_TURN_ID
            .scope(
                turn_id,
                state.finish_assistant_message(&session.id, &assistant.id),
            )
            .await
            .unwrap();

        let legacy = state.get_session(&session.id).await.unwrap();
        let domain = state
            .domain_session_state(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.session.unread_count, 1);
        assert_eq!(domain.session.unread_count, 1);

        state
            .mark_session_read(&session.id, &assistant.id)
            .await
            .unwrap();
        let legacy = state.get_session(&session.id).await.unwrap();
        let domain = state
            .domain_session_state(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.session.unread_count, 0);
        assert_eq!(domain.session.unread_count, 0);
    }

    #[tokio::test]
    async fn recovery_interrupt_stops_the_active_turn_without_marking_it_failed() {
        let state = test_state("watchdog-recovery-interrupt").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .unwrap();
        let turn_id = Uuid::new_v4().to_string();
        state
            .create_domain_turn(
                &session.id,
                &CreateTurnCommand {
                    command_id: Uuid::new_v4().to_string(),
                    turn_id: turn_id.clone(),
                    user_message_id: Uuid::new_v4().to_string(),
                    content: "wait for a tool".to_string(),
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

        DOMAIN_TURN_ID
            .scope(
                turn_id,
                state.interrupt_turn_for_recovery(
                    &session.id,
                    "Codex lost the completion event".to_string(),
                ),
            )
            .await;

        let detail = state.get_session(&session.id).await.unwrap();
        assert!(matches!(detail.session.status, SessionStatus::Interrupted));
        let domain = state
            .domain_session_state(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(domain.turns[0].status, TurnStatus::Cancelled);
        assert!(domain.session.active_turn_id.is_none());
    }

    #[tokio::test]
    async fn approval_submission_rejects_wrong_or_unresolvable_request_without_forwarding() {
        let state = test_state("approval-validation").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .unwrap();
        let (approval_tx, mut approval_rx) = mpsc::unbounded_channel();
        state.set_approval_sender(&session.id, approval_tx).await;
        let mut request = ApprovalRequest {
            request_id: "approval-1".to_string(),
            kind: crate::models::ApprovalKind::CommandExecution,
            command: Some("cargo test".to_string()),
            reason: None,
            auto_approval_reason: None,
            auto_approval_reason_kind: None,
            allow_accept_for_session: false,
            allow_cancel: true,
            resolvable: false,
        };
        state.raise_approval(&session.id, request.clone()).await;

        let wrong = state
            .submit_approval(&session.id, "approval-other", ApprovalChoice::Accept)
            .await
            .unwrap_err();
        assert!(wrong.contains("unknown approval request"));
        assert!(approval_rx.try_recv().is_err());

        let unresolvable = state
            .submit_approval(&session.id, &request.request_id, ApprovalChoice::Accept)
            .await
            .unwrap_err();
        assert!(unresolvable.contains("cannot be resolved"));
        assert!(approval_rx.try_recv().is_err());

        request.resolvable = true;
        state.raise_approval(&session.id, request.clone()).await;
        state
            .submit_approval(&session.id, &request.request_id, ApprovalChoice::Accept)
            .await
            .unwrap();
        assert!(matches!(approval_rx.try_recv(), Ok(ApprovalChoice::Accept)));
    }

    #[tokio::test]
    async fn stale_read_acknowledgement_does_not_clear_newer_unread_message() {
        let state = test_state("stale-read-ack").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .unwrap();
        let now = Utc::now();
        let first = test_message(
            "assistant-first",
            &session.id,
            MessageRole::Assistant,
            "first reply",
            now,
        );
        let second = test_message(
            "assistant-second",
            &session.id,
            MessageRole::Assistant,
            "second reply",
            now + chrono::TimeDelta::seconds(1),
        );
        state.push_message(first.clone()).await;
        state.push_message(second.clone()).await;
        state
            .mark_session_unread(&session.id, &first.id)
            .await
            .unwrap();
        state
            .mark_session_unread(&session.id, &second.id)
            .await
            .unwrap();

        let stale = state
            .mark_session_read(&session.id, &first.id)
            .await
            .unwrap();
        assert_eq!(stale.unread_count, 1);
        assert_eq!(
            state
                .unread_message_ids
                .read()
                .await
                .get(&session.id)
                .map(String::as_str),
            Some(second.id.as_str())
        );

        let current = state
            .mark_session_read(&session.id, &second.id)
            .await
            .unwrap();
        assert_eq!(current.unread_count, 0);
        assert!(
            !state
                .unread_message_ids
                .read()
                .await
                .contains_key(&session.id)
        );
    }

    #[tokio::test]
    async fn finish_assistant_message_refreshes_provider_final_snapshot() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("finish-provider-final-snapshot", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;
        let assistant_message = ChatMessage {
            id: "local-assistant".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: "partial answer".to_string(),
            created_at: now,
        };
        state.push_message(assistant_message.clone()).await;
        provider
            .set_messages(
                "codex-thread-1",
                vec![test_message(
                    "provider-assistant",
                    "codex-thread-1",
                    MessageRole::Assistant,
                    "partial answer with final tail",
                    now + chrono::TimeDelta::seconds(2),
                )],
            )
            .await;
        let mut events = state.subscribe();

        state
            .finish_assistant_message(&session.id, &assistant_message.id)
            .await
            .expect("assistant message should finish");

        let mut saw_final_message = false;
        for _ in 0..3 {
            if let SessionEvent::MessageCreated(message) = events
                .recv()
                .await
                .expect("event should be broadcast")
                .event
            {
                if message.role == MessageRole::Assistant {
                    assert_eq!(message.content, "partial answer with final tail");
                    assert_eq!(message.created_at, now + chrono::TimeDelta::seconds(2));
                    saw_final_message = true;
                    break;
                }
            }
        }

        assert!(saw_final_message);
        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "partial answer with final tail");
        assert_eq!(messages[0].created_at, now + chrono::TimeDelta::seconds(2));
    }

    #[tokio::test]
    async fn finish_assistant_message_discards_aggregate_when_provider_has_individual_messages() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("finish-provider-message-aggregate", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        state
            .set_provider_session_ref(&session.id, Some("codex-thread-1".to_string()))
            .await;
        let assistant_message = ChatMessage {
            id: "local-assistant".to_string(),
            session_id: session.id.clone(),
            role: MessageRole::Assistant,
            content: "first update\n\n---\n\nfinal answer".to_string(),
            created_at: now,
        };
        state.push_message(assistant_message.clone()).await;
        provider
            .set_messages(
                "codex-thread-1",
                vec![
                    test_message(
                        "provider-assistant-1",
                        "codex-thread-1",
                        MessageRole::Assistant,
                        "first update",
                        now + chrono::TimeDelta::seconds(1),
                    ),
                    test_message(
                        "provider-assistant-2",
                        "codex-thread-1",
                        MessageRole::Assistant,
                        "final answer",
                        now + chrono::TimeDelta::seconds(2),
                    ),
                ],
            )
            .await;
        let mut events = state.subscribe();

        state
            .finish_assistant_message(&session.id, &assistant_message.id)
            .await
            .expect("assistant message should finish");

        let final_message = loop {
            if let SessionEvent::MessageCreated(message) = events
                .recv()
                .await
                .expect("event should be broadcast")
                .event
            {
                if message.role == MessageRole::Assistant {
                    break message;
                }
            }
        };
        assert_eq!(final_message.content, "final answer");

        let messages = state
            .list_messages(&session.id)
            .await
            .expect("messages should exist");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "first update");
        assert_eq!(messages[1].content, "final answer");
    }

    #[tokio::test]
    async fn list_and_detail_expose_runtime_session_ref() {
        let state = test_state("runtime-session-ref").await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::OpenCode,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .set_provider_session_ref(&session.id, Some("opencode-session-1".to_string()))
            .await;

        let listed = state.list_sessions().await;
        let listed_session = listed
            .into_iter()
            .find(|item| item.id == session.id)
            .expect("session should appear in list");
        assert_eq!(
            listed_session.runtime_session_ref.as_deref(),
            Some("opencode-session-1")
        );

        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");
        assert_eq!(
            detail.session.runtime_session_ref.as_deref(),
            Some("opencode-session-1")
        );
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: Some(crate::models::ReasoningEffort::Medium),
                model: None,
            })
            .await
            .expect("session should be created");

        let updated = state
            .update_session_settings(&session.id, None, Some(None), None)
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Old".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Original".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
                runtime_session_ref: None,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await;

        assert_eq!(detail.title, "Restarted Title");
    }

    #[tokio::test]
    async fn restart_reconciles_stale_legacy_approval_with_terminal_domain_state() {
        let settings_path = test_path("reconcile-approval-settings");
        let runtime_path = test_path("reconcile-approval-runtime");
        let metadata_path = test_path("reconcile-approval-metadata");
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        {
            let mut sessions = state.sessions.write().await;
            sessions.get_mut(&session.id).unwrap().status = SessionStatus::AwaitingApproval;
        }
        state.write_session_metadata().await.unwrap();
        state.recover_domain_active_turns().await.unwrap();
        let recovered = state.find_session(&session.id).await.unwrap();
        assert!(matches!(recovered.status, SessionStatus::Idle));
        assert!(recovered.pending_approval.is_none());
        let body = tokio::fs::read_to_string(metadata_path)
            .await
            .expect("metadata should be written");
        let metadata: PersistedSessionMetadata =
            serde_json::from_str(&body).expect("metadata should parse");
        let persisted = metadata.sessions[&session.id].session.as_ref().unwrap();
        assert!(matches!(persisted.status, SessionStatus::Idle));
        assert!(persisted.pending_approval.is_none());
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
    async fn cancel_turn_resolves_provider_session_ref_to_local_session() {
        let now = Utc::now();
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("cancel-provider-ref", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
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
        provider
            .set_session(SessionSummary {
                id: "codex-thread".to_string(),
                project_id: session.project_id.clone(),
                title: "Provider thread".to_string(),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                status: SessionStatus::Running,
                updated_at: Utc::now(),
                unread_count: 0,
                last_message_preview: Some("provider running".to_string()),
                pending_approval: None,
                runtime_session_ref: None,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await;
        state.messages.write().await.insert(
            session.id.clone(),
            vec![test_message(
                "a1",
                &session.id,
                MessageRole::Assistant,
                "latest",
                now,
            )],
        );

        let cancelled = state
            .cancel_turn("codex-thread")
            .await
            .expect("cancel should resolve provider session ref");
        let detail = state
            .get_session(&session.id)
            .await
            .expect("session detail should exist");

        assert!(cancelled);
        assert!(matches!(detail.session.status, SessionStatus::Interrupted));
    }

    #[tokio::test]
    async fn cancel_turn_falls_back_to_provider_cancel_when_runtime_missing() {
        let provider = Arc::new(TestMessageProvider::new(HashMap::new()));
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            provider.clone() as Arc<dyn AgentProvider>,
        )]));
        let state = test_state_with_providers("cancel-provider-fallback", registry).await;
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");

        state
            .patch_session(&session.id, SessionStatus::Idle, Some("idle".to_string()))
            .await;
        state
            .set_provider_session_ref(&session.id, Some("codex-thread".to_string()))
            .await;
        state.finish_turn(&session.id).await;

        let cancelled = state
            .cancel_turn(&session.id)
            .await
            .expect("provider fallback cancel should succeed");

        assert!(cancelled);
        assert_eq!(
            provider.cancelled_sessions().await,
            vec![(session.id.clone(), Some("codex-thread".to_string()))]
        );
    }

    #[tokio::test]
    async fn resolve_provider_config_auto_uses_priority_provider() {
        let state = test_state("provider-config-auto").await;
        state
            .update_bridge_settings(BridgeSettingsInput {
                ai_approval: Some(AiApprovalSettings::default()),
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
                acp_servers: None,
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
            runtime_session_ref: None,
            provider_id: Some(AUTO_PROVIDER_ID.to_string()),
            reasoning_effort: None,
            model: None,
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
                ai_approval: Some(AiApprovalSettings::default()),
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
                acp_servers: None,
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
            runtime_session_ref: None,
            provider_id: Some(AUTO_PROVIDER_ID.to_string()),
            reasoning_effort: None,
            model: None,
        };

        let resolved = state
            .resolve_provider_config(&session, &Some("codex-explicit".to_string()))
            .await;
        let resolved = resolved.expect("explicit provider should resolve");
        assert_eq!(resolved.base_url, "https://explicit.test/v1");
        assert_eq!(resolved.provider_id.as_deref(), Some("codex-explicit"));
    }

    #[tokio::test]
    async fn stale_turn_cannot_replace_or_clear_current_runtime_channels() {
        let state = test_state("stale-runtime-channels").await;
        let session_id = "session-runtime";
        state.runtime.lock().await.insert(
            session_id.to_string(),
            SessionRuntimeState {
                current_turn_id: Some("turn-new".to_string()),
                turn_in_flight: true,
                ..SessionRuntimeState::default()
            },
        );
        let (new_approval_tx, _new_approval_rx) = mpsc::unbounded_channel();
        let (new_cancel_tx, _new_cancel_rx) = mpsc::unbounded_channel();
        DOMAIN_TURN_ID
            .scope("turn-new".to_string(), async {
                state.set_approval_sender(session_id, new_approval_tx).await;
                state.set_cancel_sender(session_id, new_cancel_tx).await;
            })
            .await;
        DOMAIN_TURN_ID
            .scope("turn-old".to_string(), async {
                let (old_approval_tx, _old_approval_rx) = mpsc::unbounded_channel();
                let (old_cancel_tx, _old_cancel_rx) = mpsc::unbounded_channel();
                state.set_approval_sender(session_id, old_approval_tx).await;
                state.set_cancel_sender(session_id, old_cancel_tx).await;
                state.clear_approval_sender(session_id).await;
                state.finish_turn(session_id).await;
            })
            .await;
        let runtime = state.runtime.lock().await;
        let runtime = runtime.get(session_id).expect("runtime should remain");
        assert_eq!(runtime.current_turn_id.as_deref(), Some("turn-new"));
        assert!(runtime.turn_in_flight);
        assert!(runtime.approval_tx.is_some());
        assert!(runtime.cancel_tx.is_some());
    }

    #[tokio::test]
    async fn provider_task_panic_fails_domain_turn_and_clears_runtime() {
        let registry = ProviderRegistry::from_map(HashMap::from([(
            AgentKind::Codex,
            Arc::new(PanickingProvider) as Arc<dyn AgentProvider>,
        )]));
        let state = Arc::new(test_state_with_providers("provider-panic", registry).await);
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        let turn_id = Uuid::new_v4().to_string();
        state
            .create_domain_turn(
                &session.id,
                &CreateTurnCommand {
                    command_id: Uuid::new_v4().to_string(),
                    turn_id: turn_id.clone(),
                    user_message_id: Uuid::new_v4().to_string(),
                    content: "trigger panic".to_string(),
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
            .expect("turn should be created");

        state
            .send_domain_message(
                &session.id,
                &turn_id,
                SendMessageInput {
                    content: "trigger panic".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    client_message_id: None,
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .await
            .expect("message should be accepted");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state_snapshot = state
                    .domain_session_state(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
                if state_snapshot.turns[0].status == TurnStatus::Failed
                    && !state.turn_in_flight_for_test(&session.id).await
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panic supervisor should settle the turn");

        let state_snapshot = state
            .domain_session_state(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state_snapshot.session.status, DomainSessionStatus::Failed);
        assert_eq!(state_snapshot.session.active_turn_id, None);
        assert_eq!(state_snapshot.turns[0].status, TurnStatus::Failed);
    }

    #[tokio::test]
    async fn missing_provider_fails_domain_turn_and_releases_session_runtime() {
        let state = Arc::new(
            test_state_with_providers(
                "missing-provider-runtime",
                ProviderRegistry::from_map(HashMap::new()),
            )
            .await,
        );
        let project = state
            .create_project(CreateProjectInput {
                name: "Project".to_string(),
                root_path: std::env::temp_dir().to_string_lossy().to_string(),
            })
            .await;
        let session = state
            .create_session(CreateSessionInput {
                client_session_id: Uuid::new_v4().to_string(),
                project_id: project.id,
                title: Some("Session".to_string()),
                agent: AgentKind::Codex,
                brief_reply_mode: false,
                provider_id: None,
                reasoning_effort: None,
                model: None,
            })
            .await
            .expect("session should be created");
        let command = CreateTurnCommand {
            command_id: Uuid::new_v4().to_string(),
            turn_id: Uuid::new_v4().to_string(),
            user_message_id: Uuid::new_v4().to_string(),
            content: "first attempt".to_string(),
            attachments: Vec::new(),
            input_mode: InputMode::Text,
            system_prompt: None,
            expected_session_version: None,
            provider_id: None,
            reasoning_effort: None,
            model: None,
        };
        state
            .create_domain_turn(&session.id, &command)
            .await
            .expect("turn should be created");

        let error = state
            .send_domain_message(
                &session.id,
                &command.turn_id,
                SendMessageInput {
                    content: command.content.clone(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    client_message_id: Some(command.user_message_id.clone()),
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .await
            .expect_err("missing provider should fail");
        assert!(error.contains("unsupported agent"));
        assert!(!state.turn_in_flight_for_test(&session.id).await);

        let snapshot = state
            .domain_session_state(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.session.status, DomainSessionStatus::Failed);
        assert_eq!(snapshot.session.active_turn_id, None);
        assert_eq!(snapshot.turns[0].status, TurnStatus::Failed);

        let retry_error = state
            .send_message(
                &session.id,
                SendMessageInput {
                    content: "second attempt".to_string(),
                    input_mode: InputMode::Text,
                    system_prompt: None,
                    client_message_id: Some(Uuid::new_v4().to_string()),
                    provider_id: None,
                    reasoning_effort: None,
                    model: None,
                },
            )
            .await
            .expect_err("retry should reach provider resolution");
        assert!(retry_error.contains("unsupported agent"));
        assert!(!retry_error.contains("already processing"));
        assert!(!state.turn_in_flight_for_test(&session.id).await);
    }
}
