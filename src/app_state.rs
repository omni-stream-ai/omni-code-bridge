use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
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
        AgentErrorEvent, AgentKind, ApprovalChoice, ApprovalRequest, ApprovalRequestEvent,
        ApprovalResolvedEvent, ChatMessage, ClientAuthRecord, ClientAuthRequestInput,
        ClientAuthStatus, CreateProjectInput, CreateSessionInput, InputMode, MessageDeltaEvent,
        MessageRole, ProjectSummary, PushDeviceRegistration, RegisterPushDeviceInput,
        SendMessageInput, SessionEvent, SessionStatus, SessionStatusEvent, SessionSummary,
        TriggerClientMessageInput, TriggerClientMessageResult,
    },
    push::PushService,
    session_store::project_id_for_path,
};

#[derive(Default)]
struct SessionRuntimeState {
    provider_session_ref: Option<String>,
    pending_approval: Option<ApprovalRequest>,
    last_resolved_approval_request_id: Option<String>,
    approval_tx: Option<mpsc::UnboundedSender<ApprovalChoice>>,
    turn_abort: Option<AbortHandle>,
    turn_in_flight: bool,
}

#[derive(Clone)]
struct AggregatedListCache {
    checked_at: Instant,
    projects: Vec<ProjectSummary>,
    sessions: Vec<SessionSummary>,
    project_sessions: HashMap<String, Vec<SessionSummary>>,
    projects_by_id: HashMap<String, ProjectSummary>,
    sessions_by_id: HashMap<String, SessionSummary>,
}

pub struct AppState {
    projects: RwLock<HashMap<String, ProjectSummary>>,
    sessions: RwLock<HashMap<String, SessionSummary>>,
    messages: RwLock<HashMap<String, Vec<ChatMessage>>>,
    devices: RwLock<HashMap<String, PushDeviceRegistration>>,
    list_cache: Mutex<Option<AggregatedListCache>>,
    runtime: Mutex<HashMap<String, SessionRuntimeState>>,
    event_tx: broadcast::Sender<SessionEvent>,
    providers: ProviderRegistry,
    settings: BridgeSettingsStore,
    client_auth: ClientAuthStore,
    push: PushService,
}

impl AppState {
    const LIST_CACHE_TTL: Duration = Duration::from_secs(5);

    pub async fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            projects: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            messages: RwLock::new(HashMap::new()),
            devices: RwLock::new(load_device_registrations()),
            list_cache: Mutex::new(None),
            runtime: Mutex::new(HashMap::new()),
            event_tx,
            providers: ProviderRegistry::new(),
            settings: BridgeSettingsStore::load().await,
            client_auth: ClientAuthStore::load().await,
            push: PushService::new(),
        }
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

    pub async fn save_bridge_settings(
        &self,
        settings: BridgeSettings,
    ) -> Result<BridgeSettings, String> {
        self.settings
            .save(settings)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn list_projects(&self) -> Vec<ProjectSummary> {
        self.ensure_list_cache().await.projects
    }

    pub async fn list_sessions(&self) -> Vec<SessionSummary> {
        self.with_runtime_approvals(self.ensure_list_cache().await.sessions)
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
        Some(self.with_runtime_approvals(items).await)
    }

    async fn with_runtime_approvals(
        &self,
        mut sessions: Vec<SessionSummary>,
    ) -> Vec<SessionSummary> {
        let runtime = self.runtime.lock().await;
        for session in &mut sessions {
            session.pending_approval = runtime
                .get(&session.id)
                .and_then(|entry| entry.pending_approval.clone());
        }
        sessions
    }

    pub async fn list_messages(&self, session_id: &str) -> Option<Vec<ChatMessage>> {
        if let Some(messages) = self.messages.read().await.get(session_id).cloned() {
            return Some(messages);
        }
        let session = self.find_session(session_id).await?;
        let provider = self.provider_for_agent(session.agent)?;
        provider.list_messages(session_id).await
    }

    pub async fn create_project(&self, input: CreateProjectInput) -> ProjectSummary {
        let project = ProjectSummary {
            id: project_id_for_path(&input.root_path),
            name: input.name,
            root_path: input.root_path,
            updated_at: Utc::now(),
            session_count: 0,
            last_session_preview: Some("项目已创建".to_string()),
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
                pending_approval: None,
                last_resolved_approval_request_id: None,
                approval_tx: None,
                turn_abort: None,
                turn_in_flight: false,
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

    pub async fn send_message(
        self: &Arc<Self>,
        session_id: &str,
        input: SendMessageInput,
    ) -> Result<(ChatMessage, ChatMessage), String> {
        {
            let mut runtime = self.runtime.lock().await;
            let entry = runtime
                .entry(session_id.to_string())
                .or_insert_with(SessionRuntimeState::default);
            if entry.turn_in_flight {
                return Err("session is already processing a message".to_string());
            }
            entry.turn_in_flight = true;
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
                content: decorate_user_content(input),
                created_at: Utc::now(),
            };
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
                }));
            return Ok((user_message.clone(), user_message));
        }

        let user_message = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::User,
            content: decorate_user_content(input),
            created_at: Utc::now(),
        };
        let pending_reply = ChatMessage {
            id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            role: MessageRole::Assistant,
            content: String::new(),
            created_at: Utc::now(),
        };

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
            }));

        let state = Arc::clone(self);
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
                    pending_reply_for_task,
                )
                .await;
            if let Err(error) = result {
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
        let abort_handle = {
            let mut runtime = self.runtime.lock().await;
            let entry = runtime
                .get_mut(session_id)
                .ok_or_else(|| format!("unknown session: {session_id}"))?;
            let handle = entry.turn_abort.take();
            let had_active_turn = entry.turn_in_flight || handle.is_some();
            entry.turn_in_flight = false;
            entry.approval_tx = None;
            entry.pending_approval = None;
            (handle, had_active_turn)
        };

        let (abort_handle, had_active_turn) = abort_handle;
        if let Some(abort_handle) = abort_handle {
            abort_handle.abort();
        }

        if had_active_turn {
            let preview = self
                .latest_assistant_preview(session_id)
                .await
                .or(Some("已停止本次回答".to_string()));
            self.patch_session(session_id, SessionStatus::Idle, preview)
                .await;
            let _ = self
                .event_tx
                .send(SessionEvent::SessionStatus(SessionStatusEvent {
                    session_id: session_id.to_string(),
                    status: SessionStatus::Idle,
                }));
        }
        Ok(had_active_turn)
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
        self.clear_pending_approval(session_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Idle,
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
        self.clear_pending_approval(session_id).await;
        let _ = self
            .event_tx
            .send(SessionEvent::AgentError(AgentErrorEvent {
                session_id: session_id.to_string(),
                message,
            }));
        let _ = self
            .event_tx
            .send(SessionEvent::SessionStatus(SessionStatusEvent {
                session_id: session_id.to_string(),
                status: SessionStatus::Failed,
            }));
    }

    pub async fn set_provider_session_ref(&self, session_id: &str, session_ref: Option<String>) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        entry.provider_session_ref = session_ref;
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
        let mut runtime = self.runtime.lock().await;
        if let Some(entry) = runtime.get_mut(session_id) {
            entry.turn_in_flight = false;
            entry.turn_abort = None;
        }
    }

    pub async fn set_turn_abort(&self, session_id: &str, abort_handle: Option<AbortHandle>) {
        let mut runtime = self.runtime.lock().await;
        let entry = runtime
            .entry(session_id.to_string())
            .or_insert_with(SessionRuntimeState::default);
        entry.turn_abort = abort_handle;
    }

    pub async fn raise_approval(&self, session_id: &str, request: ApprovalRequest) {
        {
            let mut runtime = self.runtime.lock().await;
            let entry = runtime
                .entry(session_id.to_string())
                .or_insert_with(SessionRuntimeState::default);
            entry.pending_approval = Some(request.clone());
            entry.last_resolved_approval_request_id = None;
        }
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
        {
            let mut runtime = self.runtime.lock().await;
            if let Some(entry) = runtime.get_mut(session_id) {
                entry.pending_approval = None;
                entry.last_resolved_approval_request_id = Some(request_id.to_string());
            }
        }
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
            }));
    }

    async fn clear_pending_approval(&self, session_id: &str) {
        let mut runtime = self.runtime.lock().await;
        if let Some(entry) = runtime.get_mut(session_id) {
            entry.pending_approval = None;
        }
    }

    async fn push_message(&self, message: ChatMessage) {
        let mut messages = self.messages.write().await;
        messages
            .entry(message.session_id.clone())
            .or_default()
            .push(message);
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
        if let Some(session) = self.sessions.read().await.get(session_id).cloned() {
            return Some(session);
        }
        self.ensure_list_cache()
            .await
            .sessions_by_id
            .get(session_id)
            .cloned()
    }

    async fn invalidate_list_cache(&self) {
        *self.list_cache.lock().await = None;
    }

    async fn ensure_list_cache(&self) -> AggregatedListCache {
        {
            let cache = self.list_cache.lock().await;
            if let Some(existing) = cache.as_ref()
                && existing.checked_at.elapsed() < Self::LIST_CACHE_TTL
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
            SessionStatus::AwaitingApproval => 5,
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

fn approval_choice_preview(choice: &ApprovalChoice) -> &'static str {
    match choice {
        ApprovalChoice::Accept => "审批已允许",
        ApprovalChoice::AcceptForSession => "当前会话已允许后续同类请求",
        ApprovalChoice::AlwaysAllow => "已永久允许此类请求",
        ApprovalChoice::Decline => "审批已拒绝",
        ApprovalChoice::Cancel => "审批已取消",
    }
}

fn decorate_user_content(input: SendMessageInput) -> String {
    match input.input_mode {
        InputMode::Text => input.content,
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
