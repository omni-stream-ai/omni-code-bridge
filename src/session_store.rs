use std::{
    collections::HashMap,
    fs,
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::models::{
    AgentKind, ChatMessage, MessageRole, ProfileSummary, ProjectSummary, SessionStatus,
    SessionSummary,
};

/// Codex stores a named profile as `$CODEX_HOME/<name>.config.toml`; the base
/// `config.toml` is exposed as the `default` profile.
pub fn codex_profile_for_provider(provider: &str) -> Option<(String, String)> {
    let provider = provider.trim();
    if provider.is_empty() {
        return None;
    }
    codex_profile_provider_map().remove(provider)
}

fn codex_profile_provider_map() -> HashMap<String, (String, String)> {
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|value| PathBuf::from(value).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"));
    let mut candidates = vec![("default".to_string(), home.join("config.toml"))];
    if let Ok(entries) = fs::read_dir(&home) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if let Some(id) = name.strip_suffix(".config.toml") {
                candidates.push((id.to_string(), path));
            }
        }
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let mut profiles = HashMap::new();
    for (id, path) in candidates {
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("model_provider") && line.contains('=') {
                let value = line
                    .split_once('=')
                    .map(|(_, value)| value.trim())
                    .unwrap_or_default();
                let value = value.trim_matches('\"').trim_matches('\'');
                profiles
                    .entry(value.to_string())
                    .or_insert_with(|| (id.clone(), id));
                break;
            }
        }
    }
    profiles
}

pub fn codex_profile_for_session_file(path: &Path) -> Option<(String, String)> {
    let mut first_line = String::new();
    BufReader::new(fs::File::open(path).ok()?)
        .read_line(&mut first_line)
        .ok()?;
    let value: Value = serde_json::from_str(first_line.trim_end()).ok()?;
    let provider = value
        .pointer("/payload/model_provider")
        .and_then(Value::as_str)?;
    codex_profile_for_provider(provider)
}

pub fn codex_profile_summaries() -> Vec<ProfileSummary> {
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|value| PathBuf::from(value).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"));
    let mut ids = vec!["default".to_string()];
    if let Ok(entries) = fs::read_dir(&home) {
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(ToString::to_string) else {
                continue;
            };
            if let Some(id) = name.strip_suffix(".config.toml") {
                ids.push(id.to_string());
            }
        }
    }
    ids.sort();
    ids.dedup();
    ids.sort_by(
        |left, right| match (left == "default", right == "default") {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => left.cmp(right),
        },
    );
    ids.into_iter()
        .map(|id| ProfileSummary {
            name: id.clone(),
            id,
        })
        .collect()
}

pub fn codex_profile_for_session(session_id: &str) -> Option<(String, String)> {
    codex_session_profiles().remove(session_id)
}

pub fn codex_session_profiles() -> HashMap<String, (String, String)> {
    load_session_catalog_summary()
        .sessions
        .into_iter()
        .filter_map(|(id, session)| Some((id, (session.profile_id?, session.profile_name?))))
        .collect()
}

#[derive(Clone)]
struct CachedSessionHeader {
    fingerprint: u64,
    id: String,
    cwd: String,
    provider: Option<String>,
    created_at: Option<DateTime<Utc>>,
}

// Catalog and profile lookups share parsed headers. Large instruction fields in
// session_meta need parsing only when that transcript changes.
fn cached_session_header(path: &Path) -> Option<CachedSessionHeader> {
    static HEADERS: OnceLock<Mutex<HashMap<PathBuf, CachedSessionHeader>>> = OnceLock::new();
    let headers = HEADERS.get_or_init(|| Mutex::new(HashMap::new()));
    let fingerprint = file_fingerprint(path)?;
    if let Some(header) = headers.lock().ok()?.get(path)
        && header.fingerprint == fingerprint
    {
        return Some(header.clone());
    }
    let mut line = String::new();
    BufReader::new(fs::File::open(path).ok()?)
        .read_line(&mut line)
        .ok()?;
    let value: Value = serde_json::from_str(line.trim_end()).ok()?;
    let payload = value.get("payload")?;
    let header = CachedSessionHeader {
        fingerprint,
        id: payload.get("id")?.as_str()?.to_string(),
        cwd: payload.get("cwd")?.as_str()?.to_string(),
        provider: payload
            .get("model_provider")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        created_at: payload
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp),
    };
    headers
        .lock()
        .ok()?
        .insert(path.to_path_buf(), header.clone());
    Some(header)
}

pub struct SessionArchiveSummary {
    pub fingerprint: u64,
    pub projects: HashMap<String, ProjectSummary>,
    pub sessions: HashMap<String, SessionSummary>,
    pub session_files: HashMap<String, PathBuf>,
}

#[derive(Clone)]
struct CachedDatabaseTitles {
    fingerprint: u64,
    revision: u64,
    titles: Arc<HashMap<String, String>>,
}

fn load_session_database_titles(root: &Path) -> Option<CachedDatabaseTitles> {
    static TITLES: OnceLock<Mutex<HashMap<PathBuf, CachedDatabaseTitles>>> = OnceLock::new();
    let path = fs::read_dir(root.parent()?)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let version = path
                .file_name()?
                .to_str()?
                .strip_prefix("state_")?
                .strip_suffix(".sqlite")?
                .parse::<u32>()
                .ok()?;
            Some((version, path))
        })
        .max_by_key(|(version, _)| *version)?
        .1;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    file_fingerprint(&path)?.hash(&mut hasher);
    // WAL contains committed title changes; SHM changes on reads as well.
    file_fingerprint(&path.with_extension("sqlite-wal")).hash(&mut hasher);
    let fingerprint = hasher.finish();
    let cache = TITLES.get_or_init(|| Mutex::new(HashMap::new()));
    let previous = cache.lock().ok()?.get(&path).cloned();
    if let Some(cached) = &previous
        && cached.fingerprint == fingerprint
    {
        return previous;
    }

    let titles = match read_session_database_titles(&path) {
        Ok(titles) => titles,
        // Keep the last successful metadata snapshot during a temporary lock.
        Err(_) => return previous,
    };
    let mut ordered: Vec<_> = titles.iter().collect();
    ordered.sort_by_key(|(id, _)| *id);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    ordered.hash(&mut hasher);
    let loaded = CachedDatabaseTitles {
        fingerprint,
        revision: hasher.finish(),
        titles: Arc::new(titles),
    };
    cache.lock().ok()?.insert(path, loaded.clone());
    Some(loaded)
}

fn read_session_database_titles(path: &Path) -> rusqlite::Result<HashMap<String, String>> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(std::time::Duration::from_millis(20))?;
    let columns = connection
        .prepare("PRAGMA table_info(threads)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // Older state databases do not have name or first_user_message.
    let candidates: Vec<_> = ["name", "title", "first_user_message"]
        .into_iter()
        .filter(|column| columns.iter().any(|existing| existing == column))
        .map(|column| format!("NULLIF(TRIM({column}), '')"))
        .chain(std::iter::once("NULL".to_string()))
        .collect();
    let mut statement = connection.prepare(&format!(
        "SELECT id, COALESCE({}) FROM threads",
        candidates.join(", ")
    ))?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    let mut titles = HashMap::new();
    for row in rows {
        let (id, title) = row?;
        if let Some(title) = title {
            let title = normalize_title_candidate(&title);
            if !title.is_empty() {
                titles.insert(id, title);
            }
        }
    }
    Ok(titles)
}

fn cached_transcript_title(path: &Path) -> Option<String> {
    type TitleCache = HashMap<PathBuf, (u64, Option<String>)>;
    static TITLES: OnceLock<Mutex<TitleCache>> = OnceLock::new();
    let cache = TITLES.get_or_init(|| Mutex::new(HashMap::new()));
    let fingerprint = file_fingerprint(path)?;
    if let Some((cached_fingerprint, title)) = cache.lock().ok()?.get(path)
        && *cached_fingerprint == fingerprint
    {
        return title.clone();
    }
    // Only legacy/unindexed sessions need this fallback. Bound the read even
    // when a transcript starts with large instruction or image records.
    let reader = BufReader::new(fs::File::open(path).ok()?.take(1024 * 1024));
    let mut fallback = None;
    for line in reader.lines().take(256).filter_map(Result::ok) {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if value
            .pointer("/metadata/compaction_output")
            .and_then(Value::as_bool)
            == Some(true)
        {
            continue;
        }
        let payload = value.get("payload").unwrap_or(&Value::Null);
        let text = match value.get("type").and_then(Value::as_str) {
            Some("response_item")
                if payload.get("role").and_then(Value::as_str) == Some("user") =>
            {
                extract_response_item_user_text(payload)
            }
            Some("event_msg") => {
                let completed = completed_message_event(payload);
                let payload = completed.as_ref().unwrap_or(payload);
                match payload.get("type").and_then(Value::as_str) {
                    Some("user_message") => payload
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    Some("thread_goal_updated") => {
                        extract_goal_objective(payload).map(|text| format!("/goal {text}"))
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        if let Some(text) = text {
            let title = normalize_title_candidate(&text);
            if !title.is_empty() {
                let generic = is_generic_title_candidate(&title);
                if !generic {
                    fallback = Some(title);
                    break;
                }
                fallback.get_or_insert(title);
            }
        }
    }
    cache
        .lock()
        .ok()?
        .insert(path.to_path_buf(), (fingerprint, fallback.clone()));
    fallback
}

/// Detect archive and title metadata changes without parsing transcript contents.
pub(crate) fn session_archive_fingerprint() -> Option<u64> {
    let root = sessions_root();
    let (_, mut fingerprint) = collect_jsonl_files(&root).ok()?;
    if let Some(index) = root
        .parent()
        .map(|parent| parent.join("session_index.jsonl"))
        && let Some(index_fingerprint) = file_fingerprint(&index)
    {
        fingerprint ^= index_fingerprint;
    }
    if let Some(titles) = load_session_database_titles(&root) {
        fingerprint ^= titles.revision;
    }
    Some(fingerprint)
}

/// Appending transcript events does not change the lightweight catalog, whose
/// revision tracks title metadata rather than database activity or token usage.
pub(crate) fn session_catalog_revision() -> Option<u64> {
    session_catalog_revision_for_root(&sessions_root())
}

fn session_catalog_revision_for_root(root: &Path) -> Option<u64> {
    let (paths, _) = collect_jsonl_files(root).ok()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.hash(&mut hasher);
    for path in paths {
        path.hash(&mut hasher);
        fs::metadata(&path)
            .ok()
            .map(|metadata| metadata.len() > 0)
            .hash(&mut hasher);
    }
    if let Some(index) = root
        .parent()
        .map(|parent| parent.join("session_index.jsonl"))
    {
        file_fingerprint(&index).hash(&mut hasher);
    }
    load_session_database_titles(root)
        .map(|titles| titles.revision)
        .hash(&mut hasher);
    Some(hasher.finish())
}

/// Build a lightweight Codex catalog without parsing the full JSONL history.
/// The archive can be several gigabytes; list views only need session metadata.
pub fn load_session_catalog_summary() -> SessionArchiveSummary {
    load_session_catalog_summary_for_root(&sessions_root())
}

fn load_session_catalog_summary_for_root(root: &Path) -> SessionArchiveSummary {
    let (paths, mut fingerprint) = match collect_jsonl_files(root) {
        Ok(result) => result,
        Err(_) => {
            return SessionArchiveSummary {
                fingerprint: 0,
                projects: HashMap::new(),
                sessions: HashMap::new(),
                session_files: HashMap::new(),
            };
        }
    };
    let index = load_session_index(root);
    let database_titles = load_session_database_titles(root);
    let provider_profiles = codex_profile_provider_map();
    let mut archive = SessionArchiveSummary {
        fingerprint,
        projects: HashMap::new(),
        sessions: HashMap::new(),
        session_files: HashMap::new(),
    };
    for path in paths {
        let Some(header) = cached_session_header(&path) else {
            continue;
        };
        let id = header.id.as_str();
        let cwd = header.cwd.as_str();
        let index_entry = index.get(id);
        let updated_at = index_entry
            .and_then(|entry| entry.1.as_deref())
            .and_then(parse_timestamp)
            .or(header.created_at)
            .unwrap_or_else(|| {
                fs::metadata(&path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|value| DateTime::<Utc>::from_timestamp(value.as_secs() as i64, 0))
                    .unwrap_or_else(Utc::now)
            });
        let title = index_entry
            .and_then(|entry| entry.0.clone())
            .filter(|value| !value.trim().is_empty())
            .or_else(|| database_titles.as_ref()?.titles.get(id).cloned())
            .or_else(|| cached_transcript_title(&path))
            .unwrap_or_else(|| "Codex session".to_string());
        let provider = header.provider.as_deref();
        let profile = provider
            .and_then(|value| provider_profiles.get(value))
            .cloned();
        let project_id = project_id_for_path(cwd);
        let session = SessionSummary {
            id: id.to_string(),
            project_id: project_id.clone(),
            title: truncate_preview(&title, 32),
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status: SessionStatus::Idle,
            updated_at,
            unread_count: 0,
            last_message_preview: Some(title.clone()),
            pending_approval: None,
            runtime_session_ref: Some(id.to_string()),
            provider_id: None,
            reasoning_effort: None,
            model: None,
            profile_id: profile.as_ref().map(|value| value.0.clone()),
            profile_name: profile.map(|value| value.1),
        };
        archive.session_files.insert(id.to_string(), path);
        archive
            .projects
            .entry(project_id.clone())
            .and_modify(|project| {
                project.session_count += 1;
                if updated_at > project.updated_at {
                    project.updated_at = updated_at;
                    project.last_session_preview = Some(title.clone());
                }
            })
            .or_insert_with(|| ProjectSummary {
                id: project_id,
                name: project_name_from_path(cwd),
                root_path: cwd.to_string(),
                updated_at,
                session_count: 1,
                last_session_preview: Some(title),
                git_branch: None,
                git_status: None,
            });
        archive.sessions.insert(id.to_string(), session);
    }
    if let Some(path) = root
        .parent()
        .map(|parent| parent.join("session_index.jsonl"))
    {
        if let Some(value) = file_fingerprint(&path) {
            fingerprint ^= value;
        }
    }
    if let Some(titles) = database_titles {
        fingerprint ^= titles.revision;
    }
    archive.fingerprint = fingerprint;
    archive
}

fn load_session_index(root: &Path) -> HashMap<String, (Option<String>, Option<String>)> {
    let Some(path) = root
        .parent()
        .map(|parent| parent.join("session_index.jsonl"))
    else {
        return HashMap::new();
    };
    let Ok(file) = fs::File::open(path) else {
        return HashMap::new();
    };
    BufReader::new(file)
        .lines()
        .filter_map(Result::ok)
        .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
        .filter_map(|value| {
            let id = value.get("id").and_then(Value::as_str)?.to_string();
            let title = value
                .get("thread_name")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let updated = value
                .get("updated_at")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            Some((id, (title, updated)))
        })
        .collect()
}

pub struct SessionMessages {
    pub fingerprint: u64,
    pub messages: Vec<ChatMessage>,
}

struct PendingAssistantMessage {
    created_at: DateTime<Utc>,
    blocks: Vec<String>,
}

struct ParsedSessionSummaryRecord {
    project: ProjectSummary,
    session: SessionSummary,
    session_file: PathBuf,
}

pub fn load_session_archive_summary() -> SessionArchiveSummary {
    let root = sessions_root();
    let (paths, fingerprint) = match collect_jsonl_files(&root) {
        Ok(result) => result,
        Err(_) => {
            return SessionArchiveSummary {
                fingerprint: 0,
                projects: HashMap::new(),
                sessions: HashMap::new(),
                session_files: HashMap::new(),
            };
        }
    };

    let mut archive = SessionArchiveSummary {
        fingerprint,
        projects: HashMap::new(),
        sessions: HashMap::new(),
        session_files: HashMap::new(),
    };

    for path in paths {
        let Some(record) = parse_session_summary_file(&path) else {
            continue;
        };
        archive
            .session_files
            .insert(record.session.id.clone(), record.session_file.clone());
        archive
            .projects
            .entry(record.project.id.clone())
            .and_modify(|project| {
                project.session_count += 1;
                if record.project.updated_at > project.updated_at {
                    project.updated_at = record.project.updated_at;
                    project.last_session_preview = record.project.last_session_preview.clone();
                }
            })
            .or_insert(record.project);
        archive
            .sessions
            .insert(record.session.id.clone(), record.session);
    }

    archive
}

pub fn load_session_messages(path: &Path) -> Option<SessionMessages> {
    let fingerprint = file_fingerprint(path)?;
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut messages = Vec::new();
    let mut pending_assistant: Option<PendingAssistantMessage> = None;
    let mut user_mirror: Option<UserMessageMirror> = None;
    let mut saw_agent_message = false;
    let mut active_goal_objective: Option<String> = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if value
            .pointer("/metadata/compaction_output")
            .and_then(Value::as_bool)
            == Some(true)
        {
            continue;
        }
        let completed_message = (line_type == "event_msg")
            .then(|| completed_message_event(payload))
            .flatten();
        let payload = completed_message.as_ref().unwrap_or(payload);
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now);

        match line_type {
            "session_meta" => {
                session_id = payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_started") => {
                user_mirror = None;
                saw_agent_message = false;
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_complete") => {
                user_mirror = None;
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
            }
            "event_msg"
                if payload.get("type").and_then(Value::as_str) == Some("thread_goal_updated") =>
            {
                if let Some(objective) = extract_goal_objective(payload) {
                    let status = payload
                        .pointer("/goal/status")
                        .and_then(Value::as_str)
                        .unwrap_or("active");
                    let terminal = matches!(
                        status,
                        "complete" | "completed" | "cancelled" | "canceled" | "failed"
                    );
                    let should_display =
                        !terminal && active_goal_objective.as_deref() != Some(objective);
                    if terminal {
                        if active_goal_objective.as_deref() == Some(objective) {
                            active_goal_objective = None;
                        }
                    } else {
                        active_goal_objective = Some(objective.to_string());
                    }
                    if !should_display {
                        continue;
                    }
                    flush_pending_assistant(
                        &mut messages,
                        &session_id.clone().unwrap_or_default(),
                        &mut pending_assistant,
                    );
                    messages.push(ChatMessage {
                        id: format!(
                            "{}-user-{}",
                            session_id.as_deref().unwrap_or("unknown"),
                            messages.len()
                        ),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::User,
                        content: format!("/goal {objective}"),
                        created_at: timestamp,
                    });
                }
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("user_message") => {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
                if let Some(text) = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    record_user_message(
                        &mut messages,
                        session_id.as_deref().unwrap_or_default(),
                        timestamp,
                        UserMessageMirror {
                            text: text.to_string(),
                            event: Some(payload.clone()),
                            images: Vec::new(),
                            index: 0,
                        },
                        &mut user_mirror,
                    );
                }
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("agent_message") => {
                user_mirror = None;
                saw_agent_message = true;
                pending_assistant = None;
                if let Some(text) = extract_agent_message_text(payload) {
                    messages.push(ChatMessage {
                        id: format!(
                            "{}-assistant-{}",
                            session_id.as_deref().unwrap_or("unknown"),
                            messages.len()
                        ),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::Assistant,
                        content: text,
                        created_at: timestamp,
                    });
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("user") =>
            {
                if let Some(text) = extract_response_item_user_text(payload) {
                    flush_pending_assistant(
                        &mut messages,
                        session_id.as_deref().unwrap_or_default(),
                        &mut pending_assistant,
                    );
                    record_user_message(
                        &mut messages,
                        session_id.as_deref().unwrap_or_default(),
                        timestamp,
                        UserMessageMirror {
                            text,
                            event: None,
                            images: extract_response_item_user_images(payload),
                            index: 0,
                        },
                        &mut user_mirror,
                    );
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("assistant") =>
            {
                user_mirror = None;
                if saw_agent_message {
                    continue;
                }
                if let Some(text) = extract_assistant_text(payload) {
                    flush_pending_assistant(
                        &mut messages,
                        session_id.as_deref().unwrap_or_default(),
                        &mut pending_assistant,
                    );
                    let pending =
                        pending_assistant.get_or_insert_with(|| PendingAssistantMessage {
                            created_at: timestamp,
                            blocks: Vec::new(),
                        });
                    pending.blocks.push(text);
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("function_call") =>
            {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
                if let Some(content) = render_codex_function_call(payload) {
                    messages.push(ChatMessage {
                        id: format!(
                            "{}-system-{}",
                            session_id.as_deref().unwrap_or("unknown"),
                            messages.len()
                        ),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::System,
                        content,
                        created_at: timestamp,
                    });
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("function_call_output") =>
            {
                flush_pending_assistant(
                    &mut messages,
                    &session_id.clone().unwrap_or_default(),
                    &mut pending_assistant,
                );
                if let Some(content) = render_codex_function_call_output(payload) {
                    messages.push(ChatMessage {
                        id: format!(
                            "{}-system-{}",
                            session_id.as_deref().unwrap_or("unknown"),
                            messages.len()
                        ),
                        session_id: session_id.clone().unwrap_or_default(),
                        role: MessageRole::System,
                        content,
                        created_at: timestamp,
                    });
                }
            }
            _ => {}
        }
    }

    flush_pending_assistant(
        &mut messages,
        &session_id.clone().unwrap_or_default(),
        &mut pending_assistant,
    );

    let session_id = session_id?;
    for message in &mut messages {
        message.session_id = session_id.clone();
    }

    Some(SessionMessages {
        fingerprint,
        messages,
    })
}

struct UserMessageMirror {
    text: String,
    event: Option<Value>,
    images: Vec<String>,
    index: usize,
}

/// Older archives can mirror a user turn in either order. Pair only opposite
/// record types without intervening conversation output, preserving repeated
/// identical requests from separate turns.
fn record_user_message(
    messages: &mut Vec<ChatMessage>,
    session_id: &str,
    timestamp: DateTime<Utc>,
    mut incoming: UserMessageMirror,
    mirror: &mut Option<UserMessageMirror>,
) {
    if let Some(previous) = mirror.take()
        && previous.index + 1 == messages.len()
        && previous.text == incoming.text
        && previous.event.is_some() != incoming.event.is_some()
    {
        let (event, images) = if let Some(event) = incoming.event.as_ref() {
            messages[previous.index].created_at = timestamp;
            (event, &previous.images)
        } else {
            (previous.event.as_ref().unwrap(), &incoming.images)
        };
        messages[previous.index].content =
            render_user_message_markdown(&incoming.text, event, images);
        return;
    }
    incoming.index = messages.len();
    messages.push(ChatMessage {
        id: format!("{session_id}-user-{}", messages.len()),
        session_id: session_id.to_string(),
        role: MessageRole::User,
        content: render_user_message_markdown(
            &incoming.text,
            incoming.event.as_ref().unwrap_or(&Value::Null),
            &incoming.images,
        ),
        created_at: timestamp,
    });
    *mirror = Some(incoming);
}

fn extract_response_item_user_text(payload: &Value) -> Option<String> {
    // Codex also serializes injected context with role=user. Its metadata
    // distinguishes real user inputs from environment and continuation data.
    let kinds = payload
        .pointer("/internal_chat_message_metadata_passthrough/content_item_kinds")
        .and_then(Value::as_array);
    if let Some(kinds) = kinds
        && !kinds
            .iter()
            .filter_map(Value::as_str)
            .any(|kind| kind.starts_with("user."))
    {
        return None;
    }
    let text = payload
        .get("content")?
        .as_array()?
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("input_text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .filter(|text| {
            let text = text.trim();
            !(text == "</image>" || (text.starts_with("<image ") && text.ends_with('>')))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.trim();
    if text.is_empty()
        || (kinds.is_none()
            && [
                "<environment_context>",
                "<codex_internal_context",
                "# AGENTS.md instructions for ",
                "<subagent_notification>",
            ]
            .iter()
            .any(|prefix| text.starts_with(prefix)))
    {
        return None;
    }
    Some(text.to_string())
}

fn extract_goal_objective(payload: &Value) -> Option<&str> {
    payload
        .pointer("/goal/objective")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

fn render_user_message_markdown(
    message: &str,
    payload: &Value,
    response_item_images: &[String],
) -> String {
    let placeholders = payload
        .get("text_elements")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("placeholder").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let image_destinations = collect_user_image_destinations(payload, response_item_images);

    if image_destinations.is_empty() {
        return message.to_string();
    }

    let mut content = message.to_string();
    let mut used = 0usize;
    for (index, (placeholder, destination)) in placeholders
        .iter()
        .zip(image_destinations.iter())
        .enumerate()
    {
        let alt_text = image_alt_text(placeholder, index + 1);
        let markdown = format!("![{}]({destination})", escape_markdown_alt_text(&alt_text));
        content = content.replacen(placeholder, &markdown, 1);
        used += 1;
    }

    for (index, destination) in image_destinations.iter().skip(used).enumerate() {
        let alt_text = format!("Image #{}", used + index + 1);
        if !content.is_empty() {
            content.push_str("\n\n");
        }
        content.push_str(&format!(
            "![{}]({destination})",
            escape_markdown_alt_text(&alt_text)
        ));
    }

    content
}

fn collect_user_image_destinations(
    payload: &Value,
    response_item_images: &[String],
) -> Vec<String> {
    let mut destinations = Vec::new();

    destinations.extend(
        response_item_images
            .iter()
            .map(String::as_str)
            .filter_map(user_image_destination_from_raw),
    );

    if !destinations.is_empty() {
        return destinations;
    }

    if let Some(local_images) = payload.get("local_images").and_then(Value::as_array) {
        destinations.extend(
            local_images
                .iter()
                .filter_map(user_image_destination_from_value),
        );
    }

    if let Some(images) = payload.get("images").and_then(Value::as_array) {
        destinations.extend(images.iter().filter_map(user_image_destination_from_value));
    }

    destinations
}

fn extract_response_item_user_images(payload: &Value) -> Vec<String> {
    payload
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("input_image"))
                .filter_map(|item| {
                    item.get("image_url")
                        .and_then(Value::as_str)
                        .or_else(|| item.get("url").and_then(Value::as_str))
                })
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn user_image_destination_from_value(value: &Value) -> Option<String> {
    let raw = value
        .as_str()
        .or_else(|| value.get("path").and_then(Value::as_str))
        .or_else(|| value.get("url").and_then(Value::as_str))
        .or_else(|| value.get("image_url").and_then(Value::as_str))?;

    user_image_destination_from_raw(raw)
}

fn user_image_destination_from_raw(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    Some(format!("<{raw}>"))
}

fn image_alt_text(placeholder: &str, fallback_index: usize) -> String {
    let trimmed = placeholder.trim();
    let stripped = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .map(str::trim)
        .filter(|value| !value.is_empty());

    stripped
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("Image #{fallback_index}"))
}

fn escape_markdown_alt_text(value: &str) -> String {
    value.replace('[', r"\[").replace(']', r"\]")
}

fn flush_pending_assistant(
    messages: &mut Vec<ChatMessage>,
    session_id: &str,
    pending_assistant: &mut Option<PendingAssistantMessage>,
) {
    let Some(pending) = pending_assistant.take() else {
        return;
    };
    let content = pending
        .blocks
        .into_iter()
        .map(|block| block.trim().to_string())
        .filter(|block| !block.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    if content.is_empty() {
        return;
    }
    messages.push(ChatMessage {
        id: format!("{}-assistant-{}", session_id, messages.len()),
        session_id: session_id.to_string(),
        role: MessageRole::Assistant,
        content,
        created_at: pending.created_at,
    });
}

fn extract_agent_message_text(payload: &Value) -> Option<String> {
    payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
}

/// Newer Codex archives publish visible messages as completed thread items.
/// Normalize those events so user mirrors, images and assistant boundaries use
/// the same projection as older archives.
fn completed_message_event(payload: &Value) -> Option<Value> {
    if payload.get("type").and_then(Value::as_str) != Some("item_completed") {
        return None;
    }
    let item = payload.get("item")?;
    let event_type = match item.get("type").and_then(Value::as_str)? {
        "UserMessage" => "user_message",
        "AgentMessage" => "agent_message",
        _ => return None,
    };
    let content = item.get("content")?.as_array()?;
    let text = content
        .iter()
        .filter(|part| {
            matches!(
                part.get("type").and_then(Value::as_str),
                Some("Text" | "text")
            )
        })
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    let local_images = content
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("local_image"))
        .filter_map(|part| part.get("path").and_then(Value::as_str))
        .collect::<Vec<_>>();
    let images = content
        .iter()
        .filter(|part| {
            matches!(
                part.get("type").and_then(Value::as_str),
                Some("image" | "input_image")
            )
        })
        .collect::<Vec<_>>();
    let text_elements = content
        .iter()
        .filter_map(|part| part.get("text_elements").and_then(Value::as_array))
        .flatten()
        .collect::<Vec<_>>();
    Some(serde_json::json!({
        "type": event_type,
        "message": text,
        "local_images": local_images,
        "images": images,
        "text_elements": text_elements,
    }))
}

fn parse_session_summary_file(path: &Path) -> Option<ParsedSessionSummaryRecord> {
    let content = fs::read_to_string(path).ok()?;
    let mut session_id = None;
    let mut cwd = None;
    let mut updated_at = None;
    let mut user_message_candidates = Vec::new();
    let mut last_preview = None;
    let mut status = SessionStatus::Idle;
    let mut saw_agent_message = false;
    let mut model_provider = None;

    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        let line_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if value
            .pointer("/metadata/compaction_output")
            .and_then(Value::as_bool)
            == Some(true)
        {
            continue;
        }
        let completed_message = (line_type == "event_msg")
            .then(|| completed_message_event(payload))
            .flatten();
        let payload = completed_message.as_ref().unwrap_or(payload);
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp);

        match line_type {
            "session_meta" => {
                session_id = payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                cwd = payload
                    .get("cwd")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                updated_at = timestamp.or_else(|| {
                    payload
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .and_then(parse_timestamp)
                });
                model_provider = payload
                    .get("model_provider")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_started") => {
                saw_agent_message = false;
                status = SessionStatus::Running;
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_complete") => {
                status = SessionStatus::Idle;
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("turn_aborted") => {
                status = SessionStatus::Interrupted;
            }
            "event_msg"
                if payload.get("type").and_then(Value::as_str) == Some("thread_goal_updated") =>
            {
                if let Some(objective) = extract_goal_objective(payload) {
                    let command = format!("/goal {objective}");
                    user_message_candidates.push(command.clone());
                    last_preview = Some(command);
                }
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("user_message") => {
                let text = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(ToString::to_string);
                if let Some(text) = text {
                    user_message_candidates.push(text.clone());
                    last_preview = Some(text);
                }
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("agent_message") => {
                saw_agent_message = true;
                if let Some(text) = extract_agent_message_text(payload) {
                    last_preview = Some(text);
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("assistant") =>
            {
                if saw_agent_message {
                    continue;
                }
                if let Some(text) = extract_assistant_text(payload) {
                    last_preview = Some(text);
                }
            }
            "response_item"
                if payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload.get("role").and_then(Value::as_str) == Some("user") =>
            {
                if let Some(text) = extract_response_item_user_text(payload) {
                    user_message_candidates.push(text.clone());
                    last_preview = Some(text);
                }
            }
            _ => {}
        }

        if let Some(timestamp) = timestamp {
            updated_at = Some(updated_at.map_or(timestamp, |current| current.max(timestamp)));
        }
    }

    let session_id = session_id?;
    let cwd = cwd?;
    let updated_at = updated_at.unwrap_or_else(Utc::now);
    let project_id = project_id_for_path(&cwd);
    let session_title = preferred_session_title(&user_message_candidates)
        .map(|text| truncate_preview(&text, 32))
        .unwrap_or_else(|| file_stem_or_unknown(path));
    let (profile_id, profile_name) = model_provider
        .as_deref()
        .and_then(codex_profile_for_provider)
        .map_or((None, None), |(id, name)| (Some(id), Some(name)));

    Some(ParsedSessionSummaryRecord {
        project: ProjectSummary {
            id: project_id.clone(),
            name: project_name_from_path(&cwd),
            root_path: cwd.clone(),
            updated_at,
            session_count: 1,
            last_session_preview: last_preview.clone(),
            git_branch: None,
            git_status: None,
        },
        session: SessionSummary {
            id: session_id.clone(),
            project_id,
            title: session_title,
            agent: AgentKind::Codex,
            brief_reply_mode: false,
            status,
            updated_at,
            unread_count: 0,
            last_message_preview: last_preview,
            pending_approval: None,
            runtime_session_ref: Some(session_id),
            provider_id: None,
            reasoning_effort: None,
            model: None,
            profile_id,
            profile_name,
        },
        session_file: path.to_path_buf(),
    })
}

fn preferred_session_title(messages: &[String]) -> Option<String> {
    let mut fallback = None;

    for message in messages {
        let normalized = normalize_title_candidate(message);
        if normalized.is_empty() {
            continue;
        }
        if fallback.is_none() {
            fallback = Some(normalized.clone());
        }
        if !is_generic_title_candidate(&normalized) {
            return Some(normalized);
        }
    }

    fallback
}

fn normalize_title_candidate(text: &str) -> String {
    text.replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '`')
        .to_string()
}

fn is_generic_title_candidate(text: &str) -> bool {
    matches!(
        text,
        "继续"
            | "继续。"
            | "继续做"
            | "继续吧"
            | "继续一下"
            | "继续处理"
            | "继续帮我做"
            | "继续执行"
    )
}

fn collect_jsonl_files(root: &Path) -> std::io::Result<(Vec<PathBuf>, u64)> {
    let mut files = Vec::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if !root.exists() {
        return Ok((files, 0));
    }
    collect_jsonl_files_inner(root, &mut files, &mut hasher)?;
    files.sort();
    Ok((files, hasher.finish()))
}

fn collect_jsonl_files_inner(
    root: &Path,
    files: &mut Vec<PathBuf>,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files_inner(&path, files, hasher)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            if let Some(fingerprint) = file_fingerprint(&path) {
                path.hash(hasher);
                fingerprint.hash(hasher);
            }
            files.push(path);
        }
    }
    Ok(())
}

pub(crate) fn file_fingerprint(path: &Path) -> Option<u64> {
    let metadata = fs::metadata(path).ok()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metadata.len().hash(&mut hasher);
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos()
        .hash(&mut hasher);
    Some(hasher.finish())
}

fn extract_assistant_text(payload: &Value) -> Option<String> {
    payload
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| match item.get("type").and_then(Value::as_str) {
                    Some("output_text") => item.get("text").and_then(Value::as_str),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn render_codex_function_call(payload: &Value) -> Option<String> {
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("tool");
    let call_id = payload
        .get("call_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let arguments = payload
        .get("arguments")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let mut content = format!("[tool:{name}]");
    if let Some(call_id) = call_id {
        content.push_str(&format!(" call_id={call_id}"));
    }
    if let Some(arguments) = arguments {
        content.push('\n');
        content.push_str(arguments);
    }
    Some(content)
}

fn render_codex_function_call_output(payload: &Value) -> Option<String> {
    let output = payload
        .get("output")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let call_id = payload
        .get("call_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let mut content = "[tool:output]".to_string();
    if let Some(call_id) = call_id {
        content.push_str(&format!(" call_id={call_id}"));
    }
    content.push('\n');
    content.push_str(output);
    Some(content)
}

fn sessions_root() -> PathBuf {
    std::env::var("OMNI_CODE_CODEX_SESSIONS_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("CODEX_HOME").map(|home| PathBuf::from(home).join("sessions")))
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".codex/sessions")))
        .unwrap_or_else(|_| PathBuf::from(".codex/sessions"))
}

pub fn project_id_for_path(path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn project_name_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

fn file_stem_or_unknown(path: &Path) -> String {
    path.file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("codex-session")
        .to_string()
}

pub fn truncate_preview(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        trimmed.to_string()
    } else {
        chars[..max_chars].iter().collect::<String>()
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_fixture() -> (PathBuf, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("omni-catalog-titles-{}", uuid::Uuid::new_v4()));
        let sessions = root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        (root, sessions)
    }

    fn write_catalog_session(sessions: &Path, id: &str, events: &[Value]) -> PathBuf {
        let path = sessions.join(format!("rollout-2026-10-23T18-50-13-{id}.jsonl"));
        let mut lines =
            vec![serde_json::json!({
            "type": "session_meta",
            "payload": {"id": id, "cwd": "/tmp/project", "timestamp": "2026-10-23T10:50:13Z"}
        }).to_string()];
        lines.extend(events.iter().map(Value::to_string));
        fs::write(&path, lines.join("\n") + "\n").unwrap();
        path
    }

    #[test]
    fn catalog_recovers_unindexed_database_titles_and_preserves_explicit_names() {
        let (root, sessions) = catalog_fixture();
        write_catalog_session(&sessions, "unindexed", &[]);
        let connection = rusqlite::Connection::open(root.join("state_5.sqlite")).unwrap();
        connection.execute_batch(
            "CREATE TABLE threads (id TEXT, title TEXT, name TEXT, first_user_message TEXT);
             INSERT INTO threads VALUES ('unindexed', '支持暗色模式，统一主题色', NULL, 'first user input');"
        ).unwrap();
        let initial = load_session_catalog_summary_for_root(&sessions);
        assert_eq!(
            initial.sessions["unindexed"].title,
            "支持暗色模式，统一主题色"
        );

        connection
            .execute("UPDATE threads SET name = 'Dark mode'", [])
            .unwrap();
        let renamed = load_session_catalog_summary_for_root(&sessions);
        assert_eq!(renamed.sessions["unindexed"].title, "Dark mode");
        assert_ne!(initial.fingerprint, renamed.fingerprint);

        fs::write(
            root.join("session_index.jsonl"),
            r#"{"id":"unindexed","thread_name":"Latest explicit rename"}"#,
        )
        .unwrap();
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["unindexed"].title,
            "Latest explicit rename"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn catalog_legacy_titles_skip_injected_context_and_accept_user_event_formats() {
        let (root, sessions) = catalog_fixture();
        write_catalog_session(
            &sessions,
            "response",
            &[
                serde_json::json!({"type": "response_item", "payload": {"role": "user", "content": [
                    {"type": "input_text", "text": "# AGENTS.md instructions for /tmp/project\ninternal rules"}
                ]}}),
                serde_json::json!({"type": "response_item", "payload": {"role": "user", "content": [
                {"type": "input_text", "text": "internal continuation"}
            ], "internal_chat_message_metadata_passthrough": {"content_item_kinds": ["system.internal_context"]}}}),
                serde_json::json!({"type": "response_item", "metadata": {"compaction_output": true},
                "payload": {"role": "user", "content": [{"type": "input_text", "text": "internal summary"}]}}),
                serde_json::json!({"type": "response_item", "payload": {"role": "user", "content": [
                    {"type": "input_text", "text": "Fix\n the theme"}
                ]}}),
            ],
        );
        write_catalog_session(
            &sessions,
            "completed",
            &[
                serde_json::json!({"type": "event_msg", "payload": {"type": "item_completed", "item": {
                    "type": "UserMessage", "content": [{"type": "text", "text": "修复界面颜色"}]
                }}}),
            ],
        );
        write_catalog_session(
            &sessions,
            "legacy",
            &[
                serde_json::json!({"type": "event_msg", "payload": {"type": "user_message", "message": "继续"}}),
                serde_json::json!({"type": "event_msg", "payload": {"type": "user_message", "message": "Support dark mode"}}),
            ],
        );
        let archive = load_session_catalog_summary_for_root(&sessions);
        assert_eq!(archive.sessions["response"].title, "Fix the theme");
        assert_eq!(archive.sessions["completed"].title, "修复界面颜色");
        assert_eq!(archive.sessions["legacy"].title, "Support dark mode");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn catalog_title_fallback_refreshes_empty_sessions_and_bounds_transcript_reads() {
        let (root, sessions) = catalog_fixture();
        let path = write_catalog_session(&sessions, "new", &[]);
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["new"].title,
            "Codex session"
        );
        use std::io::Write;
        writeln!(fs::OpenOptions::new().append(true).open(&path).unwrap(), "{}",
            serde_json::json!({"type": "event_msg", "payload": {"type": "user_message", "message": "New request"}})
        ).unwrap();
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["new"].title,
            "New request"
        );
        write_catalog_session(
            &sessions,
            "large",
            &[
                serde_json::json!({"type": "response_item", "payload": {"role": "assistant",
                "content": [{"type": "output_text", "text": "x".repeat(1024 * 1024)}]}}),
                serde_json::json!({"type": "event_msg", "payload": {"type": "user_message", "message": "Beyond read limit"}}),
            ],
        );
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["large"].title,
            "Codex session"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn catalog_revision_tracks_wal_titles_and_ignores_unrelated_database_updates() {
        let (root, sessions) = catalog_fixture();
        write_catalog_session(&sessions, "unindexed", &[]);
        let path = root.join("state_5.sqlite");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE threads (id TEXT, title TEXT, tokens_used INTEGER);
             INSERT INTO threads VALUES ('unindexed', 'first title', 0);",
            )
            .unwrap();
        let initial = session_catalog_revision_for_root(&sessions).unwrap();
        let database_fingerprint = file_fingerprint(&path);
        connection
            .execute("UPDATE threads SET tokens_used = 1", [])
            .unwrap();
        assert_eq!(session_catalog_revision_for_root(&sessions), Some(initial));
        connection
            .execute("UPDATE threads SET title = 'other title'", [])
            .unwrap();
        assert_eq!(file_fingerprint(&path), database_fingerprint);
        assert_ne!(session_catalog_revision_for_root(&sessions), Some(initial));
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["unindexed"].title,
            "other title"
        );
        drop(connection);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn catalog_database_titles_support_older_schemas_and_first_user_message() {
        let (root, sessions) = catalog_fixture();
        write_catalog_session(&sessions, "old", &[]);
        let connection = rusqlite::Connection::open(root.join("state_4.sqlite")).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (id TEXT, title TEXT);
             INSERT INTO threads VALUES ('old', 'Older database title');",
            )
            .unwrap();
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["old"].title,
            "Older database title"
        );

        let newer = rusqlite::Connection::open(root.join("state_5.sqlite")).unwrap();
        newer
            .execute_batch(
                "CREATE TABLE threads (id TEXT, title TEXT, name TEXT, first_user_message TEXT);
             INSERT INTO threads VALUES ('old', '', NULL, 'First user message');",
            )
            .unwrap();
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["old"].title,
            "First user message"
        );
        drop(newer);
        fs::write(root.join("state_5.sqlite"), "unreadable database").unwrap();
        // A transiently unreadable database retains the last successful titles.
        assert_eq!(
            load_session_catalog_summary_for_root(&sessions).sessions["old"].title,
            "First user message"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn catalog_revision_ignores_appends_and_detects_new_sessions_and_titles() {
        let root =
            std::env::temp_dir().join(format!("omni-catalog-revision-{}", uuid::Uuid::new_v4()));
        let sessions = root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let first = sessions.join("first.jsonl");
        fs::write(&first, "header").unwrap();
        let initial = session_catalog_revision_for_root(&sessions).unwrap();
        fs::write(&first, "header with appended transcript events").unwrap();
        assert_eq!(session_catalog_revision_for_root(&sessions), Some(initial));
        let second = sessions.join("second.jsonl");
        fs::write(&second, "another session header").unwrap();
        assert_ne!(session_catalog_revision_for_root(&sessions), Some(initial));
        fs::remove_file(second).unwrap();
        assert_eq!(session_catalog_revision_for_root(&sessions), Some(initial));
        fs::write(root.join("session_index.jsonl"), "updated title").unwrap();
        assert_ne!(session_catalog_revision_for_root(&sessions), Some(initial));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_header_cache_refreshes_rewritten_metadata_and_deleted_files() {
        let file = temp_jsonl_file(
            "codex-header-cache",
            &[
                r#"{"type":"session_meta","payload":{"id":"first","cwd":"/tmp/first","model_provider":"old"}}"#,
            ],
        );
        let first = cached_session_header(&file).unwrap();
        assert_eq!(first.id, "first");
        assert_eq!(first.cwd, "/tmp/first");
        assert_eq!(
            cached_session_header(&file).unwrap().provider.as_deref(),
            Some("old")
        );

        fs::write(&file, r#"{"type":"session_meta","payload":{"id":"replacement","cwd":"/tmp/replacement","model_provider":"new"}}"#).unwrap();
        let updated = cached_session_header(&file).unwrap();
        assert_eq!(updated.id, "replacement");
        assert_eq!(updated.cwd, "/tmp/replacement");
        assert_eq!(updated.provider.as_deref(), Some("new"));
        fs::remove_file(&file).unwrap();
        assert!(cached_session_header(&file).is_none());
    }

    #[test]
    fn load_session_messages_parses_codex_jsonl_archive() {
        let file = temp_jsonl_file(
            "codex-session",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-1","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"hello codex"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello "},{"type":"output_text","text":"user"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete"}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].session_id, "codex-1");
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "hello codex");
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "hello user");
    }

    #[test]
    fn load_session_messages_reads_completed_items_without_mirrors_or_compaction() {
        let file = temp_jsonl_file(
            "codex-completed-items",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"completed","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"task_started"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"[Image #1] fix this"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","content":[{"type":"local_image","path":"/tmp/test.png"},{"type":"text","text":"[Image #1] fix this","text_elements":[{"placeholder":"[Image #1]"}]}]}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","content":[{"type":"Text","text":"checking"}],"phase":"commentary"}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"checking"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"internal summary"}]},"metadata":{"compaction_output":true}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","content":[{"type":"Text","text":"latest reply"}],"phase":"final_answer"}}}"#,
            ],
        );
        let messages = load_session_messages(&file).unwrap().messages;
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].content, "![Image #1](</tmp/test.png>) fix this");
        assert_eq!(messages[1].content, "checking");
        assert_eq!(messages[2].content, "latest reply");
        assert_eq!(
            parse_session_summary_file(&file)
                .unwrap()
                .session
                .last_message_preview
                .as_deref(),
            Some("latest reply")
        );
    }

    #[test]
    fn load_session_messages_keeps_response_boundaries_and_switches_formats_between_turns() {
        let file = temp_jsonl_file(
            "codex-response-boundaries",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"boundaries","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"old format"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"new commentary"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"internal summary"}]},"metadata":{"compaction_output":true}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"new final"}]}}"#,
            ],
        );
        let messages = load_session_messages(&file).unwrap().messages;
        assert_eq!(
            messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>(),
            ["old format", "new commentary", "new final"]
        );
        assert_eq!(
            messages.last().unwrap().created_at,
            super::parse_timestamp("2026-05-01T00:00:05Z").unwrap()
        );
    }

    #[test]
    fn load_session_messages_reads_response_only_user_turns_and_skips_injected_context() {
        let file = temp_jsonl_file(
            "codex-response-users",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-response-users","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>\n  <cwd>/tmp/project</cwd>\n</environment_context>"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["environments.environment_context"]}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"启动 Flutter 应用并检查首页"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["user.text"]}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"好的"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"再确认登录按钮可以点击"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["user.text"]}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<send_user_message_question_reply>\n[{\"answer\":\"先检查首页\"}]\n</send_user_message_question_reply>"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["user.text"]}}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("response-only archive should parse")
            .messages;

        let users = messages
            .iter()
            .filter(|message| message.role == MessageRole::User)
            .collect::<Vec<_>>();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].content, "启动 Flutter 应用并检查首页");
        assert_eq!(users[1].content, "再确认登录按钮可以点击");
        assert!(users[2].content.contains("先检查首页"));
        assert!(
            messages
                .iter()
                .all(|message| !message.content.contains("environment_context"))
        );
        let context = crate::acceptance_context::build_context(messages, None);
        assert_eq!(context.user_message_count, 3);
        assert_eq!(context.source_message_ids.len(), 3);
        assert!(
            context
                .chunks
                .join("\n")
                .contains("启动 Flutter 应用并检查首页")
        );
        let summary = parse_session_summary_file(&file).expect("summary should parse");
        assert_eq!(summary.session.title, "启动 Flutter 应用并检查首页");
    }

    #[test]
    fn load_session_messages_merges_user_mirrors_in_both_orders_and_keeps_repeated_turns() {
        let event = serde_json::json!({
            "type": "event_msg", "payload": {"type": "user_message", "message": "try again"}
        });
        let response = serde_json::json!({
            "type": "response_item", "payload": {
                "type": "message", "role": "user",
                "content": [{"type": "input_text", "text": "try again"}]
            }
        });
        for (first, second) in [(&event, &response), (&response, &event)] {
            let records = [
                serde_json::json!({"type": "session_meta", "payload": {"id": "mirrors"}}),
                first.clone(),
                second.clone(),
                first.clone(),
                second.clone(),
            ];
            let lines = records.iter().map(Value::to_string).collect::<Vec<_>>();
            let file = temp_jsonl_file(
                "codex-user-mirrors",
                &lines.iter().map(String::as_str).collect::<Vec<_>>(),
            );
            let messages = load_session_messages(&file).unwrap().messages;
            assert_eq!(messages.len(), 2);
            assert!(
                messages
                    .iter()
                    .all(|message| message.content == "try again")
            );
            assert_ne!(messages[0].id, messages[1].id);
        }
    }

    #[test]
    fn load_session_messages_keeps_identical_response_only_requests() {
        let file = temp_jsonl_file(
            "codex-repeated-response-users",
            &[
                r#"{"type":"session_meta","payload":{"id":"repeated"}}"#,
                r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"继续"}]}}"#,
                r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"继续"}]}}"#,
            ],
        );
        let messages = load_session_messages(&file).unwrap().messages;
        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|message| message.content == "继续"));
    }

    #[test]
    fn load_session_messages_excludes_internal_response_users_from_acceptance() {
        let file = temp_jsonl_file(
            "codex-injected-users",
            &[
                r#"{"type":"session_meta","payload":{"id":"injected"}}"#,
                r##"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md instructions for /tmp/project\nproject guidelines"}]}}"##,
                r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>environment</environment_context>"}]}}"#,
                r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Continue internal maintenance"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["system.internal_context"]}}}"#,
            ],
        );
        let messages = load_session_messages(&file).unwrap().messages;
        assert!(messages.is_empty());
        let context = crate::acceptance_context::build_context(messages, None);
        assert!(context.source_message_ids.is_empty());
    }

    #[test]
    fn load_session_messages_renders_goal_objective_without_internal_continuation_prompt() {
        let file = temp_jsonl_file(
            "codex-goal-session",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-goal","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"fix attachment rendering","status":"active"}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">secret continuation instructions</codex_internal_context>"}]}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("goal archive should parse")
            .messages;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[0].content, "/goal fix attachment rendering");
        assert!(!messages[0].content.contains("codex_internal_context"));
    }

    #[test]
    fn load_session_messages_deduplicates_goal_status_updates() {
        let file = temp_jsonl_file(
            "codex-goal-updates",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-goal","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"finish task","status":"active"}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"finish task","status":"complete"}}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("goal archive should parse")
            .messages;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "/goal finish task");
    }

    #[test]
    fn load_session_messages_keeps_restarted_goal_with_same_objective() {
        let file = temp_jsonl_file(
            "codex-restarted-goal",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-goal","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"finish task","status":"active"}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"finish task","status":"complete"}}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"finish task","status":"active"}}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("restarted goal archive should parse")
            .messages;

        assert_eq!(messages.len(), 2);
        assert!(
            messages
                .iter()
                .all(|message| message.content == "/goal finish task")
        );
    }

    #[test]
    fn load_session_messages_prefers_agent_messages_over_response_item_mirrors() {
        let file = temp_jsonl_file(
            "codex-agent-message",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-agent","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"hello codex"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"commentary message","phase":"commentary"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"commentary message"}],"phase":"commentary"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"event_msg","payload":{"type":"agent_message","message":"final answer","phase":"final_answer"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"final answer"}],"phase":"final_answer"}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[1].role, MessageRole::Assistant);
        assert_eq!(messages[1].content, "commentary message");
        assert_eq!(messages[2].role, MessageRole::Assistant);
        assert_eq!(messages[2].content, "final answer");
    }

    #[test]
    fn load_session_messages_parses_codex_tool_items_as_system_messages() {
        let file = temp_jsonl_file(
            "codex-session-tools",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-tools","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"run pwd"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}","call_id":"call-1"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"Chunk ID: abc\nOutput:\n/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:04Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:05Z","type":"event_msg","payload":{"type":"task_complete"}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_eq!(messages[1].role, MessageRole::System);
        assert!(messages[1].content.starts_with("[tool:exec_command]"));
        assert!(messages[1].content.contains(r#"{"cmd":"pwd"}"#));
        assert_eq!(messages[2].role, MessageRole::System);
        assert!(messages[2].content.starts_with("[tool:output]"));
        assert!(messages[2].content.contains("/tmp/project"));
        assert_eq!(messages[3].role, MessageRole::Assistant);
        assert_eq!(messages[3].content, "done");
    }

    #[test]
    fn load_session_messages_renders_user_images_as_markdown() {
        let file = temp_jsonl_file(
            "codex-session-images",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-images","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"看看这个：[Image #1]","local_images":["/tmp/image 1.png"],"text_elements":[{"placeholder":"[Image #1]"}]}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].content,
            "看看这个：![Image #1](</tmp/image 1.png>)"
        );
    }

    #[test]
    fn load_session_messages_renders_data_images_as_markdown() {
        let file = temp_jsonl_file(
            "codex-session-data-images",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-data-images","cwd":"/tmp/project"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<image name=[Image #1]>"},{"type":"input_image","image_url":"data:image/png;base64,abc123=="},{"type":"input_text","text":"</image>"},{"type":"input_text","text":"看看这个：[Image #1]"}]}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"user_message","message":"看看这个：[Image #1]","local_images":["/tmp/image 1.png"],"text_elements":[{"placeholder":"[Image #1]"}]}}"#,
            ],
        );

        let messages = load_session_messages(&file)
            .expect("codex archive should parse")
            .messages;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].content,
            "看看这个：![Image #1](<data:image/png;base64,abc123==>)"
        );
    }

    #[test]
    fn parse_session_summary_file_builds_codex_project_and_session() {
        let file = temp_jsonl_file(
            "codex-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-2","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"implement feature"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.id, "codex-2");
        assert_eq!(record.session.agent, AgentKind::Codex);
        assert_eq!(record.session.title, "implement feature");
        assert_eq!(record.session.last_message_preview.as_deref(), Some("done"));
        assert_eq!(record.project.name, "example-app");
        assert_eq!(record.project.session_count, 1);
    }

    #[test]
    fn parse_session_summary_file_prefers_agent_message_preview() {
        let file = temp_jsonl_file(
            "codex-summary-agent-message",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-3","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"implement feature"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"visible agent reply","phase":"final_answer"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"provider mirror"}],"phase":"final_answer"}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(
            record.session.last_message_preview.as_deref(),
            Some("visible agent reply")
        );
        assert_eq!(
            record.project.last_session_preview.as_deref(),
            Some("visible agent reply")
        );
    }

    #[test]
    fn parse_session_summary_file_uses_goal_as_title_and_preview() {
        let file = temp_jsonl_file(
            "codex-goal-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-goal","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"thread_goal_updated","goal":{"objective":"fix attachment rendering","status":"active"}}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");

        assert_eq!(record.session.title, "/goal fix attachment rendering");
        assert_eq!(
            record.session.last_message_preview.as_deref(),
            Some("/goal fix attachment rendering")
        );
    }

    #[test]
    fn parse_session_summary_file_marks_unfinished_codex_task_as_running() {
        let file = temp_jsonl_file(
            "codex-running-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-running","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"keep going"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started"}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");
        assert!(matches!(record.session.status, SessionStatus::Running));
    }

    #[test]
    fn parse_session_summary_file_marks_aborted_codex_task_as_interrupted() {
        let file = temp_jsonl_file(
            "codex-interrupted-summary",
            &[
                r#"{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"codex-interrupted","cwd":"/tmp/example-app"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"keep going"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started"}}"#,
                r#"{"timestamp":"2026-05-01T00:00:03Z","type":"event_msg","payload":{"type":"turn_aborted"}}"#,
            ],
        );

        let record = parse_session_summary_file(&file).expect("summary should parse");
        assert!(matches!(record.session.status, SessionStatus::Interrupted));
    }

    fn temp_jsonl_file(name: &str, lines: &[&str]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "omni-code-bridge-test-{}-{}",
            name,
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join(format!("{name}.jsonl"));
        std::fs::write(&path, lines.join("\n")).expect("write jsonl");
        path
    }
}
