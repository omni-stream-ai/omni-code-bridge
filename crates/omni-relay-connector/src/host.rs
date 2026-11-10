//! Shared lifecycle and private configuration for embedded Core/Bridge hosts.
//! The embedding server must authorize control requests and exclude the tunnel
//! principal before calling request. No control response contains credentials.
use crate::{util, Config, ConnectorHandle, LoginFlow, LoginStatus, RelayClient, Session};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::Mutex;

pub const UPSTREAM_CLIENT_ID: &str = "omni-relay-connector";
const DEFAULT_RELAY: &str = "https://relay.omnistreamai.com";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    relay: String,
    name: String,
    enabled: bool,
    session: Option<Session>,
    service: Option<Enrollment>,
    #[serde(default)]
    verify_service: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Enrollment {
    id: String,
    token: String,
}
struct State {
    saved: Saved,
    login: Option<LoginFlow>,
    last_poll: i64,
    connector: Option<ConnectorHandle>,
}

pub struct Host {
    path: PathBuf,
    kind: String,
    target: String,
    // A fresh, process-scoped credential. Only its digest is used for validation.
    upstream: String,
    upstream_hash: String,
    enabled: AtomicBool,
    state: Mutex<State>,
}

impl Host {
    pub fn open(directory: &Path, kind: &str, target: &str, name: &str) -> Result<Self> {
        ensure!(
            matches!(kind, "omnistream" | "omni-code"),
            "invalid host kind"
        );
        let path = directory.join("relay.json");
        let saved: Saved = if path.exists() {
            check_private(&path)?;
            serde_json::from_str(&std::fs::read_to_string(&path)?)
                .map_err(|_| anyhow::anyhow!("invalid private Relay configuration"))?
        } else {
            Saved {
                relay: DEFAULT_RELAY.into(),
                name: name.into(),
                enabled: false,
                session: None,
                service: None,
                verify_service: false,
            }
        };
        util::validate_relay_url(&saved.relay)?;
        ensure!(
            !saved.name.trim().is_empty() && saved.name.len() <= 100,
            "invalid service name"
        );
        if let Some(session) = &saved.session {
            ensure!(
                util::validate_relay_url(&session.relay)?
                    == util::validate_relay_url(&saved.relay)?,
                "session relay mismatch"
            );
        }
        ensure!(
            !saved.enabled || saved.service.is_some(),
            "Relay service enrollment is missing"
        );
        ensure!(
            !saved.enabled || !saved.verify_service,
            "verify the service account before enabling Relay"
        );
        let upstream = util::secret();
        let host = Self {
            path,
            kind: kind.into(),
            target: target.into(),
            upstream_hash: util::hash(&upstream),
            upstream,
            enabled: AtomicBool::new(saved.enabled),
            state: Mutex::new(State {
                saved,
                login: None,
                last_poll: 0,
                connector: None,
            }),
        };
        host.config(&host.state.try_lock().expect("new host").saved)
            .validate()?;
        Ok(host)
    }
    fn config(&self, saved: &Saved) -> Config {
        Config {
            relay: saved.relay.clone(),
            service_id: saved
                .service
                .as_ref()
                .map(|s| s.id.clone())
                .unwrap_or_default(),
            connector_token: saved
                .service
                .as_ref()
                .map(|s| s.token.clone())
                .unwrap_or_default(),
            kind: self.kind.clone(),
            target: self.target.clone(),
            upstream_token: Some(self.upstream.clone()),
            upstream_client_id: (self.kind == "omni-code").then(|| UPSTREAM_CLIENT_ID.into()),
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
    pub fn authenticates(&self, token: &str) -> bool {
        self.enabled() && self.recognizes_upstream(token)
    }
    /// Recognize this process's upstream identity even after disabling it, so
    /// a loopback server never falls back to implicit local trust for it.
    pub fn recognizes_upstream(&self, token: &str) -> bool {
        let digest = util::hash(token);
        self.upstream_hash
            .bytes()
            .zip(digest.bytes())
            .fold(0, |d, (a, b)| d | (a ^ b))
            == 0
    }
    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.saved.enabled && state.connector.is_none() {
            state.connector = Some(crate::start(self.config(&state.saved))?);
        }
        Ok(())
    }
    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        if let Some(connector) = state.connector.take() {
            connector.shutdown().await;
        }
    }
    fn persist(&self, saved: &Saved) -> Result<()> {
        let parent = self.path.parent().context("invalid Relay directory")?;
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
        if self.path.exists() {
            check_private(&self.path)?;
        }
        let temp = parent.join(format!(".relay-{}.tmp", util::secret()));
        util::write_private(&temp, &serde_json::to_string_pretty(saved)?)?;
        if let Err(error) = std::fs::rename(&temp, &self.path) {
            let _ = std::fs::remove_file(temp);
            return Err(error.into());
        }
        Ok(())
    }
    fn status(&self, state: &State) -> Value {
        json!({"relay":state.saved.relay,"name":state.saved.name,"enabled":state.saved.enabled,"service_id":state.saved.service.as_ref().map(|s| &s.id),"connection":state.connector.as_ref().map(ConnectorHandle::status),"login":state.login.as_ref().map(|flow| json!({"user_code":flow.user_code,"verification_uri":flow.verification_uri,"expires_at":flow.expires_at,"interval":flow.interval})),"signed_in":state.saved.session.as_ref().is_some_and(|s| s.expires_at > util::now())})
    }
    /// Authorized host control API. The caller must reject tunneled requests.
    pub async fn request(&self, method: &str, path: &str, body: &[u8]) -> Result<Value> {
        ensure!(body.len() <= 4096, "Relay request too large");
        let mut state = self.state.lock().await;
        match (method, path) {
            ("GET", "/relay/status") => {}
            ("POST", "/relay/login/start") => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    relay: String,
                    name: String,
                }
                let input: Input = serde_json::from_slice(body)
                    .map_err(|_| anyhow::anyhow!("invalid login request"))?;
                let relay = util::validate_relay_url(&input.relay)?
                    .origin()
                    .ascii_serialization();
                ensure!(
                    !input.name.trim().is_empty() && input.name.len() <= 100,
                    "invalid service name"
                );
                ensure!(
                    !state.saved.enabled,
                    "disable Relay before signing in again"
                );
                let client = RelayClient::new(&relay)?;
                let previous = state.saved.session.as_ref().filter(|s| s.relay == relay);
                let flow = client.begin_login(&input.name, previous).await?;
                if relay != state.saved.relay {
                    state.saved.session = None;
                    state.saved.service = None;
                }
                state.saved.relay = relay;
                state.saved.name = input.name.trim().into();
                self.persist(&state.saved)?;
                state.login = Some(flow);
                state.last_poll = 0;
            }
            ("POST", "/relay/login/poll") => {
                let flow = state.login.as_ref().context("start Relay login first")?;
                ensure!(util::now() < flow.expires_at, "Relay login expired");
                if util::now() - state.last_poll >= flow.interval as i64 {
                    let client = RelayClient::new(&state.saved.relay)?;
                    let result = client.poll_login(flow).await?;
                    state.last_poll = util::now();
                    if let LoginStatus::SignedIn(session) = result {
                        state.saved.session = Some(session);
                        state.saved.verify_service = true;
                        state.login = None;
                        self.persist(&state.saved)?;
                    }
                }
            }
            ("POST", "/relay/login/cancel") => {
                state.login = None;
            }
            ("POST", "/relay/enable") => {
                if state.saved.verify_service {
                    let session = state
                        .saved
                        .session
                        .as_ref()
                        .context("Relay login required")?;
                    let services = RelayClient::new(&state.saved.relay)?
                        .services(session)
                        .await?;
                    let entries = services["services"]
                        .as_array()
                        .context("invalid service response")?;
                    if state.saved.service.as_ref().is_some_and(|service| {
                        !entries
                            .iter()
                            .any(|entry| entry["id"].as_str() == Some(service.id.as_str()))
                    }) {
                        state.saved.service = None;
                    }
                    state.saved.verify_service = false;
                    self.persist(&state.saved)?;
                }
                if state.saved.service.is_none() {
                    let session = state
                        .saved
                        .session
                        .as_ref()
                        .filter(|s| s.expires_at > util::now())
                        .context("Relay login required")?;
                    let client = RelayClient::new(&state.saved.relay)?;
                    let config = client
                        .enroll(
                            session,
                            &state.saved.name,
                            "auto",
                            self.config(&state.saved),
                        )
                        .await?;
                    state.saved.service = Some(Enrollment {
                        id: config.service_id,
                        token: config.connector_token,
                    });
                    self.persist(&state.saved)?;
                }
                state.saved.enabled = true;
                if let Err(error) = self.persist(&state.saved) {
                    state.saved.enabled = false;
                    return Err(error);
                }
                self.enabled.store(true, Ordering::Release);
                if state.connector.is_none() {
                    state.connector = Some(crate::start(self.config(&state.saved))?);
                }
            }
            ("POST", "/relay/disable") => {
                let previous = state.saved.enabled;
                state.saved.enabled = false;
                if let Err(error) = self.persist(&state.saved) {
                    state.saved.enabled = previous;
                    return Err(error);
                }
                self.enabled.store(false, Ordering::Release);
                state.login = None;
                if let Some(connector) = state.connector.take() {
                    connector.shutdown().await;
                }
            }
            ("GET", "/relay/clients") => {
                ensure!(state.saved.service.is_some(), "Relay is not enrolled");
                return RelayClient::new(&state.saved.relay)?
                    .provider_clients(&self.config(&state.saved))
                    .await;
            }
            ("POST", "/relay/manage") => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Action {
                    action: String,
                    client: Option<String>,
                    policy: Option<String>,
                }
                let input: Action = serde_json::from_slice(body)
                    .map_err(|_| anyhow::anyhow!("invalid provider action"))?;
                ensure!(state.saved.service.is_some(), "Relay is not enrolled");
                RelayClient::new(&state.saved.relay)?
                    .provider_manage(
                        &self.config(&state.saved),
                        &input.action,
                        input.client.as_deref(),
                        input.policy.as_deref(),
                    )
                    .await?;
            }
            _ => bail!("unknown Relay operation"),
        }
        Ok(self.status(&state))
    }
}

fn check_private(path: &Path) -> Result<()> {
    ensure!(
        path.symlink_metadata()?.file_type().is_file(),
        "Relay configuration must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            path.metadata()?.permissions().mode() & 0o077 == 0,
            "Relay credential file must have mode 600"
        );
    }
    Ok(())
}

/// Configure a host before starting its server. Arguments never accept secrets.
pub async fn host_cli(host: &Host, arguments: &[String]) -> Result<()> {
    let operation = arguments.first().map(String::as_str).unwrap_or("status");
    match operation {
        "login" => {
            ensure!(
                arguments.len() <= 3,
                "usage: relay login [relay-origin] [service-name]"
            );
            let value = host.request("GET", "/relay/status", &[]).await?;
            let relay = arguments
                .get(1)
                .map(String::as_str)
                .unwrap_or(DEFAULT_RELAY);
            let name = arguments
                .get(2)
                .map(String::as_str)
                .unwrap_or_else(|| value["name"].as_str().unwrap_or("workstation"));
            let status = host
                .request(
                    "POST",
                    "/relay/login/start",
                    &serde_json::to_vec(&json!({"relay":relay,"name":name}))?,
                )
                .await?;
            println!(
                "Open {} and approve device code {}",
                status["login"]["verification_uri"]
                    .as_str()
                    .unwrap_or_default(),
                status["login"]["user_code"].as_str().unwrap_or_default()
            );
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let status = host.request("POST", "/relay/login/poll", b"{}").await?;
                if status["login"].is_null() {
                    break;
                }
            }
            host.request("POST", "/relay/enable", b"{}").await?;
            host.shutdown().await;
            println!("Relay configured. Start or restart the host server to connect.");
        }
        "status" => {
            ensure!(arguments.len() <= 1, "usage: relay status");
            println!("{}", host.request("GET", "/relay/status", &[]).await?);
        }
        "enable" | "disable" => {
            ensure!(arguments.len() == 1, "usage: relay enable|disable");
            host.request("POST", &format!("/relay/{operation}"), b"{}")
                .await?;
            host.shutdown().await;
            println!("Relay configuration saved. Restart a running host server to apply it.");
        }
        _ => bail!("usage: relay login [relay-origin] [service-name] | status | enable | disable"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn host_status_is_redacted_and_disabled_hosts_do_not_authenticate() {
        let dir = tempfile::tempdir().unwrap();
        let host = Host::open(
            &dir.path().join("relay"),
            "omni-code",
            "http://127.0.0.1:8787",
            "code",
        )
        .unwrap();
        assert!(!host.authenticates(&host.upstream));
        let status = host
            .request("GET", "/relay/status", &[])
            .await
            .unwrap()
            .to_string();
        assert!(!status.contains(&host.upstream));
        assert!(!status.contains("token"));
        assert!(host.request("POST", "/relay/enable", b"{}").await.is_err());
        host.request("POST", "/relay/disable", b"{}").await.unwrap();
        let restored = Host::open(
            &dir.path().join("relay"),
            "omni-code",
            "http://127.0.0.1:8787",
            "code",
        )
        .unwrap();
        assert!(!restored.enabled());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join("relay/relay.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
    #[tokio::test]
    async fn resume_and_disable_revoke_the_process_scoped_upstream_identity() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("relay");
        std::fs::create_dir(&root).unwrap();
        let credential = util::secret();
        util::write_private(
            &root.join("relay.json"),
            &serde_json::to_string(&Saved {
                relay: "http://127.0.0.1:1".into(),
                name: "code".into(),
                enabled: true,
                session: None,
                verify_service: false,
                service: Some(Enrollment {
                    id: "service".into(),
                    token: credential.clone(),
                }),
            })
            .unwrap(),
        )
        .unwrap();
        let host = Host::open(&root, "omni-code", "http://127.0.0.1:8787", "code").unwrap();
        assert!(host.authenticates(&host.upstream));
        host.resume().await.unwrap();
        let status = host
            .request("GET", "/relay/status", &[])
            .await
            .unwrap()
            .to_string();
        assert!(!status.contains(&credential));
        assert!(!status.contains(&host.upstream));
        host.request("POST", "/relay/disable", b"{}").await.unwrap();
        assert!(!host.authenticates(&host.upstream));
        assert!(host.recognizes_upstream(&host.upstream));
        assert!(!host.enabled());
    }
}
