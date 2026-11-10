use crate::{
    util::{secret, validate_relay_url, write_private},
    Config,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

/// Sensitive client state. Serialize only to a private credential file.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub relay: String,
    pub token: String,
    pub client_key: String,
    pub client_id: String,
    pub expires_at: i64,
}
impl Session {
    pub fn load(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                "session file must be private"
            );
        }
        serde_json::from_str(&std::fs::read_to_string(path)?)
            .map_err(|_| anyhow::anyhow!("invalid session file"))
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        if !path.exists() {
            return write_private(path, &content);
        }
        let _ = Self::load(path)?;
        let temporary = path.with_extension(format!("{}.tmp", secret()));
        write_private(&temporary, &content)?;
        if let Err(error) = std::fs::rename(&temporary, path) {
            let _ = std::fs::remove_file(temporary);
            return Err(error.into());
        }
        Ok(())
    }
}

/// Contains a private polling capability; show only `user_code` and `verification_uri`.
pub struct LoginFlow {
    pub user_code: String,
    pub verification_uri: String,
    pub expires_at: i64,
    pub interval: u64,
    device_code: String,
    client_key: String,
}
pub enum LoginStatus {
    Pending,
    SignedIn(Session),
}

#[derive(Clone)]
pub struct RelayClient {
    relay: url::Url,
    http: reqwest::Client,
}
impl RelayClient {
    pub fn new(relay: &str) -> Result<Self> {
        Ok(Self {
            relay: validate_relay_url(relay)?,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()?,
        })
    }
    pub async fn begin_login(&self, name: &str, previous: Option<&Session>) -> Result<LoginFlow> {
        ensure!(
            !name.trim().is_empty() && name.len() <= 100,
            "invalid client name"
        );
        if let Some(previous) = previous {
            ensure!(
                validate_relay_url(&previous.relay)? == self.relay,
                "session belongs to another relay"
            );
        }
        let client_key = previous
            .map(|s| s.client_key.clone())
            .unwrap_or_else(secret);
        let response = self
            .http
            .post(self.relay.join("/v1/device-login/start")?)
            .json(&serde_json::json!({"name": name, "client_key": client_key}))
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "device login failed (HTTP {})",
            response.status()
        );
        #[derive(Deserialize)]
        struct Challenge {
            device_code: String,
            user_code: String,
            verification_uri: String,
            expires_at: i64,
            interval: u64,
        }
        let challenge: Challenge = response.json().await.context("invalid login response")?;
        ensure!(
            validate_relay_url(&challenge.verification_uri)? == self.relay,
            "unexpected login origin"
        );
        Ok(LoginFlow {
            device_code: challenge.device_code,
            user_code: challenge.user_code,
            verification_uri: challenge.verification_uri,
            expires_at: challenge.expires_at,
            interval: challenge.interval.clamp(5, 30),
            client_key,
        })
    }
    pub async fn poll_login(&self, flow: &LoginFlow) -> Result<LoginStatus> {
        ensure!(crate::util::now() < flow.expires_at, "device login expired");
        let response = self
            .http
            .post(self.relay.join("/v1/device-login/poll")?)
            .json(&serde_json::json!({"device_code": flow.device_code}))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::ACCEPTED {
            return Ok(LoginStatus::Pending);
        }
        ensure!(
            response.status().is_success(),
            "device login failed (HTTP {})",
            response.status()
        );
        #[derive(Deserialize)]
        struct Issued {
            token: String,
            client_id: String,
            expires_at: i64,
        }
        let issued: Issued = response.json().await.context("invalid session response")?;
        Ok(LoginStatus::SignedIn(Session {
            relay: self.relay.origin().ascii_serialization(),
            token: issued.token,
            client_key: flow.client_key.clone(),
            client_id: issued.client_id,
            expires_at: issued.expires_at,
        }))
    }
    pub async fn enroll(
        &self,
        session: &Session,
        name: &str,
        policy: &str,
        mut config: Config,
    ) -> Result<Config> {
        ensure!(
            validate_relay_url(&session.relay)? == self.relay
                && validate_relay_url(&config.relay)? == self.relay,
            "relay mismatch"
        );
        ensure!(
            !name.trim().is_empty() && name.len() <= 100 && matches!(policy, "auto" | "approval"),
            "invalid service registration"
        );
        config.validate()?;
        let response = self
            .http
            .post(self.relay.join("/v1/services")?)
            .bearer_auth(&session.token)
            .json(&serde_json::json!({"name": name, "kind": config.kind, "policy": policy}))
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "enrollment failed (HTTP {})",
            response.status()
        );
        #[derive(Deserialize)]
        struct Enrolled {
            id: String,
            connector_token: String,
        }
        let result: Enrolled = response
            .json()
            .await
            .context("invalid enrollment response")?;
        config.service_id = result.id;
        config.connector_token = result.connector_token;
        Ok(config)
    }
    pub async fn provider_clients(&self, config: &Config) -> Result<serde_json::Value> {
        ensure!(
            validate_relay_url(&config.relay)? == self.relay,
            "relay mismatch"
        );
        let response = self
            .http
            .get(self.relay.join("/v1/provider/clients")?)
            .bearer_auth(&config.connector_token)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "provider request failed (HTTP {})",
            response.status()
        );
        Ok(response.json().await?)
    }
    pub async fn services(&self, session: &Session) -> Result<serde_json::Value> {
        ensure!(
            validate_relay_url(&session.relay)? == self.relay,
            "relay mismatch"
        );
        let response = self
            .http
            .get(self.relay.join("/v1/services")?)
            .bearer_auth(&session.token)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "service lookup failed (HTTP {})",
            response.status()
        );
        Ok(response.json().await?)
    }
    pub async fn provider_manage(
        &self,
        config: &Config,
        action: &str,
        client: Option<&str>,
        policy: Option<&str>,
    ) -> Result<()> {
        ensure!(
            validate_relay_url(&config.relay)? == self.relay,
            "relay mismatch"
        );
        ensure!(
            matches!(
                action,
                "kick" | "ban" | "unban" | "approve" | "revoke" | "policy"
            ),
            "invalid provider action"
        );
        let response = self
            .http
            .post(self.relay.join("/v1/provider/manage")?)
            .bearer_auth(&config.connector_token)
            .json(&serde_json::json!({"action": action, "client": client, "policy": policy}))
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "provider request failed (HTTP {})",
            response.status()
        );
        Ok(())
    }
}
