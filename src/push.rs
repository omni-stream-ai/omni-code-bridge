use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::models::{ApprovalRequest, PushDeviceRegistration, SessionSummary};

#[derive(Clone)]
pub struct PushService {
    client: Client,
    fcm: Option<FcmConfig>,
    xiaomi: Option<XiaomiConfig>,
    fcm_token_cache: Arc<Mutex<Option<CachedAccessToken>>>,
}

impl PushService {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            fcm: FcmConfig::from_env(),
            xiaomi: XiaomiConfig::from_env(),
            fcm_token_cache: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn send_assistant_reply(
        &self,
        session: SessionSummary,
        body: String,
        devices: Vec<PushDeviceRegistration>,
    ) {
        let trimmed = body.trim().to_string();
        if trimmed.is_empty() {
            return;
        }

        for device in devices {
            let is_xiaomi = device
                .manufacturer
                .as_deref()
                .map(|item| item.to_ascii_lowercase().contains("xiaomi"))
                .unwrap_or(false);

            if is_xiaomi
                && device.mi_push_reg_id.is_some()
                && self.xiaomi.is_some()
                && self
                    .send_xiaomi_notification(&session, &trimmed, &device)
                    .await
                    .is_ok()
            {
                continue;
            }

            if let Err(error) = self
                .send_fcm_notification(&session, &session.title, &trimmed, &device)
                .await
            {
                eprintln!(
                    "FCM push failed for client {}: {error:?}",
                    device.client_id
                );
            }
        }
    }

    pub async fn send_approval_request(
        &self,
        session: SessionSummary,
        request: ApprovalRequest,
        devices: Vec<PushDeviceRegistration>,
    ) {
        let body = request
            .reason
            .as_deref()
            .or(request.command.as_deref())
            .unwrap_or("需要你审批后继续执行")
            .trim()
            .to_string();
        let body = if body.is_empty() {
            "需要你审批后继续执行".to_string()
        } else {
            body
        };

        for device in devices {
            if let Err(error) = self
                .send_fcm_notification(&session, "需要审批", &body, &device)
                .await
            {
                eprintln!(
                    "FCM approval push failed for client {}: {error:?}",
                    device.client_id
                );
            }
        }
    }

    async fn send_fcm_notification(
        &self,
        session: &SessionSummary,
        title: &str,
        body: &str,
        device: &PushDeviceRegistration,
    ) -> Result<()> {
        let token = device.fcm_token.as_deref().context("missing fcm token")?;
        let config = self.fcm.as_ref().context("fcm is not configured")?;
        let access_token = self.fcm_access_token().await?;

        let payload_json = serde_json::json!({
            "session": session,
        })
        .to_string();

        self.client
            .post(format!(
                "https://fcm.googleapis.com/v1/projects/{}/messages:send",
                config.project_id
            ))
            .bearer_auth(access_token)
            .json(&serde_json::json!({
                "message": {
                    "token": token,
                    "notification": {
                        "title": title,
                        "body": body,
                    },
                    "data": {
                        "payload_json": payload_json,
                        "session_id": session.id,
                    },
                    "android": {
                        "priority": "high",
                        "notification": {
                            "channel_id": "omni_code_replies",
                            "click_action": "FLUTTER_NOTIFICATION_CLICK",
                            "default_sound": true,
                            "default_vibrate_timings": true,
                        }
                    }
                }
            }))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn send_xiaomi_notification(
        &self,
        session: &SessionSummary,
        body: &str,
        device: &PushDeviceRegistration,
    ) -> Result<()> {
        let reg_id = device
            .mi_push_reg_id
            .as_deref()
            .context("missing xiaomi reg id")?;
        let config = self
            .xiaomi
            .as_ref()
            .context("xiaomi push is not configured")?;
        let payload_json = serde_json::json!({
            "session": session,
        })
        .to_string();

        self.client
            .post("https://api.xmpush.xiaomi.com/v3/message/regid")
            .header("Authorization", format!("key={}", config.app_secret))
            .form(&HashMap::from([
                ("registration_id", reg_id.to_string()),
                ("title", session.title.clone()),
                ("description", body.to_string()),
                ("payload", payload_json),
                ("restricted_package_name", config.package_name.clone()),
                ("notify_type", "1".to_string()),
                ("pass_through", "0".to_string()),
                ("extra.notify_foreground", "1".to_string()),
            ]))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn fcm_access_token(&self) -> Result<String> {
        {
            let cache = self.fcm_token_cache.lock().await;
            if let Some(token) = cache.as_ref()
                && token.expires_at > Instant::now() + Duration::from_secs(60)
            {
                return Ok(token.value.clone());
            }
        }

        let config = self.fcm.as_ref().context("fcm is not configured")?;
        let issued_at = Utc::now().timestamp();
        let expires_at = issued_at + 3600;
        let claims = FcmJwtClaims {
            iss: config.client_email.clone(),
            scope: "https://www.googleapis.com/auth/firebase.messaging".to_string(),
            aud: "https://oauth2.googleapis.com/token".to_string(),
            iat: issued_at,
            exp: expires_at,
        };

        let assertion = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(config.private_key_pem.as_bytes())?,
        )?;

        let response = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<FcmAccessTokenResponse>()
            .await?;

        let cached = CachedAccessToken {
            value: response.access_token.clone(),
            expires_at: Instant::now() + Duration::from_secs(response.expires_in.max(60)),
        };
        *self.fcm_token_cache.lock().await = Some(cached);

        Ok(response.access_token)
    }
}

#[derive(Clone)]
struct CachedAccessToken {
    value: String,
    expires_at: Instant,
}

#[derive(Clone)]
struct FcmConfig {
    project_id: String,
    client_email: String,
    private_key_pem: String,
}

impl FcmConfig {
    fn from_env() -> Option<Self> {
        if let Ok(json) = std::env::var("ECHO_MATE_FCM_SERVICE_ACCOUNT_JSON") {
            let parsed: FcmServiceAccountJson = serde_json::from_str(&json).ok()?;
            return Some(Self::from_service_account(parsed));
        }

        if let Ok(path) = std::env::var("ECHO_MATE_FCM_SERVICE_ACCOUNT_PATH") {
            let body = std::fs::read_to_string(path).ok()?;
            let parsed: FcmServiceAccountJson = serde_json::from_str(&body).ok()?;
            return Some(Self::from_service_account(parsed));
        }

        None
    }

    fn from_service_account(value: FcmServiceAccountJson) -> Self {
        Self {
            project_id: value.project_id,
            client_email: value.client_email,
            private_key_pem: value.private_key.replace("\\n", "\n"),
        }
    }
}

#[derive(Clone)]
struct XiaomiConfig {
    app_secret: String,
    package_name: String,
}

impl XiaomiConfig {
    fn from_env() -> Option<Self> {
        Some(Self {
            app_secret: std::env::var("ECHO_MATE_XIAOMI_APP_SECRET").ok()?,
            package_name: std::env::var("ECHO_MATE_XIAOMI_PACKAGE_NAME").ok()?,
        })
    }
}

#[derive(Deserialize)]
struct FcmServiceAccountJson {
    project_id: String,
    client_email: String,
    private_key: String,
}

#[derive(Serialize)]
struct FcmJwtClaims {
    iss: String,
    scope: String,
    aud: String,
    exp: i64,
    iat: i64,
}

#[derive(Deserialize)]
struct FcmAccessTokenResponse {
    access_token: String,
    expires_in: u64,
}
