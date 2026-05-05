use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiApprovalSettings {
    pub enabled: bool,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_key: String,
    pub model: String,
    pub max_risk: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeSettings {
    pub ai_approval: AiApprovalSettings,
}

pub struct BridgeSettingsStore {
    path: PathBuf,
    settings: RwLock<BridgeSettings>,
}

impl Default for AiApprovalSettings {
    fn default() -> Self {
        Self {
            enabled: env_bool("ECHO_MATE_AI_APPROVAL"),
            base_url: std::env::var("ECHO_MATE_AI_APPROVAL_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            api_key: std::env::var("ECHO_MATE_AI_APPROVAL_API_KEY")
                .or_else(|_| std::env::var("OPENAI_API_KEY"))
                .unwrap_or_default(),
            model: std::env::var("ECHO_MATE_AI_APPROVAL_MODEL")
                .unwrap_or_else(|_| "gpt-4.1-mini".to_string()),
            max_risk: std::env::var("ECHO_MATE_AI_APPROVAL_MAX_RISK")
                .unwrap_or_else(|_| "low".to_string()),
        }
    }
}

impl Default for BridgeSettings {
    fn default() -> Self {
        Self {
            ai_approval: AiApprovalSettings::default(),
        }
    }
}

impl BridgeSettingsStore {
    pub async fn load() -> Self {
        let path = settings_path();
        let settings = match tokio::fs::read_to_string(&path).await {
            Ok(body) => serde_json::from_str::<BridgeSettings>(&body).unwrap_or_default(),
            Err(_) => BridgeSettings::default(),
        };
        Self {
            path,
            settings: RwLock::new(settings),
        }
    }

    pub async fn get(&self) -> BridgeSettings {
        self.settings.read().await.clone()
    }

    pub async fn save(&self, settings: BridgeSettings) -> Result<BridgeSettings> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create settings directory: {}", parent.display())
            })?;
        }
        let body =
            serde_json::to_string_pretty(&settings).context("failed to encode bridge settings")?;
        tokio::fs::write(&self.path, body)
            .await
            .with_context(|| format!("failed to write bridge settings: {}", self.path.display()))?;
        *self.settings.write().await = settings.clone();
        Ok(settings)
    }
}

pub fn settings_path() -> PathBuf {
    std::env::var("ECHO_MATE_SETTINGS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("omni-code-desktop-bridge/settings.json"))
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}
