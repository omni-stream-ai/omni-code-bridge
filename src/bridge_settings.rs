use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::models::{
    ModelProviderConfig, SpeakerFilterSettings, SpeechProfileSelection, SpeechVoiceSelection,
};

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
    /// Global model provider configurations (sorted by priority)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_providers: Vec<ModelProviderConfig>,
    #[serde(default)]
    pub speech_profiles: SpeechProfileSelection,
    #[serde(default)]
    pub speech_voices: SpeechVoiceSelection,
    #[serde(default)]
    pub speaker_filter: SpeakerFilterSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BridgeSettingsInput {
    pub ai_approval: AiApprovalSettings,
    /// Global model provider configurations (replaces existing list when set)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_providers: Option<Vec<ModelProviderConfig>>,
    #[serde(default)]
    pub speech_profiles: Option<SpeechProfileSelection>,
    #[serde(default)]
    pub speech_voices: Option<SpeechVoiceSelection>,
    #[serde(default)]
    pub speaker_filter: Option<SpeakerFilterSettings>,
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
            model_providers: Vec::new(),
            speech_profiles: SpeechProfileSelection::default(),
            speech_voices: SpeechVoiceSelection::default(),
            speaker_filter: SpeakerFilterSettings::default(),
        }
    }
}

impl BridgeSettingsStore {
    pub async fn load_from_path(path: PathBuf) -> Self {
        let settings = match tokio::fs::read_to_string(&path).await {
            Ok(body) => match serde_json::from_str::<BridgeSettings>(&body) {
                Ok(settings) => settings,
                Err(error) => {
                    eprintln!(
                        "failed to parse bridge settings at {}: {error}",
                        path.display()
                    );
                    BridgeSettings::default()
                }
            },
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

    pub async fn update<F>(&self, update: F) -> Result<BridgeSettings>
    where
        F: FnOnce(&mut BridgeSettings),
    {
        let mut settings = self.settings.write().await;
        update(&mut settings);
        write_settings(&self.path, &settings).await?;
        Ok(settings.clone())
    }
}

async fn write_settings(path: &PathBuf, settings: &BridgeSettings) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!("failed to create settings directory: {}", parent.display())
        })?;
    }
    let body =
        serde_json::to_string_pretty(settings).context("failed to encode bridge settings")?;
    tokio::fs::write(path, body)
        .await
        .with_context(|| format!("failed to write bridge settings: {}", path.display()))
}

pub fn settings_path() -> PathBuf {
    std::env::var("ECHO_MATE_SETTINGS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            default_home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".omni-code")
                .join("settings.json")
        })
}

/// Project-scoped temporary directory (~/.omni-code/tmp/<subdir>).
/// Cross-platform: uses the same base directory as settings.
pub fn project_tmp_dir(subdir: &str) -> PathBuf {
    default_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".omni-code")
        .join("tmp")
        .join(subdir)
}

/// Validate a list of model provider configurations
pub fn validate_model_providers(providers: &[ModelProviderConfig]) -> Result<(), String> {
    let mut seen_ids = std::collections::HashSet::new();

    for provider in providers {
        // Validate ID is not empty
        if provider.id.trim().is_empty() {
            return Err("provider id must not be empty".to_string());
        }

        // Validate ID is unique
        if !seen_ids.insert(&provider.id) {
            return Err(format!("duplicate provider id: {}", provider.id));
        }

        // Validate base_url is not empty
        if provider.base_url.trim().is_empty() {
            return Err(format!(
                "provider {} base_url must not be empty",
                provider.id
            ));
        }

        // Validate base_url starts with http:// or https://
        let base_url = provider.base_url.trim();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(format!(
                "provider {} base_url must start with http:// or https://",
                provider.id
            ));
        }

        // Validate name is not empty
        if provider.name.trim().is_empty() {
            return Err(format!("provider {} name must not be empty", provider.id));
        }
    }

    Ok(())
}

fn default_home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            let mut combined = PathBuf::from(drive);
            combined.push(path);
            Some(combined)
        })
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

#[cfg(test)]
mod tests {
    use super::{
        AiApprovalSettings, BridgeSettings, BridgeSettingsInput, BridgeSettingsStore,
        ModelProviderConfig, default_home_dir, validate_model_providers,
    };
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn default_home_dir_reads_standard_home_env() {
        let expected = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .or_else(|| {
                let drive = std::env::var_os("HOMEDRIVE")?;
                let path = std::env::var_os("HOMEPATH")?;
                let mut combined = PathBuf::from(drive);
                combined.push(path);
                Some(combined)
            });

        assert_eq!(default_home_dir(), expected);
    }

    #[test]
    fn settings_input_preserves_missing_speech_settings_as_none() {
        let input: BridgeSettingsInput = serde_json::from_str(
            r#"{
                "ai_approval": {
                    "enabled": true,
                    "base_url": "https://example.test/v1",
                    "model": "review-model",
                    "max_risk": "medium"
                }
            }"#,
        )
        .unwrap();

        assert!(input.speech_profiles.is_none());
        assert!(input.speech_voices.is_none());
    }

    #[tokio::test]
    async fn update_persists_mutated_settings() {
        let store = BridgeSettingsStore {
            path: test_path("bridge-settings-update"),
            settings: tokio::sync::RwLock::new(BridgeSettings {
                ai_approval: AiApprovalSettings::default(),
                model_providers: Vec::new(),
                speech_profiles: Default::default(),
                speech_voices: Default::default(),
                speaker_filter: Default::default(),
            }),
        };

        let updated = store
            .update(|settings| {
                settings.speech_profiles.tts_default = Some("kokoro-int8-multi-lang-v1_1".into());
                settings
                    .speech_voices
                    .tts_by_model
                    .insert("kokoro-int8-multi-lang-v1_1".into(), "48".into());
            })
            .await
            .unwrap();

        assert_eq!(
            updated.speech_profiles.tts_default.as_deref(),
            Some("kokoro-int8-multi-lang-v1_1")
        );
        assert_eq!(
            updated
                .speech_voices
                .tts_by_model
                .get("kokoro-int8-multi-lang-v1_1")
                .map(String::as_str),
            Some("48")
        );

        let body = tokio::fs::read_to_string(&store.path).await.unwrap();
        assert!(body.contains("kokoro-int8-multi-lang-v1_1"));
        assert!(body.contains("\"48\""));
        let _ = tokio::fs::remove_file(&store.path).await;
    }

    fn test_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omni-code-bridge-{prefix}-{unique}.json"))
    }

    #[test]
    fn validate_model_providers_accepts_valid_config() {
        let providers = vec![
            ModelProviderConfig {
                id: "openai".to_string(),
                name: "OpenAI".to_string(),
                base_url: "https://api.openai.com/v1".to_string(),
                api_key: "sk-test".to_string(),
                model: Some("gpt-4o".to_string()),
                format: crate::models::ApiFormat::OpenAiCompatible,
                enabled: true,
                priority: 1,
            },
            ModelProviderConfig {
                id: "anthropic".to_string(),
                name: "Anthropic".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                api_key: "sk-ant-test".to_string(),
                model: None,
                format: crate::models::ApiFormat::AnthropicMessages,
                enabled: true,
                priority: 2,
            },
        ];
        assert!(validate_model_providers(&providers).is_ok());
    }

    #[test]
    fn validate_model_providers_rejects_empty_id() {
        let providers = vec![ModelProviderConfig {
            id: "".to_string(),
            name: "Test".to_string(),
            base_url: "https://api.example.com".to_string(),
            api_key: String::new(),
            model: None,
            format: crate::models::ApiFormat::OpenAiCompatible,
            enabled: true,
            priority: 1,
        }];
        assert!(validate_model_providers(&providers).is_err());
    }

    #[test]
    fn validate_model_providers_rejects_duplicate_id() {
        let providers = vec![
            ModelProviderConfig {
                id: "same-id".to_string(),
                name: "First".to_string(),
                base_url: "https://api.example.com".to_string(),
                api_key: String::new(),
                model: None,
                format: crate::models::ApiFormat::OpenAiCompatible,
                enabled: true,
                priority: 1,
            },
            ModelProviderConfig {
                id: "same-id".to_string(),
                name: "Second".to_string(),
                base_url: "https://api.other.com".to_string(),
                api_key: String::new(),
                model: None,
                format: crate::models::ApiFormat::OpenAiCompatible,
                enabled: true,
                priority: 2,
            },
        ];
        assert!(validate_model_providers(&providers).is_err());
    }

    #[test]
    fn validate_model_providers_rejects_empty_base_url() {
        let providers = vec![ModelProviderConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            base_url: "".to_string(),
            api_key: String::new(),
            model: None,
            format: crate::models::ApiFormat::OpenAiCompatible,
            enabled: true,
            priority: 1,
        }];
        assert!(validate_model_providers(&providers).is_err());
    }

    #[test]
    fn validate_model_providers_rejects_invalid_base_url() {
        let providers = vec![ModelProviderConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            base_url: "ftp://invalid.com".to_string(),
            api_key: String::new(),
            model: None,
            format: crate::models::ApiFormat::OpenAiCompatible,
            enabled: true,
            priority: 1,
        }];
        assert!(validate_model_providers(&providers).is_err());
    }
}
