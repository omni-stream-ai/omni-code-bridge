use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::models::{SpeakerFilterSettings, SpeechProfileSelection, SpeechVoiceSelection};

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
            speech_profiles: SpeechProfileSelection::default(),
            speech_voices: SpeechVoiceSelection::default(),
            speaker_filter: SpeakerFilterSettings::default(),
        }
    }
}

impl BridgeSettingsStore {
    pub async fn load() -> Self {
        let path = settings_path();
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
        default_home_dir,
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
}
