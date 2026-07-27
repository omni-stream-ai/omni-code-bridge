use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::models::{
    AcpServerConfig, ModelProviderConfig, SpeakerFilterSettings, SpeechProfileSelection,
    SpeechVoiceSelection,
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
    /// Experimental ACP agent server configurations (sorted by priority)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acp_servers: Vec<AcpServerConfig>,
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
    /// ACP agent server configurations (replaces existing list when set)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_servers: Option<Vec<AcpServerConfig>>,
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
            acp_servers: Vec::new(),
            speech_profiles: SpeechProfileSelection::default(),
            speech_voices: SpeechVoiceSelection::default(),
            speaker_filter: SpeakerFilterSettings::default(),
        }
    }
}

impl BridgeSettingsStore {
    #[allow(dead_code)]
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

    pub async fn load_from_path_strict(path: PathBuf) -> Result<Self> {
        let settings = load_settings_from_path(&path).await?;
        validate_bridge_settings(&settings).map_err(anyhow::Error::msg)?;
        Ok(Self {
            path,
            settings: RwLock::new(settings),
        })
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

pub async fn load_settings_from_path(path: &PathBuf) -> Result<BridgeSettings> {
    let body = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read bridge settings: {}", path.display()))?;
    serde_json::from_str::<BridgeSettings>(&body)
        .with_context(|| format!("failed to parse bridge settings at {}", path.display()))
}

pub fn validate_bridge_settings(settings: &BridgeSettings) -> Result<(), String> {
    validate_model_providers(&settings.model_providers)?;
    validate_acp_servers(&settings.acp_servers)?;
    Ok(())
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

pub fn validate_acp_servers(servers: &[AcpServerConfig]) -> Result<(), String> {
    let mut seen_ids = std::collections::HashSet::new();
    for server in servers {
        if server.id.trim().is_empty() {
            return Err("ACP server id must not be empty".to_string());
        }
        if !seen_ids.insert(&server.id) {
            return Err(format!("duplicate ACP server id: {}", server.id));
        }
        if server.name.trim().is_empty() {
            return Err(format!("ACP server {} name must not be empty", server.id));
        }
        let has_endpoint = server
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .is_some();
        let has_command = server
            .command
            .as_deref()
            .map(str::trim)
            .filter(|command| !command.is_empty())
            .is_some();
        match server.profile {
            crate::models::AcpProfile::Stdio => {
                if !has_command {
                    return Err(format!(
                        "ACP server {} with profile `{}` must configure command",
                        server.id,
                        acp_profile_name(server.profile)
                    ));
                }
                if has_endpoint {
                    return Err(format!(
                        "ACP server {} with profile `{}` must not configure endpoint",
                        server.id,
                        acp_profile_name(server.profile)
                    ));
                }
            }
            crate::models::AcpProfile::GenericHttp => {
                if !has_endpoint {
                    return Err(format!(
                        "ACP server {} with profile `generic_http` must configure endpoint",
                        server.id
                    ));
                }
                if has_command {
                    return Err(format!(
                        "ACP server {} with profile `generic_http` must not configure command",
                        server.id
                    ));
                }
                if !server.args.is_empty() {
                    return Err(format!(
                        "ACP server {} with profile `generic_http` must not configure args",
                        server.id
                    ));
                }
                if !server.env.is_empty() {
                    return Err(format!(
                        "ACP server {} with profile `generic_http` must not configure env",
                        server.id
                    ));
                }
            }
        }
        if let Some(endpoint) = server.endpoint.as_deref().map(str::trim)
            && !endpoint.is_empty()
            && !endpoint.starts_with("http://")
            && !endpoint.starts_with("https://")
        {
            return Err(format!(
                "ACP server {} endpoint must start with http:// or https://",
                server.id
            ));
        }
        for header in &server.headers {
            if header.key.trim().is_empty() {
                return Err(format!(
                    "ACP server {} has a header with an empty key",
                    server.id
                ));
            }
        }
        for env in &server.env {
            if env.key.trim().is_empty() {
                return Err(format!(
                    "ACP server {} has an env entry with an empty key",
                    server.id
                ));
            }
        }
    }
    Ok(())
}

fn acp_profile_name(profile: crate::models::AcpProfile) -> &'static str {
    match profile {
        crate::models::AcpProfile::Stdio => "stdio",
        crate::models::AcpProfile::GenericHttp => "generic_http",
    }
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
        ModelProviderConfig, default_home_dir, validate_acp_servers, validate_bridge_settings,
        validate_model_providers,
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
                acp_servers: Vec::new(),
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

    #[tokio::test]
    async fn load_settings_from_path_rejects_invalid_json() {
        let path = test_path("invalid-settings-json");
        tokio::fs::write(&path, "{not-json")
            .await
            .expect("invalid settings fixture should be written");

        let error = super::load_settings_from_path(&path)
            .await
            .err()
            .expect("invalid settings should fail to load");
        assert!(
            error
                .to_string()
                .contains("failed to parse bridge settings")
        );

        let _ = tokio::fs::remove_file(path).await;
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

    #[test]
    fn validate_acp_servers_accepts_kiro_command_profile() {
        let servers = vec![crate::models::AcpServerConfig {
            id: "kiro-local".to_string(),
            name: "Kiro Local".to_string(),
            profile: crate::models::AcpProfile::Stdio,
            endpoint: None,
            command: Some("kiro-cli".to_string()),
            args: vec!["acp".to_string()],
            auth_token: String::new(),
            default_model: Some("claude-sonnet-4".to_string()),
            enabled: true,
            priority: 0,
            headers: Vec::new(),
            env: Vec::new(),
        }];

        assert!(validate_acp_servers(&servers).is_ok());
    }

    #[test]
    fn acp_profile_deserializes_legacy_kiro_alias_as_stdio() {
        let server: crate::models::AcpServerConfig = serde_json::from_str(
            r#"{
                "id": "kiro-local",
                "name": "Kiro Local ACP",
                "profile": "kiro",
                "command": "kiro-cli",
                "args": ["acp"]
            }"#,
        )
        .expect("legacy kiro profile should deserialize");

        assert!(matches!(server.profile, crate::models::AcpProfile::Stdio));
        assert!(validate_acp_servers(&[server]).is_ok());
    }

    #[test]
    fn validate_acp_servers_accepts_stdio_command_profile() {
        let servers = vec![crate::models::AcpServerConfig {
            id: "opencode-acp".to_string(),
            name: "OpenCode ACP".to_string(),
            profile: crate::models::AcpProfile::Stdio,
            endpoint: None,
            command: Some("opencode".to_string()),
            args: vec!["acp".to_string()],
            auth_token: String::new(),
            default_model: None,
            enabled: false,
            priority: 20,
            headers: Vec::new(),
            env: Vec::new(),
        }];

        assert!(validate_acp_servers(&servers).is_ok());
    }

    #[test]
    fn validate_acp_servers_rejects_profile_transport_mismatch() {
        let kiro_without_command = crate::models::AcpServerConfig {
            id: "bad-kiro".to_string(),
            name: "Bad Kiro".to_string(),
            profile: crate::models::AcpProfile::Stdio,
            endpoint: Some("https://acp.example.test".to_string()),
            command: None,
            args: Vec::new(),
            auth_token: String::new(),
            default_model: None,
            enabled: true,
            priority: 0,
            headers: Vec::new(),
            env: Vec::new(),
        };
        let http_without_endpoint = crate::models::AcpServerConfig {
            id: "bad-http".to_string(),
            name: "Bad HTTP".to_string(),
            profile: crate::models::AcpProfile::GenericHttp,
            endpoint: None,
            command: Some("kiro-cli".to_string()),
            args: vec!["acp".to_string()],
            auth_token: String::new(),
            default_model: None,
            enabled: true,
            priority: 1,
            headers: Vec::new(),
            env: Vec::new(),
        };

        assert!(validate_acp_servers(&[kiro_without_command]).is_err());
        assert!(validate_acp_servers(&[http_without_endpoint]).is_err());
    }

    #[test]
    fn validate_acp_servers_rejects_mixed_transport_fields() {
        let kiro_with_endpoint = crate::models::AcpServerConfig {
            id: "kiro-mixed".to_string(),
            name: "Kiro Mixed".to_string(),
            profile: crate::models::AcpProfile::Stdio,
            endpoint: Some("https://acp.example.test".to_string()),
            command: Some("kiro-cli".to_string()),
            args: vec!["acp".to_string()],
            auth_token: String::new(),
            default_model: None,
            enabled: true,
            priority: 0,
            headers: Vec::new(),
            env: Vec::new(),
        };
        let http_with_stdio_fields = crate::models::AcpServerConfig {
            id: "http-mixed".to_string(),
            name: "HTTP Mixed".to_string(),
            profile: crate::models::AcpProfile::GenericHttp,
            endpoint: Some("https://acp.example.test".to_string()),
            command: Some("kiro-cli".to_string()),
            args: vec!["acp".to_string()],
            auth_token: String::new(),
            default_model: None,
            enabled: true,
            priority: 1,
            headers: Vec::new(),
            env: vec![crate::models::HeaderKeyValue {
                key: "FOO".to_string(),
                value: "bar".to_string(),
            }],
        };

        assert!(validate_acp_servers(&[kiro_with_endpoint]).is_err());
        assert!(validate_acp_servers(&[http_with_stdio_fields]).is_err());
    }

    #[test]
    fn validate_bridge_settings_accepts_acp_example_shape() {
        let settings = BridgeSettings {
            ai_approval: AiApprovalSettings::default(),
            model_providers: Vec::new(),
            acp_servers: vec![
                crate::models::AcpServerConfig {
                    id: "kiro-local".to_string(),
                    name: "Kiro Local ACP".to_string(),
                    profile: crate::models::AcpProfile::Stdio,
                    endpoint: None,
                    command: Some("kiro-cli".to_string()),
                    args: vec!["acp".to_string()],
                    auth_token: String::new(),
                    default_model: Some("claude-sonnet-4".to_string()),
                    enabled: true,
                    priority: 0,
                    headers: Vec::new(),
                    env: Vec::new(),
                },
                crate::models::AcpServerConfig {
                    id: "opencode-acp".to_string(),
                    name: "OpenCode ACP".to_string(),
                    profile: crate::models::AcpProfile::Stdio,
                    endpoint: None,
                    command: Some("opencode".to_string()),
                    args: vec!["acp".to_string()],
                    auth_token: String::new(),
                    default_model: Some(String::new()),
                    enabled: false,
                    priority: 20,
                    headers: Vec::new(),
                    env: Vec::new(),
                },
                crate::models::AcpServerConfig {
                    id: "codex-acp".to_string(),
                    name: "Codex ACP".to_string(),
                    profile: crate::models::AcpProfile::Stdio,
                    endpoint: None,
                    command: Some("codex".to_string()),
                    args: vec!["acp".to_string()],
                    auth_token: String::new(),
                    default_model: Some(String::new()),
                    enabled: false,
                    priority: 30,
                    headers: Vec::new(),
                    env: Vec::new(),
                },
                crate::models::AcpServerConfig {
                    id: "acp-http".to_string(),
                    name: "ACP HTTP Gateway".to_string(),
                    profile: crate::models::AcpProfile::GenericHttp,
                    endpoint: Some("https://acp.example.com".to_string()),
                    command: None,
                    args: Vec::new(),
                    auth_token: "replace-me".to_string(),
                    default_model: Some("acp-default-model".to_string()),
                    enabled: false,
                    priority: 10,
                    headers: vec![crate::models::HeaderKeyValue {
                        key: "X-ACP-Client".to_string(),
                        value: "omni-code-bridge".to_string(),
                    }],
                    env: Vec::new(),
                },
            ],
            speech_profiles: Default::default(),
            speech_voices: Default::default(),
            speaker_filter: Default::default(),
        };

        assert!(validate_bridge_settings(&settings).is_ok());
    }

    #[tokio::test]
    async fn checked_in_acp_example_file_loads_and_validates() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("settings.acp.example.json");
        let settings = super::load_settings_from_path(&path)
            .await
            .expect("checked-in ACP example should load");
        validate_bridge_settings(&settings).expect("checked-in ACP example should validate");
        assert_eq!(settings.acp_servers.len(), 4);
        assert!(
            settings
                .acp_servers
                .iter()
                .any(|server| server.id == "opencode-acp"
                    && matches!(server.profile, crate::models::AcpProfile::Stdio))
        );
        assert!(
            settings
                .acp_servers
                .iter()
                .any(|server| server.id == "codex-acp"
                    && matches!(server.profile, crate::models::AcpProfile::Stdio))
        );
    }
}
