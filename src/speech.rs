use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    bridge_settings::BridgeSettingsStore,
    models::{
        SpeechComputeBackend, SpeechDownloadStatus, SpeechDownloadTask, SpeechModelCapabilities,
        SpeechModelKind, SpeechModelSummary, SpeechProfile, SpeechProfileSelection, SpeechRuntime,
        SpeechStatus, SpeechVoiceSelection, SpeechVoiceSummary,
    },
    tts,
};

#[derive(Debug, Clone)]
struct SpeechModelCatalogEntry {
    id: &'static str,
    kind: SpeechModelKind,
    display_name: &'static str,
    description: &'static str,
    languages: &'static [&'static str],
    runtime: SpeechRuntime,
    backend: SpeechComputeBackend,
    capabilities: SpeechModelCapabilities,
    features: &'static [&'static str],
    supports_profiles: &'static [SpeechProfile],
    recommended_profiles: &'static [SpeechProfile],
    download_url: &'static str,
    docs_url: Option<&'static str>,
    download_size_mb: Option<u32>,
    memory_hint: Option<&'static str>,
    notes: Option<&'static str>,
    sample_rate_hz: Option<u32>,
    default_voice: Option<&'static str>,
    voice_count_hint: Option<u32>,
    required_files: &'static [&'static str],
    download_layout: SpeechDownloadLayout,
}

#[derive(Debug, Clone)]
pub struct InstalledSpeechModel {
    pub install_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum SpeechDownloadLayout {
    ArchiveTarBz2,
    SingleFile { filename: &'static str },
}

#[derive(Debug, Clone)]
pub enum SpeechResolutionError {
    ProfileUnset { hint: String },
    ModelNotInstalled { hint: String },
}

pub struct SpeechService {
    root_dir: PathBuf,
    installed: RwLock<HashMap<String, InstalledSpeechModel>>,
    downloads: RwLock<HashMap<String, SpeechDownloadTask>>,
}

impl SpeechService {
    pub async fn load() -> Self {
        let root_dir = speech_root_dir();
        let installed = scan_installed_models(&root_dir).await;
        let downloads = load_downloads(&root_dir).await;
        Self {
            root_dir,
            installed: RwLock::new(installed),
            downloads: RwLock::new(downloads),
        }
    }

    pub async fn status(
        &self,
        profiles: SpeechProfileSelection,
        voices: SpeechVoiceSelection,
    ) -> SpeechStatus {
        let models = self.list_models(profiles.clone()).await;
        let mut downloads = self
            .downloads
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        downloads.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        SpeechStatus {
            root_dir: self.root_dir.display().to_string(),
            profiles,
            voices,
            models,
            downloads,
        }
    }

    pub async fn list_models(&self, profiles: SpeechProfileSelection) -> Vec<SpeechModelSummary> {
        let installed = self.installed.read().await;
        catalog_entries()
            .iter()
            .map(|entry| {
                let installed_model = installed.get(entry.id);
                let selected_by = selected_profiles_for_model(&profiles, entry.id);
                let (sample_rate_hz, default_voice, voices, voice_details) =
                    runtime_model_details(entry, installed_model);
                SpeechModelSummary {
                    id: entry.id.to_string(),
                    kind: entry.kind,
                    display_name: entry.display_name.to_string(),
                    description: entry.description.to_string(),
                    languages: entry.languages.iter().map(|v| (*v).to_string()).collect(),
                    runtime: entry.runtime,
                    backend: entry.backend,
                    capabilities: entry.capabilities.clone(),
                    features: entry.features.iter().map(|v| (*v).to_string()).collect(),
                    supports_profiles: entry.supports_profiles.to_vec(),
                    recommended_profiles: entry.recommended_profiles.to_vec(),
                    download_url: entry.download_url.to_string(),
                    docs_url: entry.docs_url.map(ToString::to_string),
                    download_size_mb: entry.download_size_mb,
                    memory_hint: entry.memory_hint.map(ToString::to_string),
                    notes: entry.notes.map(ToString::to_string),
                    sample_rate_hz,
                    default_voice,
                    voices,
                    voice_details,
                    installed: installed_model.is_some(),
                    install_path: installed_model
                        .map(|item| item.install_path.display().to_string()),
                    selected_by,
                }
            })
            .collect()
    }

    pub async fn get_download(&self, task_id: &str) -> Option<SpeechDownloadTask> {
        self.downloads.read().await.get(task_id).cloned()
    }

    pub async fn resolve_model_by_id(
        &self,
        model_id: &str,
    ) -> Result<InstalledSpeechModel, String> {
        if catalog_entry(model_id).is_none() {
            return Err(format!("unknown speech model: {model_id}"));
        }

        self.refresh_installed_model(model_id).await.ok_or_else(|| {
            format!("model {model_id} is not installed; call POST /speech/models/downloads first")
        })
    }

    pub fn model_kind(&self, model_id: &str) -> Option<SpeechModelKind> {
        catalog_entry(model_id).map(|entry| entry.kind)
    }

    pub fn supports_profile(&self, model_id: &str, profile: SpeechProfile) -> bool {
        catalog_entry(model_id)
            .map(|entry| entry.supports_profiles.contains(&profile))
            .unwrap_or(false)
    }

    pub fn first_model_for_profile(&self, profile: SpeechProfile) -> Option<String> {
        catalog_entries()
            .iter()
            .filter(|entry| entry.supports_profiles.contains(&profile))
            .map(|entry| entry.id.to_string())
            .next()
    }

    pub async fn installed_model_path(&self, model_id: &str) -> Option<PathBuf> {
        self.refresh_installed_model(model_id)
            .await
            .map(|model| model.install_path)
    }

    pub async fn resolve_profile_model(
        &self,
        profiles: &SpeechProfileSelection,
        profile: SpeechProfile,
    ) -> Result<InstalledSpeechModel, SpeechResolutionError> {
        let Some(model_id) = profiles.model_for_profile(profile) else {
            return Err(SpeechResolutionError::ProfileUnset {
                hint: format!(
                    "configure speech profile {} by calling PUT /speech/profiles/{}/model",
                    profile_slug(profile),
                    profile_slug(profile)
                ),
            });
        };

        self.refresh_installed_model(model_id)
            .await
            .ok_or_else(|| SpeechResolutionError::ModelNotInstalled {
                hint: format!(
                    "download model {model_id} from POST /speech/models/downloads before using profile {}",
                    profile_slug(profile)
                ),
            })
    }

    pub async fn refresh_installed_model(&self, model_id: &str) -> Option<InstalledSpeechModel> {
        if let Some(existing) = self.installed.read().await.get(model_id).cloned() {
            return Some(existing);
        }

        let entry = catalog_entry(model_id)?;
        let install_dir = self.root_dir.join(entry.id);
        if verify_required_files(&install_dir, entry.required_files)
            .await
            .is_err()
        {
            return None;
        }

        let installed_model = InstalledSpeechModel {
            install_path: install_dir,
        };
        self.installed
            .write()
            .await
            .insert(entry.id.to_string(), installed_model.clone());
        Some(installed_model)
    }

    pub async fn queue_download(self: &Arc<Self>, model_id: &str) -> Result<SpeechDownloadTask> {
        let entry =
            catalog_entry(model_id).ok_or_else(|| anyhow!("unknown speech model: {model_id}"))?;
        tokio::fs::create_dir_all(&self.root_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create speech directory: {}",
                    self.root_dir.display()
                )
            })?;

        let now = Utc::now();
        let task = SpeechDownloadTask {
            task_id: Uuid::new_v4().to_string(),
            model_id: entry.id.to_string(),
            status: SpeechDownloadStatus::Queued,
            progress_bytes: Some(0),
            total_bytes: None,
            install_path: None,
            error: None,
            created_at: now,
            updated_at: now,
        };

        {
            let mut downloads = self.downloads.write().await;
            downloads.retain(|_, existing| existing.model_id != entry.id);
            downloads.insert(task.task_id.clone(), task.clone());
        }
        self.persist_downloads().await?;

        let service = Arc::clone(self);
        let task_id = task.task_id.clone();
        tokio::spawn(async move {
            if let Err(error) = service.perform_download(task_id.clone()).await {
                let _ = service
                    .set_download_failed(&task_id, error.to_string())
                    .await;
            }
        });

        Ok(task)
    }

    pub async fn verify_profile(
        &self,
        profiles: &SpeechProfileSelection,
        profile: SpeechProfile,
    ) -> Result<InstalledSpeechModel, String> {
        self.resolve_profile_model(profiles, profile)
            .await
            .map_err(|error| match error {
                SpeechResolutionError::ProfileUnset { hint } => hint,
                SpeechResolutionError::ModelNotInstalled { hint } => hint,
            })
    }

    async fn perform_download(&self, task_id: String) -> Result<()> {
        let model_id = {
            let downloads = self.downloads.read().await;
            downloads
                .get(&task_id)
                .map(|task| task.model_id.clone())
                .ok_or_else(|| anyhow!("download task not found: {task_id}"))?
        };
        let entry =
            catalog_entry(&model_id).ok_or_else(|| anyhow!("unknown model in task: {model_id}"))?;

        self.update_download(&task_id, |task| {
            task.status = SpeechDownloadStatus::Downloading;
            task.error = None;
            task.updated_at = Utc::now();
        })
        .await?;

        let download_path = match entry.download_layout {
            SpeechDownloadLayout::ArchiveTarBz2 => {
                self.root_dir.join(format!("{}.download", entry.id))
            }
            SpeechDownloadLayout::SingleFile { filename } => {
                self.root_dir.join(format!("{}.{}", entry.id, filename))
            }
        };
        let tmp_dir = self.root_dir.join(format!("{}.tmp", entry.id));
        let install_dir = self.root_dir.join(entry.id);
        let client = reqwest::Client::new();
        let response = client
            .get(entry.download_url)
            .send()
            .await
            .with_context(|| format!("failed to request {}", entry.download_url))?;
        if !response.status().is_success() {
            bail!("download request failed with {}", response.status());
        }

        let total = response.content_length();
        self.update_download(&task_id, |task| {
            task.total_bytes = total;
            task.updated_at = Utc::now();
        })
        .await?;

        let mut stream = response.bytes_stream();
        let mut file = tokio::fs::File::create(&download_path)
            .await
            .with_context(|| format!("failed to create {}", download_path.display()))?;
        let mut downloaded = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed while downloading model archive")?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
                .await
                .with_context(|| format!("failed to write {}", download_path.display()))?;
            downloaded += chunk.len() as u64;
            self.update_download(&task_id, |task| {
                task.progress_bytes = Some(downloaded);
                task.updated_at = Utc::now();
            })
            .await?;
        }
        tokio::io::AsyncWriteExt::flush(&mut file)
            .await
            .with_context(|| format!("failed to flush {}", download_path.display()))?;

        self.update_download(&task_id, |task| {
            task.status = SpeechDownloadStatus::Extracting;
            task.updated_at = Utc::now();
        })
        .await?;

        if tokio::fs::try_exists(&tmp_dir).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&tmp_dir)
                .await
                .with_context(|| format!("failed to clear {}", tmp_dir.display()))?;
        }
        tokio::fs::create_dir_all(&tmp_dir)
            .await
            .with_context(|| format!("failed to create {}", tmp_dir.display()))?;

        match entry.download_layout {
            SpeechDownloadLayout::ArchiveTarBz2 => {
                extract_archive(&download_path, &tmp_dir).await?;
            }
            SpeechDownloadLayout::SingleFile { filename } => {
                let model_dir = tmp_dir.join(entry.id);
                tokio::fs::create_dir_all(&model_dir)
                    .await
                    .with_context(|| format!("failed to create {}", model_dir.display()))?;
                tokio::fs::copy(&download_path, model_dir.join(filename))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to install {} into {}",
                            download_path.display(),
                            model_dir.display()
                        )
                    })?;
            }
        }

        self.update_download(&task_id, |task| {
            task.status = SpeechDownloadStatus::Verifying;
            task.updated_at = Utc::now();
        })
        .await?;

        let final_dir = detect_model_root(&tmp_dir, entry.required_files)
            .await
            .unwrap_or(tmp_dir.clone());
        verify_required_files(&final_dir, entry.required_files).await?;

        if tokio::fs::try_exists(&install_dir).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&install_dir)
                .await
                .with_context(|| format!("failed to replace {}", install_dir.display()))?;
        }
        tokio::fs::rename(&final_dir, &install_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to install model {} into {}",
                    entry.id,
                    install_dir.display()
                )
            })?;

        if tokio::fs::try_exists(&tmp_dir).await.unwrap_or(false) {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        }
        let _ = tokio::fs::remove_file(&download_path).await;

        {
            let mut installed = self.installed.write().await;
            installed.insert(
                entry.id.to_string(),
                InstalledSpeechModel {
                    install_path: install_dir.clone(),
                },
            );
        }

        self.update_download(&task_id, |task| {
            task.status = SpeechDownloadStatus::Completed;
            task.install_path = Some(install_dir.display().to_string());
            task.updated_at = Utc::now();
        })
        .await?;

        Ok(())
    }

    async fn set_download_failed(&self, task_id: &str, error: String) -> Result<()> {
        self.update_download(task_id, |task| {
            task.status = SpeechDownloadStatus::Failed;
            task.error = Some(error.clone());
            task.updated_at = Utc::now();
        })
        .await
    }

    async fn update_download(
        &self,
        task_id: &str,
        mut update: impl FnMut(&mut SpeechDownloadTask),
    ) -> Result<()> {
        {
            let mut downloads = self.downloads.write().await;
            let task = downloads
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("download task not found: {task_id}"))?;
            update(task);
        }
        self.persist_downloads().await
    }

    async fn persist_downloads(&self) -> Result<()> {
        let path = downloads_state_path(&self.root_dir);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create speech metadata directory: {}",
                    parent.display()
                )
            })?;
        }
        let body = {
            let downloads = self.downloads.read().await;
            serde_json::to_string_pretty(&*downloads)
                .context("failed to encode speech download state")?
        };
        tokio::fs::write(&path, body)
            .await
            .with_context(|| format!("failed to write {}", path.display()))
    }
}

pub async fn set_profile_model(
    settings: &BridgeSettingsStore,
    speech: &SpeechService,
    profile: SpeechProfile,
    model_id: Option<String>,
) -> Result<SpeechProfileSelection> {
    if let Some(model_id) = model_id.as_deref() {
        let entry =
            catalog_entry(model_id).ok_or_else(|| anyhow!("unknown speech model: {model_id}"))?;
        let compatible = entry.supports_profiles.contains(&profile);
        if !compatible {
            bail!(
                "model {model_id} does not support profile {}",
                profile_slug(profile)
            );
        }
        if speech.refresh_installed_model(model_id).await.is_none() {
            bail!("model {model_id} is not installed; call POST /speech/models/downloads first");
        }
    }

    let next = settings
        .update(|settings| {
            settings
                .speech_profiles
                .set_model_for_profile(profile, model_id);
        })
        .await?;
    Ok(next.speech_profiles)
}

pub async fn set_tts_model_voice(
    settings: &BridgeSettingsStore,
    speech: &SpeechService,
    model_id: &str,
    voice: Option<String>,
) -> Result<SpeechVoiceSelection> {
    validate_tts_model_voice(speech, model_id, voice.as_deref()).await?;
    let model_id = model_id.to_string();
    let next = settings
        .update(|settings| {
            if let Some(voice) = voice {
                settings.speech_voices.tts_by_model.insert(model_id, voice);
            } else {
                settings.speech_voices.tts_by_model.remove(&model_id);
            }
        })
        .await?;
    Ok(next.speech_voices)
}

pub async fn validate_tts_model_voice(
    speech: &SpeechService,
    model_id: &str,
    voice: Option<&str>,
) -> Result<()> {
    let entry =
        catalog_entry(model_id).ok_or_else(|| anyhow!("unknown speech model: {model_id}"))?;
    if entry.kind != SpeechModelKind::Tts {
        bail!("model {model_id} is not a TTS model");
    }
    let Some(voice) = voice.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let voice_id = voice
        .parse::<usize>()
        .map_err(|_| anyhow!("voice must be a numeric speaker id"))?;
    let models = speech
        .list_models(SpeechProfileSelection::default())
        .await
        .into_iter()
        .find(|model| model.id == model_id)
        .ok_or_else(|| anyhow!("unknown speech model: {model_id}"))?;
    if !models.installed {
        bail!("model {model_id} is not installed; call POST /speech/models/downloads first");
    }
    if !models.voices.iter().any(|candidate| candidate == voice) {
        let max_voice = models.voices.len().saturating_sub(1);
        bail!(
            "voice {voice_id} is not supported by model {model_id}; supported range is [0, {max_voice}]"
        );
    }
    Ok(())
}

impl SpeechProfileSelection {
    pub fn model_for_profile(&self, profile: SpeechProfile) -> Option<&str> {
        match profile {
            SpeechProfile::AsrBatch => self.asr_batch.as_deref(),
            SpeechProfile::AsrRealtime => self.asr_realtime.as_deref(),
            SpeechProfile::TtsDefault => self.tts_default.as_deref(),
            SpeechProfile::VadDefault => self.vad_default.as_deref(),
            SpeechProfile::WakeWordDefault => self.wake_word_default.as_deref(),
        }
    }

    pub fn set_model_for_profile(&mut self, profile: SpeechProfile, model_id: Option<String>) {
        match profile {
            SpeechProfile::AsrBatch => self.asr_batch = model_id,
            SpeechProfile::AsrRealtime => self.asr_realtime = model_id,
            SpeechProfile::TtsDefault => self.tts_default = model_id,
            SpeechProfile::VadDefault => self.vad_default = model_id,
            SpeechProfile::WakeWordDefault => self.wake_word_default = model_id,
        }
    }
}

pub fn profile_slug(profile: SpeechProfile) -> &'static str {
    match profile {
        SpeechProfile::AsrBatch => "asr.batch",
        SpeechProfile::AsrRealtime => "asr.realtime",
        SpeechProfile::TtsDefault => "tts.default",
        SpeechProfile::VadDefault => "vad.default",
        SpeechProfile::WakeWordDefault => "wake_word.default",
    }
}

pub fn profile_from_slug(value: &str) -> Option<SpeechProfile> {
    match value {
        "asr.batch" => Some(SpeechProfile::AsrBatch),
        "asr.realtime" => Some(SpeechProfile::AsrRealtime),
        "tts.default" => Some(SpeechProfile::TtsDefault),
        "vad.default" => Some(SpeechProfile::VadDefault),
        "wake_word.default" => Some(SpeechProfile::WakeWordDefault),
        _ => None,
    }
}

fn selected_profiles_for_model(
    profiles: &SpeechProfileSelection,
    model_id: &str,
) -> Vec<SpeechProfile> {
    [
        SpeechProfile::AsrBatch,
        SpeechProfile::AsrRealtime,
        SpeechProfile::TtsDefault,
        SpeechProfile::VadDefault,
        SpeechProfile::WakeWordDefault,
    ]
    .into_iter()
    .filter(|profile| profiles.model_for_profile(*profile) == Some(model_id))
    .collect()
}

fn catalog_entry(model_id: &str) -> Option<&'static SpeechModelCatalogEntry> {
    catalog_entries().iter().find(|entry| entry.id == model_id)
}

fn catalog_entries() -> &'static [SpeechModelCatalogEntry] {
    &[
        SpeechModelCatalogEntry {
            id: "sensevoice-small-int8",
            kind: SpeechModelKind::Asr,
            display_name: "SenseVoice Small INT8",
            description: "Chinese-first multilingual offline ASR for batch transcription.",
            languages: &["zh", "yue", "en", "ja", "ko"],
            runtime: SpeechRuntime::Offline,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: false,
                realtime_asr: false,
                batch_asr: true,
                speech_synthesis: false,
                vad: false,
                speaker_embedding: false,
                wake_word: false,
                endpointing: false,
                punctuation: true,
                inverse_text_normalization: true,
                multilingual: true,
            },
            features: &["asr", "multilingual", "punctuation", "itn", "int8"],
            supports_profiles: &[SpeechProfile::AsrBatch],
            recommended_profiles: &[SpeechProfile::AsrBatch],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-sense-voice-zh-en-ja-ko-yue-2024-07-17.tar.bz2",
            docs_url: Some("https://k2-fsa.github.io/sherpa/onnx/sense-voice/pretrained.html"),
            download_size_mb: Some(1000),
            memory_hint: Some(
                "Recommended for 16GB+ RAM when running alongside other speech models.",
            ),
            notes: Some("Best default for batch Mandarin transcription on this machine."),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["model.int8.onnx", "tokens.txt"],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "streaming-paraformer-zh-en",
            kind: SpeechModelKind::Asr,
            display_name: "Streaming Paraformer ZH-EN",
            description: "Bilingual streaming ASR for low-latency voice-call mode.",
            languages: &["zh", "en"],
            runtime: SpeechRuntime::Streaming,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: true,
                batch_asr: false,
                speech_synthesis: false,
                vad: false,
                speaker_embedding: false,
                wake_word: false,
                endpointing: true,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &["asr", "streaming", "low-latency", "bilingual", "int8"],
            supports_profiles: &[SpeechProfile::AsrRealtime],
            recommended_profiles: &[SpeechProfile::AsrRealtime],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-streaming-paraformer-bilingual-zh-en.tar.bz2",
            docs_url: Some(
                "https://k2-fsa.github.io/sherpa/onnx/pretrained_models/online-paraformer/paraformer-models.html",
            ),
            download_size_mb: Some(1000),
            memory_hint: Some("Lightweight enough for always-on realtime recognition on CPU."),
            notes: Some("Recommended ASR profile for future call mode and websocket streaming."),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["encoder.int8.onnx", "decoder.int8.onnx", "tokens.txt"],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "funasr-streaming-paraformer-zh-yue-en",
            kind: SpeechModelKind::Asr,
            display_name: "FunASR Streaming Paraformer ZH-YUE-EN",
            description: "FunASR trilingual streaming ASR for Mandarin, Cantonese, and English call mode.",
            languages: &["zh", "yue", "en"],
            runtime: SpeechRuntime::Streaming,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: true,
                batch_asr: false,
                speech_synthesis: false,
                vad: false,
                speaker_embedding: false,
                wake_word: false,
                endpointing: true,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &[
                "asr",
                "streaming",
                "low-latency",
                "funasr",
                "trilingual",
                "int8",
            ],
            supports_profiles: &[SpeechProfile::AsrRealtime],
            recommended_profiles: &[SpeechProfile::AsrRealtime],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-streaming-paraformer-trilingual-zh-cantonese-en.tar.bz2",
            docs_url: Some(
                "https://k2-fsa.github.io/sherpa/onnx/pretrained_models/online-paraformer/paraformer-models.html",
            ),
            download_size_mb: Some(1000),
            memory_hint: Some(
                "CPU-friendly streaming ASR; try this when the default realtime model is unstable for mixed Chinese/Cantonese/English speech.",
            ),
            notes: Some(
                "FunASR/ModelScope-derived online Paraformer packaged for sherpa-onnx realtime recognition.",
            ),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["encoder.int8.onnx", "decoder.int8.onnx", "tokens.txt"],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "vits-melo-tts-zh-en",
            kind: SpeechModelKind::Tts,
            display_name: "MeloTTS ZH-EN VITS",
            description: "Chinese and English offline TTS with practical CPU latency.",
            languages: &["zh", "en"],
            runtime: SpeechRuntime::Offline,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: false,
                batch_asr: false,
                speech_synthesis: true,
                vad: false,
                speaker_embedding: false,
                wake_word: false,
                endpointing: false,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &["tts", "streaming", "zh", "en", "cpu-friendly"],
            supports_profiles: &[SpeechProfile::TtsDefault],
            recommended_profiles: &[SpeechProfile::TtsDefault],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/vits-melo-tts-zh_en.tar.bz2",
            docs_url: Some("https://k2-fsa.github.io/sherpa/onnx/tts/pretrained_models/vits.html"),
            download_size_mb: Some(163),
            memory_hint: Some(
                "Reasonable on CPU; keep one TTS request at a time for lowest latency.",
            ),
            notes: Some(
                "Good default TTS for Chinese UI playback without depending on a GPU. Supports progressive playback through the bridge streaming endpoint.",
            ),
            sample_rate_hz: Some(44_100),
            default_voice: Some("0"),
            voice_count_hint: Some(1),
            required_files: &["model.onnx", "tokens.txt", "lexicon.txt"],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "kokoro-int8-multi-lang-v1_1",
            kind: SpeechModelKind::Tts,
            display_name: "Kokoro INT8 Multi-Lang v1.1",
            description: "Chinese and English offline multi-speaker TTS with a compact INT8 model.",
            languages: &["zh", "en"],
            runtime: SpeechRuntime::Offline,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: false,
                batch_asr: false,
                speech_synthesis: true,
                vad: false,
                speaker_embedding: false,
                wake_word: false,
                endpointing: false,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &["tts", "streaming", "zh", "en", "multi-speaker", "int8"],
            supports_profiles: &[SpeechProfile::TtsDefault],
            recommended_profiles: &[SpeechProfile::TtsDefault],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/kokoro-int8-multi-lang-v1_1.tar.bz2",
            docs_url: Some(
                "https://k2-fsa.github.io/sherpa/onnx/tts/pretrained_models/kokoro.html",
            ),
            download_size_mb: Some(215),
            memory_hint: Some(
                "Better speaker variety than MeloTTS while staying practical on CPU-only deployments.",
            ),
            notes: Some(
                "Recommended local multi-voice TTS option for Chinese and English playback. Supports progressive playback through the bridge streaming endpoint.",
            ),
            sample_rate_hz: Some(24_000),
            default_voice: Some("0"),
            voice_count_hint: Some(103),
            required_files: &[
                "model.int8.onnx",
                "voices.bin",
                "tokens.txt",
                "espeak-ng-data",
                "lexicon-us-en.txt",
                "lexicon-zh.txt",
            ],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "silero-vad",
            kind: SpeechModelKind::Vad,
            display_name: "Silero VAD",
            description: "Voice activity detection for realtime endpointing.",
            languages: &["universal"],
            runtime: SpeechRuntime::Streaming,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: false,
                batch_asr: false,
                speech_synthesis: false,
                vad: true,
                speaker_embedding: false,
                wake_word: false,
                endpointing: true,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &["vad", "endpointing", "realtime"],
            supports_profiles: &[SpeechProfile::VadDefault],
            recommended_profiles: &[SpeechProfile::VadDefault],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/silero_vad.onnx",
            docs_url: Some("https://k2-fsa.github.io/sherpa/onnx/vad/index.html"),
            download_size_mb: Some(3),
            memory_hint: Some("Negligible CPU and memory overhead."),
            notes: Some("Use with realtime ASR to detect speech start and stop events."),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["silero_vad.onnx"],
            download_layout: SpeechDownloadLayout::SingleFile {
                filename: "silero_vad.onnx",
            },
        },
        SpeechModelCatalogEntry {
            id: "kws-zipformer-zh-en-3m",
            kind: SpeechModelKind::WakeWord,
            display_name: "KWS Zipformer ZH-EN 3M",
            description:
                "Streaming Chinese+English keyword spotting with phone-based English tokens.",
            languages: &["zh", "en"],
            runtime: SpeechRuntime::Streaming,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: true,
                realtime_asr: false,
                batch_asr: false,
                speech_synthesis: false,
                vad: false,
                speaker_embedding: false,
                wake_word: true,
                endpointing: false,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &[
                "kws",
                "wake-word",
                "streaming",
                "zipformer",
                "cpu-friendly",
                "zh-en",
            ],
            supports_profiles: &[SpeechProfile::WakeWordDefault],
            recommended_profiles: &[SpeechProfile::WakeWordDefault],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/kws-models/sherpa-onnx-kws-zipformer-zh-en-3M-2025-12-20.tar.bz2",
            docs_url: Some("https://k2-fsa.github.io/sherpa/onnx/kws/pretrained_models/index.html"),
            download_size_mb: Some(38),
            memory_hint: Some(
                "Small streaming model suitable for always-on local wake word detection.",
            ),
            notes: Some(
                "Latest KWS model supporting both Chinese and English. \
                 English wake words are tokenized as IPA phone symbols. \
                 Chinese wake words use numbered pinyin (e.g. xiao3 ou1).",
            ),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["tokens.txt"],
            download_layout: SpeechDownloadLayout::ArchiveTarBz2,
        },
        SpeechModelCatalogEntry {
            id: "3dspeaker-speech-eres2net-base",
            kind: SpeechModelKind::Speaker,
            display_name: "3D-Speaker ERes2Net Base",
            description: "Speaker embedding model for local speaker verification.",
            languages: &["universal"],
            runtime: SpeechRuntime::Offline,
            backend: SpeechComputeBackend::Onnx,
            capabilities: SpeechModelCapabilities {
                streaming: false,
                realtime_asr: false,
                batch_asr: false,
                speech_synthesis: false,
                vad: false,
                speaker_embedding: true,
                wake_word: false,
                endpointing: false,
                punctuation: false,
                inverse_text_normalization: false,
                multilingual: true,
            },
            features: &["speaker", "embedding", "verification"],
            supports_profiles: &[],
            recommended_profiles: &[],
            download_url: "https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-recongition-models/3dspeaker_speech_eres2net_base_sv_zh-cn_3dspeaker_16k.onnx",
            docs_url: Some(
                "https://k2-fsa.github.io/sherpa/onnx/speaker-identification/index.html",
            ),
            download_size_mb: Some(25),
            memory_hint: Some("Small CPU speaker embedding model for local voiceprint matching."),
            notes: Some("Used to enroll a target speaker and filter ASR to that speaker."),
            sample_rate_hz: Some(16_000),
            default_voice: None,
            voice_count_hint: None,
            required_files: &["3dspeaker_speech_eres2net_base_sv_zh-cn_3dspeaker_16k.onnx"],
            download_layout: SpeechDownloadLayout::SingleFile {
                filename: "3dspeaker_speech_eres2net_base_sv_zh-cn_3dspeaker_16k.onnx",
            },
        },
    ]
}

async fn scan_installed_models(root_dir: &Path) -> HashMap<String, InstalledSpeechModel> {
    let mut installed = HashMap::new();
    for entry in catalog_entries() {
        let install_dir = root_dir.join(entry.id);
        if verify_required_files(&install_dir, entry.required_files)
            .await
            .is_ok()
        {
            installed.insert(
                entry.id.to_string(),
                InstalledSpeechModel {
                    install_path: install_dir,
                },
            );
        }
    }
    installed
}

fn runtime_model_details(
    entry: &SpeechModelCatalogEntry,
    installed_model: Option<&InstalledSpeechModel>,
) -> (
    Option<u32>,
    Option<String>,
    Vec<String>,
    Vec<SpeechVoiceSummary>,
) {
    if entry.kind != SpeechModelKind::Tts {
        return (entry.sample_rate_hz, None, Vec::new(), Vec::new());
    }

    if let Some(installed_model) = installed_model {
        if let Ok(metadata) = tts::inspect_model(&installed_model.install_path) {
            let num_speakers = metadata.num_speakers.max(1);
            let voices = (0..num_speakers)
                .map(|index| index.to_string())
                .collect::<Vec<_>>();
            return (
                Some(metadata.sample_rate_hz),
                Some("0".to_string()),
                voices.clone(),
                voice_details_for_model(entry.id, num_speakers),
            );
        }
    }

    let voices = (0..entry.voice_count_hint.unwrap_or(0))
        .map(|index| index.to_string())
        .collect::<Vec<_>>();
    let count = entry.voice_count_hint.unwrap_or(0) as usize;
    (
        entry.sample_rate_hz,
        entry.default_voice.map(ToString::to_string),
        voices,
        voice_details_for_model(entry.id, count),
    )
}

fn voice_details_for_model(model_id: &str, count: usize) -> Vec<SpeechVoiceSummary> {
    match model_id {
        "vits-melo-tts-zh-en" => vec![SpeechVoiceSummary {
            id: "0".to_string(),
            name: "MeloTTS Chinese-English Female".to_string(),
            language: "zh/en".to_string(),
            accent: Some("Chinese + English".to_string()),
            gender: Some("female".to_string()),
        }],
        "kokoro-int8-multi-lang-v1_1" => kokoro_voice_names()
            .iter()
            .enumerate()
            .take(count)
            .map(|(index, name)| kokoro_voice_summary(index, name))
            .collect(),
        _ => (0..count)
            .map(|index| SpeechVoiceSummary {
                id: index.to_string(),
                name: format!("Voice {index}"),
                language: "unknown".to_string(),
                accent: None,
                gender: None,
            })
            .collect(),
    }
}

fn kokoro_voice_summary(index: usize, name: &str) -> SpeechVoiceSummary {
    let (language, accent, gender) = match name.get(0..2).unwrap_or_default() {
        "af" => ("en", Some("American English"), Some("female")),
        "am" => ("en", Some("American English"), Some("male")),
        "bf" => ("en", Some("British English"), Some("female")),
        "bm" => ("en", Some("British English"), Some("male")),
        "ef" => ("es", Some("Spanish"), Some("female")),
        "em" => ("es", Some("Spanish"), Some("male")),
        "ff" => ("fr", Some("French"), Some("female")),
        "hf" => ("hi", Some("Hindi"), Some("female")),
        "hm" => ("hi", Some("Hindi"), Some("male")),
        "if" => ("it", Some("Italian"), Some("female")),
        "im" => ("it", Some("Italian"), Some("male")),
        "jf" => ("ja", Some("Japanese"), Some("female")),
        "jm" => ("ja", Some("Japanese"), Some("male")),
        "pf" => ("pt-br", Some("Brazilian Portuguese"), Some("female")),
        "pm" => ("pt-br", Some("Brazilian Portuguese"), Some("male")),
        "zf" => ("zh", Some("Chinese"), Some("female")),
        "zm" => ("zh", Some("Chinese"), Some("male")),
        _ => ("unknown", None, None),
    };
    SpeechVoiceSummary {
        id: index.to_string(),
        name: readable_voice_name(name),
        language: language.to_string(),
        accent: accent.map(ToString::to_string),
        gender: gender.map(ToString::to_string),
    }
}

fn readable_voice_name(id: &str) -> String {
    id.split('_')
        .enumerate()
        .filter(|(index, part)| !(*index == 0 && part.len() == 2))
        .map(|(_, part)| part)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => {
                    let mut value = first.to_uppercase().collect::<String>();
                    value.push_str(chars.as_str());
                    value
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn kokoro_voice_names() -> &'static [&'static str] {
    &[
        "af_heart",
        "af_alloy",
        "af_aoede",
        "af_bella",
        "af_jessica",
        "af_kore",
        "af_nicole",
        "af_nova",
        "af_river",
        "af_sarah",
        "af_sky",
        "am_adam",
        "am_echo",
        "am_eric",
        "am_fenrir",
        "am_liam",
        "am_michael",
        "am_onyx",
        "am_puck",
        "am_santa",
        "bf_alice",
        "bf_emma",
        "bf_isabella",
        "bf_lily",
        "bm_daniel",
        "bm_fable",
        "bm_george",
        "bm_lewis",
        "ef_dora",
        "em_alex",
        "em_santa",
        "ff_siwis",
        "hf_alpha",
        "hf_beta",
        "hm_omega",
        "hm_psi",
        "if_sara",
        "im_nicola",
        "jf_alpha",
        "jf_gongitsune",
        "jf_nezumi",
        "jf_tebukuro",
        "jm_kumo",
        "pf_dora",
        "pm_alex",
        "pm_santa",
        "zf_xiaobei",
        "zf_xiaoni",
        "zf_xiaoxiao",
        "zf_xiaoyi",
        "zm_yunjian",
        "zm_yunxi",
        "zm_yunxia",
        "zm_yunyang",
    ]
}

async fn verify_required_files(dir: &Path, required_files: &[&str]) -> Result<()> {
    for name in required_files {
        let file = dir.join(name);
        if !tokio::fs::try_exists(&file).await.unwrap_or(false) {
            bail!("missing required file {}", file.display());
        }
    }
    Ok(())
}

async fn detect_model_root(root: &Path, required_files: &[&str]) -> Option<PathBuf> {
    if verify_required_files(root, required_files).await.is_ok() {
        return Some(root.to_path_buf());
    }

    let mut entries = tokio::fs::read_dir(root).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if verify_required_files(&path, required_files).await.is_ok() {
            return Some(path);
        }
    }
    None
}

async fn load_downloads(root_dir: &Path) -> HashMap<String, SpeechDownloadTask> {
    let path = downloads_state_path(root_dir);
    match tokio::fs::read_to_string(path).await {
        Ok(body) => serde_json::from_str(&body).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

fn speech_root_dir() -> PathBuf {
    std::env::var("ECHO_MATE_SPEECH_MODELS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            default_home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".omni-code")
                .join("speech")
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

fn downloads_state_path(root_dir: &Path) -> PathBuf {
    root_dir.join("downloads.json")
}

async fn extract_archive(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let archive = archive_path.to_path_buf();
    let output = output_dir.to_path_buf();
    tokio::task::spawn_blocking(move || extract_archive_blocking(&archive, &output))
        .await
        .context("speech archive extraction task failed")?
}

fn extract_archive_blocking(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let file = std::fs::File::open(archive_path)
        .with_context(|| format!("failed to open {}", archive_path.display()))?;
    let decompressed = bzip2::read::BzDecoder::new(file);
    let mut archive = tar::Archive::new(decompressed);
    archive
        .unpack(output_dir)
        .with_context(|| format!("failed to unpack {}", archive_path.display()))
}
