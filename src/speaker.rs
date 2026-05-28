use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use hound::{SampleFormat, WavSpec, WavWriter};
use serde::{Deserialize, Serialize};
use sherpa_onnx::{SpeakerEmbeddingExtractor, SpeakerEmbeddingExtractorConfig};
use uuid::Uuid;

use crate::{
    asr::{TARGET_SAMPLE_RATE, decode_audio_to_mono_f32, resample_if_needed, speech_segments},
    models::{SpeakerEnrollmentResult, SpeakerFilterSettings, SpeakerRecord, SpeechModelKind},
    speech::SpeechService,
};

const DEFAULT_SPEAKER_MODEL_ID: &str = "3dspeaker-speech-eres2net-base";
const DEFAULT_SPEAKER_THRESHOLD: f32 = 0.65;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSpeaker {
    id: String,
    name: String,
    embedding_model_id: String,
    embeddings: Vec<Vec<f32>>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

pub fn normalize_speaker_filter(mut settings: SpeakerFilterSettings) -> SpeakerFilterSettings {
    settings.speaker_id = settings
        .speaker_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    settings.threshold = settings
        .threshold
        .filter(|value| value.is_finite() && *value > 0.0 && *value <= 1.0);
    if settings.speaker_id.is_none() {
        settings.enabled = false;
    }
    settings
}

pub async fn list_speakers() -> Result<Vec<SpeakerRecord>> {
    let mut records = Vec::new();
    let dir = speakers_dir();
    if !dir.exists() {
        return Ok(records);
    }
    for entry in fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let speaker = read_stored_speaker(&entry.path())?;
        records.push(record_from_stored(&speaker));
    }
    records.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(records)
}

pub async fn delete_speaker(speaker_id: &str) -> Result<()> {
    let path = speaker_path(speaker_id);
    if path.exists() {
        fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

pub async fn enroll_speaker(
    speech: Arc<SpeechService>,
    name: String,
    bytes: Vec<u8>,
    filename: String,
    content_type: Option<String>,
) -> Result<SpeakerEnrollmentResult> {
    let name = name.trim();
    if name.is_empty() {
        bail!("speaker name is required");
    }
    let (embedding_model_id, extractor) = create_extractor(speech).await?;
    let decoded = decode_audio_to_mono_f32(&bytes, &filename, content_type.as_deref())
        .map_err(|error| anyhow::anyhow!("failed to decode speaker audio: {error}"))?;
    let samples = resample_if_needed(decoded.samples, decoded.sample_rate, TARGET_SAMPLE_RATE);
    let duration_secs = samples.len() as f32 / TARGET_SAMPLE_RATE as f32;
    let embeddings = embeddings_for_samples(&extractor, &samples)?;
    if embeddings.is_empty() {
        bail!("speaker sample is too short or contains no usable speech");
    }

    let now = Utc::now();
    let speaker = StoredSpeaker {
        id: Uuid::new_v4().to_string(),
        name: name.to_string(),
        embedding_model_id,
        embeddings,
        created_at: now,
        updated_at: now,
    };
    fs::create_dir_all(speakers_dir()).context("failed to create speakers directory")?;
    let body = serde_json::to_string_pretty(&speaker).context("failed to encode speaker")?;
    fs::write(speaker_path(&speaker.id), body).context("failed to write speaker profile")?;
    Ok(SpeakerEnrollmentResult {
        speaker: record_from_stored(&speaker),
        sample_duration_secs: duration_secs,
    })
}

pub async fn should_accept_speaker_segment(
    speech: Arc<SpeechService>,
    settings: &SpeakerFilterSettings,
    samples: &[f32],
) -> Result<bool> {
    let settings = normalize_speaker_filter(settings.clone());
    if !settings.enabled {
        return Ok(true);
    }
    let Some(speaker_id) = settings.speaker_id.as_deref() else {
        return Ok(true);
    };
    let speaker = read_stored_speaker(&speaker_path(speaker_id))?;
    let (_model_id, extractor) = create_extractor(speech).await?;
    let Some(embedding) = embedding_for_samples(&extractor, samples)? else {
        return Ok(false);
    };
    Ok(verify_embedding(
        &speaker,
        &embedding,
        settings.threshold.unwrap_or(DEFAULT_SPEAKER_THRESHOLD),
    ))
}

pub struct SpeakerVerifier {
    speaker: StoredSpeaker,
    extractor: SpeakerEmbeddingExtractor,
    threshold: f32,
}

impl SpeakerVerifier {
    pub fn match_score_for_samples(&self, samples: &[f32]) -> Result<Option<f32>> {
        let Some(embedding) = embedding_for_samples(&self.extractor, samples)? else {
            return Ok(None);
        };
        Ok(Some(best_similarity(&self.speaker, &embedding)))
    }

    pub fn accepts_score(&self, score: f32) -> bool {
        score >= self.threshold
    }
}

pub async fn create_speaker_verifier(
    speech: Arc<SpeechService>,
    settings: &SpeakerFilterSettings,
) -> Result<Option<SpeakerVerifier>> {
    let settings = normalize_speaker_filter(settings.clone());
    if !settings.enabled {
        return Ok(None);
    }
    let Some(speaker_id) = settings.speaker_id.as_deref() else {
        return Ok(None);
    };
    let speaker = read_stored_speaker(&speaker_path(speaker_id))?;
    let (_model_id, extractor) = create_extractor(speech).await?;
    Ok(Some(SpeakerVerifier {
        speaker,
        extractor,
        threshold: settings.threshold.unwrap_or(DEFAULT_SPEAKER_THRESHOLD),
    }))
}

fn embeddings_for_samples(
    extractor: &SpeakerEmbeddingExtractor,
    samples: &[f32],
) -> Result<Vec<Vec<f32>>> {
    let segments = speech_segments(samples, TARGET_SAMPLE_RATE, None);
    let mut embeddings = Vec::new();
    for segment in segments {
        if let Some(embedding) =
            embedding_for_samples(extractor, &samples[segment.start..segment.end])?
        {
            embeddings.push(embedding);
        }
    }
    if embeddings.is_empty() {
        if let Some(embedding) = embedding_for_samples(extractor, samples)? {
            embeddings.push(embedding);
        }
    }
    Ok(embeddings)
}

fn embedding_for_samples(
    extractor: &SpeakerEmbeddingExtractor,
    samples: &[f32],
) -> Result<Option<Vec<f32>>> {
    if samples.is_empty() {
        return Ok(None);
    }
    let stream = extractor
        .create_stream()
        .ok_or_else(|| anyhow::anyhow!("failed to create speaker embedding stream"))?;
    stream.accept_waveform(TARGET_SAMPLE_RATE as i32, samples);
    if !extractor.is_ready(&stream) {
        return Ok(None);
    }
    Ok(extractor.compute(&stream))
}

async fn create_extractor(
    speech: Arc<SpeechService>,
) -> Result<(String, SpeakerEmbeddingExtractor)> {
    let model = speech
        .resolve_model_by_id(DEFAULT_SPEAKER_MODEL_ID)
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    if speech.model_kind(DEFAULT_SPEAKER_MODEL_ID) != Some(SpeechModelKind::Speaker) {
        bail!("{DEFAULT_SPEAKER_MODEL_ID} is not a speaker embedding model");
    }
    let model_path = model
        .install_path
        .join("3dspeaker_speech_eres2net_base_sv_zh-cn_3dspeaker_16k.onnx");
    let extractor = SpeakerEmbeddingExtractor::create(&SpeakerEmbeddingExtractorConfig {
        model: Some(model_path.display().to_string()),
        num_threads: 1,
        debug: false,
        provider: Some("cpu".to_string()),
    })
    .ok_or_else(|| anyhow::anyhow!("failed to create speaker embedding extractor"))?;
    Ok((DEFAULT_SPEAKER_MODEL_ID.to_string(), extractor))
}

fn verify_embedding(speaker: &StoredSpeaker, embedding: &[f32], threshold: f32) -> bool {
    best_similarity(speaker, embedding) >= threshold
}

fn best_similarity(speaker: &StoredSpeaker, embedding: &[f32]) -> f32 {
    speaker
        .embeddings
        .iter()
        .map(|reference| cosine_similarity(reference, embedding))
        .fold(f32::NEG_INFINITY, f32::max)
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for (left, right) in left.iter().zip(right.iter()) {
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

fn read_stored_speaker(path: &Path) -> Result<StoredSpeaker> {
    let body =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("failed to parse {}", path.display()))
}

fn record_from_stored(speaker: &StoredSpeaker) -> SpeakerRecord {
    SpeakerRecord {
        id: speaker.id.clone(),
        name: speaker.name.clone(),
        embedding_model_id: speaker.embedding_model_id.clone(),
        embedding_count: speaker.embeddings.len(),
        created_at: speaker.created_at,
        updated_at: speaker.updated_at,
    }
}

fn speaker_path(speaker_id: &str) -> PathBuf {
    speakers_dir().join(format!("{speaker_id}.json"))
}

fn speakers_dir() -> PathBuf {
    crate::bridge_settings::settings_path()
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("speakers")
}

#[allow(dead_code)]
pub fn write_reference_wav(path: &Path, samples: &[f32]) -> Result<()> {
    let spec = WavSpec {
        channels: 1,
        sample_rate: TARGET_SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: SampleFormat::Int,
    };
    let mut writer = WavWriter::create(path, spec)?;
    for sample in samples {
        writer.write_sample((sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16)?;
    }
    writer.finalize()?;
    Ok(())
}
