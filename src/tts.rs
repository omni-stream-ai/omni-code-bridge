use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    io::Cursor,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::{Context, Result, bail};
use axum::body::{Body, Bytes};
use hound::{SampleFormat, WavSpec, WavWriter};
use sherpa_onnx::{
    GenerationConfig, OfflineTts, OfflineTtsConfig, OfflineTtsKokoroModelConfig,
    OfflineTtsModelConfig, OfflineTtsVitsModelConfig,
};
use tokio::sync::mpsc;

use crate::speech::SpeechService;

type SharedTts = Arc<Mutex<OfflineTts>>;

static TTS_ENGINE_CACHE: OnceLock<Mutex<HashMap<TtsCacheKey, SharedTts>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TtsCacheKey {
    model_dir: PathBuf,
    speed_bits: u32,
}

#[derive(Debug, Clone)]
pub struct TtsModelMetadata {
    pub sample_rate_hz: u32,
    pub num_speakers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalTtsFamily {
    Vits,
    Kokoro,
}

pub async fn synthesize_speech(
    speech: Arc<SpeechService>,
    model_id: &str,
    input: String,
    voice: Option<String>,
    speed: Option<f32>,
    response_format: Option<String>,
) -> Result<(Vec<u8>, String), String> {
    let input = sanitize_tts_input(&input);
    if input.is_empty() {
        return Err("TTS input contains no speakable text".to_string());
    }
    let model_dir = speech
        .installed_model_path(model_id)
        .await
        .ok_or_else(|| format!("model {model_id} is not installed"))?;

    let format = response_format.unwrap_or_else(|| "wav".to_string());
    if format != "wav" {
        return Err(format!(
            "unsupported response_format {format}; local TTS currently supports wav only"
        ));
    }

    let tts = cached_tts(&model_dir, speed)
        .map_err(|error| format!("failed to create OfflineTts: {error}"))?;
    let requested_speaker_id = parse_speaker_id(voice.as_deref())?;
    let tts = tts
        .lock()
        .map_err(|_| "failed to lock local TTS engine".to_string())?;
    let speaker_id = resolve_speaker_id(requested_speaker_id, tts.num_speakers());

    let generated = tts
        .generate_with_config(
            &input,
            &GenerationConfig {
                speed: speed.unwrap_or(1.0),
                sid: speaker_id,
                ..Default::default()
            },
            None::<fn(&[f32], f32) -> bool>,
        )
        .ok_or_else(|| "failed to generate local TTS audio".to_string())?;

    let wav = encode_wav_pcm16(generated.sample_rate() as u32, generated.samples())
        .map_err(|error| format!("failed to encode wav output: {error}"))?;
    Ok((wav, "audio/wav".to_string()))
}

pub fn inspect_model(model_dir: &Path) -> Result<TtsModelMetadata> {
    let tts = create_tts(model_dir, None).context("failed to create OfflineTts")?;
    Ok(TtsModelMetadata {
        sample_rate_hz: tts.sample_rate() as u32,
        num_speakers: tts.num_speakers().max(0) as usize,
    })
}

pub async fn synthesize_speech_stream(
    speech: Arc<SpeechService>,
    model_id: &str,
    input: String,
    voice: Option<String>,
    speed: Option<f32>,
    response_format: Option<String>,
) -> Result<(Body, String), String> {
    let input = sanitize_tts_input(&input);
    if input.is_empty() {
        return Err("TTS input contains no speakable text".to_string());
    }
    let model_dir = speech
        .installed_model_path(model_id)
        .await
        .ok_or_else(|| format!("model {model_id} is not installed"))?;

    let format = response_format.unwrap_or_else(|| "wav".to_string());
    if format != "wav" {
        return Err(format!(
            "unsupported response_format {format}; local TTS currently supports wav only"
        ));
    }

    let speaker_id = parse_speaker_id(voice.as_deref())?;
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(16);
    std::thread::spawn(move || {
        let _ = stream_tts_wav(&model_dir, input, speaker_id, speed, tx);
    });

    Ok((
        Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
        "audio/wav".to_string(),
    ))
}

fn stream_tts_wav(
    model_dir: &Path,
    input: String,
    speaker_id: i32,
    speed: Option<f32>,
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let sample_rate_hint = tts_sample_rate_hint(model_dir);
    let header_sent = if let Some(sample_rate) = sample_rate_hint {
        tx.blocking_send(Ok::<Bytes, std::io::Error>(Bytes::from(
            wav_header_placeholder(sample_rate),
        )))
        .is_ok()
    } else {
        false
    };
    if !header_sent && sample_rate_hint.is_some() {
        return Ok(());
    }

    let tts = cached_tts(model_dir, speed).context("failed to create OfflineTts")?;
    let tts = tts
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock local TTS engine"))?;
    let speaker_id = resolve_speaker_id(speaker_id, tts.num_speakers());
    if !header_sent
        && tx
            .blocking_send(Ok::<Bytes, std::io::Error>(Bytes::from(
                wav_header_placeholder(tts.sample_rate() as u32),
            )))
            .is_err()
    {
        return Ok(());
    }

    for chunk_input in split_tts_stream_chunks(&input) {
        let Some(generated) = tts.generate_with_config(
            &chunk_input,
            &GenerationConfig {
                speed: speed.unwrap_or(1.0),
                sid: speaker_id,
                ..Default::default()
            },
            None::<fn(&[f32], f32) -> bool>,
        ) else {
            let error = std::io::Error::other("failed to generate local TTS audio");
            let _ = tx.blocking_send(Err(error));
            return Ok(());
        };
        let samples = generated.samples();
        if samples.is_empty() {
            continue;
        }
        let chunk = encode_pcm16_bytes(samples);
        if tx
            .blocking_send(Ok::<Bytes, std::io::Error>(Bytes::from(chunk)))
            .is_err()
        {
            return Ok(());
        }
    }

    Ok(())
}

fn split_tts_stream_chunks(input: &str) -> Vec<String> {
    const SOFT_MIN_CHARS: usize = 6;
    const SOFT_MAX_CHARS: usize = 80;
    const HARD_MAX_CHARS: usize = 120;

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    let mut last_soft_break: Option<usize> = None;

    for character in input.chars() {
        current.push(character);
        current_chars += 1;

        if character.is_whitespace() {
            last_soft_break = Some(current.len());
        }

        let punctuation_break = is_tts_chunk_break(character) && current_chars >= SOFT_MIN_CHARS;
        let soft_length_break = current_chars >= SOFT_MAX_CHARS && last_soft_break.is_some();
        let hard_length_break = current_chars >= HARD_MAX_CHARS;

        if punctuation_break || soft_length_break || hard_length_break {
            let split_at = if soft_length_break {
                last_soft_break.unwrap_or(current.len())
            } else {
                current.len()
            };
            let remaining = current.split_off(split_at);
            push_tts_chunk(&mut chunks, &current);
            current = remaining;
            current_chars = current.chars().count();
            last_soft_break = current
                .char_indices()
                .filter_map(|(index, character)| character.is_whitespace().then_some(index + 1))
                .last();
        }
    }

    push_tts_chunk(&mut chunks, &current);
    chunks
}

fn push_tts_chunk(chunks: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        chunks.push(trimmed.to_string());
    }
}

fn is_tts_chunk_break(character: char) -> bool {
    matches!(
        character,
        '。' | '！' | '？' | '；' | '，' | '.' | '!' | '?' | ';' | ',' | '\n' | '\r'
    )
}

fn create_tts(model_dir: &Path, speed: Option<f32>) -> Result<OfflineTts> {
    match detect_tts_family(model_dir) {
        Some(LocalTtsFamily::Vits) => create_vits_tts(model_dir, speed)
            .ok_or_else(|| anyhow::anyhow!("unsupported or invalid VITS model directory")),
        Some(LocalTtsFamily::Kokoro) => create_kokoro_tts(model_dir, speed)
            .ok_or_else(|| anyhow::anyhow!("unsupported or invalid Kokoro model directory")),
        None => bail!(
            "unsupported local TTS model layout in {}",
            model_dir.display()
        ),
    }
}

fn cached_tts(model_dir: &Path, speed: Option<f32>) -> Result<SharedTts> {
    let key = TtsCacheKey {
        model_dir: model_dir.to_path_buf(),
        speed_bits: speed.unwrap_or(1.0).to_bits(),
    };
    let cache = TTS_ENGINE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock local TTS cache"))?;
    if let Some(tts) = cache.get(&key) {
        return Ok(Arc::clone(tts));
    }

    let tts = Arc::new(Mutex::new(create_tts(model_dir, speed)?));
    cache.insert(key, Arc::clone(&tts));
    Ok(tts)
}

fn tts_sample_rate_hint(model_dir: &Path) -> Option<u32> {
    match detect_tts_family(model_dir) {
        Some(LocalTtsFamily::Vits) => Some(44_100),
        Some(LocalTtsFamily::Kokoro) => Some(24_000),
        None => None,
    }
}

fn sanitize_tts_input(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut previous_was_whitespace = false;

    for character in input.chars() {
        if is_emoji_like(character) || should_skip_tts_character(character) {
            continue;
        }
        let normalized = normalize_tts_character(character);
        if normalized.is_whitespace() {
            if !previous_was_whitespace && !output.is_empty() {
                output.push(' ');
                previous_was_whitespace = true;
            }
            continue;
        }
        output.push(normalized);
        previous_was_whitespace = false;
    }

    output.trim().to_string()
}

fn normalize_tts_character(character: char) -> char {
    match character {
        ':' | '\u{FF1A}' => ',',
        _ => character,
    }
}

fn should_skip_tts_character(character: char) -> bool {
    matches!(
        character,
        '"' | '\''
            | '`'
            | '>'
            | '\u{2018}'
            | '\u{2019}'
            | '\u{201A}'
            | '\u{201B}'
            | '\u{201C}'
            | '\u{201D}'
            | '\u{201E}'
            | '\u{201F}'
            | '\u{300C}'
            | '\u{300D}'
            | '\u{300E}'
            | '\u{300F}'
            | '\u{301D}'
            | '\u{301E}'
            | '\u{301F}'
    )
}

fn is_emoji_like(character: char) -> bool {
    let value = character as u32;
    value == 0x00A9
        || value == 0x00AE
        || value == 0x200D
        || value == 0x203C
        || value == 0x2049
        || value == 0x2122
        || value == 0x2139
        || (0x2194..=0x21AA).contains(&value)
        || (0x231A..=0x231B).contains(&value)
        || value == 0x2328
        || value == 0x23CF
        || (0x23E9..=0x23F3).contains(&value)
        || (0x23F8..=0x23FA).contains(&value)
        || value == 0x24C2
        || (0x25AA..=0x25AB).contains(&value)
        || value == 0x25B6
        || value == 0x25C0
        || (0x25FB..=0x25FE).contains(&value)
        || (0x2600..=0x27BF).contains(&value)
        || (0x2934..=0x2935).contains(&value)
        || (0x2B05..=0x2B55).contains(&value)
        || value == 0x3030
        || value == 0x303D
        || value == 0x3297
        || value == 0x3299
        || (0xFE00..=0xFE0F).contains(&value)
        || (0x1F000..=0x1FAFF).contains(&value)
}

fn create_vits_tts(model_dir: &Path, speed: Option<f32>) -> Option<OfflineTts> {
    if !is_vits_model_dir(model_dir) {
        return None;
    }
    let model = model_dir.join("model.onnx");
    let lexicon = model_dir.join("lexicon.txt");
    let tokens = model_dir.join("tokens.txt");

    OfflineTts::create(&OfflineTtsConfig {
        model: OfflineTtsModelConfig {
            vits: OfflineTtsVitsModelConfig {
                model: Some(model.display().to_string()),
                lexicon: Some(lexicon.display().to_string()),
                tokens: Some(tokens.display().to_string()),
                data_dir: None,
                noise_scale: 0.667,
                noise_scale_w: 0.8,
                length_scale: 1.0 / speed.unwrap_or(1.0).max(0.25),
                dict_dir: detect_optional_dir(model_dir, "dict"),
            },
            num_threads: tts_num_threads(),
            debug: false,
            provider: Some("cpu".to_string()),
            ..Default::default()
        },
        max_num_sentences: 1,
        ..Default::default()
    })
}

fn create_kokoro_tts(model_dir: &Path, speed: Option<f32>) -> Option<OfflineTts> {
    if !is_kokoro_model_dir(model_dir) {
        return None;
    }
    let model = detect_existing_path(model_dir, &["model.int8.onnx", "model.onnx"])?;
    let voices = model_dir.join("voices.bin");
    let tokens = model_dir.join("tokens.txt");
    let data_dir = model_dir.join("espeak-ng-data");
    let lexicon_us = model_dir.join("lexicon-us-en.txt");
    let lexicon_zh = model_dir.join("lexicon-zh.txt");

    let rule_fsts =
        collect_optional_paths(model_dir, &["date-zh.fst", "phone-zh.fst", "number-zh.fst"]);
    let lexicon_us = sanitized_lexicon_path(&lexicon_us, &tokens).unwrap_or(lexicon_us);
    let lexicon_zh = sanitized_lexicon_path(&lexicon_zh, &tokens).unwrap_or(lexicon_zh);

    OfflineTts::create(&OfflineTtsConfig {
        model: OfflineTtsModelConfig {
            kokoro: OfflineTtsKokoroModelConfig {
                model: Some(model.display().to_string()),
                voices: Some(voices.display().to_string()),
                tokens: Some(tokens.display().to_string()),
                data_dir: Some(data_dir.display().to_string()),
                length_scale: 1.0 / speed.unwrap_or(1.0).max(0.25),
                dict_dir: detect_optional_dir(model_dir, "dict"),
                lexicon: Some(join_paths(&[lexicon_us, lexicon_zh])),
                lang: None,
            },
            num_threads: tts_num_threads(),
            debug: false,
            provider: Some("cpu".to_string()),
            ..Default::default()
        },
        max_num_sentences: 1,
        rule_fsts,
        ..Default::default()
    })
}

fn detect_tts_family(model_dir: &Path) -> Option<LocalTtsFamily> {
    if is_vits_model_dir(model_dir) {
        return Some(LocalTtsFamily::Vits);
    }
    if is_kokoro_model_dir(model_dir) {
        return Some(LocalTtsFamily::Kokoro);
    }
    None
}

fn tts_num_threads() -> i32 {
    std::thread::available_parallelism()
        .map(|value| value.get().clamp(2, 4) as i32)
        .unwrap_or(2)
}

fn is_vits_model_dir(model_dir: &Path) -> bool {
    model_dir.join("model.onnx").is_file()
        && model_dir.join("lexicon.txt").is_file()
        && model_dir.join("tokens.txt").is_file()
}

fn is_kokoro_model_dir(model_dir: &Path) -> bool {
    detect_existing_path(model_dir, &["model.int8.onnx", "model.onnx"]).is_some()
        && model_dir.join("voices.bin").is_file()
        && model_dir.join("tokens.txt").is_file()
        && model_dir.join("espeak-ng-data").is_dir()
        && model_dir.join("lexicon-us-en.txt").is_file()
        && model_dir.join("lexicon-zh.txt").is_file()
}

fn sanitized_lexicon_path(lexicon_path: &Path, tokens_path: &Path) -> Option<PathBuf> {
    let lexicon = fs::read_to_string(lexicon_path).ok()?;
    let tokens = fs::read_to_string(tokens_path).ok()?;
    let sanitized = sanitize_lexicon_content(&lexicon, &tokens);
    if sanitized == lexicon {
        return Some(lexicon_path.to_path_buf());
    }

    let output_dir = crate::bridge_settings::project_tmp_dir("speech-lexicons");
    fs::create_dir_all(&output_dir).ok()?;

    let mut hasher = DefaultHasher::new();
    lexicon_path.hash(&mut hasher);
    lexicon.hash(&mut hasher);
    tokens.hash(&mut hasher);
    let output_path = output_dir.join(format!("lexicon-{:016x}.txt", hasher.finish()));
    fs::write(&output_path, sanitized).ok()?;
    Some(output_path)
}

fn sanitize_lexicon_content(lexicon: &str, tokens: &str) -> String {
    let token_set = tokens
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect::<HashSet<_>>();
    let mut output = String::with_capacity(lexicon.len());

    for line in lexicon.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            output.push_str(line);
            output.push('\n');
            continue;
        }

        let mut parts = trimmed.split_whitespace();
        let Some(word) = parts.next() else {
            continue;
        };
        let kept_tokens = parts
            .filter(|token| token_set.contains(token))
            .collect::<Vec<_>>();
        if kept_tokens.is_empty() {
            continue;
        }
        output.push_str(word);
        for token in kept_tokens {
            output.push(' ');
            output.push_str(token);
        }
        output.push('\n');
    }

    output
}

fn parse_speaker_id(voice: Option<&str>) -> Result<i32, String> {
    let Some(value) = voice.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(0);
    };

    value
        .parse::<i32>()
        .map_err(|_| format!("voice must be a numeric speaker id; got '{value}'"))
}

fn resolve_speaker_id(requested: i32, num_speakers: i32) -> i32 {
    if requested >= 0 && requested < num_speakers.max(1) {
        return requested;
    }

    eprintln!(
        "requested TTS voice {requested} is outside supported speaker range [0, {}]; using voice 0",
        num_speakers.max(1) - 1
    );
    0
}

fn detect_existing_path(model_dir: &Path, names: &[&str]) -> Option<PathBuf> {
    names
        .iter()
        .map(|name| model_dir.join(name))
        .find(|path| path.is_file())
}

fn detect_optional_dir(model_dir: &Path, name: &str) -> Option<String> {
    let path = model_dir.join(name);
    path.is_dir().then(|| path.display().to_string())
}

fn collect_optional_paths(model_dir: &Path, names: &[&str]) -> Option<String> {
    let paths = names
        .iter()
        .map(|name| model_dir.join(name))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();

    (!paths.is_empty()).then(|| join_paths(&paths))
}

fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn encode_wav_pcm16(sample_rate: u32, samples: &[f32]) -> Result<Vec<u8>> {
    if samples.is_empty() {
        bail!("tts returned empty audio");
    }

    let spec = WavSpec {
        channels: 1,
        sample_rate,
        bits_per_sample: 16,
        sample_format: SampleFormat::Int,
    };
    let mut bytes = Vec::new();
    {
        let cursor = Cursor::new(&mut bytes);
        let mut writer = WavWriter::new(cursor, spec).context("failed to create wav writer")?;
        for sample in samples {
            let value = (sample.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
            writer
                .write_sample(value)
                .context("failed to write wav sample")?;
        }
        writer.finalize().context("failed to finalize wav writer")?;
    }
    Ok(bytes)
}

fn encode_pcm16_bytes(samples: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let value = (sample.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn wav_header_placeholder(sample_rate: u32) -> Vec<u8> {
    let byte_rate = sample_rate * 2;
    let block_align = 2u16;
    let bits_per_sample = 16u16;
    let data_size = u32::MAX - 36;
    let riff_size = data_size + 36;

    let mut bytes = Vec::with_capacity(44);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&riff_size.to_le_bytes());
    bytes.extend_from_slice(b"WAVE");
    bytes.extend_from_slice(b"fmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&byte_rate.to_le_bytes());
    bytes.extend_from_slice(&block_align.to_le_bytes());
    bytes.extend_from_slice(&bits_per_sample.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&data_size.to_le_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_tts_input_removes_emoji_and_normalizes_quotes() {
        assert_eq!(sanitize_tts_input("他说：“为什么❓”"), "他说,为什么");
    }

    #[test]
    fn sanitize_tts_input_collapses_whitespace_after_removed_emoji() {
        assert_eq!(sanitize_tts_input("Why   ❓   now"), "Why now");
    }

    #[test]
    fn sanitize_lexicon_content_removes_entries_with_unknown_tokens() {
        let lexicon = "一 ㄧ 1\n呣 ❓\n好 ㄏ ㄠ 3\n";
        let tokens = "ㄧ 1\n1 2\nㄏ 3\nㄠ 4\n3 5\n";

        assert_eq!(
            sanitize_lexicon_content(lexicon, tokens),
            "一 ㄧ 1\n好 ㄏ ㄠ 3\n"
        );
    }

    #[test]
    fn resolve_speaker_id_falls_back_when_voice_is_out_of_range() {
        assert_eq!(resolve_speaker_id(48, 1), 0);
        assert_eq!(resolve_speaker_id(-1, 1), 0);
        assert_eq!(resolve_speaker_id(2, 3), 2);
    }

    #[test]
    fn split_tts_stream_chunks_breaks_at_punctuation() {
        assert_eq!(
            split_tts_stream_chunks("第一句有声音，逗号后面也继续。最后一句。"),
            vec!["第一句有声音，", "逗号后面也继续。", "最后一句。"]
        );
    }

    #[test]
    fn split_tts_stream_chunks_keeps_short_phrases_together() {
        assert_eq!(
            split_tts_stream_chunks("你好，继续播放。"),
            vec!["你好，继续播放。"]
        );
    }
}
