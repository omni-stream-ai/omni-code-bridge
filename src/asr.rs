use std::{io::Cursor, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use sherpa_onnx::{
    OfflineModelConfig, OfflineRecognizer, OfflineRecognizerConfig, OfflineSenseVoiceModelConfig,
    OfflineStream,
};
use symphonia::{
    core::conv::IntoSample,
    core::{
        audio::{AudioBufferRef, Signal},
        codecs::DecoderOptions,
        errors::Error as SymphoniaError,
        formats::FormatOptions,
        io::MediaSourceStream,
        meta::MetadataOptions,
        probe::Hint,
    },
    default::{get_codecs, get_probe},
};

use crate::{
    models::{SpeakerFilterSettings, SpeechProfileSelection},
    speaker,
    speech::SpeechService,
    vad::create_silero_vad,
};

pub const TARGET_SAMPLE_RATE: u32 = 16_000;
const MIN_AUDIO_SECS_FOR_SEGMENTING: f32 = 6.0;
const VAD_FRAME_MS: u32 = 30;
const VAD_MIN_SILENCE_MS: u32 = 650;
const VAD_PADDING_MS: u32 = 180;
const VAD_MIN_SEGMENT_MS: u32 = 300;
const VAD_MAX_SEGMENT_MS: u32 = 25_000;

#[derive(Debug, Clone)]
pub struct TranscriptionResult {
    pub text: String,
    pub duration_secs: f32,
}

pub async fn transcribe_audio(
    speech: Arc<SpeechService>,
    profiles: SpeechProfileSelection,
    model_id: &str,
    bytes: Vec<u8>,
    filename: String,
    content_type: Option<String>,
    language: Option<String>,
    speaker_filter: SpeakerFilterSettings,
) -> Result<TranscriptionResult, String> {
    let model_dir = speech
        .installed_model_path(model_id)
        .await
        .ok_or_else(|| format!("model {model_id} is not installed"))?;

    let samples = decode_audio_to_mono_f32(&bytes, &filename, content_type.as_deref())
        .map_err(|error| format!("failed to decode input audio: {error}"))?;
    let samples = resample_if_needed(samples.samples, samples.sample_rate, TARGET_SAMPLE_RATE);
    let duration_secs = samples.len() as f32 / TARGET_SAMPLE_RATE as f32;
    let vad_model_dir = match profiles.vad_default.as_deref() {
        Some(model_id) => speech.installed_model_path(model_id).await,
        None => None,
    };

    let segments = speech_segments(&samples, TARGET_SAMPLE_RATE, vad_model_dir.as_deref());
    let segments = filter_speaker_segments(speech, &speaker_filter, &samples, segments).await?;

    transcribe_segments_with_sensevoice(
        &model_dir,
        &samples,
        language.as_deref().unwrap_or("auto"),
        &segments,
    )
    .map(|text| TranscriptionResult {
        text,
        duration_secs,
    })
    .map_err(|error| format!("failed to run local ASR: {error}"))
}

pub struct DecodedAudio {
    pub sample_rate: u32,
    pub samples: Vec<f32>,
}

async fn filter_speaker_segments(
    speech: Arc<SpeechService>,
    speaker_filter: &SpeakerFilterSettings,
    samples: &[f32],
    segments: Vec<SpeechSegment>,
) -> Result<Vec<SpeechSegment>, String> {
    let speaker_filter = speaker::normalize_speaker_filter(speaker_filter.clone());
    if !speaker_filter.enabled {
        return Ok(segments);
    }

    let mut accepted = Vec::new();
    for segment in segments {
        let segment_samples = &samples[segment.start..segment.end];
        let accept = speaker::should_accept_speaker_segment(
            Arc::clone(&speech),
            &speaker_filter,
            segment_samples,
        )
        .await
        .map_err(|error| format!("failed to verify speaker: {error}"))?;
        if accept {
            accepted.push(segment);
        }
    }

    if accepted.is_empty() {
        return Err("no speech from the selected speaker was recognized".to_string());
    }
    Ok(accepted)
}

fn transcribe_segments_with_sensevoice(
    model_dir: &Path,
    samples: &[f32],
    language: &str,
    segments: &[SpeechSegment],
) -> Result<String> {
    let recognizer = OfflineRecognizer::create(&OfflineRecognizerConfig {
        model_config: OfflineModelConfig {
            sense_voice: OfflineSenseVoiceModelConfig {
                model: Some(model_dir.join("model.int8.onnx").display().to_string()),
                language: Some(language.to_string()),
                use_itn: true,
            },
            tokens: Some(model_dir.join("tokens.txt").display().to_string()),
            num_threads: 2,
            provider: Some("cpu".to_string()),
            ..Default::default()
        },
        decoding_method: Some("greedy_search".to_string()),
        max_active_paths: 4,
        hotwords_file: None,
        hotwords_score: 1.5,
        rule_fsts: None,
        rule_fars: None,
        blank_penalty: 0.0,
        ..Default::default()
    })
    .context("failed to create OfflineRecognizer")?;

    if segments.len() <= 1 {
        let segment_samples = segments
            .first()
            .map(|segment| &samples[segment.start..segment.end])
            .unwrap_or(samples);
        let text = decode_sensevoice_segment(&recognizer, segment_samples)?
            .context("recognizer returned empty transcript")?;
        return Ok(text);
    }

    let mut texts = Vec::new();
    for segment in segments {
        let segment_samples = &samples[segment.start..segment.end];
        if let Some(text) = decode_sensevoice_segment(&recognizer, segment_samples)? {
            texts.push(text);
        }
    }

    let text = join_transcript_segments(&texts);
    if text.is_empty() {
        bail!("recognizer returned empty transcript");
    }
    Ok(text)
}

fn decode_sensevoice_segment(
    recognizer: &OfflineRecognizer,
    samples: &[f32],
) -> Result<Option<String>> {
    let mut stream = recognizer.create_stream();
    accept_waveform(&mut stream, TARGET_SAMPLE_RATE, samples);
    recognizer.decode(&stream);
    let result = stream
        .get_result()
        .context("recognizer did not return a result")?;
    let text = result.text.trim().to_string();
    Ok((!text.is_empty()).then_some(text))
}

fn accept_waveform(stream: &mut OfflineStream, sample_rate: u32, samples: &[f32]) {
    stream.accept_waveform(sample_rate as i32, samples);
}

pub fn decode_audio_to_mono_f32(
    bytes: &[u8],
    filename: &str,
    content_type: Option<&str>,
) -> Result<DecodedAudio> {
    if let Some(decoded) = try_decode_wav(bytes)? {
        return Ok(decoded);
    }

    let mut hint = Hint::new();
    if let Some(extension) = Path::new(filename).extension().and_then(|ext| ext.to_str()) {
        hint.with_extension(extension);
    } else if let Some(content_type) = content_type {
        if let Some(extension) = mime_extension(content_type) {
            hint.with_extension(extension);
        }
    }

    let cursor = Cursor::new(bytes.to_vec());
    let media_source = MediaSourceStream::new(Box::new(cursor), Default::default());
    let probed = get_probe().format(
        &hint,
        media_source,
        &FormatOptions::default(),
        &MetadataOptions::default(),
    )?;
    let mut format = probed.format;
    let track = format
        .default_track()
        .context("no supported audio track found")?;
    let track_id = track.id;
    let codec_params = track.codec_params.clone();
    let mut decoder = get_codecs().make(&codec_params, &DecoderOptions::default())?;
    let sample_rate = codec_params
        .sample_rate
        .context("input audio sample rate is missing")?;

    let mut output = Vec::new();
    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::ResetRequired) => {
                bail!("audio stream reset is not supported");
            }
            Err(error) => return Err(error.into()),
        };

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = decoder.decode(&packet)?;
        append_audio_buffer(&mut output, decoded);
    }

    Ok(DecodedAudio {
        sample_rate,
        samples: output,
    })
}

fn try_decode_wav(bytes: &[u8]) -> Result<Option<DecodedAudio>> {
    let cursor = Cursor::new(bytes);
    let reader = match hound::WavReader::new(cursor) {
        Ok(reader) => reader,
        Err(_) => return Ok(None),
    };

    let spec = reader.spec();
    let sample_rate = spec.sample_rate;
    let channels = spec.channels.max(1) as usize;
    let samples = match spec.sample_format {
        hound::SampleFormat::Float => {
            let data = reader
                .into_samples::<f32>()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to read wav float samples")?;
            downmix_interleaved(data, channels)
        }
        hound::SampleFormat::Int => {
            let bits = spec.bits_per_sample.max(1) as u32;
            let scale = ((1_i64 << (bits - 1)) - 1) as f32;
            let data = reader
                .into_samples::<i32>()
                .map(|sample| sample.map(|value| (value as f32 / scale).clamp(-1.0, 1.0)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to read wav integer samples")?;
            downmix_interleaved(data, channels)
        }
    };

    Ok(Some(DecodedAudio {
        sample_rate,
        samples,
    }))
}

fn append_audio_buffer(output: &mut Vec<f32>, buffer: AudioBufferRef<'_>) {
    match buffer {
        AudioBufferRef::U8(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::U16(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::U24(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::U32(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::S8(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::S16(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::S24(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::S32(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| IntoSample::<f32>::into_sample(buf.chan(ch)[frame]),
        ),
        AudioBufferRef::F32(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| buf.chan(ch)[frame],
        ),
        AudioBufferRef::F64(buf) => append_planes(
            output,
            buf.spec().channels.count(),
            buf.frames(),
            |ch, frame| buf.chan(ch)[frame] as f32,
        ),
    }
}

fn append_planes(
    output: &mut Vec<f32>,
    channels: usize,
    frames: usize,
    sample_at: impl Fn(usize, usize) -> f32,
) {
    let channels = channels.max(1);
    for frame in 0..frames {
        let mut mixed = 0.0_f32;
        for channel in 0..channels {
            mixed += sample_at(channel, frame);
        }
        output.push(mixed / channels as f32);
    }
}

fn downmix_interleaved(samples: Vec<f32>, channels: usize) -> Vec<f32> {
    if channels <= 1 {
        return samples;
    }

    samples
        .chunks(channels)
        .map(|frame| frame.iter().copied().sum::<f32>() / frame.len() as f32)
        .collect()
}

pub fn resample_if_needed(samples: Vec<f32>, from_rate: u32, to_rate: u32) -> Vec<f32> {
    if from_rate == to_rate || samples.is_empty() {
        return samples;
    }

    let ratio = to_rate as f64 / from_rate as f64;
    let new_len = ((samples.len() as f64) * ratio).round().max(1.0) as usize;
    let mut output = Vec::with_capacity(new_len);
    for index in 0..new_len {
        let source = index as f64 / ratio;
        let left = source.floor() as usize;
        let right = (left + 1).min(samples.len() - 1);
        let frac = (source - left as f64) as f32;
        let left_sample = samples[left];
        let right_sample = samples[right];
        output.push(left_sample + (right_sample - left_sample) * frac);
    }
    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpeechSegment {
    pub start: usize,
    pub end: usize,
}

pub fn speech_segments(
    samples: &[f32],
    sample_rate: u32,
    vad_model_dir: Option<&Path>,
) -> Vec<SpeechSegment> {
    let min_audio_samples = (MIN_AUDIO_SECS_FOR_SEGMENTING * sample_rate as f32) as usize;
    if samples.len() < min_audio_samples {
        return full_audio_segment(samples);
    }

    if let Some(segments) = vad_model_dir
        .and_then(|model_dir| speech_segments_with_silero_vad(samples, sample_rate, model_dir))
    {
        if segments.len() > 1 {
            return segments;
        }
    }

    speech_segments_with_energy_vad(samples, sample_rate)
}

fn speech_segments_with_silero_vad(
    samples: &[f32],
    sample_rate: u32,
    vad_model_dir: &Path,
) -> Option<Vec<SpeechSegment>> {
    let vad = create_silero_vad(vad_model_dir, sample_rate, VAD_MIN_SILENCE_MS)?;
    vad.accept_waveform(samples);
    vad.flush();

    let padding_samples = ms_to_samples(VAD_PADDING_MS, sample_rate);
    let min_segment_samples = ms_to_samples(VAD_MIN_SEGMENT_MS, sample_rate);
    let mut segments = Vec::new();
    while let Some(segment) = vad.front() {
        let start = segment.start().max(0) as usize;
        let end = start.saturating_add(segment.n().max(0) as usize);
        push_speech_segment(
            &mut segments,
            start,
            end,
            samples.len(),
            padding_samples,
            min_segment_samples,
        );
        vad.pop();
    }
    vad.reset();

    if segments.is_empty() {
        Some(full_audio_segment(samples))
    } else {
        Some(merge_close_segments(segments, padding_samples))
    }
}

fn speech_segments_with_energy_vad(samples: &[f32], sample_rate: u32) -> Vec<SpeechSegment> {
    let frame_len = ms_to_samples(VAD_FRAME_MS, sample_rate).max(1);
    let min_silence_frames = (VAD_MIN_SILENCE_MS as usize).div_ceil(VAD_FRAME_MS as usize);
    let min_segment_samples = ms_to_samples(VAD_MIN_SEGMENT_MS, sample_rate);
    let max_segment_samples = ms_to_samples(VAD_MAX_SEGMENT_MS, sample_rate);
    let padding_samples = ms_to_samples(VAD_PADDING_MS, sample_rate);
    let rms_values = frame_rms_values(samples, frame_len);
    if rms_values.is_empty() {
        return full_audio_segment(samples);
    }

    let threshold = vad_threshold(&rms_values);
    let mut segments = Vec::new();
    let mut active_start = None;
    let mut silence_frames = 0_usize;

    for (index, rms) in rms_values.iter().enumerate() {
        let frame_start = index * frame_len;
        let voiced = *rms >= threshold;

        if voiced {
            if active_start.is_none() {
                active_start = Some(frame_start);
            }
            silence_frames = 0;
        } else if let Some(start) = active_start {
            silence_frames += 1;
            let segment_len = frame_start.saturating_sub(start);
            if silence_frames >= min_silence_frames || segment_len >= max_segment_samples {
                let silence_samples = silence_frames * frame_len;
                let speech_end = frame_start
                    .saturating_sub(silence_samples)
                    .saturating_add(frame_len);
                push_speech_segment(
                    &mut segments,
                    start,
                    speech_end,
                    samples.len(),
                    padding_samples,
                    min_segment_samples,
                );
                active_start = None;
                silence_frames = 0;
            }
        }
    }

    if let Some(start) = active_start {
        push_speech_segment(
            &mut segments,
            start,
            samples.len(),
            samples.len(),
            padding_samples,
            min_segment_samples,
        );
    }

    if segments.len() <= 1 {
        full_audio_segment(samples)
    } else {
        merge_close_segments(segments, padding_samples)
    }
}

fn frame_rms_values(samples: &[f32], frame_len: usize) -> Vec<f32> {
    samples
        .chunks(frame_len)
        .map(|frame| {
            let sum = frame.iter().map(|sample| sample * sample).sum::<f32>();
            (sum / frame.len().max(1) as f32).sqrt()
        })
        .collect()
}

fn vad_threshold(rms_values: &[f32]) -> f32 {
    let mut sorted = rms_values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let noise_floor = sorted[sorted.len() / 5];
    let peak = sorted.last().copied().unwrap_or(0.0);
    (noise_floor * 3.0).max(peak * 0.08).max(0.008)
}

fn push_speech_segment(
    segments: &mut Vec<SpeechSegment>,
    start: usize,
    end: usize,
    sample_count: usize,
    padding_samples: usize,
    min_segment_samples: usize,
) {
    if end.saturating_sub(start) < min_segment_samples {
        return;
    }

    segments.push(SpeechSegment {
        start: start.saturating_sub(padding_samples),
        end: end.saturating_add(padding_samples).min(sample_count),
    });
}

fn merge_close_segments(
    mut segments: Vec<SpeechSegment>,
    max_gap_samples: usize,
) -> Vec<SpeechSegment> {
    if segments.is_empty() {
        return segments;
    }

    segments.sort_by_key(|segment| segment.start);
    let mut merged: Vec<SpeechSegment> = Vec::with_capacity(segments.len());
    for segment in segments {
        if let Some(last) = merged.last_mut() {
            if segment.start <= last.end.saturating_add(max_gap_samples) {
                last.end = last.end.max(segment.end);
                continue;
            }
        }
        merged.push(segment);
    }
    merged
}

fn full_audio_segment(samples: &[f32]) -> Vec<SpeechSegment> {
    if samples.is_empty() {
        Vec::new()
    } else {
        vec![SpeechSegment {
            start: 0,
            end: samples.len(),
        }]
    }
}

fn ms_to_samples(ms: u32, sample_rate: u32) -> usize {
    ((ms as u64 * sample_rate as u64) / 1000) as usize
}

fn join_transcript_segments(texts: &[String]) -> String {
    let mut output = String::new();
    for text in texts
        .iter()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
    {
        if output.is_empty() {
            output.push_str(text);
            continue;
        }

        let previous = output.chars().last();
        let next = text.chars().next();
        if needs_transcript_space(previous, next) {
            output.push(' ');
        }
        output.push_str(text);
    }
    output
}

fn needs_transcript_space(previous: Option<char>, next: Option<char>) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let Some(next) = next else {
        return false;
    };
    if previous.is_whitespace()
        || matches!(
            previous,
            '，' | '。' | '！' | '？' | ',' | '.' | '!' | '?' | ':' | ';'
        )
        || matches!(
            next,
            '，' | '。' | '！' | '？' | ',' | '.' | '!' | '?' | ':' | ';'
        )
    {
        return false;
    }
    !is_cjk(previous) && !is_cjk(next)
}

fn is_cjk(character: char) -> bool {
    matches!(
        character as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

fn mime_extension(content_type: &str) -> Option<&'static str> {
    match content_type.split(';').next()?.trim() {
        "audio/wav" | "audio/x-wav" => Some("wav"),
        "audio/mpeg" => Some("mp3"),
        "audio/flac" => Some("flac"),
        "audio/ogg" => Some("ogg"),
        "audio/mp4" | "audio/x-m4a" => Some("m4a"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{SpeechSegment, join_transcript_segments, speech_segments};

    #[test]
    fn speech_segments_keep_short_audio_whole() {
        let samples = vec![0.05; 16_000];
        assert_eq!(
            speech_segments(&samples, 16_000, None),
            vec![SpeechSegment {
                start: 0,
                end: samples.len()
            }]
        );
    }

    #[test]
    fn speech_segments_split_long_silence() {
        let mut samples = Vec::new();
        samples.extend(vec![0.0; 32_000]);
        samples.extend(vec![0.08; 16_000]);
        samples.extend(vec![0.0; 24_000]);
        samples.extend(vec![0.08; 16_000]);
        samples.extend(vec![0.0; 16_000]);

        let segments = speech_segments(&samples, 16_000, None);

        assert_eq!(segments.len(), 2);
        assert!(segments[0].start < 32_000);
        assert!(segments[0].end < 60_000);
        assert!(segments[1].start > 50_000);
        assert!(segments[1].end > 88_000);
    }

    #[test]
    fn join_transcript_segments_handles_cjk_and_english_spacing() {
        assert_eq!(
            join_transcript_segments(&["你好".to_string(), "世界".to_string()]),
            "你好世界"
        );
        assert_eq!(
            join_transcript_segments(&["run".to_string(), "cargo check".to_string()]),
            "run cargo check"
        );
        assert_eq!(
            join_transcript_segments(&["打开".to_string(), "GitHub".to_string()]),
            "打开GitHub"
        );
    }
}
