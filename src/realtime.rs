use std::{collections::HashSet, path::{Path, PathBuf}, sync::Arc};

use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use sherpa_onnx::{
    KeywordSpotter, KeywordSpotterConfig, OnlineModelConfig, OnlineParaformerModelConfig,
    OnlineRecognizer, OnlineRecognizerConfig, OnlineStream, OnlineTransducerModelConfig,
    RecognizerResult, VoiceActivityDetector,
};
use uuid::Uuid;

use crate::{
    app_state::AppState, models::SpeechProfile, speaker, speech::profile_slug,
    vad::create_silero_vad,
};

const AUDIO_FORMAT: &str = "pcm_s16le";
const SAMPLE_RATE_HZ: u32 = 16_000;
const DEFAULT_ENDPOINT_TRAILING_SILENCE_MS: u32 = 1_200;
const DEFAULT_VAD_MIN_SILENCE_MS: u32 = 800;
const DEFAULT_WAKE_WORDS: &[&str] = &["xiao3 ou1"];
const DEFAULT_WAKE_WORD_DETECTOR: WakeWordDetectorKind = WakeWordDetectorKind::LocalKws;

pub async fn descriptor(state: &AppState) -> Value {
    let settings = state.bridge_settings().await;
    let models = state
        .speech()
        .list_models(settings.speech_profiles.clone())
        .await;
    let selected_realtime_asr = settings.speech_profiles.asr_realtime.clone();
    let selected_vad = settings.speech_profiles.vad_default.clone();
    let selected_wake_word = settings.speech_profiles.wake_word_default.clone();
    let realtime_asr_ready = selected_realtime_asr
        .as_deref()
        .and_then(|selected| models.iter().find(|model| model.id == selected))
        .map(|model| model.installed && model.capabilities.realtime_asr)
        .unwrap_or(false);
    let vad_ready = selected_vad
        .as_deref()
        .and_then(|selected| models.iter().find(|model| model.id == selected))
        .map(|model| model.installed && model.capabilities.vad)
        .unwrap_or(false);
    let wake_word_ready = selected_wake_word
        .as_deref()
        .and_then(|selected| models.iter().find(|model| model.id == selected))
        .map(|model| model.installed && model.capabilities.wake_word)
        .unwrap_or(false);
    let missing_requirements = [
        (!realtime_asr_ready).then(|| profile_slug(SpeechProfile::AsrRealtime)),
        (!vad_ready).then(|| profile_slug(SpeechProfile::VadDefault)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let realtime_asr_models = models
        .iter()
        .filter(|model| model.capabilities.realtime_asr)
        .map(|model| {
            json!({
                "id": model.id,
                "display_name": model.display_name,
                "installed": model.installed,
                "languages": model.languages,
                "runtime": model.runtime,
                "sample_rate_hz": model.sample_rate_hz,
                "supports_profiles": model.supports_profiles,
            })
        })
        .collect::<Vec<_>>();

    let vad_models = models
        .iter()
        .filter(|model| model.capabilities.vad)
        .map(|model| {
            json!({
                "id": model.id,
                "display_name": model.display_name,
                "installed": model.installed,
                "languages": model.languages,
                "runtime": model.runtime,
                "sample_rate_hz": model.sample_rate_hz,
                "supports_profiles": model.supports_profiles,
            })
        })
        .collect::<Vec<_>>();

    let wake_word_models = models
        .iter()
        .filter(|model| model.capabilities.wake_word)
        .map(|model| {
            json!({
                "id": model.id,
                "display_name": model.display_name,
                "installed": model.installed,
                "languages": model.languages,
                "runtime": model.runtime,
                "sample_rate_hz": model.sample_rate_hz,
                "supports_profiles": model.supports_profiles,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "protocol_version": 1,
        "websocket_path": "/speech/realtime/ws",
        "models_endpoint": "/speech/models",
        "downloads_endpoint": "/speech/models/downloads",
        "profile_endpoints": {
            "asr_realtime": "/speech/profiles/asr.realtime/model",
            "vad_default": "/speech/profiles/vad.default/model",
            "wake_word_default": "/speech/profiles/wake_word.default/model",
        },
        "capabilities": {
            "input_audio_transcription": true,
            "wake_word_gate": true,
            "wake_word_detectors": ["local_kws"],
            "output_audio_synthesis": false,
            "tts_via_http": true,
            "tts_http_endpoint": "/v1/audio/speech",
            "batch_transcription_http_endpoint": "/v1/audio/transcriptions",
        },
        "input_audio": {
            "transport": "websocket_binary",
            "format": AUDIO_FORMAT,
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "channels": [1, 2],
        },
        "session_defaults": {
            "asr_model": selected_realtime_asr,
            "vad_model": selected_vad,
            "wake_word_model": selected_wake_word,
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "channels": 1,
            "enable_vad": true,
            "enable_wake_word": false,
            "wake_word_detector": DEFAULT_WAKE_WORD_DETECTOR.as_str(),
            "wake_words": DEFAULT_WAKE_WORDS,
            "strip_wake_word": true,
            "endpoint_trailing_silence_ms": DEFAULT_ENDPOINT_TRAILING_SILENCE_MS,
            "vad_min_silence_ms": DEFAULT_VAD_MIN_SILENCE_MS,
            "ready": realtime_asr_ready && vad_ready,
            "local_kws_ready": wake_word_ready,
            "missing_requirements": missing_requirements,
        },
        "filters": {
            "realtime_asr": "capabilities.realtime_asr == true",
            "vad": "capabilities.vad == true",
            "wake_word": "capabilities.wake_word == true",
            "installed": "installed == true",
        },
        "models": {
            "realtime_asr": realtime_asr_models,
            "vad": vad_models,
            "wake_word": wake_word_models,
        },
        "client_messages": [
            {
                "type": "session.update",
                "description": "Update active models and audio input settings. Use null or an empty string to fall back to the currently selected speech profile model.",
            },
            {
                "type": "input_audio_buffer.commit",
                "description": "Flush the current utterance and emit a completed transcript if available.",
            },
            {
                "type": "input_audio_buffer.clear",
                "description": "Drop buffered realtime state and reset the current utterance.",
            },
            {
                "type": "ping",
                "description": "Application-level keepalive.",
            },
        ],
        "server_events": [
            "session.created",
            "session.updated",
            "input_audio_buffer.committed",
            "input_audio_buffer.cleared",
            "input_audio_buffer.speech_started",
            "input_audio_buffer.speech_stopped",
            "input_audio_buffer.wake_word_detected",
            "response.audio_transcript.delta",
            "response.audio_transcript.completed",
            "pong",
            "error",
        ],
        "available_models": {
            "realtime_asr": realtime_asr_models,
            "vad": vad_models,
        },
    })
}

pub async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut connection = RealtimeConnection::new(state).await;

    if send_json(&mut socket, connection.session_event("session.created"))
        .await
        .is_err()
    {
        return;
    }

    while let Some(message) = socket.next().await {
        let message = match message {
            Ok(message) => message,
            Err(_) => break,
        };

        let events = match message {
            Message::Text(text) => connection.handle_text(text.as_ref()).await,
            Message::Binary(bytes) => connection.handle_audio(bytes.as_ref()).await,
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
        };

        for event in events {
            if send_json(&mut socket, event).await.is_err() {
                return;
            }
        }
    }
}

async fn send_json(socket: &mut WebSocket, value: Value) -> Result<(), axum::Error> {
    socket.send(Message::Text(value.to_string().into())).await
}

#[derive(Debug, Clone)]
struct RealtimeSessionConfig {
    asr_model: Option<String>,
    vad_model: Option<String>,
    wake_word_model: Option<String>,
    sample_rate_hz: u32,
    channels: u8,
    enable_vad: bool,
    enable_wake_word: bool,
    wake_word_detector: WakeWordDetectorKind,
    wake_words: Vec<String>,
    strip_wake_word: bool,
    endpoint_trailing_silence_ms: u32,
    vad_min_silence_ms: u32,
}

struct RealtimeConnection {
    session_id: String,
    state: Arc<AppState>,
    config: RealtimeSessionConfig,
    engines: Option<RealtimeEngines>,
    last_error: Option<String>,
}

impl RealtimeConnection {
    async fn new(state: Arc<AppState>) -> Self {
        let settings = state.bridge_settings().await;
        let config = RealtimeSessionConfig {
            asr_model: settings.speech_profiles.asr_realtime.clone(),
            vad_model: settings.speech_profiles.vad_default.clone(),
            wake_word_model: settings.speech_profiles.wake_word_default.clone(),
            sample_rate_hz: SAMPLE_RATE_HZ,
            channels: 1,
            enable_vad: true,
            enable_wake_word: false,
            wake_word_detector: DEFAULT_WAKE_WORD_DETECTOR,
            wake_words: DEFAULT_WAKE_WORDS
                .iter()
                .map(|value| value.to_string())
                .collect(),
            strip_wake_word: true,
            endpoint_trailing_silence_ms: DEFAULT_ENDPOINT_TRAILING_SILENCE_MS,
            vad_min_silence_ms: DEFAULT_VAD_MIN_SILENCE_MS,
        };

        let (engines, last_error) = match RealtimeEngines::build(&state, &config).await {
            Ok(engines) => (Some(engines), None),
            Err(error) => (None, Some(error)),
        };

        Self {
            session_id: Uuid::new_v4().to_string(),
            state,
            config,
            engines,
            last_error,
        }
    }

    fn session_event(&self, event_type: &str) -> Value {
        json!({
            "type": event_type,
            "session": {
                "id": self.session_id,
                "ready": self.engines.is_some(),
                "input_audio_format": AUDIO_FORMAT,
                "sample_rate_hz": self.config.sample_rate_hz,
                "channels": self.config.channels,
                "enable_vad": self.config.enable_vad,
                "enable_wake_word": self.config.enable_wake_word,
                "wake_word_detector": self.config.wake_word_detector.as_str(),
                "wake_words": self.config.wake_words,
                "strip_wake_word": self.config.strip_wake_word,
                "endpoint_trailing_silence_ms": self.config.endpoint_trailing_silence_ms,
                "vad_min_silence_ms": self.config.vad_min_silence_ms,
                "asr_model": self.config.asr_model,
                "vad_model": self.config.vad_model,
                "wake_word_model": self.config.wake_word_model,
                "last_error": self.last_error,
            }
        })
    }

    async fn handle_text(&mut self, text: &str) -> Vec<Value> {
        let command = match parse_client_command(text) {
            Ok(command) => command,
            Err(error) => {
                return vec![error_event(error, "invalid_command", true)];
            }
        };

        match command {
            ClientCommand::Ping => vec![json!({ "type": "pong" })],
            ClientCommand::InputAudioBufferClear => {
                if let Some(engines) = &mut self.engines {
                    engines.clear();
                }
                vec![json!({
                    "type": "input_audio_buffer.cleared",
                    "session_id": self.session_id,
                })]
            }
            ClientCommand::InputAudioBufferCommit => match &mut self.engines {
                Some(engines) => engines.commit(&self.session_id),
                None => vec![error_event(
                    not_ready_message(self.config.enable_vad),
                    "session_not_ready",
                    true,
                )],
            },
            ClientCommand::SessionUpdate(patch) => self.apply_update(patch).await,
        }
    }

    async fn handle_audio(&mut self, bytes: &[u8]) -> Vec<Value> {
        let Some(engines) = &mut self.engines else {
            return vec![error_event(
                not_ready_message(self.config.enable_vad),
                "session_not_ready",
                true,
            )];
        };

        let samples = match decode_pcm16le(bytes, self.config.channels) {
            Ok(samples) => samples,
            Err(error) => return vec![error_event(error, "invalid_audio", true)],
        };

        if samples.is_empty() {
            return Vec::new();
        }

        engines.process_audio_chunk(&samples)
    }

    async fn apply_update(&mut self, patch: SessionPatch) -> Vec<Value> {
        let next = match self.merge_session_patch(patch).await {
            Ok(next) => next,
            Err(error) => return vec![error_event(error, "invalid_command", true)],
        };

        match RealtimeEngines::build(&self.state, &next).await {
            Ok(engines) => {
                self.config = next;
                self.engines = Some(engines);
                self.last_error = None;
                vec![self.session_event("session.updated")]
            }
            Err(error) => {
                self.config = next;
                self.engines = None;
                self.last_error = Some(error.clone());
                vec![
                    self.session_event("session.updated"),
                    error_event(error, "session_not_ready", true),
                ]
            }
        }
    }

    async fn merge_session_patch(
        &self,
        patch: SessionPatch,
    ) -> Result<RealtimeSessionConfig, String> {
        let settings = self.state.bridge_settings().await;
        let mut next = self.config.clone();

        match patch.asr_model {
            ModelPatch::Unchanged => {}
            ModelPatch::DefaultProfile => {
                next.asr_model = settings.speech_profiles.asr_realtime.clone();
            }
            ModelPatch::Explicit(model_id) => next.asr_model = Some(model_id),
        }

        match patch.vad_model {
            ModelPatch::Unchanged => {}
            ModelPatch::DefaultProfile => {
                next.vad_model = settings.speech_profiles.vad_default.clone();
            }
            ModelPatch::Explicit(model_id) => next.vad_model = Some(model_id),
        }

        next.wake_word_model = settings.speech_profiles.wake_word_default.clone();

        if let Some(sample_rate_hz) = patch.sample_rate_hz {
            next.sample_rate_hz = sample_rate_hz;
        }
        if let Some(channels) = patch.channels {
            next.channels = channels;
        }
        if let Some(enable_vad) = patch.enable_vad {
            next.enable_vad = enable_vad;
        }
        if let Some(enable_wake_word) = patch.enable_wake_word {
            next.enable_wake_word = enable_wake_word;
        }
        if let Some(wake_word_detector) = patch.wake_word_detector {
            next.wake_word_detector = wake_word_detector;
        }
        if let Some(wake_words) = patch.wake_words {
            next.wake_words = wake_words;
        }
        if let Some(strip_wake_word) = patch.strip_wake_word {
            next.strip_wake_word = strip_wake_word;
        }
        if let Some(endpoint_trailing_silence_ms) = patch.endpoint_trailing_silence_ms {
            next.endpoint_trailing_silence_ms = endpoint_trailing_silence_ms;
        }
        if let Some(vad_min_silence_ms) = patch.vad_min_silence_ms {
            next.vad_min_silence_ms = vad_min_silence_ms;
        }

        validate_session_config(&next)?;
        Ok(next)
    }
}

struct RealtimeEngines {
    recognizer: OnlineRecognizer,
    stream: OnlineStream,
    vad: Option<VoiceActivityDetector>,
    last_partial_text: String,
    current_response_id: Option<String>,
    in_speech: bool,
    total_samples_received: u64,
    current_utterance_samples: Vec<f32>,
    speaker_verifier: Option<speaker::SpeakerVerifier>,
    speaker_filter_active: bool,
    wake_word_active: bool,
    wake_word: Option<WakeWordGate>,
    kws: Option<LocalKeywordSpotter>,
}

impl RealtimeEngines {
    async fn build(state: &Arc<AppState>, config: &RealtimeSessionConfig) -> Result<Self, String> {
        validate_session_config(config)?;

        let speech = state.speech();

        let asr_model_id = config.asr_model.as_deref().ok_or_else(|| {
            format!(
                "no realtime ASR model configured; set {} via PUT /speech/profiles/{}/model or send session.update with session.asr_model",
                profile_slug(SpeechProfile::AsrRealtime),
                profile_slug(SpeechProfile::AsrRealtime),
            )
        })?;
        if !speech.supports_profile(asr_model_id, SpeechProfile::AsrRealtime) {
            return Err(format!(
                "model {asr_model_id} does not support realtime ASR; choose a model where capabilities.realtime_asr is true"
            ));
        }
        let asr_model = speech.resolve_model_by_id(asr_model_id).await?;
        let recognizer =
            create_online_recognizer(&asr_model.install_path, config.endpoint_trailing_silence_ms)
                .ok_or_else(|| {
                    format!("failed to initialize sherpa-onnx online recognizer for {asr_model_id}")
                })?;
        let stream = recognizer.create_stream();
        let kws = if config.enable_wake_word {
            let kws_model_id = config.wake_word_model.as_deref().ok_or_else(|| {
                format!(
                    "no local wake word model configured; set {} via PUT /speech/profiles/{}/model",
                    profile_slug(SpeechProfile::WakeWordDefault),
                    profile_slug(SpeechProfile::WakeWordDefault),
                )
            })?;
            let effective_kws_model_id = if speech.supports_profile(kws_model_id, SpeechProfile::WakeWordDefault) {
                kws_model_id.to_string()
            } else {
                match speech.first_model_for_profile(SpeechProfile::WakeWordDefault) {
                    Some(fallback_id) => {
                        eprintln!(
                            "[realtime] wake word model {kws_model_id} not found or incompatible; falling back to {fallback_id}"
                        );
                        fallback_id
                    }
                    None => {
                        return Err(format!(
                            "model {kws_model_id} does not support local wake word detection; choose a model where capabilities.wake_word is true"
                        ));
                    }
                }
            };
            let kws_model = speech.resolve_model_by_id(&effective_kws_model_id).await?;
            Some(
                LocalKeywordSpotter::new(&kws_model.install_path, config.wake_words.clone())
                    .map_err(|error| {
                        format!(
                            "failed to initialize sherpa-onnx local keyword spotter for {effective_kws_model_id}: {error}"
                        )
                    })?,
            )
        } else {
            None
        };

        let vad = if config.enable_vad {
            let vad_model_id = config.vad_model.as_deref().ok_or_else(|| {
                format!(
                    "no VAD model configured; set {} via PUT /speech/profiles/{}/model or disable VAD with session.update",
                    profile_slug(SpeechProfile::VadDefault),
                    profile_slug(SpeechProfile::VadDefault),
                )
            })?;
            if !speech.supports_profile(vad_model_id, SpeechProfile::VadDefault) {
                return Err(format!(
                    "model {vad_model_id} does not support VAD; choose a model where capabilities.vad is true"
                ));
            }
            let vad_model = speech.resolve_model_by_id(vad_model_id).await?;
            Some(
                create_silero_vad(
                    &vad_model.install_path,
                    SAMPLE_RATE_HZ,
                    config.vad_min_silence_ms,
                )
                .ok_or_else(|| {
                    format!("failed to initialize sherpa-onnx VAD for {vad_model_id}")
                })?,
            )
        } else {
            None
        };
        let speaker_filter = state.bridge_settings().await.speaker_filter;
        let speaker_verifier = speaker::create_speaker_verifier(speech.clone(), &speaker_filter)
            .await
            .map_err(|error| format!("failed to initialize speaker filter: {error}"))?;

        Ok(Self {
            recognizer,
            stream,
            vad,
            last_partial_text: String::new(),
            current_response_id: None,
            in_speech: false,
            total_samples_received: 0,
            current_utterance_samples: Vec::new(),
            speaker_filter_active: speaker_verifier.is_some(),
            wake_word_active: config.enable_wake_word,
            speaker_verifier,
            wake_word: config
                .enable_wake_word
                .then(|| {
                    WakeWordGate::new(
                        config.wake_words.clone(),
                        config.strip_wake_word,
                        config.wake_word_detector,
                    )
                })
                .transpose()?,
            kws,
        })
    }

    fn process_audio_chunk(&mut self, samples: &[f32]) -> Vec<Value> {
        let mut events = Vec::new();
        let chunk_start_sample = self.total_samples_received;

        let mut speech_started_at = None;
        let mut speech_segments = Vec::new();
        if let Some(vad) = &self.vad {
            let detected_before = vad.detected();
            vad.accept_waveform(samples);
            let detected_after = vad.detected();

            if !detected_before && detected_after && !self.in_speech {
                speech_started_at = Some(chunk_start_sample);
            }

            while let Some(segment) = vad.front() {
                let start_sample = segment.start().max(0) as u64;
                let end_sample = start_sample + segment.n().max(0) as u64;
                speech_segments.push((start_sample, end_sample));
                vad.pop();
            }
        }

        if let Some(start_sample) = speech_started_at {
            self.in_speech = true;
            events.push(self.speech_started_event(start_sample));
        }
        for (start_sample, end_sample) in speech_segments {
            if !self.in_speech {
                self.in_speech = true;
                events.push(self.speech_started_event(start_sample));
            }
            events.push(self.speech_stopped_event(start_sample, end_sample));
            self.in_speech = false;
        }

        self.stream.accept_waveform(SAMPLE_RATE_HZ as i32, samples);
        if let Some(kws) = &mut self.kws {
            if let Some(keyword) = kws.accept_waveform(samples) {
                eprintln!("[realtime] local KWS detected keyword={keyword}");
                if let Some(wake_word) = &mut self.wake_word {
                    wake_word.mark_detected(keyword.clone());
                }
                events.push(wake_word_detected_event(&keyword));
            }
        }
        self.current_utterance_samples.extend_from_slice(samples);
        self.total_samples_received += samples.len() as u64;

        while self.recognizer.is_ready(&self.stream) {
            self.recognizer.decode(&self.stream);
        }

        events.extend(self.partial_transcript_events());

        if self.recognizer.is_endpoint(&self.stream) {
            events.extend(self.finish_current_utterance(false));
        }

        events
    }

    fn commit(&mut self, session_id: &str) -> Vec<Value> {
        let mut events = Vec::new();
        let response_id = self.current_response_id.clone();
        let mut had_speech = self.in_speech;

        let mut speech_segments = Vec::new();
        if let Some(vad) = &self.vad {
            vad.flush();
            while let Some(segment) = vad.front() {
                let start_sample = segment.start().max(0) as u64;
                let end_sample = start_sample + segment.n().max(0) as u64;
                speech_segments.push((start_sample, end_sample));
                had_speech = true;
                vad.pop();
            }
            vad.reset();
        }

        for (start_sample, end_sample) in speech_segments {
            if !self.in_speech {
                self.in_speech = true;
                events.push(self.speech_started_event(start_sample));
            }
            events.push(self.speech_stopped_event(start_sample, end_sample));
            self.in_speech = false;
        }

        self.stream.input_finished();
        while self.recognizer.is_ready(&self.stream) {
            self.recognizer.decode(&self.stream);
        }
        let completed = self.finish_current_utterance(true);
        let had_transcript = completed.iter().any(|event| {
            event.get("type").and_then(Value::as_str) == Some("response.audio_transcript.completed")
        });
        events.extend(completed);
        events.push(json!({
            "type": "input_audio_buffer.committed",
            "session_id": session_id,
            "response_id": response_id,
            "had_speech": had_speech,
            "had_transcript": had_transcript,
        }));
        events
    }

    fn clear(&mut self) {
        if let Some(vad) = &self.vad {
            vad.reset();
        }
        self.stream = self.recognizer.create_stream();
        self.last_partial_text.clear();
        self.current_response_id = None;
        self.in_speech = false;
        self.total_samples_received = 0;
        self.current_utterance_samples.clear();
        if let Some(wake_word) = &mut self.wake_word {
            wake_word.reset();
        }
        if let Some(kws) = &mut self.kws {
            eprintln!("[realtime] local KWS reset for next wake word");
            kws.reset();
        }
    }

    fn partial_transcript_events(&mut self) -> Vec<Value> {
        let Some(result) = self.recognizer.get_result(&self.stream) else {
            return Vec::new();
        };

        let text = result.text.trim().to_string();
        if text.is_empty() || text == self.last_partial_text {
            return Vec::new();
        }

        let response_id = self.ensure_response_id();
        let delta = transcript_delta(&self.last_partial_text, &text);
        self.last_partial_text = text.clone();

        vec![json!({
            "type": "response.audio_transcript.delta",
            "response_id": response_id,
            "delta": delta,
            "text": text,
            "is_final": result.is_final,
            "segment": result.segment,
            "start_time": result.start_time,
            "timestamps": result.timestamps,
            "speaker_filter": speaker_filter_status(self.speaker_filter_active, None),
            "wake_word": wake_word_status(self.wake_word_active, false, None),
        })]
    }

    fn finish_current_utterance(&mut self, committed: bool) -> Vec<Value> {
        let result = self.recognizer.get_result(&self.stream);
        let mut final_text = result
            .as_ref()
            .map(|item| item.text.trim())
            .filter(|text| !text.is_empty())
            .map(ToString::to_string)
            .or_else(|| {
                (!self.last_partial_text.is_empty()).then(|| self.last_partial_text.clone())
            });

        let mut events = Vec::new();
        let mut speaker_filter_match = None;
        let mut wake_word_match = None;
        if let Some(speaker_verifier) = &self.speaker_verifier {
            match speaker_verifier.match_score_for_samples(&self.current_utterance_samples) {
                Ok(Some(score)) => {
                    let matched = speaker_verifier.accepts_score(score);
                    eprintln!("[realtime] speaker filter score={score:.3} matched={matched}");
                    speaker_filter_match = Some(matched);
                }
                Ok(None) => {
                    eprintln!("[realtime] speaker filter score=none matched=false");
                    speaker_filter_match = Some(false);
                }
                Err(error) => {
                    events.push(error_event(
                        format!("failed to verify speaker: {error}"),
                        "speaker_filter_failed",
                        true,
                    ));
                    self.reset_current_utterance();
                    return events;
                }
            }
        }
        let accepted_speaker = speaker_filter_match != Some(false);
        if !accepted_speaker {
            if let Some(text) = final_text {
                let response_id = self.ensure_response_id();
                events.push(completed_event(
                    &response_id,
                    &text,
                    result.as_ref(),
                    committed,
                    self.speaker_filter_active,
                    speaker_filter_match,
                    self.wake_word_active,
                    wake_word_match,
                ));
            }
            self.reset_current_utterance();
            return events;
        }
        if let Some(wake_word) = &mut self.wake_word {
            let Some(text) = final_text.as_deref() else {
                self.reset_current_utterance();
                return events;
            };
            match wake_word.accept_final(text) {
                WakeWordDecision::Rejected => {
                    wake_word_match = Some(false);
                    if let Some(text) = final_text {
                        let response_id = self.ensure_response_id();
                        events.push(completed_event(
                            &response_id,
                            &text,
                            result.as_ref(),
                            committed,
                            self.speaker_filter_active,
                            speaker_filter_match,
                            self.wake_word_active,
                            wake_word_match,
                        ));
                    }
                    self.reset_current_utterance();
                    return events;
                }
                WakeWordDecision::Accepted { keyword: _, text } => {
                    wake_word_match = Some(true);
                    final_text = Some(text);
                }
            }
        }

        if let Some(text) = final_text {
            let response_id = self.ensure_response_id();
            events.push(completed_event(
                &response_id,
                &text,
                result.as_ref(),
                committed,
                self.speaker_filter_active,
                speaker_filter_match,
                self.wake_word_active,
                wake_word_match,
            ));
        }

        self.reset_current_utterance();

        if !accepted_speaker {
            return events;
        }
        events
    }

    fn reset_current_utterance(&mut self) {
        self.stream = self.recognizer.create_stream();
        self.last_partial_text.clear();
        self.current_response_id = None;
        self.in_speech = false;
        self.total_samples_received = 0;
        self.current_utterance_samples.clear();

        if let Some(vad) = &self.vad {
            vad.reset();
        }
        if let Some(wake_word) = &mut self.wake_word {
            wake_word.reset();
        }
        if let Some(kws) = &mut self.kws {
            kws.reset();
        }
    }

    fn ensure_response_id(&mut self) -> String {
        if let Some(response_id) = &self.current_response_id {
            return response_id.clone();
        }

        let response_id = Uuid::new_v4().to_string();
        self.current_response_id = Some(response_id.clone());
        response_id
    }

    fn speech_started_event(&mut self, start_sample: u64) -> Value {
        let response_id = self.ensure_response_id();
        json!({
            "type": "input_audio_buffer.speech_started",
            "response_id": response_id,
            "audio_start_ms": sample_to_ms(start_sample),
        })
    }

    fn speech_stopped_event(&mut self, start_sample: u64, end_sample: u64) -> Value {
        let response_id = self.ensure_response_id();
        json!({
            "type": "input_audio_buffer.speech_stopped",
            "response_id": response_id,
            "audio_start_ms": sample_to_ms(start_sample),
            "audio_end_ms": sample_to_ms(end_sample),
        })
    }
}

fn completed_event(
    response_id: &str,
    text: &str,
    result: Option<&RecognizerResult>,
    committed: bool,
    speaker_filter_active: bool,
    speaker_filter_match: Option<bool>,
    wake_word_active: bool,
    wake_word_match: Option<bool>,
) -> Value {
    json!({
        "type": "response.audio_transcript.completed",
        "response_id": response_id,
        "text": text,
        "committed": committed,
        "is_final": result.map(|item| item.is_final).unwrap_or(true),
        "segment": result.and_then(|item| item.segment),
        "start_time": result.and_then(|item| item.start_time),
        "timestamps": result.and_then(|item| item.timestamps.clone()),
        "speaker_filter": speaker_filter_status(speaker_filter_active, speaker_filter_match),
        "wake_word": wake_word_status(wake_word_active, wake_word_match.is_some(), wake_word_match),
    })
}

fn speaker_filter_status(active: bool, matched: Option<bool>) -> Value {
    json!({
        "active": active,
        "verified": matched.is_some(),
        "matched": matched,
    })
}

fn wake_word_status(active: bool, verified: bool, matched: Option<bool>) -> Value {
    json!({
        "active": active,
        "verified": verified,
        "matched": matched,
    })
}

fn wake_word_detected_event(keyword: &str) -> Value {
    json!({
        "type": "input_audio_buffer.wake_word_detected",
        "keyword": keyword,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeWordDetectorKind {
    LocalKws,
}

impl WakeWordDetectorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LocalKws => "local_kws",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "" | "local_kws" => Some(Self::LocalKws),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct WakeWordGate {
    wake_words: Vec<String>,
    strip_wake_word: bool,
    detected_keyword: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WakeWordDecision {
    Rejected,
    Accepted { keyword: String, text: String },
}

impl WakeWordGate {
    fn new(
        wake_words: Vec<String>,
        strip_wake_word: bool,
        _detector: WakeWordDetectorKind,
    ) -> Result<Self, String> {
        let wake_words = normalize_wake_words(wake_words);
        if wake_words.is_empty() {
            return Err(
                "session.wake_words must include at least one non-empty phrase".to_string(),
            );
        }
        Ok(Self {
            wake_words,
            strip_wake_word,
            detected_keyword: None,
        })
    }

    fn accept_final(&mut self, text: &str) -> WakeWordDecision {
        let Some(keyword) = self.detected_keyword.take() else {
            return WakeWordDecision::Rejected;
        };
        let text = if self.strip_wake_word {
            strip_wake_word_if_present(text, &self.wake_words)
        } else {
            text.trim().to_string()
        };
        WakeWordDecision::Accepted { keyword, text }
    }

    fn mark_detected(&mut self, keyword: String) {
        self.detected_keyword = Some(keyword);
    }

    fn reset(&mut self) {
        self.detected_keyword = None;
    }
}

struct LocalKeywordSpotter {
    spotter: KeywordSpotter,
    stream: OnlineStream,
    detected: bool,
}

impl LocalKeywordSpotter {
    fn new(model_dir: &Path, wake_words: Vec<String>) -> Result<Self, String> {
        let keywords = build_kws_keywords(model_dir, wake_words)?;
        let (encoder, decoder, joiner) = find_transducer_files(model_dir)?;
        eprintln!(
            "[realtime] local KWS initializing model_dir={} encoder={} keywords={:?}",
            model_dir.display(),
            encoder.display(),
            keywords
        );
        let mut config = KeywordSpotterConfig::default();
        config.model_config = OnlineModelConfig {
            transducer: OnlineTransducerModelConfig {
                encoder: Some(encoder.display().to_string()),
                decoder: Some(decoder.display().to_string()),
                joiner: Some(joiner.display().to_string()),
            },
            tokens: Some(model_dir.join("tokens.txt").display().to_string()),
            num_threads: 2,
            provider: Some("cpu".to_string()),
            ..Default::default()
        };
        config.keywords_buf = Some(keywords);
        config.keywords_threshold = 0.15;
        config.keywords_score = 1.0;
        eprintln!(
            "[realtime] local KWS config threshold={} score={}",
            config.keywords_threshold, config.keywords_score
        );

        let spotter = KeywordSpotter::create(&config).ok_or_else(|| {
            "sherpa-onnx rejected the local wake word model or keyword configuration".to_string()
        })?;
        let stream = spotter.create_stream();
        Ok(Self {
            spotter,
            stream,
            detected: false,
        })
    }

    fn accept_waveform(&mut self, samples: &[f32]) -> Option<String> {
        if self.detected {
            return None;
        }
        self.stream.accept_waveform(SAMPLE_RATE_HZ as i32, samples);
        while self.spotter.is_ready(&self.stream) {
            self.spotter.decode(&self.stream);
        }
        let keyword = self
            .spotter
            .get_result(&self.stream)
            .map(|result| result.keyword.trim().to_string())
            .filter(|keyword| !keyword.is_empty())?;
        self.detected = true;
        Some(keyword)
    }

    fn reset(&mut self) {
        self.spotter.reset(&self.stream);
        self.detected = false;
    }
}

fn build_kws_keywords(model_dir: &Path, wake_words: Vec<String>) -> Result<String, String> {
    let tokens_path = model_dir.join("tokens.txt");
    let supported_tokens = read_kws_tokens(&tokens_path)?;
    let mut lines = Vec::new();
    for word in normalize_wake_words(wake_words) {
        let line = build_kws_keyword_line(&word, &supported_tokens, &tokens_path)?;
        eprintln!("[realtime] local KWS keyword wake_word={word:?} tokens={line:?}");
        lines.push(line);
    }

    if lines.is_empty() {
        return Err("wake_words must include at least one non-empty phrase".to_string());
    }

    Ok(lines.join("\n"))
}

fn read_kws_tokens(tokens_path: &Path) -> Result<HashSet<String>, String> {
    let body = std::fs::read_to_string(tokens_path).map_err(|error| {
        format!(
            "failed to read local wake word tokens from {}: {error}",
            tokens_path.display()
        )
    })?;
    let tokens = body
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect::<HashSet<_>>();

    if tokens.is_empty() {
        return Err(format!(
            "local wake word token file is empty: {}",
            tokens_path.display()
        ));
    }

    Ok(tokens)
}

fn find_transducer_files(model_dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let entries: Vec<(String, PathBuf)> = std::fs::read_dir(model_dir)
        .map_err(|e| format!("failed to read model dir {}: {e}", model_dir.display()))?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy().into_owned();
            if name_str.ends_with(".onnx") {
                Some((name_str, e.path()))
            } else {
                None
            }
        })
        .collect();

    let mut int8_encoder: Option<PathBuf> = None;
    let mut int8_decoder: Option<PathBuf> = None;
    let mut int8_joiner: Option<PathBuf> = None;
    let mut fp32_encoder: Option<PathBuf> = None;
    let mut fp32_decoder: Option<PathBuf> = None;
    let mut fp32_joiner: Option<PathBuf> = None;

    for (name, path) in &entries {
        let is_int8 = name.contains(".int8.onnx");
        if name.starts_with("encoder") && name.contains("chunk-16") {
            if is_int8 {
                int8_encoder = Some(path.clone());
            } else {
                fp32_encoder = Some(path.clone());
            }
        } else if name.starts_with("decoder") && name.contains("chunk-16") {
            if is_int8 {
                int8_decoder = Some(path.clone());
            } else {
                fp32_decoder = Some(path.clone());
            }
        } else if name.starts_with("joiner") && name.contains("chunk-16") {
            if is_int8 {
                int8_joiner = Some(path.clone());
            } else {
                fp32_joiner = Some(path.clone());
            }
        }
    }

    let encoder = int8_encoder
        .or(fp32_encoder)
        .ok_or_else(|| format!("no encoder ONNX file found in {}", model_dir.display()))?;
    let decoder = int8_decoder
        .or(fp32_decoder)
        .ok_or_else(|| format!("no decoder ONNX file found in {}", model_dir.display()))?;
    let joiner = int8_joiner
        .or(fp32_joiner)
        .ok_or_else(|| format!("no joiner ONNX file found in {}", model_dir.display()))?;

    Ok((encoder, decoder, joiner))
}

fn build_kws_keyword_line(
    wake_word: &str,
    supported_tokens: &HashSet<String>,
    tokens_path: &Path,
) -> Result<String, String> {
    let tokens = tokenize_kws_wake_word(wake_word, supported_tokens, tokens_path)?;
    Ok(format!("{} :2.0 #1.0", tokens.join(" ")))
}

fn tokenize_kws_wake_word(
    wake_word: &str,
    supported_tokens: &HashSet<String>,
    tokens_path: &Path,
) -> Result<Vec<String>, String> {
    if wake_word.is_ascii() {
        if let Some(tokens) = tokenize_numbered_pinyin_wake_word(wake_word, supported_tokens) {
            return tokens.map_err(|error| {
                format!(
                    "wake word \"{wake_word}\" uses unsupported numbered pinyin: {error}. Supported tokens are listed in {}",
                    tokens_path.display()
                )
            });
        }
        if let Ok(tokens) = tokenize_ascii_wake_word(wake_word, supported_tokens, tokens_path) {
            return Ok(tokens);
        }
        return try_tokenize_as_raw_tokens(wake_word, supported_tokens, tokens_path);
    }

    try_tokenize_as_raw_tokens(wake_word, supported_tokens, tokens_path)
}

fn try_tokenize_as_raw_tokens(
    wake_word: &str,
    supported_tokens: &HashSet<String>,
    tokens_path: &Path,
) -> Result<Vec<String>, String> {
    let pieces: Vec<&str> = wake_word.split_whitespace().collect();
    if pieces.is_empty() {
        return Err(format!(
            "wake word \"{wake_word}\" is not supported by the selected local KWS model. Supported tokens are listed in {}",
            tokens_path.display()
        ));
    }
    if pieces.iter().all(|piece| supported_tokens.contains(*piece)) {
        return Ok(pieces.into_iter().map(str::to_string).collect());
    }

    let missing: Vec<&str> = pieces
        .into_iter()
        .filter(|piece| !supported_tokens.contains(*piece))
        .collect();
    Err(format!(
        "wake word \"{wake_word}\" contains tokens not found in the model vocabulary: {missing:?}. Supported tokens are listed in {}",
        tokens_path.display()
    ))
}

fn tokenize_numbered_pinyin_wake_word(
    wake_word: &str,
    supported_tokens: &HashSet<String>,
) -> Option<Result<Vec<String>, String>> {
    let syllables = wake_word
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if syllables.is_empty() || !syllables.iter().any(|part| has_pinyin_tone_number(part)) {
        return None;
    }
    if !syllables.iter().all(|part| has_pinyin_tone_number(part)) {
        return Some(Err(
            "every pinyin syllable must include a tone number, for example xiao3 ou1".to_string(),
        ));
    }

    let mut output = Vec::new();
    for syllable in syllables {
        let tokens = match tokenize_numbered_pinyin_syllable(syllable) {
            Some(tokens) => tokens,
            None => {
                return Some(Err(format!(
                    "invalid pinyin syllable \"{syllable}\"; use forms like xiao3 ou1"
                )));
            }
        };
        for token in tokens {
            if !supported_tokens.contains(&token) {
                return Some(Err(format!(
                    "syllable \"{syllable}\" maps to missing token \"{token}\""
                )));
            }
            output.push(token);
        }
    }

    Some(Ok(output))
}

fn has_pinyin_tone_number(value: &str) -> bool {
    value
        .chars()
        .last()
        .is_some_and(|ch| matches!(ch, '1'..='5'))
}

fn tokenize_numbered_pinyin_syllable(syllable: &str) -> Option<Vec<String>> {
    let tone = syllable.chars().last()?.to_digit(10)? as u8;
    if !(1..=5).contains(&tone) {
        return None;
    }
    let base = syllable[..syllable.len() - 1]
        .replace("u:", "ü")
        .replace('v', "ü")
        .to_lowercase();
    if base.is_empty() {
        return None;
    }

    let (initial, final_part) = split_pinyin_initial(&base);
    if final_part.is_empty() {
        return None;
    }
    let final_token = apply_pinyin_tone(final_part, tone)?;
    let mut tokens = Vec::new();
    if !initial.is_empty() {
        tokens.push(initial.to_string());
    }
    tokens.push(final_token);
    Some(tokens)
}

fn split_pinyin_initial(syllable: &str) -> (&str, &str) {
    for initial in ["zh", "ch", "sh"] {
        if let Some(rest) = syllable.strip_prefix(initial) {
            return (initial, rest);
        }
    }

    let Some(first) = syllable.chars().next() else {
        return ("", syllable);
    };
    if matches!(
        first,
        'b' | 'p'
            | 'm'
            | 'f'
            | 'd'
            | 't'
            | 'n'
            | 'l'
            | 'g'
            | 'k'
            | 'h'
            | 'j'
            | 'q'
            | 'x'
            | 'r'
            | 'z'
            | 'c'
            | 's'
            | 'y'
            | 'w'
    ) {
        let split = first.len_utf8();
        (&syllable[..split], &syllable[split..])
    } else {
        ("", syllable)
    }
}

fn apply_pinyin_tone(final_part: &str, tone: u8) -> Option<String> {
    if tone == 5 {
        return Some(final_part.to_string());
    }

    let mut chars = final_part.chars().collect::<Vec<_>>();
    let target = chars
        .iter()
        .position(|ch| matches!(ch, 'a' | 'e'))
        .or_else(|| chars.iter().position(|ch| *ch == 'o'))
        .or_else(|| chars.iter().rposition(|ch| matches!(ch, 'i' | 'u' | 'ü')))?;
    chars[target] = tone_mark(chars[target], tone)?;
    Some(chars.into_iter().collect())
}

fn tone_mark(base: char, tone: u8) -> Option<char> {
    match (base, tone) {
        ('a', 1) => Some('ā'),
        ('a', 2) => Some('á'),
        ('a', 3) => Some('ǎ'),
        ('a', 4) => Some('à'),
        ('e', 1) => Some('ē'),
        ('e', 2) => Some('é'),
        ('e', 3) => Some('ě'),
        ('e', 4) => Some('è'),
        ('i', 1) => Some('ī'),
        ('i', 2) => Some('í'),
        ('i', 3) => Some('ǐ'),
        ('i', 4) => Some('ì'),
        ('o', 1) => Some('ō'),
        ('o', 2) => Some('ó'),
        ('o', 3) => Some('ǒ'),
        ('o', 4) => Some('ò'),
        ('u', 1) => Some('ū'),
        ('u', 2) => Some('ú'),
        ('u', 3) => Some('ǔ'),
        ('u', 4) => Some('ù'),
        ('ü', 1) => Some('ǖ'),
        ('ü', 2) => Some('ǘ'),
        ('ü', 3) => Some('ǚ'),
        ('ü', 4) => Some('ǜ'),
        _ => None,
    }
}

fn tokenize_ascii_wake_word(
    wake_word: &str,
    supported_tokens: &HashSet<String>,
    tokens_path: &Path,
) -> Result<Vec<String>, String> {
    let mut output = Vec::new();
    for ch in wake_word.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_whitespace() || ch.is_ascii_punctuation() {
            continue;
        }
        let token = ch.to_string();
        if !supported_tokens.contains(&token) {
            return Err(format!(
                "wake word \"{wake_word}\" uses unsupported token \"{token}\" for the selected local KWS model. Supported tokens are listed in {}",
                tokens_path.display()
            ));
        }
        output.push(token);
    }

    if output.is_empty() {
        return Err(format!(
            "wake word \"{wake_word}\" does not contain any supported keyword tokens"
        ));
    }

    Ok(output)
}

fn normalize_wake_words(words: Vec<String>) -> Vec<String> {
    let mut normalized = words
        .into_iter()
        .map(|word| word.trim().to_string())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    normalized.sort_by_key(|word| std::cmp::Reverse(word.chars().count()));
    normalized.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    normalized
}

fn find_wake_word(text: &str, wake_words: &[String]) -> Option<(String, (usize, usize))> {
    let normalized_text = normalize_for_wake_word_match(text);
    for wake_word in wake_words {
        let normalized_wake_word = normalize_for_wake_word_match(wake_word);
        if normalized_wake_word.text.is_empty() {
            continue;
        }
        let Some(normalized_start) = normalized_text.text.find(&normalized_wake_word.text) else {
            continue;
        };
        let normalized_end = normalized_start + normalized_wake_word.text.len();
        if normalized_start != 0 && normalized_end != normalized_text.text.len() {
            continue;
        }
        let original_start = normalized_text
            .map
            .get(normalized_start)
            .map(|range| range.0)
            .unwrap_or(0);
        let original_end = normalized_text
            .map
            .get(normalized_end.saturating_sub(1))
            .map(|range| range.1)
            .unwrap_or_else(|| text.len());
        return Some((wake_word.clone(), (original_start, original_end)));
    }
    None
}

#[derive(Debug, Clone)]
struct NormalizedWakeText {
    text: String,
    map: Vec<(usize, usize)>,
}

fn normalize_for_wake_word_match(text: &str) -> NormalizedWakeText {
    let mut normalized = String::new();
    let mut map = Vec::new();
    for (index, ch) in text.char_indices() {
        if ch.is_whitespace()
            || ch.is_ascii_punctuation()
            || matches!(ch, '，' | '。' | '！' | '？' | '、')
        {
            continue;
        }
        for lower in ch.to_lowercase() {
            let start = normalized.len();
            normalized.push(lower);
            let end = normalized.len();
            for _ in start..end {
                map.push((index, index + ch.len_utf8()));
            }
        }
    }
    NormalizedWakeText {
        text: normalized,
        map,
    }
}

fn strip_wake_word_range(text: &str, start: usize, end: usize) -> String {
    let mut stripped = String::new();
    stripped.push_str(&text[..start]);
    stripped.push_str(&text[end..]);
    stripped
        .trim_matches(|ch: char| {
            ch.is_whitespace()
                || ch.is_ascii_punctuation()
                || matches!(ch, '，' | '。' | '！' | '？' | '、')
        })
        .trim()
        .to_string()
}

fn strip_wake_word_if_present(text: &str, wake_words: &[String]) -> String {
    find_wake_word(text, wake_words)
        .map(|(_, range)| strip_wake_word_range(text, range.0, range.1))
        .unwrap_or_else(|| text.trim().to_string())
}

fn validate_session_config(config: &RealtimeSessionConfig) -> Result<(), String> {
    if config.sample_rate_hz != SAMPLE_RATE_HZ {
        return Err(format!(
            "sample_rate_hz must be {SAMPLE_RATE_HZ} for realtime sherpa-onnx audio"
        ));
    }

    if !matches!(config.channels, 1 | 2) {
        return Err("channels must be 1 or 2".to_string());
    }

    if !(300..=5_000).contains(&config.endpoint_trailing_silence_ms) {
        return Err("endpoint_trailing_silence_ms must be between 300 and 5000".to_string());
    }

    if !(200..=5_000).contains(&config.vad_min_silence_ms) {
        return Err("vad_min_silence_ms must be between 200 and 5000".to_string());
    }

    if config.enable_wake_word && normalize_wake_words(config.wake_words.clone()).is_empty() {
        return Err("wake_words must include at least one non-empty phrase".to_string());
    }

    if !config.enable_wake_word && config.wake_word_detector != DEFAULT_WAKE_WORD_DETECTOR {
        return Err(
            "wake_word_detector can only be local_kws and can only be changed when enable_wake_word is true".to_string(),
        );
    }

    Ok(())
}

fn create_online_recognizer(
    model_dir: &Path,
    endpoint_trailing_silence_ms: u32,
) -> Option<OnlineRecognizer> {
    let mut config = OnlineRecognizerConfig::default();
    config.model_config = OnlineModelConfig {
        paraformer: OnlineParaformerModelConfig {
            encoder: Some(model_dir.join("encoder.int8.onnx").display().to_string()),
            decoder: Some(model_dir.join("decoder.int8.onnx").display().to_string()),
        },
        tokens: Some(model_dir.join("tokens.txt").display().to_string()),
        num_threads: 2,
        provider: Some("cpu".to_string()),
        ..Default::default()
    };
    config.decoding_method = Some("greedy_search".to_string());
    config.enable_endpoint = true;
    let endpoint_trailing_silence_secs = endpoint_trailing_silence_ms as f32 / 1000.0;
    config.rule1_min_trailing_silence = endpoint_trailing_silence_secs.max(0.3);
    config.rule2_min_trailing_silence = (endpoint_trailing_silence_secs * 0.7).max(0.3);
    config.rule3_min_utterance_length = 20.0;
    config.max_active_paths = 4;

    OnlineRecognizer::create(&config)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    SessionUpdate(SessionPatch),
    InputAudioBufferCommit,
    InputAudioBufferClear,
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionPatch {
    asr_model: ModelPatch,
    vad_model: ModelPatch,
    sample_rate_hz: Option<u32>,
    channels: Option<u8>,
    enable_vad: Option<bool>,
    enable_wake_word: Option<bool>,
    wake_word_detector: Option<WakeWordDetectorKind>,
    wake_words: Option<Vec<String>>,
    strip_wake_word: Option<bool>,
    endpoint_trailing_silence_ms: Option<u32>,
    vad_min_silence_ms: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelPatch {
    Unchanged,
    DefaultProfile,
    Explicit(String),
}

fn parse_client_command(text: &str) -> Result<ClientCommand, String> {
    let value = serde_json::from_str::<Value>(text)
        .map_err(|error| format!("invalid JSON text message: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "text messages must be JSON objects".to_string())?;
    let command_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing command type".to_string())?;

    match command_type {
        "session.update" => {
            let session = object
                .get("session")
                .ok_or_else(|| "session.update requires a session object".to_string())?;
            let session = session
                .as_object()
                .ok_or_else(|| "session.update session must be an object".to_string())?;
            Ok(ClientCommand::SessionUpdate(parse_session_patch(session)?))
        }
        "input_audio_buffer.commit" => Ok(ClientCommand::InputAudioBufferCommit),
        "input_audio_buffer.clear" => Ok(ClientCommand::InputAudioBufferClear),
        "ping" => Ok(ClientCommand::Ping),
        other => Err(format!("unsupported realtime command type: {other}")),
    }
}

fn parse_session_patch(object: &Map<String, Value>) -> Result<SessionPatch, String> {
    Ok(SessionPatch {
        asr_model: parse_model_patch(object.get("asr_model"), "session.asr_model")?,
        vad_model: parse_model_patch(object.get("vad_model"), "session.vad_model")?,
        sample_rate_hz: parse_optional_u32(object.get("sample_rate_hz"), "session.sample_rate_hz")?,
        channels: parse_optional_u8(object.get("channels"), "session.channels")?,
        enable_vad: parse_optional_bool(object.get("enable_vad"), "session.enable_vad")?,
        enable_wake_word: parse_optional_bool(
            object.get("enable_wake_word"),
            "session.enable_wake_word",
        )?,
        wake_word_detector: parse_optional_wake_word_detector(
            object.get("wake_word_detector"),
            "session.wake_word_detector",
        )?,
        wake_words: parse_optional_string_list(object.get("wake_words"), "session.wake_words")?,
        strip_wake_word: parse_optional_bool(
            object.get("strip_wake_word"),
            "session.strip_wake_word",
        )?,
        endpoint_trailing_silence_ms: parse_optional_u32(
            object.get("endpoint_trailing_silence_ms"),
            "session.endpoint_trailing_silence_ms",
        )?,
        vad_min_silence_ms: parse_optional_u32(
            object.get("vad_min_silence_ms"),
            "session.vad_min_silence_ms",
        )?,
    })
}

fn parse_model_patch(value: Option<&Value>, field: &str) -> Result<ModelPatch, String> {
    let Some(value) = value else {
        return Ok(ModelPatch::Unchanged);
    };

    match value {
        Value::Null => Ok(ModelPatch::DefaultProfile),
        Value::String(model_id) => {
            let model_id = model_id.trim();
            if model_id.is_empty() {
                Ok(ModelPatch::DefaultProfile)
            } else {
                Ok(ModelPatch::Explicit(model_id.to_string()))
            }
        }
        _ => Err(format!("{field} must be a string or null")),
    }
}

fn parse_optional_u32(value: Option<&Value>, field: &str) -> Result<Option<u32>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let number = value
        .as_u64()
        .ok_or_else(|| format!("{field} must be an integer"))?;
    u32::try_from(number)
        .map(Some)
        .map_err(|_| format!("{field} is out of range"))
}

fn parse_optional_u8(value: Option<&Value>, field: &str) -> Result<Option<u8>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let number = value
        .as_u64()
        .ok_or_else(|| format!("{field} must be an integer"))?;
    u8::try_from(number)
        .map(Some)
        .map_err(|_| format!("{field} is out of range"))
}

fn parse_optional_bool(value: Option<&Value>, field: &str) -> Result<Option<bool>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| format!("{field} must be a boolean"))
}

fn parse_optional_wake_word_detector(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<WakeWordDetectorKind>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .as_str()
        .ok_or_else(|| format!("{field} must be a string"))?;
    WakeWordDetectorKind::parse(value)
        .map(Some)
        .ok_or_else(|| format!("{field} must be local_kws"))
}

fn parse_optional_string_list(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let items = value
        .as_array()
        .ok_or_else(|| format!("{field} must be an array of strings"))?;
    let mut output = Vec::new();
    for item in items {
        let item = item
            .as_str()
            .ok_or_else(|| format!("{field} must be an array of strings"))?
            .trim()
            .to_string();
        if !item.is_empty() {
            output.push(item);
        }
    }
    Ok(Some(output))
}

fn decode_pcm16le(bytes: &[u8], channels: u8) -> Result<Vec<f32>, String> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() % 2 != 0 {
        return Err("binary audio payload must contain whole 16-bit PCM samples".to_string());
    }

    let samples = bytes
        .chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]) as f32 / i16::MAX as f32)
        .collect::<Vec<_>>();

    match channels {
        1 => Ok(samples),
        2 => {
            if samples.len() % 2 != 0 {
                return Err("stereo PCM payload must contain a whole number of frames".to_string());
            }
            Ok(samples
                .chunks_exact(2)
                .map(|frame| (frame[0] + frame[1]) * 0.5)
                .collect())
        }
        _ => Err("channels must be 1 or 2".to_string()),
    }
}

fn transcript_delta(previous: &str, current: &str) -> String {
    current
        .strip_prefix(previous)
        .map(ToString::to_string)
        .unwrap_or_else(|| current.to_string())
}

fn error_event(message: String, code: &str, recoverable: bool) -> Value {
    json!({
        "type": "error",
        "error": {
            "message": message,
            "code": code,
            "recoverable": recoverable,
        }
    })
}

fn not_ready_message(enable_vad: bool) -> String {
    if enable_vad {
        format!(
            "realtime session is not ready; install/select {} and {} models first; local_kws also requires {}",
            profile_slug(SpeechProfile::AsrRealtime),
            profile_slug(SpeechProfile::VadDefault),
            profile_slug(SpeechProfile::WakeWordDefault),
        )
    } else {
        format!(
            "realtime session is not ready; install/select a {} model first",
            profile_slug(SpeechProfile::AsrRealtime),
        )
    }
}

fn sample_to_ms(sample: u64) -> u64 {
    sample.saturating_mul(1000) / SAMPLE_RATE_HZ as u64
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use super::{
        ClientCommand, ModelPatch, SessionPatch, decode_pcm16le, parse_client_command,
        transcript_delta,
    };

    #[test]
    fn pcm_stereo_is_downmixed_to_mono() {
        let bytes = [
            0xff, 0x7f, 0x00, 0x00, // 32767, 0
            0x00, 0x80, 0x00, 0x00, // -32768, 0
        ];
        let decoded = decode_pcm16le(&bytes, 2).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0] > 0.49);
        assert!(decoded[1] < -0.49);
    }

    #[test]
    fn transcript_delta_uses_suffix_when_possible() {
        assert_eq!(transcript_delta("hello", "hello world"), " world");
        assert_eq!(transcript_delta("hello", "bonjour"), "bonjour");
    }

    #[test]
    fn session_update_accepts_profile_fallback_models() {
        let command = parse_client_command(
            r#"{"type":"session.update","session":{"asr_model":null,"vad_model":"","channels":2,"enable_vad":false}}"#,
        )
        .unwrap();

        assert_eq!(
            command,
            ClientCommand::SessionUpdate(SessionPatch {
                asr_model: ModelPatch::DefaultProfile,
                vad_model: ModelPatch::DefaultProfile,
                sample_rate_hz: None,
                channels: Some(2),
                enable_vad: Some(false),
                enable_wake_word: None,
                wake_word_detector: None,
                wake_words: None,
                strip_wake_word: None,
                endpoint_trailing_silence_ms: None,
                vad_min_silence_ms: None,
            })
        );
    }

    #[test]
    fn session_update_accepts_pause_thresholds() {
        let command = parse_client_command(
            r#"{"type":"session.update","session":{"endpoint_trailing_silence_ms":1500,"vad_min_silence_ms":900}}"#,
        )
        .unwrap();

        assert_eq!(
            command,
            ClientCommand::SessionUpdate(SessionPatch {
                asr_model: ModelPatch::Unchanged,
                vad_model: ModelPatch::Unchanged,
                sample_rate_hz: None,
                channels: None,
                enable_vad: None,
                enable_wake_word: None,
                wake_word_detector: None,
                wake_words: None,
                strip_wake_word: None,
                endpoint_trailing_silence_ms: Some(1500),
                vad_min_silence_ms: Some(900),
            })
        );
    }

    #[test]
    fn session_update_accepts_wake_word_gate() {
        let command = parse_client_command(
            r#"{"type":"session.update","session":{"enable_wake_word":true,"wake_word_detector":"local_kws","wake_words":["xiao3 ou1"],"strip_wake_word":true}}"#,
        )
        .unwrap();

        assert_eq!(
            command,
            ClientCommand::SessionUpdate(SessionPatch {
                asr_model: ModelPatch::Unchanged,
                vad_model: ModelPatch::Unchanged,
                sample_rate_hz: None,
                channels: None,
                enable_vad: None,
                enable_wake_word: Some(true),
                wake_word_detector: Some(super::WakeWordDetectorKind::LocalKws),
                wake_words: Some(vec!["xiao3 ou1".to_string()]),
                strip_wake_word: Some(true),
                endpoint_trailing_silence_ms: None,
                vad_min_silence_ms: None,
            })
        );
    }

    #[test]
    fn local_kws_keyword_line_tokenizes_english_phrase() {
        let tokens = ["h", "e", "y", "o", "m", "n", "i"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        assert_eq!(
            super::build_kws_keyword_line("hey omni", &tokens, Path::new("tokens.txt")).unwrap(),
            "h e y o m n i :2.0 #1.0"
        );
    }

    #[test]
    fn local_kws_keyword_line_tokenizes_numbered_pinyin() {
        let tokens = ["x", "iǎo", "ōu"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        assert_eq!(
            super::build_kws_keyword_line("xiao3 ou1", &tokens, Path::new("tokens.txt")).unwrap(),
            "x iǎo ōu :2.0 #1.0"
        );
    }

    #[test]
    fn local_kws_keyword_line_rejects_unsupported_phrase() {
        let tokens = ["h", "e", "y", "o", "m", "n", "i"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        let error =
            super::build_kws_keyword_line("小欧", &tokens, Path::new("tokens.txt")).unwrap_err();

        assert!(error.contains("小欧"));
        assert!(error.contains("tokens not found"));
        assert!(error.contains("tokens.txt"));
    }

    #[test]
    fn local_kws_keyword_line_matches_raw_tokens() {
        let tokens = ["n", "ǐ", "h", "ǎo", "xi", "ǎo", "ō", "u"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        assert_eq!(
            super::build_kws_keyword_line("n ǐ h ǎo", &tokens, Path::new("tokens.txt")).unwrap(),
            "n ǐ h ǎo :2.0 #1.0"
        );
    }

    #[test]
    fn local_kws_keyword_line_reports_missing_tokens() {
        let tokens = ["n", "ǐ", "h", "ǎo"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        let error = super::build_kws_keyword_line(
            "n ǐ h ǎo xi ǎo ō u",
            &tokens,
            Path::new("tokens.txt"),
        )
        .unwrap_err();

        assert!(error.contains("tokens not found"));
        assert!(error.contains("tokens.txt"));
    }

    #[test]
    fn local_kws_gate_requires_detector_event() {
        let mut gate = super::WakeWordGate::new(
            vec!["你好小欧".to_string()],
            true,
            super::WakeWordDetectorKind::LocalKws,
        )
        .unwrap();

        assert_eq!(
            gate.accept_final("你好小欧"),
            super::WakeWordDecision::Rejected
        );

        gate.mark_detected("你好小欧".to_string());
        assert_eq!(
            gate.accept_final("打开项目"),
            super::WakeWordDecision::Accepted {
                keyword: "你好小欧".to_string(),
                text: "打开项目".to_string(),
            }
        );

        gate.mark_detected("你好小欧".to_string());
        assert_eq!(
            gate.accept_final("你好小欧，打开项目"),
            super::WakeWordDecision::Accepted {
                keyword: "你好小欧".to_string(),
                text: "打开项目".to_string(),
            }
        );
    }
}
