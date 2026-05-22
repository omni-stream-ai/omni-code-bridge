use std::path::Path;

use sherpa_onnx::{SileroVadModelConfig, VadModelConfig, VoiceActivityDetector};

pub fn create_silero_vad(
    model_dir: &Path,
    sample_rate_hz: u32,
    min_silence_ms: u32,
) -> Option<VoiceActivityDetector> {
    let config = VadModelConfig {
        silero_vad: SileroVadModelConfig {
            model: Some(model_dir.join("silero_vad.onnx").display().to_string()),
            threshold: 0.62,
            min_silence_duration: min_silence_ms as f32 / 1000.0,
            min_speech_duration: 0.25,
            window_size: 512,
            max_speech_duration: 20.0,
        },
        sample_rate: sample_rate_hz as i32,
        num_threads: 1,
        provider: Some("cpu".to_string()),
        debug: false,
        ..Default::default()
    };

    VoiceActivityDetector::create(&config, 30.0)
}
