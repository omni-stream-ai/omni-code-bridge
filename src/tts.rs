use reqwest::header::CONTENT_TYPE;
use serde::Serialize;

const BIGMODEL_SPEECH_URL: &str = "https://open.bigmodel.cn/api/paas/v4/audio/speech";
const BIGMODEL_TTS_MODEL: &str = "glm-tts";

#[derive(Debug, Serialize)]
struct BigModelSpeechRequest {
    model: &'static str,
    input: String,
    voice: String,
    speed: f32,
    volume: f32,
    response_format: String,
}

pub async fn synthesize_speech(
    input: String,
    voice: Option<String>,
    speed: Option<f32>,
    volume: Option<f32>,
    response_format: Option<String>,
) -> Result<(Vec<u8>, String), String> {
    let api_key = std::env::var("BIGMODEL_API_KEY")
        .map_err(|_| "BIGMODEL_API_KEY is not set in the bridge environment".to_string())?;

    let request = BigModelSpeechRequest {
        model: BIGMODEL_TTS_MODEL,
        input,
        voice: voice.unwrap_or_else(|| "female".to_string()),
        speed: speed.unwrap_or(1.0),
        volume: volume.unwrap_or(1.0),
        response_format: response_format.unwrap_or_else(|| "wav".to_string()),
    };

    let client = reqwest::Client::new();
    let response = client
        .post(BIGMODEL_SPEECH_URL)
        .bearer_auth(api_key)
        .json(&request)
        .send()
        .await
        .map_err(|error| format!("failed to call BigModel TTS: {error}"))?;

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("audio/wav")
        .to_string();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read BigModel TTS response: {error}"))?;

    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        return Err(format!("BigModel TTS request failed with {status}: {body}"));
    }

    Ok((bytes.to_vec(), content_type))
}
