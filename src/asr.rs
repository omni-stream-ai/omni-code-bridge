use reqwest::multipart::{Form, Part};
use serde::Deserialize;

const BIGMODEL_TRANSCRIPTIONS_URL: &str =
    "https://open.bigmodel.cn/api/paas/v4/audio/transcriptions";
const BIGMODEL_ASR_MODEL: &str = "glm-asr-2512";

#[derive(Debug, Deserialize)]
struct BigModelTranscriptionResponse {
    text: String,
}

pub async fn transcribe_audio(
    bytes: Vec<u8>,
    filename: String,
    content_type: Option<String>,
) -> Result<String, String> {
    let api_key = std::env::var("BIGMODEL_API_KEY")
        .map_err(|_| "BIGMODEL_API_KEY is not set in the bridge environment".to_string())?;

    let client = reqwest::Client::new();
    let mut file_part = Part::bytes(bytes).file_name(filename);
    if let Some(mime) = content_type {
        file_part = file_part
            .mime_str(&mime)
            .map_err(|error| format!("invalid audio content type: {error}"))?;
    }

    let form = Form::new()
        .text("model", BIGMODEL_ASR_MODEL.to_string())
        .part("file", file_part);

    let response = client
        .post(BIGMODEL_TRANSCRIPTIONS_URL)
        .bearer_auth(api_key)
        .multipart(form)
        .send()
        .await
        .map_err(|error| format!("failed to call BigModel ASR: {error}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("failed to read BigModel ASR response: {error}"))?;

    if !status.is_success() {
        return Err(format!("BigModel ASR request failed with {status}: {body}"));
    }

    let payload: BigModelTranscriptionResponse = serde_json::from_str(&body)
        .map_err(|error| format!("failed to parse BigModel ASR response: {error}; body={body}"))?;

    Ok(payload.text)
}
