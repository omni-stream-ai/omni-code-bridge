use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use axum::http::header;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const TARGET_SAMPLE_RATE: u32 = 16_000;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    let wav = decode_wav(&args.wav_path)?;
    let samples = resample_if_needed(wav.samples, wav.sample_rate, TARGET_SAMPLE_RATE);
    if samples.is_empty() {
        bail!("wav does not contain audio samples");
    }

    let mut request = args.websocket_url().into_client_request()?;
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {}", args.token).parse()?,
    );
    request
        .headers_mut()
        .insert("x-omni-code-client-id", args.client_id.parse()?);

    let (mut socket, response) = connect_async(request)
        .await
        .context("failed to connect to realtime websocket")?;
    if response.status() != 101 {
        bail!("unexpected websocket status: {}", response.status());
    }

    let created = read_text_event(&mut socket)
        .await
        .context("failed to read session.created")?;
    println!("session.created: {created}");
    let created_json: Value = serde_json::from_str(&created)?;
    let ready = created_json
        .get("session")
        .and_then(|value| value.get("ready"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !ready {
        bail!("realtime session is not ready: {created}");
    }

    socket
        .send(Message::Text(
            r#"{"type":"session.update","session":{"sample_rate_hz":16000,"channels":1,"enable_vad":true}}"#
                .to_string()
                .into(),
        ))
        .await?;
    let updated = read_text_event(&mut socket)
        .await
        .context("failed to read session.updated")?;
    println!("session.updated: {updated}");

    for chunk in samples.chunks((TARGET_SAMPLE_RATE / 10) as usize) {
        socket
            .send(Message::Binary(encode_pcm16le(chunk).into()))
            .await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    socket
        .send(Message::Text(
            r#"{"type":"input_audio_buffer.commit"}"#.to_string().into(),
        ))
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut completed_text = None;
    let mut commit_ack = false;
    while tokio::time::Instant::now() < deadline {
        let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let next = tokio::time::timeout(timeout, socket.next())
            .await
            .context("timed out waiting for realtime transcript")?;
        let Some(message) = next.transpose()? else {
            break;
        };

        match message {
            Message::Text(text) => {
                let text = text.to_string();
                println!("{text}");
                let event: Value = serde_json::from_str(&text)?;
                match event.get("type").and_then(Value::as_str) {
                    Some("response.audio_transcript.completed") => {
                        completed_text = event
                            .get("text")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                    }
                    Some("input_audio_buffer.committed") => {
                        commit_ack = true;
                    }
                    _ => {}
                }
                if completed_text.is_some() && commit_ack {
                    break;
                }
            }
            Message::Close(frame) => {
                bail!("websocket closed before transcript completed: {frame:?}");
            }
            _ => {}
        }
    }

    let completed_text =
        completed_text.ok_or_else(|| anyhow::anyhow!("did not receive completed transcript"))?;
    if !commit_ack {
        bail!("did not receive input_audio_buffer.committed ack");
    }
    println!("completed transcript: {completed_text}");

    socket.close(None).await?;
    Ok(())
}

struct Args {
    bridge_url: String,
    client_id: String,
    token: String,
    wav_path: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut bridge_url =
            env::var("BRIDGE_URL").unwrap_or_else(|_| "http://127.0.0.1:8787".to_string());
        let mut client_id = env::var("BRIDGE_CLIENT_ID").ok();
        let mut token = env::var("BRIDGE_TOKEN").ok();
        let mut wav_path = env::var("WAV_PATH").ok().map(PathBuf::from);

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bridge-url" => bridge_url = next_arg(&mut args, "--bridge-url")?,
                "--client-id" => client_id = Some(next_arg(&mut args, "--client-id")?),
                "--token" => token = Some(next_arg(&mut args, "--token")?),
                "--wav" => wav_path = Some(PathBuf::from(next_arg(&mut args, "--wav")?)),
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            bridge_url,
            client_id: client_id
                .filter(|value| !value.trim().is_empty())
                .context("missing client id; pass --client-id or set BRIDGE_CLIENT_ID")?,
            token: token
                .filter(|value| !value.trim().is_empty())
                .context("missing token; pass --token or set BRIDGE_TOKEN")?,
            wav_path: wav_path.context("missing wav path; pass --wav or set WAV_PATH")?,
        })
    }

    fn websocket_url(&self) -> String {
        let base = self.bridge_url.trim_end_matches('/');
        if let Some(rest) = base.strip_prefix("http://") {
            return format!("ws://{rest}/speech/realtime/ws");
        }
        if let Some(rest) = base.strip_prefix("https://") {
            return format!("wss://{rest}/speech/realtime/ws");
        }
        format!("{base}/speech/realtime/ws")
    }
}

fn print_help() {
    println!(
        "Usage: cargo run --example speech_realtime_smoke -- \
--wav /path/to/file.wav [--bridge-url URL] [--client-id ID] [--token TOKEN]"
    );
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("missing value for {flag}"))
}

async fn read_text_event(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<String> {
    loop {
        let message = socket
            .next()
            .await
            .transpose()?
            .context("websocket closed unexpectedly")?;
        match message {
            Message::Text(text) => return Ok(text.to_string()),
            Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
            Message::Close(frame) => bail!("websocket closed early: {frame:?}"),
            _ => {}
        }
    }
}

struct DecodedWav {
    sample_rate: u32,
    samples: Vec<f32>,
}

fn decode_wav(path: &PathBuf) -> Result<DecodedWav> {
    let reader = hound::WavReader::open(path)
        .with_context(|| format!("failed to open wav: {}", path.display()))?;
    let spec = reader.spec();
    let channels = spec.channels.max(1) as usize;
    let sample_rate = spec.sample_rate;

    let samples = match spec.sample_format {
        hound::SampleFormat::Float => {
            let data = reader
                .into_samples::<f32>()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to decode float wav")?;
            downmix_interleaved(data, channels)
        }
        hound::SampleFormat::Int => {
            let bits = spec.bits_per_sample.max(1) as u32;
            let scale = ((1_i64 << (bits - 1)) - 1) as f32;
            let data = reader
                .into_samples::<i32>()
                .map(|sample| sample.map(|value| (value as f32 / scale).clamp(-1.0, 1.0)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to decode int wav")?;
            downmix_interleaved(data, channels)
        }
    };

    Ok(DecodedWav {
        sample_rate,
        samples,
    })
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

fn resample_if_needed(samples: Vec<f32>, from_rate: u32, to_rate: u32) -> Vec<f32> {
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
        output.push(samples[left] + (samples[right] - samples[left]) * frac);
    }
    output
}

fn encode_pcm16le(samples: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let value = (sample.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}
