<p align="center">
  <img src="https://raw.githubusercontent.com/omni-stream-ai/omni-code/main/assets/app-icon.svg" width="128" alt="Omni Code Bridge">
</p>

<h1 align="center">Omni Code Bridge</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-2024-edition-dea584?logo=rust" alt="Rust"></a>
  <a href="https://github.com/omni-stream-ai/omni-code-bridge/releases"><img src="https://img.shields.io/github/v/release/omni-stream-ai/omni-code-bridge" alt="Release"></a>
  <a href="https://crates.io/crates/omni-code-bridge"><img src="https://img.shields.io/crates/v/omni-code-bridge" alt="Crates.io"></a>
</p>

<p align="center">
  <a href="README.zh-CN.md">中文文档</a>
</p>

---

Rust bridge for [Omni Code](https://github.com/omni-stream-ai/omni-code). Exposes HTTP and SSE APIs for the mobile client and connects to local coding agents such as `codex`, `claude`, and `opencode`.

## Install

| Method | Command |
| --- | --- |
| **Homebrew** (macOS / Linux) | `brew tap omni-stream-ai/homebrew-omni-code-bridge && brew install omni-code-bridge` |
| **Arch Linux** (AUR) | `yay -S omni-code-bridge-bin` |
| **cargo** | `cargo install omni-code-bridge` |
| **curl** (macOS / Linux) | `curl -fsSL https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.sh \| bash` |
| **PowerShell** (Windows) | `powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.ps1 -UseBasicParsing \| iex"` |

> **Arch Linux users:** After installing, enable the systemd service:
> ```bash
> systemctl --user enable --now omni-code-bridge.service
> ```

## Requirements

- `Rust` / `cargo`
- Optional agent CLIs: `codex`, `claude`, or `opencode`
- Optional: a local checkout of [omni-code](https://github.com/omni-stream-ai/omni-code) if you want the built-in APK update manifest endpoints to serve a local Android build

Agent binary paths can be overridden with `ECHO_MATE_CODEX_BIN` and `ECHO_MATE_OPENCODE_BIN`.

## Quick Start

```bash
cp .env.example .env
cargo run
```

The bridge listens on `http://127.0.0.1:8787` by default.

## HTTP API

| Endpoint | Description |
| --- | --- |
| `POST /client-auth/requests` | Request approval for a client |
| `POST /client/messages` | Push a message into a project/session flow |
| `POST /devices/register` | Register a client device for push notifications |
| `GET /files?path=...` | Return a file from a registered local project root |
| `GET /app-update/manifest` | APK update manifest |
| `GET /app-update/apk` | Download latest APK |
| `GET /speech`, `GET /speech/models` | Local speech model management |
| `POST /speech/models/downloads` | Download a speech model |
| `GET /speech/models/downloads/{task_id}` | Poll download status |
| `GET/PUT /speech/profiles/{profile}/model` | Bind model to profile |
| `GET /speech/realtime` | Realtime speech descriptor |
| `GET /speech/realtime/ws` | Websocket realtime/call-mode speech |
| `GET /v1/models` | OpenAI-compatible model list |
| `POST /v1/audio/transcriptions` | OpenAI-compatible ASR |
| `POST /v1/audio/speech` | OpenAI-compatible TTS |

## Local Speech API

The bridge can run local ASR and TTS through `sherpa-onnx` and exposes two API layers:

- Model management endpoints under `/speech/*` for the client to list, download, inspect, and select local speech models
- OpenAI-compatible inference endpoints under `/v1/*` so the client can reuse standard audio request flows

Current bundled model catalog includes:

- Batch ASR: `sensevoice-small-int8`
- Realtime/call-mode ASR candidates: `streaming-paraformer-zh-en`, `funasr-streaming-paraformer-zh-yue-en`
- TTS: `vits-melo-tts-zh-en`, `kokoro-int8-multi-lang-v1_1`
- VAD: `silero-vad`

`GET /speech/models` returns installation state plus metadata the client can use for filtering, including:

- `kind`, `runtime`, `backend`
- `capabilities` such as `streaming`, `batch_asr`, `realtime_asr`, `speech_synthesis`, `vad`
- `features`, `languages`, `download_size_mb`, `memory_hint`, `notes`
- `sample_rate_hz`, `default_voice`, `voices` for client-side TTS settings
  Local TTS voice selection currently uses numeric speaker IDs. `vits-melo-tts-zh-en` exposes a
  single voice (`0`), while `kokoro-int8-multi-lang-v1_1` exposes multiple speakers.
- `supports_profiles`, `recommended_profiles`, `selected_by`

### Setup Flow

1. Call `GET /speech/models`
2. Pick a compatible model and start download with `POST /speech/models/downloads`
3. Poll `GET /speech/models/downloads/{task_id}` until `completed`
4. Bind the installed model to a profile with `PUT /speech/profiles/{profile}/model`
5. Call `/v1/audio/transcriptions` or `/v1/audio/speech`

Speech profile bindings are persisted in `~/.omni-code/settings.json` by default. Set
`ECHO_MATE_SETTINGS_PATH` to override the settings file location.
TTS voice selections are stored per model through `GET/PUT /speech/models/{model_id}/voice`, so
switching between single-speaker and multi-speaker TTS models does not reuse an incompatible voice.

### OpenAI-Compatible Speech Behavior

- `GET /v1/models` returns installed local speech models that are usable by OpenAI-compatible audio endpoints
- `POST /v1/audio/transcriptions` accepts standard multipart form fields such as `file`, `model`, `language`, `prompt`, `response_format`, and `timestamp_granularities[]`
- `POST /v1/audio/speech` accepts OpenAI-style JSON with `model`, `input`, `voice`, `response_format`, and `speed`
- `model` is optional on both `/v1/audio/*` endpoints. If omitted, the bridge falls back to the selected speech profile model

Current compatibility limits:

- `/v1/audio/transcriptions` supports `response_format=json|text|verbose_json`
- `/v1/audio/transcriptions` rejects `stream=true`
- `/v1/audio/transcriptions` only supports `timestamp_granularities[]=segment`; `word` is not supported yet
- `/v1/audio/speech` currently supports `response_format=wav` only
- `/v1/audio/speech` does not support `instructions` yet

## Realtime Speech API

Realtime speech is exposed separately from `/v1/audio/*` because it is aimed at
future call mode rather than batch inference compatibility.

- `GET /speech/realtime` returns the websocket descriptor, audio requirements,
  default profile bindings, command names, and event names
- `GET /speech/realtime/ws` upgrades to a websocket session
- Websocket auth is the same as the rest of the protected API:
  `Authorization: Bearer <token>` and `x-omni-code-client-id: <client_id>`

### Realtime Contract

- Client sends binary websocket frames as raw `pcm_s16le`
- Sample rate is fixed at `16000`
- Channels can be `1` or `2`; stereo is downmixed to mono on the server
- `session.update` can override `asr_model`, `vad_model`, `channels`, `sample_rate_hz`, and `enable_vad`
- `input_audio_buffer.commit` flushes the current utterance
- `input_audio_buffer.clear` resets the current utterance state

### Server Events

- `session.created`
- `session.updated`
- `input_audio_buffer.committed`
- `input_audio_buffer.cleared`
- `input_audio_buffer.speech_started`
- `input_audio_buffer.speech_stopped`
- `response.audio_transcript.delta`
- `response.audio_transcript.completed`
- `error`

### Client-Side Filtering

- Use `GET /speech/models`
- Keep models where `installed == true`
- For realtime ASR, filter on `capabilities.realtime_asr == true`
- For VAD, filter on `capabilities.vad == true`

Example `session.update`:

```json
{
  "type": "session.update",
  "session": {
    "asr_model": "funasr-streaming-paraformer-zh-yue-en",
    "vad_model": "silero-vad",
    "sample_rate_hz": 16000,
    "channels": 1,
    "enable_vad": true
  }
}
```

## Speech Smoke Test

For local validation there is an end-to-end smoke test script:

```bash
scripts/speech-smoke.sh --keep-artifacts
```

What it does:

- Checks `GET /health`
- Auto-provisions a local client auth token when `BRIDGE_CLIENT_ID` and `BRIDGE_TOKEN` are not set
- Downloads missing ASR/TTS models through `/speech/models/downloads`
- Binds `asr.batch` and `tts.default`
- Synthesizes a wav file through `/v1/audio/speech`
- Transcribes that wav through `/v1/audio/transcriptions` using both profile fallback and explicit model selection

Useful options:

| Option | Description |
| --- | --- |
| `--with-call-models` | Also install and bind `asr.realtime` and `vad.default` |
| `--with-realtime` | Also run the websocket realtime ASR smoke example |
| `--skip-download` | Fail if required models are missing |
| `--output-dir DIR` | Store generated artifacts in a fixed directory |
| `--no-auto-auth` | Require an existing `BRIDGE_CLIENT_ID` and `BRIDGE_TOKEN` |

The script requires `curl` and `jq`.

There is also a realtime websocket smoke example:

```bash
cargo run --example speech_realtime_smoke -- \
  --bridge-url http://127.0.0.1:8787 \
  --client-id "$BRIDGE_CLIENT_ID" \
  --token "$BRIDGE_TOKEN" \
  --wav /tmp/omni-code-bridge-speech-smoke-12345/tts.wav
```

The example expects a local wav file, resamples it to `16 kHz`, streams it to
`/speech/realtime/ws`, and prints the observed realtime events and completed
transcript.

## Client Authorization

Clients register and get approved via CLI:

1. Client sends `POST /client-auth/requests` with `{ client_id, device_name }`
2. Admin reviews pending requests and approves them:

```bash
# list pending requests
omni-code-bridge client-auth list --pending

# approve a specific request
omni-code-bridge client-auth approve --request-id <request-id>

# approve all pending requests at once
omni-code-bridge client-auth approve
```

3. Client polls `GET /client-auth/requests/{request_id}` until `status` becomes `approved` and a `token` is returned
4. Client uses the token as `Authorization: Bearer <token>` together with the `x-omni-code-client-id` header for all subsequent API calls

Approved records are stored in `~/.omni-code/client-auth.json`.

## File Fetch Endpoint

`GET /files?path=<file-path>`

Returns files from registered local project directories. Requires client authorization headers:

- `Authorization: Bearer <token>`
- `x-omni-code-client-id: <client_id>`

The response body is the raw file content. `Content-Type` is inferred from the file extension, so images, text, JSON, PDF, audio, and similar assets can be returned directly.

| Parameter | Required | Description |
| --- | --- | --- |
| `path` | Yes | Absolute paths returned directly; relative paths require `project_id` or `session_id` |
| `project_id` | No | Restricts lookup to one project root |
| `session_id` | No | Restricts lookup to the local project root for that session |

> `project_id` and `session_id` cannot be used together.

```bash
# absolute path
curl -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?path=/absolute/path/to/image.png"

# relative path with project_id
curl -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?project_id=<project-id>&path=assets/logo.png"
```

## APK Update Endpoints

The bridge can serve the newest Android APK from a local checkout of [omni-code](https://github.com/omni-stream-ai/omni-code):

- `GET /app-update/manifest`
- `GET /app-update/apk`

By default it looks for APK build outputs and reads the version from that repository's `pubspec.yaml`. These endpoints are primarily useful for local development or self-hosted distribution. The client checks the official GitHub release manifest by default and only uses the bridge manifest when you explicitly configure a custom update URL.

## Development

```bash
cargo check
sh scripts/setup-git-hooks.sh
```

Run `sh scripts/setup-git-hooks.sh` once after cloning to enable the local `commit-msg` hook.

### Commit Messages

This repo validates commit messages with a shell-based `commit-msg` hook.
Use Conventional Commits. GitHub release notes are generated from these commit messages:

- `feat: add approval webhook fallback`
- `fix(api): guard missing client id header`
- `docs: update bridge deployment notes`

## Release

This repository publishes release binaries for Linux, macOS, and Windows via GitHub Actions. Release notes are generated from Conventional Commit messages.

## License

[MIT](LICENSE)
