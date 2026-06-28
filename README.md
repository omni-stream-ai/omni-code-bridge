<p align="center">
  <img src="https://raw.githubusercontent.com/omni-stream-ai/omni-code/main/assets/app-icon.svg" width="128" alt="Omni Code Bridge">
</p>

<h1 align="center">Omni Code Bridge</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-2024--edition-dea584?logo=rust" alt="Rust"></a>
  <a href="https://github.com/omni-stream-ai/omni-code-bridge/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/omni-stream-ai/omni-code-bridge/ci.yml?label=CI" alt="CI"></a>
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
| **Homebrew** (macOS / Linux) | `brew install omni-stream-ai/omni-code-bridge/omni-code-bridge` |
| **Arch Linux** (AUR) | `yay -S omni-code-bridge-bin` |
| **cargo** | `cargo install omni-code-bridge` |
| **curl** (macOS / Linux) | `curl -fsSL https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.sh \| bash` |
| **PowerShell** (Windows) | `powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.ps1 -UseBasicParsing \| iex"` |

> **Arch Linux users:** After installing, enable the systemd service:
> ```bash
> systemctl --user enable --now omni-code-bridge.service
> ```
> The bundled user service now runs `omni-code-bridge settings-validate` as `ExecStartPre`, so
> invalid bridge settings fail before the server starts.

## Quick Start

```bash
cp .env.example .env
cargo run
```

The bridge listens on `http://127.0.0.1:8787` by default.

> `cargo run` requires the Rust toolchain. If you installed via Homebrew, AUR, or the install script, just run `omni-code-bridge` directly.

## Optional Dependencies

- Agent CLIs: `codex`, `claude`, `opencode`, or `kiro-cli`
- A local checkout of [omni-code](https://github.com/omni-stream-ai/omni-code) if you want the built-in APK update manifest endpoints to serve a local Android build

Agent binary paths can be overridden with `ECHO_MATE_CODEX_BIN` and `ECHO_MATE_OPENCODE_BIN`.

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

### Provider Selection

Session creation (`POST /sessions`), session update (`PATCH /sessions/{id}`), and message send
(`POST /sessions/{id}/messages`) all accept `provider_id`.

- Omit `provider_id` to skip bridge-side provider resolution entirely
- Set `provider_id` to `"AUTO"` to auto-select from project-level or global providers by priority
- Set `provider_id` to a concrete provider config id to force that provider

Examples:

```json
// No bridge provider resolution
{
  "provider_id": null
}
```

```json
// Explicit auto-selection
{
  "provider_id": "AUTO"
}
```

```json
// Explicit provider
{
  "provider_id": "openai-primary"
}
```

For `PATCH /sessions/{id}`, omitting `provider_id` leaves the session unchanged, while
`"provider_id": null` clears the stored session-level selection.

### ACP Profiles

Experimental ACP agent support is configured through `acp_servers` in `~/.omni-code/settings.json`.
You can start from the checked-in example at
[`config/settings.acp.example.json`](config/settings.acp.example.json) and adapt it for your local
deployment.

The bridge currently supports two ACP profiles:

- `kiro`: local `stdio + JSON-RPC` via `kiro-cli acp`
- `generic_http`: experimental HTTP/SSE ACP endpoint compatibility

Before using the `kiro` profile, make sure `kiro-cli` is installed and authenticated:

```bash
kiro-cli login
```

For `AgentKind::Acp`, set `provider_id` to an `acp_servers[].id`, or use `"AUTO"` to select the
highest-priority enabled ACP server.

Example Kiro ACP configuration:

```json
{
  "ai_approval": {
    "enabled": false,
    "base_url": "https://api.openai.com/v1",
    "api_key": "",
    "model": "gpt-4.1-mini",
    "max_risk": "low"
  },
  "model_providers": [],
  "acp_servers": [
    {
      "id": "kiro-local",
      "name": "Kiro Local ACP",
      "profile": "kiro",
      "command": "kiro-cli",
      "args": ["acp"],
      "default_model": "claude-sonnet-4",
      "enabled": true,
      "priority": 0,
      "headers": [],
      "env": []
    }
  ]
}
```

Example generic HTTP ACP configuration:

```json
{
  "ai_approval": {
    "enabled": false,
    "base_url": "https://api.openai.com/v1",
    "api_key": "",
    "model": "gpt-4.1-mini",
    "max_risk": "low"
  },
  "model_providers": [],
  "acp_servers": [
    {
      "id": "acp-http",
      "name": "ACP HTTP Gateway",
      "profile": "generic_http",
      "endpoint": "https://acp.example.com",
      "auth_token": "replace-me",
      "enabled": true,
      "priority": 10,
      "headers": [
        { "key": "X-ACP-Client", "value": "omni-code-bridge" }
      ],
      "env": []
    }
  ]
}
```

Example session creation using ACP:

```json
{
  "project_id": "my-project",
  "title": "Try Kiro ACP",
  "agent": "acp",
  "provider_id": "kiro-local"
}
```

For `generic_http`, the bridge currently probes and runs against these candidate turn URLs in order:
`/turns`, `/turn`, `/sessions/{session_id}/turns`, then the configured endpoint itself.
Requests are sent as JSON with fields like `session_id`, `thread_id`, `conversation_id`, `cwd`,
`project_root`, `input`, `message`, and `prompt`. The response may be either:

- JSON containing assistant text under fields such as `output_text`, `text`, `content`, or
  `message.text`
- SSE with `data: {...}` events that emit `delta.text`, approval events, and a terminal
  `{ "type": "done" }`

For approval round-trips, the bridge currently tries these reply URLs:
`/approvals/{request_id}/reply`, `/approval/{request_id}/reply`,
`/permissions/{request_id}/reply`, `/permission/{request_id}/reply`,
`/approvals/{request_id}`, `/approval/{request_id}`.
Cancellation is best-effort via `/sessions/{session_ref}/cancel`, `/session/{session_ref}/cancel`,
`/turns/cancel`, `/turn/cancel`, and `/cancel`.

`GET /agents` now reports both binary presence and structured readiness state. For Kiro-backed ACP,
`readiness` becomes `attention_required` when `kiro-cli` is installed but still needs
`kiro-cli login`, and `readiness_message` explains the next step. ACP entries also include
`acp_diagnostic` for the currently selected enabled server, including its `server_id`, `name`,
`profile`, `command`/`args` or `endpoint`, auth/model/header/env summary, and how many ACP servers
are enabled.

For health checks and scripting, `GET /agents/acp/diagnostic` returns the selected ACP server's
structured diagnostic together with `installed`, `installed_path`, `readiness`, and
`readiness_message`. The response is marked with `source: "live_probe"` and `probed_at`, and the
endpoint accepts `?refresh=true` so callers can explicitly request a fresh probe contract. You can
also pass `?provider_id=<acp_server_id>` to diagnose a specific configured ACP server directly,
including lower-priority or disabled entries. When omitted, the response fills in the resolved
`provider_id` for the selected server. For bulk inspection, `?all=true` returns diagnostics for all
configured ACP servers in one response. For a minimal real runtime check, `?probe=true` adds a
`handshake_probe` result. Today this performs a live `initialize -> session/new -> session/prompt`
probe turn for Kiro-backed ACP servers, and a live turn-creation `POST` probe for `generic_http`
endpoints. Probe results now include `mode` and `stage` so clients can distinguish a full stdio
probe turn from a best-effort HTTP turn probe. The `generic_http` probe now follows the same
candidate turn URLs as real runtime requests and can validate both JSON and SSE turn responses, but
it is still a lightweight healthcheck rather than a full end-to-end ACP conversation guarantee. When
`provider_id` is supplied but does not match any configured ACP server, the endpoint returns
`400 Bad Request`. Bulk results also include
`is_default_selected` so clients can tell which ACP server would currently be chosen by default.
For `generic_http`, diagnostics now also expose `turn_url_candidates`,
`approval_reply_url_templates`, and `cancel_url_templates`, so operators can see the exact bridge
URL patterns used during runtime without reading the source.
`all=true` and `provider_id` are mutually exclusive and also return `400 Bad Request` when used
together.

### Reasoning Effort

Session creation (`POST /sessions`), session update (`PATCH /sessions/{id}`), and message send
(`POST /sessions/{id}/messages`) also accept optional `reasoning_effort`.

Allowed values are `"low"`, `"medium"`, `"high"`, `"xhigh"`, and `"max"`. Message-level
`reasoning_effort` overrides the session default for that turn. For `PATCH /sessions/{id}`,
omitting `reasoning_effort` leaves it unchanged, while `"reasoning_effort": null` clears the
session default.

Codex receives this as `model_reasoning_effort`, Claude Code receives it as `--effort`, and
OpenCode currently ignores the field because the headless prompt API used by the bridge does not
expose a matching option.

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

### Settings Validation

Validate a bridge settings file before starting the server:

```bash
cargo run -- settings-validate --path config/settings.acp.example.json
```

If `--path` is omitted, the command validates the resolved default settings path
(`~/.omni-code/settings.json`, or `ECHO_MATE_SETTINGS_PATH` when set).
The production server startup path now uses the same strict settings parsing and validation rules,
so invalid settings fail fast instead of silently falling back to defaults.

### Session Trace

Inspect the last few agent command/response pairs from a local Codex or Claude transcript:

```bash
omni-code-bridge session-trace --session "<session-id-or-title>"
omni-code-bridge session-trace --session "<session-id-or-title>" --limit 10
```

`--session` accepts a session id, an exact title, or a partial title. `--limit` is optional and
defaults to `5`.

### Speech Smoke Test

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

### ACP Smoke Test

For ACP validation there is also a bridge-level smoke script:

```bash
scripts/acp-smoke.sh --keep-artifacts
```

What it does:

- Checks `GET /health`
- Auto-provisions a local client auth token when `BRIDGE_CLIENT_ID` and `BRIDGE_TOKEN` are not set
- Fetches `GET /agents` and verifies the ACP summary is present
- Fetches `GET /agents/acp/diagnostic`
- By default, runs `?probe=true&refresh=true` so you get a live ACP probe result as part of the smoke check
- Fails fast when ACP readiness is not `ready`, or when the live probe does not succeed

Useful options:

| Option | Description |
| --- | --- |
| `--provider-id ID` | Diagnose a specific `acp_servers[].id` |
| `--all` | Fetch diagnostics for all configured ACP servers |
| `--no-probe` | Skip the live probe and only inspect static diagnostics |
| `--allow-attention-required` | Allow `readiness=attention_required` without failing the smoke run |
| `--output-dir DIR` | Store JSON outputs in a fixed directory |
| `--no-auto-auth` | Require an existing `BRIDGE_CLIENT_ID` and `BRIDGE_TOKEN` |

The script requires `curl` and `jq`. If auto-auth is enabled, it also uses `cargo` when no local
`omni-code-bridge` binary is already available for approving the temporary client auth request.

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
