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

Agent binary paths can be overridden with `OMNI_CODE_CODEX_BIN` and `OMNI_CODE_OPENCODE_BIN`.

## HTTP API

| Endpoint | Description |
| --- | --- |
| `POST /client-auth/requests` | Request approval for a client |
| `POST /client/messages` | Push a message into a project/session flow |
| `POST /devices/register` | Register a client device for push notifications |
| `GET /files?path=...` | Return a file from a registered local project root |
| `GET /v2/sessions` | List domain session summaries; accepts optional `project_id` |
| `GET /v2/sessions/{id}/state` | Load the atomic session and turn snapshot |
| `POST /v2/sessions/{id}/turns` | Submit an idempotent turn command |
| `GET /v2/sessions/{id}/events` | Stream durable domain events after a cursor |
| `GET /v2/sessions/{id}/messages` | Compatibility history projection with cursor pagination |
| `GET /app-update/manifest` | APK update manifest |
| `GET /app-update/apk` | Download latest APK |

`GET /v2/sessions/{id}/messages` returns messages in stored order. Without a cursor it returns the
latest page (`limit` defaults to `50` and is clamped to `1..=200`), so an in-progress assistant
reply remains visible on the default page. `before_id` returns messages before a message id;
`after_id` returns messages after a message id. `before_id` and `after_id` cannot be combined.
`limit` counts display messages: each `user` message counts as one, and each contiguous agent reply
segment counts as one even when it contains multiple `assistant`/`system` messages. The response is
`data: { messages, has_more, next_cursor }`; use `next_cursor` as the next
`before_id` when paging older history, or as the next `after_id` when polling newer messages.

### Provider Selection

Session creation (`POST /v2/sessions`), session update (`PATCH /v2/sessions/{id}`), and turn submit
(`POST /v2/sessions/{id}/turns`) all accept `provider_id`.
Session creation requires a client-generated `client_session_id`. The bridge uses it as the
canonical session ID and as the idempotency key for repeated create requests.

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

For `PATCH /v2/sessions/{id}`, omitting `provider_id` leaves the session unchanged, while
`"provider_id": null` clears the stored session-level selection.

### ACP Profiles

Experimental ACP agent support is configured through `acp_servers` in `~/.omni-code/settings.json`.
You can start from the checked-in example at
[`config/settings.acp.example.json`](config/settings.acp.example.json) and adapt it for your local
deployment.

The bridge currently supports these ACP profiles:

- `stdio`: local ACP `stdio + JSON-RPC` runtime, such as `kiro-cli acp` or another ACP-compatible CLI command
- `kiro`: backward-compatible alias for the older Kiro-specific stdio profile
- `generic_http`: experimental HTTP/SSE ACP endpoint compatibility

Before using Kiro through `stdio` or `kiro`, make sure `kiro-cli` is installed and authenticated:

```bash
kiro-cli login
```

For `AgentKind::Acp`, set `provider_id` to an `acp_servers[].id`, or use `"AUTO"` to select the
highest-priority enabled ACP server. Codex and OpenCode ACP should be configured here as ACP stdio
servers for validation; the bridge does not need a separate Codex/OpenCode integration path for
that.

Example stdio ACP configuration:

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
      "profile": "stdio",
      "command": "kiro-cli",
      "args": ["acp"],
      "default_model": "claude-sonnet-4",
      "enabled": true,
      "priority": 0,
      "headers": [],
      "env": []
    },
    {
      "id": "opencode-acp",
      "name": "OpenCode ACP",
      "profile": "stdio",
      "command": "opencode",
      "args": ["acp"],
      "enabled": false,
      "priority": 20,
      "headers": [],
      "env": []
    },
    {
      "id": "codex-acp",
      "name": "Codex ACP",
      "profile": "stdio",
      "command": "codex",
      "args": ["acp"],
      "enabled": false,
      "priority": 30,
      "headers": [],
      "env": []
    }
  ]
}
```

Adjust the `command` and `args` values to match the installed ACP runtime. Use disabled entries as
validation templates, then target them with
`GET /agents/acp/diagnostic?provider_id=<id>&probe=true&refresh=true` before enabling them by
priority.

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
  "title": "Try ACP",
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
`kiro-cli login`, and `readiness_message` explains the next step. Generic `stdio` entries only check
that the configured command exists; use `?probe=true` for the live ACP handshake. ACP entries also
include `acp_diagnostic` for the currently selected enabled server, including its `server_id`,
`name`, `profile`, `command`/`args` or `endpoint`, auth/model/header/env summary, and how many ACP
servers are enabled.

For health checks and scripting, `GET /agents/acp/diagnostic` returns the selected ACP server's
structured diagnostic together with `installed`, `installed_path`, `readiness`, and
`readiness_message`. The response is marked with `source: "live_probe"` and `probed_at`, and the
endpoint accepts `?refresh=true` so callers can explicitly request a fresh probe contract. You can
also pass `?provider_id=<acp_server_id>` to diagnose a specific configured ACP server directly,
including lower-priority or disabled entries. When omitted, the response fills in the resolved
`provider_id` for the selected server. For bulk inspection, `?all=true` returns diagnostics for all
configured ACP servers in one response. For a minimal real runtime check, `?probe=true` adds a
`handshake_probe` result. Today this performs a live `initialize -> session/new -> session/prompt`
probe turn for `stdio`/`kiro` ACP servers, and a live turn-creation `POST` probe for `generic_http`
endpoints. Probe results include `mode` and `stage` so clients can distinguish a full stdio probe
turn from a best-effort HTTP turn probe. The `generic_http` probe follows the same candidate turn
URLs as real runtime requests and can validate both JSON and SSE turn responses, but it is still a
lightweight healthcheck rather than a full end-to-end ACP conversation guarantee. When `provider_id`
is supplied but does not match any configured ACP server, the endpoint returns `400 Bad Request`.
Bulk results also include `is_default_selected` so clients can tell which ACP server would currently
be chosen by default. For `generic_http`, diagnostics now also expose `turn_url_candidates`,
`approval_reply_url_templates`, and `cancel_url_templates`, so operators can see the exact bridge
URL patterns used during runtime without reading the source. `all=true` and `provider_id` are
mutually exclusive and also return `400 Bad Request` when used together.

### Reasoning Effort

Session creation (`POST /v2/sessions`), session update (`PATCH /v2/sessions/{id}`), and turn submit
(`POST /v2/sessions/{id}/turns`) also accept optional `reasoning_effort`.

Allowed values are `"low"`, `"medium"`, `"high"`, `"xhigh"`, and `"max"`. Message-level
`reasoning_effort` overrides the session default for that turn. For `PATCH /v2/sessions/{id}`,
omitting `reasoning_effort` leaves it unchanged, while `"reasoning_effort": null` clears the
session default.

Codex receives this as `model_reasoning_effort`, Claude Code receives it as `--effort`, and
OpenCode currently ignores the field because the headless prompt API used by the bridge does not
expose a matching option.

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
(`~/.omni-code/settings.json`, or `OMNI_CODE_SETTINGS_PATH` when set).
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
