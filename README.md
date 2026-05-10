# Omni Code Bridge

[中文文档](README.zh-CN.md)

Rust bridge for Omni Code. This repository exposes the HTTP and SSE API
used by the mobile client and connects it to local coding agents such as
`codex`, `claudecode`, and `opencode`.

The Flutter client lives in:
`https://github.com/omni-stream-ai/omni-code`

## Install

**Homebrew (macOS / Linux):**

```bash
brew tap omni-stream-ai/homebrew-omni-code-bridge
brew install omni-code-bridge
```

**Arch Linux (AUR):**

```bash
yay -S omni-code-bridge-bin
systemctl --user enable --now omni-code-bridge.service
```

This package is published automatically by GitHub Actions.

**curl (macOS / Linux):**

```bash
curl -fsSL https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.sh | bash
```

**PowerShell (Windows):**

```powershell
powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.ps1 -UseBasicParsing | iex"
```

**cargo install:**

```bash
cargo install omni-code-bridge
```

## Requirements

- `Rust` / `cargo`
- Optional agent CLIs: `codex`, `claude`, or `opencode`
- Optional: a local checkout of
  `https://github.com/omni-stream-ai/omni-code` if you want the built-in APK
  update manifest endpoints to serve a local Android build

Agent binary paths can be overridden with `ECHO_MATE_CODEX_BIN` and
`ECHO_MATE_OPENCODE_BIN`.

## Run

```bash
cp .env.example .env
cargo run
```

The bridge listens on `http://127.0.0.1:8787` by default.

## HTTP API

- `POST /client-auth/requests` to request approval for a client
- `POST /client/messages` to push a message into a project/session flow
- `POST /devices/register` to register a client device for push notifications
- `GET /files?path=...` to return a file from a registered local project root
- `GET /app-update/manifest` and `GET /app-update/apk` for the built-in APK update feed

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

## Client APK Update Endpoints

The bridge can serve the newest Android APK it finds from a local checkout of
the client repository:

- `GET /app-update/manifest`
- `GET /app-update/apk`

By default it looks for APK build outputs from a local checkout of
`https://github.com/omni-stream-ai/omni-code` and reads the version from that
repository's `pubspec.yaml`.

These endpoints are primarily useful for local development or self-hosted
distribution. The client now checks the official GitHub release manifest by
default and should only use the bridge manifest when you explicitly configure a
custom update URL.

## File Fetch Endpoint

The bridge can return files from registered local project directories:

- `GET /files?path=<file-path>`

This endpoint requires the same client authorization headers as the rest of the
authenticated API:

- `Authorization: Bearer <token>`
- `x-omni-code-client-id: <client_id>`

The response body is the raw file content. `Content-Type` is inferred from the
file extension, so images, text files, JSON, PDF, audio, and similar assets can
be returned directly.

Supported query parameters:

- `path` required. Absolute paths can be returned directly. Relative paths require `project_id` or `session_id`.
- `project_id` optional. Restricts lookup to one project root.
- `session_id` optional. Restricts lookup to the local project root for that session.

`project_id` and `session_id` cannot be used together.

Examples:

```bash
curl \
  -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?path=/absolute/path/to/image.png"
```

```bash
curl \
  -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?project_id=<project-id>&path=assets/logo.png"
```

## Development

```bash
cargo check
sh scripts/setup-git-hooks.sh
```

Run `sh scripts/setup-git-hooks.sh` once after cloning to enable the local
`commit-msg` hook.

## Commit Messages

This repo validates commit messages with a shell-based `commit-msg` hook.
Use Conventional Commits. GitHub release notes are generated from these commit
messages:

- `feat: add approval webhook fallback`
- `fix(api): guard missing client id header`
- `docs: update bridge deployment notes`

## Release

This repository can publish release binaries for Linux, macOS, and Windows via
GitHub Actions. Release notes are generated from Conventional Commit messages.

## License

Omni Code Desktop Bridge is licensed under the [MIT License](LICENSE).
