# Omni Code Bridge

中文说明：[README.zh-CN.md](README.zh-CN.md)

Rust bridge for Omni Code. This repository exposes the HTTP and SSE API
used by the mobile client and connects it to local coding agents such as
`codex` and `claudecode`.

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
yay -S omni-code-bridge
```

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
- Optional: a local checkout of
  `https://github.com/omni-stream-ai/omni-code` if you want the built-in APK
  update manifest endpoints to serve a local Android build

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
