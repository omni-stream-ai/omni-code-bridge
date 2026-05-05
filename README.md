# Omni Code Desktop Bridge

Rust desktop bridge for Omni Code. This repository exposes the HTTP and SSE API
used by the mobile client and connects it to local coding agents such as
`codex` and `claudecode`.

The Flutter client lives in:
`https://github.com/omni-stream-ai/omni-code`

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

At minimum, configure:

- `ECHO_MATE_BRIDGE_TOKEN`
- `ECHO_MATE_ALLOWED_CLIENT_IDS`

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
