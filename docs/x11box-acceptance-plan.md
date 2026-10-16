# x11box acceptance plugin implementation

Status: in progress; production readiness has not been established.

## Product contract

Installing the optional x11box acceptance plugin provisions its runtime on the
Bridge execution machine. Mobile clients can start project-bound acceptance
runs, watch the application, take exclusive manual control, return control to
automation, capture evidence, and submit a result. Runs retain project/version
identity, logs, screenshots, recordings and results across Bridge restarts.

The acceptance plugin is a Bridge capability, independent of the selected coding
agent. Pi extensions may call it but are not required to use the mobile UI.

## Delivery gates

- [x] Create `feat/x11box-acceptance` branches in client, Bridge and x11box.
- [ ] Versioned plugin manifest and documented installation configuration.
- [ ] Durable, asynchronous, idempotent environment installation with progress,
      bounded commands, health checks, failure diagnostics and retry.
- [ ] Isolated Docker run per acceptance; no Docker socket exposed to the guest.
- [ ] Authenticated project-bound run lifecycle and immutable version identity.
- [ ] Authenticated HTML5 HTTP/WebSocket gateway; no public runtime ports.
- [ ] Exclusive human/automation control enforced on the server, including
      reconnects, expiry and in-flight operations.
- [ ] Automation instructions and mobile manual control, including Chinese input.
- [ ] Screenshots, recording, reports and authenticated evidence download.
- [ ] Restart recovery, expiry cleanup, stop/uninstall and retained reports.
- [ ] Localized mobile installation, run list, viewer and result flows.
- [ ] Unit/integration tests covering authorization, traversal, failures, races,
      restart, cleanup and client lifecycle.
- [ ] Real Docker/browser end-to-end acceptance, mobile viewport verification,
      release/deployment documentation and checks.

## Environment observations

2026-10-16: Client and Bridge started clean on main. x11box already contained
uncommitted changes in Dockerfile, README, deployment and Rust sources, plus
manual Telegram test files. These are pre-existing user changes and must not be
reverted or silently included in feature commits. Docker is available locally.

## Implementation decisions

Bridge owns a runtime-plugin service, separate from the Pi JavaScript extension
loader and speech service manifests. Start with a shipped x11box manifest and
explicit supported runtime configuration, avoiding executable plugin installer
hooks. Prefer immutable image digests; a configured local x11box build context
supports development and private deployments. Resolve built images to immutable
image IDs before starting runs. Provisioning does not imply starting a permanent
desktop. Use Docker CLI argument arrays with deadlines, never shell interpolation.
