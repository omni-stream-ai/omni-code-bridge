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

## Implementation checkpoint (not a release approval)

Bridge commits: 0dfe68f, 8eeca0f. Client commits: 9135d89, e58e313.
The optional built-in runtime plugin now has asynchronous installation, a Docker
image recipe, durable runs, per-run Docker networks, authenticated APIs, a scoped
ticket/cookie/WebSocket gateway, control leases, screenshots, previews, recording,
stop/result APIs, expiry maintenance and an uninstall API. The client has English
and Chinese installation/run/detail screens, remote browser launch and previews.

Verified:

- Runtime plugin unit tests: 5 passed.
- Acceptance identifier/port tests: 2 passed.
- Gateway path test: 1 passed.
- Client acceptance tests: 4 passed, including a 390 x 844 viewport.
- Flutter analyze passed, including the preview changes.
- Flutter full-suite comparison: both baseline 3913ab3 and the feature branch have
  exactly the same 58 failing test names; no new failures. Evidence is in
  /tmp/omni-acceptance-baseline-full.jsonl and
  /tmp/omni-acceptance-feature-full.jsonl. Baseline worktree is
  /tmp/omni-code-acceptance-baseline.
- Bridge full-suite runs did NOT complete: many adapter tests ran for over
  15 minutes. They were stopped. Do not report the full Bridge suite as passing.

Live verification:

- Docker build tool session 70179 (host docker process initially 1175521) is
  building omni-x11box:acceptance-dev using deploy/x11box.Dockerfile and the
  /data/code/x11box context. Rust compilation completed. Debian dependencies
  download slowly; last observed package was fonts-noto-cjk, after Chromium and
  ffmpeg. Revalidate this existing session before starting any replacement build.
- Once the image exists, resolve its immutable ID and run
  X11BOX_TEST_IMAGE=<image-id> cargo test real_container_acceptance_lifecycle
  -- --ignored --nocapture. This test has not yet run; it checks installation,
  browser launch/click, control exclusion, screenshots, restart and recording.

Required follow-up before production readiness:

- Actually run the Docker lifecycle test and browser/mobile gateway E2E. Verify
  Xpra index path, WebSocket subprotocol, cookies, Chinese input and touch behavior.
- Add HTTP authorization/ticket expiry/replay/revocation and control-race tests;
  current pure path tests are insufficient proof of gateway security.
- Finish artifact video playback/download in the client (video entries are
  currently disabled); implement byte-range support and scoped browser access.
- Expose plugin uninstall in the client and a project/session acceptance entry.
- Complete/document agent-facing plugin tools and acceptance checklist/report
  integration; raw instruction APIs alone do not complete that workflow.
- Installation interrupted during health checks can leave temporary containers;
  persist/recover their identity or bound their own lifetime. Ready installation
  requests currently re-provision rather than returning idempotently.
- Failed run startup removes the container but can leave its newly created
  network until maintenance. Improve immediate cleanup and preserve diagnostics.
- Verify recovery of unexpectedly exited containers, bounded guest commands,
  artifact limits/retention, and stale-preview/temp-screenshot cleanup.
- Preview/status polling, external-browser launch on Web, resumed app behavior,
  and create retries need lifecycle/idempotency review.
- Production deployment docs, supported-host constraints, pinned image delivery,
  resource/expiry configuration and complete release validation remain pending.

Do not shrink the original goal to this checkpoint or mark it complete.
