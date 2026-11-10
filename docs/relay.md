# Embedded Relay

Bridge embeds the vendored omni-relay-connector Rust crate. Once enabled, the server starts the connector after binding its HTTP listener and stops it before draining the listener on shutdown. It reconnects with backoff without replaying business requests.

## Setup

Configure Relay through an already authorized direct client in Settings, or before starting a headless server:

~~~sh
omni-code-bridge relay login
omni-code-bridge serve
~~~

For a self-hosted Relay:

~~~sh
omni-code-bridge relay login https://relay.example.org workstation-code
omni-code-bridge relay status
omni-code-bridge relay disable
~~~

Open the displayed verification page and approve the public device code using your own account. Private pairing capabilities and tokens never appear in CLI arguments or status output. CLI changes require a restart of an already running Bridge; the authorized direct client control API changes the running connector immediately.

GET /relay/status and /relay/clients, and POST /relay/login/start, /relay/login/poll, /relay/login/cancel, /relay/enable, /relay/disable and /relay/manage form the host control API. It uses the direct Bridge client authorization. Both /relay/* and /client-auth/* are denied to the reserved Connector principal, including otherwise public authorization registration routes. Provider management forwards the service's own Connector credential, never a mobile device credential or Relay administrator credential.

Bridge registers kind omni-code and targets its actual HTTP port on 127.0.0.1. The injected upstream identity is generated at startup, checked using a digest and discarded at exit. Remote devices are individually authorized and revoked by Relay; Bridge sees the dedicated Connector identity. Browser Origin is checked by Relay, while the connector supplies a native upstream request without browser cookies or Origin. Direct Bridge clients keep their existing authorization.

## Storage and builds

Private host state lives beside the resolved Bridge settings file in relay/relay.json, using directory mode 700 and file mode 600 on Unix. It contains the provider URL, enabled state, device session and service credential. The process-scoped upstream credential is not persisted. A later account login verifies service ownership before reusing an existing registration.

OMNI_CODE_HOME overrides Bridge's default ~/.omni-code data directory, including settings, authorization records, ACP secrets, device registrations, session runtime and metadata. OMNI_CODE_SETTINGS_PATH still overrides the settings file independently. Agent CLI configuration and login directories are separate; do not change them to enable Relay.

The crate is vendored because the Relay repository is currently private and the package is not published. Public product builds must not require private repository credentials. source.json records the content hashes; update copies from the canonical omni-relay scripts/sync-connectors.mjs script and verify with --check. Keep all copies identical.

## Verification

Run cargo fmt --check, cargo check, relevant Rust tests and clippy. The Relay repository's scripts/products.test.mjs runs actual Core and Bridge binaries against an isolated self-hosted Worker with generated credentials, including pairing, service registration, real JSON/SSE, restart recovery, disable and access revocation. It does not send real email or call real models. Public deployments, real audio engines, mobile packages and desktop browser resources require separate acceptance.

On 2026-11-08, formatting, the locked build, three desktop authorization tests and the product integration above passed. The integration also checked range evidence, large VNC messages and the Flutter browser pairing/service-selection flow. Four Docker or platform-dependent desktop tests remained ignored. Strict Clippy still reports existing diagnostics in other business modules; it is not recorded as passing.
