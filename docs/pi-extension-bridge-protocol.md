# Pi Extension Bridge Protocol

Pi extensions execute inside the Pi session owned by `omni-code-bridge`. The
Flutter client is the interactive UI surface. Requests and responses are scoped
by both `session_id` and `request_id`; request IDs are not globally unique.

## Commands

After a Pi session loads its enabled extensions, Bridge publishes the current
extension command snapshot as a session event:

```json
{
  "type": "pi_extension_commands_updated",
  "payload": {
    "session_id": "session-id",
    "commands": [
      {
        "name": "/example",
        "description": "Run the example command",
        "source": "example-extension"
      }
    ]
  }
}
```

The client merges these entries into its command suggestions. Sending a slash
command still uses the normal turn endpoint. Pi resolves extension commands
before invoking the model.

## UI Requests

Bridge publishes requests from `ExtensionUiHandler` through the session event
stream:

```json
{
  "type": "pi_extension_ui_requested",
  "payload": {
    "session_id": "session-id",
    "request": {
      "request_id": "request-id",
      "method": "confirm",
      "payload": {},
      "extension_id": "example-extension",
      "timeout_ms": 120000
    }
  }
}
```

`method` and `payload` are forwarded without schema conversion so newer Pi UI
methods remain representable. Clients render the interactive `confirm`,
`select`, `input`, `editor`, and `custom` methods; notifications, status,
title, widget, spinner, progress, editor-state, and theme methods are mapped to
their Flutter equivalents. Unknown methods must be cancelled.

The response endpoint is:

```text
POST /v2/sessions/{session_id}/pi-ui/{request_id}
```

After subscribing, clients recover requests that were created while the UI was
disconnected or on another route with:

```text
GET /v2/sessions/{session_id}/pi-ui
```

The response is an API `data` list of the same request objects. Live and
recovered requests are deduplicated by request ID.

```json
{
  "value": true,
  "cancelled": false
}
```

`value` may be any JSON value. A cancelled request omits `value` and sets
`cancelled` to `true`. Successful resolution returns `204 No Content` and
publishes `pi_extension_ui_resolved`. Duplicate, expired, or wrong-session
responses return `400` and never reach the extension.

## Capability Approval

Capability prompts use the `confirm` UI method. The client returns a structured
decision when the payload identifies a capability or permission:

```json
{"value":{"allow":true,"persist":false},"cancelled":false}
```

`persist: false` allows the operation for the current session. `persist: true`
allows Pi to save the decision in its extension permission store. Denial uses
`value: false`. A timeout, disconnected response channel, unsupported method,
or closed UI resolves as cancellation; it never grants a capability.

## Timeouts And Concurrency

- The extension timeout is clamped to 1-600 seconds; the default is 120 seconds.
- Pending requests are keyed by `(session_id, request_id)` and may run
  concurrently.
- Timed-out requests are removed from the pending map.
- Request payloads include extension provenance for display and auditing.
