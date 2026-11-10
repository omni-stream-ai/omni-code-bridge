# Local model API tests

No model account, real credential, or paid request is needed. Python 3 is the
only dependency of the HTTP fixture. It binds to `127.0.0.1` only.

```sh
python3 tests/fixtures/llm_mock.py --port 18099
```

Register a fake key before connecting an agent. Each key has an independent FIFO
plan; once exhausted it returns `mock-ok`. Unknown keys return 401. Set the agent
provider base URL to `http://127.0.0.1:18099/v1`, key to `mock-key`, and model to
`mock-model`. Select the matching protocol in the bridge settings. For the Pi
Anthropic provider use `http://127.0.0.1:18099` (the SDK appends `/v1/messages`).

```sh
curl -fsS http://127.0.0.1:18099/__mock/plan \
  -H 'Content-Type: application/json' \
  -d '{"key":"mock-key","steps":[{"status":429,"headers":{"Retry-After":"0"}},{"tool":{"name":"read","arguments":{"path":"README.md"}}},{"text":"Finished"}]}'
curl -fsS http://127.0.0.1:18099/__mock/requests
```

`GET /__mock/requests` returns actual paths, bodies, nonsecret headers and SHA-256
key fingerprints. Authentication headers are omitted. Bodies are preserved for
history/tool assertions: use synthetic prompts and fake keys only. Plan setup is
test control traffic and is not recorded as model traffic. Start a fresh process
for an empty recording. Separate processes can stand in for different providers.

Supported endpoints: `/v1/chat/completions`, `/v1/responses`, `/v1/messages`.
All implement JSON and SSE responses, text and tool calls. The selected model is
echoed; model-specific errors can be scripted with `expect` or `status`.

Each plan step can contain:

| Field | Effect |
| --- | --- |
| `text` | Assistant text, default `mock-ok` |
| `tool` | `{ "name": "read", "arguments": {"path":"README.md"} }` |
| `expect` | Exact top-level request field expectations; mismatch returns 422 |
| `status`, `error`, `headers` | Inject HTTP error and e.g. `Retry-After` |
| `delay` | Seconds before response headers, to test timeout/cancellation |
| `chunk_delay` | Seconds between SSE events |
| `fragment_bytes` | Write SSE in byte fragments, including inside UTF-8 characters |
| `disconnect` | Close socket before response headers |
| `truncate_after` | End SSE before the numbered event; 1 sends only the first |
| `malformed` | Invalid JSON or invalid SSE JSON |
| `response` | Override the nonstream JSON response, including invalid schemas |

Run the wire-level tests:

```sh
python3 -m unittest discover -s tests/fixtures -p test_llm_mock.py -v
```

Run the real Pi SDK tests (Python must be on PATH with Rust and a C linker):

```sh
cargo test model_api_mock_agent_matrix -- --nocapture
```

On the current Nix environment, the pinned SDK needs its existing experimental
Rust compatibility setting:

```sh
nix shell nixpkgs#python3 nixpkgs#cargo nixpkgs#rustc nixpkgs#gcc nixpkgs#pkg-config \
  -c env RUSTC_BOOTSTRAP=1 cargo test model_api_mock_agent_matrix -- --nocapture
```

The Rust test re-executes itself with a temporary `PI_CODING_AGENT_DIR` and uses
the bridge's actual `configure_pi_provider`, the real SDK agent loop, real read
tool, and persisted sessions. It never modifies the user's Pi configuration.

| Scenario | Wire fixture tests | Real Pi SDK tests |
| --- | --- | --- |
| Three protocols, JSON/SSE, UTF-8 fragmentation | Yes | SSE + tool loop, all three |
| Address, key, model and protocol switch | Yes | Yes, same persisted session |
| History and tool results survive switching | Captured | Asserted on actual SDK requests |
| Concurrent sessions, distinct endpoints | Yes | Yes |
| temperature/top_p/token limit/stop/stream and reset | Request capture | Not exposed by bridge provider settings |
| HTTP 400/401/403/404/408/409/413/422/429/500/502/503/504 | Yes | Errors and recovery |
| Malformed JSON/SSE, disconnect, timeout, truncated SSE | Yes | SSE/disconnect/timeout and recovery; partial EOF behavior |
| Retry sequence | Scripted client requests | Actual bridge 429 retry and completion |
| Tool execution failure | Tool response capture | Real read failure and model continuation, all protocols |
| Cancellation and subsequent turn | Client disconnect | Bridge cancellation and next-turn recovery |
| Model override reset; changing settings during a request | Capture | Actual bridge settings and turn lifecycle |
| Invalid model and invalid configuration; restore defaults | Scripted errors | Bridge failure/rejection and successful recovery |

Wire tests validate fixture behavior and capture; they do not prove agent retry,
configuration UI, or session switching behavior. Pi tests exercise the SDK,
bridge provider serialization and AppState domain turn lifecycle, not the mobile UI
or HTTP route authentication.
The extended suites below separately exercise real Codex and HTTP integration.
Claude Code and OpenCode agents remain outside this validation scope.

The pinned Pi SDK deliberately returns accumulated partial text when a Chat
Completions stream ends without `[DONE]`. The integration test records this
behavior and verifies that another prompt can succeed afterward; it does not
claim that a premature EOF is surfaced as an error.

## Extended validation: Pi and Codex

The current automated integration scope includes Pi and the real Codex CLI
(verified with Codex 0.166.0). Claude Code and OpenCode agents are excluded.
Pi Anthropic Messages remains included. The earlier Pi-only table describes the
original matrix; the suites below extend it to actual HTTP routes and Codex.

```sh
python3 -m unittest discover -s tests/fixtures -p test_llm_mock.py -v
python3 -m unittest discover -s tests/fixtures -p test_real_codex.py -v
cargo test model_api_ -- --nocapture
cargo test bridge_settings -- --nocapture
```

`CODEX_TEST_BIN` can select the Python suite's Codex executable. Missing Codex
fails validation rather than silently skipping it. Rust uses the bridge's Codex
binary resolver. With Nix, use the toolchain command above with the filter
`model_api_`; run Python through `nix shell nixpkgs#python3 -c python3 ...`.
Tests use fake credentials, local model endpoints, temporary agent homes, and
synthetic tool files. DNS failures use reserved `.invalid` names. Test TLS uses
the public localhost certificate and key committed beside the fixture; these
are test data, never production credentials.

| Boundary | Automated assertions |
| --- | --- |
| Pi protocols | Real SDK Chat Completions, Responses and Anthropic streaming/tool loops |
| Codex protocol | Real CLI Responses requests, session resume and actual shell tools |
| Provider changes | Endpoint, key, model, protocol, persisted history; per-turn snapshot during an in-flight request or retry |
| Overrides | Provider/model per turn, session model override and null reset |
| HTTP/domain | Authentication rejection, command idempotency, concurrent-turn conflict, SSE cursor replay/conflict, cancellation and late response isolation |
| Persistence | Restart with sessions/history/providers; concurrent settings writes; readers never observe partial JSON; failed save preserves active and persisted settings |
| Errors | HTTP 400/401/403/404/408/409/413/422/429/500/502/503/504 and recovery |
| Transport | DNS, refused connection, TLS mismatch/untrusted certificate, disconnection, malformed/truncated SSE, stalled response body and recovery |
| Tools | Missing file, failed shell exit, unknown tool, invalid schema/JSON, multiple tools and recorded results; Pi tool timeout and iteration budget |
| Retry | Actual SDK/CLI budgets, exhausted budgets, cancellation during backoff, recovery, parameter edits during retry |
| Isolation | Concurrent sessions use separate endpoints/keys; invalid settings retain previous configuration |

These are bridge/API and agent integration tests. They do not automate mobile
screen interaction or prove behavior against every production model provider.
Only settings actually exposed by the bridge are tested as agent parameters;
wire capture of temperature/top_p/token limits does not imply bridge support.

Additional fixture controls: `tools` scripts multiple calls; `tool.arguments_json`
injects malformed arguments; `events` scripts SSE event/data pairs; `content_type`
overrides the stream type; `stall_at`/`stall_seconds` stalls the stream body.
An `exhausted` object on plan registration sets the fallback after FIFO exhaustion.
Recorded `at` values use monotonic time for retry timing assertions.

Known semantics: Pi accepts accumulated partial text on premature Chat
Completions EOF. Pi uses its fixed retry schedule rather than adopting an HTTP
Retry-After delay. Codex defaults to unbounded connection retries; finite-budget
profiles explicitly disable that feature, and a separate test verifies default
retry cancellation. Codex request and stream retry budgets nest (one retry at
each level permits four failed HTTP attempts). Its stream idle timeout starts
after response headers; delayed first headers are tested separately. Full
backoff tests intentionally wait real seconds rather than mocking time.

The real Codex positive `Retry-After: 1` case asserts at least 0.8 seconds
between requests. Bridge network retries are separately tested with CLI retry
budgets set to zero: eight requests total with 2/5/10/30/30/30/30-second
backoff. Retry status messages must not disable retries, and cancellation
during backoff must prevent the next request.

## Native agent and client handoff

`cargo test native_client_handoff -- --nocapture` runs two additional real-agent
HTTP integration cases (also selected by `cargo test model_api_`). Both run three
native turns interleaved with three client turns, and inspect actual model
requests rather than only checking the displayed answer.

* Codex: an independent real `codex exec` creates a thread before bridge startup;
  authenticated client routes discover and continue it; independent `codex exec
  resume` then continues the original thread ID. Repeat with changed endpoint,
  key, provider and model, a real shell tool, session model override/null reset,
  an authentication failure and recovery, and bridge restart.
* Pi: an independent real SDK handle creates the transcript before bridge
  startup; register the same session ID/path, then alternate SDK prompts and
  authenticated client turns. Across the alternating turns, switch Chat
  Completions -> Anthropic Messages -> Responses -> Chat Completions, while asserting that the
  client's next Responses turn still uses its selected model. Read-tool results
  and previous user/assistant text remain in subsequent requests. Pi is embedded
  in this product; this is not an external Pi CLI discovery test.
* Client snapshots must include completed external turns without restarting the
  bridge. Repeated reads must not advance the cursor or duplicate turns. Store
  regression tests verify that existing bridge turn IDs/metadata remain intact,
  repeated identical questions remain separate, and unfinished trailing native
  input waits for an assistant reply before being appended to mixed history.

Codex resumes with explicit model/provider overrides and retains the native
rollout's provider name as a process-local alias for the selected bridge config.
This avoids requiring an external CLI's transient provider configuration to be
persisted globally. It also prevents ordinary model changes from forking and
archiving the original native thread. A rejected resume fails the turn and
retains the original thread, instead of silently starting or forking a session.
The handoff test also checks that the session list contains only the original
Codex session after client turns, model switches and bridge restart. This is
verified for Codex 0.166.0.

Handoff means alternating completed turns in one shared native transcript.
Simultaneous writes by independent processes are not covered by this
validation. Mobile UI interaction is not automated.

For an already-open session, its SSE stream checks the native transcript every
2 seconds and emits the durable history reset event when an external turn is
imported. The existing client recovery path then fetches the new state. The
handoff tests subscribe before the second native turn and require this live
notification before making another state request. The watcher belongs to the
response stream and is dropped when that connection closes.
