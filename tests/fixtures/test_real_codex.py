"""Real Codex CLI against the local model fixture; no account/model access.

CODEX_TEST_BIN may select a binary. Missing Codex is a failure, never a skip.
"""
import json
import os
import signal
import socket
import ssl
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest

from llm_mock import MockServer, fingerprint


class CodexTests(unittest.TestCase):
    def setUp(self):
        self.binary = os.environ.get("CODEX_TEST_BIN") or shutil.which("codex")
        self.assertTrue(self.binary, "Install Codex or set CODEX_TEST_BIN")
        self.temp = tempfile.TemporaryDirectory(prefix="llm-real-codex-")
        self.root = Path(self.temp.name)
        self.server = MockServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1"
        self.key = "fake-codex-key"
        self.extra_servers = []
        self.plan([])
        self.configure()

    def tearDown(self):
        for server, thread in self.extra_servers:
            server.shutdown()
            server.server_close()
            thread.join()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def plan(self, steps, exhausted=None):
        with self.server.lock:
            self.server.plans[fingerprint(self.key)] = steps
            self.server.defaults[fingerprint(self.key)] = exhausted or {}

    def configure(self, url=None, model="mock-codex", retries=0, unbounded=False):
        self.model = model
        home = self.root / "codex"
        home.mkdir(exist_ok=True)
        (home / "config.toml").write_text(
            f'model = {json.dumps(model)}\nmodel_provider = "fixture"\n'
            'approval_policy = "never"\n'
            f'[features]\nunbounded_connection_retries = {str(unbounded).lower()}\n'
            '[model_providers.fixture]\nname = "Local mock"\n'
            f'base_url = {json.dumps(url or self.url)}\nwire_api = "responses"\n'
            'requires_openai_auth = false\nenv_key = "MOCK_MODEL_KEY"\n'
            f'request_max_retries = {retries}\nstream_max_retries = {retries}\n'
            'stream_idle_timeout_ms = 800\n')

    def command(self, prompt="Say hello", resume=None):
        args = [self.binary, "exec"]
        if resume:
            args += ["resume", resume]
        args += ["--json", "--skip-git-repo-check", prompt]
        env = dict(os.environ, CODEX_HOME=str(self.root / "codex"), MOCK_MODEL_KEY=self.key,
                   NO_PROXY="*", no_proxy="*", RUST_LOG="error")
        return args, env

    def run_codex(self, prompt="Say hello", resume=None, timeout=20):
        args, env = self.command(prompt, resume)
        try:
            completed = subprocess.run(args, env=env, cwd=self.root, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.fail(f"Codex exceeded {timeout}s: stdout={(error.stdout or b'')[-3000:]!r}; stderr={(error.stderr or b'')[-3000:]!r}")
        events = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
        return completed, events

    def test_text_stream_real_cli(self):
        self.plan([{"text": "你好-codex-🌍", "fragment_bytes": 1}])
        completed, events = self.run_codex()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("你好-codex-🌍", completed.stdout)
        self.assertTrue(any(e["type"] == "turn.completed" for e in events), events)
        self.assertEqual(self.server.requests[-1]["body"]["model"], self.model)
        self.assertEqual(self.server.requests[-1]["key_sha256"], fingerprint(self.key))
        self.assertNotIn(self.key, completed.stdout + completed.stderr)

    def assert_failed(self, completed, events):
        self.assertNotEqual(completed.returncode, 0, completed.stdout)
        self.assertFalse(any(e["type"] == "turn.completed" for e in events), events)
        self.assertTrue(any(e["type"] in ("turn.failed", "error") for e in events), completed.stderr + completed.stdout)
        self.assertNotIn(self.key, completed.stderr + completed.stdout)

    def test_http_errors_and_recovery_real_cli(self):
        for status in (400, 401, 403, 404, 408, 409, 413, 422, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.plan([], {"status": status, "headers": {"Retry-After": "0"}})
                before = len(self.server.requests)
                completed, events = self.run_codex(f"http error {status}")
                self.assert_failed(completed, events)
                self.assertEqual(len(self.server.requests), before + 1, "zero retry configuration must be respected")
        self.plan([{"text": "recovered-after-http-errors"}])
        completed, _ = self.run_codex("recover HTTP errors")
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("recovered-after-http-errors", completed.stdout)

    def test_stream_faults_real_cli(self):
        for step in ({"malformed": True}, {"disconnect": True}, {"truncate_after": 1},
                     {"stall_at": 1, "stall_seconds": 2},
                     {"events": [{"event": "response.failed", "data": {
                         "type": "response.failed", "response": {"id": "resp_mock", "status": "failed", "error": {"code": "server_error", "message": "injected terminal error"}}}}]}):
            with self.subTest(step=step):
                self.plan([], step)
                completed, events = self.run_codex("broken stream")
                self.assert_failed(completed, events)
        self.plan([{"text": "stream-recovered"}])
        completed, _ = self.run_codex()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        # Codex's stream idle deadline starts after response headers; a slow
        # first header is distinct and must not be mislabeled as a body timeout.
        self.plan([{"delay":2,"text":"slow-header-success"}])
        completed, _ = self.run_codex("slow headers")
        self.assertEqual(completed.returncode,0,completed.stderr + completed.stdout)
        self.assertIn("slow-header-success",completed.stdout)

    def test_network_and_tls_faults_real_cli(self):
        tls = MockServer()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(Path(__file__).with_name("llm_mock_cert.pem"), Path(__file__).with_name("llm_mock_key.pem"))
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        thread = threading.Thread(target=tls.serve_forever, daemon=True)
        thread.start()
        self.extra_servers.append((tls, thread))
        refused = socket.socket()
        refused.bind(("127.0.0.1", 0))
        port = refused.getsockname()[1]
        refused.close()
        for url in ("http://no-such-model.invalid/v1", f"http://127.0.0.1:{port}/v1",
                    self.url.replace("http:", "https:"), f"https://127.0.0.1:{tls.server_port}/v1"):
            with self.subTest(url=url):
                self.configure(url=url)
                completed, events = self.run_codex("network fault")
                self.assert_failed(completed, events)
        self.configure()
        self.plan([{"text": "network-recovered"}])
        completed, _ = self.run_codex()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_real_tool_failures_parallel_calls_and_results(self):
        (self.root / "hello.txt").write_text("codex-tool-output-marker")
        cases = [
            {"tool": {"name": "__mock_shell__", "command": "cat hello.txt"}},
            {"tool": {"name": "__mock_shell__", "command": "exit 7"}},
            {"tool": {"name": "nonexistent_tool", "arguments": {}}},
            {"tool": {"name": "exec_command", "arguments": {"cmd": 42}}},
            {"tool": {"name": "exec_command", "arguments_json": "{broken"}},
            {"tools": [{"name": "__mock_shell__", "command": "cat hello.txt"},
                       {"name": "__mock_shell__", "command": "cat hello.txt"}]},
        ]
        for index, step in enumerate(cases):
            with self.subTest(index=index):
                self.plan([step, {"text": "after-tool-response"}])
                completed, events = self.run_codex(f"exercise tool case {index}")
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                self.assertIn("after-tool-response", completed.stdout)
                body = self.server.requests[-1]["body"]
                outputs = [item for item in body["input"] if item.get("type") == "function_call_output"]
                self.assertEqual(len(outputs), 2 if "tools" in step else 1, body)
                if index in (0, 5):
                    self.assertTrue(all("codex-tool-output-marker" in json.dumps(out) for out in outputs), outputs)
                if index in (1, 2, 3, 4):
                    self.assertTrue(any(word in json.dumps(outputs).lower() for word in ("error", "failed", "7", "unsupported")), outputs)

    def test_retry_budget_retry_after_and_recovery_real_cli(self):
        self.configure(retries=1)
        for status in (429, 500, 503):
            with self.subTest(status=status):
                self.plan([{ "status": status, "headers": {"Retry-After": "0"}}, {"text": "retry-ok"}])
                before = len(self.server.requests)
                completed, _ = self.run_codex("retry marker")
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                self.assertEqual(len(self.server.requests), before + 2)
                for record in self.server.requests[before:]:
                    self.assertEqual(json.dumps(record["body"]).count("retry marker"), 1)
        self.plan([], {"status": 503})
        before = len(self.server.requests)
        completed, events = self.run_codex("exhaust retry budget")
        self.assert_failed(completed, events)
        # Request retries run inside the stream reconnection budget: with one
        # retry at both levels an exhausted request produces four HTTP attempts.
        self.assertEqual(len(self.server.requests), before + 4)
        self.plan([{"status":429,"headers":{"Retry-After":"1"}},{"text":"after-retry-after"}])
        before = len(self.server.requests)
        completed, _ = self.run_codex("positive Retry-After")
        self.assertEqual(completed.returncode,0,completed.stdout + completed.stderr)
        self.assertEqual(len(self.server.requests),before+2)
        delay = self.server.requests[before+1]["at"] - self.server.requests[before]["at"]
        self.assertGreaterEqual(delay,0.8,"positive Retry-After was not honored")

    def test_resume_after_key_model_endpoint_switch_real_cli(self):
        self.plan([{"text": "first-turn-answer"}])
        completed, events = self.run_codex("history-before-provider-switch")
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        thread_id = next(e["thread_id"] for e in events if e["type"] == "thread.started")
        other = MockServer()
        thread = threading.Thread(target=other.serve_forever, daemon=True)
        thread.start()
        self.extra_servers.append((other, thread))
        other.plans[fingerprint("second-fake-key")] = [{"text": "second-turn-answer"}]
        self.key = "second-fake-key"
        self.configure(url=f"http://127.0.0.1:{other.server_port}/v1", model="second-model")
        completed, _ = self.run_codex("continue after provider switch", resume=thread_id)
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("second-turn-answer", completed.stdout)
        self.assertEqual(other.requests[-1]["body"]["model"], "second-model")
        self.assertEqual(other.requests[-1]["key_sha256"], fingerprint(self.key))
        self.assertIn("history-before-provider-switch", json.dumps(other.requests[-1]["body"]))

    def test_cancel_during_retry_and_tool_loop_real_cli(self):
        (self.root / "hello.txt").write_text("loop-tool-marker")
        for loop in (False, True):
            with self.subTest(loop=loop):
                self.configure(retries=2)
                self.plan([], {"tool": {"name": "__mock_shell__", "command": "cat hello.txt"}} if loop else
                          {"status": 429, "headers": {"Retry-After": "5"}})
                before = len(self.server.requests)
                args, env = self.command("loop until cancelled")
                stdout_file = tempfile.TemporaryFile(mode="w+")
                stderr_file = tempfile.TemporaryFile(mode="w+")
                child = subprocess.Popen(args, env=env, cwd=self.root, stdout=stdout_file, stderr=stderr_file,
                                         text=True, start_new_session=True)
                try:
                    deadline = time.monotonic() + 40
                    while len(self.server.requests) < before + (3 if loop else 1):
                        self.assertLess(time.monotonic(), deadline, "Codex never contacted the local mock")
                        if child.poll() is not None:
                            stdout_file.seek(0)
                            stderr_file.seek(0)
                            self.fail("Codex exited before cancellation: " + stdout_file.read() + stderr_file.read())
                        time.sleep(0.02)
                    os.killpg(child.pid, signal.SIGTERM)
                    child.communicate(timeout=5)
                    count = len(self.server.requests)
                    time.sleep(1)
                    self.assertEqual(len(self.server.requests), count, "model requests continued after cancellation")
                finally:
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.communicate(timeout=5)
                    stdout_file.close()
                    stderr_file.close()
        self.configure()
        self.plan([{"text": "after-codex-cancel"}])
        completed, _ = self.run_codex()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_default_unbounded_connection_retry_is_cancellable(self):
        self.configure(url="http://no-such-model.invalid/v1", unbounded=True)
        args, env = self.command("unbounded connection")
        with tempfile.TemporaryFile(mode="w+") as out, tempfile.TemporaryFile(mode="w+") as err:
            child = subprocess.Popen(args,env=env,cwd=self.root,stdout=out,stderr=err,start_new_session=True)
            try:
                deadline = time.monotonic() + 15
                while True:
                    out.seek(0)
                    text = out.read()
                    if "waiting for network" in text:
                        break
                    self.assertIsNone(child.poll(),"unbounded mode unexpectedly exited")
                    self.assertLess(time.monotonic(),deadline,"missing network retry notification")
                    time.sleep(0.05)
                os.killpg(child.pid,signal.SIGTERM)
                child.wait(timeout=5)
                out.seek(0)
                events = [json.loads(line) for line in out.read().splitlines() if line.startswith("{")]
                self.assertFalse(any(e["type"] == "turn.completed" for e in events))
                self.assertNotEqual(child.returncode,0)
            finally:
                if child.poll() is None:
                    os.killpg(child.pid,signal.SIGKILL)
                    child.wait(timeout=5)
        self.configure()
        self.plan([{"text":"recovered-after-unbounded-cancel"}])
        completed,_ = self.run_codex()
        self.assertEqual(completed.returncode,0,completed.stdout+completed.stderr)


if __name__ == "__main__":
    unittest.main()
