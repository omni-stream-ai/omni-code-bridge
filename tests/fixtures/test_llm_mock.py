"""Wire-level fixture checks. These are not agent integration tests."""
import concurrent.futures
import http.client
import json
import socket
import threading
import time
import unittest
import urllib.error
import urllib.request

from llm_mock import MockServer, fingerprint


class MockTests(unittest.TestCase):
    def setUp(self):
        self.server = MockServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.plan("a", [])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def request(self, path, body=None, key="a", timeout=2):
        headers = {"Content-Type": "application/json"}
        headers["x-api-key" if path.endswith("messages") else "Authorization"] = key if path.endswith("messages") else "Bearer " + key
        data = None if body is None else json.dumps(body).encode()
        try:
            with urllib.request.urlopen(urllib.request.Request(self.base + path, data, headers), timeout=timeout) as response:
                return response.status, response.read(), response.headers
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read(), error.headers

    def plan(self, key, steps):
        self.assertEqual(self.request("/__mock/plan", {"key": key, "steps": steps})[0], 200)

    def records(self):
        return json.loads(self.request("/__mock/requests")[1])

    def test_all_protocols_json_stream_unicode_and_tools(self):
        for path in ("chat/completions", "responses", "messages"):
            for stream in (False, True):
                for tool in (None, {"name": "read", "arguments": {"path": "hello.txt"}}):
                    with self.subTest(path=path, stream=stream, tool=tool):
                        self.plan("a", [{"text": "你好🌍", "tool": tool, "fragment_bytes": 1}])
                        status, data, headers = self.request("/v1/" + path, {"model": "mock", "stream": stream})
                        self.assertEqual(status, 200)
                        for token in (["read"] if tool else ["你", "好", "🌍"]):
                            self.assertIn(token.encode(), data)
                        if stream:
                            self.assertEqual(headers["Content-Type"], "text/event-stream")
                            self.assertIn(b"[DONE]" if path == "chat/completions" else b"message_stop" if path == "messages" else b"response.completed", data)
                        else:
                            json.loads(data)

    def test_parameters_switch_reset_history_and_secret_redaction(self):
        params = {"temperature": 0.2, "top_p": 0.7, "max_tokens": 42, "stop": ["END"], "stream": False}
        history = [{"role": "user", "content": "first"}, {"role": "assistant", "content": "reply"}, {"role": "user", "content": "next"}]
        self.plan("b", [{"expect": {"model": "second", **params}}])
        self.request("/v1/chat/completions", {"model": "first", "messages": history[:1]})
        self.assertEqual(self.request("/v1/chat/completions", {"model": "second", "messages": history, **params}, "b")[0], 200)
        self.request("/v1/messages", {"model": "third", "messages": history}, "b")
        records = self.records()
        self.assertEqual([r["body"]["model"] for r in records], ["first", "second", "third"])
        self.assertEqual(records[1]["key_sha256"], fingerprint("b"))
        self.assertEqual(records[1]["body"]["messages"], history)
        self.assertNotIn("temperature", records[2]["body"])
        for record in records:
            self.assertNotIn("authorization", record["headers"])
            self.assertNotIn("x-api-key", record["headers"])

    def test_errors_and_switch_back(self):
        for status in (400, 401, 403, 404, 408, 409, 413, 422, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.plan("b", [{"status": status, "headers": {"Retry-After": "0"}}])
                code, payload, headers = self.request("/v1/chat/completions", {"model": "bad"}, "b")
                self.assertEqual(code, status)
                self.assertIn("error", json.loads(payload))
                self.assertEqual(headers["Retry-After"], "0")
                self.assertEqual(self.request("/v1/chat/completions", {"model": "good"})[0], 200)

    def test_retry_sequence_and_tool_roundtrip_capture(self):
        self.plan("a", [{"status": 429}, {"status": 503}, {"tool": {"name": "read", "arguments": {"path": "hello.txt"}}}, {"text": "finished"}])
        body = {"model": "mock", "messages": [{"role": "user", "content": "read file"}]}
        self.assertEqual(self.request("/v1/chat/completions", body)[0], 429)
        self.assertEqual(self.request("/v1/chat/completions", body)[0], 503)
        reply = json.loads(self.request("/v1/chat/completions", body)[1])["choices"][0]["message"]
        body["messages"] += [reply, {"role": "tool", "tool_call_id": "call_mock", "content": "file contents"}]
        self.assertIn(b"finished", self.request("/v1/chat/completions", body)[1])
        self.assertEqual(self.records()[-1]["body"]["messages"][-1]["tool_call_id"], "call_mock")

    def test_bad_requests_auth_paths_and_expectations(self):
        self.assertEqual(self.request("/v1/chat/completions", {"model": "mock"}, "unknown")[0], 401)
        self.assertEqual(self.request("/v1/chat/completions", {})[0], 400)
        self.assertEqual(self.request("/v1/wrong", {"model": "mock"})[0], 404)
        self.assertEqual(self.request("/__mock/plan", {"steps": "bad"})[0], 400)
        self.assertEqual(self.request("/v1/messages", [1])[0], 400)
        self.plan("a", [{"expect": {"model": "expected"}}])
        self.assertEqual(self.request("/v1/responses", {"model": "wrong"})[0], 422)
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port)
        connection.request("POST", "/v1/messages", b"{broken")
        self.assertEqual(connection.getresponse().status, 400)
        connection.close()

    def test_malformed_truncated_disconnect_timeout_and_recovery(self):
        for path in ("chat/completions", "responses", "messages"):
            for stream in (False, True):
                self.plan("a", [{"malformed": True}])
                data = self.request("/v1/" + path, {"model": "mock", "stream": stream})[1]
                self.assertIn(b"{broken", data)
            self.plan("a", [{"truncate_after": 1}])
            data = self.request("/v1/" + path, {"model": "mock", "stream": True})[1]
            self.assertNotIn(b"[DONE]", data)
            self.assertNotIn(b"response.completed", data)
            self.assertNotIn(b"message_stop", data)
        self.plan("a", [{"disconnect": True}])
        with self.assertRaises((http.client.RemoteDisconnected, urllib.error.URLError)):
            self.request("/v1/chat/completions", {"model": "mock"})
        self.plan("a", [{"delay": 0.15}])
        with self.assertRaises((TimeoutError, socket.timeout)):
            self.request("/v1/chat/completions", {"model": "mock"}, timeout=0.02)
        self.assertEqual(self.request("/v1/chat/completions", {"model": "recovered"})[0], 200)

    def test_inflight_switch_parallel_sessions_and_separate_endpoints(self):
        self.plan("a", [{"delay": 0.15, "text": "old-endpoint"}])
        self.plan("b", [{"text": "new-key"}])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            old = executor.submit(self.request, "/v1/chat/completions", {"model": "old"})
            deadline = time.monotonic() + 2
            while not self.records():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.005)
            self.assertIn(b"new-key", self.request("/v1/chat/completions", {"model": "new"}, "b")[1])
            self.assertIn(b"old-endpoint", old.result()[1])
        other = MockServer()
        thread = threading.Thread(target=other.serve_forever, daemon=True)
        thread.start()
        original = self.base
        try:
            self.base = f"http://127.0.0.1:{other.server_port}"
            self.plan("a", [{"text": "other-endpoint"}])
            self.assertIn(b"other-endpoint", self.request("/v1/responses", {"model": "other"})[1])
            self.assertEqual(len(self.records()), 1)
        finally:
            self.base = original
            other.shutdown()
            other.server_close()
            thread.join()
        self.assertEqual(len(self.records()), 2)


if __name__ == "__main__":
    unittest.main()
